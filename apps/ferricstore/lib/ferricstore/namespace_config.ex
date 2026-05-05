defmodule Ferricstore.NamespaceConfig do
  @moduledoc """
  GenServer managing per-namespace (prefix) runtime configuration.

  Stores namespace-specific overrides for commit window timing in an ETS table
  (`:ferricstore_ns_config`). Prefixes that have no explicit override fall back
  to sensible defaults (1 ms window, `:quorum` durability).

  ## ETS schema

  Each entry is a tuple:

      {prefix, window_ms, durability, changed_at, changed_by}

  Where:

    * `prefix` -- binary namespace prefix (e.g. `"rate"`, `"session"`)
    * `window_ms` -- commit window in milliseconds (positive integer)
    * `durability` -- always `:quorum`; async durability is no longer supported
    * `changed_at` -- Unix timestamp (seconds) of the last change, or `0` for defaults
    * `changed_by` -- identifier of the client that made the change (empty string for defaults)

  ## Default values

  When no override exists for a prefix, the effective configuration is:

    * `window_ms` -- `1`
    * `durability` -- `:quorum`

  ## Usage

      Ferricstore.NamespaceConfig.set("rate", "window_ms", 10)
      #=> :ok

      Ferricstore.NamespaceConfig.get("rate")
      #=> {:ok, %{prefix: "rate", window_ms: 10, durability: :quorum, changed_at: 1711111111, changed_by: ""}}

      Ferricstore.NamespaceConfig.window_for("rate")
      #=> 10

      Ferricstore.NamespaceConfig.durability_for("rate")
      #=> :quorum

      Ferricstore.NamespaceConfig.reset("rate")
      #=> :ok
  """

  use GenServer

  @table :ferricstore_ns_config
  @default_window_ms 1

  defp get_default_durability, do: :quorum

  # ---------------------------------------------------------------------------
  # Types
  # ---------------------------------------------------------------------------

  @typedoc "A namespace configuration entry."
  @type ns_entry :: %{
          prefix: binary(),
          window_ms: non_neg_integer(),
          durability: :quorum,
          changed_at: non_neg_integer(),
          changed_by: binary()
        }

  @typedoc "Valid field names for `set/3`."
  @type field :: :window_ms | :durability

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Starts the NamespaceConfig GenServer and creates the backing ETS table.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Sets a configuration field for the given namespace prefix.

  ## Parameters

    * `prefix` -- namespace prefix string (e.g. `"rate"`)
    * `field` -- field name string: `"window_ms"` or `"durability"`
    * `value` -- field value string: a positive integer for `window_ms`,
      or `"quorum"` for `durability`

  ## Returns

    * `:ok` on success
    * `{:error, reason}` when the field name or value is invalid

  ## Examples

      iex> Ferricstore.NamespaceConfig.set("rate", "window_ms", "10")
      :ok

      iex> Ferricstore.NamespaceConfig.set("ts", "durability", "async")
      {:error, "ERR durability only supports 'quorum'; async durability has been removed"}
  """
  @spec set(binary(), binary(), binary()) :: :ok | {:error, binary()}
  def set(prefix, field, value)
      when is_binary(prefix) and is_binary(field) and is_binary(value) do
    set(prefix, field, value, "")
  end

  @doc """
  Sets a configuration field for the given namespace prefix with caller identity.

  Behaves identically to `set/3` but also records `changed_by` for audit
  trail purposes.

  ## Parameters

    * `prefix` -- namespace prefix string
    * `field` -- field name string: `"window_ms"` or `"durability"`
    * `value` -- field value string
    * `changed_by` -- identity of the caller making the change

  ## Returns

    * `:ok` on success
    * `{:error, reason}` when the field name or value is invalid
  """
  @spec set(binary(), binary(), binary(), binary()) :: :ok | {:error, binary()}
  def set(prefix, field, value, changed_by)
      when is_binary(prefix) and is_binary(field) and is_binary(value) and is_binary(changed_by) do
    normalized = String.trim_trailing(prefix, ":")

    case validate_field_value(field, value) do
      {:ok, parsed_field, parsed_value} ->
        do_set(normalized, parsed_field, parsed_value, changed_by)

      {:error, _} = err ->
        err
    end
  end

  @doc """
  Returns the configuration for a single namespace prefix.

  If the prefix has no explicit override, returns the default configuration.

  ## Parameters

    * `prefix` -- namespace prefix string

  ## Returns

    * `{:ok, ns_entry()}` -- always succeeds; returns defaults for unconfigured prefixes

  ## Examples

      iex> Ferricstore.NamespaceConfig.get("rate")
      {:ok, %{prefix: "rate", window_ms: 1, durability: :quorum, changed_at: 0, changed_by: ""}}
  """
  @spec get(binary()) :: {:ok, ns_entry()}
  def get(prefix) when is_binary(prefix) do
    case lookup(prefix) do
      nil ->
        {:ok, default_entry(prefix)}

      entry ->
        {:ok, entry}
    end
  end

  @doc """
  Returns configuration entries for all namespaces that have explicit overrides.

  Returns an empty list when no overrides have been set.

  ## Returns

  A list of `ns_entry()` maps, sorted by prefix.
  """
  @spec get_all() :: [ns_entry()]
  def get_all do
    try do
      :ets.tab2list(@table)
      |> Enum.map(&tuple_to_entry/1)
      |> Enum.sort_by(& &1.prefix)
    rescue
      ArgumentError -> []
    end
  end

  @doc """
  Resets the configuration for a single namespace prefix back to defaults.

  Deletes the explicit override from ETS. Subsequent calls to `get/1`,
  `window_for/1`, and `durability_for/1` will return default values.

  ## Parameters

    * `prefix` -- namespace prefix string

  ## Returns

    * `:ok` -- always succeeds (no-op if the prefix had no override)
  """
  @spec reset(binary()) :: :ok
  def reset(prefix) when is_binary(prefix) do
    try do
      :ets.delete(@table, prefix)
    rescue
      ArgumentError -> :ok
    end

    broadcast_ns_config_changed()

    :ok
  end

  @doc """
  Resets all namespace configurations back to defaults.

  Deletes all explicit overrides from ETS.

  ## Returns

    * `:ok`
  """
  @spec reset_all() :: :ok
  def reset_all do
    try do
      :ets.delete_all_objects(@table)
    rescue
      ArgumentError -> :ok
    end

    broadcast_ns_config_changed()

    :ok
  end

  @doc """
  Returns the effective `window_ms` for a namespace prefix.

  Falls back to the default (`#{@default_window_ms}`) when no override exists.

  ## Parameters

    * `prefix` -- namespace prefix string

  ## Returns

  A positive integer representing the commit window in milliseconds.
  """
  @spec window_for(binary()) :: pos_integer()
  def window_for(prefix) when is_binary(prefix) do
    case lookup(prefix) do
      nil -> @default_window_ms
      %{window_ms: w} -> w
    end
  end

  @doc """
  Returns the effective durability mode for a namespace prefix.

  Falls back to the default (`:quorum`) when no override exists.

  ## Parameters

    * `prefix` -- namespace prefix string

  ## Returns

  `:quorum`.
  """
  @spec durability_for(binary()) :: :quorum
  def durability_for(prefix) when is_binary(prefix), do: :quorum

  @doc """
  Returns the default window_ms value.
  """
  @spec default_window_ms() :: pos_integer()
  def default_window_ms, do: @default_window_ms

  @doc """
  Returns the default durability mode.
  """
  @spec get_default_durability() :: :quorum
  def default_durability, do: get_default_durability()

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(_opts) do
    case :ets.whereis(@table) do
      :undefined ->
        :ets.new(@table, [
          :set,
          :public,
          :named_table,
          {:read_concurrency, true},
          {:write_concurrency, true}
        ])

      _ref ->
        :ets.delete_all_objects(@table)
    end

    # Async durability has been removed. Keep the persistent terms stable for
    # older call sites while forcing the only supported write mode.
    init_mode = :all_quorum
    :persistent_term.put(:ferricstore_durability_mode, init_mode)
    :persistent_term.put(:ferricstore_has_async_ns, false)

    try do
      FerricStore.Instance.update_durability_mode(:default, init_mode)
    rescue
      _ -> :ok
    end

    apply_env_namespace_overrides()

    {:ok, %{}}
  end

  # ---------------------------------------------------------------------------
  # Env var: FERRICSTORE_NAMESPACE_DURABILITY
  # Format: "prefix1=mode1,prefix2=mode2". Only "quorum" is supported.
  # ---------------------------------------------------------------------------

  defp apply_env_namespace_overrides do
    case System.get_env("FERRICSTORE_NAMESPACE_DURABILITY") do
      nil ->
        :ok

      "" ->
        :ok

      raw ->
        raw
        |> String.split(",", trim: true)
        |> Enum.each(fn pair ->
          case String.split(pair, "=", parts: 2) do
            [prefix, mode] ->
              prefix = String.trim(prefix)
              mode = String.trim(mode)

              case set(prefix, "durability", mode, "env") do
                :ok ->
                  :ok

                {:error, reason} ->
                  require Logger

                  Logger.warning(
                    "FERRICSTORE_NAMESPACE_DURABILITY: failed to set #{prefix}=#{mode}: #{reason}"
                  )
              end

            _ ->
              require Logger
              Logger.warning("FERRICSTORE_NAMESPACE_DURABILITY: invalid entry #{inspect(pair)}")
          end
        end)
    end
  end

  # ---------------------------------------------------------------------------
  # Private -- ETS operations
  # ---------------------------------------------------------------------------

  defp do_set(prefix, field, value, changed_by) do
    now = System.os_time(:second)

    try do
      {old_durability, entry} = build_ns_entry(prefix, field, value, now, changed_by)
      :ets.insert(@table, entry)
      maybe_emit_durability_telemetry(field, old_durability, value, prefix, changed_by, now)
      broadcast_ns_config_changed()
      :ok
    rescue
      ArgumentError ->
        {:error, "ERR namespace config table not available"}
    end
  end

  defp build_ns_entry(prefix, field, value, now, changed_by) do
    case :ets.lookup(@table, prefix) do
      [{^prefix, window_ms, durability, _changed_at, _changed_by}] ->
        entry = build_entry_tuple(prefix, field, value, window_ms, durability, now, changed_by)
        {durability, entry}

      [] ->
        entry =
          build_entry_tuple(
            prefix,
            field,
            value,
            @default_window_ms,
            get_default_durability(),
            now,
            changed_by
          )

        {get_default_durability(), entry}
    end
  end

  defp build_entry_tuple(prefix, :window_ms, value, _window_ms, durability, now, changed_by) do
    {prefix, value, durability, now, changed_by}
  end

  defp build_entry_tuple(prefix, :durability, value, window_ms, _durability, now, changed_by) do
    {prefix, window_ms, value, now, changed_by}
  end

  defp maybe_emit_durability_telemetry(_field, _old, _new, _prefix, _by, _now), do: :ok

  defp lookup(prefix) do
    try do
      case :ets.lookup(@table, prefix) do
        [{^prefix, window_ms, durability, changed_at, changed_by}] ->
          %{
            prefix: prefix,
            window_ms: window_ms,
            durability: durability,
            changed_at: changed_at,
            changed_by: changed_by
          }

        [] ->
          nil
      end
    rescue
      ArgumentError -> nil
    end
  end

  defp tuple_to_entry({prefix, window_ms, durability, changed_at, changed_by}) do
    %{
      prefix: prefix,
      window_ms: window_ms,
      durability: durability,
      changed_at: changed_at,
      changed_by: changed_by
    }
  end

  defp default_entry(prefix) do
    %{
      prefix: prefix,
      window_ms: @default_window_ms,
      durability: get_default_durability(),
      changed_at: 0,
      changed_by: ""
    }
  end

  # ---------------------------------------------------------------------------
  # Private -- validation
  # ---------------------------------------------------------------------------

  defp validate_field_value("window_ms", value) do
    case Integer.parse(value) do
      {n, ""} when n > 0 ->
        {:ok, :window_ms, n}

      _ ->
        {:error, "ERR window_ms must be a positive integer"}
    end
  end

  defp validate_field_value("durability", "quorum") do
    {:ok, :durability, :quorum}
  end

  defp validate_field_value("durability", "async"),
    do: {:error, "ERR durability only supports 'quorum'; async durability has been removed"}

  defp validate_field_value("durability", value) do
    {:error, "ERR durability must be 'quorum', got '#{value}'"}
  end

  defp validate_field_value(field, _value) do
    {:error, "ERR unknown namespace config field '#{field}'"}
  end

  # ---------------------------------------------------------------------------
  # Private -- batcher cache invalidation
  # ---------------------------------------------------------------------------

  # Sends :ns_config_changed to all live batcher processes so they clear
  # their in-process namespace config caches. Uses send/2 (fire-and-forget)
  # and silently skips batchers that are not running (e.g. during tests or
  # when raft is disabled).
  @spec broadcast_ns_config_changed() :: :ok
  defp broadcast_ns_config_changed do
    durability_mode = :all_quorum

    :persistent_term.put(:ferricstore_durability_mode, durability_mode)
    :persistent_term.put(:ferricstore_has_async_ns, false)

    try do
      FerricStore.Instance.update_durability_mode(:default, durability_mode)
    rescue
      _ -> :ok
    end

    notify_batchers()

    :ok
  end

  defp notify_batchers do
    shard_count = Application.get_env(:ferricstore, :shard_count, 4)

    for i <- 0..(shard_count - 1) do
      name = :"Ferricstore.Raft.Batcher.#{i}"

      case Process.whereis(name) do
        nil -> :ok
        pid -> send(pid, :ns_config_changed)
      end
    end
  end
end
