defmodule Ferricstore.NamespaceConfig do
  @moduledoc """
  GenServer managing per-namespace commit-window configuration.

  Stores namespace-specific window overrides in `:ferricstore_ns_config`.
  Prefixes with no explicit override use a 1ms commit window.

  Durability mode is intentionally not configurable. All writes use the quorum
  path. A future acknowledgement policy such as `ack=all` should be added as a
  separate feature, not by reviving the removed durability-mode switch.

  ## ETS schema

      {prefix, window_ms, changed_at, changed_by}

  Where:

    * `prefix` -- binary namespace prefix (e.g. `"rate"`, `"session"`)
    * `window_ms` -- commit window in milliseconds (positive integer)
    * `changed_at` -- Unix timestamp (seconds) of the last change, or `0` for defaults
    * `changed_by` -- identifier of the client that made the change (empty string for defaults)
  """

  use GenServer

  @table :ferricstore_ns_config
  @default_window_ms 1
  @max_window_ms 10_000
  @default_max_entries 1_000
  @max_prefix_bytes 256
  @max_changed_by_bytes 256
  @has_overrides_key {__MODULE__, :has_overrides}

  @typedoc "A namespace configuration entry."
  @type ns_entry :: %{
          prefix: binary(),
          window_ms: non_neg_integer(),
          changed_at: non_neg_integer(),
          changed_by: binary()
        }

  @typedoc "Valid field names for `set/3`."
  @type field :: :window_ms

  @doc """
  Starts the NamespaceConfig GenServer and creates the backing ETS table.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Sets a configuration field for the given namespace prefix.

  Only `window_ms` is supported. Durability is not configurable.
  """
  @spec set(binary(), binary(), binary()) :: :ok | {:error, binary()}
  def set(prefix, field, value)
      when is_binary(prefix) and is_binary(field) and is_binary(value) do
    set(prefix, field, value, "")
  end

  @doc """
  Sets a configuration field and records caller identity for audit.
  """
  @spec set(binary(), binary(), binary(), binary()) :: :ok | {:error, binary()}
  def set(prefix, field, value, changed_by)
      when is_binary(prefix) and is_binary(field) and is_binary(value) and is_binary(changed_by) do
    with {:ok, normalized} <- normalize_stored_prefix(prefix),
         :ok <- validate_changed_by(changed_by),
         {:ok, parsed_field, parsed_value} <- validate_field_value(field, value) do
      GenServer.call(
        __MODULE__,
        {:set, normalized, parsed_field, parsed_value, :binary.copy(changed_by)}
      )
    end
  end

  @doc """
  Returns the configuration for a single namespace prefix.
  """
  @spec get(binary()) :: {:ok, ns_entry()}
  def get(prefix) when is_binary(prefix) do
    normalized = normalize_lookup_prefix(prefix)

    case lookup(normalized) do
      nil -> {:ok, default_entry(normalized)}
      entry -> {:ok, entry}
    end
  end

  @doc """
  Returns all explicitly configured namespace entries sorted by prefix.
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
  Resets one namespace prefix to the default window.
  """
  @spec reset(binary()) :: :ok
  def reset(prefix) when is_binary(prefix) do
    GenServer.call(__MODULE__, {:reset, normalize_lookup_prefix(prefix)})
  end

  @doc """
  Resets all namespace window overrides.
  """
  @spec reset_all() :: :ok
  def reset_all do
    GenServer.call(__MODULE__, :reset_all)
  end

  @doc """
  Returns the effective `window_ms` for a namespace prefix.
  """
  @spec window_for(binary()) :: pos_integer()
  def window_for(prefix) when is_binary(prefix) do
    case lookup(normalize_lookup_prefix(prefix)) do
      nil -> @default_window_ms
      %{window_ms: w} -> w
    end
  end

  @doc """
  Returns the default window_ms value.
  """
  @spec default_window_ms() :: pos_integer()
  def default_window_ms, do: @default_window_ms

  @doc false
  @spec has_overrides?() :: boolean()
  def has_overrides?, do: :persistent_term.get(@has_overrides_key, false)

  @impl true
  def init(_opts) do
    case :ets.whereis(@table) do
      :undefined ->
        :ets.new(@table, [
          :ordered_set,
          :protected,
          :named_table,
          {:read_concurrency, true},
          {:write_concurrency, true}
        ])

      _ref ->
        :ets.delete_all_objects(@table)
    end

    :persistent_term.put(@has_overrides_key, false)

    {:ok, %{}}
  end

  @impl true
  def handle_call({:set, prefix, :window_ms, value, changed_by}, _from, state) do
    now = System.os_time(:second)

    reply =
      if :ets.member(@table, prefix) or :ets.info(@table, :size) < max_entries() do
        :ets.insert(@table, {prefix, value, now, changed_by})
        :persistent_term.put(@has_overrides_key, true)
        :ok
      else
        {:error, "ERR namespace config limit reached (max #{max_entries()})"}
      end

    {:reply, reply, state}
  end

  def handle_call({:reset, prefix}, _from, state) do
    :ets.delete(@table, prefix)
    refresh_override_flag()
    {:reply, :ok, state}
  end

  def handle_call(:reset_all, _from, state) do
    :ets.delete_all_objects(@table)
    :persistent_term.put(@has_overrides_key, false)
    {:reply, :ok, state}
  end

  defp max_entries do
    case Application.get_env(
           :ferricstore,
           :namespace_config_max_entries,
           @default_max_entries
         ) do
      value when is_integer(value) and value >= 0 -> value
      _other -> @default_max_entries
    end
  end

  defp normalize_stored_prefix(prefix) do
    if String.valid?(prefix) do
      normalized = String.trim_trailing(prefix, ":")

      if byte_size(normalized) <= @max_prefix_bytes do
        {:ok, :binary.copy(normalized)}
      else
        {:error, "ERR namespace prefix exceeds #{@max_prefix_bytes} bytes"}
      end
    else
      {:error, "ERR namespace prefix must be valid UTF-8"}
    end
  end

  defp normalize_lookup_prefix(prefix) do
    if String.valid?(prefix), do: String.trim_trailing(prefix, ":"), else: prefix
  end

  defp validate_changed_by(changed_by) do
    cond do
      byte_size(changed_by) > @max_changed_by_bytes ->
        {:error, "ERR namespace changed_by exceeds #{@max_changed_by_bytes} bytes"}

      not String.valid?(changed_by) ->
        {:error, "ERR namespace changed_by must be valid UTF-8"}

      true ->
        :ok
    end
  end

  defp refresh_override_flag do
    has_overrides? =
      case :ets.info(@table, :size) do
        size when is_integer(size) and size > 0 -> true
        _other -> false
      end

    :persistent_term.put(@has_overrides_key, has_overrides?)
  end

  defp lookup(prefix) do
    try do
      case :ets.lookup(@table, prefix) do
        [{^prefix, window_ms, changed_at, changed_by}] ->
          %{
            prefix: prefix,
            window_ms: window_ms,
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

  defp tuple_to_entry({prefix, window_ms, changed_at, changed_by}) do
    %{
      prefix: prefix,
      window_ms: window_ms,
      changed_at: changed_at,
      changed_by: changed_by
    }
  end

  defp default_entry(prefix) do
    %{
      prefix: prefix,
      window_ms: @default_window_ms,
      changed_at: 0,
      changed_by: ""
    }
  end

  defp validate_field_value("window_ms", value) do
    case Integer.parse(value) do
      {n, ""} when n > 0 and n <= @max_window_ms ->
        {:ok, :window_ms, n}

      {n, ""} when n > @max_window_ms ->
        {:error, "ERR window_ms must be at most #{@max_window_ms} milliseconds"}

      _ ->
        {:error, "ERR window_ms must be a positive integer"}
    end
  end

  defp validate_field_value(field, _value) do
    {:error, "ERR unknown namespace config field '#{field}'"}
  end
end
