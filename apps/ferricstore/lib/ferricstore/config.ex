defmodule Ferricstore.Config do
  @moduledoc """
  GenServer managing runtime configuration for FerricStore.

  Provides a FerricStore `CONFIG GET`/`CONFIG SET` interface for reading and
  writing server parameters at runtime. All values are stored as strings.

  ## Parameter categories

  **Read-only** (CONFIG GET only -- CONFIG SET returns an error):

    * `"maxmemory"` -- max memory budget in bytes from MemoryGuard
    * `"maxclients"` -- maximum simultaneous client connections
    * `"native-port"` -- Ferric native protocol TCP port the server is listening on
    * `"data-dir"` -- Bitcask data directory path
    * `"native-tls-port"` -- Ferric native protocol TLS port (`"0"` if not configured)
    * `"native-tls-cert-file"` -- path to PEM certificate file
    * `"native-tls-key-file"` -- path to PEM private key file
    * `"native-tls-ca-cert-file"` -- path to CA certificate bundle
    * `"require-tls"` -- whether plaintext connections are rejected (`"true"` / `"false"`)

  **Read-write** (CONFIG GET and CONFIG SET):

    * `"maxmemory-policy"` -- eviction policy (volatile-lru, allkeys-lru,
      volatile-ttl, noeviction)
    * `"notify-keyspace-events"` -- keyspace notification flag string
    * `"slowlog-log-slower-than"` -- slowlog threshold in microseconds
    * `"slowlog-max-len"` -- maximum slowlog entries
    * `"hz"` -- server tick frequency (stub, always reports 10)

  Plus compatibility parameters: `timeout`, `tcp-keepalive`, `databases`,
  `bind`, `save`, `appendonly`, `loglevel`, `requirepass`.

  ## Telemetry

  On every successful `CONFIG SET`, emits:

      [:ferricstore, :config, :changed]

  with measurements `%{}` and metadata
  `%{param: key, value: new_value, old_value: previous_value}`.

  ## Usage

      Ferricstore.Config.get("max*")
      #=> [{"maxmemory", "1073741824"}, {"maxclients", "10000"}, {"maxmemory-policy", "volatile-lru"}]

      Ferricstore.Config.set("maxmemory-policy", "allkeys-lru")
      #=> :ok

      Ferricstore.Config.set("maxmemory", "999")
      #=> {:error, "ERR Unsupported CONFIG parameter: maxmemory (read-only)"}
  """

  use GenServer

  @max_config_file_bytes 1_048_576
  @max_param_name_bytes 128
  @max_param_value_bytes 65_536
  @valid_notification_flags MapSet.new(~c"KEg$hlsztxA")

  require Logger

  # Read-only parameters whose values are derived from Application env
  # or other runtime sources at init time. CONFIG SET on these returns an error.
  @read_only_params MapSet.new([
                      "maxmemory",
                      "maxclients",
                      "native-port",
                      "data-dir",
                      "native-tls-port",
                      "native-tls-cert-file",
                      "native-tls-key-file",
                      "native-tls-ca-cert-file",
                      "require-tls"
                    ])

  # Read-write parameters with validators. Each key maps to a validator
  # function that returns :ok or {:error, reason}.
  @read_write_params MapSet.new([
                       "maxmemory-policy",
                       "notify-keyspace-events",
                       "slowlog-log-slower-than",
                       "slowlog-max-len",
                       "hz",
                       "keydir-max-ram",
                       "hot-cache-max-ram",
                       "hot-cache-min-ram",
                       "hot-cache-max-value-size"
                     ])

  # Valid eviction policy names (string form used by Redis CONFIG SET)
  @valid_eviction_policies MapSet.new([
                             "volatile-lru",
                             "allkeys-lru",
                             "volatile-ttl",
                             "noeviction"
                           ])

  # Redis-compatible parameters retained for CONFIG round trips. Parameters
  # without a dedicated side effect are stored in the runtime config map.
  @redis_compatible_defaults %{
    "timeout" => "0",
    "tcp-keepalive" => "300",
    "databases" => "1",
    "bind" => "127.0.0.1",
    "save" => "",
    "appendonly" => "no",
    "loglevel" => "notice",
    "requirepass" => ""
  }

  @sensitive_params MapSet.new(["requirepass"])
  @redacted_config_value ""
  @redacted_metadata_value "[redacted]"

  # -------------------------------------------------------------------
  # Types
  # -------------------------------------------------------------------

  @typedoc "Configuration parameter name."
  @type param_name :: binary()

  @typedoc "Configuration parameter value (always a string)."
  @type param_value :: binary()

  # -------------------------------------------------------------------
  # Public API
  # -------------------------------------------------------------------

  @doc """
  Starts the Config GenServer and registers it under `Ferricstore.Config`.

  Reads initial values for read-only parameters from Application env and
  MemoryGuard configuration.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc """
  Returns all config key-value pairs whose keys match the given glob `pattern`.

  The pattern supports `*` (match any sequence) and `?` (match single char).

  ## Parameters

    - `pattern` -- glob pattern string (e.g. `"*"`, `"max*"`, `"hz"`)

  ## Returns

  A list of `{key, value}` tuples for every matching parameter, sorted by key.

  ## Examples

      iex> Ferricstore.Config.get("hz")
      [{"hz", "10"}]

      iex> Ferricstore.Config.get("nonexistent")
      []
  """
  @spec get(binary()) :: [{param_name(), param_value()}]
  def get(pattern) do
    GenServer.call(__MODULE__, {:get, pattern})
  end

  @doc """
  Sets a runtime configuration parameter.

  Validates the parameter name (must be a known read-write parameter) and
  the value (type/range checks). Applies side-effects for parameters that
  affect runtime behaviour (e.g. updating Application env for slowlog
  thresholds, updating MemoryGuard eviction policy).

  Emits a `[:ferricstore, :config, :changed]` telemetry event on success.

  ## Parameters

    - `key` -- parameter name (e.g. `"hz"`, `"maxmemory-policy"`)
    - `value` -- new value as a string

  ## Returns

    - `:ok` on success
    - `{:error, reason}` when the parameter is read-only, unknown, or the
      value fails validation

  ## Examples

      iex> Ferricstore.Config.set("maxmemory-policy", "allkeys-lru")
      :ok

      iex> Ferricstore.Config.set("maxmemory", "999")
      {:error, "ERR Unsupported CONFIG parameter: maxmemory (read-only)"}
  """
  @spec set(binary(), binary()) :: :ok | {:error, binary()}
  def set(key, value) do
    GenServer.call(__MODULE__, {:set, key, value})
  end

  @doc """
  Returns the current value for a single config key, or `nil` if not set.

  Reads directly from ETS (~100ns) instead of GenServer.call (~1-5us).
  The ETS table is updated on every CONFIG SET and at init. This eliminates
  the Config GenServer as a contention point for `requires_auth?` checks
  which run on every command.
  """
  @spec get_value(binary()) :: binary() | nil
  def get_value(key) do
    case :ets.lookup(:ferricstore_config, key) do
      [{^key, value}] -> value
      [] -> nil
    end
  rescue
    ArgumentError ->
      # ETS table not yet created (early startup); fall back to GenServer.
      GenServer.call(__MODULE__, {:get_value, key})
  end

  @doc """
  Returns true when a config parameter carries secret material and should not be
  exposed through client-visible responses, telemetry, or audit metadata.
  """
  @spec sensitive_param?(binary()) :: boolean()
  def sensitive_param?(key) when is_binary(key) do
    key
    |> String.downcase()
    |> then(&MapSet.member?(@sensitive_params, &1))
  end

  @spec sensitive_param?(term()) :: false
  def sensitive_param?(_key), do: false

  @doc """
  Redacts a config value for CONFIG GET output.
  """
  @spec redact_for_config_get(binary(), binary()) :: binary()
  def redact_for_config_get(key, value) do
    if sensitive_param?(key), do: @redacted_config_value, else: value
  end

  @doc """
  Redacts a config value for telemetry and audit metadata.
  """
  @spec redact_for_metadata(binary(), binary()) :: binary()
  def redact_for_metadata(key, value) do
    if sensitive_param?(key), do: @redacted_metadata_value, else: value
  end

  @doc """
  Returns the full map of default configuration values.

  Note: this returns the static defaults only, not the live state which
  includes values derived from Application env.
  """
  @spec defaults() :: %{param_name() => param_value()}
  def defaults do
    build_initial_state()
  end

  @doc """
  Persists the current runtime configuration to disk.

  Writes all current configuration key-value pairs to a file at
  `<data_dir>/ferricstore.conf`. Each line is formatted as `key value`.

  ## Returns

    * `:ok` on success
    * `{:error, reason}` when the file cannot be written
  """
  @spec rewrite() :: :ok | {:error, binary()}
  def rewrite do
    GenServer.call(__MODULE__, :rewrite)
  end

  @doc """
  Returns the path where `CONFIG REWRITE` persists configuration.

  The path is `<data_dir>/ferricstore.conf`.
  """
  @spec config_file_path() :: binary()
  def config_file_path do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    Path.join(data_dir, "ferricstore.conf")
  end

  # -------------------------------------------------------------------
  # GenServer callbacks
  # -------------------------------------------------------------------

  @impl true
  def init(:ok) do
    # Create a protected ETS table for lock-free reads via get_value/1.
    # Connection processes read `requirepass` on every command; routing
    # through GenServer.call serialized all connections through this process.
    if :ets.whereis(:ferricstore_config) == :undefined do
      :ets.new(:ferricstore_config, [
        :set,
        :protected,
        :named_table,
        {:read_concurrency, true}
      ])
    end

    state = build_initial_state()
    sync_ets(state)
    {:ok, state}
  end

  @impl true
  def handle_call({:get, pattern}, _from, state) do
    # Refresh read-only params from live sources before returning
    state = refresh_read_only(state)
    sync_ets(state)

    result =
      state
      |> Enum.filter(fn {key, _val} -> Ferricstore.GlobMatcher.match?(key, pattern) end)
      |> Enum.map(fn {key, val} -> {key, redact_for_config_get(key, val)} end)
      |> Enum.sort_by(fn {key, _val} -> key end)

    {:reply, result, state}
  end

  def handle_call({:set, key, value}, _from, state) do
    cond do
      not is_binary(key) or not is_binary(value) ->
        {:reply, {:error, "ERR CONFIG parameter and value must be strings"}, state}

      byte_size(key) > @max_param_name_bytes ->
        {:reply, {:error, "ERR CONFIG parameter name is too large"}, state}

      byte_size(value) > @max_param_value_bytes ->
        {:reply, {:error, "ERR CONFIG value is too large"}, state}

      MapSet.member?(@read_only_params, key) ->
        {:reply, {:error, "ERR Unsupported CONFIG parameter: #{key} (read-only)"}, state}

      MapSet.member?(@read_write_params, key) ->
        case validate_param(key, value) do
          :ok ->
            publish_config_change(key, value, state)

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      Map.has_key?(@redis_compatible_defaults, key) ->
        publish_config_change(key, value, state)

      true ->
        {:reply, {:error, "ERR Unsupported CONFIG parameter: #{key}"}, state}
    end
  end

  def handle_call({:get_value, key}, _from, state) do
    state = refresh_read_only(state)
    {:reply, Map.get(state, key), state}
  end

  def handle_call(:rewrite, _from, state) do
    state = refresh_read_only(state)
    path = config_file_path()
    dir = Path.dirname(path)
    Ferricstore.FS.mkdir_p(dir)

    case validate_config_rewrite_path(path, dir) do
      :ok -> :ok
      {:error, _reason} = error -> throw(error)
    end

    # Read existing file content (if any)
    existing_lines =
      case Ferricstore.FS.read_nofollow(path, @max_config_file_bytes) do
        {:ok, content} ->
          if String.valid?(content) do
            String.split(content, "\n")
          else
            throw({:error, "ERR config file must be valid UTF-8"})
          end

        {:error, {:not_found, _reason}} ->
          []

        {:error, {:too_large, _reason}} ->
          throw({:error, "ERR config file exceeds #{@max_config_file_bytes} bytes"})

        {:error, reason} ->
          throw({:error, "ERR failed to read config file: #{inspect(reason)}"})
      end

    # Track which keys from state have been written
    remaining_keys = MapSet.new(Map.keys(state))

    # Process existing lines: preserve comments/blanks, update known keys, keep unknowns
    # Use cons + reverse to avoid O(n^2) from repeated ++ [line] appends.
    {reversed_lines, written_keys} =
      Enum.reduce(existing_lines, {[], MapSet.new()}, fn line, {lines_acc, written_acc} ->
        rewrite_config_line(line, state, lines_acc, written_acc)
      end)

    output_lines = Enum.reverse(reversed_lines)

    # Append keys from state that weren't already in the file
    new_keys =
      MapSet.difference(remaining_keys, written_keys)
      |> Enum.sort()

    appended_lines =
      Enum.map(new_keys, fn key ->
        value = rewrite_value(key, Map.get(state, key, ""))
        "#{key} #{value}"
      end)

    all_lines = output_lines ++ appended_lines

    # Remove trailing empty strings to avoid double newlines at end
    all_lines =
      all_lines
      |> Enum.reverse()
      |> Enum.drop_while(&(&1 == ""))
      |> Enum.reverse()

    content = Enum.join(all_lines, "\n") <> "\n"

    if byte_size(content) > @max_config_file_bytes do
      throw({:error, "ERR rewritten config exceeds #{@max_config_file_bytes} bytes"})
    end

    # Atomic write: write to tmp then rename
    result = atomic_write_file(path, content)
    {:reply, result, state}
  catch
    {:error, _reason} = error -> {:reply, error, state}
  end

  defp publish_config_change(key, value, state) do
    case safe_apply_side_effect(key, value) do
      :ok ->
        old_value = Map.get(state, key, "")
        new_state = Map.put(state, key, value)
        emit_config_changed(key, value, old_value)
        sync_ets_key(key, value)
        {:reply, :ok, new_state}

      {:error, reason} ->
        message =
          "ERR failed to apply CONFIG parameter '#{key}': #{format_reason(reason)}"

        {:reply, {:error, message}, state}
    end
  end

  defp rewrite_config_line(line, state, lines_acc, written_acc) do
    trimmed = String.trim(line)

    cond do
      trimmed == "" or String.starts_with?(trimmed, "#") ->
        {[line | lines_acc], written_acc}

      true ->
        case String.split(trimmed, " ", parts: 2) do
          [key | _rest] when is_map_key(state, key) ->
            value = rewrite_value(key, Map.get(state, key, ""))
            {["#{key} #{value}" | lines_acc], MapSet.put(written_acc, key)}

          [_key | _rest] ->
            {[line | lines_acc], written_acc}

          _ ->
            {[line | lines_acc], written_acc}
        end
    end
  end

  defp rewrite_value(key, value) do
    if sensitive_param?(key), do: @redacted_config_value, else: value
  end

  defp atomic_write_file(path, content) do
    tmp_path = path <> ".tmp"

    with :ok <- secure_write_tmp(tmp_path, content),
         :ok <- Ferricstore.FS.rename(tmp_path, path),
         :ok <- File.chmod(path, 0o600),
         :ok <- Ferricstore.Bitcask.NIF.v2_fsync_dir(Path.dirname(path)) do
      :ok
    else
      {:error, reason} ->
        case Ferricstore.FS.rm(tmp_path) do
          :ok ->
            :ok

          {:error, {:not_found, _}} ->
            :ok

          {:error, cleanup_reason} ->
            :telemetry.execute(
              [:ferricstore, :config, :rewrite, :cleanup_failed],
              %{count: 1},
              %{path: tmp_path, reason: cleanup_reason}
            )

            Logger.warning(
              "CONFIG REWRITE failed to remove config rewrite tmp file #{tmp_path}: #{inspect(cleanup_reason)}"
            )
        end

        {:error, "ERR failed to write/rename config file: #{inspect(reason)}"}
    end
  end

  defp secure_write_tmp(tmp_path, content) do
    case File.open(tmp_path, [:write, :exclusive, :binary]) do
      {:ok, io} ->
        try do
          with :ok <- File.chmod(tmp_path, 0o600),
               :ok <- IO.binwrite(io, content),
               :ok <- :file.sync(io),
               :ok <- File.close(io) do
            :ok
          else
            {:error, reason} ->
              _ = File.close(io)
              {:error, reason}
          end
        rescue
          error ->
            _ = File.close(io)
            {:error, error}
        catch
          kind, reason ->
            _ = File.close(io)
            {:error, {kind, reason}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp validate_config_rewrite_path(path, data_dir) do
    abs_path = Path.expand(path)
    abs_dir = Path.expand(data_dir)

    cond do
      not (String.starts_with?(abs_path, abs_dir <> "/") or abs_path == abs_dir) ->
        {:error, "ERR CONFIG REWRITE path escapes data directory"}

      symlink?(path) ->
        {:error, "ERR CONFIG REWRITE path is a symlink, refusing for security"}

      true ->
        :ok
    end
  end

  defp symlink?(path) do
    case File.lstat(path) do
      {:ok, %{type: :symlink}} -> true
      _ -> false
    end
  end

  # -------------------------------------------------------------------
  # Private -- initial state
  # -------------------------------------------------------------------

  defp build_initial_state do
    read_only = %{
      "maxmemory" => read_maxmemory(),
      "maxclients" => read_maxclients(),
      "native-port" => read_native_port(),
      "data-dir" => read_data_dir(),
      "native-tls-port" => read_native_tls_port(),
      "native-tls-cert-file" => read_native_tls_cert_file(),
      "native-tls-key-file" => read_native_tls_key_file(),
      "native-tls-ca-cert-file" => read_native_tls_ca_cert_file(),
      "require-tls" => read_require_tls()
    }

    read_write = %{
      "maxmemory-policy" => read_eviction_policy(),
      "notify-keyspace-events" => "",
      "slowlog-log-slower-than" => read_slowlog_threshold(),
      "slowlog-max-len" => read_slowlog_max_len(),
      "hz" => "10",
      "keydir-max-ram" =>
        Integer.to_string(Application.get_env(:ferricstore, :keydir_max_ram, 256 * 1024 * 1024)),
      "hot-cache-max-ram" => read_hot_cache_max_ram(),
      "hot-cache-min-ram" =>
        Integer.to_string(Application.get_env(:ferricstore, :hot_cache_min_ram, 64 * 1024 * 1024)),
      "hot-cache-max-value-size" =>
        Integer.to_string(Application.get_env(:ferricstore, :hot_cache_max_value_size, 65_536))
    }

    Map.merge(@redis_compatible_defaults, Map.merge(read_only, read_write))
  end

  defp refresh_read_only(state) do
    state
    |> Map.put("maxmemory", read_maxmemory())
    |> Map.put("maxclients", read_maxclients())
    |> Map.put("native-port", read_native_port())
    |> Map.put("data-dir", read_data_dir())
    |> Map.put("native-tls-port", read_native_tls_port())
    |> Map.put("native-tls-cert-file", read_native_tls_cert_file())
    |> Map.put("native-tls-key-file", read_native_tls_key_file())
    |> Map.put("native-tls-ca-cert-file", read_native_tls_ca_cert_file())
    |> Map.put("require-tls", read_require_tls())
  end

  # -------------------------------------------------------------------
  # Private -- read current values from Application env / runtime
  # -------------------------------------------------------------------

  defp read_maxmemory do
    Application.get_env(:ferricstore, :max_memory_bytes, 0)
    |> to_string()
  end

  defp read_maxclients do
    Application.get_env(:ferricstore, :maxclients, 10_000)
    |> to_string()
  end

  defp read_native_port do
    Application.get_env(:ferricstore, :native_port, 6388)
    |> to_string()
  end

  defp read_data_dir do
    Application.get_env(:ferricstore, :data_dir, "data")
  end

  defp read_hot_cache_max_ram do
    case Application.get_env(:ferricstore, :hot_cache_max_ram) do
      nil ->
        max_mem = Application.get_env(:ferricstore, :max_memory_bytes, 1_073_741_824)
        keydir = Application.get_env(:ferricstore, :keydir_max_ram, 256 * 1024 * 1024)
        Integer.to_string(max(max_mem - keydir, 64 * 1024 * 1024))

      val ->
        Integer.to_string(val)
    end
  end

  defp read_eviction_policy do
    case Application.get_env(:ferricstore, :eviction_policy, :volatile_lru) do
      :volatile_lru -> "volatile-lru"
      :allkeys_lru -> "allkeys-lru"
      :volatile_ttl -> "volatile-ttl"
      :noeviction -> "noeviction"
      other -> to_string(other)
    end
  end

  defp read_slowlog_threshold do
    Application.get_env(:ferricstore, :slowlog_log_slower_than_us, 10_000)
    |> to_string()
  end

  defp read_slowlog_max_len do
    Application.get_env(:ferricstore, :slowlog_max_len, 128)
    |> to_string()
  end

  defp read_native_tls_port do
    (Application.get_env(:ferricstore, :native_tls_port) || 0)
    |> to_string()
  end

  defp read_native_tls_cert_file do
    Application.get_env(:ferricstore, :native_tls_cert_file, "")
  end

  defp read_native_tls_key_file do
    Application.get_env(:ferricstore, :native_tls_key_file, "")
  end

  defp read_native_tls_ca_cert_file do
    Application.get_env(:ferricstore, :native_tls_ca_cert_file, "")
  end

  defp read_require_tls do
    case Application.get_env(:ferricstore, :require_tls, false) do
      true -> "true"
      false -> "false"
    end
  end

  # -------------------------------------------------------------------
  # Private -- validation
  # -------------------------------------------------------------------

  defp validate_param("maxmemory-policy", value) do
    if MapSet.member?(@valid_eviction_policies, value) do
      :ok
    else
      {:error,
       "ERR Invalid argument '#{value}' for CONFIG SET 'maxmemory-policy'. " <>
         "Valid values: volatile-lru, allkeys-lru, volatile-ttl, noeviction"}
    end
  end

  defp validate_param("notify-keyspace-events", value) do
    if Enum.all?(:binary.bin_to_list(value), &MapSet.member?(@valid_notification_flags, &1)) do
      :ok
    else
      {:error, "ERR Invalid argument '#{value}' for CONFIG SET 'notify-keyspace-events'"}
    end
  end

  defp validate_param("slowlog-log-slower-than", value) do
    case Integer.parse(value) do
      {n, ""} when n >= -1 -> :ok
      _ -> {:error, "ERR Invalid argument '#{value}' for CONFIG SET 'slowlog-log-slower-than'"}
    end
  end

  defp validate_param("slowlog-max-len", value) do
    case Integer.parse(value) do
      {n, ""} when n >= 0 -> :ok
      _ -> {:error, "ERR Invalid argument '#{value}' for CONFIG SET 'slowlog-max-len'"}
    end
  end

  defp validate_param("hz", value) do
    case Integer.parse(value) do
      {n, ""} when n >= 1 and n <= 500 -> :ok
      _ -> {:error, "ERR Invalid argument '#{value}' for CONFIG SET 'hz'"}
    end
  end

  defp validate_param("hot-cache-max-ram", value) do
    case Integer.parse(value) do
      {n, ""} when n > 0 ->
        min = Application.get_env(:ferricstore, :hot_cache_min_ram, 0)

        if n < min do
          {:error, "ERR hot-cache-max-ram (#{n}) must be >= hot-cache-min-ram (#{min})"}
        else
          :ok
        end

      _ ->
        {:error, "ERR Invalid argument '#{value}' for CONFIG SET 'hot-cache-max-ram'"}
    end
  end

  defp validate_param("hot-cache-min-ram", value) do
    case Integer.parse(value) do
      {n, ""} when n > 0 ->
        max = Application.get_env(:ferricstore, :hot_cache_max_ram, :infinity)

        if max != :infinity and n > max do
          {:error, "ERR hot-cache-min-ram (#{n}) must be <= hot-cache-max-ram (#{max})"}
        else
          :ok
        end

      _ ->
        {:error, "ERR Invalid argument '#{value}' for CONFIG SET 'hot-cache-min-ram'"}
    end
  end

  defp validate_param("keydir-max-ram", value) do
    case Integer.parse(value) do
      {n, ""} when n > 0 -> :ok
      _ -> {:error, "ERR Invalid argument '#{value}' for CONFIG SET 'keydir-max-ram'"}
    end
  end

  defp validate_param("hot-cache-max-value-size", value) do
    case Integer.parse(value) do
      {n, ""} when n >= 0 -> :ok
      _ -> {:error, "ERR Invalid argument '#{value}' for CONFIG SET 'hot-cache-max-value-size'"}
    end
  end

  defp validate_param(_key, _value), do: :ok

  # -------------------------------------------------------------------
  # Private -- side effects (apply config change at runtime)
  # -------------------------------------------------------------------

  defp apply_side_effect("maxmemory-policy", value) do
    atom =
      case value do
        "volatile-lru" -> :volatile_lru
        "allkeys-lru" -> :allkeys_lru
        "volatile-ttl" -> :volatile_ttl
        "noeviction" -> :noeviction
      end

    with :ok <- reconfigure_memory_guard("maxmemory-policy", %{eviction_policy: atom}) do
      Application.put_env(:ferricstore, :eviction_policy, atom)
      :ok
    end
  end

  defp apply_side_effect("slowlog-log-slower-than", value) do
    {n, ""} = Integer.parse(value)
    Ferricstore.SlowLog.set_threshold(n)
  end

  defp apply_side_effect("slowlog-max-len", value) do
    {n, ""} = Integer.parse(value)
    Ferricstore.SlowLog.set_max_len(n)
  end

  defp apply_side_effect("keydir-max-ram", value) do
    {n, ""} = Integer.parse(value)

    with :ok <- reconfigure_memory_guard("keydir-max-ram", %{keydir_max_ram: n}) do
      Application.put_env(:ferricstore, :keydir_max_ram, n)
      :ok
    end
  end

  defp apply_side_effect("hot-cache-max-ram", value) do
    {n, ""} = Integer.parse(value)

    with :ok <- reconfigure_memory_guard("hot-cache-max-ram", %{hot_cache_max_ram: n}) do
      Application.put_env(:ferricstore, :hot_cache_max_ram, n)
      :ok
    end
  end

  defp apply_side_effect("hot-cache-min-ram", value) do
    {n, ""} = Integer.parse(value)

    with :ok <- reconfigure_memory_guard("hot-cache-min-ram", %{hot_cache_min_ram: n}) do
      Application.put_env(:ferricstore, :hot_cache_min_ram, n)
      :ok
    end
  end

  defp apply_side_effect("hot-cache-max-value-size", value) do
    {n, ""} = Integer.parse(value)
    Application.put_env(:ferricstore, :hot_cache_max_value_size, n)
    :persistent_term.put(:ferricstore_hot_cache_max_value_size, n)
  end

  defp apply_side_effect("notify-keyspace-events", value) do
    # Update persistent_term cache for KeyspaceNotifications.notify/3.
    :persistent_term.put(:ferricstore_keyspace_events, value)
  end

  defp apply_side_effect(_key, _value), do: :ok

  defp safe_apply_side_effect(key, value) do
    apply_side_effect(key, value)
  rescue
    exception ->
      emit_side_effect_failed(key, :apply, :error, exception)
      {:error, exception}
  catch
    kind, reason ->
      emit_side_effect_failed(key, :apply, kind, reason)
      {:error, reason}
  end

  defp reconfigure_memory_guard(param, params) do
    try do
      case memory_guard_reconfigure(params) do
        :ok ->
          :ok

        {:error, reason} ->
          emit_side_effect_failed(param, :memory_guard_reconfigure, :return, reason)
          {:error, reason}

        other ->
          emit_side_effect_failed(param, :memory_guard_reconfigure, :return, other)
          {:error, {:unexpected_memory_guard_result, other}}
      end
    rescue
      exception ->
        emit_side_effect_failed(param, :memory_guard_reconfigure, :error, exception)
        {:error, exception}
    catch
      kind, reason ->
        emit_side_effect_failed(param, :memory_guard_reconfigure, kind, reason)
        {:error, reason}
    end
  end

  defp format_reason(reason) when is_binary(reason), do: reason
  defp format_reason(reason), do: inspect(reason, limit: 10, printable_limit: 256)

  defp memory_guard_reconfigure(params) do
    case Application.get_env(:ferricstore, :config_memory_guard_reconfigure_hook) do
      fun when is_function(fun, 1) -> fun.(params)
      _ -> Ferricstore.MemoryGuard.reconfigure(params)
    end
  end

  # -------------------------------------------------------------------
  # Private -- telemetry
  # -------------------------------------------------------------------

  defp emit_config_changed(key, value, old_value) do
    :telemetry.execute(
      [:ferricstore, :config, :changed],
      %{},
      %{
        param: key,
        value: redact_for_metadata(key, value),
        old_value: redact_for_metadata(key, old_value)
      }
    )
  end

  defp emit_side_effect_failed(param, phase, kind, reason) do
    :telemetry.execute(
      [:ferricstore, :config, :side_effect_failed],
      %{count: 1},
      %{param: param, phase: phase, kind: kind, reason: reason}
    )
  end

  # -------------------------------------------------------------------
  # Private -- ETS sync for lock-free get_value/1 reads
  # -------------------------------------------------------------------

  defp sync_ets(state) do
    try do
      entries = Enum.map(state, fn {k, v} -> {k, v} end)
      :ets.insert(:ferricstore_config, entries)
    rescue
      ArgumentError -> :ok
    end
  end

  defp sync_ets_key(key, value) do
    try do
      :ets.insert(:ferricstore_config, {key, value})
    rescue
      ArgumentError -> :ok
    end
  end
end
