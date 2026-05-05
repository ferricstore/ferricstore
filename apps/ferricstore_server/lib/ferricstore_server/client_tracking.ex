defmodule FerricstoreServer.ClientTracking do
  @moduledoc """
  Server-assisted client-side caching via RESP3 push invalidation messages.

  When a client enables tracking with `CLIENT TRACKING ON`, the server records
  which keys the client reads. When any connection modifies a tracked key, the
  server sends a RESP3 push invalidation message to all connections tracking
  that key, allowing them to evict their local cache entry.

  ## Tracking modes

    * **Default mode** -- The server records every key the client reads (via GET,
      MGET, etc.) and sends invalidation when any of those keys are modified.

    * **BCAST mode** -- The client registers key prefixes. Every write whose key
      matches a registered prefix triggers an invalidation push. No per-read
      tracking occurs; this is purely prefix-based.

    * **OPTIN mode** -- Tracking is enabled but keys are only recorded after the
      client explicitly sends `CLIENT CACHING yes`. After one command is tracked,
      the flag resets to off.

    * **OPTOUT mode** -- Keys are tracked by default, but the client can skip
      tracking for the next command by sending `CLIENT CACHING no`.

  ## NOLOOP option

  When enabled, a client does not receive invalidation messages for keys that
  it modified itself. This avoids redundant invalidations when the writer
  already knows the new value.

  ## REDIRECT option

  Invalidation messages can be redirected to a different client connection ID,
  identified by the integer returned in the HELLO greeting. This is used when
  a dedicated connection is set up to receive all invalidation messages.

  ## ETS tables

  The module uses two ETS tables:

    * `:ferricstore_tracking` -- maps `{key, connection_pid}` to `true`, used
      to look up which connections are tracking a given key (default mode).

    * `:ferricstore_tracking_connections` -- maps `connection_pid` to the
      connection's tracking configuration (mode, prefixes, options).

  Both tables are `:public` and `:bag` / `:set` respectively, allowing
  concurrent reads from any process (the connection loop, shard GenServers,
  etc.) without serialization.

  ## Invalidation message format (RESP3 push)

      >2\\r\\n+invalidate\\r\\n*1\\r\\n$N\\r\\nkey\\r\\n

  This is a RESP3 push type containing a two-element array: the string
  "invalidate" followed by an array of invalidated keys.
  """

  alias FerricstoreServer.Resp.Encoder

  require Logger

  # ETS table names
  @tracking_table :ferricstore_tracking
  @connections_table :ferricstore_tracking_connections

  @type tracking_mode :: :default | :bcast
  @type eviction_policy :: :volatile_lru | :allkeys_lru | :volatile_ttl | :noeviction

  @type tracking_config :: %{
          enabled: boolean(),
          mode: tracking_mode(),
          prefixes: [binary()],
          redirect: pid() | nil,
          optin: boolean(),
          optout: boolean(),
          noloop: boolean(),
          caching: boolean()
        }

  # ---------------------------------------------------------------------------
  # Table initialization
  # ---------------------------------------------------------------------------

  @doc """
  Creates the ETS tables used for client tracking.

  Called once during application startup. The tables are `:public` so that
  connection processes and shard processes can read and write without going
  through a single process bottleneck.

  ## Returns

  `:ok` always. If tables already exist, this is a no-op.
  """
  @spec init_tables() :: :ok
  def init_tables do
    if :ets.whereis(@tracking_table) == :undefined do
      :ets.new(@tracking_table, [
        :bag,
        :public,
        :named_table,
        {:read_concurrency, true},
        {:write_concurrency, :auto},
        {:decentralized_counters, true}
      ])
    end

    if :ets.whereis(@connections_table) == :undefined do
      :ets.new(@connections_table, [
        :set,
        :public,
        :named_table,
        {:read_concurrency, true},
        {:write_concurrency, :auto},
        {:decentralized_counters, true}
      ])
    end

    :ok
  end

  # ---------------------------------------------------------------------------
  # Connection lifecycle
  # ---------------------------------------------------------------------------

  @doc """
  Returns a new default tracking configuration for a fresh connection.

  Tracking is disabled by default. The client must send `CLIENT TRACKING ON`
  to enable it.
  """
  @spec new_config() :: tracking_config()
  def new_config do
    %{
      enabled: false,
      mode: :default,
      prefixes: [],
      redirect: nil,
      optin: false,
      optout: false,
      noloop: false,
      caching: false
    }
  end

  @doc """
  Enables tracking for the connection identified by `conn_pid`.

  ## Options

    * `:mode` -- `:default` or `:bcast` (default: `:default`)
    * `:prefixes` -- list of key prefixes for BCAST mode (default: `[]`)
    * `:redirect` -- pid of the connection to receive invalidations (default: `nil`)
    * `:optin` -- if `true`, only track keys after `CLIENT CACHING yes` (default: `false`)
    * `:optout` -- if `true`, track by default but allow `CLIENT CACHING no` to skip (default: `false`)
    * `:noloop` -- if `true`, don't self-invalidate (default: `false`)

  ## Returns

  `{:ok, updated_config}` on success, `{:error, reason}` on failure.
  """
  @spec enable(pid(), tracking_config(), keyword()) ::
          {:ok, tracking_config()} | {:error, binary()}
  def enable(conn_pid, config, opts \\ []) do
    mode = Keyword.get(opts, :mode, :default)
    prefixes = Keyword.get(opts, :prefixes, [])
    redirect = Keyword.get(opts, :redirect, nil)
    optin = Keyword.get(opts, :optin, false)
    optout = Keyword.get(opts, :optout, false)
    noloop = Keyword.get(opts, :noloop, false)

    cond do
      optin and optout ->
        {:error, "ERR OPTIN and OPTOUT are mutually exclusive"}

      mode == :bcast and optin ->
        {:error, "ERR OPTIN is not compatible with BCAST mode"}

      mode == :bcast and optout ->
        {:error, "ERR OPTOUT is not compatible with BCAST mode"}

      true ->
        new_config = %{
          config
          | enabled: true,
            mode: mode,
            prefixes: prefixes,
            redirect: redirect,
            optin: optin,
            optout: optout,
            noloop: noloop,
            caching: not optin
        }

        :ets.insert(@connections_table, {conn_pid, new_config})
        {:ok, new_config}
    end
  end

  @doc """
  Disables tracking for the connection identified by `conn_pid`.

  Removes all tracked keys for this connection and clears its registration.

  ## Returns

  `{:ok, updated_config}` with tracking disabled.
  """
  @spec disable(pid(), tracking_config()) :: {:ok, tracking_config()}
  def disable(conn_pid, config) do
    :ets.match_delete(@tracking_table, {:_, conn_pid})
    :ets.delete(@connections_table, conn_pid)

    {:ok, %{config | enabled: false, prefixes: [], redirect: nil}}
  end

  @doc """
  Cleans up all tracking state for a connection that is closing.

  Must be called when a connection process terminates to avoid stale entries
  in the tracking tables.
  """
  @spec cleanup(pid()) :: :ok
  def cleanup(conn_pid) do
    try do
      :ets.match_delete(@tracking_table, {:_, conn_pid})
      :ets.delete(@connections_table, conn_pid)
    rescue
      ArgumentError -> :ok
    end

    :ok
  end

  # ---------------------------------------------------------------------------
  # Key tracking (read path)
  # ---------------------------------------------------------------------------

  @doc """
  Records that `conn_pid` has read `key`, making it eligible for invalidation.

  In BCAST mode this is a no-op -- BCAST uses prefix matching, not per-key
  tracking. In OPTIN mode, the key is only tracked if `config.caching` is
  `true` (set by `CLIENT CACHING yes`). In OPTOUT mode, the key is tracked
  unless `config.caching` is `false` (set by `CLIENT CACHING no`).

  ## Returns

  The (possibly updated) tracking config. In OPTIN mode the `caching` flag
  is reset to `false` after tracking one command.
  """
  @spec track_key(pid(), binary(), tracking_config()) :: tracking_config()
  def track_key(_conn_pid, _key, %{enabled: false} = config), do: config

  def track_key(_conn_pid, _key, %{mode: :bcast} = config), do: config

  def track_key(conn_pid, key, %{optin: true, caching: true} = config) do
    :ets.insert(@tracking_table, {key, conn_pid})
    # Reset caching flag after one tracked command
    %{config | caching: false}
  end

  def track_key(_conn_pid, _key, %{optin: true, caching: false} = config), do: config

  def track_key(_conn_pid, _key, %{optout: true, caching: false} = config) do
    # Skip tracking for this command, reset to tracking by default
    %{config | caching: true}
  end

  def track_key(conn_pid, key, config) do
    :ets.insert(@tracking_table, {key, conn_pid})
    config
  end

  @doc """
  Records that `conn_pid` has read multiple keys.

  Convenience wrapper over `track_key/3` for MGET and similar multi-key reads.
  """
  @spec track_keys(pid(), [binary()], tracking_config()) :: tracking_config()
  def track_keys(_conn_pid, _keys, %{enabled: false} = config), do: config
  def track_keys(_conn_pid, _keys, %{mode: :bcast} = config), do: config

  def track_keys(conn_pid, keys, %{optin: true, caching: true} = config) do
    insert_tracked_keys(conn_pid, keys)
    %{config | caching: false}
  end

  def track_keys(_conn_pid, _keys, %{optin: true, caching: false} = config), do: config

  def track_keys(_conn_pid, _keys, %{optout: true, caching: false} = config),
    do: %{config | caching: true}

  def track_keys(conn_pid, keys, config) do
    insert_tracked_keys(conn_pid, keys)
    config
  end

  defp insert_tracked_keys(_conn_pid, []), do: :ok

  defp insert_tracked_keys(conn_pid, keys) do
    :ets.insert(@tracking_table, Enum.map(keys, fn key -> {key, conn_pid} end))
  end

  # ---------------------------------------------------------------------------
  # Invalidation (write path)
  # ---------------------------------------------------------------------------

  @doc """
  Notifies all tracking connections that `key` has been modified.

  For default-mode connections, looks up all pids tracking this exact key
  in the tracking table and sends them an invalidation push message.

  For BCAST-mode connections, iterates over all registered connections with
  BCAST enabled and checks if the key matches any of their registered
  prefixes.

  The `writer_pid` is the connection that performed the write. If a tracked
  connection has NOLOOP enabled and its pid matches `writer_pid`, the
  invalidation is skipped for that connection.

  ## Parameters

    * `key` -- the key that was modified
    * `writer_pid` -- the pid of the connection that performed the write
    * `socket_sender` -- a function `(pid, iodata, keys) -> :ok` that sends
      raw bytes to a connection. This indirection allows the caller to provide
      the appropriate transport send mechanism.

  ## Returns

  `:ok` always.
  """
  @spec notify_key_modified(binary(), pid(), (pid(), iodata(), [binary()] -> :ok)) :: :ok
  def notify_key_modified(key, writer_pid, socket_sender) do
    notify_keys_modified([key], writer_pid, socket_sender)
  end

  @doc """
  Notifies tracking connections about multiple modified keys.

  Convenience wrapper for bulk writes (MSET, etc.).
  """
  @spec notify_keys_modified([binary()], pid(), (pid(), iodata(), [binary()] -> :ok)) :: :ok
  def notify_keys_modified([], _writer_pid, _socket_sender), do: :ok

  def notify_keys_modified(keys, writer_pid, socket_sender) do
    keys = Enum.uniq(keys)

    notify_default_mode_many(keys, writer_pid, socket_sender)
    notify_bcast_mode_many(keys, writer_pid, socket_sender)

    :ok
  end

  # ---------------------------------------------------------------------------
  # CLIENT CACHING command
  # ---------------------------------------------------------------------------

  @doc """
  Handles `CLIENT CACHING yes|no` for OPTIN/OPTOUT modes.

  In OPTIN mode, `CLIENT CACHING yes` enables tracking for the next command.
  In OPTOUT mode, `CLIENT CACHING no` disables tracking for the next command.

  ## Returns

  `{:ok, updated_config}` on success, `{:error, reason}` on failure.
  """
  @spec set_caching(tracking_config(), boolean()) ::
          {:ok, tracking_config()} | {:error, binary()}
  def set_caching(%{enabled: false}, _value) do
    {:error, "ERR CLIENT CACHING can be called only after CLIENT TRACKING is enabled"}
  end

  def set_caching(%{optin: false, optout: false}, _value) do
    {:error, "ERR CLIENT CACHING can be called only when OPTIN or OPTOUT mode is active"}
  end

  def set_caching(config, value) when is_boolean(value) do
    {:ok, %{config | caching: value}}
  end

  # ---------------------------------------------------------------------------
  # Tracking info
  # ---------------------------------------------------------------------------

  @doc """
  Returns a map describing the current tracking state for a connection.

  Used by `CLIENT TRACKINGINFO` to report the connection's configuration.
  """
  @spec tracking_info(tracking_config()) :: map()
  def tracking_info(config) do
    flags =
      []
      |> maybe_add_flag(config.optin, "optin")
      |> maybe_add_flag(config.optout, "optout")
      |> maybe_add_flag(config.noloop, "noloop")
      |> then(fn flags -> if flags == [], do: ["off"], else: flags end)

    %{
      "flags" => flags,
      "redirect" => if(config.redirect, do: :erlang.phash2(config.redirect), else: -1),
      "prefixes" => config.prefixes
    }
  end

  @doc """
  Returns the redirect target ID for a connection, or `0` if no redirect is set.

  Used by `CLIENT GETREDIR`.
  """
  @spec get_redirect(tracking_config()) :: integer()
  def get_redirect(%{redirect: nil}), do: 0
  def get_redirect(%{redirect: pid}), do: :erlang.phash2(pid)

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp notify_default_mode_many(keys, writer_pid, socket_sender) do
    keys
    |> Enum.reduce(%{}, fn key, target_keys ->
      tracked_pids = :ets.lookup(@tracking_table, key)

      target_keys =
        Enum.reduce(tracked_pids, target_keys, fn {^key, tracked_pid}, acc ->
          if should_send_invalidation?(tracked_pid, writer_pid) do
            target = resolve_target(tracked_pid)
            Map.update(acc, target, [key], &[key | &1])
          else
            acc
          end
        end)

      # Remove tracked entries even when NOLOOP suppresses the push. Redis
      # invalidations are one-shot; the next read re-registers interest.
      :ets.delete(@tracking_table, key)
      target_keys
    end)
    |> send_grouped_invalidations(socket_sender)
  rescue
    ArgumentError -> :ok
  end

  defp notify_bcast_mode_many(keys, writer_pid, socket_sender) do
    # Use :ets.select with a match spec to filter only BCAST-enabled connections
    # directly in ETS, avoiding copying the entire connections table into a list.
    match_spec = [
      {{:"$1", :"$2"},
       [
         {:andalso, {:is_map, :"$2"},
          {:andalso, {:==, {:map_get, :enabled, :"$2"}, true},
           {:==, {:map_get, :mode, :"$2"}, :bcast}}}
       ], [{{:"$1", :"$2"}}]}
    ]

    bcast_connections = :ets.select(@connections_table, match_spec)

    Enum.each(bcast_connections, fn {conn_pid, config} ->
      if should_send_invalidation_config?(conn_pid, writer_pid, config) do
        matching_keys = Enum.filter(keys, &prefix_matches?(&1, config.prefixes))

        if matching_keys != [] do
          target = resolve_target_config(conn_pid, config)
          safe_send(target, encode_invalidation(matching_keys), matching_keys, socket_sender)
        end
      end
    end)
  rescue
    ArgumentError -> :ok
  end

  defp send_grouped_invalidations(target_keys, socket_sender) do
    Enum.each(target_keys, fn {target, reversed_keys} ->
      keys = Enum.reverse(reversed_keys)
      safe_send(target, encode_invalidation(keys), keys, socket_sender)
    end)
  end

  defp should_send_invalidation?(tracked_pid, writer_pid) when tracked_pid == writer_pid do
    # Check NOLOOP
    case :ets.lookup(@connections_table, tracked_pid) do
      [{^tracked_pid, config}] -> not config.noloop
      [] -> true
    end
  rescue
    ArgumentError -> true
  end

  defp should_send_invalidation?(_tracked_pid, _writer_pid), do: true

  defp should_send_invalidation_config?(conn_pid, writer_pid, config)
       when conn_pid == writer_pid do
    not config.noloop
  end

  defp should_send_invalidation_config?(_conn_pid, _writer_pid, _config), do: true

  defp resolve_target(conn_pid) do
    case :ets.lookup(@connections_table, conn_pid) do
      [{^conn_pid, %{redirect: nil}}] -> conn_pid
      [{^conn_pid, %{redirect: redirect_pid}}] -> redirect_pid
      [] -> conn_pid
    end
  rescue
    ArgumentError -> conn_pid
  end

  defp resolve_target_config(conn_pid, %{redirect: nil}), do: conn_pid
  defp resolve_target_config(_conn_pid, %{redirect: redirect_pid}), do: redirect_pid

  defp prefix_matches?(_key, []), do: true

  defp prefix_matches?(key, prefixes) do
    Enum.any?(prefixes, fn prefix -> String.starts_with?(key, prefix) end)
  end

  # Sends the invalidation message (iodata) alongside the pre-extracted key
  # list to the target connection process.
  defp safe_send(target_pid, message, keys, socket_sender) do
    if Process.alive?(target_pid) do
      try do
        socket_sender.(target_pid, message, keys)
      rescue
        _ -> :ok
      end
    end
  end

  @doc """
  Encodes an invalidation message as RESP3 push iodata.

  The format is:

      >2\\r\\n+invalidate\\r\\n*N\\r\\n$len\\r\\nkey\\r\\n...

  Where N is the number of invalidated keys.
  """
  @spec encode_invalidation([binary()]) :: iodata()
  def encode_invalidation(keys) do
    Encoder.encode({:push, [{:simple, "invalidate"}, keys]})
  end

  defp maybe_add_flag(flags, true, flag), do: [flag | flags]
  defp maybe_add_flag(flags, false, _flag), do: flags
end
