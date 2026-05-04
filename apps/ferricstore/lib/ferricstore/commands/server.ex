# Suppress function clause grouping warnings (clauses added by different agents)
defmodule Ferricstore.Commands.Server do
  alias Ferricstore.HLC
  alias Ferricstore.Store.Ops

  @moduledoc """
  Handles Redis server commands: PING, ECHO, DBSIZE, KEYS, FLUSHDB, FLUSHALL,
  INFO, COMMAND, SELECT, LOLWUT, and DEBUG.

  Each handler takes the uppercased command name, a list of string arguments,
  and an injected store map. Returns plain Elixir terms — the connection layer
  handles RESP encoding.

  ## Supported commands

    * `PING [message]` — returns `{:simple, "PONG"}` or echoes the message
    * `ECHO message` — returns the message as a bulk string
    * `DBSIZE` — returns the number of keys in the store
    * `KEYS pattern` — returns keys matching a glob pattern (`*`, `?`)
    * `FLUSHDB [ASYNC|SYNC]` — deletes all keys
    * `FLUSHALL [ASYNC|SYNC]` — alias for FLUSHDB (single-db server)
    * `INFO [section]` — returns server information as a bulk string
    * `COMMAND` — returns array of command info tuples
    * `COMMAND COUNT` — returns number of supported commands
    * `COMMAND DOCS name` — returns simplified docs for a command
    * `COMMAND INFO name [name ...]` — returns info for specific commands
    * `COMMAND LIST` — returns all command names
    * `COMMAND GETKEYS command [args...]` — returns which args are keys
    * `SELECT db` — always returns error (not supported)
    * `LOLWUT [VERSION version]` — returns ASCII art with FerricStore branding
    * `DEBUG SLEEP seconds` — sleeps for N seconds (testing only)
  """

  alias Ferricstore.AuditLog
  alias Ferricstore.Commands.Catalog
  alias Ferricstore.Stats

  @doc """
  Handles a server command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"PING"`, `"KEYS"`)
    - `args` - List of string arguments
    - `store` - Injected store map with `keys`, `dbsize`, `flush` callbacks

  ## Returns

  Plain Elixir term: `{:simple, "PONG"}`, string, integer, list, `:ok`, or `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # PING
  # ---------------------------------------------------------------------------

  def handle("PING", [], _store), do: {:simple, "PONG"}
  def handle("PING", [msg], _store), do: msg

  def handle("PING", _args, _store) do
    {:error, "ERR wrong number of arguments for 'ping' command"}
  end

  # ---------------------------------------------------------------------------
  # ECHO
  # ---------------------------------------------------------------------------

  def handle("ECHO", [msg], _store), do: msg

  def handle("ECHO", _args, _store) do
    {:error, "ERR wrong number of arguments for 'echo' command"}
  end

  # ---------------------------------------------------------------------------
  # DBSIZE
  # ---------------------------------------------------------------------------

  def handle("DBSIZE", [], store) do
    alias Ferricstore.Store.CompoundKey

    Ops.keys(store)
    |> CompoundKey.user_visible_keys()
    |> length()
  end

  def handle("DBSIZE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'dbsize' command"}
  end

  # ---------------------------------------------------------------------------
  # KEYS
  # ---------------------------------------------------------------------------

  def handle("KEYS", [pattern], store) do
    alias Ferricstore.Store.CompoundKey

    Ops.keys(store)
    |> CompoundKey.user_visible_keys()
    |> Enum.filter(&Ferricstore.GlobMatcher.match?(&1, pattern))
  end

  def handle("KEYS", [], _store) do
    {:error, "ERR wrong number of arguments for 'keys' command"}
  end

  def handle("KEYS", _args, _store) do
    {:error, "ERR syntax error"}
  end

  # ---------------------------------------------------------------------------
  # FLUSHDB
  # ---------------------------------------------------------------------------

  def handle("FLUSHDB", args, store) when args in [[], ["ASYNC"], ["SYNC"]] do
    AuditLog.log(:dangerous_command, %{command: "FLUSHDB", args: args})
    Ops.flush(store)
    # Wipe prob files (bloom, CMS, cuckoo, TopK) across all shards.
    # store.flush deletes keys via Raft which should clean up files via
    # maybe_delete_prob_file, but as a safety net we also wipe the prob
    # directories directly.
    flush_all_prob_dirs()
  end

  def handle("FLUSHDB", _args, _store) do
    {:error, "ERR syntax error"}
  end

  # ---------------------------------------------------------------------------
  # FLUSHALL — alias for FLUSHDB in our single-database server
  # ---------------------------------------------------------------------------

  def handle("FLUSHALL", args, store) when args in [[], ["ASYNC"], ["SYNC"]] do
    AuditLog.log(:dangerous_command, %{command: "FLUSHALL", args: args})
    Ops.flush(store)
    flush_all_prob_dirs()
  end

  def handle("FLUSHALL", _args, _store) do
    {:error, "ERR syntax error"}
  end

  # ---------------------------------------------------------------------------
  # SELECT — not supported
  # ---------------------------------------------------------------------------

  def handle("SELECT", [_db], _store) do
    {:error, "ERR SELECT not supported. Use named caches."}
  end

  def handle("SELECT", _args, _store) do
    {:error, "ERR wrong number of arguments for 'select' command"}
  end

  # ---------------------------------------------------------------------------
  # INFO [section]
  # ---------------------------------------------------------------------------

  def handle("INFO", [], store), do: handle("INFO", ["all"], store)

  def handle("INFO", [section], store) do
    section_lower = String.downcase(section)
    info_string(section_lower, store)
  end

  def handle("INFO", _args, _store) do
    {:error, "ERR syntax error"}
  end

  # ---------------------------------------------------------------------------
  # COMMAND (no subcommand) — return all command info tuples
  # ---------------------------------------------------------------------------

  def handle("COMMAND", [], _store) do
    Catalog.all() |> Enum.map(&Catalog.info_tuple/1)
  end

  # ---------------------------------------------------------------------------
  # COMMAND subcommands
  # ---------------------------------------------------------------------------

  def handle("COMMAND", ["COUNT"], _store), do: Catalog.count()

  def handle("COMMAND", ["LIST"], _store), do: Catalog.names()

  def handle("COMMAND", ["INFO" | names], _store) when names != [] do
    Enum.map(names, fn name ->
      case Catalog.lookup(name) do
        {:ok, cmd} -> Catalog.info_tuple(cmd)
        :error -> nil
      end
    end)
  end

  def handle("COMMAND", ["INFO"], _store) do
    {:error, "ERR wrong number of arguments for 'command|info' command"}
  end

  def handle("COMMAND", ["DOCS" | names], _store) when names != [] do
    result =
      Enum.flat_map(names, fn name ->
        case Catalog.lookup(name) do
          {:ok, cmd} ->
            [cmd.name, [cmd.summary]]

          :error ->
            []
        end
      end)

    result
  end

  def handle("COMMAND", ["DOCS"], _store) do
    {:error, "ERR wrong number of arguments for 'command|docs' command"}
  end

  def handle("COMMAND", ["GETKEYS", cmd_name | cmd_args], _store) do
    case Catalog.get_keys(cmd_name, cmd_args) do
      {:ok, keys} -> keys
      {:error, msg} -> {:error, msg}
    end
  end

  def handle("COMMAND", ["GETKEYS"], _store) do
    {:error, "ERR wrong number of arguments for 'command|getkeys' command"}
  end

  def handle("COMMAND", [subcmd | _rest], _store) do
    {:error, "ERR unknown subcommand '#{subcmd}'. Try COMMAND HELP."}
  end

  # ---------------------------------------------------------------------------
  # LOLWUT [VERSION version]
  # ---------------------------------------------------------------------------

  def handle("LOLWUT", args, _store) when args in [[], ["VERSION", "1"]] do
    art = """
     _____              _      ____  _
    |  ___|__ _ __ _ __(_) ___/ ___|| |_ ___  _ __ ___
    | |_ / _ \\ '__| '__| |/ __\\___ \\| __/ _ \\| '__/ _ \\
    |  _|  __/ |  | |  | | (__ ___) | || (_) | | |  __/
    |_|  \\___|_|  |_|  |_|\\___|____/ \\__\\___/|_|  \\___|
                                          v0.1.0
    """

    String.trim_trailing(art)
  end

  def handle("LOLWUT", ["VERSION", _version], _store) do
    handle("LOLWUT", [], nil)
  end

  def handle("LOLWUT", _args, _store) do
    {:error, "ERR syntax error"}
  end

  # ---------------------------------------------------------------------------
  # DEBUG SLEEP seconds
  # ---------------------------------------------------------------------------

  def handle("DEBUG", ["SLEEP", seconds_str], _store) do
    AuditLog.log(:dangerous_command, %{command: "DEBUG", args: ["SLEEP", seconds_str]})

    case Integer.parse(seconds_str) do
      {secs, ""} when secs >= 0 ->
        Process.sleep(secs * 1000)
        :ok

      _ ->
        {:error, "ERR invalid argument for DEBUG SLEEP"}
    end
  end

  def handle("DEBUG", ["SLEEP"], _store) do
    {:error, "ERR wrong number of arguments for 'debug' command"}
  end

  def handle("DEBUG", ["RELOAD"], _store), do: :ok

  def handle("DEBUG", ["FLUSHALL" | _], store) do
    AuditLog.log(:dangerous_command, %{command: "DEBUG", args: ["FLUSHALL"]})
    handle("FLUSHALL", [], store)
  end

  def handle("DEBUG", ["SET-DURABILITY", mode], _store) when mode in ["quorum", "async"] do
    atom_mode = if mode == "quorum", do: :all_quorum, else: :all_async
    FerricStore.Instance.update_durability_mode(:default, atom_mode)
    {:simple, "OK durability_mode=#{atom_mode}"}
  end

  def handle("DEBUG", ["BATCHER-STATS"], _store) do
    ctx = FerricStore.Instance.get(:default)
    shard_count = ctx.shard_count

    batcher_parts =
      for i <- 0..(shard_count - 1) do
        name = :"Ferricstore.Raft.Batcher.#{i}"

        case Process.whereis(name) do
          nil ->
            "B#{i}=down"

          pid ->
            info = Process.info(pid, [:message_queue_len, :reductions])
            "B#{i}:mq=#{info[:message_queue_len]},r=#{info[:reductions]}"
        end
      end

    wal_name = :ra_ferricstore_raft_log_wal

    wal_part =
      case Process.whereis(wal_name) do
        nil ->
          "WAL=down"

        pid ->
          info = Process.info(pid, [:message_queue_len, :reductions])
          "WAL:mq=#{info[:message_queue_len]},r=#{info[:reductions]}"
      end

    ra_parts =
      for i <- 0..(shard_count - 1) do
        name = :"ferricstore_shard_#{i}"

        case Process.whereis(name) do
          nil ->
            "R#{i}=down"

          pid ->
            info = Process.info(pid, [:message_queue_len, :reductions])
            "R#{i}:mq=#{info[:message_queue_len]},r=#{info[:reductions]}"
        end
      end

    all = batcher_parts ++ [wal_part] ++ ra_parts
    {:simple, Enum.join(all, " | ")}
  end

  def handle("DEBUG", ["SET-ACTIVE-EXPIRE", _flag], _store), do: :ok
  def handle("DEBUG", ["CHANGE-REPL-ID"], _store), do: :ok
  def handle("DEBUG", ["QUICKLIST-PACKED-THRESHOLD" | _], _store), do: :ok
  def handle("DEBUG", ["AOFSTAT"], _store), do: %{}
  def handle("DEBUG", ["SFLAGS"], _store), do: %{}

  def handle("DEBUG", [subcmd | _rest], _store) do
    {:error, "ERR unknown subcommand '#{subcmd}'. Try DEBUG HELP."}
  end

  def handle("DEBUG", [], _store) do
    {:error, "ERR wrong number of arguments for 'debug' command"}
  end

  # ---------------------------------------------------------------------------
  # CONFIG
  # ---------------------------------------------------------------------------

  def handle("CONFIG", [subcmd | rest], _store) do
    handle_config(String.upcase(subcmd), upcase_local_modifier(rest))
  end

  def handle("CONFIG", [], _store) do
    {:error, "ERR wrong number of arguments for 'config' command"}
  end

  # ---------------------------------------------------------------------------
  # MODULE stubs
  # ---------------------------------------------------------------------------

  def handle("MODULE", ["LIST" | _], _store), do: []

  def handle("MODULE", ["LOAD" | _], _store),
    do: {:error, "ERR FerricStore does not support modules"}

  def handle("MODULE", ["UNLOAD" | _], _store),
    do: {:error, "ERR FerricStore does not support modules"}

  def handle("MODULE", _, _store), do: {:error, "ERR unknown subcommand for 'module' command"}

  # ---------------------------------------------------------------------------
  # WAITAOF stub
  # ---------------------------------------------------------------------------

  def handle("WAITAOF", [_, _, _], _store), do: [0, 0]

  def handle("WAITAOF", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'waitaof' command"}

  # ---------------------------------------------------------------------------
  # SLOWLOG
  # ---------------------------------------------------------------------------

  def handle("SLOWLOG", ["GET"], _store), do: format_slowlog_entries(Ferricstore.SlowLog.get())

  def handle("SLOWLOG", ["GET", count_str], _store) do
    case Integer.parse(count_str) do
      {count, ""} when count >= 0 ->
        format_slowlog_entries(Ferricstore.SlowLog.get(count))

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("SLOWLOG", ["LEN"], _store), do: Ferricstore.SlowLog.len()

  def handle("SLOWLOG", ["RESET"], _store) do
    Ferricstore.SlowLog.reset()
    :ok
  end

  def handle("SLOWLOG", ["HELP"], _store) do
    [
      "SLOWLOG GET [<count>] -- Return top entries from the slowlog.",
      "SLOWLOG LEN -- Return the number of entries in the slowlog.",
      "SLOWLOG RESET -- Reset the slowlog."
    ]
  end

  def handle("SLOWLOG", _args, _store) do
    {:error, "ERR unknown subcommand or wrong number of arguments for 'slowlog' command"}
  end

  # ---------------------------------------------------------------------------
  # SAVE / BGSAVE / LASTSAVE
  # ---------------------------------------------------------------------------

  def handle("SAVE", _args, _store), do: :ok
  def handle("BGSAVE", _args, _store), do: {:simple, "Background saving started"}
  def handle("LASTSAVE", _args, _store), do: System.os_time(:second)

  # ===========================================================================
  # Private helpers
  # ===========================================================================

  # ---------------------------------------------------------------------------
  # FLUSHDB helper
  # ---------------------------------------------------------------------------

  defp flush_all_prob_dirs do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    shard_count = shard_count()

    Enum.reduce_while(0..(shard_count - 1), :ok, fn i, :ok ->
      shard_path = Ferricstore.DataDir.shard_data_path(data_dir, i)
      prob_dir = Path.join(shard_path, "prob")

      case clear_prob_dir(prob_dir) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  rescue
    _ -> :ok
  end

  defp clear_prob_dir(prob_dir) do
    with true <- Ferricstore.FS.exists?(prob_dir),
         {:ok, files} <- Ferricstore.FS.ls(prob_dir) do
      with :ok <- delete_prob_files(prob_dir, files),
           :ok <- prob_fsync_dir(prob_dir, :flush_prob_dir) do
        :ok
      end
    else
      _ -> :ok
    end
  end

  defp delete_prob_files(prob_dir, files) do
    Enum.reduce_while(files, :ok, fn file, :ok ->
      case Ferricstore.FS.rm(Path.join(prob_dir, file)) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:delete_prob_file_failed, file, reason}}}
      end
    end)
  end

  defp prob_fsync_dir(path, phase) do
    case prob_fsync_dir_result(path) do
      # Fsync the prob dir so the removals are durable — without this
      # a crash after FLUSHDB can resurrect probabilistic filter files
      # that were supposed to be wiped.
      :ok -> :ok
      {:error, reason} -> {:error, {:fsync_dir_failed, phase, reason}}
    end
  end

  defp prob_fsync_dir_result(path) do
    case Process.get(:ferricstore_prob_command_fsync_dir_hook) do
      fun when is_function(fun, 1) -> fun.(path)
      _ -> Ferricstore.Bitcask.NIF.v2_fsync_dir(path)
    end
  end

  # ---------------------------------------------------------------------------
  # INFO section builders
  # ---------------------------------------------------------------------------

  @all_sections [
    "server",
    "clients",
    "memory",
    "keyspace",
    "stats",
    "persistence",
    "replication",
    "cpu",
    "namespace_config",
    "raft",
    "bitcask",
    "ferricstore",
    "keydir_analysis"
  ]

  # Read shard_count from persistent_term (set by application.ex) with
  # Application.get_env fallback for early startup / test environments.
  defp shard_count do
    try do
      FerricStore.Instance.get(:default).shard_count
    rescue
      ArgumentError ->
        Application.get_env(:ferricstore, :shard_count, 4)
    end
  end

  defp info_string(section, store) when section in ["all", "everything"] do
    Enum.map_join(@all_sections, "\r\n", fn s -> build_section(s, store) end)
  end

  defp info_string(section, store) when section in @all_sections do
    build_section(section, store)
  end

  defp info_string(_unknown, _store) do
    # Redis returns an empty string for unknown sections
    ""
  end

  defp build_section("server", _store) do
    ctx = FerricStore.Instance.get(:default)
    info = if ctx.server_info_fn, do: ctx.server_info_fn.(), else: %{}
    port = Map.get(info, :tcp_port, 0)
    redis_mode = Map.get(info, :redis_mode, "embedded")

    uptime_seconds = Stats.uptime_seconds()
    uptime_days = div(uptime_seconds, 86_400)

    {os_family, os_name} = :os.type()
    {major, minor, patch} = :os.version()
    os_string = "#{os_family}:#{os_name} #{major}.#{minor}.#{patch}"

    fields = [
      {"redis_version", "7.4.0"},
      {"ferricstore_version", "0.3.5"},
      {"redis_mode", redis_mode},
      {"os", os_string},
      {"arch_bits", "64"},
      {"tcp_port", Integer.to_string(port)},
      {"uptime_in_seconds", Integer.to_string(uptime_seconds)},
      {"uptime_in_days", Integer.to_string(uptime_days)},
      {"hz", "10"},
      {"configured_hz", "10"},
      {"process_id", Integer.to_string(System.pid() |> String.to_integer())},
      {"run_id", Stats.run_id()},
      {"ferricstore_git_sha", "dev"}
    ]

    format_section("Server", fields)
  end

  defp build_section("clients", _store) do
    ctx = FerricStore.Instance.get(:default)
    connected = if ctx.connected_clients_fn, do: ctx.connected_clients_fn.(), else: 0

    blocked = safe_ets_size(:ferricstore_waiters)

    tracking =
      safe_ets_size(:ferricstore_tracking_connections)

    fields = [
      {"connected_clients", Integer.to_string(connected)},
      {"blocked_clients", Integer.to_string(blocked)},
      {"tracking_clients", Integer.to_string(tracking)},
      {"maxclients", "10000"}
    ]

    format_section("Clients", fields)
  end

  defp build_section("memory", _store) do
    total = :erlang.memory(:total)
    process_mem = :erlang.memory(:processes)
    shard_count = shard_count()

    # Sum ETS memory across keydir tables per shard.
    keydir_bytes =
      Enum.reduce(0..(shard_count - 1), 0, fn i, acc ->
        try do
          case :ets.info(:"keydir_#{i}", :memory) do
            words when is_integer(words) ->
              acc + words * :erlang.system_info(:wordsize)

            _ ->
              acc
          end
        rescue
          ArgumentError -> acc
        end
      end)

    # RSS approximation: best we can do on BEAM is :erlang.memory(:total)
    used_memory_rss = total

    # Peak: we do not track a high-water mark yet, so report current.
    used_memory_peak = total

    # Fragmentation ratio (rss / used). With BEAM they are the same, so ~1.0.
    frag_ratio =
      if total > 0,
        do: Float.round(used_memory_rss / total, 2),
        else: 1.0

    fields = [
      {"used_memory", Integer.to_string(total)},
      {"used_memory_human", format_bytes(total)},
      {"used_memory_rss", Integer.to_string(used_memory_rss)},
      {"used_memory_peak", Integer.to_string(used_memory_peak)},
      {"mem_fragmentation_ratio", format_float_field(frag_ratio)},
      {"keydir_used_bytes", Integer.to_string(keydir_bytes)},
      {"hot_cache_used_bytes", Integer.to_string(keydir_bytes)},
      {"beam_process_memory", Integer.to_string(process_mem)}
    ]

    format_section("Memory", fields)
  end

  defp build_section("keyspace", store) do
    key_count = Ops.dbsize(store)

    ctx =
      try do
        FerricStore.Instance.get(:default)
      rescue
        _ -> nil
      end

    {expires, avg_ttl} = if ctx, do: compute_expiry_stats(ctx), else: {0, 0}

    fields = [
      {"db0", "keys=#{key_count},expires=#{expires},avg_ttl=#{avg_ttl}"}
    ]

    format_section("Keyspace", fields)
  end

  defp build_section("stats", _store) do
    rate =
      try do
        FerricStore.Instance.get(:default).read_sample_rate
      rescue
        ArgumentError -> 100
      end

    hot_sampled = Stats.total_hot_reads(FerricStore.Instance.get(:default))
    cold_sampled = Stats.total_cold_reads(FerricStore.Instance.get(:default))
    hits_sampled = Stats.keyspace_hits(FerricStore.Instance.get(:default))
    misses = Stats.keyspace_misses(FerricStore.Instance.get(:default))

    # Estimated actuals: sampled counters × sample rate
    hot_est = hot_sampled * rate
    cold_est = cold_sampled * rate
    hits_est = hits_sampled * rate
    total_reads = hits_est + misses
    hit_ratio = if total_reads > 0, do: Float.round(hits_est / total_reads * 100, 2), else: 0.0

    hot_pct =
      if hot_est + cold_est > 0,
        do: Float.round(hot_est / (hot_est + cold_est) * 100, 2),
        else: 0.0

    fields = [
      {"total_connections_received", Integer.to_string(Stats.total_connections())},
      {"total_commands_processed", Integer.to_string(Stats.total_commands())},
      {"keyspace_hits", Integer.to_string(hits_est)},
      {"keyspace_misses", Integer.to_string(misses)},
      {"keyspace_hit_ratio", format_float_field(hit_ratio)},
      {"hot_reads", Integer.to_string(hot_est)},
      {"cold_reads", Integer.to_string(cold_est)},
      {"hot_cache_hit_ratio", format_float_field(hot_pct)},
      {"read_sample_rate", "1:#{rate}"},
      {"expired_keys", Integer.to_string(Stats.expired_keys(FerricStore.Instance.get(:default)))},
      {"evicted_keys", Integer.to_string(Stats.evicted_keys(FerricStore.Instance.get(:default)))}
    ]

    format_section("Stats", fields)
  end

  defp build_section("persistence", _store) do
    fields = [
      {"loading", "0"},
      {"rdb_changes_since_last_save", "0"},
      {"rdb_last_save_time", "0"}
    ]

    format_section("Persistence", fields)
  end

  defp build_section("replication", _store) do
    fields = [
      {"role", "master"},
      {"connected_slaves", "0"}
    ]

    format_section("Replication", fields)
  end

  defp build_section("cpu", _store) do
    # Stub: BEAM does not expose per-process CPU counters cheaply.
    fields = [
      {"used_cpu_sys", "0.000000"},
      {"used_cpu_user", "0.000000"}
    ]

    format_section("CPU", fields)
  end

  defp build_section("namespace_config", _store) do
    alias Ferricstore.NamespaceConfig

    entries = NamespaceConfig.get_all()
    count = length(entries)
    all_default = if count == 0, do: "1", else: "0"

    default_fields = [
      {"namespace_config_count", Integer.to_string(count)},
      {"namespace_config_all_default", all_default},
      {"default_window_ms", Integer.to_string(NamespaceConfig.default_window_ms())},
      {"default_durability", Atom.to_string(NamespaceConfig.default_durability())}
    ]

    entry_fields =
      Enum.flat_map(entries, fn entry ->
        %{prefix: prefix, window_ms: w, durability: d} = entry
        changed_at = Map.get(entry, :changed_at, 0)
        changed_by = Map.get(entry, :changed_by, "")

        [
          {"ns_#{prefix}_window_ms", Integer.to_string(w)},
          {"ns_#{prefix}_durability", Atom.to_string(d)},
          {"ns_#{prefix}_changed_at", Integer.to_string(changed_at)},
          {"ns_#{prefix}_changed_by", changed_by}
        ]
      end)

    format_section("Namespace_Config", default_fields ++ entry_fields)
  end

  # ---------------------------------------------------------------------------
  # INFO raft -- per-shard Raft state
  # ---------------------------------------------------------------------------

  defp build_section("raft", _store) do
    shard_count = shard_count()

    fields =
      Enum.flat_map(0..(shard_count - 1), fn i ->
        shard_id = {:"ferricstore_shard_#{i}", node()}

        try do
          case :ra.members(shard_id) do
            {:ok, _members, leader} ->
              # Get counters from ra_counters
              counters =
                try do
                  :ra_counters.overview(shard_id)
                rescue
                  _ -> %{}
                catch
                  _, _ -> %{}
                end

              role =
                if leader == shard_id do
                  "leader"
                else
                  "follower"
                end

              leader_node_str =
                case leader do
                  {_name, node_name} -> Atom.to_string(node_name)
                  _ -> "unknown"
                end

              commit_index = Map.get(counters, :commit_index, 0)
              last_applied = Map.get(counters, :last_applied, 0)
              current_term = Map.get(counters, :term, 0)

              [
                {"shard_#{i}_role", role},
                {"shard_#{i}_current_term", Integer.to_string(current_term)},
                {"shard_#{i}_commit_index", Integer.to_string(commit_index)},
                {"shard_#{i}_last_applied", Integer.to_string(last_applied)},
                {"shard_#{i}_leader_node", leader_node_str}
              ]

            _ ->
              [
                {"shard_#{i}_role", "unknown"},
                {"shard_#{i}_current_term", "0"},
                {"shard_#{i}_commit_index", "0"},
                {"shard_#{i}_last_applied", "0"},
                {"shard_#{i}_leader_node", "unknown"}
              ]
          end
        rescue
          _ ->
            [
              {"shard_#{i}_role", "unknown"},
              {"shard_#{i}_current_term", "0"},
              {"shard_#{i}_commit_index", "0"},
              {"shard_#{i}_last_applied", "0"},
              {"shard_#{i}_leader_node", "unknown"}
            ]
        catch
          _, _ ->
            [
              {"shard_#{i}_role", "unknown"},
              {"shard_#{i}_current_term", "0"},
              {"shard_#{i}_commit_index", "0"},
              {"shard_#{i}_last_applied", "0"},
              {"shard_#{i}_leader_node", "unknown"}
            ]
        end
      end)

    format_section("Raft", fields)
  end

  # ---------------------------------------------------------------------------
  # INFO bitcask -- per-shard storage stats
  # ---------------------------------------------------------------------------

  defp build_section("bitcask", _store) do
    shard_count = shard_count()
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    instance_ctx = default_instance_ctx()

    fields =
      Enum.flat_map(0..(shard_count - 1), fn i ->
        shard_dir = Ferricstore.DataDir.shard_data_path(data_dir, i)

        {data_files, hint_files, total_bytes} =
          try do
            case Ferricstore.FS.ls(shard_dir) do
              {:ok, files} ->
                data = Enum.filter(files, &String.ends_with?(&1, ".log"))
                hints = Enum.filter(files, &String.ends_with?(&1, ".hint"))

                total =
                  Enum.reduce(files, 0, fn f, acc ->
                    path = Path.join(shard_dir, f)

                    case File.stat(path) do
                      {:ok, %{size: size}} -> acc + size
                      _ -> acc
                    end
                  end)

                {length(data), length(hints), total}

              {:error, _} ->
                {0, 0, 0}
            end
          rescue
            _ -> {0, 0, 0}
          end

        merge_candidates = max(0, data_files - 1)
        last_applied = atomic_metric(instance_ctx, :last_applied_index, i)
        last_released = atomic_metric(instance_ctx, :last_released_cursor_index, i)
        release_gap = max(last_applied - last_released, 0)
        checkpoint_dirty = atomic_metric(instance_ctx, :checkpoint_flags, i)
        checkpoint_in_flight = atomic_metric(instance_ctx, :checkpoint_in_flight, i)

        [
          {"shard_#{i}_data_file_count", Integer.to_string(data_files)},
          {"shard_#{i}_hint_file_count", Integer.to_string(hint_files)},
          {"shard_#{i}_total_size_bytes", Integer.to_string(total_bytes)},
          {"shard_#{i}_merge_candidates", Integer.to_string(merge_candidates)},
          {"shard_#{i}_last_applied_index", Integer.to_string(last_applied)},
          {"shard_#{i}_last_released_cursor_index", Integer.to_string(last_released)},
          {"shard_#{i}_release_cursor_gap", Integer.to_string(release_gap)},
          {"shard_#{i}_checkpoint_dirty", Integer.to_string(checkpoint_dirty)},
          {"shard_#{i}_checkpoint_in_flight", Integer.to_string(checkpoint_in_flight)}
        ]
      end)

    format_section("Bitcask", fields)
  end

  # ---------------------------------------------------------------------------
  # INFO ferricstore -- aggregate native metrics
  # ---------------------------------------------------------------------------

  defp build_section("ferricstore", _store) do
    shard_count = shard_count()

    raft_committed =
      Enum.reduce(0..(shard_count - 1), 0, fn i, acc ->
        shard_id = {:"ferricstore_shard_#{i}", node()}

        try do
          counters = :ra_counters.overview(shard_id)
          acc + Map.get(counters, :last_applied, 0)
        rescue
          _ -> acc
        catch
          _, _ -> acc
        end
      end)

    hot_cache_evictions =
      try do
        :persistent_term.get({Ferricstore.Stats, :hot_cache_evictions}, 0)
      rescue
        _ -> 0
      catch
        _, _ -> 0
      end

    keydir_full_rejections =
      try do
        :persistent_term.get({Ferricstore.Stats, :keydir_full_rejections}, 0)
      rescue
        _ -> 0
      catch
        _, _ -> 0
      end

    fields = [
      {"raft_commands_committed", Integer.to_string(raft_committed)},
      {"hot_cache_evictions", Integer.to_string(hot_cache_evictions)},
      {"keydir_full_rejections", Integer.to_string(keydir_full_rejections)}
    ]

    format_section("Ferricstore", fields)
  end

  # ---------------------------------------------------------------------------
  # INFO keydir_analysis -- per-prefix keydir breakdown
  # ---------------------------------------------------------------------------

  defp build_section("keydir_analysis", _store) do
    shard_count = shard_count()

    # Collect all keys from all keydir ETS tables and group by prefix
    prefix_data =
      Enum.reduce(0..(shard_count - 1), %{}, fn i, acc ->
        table = :"keydir_#{i}"

        try do
          :ets.foldl(
            fn {key, _value, _exp, _lfu, _fid, _off, _vsize}, inner_acc ->
              prefix = Ferricstore.Stats.extract_prefix(key)
              # Estimate per-key bytes: key binary + value + expire_at + LFU + ETS tuple overhead
              key_bytes = byte_size(key) + 8 + 8 + 64

              current = Map.get(inner_acc, prefix, {0, 0})
              {count, bytes} = current
              Map.put(inner_acc, prefix, {count + 1, bytes + key_bytes})
            end,
            acc,
            table
          )
        rescue
          _ -> acc
        catch
          _, _ -> acc
        end
      end)

    distinct_prefixes = map_size(prefix_data)

    prefix_fields =
      prefix_data
      |> Enum.sort_by(fn {_prefix, {count, _bytes}} -> count end, :desc)
      |> Enum.flat_map(fn {prefix, {count, bytes}} ->
        [
          {"prefix_#{prefix}_key_count", Integer.to_string(count)},
          {"prefix_#{prefix}_keydir_bytes", Integer.to_string(bytes)}
        ]
      end)

    fields = [{"distinct_prefixes", Integer.to_string(distinct_prefixes)} | prefix_fields]

    format_section("Keydir_Analysis", fields)
  end

  defp default_instance_ctx do
    FerricStore.Instance.get(:default)
  rescue
    _ -> nil
  catch
    _, _ -> nil
  end

  defp atomic_metric(%FerricStore.Instance{} = ctx, field, shard_index) do
    case Map.get(ctx, field) do
      ref when is_reference(ref) ->
        size = :atomics.info(ref).size
        if shard_index < size, do: :atomics.get(ref, shard_index + 1), else: 0

      _ ->
        0
    end
  rescue
    _ -> 0
  end

  defp atomic_metric(_ctx, _field, _shard_index), do: 0

  # ---------------------------------------------------------------------------
  # Keyspace expiry stats helper
  # ---------------------------------------------------------------------------

  # Compute expires count and avg_ttl from ETS keydirs.
  # Uses :ets.select_count for expires (O(n) at C level, no term creation)
  # and samples up to 20 keys per shard for avg_ttl.
  defp compute_expiry_stats(ctx) do
    now = HLC.now_ms()
    count_spec = [{{:_, :_, :"$1", :_, :_, :_, :_}, [{:>, :"$1", 0}], [true]}]
    sample_spec = [{{:_, :_, :"$1", :_, :_, :_, :_}, [{:>, :"$1", 0}], [:"$1"]}]

    {total_expires, ttl_samples} =
      for i <- 0..(ctx.shard_count - 1), reduce: {0, []} do
        {exp_acc, ttl_acc} ->
          keydir = elem(ctx.keydir_refs, i)

          try do
            count = :ets.select_count(keydir, count_spec)

            samples =
              case :ets.select(keydir, sample_spec, 20) do
                {results, _cont} -> results
                :"$end_of_table" -> []
              end

            {exp_acc + count, samples ++ ttl_acc}
          rescue
            ArgumentError -> {exp_acc, ttl_acc}
          end
      end

    avg_ttl =
      case ttl_samples do
        [] ->
          0

        _ ->
          remaining = Enum.map(ttl_samples, fn exp -> max(0, exp - now) end)
          div(Enum.sum(remaining), length(remaining))
      end

    {total_expires, avg_ttl}
  end

  # ---------------------------------------------------------------------------
  # INFO formatting helpers
  # ---------------------------------------------------------------------------

  defp format_section(header, fields) do
    lines = Enum.map(fields, fn {k, v} -> [k, ":", v] end)

    ["# ", header, "\r\n", Enum.intersperse(lines, "\r\n"), "\r\n"]
    |> IO.iodata_to_binary()
  end

  defp safe_ets_size(table) do
    case :ets.info(table, :size) do
      :undefined -> 0
      n -> n
    end
  rescue
    ArgumentError -> 0
  end

  defp format_float_field(val) do
    :erlang.float_to_binary(val, [{:decimals, 2}])
  end

  defp format_bytes(bytes) when bytes < 1024, do: "#{bytes}B"

  defp format_bytes(bytes) when bytes < 1024 * 1024 do
    kb = Float.round(bytes / 1024, 2)
    "#{kb}K"
  end

  defp format_bytes(bytes) when bytes < 1024 * 1024 * 1024 do
    mb = Float.round(bytes / (1024 * 1024), 2)
    "#{mb}M"
  end

  defp format_bytes(bytes) do
    gb = Float.round(bytes / (1024 * 1024 * 1024), 2)
    "#{gb}G"
  end

  # ---------------------------------------------------------------------------
  # CONFIG helpers
  # ---------------------------------------------------------------------------

  defp handle_config("GET", ["LOCAL" | rest]) do
    handle_config_get_local(rest)
  end

  defp handle_config("GET", [pattern]) do
    Ferricstore.Config.get(pattern)
    |> Enum.flat_map(fn {k, v} -> [k, v] end)
  end

  defp handle_config("GET", _args),
    do: {:error, "ERR wrong number of arguments for 'config|get' command"}

  defp handle_config("SET", ["LOCAL" | rest]) do
    handle_config_set_local(rest)
  end

  defp handle_config("SET", [key, value]) do
    old_value = Ferricstore.Config.get_value(key)

    case Ferricstore.Config.set(key, value) do
      :ok ->
        AuditLog.log(:config_change, %{
          parameter: key,
          old_value: old_value || "",
          new_value: value
        })

        :ok

      {:error, _reason} = err ->
        err
    end
  end

  defp handle_config("SET", _args),
    do: {:error, "ERR wrong number of arguments for 'config|set' command"}

  defp handle_config("RESETSTAT", []) do
    Stats.reset()
    Ferricstore.SlowLog.reset()
    :ok
  end

  defp handle_config("RESETSTAT", _),
    do: {:error, "ERR wrong number of arguments for 'config|resetstat' command"}

  defp handle_config("REWRITE", []) do
    Ferricstore.Config.rewrite()
  end

  defp handle_config("REWRITE", _),
    do: {:error, "ERR wrong number of arguments for 'config|rewrite' command"}

  defp handle_config(subcmd, _) do
    {:error, "ERR unknown subcommand '#{String.downcase(subcmd)}' for 'config' command"}
  end

  # -- CONFIG SET LOCAL key value -------------------------------------------------

  defp handle_config_set_local([key, value]) do
    Ferricstore.Config.Local.set(String.downcase(key), value)
  end

  defp handle_config_set_local(_args) do
    {:error, "ERR wrong number of arguments for 'config|set|local' command"}
  end

  # -- CONFIG GET LOCAL key ------------------------------------------------------

  defp handle_config_get_local([key]) do
    case Ferricstore.Config.Local.get(String.downcase(key)) do
      {:ok, value} -> [String.downcase(key), value]
      {:error, _} = err -> err
    end
  end

  defp handle_config_get_local(_args) do
    {:error, "ERR wrong number of arguments for 'config|get|local' command"}
  end

  # When the first arg to CONFIG GET/SET is "local" (any case), upcase it so
  # the pattern match `["LOCAL" | rest]` works regardless of client casing.
  defp upcase_local_modifier(["local" | rest]), do: ["LOCAL" | rest]
  defp upcase_local_modifier(["Local" | rest]), do: ["LOCAL" | rest]

  defp upcase_local_modifier([first | rest]) when is_binary(first) do
    if String.upcase(first) == "LOCAL" do
      ["LOCAL" | rest]
    else
      [first | rest]
    end
  end

  defp upcase_local_modifier(args), do: args

  # ---------------------------------------------------------------------------
  # SLOWLOG formatting
  # ---------------------------------------------------------------------------

  defp format_slowlog_entries(entries) do
    Enum.map(entries, fn {id, timestamp_us, duration_us, command} ->
      [id, timestamp_us, duration_us, command]
    end)
  end
end
