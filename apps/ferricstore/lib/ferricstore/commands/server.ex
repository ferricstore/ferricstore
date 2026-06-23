# Suppress function clause grouping warnings (clauses added by different agents)
defmodule Ferricstore.Commands.Server do
  @moduledoc """
  Handles Redis server commands: PING, ECHO, DBSIZE, KEYS, FLUSHDB, FLUSHALL,
  INFO, COMMAND, SELECT, LOLWUT, and DEBUG.

  Each handler takes the uppercased command name, a list of string arguments,
  and an injected store map. Returns plain Elixir terms — the connection layer
  handles wire encoding.

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
  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Stats
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.Router

  @waraft_table :ferricstore_waraft_backend

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

    with :ok <- Ops.flush(store) do
      Ferricstore.Commands.Stream.clear_local_state()

      # Wipe prob files (bloom, CMS, cuckoo, TopK) across all shards.
      # store.flush deletes keys via Raft which should clean up files via
      # maybe_delete_prob_file, but as a safety net we also wipe the prob
      # directories directly.
      flush_store_prob_dirs(store)
    end
  end

  def handle("FLUSHDB", _args, _store) do
    {:error, "ERR syntax error"}
  end

  # ---------------------------------------------------------------------------
  # FLUSHALL — alias for FLUSHDB in our single-database server
  # ---------------------------------------------------------------------------

  def handle("FLUSHALL", args, store) when args in [[], ["ASYNC"], ["SYNC"]] do
    AuditLog.log(:dangerous_command, %{command: "FLUSHALL", args: args})

    with :ok <- Ops.flush(store) do
      Ferricstore.Commands.Stream.clear_local_state()
      flush_store_prob_dirs(store)
    end
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
    Ferricstore.Commands.Server.Info.info_string(section_lower, store)
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

  def handle("COMMAND", [subcmd | rest], _store) do
    case String.upcase(subcmd) do
      "COUNT" when rest == [] ->
        Catalog.count()

      "LIST" when rest == [] ->
        Catalog.names()

      "INFO" ->
        case rest do
          [] ->
            {:error, "ERR wrong number of arguments for 'command|info' command"}

          names ->
            Enum.map(names, fn name ->
              case Catalog.lookup(name) do
                {:ok, cmd} -> Catalog.info_tuple(cmd)
                :error -> nil
              end
            end)
        end

      "DOCS" ->
        case rest do
          [] ->
            {:error, "ERR wrong number of arguments for 'command|docs' command"}

          names ->
            Enum.flat_map(names, fn name ->
              case Catalog.lookup(name) do
                {:ok, cmd} -> [cmd.name, [cmd.summary]]
                :error -> []
              end
            end)
        end

      "GETKEYS" ->
        case rest do
          [] ->
            {:error, "ERR wrong number of arguments for 'command|getkeys' command"}

          [cmd_name | cmd_args] ->
            case Catalog.get_keys(cmd_name, cmd_args) do
              {:ok, keys} -> keys
              {:error, msg} -> {:error, msg}
            end
        end

      _ ->
        {:error, "ERR unknown subcommand '#{subcmd}'. Try COMMAND HELP."}
    end
  end

  # ---------------------------------------------------------------------------
  # LOLWUT [VERSION version]
  # ---------------------------------------------------------------------------

  def handle("LOLWUT", [], _store), do: lolwut_art()

  def handle("LOLWUT", [version_opt, _version], _store) do
    case String.upcase(version_opt) do
      "VERSION" -> lolwut_art()
      _ -> {:error, "ERR syntax error"}
    end
  end

  def handle("LOLWUT", _args, _store) do
    {:error, "ERR syntax error"}
  end

  # ---------------------------------------------------------------------------
  # DEBUG SLEEP seconds
  # ---------------------------------------------------------------------------

  def handle("DEBUG", [subcmd | rest], store) do
    case String.upcase(subcmd) do
      "SLEEP" ->
        case rest do
          [seconds_str] ->
            AuditLog.log(:dangerous_command, %{command: "DEBUG", args: ["SLEEP", seconds_str]})

            case Integer.parse(seconds_str) do
              {secs, ""} when secs >= 0 ->
                Process.sleep(secs * 1000)
                :ok

              _ ->
                {:error, "ERR invalid argument for DEBUG SLEEP"}
            end

          _ ->
            {:error, "ERR wrong number of arguments for 'debug' command"}
        end

      "RELOAD" when rest == [] ->
        :ok

      "FLUSHALL" ->
        AuditLog.log(:dangerous_command, %{command: "DEBUG", args: ["FLUSHALL"]})
        handle("FLUSHALL", [], store)

      "BATCHER-STATS" when rest == [] ->
        ctx = FerricStore.Instance.get(:default)
        shard_count = ctx.shard_count

        {:simple, debug_batcher_stats(shard_count)}

      "SET-ACTIVE-EXPIRE" when length(rest) == 1 ->
        :ok

      "CHANGE-REPL-ID" when rest == [] ->
        :ok

      "QUICKLIST-PACKED-THRESHOLD" ->
        :ok

      "AOFSTAT" when rest == [] ->
        %{}

      "SFLAGS" when rest == [] ->
        %{}

      _ ->
        {:error, "ERR unknown subcommand '#{subcmd}'. Try DEBUG HELP."}
    end
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

  def handle("MODULE", [subcmd | _rest], _store) do
    case String.upcase(subcmd) do
      "LIST" -> []
      "LOAD" -> {:error, "ERR FerricStore does not support modules"}
      "UNLOAD" -> {:error, "ERR FerricStore does not support modules"}
      _ -> {:error, "ERR unknown subcommand for 'module' command"}
    end
  end

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

  def handle("SLOWLOG", [subcmd | rest], _store) do
    case String.upcase(subcmd) do
      "GET" ->
        case rest do
          [] ->
            format_slowlog_entries(Ferricstore.SlowLog.get())

          [count_str] ->
            case Integer.parse(count_str) do
              {count, ""} when count >= 0 ->
                format_slowlog_entries(Ferricstore.SlowLog.get(count))

              _ ->
                {:error, "ERR value is not an integer or out of range"}
            end

          _ ->
            {:error, "ERR unknown subcommand or wrong number of arguments for 'slowlog' command"}
        end

      "LEN" when rest == [] ->
        Ferricstore.SlowLog.len()

      "RESET" when rest == [] ->
        Ferricstore.SlowLog.reset()
        :ok

      "HELP" when rest == [] ->
        [
          "SLOWLOG GET [<count>] -- Return top entries from the slowlog.",
          "SLOWLOG LEN -- Return the number of entries in the slowlog.",
          "SLOWLOG RESET -- Reset the slowlog."
        ]

      _ ->
        {:error, "ERR unknown subcommand or wrong number of arguments for 'slowlog' command"}
    end
  end

  def handle("SLOWLOG", [], _store) do
    {:error, "ERR unknown subcommand or wrong number of arguments for 'slowlog' command"}
  end

  # ---------------------------------------------------------------------------
  # SAVE / BGSAVE / LASTSAVE
  # ---------------------------------------------------------------------------

  @last_save_key {__MODULE__, :last_save_unix_seconds}

  def handle("SAVE", [], store), do: save_now(store)

  def handle("SAVE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'save' command"}
  end

  def handle("BGSAVE", [], store) do
    _ = Task.start(fn -> save_now(store) end)
    {:simple, "Background saving started"}
  end

  def handle("BGSAVE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'bgsave' command"}
  end

  def handle("LASTSAVE", [], _store), do: last_save_time()

  def handle("LASTSAVE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'lastsave' command"}
  end

  def handle("FERRICSTORE.BLOBGC", [], store) do
    with {:ok, ctx} <- server_instance_ctx(store),
         {:ok, stats} <- Router.sweep_blob_garbage(ctx) do
      blob_gc_result(stats)
    else
      {:error, :no_default_instance} ->
        {:error, "ERR no default instance available for 'ferricstore.blobgc' command"}

      {:error, reason} ->
        {:error, "ERR blob gc failed: #{inspect(reason)}"}
    end
  end

  def handle("FERRICSTORE.BLOBGC", _args, _store) do
    {:error, "ERR wrong number of arguments for 'ferricstore.blobgc' command"}
  end

  def handle("FERRICSTORE.DOCTOR", args, store) when is_list(args) do
    with {:ok, ctx} <- server_instance_ctx(store) do
      Ferricstore.Doctor.handle_command(normalize_doctor_args(args), ctx)
    else
      {:error, :no_default_instance} ->
        {:error, "ERR no default instance available for 'ferricstore.doctor' command"}
    end
  end

  # ===========================================================================
  # Private helpers
  # ===========================================================================

  # ---------------------------------------------------------------------------
  # DEBUG helpers
  # ---------------------------------------------------------------------------

  defp debug_batcher_stats(shard_count), do: debug_waraft_stats(shard_count)

  defp debug_waraft_stats(shard_count) do
    if shard_count <= 0 do
      ""
    else
      0..(shard_count - 1)
      |> Enum.map(&debug_waraft_shard_stats/1)
      |> Enum.join(" | ")
    end
  end

  defp debug_waraft_shard_stats(shard_index) do
    partition = shard_index + 1

    components = [
      {"server", :wa_raft_server.registered_name(@waraft_table, partition)},
      {"acceptor", :wa_raft_acceptor.registered_name(@waraft_table, partition)},
      {"queue", :wa_raft_queue.registered_name(@waraft_table, partition)},
      {"storage", :wa_raft_storage.registered_name(@waraft_table, partition)}
    ]

    body =
      components
      |> Enum.map(fn {label, name} -> "#{label}=#{component_process_stat(name)}" end)
      |> Kernel.++(["inflight_bytes=#{WARaftBackend.inflight_commit_bytes(shard_index)}"])
      |> Enum.join(",")

    "WA#{shard_index}:#{body}"
  end

  defp component_process_stat(name) do
    case process_stat(name) do
      :down -> "down"
      {mq, reductions} -> "mq=#{mq},r=#{reductions}"
    end
  end

  defp process_stat(name) do
    with pid when is_pid(pid) <- Process.whereis(name),
         info when is_list(info) <- Process.info(pid, [:message_queue_len, :reductions]) do
      {Keyword.get(info, :message_queue_len, 0), Keyword.get(info, :reductions, 0)}
    else
      _ -> :down
    end
  end

  # ---------------------------------------------------------------------------
  # SAVE helpers
  # ---------------------------------------------------------------------------

  defp save_now(store) do
    case persistence_barrier(store) do
      :ok ->
        record_last_save()
        :ok

      {:error, msg} when is_binary(msg) ->
        {:error, msg}

      {:error, reason} ->
        {:error, "ERR save failed: #{inspect(reason)}"}

      other ->
        {:error, "ERR save failed: #{inspect(other)}"}
    end
  end

  defp persistence_barrier(%{persistence_barrier: barrier}) when is_function(barrier, 0) do
    barrier.()
  end

  defp persistence_barrier(%FerricStore.Instance{} = ctx), do: persistence_barrier_for_ctx(ctx)

  defp persistence_barrier(%{__instance_ctx__: %FerricStore.Instance{} = ctx}),
    do: persistence_barrier_for_ctx(ctx)

  defp persistence_barrier(_store), do: :ok

  defp persistence_barrier_for_ctx(ctx) do
    with :ok <- flush_raft_batchers(ctx),
         :ok <- flush_bitcask_writers(ctx),
         :ok <- sync_checkpointers(ctx) do
      :ok
    end
  end

  defp flush_raft_batchers(%{name: :default, shard_count: shard_count}) do
    Enum.reduce_while(0..(shard_count - 1), :ok, fn i, :ok ->
      case Ferricstore.Raft.Batcher.flush(i, 30_000) do
        :ok -> {:cont, :ok}
        {:error, _} = err -> {:halt, err}
        other -> {:halt, {:error, {:batcher_flush_failed, i, other}}}
      end
    end)
  end

  defp flush_raft_batchers(_ctx), do: :ok

  defp flush_bitcask_writers(%{shard_count: shard_count} = ctx) do
    Enum.reduce_while(0..(shard_count - 1), :ok, fn i, :ok ->
      case Ferricstore.Store.BitcaskWriter.flush(ctx, i, 30_000) do
        :ok -> {:cont, :ok}
        {:error, _} = err -> {:halt, err}
        other -> {:halt, {:error, {:bitcask_writer_flush_failed, i, other}}}
      end
    end)
  end

  defp sync_checkpointers(%{shard_count: shard_count} = ctx) do
    Enum.reduce_while(0..(shard_count - 1), :ok, fn i, :ok ->
      name = Ferricstore.Store.BitcaskCheckpointer.process_name(i, ctx)

      case Process.whereis(name) do
        pid when is_pid(pid) ->
          case Ferricstore.Store.BitcaskCheckpointer.sync_now(pid) do
            :ok -> {:cont, :ok}
            {:error, _} = err -> {:halt, err}
            other -> {:halt, {:error, {:checkpointer_sync_failed, i, other}}}
          end

        nil ->
          case sync_active_file(ctx, i) do
            :ok -> {:cont, :ok}
            {:error, _} = err -> {:halt, err}
            other -> {:halt, {:error, {:active_file_sync_failed, i, other}}}
          end
      end
    end)
  end

  defp sync_active_file(ctx, shard_index) do
    try do
      {_file_id, path, _shard_path} = Ferricstore.Store.ActiveFile.get(ctx, shard_index)
      Ferricstore.Bitcask.NIF.v2_fsync(path)
    rescue
      error -> {:error, {:active_file_sync_exception, shard_index, error}}
    catch
      kind, reason -> {:error, {:active_file_sync_throw, shard_index, kind, reason}}
    end
  end

  defp record_last_save do
    ts = System.os_time(:second)
    :persistent_term.put(@last_save_key, ts)
    ts
  end

  defp last_save_time do
    :persistent_term.get(@last_save_key, 0)
  end

  # ---------------------------------------------------------------------------
  # FLUSHDB helper
  # ---------------------------------------------------------------------------

  defp shard_count do
    try do
      FerricStore.Instance.get(:default).shard_count
    rescue
      ArgumentError ->
        Application.get_env(:ferricstore, :shard_count, 4)
    end
  end

  defp flush_all_prob_dirs do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    Ferricstore.ProbCleanup.flush_all(data_dir, shard_count())
  end

  defp flush_store_prob_dirs(%FerricStore.Instance{} = ctx) do
    Ferricstore.ProbCleanup.flush_all(ctx.data_dir, ctx.shard_count)
  end

  defp flush_store_prob_dirs(store) when is_map(store) do
    case Map.fetch(store, :flush_prob_dirs) do
      {:ok, flush_prob_dirs} when is_function(flush_prob_dirs, 0) -> flush_prob_dirs.()
      _ -> flush_all_prob_dirs()
    end
  end

  defp flush_store_prob_dirs(_store), do: flush_all_prob_dirs()

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
          old_value: Ferricstore.Config.redact_for_metadata(key, old_value || ""),
          new_value: Ferricstore.Config.redact_for_metadata(key, value)
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

  defp lolwut_art do
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

  defp normalize_doctor_args([]), do: []

  defp normalize_doctor_args([command | rest]) when is_binary(command) do
    case String.upcase(command) do
      "CHECK" -> ["CHECK" | normalize_doctor_scope_args(rest)]
      "START" -> ["START" | normalize_doctor_start_args(rest)]
      "STATUS" -> ["STATUS" | rest]
      "LIST" -> ["LIST" | rest]
      "CANCEL" -> ["CANCEL" | rest]
      other -> [other | rest]
    end
  end

  defp normalize_doctor_args(args), do: args

  defp normalize_doctor_start_args([kind, subject | rest])
       when is_binary(kind) and is_binary(subject) do
    [String.upcase(kind), String.upcase(subject) | normalize_doctor_scope_args(rest)]
  end

  defp normalize_doctor_start_args([kind | rest]) when is_binary(kind),
    do: [String.upcase(kind) | rest]

  defp normalize_doctor_start_args(args), do: args

  defp normalize_doctor_scope_args([scope_kw, scope | rest])
       when is_binary(scope_kw) and is_binary(scope) do
    case String.upcase(scope_kw) do
      "SCOPE" -> ["SCOPE", scope | rest]
      "SCOPES" -> ["SCOPES", scope | rest]
      _other -> [scope_kw, scope | rest]
    end
  end

  defp normalize_doctor_scope_args(args), do: args

  defp server_instance_ctx(%{instance_ctx: %FerricStore.Instance{} = ctx}), do: {:ok, ctx}

  defp server_instance_ctx(_store) do
    {:ok, FerricStore.Instance.get(:default)}
  rescue
    ArgumentError -> {:error, :no_default_instance}
  end

  defp blob_gc_result(stats) do
    [
      "deleted_files",
      Map.get(stats, :deleted_files, 0),
      "deleted_bytes",
      Map.get(stats, :deleted_bytes, 0),
      "kept_files",
      Map.get(stats, :kept_files, 0),
      "deleted_tmp_files",
      Map.get(stats, :deleted_tmp_files, 0),
      "deleted_tmp_bytes",
      Map.get(stats, :deleted_tmp_bytes, 0),
      "hardened_protections_seen",
      Map.get(stats, :hardened_protections_seen, 0),
      "hardened_protections_released",
      Map.get(stats, :hardened_protections_released, 0),
      "hardened_protections_blocked",
      Map.get(stats, :hardened_protections_blocked, 0)
    ]
  end

  # ---------------------------------------------------------------------------
  # SLOWLOG formatting
  # ---------------------------------------------------------------------------

  defp format_slowlog_entries(entries) do
    Enum.map(entries, fn {id, timestamp_us, duration_us, command} ->
      [id, timestamp_us, duration_us, command]
    end)
  end
end
