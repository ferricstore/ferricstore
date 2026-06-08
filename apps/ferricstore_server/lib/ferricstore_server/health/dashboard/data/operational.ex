defmodule FerricstoreServer.Health.Dashboard.Data.Operational do
  @moduledoc false

  alias Ferricstore.{DataDir, Health, MemoryGuard, NamespaceConfig, SlowLog, Stats}
  alias Ferricstore.Merge.Scheduler, as: MergeScheduler
  alias Ferricstore.Raft.Cluster, as: RaftCluster
  alias Ferricstore.Raft.WARaftBackend

  import FerricstoreServer.Health.Dashboard.Format, only: [safe_ets_size: 1]

  import FerricstoreServer.Health.Dashboard.Render.Admin,
    only: [config_command_reference: 0, runtime_config_parameter_reference: 0]

  def collect_dashboard(flow_summary) do
    %{
      overview: collect_overview(),
      shards: collect_shards(),
      hotcold: collect_hotcold(),
      memory: collect_memory(),
      connections: collect_connections(),
      slowlog: collect_slowlog(),
      merge: collect_merge(),
      namespace_config: NamespaceConfig.get_all(),
      cluster: collect_cluster(),
      lifecycle: collect_lifecycle(),
      flow_summary: flow_summary,
      storage_summary: collect_storage_summary()
    }
  end

  def collect_slowlog_page, do: %{slowlog: collect_slowlog()}
  def collect_merge_page, do: %{merge: collect_merge()}

  def collect_config_page do
    %{
      namespace_config: NamespaceConfig.get_all(),
      config_commands: config_command_reference(),
      config_parameters: runtime_config_parameter_reference()
    }
  end

  def collect_raft_page do
    case Application.get_env(:ferricstore, :dashboard_raft_page_fun) do
      fun when is_function(fun, 0) ->
        fun.()

      _other ->
        %{raft_shards: collect_raft_shards(), cluster: collect_cluster()}
    end
  end

  def collect_clients_page do
    %{clients: collect_client_list(), connections: collect_connections()}
  end

  def collect_storage_page do
    data_dir = Application.get_env(:ferricstore, :data_dir, "/tmp/ferricstore")

    shard_storage =
      Enum.map(0..(shard_count() - 1), fn index ->
        shard_dir = DataDir.shard_data_path(data_dir, index)
        {disk_bytes, data_files, hint_files} = scan_shard_dir(shard_dir)

        %{
          index: index,
          disk_bytes: disk_bytes,
          data_file_count: data_files,
          hint_file_count: hint_files
        }
      end)

    {total_disk, data_files, hint_files} = scan_storage_tree(data_dir)
    total_files = data_files + hint_files

    %{shards: shard_storage, total_disk_bytes: total_disk, total_files: total_files}
  end

  def collect_overview do
    health = Health.check()
    total_keys = health.shards |> Enum.map(& &1.keys) |> Enum.sum()

    %{
      status: health.status,
      uptime_seconds: health.uptime_seconds,
      total_keys: total_keys,
      total_commands: Stats.total_commands(),
      total_connections: Stats.total_connections(),
      memory_bytes: :erlang.memory(:total),
      run_id: Stats.run_id()
    }
  end

  def collect_shards do
    data_dir = Application.get_env(:ferricstore, :data_dir, "/tmp/ferricstore")

    Enum.map(0..(shard_count() - 1), fn index ->
      keydir = :"keydir_#{index}"

      {status, keys, ets_mem} =
        try do
          keys = :ets.info(keydir, :size)
          keydir_words = :ets.info(keydir, :memory)

          mem_bytes =
            if is_integer(keydir_words),
              do: keydir_words * :erlang.system_info(:wordsize),
              else: 0

          ctx = FerricStore.Instance.get(:default)
          shard_name = Ferricstore.Store.Router.shard_name(ctx, index)

          shard_status =
            case Process.whereis(shard_name) do
              pid when is_pid(pid) -> if Process.alive?(pid), do: "ok", else: "down"
              nil -> "down"
            end

          {shard_status, keys, mem_bytes}
        rescue
          ArgumentError -> {"down", 0, 0}
        end

      shard_dir = DataDir.shard_data_path(data_dir, index)
      {disk_bytes, _, _} = scan_shard_dir(shard_dir)

      %{
        index: index,
        status: status,
        keys: keys,
        ets_memory_bytes: ets_mem,
        disk_bytes: disk_bytes
      }
    end)
  end

  def collect_hotcold do
    rate = :persistent_term.get(:ferricstore_read_sample_rate, 100)
    misses_sampled = Stats.keyspace_misses()
    hot_sampled = Stats.total_hot_reads()
    cold_sampled = Stats.total_cold_reads()
    hot_est = hot_sampled * rate
    misses_est = misses_sampled * rate
    cold_exact = cold_sampled
    total_hits = hot_est + cold_exact
    total_lookups = total_hits + misses_est
    uptime = max(Stats.uptime_seconds(), 1)

    %{
      hot_read_pct: Stats.hot_read_pct(),
      cold_reads_per_sec: Stats.cold_reads_per_second(),
      total_hot: hot_est,
      total_cold: cold_exact,
      total_hits: total_hits,
      total_misses: misses_est,
      total_lookups: total_lookups,
      hit_ratio:
        if(total_lookups > 0, do: Float.round(total_hits / total_lookups * 100, 1), else: 0.0),
      ram_ratio: if(total_hits > 0, do: Float.round(hot_est / total_hits * 100, 1), else: 0.0),
      disk_ratio:
        if(total_hits > 0, do: Float.round(cold_exact / total_hits * 100, 1), else: 0.0),
      sample_rate: rate,
      hits_per_sec: Float.round(total_hits / uptime, 1),
      misses_per_sec: Float.round(misses_est / uptime, 1),
      ops_per_sec: Float.round(Stats.total_commands() / uptime, 1),
      top_prefixes: Stats.hotness_top(10)
    }
  end

  def collect_memory do
    try do
      stats = MemoryGuard.stats()

      %{
        total_bytes: stats.total_bytes,
        max_bytes: stats.max_bytes,
        ratio: stats.ratio,
        pressure_level: stats.pressure_level,
        eviction_policy: stats.eviction_policy,
        shards: stats.shards
      }
    catch
      :exit, _ ->
        %{
          total_bytes: 0,
          max_bytes: 0,
          ratio: 0.0,
          pressure_level: :ok,
          eviction_policy: :volatile_lru,
          shards: %{}
        }
    end
  end

  def collect_connections do
    %{
      active: Stats.active_connections(),
      blocked: safe_ets_size(:ferricstore_waiters),
      tracking: safe_ets_size(:ferricstore_tracking_connections)
    }
  end

  def collect_slowlog do
    try do
      SlowLog.get(128)
      |> Enum.map(fn {id, timestamp_us, duration_us, command} ->
        %{id: id, timestamp_us: timestamp_us, duration_us: duration_us, command: command}
      end)
    rescue
      _ -> []
    catch
      :exit, _ -> []
    end
  end

  def collect_merge do
    Enum.map(0..(shard_count() - 1), fn index ->
      try do
        status = MergeScheduler.status(index)

        %{
          shard_index: status.shard_index,
          mode: status.mode,
          merging: status.merging,
          last_merge_at: status.last_merge_at,
          merge_count: status.merge_count,
          total_bytes_reclaimed: status.total_bytes_reclaimed
        }
      catch
        :exit, _ ->
          %{
            shard_index: index,
            mode: :unknown,
            merging: false,
            last_merge_at: nil,
            merge_count: 0,
            total_bytes_reclaimed: 0
          }
      end
    end)
  end

  def collect_cluster do
    nodes = [Node.self() | Node.list()]
    size = length(nodes)

    %{
      node_name: node(),
      cluster_mode: if(size > 1, do: :cluster, else: :standalone),
      cluster_size: size,
      nodes: nodes
    }
  end

  def collect_lifecycle do
    mg_stats =
      try do
        MemoryGuard.stats()
      catch
        :exit, _ -> %{keydir_bytes: 0, keydir_max_ram: 0, keydir_ratio: 0.0}
      end

    keydir_full =
      try do
        MemoryGuard.keydir_full?()
      catch
        :exit, _ -> false
      end

    uptime = max(Stats.uptime_seconds(), 1)
    expired = Stats.expired_keys()
    evicted = Stats.evicted_keys()

    %{
      expired_total: expired,
      evicted_total: evicted,
      expired_per_sec: Float.round(expired / uptime, 1),
      evicted_per_sec: Float.round(evicted / uptime, 1),
      keydir_bytes: mg_stats.keydir_bytes,
      keydir_max_ram: mg_stats.keydir_max_ram,
      keydir_ratio: mg_stats.keydir_ratio,
      keydir_full: keydir_full
    }
  end

  def collect_storage_summary do
    data_dir = Application.get_env(:ferricstore, :data_dir, "/tmp/ferricstore")
    {total_disk, _, _} = scan_storage_tree(data_dir)
    %{total_disk_bytes: total_disk}
  end

  def scan_shard_dir(shard_dir), do: scan_storage_tree(shard_dir)

  def scan_storage_tree(path) when is_binary(path) do
    case File.stat(path) do
      {:ok, %{type: :regular, size: size}} ->
        file = Path.basename(path)

        {size, if(String.ends_with?(file, ".log"), do: 1, else: 0),
         if(String.ends_with?(file, ".hint"), do: 1, else: 0)}

      {:ok, %{type: :directory}} ->
        case Ferricstore.FS.ls(path) do
          {:ok, files} ->
            Enum.reduce(files, {0, 0, 0}, fn file, {bytes, data, hints} ->
              {child_bytes, child_data, child_hints} = scan_storage_tree(Path.join(path, file))
              {bytes + child_bytes, data + child_data, hints + child_hints}
            end)

          {:error, _reason} ->
            {0, 0, 0}
        end

      {:ok, _other} ->
        {0, 0, 0}

      {:error, _reason} ->
        {0, 0, 0}
    end
  end

  def collect_raft_shards do
    Enum.map(0..(shard_count() - 1), &collect_waraft_overview/1)
  end

  def collect_waraft_overview(i) do
    case RaftCluster.members(i, 1_000) do
      {:ok, members, leader} ->
        {last_applied, term} =
          case WARaftBackend.storage_position(i) do
            {:ok, {:raft_log_pos, index, position_term}}
            when is_integer(index) and is_integer(position_term) ->
              {index, position_term}

            _other ->
              {0, 0}
          end

        %{
          shard: i,
          status: :ok,
          leader: leader,
          current_term: term,
          commit_index: last_applied,
          last_applied: last_applied,
          log_size: 0,
          members: members
        }

      _error ->
        unavailable_raft_shard(i)
    end
  catch
    :exit, _ -> unavailable_raft_shard(i)
  end

  def collect_client_list do
    try do
      summaries = FerricstoreServer.Connection.Registry.summaries()

      if summaries != [] do
        collect_client_list_from_registry(summaries)
      else
        collect_client_list_from_ranch()
      end
    catch
      _, _ -> []
    end
  end

  defp collect_client_list_from_registry(summaries) do
    now = System.monotonic_time(:millisecond)

    Enum.map(summaries, fn summary ->
      created =
        if is_integer(Map.get(summary, :created_at_ms)),
          do: Map.get(summary, :created_at_ms),
          else: now

      %{
        pid: Map.get(summary, :pid, self()),
        client_id: Map.get(summary, :client_id),
        client_name: Map.get(summary, :client_name),
        username: Map.get(summary, :username),
        peer: Map.get(summary, :peer, "unknown"),
        age_seconds: max(0, div(now - created, 1000)),
        flags: Map.get(summary, :flags, "")
      }
    end)
  end

  defp collect_client_list_from_ranch do
    try do
      pids = :ranch.procs(FerricstoreServer.Listener, :connections)
      now = System.monotonic_time(:millisecond)

      Enum.map(pids, fn pid ->
        info = Process.info(pid, [:dictionary, :current_function])

        {peer, age, flags} =
          case info do
            nil ->
              {"unknown:0", 0, ""}

            kw ->
              dict = Keyword.get(kw, :dictionary, [])
              state = Keyword.get(dict, :"$conn_state", nil)

              peer_str =
                case state do
                  %{peer: {ip, port}} -> "#{:inet.ntoa(ip) |> to_string()}:#{port}"
                  _ -> "unknown:0"
                end

              created =
                case state do
                  %{created_at: ts} when is_integer(ts) -> ts
                  _ -> now
                end

              flag_list =
                []
                |> then(fn f ->
                  if state && Map.get(state, :multi_state) == :queuing, do: ["M" | f], else: f
                end)
                |> then(fn f ->
                  if state && Map.get(state, :pubsub_channels), do: ["S" | f], else: f
                end)
                |> then(fn f ->
                  if state && Map.get(state, :tracking) && Map.get(state.tracking, :enabled),
                    do: ["T" | f],
                    else: f
                end)

              {peer_str, max(0, div(now - created, 1000)), Enum.join(flag_list)}
          end

        %{pid: pid, peer: peer, age_seconds: age, flags: flags}
      end)
    catch
      _, _ -> []
    end
  end

  defp unavailable_raft_shard(i) do
    %{
      shard: i,
      status: :unavailable,
      leader: nil,
      current_term: 0,
      commit_index: 0,
      last_applied: 0,
      log_size: 0,
      members: []
    }
  end

  defp shard_count, do: :persistent_term.get(:ferricstore_shard_count, 4)
end
