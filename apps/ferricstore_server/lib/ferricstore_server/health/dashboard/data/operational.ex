defmodule FerricstoreServer.Health.Dashboard.Data.Operational do
  @moduledoc false

  alias Ferricstore.{DataDir, Health, MemoryGuard, NamespaceConfig, SlowLog, Stats}
  alias Ferricstore.Flow.PolicyMigrationWorker
  alias Ferricstore.Merge.Scheduler, as: MergeScheduler
  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Store.SegmentFilename
  alias FerricstoreServer.Health.Dashboard.StorageSnapshotCache
  alias FerricstoreServer.Health.Dashboard.Data.Clients

  import FerricstoreServer.Health.Dashboard.Format, only: [safe_ets_size: 1]

  import FerricstoreServer.Health.Dashboard.Render.Admin,
    only: [config_command_reference: 0, runtime_config_parameter_reference: 0]

  @default_storage_summary_ttl_ms 30_000

  def collect_dashboard(flow_summary) do
    slowlog = slowlog_snapshot()

    %{
      generated_at_ms: System.system_time(:millisecond),
      overview: collect_overview(),
      shards: collect_shards(),
      hotcold: collect_hotcold(),
      memory: collect_memory(),
      connections: collect_connections(),
      slowlog: slowlog.entries,
      slowlog_status: slowlog.status,
      slowlog_error: slowlog.error,
      merge: collect_merge(),
      namespace_config: NamespaceConfig.get_all(),
      cluster: collect_cluster(),
      lifecycle: collect_lifecycle(),
      flow_summary: flow_summary,
      subsystem_health: collect_subsystem_health(),
      storage_summary: collect_storage_summary()
    }
  end

  def collect_subsystem_health do
    policy_migration =
      if Application.get_env(:ferricstore, :flow_policy_migration_worker_enabled, true) do
        case FerricStore.Instance.fetch(:default) do
          {:ok, ctx} -> PolicyMigrationWorker.health_snapshot(ctx)
          :error -> %{status: :unavailable, issues: [], updated_at_ms: nil}
        end
      else
        %{status: :disabled, issues: [], updated_at_ms: nil}
      end

    %{policy_migration: policy_migration}
  end

  def collect_slowlog_page do
    snapshot = slowlog_snapshot()
    %{slowlog: snapshot.entries, slowlog_status: snapshot.status, slowlog_error: snapshot.error}
  end

  def collect_merge_page, do: %{merge: collect_merge()}

  def collect_config_page do
    %{
      namespace_config: NamespaceConfig.get_all(),
      config_commands: config_command_reference(),
      config_parameters: collect_config_parameters()
    }
  end

  defp collect_config_parameters do
    values =
      try do
        Ferricstore.Config.get("*") |> Map.new()
      catch
        :exit, _ -> %{}
      end

    Enum.map(runtime_config_parameter_reference(), fn entry ->
      {value, source} =
        if entry.parameter == "log_level" do
          {to_string(Logger.level()), "Logger.level"}
        else
          {Map.get(values, entry.parameter), "CONFIG GET"}
        end

      Map.merge(entry, %{value: value, source: if(is_nil(value), do: "Unavailable", else: source)})
    end)
  end

  def collect_raft_page do
    case Application.get_env(:ferricstore, :dashboard_raft_page_fun) do
      fun when is_function(fun, 0) ->
        fun.()

      _other ->
        %{raft_shards: collect_raft_shards(), cluster: collect_cluster()}
    end
  end

  def collect_clients_page(opts \\ []) do
    snapshot = Clients.snapshot(opts)

    %{
      clients: snapshot.clients,
      connections: Map.put(collect_connections(), :summary_scope, :shown),
      client_coverage: Map.drop(snapshot, [:clients]),
      client_filters: snapshot.filters
    }
  end

  def collect_storage_page do
    collect_storage_snapshot()
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
      run_id: Stats.run_id(),
      version: application_version()
    }
  end

  defp application_version do
    case Application.spec(:ferricstore_server, :vsn) do
      nil -> "unknown"
      version -> to_string(version)
    end
  end

  def collect_shards do
    disk_bytes_by_shard = storage_disk_bytes_by_shard(collect_storage_snapshot())

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

      %{
        index: index,
        status: status,
        keys: keys,
        ets_memory_bytes: ets_mem,
        disk_bytes: Map.get(disk_bytes_by_shard, index, 0)
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
      MemoryGuard.stats() |> memory_snapshot()
    catch
      :exit, _ ->
        %{
          total_bytes: 0,
          max_bytes: 0,
          ratio: 0.0,
          pressure_level: :unavailable,
          eviction_policy: :volatile_lru,
          shards: %{}
        }
    end
  end

  def memory_snapshot(stats) do
    Map.take(stats, [
      :total_bytes,
      :max_bytes,
      :ratio,
      :pressure_level,
      :eviction_policy,
      :shards,
      :rss_bytes,
      :rss_ratio,
      :rss_pressure_level,
      :memory_limit,
      :keydir_bytes,
      :keydir_max_ram
    ])
  end

  def collect_connections do
    %{
      active: Stats.active_connections(),
      blocked: safe_ets_size(:ferricstore_waiters),
      tracking: safe_ets_size(:ferricstore_tracking_connections)
    }
  end

  def collect_slowlog, do: slowlog_snapshot().entries

  def slowlog_snapshot(reader \\ &SlowLog.get/1) do
    try do
      entries =
        reader.(128)
        |> Enum.take(128)
        |> Enum.map(fn {id, timestamp_us, duration_us, command} ->
          %{id: id, timestamp_us: timestamp_us, duration_us: duration_us, command: command}
        end)

      %{status: :ok, entries: entries, error: nil}
    rescue
      error -> %{status: :unavailable, entries: [], error: Exception.message(error)}
    catch
      kind, reason -> %{status: :unavailable, entries: [], error: inspect({kind, reason})}
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
    %{total_disk_bytes: total_disk_bytes} = collect_storage_snapshot()
    %{total_disk_bytes: total_disk_bytes}
  end

  defp collect_storage_snapshot do
    data_dir = Application.get_env(:ferricstore, :data_dir, "/tmp/ferricstore")
    shard_count = shard_count()
    cache_ttl_ms = storage_summary_cache_ttl_ms()
    now_ms = System.monotonic_time(:millisecond)

    case cached_storage_snapshot(data_dir, shard_count, now_ms, cache_ttl_ms) do
      {:ok, snapshot} -> snapshot
      :miss -> refresh_storage_snapshot(data_dir, shard_count, cache_ttl_ms)
    end
  end

  defp refresh_storage_snapshot(data_dir, shard_count, cache_ttl_ms) do
    lock_id = {{__MODULE__, :storage_summary_refresh}, self()}

    case :global.trans(
           lock_id,
           fn ->
             now_ms = System.monotonic_time(:millisecond)

             case cached_storage_snapshot(data_dir, shard_count, now_ms, cache_ttl_ms) do
               {:ok, snapshot} -> snapshot
               :miss -> scan_and_cache_storage_snapshot(data_dir, shard_count, now_ms)
             end
           end,
           [node()]
         ) do
      {:aborted, _reason} ->
        scan_and_cache_storage_snapshot(
          data_dir,
          shard_count,
          System.monotonic_time(:millisecond)
        )

      snapshot ->
        snapshot
    end
  catch
    :exit, _reason ->
      scan_storage_snapshot(data_dir, shard_count)
  end

  defp cached_storage_snapshot(data_dir, shard_count, now_ms, cache_ttl_ms)
       when cache_ttl_ms > 0 do
    case StorageSnapshotCache.lookup() do
      {:ok, {^data_dir, ^shard_count}, cached_at_ms, snapshot}
      when is_integer(cached_at_ms) and now_ms >= cached_at_ms and
             now_ms - cached_at_ms < cache_ttl_ms ->
        {:ok, snapshot}

      _other ->
        :miss
    end
  end

  defp cached_storage_snapshot(_data_dir, _shard_count, _now_ms, _cache_ttl_ms), do: :miss

  defp scan_and_cache_storage_snapshot(data_dir, shard_count, now_ms) do
    snapshot = scan_storage_snapshot(data_dir, shard_count)

    cached_at_ms = max(now_ms, System.monotonic_time(:millisecond))

    StorageSnapshotCache.put({data_dir, shard_count}, cached_at_ms, snapshot)

    snapshot
  end

  defp scan_storage_snapshot(data_dir, shard_count) do
    observer = storage_scan_observer()
    notify_storage_scan(observer, {:scan_started, data_dir})

    shard_roots =
      Map.new(0..(shard_count - 1), fn index ->
        {DataDir.shard_data_path(data_dir, index), index}
      end)

    {total_disk, data_files, hint_files, shard_totals} =
      scan_storage_snapshot_tree(
        data_dir,
        shard_roots,
        nil,
        %{},
        observer
      )

    shard_storage =
      Enum.map(0..(shard_count - 1), fn index ->
        {disk_bytes, shard_data_files, shard_hint_files} =
          Map.get(shard_totals, index, {0, 0, 0})

        %{
          index: index,
          disk_bytes: disk_bytes,
          data_file_count: shard_data_files,
          hint_file_count: shard_hint_files
        }
      end)

    notify_storage_scan(observer, {:scan_finished, data_dir})

    %{
      shards: shard_storage,
      total_disk_bytes: total_disk,
      total_files: data_files + hint_files
    }
  end

  defp scan_storage_snapshot_tree(path, shard_roots, current_shard, shard_totals, observer) do
    notify_storage_scan(observer, {:path, path})
    current_shard = Map.get(shard_roots, path, current_shard)

    case File.lstat(path) do
      {:ok, %{type: :regular, size: size}} ->
        file = Path.basename(path)
        {data_files, hint_files} = storage_file_counts(file)

        shard_totals =
          add_shard_storage_total(
            shard_totals,
            current_shard,
            size,
            data_files,
            hint_files
          )

        {size, data_files, hint_files, shard_totals}

      {:ok, %{type: :directory}} ->
        case Ferricstore.FS.ls(path) do
          {:ok, files} ->
            Enum.reduce(files, {0, 0, 0, shard_totals}, fn file, {bytes, data, hints, totals} ->
              {child_bytes, child_data, child_hints, totals} =
                scan_storage_snapshot_tree(
                  Path.join(path, file),
                  shard_roots,
                  current_shard,
                  totals,
                  observer
                )

              {bytes + child_bytes, data + child_data, hints + child_hints, totals}
            end)

          {:error, _reason} ->
            {0, 0, 0, shard_totals}
        end

      {:ok, _other} ->
        {0, 0, 0, shard_totals}

      {:error, _reason} ->
        {0, 0, 0, shard_totals}
    end
  end

  defp add_shard_storage_total(shard_totals, shard, bytes, data_files, hint_files)
       when is_integer(shard) do
    Map.update(
      shard_totals,
      shard,
      {bytes, data_files, hint_files},
      fn {current_bytes, current_data, current_hints} ->
        {current_bytes + bytes, current_data + data_files, current_hints + hint_files}
      end
    )
  end

  defp add_shard_storage_total(shard_totals, _shard, _bytes, _data_files, _hint_files),
    do: shard_totals

  defp storage_scan_observer do
    case Application.get_env(:ferricstore, :dashboard_storage_scan_observer) do
      observer when is_function(observer, 1) -> observer
      _other -> nil
    end
  end

  defp notify_storage_scan(nil, _event), do: :ok

  defp notify_storage_scan(observer, event) do
    observer.(event)
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end

  defp storage_disk_bytes_by_shard(%{shards: shards}) when is_list(shards) do
    Map.new(shards, fn shard -> {shard.index, shard.disk_bytes} end)
  end

  defp storage_disk_bytes_by_shard(_snapshot), do: %{}

  defp storage_summary_cache_ttl_ms do
    case Application.get_env(
           :ferricstore,
           :dashboard_storage_summary_ttl_ms,
           @default_storage_summary_ttl_ms
         ) do
      value when is_integer(value) and value >= 0 -> value
      _other -> @default_storage_summary_ttl_ms
    end
  end

  def scan_shard_dir(shard_dir), do: scan_storage_tree(shard_dir)

  def scan_storage_tree(path) when is_binary(path) do
    case File.lstat(path) do
      {:ok, %{type: :regular, size: size}} ->
        file = Path.basename(path)
        {data_files, hint_files} = storage_file_counts(file)

        {size, data_files, hint_files}

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

  defp storage_file_counts(file) do
    case SegmentFilename.parse(file) do
      {:ok, _file_id} ->
        {1, 0}

      _not_a_canonical_log ->
        case SegmentFilename.parse(file, ".hint") do
          {:ok, _file_id} -> {0, 1}
          _not_a_canonical_hint -> {0, 0}
        end
    end
  end

  def collect_raft_shards do
    Enum.map(0..(shard_count() - 1), &collect_waraft_overview/1)
  end

  def collect_waraft_overview(i, opts \\ []) do
    readers = [
      Keyword.get(opts, :status, &WARaftBackend.status/1),
      Keyword.get(opts, :position, &WARaftBackend.storage_position/1)
    ]

    readers =
      case Keyword.get(opts, :members) do
        reader when is_function(reader, 1) -> readers ++ [reader]
        _ -> readers
      end

    # Storage's public deadline is much longer than a dashboard request. These
    # read-only workers are scoped to one shard and killed on deadline expiry.
    [status, position | membership] =
      Task.async_stream(readers, &safe_raft_read(&1, i),
        max_concurrency: 3,
        timeout: Keyword.get(opts, :timeout, 1_000),
        on_timeout: :kill_task
      )
      |> Enum.map(fn
        {:ok, result} -> result
        {:exit, reason} -> {:error, reason}
      end)

    members =
      case membership do
        [result] -> result
        [] -> raft_membership(status)
      end

    raft_snapshot(i, members, status, position)
  end

  defp raft_membership(status) when is_list(status) do
    case Keyword.get(status, :config) do
      %{version: 1, membership: members} = config when is_list(members) ->
        members =
          Enum.map(:wa_raft_server.get_config_members(config), fn {:raft_identity, name,
                                                                   member_node} ->
            {name, member_node}
          end)

        identity = {Keyword.get(status, :leader_name), Keyword.get(status, :leader_id)}
        leader = Enum.find(members, &(&1 == identity))
        {:ok, members, leader}

      _ ->
        {:error, :unavailable}
    end
  end

  defp raft_membership(_status), do: {:error, :unavailable}

  defp safe_raft_read(reader, index) do
    reader.(index)
  rescue
    error -> {:error, Exception.message(error)}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  def raft_snapshot(i, membership, server_status, position) do
    status =
      if is_list(server_status) and Keyword.keyword?(server_status), do: server_status, else: []

    term = raft_index(Keyword.get(status, :current_term))
    commit = raft_index(Keyword.get(status, :commit_index))

    applied =
      case position do
        {:ok, {:raft_log_pos, index, _term}} -> raft_index(index)
        _ -> nil
      end

    {members, leader, membership_ok?} =
      case membership do
        {:ok, members, leader} when is_list(members) -> {members, leader, true}
        _ -> {[], nil, false}
      end

    available = Enum.count([term, commit, applied], &is_integer/1)

    %{
      shard: i,
      status:
        cond do
          membership_ok? and available == 3 -> :ok
          membership_ok? or available > 0 -> :partial
          true -> :unavailable
        end,
      leader: leader,
      current_term: term,
      commit_index: commit,
      last_applied: applied,
      log_size: nil,
      members: members
    }
  end

  defp raft_index(value) when is_integer(value) and value >= 0, do: value
  defp raft_index(_value), do: nil

  def collect_client_list, do: Clients.snapshot().clients

  defp shard_count, do: :persistent_term.get(:ferricstore_shard_count, 4)
end
