defmodule Ferricstore.Commands.Server.Info do
  @moduledoc false

  alias Ferricstore.HLC
  alias Ferricstore.Raft.Cluster, as: RaftCluster
  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Store.Ops
  alias Ferricstore.Stats

  @last_save_key {Ferricstore.Commands.Server, :last_save_unix_seconds}

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

  def info_string(section, store) when section in ["all", "everything"] do
    Enum.map_join(@all_sections, "\r\n", fn s -> build_section(s, store) end)
  end

  def info_string(section, store) when section in @all_sections do
    build_section(section, store)
  end

  def info_string(_unknown, _store) do
    # Redis returns an empty string for unknown sections
    ""
  end

  defp build_section("server", _store) do
    ctx = default_instance_ctx()
    info = if ctx && ctx.server_info_fn, do: ctx.server_info_fn.(), else: %{}
    port = Map.get(info, :tcp_port, 0)
    redis_mode = Map.get(info, :redis_mode, "embedded")

    uptime_seconds = Stats.uptime_seconds()
    uptime_days = div(uptime_seconds, 86_400)

    {os_family, os_name} = :os.type()
    {major, minor, patch} = :os.version()
    os_string = "#{os_family}:#{os_name} #{major}.#{minor}.#{patch}"

    fields = [
      {"redis_version", "7.4.0"},
      {"ferricstore_version", "0.4.1"},
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
    ctx = default_instance_ctx()
    connected = if ctx && ctx.connected_clients_fn, do: ctx.connected_clients_fn.(), else: 0

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
    rate = read_sample_rate()
    hot_sampled = Stats.total_hot_reads()
    cold_sampled = Stats.total_cold_reads()
    hits_sampled = Stats.keyspace_hits()
    misses_sampled = Stats.keyspace_misses()

    # Estimated actuals: sampled counters × sample rate
    hot_est = hot_sampled * rate
    cold_est = cold_sampled * rate
    hits_est = hits_sampled * rate
    misses_est = misses_sampled * rate
    total_reads = hits_est + misses_est
    hit_ratio = if total_reads > 0, do: Float.round(hits_est / total_reads * 100, 2), else: 0.0

    hot_pct =
      if hot_est + cold_est > 0,
        do: Float.round(hot_est / (hot_est + cold_est) * 100, 2),
        else: 0.0

    fields = [
      {"total_connections_received", Integer.to_string(Stats.total_connections())},
      {"total_commands_processed", Integer.to_string(Stats.total_commands())},
      {"keyspace_hits", Integer.to_string(hits_est)},
      {"keyspace_misses", Integer.to_string(misses_est)},
      {"keyspace_hit_ratio", format_float_field(hit_ratio)},
      {"hot_reads", Integer.to_string(hot_est)},
      {"cold_reads", Integer.to_string(cold_est)},
      {"hot_cache_hit_ratio", format_float_field(hot_pct)},
      {"read_sample_rate", "1:#{rate}"},
      {"expired_keys", Integer.to_string(Stats.expired_keys())},
      {"evicted_keys", Integer.to_string(Stats.evicted_keys())}
    ]

    format_section("Stats", fields)
  end

  defp build_section("persistence", _store) do
    fields = [
      {"loading", "0"},
      {"rdb_changes_since_last_save", "0"},
      {"rdb_last_save_time", Integer.to_string(last_save_time())}
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
      {"default_window_ms", Integer.to_string(NamespaceConfig.default_window_ms())}
    ]

    entry_fields =
      Enum.flat_map(entries, fn entry ->
        %{prefix: prefix, window_ms: w} = entry
        changed_at = Map.get(entry, :changed_at, 0)
        changed_by = Map.get(entry, :changed_by, "")

        [
          {"ns_#{prefix}_window_ms", Integer.to_string(w)},
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
        try do
          case RaftCluster.members(i, 1_000) do
            {:ok, _members, leader} ->
              local_id = local_raft_member_id(i)
              {commit_index, last_applied, current_term} = raft_section_counters(i, local_id)

              role =
                if leader == local_id do
                  "leader"
                else
                  "follower"
                end

              leader_node_str =
                case leader do
                  {_name, node_name} -> Atom.to_string(node_name)
                  _ -> "unknown"
                end

              [
                {"shard_#{i}_role", role},
                {"shard_#{i}_current_term", Integer.to_string(current_term)},
                {"shard_#{i}_commit_index", Integer.to_string(commit_index)},
                {"shard_#{i}_last_applied", Integer.to_string(last_applied)},
                {"shard_#{i}_leader_node", leader_node_str}
              ] ++ waraft_info_fields(i)

            _ ->
              [
                {"shard_#{i}_role", "unknown"},
                {"shard_#{i}_current_term", "0"},
                {"shard_#{i}_commit_index", "0"},
                {"shard_#{i}_last_applied", "0"},
                {"shard_#{i}_leader_node", "unknown"}
              ] ++ waraft_info_fields(i)
          end
        rescue
          _ ->
            [
              {"shard_#{i}_role", "unknown"},
              {"shard_#{i}_current_term", "0"},
              {"shard_#{i}_commit_index", "0"},
              {"shard_#{i}_last_applied", "0"},
              {"shard_#{i}_leader_node", "unknown"}
            ] ++ waraft_info_fields(i)
        catch
          _, _ ->
            [
              {"shard_#{i}_role", "unknown"},
              {"shard_#{i}_current_term", "0"},
              {"shard_#{i}_commit_index", "0"},
              {"shard_#{i}_last_applied", "0"},
              {"shard_#{i}_leader_node", "unknown"}
            ] ++ waraft_info_fields(i)
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
                      {:ok, %{size: size}} ->
                        acc + size

                      {:error, reason} ->
                        emit_info_bitcask_scan_failed(:stat_shard_file, i, path, reason)
                        acc
                    end
                  end)

                {length(data), length(hints), total}

              {:error, reason} ->
                emit_info_bitcask_scan_failed(:list_shard_dir, i, shard_dir, reason)
                {0, 0, 0}
            end
          rescue
            kind ->
              emit_info_bitcask_scan_failed(:scan_shard_dir, i, shard_dir, kind)
              {0, 0, 0}
          catch
            kind, reason ->
              emit_info_bitcask_scan_failed(:scan_shard_dir, i, shard_dir, {kind, reason})
              {0, 0, 0}
          end

        merge_candidates = max(0, data_files - 1)
        last_applied = atomic_metric(instance_ctx, :last_applied_index, i)
        last_released = atomic_metric(instance_ctx, :last_released_cursor_index, i)
        replay_safe = atomic_metric(instance_ctx, :replay_safe_index, i)
        replay_safe_requested = atomic_metric(instance_ctx, :replay_safe_requested_index, i)
        replay_safe_lag = max(replay_safe_requested - replay_safe, 0)

        replay_safe_persist_failures =
          atomic_metric(instance_ctx, :replay_safe_persist_failures, i)

        flow_lmdb_replay_safe = atomic_metric(instance_ctx, :flow_lmdb_replay_safe_index, i)

        flow_lmdb_replay_safe_requested =
          atomic_metric(instance_ctx, :flow_lmdb_replay_safe_requested_index, i)

        flow_lmdb_replay_safe_lag =
          max(flow_lmdb_replay_safe_requested - flow_lmdb_replay_safe, 0)

        flow_lmdb_replay_safe_persist_failures =
          atomic_metric(instance_ctx, :flow_lmdb_replay_safe_persist_failures, i)

        flow_lmdb_mirror_enqueue_failures =
          atomic_metric(instance_ctx, :flow_lmdb_mirror_enqueue_failures, i)

        flow_lmdb_mirror_degraded =
          atomic_metric(instance_ctx, :flow_lmdb_mirror_degraded, i)

        flow_lmdb_writer_pending_ops =
          atomic_metric(instance_ctx, :flow_lmdb_writer_pending_ops, i)

        flow_lmdb_writer_oldest_pending_age_us =
          atomic_metric(instance_ctx, :flow_lmdb_writer_oldest_pending_age_us, i)

        flow_lmdb_writer_flush_failures =
          atomic_metric(instance_ctx, :flow_lmdb_writer_flush_failures, i)

        flow_history_projected = atomic_metric(instance_ctx, :flow_history_projected_index, i)
        flow_history_requested = atomic_metric(instance_ctx, :flow_history_requested_index, i)
        flow_history_lag = max(flow_history_requested - flow_history_projected, 0)

        flow_history_projector_pending_entries =
          atomic_metric(instance_ctx, :flow_history_projector_pending_entries, i)

        flow_history_projector_oldest_pending_age_us =
          atomic_metric(instance_ctx, :flow_history_projector_oldest_pending_age_us, i)

        flow_history_projector_flush_failures =
          atomic_metric(instance_ctx, :flow_history_projector_flush_failures, i)

        flow_history_projector_queue_full =
          atomic_metric(instance_ctx, :flow_history_projector_queue_full, i)

        release_gap = max(last_applied - last_released, 0)

        release_cursor_blocked_apply_count =
          atomic_metric(instance_ctx, :release_cursor_blocked_apply_count, i)

        checkpoint_dirty = atomic_metric(instance_ctx, :checkpoint_flags, i)
        checkpoint_in_flight = atomic_metric(instance_ctx, :checkpoint_in_flight, i)

        [
          {"shard_#{i}_data_file_count", Integer.to_string(data_files)},
          {"shard_#{i}_hint_file_count", Integer.to_string(hint_files)},
          {"shard_#{i}_total_size_bytes", Integer.to_string(total_bytes)},
          {"shard_#{i}_merge_candidates", Integer.to_string(merge_candidates)},
          {"shard_#{i}_last_applied_index", Integer.to_string(last_applied)},
          {"shard_#{i}_last_released_cursor_index", Integer.to_string(last_released)},
          {"shard_#{i}_replay_safe_index", Integer.to_string(replay_safe)},
          {"shard_#{i}_replay_safe_requested_index", Integer.to_string(replay_safe_requested)},
          {"shard_#{i}_replay_safe_lag", Integer.to_string(replay_safe_lag)},
          {"shard_#{i}_replay_safe_persist_failures",
           Integer.to_string(replay_safe_persist_failures)},
          {"shard_#{i}_flow_lmdb_replay_safe_index", Integer.to_string(flow_lmdb_replay_safe)},
          {"shard_#{i}_flow_lmdb_replay_safe_requested_index",
           Integer.to_string(flow_lmdb_replay_safe_requested)},
          {"shard_#{i}_flow_lmdb_replay_safe_lag", Integer.to_string(flow_lmdb_replay_safe_lag)},
          {"shard_#{i}_flow_lmdb_replay_safe_persist_failures",
           Integer.to_string(flow_lmdb_replay_safe_persist_failures)},
          {"shard_#{i}_flow_lmdb_mirror_enqueue_failures",
           Integer.to_string(flow_lmdb_mirror_enqueue_failures)},
          {"shard_#{i}_flow_lmdb_mirror_degraded", Integer.to_string(flow_lmdb_mirror_degraded)},
          {"shard_#{i}_flow_lmdb_writer_pending_ops",
           Integer.to_string(flow_lmdb_writer_pending_ops)},
          {"shard_#{i}_flow_lmdb_writer_oldest_pending_age_us",
           Integer.to_string(flow_lmdb_writer_oldest_pending_age_us)},
          {"shard_#{i}_flow_lmdb_writer_flush_failures",
           Integer.to_string(flow_lmdb_writer_flush_failures)},
          {"shard_#{i}_flow_history_projected_index", Integer.to_string(flow_history_projected)},
          {"shard_#{i}_flow_history_requested_index", Integer.to_string(flow_history_requested)},
          {"shard_#{i}_flow_history_projection_lag", Integer.to_string(flow_history_lag)},
          {"shard_#{i}_flow_history_projector_pending_entries",
           Integer.to_string(flow_history_projector_pending_entries)},
          {"shard_#{i}_flow_history_projector_oldest_pending_age_us",
           Integer.to_string(flow_history_projector_oldest_pending_age_us)},
          {"shard_#{i}_flow_history_projector_flush_failures",
           Integer.to_string(flow_history_projector_flush_failures)},
          {"shard_#{i}_flow_history_projector_queue_full",
           Integer.to_string(flow_history_projector_queue_full)},
          {"shard_#{i}_release_cursor_gap", Integer.to_string(release_gap)},
          {"shard_#{i}_release_cursor_blocked_apply_count",
           Integer.to_string(release_cursor_blocked_apply_count)},
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
        {_commit_index, last_applied, _term} = raft_section_counters(i, local_raft_member_id(i))
        acc + last_applied
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

  defp waraft_info_fields(shard_index) do
    segment_log = WARaftBackend.segment_log_memory_status(shard_index)

    [
      {"shard_#{shard_index}_waraft_inflight_commit_bytes",
       Integer.to_string(WARaftBackend.inflight_commit_bytes(shard_index))},
      {"shard_#{shard_index}_waraft_segment_log_ets_entries",
       waraft_info_value(segment_log[:ets_entries])},
      {"shard_#{shard_index}_waraft_segment_log_ets_bytes",
       waraft_info_value(segment_log[:ets_bytes])},
      {"shard_#{shard_index}_waraft_segment_log_disk_first_index",
       waraft_info_value(segment_log[:disk_first_index])},
      {"shard_#{shard_index}_waraft_segment_log_disk_last_index",
       waraft_info_value(segment_log[:disk_last_index])},
      {"shard_#{shard_index}_waraft_segment_log_max_ets_entries",
       waraft_info_value(segment_log[:max_ets_entries])},
      {"shard_#{shard_index}_waraft_segment_log_max_ets_bytes",
       waraft_info_value(segment_log[:max_ets_bytes])},
      {"shard_#{shard_index}_waraft_segment_log_min_ets_entries",
       waraft_info_value(segment_log[:min_ets_entries])}
    ]
  end

  defp waraft_info_value(:infinity), do: "infinity"
  defp waraft_info_value(:undefined), do: "0"
  defp waraft_info_value(nil), do: "0"
  defp waraft_info_value(value) when is_integer(value), do: Integer.to_string(value)
  defp waraft_info_value(value) when is_binary(value), do: value
  defp waraft_info_value(value), do: inspect(value)

  defp local_raft_member_id(shard_index) do
    RaftCluster.shard_server_id(shard_index)
  end

  defp raft_section_counters(shard_index, local_id) do
    _ = local_id
    waraft_section_counters(shard_index)
  end

  defp waraft_section_counters(shard_index) do
    {last_applied, term_from_position} =
      case WARaftBackend.storage_position(shard_index) do
        {:ok, {:raft_log_pos, index, term}} when is_integer(index) and is_integer(term) ->
          {index, term}

        _other ->
          {0, 0}
      end

    {last_applied, last_applied, term_from_position}
  end

  defp emit_info_bitcask_scan_failed(phase, shard_index, path, reason) do
    :telemetry.execute(
      [:ferricstore, :commands, :info, :bitcask_scan_failed],
      %{count: 1},
      %{
        phase: phase,
        shard_index: shard_index,
        path: path,
        reason: reason
      }
    )
  rescue
    _ -> :ok
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

  defp read_sample_rate do
    FerricStore.Instance.get(:default).read_sample_rate
  rescue
    ArgumentError -> 100
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

  defp last_save_time do
    :persistent_term.get(@last_save_key, System.system_time(:second))
  end
end
