defmodule FlowStateLMDBSoak.Sections.Part03 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      defp sample_ets_table_binaries(table, max_rows) do
        try do
          :ets.safe_fixtable(table, true)

          try do
            sample_ets_table_binaries(table, :ets.first(table), max_rows, 0, 0)
          rescue
            ArgumentError -> {0, 0}
          after
            try do
              :ets.safe_fixtable(table, false)
            rescue
              ArgumentError -> :ok
            end
          end
        rescue
          ArgumentError -> {0, 0}
        end
      end

      defp sample_ets_table_binaries(_table, :"$end_of_table", _max_rows, bytes, rows),
        do: {bytes, rows}

      defp sample_ets_table_binaries(_table, _key, max_rows, bytes, rows) when rows >= max_rows,
        do: {bytes, rows}

      defp sample_ets_table_binaries(table, key, max_rows, bytes, rows) do
        next = :ets.next(table, key)

        row_bytes =
          case :ets.lookup(table, key) do
            [row] -> term_binary_ref_bytes(row)
            rows when is_list(rows) -> Enum.reduce(rows, 0, &(&2 + term_binary_ref_bytes(&1)))
          end

        sample_ets_table_binaries(table, next, max_rows, bytes + row_bytes, rows + 1)
      rescue
        ArgumentError -> {bytes, rows}
      end

      defp term_binary_ref_bytes(term) when is_binary(term) and byte_size(term) > 64,
        do: byte_size(term)

      defp term_binary_ref_bytes(term) when is_tuple(term) do
        term
        |> Tuple.to_list()
        |> Enum.reduce(0, &(&2 + term_binary_ref_bytes(&1)))
      end

      defp term_binary_ref_bytes(term) when is_list(term),
        do: Enum.reduce(term, 0, &(&2 + term_binary_ref_bytes(&1)))

      defp term_binary_ref_bytes(term) when is_map(term) do
        :maps.fold(
          fn key, value, acc ->
            acc + term_binary_ref_bytes(key) + term_binary_ref_bytes(value)
          end,
          0,
          term
        )
      end

      defp term_binary_ref_bytes(_term), do: 0

      defp os_process_status do
        pid = :os.getpid() |> List.to_string()

        case System.cmd("ps", ["-o", "pcpu=,rss=", "-p", pid], stderr_to_stdout: true) do
          {output, 0} ->
            case String.split(output) do
              [cpu, rss | _] ->
                %{
                  cpu_pct: parse_float(cpu),
                  rss_mb: bytes_to_mb(String.to_integer(rss) * 1024)
                }

              _ ->
                %{cpu_pct: 0.0, rss_mb: 0.0}
            end

          _ ->
            %{cpu_pct: 0.0, rss_mb: 0.0}
        end
      rescue
        _ -> %{cpu_pct: 0.0, rss_mb: 0.0}
      end

      defp configure_app(data_dir, shards, max_rss_mb) do
        remove_data_dir(data_dir)
        File.mkdir_p!(data_dir)

        Application.put_env(:libcluster, :topologies, [])
        Application.put_env(:ferricstore, :data_dir, data_dir)
        Application.put_env(:ferricstore, :native_port, 0)
        Application.put_env(:ferricstore, :health_port, 0)
        Application.put_env(:ferricstore, :shard_count, shards)
        Application.put_env(:ferricstore, :protected_mode, false)

        max_memory_bytes = app_memory_budget_bytes(max_rss_mb)

        Application.put_env(:ferricstore, :max_memory_bytes, max_memory_bytes)

        Application.put_env(
          :ferricstore,
          :keydir_max_ram,
          app_keydir_max_ram_bytes(max_memory_bytes)
        )

        Application.put_env(:ferricstore, :memory_guard_interval_ms, 60 * 60 * 1000)
        Application.put_env(:ferricstore, :flow_async_history, true)

        Application.put_env(
          :ferricstore,
          :blob_side_channel_threshold_bytes,
          int_env(
            "BLOB_SIDE_CHANNEL_THRESHOLD_BYTES",
            Application.get_env(:ferricstore, :blob_side_channel_threshold_bytes, 256 * 1024)
          )
        )

        Application.put_env(
          :ferricstore,
          :blob_segment_gc_grace_ms,
          int_env(
            "BLOB_SEGMENT_GC_GRACE_MS",
            Application.get_env(:ferricstore, :blob_segment_gc_grace_ms, 600_000)
          )
        )

        Application.put_env(
          :ferricstore,
          :flow_lmdb_flush_interval_ms,
          int_env("FLOW_LMDB_FLUSH_INTERVAL_MS", 500)
        )

        Application.put_env(
          :ferricstore,
          :flow_lmdb_max_batch_ops,
          int_env("FLOW_LMDB_MAX_BATCH_OPS", 10_000)
        )

        Application.put_env(
          :ferricstore,
          :flow_lmdb_flush_chunk_ops,
          int_env("FLOW_LMDB_FLUSH_CHUNK_OPS", 5_000)
        )

        Application.put_env(
          :ferricstore,
          :flow_lmdb_flush_chunk_pause_ms,
          int_env("FLOW_LMDB_FLUSH_CHUNK_PAUSE_MS", 1)
        )

        Application.put_env(
          :ferricstore,
          :flow_lmdb_flush_jitter_ms,
          int_env("FLOW_LMDB_FLUSH_JITTER_MS", 250)
        )

        Application.put_env(
          :ferricstore,
          :flow_lmdb_max_concurrent_flushes,
          int_env("FLOW_LMDB_MAX_CONCURRENT_FLUSHES", 1)
        )

        put_optional_int_env(
          ["FLOW_LMDB_MAP_SIZE", "FERRICSTORE_FLOW_LMDB_MAP_SIZE"],
          :flow_lmdb_map_size
        )

        if any_env?([
             "FLOW_LMDB_MMAP_RECLAIM_ENABLED",
             "FERRICSTORE_FLOW_LMDB_MMAP_RECLAIM_ENABLED"
           ]) do
          Application.put_env(
            :ferricstore,
            :flow_lmdb_mmap_reclaim_enabled,
            bool_env(
              ["FLOW_LMDB_MMAP_RECLAIM_ENABLED", "FERRICSTORE_FLOW_LMDB_MMAP_RECLAIM_ENABLED"],
              true
            )
          )
        end

        put_optional_int_env(
          [
            "FLOW_LMDB_MMAP_RECLAIM_INTERVAL_MS",
            "FERRICSTORE_FLOW_LMDB_MMAP_RECLAIM_INTERVAL_MS"
          ],
          :flow_lmdb_mmap_reclaim_interval_ms
        )

        put_optional_float_env(
          ["FLOW_LMDB_MMAP_RECLAIM_RSS_RATIO", "FERRICSTORE_FLOW_LMDB_MMAP_RECLAIM_RSS_RATIO"],
          :flow_lmdb_mmap_reclaim_rss_ratio
        )

        put_optional_float_env(
          ["FLOW_CREATE_PAUSE_RSS_RATIO", "FERRICSTORE_FLOW_CREATE_PAUSE_RSS_RATIO"],
          :flow_create_pause_rss_ratio
        )

        put_optional_float_env(
          ["FLOW_CREATE_RESUME_RSS_RATIO", "FERRICSTORE_FLOW_CREATE_RESUME_RSS_RATIO"],
          :flow_create_resume_rss_ratio
        )

        put_optional_limit_env(
          [
            "FLOW_HISTORY_PROJECTOR_MAX_PENDING_ENTRIES",
            "FERRICSTORE_FLOW_HISTORY_PROJECTOR_MAX_PENDING_ENTRIES"
          ],
          :flow_history_projector_max_pending_entries
        )

        put_optional_int_env(
          "FERRICSTORE_FLOW_HISTORY_PROJECTOR_BATCH_SIZE",
          :flow_history_projector_batch_size
        )

        Application.put_env(
          :ferricstore,
          :flow_history_projector_flush_interval_ms,
          int_env("FERRICSTORE_FLOW_HISTORY_PROJECTOR_FLUSH_INTERVAL_MS", 500)
        )

        put_optional_int_env(
          "FERRICSTORE_FLOW_HISTORY_PROJECTOR_CHUNK_INTERVAL_MS",
          :flow_history_projector_chunk_interval_ms
        )

        put_optional_limit_env(
          [
            "FLOW_LMDB_WRITER_MAX_MAILBOX_MESSAGES",
            "FERRICSTORE_FLOW_LMDB_WRITER_MAX_MAILBOX_MESSAGES"
          ],
          :flow_lmdb_writer_max_mailbox_messages
        )

        put_optional_limit_env(
          ["FLOW_LMDB_WRITER_MAX_ENQUEUE_OPS", "FERRICSTORE_FLOW_LMDB_WRITER_MAX_ENQUEUE_OPS"],
          :flow_lmdb_writer_max_enqueue_ops
        )

        if any_env?([
             "FLOW_RETENTION_SWEEPER_ENABLED",
             "FERRICSTORE_FLOW_RETENTION_SWEEPER_ENABLED"
           ]) do
          Application.put_env(
            :ferricstore,
            :flow_retention_sweeper_enabled,
            bool_env(
              ["FLOW_RETENTION_SWEEPER_ENABLED", "FERRICSTORE_FLOW_RETENTION_SWEEPER_ENABLED"],
              true
            )
          )
        end

        put_optional_int_env(
          [
            "FLOW_RETENTION_SWEEPER_INITIAL_DELAY_MS",
            "FERRICSTORE_FLOW_RETENTION_SWEEPER_INITIAL_DELAY_MS"
          ],
          :flow_retention_sweeper_initial_delay_ms
        )

        put_optional_int_env(
          [
            "FLOW_RETENTION_SWEEPER_INTERVAL_MS",
            "FERRICSTORE_FLOW_RETENTION_SWEEPER_INTERVAL_MS"
          ],
          :flow_retention_sweeper_interval_ms
        )

        put_optional_int_env(
          ["FLOW_RETENTION_SWEEPER_LIMIT", "FERRICSTORE_FLOW_RETENTION_SWEEPER_LIMIT"],
          :flow_retention_sweeper_limit
        )

        put_optional_int_env(
          [
            "FLOW_RETENTION_SWEEPER_PRESSURE_INTERVAL_MS",
            "FERRICSTORE_FLOW_RETENTION_SWEEPER_PRESSURE_INTERVAL_MS"
          ],
          :flow_retention_sweeper_pressure_interval_ms
        )

        put_optional_int_env(
          [
            "FLOW_RETENTION_SWEEPER_PRESSURE_LIMIT",
            "FERRICSTORE_FLOW_RETENTION_SWEEPER_PRESSURE_LIMIT"
          ],
          :flow_retention_sweeper_pressure_limit
        )

        put_optional_int_env(
          [
            "FLOW_RETENTION_SWEEPER_PRESSURE_COMPACTION_INTERVAL_MS",
            "FERRICSTORE_FLOW_RETENTION_SWEEPER_PRESSURE_COMPACTION_INTERVAL_MS"
          ],
          :flow_retention_sweeper_pressure_compaction_interval_ms
        )

        Application.delete_env(:ferricstore, :waraft_log_module)

        put_optional_int_env(
          "FERRICSTORE_BLOB_SIDE_CHANNEL_THRESHOLD_BYTES",
          :blob_side_channel_threshold_bytes
        )

        put_optional_int_env(
          ["WARAFT_COMMIT_BATCH_INTERVAL_MS", "FERRICSTORE_WARAFT_COMMIT_BATCH_INTERVAL_MS"],
          :waraft_commit_batch_interval_ms
        )

        put_optional_int_env(
          ["WARAFT_COMMIT_BATCH_MAX", "FERRICSTORE_WARAFT_COMMIT_BATCH_MAX"],
          :waraft_commit_batch_max
        )

        put_optional_int_env(
          ["WARAFT_HOT_BATCH_WINDOW_MS", "FERRICSTORE_WARAFT_HOT_BATCH_WINDOW_MS"],
          :waraft_hot_batch_window_ms
        )

        put_optional_int_env(
          ["WARAFT_GENERIC_BATCH_WINDOW_MS", "FERRICSTORE_WARAFT_GENERIC_BATCH_WINDOW_MS"],
          :waraft_generic_batch_window_ms
        )

        if any_env?([
             "WARAFT_GENERIC_BATCH_DURING_FLUSH",
             "FERRICSTORE_WARAFT_GENERIC_BATCH_DURING_FLUSH"
           ]) do
          Application.put_env(
            :ferricstore,
            :waraft_generic_batch_during_flush,
            bool_env(
              [
                "WARAFT_GENERIC_BATCH_DURING_FLUSH",
                "FERRICSTORE_WARAFT_GENERIC_BATCH_DURING_FLUSH"
              ],
              true
            )
          )
        end

        put_optional_int_env(
          ["WARAFT_APPLY_LOG_BATCH_SIZE", "FERRICSTORE_WARAFT_APPLY_LOG_BATCH_SIZE"],
          :waraft_apply_log_batch_size
        )

        put_optional_int_env(
          ["WARAFT_APPLY_BATCH_MAX_BYTES", "FERRICSTORE_WARAFT_APPLY_BATCH_MAX_BYTES"],
          :waraft_apply_batch_max_bytes
        )

        put_optional_int_env("WARAFT_LOG_ROTATION_INTERVAL", :waraft_log_rotation_interval)
        put_optional_int_env("WARAFT_LOG_ROTATION_KEEP", :waraft_log_rotation_keep)
        put_optional_int_env("WARAFT_MAX_RETAINED_ENTRIES", :waraft_max_retained_entries)

        put_optional_limit_env(
          ["WARAFT_SEGMENT_LOG_MAX_ETS_BYTES", "FERRICSTORE_WARAFT_SEGMENT_LOG_MAX_ETS_BYTES"],
          :waraft_segment_log_max_ets_bytes
        )

        put_optional_limit_env(
          [
            "WARAFT_SEGMENT_LOG_MAX_ETS_ENTRIES",
            "FERRICSTORE_WARAFT_SEGMENT_LOG_MAX_ETS_ENTRIES"
          ],
          :waraft_segment_log_max_ets_entries
        )

        put_optional_limit_env(
          [
            "WARAFT_SEGMENT_LOG_MIN_ETS_ENTRIES",
            "FERRICSTORE_WARAFT_SEGMENT_LOG_MIN_ETS_ENTRIES"
          ],
          :waraft_segment_log_min_ets_entries
        )

        put_optional_int_env(
          "WARAFT_SEGMENT_PREALLOCATE_BYTES",
          :waraft_segment_log_preallocate_bytes
        )

        put_optional_int_env(
          "WARAFT_SEGMENT_RECORDS_PER_SEGMENT",
          :waraft_segment_log_records_per_segment
        )
      end

      defp print_effective_config do
        IO.puts(
          "flow_state_lmdb_soak_effective_config " <>
            "wal_commit_delay_us=#{Application.get_env(:ferricstore, :wal_commit_delay_us)} " <>
            "waraft_commit_batch_interval_ms=#{Ferricstore.Raft.WARaftBackend.default_commit_batch_interval_ms()} " <>
            "waraft_commit_batch_max=#{Ferricstore.Raft.WARaftBackend.default_commit_batch_max()} " <>
            "waraft_hot_batch_window_ms=#{Application.get_env(:ferricstore, :waraft_hot_batch_window_ms, 1)} " <>
            "waraft_generic_batch_window_ms=#{Application.get_env(:ferricstore, :waraft_generic_batch_window_ms, 0)} " <>
            "waraft_generic_batch_during_flush=#{Application.get_env(:ferricstore, :waraft_generic_batch_during_flush, true)} " <>
            "waraft_segment_records_per_segment=#{Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment, 65_536)} " <>
            "waraft_segment_preallocate_bytes=#{Application.get_env(:ferricstore, :waraft_segment_log_preallocate_bytes, 0)} " <>
            "waraft_segment_max_ets_bytes=#{inspect(Application.get_env(:ferricstore, :waraft_segment_log_max_ets_bytes, :adaptive))} " <>
            "waraft_segment_max_ets_entries=#{inspect(Application.get_env(:ferricstore, :waraft_segment_log_max_ets_entries, :adaptive))} " <>
            "waraft_segment_min_ets_entries=#{inspect(Application.get_env(:ferricstore, :waraft_segment_log_min_ets_entries, :adaptive))} " <>
            "waraft_apply_log_batch_size=#{inspect(Application.get_env(:ferricstore, :waraft_apply_log_batch_size, :default))} " <>
            "waraft_apply_batch_max_bytes=#{inspect(Application.get_env(:ferricstore, :waraft_apply_batch_max_bytes, :default))} " <>
            "flow_history_projector_batch_size=#{Application.get_env(:ferricstore, :flow_history_projector_batch_size, 25_000)} " <>
            "flow_history_projector_flush_interval_ms=#{Application.get_env(:ferricstore, :flow_history_projector_flush_interval_ms, 1_000)} " <>
            "max_memory_bytes=#{Application.get_env(:ferricstore, :max_memory_bytes)} " <>
            "keydir_max_ram=#{Application.get_env(:ferricstore, :keydir_max_ram)} " <>
            "flow_lmdb_flush_interval_ms=#{Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)} " <>
            "flow_lmdb_max_batch_ops=#{Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops)} " <>
            "flow_lmdb_flush_chunk_ops=#{Application.get_env(:ferricstore, :flow_lmdb_flush_chunk_ops)} " <>
            "flow_lmdb_map_size=#{Application.get_env(:ferricstore, :flow_lmdb_map_size, :default)} " <>
            "flow_lmdb_mmap_reclaim_enabled=#{Application.get_env(:ferricstore, :flow_lmdb_mmap_reclaim_enabled, true)} " <>
            "flow_lmdb_mmap_reclaim_interval_ms=#{Application.get_env(:ferricstore, :flow_lmdb_mmap_reclaim_interval_ms, :default)} " <>
            "flow_lmdb_mmap_reclaim_rss_ratio=#{Application.get_env(:ferricstore, :flow_lmdb_mmap_reclaim_rss_ratio, :default)} " <>
            "flow_create_pause_rss_ratio=#{Application.get_env(:ferricstore, :flow_create_pause_rss_ratio, :default)} " <>
            "flow_create_resume_rss_ratio=#{Application.get_env(:ferricstore, :flow_create_resume_rss_ratio, :default)} " <>
            "flow_retention_sweeper_initial_delay_ms=#{Application.get_env(:ferricstore, :flow_retention_sweeper_initial_delay_ms)} " <>
            "flow_retention_sweeper_interval_ms=#{Application.get_env(:ferricstore, :flow_retention_sweeper_interval_ms)} " <>
            "flow_retention_sweeper_limit=#{inspect(Application.get_env(:ferricstore, :flow_retention_sweeper_limit, :default))} " <>
            "flow_retention_sweeper_pressure_interval_ms=#{Application.get_env(:ferricstore, :flow_retention_sweeper_pressure_interval_ms)} " <>
            "flow_retention_sweeper_pressure_limit=#{Application.get_env(:ferricstore, :flow_retention_sweeper_pressure_limit)} " <>
            "flow_retention_sweeper_pressure_compaction_interval_ms=#{Application.get_env(:ferricstore, :flow_retention_sweeper_pressure_compaction_interval_ms)}"
        )
      end

      defp print_header do
        IO.puts(
          "sample elapsed_s flow_ops flow_ops_s write_ops write_ops_s create create_attempt create_success create_rejected transition complete fail pipeline_write claim_due cold_due_evicted cold_due_evict_stale cold_due_promoted " <>
            "flow_create_paused flow_create_pause_reason flow_create_retry_after_ms " <>
            "lmdb_pending lmdb_oldest_lag_ms lmdb_replay_lag lmdb_flush_failures lmdb_flushes " <>
            "lmdb_flush_avg_us waraft_flush_errors waraft_apply_full waraft_commit_bytes_rejected " <>
            "waraft_commit_timeouts waraft_commit_timeout_max_us " <>
            "waraft_flushes waraft_queue_wait_avg_us waraft_queue_wait_max_us " <>
            "waraft_flush_duration_avg_us waraft_flush_duration_max_us " <>
            "waraft_total_duration_avg_us waraft_total_duration_max_us " <>
            "payload_fsyncs payload_fsync_avg_us payload_fsync_max_us payload_fsync_errors " <>
            "blob_prepare_failures storage_blocked " <>
            "segment_appends segment_mb raft_segment_append_avg_us raft_segment_append_max_us " <>
            "projection_segment_append_avg_us projection_segment_append_max_us " <>
            "apply_projection_segment_append_avg_us apply_projection_segment_append_max_us " <>
            "projection_overlap projection_overlap_rebuilds projection_overlap_avg_us projection_overlap_max_us " <>
            "acceptor_commit_avg_us acceptor_commit_max_us leader_apply_avg_us leader_apply_max_us " <>
            "apply_log_avg_us apply_log_max_us storage_apply_avg_us storage_apply_max_us " <>
            "storage_phase_cache_avg_us storage_phase_cache_max_us " <>
            "storage_phase_recovery_avg_us storage_phase_recovery_max_us " <>
            "storage_phase_metadata_avg_us storage_phase_metadata_max_us " <>
            "apply_queue_avg apply_queue_max " <>
            "hot_batch_flushes hot_batch_avg_items hot_batch_max_items hot_batch_avg_groups " <>
            "hot_batch_queue_max_us hot_batch_flush_max_us hot_batch_total_max_us " <>
            "disk_mb disk_growth_mb_s blob_mb blob_files lmdb_mb " <>
            "data_mb waraft_mb waraft_log_entries waraft_log_ets_mb " <>
            "free_mb mem_total_mb beam_rss_mb beam_cpu_pct mem_binary_mb ets_mb processes run_queue " <>
            "keydir_entries keydir_binary_mb keydir_state keydir_history keydir_value " <>
            "keydir_flow_other keydir_other flow_index_entries flow_lookup_entries"
        )
      end

      defp init_table(table) do
        case :ets.whereis(table) do
          :undefined -> :ets.new(table, [:named_table, :public, :set])
          _ -> :ets.delete_all_objects(table)
        end
      end

      defp telemetry_table, do: :flow_state_lmdb_soak_telemetry

      defp duration_us(%{duration_us: value}) when is_integer(value), do: value

      defp duration_us(%{duration: value}) when is_integer(value),
        do: System.convert_time_unit(value, :native, :microsecond)

      defp duration_us(%{duration_ms: value}) when is_integer(value), do: value * 1_000

      defp duration_us(_), do: 0

      defp max_metric_us(%{queue_wait_us: value}, _duration_us) when is_integer(value), do: value
      defp max_metric_us(_measurements, duration_us), do: duration_us

      defp item_count(%{count: count}) when is_integer(count) and count >= 0, do: count
      defp item_count(%{batch_size: count}) when is_integer(count) and count >= 0, do: count
      defp item_count(_), do: 1

      defp bytes(%{bytes: bytes}) when is_integer(bytes) and bytes >= 0, do: bytes
      defp bytes(%{current_bytes: bytes}) when is_integer(bytes) and bytes >= 0, do: bytes
      defp bytes(_), do: 0

      defp update_max(table, key, value) when is_integer(value) and value >= 0 do
        case :ets.lookup(table, key) do
          [{^key, current}] when is_integer(current) and current >= value ->
            :ok

          _ ->
            :ets.insert(table, {key, value})
        end
      end

      defp update_max(_table, _key, _value), do: :ok

      defp disk_growth_mb_s(previous_mb, current_mb, previous_at, now) do
        elapsed = System.convert_time_unit(now - previous_at, :native, :millisecond) / 1000
        if elapsed > 0, do: max(current_mb - previous_mb, 0) / elapsed, else: 0.0
      end

      defp dir_mb(path), do: bytes_to_mb(dir_bytes(path))

      defp storage_breakdown(data_dir) do
        blob_stats =
          case Ferricstore.Store.BlobStore.storage_stats(data_dir) do
            {:ok, stats} -> stats
            _ -> %{files: 0, bytes: 0}
          end

        %{
          blob_mb: bytes_to_mb(Map.get(blob_stats, :bytes, 0)),
          blob_files: Map.get(blob_stats, :files, 0),
          lmdb_mb: data_dir |> Path.join("data/shard_*/flow_lmdb") |> wildcard_dir_mb(),
          data_mb: dir_mb(Path.join(data_dir, "data")),
          waraft_mb: dir_mb(Path.join(data_dir, "waraft"))
        }
      end

      defp wildcard_dir_mb(pattern) do
        pattern
        |> Path.wildcard()
        |> Enum.reduce(0.0, fn path, acc -> acc + dir_mb(path) end)
      end

      defp dir_bytes(path) when is_binary(path) do
        case System.cmd("du", ["-sk", path], stderr_to_stdout: true) do
          {output, 0} ->
            output |> String.split() |> List.first() |> String.to_integer() |> Kernel.*(1024)

          _ ->
            0
        end
      rescue
        _ -> 0
      end

      defp disk_free_mb(path) do
        path = Path.expand(path || System.tmp_dir!())
        probe = if File.exists?(path), do: path, else: Path.dirname(path)

        case System.cmd("df", ["-k", probe], stderr_to_stdout: true) do
          {output, 0} ->
            output
            |> String.split("\n", trim: true)
            |> List.last()
            |> String.split()
            |> Enum.at(3)
            |> String.to_integer()
            |> div(1024)

          _ ->
            0
        end
      rescue
        _ -> 0
      end

      defp bytes_to_mb(bytes), do: bytes / 1_048_576

      defp elapsed_s(started),
        do:
          System.convert_time_unit(System.monotonic_time() - started, :native, :millisecond) /
            1000

      defp keep_tail(binary, max) when byte_size(binary) <= max, do: binary
      defp keep_tail(binary, max), do: binary_part(binary, byte_size(binary) - max, max)

      defp stop_started_apps do
        for app <- [:ferricstore_server, :ferricstore] do
          _ = Application.stop(app)
        end
      end

      defp maybe_keep_server_running(data_dir, redis_port) do
        if truthy_env?("KEEP_SERVER_RUNNING") do
          health_port = FerricstoreServer.Health.Endpoint.port()

          IO.puts(
            "keep_server_running=true redis_port=#{redis_port} health_port=#{health_port} " <>
              "dashboard_url=http://127.0.0.1:#{health_port}/dashboard data_dir=#{data_dir}"
          )

          receive do
          after
            :infinity -> :ok
          end
        end
      end

      defp remove_data_dir(data_dir) do
        if System.get_env("KEEP_DATA_DIR") in ["1", "true", "TRUE", "yes", "YES"] do
          :ok
        else
          case System.cmd("rm", ["-rf", data_dir], stderr_to_stdout: true) do
            {_output, 0} ->
              :ok

            {output, status} ->
              IO.puts("cleanup_failed status=#{status} output=#{inspect(output)}")
          end
        end
      end

      defp python, do: env("PYTHON", Path.join(sdk_dir(), ".venv/bin/python"))
      defp sdk_dir, do: env("SDK_DIR", "/Users/yoavgea/repos/ferricstore-python")
      defp env(name, default), do: System.get_env(name) || default
      defp truthy_env?(name), do: System.get_env(name) in ["1", "true", "TRUE", "yes", "YES"]
      defp unique, do: System.unique_integer([:positive])

      defp int_env(name, default) do
        name |> env(Integer.to_string(default)) |> String.to_integer()
      end

      defp app_memory_budget_bytes(max_total_mem_mb) do
        case System.get_env("FERRICSTORE_MAX_MEMORY") do
          nil -> max_total_mem_mb * 1024 * 1024
          "auto" -> max_total_mem_mb * 1024 * 1024
          value -> String.to_integer(value)
        end
      end

      defp app_keydir_max_ram_bytes(max_memory_bytes) do
        case System.get_env("FERRICSTORE_KEYDIR_MAX_RAM") do
          nil when max_memory_bytes <= 0 ->
            256 * 1024 * 1024

          nil ->
            max(256 * 1024 * 1024, min(div(max_memory_bytes, 10), 8 * 1024 * 1024 * 1024))

          value ->
            String.to_integer(value)
        end
      end

      defp parse_float(value) do
        case Float.parse(value) do
          {float, _rest} ->
            float

          :error ->
            0.0
        end
      end

      defp bool_env(name, default) when is_binary(name) do
        case System.get_env(name) do
          nil -> default
          value when value in ["1", "true", "TRUE", "yes", "YES", "on", "ON"] -> true
          value when value in ["0", "false", "FALSE", "no", "NO", "off", "OFF"] -> false
          value -> raise "unsupported #{name}=#{inspect(value)}; expected boolean"
        end
      end

      defp bool_env(names, default) when is_list(names) do
        case first_env_value(names) do
          nil -> default
          {_name, value} when value in ["1", "true", "TRUE", "yes", "YES", "on", "ON"] -> true
          {_name, value} when value in ["0", "false", "FALSE", "no", "NO", "off", "OFF"] -> false
          {name, value} -> raise "unsupported #{name}=#{inspect(value)}; expected boolean"
        end
      end

      defp any_env?(names), do: first_env_value(names) != nil

      defp put_optional_int_env(env_names, app_key) when is_list(env_names) do
        case first_env_value(env_names) do
          nil ->
            :ok

          {_env_name, value} ->
            Application.put_env(:ferricstore, app_key, String.to_integer(value))
        end
      end

      defp put_optional_int_env(env_name, app_key) do
        case System.get_env(env_name) do
          nil -> :ok
          value -> Application.put_env(:ferricstore, app_key, String.to_integer(value))
        end
      end

      defp put_optional_float_env(env_names, app_key) when is_list(env_names) do
        case first_env_value(env_names) do
          nil -> :ok
          {_env_name, value} -> Application.put_env(:ferricstore, app_key, parse_float(value))
        end
      end

      defp put_optional_limit_env(env_names, app_key) when is_list(env_names) do
        case first_env_value(env_names) do
          nil -> :ok
          {_env_name, value} -> Application.put_env(:ferricstore, app_key, parse_limit_env(value))
        end
      end

      defp first_env_value(names) do
        Enum.find_value(names, fn env_name ->
          case System.get_env(env_name) do
            nil -> nil
            value -> {env_name, value}
          end
        end)
      end

      defp parse_limit_env(value) do
        case String.downcase(String.trim(value)) do
          value when value in ["", "false", "off", "infinity", "inf", "unlimited"] ->
            :infinity

          value ->
            String.to_integer(value)
        end
      end
    end
  end
end
