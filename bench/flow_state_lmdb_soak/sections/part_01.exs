defmodule FlowStateLMDBSoak.Sections.Part01 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      defp estimated_payload_bytes(flows, steps, payload_bytes) do
        flows * (steps + 1) * payload_bytes
      end

      defp start_python_workload(name, port, opts) do
        payload_bytes = Keyword.fetch!(opts, :payload_bytes)
        create_rate_per_sec = default_create_rate_per_sec(opts)

        args =
          [
            "examples/async_state_machine_workflow_benchmark.py",
            "--url",
            "redis://127.0.0.1:#{port}/0",
            "--shape",
            env("SHAPE", "live"),
            "--flows",
            Integer.to_string(Keyword.fetch!(opts, :flows)),
            "--steps",
            Integer.to_string(Keyword.fetch!(opts, :steps)),
            "--workers",
            env("WORKERS", "32"),
            "--producers",
            env("PRODUCERS", "8"),
            "--partitions",
            env("PARTITIONS", "1024"),
            "--partition-mode",
            env("PARTITION_MODE", "auto"),
            "--create-mode",
            env("CREATE_MODE", "many"),
            "--create-batch-size",
            env(
              "CREATE_BATCH_SIZE",
              Integer.to_string(default_create_batch_size(create_rate_per_sec))
            ),
            "--create-inflight",
            env("CREATE_INFLIGHT", "64"),
            "--create-rate-per-sec",
            env("CREATE_RATE_PER_SEC", Integer.to_string(create_rate_per_sec)),
            "--claim-batch-size",
            env("CLAIM_BATCH_SIZE", "1000"),
            "--claim-partition-batch-size",
            env("CLAIM_PARTITION_BATCH_SIZE", "32"),
            "--claim-block-ms",
            env("CLAIM_BLOCK_MS", "5000"),
            "--apply-inflight",
            env("APPLY_INFLIGHT", "8"),
            "--worker-mode",
            Keyword.get(opts, :worker_mode, env("WORKER_MODE", "blocking")),
            "--claim-states-mode",
            Keyword.fetch!(opts, :claim_states_mode),
            "--idle-sleep-ms",
            env("IDLE_SLEEP_MS", "1"),
            "--max-idle-sleep-ms",
            env("MAX_IDLE_SLEEP_MS", "10"),
            "--server-shards",
            env("SHARDS", "16"),
            "--payload-bytes",
            Integer.to_string(payload_bytes),
            "--transition-payload-bytes",
            Integer.to_string(payload_bytes),
            "--terminal-payload-bytes",
            Integer.to_string(payload_bytes),
            "--retention-ttl-ms",
            env("TERMINAL_RETENTION_TTL_MS", "0"),
            "--run-at-delay-ms",
            env("RUN_AT_DELAY_MS", "0"),
            "--progress-interval-s",
            env("PROGRESS_INTERVAL_SECONDS", "0"),
            "--terminal-mode",
            Keyword.fetch!(opts, :terminal_mode),
            "--result-bytes",
            "0"
          ]
          |> optional_cli_arg("CREATE_NOW_MS", "--create-now-ms")
          |> optional_cli_arg("CLAIM_NOW_MS", "--claim-now-ms")

        port_ref =
          Port.open({:spawn_executable, python()}, [
            :binary,
            :exit_status,
            :stderr_to_stdout,
            {:cd, sdk_dir()},
            {:args, args}
          ])

        %{name: name, port: port_ref, status: :running, output: ""}
      end

      defp default_create_rate_per_sec(opts) do
        max(div(int_env("TARGET_OPS_PER_SEC", 50_000), Keyword.fetch!(opts, :steps) + 1), 1)
      end

      defp optional_cli_arg(args, env_name, flag) do
        case System.get_env(env_name) do
          nil -> args
          "" -> args
          value -> args ++ [flag, value]
        end
      end

      # Product workers use server-side BLOCK claims against any ready state. Keep
      # the soak default on that path so a clean run does not require hidden flags
      # to match the production SDK behavior. The env knobs remain for explicit
      # comparison runs against older cursor/polling workloads.
      defp normal_claim_states_mode, do: env("NORMAL_CLAIM_STATES_MODE", "any")
      defp long_claim_states_mode, do: env("LONG_CLAIM_STATES_MODE", "any")

      defp normal_worker_mode, do: env("NORMAL_WORKER_MODE", env("WORKER_MODE", "blocking"))

      defp long_worker_mode, do: env("LONG_WORKER_MODE", env("WORKER_MODE", "blocking"))

      defp default_create_batch_size(create_rate_per_sec) do
        partition_mode = env("PARTITION_MODE", "auto")
        create_mode = env("CREATE_MODE", "many")

        if partition_mode == "auto" and create_mode == "many" do
          # The Python benchmark buffers CREATE_MANY by auto partition. With the old
          # fixed 1000-item bucket, a 50-step soak at 50K target ops only generated
          # about 980 creates/sec, so a partition could wait minutes before a batch
          # flushed and woke workers. Bound the expected per-partition flush delay
          # while keeping larger batches on higher create-rate workloads.
          flush_target_ms = int_env("CREATE_AUTO_FLUSH_TARGET_MS", 5_000)
          buckets = 256

          create_rate_per_sec
          |> Kernel.*(flush_target_ms)
          |> div(1_000 * buckets)
          |> max(1)
          |> min(1_000)
        else
          1_000
        end
      end

      defp sample_loop(state) do
        receive do
          {port, {:data, data}} ->
            sample_loop(update_output(state, port, data))

          {port, {:exit_status, status}} ->
            state
            |> mark_port_exit(port, status)
            |> maybe_finish_or_continue()
        after
          state.sample_interval_ms ->
            state
            |> sample_once()
            |> guard_resources()
            |> maybe_finish_or_continue()
        end
      end

      defp maybe_finish_or_continue(state) do
        cond do
          Enum.all?(state.ports, &(&1.status != :running)) ->
            sample_once(state)

          System.monotonic_time() >= state.deadline_native ->
            kill_running_ports(state)
            sample_once(%{state | ports: mark_running_stopped(state.ports, :duration_reached)})

          true ->
            sample_loop(state)
        end
      end

      defp guard_resources(state) do
        memory = memory_status()
        total_mem_mb = memory.total_mb
        rss_mb = rss_guard_mb(memory)
        free_mb = disk_free_mb(state.data_dir)

        cond do
          free_mb <= state.min_free_mb ->
            IO.puts("guard_stop reason=low_disk free_mb=#{free_mb}")
            kill_running_ports(state)
            %{state | ports: mark_running_stopped(state.ports, :low_disk)}

          rss_mb >= state.max_rss_mb ->
            IO.puts(
              "guard_stop reason=high_rss rss_mb=#{Float.round(rss_mb, 1)} " <>
                "max_rss_mb=#{state.max_rss_mb} beam_total_mb=#{Float.round(total_mem_mb, 1)}"
            )

            kill_running_ports(state)
            %{state | ports: mark_running_stopped(state.ports, :high_rss)}

          true ->
            state
        end
      end

      defp sample_once(state) do
        lmdb = lmdb_status()
        production_health = production_health_status()
        disk_mb = dir_mb(state.data_dir)
        free_mb = disk_free_mb(state.data_dir)
        storage = storage_breakdown(state.data_dir)
        now = System.monotonic_time()
        elapsed_s = elapsed_s(state.started_native)
        disk_growth_mb_s = disk_growth_mb_s(state.last_disk_mb, disk_mb, state.last_disk_at, now)
        memory = memory_status()
        keydir = keydir_status()
        flow_index = flow_index_status()
        waraft_log = waraft_log_status()
        admission = flow_admission_status()
        telemetry = telemetry_snapshot(state.table)
        sample_count = Map.get(state, :sample_count, 0) + 1

        IO.puts(
          "sample elapsed_s=#{Float.round(elapsed_s, 1)} " <>
            "flow_ops=#{telemetry.flow_ops} flow_ops_s=#{Float.round(telemetry.flow_ops / max(elapsed_s, 0.001), 1)} " <>
            "write_ops=#{telemetry.write_ops} write_ops_s=#{Float.round(telemetry.write_ops / max(elapsed_s, 0.001), 1)} " <>
            "create=#{telemetry.create} create_attempt=#{telemetry.create_attempt} " <>
            "create_success=#{telemetry.create_success} create_rejected=#{telemetry.create_rejected} " <>
            "transition=#{telemetry.transition} complete=#{telemetry.complete} " <>
            "fail=#{telemetry.fail} pipeline_write=#{telemetry.pipeline_write} claim_due=#{telemetry.claim_due} " <>
            "cold_due_evicted=#{telemetry.cold_due_evicted} " <>
            "cold_due_evict_stale=#{telemetry.cold_due_evict_stale} " <>
            "cold_due_promoted=#{telemetry.cold_due_promoted} " <>
            "flow_create_paused=#{admission.paused} flow_create_pause_reason=#{admission.reason} " <>
            "flow_create_retry_after_ms=#{admission.retry_after_ms} " <>
            "lmdb_pending=#{lmdb.pending_ops} lmdb_oldest_lag_ms=#{Float.round(lmdb.max_oldest_lag_ms, 2)} " <>
            "lmdb_replay_lag=#{lmdb.max_replay_safe_lag} lmdb_flush_failures=#{lmdb.flush_failures} " <>
            "history_pending=#{production_health.history_pending_entries} " <>
            "history_oldest_lag_ms=#{Float.round(production_health.history_oldest_lag_ms, 2)} " <>
            "history_projection_lag=#{production_health.history_projection_lag} " <>
            "history_flush_failures=#{production_health.history_flush_failures} " <>
            "history_queue_full=#{production_health.history_queue_full} " <>
            "blob_hardened=#{production_health.blob_hardened_count} " <>
            "blob_hardened_oldest_ms=#{production_health.blob_hardened_oldest_ms} " <>
            "release_cursor_gap=#{production_health.release_cursor_gap} " <>
            "lmdb_flushes=#{telemetry.lmdb_flushes} lmdb_flush_avg_us=#{telemetry.lmdb_flush_avg_us} " <>
            "waraft_flush_errors=#{telemetry.waraft_flush_errors} waraft_apply_full=#{telemetry.waraft_apply_full} " <>
            "waraft_commit_bytes_rejected=#{telemetry.waraft_commit_bytes_rejected} " <>
            "waraft_commit_timeouts=#{telemetry.waraft_commit_timeouts} " <>
            "waraft_commit_timeout_max_us=#{telemetry.waraft_commit_timeout_max_us} " <>
            "waraft_flushes=#{telemetry.waraft_flushes} waraft_queue_wait_avg_us=#{telemetry.waraft_queue_wait_avg_us} " <>
            "waraft_queue_wait_max_us=#{telemetry.waraft_queue_wait_max_us} " <>
            "waraft_flush_duration_avg_us=#{telemetry.waraft_flush_duration_avg_us} " <>
            "waraft_flush_duration_max_us=#{telemetry.waraft_flush_duration_max_us} " <>
            "waraft_total_duration_avg_us=#{telemetry.waraft_total_duration_avg_us} " <>
            "waraft_total_duration_max_us=#{telemetry.waraft_total_duration_max_us} " <>
            "payload_fsyncs=#{telemetry.payload_fsyncs} payload_fsync_avg_us=#{telemetry.payload_fsync_avg_us} " <>
            "payload_fsync_max_us=#{telemetry.payload_fsync_max_us} payload_fsync_errors=#{telemetry.payload_fsync_errors} " <>
            "blob_prepare_failures=#{telemetry.blob_prepare_failures} storage_blocked=#{telemetry.storage_blocked} " <>
            "segment_appends=#{telemetry.segment_appends} segment_mb=#{Float.round(telemetry.segment_mb, 1)} " <>
            "raft_segment_append_avg_us=#{telemetry.raft_segment_append_avg_us} raft_segment_append_max_us=#{telemetry.raft_segment_append_max_us} " <>
            "projection_segment_append_avg_us=#{telemetry.projection_segment_append_avg_us} projection_segment_append_max_us=#{telemetry.projection_segment_append_max_us} " <>
            "apply_projection_segment_append_avg_us=#{telemetry.apply_projection_segment_append_avg_us} apply_projection_segment_append_max_us=#{telemetry.apply_projection_segment_append_max_us} " <>
            "projection_overlap=#{telemetry.projection_overlap} projection_overlap_rebuilds=#{telemetry.projection_overlap_rebuilds} " <>
            "projection_overlap_avg_us=#{telemetry.projection_overlap_avg_us} projection_overlap_max_us=#{telemetry.projection_overlap_max_us} " <>
            "acceptor_commit_avg_us=#{telemetry.acceptor_commit_avg_us} acceptor_commit_max_us=#{telemetry.acceptor_commit_max_us} " <>
            "leader_apply_avg_us=#{telemetry.leader_apply_avg_us} leader_apply_max_us=#{telemetry.leader_apply_max_us} " <>
            "apply_log_avg_us=#{telemetry.apply_log_avg_us} apply_log_max_us=#{telemetry.apply_log_max_us} " <>
            "storage_apply_avg_us=#{telemetry.storage_apply_avg_us} storage_apply_max_us=#{telemetry.storage_apply_max_us} " <>
            "storage_phase_cache_avg_us=#{telemetry.storage_phase_cache_avg_us} storage_phase_cache_max_us=#{telemetry.storage_phase_cache_max_us} " <>
            "storage_phase_recovery_avg_us=#{telemetry.storage_phase_recovery_avg_us} storage_phase_recovery_max_us=#{telemetry.storage_phase_recovery_max_us} " <>
            "storage_phase_metadata_avg_us=#{telemetry.storage_phase_metadata_avg_us} storage_phase_metadata_max_us=#{telemetry.storage_phase_metadata_max_us} " <>
            "apply_queue_avg=#{telemetry.apply_queue_avg} apply_queue_max=#{telemetry.apply_queue_max} " <>
            "hot_batch_flushes=#{telemetry.hot_batch_flushes} hot_batch_avg_items=#{telemetry.hot_batch_avg_items} " <>
            "hot_batch_max_items=#{telemetry.hot_batch_max_items} hot_batch_avg_groups=#{telemetry.hot_batch_avg_groups} " <>
            "hot_batch_queue_max_us=#{telemetry.hot_batch_queue_max_us} hot_batch_flush_max_us=#{telemetry.hot_batch_flush_max_us} " <>
            "hot_batch_total_max_us=#{telemetry.hot_batch_total_max_us} " <>
            "disk_mb=#{Float.round(disk_mb, 1)} disk_growth_mb_s=#{Float.round(disk_growth_mb_s, 2)} " <>
            "blob_mb=#{Float.round(storage.blob_mb, 1)} blob_files=#{storage.blob_files} " <>
            "lmdb_mb=#{Float.round(storage.lmdb_mb, 1)} data_mb=#{Float.round(storage.data_mb, 1)} " <>
            "waraft_mb=#{Float.round(storage.waraft_mb, 1)} " <>
            "waraft_log_entries=#{waraft_log.entries} " <>
            "waraft_log_ets_mb=#{Float.round(waraft_log.ets_mb, 1)} " <>
            "free_mb=#{free_mb} mem_total_mb=#{Float.round(memory.total_mb, 1)} " <>
            "beam_rss_mb=#{Float.round(memory.rss_mb, 1)} beam_cpu_pct=#{Float.round(memory.cpu_pct, 1)} " <>
            "mem_binary_mb=#{Float.round(memory.binary_mb, 1)} ets_mb=#{Float.round(memory.ets_mb, 1)} " <>
            "processes=#{memory.process_count} run_queue=#{memory.run_queue} " <>
            "keydir_entries=#{keydir.entries} keydir_binary_mb=#{Float.round(keydir.binary_mb, 1)} " <>
            "keydir_state=#{keydir.state} keydir_history=#{keydir.history} " <>
            "keydir_value=#{keydir.value} keydir_flow_other=#{keydir.flow_other} " <>
            "keydir_other=#{keydir.other} flow_index_entries=#{flow_index.index_entries} " <>
            "flow_lookup_entries=#{flow_index.lookup_entries}"
        )

        print_flow_latency_line(
          telemetry.flow_latency,
          "latency_ms",
          telemetry.flow_latency_sample_rate
        )

        maybe_print_top_binary_holders(sample_count)
        maybe_print_top_ets_memory_tables(sample_count)
        maybe_print_top_ets_binary_tables(sample_count)
        process_profile = maybe_print_process_profile(state.process_profile, sample_count)

        %{
          state
          | last_disk_mb: disk_mb,
            last_disk_at: now,
            max_pending_ops: max(state.max_pending_ops, lmdb.pending_ops),
            max_oldest_lag_ms: max(state.max_oldest_lag_ms, lmdb.max_oldest_lag_ms),
            max_replay_lag: max(state.max_replay_lag, lmdb.max_replay_safe_lag),
            max_history_pending_entries:
              max(state.max_history_pending_entries, production_health.history_pending_entries),
            max_history_oldest_lag_ms:
              max(state.max_history_oldest_lag_ms, production_health.history_oldest_lag_ms),
            max_history_projection_lag:
              max(state.max_history_projection_lag, production_health.history_projection_lag),
            max_history_flush_failures:
              max(state.max_history_flush_failures, production_health.history_flush_failures),
            max_history_queue_full:
              max(state.max_history_queue_full, production_health.history_queue_full),
            max_blob_hardened:
              max(state.max_blob_hardened, production_health.blob_hardened_count),
            max_blob_hardened_oldest_ms:
              max(state.max_blob_hardened_oldest_ms, production_health.blob_hardened_oldest_ms),
            max_release_cursor_gap:
              max(state.max_release_cursor_gap, production_health.release_cursor_gap),
            max_disk_mb: max(state.max_disk_mb, disk_mb),
            max_blob_mb: max(state.max_blob_mb, storage.blob_mb),
            max_lmdb_mb: max(state.max_lmdb_mb, storage.lmdb_mb),
            max_waraft_mb: max(state.max_waraft_mb, storage.waraft_mb),
            max_waraft_log_entries: max(state.max_waraft_log_entries, waraft_log.entries),
            max_waraft_log_ets_mb: max(state.max_waraft_log_ets_mb, waraft_log.ets_mb),
            max_total_mem_mb_seen: max(state.max_total_mem_mb_seen, memory.total_mb),
            max_rss_mb_seen: max(state.max_rss_mb_seen, rss_guard_mb(memory)),
            max_binary_mem_mb: max(state.max_binary_mem_mb, memory.binary_mb),
            max_keydir_binary_mb: max(state.max_keydir_binary_mb, keydir.binary_mb),
            max_flow_index_entries: max(state.max_flow_index_entries, flow_index.index_entries),
            max_flow_lookup_entries:
              max(state.max_flow_lookup_entries, flow_index.lookup_entries),
            sample_count: sample_count,
            process_profile: process_profile
        }
      end

      defp print_summary(state) do
        telemetry = telemetry_snapshot(state.table)

        IO.puts(
          "summary ports=#{inspect(Enum.map(state.ports, &{&1.name, &1.status}))} " <>
            "flow_ops=#{telemetry.flow_ops} write_ops=#{telemetry.write_ops} " <>
            "create=#{telemetry.create} create_attempt=#{telemetry.create_attempt} " <>
            "create_success=#{telemetry.create_success} create_rejected=#{telemetry.create_rejected} " <>
            "transition=#{telemetry.transition} " <>
            "complete=#{telemetry.complete} fail=#{telemetry.fail} pipeline_write=#{telemetry.pipeline_write} " <>
            "claim_due=#{telemetry.claim_due} " <>
            "cold_due_evicted=#{telemetry.cold_due_evicted} " <>
            "cold_due_evict_stale=#{telemetry.cold_due_evict_stale} " <>
            "cold_due_promoted=#{telemetry.cold_due_promoted} " <>
            "max_lmdb_pending=#{state.max_pending_ops} max_lmdb_oldest_lag_ms=#{Float.round(state.max_oldest_lag_ms, 2)} " <>
            "max_lmdb_replay_lag=#{state.max_replay_lag} max_disk_mb=#{Float.round(state.max_disk_mb, 1)} " <>
            "max_history_pending=#{state.max_history_pending_entries} " <>
            "max_history_oldest_lag_ms=#{Float.round(state.max_history_oldest_lag_ms, 2)} " <>
            "max_history_projection_lag=#{state.max_history_projection_lag} " <>
            "max_history_flush_failures=#{state.max_history_flush_failures} " <>
            "max_history_queue_full=#{state.max_history_queue_full} " <>
            "max_blob_hardened=#{state.max_blob_hardened} " <>
            "max_blob_hardened_oldest_ms=#{state.max_blob_hardened_oldest_ms} " <>
            "max_release_cursor_gap=#{state.max_release_cursor_gap} " <>
            "max_blob_mb=#{Float.round(state.max_blob_mb, 1)} max_lmdb_mb=#{Float.round(state.max_lmdb_mb, 1)} " <>
            "max_waraft_mb=#{Float.round(state.max_waraft_mb, 1)} " <>
            "max_waraft_log_entries=#{state.max_waraft_log_entries} " <>
            "max_waraft_log_ets_mb=#{Float.round(state.max_waraft_log_ets_mb, 1)} " <>
            "max_total_mem_mb=#{Float.round(state.max_total_mem_mb_seen, 1)} " <>
            "max_rss_mb=#{Float.round(state.max_rss_mb_seen, 1)} " <>
            "max_binary_mem_mb=#{Float.round(state.max_binary_mem_mb, 1)} " <>
            "max_keydir_binary_mb=#{Float.round(state.max_keydir_binary_mb, 1)} " <>
            "max_flow_index_entries=#{state.max_flow_index_entries} " <>
            "max_flow_lookup_entries=#{state.max_flow_lookup_entries} " <>
            "waraft_flush_errors=#{telemetry.waraft_flush_errors} waraft_apply_full=#{telemetry.waraft_apply_full} " <>
            "waraft_commit_timeouts=#{telemetry.waraft_commit_timeouts} " <>
            "waraft_commit_timeout_max_us=#{telemetry.waraft_commit_timeout_max_us} " <>
            "waraft_flushes=#{telemetry.waraft_flushes} waraft_queue_wait_avg_us=#{telemetry.waraft_queue_wait_avg_us} " <>
            "waraft_queue_wait_max_us=#{telemetry.waraft_queue_wait_max_us} " <>
            "waraft_flush_duration_avg_us=#{telemetry.waraft_flush_duration_avg_us} " <>
            "waraft_flush_duration_max_us=#{telemetry.waraft_flush_duration_max_us} " <>
            "waraft_total_duration_avg_us=#{telemetry.waraft_total_duration_avg_us} " <>
            "waraft_total_duration_max_us=#{telemetry.waraft_total_duration_max_us} " <>
            "payload_fsyncs=#{telemetry.payload_fsyncs} payload_fsync_avg_us=#{telemetry.payload_fsync_avg_us} " <>
            "payload_fsync_max_us=#{telemetry.payload_fsync_max_us} payload_fsync_errors=#{telemetry.payload_fsync_errors} " <>
            "blob_prepare_failures=#{telemetry.blob_prepare_failures} storage_blocked=#{telemetry.storage_blocked} " <>
            "segment_appends=#{telemetry.segment_appends} segment_mb=#{Float.round(telemetry.segment_mb, 1)} " <>
            "raft_segment_append_avg_us=#{telemetry.raft_segment_append_avg_us} raft_segment_append_max_us=#{telemetry.raft_segment_append_max_us} " <>
            "projection_segment_append_avg_us=#{telemetry.projection_segment_append_avg_us} projection_segment_append_max_us=#{telemetry.projection_segment_append_max_us} " <>
            "apply_projection_segment_append_avg_us=#{telemetry.apply_projection_segment_append_avg_us} apply_projection_segment_append_max_us=#{telemetry.apply_projection_segment_append_max_us} " <>
            "projection_overlap=#{telemetry.projection_overlap} projection_overlap_rebuilds=#{telemetry.projection_overlap_rebuilds} " <>
            "projection_overlap_avg_us=#{telemetry.projection_overlap_avg_us} projection_overlap_max_us=#{telemetry.projection_overlap_max_us} " <>
            "acceptor_commit_avg_us=#{telemetry.acceptor_commit_avg_us} acceptor_commit_max_us=#{telemetry.acceptor_commit_max_us} " <>
            "leader_apply_avg_us=#{telemetry.leader_apply_avg_us} leader_apply_max_us=#{telemetry.leader_apply_max_us} " <>
            "apply_log_avg_us=#{telemetry.apply_log_avg_us} apply_log_max_us=#{telemetry.apply_log_max_us} " <>
            "storage_apply_avg_us=#{telemetry.storage_apply_avg_us} storage_apply_max_us=#{telemetry.storage_apply_max_us} " <>
            "storage_phase_cache_avg_us=#{telemetry.storage_phase_cache_avg_us} storage_phase_cache_max_us=#{telemetry.storage_phase_cache_max_us} " <>
            "storage_phase_recovery_avg_us=#{telemetry.storage_phase_recovery_avg_us} storage_phase_recovery_max_us=#{telemetry.storage_phase_recovery_max_us} " <>
            "storage_phase_metadata_avg_us=#{telemetry.storage_phase_metadata_avg_us} storage_phase_metadata_max_us=#{telemetry.storage_phase_metadata_max_us} " <>
            "apply_queue_avg=#{telemetry.apply_queue_avg} apply_queue_max=#{telemetry.apply_queue_max} " <>
            "hot_batch_flushes=#{telemetry.hot_batch_flushes} hot_batch_avg_items=#{telemetry.hot_batch_avg_items} " <>
            "hot_batch_max_items=#{telemetry.hot_batch_max_items} hot_batch_avg_groups=#{telemetry.hot_batch_avg_groups} " <>
            "hot_batch_queue_max_us=#{telemetry.hot_batch_queue_max_us} hot_batch_flush_max_us=#{telemetry.hot_batch_flush_max_us} " <>
            "hot_batch_total_max_us=#{telemetry.hot_batch_total_max_us}"
        )

        print_flow_latency_line(
          telemetry.flow_latency,
          "summary_latency_ms",
          telemetry.flow_latency_sample_rate
        )

        Enum.each(state.ports, fn port_state ->
          IO.puts("python_output name=#{port_state.name} status=#{inspect(port_state.status)}")
          IO.write(port_state.output)
          IO.puts("")
        end)
      end

      defp update_output(state, port, data) do
        ports =
          Enum.map(state.ports, fn port_state ->
            if port_state.port == port do
              output = keep_tail(port_state.output <> data, 128_000)
              %{port_state | output: output}
            else
              port_state
            end
          end)

        %{state | ports: ports}
      end

      defp mark_port_exit(state, port, status) do
        ports =
          Enum.map(state.ports, fn port_state ->
            if port_state.port == port,
              do: %{port_state | status: {:exit, status}},
              else: port_state
          end)

        %{state | ports: ports}
      end

      defp mark_running_stopped(ports, reason) do
        Enum.map(ports, fn
          %{status: :running} = port_state -> %{port_state | status: {:stopped, reason}}
          port_state -> port_state
        end)
      end

      defp kill_running_ports(state) do
        Enum.each(state.ports, fn
          %{status: :running, port: port} ->
            try do
              Port.close(port)
            catch
              _, _ -> :ok
            end

          _ ->
            :ok
        end)
      end

      defp attach_telemetry(handler_id, table, flow_latency_sample_rate) do
        :ok =
          :telemetry.attach_many(
            handler_id,
            @events,
            fn event, measurements, metadata, config ->
              record_event(table, event, measurements, metadata, config)
            end,
            %{latency_sample_rate: flow_latency_sample_rate}
          )
      end

      defp record_event(table, event, measurements, metadata, config) do
        key = event_key(event, metadata)
        duration_us = duration_us(measurements)
        item_count = item_count(measurements)
        bytes = bytes(measurements)
        max_us = max_metric_us(measurements, duration_us)
        latency_sample_rate = latency_sample_rate(config)

        [event_count, _duration_total, _item_total, _byte_total] =
          :ets.update_counter(
            table,
            key,
            [{2, 1}, {3, duration_us}, {4, item_count}, {5, bytes}],
            {key, 0, 0, 0, 0}
          )

        unless flow_event_key?(key), do: update_max(table, {:max_us, key}, max_us)
        record_flow_latency(table, key, duration_us, item_count, event_count, latency_sample_rate)
        record_waraft_flush_timings(table, key, measurements)
        record_waraft_hot_flush_kind(table, event, measurements, metadata)
        record_segment_append_kind(table, event, metadata, duration_us, bytes)
        record_projection_overlap(table, event, measurements)
      rescue
        _ -> :ok
      end

      defp latency_sample_rate(%{latency_sample_rate: rate}) when is_integer(rate) and rate > 0,
        do: rate

      defp latency_sample_rate(_config), do: 1

      defp flow_event_key?({:flow, command}) when command in @flow_latency_commands, do: true
      defp flow_event_key?(_key), do: false

      defp event_key([:ferricstore, :flow, command, :stop], _metadata), do: {:flow, command}

      defp event_key([:ferricstore, :flow, :create, kind], _metadata)
           when kind in [:attempt, :success, :rejected],
           do: {:flow_create, kind}

      defp event_key([:ferricstore, :flow, :lmdb_writer, :flush], metadata),
        do: {:lmdb_flush, Map.get(metadata, :status, :unknown)}

      defp event_key([:ferricstore, :flow, :hibernation, action], metadata)
           when action in [:evict_hot, :promote],
           do: {:flow_hibernation, action, Map.get(metadata, :result, :unknown)}

      defp event_key([:ferricstore, :waraft, :batcher, :slot_flush], metadata),
        do: {:waraft_flush, Map.get(metadata, :result, :unknown)}

      defp event_key([:ferricstore, :waraft, :batcher, :hot_flush], metadata),
        do: {:waraft_flush, Map.get(metadata, :result, :unknown)}

      defp event_key([:ferricstore, :waraft, :segment_log, :append], metadata),
        do: {:segment_append, Map.get(metadata, :result, :unknown)}

      defp event_key([:ferricstore, :waraft, :segment_log, :projection_overlap], metadata),
        do: {:projection_overlap, Map.get(metadata, :result, :unknown)}

      defp event_key([:ferricstore, :waraft, :commit_bytes, :rejected], _metadata),
        do: {:waraft_commit_bytes, :rejected}

      defp event_key([:ferricstore, :waraft, :commit, :timeout], metadata),
        do: {:waraft_commit_timeout, Map.get(metadata, :path, :unknown)}

      defp event_key([:ferricstore, :waraft, :internal_metric], metadata),
        do: {:waraft_internal_metric, Map.get(metadata, :metric, :unknown)}

      defp event_key([:ferricstore, :waraft, :storage_blocked], metadata),
        do: {:waraft_storage_blocked, Map.get(metadata, :reason, :unknown)}

      defp event_key([:ferricstore, :waraft, :storage, :payload_fsync], metadata),
        do: {:waraft_payload_fsync, Map.get(metadata, :result, :unknown)}

      defp event_key([:ferricstore, :waraft, :storage, :apply_phase], metadata),
        do: {:storage_apply_phase, Map.get(metadata, :phase, :unknown)}

      defp event_key([:ferricstore, :waraft, :blob_prepare_failed], metadata),
        do: {:waraft_blob_prepare_failed, Map.get(metadata, :reason, :unknown)}

      defp event_key(event, _metadata), do: {:event, event}

      defp telemetry_snapshot(table) do
        create_attempt = event_items(table, {:flow_create, :attempt})
        create_success = event_items(table, {:flow_create, :success})
        create_rejected = event_items(table, {:flow_create, :rejected})
        create = create_success
        transition = event_items(table, {:flow, :transition})
        complete = event_items(table, {:flow, :complete})
        fail = event_items(table, {:flow, :fail})
        pipeline_write = event_items(table, {:flow, :pipeline_write})
        claim_due = event_items(table, {:flow, :claim_due})
        cold_due_evicted = event_items(table, {:flow_hibernation, :evict_hot, :evicted})
        cold_due_evict_stale = event_items(table, {:flow_hibernation, :evict_hot, :stale})
        cold_due_promoted = event_items(table, {:flow_hibernation, :promote, :promoted})
        {lmdb_flushes, lmdb_flush_us} = event_count_duration_prefix(table, :lmdb_flush)

        {waraft_flushes, _waraft_total_event_us} =
          event_count_duration_prefix(table, :waraft_flush)

        {waraft_queue_samples, waraft_queue_wait_us} = timing_count_duration(table, :queue_age)
        {waraft_flush_samples, waraft_flush_duration_us} = timing_count_duration(table, :flush)
        {waraft_total_samples, waraft_total_duration_us} = timing_count_duration(table, :total)

        {payload_fsyncs, payload_fsync_us} =
          event_count_duration_prefix(table, :waraft_payload_fsync)

        {segment_appends, segment_bytes} = event_count_bytes_prefix(table, :segment_append)

        {projection_overlap, projection_overlap_us} =
          event_count_duration_prefix(table, :projection_overlap)

        raft_segment = segment_append_kind_stats(table, :raft_log)
        projection_segment = segment_append_kind_stats(table, :segment_projection)
        apply_projection_segment = segment_append_kind_stats(table, :apply_projection)
        acceptor_commit = waraft_internal_metric_stats(table, :"acceptor.commit.func")
        leader_apply = waraft_internal_metric_stats(table, :"leader.apply.func")
        apply_log = waraft_internal_metric_stats(table, :"apply_log.latency_us")
        storage_apply = waraft_internal_metric_stats(table, :"storage.apply.func")
        storage_phase_cache = storage_apply_phase_stats(table, :apply_projection_cache)
        storage_phase_recovery = storage_apply_phase_stats(table, :recovery_projection)
        storage_phase_metadata = storage_apply_phase_stats(table, :storage_metadata)
        apply_queue = waraft_internal_metric_stats(table, :"apply.queue")
        hot_batch = waraft_hot_flush_kind_stats(table, :batch)
        waraft_flush_errors = event_count_matching(table, :waraft_flush, &match?({:error, _}, &1))
        waraft_apply_full = event_count(table, {:waraft_flush, {:error, :apply_queue_full}})
        waraft_commit_bytes_rejected = event_count(table, {:waraft_commit_bytes, :rejected})
        waraft_commit_timeouts = event_count_prefix(table, :waraft_commit_timeout)
        payload_fsync_errors = event_count(table, {:waraft_payload_fsync, :error})
        blob_prepare_failures = event_count_prefix(table, :waraft_blob_prepare_failed)
        storage_blocked = event_count_prefix(table, :waraft_storage_blocked)

        %{
          create: create,
          create_attempt: create_attempt,
          create_success: create_success,
          create_rejected: create_rejected,
          transition: transition,
          complete: complete,
          fail: fail,
          pipeline_write: pipeline_write,
          claim_due: claim_due,
          cold_due_evicted: cold_due_evicted,
          cold_due_evict_stale: cold_due_evict_stale,
          cold_due_promoted: cold_due_promoted,
          flow_ops: create + transition + complete + fail + pipeline_write,
          write_ops: create + transition + complete + fail + pipeline_write + claim_due,
          lmdb_flushes: lmdb_flushes,
          lmdb_flush_avg_us: if(lmdb_flushes > 0, do: div(lmdb_flush_us, lmdb_flushes), else: 0),
          waraft_flushes: waraft_flushes,
          waraft_queue_wait_avg_us:
            if(waraft_queue_samples > 0,
              do: div(waraft_queue_wait_us, waraft_queue_samples),
              else: 0
            ),
          waraft_queue_wait_max_us: timing_max_us(table, :queue_age),
          waraft_flush_duration_avg_us:
            if(waraft_flush_samples > 0,
              do: div(waraft_flush_duration_us, waraft_flush_samples),
              else: 0
            ),
          waraft_flush_duration_max_us: timing_max_us(table, :flush),
          waraft_total_duration_avg_us:
            if(waraft_total_samples > 0,
              do: div(waraft_total_duration_us, waraft_total_samples),
              else: 0
            ),
          waraft_total_duration_max_us: timing_max_us(table, :total),
          waraft_flush_errors: waraft_flush_errors,
          waraft_apply_full: waraft_apply_full,
          waraft_commit_bytes_rejected: waraft_commit_bytes_rejected,
          waraft_commit_timeouts: waraft_commit_timeouts,
          waraft_commit_timeout_max_us: max_us(table, :waraft_commit_timeout),
          payload_fsyncs: payload_fsyncs,
          payload_fsync_avg_us:
            if(payload_fsyncs > 0, do: div(payload_fsync_us, payload_fsyncs), else: 0),
          payload_fsync_max_us: max_us(table, :waraft_payload_fsync),
          payload_fsync_errors: payload_fsync_errors,
          blob_prepare_failures: blob_prepare_failures,
          storage_blocked: storage_blocked,
          segment_appends: segment_appends,
          segment_mb: bytes_to_mb(segment_bytes),
          raft_segment_append_avg_us: raft_segment.avg_us,
          raft_segment_append_max_us: raft_segment.max_us,
          projection_segment_append_avg_us: projection_segment.avg_us,
          projection_segment_append_max_us: projection_segment.max_us,
          apply_projection_segment_append_avg_us: apply_projection_segment.avg_us,
          apply_projection_segment_append_max_us: apply_projection_segment.max_us,
          projection_overlap: projection_overlap,
          projection_overlap_rebuilds: counter_value(table, {:projection_overlap_rebuilds}),
          projection_overlap_avg_us:
            if(projection_overlap > 0,
              do: div(projection_overlap_us, projection_overlap),
              else: 0
            ),
          projection_overlap_max_us: max_us(table, :projection_overlap),
          acceptor_commit_avg_us: acceptor_commit.avg_us,
          acceptor_commit_max_us: acceptor_commit.max_us,
          leader_apply_avg_us: leader_apply.avg_us,
          leader_apply_max_us: leader_apply.max_us,
          apply_log_avg_us: apply_log.avg_us,
          apply_log_max_us: apply_log.max_us,
          storage_apply_avg_us: storage_apply.avg_us,
          storage_apply_max_us: storage_apply.max_us,
          storage_phase_cache_avg_us: storage_phase_cache.avg_us,
          storage_phase_cache_max_us: storage_phase_cache.max_us,
          storage_phase_recovery_avg_us: storage_phase_recovery.avg_us,
          storage_phase_recovery_max_us: storage_phase_recovery.max_us,
          storage_phase_metadata_avg_us: storage_phase_metadata.avg_us,
          storage_phase_metadata_max_us: storage_phase_metadata.max_us,
          apply_queue_avg: apply_queue.avg_us,
          apply_queue_max: apply_queue.max_us,
          hot_batch_flushes: hot_batch.count,
          hot_batch_avg_items: hot_batch.avg_items,
          hot_batch_max_items: hot_batch.max_items,
          hot_batch_avg_groups: hot_batch.avg_groups,
          hot_batch_queue_max_us: hot_batch.max_queue_us,
          hot_batch_flush_max_us: hot_batch.max_flush_us,
          hot_batch_total_max_us: hot_batch.max_total_us,
          flow_latency_sample_rate: flow_latency_sample_rate(table),
          flow_latency: flow_latency_snapshot(table)
        }
      end

      defp record_flow_latency(
             table,
             {:flow, command},
             duration_us,
             item_count,
             event_count,
             latency_sample_rate
           )
           when command in @flow_latency_commands and is_integer(duration_us) and duration_us >= 0 and
                  is_integer(item_count) and item_count > 0 do
        if latency_sample?(event_count, latency_sample_rate) do
          do_record_flow_latency(table, command, duration_us, item_count)
        end
      end

      defp record_flow_latency(
             _table,
             _key,
             _duration_us,
             _item_count,
             _event_count,
             _latency_sample_rate
           ),
           do: :ok

      defp latency_sample?(event_count, latency_sample_rate)
           when is_integer(event_count) and is_integer(latency_sample_rate) and
                  latency_sample_rate > 1 do
        rem(event_count, latency_sample_rate) == 0
      end

      defp latency_sample?(_event_count, _latency_sample_rate), do: true

      defp do_record_flow_latency(table, command, duration_us, item_count) do
        latency_key = {:flow_latency, command}

        :ets.update_counter(
          table,
          latency_key,
          [{2, 1}, {3, duration_us}, {4, item_count}],
          {latency_key, 0, 0, 0}
        )

        update_max(table, {:flow_latency_max_us, command}, duration_us)

        bucket = latency_bucket(duration_us)

        :ets.update_counter(
          table,
          {:flow_latency_bucket, command, bucket},
          {2, 1},
          {{:flow_latency_bucket, command, bucket}, 0}
        )
      end

      defp record_waraft_flush_timings(table, {:waraft_flush, _status}, measurements) do
        record_timing(table, :queue_age, Map.get(measurements, :queue_age_us))
        record_timing(table, :flush, Map.get(measurements, :flush_duration_us))
        record_timing(table, :total, Map.get(measurements, :total_duration_us))
      end

      defp record_waraft_flush_timings(_table, _key, _measurements), do: :ok

      defp record_waraft_hot_flush_kind(
             table,
             [:ferricstore, :waraft, :batcher, :hot_flush],
             measurements,
             metadata
           ) do
        kind = Map.get(metadata, :kind, :unknown)
        key = {:waraft_hot_flush_kind, kind}
        batch_size = Map.get(measurements, :batch_size, 0)
        group_count = Map.get(measurements, :group_count, 0)
        queue_us = Map.get(measurements, :queue_age_us, 0)
        flush_us = Map.get(measurements, :flush_duration_us, 0)
        total_us = Map.get(measurements, :total_duration_us, 0)

        :ets.update_counter(
          table,
          key,
          [
            {2, 1},
            {3, batch_size},
            {4, group_count},
            {5, queue_us},
            {6, flush_us},
            {7, total_us}
          ],
          {key, 0, 0, 0, 0, 0, 0}
        )

        update_max(table, {:max_batch_items, key}, batch_size)
        update_max(table, {:max_batch_groups, key}, group_count)
        update_max(table, {:max_queue_us, key}, queue_us)
        update_max(table, {:max_flush_us, key}, flush_us)
        update_max(table, {:max_total_us, key}, total_us)
      end

      defp record_waraft_hot_flush_kind(_table, _event, _measurements, _metadata), do: :ok

      defp record_segment_append_kind(
             table,
             [:ferricstore, :waraft, :segment_log, :append],
             metadata,
             duration_us,
             bytes
           )
           when is_integer(duration_us) and duration_us >= 0 do
        kind = Map.get(metadata, :kind, :unknown)
        key = {:segment_append_kind, kind}

        :ets.update_counter(
          table,
          key,
          [{2, 1}, {3, duration_us}, {4, bytes}],
          {key, 0, 0, 0}
        )

        update_max(table, {:max_us, key}, duration_us)
      end

      defp record_segment_append_kind(_table, _event, _metadata, _duration_us, _bytes), do: :ok

      defp record_projection_overlap(
             table,
             [:ferricstore, :waraft, :segment_log, :projection_overlap],
             measurements
           ) do
        rebuilds = Map.get(measurements, :rebuilds, 0)

        if is_integer(rebuilds) and rebuilds > 0 do
          :ets.update_counter(
            table,
            {:projection_overlap_rebuilds},
            {2, rebuilds},
            {{:projection_overlap_rebuilds}, 0}
          )
        end
      end

      defp record_projection_overlap(_table, _event, _measurements), do: :ok

      defp segment_append_kind_stats(table, kind) do
        key = {:segment_append_kind, kind}

        {count, duration_us} =
          case :ets.lookup(table, key) do
            [{^key, count, duration_us, _bytes}] -> {count, duration_us}
            _ -> {0, 0}
          end

        %{
          avg_us: if(count > 0, do: div(duration_us, count), else: 0),
          max_us: timing_key_max_us(table, key)
        }
      end
    end
  end
end
