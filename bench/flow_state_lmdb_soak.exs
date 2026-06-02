Logger.configure(level: :warning)
:logger.set_primary_config(:level, :warning)

defmodule FlowStateLMDBSoakWARaftMetrics do
  @moduledoc false
  @interesting_gauges [
    :"acceptor.commit.func",
    :"leader.apply.func",
    :"apply_log.latency_us",
    :"storage.apply.func",
    :"apply.queue"
  ]

  def count(_metric), do: :ok
  def countv(_metric, _value), do: :ok

  def gather({:raft, table, metric}, value)
      when is_integer(value) and metric in @interesting_gauges do
    :telemetry.execute(
      [:ferricstore, :waraft, :internal_metric],
      %{duration_us: value, value: value},
      %{table: table, metric: metric}
    )
  catch
    _kind, _reason -> :ok
  end

  def gather(_metric, _value), do: :ok
  def gather_latency(metric, value), do: gather(metric, value)
end

defmodule FlowStateLMDBSoak do
  @moduledoc false

  @events [
    [:ferricstore, :flow, :create, :stop],
    [:ferricstore, :flow, :create, :attempt],
    [:ferricstore, :flow, :create, :success],
    [:ferricstore, :flow, :create, :rejected],
    [:ferricstore, :flow, :transition, :stop],
    [:ferricstore, :flow, :complete, :stop],
    [:ferricstore, :flow, :fail, :stop],
    [:ferricstore, :flow, :pipeline_write, :stop],
    [:ferricstore, :flow, :claim_due, :stop],
    [:ferricstore, :flow, :hibernation, :evict_hot],
    [:ferricstore, :flow, :hibernation, :promote],
    [:ferricstore, :flow, :lmdb_writer, :flush],
    [:ferricstore, :flow, :lmdb_writer, :backlog],
    [:ferricstore, :waraft, :batcher, :slot_flush],
    [:ferricstore, :waraft, :batcher, :hot_flush],
    [:ferricstore, :waraft, :segment_log, :append],
    [:ferricstore, :waraft, :segment_log, :projection_overlap],
    [:ferricstore, :waraft, :commit_bytes, :rejected],
    [:ferricstore, :waraft, :commit, :timeout],
    [:ferricstore, :waraft, :internal_metric],
    [:ferricstore, :waraft, :storage_blocked],
    [:ferricstore, :waraft, :storage, :payload_fsync],
    [:ferricstore, :waraft, :storage, :apply_phase],
    [:ferricstore, :waraft, :blob_prepare_failed]
  ]

  @flow_latency_commands [:create, :pipeline_write, :claim_due, :transition, :complete, :fail]

  @latency_us_buckets [
    100,
    250,
    500,
    1_000,
    2_000,
    5_000,
    10_000,
    20_000,
    30_000,
    40_000,
    50_000,
    60_000,
    75_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    2_000_000,
    5_000_000,
    :infinity
  ]

  def run do
    duration_s = int_env("DURATION_SECONDS", 3_600)
    target_ops_s = int_env("TARGET_OPS_PER_SEC", 50_000)
    payload_bytes = int_env("PAYLOAD_BYTES", 5_000)
    normal_steps = int_env("NORMAL_STEPS", 50)
    long_steps = int_env("LONG_STEPS", 10_000)
    long_flows = int_env("LONG_FLOWS", 1)
    shards = int_env("SHARDS", 16)
    sample_interval_s = int_env("SAMPLE_INTERVAL_SECONDS", 30)
    min_free_mb = int_env("MIN_FREE_DISK_MB", 100_000)
    max_rss_mb = int_env("MAX_RSS_MB", int_env("MAX_TOTAL_MEM_MB", 64_000))
    server_max_rss_mb = int_env("SERVER_MAX_RSS_MB", max_rss_mb)
    flow_latency_sample_rate = max(int_env("FLOW_LATENCY_SAMPLE_RATE", 10), 1)
    terminal_retention_ttl_ms = int_env("TERMINAL_RETENTION_TTL_MS", 0)
    run_at_delay_ms = int_env("RUN_AT_DELAY_MS", 0)

    normal_flows =
      case System.get_env("NORMAL_FLOWS") do
        nil -> max(div(target_ops_s * duration_s, normal_steps + 1), 1)
        value -> String.to_integer(value)
      end

    data_dir =
      System.get_env("DATA_DIR") ||
        Path.join(System.tmp_dir!(), "ferricstore-flow-state-lmdb-soak-#{unique()}")

    estimate_bytes =
      estimated_payload_bytes(normal_flows, normal_steps, payload_bytes) +
        estimated_payload_bytes(long_flows, long_steps, payload_bytes)

    free_mb_before = disk_free_mb(data_dir)

    IO.puts(
      "flow_state_lmdb_soak_prepare backend=waraft duration_s=#{duration_s} " <>
        "target_ops_s=#{target_ops_s} payload_bytes=#{payload_bytes} normal_flows=#{normal_flows} " <>
        "normal_steps=#{normal_steps} normal_claim_states_mode=#{normal_claim_states_mode()} " <>
        "normal_worker_mode=#{normal_worker_mode()} " <>
        "long_flows=#{long_flows} long_steps=#{long_steps} " <>
        "long_claim_states_mode=#{long_claim_states_mode()} " <>
        "long_worker_mode=#{long_worker_mode()} " <>
        "flow_async_history=true " <>
        "flow_latency_sample_rate=#{flow_latency_sample_rate} " <>
        "terminal_retention_ttl_ms=#{terminal_retention_ttl_ms} " <>
        "run_at_delay_ms=#{run_at_delay_ms} " <>
        "server_max_rss_mb=#{server_max_rss_mb} max_rss_mb=#{max_rss_mb} " <>
        "estimated_raw_payload_gb=#{Float.round(estimate_bytes / 1_073_741_824, 2)} " <>
        "free_mb=#{free_mb_before} min_free_mb=#{min_free_mb}"
    )

    stop_started_apps()
    configure_app(data_dir, shards, server_max_rss_mb)
    print_effective_config()

    table = telemetry_table()
    init_table(table)
    :ets.insert(table, {{:config, :flow_latency_sample_rate}, flow_latency_sample_rate})
    handler_id = "flow-state-lmdb-soak-#{unique()}"
    {:ok, _} = Application.ensure_all_started(:telemetry)
    attach_telemetry(handler_id, table, flow_latency_sample_rate)
    :wa_raft_metrics.install(FlowStateLMDBSoakWARaftMetrics)

    started_native = System.monotonic_time()

    try do
      {:ok, _} = Application.ensure_all_started(:ferricstore_server)
      port = FerricstoreServer.Listener.port()

      normal_port =
        start_python_workload(:normal, port,
          flows: normal_flows,
          steps: normal_steps,
          terminal_mode: "complete",
          payload_bytes: payload_bytes,
          claim_states_mode: normal_claim_states_mode(),
          worker_mode: normal_worker_mode()
        )

      long_port =
        if long_flows > 0 do
          start_python_workload(:long_fail, port,
            flows: long_flows,
            steps: long_steps,
            terminal_mode: "fail",
            payload_bytes: payload_bytes,
            claim_states_mode: long_claim_states_mode(),
            worker_mode: long_worker_mode()
          )
        end

      print_header()

      state = %{
        ports: Enum.reject([normal_port, long_port], &is_nil/1),
        started_native: started_native,
        deadline_native: started_native + System.convert_time_unit(duration_s, :second, :native),
        sample_interval_ms: sample_interval_s * 1_000,
        table: table,
        data_dir: data_dir,
        min_free_mb: min_free_mb,
        max_rss_mb: max_rss_mb,
        server_max_rss_mb: server_max_rss_mb,
        last_disk_mb: dir_mb(data_dir),
        last_disk_at: started_native,
        max_pending_ops: 0,
        max_oldest_lag_ms: 0.0,
        max_replay_lag: 0,
        max_history_pending_entries: 0,
        max_history_oldest_lag_ms: 0.0,
        max_history_projection_lag: 0,
        max_history_flush_failures: 0,
        max_history_queue_full: 0,
        max_blob_hardened: 0,
        max_blob_hardened_oldest_ms: 0,
        max_release_cursor_gap: 0,
        max_disk_mb: 0.0,
        max_blob_mb: 0.0,
        max_lmdb_mb: 0.0,
        max_waraft_mb: 0.0,
        max_waraft_log_entries: 0,
        max_waraft_log_ets_mb: 0.0,
        max_total_mem_mb_seen: 0.0,
        max_rss_mb_seen: 0.0,
        max_binary_mem_mb: 0.0,
        max_keydir_binary_mb: 0.0,
        max_flow_index_entries: 0,
        max_flow_lookup_entries: 0,
        sample_count: 0,
        outputs: %{},
        process_profile: process_profile_snapshot()
      }

      state = sample_loop(state)
      print_summary(state)
      maybe_keep_server_running(data_dir, port)
    after
      :telemetry.detach(handler_id)
      stop_started_apps()

      unless env("KEEP_DATA_DIR", "false") in ["1", "true", "TRUE"] do
        remove_data_dir(data_dir)
      end
    end
  end

  defp estimated_payload_bytes(flows, steps, payload_bytes) do
    flows * (steps + 1) * payload_bytes
  end

  defp start_python_workload(name, port, opts) do
    payload_bytes = Keyword.fetch!(opts, :payload_bytes)
    create_rate_per_sec = default_create_rate_per_sec(opts)

    args = [
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
      env("CREATE_BATCH_SIZE", Integer.to_string(default_create_batch_size(create_rate_per_sec))),
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
        max_blob_hardened: max(state.max_blob_hardened, production_health.blob_hardened_count),
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
        max_flow_lookup_entries: max(state.max_flow_lookup_entries, flow_index.lookup_entries),
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
        if port_state.port == port, do: %{port_state | status: {:exit, status}}, else: port_state
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
    {waraft_flushes, _waraft_total_event_us} = event_count_duration_prefix(table, :waraft_flush)
    {waraft_queue_samples, waraft_queue_wait_us} = timing_count_duration(table, :queue_age)
    {waraft_flush_samples, waraft_flush_duration_us} = timing_count_duration(table, :flush)
    {waraft_total_samples, waraft_total_duration_us} = timing_count_duration(table, :total)
    {payload_fsyncs, payload_fsync_us} = event_count_duration_prefix(table, :waraft_payload_fsync)
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
        if(waraft_queue_samples > 0, do: div(waraft_queue_wait_us, waraft_queue_samples), else: 0),
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
        if(projection_overlap > 0, do: div(projection_overlap_us, projection_overlap), else: 0),
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
      [{2, 1}, {3, batch_size}, {4, group_count}, {5, queue_us}, {6, flush_us}, {7, total_us}],
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

  defp waraft_hot_flush_kind_stats(table, kind) do
    key = {:waraft_hot_flush_kind, kind}

    {count, items, groups} =
      case :ets.lookup(table, key) do
        [{^key, count, items, groups, _queue_us, _flush_us, _total_us}] -> {count, items, groups}
        _ -> {0, 0, 0}
      end

    %{
      count: count,
      avg_items: if(count > 0, do: div(items, count), else: 0),
      max_items: counter_max(table, {:max_batch_items, key}),
      avg_groups: if(count > 0, do: div(groups, count), else: 0),
      max_groups: counter_max(table, {:max_batch_groups, key}),
      max_queue_us: counter_max(table, {:max_queue_us, key}),
      max_flush_us: counter_max(table, {:max_flush_us, key}),
      max_total_us: counter_max(table, {:max_total_us, key})
    }
  end

  defp waraft_internal_metric_stats(table, metric) do
    key = {:waraft_internal_metric, metric}

    {count, duration_us} =
      case :ets.lookup(table, key) do
        [{^key, count, duration_us, _items, _bytes}] -> {count, duration_us}
        _ -> {0, 0}
      end

    %{
      avg_us: if(count > 0, do: div(duration_us, count), else: 0),
      max_us: max_us_for_key(table, key)
    }
  end

  defp storage_apply_phase_stats(table, phase) do
    key = {:storage_apply_phase, phase}

    {count, duration_us} =
      case :ets.lookup(table, key) do
        [{^key, count, duration_us, _items, _bytes}] -> {count, duration_us}
        _ -> {0, 0}
      end

    %{
      avg_us: if(count > 0, do: div(duration_us, count), else: 0),
      max_us: max_us_for_key(table, key)
    }
  end

  defp record_timing(table, phase, value) when is_integer(value) and value >= 0 do
    key = {:waraft_flush_timing, phase}

    :ets.update_counter(
      table,
      key,
      [{2, 1}, {3, value}],
      {key, 0, 0}
    )

    update_max(table, {:max_us, key}, value)
  end

  defp record_timing(_table, _phase, _value), do: :ok

  defp latency_bucket(duration_us) do
    Enum.find(@latency_us_buckets, :infinity, fn
      :infinity -> true
      bucket_us -> duration_us <= bucket_us
    end)
  end

  defp flow_latency_snapshot(table) do
    Map.new(@flow_latency_commands, fn command ->
      {command, flow_latency_stats(table, command)}
    end)
  end

  defp flow_latency_sample_rate(table) do
    case :ets.lookup(table, {:config, :flow_latency_sample_rate}) do
      [{{:config, :flow_latency_sample_rate}, rate}] when is_integer(rate) and rate > 0 -> rate
      _ -> 1
    end
  end

  defp flow_latency_stats(table, command) do
    {count, total_us, items} =
      case :ets.lookup(table, {:flow_latency, command}) do
        [{{:flow_latency, ^command}, count, total_us, items}] -> {count, total_us, items}
        _ -> {0, 0, 0}
      end

    max_us =
      case :ets.lookup(table, {:flow_latency_max_us, command}) do
        [{{:flow_latency_max_us, ^command}, value}] when is_integer(value) -> value
        _ -> 0
      end

    %{
      calls: count,
      items: items,
      avg_us: if(count > 0, do: div(total_us, count), else: 0),
      avg_item_us: if(items > 0, do: div(total_us, items), else: 0),
      p50_us: flow_latency_percentile(table, command, count, max_us, 0.50),
      p95_us: flow_latency_percentile(table, command, count, max_us, 0.95),
      p99_us: flow_latency_percentile(table, command, count, max_us, 0.99),
      max_us: max_us
    }
  end

  defp flow_latency_percentile(_table, _command, count, _max_us, _percentile) when count <= 0,
    do: 0

  defp flow_latency_percentile(table, command, count, max_us, percentile) do
    target = max(ceil(count * percentile), 1)

    @latency_us_buckets
    |> Enum.reduce_while(0, fn bucket, acc ->
      bucket_count =
        case :ets.lookup(table, {:flow_latency_bucket, command, bucket}) do
          [{{:flow_latency_bucket, ^command, ^bucket}, value}] -> value
          _ -> 0
        end

      next = acc + bucket_count

      if next >= target do
        {:halt, latency_bucket_value(bucket, max_us)}
      else
        {:cont, next}
      end
    end)
    |> case do
      value when is_integer(value) -> value
      _ -> max_us
    end
  end

  defp latency_bucket_value(:infinity, max_us), do: max_us
  defp latency_bucket_value(bucket_us, _max_us), do: bucket_us

  defp print_flow_latency_line(latency, prefix, sample_rate) do
    parts =
      @flow_latency_commands
      |> Enum.map(fn command ->
        latency
        |> Map.get(command, empty_latency_stats())
        |> format_flow_latency(command)
      end)
      |> Enum.reject(&(&1 == ""))

    if parts != [] do
      sample_tag = if sample_rate > 1, do: " sample_rate=#{sample_rate}", else: ""
      IO.puts(prefix <> sample_tag <> " " <> Enum.join(parts, " "))
    end
  end

  defp empty_latency_stats do
    %{
      calls: 0,
      items: 0,
      avg_us: 0,
      avg_item_us: 0,
      p50_us: 0,
      p95_us: 0,
      p99_us: 0,
      max_us: 0
    }
  end

  defp format_flow_latency(%{calls: calls}, _command) when calls <= 0, do: ""

  defp format_flow_latency(stats, command) do
    "#{command}=" <>
      "calls:#{stats.calls}," <>
      "items:#{stats.items}," <>
      "avg:#{ms(stats.avg_us)}," <>
      "avg_item:#{ms(stats.avg_item_us)}," <>
      "p50<=#{ms(stats.p50_us)}," <>
      "p95<=#{ms(stats.p95_us)}," <>
      "p99<=#{ms(stats.p99_us)}," <>
      "max:#{ms(stats.max_us)}"
  end

  defp ms(us) when is_integer(us), do: Float.round(us / 1000, 3)

  defp event_count(table, key) do
    case :ets.lookup(table, key) do
      [{^key, count, _duration, _items, _bytes}] -> count
      _ -> 0
    end
  end

  defp event_items(table, key) do
    case :ets.lookup(table, key) do
      [{^key, _count, _duration, items, _bytes}] -> items
      _ -> 0
    end
  end

  defp event_count_duration_prefix(table, prefix) do
    table
    |> :ets.tab2list()
    |> Enum.reduce({0, 0}, fn
      {{^prefix, _status}, count, duration, _items, _bytes}, {count_acc, duration_acc} ->
        {count_acc + count, duration_acc + duration}

      _row, acc ->
        acc
    end)
  end

  defp timing_count_duration(table, phase) do
    key = {:waraft_flush_timing, phase}

    case :ets.lookup(table, key) do
      [{^key, count, duration}] -> {count, duration}
      _ -> {0, 0}
    end
  end

  defp event_count_bytes_prefix(table, prefix) do
    table
    |> :ets.tab2list()
    |> Enum.reduce({0, 0}, fn
      {{^prefix, _status}, count, _duration, _items, bytes}, {count_acc, bytes_acc} ->
        {count_acc + count, bytes_acc + bytes}

      _row, acc ->
        acc
    end)
  end

  defp event_count_matching(table, prefix, predicate) do
    table
    |> :ets.tab2list()
    |> Enum.reduce(0, fn
      {{^prefix, status}, count, _duration, _items, _bytes}, acc ->
        if predicate.(status), do: acc + count, else: acc

      _row, acc ->
        acc
    end)
  end

  defp event_count_prefix(table, prefix) do
    table
    |> :ets.tab2list()
    |> Enum.reduce(0, fn
      {{^prefix, _status}, count, _duration, _items, _bytes}, acc -> acc + count
      _row, acc -> acc
    end)
  end

  defp max_us(table, prefix) do
    table
    |> :ets.tab2list()
    |> Enum.reduce(0, fn
      {{:max_us, {^prefix, _status}}, value}, acc when is_integer(value) -> max(acc, value)
      _row, acc -> acc
    end)
  end

  defp max_us_for_key(table, timing_key) do
    key = {:max_us, timing_key}

    case :ets.lookup(table, key) do
      [{^key, value}] when is_integer(value) -> value
      _ -> 0
    end
  end

  defp counter_max(table, key) do
    case :ets.lookup(table, key) do
      [{^key, value}] when is_integer(value) -> value
      _ -> 0
    end
  end

  defp counter_value(table, key) do
    case :ets.lookup(table, key) do
      [{^key, value}] when is_integer(value) -> value
      _ -> 0
    end
  end

  defp timing_max_us(table, phase) do
    key = {:max_us, {:waraft_flush_timing, phase}}

    case :ets.lookup(table, key) do
      [{^key, value}] when is_integer(value) -> value
      _ -> 0
    end
  end

  defp timing_key_max_us(table, timing_key) do
    key = {:max_us, timing_key}

    case :ets.lookup(table, key) do
      [{^key, value}] when is_integer(value) -> value
      _ -> 0
    end
  end

  defp lmdb_status do
    case safe_instance() do
      nil ->
        %{pending_ops: 0, max_oldest_lag_ms: 0.0, max_replay_safe_lag: 0, flush_failures: 0}

      ctx ->
        shards = max(Map.get(ctx, :shard_count, 0), 0)

        Enum.reduce(
          0..max(shards - 1, 0),
          %{pending_ops: 0, max_oldest_lag_ms: 0.0, max_replay_safe_lag: 0, flush_failures: 0},
          fn shard, acc ->
            pending_ops = atomic(ctx, :flow_lmdb_writer_pending_ops, shard)
            age_us = atomic(ctx, :flow_lmdb_writer_oldest_pending_age_us, shard)
            requested = atomic(ctx, :flow_lmdb_replay_safe_requested_index, shard)
            durable = atomic(ctx, :flow_lmdb_replay_safe_index, shard)
            failures = atomic(ctx, :flow_lmdb_writer_flush_failures, shard)

            %{
              pending_ops: acc.pending_ops + pending_ops,
              max_oldest_lag_ms: max(acc.max_oldest_lag_ms, age_us / 1000),
              max_replay_safe_lag: max(acc.max_replay_safe_lag, max(requested - durable, 0)),
              flush_failures: acc.flush_failures + failures
            }
          end
        )
    end
  end

  defp flow_admission_status do
    status = Ferricstore.Flow.Admission.status()

    %{
      paused: status.reject_new_creates?,
      reason: status.reason,
      retry_after_ms: status.retry_after_ms
    }
  rescue
    _ -> %{paused: false, reason: :unknown, retry_after_ms: 0}
  end

  defp production_health_status do
    case safe_instance() do
      nil ->
        empty_production_health_status()

      ctx ->
        shards = max(Map.get(ctx, :shard_count, 0), 0)

        health =
          Enum.reduce(0..max(shards - 1, 0), empty_production_health_status(), fn shard, acc ->
            history_pending = atomic(ctx, :flow_history_projector_pending_entries, shard)
            history_age_us = atomic(ctx, :flow_history_projector_oldest_pending_age_us, shard)
            history_requested = atomic(ctx, :flow_history_requested_index, shard)
            history_projected = atomic(ctx, :flow_history_projected_index, shard)
            history_lag = max(history_requested - history_projected, 0)
            history_flush_failures = atomic(ctx, :flow_history_projector_flush_failures, shard)
            history_queue_full = atomic(ctx, :flow_history_projector_queue_full, shard)
            last_applied = atomic(ctx, :last_applied_index, shard)
            last_released = atomic(ctx, :last_released_cursor_index, shard)
            release_cursor_gap = max(last_applied - last_released, 0)

            %{
              acc
              | history_pending_entries: acc.history_pending_entries + history_pending,
                history_oldest_lag_ms: max(acc.history_oldest_lag_ms, history_age_us / 1000),
                history_projection_lag: max(acc.history_projection_lag, history_lag),
                history_flush_failures: acc.history_flush_failures + history_flush_failures,
                history_queue_full: acc.history_queue_full + history_queue_full,
                release_cursor_gap: max(acc.release_cursor_gap, release_cursor_gap)
            }
          end)

        blob_stats = hardened_blob_stats(ctx)

        %{
          health
          | blob_hardened_count: blob_stats.count,
            blob_hardened_oldest_ms: blob_stats.oldest_age_ms
        }
    end
  end

  defp empty_production_health_status do
    %{
      history_pending_entries: 0,
      history_oldest_lag_ms: 0.0,
      history_projection_lag: 0,
      history_flush_failures: 0,
      history_queue_full: 0,
      blob_hardened_count: 0,
      blob_hardened_oldest_ms: 0,
      release_cursor_gap: 0
    }
  end

  defp hardened_blob_stats(%{data_dir: data_dir}) when is_binary(data_dir) do
    Ferricstore.Store.BlobStore.hardened_protection_stats(data_dir)
  rescue
    _ -> %{count: 0, oldest_age_ms: 0}
  end

  defp hardened_blob_stats(_ctx), do: %{count: 0, oldest_age_ms: 0}

  defp keydir_status do
    case safe_instance() do
      %{keydir_refs: refs} = ctx when is_tuple(refs) ->
        initial = %{
          entries: 0,
          binary_mb: atomic_total_mb(ctx, :keydir_binary_bytes),
          state: 0,
          history: 0,
          value: 0,
          flow_other: 0,
          other: 0
        }

        if bool_env("KEYDIR_BREAKDOWN", true) do
          refs
          |> Tuple.to_list()
          |> Enum.reduce(initial, &count_keydir_table/2)
        else
          entries =
            refs
            |> Tuple.to_list()
            |> Enum.reduce(0, fn table, acc -> acc + ets_info(table, :size) end)

          %{initial | entries: entries}
        end

      _ ->
        empty_keydir_status()
    end
  rescue
    _ -> empty_keydir_status()
  end

  defp empty_keydir_status do
    %{
      entries: 0,
      binary_mb: 0.0,
      state: 0,
      history: 0,
      value: 0,
      flow_other: 0,
      other: 0
    }
  end

  defp count_keydir_table(table, acc) do
    :ets.foldl(
      fn
        {key, _value, _expire_at_ms, _lfu, _fid, _offset, _value_size}, table_acc
        when is_binary(key) ->
          increment_keydir_kind(table_acc, keydir_key_kind(key))

        _row, table_acc ->
          increment_keydir_kind(table_acc, :other)
      end,
      acc,
      table
    )
  rescue
    _ -> acc
  end

  defp increment_keydir_kind(acc, kind) do
    acc
    |> Map.update!(:entries, &(&1 + 1))
    |> Map.update!(kind, &(&1 + 1))
  end

  defp keydir_key_kind("X:f:" <> _rest), do: :history

  defp keydir_key_kind("f:" <> rest) do
    cond do
      :binary.match(rest, "}:s:") != :nomatch -> :state
      :binary.match(rest, "}:v:") != :nomatch -> :value
      true -> :flow_other
    end
  end

  defp keydir_key_kind(_key), do: :other

  defp flow_index_status do
    case safe_instance() do
      %{name: name, shard_count: count} when is_atom(name) and is_integer(count) and count > 0 ->
        Enum.reduce(0..(count - 1), %{index_entries: 0, lookup_entries: 0}, fn shard, acc ->
          {index, lookup} = Ferricstore.Flow.OrderedIndex.table_names(name, shard)

          %{
            index_entries: acc.index_entries + ets_info(index, :size),
            lookup_entries: acc.lookup_entries + ets_info(lookup, :size)
          }
        end)

      _ ->
        %{index_entries: 0, lookup_entries: 0}
    end
  rescue
    _ -> %{index_entries: 0, lookup_entries: 0}
  end

  defp waraft_log_status do
    shard_count =
      case safe_instance() do
        %{shard_count: count} when is_integer(count) and count > 0 -> count
        _ -> int_env("SHARDS", 16)
      end

    Enum.reduce(1..max(shard_count, 1), %{entries: 0, ets_mb: 0.0}, fn partition, acc ->
      table = :"raft_log_ferricstore_waraft_backend_#{partition}"
      size = ets_info(table, :size)
      memory_words = ets_info(table, :memory)

      %{
        entries: acc.entries + size,
        ets_mb: acc.ets_mb + bytes_to_mb(memory_words * :erlang.system_info(:wordsize))
      }
    end)
  rescue
    _ -> %{entries: 0, ets_mb: 0.0}
  end

  defp ets_info(table, item) do
    case :ets.info(table, item) do
      value when is_integer(value) -> value
      _ -> 0
    end
  rescue
    _ -> 0
  end

  defp atomic(ctx, field, shard) do
    case Map.get(ctx, field) do
      ref when is_reference(ref) ->
        if shard < :atomics.info(ref).size, do: :atomics.get(ref, shard + 1), else: 0

      _ ->
        0
    end
  rescue
    _ -> 0
  end

  defp atomic_total_mb(ctx, field), do: bytes_to_mb(atomic_total(ctx, field))

  defp atomic_total(ctx, field) do
    case Map.get(ctx, field) do
      ref when is_reference(ref) ->
        size = :atomics.info(ref).size

        Enum.reduce(1..size, 0, fn idx, acc ->
          acc + :atomics.get(ref, idx)
        end)

      _ ->
        0
    end
  rescue
    _ -> 0
  end

  defp safe_instance do
    FerricStore.Instance.get(:default)
  rescue
    _ -> nil
  end

  defp memory_status do
    memory = :erlang.memory()
    os = os_process_status()

    %{
      total_mb: bytes_to_mb(Keyword.get(memory, :total, 0)),
      binary_mb: bytes_to_mb(Keyword.get(memory, :binary, 0)),
      ets_mb: bytes_to_mb(Keyword.get(memory, :ets, 0)),
      rss_mb: os.rss_mb,
      cpu_pct: os.cpu_pct,
      process_count: :erlang.system_info(:process_count),
      run_queue: :erlang.statistics(:run_queue)
    }
  end

  defp rss_guard_mb(%{rss_mb: rss_mb}) when rss_mb > 0, do: rss_mb
  defp rss_guard_mb(%{total_mb: total_mb}), do: total_mb

  defp maybe_print_top_binary_holders(sample_count) do
    if diagnostic_due?("TOP_BINARY_HOLDERS", "TOP_BINARY_HOLDERS_EVERY_N", sample_count) do
      limit = int_env("TOP_BINARY_HOLDERS_LIMIT", 8)

      Process.list()
      |> Enum.map(&process_binary_holder/1)
      |> Enum.filter(fn %{bytes: bytes} -> bytes > 0 end)
      |> Enum.sort_by(& &1.bytes, :desc)
      |> Enum.take(limit)
      |> Enum.each(fn holder ->
        IO.puts(
          "top_binary_holder pid=#{inspect(holder.pid)} name=#{inspect(holder.name)} " <>
            "initial_call=#{inspect(holder.initial_call)} binary_mb=#{Float.round(bytes_to_mb(holder.bytes), 1)} " <>
            "binary_count=#{holder.count}"
        )
      end)
    end
  end

  defp maybe_print_top_ets_binary_tables(sample_count) do
    if diagnostic_due?("TOP_ETS_BINARY_TABLES", "TOP_ETS_BINARY_TABLES_EVERY_N", sample_count) do
      limit = int_env("TOP_ETS_BINARY_TABLES_LIMIT", 8)
      max_rows = int_env("TOP_ETS_BINARY_TABLES_MAX_ROWS", 1_000)

      :ets.all()
      |> Enum.map(&ets_binary_table_sample(&1, max_rows))
      |> Enum.filter(fn %{bytes: bytes} -> bytes > 0 end)
      |> Enum.sort_by(& &1.bytes, :desc)
      |> Enum.take(limit)
      |> Enum.each(fn table ->
        IO.puts(
          "top_ets_binary_table table=#{inspect(table.table)} sampled_binary_mb=#{Float.round(bytes_to_mb(table.bytes), 1)} " <>
            "sampled_rows=#{table.rows} table_size=#{inspect(table.size)}"
        )
      end)
    end
  end

  defp maybe_print_top_ets_memory_tables(sample_count) do
    if diagnostic_due?("TOP_ETS_MEMORY_TABLES", "TOP_ETS_MEMORY_TABLES_EVERY_N", sample_count) do
      limit = int_env("TOP_ETS_MEMORY_TABLES_LIMIT", 12)
      wordsize = :erlang.system_info(:wordsize)

      :ets.all()
      |> Enum.map(fn table ->
        %{
          table: table,
          memory_mb: ets_table_memory_mb(table, wordsize),
          size: ets_table_info(table, :size),
          owner: ets_table_info(table, :owner),
          name: ets_table_info(table, :name),
          type: ets_table_info(table, :type)
        }
      end)
      |> Enum.filter(fn %{memory_mb: memory_mb} -> memory_mb > 0 end)
      |> Enum.sort_by(& &1.memory_mb, :desc)
      |> Enum.take(limit)
      |> Enum.each(fn table ->
        IO.puts(
          "top_ets_memory_table table=#{inspect(table.table)} name=#{inspect(table.name)} " <>
            "type=#{inspect(table.type)} owner=#{inspect(table.owner)} " <>
            "memory_mb=#{Float.round(table.memory_mb, 1)} size=#{inspect(table.size)}"
        )
      end)
    end
  end

  defp ets_table_memory_mb(table, wordsize) do
    case ets_table_info(table, :memory) do
      memory when is_integer(memory) -> bytes_to_mb(memory * wordsize)
      _ -> 0.0
    end
  end

  defp ets_table_info(table, key) do
    :ets.info(table, key)
  rescue
    _ -> nil
  end

  defp maybe_print_process_profile(previous, sample_count) do
    current = process_profile_snapshot()

    if diagnostic_due?("PROCESS_PROFILE", "PROCESS_PROFILE_EVERY_N", sample_count) do
      limit = int_env("PROCESS_PROFILE_TOP", 12)

      rows =
        current
        |> Enum.map(fn {pid, info} ->
          previous_info = Map.get(previous, pid, %{})
          reductions = Map.get(info, :reductions, 0) - Map.get(previous_info, :reductions, 0)

          Map.merge(info, %{pid: pid, reduction_delta: reductions})
        end)
        |> Enum.filter(&(&1.reduction_delta > 0))
        |> Enum.sort_by(& &1.reduction_delta, :desc)
        |> Enum.take(limit)

      IO.puts("process_profile_top_reductions count=#{length(rows)}")

      Enum.each(rows, fn row ->
        IO.puts(
          "process_profile pid=#{inspect(row.pid)} name=#{inspect(row.registered_name)} " <>
            "reductions=#{row.reduction_delta} mq=#{row.message_queue_len} " <>
            "memory_mb=#{Float.round(bytes_to_mb(row.memory), 2)} " <>
            "initial=#{inspect(row.initial_call)} current=#{inspect(row.current_function)}"
        )

        maybe_print_process_stack(row)
      end)
    end

    current
  end

  defp process_profile_snapshot do
    if env("PROCESS_PROFILE", "false") in ["1", "true", "TRUE"] do
      Process.list()
      |> Enum.reduce(%{}, fn pid, acc ->
        case Process.info(pid, [
               :registered_name,
               :initial_call,
               :current_function,
               :current_stacktrace,
               :reductions,
               :message_queue_len,
               :memory
             ]) do
          nil ->
            acc

          info ->
            Map.put(acc, pid, Map.new(info))
        end
      end)
    else
      %{}
    end
  end

  defp maybe_print_process_stack(row) do
    if env("PROCESS_PROFILE_STACK", "false") in ["1", "true", "TRUE"] do
      depth = int_env("PROCESS_PROFILE_STACK_DEPTH", 6)

      stack =
        row
        |> Map.get(:current_stacktrace, [])
        |> Enum.take(depth)
        |> Enum.map(&format_stack_frame/1)
        |> Enum.join(" <= ")

      IO.puts("process_profile_stack pid=#{inspect(row.pid)} stack=#{stack}")
    end
  end

  defp format_stack_frame({mod, fun, arity, location}) when is_integer(arity) do
    "#{inspect(mod)}.#{fun}/#{arity}#{format_stack_location(location)}"
  end

  defp format_stack_frame({mod, fun, args, location}) when is_list(args) do
    "#{inspect(mod)}.#{fun}/#{length(args)}#{format_stack_location(location)}"
  end

  defp format_stack_frame(frame), do: inspect(frame)

  defp format_stack_location(location) when is_list(location) do
    case Keyword.get(location, :file) do
      nil ->
        ""

      file ->
        line = Keyword.get(location, :line)

        ":#{Path.basename(to_string(file))}#{if line, do: ":" <> Integer.to_string(line), else: ""}"
    end
  end

  defp format_stack_location(_location), do: ""

  defp diagnostic_due?(enabled_env, every_env, sample_count) do
    case env(enabled_env, "0") do
      value when value in ["1", "true", "TRUE"] ->
        every = max(int_env(every_env, 4), 1)
        rem(sample_count, every) == 0

      _ ->
        false
    end
  end

  defp process_binary_holder(pid) do
    binaries =
      case Process.info(pid, :binary) do
        {:binary, binaries} when is_list(binaries) -> binaries
        _ -> []
      end

    {bytes, count} =
      Enum.reduce(binaries, {0, 0}, fn
        {_binary, size, _refs}, {sum, total} when is_integer(size) ->
          {sum + size, total + 1}

        _other, acc ->
          acc
      end)

    %{
      pid: pid,
      name: process_info_value(pid, :registered_name),
      initial_call: process_info_value(pid, :initial_call),
      bytes: bytes,
      count: count
    }
  end

  defp process_info_value(pid, key) do
    case Process.info(pid, key) do
      {^key, value} -> value
      _ -> nil
    end
  rescue
    _ -> nil
  end

  defp ets_binary_table_sample(table, max_rows) do
    size = ets_info(table, :size)

    {bytes, rows} =
      if is_integer(size) and size > 0 do
        sample_ets_table_binaries(table, max_rows)
      else
        {0, 0}
      end

    %{table: table, bytes: bytes, rows: rows, size: size}
  end

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
    Application.put_env(:ferricstore, :port, 0)
    Application.put_env(:ferricstore, :health_port, 0)
    Application.put_env(:ferricstore, :shard_count, shards)
    Application.put_env(:ferricstore, :protected_mode, false)

    max_memory_bytes = app_memory_budget_bytes(max_rss_mb)

    Application.put_env(:ferricstore, :max_memory_bytes, max_memory_bytes)
    Application.put_env(:ferricstore, :keydir_max_ram, app_keydir_max_ram_bytes(max_memory_bytes))

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

    if any_env?(["FLOW_LMDB_MMAP_RECLAIM_ENABLED", "FERRICSTORE_FLOW_LMDB_MMAP_RECLAIM_ENABLED"]) do
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
      ["FLOW_LMDB_MMAP_RECLAIM_INTERVAL_MS", "FERRICSTORE_FLOW_LMDB_MMAP_RECLAIM_INTERVAL_MS"],
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

    if any_env?(["FLOW_RETENTION_SWEEPER_ENABLED", "FERRICSTORE_FLOW_RETENTION_SWEEPER_ENABLED"]) do
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
      ["FLOW_RETENTION_SWEEPER_INTERVAL_MS", "FERRICSTORE_FLOW_RETENTION_SWEEPER_INTERVAL_MS"],
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
          ["WARAFT_GENERIC_BATCH_DURING_FLUSH", "FERRICSTORE_WARAFT_GENERIC_BATCH_DURING_FLUSH"],
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
      ["WARAFT_SEGMENT_LOG_MAX_ETS_ENTRIES", "FERRICSTORE_WARAFT_SEGMENT_LOG_MAX_ETS_ENTRIES"],
      :waraft_segment_log_max_ets_entries
    )

    put_optional_limit_env(
      ["WARAFT_SEGMENT_LOG_MIN_ETS_ENTRIES", "FERRICSTORE_WARAFT_SEGMENT_LOG_MIN_ETS_ENTRIES"],
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
    do: System.convert_time_unit(System.monotonic_time() - started, :native, :millisecond) / 1000

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
        {_output, 0} -> :ok
        {output, status} -> IO.puts("cleanup_failed status=#{status} output=#{inspect(output)}")
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
      nil -> :ok
      {_env_name, value} -> Application.put_env(:ferricstore, app_key, String.to_integer(value))
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

FlowStateLMDBSoak.run()
