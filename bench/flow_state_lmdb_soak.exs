Code.require_file("flow_state_lmdb_soak/sections/part_01.exs", __DIR__)
Code.require_file("flow_state_lmdb_soak/sections/part_02.exs", __DIR__)
Code.require_file("flow_state_lmdb_soak/sections/part_03.exs", __DIR__)

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
      port = FerricstoreServer.Native.Listener.port()

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

  use FlowStateLMDBSoak.Sections.Part01

  use FlowStateLMDBSoak.Sections.Part02

  use FlowStateLMDBSoak.Sections.Part03
end

FlowStateLMDBSoak.run()
