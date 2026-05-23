Logger.configure(level: :warning)
:logger.set_primary_config(:level, :warning)

defmodule FlowStateLMDBSoak do
  @moduledoc false

  @events [
    [:ferricstore, :flow, :create, :stop],
    [:ferricstore, :flow, :transition, :stop],
    [:ferricstore, :flow, :complete, :stop],
    [:ferricstore, :flow, :fail, :stop],
    [:ferricstore, :flow, :pipeline_write, :stop],
    [:ferricstore, :flow, :claim_due, :stop],
    [:ferricstore, :flow, :lmdb_writer, :flush],
    [:ferricstore, :flow, :lmdb_writer, :backlog],
    [:ferricstore, :waraft, :batcher, :slot_flush],
    [:ferricstore, :waraft, :batcher, :hot_flush],
    [:ferricstore, :waraft, :segment_log, :append],
    [:ferricstore, :waraft, :commit_bytes, :rejected],
    [:ferricstore, :waraft, :commit, :timeout],
    [:ferricstore, :waraft, :storage_blocked],
    [:ferricstore, :waraft, :storage, :payload_fsync],
    [:ferricstore, :waraft, :blob_prepare_failed]
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
    max_total_mem_mb = int_env("MAX_TOTAL_MEM_MB", 64_000)

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
        "normal_steps=#{normal_steps} long_flows=#{long_flows} long_steps=#{long_steps} " <>
        "flow_async_history=true " <>
        "estimated_raw_payload_gb=#{Float.round(estimate_bytes / 1_073_741_824, 2)} " <>
        "free_mb=#{free_mb_before} min_free_mb=#{min_free_mb}"
    )

    stop_started_apps()
    configure_app(data_dir, shards)

    table = telemetry_table()
    init_table(table)
    handler_id = "flow-state-lmdb-soak-#{unique()}"
    {:ok, _} = Application.ensure_all_started(:telemetry)
    attach_telemetry(handler_id, table)

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
          claim_states_mode: env("NORMAL_CLAIM_STATES_MODE", "cursor")
        )

      long_port =
        if long_flows > 0 do
          start_python_workload(:long_fail, port,
            flows: long_flows,
            steps: long_steps,
            terminal_mode: "fail",
            payload_bytes: payload_bytes,
            claim_states_mode: env("LONG_CLAIM_STATES_MODE", "all")
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
        max_total_mem_mb: max_total_mem_mb,
        last_disk_mb: dir_mb(data_dir),
        last_disk_at: started_native,
        max_pending_ops: 0,
        max_oldest_lag_ms: 0.0,
        max_replay_lag: 0,
        max_disk_mb: 0.0,
        max_blob_mb: 0.0,
        max_lmdb_mb: 0.0,
        max_waraft_mb: 0.0,
        max_waraft_log_entries: 0,
        max_waraft_log_ets_mb: 0.0,
        max_total_mem_mb_seen: 0.0,
        max_binary_mem_mb: 0.0,
        max_keydir_binary_mb: 0.0,
        max_flow_index_entries: 0,
        max_flow_lookup_entries: 0,
        sample_count: 0,
        outputs: %{}
      }

      state = sample_loop(state)
      print_summary(state)
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
      env("CREATE_BATCH_SIZE", "1000"),
      "--create-inflight",
      env("CREATE_INFLIGHT", "64"),
      "--create-rate-per-sec",
      env(
        "CREATE_RATE_PER_SEC",
        Integer.to_string(
          max(div(int_env("TARGET_OPS_PER_SEC", 50_000), Keyword.fetch!(opts, :steps) + 1), 1)
        )
      ),
      "--claim-batch-size",
      env("CLAIM_BATCH_SIZE", "1000"),
      "--claim-partition-batch-size",
      env("CLAIM_PARTITION_BATCH_SIZE", "32"),
      "--apply-inflight",
      env("APPLY_INFLIGHT", "8"),
      "--worker-mode",
      env("WORKER_MODE", "owner-wakeup"),
      "--claim-states-mode",
      Keyword.fetch!(opts, :claim_states_mode),
      "--wake-coalesce-ms",
      env("WAKE_COALESCE_MS", "0"),
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
      "--terminal-mode",
      Keyword.fetch!(opts, :terminal_mode),
      "--result-bytes",
      "0"
    ]

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
    total_mem_mb = bytes_to_mb(:erlang.memory(:total))
    free_mb = disk_free_mb(state.data_dir)

    cond do
      free_mb <= state.min_free_mb ->
        IO.puts("guard_stop reason=low_disk free_mb=#{free_mb}")
        kill_running_ports(state)
        %{state | ports: mark_running_stopped(state.ports, :low_disk)}

      total_mem_mb >= state.max_total_mem_mb ->
        IO.puts("guard_stop reason=high_beam_memory total_mem_mb=#{Float.round(total_mem_mb, 1)}")
        kill_running_ports(state)
        %{state | ports: mark_running_stopped(state.ports, :high_beam_memory)}

      true ->
        state
    end
  end

  defp sample_once(state) do
    lmdb = lmdb_status()
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
    telemetry = telemetry_snapshot(state.table)
    sample_count = Map.get(state, :sample_count, 0) + 1

    IO.puts(
      "sample elapsed_s=#{Float.round(elapsed_s, 1)} " <>
        "flow_ops=#{telemetry.flow_ops} flow_ops_s=#{Float.round(telemetry.flow_ops / max(elapsed_s, 0.001), 1)} " <>
        "write_ops=#{telemetry.write_ops} write_ops_s=#{Float.round(telemetry.write_ops / max(elapsed_s, 0.001), 1)} " <>
        "create=#{telemetry.create} transition=#{telemetry.transition} complete=#{telemetry.complete} " <>
        "fail=#{telemetry.fail} pipeline_write=#{telemetry.pipeline_write} claim_due=#{telemetry.claim_due} " <>
        "lmdb_pending=#{lmdb.pending_ops} lmdb_oldest_lag_ms=#{Float.round(lmdb.max_oldest_lag_ms, 2)} " <>
        "lmdb_replay_lag=#{lmdb.max_replay_safe_lag} lmdb_flush_failures=#{lmdb.flush_failures} " <>
        "lmdb_flushes=#{telemetry.lmdb_flushes} lmdb_flush_avg_us=#{telemetry.lmdb_flush_avg_us} " <>
        "waraft_flush_errors=#{telemetry.waraft_flush_errors} waraft_apply_full=#{telemetry.waraft_apply_full} " <>
        "waraft_commit_bytes_rejected=#{telemetry.waraft_commit_bytes_rejected} " <>
        "waraft_commit_timeouts=#{telemetry.waraft_commit_timeouts} " <>
        "waraft_commit_timeout_max_us=#{telemetry.waraft_commit_timeout_max_us} " <>
        "waraft_flushes=#{telemetry.waraft_flushes} waraft_queue_wait_avg_us=#{telemetry.waraft_queue_wait_avg_us} " <>
        "waraft_queue_wait_max_us=#{telemetry.waraft_queue_wait_max_us} " <>
        "payload_fsyncs=#{telemetry.payload_fsyncs} payload_fsync_avg_us=#{telemetry.payload_fsync_avg_us} " <>
        "payload_fsync_max_us=#{telemetry.payload_fsync_max_us} payload_fsync_errors=#{telemetry.payload_fsync_errors} " <>
        "blob_prepare_failures=#{telemetry.blob_prepare_failures} storage_blocked=#{telemetry.storage_blocked} " <>
        "segment_appends=#{telemetry.segment_appends} segment_mb=#{Float.round(telemetry.segment_mb, 1)} " <>
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

    maybe_print_top_binary_holders(sample_count)
    maybe_print_top_ets_binary_tables(sample_count)

    %{
      state
      | last_disk_mb: disk_mb,
        last_disk_at: now,
        max_pending_ops: max(state.max_pending_ops, lmdb.pending_ops),
        max_oldest_lag_ms: max(state.max_oldest_lag_ms, lmdb.max_oldest_lag_ms),
        max_replay_lag: max(state.max_replay_lag, lmdb.max_replay_safe_lag),
        max_disk_mb: max(state.max_disk_mb, disk_mb),
        max_blob_mb: max(state.max_blob_mb, storage.blob_mb),
        max_lmdb_mb: max(state.max_lmdb_mb, storage.lmdb_mb),
        max_waraft_mb: max(state.max_waraft_mb, storage.waraft_mb),
        max_waraft_log_entries: max(state.max_waraft_log_entries, waraft_log.entries),
        max_waraft_log_ets_mb: max(state.max_waraft_log_ets_mb, waraft_log.ets_mb),
        max_total_mem_mb_seen: max(state.max_total_mem_mb_seen, memory.total_mb),
        max_binary_mem_mb: max(state.max_binary_mem_mb, memory.binary_mb),
        max_keydir_binary_mb: max(state.max_keydir_binary_mb, keydir.binary_mb),
        max_flow_index_entries: max(state.max_flow_index_entries, flow_index.index_entries),
        max_flow_lookup_entries: max(state.max_flow_lookup_entries, flow_index.lookup_entries),
        sample_count: sample_count
    }
  end

  defp print_summary(state) do
    telemetry = telemetry_snapshot(state.table)

    IO.puts(
      "summary ports=#{inspect(Enum.map(state.ports, &{&1.name, &1.status}))} " <>
        "flow_ops=#{telemetry.flow_ops} write_ops=#{telemetry.write_ops} " <>
        "create=#{telemetry.create} transition=#{telemetry.transition} " <>
        "complete=#{telemetry.complete} fail=#{telemetry.fail} pipeline_write=#{telemetry.pipeline_write} " <>
        "claim_due=#{telemetry.claim_due} " <>
        "max_lmdb_pending=#{state.max_pending_ops} max_lmdb_oldest_lag_ms=#{Float.round(state.max_oldest_lag_ms, 2)} " <>
        "max_lmdb_replay_lag=#{state.max_replay_lag} max_disk_mb=#{Float.round(state.max_disk_mb, 1)} " <>
        "max_blob_mb=#{Float.round(state.max_blob_mb, 1)} max_lmdb_mb=#{Float.round(state.max_lmdb_mb, 1)} " <>
        "max_waraft_mb=#{Float.round(state.max_waraft_mb, 1)} " <>
        "max_waraft_log_entries=#{state.max_waraft_log_entries} " <>
        "max_waraft_log_ets_mb=#{Float.round(state.max_waraft_log_ets_mb, 1)} " <>
        "max_total_mem_mb=#{Float.round(state.max_total_mem_mb_seen, 1)} " <>
        "max_binary_mem_mb=#{Float.round(state.max_binary_mem_mb, 1)} " <>
        "max_keydir_binary_mb=#{Float.round(state.max_keydir_binary_mb, 1)} " <>
        "max_flow_index_entries=#{state.max_flow_index_entries} " <>
        "max_flow_lookup_entries=#{state.max_flow_lookup_entries} " <>
        "waraft_flush_errors=#{telemetry.waraft_flush_errors} waraft_apply_full=#{telemetry.waraft_apply_full} " <>
        "waraft_commit_timeouts=#{telemetry.waraft_commit_timeouts} " <>
        "waraft_commit_timeout_max_us=#{telemetry.waraft_commit_timeout_max_us} " <>
        "waraft_flushes=#{telemetry.waraft_flushes} waraft_queue_wait_avg_us=#{telemetry.waraft_queue_wait_avg_us} " <>
        "waraft_queue_wait_max_us=#{telemetry.waraft_queue_wait_max_us} " <>
        "payload_fsyncs=#{telemetry.payload_fsyncs} payload_fsync_avg_us=#{telemetry.payload_fsync_avg_us} " <>
        "payload_fsync_max_us=#{telemetry.payload_fsync_max_us} payload_fsync_errors=#{telemetry.payload_fsync_errors} " <>
        "blob_prepare_failures=#{telemetry.blob_prepare_failures} storage_blocked=#{telemetry.storage_blocked} " <>
        "segment_appends=#{telemetry.segment_appends} segment_mb=#{Float.round(telemetry.segment_mb, 1)}"
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

  defp attach_telemetry(handler_id, table) do
    :ok =
      :telemetry.attach_many(
        handler_id,
        @events,
        fn event, measurements, metadata, _config ->
          record_event(table, event, measurements, metadata)
        end,
        nil
      )
  end

  defp record_event(table, event, measurements, metadata) do
    key = event_key(event, metadata)
    duration_us = duration_us(measurements)
    item_count = item_count(measurements)
    bytes = bytes(measurements)
    max_us = max_metric_us(measurements, duration_us)

    :ets.update_counter(
      table,
      key,
      [{2, 1}, {3, duration_us}, {4, item_count}, {5, bytes}],
      {key, 0, 0, 0, 0}
    )

    update_max(table, {:max_us, key}, max_us)
  rescue
    _ -> :ok
  end

  defp event_key([:ferricstore, :flow, command, :stop], _metadata), do: {:flow, command}

  defp event_key([:ferricstore, :flow, :lmdb_writer, :flush], metadata),
    do: {:lmdb_flush, Map.get(metadata, :status, :unknown)}

  defp event_key([:ferricstore, :waraft, :batcher, :slot_flush], metadata),
    do: {:waraft_flush, Map.get(metadata, :result, :unknown)}

  defp event_key([:ferricstore, :waraft, :batcher, :hot_flush], metadata),
    do: {:waraft_flush, Map.get(metadata, :result, :unknown)}

  defp event_key([:ferricstore, :waraft, :segment_log, :append], metadata),
    do: {:segment_append, Map.get(metadata, :result, :unknown)}

  defp event_key([:ferricstore, :waraft, :commit_bytes, :rejected], _metadata),
    do: {:waraft_commit_bytes, :rejected}

  defp event_key([:ferricstore, :waraft, :commit, :timeout], metadata),
    do: {:waraft_commit_timeout, Map.get(metadata, :path, :unknown)}

  defp event_key([:ferricstore, :waraft, :storage_blocked], metadata),
    do: {:waraft_storage_blocked, Map.get(metadata, :reason, :unknown)}

  defp event_key([:ferricstore, :waraft, :storage, :payload_fsync], metadata),
    do: {:waraft_payload_fsync, Map.get(metadata, :result, :unknown)}

  defp event_key([:ferricstore, :waraft, :blob_prepare_failed], metadata),
    do: {:waraft_blob_prepare_failed, Map.get(metadata, :reason, :unknown)}

  defp event_key(event, _metadata), do: {:event, event}

  defp telemetry_snapshot(table) do
    create = event_items(table, {:flow, :create})
    transition = event_items(table, {:flow, :transition})
    complete = event_items(table, {:flow, :complete})
    fail = event_items(table, {:flow, :fail})
    pipeline_write = event_items(table, {:flow, :pipeline_write})
    claim_due = event_items(table, {:flow, :claim_due})
    {lmdb_flushes, lmdb_flush_us} = event_count_duration_prefix(table, :lmdb_flush)
    {waraft_flushes, waraft_queue_wait_us} = event_count_duration_prefix(table, :waraft_flush)
    {payload_fsyncs, payload_fsync_us} = event_count_duration_prefix(table, :waraft_payload_fsync)
    {segment_appends, segment_bytes} = event_count_bytes_prefix(table, :segment_append)
    waraft_flush_errors = event_count_matching(table, :waraft_flush, &match?({:error, _}, &1))
    waraft_apply_full = event_count(table, {:waraft_flush, {:error, :apply_queue_full}})
    waraft_commit_bytes_rejected = event_count(table, {:waraft_commit_bytes, :rejected})
    waraft_commit_timeouts = event_count_prefix(table, :waraft_commit_timeout)
    payload_fsync_errors = event_count(table, {:waraft_payload_fsync, :error})
    blob_prepare_failures = event_count_prefix(table, :waraft_blob_prepare_failed)
    storage_blocked = event_count_prefix(table, :waraft_storage_blocked)

    %{
      create: create,
      transition: transition,
      complete: complete,
      fail: fail,
      pipeline_write: pipeline_write,
      claim_due: claim_due,
      flow_ops: create + transition + complete + fail + pipeline_write,
      write_ops: create + transition + complete + fail + pipeline_write + claim_due,
      lmdb_flushes: lmdb_flushes,
      lmdb_flush_avg_us: if(lmdb_flushes > 0, do: div(lmdb_flush_us, lmdb_flushes), else: 0),
      waraft_flushes: waraft_flushes,
      waraft_queue_wait_avg_us:
        if(waraft_flushes > 0, do: div(waraft_queue_wait_us, waraft_flushes), else: 0),
      waraft_queue_wait_max_us: max_us(table, :waraft_flush),
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
      segment_mb: bytes_to_mb(segment_bytes)
    }
  end

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

  defp configure_app(data_dir, shards) do
    remove_data_dir(data_dir)
    File.mkdir_p!(data_dir)

    Application.put_env(:libcluster, :topologies, [])
    Application.put_env(:ferricstore, :data_dir, data_dir)
    Application.put_env(:ferricstore, :port, 0)
    Application.put_env(:ferricstore, :health_port, 0)
    Application.put_env(:ferricstore, :shard_count, shards)
    Application.put_env(:ferricstore, :protected_mode, false)
    Application.put_env(:ferricstore, :max_memory_bytes, int_env("FERRICSTORE_MAX_MEMORY", 0))
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
      int_env("FLOW_LMDB_FLUSH_INTERVAL_MS", 1_000)
    )

    Application.put_env(
      :ferricstore,
      :flow_lmdb_max_batch_ops,
      int_env("FLOW_LMDB_MAX_BATCH_OPS", 25_000)
    )

    Application.put_env(
      :ferricstore,
      :flow_lmdb_flush_chunk_ops,
      int_env("FLOW_LMDB_FLUSH_CHUNK_OPS", 10_000)
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

    put_optional_limit_env(
      [
        "FLOW_HISTORY_PROJECTOR_MAX_PENDING_ENTRIES",
        "FERRICSTORE_FLOW_HISTORY_PROJECTOR_MAX_PENDING_ENTRIES"
      ],
      :flow_history_projector_max_pending_entries
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

    Application.delete_env(:ferricstore, :waraft_log_module)
    put_optional_int_env("WARAFT_COMMIT_BATCH_INTERVAL_MS", :waraft_commit_batch_interval_ms)
    put_optional_int_env("WARAFT_COMMIT_BATCH_MAX", :waraft_commit_batch_max)
    put_optional_int_env("WARAFT_APPLY_LOG_BATCH_SIZE", :waraft_apply_log_batch_size)
    put_optional_int_env("WARAFT_APPLY_BATCH_MAX_BYTES", :waraft_apply_batch_max_bytes)
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

  defp print_header do
    IO.puts(
      "sample elapsed_s flow_ops flow_ops_s write_ops write_ops_s create transition complete fail pipeline_write claim_due " <>
        "lmdb_pending lmdb_oldest_lag_ms lmdb_replay_lag lmdb_flush_failures lmdb_flushes " <>
        "lmdb_flush_avg_us waraft_flush_errors waraft_apply_full waraft_commit_bytes_rejected " <>
        "waraft_commit_timeouts waraft_commit_timeout_max_us " <>
        "waraft_flushes waraft_queue_wait_avg_us waraft_queue_wait_max_us " <>
        "payload_fsyncs payload_fsync_avg_us payload_fsync_max_us payload_fsync_errors " <>
        "blob_prepare_failures storage_blocked " <>
        "segment_appends segment_mb disk_mb disk_growth_mb_s blob_mb blob_files lmdb_mb " <>
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
    for app <- [:ferricstore_server, :ferricstore_ecto, :ferricstore_session, :ferricstore] do
      _ = Application.stop(app)
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
  defp unique, do: System.unique_integer([:positive])

  defp int_env(name, default) do
    name |> env(Integer.to_string(default)) |> String.to_integer()
  end

  defp parse_float(value) do
    case Float.parse(value) do
      {float, _rest} ->
        float

      :error ->
        0.0
    end
  end

  defp bool_env(name, default) do
    case System.get_env(name) do
      nil -> default
      value when value in ["1", "true", "TRUE", "yes", "YES", "on", "ON"] -> true
      value when value in ["0", "false", "FALSE", "no", "NO", "off", "OFF"] -> false
      value -> raise "unsupported #{name}=#{inspect(value)}; expected boolean"
    end
  end

  defp put_optional_int_env(env_name, app_key) do
    case System.get_env(env_name) do
      nil -> :ok
      value -> Application.put_env(:ferricstore, app_key, String.to_integer(value))
    end
  end

  defp put_optional_limit_env(env_names, app_key) when is_list(env_names) do
    case Enum.find_value(env_names, fn env_name ->
           case System.get_env(env_name) do
             nil -> nil
             value -> value
           end
         end) do
      nil -> :ok
      value -> Application.put_env(:ferricstore, app_key, parse_limit_env(value))
    end
  end

  defp parse_limit_env(value) do
    case String.downcase(String.trim(value)) do
      value when value in ["", "false", "off", "infinity", "inf", "unlimited"] ->
        :infinity

      value ->
        String.to_integer(value)
    end
  end

  defp put_optional_bool_env(env_name, app_key) do
    case System.get_env(env_name) do
      nil ->
        :ok

      value when value in ["1", "true", "TRUE", "yes", "YES", "on", "ON"] ->
        Application.put_env(:ferricstore, app_key, true)

      value when value in ["0", "false", "FALSE", "no", "NO", "off", "OFF"] ->
        Application.put_env(:ferricstore, app_key, false)

      value ->
        raise "unsupported #{env_name}=#{inspect(value)}; expected boolean"
    end
  end
end

FlowStateLMDBSoak.run()
