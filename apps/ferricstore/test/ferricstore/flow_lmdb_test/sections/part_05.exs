defmodule Ferricstore.Flow.LMDBTest.Sections.Part05 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Flow.LMDBTest.FlushProbeWriter

  test "mirror writer projects terminal history from hot flow index during flush" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_flow_lmdb_history_project_#{System.unique_integer([:positive])}"
      )

    shard_index = 0
    instance_name = :"flow_lmdb_history_project_#{System.unique_integer([:positive])}"
    id = "history-project-flow"
    partition_key = "tenant-history-project"
    history_key = Ferricstore.Flow.Keys.history_key(id, partition_key)

    {flow_index, flow_lookup} =
      Ferricstore.Flow.OrderedIndex.table_names(instance_name, shard_index)

    on_exit(fn ->
      restore_env(:flow_lmdb_mode, old_mode)
      File.rm_rf!(data_dir)
    end)

    Ferricstore.DataDir.ensure_layout!(data_dir, 1)
    Ferricstore.Flow.NativeOrderedIndex.reset(flow_index, flow_lookup)

    Ferricstore.Flow.OrderedIndex.put_new_entries(flow_index, flow_lookup, [
      {history_key, "1000-1", 1_000},
      {history_key, "1001-2", 1_001}
    ])

    start_supervised!(
      {Ferricstore.Flow.LMDBWriter,
       shard_index: shard_index, data_dir: data_dir, instance_name: instance_name}
    )

    path =
      data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> Ferricstore.Flow.LMDB.path()

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
               {:history_project_from_index, flow_index, flow_lookup, id, partition_key,
                history_key, 60_000}
             ])

    assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)

    prefix = Ferricstore.Flow.LMDB.history_index_prefix(history_key)
    assert {:ok, 2} = Ferricstore.Flow.LMDB.prefix_count(path, prefix)

    assert {:ok, entries} = Ferricstore.Flow.LMDB.prefix_entries(path, prefix, 10)

    assert Enum.map(entries, fn {_key, value} ->
             {:ok, {event_id, event_ms, 60_000, compound_key}} =
               Ferricstore.Flow.LMDB.decode_history_index_value(value)

             {event_id, event_ms, compound_key}
           end) == [
             {"1000-1", 1_000,
              Ferricstore.Flow.Keys.stream_entry_key(id, "1000-1", partition_key)},
             {"1001-2", 1_001,
              Ferricstore.Flow.Keys.stream_entry_key(id, "1001-2", partition_key)}
           ]
  end

  test "mirror writer preserves projected history file locators" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_flow_lmdb_history_project_location_#{System.unique_integer([:positive])}"
      )

    shard_index = 0
    instance_name = :"flow_lmdb_history_project_location_#{System.unique_integer([:positive])}"
    id = "history-project-location-flow"
    partition_key = "tenant-history-project-location"
    history_key = Ferricstore.Flow.Keys.history_key(id, partition_key)
    event_id = "1000-1"
    event_ms = 1_000
    compound_key = Ferricstore.Flow.Keys.stream_entry_key(id, event_id, partition_key)
    history_index_key = Ferricstore.Flow.LMDB.history_index_key(history_key, event_id, event_ms)

    {flow_index, flow_lookup} =
      Ferricstore.Flow.OrderedIndex.table_names(instance_name, shard_index)

    on_exit(fn ->
      restore_env(:flow_lmdb_mode, old_mode)
      File.rm_rf!(data_dir)
    end)

    Ferricstore.DataDir.ensure_layout!(data_dir, 1)
    Ferricstore.Flow.NativeOrderedIndex.reset(flow_index, flow_lookup)

    Ferricstore.Flow.OrderedIndex.put_new_entries(flow_index, flow_lookup, [
      {history_key, event_id, event_ms}
    ])

    path =
      data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> Ferricstore.Flow.LMDB.path()

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(path, [
               {:put, history_index_key,
                Ferricstore.Flow.LMDB.encode_history_index_value(
                  event_id,
                  event_ms,
                  compound_key,
                  60_000,
                  {:flow_history, 7},
                  123,
                  456
                )}
             ])

    start_supervised!(
      {Ferricstore.Flow.LMDBWriter,
       shard_index: shard_index, data_dir: data_dir, instance_name: instance_name}
    )

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
               {:history_project_from_index, flow_index, flow_lookup, id, partition_key,
                history_key, 60_000}
             ])

    assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)

    assert {:ok, value} = Ferricstore.Flow.LMDB.get(path, history_index_key)

    assert {:ok, {^event_id, ^event_ms, 60_000, ^compound_key, {:flow_history, 7}, 123, 456}} =
             Ferricstore.Flow.LMDB.decode_history_index_location(value)
  end

  test "lagged writer defers timer flush while writes continue under max lag" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
    old_flush_jitter = Application.get_env(:ferricstore, :flow_lmdb_flush_jitter_ms)
    old_flush_quiet = Application.get_env(:ferricstore, :flow_lmdb_flush_quiet_ms)
    old_flush_max_lag = Application.get_env(:ferricstore, :flow_lmdb_flush_max_lag_ms)
    old_max_batch_ops = Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :lagged)
    Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 50)
    Application.put_env(:ferricstore, :flow_lmdb_flush_jitter_ms, 0)
    Application.put_env(:ferricstore, :flow_lmdb_flush_quiet_ms, 100)
    Application.put_env(:ferricstore, :flow_lmdb_flush_max_lag_ms, 500)
    Application.put_env(:ferricstore, :flow_lmdb_max_batch_ops, 1_000_000)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_flow_lmdb_lagged_interval_#{System.unique_integer([:positive])}"
      )

    shard_index = 0
    instance_name = :"flow_lmdb_lagged_interval_#{System.unique_integer([:positive])}"
    key1 = "flow:{flow:test}:state:lagged-interval-1"
    key2 = "flow:{flow:test}:state:lagged-interval-2"

    on_exit(fn ->
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
      restore_env(:flow_lmdb_flush_jitter_ms, old_flush_jitter)
      restore_env(:flow_lmdb_flush_quiet_ms, old_flush_quiet)
      restore_env(:flow_lmdb_flush_max_lag_ms, old_flush_max_lag)
      restore_env(:flow_lmdb_max_batch_ops, old_max_batch_ops)
      File.rm_rf!(data_dir)
    end)

    Ferricstore.DataDir.ensure_layout!(data_dir, 1)

    start_supervised!(
      {Ferricstore.Flow.LMDBWriter,
       shard_index: shard_index, data_dir: data_dir, instance_name: instance_name}
    )

    path =
      data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> Ferricstore.Flow.LMDB.path()

    now = System.monotonic_time()
    seventy_ms = System.convert_time_unit(70, :millisecond, :native)

    pending_state = %{
      pending: [{:put, key1, "v1"}],
      pending_after_flush: [],
      flush_on_max_ops?: false,
      count: 1,
      max_ops: 1_000_000,
      first_pending_at: now - seventy_ms,
      last_enqueue_at: now - seventy_ms,
      flush_quiet_ms: 100,
      flush_max_lag_ms: 500
    }

    assert {:defer, delay_ms} =
             Ferricstore.Flow.LMDBWriter.__timer_flush_decision_for_test__(
               pending_state,
               now
             )

    assert delay_ms in 1..30

    assert {:defer, delay_ms_after_second_enqueue} =
             Ferricstore.Flow.LMDBWriter.__timer_flush_decision_for_test__(
               %{pending_state | last_enqueue_at: now},
               now
             )

    assert delay_ms_after_second_enqueue == 100

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [{:put, key1, "v1"}])

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [{:put, key2, "v2"}])

    Ferricstore.Test.ShardHelpers.eventually(fn ->
      Ferricstore.Flow.LMDB.get(path, key1) == {:ok, "v1"} and
        Ferricstore.Flow.LMDB.get(path, key2) == {:ok, "v2"}
    end)
  end

  test "lagged writer flushes at max projection lag even when quiet window is not reached" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
    old_flush_jitter = Application.get_env(:ferricstore, :flow_lmdb_flush_jitter_ms)
    old_flush_quiet = Application.get_env(:ferricstore, :flow_lmdb_flush_quiet_ms)
    old_flush_max_lag = Application.get_env(:ferricstore, :flow_lmdb_flush_max_lag_ms)
    old_max_batch_ops = Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :lagged)
    Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 20)
    Application.put_env(:ferricstore, :flow_lmdb_flush_jitter_ms, 0)
    Application.put_env(:ferricstore, :flow_lmdb_flush_quiet_ms, 1_000)
    Application.put_env(:ferricstore, :flow_lmdb_flush_max_lag_ms, 100)
    Application.put_env(:ferricstore, :flow_lmdb_max_batch_ops, 1_000_000)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_flow_lmdb_lagged_max_lag_#{System.unique_integer([:positive])}"
      )

    shard_index = 0
    instance_name = :"flow_lmdb_lagged_max_lag_#{System.unique_integer([:positive])}"
    key = "flow:{flow:test}:state:lagged-max-lag"

    on_exit(fn ->
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
      restore_env(:flow_lmdb_flush_jitter_ms, old_flush_jitter)
      restore_env(:flow_lmdb_flush_quiet_ms, old_flush_quiet)
      restore_env(:flow_lmdb_flush_max_lag_ms, old_flush_max_lag)
      restore_env(:flow_lmdb_max_batch_ops, old_max_batch_ops)
      File.rm_rf!(data_dir)
    end)

    Ferricstore.DataDir.ensure_layout!(data_dir, 1)

    start_supervised!(
      {Ferricstore.Flow.LMDBWriter,
       shard_index: shard_index, data_dir: data_dir, instance_name: instance_name}
    )

    path =
      data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> Ferricstore.Flow.LMDB.path()

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [{:put, key, "v1"}])

    Ferricstore.Test.ShardHelpers.eventually(fn ->
      Ferricstore.Flow.LMDB.get(path, key) == {:ok, "v1"}
    end)
  end

  test "lagged writer coalesces replay-safe requests behind hot flush quiet window" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
    old_flush_jitter = Application.get_env(:ferricstore, :flow_lmdb_flush_jitter_ms)
    old_flush_quiet = Application.get_env(:ferricstore, :flow_lmdb_flush_quiet_ms)
    old_flush_max_lag = Application.get_env(:ferricstore, :flow_lmdb_flush_max_lag_ms)
    old_max_batch_ops = Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :lagged)
    Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 50)
    Application.put_env(:ferricstore, :flow_lmdb_flush_jitter_ms, 0)
    Application.put_env(:ferricstore, :flow_lmdb_flush_quiet_ms, 100)
    Application.put_env(:ferricstore, :flow_lmdb_flush_max_lag_ms, 500)
    Application.put_env(:ferricstore, :flow_lmdb_max_batch_ops, 1_000_000)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_flow_lmdb_replay_safe_quiet_#{System.unique_integer([:positive])}"
      )

    shard_index = 0
    instance_name = :"flow_lmdb_replay_safe_quiet_#{System.unique_integer([:positive])}"
    key1 = "flow:{flow:test}:state:replay-safe-quiet-1"
    key2 = "flow:{flow:test}:state:replay-safe-quiet-2"

    on_exit(fn ->
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
      restore_env(:flow_lmdb_flush_jitter_ms, old_flush_jitter)
      restore_env(:flow_lmdb_flush_quiet_ms, old_flush_quiet)
      restore_env(:flow_lmdb_flush_max_lag_ms, old_flush_max_lag)
      restore_env(:flow_lmdb_max_batch_ops, old_max_batch_ops)
      File.rm_rf!(data_dir)
    end)

    Ferricstore.DataDir.ensure_layout!(data_dir, 1)

    instance_ctx = %{
      name: instance_name,
      flow_lmdb_replay_safe_index: :atomics.new(1, signed: false),
      flow_lmdb_replay_safe_requested_index: :atomics.new(1, signed: false),
      flow_lmdb_replay_safe_persist_failures: :atomics.new(1, signed: false),
      flow_lmdb_mirror_degraded: :atomics.new(1, signed: false),
      flow_lmdb_writer_flush_failures: :atomics.new(1, signed: false)
    }

    start_supervised!(
      {Ferricstore.Flow.LMDBWriter,
       shard_index: shard_index,
       data_dir: data_dir,
       instance_name: instance_name,
       instance_ctx: instance_ctx}
    )

    shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
    path = Ferricstore.Flow.LMDB.path(shard_data_path)

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [{:put, key1, "v1"}])

    assert :requested =
             Ferricstore.Flow.LMDBWriter.request(instance_ctx, shard_index, shard_data_path, 123)

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [{:put, key2, "v2"}])

    Process.sleep(80)

    refute File.exists?(Path.join(path, "data.mdb"))

    Ferricstore.Test.ShardHelpers.eventually(fn ->
      Ferricstore.Flow.LMDB.get(path, key1) == {:ok, "v1"} and
        Ferricstore.Flow.LMDB.get(path, key2) == {:ok, "v2"} and
        Ferricstore.Flow.LMDBWriter.durable?(instance_ctx, shard_index, shard_data_path, 123)
    end)
  end

  test "lagged writer flushes at max ops to bound memory" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
    old_flush_jitter = Application.get_env(:ferricstore, :flow_lmdb_flush_jitter_ms)
    old_max_batch_ops = Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops)
    old_flush_on_max_ops = Application.get_env(:ferricstore, :flow_lmdb_flush_on_max_ops)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :lagged)
    Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)
    Application.put_env(:ferricstore, :flow_lmdb_flush_jitter_ms, 0)
    Application.put_env(:ferricstore, :flow_lmdb_max_batch_ops, 2)
    Application.put_env(:ferricstore, :flow_lmdb_flush_on_max_ops, true)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_flow_lmdb_lagged_cap_#{System.unique_integer([:positive])}"
      )

    shard_index = 0
    instance_name = :"flow_lmdb_lagged_cap_#{System.unique_integer([:positive])}"
    key1 = "flow:{flow:test}:state:lagged-cap-1"
    key2 = "flow:{flow:test}:state:lagged-cap-2"

    on_exit(fn ->
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
      restore_env(:flow_lmdb_flush_jitter_ms, old_flush_jitter)
      restore_env(:flow_lmdb_max_batch_ops, old_max_batch_ops)
      restore_env(:flow_lmdb_flush_on_max_ops, old_flush_on_max_ops)
      File.rm_rf!(data_dir)
    end)

    Ferricstore.DataDir.ensure_layout!(data_dir, 1)

    start_supervised!(
      {Ferricstore.Flow.LMDBWriter,
       shard_index: shard_index, data_dir: data_dir, instance_name: instance_name}
    )

    path =
      data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> Ferricstore.Flow.LMDB.path()

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [{:put, key1, "v1"}])

    assert :not_found = Ferricstore.Flow.LMDB.get(path, key1)

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [{:put, key2, "v2"}])

    Ferricstore.Test.ShardHelpers.eventually(fn ->
      Ferricstore.Flow.LMDB.get(path, key1) == {:ok, "v1"} and
        Ferricstore.Flow.LMDB.get(path, key2) == {:ok, "v2"}
    end)
  end

  test "lagged writer suspend cancels source projection timer during shutdown" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
    old_flush_jitter = Application.get_env(:ferricstore, :flow_lmdb_flush_jitter_ms)
    old_max_batch_ops = Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops)
    old_source_retries = Application.get_env(:ferricstore, :flow_lmdb_source_pending_retries)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :lagged)
    Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 20)
    Application.put_env(:ferricstore, :flow_lmdb_flush_jitter_ms, 0)
    Application.put_env(:ferricstore, :flow_lmdb_max_batch_ops, 1_000_000)
    Application.put_env(:ferricstore, :flow_lmdb_source_pending_retries, 0)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_flow_lmdb_suspend_#{System.unique_integer([:positive])}"
      )

    instance_name = :"flow_lmdb_suspend_#{System.unique_integer([:positive])}"
    shard_index = 0
    key = "flow:{flow:suspend}:state:a"
    keydir = :ets.new(:flow_lmdb_suspend_keydir, [:set, :public])

    instance_ctx = %{
      name: instance_name,
      keydir_refs: {keydir},
      flow_lmdb_writer_flush_failures: :atomics.new(1, signed: false)
    }

    test_pid = self()
    handler_id = {:flow_lmdb_writer_suspend, self(), make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :flow, :lmdb_writer, :flush],
      fn event, measurements, metadata, _config ->
        send(test_pid, {:flow_lmdb_writer_suspend, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
      restore_env(:flow_lmdb_flush_jitter_ms, old_flush_jitter)
      restore_env(:flow_lmdb_max_batch_ops, old_max_batch_ops)
      restore_env(:flow_lmdb_source_pending_retries, old_source_retries)
      File.rm_rf!(data_dir)
    end)

    Ferricstore.DataDir.ensure_layout!(data_dir, 1)

    start_supervised!(
      {Ferricstore.Flow.LMDBWriter,
       shard_index: shard_index, data_dir: data_dir, instance_ctx: instance_ctx}
    )

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
               {:project_kv_from_source, key}
             ])

    assert :ok = Ferricstore.Flow.LMDBWriter.suspend(instance_name, shard_index)
    true = :ets.delete(keydir)

    assert_receive {:flow_lmdb_writer_suspend, [:ferricstore, :flow, :lmdb_writer, :flush],
                    %{op_count: 1}, %{status: :ok}},
                   500

    refute_receive {:flow_lmdb_writer_suspend, [:ferricstore, :flow, :lmdb_writer, :flush],
                    _measurements, _metadata},
                   80

    assert :atomics.get(instance_ctx.flow_lmdb_writer_flush_failures, 1) == 0
  end

  test "mirror writer can flush a single shard without draining others" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
    old_max_batch_ops = Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)
    Application.put_env(:ferricstore, :flow_lmdb_max_batch_ops, 1_000_000)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_flow_lmdb_single_flush_#{System.unique_integer([:positive])}"
      )

    instance_name = :"flow_lmdb_single_flush_#{System.unique_integer([:positive])}"
    key0 = "flow:{flow:zero}:state:a"
    key1 = "flow:{flow:one}:state:b"

    on_exit(fn ->
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
      restore_env(:flow_lmdb_max_batch_ops, old_max_batch_ops)
      File.rm_rf!(data_dir)
    end)

    Ferricstore.DataDir.ensure_layout!(data_dir, 2)

    start_supervised!(
      {Ferricstore.Flow.LMDBWriter,
       shard_index: 0, data_dir: data_dir, instance_name: instance_name},
      id: {Ferricstore.Flow.LMDBWriter, instance_name, 0}
    )

    start_supervised!(
      {Ferricstore.Flow.LMDBWriter,
       shard_index: 1, data_dir: data_dir, instance_name: instance_name},
      id: {Ferricstore.Flow.LMDBWriter, instance_name, 1}
    )

    path0 =
      data_dir
      |> Ferricstore.DataDir.shard_data_path(0)
      |> Ferricstore.Flow.LMDB.path()

    path1 =
      data_dir
      |> Ferricstore.DataDir.shard_data_path(1)
      |> Ferricstore.Flow.LMDB.path()

    assert :ok = Ferricstore.Flow.LMDBWriter.enqueue(instance_name, 0, [{:put, key0, "v0"}])
    assert :ok = Ferricstore.Flow.LMDBWriter.enqueue(instance_name, 1, [{:put, key1, "v1"}])

    assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, 1)
    assert {:ok, "v1"} = Ferricstore.Flow.LMDB.get(path1, key1)
    assert :not_found = Ferricstore.Flow.LMDB.get(path0, key0)

    assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, 0)
    assert {:ok, "v0"} = Ferricstore.Flow.LMDB.get(path0, key0)
  end

  test "mirror writer emits backlog and flush telemetry" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
    old_max_batch_ops = Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)
    Application.put_env(:ferricstore, :flow_lmdb_max_batch_ops, 1_000_000)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_flow_lmdb_writer_telemetry_#{System.unique_integer([:positive])}"
      )

    instance_name = :"flow_lmdb_writer_telemetry_#{System.unique_integer([:positive])}"
    shard_index = 0
    key = "flow:{flow:telemetry}:state:a"
    pending_ops = :atomics.new(1, signed: false)
    oldest_pending_age_us = :atomics.new(1, signed: false)
    flush_failures = :atomics.new(1, signed: false)

    instance_ctx = %{
      name: instance_name,
      flow_lmdb_writer_pending_ops: pending_ops,
      flow_lmdb_writer_oldest_pending_age_us: oldest_pending_age_us,
      flow_lmdb_writer_flush_failures: flush_failures
    }

    test_pid = self()
    handler_id = {:flow_lmdb_writer_telemetry, self(), make_ref()}

    :telemetry.attach_many(
      handler_id,
      [
        [:ferricstore, :flow, :lmdb_writer, :backlog],
        [:ferricstore, :flow, :lmdb_writer, :flush]
      ],
      fn event, measurements, metadata, _config ->
        send(test_pid, {:flow_lmdb_writer_telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
      restore_env(:flow_lmdb_max_batch_ops, old_max_batch_ops)
      File.rm_rf!(data_dir)
    end)

    Ferricstore.DataDir.ensure_layout!(data_dir, 1)

    start_supervised!(
      {Ferricstore.Flow.LMDBWriter,
       shard_index: shard_index, data_dir: data_dir, instance_ctx: instance_ctx}
    )

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
               {:put, key, "v1"},
               {:put, key, "v2"}
             ])

    assert_receive {:flow_lmdb_writer_telemetry, [:ferricstore, :flow, :lmdb_writer, :backlog],
                    backlog,
                    %{shard_index: ^shard_index, instance_name: ^instance_name} = backlog_meta}

    assert backlog.pending_ops == 2
    assert backlog.pending_after_flush == 0
    assert backlog.oldest_pending_age_us >= 0
    assert backlog.replay_safe_lag == 0
    assert backlog_meta.shard_index == shard_index
    assert backlog_meta.instance_name == instance_name
    assert :atomics.get(pending_ops, 1) == 2
    assert :atomics.get(oldest_pending_age_us, 1) >= 0

    assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)

    assert_receive {:flow_lmdb_writer_telemetry, [:ferricstore, :flow, :lmdb_writer, :flush],
                    flush,
                    %{status: :ok, shard_index: ^shard_index, instance_name: ^instance_name} =
                      flush_meta}

    assert flush.op_count == 2
    assert flush.expanded_op_count >= 2
    assert flush.duration_us >= 0
    assert flush.pending_age_us >= 0
    assert flush.replay_safe_lag == 0
    assert flush_meta.status == :ok
    assert flush_meta.shard_index == shard_index
    assert flush_meta.instance_name == instance_name
    assert :atomics.get(pending_ops, 1) == 0
    assert :atomics.get(oldest_pending_age_us, 1) == 0
    assert :atomics.get(flush_failures, 1) == 0
  end

  test "mirror writer flush failure marks shard degraded" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_flow_lmdb_writer_flush_failure_#{System.unique_integer([:positive])}"
      )

    instance_name = :"flow_lmdb_writer_flush_failure_#{System.unique_integer([:positive])}"
    shard_index = 0
    flush_failures = :atomics.new(1, signed: false)
    degraded = :atomics.new(1, signed: false)

    instance_ctx = %{
      name: instance_name,
      flow_lmdb_writer_flush_failures: flush_failures,
      flow_lmdb_mirror_degraded: degraded
    }

    test_pid = self()
    handler_id = {:flow_lmdb_writer_flush_degraded, self(), make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :flow, :lmdb_mirror, :degraded],
      fn event, measurements, metadata, _config ->
        send(test_pid, {:flow_lmdb_writer_flush_degraded, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
      File.rm_rf!(data_dir)
    end)

    Ferricstore.DataDir.ensure_layout!(data_dir, 1)

    start_supervised!(
      {Ferricstore.Flow.LMDBWriter,
       shard_index: shard_index, data_dir: data_dir, instance_ctx: instance_ctx}
    )

    assert :ok = Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [{:bad_op}])
    assert {:error, _reason} = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)

    assert :atomics.get(flush_failures, 1) == 1
    assert :atomics.get(degraded, 1) == 1

    assert_receive {:flow_lmdb_writer_flush_degraded,
                    [:ferricstore, :flow, :lmdb_mirror, :degraded], %{count: 1},
                    %{shard_index: ^shard_index, source: :flush}}
  end

  test "mirror writer enqueue and flush failures are visible" do
    instance_name = :"flow_lmdb_missing_writer_#{System.unique_integer([:positive])}"
    shard_index = 19
    key = "flow:{flow:missing}:state:a"
    test_pid = self()
    handler_id = {:flow_lmdb_writer_unavailable, self(), make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :flow, :lmdb_writer, :unavailable],
      fn event, measurements, metadata, _config ->
        send(test_pid, {:flow_lmdb_writer_unavailable, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert {:error, :writer_not_started} =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [{:put, key, "v1"}])

    assert_receive {:flow_lmdb_writer_unavailable,
                    [:ferricstore, :flow, :lmdb_writer, :unavailable], %{op_count: 1},
                    %{
                      operation: :enqueue,
                      instance_name: ^instance_name,
                      shard_index: ^shard_index,
                      reason: :writer_not_started
                    }}

    assert {:error, :writer_not_started} =
             Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index, 10)

    assert_receive {:flow_lmdb_writer_unavailable,
                    [:ferricstore, :flow, :lmdb_writer, :unavailable], %{op_count: 0},
                    %{
                      operation: :flush,
                      instance_name: ^instance_name,
                      shard_index: ^shard_index,
                      reason: :writer_not_started
                    }}
  end

  test "state-machine mirror enqueue failure marks shard degraded" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_trap = Process.flag(:trap_exit, true)
    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1)
    writer_name = Ferricstore.Flow.LMDBWriter.name(ctx.name, 0)
    writer_pid = Process.whereis(writer_name)
    test_pid = self()
    handler_id = {:flow_lmdb_mirror_degraded, self(), make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :flow, :lmdb_mirror, :degraded],
      fn event, measurements, metadata, _config ->
        send(test_pid, {:flow_lmdb_mirror_degraded, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      Process.flag(:trap_exit, old_trap)
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
    end)

    Process.exit(writer_pid, :kill)
    assert_receive {:EXIT, ^writer_pid, :killed}

    assert :ok =
             Ferricstore.Flow.create(ctx, "mirror-enqueue-degraded",
               type: "mirror-enqueue-degraded",
               partition_key: "tenant-mirror-enqueue-degraded",
               correlation_id: "correlation-mirror-enqueue-degraded",
               run_at_ms: 1,
               now_ms: 1
             )

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, "mirror-enqueue-degraded",
               partition_key: "tenant-mirror-enqueue-degraded",
               worker: "worker-mirror-enqueue-degraded",
               now_ms: 2
             )

    assert :ok =
             Ferricstore.Flow.complete(ctx, claimed.id, claimed.lease_token,
               partition_key: "tenant-mirror-enqueue-degraded",
               fencing_token: claimed.fencing_token,
               now_ms: 3
             )

    assert :atomics.get(ctx.flow_lmdb_mirror_enqueue_failures, 1) == 1
    assert :atomics.get(ctx.flow_lmdb_mirror_degraded, 1) == 1

    assert_receive {:flow_lmdb_mirror_degraded, [:ferricstore, :flow, :lmdb_mirror, :degraded],
                    %{count: 1}, %{shard_index: 0, reason: :writer_not_started}}
  end
    end
  end
end
