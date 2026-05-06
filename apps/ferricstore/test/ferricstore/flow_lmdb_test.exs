defmodule Ferricstore.Flow.LMDBTest do
  use ExUnit.Case, async: false

  test "warm opens an empty shard env before first user read" do
    path =
      Path.join(System.tmp_dir!(), "ferricstore_flow_lmdb_#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf!(path) end)

    refute File.exists?(path)
    assert :ok = Ferricstore.Flow.LMDB.warm(path)
    assert File.dir?(path)
    assert :not_found = Ferricstore.Flow.LMDB.get(path, "missing")
  end

  test "stores, reads, overwrites, and deletes raw flow state blobs" do
    path =
      Path.join(System.tmp_dir!(), "ferricstore_flow_lmdb_#{System.unique_integer([:positive])}")

    key = "flow:{flow:test}:state:a"

    on_exit(fn -> File.rm_rf!(path) end)

    assert :ok = Ferricstore.Flow.LMDB.write_batch(path, [{:put, key, "v1"}])
    assert {:ok, "v1"} = Ferricstore.Flow.LMDB.get(path, key)

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(path, [
               {:put, key, "v2"},
               {:put, key <> ":other", "v3"}
             ])

    assert {:ok, "v2"} = Ferricstore.Flow.LMDB.get(path, key)
    assert {:ok, "v3"} = Ferricstore.Flow.LMDB.get(path, key <> ":other")

    assert :ok = Ferricstore.Flow.LMDB.write_batch(path, [{:delete, key}])
    assert :not_found = Ferricstore.Flow.LMDB.get(path, key)
  end

  test "get_many returns ordered values and misses" do
    path =
      Path.join(System.tmp_dir!(), "ferricstore_flow_lmdb_#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf!(path) end)

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(path, [
               {:put, "a", "1"},
               {:put, "c", "3"}
             ])

    assert {:ok, [{:ok, "1"}, :not_found, {:ok, "3"}]} =
             Ferricstore.Flow.LMDB.get_many(path, ["a", "b", "c"])
  end

  test "query index values carry state keys and decode legacy values" do
    state_key = Ferricstore.Flow.Keys.state_key("flow-query-value", "tenant-query-value")

    value =
      Ferricstore.Flow.LMDB.encode_query_index_value("flow-query-value", 42, 1_000, state_key)

    assert {:ok, {"flow-query-value", 42, 1_000, ^state_key}} =
             Ferricstore.Flow.LMDB.decode_query_index_value(value)

    legacy_expiring = :erlang.term_to_binary({"flow-query-value", 43, 2_000})
    legacy_permanent = :erlang.term_to_binary({"flow-query-value", 44})

    assert {:ok, {"flow-query-value", 43, 2_000, nil}} =
             Ferricstore.Flow.LMDB.decode_query_index_value(legacy_expiring)

    assert {:ok, {"flow-query-value", 44, 0, nil}} =
             Ferricstore.Flow.LMDB.decode_query_index_value(legacy_permanent)
  end

  test "terminal_counts batches count reads and caches missing counts as zero" do
    path =
      Path.join(System.tmp_dir!(), "ferricstore_flow_lmdb_#{System.unique_integer([:positive])}")

    completed_key = "flow:{flow:test}:idx:completed"
    failed_key = "flow:{flow:test}:idx:failed"

    on_exit(fn -> File.rm_rf!(path) end)

    assert {:ok, [0, 0]} =
             Ferricstore.Flow.LMDB.terminal_counts(path, [completed_key, failed_key])

    assert :ok = Ferricstore.Flow.LMDB.put_terminal_count(path, completed_key, 7)

    assert {:ok, [7, 0]} =
             Ferricstore.Flow.LMDB.terminal_counts(path, [completed_key, failed_key])
  end

  test "terminal index keys preserve numeric timestamp order in bounded prefix reads" do
    path =
      Path.join(System.tmp_dir!(), "ferricstore_flow_lmdb_#{System.unique_integer([:positive])}")

    state_index_key = "flow:{flow:test}:idx:completed"
    prefix = Ferricstore.Flow.LMDB.terminal_index_prefix(state_index_key)
    older_key = Ferricstore.Flow.LMDB.terminal_index_key(state_index_key, "older", 999)
    newer_key = Ferricstore.Flow.LMDB.terminal_index_key(state_index_key, "newer", 1_000)
    count_key = Ferricstore.Flow.LMDB.terminal_count_key(state_index_key)

    older_value =
      Ferricstore.Flow.LMDB.encode_terminal_index_value("older", 999, 0, nil, count_key)

    newer_value =
      Ferricstore.Flow.LMDB.encode_terminal_index_value("newer", 1_000, 0, nil, count_key)

    on_exit(fn -> File.rm_rf!(path) end)

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(path, [
               {:put, newer_key, newer_value},
               {:put, older_key, older_value}
             ])

    assert {:ok, [{^older_key, ^older_value}]} =
             Ferricstore.Flow.LMDB.prefix_entries(path, prefix, 1)
  end

  test "history index keys preserve numeric timestamp order in bounded prefix reads" do
    path =
      Path.join(System.tmp_dir!(), "ferricstore_flow_lmdb_#{System.unique_integer([:positive])}")

    history_key = Ferricstore.Flow.Keys.history_key("history-order", "tenant-history-order")
    prefix = Ferricstore.Flow.LMDB.history_index_prefix(history_key)
    older_key = Ferricstore.Flow.LMDB.history_index_key(history_key, "999-1", 999)
    newer_key = Ferricstore.Flow.LMDB.history_index_key(history_key, "1000-2", 1_000)
    older_value = Ferricstore.Flow.LMDB.encode_history_index_value("999-1", 999, "X:older")
    newer_value = Ferricstore.Flow.LMDB.encode_history_index_value("1000-2", 1_000, "X:newer")

    on_exit(fn -> File.rm_rf!(path) end)

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(path, [
               {:put, newer_key, newer_value},
               {:put, older_key, older_value}
             ])

    assert {:ok, [{^older_key, ^older_value}]} =
             Ferricstore.Flow.LMDB.prefix_entries(path, prefix, 1)
  end

  test "flow LMDB mode defaults to mirror and legacy off cannot disable it" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)

    on_exit(fn -> restore_env(:flow_lmdb_mode, old_mode) end)

    Application.delete_env(:ferricstore, :flow_lmdb_mode)
    assert Ferricstore.Flow.LMDB.enabled?()
    assert Ferricstore.Flow.LMDB.mode() == :mirror
    assert Ferricstore.Flow.LMDB.mirror?()

    for legacy_off <- [:off, false, "false", "0", "off"] do
      Application.put_env(:ferricstore, :flow_lmdb_mode, legacy_off)
      assert Ferricstore.Flow.LMDB.mode() == :mirror
      assert Ferricstore.Flow.LMDB.mirror?()
    end
  end

  test "batch write can return pre-batch originals for rollback" do
    path =
      Path.join(System.tmp_dir!(), "ferricstore_flow_lmdb_#{System.unique_integer([:positive])}")

    key = "flow:{flow:test}:state:rollback"

    on_exit(fn -> File.rm_rf!(path) end)

    assert {:ok, [{^key, :missing}]} =
             Ferricstore.Flow.LMDB.write_batch_with_originals(path, [{:put_new, key, "v1"}])

    assert {:ok, "v1"} = Ferricstore.Flow.LMDB.get(path, key)

    assert {:ok, [{^key, {:value, "v1"}}]} =
             Ferricstore.Flow.LMDB.write_batch_with_originals(path, [
               {:put, key, "v2"},
               {:put, key, "v3"}
             ])

    assert {:ok, "v3"} = Ferricstore.Flow.LMDB.get(path, key)
  end

  test "put_new preserves existing LMDB values" do
    path =
      Path.join(System.tmp_dir!(), "ferricstore_flow_lmdb_#{System.unique_integer([:positive])}")

    key = "flow:{flow:test}:state:existing"

    on_exit(fn -> File.rm_rf!(path) end)

    assert :ok = Ferricstore.Flow.LMDB.write_batch(path, [{:put, key, "v1"}])
    assert :ok = Ferricstore.Flow.LMDB.write_batch(path, [{:put_new, key, "v2"}])
    assert {:ok, "v1"} = Ferricstore.Flow.LMDB.get(path, key)

    assert {:ok, [{^key, {:value, "v1"}}]} =
             Ferricstore.Flow.LMDB.write_batch_with_originals(path, [{:put_new, key, "v3"}])

    assert {:ok, "v1"} = Ferricstore.Flow.LMDB.get(path, key)
  end

  test "batch write keeps duplicate key order but may sort unique keys internally" do
    path =
      Path.join(System.tmp_dir!(), "ferricstore_flow_lmdb_#{System.unique_integer([:positive])}")

    key = "flow:{flow:test}:state:dup"
    low = "flow:{flow:test}:state:a"
    high = "flow:{flow:test}:state:z"

    on_exit(fn -> File.rm_rf!(path) end)

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(path, [
               {:put, key, "v1"},
               {:put, key, "v2"}
             ])

    assert {:ok, "v2"} = Ferricstore.Flow.LMDB.get(path, key)

    assert {:ok, originals} =
             Ferricstore.Flow.LMDB.write_batch_with_originals(path, [
               {:put_new, high, "high"},
               {:put_new, low, "low"}
             ])

    assert %{^high => :missing, ^low => :missing} = Map.new(originals)
    assert {:ok, "high"} = Ferricstore.Flow.LMDB.get(path, high)
    assert {:ok, "low"} = Ferricstore.Flow.LMDB.get(path, low)
  end

  test "mirror writer batches idempotent full-record puts off the caller path" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_flow_lmdb_writer_#{System.unique_integer([:positive])}"
      )

    shard_index = 0
    instance_name = :"flow_lmdb_writer_#{System.unique_integer([:positive])}"
    key = "flow:{flow:test}:state:mirror"

    on_exit(fn ->
      restore_env(:flow_lmdb_mode, old_mode)
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
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
               {:put, key, "v1"},
               {:put, key, "v1"},
               {:put, key, "v2"}
             ])

    assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)
    assert {:ok, "v2"} = Ferricstore.Flow.LMDB.get(path, key)

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [{:put, key, "v2"}])

    assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)
    assert {:ok, "v2"} = Ferricstore.Flow.LMDB.get(path, key)
  end

  test "active mirror writes do not initialize terminal counters" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
    end)

    type = "writer-zero-counts"
    partition_key = "tenant-zero-counts"

    assert {:ok, _flow} =
             Ferricstore.Store.Router.flow_create(ctx, %{
               id: "flow-zero-counts",
               type: type,
               state: "queued",
               run_at_ms: 1,
               partition_key: partition_key,
               now_ms: 1
             })

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

    path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(0)
      |> Ferricstore.Flow.LMDB.path()

    for terminal_state <- ["completed", "failed", "cancelled"] do
      state_index_key = Ferricstore.Flow.Keys.state_index_key(type, terminal_state, partition_key)
      assert :not_found = Ferricstore.Flow.LMDB.terminal_count(path, state_index_key)
    end
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
       shard_index: shard_index, data_dir: data_dir, instance_name: instance_name}
    )

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
               {:put, key, "v1"},
               {:put, key, "v2"}
             ])

    assert_receive {:flow_lmdb_writer_telemetry, [:ferricstore, :flow, :lmdb_writer, :backlog],
                    backlog, backlog_meta}

    assert backlog.pending_ops == 2
    assert backlog.pending_after_flush == 0
    assert backlog.oldest_pending_age_us >= 0
    assert backlog.replay_safe_lag == 0
    assert backlog_meta.shard_index == shard_index
    assert backlog_meta.instance_name == instance_name

    assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)

    assert_receive {:flow_lmdb_writer_telemetry, [:ferricstore, :flow, :lmdb_writer, :flush],
                    flush, flush_meta}

    assert flush.op_count == 2
    assert flush.expanded_op_count >= 2
    assert flush.duration_us >= 0
    assert flush.pending_age_us >= 0
    assert flush.replay_safe_lag == 0
    assert flush_meta.status == :ok
    assert flush_meta.shard_index == shard_index
    assert flush_meta.instance_name == instance_name
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

    assert {:ok, %{id: "mirror-enqueue-degraded"}} =
             Ferricstore.Flow.create(ctx, "mirror-enqueue-degraded",
               type: "mirror-enqueue-degraded",
               partition_key: "tenant-mirror-enqueue-degraded",
               correlation_id: "correlation-mirror-enqueue-degraded"
             )

    assert :atomics.get(ctx.flow_lmdb_mirror_enqueue_failures, 1) == 1
    assert :atomics.get(ctx.flow_lmdb_mirror_degraded, 1) == 1

    assert_receive {:flow_lmdb_mirror_degraded, [:ferricstore, :flow, :lmdb_mirror, :degraded],
                    %{count: 1}, %{shard_index: 0, reason: :writer_not_started}}
  end

  test "mirror writer persists replay-safe marker only after pending ops flush" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_flow_lmdb_writer_marker_#{System.unique_integer([:positive])}"
      )

    shard_index = 42
    instance_name = :"flow_lmdb_writer_marker_#{System.unique_integer([:positive])}"
    shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
    key = "flow:{flow:test}:state:marker"
    atomics_size = shard_index + 1
    durable = :atomics.new(atomics_size, signed: false)
    requested = :atomics.new(atomics_size, signed: false)
    failures = :atomics.new(atomics_size, signed: false)

    instance_ctx = %{
      name: instance_name,
      flow_lmdb_replay_safe_index: durable,
      flow_lmdb_replay_safe_requested_index: requested,
      flow_lmdb_replay_safe_persist_failures: failures
    }

    on_exit(fn ->
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
      File.rm_rf!(data_dir)
    end)

    File.mkdir_p!(shard_data_path)

    start_supervised!(
      {Ferricstore.Flow.LMDBWriter,
       shard_index: shard_index,
       data_dir: data_dir,
       instance_ctx: instance_ctx,
       instance_name: instance_name}
    )

    path = Ferricstore.Flow.LMDB.path(shard_data_path)

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [{:put, key, "v1"}])

    assert :requested =
             Ferricstore.Flow.LMDBWriter.request(instance_ctx, shard_index, shard_data_path, 123)

    assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)

    assert {:ok, "v1"} = Ferricstore.Flow.LMDB.get(path, key)
    assert :atomics.get(durable, shard_index + 1) == 123
    assert Ferricstore.Flow.LMDBReplaySafeIndex.read(shard_data_path) == 123
  end

  test "mirror writer persists replay-safe marker with no pending ops" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_flow_lmdb_writer_empty_marker_#{System.unique_integer([:positive])}"
      )

    shard_index = 3
    instance_name = :"flow_lmdb_writer_empty_marker_#{System.unique_integer([:positive])}"
    shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
    atomics_size = shard_index + 1
    durable = :atomics.new(atomics_size, signed: false)
    requested = :atomics.new(atomics_size, signed: false)
    failures = :atomics.new(atomics_size, signed: false)

    instance_ctx = %{
      name: instance_name,
      flow_lmdb_replay_safe_index: durable,
      flow_lmdb_replay_safe_requested_index: requested,
      flow_lmdb_replay_safe_persist_failures: failures
    }

    on_exit(fn ->
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
      File.rm_rf!(data_dir)
    end)

    File.mkdir_p!(shard_data_path)

    pid =
      start_supervised!(
        {Ferricstore.Flow.LMDBWriter,
         shard_index: shard_index,
         data_dir: data_dir,
         instance_ctx: instance_ctx,
         instance_name: instance_name}
      )

    assert :requested =
             Ferricstore.Flow.LMDBWriter.request(instance_ctx, shard_index, shard_data_path, 321)

    assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)
    assert Process.alive?(pid)
    assert :atomics.get(durable, shard_index + 1) == 321
    assert Ferricstore.Flow.LMDBReplaySafeIndex.read(shard_data_path) == 321
  end

  test "mirror writer maintains terminal counts and TTL index atomically" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_flow_lmdb_writer_terminal_#{System.unique_integer([:positive])}"
      )

    shard_index = 7
    instance_name = :"flow_lmdb_writer_terminal_#{System.unique_integer([:positive])}"
    shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
    state_index_key = Ferricstore.Flow.Keys.state_index_key("kind", "completed", "tenant")
    terminal_key = Ferricstore.Flow.LMDB.terminal_index_key(state_index_key, "flow-a", 10)
    state_key = Ferricstore.Flow.Keys.state_key("flow-a", "tenant")
    reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)
    count_key = Ferricstore.Flow.LMDB.terminal_count_key(state_index_key)
    expire_at_ms = System.os_time(:millisecond) + 60_000
    expire_key = Ferricstore.Flow.LMDB.terminal_expire_key(expire_at_ms, terminal_key)

    value =
      Ferricstore.Flow.LMDB.encode_terminal_index_value(
        "flow-a",
        10,
        expire_at_ms,
        state_key,
        count_key
      )

    on_exit(fn ->
      restore_env(:flow_lmdb_mode, old_mode)
      File.rm_rf!(data_dir)
    end)

    File.mkdir_p!(shard_data_path)

    start_supervised!(
      {Ferricstore.Flow.LMDBWriter,
       shard_index: shard_index, data_dir: data_dir, instance_name: instance_name}
    )

    path = Ferricstore.Flow.LMDB.path(shard_data_path)

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
               {:terminal_put, terminal_key, value, state_key, count_key},
               {:terminal_put, terminal_key, value, state_key, count_key}
             ])

    assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)

    assert {:ok, ^value} = Ferricstore.Flow.LMDB.get(path, terminal_key)
    assert {:ok, ^terminal_key} = Ferricstore.Flow.LMDB.get(path, reverse_key)
    assert {:ok, 1} = Ferricstore.Flow.LMDB.terminal_count(path, state_index_key)
    assert {:ok, _expire_value} = Ferricstore.Flow.LMDB.get(path, expire_key)

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
               {:terminal_delete, terminal_key, state_key, count_key}
             ])

    assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)

    assert :not_found = Ferricstore.Flow.LMDB.get(path, terminal_key)
    assert :not_found = Ferricstore.Flow.LMDB.get(path, reverse_key)
    assert :not_found = Ferricstore.Flow.LMDB.get(path, expire_key)
    assert {:ok, 0} = Ferricstore.Flow.LMDB.terminal_count(path, state_index_key)
  end

  test "mirror writer maintains terminal metadata index without state reverse pointer" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_flow_lmdb_writer_terminal_metadata_#{System.unique_integer([:positive])}"
      )

    shard_index = 8
    instance_name = :"flow_lmdb_writer_terminal_metadata_#{System.unique_integer([:positive])}"
    shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
    metadata_index_key = Ferricstore.Flow.Keys.root_index_key("root-a", "tenant")
    terminal_key = Ferricstore.Flow.LMDB.terminal_index_key(metadata_index_key, "flow-a", 10)
    count_key = Ferricstore.Flow.LMDB.terminal_count_key(metadata_index_key)
    expire_at_ms = System.os_time(:millisecond) + 60_000
    expire_key = Ferricstore.Flow.LMDB.terminal_expire_key(expire_at_ms, terminal_key)

    value =
      Ferricstore.Flow.LMDB.encode_terminal_index_value(
        "flow-a",
        10,
        expire_at_ms,
        nil,
        count_key
      )

    on_exit(fn ->
      restore_env(:flow_lmdb_mode, old_mode)
      File.rm_rf!(data_dir)
    end)

    File.mkdir_p!(shard_data_path)

    start_supervised!(
      {Ferricstore.Flow.LMDBWriter,
       shard_index: shard_index, data_dir: data_dir, instance_name: instance_name}
    )

    path = Ferricstore.Flow.LMDB.path(shard_data_path)

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
               {:terminal_put, terminal_key, value, nil, count_key},
               {:terminal_put, terminal_key, value, nil, count_key}
             ])

    assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)

    assert {:ok, ^value} = Ferricstore.Flow.LMDB.get(path, terminal_key)
    assert {:ok, _expire_value} = Ferricstore.Flow.LMDB.get(path, expire_key)
    assert {:ok, 1} = Ferricstore.Flow.LMDB.terminal_count(path, metadata_index_key)

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
               {:terminal_delete, terminal_key, nil, count_key}
             ])

    assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)

    assert :not_found = Ferricstore.Flow.LMDB.get(path, terminal_key)
    assert :not_found = Ferricstore.Flow.LMDB.get(path, expire_key)
    assert {:ok, 0} = Ferricstore.Flow.LMDB.terminal_count(path, metadata_index_key)
  end

  test "mirror flow reads reject stale LMDB record and fall back to Bitcask truth" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
    old_max_batch_ops = Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops)
    old_hot_ttl = Application.get_env(:ferricstore, :flow_terminal_hot_ttl_ms)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)
    Application.put_env(:ferricstore, :flow_lmdb_max_batch_ops, 1_000_000)
    Application.put_env(:ferricstore, :flow_terminal_hot_ttl_ms, 0)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
      restore_env(:flow_lmdb_max_batch_ops, old_max_batch_ops)
      restore_env(:flow_terminal_hot_ttl_ms, old_hot_ttl)
    end)

    partition_key = "tenant-a"
    flow_type = "type-a"
    root_flow_id = "root-a"
    correlation_id = "order-a"

    assert {:ok, flow} =
             Ferricstore.Store.Router.flow_create(ctx, %{
               id: "flow-a",
               type: flow_type,
               state: "queued",
               payload_ref: "payload-a",
               root_flow_id: root_flow_id,
               correlation_id: correlation_id,
               partition_key: partition_key,
               now_ms: 1
             })

    assert flow.version == 1
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

    assert {:ok, [claimed]} =
             Ferricstore.Store.Router.flow_claim_due(ctx, %{
               type: flow_type,
               state: "queued",
               priority: nil,
               worker: "worker-a",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 2,
               partition_key: partition_key
             })

    assert claimed.version == 2

    assert {:ok, completed} =
             Ferricstore.Store.Router.flow_complete(ctx, %{
               id: claimed.id,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               result_ref: "result-a",
               now_ms: 3,
               partition_key: partition_key
             })

    assert completed.version == 3
    assert completed.state == "completed"
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

    state_key = Ferricstore.Flow.Keys.state_key(completed.id, partition_key)
    assert [] = :ets.lookup(elem(ctx.keydir_refs, 0), state_key)

    assert {:ok, encoded_completed} =
             Ferricstore.Flow.get(ctx, completed.id, partition_key: partition_key)

    assert encoded_completed.state == "completed"

    index_key = Ferricstore.Flow.Keys.state_index_key(flow_type, "completed", partition_key)
    prefix = Ferricstore.Flow.LMDB.terminal_index_prefix(index_key)
    path = ctx.data_dir |> Ferricstore.DataDir.shard_data_path(0) |> Ferricstore.Flow.LMDB.path()

    assert {:ok, 1} = Ferricstore.Flow.LMDB.prefix_count(path, prefix)

    root_index_key = Ferricstore.Flow.Keys.root_index_key(root_flow_id, partition_key)
    root_prefix = Ferricstore.Flow.LMDB.query_index_prefix(root_index_key)

    correlation_index_key =
      Ferricstore.Flow.Keys.correlation_index_key(correlation_id, partition_key)

    correlation_prefix = Ferricstore.Flow.LMDB.query_index_prefix(correlation_index_key)

    assert {:ok, 1} = Ferricstore.Flow.LMDB.prefix_count(path, root_prefix)
    assert {:ok, 1} = Ferricstore.Flow.LMDB.prefix_count(path, correlation_prefix)

    assert {:ok, []} =
             Ferricstore.Store.Router.zset_rank_range(ctx, root_index_key, 0, 10, false)

    assert {:ok, []} =
             Ferricstore.Store.Router.zset_rank_range(ctx, correlation_index_key, 0, 10, false)

    created_event_id = "1-1"

    assert {:ok, rewound} =
             Ferricstore.Store.Router.flow_rewind(ctx, %{
               id: completed.id,
               to_event: created_event_id,
               expect_state: "completed",
               now_ms: 4,
               partition_key: partition_key
             })

    assert rewound.state == "queued"
    assert rewound.version == 4
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)
    assert {:ok, 1} = Ferricstore.Flow.LMDB.prefix_count(path, root_prefix)
    assert {:ok, 1} = Ferricstore.Flow.LMDB.prefix_count(path, correlation_prefix)
  end

  test "lineage queries post-filter stale LMDB secondary index entries" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
    end)

    partition_key = "tenant-lineage-filter"
    id = "flow-lineage-filter"

    assert {:ok, _created} =
             Ferricstore.Store.Router.flow_create(ctx, %{
               id: id,
               type: "lineage-filter",
               state: "queued",
               run_at_ms: 10_000,
               parent_flow_id: "real-parent",
               root_flow_id: "real-root",
               correlation_id: "real-correlation",
               partition_key: partition_key,
               now_ms: 1
             })

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

    path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(0)
      |> Ferricstore.Flow.LMDB.path()

    stale_indexes = [
      Ferricstore.Flow.Keys.parent_index_key("wrong-parent", partition_key),
      Ferricstore.Flow.Keys.root_index_key("wrong-root", partition_key),
      Ferricstore.Flow.Keys.correlation_index_key("wrong-correlation", partition_key)
    ]

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(
               path,
               Enum.map(stale_indexes, fn index_key ->
                 key = Ferricstore.Flow.LMDB.query_index_key(index_key, id, 1)
                 value = Ferricstore.Flow.LMDB.encode_query_index_value(id, 1, 0)
                 {:put, key, value}
               end)
             )

    assert {:ok, []} =
             Ferricstore.Flow.by_parent(ctx, "wrong-parent", partition_key: partition_key)

    assert {:ok, []} = Ferricstore.Flow.by_root(ctx, "wrong-root", partition_key: partition_key)

    assert {:ok, []} =
             Ferricstore.Flow.by_correlation(ctx, "wrong-correlation",
               partition_key: partition_key
             )
  end

  test "mirror terminal writes keep version metadata without warming terminal record" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
    end)

    partition_key = "tenant-terminal-version"
    state_key = Ferricstore.Flow.Keys.state_key("flow-terminal-version", partition_key)

    assert {:ok, _} =
             Ferricstore.Store.Router.flow_create(ctx, %{
               id: "flow-terminal-version",
               type: "terminal-version",
               state: "queued",
               run_at_ms: 1,
               partition_key: partition_key,
               now_ms: 1
             })

    assert {:ok, [claimed]} =
             Ferricstore.Store.Router.flow_claim_due(ctx, %{
               type: "terminal-version",
               state: "queued",
               priority: nil,
               worker: "worker-terminal-version",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 2,
               partition_key: partition_key
             })

    assert {:ok, completed} =
             Ferricstore.Store.Router.flow_complete(ctx, %{
               id: claimed.id,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               result_ref: "result-terminal-version",
               now_ms: 3,
               partition_key: partition_key
             })

    assert completed.version == 3

    assert [{^state_key, nil, 0, {:flow_state_version, 3, _lfu}, fid, off, vsize}] =
             :ets.lookup(elem(ctx.keydir_refs, 0), state_key)

    assert is_integer(fid)
    assert is_integer(off)
    assert is_integer(vsize)
  end

  test "default mirror mode persists terminal Flow state for cold reads and info" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)

    Application.delete_env(:ferricstore, :flow_lmdb_mode)
    Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
    end)

    partition_key = "tenant-default-mirror"
    flow_type = "default-mirror"
    id = "flow-default-mirror"

    assert Ferricstore.Flow.LMDB.mode() == :mirror

    assert {:ok, _created} =
             Ferricstore.Store.Router.flow_create(ctx, %{
               id: id,
               type: flow_type,
               state: "queued",
               run_at_ms: 1,
               partition_key: partition_key,
               now_ms: 1
             })

    assert {:ok, [claimed]} =
             Ferricstore.Store.Router.flow_claim_due(ctx, %{
               type: flow_type,
               state: "queued",
               priority: nil,
               worker: "worker-default-mirror",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 2,
               partition_key: partition_key
             })

    assert {:ok, completed} =
             Ferricstore.Store.Router.flow_complete(ctx, %{
               id: claimed.id,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 3,
               partition_key: partition_key
             })

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

    lmdb_path =
      ctx.data_dir |> Ferricstore.DataDir.shard_data_path(0) |> Ferricstore.Flow.LMDB.path()

    assert {:ok, wrapped_blob} = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)
    assert {:ok, encoded_record} = Ferricstore.Flow.LMDB.decode_value(wrapped_blob, 3)
    assert Ferricstore.Flow.decode_record(encoded_record).id == completed.id

    assert {:ok, %{id: ^id, state: "completed"}} =
             Ferricstore.Flow.get(ctx, id, partition_key: partition_key)

    assert {:ok, %{queued: 0, running: 0, completed: 1}} =
             Ferricstore.Flow.info(ctx, flow_type,
               partition_key: partition_key,
               include_cold: true
             )
  end

  test "mirror batch flow get preserves order across hot, LMDB, and missing records" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
    end)

    partition_key = "tenant-batch-get"
    flow_type = "batch-get"

    assert {:ok, active} =
             Ferricstore.Store.Router.flow_create(ctx, %{
               id: "flow-active",
               type: flow_type,
               state: "queued",
               run_at_ms: 10_000,
               partition_key: partition_key,
               now_ms: 1
             })

    assert {:ok, _terminal_start} =
             Ferricstore.Store.Router.flow_create(ctx, %{
               id: "flow-terminal",
               type: flow_type,
               state: "queued",
               run_at_ms: 1,
               partition_key: partition_key,
               now_ms: 2
             })

    assert {:ok, [claimed]} =
             Ferricstore.Store.Router.flow_claim_due(ctx, %{
               type: flow_type,
               state: "queued",
               priority: nil,
               worker: "worker-batch-get",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 3,
               partition_key: partition_key
             })

    assert {:ok, terminal} =
             Ferricstore.Store.Router.flow_complete(ctx, %{
               id: claimed.id,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 4,
               partition_key: partition_key
             })

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

    assert [active_blob, terminal_blob, nil] =
             Ferricstore.Store.Router.flow_batch_get(
               ctx,
               [
                 active.id,
                 terminal.id,
                 "flow-missing"
               ],
               partition_key
             )

    assert Ferricstore.Flow.decode_record(active_blob).id == active.id
    assert Ferricstore.Flow.decode_record(terminal_blob).id == terminal.id
  end

  test "mirror batch flow get decodes LMDB expiry with command time" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
    end)

    id = "flow-command-time-lmdb"
    partition_key = "tenant-command-time-lmdb"
    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

    record = %{
      id: id,
      type: "command-time-lmdb",
      state: "completed",
      version: 1,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 1_000,
      updated_at_ms: 1_000,
      next_run_at_ms: nil,
      priority: 0,
      ttl_ms: nil,
      history_max_events: nil,
      partition_key: partition_key,
      payload_ref: nil,
      parent_flow_id: nil,
      root_flow_id: id,
      correlation_id: nil,
      result_ref: nil,
      error_ref: nil,
      lease_owner: nil,
      lease_token: nil,
      lease_deadline_ms: 0
    }

    encoded = Ferricstore.Flow.encode_record(record)
    wrapped = Ferricstore.Flow.LMDB.encode_value(encoded, 2_000)

    lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(0)
      |> Ferricstore.Flow.LMDB.path()

    assert :ok = Ferricstore.Flow.LMDB.write_batch(lmdb_path, [{:put, state_key, wrapped}])

    assert [^encoded] =
             Ferricstore.CommandTime.with_now_ms(1_500, fn ->
               Ferricstore.Store.Router.flow_batch_get(ctx, [id], partition_key)
             end)

    assert [nil] =
             Ferricstore.CommandTime.with_now_ms(2_500, fn ->
               Ferricstore.Store.Router.flow_batch_get(ctx, [id], partition_key)
             end)
  end

  test "flow get treats malformed LMDB mirror records as missing" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
    end)

    id = "flow-malformed-lmdb"
    partition_key = "tenant-malformed-lmdb"
    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

    wrapped = Ferricstore.Flow.LMDB.encode_value("FSF2bad", 0)

    lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(0)
      |> Ferricstore.Flow.LMDB.path()

    assert :ok = Ferricstore.Flow.LMDB.write_batch(lmdb_path, [{:put, state_key, wrapped}])

    assert {:ok, nil} = Ferricstore.Flow.get(ctx, id, partition_key: partition_key)
  end

  test "legacy write-through Flow LMDB mode aliases to mirror" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :write_through)

    on_exit(fn ->
      restore_env(:flow_lmdb_mode, old_mode)
    end)

    assert Ferricstore.Flow.LMDB.mode() == :mirror
  end

  test "mirror flow get emits telemetry for corrupt LMDB wrapper" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)
    test_pid = self()
    handler_id = {:flow_lmdb_read_error, self(), make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :flow, :lmdb, :read_error],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:flow_lmdb_read_error, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
    end)

    id = "flow-corrupt-lmdb-mirror"
    partition_key = "tenant-corrupt-lmdb-mirror"
    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

    lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(0)
      |> Ferricstore.Flow.LMDB.path()

    assert :ok = Ferricstore.Flow.LMDB.write_batch(lmdb_path, [{:put, state_key, "not-a-term"}])

    assert {:ok, nil} = Ferricstore.Flow.get(ctx, id, partition_key: partition_key)

    assert_receive {:flow_lmdb_read_error, [:ferricstore, :flow, :lmdb, :read_error], %{count: 1},
                    %{mode: :mirror, reason: :decode_error}}
  end

  test "lineage queries skip malformed LMDB mirror records" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
    end)

    id = "flow-malformed-lineage"
    partition_key = "tenant-malformed-lineage"
    correlation_id = "correlation-malformed-lineage"
    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
    index_key = Ferricstore.Flow.Keys.correlation_index_key(correlation_id, partition_key)
    query_key = Ferricstore.Flow.LMDB.query_index_key(index_key, id, 10)

    wrapped = Ferricstore.Flow.LMDB.encode_value("FSF2bad", 0)
    query_value = Ferricstore.Flow.LMDB.encode_query_index_value(id, 10, 0)

    lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(0)
      |> Ferricstore.Flow.LMDB.path()

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(lmdb_path, [
               {:put, state_key, wrapped},
               {:put, query_key, query_value}
             ])

    assert {:ok, []} =
             Ferricstore.Flow.by_correlation(ctx, correlation_id, partition_key: partition_key)
  end

  test "lineage queries hydrate directly from LMDB state keys" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
    end)

    id = "flow-direct-lineage"
    partition_key = "tenant-direct-lineage"
    parent_flow_id = "parent-direct-lineage"
    root_flow_id = "root-direct-lineage"
    correlation_id = "correlation-direct-lineage"
    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
    updated_at_ms = 12_345

    record = %{
      id: id,
      type: "direct-lineage",
      state: "queued",
      version: 1,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 1,
      updated_at_ms: updated_at_ms,
      next_run_at_ms: 20_000,
      priority: 0,
      ttl_ms: nil,
      history_max_events: nil,
      partition_key: partition_key,
      payload_ref: nil,
      parent_flow_id: parent_flow_id,
      root_flow_id: root_flow_id,
      correlation_id: correlation_id,
      result_ref: nil,
      error_ref: nil,
      lease_owner: nil,
      lease_token: nil,
      lease_deadline_ms: 0,
      rewound_to_event_id: nil
    }

    state_value =
      record
      |> Ferricstore.Flow.encode_record()
      |> Ferricstore.Flow.LMDB.encode_value(0)

    query_index_key = Ferricstore.Flow.Keys.correlation_index_key(correlation_id, partition_key)
    query_key = Ferricstore.Flow.LMDB.query_index_key(query_index_key, id, updated_at_ms)
    query_value = Ferricstore.Flow.LMDB.encode_query_index_value(id, updated_at_ms, 0, state_key)

    lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(0)
      |> Ferricstore.Flow.LMDB.path()

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(lmdb_path, [
               {:put, state_key, state_value},
               {:put, query_key, query_value}
             ])

    assert {:ok, [%{id: ^id, correlation_id: ^correlation_id, partition_key: ^partition_key}]} =
             Ferricstore.Flow.by_correlation(ctx, correlation_id,
               partition_key: partition_key,
               include_cold: true
             )
  end

  test "lineage queries overfetch past stale LMDB index rows before post-filtering" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
    end)

    partition_key = "tenant-stale-lineage-overfetch"
    correlation_id = "correlation-stale-lineage-overfetch"
    stale_id = "flow-stale-lineage-overfetch"
    live_id = "flow-live-lineage-overfetch"

    assert {:ok, _record} =
             Ferricstore.Flow.create(ctx, live_id,
               type: "stale-lineage-overfetch",
               partition_key: partition_key,
               correlation_id: correlation_id,
               run_at_ms: 20,
               now_ms: 20
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

    stale_index_key = Ferricstore.Flow.Keys.correlation_index_key(correlation_id, partition_key)
    stale_query_key = Ferricstore.Flow.LMDB.query_index_key(stale_index_key, stale_id, 10)
    stale_query_value = Ferricstore.Flow.LMDB.encode_query_index_value(stale_id, 10, 0)

    lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(0)
      |> Ferricstore.Flow.LMDB.path()

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(lmdb_path, [
               {:put, stale_query_key, stale_query_value}
             ])

    assert {:ok, [%{id: ^live_id}]} =
             Ferricstore.Flow.by_correlation(ctx, correlation_id,
               partition_key: partition_key,
               count: 1
             )
  end

  test "terminal list merges RAM and LMDB rows by score during mirror overlap" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
    end)

    partition_key = "tenant-terminal-overlap"
    flow_type = "terminal-overlap"
    old_id = "flow-terminal-overlap-old"
    new_id = "flow-terminal-overlap-new"

    assert {:ok, _} =
             Ferricstore.Flow.create(ctx, old_id,
               type: flow_type,
               partition_key: partition_key,
               run_at_ms: 1,
               now_ms: 1
             )

    assert {:ok, [old_claimed]} =
             Ferricstore.Flow.claim_due(ctx, flow_type,
               partition_key: partition_key,
               worker: "worker-terminal-overlap-old",
               limit: 1,
               now_ms: 2
             )

    assert {:ok, %{id: ^old_id, state: "completed"}} =
             Ferricstore.Flow.complete(ctx, old_id, old_claimed.lease_token,
               partition_key: partition_key,
               fencing_token: old_claimed.fencing_token,
               now_ms: 3
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

    assert {:ok, _} =
             Ferricstore.Flow.create(ctx, new_id,
               type: flow_type,
               partition_key: partition_key,
               run_at_ms: 10,
               now_ms: 10
             )

    assert {:ok, [new_claimed]} =
             Ferricstore.Flow.claim_due(ctx, flow_type,
               partition_key: partition_key,
               worker: "worker-terminal-overlap-new",
               limit: 1,
               now_ms: 11
             )

    assert {:ok, %{id: ^new_id, state: "completed"}} =
             Ferricstore.Flow.complete(ctx, new_id, new_claimed.lease_token,
               partition_key: partition_key,
               fencing_token: new_claimed.fencing_token,
               now_ms: 12
             )

    assert {:ok, [%{id: ^old_id}]} =
             Ferricstore.Flow.list(ctx, flow_type,
               state: "completed",
               partition_key: partition_key,
               count: 1,
               include_cold: true
             )
  end

  test "default hot Flow reads ignore degraded LMDB mirror" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
    end)

    :atomics.put(ctx.flow_lmdb_mirror_degraded, 1, 1)

    partition_key = "tenant-degraded-hot"
    parent = "parent-degraded-hot"
    correlation = "correlation-degraded-hot"
    id = "flow-degraded-hot"

    assert {:ok, _record} =
             Ferricstore.Flow.create(ctx, id,
               type: "degraded-hot",
               partition_key: partition_key,
               parent_flow_id: parent,
               correlation_id: correlation,
               now_ms: 1_000,
               run_at_ms: 1_000
             )

    assert {:ok, [%{id: ^id}]} =
             Ferricstore.Flow.by_parent(ctx, parent, partition_key: partition_key)

    assert {:ok, [%{id: ^id}]} =
             Ferricstore.Flow.by_correlation(ctx, correlation, partition_key: partition_key)

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, "degraded-hot",
               worker: "worker-degraded-hot",
               partition_key: partition_key,
               now_ms: 1_000
             )

    assert {:ok, completed} =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000
             )

    assert completed.state == "completed"

    assert {:ok, [%{id: ^id}]} =
             Ferricstore.Flow.list(ctx, "degraded-hot",
               state: "completed",
               partition_key: partition_key
             )

    assert {:ok, [%{id: ^id, state: "completed"}]} =
             Ferricstore.Flow.by_correlation(ctx, correlation, partition_key: partition_key)

    assert {:ok, %{completed: 1}} =
             Ferricstore.Flow.info(ctx, "degraded-hot", partition_key: partition_key)
  end

  test "terminal records stay in hot index until terminal hot ttl after LMDB flush" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_hot_ttl = Application.get_env(:ferricstore, :flow_terminal_hot_ttl_ms)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_terminal_hot_ttl_ms, 30)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_terminal_hot_ttl_ms, old_hot_ttl)
    end)

    partition_key = "tenant-hot-terminal-retention"
    flow_type = "hot-terminal-retention"
    id = "flow-hot-terminal-retention"
    parent = "parent-hot-terminal-retention"
    correlation = "correlation-hot-terminal-retention"

    assert {:ok, _record} =
             Ferricstore.Flow.create(ctx, id,
               type: flow_type,
               partition_key: partition_key,
               parent_flow_id: parent,
               correlation_id: correlation,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, flow_type,
               worker: "worker-hot-terminal-retention",
               partition_key: partition_key,
               now_ms: 1_000
             )

    assert {:ok, %{state: "completed"}} =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

    assert {:ok, [%{id: ^id}]} =
             Ferricstore.Flow.list(ctx, flow_type,
               state: "completed",
               partition_key: partition_key
             )

    assert {:ok, [%{id: ^id}]} =
             Ferricstore.Flow.by_parent(ctx, parent, partition_key: partition_key)

    Ferricstore.Test.ShardHelpers.eventually(
      fn ->
        match?(
          {:ok, []},
          Ferricstore.Flow.list(ctx, flow_type,
            state: "completed",
            partition_key: partition_key
          )
        )
      end,
      "terminal hot index was not pruned after retention ttl",
      100,
      10
    )

    assert {:ok, []} = Ferricstore.Flow.by_parent(ctx, parent, partition_key: partition_key)

    assert {:ok, [%{id: ^id}]} =
             Ferricstore.Flow.by_correlation(ctx, correlation,
               partition_key: partition_key,
               include_cold: true
             )
  end

  test "history include_cold returns LMDB-projected events trimmed from hot index" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
    end)

    id = "history-cold-projection"
    partition_key = "tenant-history-cold-projection"

    assert {:ok, _record} =
             Ferricstore.Flow.create(ctx, id,
               type: "history-cold-projection",
               partition_key: partition_key,
               history_max_events: 2,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, "history-cold-projection",
               worker: "worker-history-cold-projection",
               partition_key: partition_key,
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, _record} =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

    assert {:ok, hot_events} =
             Ferricstore.Flow.history(ctx, id, partition_key: partition_key, count: 10)

    assert Enum.map(hot_events, fn {_event_id, fields} -> fields["event"] end) == [
             "claimed",
             "completed"
           ]

    assert {:ok, cold_events} =
             Ferricstore.Flow.history(ctx, id,
               partition_key: partition_key,
               count: 10,
               include_cold: true
             )

    assert Enum.map(cold_events, fn {_event_id, fields} -> fields["event"] end) == [
             "created",
             "claimed",
             "completed"
           ]
  end

  test "history hot trim survives restart while include_cold reads projected events" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
    end)

    id = "history-cold-projection-restart"
    partition_key = "tenant-history-cold-projection-restart"

    assert {:ok, _record} =
             Ferricstore.Flow.create(ctx, id,
               type: "history-cold-projection-restart",
               partition_key: partition_key,
               history_max_events: 2,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, "history-cold-projection-restart",
               worker: "worker-history-cold-projection-restart",
               partition_key: partition_key,
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, _record} =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)
    restart_isolated_shard!(ctx, 0)

    assert {:ok, hot_events} =
             Ferricstore.Flow.history(ctx, id, partition_key: partition_key, count: 10)

    assert Enum.map(hot_events, fn {_event_id, fields} -> fields["event"] end) == [
             "claimed",
             "completed"
           ]

    assert {:ok, cold_events} =
             Ferricstore.Flow.history(ctx, id,
               partition_key: partition_key,
               count: 10,
               include_cold: true
             )

    assert Enum.map(cold_events, fn {_event_id, fields} -> fields["event"] end) == [
             "created",
             "claimed",
             "completed"
           ]
  end

  test "LMDB rebuild counts cold-read failures and marks mirror degraded" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_flow_lmdb_rebuild_cold_read_#{System.unique_integer([:positive])}"
      )

    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
    File.mkdir_p!(shard_path)
    keydir = :ets.new(:flow_lmdb_rebuild_cold_read_keydir, [:set])
    degraded = :atomics.new(1, signed: false)
    state_key = Ferricstore.Flow.Keys.state_key("cold-read-missing", "tenant-cold-read")
    test_pid = self()
    handler_id = {:flow_lmdb_rebuild_cold_read, self(), make_ref()}

    :telemetry.attach_many(
      handler_id,
      [
        [:ferricstore, :flow, :lmdb_rebuild, :cold_read_error],
        [:ferricstore, :flow, :lmdb_rebuild]
      ],
      fn event, measurements, metadata, _config ->
        send(test_pid, {:flow_lmdb_rebuild_cold_read, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      File.rm_rf!(data_dir)
    end)

    :ets.insert(keydir, {state_key, nil, 0, 0, 99, 0, 16})

    assert :ok =
             Ferricstore.Flow.LMDBRebuilder.reconcile_shard(
               shard_path,
               keydir,
               0,
               %{flow_lmdb_mirror_degraded: degraded},
               nil,
               nil,
               nil,
               nil
             )

    assert :atomics.get(degraded, 1) == 1

    assert_receive {:flow_lmdb_rebuild_cold_read,
                    [:ferricstore, :flow, :lmdb_rebuild, :cold_read_error], %{count: 1},
                    %{reason: _reason}}

    assert_receive {:flow_lmdb_rebuild_cold_read, [:ferricstore, :flow, :lmdb_rebuild],
                    %{cold_read_errors: 1}, %{shard_index: 0}}
  end

  test "mirror startup rebuilds flow working indexes and prunes terminal state to LMDB" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
    end)

    partition_key = "tenant-startup"
    flow_type = "startup"

    assert {:ok, queued} =
             Ferricstore.Store.Router.flow_create(ctx, %{
               id: "flow-queued",
               type: flow_type,
               state: "queued",
               run_at_ms: 10,
               partition_key: partition_key,
               now_ms: 1
             })

    assert {:ok, completed_start} =
             Ferricstore.Store.Router.flow_create(ctx, %{
               id: "flow-completed",
               type: flow_type,
               state: "queued",
               run_at_ms: 10,
               partition_key: partition_key,
               now_ms: 2
             })

    assert {:ok, [claimed]} =
             Ferricstore.Store.Router.flow_claim_due(ctx, %{
               type: flow_type,
               state: "queued",
               priority: nil,
               worker: "worker-startup",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 10,
               partition_key: partition_key
             })

    assert claimed.id == completed_start.id

    assert {:ok, completed} =
             Ferricstore.Store.Router.flow_complete(ctx, %{
               id: claimed.id,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 11,
               partition_key: partition_key
             })

    assert completed.state == "completed"

    completed_index_key =
      Ferricstore.Flow.Keys.state_index_key(flow_type, "completed", partition_key)

    terminal_prefix = Ferricstore.Flow.LMDB.terminal_index_prefix(completed_index_key)

    lmdb_path =
      ctx.data_dir |> Ferricstore.DataDir.shard_data_path(0) |> Ferricstore.Flow.LMDB.path()

    stale_terminal_key =
      Ferricstore.Flow.LMDB.terminal_index_key(completed_index_key, queued.id, 99)

    queued_state_key = Ferricstore.Flow.Keys.state_key(queued.id, partition_key)

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(lmdb_path, [
               {:put, stale_terminal_key,
                Ferricstore.Flow.LMDB.encode_terminal_index_value(queued.id, 99)},
               {:put, Ferricstore.Flow.LMDB.terminal_by_state_key_key(queued_state_key),
                stale_terminal_key}
             ])

    assert {:ok, 1} = Ferricstore.Flow.LMDB.prefix_count(lmdb_path, terminal_prefix)

    restart_isolated_shard!(ctx, 0)

    terminal_state_key = Ferricstore.Flow.Keys.state_key(completed.id, partition_key)
    assert [] = :ets.lookup(elem(ctx.keydir_refs, 0), terminal_state_key)

    assert {:ok, 1} = Ferricstore.Flow.LMDB.prefix_count(lmdb_path, terminal_prefix)

    assert {:ok, terminal_entries} =
             Ferricstore.Flow.LMDB.prefix_entries(lmdb_path, terminal_prefix, 10)

    assert ["flow-completed"] =
             Enum.map(terminal_entries, fn {_key, value} ->
               {:ok, {id, _updated_at_ms, _expire_at_ms, _state_key}} =
                 Ferricstore.Flow.LMDB.decode_terminal_index_value(value)

               id
             end)

    assert {:ok, %{id: "flow-completed", state: "completed"}} =
             Ferricstore.Flow.get(ctx, completed.id, partition_key: partition_key)

    assert {:ok, [claimed_after_restart]} =
             Ferricstore.Store.Router.flow_claim_due(ctx, %{
               type: flow_type,
               state: "queued",
               priority: nil,
               worker: "worker-startup-2",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 12,
               partition_key: partition_key
             })

    assert claimed_after_restart.id == queued.id
  end

  test "startup rebuilds native flow history index from durable history entries" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
    end)

    id = "history-restart"
    partition_key = "tenant-history"

    assert {:ok, _record} =
             Ferricstore.Flow.create(ctx, id,
               type: "history-restart",
               run_at_ms: 1,
               partition_key: partition_key,
               now_ms: 1
             )

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, "history-restart",
               worker: "worker-history",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1,
               partition_key: partition_key
             )

    assert {:ok, _record} =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: partition_key,
               now_ms: 2
             )

    assert {:ok, before_restart} = Ferricstore.Flow.history(ctx, id, partition_key: partition_key)

    assert Enum.map(before_restart, fn {_event_id, fields} -> fields["event"] end) == [
             "created",
             "claimed",
             "completed"
           ]

    restart_isolated_shard!(ctx, 0)

    assert {:ok, after_restart} = Ferricstore.Flow.history(ctx, id, partition_key: partition_key)

    assert Enum.map(after_restart, fn {_event_id, fields} -> fields["event"] end) == [
             "created",
             "claimed",
             "completed"
           ]
  end

  test "startup rebuild recovers terminal LMDB mirror when writer dies before flush" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
    old_max_batch_ops = Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)
    Application.put_env(:ferricstore, :flow_lmdb_max_batch_ops, 1_000_000)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
      restore_env(:flow_lmdb_max_batch_ops, old_max_batch_ops)
    end)

    id = "flow-rebuild-terminal"
    partition_key = "tenant-rebuild-terminal"
    flow_type = "rebuild-terminal"

    assert {:ok, _record} =
             Ferricstore.Flow.create(ctx, id,
               type: flow_type,
               run_at_ms: 1,
               partition_key: partition_key,
               root_flow_id: "root-rebuild-terminal",
               correlation_id: "corr-rebuild-terminal",
               now_ms: 1
             )

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, flow_type,
               worker: "worker-rebuild-terminal",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 2,
               partition_key: partition_key
             )

    assert {:ok, completed} =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: partition_key,
               ttl_ms: 60_000,
               now_ms: 3
             )

    writer_name = Ferricstore.Flow.LMDBWriter.name(ctx.name, 0)
    writer_pid = Process.whereis(writer_name)
    assert is_pid(writer_pid)
    ref = Process.monitor(writer_pid)
    :ok = GenServer.stop(writer_pid, :normal, 5_000)
    assert_receive {:DOWN, ^ref, :process, ^writer_pid, :normal}

    lmdb_path =
      ctx.data_dir |> Ferricstore.DataDir.shard_data_path(0) |> Ferricstore.Flow.LMDB.path()

    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

    completed_index_key =
      Ferricstore.Flow.Keys.state_index_key(flow_type, "completed", partition_key)

    failed_index_key = Ferricstore.Flow.Keys.state_index_key(flow_type, "failed", partition_key)

    assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)
    assert :ok = Ferricstore.Flow.LMDB.put_terminal_count(lmdb_path, completed_index_key, 7)
    assert :ok = Ferricstore.Flow.LMDB.put_terminal_count(lmdb_path, failed_index_key, 5)

    restart_isolated_shard!(ctx, 0)

    terminal_prefix = Ferricstore.Flow.LMDB.terminal_index_prefix(completed_index_key)

    root_prefix =
      Ferricstore.Flow.LMDB.query_index_prefix(
        Ferricstore.Flow.Keys.root_index_key("root-rebuild-terminal", partition_key)
      )

    correlation_prefix =
      Ferricstore.Flow.LMDB.query_index_prefix(
        Ferricstore.Flow.Keys.correlation_index_key("corr-rebuild-terminal", partition_key)
      )

    assert [] = :ets.lookup(elem(ctx.keydir_refs, 0), state_key)

    assert {:ok, %{id: ^id, state: "completed"}} =
             Ferricstore.Flow.get(ctx, id, partition_key: partition_key)

    assert {:ok, 1} = Ferricstore.Flow.LMDB.prefix_count(lmdb_path, terminal_prefix)
    assert {:ok, 1} = Ferricstore.Flow.LMDB.terminal_count(lmdb_path, completed_index_key)
    assert {:ok, 0} = Ferricstore.Flow.LMDB.terminal_count(lmdb_path, failed_index_key)
    assert {:ok, 1} = Ferricstore.Flow.LMDB.prefix_count(lmdb_path, root_prefix)
    assert {:ok, 1} = Ferricstore.Flow.LMDB.prefix_count(lmdb_path, correlation_prefix)

    assert {:ok, info} =
             Ferricstore.Flow.info(ctx, flow_type,
               partition_key: partition_key,
               include_cold: true
             )

    assert info.completed == 1
    assert completed.version == 3
  end

  test "mirror flow TTL removes expired LMDB state and terminal index on read" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
    end)

    partition_key = "tenant-ttl"
    flow_type = "ttl"

    assert {:ok, _} =
             Ferricstore.Store.Router.flow_create(ctx, %{
               id: "flow-ttl",
               type: flow_type,
               state: "queued",
               run_at_ms: 1,
               partition_key: partition_key,
               now_ms: 1
             })

    assert {:ok, [claimed]} =
             Ferricstore.Store.Router.flow_claim_due(ctx, %{
               type: flow_type,
               state: "queued",
               priority: nil,
               worker: "worker-ttl",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 2,
               partition_key: partition_key
             })

    assert {:ok, completed} =
             Ferricstore.Store.Router.flow_complete(ctx, %{
               id: claimed.id,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               ttl_ms: 20,
               now_ms: 3,
               partition_key: partition_key
             })

    assert completed.state == "completed"
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

    lmdb_path =
      ctx.data_dir |> Ferricstore.DataDir.shard_data_path(0) |> Ferricstore.Flow.LMDB.path()

    state_key = Ferricstore.Flow.Keys.state_key(completed.id, partition_key)
    reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)

    completed_index_key =
      Ferricstore.Flow.Keys.state_index_key(flow_type, "completed", partition_key)

    terminal_prefix = Ferricstore.Flow.LMDB.terminal_index_prefix(completed_index_key)

    assert {:ok, _blob} = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)
    assert {:ok, 1} = Ferricstore.Flow.LMDB.prefix_count(lmdb_path, terminal_prefix)

    Process.sleep(40)

    assert {:ok, nil} = Ferricstore.Flow.get(ctx, completed.id, partition_key: partition_key)
    assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)
    assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, reverse_key)
    assert {:ok, 0} = Ferricstore.Flow.LMDB.prefix_count(lmdb_path, terminal_prefix)
  end

  test "mirror flow TTL sweep removes expired terminal index without flow get" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
    end)

    partition_key = "tenant-ttl-sweep"
    flow_type = "ttl-sweep"

    assert {:ok, _} =
             Ferricstore.Store.Router.flow_create(ctx, %{
               id: "flow-ttl-sweep",
               type: flow_type,
               state: "queued",
               run_at_ms: 1,
               partition_key: partition_key,
               now_ms: 1
             })

    assert {:ok, [claimed]} =
             Ferricstore.Store.Router.flow_claim_due(ctx, %{
               type: flow_type,
               state: "queued",
               priority: nil,
               worker: "worker-ttl-sweep",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 2,
               partition_key: partition_key
             })

    assert {:ok, completed} =
             Ferricstore.Store.Router.flow_complete(ctx, %{
               id: claimed.id,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               ttl_ms: 20,
               now_ms: 3,
               partition_key: partition_key
             })

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

    lmdb_path =
      ctx.data_dir |> Ferricstore.DataDir.shard_data_path(0) |> Ferricstore.Flow.LMDB.path()

    completed_index_key =
      Ferricstore.Flow.Keys.state_index_key(flow_type, "completed", partition_key)

    terminal_prefix = Ferricstore.Flow.LMDB.terminal_index_prefix(completed_index_key)
    state_key = Ferricstore.Flow.Keys.state_key(completed.id, partition_key)
    reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)

    assert {:ok, 1} = Ferricstore.Flow.LMDB.prefix_count(lmdb_path, terminal_prefix)
    assert {:ok, 1} = Ferricstore.Flow.LMDB.terminal_count(lmdb_path, completed_index_key)

    Process.sleep(40)

    assert {:ok, 1} =
             Ferricstore.Flow.LMDB.sweep_expired_terminal(
               lmdb_path,
               System.os_time(:millisecond),
               100
             )

    assert {:ok, 0} = Ferricstore.Flow.LMDB.prefix_count(lmdb_path, terminal_prefix)
    assert {:ok, 0} = Ferricstore.Flow.LMDB.terminal_count(lmdb_path, completed_index_key)
    assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)
    assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, reverse_key)
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  defp restart_isolated_shard!(ctx, shard_index) do
    shard_name = elem(ctx.shard_names, shard_index)
    :ok = GenServer.call(shard_name, :flush, 5_000)
    :ok = GenServer.stop(Process.whereis(shard_name), :normal, 5_000)

    unless Process.whereis(Ferricstore.Flow.LMDBWriter.name(ctx.name, shard_index)) do
      {:ok, _pid} =
        Ferricstore.Flow.LMDBWriter.start_link(
          shard_index: shard_index,
          data_dir: ctx.data_dir,
          instance_ctx: ctx
        )
    end

    {:ok, _pid} =
      Ferricstore.Store.Shard.start_link(
        index: shard_index,
        data_dir: ctx.data_dir,
        instance_ctx: ctx
      )

    Ferricstore.Test.ShardHelpers.eventually(
      fn ->
        try do
          match?({:ok, _}, GenServer.call(shard_name, :shard_stats, 500))
        catch
          :exit, _ -> false
        end
      end,
      "shard #{shard_index} not ready after restart",
      50,
      20
    )
  end
end
