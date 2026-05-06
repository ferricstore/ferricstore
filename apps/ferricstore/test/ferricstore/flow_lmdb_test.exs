defmodule Ferricstore.Flow.LMDBTest do
  use ExUnit.Case, async: false

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

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(instance_name, shard_index + 1)
    assert {:ok, "v2"} = Ferricstore.Flow.LMDB.get(path, key)

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [{:put, key, "v2"}])

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(instance_name, shard_index + 1)
    assert {:ok, "v2"} = Ferricstore.Flow.LMDB.get(path, key)
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

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(instance_name, shard_index + 1)

    assert {:ok, "v1"} = Ferricstore.Flow.LMDB.get(path, key)
    assert :atomics.get(durable, shard_index + 1) == 123
    assert Ferricstore.Flow.LMDBReplaySafeIndex.read(shard_data_path) == 123
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

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(instance_name, shard_index + 1)

    assert {:ok, ^value} = Ferricstore.Flow.LMDB.get(path, terminal_key)
    assert {:ok, ^terminal_key} = Ferricstore.Flow.LMDB.get(path, reverse_key)
    assert {:ok, 1} = Ferricstore.Flow.LMDB.terminal_count(path, state_index_key)
    assert {:ok, _expire_value} = Ferricstore.Flow.LMDB.get(path, expire_key)

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
               {:terminal_delete, terminal_key, state_key, count_key}
             ])

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(instance_name, shard_index + 1)

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

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(instance_name, shard_index + 1)

    assert {:ok, ^value} = Ferricstore.Flow.LMDB.get(path, terminal_key)
    assert {:ok, _expire_value} = Ferricstore.Flow.LMDB.get(path, expire_key)
    assert {:ok, 1} = Ferricstore.Flow.LMDB.terminal_count(path, metadata_index_key)

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
               {:terminal_delete, terminal_key, nil, count_key}
             ])

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(instance_name, shard_index + 1)

    assert :not_found = Ferricstore.Flow.LMDB.get(path, terminal_key)
    assert :not_found = Ferricstore.Flow.LMDB.get(path, expire_key)
    assert {:ok, 0} = Ferricstore.Flow.LMDB.terminal_count(path, metadata_index_key)
  end

  test "mirror flow reads reject stale LMDB record and fall back to Bitcask truth" do
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
