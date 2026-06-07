defmodule Ferricstore.Flow.LMDBTest.Sections.Part07 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Flow.LMDBTest.FlushProbeWriter

  test "mirror flow reads reject stale LMDB record and fall back to Bitcask truth" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
    old_max_batch_ops = Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops)
    old_hot_ttl = Application.get_env(:ferricstore, :flow_terminal_hot_ttl_ms)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)
    Application.put_env(:ferricstore, :flow_lmdb_max_batch_ops, 1_000_000)
    Application.put_env(:ferricstore, :flow_terminal_hot_ttl_ms, 0)

    ctx =
      Ferricstore.Test.IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 1,
        blob_side_channel_threshold_bytes: 128
      )

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

    assert :ok =
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

    assert {:ok, flow} = Ferricstore.Flow.get(ctx, "flow-a", partition_key: partition_key)

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

    assert :ok =
             Ferricstore.Store.Router.flow_complete(ctx, %{
               id: claimed.id,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               result_ref: "result-a",
               now_ms: 3,
               partition_key: partition_key
             })

    assert {:ok, completed} = Ferricstore.Flow.get(ctx, claimed.id, partition_key: partition_key)

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

    assert :ok =
             Ferricstore.Store.Router.flow_rewind(ctx, %{
               id: completed.id,
               to_event: created_event_id,
               expect_state: "completed",
               now_ms: 4,
               partition_key: partition_key
             })

    assert {:ok, rewound} = Ferricstore.Flow.get(ctx, completed.id, partition_key: partition_key)

    assert rewound.state == "queued"
    assert rewound.version == 4
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)
    assert {:ok, 0} = Ferricstore.Flow.LMDB.prefix_count(path, root_prefix)
    assert {:ok, 0} = Ferricstore.Flow.LMDB.prefix_count(path, correlation_prefix)

    assert {:ok, [%{id: "flow-a", state: "queued"}]} =
             Ferricstore.Flow.by_correlation(ctx, correlation_id, partition_key: partition_key)
  end

  test "non-idempotent create rejects existing terminal flow while hot truth remains" do
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

    partition_key = "tenant-cold-duplicate"
    flow_type = "cold-duplicate"
    id = "flow-cold-duplicate"

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: flow_type,
               partition_key: partition_key,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, flow_type,
               worker: "worker-cold-duplicate",
               partition_key: partition_key,
               now_ms: 1_000
             )

    assert :ok =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

    assert {:ok, %{state: "completed"}} =
             Ferricstore.Flow.get(ctx, id, partition_key: partition_key)

    assert {:error, "ERR flow already exists"} =
             Ferricstore.Flow.create(ctx, id,
               type: flow_type,
               partition_key: partition_key,
               run_at_ms: 3_000,
               now_ms: 3_000
             )

    assert {:error, "ERR flow already exists"} =
             Ferricstore.Flow.create_many(
               ctx,
               partition_key,
               [%{id: id}, %{id: "flow-cold-duplicate-new"}],
               type: flow_type,
               run_at_ms: 3_000,
               now_ms: 3_000
             )

    assert {:ok, nil} =
             Ferricstore.Flow.get(ctx, "flow-cold-duplicate-new", partition_key: partition_key)
  end

  test "history expiry marker from terminal flow is cleared after rewind" do
    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
    end)

    partition_key = "tenant-history-expiry-rewind"
    flow_type = "history-expiry-rewind"
    id = "flow-history-expiry-rewind"
    create_now = System.system_time(:millisecond) + 60_000
    claim_now = create_now + 1
    complete_now = create_now + 2
    rewind_now = complete_now + 10
    sweep_now = complete_now + 1_000

    assert :ok =
             Ferricstore.Store.Router.flow_create(ctx, %{
               id: id,
               type: flow_type,
               state: "queued",
               run_at_ms: create_now,
               partition_key: partition_key,
               retention_ttl_ms: 20,
               now_ms: create_now
             })

    assert {:ok, [claimed]} =
             Ferricstore.Store.Router.flow_claim_due(ctx, %{
               type: flow_type,
               state: "queued",
               priority: nil,
               worker: "worker-history-expiry-rewind",
               lease_ms: 30_000,
               limit: 1,
               now_ms: claim_now,
               partition_key: partition_key
             })

    assert :ok =
             Ferricstore.Store.Router.flow_complete(ctx, %{
               id: claimed.id,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: complete_now,
               partition_key: partition_key
             })

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

    lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(0)
      |> Ferricstore.Flow.LMDB.path()

    history_key = Ferricstore.Flow.Keys.history_key(id, partition_key)
    history_prefix = Ferricstore.Flow.LMDB.history_index_prefix(history_key)

    assert {:ok, before_rewind_count} =
             Ferricstore.Flow.LMDB.prefix_count(lmdb_path, history_prefix)

    assert before_rewind_count >= 3

    assert {:ok, [{created_event_id, _fields} | _]} =
             Ferricstore.Flow.history(ctx, id, partition_key: partition_key, count: 10)

    assert :ok =
             Ferricstore.Store.Router.flow_rewind(ctx, %{
               id: id,
               to_event: created_event_id,
               expect_state: "completed",
               run_at_ms: sweep_now,
               now_ms: rewind_now,
               partition_key: partition_key
             })

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

    assert {:ok, after_rewind_count} =
             Ferricstore.Flow.LMDB.prefix_count(lmdb_path, history_prefix)

    assert after_rewind_count >= before_rewind_count

    assert {:ok, 0} = Ferricstore.Flow.LMDB.sweep_expired_history(lmdb_path, sweep_now, 100)

    assert {:ok, ^after_rewind_count} =
             Ferricstore.Flow.LMDB.prefix_count(lmdb_path, history_prefix)

    assert {:ok, events} =
             Ferricstore.Flow.history(ctx, id,
               partition_key: partition_key,
               count: 10,
               now_ms: sweep_now
             )

    assert Enum.any?(events, fn {_event_id, fields} -> fields["event"] == "rewound" end)
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

    assert :ok =
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

  test "terminal writes keep version metadata on the hot truth row" do
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

    assert :ok =
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

    assert :ok =
             Ferricstore.Store.Router.flow_complete(ctx, %{
               id: claimed.id,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               result_ref: "result-terminal-version",
               now_ms: 3,
               partition_key: partition_key
             })

    assert {:ok, completed} =
             Ferricstore.Flow.get(ctx, claimed.id, partition_key: partition_key)

    assert completed.version == 3

    assert [{^state_key, _value, expire_at_ms, _lfu, fid, off, vsize}] =
             :ets.lookup(elem(ctx.keydir_refs, 0), state_key)

    assert expire_at_ms == 0 or expire_at_ms > System.system_time(:millisecond)
    refute is_nil(fid)
    assert is_integer(off)
    assert is_integer(vsize)
  end

  test "lagged projection persists terminal Flow state for cold reads and info" do
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

    partition_key = "tenant-default-mirror"
    flow_type = "default-mirror"
    id = "flow-default-mirror"

    assert Ferricstore.Flow.LMDB.mode() == :lagged

    assert :ok =
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

    assert :ok =
             Ferricstore.Store.Router.flow_complete(ctx, %{
               id: claimed.id,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 3,
               partition_key: partition_key
             })

    assert {:ok, completed} =
             Ferricstore.Flow.get(ctx, claimed.id, partition_key: partition_key)

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

    assert :ok =
             Ferricstore.Store.Router.flow_create(ctx, %{
               id: "flow-active",
               type: flow_type,
               state: "queued",
               run_at_ms: 10_000,
               partition_key: partition_key,
               now_ms: 1
             })

    assert {:ok, active} =
             Ferricstore.Flow.get(ctx, "flow-active", partition_key: partition_key)

    assert :ok =
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

    assert :ok =
             Ferricstore.Store.Router.flow_complete(ctx, %{
               id: claimed.id,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 4,
               partition_key: partition_key
             })

    assert {:ok, terminal} =
             Ferricstore.Flow.get(ctx, claimed.id, partition_key: partition_key)

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
      history_hot_max_events: nil,
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

  test "legacy Flow LMDB mode values are ignored" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :write_through)

    on_exit(fn ->
      restore_env(:flow_lmdb_mode, old_mode)
    end)

    assert Ferricstore.Flow.LMDB.mode() == :lagged

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    assert Ferricstore.Flow.LMDB.mode() == :lagged
  end

  test "lagged projection flow get emits telemetry for corrupt LMDB wrapper" do
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
                    %{mode: :lagged, reason: :decode_error}}
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
      history_hot_max_events: nil,
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
    end
  end
end
