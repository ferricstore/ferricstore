defmodule Ferricstore.Flow.LMDBTest.Sections.Part08 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Flow.LMDBTest.FlushProbeWriter

  test "lineage include_cold reverse reads newest LMDB prefix rows" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_scan_limit = Application.get_env(:ferricstore, :flow_lmdb_query_scan_limit)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_lmdb_query_scan_limit, 1)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_lmdb_query_scan_limit, old_scan_limit)
    end)

    partition_key = "tenant-reverse-cold-lineage"
    correlation_id = "correlation-reverse-cold-lineage"
    query_index_key = Ferricstore.Flow.Keys.correlation_index_key(correlation_id, partition_key)

    lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(0)
      |> Ferricstore.Flow.LMDB.path()

    ops =
      Enum.flat_map([{"flow-cold-old", 10}, {"flow-cold-new", 20}], fn {id, updated_at_ms} ->
        state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

        record = %{
          id: id,
          type: "reverse-cold-lineage",
          state: "queued",
          version: 1,
          attempts: 0,
          fencing_token: 0,
          created_at_ms: updated_at_ms,
          updated_at_ms: updated_at_ms,
          next_run_at_ms: updated_at_ms,
          priority: 0,
          ttl_ms: nil,
          history_hot_max_events: nil,
          history_max_events: nil,
          partition_key: partition_key,
          payload_ref: nil,
          parent_flow_id: nil,
          root_flow_id: nil,
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

        query_key = Ferricstore.Flow.LMDB.query_index_key(query_index_key, id, updated_at_ms)

        query_value =
          Ferricstore.Flow.LMDB.encode_query_index_value(id, updated_at_ms, 0, state_key)

        [{:put, state_key, state_value}, {:put, query_key, query_value}]
      end)

    assert :ok = Ferricstore.Flow.LMDB.write_batch(lmdb_path, ops)

    assert {:ok, [%{id: "flow-cold-new"}]} =
             Ferricstore.Flow.by_correlation(ctx, correlation_id,
               partition_key: partition_key,
               include_cold: true,
               rev: true,
               count: 1
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

    assert :ok =
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

    assert :ok =
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

    assert :ok =
             Ferricstore.Flow.complete(ctx, old_id, old_claimed.lease_token,
               partition_key: partition_key,
               fencing_token: old_claimed.fencing_token,
               now_ms: 3
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

    assert :ok =
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

    assert :ok =
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

    assert :ok =
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

    assert :ok =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               ttl_ms: 40,
               now_ms: 2_000
             )

    assert {:ok, completed} = Ferricstore.Flow.get(ctx, id, partition_key: partition_key)

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

  test "terminal records remain readable from hot indexes after LMDB flush" do
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

    assert :ok =
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

    assert :ok =
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

    Process.sleep(40)

    assert {:ok, [%{id: ^id}]} =
             Ferricstore.Flow.list(ctx, flow_type,
               state: "completed",
               partition_key: partition_key
             )

    assert {:ok, [%{id: ^id}]} =
             Ferricstore.Flow.by_parent(ctx, parent, partition_key: partition_key)

    assert {:ok, %{id: ^id, state: "completed"}} =
             Ferricstore.Flow.get(ctx, id, partition_key: partition_key)

    assert {:ok, [%{id: ^id}]} =
             Ferricstore.Flow.by_correlation(ctx, correlation,
               partition_key: partition_key,
               include_cold: true
             )
  end

  test "terminal records remain in hot indexes by default after LMDB flush" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_hot_ttl = Application.get_env(:ferricstore, :flow_terminal_hot_ttl_ms)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.delete_env(:ferricstore, :flow_terminal_hot_ttl_ms)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_terminal_hot_ttl_ms, old_hot_ttl)
    end)

    partition_key = "tenant-default-terminal-retention"
    flow_type = "default-terminal-retention"
    id = "flow-default-terminal-retention"
    parent = "parent-default-terminal-retention"

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: flow_type,
               partition_key: partition_key,
               parent_flow_id: parent,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, flow_type,
               worker: "worker-default-terminal-retention",
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

    assert {:ok, [%{id: ^id}]} =
             Ferricstore.Flow.list(ctx, flow_type,
               state: "completed",
               partition_key: partition_key
             )

    assert {:ok, [%{id: ^id}]} =
             Ferricstore.Flow.by_parent(ctx, parent, partition_key: partition_key)

    assert {:ok, %{id: ^id, state: "completed"}} =
             Ferricstore.Flow.get(ctx, id, partition_key: partition_key)
  end

  test "partitioned cold terminal queries read the same LMDB shard that received projection" do
    old_hot_ttl = Application.get_env(:ferricstore, :flow_terminal_hot_ttl_ms)
    Application.delete_env(:ferricstore, :flow_terminal_hot_ttl_ms)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 4, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_terminal_hot_ttl_ms, old_hot_ttl)
    end)

    partition_key = "tenant-cold-terminal-routing"
    flow_type = "cold-terminal-routing"
    id = "flow-cold-terminal-routing"

    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
    history_key = Ferricstore.Flow.Keys.history_key(id, partition_key)
    value_key = Ferricstore.Flow.Keys.value_key(id, :payload, 1, partition_key)
    index_key = Ferricstore.Flow.Keys.state_index_key(flow_type, "completed", partition_key)
    state_shard = Ferricstore.Store.Router.shard_for(ctx, state_key)

    # Partitioned Flow keys intentionally share the same hash tag. The state-machine projection
    # goes to the apply shard, so cold query routing must stay aligned with these keys.
    assert [
             ^state_shard,
             ^state_shard,
             ^state_shard,
             ^state_shard
           ] =
             Enum.map([state_key, history_key, value_key, index_key], fn key ->
               Ferricstore.Store.Router.shard_for(ctx, key)
             end)

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: flow_type,
               partition_key: partition_key,
               payload: "payload",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, flow_type,
               worker: "worker-cold-terminal-routing",
               partition_key: partition_key,
               now_ms: 1_000
             )

    assert :ok =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

    lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(state_shard)
      |> Ferricstore.Flow.LMDB.path()

    prefix = Ferricstore.Flow.LMDB.terminal_index_prefix(index_key)
    assert {:ok, 1} = Ferricstore.Flow.LMDB.prefix_count(lmdb_path, prefix)

    assert {:ok, [%{id: ^id, state: "completed"}]} =
             Ferricstore.Flow.list(ctx, flow_type,
               state: "completed",
               partition_key: partition_key,
               include_cold: true,
               consistent_projection: true
             )
  end

  test "retention cleanup removes terminal source rows after hot prune" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_hot_ttl = Application.get_env(:ferricstore, :flow_terminal_hot_ttl_ms)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_terminal_hot_ttl_ms, 10)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_terminal_hot_ttl_ms, old_hot_ttl)
    end)

    partition_key = "tenant-hot-pruned-retention"
    flow_type = "hot-pruned-retention"
    id = "flow-hot-pruned-retention"

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: flow_type,
               partition_key: partition_key,
               run_at_ms: 1_000,
               retention_ttl_ms: 40,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, flow_type,
               worker: "worker-hot-pruned-retention",
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

    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

    Ferricstore.Test.ShardHelpers.eventually(
      fn -> [] == :ets.lookup(elem(ctx.keydir_refs, 0), state_key) end,
      "terminal state key was not hot-pruned",
      200,
      10
    )

    Process.sleep(60)

    assert {:ok, cleaned} = Ferricstore.Flow.retention_cleanup(ctx, limit: 10)
    assert cleaned.flows == 1
    assert cleaned.history >= 1

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

    restart_isolated_shard!(ctx, 0)

    assert {:ok, nil} = Ferricstore.Flow.get(ctx, id, partition_key: partition_key)
    assert [] = :ets.lookup(elem(ctx.keydir_refs, 0), state_key)
  end

  test "retention cleanup removes terminal metadata query rows" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_hot_ttl = Application.get_env(:ferricstore, :flow_terminal_hot_ttl_ms)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_terminal_hot_ttl_ms, 0)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_terminal_hot_ttl_ms, old_hot_ttl)
    end)

    partition_key = "tenant-retention-metadata"
    flow_type = "retention-metadata"
    id = "flow-retention-metadata"
    parent = "parent-retention-metadata"
    root = "root-retention-metadata"
    correlation = "correlation-retention-metadata"
    now = System.system_time(:millisecond)

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: flow_type,
               partition_key: partition_key,
               parent_flow_id: parent,
               root_flow_id: root,
               correlation_id: correlation,
               run_at_ms: now,
               retention_ttl_ms: 1,
               now_ms: now
             )

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, flow_type,
               worker: "worker-retention-metadata",
               partition_key: partition_key,
               now_ms: now
             )

    assert :ok =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               now_ms: now + 1
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)
    assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, 0, 120_000)

    lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(0)
      |> Ferricstore.Flow.LMDB.path()

    metadata_prefixes =
      [
        Ferricstore.Flow.Keys.parent_index_key(parent, partition_key),
        Ferricstore.Flow.Keys.root_index_key(root, partition_key),
        Ferricstore.Flow.Keys.correlation_index_key(correlation, partition_key)
      ]
      |> Enum.map(&Ferricstore.Flow.LMDB.query_index_prefix/1)

    assert Enum.all?(metadata_prefixes, fn prefix ->
             Ferricstore.Flow.LMDB.prefix_count(lmdb_path, prefix) == {:ok, 1}
           end)

    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

    assert {:ok, [^state_key]} =
             Ferricstore.Flow.LMDB.expired_terminal_state_keys(lmdb_path, now + 100, 10)

    totals =
      Enum.reduce_while(1..10, %{flows: 0, history: 0, values: 0}, fn _, acc ->
        assert {:ok, cleaned} =
                 Ferricstore.Flow.retention_cleanup(ctx, limit: 10, now_ms: now + 100)

        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

        next = %{
          flows: acc.flows + cleaned.flows,
          history: acc.history + cleaned.history,
          values: acc.values + cleaned.values
        }

        if next.flows == 1 do
          {:halt, next}
        else
          {:cont, next}
        end
      end)

    assert totals.flows == 1

    assert Enum.all?(metadata_prefixes, fn prefix ->
             Ferricstore.Flow.LMDB.prefix_count(lmdb_path, prefix) == {:ok, 0}
           end)
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

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: "history-cold-projection",
               partition_key: partition_key,
               history_hot_max_events: 2,
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

    assert :ok =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

    assert {:ok, default_events} =
             Ferricstore.Flow.history(ctx, id, partition_key: partition_key, count: 10)

    assert Enum.map(default_events, fn {_event_id, fields} -> fields["event"] end) == [
             "created",
             "claimed",
             "completed"
           ]

    assert {:ok, hot_events} =
             Ferricstore.Flow.history(ctx, id,
               partition_key: partition_key,
               count: 10,
               include_cold: false
             )

    assert hot_events == []
  end

  test "async history keeps history cold while default history reads LMDB locations" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_async_history = Application.get_env(:ferricstore, :flow_async_history)

    old_projector_flush =
      Application.get_env(:ferricstore, :flow_history_projector_flush_interval_ms)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_async_history, true)
    Application.put_env(:ferricstore, :flow_history_projector_flush_interval_ms, 60_000)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_async_history, old_async_history)
      restore_env(:flow_history_projector_flush_interval_ms, old_projector_flush)
    end)

    id = "history-async-cold-locations"
    partition_key = "tenant-history-async-cold-locations"
    flow_type = "history-async-cold-locations"

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: flow_type,
               partition_key: partition_key,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, %{history_hot_max_events: 0}} =
             Ferricstore.Flow.get(ctx, id, partition_key: partition_key)

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, flow_type,
               worker: "worker-history-async-cold-locations",
               partition_key: partition_key,
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_001
             )

    assert :ok =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               now_ms: 1_002
             )

    assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, 0, 120_000)

    history_key = Ferricstore.Flow.Keys.history_key(id, partition_key)
    {_flow_index, flow_lookup} = Ferricstore.Flow.OrderedIndex.table_names(ctx.name, 0)

    assert Ferricstore.Flow.OrderedIndex.count_all(flow_lookup, history_key) == 0

    lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(0)
      |> Ferricstore.Flow.LMDB.path()

    assert {:ok, 3} =
             Ferricstore.Flow.LMDB.prefix_count(
               lmdb_path,
               Ferricstore.Flow.LMDB.history_index_prefix(history_key)
             )

    assert {:ok, history} =
             Ferricstore.Flow.history(ctx, id, partition_key: partition_key, count: 10)

    assert Enum.map(history, fn {_event_id, fields} -> fields["event"] end) == [
             "created",
             "claimed",
             "completed"
           ]

    assert {:ok, hot_history} =
             Ferricstore.Flow.history(
               ctx,
               id,
               partition_key: partition_key,
               count: 10,
               include_cold: false
             )

    assert hot_history == []
  end
    end
  end
end
