defmodule Ferricstore.FlowTest.Sections.Part03 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Test.ShardHelpers

  test "flow_create stores debug lineage metadata and indexes it" do
    id = uid("flow-lineage-child")
    parent = uid("flow-lineage-parent")
    root = uid("flow-lineage-root")
    correlation = uid("order")
    partition = uid("tenant")

    assert {:ok, flow} =
             flow_create_and_get(id,
               type: "lineage",
               partition_key: partition,
               parent_flow_id: parent,
               root_flow_id: root,
               correlation_id: correlation,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert flow.parent_flow_id == parent
    assert flow.root_flow_id == root
    assert flow.correlation_id == correlation

    assert {:ok, [%{id: ^id}]} =
             FerricStore.flow_by_parent(parent, partition_key: partition, count: 10)

    assert {:ok, []} =
             FerricStore.zrange(Ferricstore.Flow.Keys.parent_index_key(parent, partition), 0, 10)

    assert {:ok, [%{id: ^id}]} =
             FerricStore.flow_by_root(root, partition_key: partition, count: 10)

    assert {:ok, []} =
             FerricStore.zrange(Ferricstore.Flow.Keys.root_index_key(root, partition), 0, 10)

    assert {:ok, [%{id: ^id}]} =
             FerricStore.flow_by_correlation(correlation, partition_key: partition, count: 10)

    assert {:ok, []} =
             FerricStore.zrange(
               Ferricstore.Flow.Keys.correlation_index_key(correlation, partition),
               0,
               10
             )

    assert {:ok, [{_event_id, fields}]} =
             FerricStore.flow_history(id, partition_key: partition, count: 10)

    assert fields["parent_flow_id"] == parent
    assert fields["root_flow_id"] == root
    assert fields["correlation_id"] == correlation
  end

  test "flow_create stores default root without indexing one unique root per flow" do
    id = uid("flow-lineage-root-default")
    partition = uid("tenant")

    assert {:ok, flow} =
             flow_create_and_get(id,
               type: "lineage",
               partition_key: partition,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert flow.root_flow_id == id

    assert {:ok, []} =
             FerricStore.zrange(Ferricstore.Flow.Keys.root_index_key(id, partition), 0, 10)
  end

  test "flow_by_parent root and correlation query lineage indexes" do
    partition = uid("tenant")
    root = uid("flow-root")
    child_a = uid("flow-child-a")
    child_b = uid("flow-child-b")
    grandchild = uid("flow-grandchild")
    correlation = uid("order")

    assert {:ok, %{id: ^root}} =
             flow_create_and_get(root,
               type: "lineage",
               partition_key: partition,
               correlation_id: correlation,
               now_ms: 1_000,
               run_at_ms: 1_000
             )

    assert {:ok, %{id: ^child_a}} =
             flow_create_and_get(child_a,
               type: "lineage",
               partition_key: partition,
               parent_flow_id: root,
               root_flow_id: root,
               correlation_id: correlation,
               now_ms: 2_000,
               run_at_ms: 2_000
             )

    assert {:ok, %{id: ^child_b}} =
             flow_create_and_get(child_b,
               type: "lineage",
               partition_key: partition,
               parent_flow_id: root,
               root_flow_id: root,
               correlation_id: correlation,
               now_ms: 3_000,
               run_at_ms: 3_000
             )

    assert {:ok, %{id: ^grandchild}} =
             flow_create_and_get(grandchild,
               type: "lineage",
               partition_key: partition,
               parent_flow_id: child_a,
               root_flow_id: root,
               correlation_id: correlation,
               now_ms: 4_000,
               run_at_ms: 4_000
             )

    assert {:ok, [%{id: ^child_a}, %{id: ^child_b}]} =
             FerricStore.flow_by_parent(root, partition_key: partition, count: 10)

    assert {:ok, [%{id: ^child_b}, %{id: ^child_a}]} =
             FerricStore.flow_by_parent(root,
               partition_key: partition,
               from_ms: 1_500,
               to_ms: 3_500,
               rev: true,
               count: 10
             )

    assert {:ok, [%{id: ^child_b}]} =
             FerricStore.flow_by_parent(root,
               partition_key: partition,
               rev: true,
               count: 1
             )

    assert {:ok, []} =
             FerricStore.zrange(Ferricstore.Flow.Keys.parent_index_key(root, partition), 0, 10)

    assert {:ok, [%{id: ^root}, %{id: ^child_a}, %{id: ^child_b}, %{id: ^grandchild}]} =
             FerricStore.flow_by_root(root, partition_key: partition, count: 10)

    assert {:ok, [%{id: ^child_b}]} =
             FerricStore.flow_by_root(root,
               partition_key: partition,
               from_ms: 2_500,
               to_ms: 3_500,
               state: "queued",
               count: 10
             )

    assert {:ok, []} =
             FerricStore.zrange(Ferricstore.Flow.Keys.root_index_key(root, partition), 0, 10)

    assert {:ok, [%{id: ^root}, %{id: ^child_a}]} =
             FerricStore.flow_by_correlation(correlation, partition_key: partition, count: 2)

    assert {:ok, child_a_record} = FerricStore.flow_get(child_a, partition_key: partition)

    assert {:ok, _cancelled} =
             flow_cancel_and_get(child_a,
               partition_key: partition,
               fencing_token: child_a_record.fencing_token,
               now_ms: 5_000
             )

    assert {:ok, [%{id: ^child_a, state: "cancelled"}]} =
             FerricStore.flow_by_correlation(correlation,
               partition_key: partition,
               terminal_only: true,
               count: 10
             )

    assert {:ok, []} =
             FerricStore.zrange(
               Ferricstore.Flow.Keys.correlation_index_key(correlation, partition),
               0,
               10
             )
  end

  test "cold lineage query seeks to filtered time window instead of sampling prefix head" do
    previous_limit = Application.get_env(:ferricstore, :flow_lmdb_query_scan_limit)

    try do
      Application.put_env(:ferricstore, :flow_lmdb_query_scan_limit, 2)

      partition = uid("tenant-cold-lineage-window")
      parent = uid("cold-lineage-parent")
      type = uid("cold-lineage-type")

      children =
        Enum.map(1..5, fn n ->
          id = uid("cold-lineage-child-#{n}")

          assert {:ok, _created} =
                   flow_create_and_get(id,
                     type: type,
                     partition_key: partition,
                     parent_flow_id: parent,
                     root_flow_id: parent,
                     state: "queued",
                     now_ms: n * 1_000,
                     run_at_ms: n * 1_000
                   )

          assert {:ok, [claimed]} =
                   FerricStore.flow_claim_due(type,
                     partition_key: partition,
                     worker: "cold-lineage-window",
                     limit: 1,
                     now_ms: n * 1_000
                   )

          assert {:ok, _completed} =
                   flow_complete_and_get(claimed.id, claimed.lease_token,
                     partition_key: partition,
                     fencing_token: claimed.fencing_token,
                     now_ms: n * 1_000
                   )

          id
        end)

      assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(:default, 4)

      assert {:ok, [%{id: id}]} =
               FerricStore.flow_by_parent(parent,
                 partition_key: partition,
                 terminal_only: true,
                 include_cold: true,
                 consistent_projection: true,
                 from_ms: 5_000,
                 to_ms: 5_000,
                 count: 1
               )

      assert id == List.last(children)

      assert {:ok, [%{id: bounded_id}]} =
               FerricStore.flow_by_parent(parent,
                 partition_key: partition,
                 terminal_only: true,
                 include_cold: true,
                 consistent_projection: true,
                 to_ms: 3_000,
                 rev: true,
                 count: 1
               )

      assert bounded_id == Enum.at(children, 2)
    after
      restore_env(:flow_lmdb_query_scan_limit, previous_limit)
    end
  end

  test "reverse cold lineage query samples from the newest side of a filtered window" do
    previous_limit = Application.get_env(:ferricstore, :flow_lmdb_query_scan_limit)

    try do
      Application.put_env(:ferricstore, :flow_lmdb_query_scan_limit, 2)

      partition = uid("tenant-cold-lineage-rev-window")
      parent = uid("cold-lineage-rev-parent")
      type = uid("cold-lineage-rev-type")

      children =
        Enum.map(1..5, fn n ->
          id = uid("cold-lineage-rev-child-#{n}")

          assert {:ok, _created} =
                   flow_create_and_get(id,
                     type: type,
                     partition_key: partition,
                     parent_flow_id: parent,
                     root_flow_id: parent,
                     state: "queued",
                     now_ms: n * 1_000,
                     run_at_ms: n * 1_000
                   )

          assert {:ok, [claimed]} =
                   FerricStore.flow_claim_due(type,
                     partition_key: partition,
                     worker: "cold-lineage-rev-window",
                     limit: 1,
                     now_ms: n * 1_000
                   )

          assert {:ok, _completed} =
                   flow_complete_and_get(claimed.id, claimed.lease_token,
                     partition_key: partition,
                     fencing_token: claimed.fencing_token,
                     now_ms: n * 1_000
                   )

          id
        end)

      assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(:default, 4)

      assert {:ok, [%{id: id}]} =
               FerricStore.flow_by_parent(parent,
                 partition_key: partition,
                 terminal_only: true,
                 include_cold: true,
                 consistent_projection: true,
                 from_ms: 2_000,
                 rev: true,
                 count: 1
               )

      assert id == List.last(children)
    after
      restore_env(:flow_lmdb_query_scan_limit, previous_limit)
    end
  end

  test "flow_create_many creates one-partition batch atomically" do
    partition = uid("tenant")
    type = uid("bulk-create")
    id_a = uid("bulk-a")
    id_b = uid("bulk-b")

    assert {:ok, flows} =
             flow_create_many_and_get(
               partition,
               [
                 %{id: id_a, payload: "payload:" <> id_a},
                 %{id: id_b, payload: "payload:" <> id_b}
               ],
               type: type,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert Enum.map(flows, & &1.id) == [id_a, id_b]
    assert Enum.all?(flows, &(&1.partition_key == partition))

    assert {:ok, claimed} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "worker-a",
               limit: 10,
               now_ms: 1_000
             )

    assert claimed |> Enum.map(& &1.id) |> MapSet.new() == MapSet.new([id_a, id_b])
  end

  test "flow write APIs return ok and reject return option" do
    partition = uid("tenant-return")
    type = uid("return-api")
    id = uid("return-flow")

    assert :ok =
             FerricStore.flow_create_many(
               partition,
               [%{id: id, payload: "payload:" <> id}],
               type: type,
               state: "queued",
               run_at_ms: 1_000,
               now_ms: 900
             )

    assert :ok =
             FerricStore.flow_transition_many(
               partition,
               "queued",
               "ready",
               [%{id: id, fencing_token: 0}],
               run_at_ms: 1_000,
               now_ms: 950
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               state: "ready",
               worker: "worker-return",
               limit: 1,
               now_ms: 1_000
             )

    assert claimed.id == id
    assert claimed.payload == "payload:" <> id
    assert is_binary(claimed.lease_token)
    assert is_integer(claimed.fencing_token)

    assert :ok =
             FerricStore.flow_complete_many(
               partition,
               [
                 %{
                   id: claimed.id,
                   lease_token: claimed.lease_token,
                   fencing_token: claimed.fencing_token
                 }
               ],
               result: "ok",
               now_ms: 1_100
             )

    rejected_id = uid("return-rejected")

    assert {:error, "ERR flow return option is not supported"} =
             FerricStore.flow_create_many(
               partition,
               [%{id: rejected_id}],
               type: type,
               state: "queued",
               run_at_ms: 1_000,
               now_ms: 900,
               return: :full
             )
  end

  test "flow_create_many lets item attrs override common attrs" do
    partition = uid("tenant")
    type = uid("bulk-create-override")
    id_a = uid("bulk-override-a")
    id_b = uid("bulk-override-b")

    assert {:ok, [flow_a, flow_b]} =
             flow_create_many_and_get(
               partition,
               [
                 %{id: id_a},
                 %{id: id_b, payload: "payload:item", correlation_id: "corr:item"}
               ],
               type: type,
               payload: "payload:common",
               correlation_id: "corr:common",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert is_binary(flow_a.payload_ref)
    assert flow_a.payload_ref != "payload:common"
    assert flow_a.correlation_id == "corr:common"
    assert is_binary(flow_b.payload_ref)
    assert flow_b.payload_ref != "payload:item"
    assert flow_b.payload_ref != flow_a.payload_ref
    assert flow_b.correlation_id == "corr:item"
  end

  test "flow_create_many auto-partitions missing partition and rolls back duplicate batches" do
    partition = uid("tenant")
    type = uid("bulk-atomic")
    existing_id = uid("bulk-existing")
    new_id = uid("bulk-new")

    assert {:ok, [auto_flow]} =
             flow_create_many_and_get(nil, [%{id: new_id}], type: type)

    assert auto_flow.id == new_id
    assert Ferricstore.Flow.Keys.auto_partition_key?(auto_flow.partition_key)

    assert {:ok, _} =
             flow_create_and_get(existing_id,
               type: type,
               partition_key: partition,
               run_at_ms: 1_000
             )

    assert {:error, "ERR flow already exists"} =
             flow_create_many_and_get(
               partition,
               [%{id: existing_id}, %{id: new_id}],
               type: type,
               run_at_ms: 1_000
             )

    assert {:ok, nil} = FerricStore.flow_get(new_id, partition_key: partition)
    assert {:ok, []} = FerricStore.flow_history(new_id, partition_key: partition)

    assert {:error, "ERR flow duplicate id in batch"} =
             flow_create_many_and_get(
               partition,
               [%{id: new_id}, %{id: new_id}],
               type: type,
               run_at_ms: 1_000
             )

    assert {:ok, nil} = FerricStore.flow_get(new_id, partition_key: partition)
    assert {:ok, []} = FerricStore.flow_history(new_id, partition_key: partition)
  end

  test "flow_create_many independent keeps successful items when one create fails" do
    partition = uid("tenant-create-many-independent")
    type = uid("bulk-independent-create")
    existing_id = uid("bulk-independent-existing")
    new_id = uid("bulk-independent-new")

    assert {:ok, _} =
             flow_create_and_get(existing_id,
               type: type,
               partition_key: partition,
               run_at_ms: 1_000
             )

    assert {:ok, [{:error, "ERR flow already exists"}, :ok]} =
             FerricStore.flow_create_many(
               partition,
               [%{id: existing_id}, %{id: new_id}],
               type: type,
               run_at_ms: 1_000,
               independent: true
             )

    assert {:ok, %{id: ^new_id, state: "queued"}} =
             FerricStore.flow_get(new_id, partition_key: partition)
  end

  test "flow_create_many independent auto-partitions without client-side grouping" do
    type = uid("bulk-independent-auto-create")
    existing_id = uid("bulk-independent-auto-existing")
    new_id = uid("bulk-independent-auto-new")
    ids = Enum.map(1..32, fn idx -> uid("bulk-independent-auto-#{idx}") end)

    assert {:ok, _} =
             flow_create_and_get(existing_id,
               type: type,
               run_at_ms: 1_000
             )

    assert {:ok, results} =
             FerricStore.flow_create_many(
               nil,
               [%{id: existing_id}, %{id: new_id} | Enum.map(ids, &%{id: &1})],
               type: type,
               run_at_ms: 1_000,
               independent: true
             )

    assert [{:error, "ERR flow already exists"} | created_results] = results
    assert Enum.all?(created_results, &(&1 == :ok))

    for id <- [new_id | ids] do
      assert {:ok, %{id: ^id, state: "queued", partition_key: partition_key}} =
               FerricStore.flow_get(id)

      assert Ferricstore.Flow.Keys.auto_partition_key?(partition_key)
    end
  end

  test "flow_create_many independent auto-partitioned flows are claimable by partition keys" do
    type = uid("bulk-independent-auto-claim")
    ids = Enum.map(1..128, fn idx -> uid("bulk-independent-auto-claim-#{idx}") end)

    assert {:ok, results} =
             FerricStore.flow_create_many(
               nil,
               Enum.map(ids, &%{id: &1}),
               type: type,
               run_at_ms: 1_000,
               now_ms: 1_000,
               independent: true
             )

    assert Enum.all?(results, &(&1 == :ok))

    partition_keys =
      ids
      |> Enum.map(fn id ->
        assert {:ok, %{partition_key: partition_key}} = FerricStore.flow_get(id)
        partition_key
      end)
      |> Enum.uniq()

    claimed =
      partition_keys
      |> Enum.chunk_every(32)
      |> Enum.flat_map(fn keys ->
        assert {:ok, records} =
                 FerricStore.flow_claim_due(type,
                   partition_keys: keys,
                   state: "queued",
                   worker: "worker-auto-create-many",
                   lease_ms: 30_000,
                   limit: length(ids),
                   now_ms: 1_000
                 )

        records
      end)

    assert MapSet.new(Enum.map(claimed, & &1.id)) == MapSet.new(ids)
  end

  test "flow_claim_due partition_keys tries later shards when an earlier stale candidate claims nothing" do
    ctx = FerricStore.Instance.get(:default)
    type = uid("claim-partitions-stale-first")

    partitions_by_shard =
      1..256
      |> Enum.map(fn idx -> "tenant:#{type}:#{idx}" end)
      |> Enum.map(fn partition_key ->
        due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition_key)
        {Ferricstore.Store.Router.shard_for(ctx, due_key), partition_key}
      end)
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))

    [{stale_shard, [stale_partition | _]}, {_real_shard, [real_partition | _]} | _] =
      Enum.sort_by(partitions_by_shard, fn {shard, _partitions} -> shard end)

    real_id = uid("real-partition")

    assert {:ok, %{id: ^real_id}} =
             flow_create_and_get(real_id,
               type: type,
               partition_key: real_partition,
               run_at_ms: 1_000,
               now_ms: 900
             )

    stale_due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, stale_partition)
    assert Ferricstore.Store.Router.shard_for(ctx, stale_due_key) == stale_shard

    {flow_index, flow_lookup} = Ferricstore.Flow.OrderedIndex.table_names(ctx.name, stale_shard)
    native = Ferricstore.Flow.NativeOrderedIndex.get(flow_index, flow_lookup)
    stale_id = uid("stale-native-candidate")

    assert :ok =
             Ferricstore.Flow.NativeOrderedIndex.put_new_member(
               native,
               stale_due_key,
               stale_id,
               1_000
             )

    assert [{^stale_id, 1_000.0}] =
             Ferricstore.Flow.NativeOrderedIndex.range_slice(
               native,
               stale_due_key,
               :neg_inf,
               {:inclusive, 1_000.0},
               false,
               0,
               10
             )

    cursor_table =
      case :ets.whereis(:ferricstore_flow_claim_due_any_cursor) do
        :undefined ->
          :ets.new(:ferricstore_flow_claim_due_any_cursor, [
            :named_table,
            :public,
            read_concurrency: true,
            write_concurrency: true
          ])

        tid ->
          tid
      end

    :ets.insert(cursor_table, {{ctx.name, type}, stale_shard})

    assert {:ok, [%{id: ^real_id}]} =
             FerricStore.flow_claim_due(type,
               partition_keys: [stale_partition, real_partition],
               state: "queued",
               worker: "worker-stale-first",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )
  end

  test "flow_create_many idempotent retry returns existing records without duplicate writes" do
    partition = uid("tenant")
    type = uid("bulk-idempotent")
    existing_id = uid("bulk-existing")
    new_id = uid("bulk-new")

    assert {:ok, %{id: ^existing_id}} =
             flow_create_and_get(existing_id,
               type: type,
               partition_key: partition,
               payload: "payload:" <> existing_id,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, records} =
             flow_create_many_and_get(
               partition,
               [
                 %{id: existing_id, payload: "payload:" <> existing_id},
                 %{id: new_id, payload: "payload:" <> new_id}
               ],
               type: type,
               run_at_ms: 1_000,
               now_ms: 1_000,
               idempotent: true
             )

    assert Enum.map(records, & &1.id) == [existing_id, new_id]

    assert {:ok, existing_history} =
             FerricStore.flow_history(existing_id, partition_key: partition)

    assert Enum.map(existing_history, fn {_event_id, fields} -> fields["event"] end) == [
             "created"
           ]

    assert {:ok, %{id: ^new_id}} = FerricStore.flow_get(new_id, partition_key: partition)
  end

  test "flow_create_many idempotency conflict rolls back same shard group" do
    partition = uid("tenant")
    type = uid("bulk-idempotency-conflict")
    existing_id = uid("bulk-existing")
    new_id = uid("bulk-new")

    assert {:ok, _} =
             flow_create_and_get(existing_id,
               type: type,
               partition_key: partition,
               payload: "payload:old",
               run_at_ms: 1_000
             )

    assert {:error, "ERR flow idempotency conflict"} =
             flow_create_many_and_get(
               partition,
               [
                 %{id: existing_id, payload: "payload:new"},
                 %{id: new_id, payload: "payload:" <> new_id}
               ],
               type: type,
               run_at_ms: 1_000,
               idempotent: true
             )

    assert {:ok, nil} = FerricStore.flow_get(new_id, partition_key: partition)
    assert {:ok, []} = FerricStore.flow_history(new_id, partition_key: partition)
  end

  test "flow_spawn_children wait none creates full child flows and advances parent" do
    parent = uid("flow-parent-none")
    child_a = uid("flow-child-none-a")
    child_b = uid("flow-child-none-b")
    partition = uid("tenant")

    assert {:ok, created_parent} =
             flow_create_and_get(parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition,
               now_ms: 1_000
             )

    assert {:ok, advanced} =
             flow_spawn_children_and_get(
               parent,
               [
                 %{id: child_a, type: "child", payload: %{n: 1}},
                 %{id: child_b, type: "child", payload: %{n: 2}}
               ],
               group_id: "fanout-1",
               wait: :none,
               on_child_failed: :ignore,
               on_parent_closed: :abandon_children,
               exhaust_to: %{success: "dispatched", failure: "dispatch_failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token,
               now_ms: 1_010
             )

    assert advanced.state == "dispatched"
    assert advanced.child_groups["fanout-1"]["resolved"] == "success"

    assert {:ok, children} = FerricStore.flow_by_parent(parent, partition_key: partition)
    assert Enum.map(children, & &1.id) |> Enum.sort() == Enum.sort([child_a, child_b])
    assert Enum.all?(children, &(&1.root_flow_id == parent))
  end

  test "flow_spawn_children wait all advances parent only after direct children finish" do
    parent = uid("flow-parent-all")
    child_a = uid("flow-child-all-a")
    child_b = uid("flow-child-all-b")
    partition = uid("tenant")

    assert {:ok, created_parent} =
             flow_create_and_get(parent,
               type: "parent",
               state: "dispatch",
               partition_key: partition,
               now_ms: 2_000
             )

    assert {:ok, waiting} =
             flow_spawn_children_and_get(
               parent,
               [
                 %{id: child_a, type: "child", state: "queued"},
                 %{id: child_b, type: "child", state: "queued"}
               ],
               group_id: "fanout-1",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :fail_parent,
               on_parent_closed: :cancel_children,
               exhaust_to: %{success: "children_done", failure: "failed"},
               partition_key: partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token,
               now_ms: 2_010
             )

    assert waiting.state == "waiting_children"
    assert waiting.child_groups["fanout-1"]["summary"]["completed"] == 0

    claimed_a = create_claimed_flow_child(child_a, partition, "worker-a")

    assert {:ok, _child_done_a} =
             flow_complete_and_get(child_a, claimed_a.lease_token,
               partition_key: partition,
               fencing_token: claimed_a.fencing_token,
               result: "child-a-result",
               now_ms: 2_020
             )

    assert {:ok, still_waiting} = FerricStore.flow_get(parent, partition_key: partition)
    assert still_waiting.state == "waiting_children"
    assert still_waiting.child_groups["fanout-1"]["summary"]["completed"] == 1
    child_a_result = still_waiting.child_groups["fanout-1"]["results"][child_a]
    assert child_a_result["status"] == "completed"
    assert is_binary(child_a_result["result_ref"])

    claimed_b = create_claimed_flow_child(child_b, partition, "worker-b")

    assert {:ok, _child_done_b} =
             flow_complete_and_get(child_b, claimed_b.lease_token,
               partition_key: partition,
               fencing_token: claimed_b.fencing_token,
               now_ms: 2_030
             )

    assert {:ok, done_parent} = FerricStore.flow_get(parent, partition_key: partition)
    assert done_parent.state == "children_done"
    assert done_parent.child_groups["fanout-1"]["resolved"] == "success"
    assert done_parent.child_groups["fanout-1"]["summary"]["completed"] == 2
  end
    end
  end
end
