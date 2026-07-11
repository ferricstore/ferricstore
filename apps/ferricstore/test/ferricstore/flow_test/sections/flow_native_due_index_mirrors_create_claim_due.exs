defmodule Ferricstore.FlowTest.Sections.FlowNativeDueIndexMirrorsCreateClaimDue do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Test.ShardHelpers

      test "Flow native due index mirrors create and claim_due" do
        ctx = FerricStore.Instance.get(:default)
        partition = uid("tenant-native-index")
        type = uid("native-index")
        id = uid("flow-native-index")

        assert {:ok, _} =
                 flow_create_and_get(id,
                   type: type,
                   partition_key: partition,
                   run_at_ms: 1_000,
                   now_ms: 900
                 )

        due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition)
        shard_index = Ferricstore.Store.Router.shard_for(ctx, due_key)

        {flow_index, flow_lookup} =
          Ferricstore.Flow.OrderedIndex.table_names(ctx.name, shard_index)

        assert native = Ferricstore.Flow.NativeOrderedIndex.get(flow_index, flow_lookup)

        {zset_index, zset_lookup} =
          Ferricstore.Store.Shard.ZSetIndex.table_names(ctx.name, shard_index)

        assert [{^id, 1000.0}] =
                 Ferricstore.Flow.NativeOrderedIndex.range_slice(
                   native,
                   due_key,
                   :neg_inf,
                   {:inclusive, 1_000.0},
                   false,
                   0,
                   10
                 )

        assert :undefined = :ets.whereis(flow_index)
        assert :undefined = :ets.whereis(flow_lookup)

        assert 0 =
                 Ferricstore.Store.Shard.ZSetIndex.count(
                   zset_index,
                   zset_lookup,
                   due_key,
                   :neg_inf,
                   :inf
                 )

        assert {:ok, [%{id: ^id, state: "running"}]} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition,
                   worker: "worker-native-index",
                   limit: 1,
                   now_ms: 1_000,
                   lease_ms: 30_000
                 )

        running_due_key = Ferricstore.Flow.Keys.due_key(type, "running", 0, partition)
        queued_due_p1_key = Ferricstore.Flow.Keys.due_key(type, "queued", 1, partition)
        queued_due_p2_key = Ferricstore.Flow.Keys.due_key(type, "queued", 2, partition)

        refute :ets.member(zset_lookup, {:ready, due_key})
        refute :ets.member(zset_lookup, {:ready, queued_due_p1_key})
        refute :ets.member(zset_lookup, {:ready, queued_due_p2_key})
        refute :ets.member(zset_lookup, {:ready, running_due_key})

        assert [] =
                 Ferricstore.Flow.NativeOrderedIndex.range_slice(
                   native,
                   due_key,
                   :neg_inf,
                   {:inclusive, 1_000.0},
                   false,
                   0,
                   10
                 )

        assert [{^id, 31_000.0}] =
                 Ferricstore.Flow.NativeOrderedIndex.range_slice(
                   native,
                   running_due_key,
                   :neg_inf,
                   {:inclusive, 31_000.0},
                   false,
                   0,
                   10
                 )

        assert [] =
                 Ferricstore.Flow.OrderedIndex.range_slice(
                   flow_index,
                   due_key,
                   :neg_inf,
                   {:inclusive, 1_000.0},
                   false,
                   0,
                   10
                 )

        assert {:ok, []} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition,
                   worker: "worker-native-index-2",
                   limit: 1,
                   now_ms: 1_000,
                   lease_ms: 30_000
                 )

        inflight_key = Ferricstore.Flow.Keys.inflight_index_key(type, partition)
        assert 1 = Ferricstore.Flow.NativeOrderedIndex.count_all(native, inflight_key)
      end

      test "FLUSHDB clears native Flow secondary indexes after durable deletes" do
        ctx = FerricStore.Instance.get(:default)
        partition = uid("tenant-native-flush")
        type = uid("native-flush")
        id = uid("flow-native-flush")

        assert {:ok, _} =
                 flow_create_and_get(id,
                   type: type,
                   partition_key: partition,
                   run_at_ms: 1_000,
                   now_ms: 900
                 )

        due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition)
        shard_index = Ferricstore.Store.Router.shard_for(ctx, due_key)

        {flow_index, flow_lookup} =
          Ferricstore.Flow.OrderedIndex.table_names(ctx.name, shard_index)

        assert native = Ferricstore.Flow.NativeOrderedIndex.get(flow_index, flow_lookup)

        assert [{^id, 1000.0}] =
                 Ferricstore.Flow.NativeOrderedIndex.range_slice(
                   native,
                   due_key,
                   :neg_inf,
                   :inf,
                   false,
                   0,
                   10
                 )

        assert :ok = FerricStore.flushdb()
        assert reset_native = Ferricstore.Flow.NativeOrderedIndex.get(flow_index, flow_lookup)
        assert reset_native != native

        assert [] =
                 Ferricstore.Flow.NativeOrderedIndex.range_slice(
                   reset_native,
                   due_key,
                   :neg_inf,
                   :inf,
                   false,
                   0,
                   10
                 )
      end

      test "flow_claim_due validates record due time when native due score is stale" do
        ctx = FerricStore.Instance.get(:default)
        partition = uid("tenant-native-stale-score")
        type = uid("native-stale-score")
        id = uid("flow-native-stale-score")

        assert {:ok, _} =
                 flow_create_and_get(id,
                   type: type,
                   partition_key: partition,
                   run_at_ms: 10_000,
                   now_ms: 1_000
                 )

        due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition)
        shard_index = Ferricstore.Store.Router.shard_for(ctx, due_key)

        {flow_index, flow_lookup} =
          Ferricstore.Flow.OrderedIndex.table_names(ctx.name, shard_index)

        assert native = Ferricstore.Flow.NativeOrderedIndex.get(flow_index, flow_lookup)

        assert :ok = Ferricstore.Flow.NativeOrderedIndex.put_member(native, due_key, id, 1_000)

        assert {:ok, []} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition,
                   worker: "worker-native-stale-score",
                   limit: 1,
                   now_ms: 1_000,
                   lease_ms: 30_000
                 )

        assert {:ok, [%{id: ^id, state: "running"}]} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition,
                   worker: "worker-native-stale-score",
                   limit: 1,
                   now_ms: 10_000,
                   lease_ms: 30_000
                 )
      end

      test "flow_claim_due keeps metadata-indexed flows claimable through generic fallback" do
        partition = uid("tenant-claim-metadata")
        type = uid("claim-metadata")
        id = uid("flow-claim-metadata")
        correlation_id = uid("correlation")

        assert {:ok, _} =
                 flow_create_and_get(id,
                   type: type,
                   partition_key: partition,
                   correlation_id: correlation_id,
                   run_at_ms: 1_000,
                   now_ms: 900
                 )

        assert {:ok, [%{id: ^id, state: "running", correlation_id: ^correlation_id}]} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition,
                   worker: "worker-claim-metadata",
                   limit: 1,
                   now_ms: 1_000,
                   lease_ms: 30_000
                 )

        assert {:ok, []} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition,
                   worker: "worker-claim-metadata-2",
                   limit: 1,
                   now_ms: 1_000,
                   lease_ms: 30_000
                 )
      end

      test "flow_claim_due restores native overfetch beyond requested limit" do
        partition = uid("tenant-native-overfetch")
        type = uid("native-overfetch")
        ids = for i <- 1..3, do: uid("flow-native-overfetch-#{i}")

        for id <- ids do
          assert {:ok, _} =
                   flow_create_and_get(id,
                     type: type,
                     partition_key: partition,
                     run_at_ms: 1_000,
                     now_ms: 900
                   )
        end

        assert {:ok, [%{id: first_id}]} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition,
                   worker: "worker-native-overfetch-1",
                   limit: 1,
                   now_ms: 1_000,
                   lease_ms: 30_000
                 )

        assert first_id in ids

        assert {:ok, [%{id: second_id}]} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition,
                   worker: "worker-native-overfetch-2",
                   limit: 1,
                   now_ms: 1_000,
                   lease_ms: 30_000
                 )

        assert second_id in ids
        assert second_id != first_id
      end

      test "partition_key scopes claim, complete, retry, get, and history" do
        partition = uid("tenant")
        id = uid("flow-partition")

        assert {:ok, flow} =
                 flow_create_and_get(id,
                   type: "email",
                   partition_key: partition,
                   state: "queued",
                   run_at_ms: 1_000,
                   now_ms: 999
                 )

        assert flow.partition_key == partition

        assert {:ok, nil} = FerricStore.flow_get(id)
        assert {:ok, fetched} = FerricStore.flow_get(id, partition_key: partition)
        assert fetched.id == id

        assert {:ok, []} =
                 FerricStore.flow_claim_due("email",
                   state: "queued",
                   worker: "worker-a",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due("email",
                   partition_key: partition,
                   state: "queued",
                   worker: "worker-a",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert claimed.partition_key == partition

        assert {:error, "ERR flow not found"} =
                 flow_complete_and_get(id, claimed.lease_token,
                   fencing_token: claimed.fencing_token
                 )

        assert {:ok, completed} =
                 flow_complete_and_get(id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   partition_key: partition
                 )

        assert completed.state == "completed"

        assert {:ok, events} = FerricStore.flow_history(id, partition_key: partition)

        assert Enum.map(events, fn {_id, fields} -> fields["event"] end) == [
                 "created",
                 "claimed",
                 "completed"
               ]
      end

      test "flow_claim_due only scans the selected partition" do
        partition_a = uid("tenant-a")
        partition_b = uid("tenant-b")
        id_a = uid("flow-partition-claim-a")
        id_b = uid("flow-partition-claim-b")

        assert {:ok, _} =
                 flow_create_and_get(id_a,
                   type: "email",
                   partition_key: partition_a,
                   run_at_ms: 1_000
                 )

        assert {:ok, _} =
                 flow_create_and_get(id_b,
                   type: "email",
                   partition_key: partition_b,
                   run_at_ms: 1_000
                 )

        assert {:ok, [claimed_a]} =
                 FerricStore.flow_claim_due("email",
                   partition_key: partition_a,
                   worker: "worker-a",
                   lease_ms: 30_000,
                   limit: 10,
                   now_ms: 1_000
                 )

        assert claimed_a.id == id_a
        assert claimed_a.partition_key == partition_a

        assert {:ok, []} =
                 FerricStore.flow_claim_due("email",
                   partition_key: partition_a,
                   worker: "worker-a",
                   lease_ms: 30_000,
                   limit: 10,
                   now_ms: 1_000
                 )

        assert {:ok, [claimed_b]} =
                 FerricStore.flow_claim_due("email",
                   partition_key: partition_b,
                   worker: "worker-b",
                   lease_ms: 30_000,
                   limit: 10,
                   now_ms: 1_000
                 )

        assert claimed_b.id == id_b
        assert claimed_b.partition_key == partition_b
      end

      test "flow_claim_due can scan selected partition_keys in one call" do
        partition_a = uid("tenant-a")
        partition_b = uid("tenant-b")
        partition_c = uid("tenant-c")
        type = uid("claim-partition-keys")
        id_a = uid("flow-partition-keys-a")
        id_b = uid("flow-partition-keys-b")
        id_c = uid("flow-partition-keys-c")

        assert {:ok, _} =
                 flow_create_and_get(id_a,
                   type: type,
                   partition_key: partition_a,
                   run_at_ms: 1_000
                 )

        assert {:ok, _} =
                 flow_create_and_get(id_b,
                   type: type,
                   partition_key: partition_b,
                   run_at_ms: 1_000
                 )

        assert {:ok, _} =
                 flow_create_and_get(id_c,
                   type: type,
                   partition_key: partition_c,
                   run_at_ms: 1_000
                 )

        assert {:ok, claimed} =
                 FerricStore.flow_claim_due(type,
                   partition_keys: [partition_a, partition_b],
                   worker: "worker-many-partitions",
                   lease_ms: 30_000,
                   limit: 10,
                   now_ms: 1_000
                 )

        assert MapSet.new(Enum.map(claimed, & &1.id)) == MapSet.new([id_a, id_b])
        assert Enum.all?(claimed, &(&1.partition_key in [partition_a, partition_b]))

        assert {:ok, [claimed_c]} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition_c,
                   worker: "worker-c",
                   lease_ms: 30_000,
                   limit: 10,
                   now_ms: 1_000
                 )

        assert claimed_c.id == id_c
      end

      test "flow_create without partition auto-spreads and default claim_due fetches across buckets" do
        type = uid("claim-auto-bucket")

        ids =
          1..64
          |> Enum.map(fn i -> uid("flow-auto-bucket-#{i}") end)

        shards =
          ids
          |> Enum.map(fn id -> shard_for(Ferricstore.Flow.Keys.state_key(id)) end)
          |> MapSet.new()

        assert MapSet.size(shards) > 1

        Enum.each(ids, fn id ->
          assert {:ok, flow} =
                   flow_create_and_get(id,
                     type: type,
                     run_at_ms: 1_000
                   )

          assert flow.id == id
          assert Ferricstore.Flow.Keys.auto_partition_key?(flow.partition_key)
        end)

        claimed =
          1..16
          |> Enum.reduce_while([], fn _round, acc ->
            case FerricStore.flow_claim_due(type,
                   worker: "worker-auto-bucket",
                   lease_ms: 30_000,
                   limit: length(ids),
                   now_ms: 1_000
                 ) do
              {:ok, []} ->
                {:cont, acc}

              {:ok, records} ->
                next = records ++ acc

                if MapSet.new(Enum.map(next, & &1.id)) == MapSet.new(ids) do
                  {:halt, next}
                else
                  {:cont, next}
                end

              {:error, _reason} = error ->
                flunk("claim_due failed: #{inspect(error)}")
            end
          end)

        assert MapSet.new(Enum.map(claimed, & &1.id)) == MapSet.new(ids)

        assert claimed
               |> Enum.map(& &1.partition_key)
               |> Enum.all?(&Ferricstore.Flow.Keys.auto_partition_key?/1)
      end

      test "flow_claim_due can scan any partition and selected states" do
        {partition_a, partition_b} = different_partition_keys()
        type = uid("claim-any")
        queued_global = uid("flow-claim-any-global")
        queued_partition = uid("flow-claim-any-queued")
        ready_partition = uid("flow-claim-any-ready")
        held_partition = uid("flow-claim-any-held")

        assert {:ok, _} = flow_create_and_get(queued_global, type: type, run_at_ms: 1_000)

        assert {:ok, _} =
                 flow_create_and_get(queued_partition,
                   type: type,
                   partition_key: partition_b,
                   run_at_ms: 1_000
                 )

        assert {:ok, _} =
                 flow_create_and_get(ready_partition,
                   type: type,
                   state: "ready",
                   partition_key: partition_a,
                   run_at_ms: 1_000
                 )

        assert {:ok, _} =
                 flow_create_and_get(held_partition,
                   type: type,
                   state: "held",
                   partition_key: partition_b,
                   run_at_ms: 1_000
                 )

        assert {:ok, claimed} =
                 FerricStore.flow_claim_due(type,
                   partition_key: :any,
                   states: ["queued", "ready"],
                   worker: "worker-any",
                   lease_ms: 30_000,
                   limit: 10,
                   now_ms: 1_000
                 )

        assert MapSet.new(Enum.map(claimed, & &1.id)) ==
                 MapSet.new([queued_global, queued_partition, ready_partition])

        assert {:ok, [claimed_held]} =
                 FerricStore.flow_claim_due(type,
                   partition_key: :any,
                   state: :any,
                   worker: "worker-any",
                   lease_ms: 30_000,
                   limit: 10,
                   now_ms: 1_000
                 )

        assert claimed_held.id == held_partition
        assert claimed_held.partition_key == partition_b
      end

      test "flow_claim_due omitted state claims any due state" do
        type = uid("claim-omitted-state-any")
        queued_id = uid("flow-claim-omitted-queued")
        ready_id = uid("flow-claim-omitted-ready")

        assert {:ok, _} = flow_create_and_get(queued_id, type: type, run_at_ms: 1_000)

        assert {:ok, _} =
                 flow_create_and_get(ready_id,
                   type: type,
                   state: "ready",
                   run_at_ms: 1_000
                 )

        assert {:ok, claimed} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-omitted-state-any",
                   lease_ms: 30_000,
                   limit: 10,
                   now_ms: 1_000
                 )

        assert MapSet.new(Enum.map(claimed, & &1.id)) == MapSet.new([queued_id, ready_id])
      end

      test "flow_claim_due any partition drains higher priority across shards first" do
        type = uid("claim-any-priority")
        low_id = uid("flow-claim-any-low")
        high_id = uid("flow-claim-any-high")

        case :ets.whereis(:ferricstore_flow_claim_due_any_cursor) do
          :undefined -> :ok
          table -> :ets.delete_all_objects(table)
        end

        partitions_by_shard =
          1..512
          |> Enum.map(&"#{type}:partition:#{&1}")
          |> Enum.group_by(fn partition ->
            shard_for(Ferricstore.Flow.Keys.state_key("probe", partition))
          end)

        [low_partition | _] = Map.fetch!(partitions_by_shard, 0)

        {_high_shard, [high_partition | _]} =
          Enum.find(partitions_by_shard, fn {shard, _partitions} -> shard != 0 end)

        assert {:ok, _} =
                 flow_create_and_get(low_id,
                   type: type,
                   partition_key: low_partition,
                   priority: 0,
                   run_at_ms: 1_000
                 )

        assert {:ok, _} =
                 flow_create_and_get(high_id,
                   type: type,
                   partition_key: high_partition,
                   priority: 2,
                   run_at_ms: 1_000
                 )

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due(type,
                   partition_key: :any,
                   state: :any,
                   worker: "worker-any-priority",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert claimed.id == high_id
        assert claimed.partition_key == high_partition
      end

      test "flow_claim_due any partition rotates start shard per type" do
        type = uid("claim-any-rotate")

        case :ets.whereis(:ferricstore_flow_claim_due_any_cursor) do
          :undefined -> :ok
          table -> :ets.delete_all_objects(table)
        end

        partitions_by_shard =
          1..512
          |> Enum.map(&"#{type}:partition:#{&1}")
          |> Enum.group_by(fn partition ->
            shard_for(Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition))
          end)

        [partition_0 | _] = Map.fetch!(partitions_by_shard, 0)
        [partition_1 | _] = Map.fetch!(partitions_by_shard, 1)

        for {partition, suffix} <- [
              {partition_0, "0a"},
              {partition_0, "0b"},
              {partition_1, "1a"},
              {partition_1, "1b"}
            ] do
          assert {:ok, _} =
                   flow_create_and_get(uid("flow-claim-any-rotate-#{suffix}"),
                     type: type,
                     partition_key: partition,
                     priority: 0,
                     run_at_ms: 1_000
                   )
        end

        assert {:ok, [first]} =
                 FerricStore.flow_claim_due(type,
                   partition_key: :any,
                   state: :any,
                   worker: "worker-any-rotate",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert first.partition_key in [partition_0, partition_1]

        assert {:ok, [second]} =
                 FerricStore.flow_claim_due(type,
                   partition_key: :any,
                   state: :any,
                   worker: "worker-any-rotate",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert second.partition_key in [partition_0, partition_1]
        assert second.partition_key != first.partition_key
      end

      test "flow_claim_due any partition spreads small limits across shards under skew" do
        type = uid("claim-any-skew")

        case :ets.whereis(:ferricstore_flow_claim_due_any_cursor) do
          :undefined -> :ok
          table -> :ets.delete_all_objects(table)
        end

        partitions_by_shard =
          1..512
          |> Enum.map(&"#{type}:partition:#{&1}")
          |> Enum.group_by(fn partition ->
            shard_for(Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition))
          end)

        [hot_partition | _] = Map.fetch!(partitions_by_shard, 0)
        [other_partition | _] = Map.fetch!(partitions_by_shard, 1)

        for suffix <- ["hot-a", "hot-b"] do
          assert {:ok, _} =
                   flow_create_and_get(uid("flow-claim-any-skew-#{suffix}"),
                     type: type,
                     partition_key: hot_partition,
                     run_at_ms: 1_000
                   )
        end

        assert {:ok, _} =
                 flow_create_and_get(uid("flow-claim-any-skew-other"),
                   type: type,
                   partition_key: other_partition,
                   run_at_ms: 1_000
                 )

        assert {:ok, claimed} =
                 FerricStore.flow_claim_due(type,
                   partition_key: :any,
                   state: :any,
                   worker: "worker-any-skew",
                   lease_ms: 30_000,
                   limit: 2,
                   now_ms: 1_000
                 )

        assert length(claimed) == 2

        assert MapSet.new(Enum.map(claimed, & &1.partition_key)) ==
                 MapSet.new([hot_partition, other_partition])
      end

      test "flow_claim_due skips stale due index members without starving live work" do
        stale_id = "a-" <> uid("flow-stale-due")
        live_id = "z-" <> uid("flow-live-due")

        assert {:ok, _} =
                 flow_create_and_get(stale_id, type: "stale-scan", run_at_ms: 1_000)

        assert {:ok, _} =
                 flow_create_and_get(live_id, type: "stale-scan", run_at_ms: 1_000)

        assert {:ok, 1} = internal_del(Ferricstore.Flow.Keys.state_key(stale_id))

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due("stale-scan",
                   worker: "worker-a",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert claimed.id == live_id
      end

      test "flow_claim_due single-key native scan continues after stale candidate batch" do
        ctx = FerricStore.Instance.get(:default)
        partition_key = uid("single-key-stale-partition")
        type = uid("claim-single-key-stale-native")
        live_id = "z-" <> uid("flow-single-key-live")
        due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition_key)
        shard_index = Ferricstore.Store.Router.shard_for(ctx, due_key)

        {flow_index, flow_lookup} =
          Ferricstore.Flow.OrderedIndex.table_names(ctx.name, shard_index)

        assert native = Ferricstore.Flow.NativeOrderedIndex.get(flow_index, flow_lookup)

        for i <- 1..32 do
          assert :ok =
                   Ferricstore.Flow.NativeOrderedIndex.put_member(
                     native,
                     due_key,
                     "a-stale-native-#{i}",
                     1_000
                   )
        end

        assert {:ok, _} =
                 flow_create_and_get(live_id,
                   type: type,
                   partition_key: partition_key,
                   state: "queued",
                   run_at_ms: 1_000
                 )

        assert {:ok, [%{id: ^live_id, partition_key: ^partition_key}]} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition_key,
                   worker: "worker-single-key-stale-native",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1_000
                 )
      end

      test "flow_claim_due multi-key native scan continues after stale candidate batch" do
        ctx = FerricStore.Instance.get(:default)
        {stale_partition, live_partition, _other_partition} = mixed_partition_keys()
        type = uid("claim-multi-key-stale-native")
        live_id = uid("flow-multi-key-live")
        due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, stale_partition)
        shard_index = Ferricstore.Store.Router.shard_for(ctx, due_key)

        {flow_index, flow_lookup} =
          Ferricstore.Flow.OrderedIndex.table_names(ctx.name, shard_index)

        assert native = Ferricstore.Flow.NativeOrderedIndex.get(flow_index, flow_lookup)

        for i <- 1..32 do
          assert :ok =
                   Ferricstore.Flow.NativeOrderedIndex.put_member(
                     native,
                     due_key,
                     "stale-native-#{i}",
                     1_000
                   )
        end

        assert {:ok, _} =
                 flow_create_and_get(live_id,
                   type: type,
                   partition_key: live_partition,
                   state: "queued",
                   run_at_ms: 1_000
                 )

        assert {:ok, [%{id: ^live_id, partition_key: ^live_partition}]} =
                 FerricStore.flow_claim_due(type,
                   partition_keys: [stale_partition, live_partition],
                   worker: "worker-multi-key-stale-native",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1_000
                 )
      end
    end
  end
end
