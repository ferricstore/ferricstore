defmodule Ferricstore.Flow.InvariantTest do
  use Ferricstore.Test.FlowCase

  describe "claim_due invariants" do
    test "states are parallel by default until policy opts a state into fifo" do
      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-default-parallel-type")
      partition_key = "tenant:default-parallel:#{suffix}"
      first_id = "z-default-parallel-first:#{suffix}"
      second_id = "a-default-parallel-second:#{suffix}"

      for {id, now_ms} <- [{first_id, 1_000}, {second_id, 1_001}] do
        assert :ok =
                 FerricStore.flow_create(id,
                   type: type,
                   state: "queued",
                   partition_key: partition_key,
                   priority: 1,
                   payload: id,
                   now_ms: now_ms,
                   run_at_ms: 2_000
                 )
      end

      assert {:ok, claimed} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 priority: 1,
                 limit: 10,
                 worker: "parallel-worker",
                 now_ms: 2_000
               )

      assert MapSet.new(Enum.map(claimed, & &1.id)) == MapSet.new([first_id, second_id])
    end

    test "fifo state claims one flow per partition lane in state-entry order" do
      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-fifo-type")
      first_id = "z-fifo-first:#{suffix}"
      second_id = "a-fifo-second:#{suffix}"
      partition_key = "tenant:fifo:#{suffix}"

      assert {:ok, %{states: %{"queued" => %{mode: :fifo}}}} =
               FerricStore.flow_policy_set(type, states: %{"queued" => [mode: :fifo]})

      assert :ok =
               FerricStore.flow_create(first_id,
                 type: type,
                 state: "queued",
                 partition_key: partition_key,
                 payload: "first",
                 now_ms: 1_000,
                 run_at_ms: 2_000
               )

      assert :ok =
               FerricStore.flow_create(second_id,
                 type: type,
                 state: "queued",
                 partition_key: partition_key,
                 payload: "second",
                 now_ms: 1_001,
                 run_at_ms: 2_000
               )

      assert {:ok, [first]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 10,
                 worker: "fifo-worker",
                 now_ms: 2_000
               )

      assert first.id == first_id

      assert {:ok, []} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 10,
                 worker: "fifo-worker",
                 now_ms: 2_001
               )

      assert :ok =
               FerricStore.flow_complete(first.id, first.lease_token,
                 result: "done",
                 fencing_token: first.fencing_token,
                 partition_key: partition_key,
                 now_ms: 2_100
               )

      assert {:ok, [second]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 10,
                 worker: "fifo-worker",
                 now_ms: 2_101
               )

      assert second.id == second_id
    end

    test "fifo retry keeps the same partition lane blocked until the retried flow leaves" do
      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-fifo-retry-type")
      first_id = "z-fifo-retry-first:#{suffix}"
      second_id = "a-fifo-retry-second:#{suffix}"
      partition_key = "tenant:fifo-retry:#{suffix}"

      assert {:ok, _policy} =
               FerricStore.flow_policy_set(type,
                 retry: [max_retries: 3, backoff: [kind: :fixed, base_ms: 100, max_ms: 100]],
                 states: %{"queued" => [mode: :fifo]}
               )

      for {id, payload, now_ms} <- [
            {first_id, "first", 1_000},
            {second_id, "second", 1_001}
          ] do
        assert :ok =
                 FerricStore.flow_create(id,
                   type: type,
                   state: "queued",
                   partition_key: partition_key,
                   payload: payload,
                   now_ms: now_ms,
                   run_at_ms: 2_000
                 )
      end

      assert {:ok, [first]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 10,
                 worker: "fifo-worker",
                 now_ms: 2_000
               )

      assert first.id == first_id

      assert :ok =
               FerricStore.flow_retry(first.id, first.lease_token,
                 error: "try again",
                 fencing_token: first.fencing_token,
                 partition_key: partition_key,
                 now_ms: 2_010,
                 run_at_ms: 2_100
               )

      assert {:ok, []} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 10,
                 worker: "fifo-worker",
                 now_ms: 2_050
               )

      assert {:ok, [retried]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 10,
                 worker: "fifo-worker",
                 now_ms: 2_100
               )

      assert retried.id == first_id

      assert :ok =
               FerricStore.flow_complete(retried.id, retried.lease_token,
                 result: "done",
                 fencing_token: retried.fencing_token,
                 partition_key: partition_key,
                 now_ms: 2_200
               )

      assert {:ok, [second]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 10,
                 worker: "fifo-worker",
                 now_ms: 2_201
               )

      assert second.id == second_id
    end

    test "fifo lanes are scoped to type state and partition" do
      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-fifo-states-type")
      partition_key = "tenant:fifo-states:#{suffix}"
      validate_id = "fifo-validate:#{suffix}"
      charge_id = "fifo-charge:#{suffix}"

      assert {:ok, _policy} =
               FerricStore.flow_policy_set(type,
                 states: %{
                   "validate" => [mode: :fifo],
                   "charge" => [mode: :fifo]
                 }
               )

      for {id, state} <- [{validate_id, "validate"}, {charge_id, "charge"}] do
        assert :ok =
                 FerricStore.flow_create(id,
                   type: type,
                   state: state,
                   partition_key: partition_key,
                   payload: state,
                   now_ms: 1_000,
                   run_at_ms: 1_000
                 )
      end

      assert {:ok, [validate]} =
               FerricStore.flow_claim_due(type,
                 state: "validate",
                 partition_key: partition_key,
                 limit: 10,
                 worker: "fifo-worker",
                 now_ms: 1_000
               )

      assert validate.id == validate_id

      assert {:ok, [charge]} =
               FerricStore.flow_claim_due(type,
                 state: "charge",
                 partition_key: partition_key,
                 limit: 10,
                 worker: "fifo-worker",
                 now_ms: 1_001
               )

      assert charge.id == charge_id
    end

    test "fifo any-partition claims at most one flow per partition lane" do
      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-fifo-any-type")
      partition_a = "tenant:fifo-any-a:#{suffix}"
      partition_b = "tenant:fifo-any-b:#{suffix}"
      first_a = "z-fifo-any-a-first:#{suffix}"
      second_a = "a-fifo-any-a-second:#{suffix}"
      first_b = "z-fifo-any-b-first:#{suffix}"
      second_b = "a-fifo-any-b-second:#{suffix}"

      put_fifo_policy!(type)

      for {id, partition_key, now_ms} <- [
            {first_a, partition_a, 1_000},
            {second_a, partition_a, 1_001},
            {first_b, partition_b, 1_002},
            {second_b, partition_b, 1_003}
          ] do
        create_fifo_flow!(id, type, partition_key, now_ms: now_ms, run_at_ms: 2_000)
      end

      assert {:ok, claimed} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: :any,
                 limit: 10,
                 worker: "fifo-worker",
                 now_ms: 2_000
               )

      assert MapSet.new(Enum.map(claimed, & &1.id)) == MapSet.new([first_a, first_b])

      assert {:ok, []} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: :any,
                 limit: 10,
                 worker: "fifo-worker",
                 now_ms: 2_001
               )

      partition_by_id = %{first_a => partition_a, first_b => partition_b}

      for record <- claimed do
        assert :ok =
                 FerricStore.flow_complete(record.id, record.lease_token,
                   result: "done",
                   fencing_token: record.fencing_token,
                   partition_key: Map.fetch!(partition_by_id, record.id),
                   now_ms: 2_100
                 )
      end

      assert {:ok, claimed_after_complete} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: :any,
                 limit: 10,
                 worker: "fifo-worker",
                 now_ms: 2_101
               )

      assert MapSet.new(Enum.map(claimed_after_complete, & &1.id)) ==
               MapSet.new([second_a, second_b])
    end

    test "fifo partition list claims at most one flow per listed partition lane" do
      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-fifo-partition-list-type")
      partition_a = "tenant:fifo-list-a:#{suffix}"
      partition_b = "tenant:fifo-list-b:#{suffix}"
      partition_c = "tenant:fifo-list-c:#{suffix}"
      first_a = "z-fifo-list-a-first:#{suffix}"
      second_a = "a-fifo-list-a-second:#{suffix}"
      first_b = "z-fifo-list-b-first:#{suffix}"
      second_b = "a-fifo-list-b-second:#{suffix}"
      ignored_c = "z-fifo-list-c-ignored:#{suffix}"

      put_fifo_policy!(type)

      for {id, partition_key, now_ms} <- [
            {first_a, partition_a, 1_000},
            {second_a, partition_a, 1_001},
            {first_b, partition_b, 1_002},
            {second_b, partition_b, 1_003},
            {ignored_c, partition_c, 1_004}
          ] do
        create_fifo_flow!(id, type, partition_key, now_ms: now_ms, run_at_ms: 2_000)
      end

      assert {:ok, claimed} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_keys: [partition_a, partition_b],
                 limit: 10,
                 worker: "fifo-worker",
                 now_ms: 2_000
               )

      assert MapSet.new(Enum.map(claimed, & &1.id)) == MapSet.new([first_a, first_b])

      assert {:ok, []} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_keys: [partition_a, partition_b],
                 limit: 10,
                 worker: "fifo-worker",
                 now_ms: 2_001
               )

      assert {:ok, [ignored]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_keys: [partition_c],
                 limit: 10,
                 worker: "fifo-worker",
                 now_ms: 2_001
               )

      assert ignored.id == ignored_c
    end

    test "fifo pipeline claim batch does not overclaim a single partition lane" do
      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-fifo-pipeline-type")
      partition_key = "tenant:fifo-pipeline:#{suffix}"
      first_id = "z-fifo-pipeline-first:#{suffix}"
      second_id = "a-fifo-pipeline-second:#{suffix}"
      ctx = FerricStore.Instance.get(:default)

      put_fifo_policy!(type)
      create_fifo_flow!(first_id, type, partition_key, now_ms: 1_000, run_at_ms: 2_000)
      create_fifo_flow!(second_id, type, partition_key, now_ms: 1_001, run_at_ms: 2_000)

      assert [
               {:ok, [%{id: ^first_id}]},
               {:ok, []}
             ] =
               Ferricstore.Flow.pipeline_claim_due_batch(ctx, [
                 {:claim_due, type,
                  [
                    state: "queued",
                    worker: "fifo-worker-a",
                    partition_key: partition_key,
                    limit: 1,
                    now_ms: 2_000
                  ]},
                 {:claim_due, type,
                  [
                    state: "queued",
                    worker: "fifo-worker-b",
                    partition_key: partition_key,
                    limit: 1,
                    now_ms: 2_000
                  ]}
               ])

      assert {:ok, []} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 10,
                 worker: "fifo-worker",
                 now_ms: 2_001
               )
    end

    test "fifo reclaim picks the expired lane head instead of the queued follower" do
      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-fifo-reclaim-type")
      partition_key = "tenant:fifo-reclaim:#{suffix}"
      first_id = "z-fifo-reclaim-first:#{suffix}"
      second_id = "a-fifo-reclaim-second:#{suffix}"

      put_fifo_policy!(type)
      create_fifo_flow!(first_id, type, partition_key, now_ms: 1_000, run_at_ms: 2_000)
      create_fifo_flow!(second_id, type, partition_key, now_ms: 1_001, run_at_ms: 2_000)

      assert {:ok, [%{id: ^first_id}]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 1,
                 lease_ms: 50,
                 worker: "old-worker",
                 now_ms: 2_000
               )

      assert {:ok, []} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 10,
                 worker: "fifo-worker",
                 now_ms: 2_010
               )

      assert {:ok, [reclaimed]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 10,
                 lease_ms: 50,
                 worker: "new-worker",
                 now_ms: 2_100
               )

      assert reclaimed.id == first_id
      assert reclaimed.lease_owner == "new-worker"

      assert {:ok, []} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 10,
                 worker: "fifo-worker",
                 now_ms: 2_101
               )
    end

    test "fifo limit one reclaims an expired lane head before its queued follower" do
      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-fifo-limit-one-reclaim-type")
      partition_key = "tenant:fifo-limit-one-reclaim:#{suffix}"
      first_id = "z-fifo-limit-one-reclaim-first:#{suffix}"
      second_id = "a-fifo-limit-one-reclaim-second:#{suffix}"

      put_fifo_policy!(type)
      create_fifo_flow!(first_id, type, partition_key, now_ms: 1_000, run_at_ms: 2_000)
      create_fifo_flow!(second_id, type, partition_key, now_ms: 1_001, run_at_ms: 2_000)

      assert {:ok, [%{id: ^first_id}]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 1,
                 lease_ms: 50,
                 worker: "old-worker",
                 now_ms: 2_000
               )

      assert {:ok, [reclaimed]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 1,
                 lease_ms: 50,
                 worker: "new-worker",
                 now_ms: 2_100
               )

      assert reclaimed.id == first_id
      assert reclaimed.lease_owner == "new-worker"

      assert {:ok, []} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 1,
                 worker: "follower-worker",
                 now_ms: 2_101
               )
    end

    test "fifo keeps a cold lane head ahead of a hot due follower" do
      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-fifo-cold-head-type")
      partition_key = "tenant:fifo-cold-head:#{suffix}"
      first_id = "z-fifo-cold-head-first:#{suffix}"
      second_id = "a-fifo-cold-head-second:#{suffix}"
      ctx = FerricStore.Instance.get(:default)

      put_fifo_policy!(type)
      create_fifo_flow!(first_id, type, partition_key, now_ms: 1_000, run_at_ms: 1_000_000)
      create_fifo_flow!(second_id, type, partition_key, now_ms: 1_001, run_at_ms: 2_000)

      state_key = Ferricstore.Flow.Keys.state_key(first_id, partition_key)
      shard_index = Ferricstore.Store.Router.shard_for(ctx, state_key)

      {index_table, lookup_table} =
        Ferricstore.Flow.OrderedIndex.table_names(ctx.name, shard_index)

      assert :ok =
               Ferricstore.Flow.OrderedIndex.delete_member(
                 index_table,
                 lookup_table,
                 Ferricstore.Flow.Keys.state_index_key(type, "queued", partition_key),
                 first_id
               )

      assert {:ok, []} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 1,
                 worker: "fifo-worker",
                 now_ms: 2_000
               )
    end

    test "fifo claims a due lane head beyond the bounded follower scan window" do
      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-fifo-deep-head-type")
      partition_key = "tenant:fifo-deep-head:#{suffix}"
      head_id = "fifo-deep-head:#{suffix}"

      followers =
        Enum.map(1..80, fn index ->
          %{id: "fifo-deep-follower-#{index}:#{suffix}", run_at_ms: 1_000}
        end)

      put_fifo_policy!(type)

      assert :ok =
               FerricStore.flow_create_many(
                 partition_key,
                 [%{id: head_id, run_at_ms: 2_000} | followers],
                 type: type,
                 state: "queued",
                 now_ms: 500
               )

      assert {:ok, %{next_run_at_ms: 2_000}} =
               FerricStore.flow_get(head_id, partition_key: partition_key)

      assert {:ok, %{next_run_at_ms: 1_000}} =
               FerricStore.flow_get(hd(followers).id, partition_key: partition_key)

      assert {:ok, [%{id: ^head_id}]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 1,
                 worker: "fifo-worker",
                 now_ms: 2_000
               )
    end

    test "fifo preserves global due order when a deep lane head is injected" do
      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-fifo-deep-order-type")
      partition_key = "tenant:fifo-deep-order:#{suffix}"
      later_head_id = "fifo-deep-order-later:#{suffix}"
      earlier_head_id = "fifo-deep-order-earlier:#{suffix}"

      followers =
        Enum.map(1..80, fn index ->
          %{id: "fifo-deep-order-follower-#{index}:#{suffix}", run_at_ms: 1_000}
        end)

      put_fifo_policy!(type, ["first", "second"])

      assert :ok =
               FerricStore.flow_create_many(
                 partition_key,
                 [%{id: later_head_id, run_at_ms: 2_000} | followers],
                 type: type,
                 state: "first",
                 now_ms: 500
               )

      assert :ok =
               FerricStore.flow_create(earlier_head_id,
                 type: type,
                 state: "second",
                 partition_key: partition_key,
                 now_ms: 600,
                 run_at_ms: 1_500
               )

      assert {:ok, [%{id: ^earlier_head_id}]} =
               FerricStore.flow_claim_due(type,
                 state: :any,
                 partition_key: partition_key,
                 limit: 1,
                 worker: "fifo-worker",
                 now_ms: 2_000
               )
    end

    test "fifo times out an expired lane head injected beyond the bounded scan window" do
      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-fifo-deep-timeout-type")
      partition_key = "tenant:fifo-deep-timeout:#{suffix}"
      head_id = "fifo-deep-timeout-head:#{suffix}"

      followers =
        Enum.map(1..80, fn index ->
          %{id: "fifo-deep-timeout-follower-#{index}:#{suffix}", run_at_ms: 1_000}
        end)

      put_fifo_policy!(type)

      assert :ok =
               FerricStore.flow_create(head_id,
                 type: type,
                 state: "queued",
                 partition_key: partition_key,
                 max_active_ms: 100,
                 now_ms: 500,
                 run_at_ms: 2_000
               )

      assert :ok =
               FerricStore.flow_create_many(partition_key, followers,
                 type: type,
                 state: "queued",
                 now_ms: 600
               )

      assert {:ok, []} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 1,
                 worker: "fifo-worker",
                 now_ms: 2_000
               )

      assert {:ok, %{state: "failed", error: %{reason: "max_active_ms"}}} =
               FerricStore.flow_get(head_id,
                 partition_key: partition_key,
                 full: true
               )

      first_follower_id = hd(followers).id

      assert {:ok, [%{id: ^first_follower_id}]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 1,
                 worker: "fifo-worker",
                 now_ms: 2_001
               )
    end

    test "fifo times out an expired follower while a live lane head blocks claims" do
      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-fifo-follower-timeout-type")
      partition_key = "tenant:fifo-follower-timeout:#{suffix}"
      head_id = "fifo-follower-timeout-head:#{suffix}"
      follower_id = "fifo-follower-timeout-follower:#{suffix}"

      put_fifo_policy!(type)

      create_fifo_flow!(head_id, type, partition_key, now_ms: 500, run_at_ms: 3_000)

      assert :ok =
               FerricStore.flow_create(follower_id,
                 type: type,
                 state: "queued",
                 partition_key: partition_key,
                 max_active_ms: 100,
                 now_ms: 600,
                 run_at_ms: 1_000
               )

      assert {:ok, []} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 1,
                 worker: "fifo-worker",
                 now_ms: 2_000
               )

      assert {:ok, %{state: "queued"}} =
               FerricStore.flow_get(head_id, partition_key: partition_key)

      assert {:ok, %{state: "failed", error: %{reason: "max_active_ms"}}} =
               FerricStore.flow_get(follower_id,
                 partition_key: partition_key,
                 full: true
               )
    end

    test "fifo unlinks a cancelled middle entry without disturbing lane order" do
      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-fifo-middle-cancel-type")
      partition_key = "tenant:fifo-middle-cancel:#{suffix}"
      first_id = "fifo-middle-cancel-first:#{suffix}"
      second_id = "fifo-middle-cancel-second:#{suffix}"
      third_id = "fifo-middle-cancel-third:#{suffix}"

      put_fifo_policy!(type)

      for {id, now_ms} <- [{first_id, 1_000}, {second_id, 1_001}, {third_id, 1_002}] do
        create_fifo_flow!(id, type, partition_key, now_ms: now_ms, run_at_ms: 2_000)
      end

      assert :ok =
               FerricStore.flow_cancel(second_id,
                 partition_key: partition_key,
                 fencing_token: 0,
                 now_ms: 1_500
               )

      assert {:ok, [first]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 1,
                 worker: "fifo-worker",
                 now_ms: 2_000
               )

      assert first.id == first_id

      assert :ok =
               FerricStore.flow_complete(first.id, first.lease_token,
                 fencing_token: first.fencing_token,
                 partition_key: partition_key,
                 now_ms: 2_100
               )

      assert {:ok, [%{id: ^third_id}]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 1,
                 worker: "fifo-worker",
                 now_ms: 2_101
               )
    end

    test "enabling fifo on a populated state uses its existing state-entry order" do
      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-fifo-activation-type")
      partition_key = "tenant:fifo-activation:#{suffix}"
      first_id = "z-fifo-activation-first:#{suffix}"
      second_id = "a-fifo-activation-second:#{suffix}"

      create_fifo_flow!(first_id, type, partition_key, now_ms: 1_000, run_at_ms: 2_000)
      create_fifo_flow!(second_id, type, partition_key, now_ms: 1_001, run_at_ms: 2_000)
      put_fifo_policy!(type)

      assert {:ok, [%{id: ^first_id}]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 10,
                 worker: "fifo-worker",
                 now_ms: 2_000
               )

      assert {:ok, []} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 10,
                 worker: "fifo-worker",
                 now_ms: 2_001
               )
    end

    test "enabling fifo waits for already leased lane members to drain" do
      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-fifo-activation-drain-type")
      partition_key = "tenant:fifo-activation-drain:#{suffix}"
      first_id = "fifo-activation-drain-first:#{suffix}"
      second_id = "fifo-activation-drain-second:#{suffix}"

      create_fifo_flow!(first_id, type, partition_key, now_ms: 1_000, run_at_ms: 5_000)
      create_fifo_flow!(second_id, type, partition_key, now_ms: 1_001, run_at_ms: 2_000)

      assert {:ok, [running]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 1,
                 worker: "parallel-worker",
                 now_ms: 2_000
               )

      assert running.id == second_id
      put_fifo_policy!(type)

      assert {:ok, []} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 1,
                 worker: "fifo-worker",
                 now_ms: 5_000
               )

      assert :ok =
               FerricStore.flow_complete(running.id, running.lease_token,
                 fencing_token: running.fencing_token,
                 partition_key: partition_key,
                 now_ms: 5_100
               )

      assert {:ok, [%{id: ^first_id}]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 1,
                 worker: "fifo-worker",
                 now_ms: 5_101
               )
    end

    test "step_continue records a fresh logical state entry order before fifo activation" do
      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-fifo-step-entry-type")
      partition_key = "tenant:fifo-step-entry:#{suffix}"
      transitioning_id = "fifo-step-entry-transitioning:#{suffix}"
      waiting_id = "fifo-step-entry-waiting:#{suffix}"

      assert :ok =
               FerricStore.flow_create(transitioning_id,
                 type: type,
                 state: "reserve",
                 partition_key: partition_key,
                 now_ms: 1_000,
                 run_at_ms: 2_000
               )

      assert :ok =
               FerricStore.flow_create(waiting_id,
                 type: type,
                 state: "charge",
                 partition_key: partition_key,
                 now_ms: 1_001,
                 run_at_ms: 2_200
               )

      assert {:ok, [claimed]} =
               FerricStore.flow_claim_due(type,
                 state: "reserve",
                 partition_key: partition_key,
                 limit: 1,
                 worker: "parallel-worker",
                 now_ms: 2_000
               )

      assert {:ok, continued} =
               FerricStore.flow_step_continue(
                 transitioning_id,
                 claimed.lease_token,
                 "reserve",
                 "charge",
                 partition_key: partition_key,
                 fencing_token: claimed.fencing_token,
                 worker: "parallel-worker",
                 now_ms: 2_100
               )

      assert :ok =
               FerricStore.flow_retry(transitioning_id, continued.lease_token,
                 partition_key: partition_key,
                 fencing_token: continued.fencing_token,
                 run_at_ms: 2_200,
                 now_ms: 2_101,
                 error: %{reason: "retry"}
               )

      put_fifo_policy!(type, ["charge"])

      assert {:ok, [%{id: ^waiting_id}]} =
               FerricStore.flow_claim_due(type,
                 state: "charge",
                 partition_key: partition_key,
                 limit: 1,
                 worker: "fifo-worker",
                 now_ms: 2_200
               )
    end

    test "fifo lane unblocks when the running flow transitions out of the fifo state" do
      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-fifo-transition-unblock-type")
      partition_key = "tenant:fifo-transition-unblock:#{suffix}"
      first_id = "z-fifo-transition-first:#{suffix}"
      second_id = "a-fifo-transition-second:#{suffix}"

      put_fifo_policy!(type, ["queued"])
      create_fifo_flow!(first_id, type, partition_key, now_ms: 1_000, run_at_ms: 2_000)
      create_fifo_flow!(second_id, type, partition_key, now_ms: 1_001, run_at_ms: 2_000)

      assert {:ok, [first]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 1,
                 worker: "fifo-worker",
                 now_ms: 2_000
               )

      assert :ok =
               FerricStore.flow_transition(first.id, "running", "waiting",
                 lease_token: first.lease_token,
                 fencing_token: first.fencing_token,
                 partition_key: partition_key,
                 run_at_ms: 5_000,
                 now_ms: 2_100
               )

      assert {:ok, [second]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 10,
                 worker: "fifo-worker",
                 now_ms: 2_101
               )

      assert second.id == second_id
    end

    test "fifo state-entry order survives restart when ids and timestamps would sort differently" do
      isolated = ShardHelpers.setup_isolated_data_dir()

      on_exit(fn ->
        ShardHelpers.teardown_isolated_data_dir(isolated)
      end)

      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-fifo-restart-type")
      partition_key = "tenant:fifo-restart:#{suffix}"
      first_id = "z-fifo-restart-first:#{suffix}"
      second_id = "a-fifo-restart-second:#{suffix}"

      put_fifo_policy!(type)
      create_fifo_flow!(first_id, type, partition_key, now_ms: 1_000, run_at_ms: 2_000)
      create_fifo_flow!(second_id, type, partition_key, now_ms: 1_000, run_at_ms: 2_000)

      :ok = ShardHelpers.restart_current_data_dir(isolated)

      assert {:ok, [first]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 10,
                 worker: "fifo-worker",
                 now_ms: 2_000
               )

      assert first.id == first_id
    end

    test "fifo state rejects priority and implicit partition entry" do
      type = unique_flow_id("flow-invariant-fifo-entry-type")
      partition_key = unique_flow_id("tenant:fifo-entry")

      assert {:ok, _policy} =
               FerricStore.flow_policy_set(type,
                 states: %{
                   "queued" => [mode: :fifo],
                   "ready" => [mode: :fifo]
                 }
               )

      assert {:error, "ERR flow partition_key is required for fifo state"} =
               FerricStore.flow_create(unique_flow_id("fifo-no-partition"),
                 type: type,
                 state: "queued",
                 payload: "payload",
                 now_ms: 1_000,
                 run_at_ms: 1_000
               )

      assert {:error, "ERR flow priority is not supported for fifo state"} =
               FerricStore.flow_create(unique_flow_id("fifo-priority"),
                 type: type,
                 state: "queued",
                 partition_key: partition_key,
                 priority: 1,
                 payload: "payload",
                 now_ms: 1_000,
                 run_at_ms: 1_000
               )

      id = unique_flow_id("fifo-transition")

      assert :ok =
               FerricStore.flow_create(id,
                 type: type,
                 state: "queued",
                 partition_key: partition_key,
                 payload: "payload",
                 now_ms: 1_000,
                 run_at_ms: 1_000
               )

      assert {:ok, [claimed]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 1,
                 worker: "fifo-worker",
                 now_ms: 1_000
               )

      assert {:error, "ERR flow priority is not supported for fifo state"} =
               FerricStore.flow_transition(claimed.id, "running", "ready",
                 lease_token: claimed.lease_token,
                 fencing_token: claimed.fencing_token,
                 partition_key: partition_key,
                 priority: 1,
                 now_ms: 1_100,
                 run_at_ms: 1_100
               )
    end

    test "fifo state rejects direct running entry" do
      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-fifo-direct-type")
      partition_key = "tenant:fifo-direct:#{suffix}"
      id = "fifo-direct:#{suffix}"

      assert {:ok, _policy} =
               FerricStore.flow_policy_set(type,
                 states: %{
                   "queued" => [mode: :parallel],
                   "charge" => [mode: :fifo]
                 }
               )

      assert {:error, "ERR flow direct running entry is not supported for fifo state"} =
               FerricStore.flow_start_and_claim(id, type, "charge",
                 partition_key: partition_key,
                 worker: "fifo-worker",
                 now_ms: 1_000
               )

      assert :ok =
               FerricStore.flow_create(id,
                 type: type,
                 state: "queued",
                 partition_key: partition_key,
                 payload: "payload",
                 now_ms: 1_000,
                 run_at_ms: 1_000
               )

      assert {:ok, [claimed]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 1,
                 worker: "fifo-worker",
                 now_ms: 1_000
               )

      assert {:error, "ERR flow direct running entry is not supported for fifo state"} =
               FerricStore.flow_step_continue(id, claimed.lease_token, "queued", "charge",
                 partition_key: partition_key,
                 fencing_token: claimed.fencing_token,
                 worker: "fifo-worker",
                 now_ms: 1_100
               )
    end

    test "fifo planning preserves due order for parallel states in mixed claims" do
      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-fifo-mixed-type")
      partition_key = "tenant:fifo-mixed:#{suffix}"
      later_due_id = "parallel-later-due:#{suffix}"
      earlier_due_id = "parallel-earlier-due:#{suffix}"

      assert {:ok, _policy} =
               FerricStore.flow_policy_set(type,
                 states: %{
                   "fifo_gate" => [mode: :fifo],
                   "retry" => [mode: :parallel]
                 }
               )

      assert :ok =
               FerricStore.flow_create(later_due_id,
                 type: type,
                 state: "retry",
                 partition_key: partition_key,
                 payload: "later due",
                 now_ms: 1_000,
                 run_at_ms: 10_000
               )

      assert :ok =
               FerricStore.flow_create(earlier_due_id,
                 type: type,
                 state: "retry",
                 partition_key: partition_key,
                 payload: "earlier due",
                 now_ms: 1_001,
                 run_at_ms: 2_000
               )

      assert {:ok, [claimed]} =
               FerricStore.flow_claim_due(type,
                 states: ["fifo_gate", "retry"],
                 partition_key: partition_key,
                 limit: 1,
                 worker: "fifo-worker",
                 now_ms: 10_000
               )

      assert claimed.id == earlier_due_id
    end

    test "public record returns hide fifo state entry sequence" do
      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-fifo-public-type")
      partition_key = "tenant:fifo-public:#{suffix}"
      start_id = "public-start:#{suffix}"
      step_id = "public-step:#{suffix}"

      assert {:ok, started} =
               FerricStore.flow_start_and_claim(start_id, type, "reserve",
                 partition_key: partition_key,
                 worker: "fifo-worker",
                 now_ms: 1_000
               )

      refute Map.has_key?(started, :state_enter_seq)

      assert :ok =
               FerricStore.flow_create(step_id,
                 type: type,
                 state: "queued",
                 partition_key: partition_key,
                 payload: "payload",
                 now_ms: 1_000,
                 run_at_ms: 1_000
               )

      assert {:ok, [claimed]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 partition_key: partition_key,
                 limit: 1,
                 worker: "fifo-worker",
                 now_ms: 1_000
               )

      assert {:ok, continued} =
               FerricStore.flow_step_continue(step_id, claimed.lease_token, "queued", "charge",
                 partition_key: partition_key,
                 fencing_token: claimed.fencing_token,
                 worker: "fifo-worker",
                 now_ms: 1_100
               )

      refute Map.has_key?(continued, :state_enter_seq)

      ctx = FerricStore.Instance.get(:default)

      assert [{:ok, pipeline_record}] =
               Ferricstore.Flow.pipeline_write_batch_independent(ctx, [
                 {:start_and_claim, "public-pipeline:#{suffix}", type, "reserve",
                  [partition_key: partition_key, worker: "fifo-worker", now_ms: 1_200]}
               ])

      refute Map.has_key?(pipeline_record, :state_enter_seq)
    end

    test "pipeline get hides fifo state entry sequence" do
      suffix = System.unique_integer([:positive])
      type = unique_flow_id("flow-invariant-fifo-pipeline-read-type")
      partition_key = "tenant:fifo-pipeline-read:#{suffix}"
      id = "pipeline-read:#{suffix}"

      assert :ok =
               FerricStore.flow_create(id,
                 type: type,
                 state: "queued",
                 partition_key: partition_key,
                 payload: "payload",
                 now_ms: 1_000,
                 run_at_ms: 1_000
               )

      ctx = FerricStore.Instance.get(:default)

      assert [{:ok, record}] =
               Ferricstore.Flow.pipeline_read_batch(ctx, [
                 {:get, id, [partition_key: partition_key]}
               ])

      refute Map.has_key?(record, :state_enter_seq)
    end

    test "spawn_children rejects reserved child group ids" do
      suffix = System.unique_integer([:positive])

      assert {:error, "ERR flow group_id is reserved"} =
               FerricStore.flow_spawn_children(
                 "parent-reserved-group:#{suffix}",
                 [%{id: "child-reserved-group:#{suffix}", type: "child"}],
                 partition_key: "tenant:reserved-group:#{suffix}",
                 group_id: "__state_enter_seq__",
                 wait: :none,
                 fencing_token: 0,
                 now_ms: 1_000
               )
    end

    test "leased and terminal flows are not returned by later claim_due calls" do
      id = unique_flow_id("flow-invariant-leased-terminal")
      type = unique_flow_id("flow-invariant-type")

      assert :ok =
               FerricStore.flow_create(id,
                 type: type,
                 state: "queued",
                 payload: "payload",
                 now_ms: 1_000,
                 run_at_ms: 1_000
               )

      assert {:ok, [claimed]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 limit: 1,
                 worker: "invariant-worker",
                 now_ms: 1_000
               )

      assert {:ok, []} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 limit: 10,
                 worker: "invariant-worker",
                 now_ms: 1_001
               )

      assert :ok =
               FerricStore.flow_complete(id, claimed.lease_token,
                 result: "done",
                 fencing_token: claimed.fencing_token,
                 now_ms: 1_100
               )

      assert {:ok, []} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 limit: 10,
                 worker: "invariant-worker",
                 now_ms: 2_000
               )
    end

    test "future due work is invisible until its due timestamp" do
      id = unique_flow_id("flow-invariant-future-due")
      type = unique_flow_id("flow-invariant-type")

      assert :ok =
               FerricStore.flow_create(id,
                 type: type,
                 state: "queued",
                 payload: "payload",
                 now_ms: 1_000,
                 run_at_ms: 10_000
               )

      assert {:ok, []} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 limit: 10,
                 worker: "invariant-worker",
                 now_ms: 9_999
               )

      assert {:ok, [claimed]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 limit: 10,
                 worker: "invariant-worker",
                 now_ms: 10_000
               )

      assert claimed.id == id
    end
  end

  describe "terminal/history invariants" do
    test "complete writes terminal state and keeps history queryable" do
      id = unique_flow_id("flow-invariant-complete")
      type = unique_flow_id("flow-invariant-type")

      assert :ok =
               FerricStore.flow_create(id,
                 type: type,
                 state: "queued",
                 payload: "payload",
                 now_ms: 1_000,
                 run_at_ms: 1_000
               )

      assert {:ok, [claimed]} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 limit: 1,
                 worker: "invariant-worker",
                 now_ms: 1_000
               )

      assert :ok =
               FerricStore.flow_complete(id, claimed.lease_token,
                 result: "done",
                 fencing_token: claimed.fencing_token,
                 now_ms: 1_100
               )

      assert {:ok, record} = FerricStore.flow_get(id)
      assert record.state == "completed"

      assert {:ok, history} = FerricStore.flow_history(id, count: 10)

      assert Enum.any?(history, fn {_event_id, fields} ->
               event = Map.get(fields, :event) || Map.get(fields, "event")
               state = Map.get(fields, :state) || Map.get(fields, "state")
               to_state = Map.get(fields, :to_state) || Map.get(fields, "to_state")

               event in ["complete", "completed", :complete, :completed] or
                 state == "completed" or to_state == "completed"
             end)
    end
  end

  describe "value ref invariants" do
    test "stored value refs are readable through mget without duplicating command flow state" do
      assert {:ok, ref} = FerricStore.flow_value_put("shared-doc", now_ms: 1_000)
      assert {:ok, ["shared-doc"]} = FerricStore.flow_value_mget([ref.ref])
    end
  end

  defp put_fifo_policy!(type, states \\ ["queued"]) do
    state_policies = Map.new(states, &{&1, [mode: :fifo]})

    assert {:ok, _policy} = FerricStore.flow_policy_set(type, states: state_policies)
    :ok
  end

  defp create_fifo_flow!(id, type, partition_key, opts) do
    assert :ok =
             FerricStore.flow_create(
               id,
               [
                 type: type,
                 state: "queued",
                 partition_key: partition_key,
                 payload: id
               ] ++ opts
             )

    :ok
  end
end
