defmodule Ferricstore.FlowTest.Sections.ClaimDueMultiStateClaimsEarliestDueJobInsteadFirstListedState do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Test.ShardHelpers

      test "claim_due multi-state claims earliest due job instead of first listed state" do
        type = uid("claim-multi-state-time-order")
        partition_key = uid("tenant")
        queued_id = uid("claim-multi-state-time-order-queued")
        retry_id = uid("claim-multi-state-time-order-retry")

        assert {:ok, %{id: ^queued_id, next_run_at_ms: 100}} =
                 flow_create_and_get(queued_id,
                   type: type,
                   state: "queued",
                   partition_key: partition_key,
                   now_ms: 1,
                   run_at_ms: 100
                 )

        assert {:ok, %{id: ^retry_id, next_run_at_ms: 10}} =
                 flow_create_and_get(retry_id,
                   type: type,
                   state: "retry",
                   partition_key: partition_key,
                   now_ms: 1,
                   run_at_ms: 10
                 )

        assert {:ok, [[^retry_id, ^partition_key, lease_token, fencing_token, "retry"]]} =
                 FerricStore.flow_claim_due(type,
                   states: ["queued", "retry"],
                   worker: "worker-multi-state-time-order",
                   partition_key: partition_key,
                   limit: 1,
                   now_ms: 101,
                   return: :jobs_compact_state
                 )

        assert is_binary(lease_token)
        assert is_integer(fencing_token)
      end

      test "pipeline_claim_due_batch groups interleaved independent partitions" do
        ctx = FerricStore.Instance.get(:default)
        type = uid("pipeline-claim-partitions")
        partition_a = uid("tenant-a")
        partition_b = uid("tenant-b")

        ids =
          for {partition_key, idx} <- [
                {partition_a, 1},
                {partition_b, 2},
                {partition_a, 3},
                {partition_b, 4}
              ] do
            id = uid("pipeline-claim-partition-#{idx}")

            assert {:ok, %{id: ^id}} =
                     flow_create_and_get(id,
                       type: type,
                       partition_key: partition_key,
                       now_ms: 1,
                       run_at_ms: 1
                     )

            id
          end

        attach_flow_telemetry([[:ferricstore, :flow, :pipeline_claim_due_batch]])

        results =
          Ferricstore.Flow.pipeline_claim_due_batch(ctx, [
            {:claim_due, type,
             [worker: "worker-a", partition_key: partition_a, limit: 1, now_ms: 2]},
            {:claim_due, type,
             [worker: "worker-a", partition_key: partition_b, limit: 1, now_ms: 2]},
            {:claim_due, type,
             [worker: "worker-a", partition_key: partition_a, limit: 1, now_ms: 2]},
            {:claim_due, type,
             [worker: "worker-a", partition_key: partition_b, limit: 1, now_ms: 2]}
          ])

        assert Enum.all?(results, fn {:ok, records} -> length(records) == 1 end)

        claimed_ids =
          results
          |> Enum.flat_map(fn {:ok, [record]} -> [record.id] end)
          |> MapSet.new()

        assert claimed_ids == MapSet.new(ids)

        assert_receive {:flow_telemetry, [:ferricstore, :flow, :pipeline_claim_due_batch],
                        %{commands: 4, groups: 2, coalesced_calls: 2}, %{source: :resp_pipeline}}
      end

      test "pipeline_claim_due_batch batches distinct partition claims without overclaiming" do
        ctx = FerricStore.Instance.get(:default)
        type = uid("pipeline-claim-distinct")

        partitions = Enum.map(1..4, &uid("tenant-distinct-#{&1}"))

        expected_first_ids =
          partitions
          |> Enum.with_index(1)
          |> Enum.map(fn {partition_key, idx} ->
            first_id = uid("pipeline-claim-distinct-#{idx}-first")
            second_id = uid("pipeline-claim-distinct-#{idx}-second")

            assert {:ok, %{id: ^first_id}} =
                     flow_create_and_get(first_id,
                       type: type,
                       partition_key: partition_key,
                       now_ms: 1,
                       run_at_ms: 1
                     )

            assert {:ok, %{id: ^second_id}} =
                     flow_create_and_get(second_id,
                       type: type,
                       partition_key: partition_key,
                       now_ms: 1,
                       run_at_ms: 1
                     )

            {partition_key, first_id, second_id}
          end)

        attach_flow_telemetry([[:ferricstore, :flow, :pipeline_claim_due_batch]])

        results =
          Ferricstore.Flow.pipeline_claim_due_batch(
            ctx,
            Enum.map(partitions, fn partition_key ->
              {:claim_due, type,
               [worker: "worker-distinct", partition_key: partition_key, limit: 1, now_ms: 2]}
            end)
          )

        assert Enum.all?(results, fn {:ok, records} -> length(records) == 1 end)

        claimed_by_partition =
          Map.new(results, fn {:ok, [record]} -> {record.partition_key, record.id} end)

        Enum.each(expected_first_ids, fn {partition_key, first_id, second_id} ->
          assert Map.fetch!(claimed_by_partition, partition_key) in [first_id, second_id]

          assert {:ok, [remaining]} =
                   FerricStore.flow_claim_due(type,
                     worker: "worker-distinct-followup",
                     partition_key: partition_key,
                     limit: 10,
                     now_ms: 3
                   )

          assert remaining.id in [first_id, second_id]
          refute remaining.id == Map.fetch!(claimed_by_partition, partition_key)
        end)

        assert_receive {:flow_telemetry, [:ferricstore, :flow, :pipeline_claim_due_batch],
                        %{commands: 4, groups: 1, coalesced_calls: 0, batched_calls: 1},
                        %{source: :resp_pipeline}}
      end

      test "pipeline_claim_due_batch singleton partition list claims across shards" do
        ctx = FerricStore.Instance.get(:default)
        type = uid("pipeline-claim-cross-shard-list")
        {partition_a, partition_b} = due_partition_keys_on_different_shards(type)
        id_a = uid("pipeline-claim-cross-shard-a")
        id_b = uid("pipeline-claim-cross-shard-b")

        assert {:ok, %{id: ^id_a}} =
                 flow_create_and_get(id_a,
                   type: type,
                   partition_key: partition_a,
                   now_ms: 1,
                   run_at_ms: 1
                 )

        assert {:ok, %{id: ^id_b}} =
                 flow_create_and_get(id_b,
                   type: type,
                   partition_key: partition_b,
                   now_ms: 1,
                   run_at_ms: 1
                 )

        assert [{:ok, claimed}] =
                 Ferricstore.Flow.pipeline_claim_due_batch(ctx, [
                   {:claim_due, type,
                    [
                      worker: "worker-cross-shard-list",
                      partition_keys: [partition_a, partition_b],
                      limit: 2,
                      now_ms: 2
                    ]}
                 ])

        assert MapSet.new(Enum.map(claimed, & &1.id)) == MapSet.new([id_a, id_b])
        assert Enum.all?(claimed, &(&1.partition_key in [partition_a, partition_b]))
      end

      test "pipeline_claim_due_batch singleton auto partition scans across shards" do
        ctx = FerricStore.Instance.get(:default)
        type = uid("pipeline-claim-auto-cross-shard")
        {id_a, id_b} = auto_ids_on_different_due_shards(type)

        assert {:ok, %{id: ^id_a, partition_key: partition_a}} =
                 flow_create_and_get(id_a,
                   type: type,
                   now_ms: 1,
                   run_at_ms: 1
                 )

        assert {:ok, %{id: ^id_b, partition_key: partition_b}} =
                 flow_create_and_get(id_b,
                   type: type,
                   now_ms: 1,
                   run_at_ms: 1
                 )

        assert Ferricstore.Flow.Keys.auto_partition_key?(partition_a)
        assert Ferricstore.Flow.Keys.auto_partition_key?(partition_b)

        assert shard_for(Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition_a)) !=
                 shard_for(Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition_b))

        assert [{:ok, claimed}] =
                 Ferricstore.Flow.pipeline_claim_due_batch(ctx, [
                   {:claim_due, type,
                    [
                      worker: "worker-auto-cross-shard",
                      limit: 2,
                      now_ms: 2
                    ]}
                 ])

        assert MapSet.new(Enum.map(claimed, & &1.id)) == MapSet.new([id_a, id_b])
      end

      test "pipeline_claim_due_batch singleton any partition scans across shards" do
        ctx = FerricStore.Instance.get(:default)
        type = uid("pipeline-claim-any-cross-shard")
        {partition_a, partition_b} = due_partition_keys_on_different_shards(type)
        id_a = uid("pipeline-claim-any-a")
        id_b = uid("pipeline-claim-any-b")

        assert {:ok, %{id: ^id_a}} =
                 flow_create_and_get(id_a,
                   type: type,
                   partition_key: partition_a,
                   now_ms: 1,
                   run_at_ms: 1
                 )

        assert {:ok, %{id: ^id_b}} =
                 flow_create_and_get(id_b,
                   type: type,
                   partition_key: partition_b,
                   now_ms: 1,
                   run_at_ms: 1
                 )

        assert [{:ok, claimed}] =
                 Ferricstore.Flow.pipeline_claim_due_batch(ctx, [
                   {:claim_due, type,
                    [
                      worker: "worker-any-cross-shard",
                      partition_key: :any,
                      limit: 2,
                      now_ms: 2
                    ]}
                 ])

        assert MapSet.new(Enum.map(claimed, & &1.id)) == MapSet.new([id_a, id_b])
      end

      test "pipeline_claim_due_batch accepts omitted NOW option" do
        ctx = FerricStore.Instance.get(:default)
        type = uid("pipeline-claim-no-now")
        partition_key = uid("tenant")
        id = uid("pipeline-claim-no-now-id")
        now = System.system_time(:millisecond)

        assert {:ok, %{id: ^id}} =
                 flow_create_and_get(id,
                   type: type,
                   partition_key: partition_key,
                   now_ms: now,
                   run_at_ms: now
                 )

        assert [{:ok, [%{id: ^id, lease_owner: "worker-a"}]}] =
                 Ferricstore.Flow.pipeline_claim_due_batch(ctx, [
                   {:claim_due, type,
                    [worker: "worker-a", partition_key: partition_key, limit: 1]}
                 ])
      end

      test "flow_claim_due BLOCK waits without polling and wakes after due create" do
        type = uid("claim-block")
        partition_key = uid("tenant")
        id = uid("blocked-job")

        task =
          Task.async(fn ->
            FerricStore.flow_claim_due(type,
              worker: "worker-a",
              partition_key: partition_key,
              limit: 1,
              block_ms: 1_000,
              payload: false
            )
          end)

        ShardHelpers.eventually(
          fn -> Ferricstore.Flow.ClaimWaiters.total_count() > 0 end,
          "claim_due waiter registered",
          100,
          5
        )

        refute Task.yield(task, 0)

        assert :ok =
                 FerricStore.flow_create(id,
                   type: type,
                   partition_key: partition_key,
                   state: "queued",
                   now_ms: 1_000,
                   run_at_ms: 1_000
                 )

        assert {:ok, [record]} = Task.await(task, 1_000)
        assert record.id == id
        assert record.partition_key == partition_key
        assert Ferricstore.Flow.ClaimWaiters.total_count() == 0
      end

      test "flow_claim_due BLOCK re-registers after a spurious wake loses the claim" do
        type = uid("claim-block-reregister")
        partition_key = uid("tenant")
        id = uid("blocked-reregister-job")

        task =
          Task.async(fn ->
            FerricStore.flow_claim_due(type,
              worker: "worker-a",
              partition_key: partition_key,
              limit: 1,
              block_ms: 3_000,
              payload: false
            )
          end)

        ShardHelpers.eventually(
          fn -> Ferricstore.Flow.ClaimWaiters.total_count() > 0 end,
          "claim_due waiter registered",
          100,
          5
        )

        assert 1 = Ferricstore.Flow.ClaimWaiters.notify_ready(type, "queued", 0, partition_key, 1)

        ShardHelpers.eventually(
          fn -> Ferricstore.Flow.ClaimWaiters.total_count() > 0 end,
          "claim_due waiter re-registered after empty wake",
          200,
          10
        )

        assert :ok =
                 FerricStore.flow_create(id,
                   type: type,
                   partition_key: partition_key,
                   state: "queued",
                   now_ms: 1_000,
                   run_at_ms: 1_000
                 )

        assert {:ok, [record]} = Task.await(task, 1_000)
        assert record.id == id
        assert Ferricstore.Flow.ClaimWaiters.total_count() == 0
      end

      test "flow_claim_due BLOCK wakes when due create uses default state" do
        type = uid("claim-block-default")
        partition_key = uid("tenant")
        id = uid("blocked-default-job")

        task =
          Task.async(fn ->
            FerricStore.flow_claim_due(type,
              state: "queued",
              worker: "worker-a",
              partition_key: partition_key,
              priority: 0,
              limit: 1,
              block_ms: 500,
              payload: false
            )
          end)

        ShardHelpers.eventually(
          fn -> Ferricstore.Flow.ClaimWaiters.total_count() > 0 end,
          "claim_due default-state waiter registered",
          100,
          5
        )

        assert :ok =
                 FerricStore.flow_create(id,
                   type: type,
                   partition_key: partition_key,
                   now_ms: 1_000,
                   run_at_ms: 1_000
                 )

        assert {:ok, [record]} = Task.await(task, 1_000)
        assert record.id == id
        assert record.state == "running"
        assert Ferricstore.Flow.ClaimWaiters.total_count() == 0
      end

      test "flow_claim_due BLOCK uses claim limit as waiter credit capacity" do
        type = uid("claim-block-credit")
        partition_key = uid("tenant")

        tasks =
          for idx <- 1..3 do
            Task.async(fn ->
              FerricStore.flow_claim_due(type,
                worker: "worker-credit-#{idx}",
                partition_key: partition_key,
                limit: 10,
                block_ms: 1_000,
                return: :jobs_compact
              )
            end)
          end

        ShardHelpers.eventually(
          fn -> Ferricstore.Flow.ClaimWaiters.total_count() >= 3 end,
          "claim_due credit waiters registered",
          100,
          5
        )

        ids = Enum.map(1..5, &uid("blocked-credit-job-#{&1}"))

        assert :ok =
                 FerricStore.flow_create_many(
                   partition_key,
                   ids,
                   type: type,
                   now_ms: 1_000,
                   run_at_ms: 1_000
                 )

        claimed =
          Enum.reduce_while(1..50, nil, fn _attempt, _acc ->
            case Enum.find_value(tasks, &yield_non_empty_claim/1) do
              nil ->
                Process.sleep(20)
                {:cont, nil}

              claimed ->
                {:halt, claimed}
            end
          end)

        assert [_ | _] = claimed
        assert Enum.sort(Enum.map(claimed, &hd/1)) == Enum.sort(ids)
        assert Ferricstore.Flow.ClaimWaiters.total_count() == 2

        Enum.each(tasks, &Task.shutdown(&1, :brutal_kill))
      end

      test "flow_claim_due BLOCK times out with an empty claim" do
        started = System.monotonic_time(:millisecond)

        assert {:ok, []} =
                 FerricStore.flow_claim_due(uid("claim-block-timeout"),
                   worker: "worker-a",
                   partition_key: uid("tenant"),
                   limit: 1,
                   block_ms: 25,
                   payload: false
                 )

        assert System.monotonic_time(:millisecond) - started >= 20
        assert Ferricstore.Flow.ClaimWaiters.total_count() == 0
      end

      test "flow_claim_due BLOCK is not consumed by retry policy backoff wake" do
        type = uid("claim-block-retry-backoff")
        partition_key = uid("tenant")
        id = uid("retry-backoff-job")
        now = System.system_time(:millisecond)

        assert {:ok, _record} =
                 flow_create_and_get(id,
                   type: type,
                   partition_key: partition_key,
                   state: "queued",
                   run_at_ms: now,
                   now_ms: now
                 )

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due(type,
                   state: "queued",
                   worker: "worker-a",
                   partition_key: partition_key,
                   limit: 1,
                   lease_ms: 30_000,
                   now_ms: now
                 )

        task =
          Task.async(fn ->
            FerricStore.flow_claim_due(type,
              state: "queued",
              worker: "worker-b",
              partition_key: partition_key,
              limit: 1,
              block_ms: 750,
              payload: false
            )
          end)

        ShardHelpers.eventually(
          fn -> Ferricstore.Flow.ClaimWaiters.total_count() > 0 end,
          "retry backoff claim_due waiter registered",
          100,
          5
        )

        retry_now = System.system_time(:millisecond)

        assert {:ok, retried} =
                 flow_retry_and_get(claimed.id, claimed.lease_token,
                   partition_key: partition_key,
                   fencing_token: claimed.fencing_token,
                   now_ms: retry_now,
                   retry: [
                     max_retries: 3,
                     backoff: [kind: :fixed, base_ms: 80, max_ms: 80, jitter_pct: 0]
                   ]
                 )

        assert retried.next_run_at_ms >= retry_now + 80

        assert {:ok, [reclaimed]} = Task.await(task, 1_000)
        assert reclaimed.id == id
        assert reclaimed.lease_owner == "worker-b"
        assert Ferricstore.Flow.ClaimWaiters.total_count() == 0
      end

      test "flow_claim_due BLOCK wakes for retry scheduled before waiter registers" do
        type = uid("claim-block-retry-late-waiter")
        partition_key = uid("tenant")
        id = uid("retry-late-waiter-job")
        now = System.system_time(:millisecond)

        assert {:ok, _record} =
                 flow_create_and_get(id,
                   type: type,
                   partition_key: partition_key,
                   state: "queued",
                   run_at_ms: now,
                   now_ms: now
                 )

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due(type,
                   state: "queued",
                   worker: "worker-a",
                   partition_key: partition_key,
                   limit: 1,
                   lease_ms: 30_000,
                   now_ms: now
                 )

        retry_now = System.system_time(:millisecond)

        assert {:ok, retried} =
                 flow_retry_and_get(claimed.id, claimed.lease_token,
                   partition_key: partition_key,
                   fencing_token: claimed.fencing_token,
                   now_ms: retry_now,
                   retry: [
                     max_retries: 3,
                     backoff: [kind: :fixed, base_ms: 150, max_ms: 150, jitter_pct: 0]
                   ]
                 )

        assert retried.next_run_at_ms >= retry_now + 150

        task =
          Task.async(fn ->
            FerricStore.flow_claim_due(type,
              state: "queued",
              worker: "worker-b",
              partition_key: partition_key,
              limit: 1,
              block_ms: 900,
              payload: false
            )
          end)

        Process.sleep(450)

        assert {:ok, [reclaimed]} = Task.await(task, 1_000)
        assert reclaimed.id == id
        assert reclaimed.lease_owner == "worker-b"
        assert Ferricstore.Flow.ClaimWaiters.total_count() == 0
      end

      test "pipeline_claim_due_batch preserves sequential reclaim preference" do
        ctx = FerricStore.Instance.get(:default)
        type = uid("pipeline-claim-reclaim")
        partition_key = uid("tenant")
        expired_a = uid("pipeline-claim-expired-a")
        expired_b = uid("pipeline-claim-expired-b")
        queued = uid("pipeline-claim-queued")

        for id <- [expired_a, expired_b, queued] do
          assert {:ok, %{id: ^id}} =
                   flow_create_and_get(id,
                     type: type,
                     partition_key: partition_key,
                     now_ms: 1_000,
                     run_at_ms: 1_000
                   )
        end

        assert {:ok, [%{id: ^expired_a}]} =
                 FerricStore.flow_claim_due(type,
                   worker: "old-worker",
                   partition_key: partition_key,
                   lease_ms: 50,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert {:ok, [%{id: ^expired_b}]} =
                 FerricStore.flow_claim_due(type,
                   worker: "old-worker",
                   partition_key: partition_key,
                   lease_ms: 50,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert [
                 {:ok, [%{id: first}]},
                 {:ok, [%{id: second}]}
               ] =
                 Ferricstore.Flow.pipeline_claim_due_batch(ctx, [
                   {:claim_due, type,
                    [worker: "worker-a", partition_key: partition_key, limit: 1, now_ms: 1_100]},
                   {:claim_due, type,
                    [worker: "worker-a", partition_key: partition_key, limit: 1, now_ms: 1_100]}
                 ])

        assert MapSet.new([first, second]) == MapSet.new([expired_a, expired_b])

        assert {:ok, [%{id: ^queued}]} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-b",
                   partition_key: partition_key,
                   now_ms: 1_100
                 )
      end

      test "pipeline_read_batch hydrates Flow GET payloads with per-command caps" do
        ctx = FerricStore.Instance.get(:default)
        id_a = uid("flow-pipeline-payload-a")
        id_b = uid("flow-pipeline-payload-b")
        payload_a = "payload-a"
        payload_b = "payload-b"
        payload_a_size = encoded_value_size(payload_a)
        payload_b_size = encoded_value_size(payload_b)

        assert {:ok, _flow} =
                 flow_create_and_get(id_a,
                   type: "pipeline-payload",
                   payload: payload_a,
                   run_at_ms: 1
                 )

        assert {:ok, _flow} =
                 flow_create_and_get(id_b,
                   type: "pipeline-payload",
                   payload: payload_b,
                   run_at_ms: 1
                 )

        assert [
                 {:ok, %{id: ^id_a, payload: ^payload_a, payload_size: ^payload_a_size}},
                 {:ok, %{id: ^id_b, payload_omitted: true, payload_size: ^payload_b_size}},
                 {:ok, no_payload}
               ] =
                 Ferricstore.Flow.pipeline_read_batch(ctx, [
                   {:get, id_a, [full: true]},
                   {:get, id_b, [full: true, payload_max_bytes: 4]},
                   {:get, id_a, [payload: false]}
                 ])

        refute Map.has_key?(no_payload, :payload)
        refute Map.has_key?(no_payload, :payload_omitted)
      end

      test "flow_create idempotent retry returns matching existing record and rejects conflicts" do
        id = uid("flow-create-idempotent")

        assert {:ok, created} =
                 flow_create_and_get(id,
                   type: "checkout",
                   state: "queued",
                   payload: "payload:" <> id,
                   run_at_ms: 1_000,
                   now_ms: 10,
                   idempotent: true
                 )

        assert {:ok, retried} =
                 flow_create_and_get(id,
                   type: "checkout",
                   state: "queued",
                   payload: "payload:" <> id,
                   run_at_ms: 1_000,
                   now_ms: 20,
                   idempotent: true
                 )

        assert retried.id == created.id
        assert retried.version == created.version
        assert retried.created_at_ms == created.created_at_ms

        assert {:ok, history} = FerricStore.flow_history(id)
        assert Enum.map(history, fn {_event_id, fields} -> fields["event"] end) == ["created"]

        assert {:error, "ERR flow idempotency conflict"} =
                 flow_create_and_get(id,
                   type: "checkout",
                   state: "queued",
                   payload: "different:" <> id,
                   run_at_ms: 1_000,
                   idempotent: true
                 )
      end

      test "flow_create idempotent retry matches large blob-backed payloads" do
        id = uid("flow-create-idempotent-blob")
        payload = :binary.copy("blob-payload", 24_000)

        assert byte_size(payload) >
                 FerricStore.Instance.get(:default).blob_side_channel_threshold_bytes

        assert {:ok, created} =
                 flow_create_and_get(id,
                   type: "checkout",
                   state: "queued",
                   payload: payload,
                   run_at_ms: 1_000,
                   now_ms: 10,
                   idempotent: true
                 )

        assert {:ok, retried} =
                 flow_create_and_get(id,
                   type: "checkout",
                   state: "queued",
                   payload: payload,
                   run_at_ms: 1_000,
                   now_ms: 20,
                   idempotent: true
                 )

        assert retried.id == created.id
        assert retried.version == created.version
        assert retried.created_at_ms == created.created_at_ms

        assert {:ok, hydrated} =
                 FerricStore.flow_get(id, full: true, payload_max_bytes: byte_size(payload))

        assert hydrated.payload == payload

        assert {:error, "ERR flow idempotency conflict"} =
                 flow_create_and_get(id,
                   type: "checkout",
                   state: "queued",
                   payload: payload <> "different",
                   run_at_ms: 1_000,
                   idempotent: true
                 )
      end

      test "flow due index stays derived and does not persist per-flow zset members" do
        id = uid("flow-due-derived")
        run_at_ms = 1_234

        assert {:ok, _flow} =
                 flow_create_and_get(id,
                   type: "checkout",
                   state: "queued",
                   run_at_ms: run_at_ms
                 )

        due_key = Ferricstore.Flow.Keys.due_key("checkout", "queued", 0, nil)
        member_key = Ferricstore.Store.CompoundKey.zset_member(due_key, id)
        type_key = Ferricstore.Store.CompoundKey.type_key(due_key)

        assert {:ok, nil} = FerricStore.get(member_key)
        assert {:ok, nil} = FerricStore.get(type_key)

        assert {:ok, [%{id: ^id}]} =
                 FerricStore.flow_claim_due("checkout",
                   state: "queued",
                   worker: "worker-a",
                   now_ms: run_at_ms,
                   lease_ms: 1_000,
                   limit: 10
                 )
      end
    end
  end
end
