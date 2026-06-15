defmodule Ferricstore.FlowTest.Sections.FlowClaimDueAutomaticallyReclaimsExpiredLeasesRatio do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Test.ShardHelpers

      test "flow_claim_due automatically reclaims expired leases by ratio" do
        type = uid("lease-ratio")
        expired_ids = Enum.map(1..4, &uid("flow-expired-#{&1}"))
        fresh_ids = Enum.map(1..4, &uid("flow-fresh-#{&1}"))

        for id <- expired_ids do
          assert {:ok, _} = flow_create_and_get(id, type: type, run_at_ms: 1_000)
        end

        assert {:ok, expired_first_claim} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-a",
                   lease_ms: 50,
                   limit: 4,
                   now_ms: 1_000
                 )

        assert MapSet.new(Enum.map(expired_first_claim, & &1.id)) == MapSet.new(expired_ids)

        for id <- fresh_ids do
          assert {:ok, _} = flow_create_and_get(id, type: type, run_at_ms: 1_050)
        end

        assert {:ok, claimed} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-b",
                   lease_ms: 50,
                   limit: 4,
                   now_ms: 1_050,
                   reclaim_ratio: 50
                 )

        reclaimed = Enum.filter(claimed, &(&1.version == 3))
        fresh = Enum.filter(claimed, &(&1.version == 2))

        assert length(reclaimed) == 2
        assert length(fresh) == 2
        assert Enum.all?(reclaimed, &(&1.id in expired_ids))
        assert Enum.all?(fresh, &(&1.id in fresh_ids))
      end

      test "flow_claim_due can disable automatic expired lease reclaim" do
        type = uid("lease-no-auto-reclaim")
        expired_id = uid("flow-expired")
        fresh_id = uid("flow-fresh")

        assert {:ok, _} = flow_create_and_get(expired_id, type: type, run_at_ms: 1_000)

        assert {:ok, [_]} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-a",
                   lease_ms: 50,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert {:ok, _} = flow_create_and_get(fresh_id, type: type, run_at_ms: 1_050)

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-b",
                   lease_ms: 50,
                   limit: 1,
                   now_ms: 1_050,
                   reclaim_expired: false
                 )

        assert claimed.id == fresh_id
        assert claimed.version == 2
      end

      test "flow_claim_due limit one prefers ready queued work before expired lease reclaim" do
        type = uid("lease-limit-one-normal-first")
        expired_id = uid("flow-expired")
        fresh_id = uid("flow-fresh")

        assert {:ok, _} = flow_create_and_get(expired_id, type: type, run_at_ms: 1_000)

        assert {:ok, [_]} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-a",
                   lease_ms: 50,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert {:ok, _} = flow_create_and_get(fresh_id, type: type, run_at_ms: 1_050)

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-b",
                   lease_ms: 50,
                   limit: 1,
                   now_ms: 1_050
                 )

        assert claimed.id == fresh_id
        assert claimed.version == 2

        assert {:ok, [reclaimed]} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-c",
                   lease_ms: 50,
                   limit: 1,
                   now_ms: 1_050
                 )

        assert reclaimed.id == expired_id
        assert reclaimed.version == 3
      end

      test "expired running lease reclaim is partition scoped" do
        partition_a = uid("tenant-reclaim-a")
        partition_b = uid("tenant-reclaim-b")
        type = uid("lease-partition")
        id_a = uid("flow-reclaim-a")
        id_b = uid("flow-reclaim-b")

        assert {:ok, _} =
                 flow_create_and_get(id_a,
                   type: type,
                   state: "queued",
                   partition_key: partition_a,
                   run_at_ms: 1_000
                 )

        assert {:ok, _} =
                 flow_create_and_get(id_b,
                   type: type,
                   state: "queued",
                   partition_key: partition_b,
                   run_at_ms: 1_000
                 )

        assert {:ok, [first_a]} =
                 FerricStore.flow_claim_due(type,
                   state: "queued",
                   partition_key: partition_a,
                   worker: "worker-a",
                   lease_ms: 50,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert {:ok, [first_b]} =
                 FerricStore.flow_claim_due(type,
                   state: "queued",
                   partition_key: partition_b,
                   worker: "worker-a",
                   lease_ms: 50,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert {:ok, [second_a]} =
                 FerricStore.flow_claim_due(type,
                   state: "running",
                   partition_key: partition_a,
                   worker: "worker-b",
                   lease_ms: 50,
                   limit: 10,
                   now_ms: 1_050
                 )

        assert second_a.id == id_a
        assert second_a.lease_owner == "worker-b"
        assert second_a.lease_token != first_a.lease_token

        assert {:ok, fetched_b} = FerricStore.flow_get(id_b, partition_key: partition_b)
        assert fetched_b.lease_owner == "worker-a"
        assert fetched_b.lease_token == first_b.lease_token

        assert {:ok, []} =
                 FerricStore.flow_stuck(type,
                   partition_key: partition_a,
                   older_than_ms: 0,
                   count: 10,
                   now_ms: 1_050
                 )

        assert {:ok, [stuck_b]} =
                 FerricStore.flow_stuck(type,
                   partition_key: partition_b,
                   older_than_ms: 0,
                   count: 10,
                   now_ms: 1_050
                 )

        assert stuck_b.id == id_b
      end

      test "flow_transition atomically moves state, due index, and history" do
        id = uid("flow-transition")

        assert {:ok, _} =
                 flow_create_and_get(id,
                   type: "checkout",
                   state: "payment_pending",
                   run_at_ms: 1_000,
                   now_ms: 900
                 )

        assert {:ok, transitioned} =
                 flow_transition_and_get(id, "payment_pending", "email_pending",
                   fencing_token: 0,
                   run_at_ms: 2_000,
                   now_ms: 1_100
                 )

        assert transitioned.state == "email_pending"
        assert transitioned.next_run_at_ms == 2_000
        assert transitioned.version == 2

        assert {:ok, []} =
                 FerricStore.flow_claim_due("checkout",
                   state: "payment_pending",
                   worker: "worker-a",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 2_000
                 )

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due("checkout",
                   state: "email_pending",
                   worker: "worker-a",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 2_000
                 )

        assert claimed.id == id

        assert {:ok, events} = FerricStore.flow_history(id)

        assert Enum.map(events, fn {_id, fields} -> fields["event"] end) == [
                 "created",
                 "transitioned",
                 "claimed"
               ]
      end

      test "flow_transition rejects direct moves into running state" do
        id = uid("flow-transition-running")

        assert {:ok, _} =
                 flow_create_and_get(id,
                   type: "checkout",
                   state: "queued",
                   run_at_ms: 1_000,
                   now_ms: 900
                 )

        assert {:error, "ERR flow running state is only entered by FLOW.CLAIM_DUE"} =
                 flow_transition_and_get(id, "queued", "running",
                   fencing_token: 0,
                   now_ms: 1_000
                 )

        assert {:ok, record} = FerricStore.flow_get(id)
        assert record.state == "queued"
      end

      test "flow_transition_many atomically moves one-partition batch" do
        partition = uid("tenant-transition")
        type = uid("bulk-transition")
        id_a = uid("transition-a")
        id_b = uid("transition-b")

        assert {:ok, _} =
                 flow_create_many_and_get(
                   partition,
                   [%{id: id_a}, %{id: id_b}],
                   type: type,
                   state: "queued",
                   run_at_ms: 1_000,
                   now_ms: 900
                 )

        assert {:ok, transitioned} =
                 flow_transition_many_and_get(
                   partition,
                   "queued",
                   "ready",
                   [
                     %{id: id_a, fencing_token: 0},
                     %{id: id_b, fencing_token: 0}
                   ],
                   run_at_ms: 2_000,
                   now_ms: 1_100
                 )

        assert Enum.map(transitioned, & &1.id) == [id_a, id_b]
        assert Enum.all?(transitioned, &(&1.state == "ready"))
        assert Enum.all?(transitioned, &(&1.partition_key == partition))

        assert {:ok, []} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition,
                   state: "queued",
                   worker: "worker-a",
                   limit: 10,
                   now_ms: 2_000
                 )

        assert {:ok, claimed} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition,
                   state: "ready",
                   worker: "worker-a",
                   limit: 10,
                   now_ms: 2_000
                 )

        assert claimed |> Enum.map(& &1.id) |> MapSet.new() == MapSet.new([id_a, id_b])
      end

      test "flow_transition_many rejects direct moves into running state" do
        partition = uid("tenant-transition-running")
        type = uid("bulk-transition-running")
        id_a = uid("transition-running-a")
        id_b = uid("transition-running-b")

        assert {:ok, _} =
                 flow_create_many_and_get(
                   partition,
                   [%{id: id_a}, %{id: id_b}],
                   type: type,
                   state: "queued",
                   run_at_ms: 1_000,
                   now_ms: 900
                 )

        assert {:error, "ERR flow running state is only entered by FLOW.CLAIM_DUE"} =
                 flow_transition_many_and_get(
                   partition,
                   "queued",
                   "running",
                   [
                     %{id: id_a, fencing_token: 0},
                     %{id: id_b, fencing_token: 0}
                   ],
                   now_ms: 1_000
                 )

        assert {:ok, record_a} = FerricStore.flow_get(id_a, partition_key: partition)
        assert {:ok, record_b} = FerricStore.flow_get(id_b, partition_key: partition)
        assert record_a.state == "queued"
        assert record_b.state == "queued"
      end

      test "flow_transition_many shared payload keeps per-flow refs and values" do
        partition = uid("tenant-transition-shared-payload")
        type = uid("bulk-transition-shared-payload")
        id_a = uid("transition-shared-payload-a")
        id_b = uid("transition-shared-payload-b")
        payload = %{"step" => "ready", "bytes" => String.duplicate("x", 256)}

        assert {:ok, _} =
                 flow_create_many_and_get(
                   partition,
                   [%{id: id_a}, %{id: id_b}],
                   type: type,
                   state: "queued",
                   run_at_ms: 1_000,
                   now_ms: 900
                 )

        assert {:ok, transitioned} =
                 flow_transition_many_and_get(
                   partition,
                   "queued",
                   "ready",
                   [
                     %{id: id_a, fencing_token: 0},
                     %{id: id_b, fencing_token: 0}
                   ],
                   run_at_ms: 2_000,
                   now_ms: 1_100,
                   payload: payload
                 )

        assert transitioned |> Enum.map(& &1.payload_ref) |> Enum.uniq() |> length() == 2

        assert {:ok, hydrated_a} =
                 FerricStore.flow_get(id_a, partition_key: partition, full: true)

        assert {:ok, hydrated_b} =
                 FerricStore.flow_get(id_b, partition_key: partition, full: true)

        assert hydrated_a.payload == payload
        assert hydrated_b.payload == payload

        assert {:ok, claimed} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition,
                   state: "ready",
                   worker: "worker-shared-payload",
                   limit: 10,
                   now_ms: 2_000
                 )

        assert length(claimed) == 2
        assert Enum.all?(claimed, &(&1.payload == payload))
      end

      test "flow_transition_many shared payload is encoded once in the replicated command" do
        partition = uid("tenant-transition-shared-command")
        type = uid("bulk-transition-shared-command")
        payload = String.duplicate("x", 4_096)

        items =
          for idx <- 1..40 do
            %{id: uid("transition-shared-command-#{idx}")}
          end

        assert {:ok, _} =
                 flow_create_many_and_get(
                   partition,
                   items,
                   type: type,
                   state: "queued",
                   run_at_ms: 1_000,
                   now_ms: 900
                 )

        attach_flow_telemetry([[:ferricstore, :waraft, :segment_log, :append]])

        assert {:ok, transitioned} =
                 flow_transition_many_and_get(
                   partition,
                   "queued",
                   "ready",
                   Enum.map(items, &Map.put(&1, :fencing_token, 0)),
                   run_at_ms: 2_000,
                   now_ms: 1_100,
                   payload: payload
                 )

        assert length(transitioned) == length(items)

        raft_log_bytes = receive_segment_append_bytes(:raft_log, 0)
        assert raft_log_bytes < byte_size(payload) * 20
      end

      test "independent flow_transition_many shared payload is encoded once per shard" do
        partition = uid("tenant-transition-independent-shared-command")
        type = uid("bulk-transition-independent-shared-command")
        payload = String.duplicate("x", 4_096)

        items =
          for idx <- 1..40 do
            %{id: uid("transition-independent-shared-command-#{idx}")}
          end

        assert {:ok, _} =
                 flow_create_many_and_get(
                   partition,
                   items,
                   type: type,
                   state: "queued",
                   run_at_ms: 1_000,
                   now_ms: 900
                 )

        attach_flow_telemetry([[:ferricstore, :waraft, :segment_log, :append]])

        assert {:ok, transitioned} =
                 flow_transition_many_and_get(
                   partition,
                   "queued",
                   "ready",
                   Enum.map(items, &Map.put(&1, :fencing_token, 0)),
                   run_at_ms: 2_000,
                   now_ms: 1_100,
                   payload: payload,
                   independent: true
                 )

        assert length(transitioned) == length(items)
        assert Enum.all?(transitioned, &match?(%{state: "ready"}, &1))

        raft_log_bytes = receive_segment_append_bytes(:raft_log, 0)
        assert raft_log_bytes < byte_size(payload) * 20
      end

      test "flow_transition_many rolls back when any item fails guard" do
        partition = uid("tenant-transition-rollback")
        type = uid("bulk-transition-rollback")
        id_a = uid("transition-good")
        id_b = uid("transition-bad")

        assert {:ok, _} =
                 flow_create_many_and_get(
                   partition,
                   [%{id: id_a}, %{id: id_b}],
                   type: type,
                   state: "queued",
                   run_at_ms: 1_000
                 )

        assert {:error, "ERR stale flow lease"} =
                 flow_transition_many_and_get(
                   partition,
                   "queued",
                   "ready",
                   [
                     %{id: id_a, fencing_token: 0},
                     %{id: id_b, fencing_token: 1}
                   ],
                   run_at_ms: 2_000
                 )

        assert {:ok, fetched_a} = FerricStore.flow_get(id_a, partition_key: partition)
        assert {:ok, fetched_b} = FerricStore.flow_get(id_b, partition_key: partition)
        assert fetched_a.state == "queued"
        assert fetched_b.state == "queued"
        assert fetched_a.version == 1
        assert fetched_b.version == 1

        assert {:ok, history_a} = FerricStore.flow_history(id_a, partition_key: partition)
        assert {:ok, history_b} = FerricStore.flow_history(id_b, partition_key: partition)
        assert Enum.map(history_a, fn {_id, fields} -> fields["event"] end) == ["created"]
        assert Enum.map(history_b, fn {_id, fields} -> fields["event"] end) == ["created"]

        assert {:ok, claimed} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition,
                   state: "queued",
                   worker: "worker-a",
                   limit: 10,
                   now_ms: 1_000
                 )

        assert claimed |> Enum.map(& &1.id) |> MapSet.new() == MapSet.new([id_a, id_b])
      end

      test "flow_transition_many independent keeps successful items when one item fails" do
        partition = uid("tenant-transition-many-independent")
        type = uid("bulk-transition-independent")
        id_a = uid("transition-independent-bad")
        id_b = uid("transition-independent-good")

        assert {:ok, _} =
                 flow_create_many_and_get(
                   partition,
                   [%{id: id_a}, %{id: id_b}],
                   type: type,
                   state: "queued",
                   run_at_ms: 1_000
                 )

        assert {:ok, [{:error, "ERR stale flow lease"}, :ok]} =
                 FerricStore.flow_transition_many(
                   partition,
                   "queued",
                   "ready",
                   [
                     %{id: id_a, fencing_token: 1},
                     %{id: id_b, fencing_token: 0}
                   ],
                   run_at_ms: 2_000,
                   independent: true
                 )

        assert {:ok, %{state: "queued"}} = FerricStore.flow_get(id_a, partition_key: partition)
        assert {:ok, %{state: "ready"}} = FerricStore.flow_get(id_b, partition_key: partition)
      end

      test "flow transition pipeline batch preserves duplicate key ordering" do
        partition = uid("tenant-transition-pipeline-duplicate")
        type = uid("pipeline-duplicate")
        id = uid("transition-pipeline-duplicate")
        ctx = FerricStore.Instance.get(:default)

        assert {:ok, %{state: "queued"}} =
                 flow_create_and_get(id,
                   type: type,
                   state: "queued",
                   partition_key: partition,
                   run_at_ms: 1_000
                 )

        assert [:ok, :ok] =
                 Ferricstore.Flow.transition_batch_independent(ctx, [
                   {id, "queued", "ready", [partition_key: partition, fencing_token: 0]},
                   {id, "ready", "processing", [partition_key: partition, fencing_token: 0]}
                 ])

        assert {:ok, %{state: "processing", version: 3}} =
                 FerricStore.flow_get(id, partition_key: partition)
      end

      test "flow_transition_many spans shards and rolls back failing shard group" do
        {same_a, same_b, other} = mixed_partition_keys()
        type = uid("bulk-mixed-transition")
        bad_id = uid("transition-mixed-bad")
        same_id = uid("transition-mixed-same")
        other_id = uid("transition-mixed-other")

        for {id, partition} <- [{bad_id, same_a}, {same_id, same_b}, {other_id, other}] do
          assert {:ok, _} =
                   flow_create_and_get(id,
                     type: type,
                     partition_key: partition,
                     state: "queued",
                     run_at_ms: 1_000
                   )
        end

        assert {:ok, results} =
                 flow_transition_many_and_get(
                   nil,
                   "queued",
                   "ready",
                   [
                     %{id: bad_id, partition_key: same_a, fencing_token: 1},
                     %{id: same_id, partition_key: same_b, fencing_token: 0},
                     %{id: other_id, partition_key: other, fencing_token: 0}
                   ],
                   run_at_ms: 2_000,
                   now_ms: 1_100
                 )

        assert [
                 {:error, "ERR stale flow lease"},
                 {:error, "ERR stale flow lease"},
                 %{id: ^other_id, partition_key: ^other, state: "ready"}
               ] = results

        assert {:ok, %{state: "queued"}} = FerricStore.flow_get(bad_id, partition_key: same_a)
        assert {:ok, %{state: "queued"}} = FerricStore.flow_get(same_id, partition_key: same_b)
        assert {:ok, %{state: "ready"}} = FerricStore.flow_get(other_id, partition_key: other)

        assert {:ok, bad_history} = FerricStore.flow_history(bad_id, partition_key: same_a)
        assert {:ok, same_history} = FerricStore.flow_history(same_id, partition_key: same_b)
        assert {:ok, other_history} = FerricStore.flow_history(other_id, partition_key: other)

        assert Enum.map(bad_history, fn {_id, fields} -> fields["event"] end) == ["created"]
        assert Enum.map(same_history, fn {_id, fields} -> fields["event"] end) == ["created"]

        assert Enum.map(other_history, fn {_id, fields} -> fields["event"] end) == [
                 "created",
                 "transitioned"
               ]
      end

      test "flow many commands route auto-partition records by their real state key" do
        type = uid("bulk-auto-many")
        id_a = uid("auto-many-a")
        id_b = uid("auto-many-b")

        assert {:ok, created} =
                 flow_create_many_and_get(
                   nil,
                   [%{id: id_a}, %{id: id_b}],
                   type: type,
                   state: "queued",
                   run_at_ms: 1_000,
                   now_ms: 900
                 )

        assert Enum.map(created, & &1.id) == [id_a, id_b]
        assert Enum.all?(created, &Ferricstore.Flow.Keys.auto_partition_key?(&1.partition_key))

        assert {:ok, transitioned} =
                 flow_transition_many_and_get(
                   nil,
                   "queued",
                   "ready",
                   [
                     %{id: id_a, fencing_token: 0},
                     %{id: id_b, fencing_token: 0}
                   ],
                   run_at_ms: 2_000,
                   now_ms: 1_100
                 )

        assert Enum.map(transitioned, & &1.id) == [id_a, id_b]
        assert Enum.all?(transitioned, &(&1.state == "ready"))

        assert {:ok, %{state: "ready"}} = FerricStore.flow_get(id_a)
        assert {:ok, %{state: "ready"}} = FerricStore.flow_get(id_b)

        assert {:ok, claimed} =
                 FerricStore.flow_claim_due(type,
                   state: "ready",
                   partition_key: :auto,
                   worker: "worker-auto-many-complete",
                   limit: 2,
                   now_ms: 2_000
                 )

        complete_items =
          claimed
          |> Enum.sort_by(& &1.id)
          |> Enum.map(fn record ->
            %{id: record.id, lease_token: record.lease_token, fencing_token: record.fencing_token}
          end)

        assert {:ok, completed} =
                 flow_complete_many_and_get(nil, complete_items,
                   result: "auto-many-result",
                   now_ms: 3_000
                 )

        assert Enum.map(completed, & &1.id) == [id_a, id_b]
        assert Enum.all?(completed, &(&1.state == "completed"))

        retry_a = uid("auto-many-retry-a")
        retry_b = uid("auto-many-retry-b")

        assert {:ok, _created} =
                 flow_create_many_and_get(
                   nil,
                   [%{id: retry_a}, %{id: retry_b}],
                   type: type,
                   state: "queued",
                   run_at_ms: 4_000,
                   now_ms: 3_500
                 )

        assert {:ok, retry_claimed} =
                 FerricStore.flow_claim_due(type,
                   state: "queued",
                   partition_key: :auto,
                   worker: "worker-auto-many-retry",
                   limit: 2,
                   now_ms: 4_000
                 )

        retry_items =
          retry_claimed
          |> Enum.sort_by(& &1.id)
          |> Enum.map(fn record ->
            %{id: record.id, lease_token: record.lease_token, fencing_token: record.fencing_token}
          end)

        assert {:ok, retried} =
                 flow_retry_many_and_get(nil, retry_items,
                   error: "retry-later",
                   run_at_ms: 5_000,
                   now_ms: 4_100
                 )

        assert Enum.map(retried, & &1.id) == [retry_a, retry_b]
        assert Enum.all?(retried, &(&1.state == "queued"))
        assert Enum.all?(retried, &(&1.attempts == 1))
      end

      test "flow_complete_many atomically completes one-partition batch" do
        partition = uid("tenant-complete-many")
        type = uid("bulk-complete-many")
        id_a = uid("complete-many-a")
        id_b = uid("complete-many-b")

        assert {:ok, _} =
                 flow_create_many_and_get(
                   partition,
                   [%{id: id_a}, %{id: id_b}],
                   type: type,
                   state: "queued",
                   run_at_ms: 1_000,
                   now_ms: 900
                 )

        assert {:ok, claimed} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition,
                   worker: "worker-complete",
                   limit: 2,
                   now_ms: 1_000
                 )

        items =
          claimed
          |> Enum.sort_by(& &1.id)
          |> Enum.map(fn record ->
            %{id: record.id, lease_token: record.lease_token, fencing_token: record.fencing_token}
          end)

        assert {:ok, completed} =
                 flow_complete_many_and_get(partition, items,
                   result: "result-batch",
                   now_ms: 2_000
                 )

        assert Enum.map(completed, & &1.id) == Enum.map(items, & &1.id)
        assert Enum.all?(completed, &(&1.state == "completed"))

        assert Enum.all?(
                 completed,
                 &(is_binary(&1.result_ref) and &1.result_ref != "result-batch")
               )

        assert {:ok, info} = FerricStore.flow_info(type, partition_key: partition)
        assert info.completed == 2
      end

      test "flow_complete_many rolls back when any item fails guard" do
        partition = uid("tenant-complete-many-rollback")
        type = uid("bulk-complete-many-rollback")
        id_a = uid("complete-many-good")
        id_b = uid("complete-many-bad")

        assert {:ok, _} =
                 flow_create_many_and_get(
                   partition,
                   [%{id: id_a}, %{id: id_b}],
                   type: type,
                   state: "queued",
                   run_at_ms: 1_000
                 )

        assert {:ok, claimed} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition,
                   worker: "worker-complete",
                   limit: 2,
                   now_ms: 1_000
                 )

        items =
          claimed
          |> Enum.sort_by(& &1.id)
          |> Enum.map(fn record ->
            fencing_token =
              if record.id == id_b, do: record.fencing_token + 1, else: record.fencing_token

            %{id: record.id, lease_token: record.lease_token, fencing_token: fencing_token}
          end)

        assert {:error, "ERR stale flow lease"} =
                 flow_complete_many_and_get(partition, items, now_ms: 2_000)

        assert {:ok, fetched_a} = FerricStore.flow_get(id_a, partition_key: partition)
        assert {:ok, fetched_b} = FerricStore.flow_get(id_b, partition_key: partition)
        assert fetched_a.state == "running"
        assert fetched_b.state == "running"
        assert fetched_a.version == 2
        assert fetched_b.version == 2

        assert {:ok, history_a} = FerricStore.flow_history(id_a, partition_key: partition)
        assert {:ok, history_b} = FerricStore.flow_history(id_b, partition_key: partition)

        assert Enum.map(history_a, fn {_id, fields} -> fields["event"] end) == [
                 "created",
                 "claimed"
               ]

        assert Enum.map(history_b, fn {_id, fields} -> fields["event"] end) == [
                 "created",
                 "claimed"
               ]
      end

      test "flow_complete_many spans shards and rolls back failing shard group" do
        {same_a, same_b, other} = mixed_partition_keys()
        type = uid("bulk-mixed-complete")
        bad = create_claimed_flow(uid("complete-mixed-bad"), same_a, type, "worker-complete")
        same = create_claimed_flow(uid("complete-mixed-same"), same_b, type, "worker-complete")

        other_flow =
          create_claimed_flow(uid("complete-mixed-other"), other, type, "worker-complete")

        assert {:ok, results} =
                 flow_complete_many_and_get(
                   nil,
                   [
                     %{
                       id: bad.id,
                       partition_key: same_a,
                       lease_token: bad.lease_token,
                       fencing_token: bad.fencing_token + 1
                     },
                     %{
                       id: same.id,
                       partition_key: same_b,
                       lease_token: same.lease_token,
                       fencing_token: same.fencing_token
                     },
                     %{
                       id: other_flow.id,
                       partition_key: other,
                       lease_token: other_flow.lease_token,
                       fencing_token: other_flow.fencing_token
                     }
                   ],
                   now_ms: 2_000
                 )

        assert [
                 {:error, "ERR stale flow lease"},
                 {:error, "ERR stale flow lease"},
                 %{id: other_id, partition_key: ^other, state: "completed"}
               ] = results

        assert other_id == other_flow.id
        assert {:ok, %{state: "running"}} = FerricStore.flow_get(bad.id, partition_key: same_a)
        assert {:ok, %{state: "running"}} = FerricStore.flow_get(same.id, partition_key: same_b)

        assert {:ok, %{state: "completed"}} =
                 FerricStore.flow_get(other_flow.id, partition_key: other)
      end
    end
  end
end
