defmodule Ferricstore.FlowTest.Sections.FlowFailManyAtomicallyFailsOnePartitionBatch do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Test.ShardHelpers

      test "flow_fail_many atomically fails one-partition batch" do
        partition = uid("tenant-fail-many")
        type = uid("bulk-fail-many")
        id_a = uid("fail-many-a")
        id_b = uid("fail-many-b")

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
                   worker: "worker-fail",
                   limit: 2,
                   now_ms: 1_000
                 )

        items =
          claimed
          |> Enum.sort_by(& &1.id)
          |> Enum.map(fn record ->
            %{id: record.id, lease_token: record.lease_token, fencing_token: record.fencing_token}
          end)

        assert {:ok, failed} =
                 flow_fail_many_and_get(partition, items,
                   error: "error-batch",
                   now_ms: 2_000
                 )

        assert Enum.map(failed, & &1.id) == Enum.map(items, & &1.id)
        assert Enum.all?(failed, &(&1.state == "failed"))
        assert Enum.all?(failed, &(is_binary(&1.error_ref) and &1.error_ref != "error-batch"))

        assert {:ok, info} = FerricStore.flow_info(type, partition_key: partition)
        assert info.failed == 2
      end

      test "flow_fail_many rolls back when any item fails guard" do
        partition = uid("tenant-fail-many-rollback")
        type = uid("bulk-fail-many-rollback")
        id_a = uid("fail-many-good")
        id_b = uid("fail-many-bad")

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
                   worker: "worker-fail",
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
                 flow_fail_many_and_get(partition, items, now_ms: 2_000)

        assert {:ok, fetched_a} = FerricStore.flow_get(id_a, partition_key: partition)
        assert {:ok, fetched_b} = FerricStore.flow_get(id_b, partition_key: partition)
        assert fetched_a.state == "running"
        assert fetched_b.state == "running"
        assert fetched_a.version == 2
        assert fetched_b.version == 2
      end

      test "flow_retry_many spans shards and rolls back failing shard group" do
        {same_a, same_b, other} = mixed_partition_keys()
        type = uid("bulk-mixed-retry")
        bad = create_claimed_flow(uid("retry-mixed-bad"), same_a, type, "worker-retry")
        same = create_claimed_flow(uid("retry-mixed-same"), same_b, type, "worker-retry")
        other_flow = create_claimed_flow(uid("retry-mixed-other"), other, type, "worker-retry")

        assert {:ok, results} =
                 flow_retry_many_and_get(
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
                   run_at_ms: 2_000,
                   now_ms: 2_000
                 )

        assert [
                 {:error, "ERR stale flow lease"},
                 {:error, "ERR stale flow lease"},
                 %{id: other_id, partition_key: ^other, state: "queued"}
               ] = results

        assert other_id == other_flow.id
        assert {:ok, %{state: "running"}} = FerricStore.flow_get(bad.id, partition_key: same_a)
        assert {:ok, %{state: "running"}} = FerricStore.flow_get(same.id, partition_key: same_b)

        assert {:ok, %{state: "queued"}} =
                 FerricStore.flow_get(other_flow.id, partition_key: other)
      end

      test "flow_retry_many terminal exhaustion updates cross-shard parent child group" do
        parent = uid("flow-retry-many-parent-cross")
        child = uid("flow-retry-many-child-cross")
        {partition, _same_partition, other_partition} = mixed_partition_keys()

        assert {:ok, created_parent} =
                 flow_create_and_get(parent,
                   type: "parent",
                   state: "dispatch",
                   partition_key: partition
                 )

        assert {:ok, waiting} =
                 flow_spawn_children_and_get(
                   parent,
                   [%{id: child, type: "child", partition_key: other_partition}],
                   group_id: "retry-many-fanout",
                   wait: :all,
                   wait_state: "waiting_children",
                   on_child_failed: :fail_parent,
                   on_parent_closed: :abandon_children,
                   exhaust_to: %{success: "children_done", failure: "children_failed"},
                   partition_key: partition,
                   from_state: "dispatch",
                   fencing_token: created_parent.fencing_token
                 )

        assert waiting.state == "waiting_children"
        claimed = create_claimed_flow_child(child, other_partition, "worker-retry-many-cross")

        assert {:ok, [exhausted_child]} =
                 flow_retry_many_and_get(
                   nil,
                   [
                     %{
                       id: child,
                       partition_key: other_partition,
                       lease_token: claimed.lease_token,
                       fencing_token: claimed.fencing_token
                     }
                   ],
                   now_ms: 2_000,
                   retry: [max_retries: 0, exhausted_to: "failed"]
                 )

        assert exhausted_child.state == "failed"

        assert {:ok, failed_parent} = FerricStore.flow_get(parent, partition_key: partition)
        assert failed_parent.state == "children_failed"
        assert failed_parent.child_groups["retry-many-fanout"]["children"][child] == "failed"
        assert failed_parent.child_groups["retry-many-fanout"]["summary"]["failed"] == 1
      end

      test "flow_fail_many spans shards and rolls back failing shard group" do
        {same_a, same_b, other} = mixed_partition_keys()
        type = uid("bulk-mixed-fail")
        bad = create_claimed_flow(uid("fail-mixed-bad"), same_a, type, "worker-fail")
        same = create_claimed_flow(uid("fail-mixed-same"), same_b, type, "worker-fail")
        other_flow = create_claimed_flow(uid("fail-mixed-other"), other, type, "worker-fail")

        assert {:ok, results} =
                 flow_fail_many_and_get(
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
                 %{id: other_id, partition_key: ^other, state: "failed"}
               ] = results

        assert other_id == other_flow.id
        assert {:ok, %{state: "running"}} = FerricStore.flow_get(bad.id, partition_key: same_a)
        assert {:ok, %{state: "running"}} = FerricStore.flow_get(same.id, partition_key: same_b)

        assert {:ok, %{state: "failed"}} =
                 FerricStore.flow_get(other_flow.id, partition_key: other)
      end

      test "flow_cancel_many atomically cancels one-partition queued batch" do
        partition = uid("tenant-cancel-many")
        type = uid("bulk-cancel-many")
        id_a = uid("cancel-many-a")
        id_b = uid("cancel-many-b")

        assert {:ok, _} =
                 flow_create_many_and_get(
                   partition,
                   [%{id: id_a}, %{id: id_b}],
                   type: type,
                   state: "queued",
                   run_at_ms: 1_000
                 )

        items = [
          %{id: id_a, fencing_token: 0},
          %{id: id_b, fencing_token: 0}
        ]

        assert {:ok, cancelled} =
                 flow_cancel_many_and_get(partition, items,
                   reason: "cancel-batch",
                   now_ms: 2_000
                 )

        assert Enum.map(cancelled, & &1.id) == [id_a, id_b]
        assert Enum.all?(cancelled, &(&1.state == "cancelled"))
        assert Enum.all?(cancelled, &(is_binary(&1.error_ref) and &1.error_ref != "cancel-batch"))

        assert {:ok, info} = FerricStore.flow_info(type, partition_key: partition)
        assert info.cancelled == 2
      end

      test "flow_cancel_many rolls back when any item fails guard" do
        partition = uid("tenant-cancel-many-rollback")
        type = uid("bulk-cancel-many-rollback")
        id_a = uid("cancel-many-good")
        id_b = uid("cancel-many-bad")

        assert {:ok, _} =
                 flow_create_many_and_get(
                   partition,
                   [%{id: id_a}, %{id: id_b}],
                   type: type,
                   state: "queued",
                   run_at_ms: 1_000
                 )

        items = [
          %{id: id_a, fencing_token: 0},
          %{id: id_b, fencing_token: 1}
        ]

        assert {:error, "ERR stale flow lease"} =
                 flow_cancel_many_and_get(partition, items, now_ms: 2_000)

        assert {:ok, fetched_a} = FerricStore.flow_get(id_a, partition_key: partition)
        assert {:ok, fetched_b} = FerricStore.flow_get(id_b, partition_key: partition)
        assert fetched_a.state == "queued"
        assert fetched_b.state == "queued"
        assert fetched_a.version == 1
        assert fetched_b.version == 1
      end

      test "terminal many independent keeps successful items when one item fails" do
        partition = uid("tenant-terminal-many-independent")

        complete_bad =
          create_claimed_flow(
            uid("complete-independent-bad"),
            partition,
            uid("complete-independent"),
            "worker-complete-independent"
          )

        complete_good =
          create_claimed_flow(
            uid("complete-independent-good"),
            partition,
            uid("complete-independent"),
            "worker-complete-independent"
          )

        assert {:ok, [{:error, "ERR stale flow lease"}, :ok]} =
                 FerricStore.flow_complete_many(
                   partition,
                   [
                     %{
                       id: complete_bad.id,
                       lease_token: complete_bad.lease_token,
                       fencing_token: complete_bad.fencing_token + 1
                     },
                     %{
                       id: complete_good.id,
                       lease_token: complete_good.lease_token,
                       fencing_token: complete_good.fencing_token
                     }
                   ],
                   now_ms: 2_000,
                   independent: true
                 )

        assert {:ok, %{state: "running"}} =
                 FerricStore.flow_get(complete_bad.id, partition_key: partition)

        assert {:ok, %{state: "completed"}} =
                 FerricStore.flow_get(complete_good.id, partition_key: partition)

        retry_bad =
          create_claimed_flow(
            uid("retry-independent-bad"),
            partition,
            uid("retry-independent"),
            "worker-retry-independent"
          )

        retry_good =
          create_claimed_flow(
            uid("retry-independent-good"),
            partition,
            uid("retry-independent"),
            "worker-retry-independent"
          )

        assert {:ok, [{:error, "ERR stale flow lease"}, :ok]} =
                 FerricStore.flow_retry_many(
                   partition,
                   [
                     %{
                       id: retry_bad.id,
                       lease_token: retry_bad.lease_token,
                       fencing_token: retry_bad.fencing_token + 1
                     },
                     %{
                       id: retry_good.id,
                       lease_token: retry_good.lease_token,
                       fencing_token: retry_good.fencing_token
                     }
                   ],
                   run_at_ms: 3_000,
                   now_ms: 2_000,
                   independent: true
                 )

        assert {:ok, %{state: "running"}} =
                 FerricStore.flow_get(retry_bad.id, partition_key: partition)

        assert {:ok, %{state: "queued"}} =
                 FerricStore.flow_get(retry_good.id, partition_key: partition)

        fail_bad =
          create_claimed_flow(
            uid("fail-independent-bad"),
            partition,
            uid("fail-independent"),
            "worker-fail-independent"
          )

        fail_good =
          create_claimed_flow(
            uid("fail-independent-good"),
            partition,
            uid("fail-independent"),
            "worker-fail-independent"
          )

        assert {:ok, [{:error, "ERR stale flow lease"}, :ok]} =
                 FerricStore.flow_fail_many(
                   partition,
                   [
                     %{
                       id: fail_bad.id,
                       lease_token: fail_bad.lease_token,
                       fencing_token: fail_bad.fencing_token + 1
                     },
                     %{
                       id: fail_good.id,
                       lease_token: fail_good.lease_token,
                       fencing_token: fail_good.fencing_token
                     }
                   ],
                   now_ms: 2_000,
                   independent: true
                 )

        assert {:ok, %{state: "running"}} =
                 FerricStore.flow_get(fail_bad.id, partition_key: partition)

        assert {:ok, %{state: "failed"}} =
                 FerricStore.flow_get(fail_good.id, partition_key: partition)

        cancel_type = uid("cancel-independent")
        cancel_bad = uid("cancel-independent-bad")
        cancel_good = uid("cancel-independent-good")

        assert {:ok, _} =
                 flow_create_many_and_get(
                   partition,
                   [%{id: cancel_bad}, %{id: cancel_good}],
                   type: cancel_type,
                   state: "queued",
                   run_at_ms: 1_000
                 )

        assert {:ok, [{:error, "ERR stale flow lease"}, :ok]} =
                 FerricStore.flow_cancel_many(
                   partition,
                   [
                     %{id: cancel_bad, fencing_token: 1},
                     %{id: cancel_good, fencing_token: 0}
                   ],
                   now_ms: 2_000,
                   independent: true
                 )

        assert {:ok, %{state: "queued"}} =
                 FerricStore.flow_get(cancel_bad, partition_key: partition)

        assert {:ok, %{state: "cancelled"}} =
                 FerricStore.flow_get(cancel_good, partition_key: partition)
      end

      test "terminal many independent does not pre-read records for cross-shard planning" do
        partition = uid("tenant-terminal-many-no-preread")
        flow_type = uid("terminal-many-no-preread")

        claimed_a =
          create_claimed_flow(
            uid("complete-independent-fast-a"),
            partition,
            flow_type,
            "worker-complete-independent-fast"
          )

        claimed_b =
          create_claimed_flow(
            uid("complete-independent-fast-b"),
            partition,
            flow_type,
            "worker-complete-independent-fast"
          )

        Process.put(:ferricstore_flow_terminal_many_values_hook, fn keys ->
          flunk("independent terminal batch pre-read #{length(keys)} records")
        end)

        try do
          result =
            FerricStore.flow_complete_many(
              partition,
              [
                %{
                  id: claimed_a.id,
                  lease_token: claimed_a.lease_token,
                  fencing_token: claimed_a.fencing_token
                },
                %{
                  id: claimed_b.id,
                  lease_token: claimed_b.lease_token,
                  fencing_token: claimed_b.fencing_token
                }
              ],
              now_ms: 2_000,
              independent: true
            )

          assert {:ok, [:ok, :ok]} = result
        after
          Process.delete(:ferricstore_flow_terminal_many_values_hook)
        end
      end

      test "flow_cancel_many spans shards and rolls back failing shard group" do
        {same_a, same_b, other} = mixed_partition_keys()
        type = uid("bulk-mixed-cancel")
        bad_id = uid("cancel-mixed-bad")
        same_id = uid("cancel-mixed-same")
        other_id = uid("cancel-mixed-other")

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
                 flow_cancel_many_and_get(
                   nil,
                   [
                     %{id: bad_id, partition_key: same_a, fencing_token: 1},
                     %{id: same_id, partition_key: same_b, fencing_token: 0},
                     %{id: other_id, partition_key: other, fencing_token: 0}
                   ],
                   now_ms: 2_000
                 )

        assert [
                 {:error, "ERR stale flow lease"},
                 {:error, "ERR stale flow lease"},
                 %{id: ^other_id, partition_key: ^other, state: "cancelled"}
               ] = results

        assert {:ok, %{state: "queued"}} = FerricStore.flow_get(bad_id, partition_key: same_a)
        assert {:ok, %{state: "queued"}} = FerricStore.flow_get(same_id, partition_key: same_b)
        assert {:ok, %{state: "cancelled"}} = FerricStore.flow_get(other_id, partition_key: other)
      end

      test "flow_transition enforces expected state and running lease guard" do
        id = uid("flow-transition-guard")

        assert {:ok, _} =
                 flow_create_and_get(id, type: "checkout", state: "queued", run_at_ms: 1_000)

        assert {:error, "ERR flow wrong state"} =
                 flow_transition_and_get(id, "running", "completed",
                   fencing_token: 0,
                   run_at_ms: 1_000
                 )

        assert {:ok, fetched} = FerricStore.flow_get(id)
        assert fetched.state == "queued"
        assert fetched.version == 1

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due("checkout",
                   state: "queued",
                   worker: "worker-a",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert {:error, "ERR stale flow lease"} =
                 flow_transition_and_get(id, "running", "next",
                   fencing_token: claimed.fencing_token,
                   run_at_ms: 2_000
                 )

        assert {:error, "ERR stale flow lease"} =
                 flow_transition_and_get(id, "running", "next",
                   lease_token: claimed.lease_token,
                   fencing_token: claimed.fencing_token + 1,
                   run_at_ms: 2_000
                 )

        assert {:ok, transitioned} =
                 flow_transition_and_get(id, "running", "next",
                   lease_token: claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   run_at_ms: 2_000
                 )

        assert transitioned.state == "next"
        assert transitioned.lease_token == nil
      end

      test "flow_transition rejects terminal states so terminal hooks stay centralized" do
        parent = uid("flow-terminal-transition-parent")
        child = uid("flow-terminal-transition-child")
        {partition, _same_partition, other_partition} = mixed_partition_keys()

        assert {:ok, created_parent} =
                 flow_create_and_get(parent,
                   type: "parent",
                   state: "dispatch",
                   partition_key: partition
                 )

        assert {:ok, waiting} =
                 flow_spawn_children_and_get(
                   parent,
                   [%{id: child, type: "child", partition_key: other_partition}],
                   group_id: "fanout",
                   wait: :all,
                   wait_state: "waiting_children",
                   on_child_failed: :ignore,
                   on_parent_closed: :abandon_children,
                   exhaust_to: %{success: "children_done", failure: "children_failed"},
                   partition_key: partition,
                   from_state: "dispatch",
                   fencing_token: created_parent.fencing_token
                 )

        assert waiting.state == "waiting_children"
        claimed = create_claimed_flow_child(child, other_partition, "worker-terminal-transition")

        assert {:error,
                "ERR terminal flow state requires FLOW.COMPLETE, FLOW.FAIL, or FLOW.CANCEL"} =
                 flow_transition_and_get(child, "running", "completed",
                   partition_key: other_partition,
                   lease_token: claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   run_at_ms: 2_000
                 )

        assert {:ok, unchanged_parent} = FerricStore.flow_get(parent, partition_key: partition)
        assert unchanged_parent.state == "waiting_children"
        assert unchanged_parent.child_groups["fanout"]["children"][child] == "running"
      end

      test "flow_transition rolls back index changes when derived keys are invalid" do
        id = uid("flow-transition-rollback")
        huge_state = String.duplicate("x", 65_536)

        assert {:ok, _} =
                 flow_create_and_get(id, type: "audit", state: "queued", run_at_ms: 1_000)

        assert {:error, "ERR key too large" <> _} =
                 flow_transition_and_get(id, "queued", huge_state,
                   fencing_token: 0,
                   run_at_ms: 2_000,
                   now_ms: 1_100
                 )

        assert {:ok, fetched} = FerricStore.flow_get(id)
        assert fetched.state == "queued"
        assert fetched.version == 1

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due("audit",
                   state: "queued",
                   worker: "worker-a",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert claimed.id == id
      end

      test "flow_fail and flow_cancel write terminal states and remove due work" do
        fail_id = uid("flow-fail")
        cancel_id = uid("flow-cancel")
        fail_type = "jobs-fail"
        cancel_type = "jobs-cancel"

        assert {:ok, _} = flow_create_and_get(fail_id, type: fail_type, run_at_ms: 1_000)
        assert {:ok, _} = flow_create_and_get(cancel_id, type: cancel_type, run_at_ms: 1_000)

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due(fail_type,
                   state: "queued",
                   worker: "worker-a",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert claimed.id == fail_id

        assert {:error, "ERR stale flow lease"} =
                 flow_fail_and_get(fail_id, claimed.lease_token,
                   fencing_token: claimed.fencing_token + 1,
                   error: "error:" <> fail_id,
                   now_ms: 1_500
                 )

        assert {:ok, failed} =
                 flow_fail_and_get(fail_id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   error: "error:" <> fail_id,
                   now_ms: 1_500
                 )

        assert failed.state == "failed"
        assert is_binary(failed.error_ref)
        assert failed.error_ref != "error:" <> fail_id
        assert failed.lease_token == nil
        assert failed.next_run_at_ms == nil

        assert {:ok, cancelled} =
                 flow_cancel_and_get(cancel_id,
                   fencing_token: 0,
                   reason: "reason:" <> cancel_id,
                   now_ms: 1_500
                 )

        assert cancelled.state == "cancelled"
        assert is_binary(cancelled.error_ref)
        assert cancelled.error_ref != "reason:" <> cancel_id
        assert cancelled.next_run_at_ms == nil

        assert {:ok, []} =
                 FerricStore.flow_claim_due(fail_type,
                   state: "queued",
                   worker: "worker-b",
                   lease_ms: 30_000,
                   limit: 10,
                   now_ms: 1_500
                 )

        assert {:ok, []} =
                 FerricStore.flow_claim_due(cancel_type,
                   state: "queued",
                   worker: "worker-b",
                   lease_ms: 30_000,
                   limit: 10,
                   now_ms: 1_500
                 )
      end

      test "flow_history returns transition events" do
        id = uid("flow-history")

        assert {:ok, _} =
                 flow_create_and_get(id,
                   type: "audit",
                   state: "queued",
                   run_at_ms: 1_000,
                   now_ms: 999
                 )

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due("audit",
                   state: "queued",
                   worker: "worker-a",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert {:ok, _} =
                 flow_complete_and_get(id, claimed.lease_token,
                   fencing_token: claimed.fencing_token
                 )

        assert {:ok, events} = FerricStore.flow_history(id, count: 10)

        assert Enum.map(events, fn {_id, fields} -> fields["event"] end) == [
                 "created",
                 "claimed",
                 "completed"
               ]

        history_key = Ferricstore.Flow.Keys.history_key(id)
        shard = shard_for(history_key)
        {flow_index, flow_lookup} = Ferricstore.Flow.OrderedIndex.table_names(:default, shard)

        assert Ferricstore.Flow.OrderedIndex.count_all(flow_lookup, history_key) == 0

        assert Ferricstore.Flow.OrderedIndex.rank_range(flow_index, history_key, 0, 2, false)
               |> length() ==
                 0

        assert [] = :ets.lookup(Ferricstore.Stream.Meta, history_key)
      end
    end
  end
end
