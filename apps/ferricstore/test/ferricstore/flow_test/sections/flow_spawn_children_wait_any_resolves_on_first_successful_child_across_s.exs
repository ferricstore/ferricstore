defmodule Ferricstore.FlowTest.Sections.FlowSpawnChildrenWaitAnyResolvesOnFirstSuccessfulChildAcrossS do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Test.ShardHelpers

      test "flow_spawn_children wait any resolves on first successful child in one Raft group" do
        parent = uid("flow-parent-any-local")
        child_a = uid("flow-child-any-local-a")
        child_b = uid("flow-child-any-local-b")
        {partition, same_partition, _other_partition} = mixed_partition_keys()

        assert {:ok, created_parent} =
                 flow_create_and_get(parent,
                   type: "parent",
                   state: "dispatch",
                   partition_key: partition
                 )

        assert {:ok, waiting} =
                 flow_spawn_children_and_get(
                   parent,
                   [
                     %{id: child_a, type: "child", partition_key: partition},
                     %{id: child_b, type: "child", partition_key: same_partition}
                   ],
                   group_id: "fanout",
                   wait: :any,
                   wait_state: "waiting_children",
                   on_child_failed: :ignore,
                   on_parent_closed: :cancel_children,
                   exhaust_to: %{success: "completed", failure: "children_failed"},
                   partition_key: partition,
                   from_state: "dispatch",
                   fencing_token: created_parent.fencing_token
                 )

        assert waiting.state == "waiting_children"

        claimed_b = create_claimed_flow_child(child_b, same_partition, "worker-local-any")

        assert {:ok, _child_done} =
                 flow_complete_and_get(child_b, claimed_b.lease_token,
                   partition_key: same_partition,
                   fencing_token: claimed_b.fencing_token
                 )

        assert {:ok, done_parent} = FerricStore.flow_get(parent, partition_key: partition)
        assert done_parent.state == "completed"
        assert done_parent.child_groups["fanout"]["resolved"] == "success"
        assert done_parent.child_groups["fanout"]["children"][child_b] == "completed"

        assert {:ok, cancelled_sibling} =
                 FerricStore.flow_get(child_a, partition_key: partition)

        assert cancelled_sibling.state == "cancelled"
      end

      test "flow_spawn_children wait any resolves failure when every local child fails" do
        parent = uid("flow-parent-any-all-failed")
        child_a = uid("flow-child-any-all-failed-a")
        child_b = uid("flow-child-any-all-failed-b")
        {partition, same_partition, _other_partition} = mixed_partition_keys()

        assert {:ok, created_parent} =
                 flow_create_and_get(parent,
                   type: "parent",
                   state: "dispatch",
                   partition_key: partition
                 )

        assert {:ok, waiting} =
                 flow_spawn_children_and_get(
                   parent,
                   [
                     %{id: child_a, type: "child", partition_key: partition},
                     %{id: child_b, type: "child", partition_key: same_partition}
                   ],
                   group_id: "fanout",
                   wait: :any,
                   wait_state: "waiting_children",
                   on_child_failed: :ignore,
                   on_parent_closed: :abandon_children,
                   exhaust_to: %{success: "completed", failure: "children_failed"},
                   partition_key: partition,
                   from_state: "dispatch",
                   fencing_token: created_parent.fencing_token
                 )

        assert waiting.state == "waiting_children"

        claimed_a = create_claimed_flow_child(child_a, partition, "worker-local-any-fail-a")
        claimed_b = create_claimed_flow_child(child_b, same_partition, "worker-local-any-fail-b")

        assert {:ok, failed_children} =
                 flow_fail_many_and_get(
                   nil,
                   [
                     %{
                       id: child_a,
                       partition_key: partition,
                       lease_token: claimed_a.lease_token,
                       fencing_token: claimed_a.fencing_token
                     },
                     %{
                       id: child_b,
                       partition_key: same_partition,
                       lease_token: claimed_b.lease_token,
                       fencing_token: claimed_b.fencing_token
                     }
                   ],
                   error: "all failed"
                 )

        assert Enum.map(failed_children, & &1.state) == ["failed", "failed"]

        assert {:ok, failed_parent} = FerricStore.flow_get(parent, partition_key: partition)
        assert failed_parent.state == "children_failed"
        assert failed_parent.child_groups["fanout"]["resolved"] == "failure"
        assert failed_parent.child_groups["fanout"]["summary"]["failed"] == 2
      end

      test "flow_fail_many fail_parent policy closes parent and cancels local siblings" do
        parent = uid("flow-parent-local-fail-many")
        failed_child = uid("flow-child-local-fail-many-failed")
        sibling = uid("flow-child-local-fail-many-sibling")
        {partition, same_partition, _other_partition} = mixed_partition_keys()

        assert {:ok, created_parent} =
                 flow_create_and_get(parent,
                   type: "parent",
                   state: "dispatch",
                   partition_key: partition
                 )

        assert {:ok, _waiting} =
                 flow_spawn_children_and_get(
                   parent,
                   [
                     %{id: failed_child, type: "child", partition_key: same_partition},
                     %{id: sibling, type: "child", partition_key: partition}
                   ],
                   group_id: "fanout",
                   wait: :all,
                   wait_state: "waiting_children",
                   on_child_failed: :fail_parent,
                   on_parent_closed: :cancel_children,
                   exhaust_to: %{success: "children_done", failure: "children_failed"},
                   partition_key: partition,
                   from_state: "dispatch",
                   fencing_token: created_parent.fencing_token
                 )

        claimed_failed =
          create_claimed_flow_child(failed_child, same_partition, "worker-local-fail")

        assert {:ok, [%{id: ^failed_child, state: "failed"}]} =
                 flow_fail_many_and_get(
                   nil,
                   [
                     %{
                       id: failed_child,
                       partition_key: same_partition,
                       lease_token: claimed_failed.lease_token,
                       fencing_token: claimed_failed.fencing_token
                     }
                   ],
                   error: "boom"
                 )

        assert {:ok, failed_parent} = FerricStore.flow_get(parent, partition_key: partition)
        assert failed_parent.state == "children_failed"
        assert failed_parent.child_groups["fanout"]["resolved"] == "failure"
        assert failed_parent.child_groups["fanout"]["children"][failed_child] == "failed"
        assert failed_parent.child_groups["fanout"]["children"][sibling] == "cancelled"

        assert {:ok, cancelled_sibling} =
                 FerricStore.flow_get(sibling, partition_key: partition)

        assert cancelled_sibling.state == "cancelled"

        assert {:ok, parent_history} = FerricStore.flow_history(parent, partition_key: partition)

        parent_events = Enum.map(parent_history, fn {_event_id, fields} -> fields["event"] end)
        assert "child_failed" in parent_events
        assert "children_cancelled" in parent_events
      end

      test "flow_create_many spans shards and rolls back failing shard group" do
        {same_a, same_b, other} = mixed_partition_keys()
        type = uid("bulk-mixed-create")
        existing_id = uid("bulk-mixed-existing")
        same_new_id = uid("bulk-mixed-same")
        other_new_id = uid("bulk-mixed-other")

        assert {:ok, _} =
                 flow_create_and_get(existing_id,
                   type: type,
                   partition_key: same_a,
                   run_at_ms: 1_000
                 )

        assert {:ok, results} =
                 flow_create_many_and_get(
                   nil,
                   [
                     %{id: existing_id, partition_key: same_a},
                     %{id: same_new_id, partition_key: same_b},
                     %{id: other_new_id, partition_key: other}
                   ],
                   type: type,
                   run_at_ms: 1_000,
                   now_ms: 1_000
                 )

        assert [
                 {:error, "ERR flow already exists"},
                 {:error, "ERR flow already exists"},
                 %{id: ^other_new_id, partition_key: ^other}
               ] = results

        assert {:ok, nil} = FerricStore.flow_get(same_new_id, partition_key: same_b)

        assert {:ok, %{id: ^other_new_id}} =
                 FerricStore.flow_get(other_new_id, partition_key: other)

        assert {:ok, []} = FerricStore.flow_history(same_new_id, partition_key: same_b)

        assert {:ok, other_history} = FerricStore.flow_history(other_new_id, partition_key: other)
        assert Enum.map(other_history, fn {_id, fields} -> fields["event"] end) == ["created"]
      end

      test "flow_create emits telemetry without automatic worker pubsub wakeups" do
        id = uid("flow-observe")
        attach_flow_telemetry([[:ferricstore, :flow, :create, :stop]])

        changed_channel = "flow_changed:#{id}"
        due_channel = "flow_due:observability"

        :ok = Ferricstore.PubSub.subscribe(changed_channel, self())
        :ok = Ferricstore.PubSub.subscribe(due_channel, self())

        on_exit(fn ->
          Ferricstore.PubSub.unsubscribe(changed_channel, self())
          Ferricstore.PubSub.unsubscribe(due_channel, self())
        end)

        assert :ok =
                 FerricStore.flow_create(id,
                   type: "observability",
                   state: "queued",
                   run_at_ms: 1_000,
                   now_ms: 1_000
                 )

        assert_receive {:flow_telemetry, [:ferricstore, :flow, :create, :stop],
                        %{duration_ms: duration_ms, count: 1},
                        %{flow_id: ^id, flow_type: "observability", result: :ok, reason: nil}}

        assert is_integer(duration_ms) and duration_ms >= 0

        refute_receive {:pubsub_message, ^changed_channel, _message}, 50
        refute_receive {:pubsub_message, ^due_channel, _message}, 50
      end

      test "flow APIs reject malformed inputs before raft apply" do
        assert {:error, "ERR flow id must be a non-empty string"} =
                 flow_create_and_get("", type: "checkout")

        assert {:error, "ERR flow opts must be a keyword list"} =
                 flow_create_and_get("bad-opts", ["checkout"])

        assert {:error, "ERR flow type is required"} =
                 flow_create_and_get("missing-type", state: "queued")

        assert {:error, "ERR flow type must be a non-empty string"} =
                 flow_create_and_get("empty-type", type: "")

        assert {:error, "ERR flow now_ms must be a non-negative integer"} =
                 flow_create_and_get("bad-now", type: "checkout", now_ms: -1)

        assert {:error, "ERR flow run_at_ms must be a non-negative integer"} =
                 flow_create_and_get("bad-run-at", type: "checkout", run_at_ms: -1)

        assert {:error, "ERR flow priority must be between 0 and 2"} =
                 flow_create_and_get("bad-priority", type: "checkout", priority: 3)

        assert {:error, "ERR flow partition_key must be a non-empty string or :global"} =
                 flow_create_and_get("bad-partition", type: "checkout", partition_key: "")

        assert {:error, "ERR flow opts must be a keyword list"} =
                 FerricStore.flow_get("bad-get", ["bad"])

        assert {:error, "ERR flow type must be a non-empty string"} =
                 FerricStore.flow_claim_due("", worker: "worker-a")

        assert {:error, "ERR flow worker is required"} =
                 FerricStore.flow_claim_due("email", [])

        assert {:error, "ERR flow lease_ms must be a positive integer"} =
                 FerricStore.flow_claim_due("email", worker: "worker-a", lease_ms: 0)

        assert {:error, "ERR flow limit must be a positive integer"} =
                 FerricStore.flow_claim_due("email", worker: "worker-a", limit: 0)

        assert {:error, "ERR flow lease_token must be a non-empty string"} =
                 flow_complete_and_get("flow", "")

        assert {:error, "ERR flow fencing_token is required"} =
                 flow_complete_and_get("flow", "token")

        assert {:error, "ERR flow now_ms must be a non-negative integer"} =
                 flow_retry_and_get("flow", "token", fencing_token: 0, now_ms: -1)

        assert {:error, "ERR flow id must be a non-empty string"} =
                 FerricStore.flow_history("")

        assert {:error, "ERR flow opts must be a keyword list"} =
                 FerricStore.flow_history("flow", ["bad"])

        assert {:error, "ERR flow count must be a positive integer"} =
                 FerricStore.flow_history("flow", count: 0)

        assert {:error, "ERR flow count exceeds maximum 10000"} =
                 FerricStore.flow_history("flow", count: 10_001)

        assert {:error, "ERR flow id must be a non-empty string"} =
                 flow_transition_and_get("", "queued", "done")

        assert {:error, "ERR flow from must be a non-empty string"} =
                 flow_transition_and_get("flow", "", "done")

        assert {:error, "ERR flow to must be a non-empty string"} =
                 flow_transition_and_get("flow", "queued", "")

        assert {:error, "ERR flow opts must be a keyword list"} =
                 flow_transition_and_get("flow", "queued", "done", ["bad"])

        assert {:error, "ERR flow lease_token must be a non-empty string"} =
                 flow_transition_and_get("flow", "queued", "done", lease_token: "")

        assert {:error, "ERR flow fencing_token is required"} =
                 flow_transition_and_get("flow", "queued", "done")

        assert {:error, "ERR flow lease_token must be a non-empty string"} =
                 flow_fail_and_get("flow", "")

        assert {:error, "ERR flow fencing_token is required"} =
                 flow_fail_and_get("flow", "token")

        assert {:error, "ERR flow now_ms must be a non-negative integer"} =
                 flow_fail_and_get("flow", "token", fencing_token: 0, now_ms: -1)

        assert {:error, "ERR flow id must be a non-empty string"} =
                 flow_cancel_and_get("")

        assert {:error, "ERR flow opts must be a keyword list"} =
                 flow_cancel_and_get("flow", ["bad"])

        assert {:error, "ERR flow fencing_token is required"} =
                 flow_cancel_and_get("flow")

        assert {:error, "ERR flow partition_key must be a non-empty string or :global"} =
                 FerricStore.flow_claim_due("email", worker: "worker-a", partition_key: "")

        large_id = String.duplicate("x", 65_536)
        large_due_state = String.duplicate("s", 65_536)

        assert {:error, "ERR key too large" <> _} =
                 flow_create_and_get(large_id, type: "checkout")

        assert {:error, "ERR key too large" <> _} =
                 FerricStore.flow_claim_due("email",
                   worker: "worker-a",
                   state: large_due_state,
                   partition_keys: ["p1", "p2"],
                   priority: nil
                 )

        assert {:error, "ERR flow result_ref input is not supported; use result"} =
                 flow_complete_and_get("flow", "token", fencing_token: 0, result_ref: "r")

        assert {:error, "ERR flow error_ref input is not supported; use error"} =
                 flow_retry_and_get("flow", "token", fencing_token: 0, error_ref: "e")

        assert {:error, "ERR flow error_ref input is not supported; use error"} =
                 flow_fail_and_get("flow", "token", fencing_token: 0, error_ref: "e")

        assert {:error, "ERR flow reason_ref input is not supported; use reason"} =
                 flow_cancel_and_get("flow", fencing_token: 0, reason_ref: "external")

        assert {:error, "ERR flow reason_ref input is not supported; use reason"} =
                 flow_cancel_and_get("flow",
                   fencing_token: 0,
                   reason: "inline",
                   reason_ref: "external"
                 )

        assert {:error, "ERR flow reason_ref input is not supported; use reason"} =
                 FerricStore.flow_rewind("flow", to_event: "1-1", reason_ref: "external")
      end

      test "flow_claim_due atomically leases due flows and removes them from due set" do
        id = uid("flow-claim")

        assert {:ok, _} =
                 flow_create_and_get(id,
                   type: "email",
                   state: "queued",
                   payload: "payload:" <> id,
                   run_at_ms: 1_000
                 )

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due("email",
                   state: "queued",
                   worker: "worker-a",
                   lease_ms: 30_000,
                   limit: 10,
                   now_ms: 1_000
                 )

        assert claimed.id == id
        assert claimed.state == "running"
        assert claimed.lease_owner == "worker-a"
        assert is_binary(claimed.lease_token)
        assert claimed.fencing_token == 1
        assert claimed.version == 2

        assert {:ok, []} =
                 FerricStore.flow_claim_due("email",
                   state: "queued",
                   worker: "worker-b",
                   lease_ms: 30_000,
                   limit: 10,
                   now_ms: 1_000
                 )
      end

      test "flow_claim_due skips Raft write when native due index proves selected partition is empty" do
        ctx = FerricStore.Instance.get(:default)
        type = uid("flow-empty-claim")
        partition = uid("tenant-empty-claim")
        id = uid("flow-empty-claim-id")

        assert {:ok, _} =
                 flow_create_and_get(id,
                   type: type,
                   partition_key: partition,
                   state: "queued",
                   run_at_ms: 1_000,
                   now_ms: 900
                 )

        due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition)
        shard_index = Ferricstore.Store.Router.shard_for(ctx, due_key)

        assert {:ok, [%{id: ^id}]} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition,
                   state: "queued",
                   worker: "worker-a",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1_000,
                   reclaim_expired: false
                 )

        before_empty_claim = :counters.get(ctx.write_version, shard_index + 1)

        assert {:ok, []} =
                 FerricStore.flow_claim_due(type,
                   partition_key: partition,
                   state: "queued",
                   worker: "worker-b",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1_000,
                   reclaim_expired: false
                 )

        assert :counters.get(ctx.write_version, shard_index + 1) == before_empty_claim
      end

      test "flow_claim_due empty precheck does not drop work due near unstamped apply time" do
        ctx = FerricStore.Instance.get(:default)
        type = uid("flow-precheck-slack")
        partition = uid("tenant-precheck-slack")
        id = uid("flow-precheck-slack-id")

        assert {:ok, _} =
                 flow_create_and_get(id,
                   type: type,
                   partition_key: partition,
                   state: "queued",
                   run_at_ms: 1_005,
                   now_ms: 900
                 )

        due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition)
        shard_index = Ferricstore.Store.Router.shard_for(ctx, due_key)
        before_claim = :counters.get(ctx.write_version, shard_index + 1)

        assert {:ok, [%{id: ^id}]} =
                 Ferricstore.CommandTime.with_now_ms(1_000, fn ->
                   FerricStore.flow_claim_due(type,
                     partition_key: partition,
                     state: "queued",
                     worker: "worker-a",
                     lease_ms: 30_000,
                     limit: 1,
                     reclaim_expired: false
                   )
                 end)

        assert :counters.get(ctx.write_version, shard_index + 1) > before_claim
      end

      test "flow_claim_due skips Raft writes when all selected partition keys are empty" do
        ctx = FerricStore.Instance.get(:default)
        type = uid("flow-empty-claim-many")
        partitions = for idx <- 1..8, do: uid("tenant-empty-claim-many-#{idx}")

        before_empty_claim =
          for shard_index <- 0..(ctx.shard_count - 1) do
            :counters.get(ctx.write_version, shard_index + 1)
          end

        assert {:ok, []} =
                 FerricStore.flow_claim_due(type,
                   partition_keys: partitions,
                   state: "queued",
                   worker: "worker-empty-many",
                   lease_ms: 30_000,
                   limit: 8,
                   now_ms: 1_000,
                   reclaim_expired: false
                 )

        after_empty_claim =
          for shard_index <- 0..(ctx.shard_count - 1) do
            :counters.get(ctx.write_version, shard_index + 1)
          end

        assert after_empty_claim == before_empty_claim
      end

      test "flow_claim_due leases large batches without duplicates and drains due members" do
        type = uid("flow-claim-large")
        ids = for i <- 1..100, do: "#{type}:#{i}"

        for id <- ids do
          assert {:ok, _} =
                   flow_create_and_get(id,
                     type: type,
                     state: "queued",
                     payload: "payload:" <> id,
                     run_at_ms: 1_000,
                     now_ms: 1_000
                   )
        end

        assert {:ok, claimed} =
                 FerricStore.flow_claim_due(type,
                   state: "queued",
                   worker: "worker-large",
                   lease_ms: 30_000,
                   limit: 100,
                   now_ms: 1_000
                 )

        claimed_ids = Enum.map(claimed, & &1.id)
        assert length(claimed_ids) == 100
        assert MapSet.new(claimed_ids) == MapSet.new(ids)
        assert Enum.all?(claimed, &(&1.state == "running"))
        assert Enum.all?(claimed, &(&1.lease_owner == "worker-large"))

        assert {:ok, []} =
                 FerricStore.flow_claim_due(type,
                   state: "queued",
                   worker: "worker-large-2",
                   lease_ms: 30_000,
                   limit: 100,
                   now_ms: 1_000
                 )
      end

      test "partition_key keeps related flow keys on one shard and can spread partitions" do
        {partition_a, partition_b} = different_partition_keys()
        id = uid("flow-partition-keys")

        state_a = Ferricstore.Flow.Keys.state_key(id, partition_a)
        history_a = Ferricstore.Flow.Keys.history_key(id, partition_a)
        due_a = Ferricstore.Flow.Keys.due_key("email", "queued", 0, partition_a)
        state_index_a = Ferricstore.Flow.Keys.state_index_key("email", "queued", partition_a)
        inflight_index_a = Ferricstore.Flow.Keys.inflight_index_key("email", partition_a)
        worker_index_a = Ferricstore.Flow.Keys.worker_index_key("worker-a", partition_a)
        state_b = Ferricstore.Flow.Keys.state_key(id, partition_b)

        assert shard_for(state_a) == shard_for(history_a)
        assert shard_for(state_a) == shard_for(due_a)
        assert shard_for(state_a) == shard_for(state_index_a)
        assert shard_for(state_a) == shard_for(inflight_index_a)
        assert shard_for(state_a) == shard_for(worker_index_a)
        assert shard_for(state_a) != shard_for(state_b)
      end

      test "flow lifecycle maintains state, inflight, and worker indexes" do
        id = uid("flow-index")
        type = "indexed"
        worker = "worker-index"
        ctx = FerricStore.Instance.get(:default)

        range_ids = fn index_key ->
          shard_index = Ferricstore.Store.Router.shard_for(ctx, index_key)

          {flow_index, flow_lookup} =
            Ferricstore.Flow.OrderedIndex.table_names(ctx.name, shard_index)

          flow_index
          |> Ferricstore.Flow.NativeOrderedIndex.get(flow_lookup)
          |> Ferricstore.Flow.NativeOrderedIndex.range_slice(
            index_key,
            :neg_inf,
            :inf,
            false,
            0,
            :all
          )
          |> Enum.map(fn {member, _score} -> member end)
        end

        assert {:ok, created} = flow_create_and_get(id, type: type, run_at_ms: 1_000)
        assert Ferricstore.Flow.Keys.auto_partition_key?(created.partition_key)

        queued_index =
          Ferricstore.Flow.Keys.state_index_key(type, "queued", created.partition_key)

        running_index =
          Ferricstore.Flow.Keys.state_index_key(type, "running", created.partition_key)

        completed_index =
          Ferricstore.Flow.Keys.state_index_key(type, "completed", created.partition_key)

        inflight_index = Ferricstore.Flow.Keys.inflight_index_key(type, created.partition_key)
        worker_index = Ferricstore.Flow.Keys.worker_index_key(worker, created.partition_key)

        assert [^id] = range_ids.(queued_index)

        assert {:ok, [first_claim]} =
                 FerricStore.flow_claim_due(type,
                   worker: worker,
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert [] = range_ids.(queued_index)
        assert [^id] = range_ids.(running_index)
        assert [^id] = range_ids.(inflight_index)
        assert [^id] = range_ids.(worker_index)

        assert {:ok, _retried} =
                 flow_retry_and_get(id, first_claim.lease_token,
                   fencing_token: first_claim.fencing_token,
                   run_at_ms: 2_000
                 )

        assert [^id] = range_ids.(queued_index)
        assert [] = range_ids.(running_index)
        assert [] = range_ids.(inflight_index)
        assert [] = range_ids.(worker_index)

        assert {:ok, [second_claim]} =
                 FerricStore.flow_claim_due(type,
                   worker: worker,
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 2_000
                 )

        assert {:ok, _completed} =
                 flow_complete_and_get(id, second_claim.lease_token,
                   fencing_token: second_claim.fencing_token
                 )

        assert [] = range_ids.(running_index)
        assert [] = range_ids.(inflight_index)
        assert [] = range_ids.(worker_index)
        assert [^id] = range_ids.(completed_index)
      end

      test "flow_list, flow_info, and flow_stuck read lifecycle indexes" do
        due_id = uid("flow-list-due")
        running_id = uid("flow-list-running")
        done_id = uid("flow-list-done")
        type = "ops"

        assert {:ok, _} = flow_create_and_get(due_id, type: type, run_at_ms: 2_000)
        assert {:ok, _} = flow_create_and_get(running_id, type: type, run_at_ms: 1_000)
        assert {:ok, _} = flow_create_and_get(done_id, type: type, run_at_ms: 1_000)

        assert {:ok, [claimed_running, claimed_done]} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-ops",
                   lease_ms: 50,
                   limit: 2,
                   now_ms: 1_000
                 )

        assert {:ok, _} =
                 flow_complete_and_get(claimed_done.id, claimed_done.lease_token,
                   fencing_token: claimed_done.fencing_token
                 )

        assert {:ok, queued} = FerricStore.flow_list(type, state: "queued", count: 10)
        assert Enum.map(queued, & &1.id) == [due_id]

        assert {:ok, running} = FerricStore.flow_list(type, state: "running", count: 10)
        assert Enum.map(running, & &1.id) == [claimed_running.id]

        assert {:ok, completed} = FerricStore.flow_list(type, state: "completed", count: 10)
        assert Enum.map(completed, & &1.id) == [claimed_done.id]

        assert {:ok, terminals} = FerricStore.flow_terminals(type, state: "completed", count: 10)
        assert Enum.map(terminals, & &1.id) == [claimed_done.id]

        assert {:ok, info} = FerricStore.flow_info(type)
        assert info.queued == 1
        assert info.running == 1
        assert info.completed == 1
        assert info.inflight == 1

        assert {:ok, stuck} =
                 FerricStore.flow_stuck(type,
                   older_than_ms: 0,
                   count: 10,
                   now_ms: 1_051
                 )

        assert Enum.map(stuck, & &1.id) == [claimed_running.id]
      end

      test "flow_info counts pending terminal records before LMDB writer flush" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
        old_max_batch_ops = Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops)

        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
        Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)
        Application.put_env(:ferricstore, :flow_lmdb_max_batch_ops, 1_000_000)

        ctx = FerricStore.Instance.get(:default)
        partition = uid("tenant-info-pending")
        type = uid("info-pending")
        id = uid("flow-info-pending")

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
          restore_env(:flow_lmdb_max_batch_ops, old_max_batch_ops)
          Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)
        end)

        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

        assert {:ok, _} =
                 flow_create_and_get(id,
                   type: type,
                   partition_key: partition,
                   run_at_ms: 1,
                   now_ms: 1
                 )

        assert {:ok, [claimed]} =
                 FerricStore.flow_claim_due(type,
                   worker: "worker-info-pending",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 2,
                   partition_key: partition
                 )

        assert {:ok, _completed} =
                 flow_complete_and_get(claimed.id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   now_ms: 3,
                   partition_key: partition
                 )

        assert {:ok, info} = FerricStore.flow_info(type, partition_key: partition)
        assert info.completed == 1
      end

      test "flow_info does not write zero LMDB terminal counters for active-only types" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

        ctx = FerricStore.Instance.get(:default)
        partition = uid("tenant-info-zero")
        type = uid("info-zero")
        id = uid("flow-info-zero")

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
        end)

        assert {:ok, _} =
                 flow_create_and_get(id,
                   type: type,
                   partition_key: partition,
                   run_at_ms: 1,
                   now_ms: 1
                 )

        completed_index_key = Ferricstore.Flow.Keys.state_index_key(type, "completed", partition)
        shard_index = Ferricstore.Store.Router.shard_for(ctx, completed_index_key)

        lmdb_path =
          ctx.data_dir
          |> Ferricstore.DataDir.shard_data_path(shard_index)
          |> Ferricstore.Flow.LMDB.path()

        count_key = Ferricstore.Flow.LMDB.terminal_count_key(completed_index_key)

        assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, count_key)
        assert {:ok, info} = FerricStore.flow_info(type, partition_key: partition)
        assert info.completed == 0
        assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, count_key)
      end

      test "flow_info does not build empty terminal score indexes for active-only types" do
        ctx = FerricStore.Instance.get(:default)
        partition = uid("tenant-info-empty-index")
        type = uid("info-empty-index")
        id = uid("flow-info-empty-index")

        assert {:ok, _} =
                 flow_create_and_get(id,
                   type: type,
                   partition_key: partition,
                   run_at_ms: 1,
                   now_ms: 1
                 )

        completed_index_key = Ferricstore.Flow.Keys.state_index_key(type, "completed", partition)
        shard_index = Ferricstore.Store.Router.shard_for(ctx, completed_index_key)

        {_index_table, lookup_table} =
          Ferricstore.Store.Shard.ZSetIndex.table_names(ctx.name, shard_index)

        refute :ets.member(lookup_table, {:ready, completed_index_key})
        assert {:ok, info} = FerricStore.flow_info(type, partition_key: partition)
        assert info.completed == 0
        refute :ets.member(lookup_table, {:ready, completed_index_key})
      end
    end
  end
end
