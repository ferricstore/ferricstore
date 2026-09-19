defmodule Ferricstore.FlowTest.Sections.FlowSpawnChildrenRejectsMissingWaitStateWaitingChildren do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Test.ShardHelpers

      test "flow_spawn_children rejects missing wait_state when waiting for children" do
        parent = uid("flow-parent-missing-wait")
        child = uid("flow-child-missing-wait")
        partition = uid("tenant")

        assert {:ok, created_parent} =
                 flow_create_and_get(parent,
                   type: "parent",
                   state: "dispatch",
                   partition_key: partition
                 )

        assert {:error, "ERR flow wait_state is required when waiting for children"} =
                 flow_spawn_children_and_get(
                   parent,
                   [%{id: child, type: "child"}],
                   group_id: "fanout-1",
                   wait: :all,
                   on_child_failed: :fail_parent,
                   on_parent_closed: :cancel_children,
                   exhaust_to: %{success: "children_done", failure: "failed"},
                   partition_key: partition,
                   from_state: "dispatch",
                   fencing_token: created_parent.fencing_token
                 )

        assert {:ok, nil} = FerricStore.flow_get(child, partition_key: partition)

        assert {:ok, unchanged_parent} = FerricStore.flow_get(parent, partition_key: partition)
        assert unchanged_parent.state == "dispatch"
        assert unchanged_parent.child_groups == %{}
      end

      test "flow_spawn_children rejects empty wait_state for running parent" do
        parent = uid("flow-parent-running-empty-wait")
        child = uid("flow-child-running-empty-wait")
        partition = uid("tenant")

        claimed_parent =
          create_claimed_flow(parent, partition, "parent-running-empty-wait", "parent-worker")

        assert {:error, "ERR flow wait_state is required when waiting for children"} =
                 flow_spawn_children_and_get(
                   parent,
                   [%{id: child, type: "child"}],
                   group_id: "fanout-1",
                   wait: :all,
                   wait_state: "",
                   on_child_failed: :fail_parent,
                   on_parent_closed: :cancel_children,
                   exhaust_to: %{success: "children_done", failure: "failed"},
                   partition_key: partition,
                   lease_token: claimed_parent.lease_token,
                   fencing_token: claimed_parent.fencing_token
                 )

        assert {:ok, nil} = FerricStore.flow_get(child, partition_key: partition)

        assert {:ok, still_running} = FerricStore.flow_get(parent, partition_key: partition)
        assert still_running.state == "running"
        assert still_running.lease_token == claimed_parent.lease_token
      end

      test "stale child terminal command does not double count parent child group" do
        parent = uid("flow-parent-stale-child")
        child_a = uid("flow-child-stale-a")
        child_b = uid("flow-child-stale-b")
        partition = uid("tenant")

        assert {:ok, created_parent} =
                 flow_create_and_get(parent,
                   type: "parent",
                   state: "dispatch",
                   partition_key: partition,
                   now_ms: 3_000
                 )

        assert {:ok, _waiting} =
                 flow_spawn_children_and_get(
                   parent,
                   [%{id: child_a, type: "child"}, %{id: child_b, type: "child"}],
                   group_id: "fanout-1",
                   wait: :all,
                   wait_state: "waiting_children",
                   on_child_failed: :fail_parent,
                   on_parent_closed: :cancel_children,
                   exhaust_to: %{success: "children_done", failure: "failed"},
                   partition_key: partition,
                   from_state: "dispatch",
                   fencing_token: created_parent.fencing_token,
                   now_ms: 3_010
                 )

        claimed_a = create_claimed_flow_child(child_a, partition, "worker-a")

        assert {:ok, _child_done_a} =
                 flow_complete_and_get(child_a, claimed_a.lease_token,
                   partition_key: partition,
                   fencing_token: claimed_a.fencing_token,
                   now_ms: 3_020
                 )

        assert {:ok, after_first} = FerricStore.flow_get(parent, partition_key: partition)
        assert after_first.child_groups["fanout-1"]["summary"]["completed"] == 1
        parent_version = after_first.version

        assert {:ok, _same_child_done_a} =
                 flow_complete_and_get(child_a, claimed_a.lease_token,
                   partition_key: partition,
                   fencing_token: claimed_a.fencing_token,
                   now_ms: 3_030
                 )

        assert {:ok, after_stale} = FerricStore.flow_get(parent, partition_key: partition)
        assert after_stale.version == parent_version
        assert after_stale.child_groups["fanout-1"]["summary"]["completed"] == 1
        assert after_stale.child_groups["fanout-1"]["children"][child_a] == "completed"
        assert after_stale.child_groups["fanout-1"]["children"][child_b] == "running"
      end

      test "nested child completion resolves parent and propagates to grandparent" do
        grandparent = uid("flow-grandparent")
        middle = uid("flow-middle-parent")
        leaf = uid("flow-leaf-child")
        partition = uid("tenant")

        assert {:ok, created_grandparent} =
                 flow_create_and_get(grandparent,
                   type: "parent",
                   state: "dispatch",
                   partition_key: partition,
                   now_ms: 4_000
                 )

        assert {:ok, _waiting_grandparent} =
                 flow_spawn_children_and_get(
                   grandparent,
                   [%{id: middle, type: "child", state: "dispatch"}],
                   group_id: "outer",
                   wait: :all,
                   wait_state: "waiting_middle",
                   on_child_failed: :fail_parent,
                   on_parent_closed: :cancel_children,
                   exhaust_to: %{success: "completed", failure: "failed"},
                   partition_key: partition,
                   from_state: "dispatch",
                   fencing_token: created_grandparent.fencing_token,
                   now_ms: 4_010
                 )

        assert {:ok, middle_record} = FerricStore.flow_get(middle, partition_key: partition)

        assert {:ok, _waiting_middle} =
                 flow_spawn_children_and_get(
                   middle,
                   [%{id: leaf, type: "child"}],
                   group_id: "inner",
                   wait: :all,
                   wait_state: "waiting_leaf",
                   on_child_failed: :fail_parent,
                   on_parent_closed: :cancel_children,
                   exhaust_to: %{success: "completed", failure: "failed"},
                   partition_key: partition,
                   from_state: "dispatch",
                   fencing_token: middle_record.fencing_token,
                   now_ms: 4_020
                 )

        claimed_leaf = create_claimed_flow_child(leaf, partition, "worker-leaf")

        assert {:ok, _leaf_done} =
                 flow_complete_and_get(leaf, claimed_leaf.lease_token,
                   partition_key: partition,
                   fencing_token: claimed_leaf.fencing_token,
                   now_ms: 4_030
                 )

        assert {:ok, resolved_middle} = FerricStore.flow_get(middle, partition_key: partition)
        assert resolved_middle.state == "completed"
        assert resolved_middle.child_groups["inner"]["resolved"] == "success"

        assert {:ok, resolved_grandparent} =
                 FerricStore.flow_get(grandparent, partition_key: partition)

        assert resolved_grandparent.state == "completed"
        assert resolved_grandparent.child_groups["outer"]["resolved"] == "success"
        assert resolved_grandparent.child_groups["outer"]["children"][middle] == "completed"
        assert resolved_grandparent.child_groups["outer"]["summary"]["completed"] == 1
      end

      test "flow_spawn_children can fail parent on child failure or ignore terminal failures" do
        fail_parent = uid("flow-parent-child-fails")
        ignore_parent = uid("flow-parent-child-ignore")
        fail_child = uid("flow-child-fails")
        ignore_child = uid("flow-child-ignore")
        partition = uid("tenant")

        assert {:ok, fail_created} =
                 flow_create_and_get(fail_parent,
                   type: "parent",
                   state: "dispatch",
                   partition_key: partition
                 )

        assert {:ok, _} =
                 flow_spawn_children_and_get(
                   fail_parent,
                   [%{id: fail_child, type: "child"}],
                   group_id: "fanout",
                   wait: :all,
                   wait_state: "waiting_children",
                   on_child_failed: :fail_parent,
                   on_parent_closed: :cancel_children,
                   exhaust_to: %{success: "children_done", failure: "children_failed"},
                   partition_key: partition,
                   from_state: "dispatch",
                   fencing_token: fail_created.fencing_token
                 )

        failed_claim = create_claimed_flow_child(fail_child, partition, "worker-fail")

        assert {:ok, _failed_child} =
                 flow_fail_and_get(fail_child, failed_claim.lease_token,
                   partition_key: partition,
                   fencing_token: failed_claim.fencing_token,
                   error: "boom"
                 )

        assert {:ok, failed_parent} = FerricStore.flow_get(fail_parent, partition_key: partition)
        assert failed_parent.state == "children_failed"
        assert failed_parent.child_groups["fanout"]["resolved"] == "failure"
        fail_result = failed_parent.child_groups["fanout"]["results"][fail_child]
        assert fail_result["status"] == "failed"
        assert is_binary(fail_result["error_ref"])

        assert {:ok, ignore_created} =
                 flow_create_and_get(ignore_parent,
                   type: "parent",
                   state: "dispatch",
                   partition_key: partition
                 )

        assert {:ok, _} =
                 flow_spawn_children_and_get(
                   ignore_parent,
                   [%{id: ignore_child, type: "child"}],
                   group_id: "fanout",
                   wait: :all,
                   wait_state: "waiting_children",
                   on_child_failed: :ignore,
                   on_parent_closed: :abandon_children,
                   exhaust_to: %{success: "children_done", failure: "children_failed"},
                   partition_key: partition,
                   from_state: "dispatch",
                   fencing_token: ignore_created.fencing_token
                 )

        ignore_claim = create_claimed_flow_child(ignore_child, partition, "worker-ignore")

        assert {:ok, _ignored_child} =
                 flow_fail_and_get(ignore_child, ignore_claim.lease_token,
                   partition_key: partition,
                   fencing_token: ignore_claim.fencing_token,
                   error: "ignored"
                 )

        assert {:ok, ignored_parent} =
                 FerricStore.flow_get(ignore_parent, partition_key: partition)

        assert ignored_parent.state == "children_done"
        assert ignored_parent.child_groups["fanout"]["resolved"] == "success"
        assert ignored_parent.child_groups["fanout"]["summary"]["failed"] == 1
      end

      test "flow_cancel parent cancels direct running children when configured" do
        parent = uid("flow-parent-cancel-children")
        child_a = uid("flow-child-cancel-a")
        child_b = uid("flow-child-cancel-b")
        partition = uid("tenant")

        assert {:ok, created_parent} =
                 flow_create_and_get(parent,
                   type: "parent",
                   state: "dispatch",
                   partition_key: partition
                 )

        assert {:ok, waiting} =
                 flow_spawn_children_and_get(
                   parent,
                   [%{id: child_a, type: "child"}, %{id: child_b, type: "child"}],
                   group_id: "fanout",
                   wait: :all,
                   wait_state: "waiting_children",
                   on_child_failed: :fail_parent,
                   on_parent_closed: :cancel_children,
                   exhaust_to: %{success: "children_done", failure: "failed"},
                   partition_key: partition,
                   from_state: "dispatch",
                   fencing_token: created_parent.fencing_token
                 )

        assert {:ok, _cancelled_parent} =
                 flow_cancel_and_get(parent,
                   partition_key: partition,
                   fencing_token: waiting.fencing_token,
                   reason: "parent closed"
                 )

        assert {:ok, child_a_record} = FerricStore.flow_get(child_a, partition_key: partition)
        assert {:ok, child_b_record} = FerricStore.flow_get(child_b, partition_key: partition)
        assert child_a_record.state == "cancelled"
        assert child_b_record.state == "cancelled"

        assert {:ok, final_parent} = FerricStore.flow_get(parent, partition_key: partition)
        assert final_parent.child_groups["fanout"]["resolved"] == "failure"
        assert final_parent.child_groups["fanout"]["summary"]["cancelled"] == 2
        assert final_parent.child_groups["fanout"]["results"][child_a]["status"] == "cancelled"
        assert final_parent.child_groups["fanout"]["results"][child_b]["status"] == "cancelled"
      end

      test "flow_spawn_children is idempotent by group id and rejects conflicts" do
        parent = uid("flow-parent-idempotent-children")
        child = uid("flow-child-idempotent")
        other_child = uid("flow-child-idempotent-conflict")
        partition = uid("tenant")

        assert {:ok, created_parent} =
                 flow_create_and_get(parent,
                   type: "parent",
                   state: "dispatch",
                   partition_key: partition
                 )

        opts = [
          group_id: "fanout",
          wait: :all,
          wait_state: "waiting_children",
          on_child_failed: :ignore,
          on_parent_closed: :abandon_children,
          exhaust_to: %{success: "children_done", failure: "children_failed"},
          partition_key: partition,
          from_state: "dispatch",
          fencing_token: created_parent.fencing_token
        ]

        assert {:ok, first} =
                 flow_spawn_children_and_get(parent, [%{id: child, type: "child"}], opts)

        assert {:ok, same} =
                 flow_spawn_children_and_get(parent, [%{id: child, type: "child"}], opts)

        assert same.id == first.id
        assert same.version == first.version

        assert {:error, "ERR flow child group idempotency conflict"} =
                 flow_spawn_children_and_get(
                   parent,
                   [%{id: other_child, type: "child"}],
                   opts
                 )
      end

      test "flow_spawn_children idempotency includes child max_active_ms" do
        parent = uid("flow-parent-idempotent-child-max-active")
        child = uid("flow-child-idempotent-max-active")
        partition = uid("tenant")

        assert {:ok, created_parent} =
                 flow_create_and_get(parent,
                   type: "parent",
                   state: "dispatch",
                   partition_key: partition
                 )

        opts = [
          group_id: "fanout",
          wait: :all,
          wait_state: "waiting_children",
          on_child_failed: :ignore,
          on_parent_closed: :abandon_children,
          exhaust_to: %{success: "children_done", failure: "children_failed"},
          partition_key: partition,
          from_state: "dispatch",
          fencing_token: created_parent.fencing_token
        ]

        assert {:ok, _waiting} =
                 flow_spawn_children_and_get(
                   parent,
                   [%{id: child, type: "child", max_active_ms: 1_000}],
                   opts
                 )

        assert {:error, "ERR flow child group idempotency conflict"} =
                 flow_spawn_children_and_get(
                   parent,
                   [%{id: child, type: "child", max_active_ms: 2_000}],
                   opts
                 )

        assert {:ok, child_record} = FerricStore.flow_get(child, partition_key: partition)
        assert child_record.max_active_ms == 1_000
      end

      test "flow_spawn_children remains idempotent after child progress" do
        parent = uid("flow-parent-idempotent-progress")
        child_a = uid("flow-child-idempotent-progress-a")
        child_b = uid("flow-child-idempotent-progress-b")
        partition = uid("tenant")

        assert {:ok, created_parent} =
                 flow_create_and_get(parent,
                   type: "parent",
                   state: "dispatch",
                   partition_key: partition
                 )

        opts = [
          group_id: "fanout",
          wait: :all,
          wait_state: "waiting_children",
          on_child_failed: :ignore,
          on_parent_closed: :abandon_children,
          exhaust_to: %{success: "children_done", failure: "children_failed"},
          partition_key: partition,
          from_state: "dispatch",
          fencing_token: created_parent.fencing_token
        ]

        assert {:ok, waiting} =
                 flow_spawn_children_and_get(
                   parent,
                   [%{id: child_a, type: "child"}, %{id: child_b, type: "child"}],
                   opts
                 )

        claimed_a = create_claimed_flow_child(child_a, partition, "worker-a")

        assert {:ok, _child_done_a} =
                 flow_complete_and_get(child_a, claimed_a.lease_token,
                   partition_key: partition,
                   fencing_token: claimed_a.fencing_token
                 )

        assert {:ok, same} =
                 flow_spawn_children_and_get(
                   parent,
                   [%{id: child_a, type: "child"}, %{id: child_b, type: "child"}],
                   opts
                 )

        assert same.id == waiting.id
        assert same.child_groups["fanout"]["summary"]["completed"] == 1
        assert same.child_groups["fanout"]["children"][child_a] == "completed"
      end

      test "flow_spawn_children rejects new groups on terminal parents" do
        parent = uid("flow-parent-terminal-spawn")
        child = uid("flow-child-terminal-spawn")
        partition = uid("tenant")

        assert {:ok, created_parent} =
                 flow_create_and_get(parent,
                   type: "parent",
                   state: "dispatch",
                   partition_key: partition
                 )

        assert {:ok, cancelled_parent} =
                 flow_cancel_and_get(parent,
                   partition_key: partition,
                   fencing_token: created_parent.fencing_token
                 )

        assert cancelled_parent.state == "cancelled"

        assert {:error, "ERR flow parent is terminal"} =
                 flow_spawn_children_and_get(
                   parent,
                   [%{id: child, type: "child"}],
                   group_id: "fanout",
                   wait: :all,
                   wait_state: "waiting_children",
                   on_child_failed: :ignore,
                   on_parent_closed: :abandon_children,
                   exhaust_to: %{success: "children_done", failure: "children_failed"},
                   partition_key: partition,
                   from_state: "cancelled",
                   fencing_token: cancelled_parent.fencing_token
                 )
      end

      test "child group resolution cancels other open child groups when parent closes" do
        parent = uid("flow-parent-close-cancels-groups")
        failing_child = uid("flow-child-close-failing")
        sibling_child = uid("flow-child-close-sibling")
        partition = uid("tenant")

        assert {:ok, created_parent} =
                 flow_create_and_get(parent,
                   type: "parent",
                   state: "dispatch",
                   partition_key: partition
                 )

        assert {:ok, waiting_first} =
                 flow_spawn_children_and_get(
                   parent,
                   [%{id: failing_child, type: "child"}],
                   group_id: "fanout-a",
                   wait: :all,
                   wait_state: "waiting_children",
                   on_child_failed: :fail_parent,
                   on_parent_closed: :cancel_children,
                   exhaust_to: %{success: "children_done", failure: "failed"},
                   partition_key: partition,
                   from_state: "dispatch",
                   fencing_token: created_parent.fencing_token
                 )

        assert {:ok, _waiting_second} =
                 flow_spawn_children_and_get(
                   parent,
                   [%{id: sibling_child, type: "child"}],
                   group_id: "fanout-b",
                   wait: :all,
                   wait_state: "waiting_children",
                   on_child_failed: :ignore,
                   on_parent_closed: :cancel_children,
                   exhaust_to: %{success: "children_done", failure: "failed"},
                   partition_key: partition,
                   from_state: "waiting_children",
                   fencing_token: waiting_first.fencing_token
                 )

        failed_claim = create_claimed_flow_child(failing_child, partition, "worker-fail")

        assert {:ok, _failed_child} =
                 flow_fail_and_get(failing_child, failed_claim.lease_token,
                   partition_key: partition,
                   fencing_token: failed_claim.fencing_token,
                   error: "boom"
                 )

        assert {:ok, closed_parent} = FerricStore.flow_get(parent, partition_key: partition)
        assert closed_parent.state == "failed"
        assert closed_parent.child_groups["fanout-a"]["resolved"] == "failure"
        assert closed_parent.child_groups["fanout-b"]["children"][sibling_child] == "cancelled"
        assert closed_parent.child_groups["fanout-b"]["resolved"] == "failure"

        assert {:ok, cancelled_sibling} =
                 FerricStore.flow_get(sibling_child, partition_key: partition)

        assert cancelled_sibling.state == "cancelled"
      end

      @tag :flow_partition_locality
      test "flow_spawn_children rejects child partition overrides across Raft groups" do
        parent = uid("flow-parent-cross-partition")
        child = uid("flow-child-cross-partition")
        {partition, _same_partition, other_partition} = mixed_partition_keys()

        assert {:ok, created_parent} =
                 flow_create_and_get(parent,
                   type: "parent",
                   state: "dispatch",
                   partition_key: partition
                 )

        assert {:error, "CROSSSLOT Flow dependency keys must hash to the same shard"} =
                 FerricStore.flow_spawn_children(
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

        assert {:ok, unchanged_parent} = FerricStore.flow_get(parent, partition_key: partition)
        assert unchanged_parent.state == "dispatch"
        assert unchanged_parent.fencing_token == created_parent.fencing_token
        assert {:ok, nil} = FerricStore.flow_get(child, partition_key: other_partition)
      end

      test "colocated child completion survives Ra shard restart without duplicate parent summary" do
        parent = uid("flow-parent-ra-replay")
        child = uid("flow-child-ra-replay")
        {partition, _same_partition, _other_partition} = mixed_partition_keys()
        child_partition = partition

        assert {:ok, created_parent} =
                 flow_create_and_get(parent,
                   type: "parent",
                   state: "dispatch",
                   partition_key: partition
                 )

        assert {:ok, _waiting} =
                 flow_spawn_children_and_get(
                   parent,
                   [%{id: child, type: "child", partition_key: child_partition}],
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

        claimed = create_claimed_flow_child(child, child_partition, "worker-ra-replay")

        assert {:ok, _completed_child} =
                 flow_complete_and_get(child, claimed.lease_token,
                   partition_key: child_partition,
                   fencing_token: claimed.fencing_token,
                   result: "ok"
                 )

        assert {:ok, resolved_parent} = FerricStore.flow_get(parent, partition_key: partition)
        assert resolved_parent.state == "children_done"
        assert resolved_parent.child_groups["fanout"]["summary"]["completed"] == 1

        ShardHelpers.compact_wal()

        ShardHelpers.kill_shard_for_key(
          Ferricstore.Flow.Keys.state_key(parent, partition),
          timeout: 30_000
        )

        assert {:ok, replayed_parent} = FerricStore.flow_get(parent, partition_key: partition)
        assert replayed_parent.state == "children_done"
        assert replayed_parent.child_groups["fanout"]["children"][child] == "completed"
        assert replayed_parent.child_groups["fanout"]["summary"]["completed"] == 1

        assert {:ok, parent_history} = FerricStore.flow_history(parent, partition_key: partition)
        parent_events = Enum.map(parent_history, fn {_event_id, fields} -> fields["event"] end)
        assert Enum.count(parent_events, &(&1 == "child_completed")) == 1
      end

      test "terminal parent cancellation cancels colocated children" do
        parent = uid("flow-parent-cross-cancel")
        child = uid("flow-child-cross-cancel")
        {partition, _same_partition, _other_partition} = mixed_partition_keys()
        child_partition = partition

        assert {:ok, created_parent} =
                 flow_create_and_get(parent,
                   type: "parent",
                   state: "dispatch",
                   partition_key: partition
                 )

        assert {:ok, waiting} =
                 flow_spawn_children_and_get(
                   parent,
                   [%{id: child, type: "child", partition_key: child_partition}],
                   group_id: "fanout",
                   wait: :all,
                   wait_state: "waiting_children",
                   on_child_failed: :ignore,
                   on_parent_closed: :cancel_children,
                   exhaust_to: %{success: "children_done", failure: "children_failed"},
                   partition_key: partition,
                   from_state: "dispatch",
                   fencing_token: created_parent.fencing_token
                 )

        assert {:ok, _cancelled_parent} =
                 flow_cancel_and_get(parent,
                   partition_key: partition,
                   fencing_token: waiting.fencing_token
                 )

        assert {:ok, cancelled_child} =
                 FerricStore.flow_get(child, partition_key: child_partition)

        assert cancelled_child.state == "cancelled"

        assert {:ok, final_parent} = FerricStore.flow_get(parent, partition_key: partition)
        assert final_parent.child_groups["fanout"]["children"][child] == "cancelled"
      end

      test "flow_complete_many resolves colocated child groups" do
        parent = uid("flow-parent-cross-complete-many")
        child_a = uid("flow-child-cross-complete-many-a")
        child_b = uid("flow-child-cross-complete-many-b")
        {partition, _same_partition, _other_partition} = mixed_partition_keys()

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
                     %{id: child_a, type: "child", partition_key: partition},
                     %{id: child_b, type: "child", partition_key: partition}
                   ],
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

        claimed_a = create_claimed_flow_child(child_a, partition, "worker-cross-a")
        claimed_b = create_claimed_flow_child(child_b, partition, "worker-cross-b")

        assert {:ok, completed} =
                 flow_complete_many_and_get(
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
                       partition_key: partition,
                       lease_token: claimed_b.lease_token,
                       fencing_token: claimed_b.fencing_token
                     }
                   ],
                   now_ms: 8_000
                 )

        assert Enum.map(completed, & &1.state) == ["completed", "completed"]

        assert {:ok, done_parent} = FerricStore.flow_get(parent, partition_key: partition)
        assert done_parent.state == "children_done"
        assert done_parent.child_groups["fanout"]["resolved"] == "success"
        assert done_parent.child_groups["fanout"]["summary"]["completed"] == 2
      end

      test "Raft routing rejects child dependencies across independent groups" do
        previous_mode = Ferricstore.ReplicationMode.current()
        Ferricstore.ReplicationMode.put_current(:raft)

        parent = uid("flow-parent-raft-cross")
        child = uid("flow-child-raft-cross")
        {partition, _same_partition, other_partition} = mixed_partition_keys()

        try do
          assert {:ok, created_parent} =
                   flow_create_and_get(parent,
                     type: "parent",
                     state: "dispatch",
                     partition_key: partition
                   )

          assert {:error, "CROSSSLOT Flow dependency keys must hash to the same shard"} =
                   FerricStore.flow_spawn_children(
                     parent,
                     [%{id: child, type: "child", partition_key: other_partition}],
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

          assert {:ok, unchanged_parent} =
                   FerricStore.flow_get(parent, partition_key: partition)

          assert unchanged_parent.state == "dispatch"
          assert unchanged_parent.fencing_token == created_parent.fencing_token
          assert {:ok, nil} = FerricStore.flow_get(child, partition_key: other_partition)
        after
          Ferricstore.ReplicationMode.put_current(previous_mode)
        end
      end

      test "colocated nested child completion resolves parent and grandparent" do
        grandparent = uid("flow-grandparent-cross")
        middle = uid("flow-middle-cross")
        leaf = uid("flow-leaf-cross")
        {grand_partition, _same_partition, _other_partition} = mixed_partition_keys()
        middle_partition = grand_partition
        leaf_partition = grand_partition

        assert {:ok, created_grandparent} =
                 flow_create_and_get(grandparent,
                   type: "parent",
                   state: "dispatch",
                   partition_key: grand_partition
                 )

        assert {:ok, _waiting_grandparent} =
                 flow_spawn_children_and_get(
                   grandparent,
                   [
                     %{
                       id: middle,
                       type: "child",
                       state: "dispatch",
                       partition_key: middle_partition
                     }
                   ],
                   group_id: "outer",
                   wait: :all,
                   wait_state: "waiting_middle",
                   on_child_failed: :fail_parent,
                   on_parent_closed: :cancel_children,
                   exhaust_to: %{success: "completed", failure: "failed"},
                   partition_key: grand_partition,
                   from_state: "dispatch",
                   fencing_token: created_grandparent.fencing_token
                 )

        assert {:ok, middle_record} =
                 FerricStore.flow_get(middle, partition_key: middle_partition)

        assert {:ok, _waiting_middle} =
                 flow_spawn_children_and_get(
                   middle,
                   [%{id: leaf, type: "child", partition_key: leaf_partition}],
                   group_id: "inner",
                   wait: :all,
                   wait_state: "waiting_leaf",
                   on_child_failed: :fail_parent,
                   on_parent_closed: :cancel_children,
                   exhaust_to: %{success: "completed", failure: "failed"},
                   partition_key: middle_partition,
                   from_state: "dispatch",
                   fencing_token: middle_record.fencing_token
                 )

        claimed_leaf = create_claimed_flow_child(leaf, leaf_partition, "worker-cross-leaf")

        assert {:ok, _leaf_done} =
                 flow_complete_and_get(leaf, claimed_leaf.lease_token,
                   partition_key: leaf_partition,
                   fencing_token: claimed_leaf.fencing_token
                 )

        assert {:ok, resolved_middle} =
                 FerricStore.flow_get(middle, partition_key: middle_partition)

        assert resolved_middle.state == "completed"
        assert resolved_middle.child_groups["inner"]["resolved"] == "success"

        assert {:ok, resolved_grandparent} =
                 FerricStore.flow_get(grandparent, partition_key: grand_partition)

        assert resolved_grandparent.state == "completed"
        assert resolved_grandparent.child_groups["outer"]["children"][middle] == "completed"
      end

      test "resolved nonterminal child join schedules the parent for a due claim" do
        type = uid("flow-parent-join-due")
        parent = uid("flow-parent-join-due")
        child = uid("flow-child-join-due")
        partition = uid("tenant-join-due")
        join_now_ms = 2_000

        assert {:ok, _created_parent} =
                 flow_create_and_get(parent,
                   type: type,
                   state: "queued",
                   partition_key: partition,
                   now_ms: 1_000,
                   run_at_ms: 1_000
                 )

        assert {:ok, [claimed_parent]} =
                 FerricStore.flow_claim_due(type,
                   state: "queued",
                   partition_key: partition,
                   worker: "worker-join-parent",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1_000
                 )

        assert {:ok, retried_parent} =
                 flow_retry_and_get(parent, claimed_parent.lease_token,
                   partition_key: partition,
                   fencing_token: claimed_parent.fencing_token,
                   error: "retry before child join",
                   run_at_ms: 1_010,
                   now_ms: 1_005
                 )

        assert retried_parent.attempts == 1

        assert {:ok, [claimed_parent_after_retry]} =
                 FerricStore.flow_claim_due(type,
                   state: "queued",
                   partition_key: partition,
                   worker: "worker-join-parent-retry",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1_010
                 )

        assert {:ok, _waiting_parent} =
                 flow_spawn_children_and_get(
                   parent,
                   [%{id: child, type: "child", partition_key: partition, run_at_ms: 1_000}],
                   group_id: "join-due",
                   wait: :all,
                   wait_state: "waiting_children",
                   on_child_failed: :fail_parent,
                   on_parent_closed: :cancel_children,
                   exhaust_to: %{success: "native_join_next", failure: "native_join_failed"},
                   partition_key: partition,
                   from_state: "running",
                   lease_token: claimed_parent_after_retry.lease_token,
                   fencing_token: claimed_parent_after_retry.fencing_token,
                   now_ms: 1_010
                 )

        child_claim = create_claimed_flow_child(child, partition, "worker-join-child")

        assert {:ok, _completed_child} =
                 flow_complete_and_get(child, child_claim.lease_token,
                   partition_key: partition,
                   fencing_token: child_claim.fencing_token,
                   now_ms: join_now_ms
                 )

        assert {:ok, joined_parent} = FerricStore.flow_get(parent, partition_key: partition)
        assert joined_parent.state == "native_join_next"
        assert joined_parent.run_state == "queued"
        assert joined_parent.attempts == 1
        assert joined_parent.next_run_at_ms == join_now_ms

        assert {:ok, [reclaimed_parent]} =
                 FerricStore.flow_claim_due(type,
                   state: "native_join_next",
                   partition_key: partition,
                   worker: "worker-join-successor",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: join_now_ms
                 )

        assert reclaimed_parent.id == parent
        assert reclaimed_parent.state == "running"
        assert reclaimed_parent.run_state == "native_join_next"
        assert reclaimed_parent.attempts == 1
      end

      test "wait any keeps a parent blocked until an unresolved sibling closes" do
        type = uid("flow-parent-join-any-due")
        parent = uid("flow-parent-join-any-due")
        failed_child = uid("flow-child-join-any-failed")
        successful_child = uid("flow-child-join-any-success")
        {partition, same_partition, _other_partition} = mixed_partition_keys()
        create_now_ms = 4_000
        failed_now_ms = create_now_ms + 10
        join_now_ms = create_now_ms + 20

        assert {:ok, _created_parent} =
                 flow_create_and_get(parent,
                   type: type,
                   state: "queued",
                   partition_key: partition,
                   now_ms: create_now_ms,
                   run_at_ms: create_now_ms
                 )

        assert {:ok, [claimed_parent]} =
                 FerricStore.flow_claim_due(type,
                   state: "queued",
                   partition_key: partition,
                   worker: "worker-join-any-parent",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: create_now_ms
                 )

        assert {:ok, _waiting_parent} =
                 flow_spawn_children_and_get(
                   parent,
                   [
                     %{id: failed_child, type: "child", partition_key: partition},
                     %{id: successful_child, type: "child", partition_key: same_partition}
                   ],
                   group_id: "join-any-due",
                   wait: :any,
                   wait_state: "waiting_children",
                   on_child_failed: :ignore,
                   on_parent_closed: :cancel_children,
                   exhaust_to: %{success: "join_any_next", failure: "join_any_failed"},
                   partition_key: partition,
                   from_state: "running",
                   lease_token: claimed_parent.lease_token,
                   fencing_token: claimed_parent.fencing_token,
                   now_ms: create_now_ms + 1
                 )

        failed_claim = create_claimed_flow_child(failed_child, partition, "worker-join-any-fail")

        successful_claim =
          create_claimed_flow_child(successful_child, same_partition, "worker-join-any-success")

        assert {:ok, failed_child_record} =
                 flow_fail_and_get(failed_child, failed_claim.lease_token,
                   partition_key: partition,
                   fencing_token: failed_claim.fencing_token,
                   error: "wait any failure",
                   now_ms: failed_now_ms
                 )

        assert failed_child_record.state == "failed"
        assert {:ok, blocked_parent} = FerricStore.flow_get(parent, partition_key: partition)
        assert blocked_parent.state == "waiting_children"
        assert blocked_parent.run_state == "queued"
        assert is_nil(blocked_parent.next_run_at_ms)
        assert is_nil(blocked_parent.child_groups["join-any-due"]["resolved"])
        assert blocked_parent.child_groups["join-any-due"]["children"][failed_child] == "failed"

        assert blocked_parent.child_groups["join-any-due"]["children"][successful_child] ==
                 "running"

        assert {:ok, []} =
                 FerricStore.flow_claim_due(type,
                   state: "join_any_next",
                   partition_key: partition,
                   worker: "worker-join-any-before-resolution",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: failed_now_ms
                 )

        assert {:ok, _successful_child_record} =
                 flow_complete_and_get(successful_child, successful_claim.lease_token,
                   partition_key: same_partition,
                   fencing_token: successful_claim.fencing_token,
                   now_ms: join_now_ms
                 )

        assert {:ok, joined_parent} = FerricStore.flow_get(parent, partition_key: partition)
        assert joined_parent.state == "join_any_next"
        assert joined_parent.run_state == "queued"
        assert joined_parent.next_run_at_ms == join_now_ms

        assert {:ok, [claimed_successor]} =
                 FerricStore.flow_claim_due(type,
                   state: "join_any_next",
                   partition_key: partition,
                   worker: "worker-join-any-successor",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: join_now_ms
                 )

        assert claimed_successor.id == parent
        assert claimed_successor.state == "running"
        assert claimed_successor.run_state == "join_any_next"
      end

      test "resolved nonterminal child join is included in a cross-shard due scan" do
        type = uid("flow-parent-join-cross-shard-due")
        parent = uid("flow-parent-join-cross-shard-due")
        child = uid("flow-child-join-cross-shard-due")
        decoy = uid("flow-decoy-join-cross-shard-due")
        {partition, _same_partition, other_partition} = mixed_partition_keys()
        create_now_ms = 5_000
        join_now_ms = create_now_ms + 20

        assert shard_for(Ferricstore.Flow.Keys.state_key(parent, partition)) !=
                 shard_for(Ferricstore.Flow.Keys.state_key(decoy, other_partition))

        assert {:ok, _created_parent} =
                 flow_create_and_get(parent,
                   type: type,
                   state: "queued",
                   partition_key: partition,
                   now_ms: create_now_ms,
                   run_at_ms: create_now_ms
                 )

        assert {:ok, _created_decoy} =
                 flow_create_and_get(decoy,
                   type: type,
                   state: "queued",
                   partition_key: other_partition,
                   now_ms: join_now_ms,
                   run_at_ms: join_now_ms
                 )

        assert {:ok, [claimed_parent]} =
                 FerricStore.flow_claim_due(type,
                   state: "queued",
                   partition_key: partition,
                   worker: "worker-join-cross-shard-parent",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: create_now_ms
                 )

        assert {:ok, _waiting_parent} =
                 flow_spawn_children_and_get(
                   parent,
                   [%{id: child, type: "child", partition_key: partition}],
                   group_id: "join-cross-shard-due",
                   wait: :all,
                   wait_state: "waiting_children",
                   on_child_failed: :fail_parent,
                   on_parent_closed: :cancel_children,
                   exhaust_to: %{success: "cross_shard_next", failure: "cross_shard_failed"},
                   partition_key: partition,
                   from_state: "running",
                   lease_token: claimed_parent.lease_token,
                   fencing_token: claimed_parent.fencing_token,
                   now_ms: create_now_ms + 1
                 )

        child_claim = create_claimed_flow_child(child, partition, "worker-join-cross-shard-child")

        assert {:ok, _completed_child} =
                 flow_complete_and_get(child, child_claim.lease_token,
                   partition_key: partition,
                   fencing_token: child_claim.fencing_token,
                   now_ms: join_now_ms
                 )

        assert {:ok, joined_parent} = FerricStore.flow_get(parent, partition_key: partition)
        assert joined_parent.state == "cross_shard_next"
        assert joined_parent.run_state == "queued"
        assert joined_parent.next_run_at_ms == join_now_ms

        assert {:ok, claimed} =
                 FerricStore.flow_claim_due(type,
                   partition_key: :any,
                   state: :any,
                   worker: "worker-join-cross-shard-successors",
                   lease_ms: 30_000,
                   limit: 2,
                   now_ms: join_now_ms
                 )

        assert MapSet.new(Enum.map(claimed, & &1.id)) == MapSet.new([parent, decoy])

        assert Enum.any?(claimed, fn record ->
                 record.id == parent and record.state == "running" and
                   record.run_state == "cross_shard_next"
               end)

        assert Enum.any?(claimed, fn record ->
                 record.id == decoy and record.state == "running" and record.run_state == "queued"
               end)
      end

      test "resolved failed child join schedules a nonterminal failure successor" do
        assert_child_join_due_case(
          "failure-successor",
          :fail,
          "join_success_next",
          "join_failure_next",
          true
        )
      end

      test "resolved successful child join leaves terminal success out of the due index" do
        assert_child_join_due_case("terminal-success", :complete, "completed", "failed", false)
      end

      test "resolved failed child join leaves terminal failure out of the due index" do
        assert_child_join_due_case("terminal-failure", :fail, "completed", "failed", false)
      end

      defp assert_child_join_due_case(
             label,
             operation,
             success_state,
             failure_state,
             expected_due?
           ) do
        type = uid("flow-join-controls-type-#{label}")
        parent = uid("flow-join-controls-parent-#{label}")
        child = uid("flow-join-controls-child-#{label}")
        partition = uid("tenant-join-controls-#{label}")
        create_now_ms = 10_000
        join_now_ms = create_now_ms + 10
        expected_state = if operation == :complete, do: success_state, else: failure_state

        assert {:ok, _created_parent} =
                 flow_create_and_get(parent,
                   type: type,
                   state: "queued",
                   partition_key: partition,
                   now_ms: create_now_ms,
                   run_at_ms: create_now_ms
                 )

        assert {:ok, [claimed_parent]} =
                 FerricStore.flow_claim_due(type,
                   state: "queued",
                   partition_key: partition,
                   worker: "worker-join-controls-parent",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: create_now_ms
                 )

        assert {:ok, _waiting_parent} =
                 flow_spawn_children_and_get(
                   parent,
                   [
                     %{
                       id: child,
                       type: "child",
                       partition_key: partition,
                       run_at_ms: create_now_ms
                     }
                   ],
                   group_id: "join-controls",
                   wait: :all,
                   wait_state: "waiting_children",
                   on_child_failed: :fail_parent,
                   on_parent_closed: :cancel_children,
                   exhaust_to: %{success: success_state, failure: failure_state},
                   partition_key: partition,
                   from_state: "running",
                   lease_token: claimed_parent.lease_token,
                   fencing_token: claimed_parent.fencing_token,
                   now_ms: create_now_ms + 1
                 )

        child_claim = create_claimed_flow_child(child, partition, "worker-join-controls-child")

        child_result =
          case operation do
            :complete ->
              flow_complete_and_get(child, child_claim.lease_token,
                partition_key: partition,
                fencing_token: child_claim.fencing_token,
                now_ms: join_now_ms
              )

            :fail ->
              flow_fail_and_get(child, child_claim.lease_token,
                partition_key: partition,
                fencing_token: child_claim.fencing_token,
                error: "join controls failure",
                now_ms: join_now_ms
              )
          end

        assert {:ok, _completed_child} = child_result
        assert {:ok, joined_parent} = FerricStore.flow_get(parent, partition_key: partition)
        assert joined_parent.state == expected_state
        assert joined_parent.run_state == "queued"

        if expected_due? do
          assert joined_parent.next_run_at_ms == join_now_ms

          assert {:ok, [claimed_successor]} =
                   FerricStore.flow_claim_due(type,
                     state: expected_state,
                     partition_key: partition,
                     worker: "worker-join-controls-successor",
                     lease_ms: 30_000,
                     limit: 1,
                     now_ms: join_now_ms
                   )

          assert claimed_successor.id == parent
          assert claimed_successor.state == "running"
          assert claimed_successor.run_state == expected_state
        else
          assert is_nil(joined_parent.next_run_at_ms)

          assert {:ok, []} =
                   FerricStore.flow_claim_due(type,
                     state: expected_state,
                     partition_key: partition,
                     worker: "worker-join-controls-terminal",
                     lease_ms: 30_000,
                     limit: 1,
                     now_ms: join_now_ms
                   )
        end
      end
    end
  end
end
