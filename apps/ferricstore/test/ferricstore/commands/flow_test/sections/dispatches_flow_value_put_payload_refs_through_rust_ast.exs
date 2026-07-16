defmodule Ferricstore.Commands.FlowTest.Sections.DispatchesFlowValuePutPayloadRefsThroughRustAst do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.Dispatcher
      alias Ferricstore.Test.{MockStore, ShardHelpers}

      test "dispatches Flow value put and payload refs through Rust AST" do
        assert %{"ref" => shared_ref} =
                 Dispatcher.dispatch(
                   "FLOW.VALUE.PUT",
                   ["shared-payload", "PARTITION", "tenant-a"],
                   MockStore.make()
                 )

        id = uid("ref-create")

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.CREATE",
                   [id, "TYPE", "checkout", "PARTITION", "tenant-a", "PAYLOAD_REF", shared_ref],
                   MockStore.make()
                 )

        assert %{"id" => ^id, "payload_ref" => ^shared_ref} =
                 Dispatcher.dispatch("FLOW.GET", [id, "PARTITION", "tenant-a"], MockStore.make())

        assert %{"payload" => "shared-payload"} =
                 Dispatcher.dispatch(
                   "FLOW.GET",
                   [id, "PARTITION", "tenant-a", "FULL"],
                   MockStore.make()
                 )

        id_a = uid("ref-create-many-a")
        id_b = uid("ref-create-many-b")

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.CREATE_MANY",
                   [
                     "MIXED",
                     "TYPE",
                     "checkout",
                     "PAYLOAD_REF",
                     shared_ref,
                     "ITEMS",
                     id_a,
                     "tenant-a",
                     id_b,
                     "tenant-b"
                   ],
                   MockStore.make()
                 )

        assert [%{"id" => ^id, "fencing_token" => fencing_token, "lease_token" => lease_token}] =
                 Dispatcher.dispatch(
                   "FLOW.CLAIM_DUE",
                   ["checkout", "PARTITION", "tenant-a", "WORKER", "worker-ref", "LIMIT", "1"],
                   MockStore.make()
                 )

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.TRANSITION_MANY",
                   [
                     "tenant-a",
                     "running",
                     "waiting",
                     "PAYLOAD_REF",
                     shared_ref,
                     "ITEMS",
                     id,
                     Integer.to_string(fencing_token),
                     lease_token
                   ],
                   MockStore.make()
                 )

        assert %{"id" => ^id, "payload_ref" => ^shared_ref} =
                 Dispatcher.dispatch("FLOW.GET", [id, "PARTITION", "tenant-a"], MockStore.make())
      end

      test "rejects unsupported Flow value ref inputs through Rust AST" do
        assert {:error, "ERR syntax error"} =
                 Dispatcher.dispatch(
                   "FLOW.COMPLETE",
                   ["ref-complete", "lease", "FENCING", "1", "RESULT_REF", "result:external"],
                   MockStore.make()
                 )

        assert {:error, "ERR syntax error"} =
                 Dispatcher.dispatch(
                   "FLOW.RETRY",
                   ["ref-retry", "lease", "FENCING", "1", "ERROR_REF", "error:external"],
                   MockStore.make()
                 )

        assert {:error, "ERR syntax error"} =
                 Dispatcher.dispatch(
                   "FLOW.FAIL",
                   ["ref-fail", "lease", "FENCING", "1", "ERROR_REF", "error:external"],
                   MockStore.make()
                 )
      end

      test "dispatches Flow create/get/list/history through Rust AST" do
        id = uid("flow-command")

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.CREATE",
                   [
                     id,
                     "TYPE",
                     "checkout",
                     "STATE",
                     "queued",
                     "RUN_AT",
                     "1000",
                     "PRIORITY",
                     "2",
                     "ROOT_FLOW_ID",
                     "checkout-root",
                     "CORRELATION_ID",
                     "order-123",
                     "RETENTION_TTL",
                     "60000",
                     "HISTORY_HOT_MAX_EVENTS",
                     "0",
                     "HISTORY_MAX_EVENTS",
                     "25"
                   ],
                   MockStore.make()
                 )

        assert %{"id" => ^id, "type" => "checkout", "state" => "queued", "priority" => 2} =
                 Dispatcher.dispatch("FLOW.GET", [id], MockStore.make())

        assert %{
                 "root_flow_id" => "checkout-root",
                 "correlation_id" => "order-123",
                 "history_hot_max_events" => 0,
                 "history_max_events" => 25
               } =
                 Dispatcher.dispatch("FLOW.GET", [id], MockStore.make())

        assert %{"id" => ^id, "type" => "checkout"} =
                 Dispatcher.dispatch("FLOW.GET", [id], MockStore.make())

        assert [%{"id" => ^id}] =
                 Dispatcher.dispatch("FLOW.LIST", ["checkout", "COUNT", "10"], MockStore.make())

        assert [[_event_id, %{"event" => "created", "version" => "1"}]] =
                 Dispatcher.dispatch("FLOW.HISTORY", [id, "COUNT", "10"], MockStore.make())

        assert [[_event_id, %{"event" => "created", "version" => "1"}]] =
                 Dispatcher.dispatch(
                   "FLOW.HISTORY",
                   [id, "COUNT", "10", "INCLUDE_COLD", "true", "CONSISTENT_PROJECTION", "false"],
                   MockStore.make()
                 )

        assert [[_event_id, %{"event" => "created", "version" => "1"}]] =
                 Dispatcher.dispatch(
                   "FLOW.HISTORY",
                   [
                     id,
                     "COUNT",
                     "10",
                     "FROM_MS",
                     "0",
                     "TO_MS",
                     "9999999999999",
                     "FROM_VERSION",
                     "1",
                     "TO_VERSION",
                     "1",
                     "EVENT",
                     "created",
                     "REV",
                     "true"
                   ],
                   MockStore.make()
                 )
      end

      test "dispatches Flow failures through Rust AST" do
        id = uid("flow-command-failures")
        type = uid("flow-command-failures-type")

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.CREATE",
                   [id, "TYPE", type, "RUN_AT", "1000", "NOW", "1000"],
                   MockStore.make()
                 )

        assert [%{"id" => ^id, "lease_token" => lease_token, "fencing_token" => fencing_token}] =
                 Dispatcher.dispatch(
                   "FLOW.CLAIM_DUE",
                   [type, "WORKER", "worker-failures", "LIMIT", "1", "NOW", "1000"],
                   MockStore.make()
                 )

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.FAIL",
                   [id, lease_token, "FENCING", Integer.to_string(fencing_token), "NOW", "1500"],
                   MockStore.make()
                 )

        assert %{"id" => ^id, "state" => "failed"} =
                 Dispatcher.dispatch("FLOW.GET", [id], MockStore.make())

        assert [%{"id" => ^id, "state" => "failed"}] =
                 Dispatcher.dispatch(
                   "FLOW.FAILURES",
                   [type, "FROM_MS", "1000", "TO_MS", "2000", "COUNT", "10"],
                   MockStore.make()
                 )

        assert [%{"id" => ^id, "state" => "failed"}] =
                 Dispatcher.dispatch(
                   "FLOW.TERMINALS",
                   [
                     type,
                     "STATE",
                     "any",
                     "FROM_MS",
                     "1000",
                     "TO_MS",
                     "2000",
                     "REV",
                     "true",
                     "COUNT",
                     "10"
                   ],
                   MockStore.make()
                 )
      end

      test "dispatches Flow retention cleanup through Rust AST" do
        assert %{"flows" => 0, "history" => 0, "values" => 0} =
                 Dispatcher.dispatch(
                   "FLOW.RETENTION_CLEANUP",
                   ["LIMIT", "10", "NOW", "1000"],
                   MockStore.make()
                 )

        assert {:error, "ERR syntax error"} =
                 Dispatcher.dispatch(
                   "FLOW.RETENTION_CLEANUP",
                   ["LIMIT"],
                   MockStore.make()
                 )
      end

      test "dispatches Flow full values through Rust AST" do
        id = uid("flow-command-values")

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.CREATE",
                   [
                     id,
                     "TYPE",
                     "value-command",
                     "STATE",
                     "queued",
                     "PAYLOAD",
                     "create-payload",
                     "RUN_AT",
                     "1000",
                     "NOW",
                     "1000"
                   ],
                   MockStore.make()
                 )

        %{"payload_ref" => payload_ref} = Dispatcher.dispatch("FLOW.GET", [id], MockStore.make())
        assert is_binary(payload_ref)
        assert payload_ref != "create-payload"

        assert %{"payload" => "create-payload", "payload_ref" => ^payload_ref} =
                 Dispatcher.dispatch("FLOW.GET", [id, "FULL", "true"], MockStore.make())

        assert [[_event_id, %{"event" => "created", "payload" => "create-payload"}]] =
                 Dispatcher.dispatch(
                   "FLOW.HISTORY",
                   [id, "COUNT", "10", "VALUES", "true"],
                   MockStore.make()
                 )

        assert [%{"lease_token" => lease_token, "fencing_token" => fencing_token}] =
                 Dispatcher.dispatch(
                   "FLOW.CLAIM_DUE",
                   ["value-command", "WORKER", "worker-a", "LEASE_MS", "30000", "NOW", "1000"],
                   MockStore.make()
                 )

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.COMPLETE",
                   [
                     id,
                     lease_token,
                     "FENCING",
                     Integer.to_string(fencing_token),
                     "RESULT",
                     "complete-result",
                     "NOW",
                     "2000"
                   ],
                   MockStore.make()
                 )

        %{"state" => "completed", "result_ref" => result_ref} =
          Dispatcher.dispatch("FLOW.GET", [id], MockStore.make())

        assert is_binary(result_ref)
        assert result_ref != "complete-result"

        assert %{"payload" => "create-payload", "result" => "complete-result"} =
                 Dispatcher.dispatch("FLOW.GET", [id, "FULL"], MockStore.make())

        assert [
                 _created,
                 _claimed,
                 [_event_id, %{"event" => "completed", "result" => "complete-result"}]
               ] =
                 Dispatcher.dispatch(
                   "FLOW.HISTORY",
                   [id, "COUNT", "10", "VALUES", "true"],
                   MockStore.make()
                 )
      end

      test "dispatches Flow spawn children through Rust AST" do
        parent = uid("flow-command-spawn-parent")
        child_a = uid("flow-command-spawn-child-a")
        child_b = uid("flow-command-spawn-child-b")
        partition = uid("tenant-command")

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.CREATE",
                   [
                     parent,
                     "TYPE",
                     "parent",
                     "STATE",
                     "dispatch",
                     "PARTITION",
                     partition,
                     "NOW",
                     "1000"
                   ],
                   MockStore.make()
                 )

        %{"fencing_token" => fencing_token} =
          Dispatcher.dispatch("FLOW.GET", [parent, "PARTITION", partition], MockStore.make())

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.SPAWN_CHILDREN",
                   [
                     parent,
                     "GROUP",
                     "fanout",
                     "PARTITION",
                     partition,
                     "FENCING",
                     Integer.to_string(fencing_token),
                     "WAIT",
                     "any",
                     "ON_CHILD_FAILED",
                     "fail_parent",
                     "ON_PARENT_CLOSED",
                     "cancel_children",
                     "SUCCESS",
                     "children_done",
                     "FAILURE",
                     "children_failed",
                     "FROM_STATE",
                     "dispatch",
                     "WAIT_STATE",
                     "waiting_children",
                     "NOW",
                     "1010",
                     "RETENTION_TTL",
                     "60000",
                     "HISTORY_HOT_MAX_EVENTS",
                     "2",
                     "HISTORY_MAX_EVENTS",
                     "5",
                     "ITEMS",
                     child_a,
                     "child",
                     "payload-a",
                     child_b,
                     "child",
                     "payload-b"
                   ],
                   MockStore.make()
                 )

        %{"id" => ^parent, "state" => "waiting_children", "child_groups" => child_groups} =
          Dispatcher.dispatch("FLOW.GET", [parent, "PARTITION", partition], MockStore.make())

        assert child_groups["fanout"]["wait"] == "any"
        assert child_groups["fanout"]["children"][child_a] == "running"
        assert child_groups["fanout"]["children"][child_b] == "running"

        assert [%{"id" => ^child_a}, %{"id" => ^child_b}] =
                 Dispatcher.dispatch(
                   "FLOW.BY_PARENT",
                   [parent, "PARTITION", partition, "COUNT", "10"],
                   MockStore.make()
                 )
                 |> Enum.sort_by(& &1["id"])

        assert %{"payload" => "payload-a"} =
                 Dispatcher.dispatch(
                   "FLOW.GET",
                   [child_a, "PARTITION", partition, "FULL"],
                   MockStore.make()
                 )

        assert %{"history_hot_max_events" => 2, "history_max_events" => 5} =
                 Dispatcher.dispatch(
                   "FLOW.GET",
                   [child_a, "PARTITION", partition],
                   MockStore.make()
                 )
      end

      test "dispatches Flow mutation values through Rust AST" do
        transition_id = uid("flow-command-transition-value")
        fail_id = uid("flow-command-fail-value")

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.CREATE",
                   [
                     transition_id,
                     "TYPE",
                     "mutation-values",
                     "STATE",
                     "queued",
                     "PAYLOAD",
                     "initial-payload",
                     "RUN_AT",
                     "1000",
                     "NOW",
                     "1000"
                   ],
                   MockStore.make()
                 )

        assert [%{"lease_token" => lease_token, "fencing_token" => fencing_token}] =
                 Dispatcher.dispatch(
                   "FLOW.CLAIM_DUE",
                   ["mutation-values", "WORKER", "worker-a", "LEASE_MS", "30000", "NOW", "1000"],
                   MockStore.make()
                 )

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.TRANSITION",
                   [
                     transition_id,
                     "running",
                     "waiting",
                     "FENCING",
                     Integer.to_string(fencing_token),
                     "LEASE_TOKEN",
                     lease_token,
                     "PAYLOAD",
                     "transition-payload",
                     "RUN_AT",
                     "2000",
                     "NOW",
                     "1100"
                   ],
                   MockStore.make()
                 )

        %{"state" => "waiting", "payload_ref" => transition_payload_ref} =
          Dispatcher.dispatch("FLOW.GET", [transition_id], MockStore.make())

        assert transition_payload_ref != "transition-payload"

        assert %{"state" => "waiting", "payload" => "transition-payload"} =
                 Dispatcher.dispatch("FLOW.GET", [transition_id, "FULL"], MockStore.make())

        assert [%{"lease_token" => retry_lease, "fencing_token" => retry_fencing}] =
                 Dispatcher.dispatch(
                   "FLOW.CLAIM_DUE",
                   [
                     "mutation-values",
                     "STATE",
                     "waiting",
                     "WORKER",
                     "worker-b",
                     "LEASE_MS",
                     "30000",
                     "NOW",
                     "2000"
                   ],
                   MockStore.make()
                 )

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.RETRY",
                   [
                     transition_id,
                     retry_lease,
                     "FENCING",
                     Integer.to_string(retry_fencing),
                     "ERROR",
                     "retry-error",
                     "PAYLOAD",
                     "retry-payload",
                     "RUN_AT",
                     "3000",
                     "NOW",
                     "2100"
                   ],
                   MockStore.make()
                 )

        %{
          "state" => "waiting",
          "error_ref" => retry_error_ref,
          "payload_ref" => retry_payload_ref
        } = Dispatcher.dispatch("FLOW.GET", [transition_id], MockStore.make())

        assert retry_error_ref != "retry-error"
        assert retry_payload_ref != "retry-payload"

        assert %{"error" => "retry-error", "payload" => "retry-payload"} =
                 Dispatcher.dispatch("FLOW.GET", [transition_id, "FULL"], MockStore.make())

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.CREATE",
                   [
                     fail_id,
                     "TYPE",
                     "mutation-values",
                     "PAYLOAD",
                     "fail-payload",
                     "RUN_AT",
                     "4000",
                     "NOW",
                     "4000"
                   ],
                   MockStore.make()
                 )

        assert [%{"lease_token" => fail_lease, "fencing_token" => fail_fencing}] =
                 Dispatcher.dispatch(
                   "FLOW.CLAIM_DUE",
                   [
                     "mutation-values",
                     "STATE",
                     "queued",
                     "WORKER",
                     "worker-c",
                     "LEASE_MS",
                     "30000",
                     "NOW",
                     "4000"
                   ],
                   MockStore.make()
                 )

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.FAIL",
                   [
                     fail_id,
                     fail_lease,
                     "FENCING",
                     Integer.to_string(fail_fencing),
                     "ERROR",
                     "fail-error",
                     "NOW",
                     "4100"
                   ],
                   MockStore.make()
                 )

        %{"state" => "failed", "error_ref" => fail_error_ref} =
          Dispatcher.dispatch("FLOW.GET", [fail_id], MockStore.make())

        assert fail_error_ref != "fail-error"

        assert %{"payload" => "fail-payload", "error" => "fail-error"} =
                 Dispatcher.dispatch("FLOW.GET", [fail_id, "FULL"], MockStore.make())
      end

      test "dispatches Flow lineage query commands through Rust AST" do
        partition = uid("tenant")
        root = uid("flow-command-root")
        child = uid("flow-command-child")
        correlation = uid("order")

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.CREATE",
                   [
                     root,
                     "TYPE",
                     "lineage",
                     "PARTITION",
                     partition,
                     "CORRELATION_ID",
                     correlation,
                     "NOW",
                     "1000"
                   ],
                   MockStore.make()
                 )

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.CREATE",
                   [
                     child,
                     "TYPE",
                     "lineage",
                     "PARTITION",
                     partition,
                     "PARENT_FLOW_ID",
                     root,
                     "ROOT_FLOW_ID",
                     root,
                     "CORRELATION_ID",
                     correlation,
                     "NOW",
                     "2000"
                   ],
                   MockStore.make()
                 )

        assert [%{"id" => ^child}] =
                 Dispatcher.dispatch(
                   "FLOW.BY_PARENT",
                   [
                     root,
                     "PARTITION",
                     partition,
                     "COUNT",
                     "10",
                     "FROM_MS",
                     "1500",
                     "TO_MS",
                     "2500",
                     "REV",
                     "true",
                     "STATE",
                     "queued"
                   ],
                   MockStore.make()
                 )

        assert [%{"id" => ^root}, %{"id" => ^child}] =
                 Dispatcher.dispatch(
                   "FLOW.BY_ROOT",
                   [
                     root,
                     "PARTITION",
                     partition,
                     "COUNT",
                     "10",
                     "INCLUDE_COLD",
                     "true",
                     "CONSISTENT_PROJECTION",
                     "false"
                   ],
                   MockStore.make()
                 )

        assert [%{"id" => ^root}, %{"id" => ^child}] =
                 Dispatcher.dispatch(
                   "FLOW.BY_CORRELATION",
                   [correlation, "PARTITION", partition, "COUNT", "10"],
                   MockStore.make()
                 )

        assert %{"queued" => 2} =
                 Dispatcher.dispatch(
                   "FLOW.INFO",
                   ["lineage", "PARTITION", partition, "INCLUDE_COLD", "true"],
                   MockStore.make()
                 )
      end

      test "dispatches Flow reclaim through Rust AST" do
        id = uid("flow-command-reclaim")

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.CREATE",
                   [id, "TYPE", "reclaim-command", "RUN_AT", "1000"],
                   MockStore.make()
                 )

        assert [%{"lease_owner" => "worker-a"}] =
                 Dispatcher.dispatch(
                   "FLOW.CLAIM_DUE",
                   ["reclaim-command", "WORKER", "worker-a", "LEASE_MS", "50", "NOW", "1000"],
                   MockStore.make()
                 )

        assert [] =
                 Dispatcher.dispatch(
                   "FLOW.RECLAIM",
                   ["reclaim-command", "WORKER", "worker-b", "LEASE_MS", "50", "NOW", "1049"],
                   MockStore.make()
                 )

        assert [%{"id" => ^id, "lease_owner" => "worker-b"}] =
                 Dispatcher.dispatch(
                   "FLOW.RECLAIM",
                   ["reclaim-command", "WORKER", "worker-b", "LEASE_MS", "50", "NOW", "1050"],
                   MockStore.make()
                 )
      end

      test "dispatches Flow claim_due reclaim controls through Rust AST" do
        type = uid("flow-command-claim-reclaim-ratio")
        expired_id = uid("flow-command-expired")
        fresh_id = uid("flow-command-fresh")

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.CREATE",
                   [expired_id, "TYPE", type, "RUN_AT", "1000"],
                   MockStore.make()
                 )

        assert [%{"id" => ^expired_id}] =
                 Dispatcher.dispatch(
                   "FLOW.CLAIM_DUE",
                   [type, "WORKER", "worker-a", "LEASE_MS", "50", "NOW", "1000"],
                   MockStore.make()
                 )

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.CREATE",
                   [fresh_id, "TYPE", type, "RUN_AT", "1050"],
                   MockStore.make()
                 )

        assert [%{"id" => ^fresh_id, "version" => 2}] =
                 Dispatcher.dispatch(
                   "FLOW.CLAIM_DUE",
                   [
                     type,
                     "WORKER",
                     "worker-b",
                     "LEASE_MS",
                     "50",
                     "NOW",
                     "1050",
                     "RECLAIM_EXPIRED",
                     "false",
                     "RECLAIM_RATIO",
                     "50"
                   ],
                   MockStore.make()
                 )
      end

      test "dispatches Flow extend_lease through Rust AST" do
        id = uid("flow-command-extend-lease")

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.CREATE",
                   [id, "TYPE", "extend-command", "RUN_AT", "1000"],
                   MockStore.make()
                 )

        assert [%{"lease_token" => lease_token, "fencing_token" => fencing_token}] =
                 Dispatcher.dispatch(
                   "FLOW.CLAIM_DUE",
                   ["extend-command", "WORKER", "worker-a", "LEASE_MS", "50", "NOW", "1000"],
                   MockStore.make()
                 )

        assert %{"id" => ^id, "lease_deadline_ms" => 1520} =
                 Dispatcher.dispatch(
                   "FLOW.EXTEND_LEASE",
                   [
                     id,
                     lease_token,
                     "FENCING",
                     Integer.to_string(fencing_token),
                     "LEASE_MS",
                     "500",
                     "NOW",
                     "1020"
                   ],
                   MockStore.make()
                 )
      end

      @tag :extend_lease_return_ok_command
      test "dispatches Flow extend_lease with an OK-only response" do
        id = uid("flow-command-extend-lease-ok")

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.CREATE",
                   [id, "TYPE", "extend-command-ok", "RUN_AT", "1000"],
                   MockStore.make()
                 )

        assert [%{"lease_token" => lease_token, "fencing_token" => fencing_token}] =
                 Dispatcher.dispatch(
                   "FLOW.CLAIM_DUE",
                   ["extend-command-ok", "WORKER", "worker-a", "LEASE_MS", "50", "NOW", "1000"],
                   MockStore.make()
                 )

        assert "OK" =
                 Dispatcher.dispatch(
                   "FLOW.EXTEND_LEASE",
                   [
                     id,
                     lease_token,
                     "FENCING",
                     Integer.to_string(fencing_token),
                     "LEASE_MS",
                     "500",
                     "NOW",
                     "1020",
                     "RETURN",
                     "OK_ON_SUCCESS"
                   ],
                   MockStore.make()
                 )
      end
    end
  end
end
