defmodule Ferricstore.Commands.FlowTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.{MockStore, ShardHelpers}

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  defp uid(prefix), do: "#{prefix}:#{System.unique_integer([:positive])}"

  test "dispatches Flow create/get/list/history through Rust AST" do
    id = uid("flow-command")

    assert %{"id" => ^id, "type" => "checkout", "state" => "queued", "priority" => 2} =
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
                 "HISTORY_MAX_EVENTS",
                 "5"
               ],
               MockStore.make()
             )

    assert %{"root_flow_id" => "checkout-root", "correlation_id" => "order-123"} =
             Dispatcher.dispatch("FLOW.GET", [id], MockStore.make())

    assert %{"id" => ^id, "type" => "checkout"} =
             Dispatcher.dispatch("FLOW.GET", [id], MockStore.make())

    assert [%{"id" => ^id}] =
             Dispatcher.dispatch("FLOW.LIST", ["checkout", "COUNT", "10"], MockStore.make())

    assert [[_event_id, %{"event" => "created", "version" => "1"}]] =
             Dispatcher.dispatch("FLOW.HISTORY", [id, "COUNT", "10"], MockStore.make())
  end

  test "dispatches Flow lineage query commands through Rust AST" do
    partition = uid("tenant")
    root = uid("flow-command-root")
    child = uid("flow-command-child")
    correlation = uid("order")

    assert %{"id" => ^root} =
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

    assert %{"id" => ^child} =
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
               [root, "PARTITION", partition, "COUNT", "10"],
               MockStore.make()
             )

    assert [%{"id" => ^root}, %{"id" => ^child}] =
             Dispatcher.dispatch(
               "FLOW.BY_ROOT",
               [root, "PARTITION", partition, "COUNT", "10"],
               MockStore.make()
             )

    assert [%{"id" => ^root}, %{"id" => ^child}] =
             Dispatcher.dispatch(
               "FLOW.BY_CORRELATION",
               [correlation, "PARTITION", partition, "COUNT", "10"],
               MockStore.make()
             )
  end

  test "dispatches Flow reclaim through Rust AST" do
    id = uid("flow-command-reclaim")

    assert %{"id" => ^id} =
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

  test "dispatches Flow create_many through Rust AST" do
    partition = uid("tenant")
    type = uid("flow-command-bulk")
    id_a = uid("flow-command-bulk-a")
    id_b = uid("flow-command-bulk-b")

    assert [
             %{"id" => ^id_a, "type" => ^type, "partition_key" => ^partition},
             %{"id" => ^id_b, "type" => ^type, "partition_key" => ^partition}
           ] =
             Dispatcher.dispatch(
               "FLOW.CREATE_MANY",
               [
                 partition,
                 "TYPE",
                 type,
                 "RUN_AT",
                 "1000",
                 "NOW",
                 "1000",
                 "ITEMS",
                 id_a,
                 "payload:" <> id_a,
                 id_b,
                 "payload:" <> id_b
               ],
               MockStore.make()
             )

    assert [%{"id" => ^id_a}, %{"id" => ^id_b}] =
             Dispatcher.dispatch(
               "FLOW.CLAIM_DUE",
               [type, "WORKER", "worker-a", "LIMIT", "10", "NOW", "1000", "PARTITION", partition],
               MockStore.make()
             )
  end

  test "dispatches idempotent Flow create_many retry through Rust AST" do
    partition = uid("tenant")
    type = uid("flow-command-bulk-idempotent")
    id_a = uid("flow-command-bulk-a")
    id_b = uid("flow-command-bulk-b")

    args = [
      partition,
      "TYPE",
      type,
      "RUN_AT",
      "1000",
      "NOW",
      "1000",
      "IDEMPOTENT",
      "true",
      "ITEMS",
      id_a,
      "payload:" <> id_a,
      id_b,
      "payload:" <> id_b
    ]

    assert [%{"id" => ^id_a}, %{"id" => ^id_b}] =
             Dispatcher.dispatch("FLOW.CREATE_MANY", args, MockStore.make())

    assert [%{"id" => ^id_a}, %{"id" => ^id_b}] =
             Dispatcher.dispatch("FLOW.CREATE_MANY", args, MockStore.make())
  end

  test "dispatches mixed-partition Flow create_many through Rust AST" do
    type = uid("flow-command-mixed")
    partition_a = uid("device-a")
    partition_b = uid("device-b")
    id_a = uid("flow-command-mixed-a")
    id_b = uid("flow-command-mixed-b")

    assert [
             %{"id" => ^id_a, "type" => ^type, "partition_key" => ^partition_a},
             %{"id" => ^id_b, "type" => ^type, "partition_key" => ^partition_b}
           ] =
             Dispatcher.dispatch(
               "FLOW.CREATE_MANY",
               [
                 "MIXED",
                 "TYPE",
                 type,
                 "RUN_AT",
                 "1000",
                 "NOW",
                 "1000",
                 "ITEMS",
                 id_a,
                 partition_a,
                 "payload:" <> id_a,
                 id_b,
                 partition_b,
                 "payload:" <> id_b
               ],
               MockStore.make()
             )
  end

  test "dispatches Flow claim and complete through Rust AST" do
    id = uid("flow-command-complete")

    assert %{"id" => ^id} =
             Dispatcher.dispatch(
               "FLOW.CREATE",
               [id, "TYPE", "email", "RUN_AT", "1000"],
               MockStore.make()
             )

    assert [
             %{
               "id" => ^id,
               "state" => "running",
               "lease_token" => lease_token,
               "fencing_token" => fencing_token
             }
           ] =
             Dispatcher.dispatch(
               "FLOW.CLAIM_DUE",
               ["email", "WORKER", "worker-a", "LEASE_MS", "30000", "LIMIT", "1", "NOW", "1000"],
               MockStore.make()
             )

    assert %{"id" => ^id, "state" => "completed", "result_ref" => "result:1"} =
             Dispatcher.dispatch(
               "FLOW.COMPLETE",
               [
                 id,
                 lease_token,
                 "FENCING",
                 Integer.to_string(fencing_token),
                 "RESULT_REF",
                 "result:1"
               ],
               MockStore.make()
             )
  end

  test "dispatches Flow complete_many through Rust AST" do
    partition = uid("flow-command-complete-many-tenant")
    type = uid("flow-command-complete-many")
    id_a = uid("flow-command-complete-many-a")
    id_b = uid("flow-command-complete-many-b")

    assert [_a, _b] =
             Dispatcher.dispatch(
               "FLOW.CREATE_MANY",
               [
                 partition,
                 "TYPE",
                 type,
                 "RUN_AT",
                 "1000",
                 "NOW",
                 "1000",
                 "ITEMS",
                 id_a,
                 "payload:" <> id_a,
                 id_b,
                 "payload:" <> id_b
               ],
               MockStore.make()
             )

    assert [
             %{"id" => ^id_a, "lease_token" => lease_a, "fencing_token" => fencing_a},
             %{"id" => ^id_b, "lease_token" => lease_b, "fencing_token" => fencing_b}
           ] =
             Dispatcher.dispatch(
               "FLOW.CLAIM_DUE",
               [type, "WORKER", "worker-a", "LIMIT", "2", "NOW", "1000", "PARTITION", partition],
               MockStore.make()
             )

    assert [
             %{"id" => ^id_a, "state" => "completed", "result_ref" => "result:batch"},
             %{"id" => ^id_b, "state" => "completed", "result_ref" => "result:batch"}
           ] =
             Dispatcher.dispatch(
               "FLOW.COMPLETE_MANY",
               [
                 partition,
                 "RESULT_REF",
                 "result:batch",
                 "NOW",
                 "2000",
                 "ITEMS",
                 id_a,
                 lease_a,
                 Integer.to_string(fencing_a),
                 id_b,
                 lease_b,
                 Integer.to_string(fencing_b)
               ],
               MockStore.make()
             )
  end

  test "dispatches Flow fail_many through Rust AST" do
    partition = uid("flow-command-fail-many-tenant")
    type = uid("flow-command-fail-many")
    id_a = uid("flow-command-fail-many-a")
    id_b = uid("flow-command-fail-many-b")

    assert [_a, _b] =
             Dispatcher.dispatch(
               "FLOW.CREATE_MANY",
               [
                 partition,
                 "TYPE",
                 type,
                 "RUN_AT",
                 "1000",
                 "NOW",
                 "1000",
                 "ITEMS",
                 id_a,
                 "payload:" <> id_a,
                 id_b,
                 "payload:" <> id_b
               ],
               MockStore.make()
             )

    assert [
             %{"id" => ^id_a, "lease_token" => lease_a, "fencing_token" => fencing_a},
             %{"id" => ^id_b, "lease_token" => lease_b, "fencing_token" => fencing_b}
           ] =
             Dispatcher.dispatch(
               "FLOW.CLAIM_DUE",
               [type, "WORKER", "worker-a", "LIMIT", "2", "NOW", "1000", "PARTITION", partition],
               MockStore.make()
             )

    assert [
             %{"id" => ^id_a, "state" => "failed", "error_ref" => "error:batch"},
             %{"id" => ^id_b, "state" => "failed", "error_ref" => "error:batch"}
           ] =
             Dispatcher.dispatch(
               "FLOW.FAIL_MANY",
               [
                 partition,
                 "ERROR_REF",
                 "error:batch",
                 "NOW",
                 "2000",
                 "ITEMS",
                 id_a,
                 lease_a,
                 Integer.to_string(fencing_a),
                 id_b,
                 lease_b,
                 Integer.to_string(fencing_b)
               ],
               MockStore.make()
             )
  end

  test "dispatches Flow retry_many through Rust AST" do
    partition = uid("flow-command-retry-many-tenant")
    type = uid("flow-command-retry-many")
    id_a = uid("flow-command-retry-many-a")
    id_b = uid("flow-command-retry-many-b")

    assert [_a, _b] =
             Dispatcher.dispatch(
               "FLOW.CREATE_MANY",
               [
                 partition,
                 "TYPE",
                 type,
                 "RUN_AT",
                 "1000",
                 "NOW",
                 "1000",
                 "ITEMS",
                 id_a,
                 "payload:" <> id_a,
                 id_b,
                 "payload:" <> id_b
               ],
               MockStore.make()
             )

    assert [
             %{"id" => ^id_a, "lease_token" => lease_a, "fencing_token" => fencing_a},
             %{"id" => ^id_b, "lease_token" => lease_b, "fencing_token" => fencing_b}
           ] =
             Dispatcher.dispatch(
               "FLOW.CLAIM_DUE",
               [type, "WORKER", "worker-a", "LIMIT", "2", "NOW", "1000", "PARTITION", partition],
               MockStore.make()
             )

    assert [
             %{"id" => ^id_a, "state" => "queued", "error_ref" => "retry:batch"},
             %{"id" => ^id_b, "state" => "queued", "error_ref" => "retry:batch"}
           ] =
             Dispatcher.dispatch(
               "FLOW.RETRY_MANY",
               [
                 partition,
                 "ERROR_REF",
                 "retry:batch",
                 "RUN_AT",
                 "2000",
                 "NOW",
                 "2000",
                 "ITEMS",
                 id_a,
                 lease_a,
                 Integer.to_string(fencing_a),
                 id_b,
                 lease_b,
                 Integer.to_string(fencing_b)
               ],
               MockStore.make()
             )
  end

  test "dispatches Flow rewind through Rust AST" do
    id = uid("flow-command-rewind")

    assert %{"id" => ^id} =
             Dispatcher.dispatch(
               "FLOW.CREATE",
               [id, "TYPE", "email", "RUN_AT", "1000", "NOW", "1000"],
               MockStore.make()
             )

    assert [[created_event_id, _fields]] =
             Dispatcher.dispatch("FLOW.HISTORY", [id, "COUNT", "10"], MockStore.make())

    assert [%{"lease_token" => lease_token, "fencing_token" => fencing_token}] =
             Dispatcher.dispatch(
               "FLOW.CLAIM_DUE",
               ["email", "WORKER", "worker-a", "LEASE_MS", "30000", "LIMIT", "1", "NOW", "1000"],
               MockStore.make()
             )

    assert %{"state" => "completed"} =
             Dispatcher.dispatch(
               "FLOW.COMPLETE",
               [id, lease_token, "FENCING", Integer.to_string(fencing_token), "NOW", "2000"],
               MockStore.make()
             )

    assert %{"id" => ^id, "state" => "queued", "next_run_at_ms" => 5000} =
             Dispatcher.dispatch(
               "FLOW.REWIND",
               [
                 id,
                 "TO_EVENT",
                 created_event_id,
                 "RUN_AT",
                 "5000",
                 "EXPECT_STATE",
                 "completed",
                 "NOW",
                 "3000"
               ],
               MockStore.make()
             )
  end

  test "dispatches Flow AST parse errors without calling embedded API" do
    assert {:error, "ERR flow limit must be a positive integer"} =
             Dispatcher.dispatch_ast(
               {:flow_claim_due, "checkout",
                {:error, "ERR flow limit must be a positive integer"}},
               MockStore.make()
             )

    assert {:error, "ERR wrong number of arguments for 'flow.complete' command"} =
             Dispatcher.dispatch_ast(
               {:flow_complete,
                {:error, "ERR wrong number of arguments for 'flow.complete' command"}},
               MockStore.make()
             )

    assert {:error, "ERR flow to_event is required"} =
             Dispatcher.dispatch_ast(
               {:flow_rewind, "flow", {:error, "ERR flow to_event is required"}},
               MockStore.make()
             )
  end

  test "Flow commands are visible in COMMAND catalog" do
    names = Dispatcher.dispatch("COMMAND", ["LIST"], MockStore.make())
    assert "flow.create" in names
    assert "flow.create_many" in names
    assert "flow.claim_due" in names
    assert "flow.complete" in names
    assert "flow.rewind" in names

    assert [["flow.create", _arity, flags, 1, 1, 1]] =
             Dispatcher.dispatch("COMMAND", ["INFO", "flow.create"], MockStore.make())

    assert "write" in flags
    assert {:ok, ["flow-id"]} = Ferricstore.Commands.Catalog.get_keys("flow.create", ["flow-id"])
  end
end
