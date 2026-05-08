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

  test "rejects public Flow value ref inputs through Rust AST" do
    assert {:error, "ERR syntax error"} =
             Dispatcher.dispatch(
               "FLOW.CREATE",
               ["ref-create", "TYPE", "checkout", "PAYLOAD_REF", "payload:external"],
               MockStore.make()
             )

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
                 "RETENTION_TTL",
                 "60000",
                 "HISTORY_HOT_MAX_EVENTS",
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

    assert [[_event_id, %{"event" => "created", "version" => "1"}]] =
             Dispatcher.dispatch(
               "FLOW.HISTORY",
               [id, "COUNT", "10", "INCLUDE_COLD", "true", "CONSISTENT_PROJECTION", "false"],
               MockStore.make()
             )
  end

  test "dispatches Flow full values through Rust AST" do
    id = uid("flow-command-values")

    assert %{"id" => ^id, "payload_ref" => payload_ref} =
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

    assert %{"state" => "completed", "result_ref" => result_ref} =
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

  test "dispatches Flow mutation values through Rust AST" do
    transition_id = uid("flow-command-transition-value")
    fail_id = uid("flow-command-fail-value")

    assert %{"id" => ^transition_id} =
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

    assert %{"state" => "waiting", "payload_ref" => transition_payload_ref} =
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

    assert %{
             "state" => "waiting",
             "error_ref" => retry_error_ref,
             "payload_ref" => retry_payload_ref
           } =
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

    assert retry_error_ref != "retry-error"
    assert retry_payload_ref != "retry-payload"

    assert %{"error" => "retry-error", "payload" => "retry-payload"} =
             Dispatcher.dispatch("FLOW.GET", [transition_id, "FULL"], MockStore.make())

    assert %{"id" => ^fail_id} =
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
               ["mutation-values", "WORKER", "worker-c", "LEASE_MS", "30000", "NOW", "4000"],
               MockStore.make()
             )

    assert %{"state" => "failed", "error_ref" => fail_error_ref} =
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

    assert fail_error_ref != "fail-error"

    assert %{"payload" => "fail-payload", "error" => "fail-error"} =
             Dispatcher.dispatch("FLOW.GET", [fail_id, "FULL"], MockStore.make())
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

  test "dispatches Flow claim_due reclaim controls through Rust AST" do
    type = uid("flow-command-claim-reclaim-ratio")
    expired_id = uid("flow-command-expired")
    fresh_id = uid("flow-command-fresh")

    assert %{"id" => ^expired_id} =
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

    assert %{"id" => ^fresh_id} =
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

    assert [
             %{"id" => ^id_a, "payload" => "payload:" <> _},
             %{"id" => ^id_b, "payload" => "payload:" <> _}
           ] =
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

    assert %{"id" => ^id, "state" => "completed", "result_ref" => result_ref} =
             Dispatcher.dispatch(
               "FLOW.COMPLETE",
               [
                 id,
                 lease_token,
                 "FENCING",
                 Integer.to_string(fencing_token),
                 "RESULT",
                 "result:1"
               ],
               MockStore.make()
             )

    assert is_binary(result_ref)
    assert result_ref != "result:1"
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
             %{"id" => ^id_a, "state" => "completed", "result_ref" => result_ref_a},
             %{"id" => ^id_b, "state" => "completed", "result_ref" => result_ref_b}
           ] =
             Dispatcher.dispatch(
               "FLOW.COMPLETE_MANY",
               [
                 partition,
                 "RESULT",
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

    assert is_binary(result_ref_a)
    assert is_binary(result_ref_b)
    assert result_ref_a != "result:batch"
    assert result_ref_b != "result:batch"
  end

  test "rejects terminal Flow TTL zero through Rust AST" do
    assert {:error, "ERR flow ttl_ms must be a positive integer"} =
             Dispatcher.dispatch(
               "FLOW.COMPLETE",
               ["flow-complete-ttl-zero", "lease", "FENCING", "1", "TTL", "0"],
               MockStore.make()
             )

    assert {:error, "ERR flow ttl_ms must be a positive integer"} =
             Dispatcher.dispatch(
               "FLOW.FAIL",
               ["flow-fail-ttl-zero", "lease", "FENCING", "1", "TTL", "0"],
               MockStore.make()
             )

    assert {:error, "ERR flow ttl_ms must be a positive integer"} =
             Dispatcher.dispatch(
               "FLOW.CANCEL",
               ["flow-cancel-ttl-zero", "FENCING", "1", "TTL", "0"],
               MockStore.make()
             )

    assert {:error, "ERR syntax error"} =
             Dispatcher.dispatch(
               "FLOW.CANCEL",
               ["flow-cancel-error-option", "FENCING", "1", "ERROR", "bad"],
               MockStore.make()
             )

    assert {:error, "ERR flow ttl_ms must be a positive integer"} =
             Dispatcher.dispatch(
               "FLOW.COMPLETE_MANY",
               ["tenant", "TTL", "0", "ITEMS", "flow-a", "lease-a", "1"],
               MockStore.make()
             )

    assert {:error, "ERR flow ttl_ms must be a positive integer"} =
             Dispatcher.dispatch(
               "FLOW.FAIL_MANY",
               ["tenant", "TTL", "0", "ITEMS", "flow-a", "lease-a", "1"],
               MockStore.make()
             )

    assert {:error, "ERR flow ttl_ms must be a positive integer"} =
             Dispatcher.dispatch(
               "FLOW.CANCEL_MANY",
               ["tenant", "TTL", "0", "ITEMS", "flow-a", "1"],
               MockStore.make()
             )
  end

  test "dispatches Flow cancel inline reason through Rust AST" do
    id = uid("flow-command-cancel-reason")
    partition = uid("flow-command-cancel-reason-tenant")
    reason = "cancelled by operator"

    assert %{"id" => ^id, "fencing_token" => fencing_token} =
             Dispatcher.dispatch(
               "FLOW.CREATE",
               [id, "TYPE", "cancel-reason", "PARTITION", partition],
               MockStore.make()
             )

    assert %{"id" => ^id, "state" => "cancelled", "error_ref" => error_ref} =
             Dispatcher.dispatch(
               "FLOW.CANCEL",
               [
                 id,
                 "FENCING",
                 Integer.to_string(fencing_token),
                 "PARTITION",
                 partition,
                 "REASON",
                 reason
               ],
               MockStore.make()
             )

    assert is_binary(error_ref)
    assert error_ref != reason

    assert {:ok, fetched} = FerricStore.flow_get(id, partition_key: partition, full: true)
    assert fetched.error == reason
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
             %{"id" => ^id_a, "state" => "failed", "error_ref" => error_ref_a},
             %{"id" => ^id_b, "state" => "failed", "error_ref" => error_ref_b}
           ] =
             Dispatcher.dispatch(
               "FLOW.FAIL_MANY",
               [
                 partition,
                 "ERROR",
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

    assert is_binary(error_ref_a)
    assert is_binary(error_ref_b)
    assert error_ref_a != "error:batch"
    assert error_ref_b != "error:batch"
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
             %{"id" => ^id_a, "state" => "queued", "error_ref" => error_ref_a},
             %{"id" => ^id_b, "state" => "queued", "error_ref" => error_ref_b}
           ] =
             Dispatcher.dispatch(
               "FLOW.RETRY_MANY",
               [
                 partition,
                 "ERROR",
                 "retry:batch",
                 "RUN_AT",
                 "2000",
                 "NOW",
                 "2000",
                 "MAX_RETRIES",
                 "5",
                 "EXHAUSTED_TO",
                 "failed",
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

    assert is_binary(error_ref_a)
    assert is_binary(error_ref_b)
    assert error_ref_a != "retry:batch"
    assert error_ref_b != "retry:batch"
  end

  test "dispatches Flow retry policy override through Rust AST" do
    type = uid("flow-command-retry-policy")
    id = uid("flow-command-retry-policy-id")

    assert %{"id" => ^id} =
             Dispatcher.dispatch(
               "FLOW.CREATE",
               [id, "TYPE", type, "STATE", "charge_card", "RUN_AT", "1000", "NOW", "1000"],
               MockStore.make()
             )

    assert [%{"lease_token" => lease_token, "fencing_token" => fencing_token}] =
             Dispatcher.dispatch(
               "FLOW.CLAIM_DUE",
               [type, "STATE", "charge_card", "WORKER", "worker-a", "LIMIT", "1", "NOW", "1000"],
               MockStore.make()
             )

    assert %{"id" => ^id, "state" => "payment_failed"} =
             Dispatcher.dispatch(
               "FLOW.RETRY",
               [
                 id,
                 lease_token,
                 "FENCING",
                 Integer.to_string(fencing_token),
                 "NOW",
                 "2000",
                 "MAX_RETRIES",
                 "0",
                 "EXHAUSTED_TO",
                 "payment_failed"
               ],
               MockStore.make()
             )
  end

  test "dispatches Flow claim_due with any partition and repeated states through Rust AST" do
    type = uid("flow-command-claim-any")
    partition = uid("tenant")
    queued_id = uid("flow-command-claim-any-queued")
    ready_id = uid("flow-command-claim-any-ready")
    held_id = uid("flow-command-claim-any-held")

    assert %{"id" => ^queued_id} =
             Dispatcher.dispatch(
               "FLOW.CREATE",
               [queued_id, "TYPE", type, "RUN_AT", "1000", "NOW", "1000"],
               MockStore.make()
             )

    assert %{"id" => ^ready_id} =
             Dispatcher.dispatch(
               "FLOW.CREATE",
               [
                 ready_id,
                 "TYPE",
                 type,
                 "STATE",
                 "ready",
                 "RUN_AT",
                 "1000",
                 "NOW",
                 "1000",
                 "PARTITION",
                 partition
               ],
               MockStore.make()
             )

    assert %{"id" => ^held_id} =
             Dispatcher.dispatch(
               "FLOW.CREATE",
               [
                 held_id,
                 "TYPE",
                 type,
                 "STATE",
                 "held",
                 "RUN_AT",
                 "1000",
                 "NOW",
                 "1000",
                 "PARTITION",
                 partition
               ],
               MockStore.make()
             )

    claimed =
      Dispatcher.dispatch(
        "FLOW.CLAIM_DUE",
        [
          type,
          "WORKER",
          "worker-a",
          "LIMIT",
          "10",
          "NOW",
          "1000",
          "PARTITION",
          "ANY",
          "STATE",
          "queued",
          "STATE",
          "ready"
        ],
        MockStore.make()
      )

    assert MapSet.new(Enum.map(claimed, & &1["id"])) == MapSet.new([queued_id, ready_id])
  end

  test "dispatches Flow cancel_many through Rust AST" do
    partition = uid("flow-command-cancel-many-tenant")
    type = uid("flow-command-cancel-many")
    id_a = uid("flow-command-cancel-many-a")
    id_b = uid("flow-command-cancel-many-b")

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
             %{"id" => ^id_a, "state" => "cancelled", "error_ref" => "cancel:batch"},
             %{"id" => ^id_b, "state" => "cancelled", "error_ref" => "cancel:batch"}
           ] =
             Dispatcher.dispatch(
               "FLOW.CANCEL_MANY",
               [
                 partition,
                 "REASON_REF",
                 "cancel:batch",
                 "NOW",
                 "2000",
                 "ITEMS",
                 id_a,
                 "0",
                 id_b,
                 "0"
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

  test "dispatches Flow policy AST through embedded API" do
    type = uid("flow-policy-ast")

    assert %{"type" => ^type, "retry" => %{"max_retries" => 5}} =
             Dispatcher.dispatch_ast(
               {:flow_policy_set, type,
                [
                  retention: [ttl_ms: 60_000, history_hot_max_events: 32],
                  retry: [
                    max_retries: 5,
                    backoff: [kind: :fixed, base_ms: 1_000, max_ms: 5_000, jitter_pct: 0],
                    exhausted_to: "failed"
                  ],
                  states: %{
                    "charge_card" => [
                      retry: [max_retries: 1, exhausted_to: "payment_failed"],
                      retention: [ttl_ms: 30_000, history_hot_max_events: 16]
                    ]
                  }
                ]},
               MockStore.make()
             )

    assert %{"retention" => %{"ttl_ms" => 60_000, "history_hot_max_events" => 32}} =
             Dispatcher.dispatch_ast({:flow_policy_get, type, []}, MockStore.make())

    assert %{
             "type" => ^type,
             "state" => "charge_card",
             "retry" => %{"max_retries" => 1, "exhausted_to" => "payment_failed"},
             "retention" => %{"ttl_ms" => 30_000, "history_hot_max_events" => 16}
           } =
             Dispatcher.dispatch_ast(
               {:flow_policy_get, type, [state: "charge_card"]},
               MockStore.make()
             )
  end

  test "dispatches Flow policy commands through Rust AST" do
    type = uid("flow-policy-rust")

    assert %{
             "type" => ^type,
             "retry" => %{
               "max_retries" => 5,
               "backoff" => %{
                 "kind" => :fixed,
                 "base_ms" => 1000,
                 "max_ms" => 5000,
                 "jitter_pct" => 0
               },
               "exhausted_to" => "failed"
             }
           } =
             Dispatcher.dispatch(
               "FLOW.POLICY.SET",
               [
                 type,
                 "MAX_RETRIES",
                 "5",
                 "BACKOFF",
                 "FIXED",
                 "BASE_MS",
                 "1000",
                 "MAX_MS",
                 "5000",
                 "JITTER_PCT",
                 "0",
                 "RETENTION_TTL",
                 "60000",
                 "HISTORY_HOT_MAX_EVENTS",
                 "32",
                 "EXHAUSTED_TO",
                 "failed",
                 "STATE",
                 "charge_card",
                 "MAX_RETRIES",
                 "1",
                 "EXHAUSTED_TO",
                 "payment_failed",
                 "RETENTION_TTL",
                 "30000",
                 "HISTORY_HOT_MAX_EVENTS",
                 "16"
               ],
               MockStore.make()
             )

    assert %{"retention" => %{"ttl_ms" => 60_000, "history_hot_max_events" => 32}} =
             Dispatcher.dispatch("FLOW.POLICY.GET", [type], MockStore.make())

    assert %{
             "type" => ^type,
             "state" => "charge_card",
             "retry" => %{"max_retries" => 1, "exhausted_to" => "payment_failed"},
             "retention" => %{"ttl_ms" => 30_000, "history_hot_max_events" => 16}
           } =
             Dispatcher.dispatch(
               "FLOW.POLICY.GET",
               [type, "STATE", "charge_card"],
               MockStore.make()
             )

    assert {:error, "ERR syntax error"} =
             Dispatcher.dispatch(
               "FLOW.POLICY.SET",
               [type, "STATE", "queued", "MAX_RETRIES"],
               MockStore.make()
             )
  end

  test "Flow commands are visible in COMMAND catalog" do
    names = Dispatcher.dispatch("COMMAND", ["LIST"], MockStore.make())
    assert "flow.create" in names
    assert "flow.create_many" in names
    assert "flow.policy_set" in names
    assert "flow.policy_get" in names
    assert "flow.claim_due" in names
    assert "flow.complete" in names
    assert "flow.rewind" in names

    assert [["flow.create", _arity, flags, 1, 1, 1]] =
             Dispatcher.dispatch("COMMAND", ["INFO", "flow.create"], MockStore.make())

    assert "write" in flags
    assert {:ok, ["flow-id"]} = Ferricstore.Commands.Catalog.get_keys("flow.create", ["flow-id"])
    assert {:ok, []} = Ferricstore.Commands.Catalog.get_keys("flow.policy_set", ["checkout"])
  end
end
