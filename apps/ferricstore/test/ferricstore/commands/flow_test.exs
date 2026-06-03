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

  test "dispatches Flow create_many through Rust AST" do
    partition = uid("tenant")
    type = uid("flow-command-bulk")
    id_a = uid("flow-command-bulk-a")
    id_b = uid("flow-command-bulk-b")

    assert "OK" =
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

  test "dispatches cold auto-bucket create_many claim_due through Rust AST" do
    ctx = FerricStore.Instance.get(:default)
    type = uid("flow-command-cold-auto-bulk") <> ":bench"
    id_a = uid("flow-command-cold-auto-a")
    id_b = uid("flow-command-cold-auto-b")
    partition = Ferricstore.Flow.Keys.auto_partition_key(id_a)

    assert ["OK", "OK"] =
             Dispatcher.dispatch(
               "FLOW.CREATE_MANY",
               [
                 partition,
                 "TYPE",
                 type,
                 "STATE",
                 "queued",
                 "RUN_AT",
                 "302000",
                 "NOW",
                 "1000",
                 "PRIORITY",
                 "0",
                 "INDEPENDENT",
                 "true",
                 "RETENTION_TTL_MS",
                 "300000",
                 "ITEMS",
                 id_a,
                 "payload:" <> id_a,
                 id_b,
                 "payload:" <> id_b
               ],
               MockStore.make()
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

    assert [job_a | _] =
             Dispatcher.dispatch(
               "FLOW.CLAIM_DUE",
               [
                 type,
                 "WORKER",
                 "worker-cold-auto",
                 "LIMIT",
                 "10",
                 "NOW",
                 "302000",
                 "PARTITIONS",
                 "1",
                 partition,
                 "RETURN",
                 "JOBS_COMPACT_STATE"
               ],
               MockStore.make()
             )

    assert [^id_a, ^partition, lease_token, fencing_token, "queued"] = job_a
    assert is_binary(lease_token)
    assert is_integer(fencing_token)
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

    assert "OK" = Dispatcher.dispatch("FLOW.CREATE_MANY", args, MockStore.make())

    assert "OK" = Dispatcher.dispatch("FLOW.CREATE_MANY", args, MockStore.make())
  end

  test "dispatches mixed-partition Flow create_many through Rust AST" do
    type = uid("flow-command-mixed")
    partition_a = uid("device-a")
    partition_b = uid("device-b")
    id_a = uid("flow-command-mixed-a")
    id_b = uid("flow-command-mixed-b")

    assert "OK" =
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

    assert "OK" =
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

    assert "OK" =
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

    %{"id" => ^id, "state" => "completed", "result_ref" => result_ref} =
      Dispatcher.dispatch("FLOW.GET", [id], MockStore.make())

    assert is_binary(result_ref)
    assert result_ref != "result:1"
  end

  test "dispatches Flow complete_many through Rust AST" do
    partition = uid("flow-command-complete-many-tenant")
    type = uid("flow-command-complete-many")
    id_a = uid("flow-command-complete-many-a")
    id_b = uid("flow-command-complete-many-b")

    assert "OK" =
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

    assert "OK" =
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

    %{"id" => ^id_a, "state" => "completed", "result_ref" => result_ref_a} =
      Dispatcher.dispatch("FLOW.GET", [id_a, "PARTITION", partition], MockStore.make())

    %{"id" => ^id_b, "state" => "completed", "result_ref" => result_ref_b} =
      Dispatcher.dispatch("FLOW.GET", [id_b, "PARTITION", partition], MockStore.make())

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

    assert "OK" =
             Dispatcher.dispatch(
               "FLOW.CREATE",
               [id, "TYPE", "cancel-reason", "PARTITION", partition],
               MockStore.make()
             )

    %{"fencing_token" => fencing_token} =
      Dispatcher.dispatch("FLOW.GET", [id, "PARTITION", partition], MockStore.make())

    assert "OK" =
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

    %{"id" => ^id, "state" => "cancelled", "error_ref" => error_ref} =
      Dispatcher.dispatch("FLOW.GET", [id, "PARTITION", partition], MockStore.make())

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

    assert "OK" =
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

    assert "OK" =
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

    %{"id" => ^id_a, "state" => "failed", "error_ref" => error_ref_a} =
      Dispatcher.dispatch("FLOW.GET", [id_a, "PARTITION", partition], MockStore.make())

    %{"id" => ^id_b, "state" => "failed", "error_ref" => error_ref_b} =
      Dispatcher.dispatch("FLOW.GET", [id_b, "PARTITION", partition], MockStore.make())

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

    assert "OK" =
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

    assert "OK" =
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

    %{"id" => ^id_a, "state" => "queued", "error_ref" => error_ref_a} =
      Dispatcher.dispatch("FLOW.GET", [id_a, "PARTITION", partition], MockStore.make())

    %{"id" => ^id_b, "state" => "queued", "error_ref" => error_ref_b} =
      Dispatcher.dispatch("FLOW.GET", [id_b, "PARTITION", partition], MockStore.make())

    assert is_binary(error_ref_a)
    assert is_binary(error_ref_b)
    assert error_ref_a != "retry:batch"
    assert error_ref_b != "retry:batch"
  end

  test "dispatches Flow retry policy override through Rust AST" do
    type = uid("flow-command-retry-policy")
    id = uid("flow-command-retry-policy-id")

    assert "OK" =
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

    assert "OK" =
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

    assert %{"id" => ^id, "state" => "payment_failed"} =
             Dispatcher.dispatch("FLOW.GET", [id], MockStore.make())
  end

  test "dispatches Flow claim_due with job-only return through Rust AST" do
    type = uid("flow-command-claim-jobs")
    id = uid("flow-command-claim-jobs-id")

    assert "OK" =
             Dispatcher.dispatch(
               "FLOW.CREATE",
               [id, "TYPE", type, "STATE", "queued", "RUN_AT", "1000", "NOW", "1000"],
               MockStore.make()
             )

    assert [
             %{
               "id" => ^id,
               "type" => ^type,
               "state" => "running",
               "run_state" => "queued",
               "lease_token" => lease_token,
               "fencing_token" => fencing_token
             } = job
           ] =
             Dispatcher.dispatch(
               "FLOW.CLAIM_DUE",
               [
                 type,
                 "STATE",
                 "queued",
                 "WORKER",
                 "worker-a",
                 "LIMIT",
                 "1",
                 "NOW",
                 "1000",
                 "RETURN",
                 "JOBS"
               ],
               MockStore.make()
             )

    assert is_binary(lease_token)
    assert is_integer(fencing_token)
    refute Map.has_key?(job, "version")
  end

  test "dispatches Flow claim_due with compact job-only return through Rust AST" do
    type = uid("flow-command-claim-jobs-compact")
    id = uid("flow-command-claim-jobs-compact-id")
    partition_key = uid("tenant")

    assert "OK" =
             Dispatcher.dispatch(
               "FLOW.CREATE",
               [
                 id,
                 "TYPE",
                 type,
                 "STATE",
                 "queued",
                 "PARTITION",
                 partition_key,
                 "RUN_AT",
                 "1000",
                 "NOW",
                 "1000"
               ],
               MockStore.make()
             )

    assert [[^id, ^partition_key, lease_token, fencing_token]] =
             Dispatcher.dispatch(
               "FLOW.CLAIM_DUE",
               [
                 type,
                 "STATE",
                 "queued",
                 "WORKER",
                 "worker-a",
                 "LIMIT",
                 "1",
                 "NOW",
                 "1000",
                 "PARTITION",
                 partition_key,
                 "RETURN",
                 "JOBS_COMPACT"
               ],
               MockStore.make()
             )

    assert is_binary(lease_token)
    assert is_integer(fencing_token)
  end

  test "dispatches Flow claim_due with compact job-state return through Rust AST" do
    type = uid("flow-command-claim-jobs-compact-state")
    id = uid("flow-command-claim-jobs-compact-state-id")
    partition_key = uid("tenant")

    assert "OK" =
             Dispatcher.dispatch(
               "FLOW.CREATE",
               [
                 id,
                 "TYPE",
                 type,
                 "STATE",
                 "ready",
                 "PARTITION",
                 partition_key,
                 "RUN_AT",
                 "1000",
                 "NOW",
                 "1000"
               ],
               MockStore.make()
             )

    assert [[^id, ^partition_key, lease_token, fencing_token, "ready"]] =
             Dispatcher.dispatch(
               "FLOW.CLAIM_DUE",
               [
                 type,
                 "WORKER",
                 "worker-a",
                 "LIMIT",
                 "1",
                 "NOW",
                 "1000",
                 "PARTITION",
                 partition_key,
                 "RETURN",
                 "JOBS_COMPACT_STATE"
               ],
               MockStore.make()
             )

    assert is_binary(lease_token)
    assert is_integer(fencing_token)
  end

  test "dispatches Flow claim_due with any partition and repeated states through Rust AST" do
    type = uid("flow-command-claim-any")
    partition = uid("tenant")
    queued_id = uid("flow-command-claim-any-queued")
    ready_id = uid("flow-command-claim-any-ready")
    held_id = uid("flow-command-claim-any-held")

    assert "OK" =
             Dispatcher.dispatch(
               "FLOW.CREATE",
               [queued_id, "TYPE", type, "RUN_AT", "1000", "NOW", "1000"],
               MockStore.make()
             )

    assert "OK" =
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

    assert "OK" =
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

    assert "OK" =
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

    assert "OK" =
             Dispatcher.dispatch(
               "FLOW.CANCEL_MANY",
               [
                 partition,
                 "REASON",
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

    %{"id" => ^id_a, "state" => "cancelled", "error_ref" => error_ref_a} =
      Dispatcher.dispatch("FLOW.GET", [id_a, "PARTITION", partition], MockStore.make())

    %{"id" => ^id_b, "state" => "cancelled", "error_ref" => error_ref_b} =
      Dispatcher.dispatch("FLOW.GET", [id_b, "PARTITION", partition], MockStore.make())

    assert is_binary(error_ref_a)
    assert is_binary(error_ref_b)
    assert error_ref_a != "cancel:batch"
    assert error_ref_b != "cancel:batch"
  end

  test "dispatches Flow rewind through Rust AST" do
    id = uid("flow-command-rewind")

    assert "OK" =
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

    assert "OK" =
             Dispatcher.dispatch(
               "FLOW.COMPLETE",
               [id, lease_token, "FENCING", Integer.to_string(fencing_token), "NOW", "2000"],
               MockStore.make()
             )

    assert "OK" =
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

    assert %{"id" => ^id, "state" => "queued", "next_run_at_ms" => 5000} =
             Dispatcher.dispatch("FLOW.GET", [id], MockStore.make())
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
                  retention: [ttl_ms: 60_000, history_max_events: 320],
                  retry: [
                    max_retries: 5,
                    backoff: [kind: :fixed, base_ms: 1_000, max_ms: 5_000, jitter_pct: 0],
                    exhausted_to: "failed"
                  ],
                  states: %{
                    "charge_card" => [
                      retry: [max_retries: 1, exhausted_to: "payment_failed"],
                      retention: [
                        ttl_ms: 30_000,
                        history_max_events: 160
                      ]
                    ]
                  }
                ]},
               MockStore.make()
             )

    assert %{
             "retention" => %{
               "ttl_ms" => 60_000,
               "history_max_events" => 320
             }
           } =
             Dispatcher.dispatch_ast({:flow_policy_get, type, []}, MockStore.make())

    refute Map.has_key?(
             Dispatcher.dispatch_ast({:flow_policy_get, type, []}, MockStore.make())["retention"],
             "history_hot_max_events"
           )

    assert %{
             "type" => ^type,
             "state" => "charge_card",
             "retry" => %{"max_retries" => 1, "exhausted_to" => "payment_failed"},
             "retention" => %{
               "ttl_ms" => 30_000,
               "history_max_events" => 160
             }
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
                 "HISTORY_MAX_EVENTS",
                 "320",
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
                 "HISTORY_MAX_EVENTS",
                 "160"
               ],
               MockStore.make()
             )

    assert %{
             "retention" => %{
               "ttl_ms" => 60_000,
               "history_max_events" => 320
             }
           } =
             Dispatcher.dispatch("FLOW.POLICY.GET", [type], MockStore.make())

    refute Map.has_key?(
             Dispatcher.dispatch("FLOW.POLICY.GET", [type], MockStore.make())["retention"],
             "history_hot_max_events"
           )

    assert %{
             "type" => ^type,
             "state" => "charge_card",
             "retry" => %{"max_retries" => 1, "exhausted_to" => "payment_failed"},
             "retention" => %{
               "ttl_ms" => 30_000,
               "history_max_events" => 160
             }
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

    assert {:error, "ERR flow retention history_hot_max_events is internal"} =
             Dispatcher.dispatch(
               "FLOW.POLICY.SET",
               [type, "HISTORY_HOT_MAX_EVENTS", "1"],
               MockStore.make()
             )
  end

  test "Flow commands are visible in COMMAND catalog" do
    names = Dispatcher.dispatch("COMMAND", ["LIST"], MockStore.make())
    assert "flow.create" in names
    assert "flow.create_many" in names
    assert "flow.spawn_children" in names
    assert "flow.policy.set" in names
    assert "flow.policy.get" in names
    assert "flow.retention_cleanup" in names
    assert "flow.claim_due" in names
    assert "flow.failures" in names
    assert "flow.terminals" in names
    assert "flow.complete" in names
    assert "flow.rewind" in names

    assert [["flow.create", _arity, flags, 1, 1, 1]] =
             Dispatcher.dispatch("COMMAND", ["INFO", "flow.create"], MockStore.make())

    assert "write" in flags
    assert {:ok, ["flow-id"]} = Ferricstore.Commands.Catalog.get_keys("flow.create", ["flow-id"])

    assert {:ok, ["checkout"]} =
             Ferricstore.Commands.Catalog.get_keys("flow.policy.set", ["checkout"])

    assert [["flow.retention_cleanup", _arity, cleanup_flags, 0, 0, 0]] =
             Dispatcher.dispatch("COMMAND", ["INFO", "flow.retention_cleanup"], MockStore.make())

    assert "admin" in cleanup_flags
  end
end
