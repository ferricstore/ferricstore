defmodule Ferricstore.Commands.FlowTest.Sections.Part02 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.Dispatcher
      alias Ferricstore.Test.{MockStore, ShardHelpers}

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
    end
  end
end
