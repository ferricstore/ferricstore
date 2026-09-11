defmodule Ferricstore.Flow.CreateRunningValidationTest do
  use Ferricstore.Test.FlowCase

  @running_error {:error, "ERR flow running state is only entered by FLOW.CLAIM_DUE"}

  test "public create rejects running without reserving the identity or disrupting writes" do
    id = unique_flow_id("invalid-running")
    opts = [type: "running-validation", partition_key: "running-validation"]

    assert FerricStore.flow_create(id, Keyword.put(opts, :state, "running")) == @running_error
    assert {:ok, nil} = FerricStore.flow_get(id, partition_key: "running-validation")
    assert :ok = FerricStore.flow_create(id, Keyword.put(opts, :state, "queued"))

    assert {:ok, %{state: "queued", lease_owner: nil}} =
             FerricStore.flow_get(id, partition_key: "running-validation")
  end

  test "public atomic batch rejects a running item without writing earlier valid items" do
    id = unique_flow_id("valid-batch-item")
    invalid_id = unique_flow_id("invalid-batch-item")
    partition = "running-validation-batch"

    assert FerricStore.flow_create_many(
             partition,
             [%{id: id, state: "queued"}, %{id: invalid_id, state: "running"}],
             type: "running-validation"
           ) == @running_error

    assert {:ok, nil} = FerricStore.flow_get(id, partition_key: partition)
    assert {:ok, nil} = FerricStore.flow_get(invalid_id, partition_key: partition)
  end

  test "the supported atomic start and claim still creates a leased running execution" do
    id = unique_flow_id("valid-start-claim")
    now = System.system_time(:millisecond)

    assert {:ok, record} =
             FerricStore.flow_start_and_claim(id, "running-validation", "queued",
               partition_key: "running-validation-claim",
               worker: "validation-worker",
               now_ms: now,
               lease_ms: 30_000
             )

    assert record.state == "running"
    assert record.run_state == "queued"
    assert record.lease_owner == "validation-worker"
    assert is_binary(record.lease_token)
    assert record.lease_deadline_ms == now + 30_000
  end
end
