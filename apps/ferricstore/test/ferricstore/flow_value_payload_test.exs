defmodule Ferricstore.FlowValuePayloadTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  test "create stores a full payload value and claim/get hydrate it from internal storage" do
    id = unique_id("flow-value-create")
    payload = %{order_id: 123, items: ["book", "pen"]}

    assert {:ok, created} =
             FerricStore.flow_create(id,
               type: "value-payload",
               partition_key: "tenant-a",
               payload: payload,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert is_binary(created.payload_ref)
    assert created.payload_ref != ""

    assert {:ok, fetched} = FerricStore.flow_get(id, partition_key: "tenant-a")
    assert fetched.payload == payload

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("value-payload",
               partition_key: "tenant-a",
               worker: "worker-1",
               limit: 1,
               now_ms: 1_000
             )

    assert claimed.payload == payload
  end

  test "transition can replace payload and retry error preserves current payload" do
    id = unique_id("flow-value-transition")

    assert {:ok, _} =
             FerricStore.flow_create(id,
               type: "value-transition",
               partition_key: "tenant-a",
               payload: "initial",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("value-transition",
               partition_key: "tenant-a",
               worker: "worker-1",
               limit: 1,
               now_ms: 1_000
             )

    assert {:ok, transitioned} =
             FerricStore.flow_transition(id, "running", "waiting",
               partition_key: "tenant-a",
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               payload: %{step: "waiting"},
               run_at_ms: 2_000,
               now_ms: 1_100
             )

    assert is_binary(transitioned.payload_ref)
    assert transitioned.payload_ref != claimed.payload_ref

    assert {:ok, [reclaimed]} =
             FerricStore.flow_claim_due("value-transition",
               partition_key: "tenant-a",
               state: "waiting",
               worker: "worker-2",
               limit: 1,
               now_ms: 2_000
             )

    assert reclaimed.payload == %{step: "waiting"}

    assert {:ok, retried} =
             FerricStore.flow_retry(id, reclaimed.lease_token,
               partition_key: "tenant-a",
               fencing_token: reclaimed.fencing_token,
               error: %{reason: "temporary"},
               run_at_ms: 3_000,
               now_ms: 2_100
             )

    assert retried.payload_ref == reclaimed.payload_ref
    assert is_binary(retried.error_ref)

    assert {:ok, fetched} = FerricStore.flow_get(id, partition_key: "tenant-a")
    assert fetched.payload == %{step: "waiting"}
    assert fetched.error == %{reason: "temporary"}
  end

  test "complete and fail store result/error values without requiring public refs" do
    complete_id = unique_id("flow-value-complete")
    fail_id = unique_id("flow-value-fail")

    assert {:ok, _} =
             FerricStore.flow_create(complete_id,
               type: "value-terminal",
               partition_key: "tenant-a",
               run_at_ms: 1_000
             )

    assert {:ok, _} =
             FerricStore.flow_create(fail_id,
               type: "value-terminal",
               partition_key: "tenant-a",
               run_at_ms: 1_000
             )

    assert {:ok, [complete_claim, fail_claim]} =
             FerricStore.flow_claim_due("value-terminal",
               partition_key: "tenant-a",
               worker: "worker-1",
               limit: 2,
               now_ms: 1_000
             )

    assert {:ok, completed} =
             FerricStore.flow_complete(complete_claim.id, complete_claim.lease_token,
               partition_key: "tenant-a",
               fencing_token: complete_claim.fencing_token,
               result: %{status: "sent"},
               now_ms: 1_100
             )

    assert {:ok, failed} =
             FerricStore.flow_fail(fail_claim.id, fail_claim.lease_token,
               partition_key: "tenant-a",
               fencing_token: fail_claim.fencing_token,
               error: %{code: "bad_input"},
               now_ms: 1_100
             )

    assert is_binary(completed.result_ref)
    assert is_binary(failed.error_ref)

    assert {:ok, fetched_completed} =
             FerricStore.flow_get(complete_claim.id, partition_key: "tenant-a")

    assert {:ok, fetched_failed} = FerricStore.flow_get(fail_claim.id, partition_key: "tenant-a")

    assert fetched_completed.result == %{status: "sent"}
    assert fetched_failed.error == %{code: "bad_input"}
  end

  test "batch APIs also persist full value fields" do
    partition = "tenant-b"
    type = "value-batch"
    complete_id = unique_id("flow-value-batch-complete")
    retry_id = unique_id("flow-value-batch-retry")
    fail_id = unique_id("flow-value-batch-fail")

    assert {:ok, created} =
             FerricStore.flow_create_many(
               partition,
               [
                 %{id: complete_id, payload: %{kind: "complete"}},
                 %{id: retry_id, payload: %{kind: "retry"}},
                 %{id: fail_id, payload: %{kind: "fail"}}
               ],
               type: type,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert Enum.all?(created, &is_binary(&1.payload_ref))

    assert {:ok, claimed} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               worker: "worker-1",
               limit: 3,
               now_ms: 1_000
             )

    claimed_by_id = Map.new(claimed, &{&1.id, &1})

    complete_claim = Map.fetch!(claimed_by_id, complete_id)
    retry_claim = Map.fetch!(claimed_by_id, retry_id)
    fail_claim = Map.fetch!(claimed_by_id, fail_id)

    assert {:ok, [completed]} =
             FerricStore.flow_complete_many(
               partition,
               [
                 %{
                   id: complete_id,
                   lease_token: complete_claim.lease_token,
                   fencing_token: complete_claim.fencing_token,
                   result: ["done"]
                 }
               ],
               now_ms: 1_100
             )

    assert {:ok, [retried]} =
             FerricStore.flow_retry_many(
               partition,
               [
                 %{
                   id: retry_id,
                   lease_token: retry_claim.lease_token,
                   fencing_token: retry_claim.fencing_token,
                   error: %{retry: true},
                   payload: %{kind: "retry-updated"}
                 }
               ],
               run_at_ms: 2_000,
               now_ms: 1_100
             )

    assert {:ok, [failed]} =
             FerricStore.flow_fail_many(
               partition,
               [
                 %{
                   id: fail_id,
                   lease_token: fail_claim.lease_token,
                   fencing_token: fail_claim.fencing_token,
                   error: {:bad, :input}
                 }
               ],
               now_ms: 1_100
             )

    assert is_binary(completed.result_ref)
    assert is_binary(retried.error_ref)
    assert retried.payload_ref != retry_claim.payload_ref
    assert is_binary(failed.error_ref)

    assert {:ok, fetched_completed} = FerricStore.flow_get(complete_id, partition_key: partition)
    assert {:ok, fetched_retried} = FerricStore.flow_get(retry_id, partition_key: partition)
    assert {:ok, fetched_failed} = FerricStore.flow_get(fail_id, partition_key: partition)

    assert fetched_completed.result == ["done"]
    assert fetched_retried.payload == %{kind: "retry-updated"}
    assert fetched_retried.error == %{retry: true}
    assert fetched_failed.payload == %{kind: "fail"}
    assert fetched_failed.error == {:bad, :input}
  end

  defp unique_id(prefix), do: "#{prefix}:#{System.unique_integer([:positive])}"
end
