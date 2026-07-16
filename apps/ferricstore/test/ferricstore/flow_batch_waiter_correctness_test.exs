defmodule Ferricstore.FlowBatchWaiterCorrectnessTest do
  use Ferricstore.Test.FlowCase

  alias Ferricstore.Flow
  alias Ferricstore.Flow.ClaimWaiters

  test "a failed independent create does not suppress a committed item's waiter wake" do
    ctx = FerricStore.Instance.get(:default)
    type = unique_flow_id("partial-batch-type")
    partition = unique_flow_id("partial-batch-partition")
    existing_id = unique_flow_id("partial-batch-existing")
    created_id = unique_flow_id("partial-batch-created")

    opts = [
      type: type,
      state: "queued",
      partition_key: partition,
      run_at_ms: 1_000,
      now_ms: 1_000
    ]

    assert :ok = Flow.create(ctx, existing_id, opts)

    keys = ClaimWaiters.wait_keys(type, "queued", 0, partition)
    deadline_ms = System.monotonic_time(:millisecond) + 1_000
    assert :ok = ClaimWaiters.register(keys, self(), deadline_ms)
    on_exit(fn -> ClaimWaiters.unregister(keys, self()) end)

    assert [
             {:error, "ERR flow already exists"},
             :ok
           ] = Flow.create_batch_independent(ctx, [{existing_id, opts}, {created_id, opts}])

    assert_receive {:flow_claim_due_wake, :ready}, 200
  end
end
