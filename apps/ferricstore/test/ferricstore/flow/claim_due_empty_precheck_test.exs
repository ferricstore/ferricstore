defmodule Ferricstore.Flow.ClaimDueEmptyPrecheckTest do
  use ExUnit.Case, async: false

  @due_keys_present {Ferricstore.Flow.NativeOrderedIndex, :due_keys_present, 3}

  # Polling workers mostly find their queue empty. Each priority used to check
  # every shard's index on its own, so an empty claim without a priority made
  # three passes over the shards. (Lease reclaim is off so the claim makes one
  # pass; with it on, the reclaim of running Flows is a pass of its own.)
  test "an automatic-partition claim on an empty queue checks each shard's index once" do
    ctx = FerricStore.Instance.get(:default)
    type = "empty-precheck-#{System.unique_integer([:positive])}"

    :erlang.trace_pattern(@due_keys_present, true, [:call_count])

    try do
      assert {:ok, []} =
               FerricStore.flow_claim_due(type,
                 state: "queued",
                 worker: "worker-a",
                 limit: 10,
                 reclaim_expired: false
               )

      assert {:call_count, checks} = :erlang.trace_info(@due_keys_present, :call_count)
      assert checks == ctx.shard_count
    after
      :erlang.trace_pattern(@due_keys_present, false, [:call_count])
    end
  end

  test "an automatic-partition claim still finds work at any priority" do
    type = "empty-precheck-work-#{System.unique_integer([:positive])}"
    id = "flow-#{System.unique_integer([:positive])}"

    assert :ok = FerricStore.flow_create(id, type: type, priority: 1)

    assert {:ok, [%{id: ^id}]} =
             FerricStore.flow_claim_due(type, state: "queued", worker: "worker-a", limit: 10)
  end
end
