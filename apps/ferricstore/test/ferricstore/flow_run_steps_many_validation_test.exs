defmodule Ferricstore.FlowRunStepsManyValidationTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow

  @moduletag :flow

  test "run_steps_many rejects batches above the configured Flow item limit" do
    items = Enum.map(1..1_001, &"flow-#{&1}")

    assert {:error, "ERR flow batch item count exceeds maximum 1000"} =
             Flow.run_steps_many(:unused, items,
               type: "batch",
               states: ["run"],
               worker: "worker"
             )
  end

  test "run_steps_many bounds the item-by-step work admitted to one Raft apply" do
    items = Enum.map(1..501, &"flow-#{&1}")

    assert {:error, "ERR flow run step operation count exceeds maximum 1000"} =
             Flow.run_steps_many(:unused, items,
               type: "batch",
               states: ["prepare", "commit"],
               worker: "worker"
             )
  end

  test "spawn_children rejects child batches above the configured Flow item limit" do
    children = Enum.map(1..1_001, &"child-#{&1}")

    assert {:error, "ERR flow batch item count exceeds maximum 1000"} =
             Flow.spawn_children(:unused, "parent", children,
               partition_key: "partition",
               group_id: "group",
               type: "child",
               fencing_token: 1,
               success: "completed",
               failure: "failed"
             )
  end

  test "pipeline claim_due rejects inexact timestamps before routing" do
    assert [{:error, "ERR flow now_ms exceeds maximum 9007199254740991"}] =
             Flow.pipeline_claim_due_batch(:unused, [
               {:claim_due, "email",
                [worker: "worker", now_ms: 9_007_199_254_740_992]}
             ])
  end
end
