defmodule Ferricstore.Flow.MutationAttrsTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.MutationAttrs

  test "create_attrs preserves default state, auto partition, and named value options" do
    assert {:ok, attrs} =
             MutationAttrs.create_attrs("flow-1",
               type: "email",
               values: ["order"],
               value_refs: %{reservation: "ref-1"}
             )

    assert attrs.id == "flow-1"
    assert attrs.type == "email"
    assert attrs.state == "queued"
    assert attrs.partition_key == Ferricstore.Flow.Keys.auto_partition_key("flow-1")
    assert attrs.values == ["order"]
    assert attrs.value_refs == %{reservation: "ref-1"}
  end

  test "transition_attrs rejects direct transition into running" do
    assert {:error, "ERR flow running state is only entered by FLOW.CLAIM_DUE"} =
             MutationAttrs.transition_attrs("flow-1", "queued", "running",
               fencing_token: 1,
               partition_key: "p1"
             )
  end

  test "create_attrs rejects timestamps that cannot be represented exactly by Flow indexes" do
    above_exact_integer = 9_007_199_254_740_992

    assert {:error, "ERR flow now_ms exceeds maximum 9007199254740991"} =
             MutationAttrs.create_attrs("flow-1",
               type: "email",
               now_ms: above_exact_integer
             )

    assert {:error, "ERR flow run_at_ms exceeds maximum 9007199254740991"} =
             MutationAttrs.create_attrs("flow-1",
               type: "email",
               now_ms: 1,
               run_at_ms: above_exact_integer
             )
  end

  test "lease mutations reject deadlines outside the exact Flow timestamp range" do
    max_exact_integer = 9_007_199_254_740_991

    assert {:error, "ERR flow lease_ms exceeds maximum 9007199254740991"} =
             MutationAttrs.start_and_claim_attrs("flow-1", "email", "queued",
               worker: "worker",
               now_ms: 0,
               lease_ms: max_exact_integer + 1
             )

    assert {:error, "ERR flow lease_ms deadline exceeds maximum 9007199254740991"} =
             MutationAttrs.start_and_claim_attrs("flow-1", "email", "queued",
               worker: "worker",
               now_ms: max_exact_integer,
               lease_ms: 1
             )
  end
end
