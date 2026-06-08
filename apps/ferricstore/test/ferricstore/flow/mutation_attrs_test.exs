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
end
