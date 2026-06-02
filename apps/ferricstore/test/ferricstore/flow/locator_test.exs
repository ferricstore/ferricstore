defmodule Ferricstore.Flow.LocatorTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Locator

  test "validates required logical generation and physical location fields" do
    assert {:ok, %Locator{}} =
             Locator.new(
               flow_id: "flow-1",
               kind: :state,
               version: 1,
               raft_index: 10,
               file_id: {:flow_state, 0},
               offset: 128,
               value_size: 512,
               checksum: <<1, 2, 3>>
             )

    assert {:error, :bad_locator} =
             Locator.new(
               flow_id: "",
               kind: :state,
               version: 1,
               raft_index: 10,
               file_id: {:flow_state, 0},
               offset: 128,
               value_size: 512
             )

    assert {:error, :bad_locator} =
             Locator.new(
               flow_id: "flow-1",
               kind: :unknown,
               version: 1,
               raft_index: 10,
               file_id: {:flow_state, 0},
               offset: 128,
               value_size: 512
             )
  end

  test "resolve reports invisible only when both hot and cold locators are missing" do
    hot = locator(version: 1, raft_index: 10, offset: 1)
    cold = locator(version: 1, raft_index: 10, offset: 1)

    assert {:error, :flow_invisible} = Locator.resolve(nil, nil)
    assert {:ok, :hot, ^hot} = Locator.resolve(hot, nil)
    assert {:ok, :cold, ^cold} = Locator.resolve(nil, cold)
  end

  test "resolve prefers hot when hot and cold describe the same generation" do
    hot = locator(version: 1, raft_index: 10, offset: 1)
    cold = locator(version: 1, raft_index: 10, offset: 1)

    assert {:ok, :hot, ^hot} = Locator.resolve(hot, cold)
  end

  test "resolve selects the newer logical generation when duplicate rows disagree" do
    older = locator(version: 1, raft_index: 10, offset: 1)
    newer = locator(version: 2, raft_index: 11, offset: 2)

    assert {:ok, :cold, ^newer} = Locator.resolve(older, newer)
    assert {:ok, :hot, ^newer} = Locator.resolve(newer, older)
  end

  test "hot eviction is safe only after durable cold locator matches unchanged hot snapshot" do
    snapshot = locator(version: 1, raft_index: 10, offset: 1)
    matching_cold = locator(version: 1, raft_index: 10, offset: 1)
    changed_hot = locator(version: 2, raft_index: 11, offset: 2)
    stale_cold = locator(version: 1, raft_index: 10, offset: 9)

    assert Locator.safe_to_evict_hot?(snapshot, matching_cold, snapshot)
    refute Locator.safe_to_evict_hot?(snapshot, matching_cold, changed_hot)
    refute Locator.safe_to_evict_hot?(snapshot, stale_cold, snapshot)
    refute Locator.safe_to_evict_hot?(snapshot, nil, snapshot)
  end

  test "stale async delete cannot remove a newer locator generation" do
    stale_delete = locator(version: 1, raft_index: 10, offset: 1)
    current = locator(version: 2, raft_index: 11, offset: 2)

    assert Locator.stale_delete?(stale_delete, current)
    refute Locator.stale_delete?(current, stale_delete)
    refute Locator.stale_delete?(current, nil)
  end

  test "compaction relocation keeps logical generation while changing physical location" do
    original = locator(version: 3, raft_index: 99, offset: 10, value_size: 20)

    relocated =
      Locator.relocate!(original,
        file_id: {:flow_state, 2},
        offset: 1000,
        value_size: 21,
        segment_generation: 4
      )

    assert Locator.same_logical_record?(original, relocated)
    refute Locator.same_physical_record?(original, relocated)
    assert relocated.file_id == {:flow_state, 2}
    assert relocated.offset == 1000
    assert relocated.value_size == 21
    assert relocated.segment_generation == 4
  end

  defp locator(overrides) do
    defaults = [
      flow_id: "flow-1",
      kind: :state,
      version: 1,
      raft_index: 1,
      file_id: {:flow_state, 0},
      offset: 0,
      value_size: 1,
      checksum: <<0>>
    ]

    defaults
    |> Keyword.merge(overrides)
    |> Locator.new!()
  end
end
