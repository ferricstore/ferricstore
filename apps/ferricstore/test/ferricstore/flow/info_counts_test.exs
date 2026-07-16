defmodule Ferricstore.Flow.InfoCountsTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.InfoCounts

  test "zero_counts creates atom keys for active and terminal states" do
    assert InfoCounts.zero_counts("queued", ["completed", "failed"]) == %{
             queued: 0,
             running: 0,
             completed: 0,
             failed: 0
           }
  end

  test "state_keys and inflight_key use Flow key layout" do
    expected_state_keys =
      Enum.map(["queued", "running", "completed"], fn state ->
        {state, Ferricstore.Flow.Keys.state_index_key("email", state, "tenant-a")}
      end)

    assert InfoCounts.state_keys("email", "tenant-a", "queued", ["completed"]) ==
             expected_state_keys

    assert InfoCounts.inflight_key("email", "tenant-a") ==
             {"inflight", Ferricstore.Flow.Keys.inflight_index_key("email", "tenant-a")}
  end

  test "merge_auto combines counts and inflight totals" do
    assert InfoCounts.merge_auto({%{queued: 1, completed: 2}, 3}, %{queued: 4, failed: 5}, 6) ==
             {%{queued: 5, completed: 2, failed: 5}, 9}
  end

  test "terminal_keys returns only terminal state index keys" do
    state_keys = [
      {"queued", "q"},
      {"running", "r"},
      {"completed", "c"},
      {"failed", "f"}
    ]

    assert InfoCounts.terminal_keys(state_keys, ["completed", "failed"]) == ["c", "f"]
  end

  test "merge_terminal_counts adds counts into existing accumulator" do
    assert {:ok, %{"c" => 4, "f" => 6}} =
             InfoCounts.merge_terminal_counts(%{"c" => 1, "f" => 2}, ["c", "f"], [3, 4])
  end

  test "merge_terminal_counts rejects a partial count vector" do
    assert {:error, {:batch_result_mismatch, 2, 1}} =
             InfoCounts.merge_terminal_counts(%{"c" => 1, "f" => 2}, ["c", "f"], [3])
  end

  test "merge_terminal_counts rejects malformed counts without raising" do
    assert {:error, {:invalid_batch_results, [3, :invalid]}} =
             InfoCounts.merge_terminal_counts(
               %{"c" => 1, "f" => 2},
               ["c", "f"],
               [3, :invalid]
             )

    assert {:error, {:invalid_batch_results, [3, -1]}} =
             InfoCounts.merge_terminal_counts(%{"c" => 1, "f" => 2}, ["c", "f"], [3, -1])
  end

  test "validate_counts rejects malformed vectors before callers inspect them" do
    assert InfoCounts.validate_counts(["c", "f"], [3, 4]) == {:ok, [3, 4]}

    assert InfoCounts.validate_counts(["c", "f"], [3, :invalid]) ==
             {:error, {:invalid_batch_results, [3, :invalid]}}

    assert InfoCounts.validate_counts(["c", "f"], [3]) ==
             {:error, {:batch_result_mismatch, 2, 1}}
  end
end
