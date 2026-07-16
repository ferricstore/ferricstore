defmodule Ferricstore.Store.AppendResultTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.AppendResult

  test "validates exact append location vectors without accepting a prefix" do
    assert :ok = AppendResult.validate_locations([{0, 12}, {12, 14}], 2)

    assert {:error, {:location_count_mismatch, 2, 1}} =
             AppendResult.validate_locations([{0, 12}], 2)

    assert {:error, {:location_count_mismatch, 2, 3}} =
             AppendResult.validate_locations([{0, 12}, {12, 14}, {26, 9}], 2)
  end

  test "rejects malformed or negative append locations" do
    assert {:error, {:invalid_location, 1, {:put, 12, 14}}} =
             AppendResult.validate_locations([{0, 12}, {:put, 12, 14}], 2)

    assert {:error, {:invalid_location, 0, {-1, 12}}} =
             AppendResult.validate_locations([{-1, 12}], 1)

    assert {:error, {:invalid_locations, :bad_reply}} =
             AppendResult.validate_locations(:bad_reply, 1)
  end

  test "validates operation locations against the submitted operation tags" do
    ops = [{:put, "a", "1", 0}, {:delete, "b"}]

    assert :ok =
             AppendResult.validate_operation_locations(
               [{:put, 0, 28}, {:delete, 28, 27}],
               ops
             )

    assert {:error, {:operation_location_mismatch, 1, :delete, {:put, 28, 27}}} =
             AppendResult.validate_operation_locations(
               [{:put, 0, 28}, {:put, 28, 27}],
               ops
             )

    assert {:error, {:location_count_mismatch, 2, 1}} =
             AppendResult.validate_operation_locations([{:put, 0, 28}], ops)
  end
end
