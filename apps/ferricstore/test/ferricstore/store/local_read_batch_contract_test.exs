defmodule Ferricstore.Store.LocalReadBatchContractTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.Ops.LocalRead
  alias Ferricstore.Store.ReadResult

  test "local batch partitions fail every requested slot closed on cardinality mismatch" do
    existing = %{0 => "already-read"}

    for backend_results <- [["first"], ["first", "second", "extra"], :invalid] do
      assert %{0 => "already-read", 1 => first_failure, 2 => second_failure} =
               LocalRead.__merge_batch_results_for_test__(
                 [1, 2],
                 existing,
                 backend_results
               )

      assert ReadResult.failure?(first_failure)
      assert ReadResult.failure?(second_failure)
    end
  end

  test "local batch partitions preserve exact result ordering" do
    assert %{4 => "four", 9 => "nine"} =
             LocalRead.__merge_batch_results_for_test__([4, 9], %{}, ["four", "nine"])
  end
end
