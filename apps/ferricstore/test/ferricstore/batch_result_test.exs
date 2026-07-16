defmodule Ferricstore.BatchResultTest do
  use ExUnit.Case, async: true

  alias Ferricstore.BatchResult

  test "map_exact maps every paired result in order" do
    assert {:ok, [{:a, 1}, {:b, 2}]} =
             BatchResult.map_exact([:a, :b], [1, 2], fn left, right -> {left, right} end)
  end

  test "map_exact rejects short, long, and invalid result collections" do
    mapper = fn left, right -> {left, right} end

    assert {:error, {:batch_result_mismatch, 2, 1}} =
             BatchResult.map_exact([:a, :b], [1], mapper)

    assert {:error, {:batch_result_mismatch, 1, 2}} =
             BatchResult.map_exact([:a], [1, 2], mapper)

    assert {:error, {:invalid_batch_results, :bad_reply}} =
             BatchResult.map_exact([:a], :bad_reply, mapper)
  end
end
