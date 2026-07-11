defmodule Ferricstore.Test.HistoryRecorderTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Test.HistoryRecorder

  test "counter outcome counts keep ambiguous writes in the upper bound" do
    results = [
      {:ok, 1},
      {:ok, 3},
      {:error, {:timeout, :unknown_outcome}},
      {:badrpc, :nodedown},
      {:error, "ERR value is not an integer"},
      {:error, :no_leader}
    ]

    assert HistoryRecorder.counter_outcome_counts(results) == %{
             acknowledged: 2,
             unknown: 2,
             failed: 2
           }
  end
end
