defmodule Ferricstore.Flow.Schedule.SummaryTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Schedule.Summary

  test "reports a later claim failure separately from claimed schedule outcomes" do
    results = [
      {%{payload: %{id: "daily"}}, {:ok, "daily:1000:1", 2}},
      {%{payload: %{id: "broken"}}, {:error, "ERR target creation failed"}}
    ]

    assert {:ok, summary} = Summary.fire_results(results, "ERR claim unavailable")
    assert summary.claimed == 2
    assert summary.fired == 1
    assert summary.skipped == 0
    assert summary.coalesced == 2
    assert summary.errors == [{"broken", "ERR target creation failed"}]
    assert summary.claim_error == "ERR claim unavailable"
    assert summary.last_target_id == "daily:1000:1"
    assert summary.fired + summary.skipped + length(summary.errors) == summary.claimed
  end

  test "omits claim_error from the normal response" do
    assert {:ok, summary} = Summary.fire_results([], nil)
    refute Map.has_key?(summary, :claim_error)
  end
end
