defmodule Ferricstore.Flow.Query.ErrorTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.Error

  test "exposes stable bounded-execution errors with retry semantics" do
    non_retryable = [
      :query_no_bounded_plan,
      :query_range_budget_exceeded,
      :query_scan_budget_exceeded,
      :query_scan_byte_budget_exceeded,
      :query_hydration_budget_exceeded,
      :query_result_budget_exceeded,
      :query_response_budget_exceeded,
      :query_memory_budget_exceeded,
      :query_deadline_exceeded,
      :query_concurrency_exceeded,
      :query_cursor_invalid,
      :query_cursor_expired,
      :query_cursor_too_large
    ]

    for reason <- non_retryable do
      assert Error.known?(reason)
      assert %{retryable: false, safe_to_retry: false} = atom_payload(reason)
    end

    assert Error.known?(:query_projection_changed)
    assert %{retryable: true, safe_to_retry: true} = atom_payload(:query_projection_changed)

    assert %{code: "query_cursor_invalid"} = atom_payload(:query_cursor_invalid)
    assert %{code: "query_cursor_expired"} = atom_payload(:query_cursor_expired)
  end

  defp atom_payload(reason) do
    Error.payload(reason)
    |> Map.new(fn {key, value} -> {String.to_atom(key), value} end)
  end
end
