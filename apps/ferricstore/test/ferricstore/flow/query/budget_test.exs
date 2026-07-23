defmodule Ferricstore.Flow.Query.BudgetTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias Ferricstore.Flow.Query.Budget

  test "rejects budgets above the immutable execution ceilings" do
    default = Budget.default()

    assert {:error, :invalid_query_budget} =
             Budget.new(range_seeks: default.range_seeks + 1)

    assert {:error, :invalid_query_budget} =
             Budget.new(scan_entries: 1 <<< 4_096)

    assert {:ok, lowered} = Budget.new(scan_entries: default.scan_entries - 1)
    assert lowered.scan_entries == default.scan_entries - 1
  end
end
