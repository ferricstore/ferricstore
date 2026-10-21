defmodule Ferricstore.Flow.GovernanceArgumentErrorsTest do
  @moduledoc """
  A governance call with a bad id, scope, key or amount names that argument
  instead of answering that the opts must be a keyword list.
  """
  use ExUnit.Case, async: false

  test "governance calls name the bad argument" do
    for {call, message} <- [
          {fn -> FerricStore.flow_effect_reserve(123, "k", "t", []) end,
           "ERR flow effect id must be a string"},
          {fn -> FerricStore.flow_effect_reserve("f", 123, "t", []) end,
           "ERR flow effect effect_key must be a string"},
          {fn -> FerricStore.flow_effect_reserve("f", "k", 1, []) end,
           "ERR flow effect effect_type must be a string"},
          {fn -> FerricStore.flow_effect_confirm("f", 123, []) end,
           "ERR flow effect effect_key must be a string"},
          {fn -> FerricStore.flow_effect_get(1, "k", []) end,
           "ERR flow effect id must be a string"},
          {fn -> FerricStore.flow_governance_ledger(1, []) end,
           "ERR flow governance ledger id must be a string"},
          {fn -> FerricStore.flow_approval_request(1, []) end,
           "ERR flow approval id must be a non-empty string"},
          {fn -> FerricStore.flow_approval_approve("", []) end,
           "ERR flow approval id must be a non-empty string"},
          {fn -> FerricStore.flow_approval_get(1, []) end,
           "ERR flow approval id must be a non-empty string"},
          {fn -> FerricStore.flow_circuit_open(1, []) end,
           "ERR flow circuit scope must be a non-empty string"},
          {fn -> FerricStore.flow_circuit_get("", []) end,
           "ERR flow circuit scope must be a non-empty string"},
          {fn -> FerricStore.flow_budget_reserve(1, 5, []) end,
           "ERR flow budget scope must be a non-empty string"},
          {fn -> FerricStore.flow_budget_reserve("s", 0, []) end,
           "ERR flow budget amount must be a positive integer"},
          {fn -> FerricStore.flow_budget_commit("s", 1, 5, []) end,
           "ERR flow budget commit reservation_id must be a non-empty string"},
          {fn -> FerricStore.flow_budget_commit("s", "r", -1, []) end,
           "ERR flow budget commit actual_amount must be a non-negative integer"},
          {fn -> FerricStore.flow_budget_release("s", 1, []) end,
           "ERR flow budget release reservation_id must be a non-empty string"},
          {fn -> FerricStore.flow_budget_get(1, []) end,
           "ERR flow budget scope must be a non-empty string"},
          {fn -> FerricStore.flow_limit_lease(1, []) end,
           "ERR flow limit scope must be a non-empty string"},
          {fn -> FerricStore.flow_limit_get(1, []) end,
           "ERR flow limit scope must be a non-empty string"},
          {fn -> FerricStore.flow_circuit_open("s", :bad) end,
           "ERR flow circuit opts must be a keyword list"},
          {fn -> FerricStore.flow_approval_list(:bad) end,
           "ERR flow approval opts must be a keyword list"}
        ] do
      assert {:error, ^message} = call.()
    end
  end
end
