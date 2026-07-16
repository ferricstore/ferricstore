defmodule Ferricstore.Flow.RetryPolicyTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.RetryPolicy

  test "flow policies reject malformed per-state policy values" do
    error = {:error, "ERR flow state policy must be a map or keyword list"}

    assert ^error =
             RetryPolicy.normalize_flow_policy("invalid-state-policy", states: %{"queued" => 42})

    assert ^error =
             RetryPolicy.normalize_flow_policy("invalid-state-policy-list",
               states: %{"queued" => [:not_a_keyword_pair]}
             )
  end

  test "flow policies omit absent fields from their canonical snapshot" do
    assert {:ok, policy} =
             RetryPolicy.normalize_flow_policy("compact-policy", states: %{"queued" => %{}})

    refute Map.has_key?(policy, :version)
    refute Map.has_key?(policy, :max_active_ms)
    assert policy.states == %{"queued" => %{}}
  end
end
