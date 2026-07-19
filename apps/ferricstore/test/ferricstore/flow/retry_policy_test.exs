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

  test "each state retains its independently configured execution mode" do
    assert {:ok, policy} =
             RetryPolicy.normalize_flow_policy("ordered-policy",
               states: %{
                 "queued" => [mode: :fifo],
                 "review" => [mode: :parallel]
               }
             )

    assert RetryPolicy.state_mode(policy, "queued") == :fifo
    assert RetryPolicy.state_mode(policy, "review") == :parallel
    assert RetryPolicy.state_mode(policy, "undeclared") == :parallel
    assert RetryPolicy.fifo_states(policy) == MapSet.new(["queued"])
    assert RetryPolicy.any_fifo_state?(policy)
    assert RetryPolicy.any_fifo_state?(policy, &(&1 == "queued"))
    refute RetryPolicy.any_fifo_state?(policy, &(&1 == "review"))
    refute RetryPolicy.any_fifo_state?(nil)
  end
end
