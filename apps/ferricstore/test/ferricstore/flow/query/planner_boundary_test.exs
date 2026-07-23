defmodule Ferricstore.Flow.Query.PlannerBoundaryTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.{Budget, IndexCatalog, Plan, Planner, RegisteredIndex, Request}

  test "the OSS planner selects a bounded composite index for a general collection query" do
    request =
      Request.collection(
        :execute,
        [
          eq(:partition_key, "tenant-a"),
          eq(:type, "invoice"),
          eq(:state, "failed"),
          {:time_window, :updated_at_ms, integer(100), integer(200)}
        ],
        [{:updated_at_ms, :desc}],
        25,
        :record
      )

    assert {:ok, catalog} = IndexCatalog.load()

    definition =
      Enum.find(
        catalog.definitions,
        &(&1.id == "flow_runs_tenant_type_state_updated")
      )

    index =
      RegisteredIndex.new!(definition, :active,
        coverage: %{complete_shards: 1, total_shards: 1, validation: :passed}
      )

    assert {:ok,
            %Plan{
              path: :ordered_range,
              index_id: "flow_runs_tenant_type_state_updated",
              order: :native,
              residual_predicates: [],
              ranges: [_range]
            }} = Planner.plan(request, [index])
  end

  test "the OSS planner retains a bounded fixed-index plan while composite indexes build" do
    request =
      Request.collection(
        :execute,
        [
          eq(:partition_key, "tenant-a"),
          eq(:type, "invoice"),
          eq(:state, "failed")
        ],
        [{:updated_at_ms, :desc}],
        25,
        :record
      )

    assert {:ok,
            %Plan{
              path: :fixed_index,
              index_id: "flow_runs_fixed_state_index_v1",
              order: :native,
              fallback_reason: :none
            }} = Planner.plan(request, [])
  end

  test "the fixed-index fallback obeys hydration and result budgets" do
    request =
      Request.collection(
        :execute,
        [
          eq(:partition_key, "tenant-a"),
          eq(:type, "invoice"),
          eq(:state, "failed")
        ],
        [{:updated_at_ms, :desc}],
        25,
        :record
      )

    assert {:ok, hydration_limited} = Budget.lower(Budget.default(), hydrated_records: 100)

    assert {:ok, %Plan{path: :reject, fallback_reason: :hydration_budget_exceeded}} =
             Planner.plan(request, [], budget: hydration_limited)

    assert {:ok, result_limited} = Budget.lower(Budget.default(), result_records: 24)

    assert {:ok, %Plan{path: :reject, fallback_reason: :result_budget_exceeded}} =
             Planner.plan(request, [], budget: result_limited)
  end

  defp eq(field, value), do: {:eq, field, {:literal, :keyword, value}}
  defp integer(value), do: {:literal, :integer, value}
end
