Code.require_file(Path.expand("../../../../../bench/support/query_workload_matrix.exs", __DIR__))

defmodule Ferricstore.Bench.QueryWorkloadMatrixTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.RegisteredIndex

  alias Ferricstore.Bench.QueryWorkloadMatrix
  alias Ferricstore.Flow.Query.{Budget, IndexCatalog, Plan, Planner}

  @record_count 100_000
  @now_ms 2_000_000_000_000

  setup_all do
    {:ok, catalog} = IndexCatalog.load()

    indexes =
      Enum.map(catalog.definitions, fn definition ->
        RegisteredIndex.new!(definition, :active,
          coverage: %{complete_shards: 1, total_shards: 1, validation: :passed}
        )
      end)

    %{catalog: catalog, indexes: indexes}
  end

  test "covers every launch index with a broad native page", %{catalog: catalog} do
    broad_indexes =
      @record_count
      |> QueryWorkloadMatrix.scenarios()
      |> Enum.filter(&(&1.class == :broad_native_page))
      |> MapSet.new(& &1.plan.index_id)

    assert broad_indexes == MapSet.new(catalog.definitions, & &1.id)
  end

  test "covers every relevant physical behavior" do
    scenarios = QueryWorkloadMatrix.scenarios(@record_count)

    assert Enum.map(scenarios, & &1.name) == Enum.uniq(Enum.map(scenarios, & &1.name))

    assert MapSet.new(scenarios, & &1.class) ==
             MapSet.new([
               :broad_native_page,
               :counter_lookup,
               :count_scan,
               :multi_range_union,
               :residual_filter,
               :bounded_top_k,
               :budget_rejection,
               :empty
             ])

    assert Enum.any?(scenarios, & &1.cursor?)
    assert Enum.any?(scenarios, &match?({:error, :query_scan_budget_exceeded}, &1.outcome))
  end

  test "weights broad scans below operational page and counter traffic" do
    scenarios = QueryWorkloadMatrix.scenarios(@record_count)
    total_weight = Enum.sum_by(scenarios, & &1.weight)

    assert total_weight == 196

    assert Enum.sum_by(scenarios, &if(&1.class == :broad_native_page, do: &1.weight, else: 0)) ==
             100

    assert Enum.sum_by(scenarios, &if(&1.class == :counter_lookup, do: &1.weight, else: 0)) == 80
    assert Enum.find(scenarios, &(&1.name == "state_broad_non_native_top_k")).weight == 1
    assert Enum.find(scenarios, &(&1.name == "lease_broad_count_scan")).weight == 1
  end

  test "every launch workload requires an exact result within its deadline" do
    scenarios = QueryWorkloadMatrix.scenarios(@record_count)

    refute Enum.any?(scenarios, &Map.has_key?(&1, :allowed_errors))

    scenario = Enum.find(scenarios, &(&1.name == "state_broad_non_native_top_k"))

    assert {:error, {:unexpected_query_error, :success, :query_deadline_exceeded}} =
             QueryWorkloadMatrix.verify_error(scenario, :query_deadline_exceeded)

    assert {:error, {:unexpected_query_error, :success, :query_memory_budget_exceeded}} =
             QueryWorkloadMatrix.verify_error(scenario, :query_memory_budget_exceeded)
  end

  test "real catalog plans every executable scenario with its declared contract", %{
    indexes: indexes
  } do
    for scenario <- QueryWorkloadMatrix.scenarios(@record_count) do
      scenario_indexes = QueryWorkloadMatrix.select_indexes(indexes, scenario)

      assert {:ok, %Plan{} = plan} =
               Planner.plan(scenario.request, scenario_indexes,
                 budget: scenario.budget,
                 now_ms: @now_ms
               )

      assert :ok = QueryWorkloadMatrix.verify_plan(scenario, plan), scenario.name
    end
  end

  test "oracles are exact and broad failures cross their configured scan ceiling" do
    records = QueryWorkloadMatrix.records(@record_count)
    scenarios = QueryWorkloadMatrix.scenarios(@record_count)

    for scenario <- scenarios do
      assert :ok = QueryWorkloadMatrix.verify_oracle(scenario, records)
    end

    for %{outcome: {:error, :query_scan_budget_exceeded}} = scenario <- scenarios do
      assert length(records) > scenario.budget.scan_entries
    end

    assert Enum.all?(scenarios, fn scenario ->
             scenario.budget.scan_entries <= Budget.default().scan_entries
           end)
  end
end
