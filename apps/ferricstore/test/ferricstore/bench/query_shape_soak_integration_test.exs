Code.require_file(Path.expand("../../../../../bench/support/query_workload_matrix.exs", __DIR__))
Code.require_file(Path.expand("../../../../../bench/support/query_soak_fixture.exs", __DIR__))

defmodule Ferricstore.Bench.QueryShapeSoakIntegrationTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Bench.{QuerySoakFixture, QueryWorkloadMatrix}
  alias Ferricstore.Flow.Query.{Executor, Planner}

  @cursor_key :binary.copy(<<0x71>>, 32)
  @now_ms 2_000_000_000_000
  @record_count 1_200

  setup_all do
    fixture = QuerySoakFixture.prepare!(@record_count)
    on_exit(fn -> QuerySoakFixture.cleanup(fixture) end)
    fixture
  end

  test "projects every launch index in one consistent generation", fixture do
    assert fixture.projection.projected_records == @record_count
    assert fixture.projection.written_entries == @record_count * length(fixture.definitions)
    assert length(fixture.indexes) == length(fixture.definitions)
  end

  test "executes the complete shape matrix through real LMDB", fixture do
    scenarios = QueryWorkloadMatrix.scenarios(@record_count, fixture.records)

    for scenario <- scenarios do
      indexes = QueryWorkloadMatrix.select_indexes(fixture.indexes, scenario)

      assert {:ok, plan} =
               Planner.plan(scenario.request, indexes,
                 budget: scenario.budget,
                 now_ms: @now_ms
               )

      assert :ok = QueryWorkloadMatrix.verify_plan(scenario, plan), scenario.name

      result =
        Executor.execute(fixture.ctx, 0, scenario.request, plan,
          cursor_key: @cursor_key,
          now_ms: @now_ms
        )

      case {scenario.outcome, result} do
        {_outcome, {:error, actual}} ->
          assert :ok = QueryWorkloadMatrix.verify_error(scenario, actual), scenario.name

        {{:records, _first, _first_more, _second, _second_more}, {:ok, first}} ->
          assert :ok = QueryWorkloadMatrix.verify_result(scenario, first, :first), scenario.name
          verify_second_page(fixture, scenario, indexes, first)

        {{:count, _expected}, {:ok, count}} ->
          assert :ok = QueryWorkloadMatrix.verify_result(scenario, count, :first), scenario.name

        {_expected, actual} ->
          flunk("#{scenario.name} returned #{inspect(actual)}")
      end
    end
  end

  defp verify_second_page(_fixture, %{cursor?: false}, _indexes, _first), do: :ok

  defp verify_second_page(fixture, scenario, indexes, %{has_more: true} = first) do
    request = %{scenario.request | cursor: {:literal, :keyword, first.continuation}}

    assert {:ok, plan} =
             Planner.plan(request, indexes, budget: scenario.budget, now_ms: @now_ms)

    assert {:ok, second} =
             Executor.execute(fixture.ctx, 0, request, plan,
               cursor_key: @cursor_key,
               now_ms: @now_ms
             )

    assert :ok = QueryWorkloadMatrix.verify_result(scenario, second, :second), scenario.name
  end

  defp verify_second_page(_fixture, _scenario, _indexes, _first), do: :ok
end
