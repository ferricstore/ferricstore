defmodule Ferricstore.Flow.Query.QueryRowDecodePlanTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.{IndexDefinition, Plan, QueryRowDecodePlan, Request}

  test "omits dynamic sections for builtin-only projected queries" do
    request = %Request{
      mode: :execute,
      source: :runs,
      predicate: {:and, [{:eq, :state, {:literal, :keyword, "queued"}}]},
      order_by: [{:updated_at_ms, :asc}],
      limit: 25,
      projection: [:run_id, :state],
      return: :record
    }

    assert %QueryRowDecodePlan{attributes: :none, state_meta: :none} =
             QueryRowDecodePlan.for_query(request, plan())
  end

  test "includes dynamic sections used anywhere in execution" do
    request = %Request{
      mode: :execute,
      source: :runs,
      predicate: {:and, [{:eq, {:attribute, "customer"}, {:literal, :keyword, "c-1"}}]},
      order_by: [{:updated_at_ms, :asc}],
      limit: 25,
      projection: [:run_id, {:state_meta, "queued", "worker"}],
      return: :record
    }

    definition =
      struct(IndexDefinition,
        fields: [
          {:partition_key, :asc, :hashed},
          {{:state_meta, "queued", "worker"}, :asc, :ordered}
        ]
      )

    plan =
      plan(
        definition: definition,
        residual_predicates: [
          {:eq, {:attribute, "customer"}, {:literal, :keyword, "c-1"}}
        ]
      )

    assert %QueryRowDecodePlan{
             attributes: attributes,
             state_meta: state_meta
           } =
             QueryRowDecodePlan.for_query(request, plan)

    assert attributes == MapSet.new(["customer"])
    assert state_meta == %{"queued" => MapSet.new(["worker"])}
  end

  test "whole-section projections take precedence over exact dynamic fields" do
    request = %Request{
      mode: :execute,
      source: :runs,
      predicate: {:and, [{:eq, {:attribute, "customer"}, {:literal, :keyword, "c-1"}}]},
      order_by: [],
      limit: 25,
      projection: [:attributes, :state_meta],
      return: :record
    }

    assert %QueryRowDecodePlan{attributes: :all, state_meta: :all} =
             QueryRowDecodePlan.for_query(request, plan())
  end

  test "rejects malformed exact selectors without raising" do
    refute QueryRowDecodePlan.valid?(%QueryRowDecodePlan{
             attributes: MapSet.new(["valid", 1]),
             state_meta: :none
           })

    refute QueryRowDecodePlan.valid?(%QueryRowDecodePlan{
             attributes: :none,
             state_meta: %{"queued" => MapSet.new(["valid", 1])}
           })
  end

  test "full record return includes both public metadata sections but count does not" do
    records = %Request{
      mode: :execute,
      source: :runs,
      predicate: {:and, []},
      order_by: [],
      limit: 25,
      projection: :all,
      return: :record
    }

    count = %{records | limit: nil, return: :count}

    assert %QueryRowDecodePlan{attributes: :all, state_meta: :all} =
             QueryRowDecodePlan.for_query(records, plan())

    assert %QueryRowDecodePlan{attributes: :none, state_meta: :none} =
             QueryRowDecodePlan.for_query(count, plan())
  end

  defp plan(overrides \\ []) do
    struct(
      Plan,
      Keyword.merge(
        [
          definition: nil,
          residual_predicates: [],
          recheck_predicates: []
        ],
        overrides
      )
    )
  end
end
