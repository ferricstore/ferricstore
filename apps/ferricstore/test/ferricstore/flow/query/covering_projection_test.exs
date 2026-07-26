defmodule Ferricstore.Flow.Query.CoveringProjectionTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.{CoveringProjection, IndexDefinition, Plan, Request}

  setup do
    definition =
      IndexDefinition.new!(%{
        id: "covering-state",
        version: 1,
        fields: [
          {:partition_key, :asc},
          {:state, :asc},
          {:updated_at_ms, :desc}
        ],
        covering_fields: [:partition_key, :run_id, :state, :updated_at_ms, :version]
      })

    %{definition: definition}
  end

  test "prepares field types for eligible count and sparse record projections", %{
    definition: definition
  } do
    count = request(:count, :all, [])

    assert %{id: :keyword, version: :integer} =
             CoveringProjection.field_types(count, definition, [])

    records = request(:record, [:run_id, :state], [{:updated_at_ms, :desc}])

    assert %{
             id: :keyword,
             partition_key: :keyword,
             state: :keyword,
             updated_at_ms: :integer,
             version: :integer
           } = CoveringProjection.field_types(records, definition, [])
  end

  test "rejects full records, uncovered fields, and uncovered residual predicates", %{
    definition: definition
  } do
    assert is_nil(CoveringProjection.field_types(request(:record, :all, []), definition, []))

    assert is_nil(
             CoveringProjection.field_types(
               request(:record, [:run_id, :type], []),
               definition,
               []
             )
           )

    assert is_nil(
             CoveringProjection.field_types(
               request(:record, [:run_id], []),
               definition,
               [{:eq, :type, {:literal, :keyword, "invoice"}}]
             )
           )

    assert %{state: :keyword} =
             CoveringProjection.field_types(
               request(:record, [:run_id], []),
               definition,
               [{:eq, :state, {:literal, :keyword, "failed"}}]
             )
  end

  test "classifies only selected covering plans and flags impossible hydration", %{
    definition: definition
  } do
    request = request(:record, [:run_id, :state], [{:updated_at_ms, :desc}])
    plan = plan(:ordered_range, :covering_index, definition, [])

    assert :ineligible =
             CoveringProjection.classify(
               request,
               %{plan | residual_predicates: [:residual]},
               usage(1, 1)
             )

    assert :empty = CoveringProjection.classify(request, plan, usage(0, 0))
    assert :covered = CoveringProjection.classify(request, plan, usage(4, 0))
    assert :inconsistent = CoveringProjection.classify(request, plan, usage(4, 4))

    assert :ineligible =
             CoveringProjection.classify(
               request,
               plan(:ordered_range, :query_row, definition, []),
               usage(4, 0)
             )

    assert :ineligible =
             CoveringProjection.classify(
               request(:count, :all, []),
               plan(:counter_lookup, :transactional_counter, definition, []),
               usage(1, 0)
             )

    assert :ineligible =
             CoveringProjection.classify(
               request,
               plan(:empty, :none, definition, []),
               usage(0, 0)
             )
  end

  defp plan(path, record_source, definition, residual_predicates) do
    struct(Plan,
      path: path,
      record_source: record_source,
      definition: definition,
      residual_predicates: residual_predicates
    )
  end

  defp request(return, projection, order_by) do
    struct(Request,
      mode: :execute,
      source: :runs,
      predicate: {:and, []},
      return: return,
      projection: projection,
      order_by: order_by
    )
  end

  defp usage(scanned, hydrated), do: %{scanned_entries: scanned, hydrated_records: hydrated}
end
