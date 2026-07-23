defmodule Ferricstore.Flow.Query.RequestTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.Request

  test "point queries may derive the auto partition from run_id" do
    request = %Request{
      mode: :execute,
      source: :runs,
      predicate: {:and, [eq(:run_id, "run-auto")]},
      order_by: [],
      limit: 1,
      return: :record
    }

    assert :ok = Request.validate_bound(request)
  end

  test "event history is bounded by one run and ordered by event id" do
    request = %Request{
      mode: :execute,
      source: :events,
      predicate: {:and, [eq(:run_id, "run-history")]},
      order_by: [{:event_id, :asc}],
      limit: 25,
      return: :record
    }

    assert :ok = Request.validate_bound(request)

    assert {:error, :unsupported_query_shape} =
             Request.validate_bound(%{request | order_by: [{:updated_at_ms, :asc}]})

    assert {:error, :unsupported_query_shape} =
             Request.validate_bound(%{request | predicate: {:and, [eq(:state, "failed")]}})
  end

  test "describes one exact partition-contained lineage shape" do
    request =
      Request.collection(
        :execute,
        [eq(:parent_flow_id, "parent-1"), eq(:partition_key, "tenant-a")],
        [{:updated_at_ms, :desc}],
        25,
        :record
      )

    assert :ok = Request.validate_bound(request)

    assert {:ok,
            %{
              kind: :parent,
              field: :parent_flow_id,
              value: "parent-1",
              partition_key: "tenant-a",
              direction: :desc
            }} = Request.lineage_descriptor(request)

    assert :error =
             Request.lineage_descriptor(%{
               request
               | predicate: {:and, [eq(:parent_flow_id, "parent-1")]}
             })
  end

  test "collection queries require one unambiguous partition equality" do
    duplicate =
      Request.collection(
        :execute,
        [
          eq(:partition_key, "tenant-a"),
          eq(:partition_key, "tenant-b"),
          eq(:state, "failed")
        ],
        [{:updated_at_ms, :desc}],
        10,
        :record
      )

    assert {:error, :unsupported_query_shape} = Request.validate_bound(duplicate)

    mixed =
      Request.collection(
        :execute,
        [
          eq(:partition_key, "tenant-a"),
          {:in, :partition_key, [keyword("tenant-a"), keyword("tenant-b")]}
        ],
        [{:updated_at_ms, :desc}],
        10,
        :record
      )

    assert {:error, :unsupported_query_shape} = Request.validate_bound(mixed)
  end

  test "EXPLAIN rejects execution cursors" do
    request =
      Request.collection(
        :explain,
        [eq(:partition_key, "tenant-a"), eq(:state, "failed")],
        [{:updated_at_ms, :desc}],
        10,
        :record,
        keyword(String.duplicate("c", 32))
      )

    assert {:error, :query_cursor_invalid} = Request.validate_bound(request)
    assert :ok = Request.validate_bound(%{request | mode: :execute})
  end

  test "EXPLAIN ANALYZE is a bounded fresh execution and rejects cursors" do
    request =
      Request.collection(
        :analyze,
        [eq(:partition_key, "tenant-a"), eq(:state, "failed")],
        [{:updated_at_ms, :desc}],
        10,
        :record
      )

    assert :ok = Request.validate_bound(request)

    assert {:error, :query_cursor_invalid} =
             request
             |> Map.put(:cursor, keyword(String.duplicate("c", 32)))
             |> Request.validate_bound()

    assert :ok =
             :analyze
             |> Request.count([eq(:partition_key, "tenant-a")])
             |> Request.validate_bound()
  end

  test "scalar counts are partition-contained and have no row pagination envelope" do
    request =
      Request.count(:execute, [
        eq(:partition_key, "tenant-a"),
        eq(:type, "payment"),
        eq(:state, "failed")
      ])

    assert %Request{order_by: [], limit: nil, cursor: nil, return: :count} = request
    assert :ok = Request.validate_bound(request)
    assert :ok = Request.validate_bound(%{request | mode: :explain})

    for malformed <- [
          %{request | order_by: [{:updated_at_ms, :desc}]},
          %{request | limit: 1},
          %{request | cursor: keyword(String.duplicate("c", 32))},
          %{request | source: :events},
          %{request | predicate: {:and, [eq(:state, "failed")]}}
        ] do
      assert {:error, :unsupported_query_shape} = Request.validate_bound(malformed)
    end
  end

  test "public collection ordering uses bounded numeric fields and an implicit run reference" do
    numeric =
      Request.collection(
        :execute,
        [eq(:partition_key, "tenant-a")],
        [{:updated_at_ms, :desc}],
        10,
        :record
      )

    lexical = %{numeric | order_by: [{:run_id, :asc}]}
    unordered = %{numeric | order_by: []}

    assert :ok = Request.validate_bound(numeric)
    assert {:error, :unsupported_query_shape} = Request.validate_bound(lexical)
    assert {:error, :unsupported_query_shape} = Request.validate_bound(unordered)
  end

  test "cursor order validation shares the canonical query ordering contract" do
    assert :ok = Request.validate_cursor_order([{:updated_at_ms, :desc}, {:version, :asc}])
    assert :ok = Request.validate_cursor_order([{:event_id, :asc}])

    for invalid <- [
          [],
          [{:run_id, :asc}],
          [{{:attribute, "rank"}, :asc}],
          [{:updated_at_ms, :asc}, {:updated_at_ms, :desc}],
          [{:updated_at_ms, :sideways}]
        ] do
      assert {:error, _reason} = Request.validate_cursor_order(invalid)
    end
  end

  test "dynamic metadata allows empty strings without allowing empty identity fields" do
    metadata =
      Request.collection(
        :execute,
        [eq(:partition_key, "tenant-a"), eq({:attribute, "region"}, "")],
        [{:updated_at_ms, :asc}],
        10,
        :record
      )

    assert :ok = Request.validate_bound(metadata)

    identity =
      Request.collection(
        :execute,
        [eq(:partition_key, "tenant-a"), eq(:type, "")],
        [{:updated_at_ms, :asc}],
        10,
        :record
      )

    assert {:error, :invalid_parameter_type} = Request.validate_bound(identity)
  end

  defp eq(field, value), do: {:eq, field, keyword(value)}
  defp keyword(value), do: {:literal, :keyword, value}
end
