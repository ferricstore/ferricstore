defmodule Ferricstore.Flow.Query.RecordOrderTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.Query.{CompositeIndex, IndexDefinition, RecordOrder}

  test "uses the same opaque tie breaker as composite entry keys" do
    definition =
      IndexDefinition.new!(%{
        id: "tenant_state_updated",
        version: 1,
        fields: [
          {:partition_key, :asc},
          {:state, :asc},
          {:updated_at_ms, :desc}
        ]
      })

    records = [record("lexically-last"), record("a"), record("middle")]

    physical_ids =
      records
      |> Enum.map(fn record ->
        assert {:ok, [entry]} =
                 CompositeIndex.entries(
                   definition,
                   record,
                   Keys.state_key(record.id, record.partition_key),
                   0
                 )

        {entry.key, record.id}
      end)
      |> Enum.sort()
      |> Enum.map(&elem(&1, 1))

    assert {:ok, logically_ordered} = RecordOrder.sort(records, [{:updated_at_ms, :desc}])
    assert Enum.map(logically_ordered, & &1.id) == physical_ids
  end

  test "explicit run-id ordering remains lexical and precedes the opaque final tie breaker" do
    records = [record("z"), record("a"), record("m")]

    assert {:ok, ascending} = RecordOrder.sort(records, [{:run_id, :asc}])
    assert Enum.map(ascending, & &1.id) == ["a", "m", "z"]

    assert {:ok, descending} = RecordOrder.sort(records, [{:run_id, :desc}])
    assert Enum.map(descending, & &1.id) == ["z", "m", "a"]
  end

  test "fails closed for an unsupported multi-valued order field" do
    record = record("run") |> Map.put(:attributes, %{"tags" => ["a", "b"]})

    assert {:error, :unsupported_query_order_value} =
             RecordOrder.sort_key(record, [{{:attribute, "tags"}, :asc}])
  end

  test "fails closed before encoding an oversized lexical order component" do
    oversized = record(:binary.copy("r", 60_000))

    assert {:error, :unsupported_query_order_value} =
             RecordOrder.sort_key(oversized, [{:run_id, :asc}])
  end

  defp record(id) do
    %{
      id: id,
      partition_key: "tenant",
      state: "failed",
      updated_at_ms: 100,
      version: 1
    }
  end
end
