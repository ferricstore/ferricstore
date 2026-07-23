defmodule Ferricstore.Flow.Query.BuilderTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query
  alias Ferricstore.Flow.Query.Builder

  test "builds every retired record-read shape as a bound FQL request" do
    cases = [
      {:list, %{partition_key: "tenant-a", type: "invoice", state: "queued"}},
      {:search,
       %{
         partition_key: "tenant-a",
         type: "invoice",
         attribute: {"region", "eu"}
       }},
      {:terminals, %{partition_key: "tenant-a", type: "invoice"}},
      {:failures, %{partition_key: "tenant-a", type: "invoice"}},
      {:stuck, %{partition_key: "tenant-a", type: "invoice", now_ms: 1_000}},
      {:by_parent, %{partition_key: "tenant-a", id: "parent-1"}},
      {:by_root, %{partition_key: "tenant-a", id: "root-1"}},
      {:by_correlation, %{partition_key: "tenant-a", id: "correlation-1"}}
    ]

    for {kind, filters} <- cases do
      assert {:ok, %{query: query, params: params}} = Builder.build(kind, filters)
      assert {:ok, request} = Query.prepare_reference("FQL1", query, params)
      assert request.source == :runs
      assert request.return == :record
      assert request.limit == 100
    end
  end

  test "uses parameters for values and validates dynamic metadata fields" do
    filters = %{
      partition_key: "tenant'a",
      type: "invoice' OR type = 'admin",
      state_meta: {"queued", "risk-tier", "high' OR 1=1"},
      limit: 25,
      direction: :desc,
      from_ms: 10,
      to_ms: 20
    }

    assert {:ok, %{query: query, params: params}} = Builder.build(:search, filters)
    refute query =~ "tenant'a"
    refute query =~ "invoice'"
    refute query =~ "high'"
    assert query =~ "state_meta.queued.risk-tier = @state_meta_value"
    assert params["partition_key"] == "tenant'a"
    assert params["type"] == "invoice' OR type = 'admin"
    assert params["state_meta_value"] == "high' OR 1=1"
    assert {:ok, _request} = Query.prepare_reference("FQL1", query, params)

    assert {:ok, %{query: quoted_query}} =
             Builder.build(:search, %{
               filters
               | state_meta: {"review.v2", "risk'] OR type = 'admin", "high"}
             })

    assert quoted_query =~ "state_meta['review.v2']['risk''] OR type = ''admin']"

    assert {:ok, _request} =
             Query.prepare_reference("FQL1", quoted_query, %{
               "partition_key" => "tenant'a",
               "type" => "invoice' OR type = 'admin",
               "state_meta_value" => "high",
               "from_ms" => 10,
               "to_ms" => 20
             })

    assert {:error, :unsupported_field} =
             Builder.build(:search, %{filters | state_meta: {"queued", "__internal", "high"}})
  end

  test "preserves the list default state without widening the index scan" do
    assert {:ok, %{query: query, params: params}} =
             Builder.build(:list, %{partition_key: "tenant-a", type: "invoice"})

    assert query =~ "state = @state"
    assert params["state"] == "queued"
  end

  test "applies stuck time filters to the expired lease deadline window" do
    assert {:ok, %{query: query, params: params}} =
             Builder.build(:stuck, %{
               partition_key: "tenant-a",
               type: "invoice",
               from_ms: 100,
               to_ms: 900,
               now_ms: 700
             })

    assert query =~ "lease_deadline_ms BETWEEN @lease_from_ms AND @lease_to_ms"
    refute query =~ "updated_at_ms BETWEEN"
    assert params["lease_from_ms"] == 100
    assert params["lease_to_ms"] == 700
    refute Map.has_key?(params, "from_ms")
    refute Map.has_key?(params, "to_ms")

    assert {:ok, request} = Query.prepare_reference("FQL1", query, params)
    assert request.order_by == [lease_deadline_ms: :asc]
  end

  test "defaults and validates the stuck lease deadline window" do
    assert {:ok, %{params: params}} =
             Builder.build(:stuck, %{
               partition_key: "tenant-a",
               type: "invoice",
               now_ms: 700
             })

    assert params["lease_from_ms"] == 0
    assert params["lease_to_ms"] == 700

    assert {:error, :invalid_query_filter} =
             Builder.build(:stuck, %{
               partition_key: "tenant-a",
               type: "invoice",
               from_ms: 701,
               now_ms: 700
             })
  end

  test "preserves typed metadata values as bound parameters" do
    for value <- ["", 42, 3.5, true, false] do
      assert {:ok, %{query: query, params: params}} =
               Builder.build(:search, %{
                 partition_key: "tenant-a",
                 type: "invoice",
                 state_meta: {"queued", "risk", value}
               })

      assert query =~ "state_meta.queued.risk = @state_meta_value"
      assert params["state_meta_value"] === value
      assert {:ok, _request} = Query.prepare_reference("FQL1", query, params)
    end
  end

  test "rejects unscoped, unbounded, and incomplete collection requests" do
    assert {:error, :query_partition_required} =
             Builder.build(:list, %{type: "invoice"})

    assert {:error, :query_limit_exceeded} =
             Builder.build(:list, %{partition_key: "tenant-a", type: "invoice", limit: 101})

    assert {:error, :query_filter_required} =
             Builder.build(:search, %{partition_key: "tenant-a", type: "invoice"})
  end
end
