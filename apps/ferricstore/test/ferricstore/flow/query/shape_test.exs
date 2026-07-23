defmodule Ferricstore.Flow.Query.ShapeTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.{Request, Shape}

  test "classifies point and history requests with their execution descriptors" do
    implicit = Request.point_read(:execute, keyword("run-1"))
    explicit = Request.point_read(:execute, keyword("tenant-a"), keyword("run-1"))

    assert {:ok, :runs_by_run_id_record} = Shape.classify(implicit)
    assert {:ok, :runs_by_partition_and_run_id_record} = Shape.classify(explicit)

    assert {:ok, %{partitioning: :auto, run_id: "run-1"}} = Shape.point_descriptor(implicit)

    assert {:ok, %{partitioning: :explicit, partition_key: "tenant-a", run_id: "run-1"}} =
             Shape.point_descriptor(explicit)

    history =
      Request.history(
        :execute,
        [{:eq, :run_id, keyword("run-1")}],
        :desc,
        25
      )

    assert {:ok, :events_by_run_id_ordered_records} = Shape.classify(history)

    assert {:ok, %{partition_key: partition, run_id: "run-1", direction: :desc, limit: 25}} =
             Shape.history_descriptor(history)

    assert partition == Ferricstore.Flow.Keys.auto_partition_key("run-1")
  end

  test "classifies generic, count, lineage, and fixed-index collection shapes" do
    assert_shape(
      :runs_by_partition_predicates_ordered_records,
      collection([eq(:partition_key, "tenant-a"), eq(:type, "invoice")])
    )

    assert_shape(
      :runs_by_partition_predicates_count,
      Request.count(:execute, [eq(:partition_key, "tenant-a"), eq(:state, "queued")])
    )

    assert_shape(
      :runs_by_partition_parent_ordered_records,
      collection([eq(:partition_key, "tenant-a"), eq(:parent_flow_id, "parent-1")])
    )

    assert_shape(
      :runs_by_partition_type_state_ordered_records,
      collection([
        eq(:partition_key, "tenant-a"),
        eq(:type, "invoice"),
        eq(:state, "queued")
      ])
    )

    assert_shape(
      :runs_by_partition_type_terminals_ordered_records,
      collection([
        eq(:partition_key, "tenant-a"),
        eq(:type, "invoice"),
        {:in, :state, Enum.map(~w(completed failed cancelled), &keyword/1)}
      ])
    )

    assert_shape(
      :runs_by_partition_metadata_ordered_records,
      collection([
        eq(:partition_key, "tenant-a"),
        eq(:type, "invoice"),
        eq({:attribute, "region"}, "eu")
      ])
    )

    stuck =
      collection(
        [
          eq(:partition_key, "tenant-a"),
          eq(:type, "invoice"),
          eq(:state, "running"),
          {:range, :lease_deadline_ms, integer(25), integer(100)}
        ],
        :lease_deadline_ms
      )

    assert_shape(
      :runs_by_partition_type_running_lease_deadline_ordered_records,
      stuck
    )

    assert {:ok, %{lease_range: {25, 100}}} = Shape.fixed_descriptor(stuck)
  end

  test "the default OSS engine owns every parser-supported execution shape" do
    assert Shape.known_names() == Ferricstore.Flow.Query.Surface.shapes()

    assert Shape.execution_names() == Shape.known_names()

    assert Shape.execution_names() ==
             Ferricstore.Flow.Query.Surface.default_capability_manifest().shapes

    assert "runs_by_partition_predicates_ordered_records" in Shape.execution_names()
    assert "runs_by_partition_type_state_ordered_records" in Shape.execution_names()
  end

  test "rejects ranges that cannot be represented exactly by fixed score indexes" do
    max_exact = 9_007_199_254_740_991

    for {field, lower, upper} <- [
          {:updated_at_ms, -1, 100},
          {:updated_at_ms, 0, max_exact + 1},
          {:lease_deadline_ms, 0, max_exact + 1}
        ] do
      predicates =
        [eq(:partition_key, "tenant-a"), eq(:type, "invoice")] ++
          if field == :lease_deadline_ms, do: [eq(:state, "running")], else: []

      request =
        collection(
          predicates ++ [{:range, field, integer(lower), integer(upper)}],
          field
        )

      assert {:error, :unsupported_query_shape} = Shape.fixed_descriptor(request)
    end
  end

  defp assert_shape(expected, request), do: assert({:ok, ^expected} = Shape.classify(request))

  defp collection(predicates, order_field \\ :updated_at_ms) do
    Request.collection(:execute, predicates, [{order_field, :desc}], 10, :record)
  end

  defp eq(field, value), do: {:eq, field, keyword(value)}
  defp keyword(value), do: {:literal, :keyword, value}
  defp integer(value), do: {:literal, :integer, value}
end
