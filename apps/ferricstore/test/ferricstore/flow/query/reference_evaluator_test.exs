defmodule Ferricstore.Flow.Query.ReferenceEvaluatorTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.{Field, ReferenceEvaluator, Request}

  @records [
    %{
      id: "run-1",
      partition_key: "tenant-a",
      type: "invoice",
      state: "failed",
      updated_at_ms: 110,
      priority: 2,
      attributes: %{"region" => "eu", "tags" => ["urgent", "finance"]},
      state_meta: %{"failed" => %{"reason" => "timeout"}}
    },
    %{
      id: "run-2",
      partition_key: "tenant-a",
      type: "invoice",
      state: "completed",
      updated_at_ms: 120,
      priority: 1,
      attributes: %{"region" => "us"}
    },
    %{
      id: "run-3",
      partition_key: "tenant-a",
      type: "invoice",
      state: "failed",
      updated_at_ms: 130,
      priority: nil,
      attributes: %{"region" => "eu"}
    },
    %{
      id: "run-4",
      partition_key: "tenant-b",
      type: "invoice",
      state: "failed",
      updated_at_ms: 125,
      attributes: %{"region" => "eu"}
    }
  ]

  test "evaluates equality, IN, bounded time windows, and metadata predicates" do
    request =
      Request.collection(
        :execute,
        [
          {:eq, :partition_key, literal("tenant-a")},
          {:eq, :type, literal("invoice")},
          {:in, :state, [literal("failed"), literal("completed")]},
          {:time_window, :updated_at_ms, integer(105), integer(125)},
          {:eq, {:attribute, "region"}, literal("eu")}
        ],
        [{:updated_at_ms, :desc}],
        10,
        :record
      )

    assert :ok = Request.validate_bound(request)
    assert {:ok, [%{id: "run-1"}]} = ReferenceEvaluator.execute(@records, request)
  end

  test "time windows include the lower bound and exclude the upper bound" do
    records = [
      %{id: "lower", partition_key: "tenant-a", updated_at_ms: 100},
      %{id: "inside", partition_key: "tenant-a", updated_at_ms: 199},
      %{id: "upper", partition_key: "tenant-a", updated_at_ms: 200}
    ]

    request =
      Request.collection(
        :execute,
        [
          {:eq, :partition_key, literal("tenant-a")},
          {:time_window, :updated_at_ms, integer(100), integer(200)}
        ],
        [{:updated_at_ms, :asc}],
        10,
        :record
      )

    assert {:ok, result} = ReferenceEvaluator.execute(records, request)
    assert Enum.map(result, & &1.id) == ["lower", "inside"]
  end

  test "attribute list equality uses membership semantics" do
    request =
      Request.collection(
        :execute,
        [
          {:eq, :partition_key, literal("tenant-a")},
          {:eq, {:attribute, "tags"}, literal("urgent")}
        ],
        [{:updated_at_ms, :asc}],
        10,
        :record
      )

    assert {:ok, [%{id: "run-1"}]} = ReferenceEvaluator.execute(@records, request)
  end

  test "keeps null and missing semantics distinct" do
    null_request =
      Request.collection(
        :execute,
        [{:eq, :partition_key, literal("tenant-a")}, {:is, :priority, :null}],
        [{:updated_at_ms, :asc}],
        10,
        :record
      )

    missing_request =
      Request.collection(
        :execute,
        [{:eq, :partition_key, literal("tenant-a")}, {:is, :priority, :missing}],
        [{:updated_at_ms, :asc}],
        10,
        :record
      )

    assert {:ok, [%{id: "run-3"}]} = ReferenceEvaluator.execute(@records, null_request)
    assert {:ok, []} = ReferenceEvaluator.execute(@records, missing_request)
    assert ReferenceEvaluator.matches?(List.last(@records), {:is, :priority, :missing})
    refute ReferenceEvaluator.matches?(List.last(@records), {:is, :priority, :null})
  end

  test "applies deterministic null/missing ordering and an opaque stable tie breaker" do
    records = [
      %{id: "z", partition_key: "tenant-a", priority: 1},
      %{id: "b", partition_key: "tenant-a", priority: nil},
      %{id: "a", partition_key: "tenant-a"},
      %{id: "a2", partition_key: "tenant-a", priority: 1}
    ]

    request =
      Request.collection(
        :execute,
        [{:eq, :partition_key, literal("tenant-a")}],
        [{:priority, :asc}],
        10,
        :record
      )

    assert {:ok, result} = ReferenceEvaluator.execute(records, request)
    concrete_ids = opaque_order(["z", "a2"])
    assert Enum.map(result, & &1.id) == concrete_ids ++ ["b", "a"]

    descending = %{request | order_by: [{:priority, :desc}]}
    assert {:ok, descending_result} = ReferenceEvaluator.execute(records, descending)
    assert Enum.map(descending_result, & &1.id) == concrete_ids ++ ["b", "a"]
  end

  test "generated predicate combinations agree with direct scalar evaluation" do
    values = [Field.missing(), nil, -2, 0, 3, "", "a", "z", false, true]

    for actual <- values, expected <- values do
      record = if actual == Field.missing(), do: %{}, else: %{priority: actual}
      predicate = {:eq, :priority, inferred_literal(expected)}

      expected_match = expected not in [Field.missing(), nil] and actual == expected
      assert ReferenceEvaluator.matches?(record, predicate) == expected_match
    end
  end

  test "ordinary comparisons reject null and missing sentinels" do
    for value <- [
          {:literal, :null, nil},
          {:literal, :missing, Field.missing()}
        ] do
      request =
        Request.collection(
          :execute,
          [{:eq, :partition_key, literal("tenant-a")}, {:eq, :priority, value}],
          [{:updated_at_ms, :asc}],
          10,
          :record
        )

      assert {:error, :invalid_parameter_type} = Request.validate_bound(request)
      refute ReferenceEvaluator.matches?(%{priority: nil}, {:eq, :priority, value})
    end
  end

  test "rejects unbounded execution and invalid mixed-type ranges" do
    invalid =
      Request.collection(
        :execute,
        [
          {:eq, :partition_key, literal("tenant-a")},
          {:range, :priority, integer(1), literal("9")}
        ],
        [{:updated_at_ms, :asc}],
        10,
        :record
      )

    assert {:error, :invalid_parameter_type} = Request.validate_bound(invalid)
    assert {:error, :invalid_query_request} = ReferenceEvaluator.execute(@records, invalid)
  end

  test "accepts mixed literal and parameter bounds before binding" do
    request =
      Request.collection(
        :execute,
        [
          {:eq, :partition_key, {:parameter, :keyword, "tenant"}},
          {:range, :updated_at_ms, integer(1), {:parameter, :integer, "until"}}
        ],
        [{:updated_at_ms, :asc}],
        10,
        :record
      )

    assert :ok = Request.validate_unbound(request)
  end

  test "rejects reversed literal ranges before execution" do
    for predicate <- [
          {:range, :updated_at_ms, integer(20), integer(10)},
          {:time_window, :updated_at_ms, integer(20), integer(10)}
        ] do
      request =
        Request.collection(
          :execute,
          [{:eq, :partition_key, literal("tenant-a")}, predicate],
          [{:updated_at_ms, :asc}],
          10,
          :record
        )

      assert {:error, :invalid_predicate_range} = Request.validate_bound(request)
    end
  end

  defp literal(value), do: {:literal, :keyword, value}
  defp integer(value), do: {:literal, :integer, value}

  defp inferred_literal({:ferric_query, :missing} = value), do: {:literal, :missing, value}
  defp inferred_literal(nil), do: {:literal, :null, nil}
  defp inferred_literal(value) when is_binary(value), do: {:literal, :keyword, value}
  defp inferred_literal(value) when is_integer(value), do: {:literal, :integer, value}
  defp inferred_literal(value) when is_boolean(value), do: {:literal, :boolean, value}

  defp opaque_order(ids), do: Enum.sort_by(ids, &:crypto.hash(:sha256, &1))
end
