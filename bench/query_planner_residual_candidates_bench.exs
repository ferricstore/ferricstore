# Compares production prepared predicates with the previous evaluator and an intermediate prototype.

Code.require_file("support/query_performance.exs", __DIR__)

defmodule Ferricstore.Bench.QueryPlannerResidualCandidates do
  @moduledoc false

  alias Ferricstore.Bench.QueryPerformance
  alias Ferricstore.Flow.Query.{Field, ReferenceEvaluator, TupleCodec}

  @page_sizes [1, 64, 512]

  def run do
    case System.get_env("BENCH_CANDIDATE_SECTION", "benchee") do
      "benchee" ->
        benchmark_benchee()

      "paired" ->
        benchmark_paired()

      invalid ->
        raise ArgumentError,
              "BENCH_CANDIDATE_SECTION must be benchee or paired, got #{inspect(invalid)}"
    end
  end

  defp benchmark_benchee do
    inputs =
      for page_size <- @page_sizes,
          {shape, predicates, record} <- predicate_shapes(),
          into: %{} do
        records = List.duplicate(record, page_size)
        {:ok, prepared} = ReferenceEvaluator.prepare(predicates)
        name = "#{page_size} rows/#{shape}"

        expected = current(records, predicates)
        ^expected = fetch_once(records, predicates)
        ^expected = prepared(records, prepared)

        {name, {records, predicates, prepared}}
      end

    Benchee.run(
      %{
        "legacy evaluator" => fn {records, predicates, _prepared} ->
          current(records, predicates)
        end,
        "fetch-field-once prototype" => fn {records, predicates, _prepared} ->
          fetch_once(records, predicates)
        end,
        "production prepared membership" => fn {records, _predicates, prepared} ->
          prepared(records, prepared)
        end
      },
      [inputs: inputs] ++ QueryPerformance.benchee_options("query-planner-residual-candidates")
    )
  end

  defp benchmark_paired do
    samples = QueryPerformance.int_env("BENCH_PAIRED_SAMPLES", 101, min: 3)
    repeats = QueryPerformance.int_env("BENCH_PAIRED_REPEATS", 10, min: 1)

    Enum.each(predicate_shapes(), fn {shape, predicates, record} ->
      records = List.duplicate(record, 512)
      {:ok, prepared} = ReferenceEvaluator.prepare(predicates)
      expected = current(records, predicates)
      ^expected = prepared(records, prepared)

      {current_samples, candidate_samples} =
        Enum.reduce(1..samples, {[], []}, fn sample, {current_acc, candidate_acc} ->
          :erlang.garbage_collect()

          current_run = fn -> repeat(repeats, fn -> current(records, predicates) end) end
          candidate_run = fn -> repeat(repeats, fn -> prepared(records, prepared) end) end

          {current_ns, ^expected, candidate_ns, ^expected} =
            if rem(sample, 2) == 0 do
              {candidate_ns, candidate_result} = QueryPerformance.timed_ns(candidate_run)
              {current_ns, current_result} = QueryPerformance.timed_ns(current_run)
              {current_ns, current_result, candidate_ns, candidate_result}
            else
              {current_ns, current_result} = QueryPerformance.timed_ns(current_run)
              {candidate_ns, candidate_result} = QueryPerformance.timed_ns(candidate_run)
              {current_ns, current_result, candidate_ns, candidate_result}
            end

          {[div(current_ns, repeats) | current_acc], [div(candidate_ns, repeats) | candidate_acc]}
        end)

      current_median = QueryPerformance.percentile(current_samples, 50)
      candidate_median = QueryPerformance.percentile(candidate_samples, 50)

      IO.puts(
        "residual_pair shape=#{shape} current_median_ns=#{current_median} " <>
          "candidate_median_ns=#{candidate_median} " <>
          "speedup=#{Float.round(current_median / candidate_median, 2)}x"
      )
    end)
  end

  defp repeat(count, fun), do: repeat(count, fun, nil)
  defp repeat(0, _fun, result), do: result
  defp repeat(count, fun, _result), do: repeat(count - 1, fun, fun.())

  defp current(records, predicates) do
    Enum.count(records, fn record ->
      Enum.all?(predicates, &legacy_match?(record, &1))
    end)
  end

  defp fetch_once(records, predicates) do
    Enum.count(records, fn record -> Enum.all?(predicates, &fetch_once_match?(record, &1)) end)
  end

  defp prepared(records, predicates) do
    Enum.count(records, fn record ->
      Enum.all?(predicates, &ReferenceEvaluator.matches_prepared?(record, &1))
    end)
  end

  defp predicate_shapes do
    values = Enum.map(1..20, &literal/1)

    [
      {"eq builtin match", [{:eq, :state, literal("failed")}], base_record()},
      {"eq attribute miss", [{:eq, {:attribute, "tier"}, literal("enterprise")}], base_record()},
      {"in20 scalar late match", [{:in, :priority, values}], base_record()},
      {"in20 scalar miss", [{:in, :priority, values}], %{base_record() | priority: 21}},
      {"in20 multivalue late match", [{:in, {:attribute, "scores"}, values}], base_record()},
      {"in20 multivalue miss", [{:in, {:attribute, "scores"}, values}],
       put_in(base_record(), [:attributes, "scores"], Enum.to_list(21..28))},
      {"four mixed predicates",
       [
         {:eq, :state, literal("failed")},
         {:in, :priority, values},
         {:range, :updated_at_ms, literal(1_000), literal(3_000)},
         {:is, {:attribute, "absent"}, :missing}
       ], base_record()}
    ]
  end

  defp base_record do
    %{
      id: "run-1",
      state: "failed",
      priority: 20,
      updated_at_ms: 2_000,
      attributes: %{"tier" => "standard", "scores" => Enum.to_list(13..20)}
    }
  end

  defp fetch_once_match?(record, {:eq, field, value}),
    do: field_value_match?(Field.fetch(record, field), literal_value(value))

  defp fetch_once_match?(record, {:in, field, values}) do
    expected = Enum.map(values, &literal_value/1)
    any_value_match?(Field.fetch(record, field), expected)
  end

  defp fetch_once_match?(record, {:range, field, lower, upper}),
    do: range_match?(Field.fetch(record, field), literal_value(lower), literal_value(upper), true)

  defp fetch_once_match?(record, {:time_window, field, lower, upper}),
    do:
      range_match?(Field.fetch(record, field), literal_value(lower), literal_value(upper), false)

  defp fetch_once_match?(record, {:is, field, :missing}),
    do: Field.fetch(record, field) == :missing

  defp fetch_once_match?(record, {:is, field, :null}),
    do: Field.fetch(record, field) == {:ok, nil}

  defp legacy_match?(record, {:eq, field, value}) do
    expected = literal_value(value)

    if expected in [Field.missing(), nil] do
      false
    else
      field_value_match?(Field.fetch(record, field), expected)
    end
  end

  defp legacy_match?(record, {:in, field, values}),
    do: Enum.any?(values, &legacy_match?(record, {:eq, field, &1}))

  defp legacy_match?(record, {:range, field, lower, upper}),
    do: range_match?(Field.fetch(record, field), literal_value(lower), literal_value(upper), true)

  defp legacy_match?(record, {:time_window, field, lower, upper}),
    do:
      range_match?(Field.fetch(record, field), literal_value(lower), literal_value(upper), false)

  defp legacy_match?(record, {:is, field, :missing}),
    do: Field.fetch(record, field) == :missing

  defp legacy_match?(record, {:is, field, :null}),
    do: Field.fetch(record, field) == {:ok, nil}

  defp field_value_match?(:missing, _expected), do: false
  defp field_value_match?({:ok, _actual}, expected) when expected in [nil, :missing], do: false

  defp field_value_match?({:ok, actual}, expected) when is_list(actual),
    do: Enum.any?(actual, &same_value?(&1, expected))

  defp field_value_match?({:ok, actual}, expected), do: same_value?(actual, expected)

  defp any_value_match?(:missing, _expected), do: false

  defp any_value_match?({:ok, actual}, expected) when is_list(actual),
    do: Enum.any?(actual, fn value -> Enum.any?(expected, &same_value?(value, &1)) end)

  defp any_value_match?({:ok, actual}, expected),
    do: Enum.any?(expected, &same_value?(actual, &1))

  defp range_match?(:missing, _lower, _upper, _upper_inclusive?), do: false

  defp range_match?({:ok, actual}, lower, upper, upper_inclusive?) when is_list(actual),
    do: Enum.any?(actual, &within_range?(&1, lower, upper, upper_inclusive?))

  defp range_match?({:ok, actual}, lower, upper, upper_inclusive?),
    do: within_range?(actual, lower, upper, upper_inclusive?)

  defp within_range?(actual, lower, upper, upper_inclusive?) do
    if same_runtime_type?(actual, lower) and same_runtime_type?(actual, upper) do
      lower_compare = TupleCodec.compare_values(actual, lower)
      upper_compare = TupleCodec.compare_values(actual, upper)

      lower_compare in [:eq, :gt] and
        (upper_compare == :lt or (upper_inclusive? and upper_compare == :eq))
    else
      false
    end
  end

  defp literal(value) when is_integer(value), do: {:literal, :integer, value}
  defp literal(value) when is_binary(value), do: {:literal, :keyword, value}
  defp literal_value({:literal, _type, value}), do: value

  defp same_value?(left, right), do: same_runtime_type?(left, right) and left == right
  defp same_runtime_type?(left, right) when is_integer(left) and is_integer(right), do: true
  defp same_runtime_type?(left, right) when is_float(left) and is_float(right), do: true
  defp same_runtime_type?(left, right) when is_binary(left) and is_binary(right), do: true
  defp same_runtime_type?(left, right) when is_boolean(left) and is_boolean(right), do: true
  defp same_runtime_type?(nil, nil), do: true
  defp same_runtime_type?(_left, _right), do: false
end

Ferricstore.Bench.QueryPlannerResidualCandidates.run()
