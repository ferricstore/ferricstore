# Compare two BENCH_SAVE result directories produced on the same pinned host.
# A slowdown reproduced across five pairs above BENCH_REGRESSION_LIMIT
# (default 0.15) fails the run.

defmodule Ferricstore.Bench.QueryPerformanceCompare do
  @default_regression_limit 0.15
  @minimum_paired_rounds 5
  @system_identity_fields ~w(os architecture cpu_model otp elixir schedulers_online)
  @compared_fields ~w(median_ns operation_median_ns memory_median_bytes ops_per_second)

  def run(argv) do
    {baseline_path, current_path} = parse_args(argv)
    regression_limit = ratio_env("BENCH_REGRESSION_LIMIT", @default_regression_limit)
    memory_limit = ratio_env("BENCH_MEMORY_REGRESSION_LIMIT", 0.20)
    baseline = load_results(baseline_path)
    current = load_results(current_path)
    ensure_comparable_systems!(baseline.systems, current.systems)

    missing = Map.keys(baseline.metrics) -- Map.keys(current.metrics)
    pairing_errors = pairing_errors(baseline.metrics, current.metrics)

    regressions =
      baseline.metrics
      |> Enum.flat_map(fn {key, baseline_metric} ->
        case current.metrics[key] do
          nil ->
            []

          current_metric ->
            compare_metric(key, baseline_metric, current_metric, regression_limit, memory_limit)
        end
      end)

    if missing != [] do
      IO.puts(:stderr, "Current run is missing #{length(missing)} baseline scenarios:")
      Enum.each(missing, &IO.puts(:stderr, "  missing #{&1}"))
    end

    if pairing_errors != [] do
      IO.puts(:stderr, "Benchmark round pairing errors:")
      Enum.each(pairing_errors, &IO.puts(:stderr, "  #{&1}"))
    end

    if regressions != [] do
      IO.puts(:stderr, "Query performance regressions:")
      Enum.each(regressions, &IO.puts(:stderr, "  #{&1}"))
    end

    if missing != [] or pairing_errors != [] or regressions != [] do
      System.halt(1)
    end

    IO.puts(
      "Query performance comparison passed: #{map_size(baseline.metrics)} scenarios, " <>
        "reproducible paired-round limit=#{percent(regression_limit)}, " <>
        "memory limit=#{percent(memory_limit)}"
    )
  end

  defp parse_args([baseline, current]), do: {baseline, current}

  defp parse_args(_argv) do
    IO.puts(:stderr, "usage: mix run bench/query_performance_compare.exs BASELINE CURRENT")
    System.halt(2)
  end

  defp load_results(path) do
    files = if File.dir?(path), do: Path.wildcard(Path.join(path, "**/*.json")), else: [path]

    if files == [] do
      raise "no JSON benchmark results found at #{path}"
    end

    files
    |> Enum.reduce(%{metrics: %{}, systems: MapSet.new()}, fn file, acc ->
      payload = file |> File.read!() |> Jason.decode!()
      suite = Map.fetch!(payload, "suite")
      metrics = extract_metrics(payload, suite)
      system = Map.get(payload, "system", %{})
      round = benchmark_round(path, file)

      merged =
        Enum.reduce(metrics, acc.metrics, fn {key, metric}, inner ->
          Map.update(inner, key, [{round, metric}], &[{round, metric} | &1])
        end)

      %{metrics: merged, systems: MapSet.put(acc.systems, system)}
    end)
    |> Map.update!(:metrics, fn metrics ->
      Map.new(metrics, fn {key, values} -> {key, summarize_metrics(values)} end)
    end)
  end

  defp benchmark_round(root, file) do
    if File.dir?(root) do
      case file |> Path.relative_to(root) |> Path.split() do
        [round | _rest] -> if Regex.match?(~r/^round-[1-9][0-9]*$/, round), do: round
        _other -> nil
      end
    end
  end

  defp summarize_metrics(entries) do
    rounds =
      Enum.reduce(entries, %{}, fn
        {nil, _metric}, acc ->
          acc

        {round, metric}, acc ->
          if Map.has_key?(acc, round), do: raise("duplicate benchmark metric for #{round}")
          Map.put(acc, round, metric)
      end)

    %{
      aggregate: entries |> Enum.map(&elem(&1, 1)) |> aggregate_metrics(),
      rounds: rounds
    }
  end

  defp extract_metrics(payload, suite) do
    [Map.get(payload, "scenarios", %{}), Map.get(payload, "metrics", %{})]
    |> Enum.reduce(%{}, fn metrics, acc ->
      Enum.reduce(metrics, acc, fn {name, metric}, inner ->
        if is_number(metric["median_ns"]) do
          Map.put(inner, "#{suite}/#{name}", metric)
        else
          inner
        end
      end)
    end)
  end

  defp ensure_comparable_systems!(baseline, current) do
    unless allow_system_mismatch?() do
      mismatches =
        Enum.flat_map(@system_identity_fields, fn field ->
          baseline_values = system_values(baseline, field)
          current_values = system_values(current, field)

          cond do
            MapSet.size(baseline_values) > 1 ->
              ["baseline mixes #{field} values: #{inspect(MapSet.to_list(baseline_values))}"]

            MapSet.size(current_values) > 1 ->
              ["current mixes #{field} values: #{inspect(MapSet.to_list(current_values))}"]

            MapSet.size(baseline_values) == 1 and MapSet.size(current_values) == 1 and
                baseline_values != current_values ->
              [
                "#{field} differs: baseline=#{inspect(MapSet.to_list(baseline_values))} " <>
                  "current=#{inspect(MapSet.to_list(current_values))}"
              ]

            true ->
              []
          end
        end)

      if mismatches != [] do
        raise "benchmark systems are not comparable; rerun on one host or set " <>
                "BENCH_ALLOW_SYSTEM_MISMATCH=1:\n  #{Enum.join(mismatches, "\n  ")}"
      end
    end
  end

  defp system_values(systems, field) do
    systems
    |> Enum.map(&Map.get(&1, field))
    |> Enum.reject(&is_nil/1)
    |> MapSet.new()
  end

  defp allow_system_mismatch? do
    System.get_env("BENCH_ALLOW_SYSTEM_MISMATCH", "0") in ["1", "true", "TRUE"]
  end

  defp pairing_errors(baseline, current) do
    baseline
    |> Enum.flat_map(fn {key, baseline_metric} ->
      case current[key] do
        nil ->
          []

        current_metric ->
          baseline_rounds = baseline_metric.rounds |> Map.keys() |> MapSet.new()
          current_rounds = current_metric.rounds |> Map.keys() |> MapSet.new()

          cond do
            baseline_rounds != current_rounds ->
              [
                "#{key} rounds differ: baseline=#{inspect(Enum.sort(baseline_rounds))} " <>
                  "current=#{inspect(Enum.sort(current_rounds))}"
              ]

            MapSet.size(baseline_rounds) in 1..(@minimum_paired_rounds - 1) ->
              [
                "#{key} requires at least #{@minimum_paired_rounds} paired rounds; " <>
                  "found #{MapSet.size(baseline_rounds)}"
              ] ++ field_pairing_errors(key, baseline_metric.rounds, current_metric.rounds)

            true ->
              field_pairing_errors(key, baseline_metric.rounds, current_metric.rounds)
          end
      end
    end)
  end

  defp field_pairing_errors(key, baseline_rounds, current_rounds) do
    Enum.flat_map(@compared_fields, fn field ->
      baseline_fields = numeric_field_rounds(baseline_rounds, field)
      current_fields = numeric_field_rounds(current_rounds, field)

      if MapSet.size(baseline_fields) == 0 or baseline_fields == current_fields do
        []
      else
        [
          "#{key} #{field} rounds differ: baseline=#{inspect(Enum.sort(baseline_fields))} " <>
            "current=#{inspect(Enum.sort(current_fields))}"
        ]
      end
    end)
  end

  defp numeric_field_rounds(rounds, field) do
    rounds
    |> Enum.reduce(MapSet.new(), fn {round, metric}, acc ->
      if is_number(metric[field]), do: MapSet.put(acc, round), else: acc
    end)
  end

  defp aggregate_metrics(metrics) do
    fields = metrics |> Enum.flat_map(&Map.keys/1) |> Enum.uniq()

    Map.new(fields, fn field ->
      values = metrics |> Enum.map(& &1[field]) |> Enum.filter(&is_number/1)
      {field, if(values == [], do: nil, else: median(values))}
    end)
  end

  defp median(values) do
    sorted = Enum.sort(values)
    middle = div(length(sorted), 2)

    if rem(length(sorted), 2) == 1 do
      Enum.at(sorted, middle)
    else
      (Enum.at(sorted, middle - 1) + Enum.at(sorted, middle)) / 2
    end
  end

  defp compare_metric(key, baseline, current, runtime_limit, memory_limit) do
    []
    |> compare_value(key, "median_ns", baseline, current, runtime_limit)
    |> compare_value(key, "operation_median_ns", baseline, current, runtime_limit)
    |> compare_value(key, "memory_median_bytes", baseline, current, memory_limit)
    |> compare_decrease(key, "ops_per_second", baseline, current, runtime_limit)
  end

  defp compare_value(regressions, key, field, baseline, current, limit) do
    baseline_value = baseline.aggregate[field]
    current_value = current.aggregate[field]
    paired = paired_ratio(baseline.rounds, current.rounds, field)

    {change, reproduced?} =
      case paired do
        %{median: ratio, ratios: ratios} ->
          {ratio - 1.0, Enum.all?(ratios, &(&1 > 1.0 + limit))}

        nil ->
          change = aggregate_increase(baseline_value, current_value)
          {change, is_number(change) and change > limit}
      end

    if reproduced? do
      evidence = comparison_evidence(paired, baseline_value, current_value)

      [
        "#{key} #{field} increased #{percent(change)} (#{evidence}, limit #{percent(limit)})"
        | regressions
      ]
    else
      regressions
    end
  end

  defp compare_decrease(regressions, key, field, baseline, current, limit) do
    baseline_value = baseline.aggregate[field]
    current_value = current.aggregate[field]
    paired = paired_ratio(baseline.rounds, current.rounds, field)

    {change, reproduced?} =
      case paired do
        %{median: ratio, ratios: ratios} ->
          {1.0 - ratio, Enum.all?(ratios, &(&1 < 1.0 - limit))}

        nil ->
          change = aggregate_decrease(baseline_value, current_value)
          {change, is_number(change) and change > limit}
      end

    if reproduced? do
      evidence = comparison_evidence(paired, baseline_value, current_value)

      [
        "#{key} #{field} decreased #{percent(change)} (#{evidence}, limit #{percent(limit)})"
        | regressions
      ]
    else
      regressions
    end
  end

  defp paired_ratio(baseline_rounds, current_rounds, field) do
    ratios =
      baseline_rounds
      |> Map.keys()
      |> Enum.sort()
      |> Enum.flat_map(fn round ->
        baseline = get_in(baseline_rounds, [round, field])
        current = get_in(current_rounds, [round, field])

        if is_number(baseline) and baseline > 0 and is_number(current),
          do: [current / baseline],
          else: []
      end)

    if ratios == [],
      do: nil,
      else: %{median: median(ratios), ratios: ratios, count: length(ratios)}
  end

  defp aggregate_increase(baseline, current)
       when is_number(baseline) and baseline > 0 and is_number(current),
       do: current / baseline - 1.0

  defp aggregate_increase(_baseline, _current), do: nil

  defp aggregate_decrease(baseline, current)
       when is_number(baseline) and baseline > 0 and is_number(current),
       do: 1.0 - current / baseline

  defp aggregate_decrease(_baseline, _current), do: nil

  defp comparison_evidence(%{median: ratio, count: count}, _baseline, _current),
    do: "paired-round ratio=#{Float.round(ratio, 4)} across #{count} pairs"

  defp comparison_evidence(nil, baseline, current),
    do: "#{format_number(baseline)} -> #{format_number(current)}"

  defp ratio_env(name, default) do
    case System.get_env(name) do
      nil ->
        default

      raw ->
        case Float.parse(raw) do
          {value, ""} when value >= 0.0 -> value
          _ -> raise ArgumentError, "#{name} must be a non-negative ratio"
        end
    end
  end

  defp percent(value), do: "#{Float.round(value * 100, 2)}%"
  defp format_number(value) when is_float(value), do: Float.round(value, 2)
  defp format_number(value), do: value
end

Ferricstore.Bench.QueryPerformanceCompare.run(System.argv())
