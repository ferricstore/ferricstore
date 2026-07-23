# Compare two BENCH_SAVE result directories produced on the same pinned host.
# A median slowdown above BENCH_REGRESSION_LIMIT (default 0.15) fails the run.

defmodule Ferricstore.Bench.QueryPerformanceCompare do
  @default_regression_limit 0.15
  @system_identity_fields ~w(os architecture cpu_model otp elixir schedulers_online)

  def run(argv) do
    {baseline_path, current_path} = parse_args(argv)
    regression_limit = ratio_env("BENCH_REGRESSION_LIMIT", @default_regression_limit)
    memory_limit = ratio_env("BENCH_MEMORY_REGRESSION_LIMIT", 0.20)
    baseline = load_results(baseline_path)
    current = load_results(current_path)
    ensure_comparable_systems!(baseline.systems, current.systems)

    missing = Map.keys(baseline.metrics) -- Map.keys(current.metrics)

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

    if regressions != [] do
      IO.puts(:stderr, "Query performance regressions:")
      Enum.each(regressions, &IO.puts(:stderr, "  #{&1}"))
    end

    if missing != [] or regressions != [] do
      System.halt(1)
    end

    IO.puts(
      "Query performance comparison passed: #{map_size(baseline.metrics)} scenarios, " <>
        "median limit=#{percent(regression_limit)}, memory limit=#{percent(memory_limit)}"
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

      merged =
        Map.merge(acc.metrics, metrics, fn _key, existing, metric ->
          [metric | List.wrap(existing)]
        end)

      %{metrics: merged, systems: MapSet.put(acc.systems, system)}
    end)
    |> Map.update!(:metrics, fn metrics ->
      Map.new(metrics, fn {key, values} -> {key, aggregate_metrics(List.wrap(values))} end)
    end)
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
    baseline_value = baseline[field]
    current_value = current[field]

    if is_number(baseline_value) and baseline_value > 0 and is_number(current_value) and
         current_value > baseline_value * (1.0 + limit) do
      change = current_value / baseline_value - 1.0

      [
        "#{key} #{field} increased #{percent(change)} " <>
          "(#{format_number(baseline_value)} -> #{format_number(current_value)}, " <>
          "limit #{percent(limit)})"
        | regressions
      ]
    else
      regressions
    end
  end

  defp compare_decrease(regressions, key, field, baseline, current, limit) do
    baseline_value = baseline[field]
    current_value = current[field]

    if is_number(baseline_value) and baseline_value > 0 and is_number(current_value) and
         current_value < baseline_value * (1.0 - limit) do
      change = 1.0 - current_value / baseline_value

      [
        "#{key} #{field} decreased #{percent(change)} " <>
          "(#{format_number(baseline_value)} -> #{format_number(current_value)}, " <>
          "limit #{percent(limit)})"
        | regressions
      ]
    else
      regressions
    end
  end

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
