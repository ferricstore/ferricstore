# mise exec -- mix run --no-start bench/oss_review_perf_summary.exs
# Summarizes complete, matched trials; excludes the smoke runs.

defmodule FerricstoreBench.OSSReviewSummary do
  def run do
    trials =
      for path <- Path.wildcard("bench/output/oss-review/*.json"),
          Regex.match?(~r/\/(release|review)-\d+\.json$/, path) do
        Jason.decode!(File.read!(path))
      end

    releases = Enum.filter(trials, &(&1["variant"] == "release"))
    reviews = Enum.filter(trials, &(&1["variant"] == "review"))
    ids = fn runs -> Enum.map(runs, & &1["trial"]) |> Enum.sort() end

    if length(releases) < 3 or ids.(releases) != ids.(reviews),
      do: raise("need at least three complete, matched release/review trials")

    for key <- [
          "base",
          "schedulers",
          "dirty_io_schedulers",
          "backend",
          "shard_count",
          "elixir",
          "otp",
          "async_seconds",
          "list_seconds",
          "warmup_seconds"
        ] do
      if length(Enum.uniq_by(trials, & &1[key])) != 1, do: raise("mismatched #{key}")
    end

    for runs <- [releases, reviews] do
      if length(Enum.uniq_by(runs, & &1["source_sha256"])) != 1,
        do: raise("sources changed between trials of the same variant")
    end

    reference_scenarios = Enum.map(hd(trials)["scenarios"], &{&1["scenario"], &1["concurrency"]})

    for trial <- trials do
      if Enum.map(trial["scenarios"], &{&1["scenario"], &1["concurrency"]}) != reference_scenarios,
        do: raise("mismatched scenario list")
    end

    rows =
      for {scenario, concurrency} <- reference_scenarios do
        variants =
          for {name, runs} <- [{"release", releases}, {"review", reviews}], into: %{} do
            entries =
              Enum.map(runs, fn run ->
                Enum.find(
                  run["scenarios"],
                  &(&1["scenario"] == scenario and &1["concurrency"] == concurrency)
                )
              end)

            stats =
              for metric <- ["ops_per_second", "p50_us", "p95_us", "p99_us", "samples"],
                  into: %{} do
                values = Enum.map(entries, & &1[metric]) |> Enum.sort()
                {metric, %{min: hd(values), median: median(values), max: List.last(values)}}
              end

            {name, stats}
          end

        delta =
          100 *
            (variants["review"]["ops_per_second"].median /
               variants["release"]["ops_per_second"].median - 1)

        Map.merge(variants, %{
          scenario: scenario,
          concurrency: concurrency,
          throughput_delta_pct: delta
        })
      end

    result = %{
      summary: rows,
      trials: trials,
      aggregation: "median and range of trial-level metrics; not pooled percentiles"
    }

    output = "bench/results/oss-review-perf-0.11.23.json"
    File.mkdir_p!(Path.dirname(output))
    File.write!(output, Jason.encode!(result, pretty: true))

    IO.puts(
      "| Scenario | Clients | Release ops/s | Review ops/s | Delta | Release p95/p99 us | Review p95/p99 us |"
    )

    IO.puts("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")

    for row <- rows do
      old = row["release"]
      new = row["review"]

      IO.puts(
        "| #{row.scenario} | #{row.concurrency} | #{fmt(old["ops_per_second"].median)} | #{fmt(new["ops_per_second"].median)} | #{fmt(row.throughput_delta_pct)}% | #{fmt(old["p95_us"].median)} / #{fmt(old["p99_us"].median)} | #{fmt(new["p95_us"].median)} / #{fmt(new["p99_us"].median)} |"
      )

      IO.puts(
        "range #{row.scenario}/#{row.concurrency}: release #{fmt(old["ops_per_second"].min)}–#{fmt(old["ops_per_second"].max)}; review #{fmt(new["ops_per_second"].min)}–#{fmt(new["ops_per_second"].max)}"
      )
    end

    IO.puts("Saved complete trial data and summary to #{output}")
  end

  defp median(values) do
    n = length(values)

    if rem(n, 2) == 1,
      do: Enum.at(values, div(n, 2)),
      else: (Enum.at(values, div(n, 2) - 1) + Enum.at(values, div(n, 2))) / 2
  end

  defp fmt(value), do: :erlang.float_to_binary(value * 1.0, decimals: 2)
end

FerricstoreBench.OSSReviewSummary.run()
