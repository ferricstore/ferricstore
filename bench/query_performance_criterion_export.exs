# Export one Criterion result label into the JSON schema consumed by
# query_performance_compare.exs.

Code.require_file("support/query_performance.exs", __DIR__)

defmodule Ferricstore.Bench.QueryPerformanceCriterionExport do
  alias Ferricstore.Bench.QueryPerformance

  def run([criterion_home, label, output_dir]) do
    paths = Path.wildcard(Path.join([criterion_home, "**", label, "estimates.json"]))

    if paths == [] do
      raise "no Criterion #{label} estimates found under #{criterion_home}"
    end

    scenarios =
      Map.new(paths, fn path ->
        estimates = path |> File.read!() |> Jason.decode!()
        relative = Path.relative_to(path, criterion_home)

        name =
          relative
          |> Path.split()
          |> Enum.drop(-2)
          |> Enum.join("/")

        median = estimates["median"]

        {name,
         %{
           "median_ns" => estimates["median"]["point_estimate"],
           "confidence_lower_ns" => median["confidence_interval"]["lower_bound"],
           "confidence_upper_ns" => median["confidence_interval"]["upper_bound"],
           "memory_median_bytes" => nil
         }}
      end)

    payload =
      QueryPerformance.result_payload("fql-rust-criterion", %{
        "criterion_label" => label,
        "scenarios" => scenarios
      })

    File.mkdir_p!(output_dir)
    path = Path.join(output_dir, "fql-rust-criterion.json")
    File.write!(path, Jason.encode!(payload, pretty: true))
    IO.puts("Saved #{map_size(scenarios)} Criterion estimates to #{path}")
  end

  def run(_argv) do
    raise "usage: mix run bench/query_performance_criterion_export.exs CRITERION_HOME LABEL OUTPUT_DIR"
  end
end

Ferricstore.Bench.QueryPerformanceCriterionExport.run(System.argv())
