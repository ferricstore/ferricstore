Code.require_file("support/query_performance.exs", __DIR__)

defmodule Ferricstore.Bench.QueryPlannerMetadataCandidates do
  @moduledoc false

  alias Ferricstore.Bench.QueryPerformance

  alias Ferricstore.Flow.Query.{
    IndexDefinition,
    IndexStatistics,
    Planner,
    RegisteredIndex,
    Request
  }

  @now_ms 1_800_000_000_000
  @scope "tenant-a"

  def run do
    inputs =
      QueryPerformance.integer_list_env("BENCH_PLANNER_INDEX_COUNTS", [1, 8, 32], min: 1)
      |> Map.new(fn count -> {"#{count} active indexes", dataset(count)} end)

    Enum.each(inputs, fn {_name, dataset} ->
      {:ok, plan} = plan(dataset)
      true = plan.index_id == "planner-metadata-01"
      true = plan.alternatives == []
    end)

    Benchee.run(
      %{"plan collection with scoped statistics" => &plan/1},
      [inputs: inputs] ++ QueryPerformance.benchee_options("query-planner-metadata")
    )
  end

  defp plan(dataset) do
    Planner.plan(dataset.request, dataset.indexes,
      now_ms: @now_ms,
      stats: dataset.statistics
    )
  end

  defp dataset(count) do
    indexes = Enum.map(1..count, &index/1)

    statistics =
      Map.new(indexes, fn index ->
        identity = {index.definition.id, index.definition.version}
        {identity, statistics(index)}
      end)

    %{
      indexes: indexes,
      statistics: statistics,
      request:
        Request.collection(
          :execute,
          [
            {:eq, :partition_key, {:literal, :keyword, @scope}},
            {:eq, :state, {:literal, :keyword, "queued"}}
          ],
          [{:updated_at_ms, :desc}],
          25,
          :record
        )
    }
  end

  defp index(number) do
    definition =
      IndexDefinition.new!(%{
        id: "planner-metadata-#{number |> Integer.to_string() |> String.pad_leading(2, "0")}",
        version: 1,
        fields: [
          {:partition_key, :asc},
          {:state, :asc},
          {:updated_at_ms, :desc}
        ]
      })

    RegisteredIndex.new!(definition, :active,
      coverage: %{complete_shards: 1, total_shards: 1, validation: :passed}
    )
  end

  defp statistics(index) do
    prefix_counts = %{
      IndexStatistics.prefix_digest([@scope]) => 10_000,
      IndexStatistics.prefix_digest([@scope, "queued"]) => 1_000
    }

    observed = Map.new(prefix_counts, fn {digest, _count} -> {digest, @now_ms} end)

    IndexStatistics.new!(%{
      index_id: index.definition.id,
      index_version: index.definition.version,
      scope_digest: IndexStatistics.scope_digest(@scope),
      collected_at_ms: @now_ms,
      source_watermark: 10_000,
      total_entries: 10_000,
      distinct_runs: 10_000,
      prefix_counts: prefix_counts,
      prefix_observed_at_ms: observed,
      histograms: %{},
      null_counts: %{},
      missing_counts: %{},
      average_entry_bytes: 96,
      average_row_bytes: 384,
      sample_rate_ppm: 1_000_000,
      confidence: :high
    })
  end
end

Ferricstore.Bench.QueryPlannerMetadataCandidates.run()
