defmodule Ferricstore.Flow.Query.IndexStatus do
  @moduledoc false

  alias Ferricstore.Flow.Query.{Field, IndexDefinition, Surface}

  alias Ferricstore.Flow.Query.{
    IndexLifecycleWorker,
    IndexRegistry,
    IndexStatistics,
    StatisticsStore,
    StatisticsWorker
  }

  @spec fetch(map(), binary() | nil, keyword()) ::
          {:ok, map()}
          | {:error,
             :invalid_query_index_filter
             | :query_index_not_found
             | :query_index_registry_unavailable}
  def fetch(ctx, index_id \\ nil, opts \\ [])

  def fetch(%{name: name} = ctx, index_id, opts)
      when is_atom(name) and is_list(opts) do
    now_ms = Keyword.get(opts, :now_ms, System.system_time(:millisecond))
    registry = Keyword.get(opts, :registry, IndexRegistry.server_name(ctx))

    with :ok <- validate_filter(index_id),
         true <- is_integer(now_ms) and now_ms >= 0,
         {:ok, overview} <- registry_overview(registry),
         {:ok, indexes} <- filter_indexes(overview.indexes, index_id) do
      identities = MapSet.new(indexes, &{&1.id, &1.version})
      {statistics, statistics_status} = statistics(ctx, identities, now_ms)

      {:ok,
       %{
         "contract_version" => Surface.index_status_contract(),
         "observed_at_ms" => now_ms,
         "registry" => %{
           "epoch" => overview.epoch,
           "catalog_version" => overview.catalog_version
         },
         "services" => services(ctx, registry, statistics_status),
         "statistics_max_age_ms" => IndexStatistics.max_age_ms(),
         "indexes" => Enum.map(indexes, &wire_index(&1, statistics))
       }}
    else
      false -> {:error, :invalid_query_index_filter}
      {:error, _reason} = error -> error
    end
  end

  def fetch(_ctx, _index_id, _opts), do: {:error, :invalid_query_index_filter}

  defp validate_filter(nil), do: :ok

  defp validate_filter(index_id) do
    if IndexDefinition.valid_id?(index_id),
      do: :ok,
      else: {:error, :invalid_query_index_filter}
  end

  defp registry_overview(registry) do
    IndexRegistry.overview(registry)
  catch
    :exit, _reason -> {:error, :query_index_registry_unavailable}
  end

  defp filter_indexes(indexes, nil), do: {:ok, indexes}

  defp filter_indexes(indexes, index_id) do
    case Enum.filter(indexes, &(&1.id == index_id)) do
      [] -> {:error, :query_index_not_found}
      matches -> {:ok, matches}
    end
  end

  defp statistics(ctx, identities, now_ms) do
    case StatisticsStore.summaries(ctx, identities, now_ms) do
      {:ok, summaries} -> {summaries, :ready}
      {:error, _reason} -> {%{}, :unavailable}
    end
  end

  defp services(ctx, registry, statistics_status) do
    %{
      "registry" => process_status(registry),
      "lifecycle_worker" => process_status(IndexLifecycleWorker.name(ctx)),
      "statistics_store" => Atom.to_string(statistics_status),
      "statistics_worker" => process_status(StatisticsWorker.server_name(ctx))
    }
  end

  defp process_status(name) when is_atom(name) do
    case Process.whereis(name) do
      pid when is_pid(pid) -> "ready"
      nil -> "unavailable"
    end
  end

  defp process_status(pid) when is_pid(pid) do
    if Process.alive?(pid), do: "ready", else: "unavailable"
  end

  defp wire_index(index, statistics) do
    %{
      "id" => index.id,
      "version" => index.version,
      "build_id" => index.build_id,
      "source" => Atom.to_string(index.source),
      "state" => Atom.to_string(index.state),
      "queryable" => index.queryable,
      "fields" => Enum.map(index.fields, &wire_field/1),
      "workloads" => index.workloads,
      "count_prefixes" => index.count_prefixes,
      "coverage" => wire_coverage(index.coverage),
      "build" => wire_build(index.build),
      "validation" => wire_validation(index.validation),
      "retirement" => wire_retirement(index.retirement),
      "statistics" => wire_statistics(Map.get(statistics, {index.id, index.version}))
    }
  end

  defp wire_field(%{name: name, direction: direction, encoding: encoding}) do
    %{
      "name" => Field.external_name(name),
      "direction" => Atom.to_string(direction),
      "encoding" => Atom.to_string(encoding)
    }
  end

  defp wire_coverage(coverage) do
    %{
      "complete_shards" => coverage.complete_shards,
      "total_shards" => coverage.total_shards,
      "validation" => Atom.to_string(coverage.validation)
    }
  end

  defp wire_build(build) do
    %{
      "scope" => "catalog_build",
      "phase_counts" => wire_phase_counts(build.phase_counts),
      "current_phases" => wire_phases(build.current_phases),
      "completed_shards" => build.completed_shards,
      "total_shards" => build.total_shards,
      "scanned_records" => build.scanned_records,
      "written_entries" => build.written_entries,
      "written_bytes" => build.written_bytes
    }
  end

  defp wire_validation(validation) do
    %{
      "scope" => "catalog_build",
      "status" => Atom.to_string(validation.status),
      "phase_counts" => wire_phase_counts(validation.phase_counts),
      "current_phases" => wire_phases(validation.current_phases),
      "completed_shards" => validation.completed_shards,
      "total_shards" => validation.total_shards,
      "checked_records" => validation.checked_records,
      "checked_entries" => validation.checked_entries,
      "mismatches" => validation.mismatches,
      "failure_reason" => wire_atom(validation.failure_reason),
      "validated_at_ms" => validation.validated_at_ms
    }
  end

  defp wire_retirement(%{status: :not_applicable}),
    do: %{"status" => "not_applicable"}

  defp wire_retirement(retirement) do
    %{
      "status" => Atom.to_string(retirement.status),
      "phase_counts" => wire_phase_counts(retirement.phase_counts),
      "current_phases" => wire_phases(retirement.current_phases),
      "completed_shards" => retirement.completed_shards,
      "total_shards" => retirement.total_shards,
      "deleted_entries" => retirement.deleted_entries,
      "deleted_bytes" => retirement.deleted_bytes,
      "rewritten_reverse_rows" => retirement.rewritten_reverse_rows
    }
  end

  defp wire_statistics(nil) do
    %{
      "status" => "unavailable",
      "samples" => 0,
      "fresh_samples" => 0,
      "stale_samples" => 0,
      "future_samples" => 0,
      "oldest_collected_at_ms" => nil,
      "newest_collected_at_ms" => nil,
      "oldest_age_ms" => nil,
      "newest_age_ms" => nil
    }
  end

  defp wire_statistics(statistics) do
    %{
      "status" => Atom.to_string(statistics.status),
      "samples" => statistics.samples,
      "fresh_samples" => statistics.fresh_samples,
      "stale_samples" => statistics.stale_samples,
      "future_samples" => statistics.future_samples,
      "oldest_collected_at_ms" => statistics.oldest_collected_at_ms,
      "newest_collected_at_ms" => statistics.newest_collected_at_ms,
      "oldest_age_ms" => statistics.oldest_age_ms,
      "newest_age_ms" => statistics.newest_age_ms
    }
  end

  defp wire_phase_counts(counts) do
    Map.new(counts, fn {phase, count} -> {Atom.to_string(phase), count} end)
  end

  defp wire_phases(phases), do: Enum.map(phases, &Atom.to_string/1)
  defp wire_atom(nil), do: nil
  defp wire_atom(value) when is_atom(value), do: Atom.to_string(value)
end
