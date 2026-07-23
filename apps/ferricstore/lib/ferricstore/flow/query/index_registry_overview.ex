defmodule Ferricstore.Flow.Query.IndexRegistryOverview do
  @moduledoc false

  @build_phases [:pending, :snapshot, :backfill, :done]
  @validation_phases [:pending, :source, :index, :counter, :cleanup, :done]
  @retirement_phases [:pending, :fence, :index, :counter, :reverse, :cleanup, :done]

  @spec build(map(), pos_integer()) :: [map()]
  def build(entries, shard_count)
      when is_map(entries) and is_integer(shard_count) and shard_count > 0 do
    entries
    |> Map.values()
    |> Enum.sort_by(fn entry -> {entry.definition.id, entry.definition.version} end)
    |> Enum.map(&index(&1, shard_count))
  end

  defp index(entry, shard_count) do
    coverage = coverage(entry, shard_count)

    %{
      id: entry.definition.id,
      version: entry.definition.version,
      source: entry.definition.source,
      state: entry.state,
      queryable:
        entry.state == :active and coverage.complete_shards == coverage.total_shards and
          coverage.validation == :passed,
      build_id: entry.build_id,
      fields:
        Enum.map(entry.definition.fields, fn {name, direction, encoding} ->
          %{name: name, direction: direction, encoding: encoding}
        end),
      workloads: entry.definition.workloads,
      count_prefixes: entry.definition.count_prefixes,
      coverage: coverage,
      build: build_progress(entry.checkpoints, shard_count),
      validation: validation_progress(entry.validation, shard_count),
      retirement: retirement_progress(entry.retirement, shard_count)
    }
  end

  defp coverage(entry, shard_count) do
    complete =
      Enum.count(0..(shard_count - 1), fn shard_index ->
        match?(%{phase: :done}, Map.get(entry.checkpoints, shard_index))
      end)

    %{
      complete_shards: complete,
      total_shards: shard_count,
      validation: validation_status(entry.validation)
    }
  end

  defp validation_status(%{status: :passed}), do: :passed
  defp validation_status(%{status: :failed}), do: :failed
  defp validation_status(_validation), do: :pending

  defp build_progress(checkpoints, shard_count) do
    phases = phase_counts(checkpoints, shard_count, @build_phases)

    %{
      phase_counts: phases,
      current_phases: current_phases(phases, @build_phases),
      completed_shards: Map.get(phases, :done, 0),
      total_shards: shard_count,
      scanned_records: counter_sum(checkpoints, :scanned_records),
      written_entries: counter_sum(checkpoints, :written_entries),
      written_bytes: counter_sum(checkpoints, :written_bytes)
    }
  end

  defp validation_progress(validation, shard_count) do
    validation = validation || empty_validation()
    checkpoints = validation.checkpoints
    phases = phase_counts(checkpoints, shard_count, @validation_phases)

    %{
      status: validation.status,
      phase_counts: phases,
      current_phases: current_phases(phases, @validation_phases),
      completed_shards: Map.get(phases, :done, 0),
      total_shards: shard_count,
      checked_records:
        max(Map.get(validation, :checked_records, 0), counter_sum(checkpoints, :checked_records)),
      checked_entries:
        max(Map.get(validation, :checked_entries, 0), counter_sum(checkpoints, :checked_entries)),
      mismatches: max(Map.get(validation, :mismatches, 0), counter_sum(checkpoints, :mismatches)),
      failure_reason: Map.get(validation, :reason),
      validated_at_ms: validation.validated_at_ms
    }
  end

  defp empty_validation do
    %{
      status: :pending,
      checkpoints: %{},
      checked_records: 0,
      checked_entries: 0,
      mismatches: 0,
      validated_at_ms: nil
    }
  end

  defp retirement_progress(nil, _shard_count), do: %{status: :not_applicable}

  defp retirement_progress(retirement, shard_count) do
    phases = phase_counts(retirement.checkpoints, shard_count, @retirement_phases, :fence)

    %{
      status: retirement.status,
      phase_counts: phases,
      current_phases: current_phases(phases, @retirement_phases),
      completed_shards: Map.get(phases, :done, 0),
      total_shards: shard_count,
      deleted_entries: counter_sum(retirement.checkpoints, :deleted_entries),
      deleted_bytes: counter_sum(retirement.checkpoints, :deleted_bytes),
      rewritten_reverse_rows: counter_sum(retirement.checkpoints, :rewritten_reverse_rows)
    }
  end

  defp phase_counts(checkpoints, shard_count, ordered_phases, missing_phase \\ :pending) do
    Enum.reduce(0..(shard_count - 1), %{}, fn shard_index, counts ->
      phase =
        case Map.get(checkpoints, shard_index) do
          %{phase: phase} -> if(phase in ordered_phases, do: phase, else: missing_phase)
          _missing -> missing_phase
        end

      Map.update(counts, phase, 1, &(&1 + 1))
    end)
  end

  defp current_phases(counts, ordered_phases) do
    Enum.filter(ordered_phases, &Map.has_key?(counts, &1))
  end

  defp counter_sum(checkpoints, counter) do
    Enum.reduce(checkpoints, 0, fn {_shard_index, checkpoint}, total ->
      total + Map.get(checkpoint, counter, 0)
    end)
  end
end
