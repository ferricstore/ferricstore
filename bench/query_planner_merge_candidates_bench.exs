# Benchmark-only merge prototypes for auto-partition and lineage queries.

Code.require_file("support/query_performance.exs", __DIR__)

defmodule Ferricstore.Bench.QueryPlannerMergeCandidates do
  @moduledoc false

  alias Ferricstore.Bench.QueryPerformance

  @counts [100, 4_096]
  @source_counts [4, 16]
  @chunk_size 64

  def run do
    auto_hot_jobs = auto_hot_jobs()
    lineage_jobs = lineage_jobs()
    jobs = Map.merge(auto_hot_jobs, lineage_jobs)
    preflight!(jobs)

    Benchee.run(
      jobs,
      QueryPerformance.benchee_options("query-planner-merge-candidates")
    )
  end

  defp auto_hot_jobs do
    Enum.reduce(@source_counts, %{}, fn source_count, jobs ->
      Enum.reduce(@counts, jobs, fn count, page_jobs ->
        sources = auto_hot_sources(source_count, count)
        suffix = "sources-#{source_count}/page-#{count}"

        page_jobs
        |> Map.put("auto-hot repeated-sort/#{suffix}", fn ->
          auto_hot_repeated_sort(sources, count, @chunk_size)
        end)
        |> Map.put("auto-hot incremental-merge/#{suffix}", fn ->
          auto_hot_incremental_merge(sources, count, @chunk_size)
        end)
        |> Map.put("auto-hot heap/#{suffix}", fn ->
          heap_take(sources, count)
        end)
      end)
    end)
  end

  defp lineage_jobs do
    Enum.reduce(@counts, %{}, fn count, jobs ->
      {hot, cold} = lineage_sources(count)

      jobs
      |> Map.put("lineage sort+group/page-#{count}", fn ->
        lineage_sort_group(hot, cold, count)
      end)
      |> Map.put("lineage validate+merge/page-#{count}", fn ->
        lineage_validate_merge(hot, cold, count)
      end)
    end)
  end

  defp preflight!(jobs) do
    results = Map.new(jobs, fn {name, job} -> {name, job.()} end)

    for source_count <- @source_counts, count <- @counts do
      suffix = "sources-#{source_count}/page-#{count}"

      true =
        Map.fetch!(results, "auto-hot repeated-sort/#{suffix}") ==
          Map.fetch!(results, "auto-hot heap/#{suffix}")

      true =
        Map.fetch!(results, "auto-hot repeated-sort/#{suffix}") ==
          Map.fetch!(results, "auto-hot incremental-merge/#{suffix}")
    end

    for count <- @counts do
      true =
        Map.fetch!(results, "lineage sort+group/page-#{count}") ==
          Map.fetch!(results, "lineage validate+merge/page-#{count}")
    end
  end

  defp auto_hot_repeated_sort(sources, count, chunk_size) do
    rounds = ceil(count / (length(sources) * chunk_size))

    loaded =
      Enum.reduce(0..(rounds - 1), [], fn round, loaded ->
        next =
          Enum.flat_map(sources, fn entries ->
            Enum.slice(entries, round * chunk_size, chunk_size)
          end)

        Enum.sort_by(next ++ loaded, &entry_rank/1)
      end)

    Enum.take(loaded, count)
  end

  defp auto_hot_incremental_merge(sources, count, chunk_size) do
    rounds = ceil(count / (length(sources) * chunk_size))

    loaded =
      Enum.reduce(0..(rounds - 1), [], fn round, loaded ->
        next =
          sources
          |> Enum.flat_map(fn entries ->
            Enum.slice(entries, round * chunk_size, chunk_size)
          end)
          |> Enum.sort_by(&entry_rank/1)

        merge_entries(loaded, next, [])
      end)

    Enum.take(loaded, count)
  end

  defp merge_entries([], right, acc), do: Enum.reverse(acc, right)
  defp merge_entries(left, [], acc), do: Enum.reverse(acc, left)

  defp merge_entries([left | left_tail] = left_entries, [right | right_tail] = right_entries, acc) do
    if entry_rank(left) <= entry_rank(right) do
      merge_entries(left_tail, right_entries, [left | acc])
    else
      merge_entries(left_entries, right_tail, [right | acc])
    end
  end

  defp heap_take(sources, count) do
    heap =
      sources
      |> Enum.with_index()
      |> Enum.reduce(:gb_sets.empty(), fn
        {[], _source}, heap ->
          heap

        {[entry | rest], source}, heap ->
          :gb_sets.add({entry_rank(entry), source, entry, rest}, heap)
      end)

    take_heap(heap, count, [])
  end

  defp take_heap(_heap, 0, acc), do: Enum.reverse(acc)

  defp take_heap(heap, count, acc) do
    if :gb_sets.is_empty(heap) do
      Enum.reverse(acc)
    else
      {{_rank, source, entry, rest}, heap} = :gb_sets.take_smallest(heap)

      heap =
        case rest do
          [] -> heap
          [next | tail] -> :gb_sets.add({entry_rank(next), source, next, tail}, heap)
        end

      take_heap(heap, count - 1, [entry | acc])
    end
  end

  defp lineage_sort_group(hot, cold, count) do
    refs = hot ++ cold
    :ok = validate_consistent_scores_grouped(refs)

    refs
    |> Enum.sort_by(fn {id, score} -> {score, id} end)
    |> Enum.uniq_by(&elem(&1, 0))
    |> Enum.take(count)
  end

  defp lineage_validate_merge(hot, cold, count) do
    :ok = validate_consistent_scores_linear(hot, cold)
    heap_take_unique([hot, cold], count)
  end

  defp validate_consistent_scores_grouped(refs) do
    refs
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.reduce_while(:ok, fn {_id, scores}, :ok ->
      if length(Enum.uniq(scores)) == 1,
        do: {:cont, :ok},
        else: {:halt, {:error, :inconsistent_score}}
    end)
  end

  defp validate_consistent_scores_linear(hot, cold) do
    Enum.reduce_while(hot ++ cold, {:ok, %{}}, fn {id, score}, {:ok, scores} ->
      case Map.fetch(scores, id) do
        :error -> {:cont, {:ok, Map.put(scores, id, score)}}
        {:ok, ^score} -> {:cont, {:ok, scores}}
        {:ok, _different} -> {:halt, {:error, :inconsistent_score}}
      end
    end)
    |> case do
      {:ok, _scores} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp heap_take_unique(sources, count) do
    heap =
      sources
      |> Enum.with_index()
      |> Enum.reduce(:gb_sets.empty(), fn
        {[], _source}, heap ->
          heap

        {[entry | rest], source}, heap ->
          :gb_sets.add({entry_rank(entry), source, entry, rest}, heap)
      end)

    take_unique_heap(heap, count, MapSet.new(), [])
  end

  defp take_unique_heap(_heap, 0, _seen, acc), do: Enum.reverse(acc)

  defp take_unique_heap(heap, count, seen, acc) do
    if :gb_sets.is_empty(heap) do
      Enum.reverse(acc)
    else
      {{_rank, source, {id, _score} = entry, rest}, heap} = :gb_sets.take_smallest(heap)

      heap =
        case rest do
          [] -> heap
          [next | tail] -> :gb_sets.add({entry_rank(next), source, next, tail}, heap)
        end

      if MapSet.member?(seen, id) do
        take_unique_heap(heap, count, seen, acc)
      else
        take_unique_heap(heap, count - 1, MapSet.put(seen, id), [entry | acc])
      end
    end
  end

  defp auto_hot_sources(source_count, count) do
    entries_per_source = ceil(count / source_count) + @chunk_size

    Enum.map(0..(source_count - 1), fn source ->
      Enum.map(0..(entries_per_source - 1), fn local_index ->
        score = local_index * source_count + source
        {"id-#{String.pad_leading(Integer.to_string(score), 8, "0")}", score, "p-#{source}"}
      end)
    end)
  end

  defp lineage_sources(count) do
    hot =
      Enum.map(1..count, fn score ->
        {"hot-#{String.pad_leading(Integer.to_string(score), 8, "0")}", score * 2}
      end)

    cold =
      Enum.map(1..count, fn score ->
        if rem(score, 4) == 0 do
          Enum.at(hot, score - 1)
        else
          {"cold-#{String.pad_leading(Integer.to_string(score), 8, "0")}", score * 2 - 1}
        end
      end)

    {hot, cold}
  end

  defp entry_rank({id, score, _partition}), do: {score, id}
  defp entry_rank({id, score}), do: {score, id}
end

Ferricstore.Bench.QueryPlannerMergeCandidates.run()
