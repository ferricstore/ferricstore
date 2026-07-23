# Benchmark-only multi-shard query prototypes. Production reads remain
# sequential and continue using the existing full-sort merge.

Code.require_file("support/query_performance.exs", __DIR__)

defmodule Ferricstore.Bench.QueryPlannerLMDBMultiShardCandidates do
  @moduledoc false

  alias Ferricstore.Bench.QueryPerformance
  alias Ferricstore.Flow.LMDB

  @prefix "query-multishard:"
  @value :binary.copy(<<11>>, 128)
  @page_sizes [100, 4_096]
  @shard_counts [4, 8, 16]
  @entries_per_shard 5_000
  @batch_size 1_000

  def run do
    datasets = Map.new(@shard_counts, &{&1, build_dataset(&1)})

    try do
      jobs =
        Enum.reduce(datasets, %{}, fn {shard_count, dataset}, jobs ->
          Enum.reduce(@page_sizes, jobs, fn page_size, page_jobs ->
            add_jobs(page_jobs, dataset, shard_count, page_size)
          end)
        end)

      preflight!(jobs)

      Benchee.run(
        jobs,
        QueryPerformance.benchee_options("query-planner-lmdb-multishard-candidates")
      )
    after
      Enum.each(datasets, fn {_shard_count, dataset} -> cleanup_dataset(dataset) end)
    end
  end

  defp add_jobs(jobs, dataset, shard_count, page_size) do
    suffix = "shards-#{shard_count}/page-#{page_size}"

    jobs
    |> Map.put("sequential+sort/#{suffix}", fn ->
      dataset.paths
      |> read_sequential(page_size)
      |> sort_take(page_size)
    end)
    |> Map.put("sequential+heap/#{suffix}", fn ->
      dataset.paths
      |> read_sequential(page_size)
      |> heap_take(page_size)
    end)
    |> Map.put("parallel+sort/#{suffix}", fn ->
      dataset.paths
      |> read_parallel(page_size)
      |> sort_take(page_size)
    end)
    |> Map.put("parallel+heap/#{suffix}", fn ->
      dataset.paths
      |> read_parallel(page_size)
      |> heap_take(page_size)
    end)
  end

  defp preflight!(jobs) do
    results = Map.new(jobs, fn {name, job} -> {name, job.()} end)

    Enum.each(@shard_counts, fn shard_count ->
      Enum.each(@page_sizes, fn page_size ->
        suffix = "shards-#{shard_count}/page-#{page_size}"
        expected = Map.fetch!(results, "sequential+sort/#{suffix}")
        ^page_size = length(expected)

        for prefix <- ["sequential+heap", "parallel+sort", "parallel+heap"] do
          true = Map.fetch!(results, "#{prefix}/#{suffix}") == expected
        end
      end)
    end)
  end

  defp read_sequential(paths, count) do
    Enum.map(paths, fn path ->
      {:ok, entries} = LMDB.prefix_entries(path, @prefix, count)
      entries
    end)
  end

  defp read_parallel(paths, count) do
    paths
    |> Task.async_stream(
      fn path ->
        {:ok, entries} = LMDB.prefix_entries(path, @prefix, count)
        entries
      end,
      max_concurrency: length(paths),
      ordered: true,
      timeout: 30_000
    )
    |> Enum.map(fn {:ok, entries} -> entries end)
  end

  defp sort_take(chunks, count) do
    chunks
    |> Enum.flat_map(& &1)
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.take(count)
  end

  defp heap_take(chunks, count) do
    heap =
      chunks
      |> Enum.with_index()
      |> Enum.reduce(:gb_sets.empty(), fn
        {[], _source}, heap ->
          heap

        {[{key, value} | rest], source}, heap ->
          :gb_sets.add({key, source, value, rest}, heap)
      end)

    take_heap(heap, count, [])
  end

  defp take_heap(_heap, 0, acc), do: Enum.reverse(acc)

  defp take_heap(heap, count, acc) do
    if :gb_sets.is_empty(heap) do
      Enum.reverse(acc)
    else
      {{key, source, value, rest}, heap} = :gb_sets.take_smallest(heap)

      heap =
        case rest do
          [] ->
            heap

          [{next_key, next_value} | tail] ->
            :gb_sets.add({next_key, source, next_value, tail}, heap)
        end

      take_heap(heap, count - 1, [{key, value} | acc])
    end
  end

  defp build_dataset(shard_count) do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-query-multishard-#{shard_count}-#{System.unique_integer([:positive])}"
      )

    paths =
      Enum.map(0..(shard_count - 1), fn shard_index ->
        path = Path.join(root, "shard-#{shard_index}")
        File.mkdir_p!(path)

        0..(@entries_per_shard - 1)
        |> Stream.chunk_every(@batch_size)
        |> Enum.each(fn local_indexes ->
          operations =
            Enum.map(local_indexes, fn local_index ->
              global_index = local_index * shard_count + shard_index
              {:put, key(global_index), @value}
            end)

          :ok = LMDB.write_batch(path, operations)
        end)

        path
      end)

    %{root: root, paths: paths}
  end

  defp key(index),
    do: @prefix <> String.pad_leading(Integer.to_string(index), 12, "0")

  defp cleanup_dataset(dataset) do
    Enum.each(dataset.paths, &Ferricstore.Bitcask.NIF.lmdb_release/1)
    File.rm_rf!(dataset.root)
  end
end

Ferricstore.Bench.QueryPlannerLMDBMultiShardCandidates.run()
