# Benchmark-only lower bound for fusing catalog/backfill stable-value checks
# into their guarded LMDB write transaction.

Code.require_file("support/query_performance.exs", __DIR__)

defmodule Ferricstore.Bench.QueryPlannerLMDBCatalogCandidates do
  @moduledoc false

  alias Ferricstore.Bench.QueryPerformance
  alias Ferricstore.Flow.LMDB

  @page_sizes [32, 256]
  @samples 30
  @value :binary.copy(<<19>>, 128)

  def run do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-query-catalog-candidates-#{System.unique_integer([:positive])}"
      )

    current_path = Path.join(root, "current")
    fused_path = Path.join(root, "fused")
    File.mkdir_p!(current_path)
    File.mkdir_p!(fused_path)

    try do
      Enum.each(@page_sizes, fn page_size ->
        benchmark_missing(current_path, fused_path, page_size)
        benchmark_existing(current_path, fused_path, page_size)
      end)
    after
      _ = Ferricstore.Bitcask.NIF.lmdb_release(current_path)
      _ = Ferricstore.Bitcask.NIF.lmdb_release(fused_path)
      File.rm_rf!(root)
    end
  end

  defp benchmark_missing(current_path, fused_path, page_size) do
    {current_samples, fused_samples} =
      Enum.reduce(1..@samples, {[], []}, fn sample, {current_acc, fused_acc} ->
        current_keys = keys("missing-current-#{page_size}-#{sample}", page_size)
        fused_keys = keys("missing-fused-#{page_size}-#{sample}", page_size)

        {current_elapsed, :ok} =
          QueryPerformance.timed_ns(fn ->
            {:ok, current, 0} = LMDB.get_many_bounded(current_path, current_keys, 1)
            true = Enum.all?(current, &(&1 == :not_found))
            LMDB.write_batch(current_path, guarded_missing_puts(current_keys))
          end)

        {fused_elapsed, :ok} =
          QueryPerformance.timed_ns(fn ->
            LMDB.write_batch(fused_path, guarded_missing_puts(fused_keys))
          end)

        {[current_elapsed | current_acc], [fused_elapsed | fused_acc]}
      end)

    print_pair("catalog missing/page-#{page_size}", current_samples, fused_samples)
  end

  defp benchmark_existing(current_path, fused_path, page_size) do
    current_keys = keys("existing-current-#{page_size}", page_size)
    fused_keys = keys("existing-fused-#{page_size}", page_size)
    :ok = LMDB.write_batch(current_path, puts(current_keys))
    :ok = LMDB.write_batch(fused_path, puts(fused_keys))

    {current_samples, fused_samples} =
      Enum.reduce(1..@samples, {[], []}, fn _sample, {current_acc, fused_acc} ->
        {current_elapsed, :ok} =
          QueryPerformance.timed_ns(fn ->
            {:ok, current, read_bytes} =
              LMDB.get_many_bounded(
                current_path,
                current_keys,
                page_size * byte_size(@value)
              )

            true = read_bytes == page_size * byte_size(@value)
            true = Enum.all?(current, &(&1 == {:ok, @value}))
            LMDB.write_batch(current_path, guarded_equal_puts(current_keys))
          end)

        {fused_elapsed, :ok} =
          QueryPerformance.timed_ns(fn ->
            LMDB.write_batch(fused_path, guarded_equal_puts(fused_keys))
          end)

        {[current_elapsed | current_acc], [fused_elapsed | fused_acc]}
      end)

    print_pair("catalog existing/page-#{page_size}", current_samples, fused_samples)
  end

  defp guarded_missing_puts(keys) do
    Enum.flat_map(keys, fn key -> [{:compare_missing, key}, {:put, key, @value}] end)
  end

  defp guarded_equal_puts(keys) do
    Enum.flat_map(keys, fn key -> [{:compare, key, @value}, {:put, key, @value}] end)
  end

  defp puts(keys), do: Enum.map(keys, &{:put, &1, @value})

  defp keys(tag, count) do
    Enum.map(1..count, fn index ->
      "catalog-bench:#{tag}:#{String.pad_leading(Integer.to_string(index), 4, "0")}"
    end)
  end

  defp print_pair(name, current, fused) do
    QueryPerformance.print_summary(
      "current pre-read+guarded-write/#{name}",
      QueryPerformance.latency_summary(current)
    )

    QueryPerformance.print_summary(
      "candidate fused-guarded-write/#{name}",
      QueryPerformance.latency_summary(fused)
    )
  end
end

Ferricstore.Bench.QueryPlannerLMDBCatalogCandidates.run()
