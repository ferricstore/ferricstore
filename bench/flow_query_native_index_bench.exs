# Native ordered-index scaling benchmark for query execution primitives.
# Full matrix:
#   BENCH_CARDINALITIES=1000,100000,1000000 MIX_ENV=bench \
#     mix run --no-start bench/flow_query_native_index_bench.exs

Code.require_file("support/query_performance.exs", __DIR__)

defmodule Ferricstore.Bench.FlowQueryNativeIndex do
  alias Ferricstore.Bench.QueryPerformance
  alias Ferricstore.Bitcask.NIF

  @uniform_partition_count 256
  @max_page_size 4_096
  @default_cardinalities [1_000, 100_000, 1_000_000]

  def run do
    cardinalities =
      QueryPerformance.integer_list_env(
        "BENCH_CARDINALITIES",
        @default_cardinalities,
        min: 1
      )

    page_sizes = [1, 25, 100, @max_page_size]
    batch_size = QueryPerformance.int_env("BENCH_INDEX_BATCH_SIZE", 50_000, min: 1)

    datasets =
      Map.new(cardinalities, fn cardinality ->
        {Integer.to_string(cardinality), build_dataset(cardinality, batch_size)}
      end)

    jobs =
      Enum.reduce(page_sizes, %{}, fn page_size, jobs ->
        jobs
        |> Map.put("forward/page-#{page_size}", fn dataset ->
          NIF.flow_index_range_slice(
            dataset.resource,
            dataset.hot_key,
            0,
            0.0,
            0,
            0.0,
            false,
            0,
            page_size
          )
        end)
        |> Map.put("reverse/page-#{page_size}", fn dataset ->
          NIF.flow_index_range_slice(
            dataset.resource,
            dataset.hot_key,
            0,
            0.0,
            0,
            0.0,
            true,
            0,
            page_size
          )
        end)
        |> Map.put("cursor/page-#{page_size}", fn dataset ->
          NIF.flow_index_range_after_slice(
            dataset.resource,
            dataset.hot_key,
            0,
            0.0,
            0,
            0.0,
            dataset.midpoint * 1.0,
            member(dataset.midpoint),
            0,
            page_size
          )
        end)
        |> Map.put("deep offset/page-#{page_size}", fn dataset ->
          NIF.flow_index_range_slice(
            dataset.resource,
            dataset.hot_key,
            0,
            0.0,
            0,
            0.0,
            false,
            dataset.midpoint,
            page_size
          )
        end)
        |> Map.put("duplicate scores/page-#{page_size}", fn dataset ->
          NIF.flow_index_range_slice(
            dataset.resource,
            dataset.duplicate_key,
            0,
            0.0,
            0,
            0.0,
            false,
            0,
            min(page_size, dataset.duplicate_count)
          )
        end)
        |> Map.put("hot partition/page-#{page_size}", fn dataset ->
          NIF.flow_index_range_slice(
            dataset.resource,
            dataset.hot_key,
            0,
            0.0,
            0,
            0.0,
            false,
            0,
            page_size
          )
        end)
        |> Map.put("uniform partitions/page-#{page_size}", fn dataset ->
          NIF.flow_index_range_slice(
            dataset.resource,
            hd(dataset.uniform_keys),
            0,
            0.0,
            0,
            0.0,
            false,
            0,
            page_size
          )
        end)
      end)

    jobs =
      Enum.reduce([1, 8, 64, 256], jobs, fn fanout, jobs ->
        Map.put(jobs, "claim fanout/#{fanout}", fn dataset ->
          keys = Enum.take(dataset.uniform_keys, fanout)

          NIF.flow_index_claim_due_candidates(
            dataset.resource,
            keys,
            dataset.cardinality * 1.0,
            100,
            4_096
          )
        end)
      end)

    Benchee.run(
      jobs,
      [inputs: datasets] ++ QueryPerformance.benchee_options("flow-query-native-index")
    )

    largest = datasets |> Enum.max_by(fn {_name, dataset} -> dataset.cardinality end) |> elem(1)
    contention = run_contention(largest)
    metrics = Map.merge(setup_metrics(datasets), contention)
    QueryPerformance.write_manual_metrics("flow-query-native-index-contention", metrics)
  end

  defp build_dataset(cardinality, batch_size) do
    resource = NIF.flow_index_new()
    hot_key = "query-bench:hot"
    duplicate_key = "query-bench:duplicate"

    uniform_keys =
      for index <- 0..(@uniform_partition_count - 1),
          do: "query-bench:uniform:#{String.pad_leading(Integer.to_string(index), 3, "0")}"

    uniform_key_tuple = List.to_tuple(uniform_keys)

    {setup_ns, :ok} =
      QueryPerformance.timed_ns(fn ->
        0..(cardinality - 1)
        |> Stream.chunk_every(batch_size)
        |> Enum.each(fn indexes ->
          hot_entries =
            Enum.map(indexes, fn index -> {hot_key, member(index), index * 1.0} end)

          uniform_entries =
            Enum.map(indexes, fn index ->
              key = elem(uniform_key_tuple, rem(index, @uniform_partition_count))
              {key, member(index), index * 1.0}
            end)

          :ok = NIF.flow_index_put_entries(resource, hot_entries)
          :ok = NIF.flow_index_put_entries(resource, uniform_entries)
        end)

        duplicate_count = min(cardinality, 10_000)

        0..(duplicate_count - 1)
        |> Stream.chunk_every(batch_size)
        |> Enum.each(fn indexes ->
          entries = Enum.map(indexes, fn index -> {duplicate_key, member(index), 42.0} end)
          :ok = NIF.flow_index_put_entries(resource, entries)
        end)

        :ok
      end)

    duplicate_count = min(cardinality, 10_000)
    ^cardinality = NIF.flow_index_count_all(resource, hot_key)
    ^duplicate_count = NIF.flow_index_count_all(resource, duplicate_key)

    [{"flow-0000000000", first_score}] =
      NIF.flow_index_range_slice(resource, hot_key, 0, 0.0, 0, 0.0, false, 0, 1)

    true = first_score == 0.0

    IO.puts(
      "native_index_setup cardinality=#{cardinality} logical_entries=#{cardinality * 2 + duplicate_count} " <>
        "elapsed_ns=#{setup_ns} entries_per_second=#{Float.round((cardinality * 2 + duplicate_count) * 1_000_000_000 / setup_ns, 2)}"
    )

    dataset = %{
      resource: resource,
      cardinality: cardinality,
      logical_entries: cardinality * 2 + duplicate_count,
      setup_ns: setup_ns,
      midpoint: div(cardinality, 2),
      hot_key: hot_key,
      duplicate_key: duplicate_key,
      duplicate_count: duplicate_count,
      uniform_keys: uniform_keys
    }

    preflight_dataset!(dataset)
    dataset
  end

  defp preflight_dataset!(dataset) do
    expected_uniform_count =
      div(dataset.cardinality + @uniform_partition_count - 1, @uniform_partition_count)

    ^expected_uniform_count = NIF.flow_index_count_all(dataset.resource, hd(dataset.uniform_keys))

    Enum.each([1, 25, 100, @max_page_size], fn page_size ->
      forward =
        NIF.flow_index_range_slice(
          dataset.resource,
          dataset.hot_key,
          0,
          0.0,
          0,
          0.0,
          false,
          0,
          page_size
        )

      assert_page!(forward, min(page_size, dataset.cardinality), 0, 1)

      reverse =
        NIF.flow_index_range_slice(
          dataset.resource,
          dataset.hot_key,
          0,
          0.0,
          0,
          0.0,
          true,
          0,
          page_size
        )

      assert_page!(reverse, min(page_size, dataset.cardinality), dataset.cardinality - 1, -1)

      cursor =
        NIF.flow_index_range_after_slice(
          dataset.resource,
          dataset.hot_key,
          0,
          0.0,
          0,
          0.0,
          dataset.midpoint * 1.0,
          member(dataset.midpoint),
          0,
          page_size
        )

      cursor_count = min(page_size, dataset.cardinality - dataset.midpoint - 1)
      assert_page!(cursor, cursor_count, dataset.midpoint + 1, 1)

      deep_offset =
        NIF.flow_index_range_slice(
          dataset.resource,
          dataset.hot_key,
          0,
          0.0,
          0,
          0.0,
          false,
          dataset.midpoint,
          page_size
        )

      deep_count = min(page_size, dataset.cardinality - dataset.midpoint)
      assert_page!(deep_offset, deep_count, dataset.midpoint, 1)

      duplicates =
        NIF.flow_index_range_slice(
          dataset.resource,
          dataset.duplicate_key,
          0,
          0.0,
          0,
          0.0,
          false,
          0,
          min(page_size, dataset.duplicate_count)
        )

      assert_page!(duplicates, min(page_size, dataset.duplicate_count), 0, 1, 42.0)

      uniform =
        NIF.flow_index_range_slice(
          dataset.resource,
          hd(dataset.uniform_keys),
          0,
          0.0,
          0,
          0.0,
          false,
          0,
          page_size
        )

      assert_page!(uniform, min(page_size, expected_uniform_count), 0, 256)
    end)

    Enum.each([1, 8, 64, 256], fn fanout ->
      keys = Enum.take(dataset.uniform_keys, fanout)

      candidates =
        NIF.flow_index_claim_due_candidates(
          dataset.resource,
          keys,
          dataset.cardinality * 1.0,
          100,
          4_096
        )

      true = is_list(candidates)
      true = Enum.sum(Enum.map(candidates, fn {_key, rows} -> length(rows) end)) <= 100

      true =
        Enum.all?(candidates, fn {key, rows} ->
          key in keys and is_list(rows) and rows != []
        end)
    end)
  end

  defp assert_page!(rows, expected_count, first_index, stride, score \\ nil) do
    ^expected_count = length(rows)

    if expected_count > 0 do
      {first_member, first_score} = hd(rows)
      {last_member, last_score} = List.last(rows)
      last_index = first_index + (expected_count - 1) * stride
      ^first_member = member(first_index)
      ^last_member = member(last_index)
      true = is_nil(score) or first_score == score
      true = is_nil(score) or last_score == score
    end
  end

  defp setup_metrics(datasets) do
    Map.new(datasets, fn {_name, dataset} ->
      {"setup/cardinality-#{dataset.cardinality}",
       %{
         "median_ns" => dataset.setup_ns,
         "operations" => dataset.logical_entries,
         "operations_per_second" =>
           dataset.logical_entries * 1_000_000_000 / max(dataset.setup_ns, 1),
         "cardinality" => dataset.cardinality
       }}
    end)
  end

  defp run_contention(dataset) do
    duration_ms = QueryPerformance.int_env("BENCH_CONTENTION_DURATION_MS", 3_000, min: 100)

    readers =
      QueryPerformance.int_env(
        "BENCH_CONTENTION_READERS",
        System.schedulers_online(),
        min: 1
      )

    sample_every = QueryPerformance.int_env("BENCH_SAMPLE_EVERY", 128, min: 1)

    deadline_ns =
      System.monotonic_time(:nanosecond) +
        System.convert_time_unit(duration_ms, :millisecond, :nanosecond)

    writer_batches = contention_writer_batches(dataset)
    writer_batch_items = writer_batches |> elem(0) |> length()

    writer =
      Task.async(fn ->
        writer_loop(dataset.resource, writer_batches, deadline_ns, 0, 0, [])
      end)

    reader_tasks =
      for _ <- 1..readers do
        Task.async(fn ->
          reader_loop(dataset, deadline_ns, sample_every, 0, [])
        end)
      end

    {writer_operations, writer_busy_attempts, writer_samples} =
      Task.await(writer, duration_ms + 30_000)

    reader_results = Enum.map(reader_tasks, &Task.await(&1, duration_ms + 30_000))
    reader_operations = Enum.reduce(reader_results, 0, fn {count, _}, total -> total + count end)
    reader_samples = Enum.flat_map(reader_results, &elem(&1, 1))
    reader_summary = QueryPerformance.latency_summary(reader_samples)
    writer_summary = QueryPerformance.latency_summary(writer_samples)

    QueryPerformance.print_summary("native index contention readers", reader_summary)
    QueryPerformance.print_summary("native index contention writer", writer_summary)

    %{
      "contention/readers" =>
        Map.merge(reader_summary, %{
          "operations" => reader_operations,
          "reader_count" => readers,
          "cardinality" => dataset.cardinality
        }),
      "contention/writer" =>
        Map.merge(writer_summary, %{
          "operations" => writer_operations,
          "busy_attempts" => writer_busy_attempts,
          "busy_attempts_per_operation" => writer_busy_attempts / max(writer_operations, 1),
          "batch_items" => writer_batch_items,
          "cardinality" => dataset.cardinality
        })
    }
  end

  defp contention_writer_batches(dataset) do
    batch_items = min(128, dataset.cardinality)

    entries =
      for index <- 0..(batch_items - 1) do
        {dataset.hot_key, member(index), dataset.cardinality + index * 1.0}
      end

    restored =
      for index <- 0..(batch_items - 1) do
        {dataset.hot_key, member(index), index * 1.0}
      end

    {entries, restored}
  end

  defp writer_loop(
         resource,
         {first, second} = batches,
         deadline_ns,
         count,
         busy_attempts,
         samples
       ) do
    if System.monotonic_time(:nanosecond) >= deadline_ns do
      {count, busy_attempts, Enum.reverse(samples)}
    else
      batch = if rem(count, 2) == 0, do: first, else: second

      {elapsed_ns, busy_count} =
        QueryPerformance.timed_ns(fn -> put_with_retry(resource, batch, 0) end)

      writer_loop(
        resource,
        batches,
        deadline_ns,
        count + 1,
        busy_attempts + busy_count,
        [elapsed_ns | samples]
      )
    end
  end

  defp put_with_retry(resource, batch, busy_attempts, delay_ms \\ 1) do
    case NIF.flow_index_put_entries(resource, batch) do
      :ok ->
        busy_attempts

      :busy ->
        Process.sleep(delay_ms)
        put_with_retry(resource, batch, busy_attempts + 1, min(delay_ms * 2, 8))

      {:error, reason} ->
        raise "native index contention write failed: #{inspect(reason)}"
    end
  end

  defp reader_loop(dataset, deadline_ns, sample_every, count, samples) do
    if System.monotonic_time(:nanosecond) >= deadline_ns do
      {count, Enum.reverse(samples)}
    else
      {elapsed_ns, rows} =
        QueryPerformance.timed_ns(fn ->
          NIF.flow_index_range_slice(
            dataset.resource,
            dataset.hot_key,
            0,
            0.0,
            0,
            0.0,
            false,
            0,
            25
          )
        end)

      expected_rows = min(25, dataset.cardinality)
      ^expected_rows = length(rows)
      next_count = count + 1

      next_samples =
        if rem(next_count, sample_every) == 0, do: [elapsed_ns | samples], else: samples

      reader_loop(dataset, deadline_ns, sample_every, next_count, next_samples)
    end
  end

  defp member(index),
    do: "flow-#{String.pad_leading(Integer.to_string(index), 10, "0")}"
end

Ferricstore.Bench.FlowQueryNativeIndex.run()
