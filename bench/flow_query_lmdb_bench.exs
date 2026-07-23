# LMDB query-read benchmark. Warm reads use Benchee; reopened and cold cache
# reads are sampled separately because cache eviction must occur between calls.

Code.require_file("support/query_performance.exs", __DIR__)

defmodule Ferricstore.Bench.FlowQueryLMDB do
  alias Ferricstore.Bench.QueryPerformance
  alias Ferricstore.Bitcask.NIF

  @max_read_bytes 64 * 1024 * 1024
  @minimum_map_size 256 * 1024 * 1024
  @page_sizes [1, 25, 100, 4_096]

  def run do
    requested_entries = QueryPerformance.int_env("BENCH_LMDB_ENTRIES", 100_000, min: 1)

    value_sizes =
      QueryPerformance.integer_list_env(
        "BENCH_LMDB_VALUE_BYTES",
        [128, 4_096, 1_048_576],
        min: 1
      )

    max_dataset_bytes =
      QueryPerformance.int_env("BENCH_LMDB_MAX_DATASET_BYTES", 512 * 1024 * 1024, min: 1)

    batch_size = QueryPerformance.int_env("BENCH_LMDB_BATCH_SIZE", 10_000, min: 1)
    page_size = system_page_size()

    datasets =
      Map.new(value_sizes, fn value_bytes ->
        row_bytes = byte_size("query:") + 12 + value_bytes
        max_entries = div(max_dataset_bytes, row_bytes)

        if max_entries == 0 do
          raise ArgumentError,
                "BENCH_LMDB_MAX_DATASET_BYTES must fit at least one #{row_bytes}-byte row"
        end

        effective_entries = min(requested_entries, max_entries)
        dataset = build_dataset(effective_entries, value_bytes, batch_size, page_size)
        {"value-#{value_bytes}", dataset}
      end)

    :erlang.garbage_collect()

    try do
      jobs = warm_jobs(@page_sizes)

      Benchee.run(
        jobs,
        [inputs: datasets] ++ QueryPerformance.benchee_options("flow-query-lmdb-warm")
      )

      run_budget_benchmark(datasets)

      manual_metrics =
        Enum.flat_map(datasets, fn {name, dataset} ->
          setup =
            {"setup/value-#{dataset.value_bytes}",
             %{
               "median_ns" => dataset.setup_ns,
               "operations" => dataset.entries,
               "operations_per_second" =>
                 dataset.entries * 1_000_000_000 / max(dataset.setup_ns, 1),
               "logical_bytes" => dataset.logical_bytes,
               "physical_bytes" => dataset.physical_bytes
             }}

          reopened = sample_reopened(name, dataset)
          cold = sample_cold_cache(name, dataset)
          [setup | reopened ++ cold]
        end)
        |> Map.new()

      QueryPerformance.write_manual_metrics("flow-query-lmdb-cache", manual_metrics)
    after
      Enum.each(datasets, fn {_name, dataset} -> cleanup_dataset(dataset) end)
    end
  end

  defp warm_jobs(page_sizes) do
    Enum.reduce(page_sizes, %{}, fn page_size, jobs ->
      jobs
      |> Map.put("warm prefix/page-#{page_size}", fn dataset ->
        NIF.lmdb_prefix_entries_after_bounded(
          dataset.path,
          dataset.prefix,
          "",
          page_size,
          @max_read_bytes,
          dataset.map_size
        )
      end)
      |> Map.put("warm range/page-#{page_size}", fn dataset ->
        NIF.lmdb_range_entries_bounded(
          dataset.path,
          dataset.prefix,
          "",
          "",
          page_size,
          @max_read_bytes,
          dataset.map_size
        )
      end)
      |> Map.put("warm hydrated get-many/page-#{page_size}", fn dataset ->
        NIF.lmdb_get_many_bounded(
          dataset.path,
          Map.fetch!(dataset.hydration_keys, page_size),
          @max_read_bytes,
          dataset.map_size
        )
      end)
    end)
  end

  defp run_budget_benchmark(datasets) do
    oversized_datasets =
      Map.filter(datasets, fn {_name, dataset} ->
        is_list(dataset.oversized_hydration_keys)
      end)

    if map_size(oversized_datasets) > 0 do
      Benchee.run(
        %{
          "bounded hydration rejection" => fn dataset ->
            {:error, :batch_value_budget_exceeded} =
              NIF.lmdb_get_many_bounded(
                dataset.path,
                dataset.oversized_hydration_keys,
                @max_read_bytes,
                dataset.map_size
              )
          end
        },
        [inputs: oversized_datasets] ++
          QueryPerformance.benchee_options("flow-query-lmdb-budget")
      )
    end
  end

  defp build_dataset(entries, value_bytes, batch_size, page_size) do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-query-lmdb-bench-#{value_bytes}-#{System.unique_integer([:positive])}"
      )

    path = Path.join(root, "env")
    File.mkdir_p!(path)
    prefix = "query:"
    value = deterministic_value(value_bytes)
    row_bytes = byte_size(prefix) + 12 + value_bytes

    if row_bytes > @max_read_bytes do
      raise ArgumentError,
            "BENCH_LMDB_VALUE_BYTES entries must fit within the #{@max_read_bytes}-byte read budget"
    end

    logical_bytes = entries * row_bytes

    map_size =
      @minimum_map_size
      |> max(logical_bytes * 3 + 64 * 1024 * 1024)
      |> round_up(page_size)

    {setup_ns, :ok} =
      QueryPerformance.timed_ns(fn ->
        0..(entries - 1)
        |> Stream.chunk_every(batch_size)
        |> Enum.each(fn indexes ->
          operations =
            Enum.map(indexes, fn index ->
              {:put, key(prefix, index), value}
            end)

          :ok = NIF.lmdb_write_batch(path, operations, map_size)
        end)

        :ok
      end)

    physical_bytes = QueryPerformance.directory_bytes(path)
    sample_keys = for index <- 0..(min(entries, 4_096) - 1), do: key(prefix, index)
    max_hydration_items = min(div(@max_read_bytes, value_bytes), 4_096)

    hydration_keys =
      Map.new(@page_sizes, fn page_size ->
        {page_size, Enum.take(sample_keys, min(page_size, max_hydration_items))}
      end)

    range_rows =
      Map.new(@page_sizes, fn page_size ->
        {page_size, min(page_size, min(entries, div(@max_read_bytes, row_bytes)))}
      end)

    oversized_hydration_keys =
      if max_hydration_items < 4_096 and entries > max_hydration_items do
        Enum.take(sample_keys, max_hydration_items + 1)
      end

    {:ok, [{first_key, first_value}]} =
      NIF.lmdb_prefix_entries_after_bounded(path, prefix, "", 1, @max_read_bytes, map_size)

    true = String.starts_with?(first_key, prefix)
    ^value_bytes = byte_size(first_value)

    IO.puts(
      "lmdb_setup entries=#{entries} value_bytes=#{value_bytes} logical bytes=#{logical_bytes} " <>
        "physical bytes=#{physical_bytes} elapsed_ns=#{setup_ns} " <>
        "records_per_second=#{Float.round(entries * 1_000_000_000 / max(setup_ns, 1), 2)}"
    )

    dataset = %{
      root: root,
      path: path,
      prefix: prefix,
      map_size: map_size,
      setup_ns: setup_ns,
      entries: entries,
      value_bytes: value_bytes,
      logical_bytes: logical_bytes,
      physical_bytes: physical_bytes,
      hydration_keys: hydration_keys,
      range_rows: range_rows,
      oversized_hydration_keys: oversized_hydration_keys
    }

    preflight_dataset!(dataset)
    dataset
  end

  defp preflight_dataset!(dataset) do
    Enum.each(@page_sizes, fn page_size ->
      expected_range_rows = Map.fetch!(dataset.range_rows, page_size)
      hydration_keys = Map.fetch!(dataset.hydration_keys, page_size)

      {:ok, prefix_rows} =
        NIF.lmdb_prefix_entries_after_bounded(
          dataset.path,
          dataset.prefix,
          "",
          page_size,
          @max_read_bytes,
          dataset.map_size
        )

      ^expected_range_rows = length(prefix_rows)

      {:ok, range_rows, _exhausted, range_bytes} =
        NIF.lmdb_range_entries_bounded(
          dataset.path,
          dataset.prefix,
          "",
          "",
          page_size,
          @max_read_bytes,
          dataset.map_size
        )

      ^expected_range_rows = length(range_rows)
      true = range_bytes <= @max_read_bytes

      {:ok, hydrated_values, hydrated_bytes} =
        NIF.lmdb_get_many_bounded(
          dataset.path,
          hydration_keys,
          @max_read_bytes,
          dataset.map_size
        )

      expected_hydrated_rows = length(hydration_keys)
      ^expected_hydrated_rows = length(hydrated_values)
      true = hydrated_bytes == expected_hydrated_rows * dataset.value_bytes

      true =
        Enum.all?(hydrated_values, fn
          {:ok, value} -> byte_size(value) == dataset.value_bytes
          _ -> false
        end)
    end)

    if is_list(dataset.oversized_hydration_keys) do
      {:error, :batch_value_budget_exceeded} =
        NIF.lmdb_get_many_bounded(
          dataset.path,
          dataset.oversized_hydration_keys,
          @max_read_bytes,
          dataset.map_size
        )
    end
  end

  defp sample_reopened(name, dataset) do
    samples = QueryPerformance.int_env("BENCH_CACHE_SAMPLES", 10, min: 1)

    latencies =
      for _ <- 1..samples do
        release!(dataset.path)

        {elapsed_ns, {:ok, rows}} =
          QueryPerformance.timed_ns(fn ->
            NIF.lmdb_prefix_entries_after_bounded(
              dataset.path,
              dataset.prefix,
              "",
              25,
              @max_read_bytes,
              dataset.map_size
            )
          end)

        true = rows != []
        elapsed_ns
      end

    summary = QueryPerformance.latency_summary(latencies)
    QueryPerformance.print_summary("LMDB reopened #{name}", summary)
    [{"reopened/#{name}", cache_metric(summary, dataset)}]
  end

  defp sample_cold_cache(name, dataset) do
    required? = QueryPerformance.bool_env("BENCH_REQUIRE_COLD_CACHE", false)
    linux? = :os.type() == {:unix, :linux}
    vmtouch? = QueryPerformance.command_available?("vmtouch")

    cond do
      linux? and vmtouch? ->
        samples = QueryPerformance.int_env("BENCH_COLD_CACHE_SAMPLES", 5, min: 1)
        data_file = Path.join(dataset.path, "data.mdb")

        latencies =
          for _ <- 1..samples do
            release!(dataset.path)
            {output, 0} = System.cmd("vmtouch", ["-e", data_file], stderr_to_stdout: true)
            IO.write(output)

            {elapsed_ns, {:ok, rows}} =
              QueryPerformance.timed_ns(fn ->
                NIF.lmdb_prefix_entries_after_bounded(
                  dataset.path,
                  dataset.prefix,
                  "",
                  25,
                  @max_read_bytes,
                  dataset.map_size
                )
              end)

            true = rows != []
            elapsed_ns
          end

        summary = QueryPerformance.latency_summary(latencies)
        QueryPerformance.print_summary("LMDB cold cache #{name}", summary)
        [{"cold cache/#{name}", cache_metric(summary, dataset)}]

      required? ->
        raise "BENCH_REQUIRE_COLD_CACHE=1 requires Linux and the vmtouch executable"

      true ->
        IO.puts("Skipping cold cache #{name}: install vmtouch on Linux or require it explicitly")
        []
    end
  end

  defp cache_metric(summary, dataset) do
    Map.merge(summary, %{
      "entries" => dataset.entries,
      "value_bytes" => dataset.value_bytes,
      "logical_bytes" => dataset.logical_bytes,
      "physical_bytes" => dataset.physical_bytes
    })
  end

  defp cleanup_dataset(dataset) do
    release!(dataset.path)
    File.rm_rf!(dataset.root)
  end

  defp release!(path, attempts \\ 20)

  defp release!(_path, 0), do: raise("LMDB benchmark environment stayed busy during release")

  defp release!(path, attempts) do
    case NIF.lmdb_release(path) do
      {:ok, _released} ->
        :ok

      {:busy, _leases} ->
        Process.sleep(5)
        release!(path, attempts - 1)

      {:error, reason} ->
        raise "LMDB release failed: #{inspect(reason)}"
    end
  end

  defp deterministic_value(bytes) do
    pattern = "0123456789abcdef"
    copies = div(bytes + byte_size(pattern) - 1, byte_size(pattern))
    pattern |> String.duplicate(copies) |> binary_part(0, bytes)
  end

  defp round_up(value, alignment), do: div(value + alignment - 1, alignment) * alignment

  defp system_page_size do
    with executable when is_binary(executable) <- System.find_executable("getconf"),
         {output, 0} <- System.cmd(executable, ["PAGESIZE"], stderr_to_stdout: true),
         {page_size, ""} when page_size > 0 <- output |> String.trim() |> Integer.parse() do
      page_size
    else
      _ -> raise "LMDB benchmark requires getconf PAGESIZE to determine map alignment"
    end
  end

  defp key(prefix, index),
    do: prefix <> String.pad_leading(Integer.to_string(index), 12, "0")
end

Ferricstore.Bench.FlowQueryLMDB.run()
