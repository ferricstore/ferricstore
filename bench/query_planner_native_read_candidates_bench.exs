# Reference-vs-production benchmarks for native bounded query reads. Every
# setup asserts exact result and corruption-contract equivalence before timing.

Code.require_file("support/query_performance.exs", __DIR__)

defmodule Ferricstore.Bench.QueryPlannerNativeReadCandidates do
  @moduledoc false

  alias Ferricstore.Bench.QueryPerformance
  alias Ferricstore.Flow.{Keys, LMDB}
  alias Ferricstore.Flow.Query.{CompositeIndex, IndexDefinition}

  @rows 100_000
  @batch 5_000
  @page_sizes [25, 100, 4_096]
  @max_bytes 64 * 1_024 * 1_024
  @multi_prefix "query-native-multishard:"
  @multi_value :binary.copy(<<11>>, 128)

  def run do
    case System.get_env("BENCH_CANDIDATE_SECTION", "all") do
      "composite" ->
        benchmark_composite()

      "multishard" ->
        benchmark_multishard()

      "multishard-manual" ->
        benchmark_multishard_manual()

      "all" ->
        benchmark_composite()
        benchmark_multishard()

      invalid ->
        raise ArgumentError, "unknown native read candidate section: #{inspect(invalid)}"
    end
  end

  defp benchmark_composite do
    definition = definition()
    root = temp_root("native-composite")
    path = Path.join(root, "index")
    File.mkdir_p!(path)

    try do
      1..@rows
      |> Stream.chunk_every(@batch)
      |> Enum.each(fn indexes ->
        ops =
          Enum.map(indexes, fn index ->
            {key, value} = compact_entry(definition, index)
            {:put, key, value}
          end)

        :ok = LMDB.write_batch(path, ops)
      end)

      prefix = IndexDefinition.storage_prefix(definition)

      jobs =
        Enum.map(@page_sizes, fn page ->
          current = fn -> current_compact_range(path, prefix, "", "", page) end
          native = fn -> native_compact_range(path, prefix, "", "", page) end
          expected = current.()
          ^expected = native.()

          cursor = expected.entries |> List.last() |> elem(0)
          expected_tail = current_compact_range(path, prefix, cursor, "", page)
          ^expected_tail = native_compact_range(path, prefix, cursor, "", page)

          {"current bounded scan + Elixir compact decode/page-#{page}", current,
           "native fused bounded scan + compact decode/page-#{page}", native}
        end)
        |> Enum.reduce(%{}, fn {current_name, current, native_name, native}, jobs ->
          jobs |> Map.put(current_name, current) |> Map.put(native_name, native)
        end)

      [{bad_key, _value}] = LMDB.prefix_entries(path, prefix, 1) |> elem(1)
      :ok = LMDB.write_batch(path, [{:put, bad_key, <<1, 0, 0, 0, 1>>}])
      {:error, :invalid_composite_entry} = current_compact_range(path, prefix, "", "", 1)

      {:error, :invalid_composite_entry} =
        LMDB.composite_range_entries_bounded(path, prefix, "", "", 1, @max_bytes)

      {restore_key, restore_value} = compact_entry(definition, 1)
      ^bad_key = restore_key
      id = "run-0000000001"
      invalid_state_key = "f:{invalid}:s:" <> id

      invalid_state_value =
        <<1, byte_size(id)::unsigned-big-32, 1::unsigned-big-64, 0::unsigned-big-64, id::binary,
          invalid_state_key::binary>>

      :ok = LMDB.write_batch(path, [{:put, restore_key, invalid_state_value}])
      {:error, :invalid_composite_entry} = current_compact_range(path, prefix, "", "", 1)

      {:error, :invalid_composite_entry} =
        LMDB.composite_range_entries_bounded(path, prefix, "", "", 1, @max_bytes)

      :ok = LMDB.write_batch(path, [{:put, restore_key, restore_value}])

      Benchee.run(
        jobs,
        QueryPerformance.benchee_options("query-planner-native-composite")
      )

      benchmark_composite_pairs(path, prefix)
    after
      release(path)
      File.rm_rf!(root)
    end
  end

  defp benchmark_composite_pairs(path, prefix) do
    Enum.each(@page_sizes, fn page ->
      current = fn -> current_compact_range(path, prefix, "", "", page) end
      native = fn -> native_compact_range(path, prefix, "", "", page) end
      expected = current.()
      ^expected = native.()
      samples = if page == 4_096, do: 100, else: 500

      {current_samples, native_samples} =
        Enum.reduce(1..samples, {[], []}, fn sample, {current_acc, native_acc} ->
          :erlang.garbage_collect()

          if rem(sample, 2) == 0 do
            {current_ns, ^expected} = QueryPerformance.timed_ns(current)
            {native_ns, ^expected} = QueryPerformance.timed_ns(native)
            {[current_ns | current_acc], [native_ns | native_acc]}
          else
            {native_ns, ^expected} = QueryPerformance.timed_ns(native)
            {current_ns, ^expected} = QueryPerformance.timed_ns(current)
            {[current_ns | current_acc], [native_ns | native_acc]}
          end
        end)

      QueryPerformance.print_summary(
        "paired current composite/page-#{page}",
        QueryPerformance.latency_summary(current_samples)
      )

      QueryPerformance.print_summary(
        "paired native composite/page-#{page}",
        QueryPerformance.latency_summary(native_samples)
      )
    end)
  end

  defp current_compact_range(path, prefix, after_key, before_key, page) do
    with {:ok, rows, exhausted, bytes} <-
           LMDB.range_entries_bounded(
             path,
             prefix,
             after_key,
             before_key,
             page,
             @max_bytes
           ),
         {:ok, entries} <- decode_rows(rows) do
      %{entries: entries, exhausted: exhausted, bytes: bytes}
    end
  end

  defp native_compact_range(path, prefix, after_key, before_key, page) do
    case LMDB.composite_range_entries_bounded(
           path,
           prefix,
           after_key,
           before_key,
           page,
           @max_bytes
         ) do
      {:ok, entries, exhausted, bytes} ->
        %{entries: entries, exhausted: exhausted, bytes: bytes}

      {:error, _reason} = error ->
        error
    end
  end

  defp decode_rows(rows) do
    rows
    |> Enum.reduce_while({:ok, []}, fn {key, value}, {:ok, acc} ->
      case decode_compact(key, value) do
        {:ok, entry} -> {:cont, {:ok, [entry | acc]}}
        :error -> {:halt, {:error, :invalid_composite_entry}}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      error -> error
    end
  end

  defp decode_compact(key, value) do
    with <<1, id_bytes::unsigned-big-32, version::unsigned-big-64, expire_at_ms::unsigned-big-64,
           payload::binary>> <- value,
         true <- id_bytes > 0 and id_bytes < byte_size(payload),
         <<id::binary-size(id_bytes), state_key::binary>> <- payload,
         true <- version <= 9_007_199_254_740_991,
         true <- byte_size(id) <= 65_535 and byte_size(state_key) <= 65_535,
         {:ok, ^id} <- Keys.run_id_from_state_key(state_key),
         true <- CompositeIndex.entry_key_matches_id?(key, id) do
      {:ok, {key, id, state_key, version, expire_at_ms, byte_size(key) + byte_size(value)}}
    else
      _invalid -> :error
    end
  end

  defp benchmark_multishard do
    Enum.each([4, 8, 16], fn shards ->
      dataset = build_multishard(shards)

      try do
        jobs =
          Enum.reduce([100, 4_096], %{}, fn page, jobs ->
            current = fn -> current_multishard(dataset.paths, page) end
            native = fn -> native_multishard(dataset.paths, page) end
            expected = current.()
            {^expected, scanned} = native.()
            true = scanned == min(shards * page, shards * 5_000)

            jobs
            |> Map.put("current BEAM read + bounded heap/shards-#{shards}/page-#{page}", current)
            |> Map.put("native bounded merge/shards-#{shards}/page-#{page}", fn ->
              native.() |> elem(0)
            end)
          end)

        Benchee.run(
          jobs,
          QueryPerformance.benchee_options("query-planner-native-multishard-#{shards}")
        )
      after
        Enum.each(dataset.paths, &release/1)
        File.rm_rf!(dataset.root)
      end
    end)
  end

  defp benchmark_multishard_manual do
    Enum.each([4, 8, 16], fn shards ->
      dataset = build_multishard(shards)

      try do
        Enum.each([100, 4_096], fn page ->
          current = fn -> current_multishard(dataset.paths, page) end
          native = fn -> native_multishard(dataset.paths, page) |> elem(0) end
          expected = current.()
          ^expected = native.()

          Enum.each(1..20, fn _ ->
            current.()
            native.()
          end)

          samples = if page == 100, do: 500, else: 100

          {current_samples, native_samples} =
            Enum.reduce(1..samples, {[], []}, fn sample, {current_acc, native_acc} ->
              :erlang.garbage_collect()

              if rem(sample, 2) == 0 do
                {current_ns, ^expected} = QueryPerformance.timed_ns(current)
                {native_ns, ^expected} = QueryPerformance.timed_ns(native)
                {[current_ns | current_acc], [native_ns | native_acc]}
              else
                {native_ns, ^expected} = QueryPerformance.timed_ns(native)
                {current_ns, ^expected} = QueryPerformance.timed_ns(current)
                {[current_ns | current_acc], [native_ns | native_acc]}
              end
            end)

          QueryPerformance.print_summary(
            "paired current/shards-#{shards}/page-#{page}",
            QueryPerformance.latency_summary(current_samples)
          )

          QueryPerformance.print_summary(
            "paired native/shards-#{shards}/page-#{page}",
            QueryPerformance.latency_summary(native_samples)
          )
        end)
      after
        Enum.each(dataset.paths, &release/1)
        File.rm_rf!(dataset.root)
      end
    end)
  end

  defp current_multishard(paths, count) do
    paths
    |> Enum.map(fn path ->
      {:ok, rows} = LMDB.prefix_entries(path, @multi_prefix, count)
      rows
    end)
    |> heap_take(count)
  end

  defp native_multishard(paths, count) do
    {:ok, rows, scanned} =
      LMDB.prefix_merge_entries(paths, @multi_prefix, count, @max_bytes)

    {rows, scanned}
  end

  defp heap_take(chunks, count) do
    heap =
      chunks
      |> Enum.with_index()
      |> Enum.reduce(:gb_sets.empty(), fn
        {[], _source}, heap -> heap
        {[{key, value} | rest], source}, heap -> :gb_sets.add({key, source, value, rest}, heap)
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

      take_heap(heap, count - 1, [{source, key, value} | acc])
    end
  end

  defp build_multishard(shards) do
    root = temp_root("native-multishard-#{shards}")

    paths =
      Enum.map(0..(shards - 1), fn shard ->
        path = Path.join(root, "shard-#{shard}")
        File.mkdir_p!(path)

        0..4_999
        |> Stream.chunk_every(1_000)
        |> Enum.each(fn locals ->
          ops =
            Enum.map(locals, fn local ->
              global = local * shards + shard

              {:put, @multi_prefix <> String.pad_leading(Integer.to_string(global), 12, "0"),
               @multi_value}
            end)

          :ok = LMDB.write_batch(path, ops)
        end)

        path
      end)

    %{root: root, paths: paths}
  end

  defp compact_entry(definition, index) do
    id = "run-#{String.pad_leading(Integer.to_string(index), 10, "0")}"
    state_key = Keys.state_key(id, "tenant-benchmark")

    record = %{
      id: id,
      type: "invoice",
      state: "queued",
      partition_key: "tenant-benchmark",
      updated_at_ms: index,
      version: rem(index, 1_000)
    }

    {:ok, [entry]} = CompositeIndex.entries(definition, record, state_key, 0)

    value =
      <<1, byte_size(id)::unsigned-big-32, record.version::unsigned-big-64, 0::unsigned-big-64,
        id::binary, state_key::binary>>

    {entry.key, value}
  end

  defp definition do
    IndexDefinition.new!(%{
      id: "bench-native-composite",
      version: 1,
      fields: [
        {:partition_key, :asc},
        {:type, :asc},
        {:state, :asc},
        {:updated_at_ms, :asc}
      ]
    })
  end

  defp temp_root(name),
    do: Path.join(System.tmp_dir!(), "ferricstore-#{name}-#{System.unique_integer([:positive])}")

  defp release(path), do: Ferricstore.Bitcask.NIF.lmdb_release(path)
end

Ferricstore.Bench.QueryPlannerNativeReadCandidates.run()
