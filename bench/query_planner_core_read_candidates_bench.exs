# Benchmark-only query read prototypes. Production query paths are unchanged.

Code.require_file("support/query_performance.exs", __DIR__)

defmodule Ferricstore.Bench.QueryPlannerCoreReadCandidates do
  @moduledoc false

  alias Ferricstore.Bench.QueryPerformance
  alias Ferricstore.Flow.{LMDB, RecordQuery}
  alias Ferricstore.Flow.RecordRead

  @count_batch_size 5_000
  @count_value :binary.copy(<<29>>, 48)
  @filter_rows 10_000
  @filter_limit 100
  @filter_chunk 400
  @key_rows 100_000
  @key_value :binary.copy(<<41>>, 64)
  @page_sizes [25, 100, 4_096]

  def run do
    case System.get_env("BENCH_CANDIDATE_SECTION", "all") do
      "all" ->
        benchmark_counts()
        benchmark_filter_continuations()
        benchmark_multistate_merge()
        benchmark_short_index_keys()

      "counts" ->
        benchmark_counts()

      "filtering" ->
        benchmark_filter_continuations()

      "filtering-lmdb" ->
        benchmark_lmdb_filter_continuations()

      "multistate" ->
        benchmark_multistate_merge()

      "short-keys" ->
        benchmark_short_index_keys()

      invalid ->
        raise ArgumentError,
              "BENCH_CANDIDATE_SECTION must be all, counts, filtering, filtering-lmdb, " <>
                "multistate, or " <>
                "short-keys; got #{inspect(invalid)}"
    end
  end

  defp benchmark_counts do
    cardinalities =
      QueryPerformance.integer_list_env(
        "BENCH_COUNT_CARDINALITIES",
        [1_000, 100_000],
        min: 1
      )

    root = temp_root("counts")

    datasets =
      Map.new(cardinalities, fn cardinality ->
        {cardinality, build_count_dataset(root, cardinality)}
      end)

    try do
      jobs =
        Enum.reduce(datasets, %{}, fn {cardinality, dataset}, jobs ->
          Map.merge(jobs, %{
            "current prefix-count/cardinality-#{cardinality}" => fn ->
              LMDB.prefix_count(dataset.path, dataset.prefix)
            end,
            "candidate durable-count/cardinality-#{cardinality}" => fn ->
              read_count(dataset.path, dataset.count_key)
            end
          })
        end)

      Benchee.run(jobs, QueryPerformance.benchee_options("query-planner-count-candidates"))
      benchmark_count_write_overhead(root)
    after
      Enum.each(datasets, fn {_cardinality, dataset} -> release(dataset.path) end)
      File.rm_rf!(root)
    end
  end

  defp build_count_dataset(root, cardinality) do
    path = Path.join(root, "cardinality-#{cardinality}")
    File.mkdir_p!(path)
    prefix = "count-candidate:#{cardinality}:"
    count_key = "count-candidate-meta:#{cardinality}"

    1..cardinality
    |> Stream.chunk_every(@count_batch_size)
    |> Enum.each(fn indexes ->
      operations =
        Enum.map(indexes, fn index ->
          {:put, prefix <> String.pad_leading(Integer.to_string(index), 12, "0"), @count_value}
        end)

      :ok = LMDB.write_batch(path, operations)
    end)

    :ok = LMDB.write_batch(path, [{:put, count_key, LMDB.encode_count(cardinality)}])
    {:ok, ^cardinality} = LMDB.prefix_count(path, prefix)
    {:ok, ^cardinality} = read_count(path, count_key)

    %{path: path, prefix: prefix, count_key: count_key}
  end

  defp read_count(path, count_key) do
    case LMDB.get(path, count_key) do
      {:ok, value} -> LMDB.decode_count(value)
      :not_found -> {:ok, 0}
      {:error, _reason} = error -> error
    end
  end

  defp benchmark_count_write_overhead(root) do
    current_path = Path.join(root, "write-current")
    candidate_path = Path.join(root, "write-candidate")
    File.mkdir_p!(current_path)
    File.mkdir_p!(candidate_path)

    try do
      Enum.each([32, 256, 1_024], fn batch_size ->
        {current_samples, candidate_samples} =
          Enum.reduce(1..20, {[], []}, fn sample, {current_acc, candidate_acc} ->
            current_prefix = "write-current:#{batch_size}:#{sample}:"
            candidate_prefix = "write-candidate:#{batch_size}:#{sample}:"
            count_key = "write-candidate-count:#{batch_size}:#{sample}"

            current_puts = count_puts(current_prefix, batch_size)
            candidate_puts = count_puts(candidate_prefix, batch_size)

            {current_elapsed, :ok} =
              QueryPerformance.timed_ns(fn -> LMDB.write_batch(current_path, current_puts) end)

            candidate_ops =
              [{:compare_missing, count_key} | candidate_puts] ++
                [{:put, count_key, LMDB.encode_count(batch_size)}]

            {candidate_elapsed, :ok} =
              QueryPerformance.timed_ns(fn -> LMDB.write_batch(candidate_path, candidate_ops) end)

            {:ok, ^batch_size} = read_count(candidate_path, count_key)

            {[current_elapsed | current_acc], [candidate_elapsed | candidate_acc]}
          end)

        print_pair(
          "count write/batch-#{batch_size}",
          current_samples,
          candidate_samples,
          "current puts",
          "candidate puts+one-counter"
        )
      end)
    after
      release(current_path)
      release(candidate_path)
    end
  end

  defp count_puts(prefix, count) do
    Enum.map(1..count, fn index ->
      {:put, prefix <> String.pad_leading(Integer.to_string(index), 8, "0"), @count_value}
    end)
  end

  defp benchmark_filter_continuations do
    jobs =
      Enum.reduce([100, 10, 1], %{}, fn selectivity_percent, jobs ->
        payloads = filter_payloads(selectivity_percent)
        current = current_filtered(payloads)
        candidate = candidate_filtered(payloads)
        true = current.records == candidate.records

        IO.puts(
          "filter_work shape=selectivity-#{selectivity_percent}-percent " <>
            "current_hydrated=#{current.hydrated} candidate_hydrated=#{candidate.hydrated}"
        )

        Map.merge(jobs, %{
          "current restart/selectivity-#{selectivity_percent}-percent" => fn ->
            current_filtered(payloads)
          end,
          "candidate selectivity-aware-continuation/selectivity-#{selectivity_percent}-percent" =>
            fn ->
              candidate_filtered(payloads)
            end
        })
      end)

    Benchee.run(jobs, QueryPerformance.benchee_options("query-planner-filter-candidates"))
  end

  defp filter_payloads(selectivity_percent) do
    every = div(100, selectivity_percent)

    Enum.map(1..@filter_rows, fn index ->
      :erlang.term_to_binary(%{
        id: "filter-#{String.pad_leading(Integer.to_string(index), 8, "0")}",
        updated_at_ms: index,
        match?: rem(index, every) == 0,
        payload: :binary.copy(<<rem(index, 251)>>, 96)
      })
    end)
  end

  defp current_filtered(payloads) do
    query = %{count: @filter_limit, rev?: false}
    Process.put({__MODULE__, :hydrated}, 0)

    fetch = fn limit ->
      records = payloads |> Enum.take(limit) |> decode_payloads()

      Process.put(
        {__MODULE__, :hydrated},
        Process.get({__MODULE__, :hydrated}, 0) + length(records)
      )

      {:ok, records, length(records) < limit or limit >= length(payloads), length(records)}
    end

    {:ok, records, _reported_scanned} =
      RecordRead.scan_filtered_candidate_windows_with_count(
        query,
        @filter_rows,
        fetch,
        &Map.fetch!(&1, :match?)
      )

    %{records: Enum.map(records, & &1.id), hydrated: Process.get({__MODULE__, :hydrated})}
  after
    Process.delete({__MODULE__, :hydrated})
  end

  defp candidate_filtered(payloads) do
    {records, hydrated} =
      continue_filter(payloads, @filter_chunk, @filter_limit, 0, [], 0)

    records =
      records
      |> RecordQuery.sort_by_update()
      |> Enum.take(@filter_limit)

    %{records: Enum.map(records, & &1.id), hydrated: hydrated}
  end

  defp continue_filter(payloads, chunk_size, wanted, matched, matches, hydrated) do
    cond do
      matched >= wanted or payloads == [] ->
        {Enum.reverse(matches), hydrated}

      true ->
        {chunk, rest} = Enum.split(payloads, chunk_size)
        records = decode_payloads(chunk)

        {next_matches, next_matched} =
          Enum.reduce(records, {matches, matched}, fn record, {acc, count} ->
            if Map.fetch!(record, :match?),
              do: {[record | acc], count + 1},
              else: {acc, count}
          end)

        continue_filter(
          rest,
          next_filter_chunk(hydrated + length(records), next_matched),
          wanted,
          next_matched,
          next_matches,
          hydrated + length(records)
        )
    end
  end

  defp decode_payloads(payloads),
    do: Enum.map(payloads, &:erlang.binary_to_term(&1, [:safe]))

  defp benchmark_lmdb_filter_continuations do
    root = temp_root("filter-continuation-lmdb")

    datasets =
      Map.new([100, 10, 1], fn selectivity ->
        path = Path.join(root, "selectivity-#{selectivity}")
        prefix = "filter-lmdb:#{selectivity}:"
        File.mkdir_p!(path)

        1..@filter_rows
        |> Stream.chunk_every(@count_batch_size)
        |> Enum.each(fn indexes ->
          every = div(100, selectivity)

          operations =
            Enum.map(indexes, fn index ->
              key = prefix <> String.pad_leading(Integer.to_string(index), 8, "0")

              value =
                :erlang.term_to_binary(%{
                  id: "filter-#{String.pad_leading(Integer.to_string(index), 8, "0")}",
                  updated_at_ms: index,
                  match?: rem(index, every) == 0,
                  payload: :binary.copy(<<rem(index, 251)>>, 96)
                })

              {:put, key, value}
            end)

          :ok = LMDB.write_batch(path, operations)
        end)

        {selectivity, %{path: path, prefix: prefix}}
      end)

    try do
      jobs =
        Enum.reduce(datasets, %{}, fn {selectivity, dataset}, jobs ->
          current = current_lmdb_filtered(dataset)
          candidate = candidate_lmdb_filtered(dataset)
          true = current.records == candidate.records

          IO.puts(
            "filter_lmdb_work shape=selectivity-#{selectivity}-percent " <>
              "current_hydrated=#{current.hydrated} candidate_hydrated=#{candidate.hydrated} " <>
              "current_calls=#{current.calls} candidate_calls=#{candidate.calls}"
          )

          Map.merge(jobs, %{
            "current LMDB restart/selectivity-#{selectivity}-percent" => fn ->
              current_lmdb_filtered(dataset)
            end,
            "candidate LMDB selectivity-aware-continuation/selectivity-#{selectivity}-percent" =>
              fn ->
                candidate_lmdb_filtered(dataset)
              end
          })
        end)

      Benchee.run(jobs, QueryPerformance.benchee_options("query-planner-lmdb-filter-candidates"))
      benchmark_lmdb_filter_pairs(datasets)
    after
      Enum.each(datasets, fn {_selectivity, dataset} -> release(dataset.path) end)
      File.rm_rf!(root)
    end
  end

  defp current_lmdb_filtered(dataset) do
    initial = read_filter_rows(dataset, "", @filter_chunk)
    matches = matching_records(initial.rows)

    {records, hydrated, calls} =
      if length(matches) >= @filter_limit or initial.exhausted do
        {matches, length(initial.rows), 1}
      else
        expanded = read_filter_rows(dataset, "", @filter_rows)
        {matching_records(expanded.rows), length(initial.rows) + length(expanded.rows), 2}
      end

    %{
      records: records |> Enum.take(@filter_limit) |> Enum.map(& &1.id),
      hydrated: hydrated,
      calls: calls
    }
  end

  defp candidate_lmdb_filtered(dataset) do
    {records, hydrated, calls} =
      continue_lmdb_filter(dataset, "", @filter_chunk, 0, [], 0, 0, 0)

    %{
      records: records |> Enum.take(@filter_limit) |> Enum.map(& &1.id),
      hydrated: hydrated,
      calls: calls
    }
  end

  defp continue_lmdb_filter(
         _dataset,
         _cursor,
         _chunk,
         scanned,
         matches,
         matched,
         hydrated,
         calls
       )
       when matched >= @filter_limit or scanned >= @filter_rows,
       do: {Enum.reverse(matches), hydrated, calls}

  defp continue_lmdb_filter(
         dataset,
         cursor,
         chunk,
         scanned,
         matches,
         matched,
         hydrated,
         calls
       ) do
    requested = min(chunk, @filter_rows - scanned)
    page = read_filter_rows(dataset, cursor, requested)
    {matches, next_matched} = collect_filter_matches(page.rows, matches, matched)

    next_scanned = scanned + length(page.rows)
    next_hydrated = hydrated + length(page.rows)
    next_calls = calls + 1

    if next_matched >= @filter_limit or page.exhausted or page.rows == [] do
      {Enum.reverse(matches), next_hydrated, next_calls}
    else
      next_cursor = page.rows |> List.last() |> elem(0)

      continue_lmdb_filter(
        dataset,
        next_cursor,
        next_filter_chunk(next_scanned, next_matched),
        next_scanned,
        matches,
        next_matched,
        next_hydrated,
        next_calls
      )
    end
  end

  defp read_filter_rows(dataset, cursor, limit) do
    {:ok, rows} = LMDB.prefix_entries_after(dataset.path, dataset.prefix, cursor, limit)

    decoded =
      Enum.map(rows, fn {key, value} ->
        {key, :erlang.binary_to_term(value, [:safe])}
      end)

    %{rows: decoded, exhausted: length(rows) < limit}
  end

  defp matching_records(rows) do
    Enum.flat_map(rows, fn {_key, record} -> if record.match?, do: [record], else: [] end)
  end

  defp collect_filter_matches(rows, matches, matched) do
    Enum.reduce_while(rows, {matches, matched}, fn {_key, record}, {acc, count} ->
      if record.match? do
        next = count + 1
        result = {[record | acc], next}
        if next >= @filter_limit, do: {:halt, result}, else: {:cont, result}
      else
        {:cont, {acc, count}}
      end
    end)
  end

  defp benchmark_lmdb_filter_pairs(datasets) do
    Enum.each(datasets, fn {selectivity, dataset} ->
      expected = current_lmdb_filtered(dataset).records
      ^expected = candidate_lmdb_filtered(dataset).records

      {current_samples, candidate_samples} =
        Enum.reduce(1..200, {[], []}, fn sample, {current_acc, candidate_acc} ->
          :erlang.garbage_collect()

          if rem(sample, 2) == 0 do
            {current_ns, %{records: ^expected}} =
              QueryPerformance.timed_ns(fn -> current_lmdb_filtered(dataset) end)

            {candidate_ns, %{records: ^expected}} =
              QueryPerformance.timed_ns(fn -> candidate_lmdb_filtered(dataset) end)

            {[current_ns | current_acc], [candidate_ns | candidate_acc]}
          else
            {candidate_ns, %{records: ^expected}} =
              QueryPerformance.timed_ns(fn -> candidate_lmdb_filtered(dataset) end)

            {current_ns, %{records: ^expected}} =
              QueryPerformance.timed_ns(fn -> current_lmdb_filtered(dataset) end)

            {[current_ns | current_acc], [candidate_ns | candidate_acc]}
          end
        end)

      QueryPerformance.print_summary(
        "paired current LMDB filter/selectivity-#{selectivity}",
        QueryPerformance.latency_summary(current_samples)
      )

      QueryPerformance.print_summary(
        "paired candidate LMDB filter/selectivity-#{selectivity}",
        QueryPerformance.latency_summary(candidate_samples)
      )
    end)
  end

  defp next_filter_chunk(scanned, 0), do: @filter_rows - scanned

  defp next_filter_chunk(scanned, matched) do
    remaining_rows = @filter_rows - scanned
    remaining_matches = max(@filter_limit - matched, 0)
    estimated_rows = div(remaining_matches * scanned + matched - 1, matched)
    min(max(estimated_rows * 2, @filter_chunk), remaining_rows)
  end

  defp benchmark_multistate_merge do
    jobs =
      Enum.reduce([2, 4, 8, 16], %{}, fn source_count, jobs ->
        Enum.reduce([100, 4_096], jobs, fn limit, page_jobs ->
          sources = multistate_sources(source_count, limit)
          current = current_multistate(sources, limit)
          candidate = candidate_multistate(sources, limit)
          true = current == candidate
          suffix = "states-#{source_count}/page-#{limit}"

          page_jobs
          |> Map.put("current flatten+uniq+sort/#{suffix}", fn ->
            current_multistate(sources, limit)
          end)
          |> Map.put("candidate bounded-k-way/#{suffix}", fn ->
            candidate_multistate(sources, limit)
          end)
        end)
      end)

    Benchee.run(jobs, QueryPerformance.benchee_options("query-planner-multistate-candidates"))
    benchmark_multistate_pairs()
  end

  defp benchmark_multistate_pairs do
    Enum.each([2, 4, 8, 16], fn source_count ->
      Enum.each([100, 4_096], fn limit ->
        sources = multistate_sources(source_count, limit)
        expected = current_multistate(sources, limit)
        ^expected = candidate_multistate(sources, limit)
        samples = if limit == 100, do: 500, else: 100

        {current_samples, candidate_samples} =
          Enum.reduce(1..samples, {[], []}, fn sample, {current_acc, candidate_acc} ->
            :erlang.garbage_collect()

            if rem(sample, 2) == 0 do
              {current_ns, ^expected} =
                QueryPerformance.timed_ns(fn -> current_multistate(sources, limit) end)

              {candidate_ns, ^expected} =
                QueryPerformance.timed_ns(fn -> candidate_multistate(sources, limit) end)

              {[current_ns | current_acc], [candidate_ns | candidate_acc]}
            else
              {candidate_ns, ^expected} =
                QueryPerformance.timed_ns(fn -> candidate_multistate(sources, limit) end)

              {current_ns, ^expected} =
                QueryPerformance.timed_ns(fn -> current_multistate(sources, limit) end)

              {[current_ns | current_acc], [candidate_ns | candidate_acc]}
            end
          end)

        suffix = "states-#{source_count}/page-#{limit}"

        QueryPerformance.print_summary(
          "paired current multistate/#{suffix}",
          QueryPerformance.latency_summary(current_samples)
        )

        QueryPerformance.print_summary(
          "paired candidate multistate/#{suffix}",
          QueryPerformance.latency_summary(candidate_samples)
        )
      end)
    end)
  end

  defp multistate_sources(source_count, limit) do
    Enum.map(0..(source_count - 1), fn source ->
      Enum.map(0..(limit - 1), fn local_index ->
        score = local_index * source_count + source

        %{
          id: "state-#{String.pad_leading(Integer.to_string(score), 10, "0")}",
          updated_at_ms: score,
          state: "s#{source}"
        }
      end)
    end)
  end

  defp current_multistate(sources, limit) do
    sources
    |> List.flatten()
    |> Enum.uniq_by(& &1.id)
    |> RecordQuery.sort_by_update()
    |> Enum.take(limit)
  end

  defp candidate_multistate(sources, limit) do
    heap =
      sources
      |> Enum.with_index()
      |> Enum.reduce(:gb_sets.empty(), fn
        {[], _source}, heap ->
          heap

        {[entry | rest], source}, heap ->
          :gb_sets.add({record_rank(entry), source, entry, rest}, heap)
      end)

    take_multistate(heap, limit, MapSet.new(), [])
  end

  defp take_multistate(_heap, 0, _seen, acc), do: Enum.reverse(acc)

  defp take_multistate(heap, remaining, seen, acc) do
    if :gb_sets.is_empty(heap) do
      Enum.reverse(acc)
    else
      {{_rank, source, entry, rest}, heap} = :gb_sets.take_smallest(heap)

      heap =
        case rest do
          [] -> heap
          [next | tail] -> :gb_sets.add({record_rank(next), source, next, tail}, heap)
        end

      if MapSet.member?(seen, entry.id) do
        take_multistate(heap, remaining, seen, acc)
      else
        take_multistate(
          heap,
          remaining - 1,
          MapSet.put(seen, entry.id),
          [entry | acc]
        )
      end
    end
  end

  defp record_rank(record), do: {record.updated_at_ms, record.id}

  defp benchmark_short_index_keys do
    root = temp_root("short-keys")
    full_path = Path.join(root, "full")
    short_path = Path.join(root, "short")
    File.mkdir_p!(full_path)
    File.mkdir_p!(short_path)

    family_digest = :crypto.hash(:sha256, "benchmark-family")
    index_digest = :crypto.hash(:sha256, "benchmark-index")
    full_prefix = "flow-query-index:" <> <<1>> <> family_digest <> index_digest <> <<0>>
    short_prefix = <<0x51, 1::unsigned-big-64, 1::unsigned-big-64>>
    catalog_key = "short-index-catalog:" <> family_digest <> index_digest
    catalog_value = <<1, family_digest::binary, index_digest::binary, 1::64, 1::64>>

    try do
      1..@key_rows
      |> Stream.chunk_every(@count_batch_size)
      |> Enum.each(fn indexes ->
        full_ops = Enum.map(indexes, &{:put, ordered_key(full_prefix, &1), @key_value})
        short_ops = Enum.map(indexes, &{:put, ordered_key(short_prefix, &1), @key_value})
        :ok = LMDB.write_batch(full_path, full_ops)
        :ok = LMDB.write_batch(short_path, short_ops)
      end)

      :ok = LMDB.write_batch(short_path, [{:put, catalog_key, catalog_value}])
      {:ok, ^catalog_value} = LMDB.get(short_path, catalog_key)

      full_entries = Map.new(@page_sizes, &{&1, read_page(full_path, full_prefix, &1)})
      short_entries = Map.new(@page_sizes, &{&1, read_page(short_path, short_prefix, &1)})

      Enum.each(@page_sizes, fn page_size ->
        true =
          normalize_ordered_rows(Map.fetch!(full_entries, page_size), full_prefix) ==
            normalize_ordered_rows(Map.fetch!(short_entries, page_size), short_prefix)
      end)

      IO.puts(
        "short_key_storage rows=#{@key_rows} " <>
          "full_physical_bytes=#{QueryPerformance.directory_bytes(full_path)} " <>
          "short_physical_bytes=#{QueryPerformance.directory_bytes(short_path)}"
      )

      jobs =
        Enum.reduce(@page_sizes, %{}, fn page_size, jobs ->
          jobs
          |> Map.put("current full-digest scan/page-#{page_size}", fn ->
            LMDB.prefix_entries(full_path, full_prefix, page_size)
          end)
          |> Map.put("candidate catalog-resolve+short scan/page-#{page_size}", fn ->
            with {:ok, ^catalog_value} <- LMDB.get(short_path, catalog_key) do
              LMDB.prefix_entries(short_path, short_prefix, page_size)
            end
          end)
          |> Map.put("candidate prepared-short scan/page-#{page_size}", fn ->
            LMDB.prefix_entries(short_path, short_prefix, page_size)
          end)
        end)

      Benchee.run(jobs, QueryPerformance.benchee_options("query-planner-short-key-candidates"))
    after
      release(full_path)
      release(short_path)
      File.rm_rf!(root)
    end
  end

  defp ordered_key(prefix, index) do
    id = "row-#{String.pad_leading(Integer.to_string(index), 10, "0")}"
    prefix <> <<index::unsigned-big-64, id::binary>>
  end

  defp read_page(path, prefix, count) do
    {:ok, entries} = LMDB.prefix_entries(path, prefix, count)
    ^count = length(entries)
    entries
  end

  defp normalize_ordered_rows(entries, prefix) do
    prefix_bytes = byte_size(prefix)

    Enum.map(entries, fn {key, value} ->
      <<_prefix::binary-size(prefix_bytes), suffix::binary>> = key
      {suffix, value}
    end)
  end

  defp print_pair(name, current, candidate, current_label, candidate_label) do
    QueryPerformance.print_summary(
      "#{current_label}/#{name}",
      QueryPerformance.latency_summary(current)
    )

    QueryPerformance.print_summary(
      "#{candidate_label}/#{name}",
      QueryPerformance.latency_summary(candidate)
    )
  end

  defp temp_root(suffix) do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-query-#{suffix}-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root)
    root
  end

  defp release(path), do: Ferricstore.Bitcask.NIF.lmdb_release(path)
end

Ferricstore.Bench.QueryPlannerCoreReadCandidates.run()
