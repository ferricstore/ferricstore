Code.require_file("support/query_performance.exs", __DIR__)
Code.require_file("support/query_storage_fixture.exs", __DIR__)

defmodule Ferricstore.Bench.QueryPlannerCoveringIndexCandidates do
  @moduledoc false

  alias Ferricstore.Bench.{QueryPerformance, QueryStorageFixture}
  alias Ferricstore.Flow.{Keys, LMDB}

  alias Ferricstore.Flow.Query.{
    CompositeIndex,
    CompositeRange,
    CompositeRangeReader,
    IndexDefinition,
    QueryRecordStore,
    RecordProjection
  }

  @maximum_read_bytes 64 * 1_024 * 1_024
  @projection [:run_id, :state, :updated_at_ms]

  def run do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferric-query-covering-#{System.unique_integer([:positive, :monotonic])}"
      )

    records_per_log_entry =
      QueryPerformance.int_env("BENCH_COVERING_RECORDS_PER_LOG_ENTRY", 1, min: 1)

    if records_per_log_entry > 256 do
      raise ArgumentError, "BENCH_COVERING_RECORDS_PER_LOG_ENTRY must be <= 256"
    end

    datasets = datasets(root, records_per_log_entry)

    try do
      Enum.each(datasets, &preflight!/1)

      case System.get_env("BENCH_CANDIDATE_SECTION", "all") do
        "all" ->
          benchmark_all(datasets)

        "paired-hydration" ->
          benchmark_paired_hydration(datasets)

        invalid ->
          raise ArgumentError,
                "BENCH_CANDIDATE_SECTION must be all or paired-hydration, got #{inspect(invalid)}"
      end
    after
      Enum.each(datasets, fn dataset ->
        QueryStorageFixture.cleanup(dataset.hydrated_ctx)
        QueryStorageFixture.cleanup(dataset.covering_ctx)
      end)

      File.rm_rf(root)
    end
  end

  defp benchmark_all(datasets) do
    inputs = Map.new(datasets, &{&1.name, &1})

    Benchee.run(
      %{
        "production index scan plus authoritative hydration" => &hydrated/1,
        "production covered projection without log reads" => &covered/1,
        "production covering scan plus unused decode and authoritative hydration" =>
          &decoded_cover_hydration/1,
        "lower-bound covering scan without cover decode plus authoritative hydration" =>
          &raw_cover_hydration/1
      },
      [inputs: inputs] ++ QueryPerformance.benchee_options("query-planner-covering-index")
    )
  end

  defp benchmark_paired_hydration(datasets) do
    samples = QueryPerformance.int_env("BENCH_PAIRED_SAMPLES", 101, min: 3)
    repeats = QueryPerformance.int_env("BENCH_PAIRED_REPEATS", 10, min: 1)

    Enum.each(datasets, fn dataset ->
      expected = dataset.expected

      {decoded_cover_samples, raw_cover_samples} =
        Enum.reduce(1..samples, {[], []}, fn sample, {decoded_acc, raw_acc} ->
          :erlang.garbage_collect()

          decoded_cover_run = fn ->
            repeat(repeats, fn -> decoded_cover_hydration(dataset) end)
          end

          raw_cover_run = fn ->
            repeat(repeats, fn -> raw_cover_hydration(dataset) end)
          end

          {decoded_cover_ns, ^expected, raw_cover_ns, ^expected} =
            if rem(sample, 2) == 0 do
              {raw_ns, raw_result} = QueryPerformance.timed_ns(raw_cover_run)
              {decoded_ns, decoded_result} = QueryPerformance.timed_ns(decoded_cover_run)
              {decoded_ns, decoded_result, raw_ns, raw_result}
            else
              {decoded_ns, decoded_result} = QueryPerformance.timed_ns(decoded_cover_run)
              {raw_ns, raw_result} = QueryPerformance.timed_ns(raw_cover_run)
              {decoded_ns, decoded_result, raw_ns, raw_result}
            end

          {[div(decoded_cover_ns, repeats) | decoded_acc], [div(raw_cover_ns, repeats) | raw_acc]}
        end)

      decoded_cover_median = QueryPerformance.percentile(decoded_cover_samples, 50)
      raw_cover_median = QueryPerformance.percentile(raw_cover_samples, 50)

      IO.puts(
        "covering_hydration_pair input=#{dataset.name} " <>
          "decoded_cover_median_ns=#{decoded_cover_median} " <>
          "raw_cover_median_ns=#{raw_cover_median} " <>
          "speedup=#{Float.round(decoded_cover_median / raw_cover_median, 2)}x"
      )
    end)
  end

  defp repeat(count, fun), do: repeat(count, fun, nil)
  defp repeat(0, _fun, result), do: result
  defp repeat(count, fun, _result), do: repeat(count - 1, fun, fun.())

  defp datasets(root, records_per_log_entry) do
    record_counts =
      QueryPerformance.integer_list_env("BENCH_COVERING_RECORD_COUNTS", [1, 25, 100], min: 1)

    value_bytes =
      QueryPerformance.integer_list_env(
        "BENCH_COVERING_VALUE_BYTES",
        [128, 4 * 1_024, 1 * 1_024 * 1_024],
        min: 1
      )

    for count <- record_counts,
        bytes <- value_bytes,
        count * bytes <= @maximum_read_bytes do
      dataset(root, count, bytes, records_per_log_entry)
    end
  end

  defp dataset(root, count, value_bytes, records_per_log_entry) do
    name = "page #{count} / payload #{value_bytes}"
    hydrated_ctx = context(Path.join(root, "hydrated-#{count}-#{value_bytes}"))
    covering_ctx = context(Path.join(root, "covering-#{count}-#{value_bytes}"))
    hydrated_path = lmdb_path(hydrated_ctx)
    covering_path = lmdb_path(covering_ctx)
    hydrated_definition = definition([])

    covering_definition =
      definition([:partition_key, :run_id, :state, :updated_at_ms, :version])

    {records, hydrated_ops, covering_ops, expected} =
      Enum.reduce(1..count, {[], [], [], []}, fn number,
                                                 {records, hydrated_ops, covering_ops, expected} ->
        id = "run-#{number |> Integer.to_string() |> String.pad_leading(6, "0")}"
        record = record(id, number, value_bytes)
        state_key = Keys.state_key(id, record.partition_key)
        {:ok, projected} = RecordProjection.project_validated(record, :runs, @projection)

        {:ok, [hydrated_entry]} =
          CompositeIndex.entries(hydrated_definition, record, state_key, 0)

        {:ok, [covering_entry]} =
          CompositeIndex.entries(covering_definition, record, state_key, 0)

        hydrated_ops = [{:put, hydrated_entry.key, hydrated_entry.value} | hydrated_ops]
        covering_ops = [{:put, covering_entry.key, covering_entry.value} | covering_ops]

        {[record | records], hydrated_ops, covering_ops, [projected | expected]}
      end)

    records = Enum.reverse(records)
    storage_opts = [page_records: min(records_per_log_entry, count)]
    hydrated_storage = QueryStorageFixture.write!(hydrated_ctx, records, storage_opts)
    covering_storage = QueryStorageFixture.write!(covering_ctx, records, storage_opts)
    :ok = LMDB.write_batch(hydrated_path, Enum.reverse(hydrated_ops))
    :ok = LMDB.write_batch(covering_path, Enum.reverse(covering_ops))

    %{
      name: name,
      count: count,
      records_per_log_entry: records_per_log_entry,
      hydrated_ctx: hydrated_ctx,
      covering_ctx: covering_ctx,
      hydrated_path: hydrated_path,
      covering_path: covering_path,
      hydrated_range: range(hydrated_definition),
      covering_range: range(covering_definition),
      expected: expected,
      source_logical_bytes: hydrated_storage.source_bytes,
      hydrated_lmdb_logical_bytes: hydrated_storage.query_row_bytes + logical_bytes(hydrated_ops),
      covering_lmdb_logical_bytes: covering_storage.query_row_bytes + logical_bytes(covering_ops),
      hydrated_physical_bytes: QueryPerformance.directory_bytes(hydrated_ctx.data_dir),
      covering_physical_bytes: QueryPerformance.directory_bytes(covering_ctx.data_dir)
    }
  end

  defp preflight!(dataset) do
    expected = dataset.expected
    ^expected = hydrated(dataset)
    ^expected = covered(dataset)
    ^expected = decoded_cover_hydration(dataset)
    ^expected = raw_cover_hydration(dataset)

    IO.puts(
      "covering #{dataset.name} " <>
        "records_per_log_entry=#{dataset.records_per_log_entry} " <>
        "source_logical_bytes=#{dataset.source_logical_bytes} " <>
        "hydrated_lmdb_logical_bytes=#{dataset.hydrated_lmdb_logical_bytes} " <>
        "covering_lmdb_logical_bytes=#{dataset.covering_lmdb_logical_bytes} " <>
        "hydrated_physical_bytes=#{dataset.hydrated_physical_bytes} " <>
        "covering_physical_bytes=#{dataset.covering_physical_bytes}"
    )
  end

  defp logical_bytes(ops) do
    Enum.reduce(ops, 0, fn {:put, key, value}, bytes ->
      bytes + byte_size(key) + byte_size(value)
    end)
  end

  defp hydrated(dataset) do
    {:ok, %{entries: entries}} =
      CompositeRangeReader.read(
        dataset.hydrated_path,
        dataset.hydrated_range,
        nil,
        dataset.count,
        @maximum_read_bytes
      )

    state_keys = Enum.map(entries, & &1.state_key)

    hydrate(dataset.hydrated_ctx, dataset.hydrated_path, state_keys)
  end

  defp decoded_cover_hydration(dataset) do
    {:ok, %{entries: entries}} =
      CompositeRangeReader.read(
        dataset.covering_path,
        dataset.covering_range,
        nil,
        dataset.count,
        @maximum_read_bytes
      )

    hydrate(dataset.covering_ctx, dataset.covering_path, Enum.map(entries, & &1.state_key))
  end

  defp raw_cover_hydration(dataset) do
    range = dataset.covering_range

    {:ok, rows, _exhausted, _scan_bytes} =
      LMDB.composite_range_entries_bounded(
        dataset.covering_path,
        range.prefix,
        range.after_key,
        range.before_key,
        dataset.count,
        @maximum_read_bytes
      )

    state_keys =
      Enum.map(rows, fn {_key, _id, state_key, _version, _expiry, _bytes, _cover} -> state_key end)

    hydrate(dataset.covering_ctx, dataset.covering_path, state_keys)
  end

  defp hydrate(ctx, path, state_keys) do
    {:ok, records, true} =
      QueryRecordStore.read_many(ctx, 0, path, state_keys, 0, @maximum_read_bytes)

    Enum.map(records, fn record ->
      {:ok, projected} = RecordProjection.project_validated(record, :runs, @projection)
      projected
    end)
  end

  defp covered(dataset) do
    {:ok, %{entries: entries}} =
      CompositeRangeReader.read(
        dataset.covering_path,
        dataset.covering_range,
        nil,
        dataset.count,
        @maximum_read_bytes
      )

    Enum.map(entries, fn %{covering_record: record} ->
      {:ok, projected} = RecordProjection.project_validated(record, :runs, @projection)
      projected
    end)
  end

  defp lmdb_path(ctx) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(0)
    |> LMDB.path()
  end

  defp context(data_dir) do
    %{
      name: :query_planner_covering_index_candidates,
      data_dir: data_dir,
      shard_count: 1,
      slot_map: List.to_tuple(List.duplicate(0, 1_024))
    }
  end

  defp definition(covering_fields) do
    IndexDefinition.new!(%{
      id: "bench-covering-index",
      version: 1,
      fields: [
        {:partition_key, :asc},
        {:state, :asc},
        {:updated_at_ms, :desc}
      ],
      covering_fields: covering_fields
    })
  end

  defp range(definition) do
    {:ok, range} = CompositeRange.prefix(definition, ["tenant-a", "queued"])
    range
  end

  defp record(id, number, value_bytes) do
    %{
      id: id,
      type: "invoice",
      state: "queued",
      version: number,
      priority: 0,
      partition_key: "tenant-a",
      created_at_ms: number,
      updated_at_ms: number,
      next_run_at_ms: number,
      lease_deadline_ms: nil,
      attempts: 0,
      run_state: "ready",
      max_active_ms: nil,
      parent_flow_id: nil,
      root_flow_id: nil,
      correlation_id: nil,
      attributes: %{},
      state_meta: %{},
      child_groups: %{"benchmark_payload" => :binary.copy("x", value_bytes)}
    }
  end
end

Ferricstore.Bench.QueryPlannerCoveringIndexCandidates.run()
