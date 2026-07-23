# Benchmark-only projection and backfill write prototypes. Production writers
# and transaction contracts are unchanged.

Code.require_file("support/query_performance.exs", __DIR__)

defmodule Ferricstore.Bench.QueryPlannerProjectionCandidates do
  @moduledoc false

  alias Ferricstore.Bench.QueryPerformance
  alias Ferricstore.Flow.{Keys, LMDB}
  alias Ferricstore.Flow.LMDBWriter.ProjectionOps

  alias Ferricstore.Flow.Query.{
    CompositeIndex,
    IndexDefinition
  }

  @samples 30
  @index_value :binary.copy(<<53>>, 112)
  @projection_value :binary.copy(<<67>>, 192)
  @projection_fanout 8

  defmodule Provider do
    @moduledoc false
    @behaviour FerricStore.Flow.QueryIndexProvider

    alias Ferricstore.Flow.Query.{RegisteredIndex, RegistrySnapshot}

    @impl true
    def snapshot(%{definitions: definitions}, _shard_index) do
      indexes = Enum.map(definitions, &RegisteredIndex.new!(&1, :active))
      {:ok, RegistrySnapshot.new!(%{epoch: 1, catalog_version: 1, indexes: indexes})}
    end
  end

  def run do
    case System.get_env("BENCH_CANDIDATE_SECTION", "all") do
      "all" ->
        benchmark_counter_coalescing()
        benchmark_projection_prefetch()
        benchmark_adaptive_backfill_pages()

      "counters" ->
        benchmark_counter_coalescing()

      "prefetch" ->
        benchmark_projection_prefetch()

      "pages" ->
        benchmark_adaptive_backfill_pages()

      "projection-batch" ->
        benchmark_projection_batch()

      invalid ->
        raise ArgumentError,
              "BENCH_CANDIDATE_SECTION must be all, counters, prefetch, pages, or " <>
                "projection-batch; " <>
                "got #{inspect(invalid)}"
    end
  end

  defp benchmark_counter_coalescing do
    root = temp_root("counter-coalescing")
    current_path = Path.join(root, "current")
    candidate_path = Path.join(root, "candidate")
    File.mkdir_p!(current_path)
    File.mkdir_p!(candidate_path)

    try do
      Enum.each([16, 64, 256], fn records ->
        {current_samples, candidate_samples} =
          Enum.reduce(1..@samples, {[], []}, fn sample, {current_acc, candidate_acc} ->
            current_tag = "current:#{records}:#{sample}"
            candidate_tag = "candidate:#{records}:#{sample}"

            current_ops = repeated_counter_ops(current_tag, records)
            candidate_ops = coalesced_counter_ops(candidate_tag, records)

            {current_elapsed, :ok} =
              QueryPerformance.timed_ns(fn -> LMDB.write_batch(current_path, current_ops) end)

            {candidate_elapsed, :ok} =
              QueryPerformance.timed_ns(fn ->
                LMDB.write_batch(candidate_path, candidate_ops)
              end)

            {:ok, ^records} = read_count(current_path, counter_key(current_tag))
            {:ok, ^records} = read_count(candidate_path, counter_key(candidate_tag))
            {:ok, ^records} = LMDB.prefix_count(current_path, index_prefix(current_tag))
            {:ok, ^records} = LMDB.prefix_count(candidate_path, index_prefix(candidate_tag))

            {[current_elapsed | current_acc], [candidate_elapsed | candidate_acc]}
          end)

        IO.puts(
          "counter_ops records=#{records} current_ops=#{records * 3} " <>
            "candidate_ops=#{records + 2}"
        )

        print_pair(
          "counter coalescing/records-#{records}",
          current_samples,
          candidate_samples,
          "current repeated-counter",
          "candidate coalesced-counter"
        )
      end)
    after
      release(current_path)
      release(candidate_path)
      File.rm_rf!(root)
    end
  end

  defp repeated_counter_ops(tag, records) do
    count_key = counter_key(tag)

    Enum.flat_map(1..records, fn index ->
      compare =
        if index == 1,
          do: {:compare_missing, count_key},
          else: {:compare, count_key, LMDB.encode_count(index - 1)}

      [
        compare,
        {:put, index_key(tag, index), @index_value},
        {:put, count_key, LMDB.encode_count(index)}
      ]
    end)
  end

  defp coalesced_counter_ops(tag, records) do
    count_key = counter_key(tag)

    [{:compare_missing, count_key}] ++
      Enum.map(1..records, &{:put, index_key(tag, &1), @index_value}) ++
      [{:put, count_key, LMDB.encode_count(records)}]
  end

  defp benchmark_projection_prefetch do
    root = temp_root("projection-prefetch")
    path = Path.join(root, "env")
    key_counts = QueryPerformance.integer_list_env("BENCH_PROJECTION_PREFETCH_KEYS", [16, 64])
    File.mkdir_p!(path)

    try do
      jobs =
        Enum.reduce(key_counts, %{}, fn key_count, jobs ->
          {rows, puts, max_bytes} = reverse_rows(key_count)

          :ok = LMDB.write_batch(path, puts)

          current = read_individually(path, rows)
          candidate = read_together(path, rows, max_bytes)
          true = current == candidate

          Map.merge(jobs, %{
            "current individual reverse reads/keys-#{key_count}" => fn ->
              read_individually(path, rows)
            end,
            "candidate bounded prefetch/keys-#{key_count}" => fn ->
              read_together(path, rows, max_bytes)
            end
          })
        end)

      Benchee.run(jobs, QueryPerformance.benchee_options("query-planner-projection-prefetch"))
    after
      release(path)
      File.rm_rf!(root)
    end
  end

  defp read_individually(path, rows) do
    Enum.map(rows, fn {state_key, reverse_key} ->
      {:ok, blob} = LMDB.get(path, reverse_key)
      {:ok, state} = CompositeIndex.decode_reverse_state(blob, state_key)
      state
    end)
  end

  defp read_together(path, rows, max_bytes) do
    keys = Enum.map(rows, &elem(&1, 1))
    {:ok, values, ^max_bytes} = LMDB.get_many_bounded(path, keys, max_bytes)

    rows
    |> Enum.zip(values)
    |> Enum.map(fn {{state_key, _reverse_key}, {:ok, blob}} ->
      {:ok, state} = CompositeIndex.decode_reverse_state(blob, state_key)
      state
    end)
  end

  defp reverse_rows(key_count) do
    definition =
      IndexDefinition.new!(%{
        id: "projection_prefetch",
        version: 1,
        fields: [{:partition_key, :asc}, {:updated_at_ms, :desc}]
      })

    1..key_count
    |> Enum.reduce({[], [], 0}, fn number, {rows, puts, bytes} ->
      id = "projection-prefetch-#{key_count}-#{number}"
      state_key = Keys.state_key(id, "tenant-a")

      record = %{
        run_id: id,
        id: id,
        partition_key: "tenant-a",
        updated_at_ms: number,
        version: number
      }

      {:ok, [entry]} = CompositeIndex.entries(definition, record, state_key, 0)
      reverse_key = CompositeIndex.reverse_key(state_key)
      reverse_value = CompositeIndex.encode_reverse_value(state_key, [entry.key])

      {[{state_key, reverse_key} | rows], [{:put, reverse_key, reverse_value} | puts],
       bytes + byte_size(reverse_value)}
    end)
    |> then(fn {rows, puts, bytes} -> {Enum.reverse(rows), Enum.reverse(puts), bytes} end)
  end

  defp benchmark_adaptive_backfill_pages do
    root = temp_root("adaptive-pages")

    try do
      Enum.each([16, 64, 256], fn page_records ->
        path = Path.join(root, "page-#{page_records}")
        File.mkdir_p!(path)

        samples =
          Enum.map(1..@samples, fn sample ->
            operations = projection_page_ops(page_records, sample)

            {elapsed, :ok} =
              QueryPerformance.timed_ns(fn -> LMDB.write_batch(path, operations) end)

            elapsed
          end)

        expected_rows = @samples * page_records * (@projection_fanout + 1)
        {:ok, ^expected_rows} = LMDB.prefix_count(path, "projection-page:")

        QueryPerformance.print_summary(
          "adaptive page transaction/records-#{page_records}",
          QueryPerformance.latency_summary(samples)
        )

        per_record = Enum.map(samples, &div(&1, page_records))

        QueryPerformance.print_summary(
          "adaptive page per-record/records-#{page_records}",
          QueryPerformance.latency_summary(per_record)
        )

        operation_count = page_records * (@projection_fanout + 1)
        logical_bytes = operation_count * byte_size(@projection_value)

        IO.puts(
          "adaptive_page_shape records=#{page_records} operations=#{operation_count} " <>
            "logical_value_bytes=#{logical_bytes}"
        )

        release(path)
      end)
    after
      File.rm_rf!(root)
    end
  end

  defp projection_page_ops(page_records, sample) do
    Enum.flat_map(1..page_records, fn record ->
      base = "projection-page:#{sample}:#{record}:"

      index_ops =
        Enum.map(1..@projection_fanout, fn fanout ->
          {:put, base <> "index:#{fanout}", @projection_value}
        end)

      [{:put, base <> "reverse", @projection_value} | index_ops]
    end)
  end

  defp benchmark_projection_batch do
    root = temp_root("projection-batch")

    record_counts =
      QueryPerformance.integer_list_env("BENCH_PROJECTION_BATCH_RECORDS", [1, 64, 512])

    definition =
      IndexDefinition.new!(%{
        id: "projection_batch",
        version: 1,
        fields: [{:partition_key, :asc}, {:state, :asc}, {:updated_at_ms, :desc}]
      })

    try do
      jobs =
        Enum.reduce(record_counts, %{}, fn record_count, jobs ->
          path = Path.join(root, "records-#{record_count}")
          File.mkdir_p!(path)

          state = %{
            path: path,
            shard_index: 0,
            instance_ctx: %{query_index_provider: Provider, definitions: [definition]},
            terminal_count_inits: MapSet.new()
          }

          initial = projection_records(record_count, 2)
          candidate = projection_records(record_count, 3)

          {:ok, initial_ops, _state} = ProjectionOps.expand_ops(state, initial)
          :ok = LMDB.write_batch(path, initial_ops)

          Map.put(jobs, "query-only expansion/records-#{record_count}", fn ->
            {:ok, ops, _state} = ProjectionOps.expand_ops(state, candidate)
            length(ops)
          end)
        end)

      Benchee.run(
        jobs,
        QueryPerformance.benchee_options("query-planner-projection-batch")
      )
    after
      for record_count <- record_counts do
        release(Path.join(root, "records-#{record_count}"))
      end

      File.rm_rf!(root)
    end
  end

  defp projection_records(record_count, version) do
    Enum.map(1..record_count, fn number ->
      id = "projection-batch-#{number}"
      state_key = Keys.state_key(id, "tenant-a")
      encoded = id |> projection_record(version, number) |> Ferricstore.Flow.encode_record()
      {:project_flow_query_state, state_key, encoded, 0}
    end)
  end

  defp projection_record(id, version, number) do
    %{
      id: id,
      type: "invoice",
      state: "running",
      version: version,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: number,
      updated_at_ms: version * 1_000 + number,
      next_run_at_ms: number,
      priority: 0,
      ttl_ms: nil,
      history_hot_max_events: nil,
      history_max_events: nil,
      retention_ttl_ms: nil,
      max_active_ms: nil,
      terminal_retention_until_ms: nil,
      partition_key: "tenant-a",
      payload_ref: nil,
      parent_flow_id: nil,
      parent_partition_key: nil,
      root_flow_id: id,
      correlation_id: nil,
      result_ref: nil,
      error_ref: nil,
      lease_owner: nil,
      lease_token: nil,
      lease_deadline_ms: 0,
      run_state: nil,
      state_enter_seq: version,
      child_groups: %{}
    }
  end

  defp read_count(path, key) do
    case LMDB.get(path, key) do
      {:ok, value} -> LMDB.decode_count(value)
      :not_found -> {:ok, 0}
      {:error, _reason} = error -> error
    end
  end

  defp counter_key(tag), do: "projection-counter:#{tag}"
  defp index_prefix(tag), do: "projection-index:#{tag}:"

  defp index_key(tag, index),
    do: index_prefix(tag) <> String.pad_leading(Integer.to_string(index), 6, "0")

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

Ferricstore.Bench.QueryPlannerProjectionCandidates.run()
