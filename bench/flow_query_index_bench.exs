defmodule Ferricstore.Flow.Query.IndexBenchmark do
  alias FerricStore.Flow.MetadataExtension
  alias Ferricstore.Flow.{Codec, Keys, LMDB}

  alias Ferricstore.Flow.Query.{
    CompositeBackfill,
    MandatoryScope,
    RegisteredIndex,
    Request
  }

  alias Ferricstore.Flow.Query.{
    Budget,
    Executor,
    IndexCatalog,
    Planner,
    Response
  }

  @cursor_key :binary.copy(<<0x5A>>, 32)
  @backfill_page_records 16

  def run do
    records = env_integer("BENCH_RECORDS", 10_000, 32, 1_000_000)
    iterations = env_integer("BENCH_QUERY_ITERATIONS", 500, 1, 100_000)
    warmup = env_integer("BENCH_QUERY_WARMUP", 50, 0, 10_000)

    {:ok, catalog} = IndexCatalog.load()
    definitions = selected_definitions(catalog.definitions)

    results =
      Enum.map(definitions, fn definition ->
        IO.puts(:stderr, "benchmark_index_start id=#{definition.id}")
        result = benchmark_definition(definition, records, iterations, warmup)
        IO.puts(:stderr, "benchmark_index_complete id=#{definition.id}")
        result
      end)

    report = %{
      benchmark: "ferric.flow.query.launch-indexes/v1",
      generated_at: DateTime.utc_now() |> DateTime.to_iso8601(),
      runtime: %{
        elixir: System.version(),
        otp: System.otp_release(),
        records: records,
        query_iterations: iterations,
        query_warmup: warmup
      },
      indexes: results
    }

    encoded = Jason.encode!(report, pretty: true)
    IO.puts(encoded)

    case System.get_env("BENCH_OUTPUT") do
      nil -> :ok
      "" -> :ok
      path -> File.write!(path, encoded <> "\n")
    end
  end

  defp benchmark_definition(definition, record_count, iterations, warmup) do
    suffix = System.unique_integer([:positive, :monotonic])
    data_dir = Path.join(System.tmp_dir!(), "ferricstore_query_bench_#{suffix}")
    ctx = context(data_dir)
    records = records(record_count)

    try do
      source_bytes = write_source_records(ctx, records)
      before_bytes = directory_bytes(data_dir)

      {backfill_us, metrics} =
        timed(fn -> project_records(ctx, records, definition) end)

      after_bytes = directory_bytes(data_dir)
      request = request_for(definition.id, record_count)
      active = active_index(definition)

      if warmup > 0 do
        for _iteration <- 1..warmup do
          :ok = execute_query(ctx, request, active)
        end
      end

      latencies =
        for _iteration <- 1..iterations do
          {elapsed_us, :ok} = timed(fn -> execute_query(ctx, request, active) end)
          elapsed_us
        end

      %{
        id: definition.id,
        version: definition.version,
        read_latency_us: percentiles(latencies),
        backfill: %{
          elapsed_ms: round_to(backfill_us / 1_000, 3),
          records_per_second: round_to(rate(record_count, backfill_us), 2),
          projected_records: metrics.projected_records,
          index_entries: metrics.written_entries
        },
        write_amplification: %{
          operations_per_record: round_to(metrics.write_ops / record_count, 3),
          index_entries_per_record: round_to(metrics.written_entries / record_count, 3),
          logical_bytes_per_record: round_to(metrics.written_bytes / record_count, 2),
          logical_bytes_per_source_byte: round_to(metrics.written_bytes / source_bytes, 4)
        },
        storage: %{
          source_logical_bytes: source_bytes,
          index_logical_write_bytes: metrics.written_bytes,
          lmdb_file_growth_bytes: max(after_bytes - before_bytes, 0),
          lmdb_bytes_before: before_bytes,
          lmdb_bytes_after: after_bytes
        }
      }
    after
      File.rm_rf!(data_dir)
    end
  end

  defp write_source_records(ctx, records) do
    path = lmdb_path(ctx)

    records
    |> Enum.chunk_every(256)
    |> Enum.reduce(0, fn page, total_bytes ->
      ops =
        Enum.map(page, fn record ->
          key = Keys.state_key(record.id, record.partition_key)
          value = LMDB.encode_value(Codec.encode_record(record), 0)
          {:put, key, value}
        end)

      :ok = LMDB.write_batch(path, ops)

      total_bytes +
        Enum.reduce(ops, 0, fn {:put, key, value}, bytes ->
          bytes + byte_size(key) + byte_size(value)
        end)
    end)
  end

  defp project_records(ctx, records, definition) do
    encoded_by_key =
      Map.new(records, fn record ->
        {Keys.state_key(record.id, record.partition_key), Codec.encode_record(record)}
      end)

    read_entries = fn _ctx, 0, keys ->
      {:ok,
       Enum.map(keys, fn key ->
         case Map.fetch(encoded_by_key, key) do
           {:ok, encoded} -> {encoded, 0}
           :error -> nil
         end
       end)}
    end

    records
    |> Enum.chunk_every(@backfill_page_records)
    |> Enum.reduce(empty_metrics(), fn page, total ->
      projected =
        Enum.map(page, fn record ->
          %{
            state_key: Keys.state_key(record.id, record.partition_key),
            record: record,
            expire_at_ms: 0
          }
        end)

      {:ok, metrics} =
        CompositeBackfill.project_page(ctx, 0, projected, [definition],
          read_entries_fun: read_entries
        )

      merge_metrics(total, metrics)
    end)
  end

  defp execute_query(ctx, request, active) do
    with {:ok, plan} <- Planner.plan(request, [active], now_ms: 2_000_000_000_000),
         true <- plan.path == :ordered_range and length(plan.ranges) == 1,
         {:ok, result} <-
           Executor.execute(ctx, 0, request, plan,
             cursor_key: @cursor_key,
             now_ms: 2_000_000_000_000
           ),
         {:ok, _response} <-
           Response.build(
             result.records,
             result.has_more,
             result.continuation,
             result.quality,
             result.usage,
             Budget.default()
           ) do
      :ok
    else
      failure -> raise "query benchmark failed: #{inspect(failure)}"
    end
  end

  defp request_for("flow_runs_tenant_updated", record_count) do
    collection([time_window(record_count)])
  end

  defp request_for("flow_runs_tenant_state_updated", record_count) do
    collection([eq(:state, "failed"), time_window(record_count)])
  end

  defp request_for("flow_runs_tenant_type_updated", record_count) do
    collection([eq(:type, "invoice"), time_window(record_count)])
  end

  defp request_for("flow_runs_tenant_type_state_updated", record_count) do
    collection([eq(:type, "invoice"), eq(:state, "failed"), time_window(record_count)])
  end

  defp request_for("flow_runs_tenant_type_state_lease_deadline", record_count) do
    Request.collection(
      :execute,
      [
        eq(:partition_key, "benchmark-tenant"),
        eq(:type, "workflow"),
        eq(:state, "running"),
        {:range, :lease_deadline_ms, integer(0), integer(record_count + 1)}
      ],
      [{:lease_deadline_ms, :asc}],
      25,
      :record
    )
  end

  defp request_for(id, _record_count), do: raise("unsupported launch index: #{id}")

  defp collection(predicates) do
    Request.collection(
      :execute,
      [eq(:partition_key, "benchmark-tenant") | predicates],
      [{:updated_at_ms, :desc}],
      25,
      :record
    )
  end

  defp time_window(record_count) do
    {:time_window, :updated_at_ms, integer(0), integer(record_count + 1)}
  end

  defp records(count) do
    Enum.map(1..count, fn ordinal ->
      %{
        id: "run-#{String.pad_leading(Integer.to_string(ordinal), 12, "0")}",
        type: if(rem(ordinal, 3) == 0, do: "invoice", else: "workflow"),
        state: Enum.at(["failed", "running", "completed"], rem(ordinal, 3)),
        version: 1,
        priority: rem(ordinal, 10),
        partition_key: "benchmark-tenant",
        created_at_ms: ordinal - 1,
        updated_at_ms: ordinal,
        lease_deadline_ms: ordinal,
        attempts: rem(ordinal, 4)
      }
    end)
  end

  defp active_index(definition) do
    RegisteredIndex.new!(definition, :active,
      coverage: %{complete_shards: 1, total_shards: 1, validation: :passed}
    )
  end

  defp empty_metrics do
    %{projected_records: 0, written_entries: 0, write_ops: 0, written_bytes: 0}
  end

  defp merge_metrics(left, right) do
    Map.new(left, fn {field, value} -> {field, value + Map.fetch!(right, field)} end)
  end

  defp percentiles(values) do
    sorted = Enum.sort(values)

    %{
      min: hd(sorted),
      p50: percentile(sorted, 50),
      p95: percentile(sorted, 95),
      p99: percentile(sorted, 99),
      p99_9: percentile(sorted, 99.9),
      max: List.last(sorted)
    }
  end

  defp percentile(sorted, percentile) do
    index = max(ceil(length(sorted) * percentile / 100) - 1, 0)
    Enum.at(sorted, index)
  end

  defp timed(fun) do
    started = System.monotonic_time(:microsecond)
    result = fun.()
    {System.monotonic_time(:microsecond) - started, result}
  end

  defp directory_bytes(path) do
    path
    |> Path.join("**/*")
    |> Path.wildcard(match_dot: true)
    |> Enum.reduce(0, fn entry, bytes ->
      case File.stat(entry) do
        {:ok, %File.Stat{type: :regular, size: size}} -> bytes + size
        _other -> bytes
      end
    end)
  end

  defp rate(records, elapsed_us) when elapsed_us > 0, do: records * 1_000_000 / elapsed_us
  defp rate(_records, _elapsed_us), do: 0.0
  defp round_to(value, places), do: Float.round(value * 1.0, places)

  defp lmdb_path(ctx) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(0)
    |> LMDB.path()
  end

  defp context(data_dir) do
    {:ok, metadata_snapshot} =
      MetadataExtension.configure(FerricStore.Flow.MetadataExtension.Disabled, [])

    %{
      name: :flow_query_index_benchmark,
      data_dir: data_dir,
      shard_count: 1,
      slot_map: List.to_tuple(List.duplicate(0, 1_024)),
      flow_metadata_snapshot: metadata_snapshot,
      query_mandatory_scope: MandatoryScope.dedicated()
    }
  end

  defp env_integer(name, default, minimum, maximum) do
    case System.get_env(name) do
      nil ->
        default

      value ->
        case Integer.parse(value) do
          {parsed, ""} when parsed >= minimum and parsed <= maximum -> parsed
          _invalid -> raise "#{name} must be an integer in #{minimum}..#{maximum}"
        end
    end
  end

  defp selected_definitions(definitions) do
    case System.get_env("BENCH_INDEX") do
      nil ->
        definitions

      "" ->
        definitions

      id ->
        case Enum.filter(definitions, &(&1.id == id)) do
          [_definition] = selected -> selected
          [] -> raise "BENCH_INDEX does not match a launch index: #{id}"
        end
    end
  end

  defp eq(field, value), do: {:eq, field, keyword(value)}
  defp keyword(value), do: {:literal, :keyword, value}
  defp integer(value), do: {:literal, :integer, value}
end

Ferricstore.Flow.Query.IndexBenchmark.run()
