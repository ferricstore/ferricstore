# FerricStore Stream engine benchmark.
#
# Run:
#   MIX_ENV=bench mix run --no-start bench/stream_bench.exs
#
# This runner measures the embedded API through the production Router and
# replicated Stream commands. It intentionally excludes socket/client codec
# time; use a native-protocol SDK runner for transport comparisons.

alias Ferricstore.Commands.Stream
alias Ferricstore.Commands.Stream.Index

Logger.configure(level: :warning)

env_integer = fn name, default ->
  case Integer.parse(System.get_env(name, default)) do
    {value, ""} when value > 0 -> value
    _ -> raise ArgumentError, "#{name} must be a positive integer"
  end
end

env_number = fn name, default ->
  case Float.parse(System.get_env(name, default)) do
    {value, ""} when value >= 0 -> value
    _ -> raise ArgumentError, "#{name} must be a non-negative number"
  end
end

env_boolean = fn name, default ->
  case System.get_env(name, default) do
    value when value in ["1", "true", "TRUE"] -> true
    value when value in ["0", "false", "FALSE"] -> false
    _ -> raise ArgumentError, "#{name} must be true/false or 1/0"
  end
end

warmup = env_number.("BENCH_WARMUP", "2")
time = env_number.("BENCH_TIME", "5")
memory_time = env_number.("BENCH_MEMORY_TIME", "1")
seed_entries = env_integer.("BENCH_STREAM_SEED", "3000")
concurrency = env_integer.("BENCH_CONCURRENCY", "64")
concurrent_ops = env_integer.("BENCH_STREAM_CONCURRENT_OPS", "1024")
batch_size = env_integer.("BENCH_STREAM_BATCH", "64")
activity_log_max_len = trunc(env_number.("BENCH_STREAM_ACTIVITY_LOG_MAX_LEN", "512"))
require_catalog_ranges? = env_boolean.("BENCH_REQUIRE_CATALOG_RANGES", "true")
require_linearizable_ids? = env_boolean.("BENCH_REQUIRE_LINEARIZABLE_IDS", "true")
trace_stream? = env_boolean.("BENCH_STREAM_TRACE", "false")
run_id = "#{System.os_time(:microsecond)}_#{System.unique_integer([:positive, :monotonic])}"

data_dir =
  Path.join(
    System.tmp_dir!(),
    "ferricstore_stream_bench_#{run_id}"
  )

File.mkdir_p!(data_dir)
Application.put_env(:ferricstore, :data_dir, data_dir)
Application.put_env(:ferricstore, :native_port, 0)
Application.put_env(:ferricstore, :stream_activity_log_max_len, activity_log_max_len)

{:ok, _started} = Application.ensure_all_started(:ferricstore)
ctx = FerricStore.Instance.get(:default)

cleanup = fn ->
  _ = Application.stop(:ferricstore)
  File.rm_rf!(data_dir)
end

System.at_exit(fn _status -> cleanup.() end)

suffix = System.unique_integer([:positive, :monotonic])
append_key = "bench:stream:append:#{suffix}"
batch_append_key = "bench:stream:batch-append:#{suffix}"
trim_key = "bench:stream:trim:#{suffix}"
range_key = "bench:stream:range:#{suffix}"
group_key = "bench:stream:group:#{suffix}"
concurrent_key = "bench:stream:concurrent:#{suffix}"

for id <- 1..seed_entries do
  id_string = "#{id}-0"
  ^id_string = Stream.handle("XADD", [range_key, id_string, "field", "value"], ctx)
end

for _ <- 1..100 do
  result = Stream.handle("XADD", [trim_key, "MAXLEN", "100", "*", "field", "value"], ctx)
  true = is_binary(result)
end

:ok = Stream.handle("XGROUP", ["CREATE", group_key, "workers", "0", "MKSTREAM"], ctx)

midpoint = max(div(seed_entries, 2), 1)
midpoint_id = "#{midpoint}-0"

if require_catalog_ranges?, do: false = Index.ready?(range_key, ctx)

[[^midpoint_id, "field", "value"] | _] =
  Stream.handle("XRANGE", [range_key, midpoint_id, "+", "COUNT", "10"], ctx)

if require_catalog_ranges?, do: false = Index.ready?(range_key, ctx)
reverse_entries = Stream.handle("XREVRANGE", [range_key, "+", "-", "COUNT", "10"], ctx)
true = length(reverse_entries) == min(seed_entries, 10)
if require_catalog_ranges?, do: false = Index.ready?(range_key, ctx)

IO.puts("=== FerricStore Stream Engine Benchmark ===")
IO.puts("seed_entries=#{seed_entries}")
IO.puts("stream_activity_log_max_len=#{activity_log_max_len}")
IO.puts("data_dir=#{data_dir}")

IO.puts(
  "throughput_units=XADD_MANY ips is batches/s (entries/s = ips * #{batch_size}); " <>
    "range ips is requests/s (returned entries/s = ips * COUNT)"
)

IO.puts(
  "range_catalog_preflight=ok local_stream_index=#{Index.ready?(range_key, ctx)} " <>
    "required_catalog_ranges=#{require_catalog_ranges?}"
)

if trace_stream? do
  trace_key = "bench:stream:trace:#{suffix}"
  trace_items = List.duplicate({trace_key, ["field", "value"]}, batch_size)
  _ = FerricStore.xadd_many(trace_items)
  previous_trace = Ferricstore.LatencyTrace.start(%{})
  trace_started_at = System.monotonic_time(:microsecond)

  try do
    results = FerricStore.xadd_many(trace_items)
    true = length(results) == batch_size
  after
    IO.puts("trace total_request_us=#{System.monotonic_time(:microsecond) - trace_started_at}")
    trace = Ferricstore.LatencyTrace.finish(previous_trace)

    trace
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.each(fn {name, duration_us} ->
      IO.puts("trace #{name}=#{duration_us}")
    end)
  end
end

counter = :counters.new(1, [:atomics])

next_value = fn ->
  :counters.add(counter, 1, 1)
  Integer.to_string(:counters.get(counter, 1))
end

benchmarks = %{
  "XADD append-only" => fn ->
    value = next_value.()
    result = Stream.handle("XADD", [append_key, "*", "field", value], ctx)
    true = is_binary(result)
  end,
  "XADD_MANY append-only x#{batch_size}" => fn ->
    items =
      Enum.map(1..batch_size, fn _item ->
        {batch_append_key, ["field", next_value.()]}
      end)

    results = FerricStore.xadd_many(items)
    true = length(results) == batch_size
    true = Enum.all?(results, &match?({:ok, id} when is_binary(id), &1))
  end,
  "XADD MAXLEN 100 exact" => fn ->
    value = next_value.()

    result =
      Stream.handle("XADD", [trim_key, "MAXLEN", "100", "*", "field", value], ctx)

    true = is_binary(result)
  end,
  "XLEN seeded stream" => fn ->
    ^seed_entries = Stream.handle("XLEN", [range_key], ctx)
  end,
  "XRANGE midpoint COUNT 10" => fn ->
    entries = Stream.handle("XRANGE", [range_key, midpoint_id, "+", "COUNT", "10"], ctx)
    true = length(entries) == min(seed_entries - midpoint + 1, 10)
  end,
  "XRANGE structured AST midpoint COUNT 10" => fn ->
    entries = Stream.handle_ast({:xrange, range_key, {midpoint, 0}, :max, 10}, ctx)
    true = length(entries) == min(seed_entries - midpoint + 1, 10)
  end,
  "XREVRANGE tail COUNT 10" => fn ->
    entries = Stream.handle("XREVRANGE", [range_key, "+", "-", "COUNT", "10"], ctx)
    true = length(entries) == min(seed_entries, 10)
  end,
  "XADD + XREADGROUP + XACK" => fn ->
    value = next_value.()
    id = Stream.handle("XADD", [group_key, "*", "field", value], ctx)
    true = is_binary(id)

    [[^group_key, [[^id, "field", ^value]]]] =
      Stream.handle(
        "XREADGROUP",
        ["GROUP", "workers", "consumer", "COUNT", "1", "STREAMS", group_key, ">"],
        ctx
      )

    1 = Stream.handle("XACK", [group_key, "workers", id], ctx)
  end
}

benchmarks =
  case System.get_env("BENCH_FILTER") do
    nil ->
      benchmarks

    "" ->
      benchmarks

    filter ->
      selected = Map.filter(benchmarks, fn {name, _fun} -> String.contains?(name, filter) end)

      if map_size(selected) == 0 do
        raise ArgumentError, "BENCH_FILTER matched no Stream benchmark: #{inspect(filter)}"
      end

      selected
  end

Benchee.run(
  benchmarks,
  time: time,
  warmup: warmup,
  memory_time: memory_time,
  formatters: [Benchee.Formatters.Console]
)

started_at = System.monotonic_time()

results =
  1..concurrent_ops
  |> Task.async_stream(
    fn value ->
      Stream.handle("XADD", [concurrent_key, "*", "field", Integer.to_string(value)], ctx)
    end,
    max_concurrency: concurrency,
    ordered: false,
    timeout: 60_000
  )
  |> Enum.to_list()

elapsed_native = System.monotonic_time() - started_at
elapsed_seconds = System.convert_time_unit(elapsed_native, :native, :microsecond) / 1_000_000
ids = for {:ok, id} when is_binary(id) <- results, do: id

true = length(ids) == concurrent_ops
unique_ids = MapSet.size(MapSet.new(ids))
physical_entries = Stream.handle("XLEN", [concurrent_key], ctx)

if require_linearizable_ids? do
  true = unique_ids == concurrent_ops
  true = physical_entries == concurrent_ops
end

throughput = concurrent_ops / max(elapsed_seconds, 0.000_001)

IO.puts(
  "same_key_concurrent_xadd ops=#{concurrent_ops} concurrency=#{concurrency} " <>
    "seconds=#{Float.round(elapsed_seconds, 3)} ops_per_second=#{Float.round(throughput, 1)} " <>
    "unique_ids=#{unique_ids} physical_entries=#{physical_entries} " <>
    "required_linearizable_ids=#{require_linearizable_ids?}"
)
