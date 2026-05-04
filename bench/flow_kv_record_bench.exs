# bench/flow_kv_record_bench.exs
#
# Flow KV/encoded-record benchmark.
#
# Measures the current record strategy for Flow: an encoded latest-state blob
# stored at one KV key. Covers hot reads, cold reads, batch reads, and durable
# updates for small and larger records.
#
# Run:
#   MIX_ENV=bench FERRICSTORE_BUILD=1 mix run --no-start bench/flow_kv_record_bench.exs
#
# Options:
#   FLOW_KV_SIZES=128,512,4096,65536
#   FLOW_KV_KEYS=1000
#   BENCH_WARMUP=1
#   BENCH_TIME=3
#   BENCH_PARALLEL=1
#   FLOW_KV_HOT_CACHE_MAX=512

bench_warmup = System.get_env("BENCH_WARMUP", "1") |> String.to_integer()
bench_time = System.get_env("BENCH_TIME", "3") |> String.to_integer()
bench_parallel = System.get_env("BENCH_PARALLEL", "1") |> String.to_integer()
key_count = System.get_env("FLOW_KV_KEYS", "1000") |> String.to_integer()
hot_cache_max = System.get_env("FLOW_KV_HOT_CACHE_MAX", "512") |> String.to_integer()

sizes =
  System.get_env("FLOW_KV_SIZES", "128,512,4096,65536")
  |> String.split(",", trim: true)
  |> Enum.map(&String.to_integer/1)

bench_data_dir =
  Path.join(System.tmp_dir!(), "ferricstore_flow_kv_bench_#{System.unique_integer([:positive])}")

File.mkdir_p!(bench_data_dir)

Application.put_env(:ferricstore, :data_dir, bench_data_dir)
Application.put_env(:ferricstore, :port, 0)
Application.put_env(:ferricstore, :health_port, 0)
Application.put_env(:ferricstore, :shard_count, 1)
Application.put_env(:ferricstore, :hot_cache_max_value_size, hot_cache_max)

{:ok, _} = Application.ensure_all_started(:ferricstore)

IO.puts("=== FerricStore Flow KV Record Bench ===")
IO.puts("Data dir: #{bench_data_dir}")

IO.puts(
  "sizes=#{Enum.join(sizes, ",")} keys=#{key_count} hot_cache_max=#{hot_cache_max} warmup=#{bench_warmup}s time=#{bench_time}s parallel=#{bench_parallel}\n"
)

defmodule FlowKVRecordBench do
  def key(prefix, i), do: "flow:{" <> prefix <> ":" <> Integer.to_string(i) <> "}:state"

  def record(id, target_size, version \\ 0) do
    base = %{
      id: id,
      state: "running",
      version: version,
      attempts: rem(version, 7),
      next_run_at_ms: 1_000 + version,
      lease_owner: "worker:" <> Integer.to_string(rem(version, 32)),
      payload_ref: "payload:" <> id,
      result_ref: nil
    }

    encoded = :erlang.term_to_binary(base)
    pad_size = max(target_size - byte_size(encoded), 0)
    :erlang.term_to_binary(Map.put(base, :pad, :binary.copy("x", pad_size)))
  end

  def decode_record(value) when is_binary(value), do: :erlang.binary_to_term(value)

  def seed(prefix, count, size) do
    Enum.each(1..count, fn i ->
      :ok = FerricStore.set(key(prefix, i), record("#{prefix}:#{i}", size))
    end)
  end

  def hot_read(prefix, i) do
    {:ok, value} = FerricStore.get(key(prefix, i))
    value
  end

  def decode_read(prefix, i) do
    prefix
    |> hot_read(i)
    |> decode_record()
  end

  def batch_read(prefix, indexes) do
    indexes
    |> Enum.map(&key(prefix, &1))
    |> FerricStore.batch_get()
  end

  def update_record(prefix, i, size, version) do
    :ok = FerricStore.set(key(prefix, i), record("#{prefix}:#{i}", size, version))
  end

  def batch_update(prefix, start_i, count, size, version) do
    kvs =
      Enum.map(0..(count - 1), fn offset ->
        i = start_i + offset
        {key(prefix, i), record("#{prefix}:#{i}", size, version + offset)}
      end)

    FerricStore.batch_set(kvs)
  end

  def next_i(counter, max_i) do
    :counters.add(counter, 1, 1)
    rem(:counters.get(counter, 1), max_i) + 1
  end

  def timed(label, fun) do
    {us, result} = :timer.tc(fun)
    IO.puts("#{label}: #{Float.round(us / 1000, 2)} ms result=#{inspect(short(result))}")
    result
  end

  def short({:ok, list}) when is_list(list), do: {:ok, length(list)}
  def short(list) when is_list(list), do: length(list)
  def short(value) when is_binary(value), do: {:binary, byte_size(value)}
  def short(other), do: other
end

try do
  Enum.each(sizes, fn size ->
    prefix = "kv#{size}:#{System.unique_integer([:positive])}"
    counter = :counters.new(1, [:atomics])
    batch_indexes = Enum.to_list(1..min(key_count, 100))

    IO.puts("\n--- record_size_target=#{size} ---")

    FlowKVRecordBench.timed("seed #{key_count} records size=#{size}", fn ->
      FlowKVRecordBench.seed(prefix, key_count, size)
    end)

    FlowKVRecordBench.hot_read(prefix, 1)
    FlowKVRecordBench.decode_read(prefix, 1)
    FlowKVRecordBench.batch_read(prefix, batch_indexes)

    Benchee.run(
      %{
        "flow kv get hot size=#{size}" => fn ->
          i = FlowKVRecordBench.next_i(counter, key_count)
          value = FlowKVRecordBench.hot_read(prefix, i)
          true = is_binary(value)
        end,
        "flow kv get+decode size=#{size}" => fn ->
          i = FlowKVRecordBench.next_i(counter, key_count)
          record = FlowKVRecordBench.decode_read(prefix, i)
          true = is_map(record)
        end,
        "flow kv batch_get100 size=#{size}" => fn ->
          values = FlowKVRecordBench.batch_read(prefix, batch_indexes)
          true = length(values) == length(batch_indexes)
        end,
        "flow kv set update size=#{size}" => fn ->
          i = FlowKVRecordBench.next_i(counter, key_count)
          FlowKVRecordBench.update_record(prefix, i, size, i)
        end,
        "flow kv batch_set10 size=#{size}" => fn ->
          i = FlowKVRecordBench.next_i(counter, key_count - 10)
          results = FlowKVRecordBench.batch_update(prefix, i, 10, size, i)
          true = length(results) == 10
        end,
        "flow kv batch_set100 size=#{size}" => fn ->
          i = FlowKVRecordBench.next_i(counter, key_count - 100)
          results = FlowKVRecordBench.batch_update(prefix, i, 100, size, i)
          true = length(results) == 100
        end
      },
      warmup: bench_warmup,
      time: bench_time,
      parallel: bench_parallel,
      memory_time: 0,
      formatters: [Benchee.Formatters.Console]
    )
  end)
after
  IO.puts("\nCleaning up bench data directory: #{bench_data_dir}")
  File.rm_rf!(bench_data_dir)
end
