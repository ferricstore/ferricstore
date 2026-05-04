# bench/stream_bench.exs
#
# Stream baseline/optimization bench.
#
# Run:
#   MIX_ENV=bench FERRICSTORE_BUILD=1 mix run --no-start bench/stream_bench.exs
#
# Options:
#   STREAM_BENCH_SIZES=100,1000,5000
#   BENCH_WARMUP=1
#   BENCH_TIME=3
#   BENCH_PARALLEL=1
#   STREAM_BENCH_BATCH_DEPTH=100

bench_warmup = System.get_env("BENCH_WARMUP", "1") |> String.to_integer()
bench_time = System.get_env("BENCH_TIME", "3") |> String.to_integer()
bench_parallel = System.get_env("BENCH_PARALLEL", "1") |> String.to_integer()
batch_depth = System.get_env("STREAM_BENCH_BATCH_DEPTH", "100") |> String.to_integer()

sizes =
  System.get_env("STREAM_BENCH_SIZES", "100,1000,5000")
  |> String.split(",", trim: true)
  |> Enum.map(&String.to_integer/1)

bench_data_dir =
  Path.join(System.tmp_dir!(), "ferricstore_stream_bench_#{System.unique_integer([:positive])}")

File.mkdir_p!(bench_data_dir)

Application.put_env(:ferricstore, :data_dir, bench_data_dir)
Application.put_env(:ferricstore, :port, 0)
Application.put_env(:ferricstore, :health_port, 0)
Application.put_env(:ferricstore, :shard_count, 1)
Application.put_env(:ferricstore, :hot_cache_max_value_size, 256)

{:ok, _} = Application.ensure_all_started(:ferricstore)

IO.puts("=== FerricStore Stream Baseline ===")
IO.puts("Data dir: #{bench_data_dir}")

IO.puts(
  "sizes=#{Enum.join(sizes, ",")} warmup=#{bench_warmup}s time=#{bench_time}s parallel=#{bench_parallel} batch_depth=#{batch_depth}\n"
)

defmodule StreamBench do
  def fields(i), do: ["workflow", "wf:" <> Integer.to_string(i), "state", "queued"]

  def xadd_explicit(key, i) do
    Ferricstore.Commands.Stream.handle("XADD", [key, "#{i}-0" | fields(i)], store())
  end

  def xadd_batch(key, depth, start) do
    queue =
      for i <- start..(start + depth - 1) do
        {"XADD", [key, "#{i}-0" | fields(i)],
         {:xadd, key, {{:explicit, i, 0}, fields(i), nil, false}}}
      end

    Ferricstore.Transaction.Coordinator.execute(queue, %{}, nil)
  end

  def xrange_batch(key, depth) do
    queue =
      for _ <- 1..depth do
        {"XRANGE", [key, "-", "+", "COUNT", "10"], {:xrange, key, :min, :max, 10}}
      end

    Ferricstore.Transaction.Coordinator.execute(queue, %{}, nil)
  end

  def xrevrange_batch(key, depth) do
    queue =
      for _ <- 1..depth do
        {"XREVRANGE", [key, "+", "-", "COUNT", "10"], {:xrevrange, key, :min, :max, 10}}
      end

    Ferricstore.Transaction.Coordinator.execute(queue, %{}, nil)
  end

  def xread_batch(key, depth) do
    queue =
      for _ <- 1..depth do
        {"XREAD", ["COUNT", "10", "STREAMS", key, "0"], {:xread, 10, :no_block, [{key, "0"}]}}
      end

    Ferricstore.Transaction.Coordinator.execute(queue, %{}, nil)
  end

  def store do
    FerricStore.Instance.get(:default)
  end

  def timed(label, fun) do
    {us, result} = :timer.tc(fun)
    IO.puts("#{label}: #{Float.round(us / 1000, 2)} ms result=#{inspect(short(result))}")
    result
  end

  def short({:ok, list}) when is_list(list), do: {:ok, length(list)}
  def short(list) when is_list(list), do: length(list)
  def short(other), do: other
end

try do
  Enum.each(sizes, fn size ->
    key = "streambench:#{size}:#{System.unique_integer([:positive])}"

    IO.puts("\n--- size=#{size} ---")

    StreamBench.timed("seed xadd #{size} entries", fn ->
      Enum.each(1..size, fn i -> StreamBench.xadd_explicit(key, i) end)
    end)

    StreamBench.timed("verify xlen", fn -> FerricStore.xlen(key) end)

    FerricStore.xlen(key)
    FerricStore.xrange(key, "-", "+", count: 10)
    FerricStore.xrevrange(key, "+", "-", count: 10)

    Ferricstore.Commands.Stream.handle_ast(
      {:xread, 10, :no_block, [{key, "0"}]},
      StreamBench.store()
    )

    Benchee.run(
      %{
        "xadd explicit size=#{size}" => fn ->
          id = System.unique_integer([:positive])
          result = StreamBench.xadd_explicit("#{key}:write", id)
          true = is_binary(result)
        end,
        "xlen size=#{size}" => fn ->
          {:ok, ^size} = FerricStore.xlen(key)
        end,
        "xrange first10 size=#{size}" => fn ->
          {:ok, result} = FerricStore.xrange(key, "-", "+", count: 10)
          true = length(result) == min(size, 10)
        end,
        "xrevrange last10 size=#{size}" => fn ->
          {:ok, result} = FerricStore.xrevrange(key, "+", "-", count: 10)
          true = length(result) == min(size, 10)
        end,
        "xread first10 size=#{size}" => fn ->
          result =
            Ferricstore.Commands.Stream.handle_ast(
              {:xread, 10, :no_block, [{key, "0"}]},
              StreamBench.store()
            )

          [[^key, entries]] = result
          true = length(entries) == min(size, 10)
        end,
        "batch#{batch_depth} xrange first10 size=#{size}" => fn ->
          results = StreamBench.xrange_batch(key, batch_depth)
          true = length(results) == batch_depth
        end,
        "batch#{batch_depth} xrevrange last10 size=#{size}" => fn ->
          results = StreamBench.xrevrange_batch(key, batch_depth)
          true = length(results) == batch_depth
        end,
        "batch#{batch_depth} xread first10 size=#{size}" => fn ->
          results = StreamBench.xread_batch(key, batch_depth)
          true = length(results) == batch_depth
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
