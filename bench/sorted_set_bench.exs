# bench/sorted_set_bench.exs
#
# Current sorted-set baseline.
#
# Run:
#   MIX_ENV=bench mix run --no-start bench/sorted_set_bench.exs
#
# Options:
#   ZSET_BENCH_SIZES=100,1000,5000
#   BENCH_WARMUP=1
#   BENCH_TIME=3
#   BENCH_PARALLEL=1
#   ZSET_BENCH_BATCH_DEPTH=100
#   ZSET_BENCH_TCP=1

bench_warmup = System.get_env("BENCH_WARMUP", "1") |> String.to_integer()
bench_time = System.get_env("BENCH_TIME", "3") |> String.to_integer()
bench_parallel = System.get_env("BENCH_PARALLEL", "1") |> String.to_integer()
batch_depth = System.get_env("ZSET_BENCH_BATCH_DEPTH", "100") |> String.to_integer()
tcp? = System.get_env("ZSET_BENCH_TCP", "0") in ["1", "true", "TRUE", "yes", "YES"]

sizes =
  System.get_env("ZSET_BENCH_SIZES", "100,1000,5000")
  |> String.split(",", trim: true)
  |> Enum.map(&String.to_integer/1)

bench_data_dir =
  Path.join(System.tmp_dir!(), "ferricstore_zset_bench_#{System.unique_integer([:positive])}")

File.mkdir_p!(bench_data_dir)

Application.put_env(:ferricstore, :data_dir, bench_data_dir)
Application.put_env(:ferricstore, :port, 0)
Application.put_env(:ferricstore, :health_port, 0)
Application.put_env(:ferricstore, :shard_count, 1)
Application.put_env(:ferricstore, :hot_cache_max_value_size, 256)

{:ok, _} = Application.ensure_all_started(if(tcp?, do: :ferricstore_server, else: :ferricstore))
tcp_port = if tcp?, do: FerricstoreServer.Listener.port(), else: nil

IO.puts("=== FerricStore Sorted Set Baseline ===")
IO.puts("Data dir: #{bench_data_dir}")
if tcp?, do: IO.puts("TCP port: #{tcp_port}")

IO.puts(
  "sizes=#{Enum.join(sizes, ",")} warmup=#{bench_warmup}s time=#{bench_time}s parallel=#{bench_parallel} batch_depth=#{batch_depth} tcp=#{tcp?}\n"
)

defmodule SortedSetBench do
  def member(i), do: "member:" <> String.pad_leading(Integer.to_string(i), 12, "0")

  def pairs(size) do
    1..size
    |> Enum.map(fn i ->
      # Deterministic shuffled score distribution. Keeps lexical member order
      # different from score order, forcing real score ordering in range paths.
      score = rem(i * 104_729, size * 10) / 10.0
      {score, member(i)}
    end)
  end

  def timed(label, fun) do
    {us, result} = :timer.tc(fun)
    IO.puts("#{label}: #{Float.round(us / 1000, 2)} ms result=#{inspect(short(result))}")
    result
  end

  def short({:ok, list}) when is_list(list), do: {:ok, length(list)}
  def short(other), do: other

  def zscore_batch(key, member, depth) do
    queue =
      for _ <- 1..depth do
        {"ZSCORE", [key, member], {:zscore, key, member}}
      end

    Ferricstore.Transaction.Coordinator.execute(queue, %{}, nil)
  end

  def zrangebyscore_batch(key, min_score, max_score, depth) do
    min = String.to_float(min_score)
    max = String.to_float(max_score)

    queue =
      for _ <- 1..depth do
        {"ZRANGEBYSCORE", [key, min_score, max_score],
         {:zrangebyscore, key, {:inclusive, min}, {:inclusive, max}, []}}
      end

    Ferricstore.Transaction.Coordinator.execute(queue, %{}, nil)
  end

  def zrangebyscore_limit(key) do
    unwrap_range_result(
      Ferricstore.Commands.SortedSet.handle(
        "ZRANGEBYSCORE",
        [key, "-inf", "+inf", "LIMIT", "0", "10"],
        FerricStore.Instance.get(:default)
      )
    )
  end

  def zrevrangebyscore_limit(key) do
    unwrap_range_result(
      Ferricstore.Commands.SortedSet.handle(
        "ZREVRANGEBYSCORE",
        [key, "+inf", "-inf", "LIMIT", "0", "10"],
        FerricStore.Instance.get(:default)
      )
    )
  end

  def unwrap_range_result({:ok, {:ok, result}}), do: result
  def unwrap_range_result({:ok, result}), do: result
  def unwrap_range_result(result) when is_list(result), do: result

  def unwrap_range_result(other) do
    raise("unexpected range result: #{inspect(other)}")
  end

  def zrangebyscore_limit_batch(key, depth) do
    queue =
      for _ <- 1..depth do
        {"ZRANGEBYSCORE", [key, "-inf", "+inf", "LIMIT", "0", "10"],
         {:zrangebyscore, key, :neg_inf, :inf, [{:limit, {0, 10}}]}}
      end

    Ferricstore.Transaction.Coordinator.execute(queue, %{}, nil)
  end

  def tcp_connect(port) do
    {:ok, socket} =
      :gen_tcp.connect(~c"127.0.0.1", port, [
        :binary,
        active: false,
        packet: :raw,
        nodelay: true
      ])

    socket
  end

  def resp_pipeline(command_args, depth) do
    command = resp_command(command_args)
    :binary.copy(command, depth)
  end

  def tcp_roundtrip(socket, payload, expected_replies) do
    :ok = :gen_tcp.send(socket, payload)
    read_replies(socket, expected_replies, "", [])
  end

  defp read_replies(_socket, expected, rest, acc) when length(acc) >= expected do
    if rest == "", do: acc, else: raise("unexpected trailing TCP parser buffer: #{inspect(rest)}")
  end

  defp read_replies(socket, expected, buffer, acc) do
    {:ok, data} = :gen_tcp.recv(socket, 0, 5_000)

    case FerricstoreServer.Resp.Parser.parse(buffer <> data) do
      {:ok, replies, rest} ->
        read_replies(socket, expected, rest, acc ++ replies)

      {:error, reason} ->
        raise("RESP parse failed: #{inspect(reason)}")
    end
  end

  defp resp_command(args) do
    [
      "*",
      Integer.to_string(length(args)),
      "\r\n",
      Enum.map(args, fn arg ->
        arg = to_string(arg)
        ["$", Integer.to_string(byte_size(arg)), "\r\n", arg, "\r\n"]
      end)
    ]
    |> IO.iodata_to_binary()
  end
end

try do
  Enum.each(sizes, fn size ->
    key = "zbench:#{size}"
    mid = div(size, 2)
    low_score = Float.to_string(mid * 1.0)
    high_score = Float.to_string((mid + 10) * 1.0)

    IO.puts("\n--- size=#{size} ---")

    pairs = SortedSetBench.pairs(size)
    SortedSetBench.timed("seed zadd #{size} members", fn -> FerricStore.zadd(key, pairs) end)
    SortedSetBench.timed("verify zcard", fn -> FerricStore.zcard(key) end)

    # Warm reads outside Benchee.
    FerricStore.zscore(key, SortedSetBench.member(mid))
    FerricStore.zrange(key, 0, 9)
    FerricStore.zrangebyscore(key, low_score, high_score)
    SortedSetBench.zrangebyscore_limit(key)
    SortedSetBench.zrevrangebyscore_limit(key)
    FerricStore.zcount(key, low_score, high_score)

    tcp_zscore =
      if tcp? do
        socket = SortedSetBench.tcp_connect(tcp_port)

        payload =
          SortedSetBench.resp_pipeline(["ZSCORE", key, SortedSetBench.member(mid)], batch_depth)

        {socket, payload}
      end

    tcp_zrangebyscore =
      if tcp? do
        socket = SortedSetBench.tcp_connect(tcp_port)

        payload =
          SortedSetBench.resp_pipeline(["ZRANGEBYSCORE", key, low_score, high_score], batch_depth)

        {socket, payload}
      end

    tcp_zrangebyscore_limit =
      if tcp? do
        socket = SortedSetBench.tcp_connect(tcp_port)

        payload =
          SortedSetBench.resp_pipeline(
            ["ZRANGEBYSCORE", key, "-inf", "+inf", "LIMIT", "0", "10"],
            batch_depth
          )

        {socket, payload}
      end

    base_jobs = %{
      "zscore hit size=#{size}" => fn ->
        {:ok, _} = FerricStore.zscore(key, SortedSetBench.member(mid))
      end,
      "zcard size=#{size}" => fn ->
        {:ok, ^size} = FerricStore.zcard(key)
      end,
      "zrange first10 size=#{size}" => fn ->
        {:ok, result} = FerricStore.zrange(key, 0, 9)
        true = length(result) <= 10
      end,
      "zrangebyscore 10ish size=#{size}" => fn ->
        {:ok, result} = FerricStore.zrangebyscore(key, low_score, high_score)
        true = is_list(result)
      end,
      "zrangebyscore full-limit10 size=#{size}" => fn ->
        result = SortedSetBench.zrangebyscore_limit(key)
        true = length(result) == 10
      end,
      "zrevrangebyscore full-limit10 size=#{size}" => fn ->
        result = SortedSetBench.zrevrangebyscore_limit(key)
        true = length(result) == 10
      end,
      "zcount 10ish size=#{size}" => fn ->
        {:ok, count} = FerricStore.zcount(key, low_score, high_score)
        true = is_integer(count)
      end,
      "batch#{batch_depth} zscore size=#{size}" => fn ->
        results = SortedSetBench.zscore_batch(key, SortedSetBench.member(mid), batch_depth)
        true = length(results) == batch_depth
      end,
      "batch#{batch_depth} zrangebyscore size=#{size}" => fn ->
        results = SortedSetBench.zrangebyscore_batch(key, low_score, high_score, batch_depth)
        true = length(results) == batch_depth
      end,
      "batch#{batch_depth} zrangebyscore full-limit10 size=#{size}" => fn ->
        results = SortedSetBench.zrangebyscore_limit_batch(key, batch_depth)
        true = length(results) == batch_depth
      end
    }

    tcp_jobs =
      if tcp? do
        %{
          "tcp-pipeline#{batch_depth} zscore size=#{size}" => fn ->
            {socket, payload} = tcp_zscore
            results = SortedSetBench.tcp_roundtrip(socket, payload, batch_depth)
            true = length(results) == batch_depth
          end,
          "tcp-pipeline#{batch_depth} zrangebyscore size=#{size}" => fn ->
            {socket, payload} = tcp_zrangebyscore
            results = SortedSetBench.tcp_roundtrip(socket, payload, batch_depth)
            true = length(results) == batch_depth
          end,
          "tcp-pipeline#{batch_depth} zrangebyscore full-limit10 size=#{size}" => fn ->
            {socket, payload} = tcp_zrangebyscore_limit
            results = SortedSetBench.tcp_roundtrip(socket, payload, batch_depth)
            true = length(results) == batch_depth
          end
        }
      else
        %{}
      end

    Benchee.run(
      Map.merge(base_jobs, tcp_jobs),
      warmup: bench_warmup,
      time: bench_time,
      parallel: bench_parallel,
      memory_time: 0,
      formatters: [Benchee.Formatters.Console]
    )

    if tcp_zscore, do: :gen_tcp.close(elem(tcp_zscore, 0))
    if tcp_zrangebyscore, do: :gen_tcp.close(elem(tcp_zrangebyscore, 0))
    if tcp_zrangebyscore_limit, do: :gen_tcp.close(elem(tcp_zrangebyscore_limit, 0))
  end)
after
  IO.puts("\nCleaning up bench data directory: #{bench_data_dir}")
  File.rm_rf!(bench_data_dir)
end
