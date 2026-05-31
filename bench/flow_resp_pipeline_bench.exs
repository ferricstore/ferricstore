# bench/flow_resp_pipeline_bench.exs
#
# Redis/RESP pipeline benchmark for native Flow commands, not *_MANY.
#
# Cases:
#   * 100 x FLOW.CREATE in one TCP pipeline
#   * 100 x FLOW.CLAIM_DUE LIMIT 1 in one TCP pipeline
#   * 100 x FLOW.TRANSITION in one TCP pipeline
#
# Run:
#   MIX_ENV=bench mix run --no-start \
#     -e 'Code.require_file("bench/flow_resp_pipeline_bench.exs")'
#
# Options:
#   FLOW_RESP_BACKLOG=100000
#   FLOW_RESP_ITER=100
#   FLOW_RESP_BATCH=100
#   FLOW_RESP_SHARDS=4
#   FLOW_RESP_PARTITIONS=4

backlog = System.get_env("FLOW_RESP_BACKLOG", "100000") |> String.to_integer()
iterations = System.get_env("FLOW_RESP_ITER", "100") |> String.to_integer()
batch_size = System.get_env("FLOW_RESP_BATCH", "100") |> String.to_integer()
shard_count = System.get_env("FLOW_RESP_SHARDS", "4") |> String.to_integer()
partition_count = System.get_env("FLOW_RESP_PARTITIONS", "4") |> String.to_integer()

bench_data_dir =
  Path.join(System.tmp_dir!(), "ferricstore_flow_resp_#{System.unique_integer([:positive])}")

File.mkdir_p!(bench_data_dir)

Application.put_env(:ferricstore, :data_dir, bench_data_dir)
Application.put_env(:ferricstore, :port, 0)
Application.put_env(:ferricstore, :health_port, 0)
Application.put_env(:ferricstore, :shard_count, shard_count)
Application.put_env(:ferricstore, :hot_cache_max_value_size, 512)

{:ok, _} = Application.ensure_all_started(:ferricstore)
{:ok, _} = Application.ensure_all_started(:ferricstore_server)
port = :ranch.get_port(FerricstoreServer.Listener)

defmodule FlowRespPipelineBench do
  def run(port, backlog, iterations, batch_size, partition_count, data_dir) do
    prefix = "flowresp:" <> Integer.to_string(System.unique_integer([:positive]))

    IO.puts("=== FerricStore Flow RESP Pipeline Bench ===")
    IO.puts("data_dir=#{data_dir}")

    IO.puts(
      "mode=#{Ferricstore.ReplicationMode.current()} port=#{port} backlog=#{backlog} iterations=#{iterations} batch=#{batch_size} partitions=#{partition_count}"
    )

    timed("seed active backlog #{backlog}", fn ->
      seed_create_many(
        prefix,
        "active",
        type(prefix, "active"),
        backlog,
        batch_size,
        partition_count
      )
    end)

    timed("seed claim #{iterations * batch_size}", fn ->
      seed_create_many(
        prefix,
        "claim",
        type(prefix, "claim"),
        iterations * batch_size,
        batch_size,
        partition_count
      )
    end)

    timed("seed transition #{iterations * batch_size}", fn ->
      seed_create_many(
        prefix,
        "transition",
        type(prefix, "transition"),
        iterations * batch_size,
        batch_size,
        partition_count
      )
    end)

    create_sock = connect(port)
    claim_sock = connect(port)
    transition_sock = connect(port)

    results = [
      result("resp.pipeline FLOW.CREATE x#{batch_size} under #{backlog}", iterations, fn i ->
        payload =
          1..batch_size
          |> Enum.map(fn j ->
            id = id(prefix, "create", i, j)

            command([
              "FLOW.CREATE",
              id,
              "TYPE",
              type(prefix, "create"),
              "STATE",
              "queued",
              "PAYLOAD",
              "payload:" <> id,
              "RUN_AT",
              "1000",
              "NOW",
              "1000",
              "PARTITION",
              partition(prefix, i + j, partition_count)
            ])
          end)
          |> IO.iodata_to_binary()

        tcp_roundtrip(create_sock, payload, batch_size)
      end),
      result(
        "resp.pipeline FLOW.CLAIM_DUE x#{batch_size} limit=1 under #{backlog}",
        iterations,
        fn i ->
          payload =
            1..batch_size
            |> Enum.map(fn j ->
              command([
                "FLOW.CLAIM_DUE",
                type(prefix, "claim"),
                "STATE",
                "queued",
                "WORKER",
                "worker-resp",
                "LEASE_MS",
                "30000",
                "LIMIT",
                "1",
                "NOW",
                "1000",
                "PARTITION",
                partition(prefix, i + j, partition_count)
              ])
            end)
            |> IO.iodata_to_binary()

          tcp_roundtrip(claim_sock, payload, batch_size)
        end
      ),
      result("resp.pipeline FLOW.TRANSITION x#{batch_size} under #{backlog}", iterations, fn i ->
        payload =
          1..batch_size
          |> Enum.map(fn j ->
            flow_id = id(prefix, "transition", (i - 1) * batch_size + j)

            command([
              "FLOW.TRANSITION",
              flow_id,
              "queued",
              "waiting",
              "FENCING",
              "0",
              "RUN_AT",
              "2000",
              "NOW",
              "2000",
              "PARTITION",
              partition(prefix, (i - 1) * batch_size + j, partition_count)
            ])
          end)
          |> IO.iodata_to_binary()

        tcp_roundtrip(transition_sock, payload, batch_size)
      end)
    ]

    Enum.each([create_sock, claim_sock, transition_sock], &:gen_tcp.close/1)
    print_table(results)
  after
    IO.puts("\nCleaning up bench data directory: #{data_dir}")
    File.rm_rf(data_dir)
  end

  defp seed_create_many(prefix, group, flow_type, total, batch_size, partition_count) do
    total_batches = div(total + batch_size - 1, batch_size)

    for i <- 1..total_batches do
      partition_key = partition(prefix, i, partition_count)
      first = (i - 1) * batch_size + 1
      last = min(i * batch_size, total)

      items =
        for j <- first..last do
          %{id: id(prefix, group, j), payload: "payload:" <> id(prefix, group, j)}
        end

      case FerricStore.flow_create_many(partition_key, items,
             type: flow_type,
             state: "queued",
             run_at_ms: 1_000,
             now_ms: 1_000
           ) do
        :ok -> :ok
        {:ok, _flows} -> :ok
        other -> raise("seed flow_create_many failed: #{inspect(other)}")
      end
    end
  end

  defp connect(port) do
    {:ok, socket} =
      :gen_tcp.connect(~c"127.0.0.1", port, [
        :binary,
        active: false,
        packet: :raw,
        nodelay: true
      ])

    socket
  end

  defp tcp_roundtrip(socket, payload, expected_replies) do
    :ok = :gen_tcp.send(socket, payload)
    read_replies(socket, expected_replies, "", [])
  end

  defp read_replies(_socket, expected, rest, acc) when length(acc) >= expected do
    if rest == "" do
      acc
    else
      raise("unexpected trailing TCP parser buffer: #{inspect(rest)}")
    end
  end

  defp read_replies(socket, expected, buffer, acc) do
    {:ok, data} = :gen_tcp.recv(socket, 0, 30_000)

    case FerricstoreServer.Resp.Parser.parse(buffer <> data) do
      {:ok, replies, rest} ->
        read_replies(socket, expected, rest, acc ++ replies)

      {:error, reason} ->
        raise("RESP parse failed: #{inspect(reason)}")
    end
  end

  defp command(args) do
    [
      "*",
      Integer.to_string(length(args)),
      "\r\n",
      Enum.map(args, fn arg ->
        arg = to_string(arg)
        ["$", Integer.to_string(byte_size(arg)), "\r\n", arg, "\r\n"]
      end)
    ]
  end

  defp result(label, iterations, fun) do
    samples =
      for i <- 1..iterations do
        {us, replies} = :timer.tc(fn -> fun.(i) end)
        true = length(replies) > 0
        us
      end

    sorted = Enum.sort(samples)
    n = length(sorted)
    sum = Enum.sum(sorted)

    %{
      label: label,
      n: n,
      ops_s: round(n / (sum / 1_000_000)),
      cmds_s: round(n * batch_size(label) / (sum / 1_000_000)),
      avg_us: round(sum / n),
      p50_us: percentile(sorted, 0.50),
      p95_us: percentile(sorted, 0.95),
      p99_us: percentile(sorted, 0.99),
      max_us: List.last(sorted)
    }
  end

  defp batch_size(label) do
    [_, size | _] = Regex.run(~r/x(\d+)/, label)
    String.to_integer(size)
  end

  defp print_table(results) do
    IO.puts(
      "| command | pipelines/s | commands/s | avg us | p50 us | p95 us | p99 us | max us | n |"
    )

    IO.puts("|---|---:|---:|---:|---:|---:|---:|---:|---:|")

    Enum.each(results, fn row ->
      IO.puts(
        "| #{row.label} | #{row.ops_s} | #{row.cmds_s} | #{row.avg_us} | #{row.p50_us} | #{row.p95_us} | #{row.p99_us} | #{row.max_us} | #{row.n} |"
      )
    end)
  end

  defp percentile(sorted, pct) do
    idx =
      sorted
      |> length()
      |> Kernel.*(pct)
      |> Float.ceil()
      |> trunc()
      |> max(1)
      |> min(length(sorted))

    Enum.at(sorted, idx - 1)
  end

  defp timed(label, fun) do
    {us, result} = :timer.tc(fun)
    IO.puts("#{label}: #{Float.round(us / 1000, 2)} ms")
    result
  end

  defp id(prefix, group, i), do: "#{prefix}:#{group}:#{i}"
  defp id(prefix, group, i, j), do: "#{prefix}:#{group}:#{i}:#{j}"
  defp type(prefix, group), do: "#{prefix}:#{group}"
  defp partition(_prefix, _i, partition_count) when partition_count <= 1, do: nil
  defp partition(prefix, i, partition_count), do: "#{prefix}:p:#{rem(i - 1, partition_count)}"
end

FlowRespPipelineBench.run(port, backlog, iterations, batch_size, partition_count, bench_data_dir)
