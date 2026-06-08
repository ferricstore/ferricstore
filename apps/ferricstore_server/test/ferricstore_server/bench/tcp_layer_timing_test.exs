defmodule FerricstoreServer.Bench.TcpLayerTimingTest do
  use ExUnit.Case, async: false
  @moduletag :bench
  @moduletag timeout: 120_000

  test "component timing: each layer measured independently (10000 iterations)" do
    port = FerricstoreServer.Listener.port()
    prefix = "tlt_#{System.unique_integer([:positive])}"

    for i <- 1..1000 do
      FerricStore.set("#{prefix}:#{i}", String.duplicate("v", 100))
    end

    Process.sleep(300)

    key = "#{prefix}:500"
    resp3_cmd = "*2\r\n$3\r\nGET\r\n$#{byte_size(key)}\r\n#{key}\r\n"
    n = 10_000

    IO.puts("\n=== Component Timing: each layer measured independently (#{n} iterations) ===\n")

    # 1. RESP3 parse
    {parse_us, _} =
      :timer.tc(fn ->
        for _ <- 1..n, do: FerricstoreServer.Resp.Parser.parse(resp3_cmd)
      end)

    # 2. Command dispatch (String.upcase + lookup)
    {dispatch_us, _} =
      :timer.tc(fn ->
        for _ <- 1..n, do: String.upcase("get")
      end)

    # 3. Router.get (shard_for + ETS lookup)
    {router_us, _} =
      :timer.tc(fn ->
        for _ <- 1..n, do: Ferricstore.Store.Router.get(FerricStore.Instance.get(:default), key)
      end)

    # 4. RESP3 encode
    value = String.duplicate("v", 100)

    {encode_us, _} =
      :timer.tc(fn ->
        for _ <- 1..n, do: FerricstoreServer.Resp.Encoder.encode(value)
      end)

    # 5. Full TCP round-trip (1 GET per round-trip)
    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw, buffer: 65_536])

    :gen_tcp.send(sock, "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
    {:ok, _} = :gen_tcp.recv(sock, 0, 5000)

    {tcp_us, _} =
      :timer.tc(fn ->
        for _ <- 1..n do
          :gen_tcp.send(sock, resp3_cmd)
          {:ok, _} = :gen_tcp.recv(sock, 0, 5000)
        end
      end)

    :gen_tcp.close(sock)

    # 6. Full TCP pipelined (50 GETs per round-trip)
    pipeline = 50

    batch =
      for _ <- 1..pipeline do
        k = "#{prefix}:#{:rand.uniform(1000)}"
        "*2\r\n$3\r\nGET\r\n$#{byte_size(k)}\r\n#{k}\r\n"
      end
      |> IO.iodata_to_binary()

    resp_size = pipeline * 108

    {:ok, sock2} =
      :gen_tcp.connect(~c"127.0.0.1", port, [
        :binary,
        active: false,
        packet: :raw,
        buffer: 1_048_576
      ])

    :gen_tcp.send(sock2, "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
    {:ok, _} = :gen_tcp.recv(sock2, 0, 5000)

    pipeline_iters = div(n, pipeline)

    {tcp_pipe_us, _} =
      :timer.tc(fn ->
        for _ <- 1..pipeline_iters do
          :gen_tcp.send(sock2, batch)
          drain_bytes(sock2, resp_size)
        end
      end)

    :gen_tcp.close(sock2)

    parse_ns = div(parse_us * 1000, n)
    dispatch_ns = div(dispatch_us * 1000, n)
    router_ns = div(router_us * 1000, n)
    encode_ns = div(encode_us * 1000, n)
    tcp_ns = div(tcp_us * 1000, n)
    tcp_pipe_ns = div(tcp_pipe_us * 1000, n)

    server_total = parse_ns + dispatch_ns + router_ns + encode_ns
    tcp_overhead = tcp_ns - server_total

    IO.puts(
      "  #{String.pad_trailing("Layer", 40)} #{String.pad_leading("Time/op", 10)} #{String.pad_leading("% of TCP", 10)}"
    )

    IO.puts("  #{String.duplicate("-", 65)}")

    layers = [
      {"RESP3 parse", parse_ns},
      {"Command dispatch (upcase)", dispatch_ns},
      {"Router.get (hash+ETS)", router_ns},
      {"RESP3 encode", encode_ns},
      {"--- Server subtotal ---", server_total},
      {"TCP+kernel overhead", tcp_overhead},
      {"=== Full TCP GET (sequential) ===", tcp_ns},
      {"=== Full TCP GET (pipeline 50) ===", tcp_pipe_ns}
    ]

    for {label, ns} <- layers do
      pct = if tcp_ns > 0, do: Float.round(ns / tcp_ns * 100, 1), else: 0.0

      IO.puts(
        "  #{String.pad_trailing(label, 40)} #{String.pad_leading("#{ns}ns", 10)} #{String.pad_leading("#{pct}%", 10)}"
      )
    end
  end

  defp drain_bytes(_sock, remaining) when remaining <= 0, do: :ok

  defp drain_bytes(sock, remaining) do
    {:ok, data} = :gen_tcp.recv(sock, 0, 10_000)
    drain_bytes(sock, remaining - byte_size(data))
  end
end
