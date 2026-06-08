defmodule FerricstoreServer.Bench.TcpCallTraceTest do
  use ExUnit.Case, async: false
  @moduletag :bench
  @moduletag timeout: 120_000

  test "call count trace: what functions are called per TCP GET" do
    port = FerricstoreServer.Listener.port()
    prefix = "cct_#{System.unique_integer([:positive])}"

    for i <- 1..1000 do
      FerricStore.set("#{prefix}:#{i}", String.duplicate("v", 100))
    end

    Process.sleep(300)

    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", port, [
        :binary,
        active: false,
        packet: :raw,
        buffer: 1_048_576
      ])

    :gen_tcp.send(sock, "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
    {:ok, _} = :gen_tcp.recv(sock, 0, 5000)

    # Warm up
    for _ <- 1..500 do
      key = "#{prefix}:#{:rand.uniform(1000)}"
      cmd = "*2\r\n$3\r\nGET\r\n$#{byte_size(key)}\r\n#{key}\r\n"
      :gen_tcp.send(sock, cmd)
      {:ok, _} = :gen_tcp.recv(sock, 0, 5000)
    end

    # Enable call count tracing on key modules
    modules_to_trace = [
      {FerricstoreServer.Resp.Parser, "RESP3 parse"},
      {FerricstoreServer.Resp.Encoder, "RESP3 encode"},
      {Ferricstore.Store.Router, "Router"},
      {Ferricstore.Store.LFU, "LFU"},
      {Ferricstore.Commands.Dispatcher, "Dispatcher"},
      {Ferricstore.Commands.Strings, "Strings cmd"},
      {Ferricstore.Commands.Generic, "Generic cmd"},
      {FerricstoreServer.Connection, "Connection"},
      {:ets, "ETS"},
      {:erlang, "erlang BIFs"},
      {:prim_inet, "TCP socket"}
    ]

    # Use :cprof for call counting
    :cprof.start()

    # Do exactly 1000 GETs
    for _ <- 1..1000 do
      key = "#{prefix}:#{:rand.uniform(1000)}"
      cmd = "*2\r\n$3\r\nGET\r\n$#{byte_size(key)}\r\n#{key}\r\n"
      :gen_tcp.send(sock, cmd)
      {:ok, _} = :gen_tcp.recv(sock, 0, 5000)
    end

    :cprof.pause()
    :gen_tcp.close(sock)

    IO.puts("\n=== Call Count Profile: 1000 sequential TCP GETs ===\n")

    # Analyze per module
    for {mod, label} <- modules_to_trace do
      {_mod, total_calls, func_list} = :cprof.analyse(mod)

      if total_calls > 0 do
        IO.puts("  #{label} (#{inspect(mod)}): #{total_calls} total calls")

        func_list
        |> Enum.sort_by(fn {_mfa, count} -> -count end)
        |> Enum.take(10)
        |> Enum.each(fn {{_m, f, a}, count} ->
          per_get = Float.round(count / 1000, 1)
          IO.puts("    #{f}/#{a}: #{count} calls (#{per_get} per GET)")
        end)

        IO.puts("")
      end
    end

    # Overall top functions
    {_total, top_modules} = :cprof.analyse()
    IO.puts("  === Top modules by call count ===\n")

    top_modules
    |> Enum.sort_by(fn {_mod, count, _} -> -count end)
    |> Enum.take(20)
    |> Enum.each(fn {mod, count, _funcs} ->
      per_get = Float.round(count / 1000, 1)

      IO.puts(
        "    #{String.pad_trailing(inspect(mod), 50)} #{String.pad_leading("#{count}", 10)} (#{per_get}/GET)"
      )
    end)

    :cprof.stop()
  end

  test "timed trace: measure actual time in each layer for 1 TCP GET" do
    port = FerricstoreServer.Listener.port()
    prefix = "tt_#{System.unique_integer([:positive])}"

    for i <- 1..1000 do
      FerricStore.set("#{prefix}:#{i}", String.duplicate("v", 100))
    end

    Process.sleep(300)

    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", port, [
        :binary,
        active: false,
        packet: :raw
      ])

    :gen_tcp.send(sock, "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
    {:ok, _} = :gen_tcp.recv(sock, 0, 5000)

    # Warm up
    for _ <- 1..200 do
      key = "#{prefix}:#{:rand.uniform(1000)}"
      cmd = "*2\r\n$3\r\nGET\r\n$#{byte_size(key)}\r\n#{key}\r\n"
      :gen_tcp.send(sock, cmd)
      {:ok, _} = :gen_tcp.recv(sock, 0, 5000)
    end

    IO.puts("\n=== Component timing: each layer measured independently (10000 iterations) ===\n")

    key = "#{prefix}:500"
    resp3_cmd = "*2\r\n$3\r\nGET\r\n$#{byte_size(key)}\r\n#{key}\r\n"

    # 1. RESP3 parse
    {parse_us, _} =
      :timer.tc(fn ->
        for _ <- 1..10_000 do
          FerricstoreServer.Resp.Parser.parse(resp3_cmd)
        end
      end)

    # 2. Command dispatch (String.upcase + lookup in dispatch map)
    {dispatch_us, _} =
      :timer.tc(fn ->
        for _ <- 1..10_000 do
          String.upcase("get")
        end
      end)

    # 3. Router.get (shard_for + ETS lookup + LFU touch)
    {router_us, _} =
      :timer.tc(fn ->
        for _ <- 1..10_000 do
          Ferricstore.Store.Router.get(FerricStore.Instance.get(:default), key)
        end
      end)

    # 4. RESP3 encode
    value = String.duplicate("v", 100)

    {encode_us, _} =
      :timer.tc(fn ->
        for _ <- 1..10_000 do
          FerricstoreServer.Resp.Encoder.encode(value)
        end
      end)

    # 5. Full TCP round-trip
    {tcp_us, _} =
      :timer.tc(fn ->
        for _ <- 1..10_000 do
          :gen_tcp.send(sock, resp3_cmd)
          {:ok, _} = :gen_tcp.recv(sock, 0, 5000)
        end
      end)

    :gen_tcp.close(sock)

    # 6. Embedded FerricStore.get
    {embedded_us, _} =
      :timer.tc(fn ->
        for _ <- 1..10_000 do
          FerricStore.get(key)
        end
      end)

    parse_ns = div(parse_us * 1000, 10_000)
    dispatch_ns = div(dispatch_us * 1000, 10_000)
    router_ns = div(router_us * 1000, 10_000)
    encode_ns = div(encode_us * 1000, 10_000)
    tcp_ns = div(tcp_us * 1000, 10_000)
    embedded_ns = div(embedded_us * 1000, 10_000)

    server_total = parse_ns + dispatch_ns + router_ns + encode_ns
    tcp_overhead = tcp_ns - server_total

    IO.puts(
      "  #{String.pad_trailing("Layer", 35)} #{String.pad_leading("Time/op", 10)} #{String.pad_leading("% of TCP", 10)}"
    )

    IO.puts("  #{String.duplicate("-", 60)}")

    layers = [
      {"RESP3 parse", parse_ns},
      {"Command dispatch (upcase)", dispatch_ns},
      {"Router.get (hash+ETS+LFU)", router_ns},
      {"RESP3 encode", encode_ns},
      {"--- Server subtotal ---", server_total},
      {"TCP+kernel overhead", tcp_overhead},
      {"=== Full TCP GET ===", tcp_ns},
      {"Embedded FerricStore.get", embedded_ns}
    ]

    for {label, ns} <- layers do
      pct = if tcp_ns > 0, do: Float.round(ns / tcp_ns * 100, 1), else: 0.0

      IO.puts(
        "  #{String.pad_trailing(label, 35)} #{String.pad_leading("#{ns}ns", 10)} #{String.pad_leading("#{pct}%", 10)}"
      )
    end
  end
end
