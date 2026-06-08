defmodule FerricstoreServer.Bench.TcpDetailedProfileTest do
  use ExUnit.Case, async: false
  @moduletag :bench
  @moduletag timeout: 120_000

  @doc """
  Profiles the ENTIRE TCP read path with timing at every step.

  Server-side timing is done by temporarily instrumenting the connection
  process via process dictionary timestamps. The connection dispatches GET
  through several layers — we measure each one.
  """
  test "detailed TCP GET profiling - every layer timed" do
    port = FerricstoreServer.Listener.port()
    prefix = "dtp_#{System.unique_integer([:positive])}"

    # Pre-populate
    for i <- 1..1000 do
      FerricStore.set("#{prefix}:#{i}", String.duplicate("v", 100))
    end

    Process.sleep(300)

    # ---- SERVER-SIDE PROFILING ----
    # We can't easily instrument inside the connection process from outside.
    # Instead, we profile the individual components independently:

    IO.puts("\n=== Detailed TCP Read Path Profile ===\n")

    # 1. Measure RESP3 parse time
    sample_cmd = "*2\r\n$3\r\nGET\r\n$#{byte_size("#{prefix}:500")}\r\n#{prefix}:500\r\n"

    parse_times =
      for _ <- 1..10_000 do
        t0 = :erlang.monotonic_time(:nanosecond)
        {:ok, _cmds, _rest} = FerricstoreServer.Resp.Parser.parse(sample_cmd)
        t1 = :erlang.monotonic_time(:nanosecond)
        t1 - t0
      end

    # 2. Measure command dispatch (Router.get — the ETS lookup path)
    key = "#{prefix}:500"

    dispatch_times =
      for _ <- 1..10_000 do
        t0 = :erlang.monotonic_time(:nanosecond)
        _val = Ferricstore.Store.Router.get(FerricStore.Instance.get(:default), key)
        t1 = :erlang.monotonic_time(:nanosecond)
        t1 - t0
      end

    # 3. Measure RESP3 encode time
    sample_value = String.duplicate("v", 100)

    encode_times =
      for _ <- 1..10_000 do
        t0 = :erlang.monotonic_time(:nanosecond)
        _encoded = FerricstoreServer.Resp.Encoder.encode(sample_value)
        t1 = :erlang.monotonic_time(:nanosecond)
        t1 - t0
      end

    # 4. Measure full TCP round-trip
    {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])
    :gen_tcp.send(sock, "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
    {:ok, _} = :gen_tcp.recv(sock, 0, 5000)

    # Warm up
    for _ <- 1..200 do
      k = "#{prefix}:#{:rand.uniform(1000)}"
      cmd = "*2\r\n$3\r\nGET\r\n$#{byte_size(k)}\r\n#{k}\r\n"
      :gen_tcp.send(sock, cmd)
      {:ok, _} = :gen_tcp.recv(sock, 0, 5000)
    end

    tcp_times =
      for _ <- 1..10_000 do
        k = "#{prefix}:#{:rand.uniform(1000)}"
        cmd = "*2\r\n$3\r\nGET\r\n$#{byte_size(k)}\r\n#{k}\r\n"
        t0 = :erlang.monotonic_time(:nanosecond)
        :gen_tcp.send(sock, cmd)
        {:ok, _} = :gen_tcp.recv(sock, 0, 5000)
        t1 = :erlang.monotonic_time(:nanosecond)
        t1 - t0
      end

    :gen_tcp.close(sock)

    # 5. Measure embedded API (FerricStore.get) for comparison
    embedded_times =
      for _ <- 1..10_000 do
        k = "#{prefix}:#{:rand.uniform(1000)}"
        t0 = :erlang.monotonic_time(:nanosecond)
        {:ok, _} = FerricStore.get(k)
        t1 = :erlang.monotonic_time(:nanosecond)
        t1 - t0
      end

    # 6. Measure TCP send/recv kernel overhead (PING — minimal server work)
    {:ok, sock2} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])
    :gen_tcp.send(sock2, "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
    {:ok, _} = :gen_tcp.recv(sock2, 0, 5000)

    ping_times =
      for _ <- 1..10_000 do
        t0 = :erlang.monotonic_time(:nanosecond)
        :gen_tcp.send(sock2, "*1\r\n$4\r\nPING\r\n")
        {:ok, _} = :gen_tcp.recv(sock2, 0, 5000)
        t1 = :erlang.monotonic_time(:nanosecond)
        t1 - t0
      end

    :gen_tcp.close(sock2)

    # 7. Measure setopts overhead
    {:ok, sock3} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])

    setopts_times =
      for _ <- 1..10_000 do
        t0 = :erlang.monotonic_time(:nanosecond)
        :inet.setopts(sock3, active: :once)
        t1 = :erlang.monotonic_time(:nanosecond)
        :inet.setopts(sock3, active: false)
        t1 - t0
      end

    :gen_tcp.close(sock3)

    # Print results
    IO.puts(
      "  #{String.pad_trailing("Component", 35)} #{String.pad_leading("avg", 8)} #{String.pad_leading("p50", 8)} #{String.pad_leading("p99", 8)} #{String.pad_leading("min", 8)} #{String.pad_leading("max", 8)}"
    )

    IO.puts("  #{String.duplicate("-", 85)}")

    tcp_avg = div(Enum.sum(tcp_times), 10_000)

    measurements = [
      {"RESP3 parse", parse_times},
      {"Router.get (ETS + LFU)", dispatch_times},
      {"RESP3 encode", encode_times},
      {"Embedded FerricStore.get", embedded_times},
      {"TCP PING round-trip (baseline)", ping_times},
      {"TCP GET round-trip (full)", tcp_times},
      {"inet:setopts active:once", setopts_times}
    ]

    for {label, times} <- measurements do
      sorted = Enum.sort(times)
      count = length(sorted)
      avg = div(Enum.sum(times), count)
      p50 = Enum.at(sorted, div(count, 2))
      p99 = Enum.at(sorted, round(count * 0.99))
      min_v = Enum.min(times)
      max_v = Enum.max(times)

      IO.puts(
        "  #{String.pad_trailing(label, 35)} #{String.pad_leading("#{avg}ns", 8)} #{String.pad_leading("#{p50}ns", 8)} #{String.pad_leading("#{p99}ns", 8)} #{String.pad_leading("#{min_v}ns", 8)} #{String.pad_leading("#{max_v}ns", 8)}"
      )
    end

    # Compute percentages
    parse_avg = div(Enum.sum(parse_times), 10_000)
    router_avg = div(Enum.sum(dispatch_times), 10_000)
    encode_avg = div(Enum.sum(encode_times), 10_000)
    ping_avg = div(Enum.sum(ping_times), 10_000)
    embedded_avg = div(Enum.sum(embedded_times), 10_000)
    setopts_avg = div(Enum.sum(setopts_times), 10_000)

    server_processing = parse_avg + router_avg + encode_avg
    # PING is minimal server work, so it's mostly TCP+kernel
    tcp_overhead = ping_avg
    unaccounted = tcp_avg - server_processing - tcp_overhead

    IO.puts("\n\n=== TCP GET Breakdown (estimated from component timing) ===\n")

    IO.puts(
      "  Full TCP GET round-trip:         #{tcp_avg}ns (#{Float.round(tcp_avg / 1000, 1)}us)"
    )

    IO.puts("")
    IO.puts("  Server-side processing:")

    IO.puts(
      "    RESP3 parse:                   #{parse_avg}ns  (#{Float.round(parse_avg / tcp_avg * 100, 1)}%)"
    )

    IO.puts(
      "    Router.get (ETS + LFU + hash): #{router_avg}ns  (#{Float.round(router_avg / tcp_avg * 100, 1)}%)"
    )

    IO.puts(
      "    RESP3 encode:                  #{encode_avg}ns  (#{Float.round(encode_avg / tcp_avg * 100, 1)}%)"
    )

    IO.puts(
      "    Subtotal server processing:    #{server_processing}ns  (#{Float.round(server_processing / tcp_avg * 100, 1)}%)"
    )

    IO.puts("")
    IO.puts("  TCP + kernel overhead:")

    IO.puts(
      "    TCP PING baseline:             #{ping_avg}ns  (#{Float.round(ping_avg / tcp_avg * 100, 1)}%)"
    )

    IO.puts("    setopts active:once:           #{setopts_avg}ns")
    IO.puts("")
    IO.puts("  Unaccounted (scheduling, recv loop, buffer, dispatch overhead):")
    IO.puts("    #{unaccounted}ns  (#{Float.round(max(unaccounted, 0) / tcp_avg * 100, 1)}%)")
    IO.puts("")
    IO.puts("  For comparison:")

    IO.puts(
      "    Embedded FerricStore.get:      #{embedded_avg}ns  (#{Float.round(embedded_avg / tcp_avg * 100, 1)}% of TCP)"
    )

    IO.puts("")
    IO.puts("  Reads/sec:")
    IO.puts("    Sequential TCP GET:            #{div(1_000_000_000, max(tcp_avg, 1))}")
    IO.puts("    Sequential embedded GET:       #{div(1_000_000_000, max(embedded_avg, 1))}")

    IO.puts(
      "    TCP overhead per read:         #{tcp_avg - embedded_avg}ns (#{Float.round((tcp_avg - embedded_avg) / 1000, 1)}us)"
    )
  end
end
