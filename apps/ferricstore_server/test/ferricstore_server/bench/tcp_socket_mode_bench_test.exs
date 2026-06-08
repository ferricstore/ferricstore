defmodule FerricstoreServer.Bench.TcpSocketModeBenchTest do
  use ExUnit.Case, async: false
  @moduletag :bench
  @moduletag timeout: 120_000

  test "TCP read throughput baseline (current socket mode)" do
    port = FerricstoreServer.Listener.port()
    prefix = "smb_#{System.unique_integer([:positive])}"

    # Pre-populate
    for i <- 1..10_000 do
      FerricStore.set("#{prefix}:#{i}", String.duplicate("v", 100))
    end

    Process.sleep(300)

    IO.puts("\n=== TCP Read Throughput (current socket mode) ===\n")

    IO.puts(
      "  #{String.pad_trailing("Conns", 8)} #{String.pad_trailing("Pipeline", 10)} #{String.pad_leading("Reads/sec", 12)} #{String.pad_leading("Per-conn", 10)}"
    )

    IO.puts("  #{String.duplicate("-", 45)}")

    for {num_conn, pipeline} <- [{1, 1}, {5, 10}, {5, 50}, {10, 50}, {25, 50}] do
      stop = :atomics.new(1, [])
      counter = :counters.new(1, [:atomics])

      workers =
        for _c <- 1..num_conn do
          Task.async(fn ->
            {:ok, sock} =
              :gen_tcp.connect(~c"127.0.0.1", port, [
                :binary,
                active: false,
                packet: :raw,
                buffer: 1_048_576
              ])

            :gen_tcp.send(sock, "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
            {:ok, _} = :gen_tcp.recv(sock, 0, 5000)

            batch = build_get_pipeline(prefix, pipeline)
            # approximate response size per GET
            resp_size = pipeline * 108

            l = fn l ->
              if :atomics.get(stop, 1) == 1, do: throw(:stop)
              :gen_tcp.send(sock, batch)
              drain_bytes(sock, resp_size)
              :counters.add(counter, 1, pipeline)
              l.(l)
            end

            try do
              l.(l)
            catch
              :throw, :stop -> :ok
            end

            :gen_tcp.close(sock)
          end)
        end

      Process.sleep(5_000)
      :atomics.put(stop, 1, 1)
      Task.await_many(workers, 30_000)

      total = :counters.get(counter, 1)
      rps = div(total, 5)
      per_conn = div(rps, max(num_conn, 1))

      IO.puts(
        "  #{String.pad_trailing("#{num_conn}", 8)} #{String.pad_trailing("#{pipeline}", 10)} #{String.pad_leading("#{rps}", 12)} #{String.pad_leading("#{per_conn}", 10)}"
      )

      Process.sleep(200)
    end
  end

  defp build_get_pipeline(prefix, count) do
    for _ <- 1..count do
      key = "#{prefix}:#{:rand.uniform(10_000)}"
      "*2\r\n$3\r\nGET\r\n$#{byte_size(key)}\r\n#{key}\r\n"
    end
    |> IO.iodata_to_binary()
  end

  defp drain_bytes(_sock, remaining) when remaining <= 0, do: :ok

  defp drain_bytes(sock, remaining) do
    {:ok, data} = :gen_tcp.recv(sock, 0, 10_000)
    drain_bytes(sock, remaining - byte_size(data))
  end
end
