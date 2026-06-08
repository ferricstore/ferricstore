defmodule FerricstoreServer.Bench.TcpPipelinePoolTest do
  use ExUnit.Case, async: false
  @moduletag :bench
  @moduletag timeout: 180_000

  test "pipelined connection pool throughput" do
    port = FerricstoreServer.Listener.port()
    prefix = "pp_#{System.unique_integer([:positive])}"

    for i <- 1..10_000 do
      FerricStore.set("#{prefix}:#{i}", String.duplicate("v", 100))
    end

    Process.sleep(300)

    :erlang.system_flag(:scheduler_wall_time, true)

    IO.puts("\n=== Pipelined Connection Pool Throughput (5s each) ===\n")

    IO.puts(
      "  #{String.pad_trailing("Connections", 13)} #{String.pad_trailing("Pipeline", 10)} #{String.pad_leading("Reads/sec", 12)} #{String.pad_leading("Sched util", 12)} #{String.pad_leading("Per-conn", 12)}"
    )

    IO.puts("  #{String.duplicate("-", 65)}")

    configs = [
      {1, 1},
      {1, 100},
      {5, 10},
      {5, 50},
      {5, 100},
      {10, 10},
      {10, 50},
      {10, 100},
      {25, 50},
      {25, 100},
      {50, 50},
      {50, 100}
    ]

    for {num_conn, pipeline_size} <- configs do
      stop = :atomics.new(1, [])
      counter = :counters.new(1, [:atomics])

      sched_start = :erlang.statistics(:scheduler_wall_time)

      workers =
        for _ <- 1..num_conn do
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

            batch = build_pipeline(prefix, pipeline_size)

            l = fn l ->
              if :atomics.get(stop, 1) == 1, do: throw(:stop)
              :gen_tcp.send(sock, batch)
              drain_all(sock, pipeline_size)
              :counters.add(counter, 1, pipeline_size)
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

      sched_end = :erlang.statistics(:scheduler_wall_time)
      total = :counters.get(counter, 1)

      utils =
        Enum.zip(Enum.sort(sched_start), Enum.sort(sched_end))
        |> Enum.map(fn {{_, a1, t1}, {_, a2, t2}} ->
          if t2 - t1 > 0, do: (a2 - a1) / (t2 - t1) * 100, else: 0.0
        end)

      avg_util = Float.round(Enum.sum(utils) / length(utils), 1)

      rps = div(total, 5)
      per_conn = div(rps, max(num_conn, 1))

      IO.puts(
        "  #{String.pad_trailing("#{num_conn}", 13)} #{String.pad_trailing("#{pipeline_size}", 10)} #{String.pad_leading("#{rps}", 12)} #{String.pad_leading("#{avg_util}%", 12)} #{String.pad_leading("#{per_conn}", 12)}"
      )

      Process.sleep(300)
    end
  end

  defp build_pipeline(prefix, size) do
    for _ <- 1..size do
      key = "#{prefix}:#{:rand.uniform(10_000)}"
      "*2\r\n$3\r\nGET\r\n$#{byte_size(key)}\r\n#{key}\r\n"
    end
    |> IO.iodata_to_binary()
  end

  # Fast drain: each response is "$100\r\n<100 bytes>\r\n" = 108 bytes.
  # Just read until we've consumed enough bytes for all responses.
  # No parsing, no pattern matching — just bulk recv.
  # "$100\r\n" (6) + value (100) + "\r\n" (2)
  @resp_size_per_value 108

  defp drain_all(sock, expected) do
    needed = expected * @resp_size_per_value
    drain_bytes(sock, needed)
  end

  defp drain_bytes(_sock, remaining) when remaining <= 0, do: :ok

  defp drain_bytes(sock, remaining) do
    {:ok, data} = :gen_tcp.recv(sock, 0, 5000)
    drain_bytes(sock, remaining - byte_size(data))
  end
end
