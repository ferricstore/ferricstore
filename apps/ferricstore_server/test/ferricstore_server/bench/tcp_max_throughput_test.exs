defmodule FerricstoreServer.Bench.TcpMaxThroughputTest do
  use ExUnit.Case, async: false
  @moduletag :bench
  @moduletag timeout: 300_000

  @resp_size_per_value 108

  test "find peak TCP read throughput" do
    port = FerricstoreServer.Listener.port()
    prefix = "max_#{System.unique_integer([:positive])}"

    for i <- 1..10_000 do
      FerricStore.set("#{prefix}:#{i}", String.duplicate("v", 100))
    end

    Process.sleep(300)

    :erlang.system_flag(:scheduler_wall_time, true)

    IO.puts("\n=== Finding Peak TCP Read Throughput ===\n")

    IO.puts(
      "  #{String.pad_trailing("Conns", 8)} #{String.pad_trailing("Pipeline", 10)} #{String.pad_leading("Reads/sec", 12)} #{String.pad_leading("Sched %", 10)} #{String.pad_leading("Per-conn", 10)}"
    )

    IO.puts("  #{String.duplicate("-", 55)}")

    configs = [
      {5, 100},
      {10, 100},
      {15, 100},
      {20, 100},
      {25, 100},
      {30, 100},
      {40, 100},
      {50, 100},
      {75, 100},
      {100, 100},
      {150, 100},
      {200, 100}
    ]

    for {num_conn, pipeline} <- configs do
      batch = build_pipeline(prefix, pipeline)
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

            l = fn l ->
              if :atomics.get(stop, 1) == 1, do: throw(:stop)
              :gen_tcp.send(sock, batch)
              drain_bytes(sock, pipeline * @resp_size_per_value)
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
        "  #{String.pad_trailing("#{num_conn}", 8)} #{String.pad_trailing("#{pipeline}", 10)} #{String.pad_leading("#{rps}", 12)} #{String.pad_leading("#{avg_util}%", 10)} #{String.pad_leading("#{per_conn}", 10)}"
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

  defp drain_bytes(_sock, remaining) when remaining <= 0, do: :ok

  defp drain_bytes(sock, remaining) do
    {:ok, data} = :gen_tcp.recv(sock, 0, 5000)
    drain_bytes(sock, remaining - byte_size(data))
  end
end
