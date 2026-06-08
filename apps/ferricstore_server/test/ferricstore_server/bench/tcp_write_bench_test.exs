defmodule FerricstoreServer.Bench.TcpWriteBenchTest do
  use ExUnit.Case, async: false
  @moduletag :bench
  @moduletag timeout: 180_000

  test "TCP write throughput - quorum (raft ON)" do
    port = FerricstoreServer.Listener.port()
    prefix = "tw_q_#{System.unique_integer([:positive])}"

    :erlang.system_flag(:scheduler_wall_time, true)

    IO.puts("\n=== TCP Write Throughput — Quorum (Raft ON) ===\n")

    IO.puts(
      "  #{String.pad_trailing("Conns", 8)} #{String.pad_trailing("Pipeline", 10)} #{String.pad_leading("Writes/sec", 12)} #{String.pad_leading("Sched %", 10)} #{String.pad_leading("Per-conn", 10)}"
    )

    IO.puts("  #{String.duplicate("-", 55)}")

    configs = [{1, 1}, {1, 10}, {5, 1}, {5, 10}, {5, 50}, {10, 10}, {10, 50}, {25, 10}, {50, 10}]

    for {num_conn, pipeline} <- configs do
      _batch = build_set_pipeline(prefix, pipeline)
      stop = :atomics.new(1, [])
      counter = :counters.new(1, [:atomics])

      sched_start = :erlang.statistics(:scheduler_wall_time)

      workers =
        for c <- 1..num_conn do
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
              b = build_set_pipeline("#{prefix}:#{c}", pipeline)
              :gen_tcp.send(sock, b)
              drain_ok_responses(sock, pipeline)
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

  test "TCP write throughput summary" do
    port = FerricstoreServer.Listener.port()

    IO.puts("\n=== TCP Write Throughput (5 conn × 10 pipeline, 5s) ===\n")

    q_total = run_write_bench(port, "cmp_q", 5, 10, 5_000, true)

    IO.puts("  Quorum (Raft):  #{div(q_total, 5)} writes/sec")
  end

  defp run_write_bench(port, prefix, num_conn, pipeline, duration_ms, _raft) do
    stop = :atomics.new(1, [])
    counter = :counters.new(1, [:atomics])

    workers =
      for c <- 1..num_conn do
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
            b = build_set_pipeline("#{prefix}:#{c}", pipeline)
            :gen_tcp.send(sock, b)
            drain_ok_responses(sock, pipeline)
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

    Process.sleep(duration_ms)
    :atomics.put(stop, 1, 1)
    Task.await_many(workers, 30_000)
    :counters.get(counter, 1)
  end

  defp build_set_pipeline(prefix, count) do
    for i <- 1..count do
      key = "#{prefix}:#{i}:#{System.unique_integer([:positive])}"
      val = "v"
      "*3\r\n$3\r\nSET\r\n$#{byte_size(key)}\r\n#{key}\r\n$#{byte_size(val)}\r\n#{val}\r\n"
    end
    |> IO.iodata_to_binary()
  end

  # Each SET response is "+OK\r\n" = 5 bytes
  defp drain_ok_responses(sock, expected) do
    drain_bytes(sock, expected * 5)
  end

  defp drain_bytes(_sock, remaining) when remaining <= 0, do: :ok

  defp drain_bytes(sock, remaining) do
    {:ok, data} = :gen_tcp.recv(sock, 0, 10_000)
    drain_bytes(sock, remaining - byte_size(data))
  end
end
