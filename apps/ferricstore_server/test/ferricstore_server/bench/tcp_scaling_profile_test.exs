defmodule FerricstoreServer.Bench.TcpScalingProfileTest do
  use ExUnit.Case, async: false
  @moduletag :bench
  @moduletag timeout: 180_000

  test "TCP read scaling: throughput + scheduler utilization vs connection count" do
    port = FerricstoreServer.Listener.port()
    prefix = "tsp_#{System.unique_integer([:positive])}"

    for i <- 1..10_000 do
      FerricStore.set("#{prefix}:#{i}", String.duplicate("v", 100))
    end

    Process.sleep(300)

    pipeline = 50
    :erlang.system_flag(:scheduler_wall_time, true)

    IO.puts("\n=== TCP Read Scaling Profile (pipeline=#{pipeline}, 5s each) ===\n")

    IO.puts(
      "  #{String.pad_trailing("Conns", 8)} #{String.pad_leading("Reads/sec", 12)} #{String.pad_leading("Per-conn", 10)} #{String.pad_leading("CPU %", 8)} #{String.pad_leading("RunQ", 8)} #{String.pad_leading("Procs", 8)} #{String.pad_leading("Reduct/s", 10)}"
    )

    IO.puts("  #{String.duplicate("-", 75)}")

    for num_conn <- [1, 5, 10, 25, 50] do
      stop = :atomics.new(1, [])
      counter = :counters.new(1, [:atomics])

      sched_start = :erlang.statistics(:scheduler_wall_time)
      {red_start, _} = :erlang.statistics(:reductions)

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

      Process.sleep(3_000)
      :atomics.put(stop, 1, 1)

      sched_end = :erlang.statistics(:scheduler_wall_time)
      {red_end, _} = :erlang.statistics(:reductions)
      proc_count = length(Process.list())

      Task.await_many(workers, 30_000)

      total = :counters.get(counter, 1)
      rps = div(total, 3)
      per_conn = div(rps, max(num_conn, 1))

      # Scheduler utilization
      utils =
        Enum.zip(Enum.sort(sched_start), Enum.sort(sched_end))
        |> Enum.map(fn {{_, a1, t1}, {_, a2, t2}} ->
          if t2 - t1 > 0, do: (a2 - a1) / (t2 - t1) * 100, else: 0.0
        end)

      avg_cpu = Float.round(Enum.sum(utils) / length(utils), 1)

      # Run queue (scheduler pressure indicator)
      run_q = :erlang.statistics(:total_run_queue_lengths)

      # Reductions per second
      red_per_sec = div(red_end - red_start, 3)

      IO.puts(
        "  #{String.pad_trailing("#{num_conn}", 8)} #{String.pad_leading("#{rps}", 12)} #{String.pad_leading("#{per_conn}", 10)} #{String.pad_leading("#{avg_cpu}%", 8)} #{String.pad_leading("#{run_q}", 8)} #{String.pad_leading("#{proc_count}", 8)} #{String.pad_leading("#{red_per_sec}", 10)}"
      )

      Process.sleep(300)
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
