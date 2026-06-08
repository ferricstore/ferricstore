defmodule FerricstoreServer.Bench.TcpPipelineProfileTest do
  use ExUnit.Case, async: false
  @moduletag :bench
  @moduletag timeout: 180_000

  @resp_size_per_value 108

  test "profile pipelined connections at different scales" do
    port = FerricstoreServer.Listener.port()
    prefix = "ppp_#{System.unique_integer([:positive])}"

    for i <- 1..10_000 do
      FerricStore.set("#{prefix}:#{i}", String.duplicate("v", 100))
    end

    Process.sleep(300)

    :erlang.system_flag(:scheduler_wall_time, true)

    configs = [
      {5, 10, "5×10 (best)"},
      {5, 50, "5×50"},
      {10, 50, "10×50"},
      {25, 50, "25×50"}
    ]

    for {num_conn, pipeline, label} <- configs do
      IO.puts("\n=== Profile: #{label} — #{num_conn} connections × #{pipeline} pipeline ===\n")

      stop = :atomics.new(1, [])
      counter = :counters.new(1, [:atomics])

      # We'll find connection processes after they're created

      sched_start = :erlang.statistics(:scheduler_wall_time)

      # Start pipelined workers
      batch = build_pipeline(prefix, pipeline)

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

      # Let it run 1 second to warm up
      Process.sleep(1000)

      # Now profile for 3 seconds
      # Find connection processes by scanning for the module
      # Use Process.list and filter by initial_call
      new_pids =
        Process.list()
        |> Enum.filter(fn pid ->
          case Process.info(pid, :dictionary) do
            {:dictionary, dict} ->
              case Keyword.get(dict, :"$initial_call") do
                {FerricstoreServer.Connection, _, _} -> true
                _ -> false
              end

            _ ->
              false
          end
        end)
        |> Enum.take(10)

      samples = :ets.new(:pipe_samples, [:ordered_set, :public])

      poller =
        Task.async(fn ->
          poll = fn poll, n ->
            receive do
              :stop -> n
            after
              0 ->
                for {pid, idx} <- Enum.with_index(new_pids) do
                  if Process.alive?(pid) do
                    info = Process.info(pid, [:current_function, :status, :message_queue_len])
                    if info, do: :ets.insert(samples, {{n, idx}, info})
                  end
                end

                # Busy wait 200us
                target = :erlang.monotonic_time(:microsecond) + 200
                bw = fn bw -> if :erlang.monotonic_time(:microsecond) < target, do: bw.(bw) end
                bw.(bw)
                poll.(poll, n + 1)
            end
          end

          poll.(poll, 0)
        end)

      Process.sleep(3_000)

      # Stop
      :atomics.put(stop, 1, 1)
      send(poller.pid, :stop)
      Task.await_many(workers ++ [poller], 30_000)

      sched_end = :erlang.statistics(:scheduler_wall_time)
      total = :counters.get(counter, 1)

      utils =
        Enum.zip(Enum.sort(sched_start), Enum.sort(sched_end))
        |> Enum.map(fn {{_, a1, t1}, {_, a2, t2}} ->
          if t2 - t1 > 0, do: (a2 - a1) / (t2 - t1) * 100, else: 0.0
        end)

      avg_util = Float.round(Enum.sum(utils) / length(utils), 1)

      # 4 seconds (1s warmup + 3s profiling)
      rps = div(total, 4)

      IO.puts("  Throughput: #{rps} reads/sec")
      IO.puts("  Scheduler utilization: #{avg_util}%")

      # Analyze connection process samples
      all = :ets.tab2list(samples)
      total_samples = length(all)

      if total_samples > 0 do
        # Function distribution across ALL connection processes
        funcs =
          all
          |> Enum.map(fn {_key, info} ->
            case Keyword.get(info || [], :current_function) do
              {m, f, a} -> "#{inspect(m)}.#{f}/#{a}"
              other -> inspect(other)
            end
          end)
          |> Enum.frequencies()
          |> Enum.sort_by(fn {_, c} -> -c end)

        IO.puts(
          "\n  Connection process profile (#{total_samples} samples across #{min(length(new_pids), 10)} processes):\n"
        )

        Enum.take(funcs, 15)
        |> Enum.each(fn {func, count} ->
          pct = Float.round(count / total_samples * 100, 1)
          bar = String.duplicate("█", min(round(pct / 2), 40))

          IO.puts(
            "    #{String.pad_trailing(func, 55)} #{String.pad_leading("#{pct}%", 6)} #{bar}"
          )
        end)

        # Status distribution
        statuses =
          all
          |> Enum.map(fn {_key, info} -> Keyword.get(info || [], :status) end)
          |> Enum.frequencies()

        IO.puts(
          "\n  Status: #{Enum.map_join(statuses, ", ", fn {s, c} -> "#{s}=#{Float.round(c / total_samples * 100, 1)}%" end)}"
        )

        # Message queue
        queues =
          Enum.map(all, fn {_key, info} -> Keyword.get(info || [], :message_queue_len, 0) end)

        IO.puts(
          "  Msg queue: avg=#{div(Enum.sum(queues), max(total_samples, 1))} max=#{Enum.max(queues)}"
        )
      end

      :ets.delete(samples)
      Process.sleep(500)
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
