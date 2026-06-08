defmodule FerricstoreServer.Bench.TcpCpuProfileTest do
  use ExUnit.Case, async: false
  @moduletag :bench
  @moduletag timeout: 120_000

  @resp_size_per_value 108

  test "CPU profile: 5×10 pipeline (best config)" do
    port = FerricstoreServer.Listener.port()
    prefix = "cpu_#{System.unique_integer([:positive])}"

    for i <- 1..10_000 do
      FerricStore.set("#{prefix}:#{i}", String.duplicate("v", 100))
    end

    Process.sleep(300)

    batch = build_pipeline(prefix, 10)
    stop = :atomics.new(1, [])
    counter = :counters.new(1, [:atomics])

    # Start 5 pipelined workers
    workers =
      for _ <- 1..5 do
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
            drain_bytes(sock, 10 * @resp_size_per_value)
            :counters.add(counter, 1, 10)
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

    # Warm up 1 second
    Process.sleep(1_000)

    IO.puts("\n=== CPU Profile: 5 connections × 10 pipeline, 5 seconds ===\n")

    # Use process polling for CPU profiling — sample every 200us
    samples = :ets.new(:cpu_samples, [:duplicate_bag, :public])

    poller =
      Task.async(fn ->
        poll = fn poll, n ->
          receive do
            :stop -> n
          after
            0 ->
              # Sample ALL schedulable processes (not just connection procs)
              # Check the run queue — which processes are currently running
              for pid <- Process.list() do
                case Process.info(pid, [:status, :current_function]) do
                  [status: :running, current_function: {m, f, a}] ->
                    :ets.insert(samples, {:running, {m, f, a}})

                  [status: :runnable, current_function: {m, f, a}] ->
                    :ets.insert(samples, {:runnable, {m, f, a}})

                  _ ->
                    :ok
                end
              end

              # Busy wait 500us
              target = :erlang.monotonic_time(:microsecond) + 500
              bw = fn bw -> if :erlang.monotonic_time(:microsecond) < target, do: bw.(bw) end
              bw.(bw)
              poll.(poll, n + 1)
          end
        end

        poll.(poll, 0)
      end)

    Process.sleep(5_000)

    :atomics.put(stop, 1, 1)
    send(poller.pid, :stop)
    Task.await_many(workers ++ [poller], 30_000)

    total = :counters.get(counter, 1)
    IO.puts("  Throughput: #{div(total, 6)} reads/sec (6s total)\n")

    # Analyze running samples
    all = :ets.tab2list(samples)
    running = Enum.filter(all, fn {status, _} -> status == :running end)
    total_running = length(running)

    IO.puts("  Total samples: #{length(all)}")
    IO.puts("  Running samples: #{total_running}\n")

    if total_running > 0 do
      funcs =
        running
        |> Enum.map(fn {_, {m, f, a}} -> "#{inspect(m)}.#{f}/#{a}" end)
        |> Enum.frequencies()
        |> Enum.sort_by(fn {_, c} -> -c end)

      IO.puts("  Top functions by CPU time (running processes):\n")

      Enum.take(funcs, 25)
      |> Enum.each(fn {func, count} ->
        pct = Float.round(count / total_running * 100, 1)
        bar = String.duplicate("█", min(round(pct / 2), 40))
        IO.puts("    #{String.pad_trailing(func, 60)} #{String.pad_leading("#{pct}%", 6)} #{bar}")
      end)
    end

    :ets.delete(samples)
  end

  test "CPU profile: 5×50 pipeline (slow config)" do
    port = FerricstoreServer.Listener.port()
    prefix = "cpu50_#{System.unique_integer([:positive])}"

    for i <- 1..10_000 do
      FerricStore.set("#{prefix}:#{i}", String.duplicate("v", 100))
    end

    Process.sleep(300)

    batch = build_pipeline(prefix, 50)
    stop = :atomics.new(1, [])
    counter = :counters.new(1, [:atomics])

    workers =
      for _ <- 1..5 do
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
            drain_bytes(sock, 50 * @resp_size_per_value)
            :counters.add(counter, 1, 50)
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

    Process.sleep(1_000)

    IO.puts("\n=== CPU Profile: 5 connections × 50 pipeline, 5 seconds ===\n")

    :eprof.start()
    :eprof.start_profiling(Process.list())

    Process.sleep(5_000)

    :eprof.stop_profiling()
    :atomics.put(stop, 1, 1)
    Task.await_many(workers, 30_000)

    total = :counters.get(counter, 1)
    IO.puts("  Throughput: #{div(total, 6)} reads/sec (6s total)\n")

    IO.puts("  Top functions by CPU time:\n")
    :eprof.analyze(:total)
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
