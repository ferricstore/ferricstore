defmodule FerricstoreServer.Bench.TcpMethodProfileTest do
  use ExUnit.Case, async: false
  @moduletag :bench
  @moduletag timeout: 120_000

  test "TCP read: cprof function call counts at 5 and 25 connections" do
    port = FerricstoreServer.Listener.port()
    prefix = "tmp_#{System.unique_integer([:positive])}"

    for i <- 1..10_000 do
      FerricStore.set("#{prefix}:#{i}", String.duplicate("v", 100))
    end

    Process.sleep(300)

    pipeline = 50

    for num_conn <- [5, 25] do
      IO.puts("\n=== cprof: #{num_conn} connections × #{pipeline} pipeline, 3s ===\n")

      stop = :atomics.new(1, [])
      counter = :counters.new(1, [:atomics])

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

            batch = build_pipeline(prefix, pipeline)
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

      # Warm up 1s
      Process.sleep(1_000)

      # Profile for 2s
      :cprof.start()
      Process.sleep(2_000)
      :cprof.pause()

      :atomics.put(stop, 1, 1)
      Task.await_many(workers, 15_000)

      total = :counters.get(counter, 1)
      rps = div(total, 3)
      IO.puts("  Throughput: #{rps} reads/sec\n")

      # Analyze top modules
      {_total, top_modules} = :cprof.analyse()

      IO.puts("  Top 20 modules by call count:\n")

      top_modules
      |> Enum.sort_by(fn {_mod, count, _} -> -count end)
      |> Enum.take(20)
      |> Enum.each(fn {mod, count, funcs} ->
        top_func =
          funcs
          |> Enum.sort_by(fn {_mfa, c} -> -c end)
          |> Enum.take(3)
          |> Enum.map_join(", ", fn {{_m, f, a}, c} -> "#{f}/#{a}=#{c}" end)

        IO.puts(
          "    #{String.pad_trailing(inspect(mod), 50)} #{String.pad_leading("#{count}", 10)}  (#{top_func})"
        )
      end)

      :cprof.stop()
      Process.sleep(300)
    end
  end

  test "TCP read: process sampling at 5 and 25 connections" do
    port = FerricstoreServer.Listener.port()
    prefix = "smp_#{System.unique_integer([:positive])}"

    for i <- 1..10_000 do
      FerricStore.set("#{prefix}:#{i}", String.duplicate("v", 100))
    end

    Process.sleep(300)

    pipeline = 50

    for num_conn <- [5, 25] do
      IO.puts("\n=== Process sampling: #{num_conn} connections × #{pipeline} pipeline, 2s ===\n")

      stop = :atomics.new(1, [])
      counter = :counters.new(1, [:atomics])

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

            batch = build_pipeline(prefix, pipeline)
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

      Process.sleep(500)

      # Sample running processes every 200us for 2 seconds
      samples = :ets.new(:samples, [:bag, :public])
      sample_stop = :atomics.new(1, [])

      sampler =
        Task.async(fn ->
          sample_loop(samples, sample_stop, 0)
        end)

      Process.sleep(2_000)

      :atomics.put(sample_stop, 1, 1)
      :atomics.put(stop, 1, 1)

      num_samples = Task.await(sampler, 5_000)
      Task.await_many(workers, 15_000)

      total = :counters.get(counter, 1)
      rps = div(total, 3)
      IO.puts("  Throughput: #{rps} reads/sec")
      IO.puts("  Samples: #{num_samples}\n")

      # Analyze
      all = :ets.tab2list(samples)
      running = Enum.filter(all, fn {status, _} -> status == :running end)

      if running != [] do
        funcs =
          running
          |> Enum.map(fn {_, {m, f, a}} -> "#{inspect(m)}.#{f}/#{a}" end)
          |> Enum.frequencies()
          |> Enum.sort_by(fn {_, c} -> -c end)

        total_running = length(running)
        IO.puts("  Top functions by CPU time (#{total_running} running samples):\n")

        Enum.take(funcs, 15)
        |> Enum.each(fn {func, count} ->
          pct = Float.round(count / total_running * 100, 1)
          bar = String.duplicate("█", min(round(pct / 2), 40))

          IO.puts(
            "    #{String.pad_trailing(func, 55)} #{String.pad_leading("#{pct}%", 6)} #{bar}"
          )
        end)
      end

      :ets.delete(samples)
      Process.sleep(300)
    end
  end

  defp sample_loop(tab, stop, n) do
    if :atomics.get(stop, 1) == 1 do
      n
    else
      for pid <- Process.list() do
        case Process.info(pid, [:status, :current_function]) do
          [status: :running, current_function: {m, f, a}] ->
            :ets.insert(tab, {:running, {m, f, a}})

          [status: :runnable, current_function: {m, f, a}] ->
            :ets.insert(tab, {:runnable, {m, f, a}})

          _ ->
            :ok
        end
      end

      # Busy-wait ~200us
      target = :erlang.monotonic_time(:microsecond) + 200
      busy_wait(target)
      sample_loop(tab, stop, n + 1)
    end
  end

  defp busy_wait(target) do
    if :erlang.monotonic_time(:microsecond) < target, do: busy_wait(target)
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
    {:ok, data} = :gen_tcp.recv(sock, 0, 10_000)
    drain_bytes(sock, remaining - byte_size(data))
  end
end
