defmodule FerricstoreServer.Bench.TcpReadProfileTest do
  use ExUnit.Case, async: false
  @moduletag :bench
  @moduletag timeout: 120_000

  test "profile TCP read path - sequential round trips" do
    port = FerricstoreServer.Listener.port()
    prefix = "tcp_rp_#{System.unique_integer([:positive])}"

    for i <- 1..1000 do
      FerricStore.set("#{prefix}:#{i}", "value_#{i}")
    end

    Process.sleep(300)

    {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])
    :gen_tcp.send(sock, "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
    {:ok, _} = :gen_tcp.recv(sock, 0, 5000)

    # Warm up
    for _ <- 1..100 do
      key = "#{prefix}:#{:rand.uniform(1000)}"
      cmd = "*2\r\n$3\r\nGET\r\n$#{byte_size(key)}\r\n#{key}\r\n"
      :gen_tcp.send(sock, cmd)
      {:ok, _} = :gen_tcp.recv(sock, 0, 5000)
    end

    # Profile 10000 GETs
    timings =
      for _ <- 1..10_000 do
        key = "#{prefix}:#{:rand.uniform(1000)}"
        cmd = "*2\r\n$3\r\nGET\r\n$#{byte_size(key)}\r\n#{key}\r\n"

        t0 = :erlang.monotonic_time(:nanosecond)
        :ok = :gen_tcp.send(sock, cmd)
        t1 = :erlang.monotonic_time(:nanosecond)
        {:ok, _resp} = :gen_tcp.recv(sock, 0, 5000)
        t2 = :erlang.monotonic_time(:nanosecond)

        %{tcp_send: t1 - t0, server_roundtrip: t2 - t1, total: t2 - t0}
      end

    :gen_tcp.close(sock)

    IO.puts("\n=== TCP Read Profile (10000 sequential GETs, nanoseconds) ===\n")

    IO.puts(
      "  #{String.pad_trailing("Step", 25)} #{String.pad_leading("avg", 8)} #{String.pad_leading("p50", 8)} #{String.pad_leading("p99", 8)} #{String.pad_leading("min", 8)} #{String.pad_leading("max", 8)}  %"
    )

    IO.puts("  #{String.duplicate("-", 85)}")

    total_avg = div(Enum.sum(Enum.map(timings, & &1.total)), 10_000)

    for step <- [:tcp_send, :server_roundtrip, :total] do
      values = Enum.map(timings, &Map.get(&1, step))
      sorted = Enum.sort(values)
      count = length(sorted)
      avg = div(Enum.sum(values), count)
      p50 = Enum.at(sorted, div(count, 2))
      p99 = Enum.at(sorted, round(count * 0.99))
      min_v = Enum.min(values)
      max_v = Enum.max(values)
      pct = if total_avg > 0, do: Float.round(avg / total_avg * 100, 1), else: 0.0

      IO.puts(
        "  #{String.pad_trailing(to_string(step), 25)} #{String.pad_leading("#{avg}ns", 8)} #{String.pad_leading("#{p50}ns", 8)} #{String.pad_leading("#{p99}ns", 8)} #{String.pad_leading("#{min_v}ns", 8)} #{String.pad_leading("#{max_v}ns", 8)}  #{pct}%"
      )
    end

    IO.puts("\n  Sequential TCP reads/sec: #{div(1_000_000_000, max(total_avg, 1))}")
  end

  test "50 concurrent TCP connections" do
    port = FerricstoreServer.Listener.port()
    prefix = "tcp_conc_#{System.unique_integer([:positive])}"

    for i <- 1..10_000 do
      FerricStore.set("#{prefix}:#{i}", "value_#{i}")
    end

    Process.sleep(300)

    stop = :atomics.new(1, [])
    counter = :counters.new(1, [:atomics])

    :erlang.system_flag(:scheduler_wall_time, true)
    sched_start = :erlang.statistics(:scheduler_wall_time)

    readers =
      for _ <- 1..50 do
        Task.async(fn ->
          {:ok, sock} =
            :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])

          :gen_tcp.send(sock, "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
          {:ok, _} = :gen_tcp.recv(sock, 0, 5000)

          l = fn l ->
            if :atomics.get(stop, 1) == 1, do: throw(:stop)
            key = "#{prefix}:#{:rand.uniform(10_000)}"
            cmd = "*2\r\n$3\r\nGET\r\n$#{byte_size(key)}\r\n#{key}\r\n"
            :gen_tcp.send(sock, cmd)
            {:ok, _} = :gen_tcp.recv(sock, 0, 5000)
            :counters.add(counter, 1, 1)
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

    Process.sleep(10_000)
    :atomics.put(stop, 1, 1)
    Task.await_many(readers, 30_000)

    sched_end = :erlang.statistics(:scheduler_wall_time)
    total = :counters.get(counter, 1)

    utils =
      Enum.zip(Enum.sort(sched_start), Enum.sort(sched_end))
      |> Enum.map(fn {{_, a1, t1}, {_, a2, t2}} ->
        if t2 - t1 > 0, do: (a2 - a1) / (t2 - t1) * 100, else: 0.0
      end)

    avg_util = Float.round(Enum.sum(utils) / length(utils), 1)

    IO.puts("\n=== TCP Concurrent Read (50 connections, 10s) ===")
    IO.puts("  Total reads: #{total}")
    IO.puts("  Reads/sec: #{div(total, 10)}")
    IO.puts("  Avg per read: #{div(10_000_000, max(div(total, 10), 1))}us")
    IO.puts("  Scheduler utilization: #{avg_util}%")
  end

  test "embedded vs TCP comparison" do
    port = FerricstoreServer.Listener.port()
    prefix = "cmp_#{System.unique_integer([:positive])}"

    for i <- 1..1000 do
      FerricStore.set("#{prefix}:#{i}", "value_#{i}")
    end

    Process.sleep(200)

    # Embedded
    stop1 = :atomics.new(1, [])
    c1 = :counters.new(1, [:atomics])

    e_readers =
      for _ <- 1..50 do
        Task.async(fn ->
          l = fn l ->
            if :atomics.get(stop1, 1) == 1, do: throw(:stop)
            {:ok, _} = FerricStore.get("#{prefix}:#{:rand.uniform(1000)}")
            :counters.add(c1, 1, 1)
            l.(l)
          end

          try do
            l.(l)
          catch
            :throw, :stop -> :ok
          end
        end)
      end

    Process.sleep(5_000)
    :atomics.put(stop1, 1, 1)
    Task.await_many(e_readers, 30_000)
    e_total = :counters.get(c1, 1)

    Process.sleep(300)

    # TCP
    stop2 = :atomics.new(1, [])
    c2 = :counters.new(1, [:atomics])

    t_readers =
      for _ <- 1..50 do
        Task.async(fn ->
          {:ok, sock} =
            :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])

          :gen_tcp.send(sock, "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
          {:ok, _} = :gen_tcp.recv(sock, 0, 5000)

          l = fn l ->
            if :atomics.get(stop2, 1) == 1, do: throw(:stop)
            key = "#{prefix}:#{:rand.uniform(1000)}"
            cmd = "*2\r\n$3\r\nGET\r\n$#{byte_size(key)}\r\n#{key}\r\n"
            :gen_tcp.send(sock, cmd)
            {:ok, _} = :gen_tcp.recv(sock, 0, 5000)
            :counters.add(c2, 1, 1)
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
    :atomics.put(stop2, 1, 1)
    Task.await_many(t_readers, 30_000)
    t_total = :counters.get(c2, 1)

    IO.puts("\n=== Embedded vs TCP (50 readers, 5s each) ===")
    IO.puts("  Embedded: #{div(e_total, 5)} reads/sec")
    IO.puts("  TCP:      #{div(t_total, 5)} reads/sec")
    IO.puts("  Overhead: #{Float.round(e_total / max(t_total, 1), 2)}x faster embedded")
  end

  test "profile connection process internals during reads" do
    port = FerricstoreServer.Listener.port()
    prefix = "cp_#{System.unique_integer([:positive])}"

    for i <- 1..1000 do
      FerricStore.set("#{prefix}:#{i}", "value_#{i}")
    end

    Process.sleep(200)

    {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])
    :gen_tcp.send(sock, "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
    {:ok, _} = :gen_tcp.recv(sock, 0, 5000)

    # Find connection processes
    conn_pids =
      try do
        :ranch.procs(FerricstoreServer.Listener, :connections)
      rescue
        _ -> []
      catch
        _, _ -> []
      end

    if conn_pids == [] do
      IO.puts("\n=== Connection Process Profile: SKIPPED ===")
      :gen_tcp.close(sock)
    else
      conn_pid = List.last(conn_pids)
      samples = :ets.new(:cp_samples, [:set, :public])

      poller =
        Task.async(fn ->
          poll = fn poll, n ->
            receive do
              :stop -> n
            after
              0 ->
                info = Process.info(conn_pid, [:current_function, :status, :message_queue_len])
                :ets.insert(samples, {n, info})
                target = :erlang.monotonic_time(:microsecond) + 100
                bw = fn bw -> if :erlang.monotonic_time(:microsecond) < target, do: bw.(bw) end
                bw.(bw)
                poll.(poll, n + 1)
            end
          end

          poll.(poll, 0)
        end)

      # Pump reads
      for _ <- 1..30_000 do
        key = "#{prefix}:#{:rand.uniform(1000)}"
        cmd = "*2\r\n$3\r\nGET\r\n$#{byte_size(key)}\r\n#{key}\r\n"
        :gen_tcp.send(sock, cmd)
        {:ok, _} = :gen_tcp.recv(sock, 0, 5000)
      end

      send(poller.pid, :stop)
      Task.await(poller, 5000)
      :gen_tcp.close(sock)

      all = :ets.tab2list(samples)
      total = length(all)

      if total > 0 do
        IO.puts("\n=== Connection Process Profile (#{total} samples @ 100us) ===\n")

        funcs =
          all
          |> Enum.map(fn {_, info} ->
            case Keyword.get(info || [], :current_function) do
              {m, f, a} -> "#{inspect(m)}.#{f}/#{a}"
              other -> inspect(other)
            end
          end)
          |> Enum.frequencies()
          |> Enum.sort_by(fn {_, c} -> -c end)

        Enum.take(funcs, 20)
        |> Enum.each(fn {func, count} ->
          pct = Float.round(count / total * 100, 1)
          bar = String.duplicate("█", min(round(pct / 2), 40))
          IO.puts("  #{String.pad_trailing(func, 60)} #{String.pad_leading("#{pct}%", 6)} #{bar}")
        end)

        statuses =
          all
          |> Enum.map(fn {_, info} -> Keyword.get(info || [], :status) end)
          |> Enum.frequencies()

        IO.puts("\n  Status: #{inspect(statuses)}")

        queues = Enum.map(all, fn {_, info} -> Keyword.get(info || [], :message_queue_len, 0) end)

        IO.puts(
          "  Msg queue: avg=#{div(Enum.sum(queues), max(total, 1))} max=#{Enum.max(queues)}"
        )
      end

      :ets.delete(samples)
    end
  end
end
