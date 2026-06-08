defmodule FerricstoreServer.Bench.TcpThroughputProfileTest do
  use ExUnit.Case, async: false
  @moduletag :bench
  @moduletag timeout: 180_000

  test "TCP read throughput scaling" do
    port = FerricstoreServer.Listener.port()
    prefix = "ttp_#{System.unique_integer([:positive])}"

    for i <- 1..10_000 do
      FerricStore.set("#{prefix}:#{i}", String.duplicate("v", 100))
    end

    Process.sleep(300)

    :erlang.system_flag(:scheduler_wall_time, true)

    IO.puts("\n=== TCP Read Throughput Scaling ===\n")

    IO.puts(
      "  #{String.pad_trailing("Connections", 15)} #{String.pad_leading("Reads/sec", 12)} #{String.pad_leading("Sched util", 12)} #{String.pad_leading("Per-conn", 12)}"
    )

    IO.puts("  #{String.duplicate("-", 55)}")

    for num_conn <- [1, 5, 10, 25, 50, 100, 200] do
      stop = :atomics.new(1, [])
      counter = :counters.new(1, [:atomics])

      sched_start = :erlang.statistics(:scheduler_wall_time)

      readers =
        for _ <- 1..num_conn do
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

      Process.sleep(5_000)
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

      rps = div(total, 5)
      per_conn = div(rps, max(num_conn, 1))

      IO.puts(
        "  #{String.pad_trailing("#{num_conn}", 15)} #{String.pad_leading("#{rps}", 12)} #{String.pad_leading("#{avg_util}%", 12)} #{String.pad_leading("#{per_conn}", 12)}"
      )

      Process.sleep(500)
    end
  end

  test "TCP pipelined read throughput" do
    port = FerricstoreServer.Listener.port()
    prefix = "tpl_#{System.unique_integer([:positive])}"

    for i <- 1..10_000 do
      FerricStore.set("#{prefix}:#{i}", String.duplicate("v", 100))
    end

    Process.sleep(300)

    IO.puts("\n=== TCP Pipelined Read Throughput ===\n")

    IO.puts(
      "  #{String.pad_trailing("Pipeline size", 15)} #{String.pad_leading("Reads/sec", 12)} #{String.pad_leading("Avg/read", 12)}"
    )

    IO.puts("  #{String.duplicate("-", 45)}")

    for pipeline_size <- [1, 10, 50, 100, 500] do
      {:ok, sock} =
        :gen_tcp.connect(~c"127.0.0.1", port, [
          :binary,
          active: false,
          packet: :raw,
          buffer: 1_048_576
        ])

      :gen_tcp.send(sock, "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
      {:ok, _} = :gen_tcp.recv(sock, 0, 5000)

      # Build pipeline batch
      batch =
        for _ <- 1..pipeline_size do
          key = "#{prefix}:#{:rand.uniform(10_000)}"
          "*2\r\n$3\r\nGET\r\n$#{byte_size(key)}\r\n#{key}\r\n"
        end
        |> IO.iodata_to_binary()

      # Run for 5 seconds
      t_start = System.monotonic_time(:millisecond)
      total = pipeline_loop(sock, batch, pipeline_size, t_start, 5_000, 0)
      elapsed = System.monotonic_time(:millisecond) - t_start

      rps = div(total * 1000, max(elapsed, 1))
      avg_ns = if total > 0, do: div(elapsed * 1_000_000, total), else: 0

      IO.puts(
        "  #{String.pad_trailing("#{pipeline_size}", 15)} #{String.pad_leading("#{rps}", 12)} #{String.pad_leading("#{avg_ns}ns", 12)}"
      )

      :gen_tcp.close(sock)
      Process.sleep(200)
    end
  end

  defp pipeline_loop(sock, batch, pipeline_size, start, duration_ms, total) do
    if System.monotonic_time(:millisecond) - start > duration_ms do
      total
    else
      :gen_tcp.send(sock, batch)
      drain_responses(sock, pipeline_size)
      pipeline_loop(sock, batch, pipeline_size, start, duration_ms, total + pipeline_size)
    end
  end

  defp drain_responses(_sock, 0), do: :ok

  defp drain_responses(sock, remaining) do
    {:ok, data} = :gen_tcp.recv(sock, 0, 5000)
    # Count RESP3 responses in the data (each starts with $ or + or - or : or *)
    count = count_responses(data, 0)

    if count >= remaining do
      :ok
    else
      drain_responses(sock, remaining - count)
    end
  end

  defp count_responses(<<>>, count), do: count

  defp count_responses(<<"$", rest::binary>>, count) do
    # Bulk string: $N\r\n...data...\r\n — skip to after \r\n\r\n
    case skip_bulk_string(rest) do
      {:ok, remaining} -> count_responses(remaining, count + 1)
      :incomplete -> count + 1
    end
  end

  defp count_responses(<<"+", rest::binary>>, count) do
    case skip_line(rest) do
      {:ok, remaining} -> count_responses(remaining, count + 1)
      :incomplete -> count + 1
    end
  end

  defp count_responses(<<"-", rest::binary>>, count) do
    case skip_line(rest) do
      {:ok, remaining} -> count_responses(remaining, count + 1)
      :incomplete -> count + 1
    end
  end

  defp count_responses(<<":", rest::binary>>, count) do
    case skip_line(rest) do
      {:ok, remaining} -> count_responses(remaining, count + 1)
      :incomplete -> count + 1
    end
  end

  defp count_responses(<<_, rest::binary>>, count) do
    count_responses(rest, count)
  end

  defp skip_line(<<"\r\n", rest::binary>>), do: {:ok, rest}
  defp skip_line(<<>>), do: :incomplete
  defp skip_line(<<_, rest::binary>>), do: skip_line(rest)

  defp skip_bulk_string(data) do
    case skip_line(data) do
      {:ok, rest} ->
        # After $N\r\n, skip N bytes + \r\n
        case skip_line(rest) do
          {:ok, remaining} -> {:ok, remaining}
          :incomplete -> :incomplete
        end

      :incomplete ->
        :incomplete
    end
  end

  test "embedded vs TCP throughput scaling comparison" do
    prefix = "evst_#{System.unique_integer([:positive])}"
    port = FerricstoreServer.Listener.port()

    for i <- 1..10_000 do
      FerricStore.set("#{prefix}:#{i}", String.duplicate("v", 100))
    end

    Process.sleep(300)

    IO.puts("\n=== Embedded vs TCP Throughput Scaling ===\n")

    IO.puts(
      "  #{String.pad_trailing("Readers", 10)} #{String.pad_leading("Embedded", 12)} #{String.pad_leading("TCP", 12)} #{String.pad_leading("Ratio", 10)}"
    )

    IO.puts("  #{String.duplicate("-", 50)}")

    for num <- [1, 10, 50, 100] do
      # Embedded
      stop1 = :atomics.new(1, [])
      c1 = :counters.new(1, [:atomics])

      e =
        for _ <- 1..num do
          Task.async(fn ->
            l = fn l ->
              if :atomics.get(stop1, 1) == 1, do: throw(:stop)
              {:ok, _} = FerricStore.get("#{prefix}:#{:rand.uniform(10_000)}")
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
      Task.await_many(e, 30_000)
      e_rps = div(:counters.get(c1, 1), 5)

      Process.sleep(300)

      # TCP
      stop2 = :atomics.new(1, [])
      c2 = :counters.new(1, [:atomics])

      t =
        for _ <- 1..num do
          Task.async(fn ->
            {:ok, sock} =
              :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])

            :gen_tcp.send(sock, "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
            {:ok, _} = :gen_tcp.recv(sock, 0, 5000)

            l = fn l ->
              if :atomics.get(stop2, 1) == 1, do: throw(:stop)
              key = "#{prefix}:#{:rand.uniform(10_000)}"
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
      Task.await_many(t, 30_000)
      t_rps = div(:counters.get(c2, 1), 5)

      ratio = Float.round(e_rps / max(t_rps, 1), 2)

      IO.puts(
        "  #{String.pad_trailing("#{num}", 10)} #{String.pad_leading("#{e_rps}", 12)} #{String.pad_leading("#{t_rps}", 12)} #{String.pad_leading("#{ratio}x", 10)}"
      )

      Process.sleep(300)
    end
  end
end
