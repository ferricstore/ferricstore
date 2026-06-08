defmodule FerricstoreServer.Bench.TcpActiveModeBenchTest do
  @moduledoc """
  Benchmarks TCP read throughput across different socket active modes
  and connection counts.

  Compares: active:true vs active:100 vs active:500

  Run: mix test apps/ferricstore_server/test/ferricstore_server/bench/tcp_active_mode_bench_test.exs --include bench
  """
  use ExUnit.Case, async: false
  @moduletag :global_state
  @moduletag :bench
  @moduletag timeout: 600_000

  @test_duration_sec 5
  @pipeline_size 50
  @num_keys 10_000
  @value_size 100

  test "compare active modes across connection counts" do
    prefix = "abm_#{System.unique_integer([:positive])}"

    # Pre-populate keys
    IO.puts("\nPopulating #{@num_keys} keys...")
    for i <- 1..@num_keys do
      FerricStore.set("#{prefix}:#{i}", String.duplicate("v", @value_size))
    end
    Process.sleep(500)

    modes = [true, 100, 200, 300, 500]
    # Results on macOS M-series (same-machine, client competes for CPU):
    #   true:  148K→84K→77K→80K→82K  (1→10→50→100→200 conns)
    #   100:   144K→84K→81K→80K→79K
    #   200:   130K→76K→79K→81K→81K
    #   300:   133K→74K→77K→79K→80K
    #   500:   123K→75K→77K→78K→81K
    # All converge at 50+ connections. Real difference shows on separate machines.
    conn_counts = [1, 10, 50, 100, 200]

    IO.puts("\n=== TCP Active Mode Benchmark ===")
    IO.puts("Duration: #{@test_duration_sec}s per test, Pipeline: #{@pipeline_size}")
    IO.puts("")

    # Header
    header =
      "  #{String.pad_trailing("Mode", 10)}" <>
      Enum.map_join(conn_counts, "", fn c ->
        String.pad_leading("#{c}c", 12)
      end)
    IO.puts(header)
    IO.puts("  #{String.duplicate("-", 10 + length(conn_counts) * 12)}")

    for mode <- modes do
      # Start a listener with this active mode
      listener_name = :"bench_listener_#{inspect(mode)}"

      {:ok, _} =
        :ranch.start_listener(
          listener_name,
          :ranch_tcp,
          %{socket_opts: [port: 0, nodelay: true]},
          FerricstoreServer.Connection,
          %{}
        )

      port = :ranch.get_port(listener_name)

      # Temporarily set the active mode
      old_mode = Application.get_env(:ferricstore, :socket_active_mode, 100)
      Application.put_env(:ferricstore, :socket_active_mode, mode)

      # Also need to clear the cached raw_store since active_mode is baked into connection init
      :persistent_term.erase(:ferricstore_raw_store)

      results =
        for num_conn <- conn_counts do
          rps = run_bench(port, prefix, num_conn, @pipeline_size, @test_duration_sec)
          Process.sleep(200)
          rps
        end

      # Print row
      row =
        "  #{String.pad_trailing("#{inspect(mode)}", 10)}" <>
        Enum.map_join(results, "", fn rps ->
          String.pad_leading("#{div(rps, 1000)}K", 12)
        end)
      IO.puts(row)

      Application.put_env(:ferricstore, :socket_active_mode, old_mode)
      :ranch.stop_listener(listener_name)
      Process.sleep(500)
    end

    IO.puts("")
    IO.puts("Lower per-connection throughput at high connection count")
    IO.puts("indicates contention. Flat = good scalability.")
    IO.puts("")

    # Also print per-connection numbers
    IO.puts("\n  Per-connection reads/sec:")
    header2 =
      "  #{String.pad_trailing("Mode", 10)}" <>
      Enum.map_join(conn_counts, "", fn c ->
        String.pad_leading("#{c}c", 12)
      end)
    IO.puts(header2)
    IO.puts("  #{String.duplicate("-", 10 + length(conn_counts) * 12)}")
    # Re-run would be needed for per-conn, but we can infer from above
  end

  defp run_bench(port, prefix, num_conn, pipeline, duration_sec) do
    # Start pg group for ACL if not running
    pg_group = FerricstoreServer.Connection.acl_pg_group()
    case :pg.start_link(pg_group) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

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

          batch = build_get_pipeline(prefix, pipeline)
          resp_size = pipeline * 108

          loop = fn loop_fn ->
            if :atomics.get(stop, 1) == 1, do: throw(:stop)
            :gen_tcp.send(sock, batch)
            drain_bytes(sock, resp_size)
            :counters.add(counter, 1, pipeline)
            loop_fn.(loop_fn)
          end

          try do
            loop.(loop)
          catch
            :throw, :stop -> :ok
          end

          :gen_tcp.close(sock)
        end)
      end

    Process.sleep(duration_sec * 1000)
    :atomics.put(stop, 1, 1)
    Task.await_many(workers, 30_000)

    total = :counters.get(counter, 1)
    div(total, duration_sec)
  end

  defp build_get_pipeline(prefix, count) do
    for _ <- 1..count do
      key = "#{prefix}:#{:rand.uniform(@num_keys)}"
      "*2\r\n$3\r\nGET\r\n$#{byte_size(key)}\r\n#{key}\r\n"
    end
    |> IO.iodata_to_binary()
  end

  defp drain_bytes(_sock, remaining) when remaining <= 0, do: :ok

  defp drain_bytes(sock, remaining) do
    case :gen_tcp.recv(sock, 0, 10_000) do
      {:ok, data} -> drain_bytes(sock, remaining - byte_size(data))
      {:error, _} -> :ok
    end
  end
end
