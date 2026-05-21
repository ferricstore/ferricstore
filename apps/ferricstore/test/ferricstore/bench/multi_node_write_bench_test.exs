defmodule Ferricstore.Bench.MultiNodeWriteBenchTest do
  @moduledoc """
  Multi-node write throughput benchmark using real peer BEAM nodes.

  Measures Raft replication cost across 1, 3, and 5-node clusters where
  each ra member lives on a separate BEAM node (via OTP :peer). This
  captures actual cross-node message passing overhead that single-node
  multi-member benchmarks miss.

  ## Running

      cd apps/ferricstore && mix test test/ferricstore/bench/multi_node_write_bench_test.exs --include bench --timeout 600000
  """

  use ExUnit.Case, async: false

  @moduletag :bench
  @moduletag timeout: 300_000

  require Logger

  @machine_mod Ferricstore.Test.SimpleKvMachine
  @bench_duration_ms 10_000

  # ---------------------------------------------------------------------------
  # Setup: start peer nodes
  # ---------------------------------------------------------------------------

  setup_all do
    ensure_distribution!()
    peers = start_peers(5)

    on_exit(fn ->
      Enum.each(peers, fn {_name, peer_pid} ->
        try do
          :peer.stop(peer_pid)
        catch
          _, _ -> :ok
        end
      end)
    end)

    %{peers: peers}
  end

  # ---------------------------------------------------------------------------
  # Benchmark
  # ---------------------------------------------------------------------------

  test "write throughput: 1-node vs 3-node vs 5-node (real peer nodes)", %{peers: peers} do
    header = """

    === Multi-Node Raft Write Throughput (#{div(@bench_duration_ms, 1000)}s each, real peer BEAM nodes) ===

      #{pad("Cluster", 10)} #{pad("Writers", 10)} #{rpad("Writes/sec", 12)} #{rpad("Per-writer", 12)} #{rpad("p50 us", 10)} #{rpad("p99 us", 10)}
      #{String.duplicate("-", 72)}
    """

    IO.puts(header)

    for {cluster_size, label} <- [{1, "1-node"}, {3, "3-node"}, {5, "5-node"}] do
      selected = Enum.take(peers, cluster_size)

      for num_writers <- [1, 10, 50] do
        result = run_cluster_bench(selected, num_writers)
        print_row(label, num_writers, result)
      end

      IO.puts("")
    end
  end

  # ---------------------------------------------------------------------------
  # Core benchmark runner
  # ---------------------------------------------------------------------------

  defp run_cluster_bench(peers, num_writers) do
    id = :erlang.unique_integer([:positive])
    ra_sys = :"mnb_#{id}"
    cluster_name = :"mnbc_#{id}"
    prefix = "m#{id}_"
    peer_nodes = Enum.map(peers, fn {name, _pid} -> name end)

    # Start ra system on each peer
    Enum.each(peer_nodes, fn nd ->
      dir = Path.join(System.tmp_dir!(), "mn_bench_#{id}_#{nd}")
      :rpc.call(nd, File, :mkdir_p!, [dir])

      names = :rpc.call(nd, :ra_system, :derive_names, [ra_sys])

      config = %{
        name: ra_sys,
        names: names,
        data_dir: to_charlist(dir),
        wal_data_dir: to_charlist(dir),
        segment_max_entries: 8192
      }

      case :rpc.call(nd, :ra_system, :start, [config]) do
        {:ok, _} -> :ok
        {:error, {:already_started, _}} -> :ok
      end
    end)

    # One member per peer node
    members =
      peer_nodes
      |> Enum.with_index(1)
      |> Enum.map(fn {nd, i} -> {:"#{prefix}#{i}", nd} end)

    machine = {:module, @machine_mod, %{}}
    {:ok, started, _} = :ra.start_cluster(ra_sys, cluster_name, machine, members)
    assert length(started) == length(members)

    leader = wait_for_leader(members)
    Process.sleep(300)

    # Run writers
    counter = :counters.new(1, [:atomics])
    lat_tab = :ets.new(:lat, [:bag, :public])
    stop = :atomics.new(1, [])

    tasks =
      for w <- 1..num_writers do
        Task.async(fn ->
          writer_loop(leader, stop, counter, lat_tab, prefix, w, 0)
        end)
      end

    Process.sleep(@bench_duration_ms)
    :atomics.put(stop, 1, 1)
    Task.await_many(tasks, 30_000)

    total = :counters.get(counter, 1)
    duration_s = @bench_duration_ms / 1000
    wps = trunc(total / duration_s)
    per_writer = div(wps, max(num_writers, 1))

    latencies =
      :ets.match(lat_tab, {:lat, :"$1"})
      |> List.flatten()
      |> Enum.sort()

    :ets.delete(lat_tab)

    p50 = percentile(latencies, 50)
    p99 = percentile(latencies, 99)

    # Cleanup
    Enum.each(members, fn sid ->
      try do
        :ra.stop_server(ra_sys, sid)
      catch
        _, _ -> :ok
      end

      try do
        :ra.force_delete_server(ra_sys, sid)
      catch
        _, _ -> :ok
      end
    end)

    Process.sleep(100)

    %{total: total, wps: wps, per_writer: per_writer, p50: p50, p99: p99}
  end

  # ---------------------------------------------------------------------------
  # Writer loop
  # ---------------------------------------------------------------------------

  defp writer_loop(leader, stop, counter, lat_tab, prefix, w, i) do
    if :atomics.get(stop, 1) == 1 do
      :ok
    else
      corr = make_ref()
      cmd = {:put, "#{prefix}w#{w}:#{i}", "v#{i}"}
      start_us = System.monotonic_time(:microsecond)

      case :ra.pipeline_command(leader, cmd, corr, :low) do
        :ok ->
          receive do
            {:ra_event, _, {:applied, applied}} ->
              case List.keyfind(applied, corr, 0) do
                {^corr, _} ->
                  elapsed = System.monotonic_time(:microsecond) - start_us
                  :counters.add(counter, 1, 1)
                  :ets.insert(lat_tab, {:lat, elapsed})

                nil ->
                  :ok
              end

            {:ra_event, _, _} ->
              :ok
          after
            5_000 -> :ok
          end

        _ ->
          :ok
      end

      writer_loop(leader, stop, counter, lat_tab, prefix, w, i + 1)
    end
  end

  # ---------------------------------------------------------------------------
  # Peer node management
  # ---------------------------------------------------------------------------

  defp start_peers(n) do
    code_paths = Enum.flat_map(:code.get_path(), fn p -> [~c"-pa", p] end)

    Enum.map(1..n, fn i ->
      name = :"mnbench_#{:erlang.unique_integer([:positive])}_#{i}"

      {:ok, peer_pid, node_name} =
        :peer.start(%{
          name: name,
          args:
            code_paths ++
              [~c"-connect_all", ~c"false", ~c"-setcookie", Atom.to_charlist(Node.get_cookie())]
        })

      # Start ra on the peer
      {:ok, _} = :rpc.call(node_name, Application, :ensure_all_started, [:ra])

      # Load the state machine module on the peer
      {mod, bin, file} = :code.get_object_code(@machine_mod)
      {:module, ^mod} = :rpc.call(node_name, :code, :load_binary, [mod, file, bin])

      # Connect peer to the test runner
      :rpc.call(node_name, Node, :connect, [node()])

      {node_name, peer_pid}
    end)
  end

  defp ensure_distribution! do
    Ferricstore.Test.ShardHelpers.ensure_distribution_started!(:mnbench_runner)
  end

  # ---------------------------------------------------------------------------
  # Ra helpers
  # ---------------------------------------------------------------------------

  defp wait_for_leader(members, attempts \\ 30) do
    [first | _] = members

    case :ra.members(first, 5_000) do
      {:ok, _, leader} when is_tuple(leader) ->
        leader

      _ when attempts > 0 ->
        Process.sleep(200)
        wait_for_leader(members, attempts - 1)

      _ ->
        raise "Timeout waiting for leader among #{inspect(members)}"
    end
  end

  # ---------------------------------------------------------------------------
  # Stats / formatting
  # ---------------------------------------------------------------------------

  defp percentile([], _p), do: 0

  defp percentile(sorted, p) do
    len = length(sorted)
    idx = max(0, round(len * p / 100) - 1)
    Enum.at(sorted, min(idx, len - 1))
  end

  defp pad(s, w), do: String.pad_trailing(s, w)
  defp rpad(s, w), do: String.pad_leading(s, w)

  defp print_row(label, writers, %{wps: wps, per_writer: pw, p50: p50, p99: p99}) do
    IO.puts(
      "  #{pad(label, 10)} #{pad("#{writers}", 10)} #{rpad(format_num(wps), 12)} #{rpad(format_num(pw), 12)} #{rpad(format_us(p50), 10)} #{rpad(format_us(p99), 10)}"
    )
  end

  defp format_num(n) do
    n
    |> Integer.to_string()
    |> String.graphemes()
    |> Enum.reverse()
    |> Enum.chunk_every(3)
    |> Enum.map_join(",", &Enum.join/1)
    |> String.reverse()
  end

  defp format_us(0), do: "N/A"
  defp format_us(us) when us < 1_000, do: "#{us}"
  defp format_us(us), do: "#{Float.round(us / 1_000, 1)}ms"

  # ---------------------------------------------------------------------------
  # Replication lag test
  # ---------------------------------------------------------------------------

  test "async replication: fire-and-forget bulk writes, verify follower catches up", %{
    peers: peers
  } do
    selected = Enum.take(peers, 3)

    id = :erlang.unique_integer([:positive])
    ra_sys = :"lag_#{id}"
    cluster_name = :"lagc_#{id}"
    prefix = "lag#{id}_"
    peer_nodes = Enum.map(selected, fn {name, _} -> name end)

    Enum.each(peer_nodes, fn nd ->
      dir = Path.join(System.tmp_dir!(), "lag_bench_#{id}_#{nd}")
      :rpc.call(nd, File, :mkdir_p!, [dir])
      names = :rpc.call(nd, :ra_system, :derive_names, [ra_sys])

      config = %{
        name: ra_sys,
        names: names,
        data_dir: to_charlist(dir),
        wal_data_dir: to_charlist(dir),
        segment_max_entries: 8192
      }

      case :rpc.call(nd, :ra_system, :start, [config]) do
        {:ok, _} -> :ok
        {:error, {:already_started, _}} -> :ok
      end
    end)

    members =
      peer_nodes
      |> Enum.with_index(1)
      |> Enum.map(fn {nd, i} -> {:"#{prefix}#{i}", nd} end)

    machine = {:module, @machine_mod, %{}}
    {:ok, started, _} = :ra.start_cluster(ra_sys, cluster_name, machine, members)
    assert length(started) == 3

    leader = wait_for_leader(members)
    followers = Enum.reject(members, &(&1 == leader))

    Process.sleep(500)

    IO.puts("\n=== Async Replication Test (3-node, fire-and-forget) ===")
    IO.puts("  Leader: #{inspect(leader)}")
    IO.puts("  Followers: #{inspect(followers)}\n")

    # Test with different value sizes
    for {value_size, num_writes} <- [
          {100, 100_000},
          {1_000, 50_000},
          {10_000, 10_000},
          {50_000, 5_000},
          {100_000, 2_000},
          {500_000, 500}
        ] do
      test_value_size(leader, followers, prefix, value_size, num_writes)
    end

    # Cleanup
    Enum.each(members, fn sid ->
      try do
        :ra.stop_server(ra_sys, sid)
      catch
        _, _ -> :ok
      end

      try do
        :ra.force_delete_server(ra_sys, sid)
      catch
        _, _ -> :ok
      end
    end)
  end

  defp test_value_size(leader, followers, prefix, value_size, num_writes) do
    value = String.duplicate("x", value_size)

    size_label =
      cond do
        value_size >= 1000 -> "#{div(value_size, 1000)}KB"
        true -> "#{value_size}B"
      end

    IO.puts("\n  --- Value size: #{size_label}, #{num_writes} writes ---")

    # Get leader PID for mailbox monitoring
    {leader_name, leader_node} = leader
    leader_pid = :rpc.call(leader_node, Process, :whereis, [leader_name])

    mq_before =
      case :rpc.call(leader_node, Process, :info, [leader_pid, :message_queue_len]) do
        {:message_queue_len, n} -> n
        _ -> -1
      end

    IO.puts("  Leader mailbox before: #{mq_before}")

    # Blast all writes fire-and-forget
    write_start = System.monotonic_time(:microsecond)

    size_prefix = "#{prefix}s#{value_size}_"

    for i <- 1..num_writes do
      :ra.pipeline_command(leader, {:put, "#{size_prefix}k#{i}", value}, make_ref(), :low)
    end

    write_elapsed = System.monotonic_time(:microsecond) - write_start

    IO.puts(
      "  #{num_writes} fire-and-forget writes submitted in #{write_elapsed}us (#{div(write_elapsed, num_writes)}us/op)"
    )

    # Check leader mailbox immediately after blast
    mq_after =
      case :rpc.call(leader_node, Process, :info, [leader_pid, :message_queue_len]) do
        {:message_queue_len, n} -> n
        _ -> -1
      end

    IO.puts("  Leader mailbox after blast: #{mq_after}")

    # Check leader alive
    leader_alive = :rpc.call(leader_node, Process, :alive?, [leader_pid])
    IO.puts("  Leader alive after blast: #{leader_alive}")

    # Check follower state at increasing intervals
    IO.puts("  Checking follower replication over time:")

    IO.puts(
      "  #{pad("Delay", 12)} #{rpad("On leader", 12)} #{rpad("On follower1", 14)} #{rpad("On follower2", 14)} #{rpad("Lost", 8)}"
    )

    IO.puts("  #{String.duplicate("-", 65)}")

    for delay_ms <- [0, 100, 500, 1000, 2000, 5000, 10_000] do
      Process.sleep(delay_ms)

      # Query leader state (via consistent_query — blocks but gives accurate count)
      leader_count =
        case :ra.local_query(leader, &Ferricstore.Test.SimpleKvMachine.count/1) do
          {:ok, {_, count}, _} -> count
          _ -> -1
        end

      # Query each follower
      follower_counts =
        Enum.map(followers, fn f ->
          case :ra.local_query(f, &Ferricstore.Test.SimpleKvMachine.count/1) do
            {:ok, {_, count}, _} -> count
            _ -> -1
          end
        end)

      [f1, f2] = follower_counts
      lost = num_writes - min(f1, f2)

      IO.puts(
        "  #{pad("#{delay_ms}ms", 12)} #{rpad("#{leader_count}", 12)} #{rpad("#{f1}", 14)} #{rpad("#{f2}", 14)} #{rpad("#{lost}", 8)}"
      )
    end

    # Wait for full replication of this batch
    Process.sleep(5000)

    final_counts =
      Enum.map([leader | followers], fn m ->
        case :ra.local_query(m, &Ferricstore.Test.SimpleKvMachine.count/1) do
          {:ok, {_, count}, _} -> count
          _ -> -1
        end
      end)

    all_same = length(Enum.uniq(final_counts)) == 1
    IO.puts("  Final counts: #{inspect(final_counts)} — all consistent: #{all_same}")
  end
end
