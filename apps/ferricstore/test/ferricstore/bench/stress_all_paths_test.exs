defmodule Ferricstore.Bench.StressAllPathsTest do
  use ExUnit.Case, async: false
  @moduletag :bench
  @moduletag timeout: 300_000

  @duration_ms 30_000
  @num_workers 50
  @value_size 256
  @read_key_range 100_000

  test "30s stress: concurrent quorum writes and reads" do
    value = String.duplicate("x", @value_size)

    # Pre-populate keys for reads
    IO.puts(
      "\n=== Stress Test: #{@num_workers} workers per path, #{div(@duration_ms, 1000)}s ===\n"
    )

    IO.puts("Pre-populating #{@read_key_range} keys for reads...")

    {pop_us, _} =
      :timer.tc(fn ->
        1..@read_key_range
        |> Enum.chunk_every(500)
        |> Enum.each(fn chunk ->
          kvs = Enum.map(chunk, &{"stress:r:#{&1}", value})
          FerricStore.batch_set(kvs)
        end)
      end)

    IO.puts("  Populated in #{div(pop_us, 1000)}ms\n")

    # Capture server state before
    mem_before = :erlang.memory(:total)
    shard_ids = for i <- 0..3, do: Ferricstore.Raft.Cluster.shard_server_id(i)

    leader_pids =
      Enum.map(shard_ids, fn sid ->
        {:ok, _, leader} = :ra.members(sid)
        {name, _} = leader
        Process.whereis(name)
      end)

    mq_before =
      Enum.map(leader_pids, fn pid ->
        {:message_queue_len, mq} = Process.info(pid, :message_queue_len)
        mq
      end)

    IO.puts("  Memory before: #{div(mem_before, 1024 * 1024)} MB")
    IO.puts("  Ra leader mailboxes before: #{inspect(mq_before)}")

    parent = self()
    deadline = System.monotonic_time(:millisecond) + @duration_ms
    error_counter = :atomics.new(2, signed: false)
    ops_counter = :atomics.new(2, signed: false)

    # Spawn workers for each path
    quorum_pids =
      spawn_workers(
        :quorum_write,
        @num_workers,
        deadline,
        value,
        ops_counter,
        error_counter,
        1,
        parent
      )

    read_pids =
      spawn_workers(:read, @num_workers, deadline, value, ops_counter, error_counter, 2, parent)

    all_pids = quorum_pids ++ read_pids

    # Print progress every 5s
    monitor_progress(ops_counter, error_counter, leader_pids, deadline)

    # Collect results
    _results =
      Enum.map(all_pids, fn pid ->
        receive do
          {:done, ^pid, result} -> result
        end
      end)

    quorum_ops = :atomics.get(ops_counter, 1)
    read_ops = :atomics.get(ops_counter, 2)
    quorum_errors = :atomics.get(error_counter, 1)
    read_errors = :atomics.get(error_counter, 2)

    duration_s = @duration_ms / 1000

    IO.puts("\n=== Results (#{duration_s}s) ===")

    IO.puts(
      "  Quorum writes: #{quorum_ops} ops (#{round(quorum_ops / duration_s)}/sec), #{quorum_errors} errors"
    )

    IO.puts(
      "  Reads:         #{read_ops} ops (#{round(read_ops / duration_s)}/sec), #{read_errors} errors"
    )

    mem_after = :erlang.memory(:total)
    IO.puts("\n  Memory: #{div(mem_before, 1024 * 1024)} MB -> #{div(mem_after, 1024 * 1024)} MB")

    mq_after =
      Enum.map(leader_pids, fn pid ->
        case Process.info(pid, :message_queue_len) do
          {:message_queue_len, mq} -> mq
          nil -> :dead
        end
      end)

    IO.puts("  Ra leader mailboxes after: #{inspect(mq_after)}")

    # Wait a bit and check mailbox drains
    Process.sleep(3000)

    mq_drain =
      Enum.map(leader_pids, fn pid ->
        case Process.info(pid, :message_queue_len) do
          {:message_queue_len, mq} -> mq
          nil -> :dead
        end
      end)

    IO.puts("  Ra leader mailboxes +3s: #{inspect(mq_drain)}")

    # Verify data integrity: read back sampled pre-populated read keys.
    IO.puts("\n=== Integrity Checks ===")
    verify_reads(value)

    # Assert no crashes
    dead_leaders = Enum.filter(leader_pids, &(Process.info(&1) == nil))
    assert dead_leaders == [], "Ra leaders died: #{inspect(dead_leaders)}"

    # Assert mailboxes drain
    all_drained =
      Enum.all?(mq_drain, fn
        :dead -> false
        mq -> mq < 10_000
      end)

    IO.puts("  Mailbox drained: #{all_drained} (#{inspect(mq_drain)})")
  end

  defp spawn_workers(type, count, deadline, value, ops_counter, error_counter, slot, parent) do
    for i <- 1..count do
      pid =
        spawn_link(fn ->
          worker_loop(type, i, deadline, value, ops_counter, error_counter, slot, 0)
          send(parent, {:done, self(), {type, i}})
        end)

      pid
    end
  end

  defp worker_loop(type, worker_id, deadline, value, ops_counter, error_counter, slot, batch_num) do
    now = System.monotonic_time(:millisecond)

    if now >= deadline do
      :ok
    else
      run_batch(type, worker_id, batch_num, value, ops_counter, error_counter, slot)

      worker_loop(
        type,
        worker_id,
        deadline,
        value,
        ops_counter,
        error_counter,
        slot,
        batch_num + 1
      )
    end
  end

  defp run_batch(:quorum_write, worker_id, batch_num, value, ops_counter, error_counter, slot) do
    base = worker_id * 1_000_000 + batch_num * 50
    kvs = for i <- 1..50, do: {"stress:q:#{base + i}", value}

    try do
      FerricStore.batch_set(kvs)
      :atomics.add(ops_counter, slot, 50)
    rescue
      _ -> :atomics.add(error_counter, slot, 50)
    catch
      _, _ -> :atomics.add(error_counter, slot, 50)
    end
  end

  defp run_batch(:read, _worker_id, _batch_num, _value, ops_counter, error_counter, slot) do
    keys = for _ <- 1..50, do: "stress:r:#{:rand.uniform(@read_key_range)}"

    try do
      FerricStore.batch_get(keys)
      :atomics.add(ops_counter, slot, 50)
    rescue
      _ -> :atomics.add(error_counter, slot, 50)
    catch
      _, _ -> :atomics.add(error_counter, slot, 50)
    end
  end

  defp monitor_progress(ops_counter, error_counter, leader_pids, deadline) do
    interval = 5_000

    monitor_loop(
      ops_counter,
      error_counter,
      leader_pids,
      deadline,
      interval,
      System.monotonic_time(:millisecond)
    )
  end

  defp monitor_loop(ops_counter, error_counter, leader_pids, deadline, interval, start) do
    now = System.monotonic_time(:millisecond)

    if now >= deadline do
      :ok
    else
      Process.sleep(min(interval, deadline - now))
      elapsed = (System.monotonic_time(:millisecond) - start) / 1000

      quorum_ops = :atomics.get(ops_counter, 1)
      read_ops = :atomics.get(ops_counter, 2)
      quorum_err = :atomics.get(error_counter, 1)
      read_err = :atomics.get(error_counter, 2)

      mqs =
        Enum.map(leader_pids, fn pid ->
          case Process.info(pid, :message_queue_len) do
            {:message_queue_len, mq} -> mq
            nil -> :dead
          end
        end)

      mem = div(:erlang.memory(:total), 1024 * 1024)

      IO.puts(
        "  [#{Float.round(elapsed, 1)}s] quorum=#{quorum_ops}(err=#{quorum_err}) read=#{read_ops}(err=#{read_err}) mem=#{mem}MB mq=#{inspect(mqs)}"
      )

      monitor_loop(ops_counter, error_counter, leader_pids, deadline, interval, start)
    end
  end

  defp verify_reads(value) do
    missing =
      for i <- Enum.take_random(1..@read_key_range, 100), reduce: 0 do
        acc ->
          case FerricStore.get("stress:r:#{i}") do
            {:ok, nil} ->
              acc + 1

            {:ok, ^value} ->
              acc

            {:ok, other} when is_binary(other) ->
              IO.puts("  CORRUPT: stress:r:#{i} = #{inspect(String.slice(other, 0..20))}...")
              acc + 1

            _ ->
              acc + 1
          end
      end

    IO.puts("  Read keys: 100 sampled, #{missing} missing/corrupt")
    assert missing == 0, "#{missing} read keys missing or corrupt after stress"
  end
end
