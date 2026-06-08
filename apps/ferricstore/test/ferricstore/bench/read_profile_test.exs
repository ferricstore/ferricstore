defmodule Ferricstore.Bench.ReadProfileTest do
  use ExUnit.Case, async: false
  @moduletag :bench
  @moduletag timeout: 120_000

  test "profile read path - every step timed" do
    # Pre-populate 1000 keys directly into ETS (bypass Raft for speed)
    prefix = "rp_#{System.unique_integer([:positive])}"

    for i <- 1..1000 do
      key = "#{prefix}:#{i}"
      idx = :erlang.phash2(key, 4)
      keydir = :"keydir_#{idx}"
      :ets.insert(keydir, {key, "value_#{i}", 0, Ferricstore.Store.LFU.initial(), 0, 0, 8})
    end

    # Profile 10000 sequential GETs - time each step
    timings =
      for _i <- 1..10_000 do
        key = "#{prefix}:#{:rand.uniform(1000)}"

        t0 = :erlang.monotonic_time(:nanosecond)

        # Step 1: sandbox_key (removed; identity)
        resolved = key
        t1 = :erlang.monotonic_time(:nanosecond)

        # Step 2: shard_for (hash + extract_hash_tag)
        idx = Ferricstore.Store.Router.shard_for(FerricStore.Instance.get(:default), resolved)
        t2 = :erlang.monotonic_time(:nanosecond)

        # Step 3: ETS lookup
        keydir = :"keydir_#{idx}"
        result = :ets.lookup(keydir, resolved)
        t3 = :erlang.monotonic_time(:nanosecond)

        # Step 4: pattern match + expiry check
        _value =
          case result do
            [{^resolved, val, 0, _lfu, _fid, _off, _vsize}] when val != nil -> val
            [{^resolved, val, exp, _lfu, _fid, _off, _vsize}] when exp > 0 and val != nil -> val
            _ -> nil
          end

        t4 = :erlang.monotonic_time(:nanosecond)

        # Step 5: LFU touch
        case result do
          [{^resolved, _val, _exp, lfu, _fid, _off, _vsize}] ->
            Ferricstore.Store.LFU.touch(keydir, resolved, lfu)

          _ ->
            :ok
        end

        t5 = :erlang.monotonic_time(:nanosecond)

        # Step 6: Stats recording (if any)
        t6 = :erlang.monotonic_time(:nanosecond)

        %{
          sandbox: t1 - t0,
          shard_for: t2 - t1,
          ets_lookup: t3 - t2,
          pattern_match: t4 - t3,
          lfu_touch: t5 - t4,
          total: t6 - t0
        }
      end

    IO.puts("\n=== Read Path Profile (10000 sequential GETs, nanoseconds) ===\n")

    IO.puts(
      "  #{String.pad_trailing("Step", 20)} #{String.pad_leading("avg", 8)} #{String.pad_leading("p50", 8)} #{String.pad_leading("p99", 8)} #{String.pad_leading("min", 8)} #{String.pad_leading("max", 8)}  % of total"
    )

    IO.puts("  #{String.duplicate("-", 85)}")

    total_avg = div(Enum.sum(Enum.map(timings, & &1.total)), 10_000)

    for step <- [:sandbox, :shard_for, :ets_lookup, :pattern_match, :lfu_touch, :total] do
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
        "  #{String.pad_trailing(to_string(step), 20)} #{String.pad_leading("#{avg}ns", 8)} #{String.pad_leading("#{p50}ns", 8)} #{String.pad_leading("#{p99}ns", 8)} #{String.pad_leading("#{min_v}ns", 8)} #{String.pad_leading("#{max_v}ns", 8)}  #{pct}%"
      )
    end
  end

  test "profile read path - 50 concurrent readers" do
    prefix = "rpc_#{System.unique_integer([:positive])}"

    for i <- 1..10_000 do
      key = "#{prefix}:#{i}"
      idx = :erlang.phash2(key, 4)
      keydir = :"keydir_#{idx}"
      :ets.insert(keydir, {key, "value_#{i}", 0, Ferricstore.Store.LFU.initial(), 0, 0, 8})
    end

    stop = :atomics.new(1, [])
    counter = :counters.new(1, [:atomics])

    # Also poll the schedulers
    sched_start = :erlang.statistics(:scheduler_wall_time)

    readers =
      for _i <- 1..50 do
        Task.async(fn ->
          l = fn l ->
            if :atomics.get(stop, 1) == 1, do: throw(:stop)
            {:ok, _} = FerricStore.get("#{prefix}:#{:rand.uniform(10_000)}")
            :counters.add(counter, 1, 1)
            l.(l)
          end

          try do
            l.(l)
          catch
            :throw, :stop -> :ok
          end
        end)
      end

    Process.sleep(10_000)
    :atomics.put(stop, 1, 1)
    Task.await_many(readers, 30_000)

    sched_end = :erlang.statistics(:scheduler_wall_time)

    total = :counters.get(counter, 1)

    # Compute scheduler utilization
    utils =
      Enum.zip(Enum.sort(sched_start), Enum.sort(sched_end))
      |> Enum.map(fn {{_id, a1, t1}, {_id2, a2, t2}} ->
        if t2 - t1 > 0, do: (a2 - a1) / (t2 - t1) * 100, else: 0.0
      end)

    avg_util = Float.round(Enum.sum(utils) / length(utils), 1)

    IO.puts("\n=== Concurrent Read Profile (50 readers, 10s) ===")
    IO.puts("  Total reads: #{total}")
    IO.puts("  Reads/sec: #{div(total, 10)}")
    IO.puts("  Avg per read: #{div(10_000_000_000, max(total, 1))}ns")
    IO.puts("  Scheduler utilization: #{avg_util}%")
  end

  test "profile hot key vs random key" do
    prefix = "rhk_#{System.unique_integer([:positive])}"

    # Populate 10000 keys
    for i <- 1..10_000 do
      key = "#{prefix}:#{i}"
      idx = :erlang.phash2(key, 4)
      keydir = :"keydir_#{idx}"
      :ets.insert(keydir, {key, "value_#{i}", 0, Ferricstore.Store.LFU.initial(), 0, 0, 8})
    end

    stop = :atomics.new(1, [])

    # Test 1: random keys
    counter_random = :counters.new(1, [:atomics])

    random_readers =
      for _ <- 1..50 do
        Task.async(fn ->
          l = fn l ->
            if :atomics.get(stop, 1) == 1, do: throw(:stop)
            {:ok, _} = FerricStore.get("#{prefix}:#{:rand.uniform(10_000)}")
            :counters.add(counter_random, 1, 1)
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
    :atomics.put(stop, 1, 1)
    Task.await_many(random_readers, 30_000)
    random_total = :counters.get(counter_random, 1)

    # Reset
    :atomics.put(stop, 1, 0)
    Process.sleep(100)

    # Test 2: same hot key
    hot_key = "#{prefix}:42"
    counter_hot = :counters.new(1, [:atomics])

    hot_readers =
      for _ <- 1..50 do
        Task.async(fn ->
          l = fn l ->
            if :atomics.get(stop, 1) == 1, do: throw(:stop)
            {:ok, _} = FerricStore.get(hot_key)
            :counters.add(counter_hot, 1, 1)
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
    :atomics.put(stop, 1, 1)
    Task.await_many(hot_readers, 30_000)
    hot_total = :counters.get(counter_hot, 1)

    IO.puts("\n=== Hot Key vs Random Key (50 readers, 5s each) ===")
    IO.puts("  Random keys:  #{div(random_total, 5)} reads/sec")
    IO.puts("  Hot key:      #{div(hot_total, 5)} reads/sec")
    IO.puts("  Ratio:        #{Float.round(hot_total / max(random_total, 1), 2)}x")
  end

  test "profile value size impact on reads" do
    prefix = "rvs_#{System.unique_integer([:positive])}"

    for {size, label} <- [
          {10, "10B"},
          {100, "100B"},
          {1000, "1KB"},
          {10_000, "10KB"},
          {50_000, "50KB"}
        ] do
      value = String.duplicate("x", size)
      key = "#{prefix}:#{label}"
      idx = :erlang.phash2(key, 4)
      keydir = :"keydir_#{idx}"
      :ets.insert(keydir, {key, value, 0, Ferricstore.Store.LFU.initial(), 0, 0, size})

      # Time 10000 reads
      times =
        for _ <- 1..10_000 do
          t0 = :erlang.monotonic_time(:nanosecond)
          {:ok, _} = FerricStore.get(key)
          t1 = :erlang.monotonic_time(:nanosecond)
          t1 - t0
        end

      sorted = Enum.sort(times)
      avg = div(Enum.sum(times), 10_000)
      p50 = Enum.at(sorted, 5000)
      p99 = Enum.at(sorted, 9900)

      IO.puts(
        "  #{String.pad_trailing(label, 8)} avg=#{avg}ns  p50=#{p50}ns  p99=#{p99}ns  reads/sec=#{div(1_000_000_000, max(avg, 1))}"
      )
    end
  end
end
