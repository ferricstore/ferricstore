defmodule Ferricstore.Bench.EmbeddedWriteProfileTest do
  use ExUnit.Case, async: false
  @moduletag :bench
  @moduletag timeout: 120_000

  @iterations 100_000

  test "profile embedded SET - every layer timed" do
    alias Ferricstore.Store.Router

    prefix = "emb_wprof_#{System.unique_integer([:positive])}"
    value = String.duplicate("v", 100)

    IO.puts("\n=== Embedded SET Profile (#{@iterations} iterations) ===\n")

    # 1. Full FerricStore.set
    {full_us, _} = :timer.tc(fn ->
      for i <- 1..@iterations, do: FerricStore.set("#{prefix}:full:#{i}", value)
    end)

    # 2. Router.put (skip sandbox_key + opts parsing)
    {router_us, _} = :timer.tc(fn ->
      for i <- 1..@iterations, do: Router.put(FerricStore.Instance.get(:default), "#{prefix}:rtr:#{i}", value, 0)
    end)

    # 3. sandbox_key (removed; identity function as baseline)
    {sandbox_us, _} = :timer.tc(fn ->
      for _ <- 1..@iterations, do: "somekey"
    end)

    # 4. shard_for
    {hash_us, _} = :timer.tc(fn ->
      for _ <- 1..@iterations, do: Router.shard_for(FerricStore.Instance.get(:default), "somekey")
    end)

    # 5. Direct ETS insert (no Raft, no Bitcask, just ETS)
    keydir = :keydir_0
    {ets_us, _} = :timer.tc(fn ->
      for i <- 1..@iterations do
        :ets.insert(keydir, {"#{prefix}:ets:#{i}", value, 0, 0, 0, 0, 0})
      end
    end)

    full_ns = div(full_us * 1000, @iterations)
    router_ns = div(router_us * 1000, @iterations)
    sandbox_ns = div(sandbox_us * 1000, @iterations)
    hash_ns = div(hash_us * 1000, @iterations)
    ets_ns = div(ets_us * 1000, @iterations)

    IO.puts("  #{String.pad_trailing("Layer", 40)} #{String.pad_leading("Time/op", 10)} #{String.pad_leading("% of full", 10)}")
    IO.puts("  #{String.duplicate("-", 65)}")

    layers = [
      {"Direct ETS insert", ets_ns},
      {"shard_for (crc32)", hash_ns},
      {"sandbox_key", sandbox_ns},
      {"--- Router.put total ---", router_ns},
      {"=== FerricStore.set total ===", full_ns},
      {"Overhead (full - ETS)", full_ns - ets_ns}
    ]

    for {label, ns} <- layers do
      pct = if full_ns > 0, do: Float.round(ns / full_ns * 100, 1), else: 0.0
      IO.puts("  #{String.pad_trailing(label, 40)} #{String.pad_leading("#{ns}ns", 10)} #{String.pad_leading("#{pct}%", 10)}")
    end
  end

  test "throughput: single writer vs N concurrent writers (quorum)" do
    prefix = "emb_wthr_q_#{System.unique_integer([:positive])}"
    value = String.duplicate("v", 100)

    IO.puts("\n=== Embedded SET Throughput (Quorum / Raft ON) ===\n")
    IO.puts("  #{String.pad_trailing("Writers", 10)} #{String.pad_leading("Writes/sec", 12)} #{String.pad_leading("Per-writer", 12)} #{String.pad_leading("us/op", 10)}")
    IO.puts("  #{String.duplicate("-", 50)}")

    for num_writers <- [1, 2, 4, 8, 16, 32, 50] do
      counter = :counters.new(1, [:atomics])
      stop = :atomics.new(1, [])

      tasks = for w <- 1..num_writers do
        Task.async(fn ->
          l = fn l, i ->
            if :atomics.get(stop, 1) == 1, do: throw(:stop)
            FerricStore.set("#{prefix}:#{w}:#{i}", value)
            :counters.add(counter, 1, 1)
            l.(l, i + 1)
          end
          try do l.(l, 0) catch :throw, :stop -> :ok end
        end)
      end

      Process.sleep(3_000)
      :atomics.put(stop, 1, 1)
      Task.await_many(tasks, 10_000)

      total = :counters.get(counter, 1)
      wps = div(total, 3)
      per_writer = div(wps, max(num_writers, 1))
      us_per_op = if wps > 0, do: div(1_000_000, wps), else: 0

      IO.puts("  #{String.pad_trailing("#{num_writers}", 10)} #{String.pad_leading("#{wps}", 12)} #{String.pad_leading("#{per_writer}", 12)} #{String.pad_leading("#{us_per_op}", 10)}")
    end
  end

  test "comparison: FerricStore.set vs Router.put vs direct ETS (50 writers, 3s)" do
    value = String.duplicate("v", 100)

    IO.puts("\n=== Embedded SET Comparison (50 concurrent writers, 3s) ===\n")
    IO.puts("  #{String.pad_trailing("Method", 30)} #{String.pad_leading("Writes/sec", 12)} #{String.pad_leading("us/op", 10)}")
    IO.puts("  #{String.duplicate("-", 55)}")

    for {label, fun_maker} <- [
      {"FerricStore.set (quorum)", fn prefix ->
        fn w, i -> FerricStore.set("#{prefix}:#{w}:#{i}", value) end
      end},
      {"Router.put (quorum)", fn prefix ->
        fn w, i -> Ferricstore.Store.Router.put(FerricStore.Instance.get(:default), "#{prefix}:#{w}:#{i}", value, 0) end
      end},
      {"Direct ETS insert", fn prefix ->
        keydir = :keydir_0
        fn _w, i -> :ets.insert(keydir, {"#{prefix}:ets:#{i}", value, 0, 0, 0, 0, 0}) end
      end}
    ] do
      prefix = "emb_wcmp_#{System.unique_integer([:positive])}"
      write_fn = fun_maker.(prefix)
      counter = :counters.new(1, [:atomics])
      stop = :atomics.new(1, [])

      tasks = for w <- 1..50 do
        Task.async(fn ->
          l = fn l, i ->
            if :atomics.get(stop, 1) == 1, do: throw(:stop)
            write_fn.(w, i)
            :counters.add(counter, 1, 1)
            l.(l, i + 1)
          end
          try do l.(l, 0) catch :throw, :stop -> :ok end
        end)
      end

      Process.sleep(3_000)
      :atomics.put(stop, 1, 1)
      Task.await_many(tasks, 10_000)

      total = :counters.get(counter, 1)
      wps = div(total, 3)
      us_per_op = if wps > 0, do: div(1_000_000, wps), else: 0

      IO.puts("  #{String.pad_trailing(label, 30)} #{String.pad_leading("#{wps}", 12)} #{String.pad_leading("#{us_per_op}", 10)}")
    end
  end
end
