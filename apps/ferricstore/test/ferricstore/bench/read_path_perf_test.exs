defmodule Ferricstore.Bench.ReadPathPerfTest do
  @moduledoc """
  Microbenchmark for the Router.get hot-read path.

  Measures per-operation cost of each layer in the read path and overall
  throughput at 50 concurrent readers. Used to validate read-path optimizations.
  """
  use ExUnit.Case, async: false
  @moduletag :bench
  @moduletag timeout: 120_000

  alias Ferricstore.Store.{Router, LFU}
  alias Ferricstore.Stats

  @iterations 200_000

  setup_all do
    # Use production sample rate for benchmarks
    :persistent_term.put(:ferricstore_read_sample_rate, 100)
    on_exit(fn -> :persistent_term.put(:ferricstore_read_sample_rate, 1) end)
    :ok
  end

  test "per-layer cost breakdown" do
    prefix = "rppt_#{System.unique_integer([:positive])}"

    # Pre-populate 1000 keys via Router.put (goes through Raft)
    for i <- 1..1000 do
      Router.put(
        FerricStore.Instance.get(:default),
        "#{prefix}:#{i}",
        String.duplicate("v", 100),
        0
      )
    end

    Process.sleep(200)

    key = "#{prefix}:500"
    idx = Router.shard_for(FerricStore.Instance.get(:default), key)
    keydir = Router.resolve_keydir(FerricStore.Instance.get(:default), idx)

    IO.puts(
      "\n=== Read Path Per-Layer Cost (#{@iterations} iters, production sample_rate=100) ===\n"
    )

    # 1. Full Router.get
    {router_us, _} =
      :timer.tc(fn ->
        for _ <- 1..@iterations, do: Router.get(FerricStore.Instance.get(:default), key)
      end)

    # 2. Direct ETS lookup only
    {ets_us, _} =
      :timer.tc(fn ->
        for _ <- 1..@iterations do
          case :ets.lookup(keydir, key) do
            [{^key, value, _exp, _lfu, _fid, _off, _vsize}] when value != nil -> value
            _ -> nil
          end
        end
      end)

    # 3. shard_for
    {hash_us, _} =
      :timer.tc(fn ->
        for _ <- 1..@iterations, do: Router.shard_for(FerricStore.Instance.get(:default), key)
      end)

    # 4. Stats.incr_keyspace_hits
    {hits_us, _} =
      :timer.tc(fn ->
        for _ <- 1..@iterations, do: Stats.incr_keyspace_hits()
      end)

    # 5. maybe_record_hot_read (at sample_rate=100, ~1% actually records)
    # We measure the sampling check + occasional recording
    rate = :persistent_term.get(:ferricstore_read_sample_rate, 100)

    {hot_read_us, _} =
      :timer.tc(fn ->
        for _ <- 1..@iterations do
          if rate <= 1 or :rand.uniform(rate) == 1 do
            Stats.record_hot_read(key)
          end
        end
      end)

    # 6. maybe_lfu_touch (at sample_rate=100)
    [{^key, _v, _e, packed_lfu, _f, _o, _vs}] = :ets.lookup(keydir, key)

    {lfu_us, _} =
      :timer.tc(fn ->
        for _ <- 1..@iterations do
          if rate <= 1 or :rand.uniform(rate) == 1 do
            LFU.touch(keydir, key, packed_lfu)
          end
        end
      end)

    # 7. Just the sampling overhead (2x persistent_term.get + 2x rand.uniform)
    {sample_overhead_us, _} =
      :timer.tc(fn ->
        for _ <- 1..@iterations do
          r1 = :persistent_term.get(:ferricstore_read_sample_rate, 100)
          _ = :rand.uniform(r1)
          r2 = :persistent_term.get(:ferricstore_read_sample_rate, 100)
          _ = :rand.uniform(r2)
        end
      end)

    # 8. Single persistent_term.get + single rand.uniform (proposed)
    {sample_single_us, _} =
      :timer.tc(fn ->
        for _ <- 1..@iterations do
          r = :persistent_term.get(:ferricstore_read_sample_rate, 100)
          _ = :rand.uniform(r)
        end
      end)

    router_ns = div(router_us * 1000, @iterations)
    ets_ns = div(ets_us * 1000, @iterations)
    hash_ns = div(hash_us * 1000, @iterations)
    hits_ns = div(hits_us * 1000, @iterations)
    hot_read_ns = div(hot_read_us * 1000, @iterations)
    lfu_ns = div(lfu_us * 1000, @iterations)
    sample_overhead_ns = div(sample_overhead_us * 1000, @iterations)
    sample_single_ns = div(sample_single_us * 1000, @iterations)

    IO.puts(
      "  #{String.pad_trailing("Layer", 45)} #{String.pad_leading("ns/op", 8)} #{String.pad_leading("% of total", 10)}"
    )

    IO.puts("  #{String.duplicate("-", 68)}")

    layers = [
      {"Direct ETS lookup", ets_ns},
      {"shard_for (crc32 + slot_map)", hash_ns},
      {"Stats.incr_keyspace_hits", hits_ns},
      {"maybe_record_hot_read (sampled)", hot_read_ns},
      {"maybe_lfu_touch (sampled)", lfu_ns},
      {"2x sample check (current overhead)", sample_overhead_ns},
      {"1x sample check (proposed)", sample_single_ns},
      {"--- Router.get total ---", router_ns},
      {"Overhead (Router.get - ETS)", router_ns - ets_ns}
    ]

    for {label, ns} <- layers do
      pct = if router_ns > 0, do: Float.round(ns / router_ns * 100, 1), else: 0.0

      IO.puts(
        "  #{String.pad_trailing(label, 45)} #{String.pad_leading("#{ns}ns", 8)} #{String.pad_leading("#{pct}%", 10)}"
      )
    end
  end

  test "throughput: 50 concurrent readers (3s)" do
    prefix = "rppt_thr_#{System.unique_integer([:positive])}"

    for i <- 1..10_000 do
      Router.put(
        FerricStore.Instance.get(:default),
        "#{prefix}:#{i}",
        String.duplicate("v", 100),
        0
      )
    end

    Process.sleep(300)

    counter = :counters.new(1, [:atomics])
    stop = :atomics.new(1, [])

    tasks =
      for _ <- 1..50 do
        Task.async(fn ->
          l = fn l ->
            if :atomics.get(stop, 1) == 1, do: throw(:stop)
            key = "#{prefix}:#{:rand.uniform(10_000)}"
            Router.get(FerricStore.Instance.get(:default), key)
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

    Process.sleep(3_000)
    :atomics.put(stop, 1, 1)
    Task.await_many(tasks, 10_000)

    total = :counters.get(counter, 1)
    rps = div(total, 3)
    ns_per_op = if rps > 0, do: div(1_000_000_000, rps), else: 0

    IO.puts("\n=== Throughput: 50 concurrent readers, 3s ===")
    IO.puts("  Total reads: #{total}")
    IO.puts("  Reads/sec:   #{rps}")
    IO.puts("  ns/op:       #{ns_per_op}")
  end
end
