defmodule Ferricstore.Bench.EmbeddedReadProfileTest do
  use ExUnit.Case, async: false
  @moduletag :bench
  @moduletag timeout: 120_000

  @iterations 100_000

  setup_all do
    # Use production sample rate for benchmarks (default is 1 in test config)
    :persistent_term.put(:ferricstore_read_sample_rate, 100)
    on_exit(fn -> :persistent_term.put(:ferricstore_read_sample_rate, 1) end)
    :ok
  end

  test "profile embedded GET - every layer timed" do
    alias Ferricstore.Store.{Router, LFU}

    prefix = "emb_prof_#{System.unique_integer([:positive])}"

    # Pre-populate 1000 keys
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

    IO.puts("\n=== Embedded GET Profile (#{@iterations} iterations) ===\n")

    # 1. Full FerricStore.get (the user-facing API)
    {full_us, _} =
      :timer.tc(fn ->
        for _ <- 1..@iterations, do: FerricStore.get(key)
      end)

    # 2. Router.get (skip sandbox_key + {:ok, _} wrap)
    {router_us, _} =
      :timer.tc(fn ->
        for _ <- 1..@iterations, do: Router.get(FerricStore.Instance.get(:default), key)
      end)

    # 3. Direct ETS lookup (skip Router dispatch, shard_for, stats)
    idx = Router.shard_for(FerricStore.Instance.get(:default), key)
    keydir = :"keydir_#{idx}"

    {ets_us, _} =
      :timer.tc(fn ->
        for _ <- 1..@iterations do
          case :ets.lookup(keydir, key) do
            [{^key, value, _exp, _lfu, _fid, _off, _vsize}] when value != nil -> value
            _ -> nil
          end
        end
      end)

    # 4. Just shard_for (hash computation)
    {hash_us, _} =
      :timer.tc(fn ->
        for _ <- 1..@iterations, do: Router.shard_for(FerricStore.Instance.get(:default), key)
      end)

    # 5. sandbox_key overhead (removed; identity function as baseline)
    {sandbox_us, _} =
      :timer.tc(fn ->
        for _ <- 1..@iterations, do: key
      end)

    # 6. Stats overhead (incr_keyspace_hits)
    {stats_us, _} =
      :timer.tc(fn ->
        for _ <- 1..@iterations, do: Ferricstore.Stats.incr_keyspace_hits()
      end)

    # 7. LFU touch
    [{^key, _v, _e, packed_lfu, _f, _o, _vs}] = :ets.lookup(keydir, key)

    {lfu_us, _} =
      :timer.tc(fn ->
        for _ <- 1..@iterations, do: LFU.touch(keydir, key, packed_lfu)
      end)

    # 8. Stats.record_hot_read
    {hot_read_us, _} =
      :timer.tc(fn ->
        for _ <- 1..@iterations, do: Ferricstore.Stats.record_hot_read(key)
      end)

    full_ns = div(full_us * 1000, @iterations)
    router_ns = div(router_us * 1000, @iterations)
    ets_ns = div(ets_us * 1000, @iterations)
    hash_ns = div(hash_us * 1000, @iterations)
    sandbox_ns = div(sandbox_us * 1000, @iterations)
    stats_ns = div(stats_us * 1000, @iterations)
    lfu_ns = div(lfu_us * 1000, @iterations)
    hot_read_ns = div(hot_read_us * 1000, @iterations)

    IO.puts(
      "  #{String.pad_trailing("Layer", 40)} #{String.pad_leading("Time/op", 10)} #{String.pad_leading("% of full", 10)}"
    )

    IO.puts("  #{String.duplicate("-", 65)}")

    layers = [
      {"Direct ETS lookup", ets_ns},
      {"shard_for (crc32)", hash_ns},
      {"sandbox_key", sandbox_ns},
      {"LFU touch", lfu_ns},
      {"Stats.incr_keyspace_hits", stats_ns},
      {"Stats.record_hot_read", hot_read_ns},
      {"--- Router.get total ---", router_ns},
      {"=== FerricStore.get total ===", full_ns},
      {"Overhead (full - ETS)", full_ns - ets_ns}
    ]

    for {label, ns} <- layers do
      pct = if full_ns > 0, do: Float.round(ns / full_ns * 100, 1), else: 0.0

      IO.puts(
        "  #{String.pad_trailing(label, 40)} #{String.pad_leading("#{ns}ns", 10)} #{String.pad_leading("#{pct}%", 10)}"
      )
    end
  end

  test "throughput: single reader vs N concurrent readers" do
    prefix = "emb_thr_#{System.unique_integer([:positive])}"

    for i <- 1..10_000 do
      Ferricstore.Store.Router.put(
        FerricStore.Instance.get(:default),
        "#{prefix}:#{i}",
        String.duplicate("v", 100),
        0
      )
    end

    Process.sleep(300)

    IO.puts("\n=== Embedded GET Throughput ===\n")

    IO.puts(
      "  #{String.pad_trailing("Readers", 10)} #{String.pad_leading("Reads/sec", 12)} #{String.pad_leading("Per-reader", 12)} #{String.pad_leading("ns/op", 10)}"
    )

    IO.puts("  #{String.duplicate("-", 50)}")

    for num_readers <- [1, 2, 4, 8, 16, 32, 50] do
      counter = :counters.new(1, [:atomics])
      stop = :atomics.new(1, [])

      tasks =
        for _ <- 1..num_readers do
          Task.async(fn ->
            l = fn l ->
              if :atomics.get(stop, 1) == 1, do: throw(:stop)
              key = "#{prefix}:#{:rand.uniform(10_000)}"
              FerricStore.get(key)
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
      per_reader = div(rps, max(num_readers, 1))
      ns_per_op = if rps > 0, do: div(1_000_000_000, rps), else: 0

      IO.puts(
        "  #{String.pad_trailing("#{num_readers}", 10)} #{String.pad_leading("#{rps}", 12)} #{String.pad_leading("#{per_reader}", 12)} #{String.pad_leading("#{ns_per_op}", 10)}"
      )
    end
  end

  test "comparison: FerricStore.get vs Router.get vs direct ETS" do
    prefix = "emb_cmp_#{System.unique_integer([:positive])}"

    for i <- 1..1000 do
      Ferricstore.Store.Router.put(
        FerricStore.Instance.get(:default),
        "#{prefix}:#{i}",
        String.duplicate("v", 100),
        0
      )
    end

    Process.sleep(200)

    key = "#{prefix}:500"
    idx = Ferricstore.Store.Router.shard_for(FerricStore.Instance.get(:default), key)
    keydir = :"keydir_#{idx}"

    IO.puts("\n=== Embedded GET Comparison (50 concurrent readers, 3s) ===\n")

    IO.puts(
      "  #{String.pad_trailing("Method", 30)} #{String.pad_leading("Reads/sec", 12)} #{String.pad_leading("ns/op", 10)}"
    )

    IO.puts("  #{String.duplicate("-", 55)}")

    for {label, fun} <- [
          {"FerricStore.get", fn -> FerricStore.get(key) end},
          {"Router.get",
           fn -> Ferricstore.Store.Router.get(FerricStore.Instance.get(:default), key) end},
          {"Direct ETS",
           fn ->
             case :ets.lookup(keydir, key) do
               [{^key, v, _, _, _, _, _}] when v != nil -> v
               _ -> nil
             end
           end}
        ] do
      counter = :counters.new(1, [:atomics])
      stop = :atomics.new(1, [])

      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            l = fn l ->
              if :atomics.get(stop, 1) == 1, do: throw(:stop)
              fun.()
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

      IO.puts(
        "  #{String.pad_trailing(label, 30)} #{String.pad_leading("#{rps}", 12)} #{String.pad_leading("#{ns_per_op}", 10)}"
      )
    end
  end
end
