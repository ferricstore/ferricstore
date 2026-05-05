defmodule Ferricstore.Bench.EmbeddedDurabilityComparisonTest do
  use ExUnit.Case, async: false
  @moduletag skip: "async durability feature removed; quorum is the only supported durability"
  @moduletag :bench
  @moduletag timeout: 120_000

  test "write throughput: quorum vs async durability (embedded SET)" do
    value = String.duplicate("v", 100)

    IO.puts("\n=== Embedded SET: Quorum vs Async Durability ===\n")
    IO.puts("  #{String.pad_trailing("Mode", 12)} #{String.pad_trailing("Writers", 10)} #{String.pad_leading("Writes/sec", 12)} #{String.pad_leading("Per-writer", 12)} #{String.pad_leading("us/op", 10)}")
    IO.puts("  #{String.duplicate("-", 60)}")

    for {mode, prefix_ns} <- [{"quorum", "bench_q"}, {"async", "bench_a"}] do
      # Configure namespace durability
      if mode == "async" do
        Ferricstore.NamespaceConfig.set("bench_a", "durability", "async")
      end

      for num_writers <- [1, 4, 8, 16, 50] do
        counter = :counters.new(1, [:atomics])
        stop = :atomics.new(1, [])

        tasks = for w <- 1..num_writers do
          Task.async(fn ->
            l = fn l, i ->
              if :atomics.get(stop, 1) == 1, do: throw(:stop)
              FerricStore.set("#{prefix_ns}:w#{w}:#{i}", value)
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

        IO.puts("  #{String.pad_trailing(mode, 12)} #{String.pad_trailing("#{num_writers}", 10)} #{String.pad_leading("#{wps}", 12)} #{String.pad_leading("#{per_writer}", 12)} #{String.pad_leading("#{us_per_op}", 10)}")

        Process.sleep(200)
      end

      # Reset namespace config
      if mode == "async" do
        Ferricstore.NamespaceConfig.set("bench_a", "durability", "quorum")
      end
    end
  end

  test "read-after-write latency: quorum vs async" do
    value = String.duplicate("v", 100)
    iterations = 10_000

    IO.puts("\n=== Embedded SET+GET Round-Trip: Quorum vs Async (#{iterations} iterations) ===\n")

    for {mode, prefix_ns} <- [{"quorum", "bench_rw_q"}, {"async", "bench_rw_a"}] do
      if mode == "async" do
        Ferricstore.NamespaceConfig.set("bench_rw_a", "durability", "async")
      end

      {us, _} = :timer.tc(fn ->
        for i <- 1..iterations do
          key = "#{prefix_ns}:#{i}"
          FerricStore.set(key, value)
          {:ok, ^value} = FerricStore.get(key)
        end
      end)

      ns_per_op = div(us * 1000, iterations)
      ops_per_sec = if us > 0, do: div(iterations * 1_000_000, us), else: 0

      IO.puts("  #{String.pad_trailing(mode, 12)} #{ns_per_op}ns/round-trip   #{ops_per_sec} ops/sec")

      if mode == "async" do
        Ferricstore.NamespaceConfig.set("bench_rw_a", "durability", "quorum")
      end
    end
  end
end
