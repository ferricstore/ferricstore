# bench/router_write_bench.exs
#
# Measures concurrent write throughput through Router and the configured
# replicated storage backend.
#
# This intentionally excludes native-protocol parsing, framing, sockets, and
# client scheduling. Every measured write goes through:
#
#   Router.put/4
#     -> key admission and shard routing
#       -> the active Raft backend and its batcher
#         -> replicated state-machine apply and durable log acknowledgement
#
# Writes to different shard groups can be in flight independently. Concurrency
# also gives the backend an opportunity to batch commits within each group.
#
# Benchmark sections:
#
#   1. Single shard, N concurrent writers  — isolates one Shard's capacity
#      (same key space, all writes hash to the same shard)
#
#   2. All shards, N concurrent writers    — distributed key space, full
#      sharding benefit visible
#
#   3. Writes/second scaling curve         — total throughput as N grows:
#      4, 16, 64, 256 writers on all shards
#
# Run locally:
#   MIX_ENV=bench mix run --no-start bench/router_write_bench.exs

alias Ferricstore.Store.Router

Logger.configure(level: :warning)

bench_warmup = System.get_env("BENCH_WARMUP", "2") |> String.to_integer()
bench_time = System.get_env("BENCH_TIME", "5") |> String.to_integer()

# ---------------------------------------------------------------------------
# Application startup
# ---------------------------------------------------------------------------

bench_data_dir = System.tmp_dir!() <> "/ferricstore_rwb_#{:rand.uniform(9_999_999)}"
File.mkdir_p!(bench_data_dir)
Application.put_env(:ferricstore, :data_dir, bench_data_dir)
Application.put_env(:ferricstore, :native_port, 0)
{:ok, _} = Application.ensure_all_started(:ferricstore)

ctx = FerricStore.Instance.get(:default)
shard_count = ctx.shard_count

IO.puts("""
=== Router Write Throughput Benchmark ===
Shards: #{shard_count}  |  Backend: #{Ferricstore.Raft.Backend.running_or_selected()}
Warmup: #{bench_warmup}s  |  Run: #{bench_time}s per scenario
""")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

defmodule RouterBench do
  @moduledoc false

  # Spawn N tasks each calling Router.put once with a unique key.
  # Returns :ok when all tasks finish. Unique keys spread across shards.
  def concurrent_puts(ctx, n, counter, prefix) do
    base = :counters.get(counter, 1)
    :counters.add(counter, 1, n)

    0..(n - 1)
    |> Enum.map(fn i ->
      Task.async(fn ->
        :ok = Router.put(ctx, "#{prefix}_#{base + i}", "v", 0)
      end)
    end)
    |> Task.await_many(30_000)

    :ok
  end

  # Same but all keys hash to the same shard (used for single-shard isolation).
  # We pre-compute a set of keys that all map to shard 0.
  def same_shard_puts(ctx, n, keys, counter) do
    base = :counters.get(counter, 1)
    :counters.add(counter, 1, n)
    key_count = length(keys)

    0..(n - 1)
    |> Enum.map(fn i ->
      key = Enum.at(keys, rem(base + i, key_count))
      Task.async(fn -> :ok = Router.put(ctx, key, "v_#{base + i}", 0) end)
    end)
    |> Task.await_many(30_000)

    :ok
  end

  # Find `n` keys that all hash to shard 0.
  def keys_for_shard(ctx, shard_idx, count) do
    Stream.iterate(0, &(&1 + 1))
    |> Stream.filter(fn i ->
      Router.shard_for(ctx, "shard_key_#{i}") == shard_idx
    end)
    |> Enum.take(count)
    |> Enum.map(fn i -> "shard_key_#{i}" end)
  end
end

# Pre-compute keys that all route to shard 0 (for single-shard section)
shard0_keys = RouterBench.keys_for_shard(ctx, 0, 500)
IO.puts("Pre-computed #{length(shard0_keys)} keys routing to shard 0.\n")

# ---------------------------------------------------------------------------
# Section 1: Single shard — N concurrent writers, all keys hash to shard 0
# ---------------------------------------------------------------------------

IO.puts("--- Section 1: Single-shard capacity (all writes → shard 0) ---\n")
IO.puts("This isolates one replicated shard group's write path.\n")

single_shard_counter = :counters.new(1, [:atomics])

single_shard_scenarios =
  Enum.map([4, 16, 64, 256], fn n ->
    {
      "#{String.pad_leading(to_string(n), 3)} writers → 1 shard",
      fn ->
        RouterBench.same_shard_puts(ctx, n, shard0_keys, single_shard_counter)
      end
    }
  end)
  |> Map.new()

Benchee.run(
  single_shard_scenarios,
  time: bench_time,
  warmup: bench_warmup,
  memory_time: 1,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML, file: "bench/output/router_single_shard.html", auto_open: false}
  ]
)

# ---------------------------------------------------------------------------
# Section 2: All shards — N concurrent writers, keys distributed across shards
# ---------------------------------------------------------------------------

IO.puts(
  "\n--- Section 2: All #{shard_count} shards — keys distributed (realistic workload) ---\n"
)

IO.puts("This shows the benefit of sharding: #{shard_count} independent write paths.\n")

all_shards_counter = :counters.new(1, [:atomics])

all_shards_scenarios =
  Enum.map([4, 16, 64, 256], fn n ->
    {
      "#{String.pad_leading(to_string(n), 3)} writers → #{shard_count} shards",
      fn ->
        RouterBench.concurrent_puts(ctx, n, all_shards_counter, "rw#{n}")
      end
    }
  end)
  |> Map.new()

Benchee.run(
  all_shards_scenarios,
  time: bench_time,
  warmup: bench_warmup,
  memory_time: 1,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML, file: "bench/output/router_all_shards.html", auto_open: false}
  ]
)

# ---------------------------------------------------------------------------
# Section 3: Writes/second summary — total entries written per second
# ---------------------------------------------------------------------------

IO.puts("\n--- Section 3: Direct writes/second sample ---\n")

IO.puts("Running a separate fixed-iteration sample through all shards:\n")

IO.puts(
  String.pad_trailing("writers", 10) <>
    String.pad_trailing("batches/s", 22) <>
    "total writes/s"
)

IO.puts(String.duplicate("-", 50))

# Print a note — actual numbers come from Section 2 Benchee output above.
# We also run a quick timed pass to compute writes/s directly.

for n <- [4, 16, 64, 256] do
  iterations = 20
  counter = :counters.new(1, [:atomics])

  {elapsed_us, _} =
    :timer.tc(fn ->
      for _ <- 1..iterations do
        RouterBench.concurrent_puts(ctx, n, counter, "summary_#{n}")
      end
    end)

  elapsed_s = elapsed_us / 1_000_000
  total_writes = n * iterations
  writes_per_s = round(total_writes / elapsed_s)
  batches_per_s = round(iterations / elapsed_s)

  IO.puts(
    String.pad_trailing("#{n}", 10) <>
      String.pad_trailing("~#{batches_per_s}", 22) <>
      "~#{writes_per_s} writes/s"
  )
end

IO.puts("""

=== Interpreting these results ===

These are server-side Router write numbers, not end-to-end native SET
throughput. Native clients additionally pay connection scheduling, frame
decoding, command preparation, ACL checks, response encoding, and network I/O.

Each successful sample has completed the active replicated backend's
acknowledgement contract. The durable Raft log is authoritative; derived
Bitcask projections may checkpoint asynchronously and are recoverable by log
replay. Compare runs only when the backend, shard count, hardware, storage,
warmup, and duration are equivalent.
""")

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

:ok = Application.stop(:ferricstore)
File.rm_rf!(bench_data_dir)
