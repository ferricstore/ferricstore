# FerricStore erpc Benchmark
#
# Measures throughput and latency for FerricStore operations over
# Erlang distribution (erpc). Compare against memtier_benchmark
# results for Redis protocol to measure protocol overhead.
#
# FerricStore is quorum-only: every write goes through Raft.
#
# Usage:
#   # Single node (embedded — no network):
#   mix run bench/erpc_throughput.exs
#
#   # Remote node (erpc — Erlang distribution):
#   # Terminal 1: start FerricStore
#   elixir --sname ferricstore --cookie bench -S mix run --no-halt
#
#   # Terminal 2: run benchmark
#   elixir --sname bench_client --cookie bench -S mix run bench/erpc_throughput.exs --remote ferricstore@hostname
#
# Equivalent memtier command for comparison:
#   bash bench/tcp_throughput.sh

remote_node =
  case System.argv() do
    ["--remote", node_str] -> String.to_atom(node_str)
    _ -> nil
  end

if remote_node do
  IO.puts("Connecting to #{remote_node}...")
  Node.connect(remote_node) || raise "Cannot connect to #{remote_node}"
  IO.puts("Connected.")
end

# Configuration — match memtier defaults
key_max = 1_000_000
payload = String.duplicate("x", 100)

# Helper to call local or remote
call = fn module, fun, args ->
  if remote_node do
    :erpc.call(remote_node, module, fun, args)
  else
    apply(module, fun, args)
  end
end

# Pre-populate keys for GET tests
IO.puts("Pre-populating 10K keys...")

for i <- 1..10_000 do
  call.(FerricStore, :set, ["bench:#{i}", payload])
end

IO.puts("Done.")

mode = if remote_node, do: "erpc (#{remote_node})", else: "embedded (local)"
IO.puts("\nBenchmark mode: #{mode}")
IO.puts("Payload: #{byte_size(payload)} bytes")
IO.puts("Key space: #{key_max}")
IO.puts("Concurrency: 50 parallel processes\n")

# ---------------------------------------------------------------------------
# Quorum writes (waits for Raft consensus + fdatasync)
# ---------------------------------------------------------------------------

IO.puts("=== QUORUM WRITES ===\n")

Benchee.run(
  %{
    "SET" => fn ->
      call.(FerricStore, :set, ["bench:#{:rand.uniform(key_max)}", payload])
    end,
    "GET" => fn ->
      call.(FerricStore, :get, ["bench:#{:rand.uniform(10_000)}"])
    end,
    "HSET" => fn ->
      call.(FerricStore, :hset, ["bench:hash:#{:rand.uniform(key_max)}", %{"field" => payload}])
    end,
    "LPUSH" => fn ->
      call.(FerricStore, :rpush, ["bench:list:#{:rand.uniform(key_max)}", [payload]])
    end,
    "SADD" => fn ->
      call.(FerricStore, :sadd, ["bench:set:#{:rand.uniform(key_max)}", [payload]])
    end,
    "INCR" => fn ->
      call.(FerricStore, :incr, ["bench:counter:#{:rand.uniform(key_max)}"])
    end
  },
  time: 10,
  warmup: 2,
  parallel: 50,
  formatters: [Benchee.Formatters.Console]
)
