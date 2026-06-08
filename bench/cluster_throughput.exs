# FerricStore 3-Node Cluster Benchmark
#
# Measures throughput for quorum writes in a real 3-node Raft cluster.
# Quorum = 2/3 nodes must fsync before ACK → latency = max(node1, node2).
#
# Setup:
#   # Node 1 (leader):
#   elixir --sname fs1 --cookie bench -S mix run --no-halt
#
#   # Node 2:
#   elixir --sname fs2 --cookie bench -S mix run --no-halt
#
#   # Node 3:
#   elixir --sname fs3 --cookie bench -S mix run --no-halt
#
#   # Connect nodes (on any node):
#   Node.connect(:"fs2@hostname")
#   Node.connect(:"fs3@hostname")
#
#   # Run benchmark (from any node or separate client):
#   elixir --sname bench --cookie bench -S mix run bench/cluster_throughput.exs \
#     --node fs1@hostname --node fs2@hostname --node fs3@hostname
#
# For TCP benchmark on cluster:
#   bash bench/tcp_throughput.sh <leader_host> 6379
#
# Compare:
#   - Single node quorum (local fsync only, no network)
#   - 3-node quorum (network + 2/3 fsync)
#   - 3-node with writes to different nodes (tests leader forwarding)

nodes =
  System.argv()
  |> Enum.chunk_every(2)
  |> Enum.filter(fn [flag, _] -> flag == "--node" end)
  |> Enum.map(fn [_, node_str] -> String.to_atom(node_str) end)

if nodes == [] do
  IO.puts("""
  Usage: mix run bench/cluster_throughput.exs --node fs1@host --node fs2@host --node fs3@host

  Or run single-node for comparison:
    mix run bench/erpc_throughput.exs
  """)

  System.halt(1)
end

IO.puts("Connecting to #{length(nodes)} nodes...")

for node <- nodes do
  case Node.connect(node) do
    true -> IO.puts("  Connected: #{node}")
    false -> IO.puts("  FAILED: #{node}")
  end
end

# Use first node as primary target
primary = hd(nodes)
key_max = 1_000_000
payload = String.duplicate("x", 100)

# Pre-populate
IO.puts("\nPre-populating 10K keys on #{primary}...")

for i <- 1..10_000 do
  :erpc.call(primary, FerricStore, :set, ["bench:#{i}", payload])
end

IO.puts("\n=== 3-NODE CLUSTER BENCHMARK ===")
IO.puts("Nodes: #{inspect(nodes)}")
IO.puts("Primary target: #{primary}")
IO.puts("Payload: #{byte_size(payload)} bytes")
IO.puts("Concurrency: 50 parallel\n")

# ---------------------------------------------------------------------------
# Writes to primary (leader handles locally)
# ---------------------------------------------------------------------------

IO.puts("--- Writes to PRIMARY node (#{primary}) ---\n")

Benchee.run(
  %{
    "SET (quorum, primary)" => fn ->
      :erpc.call(primary, FerricStore, :set, ["bench:q:#{:rand.uniform(key_max)}", payload])
    end,
    "GET (primary)" => fn ->
      :erpc.call(primary, FerricStore, :get, ["bench:#{:rand.uniform(10_000)}"])
    end,
    "HSET (quorum, primary)" => fn ->
      :erpc.call(primary, FerricStore, :hset, [
        "bench:q:hash:#{:rand.uniform(key_max)}",
        %{"field" => payload}
      ])
    end,
    "INCR (quorum, primary)" => fn ->
      :erpc.call(primary, FerricStore, :incr, ["bench:q:counter:#{:rand.uniform(key_max)}"])
    end
  },
  time: 10,
  warmup: 2,
  parallel: 50,
  formatters: [Benchee.Formatters.Console]
)

# ---------------------------------------------------------------------------
# Reads from different nodes (test follower read path)
# ---------------------------------------------------------------------------

if length(nodes) >= 2 do
  follower = Enum.at(nodes, 1)

  IO.puts("\n--- Reads from FOLLOWER node (#{follower}) ---\n")

  Benchee.run(
    %{
      "GET (follower)" => fn ->
        :erpc.call(follower, FerricStore, :get, ["bench:#{:rand.uniform(10_000)}"])
      end,
      "GET (primary)" => fn ->
        :erpc.call(primary, FerricStore, :get, ["bench:#{:rand.uniform(10_000)}"])
      end
    },
    time: 10,
    warmup: 2,
    parallel: 50,
    formatters: [Benchee.Formatters.Console]
  )
end

# ---------------------------------------------------------------------------
# Writes distributed across all nodes (round-robin)
# ---------------------------------------------------------------------------

IO.puts("\n--- Writes DISTRIBUTED across all #{length(nodes)} nodes ---\n")

node_count = length(nodes)
nodes_tuple = List.to_tuple(nodes)

Benchee.run(
  %{
    "SET (quorum, distributed)" => fn ->
      node = elem(nodes_tuple, :rand.uniform(node_count) - 1)
      :erpc.call(node, FerricStore, :set, ["bench:d:#{:rand.uniform(key_max)}", payload])
    end,
    "INCR (quorum, distributed)" => fn ->
      node = elem(nodes_tuple, :rand.uniform(node_count) - 1)
      :erpc.call(node, FerricStore, :incr, ["bench:d:counter:#{:rand.uniform(key_max)}"])
    end
  },
  time: 10,
  warmup: 2,
  parallel: 50,
  formatters: [Benchee.Formatters.Console]
)

IO.puts("\n=== Done ===")
