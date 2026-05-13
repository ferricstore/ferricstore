# erpc multi-connection benchmark
#
# Spawns N slave nodes on the client, each with its own distribution
# TCP connection to the server. This mimics memtier's 200 independent
# TCP connections. Each Benchee parallel process routes through a
# different slave → server connection.
#
# Usage:
#   MIX_ENV=bench elixir --sname bench_multi --cookie ferricstore_bench \
#     -S mix run --no-start bench/erpc_vs_tcp.exs --remote ferricstore@hostname

remote_node =
  case System.argv() do
    ["--remote", node_str] -> String.to_atom(node_str)
    _ -> nil
  end

unless remote_node do
  IO.puts("ERROR: --remote required for multi-connection benchmark")
  System.halt(1)
end

IO.puts("Connecting to #{remote_node}...")
Node.connect(remote_node) || raise "Cannot connect to #{remote_node}"
IO.puts("Connected (1 distribution connection).")

payload = String.duplicate("x", 256)
parallel = 50
num_connections = 8

# Start slave/peer nodes for extra distribution connections
IO.puts("\nStarting #{num_connections} peer nodes for parallel distribution connections...")

peers =
  for i <- 1..num_connections do
    name = :"bench_ch#{i}"
    # Use :peer module (OTP 25+) to start a node on same host
    {:ok, pid, node} = :peer.start(%{
      name: name,
      connection: :standard_io,
      args: [
        ~c"-setcookie", ~c"ferricstore_bench",
        ~c"-connect_all", ~c"false",
        ~c"-hidden"
      ]
    })

    # Connect peer to server
    true = :erpc.call(node, Node, :connect, [remote_node])

    # Load our modules on the peer
    {mod, bin, file} = :code.get_object_code(FerricStore)
    :erpc.call(node, :code, :load_binary, [mod, file, bin])
    {mod2, bin2, file2} = :code.get_object_code(FerricStore.Pipe)
    :erpc.call(node, :code, :load_binary, [mod2, file2, bin2])

    IO.puts("  peer #{node} connected to #{remote_node}")
    {pid, node}
  end

peer_nodes = Enum.map(peers, &elem(&1, 1)) |> List.to_tuple()
IO.puts("#{num_connections} parallel distribution connections ready.\n")

# Pre-populate via main connection
IO.puts("Pre-populating 100K keys...")
for chunk <- Enum.chunk_every(1..100_000, 500) do
  kv_pairs = Enum.map(chunk, fn i -> {"bench:#{i}", payload} end)
  :erpc.call(remote_node, FerricStore, :batch_set, [kv_pairs])
end
IO.puts("Done.\n")

# Pre-build key pools
IO.puts("Building key pools...")
keys_100 = for _ <- 1..500, do: (for _ <- 1..100, do: "bench:#{:rand.uniform(100_000)}")
keys_100_t = List.to_tuple(keys_100)
keys_500 = for _ <- 1..200, do: (for _ <- 1..500, do: "bench:#{:rand.uniform(100_000)}")
keys_500_t = List.to_tuple(keys_500)
keys_1000 = for _ <- 1..100, do: (for _ <- 1..1000, do: "bench:#{:rand.uniform(100_000)}")
keys_1000_t = List.to_tuple(keys_1000)
IO.puts("Done.\n")

# ---- BENCHMARK ----

IO.puts("=" |> String.duplicate(60))
IO.puts("SINGLE CONNECTION vs #{num_connections} CONNECTIONS")
IO.puts("=" |> String.duplicate(60))

for {depth, pool, pool_size} <- [
  {100, keys_100_t, 500},
  {500, keys_500_t, 200},
  {1000, keys_1000_t, 100}
] do
  counter_single = :atomics.new(1, signed: false)
  counter_multi = :atomics.new(1, signed: false)
  counter_route = :atomics.new(1, signed: false)

  IO.puts("\n=== #{depth} keys, #{parallel}p ===\n")

  Benchee.run(
    %{
      "1 conn (erpc.call)" => fn ->
        idx = rem(:atomics.add_get(counter_single, 1, 1), pool_size)
        keys = elem(pool, idx)
        :erpc.call(remote_node, FerricStore, :batch_get, [keys])
      end,
      "#{num_connections} conns (via peers)" => fn ->
        idx = rem(:atomics.add_get(counter_multi, 1, 1), pool_size)
        keys = elem(pool, idx)
        # Route through a peer node — each peer has its own dist connection
        route = rem(:atomics.add_get(counter_route, 1, 1), num_connections)
        peer = elem(peer_nodes, route)
        :erpc.call(peer, :erpc, :call, [remote_node, FerricStore, :batch_get, [keys]])
      end
    },
    time: 10, warmup: 3, parallel: parallel,
    formatters: [Benchee.Formatters.Console]
  )

  IO.puts("")
end

# Cleanup peers
IO.puts("Stopping peer nodes...")
Enum.each(peers, fn {pid, _} -> :peer.stop(pid) end)
IO.puts("Done.")

IO.puts("\nops/sec = ips × depth")
