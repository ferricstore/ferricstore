# FerricStore erpc Pipeline Benchmark
#
# Compares 3 erpc modes:
#   1. erpc.call per batch  — spawns a process per call
#   2. BatchWorker           — persistent GenServer, call over distribution
#   3. batch_get/batch_set   — direct function call (embedded baseline)
#
# Usage:
#   MIX_ENV=bench mix run bench/erpc_pipeline_throughput.exs
#   MIX_ENV=bench elixir --sname bench_pipe --cookie ferricstore_bench \
#     -S mix run --no-start bench/erpc_pipeline_throughput.exs --remote ferricstore@hostname

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

key_max = 1_000_000
payload = String.duplicate("x", 256)
parallel = 50

call = fn module, fun, args ->
  if remote_node do
    :erpc.call(remote_node, module, fun, args)
  else
    apply(module, fun, args)
  end
end

# Pre-populate
IO.puts("Pre-populating 10K keys...")
for chunk <- Enum.chunk_every(1..10_000, 100) do
  kv_pairs = Enum.map(chunk, fn i -> {"bench:#{i}", payload} end)
  call.(FerricStore, :batch_set, [kv_pairs])
end
IO.puts("Done.")

mode = if remote_node, do: "erpc (#{remote_node})", else: "embedded (local)"
IO.puts("\nBenchmark mode: #{mode}")
IO.puts("Payload: #{byte_size(payload)} bytes, Parallel: #{parallel}")
IO.puts("Key space: #{key_max}\n")

# ---------------------------------------------------------------------------
# Start BatchWorker pool on remote (one per parallel slot)
# ---------------------------------------------------------------------------

workers =
  if remote_node do
    IO.puts("Starting #{parallel} BatchWorker processes on #{remote_node}...")
    for _ <- 1..parallel do
      {:ok, pid} = :erpc.call(remote_node, FerricStore.BatchWorker, :start, [])
      pid
    end
  else
    for _ <- 1..parallel do
      {:ok, pid} = FerricStore.BatchWorker.start()
      pid
    end
  end

IO.puts("Workers ready.\n")

# Each Benchee parallel slot gets its own worker via process dictionary
get_worker = fn ->
  case Process.get(:bench_worker) do
    nil ->
      idx = :atomics.add_get(:persistent_term.get(:bench_worker_counter), 1, 1)
      worker = Enum.at(workers, rem(idx - 1, length(workers)))
      Process.put(:bench_worker, worker)
      worker
    pid -> pid
  end
end

:persistent_term.put(:bench_worker_counter, :atomics.new(1, signed: false))

# ---------------------------------------------------------------------------
# READ BENCHMARK — compare erpc.call vs BatchWorker
# ---------------------------------------------------------------------------

for depth <- [10, 100, 500] do
  :persistent_term.put(:bench_worker_counter, :atomics.new(1, signed: false))

  scenarios = %{
    "erpc.call #{depth}×GET" => fn ->
      keys = for _ <- 1..depth, do: "bench:#{:rand.uniform(10_000)}"
      call.(FerricStore, :batch_get, [keys])
    end,
    "worker #{depth}×GET" => fn ->
      keys = for _ <- 1..depth, do: "bench:#{:rand.uniform(10_000)}"
      GenServer.call(get_worker.(), {:batch_get, keys})
    end
  }

  IO.puts("=== READ #{depth} commands, #{parallel} parallel ===\n")

  Benchee.run(scenarios,
    time: 10, warmup: 2, parallel: parallel,
    formatters: [Benchee.Formatters.Console]
  )

  IO.puts("")
end

# ---------------------------------------------------------------------------
# QUORUM WRITE BENCHMARK — compare erpc.call vs BatchWorker
# ---------------------------------------------------------------------------

for depth <- [10, 100, 500] do
  :persistent_term.put(:bench_worker_counter, :atomics.new(1, signed: false))

  scenarios = %{
    "erpc.call #{depth}×SET" => fn ->
      kv = for _ <- 1..depth, do: {"bench:q:#{:rand.uniform(key_max)}", payload}
      call.(FerricStore, :batch_set, [kv])
    end,
    "worker #{depth}×SET" => fn ->
      kv = for _ <- 1..depth, do: {"bench:q:#{:rand.uniform(key_max)}", payload}
      GenServer.call(get_worker.(), {:batch_set, kv}, 30_000)
    end
  }

  IO.puts("=== QUORUM WRITE #{depth} commands, #{parallel} parallel ===\n")

  Benchee.run(scenarios,
    time: 10, warmup: 2, parallel: parallel,
    formatters: [Benchee.Formatters.Console]
  )

  IO.puts("")
end

# ---------------------------------------------------------------------------
# MIXED 80/20 — BatchWorker only (erpc.call can't send fns)
# ---------------------------------------------------------------------------

for depth <- [10, 100] do
  :persistent_term.put(:bench_worker_counter, :atomics.new(1, signed: false))
  get_count = div(depth * 4, 5)
  set_count = depth - get_count

  exec_pipe = fn pipe ->
    if remote_node do
      :erpc.call(remote_node, FerricStore.Pipe, :execute, [pipe])
    else
      FerricStore.Pipe.execute(pipe)
    end
  end

  scenarios = %{
    "erpc.call #{get_count}G+#{set_count}S" => fn ->
      pipe = FerricStore.Pipe.new()
      pipe = Enum.reduce(1..get_count, pipe, fn _, p ->
        FerricStore.Pipe.get(p, "bench:#{:rand.uniform(10_000)}")
      end)
      pipe = Enum.reduce(1..set_count, pipe, fn _, p ->
        FerricStore.Pipe.set(p, "bench:q:#{:rand.uniform(key_max)}", payload)
      end)
      exec_pipe.(pipe)
    end,
    "worker #{get_count}G+#{set_count}S" => fn ->
      cmds =
        (for _ <- 1..get_count, do: {:get, "bench:#{:rand.uniform(10_000)}"}) ++
        (for _ <- 1..set_count, do: {:set, "bench:q:#{:rand.uniform(key_max)}", payload, []})
      GenServer.call(get_worker.(), {:batch_mixed, cmds}, 30_000)
    end
  }

  IO.puts("=== MIXED 80/20 #{depth} commands, #{parallel} parallel ===\n")

  Benchee.run(scenarios,
    time: 10, warmup: 2, parallel: parallel,
    formatters: [Benchee.Formatters.Console]
  )

  IO.puts("")
end

# Cleanup workers
Enum.each(workers, &GenServer.stop/1)

IO.puts("\nNOTE: multiply ips × depth for ops/sec")
