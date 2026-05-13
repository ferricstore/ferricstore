# 4-cell read/write quorum benchmark for v0.3.6 cluster.
#
# Measures throughput (ops/sec) and latency percentiles (p50/p99/p99.9)
# across the four cells:
#
#   write  — SET through Raft
#   read   — GET on pre-populated keys
#
# Run from a client node connected to the cluster:
#
#   MIX_ENV=bench elixir --sname bench_4cell --cookie ferricstore_bench \
#     -S mix run --no-start bench/erpc_4cell.exs --remote ferricstore@HOST

remote_node =
  case System.argv() do
    ["--remote", node_str] -> String.to_atom(node_str)
    _ -> raise "Usage: --remote ferricstore@HOST"
  end

IO.puts("Connecting to #{remote_node}...")
Node.connect(remote_node) || raise "Cannot connect to #{remote_node}"

payload = String.duplicate("x", 256)
n_keys = 100_000
parallel = 50
duration_s = 15
warmup_s = 3

# ---- Pre-populate ----

IO.puts("Pre-populating #{n_keys} keys...")

for chunk <- Enum.chunk_every(1..n_keys, 500) do
  pairs = Enum.map(chunk, fn i -> {"q:#{i}", payload} end)
  :erpc.call(remote_node, FerricStore, :batch_set, [pairs])
end

IO.puts("Pre-populated.\n")

# ---- Helpers ----

defmodule Cell do
  @doc """
  Runs `fun` for `duration_s` seconds with `parallel` workers and a `warmup_s`
  warmup period. Returns %{ops_sec, p50_us, p99_us, p999_us}.
  """
  def measure(label, fun, parallel, duration_s, warmup_s) do
    IO.puts("=== #{label} (parallel=#{parallel}, duration=#{duration_s}s) ===")

    # Warmup
    warmup_until = System.monotonic_time(:millisecond) + warmup_s * 1000
    warmup_pids =
      for _ <- 1..parallel do
        spawn_link(fn -> warmup_loop(fun, warmup_until) end)
      end
    Enum.each(warmup_pids, fn pid ->
      ref = Process.monitor(pid)
      receive do {:DOWN, ^ref, :process, ^pid, _} -> :ok end
    end)

    # Real run
    parent = self()
    deadline = System.monotonic_time(:millisecond) + duration_s * 1000

    workers =
      for _ <- 1..parallel do
        spawn_link(fn ->
          send(parent, {:result, run_loop(fun, deadline, [], 0)})
        end)
      end

    {all_latencies, total_ops} =
      Enum.reduce(workers, {[], 0}, fn _, {ls, o} ->
        receive do {:result, {worker_ls, worker_ops}} -> {worker_ls ++ ls, o + worker_ops} end
      end)

    ops_sec = total_ops / duration_s

    sorted = Enum.sort(all_latencies)
    n = length(sorted)
    p50 = pct(sorted, n, 0.50)
    p99 = pct(sorted, n, 0.99)
    p999 = pct(sorted, n, 0.999)

    result = %{
      label: label,
      ops_sec: round(ops_sec),
      p50_us: p50,
      p99_us: p99,
      p999_us: p999,
      total_ops: total_ops,
      duration_s: duration_s,
      parallel: parallel
    }

    IO.puts("  ops_sec = #{result.ops_sec}")
    IO.puts("  p50 = #{p50} µs   p99 = #{p99} µs   p99.9 = #{p999} µs")
    IO.puts("")
    result
  end

  defp warmup_loop(_fun, deadline) do
    if System.monotonic_time(:millisecond) >= deadline, do: :ok, else: (fn -> :ok end).()
  end

  defp run_loop(fun, deadline, latencies, ops) do
    if System.monotonic_time(:millisecond) >= deadline do
      {latencies, ops}
    else
      t0 = System.monotonic_time(:microsecond)
      _ = fun.()
      lat = System.monotonic_time(:microsecond) - t0
      run_loop(fun, deadline, [lat | latencies], ops + 1)
    end
  end

  defp pct(sorted, n, p) when n > 0, do: Enum.at(sorted, min(n - 1, round(n * p)))
  defp pct(_, _, _), do: 0
end

# ---- Run cells ----

results = []

# Write: SET to default namespace
key_counter = :counters.new(1, [])
results = results ++ [
  Cell.measure(
    "write",
    fn ->
      i = :counters.get(key_counter, 1)
      :counters.add(key_counter, 1, 1)
      :erpc.call(remote_node, FerricStore, :set, ["w:#{i}", payload])
    end,
    parallel, duration_s, warmup_s
  )
]

# Read: GET pre-populated keys
results = results ++ [
  Cell.measure(
    "read",
    fn ->
      i = :rand.uniform(n_keys)
      :erpc.call(remote_node, FerricStore, :get, ["q:#{i}"])
    end,
    parallel, duration_s, warmup_s
  )
]

# ---- Summary ----

IO.puts("\n========== 4-CELL SUMMARY ==========")
IO.puts(String.pad_trailing("cell", 16) <> String.pad_leading("ops/sec", 12) <>
        String.pad_leading("p50 µs", 12) <> String.pad_leading("p99 µs", 12) <>
        String.pad_leading("p99.9 µs", 14))
Enum.each(results, fn r ->
  IO.puts(
    String.pad_trailing(r.label, 16) <>
    String.pad_leading("#{r.ops_sec}", 12) <>
    String.pad_leading("#{r.p50_us}", 12) <>
    String.pad_leading("#{r.p99_us}", 12) <>
    String.pad_leading("#{r.p999_us}", 14)
  )
end)

# ---- Outputs (CSV + Markdown) ----

stamp = Calendar.strftime(DateTime.utc_now(), "%Y%m%d_%H%M%S")
csv_path = "/tmp/erpc_4cell_#{stamp}.csv"
md_path = "/tmp/erpc_4cell_#{stamp}.md"

csv_lines = ["cell,ops_sec,p50_us,p99_us,p999_us,total_ops,duration_s,parallel"]
csv_lines = csv_lines ++ Enum.map(results, fn r ->
  "#{r.label},#{r.ops_sec},#{r.p50_us},#{r.p99_us},#{r.p999_us},#{r.total_ops},#{r.duration_s},#{r.parallel}"
end)
File.write!(csv_path, Enum.join(csv_lines, "\n") <> "\n")

md = """
# FerricStore v0.3.6 — 4-cell read/write quorum benchmark

- Date (UTC): #{DateTime.utc_now() |> DateTime.to_string()}
- Cluster: 3 nodes, #{:erpc.call(remote_node, Application, :get_env, [:ferricstore, :shard_count, "?"])} shards
- Remote node: `#{inspect(remote_node)}`
- Payload: 256 bytes
- Parallel workers: #{parallel}
- Run duration: #{duration_s}s (after #{warmup_s}s warmup)
- Pre-populated keys: #{n_keys}

## Results

| cell | ops/sec | p50 (µs) | p99 (µs) | p99.9 (µs) | total ops |
|---|---:|---:|---:|---:|---:|
""" <> Enum.map_join(results, "\n", fn r ->
  "| #{r.label} | #{r.ops_sec} | #{r.p50_us} | #{r.p99_us} | #{r.p999_us} | #{r.total_ops} |"
end) <> """


## Notes

- `write`: SET through Raft consensus and Bitcask.
- `read`: GET on pre-populated keys, served from ETS hot cache or pread fallback.

CSV: `#{csv_path}`
"""

File.write!(md_path, md)

IO.puts("\nCSV: #{csv_path}")
IO.puts("MD:  #{md_path}")
