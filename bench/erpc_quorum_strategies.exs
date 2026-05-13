# Quorum write throughput via erpc: explore batching strategies.
#
# Measures how many ops/sec we can sustain on the SET (quorum) path with
# different batch sizes and concurrencies, all over Erlang distribution.
#
# Usage:
#   MIX_ENV=prod elixir --sname bench --cookie ferricstore_bench \
#     -S mix run --no-start bench/erpc_quorum_strategies.exs --remote ferricstore@HOST

remote_node =
  case System.argv() do
    ["--remote", n] -> String.to_atom(n)
    _ -> raise "Usage: --remote ferricstore@HOST"
  end

Node.connect(remote_node) || raise "cannot connect to #{remote_node}"
IO.puts("connected to #{remote_node}")

payload = String.duplicate("x", 256)

# ---- helpers ----

defmodule Driver do
  def run_workers(workers_count, fun, duration_ms) do
    parent = self()
    deadline = System.monotonic_time(:millisecond) + duration_ms

    pids =
      for _ <- 1..workers_count do
        spawn_link(fn -> loop(fun, deadline, 0, [], parent) end)
      end

    # Collect results
    {total_ops, all_lats} =
      Enum.reduce(pids, {0, []}, fn _, {ops, lats} ->
        receive do
          {:done, w_ops, w_lats} -> {ops + w_ops, w_lats ++ lats}
        end
      end)

    {total_ops, all_lats}
  end

  defp loop(fun, deadline, ops, lats, parent) do
    if System.monotonic_time(:millisecond) >= deadline do
      send(parent, {:done, ops, lats})
    else
      t0 = System.monotonic_time(:microsecond)
      _ = fun.()
      lat = System.monotonic_time(:microsecond) - t0
      loop(fun, deadline, ops + 1, [lat | lats], parent)
    end
  end

  def pct(sorted, n, p) when n > 0, do: Enum.at(sorted, min(n - 1, round(n * p)))
  def pct(_, _, _), do: 0

  def report(label, ops, lats, duration_s) do
    n = length(lats)
    sorted = Enum.sort(lats)
    p50 = pct(sorted, n, 0.50)
    p99 = pct(sorted, n, 0.99)
    p999 = pct(sorted, n, 0.999)
    avg = if n > 0, do: div(Enum.sum(lats), n), else: 0
    ops_sec = ops / duration_s
    IO.puts("  #{label}: ops/s=#{round(ops_sec)}, calls=#{n}, avg=#{avg}µs p50=#{p50}µs p99=#{p99}µs p99.9=#{p999}µs")
    %{label: label, ops_sec: round(ops_sec), p50: p50, p99: p99, p999: p999, calls: n}
  end
end

# ---- ensure a clean ferricstore on the remote — flush all keys ----

IO.puts("\nWarming remote FerricStore...")
:erpc.call(remote_node, FerricStore, :set, ["__warmup__", payload])
IO.puts("ok\n")

duration_s = 15
duration_ms = duration_s * 1000

# ---- strategy 1: single set per call (no batching, no pipeline) ----

results = []

IO.puts("=== STRATEGY 1: single set per erpc call (workers vary) ===")
results = results ++
  for workers <- [1, 10, 50, 100] do
    counter = :atomics.new(1, signed: false)
    {ops, lats} = Driver.run_workers(workers, fn ->
      i = :atomics.add_get(counter, 1, 1)
      :erpc.call(remote_node, FerricStore, :set, ["s1:#{workers}:#{i}", payload])
    end, duration_ms)
    Driver.report("single set (w=#{workers})", ops, lats, duration_s)
  end

# ---- strategy 2: batch_set per erpc call (batch sizes vary) ----

IO.puts("\n=== STRATEGY 2: batch_set per call, 50 workers ===")
results = results ++
  for batch <- [10, 50, 100, 500, 1000] do
    counter = :atomics.new(1, signed: false)
    {ops, lats} = Driver.run_workers(50, fn ->
      base = :atomics.add_get(counter, 1, batch)
      kvs = for i <- (base - batch + 1)..base, do: {"s2:b#{batch}:#{i}", payload}
      :erpc.call(remote_node, FerricStore, :batch_set, [kvs])
    end, duration_ms)
    # ops here is "calls"; multiply by batch for ops_sec
    actual_ops = ops * batch
    actual_ops_sec = actual_ops / duration_s
    n = length(lats)
    sorted = Enum.sort(lats)
    p50 = Driver.pct(sorted, n, 0.50)
    p99 = Driver.pct(sorted, n, 0.99)
    p999 = Driver.pct(sorted, n, 0.999)
    IO.puts("  batch=#{batch} (w=50): ops/s=#{round(actual_ops_sec)} (calls/s=#{round(ops/duration_s)}), p50=#{p50}µs p99=#{p99}µs p99.9=#{p999}µs")
    %{label: "batch_set b=#{batch} w=50", ops_sec: round(actual_ops_sec), p50: p50, p99: p99, p999: p999, calls: n}
  end

# ---- strategy 3: batch_set, vary workers (batch=100) ----

IO.puts("\n=== STRATEGY 3: batch_set b=100, vary workers ===")
results = results ++
  for workers <- [1, 10, 50, 100, 200] do
    counter = :atomics.new(1, signed: false)
    {ops, lats} = Driver.run_workers(workers, fn ->
      base = :atomics.add_get(counter, 1, 100)
      kvs = for i <- (base - 99)..base, do: {"s3:w#{workers}:#{i}", payload}
      :erpc.call(remote_node, FerricStore, :batch_set, [kvs])
    end, duration_ms)
    actual_ops = ops * 100
    actual_ops_sec = actual_ops / duration_s
    n = length(lats)
    sorted = Enum.sort(lats)
    p50 = Driver.pct(sorted, n, 0.50)
    p99 = Driver.pct(sorted, n, 0.99)
    p999 = Driver.pct(sorted, n, 0.999)
    IO.puts("  w=#{workers} batch=100: ops/s=#{round(actual_ops_sec)}, p50=#{p50}µs p99=#{p99}µs p99.9=#{p999}µs")
    %{label: "batch=100 w=#{workers}", ops_sec: round(actual_ops_sec), p50: p50, p99: p99, p999: p999, calls: n}
  end

# ---- output ----

IO.puts("\n========== SUMMARY (quorum writes via erpc) ==========")
IO.puts(String.pad_trailing("strategy", 32) <>
        String.pad_leading("ops/sec", 12) <>
        String.pad_leading("p50 µs", 12) <>
        String.pad_leading("p99 µs", 12) <>
        String.pad_leading("p99.9 µs", 14))
Enum.each(results, fn r ->
  IO.puts(
    String.pad_trailing(r.label, 32) <>
    String.pad_leading("#{r.ops_sec}", 12) <>
    String.pad_leading("#{r.p50}", 12) <>
    String.pad_leading("#{r.p99}", 12) <>
    String.pad_leading("#{r.p999}", 14)
  )
end)

stamp = Calendar.strftime(DateTime.utc_now(), "%Y%m%d_%H%M%S")
csv_path = "/tmp/erpc_quorum_strategies_#{stamp}.csv"
File.write!(csv_path,
  "strategy,ops_sec,p50_us,p99_us,p999_us,calls\n" <>
    Enum.map_join(results, "\n", fn r ->
      "\"#{r.label}\",#{r.ops_sec},#{r.p50},#{r.p99},#{r.p999},#{r.calls}"
    end))
IO.puts("\nCSV: #{csv_path}")
