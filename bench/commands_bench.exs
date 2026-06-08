# bench/commands_bench.exs
#
# Benchmarks for the command dispatcher layer.
#
# Run:
#   MIX_ENV=bench mix run bench/commands_bench.exs
#
# Starts the full application so Router/Shard GenServers are available.

alias Ferricstore.Commands.Dispatcher
alias Ferricstore.Store.Router

bench_warmup = System.get_env("BENCH_WARMUP", "2") |> String.to_integer()
bench_time = System.get_env("BENCH_TIME", "5") |> String.to_integer()

# ---------------------------------------------------------------------------
# Application startup
# ---------------------------------------------------------------------------

bench_data_dir = System.tmp_dir!() <> "/ferricstore_cmd_bench_#{:rand.uniform(9_999_999)}"
File.mkdir_p!(bench_data_dir)

Application.put_env(:ferricstore, :data_dir, bench_data_dir)
Application.put_env(:ferricstore, :port, 0)

{:ok, _} = Application.ensure_all_started(:ferricstore)

IO.puts("=== Command Dispatcher Benchmarks ===")
IO.puts("Data dir: #{bench_data_dir}\n")

# ---------------------------------------------------------------------------
# Build the store map (same shape as Connection.build_store/0)
# ---------------------------------------------------------------------------

store = %{
  get: &Router.get/1,
  get_meta: &Router.get_meta/1,
  put: &Router.put/3,
  delete: &Router.delete/1,
  exists?: &Router.exists?/1,
  keys: &Router.keys/0,
  flush: fn ->
    Enum.each(Router.keys(), &Router.delete/1)
    :ok
  end,
  dbsize: &Router.dbsize/0
}

# ---------------------------------------------------------------------------
# Seed data
# ---------------------------------------------------------------------------

# 1000 keys for KEYS and MGET benchmarks
Enum.each(1..1000, fn i ->
  Router.put("cmd_key_#{i}", "cmd_value_#{i}", 0)
end)

# Pre-read to warm ETS
Enum.each(1..1000, fn i -> Router.get("cmd_key_#{i}") end)

# 100 keys for MGET
keys_100 = Enum.map(1..100, fn i -> "cmd_key_#{i}" end)

# 100 key-value pairs for MSET (flat list: [k1, v1, k2, v2, ...])
kv_pairs_100 =
  Enum.flat_map(1..100, fn i ->
    ["mset_key_#{i}", "mset_value_#{i}"]
  end)

counter = :counters.new(1, [:atomics])

# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------

Benchee.run(
  %{
    "Dispatch GET: key present" => fn ->
      _val = Dispatcher.dispatch("GET", ["cmd_key_1"], store)
    end,
    "Dispatch GET: key absent" => fn ->
      _val = Dispatcher.dispatch("GET", ["nonexistent_key"], store)
    end,
    "Dispatch SET: simple" => fn ->
      idx = :counters.get(counter, 1)
      :counters.add(counter, 1, 1)
      Dispatcher.dispatch("SET", ["set_bench_#{idx}", "value"], store)
    end,
    "Dispatch SET: with EX 100" => fn ->
      idx = :counters.get(counter, 1)
      :counters.add(counter, 1, 1)
      Dispatcher.dispatch("SET", ["set_ex_bench_#{idx}", "value", "EX", "100"], store)
    end,
    "Dispatch MGET: 100 keys" => fn ->
      Dispatcher.dispatch("MGET", keys_100, store)
    end,
    "Dispatch MSET: 100 pairs" => fn ->
      Dispatcher.dispatch("MSET", kv_pairs_100, store)
    end,
    "Dispatch KEYS: * (1000 keys)" => fn ->
      Dispatcher.dispatch("KEYS", ["*"], store)
    end
  },
  time: bench_time,
  warmup: bench_warmup,
  memory_time: 1,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML, file: "bench/output/commands.html", auto_open: false}
  ]
)

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

IO.puts("\nCleaning up bench data directory: #{bench_data_dir}")
File.rm_rf!(bench_data_dir)
