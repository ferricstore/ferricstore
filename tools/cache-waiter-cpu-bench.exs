# MIX_ENV=test mix run --no-start tools/cache-waiter-cpu-bench.exs
baseline_ref = System.get_env("CPU_BENCH_BASELINE_REF", "v0.11.18")
iterations = String.to_integer(System.get_env("CPU_BENCH_ITERATIONS", "20000"))

for {path, name} <- [
      {"apps/ferricstore/lib/ferricstore/bitcask/async.ex", Ferricstore.AsyncBenchmarkBaseline},
      {"apps/ferricstore/lib/ferricstore/store/shard/ets.ex", Ferricstore.CacheBenchmarkBaseline}
    ] do
  {source, 0} = System.cmd("git", ["show", baseline_ref <> ":" <> path])
  {:defmodule, metadata, [_name, body]} = Code.string_to_quoted!(source)
  Code.compile_quoted({:defmodule, metadata, [name, body]})
end

measure = fn implementation, scenario, fun ->
  Enum.each(1..1000, fn _ -> fun.() end)
  :erlang.garbage_collect()
  {before_reductions, _} = :erlang.statistics(:reductions)
  {before_cpu, _} = :erlang.statistics(:runtime)
  {us, :ok} = :timer.tc(fn -> Enum.each(1..iterations, fn _ -> fun.() end) end)
  {after_cpu, _} = :erlang.statistics(:runtime)
  {after_reductions, _} = :erlang.statistics(:reductions)

  IO.inspect(%{
    implementation: implementation,
    scenario: scenario,
    iterations: iterations,
    wall_us: us,
    cpu_ms: after_cpu - before_cpu,
    reductions: after_reductions - before_reductions
  })
end

for {mailbox, timeout} <- [{0, :infinity}, {0, 5000}, {10_000, :infinity}] do
  if mailbox > 0, do: Enum.each(1..mailbox, fn _ -> send(self(), :benchmark_unrelated) end)

  for _round <- 1..3,
      module <- [Ferricstore.AsyncBenchmarkBaseline, Ferricstore.Bitcask.Async] do
    measure.(module, {:await, mailbox, timeout}, fn ->
      {:ok, "value"} =
        module.await(
          fn proxy, correlation ->
            send(proxy, {:tokio_complete, correlation, :ok, "value"})
            :ok
          end,
          timeout
        )
    end)
  end

  if mailbox > 0 do
    Enum.each(1..mailbox, fn _ -> receive do: (:benchmark_unrelated -> :ok) end)
  end
end

table = :ets.new(:cache_benchmark, [:set, :public])
state = %{keydir: table, instance_ctx: %{hot_cache_max_value_size: 128}}
value = :binary.copy("x", 4096)

try do
  for _round <- 1..3,
      module <- [Ferricstore.CacheBenchmarkBaseline, Ferricstore.Store.Shard.ETS] do
    :ets.insert(table, {"key", nil, 0, Ferricstore.Store.LFU.initial(), 1, 0, 4096})

    measure.(module, :uncacheable_cold_value, fn ->
      module.cold_read_warm_ets(state, "key", value, 0, 1, 0, 4096)
    end)
  end
after
  :ets.delete(table)
end
