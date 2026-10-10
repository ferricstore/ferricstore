# Stats hotness microbenchmark.
#
# Run from the repository root with:
#
#     mise exec -- env ERL_FLAGS='+S 2:2' MIX_ENV=test mix run --no-start tools/stats-hotness-bench.exs
#
# The overflow case must be measured only after the table has reached its
# named prefix cap. The coordinator reduction delta makes accidental
# GenServer.call traffic visible even when wall-clock noise is high.

alias Ferricstore.Stats

iterations =
  case System.get_env("FERRICSTORE_STATS_HOTNESS_ITERATIONS") do
    nil -> 20_000
    value -> String.to_integer(value)
  end

{:ok, _started} = Application.ensure_all_started(:ferricstore)

try do
  Stats.reset_hotness()

  tracked_prefix = "stats-bench-tracked-#{System.unique_integer([:positive])}"
  tracked_key = "#{tracked_prefix}:key"
  Stats.record_hot_read(tracked_key)

  for index <- 1..998 do
    Stats.record_hot_read("stats-bench-named-#{index}:key")
  end

  overflow_key = "stats-bench-overflow:key"
  Stats.record_hot_read(overflow_key)

  stats = Process.whereis(Stats)

  measure = fn label, key ->
    {:reductions, before_reductions} = Process.info(stats, :reductions)

    {elapsed_us, :ok} =
      :timer.tc(fn ->
        for _ <- 1..iterations, do: Stats.record_hot_read(key)
        :ok
      end)

    {:reductions, after_reductions} = Process.info(stats, :reductions)

    IO.inspect(%{
      case: label,
      iterations: iterations,
      elapsed_us: elapsed_us,
      us_per_op: elapsed_us / iterations,
      coordinator_reductions: after_reductions - before_reductions
    })
  end

  IO.puts("Stats hotness benchmark")
  measure.("tracked", tracked_key)
  measure.("overflow", overflow_key)
  IO.inspect(:ets.info(:ferricstore_hotness, :size), label: "hotness_table_size")
after
  Application.stop(:ferricstore)
end
