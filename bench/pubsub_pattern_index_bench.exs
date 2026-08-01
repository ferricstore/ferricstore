# FerricStore high-cardinality Pub/Sub pattern-index benchmark.
#
# Run:
#   MIX_ENV=bench mix bench.pubsub_pattern_index

alias Ferricstore.PubSub
alias Ferricstore.PubSub.ActivityLog

Logger.configure(level: :warning)

env_integer = fn name, default, minimum ->
  case Integer.parse(System.get_env(name, default)) do
    {value, ""} when value >= minimum -> value
    _ -> raise ArgumentError, "#{name} must be an integer >= #{minimum}"
  end
end

env_number = fn name, default ->
  case Float.parse(System.get_env(name, default)) do
    {value, ""} when value >= 0 -> value
    _ -> raise ArgumentError, "#{name} must be a non-negative number"
  end
end

entries = env_integer.("BENCH_PUBSUB_PATTERN_INDEX_ENTRIES", "4096", 2)
warmup = env_number.("BENCH_WARMUP", "1")
time = env_number.("BENCH_TIME", "3")

defmodule Ferricstore.Bench.PubSubPatternIndex do
  @moduledoc false

  alias Ferricstore.PubSub

  def drain do
    receive do
      {:pubsub_pmessage, _pattern, _channel, _message} -> drain()
      :stop -> :ok
    end
  end

  def run_case(name, patterns, match_channel, miss_channel, sink, options) do
    :ok = PubSub.psubscribe_many(patterns, sink)
    1 = PubSub.publish(match_channel, "x")
    0 = PubSub.publish(miss_channel, "x")

    Benchee.run(
      %{
        "#{name} match" => fn -> 1 = PubSub.publish(match_channel, "x") end,
        "#{name} miss" => fn -> 0 = PubSub.publish(miss_channel, "x") end
      },
      options
    )

    :ok = PubSub.cleanup(sink)
  end
end

Application.put_env(:ferricstore, :pubsub_activity_log_max_len, 0)

{:ok, activity_log} = ActivityLog.start_link()
{:ok, pubsub} = PubSub.start_link()
sink = spawn(&Ferricstore.Bench.PubSubPatternIndex.drain/0)

cleanup = fn ->
  if Process.alive?(pubsub), do: GenServer.stop(pubsub)
  if Process.alive?(activity_log), do: GenServer.stop(activity_log)
  if Process.alive?(sink), do: send(sink, :stop)
end

System.at_exit(fn _status -> cleanup.() end)

suffix = System.unique_integer([:positive, :monotonic])

options = [
  warmup: warmup,
  time: time,
  memory_time: 0,
  reduction_time: 0,
  parallel: 1,
  percentiles: [50, 95, 99],
  formatters: [Benchee.Formatters.Console]
]

cases = [
  {"exact",
   ["bench:index:exact:target:#{suffix}" | for(i <- 2..entries, do: "exact:#{suffix}:#{i}")],
   "bench:index:exact:target:#{suffix}", "bench:index:exact:miss:#{suffix}"},
  {"prefix",
   [
     "bench:index:prefix:target:#{suffix}:*"
     | for(i <- 2..entries, do: "prefix:#{suffix}:#{i}:*")
   ], "bench:index:prefix:target:#{suffix}:value", "bench:index:prefix:miss:#{suffix}"},
  {"suffix",
   [
     "*:bench:index:suffix:target:#{suffix}"
     | for(i <- 2..entries, do: "*:suffix:#{suffix}:#{i}")
   ], "value:bench:index:suffix:target:#{suffix}", "bench:index:suffix:miss:#{suffix}"},
  {"glob",
   [
     "bench:index:glob:target:#{suffix}:?"
     | for(i <- 2..entries, do: "glob:#{suffix}:#{i}:?")
   ], "bench:index:glob:target:#{suffix}:x", "bench:index:glob:miss:#{suffix}"},
  {"glob suffix",
   [
     "?:bench:index:glob-suffix:target:#{suffix}"
     | for(i <- 2..entries, do: "?:glob-suffix:#{suffix}:#{i}")
   ], "x:bench:index:glob-suffix:target:#{suffix}", "bench:index:glob-suffix:miss:#{suffix}"},
  {"glob unanchored",
   [
     "?*bench:index:glob-unanchored:target:#{suffix}*?"
     | for(i <- 2..entries, do: "?*glob-unanchored:#{suffix}:#{i}*?")
   ], "xbench:index:glob-unanchored:target:#{suffix}y",
   "xbench:index:glob-unanchored:miss:#{suffix}y"}
]

IO.puts("=== FerricStore Pub/Sub Pattern Index Benchmark ===")
IO.puts("patterns_per_case=#{entries}")

Enum.each(cases, fn {name, patterns, match_channel, miss_channel} ->
  Ferricstore.Bench.PubSubPatternIndex.run_case(
    name,
    patterns,
    match_channel,
    miss_channel,
    sink,
    options
  )
end)

cleanup.()
