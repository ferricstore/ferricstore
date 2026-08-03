# FerricStore Pub/Sub observability snapshot benchmark.
#
# Run:
#   MIX_ENV=bench mix bench.pubsub_snapshot

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

entries = env_integer.("BENCH_PUBSUB_SNAPSHOT_ENTRIES", "4096", 100)
warmup = env_number.("BENCH_WARMUP", "1")
time = env_number.("BENCH_TIME", "3")
memory_time = env_number.("BENCH_MEMORY_TIME", "0")

Application.put_env(:ferricstore, :pubsub_activity_log_max_len, 0)

{:ok, activity_log} = ActivityLog.start_link()
{:ok, pubsub} = PubSub.start_link()

cleanup = fn ->
  if Process.alive?(pubsub), do: GenServer.stop(pubsub)
  if Process.alive?(activity_log), do: GenServer.stop(activity_log)
end

System.at_exit(fn _status -> cleanup.() end)

suffix = System.unique_integer([:positive, :monotonic])

channels =
  for index <- 1..entries,
      do:
        "bench:snapshot:channel:#{suffix}:#{String.pad_leading(Integer.to_string(index), 8, "0")}"

patterns =
  for index <- 1..entries,
      do:
        "bench:snapshot:pattern:#{suffix}:#{String.pad_leading(Integer.to_string(index), 8, "0")}:*"

:ok = PubSub.subscribe_many(channels, self())
:ok = PubSub.psubscribe_many(patterns, self())

missing_channel = "bench:snapshot:missing:#{suffix}"
query_channels = [missing_channel | channels]

preflight = PubSub.subscription_snapshot(10)
^entries = preflight.exact_subscriptions
^entries = preflight.pattern_subscriptions
10 = length(preflight.channels)
10 = length(preflight.patterns)

[^missing_channel, 0 | numsub_tail] = PubSub.numsub(query_channels)
true = length(numsub_tail) == entries * 2

IO.puts("=== FerricStore Pub/Sub Snapshot Benchmark ===")
IO.puts("channels=#{entries} patterns=#{entries}")

Benchee.run(
  %{
    "subscription snapshot limit=10" => fn ->
      %{channels: channels, patterns: patterns} = PubSub.subscription_snapshot(10)
      10 = length(channels)
      10 = length(patterns)
    end,
    "subscription snapshot limit=100" => fn ->
      %{channels: channels, patterns: patterns} = PubSub.subscription_snapshot(100)
      100 = length(channels)
      100 = length(patterns)
    end,
    "numsub channels=#{entries + 1}" => fn ->
      PubSub.numsub(query_channels)
    end
  },
  warmup: warmup,
  time: time,
  memory_time: memory_time,
  reduction_time: 0,
  parallel: 1,
  percentiles: [50, 95, 99],
  formatters: [Benchee.Formatters.Console]
)

cleanup.()
