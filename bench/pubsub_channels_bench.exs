# FerricStore PUBSUB CHANNELS filter benchmark.
#
# Run:
#   MIX_ENV=bench mix bench.pubsub_channels

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

entries = env_integer.("BENCH_PUBSUB_CHANNEL_ENTRIES", "16384", 16)
warmup = env_number.("BENCH_WARMUP", "1")
time = env_number.("BENCH_TIME", "3")

Application.put_env(:ferricstore, :pubsub_activity_log_max_len, 0)

{:ok, activity_log} = ActivityLog.start_link()
{:ok, pubsub} = PubSub.start_link()
subscriber = spawn(fn -> Process.sleep(:infinity) end)

cleanup = fn ->
  if Process.alive?(pubsub), do: GenServer.stop(pubsub)
  if Process.alive?(activity_log), do: GenServer.stop(activity_log)
  if Process.alive?(subscriber), do: Process.exit(subscriber, :kill)
end

System.at_exit(fn _status -> cleanup.() end)

suffix = System.unique_integer([:positive, :monotonic])

channels =
  for index <- 1..entries do
    padded = index |> Integer.to_string() |> String.pad_leading(8, "0")
    "bench:channels:#{suffix}:#{padded}:tail"
  end

:ok = PubSub.subscribe_many(channels, subscriber)

literal = Enum.at(channels, div(entries, 2))
prefix = "bench:channels:#{suffix}:0000001*"
suffix_pattern = "*:tail"
complex = "bench:channels:#{suffix}:0000000[1-8]:tail"

1 = length(PubSub.channels(literal))
prefix_count = length(PubSub.channels(prefix))
^entries = length(PubSub.channels(suffix_pattern))
complex_count = length(PubSub.channels(complex))

IO.puts("=== FerricStore PUBSUB CHANNELS Benchmark ===")
IO.puts("active_channels=#{entries}")

Benchee.run(
  %{
    "literal filter" => fn -> 1 = length(PubSub.channels(literal)) end,
    "prefix filter" => fn -> ^prefix_count = length(PubSub.channels(prefix)) end,
    "suffix filter" => fn -> ^entries = length(PubSub.channels(suffix_pattern)) end,
    "complex glob filter" => fn -> ^complex_count = length(PubSub.channels(complex)) end
  },
  warmup: warmup,
  time: time,
  memory_time: 0,
  reduction_time: 0,
  parallel: 1,
  percentiles: [50, 95, 99],
  formatters: [Benchee.Formatters.Console]
)

cleanup.()
