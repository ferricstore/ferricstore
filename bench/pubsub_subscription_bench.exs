# FerricStore Pub/Sub subscription-cache mutation benchmark.
#
# Run:
#   MIX_ENV=bench mix bench.pubsub_subscription

alias Ferricstore.PubSub
alias Ferricstore.PubSub.ActivityLog

Logger.configure(level: :warning)

env_number = fn name, default ->
  case Float.parse(System.get_env(name, default)) do
    {value, ""} when value >= 0 -> value
    _ -> raise ArgumentError, "#{name} must be a non-negative number"
  end
end

fanouts =
  System.get_env("BENCH_PUBSUB_SUBSCRIPTION_FANOUTS", "1,32,256")
  |> String.split(",", trim: true)
  |> Enum.map(fn raw ->
    case Integer.parse(String.trim(raw)) do
      {value, ""} when value >= 1 -> value
      _ -> raise ArgumentError, "BENCH_PUBSUB_SUBSCRIPTION_FANOUTS must contain integers >= 1"
    end
  end)
  |> Enum.uniq()

warmup = env_number.("BENCH_WARMUP", "1")
time = env_number.("BENCH_TIME", "3")

batch_size =
  case Integer.parse(System.get_env("BENCH_PUBSUB_SUBSCRIPTION_BATCH", "256")) do
    {value, ""} when value >= 1 -> value
    _ -> raise ArgumentError, "BENCH_PUBSUB_SUBSCRIPTION_BATCH must be an integer >= 1"
  end

pid_cardinality =
  case Integer.parse(System.get_env("BENCH_PUBSUB_PID_CARDINALITY", "16384")) do
    {value, ""} when value >= 1 -> value
    _ -> raise ArgumentError, "BENCH_PUBSUB_PID_CARDINALITY must be an integer >= 1"
  end

defmodule Ferricstore.Bench.PubSubSubscription do
  @moduledoc false

  alias Ferricstore.PubSub

  def sleeper do
    receive do
      :stop -> :ok
    end
  end

  def start_background(fanout, channel, pattern) do
    for _index <- 1..fanout do
      pid = spawn(&sleeper/0)
      :ok = PubSub.subscribe(channel, pid)
      :ok = PubSub.psubscribe(pattern, pid)
      pid
    end
  end

  def stop_background(pids) do
    Enum.each(pids, fn pid ->
      :ok = PubSub.cleanup(pid)
      send(pid, :stop)
    end)
  end
end

Application.put_env(:ferricstore, :pubsub_activity_log_max_len, 0)

{:ok, activity_log} = ActivityLog.start_link()
{:ok, pubsub} = PubSub.start_link()
target = spawn(&Ferricstore.Bench.PubSubSubscription.sleeper/0)

cleanup = fn ->
  if Process.alive?(pubsub), do: GenServer.stop(pubsub)
  if Process.alive?(activity_log), do: GenServer.stop(activity_log)
  if Process.alive?(target), do: send(target, :stop)
end

System.at_exit(fn _status -> cleanup.() end)

options = [
  warmup: warmup,
  time: time,
  memory_time: 0,
  reduction_time: 0,
  parallel: 1,
  percentiles: [50, 95, 99],
  formatters: [Benchee.Formatters.Console]
]

suffix = System.unique_integer([:positive, :monotonic])

IO.puts("=== FerricStore Pub/Sub Subscription Benchmark ===")

Enum.each(fanouts, fn fanout ->
  channel = "bench:subscription:#{suffix}:#{fanout}"
  pattern = "bench:subscription:#{suffix}:#{fanout}:*"
  background = Ferricstore.Bench.PubSubSubscription.start_background(fanout, channel, pattern)

  Benchee.run(
    %{
      "exact subscribe fanout=#{fanout}" =>
        {fn _ -> :ok = PubSub.subscribe(channel, target) end,
         before_each: fn _ -> PubSub.unsubscribe(channel, target) end,
         after_each: fn _ -> PubSub.unsubscribe(channel, target) end},
      "exact unsubscribe fanout=#{fanout}" =>
        {fn _ -> :ok = PubSub.unsubscribe(channel, target) end,
         before_each: fn _ -> PubSub.subscribe(channel, target) end,
         after_each: fn _ -> PubSub.unsubscribe(channel, target) end},
      "pattern subscribe fanout=#{fanout}" =>
        {fn _ -> :ok = PubSub.psubscribe(pattern, target) end,
         before_each: fn _ -> PubSub.punsubscribe(pattern, target) end,
         after_each: fn _ -> PubSub.punsubscribe(pattern, target) end},
      "pattern unsubscribe fanout=#{fanout}" =>
        {fn _ -> :ok = PubSub.punsubscribe(pattern, target) end,
         before_each: fn _ -> PubSub.psubscribe(pattern, target) end,
         after_each: fn _ -> PubSub.punsubscribe(pattern, target) end}
    },
    options
  )

  :ok = PubSub.cleanup(target)
  Ferricstore.Bench.PubSubSubscription.stop_background(background)
end)

batch_channels =
  for index <- 1..batch_size, do: "bench:subscription:batch:#{suffix}:#{index}"

Benchee.run(
  %{
    "public deduplicating batch=#{batch_size}" =>
      {fn _ -> PubSub.subscribe_many(batch_channels, target) end,
       before_each: fn _ -> PubSub.cleanup(target) end,
       after_each: fn _ -> PubSub.cleanup(target) end},
    "trusted unique batch=#{batch_size}" =>
      {fn _ -> PubSub.subscribe_unique_many(batch_channels, target) end,
       before_each: fn _ -> PubSub.cleanup(target) end,
       after_each: fn _ -> PubSub.cleanup(target) end}
  },
  options
)

cardinality_channels =
  for index <- 1..pid_cardinality,
      do: "bench:subscription:pid-cardinality:#{suffix}:#{index}"

cardinality_target = "bench:subscription:pid-cardinality:#{suffix}:target"
:ok = PubSub.subscribe_unique_many(cardinality_channels, target)

Benchee.run(
  %{
    "subscribe with existing pid cardinality=#{pid_cardinality}" =>
      {fn _ -> PubSub.subscribe(cardinality_target, target) end,
       before_each: fn _ -> PubSub.unsubscribe(cardinality_target, target) end,
       after_each: fn _ -> PubSub.unsubscribe(cardinality_target, target) end}
  },
  options
)

:ok = PubSub.cleanup(target)

cleanup.()
