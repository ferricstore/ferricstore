# FerricStore high-cardinality Pub/Sub cleanup benchmark.
#
# Run:
#   MIX_ENV=bench mix bench.pubsub_cleanup

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

background_entries = env_integer.("BENCH_PUBSUB_CLEANUP_BACKGROUND", "16384", 1)
target_subscriptions = env_integer.("BENCH_PUBSUB_CLEANUP_TARGET", "8", 1)
warmup = env_number.("BENCH_WARMUP", "1")
time = env_number.("BENCH_TIME", "3")
memory_time = env_number.("BENCH_MEMORY_TIME", "0")

defmodule Ferricstore.Bench.PubSubCleanup do
  @moduledoc false

  def sleeper do
    receive do
      :stop -> :ok
    end
  end

  def stop(pid) do
    monitor = Process.monitor(pid)
    send(pid, :stop)

    receive do
      {:DOWN, ^monitor, :process, ^pid, :normal} -> :ok
    after
      5_000 -> raise "Pub/Sub cleanup benchmark process failed to stop"
    end
  end
end

Application.put_env(:ferricstore, :pubsub_activity_log_max_len, 0)

{:ok, activity_log} = ActivityLog.start_link()
{:ok, pubsub} = PubSub.start_link()

background_pid = spawn(&Ferricstore.Bench.PubSubCleanup.sleeper/0)
target_pid = spawn(&Ferricstore.Bench.PubSubCleanup.sleeper/0)

cleanup = fn ->
  if Process.alive?(pubsub), do: GenServer.stop(pubsub)
  if Process.alive?(activity_log), do: GenServer.stop(activity_log)
  if Process.alive?(background_pid), do: Ferricstore.Bench.PubSubCleanup.stop(background_pid)
  if Process.alive?(target_pid), do: Ferricstore.Bench.PubSubCleanup.stop(target_pid)
end

System.at_exit(fn _status -> cleanup.() end)

suffix = System.unique_integer([:positive, :monotonic])

background_channels =
  for index <- 1..background_entries,
      do: "bench:cleanup:background:channel:#{suffix}:#{index}"

background_patterns =
  for index <- 1..background_entries,
      do: "bench:cleanup:background:pattern:#{suffix}:#{index}:*"

target_channels =
  for index <- 1..target_subscriptions,
      do: "bench:cleanup:target:channel:#{suffix}:#{index}"

target_patterns =
  for index <- 1..target_subscriptions,
      do: "bench:cleanup:target:pattern:#{suffix}:#{index}:*"

:ok = PubSub.subscribe_many(background_channels, background_pid)
:ok = PubSub.psubscribe_many(background_patterns, background_pid)

^background_entries = :ets.info(:ferricstore_pubsub, :size)
^background_entries = :ets.info(:ferricstore_pubsub_patterns, :size)

subscribe_target = fn ->
  :ok = PubSub.subscribe_many(target_channels, target_pid)
  :ok = PubSub.psubscribe_many(target_patterns, target_pid)
  target_pid
end

:ok = PubSub.cleanup(subscribe_target.())
^background_entries = :ets.info(:ferricstore_pubsub, :size)
^background_entries = :ets.info(:ferricstore_pubsub_patterns, :size)

IO.puts("=== FerricStore Pub/Sub Cleanup Benchmark ===")

IO.puts(
  "background_channels=#{background_entries} background_patterns=#{background_entries} " <>
    "target_channels=#{target_subscriptions} target_patterns=#{target_subscriptions}"
)

Benchee.run(
  %{
    "cleanup target subscriptions=#{target_subscriptions * 2}" =>
      {fn pid ->
         :ok = PubSub.cleanup(pid)
       end, before_each: fn _input -> subscribe_target.() end},
    "subscribe target subscriptions=#{target_subscriptions * 2}" =>
      {fn _input ->
         subscribe_target.()
       end,
       before_each: fn _input ->
         :ok = PubSub.cleanup(target_pid)
         :ready
       end,
       after_each: fn _result ->
         :ok = PubSub.cleanup(target_pid)
       end}
  },
  inputs: %{"registry entries=#{background_entries * 2}" => :ready},
  warmup: warmup,
  time: time,
  memory_time: memory_time,
  reduction_time: 0,
  parallel: 1,
  percentiles: [50, 95, 99],
  formatters: [Benchee.Formatters.Console]
)

cleanup.()
