# FerricStore embedded Pub/Sub benchmark.
#
# Run:
#   MIX_ENV=bench mix run --no-start bench/pubsub_bench.exs
#
# This runner measures registry lookup, delivery admission, BEAM mailbox
# insertion, pattern matching, and the production activity log. It excludes
# native-protocol request/event encoding and socket I/O.

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

subscriber_counts =
  System.get_env("BENCH_PUBSUB_SUBSCRIBERS", "0,1,8,64")
  |> String.split(",", trim: true)
  |> Enum.map(fn raw ->
    case Integer.parse(String.trim(raw)) do
      {value, ""} when value >= 0 -> value
      _ -> raise ArgumentError, "BENCH_PUBSUB_SUBSCRIBERS entries must be integers >= 0"
    end
  end)
  |> Enum.uniq()

if subscriber_counts == [] do
  raise ArgumentError, "BENCH_PUBSUB_SUBSCRIBERS cannot be empty"
end

pattern_subscribers = env_integer.("BENCH_PUBSUB_PATTERN_SUBSCRIBERS", "64", 1)
message_bytes = env_integer.("BENCH_PUBSUB_MESSAGE_BYTES", "256", 0)
activity_log_max_len = env_integer.("BENCH_PUBSUB_ACTIVITY_LOG_MAX_LEN", "512", 0)
warmup = env_number.("BENCH_WARMUP", "2")
time = env_number.("BENCH_TIME", "5")
memory_time = env_number.("BENCH_MEMORY_TIME", "0")
parallel = env_integer.("BENCH_PARALLEL", "1", 1)

defmodule Ferricstore.Bench.PubSub do
  @moduledoc false

  alias Ferricstore.PubSub

  def start_subscribers(0, _channel, _guarded?), do: []

  def start_subscribers(count, channel, guarded?) do
    for _index <- 1..count do
      parent = self()

      pid =
        spawn_link(fn ->
          initialize_subscriber(parent, channel, guarded?)
        end)

      receive do
        {:pubsub_bench_ready, ^pid} -> pid
      after
        5_000 -> raise "Pub/Sub benchmark subscriber failed to start"
      end
    end
  end

  defp initialize_subscriber(parent, channel, guarded?) do
    if guarded? do
      :ok = PubSub.set_delivery_guard(self(), fn _bytes -> {:ok, :bench_lease} end)
    end

    :ok = PubSub.subscribe(channel, self())
    send(parent, {:pubsub_bench_ready, self()})
    drain()
  end

  def start_pattern_subscribers(count, pattern) do
    for _index <- 1..count do
      parent = self()

      pid =
        spawn_link(fn ->
          :ok = PubSub.psubscribe(pattern, self())
          send(parent, {:pubsub_bench_ready, self()})
          drain()
        end)

      receive do
        {:pubsub_bench_ready, ^pid} -> pid
      after
        5_000 -> raise "Pub/Sub benchmark pattern subscriber failed to start"
      end
    end
  end

  def stop_subscribers(subscribers) do
    monitored = Enum.map(subscribers, &{&1, Process.monitor(&1)})

    Enum.each(monitored, fn {pid, _monitor} ->
      :ok = PubSub.cleanup(pid)
      Process.unlink(pid)
      Process.exit(pid, :shutdown)
    end)

    Enum.each(monitored, fn {pid, monitor} ->
      receive do
        {:DOWN, ^monitor, :process, ^pid, :shutdown} -> :ok
      after
        5_000 -> raise "Pub/Sub benchmark subscriber failed to stop"
      end
    end)
  end

  defp drain do
    receive do
      {:pubsub_message, _channel, _message} -> drain()
      {:pubsub_message, _channel, _message, _lease} -> drain()
      {:pubsub_pmessage, _pattern, _channel, _message} -> drain()
      {:pubsub_pmessage, _pattern, _channel, _message, _lease} -> drain()
    end
  end
end

Application.put_env(:ferricstore, :pubsub_activity_log_max_len, activity_log_max_len)

{:ok, activity_log} = ActivityLog.start_link()
{:ok, pubsub} = PubSub.start_link()

cleanup = fn ->
  if Process.alive?(pubsub), do: GenServer.stop(pubsub)
  if Process.alive?(activity_log), do: GenServer.stop(activity_log)
end

System.at_exit(fn _status -> cleanup.() end)

message = :binary.copy("x", message_bytes)
suffix = System.unique_integer([:positive, :monotonic])

exact_setups =
  for count <- subscriber_counts do
    channel = "bench:pubsub:exact:#{count}:#{suffix}"
    subscribers = Ferricstore.Bench.PubSub.start_subscribers(count, channel, false)
    {count, channel, subscribers}
  end

guarded_setups =
  for count <- Enum.reject(subscriber_counts, &(&1 == 0)) do
    channel = "bench:pubsub:guarded:#{count}:#{suffix}"
    subscribers = Ferricstore.Bench.PubSub.start_subscribers(count, channel, true)
    {count, channel, subscribers}
  end

exact_jobs =
  Enum.reduce(exact_setups, %{}, fn {count, channel, _subscribers}, jobs ->
    Map.put(jobs, "exact publish fanout=#{count}", fn ->
      ^count = PubSub.publish(channel, message)
    end)
  end)

exact_jobs =
  Enum.reduce(guarded_setups, exact_jobs, fn {count, channel, _subscribers}, jobs ->
    Map.put(jobs, "guarded exact publish fanout=#{count}", fn ->
      ^count = PubSub.publish(channel, message)
    end)
  end)

IO.puts("=== FerricStore Pub/Sub Benchmark ===")

IO.puts(
  "message_bytes=#{message_bytes} activity_log_max_len=#{activity_log_max_len} " <>
    "parallel=#{parallel} subscriber_counts=#{Enum.join(subscriber_counts, ",")}"
)

Benchee.run(
  exact_jobs,
  warmup: warmup,
  time: time,
  memory_time: memory_time,
  reduction_time: 0,
  parallel: parallel,
  percentiles: [50, 95, 99],
  formatters: [Benchee.Formatters.Console]
)

Enum.each(exact_setups ++ guarded_setups, fn {_count, _channel, subscribers} ->
  Ferricstore.Bench.PubSub.stop_subscribers(subscribers)
end)

pattern = "bench:pubsub:pattern:*"
pattern_channel = "bench:pubsub:pattern:matched"
miss_channel = "bench:pubsub:pattern-miss"

pattern_pids =
  Ferricstore.Bench.PubSub.start_pattern_subscribers(pattern_subscribers, pattern)

^pattern_subscribers = PubSub.publish(pattern_channel, message)
0 = PubSub.publish(miss_channel, message)

Benchee.run(
  %{
    "pattern publish matches=#{pattern_subscribers}" => fn ->
      ^pattern_subscribers = PubSub.publish(pattern_channel, message)
    end,
    "pattern publish misses=#{pattern_subscribers}" => fn ->
      0 = PubSub.publish(miss_channel, message)
    end
  },
  warmup: warmup,
  time: time,
  memory_time: memory_time,
  reduction_time: 0,
  parallel: parallel,
  percentiles: [50, 95, 99],
  formatters: [Benchee.Formatters.Console]
)

Ferricstore.Bench.PubSub.stop_subscribers(pattern_pids)
cleanup.()
