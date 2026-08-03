# FerricStore same-channel Pub/Sub pattern-batch benchmark.
#
# Run:
#   BENCH_PUBSUB_PATTERN_ENTRIES=64 BENCH_PUBSUB_BATCH_SIZE=1024 \
#     MIX_ENV=bench mix run --no-start bench/pubsub_pattern_batch_bench.exs

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

entries = env_integer.("BENCH_PUBSUB_PATTERN_ENTRIES", "64", 17)
batch_size = env_integer.("BENCH_PUBSUB_BATCH_SIZE", "1024", 1)
payload_size = env_integer.("BENCH_PUBSUB_PAYLOAD_BYTES", "256", 1)
warmup = env_number.("BENCH_WARMUP", "1")
time = env_number.("BENCH_TIME", "3")

defmodule Ferricstore.Bench.PubSubPatternBatch do
  @moduledoc false

  def drain do
    receive do
      {:pubsub_pmessage, _pattern, _channel, _message} -> drain()
      :stop -> :ok
    end
  end
end

Application.put_env(:ferricstore, :pubsub_activity_log_max_len, 0)

{:ok, activity_log} = ActivityLog.start_link()
{:ok, pubsub} = PubSub.start_link()
sink = spawn(&Ferricstore.Bench.PubSubPatternBatch.drain/0)

cleanup = fn ->
  if Process.alive?(pubsub), do: GenServer.stop(pubsub)
  if Process.alive?(activity_log), do: GenServer.stop(activity_log)
  if Process.alive?(sink), do: send(sink, :stop)
end

System.at_exit(fn _status -> cleanup.() end)

suffix = System.unique_integer([:positive, :monotonic])
channel = "xbench:indexed-batch:target:#{suffix}y"
pattern = "?*indexed-batch:target:#{suffix}*?"

decoys =
  for index <- 1..(entries - 1),
      do: "bench:indexed-batch:decoy:#{suffix}:#{index}"

:ok = PubSub.psubscribe_many(decoys ++ [pattern], sink)
payload = :binary.copy("x", payload_size)
publishes = List.duplicate({channel, payload}, batch_size)
expected = List.duplicate(1, batch_size)
^expected = PubSub.publish_many(publishes)

IO.puts("=== FerricStore Pub/Sub Pattern Batch Benchmark ===")
IO.puts("patterns=#{entries} batch_size=#{batch_size} payload_bytes=#{payload_size}")

Benchee.run(
  %{
    "legacy publish loop" => fn ->
      ^expected =
        Enum.map(publishes, fn {item_channel, message} ->
          PubSub.publish(item_channel, message)
        end)
    end,
    "indexed pattern publish_many" => fn ->
      ^expected = PubSub.publish_many(publishes)
    end
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
