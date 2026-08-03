# FerricStore Pub/Sub activity-log benchmark.
#
# Run:
#   MIX_ENV=bench mix run --no-start bench/pubsub_activity_log_bench.exs

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

warmup = env_number.("BENCH_WARMUP", "2")
time = env_number.("BENCH_TIME", "5")
memory_time = env_number.("BENCH_MEMORY_TIME", "0")
max_len = env_integer.("BENCH_PUBSUB_ACTIVITY_LOG_MAX_LEN", "512", 1)
sample_every = env_integer.("BENCH_PUBSUB_ACTIVITY_LOG_SAMPLE_EVERY", "1", 1)

Application.put_env(:ferricstore, :pubsub_activity_log_max_len, max_len)
Application.put_env(:ferricstore, :pubsub_activity_log_sample_every, sample_every)

{:ok, activity_log} = ActivityLog.start_link()

cleanup = fn ->
  if Process.alive?(activity_log), do: GenServer.stop(activity_log)
end

System.at_exit(fn _status -> cleanup.() end)

short_channel = "bench:pubsub:activity"
oversized_channel = :binary.copy("x", 4_096)
subscription_targets = Enum.map(1..8, &"bench:pubsub:subscription:#{&1}")
batch_messages = List.duplicate(:binary.copy("x", 256), 1_024)
batch_counts = List.duplicate(8, 1_024)

:ok = ActivityLog.record_publish("preflight", 17, 3)

[
  %{
    command: "PUBLISH",
    target_type: :channel,
    target: "preflight",
    targets: 1,
    subscribers: 3,
    message_bytes: 17
  }
] = ActivityLog.get(1)

ActivityLog.reset()

for index <- 1..max_len do
  :ok = ActivityLog.record_publish("seed:#{index}", index, rem(index, 8))
end

IO.puts("=== FerricStore Pub/Sub Activity Log Benchmark ===")
IO.puts("max_len=#{max_len} publish_sample_every=#{sample_every}")

Benchee.run(
  %{
    "get 1 newest entry" => fn ->
      [_entry] = ActivityLog.get(1)
    end,
    "get 128 newest entries" => fn ->
      entries = ActivityLog.get(128)
      true = entries != [] and length(entries) <= min(max_len, 128)
    end,
    "record publish short channel" => fn ->
      :ok = ActivityLog.record_publish(short_channel, 256, 8)
    end,
    "record publish oversized channel" => fn ->
      :ok = ActivityLog.record_publish(oversized_channel, 256, 0)
    end,
    "record publish loop x1024" => fn ->
      Enum.zip_with(batch_messages, batch_counts, fn message, subscribers ->
        ActivityLog.record_publish(short_channel, byte_size(message), subscribers)
      end)
    end,
    "record publish batch x1024" => fn ->
      :ok = ActivityLog.record_publish_batch(short_channel, batch_messages, batch_counts)
    end,
    "record subscription x8" => fn ->
      :ok = ActivityLog.record_subscription("SUBSCRIBE", :channel, subscription_targets)
    end
  },
  warmup: warmup,
  time: time,
  memory_time: memory_time,
  reduction_time: 0,
  percentiles: [50, 95, 99],
  formatters: [Benchee.Formatters.Console]
)

cleanup.()
