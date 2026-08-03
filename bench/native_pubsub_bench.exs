# Ferric native-protocol PubSub round-trip benchmark.
#
# Run:
#   MIX_ENV=bench mix bench.native_pubsub
#
# Each timed iteration sends a real PUBLISH frame over loopback TCP, decodes
# the publisher acknowledgement, receives and decodes one pushed event on every
# subscriber connection, and validates the channel and payload.

Code.require_file(Path.expand("support/native_pubsub_client.exs", __DIR__))

alias FerricstoreBench.NativePubSubClient

Logger.configure(level: :warning)

env_integer = fn name, default, minimum ->
  case Integer.parse(System.get_env(name, default)) do
    {value, ""} when value >= minimum -> value
    _invalid -> raise ArgumentError, "#{name} must be an integer >= #{minimum}"
  end
end

env_number = fn name, default ->
  case Float.parse(System.get_env(name, default)) do
    {value, ""} when value >= 0 -> value
    _invalid -> raise ArgumentError, "#{name} must be a non-negative number"
  end
end

fanouts =
  System.get_env("BENCH_PUBSUB_TCP_FANOUTS", "1,8,64")
  |> String.split(",", trim: true)
  |> Enum.map(fn raw ->
    case Integer.parse(String.trim(raw)) do
      {value, ""} when value >= 1 -> value
      _invalid -> raise ArgumentError, "BENCH_PUBSUB_TCP_FANOUTS must contain integers >= 1"
    end
  end)
  |> Enum.uniq()

message_bytes = env_integer.("BENCH_PUBSUB_MESSAGE_BYTES", "256", 0)
activity_log_max_len = env_integer.("BENCH_PUBSUB_ACTIVITY_LOG_MAX_LEN", "512", 0)
warmup = env_number.("BENCH_WARMUP", "1")
time = env_number.("BENCH_TIME", "3")
memory_time = env_number.("BENCH_MEMORY_TIME", "0")
batch_events = System.get_env("BENCH_PUBSUB_BATCH_EVENTS", "0") == "1"

run_id = "#{System.os_time(:microsecond)}_#{System.unique_integer([:positive, :monotonic])}"
data_dir = Path.join(System.tmp_dir!(), "ferricstore_native_pubsub_bench_#{run_id}")
File.mkdir_p!(data_dir)

Application.put_env(:ferricstore, :data_dir, data_dir)
Application.put_env(:ferricstore, :native_port, 0)
Application.put_env(:ferricstore, :pubsub_activity_log_max_len, activity_log_max_len)

{:ok, _started} = Application.ensure_all_started(:ferricstore_server)

port = FerricstoreServer.Native.Listener.port()
ctx = FerricStore.Instance.get(:default)
message = :binary.copy("x", message_bytes)
suffix = System.unique_integer([:positive, :monotonic])
{:ok, publisher_socket} = NativePubSubClient.connect(port)

setups =
  Enum.map(fanouts, fn fanout ->
    channel = "bench:native:pubsub:#{suffix}:#{fanout}"

    subscribers =
      Enum.map(1..fanout, fn index ->
        {:ok, socket} = NativePubSubClient.connect(port)

        if batch_events,
          do: :ok = NativePubSubClient.negotiate_pubsub_batches(socket, 100_000 + index)

        :ok = NativePubSubClient.subscribe(socket, channel, index)
        socket
      end)

    request_id = 10_000 + fanout
    lane_id = Ferricstore.Store.Router.shard_for(ctx, channel) + 1

    frame =
      NativePubSubClient.command_exec_frame(
        request_id,
        "PUBLISH",
        [channel, message],
        lane_id
      )

    :ok = NativePubSubClient.publish(publisher_socket, frame, request_id, fanout)
    Enum.each(subscribers, &NativePubSubClient.receive_pubsub(&1, channel, message))

    %{
      channel: channel,
      subscribers: subscribers,
      fanout: fanout,
      lane_id: lane_id,
      request_id: request_id,
      frame: frame,
      preencoded_counter: :counters.new(1, [:atomics]),
      encoded_counter: :counters.new(1, [:atomics])
    }
  end)

run_iteration = fn setup, frame ->
  :ok =
    NativePubSubClient.publish(
      publisher_socket,
      frame,
      setup.request_id,
      setup.fanout
    )

  Enum.each(
    setup.subscribers,
    &NativePubSubClient.receive_pubsub(&1, setup.channel, message)
  )
end

benchmarks =
  Enum.reduce(setups, %{}, fn setup, jobs ->
    jobs
    |> Map.put("native TCP preencoded publish+receive fanout=#{setup.fanout}", fn ->
      run_iteration.(setup, setup.frame)
      :counters.add(setup.preencoded_counter, 1, 1)
    end)
    |> Map.put("native TCP encoded publish+receive fanout=#{setup.fanout}", fn ->
      frame =
        NativePubSubClient.command_exec_frame(
          setup.request_id,
          "PUBLISH",
          [setup.channel, message],
          setup.lane_id
        )

      run_iteration.(setup, frame)
      :counters.add(setup.encoded_counter, 1, 1)
    end)
  end)

IO.puts("=== Ferric Native PubSub TCP Benchmark ===")

IO.puts(
  "native_port=#{port} message_bytes=#{message_bytes} " <>
    "activity_log_max_len=#{activity_log_max_len} fanouts=#{Enum.join(fanouts, ",")} " <>
    "batch_events=#{batch_events}"
)

IO.puts("preflight=ok transport=tcp acknowledgement=decoded pushed_events=decoded_and_validated")
IO.puts("throughput_units=Benchee ips is publishes/s; delivered messages/s = ips * fanout")

Benchee.run(
  benchmarks,
  time: time,
  warmup: warmup,
  memory_time: memory_time,
  reduction_time: 0,
  parallel: 1,
  percentiles: [50, 95, 99],
  formatters: [Benchee.Formatters.Console]
)

Enum.each(setups, fn setup ->
  for {mode, counter} <- [
        {:preencoded, setup.preencoded_counter},
        {:encoded, setup.encoded_counter}
      ] do
    publishes = :counters.get(counter, 1)

    IO.puts(
      "correctness mode=#{mode} fanout=#{setup.fanout} publishes=#{publishes} " <>
        "validated_deliveries=#{publishes * setup.fanout}"
    )
  end

  Enum.with_index(setup.subscribers, 20_000)
  |> Enum.each(fn {socket, request_id} ->
    :ok = NativePubSubClient.unsubscribe(socket, setup.channel, request_id)
    :gen_tcp.close(socket)
  end)
end)

:gen_tcp.close(publisher_socket)
_ = Application.stop(:ferricstore_server)
_ = Application.stop(:ferricstore)
File.rm_rf!(data_dir)
