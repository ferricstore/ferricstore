# Saturated concurrent-publisher and slow-consumer PubSub benchmark.
#
# Run:
#   MIX_ENV=bench mix bench.native_pubsub_load
#
# The concurrent phase uses one native TCP connection per publisher and one
# passive reader per subscriber. The slow-consumer phase deliberately stops
# reading one subscriber and verifies that its bounded outbound budget evicts
# only that connection while a fast subscriber keeps receiving every message.

Code.require_file(Path.expand("support/native_pubsub_client.exs", __DIR__))

alias FerricstoreBench.NativePubSubClient

Logger.configure(level: :warning)

env_integer = fn name, default, minimum ->
  case Integer.parse(System.get_env(name, default)) do
    {value, ""} when value >= minimum -> value
    _invalid -> raise ArgumentError, "#{name} must be an integer >= #{minimum}"
  end
end

concurrencies =
  System.get_env("BENCH_PUBSUB_CONCURRENCIES", "1,4,16")
  |> String.split(",", trim: true)
  |> Enum.map(fn raw ->
    case Integer.parse(String.trim(raw)) do
      {value, ""} when value >= 1 -> value
      _invalid -> raise ArgumentError, "BENCH_PUBSUB_CONCURRENCIES requires integers >= 1"
    end
  end)
  |> Enum.uniq()

fanout = env_integer.("BENCH_PUBSUB_LOAD_FANOUT", "8", 1)
publishes_per_publisher = env_integer.("BENCH_PUBSUB_PUBLISHES_PER_PUBLISHER", "500", 1)
message_bytes = env_integer.("BENCH_PUBSUB_LOAD_MESSAGE_BYTES", "256", 0)
slow_samples = env_integer.("BENCH_PUBSUB_SLOW_SAMPLES", "5", 1)
slow_message_bytes = env_integer.("BENCH_PUBSUB_SLOW_MESSAGE_BYTES", "1048576", 1)
slow_max_publishes = env_integer.("BENCH_PUBSUB_SLOW_MAX_PUBLISHES", "16", 2)
activity_log_max_len = env_integer.("BENCH_PUBSUB_ACTIVITY_LOG_MAX_LEN", "512", 0)
activity_log_sample_every = env_integer.("BENCH_PUBSUB_ACTIVITY_LOG_SAMPLE_EVERY", "1", 1)
load_outbound_bytes = env_integer.("BENCH_PUBSUB_LOAD_OUTBOUND_BYTES", "134217728", 1)
coalesce_max = env_integer.("BENCH_PUBSUB_COALESCE_MAX", "64", 1)
publish_pipeline = env_integer.("BENCH_PUBSUB_PUBLISH_PIPELINE", "1", 1)
skip_slow_phase = System.get_env("BENCH_PUBSUB_SKIP_SLOW_PHASE", "0") == "1"
batch_events = System.get_env("BENCH_PUBSUB_BATCH_EVENTS", "0") == "1"

pipeline_encoding =
  case System.get_env("BENCH_PUBSUB_PIPELINE_ENCODING", "generic") do
    encoding when encoding in ["generic", "compact"] ->
      encoding

    invalid ->
      raise ArgumentError,
            "BENCH_PUBSUB_PIPELINE_ENCODING must be generic or compact, got: #{inspect(invalid)}"
  end

slow_outbound_bytes =
  env_integer.(
    "BENCH_PUBSUB_OUTBOUND_BYTES",
    Integer.to_string(slow_message_bytes * 2 + 4_096),
    slow_message_bytes
  )

run_id = "#{System.os_time(:microsecond)}_#{System.unique_integer([:positive, :monotonic])}"
data_dir = Path.join(System.tmp_dir!(), "ferricstore_native_pubsub_load_#{run_id}")
File.mkdir_p!(data_dir)

Application.put_env(:ferricstore, :data_dir, data_dir)
Application.put_env(:ferricstore, :native_port, 0)
Application.put_env(:ferricstore, :pubsub_activity_log_max_len, activity_log_max_len)
Application.put_env(:ferricstore, :pubsub_activity_log_sample_every, activity_log_sample_every)
Application.put_env(:ferricstore, :native_max_outbound_bytes_per_connection, load_outbound_bytes)
Application.put_env(:ferricstore, :native_response_coalesce_max, coalesce_max)

Application.put_env(
  :ferricstore,
  :native_max_global_outbound_bytes,
  max(load_outbound_bytes * 4, 512 * 1024 * 1024)
)

Application.put_env(:ferricstore, :native_send_timeout_ms, 2_000)

{:ok, _started} = Application.ensure_all_started(:ferricstore_server)

port = FerricstoreServer.Native.Listener.port()
ctx = FerricStore.Instance.get(:default)
message = :binary.copy("x", message_bytes)
suffix = System.unique_integer([:positive, :monotonic])

start_reader = fn socket, channel, payload, count ->
  task =
    Task.async(fn ->
      receive do
        :start -> NativePubSubClient.receive_pubsub_many(socket, channel, payload, count, 120_000)
      end
    end)

  :ok = :gen_tcp.controlling_process(socket, task.pid)
  send(task.pid, :start)
  task
end

run_concurrent = fn concurrency ->
  channel = "bench:native:pubsub:load:#{suffix}:#{concurrency}"
  total_publishes = concurrency * publishes_per_publisher
  lane_id = Ferricstore.Store.Router.shard_for(ctx, channel) + 1

  subscribers =
    Enum.map(1..fanout, fn index ->
      {:ok, socket} = NativePubSubClient.connect(port)

      if batch_events,
        do: :ok = NativePubSubClient.negotiate_pubsub_batches(socket, 100_000 + index)

      :ok = NativePubSubClient.subscribe(socket, channel, index)
      socket
    end)

  reader_tasks =
    Enum.map(subscribers, &start_reader.(&1, channel, message, total_publishes))

  gate = make_ref()

  publisher_tasks =
    Enum.map(1..concurrency, fn index ->
      {:ok, socket} = NativePubSubClient.connect(port)
      request_id = 10_000 + index

      frame =
        NativePubSubClient.command_exec_frame(
          request_id,
          "PUBLISH",
          [channel, message],
          lane_id
        )

      pipeline_frames =
        if publish_pipeline > 1 do
          full_batches = div(publishes_per_publisher, publish_pipeline)
          remainder = rem(publishes_per_publisher, publish_pipeline)

          batch_sizes =
            List.duplicate(publish_pipeline, full_batches) ++
              if(remainder == 0, do: [], else: [remainder])

          Enum.map(batch_sizes, fn batch_size ->
            publishes = List.duplicate({channel, message}, batch_size)

            pipeline_frame =
              case pipeline_encoding do
                "generic" ->
                  NativePubSubClient.publish_pipeline_frame(request_id, publishes, lane_id)

                "compact" ->
                  NativePubSubClient.compact_publish_pipeline_frame(
                    request_id,
                    publishes,
                    lane_id
                  )
              end

            {pipeline_frame, batch_size}
          end)
        else
          []
        end

      task =
        Task.async(fn ->
          receive do
            {^gate, :go} ->
              if publish_pipeline == 1 do
                Enum.each(1..publishes_per_publisher, fn _iteration ->
                  :ok = NativePubSubClient.publish(socket, frame, request_id, fanout)
                end)
              else
                Enum.each(pipeline_frames, fn {pipeline_frame, batch_size} ->
                  :ok =
                    NativePubSubClient.publish_pipeline(
                      socket,
                      pipeline_frame,
                      request_id,
                      batch_size,
                      fanout
                    )
                end)
              end
          end
        end)

      :ok = :gen_tcp.controlling_process(socket, task.pid)
      task
    end)

  started = System.monotonic_time(:microsecond)
  Enum.each(publisher_tasks, &send(&1.pid, {gate, :go}))

  :ok =
    publisher_tasks
    |> Task.await_many(120_000)
    |> then(fn results ->
      if Enum.all?(results, &(&1 == :ok)), do: :ok, else: raise("publisher task failed")
    end)

  :ok =
    reader_tasks
    |> Task.await_many(120_000)
    |> then(fn results ->
      if Enum.all?(results, &(&1 == :ok)), do: :ok, else: raise("subscriber task failed")
    end)

  elapsed_us = System.monotonic_time(:microsecond) - started

  %{
    concurrency: concurrency,
    publishes: total_publishes,
    deliveries: total_publishes * fanout,
    elapsed_us: elapsed_us,
    publishes_per_second: total_publishes * 1_000_000 / elapsed_us,
    deliveries_per_second: total_publishes * fanout * 1_000_000 / elapsed_us
  }
end

IO.puts("=== Ferric Native PubSub Load Benchmark ===")

IO.puts(
  "native_port=#{port} message_bytes=#{message_bytes} fanout=#{fanout} " <>
    "publishes_per_publisher=#{publishes_per_publisher} " <>
    "concurrencies=#{Enum.join(concurrencies, ",")} " <>
    "outbound_limit_bytes=#{load_outbound_bytes} " <>
    "coalesce_max=#{coalesce_max} " <>
    "publish_pipeline=#{publish_pipeline} " <>
    "pipeline_encoding=#{pipeline_encoding} " <>
    "batch_events=#{batch_events} " <>
    "activity_log_max_len=#{activity_log_max_len} " <>
    "activity_log_sample_every=#{activity_log_sample_every}"
)

concurrent_results = Enum.map(concurrencies, run_concurrent)

Enum.each(concurrent_results, fn result ->
  IO.puts(
    "concurrent publishers=#{result.concurrency} publishes=#{result.publishes} " <>
      "deliveries=#{result.deliveries} elapsed_ms=#{Float.round(result.elapsed_us / 1_000, 2)} " <>
      "publishes_per_second=#{Float.round(result.publishes_per_second, 2)} " <>
      "deliveries_per_second=#{Float.round(result.deliveries_per_second, 2)} correctness=ok"
  )
end)

if skip_slow_phase do
  System.halt(0)
end

_ = Application.stop(:ferricstore_server)
_ = Application.stop(:ferricstore)

Application.put_env(
  :ferricstore,
  :native_max_outbound_bytes_per_connection,
  slow_outbound_bytes
)

Application.put_env(
  :ferricstore,
  :native_max_global_outbound_bytes,
  max(slow_outbound_bytes * 32, 64 * 1024 * 1024)
)

{:ok, _restarted} = Application.ensure_all_started(:ferricstore_server)

slow_port = FerricstoreServer.Native.Listener.port()
slow_ctx = FerricStore.Instance.get(:default)

reader_loop = fn reader_loop, socket, channel, payload ->
  receive do
    {:read_one, caller} ->
      :ok = NativePubSubClient.receive_pubsub(socket, channel, payload)
      send(caller, {:fast_pubsub_received, self()})
      reader_loop.(reader_loop, socket, channel, payload)

    :stop ->
      :ok
  end
end

{:ok, slow_publisher} = NativePubSubClient.connect(slow_port)
slow_message = :binary.copy("s", slow_message_bytes)

slow_results =
  Enum.map(1..slow_samples, fn sample ->
    channel = "bench:native:pubsub:slow:#{suffix}:#{sample}"
    lane_id = Ferricstore.Store.Router.shard_for(slow_ctx, channel) + 1
    request_id = 30_000 + sample

    frame =
      NativePubSubClient.command_exec_frame(
        request_id,
        "PUBLISH",
        [channel, slow_message],
        lane_id
      )

    {:ok, fast_socket} = NativePubSubClient.connect(slow_port)

    if batch_events,
      do: :ok = NativePubSubClient.negotiate_pubsub_batches(fast_socket, 200_000 + sample)

    :ok = NativePubSubClient.subscribe(fast_socket, channel, 40_000 + sample)

    fast_reader =
      Task.async(fn ->
        receive do
          :start -> reader_loop.(reader_loop, fast_socket, channel, slow_message)
        end
      end)

    :ok = :gen_tcp.controlling_process(fast_socket, fast_reader.pid)
    send(fast_reader.pid, :start)

    {:ok, slow_socket} = NativePubSubClient.connect(slow_port, recbuf: 1_024)

    if batch_events,
      do: :ok = NativePubSubClient.negotiate_pubsub_batches(slow_socket, 300_000 + sample)

    :ok = NativePubSubClient.subscribe(slow_socket, channel, 50_000 + sample)

    started = System.monotonic_time(:microsecond)

    eviction =
      Enum.reduce_while(1..slow_max_publishes, nil, fn attempt, _result ->
        send(fast_reader.pid, {:read_one, self()})
        subscriber_count = NativePubSubClient.publish_count(slow_publisher, frame, request_id)

        unless subscriber_count in [1, 2] do
          raise "slow-consumer benchmark expected one or two subscribers, got #{subscriber_count}"
        end

        receive do
          {:fast_pubsub_received, pid} when pid == fast_reader.pid -> :ok
        after
          30_000 -> raise "fast PubSub subscriber did not receive slow-consumer sample"
        end

        if subscriber_count == 1 do
          {:halt, %{attempt: attempt, elapsed_us: System.monotonic_time(:microsecond) - started}}
        else
          {:cont, nil}
        end
      end)

    if eviction == nil do
      raise "slow subscriber was not evicted within #{slow_max_publishes} publishes"
    end

    send(fast_reader.pid, {:read_one, self()})
    :ok = NativePubSubClient.publish(slow_publisher, frame, request_id, 1)

    receive do
      {:fast_pubsub_received, pid} when pid == fast_reader.pid -> :ok
    after
      30_000 -> raise "fast PubSub subscriber stopped after slow-consumer eviction"
    end

    send(fast_reader.pid, :stop)
    :ok = Task.await(fast_reader, 5_000)
    :gen_tcp.close(slow_socket)
    eviction
  end)

median = fn values ->
  sorted = Enum.sort(values)
  Enum.at(sorted, div(length(sorted), 2))
end

median_attempts = slow_results |> Enum.map(& &1.attempt) |> median.()
median_eviction_us = slow_results |> Enum.map(& &1.elapsed_us) |> median.()

IO.puts(
  "slow_consumer samples=#{slow_samples} message_bytes=#{slow_message_bytes} " <>
    "outbound_limit_bytes=#{slow_outbound_bytes} median_publishes_to_evict=#{median_attempts} " <>
    "median_eviction_ms=#{Float.round(median_eviction_us / 1_000, 2)} " <>
    "fast_subscriber_followup=ok isolation=ok"
)

:gen_tcp.close(slow_publisher)
_ = Application.stop(:ferricstore_server)
_ = Application.stop(:ferricstore)
File.rm_rf!(data_dir)
