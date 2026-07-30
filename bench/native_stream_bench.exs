# Ferric native-protocol Stream producer benchmark.
#
# Run:
#   BENCH_STREAM_BATCH=64 MIX_ENV=bench mix run --no-start bench/native_stream_bench.exs
#
# This measures the complete TCP frame/decode/dispatch/replication/response path.
# It separates strictly sequential request/response traffic from an ordinary
# COMMAND_EXEC burst that the server can coalesce, plus explicit typed and compact
# PIPELINE requests.

defmodule FerricstoreBench.NativeStreamClient do
  @moduledoc false

  alias FerricstoreServer.Native.Codec

  @op_pipeline 0x000E
  @op_command_exec 0x0100
  @custom_payload Codec.flags().custom_payload

  def connect(port) do
    :gen_tcp.connect(
      {127, 0, 0, 1},
      port,
      [:binary, active: false, packet: :raw, nodelay: true],
      5_000
    )
  end

  def command_exec_frames(lane_id, key, count) do
    1..count
    |> Enum.map(&command_exec_frame(lane_id, &1, key))
    |> IO.iodata_to_binary()
  end

  def command_exec_frame(lane_id, request_id, key) do
    body =
      Codec.encode_value(%{
        "command" => "XADD",
        "args" => [key, "*", "field", "value"]
      })

    Codec.encode_frame(@op_command_exec, lane_id, request_id, body)
  end

  def typed_pipeline_frame(lane_id, request_id, key, count) do
    commands =
      Enum.map(1..count, fn inner_request_id ->
        %{
          "opcode" => @op_command_exec,
          "lane_id" => lane_id,
          "request_id" => inner_request_id,
          "body" => %{
            "command" => "XADD",
            "args" => [key, "*", "field", "value"]
          }
        }
      end)

    body =
      Codec.encode_value(%{
        "atomicity" => "same_shard",
        "return" => "compact",
        "commands" => commands
      })

    Codec.encode_frame(@op_pipeline, lane_id, request_id, body)
  end

  def compact_pipeline_frame(lane_id, request_id, key, count) do
    item = [
      compact_binary(key),
      <<1::unsigned-16>>,
      compact_binary("field"),
      compact_binary("value")
    ]

    body =
      [<<0x94, 0xA2, count::unsigned-32>>, List.duplicate(item, count)]
      |> IO.iodata_to_binary()

    Codec.encode_frame(@op_pipeline, lane_id, request_id, body, @custom_payload)
  end

  def compact_pipeline_frames(lane_id, key, frame_count, entries_per_frame) do
    lane_id
    |> compact_pipeline_frame_list(key, frame_count, entries_per_frame)
    |> IO.iodata_to_binary()
  end

  def compact_pipeline_frame_list(lane_id, key, frame_count, entries_per_frame) do
    Enum.map(
      1..frame_count,
      &compact_pipeline_frame(lane_id, &1, key, entries_per_frame)
    )
  end

  def round_trip(socket, frame, expected_responses) do
    :ok = :gen_tcp.send(socket, frame)
    responses = receive_responses(socket, expected_responses, "", [])

    unless Enum.all?(responses, fn {_request_id, status, _flags, _payload} -> status == 0 end) do
      raise "native Stream benchmark received a non-OK response: #{inspect(responses)}"
    end

    responses
  end

  def split_send_round_trip(socket, frames) do
    Enum.each(frames, fn frame -> :ok = :gen_tcp.send(socket, frame) end)
    responses = receive_responses(socket, length(frames), "", [])

    unless Enum.all?(responses, fn {_request_id, status, _flags, _payload} -> status == 0 end) do
      raise "native Stream benchmark received a non-OK response: #{inspect(responses)}"
    end

    responses
  end

  def repeated_round_trips(socket, frame, count) do
    for _iteration <- 1..count do
      round_trip(socket, frame, 1)
    end

    :ok
  end

  defp receive_responses(_socket, expected, _buffer, responses)
       when length(responses) == expected,
       do: Enum.reverse(responses)

  defp receive_responses(socket, expected, buffer, responses) do
    {decoded, rest} = decode_response_frames(buffer, [])
    responses = Enum.reverse(decoded, responses)

    if length(responses) == expected do
      if rest != "", do: raise("native Stream benchmark received unexpected trailing bytes")
      Enum.reverse(responses)
    else
      case :gen_tcp.recv(socket, 0, 30_000) do
        {:ok, bytes} -> receive_responses(socket, expected, rest <> bytes, responses)
        {:error, reason} -> raise "native Stream benchmark receive failed: #{inspect(reason)}"
      end
    end
  end

  defp decode_response_frames(
         <<"FSNP", 0x81, flags, _lane_id::unsigned-32, _opcode::unsigned-16,
           request_id::unsigned-64, body_len::unsigned-32, body_and_rest::binary>> = buffer,
         responses
       ) do
    if byte_size(body_and_rest) >= body_len do
      <<body::binary-size(body_len), rest::binary>> = body_and_rest
      <<status::unsigned-16, payload::binary>> = body

      decode_response_frames(rest, [{request_id, status, flags, payload} | responses])
    else
      {responses, buffer}
    end
  end

  defp decode_response_frames(buffer, responses), do: {responses, buffer}

  defp compact_binary(value) do
    value = IO.iodata_to_binary(value)
    [<<byte_size(value)::unsigned-32>>, value]
  end
end

alias FerricstoreBench.NativeStreamClient

Logger.configure(level: :warning)

env_integer = fn name, default ->
  case Integer.parse(System.get_env(name, default)) do
    {value, ""} when value > 0 -> value
    _invalid -> raise ArgumentError, "#{name} must be a positive integer"
  end
end

env_number = fn name, default ->
  case Float.parse(System.get_env(name, default)) do
    {value, ""} when value >= 0 -> value
    _invalid -> raise ArgumentError, "#{name} must be a non-negative number"
  end
end

batch_size = env_integer.("BENCH_STREAM_BATCH", "64")
compact_frame_count = env_integer.("BENCH_STREAM_COMPACT_FRAMES", "8")
native_concurrency = env_integer.("BENCH_NATIVE_CONCURRENCY", "16")
native_batches_per_connection = env_integer.("BENCH_NATIVE_BATCHES_PER_CONNECTION", "4")
warmup = env_number.("BENCH_WARMUP", "2")
time = env_number.("BENCH_TIME", "5")
memory_time = env_number.("BENCH_MEMORY_TIME", "0")
activity_log_max_len = trunc(env_number.("BENCH_STREAM_ACTIVITY_LOG_MAX_LEN", "512"))

if rem(batch_size, compact_frame_count) != 0 do
  raise ArgumentError,
        "BENCH_STREAM_BATCH must be divisible by BENCH_STREAM_COMPACT_FRAMES"
end

compact_entries_per_frame = div(batch_size, compact_frame_count)
run_id = "#{System.os_time(:microsecond)}_#{System.unique_integer([:positive, :monotonic])}"

data_dir =
  Path.join(
    System.tmp_dir!(),
    "ferricstore_native_stream_bench_#{run_id}"
  )

File.mkdir_p!(data_dir)
Application.put_env(:ferricstore, :data_dir, data_dir)
Application.put_env(:ferricstore, :native_port, 0)
Application.put_env(:ferricstore, :stream_activity_log_max_len, activity_log_max_len)

{:ok, _started} = Application.ensure_all_started(:ferricstore_server)

ctx = FerricStore.Instance.get(:default)
port = FerricstoreServer.Native.Listener.port()
suffix = System.unique_integer([:positive, :monotonic])
sequential_key = "bench:native:stream:sequential:{#{suffix}}"
burst_key = "bench:native:stream:burst:{#{suffix}}"
typed_key = "bench:native:stream:typed:{#{suffix}}"
compact_key = "bench:native:stream:compact:{#{suffix}}"
compact_burst_key = "bench:native:stream:compact-burst:{#{suffix}}"
compact_split_key = "bench:native:stream:compact-split:{#{suffix}}"
compact_concurrent_key = "bench:native:stream:compact-concurrent:{#{suffix}}"

shards =
  [
    sequential_key,
    burst_key,
    typed_key,
    compact_key,
    compact_burst_key,
    compact_split_key,
    compact_concurrent_key
  ]
  |> Enum.map(&Ferricstore.Store.Router.shard_for(ctx, &1))
  |> Enum.uniq()

true = length(shards) == 1
[shard_index] = shards
lane_id = shard_index + 1

{:ok, sequential_socket} = NativeStreamClient.connect(port)
{:ok, burst_socket} = NativeStreamClient.connect(port)
{:ok, typed_socket} = NativeStreamClient.connect(port)
{:ok, compact_socket} = NativeStreamClient.connect(port)
{:ok, compact_burst_socket} = NativeStreamClient.connect(port)
{:ok, compact_split_socket} = NativeStreamClient.connect(port)

compact_concurrent_sockets =
  Enum.map(1..native_concurrency, fn _connection ->
    {:ok, socket} = NativeStreamClient.connect(port)
    socket
  end)

sequential_frame = NativeStreamClient.command_exec_frame(lane_id, 1, sequential_key)
burst_frame = NativeStreamClient.command_exec_frames(lane_id, burst_key, batch_size)
typed_frame = NativeStreamClient.typed_pipeline_frame(lane_id, 1, typed_key, batch_size)
compact_frame = NativeStreamClient.compact_pipeline_frame(lane_id, 1, compact_key, batch_size)

compact_concurrent_frame =
  NativeStreamClient.compact_pipeline_frame(lane_id, 1, compact_concurrent_key, batch_size)

compact_burst_frames =
  NativeStreamClient.compact_pipeline_frames(
    lane_id,
    compact_burst_key,
    compact_frame_count,
    compact_entries_per_frame
  )

compact_split_frames =
  NativeStreamClient.compact_pipeline_frame_list(
    lane_id,
    compact_split_key,
    compact_frame_count,
    compact_entries_per_frame
  )

sequential_counter = :counters.new(1, [:atomics])
burst_counter = :counters.new(1, [:atomics])
typed_counter = :counters.new(1, [:atomics])
compact_counter = :counters.new(1, [:atomics])
compact_burst_counter = :counters.new(1, [:atomics])
compact_split_counter = :counters.new(1, [:atomics])

raft_position = fn ->
  {:ok, {:raft_log_pos, index, _term}} =
    Ferricstore.Raft.WARaftBackend.storage_position(shard_index)

  index
end

preflight = fn execute, key, expected_raft_entries ->
  before_position = raft_position.()
  execute.()
  {:ok, ^batch_size} = FerricStore.xlen(key)
  actual_raft_entries = raft_position.() - before_position

  if actual_raft_entries < expected_raft_entries do
    raise "missing Raft durability progress for #{key}: expected at least #{expected_raft_entries}, got #{actual_raft_entries}"
  end

  IO.puts(
    "preflight_key=#{key} expected_command_entries=#{expected_raft_entries} observed_shard_delta=#{actual_raft_entries}"
  )
end

preflight.(
  fn ->
    NativeStreamClient.repeated_round_trips(sequential_socket, sequential_frame, batch_size)
  end,
  sequential_key,
  batch_size
)

preflight.(
  fn -> NativeStreamClient.round_trip(burst_socket, burst_frame, batch_size) end,
  burst_key,
  1
)

preflight.(
  fn -> NativeStreamClient.split_send_round_trip(compact_split_socket, compact_split_frames) end,
  compact_split_key,
  1
)

preflight.(fn -> NativeStreamClient.round_trip(typed_socket, typed_frame, 1) end, typed_key, 1)

preflight.(
  fn -> NativeStreamClient.round_trip(compact_socket, compact_frame, 1) end,
  compact_key,
  1
)

preflight.(
  fn ->
    NativeStreamClient.round_trip(
      compact_burst_socket,
      compact_burst_frames,
      compact_frame_count
    )
  end,
  compact_burst_key,
  1
)

IO.puts("=== Ferric Native Stream Producer Benchmark ===")
IO.puts("batch_size=#{batch_size} shard=#{shard_index} lane=#{lane_id} native_port=#{port}")

IO.puts(
  "compact_burst_frames=#{compact_frame_count} entries_per_frame=#{compact_entries_per_frame}"
)

IO.puts("stream_activity_log_max_len=#{activity_log_max_len}")
IO.puts("data_dir=#{data_dir}")
IO.puts("preflight=ok durability=replicated socket_path=tcp")

IO.puts("throughput_units=Benchee ips is outer batches/s; XADD entries/s = ips * #{batch_size}")

concurrent_started_at = System.monotonic_time()

concurrent_results =
  compact_concurrent_sockets
  |> Task.async_stream(
    fn socket ->
      NativeStreamClient.repeated_round_trips(
        socket,
        compact_concurrent_frame,
        native_batches_per_connection
      )
    end,
    max_concurrency: native_concurrency,
    ordered: false,
    timeout: 60_000
  )
  |> Enum.to_list()

true = Enum.all?(concurrent_results, &match?({:ok, :ok}, &1))
concurrent_elapsed = System.monotonic_time() - concurrent_started_at

concurrent_seconds =
  System.convert_time_unit(concurrent_elapsed, :native, :microsecond) / 1_000_000

concurrent_batches = native_concurrency * native_batches_per_connection
concurrent_entries = concurrent_batches * batch_size
{:ok, ^concurrent_entries} = FerricStore.xlen(compact_concurrent_key)

IO.puts(
  "native_compact_concurrent connections=#{native_concurrency} " <>
    "batches_per_connection=#{native_batches_per_connection} batches=#{concurrent_batches} " <>
    "entries=#{concurrent_entries} seconds=#{Float.round(concurrent_seconds, 4)} " <>
    "batches_per_second=#{Float.round(concurrent_batches / concurrent_seconds, 1)} " <>
    "entries_per_second=#{Float.round(concurrent_entries / concurrent_seconds, 1)} correctness=ok"
)

benchmarks =
  %{
    "native COMMAND_EXEC round trips XADD x#{batch_size}" => fn ->
      NativeStreamClient.repeated_round_trips(sequential_socket, sequential_frame, batch_size)
      :counters.add(sequential_counter, 1, batch_size)
    end,
    "native COMMAND_EXEC burst XADD x#{batch_size} (auto-batched)" => fn ->
      NativeStreamClient.round_trip(burst_socket, burst_frame, batch_size)
      :counters.add(burst_counter, 1, batch_size)
    end,
    "native typed PIPELINE XADD x#{batch_size}" => fn ->
      NativeStreamClient.round_trip(typed_socket, typed_frame, 1)
      :counters.add(typed_counter, 1, batch_size)
    end,
    "native compact PIPELINE XADD x#{batch_size}" => fn ->
      NativeStreamClient.round_trip(compact_socket, compact_frame, 1)
      :counters.add(compact_counter, 1, batch_size)
    end,
    "native compact PIPELINE burst #{compact_frame_count}x#{compact_entries_per_frame} XADD (lane-coalesced)" =>
      fn ->
        NativeStreamClient.round_trip(
          compact_burst_socket,
          compact_burst_frames,
          compact_frame_count
        )

        :counters.add(compact_burst_counter, 1, batch_size)
      end,
    "native compact PIPELINE #{compact_frame_count}x#{compact_entries_per_frame} XADD (separate TCP sends)" =>
      fn ->
        NativeStreamClient.split_send_round_trip(compact_split_socket, compact_split_frames)
        :counters.add(compact_split_counter, 1, batch_size)
      end
  }

benchmarks =
  case System.get_env("BENCH_FILTER") do
    filter when is_binary(filter) and filter != "" ->
      selected = Map.filter(benchmarks, fn {name, _fun} -> String.contains?(name, filter) end)

      if map_size(selected) == 0 do
        raise ArgumentError, "BENCH_FILTER matched no native Stream benchmark: #{inspect(filter)}"
      end

      selected

    _none ->
      benchmarks
  end

Benchee.run(
  benchmarks,
  time: time,
  warmup: warmup,
  memory_time: memory_time,
  formatters: [Benchee.Formatters.Console]
)

for {key, counter} <- [
      {sequential_key, sequential_counter},
      {burst_key, burst_counter},
      {typed_key, typed_counter},
      {compact_key, compact_counter},
      {compact_burst_key, compact_burst_counter},
      {compact_split_key, compact_split_counter}
    ] do
  expected = batch_size + :counters.get(counter, 1)
  {:ok, actual} = FerricStore.xlen(key)
  true = actual == expected
  IO.puts("correctness key=#{key} expected_entries=#{expected} actual_entries=#{actual}")
end

Enum.each(
  [
    sequential_socket,
    burst_socket,
    typed_socket,
    compact_socket,
    compact_burst_socket,
    compact_split_socket
  ],
  &:gen_tcp.close/1
)

Enum.each(compact_concurrent_sockets, &:gen_tcp.close/1)

_ = Application.stop(:ferricstore_server)
_ = Application.stop(:ferricstore)
File.rm_rf!(data_dir)
