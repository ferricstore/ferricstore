defmodule FerricstoreBench.NativePubSubClient do
  @moduledoc false

  alias FerricstoreServer.Native.Codec

  @op_hello 0x0001
  @op_pipeline 0x000E
  @op_command_exec 0x0100
  @op_event 0x0010
  @custom_payload Codec.flags().custom_payload
  @compact_integer_list Codec.compact_tags().integer_list
  @compressed_flag 0x08
  @header_bytes 24

  def connect(port, opts \\ []) do
    socket_opts =
      [:binary | Keyword.merge([active: false, packet: :raw, nodelay: true], opts)]

    :gen_tcp.connect(
      {127, 0, 0, 1},
      port,
      socket_opts,
      5_000
    )
  end

  def command_exec_frame(request_id, command, args, lane_id \\ 0) do
    body = Codec.encode_value(%{"command" => command, "args" => args})
    Codec.encode_frame(@op_command_exec, lane_id, request_id, body)
  end

  def pubsub_batch_hello_frame(request_id) do
    body =
      Codec.encode_value(%{
        "compact_flow_responses" => false,
        "compact_response_codecs" => ["pubsub_batch_v1"]
      })

    Codec.encode_frame(@op_hello, 0, request_id, body)
  end

  def negotiate_pubsub_batches(socket, request_id) do
    frame = pubsub_batch_hello_frame(request_id)
    {_hello, ""} = command_round_trip(socket, frame, request_id)
    :ok
  end

  def publish_pipeline_frame(request_id, publishes, lane_id \\ 0) when is_list(publishes) do
    commands =
      publishes
      |> Enum.with_index(1)
      |> Enum.map(fn {{channel, message}, index} ->
        %{
          "opcode" => @op_command_exec,
          "lane_id" => lane_id,
          "request_id" => index,
          "body" => %{"command" => "PUBLISH", "args" => [channel, message]}
        }
      end)

    body =
      Codec.encode_value(%{
        "atomicity" => "none",
        "commands" => commands,
        "return" => "pairs"
      })

    Codec.encode_frame(@op_pipeline, lane_id, request_id, body)
  end

  def compact_publish_pipeline_frame(request_id, publishes, lane_id \\ 0)
      when is_list(publishes) do
    items =
      Enum.map(publishes, fn {channel, message} ->
        [compact_binary(channel), compact_binary(message)]
      end)

    body =
      [<<0x94, 0xA3, length(publishes)::unsigned-32>>, items]
      |> IO.iodata_to_binary()

    Codec.encode_frame(@op_pipeline, lane_id, request_id, body, @custom_payload)
  end

  def subscribe(socket, channel, request_id) do
    frame = command_exec_frame(request_id, "SUBSCRIBE", [channel])
    {_acknowledgement, ""} = command_round_trip(socket, frame, request_id)
    :ok
  end

  def unsubscribe(socket, channel, request_id) do
    frame = command_exec_frame(request_id, "UNSUBSCRIBE", [channel])
    {_acknowledgement, ""} = command_round_trip(socket, frame, request_id)
    :ok
  end

  def publish(socket, frame, request_id, expected_subscribers) do
    actual_subscribers = publish_count(socket, frame, request_id)

    if actual_subscribers != expected_subscribers do
      raise "native PubSub publish expected #{expected_subscribers} subscribers, got #{inspect(actual_subscribers)}"
    end

    :ok
  end

  def publish_count(socket, frame, request_id) do
    :ok = :gen_tcp.send(socket, frame)
    {response, rest} = receive_frame(socket, "", 30_000)

    if rest != "" do
      raise "native PubSub publish response contained unexpected trailing bytes"
    end

    publish_count_from_frames([response], request_id)
  end

  def publish_count_from_frames(
        [%{request_id: request_id, status: 0, value: count}],
        request_id
      )
      when is_integer(count) and count >= 0,
      do: count

  def publish_count_from_frames(frames, request_id) do
    raise "native PubSub benchmark received an unexpected publish response " <>
            "for request #{request_id}: #{inspect(frames)}"
  end

  def publish_pipeline(socket, frame, request_id, publish_count, expected_subscribers) do
    :ok = :gen_tcp.send(socket, frame)
    {response, rest} = receive_frame(socket, "", 30_000)

    if rest != "" do
      raise "native PubSub pipeline response contained unexpected trailing bytes"
    end

    validate_publish_pipeline_frames(
      [response],
      request_id,
      publish_count,
      expected_subscribers
    )
  end

  def validate_publish_pipeline_frames(
        [%{request_id: request_id, status: 0, value: results}],
        request_id,
        publish_count,
        expected_subscribers
      )
      when is_list(results) do
    valid? =
      length(results) == publish_count and
        Enum.all?(results, fn
          ["ok", ^expected_subscribers] -> true
          ^expected_subscribers -> true
          _unexpected -> false
        end)

    if valid? do
      :ok
    else
      unexpected_publish_pipeline!([results], request_id, publish_count, expected_subscribers)
    end
  end

  def validate_publish_pipeline_frames(
        frames,
        request_id,
        publish_count,
        expected_subscribers
      ) do
    unexpected_publish_pipeline!(frames, request_id, publish_count, expected_subscribers)
  end

  def receive_pubsub(socket, channel, message) do
    {frame, ""} = receive_frame(socket, "", 30_000)
    validate_pubsub_frames([frame], channel, message)
  end

  def receive_pubsub_many(socket, channel, message, count, timeout \\ 30_000)
      when is_integer(count) and count >= 0 do
    deadline = System.monotonic_time(:millisecond) + timeout
    receive_pubsub_many(socket, channel, message, count, "", deadline)
  end

  def validate_pubsub_frames(frames, channel, message) when is_list(frames) do
    _delivery_count = pubsub_delivery_count(frames, channel, message)
    :ok
  end

  def pubsub_delivery_count(frames, channel, message) when is_list(frames) do
    Enum.reduce(frames, 0, fn frame, count ->
      count + validate_pubsub_frame(frame, channel, message)
    end)
  end

  def command_round_trip(socket, frame, request_id) do
    :ok = :gen_tcp.send(socket, frame)
    {response, rest} = receive_frame(socket, "", 30_000)

    case response do
      %{request_id: ^request_id, status: 0, value: value} ->
        {value, rest}

      unexpected ->
        raise "native PubSub benchmark received an unexpected response: #{inspect(unexpected)}"
    end
  end

  def receive_frame(socket, buffer, timeout) do
    case decode_server_frames(buffer) do
      {[frame | frames], rest} ->
        trailing = encode_decoded_remainder(frames, rest)
        {frame, trailing}

      {[], rest} ->
        case :gen_tcp.recv(socket, 0, timeout) do
          {:ok, bytes} -> receive_frame(socket, rest <> bytes, timeout)
          {:error, reason} -> raise "native PubSub benchmark receive failed: #{inspect(reason)}"
        end
    end
  end

  def decode_server_frames(buffer) when is_binary(buffer) do
    decode_server_frames(buffer, [])
  end

  defp receive_pubsub_many(_socket, _channel, _message, 0, "", _deadline), do: :ok

  defp receive_pubsub_many(_socket, _channel, _message, 0, trailing, _deadline) do
    raise "native PubSub load benchmark received trailing event bytes: #{byte_size(trailing)}"
  end

  defp receive_pubsub_many(socket, channel, message, remaining, buffer, deadline) do
    case decode_server_frames(buffer) do
      {[], rest} ->
        timeout = max(deadline - System.monotonic_time(:millisecond), 0)

        case :gen_tcp.recv(socket, 0, timeout) do
          {:ok, bytes} ->
            receive_pubsub_many(socket, channel, message, remaining, rest <> bytes, deadline)

          {:error, reason} ->
            raise "native PubSub load receive failed with #{remaining} events remaining: " <>
                    inspect(reason)
        end

      {frames, rest} ->
        deliveries = pubsub_delivery_count(frames, channel, message)

        if deliveries <= remaining do
          receive_pubsub_many(
            socket,
            channel,
            message,
            remaining - deliveries,
            rest,
            deadline
          )
        else
          raise "native PubSub load benchmark received #{deliveries} deliveries with only " <>
                  "#{remaining} expected"
        end
    end
  end

  defp validate_pubsub_frame(
         %{
           opcode: @op_event,
           request_id: 0,
           status: 0,
           value: %{
             "event" => "PUBSUB_MESSAGE",
             "payload" => %{
               "kind" => "message",
               "channel" => channel,
               "message" => message
             }
           }
         },
         channel,
         message
       ),
       do: 1

  defp validate_pubsub_frame(
         %{
           opcode: @op_event,
           request_id: 0,
           status: 0,
           value: %{
             "event" => "PUBSUB_MESSAGE",
             "payload" => %{
               "kind" => "message_batch",
               "channel" => channel,
               "messages" => messages
             }
           }
         },
         channel,
         message
       )
       when is_list(messages) and messages != [] do
    if Enum.all?(messages, &(&1 == message)),
      do: length(messages),
      else: raise("native PubSub benchmark received a batch with an unexpected message")
  end

  defp validate_pubsub_frame(unexpected, _channel, _message) do
    raise "native PubSub benchmark received an unexpected pushed event: #{inspect(unexpected)}"
  end

  defp unexpected_publish_pipeline!(frames, request_id, publish_count, expected_subscribers) do
    raise "native PubSub benchmark received an unexpected pipelined publish response " <>
            "for request #{request_id}, #{publish_count} publishes, and " <>
            "#{expected_subscribers} subscribers: #{inspect(frames)}"
  end

  defp compact_binary(value) do
    value = IO.iodata_to_binary(value)
    [<<byte_size(value)::unsigned-32>>, value]
  end

  defp decode_server_frames(buffer, frames) when byte_size(buffer) < @header_bytes,
    do: {Enum.reverse(frames), buffer}

  defp decode_server_frames(
         <<"FSNP", 0x81, flags, lane_id::unsigned-32, opcode::unsigned-16,
           request_id::unsigned-64, body_len::unsigned-32, body_and_rest::binary>> = buffer,
         frames
       ) do
    if byte_size(body_and_rest) < body_len do
      {Enum.reverse(frames), buffer}
    else
      <<encoded_body::binary-size(^body_len), rest::binary>> = body_and_rest
      body = maybe_decompress(encoded_body, flags)

      case body do
        <<status::unsigned-16, encoded_value::binary>> ->
          value = decode_value!(encoded_value, flags)

          frame = %{
            flags: flags,
            lane_id: lane_id,
            opcode: opcode,
            request_id: request_id,
            status: status,
            value: value
          }

          decode_server_frames(rest, [frame | frames])

        _invalid_body ->
          raise "native PubSub benchmark received a response body shorter than its status"
      end
    end
  end

  defp decode_server_frames(_invalid, _frames) do
    raise "native PubSub benchmark received an invalid response frame"
  end

  defp maybe_decompress(body, flags) do
    if Bitwise.band(flags, @compressed_flag) == 0, do: body, else: :zlib.uncompress(body)
  end

  defp decode_value!(encoded, flags) do
    if Bitwise.band(flags, @custom_payload) != 0 do
      decode_compact_value!(encoded)
    else
      decode_generic_value!(encoded)
    end
  end

  defp decode_compact_value!(<<@compact_integer_list, count::unsigned-32, integers::binary>>)
       when byte_size(integers) == count * 8 do
    for <<integer::signed-64 <- integers>>, do: integer
  end

  defp decode_compact_value!(_encoded) do
    raise "native PubSub response compact value is invalid"
  end

  defp decode_generic_value!(encoded) do
    case Codec.decode_value(encoded) do
      {:ok, value, ""} -> value
      {:ok, _value, _rest} -> raise "native PubSub response value has trailing bytes"
      {:error, reason} -> raise "native PubSub response value is invalid: #{reason}"
    end
  end

  defp encode_decoded_remainder([], rest), do: rest

  defp encode_decoded_remainder(_frames, _rest) do
    raise "native PubSub benchmark decoded more than one frame from a single round trip"
  end
end
