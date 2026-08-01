defmodule FerricstoreServer.Native.Connection.Responses do
  @moduledoc false

  alias Ferricstore.{NativeValueCodec, Stats}
  alias FerricstoreServer.Native.Codec
  alias FerricstoreServer.Native.Connection.PreparedPubSubBatch

  @minimum_integer -0x8000_0000_0000_0000
  @maximum_integer 0x7FFF_FFFF_FFFF_FFFF
  @pubsub_event "PUBSUB_MESSAGE"

  def maxclients_exceeded? do
    Stats.active_connections() > Application.get_env(:ferricstore, :maxclients, 10_000)
  end

  def generate_client_id do
    System.unique_integer([:positive, :monotonic])
  end

  def require_tls? do
    Application.get_env(:ferricstore, :require_tls, false)
  end

  def invalidated_username(:all), do: "all"
  def invalidated_username(username), do: username

  def encode_response(state, opcode, lane_id, request_id, status, value) do
    Codec.encode_command_response_frames(opcode, lane_id, request_id, status, value,
      compression: state.compression,
      compact_flow_responses: state.compact_flow_responses,
      compact_response_codecs: Map.get(state, :compact_response_codecs),
      chunk_bytes: response_chunk_bytes(state),
      max_response_bytes: Map.get(state, :max_response_bytes)
    )
  end

  def encode_event(state, opcode, value) do
    Codec.encode_response_frames(opcode, 0, 0, :ok, value,
      compression: state.compression,
      chunk_bytes: response_chunk_bytes(state),
      max_response_bytes: Map.get(state, :max_response_bytes)
    )
  end

  @doc false
  def encode_pubsub_events(state, opcode, events, at_ms)
      when is_list(events) and is_integer(opcode) and is_integer(at_ms) do
    fast_context = pubsub_fast_context(state, opcode, at_ms)

    Enum.map(events, fn event ->
      encode_pubsub_event(state, opcode, event, at_ms, fast_context)
    end)
  end

  @doc false
  def encode_pubsub_message_batch(state, opcode, channel, messages, at_ms)
      when is_binary(channel) and is_list(messages) and is_integer(opcode) and
             is_integer(at_ms) do
    channel
    |> prepare_pubsub_message_batch(messages, at_ms)
    |> then(&encode_prepared_pubsub_message_batch(state, opcode, &1))
  end

  @doc false
  @spec prepare_pubsub_message_batch(binary(), [binary()], integer()) :: PreparedPubSubBatch.t()
  def prepare_pubsub_message_batch(channel, messages, at_ms)
      when is_binary(channel) and is_list(messages) and is_integer(at_ms) do
    encoded_value =
      NativeValueCodec.encode(%{
        event: @pubsub_event,
        payload: %{kind: "message_batch", channel: channel, messages: messages},
        at_ms: at_ms
      })

    %PreparedPubSubBatch{encoded_value: encoded_value}
  end

  @doc false
  @spec encode_prepared_pubsub_message_batch(map(), integer(), PreparedPubSubBatch.t()) :: [
          iodata()
        ]
  def encode_prepared_pubsub_message_batch(
        state,
        opcode,
        %PreparedPubSubBatch{encoded_value: encoded_value}
      )
      when is_integer(opcode) do
    Codec.encode_preencoded_ok_response_frames(opcode, 0, 0, encoded_value,
      compression: state.compression,
      chunk_bytes: response_chunk_bytes(state),
      max_response_bytes: Map.get(state, :max_response_bytes)
    )
  end

  defp response_chunk_bytes(state) do
    max_frame_bytes =
      Map.get(state, :max_frame_bytes) ||
        Application.get_env(:ferricstore, :native_max_frame_bytes, 16 * 1024 * 1024)

    Codec.effective_response_chunk_bytes(
      Map.get(state, :response_chunk_bytes, 0),
      max_frame_bytes
    )
  end

  defp pubsub_fast_context(%{compression: :none} = state, opcode, at_ms)
       when opcode >= 0 and opcode <= 0xFFFF and at_ms >= @minimum_integer and
              at_ms <= @maximum_integer do
    {:fast, response_chunk_bytes(state), Map.get(state, :max_response_bytes)}
  end

  defp pubsub_fast_context(_state, _opcode, _at_ms), do: :generic

  defp encode_pubsub_event(state, opcode, event, at_ms, {:fast, chunk_bytes, response_bytes}) do
    case pubsub_value_iodata(event, at_ms) do
      {:ok, value} ->
        value_bytes = IO.iodata_length(value)
        body_bytes = value_bytes + 2

        if body_bytes <= chunk_bytes and response_bytes_allow?(response_bytes, value_bytes) do
          [
            <<"FSNP", 0x81, 0, 0::unsigned-32, opcode::unsigned-16, 0::unsigned-64,
              body_bytes::unsigned-32, 0::unsigned-16>>,
            value
          ]
        else
          encode_generic_pubsub_event(state, opcode, event, at_ms)
        end

      :error ->
        encode_generic_pubsub_event(state, opcode, event, at_ms)
    end
  end

  defp encode_pubsub_event(state, opcode, event, at_ms, :generic),
    do: encode_generic_pubsub_event(state, opcode, event, at_ms)

  defp pubsub_value_iodata({:message, channel, message}, at_ms)
       when is_binary(channel) and is_binary(message) do
    payload = [
      <<6, 3::unsigned-32, 7::unsigned-32, "channel">>,
      encoded_binary(channel),
      <<4::unsigned-32, "kind">>,
      encoded_binary("message"),
      <<7::unsigned-32, "message">>,
      encoded_binary(message)
    ]

    {:ok, pubsub_event_value(payload, at_ms)}
  end

  defp pubsub_value_iodata({:pmessage, pattern, channel, message}, at_ms)
       when is_binary(pattern) and is_binary(channel) and is_binary(message) do
    payload = [
      <<6, 4::unsigned-32, 7::unsigned-32, "channel">>,
      encoded_binary(channel),
      <<4::unsigned-32, "kind">>,
      encoded_binary("pmessage"),
      <<7::unsigned-32, "message">>,
      encoded_binary(message),
      <<7::unsigned-32, "pattern">>,
      encoded_binary(pattern)
    ]

    {:ok, pubsub_event_value(payload, at_ms)}
  end

  defp pubsub_value_iodata(_event, _at_ms), do: :error

  defp pubsub_event_value(payload, at_ms) do
    [
      <<6, 3::unsigned-32, 5::unsigned-32, "at_ms", 3, at_ms::signed-64, 5::unsigned-32,
        "event">>,
      encoded_binary(@pubsub_event),
      <<7::unsigned-32, "payload">>,
      payload
    ]
  end

  defp encoded_binary(value), do: [<<4, byte_size(value)::unsigned-32>>, value]

  defp response_bytes_allow?(limit, value_bytes) when is_integer(limit) and limit > 0,
    do: limit >= 2 and value_bytes <= limit - 2

  defp response_bytes_allow?(_limit, _value_bytes), do: true

  defp encode_generic_pubsub_event(state, opcode, event, at_ms) do
    encode_event(state, opcode, %{
      "event" => @pubsub_event,
      "payload" => pubsub_payload(event),
      "at_ms" => at_ms
    })
  end

  defp pubsub_payload({:message, channel, message}) do
    %{"kind" => "message", "channel" => channel, "message" => message}
  end

  defp pubsub_payload({:pmessage, pattern, channel, message}) do
    %{
      "kind" => "pmessage",
      "pattern" => pattern,
      "channel" => channel,
      "message" => message
    }
  end

  def topology_payload do
    %{
      "route_epoch" => :erlang.phash2(FerricStore.Instance.get(:default).slot_map),
      "node" => Atom.to_string(node())
    }
  rescue
    _ -> %{"route_epoch" => 0, "node" => Atom.to_string(node())}
  end

  def acl_invalidation_affects_session?(_state, :all), do: true
  def acl_invalidation_affects_session?(state, username), do: state.username == username

  def coalesce_iodata_size(%{response_coalesce_bytes: limit}, iodata)
      when is_integer(limit) and limit > 0,
      do: IO.iodata_length(iodata)

  def coalesce_iodata_size(_state, _iodata), do: 0

  def coalesce_add_iodata_size(%{response_coalesce_bytes: limit}, bytes, iodata)
      when is_integer(limit) and limit > 0,
      do: bytes + IO.iodata_length(iodata)

  def coalesce_add_iodata_size(_state, bytes, _iodata), do: bytes

  def coalesce_bytes_reached?(%{response_coalesce_bytes: limit}, bytes)
      when is_integer(limit) and limit > 0,
      do: bytes >= limit

  def coalesce_bytes_reached?(_state, _bytes), do: false
end
