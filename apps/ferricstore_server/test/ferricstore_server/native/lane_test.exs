defmodule FerricstoreServer.Native.LaneTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.Native.{Codec, Lane}

  @op_ping 0x0003
  @op_set 0x0102
  @op_get 0x0101
  @op_mget 0x0104

  test "lane keeps command state internally and applies explicit updates" do
    {:ok, lane} = Lane.start_link(self(), 1, command_state(response_chunk_bytes: 0))

    Lane.enqueue(lane, {1, @op_ping, 1, 0, Codec.encode_value(%{})})

    assert_receive {:native_lane_response, 1, first_response}
    assert [_single_frame] = first_response

    Lane.update_command_state(lane, command_state(response_chunk_bytes: 4))

    Lane.enqueue(lane, {1, @op_ping, 2, 0, Codec.encode_value(%{})})

    assert_receive {:native_lane_response, 1, second_response}
    assert length(second_response) > 1

    send(lane, :shutdown)
  end

  test "trace frames include lane execution timings in response payload" do
    {:ok, lane} = Lane.start_link(self(), 1, command_state(response_chunk_bytes: 0))

    Lane.enqueue(
      lane,
      {:native_trace, {1, @op_ping, 1, Codec.flags().trace, Codec.encode_value(%{})},
       %{
         "server_decode_us" => 1,
         "server_route_us" => 2,
         "server_lane_enqueue_us" => System.monotonic_time(:microsecond)
       }}
    )

    assert_receive {:native_lane_response, 1, [response]}

    <<"FSNP", 0x81, flags, 1::unsigned-32, @op_ping::unsigned-16, 1::unsigned-64,
      body_len::unsigned-32, body::binary>> = response

    assert Bitwise.band(flags, Codec.flags().trace) != 0
    assert body_len == byte_size(body)
    <<0::unsigned-16, value_body::binary>> = body
    assert {:ok, %{"value" => "PONG", "trace" => trace}} = Codec.decode_body(value_body)

    for key <- [
          "server_decode_us",
          "server_route_us",
          "server_lane_queue_wait_us",
          "server_body_decode_us",
          "server_command_execute_us",
          "server_response_encode_us"
        ] do
      assert is_integer(trace[key])
      assert trace[key] >= 0
    end

    refute Map.has_key?(trace, "server_lane_enqueue_us")

    send(lane, :shutdown)
  end

  test "lane batches plain SET frames while preserving ordered replies" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-lane-set-batch-#{System.unique_integer([:positive])}"

    {:ok, lane} =
      Lane.start_link(self(), 3, command_state(instance_ctx: ctx, response_chunk_bytes: 0))

    frames =
      for index <- 1..3 do
        key = "#{prefix}:#{index}"
        {3, @op_set, index, 0, Codec.encode_value(%{"key" => key, "value" => "v#{index}"})}
      end

    Lane.enqueue_many(lane, frames)

    assert_receive {:native_lane_responses, 3, responses, 3}, 2_000
    assert Enum.map(responses, &decode_single_response/1) == [{1, "OK"}, {2, "OK"}, {3, "OK"}]

    for index <- 1..3 do
      value = "v#{index}"
      assert {:ok, ^value} = FerricStore.Impl.get(ctx, "#{prefix}:#{index}")
    end

    send(lane, :shutdown)
  end

  test "lane batches compact GET frames while preserving ordered replies" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-lane-get-batch-#{System.unique_integer([:positive])}"

    assert :ok = FerricStore.Impl.set(ctx, "#{prefix}:1", "v1")
    assert :ok = FerricStore.Impl.set(ctx, "#{prefix}:2", "v2")

    {:ok, lane} =
      Lane.start_link(self(), 5, command_state(instance_ctx: ctx, response_chunk_bytes: 0))

    frames =
      for index <- 1..3 do
        key = "#{prefix}:#{index}"
        {5, @op_get, index, 0, Codec.encode_value(%{"key" => key})}
      end

    Lane.enqueue_many(lane, frames)

    assert_receive {:native_lane_responses, 5, responses, 3}, 2_000
    assert Enum.map(responses, &decode_compact_get_response/1) == [{1, "v1"}, {2, "v2"}, {3, nil}]

    send(lane, :shutdown)
  end

  test "lane coalesces compact MGET frames while preserving ordered batch replies" do
    ctx = FerricStore.Instance.get(:default)
    prefix = "native-lane-mget-batch-#{System.unique_integer([:positive])}"

    assert :ok = FerricStore.Impl.set(ctx, "#{prefix}:1", "v1")
    assert :ok = FerricStore.Impl.set(ctx, "#{prefix}:2", "v2")
    assert :ok = FerricStore.Impl.set(ctx, "#{prefix}:3", "v3")

    {:ok, lane} =
      Lane.start_link(self(), 6, command_state(instance_ctx: ctx, response_chunk_bytes: 0))

    Lane.enqueue_many(lane, [
      {6, @op_mget, 11, Codec.flags().custom_payload,
       compact_mget_body(["#{prefix}:1", "#{prefix}:2"])},
      {6, @op_mget, 12, Codec.flags().custom_payload,
       compact_mget_body(["#{prefix}:3", "#{prefix}:missing"])}
    ])

    assert_receive {:native_lane_responses, 6, responses, 2}, 2_000

    assert Enum.map(responses, &decode_compact_mget_response/1) == [
             {11, ["v1", "v2"]},
             {12, ["v3", nil]}
           ]

    send(lane, :shutdown)
  end

  test "lane falls back for SET frames with options" do
    ctx = FerricStore.Instance.get(:default)
    key = "native-lane-set-ttl-#{System.unique_integer([:positive])}"
    other_key = key <> ":other"

    {:ok, lane} =
      Lane.start_link(self(), 4, command_state(instance_ctx: ctx, response_chunk_bytes: 0))

    Lane.enqueue_many(lane, [
      {4, @op_set, 11, 0, Codec.encode_value(%{"key" => key, "value" => "ttl", "ttl" => 60_000})},
      {4, @op_set, 12, 0, Codec.encode_value(%{"key" => other_key, "value" => "other"})}
    ])

    assert_receive {:native_lane_responses, 4, responses, 2}, 2_000
    assert Enum.map(responses, &decode_single_response/1) == [{11, "OK"}, {12, "OK"}]
    assert {:ok, "ttl"} = FerricStore.Impl.get(ctx, key)
    assert {:ok, "other"} = FerricStore.Impl.get(ctx, other_key)

    send(lane, :shutdown)
  end

  test "stop terminates lane without waiting behind queued frames" do
    {:ok, lane} = Lane.start_link(self(), 7, command_state(response_chunk_bytes: 0))
    ref = Process.monitor(lane)

    frames =
      for index <- 1..10_000 do
        {7, @op_ping, index, 0, Codec.encode_value(%{})}
      end

    Lane.enqueue_many(lane, frames)
    assert :ok = Lane.stop(lane)

    assert_receive {:DOWN, ^ref, :process, ^lane, :shutdown}, 250
  end

  defp decode_single_response([frame]), do: decode_single_response(frame)

  defp decode_single_response(frame) do
    <<"FSNP", 0x81, _flags, _lane_id::unsigned-32, @op_set::unsigned-16, request_id::unsigned-64,
      body_len::unsigned-32, body::binary>> = frame

    assert body_len == byte_size(body)
    <<0::unsigned-16, value_body::binary>> = body

    value =
      case value_body do
        <<0x81, 1::unsigned-32>> ->
          "OK"

        "OK" ->
          "OK"

        _ ->
          assert {:ok, decoded} = Codec.decode_body(value_body)
          decoded
      end

    {request_id, value}
  end

  defp decode_compact_get_response([frame]), do: decode_compact_get_response(frame)

  defp decode_compact_get_response(frame) do
    <<"FSNP", 0x81, flags, _lane_id::unsigned-32, @op_get::unsigned-16, request_id::unsigned-64,
      body_len::unsigned-32, body::binary>> = frame

    assert Bitwise.band(flags, Codec.flags().custom_payload) != 0
    assert body_len == byte_size(body)

    case body do
      <<0::unsigned-16, 0x82, 0>> ->
        {request_id, nil}

      <<0::unsigned-16, 0x82, 1, size::unsigned-32, value::binary-size(size)>> ->
        {request_id, value}
    end
  end

  defp decode_compact_mget_response([frame]), do: decode_compact_mget_response(frame)

  defp decode_compact_mget_response(frame) do
    <<"FSNP", 0x81, flags, _lane_id::unsigned-32, @op_mget::unsigned-16, request_id::unsigned-64,
      body_len::unsigned-32, body::binary>> = frame

    assert Bitwise.band(flags, Codec.flags().custom_payload) != 0
    assert body_len == byte_size(body)

    <<0::unsigned-16, payload::binary>> = body
    {request_id, decode_compact_mget_payload(payload)}
  end

  defp decode_compact_mget_payload(<<0x89, _count::unsigned-32, size::unsigned-32, rest::binary>>) do
    for <<value::binary-size(size) <- rest>>, into: [], do: value
  end

  defp decode_compact_mget_payload(<<0x83, count::unsigned-32, rest::binary>>) do
    {values, ""} =
      Enum.map_reduce(1..count, rest, fn _index, data ->
        case data do
          <<0, next::binary>> ->
            {nil, next}

          <<1, size::unsigned-32, value::binary-size(size), next::binary>> ->
            {value, next}
        end
      end)

    values
  end

  defp compact_mget_body(keys) do
    [
      <<0x94, 2, length(keys)::unsigned-32>>,
      Enum.map(keys, &compact_bin/1)
    ]
    |> IO.iodata_to_binary()
  end

  defp compact_bin(value) do
    value = IO.iodata_to_binary(value)
    <<byte_size(value)::unsigned-32, value::binary>>
  end

  defp command_state(overrides) do
    Map.merge(
      %{
        client_id: System.unique_integer([:positive]),
        client_name: nil,
        username: "default",
        authenticated: true,
        require_auth: false,
        peer: nil,
        created_at: 0,
        instance_ctx: nil,
        stats_counter: :counters.new(10, []),
        acl_cache: FerricstoreServer.Connection.Auth.build_acl_cache("default"),
        event_subscriptions: MapSet.new(),
        flow_wake_subscription: nil,
        compression: :none,
        compact_flow_responses: false,
        response_chunk_bytes: 0,
        close_after_reply: false
      },
      Map.new(overrides)
    )
  end
end
