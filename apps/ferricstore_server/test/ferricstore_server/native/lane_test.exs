defmodule FerricstoreServer.Native.LaneTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.Native.{Codec, Lane}

  @op_ping 0x0003

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
