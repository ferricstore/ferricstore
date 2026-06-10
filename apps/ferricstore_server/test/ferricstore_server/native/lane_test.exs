defmodule FerricstoreServer.Native.LaneTest do
  use ExUnit.Case, async: true

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
