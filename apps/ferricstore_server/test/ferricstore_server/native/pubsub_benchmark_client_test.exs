Code.require_file(Path.expand("../../../../../bench/support/native_pubsub_client.exs", __DIR__))

defmodule FerricstoreServer.Native.PubSubBenchmarkClientTest do
  use ExUnit.Case, async: true

  alias FerricstoreBench.NativePubSubClient
  alias FerricstoreServer.Native.Codec
  alias FerricstoreServer.Native.Connection.Responses

  test "encodes Redis-compatible PubSub commands as native COMMAND_EXEC frames" do
    frame = NativePubSubClient.command_exec_frame(7, "PUBLISH", ["orders", "ready"])
    assert {:ok, [{0, 0x0100, 7, 0, body}], "", :done} = Codec.decode_frames(frame, 1_024)

    assert {:ok, %{"command" => "PUBLISH", "args" => ["orders", "ready"]}} =
             Codec.decode_body(body)

    routed_frame =
      NativePubSubClient.command_exec_frame(8, "PUBLISH", ["orders", "ready"], 3)

    assert {:ok, [{3, 0x0100, 8, 0, _body}], "", :done} =
             Codec.decode_frames(routed_frame, 1_024)
  end

  test "encodes explicit PubSub batch capability negotiation as a native HELLO frame" do
    frame = NativePubSubClient.pubsub_batch_hello_frame(11)

    assert {:ok, [{0, 0x0001, 11, 0, body}], "", :done} = Codec.decode_frames(frame, 1_024)

    assert {:ok,
            %{
              "compact_flow_responses" => false,
              "compact_response_codecs" => ["pubsub_batch_v1"]
            }} = Codec.decode_body(body)
  end

  test "encodes a batch of publishes as one native PIPELINE frame" do
    frame =
      NativePubSubClient.publish_pipeline_frame(
        19,
        [{"orders", "one"}, {"orders", "two"}],
        3
      )

    assert {:ok, [{3, 0x000E, 19, 0, body}], "", :done} = Codec.decode_frames(frame, 4_096)
    assert {:ok, payload} = Codec.decode_body(body)
    assert payload["atomicity"] == "none"
    assert payload["return"] == "pairs"

    assert [first, second] = payload["commands"]
    assert first["request_id"] == 1
    assert first["lane_id"] == 3
    assert first["body"] == %{"command" => "PUBLISH", "args" => ["orders", "one"]}
    assert second["request_id"] == 2
    assert second["body"] == %{"command" => "PUBLISH", "args" => ["orders", "two"]}
  end

  test "encodes compact PubSub pipelines and validates their integer counts" do
    frame =
      NativePubSubClient.compact_publish_pipeline_frame(
        20,
        [{"orders", "one"}, {"priority", "two"}],
        3
      )

    custom_payload = Codec.flags().custom_payload

    assert {:ok, [{3, 0x000E, 20, ^custom_payload, body}], "", :done} =
             Codec.decode_frames(frame, 4_096)

    assert {:ok,
            %{
              "compact_count" => 2,
              "compact_values" => true,
              "compact_pipeline" => {35, [{"orders", "one"}, {"priority", "two"}]}
            }} = Codec.decode_body(0x000E, custom_payload, body)

    response = %{request_id: 20, status: 0, value: [8, 8]}
    assert :ok = NativePubSubClient.validate_publish_pipeline_frames([response], 20, 2, 8)
  end

  test "decodes coalesced command responses and pushed PubSub events" do
    response = Codec.encode_response(0x0100, 0, 9, :ok, 1)

    event =
      Codec.encode_event(0x0010, %{
        "event" => "PUBSUB_MESSAGE",
        "payload" => %{
          "kind" => "message",
          "channel" => "orders",
          "message" => "ready"
        }
      })

    assert {[%{request_id: 9, status: 0, value: 1}, decoded_event], ""} =
             NativePubSubClient.decode_server_frames(response <> event)

    assert decoded_event.request_id == 0
    assert decoded_event.opcode == 0x0010
    assert decoded_event.status == 0
    assert decoded_event.value["event"] == "PUBSUB_MESSAGE"
    assert decoded_event.value["payload"]["channel"] == "orders"
  end

  test "decodes compact integer-list pipeline responses" do
    payload = Codec.encode_compact_integer_list([8, 8, 8])
    [response] = Codec.encode_compact_response_frames(0x000E, 3, 20, :ok, payload)

    assert {[%{flags: flags, lane_id: 3, request_id: 20, status: 0, value: [8, 8, 8]}], ""} =
             NativePubSubClient.decode_server_frames(response)

    assert Bitwise.band(flags, Codec.flags().custom_payload) != 0
  end

  test "retains an incomplete pushed frame for the next socket read" do
    frame = Codec.encode_event(0x0010, %{"event" => "PUBSUB_MESSAGE"})
    split_at = byte_size(frame) - 3
    <<partial::binary-size(split_at), final::binary>> = frame

    assert {[], ^partial} = NativePubSubClient.decode_server_frames(partial)

    assert {[%{request_id: 0, status: 0}], ""} =
             NativePubSubClient.decode_server_frames(partial <> final)
  end

  test "batch-encodes message and pattern events as ordinary native frames" do
    state = %{
      compression: :none,
      max_frame_bytes: 1_024,
      max_response_bytes: 1_024,
      response_chunk_bytes: 0
    }

    frames =
      Responses.encode_pubsub_events(
        state,
        0x0010,
        [
          {:message, "orders", "ready"},
          {:pmessage, "orders:*", "orders:priority", "urgent"}
        ],
        1_234
      )

    assert length(frames) == 2

    assert {decoded, ""} =
             frames
             |> IO.iodata_to_binary()
             |> NativePubSubClient.decode_server_frames()

    assert [message, pattern] = Enum.map(decoded, & &1.value)

    assert message == %{
             "event" => "PUBSUB_MESSAGE",
             "payload" => %{
               "kind" => "message",
               "channel" => "orders",
               "message" => "ready"
             },
             "at_ms" => 1_234
           }

    assert pattern == %{
             "event" => "PUBSUB_MESSAGE",
             "payload" => %{
               "kind" => "pmessage",
               "pattern" => "orders:*",
               "channel" => "orders:priority",
               "message" => "urgent"
             },
             "at_ms" => 1_234
           }
  end

  test "encodes one negotiated batch envelope with ordered message payloads" do
    state = %{
      compression: :none,
      max_frame_bytes: 1_024,
      max_response_bytes: 1_024,
      response_chunk_bytes: 0
    }

    [encoded] =
      Responses.encode_pubsub_message_batch(
        state,
        0x0010,
        "orders",
        ["one", "two", "three"],
        1_234
      )

    assert {[frame], ""} =
             encoded
             |> IO.iodata_to_binary()
             |> NativePubSubClient.decode_server_frames()

    assert frame.value == %{
             "event" => "PUBSUB_MESSAGE",
             "payload" => %{
               "kind" => "message_batch",
               "channel" => "orders",
               "messages" => ["one", "two", "three"]
             },
             "at_ms" => 1_234
           }
  end

  test "prepared negotiated batches reuse one encoded value across connection frames" do
    state = %{
      compression: :none,
      max_frame_bytes: 1_024,
      max_response_bytes: 1_024,
      response_chunk_bytes: 0
    }

    prepared =
      Responses.prepare_pubsub_message_batch("orders", ["one", "two", "three"], 1_234)

    [first] = Responses.encode_prepared_pubsub_message_batch(state, 0x0010, prepared)
    [second] = Responses.encode_prepared_pubsub_message_batch(state, 0x0010, prepared)

    assert [_header, <<0::unsigned-16>>, first_value] = first
    assert [_header, <<0::unsigned-16>>, second_value] = second
    assert :erts_debug.same(first_value, prepared.encoded_value)
    assert :erts_debug.same(second_value, prepared.encoded_value)

    assert IO.iodata_to_binary(first) ==
             Responses.encode_event(state, 0x0010, %{
               event: "PUBSUB_MESSAGE",
               payload: %{
                 kind: "message_batch",
                 channel: "orders",
                 messages: ["one", "two", "three"]
               },
               at_ms: 1_234
             })
             |> IO.iodata_to_binary()
  end

  test "prepared negotiated batches preserve compression, chunking, and response limits" do
    payload = %{
      event: "PUBSUB_MESSAGE",
      payload: %{
        kind: "message_batch",
        channel: "orders",
        messages: [String.duplicate("x", 128), String.duplicate("y", 128)]
      },
      at_ms: 1_234
    }

    prepared =
      Responses.prepare_pubsub_message_batch(
        "orders",
        [String.duplicate("x", 128), String.duplicate("y", 128)],
        1_234
      )

    for state <- [
          %{
            compression: :zlib,
            max_frame_bytes: 1_024,
            max_response_bytes: 1_024,
            response_chunk_bytes: 0
          },
          %{
            compression: :none,
            max_frame_bytes: 64,
            max_response_bytes: 1_024,
            response_chunk_bytes: 0
          },
          %{
            compression: :none,
            max_frame_bytes: 1_024,
            max_response_bytes: 64,
            response_chunk_bytes: 0
          }
        ] do
      assert state
             |> Responses.encode_prepared_pubsub_message_batch(0x0010, prepared)
             |> IO.iodata_to_binary() ==
               state
               |> Responses.encode_event(0x0010, payload)
               |> IO.iodata_to_binary()
    end
  end

  test "batch event encoding preserves the generic compressed framing fallback" do
    state = %{
      compression: :zlib,
      max_frame_bytes: 1_024,
      max_response_bytes: 1_024,
      response_chunk_bytes: 0
    }

    [frame] =
      Responses.encode_pubsub_events(
        state,
        0x0010,
        [{:message, "orders", "ready"}],
        1_234
      )

    payload = %{
      "event" => "PUBSUB_MESSAGE",
      "payload" => %{"kind" => "message", "channel" => "orders", "message" => "ready"},
      "at_ms" => 1_234
    }

    assert frame == Responses.encode_event(state, 0x0010, payload)
  end

  test "batch event encoding preserves generic frame and response limits" do
    payload = %{
      "event" => "PUBSUB_MESSAGE",
      "payload" => %{
        "kind" => "message",
        "channel" => "orders",
        "message" => String.duplicate("x", 128)
      },
      "at_ms" => 1_234
    }

    for state <- [
          %{
            compression: :none,
            max_frame_bytes: 64,
            max_response_bytes: 1_024,
            response_chunk_bytes: 0
          },
          %{
            compression: :none,
            max_frame_bytes: 1_024,
            max_response_bytes: 64,
            response_chunk_bytes: 0
          }
        ] do
      [frames] =
        Responses.encode_pubsub_events(
          state,
          0x0010,
          [{:message, "orders", String.duplicate("x", 128)}],
          1_234
        )

      assert frames == Responses.encode_event(state, 0x0010, payload)
    end
  end

  test "validates publish counts and batches of pushed events for load benchmarks" do
    assert 2 ==
             NativePubSubClient.publish_count_from_frames(
               [%{request_id: 17, status: 0, value: 2}],
               17
             )

    frames =
      Enum.map(1..3, fn _index ->
        %{
          opcode: 0x0010,
          request_id: 0,
          status: 0,
          value: %{
            "event" => "PUBSUB_MESSAGE",
            "payload" => %{
              "kind" => "message",
              "channel" => "orders",
              "message" => "ready"
            }
          }
        }
      end)

    assert :ok = NativePubSubClient.validate_pubsub_frames(frames, "orders", "ready")

    batch_frame = %{
      opcode: 0x0010,
      request_id: 0,
      status: 0,
      value: %{
        "event" => "PUBSUB_MESSAGE",
        "payload" => %{
          "kind" => "message_batch",
          "channel" => "orders",
          "messages" => ["ready", "ready", "ready"]
        }
      }
    }

    assert 3 == NativePubSubClient.pubsub_delivery_count([batch_frame], "orders", "ready")

    assert_raise RuntimeError, ~r/unexpected publish response/, fn ->
      NativePubSubClient.publish_count_from_frames(
        [%{request_id: 18, status: 0, value: 2}],
        17
      )
    end

    assert_raise RuntimeError, ~r/unexpected pushed event/, fn ->
      NativePubSubClient.validate_pubsub_frames(frames, "other", "ready")
    end
  end

  test "validates every result in a pipelined publish response" do
    response = %{request_id: 44, status: 0, value: [["ok", 8], ["ok", 8], ["ok", 8]]}

    assert :ok =
             NativePubSubClient.validate_publish_pipeline_frames([response], 44, 3, 8)

    assert_raise RuntimeError, ~r/unexpected pipelined publish response/, fn ->
      NativePubSubClient.validate_publish_pipeline_frames([response], 44, 2, 8)
    end

    assert_raise RuntimeError, ~r/unexpected pipelined publish response/, fn ->
      NativePubSubClient.validate_publish_pipeline_frames(
        [%{response | value: [["ok", 8], ["ok", 7], ["ok", 8]]}],
        44,
        3,
        8
      )
    end
  end
end
