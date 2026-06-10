defmodule FerricstoreServer.Native.CodecTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Native.Codec
  alias FerricstoreServer.Native.NIF

  test "request frames round-trip with lane id and request id" do
    body = Codec.encode_value(%{"key" => "a", "value" => "b"})
    frame = Codec.encode_frame(0x0102, 7, 42, body)

    assert {:ok, [{7, 0x0102, 42, 0, ^body}], ""} = Codec.decode_frames(frame, 1024)
  end

  test "native NIF decodes the same frame tuple shape as the Elixir codec" do
    body = Codec.encode_value(%{"key" => "a", "value" => "b"})
    frame = Codec.encode_frame(0x0102, 7, 42, body)

    assert {:ok, [{7, 0x0102, 42, 0, ^body}], ""} = NIF.decode_frames(frame, 1024)
  end

  test "native NIF encodes the same frame header as the Elixir codec" do
    body = Codec.encode_value(%{"key" => "a"})

    assert NIF.encode_frame(0x0101, 3, 9, body, 0, false) ==
             Codec.encode_frame(0x0101, 3, 9, body)
  end

  test "partial request frame leaves bytes in remainder" do
    body = Codec.encode_value(%{"key" => "a"})
    frame = Codec.encode_frame(0x0101, 3, 9, body)
    partial = binary_part(frame, 0, byte_size(frame) - 2)

    assert {:ok, [], ^partial} = Codec.decode_frames(partial, 1024)
  end

  test "response frames use response direction bit and are rejected as client input" do
    response = Codec.encode_response(0x0101, 2, 99, :ok, "value")

    assert {:error, "ERR native client frame cannot use response direction"} =
             Codec.decode_frames(response, 1024)
  end

  test "busy response is structured and retryable" do
    response = Codec.encode_response(0x0101, 2, 99, :busy, "ERR overloaded")

    <<"FSNP", 0x81, _flags, 2::unsigned-32, 0x0101::unsigned-16, 99::unsigned-64,
      body_len::unsigned-32, body::binary>> = response

    assert body_len == byte_size(body)
    <<4::unsigned-16, value_body::binary>> = body
    assert {:ok, value} = Codec.decode_body(value_body)
    assert value["code"] == "busy"
    assert value["message"] == "ERR overloaded"
    assert value["retryable"] == true
    assert value["safe_to_retry"] == true
    assert value["retry_after_ms"] == 100
  end

  test "chunked compressed responses mark compression only on the final logical chunk" do
    frames =
      Codec.encode_response_frames(0x0101, 2, 99, :ok, String.duplicate("x", 256),
        compression: :zlib,
        chunk_bytes: 12
      )

    assert length(frames) > 1
    compressed = Codec.flags().compressed
    more_chunks = Codec.flags().more_chunks

    Enum.each(Enum.drop(frames, -1), fn frame ->
      <<"FSNP", 0x81, flags, _rest::binary>> = frame
      assert Bitwise.band(flags, more_chunks) != 0
      assert Bitwise.band(flags, compressed) == 0
    end)

    <<"FSNP", 0x81, final_flags, _rest::binary>> = List.last(frames)
    assert Bitwise.band(final_flags, more_chunks) == 0
    assert Bitwise.band(final_flags, compressed) != 0
  end

  test "typed values round-trip nested protocol payloads" do
    value = %{
      "nil" => nil,
      "ok" => true,
      "count" => 123,
      "items" => ["a", %{"b" => false}],
      "ratio" => 1.5
    }

    encoded = Codec.encode_value(value)
    assert {:ok, ^value} = Codec.decode_body(encoded)
  end

  test "compact Flow claim jobs response uses custom payload tag" do
    payload =
      Codec.encode_compact_flow_claim_jobs([
        ["flow-1", "bucket-1", "lease-1", 42],
        ["flow-2", nil, "lease-2", 43]
      ])

    tag = Codec.compact_tags().flow_claim_jobs
    assert <<^tag, 2::unsigned-32, _rest::binary>> = payload
  end

  test "compact OK-list response stores only count" do
    tag = Codec.compact_tags().ok_list
    assert <<^tag, 3::unsigned-32>> = Codec.encode_compact_ok_list(["OK", "OK", "OK"])
    assert <<^tag, 3::unsigned-32>> = Codec.encode_compact_ok_list(["ok", "Ok", "oK"])
    assert Codec.encode_compact_ok_list(["OK", "ERR failed"]) == nil
  end

  test "command response frames compact hot Flow many responses when negotiated" do
    [frame] =
      Codec.encode_command_response_frames(0x0210, 3, 42, :ok, ["OK", "OK"],
        compact_flow_responses: true
      )

    tag = Codec.compact_tags().ok_list
    custom_payload = Codec.flags().custom_payload

    <<"FSNP", 0x81, flags, 3::unsigned-32, 0x0210::unsigned-16, 42::unsigned-64,
      body_len::unsigned-32, body::binary>> = frame

    assert body_len == byte_size(body)
    assert Bitwise.band(flags, custom_payload) != 0
    assert <<0::unsigned-16, ^tag, 2::unsigned-32>> = body
  end

  test "command response frames use native compact claim jobs fast path for list jobs" do
    [frame] =
      Codec.encode_command_response_frames(
        0x0203,
        2,
        99,
        :ok,
        [
          ["flow-1", "bucket-1", "lease-1", 42],
          ["flow-2", nil, "lease-2", 43]
        ],
        compact_flow_responses: true
      )

    tag = Codec.compact_tags().flow_claim_jobs
    custom_payload = Codec.flags().custom_payload

    <<"FSNP", 0x81, flags, 2::unsigned-32, 0x0203::unsigned-16, 99::unsigned-64,
      body_len::unsigned-32, body::binary>> = frame

    assert body_len == byte_size(body)
    assert Bitwise.band(flags, custom_payload) != 0
    assert <<0::unsigned-16, ^tag, 2::unsigned-32, _items::binary>> = body
  end

  test "command response frames fall back for compact claim job maps" do
    [frame] =
      Codec.encode_command_response_frames(
        0x0203,
        2,
        99,
        :ok,
        [
          %{
            "id" => "flow-1",
            "partition_key" => "bucket-1",
            "lease_token" => "lease-1",
            "fencing_token" => 42
          }
        ],
        compact_flow_responses: true
      )

    tag = Codec.compact_tags().flow_claim_jobs

    <<"FSNP", 0x81, _flags, _lane::unsigned-32, 0x0203::unsigned-16, _request::unsigned-64,
      _body_len::unsigned-32, body::binary>> = frame

    assert <<0::unsigned-16, ^tag, 1::unsigned-32, _items::binary>> = body
  end

  test "command response frames keep normal encoding when compact is disabled" do
    [frame] =
      Codec.encode_command_response_frames(0x0210, 3, 42, :ok, ["OK", "OK"],
        compact_flow_responses: false
      )

    custom_payload = Codec.flags().custom_payload

    <<"FSNP", 0x81, flags, _rest::binary>> = frame
    assert Bitwise.band(flags, custom_payload) == 0
  end

  test "decodes compact FLOW.CREATE_MANY request body" do
    body =
      <<
        0x90,
        compact_bin("email")::binary,
        compact_bin("queued")::binary,
        123::signed-64,
        124::signed-64,
        2,
        1,
        2::unsigned-32,
        compact_bin("flow-1")::binary,
        compact_bin("payload-1")::binary,
        compact_bin("flow-2")::binary,
        compact_bin("payload-2")::binary
      >>

    assert {:ok,
            %{
              "type" => "email",
              "state" => "queued",
              "now_ms" => 123,
              "run_at_ms" => 124,
              "independent" => true,
              "return" => "OK_ON_SUCCESS",
              "items" => [["flow-1", "payload-1"], ["flow-2", "payload-2"]]
            }} = Codec.decode_body(0x020F, 0x02, body)
  end

  test "decodes compact FLOW.CLAIM_DUE request body" do
    body =
      <<
        0x91,
        compact_bin("email")::binary,
        compact_bin("queued")::binary,
        compact_bin("worker-1")::binary,
        30_000::signed-64,
        500::signed-64,
        -1::signed-64,
        0,
        25::signed-64,
        -9_223_372_036_854_775_808::signed-64,
        1,
        2,
        2::unsigned-32,
        compact_bin("p1")::binary,
        compact_bin("p2")::binary
      >>

    assert {:ok,
            %{
              "type" => "email",
              "state" => "queued",
              "worker" => "worker-1",
              "lease_ms" => 30_000,
              "limit" => 500,
              "reclaim_expired" => false,
              "reclaim_ratio" => 25,
              "return" => "jobs_compact",
              "partition_keys" => ["p1", "p2"]
            }} = Codec.decode_body(0x0203, 0x02, body)
  end

  test "decodes compact FLOW.COMPLETE_MANY request body" do
    body =
      <<
        0x92,
        0xFFFF_FFFF::unsigned-32,
        123::signed-64,
        2,
        1::unsigned-32,
        compact_bin("flow-1")::binary,
        compact_bin("p1")::binary,
        compact_bin("lease-1")::binary,
        7::signed-64
      >>

    assert {:ok,
            %{
              "now_ms" => 123,
              "independent" => true,
              "items" => [["flow-1", "p1", "lease-1", 7]]
            }} = Codec.decode_body(0x0210, 0x02, body)
  end

  defp compact_bin(value) do
    value = IO.iodata_to_binary(value)
    <<byte_size(value)::unsigned-32, value::binary>>
  end
end
