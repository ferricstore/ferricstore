defmodule FerricstoreServer.Native.ConnectionDecodeBudgetTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.Native.{Codec, Listener}
  alias FerricstoreServer.Native.Connection.{FrameBuffer, Responses}

  @ping_opcode 0x0003
  @command_exec_opcode 0x0100
  @frame_count 129
  @socket_chunk_bytes 64 * 1024
  @large_frame_body_bytes 4 * 1024 * 1024
  @max_frame_bytes 16 * 1024 * 1024
  @max_buffer_bytes 128 * 1024 * 1024
  @receive_timeout 5_000

  test "drains budgeted frame continuations without waiting for more socket data" do
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    requests =
      1..@frame_count
      |> Enum.map(&Codec.encode_frame(@ping_opcode, 0, &1, ""))
      |> IO.iodata_to_binary()

    assert :ok = :gen_tcp.send(socket, requests)
    assert receive_response_ids(socket, @frame_count) == Enum.to_list(1..@frame_count)
  end

  test "preserves an incomplete frame until the remaining socket bytes arrive" do
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    request = Codec.encode_frame(@ping_opcode, 0, 42, "")
    split_at = byte_size(request) - 2
    <<partial::binary-size(split_at), final_bytes::binary>> = request

    assert :ok = :gen_tcp.send(socket, partial)
    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 25)
    assert :ok = :gen_tcp.send(socket, final_bytes)
    assert receive_response_ids(socket, 1) == [42]
  end

  test "blocking commands inside MULTI are rejected by the session path" do
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    requests =
      [
        command_exec_frame(201, "MULTI", []),
        command_exec_frame(202, "BLPOP", ["transaction:blocking:key", "0.01"]),
        command_exec_frame(203, "EXEC", [])
      ]
      |> IO.iodata_to_binary()

    assert :ok = :gen_tcp.send(socket, requests)

    assert socket |> receive_response_statuses(3) |> Map.new() == %{
             201 => 0,
             202 => 1,
             203 => 1
           }
  end

  test "keeps fragmented multi-megabyte frames chunked until the frame is complete" do
    frame = large_ping_frame(43)
    chunks = binary_chunks(frame, @socket_chunk_bytes)
    {partial_chunks, [final_chunk]} = Enum.split(chunks, -1)

    accumulator =
      Enum.reduce(partial_chunks, FrameBuffer.new(), fn chunk, accumulator ->
        assert {:incomplete, next} =
                 FrameBuffer.append(
                   accumulator,
                   chunk,
                   @max_frame_bytes,
                   @max_buffer_bytes
                 )

        next
      end)

    assert FrameBuffer.stats(accumulator) == %{
             buffered_bytes: Enum.sum(Enum.map(partial_chunks, &byte_size/1)),
             chunk_count: length(partial_chunks),
             complete?: false,
             header_bytes: 24,
             storage: :iodata
           }

    assert {:ready, complete} =
             FrameBuffer.append(
               accumulator,
               final_chunk,
               @max_frame_bytes,
               @max_buffer_bytes
             )

    assert FrameBuffer.materialize(complete) == frame
  end

  test "accepts a complete maximum-size frame coalesced with continuation bytes" do
    max_frame_bytes = 32
    max_buffer_bytes = max_frame_bytes + 24
    body = String.duplicate("x", max_frame_bytes)
    frame_and_continuation = Codec.encode_frame(@ping_opcode, 0, 45, body) <> "N"

    assert {:ready, buffer} =
             FrameBuffer.append(
               FrameBuffer.new(),
               frame_and_continuation,
               max_frame_bytes,
               max_buffer_bytes
             )

    assert {:ok, [{0, @ping_opcode, 45, 0, ^body}], "N", :done} =
             buffer
             |> FrameBuffer.materialize()
             |> Codec.decode_frames(max_frame_bytes)
  end

  test "rejects an oversized coalesced read even when its first frame is complete" do
    max_frame_bytes = 32
    max_buffer_bytes = max_frame_bytes + 24
    body = String.duplicate("x", max_frame_bytes)
    oversized_continuation = :binary.copy("N", 64 * 1024 + 1)

    assert {:error, :buffer_limit} =
             FrameBuffer.append(
               FrameBuffer.new(),
               Codec.encode_frame(@ping_opcode, 0, 46, body) <> oversized_continuation,
               max_frame_bytes,
               max_buffer_bytes
             )
  end

  test "accepts a multi-megabyte frame sent as 64 KiB socket chunks" do
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    frame = large_ping_frame(44)
    chunks = binary_chunks(frame, @socket_chunk_bytes)

    assert length(chunks) > 64
    Enum.each(chunks, fn chunk -> assert :ok = :gen_tcp.send(socket, chunk) end)
    assert receive_response_ids(socket, 1) == [44]
  end

  test "validates configured frame bodies against the wire buffer limit" do
    max_frame_body_bytes = @max_buffer_bytes - 24

    assert FrameBuffer.validate_max_frame_bytes!(max_frame_body_bytes) == max_frame_body_bytes
    assert FrameBuffer.validate_frame_body_bytes!(0) == 0
    assert FrameBuffer.validate_frame_body_bytes!(max_frame_body_bytes) == max_frame_body_bytes

    assert_raise ArgumentError, ~r/native frame body must be between 0 and/, fn ->
      FrameBuffer.validate_frame_body_bytes!(max_frame_body_bytes + 1)
    end

    assert_raise ArgumentError, ~r/native_max_frame_bytes must be an integer between 1 and/, fn ->
      FrameBuffer.validate_max_frame_bytes!(max_frame_body_bytes + 1)
    end

    assert_raise ArgumentError, ~r/native_max_frame_bytes must be an integer between 1 and/, fn ->
      FrameBuffer.validate_max_frame_bytes!(4_294_967_296)
    end

    assert_raise ArgumentError, ~r/native_max_frame_bytes must be an integer between 1 and/, fn ->
      Codec.decode_frames("", 4_294_967_296)
    end
  end

  test "outbound responses never exceed the connection frame limit" do
    max_frame_bytes = 64
    value = String.duplicate("x", max_frame_bytes * 3)

    for configured_chunk_bytes <- [0, max_frame_bytes * 10] do
      state = %{
        compression: :none,
        compact_flow_responses: false,
        max_frame_bytes: max_frame_bytes,
        response_chunk_bytes: configured_chunk_bytes
      }

      frames = Responses.encode_response(state, @ping_opcode, 1, 99, :ok, value)

      assert length(frames) > 1

      bodies =
        for <<"FSNP", 0x81, _flags, _lane_id::unsigned-32, _opcode::unsigned-16,
              _request_id::unsigned-64, body_len::unsigned-32, body::binary>> <- frames do
          assert body_len == byte_size(body)
          assert body_len <= max_frame_bytes
          body
        end

      <<0::unsigned-16, value_body::binary>> = IO.iodata_to_binary(bodies)
      assert {:ok, ^value} = Codec.decode_body(value_body)
    end
  end

  test "compact MGET responses use the connection frame limit when chunking is disabled" do
    max_frame_bytes = 64

    state = %{
      compression: :none,
      compact_flow_responses: false,
      max_frame_bytes: max_frame_bytes,
      response_chunk_bytes: 0
    }

    frames =
      Responses.encode_response(
        state,
        0x0104,
        1,
        100,
        :ok,
        List.duplicate(String.duplicate("x", 32), 8)
      )

    assert length(frames) > 1

    for <<"FSNP", 0x81, _flags, _lane_id::unsigned-32, _opcode::unsigned-16,
          _request_id::unsigned-64, body_len::unsigned-32, body::binary>> <- frames do
      assert body_len == byte_size(body)
      assert body_len <= max_frame_bytes
    end
  end

  test "large response encoders run on dirty CPU schedulers" do
    source_path =
      Path.expand("../../../native/native_protocol_nif/src/lib.rs", __DIR__)

    source = File.read!(source_path)

    for function <- [
          "encode_frame",
          "encode_compact_kv_mget_response_frame",
          "encode_compact_kv_mget"
        ] do
      assert source =~
               ~r/#\[rustler::nif\(schedule = "DirtyCpu"\)\]\s+fn #{function}\b/,
             "expected #{function} to stay off normal BEAM schedulers"
    end
  end

  defp connect do
    {:ok, socket} =
      :gen_tcp.connect(
        {127, 0, 0, 1},
        Listener.port(),
        [:binary, active: false, packet: :raw],
        @receive_timeout
      )

    socket
  end

  defp large_ping_frame(request_id) do
    body =
      Codec.encode_value(%{
        "message" => "PONG",
        "padding" => String.duplicate("x", @large_frame_body_bytes)
      })

    Codec.encode_frame(@ping_opcode, 0, request_id, body)
  end

  defp command_exec_frame(request_id, command, args) do
    body = Codec.encode_value(%{"command" => command, "args" => args})
    Codec.encode_frame(@command_exec_opcode, 0, request_id, body)
  end

  defp binary_chunks(binary, chunk_bytes) do
    full_chunks = for <<chunk::binary-size(chunk_bytes) <- binary>>, do: chunk
    consumed = length(full_chunks) * chunk_bytes

    case binary_part(binary, consumed, byte_size(binary) - consumed) do
      "" -> full_chunks
      remainder -> full_chunks ++ [remainder]
    end
  end

  defp receive_response_ids(socket, expected_count) do
    receive_response_ids(socket, expected_count, "", [])
  end

  defp receive_response_ids(_socket, expected_count, _buffer, ids)
       when length(ids) >= expected_count,
       do: Enum.reverse(ids)

  defp receive_response_ids(socket, expected_count, buffer, ids) do
    {decoded_ids, rest} = decode_response_ids(buffer, [])
    ids = decoded_ids ++ ids

    if length(ids) >= expected_count do
      Enum.reverse(ids)
    else
      assert {:ok, data} = :gen_tcp.recv(socket, 0, @receive_timeout)
      receive_response_ids(socket, expected_count, rest <> data, ids)
    end
  end

  defp decode_response_ids(
         <<"FSNP", 0x81, _flags, 0::unsigned-32, @ping_opcode::unsigned-16,
           request_id::unsigned-64, body_len::unsigned-32, body_and_rest::binary>> = buffer,
         ids
       ) do
    if byte_size(body_and_rest) >= body_len do
      <<_body::binary-size(body_len), rest::binary>> = body_and_rest
      decode_response_ids(rest, [request_id | ids])
    else
      {ids, buffer}
    end
  end

  defp decode_response_ids(buffer, ids), do: {ids, buffer}

  defp receive_response_statuses(socket, expected_count) do
    receive_response_statuses(socket, expected_count, "", [])
  end

  defp receive_response_statuses(_socket, expected_count, _buffer, responses)
       when length(responses) >= expected_count,
       do: Enum.reverse(responses)

  defp receive_response_statuses(socket, expected_count, buffer, responses) do
    {decoded, rest} = decode_response_statuses(buffer, [])
    responses = decoded ++ responses

    if length(responses) >= expected_count do
      Enum.reverse(responses)
    else
      assert {:ok, data} = :gen_tcp.recv(socket, 0, @receive_timeout)
      receive_response_statuses(socket, expected_count, rest <> data, responses)
    end
  end

  defp decode_response_statuses(
         <<"FSNP", 0x81, _flags, 0::unsigned-32, @command_exec_opcode::unsigned-16,
           request_id::unsigned-64, body_len::unsigned-32, body_and_rest::binary>> = buffer,
         responses
       ) do
    if byte_size(body_and_rest) >= body_len do
      <<body::binary-size(body_len), rest::binary>> = body_and_rest
      <<status::unsigned-16, _value::binary>> = body
      decode_response_statuses(rest, [{request_id, status} | responses])
    else
      {responses, buffer}
    end
  end

  defp decode_response_statuses(buffer, responses), do: {responses, buffer}
end
