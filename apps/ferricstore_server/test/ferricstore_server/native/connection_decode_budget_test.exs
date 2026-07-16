defmodule FerricstoreServer.Native.ConnectionDecodeBudgetTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.Native.{Codec, Listener, ResourceBudget}
  alias FerricstoreServer.Native.Connection.{FrameBuffer, Responses}
  alias FerricstoreServer.Acl

  @ping_opcode 0x0003
  @command_exec_opcode 0x0100
  @get_opcode 0x0101
  @no_reply_flag 0x10
  @more_chunks_flag 0x20
  @frame_count 129
  @socket_chunk_bytes 64 * 1024
  @large_frame_body_bytes 4 * 1024 * 1024
  @max_frame_bytes 16 * 1024 * 1024
  @max_buffer_bytes 128 * 1024 * 1024
  @receive_timeout 5_000

  setup do
    Acl.reset!()
    on_exit(fn -> Acl.reset!() end)
    :ok
  end

  @tag :native_command_peek
  test "session classification does not fully decode command payloads" do
    source_path =
      Path.expand("../../../lib/ferricstore_server/native/connection.ex", __DIR__)

    source = File.read!(source_path)

    [_prefix, classifier_and_rest] =
      String.split(source, "defp native_session_payload?", parts: 2)

    [classifier | _rest] = String.split(classifier_and_rest, "\n  defp ", parts: 2)

    assert classifier =~ "Codec.peek_command_name"
    refute classifier =~ "Codec.decode_body"
  end

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

  @tag :frame_assembly_deadline
  test "partial frame assembly has an absolute deadline" do
    previous_timeout = Application.get_env(:ferricstore, :native_frame_assembly_timeout_ms)
    Application.put_env(:ferricstore, :native_frame_assembly_timeout_ms, 40)

    on_exit(fn ->
      restore_env(:native_frame_assembly_timeout_ms, previous_timeout)
    end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    frame = Codec.encode_frame(@ping_opcode, 0, 47, String.duplicate("x", 128))
    assert :ok = :gen_tcp.send(socket, binary_part(frame, 0, 25))

    Process.sleep(80)
    assert_socket_closed(socket)
  end

  @tag :chunk_assembly_deadline
  test "chunked request assembly has an absolute deadline across complete wire frames" do
    previous_timeout = Application.get_env(:ferricstore, :native_frame_assembly_timeout_ms)
    Application.put_env(:ferricstore, :native_frame_assembly_timeout_ms, 80)

    on_exit(fn ->
      restore_env(:native_frame_assembly_timeout_ms, previous_timeout)
    end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    partial_request =
      Codec.encode_frame(@ping_opcode, 1, 48, "partial", @more_chunks_flag)

    assert :ok = :gen_tcp.send(socket, partial_request)
    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 20)

    Process.sleep(35)
    assert :ok = :gen_tcp.send(socket, Codec.encode_frame(@ping_opcode, 0, 49, ""))
    assert receive_response_ids(socket, 1) == [49]

    Process.sleep(60)
    assert_socket_closed(socket)
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

  test "blocking session commands consume the connection inflight window" do
    previous_connection_limit =
      Application.get_env(:ferricstore, :native_max_inflight_per_connection)

    previous_lane_limit = Application.get_env(:ferricstore, :native_max_inflight_per_lane)
    Application.put_env(:ferricstore, :native_max_inflight_per_connection, 1)
    Application.put_env(:ferricstore, :native_max_inflight_per_lane, 1)

    on_exit(fn ->
      restore_env(:native_max_inflight_per_connection, previous_connection_limit)
      restore_env(:native_max_inflight_per_lane, previous_lane_limit)
    end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    requests =
      [
        command_exec_frame(211, "BLPOP", ["native:blocking:held", "5"]),
        command_exec_frame(212, "BLPOP", ["native:blocking:rejected", "0.01"])
      ]
      |> IO.iodata_to_binary()

    assert :ok = :gen_tcp.send(socket, requests)
    assert [{212, 4}] = receive_response_statuses(socket, 1)
  end

  @tag :session_execution_budget
  test "EXEC preserves its transaction when server-wide execution capacity is exhausted" do
    execution_limit =
      Application.get_env(
        :ferricstore,
        :native_max_global_executions,
        max(System.schedulers_online(), 1) * 8
      )

    assert {:ok, budget_token} =
             ResourceBudget.acquire(ResourceBudget, :executions, self(), execution_limit)

    on_exit(fn -> ResourceBudget.release(ResourceBudget, budget_token) end)

    key = "native:session-execution-budget:#{System.unique_integer([:positive])}"
    on_exit(fn -> FerricStore.del(key) end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    assert :ok =
             :gen_tcp.send(
               socket,
               [
                 command_exec_frame(216, "MULTI", []),
                 command_exec_frame(217, "SET", [key, "value"])
               ]
             )

    assert socket |> receive_response_statuses(2) |> Map.new() == %{216 => 0, 217 => 0}

    assert :ok = :gen_tcp.send(socket, command_exec_frame(218, "EXEC", []))
    assert [{218, 4}] = receive_response_statuses(socket, 1)

    assert :ok = ResourceBudget.release(ResourceBudget, budget_token)
    assert :ok = :gen_tcp.send(socket, command_exec_frame(219, "EXEC", []))
    assert [{219, 0}] = receive_response_statuses(socket, 1)
    assert FerricStore.get(key) == {:ok, "value"}
  end

  test "NO_REPLY suppresses a data command rejected by the inflight gate" do
    previous_connection_limit =
      Application.get_env(:ferricstore, :native_max_inflight_per_connection)

    previous_lane_limit = Application.get_env(:ferricstore, :native_max_inflight_per_lane)
    Application.put_env(:ferricstore, :native_max_inflight_per_connection, 1)
    Application.put_env(:ferricstore, :native_max_inflight_per_lane, 1)

    on_exit(fn ->
      restore_env(:native_max_inflight_per_connection, previous_connection_limit)
      restore_env(:native_max_inflight_per_lane, previous_lane_limit)
    end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    requests =
      [
        command_exec_frame(213, "BLPOP", ["native:blocking:no-reply-gate", "5"]),
        Codec.encode_frame(
          @get_opcode,
          1,
          214,
          Codec.encode_value(%{"key" => "native:rejected:no-reply"}),
          @no_reply_flag
        )
      ]
      |> IO.iodata_to_binary()

    assert :ok = :gen_tcp.send(socket, requests)
    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 75)

    assert :ok = :gen_tcp.send(socket, Codec.encode_frame(@ping_opcode, 0, 215, ""))
    assert receive_response_ids(socket, 1) == [215]
  end

  test "NO_REPLY suppresses native session command responses" do
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    requests =
      [
        command_exec_frame(221, "MULTI", [], @no_reply_flag),
        command_exec_frame(222, "DISCARD", [], @no_reply_flag)
      ]
      |> IO.iodata_to_binary()

    assert :ok = :gen_tcp.send(socket, requests)
    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 75)

    assert :ok = :gen_tcp.send(socket, Codec.encode_frame(@ping_opcode, 0, 223, ""))
    assert receive_response_ids(socket, 1) == [223]
  end

  test "NO_REPLY suppresses delayed native blocking responses" do
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    assert :ok =
             :gen_tcp.send(
               socket,
               command_exec_frame(
                 231,
                 "BLPOP",
                 ["native:blocking:no-reply", "0.01"],
                 @no_reply_flag
               )
             )

    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 75)

    assert :ok = :gen_tcp.send(socket, Codec.encode_frame(@ping_opcode, 0, 232, ""))
    assert receive_response_ids(socket, 1) == [232]
  end

  @tag :inflight_idle_timeout
  test "idle timeout does not terminate an active blocking request" do
    previous_timeout = Application.get_env(:ferricstore, :native_idle_timeout_ms)
    Application.put_env(:ferricstore, :native_idle_timeout_ms, 40)

    on_exit(fn ->
      restore_env(:native_idle_timeout_ms, previous_timeout)
    end)

    key = "native:blocking:idle-timeout:#{System.unique_integer([:positive])}"
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    assert :ok =
             :gen_tcp.send(socket, command_exec_frame(232, "BLPOP", [key, "0.12"]))

    assert [{232, 0}] = receive_response_statuses(socket, 1)
  end

  @tag :native_response_byte_budget
  test "all native command forms enforce the connection response byte budget" do
    previous_limit = Application.get_env(:ferricstore, :native_max_response_bytes)
    Application.put_env(:ferricstore, :native_max_response_bytes, 64)

    on_exit(fn ->
      restore_env(:native_max_response_bytes, previous_limit)
    end)

    key = "native:response-budget:#{System.unique_integer([:positive])}"
    assert :ok = FerricStore.set(key, String.duplicate("x", 128))
    on_exit(fn -> FerricStore.del(key) end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    requests =
      [
        Codec.encode_frame(@get_opcode, 1, 233, Codec.encode_value(%{"key" => key})),
        Codec.encode_frame(
          @command_exec_opcode,
          1,
          234,
          Codec.encode_value(%{"command" => "GET", "args" => [key]})
        )
      ]
      |> IO.iodata_to_binary()

    assert :ok = :gen_tcp.send(socket, requests)

    assert socket |> receive_response_statuses(2) |> Map.new() == %{
             233 => 6,
             234 => 6
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

  test "rejects a declared frame larger than the current buffer budget at its header" do
    body = String.duplicate("x", 128)
    header = binary_part(Codec.encode_frame(@ping_opcode, 0, 48, body), 0, 24)

    assert {:error, :buffer_limit} =
             FrameBuffer.append(FrameBuffer.new(), header, 1_024, 64)
  end

  @tag :preauth_frame_budget
  test "unauthenticated connections apply the smaller frame budget" do
    previous_limit =
      Application.get_env(:ferricstore, :native_unauthenticated_max_frame_bytes)

    Application.put_env(:ferricstore, :native_unauthenticated_max_frame_bytes, 64)

    on_exit(fn ->
      restore_env(:native_unauthenticated_max_frame_bytes, previous_limit)
    end)

    username = "preauth-budget-#{System.unique_integer([:positive])}"
    assert :ok = Acl.set_user(username, ["on", ">secret", "+@all", "~*"])
    assert Acl.has_configured_users?()

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    oversized = Codec.encode_frame(@ping_opcode, 0, 49, String.duplicate("x", 128))
    assert :ok = :gen_tcp.send(socket, binary_part(oversized, 0, 24))
    assert_socket_closed(socket)
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

  @tag :queued_request_byte_budget
  test "decoded requests remain charged to the inbound byte budget while queued in a lane" do
    execution_limit =
      Application.get_env(
        :ferricstore,
        :native_max_global_executions,
        max(System.schedulers_online(), 1) * 8
      )

    assert {:ok, execution_token} =
             ResourceBudget.acquire(ResourceBudget, :executions, self(), execution_limit)

    on_exit(fn -> ResourceBudget.release(ResourceBudget, execution_token) end)

    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    body =
      Codec.encode_value(%{
        "key" => String.duplicate("q", @large_frame_body_bytes)
      })

    request = Codec.encode_frame(@get_opcode, 1, 441, body)
    assert :ok = :gen_tcp.send(socket, request)

    assert eventually(fn ->
             usage = ResourceBudget.usage(ResourceBudget)
             usage.lanes >= 1 and usage.inbound_bytes >= byte_size(request)
           end)

    assert :ok = ResourceBudget.release(ResourceBudget, execution_token)
    assert [{441, _status}] = receive_response_statuses(socket, 1)

    assert eventually(fn ->
             ResourceBudget.usage(ResourceBudget).inbound_bytes == 0
           end)
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

    for encoded <- frames do
      frame = IO.iodata_to_binary(encoded)

      assert <<"FSNP", 0x81, _flags, _lane_id::unsigned-32, _opcode::unsigned-16,
               _request_id::unsigned-64, body_len::unsigned-32, body::binary>> = frame

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
          "encode_compact_claim_jobs_response_frame",
          "encode_compact_ok_list_response_frame",
          "encode_compact_kv_get_response_frame",
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

  defp command_exec_frame(request_id, command, args, flags \\ 0) do
    body = Codec.encode_value(%{"command" => command, "args" => args})
    Codec.encode_frame(@command_exec_opcode, 0, request_id, body, flags)
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  defp eventually(fun, attempts \\ 100)

  defp eventually(fun, attempts) do
    cond do
      fun.() ->
        true

      attempts == 0 ->
        false

      true ->
        Process.sleep(10)
        eventually(fun, attempts - 1)
    end
  end

  defp assert_socket_closed(socket, attempts \\ 20)

  defp assert_socket_closed(_socket, 0), do: flunk("native connection remained open")

  defp assert_socket_closed(socket, attempts) do
    case :gen_tcp.recv(socket, 0, 25) do
      {:error, :closed} -> :ok
      {:error, :timeout} -> assert_socket_closed(socket, attempts - 1)
      {:ok, _data} -> assert_socket_closed(socket, attempts - 1)
    end
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
         <<"FSNP", 0x81, _flags, _lane_id::unsigned-32, _opcode::unsigned-16,
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
