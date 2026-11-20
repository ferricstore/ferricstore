defmodule FerricstoreServer.Native.ConnectionIdleLaneTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.Native.{Codec, Listener, ResourceBudget}
  alias FerricstoreServer.Acl

  @get_opcode 0x0101
  @receive_timeout 5_000

  setup do
    Acl.reset!()
    previous_idle_ms = Application.get_env(:ferricstore, :native_lane_idle_ms)

    on_exit(fn ->
      Acl.reset!()

      if previous_idle_ms,
        do: Application.put_env(:ferricstore, :native_lane_idle_ms, previous_idle_ms),
        else: Application.delete_env(:ferricstore, :native_lane_idle_ms)
    end)

    assert eventually(fn -> ResourceBudget.usage(ResourceBudget).lanes == 0 end)
    :ok
  end

  # A client that spreads requests over many lanes leaves a lane process, and
  # its server-wide lane slot, behind for every lane id it ever used. Once
  # those fill the budget, a new lane was refused even though the lanes holding
  # it had nothing to do.
  test "a connection frees its idle lanes when the server's lane budget is full" do
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    for lane <- 1..4, do: assert([{^lane, 0}] = get(socket, lane, lane))

    held = lane_limit() - ResourceBudget.usage(ResourceBudget).lanes
    assert {:ok, token} = ResourceBudget.acquire(ResourceBudget, :lanes, self(), held)
    on_exit(fn -> ResourceBudget.release(ResourceBudget, token) end)

    assert [{105, 0}] = get(socket, 5, 105)
    assert [{106, 0}] = get(socket, 1, 106)
  end

  test "lanes idle past the idle period stop and start again on their next frame" do
    Application.put_env(:ferricstore, :native_lane_idle_ms, 50)
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    for lane <- 1..4, do: assert([{^lane, 0}] = get(socket, lane, lane))

    assert eventually(fn -> ResourceBudget.usage(ResourceBudget).lanes == 0 end)
    assert [{202, 0}] = get(socket, 2, 202)
    assert ResourceBudget.usage(ResourceBudget).lanes == 1
  end

  test "a lane with a request in flight is not stopped as idle" do
    Application.put_env(:ferricstore, :native_lane_idle_ms, 50)
    socket = connect()
    on_exit(fn -> :gen_tcp.close(socket) end)

    assert [{1, 0}] = get(socket, 1, 1)
    assert [{2, 0}] = get(socket, 2, 2)
    Process.sleep(20)
    # Lane 2 keeps getting work; lane 1 goes idle and is stopped.
    for request <- 3..12 do
      assert [{^request, 0}] = get(socket, 2, request)
      Process.sleep(10)
    end

    assert eventually(fn -> ResourceBudget.usage(ResourceBudget).lanes == 0 end)
  end

  defp lane_limit do
    Application.get_env(
      :ferricstore,
      :native_max_global_lanes,
      max(System.schedulers_online(), 1) * 32
    )
  end

  defp get(socket, lane, request_id) do
    body = Codec.encode_value(%{"key" => "idle-lane-#{System.unique_integer([:positive])}"})
    assert :ok = :gen_tcp.send(socket, Codec.encode_frame(@get_opcode, lane, request_id, body))
    receive_response_statuses(socket, 1)
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

  defp eventually(fun, attempts \\ 100)
  defp eventually(_fun, 0), do: false

  defp eventually(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(20)
      eventually(fun, attempts - 1)
    end
  end

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
      <<body::binary-size(^body_len), rest::binary>> = body_and_rest
      <<status::unsigned-16, _value::binary>> = body
      decode_response_statuses(rest, [{request_id, status} | responses])
    else
      {responses, buffer}
    end
  end

  defp decode_response_statuses(buffer, responses), do: {responses, buffer}
end
