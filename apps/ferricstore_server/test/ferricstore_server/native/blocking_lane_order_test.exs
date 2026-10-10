defmodule FerricstoreServer.Native.BlockingLaneOrderTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Native.{Codec, Listener}

  setup do
    Acl.reset!()
    on_exit(fn -> Acl.reset!() end)

    socket = connect()
    pusher = connect()
    suffix = System.unique_integer([:positive, :monotonic])
    keys = Enum.map(1..3, &"native:blocking-order:#{suffix}:#{&1}")
    on_exit(fn -> Enum.each(keys, &FerricStore.del/1) end)
    %{socket: socket, pusher: pusher, keys: keys}
  end

  test "later independent lane can unblock a lane with deferred work", %{
    socket: socket,
    keys: [list, key, _]
  } do
    assert :ok = FerricStore.set(key, "value")

    assert :ok =
             :gen_tcp.send(socket, [
               command(1, 10, "BLPOP", [list, "5"]),
               command(1, 11, "GET", [key]),
               command(2, 12, "RPUSH", [list, "item"])
             ])

    responses = Enum.map(1..3, fn _ -> response(socket) end)
    assert Enum.filter(responses, fn {lane, _, _} -> lane == 1 end) == [{1, 10, 0}, {1, 11, 0}]
    assert Enum.filter(responses, fn {lane, _, _} -> lane == 2 end) == [{2, 12, 0}]
  end

  test "replayed work defers again when its next command also blocks", %{
    socket: socket,
    pusher: pusher,
    keys: [first, second, key]
  } do
    assert :ok = FerricStore.set(key, "value")

    assert :ok =
             :gen_tcp.send(socket, [
               command(1, 20, "BLPOP", [first, "5"]),
               command(1, 21, "BLPOP", [second, "5"]),
               command(1, 22, "GET", [key])
             ])

    push(pusher, 30, first, "first")
    assert response(socket) == {1, 20, 0}
    assert {:error, :timeout} = :gen_tcp.recv(socket, 1, 100)
    push(pusher, 31, second, "second")
    assert response(socket) == {1, 21, 0}
    assert response(socket) == {1, 22, 0}
  end

  defp connect do
    {:ok, socket} =
      :gen_tcp.connect({127, 0, 0, 1}, Listener.port(), [:binary, active: false], 2_000)

    on_exit(fn -> :gen_tcp.close(socket) end)
    socket
  end

  defp push(socket, id, key, value) do
    assert :ok = :gen_tcp.send(socket, command(1, id, "RPUSH", [key, value]))
    assert response(socket) == {1, id, 0}
  end

  defp command(lane, id, command, args) do
    Codec.encode_frame(
      0x0100,
      lane,
      id,
      Codec.encode_value(%{"command" => command, "args" => args})
    )
  end

  defp response(socket) do
    assert {:ok,
            <<"FSNP", 0x81, flags, lane::unsigned-32, _opcode::unsigned-16, id::unsigned-64,
              size::unsigned-32>>} = :gen_tcp.recv(socket, 24, 2_000)

    assert {:ok, body} = :gen_tcp.recv(socket, size, 2_000)
    body = if Bitwise.band(flags, 0x08) != 0, do: :zlib.uncompress(body), else: body
    <<status::unsigned-16, _::binary>> = body
    {lane, id, status}
  end
end
