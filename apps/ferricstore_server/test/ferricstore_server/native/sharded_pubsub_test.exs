defmodule FerricstoreServer.Native.ShardedPubSubTest do
  @moduledoc """
  SSUBSCRIBE / SUNSUBSCRIBE over the native protocol: Redis's acknowledgements
  (counting shard channels only), SPUBLISH delivered as a `PUBSUB_MESSAGE`
  event of kind `smessage`, and shard channels kept apart from ordinary ones.
  """

  use ExUnit.Case, async: false

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Native.{Codec, Listener, Session}

  @command_exec_opcode 0x0100
  @receive_timeout 5_000

  setup do
    Acl.reset!()
    on_exit(fn -> Acl.reset!() end)
    {:ok, channel: "native-shard:#{System.unique_integer([:positive])}"}
  end

  test "SPUBLISH arrives as an smessage event; PUBLISH does not", %{channel: channel} do
    shard = connect()
    plain = connect()

    assert {0, [["ssubscribe", ^channel, 1]]} = call(shard, 1, "SSUBSCRIBE", [channel])
    assert {0, [["subscribe", ^channel, 1]]} = call(plain, 2, "SUBSCRIBE", [channel])

    assert 1 == Ferricstore.PubSub.spublish(channel, "hello")

    assert %{
             "event" => "PUBSUB_MESSAGE",
             "payload" => %{"kind" => "smessage", "channel" => ^channel, "message" => "hello"}
           } = event(shard)

    assert 1 == Ferricstore.PubSub.publish(channel, "plain")
    assert %{"payload" => %{"kind" => "message", "message" => "plain"}} = event(plain)

    assert Process.get({:buffer, shard}) == nil
    assert {:error, :timeout} = :gen_tcp.recv(shard, 0, 50)
  end

  test "acknowledgements count shard channels apart from the others", %{channel: channel} do
    socket = connect()
    a = channel <> ":a"
    b = channel <> ":b"

    assert {0, [["subscribe", ^channel, 1]]} = call(socket, 1, "SUBSCRIBE", [channel])

    # As in Redis, every argument is acknowledged, a repeated one too.
    assert {0, [["ssubscribe", ^a, 1], ["ssubscribe", ^b, 2], ["ssubscribe", ^a, 2]]} =
             call(socket, 2, "SSUBSCRIBE", [a, b, a])

    assert {0, [["sunsubscribe", ^a, 1]]} = call(socket, 3, "SUNSUBSCRIBE", [a])
    assert {0, [["sunsubscribe", ^b, 0]]} = call(socket, 4, "SUNSUBSCRIBE", [])
    assert {0, [["sunsubscribe", nil, 0]]} = call(socket, 5, "SUNSUBSCRIBE", [])

    # The ordinary subscription is untouched.
    assert {0, [["unsubscribe", ^channel, 0]]} = call(socket, 6, "UNSUBSCRIBE", [])
    assert 0 == Ferricstore.PubSub.spublish(a, "gone")
  end

  test "SSUBSCRIBE needs a channel and is refused inside MULTI", %{channel: channel} do
    socket = connect()

    assert {status, %{"message" => "ERR wrong number of arguments for 'ssubscribe' command"}} =
             call(socket, 1, "SSUBSCRIBE", [])

    assert status != 0

    assert {0, "OK"} = call(socket, 2, "MULTI", [])
    assert {status, %{"message" => message}} = call(socket, 3, "SSUBSCRIBE", [channel])
    assert status != 0
    assert message =~ "not allowed inside a transaction"
    assert {0, "OK"} = call(socket, 4, "DISCARD", [])
  end

  test "SSUBSCRIBE and SPUBLISH check channel permissions" do
    assert :ok =
             Acl.set_user("shard-sub", [
               "on",
               "nopass",
               "-@all",
               "+ssubscribe",
               "+spublish",
               "&tenant:a:*"
             ])

    state = %{
      username: "shard-sub",
      authenticated: true,
      require_auth: false,
      acl_cache: ConnAuth.build_acl_cache("shard-sub"),
      peer: {{127, 0, 0, 1}, 5_678},
      client_id: 321
    }

    assert {:noperm, payload, _state} =
             Session.execute(%{"command" => "SSUBSCRIBE", "args" => ["tenant:b:x"]}, state)

    assert payload =~ "channels mentioned"

    assert {:ok, [["ssubscribe", "tenant:a:x", 1]], _state} =
             Session.execute(%{"command" => "SSUBSCRIBE", "args" => ["tenant:a:x"]}, state)

    assert Ferricstore.PubSub.shard_numsub(["tenant:a:x"]) == ["tenant:a:x", 1]
    Ferricstore.PubSub.cleanup(self())
  end

  test "the dashboard lists shard channels apart from channels", %{channel: channel} do
    alias FerricstoreServer.Health.Dashboard.Data.Messaging
    alias FerricstoreServer.Health.Dashboard.Render.MessagingPages

    socket = connect()
    assert {0, [["ssubscribe", ^channel, 1]]} = call(socket, 1, "SSUBSCRIBE", [channel])
    assert 1 == Ferricstore.PubSub.spublish(channel, "seen")

    data = Messaging.collect_pubsub_page()
    assert Enum.any?(data.shard_channels, &(&1.channel == channel))
    refute Enum.any?(data.channels, &(&1.channel == channel))
    assert data.summary.shard_subscriptions >= 1

    assert MessagingPages.render_pubsub_shard_channels(data) =~ channel
    refute MessagingPages.render_pubsub_channels(data) =~ channel
    assert MessagingPages.render_pubsub_shard_channels(%{}) =~ ~s(<p class="section-empty">)

    assert Enum.any?(
             Ferricstore.PubSub.ActivityLog.get(),
             &(&1.command == "SPUBLISH" and &1.target == channel and &1.subscribers == 1)
           )
  end

  defp connect do
    {:ok, socket} =
      :gen_tcp.connect(
        {127, 0, 0, 1},
        Listener.port(),
        [:binary, active: false, packet: :raw],
        @receive_timeout
      )

    on_exit(fn -> :gen_tcp.close(socket) end)
    socket
  end

  defp call(socket, request_id, command, args) do
    body = Codec.encode_value(%{"command" => command, "args" => args})
    :ok = :gen_tcp.send(socket, Codec.encode_frame(@command_exec_opcode, 0, request_id, body))
    {^request_id, status, value} = receive_frame(socket)
    {status, value}
  end

  defp event(socket) do
    {_request_id, 0, value} = receive_frame(socket)
    value
  end

  # Leftover bytes of a read that carried more than one frame wait in the
  # process dictionary for the next call.
  defp receive_frame(socket) do
    receive_frame(socket, Process.delete({:buffer, socket}) || "")
  end

  defp receive_frame(socket, buffer) do
    case buffer do
      <<"FSNP", 0x81, _flags, _lane::unsigned-32, _opcode::unsigned-16, request_id::unsigned-64,
        body_len::unsigned-32, body::binary-size(body_len), rest::binary>> ->
        if rest != "", do: Process.put({:buffer, socket}, rest)
        <<status::unsigned-16, value_body::binary>> = body
        {:ok, value} = Codec.decode_body(value_body)
        {request_id, status, value}

      _incomplete ->
        {:ok, data} = :gen_tcp.recv(socket, 0, @receive_timeout)
        receive_frame(socket, buffer <> data)
    end
  end
end
