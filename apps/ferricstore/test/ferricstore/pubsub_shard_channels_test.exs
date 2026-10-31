defmodule Ferricstore.PubSubShardChannelsTest do
  @moduledoc """
  Shard channels (SSUBSCRIBE, SPUBLISH) are their own namespace, as in Redis:
  PUBLISH never reaches a shard subscriber, SPUBLISH never reaches a SUBSCRIBE
  or PSUBSCRIBE subscriber, and PUBSUB CHANNELS / NUMSUB list only ordinary
  channels while PUBSUB SHARDCHANNELS / SHARDNUMSUB list only shard channels.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.PubSub

  setup do
    {:ok, channel: "shard-test:#{System.unique_integer([:positive])}"}
  end

  test "SPUBLISH reaches shard subscribers only, and PUBLISH never does", %{channel: channel} do
    shard = subscriber(fn pid -> PubSub.ssubscribe_many([channel], pid) end)
    plain = subscriber(fn pid -> PubSub.subscribe_many([channel], pid) end)
    patterned = subscriber(fn pid -> PubSub.psubscribe_many([channel <> "*"], pid) end)

    assert 1 == PubSub.spublish(channel, "s1")
    assert_receive {:got, ^shard, {:pubsub_smessage, ^channel, "s1"}}
    refute_receive {:got, ^plain, _}, 50
    refute_receive {:got, ^patterned, _}, 50

    assert 2 == PubSub.publish(channel, "p1")
    assert_receive {:got, ^plain, {:pubsub_message, ^channel, "p1"}}
    assert_receive {:got, ^patterned, {:pubsub_pmessage, _pattern, ^channel, "p1"}}
    refute_receive {:got, ^shard, _}, 50
  end

  test "introspection keeps the two namespaces apart", %{channel: channel} do
    other = channel <> ":other"
    subscriber(fn pid -> PubSub.ssubscribe_many([channel, other], pid) end)
    subscriber(fn pid -> PubSub.ssubscribe_many([channel], pid) end)
    subscriber(fn pid -> PubSub.subscribe_many([other], pid) end)

    assert Enum.sort(PubSub.shard_channels(channel <> "*")) == [channel, other]
    assert PubSub.channels(channel <> "*") == [other]

    assert PubSub.shard_numsub([channel, other, "missing"]) == [
             channel,
             2,
             other,
             1,
             "missing",
             0
           ]

    assert PubSub.numsub([channel, other]) == [channel, 0, other, 1]
    assert PubSub.numpat() >= 0

    assert Enum.sort(dispatch("PUBSUB", ["SHARDCHANNELS", channel <> "*"])) == [channel, other]
    assert dispatch("PUBSUB", ["shardnumsub", channel]) == [channel, 2]
    assert dispatch("PUBSUB", ["CHANNELS", channel <> "*"]) == [other]

    snapshot = PubSub.subscription_snapshot(1_000)
    assert Enum.all?(snapshot.channels, &is_binary(&1.channel))

    assert %{channel: ^other, subscribers: 1} =
             Enum.find(snapshot.channels, &(&1.channel == other))

    refute Enum.any?(snapshot.channels, &(&1.channel == channel))

    assert %{channel: ^channel, subscribers: 2} =
             Enum.find(snapshot.shard_channels, &(&1.channel == channel))
  end

  test "SPUBLISH through the dispatcher counts receivers and checks arity", %{channel: channel} do
    assert 0 == dispatch("SPUBLISH", [channel, "nobody"])

    subscriber(fn pid -> PubSub.ssubscribe_many([channel], pid) end)
    assert 1 == dispatch("SPUBLISH", [channel, "one"])

    assert {:error, "ERR wrong number of arguments for 'spublish' command"} =
             dispatch("SPUBLISH", [channel])

    assert {:ok, prepared} = Dispatcher.prepare_raw("SPUBLISH", [channel, "payload"])
    assert prepared.channel_keys == [channel]
    assert prepared.acl_keys == []
    assert prepared.routing_scope == :none

    assert {:ok, prepared} = Dispatcher.prepare_raw("SSUBSCRIBE", [channel, "b"])
    assert prepared.channel_keys == [channel, "b"]
    assert prepared.acl_keys == []

    assert {:ok, prepared} = Dispatcher.prepare_raw("PUBSUB", ["SHARDNUMSUB", channel])
    assert prepared.channel_keys == [channel]
  end

  test "unsubscribing and exiting drop shard subscriptions", %{channel: channel} do
    pid = subscriber(fn pid -> PubSub.ssubscribe_many([channel], pid) end)
    assert 1 == PubSub.spublish(channel, "x")

    assert :ok == PubSub.sunsubscribe_many([channel], pid)
    assert 0 == PubSub.spublish(channel, "y")

    pid = subscriber(fn pid -> PubSub.ssubscribe_many([channel], pid) end)
    ref = Process.monitor(pid)
    send(pid, :stop)
    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}

    assert eventually(fn -> PubSub.shard_numsub([channel]) == [channel, 0] end)
    assert PubSub.shard_channels(channel) == []
  end

  test "cleanup drops shard subscriptions with the others", %{channel: channel} do
    pid = subscriber(fn pid -> PubSub.ssubscribe_many([channel], pid) end)
    subscribe_also(pid, fn -> PubSub.subscribe_many([channel], pid) end)

    assert :ok == PubSub.cleanup(pid)
    assert PubSub.shard_numsub([channel]) == [channel, 0]
    assert PubSub.numsub([channel]) == [channel, 0]
  end

  # A process that subscribes with `fun` and forwards what it receives.
  defp subscriber(fun) do
    parent = self()

    pid =
      spawn_link(fn ->
        receive do
          :go -> :ok
        end

        forward(parent)
      end)

    :ok = fun.(pid)
    send(pid, :go)
    pid
  end

  defp subscribe_also(_pid, fun), do: :ok = fun.()

  defp forward(parent) do
    receive do
      :stop ->
        :ok

      message ->
        send(parent, {:got, self(), message})
        forward(parent)
    end
  end

  defp dispatch(command, args),
    do: Dispatcher.dispatch(command, args, FerricStore.Instance.get(:default))

  defp eventually(fun, attempts \\ 100)
  defp eventually(fun, 0), do: fun.()

  defp eventually(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end
end
