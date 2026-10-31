defmodule Ferricstore.Cluster.PubSubRelayClusterTest do
  @moduledoc """
  Subscriptions live in each node's registry, so a PUBLISH through one node
  reached only that node's subscribers. A publish now reaches subscribers on
  every member, once each, and answers the count on the node that ran it, as
  Redis Cluster does.
  """
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag timeout: 300_000

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.PubSub
  alias Ferricstore.Test.ClusterHelper

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "OTP 25+ required for :peer module"
    end

    :ok
  end

  setup do
    nodes = ClusterHelper.start_cluster(3, shards: 2)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
    for node <- nodes, do: await_peers(node, 2, 100)
    {:ok, nodes: nodes}
  end

  test "PUBLISH reaches subscribers on every node once", %{nodes: [first, second, third]} do
    channel = "relay:#{System.unique_integer([:positive])}"

    for node <- [first, second, third] do
      assert :ok == ClusterHelper.run(node.name, PubSub, :subscribe, [channel, self()])
    end

    assert :ok == ClusterHelper.run(third.name, PubSub, :psubscribe, ["relay:*", self()])

    assert 1 == dispatch(first, "PUBLISH", [channel, "one"])
    assert 2 == dispatch(third, "PUBLISH", [channel, "two"])

    assert [{:pubsub_message, ^channel, "one"}] ++ _rest = received_for("one", 4)
    assert 3 == length(Enum.filter(received_for("two", 4), &match?({:pubsub_message, _, _}, &1)))

    # Batched publishes (pipelined SDK PUBLISH) go the same way, in order.
    assert [1, 1] ==
             ClusterHelper.run(first.name, PubSub, :publish_many, [
               [{channel, "b1"}, {channel, "b2"}]
             ])

    messages = collect(8)
    exact = for {:pubsub_message, ^channel, message} <- messages, do: message
    patterned = for {:pubsub_pmessage, "relay:*", ^channel, message} <- messages, do: message
    assert Enum.frequencies(exact) == %{"b1" => 3, "b2" => 3}
    assert patterned == ["b1", "b2"]
    refute_receive _more, 300
  end

  test "SPUBLISH reaches shard subscribers on every node, and only them", %{
    nodes: [first, second, third]
  } do
    channel = "shard-relay:#{System.unique_integer([:positive])}"

    for node <- [second, third] do
      assert :ok == ClusterHelper.run(node.name, PubSub, :ssubscribe_many, [[channel], self()])
    end

    # Ordinary subscribers to the same name, and a pattern, on every node.
    for node <- [first, second, third] do
      assert :ok == ClusterHelper.run(node.name, PubSub, :subscribe, [channel, self()])
      assert :ok == ClusterHelper.run(node.name, PubSub, :psubscribe, ["shard-relay:*", self()])
    end

    assert 0 == dispatch(first, "SPUBLISH", [channel, "s1"])
    assert 1 == dispatch(third, "SPUBLISH", [channel, "s2"])

    messages = collect(4)

    assert Enum.frequencies(for {:pubsub_smessage, ^channel, message} <- messages, do: message) ==
             %{"s1" => 2, "s2" => 2}

    refute_receive _more, 300

    # Each node counts its own ordinary subscriber, and not the shard one.
    for node <- [first, second, third] do
      assert [^channel, 1] = dispatch(node, "PUBSUB", ["NUMSUB", channel])
    end

    assert [^channel] = dispatch(second, "PUBSUB", ["SHARDCHANNELS", "shard-relay:*"])
    assert [^channel, 1] = dispatch(second, "PUBSUB", ["SHARDNUMSUB", channel])
    assert [] = dispatch(first, "PUBSUB", ["SHARDCHANNELS", "shard-relay:*"])
  end

  # Collects the messages that carry `payload`, expecting `count` of them.
  defp received_for(payload, count) do
    messages =
      for _ <- 1..count do
        receive do
          {:pubsub_message, _channel, ^payload} = message -> message
          {:pubsub_pmessage, _pattern, _channel, ^payload} = message -> message
        after
          5_000 -> flunk("missing a delivery of #{payload}")
        end
      end

    Enum.sort_by(messages, &tuple_size/1)
  end

  defp collect(count) do
    for _ <- 1..count do
      receive do
        message -> message
      after
        5_000 -> flunk("missing deliveries")
      end
    end
  end

  defp await_peers(node, count, attempts) do
    peers = ClusterHelper.run(node.name, Ferricstore.Cluster.Relay, :peers, [])

    cond do
      length(peers) == count ->
        :ok

      attempts > 0 ->
        Process.sleep(50)
        await_peers(node, count, attempts - 1)

      true ->
        flunk("#{node.name} relays to #{inspect(peers)}")
    end
  end

  defp dispatch(node, command, args),
    do: ClusterHelper.run(node.name, Dispatcher, :dispatch, [command, args, ctx(node)])

  defp ctx(node), do: ClusterHelper.run(node.name, FerricStore.Instance, :get, [:default])
end
