defmodule Ferricstore.Cluster.CrossNodeDecideWriteTest do
  @moduledoc """
  Commands that read a key and decide their write from it hold a key latch,
  but the latch is node-local: clients on other nodes are not serialized by
  it. Such a command has to make its decision where the write applies.
  """
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag timeout: 300_000

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.ClusterHelper

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "OTP 25+ required for :peer module"
    end

    :ok
  end

  setup do
    nodes = ClusterHelper.start_cluster(3, shards: 1)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
    [first | _rest] = nodes

    assert {:ok, %{active: 5}} =
             ClusterHelper.run(first.name, FerricStore, :raise_cluster_feature_level, [5])

    {:ok, nodes: nodes}
  end

  test "EXPIRE NX from every node has one winner", %{nodes: [first | _] = nodes} do
    for round <- 1..10 do
      string = "{decide}s#{round}"
      hash = "{decide}h#{round}"
      assert :ok == dispatch(first, "SET", [string, "v"])
      assert 1 == dispatch(first, "HSET", [hash, "f", "v"])

      for key <- [string, hash] do
        replies =
          race(nodes, fn node, client ->
            dispatch(node, "EXPIRE", [key, Integer.to_string(1000 + client), "NX"])
          end)

        assert Enum.count(replies, &(&1 == 1)) == 1, "#{key}: #{inspect(replies)}"
      end
    end
  end

  # Two clients on each node, all at once.
  defp race(nodes, fun) do
    for node <- nodes, client <- 1..2 do
      Task.async(fn -> fun.(node, client) end)
    end
    |> Task.await_many(120_000)
  end

  defp dispatch(node, command, args),
    do: ClusterHelper.run(node.name, Dispatcher, :dispatch, [command, args, ctx(node)])

  defp ctx(node), do: ClusterHelper.run(node.name, FerricStore.Instance, :get, [:default])
end
