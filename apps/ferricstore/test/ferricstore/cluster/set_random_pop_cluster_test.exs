defmodule Ferricstore.Cluster.SetRandomPopClusterTest do
  @moduledoc """
  SPOP's member comes from the entry's Raft index, so every replica removes
  the same one: their sets stay equal while the pops spread over the set.
  """
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag timeout: 300_000

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.ClusterHelper

  @set "{spop-cluster}set"

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "OTP 25+ required for :peer module"
    end

    :ok
  end

  setup do
    nodes = ClusterHelper.start_cluster(3, shards: 1)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
    {:ok, nodes: nodes}
  end

  test "every replica removes the same members", %{nodes: [first | _] = nodes} do
    assert {:ok, %{active: 11}} =
             ClusterHelper.run(first.name, FerricStore, :raise_cluster_feature_level, [11])

    assert 200 == dispatch(first, "SADD", [@set | for(i <- 1..200, do: "m#{i}")])

    popped =
      for round <- 1..30 do
        node = Enum.at(nodes, rem(round, 3))
        dispatch(node, "SPOP", [@set])
      end

    popped = popped ++ dispatch(first, "SPOP", [@set, "20"])
    assert 50 == popped |> Enum.uniq() |> length()

    expected = for(i <- 1..200, do: "m#{i}") -- popped

    for node <- nodes do
      assert eventually(fn ->
               Enum.sort(dispatch(node, "SMEMBERS", [@set])) == Enum.sort(expected)
             end),
             "#{node.name}: #{dispatch(node, "SCARD", [@set])} members"
    end
  end

  defp dispatch(node, command, args),
    do: ClusterHelper.run(node.name, Dispatcher, :dispatch, [command, args, ctx(node)])

  defp ctx(node), do: ClusterHelper.run(node.name, FerricStore.Instance, :get, [:default])

  defp eventually(fun, attempts \\ 100)
  defp eventually(fun, 0), do: fun.()

  defp eventually(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(100)
      eventually(fun, attempts - 1)
    end
  end
end
