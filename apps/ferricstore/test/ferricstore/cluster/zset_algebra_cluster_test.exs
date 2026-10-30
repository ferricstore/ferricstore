defmodule Ferricstore.Cluster.ZSetAlgebraClusterTest do
  @moduledoc """
  A transaction's sorted-set algebra runs in every replica's state machine
  (from feature level 10), so every replica must store the same result.
  """
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag timeout: 300_000

  alias Ferricstore.Commands.{Dispatcher, PreparedCommand}
  alias Ferricstore.Store.{CompoundKey, Router}
  alias Ferricstore.Test.ClusterHelper
  alias Ferricstore.Transaction.Coordinator

  @z1 "{zalg-cluster}z1"
  @z2 "{zalg-cluster}z2"
  @union "{zalg-cluster}union"
  @diff "{zalg-cluster}diff"

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

  test "every replica stores the same result", %{nodes: [first, second | _] = nodes} do
    assert {:ok, %{active: 10}} =
             ClusterHelper.run(first.name, FerricStore, :raise_cluster_feature_level, [10])

    assert 2 == dispatch(first, "ZADD", [@z1, "1", "a", "2", "b"])
    assert 2 == dispatch(first, "ZADD", [@z2, "3", "b", "4", "c"])

    assert [3, 1] ==
             exec(first, [
               {"ZUNIONSTORE", [@union, "2", @z1, @z2, "WEIGHTS", "1", "0.5"]},
               {"ZINTERSTORE", [@z1, "2", @z1, @z2]}
             ])

    assert 2 == dispatch(second, "ZDIFFSTORE", [@diff, "2", @union, @z1])

    for node <- nodes do
      assert eventually(fn ->
               members(node, @union) == [{"a", "1.0"}, {"b", "3.5"}, {"c", "2.0"}] and
                 members(node, @z1) == [{"b", "5.0"}] and
                 members(node, @diff) == [{"a", "1.0"}, {"c", "2.0"}]
             end),
             "#{node.name}: #{inspect(Enum.map([@union, @z1, @diff], &members(node, &1)))}"
    end
  end

  defp members(node, key) do
    node.name
    |> ClusterHelper.run(Router, :compound_scan, [ctx(node), key, CompoundKey.zset_prefix(key)])
    |> Enum.sort()
  end

  defp exec(node, commands) do
    queue =
      Enum.map(commands, fn {command, args} ->
        {:ok, prepared} = ClusterHelper.run(node.name, PreparedCommand, :prepare, [command, args])
        prepared
      end)

    ClusterHelper.run(node.name, Coordinator, :execute, [queue, %{}, nil])
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
