defmodule Ferricstore.Cluster.FeatureLevelClusterTest do
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag timeout: 180_000

  alias Ferricstore.Test.ClusterHelper

  @shards 2

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "OTP 25+ required for :peer module"
    end

    :ok
  end

  test "raising on one member activates the level on every member's shards" do
    nodes = ClusterHelper.start_cluster(3, shards: @shards)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
    [first | _] = nodes

    assert {:ok, %{active: 0}} =
             ClusterHelper.run(first.name, FerricStore, :cluster_feature_level, [])

    assert {:ok, %{active: 1, shards: [1, 1]}} =
             ClusterHelper.run(first.name, FerricStore, :raise_cluster_feature_level, [1])

    for node <- nodes do
      assert eventually(fn ->
               match?(
                 {:ok, %{active: 1}},
                 ClusterHelper.run(node.name, FerricStore, :cluster_feature_level, [])
               )
             end),
             "#{node.name} did not apply feature level 1"
    end
  end

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
