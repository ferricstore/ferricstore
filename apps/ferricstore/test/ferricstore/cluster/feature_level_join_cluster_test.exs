defmodule Ferricstore.Cluster.FeatureLevelJoinClusterTest do
  @moduledoc """
  Raising the feature level is the operator's call: a new node never raises
  its own level, so it cannot write level markers into a cluster it joins
  while its start-up is still under way.
  """

  use ExUnit.Case, async: false

  @moduletag :cluster

  alias Ferricstore.Cluster.FeatureLevel
  alias Ferricstore.Test.ClusterHelper

  @shards 2

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "OTP 25+ required for :peer module"
    end

    :ok
  end

  @tag timeout: 180_000
  test "a new node keeps its level and joining it leaves the cluster's level unchanged" do
    nodes = ClusterHelper.start_cluster(3, shards: @shards)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
    [%{name: node_a} | _] = nodes

    assert {:ok, %{shards: [0, 0]}} = level(node_a)

    node_d = ClusterHelper.start_node(shards: @shards)
    on_exit(fn -> ClusterHelper.stop_node(node_d) end)

    for _ <- 1..10 do
      assert {:ok, %{shards: [0, 0]}} = level(node_d)
      Process.sleep(100)
    end

    assert :ok = :erpc.call(node_a, Ferricstore.Cluster.Manager, :add_node, [node_d], 120_000)

    for _ <- 1..10 do
      assert {:ok, %{shards: [0, 0]}} = level(node_a)
      Process.sleep(200)
    end

    assert {:ok, %{shards: [0, 0]}} = level(node_d)
  end

  defp level(node) do
    ctx = :erpc.call(node, FerricStore.Instance, :get, [:default])
    :erpc.call(node, FeatureLevel, :status, [ctx])
  end
end
