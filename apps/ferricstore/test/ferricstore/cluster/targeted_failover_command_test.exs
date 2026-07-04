defmodule Ferricstore.Cluster.TargetedFailoverCommandTest do
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag timeout: 120_000

  alias Ferricstore.Test.ClusterHelper

  @shards 2

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "requires OTP 25+ for :peer"
    end

    nodes = ClusterHelper.start_cluster(3, shards: @shards, timeout: 60_000)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
    %{nodes: nodes}
  end

  test "CLUSTER.FAILOVER targets the key shard leader and preserves convergence", %{nodes: nodes} do
    assert :ok = ClusterHelper.wait_for_leaders(nodes, @shards, timeout: 60_000)

    key = "cluster-targeted-failover:#{System.unique_integer([:positive])}"
    shard = remote_shard_for(hd(nodes).name, key)
    old_leader = eventually_result(fn -> ClusterHelper.find_leader(nodes, shard) end)
    target = Enum.find(nodes, &(&1.name != old_leader))

    assert :ok = remote_router(old_leader, :put, [key, "before-failover", 0])
    eventually_value_on_nodes(nodes, key, "before-failover")

    assert :ok =
             remote_dispatch(
               target.name,
               "CLUSTER.FAILOVER",
               [Integer.to_string(shard), Atom.to_string(target.name)]
             )

    eventually(fn ->
      assert ClusterHelper.find_leader(nodes, shard) == target.name
    end)

    assert :ok = remote_router(target.name, :put, [key, "after-failover", 0])
    eventually_value_on_nodes(nodes, key, "after-failover")
  end

  defp remote_router(node_name, fun, args) do
    :ok = ClusterHelper.ensure_node_reachable(node_name, timeout: 5_000)
    ctx = :erpc.call(node_name, FerricStore.Instance, :get, [:default], 5_000)
    :erpc.call(node_name, Ferricstore.Store.Router, fun, [ctx | args], 30_000)
  end

  defp remote_dispatch(node_name, cmd, args) do
    :ok = ClusterHelper.ensure_node_reachable(node_name, timeout: 5_000)
    ctx = :erpc.call(node_name, FerricStore.Instance, :get, [:default], 5_000)
    :erpc.call(node_name, Ferricstore.Commands.Dispatcher, :dispatch, [cmd, args, ctx], 30_000)
  end

  defp remote_shard_for(node_name, key) do
    :ok = ClusterHelper.ensure_node_reachable(node_name, timeout: 5_000)
    ctx = :erpc.call(node_name, FerricStore.Instance, :get, [:default], 5_000)
    :erpc.call(node_name, Ferricstore.Store.Router, :shard_for, [ctx, key], 5_000)
  end

  defp eventually_value_on_nodes(nodes, key, expected) do
    eventually(fn ->
      Enum.each(nodes, fn node ->
        assert remote_router(node.name, :get, [key]) == expected
      end)
    end)
  end

  defp eventually(fun, attempts \\ 100, interval_ms \\ 250)

  defp eventually(fun, attempts, interval_ms) when attempts > 0 do
    fun.()
  rescue
    error in [ExUnit.AssertionError, RuntimeError] ->
      if attempts == 1 do
        reraise(error, __STACKTRACE__)
      else
        Process.sleep(interval_ms)
        eventually(fun, attempts - 1, interval_ms)
      end
  end

  defp eventually_result(fun, attempts \\ 100, interval_ms \\ 250)

  defp eventually_result(fun, attempts, interval_ms) when attempts > 0 do
    fun.()
  rescue
    error in [RuntimeError] ->
      if attempts == 1 do
        reraise(error, __STACKTRACE__)
      else
        Process.sleep(interval_ms)
        eventually_result(fun, attempts - 1, interval_ms)
      end
  end
end
