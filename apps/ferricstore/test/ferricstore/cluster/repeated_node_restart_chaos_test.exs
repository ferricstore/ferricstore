defmodule Ferricstore.Cluster.RepeatedNodeRestartChaosTest do
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag :shard_kill
  @moduletag timeout: 240_000

  alias Ferricstore.Test.ClusterHelper

  @shards 2
  @cycles 3

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "requires OTP 25+ for :peer"
    end

    :ok
  end

  test "cluster survives repeated leader crash and same-version restart cycles" do
    nodes = ClusterHelper.start_cluster(3, shards: @shards, timeout: 60_000)
    {:ok, node_holder} = Agent.start_link(fn -> nodes end)

    try do
      assert_distinct_peer_processes!(nodes)
      assert :ok = ClusterHelper.wait_for_leaders(nodes, @shards, timeout: 60_000)

      baseline =
        for shard <- 0..(@shards - 1), into: %{} do
          key = key_for_shard(hd(nodes).name, shard, "cluster-restart-baseline")
          value = "baseline-#{shard}"
          assert :ok = remote_router(hd(nodes).name, :put, [key, value, 0])
          {key, value}
        end

      eventually_values_on_nodes(nodes, baseline)

      {_nodes, expected_values} =
        Enum.reduce(1..@cycles, {nodes, baseline}, fn cycle, {current_nodes, expected_values} ->
          shard = rem(cycle - 1, @shards)
          leader = ClusterHelper.find_leader(current_nodes, shard)
          leader_node = Enum.find(current_nodes, &(&1.name == leader))

          {killed, remaining} = ClusterHelper.kill_node(current_nodes, leader_node)
          assert :ok = ClusterHelper.wait_for_leaders(remaining, @shards, timeout: 60_000)

          writer = hd(remaining)
          key = key_for_shard(writer.name, shard, "cluster-restart-cycle-#{cycle}")
          value = "cycle-#{cycle}"

          assert :ok = remote_router(writer.name, :put, [key, value, 0])
          expected_values = Map.put(expected_values, key, value)
          eventually_values_on_nodes(remaining, expected_values)

          restarted =
            ClusterHelper.restart_node([killed | remaining], killed,
              shards: @shards,
              timeout: 60_000
            )

          current_nodes = [restarted | remaining]
          Agent.update(node_holder, fn _ -> current_nodes end)

          assert :ok = ClusterHelper.wait_for_leaders(current_nodes, @shards, timeout: 60_000)
          eventually_values_on_nodes(current_nodes, expected_values)

          {current_nodes, expected_values}
        end)

      active_nodes = Agent.get(node_holder, & &1)
      eventually_values_on_nodes(active_nodes, expected_values)
    after
      active_nodes = Agent.get(node_holder, & &1)
      ClusterHelper.stop_cluster(active_nodes)
      Agent.stop(node_holder)
    end
  end

  defp assert_distinct_peer_processes!(nodes) do
    local_pid = System.pid()
    peer_pids = Enum.map(nodes, &:erpc.call(&1.name, System, :pid, [], 5_000))

    assert Enum.uniq(peer_pids) == peer_pids
    refute local_pid in peer_pids
  end

  defp key_for_shard(node_name, shard, prefix) do
    Enum.find_value(1..10_000, fn index ->
      key = "#{prefix}:#{System.unique_integer([:positive])}:#{index}"
      if remote_shard_for(node_name, key) == shard, do: key
    end) || flunk("could not find key for shard #{shard}")
  end

  defp remote_router(node_name, fun, args) do
    :ok = ClusterHelper.ensure_node_reachable(node_name, timeout: 5_000)
    ctx = :erpc.call(node_name, FerricStore.Instance, :get, [:default], 5_000)
    :erpc.call(node_name, Ferricstore.Store.Router, fun, [ctx | args], 30_000)
  end

  defp remote_shard_for(node_name, key) do
    :ok = ClusterHelper.ensure_node_reachable(node_name, timeout: 5_000)
    ctx = :erpc.call(node_name, FerricStore.Instance, :get, [:default], 5_000)
    :erpc.call(node_name, Ferricstore.Store.Router, :shard_for, [ctx, key], 5_000)
  end

  defp eventually_values_on_nodes(nodes, expected_values) do
    eventually(fn ->
      Enum.each(nodes, fn node ->
        Enum.each(expected_values, fn {key, value} ->
          assert remote_router(node.name, :get, [key]) == value
        end)
      end)
    end)
  end

  defp eventually(fun, attempts \\ 120, interval_ms \\ 250)

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
end
