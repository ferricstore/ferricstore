defmodule Ferricstore.Cluster.FargateReplacementTest do
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag timeout: 240_000

  alias Ferricstore.Test.ClusterHelper

  @shards 2

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "requires OTP 25+ for :peer"
    end

    :ok
  end

  test "a replacement with the same node identity and an empty disk catches up" do
    nodes = ClusterHelper.start_cluster(3, shards: @shards, timeout: 60_000)
    {:ok, node_holder} = Agent.start_link(fn -> nodes end)

    try do
      expected =
        for shard <- 0..(@shards - 1), into: %{} do
          key = key_for_shard(hd(nodes).name, shard, "fargate-before")
          value = "before-#{shard}"
          assert :ok = remote_router(hd(nodes).name, :put, [key, value, 0])
          {key, value}
        end

      target = List.last(nodes)
      {killed, survivors} = ClusterHelper.kill_node(nodes, target)
      assert :ok = ClusterHelper.wait_for_leaders(survivors, @shards, timeout: 60_000)

      expected =
        for shard <- 0..(@shards - 1), reduce: expected do
          acc ->
            key = key_for_shard(hd(survivors).name, shard, "fargate-during")
            value = "during-#{shard}"
            assert :ok = remote_router(hd(survivors).name, :put, [key, value, 0])
            Map.put(acc, key, value)
        end

      restarted =
        ClusterHelper.restart_node([killed | survivors], killed,
          shards: @shards,
          timeout: 60_000,
          fresh_data: true
        )

      active_nodes = [restarted | survivors]
      Agent.update(node_holder, fn _ -> active_nodes end)

      assert restarted.data_dir != killed.data_dir
      assert :ok = ClusterHelper.wait_for_leaders(active_nodes, @shards, timeout: 60_000)

      eventually(fn ->
        assert :erpc.call(
                 restarted.name,
                 Ferricstore.Cluster.Recovery,
                 :ready?,
                 [[timeout_ms: 5_000]],
                 10_000
               )

        Enum.each(expected, fn {key, value} ->
          assert remote_router(restarted.name, :get, [key]) == value
        end)
      end)
    after
      active_nodes = Agent.get(node_holder, & &1)
      ClusterHelper.stop_cluster(active_nodes)

      Enum.each(nodes, fn original ->
        File.rm_rf(original.data_dir)
      end)

      Agent.stop(node_holder)
    end
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
    ctx = :erpc.call(node_name, FerricStore.Instance, :get, [:default], 5_000)
    :erpc.call(node_name, Ferricstore.Store.Router, :shard_for, [ctx, key], 5_000)
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
