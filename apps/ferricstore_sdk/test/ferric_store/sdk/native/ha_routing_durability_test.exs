defmodule FerricStore.SDK.Native.HARoutingDurabilityTest do
  use ExUnit.Case, async: false

  @moduletag :shard_kill
  @moduletag timeout: 180_000

  alias FerricStore.SDK.Native.Client
  alias Ferricstore.Test.ClusterHelper

  @shards 2

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "requires OTP 25+ for :peer"
    end

    :ok
  end

  test "SDK exposes unknown leader-fall writes and recovers after user refresh" do
    nodes = ClusterHelper.start_cluster(3, shards: @shards)

    try do
      seeds = start_native_servers(nodes)
      {:ok, client} = start_client(seeds)

      try do
        key = "{sdk-ha-leader-fall}:#{System.unique_integer([:positive])}"
        assert :ok = Client.set(client, key, "before-leader-fall", timeout: 10_000)
        assert {:ok, before_route} = Client.route(client, key)
        assert before_route.endpoint.host == "127.0.0.1"

        eventually_value_on_nodes(nodes, key, "before-leader-fall")

        killed = node_by_name!(nodes, before_route.leader_node)
        {_killed, remaining} = ClusterHelper.kill_node(nodes, killed)
        assert :ok = ClusterHelper.wait_for_leaders(remaining, @shards, timeout: 60_000)

        case Client.set(client, key, "after-leader-fall", timeout: 15_000) do
          :ok ->
            :ok

          {:error, :closed} ->
            eventually_value_on_nodes(remaining, key, "before-leader-fall")
            assert :ok = Client.refresh_topology(client)
            assert :ok = Client.set(client, key, "after-leader-fall", timeout: 15_000)
        end

        assert {:ok, "after-leader-fall"} = Client.get(client, key, timeout: 10_000)

        assert {:ok, after_route} = Client.route(client, key)
        refute after_route.leader_node == before_route.leader_node
        assert node_by_name!(remaining, after_route.leader_node)

        eventually_value_on_nodes(remaining, key, "after-leader-fall")
      after
        close_client(client)
      end
    after
      ClusterHelper.stop_cluster(nodes)
    end
  end

  test "SDK surfaces no-quorum write failure and preserves the last durable value" do
    nodes = ClusterHelper.start_cluster(3, shards: @shards)

    try do
      seeds = start_native_servers(nodes)
      {:ok, client} = start_client(seeds)

      try do
        key = "{sdk-ha-no-quorum}:#{System.unique_integer([:positive])}"
        assert :ok = Client.set(client, key, "before-quorum-loss", timeout: 10_000)
        assert {:ok, route} = Client.route(client, key)

        eventually_value_on_nodes(nodes, key, "before-quorum-loss")

        leader = node_by_name!(nodes, route.leader_node)
        second_victim = Enum.find(nodes, &(&1.name != leader.name))

        {_first, remaining_after_first} = ClusterHelper.kill_node(nodes, leader)
        {_second, [survivor]} = ClusterHelper.kill_node(remaining_after_first, second_victim)

        assert {:error, reason} = Client.set(client, key, "uncommitted", timeout: 2_500)
        assert reason != nil
        assert remote_get(survivor.name, key) == "before-quorum-loss"
      after
        close_client(client)
      end
    after
      ClusterHelper.stop_cluster(nodes)
    end
  end

  defp start_client(seeds) do
    Client.start_link(
      seeds: seeds,
      trusted_hosts: ["127.0.0.1"],
      warm_connections: true,
      connect_timeout: 1_000
    )
  end

  defp start_native_servers(nodes) do
    Enum.map(nodes, fn node ->
      :ok =
        :rpc.call(node.name, Application, :put_env, [
          :ferricstore,
          :native_advertise_host,
          "127.0.0.1"
        ])

      {:ok, _apps} =
        :rpc.call(node.name, Application, :ensure_all_started, [:ferricstore_server], 120_000)

      port = :rpc.call(node.name, FerricstoreServer.Native.Listener, :port, [], 10_000)
      {"127.0.0.1", port}
    end)
  end

  defp node_by_name!(nodes, node_name) when is_binary(node_name) do
    Enum.find(nodes, &(Atom.to_string(&1.name) == node_name)) ||
      flunk("node #{inspect(node_name)} not found in #{inspect(Enum.map(nodes, & &1.name))}")
  end

  defp eventually_value_on_nodes(nodes, key, expected) do
    eventually(fn ->
      Enum.each(nodes, fn node ->
        assert remote_get(node.name, key) == expected
      end)
    end)
  end

  defp remote_get(node_name, key) do
    ctx = :erpc.call(node_name, FerricStore.Instance, :get, [:default], 10_000)
    :erpc.call(node_name, Ferricstore.Store.Router, :get, [ctx, key], 30_000)
  end

  defp eventually(fun, attempts \\ 120, interval_ms \\ 250)

  defp eventually(fun, attempts, interval_ms) when attempts > 0 do
    fun.()
  rescue
    error in [ExUnit.AssertionError] ->
      if attempts == 1 do
        reraise(error, __STACKTRACE__)
      else
        Process.sleep(interval_ms)
        eventually(fun, attempts - 1, interval_ms)
      end
  end

  defp close_client(nil), do: :ok

  defp close_client(client) when is_pid(client) do
    if Process.alive?(client), do: Client.close(client)
    :ok
  catch
    :exit, _ -> :ok
  end
end
