defmodule Ferricstore.Test.ClusterHelper.Partition do
  @moduledoc false

  alias Ferricstore.Test.ClusterHelper.Consensus

  @spec partition_node(map(), [map()]) :: :ok
  def partition_node(node, all_nodes) do
    others = Enum.reject(all_nodes, &(&1.name == node.name))

    Enum.each(all_nodes, fn n ->
      cm_pid = :rpc.call(n.name, Process, :whereis, [Ferricstore.Cluster.Manager])
      if is_pid(cm_pid), do: :rpc.call(n.name, :sys, :suspend, [cm_pid])
    end)

    Consensus.stop_consensus(node.name)

    cookie_state =
      Enum.flat_map(others, fn other ->
        node_to_other = :rpc.call(node.name, :erlang, :get_cookie, [other.name])
        other_to_node = :rpc.call(other.name, :erlang, :get_cookie, [node.name])
        nonce = :erlang.unique_integer([:positive])
        blocked_from_node = :"partitioned_node_blocked_#{nonce}_a"
        blocked_from_other = :"partitioned_node_blocked_#{nonce}_b"

        :rpc.call(node.name, :erlang, :set_cookie, [other.name, blocked_from_node])
        :rpc.call(other.name, :erlang, :set_cookie, [node.name, blocked_from_other])

        [
          {node.name, other.name, node_to_other},
          {other.name, node.name, other_to_node}
        ]
      end)

    Ferricstore.Test.ShardHelpers.eventually(
      fn ->
        Enum.each(others, fn other ->
          :rpc.call(node.name, :erlang, :disconnect_node, [other.name])
          :rpc.call(other.name, :erlang, :disconnect_node, [node.name])
        end)

        Process.sleep(100)
        assert_partition_disconnected!(node, others)
      end,
      "partition should disconnect #{node.name}",
      50,
      100
    )

    Process.put({:partition_cookies, node.name}, cookie_state)

    shards = :rpc.call(hd(others).name, Application, :get_env, [:ferricstore, :shard_count, 4])
    Consensus.wait_for_leaders(others, shards, timeout: 10_000)

    :ok
  end

  @spec heal_partition(map(), [map()]) :: :ok
  def heal_partition(node, all_nodes) do
    others = Enum.reject(all_nodes, &(&1.name == node.name))

    node.name
    |> restored_partition_cookies()
    |> Enum.each(fn {from, to, cookie} ->
      :rpc.call(from, :erlang, :set_cookie, [to, cookie])
    end)

    normalize_cluster_cookies(all_nodes)

    connect = fn from, to ->
      try do
        :erpc.call(from, Node, :connect, [to], 2_000)
      catch
        _, _ -> false
      end
    end

    Ferricstore.Test.ShardHelpers.eventually(
      fn ->
        Enum.each(others, fn other ->
          connect.(other.name, node.name)
          connect.(node.name, other.name)
        end)

        Process.sleep(200)
        other_names = Enum.map(others, & &1.name)

        node_peers =
          case :rpc.call(node.name, :erlang, :nodes, [], 2_000) do
            peers when is_list(peers) -> MapSet.new(peers)
            _ -> MapSet.new()
          end

        peers_see_node? =
          Enum.all?(others, fn other ->
            case :rpc.call(other.name, :erlang, :nodes, [], 2_000) do
              peers when is_list(peers) -> node.name in peers
              _ -> false
            end
          end)

        unless Enum.all?(other_names, &MapSet.member?(node_peers, &1)) and peers_see_node? do
          seen = Enum.count(other_names, &MapSet.member?(node_peers, &1))
          raise "#{node.name} sees #{seen}/#{length(others)} peers"
        end

        true
      end,
      "heal should reconnect #{node.name}",
      40,
      500
    )

    Consensus.start_consensus(node.name)

    Enum.each(all_nodes, fn n ->
      cm_pid = :rpc.call(n.name, Process, :whereis, [Ferricstore.Cluster.Manager])
      if is_pid(cm_pid), do: :rpc.call(n.name, :sys, :resume, [cm_pid])
    end)

    :ok
  end

  defp assert_partition_disconnected!(node, others) do
    other_names = Enum.map(others, & &1.name)

    node_peers =
      case :rpc.call(node.name, :erlang, :nodes, [], 2_000) do
        peers when is_list(peers) -> peers
        other -> raise "#{node.name} peer list unavailable: #{inspect(other)}"
      end

    if Enum.any?(other_names, &(&1 in node_peers)) do
      raise "#{node.name} still sees partition peers #{inspect(node_peers)}"
    end

    Enum.each(others, fn other ->
      peers =
        case :rpc.call(other.name, :erlang, :nodes, [], 2_000) do
          peers when is_list(peers) -> peers
          value -> raise "#{other.name} peer list unavailable: #{inspect(value)}"
        end

      if node.name in peers do
        raise "#{other.name} still sees partitioned node #{node.name}"
      end
    end)
  end

  defp restored_partition_cookies(node_name) do
    case Process.get({:partition_cookies, node_name}) do
      cookies when is_list(cookies) ->
        cookies

      _ ->
        case Process.get({:partition_cookie, node_name}) do
          cookie when is_atom(cookie) -> [{node_name, node_name, cookie}]
          _ -> []
        end
    end
  end

  def normalize_cluster_cookies(nodes) do
    cookie = Node.get_cookie()

    Enum.each(nodes, fn from ->
      Enum.each(nodes, fn to ->
        :rpc.call(from.name, :erlang, :set_cookie, [to.name, cookie])
      end)
    end)
  end
end
