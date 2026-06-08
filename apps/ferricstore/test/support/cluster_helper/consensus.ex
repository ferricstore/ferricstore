defmodule Ferricstore.Test.ClusterHelper.Consensus do
  @moduledoc false

  @spec stop_consensus(atom()) :: :ok | term()
  def stop_consensus(node_name) do
    :rpc.call(node_name, Ferricstore.Raft.WARaftBackend, :stop, [])
  end

  @spec start_consensus(atom()) :: :ok | term()
  def start_consensus(node_name) do
    case :rpc.call(node_name, FerricStore.Instance, :get, [:default]) do
      %FerricStore.Instance{} = ctx ->
        :rpc.call(node_name, Ferricstore.Raft.WARaftBackend, :start, [ctx, []])

      other ->
        {:error, {:default_instance_unavailable, other}}
    end
  end

  @spec wait_for_leaders([map()], pos_integer() | Range.t(), keyword()) ::
          :ok | {:error, :timeout_waiting_for_leaders}
  def wait_for_leaders(nodes, shards, opts \\ [])

  def wait_for_leaders(nodes, shards, opts) when is_integer(shards) do
    wait_for_leaders(nodes, 0..(shards - 1), opts)
  end

  def wait_for_leaders(nodes, shard_range, opts) do
    timeout = Keyword.get(opts, :timeout, 15_000)
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_leaders(nodes, shard_range, deadline)
  end

  @spec wait_for_node_leaders(atom(), pos_integer(), keyword()) ::
          :ok | {:error, :timeout_waiting_for_leaders}
  def wait_for_node_leaders(node_name, shards, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 15_000)
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_node_leaders(node_name, 0..(shards - 1), deadline)
  end

  def members_on_node(node_name, shard, timeout \\ :default)

  def members_on_node(node_name, shard, :default) do
    :rpc.call(node_name, Ferricstore.Raft.Cluster, :members, [shard])
  end

  def members_on_node(node_name, shard, timeout) do
    :rpc.call(node_name, Ferricstore.Raft.Cluster, :members, [shard, timeout])
  end

  defp do_wait_leaders(nodes, shard_range, deadline) do
    alive_names = MapSet.new(nodes, & &1.name)

    all_have_leaders =
      Enum.all?(shard_range, fn shard ->
        Enum.any?(nodes, fn node ->
          case members_on_node(node.name, shard) do
            {:ok, _members, {_shard_name, leader_node}} ->
              MapSet.member?(alive_names, leader_node)

            _ ->
              false
          end
        end)
      end)

    cond do
      all_have_leaders ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        {:error, :timeout_waiting_for_leaders}

      true ->
        Process.sleep(100)
        do_wait_leaders(nodes, shard_range, deadline)
    end
  end

  defp do_wait_node_leaders(node_name, shard_range, deadline) do
    all_ready =
      Enum.all?(shard_range, fn shard ->
        case members_on_node(node_name, shard) do
          {:ok, _members, _leader} -> true
          _ -> false
        end
      end)

    cond do
      all_ready ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        {:error, :timeout_waiting_for_leaders}

      true ->
        Process.sleep(100)
        do_wait_node_leaders(node_name, shard_range, deadline)
    end
  end
end
