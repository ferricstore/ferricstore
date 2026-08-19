defmodule Ferricstore.Cluster.Recovery do
  @moduledoc """
  Reports whether the local cluster replica has caught up with every shard
  leader.

  This is intentionally stricter than ordinary HTTP readiness. It is used by
  rolling infrastructure deployments before another replica is stopped, so a
  freshly replaced node with an empty local disk is not mistaken for a fully
  recovered replica merely because it has connected and found a leader.
  """

  alias Ferricstore.Raft.Cluster
  alias Ferricstore.Raft.WARaftBackend

  @default_timeout_ms 2_000
  @default_max_lag_entries 10

  @type shard_status :: %{
          required(:ready) => boolean(),
          optional(:leader) => node(),
          optional(:local_index) => non_neg_integer(),
          optional(:leader_index) => non_neg_integer(),
          optional(:lag_entries) => non_neg_integer(),
          optional(:reason) => term()
        }

  @doc "Returns true when this node is connected and caught up on every shard."
  @spec ready?(keyword()) :: boolean()
  def ready?(opts \\ []) do
    status(opts).ready
  end

  @doc "Returns local recovery details suitable for deployment diagnostics."
  @spec status(keyword()) :: %{ready: boolean(), connected_nodes: [node()], shards: map()}
  def status(opts \\ []) do
    timeout_ms = Keyword.get(opts, :timeout_ms, @default_timeout_ms)
    max_lag_entries = Keyword.get(opts, :max_lag_entries, @default_max_lag_entries)
    required_nodes = configured_nodes()
    connected_nodes = [node() | Node.list()]
    mesh_ready? = Enum.all?(required_nodes, &(&1 in connected_nodes))

    shards =
      shard_indexes()
      |> Task.async_stream(
        &shard_status(&1, length(required_nodes), max_lag_entries, timeout_ms),
        ordered: true,
        timeout: timeout_ms * 2 + 500,
        on_timeout: :kill_task
      )
      |> Enum.zip(shard_indexes())
      |> Map.new(fn
        {{:ok, result}, shard_index} -> {shard_index, result}
        {{:exit, reason}, shard_index} -> {shard_index, %{ready: false, reason: reason}}
      end)

    %{
      ready: mesh_ready? and map_size(shards) > 0 and Enum.all?(shards, &elem(&1, 1).ready),
      connected_nodes: connected_nodes,
      shards: shards
    }
  end

  defp shard_status(shard_index, required_members, max_lag_entries, timeout_ms) do
    with {:ok, members, {_server, leader_node}} <- Cluster.members(shard_index, timeout_ms),
         true <- node() in Enum.map(members, &elem(&1, 1)),
         true <- length(members) >= required_members,
         {:ok, {:raft_log_pos, local_index, _local_term}} <-
           WARaftBackend.storage_position(shard_index),
         {:ok, {:raft_log_pos, leader_index, _leader_term}} <-
           leader_storage_position(leader_node, shard_index, timeout_ms) do
      lag_entries = max(leader_index - local_index, 0)

      %{
        ready: lag_entries <= max_lag_entries,
        leader: leader_node,
        local_index: local_index,
        leader_index: leader_index,
        lag_entries: lag_entries
      }
    else
      false -> %{ready: false, reason: :incomplete_cluster_membership}
      {:error, reason} -> %{ready: false, reason: reason}
      other -> %{ready: false, reason: other}
    end
  catch
    kind, reason -> %{ready: false, reason: {kind, reason}}
  end

  defp leader_storage_position(leader_node, shard_index, _timeout_ms)
       when leader_node == node(),
       do: WARaftBackend.storage_position(shard_index)

  defp leader_storage_position(leader_node, shard_index, timeout_ms) do
    :erpc.call(leader_node, WARaftBackend, :storage_position, [shard_index], timeout_ms)
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp configured_nodes do
    case Application.get_env(:ferricstore, :cluster_nodes, []) do
      nodes when is_list(nodes) and nodes != [] -> Enum.uniq(nodes)
      _other -> [node()]
    end
  end

  defp shard_indexes do
    case :persistent_term.get(:ferricstore_shard_count, :undefined) do
      count when is_integer(count) and count > 0 -> Enum.to_list(0..(count - 1))
      _other -> []
    end
  end
end
