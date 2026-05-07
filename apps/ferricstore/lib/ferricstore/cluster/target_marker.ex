defmodule Ferricstore.Cluster.TargetMarker do
  @moduledoc false

  @type rpc_fun :: (node(), module(), atom(), list(), timeout() -> term())

  @spec write(node(), map(), map(), keyword()) :: :ok | {:error, term()}
  def write(target_node, ctx, barrier_indices, opts \\ []) do
    rpc = Keyword.get(opts, :rpc, &:erpc.call/5)

    case Ferricstore.ReplicationMode.read(ctx.data_dir) do
      {:ok, %{cluster_id: cluster_id}} when is_binary(cluster_id) ->
        write_remote_marker(target_node, ctx, barrier_indices, cluster_id, rpc)

      {:ok, _} ->
        {:error, :local_cluster_id_missing}

      {:error, reason} ->
        {:error, {:local_cluster_state_unreadable, reason}}
    end
  end

  defp write_remote_marker(target_node, ctx, barrier_indices, cluster_id, rpc) do
    with {:ok, target_ctx} <- safe_rpc(rpc, target_node, FerricStore.Instance, :get, [:default]),
         {:ok, target_data_dir} <- target_data_dir(target_ctx),
         {:ok, :ok} <-
           safe_rpc(
             rpc,
             target_node,
             Ferricstore.ReplicationMode,
             :write!,
             [
               target_data_dir,
               %{
                 replication_mode: :raft,
                 shard_count: ctx.shard_count,
                 cluster_id: cluster_id,
                 barrier_indices: barrier_indices
               }
             ]
           ) do
      :ok
    else
      {:ok, other} ->
        {:error, {:target_cluster_marker_write_failed, target_node, other}}

      {:error, reason} ->
        {:error, {:target_cluster_marker_write_failed, target_node, reason}}
    end
  end

  defp target_data_dir(%{data_dir: data_dir}) when is_binary(data_dir), do: {:ok, data_dir}
  defp target_data_dir(other), do: {:error, {:invalid_target_context, other}}

  defp safe_rpc(rpc, target_node, module, function, args) do
    {:ok, rpc.(target_node, module, function, args, 5_000)}
  catch
    kind, reason -> {:error, {kind, reason}}
  end
end
