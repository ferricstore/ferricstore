defmodule Ferricstore.Cluster.JoinIdentity do
  @moduledoc false

  @type marker_read :: {:ok, map()} | {:error, term()}

  @spec validate(marker_read(), marker_read(), node()) :: :ok | {:error, term()}
  def validate(local_state, target_state, target_node) do
    case {local_state, target_state} do
      {{:ok, local}, _}
      when not is_map(local) or not is_map_key(local, :cluster_id) ->
        {:error, {:local_cluster_state_invalid, local}}

      {{:ok, %{cluster_id: cluster_id} = local}, _} when not is_binary(cluster_id) ->
        {:error, {:local_cluster_state_invalid, local}}

      {_, {:ok, target}}
      when not is_map(target) or not is_map_key(target, :cluster_id) ->
        {:error, {:target_cluster_state_invalid, target_node, target}}

      {_, {:ok, %{cluster_id: cluster_id} = target}} when not is_binary(cluster_id) ->
        {:error, {:target_cluster_state_invalid, target_node, target}}

      {{:ok, %{cluster_id: cluster_id}},
       {:ok, %{cluster_id: cluster_id, replication_mode: :raft}}}
      when is_binary(cluster_id) ->
        :ok

      {{:error, :enoent}, {:error, :enoent}} ->
        {:error, {:local_cluster_state_missing, target_node}}

      {{:error, :enoent}, {:ok, _}} ->
        {:error, {:local_cluster_state_missing, target_node}}

      {{:ok, _}, {:error, :enoent}} ->
        {:error, {:target_cluster_state_missing, target_node}}

      {{:ok, %{cluster_id: cluster_id}}, {:ok, %{cluster_id: cluster_id}}}
      when is_binary(cluster_id) ->
        {:error, {:target_cluster_state_unusable, target_node, :missing_replication_mode}}

      {{:ok, %{cluster_id: local}}, {:ok, %{cluster_id: remote}}} ->
        {:error, {:target_cluster_id_mismatch, target_node, local, remote}}

      {{:error, reason}, _} ->
        {:error, {:local_cluster_state_unreadable, reason}}

      {_, {:error, reason}} ->
        {:error, {:target_cluster_state_unreadable, target_node, reason}}
    end
  end
end
