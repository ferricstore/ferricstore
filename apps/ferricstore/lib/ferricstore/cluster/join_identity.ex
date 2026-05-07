defmodule Ferricstore.Cluster.JoinIdentity do
  @moduledoc false

  @type marker_read :: {:ok, map()} | {:error, term()}

  @spec validate(marker_read(), marker_read(), node()) :: :ok | {:error, term()}
  def validate(local_state, target_state, target_node) do
    case {local_state, target_state} do
      {{:ok, %{cluster_id: cluster_id}}, {:ok, %{cluster_id: cluster_id}}}
      when is_binary(cluster_id) ->
        :ok

      {{:error, :enoent}, _} ->
        :ok

      {{:ok, _}, {:error, :enoent}} ->
        {:error, {:target_cluster_state_missing, target_node}}

      {{:ok, %{cluster_id: local}}, {:ok, %{cluster_id: remote}}} ->
        {:error, {:target_cluster_id_mismatch, target_node, local, remote}}

      {{:error, reason}, _} ->
        {:error, {:local_cluster_state_unreadable, reason}}

      {_, {:error, reason}} ->
        {:error, {:target_cluster_state_unreadable, target_node, reason}}
    end
  end
end
