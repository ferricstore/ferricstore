defmodule Ferricstore.Raft.CommandBatching do
  @moduledoc false

  @type barrier_kind :: :tx_execute | :cross_shard_tx | :apply_context

  @spec barrier_kind(term()) :: barrier_kind() | nil
  def barrier_kind({:tx_execute, queue, _sandbox_namespace}) when is_list(queue),
    do: :tx_execute

  def barrier_kind({:tx_execute, queue, _sandbox_namespace, watched_keys})
      when is_list(queue) and is_map(watched_keys),
      do: :tx_execute

  def barrier_kind({:cross_shard_tx, shard_batches}) when is_list(shard_batches),
    do: :cross_shard_tx

  def barrier_kind({:ferricstore_apply_context_barrier, _encoded}), do: :apply_context

  def barrier_kind({:ferricstore_latency_trace, inner}) when is_tuple(inner),
    do: barrier_kind(inner)

  def barrier_kind({:ferricstore_apply_context, _encoded, inner}) when is_tuple(inner),
    do: barrier_kind(inner)

  def barrier_kind({:flow_policy_fence, _installs, inner}) when is_tuple(inner),
    do: barrier_kind(inner)

  def barrier_kind({:async, _origin, inner}) when is_tuple(inner), do: barrier_kind(inner)

  def barrier_kind({inner, %{hlc_ts: {_physical_ms, _logical}}}) when is_tuple(inner),
    do: barrier_kind(inner)

  def barrier_kind(_command), do: nil

  @spec batchable?(term()) :: boolean()
  def batchable?(command), do: barrier_kind(command) == nil
end
