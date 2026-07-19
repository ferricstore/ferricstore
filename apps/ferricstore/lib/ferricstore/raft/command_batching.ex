defmodule Ferricstore.Raft.CommandBatching do
  @moduledoc false

  @type barrier_kind :: :tx_execute | :cross_shard_tx | :apply_context

  @direct_only_tags [
    :batch,
    :clear_key_locks,
    :delete_batch,
    :expire_if_batch,
    :fetch_or_compute_fail,
    :fetch_or_compute_lock,
    :fetch_or_compute_publish,
    :fetch_or_compute_publish_blob_ref,
    :fetch_or_compute_release,
    :flow_governance_limit_catalog_outbox_ack,
    :flow_governance_release_outbox_ack,
    :flow_governance_release_outbox_mark_completed,
    :flow_policy_attribute_catalog_repair,
    :flow_policy_attribute_catalog_repair_request,
    :flow_policy_catalog_backfill_step,
    :flow_policy_migration_step,
    :flow_policy_put,
    :flow_policy_patch_allocate,
    :flush_shard,
    :key_lifecycle,
    :put_batch,
    :put_blob_batch,
    :server_catalog_mutate,
    :server_catalog_replace,
    :ttb,
    :zadd_many_single
  ]

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

  def barrier_kind({inner, %{hlc_ts: {_physical_ms, _logical}, wall_time_ms: wall_time_ms}})
      when is_tuple(inner) and is_integer(wall_time_ms),
      do: barrier_kind(inner)

  def barrier_kind(_command), do: nil

  @spec batchable?(term()) :: boolean()
  def batchable?(command), do: barrier_kind(command) == nil and coalescible_shape?(command)

  defp coalescible_shape?(command)
       when is_tuple(command) and tuple_size(command) > 0 and
              elem(command, 0) in @direct_only_tags,
       do: false

  defp coalescible_shape?({:ferricstore_latency_trace, inner}) when is_tuple(inner),
    do: coalescible_shape?(inner)

  defp coalescible_shape?({:ferricstore_apply_context, _encoded, inner}) when is_tuple(inner),
    do: coalescible_shape?(inner)

  defp coalescible_shape?({:flow_policy_fence, _installs, inner}) when is_tuple(inner),
    do: coalescible_shape?(inner)

  defp coalescible_shape?({:flow_shared_ref_write, _shard_index, inner}) when is_tuple(inner),
    do: coalescible_shape?(inner)

  defp coalescible_shape?({:async, _origin, inner}) when is_tuple(inner),
    do: coalescible_shape?(inner)

  defp coalescible_shape?(
         {:origin_checked, _key, inner, _before_value, _before_expire_at_ms, _expected_value,
          _expire_at_ms}
       )
       when is_tuple(inner),
       do: coalescible_shape?(inner)

  defp coalescible_shape?({:origin_checked, _key, inner, _expected_value, _expire_at_ms})
       when is_tuple(inner),
       do: coalescible_shape?(inner)

  defp coalescible_shape?({inner, metadata}) when is_tuple(inner) and is_map(metadata),
    do: coalescible_shape?(inner)

  defp coalescible_shape?(_command), do: true
end
