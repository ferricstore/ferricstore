defmodule Ferricstore.Raft.ApplyFailure do
  @moduledoc false

  @rollback_failure_tags MapSet.new(~w(
                             compaction_hot_rollback_failed
                             compaction_rollback_failed
                             compound_batch_mutate_rollback_failed
                             compound_clear_rollback_failed
                             geo_zset_type_marker_rollback_failed
                             hash_delete_rollback_failed
                             hash_type_marker_rollback_failed
                             list_delete_rollback_failed
                             list_element_rollback_failed
                             list_pop_rollback_failed
                             list_type_marker_rollback_failed
                             lmove_source_rollback_failed
                             mset_rollback_failed
                             set_delete_rollback_failed
                             set_type_marker_rollback_failed
                             smove_cleanup_rollback_failed
                             smove_rollback_failed
                             stream_group_metadata_rollback_failed
                             stream_metadata_rollback_failed
                             stream_type_marker_rollback_failed
                             zset_delete_rollback_failed
                             zset_type_marker_rollback_failed
                           )a)

  @spec storage_result?(term()) :: boolean()
  def storage_result?({:error, reason}), do: storage_reason?(reason)
  def storage_result?(_result), do: false

  @spec storage_reason?(term()) :: boolean()
  def storage_reason?(:active_file_unavailable), do: true
  def storage_reason?(:invalid_preencoded_command), do: true
  def storage_reason?({:bitcask_append_failed, _reason}), do: true
  def storage_reason?({:bitcask_append_result_mismatch, _reason}), do: true
  def storage_reason?({:bitcask_writer_flush_failed, _reason}), do: true
  def storage_reason?({:blob_externalize_failed, _reason}), do: true
  def storage_reason?({:blob_ref_unavailable, _reason}), do: true
  def storage_reason?({:state_read_failed, _reason}), do: true
  def storage_reason?({:cross_shard_compensation_failed, _reason}), do: true
  def storage_reason?({:flow_history_projection_failed, _reason}), do: true
  def storage_reason?({:waraft_projection_failed, _reason}), do: true
  def storage_reason?({:flush_shard_apply_failed, _reason}), do: true
  def storage_reason?({:batch_result_mismatch, _expected, _actual}), do: true
  def storage_reason?({:tombstone_batch_result_mismatch, _expected, _actual}), do: true
  def storage_reason?({:fsync_dir_failed, _phase, _reason}), do: true
  def storage_reason?({:delete_prob_file_failed, _reason}), do: true

  def storage_reason?({:prob_sidecar_publish_failed, _final_path, _staged_path, _reason}),
    do: true

  def storage_reason?({:prob_sidecar_delete_failed, _path, _reason}), do: true

  def storage_reason?({:prob_dir_create_failed, _reason}), do: true
  def storage_reason?({:prob_sidecar_create_failed, _reason}), do: true
  def storage_reason?({:prob_sidecar_apply_failed, _operation, _reason}), do: true

  def storage_reason?(reason) when is_tuple(reason) and tuple_size(reason) > 1,
    do: MapSet.member?(@rollback_failure_tags, elem(reason, 0))

  def storage_reason?(_reason), do: false
end
