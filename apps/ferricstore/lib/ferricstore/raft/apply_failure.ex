defmodule Ferricstore.Raft.ApplyFailure do
  @moduledoc false

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
  def storage_reason?({:flush_shard_apply_failed, _reason}), do: true
  def storage_reason?({:batch_result_mismatch, _expected, _actual}), do: true
  def storage_reason?({:tombstone_batch_result_mismatch, _expected, _actual}), do: true
  def storage_reason?({:fsync_dir_failed, _phase, _reason}), do: true
  def storage_reason?({:delete_prob_file_failed, _reason}), do: true
  def storage_reason?(_reason), do: false
end
