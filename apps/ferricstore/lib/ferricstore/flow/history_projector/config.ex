defmodule Ferricstore.Flow.HistoryProjector.Config do
  @moduledoc false

  @default_batch_size 25_000
  @default_flush_interval_ms 1_000
  @default_chunk_interval_ms 1
  @default_max_pending_entries 100_000

  def default_batch_size, do: @default_batch_size
  def default_flush_interval_ms, do: @default_flush_interval_ms
  def default_chunk_interval_ms, do: @default_chunk_interval_ms
  def default_max_pending_entries, do: @default_max_pending_entries

  def initial_state(
        projector_name,
        shard_index,
        shard_data_path,
        instance_ctx,
        pending_counter,
        max_pending_entries,
        flushed_index
      ) do
    %{
      projector_name: projector_name,
      shard_index: shard_index,
      shard_data_path: shard_data_path,
      instance_ctx: instance_ctx,
      pending_counter: pending_counter,
      pending: [],
      pending_count: 0,
      first_pending_at: nil,
      flush_timer: nil,
      requested_index: nil,
      flushed_index: flushed_index,
      batch_size: app_env(:flow_history_projector_batch_size, @default_batch_size),
      max_pending_entries: max_pending_entries,
      flush_interval_ms:
        app_env(:flow_history_projector_flush_interval_ms, @default_flush_interval_ms),
      chunk_interval_ms:
        app_env(:flow_history_projector_chunk_interval_ms, @default_chunk_interval_ms)
    }
  end

  defp app_env(key, default) do
    case Application.get_env(:ferricstore, key, default) do
      value when is_integer(value) and value > 0 -> value
      _ -> default
    end
  end
end
