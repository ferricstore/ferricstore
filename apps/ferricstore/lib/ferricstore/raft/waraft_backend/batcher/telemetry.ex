defmodule Ferricstore.Raft.WARaftBackend.Batcher.Telemetry do
  @moduledoc false

  def emit_flush_telemetry(state, prefix, slot, result, flush_started, flush_finished) do
    measurements = flush_measurements(slot, flush_started, flush_finished, slot.count)
    result = result_shape(result)

    :telemetry.execute(
      [:ferricstore, :waraft, :batcher, :slot_flush],
      measurements,
      %{
        shard_index: state.shard_index,
        prefix: prefix,
        window_ms: slot.window_ms,
        result: result
      }
    )

    emit_quorum_submit_telemetry(state, measurements, result, slot.count)
  end

  def result_shape({:ok, _replies}), do: :ok
  def result_shape({:error, reason}), do: {:error, reason}
  def result_shape(_other), do: :other

  def emit_hot_flush_telemetry(state, kind, slot, result, flush_started, flush_finished) do
    measurements =
      slot
      |> flush_measurements(flush_started, flush_finished, slot.count)
      |> Map.put(:group_count, length(slot.groups))

    result = result_shape(result)

    :telemetry.execute(
      [:ferricstore, :waraft, :batcher, :hot_flush],
      measurements,
      %{
        shard_index: state.shard_index,
        kind: kind,
        window_ms: slot.window_ms,
        result: result
      }
    )

    emit_quorum_submit_telemetry(state, measurements, result, length(slot.groups))
  end

  def emit_quorum_submit_telemetry(state, measurements, result, caller_count) do
    # Keep the product-level quorum metric stable while WARaft owns the backend.
    # This fires once per flushed batch, so it preserves observability without
    # adding per-command telemetry on the hot path.
    :telemetry.execute(
      [:ferricstore, :batcher, :quorum_submit],
      measurements
      |> Map.put(:caller_count, caller_count)
      |> Map.put_new(:command_bytes, 0),
      %{
        backend: :waraft,
        shard_index: state.shard_index,
        kind: :batch,
        status: quorum_submit_status(result)
      }
    )
  end

  def quorum_submit_status(:ok), do: :ok
  def quorum_submit_status({:error, _reason}), do: :error
  def quorum_submit_status(_other), do: :unknown

  def flush_measurements(slot, flush_started, flush_finished, batch_size) do
    queue_age_us = native_to_us(flush_started - slot.created_mono)
    flush_duration_us = native_to_us(flush_finished - flush_started)
    total_duration_us = native_to_us(flush_finished - slot.created_mono)

    %{
      batch_size: batch_size,
      duration_us: total_duration_us,
      queue_wait_us: queue_age_us,
      queue_age_us: queue_age_us,
      flush_duration_us: flush_duration_us,
      total_duration_us: total_duration_us
    }
  end

  def native_to_us(value), do: System.convert_time_unit(value, :native, :microsecond)
end
