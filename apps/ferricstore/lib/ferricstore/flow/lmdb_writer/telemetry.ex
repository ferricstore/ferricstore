defmodule Ferricstore.Flow.LMDBWriter.Telemetry do
  @moduledoc false

  def publish_durable(%{flow_lmdb_replay_safe_index: replay_safe_index}, shard_index, index)
      when is_reference(replay_safe_index) do
    if shard_index < :atomics.info(replay_safe_index).size do
      :atomics.put(replay_safe_index, shard_index + 1, index)
    end

    :ok
  rescue
    _ -> :ok
  end

  def publish_durable(_instance_ctx, _shard_index, _index), do: :ok

  def publish_requested(
        %{flow_lmdb_replay_safe_requested_index: requested_index},
        shard_index,
        index
      )
      when is_reference(requested_index) do
    put_atomic_max(requested_index, shard_index, index)
  rescue
    _ -> :ok
  end

  def publish_requested(_instance_ctx, _shard_index, _index), do: :ok

  def reset_replay_safe(instance_ctx, shard_index, index)
      when is_integer(shard_index) and shard_index >= 0 and is_integer(index) and index >= 0 do
    reset_atomic(Map.get(instance_ctx || %{}, :flow_lmdb_replay_safe_index), shard_index, index)

    reset_atomic(
      Map.get(instance_ctx || %{}, :flow_lmdb_replay_safe_requested_index),
      shard_index,
      index
    )

    reset_atomic(Map.get(instance_ctx || %{}, :flow_lmdb_mirror_degraded), shard_index, 0)
    :ok
  end

  def record_persist_failure(%{flow_lmdb_replay_safe_persist_failures: failures}, shard_index)
      when is_reference(failures) do
    if shard_index < :atomics.info(failures).size do
      :atomics.add(failures, shard_index + 1, 1)
    end

    :ok
  rescue
    _ -> :ok
  end

  def record_persist_failure(_instance_ctx, _shard_index), do: :ok

  def record_flush_failure(%{flow_lmdb_writer_flush_failures: failures}, shard_index)
      when is_reference(failures) do
    if shard_index < :atomics.info(failures).size do
      :atomics.add(failures, shard_index + 1, 1)
    end

    :ok
  rescue
    _ -> :ok
  end

  def record_flush_failure(_instance_ctx, _shard_index), do: :ok

  def mark_mirror_degraded(%{flow_lmdb_mirror_degraded: degraded}, shard_index, reason)
      when is_reference(degraded) do
    if shard_index < :atomics.info(degraded).size do
      :atomics.put(degraded, shard_index + 1, 1)
    end

    :telemetry.execute(
      [:ferricstore, :flow, :lmdb_mirror, :degraded],
      %{count: 1},
      %{shard_index: shard_index, reason: reason, source: :flush}
    )

    :ok
  rescue
    _ -> :ok
  end

  def mark_mirror_degraded(_instance_ctx, _shard_index, _reason), do: :ok

  def emit_backlog(state, now) do
    pending_age_us = pending_age_us(state, now)
    publish_backlog(state, pending_age_us)

    :telemetry.execute(
      [:ferricstore, :flow, :lmdb_writer, :backlog],
      %{
        pending_ops: state.count,
        pending_after_flush: length(state.pending_after_flush),
        oldest_pending_age_us: pending_age_us,
        requested_index: state.requested_index,
        durable_index: state.durable_index,
        replay_safe_lag: replay_safe_lag(state)
      },
      writer_metadata(state)
    )
  end

  def publish_backlog(state, pending_age_us) do
    publish_atomic(
      state.instance_ctx,
      :flow_lmdb_writer_pending_ops,
      state.shard_index,
      state.count
    )

    publish_atomic(
      state.instance_ctx,
      :flow_lmdb_writer_oldest_pending_age_us,
      state.shard_index,
      pending_age_us
    )
  end

  def emit_flush(status, state, started_at, op_count, expanded_op_count, pending_age_us) do
    :telemetry.execute(
      [:ferricstore, :flow, :lmdb_writer, :flush],
      %{
        duration_us: duration_us(started_at),
        op_count: op_count,
        expanded_op_count: expanded_op_count,
        pending_age_us: pending_age_us,
        requested_index: state.requested_index,
        durable_index: state.durable_index,
        replay_safe_lag: replay_safe_lag(state)
      },
      state
      |> writer_metadata()
      |> Map.put(:status, persist_status(status))
      |> Map.put(:reason, persist_reason(status))
    )
  end

  def emit_persist(status, state, index, started_at) do
    requested_index = max(state.requested_index, index)
    durable_index = if status == :ok, do: index, else: state.durable_index

    :telemetry.execute(
      [:ferricstore, :flow, :lmdb_replay_safe_index, :persist],
      %{
        duration_us: duration_us(started_at),
        index: index,
        requested_index: requested_index,
        durable_index: durable_index,
        lag: max(requested_index - durable_index, 0)
      },
      %{
        status: persist_status(status),
        shard_index: state.shard_index,
        reason: persist_reason(status)
      }
    )
  end

  def writer_unavailable(operation, instance_name, shard_index, reason, op_count) do
    :telemetry.execute(
      [:ferricstore, :flow, :lmdb_writer, :unavailable],
      %{op_count: op_count},
      %{
        operation: operation,
        instance_name: instance_name,
        shard_index: shard_index,
        reason: reason
      }
    )

    {:error, reason}
  end

  def pending_age_us(%{first_pending_at: nil}, _now), do: 0

  def pending_age_us(%{first_pending_at: first_pending_at}, now) do
    now
    |> Kernel.-(first_pending_at)
    |> System.convert_time_unit(:native, :microsecond)
    |> max(0)
  end

  def duration_us(started_at) do
    System.monotonic_time()
    |> Kernel.-(started_at)
    |> System.convert_time_unit(:native, :microsecond)
  end

  defp put_atomic_max(ref, shard_index, value) do
    if shard_index < :atomics.info(ref).size do
      position = shard_index + 1
      current = :atomics.get(ref, position)

      if value > current do
        :atomics.put(ref, position, value)
      end
    end

    :ok
  end

  defp publish_atomic(ctx, field, shard_index, value) when is_map(ctx) do
    case Map.get(ctx, field) do
      ref when is_reference(ref) ->
        if shard_index < :atomics.info(ref).size do
          :atomics.put(ref, shard_index + 1, max(value, 0))
        end

        :ok

      _ ->
        :ok
    end
  rescue
    _ -> :ok
  end

  defp publish_atomic(_ctx, _field, _shard_index, _value), do: :ok

  defp persist_status(:ok), do: :ok
  defp persist_status({:error, _}), do: :error

  defp persist_reason(:ok), do: :none
  defp persist_reason({:error, reason}), do: reason

  defp replay_safe_lag(state), do: max(state.requested_index - state.durable_index, 0)

  defp reset_atomic(ref, shard_index, value) when is_reference(ref) do
    if shard_index < :atomics.info(ref).size do
      :atomics.put(ref, shard_index + 1, value)
    end

    :ok
  rescue
    _ -> :ok
  end

  defp reset_atomic(_ref, _shard_index, _value), do: :ok

  defp writer_metadata(state) do
    %{
      shard_index: state.shard_index,
      instance_name: state.instance_name
    }
  end
end
