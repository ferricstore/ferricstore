defmodule Ferricstore.Flow.HistoryProjector.Telemetry do
  @moduledoc false

  def emit_recover_error(instance_ctx, shard_index, reason) do
    :telemetry.execute(
      [:ferricstore, :flow, :history_projector, :recover],
      %{errors: 1},
      %{
        instance: instance_name(instance_ctx),
        shard_index: shard_index,
        reason: reason
      }
    )
  rescue
    _ -> :ok
  end

  def emit_queue_full(
        instance_ctx,
        shard_index,
        pending_entries,
        incoming_entries,
        max_pending_entries
      ) do
    :telemetry.execute(
      [:ferricstore, :flow, :history_projector, :queue_full],
      %{
        count: 1,
        pending_entries: pending_entries,
        incoming_entries: incoming_entries,
        max_pending_entries: max_pending_entries
      },
      %{
        instance: instance_name(instance_ctx),
        shard_index: shard_index
      }
    )
  rescue
    _ -> :ok
  end

  def publish_requested_index(instance_ctx, shard_index, index)
      when is_integer(index) and index >= 0 do
    put_atomic_max(instance_ctx, :flow_history_requested_index, shard_index, index)
  end

  def publish_requested_index(_instance_ctx, _shard_index, _index), do: :ok

  def mark_queue_full(instance_ctx, shard_index) do
    add_atomic(instance_ctx, :flow_history_projector_queue_full, shard_index, 1)
  end

  defp put_atomic_max(instance_ctx, field, shard_index, value)
       when is_integer(shard_index) and shard_index >= 0 and is_integer(value) and value >= 0 do
    case Map.get(instance_ctx || %{}, field) do
      ref when is_reference(ref) ->
        size = :atomics.info(ref).size

        if shard_index < size do
          index = shard_index + 1
          current = :atomics.get(ref, index)
          if value > current, do: :atomics.put(ref, index, value)
        end

      _ ->
        :ok
    end

    :ok
  rescue
    _ -> :ok
  end

  defp add_atomic(instance_ctx, field, shard_index, increment)
       when is_integer(shard_index) and shard_index >= 0 and is_integer(increment) do
    case Map.get(instance_ctx || %{}, field) do
      ref when is_reference(ref) ->
        size = :atomics.info(ref).size
        if shard_index < size, do: :atomics.add(ref, shard_index + 1, increment)

      _ ->
        :ok
    end

    :ok
  rescue
    _ -> :ok
  end

  defp instance_name(%{name: name}), do: name
  defp instance_name(_instance_ctx), do: :default
end
