defmodule Ferricstore.Store.ExpiryTracker do
  @moduledoc false

  def expiring?(expire_at_ms) when is_integer(expire_at_ms) and expire_at_ms > 0, do: true
  def expiring?(_expire_at_ms), do: false

  def reset(nil, _shard_index), do: :ok

  def reset(ctx, shard_index) do
    idx = shard_index + 1

    if ref = counter_ref(ctx, shard_index), do: :atomics.put(ref, idx, 0)
    if ref = due_ref(ctx, shard_index), do: :atomics.put(ref, idx, 0)

    :ok
  end

  def count(nil, _shard_index), do: :unknown

  def count(ctx, shard_index) do
    case counter_ref(ctx, shard_index) do
      nil -> :unknown
      ref -> :atomics.get(ref, shard_index + 1)
    end
  end

  def count_for_state(%{instance_ctx: ctx, index: shard_index}), do: count(ctx, shard_index)
  def count_for_state(%{instance_ctx: ctx, shard_index: shard_index}), do: count(ctx, shard_index)
  def count_for_state(_state), do: :unknown

  def next_due(nil, _shard_index), do: :unknown

  def next_due(ctx, shard_index) do
    case due_ref(ctx, shard_index) do
      nil -> :unknown
      ref -> :atomics.get(ref, shard_index + 1)
    end
  end

  def next_due_for_state(%{instance_ctx: ctx, index: shard_index}), do: next_due(ctx, shard_index)

  def next_due_for_state(%{instance_ctx: ctx, shard_index: shard_index}),
    do: next_due(ctx, shard_index)

  def next_due_for_state(_state), do: :unknown

  def due_for_state?(state, now_ms) do
    case {count_for_state(state), next_due_for_state(state)} do
      {0, _due} -> false
      {:unknown, _due} -> true
      {_count, :unknown} -> true
      {_count, due} when not is_integer(due) or due <= 0 -> true
      {_count, due} -> due <= now_ms
    end
  end

  def defer_due_for_state(%{instance_ctx: ctx, index: shard_index}, due_at_ms),
    do: set_due(ctx, shard_index, due_at_ms)

  def defer_due_for_state(%{instance_ctx: ctx, shard_index: shard_index}, due_at_ms),
    do: set_due(ctx, shard_index, due_at_ms)

  def defer_due_for_state(_state, _due_at_ms), do: :ok

  def adjust_for_state(
        %{instance_ctx: ctx, index: shard_index},
        old_expire_at_ms,
        new_expire_at_ms
      ),
      do: adjust(ctx, shard_index, old_expire_at_ms, new_expire_at_ms)

  def adjust_for_state(
        %{instance_ctx: ctx, shard_index: shard_index},
        old_expire_at_ms,
        new_expire_at_ms
      ),
      do: adjust(ctx, shard_index, old_expire_at_ms, new_expire_at_ms)

  def adjust_for_state(_state, _old_expire_at_ms, _new_expire_at_ms), do: :ok

  def adjust(nil, _shard_index, _old_expire_at_ms, _new_expire_at_ms), do: :ok

  def adjust(ctx, shard_index, old_expire_at_ms, new_expire_at_ms) do
    old? = expiring?(old_expire_at_ms)
    new? = expiring?(new_expire_at_ms)

    cond do
      old? == new? ->
        track_due(ctx, shard_index, new_expire_at_ms)
        :ok

      new? ->
        track_due(ctx, shard_index, new_expire_at_ms)
        add(ctx, shard_index, 1)

      true ->
        add(ctx, shard_index, -1)
    end
  end

  def entry_expire_at([{_key, _value, expire_at_ms, _lfu, _fid, _offset, _value_size}]),
    do: expire_at_ms

  def entry_expire_at(_), do: 0

  defp add(ctx, shard_index, delta) do
    case counter_ref(ctx, shard_index) do
      nil ->
        :ok

      ref when delta < 0 ->
        idx = shard_index + 1

        case :atomics.get(ref, idx) do
          count when count > 0 -> :atomics.add(ref, idx, delta)
          _ -> :atomics.put(ref, idx, 0)
        end

      ref ->
        :atomics.add(ref, shard_index + 1, delta)
    end
  end

  defp counter_ref(%{expiry_key_counts: ref, shard_count: count}, shard_index)
       when ref != nil and is_integer(shard_index) and shard_index >= 0 and shard_index < count,
       do: ref

  defp counter_ref(_ctx, _shard_index), do: nil

  defp track_due(_ctx, _shard_index, expire_at_ms)
       when not is_integer(expire_at_ms) or
              expire_at_ms <= 0,
       do: :ok

  defp track_due(ctx, shard_index, expire_at_ms) do
    case due_ref(ctx, shard_index) do
      nil ->
        :ok

      ref ->
        idx = shard_index + 1
        current = :atomics.get(ref, idx)

        if current <= 0 or expire_at_ms < current do
          :atomics.put(ref, idx, expire_at_ms)
        end
    end
  end

  defp due_ref(%{expiry_next_due_at: ref, shard_count: count}, shard_index)
       when ref != nil and is_integer(shard_index) and shard_index >= 0 and shard_index < count,
       do: ref

  defp due_ref(_ctx, _shard_index), do: nil

  defp set_due(_ctx, _shard_index, due_at_ms) when not is_integer(due_at_ms) or due_at_ms <= 0,
    do: :ok

  defp set_due(ctx, shard_index, due_at_ms) do
    case due_ref(ctx, shard_index) do
      nil -> :ok
      ref -> :atomics.put(ref, shard_index + 1, due_at_ms)
    end
  end
end
