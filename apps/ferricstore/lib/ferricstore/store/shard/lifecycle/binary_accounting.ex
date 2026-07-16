defmodule Ferricstore.Store.Shard.Lifecycle.BinaryAccounting do
  @moduledoc false

  alias Ferricstore.Store.ExpiryTracker

  defp keydir_binary_ref(%{keydir_binary_bytes: ref, shard_count: count}, shard_index)
       when ref != nil do
    if shard_index < count, do: ref, else: nil
  end

  defp keydir_binary_ref(name, shard_index) when is_atom(name) and not is_nil(name) do
    keydir_binary_ref_for_instance(name, shard_index)
  end

  defp keydir_binary_ref(_instance_ctx, _shard_index), do: nil

  defp keydir_binary_ref_for_instance(name, shard_index) do
    try do
      %{keydir_binary_bytes: ref, shard_count: count} = FerricStore.Instance.get(name)
      if ref != nil and shard_index < count, do: ref, else: nil
    rescue
      _ -> nil
    catch
      :exit, _ -> nil
    end
  end

  # Tracks bytes added for a fresh insert (no existing entry expected, or replaces).
  def track_add(shard_index, key, value, instance_ctx) do
    ref = keydir_binary_ref(instance_ctx, shard_index)

    if ref do
      bytes = offheap_size(key) + offheap_size(value)
      if bytes > 0, do: :atomics.add(ref, shard_index + 1, bytes)
    end
  end

  # Tracks bytes removed for a delete (lookup existing entry first).
  def track_remove(keydir, shard_index, key, instance_ctx) do
    ref = keydir_binary_ref(instance_ctx, shard_index)

    if ref do
      bytes =
        case :ets.lookup(keydir, key) do
          [{^key, val, _, _, _, _, _}] -> offheap_size(key) + offheap_size(val)
          _ -> 0
        end

      if bytes > 0, do: :atomics.sub(ref, shard_index + 1, bytes)
    end
  end

  def rebuild(keydir, shard_index, instance_ctx) do
    {binary_bytes, expiry_count, next_due_at_ms} =
      :ets.foldl(
        fn
          {key, value, expire_at_ms, _lfu, _file_id, _offset, _value_size},
          {bytes, count, next_due} ->
            bytes = bytes + offheap_size(key) + offheap_size(value)

            if ExpiryTracker.expiring?(expire_at_ms) do
              next_due = if next_due == 0, do: expire_at_ms, else: min(next_due, expire_at_ms)
              {bytes, count + 1, next_due}
            else
              {bytes, count, next_due}
            end

          _entry, totals ->
            totals
        end,
        {0, 0, 0},
        keydir
      )

    if ref = keydir_binary_ref(instance_ctx, shard_index) do
      :atomics.put(ref, shard_index + 1, binary_bytes)
    end

    ExpiryTracker.restore(instance_ctx, shard_index, expiry_count, next_due_at_ms)
  end

  defp offheap_size(v) when is_binary(v) and byte_size(v) > 64, do: byte_size(v)
  defp offheap_size(_), do: 0
end
