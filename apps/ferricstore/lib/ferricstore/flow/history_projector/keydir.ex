defmodule Ferricstore.Flow.HistoryProjector.Keydir do
  @moduledoc false

  def track_keydir_binary_delta(%{keydir_binary_bytes: ref}, keydir, shard_index, key, new_value)
      when is_reference(ref) do
    old_bytes =
      case safe_ets_lookup(keydir, key) do
        [{^key, old_value, _expire_at_ms, _lfu, _file_id, _offset, _value_size}] ->
          binary_bytes(key) + binary_bytes(old_value)

        _ ->
          0
      end

    new_bytes = binary_bytes(key) + binary_bytes(new_value)
    delta = new_bytes - old_bytes
    if delta != 0, do: :atomics.add(ref, shard_index + 1, delta)
    :ok
  rescue
    _ -> :ok
  end

  def track_keydir_binary_delta(_instance_ctx, _keydir, _shard_index, _key, _new_value), do: :ok

  def track_keydir_binary_remove_row(%{keydir_binary_bytes: ref}, shard_index, row)
      when is_reference(ref) do
    bytes = keydir_row_binary_bytes(row)
    if bytes > 0, do: :atomics.sub(ref, shard_index + 1, bytes)
    :ok
  rescue
    _ -> :ok
  end

  def track_keydir_binary_remove_row(_instance_ctx, _shard_index, _row), do: :ok

  def delete_keydir_row(instance_ctx, keydir, shard_index, key) do
    case safe_ets_lookup(keydir, key) do
      [row] ->
        track_keydir_binary_remove_row(instance_ctx, shard_index, row)
        delete_apply_projection_cache_for_row(instance_ctx, shard_index, row)
        safe_ets_delete(keydir, key)

      _missing ->
        :ok
    end
  end

  def delete_apply_projection_cache_for_row(
        %{data_dir: data_dir},
        shard_index,
        {key, _value, _expire_at_ms, _lfu, {:waraft_apply_projection, index}, _offset,
         _value_size}
      )
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_binary(key) and is_integer(index) and index > 0 do
    Ferricstore.Raft.WARaftSegmentReader.delete_apply_projection_entries(data_dir, shard_index, [
      {index, key}
    ])

    :ok
  end

  def delete_apply_projection_cache_for_row(_instance_ctx, _shard_index, _row), do: :ok

  def keydir_row_binary_bytes(
        {key, old_value, _expire_at_ms, _lfu, _file_id, _offset, _value_size}
      ),
      do: binary_bytes(key) + binary_bytes(old_value)

  def keydir_row_binary_bytes(_row), do: 0

  def binary_bytes(value) when is_binary(value) and byte_size(value) > 64, do: byte_size(value)
  def binary_bytes(_value), do: 0

  def safe_ets_lookup(table, key) do
    :ets.lookup(table, key)
  rescue
    ArgumentError -> []
  end

  def safe_ets_insert(table, row) do
    :ets.insert(table, row)
    :ok
  rescue
    ArgumentError -> :ok
  end

  def safe_ets_delete(table, key) do
    :ets.delete(table, key)
    :ok
  rescue
    ArgumentError -> :ok
  end
end
