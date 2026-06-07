defmodule Ferricstore.Store.Shard.Compound.Support do
  @moduledoc false

  alias Ferricstore.Store.BlobValue
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  @cold_batch_read_timeout_ms 10_000

  def read_cold_async(state, path, offset, key) do
    with {:ok, value} <-
           Ferricstore.Store.ColdRead.pread_keyed(path, offset, key, @cold_batch_read_timeout_ms),
         {:ok, materialized} <- materialize_blob_value(state, value) do
      {:ok, materialized}
    end
  end

  defp materialize_blob_value(%{data_dir: data_dir, index: shard_index} = state, value) do
    BlobValue.maybe_materialize(data_dir, shard_index, blob_side_channel_threshold(state), value)
  end

  defp materialize_blob_value(_state, value), do: {:ok, value}

  def persisted_disk_entries(state, entries) do
    {prepared_reversed, disk_values_reversed} =
      Enum.reduce(entries, {[], []}, fn {compound_key, value, expire_at_ms},
                                        {prepared_acc, disk_acc} ->
        disk_value = ShardETS.to_disk_binary(value)

        {
          [{compound_key, expire_at_ms} | prepared_acc],
          [disk_value | disk_acc]
        }
      end)

    with {:ok, persisted_values} <-
           BlobValue.maybe_externalize_many(
             Map.get(state, :data_dir),
             Map.get(state, :index, 0),
             blob_side_channel_threshold(state),
             Enum.reverse(disk_values_reversed)
           ),
         {:ok, persisted_entries} <-
           attach_persisted_disk_entries(Enum.reverse(prepared_reversed), persisted_values) do
      {:ok, persisted_entries}
    end
  end

  defp attach_persisted_disk_entries(prepared, persisted_values),
    do: attach_persisted_disk_entries(prepared, persisted_values, [])

  defp attach_persisted_disk_entries(
         [{compound_key, expire_at_ms} | prepared],
         [persisted_value | persisted_values],
         acc
       ) do
    attach_persisted_disk_entries(prepared, persisted_values, [
      {compound_key, persisted_value, expire_at_ms} | acc
    ])
  end

  defp attach_persisted_disk_entries([], [], acc), do: {:ok, Enum.reverse(acc)}

  defp attach_persisted_disk_entries(_prepared, _persisted_values, _acc),
    do: {:error, :blob_externalize_result_mismatch}

  def persisted_disk_value(state, value) do
    disk_value = ShardETS.to_disk_binary(value)

    BlobValue.maybe_externalize(
      Map.get(state, :data_dir),
      Map.get(state, :index, 0),
      blob_side_channel_threshold(state),
      disk_value
    )
  end

  def blob_side_channel_threshold(%{instance_ctx: ctx}), do: BlobValue.threshold(ctx)
  def blob_side_channel_threshold(_state), do: 0

  # -- Off-heap binary byte tracking --

  def keydir_binary_ref(%{instance_ctx: %{keydir_binary_bytes: ref, shard_count: count}} = state)
      when ref != nil do
    index = Map.fetch!(state, :index)
    if index < count, do: ref, else: nil
  end

  def keydir_binary_ref(%{instance_name: name} = state) when is_atom(name) do
    keydir_binary_ref_for_instance(name, Map.fetch!(state, :index))
  end

  def keydir_binary_ref(state) do
    keydir_binary_ref_for_instance(:default, Map.fetch!(state, :index))
  end

  def keydir_binary_ref_for_instance(name, index) do
    try do
      %{keydir_binary_bytes: ref, shard_count: count} = FerricStore.Instance.get(name)
      if ref != nil and index < count, do: ref, else: nil
    rescue
      _ -> nil
    catch
      :exit, _ -> nil
    end
  end

  def track_binary_insert(nil, _, _, _), do: :ok

  def track_binary_insert(ref, state, key, new_val) do
    new_bytes = offheap_size(key) + offheap_size(new_val)

    old_bytes =
      case :ets.lookup(state.keydir, key) do
        [{^key, old_val, _, _, _, _, _}] -> offheap_size(key) + offheap_size(old_val)
        _ -> 0
      end

    delta = new_bytes - old_bytes
    if delta != 0, do: :atomics.add(ref, state.index + 1, delta)
  end

  defp offheap_size(v) when is_binary(v) and byte_size(v) > 64, do: byte_size(v)
  defp offheap_size(_), do: 0
end
