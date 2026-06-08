defmodule Ferricstore.Flow.HistoryProjector.Storage do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.HistoryProjector
  alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
  alias Ferricstore.Store.LFU

  @spec history_dir(binary()) :: binary()
  def history_dir(shard_data_path), do: Path.join(shard_data_path, "history")

  @spec history_file_path(binary(), non_neg_integer()) :: binary()
  def history_file_path(shard_data_path, file_id) do
    Path.join(
      history_dir(shard_data_path),
      "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log"
    )
  end

  @spec read_value(binary(), {:flow_history, non_neg_integer()}, non_neg_integer()) ::
          {:ok, binary()} | {:error, term()}
  defdelegate read_value(shard_data_path, file_id, offset),
    to: Ferricstore.Flow.HistoryProjector.Log

  @spec scan_event_value(binary(), binary()) :: {:ok, binary()} | :miss | {:error, term()}
  defdelegate scan_event_value(shard_data_path, target_key),
    to: Ferricstore.Flow.HistoryProjector.Log

  def history_active_file(shard_data_path) do
    :ok = ensure_history_file(shard_data_path)
    {:ok, 0, history_file_path(shard_data_path, 0)}
  rescue
    error -> {:error, {:history_file_unavailable, error}}
  end

  def ensure_history_file(shard_data_path) do
    dir = history_dir(shard_data_path)
    path = history_file_path(shard_data_path, 0)

    with :ok <- Ferricstore.FS.mkdir_p(dir),
         :ok <- touch_if_missing(path) do
      :ok
    end
  end

  def touch_if_missing(path) do
    if Ferricstore.FS.exists?(path), do: :ok, else: Ferricstore.FS.touch(path)
  end

  def append_batch(file_path, batch), do: NIF.v2_append_batch_nosync(file_path, batch)

  def sync_history_log_before_publish(file_path) do
    with :ok <- NIF.v2_fsync(file_path),
         :ok <-
           Ferricstore.FaultInjection.maybe_pause(:after_flow_history_fsync, %{
             file_path: file_path
           }) do
      maybe_history_projector_fsync_hook(file_path)
    end
  end

  def maybe_history_projector_fsync_hook(file_path) do
    case Application.get_env(:ferricstore, :flow_history_projector_fsync_hook) do
      fun when is_function(fun, 1) -> fun.(file_path)
      _other -> :ok
    end
  end

  def maybe_persist_projected_index(
        _instance_ctx,
        _shard_index,
        _shard_data_path,
        _file_path,
        _index,
        nil
      ),
      do: :ok

  def maybe_persist_projected_index(
        instance_ctx,
        shard_index,
        shard_data_path,
        _file_path,
        index,
        requested_index
      )
      when is_integer(index) and is_integer(requested_index) do
    HistoryProjector.publish_projected_index(instance_ctx, shard_index, shard_data_path, index)
  end

  def encode_entry(%{value: value} = entry) when is_binary(value), do: entry

  def encode_entry(%{snapshot: snapshot} = entry) do
    Map.put(entry, :value, flow_call(:encode_history_snapshot, [snapshot]))
  end

  def encode_entry(%{record: record, event: event, now_ms: now_ms} = entry) do
    value = flow_call(:encode_history_fields, [record, event, now_ms, Map.get(entry, :meta, %{})])
    Map.put(entry, :value, value)
  end

  def flow_call(function, args) do
    apply(Ferricstore.Flow, function, args)
  end

  def validate_locations(entries, locations) when length(entries) == length(locations), do: :ok

  def validate_locations(entries, locations),
    do: {:error, {:location_count_mismatch, length(entries), length(locations)}}

  def publish_keydir_entries(instance_ctx, shard_index, keydir, file_id, entries, locations) do
    initial_lfu = LFU.initial()

    entries
    |> Enum.zip(locations)
    |> Enum.each(fn {entry, {offset, value_size}} ->
      case HistoryProjector.safe_ets_lookup(keydir, entry.key) do
        [row] ->
          HistoryProjector.delete_apply_projection_cache_for_row(instance_ctx, shard_index, row)

        _missing ->
          :ok
      end

      HistoryProjector.track_keydir_binary_delta(
        instance_ctx,
        keydir,
        shard_index,
        entry.key,
        nil
      )

      HistoryProjector.safe_ets_insert(
        keydir,
        {entry.key, nil, entry.expire_at_ms, initial_lfu, {:flow_history, file_id}, offset,
         value_size}
      )
    end)
  end

  def publish_history_index(instance_ctx, shard_index, entries) do
    {flow_index, flow_lookup} =
      NativeFlowIndex.table_names(HistoryProjector.instance_name(instance_ctx), shard_index)

    native = NativeFlowIndex.get(flow_index, flow_lookup)
    {new_entries, update_entries} = history_index_entries(entries)

    if native && new_entries != [], do: NativeFlowIndex.put_new_entries(native, new_entries)
    if native && update_entries != [], do: NativeFlowIndex.put_entries(native, update_entries)

    :ok
  end

  @doc false
  def __history_index_entries_for_test__(entries), do: history_index_entries(entries)

  def history_index_entries(entries) do
    {new_entries, update_entries} =
      Enum.reduce(entries, {[], []}, fn entry, {new_acc, update_acc} ->
        index_entry = {entry.history_key, entry.event_id, entry.event_ms}

        if entry.version == 1 do
          {[index_entry | new_acc], update_acc}
        else
          {new_acc, [index_entry | update_acc]}
        end
      end)

    {Enum.reverse(new_entries), Enum.reverse(update_entries)}
  end

  def publish_lmdb_history_locations(shard_data_path, file_id, entries, locations) do
    with :ok <- maybe_history_projector_lmdb_publish_hook(shard_data_path, file_id, entries) do
      write_lmdb_ops(shard_data_path, lmdb_history_location_ops(file_id, entries, locations))
    end
  end

  def maybe_history_projector_lmdb_publish_hook(shard_data_path, file_id, entries) do
    case Application.get_env(:ferricstore, :flow_history_projector_lmdb_publish_hook) do
      fun when is_function(fun, 3) -> fun.(shard_data_path, file_id, entries)
      _other -> :ok
    end
  end

  def lmdb_history_location_ops(file_id, entries, locations) do
    entries
    |> Enum.zip(locations)
    |> Enum.flat_map(fn {entry, {offset, value_size}} ->
      history_index_key =
        Ferricstore.Flow.LMDB.history_index_key(
          entry.history_key,
          entry.event_id,
          entry.event_ms
        )

      [
        {:put, history_index_key,
         Ferricstore.Flow.LMDB.encode_history_index_value(
           entry.event_id,
           entry.event_ms,
           entry.key,
           entry.expire_at_ms,
           {:flow_history, file_id},
           offset,
           value_size
         )}
      ]
      |> maybe_history_expire_put(entry.expire_at_ms, history_index_key)
      |> Enum.reverse()
    end)
    |> maybe_history_flow_expire_puts(entries)
  end

  def write_lmdb_ops(_shard_data_path, []), do: :ok

  def write_lmdb_ops(shard_data_path, ops) do
    shard_data_path
    |> Ferricstore.Flow.LMDB.path()
    |> Ferricstore.Flow.LMDB.write_batch(ops)
  end

  def maybe_history_expire_put(ops, expire_at_ms, history_index_key) do
    case Ferricstore.Flow.LMDB.history_expire_key(expire_at_ms, history_index_key) do
      nil ->
        ops

      expire_key ->
        [
          {:put, expire_key, Ferricstore.Flow.LMDB.encode_history_expire_value(history_index_key)}
          | ops
        ]
    end
  end

  def maybe_history_flow_expire_puts(ops, entries) do
    entries
    |> Enum.reduce(%{}, fn entry, acc ->
      if is_integer(entry.expire_at_ms) and entry.expire_at_ms > 0 do
        Map.put(acc, entry.history_key, entry.expire_at_ms)
      else
        acc
      end
    end)
    |> Enum.reduce(ops, fn {history_key, expire_at_ms}, acc ->
      case Ferricstore.Flow.LMDB.history_flow_expire_key(expire_at_ms, history_key) do
        nil ->
          acc

        expire_key ->
          [
            {:put, expire_key,
             Ferricstore.Flow.LMDB.encode_history_flow_expire_value(history_key, expire_at_ms)}
            | acc
          ]
      end
    end)
  end
end
