defmodule Ferricstore.Flow.HistoryProjector.Recovery do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.HistoryProjectedIndex
  alias Ferricstore.Flow.HistoryProjector
  alias Ferricstore.Flow.HistoryProjector.KeyCodec
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  def recover_history_log(instance_ctx, shard_index, shard_data_path, keydir_override) do
    file_path = HistoryProjector.history_file_path(shard_data_path, 0)

    case NIF.v2_scan_file(file_path) do
      {:ok, records} ->
        keydir = keydir_override || HistoryProjector.keydir(instance_ctx, shard_index)
        live_records = live_history_records(records)
        {entries, locations} = recovered_history_entries(live_records, keydir, shard_data_path)

        with :ok <- HistoryProjector.publish_lmdb_history_locations(shard_data_path, 0, entries, locations),
             :ok <-
               HistoryProjector.publish_keydir_entries(instance_ctx, shard_index, keydir, 0, entries, locations),
             :ok <- HistoryProjector.publish_history_index(instance_ctx, shard_index, entries),
             :ok <- HistoryProjector.trim_history_hot_cache(instance_ctx, shard_index, keydir, entries) do
          :ok
        end

      {:error, reason} ->
        {:error, {:history_scan_failed, reason}}

      other ->
        {:error, {:history_scan_unexpected, other}}
    end
  rescue
    error -> {:error, {:history_recover_exception, error}}
  end

  def live_history_records(records) do
    Enum.reduce(records, %{}, fn
      {key, _offset, _value_size, _expire_at_ms, true}, acc ->
        Map.delete(acc, key)

      {key, offset, value_size, expire_at_ms, false}, acc ->
        Map.put(acc, key, {offset, value_size, expire_at_ms})
    end)
  end

  def recovered_history_entries(live_records, keydir, shard_data_path) do
    {entries, locations, _caps} =
      Enum.reduce(live_records, {[], [], %{}}, fn {key, {offset, value_size, expire_at_ms}},
                                                  {entries, locations, caps} ->
        case KeyCodec.parse_history_entry_key(key) do
          {:ok, history_key, event_id, event_ms} ->
            {history_hot_max_events, caps} =
              recovered_history_hot_cap(history_key, keydir, shard_data_path, caps)

            version = recovered_history_event_version(event_id)

            entry = %{
              key: key,
              expire_at_ms: expire_at_ms,
              history_key: history_key,
              event_id: event_id,
              event_ms: event_ms,
              version: version,
              history_hot_max_events: history_hot_max_events
            }

            {[entry | entries], [{offset, value_size} | locations], caps}

          :error ->
            {entries, locations, caps}
        end
      end)

    {Enum.reverse(entries), Enum.reverse(locations)}
  end

  def recovered_history_hot_cap(history_key, keydir, shard_data_path, caps) do
    case Map.fetch(caps, history_key) do
      {:ok, max_events} ->
        {max_events, caps}

      :error ->
        max_events = load_history_hot_cap(history_key, keydir, shard_data_path)
        {max_events, Map.put(caps, history_key, max_events)}
    end
  end

  def recovered_history_event_version(event_id) do
    case KeyCodec.parse_event_version(event_id) do
      {:ok, version} -> version
      :error -> 1
    end
  end

  def load_history_hot_cap(history_key, keydir, shard_data_path) do
    with {:ok, state_key} <- history_state_key(history_key),
         {:ok, %{history_hot_max_events: max_events}} <-
           load_history_state_record(state_key, keydir, shard_data_path),
         true <- is_integer(max_events) and max_events >= 0 do
      max_events
    else
      _ -> default_history_hot_max_events()
    end
  end

  def load_history_state_record(state_key, keydir, shard_data_path) do
    case HistoryProjector.safe_ets_lookup(keydir, state_key) do
      [{^state_key, _value, _expire_at_ms, _lfu, _file_id, _offset, _value_size} = row] ->
        with {:ok, value} <- keydir_row_value(shard_data_path, row) do
          decode_flow_record(value)
        end

      _ ->
        load_lmdb_history_state_record(state_key, shard_data_path)
    end
  end

  def load_lmdb_history_state_record(state_key, shard_data_path) do
    path = Ferricstore.Flow.LMDB.path(shard_data_path)

    with {:ok, blob} <- Ferricstore.Flow.LMDB.get(path, state_key),
         {:ok, value} <-
           Ferricstore.Flow.LMDB.decode_value(blob, System.system_time(:millisecond)) do
      decode_flow_record(value)
    end
  end

  def history_state_key(history_key) when is_binary(history_key) do
    case :binary.split(history_key, ":h:") do
      [prefix, id] when byte_size(prefix) > 0 and byte_size(id) > 0 ->
        {:ok, prefix <> ":s:" <> id}

      _ ->
        :error
    end
  end

  def keydir_row_value(
         _shard_data_path,
         {_key, value, _expire_at_ms, _lfu, _file_id, _offset, _size}
       )
       when is_binary(value),
       do: {:ok, value}

  def keydir_row_value(shard_data_path, {_key, nil, _expire_at_ms, _lfu, file_id, offset, _size})
       when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 do
    shard_data_path
    |> ShardETS.file_path(file_id)
    |> NIF.v2_pread_at(offset)
  end

  def keydir_row_value(_shard_data_path, _row), do: :error

  def decode_flow_record(value) when is_binary(value) do
    {:ok, HistoryProjector.flow_call(:decode_record, [value])}
  rescue
    _ -> :error
  end

  def default_history_hot_max_events do
    Ferricstore.Flow.RetryPolicy.default_retention().history_hot_max_events
  rescue
    _ -> 1
  end

  def skip_history_log_recover?(shard_data_path, projected)
       when is_integer(projected) and projected >= 0 do
    default_history_hot_max_events() == 0 and lmdb_projection_present?(shard_data_path) and
      history_log_safe_to_skip?(shard_data_path)
  end

  def skip_history_log_recover?(_shard_data_path, _projected), do: false

  def history_log_safe_to_skip?(shard_data_path) do
    shard_data_path
    |> HistoryProjector.history_file_path(0)
    |> File.stat()
    |> case do
      {:ok, %{type: :regular, size: 0}} -> true
      {:ok, %{type: :directory}} -> true
      {:error, :enoent} -> true
      _ -> false
    end
  end

  def lmdb_projection_present?(shard_data_path) do
    shard_data_path
    |> Ferricstore.Flow.LMDB.path()
    |> Path.join("data.mdb")
    |> File.stat()
    |> case do
      {:ok, %{type: :regular, size: size}} when size > 0 -> true
      _ -> false
    end
  end

  def prepare_recovered_history_projector(instance_ctx, shard_index, shard_data_path) do
    with :ok <- HistoryProjector.ensure_history_file(shard_data_path) do
      projected = HistoryProjectedIndex.read(shard_data_path)
      HistoryProjector.publish_projected_index(instance_ctx, shard_index, shard_data_path, projected)
    end
  rescue
    error -> {:error, {:history_projector_prepare_failed, error}}
  end
end
