defmodule Ferricstore.Flow.HistoryProjector.Recovery do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.HistoryProjectedIndex
  alias Ferricstore.Flow.HistoryProjector
  alias Ferricstore.Flow.HistoryProjector.KeyCodec
  alias Ferricstore.Flow.HistoryProjector.Log
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.Query.QueryRowCodec
  alias Ferricstore.Flow.RecordIdentity
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  @max_exact_integer 9_007_199_254_740_991

  def recover_history_log(instance_ctx, shard_index, shard_data_path, keydir_override) do
    file_path = HistoryProjector.history_file_path(shard_data_path, 0)

    case Log.reduce_metadata_pages(file_path, {:ok, %{}}, &accumulate_live_history_record/2) do
      {:ok, {:ok, live_records}} ->
        keydir = keydir_override || HistoryProjector.keydir(instance_ctx, shard_index)

        with {:ok, {entries, locations}} <-
               recovered_history_entries(live_records, keydir, shard_data_path),
             :ok <-
               HistoryProjector.publish_lmdb_history_locations(
                 shard_data_path,
                 0,
                 entries,
                 locations
               ),
             :ok <-
               HistoryProjector.publish_keydir_entries(
                 instance_ctx,
                 shard_index,
                 keydir,
                 0,
                 entries,
                 locations
               ),
             :ok <- HistoryProjector.publish_history_index(instance_ctx, shard_index, entries),
             :ok <-
               HistoryProjector.trim_history_hot_cache(instance_ctx, shard_index, keydir, entries) do
          :ok
        end

      {:ok, {:error, reason}} ->
        {:error, reason}

      {:error, reason} ->
        {:error, {:history_scan_failed, reason}}

      other ->
        {:error, {:history_scan_unexpected, other}}
    end
  rescue
    error -> {:error, {:history_recover_exception, error}}
  end

  def live_history_records(records) do
    Enum.reduce(records, {:ok, %{}}, &accumulate_live_history_record/2)
  end

  defp accumulate_live_history_record(_record, {:error, _reason} = error), do: error

  defp accumulate_live_history_record(
         {key, offset, value_size, expire_at_ms, deleted?} = record,
         {:ok, acc}
       ),
       do:
         accumulate_valid_history_record(
           record,
           key,
           offset,
           value_size,
           expire_at_ms,
           deleted?,
           acc
         )

  defp accumulate_live_history_record(record, {:ok, _acc}),
    do: {:error, {:invalid_history_log_record, record}}

  defp accumulate_valid_history_record(
         _record,
         key,
         offset,
         value_size,
         expire_at_ms,
         deleted?,
         acc
       )
       when is_binary(key) and is_integer(offset) and offset >= 0 and is_integer(value_size) and
              value_size >= 0 and is_integer(expire_at_ms) and expire_at_ms >= 0 and
              expire_at_ms <= @max_exact_integer and is_boolean(deleted?) do
    case KeyCodec.parse_history_entry_key(key) do
      {:ok, _history_key, _event_id, _event_ms} ->
        if deleted? do
          {:ok, Map.delete(acc, key)}
        else
          {:ok, Map.put(acc, key, {offset, value_size, expire_at_ms})}
        end

      :error ->
        if Keys.value_key?(key) do
          {:ok, acc}
        else
          {:error, {:invalid_history_log_key, key}}
        end
    end
  end

  defp accumulate_valid_history_record(record, _key, _offset, _size, _expire, _deleted, _acc),
    do: {:error, {:invalid_history_log_record, record}}

  def recovered_history_entries(live_records, keydir, shard_data_path) do
    live_records
    |> Enum.reduce_while({:ok, [], [], %{}}, fn
      {key, {offset, value_size, expire_at_ms}}, {:ok, entries, locations, caps} ->
        case KeyCodec.parse_history_entry_key(key) do
          {:ok, history_key, event_id, event_ms} ->
            case recovered_history_hot_cap(history_key, keydir, shard_data_path, caps) do
              {:ok, history_hot_max_events, next_caps} ->
                entry = %{
                  key: key,
                  expire_at_ms: expire_at_ms,
                  history_key: history_key,
                  event_id: event_id,
                  event_ms: event_ms,
                  version: recovered_history_event_version(event_id),
                  history_hot_max_events: history_hot_max_events
                }

                {:cont, {:ok, [entry | entries], [{offset, value_size} | locations], next_caps}}

              {:error, _reason} = error ->
                {:halt, error}
            end

          :error ->
            {:halt, {:error, {:invalid_history_log_key, key}}}
        end

      _invalid, _acc ->
        {:halt, {:error, :invalid_history_recovery_entry}}
    end)
    |> case do
      {:ok, entries, locations, _caps} ->
        {:ok, {Enum.reverse(entries), Enum.reverse(locations)}}

      {:error, _reason} = error ->
        error
    end
  end

  def recovered_history_hot_cap(history_key, keydir, shard_data_path, caps) do
    case Map.fetch(caps, history_key) do
      {:ok, max_events} ->
        {:ok, max_events, caps}

      :error ->
        case load_history_hot_cap(history_key, keydir, shard_data_path) do
          {:ok, max_events} -> {:ok, max_events, Map.put(caps, history_key, max_events)}
          {:error, _reason} = error -> error
        end
    end
  end

  def recovered_history_event_version(event_id) do
    case KeyCodec.parse_event_version(event_id) do
      {:ok, version} -> version
      :error -> 1
    end
  end

  def load_history_hot_cap(history_key, keydir, shard_data_path) do
    case history_state_key(history_key) do
      {:ok, state_key} ->
        case load_history_state_record(state_key, keydir, shard_data_path) do
          {:ok, %{history_hot_max_events: max_events}}
          when is_integer(max_events) and max_events >= 0 and max_events <= @max_exact_integer ->
            {:ok, max_events}

          :not_found ->
            {:ok, default_history_hot_max_events()}

          {:ok, _invalid} ->
            {:error, :invalid_history_hot_cap}

          {:error, _reason} = error ->
            error
        end

      :error ->
        {:error, :invalid_history_key}
    end
  end

  def load_history_state_record(state_key, keydir, shard_data_path) do
    case HistoryProjector.safe_ets_lookup(keydir, state_key) do
      [{^state_key, _value, _expire_at_ms, _lfu, _file_id, _offset, _value_size} = row] ->
        case keydir_row_value(shard_data_path, row) do
          {:ok, value} ->
            case decode_flow_record(value) do
              {:ok, record} when is_map(record) ->
                if RecordIdentity.owns_state_key?(record, state_key),
                  do: {:ok, record},
                  else: {:error, :corrupt_history_state_record}

              _invalid ->
                {:error, :corrupt_history_state_record}
            end

          {:error, reason} ->
            {:error, {:history_state_read_failed, reason}}

          _invalid ->
            {:error, :corrupt_history_state_keydir_entry}
        end

      [] ->
        load_lmdb_history_state_record(state_key, shard_data_path)

      _invalid ->
        {:error, :corrupt_history_state_keydir_entry}
    end
  end

  def load_lmdb_history_state_record(state_key, shard_data_path) do
    path = Ferricstore.Flow.LMDB.path(shard_data_path)

    case Ferricstore.Flow.LMDB.get(path, state_key) do
      {:ok, blob} when is_binary(blob) ->
        case QueryRowCodec.decode(blob, state_key) do
          {:ok, %{record: record}} when is_map(record) -> {:ok, record}
          _invalid -> {:error, :corrupt_history_query_row}
        end

      :not_found ->
        :not_found

      {:error, reason} ->
        {:error, {:history_query_row_read_failed, reason}}

      _invalid ->
        {:error, :corrupt_history_query_row}
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

      HistoryProjector.publish_projected_index(
        instance_ctx,
        shard_index,
        shard_data_path,
        projected
      )
    end
  rescue
    error -> {:error, {:history_projector_prepare_failed, error}}
  end
end
