defmodule Ferricstore.Flow.HistoryProjector.Trim do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.HistoryProjector.KeyCodec
  alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex

  @trim_batch_size 4_096

  def trim_history_caps(
        instance_ctx,
        shard_index,
        shard_data_path,
        keydir,
        file_path,
        entries,
        callbacks
      ) do
    cap_requirements =
      history_cap_requirements(entries, fn history_key ->
        callbacks.load_history_max_cap.(history_key, keydir, shard_data_path)
      end)

    if map_size(cap_requirements) == 0 do
      :ok
    else
      {flow_index, flow_lookup} =
        NativeFlowIndex.table_names(instance_name(instance_ctx), shard_index)

      native = NativeFlowIndex.get(flow_index, flow_lookup)

      trim_history_cap_requirements(
        cap_requirements,
        instance_ctx,
        shard_index,
        shard_data_path,
        keydir,
        file_path,
        native,
        callbacks
      )
    end
  end

  defp trim_history_cap_requirements(
         cap_requirements,
         instance_ctx,
         shard_index,
         shard_data_path,
         keydir,
         file_path,
         native,
         callbacks
       ) do
    cap_requirements
    |> Enum.reduce_while(:ok, fn
      {history_key, {max_events, true}}, :ok ->
        case trim_history_cap_batches(
               history_key,
               max_events,
               instance_ctx,
               shard_index,
               shard_data_path,
               keydir,
               file_path,
               native,
               callbacks
             ) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end

      {_history_key, {_max_events, false}}, :ok ->
        {:cont, :ok}
    end)
  end

  defp trim_history_cap_batches(
         history_key,
         max_events,
         instance_ctx,
         shard_index,
         shard_data_path,
         keydir,
         file_path,
         native,
         callbacks
       ) do
    with {:ok, excess} <- history_trim_excess_count(history_key, shard_data_path, max_events) do
      trim_history_excess_batches(
        history_key,
        excess,
        instance_ctx,
        shard_index,
        shard_data_path,
        keydir,
        file_path,
        native,
        callbacks
      )
    end
  end

  defp trim_history_excess_batches(
         _history_key,
         0,
         _instance_ctx,
         _shard_index,
         _shard_data_path,
         _keydir,
         _file_path,
         _native,
         _callbacks
       ),
       do: :ok

  defp trim_history_excess_batches(
         history_key,
         excess,
         instance_ctx,
         shard_index,
         shard_data_path,
         keydir,
         file_path,
         native,
         callbacks
       ) do
    batch_size = history_trim_batch_size(excess)

    case lmdb_history_trim_batch(history_key, shard_data_path, batch_size) do
      {:ok, items} ->
        trim_items =
          Enum.map(items, fn {event_id, key, history_index_key} ->
            {history_key, event_id, key, history_index_key}
          end)

        tombstone_keys =
          Enum.map(trim_items, fn {_history_key, _event_id, key, _history_index_key} -> key end)

        with {:ok, lmdb_delete_ops} <-
               lmdb_history_delete_ops(shard_data_path, trim_items),
             :ok <- append_tombstones(file_path, tombstone_keys, callbacks),
             :ok <- write_lmdb_history_delete_ops(shard_data_path, lmdb_delete_ops) do
          if native do
            NativeFlowIndex.delete_entries(native, history_delete_entries(trim_items))
          end

          Enum.each(tombstone_keys, fn key ->
            callbacks.delete_keydir_row.(instance_ctx, keydir, shard_index, key)
          end)

          trim_history_excess_batches(
            history_key,
            excess - batch_size,
            instance_ctx,
            shard_index,
            shard_data_path,
            keydir,
            file_path,
            native,
            callbacks
          )
        end

      {:error, _reason} = error ->
        error
    end
  end

  def lmdb_history_over_cap_items(history_key, shard_data_path, max_events) do
    with {:ok, excess} <- history_trim_excess_count(history_key, shard_data_path, max_events) do
      case excess do
        0 ->
          {:ok, []}

        count ->
          lmdb_history_trim_batch(history_key, shard_data_path, history_trim_batch_size(count))
      end
    end
  end

  defp history_trim_excess_count(history_key, shard_data_path, max_events) do
    lmdb_path = Ferricstore.Flow.LMDB.path(shard_data_path)
    prefix = Ferricstore.Flow.LMDB.history_index_prefix(history_key)

    case Ferricstore.Flow.LMDB.prefix_count(lmdb_path, prefix) do
      {:ok, count} when is_integer(count) and count >= 0 and count <= max_events ->
        {:ok, 0}

      {:ok, count} when is_integer(count) and count > max_events ->
        {:ok, count - max_events}

      {:ok, invalid_count} ->
        {:error, {:invalid_history_trim_count, invalid_count}}

      {:error, _reason} = error ->
        error

      invalid ->
        {:error, {:invalid_history_trim_count_result, invalid}}
    end
  end

  defp lmdb_history_trim_batch(history_key, shard_data_path, trim_count) do
    lmdb_path = Ferricstore.Flow.LMDB.path(shard_data_path)
    prefix = Ferricstore.Flow.LMDB.history_index_prefix(history_key)

    case Ferricstore.Flow.LMDB.prefix_entries(lmdb_path, prefix, trim_count) do
      {:ok, entries} -> decode_lmdb_history_trim_items(entries, trim_count)
      {:error, _reason} = error -> error
      invalid -> {:error, {:invalid_history_trim_entries_result, invalid}}
    end
  end

  def history_trim_batch_size(excess) when is_integer(excess) and excess > 0,
    do: min(excess, @trim_batch_size)

  def decode_lmdb_history_trim_items(entries, expected_count)
      when is_list(entries) and is_integer(expected_count) and expected_count >= 0 do
    if length(entries) == expected_count do
      entries
      |> Enum.reduce_while({:ok, []}, fn entry, {:ok, acc} ->
        case decode_lmdb_history_trim_item(entry) do
          {:ok, item} -> {:cont, {:ok, [item | acc]}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, items} -> {:ok, Enum.reverse(items)}
        {:error, _reason} = error -> error
      end
    else
      {:error, {:history_trim_batch_result_mismatch, expected_count, length(entries)}}
    end
  end

  def decode_lmdb_history_trim_items(entries, expected_count),
    do: {:error, {:invalid_history_trim_batch, expected_count, entries}}

  def history_cap_requirements(entries, load_cap_fun) do
    entries
    |> Enum.reduce(%{}, &put_history_cap_requirement/2)
    |> Enum.reduce(%{}, fn {history_key, state}, acc ->
      cap = state.cap || load_cap_fun.(history_key)

      case cap do
        max_events when is_integer(max_events) and max_events > 0 ->
          Map.put(acc, history_key, {max_events, history_cap_required?(state, max_events)})

        _ ->
          acc
      end
    end)
  end

  def put_history_cap_requirement(%{history_key: history_key} = entry, acc)
      when is_binary(history_key) do
    state =
      Map.get(acc, history_key, %{
        cap: nil,
        max_version: nil,
        unknown_version?: false
      })

    state =
      entry
      |> history_cap_entry_state()
      |> merge_history_cap_entry_state(state)

    Map.put(acc, history_key, state)
  end

  def put_history_cap_requirement(_entry, acc), do: acc

  def history_cap_entry_state(entry) do
    %{
      cap: history_cap_from_entry(entry),
      version: Map.get(entry, :version, :missing)
    }
  end

  def history_cap_from_entry(%{history_max_events: max_events})
      when is_integer(max_events) and max_events > 0,
      do: max_events

  def history_cap_from_entry(_entry), do: nil

  def merge_history_cap_entry_state(%{cap: cap, version: version}, state) do
    state
    |> maybe_put_history_cap(cap)
    |> put_history_cap_version(version)
  end

  def maybe_put_history_cap(state, cap) when is_integer(cap) and cap > 0,
    do: %{state | cap: cap}

  def maybe_put_history_cap(state, _cap), do: state

  def put_history_cap_version(state, version) when is_integer(version) and version >= 0 do
    max_version =
      case state.max_version do
        existing when is_integer(existing) -> max(existing, version)
        _ -> version
      end

    %{state | max_version: max_version}
  end

  def put_history_cap_version(state, _version), do: %{state | unknown_version?: true}

  def history_cap_required?(%{unknown_version?: true}, _max_events), do: true

  def history_cap_required?(%{max_version: version}, max_events)
      when is_integer(version) and version > max_events,
      do: true

  def history_cap_required?(_state, _max_events), do: false

  def decode_lmdb_history_trim_item({history_index_key, value}) do
    case Ferricstore.Flow.LMDB.decode_history_index_value(value) do
      {:ok, {event_id, _event_ms, _expire_at_ms, compound_key}} ->
        {:ok, {event_id, compound_key, history_index_key}}

      _ ->
        {:error, {:invalid_history_trim_index, history_index_key}}
    end
  end

  def decode_lmdb_history_trim_item(invalid),
    do: {:error, {:invalid_history_trim_index_entry, invalid}}

  def trim_history_hot_cache(instance_ctx, shard_index, keydir, entries, callbacks) do
    direct_items = direct_hot_history_evict_items(entries)
    direct_native = history_native_index(instance_ctx, shard_index, direct_items)

    with :ok <-
           evict_hot_history_items(
             direct_items,
             instance_ctx,
             shard_index,
             keydir,
             direct_native,
             callbacks
           ) do
      trim_history_hot_cache_by_rank(
        instance_ctx,
        shard_index,
        keydir,
        history_hot_rank_entries(entries),
        callbacks
      )
    end
  end

  def trim_history_hot_cache_by_rank(_instance_ctx, _shard_index, _keydir, [], _callbacks),
    do: :ok

  def trim_history_hot_cache_by_rank(instance_ctx, shard_index, keydir, entries, callbacks) do
    caps = history_hot_caps(entries)

    if map_size(caps) == 0 do
      :ok
    else
      {flow_index, flow_lookup} =
        NativeFlowIndex.table_names(instance_name(instance_ctx), shard_index)

      native = NativeFlowIndex.get(flow_index, flow_lookup)

      caps
      |> Enum.flat_map(fn {history_key, max_events} ->
        if native do
          count = NativeFlowIndex.count_all(native, history_key)

          if count > max_events do
            native
            |> NativeFlowIndex.rank_range(history_key, 0, count - max_events - 1, false)
            |> Enum.map(fn {event_id, _score} ->
              {history_key, event_id, history_entry_key(history_key, event_id)}
            end)
          else
            []
          end
        else
          []
        end
      end)
      |> evict_hot_history_items(
        instance_ctx,
        shard_index,
        keydir,
        native,
        callbacks
      )
    end
  end

  def history_native_index(_instance_ctx, _shard_index, []), do: nil

  def history_native_index(instance_ctx, shard_index, _items) do
    {flow_index, flow_lookup} =
      NativeFlowIndex.table_names(instance_name(instance_ctx), shard_index)

    NativeFlowIndex.get(flow_index, flow_lookup)
  end

  def direct_hot_history_evict_items(entries) do
    entries
    |> Enum.flat_map(fn entry ->
      entry
      |> direct_hot_history_evict_event_ids()
      |> Enum.map(fn event_id ->
        {entry.history_key, event_id, history_entry_key(entry.history_key, event_id)}
      end)
    end)
    |> Enum.uniq()
  end

  def direct_hot_history_evict_event_ids(%{history_key: history_key, terminal?: true} = entry)
      when is_binary(history_key) do
    entry
    |> Map.get(:hot_evict_event_ids, [])
    |> List.wrap()
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
  end

  def direct_hot_history_evict_event_ids(
        %{history_key: history_key, history_hot_max_events: 0, event_id: event_id} = entry
      )
      when is_binary(history_key) and is_binary(event_id) and event_id != "" do
    entry
    |> Map.get(:hot_evict_event_ids, [])
    |> List.wrap()
    |> then(&[event_id | &1])
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
    |> Enum.uniq()
  end

  def direct_hot_history_evict_event_ids(%{history_key: history_key} = entry)
      when is_binary(history_key) do
    entry
    |> Map.get(:hot_evict_event_ids, [])
    |> List.wrap()
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
  end

  def direct_hot_history_evict_event_ids(_entry), do: []

  def history_hot_rank_entries(entries) do
    Enum.reject(entries, &history_hot_direct_or_under_cap?/1)
  end

  def history_hot_direct_or_under_cap?(%{terminal?: true}), do: false

  def history_hot_direct_or_under_cap?(%{history_hot_max_events: 0}), do: true

  def history_hot_direct_or_under_cap?(%{
        history_hot_max_events: 1,
        version: version,
        hot_evict_event_ids: [_ | _]
      })
      when is_integer(version) and version > 1,
      do: true

  def history_hot_direct_or_under_cap?(%{history_hot_max_events: max_events, version: version})
      when is_integer(max_events) and max_events > 0 and is_integer(version) and
             version <= max_events,
      do: true

  def history_hot_direct_or_under_cap?(_entry), do: false

  def history_hot_caps(entries) do
    Enum.reduce(entries, %{}, fn
      %{history_key: history_key, terminal?: true}, acc when is_binary(history_key) ->
        put_history_hot_cap(acc, history_key, 0)

      %{history_key: history_key, history_hot_max_events: max_events}, acc
      when is_binary(history_key) and is_integer(max_events) and max_events >= 0 ->
        put_history_hot_cap(acc, history_key, max_events)

      _entry, acc ->
        acc
    end)
  end

  def put_history_hot_cap(caps, history_key, max_events) do
    Map.update(caps, history_key, max_events, &min(&1, max_events))
  end

  def evict_hot_history_items(
        [],
        _instance_ctx,
        _shard_index,
        _keydir,
        _native,
        _callbacks
      ),
      do: :ok

  def evict_hot_history_items(
        items,
        instance_ctx,
        shard_index,
        keydir,
        native,
        callbacks
      ) do
    if native do
      NativeFlowIndex.delete_entries(native, history_delete_entries(items))
    end

    Enum.each(items, fn {_history_key, _event_id, key} ->
      callbacks.delete_keydir_row.(instance_ctx, keydir, shard_index, key)
    end)

    :ok
  end

  def history_delete_entries(items) do
    Enum.map(items, fn
      {history_key, event_id, _key} ->
        {history_key, event_id}

      {history_key, event_id, _key, _history_index_key} ->
        {history_key, event_id}
    end)
  end

  def append_tombstones(_file_path, []), do: :ok

  def append_tombstones(file_path, keys, callbacks) do
    ops = Enum.map(keys, &{:delete, &1})

    with {:ok, locations} <- NIF.v2_append_ops_batch(file_path, ops),
         :ok <- validate_tombstone_locations(locations, length(keys)),
         :ok <- callbacks.sync_history_log_before_publish.(file_path) do
      :ok
    end
  end

  def validate_tombstone_locations(locations, expected_count)
      when is_list(locations) and length(locations) == expected_count do
    if Enum.all?(locations, &valid_tombstone_location?/1) do
      :ok
    else
      {:error, {:history_tombstone_batch_result_mismatch, expected_count, locations}}
    end
  end

  def validate_tombstone_locations(locations, expected_count),
    do: {:error, {:history_tombstone_batch_result_mismatch, expected_count, locations}}

  def valid_tombstone_location?({:delete, offset, record_size})
      when is_integer(offset) and offset >= 0 and is_integer(record_size) and record_size > 0,
      do: true

  def valid_tombstone_location?(_location), do: false

  def delete_lmdb_history_entries(_shard_data_path, []), do: :ok

  def delete_lmdb_history_entries(shard_data_path, items) do
    with {:ok, ops} <- lmdb_history_delete_ops(shard_data_path, items) do
      write_lmdb_history_delete_ops(shard_data_path, ops)
    end
  end

  defp lmdb_history_delete_ops(shard_data_path, items) do
    path = Ferricstore.Flow.LMDB.path(shard_data_path)

    items
    |> Enum.reduce_while({:ok, []}, fn item, {:ok, acc} ->
      with {:ok, history_index_key} <- history_index_key_for_delete(item),
           {:ok, item_ops} <-
             Ferricstore.Flow.LMDB.history_index_delete_ops_result(path, history_index_key) do
        {:cont, {:ok, Enum.reverse(item_ops, acc)}}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed_ops} -> {:ok, Enum.reverse(reversed_ops)}
      {:error, _reason} = error -> error
    end
  end

  defp history_index_key_for_delete({_history_key, _event_id, _key, history_index_key})
       when is_binary(history_index_key),
       do: {:ok, history_index_key}

  defp history_index_key_for_delete({history_key, event_id, _key})
       when is_binary(history_key) and is_binary(event_id) do
    case parse_event_ms(event_id) do
      {:ok, event_ms} ->
        {:ok, Ferricstore.Flow.LMDB.history_index_key(history_key, event_id, event_ms)}

      :error ->
        {:error, {:invalid_history_event_id, event_id}}
    end
  end

  defp history_index_key_for_delete(item), do: {:error, {:invalid_history_trim_item, item}}

  defp write_lmdb_history_delete_ops(shard_data_path, ops) do
    path = Ferricstore.Flow.LMDB.path(shard_data_path)
    Ferricstore.Flow.LMDB.write_batch(path, ops)
  end

  def history_entry_key(history_key, event_id), do: "X:" <> history_key <> <<0>> <> event_id

  def parse_event_ms(event_id), do: KeyCodec.parse_event_ms(event_id)

  def instance_name(%{name: name}), do: name
  def instance_name(_instance_ctx), do: :default
end
