defmodule Ferricstore.Flow.HistoryProjector.ValueProjection do
  @moduledoc false

  alias Ferricstore.Flow.HistoryProjector
  alias Ferricstore.Store.BlobRef
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  def compact_projected_flow_values(
         instance_ctx,
         shard_index,
         shard_data_path,
         keydir,
         file_path,
         file_id,
         entries
       ) do
    refs = projected_flow_value_refs(entries)

    keydir_items = projected_flow_value_keydir_items(keydir, refs)

    {direct_segment_items, lmdb_checked_items} =
      Enum.split_with(keydir_items, fn {_ref, file_id} ->
        direct_segment_value_file_id?(file_id)
      end)

    direct_segment_refs = Enum.map(direct_segment_items, &elem(&1, 0))
    lmdb_checked_refs = Enum.map(lmdb_checked_items, &elem(&1, 0))

    # Do not flush the async LMDB writer here. The projector writes value
    # locators itself before removing keydir refs, so waiting behind unrelated
    # state/history LMDB work would turn cold projection into an apply-adjacent
    # latency source.
    {projected_refs, pending_refs} =
      split_projected_flow_value_refs(shard_data_path, lmdb_checked_refs)

    delete_projected_flow_value_keydir_refs(instance_ctx, shard_index, keydir, projected_refs)

    case collect_projected_flow_values(
           instance_ctx,
           shard_index,
           shard_data_path,
           keydir,
           direct_segment_refs ++ pending_refs
         ) do
      [] ->
        :ok

      value_entries ->
        {direct_entries, remaining_entries} =
          Enum.split_with(value_entries, &direct_segment_value_entry?/1)

        {direct_value_entries, copied_entries} =
          Enum.split_with(remaining_entries, &direct_lmdb_value_entry?/1)

        with :ok <- publish_lmdb_direct_value_locations(shard_data_path, direct_entries),
             :ok <-
               delete_projected_flow_value_keydir_rows(
                 instance_ctx,
                 shard_index,
                 keydir,
                 direct_entries,
                 delete_apply_projection_cache?: false
               ),
             :ok <- publish_lmdb_direct_values(shard_data_path, direct_value_entries),
             :ok <-
               delete_projected_flow_value_keydir_rows(
                 instance_ctx,
                 shard_index,
                 keydir,
                 direct_value_entries
               ),
             :ok <-
               copy_projected_flow_values(
                 instance_ctx,
                 shard_index,
                 shard_data_path,
                 keydir,
                 file_path,
                 file_id,
                 copied_entries
               ) do
          :ok
        end
    end
  end

  @doc false
  def __projected_flow_value_keydir_refs_for_test__(keydir, refs) do
    projected_flow_value_keydir_refs(keydir, refs)
  end

  def projected_flow_value_keydir_refs(keydir, refs) do
    keydir
    |> projected_flow_value_keydir_items(refs)
    |> Enum.map(&elem(&1, 0))
  end

  def projected_flow_value_keydir_items(keydir, refs) do
    refs
    |> Enum.reduce([], fn ref, acc ->
      case HistoryProjector.safe_ets_lookup(keydir, ref) do
        [{^ref, _value, _expire_at_ms, _lfu, file_id, offset, value_size}] ->
          if readable_value_locator?(file_id, offset, value_size),
            do: [{ref, file_id} | acc],
            else: acc

        _missing_or_unreadable ->
          acc
      end
    end)
    |> Enum.reverse()
  end

  def direct_segment_value_file_id?({:waraft_segment, index})
       when is_integer(index) and index > 0,
       do: true

  def direct_segment_value_file_id?(_file_id), do: false

  def split_projected_flow_value_refs(shard_data_path, refs) do
    refs = Enum.to_list(refs)

    case refs do
      [] ->
        {[], []}

      [_ | _] ->
        path = Ferricstore.Flow.LMDB.path(shard_data_path)

        case Ferricstore.Flow.LMDB.get_many(path, refs) do
          {:ok, results} when length(results) == length(refs) ->
            now_ms = System.system_time(:millisecond)

            Enum.zip(refs, results)
            |> Enum.reduce({[], []}, fn
              {ref, result}, {projected, pending} ->
                if projected_flow_value_lmdb_live?(result, now_ms) do
                  {[ref | projected], pending}
                else
                  {projected, [ref | pending]}
                end
            end)
            |> then(fn {projected, pending} ->
              {Enum.reverse(projected), Enum.reverse(pending)}
            end)

          _other ->
            {[], refs}
        end
    end
  end

  def projected_flow_value_lmdb_live?({:ok, blob}, now_ms) when is_binary(blob) do
    case Ferricstore.Flow.LMDB.decode_value_locator(blob, now_ms) do
      {:ok, {{:waraft_apply_projection, _index}, _offset, _value_size}} ->
        false

      {:ok, _locator} ->
        true

      :not_locator ->
        match?({:ok, _value}, Ferricstore.Flow.LMDB.decode_value(blob, now_ms))

      _expired_or_error ->
        false
    end
  end

  def projected_flow_value_lmdb_live?(_result, _now_ms), do: false

  def collect_projected_flow_values(instance_ctx, shard_index, shard_data_path, keydir, refs) do
    refs
    |> Enum.map(
      &projected_flow_value_source(instance_ctx, shard_index, shard_data_path, keydir, &1)
    )
    |> materialize_projected_flow_value_sources(instance_ctx, shard_index)
  end

  def projected_flow_value_source(instance_ctx, shard_index, shard_data_path, keydir, key) do
    case HistoryProjector.safe_ets_lookup(keydir, key) do
      [{^key, value, expire_at_ms, lfu, file_id, offset, value_size} = row] ->
        if readable_value_locator?(file_id, offset, value_size) do
          case {value, file_id, value_size} do
            {nil, {tag, _index}, 0} when tag in [:waraft_segment, :waraft_apply_projection] ->
              {:entry,
               projected_flow_value_entry_from_row(
                 key,
                 <<>>,
                 expire_at_ms,
                 lfu,
                 file_id,
                 offset,
                 value_size,
                 row
               )}

            {nil, {:waraft_segment, _index}, _value_size} ->
              {:entry,
               direct_projected_flow_value_entry_from_row(
                 key,
                 expire_at_ms,
                 lfu,
                 file_id,
                 offset,
                 value_size,
                 row
               )}

            _other ->
              case projected_flow_value_bytes(
                     instance_ctx,
                     shard_index,
                     shard_data_path,
                     key,
                     value,
                     file_id,
                     offset
                   ) do
                {:ok, bytes} ->
                  {:entry,
                   projected_flow_value_entry_from_row(
                     key,
                     bytes,
                     expire_at_ms,
                     lfu,
                     file_id,
                     offset,
                     value_size,
                     row
                   )}

                _error ->
                  :skip
              end
          end
        else
          :skip
        end

      _other ->
        :skip
    end
  end

  def materialize_projected_flow_value_sources(sources, _instance_ctx, _shard_index) do
    sources
    |> Enum.reduce([], fn
      {:entry, entry}, acc ->
        [entry | acc]

      _skip, acc ->
        acc
    end)
    |> Enum.reverse()
  end

  def projected_flow_value_entry_from_row(
         key,
         bytes,
         expire_at_ms,
         lfu,
         file_id,
         offset,
         value_size,
         row
       ) do
    %{
      key: key,
      value: bytes,
      expire_at_ms: expire_at_ms,
      lfu: lfu,
      source_file_id: file_id,
      source_offset: offset,
      source_value_size: value_size,
      source_row: row
    }
  end

  def direct_projected_flow_value_entry_from_row(
         key,
         expire_at_ms,
         lfu,
         file_id,
         offset,
         value_size,
         row
       ) do
    %{
      key: key,
      value: nil,
      expire_at_ms: expire_at_ms,
      lfu: lfu,
      source_file_id: file_id,
      source_offset: offset,
      source_value_size: value_size,
      source_row: row
    }
  end

  def projected_flow_value_bytes(
         _instance_ctx,
         _shard_index,
         _shard_data_path,
         _key,
         value,
         _file_id,
         _offset
       )
       when is_binary(value),
       do: {:ok, value}

  def projected_flow_value_bytes(
         _instance_ctx,
         _shard_index,
         shard_data_path,
         key,
         nil,
         file_id,
         offset
       )
       when is_integer(file_id) and file_id >= 0 do
    shard_data_path
    |> ShardETS.file_path(file_id)
    |> Ferricstore.Store.ColdRead.pread_keyed(offset, key, 10_000)
  end

  def projected_flow_value_bytes(
         _instance_ctx,
         _shard_index,
         shard_data_path,
         _key,
         nil,
         {:flow_history, _file_id} = file_id,
         offset
       ) do
    HistoryProjector.read_value(shard_data_path, file_id, offset)
  end

  def projected_flow_value_bytes(
         instance_ctx,
         shard_index,
         _shard_data_path,
         key,
         nil,
         file_id,
         _offset
       )
       when is_tuple(file_id) do
    Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
      instance_ctx,
      shard_index,
      file_id,
      key
    )
  end

  def projected_flow_value_bytes(
         _instance_ctx,
         _shard_index,
         _shard_data_path,
         _key,
         _value,
         _file_id,
         _offset
       ),
       do: :error

  def validate_projected_value_locations(entries, locations)
       when length(entries) == length(locations) do
    if Enum.all?(locations, &valid_projected_value_location?/1) do
      :ok
    else
      {:error, {:flow_value_projection_location_mismatch, length(entries), locations}}
    end
  end

  def validate_projected_value_locations(entries, locations),
    do: {:error, {:flow_value_projection_location_mismatch, length(entries), locations}}

  def valid_projected_value_location?({offset, value_size})
       when is_integer(offset) and offset >= 0 and is_integer(value_size) and value_size >= 0,
       do: true

  def valid_projected_value_location?(_location), do: false

  def direct_segment_value_entry?(%{
         source_file_id: {tag, index},
         source_offset: offset,
         source_value_size: value_size
       })
       when tag == :waraft_segment and is_integer(index) and index > 0 and
              is_integer(offset) and offset >= 0 and is_integer(value_size) and value_size > 0,
       do: true

  def direct_segment_value_entry?(_entry), do: false

  def direct_lmdb_value_entry?(%{value: value, source_value_size: 0}) when is_binary(value),
    do: true

  def direct_lmdb_value_entry?(%{value: value}) when is_binary(value), do: BlobRef.ref?(value)
  def direct_lmdb_value_entry?(_entry), do: false

  def publish_lmdb_direct_value_locations(_shard_data_path, []), do: :ok

  def publish_lmdb_direct_value_locations(shard_data_path, entries) do
    HistoryProjector.write_lmdb_ops(
      shard_data_path,
      Ferricstore.Flow.LMDB.segment_value_pin_batch_put_ops(entries)
    )
  end

  def publish_lmdb_direct_values(_shard_data_path, []), do: :ok

  def publish_lmdb_direct_values(shard_data_path, entries) do
    HistoryProjector.write_lmdb_ops(shard_data_path, direct_lmdb_value_put_ops(entries))
  end

  def direct_lmdb_value_put_ops(entries) do
    Enum.map(entries, fn %{key: key, value: value, expire_at_ms: expire_at_ms} ->
      {:put, key, Ferricstore.Flow.LMDB.encode_value(value, expire_at_ms)}
    end)
  end

  def copy_projected_flow_values(
         _instance_ctx,
         _shard_index,
         _shard_data_path,
         _keydir,
         _file_path,
         _file_id,
         []
       ),
       do: :ok

  def copy_projected_flow_values(
         instance_ctx,
         shard_index,
         shard_data_path,
         keydir,
         file_path,
         file_id,
         value_entries
       ) do
    batch =
      Enum.map(value_entries, fn %{key: key, value: value, expire_at_ms: expire_at_ms} ->
        {key, value, expire_at_ms}
      end)

    with {:ok, locations} <- HistoryProjector.append_batch(file_path, batch),
         :ok <- validate_projected_value_locations(value_entries, locations),
         :ok <- HistoryProjector.sync_history_log_before_publish(file_path),
         :ok <-
           publish_lmdb_value_locations(
             shard_data_path,
             file_id,
             value_entries,
             locations
           ) do
      delete_projected_flow_value_keydir_rows(
        instance_ctx,
        shard_index,
        keydir,
        value_entries
      )
    end
  end

  def publish_lmdb_value_locations(shard_data_path, file_id, entries, locations) do
    HistoryProjector.write_lmdb_ops(shard_data_path, lmdb_value_location_ops(file_id, entries, locations))
  end

  def lmdb_value_location_ops(file_id, entries, locations) do
    entries
    |> Enum.zip(locations)
    |> Enum.map(fn {%{key: key, expire_at_ms: expire_at_ms}, {offset, value_size}} ->
      {:put, key,
       Ferricstore.Flow.LMDB.encode_value_locator(
         expire_at_ms,
         {:flow_history, file_id},
         offset,
         value_size
       )}
    end)
  end

  def delete_projected_flow_value_keydir_rows(instance_ctx, shard_index, keydir, entries) do
    delete_projected_flow_value_keydir_rows(instance_ctx, shard_index, keydir, entries,
      delete_apply_projection_cache?: true
    )
  end

  def delete_projected_flow_value_keydir_rows(instance_ctx, shard_index, keydir, entries, opts) do
    delete_apply_projection_cache? = Keyword.get(opts, :delete_apply_projection_cache?, true)

    {count, bytes} =
      Enum.reduce(entries, {0, 0}, fn %{key: key, value: value, source_row: row},
                                      {count_acc, bytes_acc} ->
        case HistoryProjector.safe_ets_lookup(keydir, key) do
          [^row] ->
            bytes = HistoryProjector.binary_bytes(value)
            HistoryProjector.track_keydir_binary_remove_row(instance_ctx, shard_index, row)

            if delete_apply_projection_cache? do
              HistoryProjector.delete_apply_projection_cache_for_row(instance_ctx, shard_index, row)
            end

            HistoryProjector.safe_ets_delete(keydir, key)
            {count_acc + 1, bytes_acc + bytes}

          _changed ->
            {count_acc, bytes_acc}
        end
      end)

    emit_value_dematerialize(instance_ctx, shard_index, count, bytes)
    :ok
  end

  def delete_projected_flow_value_keydir_refs(_instance_ctx, _shard_index, _keydir, []),
    do: :ok

  def delete_projected_flow_value_keydir_refs(instance_ctx, shard_index, keydir, refs) do
    {count, bytes} =
      Enum.reduce(refs, {0, 0}, fn key, {count_acc, bytes_acc} ->
        case HistoryProjector.safe_ets_lookup(keydir, key) do
          [{^key, value, _expire_at_ms, _lfu, _file_id, _offset, _value_size} = row] ->
            bytes = HistoryProjector.binary_bytes(value)
            HistoryProjector.track_keydir_binary_remove_row(instance_ctx, shard_index, row)
            HistoryProjector.delete_apply_projection_cache_for_row(instance_ctx, shard_index, row)
            HistoryProjector.safe_ets_delete(keydir, key)
            {count_acc + 1, bytes_acc + bytes}

          _missing_or_changed ->
            {count_acc, bytes_acc}
        end
      end)

    emit_value_dematerialize(instance_ctx, shard_index, count, bytes)
    :ok
  end

  def projected_flow_value_refs(entries) do
    entries
    |> Enum.reduce(MapSet.new(), fn entry, acc ->
      entry
      |> entry_flow_value_refs()
      |> Enum.reduce(acc, fn ref, refs ->
        if generated_flow_value_ref?(ref), do: MapSet.put(refs, ref), else: refs
      end)
    end)
  end

  @doc false
  def __projected_flow_value_refs_for_test__(entries), do: projected_flow_value_refs(entries)

  def entry_flow_value_refs(%{value_refs: refs}), do: entry_value_refs(refs)

  def entry_flow_value_refs(%{record: record}) when is_map(record),
    do: record_flow_value_refs(record)

  def entry_flow_value_refs(%{snapshot: snapshot}) do
    :encode_history_snapshot
    |> HistoryProjector.flow_call([snapshot])
    |> history_value_refs_from_encoded()
  rescue
    _ -> []
  end

  def entry_flow_value_refs(%{value: value}) when is_binary(value),
    do: history_value_refs_from_encoded(value)

  def entry_flow_value_refs(_entry), do: []

  def entry_value_refs(refs) when is_list(refs) do
    Enum.filter(refs, &(is_binary(&1) and &1 != ""))
  end

  def entry_value_refs(%{} = refs), do: named_flow_value_refs(refs)

  def entry_value_refs(refs) when is_binary(refs), do: named_flow_value_refs(refs)

  def entry_value_refs(_refs), do: []

  def record_flow_value_refs(record) when is_map(record) do
    [
      Map.get(record, :payload_ref),
      Map.get(record, :result_ref),
      Map.get(record, :error_ref)
      | named_flow_value_refs(Map.get(record, :value_refs))
    ]
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
  end

  def history_value_refs_from_encoded(value) when is_binary(value) do
    :decode_history_fields
    |> HistoryProjector.flow_call([value])
    |> history_fields_to_map()
    |> history_fields_value_refs()
  rescue
    _ -> []
  end

  def history_fields_to_map(fields) when is_list(fields) do
    fields
    |> Enum.chunk_every(2)
    |> Enum.reduce(%{}, fn
      [key, value], acc when is_binary(key) -> Map.put(acc, key, value)
      _field, acc -> acc
    end)
  end

  def history_fields_to_map(_fields), do: %{}

  def history_fields_value_refs(fields) when is_map(fields) do
    [
      Map.get(fields, "payload_ref"),
      Map.get(fields, "result_ref"),
      Map.get(fields, "error_ref")
      | named_flow_value_refs(Map.get(fields, "value_refs"))
    ]
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
  end

  def named_flow_value_refs(%{} = refs) do
    Enum.flat_map(refs, fn
      {_name, %{ref: ref}} when is_binary(ref) -> [ref]
      {_name, %{"ref" => ref}} when is_binary(ref) -> [ref]
      {_name, ref} when is_binary(ref) -> [ref]
      _entry -> []
    end)
  end

  def named_flow_value_refs(value) when is_binary(value) do
    case Jason.decode(value) do
      {:ok, refs} -> named_flow_value_refs(refs)
      _error -> []
    end
  end

  def named_flow_value_refs(_refs), do: []

  def generated_flow_value_ref?("f:" <> _rest = ref) do
    case :binary.split(ref, ":v:") do
      ["f:" <> tag, <<kind, ?:, rest::binary>>]
      when byte_size(tag) > 0 and kind in [?p, ?r, ?e, ?s] and byte_size(rest) > 0 ->
        true

      _other ->
        false
    end
  end

  def generated_flow_value_ref?(_ref), do: false

  def readable_value_locator?(file_id, offset, value_size)
       when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 and
              is_integer(value_size) and value_size >= 0,
       do: true

  def readable_value_locator?({tag, index}, offset, value_size)
       when tag in [:waraft_segment, :waraft_projection, :waraft_apply_projection] and
              is_integer(index) and index > 0 and is_integer(offset) and offset >= 0 and
              is_integer(value_size) and value_size >= 0,
       do: true

  def readable_value_locator?(_file_id, _offset, _value_size), do: false

  def emit_value_dematerialize(_instance_ctx, _shard_index, 0, _bytes), do: :ok

  def emit_value_dematerialize(instance_ctx, shard_index, count, bytes) do
    :telemetry.execute(
      [:ferricstore, :flow, :value_dematerialize],
      %{count: count, bytes: bytes},
      %{instance: HistoryProjector.instance_name(instance_ctx), shard_index: shard_index}
    )
  rescue
    _ -> :ok
  end
end
