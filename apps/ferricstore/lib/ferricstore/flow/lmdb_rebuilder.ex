defmodule Ferricstore.Flow.LMDBRebuilder do
  @moduledoc false

  alias Ferricstore.Flow
  alias Ferricstore.Flow.Index, as: FlowIndex
  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Store.ColdRead
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.ZSetIndex

  @batch_size 512
  @cold_read_timeout_ms 30_000

  def reconcile_shard(
        shard_path,
        keydir,
        shard_index,
        instance_ctx,
        zset_score_index,
        zset_score_lookup,
        flow_index \\ nil,
        flow_lookup \\ nil
      ) do
    lmdb_path = LMDB.path(shard_path)

    stats =
      keydir
      |> select_state_entries()
      |> Enum.reduce(%{seen: 0, lmdb: 0, terminal: 0, active: 0, lmdb_errors: 0}, fn entries,
                                                                                     acc ->
        reconcile_batch(
          entries,
          shard_path,
          lmdb_path,
          keydir,
          shard_index,
          instance_ctx,
          zset_score_index,
          zset_score_lookup,
          flow_index,
          flow_lookup,
          acc
        )
      end)

    :telemetry.execute(
      [:ferricstore, :flow, :lmdb_rebuild],
      stats,
      %{shard_index: shard_index}
    )

    :ok
  end

  defp reconcile_batch(
         entries,
         shard_path,
         lmdb_path,
         keydir,
         shard_index,
         instance_ctx,
         zset_score_index,
         zset_score_lookup,
         flow_index,
         flow_lookup,
         acc
       ) do
    decoded = read_and_decode(entries, shard_path)

    {ops, terminal_prunes, active_records} =
      Enum.reduce(decoded, {[], [], []}, fn {key, value, expire_at_ms, record},
                                            {ops, prunes, active} ->
        lmdb_ops = [{:put, key, LMDB.encode_value(value, expire_at_ms)} | ops]

        if LMDB.terminal_state?(Map.get(record, :state)) do
          index_key =
            Flow.Keys.state_index_key(record.type, record.state, Map.get(record, :partition_key))

          updated_at_ms = Map.get(record, :updated_at_ms, 0)

          terminal_key =
            LMDB.terminal_index_key(index_key, record.id, updated_at_ms)

          terminal_value =
            LMDB.encode_terminal_index_value(record.id, updated_at_ms, expire_at_ms, key)

          reverse_key = LMDB.terminal_by_state_key_key(key)
          metadata_ops = query_metadata_index_ops(record, expire_at_ms)

          {
            [
              {:put, reverse_key, terminal_key},
              {:put, terminal_key, terminal_value}
              | metadata_ops ++ lmdb_ops
            ],
            [{key, record} | prunes],
            active
          }
        else
          {query_metadata_index_ops(record, expire_at_ms) ++
             cleanup_stale_terminal_ops(lmdb_path, key) ++ lmdb_ops, prunes,
           [{key, record} | active]}
        end
      end)

    case LMDB.write_batch(lmdb_path, Enum.reverse(ops)) do
      :ok ->
        Enum.each(Enum.reverse(active_records), fn {_key, record} ->
          rebuild_active_indexes(zset_score_index, zset_score_lookup, record)
          rebuild_active_flow_indexes(flow_index, flow_lookup, record)
        end)

        Enum.each(Enum.reverse(terminal_prunes), fn {key, _record} ->
          track_binary_remove(keydir, shard_index, key, instance_ctx)
          :ets.delete(keydir, key)
        end)

        %{
          seen: acc.seen + length(entries),
          lmdb: acc.lmdb + length(decoded),
          terminal: acc.terminal + length(terminal_prunes),
          active: acc.active + length(active_records)
        }

      {:error, _reason} ->
        Enum.each(Enum.reverse(active_records), fn {_key, record} ->
          rebuild_active_indexes(zset_score_index, zset_score_lookup, record)
          rebuild_active_flow_indexes(flow_index, flow_lookup, record)
        end)

        %{
          acc
          | seen: acc.seen + length(entries),
            active: acc.active + length(active_records),
            lmdb_errors: acc.lmdb_errors + 1
        }
    end
  end

  defp read_and_decode(entries, shard_path) do
    {hot, cold} =
      Enum.split_with(entries, fn
        {_key, value, _expire_at_ms, _lfu, _fid, _off, _vsize} when is_binary(value) -> true
        _entry -> false
      end)

    hot_decoded =
      Enum.flat_map(hot, fn {key, value, expire_at_ms, _lfu, _fid, _off, _vsize} ->
        decode_state_record(key, value, expire_at_ms)
      end)

    cold_decoded =
      cold
      |> cold_locations(shard_path)
      |> read_cold_locations()

    hot_decoded ++ cold_decoded
  end

  defp cleanup_stale_terminal_ops(lmdb_path, state_key) do
    reverse_key = LMDB.terminal_by_state_key_key(state_key)

    case LMDB.get(lmdb_path, reverse_key) do
      {:ok, terminal_key} when is_binary(terminal_key) ->
        [{:delete, reverse_key}, {:delete, terminal_key}]

      _ ->
        []
    end
  end

  defp cold_locations(entries, shard_path) do
    Enum.flat_map(entries, fn
      {key, nil, expire_at_ms, _lfu, fid, off, vsize}
      when is_integer(fid) and is_integer(off) and is_integer(vsize) and off >= 0 and vsize >= 0 ->
        path = ShardETS.file_path(shard_path, fid)
        [{path, off, key, expire_at_ms}]

      _entry ->
        []
    end)
  end

  defp read_cold_locations([]), do: []

  defp read_cold_locations(locations) do
    reads = Enum.map(locations, fn {path, off, key, _expire_at_ms} -> {path, off, key} end)

    case ColdRead.pread_batch_keyed(reads, @cold_read_timeout_ms) do
      {:ok, values} ->
        locations
        |> Enum.zip(values)
        |> Enum.flat_map(fn
          {{_path, _off, key, expire_at_ms}, value} when is_binary(value) ->
            decode_state_record(key, value, expire_at_ms)

          _ ->
            []
        end)

      {:error, _reason} ->
        []
    end
  end

  defp decode_state_record(key, value, expire_at_ms) do
    case Flow.decode_record(value) do
      %{id: id, type: type, state: state} = record
      when is_binary(id) and is_binary(type) and is_binary(state) ->
        [{key, value, expire_at_ms, record}]

      _ ->
        []
    end
  rescue
    _ -> []
  end

  defp rebuild_active_indexes(zset_score_index, zset_score_lookup, record) do
    partition_key = Map.get(record, :partition_key)
    updated_score = score_string(Map.get(record, :updated_at_ms, 0))
    state_index_key = Flow.Keys.state_index_key(record.type, record.state, partition_key)

    ZSetIndex.put_member(
      zset_score_index,
      zset_score_lookup,
      state_index_key,
      record.id,
      updated_score
    )

    maybe_rebuild_due_index(zset_score_index, zset_score_lookup, record)
    maybe_rebuild_running_indexes(zset_score_index, zset_score_lookup, record)
  end

  defp maybe_rebuild_due_index(
         zset_score_index,
         zset_score_lookup,
         %{next_run_at_ms: next_run_at_ms} = record
       )
       when is_integer(next_run_at_ms) do
    partition_key = Map.get(record, :partition_key)
    priority = Map.get(record, :priority, 0)
    due_key = Flow.Keys.due_key(record.type, record.state, priority, partition_key)

    ZSetIndex.put_member(
      zset_score_index,
      zset_score_lookup,
      due_key,
      record.id,
      score_string(next_run_at_ms)
    )
  end

  defp maybe_rebuild_due_index(_zset_score_index, _zset_score_lookup, _record), do: :ok

  defp maybe_rebuild_running_indexes(
         zset_score_index,
         zset_score_lookup,
         %{state: "running", lease_deadline_ms: lease_deadline_ms} = record
       )
       when is_integer(lease_deadline_ms) do
    partition_key = Map.get(record, :partition_key)
    score = score_string(lease_deadline_ms)
    inflight_key = Flow.Keys.inflight_index_key(record.type, partition_key)
    worker_key = Flow.Keys.worker_index_key(Map.get(record, :lease_owner, ""), partition_key)

    ZSetIndex.put_member(zset_score_index, zset_score_lookup, inflight_key, record.id, score)
    ZSetIndex.put_member(zset_score_index, zset_score_lookup, worker_key, record.id, score)
  end

  defp maybe_rebuild_running_indexes(_zset_score_index, _zset_score_lookup, _record), do: :ok

  defp rebuild_active_flow_indexes(nil, _flow_lookup, _record), do: :ok
  defp rebuild_active_flow_indexes(_flow_index, nil, _record), do: :ok

  defp rebuild_active_flow_indexes(flow_index, flow_lookup, record) do
    partition_key = Map.get(record, :partition_key)
    updated_score = Map.get(record, :updated_at_ms, 0)
    state_index_key = Flow.Keys.state_index_key(record.type, record.state, partition_key)

    FlowIndex.put_member(flow_index, flow_lookup, state_index_key, record.id, updated_score)
    maybe_rebuild_flow_due_index(flow_index, flow_lookup, record)
    maybe_rebuild_flow_running_indexes(flow_index, flow_lookup, record)
  end

  defp maybe_rebuild_flow_due_index(
         flow_index,
         flow_lookup,
         %{next_run_at_ms: next_run_at_ms} = record
       )
       when is_integer(next_run_at_ms) do
    partition_key = Map.get(record, :partition_key)
    priority = Map.get(record, :priority, 0)
    due_key = Flow.Keys.due_key(record.type, record.state, priority, partition_key)

    FlowIndex.put_member(flow_index, flow_lookup, due_key, record.id, next_run_at_ms)
  end

  defp maybe_rebuild_flow_due_index(_flow_index, _flow_lookup, _record), do: :ok

  defp maybe_rebuild_flow_running_indexes(
         flow_index,
         flow_lookup,
         %{state: "running", lease_deadline_ms: lease_deadline_ms} = record
       )
       when is_integer(lease_deadline_ms) do
    partition_key = Map.get(record, :partition_key)
    inflight_key = Flow.Keys.inflight_index_key(record.type, partition_key)
    worker_key = Flow.Keys.worker_index_key(Map.get(record, :lease_owner, ""), partition_key)

    FlowIndex.put_member(flow_index, flow_lookup, inflight_key, record.id, lease_deadline_ms)
    FlowIndex.put_member(flow_index, flow_lookup, worker_key, record.id, lease_deadline_ms)
  end

  defp maybe_rebuild_flow_running_indexes(_flow_index, _flow_lookup, _record), do: :ok

  defp query_metadata_index_ops(record, expire_at_ms) do
    partition_key = Map.get(record, :partition_key)
    score = Map.get(record, :updated_at_ms, 0)

    metadata_index_entries(record)
    |> Enum.map(fn {kind, value} ->
      key =
        case kind do
          :parent -> Flow.Keys.parent_index_key(value, partition_key)
          :root -> Flow.Keys.root_index_key(value, partition_key)
          :correlation -> Flow.Keys.correlation_index_key(value, partition_key)
        end

      query_key = LMDB.query_index_key(key, record.id, score)
      value = LMDB.encode_query_index_value(record.id, score, expire_at_ms)
      {:put, query_key, value}
    end)
  end

  defp metadata_index_entries(record) do
    [
      {:parent, Map.get(record, :parent_flow_id)},
      {:root, non_default_root_flow_id(record)},
      {:correlation, Map.get(record, :correlation_id)}
    ]
    |> Enum.filter(fn {_kind, value} -> is_binary(value) and value != "" end)
  end

  defp non_default_root_flow_id(record) do
    id = Map.get(record, :id)

    case Map.get(record, :root_flow_id) do
      root_flow_id when root_flow_id in [nil, "", id] -> nil
      root_flow_id -> root_flow_id
    end
  end

  defp score_string(value) when is_integer(value), do: Float.to_string(value * 1.0)
  defp score_string(value) when is_float(value), do: Float.to_string(value)
  defp score_string(_value), do: "0.0"

  defp select_state_entries(keydir) do
    Stream.resource(
      fn -> :start end,
      fn
        :done ->
          {:halt, :done}

        :start ->
          select_next(:ets.select(keydir, keydir_match_spec(), @batch_size))

        continuation ->
          select_next(:ets.select(continuation))
      end,
      fn _ -> :ok end
    )
  end

  defp select_next(:"$end_of_table"), do: {:halt, :done}

  defp select_next({entries, continuation}) do
    state_entries = Enum.filter(entries, &flow_state_entry?/1)

    if state_entries == [] do
      {[], continuation}
    else
      {[state_entries], continuation}
    end
  end

  defp keydir_match_spec do
    [
      {{:"$1", :"$2", :"$3", :"$4", :"$5", :"$6", :"$7"}, [],
       [{{:"$1", :"$2", :"$3", :"$4", :"$5", :"$6", :"$7"}}]}
    ]
  end

  defp flow_state_entry?({key, _value, _expire_at_ms, _lfu, _fid, _off, _vsize})
       when is_binary(key) do
    Flow.Keys.state_key?(key)
  end

  defp flow_state_entry?(_entry), do: false

  defp track_binary_remove(keydir, shard_index, key, instance_ctx) do
    ref = keydir_binary_ref(instance_ctx, shard_index)

    if ref do
      bytes =
        case :ets.lookup(keydir, key) do
          [{^key, value, _expire_at_ms, _lfu, _fid, _off, _vsize}] ->
            offheap_size(key) + offheap_size(value)

          _ ->
            0
        end

      if bytes > 0, do: :atomics.sub(ref, shard_index + 1, bytes)
    end
  end

  defp keydir_binary_ref(%{keydir_binary_bytes: ref, shard_count: count}, shard_index)
       when ref != nil do
    if shard_index < count, do: ref, else: nil
  end

  defp keydir_binary_ref(_instance_ctx, _shard_index), do: nil

  defp offheap_size(value) when is_binary(value) and byte_size(value) > 64, do: byte_size(value)
  defp offheap_size(_value), do: 0
end
