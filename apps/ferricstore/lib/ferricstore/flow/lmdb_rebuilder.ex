defmodule Ferricstore.Flow.LMDBRebuilder do
  @moduledoc false

  alias Ferricstore.Flow
  alias Ferricstore.Flow.OrderedIndex, as: FlowIndex
  alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Store.BlobValue
  alias Ferricstore.Store.ColdRead
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.ZSetIndex

  @batch_size 512
  @cold_read_timeout_ms 30_000

  def rebuild_active_indexes_from_keydir(
        shard_path,
        keydir,
        shard_index,
        instance_ctx,
        zset_score_index,
        zset_score_lookup,
        flow_index \\ nil,
        flow_lookup \\ nil
      ) do
    Process.put(:flow_lmdb_rebuild_cold_read_errors, 0)

    stats =
      keydir
      |> select_state_entries()
      |> Enum.reduce(%{seen: 0, active: 0, terminal: 0, cold_read_errors: 0}, fn entries, acc ->
        entries
        |> read_and_decode(shard_path, shard_index, instance_ctx)
        |> Enum.reduce(%{acc | seen: acc.seen + length(entries)}, fn {_key, _value, _expire_at_ms,
                                                                      record},
                                                                     next_acc ->
          if LMDB.terminal_state?(Map.get(record, :state)) do
            %{next_acc | terminal: next_acc.terminal + 1}
          else
            rebuild_active_indexes(zset_score_index, zset_score_lookup, record)
            rebuild_active_flow_indexes(flow_index, flow_lookup, record)
            %{next_acc | active: next_acc.active + 1}
          end
        end)
      end)
      |> Map.put(:cold_read_errors, Process.get(:flow_lmdb_rebuild_cold_read_errors, 0))

    Process.delete(:flow_lmdb_rebuild_cold_read_errors)

    :telemetry.execute(
      [:ferricstore, :flow, :active_index_rebuild],
      stats,
      %{shard_index: shard_index}
    )

    :ok
  end

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
    Process.put(:flow_lmdb_rebuild_cold_read_errors, 0)

    stats =
      keydir
      |> select_state_entries()
      |> Enum.reduce(
        %{
          seen: 0,
          lmdb: 0,
          terminal: 0,
          active: 0,
          lmdb_errors: 0,
          cold_read_errors: 0,
          terminal_counts: %{}
        },
        fn entries, acc ->
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
        end
      )
      |> persist_terminal_counts(lmdb_path)

    lmdb_active_rebuilt =
      rebuild_active_flow_indexes_from_lmdb(
        lmdb_path,
        zset_score_index,
        zset_score_lookup,
        flow_index,
        flow_lookup
      )

    stats =
      stats
      |> Map.put(:lmdb_active_rebuilt, lmdb_active_rebuilt)
      |> Map.put(:cold_read_errors, Process.get(:flow_lmdb_rebuild_cold_read_errors, 0))
      |> Map.put(
        :history,
        rebuild_flow_history_indexes(
          keydir,
          shard_path,
          lmdb_path,
          shard_index,
          instance_ctx,
          flow_index,
          flow_lookup
        )
      )

    Process.delete(:flow_lmdb_rebuild_cold_read_errors)
    publish_mirror_health(instance_ctx, shard_index, stats)

    telemetry_stats =
      stats
      |> Map.put(:terminal_count_keys, map_size(stats.terminal_counts))
      |> Map.delete(:terminal_counts)

    :telemetry.execute(
      [:ferricstore, :flow, :lmdb_rebuild],
      telemetry_stats,
      %{shard_index: shard_index}
    )

    :ok
  end

  defp persist_terminal_counts(%{terminal_counts: counts} = stats, lmdb_path) do
    if map_size(counts) == 0 and not Ferricstore.FS.dir?(lmdb_path) do
      stats
    else
      do_persist_terminal_counts(stats, counts, lmdb_path)
    end
  end

  defp do_persist_terminal_counts(stats, counts, lmdb_path) do
    count_keys =
      lmdb_path
      |> existing_terminal_count_keys()
      |> MapSet.union(MapSet.new(Map.keys(counts)))

    ops =
      Enum.map(count_keys, fn count_key ->
        {:put, count_key, LMDB.encode_count(Map.get(counts, count_key, 0))}
      end)

    case LMDB.write_batch(lmdb_path, ops) do
      :ok ->
        Enum.each(count_keys, fn count_key ->
          LMDB.put_cached_terminal_count_key(lmdb_path, count_key, Map.get(counts, count_key, 0))
        end)

        stats

      {:error, _reason} ->
        %{stats | lmdb_errors: stats.lmdb_errors + 1}
    end
  end

  defp existing_terminal_count_keys(lmdb_path) do
    limit = Application.get_env(:ferricstore, :flow_lmdb_rebuild_count_key_scan_limit, 1_000_000)

    case LMDB.prefix_entries(lmdb_path, LMDB.terminal_count_prefix(), limit) do
      {:ok, entries} -> MapSet.new(entries, fn {key, _value} -> key end)
      {:error, _reason} -> MapSet.new()
    end
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
    decoded = read_and_decode(entries, shard_path, shard_index, instance_ctx)

    {ops, terminal_prunes, active_records, terminal_counts} =
      Enum.reduce(decoded, {[], [], [], acc.terminal_counts}, fn {key, value, expire_at_ms,
                                                                  record},
                                                                 {ops, prunes, active, counts} ->
        lmdb_ops = [{:put, key, LMDB.encode_value(value, expire_at_ms)} | ops]

        if LMDB.terminal_state?(Map.get(record, :state)) do
          index_key =
            Flow.Keys.state_index_key(record.type, record.state, Map.get(record, :partition_key))

          count_key = LMDB.terminal_count_key(index_key)
          updated_at_ms = Map.get(record, :updated_at_ms, 0)

          terminal_key =
            LMDB.terminal_index_key(index_key, record.id, updated_at_ms)

          terminal_value =
            LMDB.encode_terminal_index_value(
              record.id,
              updated_at_ms,
              expire_at_ms,
              key,
              count_key
            )

          terminal_expire_key = LMDB.terminal_expire_key(expire_at_ms, terminal_key)
          terminal_expire_value = LMDB.encode_terminal_expire_value(terminal_key, key, count_key)

          terminal_expire_ops =
            if is_binary(terminal_expire_key) do
              [{:put, terminal_expire_key, terminal_expire_value}]
            else
              []
            end

          reverse_key = LMDB.terminal_by_state_key_key(key)
          metadata_ops = query_metadata_index_ops(record, expire_at_ms)

          {
            [
              {:put, reverse_key, terminal_key},
              {:put, terminal_key, terminal_value}
              | terminal_expire_ops ++ metadata_ops ++ lmdb_ops
            ],
            [{key, record} | prunes],
            active,
            Map.update(counts, count_key, 1, &(&1 + 1))
          }
        else
          {lmdb_ops, prunes, [{key, record} | active], counts}
        end
      end)

    case LMDB.write_batch(lmdb_path, Enum.reverse(ops)) do
      :ok ->
        active_records = Enum.reverse(active_records)

        cleanup_ops =
          cleanup_stale_terminal_reverse_ops(lmdb_path, keydir, shard_path) ++
            Enum.flat_map(active_records, fn {key, record} ->
              cleanup_stale_terminal_ops(lmdb_path, key, record)
            end)

        cleanup_result = LMDB.write_batch(lmdb_path, cleanup_ops)

        Enum.each(active_records, fn {_key, record} ->
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
          active: acc.active + length(active_records),
          lmdb_errors: acc.lmdb_errors + if(cleanup_result == :ok, do: 0, else: 1),
          terminal_counts: terminal_counts
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

  defp read_and_decode(entries, shard_path), do: read_and_decode(entries, shard_path, nil, nil)

  defp read_and_decode(entries, shard_path, shard_index, instance_ctx) do
    {hot, cold} =
      Enum.split_with(entries, fn
        {_key, value, _expire_at_ms, _lfu, _fid, _off, _vsize} when is_binary(value) -> true
        _entry -> false
      end)

    hot_decoded =
      Enum.flat_map(hot, fn {key, value, expire_at_ms, _lfu, _fid, _off, _vsize} ->
        decode_state_record(key, value, expire_at_ms, shard_index, instance_ctx)
      end)

    cold_decoded =
      cold
      |> cold_locations(shard_path)
      |> read_cold_locations(shard_index, instance_ctx)

    hot_decoded ++ cold_decoded
  end

  defp cleanup_stale_terminal_ops(lmdb_path, state_key, record) do
    reverse_key = LMDB.terminal_by_state_key_key(state_key)

    reverse_ops =
      case LMDB.get(lmdb_path, reverse_key) do
        {:ok, terminal_key} when is_binary(terminal_key) ->
          [{:delete, reverse_key}, {:delete, terminal_key}]

        _ ->
          []
      end

    reverse_ops ++ cleanup_stale_terminal_ops_by_id(lmdb_path, state_key, record)
  end

  defp cleanup_stale_terminal_ops_by_id(lmdb_path, state_key, %{id: id, type: type} = record)
       when is_binary(id) and is_binary(type) do
    partition_key = Map.get(record, :partition_key)

    specific_ops =
      ["completed", "failed", "cancelled"]
      |> Enum.flat_map(fn terminal_state ->
        index_key = Flow.Keys.state_index_key(type, terminal_state, partition_key)

        cleanup_stale_terminal_ops_under_prefix(
          lmdb_path,
          LMDB.terminal_index_prefix(index_key),
          id,
          state_key,
          false
        )
      end)

    if specific_ops == [] do
      cleanup_stale_terminal_ops_under_prefix(
        lmdb_path,
        LMDB.terminal_index_global_prefix(),
        id,
        state_key,
        true
      )
    else
      specific_ops
    end
  end

  defp cleanup_stale_terminal_ops_by_id(_lmdb_path, _state_key, _record), do: []

  defp cleanup_stale_terminal_reverse_ops(lmdb_path, keydir, shard_path) do
    case LMDB.prefix_entries(
           lmdb_path,
           LMDB.terminal_by_state_global_prefix(),
           terminal_projection_scan_limit()
         ) do
      {:ok, entries} ->
        Enum.flat_map(entries, fn {reverse_key, terminal_key} ->
          with {:ok, state_key} <- terminal_state_key_from_reverse_key(reverse_key),
               false <- terminal_state_key?(keydir, shard_path, state_key),
               true <- is_binary(terminal_key) do
            [
              {:delete, reverse_key}
              | LMDB.terminal_index_delete_ops(lmdb_path, terminal_key, nil)
            ]
          else
            _ -> []
          end
        end)

      _ ->
        []
    end
  end

  defp terminal_state_key_from_reverse_key(<<"flow-terminal-by-state:", state_key::binary>>)
       when byte_size(state_key) > 0,
       do: {:ok, state_key}

  defp terminal_state_key_from_reverse_key(_reverse_key), do: :error

  defp terminal_state_key?(keydir, shard_path, state_key) when is_binary(state_key) do
    case :ets.lookup(keydir, state_key) do
      [entry] ->
        case read_and_decode([entry], shard_path) do
          [{_key, _value, _expire_at_ms, record}] ->
            LMDB.terminal_state?(Map.get(record, :state))

          _ ->
            false
        end

      _ ->
        false
    end
  end

  defp cleanup_stale_terminal_ops_under_prefix(
         lmdb_path,
         prefix,
         id,
         current_state_key,
         legacy_only?
       ) do
    case LMDB.prefix_entries(lmdb_path, prefix, terminal_projection_scan_limit()) do
      {:ok, entries} ->
        Enum.flat_map(entries, fn {terminal_key, value} ->
          case LMDB.decode_terminal_index_value(value) do
            {:ok, {^id, _updated_at_ms, _expire_at_ms, nil}} ->
              LMDB.terminal_index_delete_ops(lmdb_path, terminal_key, nil)

            {:ok, {^id, _updated_at_ms, _expire_at_ms, ^current_state_key}}
            when is_binary(current_state_key) and not legacy_only? ->
              LMDB.terminal_index_delete_ops(lmdb_path, terminal_key, current_state_key)

            _ ->
              []
          end
        end)

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

  defp read_cold_locations([], _shard_index, _instance_ctx), do: []

  defp read_cold_locations(locations, shard_index, instance_ctx) do
    reads = Enum.map(locations, fn {path, off, key, _expire_at_ms} -> {path, off, key} end)

    case ColdRead.pread_batch_keyed(reads, @cold_read_timeout_ms) do
      {:ok, values} ->
        locations
        |> Enum.zip(values)
        |> Enum.flat_map(fn
          {{_path, _off, key, expire_at_ms}, value} when is_binary(value) ->
            decode_state_record(key, value, expire_at_ms, shard_index, instance_ctx)

          _ ->
            observe_cold_read_error(1, :missing_value)
            []
        end)

      {:error, reason} ->
        observe_cold_read_error(length(locations), reason)
        []
    end
  end

  defp observe_cold_read_error(count, reason) do
    previous = Process.get(:flow_lmdb_rebuild_cold_read_errors, 0)
    Process.put(:flow_lmdb_rebuild_cold_read_errors, previous + count)

    :telemetry.execute(
      [:ferricstore, :flow, :lmdb_rebuild, :cold_read_error],
      %{count: count},
      %{reason: reason}
    )
  end

  defp publish_mirror_health(instance_ctx, shard_index, stats) do
    degraded? = stats.lmdb_errors > 0 or Map.get(stats, :cold_read_errors, 0) > 0
    flag_idx = shard_index + 1

    case Map.get(instance_ctx || %{}, :flow_lmdb_mirror_degraded) do
      ref when is_reference(ref) ->
        if flag_idx <= :atomics.info(ref).size do
          :atomics.put(ref, flag_idx, if(degraded?, do: 1, else: 0))
        end

      _ ->
        :ok
    end
  rescue
    _ -> :ok
  end

  defp decode_state_record(key, value, expire_at_ms, shard_index, instance_ctx) do
    case materialize_rebuilt_value(value, shard_index, instance_ctx) do
      {:ok, materialized_value} ->
        case Flow.decode_record(materialized_value) do
          %{id: id, type: type, state: state} = record
          when is_binary(id) and is_binary(type) and is_binary(state) ->
            [{key, materialized_value, expire_at_ms, record}]

          _ ->
            []
        end

      {:error, reason} ->
        observe_cold_read_error(1, {:blob_materialize_failed, reason})
        []
    end
  rescue
    _ -> []
  end

  defp materialize_rebuilt_value(value, shard_index, %{data_dir: data_dir} = instance_ctx)
       when is_binary(value) and is_binary(data_dir) and is_integer(shard_index) and
              shard_index >= 0 do
    BlobValue.maybe_materialize(
      data_dir,
      shard_index,
      BlobValue.threshold(instance_ctx),
      value
    )
  end

  defp materialize_rebuilt_value(value, _shard_index, _instance_ctx) when is_binary(value),
    do: {:ok, value}

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
    entries = active_flow_index_entries(record)

    Enum.each(entries, fn {key, member, score} ->
      FlowIndex.put_member(flow_index, flow_lookup, key, member, score)
    end)

    case NativeFlowIndex.get(flow_index, flow_lookup) do
      nil -> :ok
      native -> NativeFlowIndex.put_entries(native, entries)
    end
  end

  defp rebuild_active_flow_indexes_from_lmdb(
         lmdb_path,
         zset_score_index,
         zset_score_lookup,
         flow_index,
         flow_lookup
       ) do
    now_ms = Ferricstore.CommandTime.now_ms()

    case LMDB.prefix_entries(lmdb_path, "f:", lmdb_state_rebuild_scan_limit()) do
      {:ok, entries} ->
        Enum.reduce(entries, 0, fn {key, blob}, count ->
          with true <- Flow.Keys.state_key?(key),
               {:ok, value} <- LMDB.decode_value(blob, now_ms),
               %{state: state} = record <- Flow.decode_record(value),
               false <- LMDB.terminal_state?(state) do
            rebuild_active_indexes(zset_score_index, zset_score_lookup, record)
            rebuild_active_flow_indexes(flow_index, flow_lookup, record)
            count + 1
          else
            _ -> count
          end
        end)

      _ ->
        0
    end
  end

  defp active_flow_index_entries(record) do
    partition_key = Map.get(record, :partition_key)
    updated_score = Map.get(record, :updated_at_ms, 0)
    state_index_key = Flow.Keys.state_index_key(record.type, record.state, partition_key)

    [{state_index_key, record.id, updated_score}]
    |> maybe_add_due_index_entry(record, partition_key)
    |> maybe_add_running_index_entries(record, partition_key)
  end

  defp maybe_add_due_index_entry(
         entries,
         %{next_run_at_ms: next_run_at_ms} = record,
         partition_key
       )
       when is_integer(next_run_at_ms) do
    priority = Map.get(record, :priority, 0)
    due_key = Flow.Keys.due_key(record.type, record.state, priority, partition_key)

    [{due_key, record.id, next_run_at_ms} | entries]
  end

  defp maybe_add_due_index_entry(entries, _record, _partition_key), do: entries

  defp maybe_add_running_index_entries(
         entries,
         %{state: "running", lease_deadline_ms: lease_deadline_ms} = record,
         partition_key
       )
       when is_integer(lease_deadline_ms) do
    inflight_key = Flow.Keys.inflight_index_key(record.type, partition_key)
    worker_key = Flow.Keys.worker_index_key(Map.get(record, :lease_owner, ""), partition_key)

    [
      {worker_key, record.id, lease_deadline_ms},
      {inflight_key, record.id, lease_deadline_ms}
      | entries
    ]
  end

  defp maybe_add_running_index_entries(entries, _record, _partition_key), do: entries

  defp rebuild_flow_history_indexes(
         _keydir,
         _shard_path,
         _lmdb_path,
         _shard_index,
         _instance_ctx,
         nil,
         _flow_lookup
       ),
       do: 0

  defp rebuild_flow_history_indexes(
         _keydir,
         _shard_path,
         _lmdb_path,
         _shard_index,
         _instance_ctx,
         _flow_index,
         nil
       ),
       do: 0

  defp rebuild_flow_history_indexes(
         keydir,
         shard_path,
         lmdb_path,
         shard_index,
         instance_ctx,
         flow_index,
         flow_lookup
       ) do
    {count, history_entries_by_key} =
      :ets.foldl(
        fn
          {key, _value, _expire_at_ms, _lfu, _fid, _off, _vsize}, {count, history_entries_by_key}
          when is_binary(key) ->
            case parse_flow_history_entry_key(key) do
              {:ok, history_key, event_id, event_ms} ->
                FlowIndex.put_member(flow_index, flow_lookup, history_key, event_id, event_ms)

                entry = {event_id, event_ms, key}

                {count + 1,
                 Map.update(history_entries_by_key, history_key, [entry], &[entry | &1])}

              :skip ->
                {count, history_entries_by_key}
            end

          _entry, acc ->
            acc
        end,
        {0, %{}},
        keydir
      )

    history_keys = Map.keys(history_entries_by_key)

    trim_rebuilt_flow_history_indexes(
      keydir,
      shard_path,
      lmdb_path,
      shard_index,
      instance_ctx,
      flow_index,
      flow_lookup,
      history_keys
    )

    rebuild_lmdb_history_projections(
      keydir,
      shard_path,
      lmdb_path,
      shard_index,
      instance_ctx,
      history_entries_by_key
    )

    count
  end

  defp trim_rebuilt_flow_history_indexes(
         keydir,
         shard_path,
         lmdb_path,
         shard_index,
         instance_ctx,
         flow_index,
         flow_lookup,
         history_keys
       ) do
    Enum.each(history_keys, fn history_key ->
      case history_max_events_for_key(
             keydir,
             shard_path,
             lmdb_path,
             shard_index,
             instance_ctx,
             history_key
           ) do
        max when is_integer(max) and max > 0 ->
          case FlowIndex.count_all(flow_lookup, history_key) do
            count when count > max ->
              delete_count = count - max

              event_ids =
                flow_index
                |> FlowIndex.rank_range(history_key, 0, delete_count - 1, false)
                |> Enum.map(fn {event_id, _score} -> event_id end)

              FlowIndex.delete_members(flow_index, flow_lookup, history_key, event_ids)

            _ ->
              :ok
          end

        _ ->
          :ok
      end
    end)

    :ok
  end

  defp rebuild_lmdb_history_projections(
         keydir,
         shard_path,
         lmdb_path,
         shard_index,
         instance_ctx,
         history_entries_by_key
       ) do
    Enum.each(history_entries_by_key, fn {history_key, entries} ->
      with {:ok, state_key} <- state_key_from_history_key(history_key),
           {:ok, record} <-
             read_rebuild_state_record(
               keydir,
               shard_path,
               lmdb_path,
               state_key,
               shard_index,
               instance_ctx
             ),
           true <- LMDB.terminal_state?(Map.get(record, :state)) do
        rebuild_lmdb_history_projection(lmdb_path, history_key, record, entries)
      else
        _ -> :ok
      end
    end)
  end

  defp rebuild_lmdb_history_projection(lmdb_path, history_key, record, entries) do
    expire_at_ms = flow_record_expire_at(record)

    delete_ops =
      lmdb_path
      |> LMDB.prefix_entries(
        history_key |> LMDB.history_index_prefix(),
        history_projection_scan_limit()
      )
      |> case do
        {:ok, existing} ->
          Enum.flat_map(existing, fn {history_index_key, _value} ->
            LMDB.history_index_delete_ops(lmdb_path, history_index_key)
          end)

        {:error, _reason} ->
          []
      end

    put_ops =
      entries
      |> Enum.sort_by(fn {event_id, event_ms, _compound_key} -> {event_ms, event_id} end)
      |> take_latest_history_events(Map.get(record, :history_max_events))
      |> Enum.flat_map(fn {event_id, event_ms, compound_key} ->
        history_index_key = LMDB.history_index_key(history_key, event_id, event_ms)

        ops = [
          {:put, history_index_key,
           LMDB.encode_history_index_value(event_id, event_ms, compound_key, expire_at_ms)}
        ]

        ops =
          case LMDB.history_expire_key(expire_at_ms, history_index_key) do
            nil ->
              ops

            expire_key ->
              [{:put, expire_key, LMDB.encode_history_expire_value(history_index_key)} | ops]
          end

        case LMDB.history_flow_expire_key(expire_at_ms, history_key) do
          nil ->
            ops

          expire_key ->
            [
              {:put, expire_key, LMDB.encode_history_flow_expire_value(history_key, expire_at_ms)}
              | ops
            ]
        end
      end)

    (delete_ops ++ put_ops)
    |> Enum.chunk_every(@batch_size)
    |> Enum.each(fn ops -> LMDB.write_batch(lmdb_path, ops) end)
  end

  defp take_latest_history_events(entries, max) when is_integer(max) and max > 0,
    do: Enum.take(entries, -max)

  defp take_latest_history_events(entries, _max), do: entries

  defp history_projection_scan_limit do
    Application.get_env(:ferricstore, :flow_lmdb_history_rebuild_scan_limit, 1_000_000)
  end

  defp terminal_projection_scan_limit do
    Application.get_env(:ferricstore, :flow_lmdb_terminal_rebuild_scan_limit, 1_000_000)
  end

  defp lmdb_state_rebuild_scan_limit do
    Application.get_env(:ferricstore, :flow_lmdb_state_rebuild_scan_limit, 1_000_000)
  end

  defp flow_record_expire_at(%{terminal_retention_until_ms: expire_at_ms})
       when is_integer(expire_at_ms),
       do: expire_at_ms

  defp flow_record_expire_at(_record), do: 0

  defp history_max_events_for_key(
         keydir,
         shard_path,
         lmdb_path,
         shard_index,
         instance_ctx,
         history_key
       ) do
    with {:ok, state_key} <- state_key_from_history_key(history_key),
         {:ok, record} <-
           read_rebuild_state_record(
             keydir,
             shard_path,
             lmdb_path,
             state_key,
             shard_index,
             instance_ctx
           ) do
      Map.get(record, :history_max_events)
    else
      _ -> nil
    end
  end

  defp state_key_from_history_key(history_key) when is_binary(history_key) do
    case :binary.split(history_key, "}:h:") do
      [prefix, id] -> {:ok, prefix <> "}:s:" <> id}
      _ -> :error
    end
  end

  defp state_key_from_history_key(_history_key), do: :error

  defp read_rebuild_state_record(
         keydir,
         shard_path,
         lmdb_path,
         state_key,
         shard_index,
         instance_ctx
       ) do
    case :ets.lookup(keydir, state_key) do
      [{^state_key, value, expire_at_ms, _lfu, _fid, _off, _vsize}] when is_binary(value) ->
        case decode_state_record(state_key, value, expire_at_ms, shard_index, instance_ctx) do
          [{_key, _value, _expire_at_ms, record}] -> {:ok, record}
          _ -> :error
        end

      [
        {^state_key, nil, expire_at_ms, _lfu, fid, off, vsize}
      ]
      when is_integer(fid) and is_integer(off) and is_integer(vsize) and off >= 0 and vsize >= 0 ->
        shard_path
        |> cold_locations_for_state(state_key, expire_at_ms, fid, off, vsize)
        |> read_cold_locations(shard_index, instance_ctx)
        |> case do
          [{_key, _value, _expire_at_ms, record}] -> {:ok, record}
          _ -> :error
        end

      _ ->
        read_lmdb_rebuild_state_record(lmdb_path, state_key)
    end
  end

  defp read_lmdb_rebuild_state_record(lmdb_path, state_key) do
    with {:ok, blob} <- LMDB.get(lmdb_path, state_key),
         {:ok, value} <- LMDB.decode_value(blob, System.system_time(:millisecond)),
         %{id: id} = record <- Flow.decode_record(value),
         true <- is_binary(id) do
      {:ok, record}
    else
      _ -> :error
    end
  rescue
    _ -> :error
  end

  defp cold_locations_for_state(shard_path, state_key, expire_at_ms, fid, off, vsize) do
    cold_locations([{state_key, nil, expire_at_ms, nil, fid, off, vsize}], shard_path)
  end

  defp parse_flow_history_entry_key("X:" <> rest) do
    case :binary.split(rest, <<0>>) do
      [history_key, event_id] ->
        with true <- String.starts_with?(history_key, "f:{f"),
             true <- String.contains?(history_key, "}:h:"),
             {:ok, event_ms} <- parse_history_event_ms(event_id) do
          {:ok, history_key, event_id, event_ms}
        else
          _ -> :skip
        end

      _ ->
        :skip
    end
  end

  defp parse_flow_history_entry_key(_key), do: :skip

  defp parse_history_event_ms(event_id) do
    case String.split(event_id, "-", parts: 2) do
      [ms, _seq] ->
        case Integer.parse(ms) do
          {value, ""} when value >= 0 -> {:ok, value}
          _ -> :error
        end

      _ ->
        :error
    end
  end

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
      state_key = Flow.Keys.state_key(record.id, partition_key)
      value = LMDB.encode_query_index_value(record.id, score, expire_at_ms, state_key)
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
