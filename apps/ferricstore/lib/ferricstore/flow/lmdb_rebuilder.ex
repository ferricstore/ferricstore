defmodule Ferricstore.Flow.LMDBRebuilder do
  @moduledoc false

  alias Ferricstore.Flow
  alias Ferricstore.Flow.LMDBRebuilder.ActiveIndexes
  alias Ferricstore.Flow.LMDBRebuilder.ColdState
  alias Ferricstore.Flow.LMDBRebuilder.TerminalProjection
  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex

  @batch_size 512

  @doc false
  def __startup_active_rebuild_concurrency_for_test__,
    do: ActiveIndexes.startup_active_rebuild_concurrency()

  @doc false
  def __with_startup_active_rebuild_slot_for_test__(fun) when is_function(fun, 0),
    do: ActiveIndexes.with_startup_active_rebuild_slot(fun)

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

    try do
      stats =
        keydir
        |> select_state_entries()
        |> Enum.reduce(%{seen: 0, active: 0, terminal: 0, cold_read_errors: 0}, fn entries, acc ->
          entries
          |> ColdState.read_and_decode(shard_path, shard_index, instance_ctx)
          |> Enum.reduce(%{acc | seen: acc.seen + length(entries)}, fn {_key, _value,
                                                                        _expire_at_ms, record},
                                                                       next_acc ->
            if LMDB.terminal_state?(Map.get(record, :state)) do
              %{next_acc | terminal: next_acc.terminal + 1}
            else
              ActiveIndexes.rebuild_score_indexes(zset_score_index, zset_score_lookup, record)
              ActiveIndexes.rebuild_flow_indexes(flow_index, flow_lookup, record)
              %{next_acc | active: next_acc.active + 1}
            end
          end)
        end)
        |> Map.put(:cold_read_errors, Process.get(:flow_lmdb_rebuild_cold_read_errors, 0))

      :telemetry.execute(
        [:ferricstore, :flow, :active_index_rebuild],
        stats,
        %{shard_index: shard_index}
      )

      :ok
    after
      Process.delete(:flow_lmdb_rebuild_cold_read_errors)
    end
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

    try do
      stats =
        keydir
        |> select_state_entries()
        |> Enum.reduce(
          initial_reconcile_stats(lmdb_path, keydir, shard_path),
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
        |> TerminalProjection.persist_terminal_counts(lmdb_path)

      lmdb_active_rebuilt =
        ActiveIndexes.rebuild_flow_indexes_from_lmdb(
          lmdb_path,
          zset_score_index,
          zset_score_lookup,
          flow_index,
          flow_lookup
        )

      {history_count, history_lmdb_errors} =
        maybe_rebuild_flow_history_indexes(
          keydir,
          shard_path,
          lmdb_path,
          shard_index,
          instance_ctx,
          flow_index,
          flow_lookup
        )

      stats =
        stats
        |> Map.put(:lmdb_active_rebuilt, lmdb_active_rebuilt)
        |> Map.put(:cold_read_errors, Process.get(:flow_lmdb_rebuild_cold_read_errors, 0))
        |> Map.put(:history, history_count)
        |> Map.put(:history_lmdb_errors, history_lmdb_errors)
        |> Map.update!(:lmdb_errors, &(&1 + history_lmdb_errors))

      ColdState.publish_mirror_health(instance_ctx, shard_index, stats)

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
    after
      Process.delete(:flow_lmdb_rebuild_cold_read_errors)
    end
  end

  def reconcile_startup_shard(
        shard_path,
        keydir,
        shard_index,
        instance_ctx,
        zset_score_index,
        zset_score_lookup,
        flow_index,
        flow_lookup,
        opts \\ []
      )

  def reconcile_startup_shard(
        shard_path,
        keydir,
        shard_index,
        %{name: :default} = instance_ctx,
        zset_score_index,
        zset_score_lookup,
        flow_index,
        flow_lookup,
        opts
      ) do
    lmdb_path = LMDB.path(shard_path)

    cond do
      Keyword.get(opts, :force_full_reconcile?, false) ->
        flush_marker? = LMDB.flush_in_progress?(lmdb_path)

        :ok =
          reconcile_shard(
            shard_path,
            keydir,
            shard_index,
            instance_ctx,
            zset_score_index,
            zset_score_lookup,
            flow_index,
            flow_lookup
          )

        if flush_marker? do
          # The forced path is used after WARaft segment replay mutates the keydir.
          # If a previous LMDB flush marker also survived a crash, the full
          # reconcile repaired the projection and can clear it now.
          :ok = LMDB.write_batch(lmdb_path, [LMDB.flush_in_progress_delete_op()])
        end

        :telemetry.execute(
          [:ferricstore, :flow, :lmdb_startup_rebuild],
          %{lmdb_active_rebuilt: 0, full_reconcile: 1},
          %{
            shard_index: shard_index,
            mode: :default_waraft_active_projection,
            reason: Keyword.get(opts, :reason, :forced_full_reconcile)
          }
        )

        :ok

      LMDB.flush_in_progress?(lmdb_path) ->
        :ok =
          reconcile_shard(
            shard_path,
            keydir,
            shard_index,
            instance_ctx,
            zset_score_index,
            zset_score_lookup,
            flow_index,
            flow_lookup
          )

        # A crash can leave the marker behind after LMDB has only part of one
        # logical projection. Full reconcile repairs from the durable Flow source,
        # then clears the marker so future boots can take the cheap active rebuild.
        :ok = LMDB.write_batch(lmdb_path, [LMDB.flush_in_progress_delete_op()])

        :telemetry.execute(
          [:ferricstore, :flow, :lmdb_startup_rebuild],
          %{lmdb_active_rebuilt: 0, full_reconcile: 1},
          %{
            shard_index: shard_index,
            mode: :default_waraft_active_projection,
            reason: :incomplete_lmdb_flush
          }
        )

        :ok

      not LMDB.env_present?(lmdb_path) ->
        :ok =
          reconcile_shard(
            shard_path,
            keydir,
            shard_index,
            instance_ctx,
            zset_score_index,
            zset_score_lookup,
            flow_index,
            flow_lookup
          )

        :telemetry.execute(
          [:ferricstore, :flow, :lmdb_startup_rebuild],
          %{lmdb_active_rebuilt: 0, full_reconcile: 1},
          %{
            shard_index: shard_index,
            mode: :default_waraft_active_projection,
            reason: :missing_lmdb_env
          }
        )

        :ok

      true ->
        rebuilt =
          ActiveIndexes.rebuild_flow_indexes_from_lmdb(
            lmdb_path,
            zset_score_index,
            zset_score_lookup,
            flow_index,
            flow_lookup
          )

        :telemetry.execute(
          [:ferricstore, :flow, :lmdb_startup_rebuild],
          %{lmdb_active_rebuilt: rebuilt, full_reconcile: 0},
          %{shard_index: shard_index, mode: :default_waraft_active_projection}
        )

        :ok
    end
  end

  def reconcile_startup_shard(
        shard_path,
        keydir,
        shard_index,
        instance_ctx,
        zset_score_index,
        zset_score_lookup,
        flow_index,
        flow_lookup,
        _opts
      ) do
    reconcile_shard(
      shard_path,
      keydir,
      shard_index,
      instance_ctx,
      zset_score_index,
      zset_score_lookup,
      flow_index,
      flow_lookup
    )
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
    decoded = ColdState.read_and_decode(entries, shard_path, shard_index, instance_ctx)

    {ops, terminal_prunes, active_records, terminal_counts} =
      Enum.reduce(decoded, {[], [], [], acc.terminal_counts}, fn {key, value, expire_at_ms,
                                                                  record},
                                                                 {ops, prunes, active, counts} ->
        lmdb_ops = [{:put, key, LMDB.encode_value(value, expire_at_ms)} | ops]

        if LMDB.terminal_state?(Map.get(record, :state)) do
          projection_expire_at_ms = flow_state_projection_expire_at(record, expire_at_ms)

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
              projection_expire_at_ms,
              key,
              count_key
            )

          terminal_expire_key = LMDB.terminal_expire_key(projection_expire_at_ms, terminal_key)
          terminal_expire_value = LMDB.encode_terminal_expire_value(terminal_key, key, count_key)

          terminal_expire_ops =
            if is_binary(terminal_expire_key) do
              [{:put, terminal_expire_key, terminal_expire_value}]
            else
              []
            end

          reverse_key = LMDB.terminal_by_state_key_key(key)
          metadata_ops = TerminalProjection.query_metadata_index_ops(record, projection_expire_at_ms)
          active_delete_ops = LMDB.active_index_delete_ops(lmdb_path, key)

          {
            [
              {:put, reverse_key, terminal_key},
              {:put, terminal_key, terminal_value}
              | active_delete_ops ++ terminal_expire_ops ++ metadata_ops ++ lmdb_ops
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
          Enum.flat_map(active_records, fn {key, record} ->
            TerminalProjection.cleanup_stale_terminal_ops(lmdb_path, key, record)
          end)

        cleanup_result = LMDB.write_batch(lmdb_path, cleanup_ops)

        Enum.each(active_records, fn {_key, record} ->
          ActiveIndexes.rebuild_score_indexes(zset_score_index, zset_score_lookup, record)
          ActiveIndexes.rebuild_flow_indexes(flow_index, flow_lookup, record)
        end)

        Enum.each(Enum.reverse(terminal_prunes), fn {key, record} ->
          safe_prune_terminal_keydir_entry(keydir, shard_index, key, record, instance_ctx)
        end)

        %{
          acc
          | seen: acc.seen + length(entries),
            lmdb: acc.lmdb + length(decoded),
            terminal: acc.terminal + length(terminal_prunes),
            active: acc.active + length(active_records),
            lmdb_errors: acc.lmdb_errors + if(cleanup_result == :ok, do: 0, else: 1),
            terminal_counts: terminal_counts
        }

      {:error, _reason} ->
        Enum.each(Enum.reverse(active_records), fn {_key, record} ->
          ActiveIndexes.rebuild_score_indexes(zset_score_index, zset_score_lookup, record)
          ActiveIndexes.rebuild_flow_indexes(flow_index, flow_lookup, record)
        end)

        %{
          acc
          | seen: acc.seen + length(entries),
            active: acc.active + length(active_records),
            lmdb_errors: acc.lmdb_errors + 1
        }
    end
  end

  defp initial_reconcile_stats(lmdb_path, keydir, shard_path) do
    cleanup_ops =
      TerminalProjection.cleanup_stale_terminal_reverse_ops(lmdb_path, keydir, fn entry ->
        ColdState.read_and_decode([entry], shard_path)
      end)

    cleanup_result = LMDB.write_batch(lmdb_path, cleanup_ops)

    %{
      seen: 0,
      lmdb: 0,
      terminal: 0,
      active: 0,
      lmdb_errors: if(cleanup_result == :ok, do: 0, else: 1),
      cold_read_errors: 0,
      terminal_counts: %{},
      terminal_reverse_cleanup_scans: 1,
      terminal_reverse_cleanup_ops: length(cleanup_ops)
    }
  end

  defp rebuild_flow_history_indexes(
         _keydir,
         _shard_path,
         _lmdb_path,
         _shard_index,
         _instance_ctx,
         nil,
         _flow_lookup
       ),
       do: {0, 0}

  defp rebuild_flow_history_indexes(
         _keydir,
         _shard_path,
         _lmdb_path,
         _shard_index,
         _instance_ctx,
         _flow_index,
         nil
       ),
       do: {0, 0}

  defp rebuild_flow_history_indexes(
         keydir,
         shard_path,
         lmdb_path,
         shard_index,
         instance_ctx,
         flow_index,
         flow_lookup
       ) do
    native = NativeFlowIndex.get(flow_index, flow_lookup)

    {count, history_entries_by_key} =
      :ets.foldl(
        fn
          {key, _value, _expire_at_ms, _lfu, _fid, _off, _vsize}, {count, history_entries_by_key}
          when is_binary(key) ->
            case parse_flow_history_entry_key(key) do
              {:ok, history_key, event_id, event_ms} ->
                if native do
                  NativeFlowIndex.put_member(native, history_key, event_id, event_ms)
                end

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

    history_lmdb_errors =
      rebuild_lmdb_history_projections(
        keydir,
        shard_path,
        lmdb_path,
        shard_index,
        instance_ctx,
        history_entries_by_key
      )

    {count, history_lmdb_errors}
  end

  defp maybe_rebuild_flow_history_indexes(
         keydir,
         shard_path,
         lmdb_path,
         shard_index,
         instance_ctx,
         flow_index,
         flow_lookup
       ) do
    if flow_async_history_enabled?() do
      {0, 0}
    else
      rebuild_flow_history_indexes(
        keydir,
        shard_path,
        lmdb_path,
        shard_index,
        instance_ctx,
        flow_index,
        flow_lookup
      )
    end
  end

  defp flow_async_history_enabled? do
    case Application.get_env(:ferricstore, :flow_async_history, true) do
      value when value in [true, "1", "true"] -> true
      value when value in [false, "0", "false"] -> false
      _ -> true
    end
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
    native = NativeFlowIndex.get(flow_index, flow_lookup)

    Enum.each(history_keys, fn history_key ->
      case history_max_events_for_key(
             keydir,
             shard_path,
             lmdb_path,
             shard_index,
             instance_ctx,
             history_key
           ) do
        max when is_integer(max) and max > 0 and native != nil ->
          trim_rebuilt_flow_history_index(native, history_key, max)

        _ ->
          :ok
      end
    end)

    :ok
  end

  defp trim_rebuilt_flow_history_index(native, history_key, max) do
    case NativeFlowIndex.count_all(native, history_key) do
      count when count > max ->
        delete_count = count - max

        event_ids =
          native
          |> NativeFlowIndex.rank_range(history_key, 0, delete_count - 1, false)
          |> Enum.map(fn {event_id, _score} -> event_id end)

        NativeFlowIndex.delete_members(native, history_key, event_ids)

      _ ->
        :ok
    end
  end

  defp rebuild_lmdb_history_projections(
         keydir,
         shard_path,
         lmdb_path,
         shard_index,
         instance_ctx,
         history_entries_by_key
       ) do
    Enum.reduce(history_entries_by_key, 0, fn {history_key, entries}, errors ->
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
        errors + rebuild_lmdb_history_projection(lmdb_path, history_key, record, entries)
      else
        _ -> errors
      end
    end)
  end

  defp rebuild_lmdb_history_projection(lmdb_path, history_key, record, entries) do
    expire_at_ms = flow_record_expire_at(record)

    {delete_ops, scan_errors} =
      lmdb_path
      |> LMDB.prefix_entries(
        history_key |> LMDB.history_index_prefix(),
        history_projection_scan_limit()
      )
      |> case do
        {:ok, existing} ->
          {
            Enum.flat_map(existing, fn {history_index_key, _value} ->
              LMDB.history_index_delete_ops(lmdb_path, history_index_key)
            end),
            0
          }

        {:error, _reason} ->
          {[], 1}
      end

    retained_entries =
      entries
      |> Enum.sort_by(fn {event_id, event_ms, _compound_key} -> {event_ms, event_id} end)
      |> take_latest_history_events(Map.get(record, :history_max_events))

    event_ops =
      retained_entries
      |> Enum.flat_map(fn {event_id, event_ms, compound_key} ->
        history_index_key = LMDB.history_index_key(history_key, event_id, event_ms)

        ops = [
          {:put, history_index_key,
           LMDB.encode_history_index_value(event_id, event_ms, compound_key, expire_at_ms)}
        ]

        case LMDB.history_expire_key(expire_at_ms, history_index_key) do
          nil ->
            ops

          expire_key ->
            [{:put, expire_key, LMDB.encode_history_expire_value(history_index_key)} | ops]
        end
      end)

    put_ops =
      case {retained_entries, LMDB.history_flow_expire_key(expire_at_ms, history_key)} do
        {[], _expire_key} ->
          event_ops

        {_entries, nil} ->
          event_ops

        {_entries, expire_key} ->
          [
            {:put, expire_key, LMDB.encode_history_flow_expire_value(history_key, expire_at_ms)}
            | event_ops
          ]
      end

    write_errors =
      (delete_ops ++ put_ops)
      |> Enum.chunk_every(@batch_size)
      |> Enum.reduce(0, fn ops, errors ->
        case write_lmdb_history_projection_batch(lmdb_path, ops) do
          :ok -> errors
          {:error, _reason} -> errors + 1
        end
      end)

    scan_errors + write_errors
  end

  defp write_lmdb_history_projection_batch(lmdb_path, ops) do
    case Application.get_env(:ferricstore, :flow_lmdb_rebuild_history_write_hook) do
      fun when is_function(fun, 2) -> fun.(lmdb_path, ops)
      _ -> LMDB.write_batch(lmdb_path, ops)
    end
  rescue
    reason -> {:error, {:history_projection_write_hook_failed, reason}}
  catch
    kind, reason -> {:error, {:history_projection_write_hook_failed, kind, reason}}
  end

  defp take_latest_history_events(entries, max) when is_integer(max) and max > 0,
    do: Enum.take(entries, -max)

  defp take_latest_history_events(entries, _max), do: entries

  defp history_projection_scan_limit do
    Application.get_env(:ferricstore, :flow_lmdb_history_rebuild_scan_limit, 1_000_000)
  end

  defp flow_record_expire_at(%{terminal_retention_until_ms: expire_at_ms})
       when is_integer(expire_at_ms),
       do: expire_at_ms

  defp flow_record_expire_at(_record), do: 0

  defp flow_state_projection_expire_at(record, fallback_expire_at_ms) when is_map(record) do
    case flow_record_expire_at(record) do
      expire_at_ms when is_integer(expire_at_ms) and expire_at_ms > 0 -> expire_at_ms
      _other -> fallback_expire_at_ms
    end
  end

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
        case ColdState.decode_state_record(state_key, value, expire_at_ms, shard_index, instance_ctx) do
          [{_key, _value, _expire_at_ms, record}] -> {:ok, record}
          _ -> :error
        end

      [
        {^state_key, nil, expire_at_ms, _lfu, fid, off, vsize}
      ]
      when is_integer(fid) and is_integer(off) and is_integer(vsize) and off >= 0 and vsize >= 0 ->
        shard_path
        |> ColdState.cold_locations_for_state(state_key, expire_at_ms, fid, off, vsize)
        |> ColdState.read_cold_locations(shard_index, instance_ctx)
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

  defp select_state_entries(keydir) do
    :ets.safe_fixtable(keydir, true)

    try do
      keydir
      |> select_state_entry_chunks(:ets.select(keydir, keydir_match_spec(), @batch_size), [])
      |> Enum.reverse()
    after
      :ets.safe_fixtable(keydir, false)
    end
  rescue
    ArgumentError -> []
  end

  defp select_state_entry_chunks(_keydir, :"$end_of_table", acc), do: acc

  defp select_state_entry_chunks(keydir, {entries, continuation}, acc) do
    state_entries = Enum.filter(entries, &flow_state_entry?/1)
    acc = if state_entries == [], do: acc, else: [state_entries | acc]
    select_state_entry_chunks(keydir, :ets.select(continuation), acc)
  end

  defp safe_prune_terminal_keydir_entry(keydir, shard_index, key, record, instance_ctx) do
    version = Map.get(record, :version)

    with [{^key, value, expire_at_ms, _lfu, _fid, _off, _vsize}] <- :ets.lookup(keydir, key),
         true <- is_binary(value),
         [{^key, _materialized, _expire_at_ms, current}] <-
           ColdState.decode_state_record(key, value, expire_at_ms, shard_index, instance_ctx),
         ^version <- Map.get(current, :version),
         true <- LMDB.terminal_state?(Map.get(current, :state)) do
      track_binary_remove(keydir, shard_index, key, instance_ctx)
      :ets.delete(keydir, key)
    end

    :ok
  rescue
    ArgumentError -> :ok
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
