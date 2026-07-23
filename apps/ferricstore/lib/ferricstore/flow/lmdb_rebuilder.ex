defmodule Ferricstore.Flow.LMDBRebuilder do
  @moduledoc false

  alias Ferricstore.Flow
  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.LMDBFlushCoordinator
  alias Ferricstore.Flow.LMDBRebuilder.ActiveIndexes
  alias Ferricstore.Flow.LMDBRebuilder.ColdState
  alias Ferricstore.Flow.LMDBRebuilder.TerminalCounts
  alias Ferricstore.Flow.LMDBRebuilder.TerminalProjection
  alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
  alias Ferricstore.Flow.PolicyMigration
  alias Ferricstore.Flow.SharedRefBackfill
  alias Ferricstore.Store.Shard.ZSetIndex

  @batch_size 512
  @default_history_projection_page_size 4_096
  @max_history_projection_page_size 65_536

  def init_startup_active_rebuild_limiter,
    do: ActiveIndexes.init_startup_active_rebuild_limiter()

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
      with {:ok, stats} <-
             reduce_state_entries(
               keydir,
               %{seen: 0, active: 0, terminal: 0, cold_read_errors: 0},
               fn entries, acc ->
                 entries
                 |> ColdState.read_and_decode(shard_path, shard_index, instance_ctx)
                 |> Enum.reduce(
                   %{acc | seen: acc.seen + length(entries)},
                   fn {_key, _value, _expire_at_ms, record}, next_acc ->
                     if LMDB.terminal_state?(Map.get(record, :state)) do
                       %{next_acc | terminal: next_acc.terminal + 1}
                     else
                       ActiveIndexes.rebuild_score_indexes(
                         zset_score_index,
                         zset_score_lookup,
                         record
                       )

                       ActiveIndexes.rebuild_flow_indexes(flow_index, flow_lookup, record)
                       %{next_acc | active: next_acc.active + 1}
                     end
                   end
                 )
               end
             ) do
        stats =
          Map.put(
            stats,
            :cold_read_errors,
            Process.get(:flow_lmdb_rebuild_cold_read_errors, 0)
          )

        :telemetry.execute(
          [:ferricstore, :flow, :active_index_rebuild],
          stats,
          %{shard_index: shard_index}
        )

        case stats.cold_read_errors do
          0 -> :ok
          count -> {:error, {:cold_read_errors, count}}
        end
      end
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
        flow_lookup \\ nil,
        opts \\ []
      )

  def reconcile_shard(
        shard_path,
        keydir,
        shard_index,
        instance_ctx,
        zset_score_index,
        zset_score_lookup,
        flow_index,
        flow_lookup,
        opts
      ) do
    instance_name = Map.get(instance_ctx || %{}, :name, :default)

    LMDBFlushCoordinator.with_shard_permit(instance_name, shard_index, fn ->
      do_reconcile_shard(
        shard_path,
        keydir,
        shard_index,
        instance_ctx,
        zset_score_index,
        zset_score_lookup,
        flow_index,
        flow_lookup,
        opts
      )
    end)
  end

  defp do_reconcile_shard(
         shard_path,
         keydir,
         shard_index,
         instance_ctx,
         zset_score_index,
         zset_score_lookup,
         flow_index,
         flow_lookup,
         opts
       ) do
    lmdb_path = LMDB.path(shard_path)
    prune_terminal_keydir? = Keyword.get(opts, :prune_terminal_keydir?, false)
    Process.put(:flow_lmdb_rebuild_cold_read_errors, 0)

    try do
      with {:ok, state_entries_present?} <- state_entries_present?(keydir),
           {:ok, marker_started?} <-
             begin_reconcile_marker(lmdb_path, state_entries_present?),
           :ok <-
             maybe_reset_active_projection(
               lmdb_path,
               Keyword.get(opts, :reset_active_projection?, false)
             ),
           {:ok, stats} <-
             reduce_state_entries(
               keydir,
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
                   prune_terminal_keydir?,
                   acc
                 )
               end
             ) do
        stats = TerminalCounts.persist(stats, lmdb_path)

        lmdb_active_rebuild_result =
          ActiveIndexes.rebuild_flow_indexes_from_lmdb(
            lmdb_path,
            zset_score_index,
            zset_score_lookup,
            flow_index,
            flow_lookup
          )

        {lmdb_active_rebuilt, active_index_lmdb_errors} =
          case lmdb_active_rebuild_result do
            {:ok, rebuilt} when is_integer(rebuilt) and rebuilt >= 0 -> {rebuilt, 0}
            {:error, _reason} -> {0, 1}
            _invalid -> {0, 1}
          end

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
          |> Map.update!(
            :lmdb_errors,
            &(&1 + history_lmdb_errors + active_index_lmdb_errors)
          )

        {stats, healthy?} = finalize_reconcile_marker(lmdb_path, marker_started?, stats)

        ColdState.publish_mirror_health(instance_ctx, shard_index, stats)

        telemetry_stats =
          stats
          |> Map.put_new(:terminal_count_keys, 0)

        :telemetry.execute(
          [:ferricstore, :flow, :lmdb_rebuild],
          telemetry_stats,
          %{shard_index: shard_index}
        )

        cond do
          not healthy? ->
            {:error,
             {:flow_lmdb_reconcile_unhealthy,
              Map.take(stats, [:lmdb_errors, :cold_read_errors, :history_lmdb_errors])}}

          Keyword.get(opts, :rotate_policy_source?, false) ->
            PolicyMigration.rotate_source_token(lmdb_path)

          true ->
            :ok
        end
      end
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

    result =
      cond do
        Keyword.get(opts, :force_full_reconcile?, false) ->
          case reconcile_shard(
                 shard_path,
                 keydir,
                 shard_index,
                 instance_ctx,
                 zset_score_index,
                 zset_score_lookup,
                 flow_index,
                 flow_lookup,
                 rotate_policy_source?: true,
                 reset_active_projection?: true
               ) do
            :ok ->
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

            {:error, _reason} = error ->
              error
          end

        LMDB.flush_in_progress?(lmdb_path) ->
          case reconcile_shard(
                 shard_path,
                 keydir,
                 shard_index,
                 instance_ctx,
                 zset_score_index,
                 zset_score_lookup,
                 flow_index,
                 flow_lookup,
                 rotate_policy_source?: true,
                 reset_active_projection?: true
               ) do
            :ok ->
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

            {:error, _reason} = error ->
              error
          end

        not LMDB.env_present?(lmdb_path) ->
          case reconcile_shard(
                 shard_path,
                 keydir,
                 shard_index,
                 instance_ctx,
                 zset_score_index,
                 zset_score_lookup,
                 flow_index,
                 flow_lookup,
                 rotate_policy_source?: true,
                 reset_active_projection?: true
               ) do
            :ok ->
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

            {:error, _reason} = error ->
              error
          end

        true ->
          case ActiveIndexes.rebuild_flow_indexes_from_lmdb(
                 lmdb_path,
                 zset_score_index,
                 zset_score_lookup,
                 flow_index,
                 flow_lookup
               ) do
            {:ok, rebuilt} when is_integer(rebuilt) and rebuilt >= 0 ->
              :telemetry.execute(
                [:ferricstore, :flow, :lmdb_startup_rebuild],
                %{lmdb_active_rebuilt: rebuilt, full_reconcile: 0, lmdb_errors: 0},
                %{shard_index: shard_index, mode: :default_waraft_active_projection}
              )

              :ok

            {:error, reason} ->
              repair_invalid_startup_active_projection(
                shard_path,
                keydir,
                shard_index,
                instance_ctx,
                zset_score_index,
                zset_score_lookup,
                flow_index,
                flow_lookup,
                reason
              )
          end
      end

    with :ok <- result do
      maybe_run_shared_ref_backfill(
        shard_path,
        keydir,
        shard_index,
        instance_ctx,
        flow_index,
        flow_lookup,
        opts
      )
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
        opts
      ) do
    with :ok <-
           reconcile_shard(
             shard_path,
             keydir,
             shard_index,
             instance_ctx,
             zset_score_index,
             zset_score_lookup,
             flow_index,
             flow_lookup
           ) do
      maybe_run_shared_ref_backfill(
        shard_path,
        keydir,
        shard_index,
        instance_ctx,
        flow_index,
        flow_lookup,
        opts
      )
    end
  end

  defp repair_invalid_startup_active_projection(
         shard_path,
         keydir,
         shard_index,
         instance_ctx,
         zset_score_index,
         zset_score_lookup,
         flow_index,
         flow_lookup,
         reason
       ) do
    lmdb_path = LMDB.path(shard_path)

    result =
      with :ok <-
             reset_startup_active_indexes(
               zset_score_index,
               zset_score_lookup,
               flow_index,
               flow_lookup
             ) do
        reconcile_shard(
          shard_path,
          keydir,
          shard_index,
          instance_ctx,
          zset_score_index,
          zset_score_lookup,
          flow_index,
          flow_lookup,
          rotate_policy_source?: true,
          reset_active_projection?: true
        )
      end

    :telemetry.execute(
      [:ferricstore, :flow, :lmdb_startup_rebuild],
      %{lmdb_active_rebuilt: 0, full_reconcile: 1, lmdb_errors: 1},
      %{
        shard_index: shard_index,
        mode: :default_waraft_active_projection,
        reason: reason
      }
    )

    case {result, LMDB.flush_in_progress?(lmdb_path)} do
      {:ok, false} ->
        :ok

      {{:error, _repair_reason} = error, _marker_state} ->
        error

      {_invalid_or_unhealthy, _marker_state} ->
        {:error, {:flow_lmdb_startup_reconcile_failed, reason}}
    end
  end

  defp reset_startup_active_indexes(
         zset_score_index,
         zset_score_lookup,
         flow_index,
         flow_lookup
       ) do
    with :ok <- reset_startup_flow_score_index(zset_score_index, zset_score_lookup) do
      reset_startup_flow_index(flow_index, flow_lookup)
    end
  end

  defp reset_startup_flow_score_index(nil, _zset_score_lookup), do: :ok
  defp reset_startup_flow_score_index(_zset_score_index, nil), do: :ok

  defp reset_startup_flow_score_index(zset_score_index, zset_score_lookup) do
    ZSetIndex.clear_key_prefix(
      zset_score_index,
      zset_score_lookup,
      "f:{",
      &Ferricstore.Flow.InternalKey.internal?/1
    )
  end

  defp reset_startup_flow_index(nil, _flow_lookup), do: :ok
  defp reset_startup_flow_index(_flow_index, nil), do: :ok

  defp reset_startup_flow_index(flow_index, flow_lookup) do
    _resource = NativeFlowIndex.reset(flow_index, flow_lookup)
    :ok
  end

  defp maybe_run_shared_ref_backfill(
         shard_path,
         keydir,
         shard_index,
         instance_ctx,
         flow_index,
         flow_lookup,
         opts
       ) do
    cond do
      Keyword.get(opts, :shared_ref_backfill?, true) == false ->
        :ok

      shared_ref_backfill_empty_keydir?(keydir, shard_index) ->
        SharedRefBackfill.finalize_empty_shard!(
          shard_path,
          keydir,
          shard_index,
          instance_ctx,
          opts
        )

      true ->
        SharedRefBackfill.run!(
          shard_path,
          keydir,
          shard_index,
          instance_ctx,
          flow_index,
          flow_lookup,
          opts
        )
    end
  end

  defp shared_ref_backfill_empty_keydir?(keydir, shard_index) do
    case :ets.info(keydir, :size) do
      0 ->
        true

      size when is_integer(size) and size <= 2 ->
        allowed = [
          Flow.Keys.shared_value_ref_backfill_key(shard_index),
          SharedRefBackfill.progress_key(shard_index)
        ]

        Enum.count(allowed, &:ets.member(keydir, &1)) == size

      _nonempty_or_missing ->
        false
    end
  rescue
    ArgumentError -> false
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
         prune_terminal_keydir?,
         acc
       ) do
    decoded = ColdState.read_and_decode(entries, shard_path, shard_index, instance_ctx)

    {ops, terminal_prunes, active_records, projection_read_errors} =
      Enum.reduce(decoded, {[], [], [], 0}, fn
        {key, value, expire_at_ms, record}, {ops, prunes, active, read_errors} ->
          state_put_op = {:put, key, LMDB.encode_value(value, expire_at_ms)}

          if LMDB.terminal_state?(Map.get(record, :state)) do
            projection_expire_at_ms = flow_state_projection_expire_at(record, expire_at_ms)

            index_key =
              Flow.Keys.state_index_key(
                record.type,
                record.state,
                Map.get(record, :partition_key)
              )

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

            terminal_expire_value =
              LMDB.encode_terminal_expire_value(terminal_key, key, count_key)

            terminal_expire_ops =
              if is_binary(terminal_expire_key) do
                [{:put, terminal_expire_key, terminal_expire_value}]
              else
                []
              end

            reverse_key = LMDB.terminal_by_state_key_key(key)

            metadata_ops =
              TerminalProjection.query_metadata_index_ops(record, projection_expire_at_ms)

            {active_delete_ops, next_read_errors} =
              case LMDB.active_index_delete_ops_result(lmdb_path, key) do
                {:ok, active_delete_ops} -> {active_delete_ops, read_errors}
                {:error, _reason} -> {[], read_errors + 1}
              end

            reconcile_ops =
              active_delete_ops ++
                [
                  state_put_op,
                  {:put, reverse_key, terminal_key},
                  {:put, terminal_key, terminal_value}
                ] ++ terminal_expire_ops ++ metadata_ops

            {
              :lists.reverse(reconcile_ops, ops),
              [{key, record} | prunes],
              active,
              next_read_errors
            }
          else
            projection_expire_at_ms = flow_state_projection_expire_at(record, expire_at_ms)

            attribute_ops =
              Ferricstore.Flow.LMDBWriter.ProjectionOps.flow_attribute_query_ops(
                record,
                projection_expire_at_ms,
                key
              )

            {active_ops, next_read_errors} =
              case LMDB.active_index_delete_ops_result(lmdb_path, key) do
                {:ok, active_delete_ops} ->
                  {active_put_ops, _reverse_value} =
                    LMDB.active_index_put_ops_with_reverse(
                      key,
                      record,
                      projection_expire_at_ms
                    )

                  {active_delete_ops ++ active_put_ops, read_errors}

                {:error, _reason} ->
                  {[], read_errors + 1}
              end

            reconcile_ops = [state_put_op | active_ops ++ attribute_ops]

            {:lists.reverse(reconcile_ops, ops), prunes, [{key, record} | active],
             next_read_errors}
          end
      end)

    case LMDB.write_batch(lmdb_path, Enum.reverse(ops)) do
      :ok ->
        active_records = Enum.reverse(active_records)

        cleanup_result =
          Enum.reduce_while(active_records, {:ok, []}, fn {key, record}, {:ok, reversed_ops} ->
            case TerminalProjection.cleanup_stale_terminal_ops(lmdb_path, key, record) do
              {:ok, ops} -> {:cont, {:ok, :lists.reverse(ops, reversed_ops)}}
              {:error, _reason} = error -> {:halt, error}
            end
          end)
          |> case do
            {:ok, reversed_ops} -> LMDB.write_batch(lmdb_path, Enum.reverse(reversed_ops))
            {:error, _reason} = error -> error
          end

        Enum.each(active_records, fn {_key, record} ->
          ActiveIndexes.rebuild_score_indexes(zset_score_index, zset_score_lookup, record)
          ActiveIndexes.rebuild_flow_indexes(flow_index, flow_lookup, record)
        end)

        if prune_terminal_keydir? do
          Enum.each(Enum.reverse(terminal_prunes), fn {key, record} ->
            safe_prune_terminal_keydir_entry(
              keydir,
              shard_path,
              shard_index,
              key,
              record,
              instance_ctx
            )
          end)
        end

        %{
          acc
          | seen: acc.seen + length(entries),
            lmdb: acc.lmdb + length(decoded),
            terminal: acc.terminal + length(terminal_prunes),
            active: acc.active + length(active_records),
            lmdb_errors:
              acc.lmdb_errors + projection_read_errors +
                if(cleanup_result == :ok, do: 0, else: 1)
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
            lmdb_errors: acc.lmdb_errors + projection_read_errors + 1
        }
    end
  end

  defp initial_reconcile_stats(lmdb_path, keydir, shard_path) do
    {cleanup_op_count, scan_errors} =
      case cleanup_stale_terminal_reverse(lmdb_path, keydir, shard_path) do
        {:ok, cleanup_op_count} -> {cleanup_op_count, 0}
        {:error, _reason} -> {0, 1}
      end

    %{
      seen: 0,
      lmdb: 0,
      terminal: 0,
      active: 0,
      lmdb_errors: scan_errors,
      cold_read_errors: 0,
      terminal_reverse_cleanup_scans: 1,
      terminal_reverse_cleanup_ops: cleanup_op_count
    }
  end

  defp cleanup_stale_terminal_reverse(lmdb_path, keydir, shard_path) do
    TerminalProjection.cleanup_stale_terminal_reverse(lmdb_path, keydir, fn entry ->
      ColdState.read_and_decode([entry], shard_path)
    end)
  end

  defp begin_reconcile_marker(lmdb_path, state_entries_present?) do
    marker_required? = state_entries_present? or LMDB.env_present?(lmdb_path)

    if marker_required? do
      case LMDB.write_batch(lmdb_path, [LMDB.flush_in_progress_put_op()]) do
        :ok -> {:ok, true}
        {:error, _reason} = error -> error
      end
    else
      {:ok, false}
    end
  end

  defp maybe_reset_active_projection(_lmdb_path, false), do: :ok

  defp maybe_reset_active_projection(lmdb_path, true) do
    [LMDB.active_index_global_prefix(), LMDB.active_by_state_global_prefix()]
    |> Enum.reduce_while(:ok, fn prefix, :ok ->
      case delete_projection_prefix(lmdb_path, prefix, <<>>) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp delete_projection_prefix(lmdb_path, prefix, after_key) do
    case LMDB.prefix_entries_after(lmdb_path, prefix, after_key, @batch_size) do
      {:ok, []} ->
        :ok

      {:ok, entries} when is_list(entries) ->
        case List.last(entries) do
          {last_key, _value} when is_binary(last_key) and last_key > after_key ->
            delete_ops = Enum.map(entries, fn {key, _value} -> {:delete, key} end)

            with :ok <- LMDB.write_batch(lmdb_path, delete_ops) do
              delete_projection_prefix(lmdb_path, prefix, last_key)
            end

          _invalid ->
            {:error, :invalid_active_projection_page}
        end

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, :invalid_active_projection_page}
    end
  end

  defp finalize_reconcile_marker(lmdb_path, marker_started?, stats) do
    healthy? = stats.lmdb_errors == 0 and Map.get(stats, :cold_read_errors, 0) == 0

    cond do
      marker_started? and healthy? ->
        case LMDB.write_batch(lmdb_path, [LMDB.flush_in_progress_delete_op()]) do
          :ok -> {stats, true}
          {:error, _reason} -> {%{stats | lmdb_errors: stats.lmdb_errors + 1}, false}
        end

      true ->
        {stats, healthy?}
    end
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

    history_events =
      :ets.new(:flow_lmdb_rebuild_history_events, [:ordered_set, :private, :compressed])

    history_keys = :ets.new(:flow_lmdb_rebuild_history_keys, [:set, :private])

    try do
      count = stage_history_entries(keydir, history_events, history_keys, native)

      history_lmdb_errors =
        rebuild_staged_history_projections(
          keydir,
          shard_path,
          lmdb_path,
          shard_index,
          instance_ctx,
          native,
          history_events,
          history_keys
        )

      {count, history_lmdb_errors}
    after
      :ets.delete(history_events)
      :ets.delete(history_keys)
    end
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

  defp stage_history_entries(keydir, history_events, history_keys, native) do
    :ets.foldl(
      fn
        {key, _value, _expire_at_ms, _lfu, _fid, _off, _vsize}, count when is_binary(key) ->
          case parse_flow_history_entry_key(key) do
            {:ok, history_key, event_id, event_ms} ->
              if native do
                NativeFlowIndex.put_member(native, history_key, event_id, event_ms)
              end

              stage_history_entry(
                history_events,
                history_keys,
                history_key,
                event_id,
                event_ms,
                key
              )

              count + 1

            :skip ->
              count
          end

        _entry, count ->
          count
      end,
      0,
      keydir
    )
  end

  defp stage_history_entry(
         history_events,
         history_keys,
         history_key,
         event_id,
         event_ms,
         compound_key
       ) do
    :ets.insert(
      history_events,
      {{history_key, event_ms, event_id, compound_key}}
    )

    :ets.update_counter(history_keys, history_key, {2, 1}, {history_key, 0})
    :ok
  end

  defp rebuild_staged_history_projections(
         keydir,
         shard_path,
         lmdb_path,
         shard_index,
         instance_ctx,
         native,
         history_events,
         history_keys
       ) do
    :ets.foldl(
      fn {history_key, event_count}, errors ->
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
          max_events = Map.get(record, :history_max_events)

          if is_integer(max_events) and max_events > 0 and native != nil do
            trim_rebuilt_flow_history_index(native, history_key, max_events)
          end

          if LMDB.terminal_state?(Map.get(record, :state)) do
            errors +
              rebuild_lmdb_history_projection(
                lmdb_path,
                history_key,
                record,
                history_events,
                event_count
              )
          else
            errors
          end
        else
          _ -> errors
        end
      end,
      0,
      history_keys
    )
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

  defp rebuild_lmdb_history_projection(
         lmdb_path,
         history_key,
         record,
         history_events,
         event_count
       ) do
    expire_at_ms = flow_record_expire_at(record)

    with :ok <- delete_existing_history_projection(lmdb_path, history_key),
         :ok <-
           write_staged_history_projection(
             lmdb_path,
             history_events,
             history_key,
             event_count,
             Map.get(record, :history_max_events),
             expire_at_ms
           ) do
      0
    else
      {:error, _reason} -> 1
    end
  end

  defp write_staged_history_projection(
         lmdb_path,
         history_events,
         history_key,
         event_count,
         max_events,
         expire_at_ms
       ) do
    retained_count = retained_history_event_count(event_count, max_events)
    skip_count = event_count - retained_count

    initial_ops =
      case LMDB.history_flow_expire_key(expire_at_ms, history_key) do
        nil ->
          []

        expire_key when retained_count > 0 ->
          [
            {:put, expire_key, LMDB.encode_history_flow_expire_value(history_key, expire_at_ms)}
          ]

        _expire_key ->
          []
      end

    with {:ok, reversed_ops, op_count} <-
           append_history_projection_ops(lmdb_path, [], 0, initial_ops) do
      history_events
      |> first_staged_history_event(history_key)
      |> stream_staged_history_projection(
        history_events,
        history_key,
        skip_count,
        expire_at_ms,
        lmdb_path,
        reversed_ops,
        op_count
      )
    end
  end

  defp first_staged_history_event(history_events, history_key),
    do: :ets.next(history_events, {history_key, -1, <<>>, <<>>})

  defp stream_staged_history_projection(
         :"$end_of_table",
         _history_events,
         _history_key,
         _skip_count,
         _expire_at_ms,
         lmdb_path,
         reversed_ops,
         _op_count
       ),
       do: flush_staged_history_projection_ops(lmdb_path, reversed_ops)

  defp stream_staged_history_projection(
         {history_key, _event_ms, _event_id, _compound_key} = staged_key,
         history_events,
         history_key,
         skip_count,
         expire_at_ms,
         lmdb_path,
         reversed_ops,
         op_count
       )
       when skip_count > 0 do
    staged_key
    |> then(&:ets.next(history_events, &1))
    |> stream_staged_history_projection(
      history_events,
      history_key,
      skip_count - 1,
      expire_at_ms,
      lmdb_path,
      reversed_ops,
      op_count
    )
  end

  defp stream_staged_history_projection(
         {history_key, event_ms, event_id, compound_key} = staged_key,
         history_events,
         history_key,
         0,
         expire_at_ms,
         lmdb_path,
         reversed_ops,
         op_count
       ) do
    history_index_key = LMDB.history_index_key(history_key, event_id, event_ms)

    event_ops = [
      {:put, history_index_key,
       LMDB.encode_history_index_value(event_id, event_ms, compound_key, expire_at_ms)}
    ]

    event_ops =
      case LMDB.history_expire_key(expire_at_ms, history_index_key) do
        nil ->
          event_ops

        expire_key ->
          [{:put, expire_key, LMDB.encode_history_expire_value(history_index_key)} | event_ops]
      end

    with {:ok, reversed_ops, op_count} <-
           append_history_projection_ops(lmdb_path, reversed_ops, op_count, event_ops) do
      staged_key
      |> then(&:ets.next(history_events, &1))
      |> stream_staged_history_projection(
        history_events,
        history_key,
        0,
        expire_at_ms,
        lmdb_path,
        reversed_ops,
        op_count
      )
    end
  end

  defp stream_staged_history_projection(
         _next_history_key,
         _history_events,
         _history_key,
         _skip_count,
         _expire_at_ms,
         lmdb_path,
         reversed_ops,
         _op_count
       ),
       do: flush_staged_history_projection_ops(lmdb_path, reversed_ops)

  defp append_history_projection_ops(lmdb_path, reversed_ops, op_count, ops) do
    added_count = length(ops)

    if op_count > 0 and op_count + added_count > @batch_size do
      with :ok <- flush_staged_history_projection_ops(lmdb_path, reversed_ops) do
        {:ok, Enum.reverse(ops), added_count}
      end
    else
      {:ok, :lists.reverse(ops, reversed_ops), op_count + added_count}
    end
  end

  defp flush_staged_history_projection_ops(_lmdb_path, []), do: :ok

  defp flush_staged_history_projection_ops(lmdb_path, reversed_ops),
    do: write_lmdb_history_projection_batch(lmdb_path, Enum.reverse(reversed_ops))

  defp retained_history_event_count(event_count, max_events)
       when is_integer(max_events) and max_events > 0,
       do: min(event_count, max_events)

  defp retained_history_event_count(event_count, _max_events), do: event_count

  defp delete_existing_history_projection(lmdb_path, history_key) do
    result =
      LMDB.reduce_prefix_entries(
        lmdb_path,
        LMDB.history_index_prefix(history_key),
        history_projection_page_size(),
        :ok,
        fn existing, :ok ->
          with {:ok, delete_ops} <- strict_history_delete_ops(lmdb_path, existing),
               :ok <- write_lmdb_history_projection_ops(lmdb_path, delete_ops) do
            {:ok, :ok}
          end
        end
      )

    case result do
      {:ok, :ok} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp write_lmdb_history_projection_ops(lmdb_path, ops) do
    ops
    |> Enum.chunk_every(@batch_size)
    |> Enum.reduce_while(:ok, fn batch, :ok ->
      case write_lmdb_history_projection_batch(lmdb_path, batch) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp strict_history_delete_ops(lmdb_path, existing) do
    Enum.reduce_while(existing, {:ok, []}, fn
      {history_index_key, _value}, {:ok, reversed_ops} when is_binary(history_index_key) ->
        case LMDB.history_index_delete_ops_result(lmdb_path, history_index_key) do
          {:ok, ops} -> {:cont, {:ok, :lists.reverse(ops, reversed_ops)}}
          {:error, _reason} = error -> {:halt, error}
        end

      invalid, _acc ->
        {:halt, {:error, {:invalid_history_index_entry, invalid}}}
    end)
    |> case do
      {:ok, reversed_ops} -> {:ok, Enum.reverse(reversed_ops)}
      {:error, _reason} = error -> error
    end
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

  defp history_projection_page_size do
    case Application.get_env(
           :ferricstore,
           :flow_lmdb_history_rebuild_page_size,
           @default_history_projection_page_size
         ) do
      value when is_integer(value) and value > 0 ->
        min(value, @max_history_projection_page_size)

      _invalid ->
        @default_history_projection_page_size
    end
  end

  @doc false
  def __history_projection_scan_limit_for_test__, do: history_projection_page_size()

  @doc false
  def __retained_staged_history_entries_for_test__(entries, history_key, max_events)
      when is_list(entries) and is_binary(history_key) do
    history_events = :ets.new(:flow_lmdb_rebuild_history_events_test, [:ordered_set, :private])
    history_keys = :ets.new(:flow_lmdb_rebuild_history_keys_test, [:set, :private])

    try do
      Enum.each(entries, fn
        {entry_history_key, event_id, event_ms, compound_key}
        when is_binary(entry_history_key) and is_binary(event_id) and is_integer(event_ms) and
               event_ms >= 0 and is_binary(compound_key) ->
          stage_history_entry(
            history_events,
            history_keys,
            entry_history_key,
            event_id,
            event_ms,
            compound_key
          )

        _invalid ->
          :ok
      end)

      event_count =
        case :ets.lookup(history_keys, history_key) do
          [{^history_key, count}] -> count
          [] -> 0
        end

      skip_count = event_count - retained_history_event_count(event_count, max_events)

      history_events
      |> first_staged_history_event(history_key)
      |> collect_staged_history_entries(history_events, history_key, skip_count, [])
    after
      :ets.delete(history_events)
      :ets.delete(history_keys)
    end
  end

  defp collect_staged_history_entries(
         :"$end_of_table",
         _history_events,
         _history_key,
         _skip_count,
         acc
       ),
       do: Enum.reverse(acc)

  defp collect_staged_history_entries(
         {history_key, _event_ms, _event_id, _compound_key} = staged_key,
         history_events,
         history_key,
         skip_count,
         acc
       )
       when skip_count > 0 do
    collect_staged_history_entries(
      :ets.next(history_events, staged_key),
      history_events,
      history_key,
      skip_count - 1,
      acc
    )
  end

  defp collect_staged_history_entries(
         {history_key, event_ms, event_id, compound_key} = staged_key,
         history_events,
         history_key,
         0,
         acc
       ) do
    collect_staged_history_entries(
      :ets.next(history_events, staged_key),
      history_events,
      history_key,
      0,
      [{event_id, event_ms, compound_key} | acc]
    )
  end

  defp collect_staged_history_entries(
         _next_history_key,
         _history_events,
         _history_key,
         _skip_count,
         acc
       ),
       do: Enum.reverse(acc)

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
        case ColdState.decode_state_record(
               state_key,
               value,
               expire_at_ms,
               shard_index,
               instance_ctx
             ) do
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

  defp reduce_state_entries(keydir, acc, reducer) when is_function(reducer, 2) do
    with :ok <- safe_fix_keydir(keydir) do
      try do
        with {:ok, page} <- safe_select_initial_state_entry_page(keydir) do
          reduce_state_entry_chunks(page, acc, reducer)
        end
      after
        safe_unfix_keydir(keydir)
      end
    end
  end

  @doc false
  def __reduce_state_entries_for_test__(keydir, acc, reducer),
    do: reduce_state_entries(keydir, acc, reducer)

  @doc false
  def __select_state_entries_for_test__(keydir) do
    case reduce_state_entries(keydir, [], fn entries, acc -> [entries | acc] end) do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp reduce_state_entry_chunks(:"$end_of_table", acc, _reducer), do: {:ok, acc}

  defp reduce_state_entry_chunks({entries, continuation}, acc, reducer) do
    state_entries = Enum.filter(entries, &flow_state_entry?/1)
    acc = if state_entries == [], do: acc, else: reducer.(state_entries, acc)

    with {:ok, page} <- safe_select_state_entry_page(continuation) do
      reduce_state_entry_chunks(page, acc, reducer)
    end
  end

  defp safe_fix_keydir(keydir) do
    :ets.safe_fixtable(keydir, true)
    :ok
  rescue
    ArgumentError -> {:error, :source_keydir_unavailable}
  end

  defp safe_unfix_keydir(keydir) do
    :ets.safe_fixtable(keydir, false)
    :ok
  rescue
    ArgumentError -> :ok
  end

  defp safe_select_initial_state_entry_page(keydir) do
    {:ok, :ets.select(keydir, keydir_match_spec(), @batch_size)}
  rescue
    ArgumentError -> {:error, :source_keydir_unavailable}
  end

  defp safe_select_state_entry_page(continuation) do
    {:ok, :ets.select(continuation)}
  rescue
    ArgumentError -> {:error, :source_keydir_unavailable}
  end

  defp state_entries_present?(keydir) do
    :ets.safe_fixtable(keydir, true)

    try do
      {:ok, state_entry_chunk_present?(:ets.select(keydir, keydir_match_spec(), @batch_size))}
    after
      :ets.safe_fixtable(keydir, false)
    end
  rescue
    ArgumentError -> {:error, :source_keydir_unavailable}
  end

  defp state_entry_chunk_present?(:"$end_of_table"), do: false

  defp state_entry_chunk_present?({entries, continuation}) do
    if Enum.any?(entries, &flow_state_entry?/1) do
      true
    else
      state_entry_chunk_present?(:ets.select(continuation))
    end
  end

  defp safe_prune_terminal_keydir_entry(
         keydir,
         shard_path,
         shard_index,
         key,
         record,
         instance_ctx
       ) do
    version = Map.get(record, :version)

    with [row] <- :ets.lookup(keydir, key),
         {:ok, current} <-
           prune_terminal_keydir_record(row, shard_path, shard_index, key, instance_ctx),
         ^version <- Map.get(current, :version),
         true <- LMDB.terminal_state?(Map.get(current, :state)) do
      track_binary_remove(keydir, shard_index, key, instance_ctx)
      delete_apply_projection_cache_for_row(instance_ctx, shard_index, row)
      :ets.delete(keydir, key)
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  defp delete_apply_projection_cache_for_row(
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
  rescue
    _ -> :ok
  end

  defp delete_apply_projection_cache_for_row(_instance_ctx, _shard_index, _row), do: :ok

  defp prune_terminal_keydir_record(
         {key, value, expire_at_ms, _lfu, _fid, _off, _vsize},
         _shard_path,
         shard_index,
         key,
         instance_ctx
       )
       when is_binary(value) do
    case ColdState.decode_state_record(key, value, expire_at_ms, shard_index, instance_ctx) do
      [{^key, _materialized, _expire_at_ms, record}] -> {:ok, record}
      _ -> :error
    end
  end

  defp prune_terminal_keydir_record(
         {key, nil, expire_at_ms, _lfu, fid, off, vsize},
         shard_path,
         shard_index,
         key,
         instance_ctx
       ) do
    shard_path
    |> ColdState.cold_locations_for_state(key, expire_at_ms, fid, off, vsize)
    |> ColdState.read_cold_locations(shard_index, instance_ctx)
    |> case do
      [{^key, _materialized, _expire_at_ms, record}] -> {:ok, record}
      _ -> :error
    end
  end

  defp prune_terminal_keydir_record(_row, _shard_path, _shard_index, _key, _instance_ctx),
    do: :error

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
