defmodule Ferricstore.Raft.StateMachine.Sections.PendingLocations do
  @moduledoc false

  import Kernel, except: [apply: 3]

  defmacro __using__(_opts) do
    quote do
      import Kernel, except: [apply: 3]
      import Bitwise

      require Logger

      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.CommandTime
      alias Ferricstore.Commands.Dispatcher
      alias Ferricstore.Commands.HyperLogLog
      alias Ferricstore.Raft.BlobCommand
      alias Ferricstore.Flow
      alias Ferricstore.Flow.Hibernation
      alias Ferricstore.Flow.HistoryProjector
      alias Ferricstore.Flow.Locator
      alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
      alias Ferricstore.Flow.Keys, as: FlowKeys
      alias Ferricstore.Flow.RetryPolicy
      alias Ferricstore.HLC

      alias Ferricstore.Store.{
        BitcaskWriter,
        BlobRef,
        BlobStore,
        BlobValue,
        ColdRead,
        CompoundKey,
        ExpiryTracker,
        LFU,
        ListOps,
        Promotion,
        Router,
        ValueCodec
      }

      alias Ferricstore.Store.Shard.ZSetIndex
      alias Ferricstore.Store.Shard.CompoundMemberIndex
      alias Ferricstore.Store.Shard.LogicalKeyIndex
      alias Ferricstore.Store.Shard.Transaction, as: ShardTransaction
      alias Ferricstore.Store.Shard.Flush, as: ShardFlush
      alias Ferricstore.Transaction.Ast, as: TxAst

      defp do_apply_fast_staged_put_pending_locations(
             _state,
             _file_id,
             [],
             [],
             _hot_threshold
           ) do
        :ok
      end

      defp do_apply_fast_staged_put_pending_locations(
             state,
             file_id,
             batch,
             locations,
             hot_threshold
           ) do
        refs =
          do_apply_fast_staged_put_pending_locations(
            state,
            file_id,
            batch,
            locations,
            hot_threshold,
            []
          )

        delete_apply_projection_cache_refs(state, refs)
      end

      defp do_apply_fast_staged_put_pending_locations(
             _state,
             _file_id,
             [],
             [],
             _hot_threshold,
             refs
           ),
           do: refs

      defp do_apply_fast_staged_put_pending_locations(
             state,
             file_id,
             [{:put, key, value, expire_at_ms} | batch],
             [{:put, offset, value_size} | locations],
             hot_threshold,
             refs
           ) do
        expected_value = value_for_ets(value, hot_threshold)
        expected_staged_size = byte_size(to_disk_binary(value))

        refs =
          case safe_ets_lookup(state.ets, key) do
            [{^key, ^expected_value, ^expire_at_ms, lfu, :pending, 0, ^expected_staged_size}] ->
              refs = maybe_prepend_apply_projection_cache_ref(state, key, refs, file_id)

              safe_ets_insert(
                state.ets,
                {key, expected_value, expire_at_ms, lfu, file_id, offset, value_size}
              )

              CompoundMemberIndex.put(
                Map.get(state, :compound_member_index_name),
                key,
                expire_at_ms
              )

              logical_key_index_put(state, key, value, expire_at_ms)
              refs

            _other ->
              apply_put_pending_location(
                state,
                key,
                value,
                expire_at_ms,
                file_id,
                offset,
                value_size
              )

              refs
          end

        do_apply_fast_staged_put_pending_locations(
          state,
          file_id,
          batch,
          locations,
          hot_threshold,
          refs
        )
      end

      defp do_apply_fast_staged_put_pending_locations(
             state,
             file_id,
             [{:put_cold, key, value, expire_at_ms, lfu} | batch],
             [{:put, offset, value_size} | locations],
             hot_threshold,
             refs
           ) do
        expected_staged_size = byte_size(to_disk_binary(value))

        refs =
          case safe_ets_lookup(state.ets, key) do
            [{^key, nil, ^expire_at_ms, ^lfu, :pending, 0, ^expected_staged_size}] ->
              refs = maybe_prepend_apply_projection_cache_ref(state, key, refs, file_id)

              safe_ets_insert(
                state.ets,
                {key, nil, expire_at_ms, lfu, file_id, offset, value_size}
              )

              CompoundMemberIndex.put(
                Map.get(state, :compound_member_index_name),
                key,
                expire_at_ms
              )

              logical_key_index_put(state, key, value, expire_at_ms)
              refs

            _other ->
              apply_put_cold_pending_location(
                state,
                key,
                value,
                expire_at_ms,
                lfu,
                file_id,
                offset,
                value_size
              )

              refs
          end

        do_apply_fast_staged_put_pending_locations(
          state,
          file_id,
          batch,
          locations,
          hot_threshold,
          refs
        )
      end

      defp batch_has_duplicate_put_key?([_, _ | _] = batch) do
        batch
        |> Enum.reduce_while(MapSet.new(), fn
          {:put, key, _value, _expire_at_ms}, seen ->
            if MapSet.member?(seen, key), do: {:halt, true}, else: {:cont, MapSet.put(seen, key)}

          {:put_cold, key, _value, _expire_at_ms, _lfu}, seen ->
            if MapSet.member?(seen, key), do: {:halt, true}, else: {:cont, MapSet.put(seen, key)}

          _entry, seen ->
            {:cont, seen}
        end)
        |> case do
          true -> true
          _seen -> false
        end
      end

      defp batch_has_duplicate_put_key?(_batch), do: false

      defp apply_final_staged_put_pending_locations(
             state,
             file_id,
             batch,
             locations,
             _hot_threshold
           ) do
        final_indexes = final_put_key_indexes(batch)

        batch
        |> Enum.zip(locations)
        |> Enum.with_index()
        |> Enum.each(fn
          {{{:put, key, value, expire_at_ms}, {:put, offset, value_size}}, index} ->
            if Map.get(final_indexes, key) == index do
              apply_put_pending_location(
                state,
                key,
                value,
                expire_at_ms,
                file_id,
                offset,
                value_size
              )
            end

          {{{:put_cold, key, value, expire_at_ms, lfu}, {:put, offset, value_size}}, index} ->
            if Map.get(final_indexes, key) == index do
              apply_put_cold_pending_location(
                state,
                key,
                value,
                expire_at_ms,
                lfu,
                file_id,
                offset,
                value_size
              )
            end

          _other ->
            :ok
        end)

        :ok
      end

      defp final_put_key_indexes(batch) do
        batch
        |> Enum.with_index()
        |> Enum.reduce(%{}, fn
          {{:put, key, _value, _expire_at_ms}, index}, acc -> Map.put(acc, key, index)
          {{:put_cold, key, _value, _expire_at_ms, _lfu}, index}, acc -> Map.put(acc, key, index)
          {_entry, _index}, acc -> acc
        end)
      end

      defp apply_pending_locations(_state, _file_id, [], [], _staged?), do: :ok

      defp apply_pending_locations(
             state,
             file_id,
             [{:put, key, val, exp} | batch],
             [{:put, offset, value_size} | locations],
             staged?
           ) do
        apply_put_pending_location(state, key, val, exp, file_id, offset, value_size)
        apply_pending_locations(state, file_id, batch, locations, staged?)
      end

      defp apply_pending_locations(
             state,
             file_id,
             [{:put_cold, key, val, exp, lfu} | batch],
             [{:put, offset, value_size} | locations],
             staged?
           ) do
        apply_put_cold_pending_location(state, key, val, exp, lfu, file_id, offset, value_size)
        apply_pending_locations(state, file_id, batch, locations, staged?)
      end

      defp apply_pending_locations(
             state,
             file_id,
             [{:delete, key, nil} | batch],
             [{:delete, _offset, _record_size} | locations],
             staged?
           ) do
        delete_apply_projection_cache_for_pending_original(state, key)

        if staged? do
          track_keydir_binary_remove(state, key)
          :ets.delete(state.ets, key)
          maybe_queue_lmdb_state_delete_after_publish(state, key)
        end

        CompoundMemberIndex.delete(Map.get(state, :compound_member_index_name), key)
        logical_key_index_delete(state, key)
        apply_pending_locations(state, file_id, batch, locations, staged?)
      end

      defp apply_pending_locations(
             state,
             file_id,
             [{:delete, key, prob_path} | batch],
             [{:delete, _offset, _record_size} | locations],
             staged?
           ) do
        delete_apply_projection_cache_for_pending_original(state, key)

        if staged? do
          track_keydir_binary_remove(state, key)
          :ets.delete(state.ets, key)
          maybe_queue_lmdb_state_delete_after_publish(state, key)
        end

        CompoundMemberIndex.delete(Map.get(state, :compound_member_index_name), key)
        logical_key_index_delete(state, key)
        maybe_delete_prob_file_path(state, prob_path)
        apply_pending_locations(state, file_id, batch, locations, staged?)
      end

      defp apply_put_cold_pending_location(
             state,
             key,
             value,
             expire_at_ms,
             lfu,
             file_id,
             offset,
             value_size
           ) do
        if standalone_staged_apply?() do
          delete_apply_projection_cache_for_pending_original(state, key, file_id)
          track_keydir_binary_delta(state, key, nil, expire_at_ms)
          :ets.insert(state.ets, {key, nil, expire_at_ms, lfu, file_id, offset, value_size})

          CompoundMemberIndex.put(
            Map.get(state, :compound_member_index_name),
            key,
            expire_at_ms
          )

          logical_key_index_put(state, key, value, expire_at_ms)
        else
          expected_staged_size = byte_size(to_disk_binary(value))

          replaced =
            replace_current_pending_location(
              state,
              key,
              value,
              expire_at_ms,
              expected_staged_size,
              file_id,
              offset,
              value_size
            )

          replaced =
            if replaced > 0 do
              replaced
            else
              replace_pending_location(
                state,
                key,
                nil,
                expire_at_ms,
                expected_staged_size,
                file_id,
                offset,
                value_size
              )
            end

          if replaced > 0 do
            delete_apply_projection_cache_for_pending_original(state, key, file_id)

            CompoundMemberIndex.put(
              Map.get(state, :compound_member_index_name),
              key,
              expire_at_ms
            )

            logical_key_index_put(state, key, value, expire_at_ms)
          end
        end

        :ok
      end

      defp apply_put_pending_location(
             state,
             key,
             value,
             expire_at_ms,
             file_id,
             offset,
             value_size
           ) do
        expected_value = value_for_ets(value, hot_cache_threshold(state))
        expected_staged_size = byte_size(to_disk_binary(value))

        if standalone_staged_apply?() do
          delete_apply_projection_cache_for_pending_original(state, key, file_id)
          track_keydir_binary_delta(state, key, expected_value, expire_at_ms)

          :ets.insert(
            state.ets,
            {key, expected_value, expire_at_ms, LFU.initial(), file_id, offset, value_size}
          )

          CompoundMemberIndex.put(
            Map.get(state, :compound_member_index_name),
            key,
            expire_at_ms
          )

          logical_key_index_put(state, key, value, expire_at_ms)
        else
          replaced =
            replace_current_pending_location(
              state,
              key,
              value,
              expire_at_ms,
              expected_staged_size,
              file_id,
              offset,
              value_size
            )

          replaced =
            if replaced > 0 do
              replaced
            else
              replace_pending_location(
                state,
                key,
                expected_value,
                expire_at_ms,
                expected_staged_size,
                file_id,
                offset,
                value_size
              )
            end

          if replaced == 0 and expected_staged_size != 0 do
            # Older staged writes can carry vsize=0; state-machine apply must still
            # CAS on value/expiry so stale append results cannot publish.
            fallback_replaced =
              replace_pending_location(
                state,
                key,
                expected_value,
                expire_at_ms,
                0,
                file_id,
                offset,
                value_size
              )

            if fallback_replaced > 0 do
              delete_apply_projection_cache_for_pending_original(state, key, file_id)

              CompoundMemberIndex.put(
                Map.get(state, :compound_member_index_name),
                key,
                expire_at_ms
              )

              logical_key_index_put(state, key, value, expire_at_ms)
            end
          else
            if replaced > 0 do
              delete_apply_projection_cache_for_pending_original(state, key, file_id)

              CompoundMemberIndex.put(
                Map.get(state, :compound_member_index_name),
                key,
                expire_at_ms
              )

              logical_key_index_put(state, key, value, expire_at_ms)
            end
          end
        end

        :ok
      end

      defp logical_key_index_put(state, key, value, expire_at_ms) do
        :ok =
          LogicalKeyIndex.put(
            Map.get(state, :logical_key_index_name),
            Map.get(state, :logical_key_slots_name),
            key,
            value,
            expire_at_ms
          )
      end

      defp logical_key_index_delete(state, key) do
        :ok =
          LogicalKeyIndex.delete(
            Map.get(state, :logical_key_index_name),
            Map.get(state, :logical_key_slots_name),
            key
          )
      end

      defp maybe_prepend_apply_projection_cache_ref(state, key, refs, current_file_id) do
        case apply_projection_cache_ref_for_pending_original(state, key) do
          nil -> refs
          ref -> maybe_prepend_apply_projection_cache_ref_result(ref, refs, current_file_id)
        end
      end

      defp maybe_prepend_apply_projection_cache_ref_result(
             {index, _key},
             refs,
             {:waraft_apply_projection, index}
           ),
           do: refs

      defp maybe_prepend_apply_projection_cache_ref_result(ref, refs, _current_file_id),
        do: [ref | refs]

      defp apply_projection_cache_ref_for_pending_original(state, key) do
        case Process.get(:sm_pending_originals, %{}) do
          %{^key => {:entry, row}} -> apply_projection_cache_ref_for_row(state, row)
          _ -> nil
        end
      rescue
        _ -> nil
      end

      defp apply_projection_cache_ref_for_row(
             _state,
             {key, _value, _expire_at_ms, _lfu, {:waraft_apply_projection, index}, _offset,
              _value_size}
           )
           when is_binary(key) and is_integer(index) and index > 0,
           do: {index, key}

      defp apply_projection_cache_ref_for_row(_state, _row), do: nil

      defp delete_apply_projection_cache_refs(_state, []), do: :ok

      defp delete_apply_projection_cache_refs(
             %{data_dir: data_dir, shard_index: shard_index},
             refs
           )
           when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
                  is_list(refs) do
        Ferricstore.Raft.WARaftSegmentReader.delete_apply_projection_entries(
          data_dir,
          shard_index,
          refs
        )

        :ok
      rescue
        _ -> :ok
      end

      defp delete_apply_projection_cache_refs(_state, _refs), do: :ok

      defp delete_apply_projection_cache_for_pending_original(state, key, current_file_id \\ nil) do
        case Process.get(:sm_pending_originals, %{}) do
          %{^key => {:entry, row}} ->
            delete_apply_projection_cache_for_row(state, row, current_file_id)

          _ ->
            :ok
        end
      rescue
        _ -> :ok
      end

      defp delete_apply_projection_cache_for_row(
             _state,
             {_key, _value, _expire_at_ms, _lfu, {:waraft_apply_projection, index}, _offset,
              _value_size},
             {:waraft_apply_projection, index}
           ),
           do: :ok

      defp delete_apply_projection_cache_for_row(
             %{data_dir: data_dir, shard_index: shard_index},
             {key, _value, _expire_at_ms, _lfu, {:waraft_apply_projection, index}, _offset,
              _value_size},
             _current_file_id
           )
           when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
                  is_binary(key) and is_integer(index) and index > 0 do
        Ferricstore.Raft.WARaftSegmentReader.delete_apply_projection_entries(
          data_dir,
          shard_index,
          [
            {index, key}
          ]
        )

        :ok
      rescue
        _ -> :ok
      end

      defp delete_apply_projection_cache_for_row(_state, _row, _current_file_id), do: :ok

      defp replace_pending_location(
             state,
             key,
             expected_value,
             expire_at_ms,
             expected_staged_size,
             file_id,
             offset,
             value_size
           ) do
        try do
          file_id_spec = pending_location_file_id_matchspec(file_id)

          :ets.select_replace(state.ets, [
            {
              {key, expected_value, expire_at_ms, :"$1", :pending, 0, expected_staged_size},
              [],
              [{{key, expected_value, expire_at_ms, :"$1", file_id_spec, offset, value_size}}]
            }
          ])
        rescue
          ArgumentError -> 0
        end
      end

      defp replace_current_pending_location(
             state,
             key,
             value,
             expire_at_ms,
             expected_staged_size,
             file_id,
             offset,
             value_size
           ) do
        case Process.get(:sm_pending_values, %{}) do
          %{^key => {^value, ^expire_at_ms}} ->
            try do
              file_id_spec = pending_location_file_id_matchspec(file_id)

              :ets.select_replace(state.ets, [
                {
                  {key, :"$1", expire_at_ms, :"$2", :pending, 0, expected_staged_size},
                  [
                    {:orelse, {:==, :"$1", nil}, {:==, :"$1", value}}
                  ],
                  [
                    {{key, :"$1", expire_at_ms, :"$2", file_id_spec, offset, value_size}}
                  ]
                }
              ])
            rescue
              ArgumentError -> 0
            end

          _stale_or_missing ->
            0
        end
      end

      defp pending_location_file_id_matchspec(file_id) when is_tuple(file_id),
        do: {:const, file_id}

      defp pending_location_file_id_matchspec(file_id), do: file_id

      defp queue_pending_put(key, value, expire_at_ms) do
        pending = Process.get(:sm_pending_writes, [])
        Process.put(:sm_pending_writes, [{:put, key, value, expire_at_ms} | pending])
        pending_values = Process.get(:sm_pending_values, %{})
        Process.put(:sm_pending_values, Map.put(pending_values, key, {value, expire_at_ms}))
      end

      defp queue_pending_put_cold(key, value, expire_at_ms, lfu) do
        pending = Process.get(:sm_pending_writes, [])
        Process.put(:sm_pending_writes, [{:put_cold, key, value, expire_at_ms, lfu} | pending])
        pending_values = Process.get(:sm_pending_values, %{})
        Process.put(:sm_pending_values, Map.put(pending_values, key, {value, expire_at_ms}))
      end

      defp queue_pending_flow_history_projection(entry) do
        pending = Process.get(:sm_pending_flow_history_projections, [])
        Process.put(:sm_pending_flow_history_projections, [entry | pending])
        :ok
      end

      defp queue_pending_flow_history_projections_batch([]), do: :ok

      defp queue_pending_flow_history_projections_batch(entries) when is_list(entries) do
        pending = Process.get(:sm_pending_flow_history_projections, [])
        Process.put(:sm_pending_flow_history_projections, Enum.reverse(entries, pending))
        :ok
      end

      defp publish_pending_flow_history_projections(state) do
        case Process.get(:sm_pending_flow_history_projections, []) do
          [] ->
            :ok

          pending when is_list(pending) ->
            entries = Enum.reverse(pending)
            ra_index = current_ra_index()
            ctx = checkpoint_ctx_for_state(state)

            publish_pending_flow_history_projection_entries(state, ctx, entries, ra_index)
        end
      end

      defp publish_pending_flow_history_projection_entries(state, ctx, entries, ra_index) do
        if flow_history_projection_same_shard?(ctx, state, entries) do
          publish_pending_flow_history_projection_shard(
            state,
            ctx,
            state.shard_index,
            entries,
            ra_index
          )
        else
          entries
          |> Enum.group_by(&flow_history_projection_shard(ctx, state, &1))
          |> Enum.reduce_while(:ok, fn {shard_index, shard_entries}, :ok ->
            case publish_pending_flow_history_projection_shard(
                   state,
                   ctx,
                   shard_index,
                   shard_entries,
                   ra_index
                 ) do
              :ok -> {:cont, :ok}
              {:error, _reason} = error -> {:halt, error}
            end
          end)
        end
      end

      defp flow_history_projection_same_shard?(_ctx, %{shard_index: shard_index}, [
             %{shard_index: shard_index} | _
           ])
           when is_integer(shard_index) and shard_index >= 0,
           do: true

      defp flow_history_projection_same_shard?(ctx, state, entries) do
        Enum.all?(entries, &(flow_history_projection_shard(ctx, state, &1) == state.shard_index))
      end

      defp flow_history_projection_shard(_ctx, _state, %{shard_index: shard_index})
           when is_integer(shard_index) and shard_index >= 0,
           do: shard_index

      defp flow_history_projection_shard(ctx, state, %{key: key})
           when is_map(ctx) and is_binary(key) do
        Router.shard_for(ctx, key)
      rescue
        _ -> state.shard_index
      catch
        :exit, _ -> state.shard_index
      end

      defp flow_history_projection_shard(nil, state, %{key: key})
           when is_binary(key) do
        state.shard_index
      end

      defp flow_history_projection_shard(_ctx, state, _entry), do: state.shard_index

      defp publish_pending_flow_history_projection_shard(
             state,
             ctx,
             shard_index,
             entries,
             ra_index
           ) do
        result =
          case HistoryProjector.enqueue_async(ctx, shard_index, entries, ra_index) do
            :ok -> :ok
            {:error, reason} -> {:error, reason}
          end

        case result do
          :ok ->
            record_waraft_replay_dependency(:history, shard_index, ra_index)
            :ok

          {:error, reason} ->
            {:error, {:flow_history_projection_failed, reason}}
        end
      end

      defp handle_flow_history_projection_publish_failure(state, reason) do
        block_release_cursor_for_apply()

        :telemetry.execute(
          [:ferricstore, :flow, :history_projection, :publish_failed],
          %{count: 1},
          %{shard_index: Map.get(state, :shard_index), reason: reason}
        )
      rescue
        _ -> :ok
      end

      defp record_waraft_replay_dependency(kind, shard_index, index)
           when kind in [:history] and is_integer(shard_index) and shard_index >= 0 and
                  is_integer(index) and index > 0 do
        dependencies =
          apply_state_get(:waraft_replay_dependencies, %{history: %{}, apply_projection: %{}})

        updated =
          dependencies
          |> Map.update(kind, %{shard_index => index}, fn by_shard ->
            Map.update(by_shard, shard_index, index, &max(&1, index))
          end)

        apply_state_put(:waraft_replay_dependencies, updated)
      end

      defp record_waraft_replay_dependency(_kind, _shard_index, _index), do: :ok

      defp queue_pending_delete(key, prob_path) do
        pending = Process.get(:sm_pending_writes, [])
        Process.put(:sm_pending_writes, [{:delete, key, prob_path} | pending])
        Process.put(:sm_pending_has_delete, true)
        pending_values = Process.get(:sm_pending_values, %{})
        Process.put(:sm_pending_values, Map.put(pending_values, key, :deleted))
      end

      defp queue_pending_delete_fast(key, prob_path) do
        pending = Process.get(:sm_pending_writes, [])
        Process.put(:sm_pending_writes, [{:delete, key, prob_path} | pending])
        Process.put(:sm_pending_has_delete, true)

        unmaterialized = Process.get(:sm_pending_unmaterialized_fast_delete_keys, [])
        Process.put(:sm_pending_unmaterialized_fast_delete_keys, [key | unmaterialized])
        :ok
      end

      defp standalone_staged_apply?, do: Process.get(@sm_standalone_staged_key) == true

      defp waraft_segment_projection_apply?,
        do: is_function(Process.get(@sm_waraft_projection_writer_key))

      defp emit_raft_apply_telemetry(state, started_at, result, flush_result) do
        :telemetry.execute(
          [:ferricstore, :raft, :apply],
          %{duration_us: duration_us(started_at)},
          %{
            shard_index: state.shard_index,
            result: result_class(result),
            disk: flush_result_class(flush_result)
          }
        )
      end

      defp emit_bitcask_append_telemetry(
             state,
             started_at,
             batch_size,
             batch_bytes,
             delete_count,
             append_result
           ) do
        :telemetry.execute(
          [:ferricstore, :bitcask, :append],
          %{
            duration_us: duration_us(started_at),
            batch_size: batch_size,
            batch_bytes: batch_bytes,
            delete_count: delete_count
          },
          %{shard_index: state.shard_index, status: append_result_class(append_result)}
        )
      end

      defp result_class({:error, _}), do: :error
      defp result_class(_), do: :ok

      defp flush_result_class(:ok), do: :ok
      defp flush_result_class({:error, _}), do: :error
      defp flush_result_class(_), do: :unknown

      defp append_result_class({:ok, _}), do: :ok
      defp append_result_class({:error, _}), do: :error
      defp append_result_class(:stale), do: :stale
      defp append_result_class(_), do: :unknown

      defp set_disk_pressure(state) do
        case checkpoint_ctx_for_state(state) do
          nil ->
            Ferricstore.Store.DiskPressure.set(state.shard_index)

          ctx ->
            Ferricstore.Store.DiskPressure.set(ctx, state.shard_index)
        end
      end

      defp clear_disk_pressure(state) do
        case checkpoint_ctx_for_state(state) do
          nil ->
            Ferricstore.Store.DiskPressure.clear(state.shard_index)

          ctx ->
            flag_idx = state.shard_index + 1

            if flag_idx <= :atomics.info(ctx.checkpoint_flags).size do
              remember_checkpoint_clean_before_write(state, ctx)
              remember_checkpoint_dependencies_clean_before_write(state)
              :atomics.put(ctx.checkpoint_flags, flag_idx, 1)
              record_checkpoint_dirty_index(state.shard_index)
            end

            Ferricstore.Store.DiskPressure.clear(ctx, state.shard_index)
        end
      end

      defp remember_checkpoint_clean_before_write(state, ctx) do
        if apply_state_get(:checkpoint_clean_before_write) != true and
             checkpoint_clean?(%{state | instance_ctx: ctx}) do
          apply_state_put(:checkpoint_clean_before_write, true)
        end
      rescue
        _ -> :ok
      end

      defp checkpoint_ctx_for_state(%{instance_ctx: %FerricStore.Instance{} = ctx}), do: ctx

      defp checkpoint_ctx_for_state(%{instance_ctx: ctx}) when is_map(ctx), do: ctx

      defp checkpoint_ctx_for_state(%{instance_name: name} = state) when is_atom(name),
        do: instance_ctx_for_state(state)

      defp checkpoint_ctx_for_state(_state), do: nil

      defp instance_data_path?(
             %FerricStore.Instance{data_dir_expanded: data_dir},
             %{shard_data_path_expanded: shard_data_path}
           )
           when is_binary(data_dir) and is_binary(shard_data_path) do
        shard_data_path == data_dir or String.starts_with?(shard_data_path, data_dir <> "/")
      end

      defp instance_data_path?(_ctx, _state), do: false

      defp initial_file_stats(shard_data_path, ets, active_file_id) do
        stats = ShardFlush.compute_file_stats(shard_data_path, ets)

        Map.put_new(
          stats,
          active_file_id,
          {file_size_or_zero(bitcask_file_path(shard_data_path, active_file_id)), 0}
        )
      end

      defp bitcask_file_path(shard_data_path, file_id) do
        Path.join(
          shard_data_path,
          "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log"
        )
      end

      defp file_size_or_zero(path) do
        case File.stat(path) do
          {:ok, %{size: size}} -> size
          _ -> 0
        end
      end

      defp default_merge_config do
        %{
          fragmentation_threshold: @default_fragmentation_threshold,
          dead_bytes_threshold: @default_dead_bytes_threshold
        }
      end

      defp flow_async_history_config(_config), do: true

      defp duration_us(started_at) do
        System.monotonic_time()
        |> Kernel.-(started_at)
        |> System.convert_time_unit(:native, :microsecond)
      end

      defp record_pending_original(state, key) do
        originals = Process.get(:sm_pending_originals, %{})
        previous = safe_ets_lookup(state.ets, key)
        updated = record_pending_original_from_previous(key, previous, originals)

        if updated != originals do
          Process.put(:sm_pending_originals, updated)
        end
      end

      defp record_pending_original_from_previous(key, previous, originals) do
        if Map.has_key?(originals, key) do
          originals
        else
          Map.put(originals, key, pending_original_from_previous(previous))
        end
      end

      defp pending_original_from_previous([entry]), do: {:entry, entry}
      defp pending_original_from_previous([]), do: :missing

      defp flow_lmdb_record_path(state), do: Map.fetch!(state, :flow_lmdb_path)

      defp flow_hibernation_enabled?(state),
        do: Hibernation.enabled?(raft_apply_context(state))

      defp maybe_queue_flow_hibernation_candidate(state, key, record, state_value)
           when is_binary(key) and is_map(record) do
        context = raft_apply_context(state)

        if flow_hibernation_enabled?(state) and
             Hibernation.demotable?(record, apply_now_ms(),
               hot_window_ms: Hibernation.hot_window_ms(context),
               safety_margin_ms: Hibernation.safety_margin_ms(context)
             ) do
          pending = Process.get(:sm_pending_flow_hibernation_candidates, [])

          Process.put(:sm_pending_flow_hibernation_candidates, [
            {key, record, state_value} | pending
          ])
        end

        :ok
      end

      defp maybe_queue_flow_hibernation_candidate(_state, _key, _record, _state_value), do: :ok

      defp queue_pending_lmdb_mirror_put(key, value, expire_at_ms) when is_binary(value) do
        queue_pending_lmdb_mirror_op(
          {:put, key, Ferricstore.Flow.LMDB.encode_value(value, expire_at_ms)}
        )

        :ok
      end

      defp queue_pending_lmdb_mirror_put(key, _value, _expire_at_ms) do
        queue_pending_lmdb_mirror_op({:project_kv_from_source, key})
        :ok
      end

      defp maybe_queue_lmdb_indexes_for_state_record(
             state,
             state_key,
             value,
             expire_at_ms,
             record
           )
           when is_map(record) do
        with_lmdb_mirror_shard(state, fn ->
          cond do
            flow_record_has_indexed_attributes?(record) and is_binary(value) and
                is_integer(expire_at_ms) ->
              queue_pending_lmdb_flow_state_projection(state_key, value, expire_at_ms)
              maybe_queue_lmdb_terminal_state_prune_after_flush(state, state_key, record)

            Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) ->
              case Ferricstore.Flow.LMDB.mode() do
                :lagged ->
                  :ok

                _mode ->
                  queue_pending_lmdb_projection_outbox(state_key, Map.fetch!(record, :version))
              end

            flow_record_has_indexed_attributes?(record) ->
              queue_pending_lmdb_flow_state_projection_from_source(state_key)

            true ->
              :ok
          end
        end)

        :ok
      end

      defp flow_record_has_indexed_attributes?(record) when is_map(record) do
        flow_record_has_attributes?(record) or flow_record_has_indexed_state_meta?(record)
      end

      defp flow_record_has_indexed_attributes?(_record), do: false

      defp maybe_queue_lmdb_terminal_state_prune_after_flush(state, state_key, record)
           when is_binary(state_key) and is_map(record) do
        with true <- Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)),
             id when is_binary(id) <- Map.get(record, :id),
             type when is_binary(type) <- Map.get(record, :type),
             terminal_state when is_binary(terminal_state) <- Map.get(record, :state),
             version when is_integer(version) <- Map.get(record, :version),
             data_dir when is_binary(data_dir) <- Map.get(state, :data_dir),
             shard_index when is_integer(shard_index) and shard_index >= 0 <-
               Map.get(state, :shard_index) do
          {zset_index, zset_lookup} =
            ZSetIndex.table_names(Map.get(state, :instance_name), shard_index)

          {flow_index, flow_lookup} =
            NativeFlowIndex.table_names(Map.get(state, :instance_name), shard_index)

          action =
            {:prune_terminal_flow, data_dir, shard_index, state.ets, zset_index, zset_lookup,
             flow_index, flow_lookup, state_key, type, terminal_state,
             Map.get(record, :partition_key), Map.get(record, :parent_flow_id),
             Map.get(record, :root_flow_id), Map.get(record, :correlation_id), id, version}

          queue_pending_lmdb_mirror_after_flush(
            {:defer_after_flush, Ferricstore.Flow.LMDBWriter.terminal_hot_ttl_ms(), action}
          )
        else
          _ -> :ok
        end

        :ok
      end

      defp maybe_queue_lmdb_terminal_state_prune_after_flush(_state, _state_key, _record),
        do: :ok

      defp flow_record_has_attributes?(record) do
        case Map.get(record, :attributes) do
          attrs when is_map(attrs) -> map_size(attrs) > 0
          _other -> false
        end
      end

      defp flow_record_has_indexed_state_meta?(record) do
        case {Map.get(record, :state_meta), Map.get(record, :indexed_state_meta)} do
          {meta, key} when is_map(meta) and is_binary(key) -> map_size(meta) > 0
          _other -> false
        end
      end
    end
  end
end
