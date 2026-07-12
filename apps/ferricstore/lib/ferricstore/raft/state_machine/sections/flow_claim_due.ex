defmodule Ferricstore.Raft.StateMachine.Sections.FlowClaimDue do
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
      alias Ferricstore.Store.Shard.Transaction, as: ShardTransaction
      alias Ferricstore.Store.Shard.Flush, as: ShardFlush
      alias Ferricstore.Transaction.Ast, as: TxAst

      defp flow_create_many_apply(state, plans) do
        if flow_create_fast_staged_put_batch?(state) do
          Process.put(:sm_pending_fast_staged_put_batch, true)
        end

        records = Enum.map(plans, fn {record, _attrs} -> record end)

        with :ok <- flow_create_put_record_values(state, plans),
             :ok <- flow_create_put_state_records(state, records),
             :ok <- flow_create_put_type_catalog_members(state, records),
             :ok <- flow_create_put_registry_markers(state, records),
             :ok <- flow_due_put_many_new(state, records),
             :ok <- flow_index_put_many_new(state, records),
             :ok <- flow_create_put_history(state, records) do
          :ok
        end
      end

      defp flow_create_many_fast_apply(state, plans) do
        if flow_create_fast_staged_put_batch?(state) do
          Process.put(:sm_pending_fast_staged_put_batch, true)
        end

        with :ok <- flow_create_fast_put_record_values(state, plans),
             :ok <- flow_create_put_fast_state_records(state, plans),
             :ok <-
               flow_create_put_type_catalog_members(
                 state,
                 Enum.map(plans, &Map.fetch!(&1, :record))
               ),
             :ok <- flow_create_put_fast_registry_markers(state, plans),
             :ok <- flow_create_put_fast_indexes(state, plans),
             :ok <- flow_create_put_fast_history(state, plans) do
          :ok
        end
      end

      defp flow_create_fast_put_record_values(state, plans) do
        Enum.reduce_while(plans, :ok, fn %{record: record, attrs: attrs}, :ok ->
          case flow_put_record_values(state, record, attrs) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp flow_create_put_registry_markers(state, records) do
        Enum.reduce_while(records, :ok, fn record, :ok ->
          case flow_create_put_registry_marker(state, record) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp flow_create_put_type_catalog_members(state, records) do
        Enum.reduce_while(records, :ok, fn record, :ok ->
          case flow_put_type_catalog_member(state, record) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp flow_create_put_fast_registry_markers(state, plans) do
        Enum.reduce_while(plans, :ok, fn %{registry_key: key, record: record}, :ok ->
          case flow_put_registry_marker(state, key, record) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp flow_create_put_registry_marker(state, %{id: id} = record) when is_binary(id) do
        flow_put_registry_marker(
          state,
          FlowKeys.registry_key(id, Map.get(record, :partition_key)),
          record
        )
      end

      defp flow_put_registry_marker(state, key, record) do
        flow_put_hot(state, key, <<1>>, flow_record_expire_at(record))
      end

      defp flow_create_fast_staged_put_batch?(state) do
        not cross_shard_pending_active?() and not standalone_staged_apply?() and
          Map.get(state, :cross_shard_locks, %{}) == %{} and
          Process.get(:sm_pending_writes, []) == [] and
          Process.get(:sm_pending_values, %{}) == %{}
      end

      @flow_claim_due_phase_telemetry Application.compile_env(
                                        :ferricstore,
                                        :flow_claim_due_phase_telemetry,
                                        false
                                      )

      if @flow_claim_due_phase_telemetry do
        defmacrop flow_claim_due_phase(phase, metadata, fun) do
          quote do
            flow_claim_due_phase_emit(unquote(phase), unquote(metadata), unquote(fun))
          end
        end

        defmacrop flow_claim_due_internal_phase(phase, metadata, measurements, fun) do
          quote do
            flow_claim_due_internal_phase_emit(
              unquote(phase),
              unquote(metadata),
              unquote(measurements),
              unquote(fun)
            )
          end
        end
      else
        defmacrop flow_claim_due_phase(phase, metadata, fun) do
          quote generated: true do
            _ =
              case false do
                true -> {unquote(phase), unquote(metadata)}
                false -> :ok
              end

            unquote(fun).()
          end
        end

        defmacrop flow_claim_due_internal_phase(phase, metadata, measurements, fun) do
          quote generated: true do
            _ =
              case false do
                true -> {unquote(phase), unquote(metadata), unquote(measurements)}
                false -> :ok
              end

            unquote(fun).()
          end
        end
      end

      defp do_flow_claim_due(
             state,
             %{
               type: type,
               state: state_filter,
               worker: worker,
               lease_ms: lease_ms,
               limit: limit,
               priority: priority
             } = attrs
           ) do
        with_flow_governance_limit(attrs, fn ->
          now_ms = flow_attrs_now_ms(attrs)
          partition_key = Map.get(attrs, :partition_keys) || Map.get(attrs, :partition_key)

          flow_claim_due_phase(
            :total,
            flow_claim_due_phase_meta(state, partition_key, priority, limit),
            fn ->
              state_filter =
                flow_claim_state_filter(state_filter, Map.get(attrs, :exclude_states, []))

              case flow_claim_due_priorities(
                     state,
                     attrs,
                     type,
                     state_filter,
                     worker,
                     lease_ms,
                     now_ms,
                     partition_key,
                     flow_claim_priorities(priority),
                     limit,
                     []
                   ) do
                {:error, _reason} = error -> error
                claimed -> {:ok, claimed}
              end
            end
          )
        end)
      end

      defp with_flow_governance_limit(attrs, fun) when is_map(attrs) and is_function(fun, 0) do
        case Map.get(attrs, :governance_limit) do
          %{
            scope: scope,
            shard_id: shard_id,
            enforcement: enforcement,
            reservation_ids: [_ | _] = reservation_ids
          } = reservation
          when is_binary(scope) and scope != "" and is_integer(shard_id) and shard_id >= 0 and
                 enforcement in [:strict_global, :approximate_global] ->
            if length(Enum.uniq(reservation_ids)) == length(reservation_ids) and
                 Enum.all?(reservation_ids, &(is_binary(&1) and &1 != "")) do
              with_valid_flow_governance_limit(reservation, fun)
            else
              {:error, "ERR invalid flow governance limit reservation"}
            end

          nil ->
            fun.()

          _invalid ->
            {:error, "ERR invalid flow governance limit reservation"}
        end
      end

      defp with_valid_flow_governance_limit(reservation, fun) do
        previous = Process.get(:sm_flow_governance_limit, :undefined)
        Process.put(:sm_flow_governance_limit, reservation)

        try do
          fun.()
        after
          case previous do
            :undefined -> Process.delete(:sm_flow_governance_limit)
            value -> Process.put(:sm_flow_governance_limit, value)
          end
        end
      end

      defp flow_claim_priorities(nil), do: [2, 1, 0]
      defp flow_claim_priorities(priority), do: [priority]

      @flow_due_any_index_enabled Application.compile_env(
                                    :ferricstore,
                                    :flow_due_any_index,
                                    false
                                  )
      defp flow_due_any_index_enabled?, do: @flow_due_any_index_enabled

      if @flow_claim_due_phase_telemetry do
        defp flow_claim_due_phase_emit(phase, metadata, fun) when is_function(fun, 0) do
          started_at = System.monotonic_time()
          result = fun.()

          measurements =
            result
            |> flow_claim_due_phase_measurements()
            |> Map.put(:duration_us, duration_us(started_at))

          :telemetry.execute(
            [:ferricstore, :flow, :claim_due_phase],
            measurements,
            Map.put(metadata, :phase, phase)
          )

          result
        end
      end

      if @flow_claim_due_phase_telemetry do
        defp flow_claim_due_phase_meta(state) do
          %{shard_index: Map.get(state, :shard_index)}
        end

        defp flow_claim_due_phase_meta(state, partition_key, priority, limit) do
          state
          |> flow_claim_due_phase_meta()
          |> Map.merge(%{
            partition_mode: flow_claim_due_partition_mode(partition_key),
            priority: priority,
            limit: limit
          })
        end
      else
        defp flow_claim_due_phase_meta(_state), do: %{}
        defp flow_claim_due_phase_meta(_state, _partition_key, _priority, _limit), do: %{}
      end

      if @flow_claim_due_phase_telemetry do
        defp flow_claim_due_partition_mode(:any), do: :any

        defp flow_claim_due_partition_mode(partition_keys) when is_list(partition_keys),
          do: :specific_many

        defp flow_claim_due_partition_mode(nil), do: :default
        defp flow_claim_due_partition_mode(_partition_key), do: :specific

        defp flow_claim_due_phase_measurements({:ok, records}) when is_list(records),
          do: %{items: length(records)}

        defp flow_claim_due_phase_measurements({plans, stale_due_ids, accepted})
             when is_list(plans) and is_list(stale_due_ids) and is_integer(accepted),
             do: %{plans: length(plans), stale_due_ids: length(stale_due_ids), accepted: accepted}

        defp flow_claim_due_phase_measurements({plans, stale_due_ids})
             when is_list(plans) and is_list(stale_due_ids),
             do: %{plans: length(plans), stale_due_ids: length(stale_due_ids)}

        defp flow_claim_due_phase_measurements({records, native_taken?})
             when is_list(records) and is_boolean(native_taken?),
             do: %{items: length(records)}

        defp flow_claim_due_phase_measurements(records) when is_list(records),
          do: %{items: length(records)}

        defp flow_claim_due_phase_measurements({:error, _reason}), do: %{errors: 1}
        defp flow_claim_due_phase_measurements(_result), do: %{}
      end

      if @flow_claim_due_phase_telemetry do
        defp flow_claim_due_internal_phase_emit(phase, metadata, measurements, fun)
             when is_function(fun, 0) do
          started_at = System.monotonic_time()
          result = fun.()

          :telemetry.execute(
            [:ferricstore, :flow, :claim_due_phase],
            Map.put(measurements, :duration_us, duration_us(started_at)),
            Map.put(metadata, :phase, phase)
          )

          result
        end
      end

      defp flow_claim_state_filter(state_filter, []), do: state_filter

      defp flow_claim_state_filter(state_filter, exclude_states),
        do: {:exclude, state_filter, exclude_states}

      defp flow_claim_due_priorities(
             _state,
             _attrs,
             _type,
             _state_filter,
             _worker,
             _lease_ms,
             _now_ms,
             _partition_key,
             _priorities,
             limit,
             claimed
           )
           when length(claimed) >= limit do
        claimed |> Enum.reverse() |> Enum.take(limit)
      end

      defp flow_claim_due_priorities(
             _state,
             _attrs,
             _type,
             _state_filter,
             _worker,
             _lease_ms,
             _now_ms,
             _partition_key,
             [],
             _limit,
             claimed
           ) do
        Enum.reverse(claimed)
      end

      defp flow_claim_due_priorities(
             state,
             attrs,
             type,
             state_filter,
             worker,
             lease_ms,
             now_ms,
             partition_key,
             [priority | rest],
             limit,
             claimed
           ) do
        due_keys =
          flow_claim_due_phase(
            :due_keys,
            flow_claim_due_phase_meta(state, partition_key, priority, limit),
            fn -> flow_claim_due_keys(state, type, state_filter, partition_key, priority) end
          )

        case flow_claim_due_scan_keys(
               state,
               due_keys,
               type,
               state_filter,
               worker,
               lease_ms,
               now_ms,
               partition_key,
               limit,
               claimed
             ) do
          {:error, _reason} = error ->
            error

          next_claimed ->
            next_claimed =
              maybe_promote_and_rescan_cold_due_for_claim(
                state,
                attrs,
                type,
                state_filter,
                worker,
                lease_ms,
                now_ms,
                partition_key,
                priority,
                due_keys,
                limit,
                claimed,
                next_claimed
              )

            case next_claimed do
              {:error, _reason} = error ->
                error

              next_claimed ->
                flow_claim_due_priorities(
                  state,
                  attrs,
                  type,
                  state_filter,
                  worker,
                  lease_ms,
                  now_ms,
                  partition_key,
                  rest,
                  limit,
                  next_claimed
                )
            end
        end
      end

      defp maybe_promote_and_rescan_cold_due_for_claim(
             state,
             attrs,
             type,
             state_filter,
             worker,
             lease_ms,
             now_ms,
             partition_key,
             priority,
             due_keys,
             limit,
             claimed,
             next_claimed
           ) do
        remaining = max(limit - length(next_claimed), 0)

        if flow_should_promote_cold_due_for_claim?(
             state,
             attrs,
             claimed,
             next_claimed,
             remaining
           ) do
          promoted =
            maybe_promote_cold_due_for_claim(
              state,
              type,
              state_filter,
              partition_key,
              priority,
              now_ms,
              remaining
            )

          if promoted > 0 do
            promoted_due_keys =
              flow_claim_due_keys(state, type, state_filter, partition_key, priority)

            flow_claim_due_scan_keys(
              state,
              promoted_due_keys,
              type,
              state_filter,
              worker,
              lease_ms,
              now_ms,
              partition_key,
              limit,
              next_claimed
            )
          else
            next_claimed
          end
        else
          next_claimed
        end
      end

      defp flow_should_promote_cold_due_for_claim?(
             state,
             attrs,
             claimed,
             next_claimed,
             remaining
           ) do
        mode = Map.get(attrs, :cold_due_mode, :skip)
        hot_miss? = length(next_claimed) == length(claimed)

        flow_hibernation_enabled?(state) and remaining > 0 and hot_miss? and
          mode in [:allow, :block]
      end

      defp maybe_promote_cold_due_for_claim(
             state,
             type,
             state_filter,
             partition_key,
             priority,
             now_ms,
             remaining
           ) do
        if flow_hibernation_enabled?(state) and remaining > 0 do
          promote_limit = flow_hibernation_promote_limit(remaining)
          path = flow_lmdb_record_path(state)

          now_ms
          |> flow_hibernation_promote_prefixes(state)
          |> Enum.reduce_while(0, fn prefix, promoted ->
            if promoted >= promote_limit do
              {:halt, promoted}
            else
              scan_limit = promote_limit - promoted

              case Ferricstore.Flow.LMDB.prefix_entries(path, prefix, scan_limit) do
                {:ok, entries} ->
                  next_promoted =
                    Enum.reduce_while(entries, promoted, fn {due_key, park_key}, acc ->
                      if acc >= promote_limit do
                        {:halt, acc}
                      else
                        case flow_promote_cold_due_entry(
                               state,
                               path,
                               due_key,
                               park_key,
                               type,
                               state_filter,
                               partition_key,
                               priority,
                               now_ms
                             ) do
                          :ok -> {:cont, acc + 1}
                          _ -> {:cont, acc}
                        end
                      end
                    end)

                  {:cont, next_promoted}

                _ ->
                  {:cont, promoted}
              end
            end
          end)
        else
          0
        end
      end

      defp flow_hibernation_promote_limit(remaining) do
        max(remaining * 4, min(remaining + 16, 128))
      end

      defp flow_hibernation_promote_prefixes(now_ms, state) do
        context = raft_apply_context(state)
        start_ms = max(now_ms - Hibernation.late_promote_window_ms(context), 0)
        horizon_ms = now_ms + Hibernation.promote_window_ms(context)
        Hibernation.promotion_bucket_prefixes(start_ms, horizon_ms, 60_000)
      end

      defp flow_promote_cold_due_entry(
             state,
             path,
             due_key,
             park_key,
             type,
             state_filter,
             partition_key,
             priority,
             now_ms
           )
           when is_binary(due_key) and is_binary(park_key) do
        with {:ok, park_blob} <- Ferricstore.Flow.LMDB.get(path, park_key),
             {:ok, %{locator: %Locator{kind: :state} = locator, state_key: state_key} = park} <-
               Ferricstore.Flow.LMDB.decode_cold_park(park_blob),
             true <- is_binary(state_key),
             {:ok, value} <- flow_read_cold_park_state_value(state, state_key, locator, park),
             record when is_map(record) <- flow_decode_hot_state_value(value),
             true <- flow_locator_matches_record?(locator, record),
             true <-
               flow_cold_due_record_matches_claim?(
                 record,
                 type,
                 state_filter,
                 partition_key,
                 priority,
                 now_ms
               ),
             :ok <- flow_install_hot_cold_record(state, state_key, value, locator, record) do
          %{locator: locator, park_key: park_key, due_key: due_key}
          |> Hibernation.cleanup_ops()
          |> Enum.each(&queue_pending_lmdb_mirror_op/1)

          :telemetry.execute(
            [:ferricstore, :flow, :hibernation, :promote],
            %{count: 1},
            %{result: :promoted, shard_index: Map.get(state, :shard_index)}
          )

          :ok
        else
          _ -> :skip
        end
      end

      defp flow_promote_cold_due_entry(
             _state,
             _path,
             _due_key,
             _park_key,
             _type,
             _state_filter,
             _partition_key,
             _priority,
             _now_ms
           ),
           do: :skip

      defp flow_cold_due_record_matches_claim?(
             record,
             type,
             state_filter,
             partition_key,
             priority,
             now_ms
           ) do
        Map.get(record, :type) == type and
          flow_claim_state_match?(state_filter, Map.get(record, :state)) and
          not flow_claim_state_excluded?(state_filter, Map.get(record, :state)) and
          flow_claim_partition_match?(partition_key, Map.get(record, :partition_key)) and
          flow_claim_priority_match?(priority, Map.get(record, :priority, 0)) and
          flow_claim_record_due_ready?(record, now_ms)
      end

      defp flow_claim_partition_match?(:any, _record_partition), do: true

      defp flow_claim_partition_match?(partitions, record_partition) when is_list(partitions),
        do: record_partition in partitions

      defp flow_claim_partition_match?(partition, partition), do: true
      defp flow_claim_partition_match?(_partition, _record_partition), do: false

      defp flow_claim_priority_match?(nil, _record_priority), do: true
      defp flow_claim_priority_match?(:any, _record_priority), do: true
      defp flow_claim_priority_match?(priority, priority), do: true
      defp flow_claim_priority_match?(_priority, _record_priority), do: false

      defp flow_install_hot_cold_record(state, state_key, value, %Locator{} = locator, record) do
        case :ets.lookup(state.ets, state_key) do
          [] ->
            ets_value = value_for_ets(value, hot_cache_threshold(state))
            track_keydir_binary_warm(state, ets_value)

            :ets.insert(
              state.ets,
              {state_key, ets_value, locator.expire_at_ms || 0, LFU.initial(), locator.file_id,
               locator.offset, locator.value_size}
            )

            case flow_install_hot_cold_indexes_now(state, record) do
              :ok ->
                :ok

              error ->
                :ets.delete(state.ets, state_key)
                error
            end

          _ ->
            :skip
        end
      rescue
        ArgumentError -> :skip
      end

      defp flow_install_hot_cold_indexes_now(state, record) do
        case flow_native_index(state) do
          nil ->
            {:error, :flow_native_index_unavailable}

          native ->
            entries = flow_hot_cold_index_entries(record)
            NativeFlowIndex.apply_batch(native, [{:put_entries, entries}])
        end
      end

      defp flow_hot_cold_index_entries(%{id: id, type: type, state: flow_state} = record) do
        partition_key = Map.get(record, :partition_key)
        updated_score = Map.get(record, :updated_at_ms, 0)
        priority = Map.get(record, :priority, 0)

        entries = [
          {FlowKeys.state_index_key(type, flow_state, partition_key), id, updated_score}
          | flow_metadata_index_entries(record) ++
              flow_active_timeout_index_entries(record) ++
              flow_terminal_retention_index_entries(record)
        ]

        case Map.get(record, :next_run_at_ms) do
          due_at_ms when is_integer(due_at_ms) ->
            due_entries = [
              {FlowKeys.due_key(type, flow_state, priority, partition_key), id, due_at_ms}
            ]

            due_entries =
              if flow_due_any_index_enabled?() do
                [
                  {FlowKeys.due_any_key(type, priority, partition_key), id, due_at_ms}
                  | due_entries
                ]
              else
                due_entries
              end

            due_entries ++ entries

          _ ->
            entries
        end
      end

      defp flow_claim_due_scan_keys(
             state,
             [due_key],
             type,
             state_filter,
             worker,
             lease_ms,
             now_ms,
             partition_key,
             limit,
             claimed
           ) do
        flow_ensure_due_index_ready(state, due_key)
        claimed_count = length(claimed)
        max_scan = max((limit - claimed_count) * 16, limit + 64)

        case flow_claim_due_scan(
               state,
               due_key,
               type,
               state_filter,
               worker,
               lease_ms,
               now_ms,
               partition_key,
               limit,
               max_scan,
               0,
               claimed_count,
               claimed
             ) do
          {:error, _reason} = error -> error
          {scanned_claimed, _scanned_count} -> scanned_claimed
        end
      end

      defp flow_claim_due_scan_keys(
             state,
             due_keys,
             type,
             state_filter,
             worker,
             lease_ms,
             now_ms,
             partition_key,
             limit,
             claimed
           ) do
        Enum.each(due_keys, &flow_ensure_due_index_ready(state, &1))

        case flow_claim_due_scan_keys_native_multi(
               state,
               due_keys,
               type,
               state_filter,
               worker,
               lease_ms,
               now_ms,
               partition_key,
               limit,
               claimed
             ) do
          {:error, _reason} = error ->
            error

          {:ok, native_claimed} when length(native_claimed) >= limit ->
            native_claimed

          {:ok, native_claimed} ->
            native_claimed

          :fallback ->
            if standalone_staged_apply?() do
              flow_claim_due_scan_keys_staged_once(
                state,
                due_keys,
                type,
                state_filter,
                worker,
                lease_ms,
                now_ms,
                partition_key,
                limit,
                claimed
              )
            else
              flow_claim_due_scan_key_rounds(
                state,
                due_keys,
                type,
                state_filter,
                worker,
                lease_ms,
                now_ms,
                partition_key,
                limit,
                claimed
              )
            end
        end
      end

      defp flow_claim_due_scan_keys_staged_once(
             _state,
             [],
             _type,
             _state_filter,
             _worker,
             _lease_ms,
             _now_ms,
             _partition_key,
             _limit,
             claimed
           ),
           do: claimed

      defp flow_claim_due_scan_keys_staged_once(
             state,
             due_keys,
             type,
             state_filter,
             worker,
             lease_ms,
             now_ms,
             partition_key,
             limit,
             claimed
           ) do
        due_keys
        |> Enum.reduce_while({claimed, length(claimed)}, fn due_key, {acc, acc_count} ->
          if acc_count >= limit do
            {:halt, {acc, acc_count}}
          else
            remaining = limit - acc_count
            max_scan = max(remaining * 16, remaining + 64)

            case flow_claim_due_scan(
                   state,
                   due_key,
                   type,
                   state_filter,
                   worker,
                   lease_ms,
                   now_ms,
                   partition_key,
                   limit,
                   max_scan,
                   0,
                   acc_count,
                   acc
                 ) do
              {:error, _reason} = error ->
                {:halt, error}

              {next_acc, next_count} ->
                if next_count >= limit do
                  {:halt, {next_acc, next_count}}
                else
                  {:cont, {next_acc, next_count}}
                end
            end
          end
        end)
        |> case do
          {:error, _reason} = error -> error
          {next_claimed, _next_count} -> next_claimed
        end
      end

      defp flow_claim_due_scan_keys_native_multi(
             state,
             due_keys,
             type,
             state_filter,
             worker,
             lease_ms,
             now_ms,
             partition_key,
             limit,
             claimed
           ) do
        remaining = limit - length(claimed)

        cond do
          remaining <= 0 ->
            {:ok, claimed}

          flow_governance_limit_active?() ->
            :fallback

          flow_native_index(state) == nil ->
            :fallback

          true ->
            max_scan = max(remaining * 16, remaining + 64)

            flow_claim_due_scan_keys_native_multi_loop(
              state,
              due_keys,
              type,
              state_filter,
              worker,
              lease_ms,
              now_ms,
              partition_key,
              limit,
              claimed,
              0,
              max_scan
            )
        end
      end
    end
  end
end
