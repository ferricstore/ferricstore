defmodule Ferricstore.Raft.StateMachine.Sections.FlowRetentionState do
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

      @doc false
      def flow_retention_plan(state, attrs) when is_map(state) and is_map(attrs) do
        now_ms = flow_attrs_now_ms(attrs)
        limit = Map.get(attrs, :limit, 100)
        plan_kind = Map.get(attrs, :plan_kind, :all)
        projection_pending? = flow_retention_history_projection_pending?(state)
        lmdb_pending? = flow_retention_lmdb_projection_pending?(state)
        backfill_pending? = not flow_retention_backfill_complete?(state)

        active_candidates =
          if plan_kind in [:active, :all] do
            state
            |> flow_active_timeout_expired_state_entries(now_ms, limit)
            |> Enum.flat_map(&flow_retention_plan_active_candidate(state, &1, now_ms))
          else
            []
          end

        terminal_candidates =
          if plan_kind not in [:terminal, :all] or projection_pending? or lmdb_pending? or
               backfill_pending? do
            []
          else
            flow_retention_plan_terminal_candidates(state, now_ms, limit, attrs)
          end

        {:ok,
         %{
           shard_index: state.shard_index,
           active_candidates: active_candidates,
           terminal_candidates: terminal_candidates,
           projection_pending?: projection_pending?,
           lmdb_pending?: lmdb_pending?,
           backfill_pending?: backfill_pending?
         }}
      end

      defp flow_retention_backfill_complete?(state) do
        watermark_key = FlowKeys.shared_value_ref_backfill_key(state.shard_index)

        instance_name =
          case instance_ctx_for_state(state) do
            %{name: name} -> name
            _missing -> Map.get(state, :instance_name, :default)
          end

        Ferricstore.Flow.SharedRefBackfill.verified_complete?(
          instance_name,
          state.shard_index
        ) and
          case sm_store_batch_get(state, [watermark_key], &sm_file_path/2) do
            [<<1>>] -> true
            _missing -> false
          end
      end

      defp flow_retention_plan_active_candidate(state, state_key, now_ms) do
        case flow_active_timeout_current_record(state, state_key) do
          record when is_map(record) ->
            with true <- flow_active_timeout_expired_record?(record, now_ms),
                 {:ok, guard_key, expected_guard} <-
                   flow_retention_planned_guard(state, record) do
              [
                %{
                  state_key: state_key,
                  expected_version: Map.fetch!(record, :version),
                  guard_key: guard_key,
                  expected_guard: expected_guard,
                  planned_key_count: 1,
                  record: record
                }
              ]
            else
              _not_expired_or_unprotected -> []
            end

          nil ->
            []
        end
      end

      defp flow_retention_plan_terminal_candidates(state, now_ms, limit, attrs) do
        hot_state_keys = flow_retention_plan_hot_terminal_state_keys(state, now_ms, limit)
        lmdb_limit = limit + length(hot_state_keys)

        lmdb_state_keys =
          case Ferricstore.Flow.LMDB.expired_terminal_state_keys(
                 flow_lmdb_record_path(state),
                 now_ms,
                 lmdb_limit
               ) do
            {:ok, state_keys} -> state_keys
            {:error, _reason} -> []
          end

        {state_keys, _seen} =
          Enum.reduce(hot_state_keys ++ lmdb_state_keys, {[], MapSet.new()}, fn state_key,
                                                                                {acc, seen} ->
            if MapSet.member?(seen, state_key) do
              {acc, seen}
            else
              {[state_key | acc], MapSet.put(seen, state_key)}
            end
          end)

        key_budget = flow_retention_cleanup_key_budget(state, attrs)
        byte_budget = flow_retention_cleanup_byte_budget(state, attrs)

        {candidates, _remaining_limit, _remaining_keys, _remaining_bytes} =
          state_keys
          |> Enum.reverse()
          |> Enum.reduce_while({[], limit, key_budget, byte_budget}, fn
            _state_key, {acc, remaining_limit, remaining_keys, remaining_bytes}
            when remaining_limit <= 0 or remaining_keys <= 0 or remaining_bytes <= 0 ->
              {:halt, {acc, remaining_limit, remaining_keys, remaining_bytes}}

            state_key, {acc, remaining_limit, remaining_keys, remaining_bytes} ->
              candidates =
                case flow_active_timeout_current_record(state, state_key) do
                  record when is_map(record) ->
                    flow_retention_plan_terminal_candidate(
                      state,
                      state_key,
                      record,
                      now_ms,
                      remaining_keys,
                      remaining_bytes
                    )

                  nil ->
                    []
                end

              case candidates do
                [candidate] ->
                  candidate_keys = Map.fetch!(candidate, :planned_key_count)
                  candidate_bytes = :erlang.external_size(candidate)

                  {:cont,
                   {
                     [candidate | acc],
                     remaining_limit - 1,
                     remaining_keys - candidate_keys,
                     remaining_bytes - candidate_bytes
                   }}

                [] ->
                  {:cont, {acc, remaining_limit, remaining_keys, remaining_bytes}}
              end
          end)

        Enum.reverse(candidates)
      end

      defp flow_retention_plan_hot_terminal_state_keys(state, now_ms, limit) do
        case flow_native_index(state) do
          nil ->
            []

          native ->
            native
            |> NativeFlowIndex.range_slice(
              FlowKeys.terminal_retention_index_key(),
              :neg_inf,
              {:inclusive, now_ms},
              false,
              0,
              limit
            )
            |> Enum.map(fn {state_key, _retention_until_ms} -> state_key end)
        end
      end

      defp flow_retention_plan_terminal_candidate(
             state,
             state_key,
             record,
             now_ms,
             key_budget,
             byte_budget
           ) do
        if flow_retention_expired_terminal_record?(record, now_ms) do
          with {:ok, guard_key, expected_guard} <- flow_retention_planned_guard(state, record),
               {:ok, cleanup_plan} <-
                 flow_retention_build_cleanup_plan(state, state_key, record, key_budget),
               planned_key_count when planned_key_count > 0 <-
                 Map.fetch!(cleanup_plan, :planned_key_count) do
            candidate = %{
              state_key: state_key,
              expected_version: Map.fetch!(record, :version),
              guard_key: guard_key,
              expected_guard: expected_guard,
              planned_key_count: planned_key_count,
              record: record,
              cleanup_plan: cleanup_plan
            }

            if :erlang.external_size(candidate) <= byte_budget do
              [candidate]
            else
              flow_retention_retry_smaller_cleanup_plan(
                state,
                state_key,
                record,
                now_ms,
                key_budget,
                byte_budget
              )
            end
          else
            _missing_guard_or_plan -> []
          end
        else
          []
        end
      end

      defp flow_retention_retry_smaller_cleanup_plan(
             _state,
             _state_key,
             _record,
             _now_ms,
             key_budget,
             _byte_budget
           )
           when key_budget <= 8,
           do: []

      defp flow_retention_retry_smaller_cleanup_plan(
             state,
             state_key,
             record,
             now_ms,
             key_budget,
             byte_budget
           ) do
        flow_retention_plan_terminal_candidate(
          state,
          state_key,
          record,
          now_ms,
          max(div(key_budget, 2), 8),
          byte_budget
        )
      end

      defp flow_retention_planned_guard(state, %{id: id} = record) when is_binary(id) do
        guard_key = FlowKeys.retention_guard_key(id, Map.get(record, :partition_key))
        expected_guard = Ferricstore.Flow.RetentionGuard.encode(record)

        case sm_store_batch_get(state, [guard_key], &sm_file_path/2) do
          [^expected_guard] -> {:ok, guard_key, expected_guard}
          _missing_or_stale -> {:error, :flow_retention_guard_missing}
        end
      end

      defp flow_retention_planned_guard(_state, _record),
        do: {:error, :flow_retention_guard_missing}

      defp flow_retention_zero_counts,
        do: {:ok, %{flows: 0, history: 0, values: 0, active_timeouts: 0}}

      defp flow_retention_current_state_record(state, state_key) do
        case :ets.lookup(state.ets, state_key) do
          [{^state_key, value, _expire_at_ms, _lfu, fid, offset, value_size}] ->
            flow_retention_decode_state_record(state, state_key, value, fid, offset, value_size)

          _other ->
            :miss
        end
      rescue
        ArgumentError -> :miss
      end

      defp flow_retention_expired_terminal_record?(record, now_ms) do
        Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) and
          case Map.get(record, :terminal_retention_until_ms) do
            expire_at_ms when is_integer(expire_at_ms) and expire_at_ms <= now_ms -> true
            _other -> false
          end
      end

      defp flow_retention_decode_lmdb_state_record(state, state_key) do
        case Ferricstore.Flow.LMDB.get(flow_lmdb_record_path(state), state_key) do
          {:ok, blob} ->
            flow_retention_decode_lmdb_state_value(blob)

          _ ->
            :miss
        end
      end

      defp flow_retention_decode_lmdb_state_value(blob) when is_binary(blob) do
        case Ferricstore.Flow.LMDB.decode_value(blob, 0) do
          {:ok, value} -> flow_decode_record_blob(value)
          _ -> :miss
        end
      end

      defp flow_retention_decode_lmdb_state_value(_blob), do: :miss

      defp flow_active_timeout_expired_state_entries(_state, _now_ms, limit)
           when not is_integer(limit) or limit <= 0,
           do: []

      defp flow_active_timeout_expired_state_entries(state, now_ms, limit) do
        hot = flow_active_timeout_expired_hot_state_keys(state, now_ms, limit)
        lmdb_limit = limit + length(hot)

        lmdb =
          case Ferricstore.Flow.LMDB.expired_active_timeout_state_keys(
                 flow_lmdb_record_path(state),
                 now_ms,
                 lmdb_limit
               ) do
            {:ok, state_keys} -> state_keys
            {:error, _reason} -> []
          end

        {state_keys, _seen} =
          Enum.reduce(hot ++ lmdb, {[], MapSet.new()}, fn state_key, {acc, seen} ->
            if MapSet.member?(seen, state_key) do
              {acc, seen}
            else
              {[state_key | acc], MapSet.put(seen, state_key)}
            end
          end)

        state_keys
        |> Enum.reverse()
        |> Enum.take(limit)
      end

      defp flow_active_timeout_expired_hot_state_keys(state, now_ms, limit) do
        case flow_native_index(state) do
          nil ->
            []

          native ->
            native
            |> NativeFlowIndex.range_slice(
              FlowKeys.active_timeout_index_key(),
              :neg_inf,
              {:inclusive, now_ms},
              false,
              0,
              limit
            )
            |> Enum.map(fn {state_key, _deadline_ms} -> state_key end)
        end
      end

      defp flow_active_timeout_expired_record?(record, now_ms) do
        not Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) and
          case {Map.get(record, :created_at_ms), Map.get(record, :max_active_ms)} do
            {created_at_ms, max_active_ms}
            when is_integer(created_at_ms) and is_integer(max_active_ms) and max_active_ms > 0 ->
              created_at_ms + max_active_ms <= now_ms

            _other ->
              false
          end
      end

      defp flow_active_timeout_entry(state, state_key, now_ms) when is_binary(state_key) do
        case flow_active_timeout_current_record(state, state_key) do
          record when is_map(record) ->
            if flow_active_timeout_expired_record?(record, now_ms) do
              flow_active_timeout_record(state, state_key, record, now_ms)
            else
              flow_retention_zero_counts()
            end

          nil ->
            flow_retention_zero_counts()
        end
      end

      defp flow_active_timeout_current_record(state, state_key) do
        case flow_active_timeout_pending_record(state, state_key) do
          {:ok, record} -> record
          :deleted -> nil
          :miss -> flow_read_record_by_key(state, state_key)
        end
      end

      defp flow_active_timeout_pending_record(state, state_key) do
        Process.get(:sm_cross_shard_pending_writes, [])
        |> Enum.find(fn
          {:put, _idx, keydir, _file_path, _file_id, key, _ets_value, _disk_value, _expire_at_ms} ->
            keydir == state.ets and key == state_key

          {:delete, _idx, keydir, _file_path, _file_id, key} ->
            keydir == state.ets and key == state_key

          _other ->
            false
        end)
        |> case do
          {:put, _idx, _keydir, _file_path, _file_id, ^state_key, ets_value, disk_value,
           _expire_at_ms} ->
            flow_active_timeout_decode_pending_record(state, ets_value, disk_value)

          {:delete, _idx, _keydir, _file_path, _file_id, ^state_key} ->
            :deleted

          nil ->
            :miss
        end
      end

      defp flow_active_timeout_decode_pending_record(_state, value, _disk_value)
           when is_binary(value) do
        flow_decode_record_blob(value)
      end

      defp flow_active_timeout_decode_pending_record(state, _value, disk_value)
           when is_binary(disk_value) do
        case flow_retention_materialize_state_value(state, disk_value) do
          {:ok, value} -> flow_decode_record_blob(value)
          :miss -> :miss
        end
      end

      defp flow_active_timeout_decode_pending_record(_state, _value, _disk_value), do: :miss

      defp flow_active_timeout_record(state, state_key, record, now_ms) do
        version = Map.fetch!(record, :version) + 1
        id = Map.fetch!(record, :id)
        partition_key = Map.get(record, :partition_key)
        max_active_ms = Map.fetch!(record, :max_active_ms)
        attrs = %{error: %{reason: "max_active_ms", max_active_ms: max_active_ms}}

        next =
          record
          |> Map.merge(%{
            state: "failed",
            version: version,
            updated_at_ms: now_ms,
            ttl_ms: nil,
            error_ref: flow_value_ref(attrs, :error, id, version, partition_key),
            lease_owner: nil,
            lease_token: nil,
            lease_deadline_ms: 0,
            next_run_at_ms: nil
          })
          |> flow_stamp_terminal_retention(now_ms)

        meta = %{
          reason: "max_active_ms",
          max_active_ms: max_active_ms
        }

        with :ok <- flow_maybe_queue_hibernated_timeout_cleanup(state, state_key),
             :ok <- flow_put_record_values(state, next, attrs),
             :ok <- flow_transition_move_indexes(state, [{record, next}]),
             :ok <-
               flow_put_state_record(
                 state,
                 FlowKeys.state_key(next.id, Map.get(next, :partition_key)),
                 next
               ),
             :ok <- flow_history_put_planned(state, record, next, "failed", now_ms, meta),
             :ok <- flow_after_history_put(state, next),
             :ok <- flow_maybe_cancel_children_on_parent_closed(state, next, now_ms),
             :ok <- flow_maybe_apply_child_terminal(state, next, "failed", now_ms) do
          {:ok, %{flows: 0, history: 0, values: 0, active_timeouts: 1}}
        end
      end

      defp flow_maybe_queue_hibernated_timeout_cleanup(state, state_key) do
        case :ets.lookup(state.ets, state_key) do
          [] -> flow_queue_hibernated_timeout_cleanup(state, state_key)
          _present -> :ok
        end
      rescue
        ArgumentError -> :ok
      end

      defp flow_queue_hibernated_timeout_cleanup(state, state_key) do
        park_key = Ferricstore.Flow.LMDB.cold_park_key_for_state_key(state_key)

        with {:ok, park_blob} <- Ferricstore.Flow.LMDB.get(flow_lmdb_record_path(state), park_key),
             {:ok, %{locator: %Locator{} = locator} = park} <-
               Ferricstore.Flow.LMDB.decode_cold_park(park_blob) do
          row =
            park
            |> Map.put(:park_key, park_key)
            |> flow_hibernated_timeout_due_key(locator)

          active_index_ops = flow_hibernated_timeout_active_index_delete_ops(state, state_key)

          with_lmdb_mirror_shard(state, fn ->
            (Hibernation.cleanup_ops(row) ++ active_index_ops)
            |> Enum.each(&queue_pending_lmdb_mirror_op/1)
          end)
        end

        :ok
      end

      defp flow_hibernated_timeout_active_index_delete_ops(state, state_key) do
        reverse_key = Ferricstore.Flow.LMDB.active_by_state_key_key(state_key)

        case Ferricstore.Flow.LMDB.get(flow_lmdb_record_path(state), reverse_key) do
          {:ok, reverse_value} ->
            Ferricstore.Flow.LMDB.active_index_delete_ops_from_reverse(
              state_key,
              reverse_value
            )

          _missing ->
            []
        end
      end

      defp flow_hibernated_timeout_due_key(park, locator) do
        case {Map.get(park, :type), Map.get(park, :state), Map.get(park, :due_at_ms)} do
          {type, flow_state, due_at_ms}
          when is_binary(type) and is_binary(flow_state) and is_integer(due_at_ms) ->
            due_key =
              Ferricstore.Flow.LMDB.cold_due_key(
                type: type,
                state: flow_state,
                partition_key: Map.get(park, :partition_key, ""),
                priority: Map.get(park, :priority, 0),
                due_at_ms: due_at_ms,
                flow_id: locator.flow_id,
                version: locator.version
              )

            Map.put(park, :due_key, due_key)

          _other ->
            park
        end
      end

      defp flow_retention_decode_state_record(_state, _key, value, _fid, _offset, _value_size)
           when is_binary(value) do
        flow_decode_record_blob(value)
      end

      defp flow_retention_decode_state_record(state, key, nil, fid, offset, value_size)
           when valid_cold_location(fid, offset, value_size) or
                  valid_waraft_segment_location(fid, offset, value_size) do
        case flow_retention_read_state_value(state, key, fid, offset, value_size) do
          {:ok, value} ->
            flow_decode_record_blob(value)

          _other ->
            :miss
        end
      end

      defp flow_retention_decode_state_record(_state, _key, _value, _fid, _offset, _value_size),
        do: :miss

      defp flow_retention_read_state_value(state, key, fid, offset, value_size)
           when valid_cold_location(fid, offset, value_size) do
        state
        |> sm_file_path(fid)
        |> Ferricstore.Store.ColdRead.pread_keyed(offset, key, @cold_read_timeout_ms)
        |> case do
          {:ok, value} when is_binary(value) ->
            flow_retention_materialize_state_value(state, value)

          _other ->
            :miss
        end
      end

      defp flow_retention_read_state_value(state, key, fid, _offset, value_size)
           when valid_waraft_segment_location(fid, 0, value_size) do
        state
        |> instance_ctx_for_state()
        |> Ferricstore.Raft.WARaftSegmentReader.read_value_from_location_including_expired(
          state.shard_index,
          fid,
          key
        )
        |> case do
          {:ok, value} when is_binary(value) ->
            flow_retention_materialize_state_value(state, value)

          _other ->
            :miss
        end
      end

      defp flow_retention_read_state_value(_state, _key, _fid, _offset, _value_size), do: :miss

      defp flow_retention_materialize_state_value(state, value) when is_binary(value) do
        case materialize_cold_blob_value(state, value) do
          {:ok, materialized} when is_binary(materialized) -> {:ok, materialized}
          _other -> :miss
        end
      end

      defp flow_retention_build_cleanup_plan(
             state,
             state_key,
             record,
             key_budget \\ nil
           ) do
        if flow_retention_keydir_available?(state) do
          context = raft_apply_context(state)

          key_budget =
            case key_budget do
              value when is_integer(value) and value > 0 ->
                min(value, context.flow_retention_cleanup_key_budget)

              _missing ->
                flow_retention_cleanup_key_budget(state, %{})
            end

          history_key =
            FlowKeys.history_key(Map.fetch!(record, :id), Map.get(record, :partition_key))

          history_limit = min(key_budget, flow_retention_history_lmdb_scan_limit(state))

          with {:ok, history_entries, history_complete?} <-
                 flow_retention_history_entries(state, history_key, history_limit) do
            history_cost = length(history_entries)
            remaining = max(key_budget - history_cost, 0)

            {cleanup_member_entries, cleanup_members_complete?} =
              if history_complete? do
                member_limit =
                  min(div(remaining, 2), flow_retention_value_lmdb_scan_limit(state))

                flow_retention_cleanup_member_entries(state, record, member_limit)
              else
                {[], false}
              end

            cleanup_member_cost =
              Enum.reduce(cleanup_member_entries, 0, fn
                {_member_key, owned_key}, count when is_binary(owned_key) -> count + 2
                {_member_key, nil}, count -> count + 1
              end)

            remaining = max(remaining - cleanup_member_cost, 0)
            delete_state? = history_complete? and cleanup_members_complete? and remaining >= 5
            partition_key = Map.get(record, :partition_key)
            id = Map.fetch!(record, :id)

            {:ok,
             %{
               state_key: state_key,
               history_key: history_key,
               history_entries: history_entries,
               cleanup_index_key: FlowKeys.retention_cleanup_index_key(id, partition_key),
               cleanup_member_entries: cleanup_member_entries,
               registry_key: FlowKeys.registry_key(id, partition_key),
               guard_key: FlowKeys.retention_guard_key(id, partition_key),
               governance_ledger_index_key:
                 FlowKeys.governance_ledger_index_key(id, partition_key),
               delete_state?: delete_state?,
               planned_key_count:
                 history_cost + cleanup_member_cost + if(delete_state?, do: 5, else: 0)
             }}
          end
        else
          {:error, :flow_retention_keydir_unavailable}
        end
      end

      defp flow_retention_cleanup_key_budget(state, attrs) do
        context = raft_apply_context(state)

        attrs
        |> Map.get(
          :cleanup_key_budget,
          context.flow_retention_cleanup_key_budget
        )
        |> flow_retention_positive_integer(context.flow_retention_cleanup_key_budget)
        |> min(context.flow_retention_cleanup_key_budget)
      end

      defp flow_retention_cleanup_byte_budget(state, attrs) do
        context = raft_apply_context(state)

        attrs
        |> Map.get(
          :cleanup_byte_budget,
          context.flow_retention_cleanup_byte_budget
        )
        |> flow_retention_positive_integer(context.flow_retention_cleanup_byte_budget)
        |> min(context.flow_retention_cleanup_byte_budget)
      end

      defp flow_retention_cleanup_record(
             state,
             state_key,
             record,
             %{
               state_key: planned_state_key,
               history_key: history_key,
               history_entries: history_entries,
               cleanup_index_key: cleanup_index_key,
               cleanup_member_entries: cleanup_member_entries,
               registry_key: registry_key,
               guard_key: guard_key,
               governance_ledger_index_key: governance_ledger_index_key,
               delete_state?: delete_state?
             }
           )
           when planned_state_key == state_key and is_list(history_entries) and
                  is_binary(cleanup_index_key) and is_list(cleanup_member_entries) and
                  is_binary(registry_key) and is_binary(guard_key) and
                  is_binary(governance_ledger_index_key) and is_boolean(delete_state?) do
        history_keys = Enum.map(history_entries, &flow_retention_history_entry_key/1)

        owned_keys =
          cleanup_member_entries
          |> Enum.map(&elem(&1, 1))
          |> Enum.filter(&is_binary/1)

        {value_refs, auxiliary_keys} = Enum.split_with(owned_keys, &FlowKeys.value_key?/1)

        {private_value_refs, shareable_value_refs} =
          flow_retention_split_owned_value_refs(value_refs, record)

        delete_state? =
          delete_state? and
            flow_retention_cleanup_members_complete?(
              state,
              cleanup_index_key,
              cleanup_member_entries
            )

        flow_retention_enqueue_shared_value_orphans(state, shareable_value_refs, record)

        with :ok <- flow_retention_delete_history_index(state, history_key, history_entries),
             {:ok, history_count} <- flow_retention_delete_keys(state, history_keys),
             {:ok, values_count} <- flow_retention_delete_keys(state, private_value_refs),
             {:ok, _auxiliary_count} <- flow_retention_delete_keys(state, auxiliary_keys),
             :ok <-
               flow_retention_delete_cleanup_members(
                 state,
                 cleanup_index_key,
                 cleanup_member_entries
               ) do
          if delete_state? do
            with {:ok, _registry_count} <- flow_retention_delete_keys(state, [registry_key]),
                 {:ok, _governance_index_count} <-
                   flow_retention_delete_keys(state, [governance_ledger_index_key]),
                 :ok <- flow_delete_type_catalog_member(state, record),
                 :ok <- do_delete(state, state_key),
                 :ok <- flow_release_shared_value_refs(state, record),
                 :ok <- do_delete(state, guard_key),
                 :ok <-
                   flow_index_delete_members(
                     state,
                     FlowKeys.terminal_retention_index_key(),
                     [state_key]
                   ) do
              maybe_queue_terminal_lmdb_index_delete(state, record)
              queue_lmdb_metadata_index_deletes(state, record)

              {:ok, %{flows: 1, history: history_count, values: values_count}}
            end
          else
            {:ok, %{flows: 0, history: history_count, values: values_count}}
          end
        end
      end

      defp flow_retention_cleanup_record(_state, _state_key, _record, _cleanup_plan),
        do: {:error, "ERR invalid flow retention cleanup plan"}

      defp flow_retention_split_owned_value_refs(refs, owner_record) do
        {shareable_refs, private_refs} =
          refs
          |> Enum.filter(&is_binary/1)
          |> Enum.uniq()
          |> Enum.split_with(&flow_retention_shareable_owned_value_ref?(&1, owner_record))

        {private_refs, shareable_refs}
      end

      defp flow_retention_enqueue_shared_value_orphans(_state, [], _owner_record), do: :ok

      defp flow_retention_enqueue_shared_value_orphans(state, refs, _owner_record) do
        case Process.get(:flow_retention_shared_value_orphans) do
          orphans when is_map(orphans) ->
            next = Enum.reduce(refs, orphans, &Map.put_new(&2, &1, state.shard_index))
            Process.put(:flow_retention_shared_value_orphans, next)
            :ok

          _outside_cross_shard_cleanup ->
            :ok
        end
      end

      defp flow_release_shared_value_refs(state, record) do
        registry_key =
          FlowKeys.shared_value_ref_registry_key(
            Map.fetch!(record, :id),
            Map.get(record, :partition_key)
          )

        refs = flow_shared_value_ref_registry(state, registry_key)

        with :ok <- flow_retention_enqueue_released_shared_refs(refs),
             :ok <- flow_decrement_shared_value_ref_counts(state, refs),
             :ok <- do_delete(state, registry_key) do
          :ok
        end
      end

      defp flow_retention_enqueue_released_shared_refs([]), do: :ok

      defp flow_retention_enqueue_released_shared_refs(refs) do
        case Process.get(:flow_retention_released_shared_refs) do
          %MapSet{} = released ->
            Process.put(
              :flow_retention_released_shared_refs,
              Enum.reduce(refs, released, &MapSet.put(&2, &1))
            )

            :ok

          _outside_cross_shard_cleanup ->
            :ok
        end
      end

      defp flow_decrement_shared_value_ref_counts(_state, []), do: :ok

      defp flow_decrement_shared_value_ref_counts(state, refs) do
        Enum.reduce_while(refs, :ok, fn ref, :ok ->
          count_key = FlowKeys.shared_value_ref_count_key(ref, state.shard_index)

          result =
            case flow_shared_value_ref_count(state, count_key) do
              count when count > 1 ->
                flow_put_hot_value(state, count_key, :erlang.term_to_binary(count - 1), 0)

              _last_or_missing ->
                do_delete(state, count_key)
            end

          case result do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp flow_retention_finalize_shared_value_orphans(anchor_state) do
        orphans = Process.get(:flow_retention_shared_value_orphans, %{})
        released = Process.get(:flow_retention_released_shared_refs, MapSet.new())

        refs =
          orphans
          |> Map.keys()
          |> MapSet.new()
          |> MapSet.union(released)
          |> Enum.sort()

        Enum.reduce_while(refs, {:ok, 0}, fn ref, {:ok, deleted} ->
          target_state = flow_retention_shared_value_owner_state(anchor_state, orphans, ref)
          count = flow_retention_shared_value_ref_count(anchor_state, ref)
          owner_expired? = Map.has_key?(orphans, ref)
          orphaned? = flow_retention_shared_value_orphaned?(target_state, ref)

          cond do
            count == 0 and (owner_expired? or orphaned?) ->
              case flow_retention_delete_shared_value(target_state, ref) do
                :ok -> {:cont, {:ok, deleted + 1}}
                {:error, _reason} = error -> {:halt, error}
              end

            owner_expired? ->
              marker_key = FlowKeys.shared_value_orphan_key(ref)
              :ok = flow_put_hot_value(target_state, marker_key, ref, 0)
              {:cont, {:ok, deleted}}

            true ->
              {:cont, {:ok, deleted}}
          end
        end)
      end

      defp flow_retention_shared_value_owner_state(anchor_state, orphans, ref) do
        case Map.get(orphans, ref) do
          shard_index when is_integer(shard_index) and shard_index >= 0 ->
            cross_shard_state_for_index(anchor_state, shard_index)

          _released_reference ->
            cross_shard_state_for_key(anchor_state, ref)
        end
      end

      defp flow_retention_shared_value_orphaned?(state, ref) do
        marker_key = FlowKeys.shared_value_orphan_key(ref)

        case sm_store_batch_get(state, [marker_key], &sm_file_path/2) do
          [^ref] -> true
          _missing_or_collision -> false
        end
      end

      defp flow_retention_delete_shared_value(state, ref) do
        marker_key = FlowKeys.shared_value_orphan_key(ref)

        with :ok <- do_delete(state, ref),
             :ok <- do_delete(state, marker_key) do
          :ok
        end
      end

      defp flow_retention_shared_value_ref_count(anchor_state, ref) do
        shard_count =
          case instance_ctx_for_state(anchor_state) do
            %{shard_count: count} when is_integer(count) and count > 0 -> count
            _missing -> 1
          end

        0..(shard_count - 1)
        |> Enum.reduce(0, fn shard_index, total ->
          state = cross_shard_state_for_index(anchor_state, shard_index)
          count_key = FlowKeys.shared_value_ref_count_key(ref, shard_index)
          total + flow_shared_value_ref_count(state, count_key)
        end)
      end

      defp flow_retention_lmdb_projection_state(state) do
        path = flow_lmdb_record_path(state)

        cond do
          Ferricstore.Flow.LMDB.env_present?(path) ->
            :available

          flow_retention_keydir_has_flow_entries?(state) ->
            :unavailable

          true ->
            :empty
        end
      end

      defp flow_retention_keydir_has_flow_entries?(state) do
        Enum.any?(["f:{f", "X:f:{"], fn prefix ->
          {keys, _complete?} = flow_retention_keys_with_prefix_page(state, prefix, 1)
          keys != []
        end)
      end
    end
  end
end
