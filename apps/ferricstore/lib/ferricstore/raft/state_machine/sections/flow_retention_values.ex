defmodule Ferricstore.Raft.StateMachine.Sections.FlowRetentionValues do
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

      defp flow_retention_keydir_available?(state) do
        :ets.info(state.ets, :name) != :undefined
      rescue
        ArgumentError -> false
      end

      defp flow_retention_keys_with_prefix_page(_state, prefix, _limit)
           when not is_binary(prefix),
           do: {[], true}

      defp flow_retention_keys_with_prefix_page(_state, _prefix, limit) when limit <= 0,
        do: {[], false}

      defp flow_retention_keys_with_prefix_page(state, prefix, limit) do
        prefix_len = byte_size(prefix)

        match_spec = [
          {{:"$1", :_, :_, :_, :_, :_, :_},
           [
             {:andalso, {:is_binary, :"$1"},
              {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
               {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
           ], [:"$1"]}
        ]

        safe_ets_select_page(state.ets, match_spec, limit)
      end

      defp flow_retention_history_entries(state, history_key, limit)
           when is_integer(limit) and limit > 0 do
        with {:ok, lmdb_entries, lmdb_complete?} <-
               flow_retention_lmdb_history_entries(state, history_key, limit) do
          remaining = max(limit - length(lmdb_entries), 0)

          cond do
            not lmdb_complete? ->
              {:ok, Enum.uniq_by(lmdb_entries, &flow_retention_history_entry_key/1), false}

            remaining <= 0 ->
              {:ok, Enum.uniq_by(lmdb_entries, &flow_retention_history_entry_key/1), false}

            true ->
              {ets_entries, ets_complete?} =
                flow_retention_native_history_entries(state, history_key, remaining)

              entries =
                (lmdb_entries ++ ets_entries)
                |> Enum.uniq_by(&flow_retention_history_entry_key/1)

              {:ok, entries, ets_complete?}
          end
        end
      end

      defp flow_retention_history_entries(_state, _history_key, _limit),
        do: {:ok, [], false}

      defp flow_retention_lmdb_history_entries(_state, _history_key, remaining)
           when remaining <= 0,
           do: {:ok, [], false}

      defp flow_retention_lmdb_history_entries(state, history_key, remaining) do
        path = flow_lmdb_record_path(state)
        prefix = Ferricstore.Flow.LMDB.history_index_prefix(history_key)

        case flow_retention_lmdb_projection_state(state) do
          :available ->
            flow_retention_lmdb_history_entries_after(path, prefix, <<>>, remaining, [])

          :empty ->
            {:ok, [], true}

          :unavailable ->
            {:ok, [], false}
        end
      end

      defp flow_retention_lmdb_history_entries_after(path, prefix, after_key, limit, acc) do
        case Ferricstore.Flow.LMDB.prefix_entries_after(path, prefix, after_key, limit) do
          {:ok, []} ->
            {:ok, Enum.reverse(acc), true}

          {:ok, entries} ->
            decoded = flow_retention_decode_lmdb_history_entries(entries, acc)
            complete? = length(entries) < limit
            {:ok, Enum.reverse(decoded), complete?}

          {:error, _reason} = error ->
            error
        end
      end

      defp flow_retention_decode_lmdb_history_entries(entries, acc) do
        Enum.reduce(entries, acc, fn {_history_index_key, value}, acc ->
          case Ferricstore.Flow.LMDB.decode_history_index_value(value) do
            {:ok, {event_id, _event_ms, _expire_at_ms, compound_key}} ->
              [{compound_key, event_id, value} | acc]

            :error ->
              acc
          end
        end)
      end

      defp flow_retention_native_history_entries(state, history_key, limit) do
        case flow_native_index(state) do
          nil ->
            {[], false}

          native ->
            entries =
              native
              |> NativeFlowIndex.range_slice(history_key, :neg_inf, :inf, false, 0, limit)
              |> Enum.map(fn {event_id, _event_ms} ->
                {"X:" <> history_key <> <<0>> <> event_id, event_id}
              end)

            {entries, NativeFlowIndex.count_all(native, history_key) <= length(entries)}
        end
      end

      defp flow_retention_cleanup_member_entries(state, %{id: id} = record, limit)
           when is_binary(id) and is_integer(limit) and limit >= 0 do
        index_key =
          FlowKeys.retention_cleanup_index_key(id, Map.get(record, :partition_key))

        case flow_native_index(state) do
          nil ->
            {[], false}

          native ->
            member_keys =
              if limit == 0 do
                []
              else
                native
                |> NativeFlowIndex.range_slice(index_key, :neg_inf, :inf, false, 0, limit)
                |> Enum.map(fn {member_key, _score} -> member_key end)
              end

            entries =
              member_keys
              |> Enum.zip(sm_store_batch_get(state, member_keys, &sm_file_path/2))
              |> Enum.map(fn
                {member_key, value} when is_binary(value) ->
                  case Ferricstore.Flow.RetentionCleanupMember.decode(value) do
                    {:ok, {^index_key, owned_key}} ->
                      if flow_retention_cleanup_owned_key?(owned_key, record),
                        do: {member_key, owned_key},
                        else: {member_key, nil}

                    _invalid ->
                      {member_key, nil}
                  end

                {member_key, _missing} ->
                  {member_key, nil}
              end)

            {entries, NativeFlowIndex.count_all(native, index_key) <= length(member_keys)}
        end
      end

      defp flow_retention_cleanup_member_entries(_state, _record, _limit), do: {[], false}

      defp flow_retention_cleanup_owned_key?(owned_key, %{id: id} = record)
           when is_binary(owned_key) and is_binary(id) do
        partition_key = Map.get(record, :partition_key)

        flow_retention_owned_value_ref?(owned_key, record) or
          String.starts_with?(
            owned_key,
            FlowKeys.shared_value_link_prefix(id, partition_key)
          ) or
          String.starts_with?(
            owned_key,
            FlowKeys.governance_effect_key_prefix(id, partition_key)
          ) or
          String.starts_with?(
            owned_key,
            FlowKeys.governance_ledger_key_prefix(id, partition_key)
          ) or
          owned_key == FlowKeys.governance_ledger_index_key(id, partition_key)
      end

      defp flow_retention_cleanup_owned_key?(_owned_key, _record), do: false

      defp flow_retention_cleanup_members_complete?(state, index_key, entries) do
        member_keys = Enum.map(entries, &elem(&1, 0))

        case flow_native_index(state) do
          nil ->
            false

          native ->
            NativeFlowIndex.count_all(native, index_key) == length(member_keys) and
              Enum.all?(member_keys, fn member_key ->
                flow_index_score_of(state, index_key, member_key) != :miss
              end)
        end
      end

      defp flow_retention_delete_cleanup_members(_state, _index_key, []), do: :ok

      defp flow_retention_delete_cleanup_members(state, index_key, entries) do
        member_keys = Enum.map(entries, &elem(&1, 0))

        with :ok <- flow_index_delete_members(state, index_key, member_keys),
             {:ok, _deleted} <- flow_retention_delete_keys(state, member_keys) do
          :ok
        end
      end

      defp flow_retention_delete_history_index(_state, _history_key, []), do: :ok

      defp flow_retention_delete_history_index(state, history_key, entries) do
        event_ids = Enum.map(entries, &flow_retention_history_entry_event_id/1)

        with :ok <- flow_index_delete_members(state, history_key, event_ids) do
          with_lmdb_mirror_shard(state, fn ->
            Enum.each(entries, fn entry ->
              event_id = flow_retention_history_entry_event_id(entry)
              queue_lmdb_history_index_delete(nil, history_key, event_id, flow_event_ms(event_id))
            end)
          end)

          :ok
        end
      end

      defp flow_retention_positive_integer(value, _default) when is_integer(value) and value > 0,
        do: value

      defp flow_retention_positive_integer(value, default) when is_binary(value) do
        case Integer.parse(value) do
          {parsed, ""} when parsed > 0 -> parsed
          _ -> default
        end
      end

      defp flow_retention_positive_integer(_value, default), do: default

      defp flow_retention_history_lmdb_scan_limit(state) do
        state
        |> raft_apply_context()
        |> Map.fetch!(:flow_lmdb_history_cleanup_scan_limit)
      end

      defp flow_retention_value_lmdb_scan_limit(state) do
        state
        |> raft_apply_context()
        |> Map.fetch!(:flow_lmdb_value_cleanup_scan_limit)
      end

      defp flow_event_ms(event_id) when is_binary(event_id) do
        event_id
        |> String.split("-", parts: 2)
        |> case do
          [ms, _seq] -> String.to_integer(ms)
          _ -> 0
        end
      rescue
        _ -> 0
      end

      defp flow_event_ms(_event_id), do: 0

      defp flow_retention_history_entry_key({key, _event_id}), do: key
      defp flow_retention_history_entry_key({key, _event_id, _lmdb_value}), do: key

      defp flow_retention_history_entry_event_id({_key, event_id}), do: event_id
      defp flow_retention_history_entry_event_id({_key, event_id, _lmdb_value}), do: event_id

      defp flow_retention_owned_value_ref?(ref, record) when is_binary(ref) and is_map(record) do
        with id when is_binary(id) <- Map.get(record, :id) || Map.get(record, "id") do
          partition_key = Map.get(record, :partition_key) || Map.get(record, "partition_key")
          flow_retention_exact_owned_value_ref?(ref, id, partition_key)
        else
          _other -> false
        end
      end

      defp flow_retention_owned_value_ref?(_ref, _record), do: false

      defp flow_retention_exact_owned_value_ref?(ref, id, partition_key) do
        [:payload, :result, :error, :shared]
        |> Enum.any?(fn kind ->
          flow_retention_exact_owned_value_ref?(ref, id, partition_key, kind)
        end)
      end

      defp flow_retention_shareable_owned_value_ref?(ref, record)
           when is_binary(ref) and is_map(record) do
        with id when is_binary(id) <- Map.get(record, :id) || Map.get(record, "id") do
          partition_key = Map.get(record, :partition_key) || Map.get(record, "partition_key")
          flow_retention_exact_owned_value_ref?(ref, id, partition_key, :shared)
        else
          _other -> false
        end
      end

      defp flow_retention_shareable_owned_value_ref?(_ref, _record), do: false

      defp flow_retention_exact_owned_value_ref?(ref, id, partition_key, kind) do
        key = FlowKeys.value_key(id, kind, 0, partition_key)
        prefix = flow_retention_owned_value_ref_prefix(key)
        prefix_len = byte_size(prefix)

        if String.starts_with?(ref, prefix) do
          ref
          |> binary_part(prefix_len, byte_size(ref) - prefix_len)
          |> flow_retention_owned_value_suffix?(kind)
        else
          false
        end
      end

      defp flow_retention_owned_value_ref_prefix(key) when is_binary(key) do
        case :binary.matches(key, ":") do
          [] ->
            key

          matches ->
            {idx, 1} = List.last(matches)
            binary_part(key, 0, idx + 1)
        end
      end

      defp flow_retention_owned_value_suffix?(suffix, :shared) when is_binary(suffix) do
        flow_retention_value_version?(suffix) or
          case :binary.matches(suffix, ":") do
            [] ->
              false

            matches ->
              {idx, 1} = List.last(matches)
              version = binary_part(suffix, idx + 1, byte_size(suffix) - idx - 1)
              flow_retention_value_version?(version)
          end
      end

      defp flow_retention_owned_value_suffix?(suffix, _kind),
        do: flow_retention_value_version?(suffix)

      defp flow_retention_value_version?(version) when is_binary(version) do
        case Integer.parse(version) do
          {parsed, ""} when parsed >= 0 -> true
          _other -> false
        end
      end

      defp flow_owned_value_ref?(<<"f:{", rest::binary>>),
        do: :binary.match(rest, "}:v:") != :nomatch

      defp flow_owned_value_ref?(_ref), do: false

      defp flow_retention_delete_keys(state, keys) do
        keys
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq()
        |> Enum.reduce_while({:ok, 0}, fn key, {:ok, count} ->
          case do_delete(state, key) do
            :ok -> {:cont, {:ok, count + 1}}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp flow_retention_registry_key(%{id: id} = record) when is_binary(id) do
        FlowKeys.registry_key(id, Map.get(record, :partition_key))
      end

      defp flow_retention_registry_key(_record), do: nil

      defp flow_retention_merge_counts(left, right) do
        %{
          flows: Map.get(left, :flows, 0) + Map.get(right, :flows, 0),
          history: Map.get(left, :history, 0) + Map.get(right, :history, 0),
          values: Map.get(left, :values, 0) + Map.get(right, :values, 0),
          active_timeouts:
            Map.get(left, :active_timeouts, 0) + Map.get(right, :active_timeouts, 0)
        }
      end

      defp do_flow_rewind(state, %{id: id, to_event: to_event} = attrs) do
        now_ms = flow_attrs_now_ms(attrs)
        partition_key = Map.get(attrs, :partition_key)

        with {:ok, record} <- flow_require_record(state, id, partition_key),
             :ok <- flow_require_rewindable(record),
             :ok <- flow_require_expected_state(record, Map.get(attrs, :expect_state)),
             {:ok, target_fields} <-
               flow_history_event_fields(state, record, to_event, partition_key),
             {:ok, next} <- flow_rewind_record(record, target_fields, attrs, now_ms) do
          next = Map.put(next, :rewound_to_event_id, to_event)

          with :ok <- flow_validate_record_keys(record),
               :ok <- flow_validate_record_keys(next),
               :ok <- flow_transition_move_indexes(state, [{record, next}]),
               :ok <- flow_refresh_record_value_expirations(state, next, %{}),
               state_key = FlowKeys.state_key(id, partition_key),
               :ok <- flow_put_state_record(state, state_key, next),
               :ok <- flow_queue_lmdb_reactivated_state_projection(state, state_key, next),
               :ok <- flow_history_put_planned(state, record, next, "rewound", now_ms),
               :ok <- flow_after_history_put(state, next) do
            :ok
          end
        end
      end

      defp flow_queue_lmdb_reactivated_state_projection(state, state_key, record)
           when is_binary(state_key) and is_map(record) do
        if flow_lmdb_projection_enabled?(state) and
             not Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) do
          queue_pending_lmdb_flow_state_projection(
            state_key,
            flow_encode(record),
            flow_record_expire_at(record)
          )
        end

        :ok
      end

      defp flow_prepare_claim_candidate_record(
             record,
             _id,
             type,
             state_filter,
             worker,
             lease_ms,
             now_ms,
             due_score
           ) do
        case record do
          nil ->
            :delete_due

          %{type: record_type} when record_type != type ->
            {:skip, flow_claim_restore_due_score(record, due_score)}

          %{state: record_state} = record ->
            cond do
              flow_claim_state_excluded?(state_filter, record_state) ->
                {:skip, flow_claim_restore_due_score(record, due_score)}

              not flow_claim_record_due_ready?(record, now_ms) ->
                {:skip, flow_claim_restore_due_score(record, due_score)}

              flow_claim_state_match?(state_filter, record_state) ->
                next_version = Map.fetch!(record, :version) + 1
                next_fencing_token = Map.get(record, :fencing_token, 0) + 1
                deadline_ms = now_ms + lease_ms

                token =
                  worker <>
                    ":" <>
                    Integer.to_string(now_ms) <> ":" <> Integer.to_string(next_fencing_token)

                next =
                  flow_claim_next_record(
                    record,
                    next_version,
                    next_fencing_token,
                    worker,
                    token,
                    deadline_ms,
                    now_ms
                  )
                  |> flow_bind_governance_limit()

                with {:ok, from_due_score} <- flow_claim_numeric_score(due_score),
                     :ok <- flow_validate_claim_next_record_keys(next) do
                  {:ok, record, next, from_due_score}
                else
                  _ -> {:skip, flow_claim_restore_due_score(record, due_score)}
                end

              true ->
                :delete_due
            end

          _record ->
            :delete_due
        end
      end

      defp flow_claim_next_record(
             %{
               state: _state,
               version: _version,
               fencing_token: _fencing_token,
               updated_at_ms: _updated_at_ms,
               ttl_ms: _ttl_ms,
               retention_ttl_ms: _retention_ttl_ms,
               terminal_retention_until_ms: _terminal_retention_until_ms,
               history_hot_max_events: _history_hot_max_events,
               history_max_events: _history_max_events,
               lease_owner: _lease_owner,
               lease_token: _lease_token,
               lease_deadline_ms: _lease_deadline_ms,
               next_run_at_ms: _next_run_at_ms,
               run_state: _run_state
             } = record,
             next_version,
             next_fencing_token,
             worker,
             token,
             deadline_ms,
             now_ms
           ) do
        %{
          record
          | state: "running",
            version: next_version,
            fencing_token: next_fencing_token,
            updated_at_ms: now_ms,
            ttl_ms: nil,
            terminal_retention_until_ms: nil,
            lease_owner: worker,
            lease_token: token,
            lease_deadline_ms: deadline_ms,
            next_run_at_ms: deadline_ms,
            run_state: flow_claim_run_state(record)
        }
      end

      defp flow_claim_next_record(
             record,
             next_version,
             next_fencing_token,
             worker,
             token,
             deadline_ms,
             now_ms
           ) do
        Map.merge(record, %{
          state: "running",
          version: next_version,
          fencing_token: next_fencing_token,
          updated_at_ms: now_ms,
          ttl_ms: nil,
          retention_ttl_ms: Map.get(record, :retention_ttl_ms),
          terminal_retention_until_ms: nil,
          history_hot_max_events: Map.get(record, :history_hot_max_events),
          history_max_events: Map.get(record, :history_max_events),
          lease_owner: worker,
          lease_token: token,
          lease_deadline_ms: deadline_ms,
          next_run_at_ms: deadline_ms,
          run_state: flow_claim_run_state(record)
        })
      end

      defp flow_claim_state_excluded?({:exclude, _state_filter, exclude_states}, state),
        do: state in exclude_states

      defp flow_claim_state_excluded?(_state_filter, _state), do: false

      defp flow_claim_state_match?({:exclude, state_filter, _exclude_states}, state),
        do: flow_claim_state_match?(state_filter, state)

      defp flow_claim_state_match?(:any, state) when is_binary(state), do: true
      defp flow_claim_state_match?(states, state) when is_list(states), do: state in states
      defp flow_claim_state_match?(state, state), do: true
      defp flow_claim_state_match?(_state_filter, _state), do: false

      defp flow_claim_run_state(%{state: "running"} = record),
        do: Map.get(record, :run_state) || "queued"

      defp flow_claim_run_state(%{state: flow_state}), do: flow_state

      defp flow_bind_governance_limit(record) when is_map(record) do
        case Process.get(:sm_flow_governance_limit) do
          %{reservation_ids: [reservation_id | remaining]} = reservation
          when is_binary(reservation_id) and reservation_id != "" ->
            Process.put(:sm_flow_governance_limit, %{reservation | reservation_ids: remaining})

            reservation =
              reservation
              |> Map.delete(:reservation_ids)
              |> Map.put(:reservation_id, reservation_id)

            Map.put(record, :governance_limit, reservation)

          _none ->
            Map.delete(record, :governance_limit)
        end
      end

      defp flow_governance_limit_active? do
        match?(
          %{
            scope: scope,
            shard_id: shard_id,
            enforcement: enforcement,
            reservation_ids: [_ | _]
          }
          when is_binary(scope) and is_integer(shard_id) and
                 enforcement in [:strict_global, :approximate_global],
          Process.get(:sm_flow_governance_limit)
        )
      end

      defp flow_claim_plan_pair(
             {:native_claim, next, _entry, _state_key, _value, _previous_history_ms}
           ),
           do: {next, next}

      defp flow_claim_plan_pair(
             {:native_claim, next, _entry, _state_key, _value, _previous_history_ms,
              _history_entry}
           ),
           do: {next, next}

      defp flow_claim_plan_pair({_child_state, record, next, _attrs, _partition_key, _now_ms}),
        do: {record, next}

      defp flow_claim_plan_pair(
             {_child_state, record, next, _attrs, _partition_key, _now_ms, _history_meta}
           ),
           do: {record, next}

      defp flow_claim_plan_pair({record, next, _from_due_score}), do: {record, next}
      defp flow_claim_plan_pair({record, next, _history_meta, _attrs}), do: {record, next}
      defp flow_claim_plan_pair({record, next}), do: {record, next}

      defp flow_claim_record_state_score(record),
        do: flow_claim_numeric_score(Map.get(record, :updated_at_ms, 0))

      defp flow_claim_record_due_ready?(record, now_ms) do
        with {:ok, due_score} <- flow_claim_numeric_score(Map.get(record, :next_run_at_ms)),
             {:ok, now_score} <- flow_claim_numeric_score(now_ms) do
          due_score <= now_score
        else
          _ -> false
        end
      end

      defp flow_claim_numeric_score(score) when is_float(score), do: {:ok, score}
      defp flow_claim_numeric_score(score) when is_integer(score), do: {:ok, score * 1.0}
      defp flow_claim_numeric_score(_score), do: :error

      defp flow_claim_restore_due_score(record, due_score) do
        case flow_claim_numeric_score(Map.get(record, :next_run_at_ms)) do
          {:ok, score} ->
            score

          :error ->
            case flow_claim_numeric_score(due_score) do
              {:ok, score} -> score
              :error -> 0.0
            end
        end
      end
    end
  end
end
