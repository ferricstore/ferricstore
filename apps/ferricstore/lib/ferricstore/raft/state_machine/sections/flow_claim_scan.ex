defmodule Ferricstore.Raft.StateMachine.Sections.FlowClaimScan do
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
      alias Ferricstore.Flow.DueCatalog
      alias Ferricstore.Flow.Hibernation
      alias Ferricstore.Flow.HistoryProjector
      alias Ferricstore.Flow.Locator
      alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
      alias Ferricstore.Flow.Keys, as: FlowKeys
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

      defp flow_claim_due_scan_keys_native_multi_loop(
             _state,
             _due_keys,
             _type,
             _state_filter,
             _worker,
             _lease_ms,
             _now_ms,
             _partition_key,
             limit,
             claimed,
             _scanned,
             _max_scan
           )
           when length(claimed) >= limit,
           do: {:ok, claimed}

      defp flow_claim_due_scan_keys_native_multi_loop(
             _state,
             _due_keys,
             _type,
             _state_filter,
             _worker,
             _lease_ms,
             _now_ms,
             _partition_key,
             _limit,
             claimed,
             scanned,
             max_scan
           )
           when scanned >= max_scan,
           do: {:ok, claimed}

      defp flow_claim_due_scan_keys_native_multi_loop(
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
             scanned,
             max_scan
           ) do
        native = flow_native_index(state)
        remaining = limit - length(claimed)
        batch_size = min(max(remaining, 32), max_scan - scanned)

        policy = flow_read_policy(state, type)

        candidates =
          native
          |> NativeFlowIndex.claim_due_candidates(due_keys, now_ms, batch_size, batch_size)
          |> then(
            &flow_replace_fifo_candidate_runs(
              state,
              policy,
              type,
              state_filter,
              due_keys,
              &1,
              now_ms
            )
          )

        candidate_count = flow_claim_candidate_group_count(candidates)

        if candidate_count == 0 do
          {:ok, claimed}
        else
          case flow_claim_native_multi_candidate_groups(
                 state,
                 candidates,
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

            {:ok, next_claimed} when length(next_claimed) == length(claimed) ->
              if standalone_staged_apply?(), do: {:ok, next_claimed}, else: :fallback

            {:ok, next_claimed} ->
              if standalone_staged_apply?() or candidate_count < batch_size do
                {:ok, next_claimed}
              else
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
                  next_claimed,
                  scanned + candidate_count,
                  max_scan
                )
              end
          end
        end
      end

      defp flow_claim_candidate_group_count(candidates) when is_list(candidates) do
        Enum.reduce(candidates, 0, fn {_due_key, due_candidates}, acc ->
          acc + length(due_candidates)
        end)
      end

      defp flow_claim_native_multi_candidate_groups(
             state,
             candidates,
             type,
             state_filter,
             worker,
             lease_ms,
             now_ms,
             partition_key,
             limit,
             claimed
           ) do
        claimed_count = length(claimed)

        candidates
        |> Enum.reduce_while({claimed_count, claimed}, fn {due_key, due_candidates},
                                                          {acc_count, acc} ->
          if acc_count >= limit do
            {:halt, {acc_count, acc}}
          else
            case flow_claim_candidate_batch(
                   state,
                   due_key,
                   type,
                   state_filter,
                   worker,
                   lease_ms,
                   now_ms,
                   partition_key,
                   due_candidates,
                   limit - acc_count,
                   acc_count,
                   acc
                 ) do
              {:error, _reason} = error -> {:halt, error}
              {next_count, next_claimed} -> {:cont, {next_count, next_claimed}}
            end
          end
        end)
        |> case do
          {:error, _reason} = error -> error
          {_count, next_claimed} -> {:ok, next_claimed}
        end
      end

      defp flow_claim_due_scan_key_rounds(
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

      defp flow_claim_due_scan_key_rounds(
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
          claimed,
          length(claimed)
        )
      end

      defp flow_claim_due_scan_key_rounds(
             _state,
             _due_keys,
             _type,
             _state_filter,
             _worker,
             _lease_ms,
             _now_ms,
             _partition_key,
             limit,
             claimed,
             claimed_count
           )
           when claimed_count >= limit,
           do: claimed

      defp flow_claim_due_scan_key_rounds(
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
             claimed_count
           ) do
        round_chunk =
          flow_claim_due_scan_key_round_chunk(limit - claimed_count, length(due_keys))

        scan_result =
          Enum.reduce_while(due_keys, {claimed, claimed_count, false}, fn due_key,
                                                                          {acc, acc_count,
                                                                           progressed?} ->
            if acc_count >= limit do
              {:halt, {acc, acc_count, progressed?}}
            else
              target_count = min(limit, acc_count + round_chunk)

              case flow_claim_due_scan(
                     state,
                     due_key,
                     type,
                     state_filter,
                     worker,
                     lease_ms,
                     now_ms,
                     partition_key,
                     target_count,
                     max((target_count - acc_count) * 16, 32),
                     0,
                     acc_count,
                     acc
                   ) do
                {:error, _reason} = error ->
                  {:halt, error}

                {next_acc, next_count} ->
                  {:cont, {next_acc, next_count, progressed? or next_count > acc_count}}
              end
            end
          end)

        case scan_result do
          {:error, _reason} = error ->
            error

          {next_claimed, next_count, progressed?} ->
            cond do
              next_count >= limit ->
                next_claimed

              progressed? ->
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
                  next_claimed,
                  next_count
                )

              true ->
                next_claimed
            end
        end
      end

      defp flow_claim_due_scan_key_round_chunk(remaining, due_key_count)
           when remaining > 0 and due_key_count > 0 do
        max(1, div(remaining + due_key_count - 1, due_key_count))
      end

      defp flow_claim_due_scan_key_round_chunk(_remaining, _due_key_count), do: 1

      defp flow_claim_due_keys(state, type, :any, partition_keys, priority)
           when is_list(partition_keys) do
        if flow_due_any_index_enabled?() do
          Enum.map(partition_keys, &FlowKeys.due_any_key(type, priority, &1))
        else
          flow_claim_due_matching_keys(state, type, :any, partition_keys, priority)
        end
      end

      defp flow_claim_due_keys(
             state,
             type,
             {:exclude, :any, _exclude_states} = state_filter,
             partition_keys,
             priority
           )
           when is_list(partition_keys) do
        if flow_due_any_index_enabled?() do
          Enum.map(partition_keys, &FlowKeys.due_any_key(type, priority, &1))
        else
          flow_claim_due_matching_keys(state, type, state_filter, partition_keys, priority)
        end
      end

      defp flow_claim_due_keys(_state, type, state_filter, partition_keys, priority)
           when is_binary(state_filter) and is_list(partition_keys) do
        Enum.map(partition_keys, &FlowKeys.due_key(type, state_filter, priority, &1))
      end

      defp flow_claim_due_keys(_state, type, states, partition_keys, priority)
           when is_list(states) and is_list(partition_keys) do
        for partition_key <- partition_keys, state <- states do
          FlowKeys.due_key(type, state, priority, partition_key)
        end
      end

      defp flow_claim_due_keys(_state, type, state_filter, partition_key, priority)
           when partition_key not in [:any, :auto] and is_binary(state_filter) do
        [FlowKeys.due_key(type, state_filter, priority, partition_key)]
      end

      defp flow_claim_due_keys(_state, type, states, partition_key, priority)
           when partition_key not in [:any, :auto] and is_list(states) do
        Enum.map(states, &FlowKeys.due_key(type, &1, priority, partition_key))
      end

      defp flow_claim_due_keys(state, type, :any, partition_key, priority)
           when partition_key not in [:any, :auto] do
        if flow_due_any_index_enabled?() do
          [FlowKeys.due_any_key(type, priority, partition_key)]
        else
          flow_claim_due_matching_keys(state, type, :any, partition_key, priority)
        end
      end

      defp flow_claim_due_keys(
             state,
             type,
             {:exclude, :any, _exclude_states} = state_filter,
             partition_key,
             priority
           )
           when partition_key not in [:any, :auto] do
        if flow_due_any_index_enabled?() do
          [FlowKeys.due_any_key(type, priority, partition_key)]
        else
          flow_claim_due_matching_keys(state, type, state_filter, partition_key, priority)
        end
      end

      defp flow_claim_due_keys(state, type, state_filter, partition_key, priority) do
        flow_claim_due_matching_keys(state, type, state_filter, partition_key, priority)
      end

      defp flow_claim_due_matching_keys(state, type, state_filter, partition_key, priority) do
        catalog =
          apply_state_get(
            :flow_due_catalog,
            Map.get(state, :flow_due_catalog, DueCatalog.new())
          )

        case DueCatalog.start_selection(
               catalog,
               type,
               priority,
               partition_key,
               state_filter
             ) do
          {:ok, selection} -> selection
          {:error, _reason} -> []
        end
      end

      defp flow_claim_due_scan(
             state,
             due_key,
             type,
             expected_state,
             worker,
             lease_ms,
             now_ms,
             partition_key,
             limit,
             max_scan,
             scanned,
             claimed_count,
             claimed
           ) do
        case flow_claim_due_scan_status(
               state,
               due_key,
               type,
               expected_state,
               worker,
               lease_ms,
               now_ms,
               partition_key,
               limit,
               max_scan,
               scanned,
               claimed_count,
               claimed
             ) do
          {:error, _reason} = error ->
            error

          {next_claimed, next_count, _exhausted?, _scanned} ->
            {next_claimed, next_count}
        end
      end

      defp flow_claim_due_scan_status(
             _state,
             _due_key,
             _type,
             _expected_state,
             _worker,
             _lease_ms,
             _now_ms,
             _partition_key,
             limit,
             _max_scan,
             scanned,
             claimed_count,
             claimed
           )
           when claimed_count >= limit do
        {claimed, claimed_count, false, scanned}
      end

      defp flow_claim_due_scan_status(
             _state,
             _due_key,
             _type,
             _expected_state,
             _worker,
             _lease_ms,
             _now_ms,
             _partition_key,
             _limit,
             max_scan,
             scanned,
             claimed_count,
             claimed
           )
           when scanned >= max_scan do
        {claimed, claimed_count, false, scanned}
      end

      defp flow_claim_due_scan_status(
             state,
             due_key,
             type,
             expected_state,
             worker,
             lease_ms,
             now_ms,
             partition_key,
             limit,
             max_scan,
             scanned,
             claimed_count,
             claimed
           ) do
        remaining = limit - claimed_count

        batch_size =
          if standalone_staged_apply?() do
            max_scan - scanned
          else
            min(max(remaining, 32), max_scan - scanned)
          end

        candidates =
          flow_claim_due_phase(
            :range_slice,
            Map.put(
              flow_claim_due_phase_meta(state, partition_key, nil, remaining),
              :batch_size,
              batch_size
            ),
            fn ->
              flow_claim_due_candidate_slice(state, due_key, now_ms, batch_size)
            end
          )

        if candidates == [] do
          _ =
            flow_after_due_catalog_mutation(
              state,
              [due_key],
              flow_native_ops_queued?()
            )

          {claimed, claimed_count, true, scanned}
        else
          case flow_claim_candidate_batch(
                 state,
                 due_key,
                 type,
                 expected_state,
                 worker,
                 lease_ms,
                 now_ms,
                 partition_key,
                 candidates,
                 limit - claimed_count,
                 claimed_count,
                 claimed
               ) do
            {:error, _reason} = error ->
              error

            {next_claimed_count, next_claimed} ->
              next_scanned = scanned + length(candidates)

              cond do
                next_claimed_count >= limit ->
                  {next_claimed, next_claimed_count, false, next_scanned}

                length(candidates) < batch_size ->
                  {next_claimed, next_claimed_count, true, next_scanned}

                standalone_staged_apply?() ->
                  {next_claimed, next_claimed_count, false, next_scanned}

                true ->
                  flow_claim_due_scan_status(
                    state,
                    due_key,
                    type,
                    expected_state,
                    worker,
                    lease_ms,
                    now_ms,
                    partition_key,
                    limit,
                    max_scan,
                    next_scanned,
                    next_claimed_count,
                    next_claimed
                  )
              end
          end
        end
      end

      defp flow_claim_due_candidate_slice(state, due_key, now_ms, batch_size) do
        case flow_native_index(state) do
          nil ->
            []

          native ->
            candidates =
              case NativeFlowIndex.claim_due_candidates(
                     native,
                     [due_key],
                     now_ms,
                     batch_size,
                     batch_size
                   ) do
                [{^due_key, due_candidates}] -> due_candidates
                [{_key, due_candidates}] -> due_candidates
                _other -> []
              end

            candidates
        end
      end

      defp flow_claim_candidate_batch(
             state,
             due_key,
             type,
             expected_state,
             worker,
             lease_ms,
             now_ms,
             partition_key,
             candidates,
             remaining,
             claimed_count,
             claimed
           ) do
        with {:ok, candidates, records, deferred_timeout_records, fifo_planning?} <-
               flow_prepare_claim_candidates(
                 state,
                 due_key,
                 type,
                 expected_state,
                 partition_key,
                 candidates,
                 remaining,
                 now_ms
               ) do
          {plans, stale_due_ids} =
            flow_plan_claim_candidates(
              state,
              due_key,
              type,
              expected_state,
              worker,
              lease_ms,
              now_ms,
              partition_key,
              candidates,
              records,
              remaining,
              fifo_planning?
            )

          phase_meta =
            state
            |> flow_claim_due_phase_meta(partition_key, nil, remaining)
            |> Map.merge(%{candidates: length(candidates)})

          case flow_apply_claim_batch(
                 state,
                 due_key,
                 plans,
                 stale_due_ids,
                 deferred_timeout_records,
                 now_ms
               ) do
            :ok ->
              next_claimed =
                flow_claim_due_phase(:return_assemble, phase_meta, fn ->
                  Enum.reduce(plans, claimed, fn plan, acc ->
                    {_record, next} = flow_claim_plan_pair(plan)
                    [next | acc]
                  end)
                end)

              {claimed_count + length(plans), next_claimed}

            {:error, _reason} = error ->
              error
          end
        end
      end

      defp flow_prepare_claim_candidates(
             state,
             due_key,
             type,
             state_filter,
             partition_key,
             candidates,
             remaining,
             now_ms
           ) do
        phase_meta =
          state
          |> flow_claim_due_phase_meta(partition_key, nil, remaining)
          |> Map.merge(%{candidates: length(candidates)})

        records =
          flow_claim_due_phase(:hydrate_records, phase_meta, fn ->
            flow_read_claim_candidate_records(state, partition_key, due_key, candidates)
          end)

        policy = flow_read_policy(state, type)
        fifo_planning? = flow_claim_fifo_planning?(policy, type, state_filter, due_key)

        {selected_candidates, selected_records} =
          flow_apply_fifo_candidate_ordering(
            state,
            policy,
            type,
            state_filter,
            due_key,
            partition_key,
            now_ms,
            candidates,
            records,
            fifo_planning?
          )

        {timeout_candidates, timeout_records} =
          flow_timeout_candidate_union(
            candidates,
            records,
            selected_candidates,
            selected_records,
            fifo_planning?
          )

        if Enum.any?(timeout_records, fn
             record when is_map(record) -> flow_active_timeout_expired_record?(record, now_ms)
             _other -> false
           end) do
          case flow_timeout_expired_claim_candidates_fresh(
                 state,
                 due_key,
                 partition_key,
                 timeout_candidates,
                 now_ms
               ) do
            {:ok, active_candidates, active_records, deferred_timeout_records} ->
              {active_candidates, active_records} =
                flow_select_preordered_candidates(
                  active_candidates,
                  active_records,
                  selected_candidates
                )

              {:ok, active_candidates, active_records, deferred_timeout_records, fifo_planning?}

            {:error, _reason} = error ->
              error
          end
        else
          {:ok, selected_candidates, selected_records, [], fifo_planning?}
        end
      end

      defp flow_timeout_expired_claim_candidates_fresh(
             state,
             due_key,
             partition_key,
             candidates,
             now_ms
           ) do
        records =
          flow_read_claim_candidate_records(state, partition_key, due_key, candidates)

        candidates
        |> Enum.zip(records)
        |> Enum.reduce_while(
          {:ok, [], [], []},
          fn {candidate, record}, {:ok, candidate_acc, record_acc, deferred_acc} ->
            case flow_maybe_timeout_claim_record(state, record, now_ms) do
              :active ->
                {:cont, {:ok, [candidate | candidate_acc], [record | record_acc], deferred_acc}}

              :deferred ->
                {:cont, {:ok, candidate_acc, record_acc, [record | deferred_acc]}}

              :timed_out ->
                {:cont, {:ok, candidate_acc, record_acc, deferred_acc}}

              {:error, _reason} = error ->
                {:halt, error}
            end
          end
        )
        |> case do
          {:ok, active_candidates, active_records, deferred_timeout_records} ->
            {:ok, Enum.reverse(active_candidates), Enum.reverse(active_records),
             Enum.reverse(deferred_timeout_records)}

          {:error, _reason} = error ->
            error
        end
      end

      defp flow_maybe_timeout_claim_record(state, record, now_ms) when is_map(record) do
        if flow_active_timeout_expired_record?(record, now_ms) and
             flow_claim_timeout_requires_cross_shard?(state, record) do
          :deferred
        else
          flow_maybe_timeout_active_record(state, record, now_ms)
        end
      end

      defp flow_maybe_timeout_claim_record(_state, _record, _now_ms), do: :active

      defp flow_claim_timeout_requires_cross_shard?(
             state,
             %{parent_flow_id: parent_id} = record
           )
           when is_binary(parent_id) and parent_id != "" do
        not cross_shard_pending_active?() and flow_parent_on_other_shard?(state, record)
      end

      defp flow_claim_timeout_requires_cross_shard?(_state, _record), do: false

      defp flow_parent_on_other_shard?(
             state,
             %{id: child_id, parent_flow_id: parent_id} = record
           )
           when is_binary(child_id) and is_binary(parent_id) and parent_id != "" do
        child_partition_key = Map.get(record, :partition_key)
        parent_partition_key = Map.get(record, :parent_partition_key) || child_partition_key
        child_key = FlowKeys.state_key(child_id, child_partition_key)
        parent_key = FlowKeys.state_key(parent_id, parent_partition_key)

        case cross_shard_instance_ctx(state) do
          %{shard_count: shard_count} = ctx when is_integer(shard_count) and shard_count > 0 ->
            Router.shard_for(ctx, child_key) != Router.shard_for(ctx, parent_key)

          _other ->
            false
        end
      end

      defp flow_parent_on_other_shard?(_state, _record), do: false

      defp flow_maybe_timeout_active_record(state, record, now_ms) when is_map(record) do
        if flow_active_timeout_expired_record?(record, now_ms) do
          state_key = FlowKeys.state_key(Map.fetch!(record, :id), Map.get(record, :partition_key))

          case flow_active_timeout_record(state, state_key, record, now_ms) do
            {:ok, _counts} -> :timed_out
            {:error, _reason} = error -> error
          end
        else
          :active
        end
      end

      defp flow_maybe_timeout_active_record(_state, _record, _now_ms), do: :active

      defp flow_plan_claim_candidates(
             state,
             due_key,
             type,
             state_filter,
             worker,
             lease_ms,
             now_ms,
             partition_key,
             candidates,
             records,
             remaining,
             fifo_planning?
           ) do
        if flow_governance_limit_active?() or fifo_planning? do
          flow_plan_claim_candidates_elixir(
            state,
            due_key,
            type,
            state_filter,
            worker,
            lease_ms,
            now_ms,
            partition_key,
            candidates,
            records,
            remaining
          )
        else
          case flow_plan_claim_candidates_native(
                 state,
                 due_key,
                 type,
                 state_filter,
                 worker,
                 lease_ms,
                 now_ms,
                 partition_key,
                 candidates,
                 remaining
               ) do
            {:ok, result} ->
              result

            :fallback ->
              flow_plan_claim_candidates_elixir(
                state,
                due_key,
                type,
                state_filter,
                worker,
                lease_ms,
                now_ms,
                partition_key,
                candidates,
                records,
                remaining
              )
          end
        end
      end

      defp flow_plan_claim_candidates_elixir(
             state,
             due_key,
             type,
             state_filter,
             worker,
             lease_ms,
             now_ms,
             partition_key,
             candidates,
             records,
             remaining
           ) do
        phase_meta =
          state
          |> flow_claim_due_phase_meta(partition_key, nil, remaining)
          |> Map.merge(%{candidates: length(candidates)})

        {plans, stale_due_ids, _count} =
          flow_claim_due_phase(:plan_candidates, phase_meta, fn ->
            flow_plan_claim_candidate_records(
              candidates,
              records,
              type,
              state_filter,
              worker,
              lease_ms,
              now_ms,
              remaining
            )
          end)

        {Enum.reverse(plans), Enum.reverse(stale_due_ids)}
      end

      defp flow_plan_claim_candidate_records(
             candidates,
             records,
             type,
             state_filter,
             worker,
             lease_ms,
             now_ms,
             remaining
           ) do
        flow_plan_claim_candidate_records(
          candidates,
          records,
          type,
          state_filter,
          worker,
          lease_ms,
          now_ms,
          remaining,
          [],
          [],
          0
        )
      end

      defp flow_plan_claim_candidate_records(
             _candidates,
             _records,
             _type,
             _state_filter,
             _worker,
             _lease_ms,
             _now_ms,
             remaining,
             plans,
             stale_due_ids,
             count
           )
           when count >= remaining do
        {plans, stale_due_ids, count}
      end

      defp flow_plan_claim_candidate_records(
             [],
             _records,
             _type,
             _state_filter,
             _worker,
             _lease_ms,
             _now_ms,
             _remaining,
             plans,
             stale_due_ids,
             count
           ) do
        {plans, stale_due_ids, count}
      end

      defp flow_plan_claim_candidate_records(
             _candidates,
             [],
             _type,
             _state_filter,
             _worker,
             _lease_ms,
             _now_ms,
             _remaining,
             plans,
             stale_due_ids,
             count
           ) do
        {plans, stale_due_ids, count}
      end

      defp flow_plan_claim_candidate_records(
             [{id, due_score} | candidates],
             [record | records],
             type,
             state_filter,
             worker,
             lease_ms,
             now_ms,
             remaining,
             plans,
             stale_due_ids,
             count
           ) do
        case flow_prepare_claim_candidate_record(
               record,
               id,
               type,
               state_filter,
               worker,
               lease_ms,
               now_ms,
               due_score
             ) do
          {:ok, record, next, from_due_score} ->
            flow_plan_claim_candidate_records(
              candidates,
              records,
              type,
              state_filter,
              worker,
              lease_ms,
              now_ms,
              remaining,
              [{record, next, from_due_score} | plans],
              stale_due_ids,
              count + 1
            )

          :delete_due ->
            flow_plan_claim_candidate_records(
              candidates,
              records,
              type,
              state_filter,
              worker,
              lease_ms,
              now_ms,
              remaining,
              plans,
              [id | stale_due_ids],
              count
            )

          {:skip, _restore_score} ->
            flow_plan_claim_candidate_records(
              candidates,
              records,
              type,
              state_filter,
              worker,
              lease_ms,
              now_ms,
              remaining,
              plans,
              stale_due_ids,
              count
            )
        end
      end
    end
  end
end
