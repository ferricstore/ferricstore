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

        candidates =
          NativeFlowIndex.claim_due_candidates(native, due_keys, now_ms, batch_size, batch_size)

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
          Enum.flat_map(partition_keys, fn partition_key ->
            flow_claim_due_matching_keys(state, type, :any, partition_key, priority)
          end)
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
          Enum.flat_map(partition_keys, fn partition_key ->
            flow_claim_due_matching_keys(state, type, state_filter, partition_key, priority)
          end)
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
        state
        |> flow_claim_index_count_keys()
        |> Enum.filter(&flow_due_key_matches?(&1, type, state_filter, partition_key, priority))
      end

      defp flow_due_key_matches?(key, type, state_filter, partition_key, priority)
           when is_binary(key) do
        String.starts_with?(key, "f:{f") and
          flow_due_key_partition_match?(key, partition_key) and
          flow_due_key_state_match?(key, type, state_filter) and
          String.ends_with?(key, ":p" <> Integer.to_string(priority))
      end

      defp flow_due_key_matches?(_key, _type, _state_filter, _partition_key, _priority), do: false

      defp flow_due_key_partition_match?(_key, :any), do: true

      defp flow_due_key_partition_match?(key, :auto) do
        String.starts_with?(key, "f:{fa:") and
          (String.contains?(key, "}:d:") or String.contains?(key, "}:da:"))
      end

      defp flow_due_key_partition_match?(key, partition_keys) when is_list(partition_keys) do
        Enum.any?(partition_keys, &flow_due_key_partition_match?(key, &1))
      end

      defp flow_due_key_partition_match?(key, partition_key) do
        tag = FlowKeys.tag(partition_key)

        String.starts_with?(key, "f:" <> tag <> ":d:") or
          String.starts_with?(key, "f:" <> tag <> ":da:")
      end

      defp flow_due_key_state_match?(key, type, :any) do
        if flow_due_any_index_enabled?() do
          String.contains?(key, "}:da:" <> type <> ":p")
        else
          String.contains?(key, "}:d:" <> type <> ":")
        end
      end

      defp flow_due_key_state_match?(key, type, {:exclude, state_filter, _exclude_states}) do
        flow_due_key_state_match?(key, type, state_filter)
      end

      defp flow_due_key_state_match?(key, type, states) when is_list(states) do
        Enum.any?(states, &flow_due_key_state_match?(key, type, &1))
      end

      defp flow_due_key_state_match?(key, type, state) when is_binary(state) do
        String.contains?(key, "}:d:" <> type <> ":" <> state <> ":p")
      end

      defp flow_due_key_state_match?(_key, _type, _state), do: false

      defp flow_claim_due_scan(
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
             _scanned,
             claimed_count,
             claimed
           )
           when claimed_count >= limit do
        {claimed, claimed_count}
      end

      defp flow_claim_due_scan(
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
        {claimed, claimed_count}
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
          {claimed, claimed_count}
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
              if standalone_staged_apply?() or length(candidates) < batch_size do
                {next_claimed, next_claimed_count}
              else
                flow_claim_due_scan(
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
                  scanned + length(candidates),
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
        with {:ok, candidates, deferred_timeout_records} <-
               flow_timeout_expired_claim_candidates(
                 state,
                 due_key,
                 partition_key,
                 candidates,
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
              remaining
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

      defp flow_timeout_expired_claim_candidates(
             state,
             due_key,
             partition_key,
             candidates,
             now_ms
           ) do
        records = flow_read_claim_candidate_records(state, partition_key, due_key, candidates)

        if Enum.any?(records, fn
             record when is_map(record) -> flow_active_timeout_expired_record?(record, now_ms)
             _other -> false
           end) do
          flow_timeout_expired_claim_candidates_fresh(
            state,
            due_key,
            partition_key,
            candidates,
            now_ms
          )
        else
          {:ok, candidates, []}
        end
      end

      defp flow_timeout_expired_claim_candidates_fresh(
             state,
             due_key,
             partition_key,
             candidates,
             now_ms
           ) do
        candidates
        |> Enum.reduce_while({:ok, [], []}, fn candidate, {:ok, active_acc, deferred_acc} ->
          record =
            case flow_read_claim_candidate_records(
                   state,
                   partition_key,
                   due_key,
                   [candidate]
                 ) do
              [record] -> record
              _other -> nil
            end

          case flow_maybe_timeout_claim_record(state, record, now_ms) do
            :active -> {:cont, {:ok, [candidate | active_acc], deferred_acc}}
            :deferred -> {:cont, {:ok, active_acc, [record | deferred_acc]}}
            :timed_out -> {:cont, {:ok, active_acc, deferred_acc}}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
        |> case do
          {:ok, active_candidates, deferred_timeout_records} ->
            {:ok, Enum.reverse(active_candidates), Enum.reverse(deferred_timeout_records)}

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
             remaining
           ) do
        if flow_claim_fifo_planning?(state, type, state_filter, due_key) do
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
             remaining
           ) do
        phase_meta =
          state
          |> flow_claim_due_phase_meta(partition_key, nil, remaining)
          |> Map.merge(%{candidates: length(candidates)})

        records =
          flow_claim_due_phase(:hydrate_records, phase_meta, fn ->
            flow_read_claim_candidate_records(state, partition_key, due_key, candidates)
          end)

        {candidates, records} =
          flow_apply_fifo_candidate_ordering(
            state,
            type,
            state_filter,
            due_key,
            now_ms,
            candidates,
            records
          )

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

      defp flow_claim_fifo_planning?(state, type, state_filter, due_key) do
        policy = flow_read_policy(state, type)

        cond do
          is_binary(state_filter) ->
            RetryPolicy.state_fifo?(policy, state_filter)

          is_list(state_filter) ->
            case flow_due_key_state(type, due_key) do
              {:ok, flow_state} ->
                flow_state in state_filter and RetryPolicy.state_fifo?(policy, flow_state)

              :any ->
                Enum.any?(state_filter, &RetryPolicy.state_fifo?(policy, &1))

              :error ->
                false
            end

          match?({:exclude, _state_filter, _exclude_states}, state_filter) ->
            flow_claim_fifo_planning_for_exclusion?(policy, state_filter, due_key, type)

          true ->
            case flow_due_key_state(type, due_key) do
              {:ok, flow_state} -> RetryPolicy.state_fifo?(policy, flow_state)
              :any -> MapSet.size(RetryPolicy.fifo_states(policy)) > 0
              :error -> false
            end
        end
      end

      defp flow_claim_fifo_planning_for_exclusion?(
             policy,
             {:exclude, state_filter, exclude_states},
             due_key,
             type
           ) do
        case flow_due_key_state(type, due_key) do
          {:ok, flow_state} ->
            flow_state not in exclude_states and RetryPolicy.state_fifo?(policy, flow_state)

          :any ->
            policy
            |> RetryPolicy.fifo_states()
            |> Enum.any?(fn flow_state ->
              flow_state not in exclude_states and
                flow_claim_state_match?(state_filter, flow_state)
            end)

          :error ->
            false
        end
      end

      defp flow_due_key_state(type, due_key) when is_binary(type) and is_binary(due_key) do
        marker = "}:d:" <> type <> ":"

        case :binary.match(due_key, marker) do
          {pos, len} ->
            start = pos + len
            rest = binary_part(due_key, start, byte_size(due_key) - start)

            case :binary.match(rest, ":p") do
              {state_len, _priority_len} when state_len > 0 ->
                {:ok, binary_part(rest, 0, state_len)}

              _other ->
                :error
            end

          :nomatch ->
            if :binary.match(due_key, "}:da:" <> type <> ":p") == :nomatch do
              :error
            else
              :any
            end
        end
      end

      defp flow_due_key_state(_type, _due_key), do: :error

      defp flow_apply_fifo_candidate_ordering(
             state,
             type,
             state_filter,
             due_key,
             now_ms,
             candidates,
             records
           ) do
        if flow_claim_fifo_planning?(state, type, state_filter, due_key) do
          candidates
          |> Enum.zip(records)
          |> flow_filter_fifo_candidate_pairs(state, now_ms)
          |> Enum.unzip()
        else
          {candidates, records}
        end
      end

      defp flow_filter_fifo_candidate_pairs(pairs, state, now_ms) do
        classified =
          pairs
          |> Enum.with_index()
          |> Enum.map(fn {{_candidate, record} = pair, idx} ->
            {idx, flow_fifo_candidate_lane(state, record, now_ms), pair}
          end)

        fifo_pairs =
          classified
          |> Enum.filter(fn {_idx, lane, _pair} -> match?({:fifo, _lane}, lane) end)
          |> Enum.group_by(fn {_idx, {:fifo, lane}, _pair} -> lane end)
          |> Enum.flat_map(fn {lane, entries} ->
            flow_select_fifo_lane_candidate(state, lane, entries, now_ms)
          end)

        pass_through =
          classified
          |> Enum.filter(fn
            {_idx, {:fifo, _lane}, _pair} -> false
            _entry -> true
          end)
          |> Enum.map(fn {idx, _lane, pair} -> {idx, pair} end)

        (fifo_pairs ++ pass_through)
        |> Enum.sort_by(fn {idx, _pair} -> idx end)
        |> Enum.map(fn {_idx, pair} -> pair end)
      end

      defp flow_select_fifo_lane_candidate(state, lane, entries, now_ms) do
        cond do
          flow_fifo_lane_active?(state, lane, now_ms) ->
            []

          true ->
            head_id = flow_fifo_lane_head_id(state, lane)

            entries
            |> Enum.find_value(fn {idx, {:fifo, _lane},
                                   {{candidate_id, _due_score}, record} = pair} ->
              record_id = Map.get(record || %{}, :id)

              if head_id in [candidate_id, record_id] do
                {idx, pair}
              else
                nil
              end
            end)
            |> case do
              nil -> []
              selected -> [selected]
            end
        end
      end

      defp flow_fifo_candidate_lane(_state, nil, _now_ms), do: :stale

      defp flow_fifo_candidate_lane(state, record, _now_ms) when is_map(record) do
        flow_state = flow_record_logical_state(record)
        policy = flow_read_policy(state, Map.get(record, :type))

        if RetryPolicy.state_fifo?(policy, flow_state) do
          {:fifo, {Map.get(record, :type), flow_state, Map.get(record, :partition_key)}}
        else
          :parallel
        end
      end

      defp flow_fifo_lane_active?(_state, {_type, _flow_state, nil}, _now_ms), do: false

      defp flow_fifo_lane_active?(state, {type, flow_state, partition_key}, now_ms) do
        key = FlowKeys.inflight_index_key(type, partition_key)

        case flow_index_count_all(state, key) do
          count when count > 0 ->
            state
            |> flow_index_rank_range(key, 0, count - 1, false)
            |> Enum.any?(fn {id, _score} ->
              case flow_read_record(state, id, partition_key) do
                %{state: "running"} = running ->
                  flow_record_logical_state(running) == flow_state and
                    flow_live_lease?(running, now_ms)

                _other ->
                  false
              end
            end)

          _count ->
            false
        end
      end

      defp flow_fifo_lane_head_id(_state, {_type, _flow_state, nil}), do: nil

      defp flow_fifo_lane_head_id(state, {type, flow_state, partition_key}) do
        key = FlowKeys.state_index_key(type, flow_state, partition_key)

        case flow_index_count_all(state, key) do
          count when count > 0 ->
            state
            |> flow_index_rank_range(key, 0, count - 1, false)
            |> Enum.flat_map(fn {id, _score} ->
              case flow_read_record(state, id, partition_key) do
                %{id: record_id, type: ^type} = record
                when is_binary(record_id) ->
                  if flow_record_logical_state(record) == flow_state do
                    [record]
                  else
                    []
                  end

                _other ->
                  []
              end
            end)
            |> Enum.min_by(&flow_fifo_record_order/1, fn -> nil end)
            |> case do
              %{id: id} -> id
              _other -> nil
            end

          _count ->
            nil
        end
      end

      defp flow_fifo_record_order(%{state_enter_seq: seq, id: id}) when is_integer(seq),
        do: {0, seq, id}

      defp flow_fifo_record_order(%{created_at_ms: created_at_ms, id: id})
           when is_integer(created_at_ms),
           do: {1, created_at_ms, id}

      defp flow_fifo_record_order(%{id: id}), do: {2, id}

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
