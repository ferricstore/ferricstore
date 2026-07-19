defmodule Ferricstore.Raft.StateMachine.Sections.FlowFifoLane do
  @moduledoc false

  import Kernel, except: [apply: 3]

  defmacro __using__(_opts) do
    quote location: :keep do
      import Kernel, except: [apply: 3]

      alias Ferricstore.Flow.FifoLane
      alias Ferricstore.Flow.Keys, as: FlowKeys
      alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
      alias Ferricstore.Flow.RetryPolicy

      defp flow_claim_fifo_planning?(policy, type, state_filter, due_key) do
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
              :any -> RetryPolicy.any_fifo_state?(policy)
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
            RetryPolicy.any_fifo_state?(policy, fn flow_state ->
              flow_state not in exclude_states and
                flow_claim_state_match?(state_filter, flow_state)
            end)

          :error ->
            false
        end
      end

      defp flow_due_key_state(type, due_key) when is_binary(type) and is_binary(due_key) do
        encoded_type = FlowKeys.index_component(type)
        marker = "}:d:" <> encoded_type <> ":"

        case :binary.match(due_key, marker) do
          {pos, len} ->
            start = pos + len
            rest = binary_part(due_key, start, byte_size(due_key) - start)

            case :binary.match(rest, ":p") do
              {state_len, _priority_len} when state_len > 0 ->
                rest
                |> binary_part(0, state_len)
                |> Base.url_decode64(padding: false)

              _other ->
                :error
            end

          :nomatch ->
            if :binary.match(due_key, "}:da:" <> encoded_type <> ":p") == :nomatch do
              :error
            else
              :any
            end
        end
      end

      defp flow_due_key_state(_type, _due_key), do: :error

      defp flow_apply_fifo_candidate_ordering(
             state,
             policy,
             type,
             state_filter,
             due_key,
             partition_key,
             now_ms,
             candidates,
             records,
             true
           ) do
        candidates
        |> Enum.zip(records)
        |> flow_filter_fifo_candidate_pairs(
          state,
          policy,
          type,
          state_filter,
          due_key,
          partition_key,
          now_ms
        )
        |> Enum.unzip()
      end

      defp flow_apply_fifo_candidate_ordering(
             _state,
             _policy,
             _type,
             _state_filter,
             _due_key,
             _partition_key,
             _now_ms,
             candidates,
             records,
             false
           ),
           do: {candidates, records}

      defp flow_filter_fifo_candidate_pairs(
             pairs,
             state,
             policy,
             type,
             state_filter,
             due_key,
             partition_key,
             now_ms
           ) do
        classified =
          Enum.map(pairs, fn {_candidate, record} = pair ->
            {flow_fifo_candidate_lane(policy, record), pair}
          end)

        fifo_entries =
          classified
          |> Enum.filter(fn {lane, _pair} -> match?({:fifo, _lane}, lane) end)
          |> Enum.group_by(fn {{:fifo, lane}, _pair} -> lane end, fn {_lane, pair} -> pair end)

        fifo_entries_by_key =
          Map.new(fifo_entries, fn {{lane_type, lane_state, lane_partition}, entries} ->
            {FifoLane.lane_key(lane_type, lane_state, lane_partition), entries}
          end)

        lane_keys = flow_fifo_claim_lane_keys(policy, type, state_filter, due_key)
        lane_heads = flow_fifo_lane_heads(state, due_key, lane_keys)

        injected_heads =
          flow_due_fifo_lane_head_pairs(
            state,
            policy,
            state_filter,
            due_key,
            partition_key,
            fifo_entries_by_key,
            lane_heads,
            now_ms
          )

        selected_fifo_pairs =
          Enum.flat_map(lane_heads, fn {lane_key, %{id: head_id}} ->
            entries = Map.get(fifo_entries_by_key, lane_key, [])

            case flow_find_fifo_head_pair(entries, head_id) do
              nil -> List.wrap(Map.get(injected_heads, lane_key))
              pair -> [pair]
            end
          end)

        parallel_pairs =
          Enum.flat_map(classified, fn
            {{:fifo, _lane}, _pair} -> []
            {_lane, pair} -> [pair]
          end)

        Enum.sort(parallel_pairs ++ selected_fifo_pairs, &flow_fifo_candidate_pair_before?/2)
      end

      defp flow_fifo_claim_lane_keys(policy, type, state_filter, due_key) do
        fifo_states = RetryPolicy.fifo_states(policy)

        states =
          case flow_due_key_state(type, due_key) do
            {:ok, flow_state} -> [flow_state]
            :any -> MapSet.to_list(fifo_states)
            :error -> []
          end

        states
        |> Enum.filter(
          &(MapSet.member?(fifo_states, &1) and flow_claim_state_match?(state_filter, &1))
        )
        |> Enum.map(&FifoLane.lane_key_from_due_key(due_key, type, &1))
      end

      defp flow_replace_fifo_candidate_runs(
             state,
             policy,
             type,
             state_filter,
             due_keys,
             candidate_runs,
             now_ms
           ) do
        descriptors = flow_fifo_due_descriptors(policy, type, state_filter, due_keys)

        if descriptors == [] do
          candidate_runs
        else
          fifo_due_keys = descriptors |> Enum.map(&elem(&1, 0)) |> MapSet.new()

          non_fifo_rows =
            Enum.flat_map(candidate_runs, fn {due_key, candidates} ->
              if MapSet.member?(fifo_due_keys, due_key) do
                []
              else
                Enum.map(candidates, fn {id, score} -> {due_key, id, score} end)
              end
            end)

          fifo_rows = flow_fifo_head_due_rows(state, descriptors, now_ms)

          (non_fifo_rows ++ fifo_rows)
          |> Enum.sort(&flow_fifo_due_row_before?/2)
          |> flow_fifo_candidate_runs_from_rows()
        end
      end

      defp flow_order_fifo_catalog_due_keys(
             state,
             policy,
             type,
             state_filter,
             due_keys,
             catalog_entries
           ) do
        descriptors = flow_fifo_due_descriptors(policy, type, state_filter, due_keys)
        fifo_due_keys = descriptors |> Enum.map(&elem(&1, 0)) |> MapSet.new()
        fifo_scores = flow_fifo_head_score_map(state, descriptors)

        due_keys
        |> Enum.with_index()
        |> Enum.sort_by(fn {due_key, index} ->
          score =
            if MapSet.member?(fifo_due_keys, due_key) do
              Map.get(fifo_scores, due_key)
            else
              case Map.get(catalog_entries, due_key) do
                %{score: score} when is_number(score) -> score
                _missing -> nil
              end
            end

          case score do
            score when is_number(score) -> {0, score, index, due_key}
            _blocked_or_missing -> {1, 0, index, due_key}
          end
        end)
        |> Enum.map(&elem(&1, 0))
      end

      defp flow_fifo_due_descriptors(policy, type, state_filter, due_keys) do
        Enum.flat_map(due_keys, fn due_key ->
          case flow_due_key_state(type, due_key) do
            {:ok, flow_state} ->
              if RetryPolicy.state_fifo?(policy, flow_state) and
                   flow_claim_state_match?(state_filter, flow_state) do
                [{due_key, FifoLane.lane_key_from_due_key(due_key, type, flow_state)}]
              else
                []
              end

            _any_or_invalid ->
              []
          end
        end)
      end

      defp flow_fifo_head_score_map(_state, []), do: %{}

      defp flow_fifo_head_score_map(state, descriptors) do
        case flow_native_index(state) do
          nil ->
            %{}

          native ->
            case NativeFlowIndex.fifo_lane_heads_many(native, descriptors) do
              rows when is_list(rows) ->
                Map.new(rows, fn
                  {due_key, _lane_key, _member, due_score} -> {due_key, due_score}
                end)

              _native_error ->
                %{}
            end
        end
      end

      defp flow_fifo_head_due_rows(state, descriptors, now_ms) do
        case flow_native_index(state) do
          nil ->
            []

          native ->
            case NativeFlowIndex.fifo_lane_heads_many(native, descriptors) do
              rows when is_list(rows) ->
                Enum.flat_map(rows, fn
                  {due_key, _lane_key, member, due_score}
                  when is_number(due_score) and due_score <= now_ms ->
                    case FifoLane.decode_member(member) do
                      {:ok, {_sequence, id}} -> [{due_key, id, due_score}]
                      :error -> []
                    end

                  _missing_not_due_or_invalid ->
                    []
                end)

              _native_error ->
                []
            end
        end
      end

      defp flow_fifo_due_row_before?(
             {left_key, left_id, left_score},
             {right_key, right_id, right_score}
           ) do
        left_score < right_score or
          (left_score == right_score and
             (left_id < right_id or (left_id == right_id and left_key <= right_key)))
      end

      defp flow_fifo_candidate_runs_from_rows(rows) do
        rows
        |> Enum.reduce([], fn {due_key, id, score}, runs ->
          case runs do
            [{^due_key, candidates} | rest] ->
              [{due_key, [{id, score} | candidates]} | rest]

            _other ->
              [{due_key, [{id, score}]} | runs]
          end
        end)
        |> Enum.reverse()
        |> Enum.map(fn {due_key, candidates} -> {due_key, Enum.reverse(candidates)} end)
      end

      defp flow_fifo_lane_heads(_state, _due_key, []), do: %{}

      defp flow_fifo_lane_heads(state, due_key, lane_keys) do
        case flow_native_index(state) do
          nil ->
            %{}

          native ->
            case NativeFlowIndex.fifo_lane_heads(native, due_key, lane_keys) do
              rows when is_list(rows) ->
                Enum.reduce(rows, %{}, fn
                  {lane_key, member, due_score}, acc ->
                    case FifoLane.decode_member(member) do
                      {:ok, {_sequence, id}} ->
                        Map.put(acc, lane_key, %{id: id, due_score: due_score})

                      :error ->
                        acc
                    end

                  _invalid, acc ->
                    acc
                end)

              _native_error ->
                %{}
            end
        end
      end

      defp flow_due_fifo_lane_head_pairs(
             state,
             policy,
             state_filter,
             due_key,
             partition_key,
             fifo_entries_by_key,
             lane_heads,
             now_ms
           ) do
        requests =
          Enum.flat_map(lane_heads, fn
            {lane_key, %{id: head_id, due_score: due_score}}
            when is_number(due_score) and due_score <= now_ms ->
              entries = Map.get(fifo_entries_by_key, lane_key, [])

              if flow_find_fifo_head_pair(entries, head_id) == nil do
                [{lane_key, {head_id, due_score}}]
              else
                []
              end

            {_lane_key, _missing_or_not_due} ->
              []
          end)

        candidates = Enum.map(requests, fn {_lane_key, candidate} -> candidate end)

        records =
          flow_read_claim_candidate_records(state, partition_key, due_key, candidates)

        requests
        |> Enum.zip(records)
        |> Enum.reduce(%{}, fn {{lane_key, candidate}, record}, acc ->
          case flow_fifo_candidate_lane(policy, record) do
            {:fifo, {lane_type, flow_state, lane_partition}} ->
              if flow_claim_state_match?(state_filter, flow_state) and
                   FifoLane.lane_key(lane_type, flow_state, lane_partition) == lane_key do
                Map.put(acc, lane_key, {candidate, record})
              else
                acc
              end

            _not_fifo ->
              acc
          end
        end)
      end

      defp flow_find_fifo_head_pair(entries, head_id) do
        Enum.find_value(entries, fn {{candidate_id, _due_score}, record} = pair ->
          record_id = Map.get(record || %{}, :id)

          if head_id in [candidate_id, record_id] do
            pair
          else
            nil
          end
        end)
      end

      defp flow_fifo_candidate_pair_before?(
             {{left_id, left_score}, _left_record},
             {{right_id, right_score}, _right_record}
           ) do
        left_score < right_score or (left_score == right_score and left_id <= right_id)
      end

      defp flow_fifo_candidate_lane(_policy, nil), do: :stale

      defp flow_fifo_candidate_lane(policy, record) when is_map(record) do
        flow_state = flow_record_logical_state(record)

        if RetryPolicy.state_fifo?(policy, flow_state) do
          {:fifo, {Map.get(record, :type), flow_state, Map.get(record, :partition_key)}}
        else
          :parallel
        end
      end

      defp flow_timeout_candidate_union(
             candidates,
             records,
             _selected_candidates,
             _selected_records,
             false
           ),
           do: {candidates, records}

      defp flow_timeout_candidate_union(
             candidates,
             records,
             selected_candidates,
             selected_records,
             true
           ) do
        original_ids =
          candidates
          |> Enum.map(&flow_claim_candidate_id/1)
          |> MapSet.new()

        {extra_candidates, extra_records} =
          selected_candidates
          |> Enum.zip(selected_records)
          |> Enum.reject(fn {candidate, _record} ->
            MapSet.member?(original_ids, flow_claim_candidate_id(candidate))
          end)
          |> Enum.unzip()

        {candidates ++ extra_candidates, records ++ extra_records}
      end

      defp flow_select_preordered_candidates(candidates, records, selected_candidates) do
        pairs_by_id =
          candidates
          |> Enum.zip(records)
          |> Map.new(fn {candidate, _record} = pair ->
            {flow_claim_candidate_id(candidate), pair}
          end)

        selected_candidates
        |> Enum.flat_map(fn candidate ->
          case Map.fetch(pairs_by_id, flow_claim_candidate_id(candidate)) do
            {:ok, pair} -> [pair]
            :error -> []
          end
        end)
        |> Enum.unzip()
      end

      defp flow_claim_candidate_id({id, _score}), do: id

      defp flow_lane_insert_records(state, records) do
        with {:ok, entries} <- flow_lane_entries(records) do
          flow_native_put_new_entries(state, entries)
        end
      end

      defp flow_lane_insert_record(state, record) do
        flow_lane_insert_records(state, [record])
      end

      defp flow_lane_entries(records) do
        Enum.reduce_while(records, {:ok, []}, fn record, {:ok, entries} ->
          case flow_lane_record_identity(record) do
            nil -> {:cont, {:ok, entries}}
            {:ok, identity} -> {:cont, {:ok, [flow_lane_index_entry(identity) | entries]}}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
        |> case do
          {:ok, entries} -> {:ok, Enum.reverse(entries)}
          {:error, _reason} = error -> error
        end
      end

      defp flow_lane_move_plans(state, plans) do
        plans
        |> Enum.reduce_while({:ok, %{}, []}, fn plan, {:ok, deletes, puts} ->
          case flow_lane_move_plan_changes(plan, deletes, puts) do
            {:ok, next_deletes, next_puts} ->
              {:cont, {:ok, next_deletes, next_puts}}

            {:error, _reason} = error ->
              {:halt, error}
          end
        end)
        |> case do
          {:ok, deletes, puts} ->
            with :ok <- flow_lane_delete_groups(state, deletes) do
              flow_native_put_new_entries(state, Enum.reverse(puts))
            end

          {:error, _reason} = error ->
            error
        end
      end

      defp flow_lane_move_plan_changes(
             {:native_claim, next, _entry, _state_key, _value, _previous_history_ms},
             deletes,
             puts
           ),
           do: flow_lane_native_claim_changes(next, deletes, puts)

      defp flow_lane_move_plan_changes(
             {:native_claim, next, _entry, _state_key, _value, _previous_history_ms,
              _history_entry},
             deletes,
             puts
           ),
           do: flow_lane_native_claim_changes(next, deletes, puts)

      defp flow_lane_move_plan_changes(plan, deletes, puts) do
        {record, next} = flow_claim_plan_pair(plan)
        flow_lane_move_changes(record, next, deletes, puts)
      end

      defp flow_lane_native_claim_changes(next, deletes, puts) do
        previous =
          next
          |> Map.put(:state, flow_record_logical_state(next))
          |> Map.put(:run_state, nil)

        flow_lane_move_changes(previous, next, deletes, puts)
      end

      defp flow_lane_move_changes(record, next, deletes, puts) do
        from = flow_lane_record_identity(record)
        to = flow_lane_record_identity(next)

        cond do
          match?({:error, _reason}, from) ->
            from

          match?({:error, _reason}, to) ->
            to

          flow_lane_same_identity?(from, to) ->
            {:ok, deletes, puts}

          true ->
            deletes = flow_lane_add_delete(deletes, from)
            puts = flow_lane_add_put(puts, to)
            {:ok, deletes, puts}
        end
      end

      defp flow_lane_same_identity?(nil, nil), do: true

      defp flow_lane_same_identity?(
             {:ok, %{lane_key: lane_key, member: member, score: score}},
             {:ok, %{lane_key: lane_key, member: member, score: score}}
           ),
           do: true

      defp flow_lane_same_identity?(_from, _to), do: false

      defp flow_lane_add_delete(deletes, nil), do: deletes

      defp flow_lane_add_delete(deletes, {:ok, %{lane_key: lane_key, member: member}}) do
        Map.update(deletes, lane_key, [member], &[member | &1])
      end

      defp flow_lane_add_put(puts, nil), do: puts
      defp flow_lane_add_put(puts, {:ok, identity}), do: [flow_lane_index_entry(identity) | puts]

      defp flow_lane_delete_groups(state, deletes) do
        entries =
          Enum.flat_map(deletes, fn {lane_key, members} ->
            Enum.map(members, &{lane_key, &1})
          end)

        flow_native_delete_entries(state, entries)
      end

      defp flow_lane_index_entry(%{lane_key: lane_key, member: member, score: score}),
        do: {lane_key, member, score}

      defp flow_lane_record_identity(record) when is_map(record) do
        case FifoLane.identity(record) do
          :error -> {:error, "ERR flow record has invalid lane identity"}
          identity -> identity
        end
      end

      defp flow_lane_record_identity(_record),
        do: {:error, "ERR flow record has invalid lane identity"}
    end
  end
end
