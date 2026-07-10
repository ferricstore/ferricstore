defmodule Ferricstore.Raft.StateMachine.Sections.FlowClaimIndexes do
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

      defp flow_apply_claim_batch(_state, _due_key, [], [], [], _now_ms), do: :ok

      defp flow_apply_claim_batch(
             state,
             due_key,
             plans,
             stale_due_ids,
             deferred_timeout_records,
             now_ms
           ) do
        stale_due_entries =
          Enum.map(stale_due_ids, &{due_key, &1}) ++
            flow_deferred_timeout_due_index_deletes(deferred_timeout_records)

        phase_meta =
          state
          |> flow_claim_due_phase_meta()
          |> Map.merge(%{plans: length(plans), stale_due_ids: length(stale_due_entries)})

        with :ok <-
               flow_claim_due_phase(:delete_stale_due, phase_meta, fn ->
                 flow_zset_lifecycle_index_delete_grouped(state, stale_due_entries)
               end),
             :ok <-
               flow_claim_due_phase(:move_indexes, phase_meta, fn ->
                 flow_claim_move_indexes(state, plans)
               end),
             :ok <-
               flow_claim_due_phase(:state_write, phase_meta, fn ->
                 flow_claim_put_state_records(state, plans)
               end),
             :ok <-
               flow_claim_due_phase(:history_write, phase_meta, fn ->
                 flow_claim_put_history(state, plans, now_ms)
               end) do
          :ok
        end
      end

      defp flow_deferred_timeout_due_index_deletes(records) do
        Enum.flat_map(records, fn record ->
          entries =
            case flow_due_index_key(record) do
              key when is_binary(key) -> [{key, record.id}]
              nil -> []
            end

          if flow_due_any_index_enabled?() do
            case flow_due_any_index_key(record) do
              key when is_binary(key) -> [{key, record.id} | entries]
              nil -> entries
            end
          else
            entries
          end
        end)
      end

      defp flow_claim_move_indexes(_state, []), do: :ok

      defp flow_claim_move_indexes(state, plans) do
        case flow_claim_move_indexes_fast(state, plans) do
          :ok -> flow_claim_move_due_any_indexes(state, plans)
          {:error, _reason} = error -> error
          :fallback -> flow_claim_move_indexes_generic(state, plans)
        end
      end

      defp flow_claim_move_indexes_generic(state, plans) do
        {moves, deletes, puts} =
          Enum.reduce(plans, {[], [], %{}}, fn plan, {moves, deletes, puts} ->
            {record, next} = flow_claim_plan_pair(plan)

            {moves, deletes, puts} =
              flow_claim_due_index_plan(record, next, moves, deletes, puts)

            moves =
              record
              |> flow_claim_state_index_move(next)
              |> then(&[&1 | moves])

            moves =
              record
              |> flow_claim_metadata_index_moves(next)
              |> Enum.reduce(moves, fn move, acc -> [move | acc] end)

            flow_claim_queue_old_terminal_lmdb_deletes(state, record)
            deletes = flow_claim_old_running_index_deletes(record, deletes)
            puts = flow_claim_new_running_index_puts(next, puts)

            {moves, deletes, puts}
          end)

        with :ok <- flow_index_move_entries(state, Enum.reverse(moves)),
             :ok <- flow_zset_index_delete_grouped(state, deletes) do
          flow_claim_put_grouped_zset_entries(state, puts)
        end
      end

      defp flow_claim_move_due_any_indexes(_state, []), do: :ok

      defp flow_claim_move_due_any_indexes(state, plans) do
        if flow_due_any_index_enabled?() do
          {moves, deletes, puts} =
            Enum.reduce(plans, {[], [], %{}}, fn plan, acc ->
              {record, next} = flow_claim_plan_pair(plan)
              flow_due_any_index_plan(record, next, acc)
            end)

          with :ok <- flow_index_move_lifecycle_entries(state, Enum.reverse(moves)),
               :ok <- flow_zset_lifecycle_index_delete_grouped(state, deletes) do
            flow_claim_put_grouped_zset_entries(state, puts)
          end
        else
          :ok
        end
      end

      defp flow_claim_move_indexes_fast(state, plans) do
        cond do
          flow_native_index(state) == nil ->
            {:error, :flow_native_index_unavailable}

          true ->
            phase_meta =
              state
              |> flow_claim_due_phase_meta()
              |> Map.merge(%{plans: length(plans)})

            case flow_claim_due_internal_phase(
                   :fast_index_entries,
                   phase_meta,
                   %{items: length(plans)},
                   fn -> flow_claim_fast_index_entries(state, plans) end
                 ) do
              {:ok, entries} ->
                flow_claim_apply_fast_index_entries(state, entries)

              :fallback ->
                :fallback
            end
        end
      end

      defp flow_claim_fast_index_entries(
             _state,
             [{:native_claim, _next, _entry, _key, _value, _prev} | _] = plans
           ) do
        Enum.reduce_while(plans, {:ok, []}, fn
          {:native_claim, _next, entry, _state_key, _value, _previous_history_ms}, {:ok, acc} ->
            {:cont, {:ok, [entry | acc]}}

          _other, _acc ->
            {:halt, :fallback}
        end)
        |> case do
          {:ok, entries} -> {:ok, Enum.reverse(entries)}
          :fallback -> :fallback
        end
      end

      defp flow_claim_fast_index_entries(
             _state,
             [{:native_claim, _next, _entry, _key, _value, _prev, _history_entry} | _] = plans
           ) do
        Enum.reduce_while(plans, {:ok, []}, fn
          {:native_claim, _next, entry, _state_key, _value, _previous_history_ms, _history_entry},
          {:ok, acc} ->
            {:cont, {:ok, [entry | acc]}}

          _other, _acc ->
            {:halt, :fallback}
        end)
        |> case do
          {:ok, entries} -> {:ok, Enum.reverse(entries)}
          :fallback -> :fallback
        end
      end

      defp flow_claim_fast_index_entries(_state, plans) do
        flow_claim_fast_index_entries_loop(plans, [], nil, nil, nil, nil, nil, nil)
      end

      defp flow_claim_fast_index_entries_loop(
             [],
             entries,
             _from_due_cache,
             _to_due_cache,
             _from_state_cache,
             _to_state_cache,
             _inflight_cache,
             _worker_cache
           ),
           do: {:ok, entries}

      defp flow_claim_fast_index_entries_loop(
             [plan | rest],
             entries,
             from_due_cache,
             to_due_cache,
             from_state_cache,
             to_state_cache,
             inflight_cache,
             worker_cache
           ) do
        case flow_claim_fast_index_entry(
               plan,
               from_due_cache,
               to_due_cache,
               from_state_cache,
               to_state_cache,
               inflight_cache,
               worker_cache
             ) do
          {:ok, entry, from_due_cache, to_due_cache, from_state_cache, to_state_cache,
           inflight_cache, worker_cache} ->
            flow_claim_fast_index_entries_loop(
              rest,
              [entry | entries],
              from_due_cache,
              to_due_cache,
              from_state_cache,
              to_state_cache,
              inflight_cache,
              worker_cache
            )

          :fallback ->
            :fallback
        end
      end

      defp flow_claim_fast_index_entry(
             {record, next, from_due_score},
             from_due_cache,
             to_due_cache,
             from_state_cache,
             to_state_cache,
             inflight_cache,
             worker_cache
           )
           when is_map(record) and is_map(next) do
        flow_claim_fast_index_entry_from_records(
          record,
          next,
          flow_claim_numeric_score(from_due_score),
          from_due_cache,
          to_due_cache,
          from_state_cache,
          to_state_cache,
          inflight_cache,
          worker_cache
        )
      end

      defp flow_claim_fast_index_entry(
             {record, next, _history_meta, _attrs},
             from_due_cache,
             to_due_cache,
             from_state_cache,
             to_state_cache,
             inflight_cache,
             worker_cache
           )
           when is_map(record) and is_map(next) do
        flow_claim_fast_index_entry_from_records(
          record,
          next,
          flow_claim_numeric_score(Map.get(record, :next_run_at_ms)),
          from_due_cache,
          to_due_cache,
          from_state_cache,
          to_state_cache,
          inflight_cache,
          worker_cache
        )
      end

      defp flow_claim_fast_index_entry(
             {record, next},
             from_due_cache,
             to_due_cache,
             from_state_cache,
             to_state_cache,
             inflight_cache,
             worker_cache
           )
           when is_map(record) and is_map(next) do
        flow_claim_fast_index_entry_from_records(
          record,
          next,
          flow_claim_numeric_score(Map.get(record, :next_run_at_ms)),
          from_due_cache,
          to_due_cache,
          from_state_cache,
          to_state_cache,
          inflight_cache,
          worker_cache
        )
      end

      defp flow_claim_fast_index_entry(
             _plan,
             _from_due_cache,
             _to_due_cache,
             _from_state_cache,
             _to_state_cache,
             _inflight_cache,
             _worker_cache
           ),
           do: :fallback

      defp flow_claim_fast_index_entry_from_records(
             record,
             next,
             from_due_score_result,
             from_due_cache,
             to_due_cache,
             from_state_cache,
             to_state_cache,
             inflight_cache,
             worker_cache
           ) do
        if flow_claim_fast_index_record_shape?(record, next) do
          id = next.id
          record_partition_key = Map.get(record, :partition_key)
          partition_key = Map.get(next, :partition_key)

          {from_due_key, from_due_cache} =
            flow_claim_cached_due_index_key(from_due_cache, record)

          {to_due_key, to_due_cache} =
            flow_claim_cached_due_index_key(to_due_cache, next)

          {from_state_key, from_state_cache} =
            flow_claim_cached_state_index_key(
              from_state_cache,
              record.type,
              record.state,
              record_partition_key
            )

          {to_state_key, to_state_cache} =
            flow_claim_cached_state_index_key(
              to_state_cache,
              next.type,
              next.state,
              partition_key
            )

          {inflight_key, inflight_cache} =
            flow_claim_cached_inflight_index_key(inflight_cache, next.type, partition_key)

          {worker_key, worker_cache} =
            flow_claim_cached_worker_index_key(
              worker_cache,
              Map.get(next, :lease_owner, ""),
              partition_key
            )

          with true <- is_binary(from_due_key) and is_binary(to_due_key),
               {:ok, from_due_score} <- from_due_score_result,
               {:ok, from_state_score} <- flow_claim_record_state_score(record) do
            lease_score = Map.get(next, :lease_deadline_ms, 0) * 1.0

            entry =
              {id, from_due_key, from_due_score * 1.0, to_due_key,
               Map.fetch!(next, :next_run_at_ms) * 1.0, from_state_key, from_state_score * 1.0,
               to_state_key, Map.get(next, :updated_at_ms, 0) * 1.0, inflight_key, worker_key,
               lease_score}

            {:ok, entry, from_due_cache, to_due_cache, from_state_cache, to_state_cache,
             inflight_cache, worker_cache}
          else
            _ -> :fallback
          end
        else
          :fallback
        end
      end

      defp flow_claim_cached_due_index_key(cache, %{next_run_at_ms: nil}), do: {nil, cache}

      defp flow_claim_cached_due_index_key(
             cache,
             %{type: type, state: flow_state, priority: priority} = record
           ) do
        partition_key = Map.get(record, :partition_key)
        cache_key = {type, flow_state, priority, partition_key}

        case cache do
          {^cache_key, key} ->
            {key, cache}

          _ ->
            key = FlowKeys.due_key(type, flow_state, priority, partition_key)
            {key, {cache_key, key}}
        end
      end

      defp flow_claim_cached_state_index_key(cache, type, flow_state, partition_key) do
        cache_key = {type, flow_state, partition_key}

        case cache do
          {^cache_key, key} ->
            {key, cache}

          _ ->
            key = FlowKeys.state_index_key(type, flow_state, partition_key)
            {key, {cache_key, key}}
        end
      end

      defp flow_claim_cached_inflight_index_key(cache, type, partition_key) do
        cache_key = {type, partition_key}

        case cache do
          {^cache_key, key} ->
            {key, cache}

          _ ->
            key = FlowKeys.inflight_index_key(type, partition_key)
            {key, {cache_key, key}}
        end
      end

      defp flow_claim_cached_worker_index_key(cache, worker, partition_key) do
        cache_key = {worker, partition_key}

        case cache do
          {^cache_key, key} ->
            {key, cache}

          _ ->
            key = FlowKeys.worker_index_key(worker, partition_key)
            {key, {cache_key, key}}
        end
      end

      defp flow_claim_fast_index_record_shape?(record, next) do
        Map.get(record, :state) != "running" and Map.get(next, :state) == "running" and
          flow_claim_fast_metadata_empty?(record) and flow_claim_fast_metadata_empty?(next)
      end

      defp flow_claim_fast_metadata_empty?(%{
             id: id,
             parent_flow_id: parent_flow_id,
             correlation_id: correlation_id,
             root_flow_id: root_flow_id
           }) do
        flow_blank_metadata?(parent_flow_id) and flow_blank_metadata?(correlation_id) and
          (root_flow_id == nil or root_flow_id == "" or root_flow_id == id)
      end

      defp flow_claim_fast_metadata_empty?(record) do
        id = Map.get(record, :id)

        flow_blank_metadata?(Map.get(record, :parent_flow_id)) and
          flow_blank_metadata?(Map.get(record, :correlation_id)) and
          Map.get(record, :root_flow_id) in [nil, "", id]
      end

      defp flow_claim_apply_fast_index_entries(state, entries) do
        phase_meta =
          state
          |> flow_claim_due_phase_meta()
          |> Map.merge(%{entries: length(entries)})

        case flow_native_index(state) do
          nil ->
            {:error, :flow_native_index_unavailable}

          _native ->
            flow_claim_due_internal_phase(
              :fast_index_native_due_apply,
              phase_meta,
              %{items: length(entries)},
              fn ->
                flow_native_apply_claim_entries(state, entries)
              end
            )
        end
      end

      defp flow_claim_queue_old_terminal_lmdb_deletes(state, record) do
        maybe_queue_terminal_lmdb_index_delete(state, record)

        if Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) do
          queue_lmdb_metadata_index_deletes(state, record)
        end

        :ok
      end

      defp flow_claim_due_index_plan(record, next, moves, deletes, puts) do
        {moves, deletes, puts} = flow_due_state_index_plan(record, next, {moves, deletes, puts})

        if flow_due_any_index_enabled?() do
          flow_due_any_index_plan(record, next, {moves, deletes, puts})
        else
          {moves, deletes, puts}
        end
      end

      defp flow_due_state_index_plan(record, next, {moves, deletes, puts}) do
        flow_due_index_plan(flow_due_index_key(record), flow_due_index_key(next), record, next, {
          moves,
          deletes,
          puts
        })
      end

      defp flow_due_any_index_plan(record, next, {moves, deletes, puts}) do
        flow_due_index_plan(
          flow_due_any_index_key(record),
          flow_due_any_index_key(next),
          record,
          next,
          {
            moves,
            deletes,
            puts
          }
        )
      end

      defp flow_due_index_plan(from_key, to_key, record, next, {moves, deletes, puts}) do
        cond do
          is_binary(from_key) and is_binary(to_key) ->
            {[{from_key, to_key, next.id, Map.fetch!(next, :next_run_at_ms)} | moves], deletes,
             puts}

          is_binary(from_key) ->
            {moves, [{from_key, record.id} | deletes], puts}

          is_binary(to_key) ->
            puts =
              flow_claim_add_zset_entry(puts, to_key, next.id, Map.fetch!(next, :next_run_at_ms))

            {moves, deletes, puts}

          true ->
            {moves, deletes, puts}
        end
      end

      defp flow_claim_state_index_move(record, next) do
        from_key =
          FlowKeys.state_index_key(record.type, record.state, Map.get(record, :partition_key))

        to_key = FlowKeys.state_index_key(next.type, next.state, Map.get(next, :partition_key))

        {from_key, to_key, next.id, Map.get(next, :updated_at_ms, 0)}
      end

      defp flow_claim_metadata_index_moves(record, next) do
        score = Map.get(next, :updated_at_ms, 0)

        Enum.map(flow_metadata_index_entries(record), fn {key, id, _old_score} ->
          {key, key, id, score}
        end)
      end

      defp flow_claim_old_running_index_deletes(%{state: "running"} = record, deletes) do
        partition_key = Map.get(record, :partition_key)

        [
          {FlowKeys.inflight_index_key(record.type, partition_key), record.id},
          {FlowKeys.worker_index_key(Map.get(record, :lease_owner, ""), partition_key), record.id}
          | deletes
        ]
      end

      defp flow_claim_old_running_index_deletes(_record, deletes), do: deletes

      defp flow_claim_new_running_index_puts(%{state: "running"} = next, puts) do
        partition_key = Map.get(next, :partition_key)
        lease_score = Map.get(next, :lease_deadline_ms, 0)

        puts
        |> flow_claim_add_zset_entry(
          FlowKeys.inflight_index_key(next.type, partition_key),
          next.id,
          lease_score
        )
        |> flow_claim_add_zset_entry(
          FlowKeys.worker_index_key(Map.get(next, :lease_owner, ""), partition_key),
          next.id,
          lease_score
        )
      end

      defp flow_claim_new_running_index_puts(_next, puts), do: puts

      defp flow_claim_put_grouped_zset_entries(_state, puts) when map_size(puts) == 0, do: :ok

      defp flow_claim_put_grouped_zset_entries(state, puts) do
        Enum.each(puts, fn {key, member_score_pairs} ->
          flow_zset_put_many_new(state, key, Enum.reverse(member_score_pairs))
        end)

        :ok
      end

      defp flow_transition_move_indexes(_state, []), do: :ok

      defp flow_transition_move_indexes(state, plans) do
        with :ok <- flow_transition_move_due_indexes(state, plans),
             :ok <- flow_transition_move_state_indexes(state, plans),
             :ok <- flow_transition_move_metadata_indexes(state, plans),
             :ok <- flow_transition_move_active_timeout_indexes(state, plans),
             :ok <- flow_transition_move_terminal_retention_indexes(state, plans),
             :ok <- flow_transition_delete_old_secondary_indexes(state, plans) do
          flow_transition_put_new_running_indexes(state, plans)
        end
      end

      defp flow_terminal_transition_move_indexes(state, plans) do
        with :ok <- flow_transition_move_due_indexes(state, plans),
             :ok <- flow_transition_move_state_indexes(state, plans),
             :ok <- flow_transition_move_metadata_indexes(state, plans),
             :ok <- flow_transition_move_active_timeout_indexes(state, plans),
             :ok <- flow_transition_move_terminal_retention_indexes(state, plans) do
          flow_transition_delete_old_secondary_indexes(state, plans)
        end
      end

      defp flow_transition_move_active_timeout_indexes(state, plans) do
        {moves, deletes, puts} =
          Enum.reduce(plans, {[], [], []}, fn plan, {moves, deletes, puts} ->
            {record, next} = flow_claim_plan_pair(plan)

            flow_active_timeout_index_plan(
              flow_active_timeout_index_entry(record),
              flow_active_timeout_index_entry(next),
              moves,
              deletes,
              puts
            )
          end)

        with :ok <- flow_index_move_entries(state, Enum.reverse(moves)),
             :ok <- flow_zset_index_delete_grouped(state, Enum.reverse(deletes)) do
          flow_index_put_new_entries(state, Enum.reverse(puts))
        end
      end

      defp flow_transition_move_terminal_retention_indexes(state, plans) do
        {moves, deletes, puts} =
          Enum.reduce(plans, {[], [], []}, fn plan, {moves, deletes, puts} ->
            {record, next} = flow_claim_plan_pair(plan)

            flow_active_timeout_index_plan(
              flow_terminal_retention_index_entry(record),
              flow_terminal_retention_index_entry(next),
              moves,
              deletes,
              puts
            )
          end)

        with :ok <- flow_index_move_entries(state, Enum.reverse(moves)),
             :ok <- flow_zset_index_delete_grouped(state, Enum.reverse(deletes)) do
          flow_index_put_new_entries(state, Enum.reverse(puts))
        end
      end

      defp flow_active_timeout_index_plan(nil, nil, moves, deletes, puts),
        do: {moves, deletes, puts}

      defp flow_active_timeout_index_plan(
             {key, member, old_score},
             {key, member, new_score},
             moves,
             deletes,
             puts
           ) do
        if old_score == new_score do
          {moves, deletes, puts}
        else
          {[{key, key, member, new_score} | moves], deletes, puts}
        end
      end

      defp flow_active_timeout_index_plan(
             {old_key, old_member, _old_score},
             nil,
             moves,
             deletes,
             puts
           ),
           do: {moves, [{old_key, old_member} | deletes], puts}

      defp flow_active_timeout_index_plan(
             nil,
             {new_key, new_member, new_score},
             moves,
             deletes,
             puts
           ),
           do: {moves, deletes, [{new_key, new_member, new_score} | puts]}

      defp flow_active_timeout_index_plan(
             {old_key, old_member, _old_score},
             {new_key, new_member, new_score},
             moves,
             deletes,
             puts
           ),
           do:
             {moves, [{old_key, old_member} | deletes], [{new_key, new_member, new_score} | puts]}

      defp flow_transition_move_due_indexes(state, plans) do
        if flow_transition_plans_due_index_empty?(plans) do
          :ok
        else
          flow_transition_move_due_indexes_nonempty(state, plans)
        end
      end

      defp flow_transition_plans_due_index_empty?([]), do: true

      defp flow_transition_plans_due_index_empty?([plan | rest]) do
        {record, next} = flow_claim_plan_pair(plan)

        is_nil(Map.get(record, :next_run_at_ms)) and is_nil(Map.get(next, :next_run_at_ms)) and
          flow_transition_plans_due_index_empty?(rest)
      end

      defp flow_transition_move_due_indexes_nonempty(state, plans) do
        {moves, deletes, puts, _from_due_cache, _to_due_cache, _from_any_cache, _to_any_cache} =
          Enum.reduce(plans, {[], [], %{}, nil, nil, nil, nil}, fn plan,
                                                                   {moves, deletes, puts,
                                                                    from_due_cache, to_due_cache,
                                                                    from_any_cache, to_any_cache} ->
            {record, next} = flow_claim_plan_pair(plan)

            {from_due_key, from_due_cache} =
              flow_claim_cached_due_index_key(from_due_cache, record)

            {to_due_key, to_due_cache} = flow_claim_cached_due_index_key(to_due_cache, next)

            {moves, deletes, puts} =
              flow_due_index_plan(from_due_key, to_due_key, record, next, {moves, deletes, puts})

            if flow_due_any_index_enabled?() do
              {from_any_key, from_any_cache} =
                flow_claim_cached_due_any_index_key(from_any_cache, record)

              {to_any_key, to_any_cache} = flow_claim_cached_due_any_index_key(to_any_cache, next)

              {moves, deletes, puts} =
                flow_due_index_plan(
                  from_any_key,
                  to_any_key,
                  record,
                  next,
                  {moves, deletes, puts}
                )

              {moves, deletes, puts, from_due_cache, to_due_cache, from_any_cache, to_any_cache}
            else
              {moves, deletes, puts, from_due_cache, to_due_cache, from_any_cache, to_any_cache}
            end
          end)

        with :ok <- flow_index_move_lifecycle_entries(state, Enum.reverse(moves)),
             :ok <- flow_zset_lifecycle_index_delete_grouped(state, deletes) do
          puts
          |> Enum.each(fn {key, member_score_pairs} ->
            flow_index_put_new_lifecycle_members(state, key, Enum.reverse(member_score_pairs))
          end)

          :ok
        end
      end

      defp flow_claim_cached_due_any_index_key(cache, %{next_run_at_ms: nil}), do: {nil, cache}

      defp flow_claim_cached_due_any_index_key(cache, %{type: type, priority: priority} = record) do
        partition_key = Map.get(record, :partition_key)
        cache_key = {type, priority, partition_key}

        case cache do
          {^cache_key, key} ->
            {key, cache}

          _ ->
            key = FlowKeys.due_any_key(type, priority, partition_key)
            {key, {cache_key, key}}
        end
      end

      defp flow_due_index_key(%{next_run_at_ms: nil}), do: nil

      defp flow_due_index_key(%{type: type, state: flow_state, priority: priority} = record) do
        FlowKeys.due_key(type, flow_state, priority, Map.get(record, :partition_key))
      end

      defp flow_due_any_index_key(%{next_run_at_ms: nil}), do: nil

      defp flow_due_any_index_key(%{type: type, priority: priority} = record) do
        FlowKeys.due_any_key(type, priority, Map.get(record, :partition_key))
      end

      defp flow_transition_move_state_indexes(state, [plan]) do
        {record, next} = flow_claim_plan_pair(plan)

        from_key =
          FlowKeys.state_index_key(record.type, record.state, Map.get(record, :partition_key))

        to_key = FlowKeys.state_index_key(next.type, next.state, Map.get(next, :partition_key))

        flow_index_move_lifecycle_entries(
          state,
          [{from_key, to_key, next.id, Map.get(next, :updated_at_ms, 0)}]
        )
      end

      defp flow_transition_move_state_indexes(state, plans) do
        {moves, _from_cache, _to_cache} =
          Enum.reduce(plans, {[], nil, nil}, fn plan, {moves, from_cache, to_cache} ->
            {record, next} = flow_claim_plan_pair(plan)
            record_partition_key = Map.get(record, :partition_key)
            next_partition_key = Map.get(next, :partition_key)

            {from_key, from_cache} =
              flow_claim_cached_state_index_key(
                from_cache,
                record.type,
                record.state,
                record_partition_key
              )

            {to_key, to_cache} =
              flow_claim_cached_state_index_key(
                to_cache,
                next.type,
                next.state,
                next_partition_key
              )

            {
              [{from_key, to_key, next.id, Map.get(next, :updated_at_ms, 0)} | moves],
              from_cache,
              to_cache
            }
          end)

        flow_index_move_lifecycle_entries(state, Enum.reverse(moves))
      end

      defp flow_transition_move_metadata_indexes(state, plans) do
        if flow_transition_plans_metadata_index_empty?(plans) do
          :ok
        else
          flow_transition_move_metadata_indexes_nonempty(state, plans)
        end
      end

      defp flow_transition_plans_metadata_index_empty?([]), do: true

      defp flow_transition_plans_metadata_index_empty?([plan | rest]) do
        {record, next} = flow_claim_plan_pair(plan)

        flow_metadata_index_record_empty?(record) and flow_metadata_index_record_empty?(next) and
          flow_transition_plans_metadata_index_empty?(rest)
      end

      defp flow_transition_move_metadata_indexes_nonempty(state, [plan]) do
        {record, next} = flow_claim_plan_pair(plan)

        case flow_transition_metadata_index_plan(record, next, [], [], []) do
          {[], [], []} ->
            :ok

          {moves, deletes, puts} ->
            with :ok <- flow_index_move_entries(state, Enum.reverse(moves)),
                 :ok <- flow_zset_index_delete_grouped(state, deletes) do
              flow_index_put_new_entries(state, Enum.reverse(puts))
            end
        end
      end

      defp flow_transition_move_metadata_indexes_nonempty(state, plans) do
        {moves, deletes, puts} =
          Enum.reduce(plans, {[], [], []}, fn plan, {moves, deletes, puts} ->
            {record, next} = flow_claim_plan_pair(plan)
            flow_transition_metadata_index_plan(record, next, moves, deletes, puts)
          end)

        case {moves, deletes, puts} do
          {[], [], []} ->
            :ok

          _ ->
            with :ok <- flow_index_move_entries(state, Enum.reverse(moves)),
                 :ok <- flow_zset_index_delete_grouped(state, deletes) do
              flow_index_put_new_entries(state, Enum.reverse(puts))
            end
        end
      end

      defp flow_transition_metadata_index_plan(record, next, moves, deletes, puts) do
        if flow_metadata_index_record_empty?(record) and flow_metadata_index_record_empty?(next) do
          {moves, deletes, puts}
        else
          old_entries_list = flow_metadata_index_entries(record)
          new_entries_list = flow_metadata_index_entries(next)

          case {old_entries_list, new_entries_list} do
            {[], []} ->
              {moves, deletes, puts}

            _ ->
              old_entries =
                Map.new(old_entries_list, fn {key, id, score} ->
                  {key, {id, score}}
                end)

              new_entries =
                Map.new(new_entries_list, fn {key, id, score} -> {key, {id, score}} end)

              moves =
                Enum.reduce(new_entries, moves, fn {key, {id, score}}, acc ->
                  if Map.has_key?(old_entries, key) do
                    [{key, key, id, score} | acc]
                  else
                    acc
                  end
                end)

              deletes =
                Enum.reduce(old_entries, deletes, fn {key, {id, _score}}, acc ->
                  if Map.has_key?(new_entries, key), do: acc, else: [{key, id} | acc]
                end)

              puts =
                Enum.reduce(new_entries, puts, fn {key, {id, score}}, acc ->
                  if Map.has_key?(old_entries, key), do: acc, else: [{key, id, score} | acc]
                end)

              {moves, deletes, puts}
          end
        end
      end

      defp flow_metadata_index_record_empty?(%{
             id: id,
             parent_flow_id: parent_flow_id,
             root_flow_id: root_flow_id,
             correlation_id: correlation_id
           }) do
        flow_metadata_index_empty?(parent_flow_id, root_flow_id, correlation_id, id)
      end

      defp flow_metadata_index_record_empty?(record) do
        id = Map.get(record, :id)

        flow_metadata_index_empty?(
          Map.get(record, :parent_flow_id),
          Map.get(record, :root_flow_id),
          Map.get(record, :correlation_id),
          id
        )
      end

      defp flow_transition_delete_old_secondary_indexes(state, [plan]) do
        {record, _next} = flow_claim_plan_pair(plan)

        cond do
          Map.get(record, :state) == "running" ->
            partition_key = Map.get(record, :partition_key)

            flow_zset_lifecycle_index_delete_grouped(state, [
              {FlowKeys.inflight_index_key(record.type, partition_key), record.id},
              {FlowKeys.worker_index_key(Map.get(record, :lease_owner, ""), partition_key),
               record.id}
            ])

          Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) ->
            maybe_queue_terminal_lmdb_index_delete(state, record)
            queue_lmdb_metadata_index_deletes(state, record)

          true ->
            :ok
        end

        :ok
      end

      defp flow_transition_delete_old_secondary_indexes(state, plans) do
        {terminal_records, running_deletes, _inflight_cache, _worker_cache} =
          Enum.reduce(plans, {[], [], nil, nil}, fn plan,
                                                    {terminal_records, running_deletes,
                                                     inflight_cache, worker_cache} ->
            {record, _next} = flow_claim_plan_pair(plan)

            if Map.get(record, :state) == "running" do
              partition_key = Map.get(record, :partition_key)
              worker = Map.get(record, :lease_owner, "")

              {inflight_key, inflight_cache} =
                flow_claim_cached_inflight_index_key(inflight_cache, record.type, partition_key)

              {worker_key, worker_cache} =
                flow_claim_cached_worker_index_key(worker_cache, worker, partition_key)

              running_deletes = [
                {inflight_key, record.id},
                {worker_key, record.id}
                | running_deletes
              ]

              {terminal_records, running_deletes, inflight_cache, worker_cache}
            else
              if Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) do
                {[record | terminal_records], running_deletes, inflight_cache, worker_cache}
              else
                {terminal_records, running_deletes, inflight_cache, worker_cache}
              end
            end
          end)

        Enum.each(terminal_records, fn record ->
          maybe_queue_terminal_lmdb_index_delete(state, record)
          queue_lmdb_metadata_index_deletes(state, record)
        end)

        flow_zset_lifecycle_index_delete_grouped(state, running_deletes)
      end
    end
  end
end
