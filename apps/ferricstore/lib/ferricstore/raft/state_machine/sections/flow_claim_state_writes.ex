defmodule Ferricstore.Raft.StateMachine.Sections.FlowClaimStateWrites do
  @moduledoc false

  import Kernel, except: [apply: 3]

  defmacro __using__(_opts) do
    quote do
      import Kernel, except: [apply: 3]
      import Bitwise

      require Logger

      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.{CommandTime, Flow, HLC}
      alias Ferricstore.Commands.Dispatcher
      alias Ferricstore.Commands.HyperLogLog
      alias Ferricstore.Raft.BlobCommand
      alias Ferricstore.Flow.Hibernation
      alias Ferricstore.Flow.HistoryProjector
      alias Ferricstore.Flow.Locator
      alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
      alias Ferricstore.Flow.Keys, as: FlowKeys
      alias Ferricstore.Flow.RetryPolicy

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

      defp flow_transition_put_new_running_indexes(state, [plan]) do
        {_record, next} = flow_claim_plan_pair(plan)

        if Map.get(next, :state) == "running" do
          flow_claim_put_running_indexes(state, [plan])
        else
          :ok
        end
      end

      defp flow_transition_put_new_running_indexes(state, plans) do
        plans
        |> Enum.filter(fn plan ->
          {_record, next} = flow_claim_plan_pair(plan)
          Map.get(next, :state) == "running"
        end)
        |> then(&flow_claim_put_running_indexes(state, &1))
      end

      defp flow_claim_put_running_indexes(state, plans) do
        plans
        |> Enum.reduce(%{}, fn plan, acc ->
          {_record, next} = flow_claim_plan_pair(plan)
          partition_key = Map.get(next, :partition_key)
          lease_score = Map.get(next, :lease_deadline_ms, 0)

          acc
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
        end)
        |> Enum.each(fn {key, member_score_pairs} ->
          flow_zset_put_many_new(state, key, Enum.reverse(member_score_pairs))
        end)

        :ok
      end

      defp flow_claim_put_state_records(state, plans) do
        case flow_claim_put_native_state_records_batch(state, plans) do
          :ok ->
            :ok

          :fallback ->
            case flow_claim_put_state_records_batch(state, plans) do
              :ok ->
                :ok

              :fallback ->
                flow_claim_put_state_records_loop(state, plans, nil)
                :ok
            end
        end
      end

      defp flow_claim_put_native_state_records_batch(
             state,
             [{:native_claim, _next, _entry, _state_key, _next_value, _previous_history_ms} | _] =
               plans
           ) do
        cond do
          cross_shard_pending_active?() ->
            :fallback

          standalone_staged_apply?() ->
            :fallback

          true ->
            with {:ok, staged_entries} <-
                   flow_claim_stage_native_state_record_entries(plans, []) do
              original_originals = Process.get(:sm_pending_originals, %{})
              original_pending_writes = Process.get(:sm_pending_writes, [])
              original_pending_values = Process.get(:sm_pending_values, %{})

              {entries, pending_writes, pending_values, originals} =
                Enum.reduce(
                  staged_entries,
                  {[], original_pending_writes, original_pending_values, original_originals},
                  fn {state_key, next, next_value, disk_val, entry},
                     {entries, pending_writes, pending_values, originals} ->
                    previous = safe_ets_lookup(state.ets, state_key)

                    updated =
                      record_pending_original_from_previous(state_key, previous, originals)

                    track_keydir_binary_delta_from_previous(
                      state,
                      state_key,
                      previous,
                      next_value,
                      0
                    )

                    maybe_queue_lmdb_policy_put(state_key, disk_val, 0)

                    if flow_record_has_indexed_attributes?(next) do
                      maybe_queue_lmdb_indexes_for_state_record(
                        state,
                        state_key,
                        next_value,
                        0,
                        next
                      )
                    end

                    {
                      [entry | entries],
                      [{:put, state_key, disk_val, 0} | pending_writes],
                      Map.put(pending_values, state_key, {disk_val, 0}),
                      updated
                    }
                  end
                )

              if originals != original_originals do
                Process.put(:sm_pending_originals, originals)
              end

              Process.put(:sm_pending_writes, pending_writes)
              Process.put(:sm_pending_values, pending_values)

              Process.put(:sm_pending_fast_staged_put_batch, true)
              safe_ets_insert(state.ets, Enum.reverse(entries))
              :ok
            end
        end
      end

      defp flow_claim_put_native_state_records_batch(
             state,
             [
               {:native_claim, _next, _entry, _state_key, _next_value, _previous_history_ms,
                _history_entry}
               | _
             ] = plans
           ) do
        cond do
          cross_shard_pending_active?() ->
            :fallback

          standalone_staged_apply?() ->
            :fallback

          true ->
            with {:ok, staged_entries} <-
                   flow_claim_stage_native_state_record_entries(plans, []) do
              original_originals = Process.get(:sm_pending_originals, %{})
              original_pending_writes = Process.get(:sm_pending_writes, [])
              original_pending_values = Process.get(:sm_pending_values, %{})

              {entries, pending_writes, pending_values, originals} =
                Enum.reduce(
                  staged_entries,
                  {[], original_pending_writes, original_pending_values, original_originals},
                  fn {state_key, next, next_value, disk_val, entry},
                     {entries, pending_writes, pending_values, originals} ->
                    previous = safe_ets_lookup(state.ets, state_key)

                    updated =
                      record_pending_original_from_previous(state_key, previous, originals)

                    track_keydir_binary_delta_from_previous(
                      state,
                      state_key,
                      previous,
                      next_value,
                      0
                    )

                    maybe_queue_lmdb_policy_put(state_key, disk_val, 0)

                    if flow_record_has_indexed_attributes?(next) do
                      maybe_queue_lmdb_indexes_for_state_record(
                        state,
                        state_key,
                        next_value,
                        0,
                        next
                      )
                    end

                    {
                      [entry | entries],
                      [{:put, state_key, disk_val, 0} | pending_writes],
                      Map.put(pending_values, state_key, {disk_val, 0}),
                      updated
                    }
                  end
                )

              if originals != original_originals do
                Process.put(:sm_pending_originals, originals)
              end

              Process.put(:sm_pending_writes, pending_writes)
              Process.put(:sm_pending_values, pending_values)

              Process.put(:sm_pending_fast_staged_put_batch, true)
              safe_ets_insert(state.ets, Enum.reverse(entries))
              :ok
            end
        end
      end

      defp flow_claim_put_native_state_records_batch(_state, _plans), do: :fallback

      defp flow_claim_stage_native_state_record_entries([], acc),
        do: {:ok, Enum.reverse(acc)}

      defp flow_claim_stage_native_state_record_entries(
             [
               {:native_claim, next, _entry, state_key, next_value, _previous_history_ms}
               | rest
             ],
             acc
           ) do
        disk_val = to_disk_binary(next_value)
        entry = {state_key, next_value, 0, LFU.initial(), :pending, 0, byte_size(disk_val)}

        flow_claim_stage_native_state_record_entries(rest, [
          {state_key, next, next_value, disk_val, entry} | acc
        ])
      end

      defp flow_claim_stage_native_state_record_entries(
             [
               {:native_claim, next, _entry, state_key, next_value, _previous_history_ms,
                _history_entry}
               | rest
             ],
             acc
           ) do
        disk_val = to_disk_binary(next_value)
        entry = {state_key, next_value, 0, LFU.initial(), :pending, 0, byte_size(disk_val)}

        flow_claim_stage_native_state_record_entries(rest, [
          {state_key, next, next_value, disk_val, entry} | acc
        ])
      end

      defp flow_claim_stage_native_state_record_entries(_plans, _acc),
        do: :fallback

      defp flow_claim_put_state_records_batch(state, plans) do
        case flow_claim_state_record_key_records(plans) do
          {:ok, key_records} -> flow_put_state_records_batch(state, key_records)
          :fallback -> :fallback
        end
      end

      defp flow_claim_state_record_key_records(plans) do
        plans
        |> Enum.reduce_while({:ok, [], nil}, fn
          {:native_claim, _next, _entry, _state_key, _next_value, _previous_history_ms}, _acc ->
            {:halt, :fallback}

          {:native_claim, _next, _entry, _state_key, _next_value, _previous_history_ms,
           _history_entry},
          _acc ->
            {:halt, :fallback}

          {_record, next, _from_due_score}, {:ok, acc, cache} ->
            {key, cache} = flow_state_record_key(cache, next)
            {:cont, {:ok, [{key, next} | acc], cache}}

          {_record, next, _history_meta, _attrs}, {:ok, acc, cache} ->
            {key, cache} = flow_state_record_key(cache, next)
            {:cont, {:ok, [{key, next} | acc], cache}}

          {_record, next}, {:ok, acc, cache} ->
            {key, cache} = flow_state_record_key(cache, next)
            {:cont, {:ok, [{key, next} | acc], cache}}

          _other, _acc ->
            {:halt, :fallback}
        end)
        |> case do
          {:ok, key_records, _cache} -> {:ok, Enum.reverse(key_records)}
          :fallback -> :fallback
        end
      end

      defp flow_claim_put_state_records_loop(_state, [], _cache), do: :ok

      defp flow_claim_put_state_records_loop(
             state,
             [{:native_claim, next, _entry, state_key, next_value, _previous_history_ms} | rest],
             cache
           ) do
        with :ok <- flow_put_state_record_encoded(state, state_key, next_value, 0, next) do
          flow_claim_put_state_records_loop(state, rest, cache)
        end
      end

      defp flow_claim_put_state_records_loop(
             state,
             [
               {:native_claim, next, _entry, state_key, next_value, _previous_history_ms,
                _history_entry}
               | rest
             ],
             cache
           ) do
        with :ok <- flow_put_state_record_encoded(state, state_key, next_value, 0, next) do
          flow_claim_put_state_records_loop(state, rest, cache)
        end
      end

      defp flow_claim_put_state_records_loop(
             state,
             [{_record, next, _from_due_score} | rest],
             cache
           ) do
        {key, cache} = flow_state_record_key(cache, next)

        with :ok <- flow_put_state_record(state, key, next) do
          flow_claim_put_state_records_loop(state, rest, cache)
        end
      end

      defp flow_claim_put_state_records_loop(
             state,
             [{_record, next, _history_meta, _attrs} | rest],
             cache
           ) do
        {key, cache} = flow_state_record_key(cache, next)

        with :ok <- flow_put_state_record(state, key, next) do
          flow_claim_put_state_records_loop(state, rest, cache)
        end
      end

      defp flow_claim_put_state_records_loop(state, [{_record, next} | rest], cache) do
        {key, cache} = flow_state_record_key(cache, next)

        with :ok <- flow_put_state_record(state, key, next) do
          flow_claim_put_state_records_loop(state, rest, cache)
        end
      end

      defp flow_state_record_key(cache, %{id: id} = record) do
        partition_key = Map.get(record, :partition_key)

        case cache do
          {^partition_key, prefix} when is_binary(prefix) ->
            {prefix <> id, cache}

          _ ->
            prefix = FlowKeys.state_key("", partition_key)
            {prefix <> id, {partition_key, prefix}}
        end
      end

      defp flow_state_key_with_tag(tag, id), do: "f:" <> tag <> ":s:" <> id
      defp flow_registry_key_with_tag(tag, id), do: "f:" <> tag <> ":r:" <> id
      defp flow_history_key_with_tag(tag, id), do: "f:" <> tag <> ":h:" <> id

      defp flow_due_key_with_tag(tag, type, flow_state, priority) do
        "f:" <>
          tag <>
          ":d:" <>
          FlowKeys.index_component(type) <>
          ":" <>
          FlowKeys.index_component(flow_state) <> ":p" <> Integer.to_string(priority)
      end

      defp flow_due_any_key_with_tag(tag, type, priority) do
        "f:" <>
          tag <>
          ":da:" <> FlowKeys.index_component(type) <> ":p" <> Integer.to_string(priority)
      end

      defp flow_state_index_key_with_tag(tag, type, flow_state) do
        "f:" <>
          tag <>
          ":i:s:" <> FlowKeys.index_component(type) <> ":" <> FlowKeys.index_component(flow_state)
      end

      defp flow_inflight_index_key_with_tag(tag, type), do: "f:" <> tag <> ":i:r:" <> type
      defp flow_worker_index_key_with_tag(tag, worker), do: "f:" <> tag <> ":i:w:" <> worker
      defp flow_parent_index_key_with_tag(tag, parent_id), do: "f:" <> tag <> ":i:p:" <> parent_id
      defp flow_root_index_key_with_tag(tag, root_id), do: "f:" <> tag <> ":i:o:" <> root_id

      defp flow_correlation_index_key_with_tag(tag, correlation_id),
        do: "f:" <> tag <> ":i:c:" <> correlation_id

      defp flow_create_put_state_records(state, records) do
        {key_records, _cache} =
          Enum.map_reduce(records, nil, fn record, cache ->
            {key, cache} = flow_state_record_key(cache, record)
            {{key, record}, cache}
          end)

        case flow_put_state_records_batch(state, key_records) do
          :ok ->
            :ok

          :fallback ->
            Enum.reduce_while(key_records, :ok, fn {key, record}, :ok ->
              case flow_put_new_state_record(state, key, record) do
                :ok -> {:cont, :ok}
                {:error, _reason} = error -> {:halt, error}
              end
            end)

          {:error, _reason} = error ->
            error
        end
      end

      defp flow_create_put_fast_state_records(state, plans) do
        key_records = Enum.map(plans, fn %{state_key: key, record: record} -> {key, record} end)

        case flow_put_new_state_records_batch(state, key_records) do
          :ok ->
            :ok

          :fallback ->
            Enum.reduce_while(key_records, :ok, fn {key, record}, :ok ->
              case flow_put_new_state_record(state, key, record) do
                :ok -> {:cont, :ok}
                {:error, _reason} = error -> {:halt, error}
              end
            end)

          {:error, _reason} = error ->
            error
        end
      end

      defp flow_put_new_state_records_batch(_state, []), do: :ok

      defp flow_put_new_state_records_batch(state, key_records) do
        cond do
          cross_shard_pending_active?() ->
            :fallback

          standalone_staged_apply?() ->
            :fallback

          true ->
            lagged_projection? = Ferricstore.Flow.LMDB.mode() == :lagged
            originals = Process.get(:sm_pending_originals, %{})
            pending_values = Process.get(:sm_pending_values, %{})

            case Enum.reduce_while(
                   key_records,
                   {:ok, [], [], originals, pending_values, false},
                   fn {key, record}, {:ok, entries, writes, originals, pending_values, dirty?} ->
                     value = flow_encode(record)
                     expire_at_ms = flow_state_record_expire_at(record)
                     originals = Map.put_new(originals, key, :missing)

                     case maybe_externalize_apply_value(state, value) do
                       {:ok, :value, stored_value} ->
                         flow_stage_new_state_record_batch_entry(
                           state,
                           key,
                           record,
                           value,
                           stored_value,
                           stored_value,
                           expire_at_ms,
                           entries,
                           writes,
                           originals,
                           pending_values,
                           lagged_projection?,
                           dirty?
                         )

                       {:ok, :blob_ref, stored_value, pending_value} ->
                         flow_stage_new_state_record_batch_entry(
                           state,
                           key,
                           record,
                           value,
                           stored_value,
                           pending_value,
                           expire_at_ms,
                           entries,
                           writes,
                           originals,
                           pending_values,
                           lagged_projection?,
                           dirty?
                         )

                       {:error, _reason} = error ->
                         {:halt, error}
                     end
                   end
                 ) do
              {:ok, entries, writes, originals, pending_values, dirty?} ->
                if dirty?, do: queue_pending_lmdb_projection_dirty()

                Process.put(:sm_pending_originals, originals)
                Process.put(:sm_pending_values, pending_values)
                Process.put(:sm_pending_writes, writes ++ Process.get(:sm_pending_writes, []))
                Process.put(:sm_pending_fast_staged_put_batch, true)

                safe_ets_insert(state.ets, entries)

                with :ok <- flow_track_state_retention_metadata_batch(state, key_records) do
                  flow_enqueue_governance_release_intents(state, key_records)
                end

              {:error, _reason} = error ->
                error
            end
        end
      end

      defp flow_put_state_records_batch(_state, []), do: :ok

      defp flow_put_state_records_batch(state, key_records) do
        cond do
          cross_shard_pending_active?() ->
            :fallback

          standalone_staged_apply?() ->
            :fallback

          true ->
            lagged_projection? = Ferricstore.Flow.LMDB.mode() == :lagged
            originals = Process.get(:sm_pending_originals, %{})
            pending_values = Process.get(:sm_pending_values, %{})

            case Enum.reduce_while(
                   key_records,
                   {:ok, [], [], originals, pending_values, false},
                   fn {key, record}, {:ok, entries, writes, originals, pending_values, dirty?} ->
                     value = flow_encode(record)
                     expire_at_ms = flow_state_record_expire_at(record)
                     previous = safe_ets_lookup(state.ets, key)
                     originals = record_pending_original_from_previous(key, previous, originals)

                     case maybe_externalize_apply_value(state, value) do
                       {:ok, :value, stored_value} ->
                         flow_stage_state_record_batch_entry(
                           state,
                           key,
                           record,
                           value,
                           stored_value,
                           stored_value,
                           expire_at_ms,
                           previous,
                           entries,
                           writes,
                           originals,
                           pending_values,
                           lagged_projection?,
                           dirty?
                         )

                       {:ok, :blob_ref, stored_value, pending_value} ->
                         flow_stage_state_record_batch_entry(
                           state,
                           key,
                           record,
                           value,
                           stored_value,
                           pending_value,
                           expire_at_ms,
                           previous,
                           entries,
                           writes,
                           originals,
                           pending_values,
                           lagged_projection?,
                           dirty?
                         )

                       {:error, _reason} = error ->
                         {:halt, error}
                     end
                   end
                 ) do
              {:ok, entries, writes, originals, pending_values, dirty?} ->
                if dirty?, do: queue_pending_lmdb_projection_dirty()

                Process.put(:sm_pending_originals, originals)
                Process.put(:sm_pending_values, pending_values)
                Process.put(:sm_pending_writes, writes ++ Process.get(:sm_pending_writes, []))
                Process.put(:sm_pending_fast_staged_put_batch, true)

                safe_ets_insert(state.ets, entries)

                with :ok <- flow_track_state_retention_metadata_batch(state, key_records) do
                  flow_enqueue_governance_release_intents(state, key_records)
                end

              {:error, _reason} = error ->
                error
            end
        end
      end

      defp flow_stage_state_record_batch_entry(
             state,
             key,
             record,
             encoded_record,
             stored_value,
             pending_value,
             expire_at_ms,
             previous,
             entries,
             writes,
             originals,
             pending_values,
             lagged_projection?,
             dirty?
           ) do
        terminal? = Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state))
        disk_val = to_disk_binary(stored_value)
        blob_ref? = stored_value != encoded_record

        if flow_record_has_indexed_attributes?(record) do
          maybe_queue_lmdb_indexes_for_state_record(
            state,
            key,
            encoded_record,
            expire_at_ms,
            record
          )
        end

        dirty? = dirty? or (lagged_projection? and terminal?)

        {ets_value, lfu, write} =
          cond do
            blob_ref? ->
              lfu =
                if terminal?,
                  do: flow_record_lfu(record, encoded_record),
                  else: LFU.initial()

              {nil, lfu, {:put_cold, key, disk_val, expire_at_ms, lfu}}

            true ->
              maybe_queue_flow_hibernation_candidate(state, key, record, encoded_record)
              maybe_queue_lmdb_policy_put(key, disk_val, expire_at_ms)
              {encoded_record, LFU.initial(), {:put, key, disk_val, expire_at_ms}}
          end

        track_keydir_binary_delta_from_previous(
          state,
          key,
          previous,
          ets_value,
          expire_at_ms
        )

        entry = {key, ets_value, expire_at_ms, lfu, :pending, 0, byte_size(disk_val)}
        pending_values = Map.put(pending_values, key, {pending_value, expire_at_ms})

        {:cont, {:ok, [entry | entries], [write | writes], originals, pending_values, dirty?}}
      end

      defp flow_stage_new_state_record_batch_entry(
             state,
             key,
             record,
             encoded_record,
             stored_value,
             pending_value,
             expire_at_ms,
             entries,
             writes,
             originals,
             pending_values,
             lagged_projection?,
             dirty?
           ) do
        terminal? = Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state))
        disk_val = to_disk_binary(stored_value)
        blob_ref? = stored_value != encoded_record
        lfu = LFU.initial()

        if flow_record_has_indexed_attributes?(record) do
          maybe_queue_lmdb_indexes_for_state_record(
            state,
            key,
            encoded_record,
            expire_at_ms,
            record
          )
        end

        dirty? = dirty? or (lagged_projection? and terminal?)

        {ets_value, lfu, write} =
          cond do
            blob_ref? ->
              lfu =
                if terminal?,
                  do: flow_record_lfu(record, encoded_record),
                  else: lfu

              {nil, lfu, {:put_cold, key, disk_val, expire_at_ms, lfu}}

            true ->
              maybe_queue_flow_hibernation_candidate(state, key, record, encoded_record)
              maybe_queue_lmdb_policy_put(key, disk_val, expire_at_ms)
              {encoded_record, lfu, {:put, key, disk_val, expire_at_ms}}
          end

        track_keydir_binary_delta_from_missing(state, key, ets_value, expire_at_ms)

        entry = {key, ets_value, expire_at_ms, lfu, :pending, 0, byte_size(disk_val)}
        pending_values = Map.put(pending_values, key, {pending_value, expire_at_ms})

        {:cont, {:ok, [entry | entries], [write | writes], originals, pending_values, dirty?}}
      end

      defp flow_index_put_many_new(state, records) do
        records
        |> flow_index_grouped_entries()
        |> Enum.each(fn {key, member_score_pairs} ->
          flow_zset_put_many_new(state, key, Enum.reverse(member_score_pairs))
        end)

        secondary_entries =
          Enum.flat_map(records, fn record ->
            flow_metadata_index_entries(record) ++
              flow_active_timeout_index_entries(record) ++
              flow_terminal_retention_index_entries(record)
          end)

        flow_index_put_new_entries(state, secondary_entries)

        :ok
      end

      defp flow_create_put_fast_indexes(state, plans) do
        lifecycle_groups =
          Enum.reduce(plans, %{}, fn %{record: record} = plan, acc ->
            acc =
              flow_claim_add_zset_entry(
                acc,
                plan.state_index_key,
                record.id,
                plan.state_index_score
              )

            acc =
              acc
              |> flow_create_fast_due_entry(plan.due_key, record)
              |> flow_create_fast_due_entry(plan.due_any_key, record)

            Enum.reduce(plan.running_index_entries, acc, fn {key, score}, inner_acc ->
              flow_claim_add_zset_entry(inner_acc, key, record.id, score)
            end)
          end)

        with :ok <- flow_put_lifecycle_index_groups(lifecycle_groups, state) do
          plans
          |> Enum.flat_map(fn %{record: record, metadata_index_entries: entries} ->
            entries ++
              flow_active_timeout_index_entries(record) ++
              flow_terminal_retention_index_entries(record)
          end)
          |> then(&flow_index_put_new_entries(state, &1))
        end
      end

      defp flow_create_fast_due_entry(acc, nil, _record), do: acc

      defp flow_create_fast_due_entry(acc, key, %{id: id, next_run_at_ms: score}) do
        flow_claim_add_zset_entry(acc, key, id, score)
      end

      defp flow_put_lifecycle_index_groups(groups, state) do
        Enum.each(groups, fn {key, member_score_pairs} ->
          flow_index_put_new_lifecycle_members(state, key, Enum.reverse(member_score_pairs))
        end)

        :ok
      end

      defp flow_index_grouped_entries(records) do
        records
        |> Enum.reduce(%{}, fn record, acc ->
          partition_key = Map.get(record, :partition_key)
          updated_score = Map.get(record, :updated_at_ms, 0)

          acc =
            flow_claim_add_zset_entry(
              acc,
              FlowKeys.state_index_key(record.type, record.state, partition_key),
              record.id,
              updated_score
            )

          acc =
            if Map.get(record, :state) == "running" do
              lease_score = Map.get(record, :lease_deadline_ms, 0)

              acc
              |> flow_claim_add_zset_entry(
                FlowKeys.inflight_index_key(record.type, partition_key),
                record.id,
                lease_score
              )
              |> flow_claim_add_zset_entry(
                FlowKeys.worker_index_key(Map.get(record, :lease_owner, ""), partition_key),
                record.id,
                lease_score
              )
            else
              acc
            end

          acc
        end)
      end

      defp flow_claim_add_zset_entry(acc, key, member, score) do
        Map.update(acc, key, [{member, score}], &[{member, score} | &1])
      end

      defp flow_claim_put_history(state, plans, now_ms) do
        flow_with_forced_async_history(fn ->
          flow_claim_put_history_batch(state, plans, now_ms)
        end)
      end

      defp flow_claim_put_history_batch(_state, [], _now_ms), do: :ok

      defp flow_claim_put_history_batch(state, plans, now_ms) do
        if flow_async_history_enabled?(state) do
          {projection_entries, after_history_records} =
            flow_claim_async_history_entries(state, plans, now_ms, [], [])

          with :ok <- queue_pending_flow_history_projections_batch(projection_entries) do
            flow_claim_after_history_put_records_batch(state, after_history_records)
          end
        else
          history_entries =
            Enum.map(plans, fn
              {:native_claim, next, _entry, _state_key, _value, previous_history_ms} ->
                flow_history_put_ready_entry(
                  state,
                  next,
                  "claimed",
                  now_ms,
                  previous_history_ms
                )

              plan ->
                {record, next} = flow_claim_plan_pair(plan)

                flow_history_put_ready_entry(
                  state,
                  next,
                  "claimed",
                  now_ms,
                  flow_previous_history_ms(record)
                )
            end)

          with :ok <- flow_history_index_put_entries(state, history_entries) do
            flow_claim_after_history_put_batch(state, plans)
          end
        end
      end

      defp flow_claim_async_history_entries(_state, [], _now_ms, entries, after_history_records) do
        {Enum.reverse(entries), Enum.reverse(after_history_records)}
      end

      defp flow_claim_async_history_entries(
             state,
             [{:native_claim, next, _entry, _state_key, _value, previous_history_ms} | rest],
             now_ms,
             entries,
             after_history_records
           ) do
        entry = flow_claim_async_history_entry(state, next, now_ms, previous_history_ms)
        after_history_records = flow_claim_after_history_record_acc(next, after_history_records)

        flow_claim_async_history_entries(
          state,
          rest,
          now_ms,
          [entry | entries],
          after_history_records
        )
      end

      defp flow_claim_async_history_entries(
             state,
             [
               {:native_claim, next, _entry, _state_key, _value, previous_history_ms, entry}
               | rest
             ],
             now_ms,
             entries,
             after_history_records
           ) do
        entry =
          flow_history_maybe_put_hot_evict_event_ids(
            entry,
            flow_history_hot_evict_event_ids(
              next,
              Map.fetch!(entry, :event_id),
              Map.fetch!(entry, :version),
              previous_history_ms
            )
          )
          |> Map.put(:shard_index, state.shard_index)

        after_history_records = flow_claim_after_history_record_acc(next, after_history_records)

        flow_claim_async_history_entries(
          state,
          rest,
          now_ms,
          [entry | entries],
          after_history_records
        )
      end

      defp flow_claim_async_history_entries(
             state,
             [plan | rest],
             now_ms,
             entries,
             after_history_records
           ) do
        {record, next} = flow_claim_plan_pair(plan)

        entry =
          flow_claim_async_history_entry(state, next, now_ms, flow_previous_history_ms(record))

        after_history_records = flow_claim_after_history_record_acc(next, after_history_records)

        flow_claim_async_history_entries(
          state,
          rest,
          now_ms,
          [entry | entries],
          after_history_records
        )
      end

      defp flow_claim_after_history_record_acc(record, acc) do
        if flow_claim_after_history_fast_record?(record), do: acc, else: [record | acc]
      end

      defp flow_claim_async_history_entry(
             state,
             %{id: id} = record,
             now_ms,
             previous_history_ms
           ) do
        partition_key = Map.get(record, :partition_key)
        history_key = FlowKeys.history_key(id, partition_key)

        flow_history_projection_entry(
          state,
          record,
          history_key,
          "claimed",
          now_ms,
          previous_history_ms,
          %{}
        )
      end
    end
  end
end
