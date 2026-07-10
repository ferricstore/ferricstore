defmodule Ferricstore.Raft.StateMachine.Sections.FlowTransition do
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

      defp do_flow_transition(
             state,
             %{id: id, from_state: from_state, to_state: to_state} = attrs
           ) do
        now_ms = flow_attrs_now_ms(attrs)
        run_at_ms = Map.get(attrs, :run_at_ms, now_ms)
        partition_key = Map.get(attrs, :partition_key)

        case flow_prepare_transition_record(
               state,
               attrs,
               id,
               from_state,
               to_state,
               run_at_ms,
               now_ms
             ) do
          {:ok, :noop} ->
            :ok

          {:ok, record, next} ->
            flow_apply_transition(state, record, next, partition_key, now_ms, attrs)

          {:error, _reason} = error ->
            error
        end
      end

      defp do_flow_reschedule(
             state,
             %{id: id, lease_token: lease_token, state: logical_state, run_at_ms: run_at_ms} =
               attrs
           ) do
        now_ms = flow_attrs_now_ms(attrs)
        partition_key = Map.get(attrs, :partition_key)

        with {:ok, record} <- flow_require_record(state, id, partition_key),
             {:ok, record, next} <-
               flow_prepare_reschedule_existing_record(
                 record,
                 attrs,
                 lease_token,
                 logical_state,
                 run_at_ms,
                 now_ms
               ),
             next = flow_stamp_state_enter_seq_on_change(state, record, next),
             next = flow_refresh_indexed_attributes(state, next),
             :ok <- flow_require_fifo_entry(state, attrs, next, true) do
          flow_apply_reschedule(state, record, next, partition_key, now_ms, attrs)
        end
      end

      defp do_flow_schedule_replace(
             state,
             %{id: id, type: expected_type, state: logical_state, run_at_ms: run_at_ms} = attrs
           ) do
        now_ms = flow_attrs_now_ms(attrs)
        partition_key = Map.get(attrs, :partition_key)

        with {:ok, record} <- flow_require_record(state, id, partition_key),
             :ok <- flow_require_schedule_replaceable(record, expected_type, now_ms),
             {:ok, record, next} <-
               flow_prepare_schedule_replace_existing_record(
                 record,
                 attrs,
                 logical_state,
                 run_at_ms,
                 now_ms
               ),
             next = flow_stamp_state_enter_seq_on_change(state, record, next),
             next = flow_refresh_indexed_attributes(state, next),
             :ok <- flow_require_fifo_entry(state, attrs, next, true) do
          flow_apply_transition(state, record, next, partition_key, now_ms, attrs)
        end
      end

      defp do_flow_step_continue(
             state,
             %{
               id: id,
               lease_token: lease_token,
               from_state: from_state,
               to_state: to_state,
               lease_ms: lease_ms
             } = attrs
           ) do
        now_ms = flow_attrs_now_ms(attrs)
        partition_key = Map.get(attrs, :partition_key)

        with {:ok, record} <- flow_require_record(state, id, partition_key),
             {:ok, record, next} <-
               flow_prepare_step_continue_existing_record(
                 record,
                 attrs,
                 lease_token,
                 from_state,
                 to_state,
                 lease_ms,
                 now_ms
               ),
             next = flow_stamp_state_enter_seq_on_change(state, record, next),
             next = flow_refresh_indexed_attributes(state, next),
             :ok <- flow_require_fifo_entry(state, attrs, next, true),
             :ok <- flow_apply_step_continue(state, record, next, partition_key, now_ms, attrs) do
          {:ok, next}
        end
      end

      defp do_flow_step_continue_many(state, %{records: [_ | _] = records} = attrs) do
        records
        |> flow_expand_shared_attrs(Map.get(attrs, :shared))
        |> Enum.map(&do_flow_step_continue(state, &1))
      end

      defp do_flow_step_continue_many(_state, _attrs),
        do: {:error, "ERR flow step_continue_many requires records"}

      defp do_flow_run_steps_many(state, %{records: [_ | _] = records} = attrs) do
        stamped_shard = Map.get(attrs, @flow_shard_marker)

        with :ok <-
               Ferricstore.LatencyTrace.span("server_flow_run_steps_partition_validate_us", fn ->
                 flow_many_partitions_valid?(state, records, stamped_shard)
               end),
             :ok <-
               Ferricstore.LatencyTrace.span("server_flow_run_steps_unique_validate_us", fn ->
                 flow_create_many_unique?(records)
               end),
             key_infos =
               Ferricstore.LatencyTrace.span("server_flow_run_steps_key_infos_us", fn ->
                 flow_create_fast_key_infos(records, stamped_shard)
               end),
             :ok <-
               Ferricstore.LatencyTrace.span("server_flow_run_steps_shard_validate_us", fn ->
                 flow_many_same_state_machine_shard_by_keys?(state, key_infos)
               end),
             {:ok, plans} <-
               Ferricstore.LatencyTrace.span("server_flow_run_steps_prepare_us", fn ->
                 flow_run_steps_prepare_direct_many(state, records, key_infos)
               end),
             :ok <-
               Ferricstore.LatencyTrace.span("server_flow_run_steps_apply_us", fn ->
                 flow_run_steps_apply_direct_many(state, plans)
               end) do
          :ok
        end
      end

      defp do_flow_run_steps_many(_state, _attrs),
        do: {:error, "ERR flow run_steps_many requires records"}

      defp flow_run_steps_prepare_direct_many(state, attrs_list, key_infos) do
        keys = Enum.map(key_infos, & &1.state_key)
        registry_keys = Enum.map(key_infos, & &1.registry_key)

        if Enum.any?(flow_registry_keys_present_hot_only(state, registry_keys), & &1) or
             Enum.any?(flow_state_keys_present_hot_only(state, keys), & &1) do
          {:error, "ERR flow already exists"}
        else
          attrs_list
          |> Enum.zip(key_infos)
          |> Enum.reduce_while({:ok, []}, fn {attrs, key_info}, {:ok, acc} ->
            case flow_run_steps_prepare_direct(state, attrs, key_info) do
              {:ok, plan} -> {:cont, {:ok, [plan | acc]}}
              {:error, _reason} = error -> {:halt, error}
            end
          end)
          |> case do
            {:ok, plans} -> {:ok, Enum.reverse(plans)}
            {:error, _reason} = error -> error
          end
        end
      end

      defp flow_run_steps_prepare_direct(
             state,
             %{step_states: [_ | _] = step_states} = attrs,
             key_info
           ) do
        with {:ok, created, final} <- flow_run_steps_build_records(state, attrs, step_states),
             plan = flow_create_fast_plan(final, attrs, key_info),
             :ok <- flow_validate_create_fast_plan_keys(plan) do
          {:ok, Map.merge(plan, %{created: created, step_states: step_states})}
        end
      end

      defp flow_run_steps_prepare_direct(_state, _attrs, _key_info),
        do: {:error, "ERR flow run_steps_many states must be non-empty"}

      defp flow_run_steps_apply_direct_many(state, plans) do
        with :ok <- flow_many_same_state_machine_shard_by_keys?(state, plans),
             :ok <- flow_create_fast_put_record_values(state, plans),
             :ok <- flow_create_put_fast_state_records(state, plans),
             :ok <- flow_create_put_fast_registry_markers(state, plans),
             :ok <- flow_create_put_fast_indexes(state, plans),
             :ok <- flow_run_steps_put_histories(state, plans),
             :ok <- flow_run_steps_after_history_put(state, plans) do
          :ok
        end
      end

      defp flow_run_steps_build_records(state, attrs, step_states) do
        now_ms = Map.fetch!(attrs, :step_now_ms)
        step_count = Map.get(attrs, :step_count) || length(step_states)
        final_version = step_count + 1
        partition_key = Map.fetch!(attrs, :partition_key)
        id = Map.fetch!(attrs, :id)
        final_run_state = Map.get(attrs, :final_run_state) || List.last(step_states)

        created =
          state
          |> flow_create_record(attrs)
          |> flow_start_and_claim_record(attrs)

        final =
          %{
            created
            | state: "completed",
              version: final_version,
              updated_at_ms: now_ms + step_count,
              result_ref: flow_value_ref(attrs, :result, id, final_version, partition_key),
              ttl_ms: nil,
              retention_ttl_ms: Map.get(attrs, :ttl_ms) || Map.get(created, :retention_ttl_ms),
              terminal_retention_until_ms: nil,
              lease_owner: nil,
              lease_token: nil,
              lease_deadline_ms: 0,
              next_run_at_ms: nil,
              run_state: final_run_state,
              fencing_token: step_count
          }
          |> flow_stamp_terminal_retention(now_ms + step_count)

        {:ok, created, final}
      end

      defp flow_run_steps_put_histories(state, plans) do
        entries =
          Enum.map(plans, fn %{created: created, record: final, step_states: step_states} ->
            flow_run_steps_history_projection_chain(state, created, final, step_states)
          end)

        queue_pending_flow_history_projections_batch(entries)
      end

      defp flow_run_steps_after_history_put(state, plans) do
        lmdb_mirror? = flow_lmdb_projection_enabled?(state)

        if Enum.all?(plans, fn %{record: final} ->
             flow_after_history_fast_record?(lmdb_mirror?, final)
           end) do
          :ok
        else
          plans
          |> Enum.map(fn %{record: final} -> final end)
          |> flow_after_history_put_many(state)
        end
      end

      defp flow_run_steps_history_projection_chain(_state, created, final, step_states) do
        history_key =
          FlowKeys.history_key(Map.fetch!(created, :id), Map.get(created, :partition_key))

        %{
          history_key: history_key,
          history_chain: {created, final, step_states}
        }
      end

      defp do_flow_transition_many(state, %{records: [_ | _] = records} = attrs) do
        attrs_list = flow_expand_shared_attrs(records, Map.get(attrs, :shared))
        stamped_shard = Map.get(attrs, @flow_shard_marker)

        if Map.get(attrs, :independent) == true do
          do_flow_transition_many_independent(state, attrs_list, stamped_shard)
        else
          with :ok <- flow_many_partitions_valid?(state, attrs_list, stamped_shard),
               :ok <- flow_transition_many_unique?(attrs_list),
               {:ok, plans, value_mode} <- flow_transition_many_prepare(state, attrs_list),
               :ok <- flow_transition_many_apply(state, plans, value_mode) do
            :ok
          end
        end
      end

      defp do_flow_transition_many(_state, _attrs),
        do: {:error, "ERR flow items must be a non-empty list"}

      defp flow_expand_shared_attrs(records, shared)
           when is_map(shared) and map_size(shared) > 0 do
        Enum.map(records, &Map.merge(shared, &1))
      end

      defp flow_expand_shared_attrs(records, _shared), do: records

      defp do_flow_transition_many_independent(state, attrs_list, stamped_shard) do
        case flow_many_same_state_machine_shard?(state, attrs_list, stamped_shard) do
          :ok ->
            Enum.map(attrs_list, fn attrs ->
              case do_flow_transition(state, attrs) do
                :ok -> :ok
                {:error, _reason} = error -> error
                other -> other
              end
            end)

          {:error, _reason} = error ->
            error
        end
      end

      defp flow_prepare_transition_record(
             state,
             attrs,
             id,
             from_state,
             to_state,
             run_at_ms,
             now_ms
           ) do
        partition_key = Map.get(attrs, :partition_key)

        with {:ok, record} <- flow_require_record(state, id, partition_key),
             {:ok, record, next} <-
               flow_prepare_transition_existing_record(
                 record,
                 attrs,
                 id,
                 from_state,
                 to_state,
                 run_at_ms,
                 now_ms
               ),
             next = flow_stamp_state_enter_seq_on_change(state, record, next),
             next = flow_refresh_indexed_attributes(state, next),
             :ok <- flow_require_fifo_entry(state, attrs, next, true) do
          {:ok, record, next}
        end
      end

      defp flow_prepare_transition_existing_record(
             nil,
             _attrs,
             _id,
             _from_state,
             _to_state,
             _run_at_ms,
             _now_ms
           ),
           do: {:error, "ERR flow not found"}

      defp flow_prepare_transition_existing_record(
             record,
             attrs,
             _id,
             from_state,
             to_state,
             run_at_ms,
             now_ms
           ) do
        if flow_duplicate_transition_noop?(record, attrs, to_state) do
          {:ok, :noop}
        else
          with id when is_binary(id) <- Map.get(record, :id),
               :ok <- flow_require_expected_state(record, from_state),
               :ok <- flow_reject_terminal_current(record),
               :ok <- flow_require_fencing_token(record, Map.fetch!(attrs, :fencing_token)),
               :ok <- flow_require_transition_lease(record, Map.get(attrs, :lease_token)),
               :ok <- flow_reject_running_transition(to_state),
               :ok <- flow_reject_terminal_transition(to_state) do
            version = Map.fetch!(record, :version) + 1
            partition_key = Map.get(record, :partition_key)

            with {:ok, value_refs} <-
                   flow_named_value_refs(record, attrs, id, version, partition_key) do
              next =
                %{
                  record
                  | state: to_state,
                    version: version,
                    updated_at_ms: now_ms,
                    next_run_at_ms: run_at_ms,
                    priority: Map.get(attrs, :priority) || Map.get(record, :priority, 0),
                    payload_ref:
                      flow_value_ref(
                        attrs,
                        :payload,
                        id,
                        version,
                        partition_key,
                        Map.get(record, :payload_ref)
                      ),
                    ttl_ms: nil,
                    retention_ttl_ms: Map.get(record, :retention_ttl_ms),
                    history_hot_max_events: Map.get(record, :history_hot_max_events),
                    history_max_events: Map.get(record, :history_max_events),
                    lease_owner: nil,
                    lease_token: nil,
                    lease_deadline_ms: 0
                }
                |> flow_put_record_value_refs(value_refs)
                |> flow_apply_attribute_updates(attrs)
                |> flow_stamp_terminal_retention(now_ms)

              with :ok <- flow_validate_record_keys(next) do
                {:ok, record, next}
              end
            end
          else
            {:error, _reason} = error -> error
            _ -> {:error, "ERR flow not found"}
          end
        end
      end

      defp flow_prepare_step_continue_existing_record(
             record,
             attrs,
             lease_token,
             from_state,
             to_state,
             lease_ms,
             now_ms
           ) do
        with :ok <- flow_require_running_lease(record, lease_token),
             :ok <- flow_require_fencing_token(record, Map.fetch!(attrs, :fencing_token)),
             :ok <- flow_require_step_run_state(record, from_state),
             :ok <- flow_require_step_worker(record, Map.get(attrs, :worker)),
             :ok <- flow_reject_running_transition(to_state),
             :ok <- flow_reject_terminal_transition(to_state) do
          version = Map.fetch!(record, :version) + 1
          next_fencing_token = Map.get(record, :fencing_token, 0) + 1
          id = Map.fetch!(record, :id)
          partition_key = Map.get(record, :partition_key)
          worker = Map.fetch!(record, :lease_owner)
          deadline_ms = now_ms + lease_ms

          token =
            worker <>
              ":" <>
              Integer.to_string(now_ms) <> ":" <> Integer.to_string(next_fencing_token)

          with {:ok, value_refs} <-
                 flow_named_value_refs(record, attrs, id, version, partition_key) do
            next =
              %{
                record
                | state: "running",
                  run_state: to_state,
                  version: version,
                  fencing_token: next_fencing_token,
                  updated_at_ms: now_ms,
                  payload_ref:
                    flow_value_ref(
                      attrs,
                      :payload,
                      id,
                      version,
                      partition_key,
                      Map.get(record, :payload_ref)
                    ),
                  ttl_ms: nil,
                  terminal_retention_until_ms: nil,
                  lease_owner: worker,
                  lease_token: token,
                  lease_deadline_ms: deadline_ms,
                  next_run_at_ms: deadline_ms
              }
              |> flow_put_record_value_refs(value_refs)
              |> flow_apply_attribute_updates(attrs)

            with :ok <- flow_validate_claim_next_record_keys(next) do
              {:ok, record, next}
            end
          end
        end
      end

      defp flow_require_step_run_state(record, expected_state) do
        case flow_retry_run_state(record) do
          ^expected_state -> :ok
          _other -> {:error, "ERR flow wrong state"}
        end
      end

      defp flow_require_step_worker(%{lease_owner: worker}, nil)
           when is_binary(worker) and worker != "",
           do: :ok

      defp flow_require_step_worker(%{lease_owner: worker}, worker)
           when is_binary(worker) and worker != "",
           do: :ok

      defp flow_require_step_worker(_record, _worker), do: {:error, "ERR stale flow lease"}

      defp flow_reject_terminal_transition(to_state) do
        if Ferricstore.Flow.LMDB.terminal_state?(to_state) do
          {:error, "ERR terminal flow state requires FLOW.COMPLETE, FLOW.FAIL, or FLOW.CANCEL"}
        else
          :ok
        end
      end

      defp flow_reject_terminal_current(record) do
        if Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) do
          {:error, "ERR flow is terminal; use FLOW.REWIND"}
        else
          :ok
        end
      end

      defp flow_reject_running_transition("running"),
        do: {:error, "ERR flow running state is only entered by FLOW.CLAIM_DUE"}

      defp flow_reject_running_transition(_to_state), do: :ok

      defp flow_prepare_reschedule_existing_record(
             record,
             attrs,
             lease_token,
             logical_state,
             run_at_ms,
             now_ms
           ) do
        with :ok <- flow_require_running_lease(record, lease_token),
             :ok <- flow_require_fencing_token(record, Map.fetch!(attrs, :fencing_token)),
             :ok <- flow_require_step_run_state(record, logical_state),
             :ok <- flow_reject_terminal_transition(logical_state) do
          version = Map.fetch!(record, :version) + 1
          id = Map.fetch!(record, :id)
          partition_key = Map.get(record, :partition_key)

          with {:ok, value_refs} <-
                 flow_named_value_refs(record, attrs, id, version, partition_key) do
            next =
              %{
                record
                | state: logical_state,
                  run_state: nil,
                  version: version,
                  updated_at_ms: now_ms,
                  next_run_at_ms: run_at_ms,
                  priority: Map.get(attrs, :priority) || Map.get(record, :priority, 0),
                  payload_ref:
                    flow_value_ref(
                      attrs,
                      :payload,
                      id,
                      version,
                      partition_key,
                      Map.get(record, :payload_ref)
                    ),
                  ttl_ms: nil,
                  terminal_retention_until_ms: nil,
                  lease_owner: nil,
                  lease_token: nil,
                  lease_deadline_ms: 0
              }
              |> flow_put_record_value_refs(value_refs)
              |> flow_apply_attribute_updates(attrs)

            with :ok <- flow_validate_claim_next_record_keys(next) do
              {:ok, record, next}
            end
          end
        end
      end

      defp flow_require_schedule_replaceable(nil, _expected_type, _now_ms),
        do: {:error, "ERR flow schedule not found"}

      defp flow_require_schedule_replaceable(record, expected_type, now_ms) do
        cond do
          Map.get(record, :type) != expected_type ->
            {:error, "ERR flow schedule not found"}

          flow_live_lease?(record, now_ms) ->
            {:error, "ERR flow schedule is currently leased"}

          true ->
            :ok
        end
      end

      defp flow_live_lease?(record, now_ms) do
        lease_token = Map.get(record, :lease_token)

        is_binary(lease_token) and lease_token != "" and
          Map.get(record, :lease_deadline_ms, 0) > now_ms
      end

      defp flow_prepare_schedule_replace_existing_record(
             record,
             attrs,
             logical_state,
             run_at_ms,
             now_ms
           ) do
        version = Map.fetch!(record, :version) + 1
        id = Map.fetch!(record, :id)
        partition_key = Map.get(record, :partition_key)

        with {:ok, value_refs} <-
               flow_named_value_refs(record, attrs, id, version, partition_key) do
          next =
            record
            |> Map.merge(%{
              state: logical_state,
              run_state: nil,
              version: version,
              attempts: 0,
              fencing_token: Map.get(record, :fencing_token, 0) + 1,
              updated_at_ms: now_ms,
              next_run_at_ms: run_at_ms,
              priority: Map.get(attrs, :priority) || 0,
              payload_ref: flow_value_ref(attrs, :payload, id, version, partition_key),
              value_refs: value_refs,
              ttl_ms: nil,
              terminal_retention_until_ms: nil,
              lease_owner: nil,
              lease_token: nil,
              lease_deadline_ms: 0,
              result_ref: nil,
              error_ref: nil
            })

          with :ok <- flow_validate_claim_next_record_keys(next) do
            {:ok, record, next}
          end
        end
      end

      defp flow_apply_reschedule(state, record, next, partition_key, now_ms, attrs) do
        plans = [{record, next}]

        with :ok <- flow_put_record_values(state, next, attrs),
             :ok <- flow_transition_move_indexes(state, plans),
             :ok <-
               flow_put_state_record(
                 state,
                 FlowKeys.state_key(next.id, partition_key),
                 next
               ),
             :ok <- flow_history_put_planned(state, record, next, "rescheduled", now_ms),
             :ok <- flow_after_history_put(state, next) do
          :ok
        end
      end

      defp flow_apply_transition(state, record, next, partition_key, now_ms, attrs) do
        plans = [{record, next}]

        with :ok <- flow_put_record_values(state, next, attrs),
             :ok <- flow_transition_move_indexes(state, plans),
             :ok <-
               flow_put_state_record(
                 state,
                 FlowKeys.state_key(next.id, partition_key),
                 next
               ),
             :ok <- flow_history_put_planned(state, record, next, "transitioned", now_ms),
             :ok <- flow_after_history_put(state, next) do
          :ok
        end
      end

      defp flow_apply_step_continue(state, record, next, partition_key, now_ms, attrs) do
        plans = [{record, next}]

        with :ok <- flow_put_record_values(state, next, attrs),
             :ok <- flow_transition_move_indexes(state, plans),
             :ok <-
               flow_put_state_record(
                 state,
                 FlowKeys.state_key(next.id, partition_key),
                 next
               ),
             :ok <- flow_history_put_planned(state, record, next, "step_continued", now_ms),
             :ok <- flow_after_history_put(state, next) do
          :ok
        end
      end

      defp flow_transition_many_unique?(attrs_list) do
        {_seen, result} =
          Enum.reduce_while(attrs_list, {MapSet.new(), :ok}, fn %{id: id}, {seen, :ok} ->
            if MapSet.member?(seen, id) do
              {:halt, {seen, {:error, "ERR flow duplicate id in batch"}}}
            else
              {:cont, {MapSet.put(seen, id), :ok}}
            end
          end)

        result
      end

      defp flow_transition_many_prepare(state, attrs_list) do
        existing_records = flow_read_records(state, attrs_list)

        attrs_list
        |> Enum.zip(existing_records)
        |> Enum.reduce_while({:ok, [], :empty}, fn
          {%{id: id, from_state: from_state, to_state: to_state} = attrs, existing},
          {:ok, acc, value_mode} ->
            now_ms = flow_attrs_now_ms(attrs)
            run_at_ms = Map.get(attrs, :run_at_ms, now_ms)

            case flow_prepare_transition_existing_record(
                   existing,
                   attrs,
                   id,
                   from_state,
                   to_state,
                   run_at_ms,
                   now_ms
                 ) do
              {:ok, record, next} ->
                next = flow_stamp_state_enter_seq_on_change(state, record, next)
                next = flow_refresh_indexed_attributes(state, next)

                case flow_require_fifo_entry(state, attrs, next, true) do
                  :ok ->
                    {:cont,
                     {:ok, [{record, next, attrs} | acc],
                      flow_merge_record_value_mode(
                        value_mode,
                        flow_attrs_record_value_mode(attrs)
                      )}}

                  {:error, _reason} = error ->
                    {:halt, error}
                end

              {:ok, :noop} ->
                {:cont, {:ok, acc, value_mode}}

              {:error, _reason} = error ->
                {:halt, error}
            end

          {_bad, _existing}, {:ok, _acc, _value_mode} ->
            {:halt, {:error, "ERR flow id must be a non-empty string"}}
        end)
        |> case do
          {:ok, plans, value_mode} ->
            {:ok, Enum.reverse(plans), flow_finalize_record_value_mode(value_mode)}

          {:error, _reason} = error ->
            error
        end
      end

      defp flow_transition_many_apply(state, plans, value_mode) do
        with :ok <- flow_transition_many_put_record_values(state, plans, value_mode),
             :ok <- flow_transition_move_indexes(state, plans),
             :ok <- flow_claim_put_state_records(state, plans),
             :ok <- flow_transition_put_history(state, plans) do
          :ok
        end
      end

      defp flow_transition_many_put_record_values(_state, _plans, :none), do: :ok

      defp flow_transition_many_put_record_values(state, plans, :payload_only) do
        flow_transition_many_put_payloads(state, plans)
      end

      defp flow_transition_many_put_record_values(state, plans, :mixed) do
        flow_many_put_record_values(state, plans, true)
      end

      defp flow_transition_many_put_record_values(state, plans, :unknown) do
        if flow_transition_many_payload_only?(plans) do
          flow_transition_many_put_payloads(state, plans)
        else
          flow_many_put_record_values(state, plans)
        end
      end

      defp flow_transition_many_payload_only?(plans) do
        Enum.all?(plans, fn
          {_record, _next, attrs} ->
            Map.has_key?(attrs, :payload) and not Map.has_key?(attrs, :result) and
              not Map.has_key?(attrs, :error) and
              map_size(flow_named_values(Map.get(attrs, :values))) == 0
        end)
      end

      defp flow_transition_many_put_payloads(state, plans) do
        case flow_transition_many_shared_payload(plans) do
          {:blob_ref, encoded_ref} ->
            flow_transition_many_put_blob_payload_refs(state, plans, encoded_ref)

          {:value, encoded_value} ->
            flow_transition_many_put_encoded_payloads(state, plans, encoded_value)

          :mixed ->
            flow_transition_many_put_payloads_per_record(state, plans)
        end
      end

      defp flow_transition_many_shared_payload([
             {_record, _next, %{payload: first_payload}} | rest
           ]) do
        if flow_transition_many_same_payload?(rest, first_payload) do
          case BlobCommand.flow_blob_value_ref(first_payload) do
            {:ok, encoded_ref} -> {:blob_ref, encoded_ref}
            :error -> {:value, Flow.encode_value(first_payload)}
          end
        else
          :mixed
        end
      end

      defp flow_transition_many_shared_payload(_plans), do: :mixed

      defp flow_transition_many_same_payload?([], _payload), do: true

      defp flow_transition_many_same_payload?(
             [{_record, _next, %{payload: next_payload}} | rest],
             payload
           )
           when next_payload == payload do
        flow_transition_many_same_payload?(rest, payload)
      end

      defp flow_transition_many_same_payload?(_plans, _payload), do: false

      defp flow_transition_many_put_blob_payload_refs(state, plans, encoded_ref) do
        Enum.reduce_while(plans, :ok, fn {_record, next, _attrs}, :ok ->
          key = Map.fetch!(next, :payload_ref)

          case flow_put_record_blob_value(state, next, key, encoded_ref) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp flow_transition_many_put_encoded_payloads(state, plans, encoded_value) do
        Enum.reduce_while(plans, :ok, fn {_record, next, _attrs}, :ok ->
          key = Map.fetch!(next, :payload_ref)

          case flow_put_record_encoded_payload_value(state, next, key, encoded_value) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp flow_transition_many_put_payloads_per_record(state, plans) do
        Enum.reduce_while(plans, :ok, fn {_record, next, %{payload: payload}}, :ok ->
          key = Map.fetch!(next, :payload_ref)

          case flow_put_record_payload_value(state, next, key, payload) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp flow_put_record_encoded_payload_value(state, record, key, encoded_value) do
        with :ok <- flow_validate_key_size(key) do
          raw_put_cold(state, key, encoded_value, flow_record_expire_at(record))
        end
      end

      defp flow_put_record_payload_value(state, record, key, payload) do
        case BlobCommand.flow_blob_value_ref(payload) do
          {:ok, encoded_ref} ->
            flow_put_record_blob_value(state, record, key, encoded_ref)

          :error ->
            with :ok <- flow_validate_key_size(key) do
              raw_put_cold(
                state,
                key,
                Flow.encode_value(payload),
                flow_record_expire_at(record)
              )
            end
        end
      end

      defp flow_put_record_blob_value(state, record, key, encoded_ref) do
        with :ok <- flow_validate_key_size(key),
             {:ok, _ref} <- decode_blob_ref(encoded_ref) do
          raw_put_flow_blob_ref(state, key, encoded_ref, flow_record_expire_at(record))
        end
      end

      defp do_flow_retry(state, %{id: id, lease_token: lease_token} = attrs) do
        now_ms = flow_attrs_now_ms(attrs)
        partition_key = Map.get(attrs, :partition_key)

        with {:ok, record} <- flow_require_record(state, id, partition_key) do
          case flow_prepare_retry_existing_record(state, record, attrs, lease_token, now_ms) do
            {:ok, :noop} ->
              :ok

            {:ok, record, next, history_meta} ->
              case flow_apply_retry(
                     state,
                     record,
                     next,
                     partition_key,
                     now_ms,
                     history_meta,
                     attrs
                   ) do
                :ok -> flow_governance_release_result(record)
                {:error, _reason} = error -> error
              end

            {:error, _reason} = error ->
              error
          end
        end
      end

      defp do_flow_retry_many(state, %{records: [_ | _] = attrs_list} = attrs) do
        with :ok <-
               flow_many_partitions_valid?(state, attrs_list, Map.get(attrs, @flow_shard_marker)),
             :ok <- flow_transition_many_unique?(attrs_list),
             {:ok, plans, has_record_values?, has_after_terminal?} <-
               flow_retry_many_prepare(state, attrs_list),
             :ok <- flow_retry_many_apply(state, plans, has_record_values?, has_after_terminal?) do
          flow_governance_release_results(plans)
        end
      end

      defp do_flow_retry_many(_state, _attrs),
        do: {:error, "ERR flow items must be a non-empty list"}

      defp flow_prepare_retry_existing_record(state, record, attrs, lease_token, now_ms) do
        if flow_duplicate_retry_noop?(state, record, attrs) do
          {:ok, :noop}
        else
          with :ok <- flow_require_running_lease(record, lease_token),
               :ok <- flow_require_fencing_token(record, Map.fetch!(attrs, :fencing_token)) do
            retry_policy = flow_retry_policy_for_record(state, record, attrs)
            next_attempts = Map.get(record, :attempts, 0) + 1
            version = Map.fetch!(record, :version) + 1
            id = Map.fetch!(record, :id)
            partition_key = Map.get(record, :partition_key)

            {next_state, next_run_at_ms, retry_decision} =
              flow_retry_next_state(record, attrs, retry_policy, next_attempts, now_ms)

            payload_ref =
              flow_value_ref(
                attrs,
                :payload,
                id,
                version,
                partition_key,
                Map.get(record, :payload_ref)
              )

            error_ref = flow_value_ref(attrs, :error, id, version, partition_key)

            with {:ok, value_refs} <-
                   flow_named_value_refs(record, attrs, id, version, partition_key) do
              next =
                %{
                  record
                  | state: next_state,
                    version: version,
                    attempts: next_attempts,
                    updated_at_ms: now_ms,
                    next_run_at_ms: next_run_at_ms,
                    payload_ref: payload_ref,
                    error_ref: error_ref,
                    ttl_ms: nil,
                    retention_ttl_ms: Map.get(record, :retention_ttl_ms),
                    lease_owner: nil,
                    lease_token: nil,
                    lease_deadline_ms: 0,
                    run_state: nil
                }
                |> flow_put_record_value_refs(value_refs)
                |> flow_apply_attribute_updates(attrs)
                |> flow_stamp_terminal_retention(now_ms)

              next = flow_stamp_state_enter_seq_on_change(state, record, next)
              next = flow_refresh_indexed_attributes(state, next)

              with :ok <- flow_require_fifo_entry(state, attrs, next, false),
                   :ok <- flow_validate_claim_next_record_keys(next) do
                {:ok, record, next,
                 flow_retry_history_meta(record, next, retry_policy, retry_decision)}
              end
            end
          end
        end
      end

      defp flow_retry_next_state(record, attrs, retry_policy, next_attempts, now_ms) do
        if RetryPolicy.attempt_allowed?(retry_policy, next_attempts) do
          run_at_ms =
            Map.get(attrs, :run_at_ms) ||
              RetryPolicy.next_run_at_ms(
                retry_policy,
                Map.fetch!(record, :id),
                next_attempts,
                now_ms
              )

          {flow_retry_run_state(record), run_at_ms, "scheduled"}
        else
          exhausted_to = Map.fetch!(retry_policy, :exhausted_to)

          next_run_at_ms =
            if Ferricstore.Flow.LMDB.terminal_state?(exhausted_to), do: nil, else: now_ms

          {exhausted_to, next_run_at_ms, "exhausted"}
        end
      end

      defp flow_retry_history_meta(record, next, retry_policy, retry_decision) do
        backoff = Map.fetch!(retry_policy, :backoff)

        %{
          "retry_decision" => retry_decision,
          "retry_run_state" => flow_retry_run_state(record),
          "retry_next_run_at_ms" => Map.get(next, :next_run_at_ms),
          "retry_max_retries" => Map.get(retry_policy, :max_retries),
          "retry_backoff_kind" => Map.get(backoff, :kind),
          "retry_backoff_base_ms" => Map.get(backoff, :base_ms),
          "retry_backoff_max_ms" => Map.get(backoff, :max_ms),
          "retry_jitter_pct" => Map.get(backoff, :jitter_pct),
          "retry_exhausted_to" => Map.get(retry_policy, :exhausted_to)
        }
      end

      defp flow_retry_policy_for_record(state, record, attrs) do
        run_state = flow_retry_run_state(record)
        flow_policy = flow_read_policy(state, Map.get(record, :type))
        RetryPolicy.resolve(flow_policy, run_state, Map.get(attrs, :retry_policy))
      end

      defp flow_retry_run_state(record) do
        case Map.get(record, :run_state) do
          state when is_binary(state) and state != "" -> state
          _ -> "queued"
        end
      end

      defp flow_apply_retry(state, record, next, partition_key, now_ms, history_meta, attrs) do
        plans = [{record, next}]

        with :ok <- flow_put_record_values(state, next, attrs),
             :ok <- flow_transition_move_indexes(state, plans),
             :ok <-
               flow_put_state_record(
                 state,
                 FlowKeys.state_key(next.id, partition_key),
                 next
               ),
             :ok <- flow_history_put_planned(state, record, next, "retry", now_ms, history_meta),
             :ok <- flow_after_history_put(state, next),
             :ok <- flow_maybe_after_retry_terminal(state, next, now_ms) do
          :ok
        end
      end

      defp flow_retry_many_prepare(state, attrs_list) do
        existing_records = flow_read_records(state, attrs_list)

        attrs_list
        |> Enum.zip(existing_records)
        |> Enum.reduce_while({:ok, [], false, false}, fn
          {%{id: _id, lease_token: lease_token} = attrs, existing},
          {:ok, acc, has_values?, has_after_terminal?} ->
            now_ms = flow_attrs_now_ms(attrs)

            case existing do
              nil ->
                {:halt, {:error, "ERR flow not found"}}

              record ->
                case flow_prepare_retry_existing_record(state, record, attrs, lease_token, now_ms) do
                  {:ok, record, next, history_meta} ->
                    {:cont,
                     {:ok, [{record, next, history_meta, attrs} | acc],
                      has_values? or flow_attrs_have_record_values?(attrs),
                      has_after_terminal? or flow_terminal_after_required?(:retry, next)}}

                  {:ok, :noop} ->
                    {:cont, {:ok, acc, has_values?, has_after_terminal?}}

                  {:error, _reason} = error ->
                    {:halt, error}
                end
            end

          {_bad, _existing}, {:ok, _acc, _has_values?, _has_after_terminal?} ->
            {:halt, {:error, "ERR flow id must be a non-empty string"}}
        end)
        |> case do
          {:ok, plans, has_record_values?, has_after_terminal?} ->
            {:ok, Enum.reverse(plans), has_record_values?, has_after_terminal?}

          {:error, _reason} = error ->
            error
        end
      end

      defp flow_retry_many_apply(state, plans, has_record_values?, has_after_terminal?) do
        with :ok <- flow_many_put_record_values(state, plans, has_record_values?),
             :ok <- flow_transition_move_indexes(state, plans),
             :ok <- flow_claim_put_state_records(state, plans),
             :ok <- flow_retry_many_put_history(state, plans),
             :ok <- flow_many_after_retry_terminal(state, plans, has_after_terminal?) do
          :ok
        end
      end

      defp flow_maybe_after_retry_terminal(state, next, now_ms) do
        status = Map.get(next, :state)

        if Ferricstore.Flow.LMDB.terminal_state?(status) do
          with :ok <- flow_maybe_cancel_children_on_parent_closed(state, next, now_ms) do
            flow_maybe_apply_child_terminal(state, next, status, now_ms)
          end
        else
          :ok
        end
      end

      defp flow_many_after_retry_terminal(_state, _plans, false), do: :ok

      defp flow_many_after_retry_terminal(state, plans, true) do
        flow_many_after_retry_terminal(state, plans)
      end

      defp flow_many_after_retry_terminal(state, plans) do
        Enum.reduce_while(plans, :ok, fn plan, :ok ->
          {_record, next} = flow_claim_plan_pair(plan)
          now_ms = flow_record_updated_at_ms(next)

          case flow_maybe_after_retry_terminal(state, next, now_ms) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp do_flow_fail(state, %{id: id, lease_token: lease_token} = attrs) do
        now_ms = flow_attrs_now_ms(attrs)
        partition_key = Map.get(attrs, :partition_key)

        with {:ok, record} <- flow_require_record(state, id, partition_key) do
          case flow_prepare_fail_existing_record(record, attrs, lease_token, now_ms) do
            {:ok, :noop} ->
              :ok

            {:ok, record, next} ->
              next = flow_refresh_indexed_attributes(state, next)

              case flow_apply_fail(state, record, next, partition_key, now_ms, attrs) do
                :ok -> flow_governance_release_result(record)
                {:error, _reason} = error -> error
              end

            {:error, _reason} = error ->
              error
          end
        end
      end

      defp do_flow_fail_many(state, %{records: [_ | _] = attrs_list} = attrs) do
        with :ok <-
               flow_many_partitions_valid?(state, attrs_list, Map.get(attrs, @flow_shard_marker)),
             :ok <- flow_transition_many_unique?(attrs_list),
             {:ok, plans, has_record_values?, has_after_terminal?} <-
               flow_fail_many_prepare(state, attrs_list),
             :ok <- flow_fail_many_apply(state, plans, has_record_values?, has_after_terminal?) do
          flow_governance_release_results(plans)
        end
      end

      defp do_flow_fail_many(_state, _attrs),
        do: {:error, "ERR flow items must be a non-empty list"}

      defp flow_prepare_fail_existing_record(record, attrs, lease_token, now_ms) do
        if flow_duplicate_terminal_noop?(record, attrs, "failed") do
          {:ok, :noop}
        else
          with :ok <- flow_require_running_lease(record, lease_token),
               :ok <- flow_require_fencing_token(record, Map.fetch!(attrs, :fencing_token)) do
            version = Map.fetch!(record, :version) + 1
            id = Map.fetch!(record, :id)
            partition_key = Map.get(record, :partition_key)

            payload_ref =
              flow_value_ref(
                attrs,
                :payload,
                id,
                version,
                partition_key,
                Map.get(record, :payload_ref)
              )

            error_ref = flow_value_ref(attrs, :error, id, version, partition_key)
            retention_ttl_ms = Map.get(attrs, :ttl_ms) || Map.get(record, :retention_ttl_ms)

            with {:ok, value_refs} <-
                   flow_named_value_refs(record, attrs, id, version, partition_key) do
              next =
                %{
                  record
                  | state: "failed",
                    version: version,
                    updated_at_ms: now_ms,
                    payload_ref: payload_ref,
                    error_ref: error_ref,
                    ttl_ms: nil,
                    retention_ttl_ms: retention_ttl_ms,
                    lease_owner: nil,
                    lease_token: nil,
                    lease_deadline_ms: 0,
                    next_run_at_ms: nil
                }
                |> flow_put_record_value_refs(value_refs)
                |> flow_apply_attribute_updates(attrs)
                |> flow_stamp_terminal_retention(now_ms)

              with :ok <- flow_validate_terminal_state_index_key(next) do
                {:ok, record, next}
              end
            end
          end
        end
      end

      defp flow_apply_fail(state, record, next, partition_key, now_ms, attrs) do
        plans = [{record, next}]

        with :ok <- flow_put_record_values(state, next, attrs),
             :ok <- flow_transition_move_indexes(state, plans),
             :ok <-
               flow_put_state_record(
                 state,
                 FlowKeys.state_key(next.id, partition_key),
                 next
               ),
             :ok <- flow_history_put_planned(state, record, next, "failed", now_ms),
             :ok <- flow_after_history_put(state, next),
             :ok <- flow_maybe_cancel_children_on_parent_closed(state, next, now_ms),
             :ok <- flow_maybe_apply_child_terminal(state, next, "failed", now_ms) do
          :ok
        end
      end

      defp flow_fail_many_prepare(state, attrs_list) do
        existing_records = flow_read_records(state, attrs_list)

        attrs_list
        |> Enum.zip(existing_records)
        |> Enum.reduce_while({:ok, [], false, false}, fn
          {%{id: _id, lease_token: lease_token} = attrs, existing},
          {:ok, acc, has_values?, has_after_terminal?} ->
            now_ms = flow_attrs_now_ms(attrs)

            case existing do
              nil ->
                {:halt, {:error, "ERR flow not found"}}

              record ->
                case flow_prepare_fail_existing_record(record, attrs, lease_token, now_ms) do
                  {:ok, record, next} ->
                    {:cont,
                     {:ok, [{record, next, attrs} | acc],
                      has_values? or flow_attrs_have_record_values?(attrs),
                      has_after_terminal? or flow_terminal_after_required?(:fail, next)}}

                  {:ok, :noop} ->
                    {:cont, {:ok, acc, has_values?, has_after_terminal?}}

                  {:error, _reason} = error ->
                    {:halt, error}
                end
            end

          {_bad, _existing}, {:ok, _acc, _has_values?, _has_after_terminal?} ->
            {:halt, {:error, "ERR flow id must be a non-empty string"}}
        end)
        |> case do
          {:ok, plans, has_record_values?, has_after_terminal?} ->
            {:ok, Enum.reverse(plans), has_record_values?, has_after_terminal?}

          {:error, _reason} = error ->
            error
        end
      end

      defp flow_fail_many_apply(state, plans, has_record_values?, has_after_terminal?) do
        with :ok <- flow_many_put_record_values(state, plans, has_record_values?),
             :ok <- flow_terminal_transition_move_indexes(state, plans),
             :ok <- flow_claim_put_state_records(state, plans),
             :ok <- flow_many_put_history(state, plans, "failed"),
             :ok <- flow_many_after_terminal(state, plans, "failed", has_after_terminal?) do
          :ok
        end
      end

      defp do_flow_cancel(state, %{id: id} = attrs) do
        now_ms = flow_attrs_now_ms(attrs)
        partition_key = Map.get(attrs, :partition_key)

        with {:ok, record} <- flow_require_record(state, id, partition_key),
             {:ok, record, next} <- flow_prepare_cancel_existing_record(record, attrs, now_ms),
             next = flow_refresh_indexed_attributes(state, next),
             :ok <- flow_apply_cancel(state, record, next, attrs, partition_key, now_ms) do
          flow_governance_release_result(record)
        end
      end

      defp do_flow_cancel_many(state, %{records: [_ | _] = attrs_list} = attrs) do
        with :ok <-
               flow_many_partitions_valid?(state, attrs_list, Map.get(attrs, @flow_shard_marker)),
             :ok <- flow_transition_many_unique?(attrs_list),
             {:ok, plans, has_record_values?, has_after_terminal?} <-
               flow_cancel_many_prepare(state, attrs_list),
             :ok <- flow_cancel_many_apply(state, plans, has_record_values?, has_after_terminal?) do
          flow_governance_release_results(plans)
        end
      end

      defp do_flow_cancel_many(_state, _attrs),
        do: {:error, "ERR flow items must be a non-empty list"}

      defp flow_prepare_cancel_existing_record(record, attrs, now_ms) do
        with :ok <- flow_require_fencing_token(record, Map.fetch!(attrs, :fencing_token)),
             :ok <- flow_reject_terminal_current(record),
             :ok <- flow_require_transition_lease(record, Map.get(attrs, :lease_token)) do
          version = Map.fetch!(record, :version) + 1
          partition_key = Map.get(record, :partition_key)

          error_ref =
            flow_value_ref(
              attrs,
              :error,
              Map.fetch!(record, :id),
              version,
              partition_key,
              Map.get(attrs, :reason_ref)
            )

          retention_ttl_ms = Map.get(attrs, :ttl_ms) || Map.get(record, :retention_ttl_ms)

          with {:ok, value_refs} <-
                 flow_named_value_refs(
                   record,
                   attrs,
                   Map.fetch!(record, :id),
                   version,
                   partition_key
                 ) do
            next =
              %{
                record
                | state: "cancelled",
                  version: version,
                  updated_at_ms: now_ms,
                  error_ref: error_ref,
                  ttl_ms: nil,
                  retention_ttl_ms: retention_ttl_ms,
                  lease_owner: nil,
                  lease_token: nil,
                  lease_deadline_ms: 0,
                  next_run_at_ms: nil
              }
              |> flow_put_record_value_refs(value_refs)
              |> flow_apply_attribute_updates(attrs)
              |> flow_stamp_terminal_retention(now_ms)

            with :ok <- flow_validate_terminal_state_index_key(next) do
              {:ok, record, next}
            end
          end
        end
      end

      defp flow_apply_cancel(state, record, next, attrs, partition_key, now_ms) do
        plans = [{record, next}]
        refresh_attrs = flow_cancel_refresh_attrs(attrs)

        with :ok <- do_flow_put_record_values(state, next, attrs),
             :ok <- flow_refresh_terminal_value_expirations(state, next, refresh_attrs),
             :ok <- flow_terminal_transition_move_indexes(state, plans),
             :ok <-
               flow_put_state_record(
                 state,
                 FlowKeys.state_key(next.id, partition_key),
                 next
               ),
             :ok <- flow_history_put_planned(state, record, next, "cancelled", now_ms),
             :ok <- flow_after_history_put(state, next),
             :ok <- flow_maybe_cancel_children_on_parent_closed(state, next, now_ms),
             :ok <- flow_maybe_apply_child_terminal(state, next, "cancelled", now_ms) do
          :ok
        end
      end
    end
  end
end
