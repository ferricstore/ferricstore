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
      alias Ferricstore.Commands.Json
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

        with {:ok, record, next} <-
               flow_prepare_transition_record(
                 state,
                 attrs,
                 id,
                 from_state,
                 to_state,
                 run_at_ms,
                 now_ms
               ),
             :ok <- flow_apply_transition(state, record, next, partition_key, now_ms, attrs) do
          :ok
        end
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
               ) do
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
                {:cont,
                 {:ok, [{record, next, attrs} | acc],
                  flow_merge_record_value_mode(value_mode, flow_attrs_record_value_mode(attrs))}}

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

        with {:ok, record} <- flow_require_record(state, id, partition_key),
             {:ok, record, next, history_meta} <-
               flow_prepare_retry_existing_record(state, record, attrs, lease_token, now_ms),
             :ok <-
               flow_apply_retry(state, record, next, partition_key, now_ms, history_meta, attrs) do
          :ok
        end
      end

      defp do_flow_retry_many(state, %{records: [_ | _] = attrs_list} = attrs) do
        with :ok <-
               flow_many_partitions_valid?(state, attrs_list, Map.get(attrs, @flow_shard_marker)),
             :ok <- flow_transition_many_unique?(attrs_list),
             {:ok, plans, has_record_values?, has_after_terminal?} <-
               flow_retry_many_prepare(state, attrs_list),
             :ok <- flow_retry_many_apply(state, plans, has_record_values?, has_after_terminal?) do
          :ok
        end
      end

      defp do_flow_retry_many(_state, _attrs),
        do: {:error, "ERR flow items must be a non-empty list"}

      defp flow_prepare_retry_existing_record(state, record, attrs, lease_token, now_ms) do
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
              |> flow_stamp_terminal_retention(now_ms)

            with :ok <- flow_validate_claim_next_record_keys(next) do
              {:ok, record, next,
               flow_retry_history_meta(record, next, retry_policy, retry_decision)}
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

        with {:ok, record} <- flow_require_record(state, id, partition_key),
             {:ok, record, next} <-
               flow_prepare_fail_existing_record(record, attrs, lease_token, now_ms),
             :ok <- flow_apply_fail(state, record, next, partition_key, now_ms, attrs) do
          :ok
        end
      end

      defp do_flow_fail_many(state, %{records: [_ | _] = attrs_list} = attrs) do
        with :ok <-
               flow_many_partitions_valid?(state, attrs_list, Map.get(attrs, @flow_shard_marker)),
             :ok <- flow_transition_many_unique?(attrs_list),
             {:ok, plans, has_record_values?, has_after_terminal?} <-
               flow_fail_many_prepare(state, attrs_list),
             :ok <- flow_fail_many_apply(state, plans, has_record_values?, has_after_terminal?) do
          :ok
        end
      end

      defp do_flow_fail_many(_state, _attrs),
        do: {:error, "ERR flow items must be a non-empty list"}

      defp flow_prepare_fail_existing_record(record, attrs, lease_token, now_ms) do
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
              |> flow_stamp_terminal_retention(now_ms)

            with :ok <- flow_validate_terminal_state_index_key(next) do
              {:ok, record, next}
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
             :ok <- flow_apply_cancel(state, record, next, attrs, partition_key, now_ms) do
          :ok
        end
      end

      defp do_flow_cancel_many(state, %{records: [_ | _] = attrs_list} = attrs) do
        with :ok <-
               flow_many_partitions_valid?(state, attrs_list, Map.get(attrs, @flow_shard_marker)),
             :ok <- flow_transition_many_unique?(attrs_list),
             {:ok, plans, has_record_values?, has_after_terminal?} <-
               flow_cancel_many_prepare(state, attrs_list),
             :ok <- flow_cancel_many_apply(state, plans, has_record_values?, has_after_terminal?) do
          :ok
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
