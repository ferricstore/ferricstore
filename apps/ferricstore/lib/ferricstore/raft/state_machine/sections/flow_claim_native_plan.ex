defmodule Ferricstore.Raft.StateMachine.Sections.FlowClaimNativePlan do
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

      defp flow_plan_claim_candidates_native(
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
           when is_binary(type) and is_binary(worker) do
        cond do
          remaining <= 0 ->
            {:ok, {[], []}}

          flow_native_index(state) == nil ->
            :fallback

          true ->
            with {:ok,
                  {expected_state, from_due_key, to_due_key, from_state_key, to_state_key,
                   inflight_key, worker_key, state_key_prefix, history_key_prefix}} <-
                   flow_native_claim_keys(due_key, type, state_filter, worker) do
              phase_meta =
                state
                |> flow_claim_due_phase_meta(partition_key, nil, remaining)
                |> Map.merge(%{candidates: length(candidates)})

              values =
                flow_claim_due_phase(:hydrate_values, phase_meta, fn ->
                  flow_read_claim_candidate_hot_values(
                    state,
                    candidates,
                    state_key_prefix,
                    flow_claim_read_partition_key(partition_key)
                  )
                end)

              if Enum.any?(values, &is_nil/1) do
                :fallback
              else
                native_result =
                  flow_claim_due_phase(:plan_candidates_native, phase_meta, fn ->
                    NativeFlowIndex.plan_claims_with_history(
                      candidates,
                      values,
                      type,
                      expected_state,
                      worker,
                      lease_ms,
                      now_ms,
                      remaining,
                      from_due_key,
                      to_due_key,
                      from_state_key,
                      to_state_key,
                      inflight_key,
                      worker_key,
                      state_key_prefix,
                      history_key_prefix
                    )
                  end)

                case native_result do
                  {:ok, native_plans, stale_due_ids, count} ->
                    case flow_decode_native_claim_plans(native_plans) do
                      {:ok, plans} when length(plans) == count -> {:ok, {plans, stale_due_ids}}
                      :fallback -> :fallback
                      _ -> :fallback
                    end

                  :fallback ->
                    :fallback
                end
              end
            else
              _ -> :fallback
            end
        end
      end

      defp flow_plan_claim_candidates_native(
             _state,
             _due_key,
             _type,
             _state_filter,
             _worker,
             _lease_ms,
             _now_ms,
             _partition_key,
             _candidates,
             _remaining
           ),
           do: :fallback

      defp flow_native_claim_keys(due_key, type, state_filter, worker) do
        with {:ok, tag} <- flow_due_key_tag(due_key),
             {:ok, expected_state} <- flow_due_key_expected_state(state_filter),
             {:ok, priority} <- flow_due_key_priority(due_key, tag, type, expected_state),
             false <- expected_state == "running" do
          encoded_type = FlowKeys.index_component(type)
          encoded_expected_state = FlowKeys.index_component(expected_state)
          encoded_running = FlowKeys.index_component("running")
          from_due_key = due_key

          to_due_key =
            "f:" <>
              tag <>
              ":d:" <>
              encoded_type <> ":" <> encoded_running <> ":p" <> Integer.to_string(priority)

          from_state_key =
            "f:" <> tag <> ":i:s:" <> encoded_type <> ":" <> encoded_expected_state

          to_state_key = "f:" <> tag <> ":i:s:" <> encoded_type <> ":" <> encoded_running
          inflight_key = "f:" <> tag <> ":i:r:" <> type
          worker_key = "f:" <> tag <> ":i:w:" <> worker
          state_key_prefix = "f:" <> tag <> ":s:"
          history_key_prefix = "f:" <> tag <> ":h:"

          with :ok <-
                 flow_validate_native_claim_key_sizes(
                   from_due_key,
                   to_due_key,
                   from_state_key,
                   to_state_key,
                   inflight_key,
                   worker_key
                 ) do
            {:ok,
             {expected_state, from_due_key, to_due_key, from_state_key, to_state_key,
              inflight_key, worker_key, state_key_prefix, history_key_prefix}}
          end
        end
      end

      defp flow_validate_native_claim_key_sizes(
             from_due_key,
             to_due_key,
             from_state_key,
             to_state_key,
             inflight_key,
             worker_key
           ) do
        if byte_size(from_due_key) <= @flow_max_key_size and
             byte_size(to_due_key) <= @flow_max_key_size and
             byte_size(from_state_key) <= @flow_max_key_size and
             byte_size(to_state_key) <= @flow_max_key_size and
             byte_size(inflight_key) <= @flow_max_key_size and
             byte_size(worker_key) <= @flow_max_key_size do
          :ok
        else
          {:error, "ERR key too large (max #{@flow_max_key_size} bytes)"}
        end
      end

      defp flow_due_key_expected_state({:exclude, state_filter, exclude_states})
           when is_list(exclude_states) do
        with {:ok, expected_state} <- flow_due_key_expected_state(state_filter),
             false <- expected_state in exclude_states do
          {:ok, expected_state}
        else
          _ -> :error
        end
      end

      defp flow_due_key_expected_state(state_filter) when is_binary(state_filter),
        do: {:ok, state_filter}

      defp flow_due_key_expected_state(_state_filter), do: :error

      defp flow_due_key_tag(due_key) when is_binary(due_key) do
        case flow_due_key_tag_match(due_key) do
          {pos, _len} when pos >= 2 ->
            {:ok, binary_part(due_key, 2, pos + 1 - 2)}

          nil ->
            :error
        end
      end

      defp flow_due_key_tag(_due_key), do: :error

      defp flow_due_key_tag_match(due_key) do
        case :binary.match(due_key, "}:d:") do
          {pos, len} ->
            {pos, len}

          :nomatch ->
            case :binary.match(due_key, "}:da:") do
              {pos, len} -> {pos, len}
              :nomatch -> nil
            end
        end
      end

      defp flow_due_key_priority(due_key, tag, type, expected_state)
           when is_binary(due_key) and is_binary(tag) and is_binary(type) and
                  is_binary(expected_state) do
        prefix =
          "f:" <>
            tag <>
            ":d:" <>
            FlowKeys.index_component(type) <>
            ":" <> FlowKeys.index_component(expected_state) <> ":p"

        prefix_size = byte_size(prefix)

        with true <- byte_size(due_key) > prefix_size,
             <<^prefix::binary-size(prefix_size), priority_bin::binary>> <- due_key do
          case Integer.parse(priority_bin) do
            {priority, ""} -> {:ok, priority}
            _ -> :error
          end
        else
          _ -> :error
        end
      end

      defp flow_due_key_priority(_due_key, _tag, _type, _expected_state), do: :error

      defp flow_decode_native_claim_plans(native_plans) do
        native_plans
        |> Enum.reduce_while({:ok, []}, fn
          {next_value, entry, state_key, previous_history_ms}, {:ok, acc}
          when is_binary(next_value) and is_tuple(entry) and is_binary(state_key) ->
            with next when is_map(next) <- flow_decode_native_claim_record(next_value) do
              {:cont,
               {:ok,
                [
                  {:native_claim, next, entry, state_key, next_value, previous_history_ms}
                  | acc
                ]}}
            else
              _ -> {:halt, :fallback}
            end

          {next_value, entry, state_key, previous_history_ms, history_entry}, {:ok, acc}
          when is_binary(next_value) and is_tuple(entry) and is_binary(state_key) ->
            with next when is_map(next) <- flow_decode_native_claim_record(next_value),
                 {:ok, history_entry} <- flow_decode_native_claim_history_entry(history_entry) do
              {:cont,
               {:ok,
                [
                  {:native_claim, next, entry, state_key, next_value, previous_history_ms,
                   history_entry}
                  | acc
                ]}}
            else
              _ -> {:halt, :fallback}
            end

          _other, _acc ->
            {:halt, :fallback}
        end)
        |> case do
          {:ok, plans} -> {:ok, Enum.reverse(plans)}
          :fallback -> :fallback
        end
      end

      defp flow_decode_native_claim_record(value) do
        Flow.decode_record(value)
      rescue
        _ -> nil
      end

      defp flow_decode_native_claim_history_entry(
             {history_key, event_id, event_ms, version, key, value, history_hot_max_events,
              history_max_events, terminal?}
           )
           when is_binary(history_key) and is_binary(event_id) and is_integer(event_ms) and
                  event_ms >= 0 and is_integer(version) and version >= 0 and is_binary(key) and
                  is_binary(value) and
                  is_boolean(terminal?) do
        {:ok,
         %{
           key: key,
           expire_at_ms: 0,
           history_key: history_key,
           event_id: event_id,
           event_ms: event_ms,
           version: version,
           history_hot_max_events: flow_native_optional_integer(history_hot_max_events),
           history_max_events: flow_native_optional_integer(history_max_events),
           terminal?: terminal?,
           value: value
         }}
      end

      defp flow_decode_native_claim_history_entry(_entry), do: :fallback

      defp flow_native_optional_integer(value) when is_integer(value), do: value
      defp flow_native_optional_integer(_value), do: nil

      defp flow_read_claim_candidate_records(state, :any, due_key, candidates) do
        prefix = flow_state_key_prefix_from_due_key(due_key)
        flow_read_claim_candidate_hot_records(state, candidates, prefix, nil)
      end

      defp flow_read_claim_candidate_records(state, partition_key, due_key, candidates) do
        prefix = flow_state_key_prefix_from_due_key(due_key)
        flow_read_claim_candidate_hot_records(state, candidates, prefix, partition_key)
      end

      defp flow_claim_read_partition_key(:any), do: nil
      defp flow_claim_read_partition_key(partition_key), do: partition_key

      defp flow_read_claim_candidate_hot_values(state, candidates, prefix, partition_key) do
        candidates
        |> flow_read_claim_candidate_hot_values_loop(state, prefix, partition_key, [])
        |> Enum.reverse()
      end

      @doc false
      def __flow_read_claim_hot_values_for_test__(state, candidates, prefix, partition_key) do
        flow_read_claim_candidate_hot_values(state, candidates, prefix, partition_key)
      end

      defp flow_read_claim_candidate_hot_values_loop([], _state, _prefix, _partition_key, acc),
        do: acc

      defp flow_read_claim_candidate_hot_values_loop(
             [{id, _score} | rest],
             state,
             prefix,
             partition_key,
             acc
           ) do
        key =
          if is_binary(prefix) do
            prefix <> id
          else
            FlowKeys.state_key(id, partition_key)
          end

        flow_read_claim_candidate_hot_values_loop(
          rest,
          state,
          prefix,
          partition_key,
          [flow_read_hot_state_value(state, key) | acc]
        )
      end

      defp flow_read_hot_state_value(state, key) do
        case safe_ets_lookup(state.ets, key) do
          [{^key, value, 0, _lfu, _pending, _touched_at, _disk_size}] when is_binary(value) ->
            value

          [{^key, value, expire_at_ms, _lfu, _pending, _touched_at, _disk_size}]
          when is_binary(value) and is_integer(expire_at_ms) ->
            if expire_at_ms > apply_now_ms(), do: value, else: nil

          _cold_missing_or_expired ->
            nil
        end
      end

      defp flow_read_claim_candidate_hot_records(state, candidates, prefix, partition_key) do
        candidates
        |> flow_read_claim_candidate_hot_records_loop(state, prefix, partition_key, [])
        |> Enum.reverse()
      end

      defp flow_read_claim_candidate_hot_records_loop([], _state, _prefix, _partition_key, acc),
        do: acc

      defp flow_read_claim_candidate_hot_records_loop(
             [{id, _score} | rest],
             state,
             prefix,
             partition_key,
             acc
           ) do
        key =
          if is_binary(prefix) do
            prefix <> id
          else
            FlowKeys.state_key(id, partition_key)
          end

        flow_read_claim_candidate_hot_records_loop(
          rest,
          state,
          prefix,
          partition_key,
          [flow_read_hot_state_record(state, key) | acc]
        )
      end

      defp flow_state_key_prefix_from_due_key(due_key) when is_binary(due_key) do
        case flow_due_key_tag_match(due_key) do
          {pos, _len} when pos >= 2 ->
            tag = binary_part(due_key, 2, pos + 1 - 2)
            "f:" <> tag <> ":s:"

          nil ->
            nil
        end
      end

      defp do_flow_extend_lease(state, %{id: id, lease_token: lease_token} = attrs) do
        now_ms = flow_attrs_now_ms(attrs)
        lease_ms = Map.fetch!(attrs, :lease_ms)
        partition_key = Map.get(attrs, :partition_key)

        with {:ok, record} <- flow_require_record(state, id, partition_key),
             :ok <- flow_require_running_lease(record, lease_token),
             :ok <- flow_require_fencing_token(record, Map.fetch!(attrs, :fencing_token)) do
          version = Map.fetch!(record, :version) + 1
          deadline_ms = now_ms + lease_ms

          next =
            record
            |> Map.merge(%{
              version: version,
              updated_at_ms: now_ms,
              ttl_ms: nil,
              retention_ttl_ms: Map.get(record, :retention_ttl_ms),
              history_hot_max_events: Map.get(record, :history_hot_max_events),
              history_max_events: Map.get(record, :history_max_events),
              lease_deadline_ms: deadline_ms,
              next_run_at_ms: deadline_ms
            })

          with :ok <- flow_validate_record_keys(record),
               :ok <- flow_validate_record_keys(next),
               :ok <- flow_transition_move_indexes(state, [{record, next}]),
               :ok <-
                 flow_put_state_record(
                   state,
                   FlowKeys.state_key(next.id, Map.get(next, :partition_key)),
                   next
                 ),
               :ok <- flow_history_put_planned(state, record, next, "lease_extended", now_ms),
               :ok <- flow_after_history_put(state, next) do
            {:ok, next}
          end
        end
      end

      defp do_flow_complete(state, %{id: id, lease_token: lease_token} = attrs) do
        now_ms = flow_attrs_now_ms(attrs)
        partition_key = Map.get(attrs, :partition_key)

        with {:ok, record} <- flow_require_record(state, id, partition_key) do
          case flow_maybe_timeout_active_record(state, record, now_ms) do
            :active ->
              case flow_prepare_complete_existing_record(record, attrs, lease_token, now_ms) do
                {:ok, :noop} ->
                  :ok

                {:ok, record, next} ->
                  next = flow_refresh_indexed_attributes(state, next)

                  case flow_apply_complete(state, record, next, partition_key, now_ms, attrs) do
                    :ok -> flow_governance_release_result(record)
                    {:error, _reason} = error -> error
                  end

                {:error, _reason} = error ->
                  error
              end

            :timed_out ->
              {:ok, {:error, "ERR flow max_active_ms exceeded"}}

            {:error, _reason} = error ->
              error
          end
        end
      end

      defp do_flow_complete_many(state, %{records: [_ | _] = attrs_list} = attrs) do
        with :ok <-
               flow_many_partitions_valid?(state, attrs_list, Map.get(attrs, @flow_shard_marker)),
             :ok <- flow_transition_many_unique?(attrs_list),
             {:ok, plans, has_record_values?, has_after_terminal?} <-
               flow_complete_many_prepare(state, attrs_list),
             :ok <-
               flow_complete_many_apply(state, plans, has_record_values?, has_after_terminal?) do
          flow_governance_release_results(plans)
        end
      end

      defp do_flow_complete_many(_state, _attrs),
        do: {:error, "ERR flow items must be a non-empty list"}

      defp flow_prepare_complete_existing_record(record, attrs, lease_token, now_ms) do
        cond do
          flow_active_timeout_expired_record?(record, now_ms) ->
            {:error, "ERR flow max_active_ms exceeded"}

          flow_duplicate_terminal_noop?(record, attrs, "completed") ->
            {:ok, :noop}

          true ->
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

              result_ref = flow_value_ref(attrs, :result, id, version, partition_key)
              retention_ttl_ms = Map.get(attrs, :ttl_ms) || Map.get(record, :retention_ttl_ms)

              with {:ok, value_refs} <-
                     flow_named_value_refs(record, attrs, id, version, partition_key) do
                next =
                  %{
                    record
                    | state: "completed",
                      version: version,
                      updated_at_ms: now_ms,
                      payload_ref: payload_ref,
                      result_ref: result_ref,
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

      defp flow_apply_complete(state, record, next, partition_key, now_ms, attrs) do
        plans = [{record, next}]

        with :ok <- flow_put_record_values(state, next, attrs),
             :ok <- flow_transition_move_indexes(state, plans),
             :ok <-
               flow_put_state_record(
                 state,
                 FlowKeys.state_key(next.id, partition_key),
                 next
               ),
             :ok <- flow_history_put_planned(state, record, next, "completed", now_ms),
             :ok <- flow_after_history_put(state, next),
             :ok <- flow_maybe_cancel_children_on_parent_closed(state, next, now_ms),
             :ok <- flow_maybe_apply_child_terminal(state, next, "completed", now_ms) do
          :ok
        end
      end

      defp flow_complete_many_prepare(state, attrs_list) do
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
                case flow_prepare_complete_existing_record(record, attrs, lease_token, now_ms) do
                  {:ok, record, next} ->
                    {:cont,
                     {:ok, [{record, next, attrs} | acc],
                      has_values? or flow_attrs_have_record_values?(attrs),
                      has_after_terminal? or flow_terminal_after_required?(:complete, next)}}

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

      defp flow_complete_many_apply(state, plans, has_record_values?, has_after_terminal?) do
        with :ok <- flow_many_put_record_values(state, plans, has_record_values?),
             :ok <- flow_terminal_transition_move_indexes(state, plans),
             :ok <- flow_claim_put_state_records(state, plans),
             :ok <- flow_many_put_history(state, plans, "completed"),
             :ok <- flow_many_after_terminal(state, plans, "completed", has_after_terminal?) do
          :ok
        end
      end

      defp do_flow_terminal_pipeline_batch(state, op, %{records: [_ | _] = attrs_list})
           when op in [:complete, :retry, :fail, :cancel] do
        {results, plans, has_record_values?, has_after_terminal?} =
          flow_terminal_pipeline_prepare(state, op, attrs_list)

        case flow_terminal_pipeline_apply(
               state,
               op,
               Enum.reverse(plans),
               has_record_values?,
               has_after_terminal?
             ) do
          :ok -> Enum.reverse(results)
          {:error, _reason} = error -> error
        end
      end

      defp do_flow_terminal_pipeline_batch(_state, _op, _attrs),
        do: {:error, "ERR flow items must be a non-empty list"}

      defp flow_terminal_pipeline_prepare(state, op, attrs_list) do
        case flow_terminal_pipeline_unique_records(state, attrs_list) do
          {:ok, records} ->
            flow_terminal_pipeline_prepare_records(
              state,
              op,
              attrs_list,
              records,
              [],
              [],
              false,
              false
            )

          :fallback ->
            flow_terminal_pipeline_prepare(state, op, attrs_list, %{}, [], [], false, false)
        end
      end

      defp flow_terminal_pipeline_unique_records(state, attrs_list) do
        attrs_list
        |> Enum.reduce_while({:ok, [], MapSet.new()}, fn
          %{id: id} = attrs, {:ok, keys, seen} when is_binary(id) and id != "" ->
            key = FlowKeys.state_key(id, Map.get(attrs, :partition_key))

            if MapSet.member?(seen, key) do
              {:halt, :fallback}
            else
              {:cont, {:ok, [key | keys], MapSet.put(seen, key)}}
            end

          _attrs, _acc ->
            {:halt, :fallback}
        end)
        |> case do
          {:ok, keys, _seen} -> {:ok, flow_read_records_by_keys(state, Enum.reverse(keys))}
          :fallback -> :fallback
        end
      end

      defp flow_terminal_pipeline_prepare_records(
             _state,
             _op,
             [],
             [],
             results,
             plans,
             has_record_values?,
             has_after_terminal?
           ) do
        {results, plans, has_record_values?, has_after_terminal?}
      end

      defp flow_terminal_pipeline_prepare_records(
             state,
             op,
             [%{id: id} = attrs | rest_attrs],
             [record | rest_records],
             results,
             plans,
             has_record_values?,
             has_after_terminal?
           )
           when is_binary(id) and id != "" do
        now_ms = flow_attrs_now_ms(attrs)

        case flow_terminal_pipeline_prepare_one(state, op, record, attrs, now_ms) do
          {:ok, next, plan} ->
            flow_terminal_pipeline_prepare_records(
              state,
              op,
              rest_attrs,
              rest_records,
              [flow_governance_release_result(record) | results],
              [plan | plans],
              has_record_values? or flow_attrs_have_record_values?(attrs),
              has_after_terminal? or flow_terminal_after_required?(op, next)
            )

          {:ok, :noop} ->
            flow_terminal_pipeline_prepare_records(
              state,
              op,
              rest_attrs,
              rest_records,
              [:ok | results],
              plans,
              has_record_values?,
              has_after_terminal?
            )

          {:error, _reason} = error ->
            flow_terminal_pipeline_prepare_records(
              state,
              op,
              rest_attrs,
              rest_records,
              [error | results],
              plans,
              has_record_values?,
              has_after_terminal?
            )
        end
      end

      defp flow_terminal_pipeline_prepare_records(
             state,
             op,
             [_bad | rest_attrs],
             [_record | rest_records],
             results,
             plans,
             has_record_values?,
             has_after_terminal?
           ) do
        flow_terminal_pipeline_prepare_records(
          state,
          op,
          rest_attrs,
          rest_records,
          [{:error, "ERR flow id must be a non-empty string"} | results],
          plans,
          has_record_values?,
          has_after_terminal?
        )
      end

      defp flow_terminal_pipeline_prepare_records(
             state,
             op,
             _attrs,
             _records,
             results,
             plans,
             has_record_values?,
             has_after_terminal?
           ) do
        flow_terminal_pipeline_prepare(
          state,
          op,
          [],
          %{},
          results,
          plans,
          has_record_values?,
          has_after_terminal?
        )
      end

      defp flow_terminal_pipeline_prepare(
             _state,
             _op,
             [],
             _virtual_records,
             results,
             plans,
             has_record_values?,
             has_after_terminal?
           ) do
        {results, plans, has_record_values?, has_after_terminal?}
      end

      defp flow_terminal_pipeline_prepare(
             state,
             op,
             [%{id: id} = attrs | rest],
             virtual_records,
             results,
             plans,
             has_record_values?,
             has_after_terminal?
           )
           when is_binary(id) and id != "" do
        partition_key = Map.get(attrs, :partition_key)
        state_key = FlowKeys.state_key(id, partition_key)
        now_ms = flow_attrs_now_ms(attrs)
        record = Map.get(virtual_records, state_key) || flow_read_record(state, id, partition_key)

        case flow_terminal_pipeline_prepare_one(state, op, record, attrs, now_ms) do
          {:ok, next, plan} ->
            flow_terminal_pipeline_prepare(
              state,
              op,
              rest,
              Map.put(virtual_records, state_key, next),
              [flow_governance_release_result(record) | results],
              [plan | plans],
              has_record_values? or flow_attrs_have_record_values?(attrs),
              has_after_terminal? or flow_terminal_after_required?(op, next)
            )

          {:ok, :noop} ->
            flow_terminal_pipeline_prepare(
              state,
              op,
              rest,
              virtual_records,
              [:ok | results],
              plans,
              has_record_values?,
              has_after_terminal?
            )

          {:error, _reason} = error ->
            flow_terminal_pipeline_prepare(
              state,
              op,
              rest,
              virtual_records,
              [error | results],
              plans,
              has_record_values?,
              has_after_terminal?
            )
        end
      end

      defp flow_terminal_pipeline_prepare(
             state,
             op,
             [_bad | rest],
             virtual_records,
             results,
             plans,
             has_record_values?,
             has_after_terminal?
           ) do
        flow_terminal_pipeline_prepare(
          state,
          op,
          rest,
          virtual_records,
          [{:error, "ERR flow id must be a non-empty string"} | results],
          plans,
          has_record_values?,
          has_after_terminal?
        )
      end

      defp flow_terminal_pipeline_prepare_one(_state, _op, nil, _attrs, _now_ms),
        do: {:error, "ERR flow not found"}

      defp flow_terminal_pipeline_prepare_one(_state, :complete, record, attrs, now_ms) do
        case flow_prepare_complete_existing_record(
               record,
               attrs,
               Map.get(attrs, :lease_token),
               now_ms
             ) do
          {:ok, record, next} -> {:ok, next, {record, next, attrs}}
          {:ok, :noop} -> {:ok, :noop}
          {:error, _reason} = error -> error
        end
      end

      defp flow_terminal_pipeline_prepare_one(state, :retry, record, attrs, now_ms) do
        case flow_prepare_retry_existing_record(
               state,
               record,
               attrs,
               Map.get(attrs, :lease_token),
               now_ms
             ) do
          {:ok, record, next, history_meta} -> {:ok, next, {record, next, history_meta, attrs}}
          {:ok, :noop} -> {:ok, :noop}
          {:error, _reason} = error -> error
        end
      end

      defp flow_terminal_pipeline_prepare_one(_state, :fail, record, attrs, now_ms) do
        case flow_prepare_fail_existing_record(
               record,
               attrs,
               Map.get(attrs, :lease_token),
               now_ms
             ) do
          {:ok, record, next} -> {:ok, next, {record, next, attrs}}
          {:ok, :noop} -> {:ok, :noop}
          {:error, _reason} = error -> error
        end
      end

      defp flow_terminal_pipeline_prepare_one(_state, :cancel, record, attrs, now_ms) do
        case flow_prepare_cancel_existing_record(record, attrs, now_ms) do
          {:ok, record, next} -> {:ok, next, {record, next, attrs}}
          {:error, _reason} = error -> error
        end
      end

      defp flow_terminal_pipeline_apply(
             _state,
             _op,
             [],
             _has_record_values?,
             _has_after_terminal?
           ),
           do: :ok

      defp flow_terminal_pipeline_apply(
             state,
             :complete,
             plans,
             has_record_values?,
             has_after_terminal?
           ),
           do: flow_complete_many_apply(state, plans, has_record_values?, has_after_terminal?)

      defp flow_terminal_pipeline_apply(
             state,
             :retry,
             plans,
             has_record_values?,
             has_after_terminal?
           ),
           do: flow_retry_many_apply(state, plans, has_record_values?, has_after_terminal?)

      defp flow_terminal_pipeline_apply(
             state,
             :fail,
             plans,
             has_record_values?,
             has_after_terminal?
           ),
           do: flow_fail_many_apply(state, plans, has_record_values?, has_after_terminal?)

      defp flow_terminal_pipeline_apply(
             state,
             :cancel,
             plans,
             has_record_values?,
             has_after_terminal?
           ),
           do: flow_cancel_many_apply(state, plans, has_record_values?, has_after_terminal?)
    end
  end
end
