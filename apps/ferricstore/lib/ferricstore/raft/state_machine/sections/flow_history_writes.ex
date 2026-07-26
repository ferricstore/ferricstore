defmodule Ferricstore.Raft.StateMachine.Sections.FlowHistoryWrites do
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
      alias Ferricstore.Flow.Query.{QueryRecordStore, QueryRowCodec, QueryRowStore}
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

      @flow_lmdb_hydration_batch_size 16
      @flow_lmdb_hydration_max_bytes 1_073_741_824
      @flow_lmdb_presence_batch_size 256

      defp flow_claim_after_history_put_batch(state, plans) do
        records =
          Enum.map(plans, fn plan ->
            {_record, next} = flow_claim_plan_pair(plan)
            next
          end)

        flow_claim_after_history_put_records_batch(state, records)
      end

      defp flow_claim_after_history_put_records_batch(state, records) do
        flow_after_history_put_records_batch(state, records)
      end

      defp flow_claim_after_history_fast_record?(%{state: "running"} = record) do
        flow_history_trim_skippable?(record)
      end

      defp flow_claim_after_history_fast_record?(_record), do: false

      defp flow_history_trim_skippable?(%{history_max_events: nil}), do: true

      defp flow_history_trim_skippable?(%{history_max_events: max}) when not is_integer(max),
        do: true

      defp flow_history_trim_skippable?(%{history_max_events: max, version: version})
           when is_integer(version) and version <= max,
           do: true

      defp flow_history_trim_skippable?(_record), do: false

      defp flow_transition_put_history(state, plans) do
        flow_many_put_history(state, plans, "transitioned")
      end

      defp flow_many_put_history(state, plans, event) do
        flow_with_forced_async_history(fn ->
          {projection_entries, records} =
            flow_many_projection_entries_and_records(state, plans, event, [], [])

          with :ok <- queue_pending_flow_history_projections_batch(projection_entries) do
            flow_after_history_put_records_batch(state, records)
          end
        end)
      end

      defp flow_many_projection_entries_and_records(_state, [], _event, entries, records) do
        {Enum.reverse(entries), Enum.reverse(records)}
      end

      defp flow_many_projection_entries_and_records(state, [plan | rest], event, entries, records) do
        {record, next} = flow_claim_plan_pair(plan)
        partition_key = Map.get(next, :partition_key)
        history_key = FlowKeys.history_key(Map.fetch!(next, :id), partition_key)

        entry =
          flow_history_projection_entry(
            state,
            next,
            history_key,
            event,
            Map.get(next, :updated_at_ms),
            flow_previous_history_ms(record),
            %{}
          )

        flow_many_projection_entries_and_records(state, rest, event, [entry | entries], [
          next | records
        ])
      end

      defp flow_retry_many_put_history(state, plans) do
        flow_with_forced_async_history(fn ->
          {projection_entries, records} =
            flow_retry_projection_entries_and_records(state, plans, [], [])

          with :ok <- queue_pending_flow_history_projections_batch(projection_entries) do
            flow_after_history_put_records_batch(state, records)
          end
        end)
      end

      defp flow_retry_projection_entries_and_records(_state, [], entries, records) do
        {Enum.reverse(entries), Enum.reverse(records)}
      end

      defp flow_retry_projection_entries_and_records(state, [plan | rest], entries, records) do
        {record, next, history_meta} = flow_retry_history_plan(plan)
        partition_key = Map.get(next, :partition_key)
        history_key = FlowKeys.history_key(Map.fetch!(next, :id), partition_key)

        entry =
          flow_history_projection_entry(
            state,
            next,
            history_key,
            "retry",
            Map.get(next, :updated_at_ms),
            flow_previous_history_ms(record),
            history_meta
          )

        flow_retry_projection_entries_and_records(state, rest, [entry | entries], [next | records])
      end

      defp flow_retry_history_plan({record, next, history_meta, _attrs}),
        do: {record, next, history_meta}

      defp flow_retry_history_plan({record, next, history_meta}), do: {record, next, history_meta}

      defp flow_create_put_history(state, records) do
        if flow_async_history_enabled?(state) do
          {projection_entries, records} =
            flow_create_projection_entries_and_records(state, records, [], [])

          with :ok <- queue_pending_flow_history_projections_batch(projection_entries) do
            flow_after_history_put_records_batch(state, records)
          end
        else
          history_entries =
            Enum.map(records, fn record ->
              flow_history_put_ready_entry(
                state,
                record,
                "created",
                Map.get(record, :created_at_ms),
                nil
              )
            end)

          with :ok <- flow_history_index_put_entries(state, history_entries) do
            flow_after_history_put_records_batch(state, records)
          end
        end
      end

      defp flow_create_put_fast_history(state, plans) do
        if flow_async_history_enabled?(state) do
          {projection_entries, records} =
            flow_create_fast_projection_entries_and_records(state, plans, [], [])

          with :ok <- queue_pending_flow_history_projections_batch(projection_entries) do
            flow_after_history_put_records_batch(state, records)
          end
        else
          {history_entries, records} =
            flow_create_fast_history_entries_and_records(state, plans, [], [])

          with :ok <- flow_history_index_put_entries(state, history_entries) do
            flow_after_history_put_records_batch(state, records)
          end
        end
      end

      defp flow_create_projection_entries_and_records(_state, [], entries, records) do
        {Enum.reverse(entries), Enum.reverse(records)}
      end

      defp flow_create_projection_entries_and_records(state, [record | rest], entries, records) do
        partition_key = Map.get(record, :partition_key)
        history_key = FlowKeys.history_key(Map.fetch!(record, :id), partition_key)

        entry =
          flow_history_projection_entry(
            state,
            record,
            history_key,
            "created",
            Map.get(record, :created_at_ms),
            nil,
            %{}
          )

        flow_create_projection_entries_and_records(state, rest, [entry | entries], [
          record | records
        ])
      end

      defp flow_create_fast_projection_entries_and_records(_state, [], entries, records) do
        {Enum.reverse(entries), Enum.reverse(records)}
      end

      defp flow_create_fast_projection_entries_and_records(
             state,
             [%{record: record, history_key: history_key} | rest],
             entries,
             records
           ) do
        entry =
          flow_history_projection_entry(
            state,
            record,
            history_key,
            "created",
            Map.get(record, :created_at_ms),
            nil,
            %{}
          )

        flow_create_fast_projection_entries_and_records(state, rest, [entry | entries], [
          record | records
        ])
      end

      defp flow_create_fast_history_entries_and_records(_state, [], entries, records) do
        {Enum.reverse(entries), Enum.reverse(records)}
      end

      defp flow_create_fast_history_entries_and_records(
             state,
             [%{record: record, history_key: history_key} | rest],
             entries,
             records
           ) do
        entry =
          flow_history_put_ready_entry_with_key(
            state,
            record,
            history_key,
            "created",
            Map.get(record, :created_at_ms),
            nil,
            %{}
          )

        flow_create_fast_history_entries_and_records(state, rest, [entry | entries], [
          record | records
        ])
      end

      defp flow_require_record(state, id, partition_key) do
        case flow_read_record(state, id, partition_key) do
          nil -> {:error, "ERR flow not found"}
          record -> {:ok, record}
        end
      end

      defp flow_history_put_ready_entry(
             state,
             record,
             event,
             now_ms,
             previous_history_ms
           ) do
        flow_history_put_ready_entry(state, record, event, now_ms, previous_history_ms, %{})
      end

      defp flow_history_put_ready_entry(
             state,
             %{id: id, version: _version} = record,
             event,
             now_ms,
             previous_history_ms,
             meta
           ) do
        partition_key = Map.get(record, :partition_key)
        history_key = FlowKeys.history_key(id, partition_key)

        flow_history_put_ready_entry_with_key(
          state,
          record,
          history_key,
          event,
          now_ms,
          previous_history_ms,
          meta
        )
      end

      defp flow_history_put_ready_entry_with_key(
             state,
             record,
             history_key,
             event,
             now_ms,
             previous_history_ms,
             meta
           ) do
        entry =
          flow_history_projection_entry(
            state,
            record,
            history_key,
            event,
            now_ms,
            previous_history_ms,
            meta
          )

        :ok = flow_history_put_or_queue_entry(state, entry)

        {history_key, entry.event_id, entry.event_ms}
      end

      defp flow_history_projection_entry(
             state,
             %{version: version} = record,
             history_key,
             event,
             now_ms,
             previous_history_ms,
             meta
           ) do
        {event_id, event_ms} =
          flow_history_next_event(state, history_key, now_ms, version, previous_history_ms)

        %{
          key: FlowKeys.stream_entry_key_from_history_key(history_key, event_id),
          expire_at_ms: 0,
          history_key: history_key,
          event_id: event_id,
          event_ms: event_ms,
          version: version,
          shard_index: state.shard_index,
          history_hot_max_events: Map.get(record, :history_hot_max_events),
          history_max_events: Map.get(record, :history_max_events),
          terminal?: flow_terminal_record?(record),
          value: {:flow_history_fields, record, event, now_ms, meta}
        }
        |> flow_history_maybe_put_hot_evict_event_ids(
          flow_history_hot_evict_event_ids(record, event_id, version, previous_history_ms)
        )
      end

      defp flow_terminal_record?(record) do
        Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state))
      end

      def __flow_history_projection_value_refs_for_test__(record),
        do: flow_history_projection_value_refs(record)

      defp flow_history_projection_value_refs(record) when is_map(record) do
        named_refs = flow_history_projection_named_value_refs(Map.get(record, :value_refs))

        [
          Map.get(record, :payload_ref),
          Map.get(record, :result_ref),
          Map.get(record, :error_ref)
          | named_refs
        ]
        |> Enum.filter(&(is_binary(&1) and &1 != ""))
      end

      defp flow_history_projection_value_refs(_record), do: []

      defp flow_history_projection_named_value_refs(%{} = refs) do
        Enum.flat_map(refs, fn
          {_name, %{ref: ref}} when is_binary(ref) -> [ref]
          {_name, %{"ref" => ref}} when is_binary(ref) -> [ref]
          {_name, ref} when is_binary(ref) -> [ref]
          _entry -> []
        end)
      end

      defp flow_history_projection_named_value_refs(_refs), do: []

      defp flow_history_hot_evict_event_ids(record, event_id, version, previous_history_ms) do
        []
        |> flow_history_maybe_add_terminal_evict_id(record, event_id)
        |> flow_history_maybe_add_previous_evict_id(record, version, previous_history_ms)
        |> Enum.uniq()
      end

      defp flow_history_maybe_add_terminal_evict_id(ids, record, event_id) do
        if flow_terminal_record?(record) and is_binary(event_id) and event_id != "" do
          [event_id | ids]
        else
          ids
        end
      end

      defp flow_history_maybe_add_previous_evict_id(ids, record, version, previous_history_ms) do
        if Map.get(record, :history_hot_max_events) == 1 and is_integer(version) and version > 1 and
             is_integer(previous_history_ms) do
          previous_id =
            Integer.to_string(previous_history_ms) <> "-" <> Integer.to_string(version - 1)

          [previous_id | ids]
        else
          ids
        end
      end

      defp flow_history_maybe_put_hot_evict_event_ids(entry, []), do: entry

      defp flow_history_maybe_put_hot_evict_event_ids(entry, ids),
        do: Map.put(entry, :hot_evict_event_ids, ids)

      defp flow_require_expected_state(_record, nil), do: :ok
      defp flow_require_expected_state(%{state: expected_state}, expected_state), do: :ok

      defp flow_require_expected_state(_record, _expected_state),
        do: {:error, "ERR flow wrong state"}

      defp flow_require_running_lease(%{state: "running", lease_token: token}, token), do: :ok
      defp flow_require_running_lease(_record, _token), do: {:error, "ERR stale flow lease"}

      defp flow_require_fencing_token(record, fencing_token) do
        if Map.get(record, :fencing_token, 0) == fencing_token do
          :ok
        else
          {:error, "ERR stale flow lease"}
        end
      end

      defp flow_require_transition_lease(%{lease_token: nil}, nil), do: :ok
      defp flow_require_transition_lease(%{lease_token: token}, token), do: :ok
      defp flow_require_transition_lease(_record, _token), do: {:error, "ERR stale flow lease"}

      defp flow_duplicate_terminal_noop?(record, attrs, terminal_state) do
        Map.get(record, :state) == terminal_state and
          Ferricstore.Flow.LMDB.terminal_state?(terminal_state) and
          flow_same_fencing_token?(record, attrs)
      end

      defp flow_duplicate_transition_noop?(record, attrs, to_state) do
        Map.get(record, :state) == to_state and
          not Ferricstore.Flow.LMDB.terminal_state?(to_state) and
          is_nil(Map.get(record, :lease_token)) and
          flow_same_fencing_token?(record, attrs)
      end

      defp flow_duplicate_retry_noop?(state, record, attrs) do
        case flow_attrs_fencing_token(attrs) do
          fencing_token when is_integer(fencing_token) ->
            is_nil(Map.get(record, :lease_token)) and
              flow_same_fencing_token?(record, attrs) and
              (flow_duplicate_retry_schedule_noop?(record, attrs) or
                 flow_latest_history_event_matches?(state, record, "retry", fencing_token))

          _ ->
            false
        end
      end

      defp flow_duplicate_retry_schedule_noop?(record, attrs) do
        run_at_ms = Map.get(attrs, :run_at_ms)
        state = Map.get(record, :state)
        scheduled? = is_integer(Map.get(record, :next_run_at_ms))

        cond do
          state == "running" or Ferricstore.Flow.LMDB.terminal_state?(state) ->
            false

          is_integer(run_at_ms) ->
            Map.get(record, :next_run_at_ms) == run_at_ms

          scheduled? ->
            state == flow_retry_run_state(record) and Map.get(record, :attempts, 0) > 0

          true ->
            false
        end
      end

      defp flow_same_fencing_token?(record, attrs) do
        case flow_attrs_fencing_token(attrs) do
          fencing_token when is_integer(fencing_token) ->
            Map.get(record, :fencing_token, 0) == fencing_token

          _ ->
            false
        end
      end

      defp flow_attrs_fencing_token(attrs) do
        case Map.fetch(attrs, :fencing_token) do
          {:ok, fencing_token} -> fencing_token
          :error -> nil
        end
      end

      defp flow_latest_history_event_matches?(state, record, event, fencing_token) do
        id = Map.get(record, :id)

        if is_binary(id) and is_binary(event) do
          history_key = FlowKeys.history_key(id, Map.get(record, :partition_key))

          case flow_index_rank_range(state, history_key, 0, 0, true) do
            [{event_id, _event_ms} | _] when is_binary(event_id) ->
              flow_history_event_matches?(
                state,
                record,
                history_key,
                event_id,
                event,
                fencing_token
              )

            _ ->
              false
          end
        else
          false
        end
      end

      defp flow_history_event_matches?(
             state,
             record,
             history_key,
             event_id,
             event,
             fencing_token
           ) do
        compound_key = FlowKeys.stream_entry_key_from_history_key(history_key, event_id)

        case flow_history_lookup_value(state, compound_key) do
          {:hit, value, _expire_at_ms} ->
            fields =
              value
              |> flow_decode_history_fields(record)
              |> flow_history_fields_to_map()

            Map.get(fields, "event") == event and
              flow_history_integer_field(fields, "fencing_token") == fencing_token and
              flow_history_integer_field(fields, "version") == Map.get(record, :version)

          _ ->
            false
        end
      end

      defp flow_history_integer_field(fields, key) do
        case Map.get(fields, key) do
          value when is_integer(value) ->
            value

          value when is_binary(value) ->
            case Integer.parse(value) do
              {integer, ""} -> integer
              _ -> nil
            end

          _ ->
            nil
        end
      end

      defp flow_require_rewindable(%{lease_token: token}) when is_binary(token),
        do: {:error, "ERR flow cannot rewind leased flow"}

      defp flow_require_rewindable(%{parent_flow_id: parent_id})
           when is_binary(parent_id) and parent_id != "",
           do: {:error, "ERR flow cannot rewind parent or child flow"}

      defp flow_require_rewindable(%{child_groups: groups})
           when is_map(groups) and map_size(groups) > 0,
           do: {:error, "ERR flow cannot rewind parent or child flow"}

      defp flow_require_rewindable(_record), do: :ok

      defp flow_validate_record_keys(
             %{id: id, type: type, state: flow_state, priority: priority} = record
           ) do
        partition_key = Map.get(record, :partition_key)
        state_key = FlowKeys.state_key(id, partition_key)
        history_key = FlowKeys.history_key(id, partition_key)

        with :ok <- Ferricstore.Flow.Query.QueryRowCodec.validate_record(state_key, record),
             :ok <- flow_validate_key_size(state_key),
             :ok <- flow_validate_key_size(history_key),
             :ok <-
               flow_validate_key_size(FlowKeys.state_index_key(type, flow_state, partition_key)),
             :ok <-
               flow_validate_key_size(
                 FlowKeys.stream_entry_key_from_history_key(
                   history_key,
                   "18446744073709551615-18446744073709551615"
                 )
               ) do
          with :ok <- flow_validate_due_key(record, type, flow_state, priority, partition_key),
               :ok <- flow_validate_running_index_keys(record, type, partition_key),
               :ok <-
                 flow_validate_shared_value_ref_locality(
                   record,
                   flow_record_shared_value_refs(record)
                 ) do
            flow_validate_metadata_index_keys(record, partition_key)
          end
        end
      end

      defp flow_validate_terminal_state_index_key(%{type: type, state: flow_state} = record) do
        partition_key = Map.get(record, :partition_key)

        with :ok <- flow_validate_query_row(record) do
          type
          |> FlowKeys.state_index_key(flow_state, partition_key)
          |> flow_validate_key_size()
        end
      end

      defp flow_validate_claim_next_record_keys(
             %{type: type, state: flow_state, priority: priority} = record
           ) do
        partition_key = Map.get(record, :partition_key)

        with :ok <- flow_validate_query_row(record),
             :ok <-
               flow_validate_key_size(FlowKeys.state_index_key(type, flow_state, partition_key)),
             :ok <- flow_validate_due_key(record, type, flow_state, priority, partition_key) do
          flow_validate_running_index_keys(record, type, partition_key)
        end
      end

      defp flow_validate_query_row(%{id: id} = record) when is_binary(id) do
        state_key = FlowKeys.state_key(id, Map.get(record, :partition_key))
        Ferricstore.Flow.Query.QueryRowCodec.validate_record(state_key, record)
      end

      defp flow_validate_query_row(_record), do: {:error, "ERR invalid flow query metadata"}

      defp flow_validate_due_key(record, type, flow_state, priority, partition_key) do
        case Map.get(record, :next_run_at_ms) do
          nil ->
            :ok

          _ ->
            with :ok <-
                   flow_validate_key_size(
                     FlowKeys.due_key(type, flow_state, priority, partition_key)
                   ) do
              if flow_due_any_index_enabled?() do
                flow_validate_key_size(FlowKeys.due_any_key(type, priority, partition_key))
              else
                :ok
              end
            end
        end
      end

      defp flow_validate_running_index_keys(%{state: "running"} = record, type, partition_key) do
        with :ok <- flow_validate_key_size(FlowKeys.inflight_index_key(type, partition_key)) do
          flow_validate_key_size(
            FlowKeys.worker_index_key(Map.get(record, :lease_owner, ""), partition_key)
          )
        end
      end

      defp flow_validate_running_index_keys(_record, _type, _partition_key), do: :ok

      defp flow_validate_metadata_index_keys(record, partition_key) do
        id = Map.get(record, :id)

        [
          {Map.get(record, :parent_flow_id), &FlowKeys.parent_index_key(&1, partition_key)},
          {flow_non_default_root_flow_id(record, id),
           &FlowKeys.root_index_key(&1, partition_key)},
          {Map.get(record, :correlation_id), &FlowKeys.correlation_index_key(&1, partition_key)}
        ]
        |> Enum.reduce_while(:ok, fn
          {value, key_fun}, :ok when is_binary(value) and value != "" ->
            case flow_validate_key_size(key_fun.(value)) do
              :ok -> {:cont, :ok}
              {:error, _reason} = error -> {:halt, error}
            end

          _entry, :ok ->
            {:cont, :ok}
        end)
      end

      defp flow_non_default_root_flow_id(record, id) do
        case Map.get(record, :root_flow_id) do
          ^id -> nil
          root_flow_id -> root_flow_id
        end
      end

      defp flow_validate_key_size(key) do
        if byte_size(key) <= @flow_max_key_size do
          :ok
        else
          {:error, "ERR key too large (max #{@flow_max_key_size} bytes)"}
        end
      end

      defp flow_read_record(state, id, partition_key) do
        key = FlowKeys.state_key(id, partition_key)

        flow_read_record_by_key(state, key)
      end

      defp flow_read_record_by_key(state, key) do
        case flow_read_state_record_status(state, key) do
          {:record, record} ->
            record

          :expired ->
            nil

          :miss ->
            case flow_read_lmdb_record(state, key) do
              {:ok, record} -> record
              :miss -> nil
            end
        end
      end

      defp flow_read_policy(_state, type) when not is_binary(type), do: nil

      defp flow_read_policy(_state, type) do
        case Process.get(:sm_flow_policy_snapshots, %{}) do
          %{^type => %{policy: policy}} when is_map(policy) ->
            policy

          _missing ->
            nil
        end
      end

      defp flow_read_records(state, attrs_list) do
        flow_read_records_by_keys(state, flow_state_keys_for_attrs(attrs_list))
      end

      defp flow_state_keys_for_attrs(attrs_list) do
        Enum.map(attrs_list, fn attrs ->
          FlowKeys.state_key(Map.fetch!(attrs, :id), Map.get(attrs, :partition_key))
        end)
      end

      defp flow_state_keys_present(state, keys) do
        hot_results = Enum.map(keys, &flow_state_key_present_hot?(state, &1))

        if Enum.any?(hot_results, &(&1 == false)) do
          lmdb_reads =
            keys
            |> Enum.zip(hot_results)
            |> Enum.with_index()
            |> Enum.flat_map(fn
              {{key, false}, idx} -> [{idx, key}]
              {_present, _idx} -> []
            end)

          lmdb_results =
            flow_lmdb_records_present(
              state,
              Enum.map(lmdb_reads, fn {_idx, key} -> key end)
            )

          lmdb_by_idx =
            lmdb_reads
            |> Enum.zip(lmdb_results)
            |> Map.new(fn {{idx, _key}, present?} -> {idx, present?} end)

          hot_results
          |> Enum.with_index()
          |> Enum.map(fn
            {true, _idx} -> true
            {false, idx} -> Map.get(lmdb_by_idx, idx, false)
          end)
        else
          hot_results
        end
      end

      defp flow_state_keys_present_hot_only(state, keys) do
        Enum.map(keys, &flow_state_key_present_hot?(state, &1))
      end

      defp flow_registry_keys_present_hot_only(state, keys) do
        Enum.map(keys, &:ets.member(state.ets, &1))
      end

      defp flow_state_key_present?(state, key) do
        [present?] = flow_state_keys_present(state, [key])
        present?
      end

      defp flow_state_key_present_hot?(state, key) do
        case flow_read_state_record_status(state, key) do
          {:record, _record} -> true
          :expired -> false
          :miss -> false
        end
      end

      defp flow_read_records_by_keys(state, keys) do
        flow_read_mirror_records(state, keys)
      end

      defp flow_read_hot_state_record(state, key) do
        case :ets.lookup(state.ets, key) do
          [{^key, value, 0, _lfu, _fid, _off, _vsize}] when is_binary(value) ->
            flow_decode_hot_state_value(value)

          _ ->
            flow_read_ets_record(state, key)
        end
      rescue
        ArgumentError -> nil
      end

      defp flow_read_mirror_records(state, keys) do
        ets_results = Enum.map(keys, &flow_read_state_record_status(state, &1))

        lmdb_reads =
          keys
          |> Enum.zip(ets_results)
          |> Enum.with_index()
          |> Enum.flat_map(fn
            {{key, :miss}, idx} -> [{idx, key}]
            {_present, _idx} -> []
          end)

        lmdb_results =
          flow_read_lmdb_records(state, Enum.map(lmdb_reads, fn {_idx, key} -> key end))

        results =
          lmdb_reads
          |> Enum.zip(lmdb_results)
          |> Enum.reduce(%{}, fn
            {{idx, _key}, {:ok, record}}, acc -> Map.put(acc, idx, record)
            {{idx, _key}, _result}, acc -> Map.put(acc, idx, nil)
          end)

        results =
          ets_results
          |> Enum.with_index()
          |> Enum.reduce(results, fn
            {:miss, _idx}, acc ->
              acc

            {:expired, idx}, acc ->
              Map.put(acc, idx, nil)

            {{:record, record}, idx}, acc ->
              Map.put(acc, idx, record)

            {nil, _idx}, acc ->
              acc

            {record, idx}, acc ->
              Map.put(acc, idx, record)
          end)

        keys
        |> Enum.with_index()
        |> Enum.map(fn {_key, idx} -> Map.get(results, idx) end)
      end

      defp flow_read_lmdb_records(_state, []), do: []

      defp flow_read_lmdb_records(state, keys) do
        flow_read_lmdb_records_at(state, keys, apply_now_ms(), [])
      end

      defp flow_read_lmdb_records_including_expired(state, keys) do
        flow_read_lmdb_records_at(state, keys, 0, include_expired: true)
      end

      defp flow_read_lmdb_records_at(state, keys, now_ms, opts) do
        ctx = instance_ctx_for_state(state)
        do_flow_read_lmdb_records(ctx, state, keys, now_ms, opts, [])
      end

      defp do_flow_read_lmdb_records(_ctx, _state, [], _now_ms, _opts, acc),
        do: acc |> Enum.reverse() |> List.flatten()

      defp do_flow_read_lmdb_records(ctx, state, keys, now_ms, opts, acc) do
        {batch, rest} = Enum.split(keys, @flow_lmdb_hydration_batch_size)

        max_input_bytes =
          ctx
          |> QueryRecordStore.max_input_bytes()
          |> Kernel.*(length(batch))
          |> min(@flow_lmdb_hydration_max_bytes)

        case QueryRecordStore.read_many(
               ctx,
               state.shard_index,
               flow_lmdb_record_path(state),
               batch,
               now_ms,
               max_input_bytes,
               opts
             ) do
          {:ok, records, true} when length(records) == length(batch) ->
            results =
              Enum.map(records, fn
                record when is_map(record) -> {:ok, record}
                nil -> :miss
              end)

            do_flow_read_lmdb_records(ctx, state, rest, now_ms, opts, [results | acc])

          {:error, reason} ->
            record_state_read_failure({:flow_query_record_read_failed, reason})
            Enum.reverse(acc) |> List.flatten() |> Kernel.++(List.duplicate(:miss, length(keys)))

          invalid ->
            record_state_read_failure({:invalid_flow_query_record_read, invalid})
            Enum.reverse(acc) |> List.flatten() |> Kernel.++(List.duplicate(:miss, length(keys)))
        end
      end

      defp flow_lmdb_records_present(_state, []), do: []

      defp flow_lmdb_records_present(state, keys) do
        do_flow_lmdb_records_present(state, keys, apply_now_ms(), [])
      end

      defp do_flow_lmdb_records_present(_state, [], _now_ms, acc),
        do: acc |> Enum.reverse() |> List.flatten()

      defp do_flow_lmdb_records_present(state, keys, now_ms, acc) do
        {batch, rest} = Enum.split(keys, @flow_lmdb_presence_batch_size)
        max_bytes = max(length(batch) * QueryRowCodec.max_encoded_bytes(), 1)

        case QueryRowStore.read_many(
               flow_lmdb_record_path(state),
               batch,
               now_ms,
               max_bytes
             ) do
          {:ok, rows, _value_bytes, true} when length(rows) == length(batch) ->
            present = Enum.map(rows, &(not is_nil(&1)))
            do_flow_lmdb_records_present(state, rest, now_ms, [present | acc])

          {:error, reason} ->
            record_state_read_failure({:flow_query_row_read_failed, reason})
            Enum.reverse(acc) |> List.flatten() |> Kernel.++(List.duplicate(false, length(keys)))

          invalid ->
            record_state_read_failure({:invalid_flow_query_row_read, invalid})
            Enum.reverse(acc) |> List.flatten() |> Kernel.++(List.duplicate(false, length(keys)))
        end
      end

      defp flow_read_ets_record(state, key) do
        case flow_read_state_record_status(state, key) do
          {:record, record} -> record
          :expired -> nil
          :miss -> nil
        end
      end

      defp flow_read_state_record_status(state, key) do
        case ets_lookup(state, key) do
          {:hit, value, _expire_at_ms} when is_binary(value) ->
            case flow_decode_hot_state_value(value) do
              nil -> :miss
              record -> {:record, record}
            end

          :expired ->
            :expired

          _ ->
            case flow_read_cold_ets_record(state, key) do
              nil -> :miss
              record -> {:record, record}
            end
        end
      end

      defp flow_read_cold_ets_record(state, key) do
        case :ets.lookup(state.ets, key) do
          [{^key, nil, _expire_at_ms, _lfu, fid, off, vsize}]
          when valid_cold_location(fid, off, vsize) or
                 valid_waraft_segment_location(fid, off, vsize) ->
            case sm_store_batch_get(state, [key], &sm_file_path/2) do
              [value] when is_binary(value) -> flow_decode_hot_state_value(value)
              _ -> nil
            end

          _ ->
            nil
        end
      rescue
        ArgumentError -> nil
      end

      defp flow_decode_hot_state_value(value) when is_binary(value) do
        try do
          Flow.decode_record(value)
        rescue
          _ -> nil
        end
      end

      defp flow_read_lmdb_record(state, key) do
        [result] = flow_read_lmdb_records(state, [key])
        result
      end
    end
  end
end
