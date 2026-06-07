defmodule Ferricstore.Raft.StateMachine.Sections.Part19 do
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
      value_refs: flow_history_projection_value_refs(record),
      value: Flow.encode_history_fields(record, event, now_ms, meta)
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
  defp flow_require_expected_state(_record, _expected_state), do: {:error, "ERR flow wrong state"}

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

    with :ok <- flow_validate_key_size(state_key),
         :ok <- flow_validate_key_size(history_key),
         :ok <- flow_validate_key_size(FlowKeys.state_index_key(type, flow_state, partition_key)),
         :ok <-
           flow_validate_key_size(
             FlowKeys.stream_entry_key_from_history_key(
               history_key,
               "18446744073709551615-18446744073709551615"
             )
           ) do
      with :ok <- flow_validate_due_key(record, type, flow_state, priority, partition_key),
           :ok <- flow_validate_running_index_keys(record, type, partition_key) do
        flow_validate_metadata_index_keys(record, partition_key)
      end
    end
  end

  defp flow_validate_terminal_state_index_key(%{type: type, state: flow_state} = record) do
    type
    |> FlowKeys.state_index_key(flow_state, Map.get(record, :partition_key))
    |> flow_validate_key_size()
  end

  defp flow_validate_claim_next_record_keys(
         %{type: type, state: flow_state, priority: priority} = record
       ) do
    partition_key = Map.get(record, :partition_key)

    with :ok <- flow_validate_key_size(FlowKeys.state_index_key(type, flow_state, partition_key)),
         :ok <- flow_validate_due_key(record, type, flow_state, priority, partition_key) do
      flow_validate_running_index_keys(record, type, partition_key)
    end
  end

  defp flow_validate_due_key(record, type, flow_state, priority, partition_key) do
    case Map.get(record, :next_run_at_ms) do
      nil ->
        :ok

      _ ->
        with :ok <-
               flow_validate_key_size(FlowKeys.due_key(type, flow_state, priority, partition_key)) do
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
      {flow_non_default_root_flow_id(record, id), &FlowKeys.root_index_key(&1, partition_key)},
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

  defp flow_read_policy(state, type) do
    case ets_lookup(state, FlowKeys.policy_key(type)) do
      {:hit, value, _expire_at_ms} when is_binary(value) ->
        case RetryPolicy.decode_flow_policy(value) do
          {:ok, policy} -> policy
          :error -> nil
        end

      _ ->
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

    lmdb_results = flow_read_lmdb_records(state, Enum.map(lmdb_reads, fn {_idx, key} -> key end))

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
    pending = Process.get(:sm_pending_lmdb_values, %{})

    {results, lmdb_keys} =
      keys
      |> Enum.with_index()
      |> Enum.reduce({%{}, []}, fn {key, idx}, {result_acc, read_acc} ->
        if Map.has_key?(pending, key) do
          result = flow_decode_pending_lmdb_record(pending, key)
          {Map.put(result_acc, idx, result), read_acc}
        else
          {result_acc, [{idx, key} | read_acc]}
        end
      end)

    lmdb_values =
      flow_lmdb_get_many(
        state,
        lmdb_keys
        |> Enum.reverse()
        |> Enum.map(fn {_idx, key} -> key end)
      )

    results =
      lmdb_keys
      |> Enum.reverse()
      |> Enum.zip(lmdb_values)
      |> Enum.reduce(results, fn {{idx, _key}, result}, acc -> Map.put(acc, idx, result) end)

    for idx <- 0..(length(keys) - 1)//1, do: Map.get(results, idx, :miss)
  end

  defp flow_lmdb_get_many(_state, []), do: []

  defp flow_lmdb_get_many(state, keys) do
    case Ferricstore.Flow.LMDB.get_many(flow_lmdb_record_path(state), keys) do
      {:ok, results} ->
        keys
        |> Enum.zip(results)
        |> Enum.map(fn
          {_key, {:ok, blob}} ->
            flow_decode_lmdb_blob(blob)

          {key, :not_found} ->
            flow_read_lmdb_cold_park_record(state, key)

          {_key, {:error, _reason}} ->
            :miss
        end)

      {:error, _reason} ->
        Enum.map(keys, fn key ->
          case Ferricstore.Flow.LMDB.get(flow_lmdb_record_path(state), key) do
            {:ok, blob} -> flow_decode_lmdb_blob(blob)
            :not_found -> flow_read_lmdb_cold_park_record(state, key)
            {:error, _reason} -> :miss
          end
        end)
    end
  end

  defp flow_lmdb_records_present(_state, []), do: []

  defp flow_lmdb_records_present(state, keys) do
    pending = Process.get(:sm_pending_lmdb_values, %{})

    {results, lmdb_keys} =
      keys
      |> Enum.with_index()
      |> Enum.reduce({%{}, []}, fn {key, idx}, {result_acc, read_acc} ->
        if Map.has_key?(pending, key) do
          result = flow_pending_lmdb_record_present?(pending, key)
          {Map.put(result_acc, idx, result), read_acc}
        else
          {result_acc, [{idx, key} | read_acc]}
        end
      end)

    lmdb_results =
      flow_lmdb_get_many_present(
        state,
        lmdb_keys
        |> Enum.reverse()
        |> Enum.map(fn {_idx, key} -> key end)
      )

    results =
      lmdb_keys
      |> Enum.reverse()
      |> Enum.zip(lmdb_results)
      |> Enum.reduce(results, fn {{idx, _key}, present?}, acc -> Map.put(acc, idx, present?) end)

    for idx <- 0..(length(keys) - 1)//1, do: Map.get(results, idx, false)
  end

  defp flow_lmdb_get_many_present(_state, []), do: []

  defp flow_lmdb_get_many_present(state, keys) do
    case Ferricstore.Flow.LMDB.get_many(flow_lmdb_record_path(state), keys) do
      {:ok, results} ->
        keys
        |> Enum.zip(results)
        |> Enum.map(fn
          {_key, {:ok, blob}} ->
            flow_lmdb_blob_present?(blob)

          {key, :not_found} ->
            flow_lmdb_cold_park_present?(state, key)

          {_key, {:error, _reason}} ->
            false
        end)

      {:error, _reason} ->
        Enum.map(keys, fn key ->
          case Ferricstore.Flow.LMDB.get(flow_lmdb_record_path(state), key) do
            {:ok, blob} -> flow_lmdb_blob_present?(blob)
            :not_found -> flow_lmdb_cold_park_present?(state, key)
            {:error, _reason} -> false
          end
        end)
    end
  end

  defp flow_pending_lmdb_record_present?(pending, key) do
    case Map.get(pending, key) do
      {:put, blob} -> flow_lmdb_blob_present?(blob)
      :delete -> false
      _ -> false
    end
  end

  defp flow_lmdb_blob_present?(blob) when is_binary(blob) do
    case Ferricstore.Flow.LMDB.decode_value(blob, apply_now_ms()) do
      {:ok, _value} -> true
      :expired -> false
      :error -> false
    end
  end

  defp flow_lmdb_blob_present?(_blob), do: false

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
    cond do
      Map.has_key?(Process.get(:sm_pending_lmdb_values, %{}), key) ->
        flow_decode_pending_lmdb_record(Process.get(:sm_pending_lmdb_values, %{}), key)

      true ->
        case Ferricstore.Flow.LMDB.get(flow_lmdb_record_path(state), key) do
          {:ok, blob} -> flow_decode_lmdb_blob(blob)
          :not_found -> flow_read_lmdb_cold_park_record(state, key)
          {:error, _reason} -> :miss
        end
    end
  end

    end
  end
end
