defmodule Ferricstore.Raft.StateMachine.Sections.Part16 do
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

  defp flow_retention_value_refs_used_by_lmdb_histories_after(
         _path,
         _prefix,
         _after_key,
         limit,
         _state,
         _owner_record,
         target_refs,
         _referenced
       )
       when limit <= 0,
       do: target_refs

  defp flow_retention_value_refs_used_by_lmdb_histories_after(
         path,
         prefix,
         after_key,
         limit,
         state,
         owner_record,
         target_refs,
         referenced
       ) do
    case Ferricstore.Flow.LMDB.prefix_entries_after(path, prefix, after_key, limit) do
      {:ok, []} ->
        referenced

      {:ok, entries} ->
        referenced =
          Enum.reduce_while(entries, referenced, fn {_history_index_key, lmdb_value}, acc ->
            acc =
              case Ferricstore.Flow.LMDB.decode_history_index_value(lmdb_value) do
                {:ok, {_event_id, _event_ms, _expire_at_ms, compound_key}} ->
                  if flow_retention_same_flow_history_key?(compound_key, owner_record) do
                    acc
                  else
                    state
                    |> flow_retention_history_value_from_lmdb(lmdb_value)
                    |> flow_retention_value_refs_used_by_history_value(target_refs, acc)
                  end

                :error ->
                  acc
              end

            if MapSet.size(acc) >= MapSet.size(target_refs), do: {:halt, acc}, else: {:cont, acc}
          end)

        cond do
          MapSet.size(referenced) >= MapSet.size(target_refs) ->
            referenced

          length(entries) < limit ->
            referenced

          true ->
            {last_key, _last_value} = List.last(entries)

            flow_retention_value_refs_used_by_lmdb_histories_after(
              path,
              prefix,
              last_key,
              limit,
              state,
              owner_record,
              target_refs,
              referenced
            )
        end

      {:error, _reason} ->
        MapSet.union(referenced, target_refs)
    end
  end

  defp flow_retention_value_refs_used_by_history_value(value, target_refs, referenced)
       when is_binary(value) do
    value
    |> flow_retention_all_history_value_refs()
    |> Enum.reduce(referenced, fn ref, acc ->
      if MapSet.member?(target_refs, ref), do: MapSet.put(acc, ref), else: acc
    end)
  end

  defp flow_retention_value_refs_used_by_history_value(_value, _target_refs, referenced),
    do: referenced

  defp flow_retention_same_flow_history_key?(key, owner_record) when is_binary(key) do
    with id when is_binary(id) <- flow_retention_record_id(owner_record) do
      partition_key = flow_retention_record_partition_key(owner_record)
      history_key = FlowKeys.history_key(id, partition_key)
      history_index_prefix = Ferricstore.Flow.LMDB.history_index_prefix(history_key)

      key == history_key or
        String.starts_with?(key, history_key <> <<0>>) or
        String.starts_with?(key, "X:" <> history_key <> <<0>>) or
        String.starts_with?(key, history_index_prefix)
    else
      _other -> false
    end
  end

  defp flow_retention_same_flow_history_key?(_key, _owner_record), do: false

  defp flow_retention_same_flow_record?(record, owner_record) do
    flow_retention_record_id(record) == flow_retention_record_id(owner_record) and
      flow_retention_record_partition_key(record) ==
        flow_retention_record_partition_key(owner_record)
  end

  defp flow_retention_record_id(record) when is_map(record),
    do: Map.get(record, :id) || Map.get(record, "id")

  defp flow_retention_record_id(_record), do: nil

  defp flow_retention_record_partition_key(record) when is_map(record),
    do: Map.get(record, :partition_key) || Map.get(record, "partition_key")

  defp flow_retention_record_partition_key(_record), do: nil

  defp flow_retention_keydir_available?(state) do
    :ets.info(state.ets, :name) != :undefined
  rescue
    ArgumentError -> false
  end

  defp flow_retention_shared_value_links(state, record) do
    prefix =
      FlowKeys.shared_value_link_prefix(
        Map.fetch!(record, :id),
        Map.get(record, :partition_key)
      )

    prefix_len = byte_size(prefix)

    match_spec = [
      {{:"$1", :_, :_, :_, :_, :_, :_},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
       ], [:"$1"]}
    ]

    keys = safe_ets_select(state.ets, match_spec)
    values = sm_store_batch_get(state, keys, &sm_file_path/2)

    keys
    |> Enum.zip(values)
    |> Enum.flat_map(fn
      {key, ref} when is_binary(ref) and ref != "" -> [{key, ref}]
      _other -> []
    end)
  end

  defp flow_retention_owned_value_keys_page(state, %{id: id} = record) when is_binary(id) do
    limit = flow_retention_value_lmdb_scan_limit()

    {keys, complete?} =
      record
      |> flow_retention_owned_value_prefixes()
      |> flow_retention_owned_value_keys_page_prefixes(state, record, limit, [])

    keys =
      keys
      |> Enum.filter(&flow_retention_owned_value_ref?(&1, record))
      |> Enum.uniq()

    {keys, complete?}
  end

  defp flow_retention_owned_value_keys_page(_state, _record), do: {[], true}

  defp flow_retention_owned_value_keys_page_prefixes(_prefixes, _state, _record, remaining, acc)
       when remaining <= 0,
       do: {Enum.reverse(acc), false}

  defp flow_retention_owned_value_keys_page_prefixes([], _state, _record, _remaining, acc),
    do: {Enum.reverse(acc), true}

  defp flow_retention_owned_value_keys_page_prefixes(
         [prefix | rest],
         state,
         record,
         remaining,
         acc
       ) do
    {ets_keys, ets_complete?} = flow_retention_keys_with_prefix_page(state, prefix, remaining)
    remaining = remaining - length(ets_keys)
    acc = Enum.reverse(ets_keys, acc)

    cond do
      not ets_complete? ->
        {Enum.reverse(acc), false}

      remaining <= 0 ->
        {Enum.reverse(acc), false}

      true ->
        {lmdb_keys, lmdb_complete?} =
          flow_retention_lmdb_keys_with_prefix_page(state, prefix, remaining)

        remaining = remaining - length(lmdb_keys)
        acc = Enum.reverse(lmdb_keys, acc)

        cond do
          not lmdb_complete? ->
            {Enum.reverse(acc), false}

          remaining <= 0 and rest != [] ->
            {Enum.reverse(acc), false}

          true ->
            flow_retention_owned_value_keys_page_prefixes(rest, state, record, remaining, acc)
        end
    end
  end

  defp flow_retention_owned_value_prefixes(%{id: id} = record) do
    partition_key = Map.get(record, :partition_key)

    [:payload, :result, :error, :shared]
    |> Enum.map(fn kind ->
      key = FlowKeys.value_key(id, kind, 0, partition_key)
      flow_retention_owned_value_ref_prefix(key)
    end)
  end

  defp flow_retention_keys_with_prefix_page(_state, prefix, _limit) when not is_binary(prefix),
    do: {[], true}

  defp flow_retention_keys_with_prefix_page(_state, _prefix, limit) when limit <= 0,
    do: {[], false}

  defp flow_retention_keys_with_prefix_page(state, prefix, limit) do
    prefix_len = byte_size(prefix)

    match_spec = [
      {{:"$1", :_, :_, :_, :_, :_, :_},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
       ], [:"$1"]}
    ]

    safe_ets_select_page(state.ets, match_spec, limit)
  end

  defp flow_retention_lmdb_keys_with_prefix_page(_state, prefix, _limit)
       when not is_binary(prefix),
       do: {[], true}

  defp flow_retention_lmdb_keys_with_prefix_page(_state, _prefix, limit) when limit <= 0,
    do: {[], false}

  defp flow_retention_lmdb_keys_with_prefix_page(state, prefix, limit) do
    path = flow_lmdb_record_path(state)

    case flow_retention_lmdb_projection_state(state) do
      :available ->
        case Ferricstore.Flow.LMDB.prefix_entries_after(path, prefix, <<>>, limit) do
          {:ok, entries} ->
            keys = Enum.map(entries, fn {key, _value} -> key end)
            {keys, length(entries) < limit}

          {:error, _reason} ->
            {[], false}
        end

      :empty ->
        {[], true}

      :unavailable ->
        {[], false}
    end
  end

  defp flow_retention_history_entries(state, history_key) do
    limit = flow_retention_history_lmdb_scan_limit()

    with {:ok, lmdb_entries, lmdb_complete?} <-
           flow_retention_lmdb_history_entries(state, history_key, limit) do
      remaining = max(limit - length(lmdb_entries), 0)

      cond do
        not lmdb_complete? ->
          {:ok, Enum.uniq_by(lmdb_entries, &flow_retention_history_entry_key/1), false}

        remaining <= 0 ->
          {:ok, Enum.uniq_by(lmdb_entries, &flow_retention_history_entry_key/1), false}

        true ->
          {ets_entries, ets_complete?} =
            flow_retention_ets_history_entries(state, history_key, remaining)

          entries =
            (lmdb_entries ++ ets_entries)
            |> Enum.uniq_by(&flow_retention_history_entry_key/1)

          {:ok, entries, ets_complete?}
      end
    end
  end

  defp flow_retention_lmdb_history_entries(_state, _history_key, remaining) when remaining <= 0,
    do: {:ok, [], false}

  defp flow_retention_lmdb_history_entries(state, history_key, remaining) do
    path = flow_lmdb_record_path(state)
    prefix = Ferricstore.Flow.LMDB.history_index_prefix(history_key)

    case flow_retention_lmdb_projection_state(state) do
      :available ->
        flow_retention_lmdb_history_entries_after(path, prefix, <<>>, remaining, [])

      :empty ->
        {:ok, [], true}

      :unavailable ->
        {:ok, [], false}
    end
  end

  defp flow_retention_lmdb_history_entries_after(path, prefix, after_key, limit, acc) do
    case Ferricstore.Flow.LMDB.prefix_entries_after(path, prefix, after_key, limit) do
      {:ok, []} ->
        {:ok, Enum.reverse(acc), true}

      {:ok, entries} ->
        decoded = flow_retention_decode_lmdb_history_entries(entries, acc)
        complete? = length(entries) < limit
        {:ok, Enum.reverse(decoded), complete?}

      {:error, _reason} = error ->
        error
    end
  end

  defp flow_retention_decode_lmdb_history_entries(entries, acc) do
    Enum.reduce(entries, acc, fn {_history_index_key, value}, acc ->
      case Ferricstore.Flow.LMDB.decode_history_index_value(value) do
        {:ok, {event_id, _event_ms, _expire_at_ms, compound_key}} ->
          [{compound_key, event_id, value} | acc]

        :error ->
          acc
      end
    end)
  end

  defp flow_retention_ets_history_entries(state, history_key, limit) do
    prefix = "X:" <> history_key <> <<0>>
    prefix_len = byte_size(prefix)

    match_spec = [
      {{:"$1", :_, :_, :_, :_, :_, :_},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
       ], [:"$1"]}
    ]

    {ets_keys, ets_complete?} =
      state.ets
      |> safe_ets_select_page(match_spec, limit)

    ets_entries =
      ets_keys
      |> Enum.map(fn key -> {key, binary_part(key, prefix_len, byte_size(key) - prefix_len)} end)

    {ets_entries, ets_complete?}
  end

  defp flow_retention_delete_history_index(_state, _history_key, []), do: :ok

  defp flow_retention_delete_history_index(state, history_key, entries) do
    event_ids = Enum.map(entries, &flow_retention_history_entry_event_id/1)

    with :ok <- flow_index_delete_members(state, history_key, event_ids) do
      with_lmdb_mirror_shard(state, fn ->
        Enum.each(entries, fn entry ->
          event_id = flow_retention_history_entry_event_id(entry)
          queue_lmdb_history_index_delete(nil, history_key, event_id, flow_event_ms(event_id))
        end)
      end)

      :ok
    end
  end

  defp flow_retention_history_lmdb_scan_limit do
    :ferricstore
    |> Application.get_env(:flow_lmdb_history_cleanup_scan_limit, 100_000)
    |> flow_retention_positive_integer(100_000)
  end

  defp flow_retention_value_lmdb_scan_limit do
    :ferricstore
    |> Application.get_env(:flow_lmdb_value_cleanup_scan_limit, 100_000)
    |> flow_retention_positive_integer(100_000)
  end

  defp flow_retention_positive_integer(value, _default) when is_integer(value) and value > 0,
    do: value

  defp flow_retention_positive_integer(value, default) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed > 0 -> parsed
      _ -> default
    end
  end

  defp flow_retention_positive_integer(_value, default), do: default

  defp flow_event_ms(event_id) when is_binary(event_id) do
    event_id
    |> String.split("-", parts: 2)
    |> case do
      [ms, _seq] -> String.to_integer(ms)
      _ -> 0
    end
  rescue
    _ -> 0
  end

  defp flow_event_ms(_event_id), do: 0

  defp flow_retention_history_entry_key({key, _event_id}), do: key
  defp flow_retention_history_entry_key({key, _event_id, _lmdb_value}), do: key

  defp flow_retention_history_entry_event_id({_key, event_id}), do: event_id
  defp flow_retention_history_entry_event_id({_key, event_id, _lmdb_value}), do: event_id

  defp flow_retention_history_values(state, entries) do
    keys = Enum.map(entries, &flow_retention_history_entry_key/1)
    hot_values = sm_store_batch_get(state, keys, &sm_file_path/2)

    entries
    |> Enum.zip(hot_values)
    |> Enum.map(fn
      {_entry, value} when is_binary(value) ->
        value

      {{_key, _event_id, lmdb_value}, _missing} ->
        flow_retention_history_value_from_lmdb(state, lmdb_value)

      {_entry, _missing} ->
        nil
    end)
  end

  defp flow_retention_history_value_from_lmdb(state, lmdb_value) when is_binary(lmdb_value) do
    case Ferricstore.Flow.LMDB.decode_history_index_location(lmdb_value) do
      {:ok,
       {_event_id, _event_ms, _expire_at_ms, _compound_key, {:flow_history, _file_id} = file_ref,
        offset, _value_size}}
      when is_integer(offset) and offset >= 0 ->
        case Ferricstore.Flow.HistoryProjector.read_value(state.shard_data_path, file_ref, offset) do
          {:ok, value} when is_binary(value) -> value
          _ -> nil
        end

      _ ->
        nil
    end
  end

  defp flow_retention_history_value_from_lmdb(_state, _lmdb_value), do: nil

  defp flow_retention_all_history_value_refs(value) when is_binary(value) do
    value
    |> Flow.decode_history_fields()
    |> flow_history_fields_to_map()
    |> flow_retention_all_record_value_refs()
  end

  defp flow_retention_all_history_value_refs(_value), do: []

  defp flow_retention_record_value_refs(record) do
    record_refs =
      [:payload_ref, :result_ref, :error_ref]
      |> Enum.flat_map(fn key ->
        string_key = Atom.to_string(key)
        [Map.get(record, key), Map.get(record, string_key)]
      end)

    named_refs =
      record
      |> flow_retention_named_value_refs()
      |> Map.values()
      |> Enum.map(&Map.get(&1, :ref))

    (record_refs ++ named_refs)
    |> Enum.filter(&flow_retention_owned_value_ref?(&1, record))
  end

  defp flow_retention_all_record_value_refs(record) when is_map(record) do
    direct_refs =
      [:payload_ref, :result_ref, :error_ref]
      |> Enum.flat_map(fn key ->
        string_key = Atom.to_string(key)
        [Map.get(record, key), Map.get(record, string_key)]
      end)

    named_refs =
      record
      |> flow_retention_named_value_refs()
      |> Map.values()
      |> Enum.map(&Map.get(&1, :ref))

    (direct_refs ++ named_refs)
    |> Enum.filter(&is_binary/1)
    |> Enum.uniq()
  end

  defp flow_retention_all_record_value_refs(_record), do: []

  defp flow_retention_owned_value_ref?(ref, record) when is_binary(ref) and is_map(record) do
    with id when is_binary(id) <- Map.get(record, :id) || Map.get(record, "id") do
      partition_key = Map.get(record, :partition_key) || Map.get(record, "partition_key")
      flow_retention_exact_owned_value_ref?(ref, id, partition_key)
    else
      _other -> false
    end
  end

  defp flow_retention_owned_value_ref?(_ref, _record), do: false

  defp flow_retention_exact_owned_value_ref?(ref, id, partition_key) do
    [:payload, :result, :error, :shared]
    |> Enum.any?(fn kind ->
      flow_retention_exact_owned_value_ref?(ref, id, partition_key, kind)
    end)
  end

  defp flow_retention_shareable_owned_value_ref?(ref, record)
       when is_binary(ref) and is_map(record) do
    with id when is_binary(id) <- Map.get(record, :id) || Map.get(record, "id") do
      partition_key = Map.get(record, :partition_key) || Map.get(record, "partition_key")
      flow_retention_exact_owned_value_ref?(ref, id, partition_key, :shared)
    else
      _other -> false
    end
  end

  defp flow_retention_shareable_owned_value_ref?(_ref, _record), do: false

  defp flow_retention_exact_owned_value_ref?(ref, id, partition_key, kind) do
    key = FlowKeys.value_key(id, kind, 0, partition_key)
    prefix = flow_retention_owned_value_ref_prefix(key)
    prefix_len = byte_size(prefix)

    if String.starts_with?(ref, prefix) do
      ref
      |> binary_part(prefix_len, byte_size(ref) - prefix_len)
      |> flow_retention_owned_value_suffix?(kind)
    else
      false
    end
  end

  defp flow_retention_owned_value_ref_prefix(key) when is_binary(key) do
    case :binary.matches(key, ":") do
      [] ->
        key

      matches ->
        {idx, 1} = List.last(matches)
        binary_part(key, 0, idx + 1)
    end
  end

  defp flow_retention_owned_value_suffix?(suffix, :shared) when is_binary(suffix) do
    flow_retention_value_version?(suffix) or
      case :binary.matches(suffix, ":") do
        [] ->
          false

        matches ->
          {idx, 1} = List.last(matches)
          version = binary_part(suffix, idx + 1, byte_size(suffix) - idx - 1)
          flow_retention_value_version?(version)
      end
  end

  defp flow_retention_owned_value_suffix?(suffix, _kind),
    do: flow_retention_value_version?(suffix)

  defp flow_retention_value_version?(version) when is_binary(version) do
    case Integer.parse(version) do
      {parsed, ""} when parsed >= 0 -> true
      _other -> false
    end
  end

  defp flow_retention_named_value_refs(%{} = record) do
    cond do
      Map.has_key?(record, :value_refs) ->
        flow_record_value_refs(record)

      refs = Map.get(record, "value_refs") ->
        flow_normalize_value_refs(refs)

      true ->
        %{}
    end
  end

  defp flow_retention_named_value_refs(_record), do: %{}

  defp flow_retention_history_value_refs(values) do
    values
    |> Enum.flat_map(fn
      value when is_binary(value) ->
        value
        |> Flow.decode_history_fields()
        |> flow_history_fields_to_map()
        |> flow_retention_record_value_refs()

      _other ->
        []
    end)
  end

  defp flow_owned_value_ref?(<<"f:{", rest::binary>>), do: :binary.match(rest, "}:v:") != :nomatch

  defp flow_owned_value_ref?(_ref), do: false

  defp flow_retention_delete_keys(state, keys) do
    keys
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> Enum.reduce_while({:ok, 0}, fn key, {:ok, count} ->
      case do_delete(state, key) do
        :ok -> {:cont, {:ok, count + 1}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp flow_retention_registry_key(%{id: id} = record) when is_binary(id) do
    FlowKeys.registry_key(id, Map.get(record, :partition_key))
  end

  defp flow_retention_registry_key(_record), do: nil

  defp flow_retention_merge_counts(left, right) do
    %{
      flows: Map.get(left, :flows, 0) + Map.get(right, :flows, 0),
      history: Map.get(left, :history, 0) + Map.get(right, :history, 0),
      values: Map.get(left, :values, 0) + Map.get(right, :values, 0)
    }
  end

  defp do_flow_rewind(state, %{id: id, to_event: to_event} = attrs) do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_key)

    with {:ok, record} <- flow_require_record(state, id, partition_key),
         :ok <- flow_require_rewindable(record),
         :ok <- flow_require_expected_state(record, Map.get(attrs, :expect_state)),
         {:ok, target_fields} <- flow_history_event_fields(state, record, to_event, partition_key),
         {:ok, next} <- flow_rewind_record(record, target_fields, attrs, now_ms) do
      next = Map.put(next, :rewound_to_event_id, to_event)

      with :ok <- flow_validate_record_keys(record),
           :ok <- flow_validate_record_keys(next),
           :ok <- flow_transition_move_indexes(state, [{record, next}]),
           :ok <- flow_refresh_record_value_expirations(state, next, %{}),
           state_key = FlowKeys.state_key(id, partition_key),
           :ok <- flow_put_state_record(state, state_key, next),
           :ok <- flow_queue_lmdb_reactivated_state_projection(state, state_key, next),
           :ok <- flow_history_put_planned(state, record, next, "rewound", now_ms),
           :ok <- flow_after_history_put(state, next) do
        :ok
      end
    end
  end

  defp flow_queue_lmdb_reactivated_state_projection(state, state_key, record)
       when is_binary(state_key) and is_map(record) do
    if flow_lmdb_projection_enabled?(state) and
         not Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) do
      queue_pending_lmdb_flow_state_projection(
        state_key,
        flow_encode(record),
        flow_record_expire_at(record)
      )
    end

    :ok
  end

  defp flow_prepare_claim_candidate_record(
         record,
         _id,
         type,
         state_filter,
         worker,
         lease_ms,
         now_ms,
         due_score
       ) do
    case record do
      nil ->
        :delete_due

      %{type: record_type} when record_type != type ->
        {:skip, flow_claim_restore_due_score(record, due_score)}

      %{state: record_state} = record ->
        cond do
          flow_claim_state_excluded?(state_filter, record_state) ->
            {:skip, flow_claim_restore_due_score(record, due_score)}

          not flow_claim_record_due_ready?(record, now_ms) ->
            {:skip, flow_claim_restore_due_score(record, due_score)}

          flow_claim_state_match?(state_filter, record_state) ->
            next_version = Map.fetch!(record, :version) + 1
            next_fencing_token = Map.get(record, :fencing_token, 0) + 1
            deadline_ms = now_ms + lease_ms

            token =
              worker <>
                ":" <> Integer.to_string(now_ms) <> ":" <> Integer.to_string(next_fencing_token)

            next =
              flow_claim_next_record(
                record,
                next_version,
                next_fencing_token,
                worker,
                token,
                deadline_ms,
                now_ms
              )

            with {:ok, from_due_score} <- flow_claim_numeric_score(due_score),
                 :ok <- flow_validate_claim_next_record_keys(next) do
              {:ok, record, next, from_due_score}
            else
              _ -> {:skip, flow_claim_restore_due_score(record, due_score)}
            end

          true ->
            :delete_due
        end

      _record ->
        :delete_due
    end
  end

  defp flow_claim_next_record(
         %{
           state: _state,
           version: _version,
           fencing_token: _fencing_token,
           updated_at_ms: _updated_at_ms,
           ttl_ms: _ttl_ms,
           retention_ttl_ms: _retention_ttl_ms,
           terminal_retention_until_ms: _terminal_retention_until_ms,
           history_hot_max_events: _history_hot_max_events,
           history_max_events: _history_max_events,
           lease_owner: _lease_owner,
           lease_token: _lease_token,
           lease_deadline_ms: _lease_deadline_ms,
           next_run_at_ms: _next_run_at_ms,
           run_state: _run_state
         } = record,
         next_version,
         next_fencing_token,
         worker,
         token,
         deadline_ms,
         now_ms
       ) do
    %{
      record
      | state: "running",
        version: next_version,
        fencing_token: next_fencing_token,
        updated_at_ms: now_ms,
        ttl_ms: nil,
        terminal_retention_until_ms: nil,
        lease_owner: worker,
        lease_token: token,
        lease_deadline_ms: deadline_ms,
        next_run_at_ms: deadline_ms,
        run_state: flow_claim_run_state(record)
    }
  end

  defp flow_claim_next_record(
         record,
         next_version,
         next_fencing_token,
         worker,
         token,
         deadline_ms,
         now_ms
       ) do
    Map.merge(record, %{
      state: "running",
      version: next_version,
      fencing_token: next_fencing_token,
      updated_at_ms: now_ms,
      ttl_ms: nil,
      retention_ttl_ms: Map.get(record, :retention_ttl_ms),
      terminal_retention_until_ms: nil,
      history_hot_max_events: Map.get(record, :history_hot_max_events),
      history_max_events: Map.get(record, :history_max_events),
      lease_owner: worker,
      lease_token: token,
      lease_deadline_ms: deadline_ms,
      next_run_at_ms: deadline_ms,
      run_state: flow_claim_run_state(record)
    })
  end

  defp flow_claim_state_excluded?({:exclude, _state_filter, exclude_states}, state),
    do: state in exclude_states

  defp flow_claim_state_excluded?(_state_filter, _state), do: false

  defp flow_claim_state_match?({:exclude, state_filter, _exclude_states}, state),
    do: flow_claim_state_match?(state_filter, state)

  defp flow_claim_state_match?(:any, state) when is_binary(state), do: true
  defp flow_claim_state_match?(states, state) when is_list(states), do: state in states
  defp flow_claim_state_match?(state, state), do: true
  defp flow_claim_state_match?(_state_filter, _state), do: false

  defp flow_claim_run_state(%{state: "running"} = record),
    do: Map.get(record, :run_state) || "queued"

  defp flow_claim_run_state(%{state: flow_state}), do: flow_state

  defp flow_claim_plan_pair(
         {:native_claim, next, _entry, _state_key, _value, _previous_history_ms}
       ),
       do: {next, next}

  defp flow_claim_plan_pair(
         {:native_claim, next, _entry, _state_key, _value, _previous_history_ms, _history_entry}
       ),
       do: {next, next}

  defp flow_claim_plan_pair({record, next, _from_due_score}), do: {record, next}
  defp flow_claim_plan_pair({record, next, _history_meta, _attrs}), do: {record, next}
  defp flow_claim_plan_pair({record, next}), do: {record, next}

  defp flow_claim_record_state_score(record),
    do: flow_claim_numeric_score(Map.get(record, :updated_at_ms, 0))

  defp flow_claim_record_due_ready?(record, now_ms) do
    with {:ok, due_score} <- flow_claim_numeric_score(Map.get(record, :next_run_at_ms)),
         {:ok, now_score} <- flow_claim_numeric_score(now_ms) do
      due_score <= now_score
    else
      _ -> false
    end
  end

  defp flow_claim_numeric_score(score) when is_float(score), do: {:ok, score}
  defp flow_claim_numeric_score(score) when is_integer(score), do: {:ok, score * 1.0}
  defp flow_claim_numeric_score(_score), do: :error

  defp flow_claim_restore_due_score(record, due_score) do
    case flow_claim_numeric_score(Map.get(record, :next_run_at_ms)) do
      {:ok, score} ->
        score

      :error ->
        case flow_claim_numeric_score(due_score) do
          {:ok, score} -> score
          :error -> 0.0
        end
    end
  end

    end
  end
end
