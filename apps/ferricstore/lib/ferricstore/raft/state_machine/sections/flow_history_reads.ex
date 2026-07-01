defmodule Ferricstore.Raft.StateMachine.Sections.FlowHistoryReads do
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

      defp flow_lmdb_cold_park_present?(state, key) do
        case flow_read_lmdb_cold_park_record(state, key) do
          {:ok, _record} -> true
          _ -> false
        end
      end

      defp flow_read_lmdb_cold_park_record(state, key) do
        park_key = Ferricstore.Flow.LMDB.cold_park_key_for_state_key(key)

        with {:ok, park_blob} <- Ferricstore.Flow.LMDB.get(flow_lmdb_record_path(state), park_key),
             {:ok, %{locator: %Locator{kind: :state} = locator} = park} <-
               Ferricstore.Flow.LMDB.decode_cold_park(park_blob),
             {:ok, value} <- flow_read_cold_park_state_value(state, key, locator, park),
             record when is_map(record) <- flow_decode_hot_state_value(value),
             true <- flow_locator_matches_record?(locator, record) do
          {:ok, record}
        else
          _ -> :miss
        end
      end

      defp flow_read_state_locator_value(
             state,
             key,
             %Locator{file_id: fid, offset: offset, value_size: value_size}
           )
           when valid_cold_location(fid, offset, value_size) do
        case ColdRead.pread_keyed(sm_file_path(state, fid), offset, key, @cold_read_timeout_ms) do
          {:ok, value} when is_binary(value) -> {:ok, value}
          _ -> :miss
        end
      end

      defp flow_read_state_locator_value(
             state,
             key,
             %Locator{file_id: fid, offset: offset, value_size: value_size}
           )
           when valid_waraft_segment_location(fid, offset, value_size) do
        case Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
               instance_ctx_for_state(state),
               state.shard_index,
               fid,
               key
             ) do
          {:ok, value} when is_binary(value) -> {:ok, value}
          _ -> :miss
        end
      end

      defp flow_read_state_locator_value(_state, _key, _locator), do: :miss

      defp flow_read_cold_park_state_value(state, key, %Locator{} = locator, park)
           when is_map(park) do
        case flow_read_state_locator_value(state, key, locator) do
          {:ok, value} ->
            {:ok, value}

          _ ->
            case Map.get(park, :state_value) do
              value when is_binary(value) -> {:ok, value}
              _ -> :miss
            end
        end
      end

      defp flow_locator_matches_record?(%Locator{} = locator, record) do
        Map.get(record, :id) == locator.flow_id and Map.get(record, :version) == locator.version
      end

      defp flow_decode_pending_lmdb_record(pending, key) do
        case Map.get(pending, key) do
          {:put, blob} -> flow_decode_lmdb_blob(blob)
          :delete -> :miss
          _ -> :miss
        end
      end

      defp flow_decode_lmdb_blob(blob) do
        case Ferricstore.Flow.LMDB.decode_value(blob, apply_now_ms()) do
          {:ok, value} ->
            flow_decode_record_blob(value)

          :expired ->
            :miss

          :error ->
            :miss
        end
      end

      defp flow_decode_record_blob(value) when is_binary(value) do
        try do
          {:ok, Flow.decode_record(value)}
        rescue
          _ -> :miss
        end
      end

      defp flow_decode_record_blob(_value), do: :miss

      defp flow_history_event_fields(state, %{id: id} = record, event_id, partition_key) do
        history_key = FlowKeys.history_key(id, partition_key)

        case flow_history_indexed_event_fields(state, record, history_key, event_id) do
          {:ok, _fields} = ok -> ok
          :trimmed -> {:error, "ERR flow rewind target event not found"}
          :miss -> flow_history_scanned_event_fields(state, record, history_key, event_id)
        end
      end

      defp flow_history_indexed_event_fields(state, record, history_key, event_id) do
        compound_key = FlowKeys.stream_entry_key_from_history_key(history_key, event_id)

        case flow_index_score_of(state, history_key, event_id) do
          {:ok, _score} ->
            case flow_history_lookup_value(state, compound_key) do
              {:hit, value, _expire_at_ms} ->
                {:ok, value |> flow_decode_history_fields(record) |> flow_history_fields_to_map()}

              _ ->
                :miss
            end

          :miss ->
            if flow_async_history_enabled?(state) do
              :miss
            else
              if flow_native_index(state) != nil, do: :trimmed, else: :miss
            end

          _other ->
            :miss
        end
      end

      defp flow_history_scanned_event_fields(state, record, history_key, event_id) do
        prefix = "X:" <> history_key <> <<0>>
        target_key = prefix <> event_id

        case HistoryProjector.scan_event_value(state.shard_data_path, target_key) do
          {:ok, value} ->
            {:ok, value |> flow_decode_history_fields(record) |> flow_history_fields_to_map()}

          _ ->
            state
            |> shard_ets_state()
            |> Ferricstore.Store.Shard.ETS.prefix_scan_entries(prefix, state.shard_data_path)
            |> Enum.find(fn {entry_id, _value} -> prefix <> entry_id == target_key end)
            |> case do
              {_entry_id, value} ->
                {:ok, value |> flow_decode_history_fields(record) |> flow_history_fields_to_map()}

              nil ->
                {:error, "ERR flow rewind target event not found"}
            end
        end
      end

      defp flow_history_lookup_value(state, key) do
        case :ets.lookup(state.ets, key) do
          [
            {^key, nil, expire_at_ms, _lfu, {:flow_history, _file_id} = file_id, offset,
             _value_size}
          ] ->
            case HistoryProjector.read_value(state.shard_data_path, file_id, offset) do
              {:ok, value} -> {:hit, value, expire_at_ms}
              _ -> :miss
            end

          _ ->
            ets_lookup(state, key)
        end
      rescue
        _ -> :miss
      end

      defp flow_decode_history_fields(value, context),
        do: Flow.decode_history_fields(value, context)

      defp flow_history_fields_to_map(fields) when is_list(fields) do
        fields
        |> Enum.chunk_every(2)
        |> Enum.reduce(%{}, fn
          [key, value], acc when is_binary(key) -> Map.put(acc, key, value)
          _pair, acc -> acc
        end)
      end

      defp flow_rewind_record(record, fields, attrs, now_ms) do
        with {:ok, target_state} <- flow_history_required_field(fields, "state"),
             {:ok, priority} <-
               flow_history_integer_field(fields, "priority", Map.get(record, :priority, 0)),
             {:ok, attempts} <-
               flow_history_integer_field(fields, "attempts", Map.get(record, :attempts, 0)),
             {:ok, history_run_at_ms} <-
               flow_history_optional_integer_field(fields, "next_run_at_ms"),
             {:ok, created_at_ms} <-
               flow_history_integer_field(
                 fields,
                 "created_at_ms",
                 Map.get(record, :created_at_ms, now_ms)
               ) do
          next_run_at_ms =
            case Map.get(attrs, :run_at_ms) do
              value when is_integer(value) -> value
              _ -> history_run_at_ms
            end

          {:ok,
           record
           |> Map.merge(%{
             state: target_state,
             version: Map.fetch!(record, :version) + 1,
             attempts: attempts,
             fencing_token: Map.get(record, :fencing_token, 0) + 1,
             created_at_ms: created_at_ms,
             updated_at_ms: now_ms,
             next_run_at_ms: next_run_at_ms,
             priority: priority,
             payload_ref: flow_nilable_history_field(fields, "payload_ref"),
             parent_flow_id: flow_nilable_history_field(fields, "parent_flow_id"),
             root_flow_id:
               flow_nilable_history_field(fields, "root_flow_id") || Map.get(record, :id),
             correlation_id: flow_nilable_history_field(fields, "correlation_id"),
             result_ref: flow_nilable_history_field(fields, "result_ref"),
             error_ref:
               Map.get(attrs, :reason_ref) || flow_nilable_history_field(fields, "error_ref"),
             value_refs: flow_history_named_value_refs_field(fields),
             lease_owner: nil,
             lease_token: nil,
             lease_deadline_ms: 0
           })
           |> flow_put_history_attributes(fields)
           |> flow_put_history_state_meta(fields)
           |> flow_stamp_terminal_retention(now_ms)}
        end
      end

      defp flow_history_required_field(fields, key) do
        case Map.get(fields, key) do
          value when is_binary(value) and value != "" -> {:ok, value}
          _ -> {:error, "ERR flow rewind target event cannot restore state"}
        end
      end

      defp flow_history_integer_field(fields, key, default) do
        case Map.get(fields, key) do
          nil -> {:ok, default}
          value -> flow_parse_history_integer(value)
        end
      end

      defp flow_history_optional_integer_field(fields, key) do
        case Map.get(fields, key) do
          nil -> {:ok, nil}
          "" -> {:ok, nil}
          value -> flow_parse_history_integer(value)
        end
      end

      defp flow_parse_history_integer(value) when is_integer(value), do: {:ok, value}

      defp flow_parse_history_integer(value) when is_binary(value) do
        case Integer.parse(value) do
          {int, ""} -> {:ok, int}
          _ -> {:error, "ERR flow rewind target event cannot restore state"}
        end
      end

      defp flow_parse_history_integer(_value),
        do: {:error, "ERR flow rewind target event cannot restore state"}

      defp flow_history_named_value_refs_field(fields) do
        fields
        |> Map.get("value_refs")
        |> flow_normalize_value_refs()
      end

      defp flow_put_history_attributes(record, fields) do
        case Map.get(fields, "attributes") do
          value when is_binary(value) and value != "" ->
            case Jason.decode(value) do
              {:ok, decoded} ->
                case Ferricstore.Flow.Attributes.normalize(decoded) do
                  {:ok, attrs} -> Ferricstore.Flow.Attributes.put_record(record, attrs)
                  {:error, _reason} -> record
                end

              {:error, _reason} ->
                record
            end

          _missing_or_empty ->
            record
        end
      end

      defp flow_put_history_state_meta(record, fields) do
        case Map.get(fields, "state_meta") do
          value when is_binary(value) and value != "" ->
            case Jason.decode(value) do
              {:ok, decoded} ->
                case Ferricstore.Flow.StateMeta.normalize(decoded) do
                  {:ok, state_meta} -> Ferricstore.Flow.StateMeta.put_record(record, state_meta)
                  {:error, _reason} -> record
                end

              {:error, _reason} ->
                record
            end

          _missing_or_empty ->
            record
        end
      end

      defp flow_nilable_history_field(fields, key) do
        case Map.get(fields, key) do
          "" -> nil
          value -> value
        end
      end

      defp flow_due_put(_state, %{next_run_at_ms: nil}), do: :ok

      defp flow_due_put(
             state,
             %{type: type, state: flow_state, priority: priority, id: id} = record
           ) do
        partition_key = Map.get(record, :partition_key)
        due_key = FlowKeys.due_key(type, flow_state, priority, partition_key)
        score = Map.fetch!(record, :next_run_at_ms)

        with :ok <- flow_index_put_new_lifecycle_members(state, due_key, [{id, score}]) do
          if flow_due_any_index_enabled?() do
            due_any_key = FlowKeys.due_any_key(type, priority, partition_key)
            flow_index_put_new_lifecycle_members(state, due_any_key, [{id, score}])
          else
            :ok
          end
        end
      end

      defp flow_zset_delete_members_from_key(_state, _due_key, []), do: :ok

      defp flow_zset_delete_members_from_key(state, due_key, ids) do
        flow_index_delete_lifecycle_members(state, due_key, Enum.uniq(ids))
      end

      defp flow_zset_index_delete_grouped(state, key_ids) do
        key_ids
        |> Enum.group_by(fn {key, _id} -> key end, fn {_key, id} -> id end)
        |> Enum.each(fn {key, ids} ->
          flow_zset_delete_many(state, key, Enum.uniq(ids))
        end)

        :ok
      end

      defp flow_zset_lifecycle_index_delete_grouped(_state, []), do: :ok

      defp flow_zset_lifecycle_index_delete_grouped(state, [{key, id}]) do
        flow_index_delete_lifecycle_members(state, key, [id])
      end

      defp flow_zset_lifecycle_index_delete_grouped(state, [{key, id1}, {key, id2}]) do
        ids = if id1 == id2, do: [id1], else: [id1, id2]
        flow_index_delete_lifecycle_members(state, key, ids)
      end

      defp flow_zset_lifecycle_index_delete_grouped(state, [{key1, id1}, {key2, id2}]) do
        flow_index_delete_lifecycle_members(state, key1, [id1])
        flow_index_delete_lifecycle_members(state, key2, [id2])
      end

      defp flow_zset_lifecycle_index_delete_grouped(state, key_ids) do
        key_ids
        |> Enum.group_by(fn {key, _id} -> key end, fn {_key, id} -> id end)
        |> Enum.each(fn {key, ids} ->
          flow_index_delete_lifecycle_members(state, key, Enum.uniq(ids))
        end)

        :ok
      end

      defp flow_index_put(state, %{id: id, type: type, state: flow_state} = record) do
        partition_key = Map.get(record, :partition_key)
        state_index_key = FlowKeys.state_index_key(type, flow_state, partition_key)
        updated_score = Map.get(record, :updated_at_ms, 0)

        with :ok <-
               flow_index_put_new_lifecycle_members(state, state_index_key, [{id, updated_score}]),
             :ok <- flow_metadata_index_put(state, record, updated_score) do
          flow_running_index_put(state, record)
        end
      end

      defp flow_metadata_index_put(state, record, _score) do
        flow_index_put_new_entries(state, flow_metadata_index_entries(record))
      end

      defp flow_metadata_index_entries(record) do
        id = Map.get(record, :id)
        parent_flow_id = Map.get(record, :parent_flow_id)
        root_flow_id = Map.get(record, :root_flow_id)
        correlation_id = Map.get(record, :correlation_id)

        if flow_metadata_index_empty?(parent_flow_id, root_flow_id, correlation_id, id) do
          []
        else
          partition_key = Map.get(record, :partition_key)
          score = Map.get(record, :updated_at_ms, 0)

          []
          |> flow_metadata_index_entry(:parent, parent_flow_id, partition_key, id, score)
          |> flow_metadata_index_entry(:root, root_flow_id, partition_key, id, score)
          |> flow_metadata_index_entry(:correlation, correlation_id, partition_key, id, score)
        end
      end

      defp flow_metadata_index_empty?(parent_flow_id, root_flow_id, correlation_id, id) do
        flow_blank_metadata?(parent_flow_id) and flow_blank_metadata?(correlation_id) and
          flow_blank_or_same_metadata?(root_flow_id, id)
      end

      defp flow_blank_metadata?(nil), do: true
      defp flow_blank_metadata?(""), do: true
      defp flow_blank_metadata?(_value), do: false

      defp flow_blank_or_same_metadata?(nil, _id), do: true
      defp flow_blank_or_same_metadata?("", _id), do: true
      defp flow_blank_or_same_metadata?(value, id), do: value == id

      defp flow_metadata_index_entry(entries, :root, nil, _partition_key, _id, _score),
        do: entries

      defp flow_metadata_index_entry(entries, :root, "", _partition_key, _id, _score), do: entries
      defp flow_metadata_index_entry(entries, :root, id, _partition_key, id, _score), do: entries

      defp flow_metadata_index_entry(entries, kind, value, partition_key, id, score)
           when is_binary(value) and value != "" do
        key =
          case kind do
            :parent -> FlowKeys.parent_index_key(value, partition_key)
            :root -> FlowKeys.root_index_key(value, partition_key)
            :correlation -> FlowKeys.correlation_index_key(value, partition_key)
          end

        [{key, id, score} | entries]
      end

      defp flow_metadata_index_entry(entries, _kind, _value, _partition_key, _id, _score),
        do: entries

      defp flow_metadata_index_entries_with_tag(record, tag) do
        id = Map.get(record, :id)
        parent_flow_id = Map.get(record, :parent_flow_id)
        root_flow_id = Map.get(record, :root_flow_id)
        correlation_id = Map.get(record, :correlation_id)

        if flow_metadata_index_empty?(parent_flow_id, root_flow_id, correlation_id, id) do
          []
        else
          score = Map.get(record, :updated_at_ms, 0)

          []
          |> flow_metadata_index_entry_with_tag(:parent, parent_flow_id, tag, id, score)
          |> flow_metadata_index_entry_with_tag(:root, root_flow_id, tag, id, score)
          |> flow_metadata_index_entry_with_tag(:correlation, correlation_id, tag, id, score)
        end
      end

      defp flow_metadata_index_entry_with_tag(entries, :root, nil, _tag, _id, _score), do: entries
      defp flow_metadata_index_entry_with_tag(entries, :root, "", _tag, _id, _score), do: entries
      defp flow_metadata_index_entry_with_tag(entries, :root, id, _tag, id, _score), do: entries

      defp flow_metadata_index_entry_with_tag(entries, kind, value, tag, id, score)
           when is_binary(value) and value != "" do
        key =
          case kind do
            :parent -> flow_parent_index_key_with_tag(tag, value)
            :root -> flow_root_index_key_with_tag(tag, value)
            :correlation -> flow_correlation_index_key_with_tag(tag, value)
          end

        [{key, id, score} | entries]
      end

      defp flow_metadata_index_entry_with_tag(entries, _kind, _value, _tag, _id, _score),
        do: entries

      defp flow_running_index_put(state, %{state: "running", id: id, type: type} = record) do
        partition_key = Map.get(record, :partition_key)
        lease_score = Map.get(record, :lease_deadline_ms, 0)
        inflight_index_key = FlowKeys.inflight_index_key(type, partition_key)

        worker_index_key =
          FlowKeys.worker_index_key(Map.get(record, :lease_owner, ""), partition_key)

        with :ok <-
               flow_index_put_new_lifecycle_members(state, inflight_index_key, [{id, lease_score}]) do
          flow_index_put_new_lifecycle_members(state, worker_index_key, [{id, lease_score}])
        end
      end

      defp flow_running_index_put(_state, _record), do: :ok

      defp flow_due_put_many_new(_state, []), do: :ok

      defp flow_due_put_many_new(state, records) do
        flow_due_put_many_with(state, records, &flow_index_put_new_lifecycle_members/3)
      end

      defp flow_due_put_many_with(state, records, put_fun) do
        records
        |> Enum.flat_map(fn record ->
          partition_key = Map.get(record, :partition_key)

          entries = [
            {FlowKeys.due_key(record.type, record.state, record.priority, partition_key), record}
          ]

          if flow_due_any_index_enabled?() do
            [
              {FlowKeys.due_any_key(record.type, record.priority, partition_key), record}
              | entries
            ]
          else
            entries
          end
        end)
        |> Enum.group_by(
          fn {due_key, _record} ->
            due_key
          end,
          fn {_due_key, record} ->
            record
          end
        )
        |> Enum.each(fn {due_key, due_records} ->
          member_score_pairs =
            Enum.map(due_records, fn record ->
              {record.id, Map.fetch!(record, :next_run_at_ms)}
            end)

          put_fun.(state, due_key, member_score_pairs)
        end)

        :ok
      end

      defp flow_ensure_due_index_ready(_state, _due_key), do: :ok

      defp flow_native_index(%{flow_index_name: index, flow_lookup_name: lookup})
           when index != nil and lookup != nil do
        NativeFlowIndex.get(index, lookup)
      end

      defp flow_native_index(_state), do: nil

      defp ensure_flow_native_index_registered(
             %{flow_index_name: index, flow_lookup_name: lookup} = state
           )
           when index != nil and lookup != nil do
        case NativeFlowIndex.get(index, lookup) do
          nil -> NativeFlowIndex.register(index, lookup, NativeFlowIndex.new())
          _native -> :ok
        end

        state
      end

      defp ensure_flow_native_index_registered(state), do: state

      defp flow_claim_index_count_keys(state) do
        case flow_native_index(state) do
          nil -> []
          native -> NativeFlowIndex.due_count_keys(native)
        end
      end

      defp flow_index_rank_range(state, key, start_idx, stop_idx, reverse?) do
        case flow_native_index(state) do
          nil -> []
          native -> NativeFlowIndex.rank_range(native, key, start_idx, stop_idx, reverse?)
        end
      end

      defp flow_index_count_all(state, key) do
        case flow_native_index(state) do
          nil -> 0
          native -> NativeFlowIndex.count_all(native, key)
        end
      end

      defp flow_index_score_of(state, key, member) do
        case flow_native_index(state) do
          nil ->
            :miss

          native ->
            NativeFlowIndex.score_of(native, key, member)
        end
      end

      defp flow_zset_put_many_new(
             state,
             due_key,
             member_score_pairs
           ) do
        flow_index_put_new_members(state, due_key, member_score_pairs)
      end

      defp flow_zset_delete_many(
             state,
             due_key,
             ids
           ) do
        flow_index_delete_members(state, due_key, ids)
      end

      defp flow_index_put_members(state, key, member_score_pairs) do
        flow_native_put_members(state, key, member_score_pairs)
      end

      defp flow_index_put_new_members(state, key, member_score_pairs) do
        flow_native_put_new_members(state, key, member_score_pairs)
      end

      defp flow_index_put_new_lifecycle_members(state, key, member_score_pairs) do
        flow_native_put_new_members(state, key, member_score_pairs)
      end

      defp flow_index_put_new_entries(state, key_member_score_triples) do
        flow_native_put_new_entries(state, key_member_score_triples)
      end

      defp flow_index_move_entries(state, key_key_member_score_quads) do
        flow_native_move_entries(state, key_key_member_score_quads)
      end

      defp flow_index_move_lifecycle_entries(state, [
             {_from_key, _to_key, _member, _score} = entry
           ]) do
        flow_native_move_entries(state, [entry])
      end

      defp flow_index_move_lifecycle_entries(state, key_key_member_score_quads) do
        flow_native_move_entries(state, key_key_member_score_quads)
      end

      defp flow_index_delete_members(state, key, members) do
        flow_native_delete_members(state, key, members)
      end

      defp flow_index_delete_lifecycle_members(state, key, members) do
        flow_native_delete_members(state, key, members)
      end

      defp flow_native_put_members(_state, _key, []), do: :ok

      defp flow_native_put_members(state, key, member_score_pairs) do
        case flow_native_index(state) do
          nil ->
            {:error, :flow_native_index_unavailable}

          native ->
            entries = Enum.map(member_score_pairs, fn {member, score} -> {key, member, score} end)
            flow_native_apply_or_queue(native, {:put_entries, entries})
        end
      end

      defp flow_native_put_new_members(_state, _key, []), do: :ok

      defp flow_native_put_new_members(state, key, member_score_pairs) do
        case flow_native_index(state) do
          nil ->
            {:error, :flow_native_index_unavailable}

          native ->
            entries = Enum.map(member_score_pairs, fn {member, score} -> {key, member, score} end)
            flow_native_apply_or_queue(native, {:put_new_entries, entries})
        end
      end

      defp flow_native_put_new_entries(_state, []), do: :ok

      defp flow_native_put_new_entries(state, key_member_score_triples) do
        case flow_native_index(state) do
          nil ->
            {:error, :flow_native_index_unavailable}

          native ->
            flow_native_apply_or_queue(native, {:put_new_entries, key_member_score_triples})
        end
      end

      defp flow_native_move_entries(_state, []), do: :ok

      defp flow_native_move_entries(state, key_key_member_score_quads) do
        case flow_native_index(state) do
          nil ->
            {:error, :flow_native_index_unavailable}

          native ->
            flow_native_apply_or_queue(native, {:move_entries, key_key_member_score_quads})
        end
      end

      defp flow_native_delete_members(_state, _key, []), do: :ok

      defp flow_native_delete_members(state, key, members) do
        case flow_native_index(state) do
          nil -> {:error, :flow_native_index_unavailable}
          native -> flow_native_apply_or_queue(native, {:delete_members, key, members})
        end
      end

      defp flow_native_apply_claim_entries(_state, []), do: :ok

      defp flow_native_apply_claim_entries(state, entries) do
        case flow_native_index(state) do
          nil -> {:error, :flow_native_index_unavailable}
          native -> flow_native_apply_or_queue(native, {:apply_claim_entries, entries})
        end
      end

      defp flow_native_apply_or_queue(native, op) do
        case Process.get(:sm_pending_flow_native_ops, :undefined) do
          ops when is_list(ops) ->
            Process.put(:sm_pending_flow_native_ops, [{native, op} | ops])

          _ ->
            NativeFlowIndex.apply_batch(native, [op])
        end

        :ok
      end

      defp rollback_pending_flow_indexes(state) do
        if Process.get(:sm_pending_flow_native_flush?, false) do
          reset_flow_native_index_from_keydir(state)
        else
          :ok
        end
      end

      defp reset_flow_native_index_from_keydir(
             %{flow_index_name: index, flow_lookup_name: lookup} = state
           )
           when index != nil and lookup != nil do
        NativeFlowIndex.reset(index, lookup)

        Ferricstore.Flow.LMDBRebuilder.rebuild_active_indexes_from_keydir(
          state.shard_data_path,
          state.ets,
          state.shard_index,
          Map.get(state, :instance_ctx),
          nil,
          nil,
          index,
          lookup
        )
      end

      defp reset_flow_native_index_from_keydir(_state), do: :ok

      defp flow_history_put(state, record, event, now_ms) do
        flow_history_put_ready(state, record, event, now_ms)
      end

      defp flow_history_put_planned(state, previous, record, event, now_ms) do
        flow_history_put_ready(state, record, event, now_ms, flow_previous_history_ms(previous))
      end

      defp flow_history_put_planned(state, previous, record, event, now_ms, meta) do
        flow_history_put_ready(
          state,
          record,
          event,
          now_ms,
          flow_previous_history_ms(previous),
          meta
        )
      end

      defp flow_history_put_ready(state, record, event, now_ms) do
        flow_history_put_ready(state, record, event, now_ms, nil)
      end

      defp flow_history_put_ready(state, record, event, now_ms, previous_history_ms) do
        flow_history_put_ready(state, record, event, now_ms, previous_history_ms, %{})
      end

      defp flow_history_put_ready(
             state,
             %{id: id, version: version} = record,
             event,
             now_ms,
             previous_history_ms,
             meta
           ) do
        partition_key = Map.get(record, :partition_key)
        history_key = FlowKeys.history_key(id, partition_key)

        {event_id, event_ms} =
          flow_history_next_event(state, history_key, now_ms, version, previous_history_ms)

        compound_key = FlowKeys.stream_entry_key_from_history_key(history_key, event_id)

        entry =
          %{
            key: compound_key,
            expire_at_ms: 0,
            history_key: history_key,
            event_id: event_id,
            event_ms: event_ms,
            version: version,
            history_hot_max_events: Map.get(record, :history_hot_max_events),
            history_max_events: Map.get(record, :history_max_events),
            terminal?: flow_terminal_record?(record),
            value: {:flow_history_fields, record, event, now_ms, meta}
          }
          |> flow_history_maybe_put_hot_evict_event_ids(
            flow_history_hot_evict_event_ids(record, event_id, version, previous_history_ms)
          )

        with :ok <-
               flow_history_put_or_queue_entry(state, entry) do
          if flow_async_history_enabled?(state) do
            :ok
          else
            flow_history_index_put(state, history_key, event_id, event_ms, version)
          end
        end
      end

      defp flow_history_next_event(_state, _history_key, now_ms, 1, _previous_history_ms) do
        {Integer.to_string(trunc(now_ms)) <> "-1", trunc(now_ms)}
      end

      defp flow_history_next_event(_state, _history_key, now_ms, version, previous_history_ms)
           when is_integer(previous_history_ms) do
        ms = max(trunc(now_ms), previous_history_ms)
        {Integer.to_string(ms) <> "-" <> Integer.to_string(version), ms}
      end

      defp flow_history_next_event(state, history_key, now_ms, version, _previous_history_ms) do
        ms =
          case flow_index_rank_range(state, history_key, 0, 0, true) do
            [{_event_id, last_ms}] when is_number(last_ms) and last_ms > now_ms ->
              last_ms

            _ ->
              now_ms
          end

        {Integer.to_string(trunc(ms)) <> "-" <> Integer.to_string(version), trunc(ms)}
      end

      defp flow_previous_history_ms(%{updated_at_ms: updated_at_ms})
           when is_integer(updated_at_ms),
           do: updated_at_ms

      defp flow_previous_history_ms(%{created_at_ms: created_at_ms})
           when is_integer(created_at_ms),
           do: created_at_ms

      defp flow_previous_history_ms(_record), do: nil

      defp flow_history_put_or_queue_entry(state, entry) do
        if Process.get(:sm_pending_flow_history_projections, :undefined) == :undefined do
          ra_index = current_ra_index()

          case HistoryProjector.enqueue_async(
                 instance_ctx_for_state(state),
                 state.shard_index,
                 [entry],
                 ra_index
               ) do
            :ok ->
              record_waraft_replay_dependency(:history, state.shard_index, ra_index)
              :ok

            {:error, reason} ->
              {:error, {:flow_history_projection_failed, reason}}
          end
        else
          queue_pending_flow_history_projection(entry)
        end
      end

      defp flow_async_history?(_state), do: true

      defp flow_async_history_enabled?(_state), do: true

      defp flow_with_forced_async_history(fun) when is_function(fun, 0) do
        fun.()
      end

      defp flow_history_index_put(state, history_key, event_id, ms, 1) do
        flow_index_put_new_members(state, history_key, [{event_id, ms}])
        :ok
      end

      defp flow_history_index_put(state, history_key, event_id, ms, _version) do
        flow_index_put_members(state, history_key, [{event_id, ms}])
        :ok
      end

      defp flow_history_index_put_entries(_state, []), do: :ok

      defp flow_history_index_put_entries(state, entries) do
        if flow_async_history_enabled?(state) do
          :ok
        else
          flow_index_put_new_entries(state, entries)
        end
      end

      defp flow_history_trim(_state, %{history_max_events: nil}), do: :ok
      defp flow_history_trim(_state, %{history_max_events: max}) when not is_integer(max), do: :ok

      defp flow_history_trim(_state, %{history_max_events: max, version: version})
           when is_integer(version) and version <= max,
           do: :ok

      defp flow_history_trim(state, %{id: id, history_max_events: max} = record) when max > 0 do
        partition_key = Map.get(record, :partition_key)
        history_key = FlowKeys.history_key(id, partition_key)

        case flow_index_count_all(state, history_key) do
          len when len > max ->
            delete_count = len - max
            flow_history_trim_oldest(state, record, id, partition_key, history_key, delete_count)

          _ ->
            :ok
        end
      end

      defp flow_history_trim(_state, _record), do: :ok

      defp flow_after_history_put_many(records, state) do
        Enum.reduce_while(records, :ok, fn record, :ok ->
          case flow_after_history_put(state, record) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp flow_after_history_put_records_batch(_state, []), do: :ok

      defp flow_after_history_put_records_batch(state, records) do
        lmdb_mirror? = flow_lmdb_projection_enabled?(state)

        if Enum.all?(records, &flow_after_history_fast_record?(lmdb_mirror?, &1)) do
          :ok
        else
          flow_after_history_put_many(records, state)
        end
      end
    end
  end
end
