defmodule Ferricstore.Raft.StateMachine.Sections.FlowRetentionState do
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

  defp flow_retention_state_key_owned_by_shard?(state, state_key) when is_binary(state_key) do
    case instance_ctx_for_state(state) do
      nil ->
        true

      _ctx ->
        true
    end
  rescue
    _ -> false
  end

  defp flow_retention_state_key_owned_by_shard?(_state, _state_key), do: false

  defp flow_retention_cleanup_lmdb_state_key(state, state_key, now_ms) do
    case flow_retention_decode_lmdb_state_record(state, state_key) do
      {:ok, lmdb_record} ->
        if flow_retention_expired_terminal_record?(lmdb_record, now_ms) do
          case flow_retention_current_state_record(state, state_key) do
            {:ok, current_record} ->
              if flow_retention_expired_terminal_record?(current_record, now_ms) do
                flow_retention_cleanup_record(state, state_key, current_record)
              else
                flow_retention_zero_counts()
              end

            :miss ->
              if flow_retention_lmdb_projection_pending?(state) do
                flow_retention_zero_counts()
              else
                flow_retention_cleanup_record(state, state_key, lmdb_record)
              end
          end
        else
          flow_retention_zero_counts()
        end

      :miss ->
        flow_retention_zero_counts()
    end
  end

  defp flow_retention_zero_counts, do: {:ok, %{flows: 0, history: 0, values: 0}}

  defp flow_retention_current_state_record(state, state_key) do
    case :ets.lookup(state.ets, state_key) do
      [{^state_key, value, _expire_at_ms, _lfu, fid, offset, value_size}] ->
        flow_retention_decode_state_record(state, state_key, value, fid, offset, value_size)

      _other ->
        :miss
    end
  rescue
    ArgumentError -> :miss
  end

  defp flow_retention_expired_terminal_record?(record, now_ms) do
    Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) and
      case Map.get(record, :terminal_retention_until_ms) do
        expire_at_ms when is_integer(expire_at_ms) and expire_at_ms <= now_ms -> true
        _other -> false
      end
  end

  defp flow_retention_decode_lmdb_state_record(state, state_key) do
    case Ferricstore.Flow.LMDB.get(flow_lmdb_record_path(state), state_key) do
      {:ok, blob} ->
        flow_retention_decode_lmdb_state_value(blob)

      _ ->
        :miss
    end
  end

  defp flow_retention_decode_lmdb_state_value(blob) when is_binary(blob) do
    case Ferricstore.Flow.LMDB.decode_value(blob, 0) do
      {:ok, value} -> flow_decode_record_blob(value)
      _ -> :miss
    end
  end

  defp flow_retention_decode_lmdb_state_value(_blob), do: :miss

  defp flow_retention_expired_state_entries(state, now_ms, limit) do
    prefix = "f:{"
    prefix_len = byte_size(prefix)

    match_spec = [
      {{:"$1", :"$2", :"$3", :_, :"$5", :"$6", :"$7"},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
       ], [{{:"$1", :"$2", :"$3", :"$5", :"$6", :"$7"}}]}
    ]

    state.ets
    |> safe_ets_select(match_spec)
    |> Enum.filter(fn {key, value, _expire_at_ms, fid, offset, value_size} ->
      FlowKeys.state_key?(key) and
        case flow_retention_decode_state_record(state, key, value, fid, offset, value_size) do
          {:ok, record} -> flow_retention_expired_terminal_record?(record, now_ms)
          :miss -> false
        end
    end)
    |> Enum.take(limit)
  end

  defp flow_retention_cleanup_entry(
         state,
         {state_key, value, _expire_at_ms, fid, offset, value_size}
       ) do
    case flow_retention_decode_state_record(state, state_key, value, fid, offset, value_size) do
      {:ok, record} ->
        if Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) do
          flow_retention_cleanup_record(state, state_key, record)
        else
          {:ok, %{flows: 0, history: 0, values: 0}}
        end

      :miss ->
        {:ok, %{flows: 0, history: 0, values: 0}}
    end
  end

  defp flow_retention_decode_state_record(_state, _key, value, _fid, _offset, _value_size)
       when is_binary(value) do
    flow_decode_record_blob(value)
  end

  defp flow_retention_decode_state_record(state, key, nil, fid, offset, value_size)
       when valid_cold_location(fid, offset, value_size) or
              valid_waraft_segment_location(fid, offset, value_size) do
    case flow_retention_read_state_value(state, key, fid, offset, value_size) do
      {:ok, value} ->
        flow_decode_record_blob(value)

      _other ->
        :miss
    end
  end

  defp flow_retention_decode_state_record(_state, _key, _value, _fid, _offset, _value_size),
    do: :miss

  defp flow_retention_read_state_value(state, key, fid, offset, value_size)
       when valid_cold_location(fid, offset, value_size) do
    state
    |> sm_file_path(fid)
    |> Ferricstore.Store.ColdRead.pread_keyed(offset, key, @cold_read_timeout_ms)
    |> case do
      {:ok, value} when is_binary(value) -> flow_retention_materialize_state_value(state, value)
      _other -> :miss
    end
  end

  defp flow_retention_read_state_value(state, key, fid, _offset, value_size)
       when valid_waraft_segment_location(fid, 0, value_size) do
    state
    |> instance_ctx_for_state()
    |> Ferricstore.Raft.WARaftSegmentReader.read_value_from_location_including_expired(
      state.shard_index,
      fid,
      key
    )
    |> case do
      {:ok, value} when is_binary(value) -> flow_retention_materialize_state_value(state, value)
      _other -> :miss
    end
  end

  defp flow_retention_read_state_value(_state, _key, _fid, _offset, _value_size), do: :miss

  defp flow_retention_materialize_state_value(state, value) when is_binary(value) do
    case materialize_cold_blob_value(state, value) do
      {:ok, materialized} when is_binary(materialized) -> {:ok, materialized}
      _other -> :miss
    end
  end

  defp flow_retention_cleanup_record(state, state_key, record) do
    if flow_retention_keydir_available?(state) do
      history_key = FlowKeys.history_key(Map.fetch!(record, :id), Map.get(record, :partition_key))

      with {:ok, history_entries, history_complete?} <-
             flow_retention_history_entries(state, history_key) do
        history_keys = Enum.map(history_entries, &flow_retention_history_entry_key/1)
        history_values = flow_retention_history_values(state, history_entries)
        history_value_refs = flow_retention_history_value_refs(history_values) |> Enum.uniq()

        flow_retention_cleanup_record_after_history_page(
          state,
          state_key,
          record,
          history_key,
          history_entries,
          history_keys,
          history_value_refs,
          history_complete?
        )
      end
    else
      flow_retention_zero_counts()
    end
  end

  defp flow_retention_cleanup_record_after_history_page(
         state,
         _state_key,
         _record,
         history_key,
         history_entries,
         history_keys,
         _history_value_refs,
         false
       ) do
    with :ok <- flow_retention_delete_history_index(state, history_key, history_entries),
         {:ok, history_count} <- flow_retention_delete_keys(state, history_keys) do
      {:ok, %{flows: 0, history: history_count, values: 0}}
    end
  end

  defp flow_retention_cleanup_record_after_history_page(
         state,
         state_key,
         record,
         history_key,
         history_entries,
         history_keys,
         history_value_refs,
         true
       ) do
    {owned_value_keys, owned_values_complete?} =
      flow_retention_owned_value_keys_page(state, record)

    value_refs =
      if owned_values_complete? do
        shared_value_links = flow_retention_shared_value_links(state, record)
        shared_value_refs = Enum.map(shared_value_links, fn {_key, ref} -> ref end)

        record
        |> flow_retention_record_value_refs()
        |> Kernel.++(history_value_refs)
        |> Kernel.++(owned_value_keys)
        |> Kernel.++(shared_value_refs)
      else
        owned_value_keys
      end

    value_refs =
      value_refs
      |> flow_retention_deletable_owned_value_refs(state, record)

    if owned_values_complete? do
      shared_value_links = flow_retention_shared_value_links(state, record)
      shared_link_keys = Enum.map(shared_value_links, fn {key, _ref} -> key end)
      registry_key = flow_retention_registry_key(record)

      with :ok <- flow_retention_delete_history_index(state, history_key, history_entries),
           {:ok, history_count} <- flow_retention_delete_keys(state, history_keys),
           {:ok, values_count} <- flow_retention_delete_keys(state, value_refs),
           {:ok, _shared_link_count} <- flow_retention_delete_keys(state, shared_link_keys),
           {:ok, _registry_count} <- flow_retention_delete_keys(state, [registry_key]),
           :ok <- do_delete(state, state_key) do
        maybe_queue_terminal_lmdb_index_delete(state, record)
        queue_lmdb_metadata_index_deletes(state, record)

        {:ok, %{flows: 1, history: history_count, values: values_count}}
      end
    else
      with :ok <- flow_retention_delete_history_index(state, history_key, history_entries),
           {:ok, history_count} <- flow_retention_delete_keys(state, history_keys),
           {:ok, values_count} <- flow_retention_delete_keys(state, value_refs) do
        {:ok, %{flows: 0, history: history_count, values: values_count}}
      end
    end
  end

  defp flow_retention_deletable_owned_value_refs(refs, state, owner_record) do
    refs =
      refs
      |> Enum.filter(&is_binary/1)
      |> Enum.uniq()

    case refs do
      [] ->
        []

      refs ->
        {shareable_refs, private_refs} =
          Enum.split_with(refs, &flow_retention_shareable_owned_value_ref?(&1, owner_record))

        referenced =
          case shareable_refs do
            [] ->
              MapSet.new()

            shareable_refs ->
              # Payload/result/error refs are private generated values. Only
              # owner-named shared refs are allowed to be reused by another
              # Flow, so broad reference scans stay off the common cleanup path.
              state
              |> flow_retention_value_refs_used_by_other_states(
                owner_record,
                MapSet.new(shareable_refs)
              )
          end

        private_refs ++ Enum.reject(shareable_refs, &MapSet.member?(referenced, &1))
    end
  end

  defp flow_retention_value_refs_used_by_other_states(state, owner_record, target_refs) do
    maybe_run_flow_retention_reference_scan_hook(owner_record, target_refs)

    state
    |> flow_retention_reference_scan_states()
    |> Enum.reduce_while(MapSet.new(), fn scan_state, referenced ->
      referenced =
        target_refs
        |> flow_retention_value_refs_used_by_other_ets_states(
          scan_state,
          owner_record,
          referenced
        )
        |> then(fn referenced ->
          if MapSet.size(referenced) >= MapSet.size(target_refs) do
            referenced
          else
            flow_retention_value_refs_used_by_other_lmdb_states(
              scan_state,
              owner_record,
              target_refs,
              referenced
            )
          end
        end)

      if MapSet.size(referenced) >= MapSet.size(target_refs) do
        {:halt, referenced}
      else
        {:cont, referenced}
      end
    end)
    |> then(fn referenced ->
      if MapSet.size(referenced) >= MapSet.size(target_refs) do
        referenced
      else
        flow_retention_value_refs_used_by_other_histories(
          state,
          owner_record,
          target_refs,
          referenced
        )
      end
    end)
  end

  defp maybe_run_flow_retention_reference_scan_hook(owner_record, target_refs) do
    case Application.get_env(:ferricstore, :flow_retention_reference_scan_hook) do
      hook when is_function(hook, 2) -> hook.(owner_record, MapSet.to_list(target_refs))
      _other -> :ok
    end
  end

  defp flow_retention_reference_scan_states(state) do
    case instance_ctx_for_state(state) do
      %{shard_count: shard_count, keydir_refs: keydir_refs, data_dir: data_dir} = ctx
      when is_integer(shard_count) and shard_count > 0 and is_tuple(keydir_refs) ->
        0..(shard_count - 1)
        |> Enum.map(fn shard_index ->
          if shard_index == state.shard_index do
            state
          else
            shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)

            state
            |> Map.put(:shard_index, shard_index)
            |> Map.put(:shard_data_path, shard_data_path)
            |> Map.put(:shard_data_path_expanded, Path.expand(shard_data_path))
            |> Map.put(:ets, elem(ctx.keydir_refs, shard_index))
            |> Map.put(:flow_lmdb_path, Ferricstore.Flow.LMDB.path(shard_data_path))
          end
        end)

      _other ->
        [state]
    end
  end

  defp flow_retention_lmdb_projection_state(state) do
    path = flow_lmdb_record_path(state)

    cond do
      Ferricstore.Flow.LMDB.env_present?(path) ->
        :available

      flow_retention_keydir_has_flow_entries?(state) ->
        :unavailable

      true ->
        :empty
    end
  end

  defp flow_retention_keydir_has_flow_entries?(state) do
    Enum.any?(["f:{f", "X:f:{"], fn prefix ->
      {keys, _complete?} = flow_retention_keys_with_prefix_page(state, prefix, 1)
      keys != []
    end)
  end

  defp flow_retention_value_refs_used_by_other_ets_states(
         target_refs,
         state,
         owner_record,
         referenced
       ) do
    prefix = "f:{f"
    prefix_len = byte_size(prefix)

    match_spec = [
      {{:"$1", :"$2", :_, :_, :"$5", :"$6", :"$7"},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
       ], [{{:"$1", :"$2", :"$5", :"$6", :"$7"}}]}
    ]

    state.ets
    |> safe_ets_select(match_spec)
    |> Enum.reduce_while(referenced, fn {key, value, fid, offset, value_size}, acc ->
      acc =
        flow_retention_value_refs_used_by_state_entry(
          state,
          owner_record,
          target_refs,
          acc,
          key,
          value,
          fid,
          offset,
          value_size
        )

      if MapSet.size(acc) >= MapSet.size(target_refs), do: {:halt, acc}, else: {:cont, acc}
    end)
  end

  defp flow_retention_value_refs_used_by_state_entry(
         state,
         owner_record,
         target_refs,
         referenced,
         state_key,
         value,
         fid,
         offset,
         value_size
       ) do
    if FlowKeys.state_key?(state_key) do
      case flow_retention_decode_state_record(state, state_key, value, fid, offset, value_size) do
        {:ok, record} ->
          flow_retention_value_refs_used_by_record(record, owner_record, target_refs, referenced)

        :miss ->
          referenced
      end
    else
      referenced
    end
  end

  defp flow_retention_value_refs_used_by_record(record, owner_record, target_refs, referenced) do
    if flow_retention_same_flow_record?(record, owner_record) do
      referenced
    else
      record
      |> flow_retention_all_record_value_refs()
      |> Enum.reduce(referenced, fn ref, acc ->
        if MapSet.member?(target_refs, ref), do: MapSet.put(acc, ref), else: acc
      end)
    end
  end

  defp flow_retention_value_refs_used_by_other_lmdb_states(
         state,
       owner_record,
       target_refs,
       referenced
     ) do
    prefix = "f:{"
    limit = flow_retention_value_lmdb_scan_limit()
    path = flow_lmdb_record_path(state)

    case flow_retention_lmdb_projection_state(state) do
      :available ->
        flow_retention_value_refs_used_by_lmdb_states_after(
          path,
          prefix,
          <<>>,
          limit,
          state,
          owner_record,
          target_refs,
          referenced
        )

      :empty ->
        referenced

      :unavailable ->
        MapSet.union(referenced, target_refs)
    end
  end

  defp flow_retention_value_refs_used_by_lmdb_states_after(
         _path,
         _prefix,
         _after_key,
         limit,
         _state,
         _owner_record,
         target_refs,
         referenced
       )
       when limit <= 0,
       do: MapSet.union(referenced, target_refs)

  defp flow_retention_value_refs_used_by_lmdb_states_after(
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
          Enum.reduce_while(entries, referenced, fn {key, lmdb_value}, acc ->
            acc =
              if FlowKeys.state_key?(key) do
                case flow_retention_decode_lmdb_state_value(lmdb_value) do
                  {:ok, record} ->
                    flow_retention_value_refs_used_by_record(
                      record,
                      owner_record,
                      target_refs,
                      acc
                    )

                  :miss ->
                    acc
                end
              else
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

            flow_retention_value_refs_used_by_lmdb_states_after(
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

  defp flow_retention_value_refs_used_by_other_histories(
         state,
         owner_record,
         target_refs,
         referenced
       ) do
    state
    |> flow_retention_reference_scan_states()
    |> Enum.reduce_while(referenced, fn scan_state, referenced ->
      referenced =
        flow_retention_value_refs_used_by_other_ets_histories(
          scan_state,
          owner_record,
          target_refs,
          referenced
        )

      referenced =
        if MapSet.size(referenced) >= MapSet.size(target_refs) do
          referenced
        else
          flow_retention_value_refs_used_by_other_lmdb_histories(
            scan_state,
            owner_record,
            target_refs,
            referenced
          )
        end

      if MapSet.size(referenced) >= MapSet.size(target_refs) do
        {:halt, referenced}
      else
        {:cont, referenced}
      end
    end)
  end

  defp flow_retention_value_refs_used_by_other_ets_histories(
         state,
         owner_record,
         target_refs,
         referenced
       ) do
    prefix = "X:f:{"
    prefix_len = byte_size(prefix)

    match_spec = [
      {{:"$1", :"$2", :_, :_, :"$5", :"$6", :"$7"},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
       ], [{{:"$1", :"$2", :"$5", :"$6", :"$7"}}]}
    ]

    limit = flow_retention_history_lmdb_scan_limit()

    flow_retention_value_refs_used_by_ets_history_page(
      state.ets,
      match_spec,
      limit,
      state,
      owner_record,
      target_refs,
      referenced
    )
  end

  defp flow_retention_value_refs_used_by_ets_history_page(
         _table,
         _match_spec,
         limit,
         _state,
         _owner_record,
         target_refs,
         _referenced
       )
       when limit <= 0,
       do: target_refs

  defp flow_retention_value_refs_used_by_ets_history_page(
         table,
         match_spec,
         limit,
         state,
         owner_record,
         target_refs,
         referenced
       ) do
    case :ets.select(table, match_spec, limit) do
      :"$end_of_table" ->
        referenced

      {entries, :"$end_of_table"} ->
        flow_retention_value_refs_used_by_ets_history_entries(
          entries,
          state,
          owner_record,
          target_refs,
          referenced
        )

      {entries, continuation} ->
        referenced =
          flow_retention_value_refs_used_by_ets_history_entries(
            entries,
            state,
            owner_record,
            target_refs,
            referenced
          )

        if MapSet.size(referenced) >= MapSet.size(target_refs) do
          referenced
        else
          flow_retention_value_refs_used_by_ets_history_continue(
            continuation,
            state,
            owner_record,
            target_refs,
            referenced
          )
        end
    end
  rescue
    ArgumentError -> MapSet.union(referenced, target_refs)
  end

  defp flow_retention_value_refs_used_by_ets_history_continue(
         continuation,
         state,
         owner_record,
         target_refs,
         referenced
       ) do
    case :ets.select(continuation) do
      :"$end_of_table" ->
        referenced

      {entries, :"$end_of_table"} ->
        flow_retention_value_refs_used_by_ets_history_entries(
          entries,
          state,
          owner_record,
          target_refs,
          referenced
        )

      {entries, continuation} ->
        referenced =
          flow_retention_value_refs_used_by_ets_history_entries(
            entries,
            state,
            owner_record,
            target_refs,
            referenced
          )

        if MapSet.size(referenced) >= MapSet.size(target_refs) do
          referenced
        else
          flow_retention_value_refs_used_by_ets_history_continue(
            continuation,
            state,
            owner_record,
            target_refs,
            referenced
          )
        end
    end
  rescue
    ArgumentError -> MapSet.union(referenced, target_refs)
  end

  defp flow_retention_value_refs_used_by_ets_history_entries(
         entries,
         state,
         owner_record,
         target_refs,
         referenced
       ) do
    Enum.reduce_while(entries, referenced, fn {key, value, fid, offset, value_size}, acc ->
      acc =
        if flow_retention_same_flow_history_key?(key, owner_record) do
          acc
        else
          value =
            case value do
              value when is_binary(value) ->
                value

              _missing
              when valid_cold_location(fid, offset, value_size) or
                     valid_waraft_segment_location(fid, offset, value_size) ->
                case flow_retention_read_state_value(state, key, fid, offset, value_size) do
                  {:ok, value} when is_binary(value) -> value
                  _other -> nil
                end

              _other ->
                nil
            end

          flow_retention_value_refs_used_by_history_value(value, target_refs, acc)
        end

      if MapSet.size(acc) >= MapSet.size(target_refs), do: {:halt, acc}, else: {:cont, acc}
    end)
  end

  defp flow_retention_value_refs_used_by_other_lmdb_histories(
         state,
         owner_record,
         target_refs,
         referenced
       ) do
    if flow_lmdb_projection_enabled?(state) do
      path = flow_lmdb_record_path(state)
      prefix = "flow-history-index:"
      limit = flow_retention_history_lmdb_scan_limit()

      case flow_retention_lmdb_projection_state(state) do
        :available ->
          flow_retention_value_refs_used_by_lmdb_histories_after(
            path,
            prefix,
            <<>>,
            limit,
            state,
            owner_record,
            target_refs,
            referenced
          )

        :empty ->
          referenced

        :unavailable ->
          MapSet.union(referenced, target_refs)
      end
    else
      referenced
    end
  end

    end
  end
end
