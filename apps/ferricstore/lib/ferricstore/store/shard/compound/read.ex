defmodule Ferricstore.Store.Shard.Compound.Read do
  @moduledoc false

  alias Ferricstore.ExpiryContext
  alias Ferricstore.Store.{BlobValue, ColdRead, ReadResult}
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush
  alias Ferricstore.Store.Shard.Compound.{Promoted, Support}

  @cold_batch_read_timeout_ms 10_000
  @storage_read_failure ReadResult.failure(:invalid_keydir_entry)

  @spec handle_compound_get(binary(), binary(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_compound_get(redis_key, compound_key, state) do
    {value, state} = compound_get_value(redis_key, compound_key, state)
    {:reply, value, state}
  end

  @spec handle_compound_batch_get(binary(), [binary()], map()) :: {:reply, [term()], map()}
  @doc false
  def handle_compound_batch_get(redis_key, compound_keys, state) do
    {values, state} =
      case Promoted.promoted_store(state, redis_key) do
        nil ->
          compound_batch_get_shared(compound_keys, state)

        dedicated_path ->
          if Enum.any?(compound_keys, &Promoted.shared_log_compound_key?/1) do
            compound_batch_get_mixed(dedicated_path, compound_keys, state)
          else
            compound_batch_get_dedicated(dedicated_path, compound_keys, state)
          end
      end

    {:reply, values, state}
  end

  defp compound_batch_get_mixed(dedicated_path, compound_keys, state) do
    expiry_context = ExpiryContext.capture()

    {results, {state, shared_entries, _shared_count, dedicated_entries, _dedicated_count}} =
      Enum.map_reduce(compound_keys, {state, [], 0, [], 0}, fn compound_key,
                                                               {state, shared_entries,
                                                                shared_count, dedicated_entries,
                                                                dedicated_count} ->
        if Promoted.shared_log_compound_key?(compound_key) do
          case ShardETS.ets_lookup(state, compound_key, expiry_context) do
            {:hit, value, _exp} ->
              {{:value, value},
               {state, shared_entries, shared_count, dedicated_entries, dedicated_count}}

            {:cold, fid, off, vsize, exp} ->
              file_path = ShardETS.file_path(state.shard_data_path, fid)
              entry = {state, compound_key, file_path, fid, off, vsize, exp}

              {{:shared_cold, shared_count},
               {state, [entry | shared_entries], shared_count + 1, dedicated_entries,
                dedicated_count}}

            {:error, :invalid_keydir_entry} ->
              {{:value, @storage_read_failure},
               {state, shared_entries, shared_count, dedicated_entries, dedicated_count}}

            {:error, {:storage_read_failed, _reason}} = failure ->
              {{:value, failure},
               {state, shared_entries, shared_count, dedicated_entries, dedicated_count}}

            :expired ->
              {{:value, nil},
               {state, shared_entries, shared_count, dedicated_entries, dedicated_count}}

            :miss ->
              if ShardETS.pending_cold?(state, compound_key) do
                state = ShardFlush.flush_pending_for_read(state)

                {{:value, ShardETS.warm_from_store(state, compound_key)},
                 {state, shared_entries, shared_count, dedicated_entries, dedicated_count}}
              else
                {{:value, nil},
                 {state, shared_entries, shared_count, dedicated_entries, dedicated_count}}
              end
          end
        else
          case ShardETS.ets_lookup(state, compound_key, expiry_context) do
            {:hit, value, _exp} ->
              {{:value, value},
               {state, shared_entries, shared_count, dedicated_entries, dedicated_count}}

            {:cold, fid, off, vsize, exp} ->
              file_path = Promoted.dedicated_file_path(dedicated_path, fid)
              entry = {state, compound_key, file_path, fid, off, vsize, exp}

              {{:dedicated_cold, dedicated_count},
               {state, shared_entries, shared_count, [entry | dedicated_entries],
                dedicated_count + 1}}

            {:error, :invalid_keydir_entry} ->
              {{:value, @storage_read_failure},
               {state, shared_entries, shared_count, dedicated_entries, dedicated_count}}

            {:error, {:storage_read_failed, _reason}} = failure ->
              {{:value, failure},
               {state, shared_entries, shared_count, dedicated_entries, dedicated_count}}

            :expired ->
              {{:value, nil},
               {state, shared_entries, shared_count, dedicated_entries, dedicated_count}}

            :miss ->
              {{:value, nil},
               {state, shared_entries, shared_count, dedicated_entries, dedicated_count}}
          end
        end
      end)

    shared_values =
      shared_entries
      |> Enum.reverse()
      |> read_shared_cold_batch_async()
      |> List.to_tuple()

    dedicated_values =
      dedicated_entries
      |> Enum.reverse()
      |> read_compound_cold_batch_async()
      |> List.to_tuple()

    values =
      Enum.map(results, fn
        {:value, value} -> value
        {:shared_cold, index} -> elem(shared_values, index)
        {:dedicated_cold, index} -> elem(dedicated_values, index)
      end)

    {values, state}
  end

  defp compound_batch_get_shared(compound_keys, state) do
    expiry_context = ExpiryContext.capture()

    {results, {state, cold_entries, _cold_count}} =
      Enum.map_reduce(compound_keys, {state, [], 0}, fn compound_key,
                                                        {state, cold_entries, cold_count} ->
        case ShardETS.ets_lookup(state, compound_key, expiry_context) do
          {:hit, value, _exp} ->
            {{:value, value}, {state, cold_entries, cold_count}}

          {:cold, fid, off, vsize, exp} ->
            file_path = ShardETS.file_path(state.shard_data_path, fid)
            entry = {state, compound_key, file_path, fid, off, vsize, exp}
            {{:cold, cold_count}, {state, [entry | cold_entries], cold_count + 1}}

          {:error, :invalid_keydir_entry} ->
            {{:value, @storage_read_failure}, {state, cold_entries, cold_count}}

          {:error, {:storage_read_failed, _reason}} = failure ->
            {{:value, failure}, {state, cold_entries, cold_count}}

          :expired ->
            {{:value, nil}, {state, cold_entries, cold_count}}

          :miss ->
            if ShardETS.pending_cold?(state, compound_key) do
              state = ShardFlush.flush_pending_for_read(state)

              {{:value, ShardETS.warm_from_store(state, compound_key)},
               {state, cold_entries, cold_count}}
            else
              {{:value, nil}, {state, cold_entries, cold_count}}
            end
        end
      end)

    cold_values =
      cold_entries
      |> Enum.reverse()
      |> read_shared_cold_batch_async()
      |> List.to_tuple()

    values =
      Enum.map(results, fn
        {:value, value} -> value
        {:cold, index} -> elem(cold_values, index)
      end)

    {values, state}
  end

  defp compound_batch_get_dedicated(dedicated_path, compound_keys, state) do
    expiry_context = ExpiryContext.capture()

    {results, {cold_entries, _cold_count}} =
      Enum.map_reduce(compound_keys, {[], 0}, fn compound_key, {cold_entries, cold_count} ->
        case ShardETS.ets_lookup(state, compound_key, expiry_context) do
          {:hit, value, _exp} ->
            {{:value, value}, {cold_entries, cold_count}}

          {:cold, fid, off, vsize, exp} ->
            file_path = Promoted.dedicated_file_path(dedicated_path, fid)
            entry = {state, compound_key, file_path, fid, off, vsize, exp}
            {{:cold, cold_count}, {[entry | cold_entries], cold_count + 1}}

          {:error, :invalid_keydir_entry} ->
            {{:value, @storage_read_failure}, {cold_entries, cold_count}}

          {:error, {:storage_read_failed, _reason}} = failure ->
            {{:value, failure}, {cold_entries, cold_count}}

          :expired ->
            {{:value, nil}, {cold_entries, cold_count}}

          :miss ->
            {{:value, nil}, {cold_entries, cold_count}}
        end
      end)

    cold_values =
      cold_entries
      |> Enum.reverse()
      |> read_compound_cold_batch_async()
      |> List.to_tuple()

    values =
      Enum.map(results, fn
        {:value, value} -> value
        {:cold, index} -> elem(cold_values, index)
      end)

    {values, state}
  end

  defp read_shared_cold_batch_async(entries), do: read_compound_cold_batch_async(entries)

  defp read_compound_cold_batch_async(entries) do
    if entries == [] do
      []
    else
      {unique_entries, value_indexes} = dedupe_compound_cold_batch_entries(entries)
      unique_values = read_unique_compound_cold_batch_async(unique_entries) |> List.to_tuple()

      Enum.map(value_indexes, fn index -> elem(unique_values, index) end)
    end
  end

  defp dedupe_compound_cold_batch_entries(entries) do
    {unique_entries, _index_by_location, value_indexes} =
      Enum.reduce(entries, {[], %{}, []}, fn entry, {unique_acc, index_acc, value_index_acc} ->
        location = compound_cold_batch_entry_location(entry)

        case Map.fetch(index_acc, location) do
          {:ok, index} ->
            {unique_acc, index_acc, [index | value_index_acc]}

          :error ->
            index = map_size(index_acc)
            {[entry | unique_acc], Map.put(index_acc, location, index), [index | value_index_acc]}
        end
      end)

    {Enum.reverse(unique_entries), Enum.reverse(value_indexes)}
  end

  defp compound_cold_batch_entry_location(
         {_state, compound_key, file_path, _fid, off, _vsize, _exp}
       ) do
    {file_path, off, compound_key}
  end

  defp read_unique_compound_cold_batch_async(entries) do
    locations =
      Enum.map(entries, fn {_state, compound_key, file_path, _fid, off, _vsize, _exp} ->
        {file_path, off, compound_key}
      end)

    values =
      case Ferricstore.Store.ColdRead.pread_batch_keyed(
             locations,
             @cold_batch_read_timeout_ms
           ) do
        {:ok, values} when is_list(values) ->
          if length(values) == length(entries) do
            values
          else
            List.duplicate({:error, :batch_result_length_mismatch}, length(entries))
          end

        {:error, reason} ->
          List.duplicate({:error, reason}, length(entries))
      end

    emit_compound_batch_cold_read_errors(entries, values)

    Enum.zip(entries, materialize_compound_blob_values(entries, values))
    |> Enum.map(fn
      {{state, compound_key, _file_path, fid, off, vsize, exp}, {:ok, materialized}} ->
        ShardETS.cold_read_warm_ets(state, compound_key, materialized, exp, fid, off, vsize)
        materialized

      {_entry, {:error, reason}} ->
        ReadResult.failure({:cold_read_failed, reason})
    end)
  end

  defp materialize_compound_blob_values(entries, values) do
    {groups, indexed_results} =
      entries
      |> Enum.zip(values)
      |> Enum.with_index()
      |> Enum.reduce({%{}, %{}}, fn
        {{{state, _compound_key, _file_path, _fid, _off, _vsize, _exp}, value}, index},
        {groups, indexed_results}
        when is_binary(value) ->
          group_key = {state.data_dir, state.index, Support.blob_side_channel_threshold(state)}
          item = {index, value}

          {Map.update(groups, group_key, [item], &[item | &1]), indexed_results}

        {{{_state, _compound_key, _file_path, _fid, _off, _vsize, _exp}, {:error, reason}}, index},
        {groups, indexed_results} ->
          {groups, Map.put(indexed_results, index, {:error, reason})}

        {{{_state, _compound_key, _file_path, _fid, _off, _vsize, _exp}, nil}, index},
        {groups, indexed_results} ->
          {groups, Map.put(indexed_results, index, {:error, :missing_live_cold_entry})}

        {{{_state, _compound_key, _file_path, _fid, _off, _vsize, _exp}, invalid}, index},
        {groups, indexed_results} ->
          {groups,
           Map.put(indexed_results, index, {:error, {:invalid_cold_read_result, invalid}})}
      end)

    indexed_results =
      Enum.reduce(groups, indexed_results, fn {{data_dir, shard_index, threshold}, items}, acc ->
        ordered_items = Enum.reverse(items)
        values = Enum.map(ordered_items, fn {_index, value} -> value end)

        ordered_items
        |> Enum.zip(BlobValue.maybe_materialize_many(data_dir, shard_index, threshold, values))
        |> Enum.reduce(acc, fn {{index, _value}, result}, acc -> Map.put(acc, index, result) end)
      end)

    values
    |> Enum.with_index()
    |> Enum.map(fn {_value, index} -> Map.fetch!(indexed_results, index) end)
  end

  defp emit_compound_batch_cold_read_errors(entries, values) do
    entries
    |> Enum.zip(values)
    |> Enum.reduce(%{}, fn
      {{_state, _compound_key, file_path, _fid, _off, _vsize, _exp}, {:error, raw_reason}}, acc ->
        Map.update(acc, {file_path, raw_reason}, 1, &(&1 + 1))

      {_entry, _value}, acc ->
        acc
    end)
    |> Enum.each(fn {{path, raw_reason}, count} ->
      ColdRead.emit_pread_error(path, raw_reason, count)
    end)
  end

  defp compound_get_value(redis_key, compound_key, state) do
    case Promoted.promoted_store_for_compound(state, redis_key, compound_key) do
      nil ->
        case ShardETS.ets_lookup_warm_result(state, compound_key) do
          {:hit, value, _exp} ->
            {value, state}

          {:error, :cold_read_failed} ->
            {@storage_read_failure, state}

          {:error, {:storage_read_failed, _reason}} = failure ->
            {failure, state}

          :expired ->
            {nil, state}

          :miss ->
            if ShardETS.pending_cold?(state, compound_key) do
              state = ShardFlush.flush_pending_for_read(state)
              {ShardETS.warm_from_store(state, compound_key), state}
            else
              {nil, state}
            end
        end

      dedicated_path ->
        case ShardETS.ets_lookup(state, compound_key) do
          {:hit, value, _exp} ->
            {value, state}

          :expired ->
            {nil, state}

          {:error, :invalid_keydir_entry} ->
            {@storage_read_failure, state}

          {:error, {:storage_read_failed, _reason}} = failure ->
            {failure, state}

          _cold_or_miss ->
            case Promoted.promoted_read(dedicated_path, compound_key, state) do
              {:ok, nil} ->
                {nil, state}

              {:ok, value, exp} ->
                ShardETS.ets_insert(state, compound_key, value, exp)
                {value, state}

              {:ok, value, exp, fid, off, vsize} ->
                ShardETS.cold_read_warm_ets(state, compound_key, value, exp, fid, off, vsize)
                {value, state}

              {:ok, value} ->
                ShardETS.ets_insert(state, compound_key, value, 0)
                {value, state}

              {:error, reason} ->
                {promoted_read_failure(reason), state}

              invalid ->
                {ReadResult.failure({:invalid_promoted_read_result, invalid}), state}
            end
        end
    end
  end

  @spec handle_compound_get_meta(binary(), binary(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_compound_get_meta(redis_key, compound_key, state) do
    {meta, state} = compound_get_meta_value(redis_key, compound_key, state)
    {:reply, meta, state}
  end

  @spec handle_compound_batch_get_meta(binary(), [binary()], map()) :: {:reply, [term()], map()}
  @doc false
  def handle_compound_batch_get_meta(redis_key, compound_keys, state) do
    {metas, state} =
      case Promoted.promoted_store(state, redis_key) do
        nil ->
          compound_batch_get_meta_shared(redis_key, compound_keys, state)

        dedicated_path ->
          compound_batch_get_meta_dedicated(dedicated_path, compound_keys, state)
      end

    {:reply, metas, state}
  end

  defp compound_batch_get_meta_shared(_redis_key, compound_keys, state) do
    expiry_context = ExpiryContext.capture()

    {results, {state, cold_entries, _cold_count}} =
      Enum.map_reduce(compound_keys, {state, [], 0}, fn compound_key,
                                                        {state, cold_entries, cold_count} ->
        case ShardETS.ets_lookup(state, compound_key, expiry_context) do
          {:hit, value, expire_at_ms} ->
            {{:value, {value, expire_at_ms}}, {state, cold_entries, cold_count}}

          {:cold, fid, off, vsize, exp} ->
            file_path = ShardETS.file_path(state.shard_data_path, fid)
            entry = {state, compound_key, file_path, fid, off, vsize, exp}
            {{:cold, cold_count}, {state, [entry | cold_entries], cold_count + 1}}

          {:error, :invalid_keydir_entry} ->
            {{:value, @storage_read_failure}, {state, cold_entries, cold_count}}

          {:error, {:storage_read_failed, _reason}} = failure ->
            {{:value, failure}, {state, cold_entries, cold_count}}

          :expired ->
            {{:value, nil}, {state, cold_entries, cold_count}}

          :miss ->
            if ShardETS.pending_cold?(state, compound_key) do
              state = ShardFlush.flush_pending_for_read(state)

              {{:value, ShardETS.warm_meta_from_store(state, compound_key)},
               {state, cold_entries, cold_count}}
            else
              {{:value, nil}, {state, cold_entries, cold_count}}
            end
        end
      end)

    cold_entries = Enum.reverse(cold_entries)

    cold_metas =
      read_compound_cold_batch_async(cold_entries)
      |> Enum.zip(cold_entries)
      |> Enum.map(fn
        {{:error, {:storage_read_failed, _reason}} = failure, _entry} ->
          failure

        {nil, _entry} ->
          nil

        {value, {_state, _compound_key, _file_path, _fid, _off, _vsize, exp}} ->
          {value, exp}
      end)
      |> List.to_tuple()

    metas =
      Enum.map(results, fn
        {:value, meta} -> meta
        {:cold, index} -> elem(cold_metas, index)
      end)

    {metas, state}
  end

  defp compound_batch_get_meta_dedicated(dedicated_path, compound_keys, state) do
    expiry_context = ExpiryContext.capture()

    {results, {cold_entries, _cold_count}} =
      Enum.map_reduce(compound_keys, {[], 0}, fn compound_key, {cold_entries, cold_count} ->
        case ShardETS.ets_lookup(state, compound_key, expiry_context) do
          {:hit, value, expire_at_ms} ->
            {{:value, {value, expire_at_ms}}, {cold_entries, cold_count}}

          {:cold, fid, off, vsize, exp} ->
            file_path = Promoted.dedicated_file_path(dedicated_path, fid)
            entry = {state, compound_key, file_path, fid, off, vsize, exp}
            {{:cold, cold_count}, {[entry | cold_entries], cold_count + 1}}

          {:error, :invalid_keydir_entry} ->
            {{:value, @storage_read_failure}, {cold_entries, cold_count}}

          {:error, {:storage_read_failed, _reason}} = failure ->
            {{:value, failure}, {cold_entries, cold_count}}

          :expired ->
            {{:value, nil}, {cold_entries, cold_count}}

          :miss ->
            {{:value, nil}, {cold_entries, cold_count}}
        end
      end)

    cold_entries = Enum.reverse(cold_entries)

    cold_metas =
      read_compound_cold_batch_async(cold_entries)
      |> Enum.zip(cold_entries)
      |> Enum.map(fn
        {{:error, {:storage_read_failed, _reason}} = failure, _entry} ->
          failure

        {nil, _entry} ->
          nil

        {value, {_state, _compound_key, _file_path, _fid, _off, _vsize, exp}} ->
          {value, exp}
      end)
      |> List.to_tuple()

    metas =
      Enum.map(results, fn
        {:value, meta} -> meta
        {:cold, index} -> elem(cold_metas, index)
      end)

    {metas, state}
  end

  defp compound_get_meta_value(redis_key, compound_key, state) do
    case Promoted.promoted_store(state, redis_key) do
      nil ->
        case ShardETS.ets_lookup_warm_result(state, compound_key) do
          {:hit, value, expire_at_ms} ->
            {{value, expire_at_ms}, state}

          {:error, :cold_read_failed} ->
            {@storage_read_failure, state}

          {:error, {:storage_read_failed, _reason}} = failure ->
            {failure, state}

          :expired ->
            {nil, state}

          :miss ->
            if ShardETS.pending_cold?(state, compound_key) do
              state = ShardFlush.flush_pending_for_read(state)
              {ShardETS.warm_meta_from_store(state, compound_key), state}
            else
              {nil, state}
            end
        end

      dedicated_path ->
        case ShardETS.ets_lookup(state, compound_key) do
          {:hit, value, expire_at_ms} ->
            {{value, expire_at_ms}, state}

          :expired ->
            {nil, state}

          {:error, :invalid_keydir_entry} ->
            {@storage_read_failure, state}

          {:error, {:storage_read_failed, _reason}} = failure ->
            {failure, state}

          _cold_or_miss ->
            case Promoted.promoted_read(dedicated_path, compound_key, state) do
              {:ok, nil} ->
                {nil, state}

              {:ok, value, exp} ->
                ShardETS.ets_insert(state, compound_key, value, exp)
                {{value, exp}, state}

              {:ok, value, exp, fid, off, vsize} ->
                ShardETS.cold_read_warm_ets(state, compound_key, value, exp, fid, off, vsize)
                {{value, exp}, state}

              {:ok, value} ->
                ShardETS.ets_insert(state, compound_key, value, 0)
                {{value, 0}, state}

              {:error, reason} ->
                {promoted_read_failure(reason), state}

              invalid ->
                {ReadResult.failure({:invalid_promoted_read_result, invalid}), state}
            end
        end
    end
  end

  defp promoted_read_failure(:invalid_keydir_entry), do: @storage_read_failure
  defp promoted_read_failure(reason), do: ReadResult.failure({:cold_read_failed, reason})
end
