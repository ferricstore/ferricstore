defmodule Ferricstore.Store.Router.Part03 do
  @moduledoc false

  # Extracted from Router: batch_get_with_deferred_blob_file_refs_planned .. blob_ref_candidate
  defmacro __using__(_opts) do
    quote do
alias Ferricstore.CommandTime
alias Ferricstore.ErrorReasons
alias Ferricstore.HLC
alias Ferricstore.HyperLogLog, as: HLL
alias Ferricstore.Flow.Locator
alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
alias Ferricstore.Raft.ReplyAwaiter
alias Ferricstore.Stats
alias Ferricstore.Store.BlobRef
alias Ferricstore.Store.BlobStore
alias Ferricstore.Store.BlobValue
alias Ferricstore.Store.CompoundCommand
alias Ferricstore.Store.CompoundKey
alias Ferricstore.Store.LFU
alias Ferricstore.Store.ListOps
alias Ferricstore.Store.Router
alias Ferricstore.Store.SlotMap
alias Ferricstore.Store.TypeRegistry
        @doc false
        @spec batch_get_with_deferred_blob_file_refs_planned(
                FerricStore.Instance.t(),
                [tuple()],
                non_neg_integer()
              ) :: [binary() | nil | {:file_ref, binary(), non_neg_integer(), non_neg_integer()}]
        def batch_get_with_deferred_blob_file_refs_planned(ctx, planned_keys, min_file_ref_size) do
          {results, _present?} =
            batch_get_with_deferred_blob_file_refs_planned_and_presence(
              ctx,
              planned_keys,
              min_file_ref_size
            )
      
          results
        end
      
        defp planned_lookup_key({_original_key, lookup_key, _shard_index, _keydir})
             when is_binary(lookup_key),
             do: lookup_key
      
        defp planned_lookup_key(key) when is_binary(key), do: key
      
        defp file_ref_read_result?({:file_ref, _path, _offset, _size}), do: true
        defp file_ref_read_result?(_value), do: false
      
        defp maybe_file_ref_or_cold_entry(
               ctx,
               idx,
               keydir,
               key,
               path,
               file_id,
               offset,
               value_size,
               min_file_ref_size,
               cold_entries,
               cold_count,
               waraft_entries,
               waraft_count,
               hot_hits,
               now
             )
             when value_size >= min_file_ref_size do
          if blob_ref_candidate?(ctx, value_size) do
            entry = {ctx, idx, keydir, key, path, file_id, offset, value_size}
      
            {{:cold, cold_count},
             {[entry | cold_entries], cold_count + 1, waraft_entries, waraft_count, hot_hits}}
          else
            case file_ref_from_cold_location(ctx, idx, path, offset, key, value_size, true) do
              {:ok, {file_ref_path, value_offset, size}} ->
                Stats.record_cold_read(ctx, key)
      
                {{:file_ref, file_ref_path, value_offset, size},
                 {cold_entries, cold_count, waraft_entries, waraft_count, hot_hits}}
      
              nil ->
                case retry_changed_file_ref(ctx, idx, keydir, key, {file_id, offset, value_size}, now) do
                  {:cold_ref, retry_path, value_offset, retry_size} ->
                    Stats.record_cold_read(ctx, key)
      
                    {{:file_ref, retry_path, value_offset, retry_size},
                     {cold_entries, cold_count, waraft_entries, waraft_count, hot_hits}}
      
                  {:hot, value} ->
                    {{:value, value},
                     {cold_entries, cold_count, waraft_entries, waraft_count, hot_hits}}
      
                  :miss ->
                    record_keyspace_miss(ctx, key)
                    {{:value, nil}, {cold_entries, cold_count, waraft_entries, waraft_count, hot_hits}}
                end
            end
          end
        end
      
        defp maybe_file_ref_or_cold_entry(
               ctx,
               idx,
               keydir,
               key,
               path,
               file_id,
               offset,
               value_size,
               _min_file_ref_size,
               cold_entries,
               cold_count,
               waraft_entries,
               waraft_count,
               hot_hits,
               _now
             ) do
          entry = {ctx, idx, keydir, key, path, file_id, offset, value_size}
      
          {{:cold, cold_count},
           {[entry | cold_entries], cold_count + 1, waraft_entries, waraft_count, hot_hits}}
        end
      
        defp read_cold_batch_async([], _now), do: []
      
        defp read_cold_batch_async(entries, now) do
          {unique_entries, value_indexes} = dedupe_cold_batch_entries(entries)
          unique_values = read_unique_cold_batch_async(unique_entries, now) |> List.to_tuple()
      
          Enum.map(value_indexes, fn index -> elem(unique_values, index) end)
        end
      
        defp read_cold_batch_file_ref_async([], _now, _min_file_ref_size, _validate_blob_ref?), do: []
      
        defp read_cold_batch_file_ref_async(entries, now, min_file_ref_size, validate_blob_ref?) do
          {unique_entries, value_indexes} = dedupe_cold_batch_entries(entries)
      
          unique_values =
            unique_entries
            |> read_unique_cold_batch_file_ref_async(now, min_file_ref_size, validate_blob_ref?)
            |> List.to_tuple()
      
          Enum.map(value_indexes, fn index -> elem(unique_values, index) end)
        end
      
        defp read_waraft_segment_batch_materialized([]), do: []
      
        defp read_waraft_segment_batch_materialized(entries) do
          entries
          |> Enum.with_index()
          |> Enum.group_by(
            fn {{ctx, idx, _keydir, _key, file_id, _offset, _value_size}, _index} ->
              {ctx.data_dir, idx, file_id}
            end,
            fn indexed_entry -> indexed_entry end
          )
          |> Enum.reduce(%{}, &read_waraft_segment_group/2)
          |> waraft_segment_results(entries)
        end
      
        defp read_waraft_segment_batch_file_ref_or_materialized(
               [],
               _min_file_ref_size,
               _validate_blob_ref?
             ),
             do: []
      
        defp read_waraft_segment_batch_file_ref_or_materialized(
               entries,
               min_file_ref_size,
               validate_blob_ref?
             ) do
          entries
          |> Enum.with_index()
          |> Enum.group_by(
            fn {{ctx, idx, _keydir, _key, file_id, _offset, _value_size}, _index} ->
              {ctx.data_dir, idx, file_id}
            end,
            fn indexed_entry -> indexed_entry end
          )
          |> Enum.reduce(%{}, fn group, acc ->
            read_waraft_segment_file_ref_group(group, min_file_ref_size, validate_blob_ref?, acc)
          end)
          |> waraft_segment_results(entries)
        end
      
        defp read_waraft_segment_file_ref_group(
               {_group_key,
                [{{ctx, idx, _keydir, _key, file_id, _offset, _value_size}, _index} | _] =
                  indexed_entries},
               min_file_ref_size,
               validate_blob_ref?,
               acc
             ) do
          keys =
            indexed_entries
            |> Enum.map(fn {{_ctx, _idx, _keydir, key, _file_id, _offset, _value_size}, _index} ->
              key
            end)
            |> Enum.uniq()
      
          case read_waraft_segment_values(ctx, idx, file_id, keys, length(indexed_entries)) do
            {:ok, value_by_key} ->
              Enum.reduce(indexed_entries, acc, fn
                {{ctx, idx, keydir, key, file_id, offset, value_size}, index}, acc ->
                  case Map.fetch(value_by_key, key) do
                    {:ok, value} when is_binary(value) ->
                      case waraft_segment_file_ref_or_materialized_value(
                             ctx,
                             idx,
                             keydir,
                             key,
                             file_id,
                             offset,
                             value_size,
                             value,
                             min_file_ref_size,
                             validate_blob_ref?
                           ) do
                        {:ok, result} -> Map.put(acc, index, result)
                        {:error, reason} -> Map.put(acc, index, {:waraft_read_error, reason})
                      end
      
                    _missing ->
                      acc
                  end
              end)
      
            {:error, reason} ->
              put_waraft_read_errors(acc, indexed_entries, reason)
          end
        end
      
        defp waraft_segment_file_ref_or_materialized_value(
               ctx,
               idx,
               keydir,
               key,
               file_id,
               offset,
               _value_size,
               value,
               min_file_ref_size,
               validate_blob_ref?
             ) do
          if blob_ref_candidate?(ctx, byte_size(value)) do
            case BlobRef.decode(value) do
              {:ok, %BlobRef{size: blob_size} = ref} when blob_size >= min_file_ref_size ->
                case blob_ref_file_ref(ctx, idx, ref, validate_blob_ref?) do
                  {:ok, {path, value_offset, size}} ->
                    Stats.record_cold_read(ctx, key)
                    {:ok, {:file_ref, path, value_offset, size}}
      
                  {:error, reason} ->
                    {:error, reason}
                end
      
              {:ok, %BlobRef{} = ref} ->
                case BlobStore.get(ctx.data_dir, idx, ref) do
                  {:ok, materialized} ->
                    Stats.record_cold_read(ctx, key)
                    warm_ets_after_cold_read(ctx, idx, keydir, key, materialized, file_id, offset)
                    {:ok, materialized}
      
                  {:error, reason} ->
                    {:error, reason}
                end
      
              :error ->
                Stats.record_cold_read(ctx, key)
                warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
                {:ok, value}
            end
          else
            Stats.record_cold_read(ctx, key)
            warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
            {:ok, value}
          end
        end
      
        defp read_waraft_segment_group(
               {_group_key,
                [{{ctx, idx, _keydir, _key, file_id, _offset, _value_size}, _index} | _] =
                  indexed_entries},
               acc
             ) do
          keys =
            indexed_entries
            |> Enum.map(fn {{_ctx, _idx, _keydir, key, _file_id, _offset, _value_size}, _index} ->
              key
            end)
            |> Enum.uniq()
      
          case read_waraft_segment_values(ctx, idx, file_id, keys, length(indexed_entries)) do
            {:ok, value_by_key} ->
              read_waraft_segment_found_values(ctx, idx, indexed_entries, value_by_key, acc)
      
            {:error, reason} ->
              put_waraft_read_errors(acc, indexed_entries, reason)
          end
        end
      
        defp read_waraft_segment_found_values(ctx, idx, indexed_entries, value_by_key, acc) do
          found =
            indexed_entries
            |> Enum.reduce([], fn
              {{ctx, idx, keydir, key, file_id, offset, _value_size}, index}, found ->
                case Map.fetch(value_by_key, key) do
                  {:ok, value} when is_binary(value) ->
                    [{index, ctx, idx, keydir, key, file_id, offset, value} | found]
      
                  _missing ->
                    found
                end
            end)
            |> Enum.reverse()
      
          materialized =
            found
            |> Enum.map(fn {_index, _ctx, _idx, _keydir, _key, _file_id, _offset, value} -> value end)
            |> then(fn values ->
              BlobValue.maybe_materialize_many(ctx.data_dir, idx, BlobValue.threshold(ctx), values)
            end)
      
          found
          |> Enum.zip(materialized)
          |> Enum.reduce(acc, fn
            {{index, ctx, idx, keydir, key, file_id, offset, _raw_value}, {:ok, value}}, acc
            when is_binary(value) ->
              Stats.record_cold_read(ctx, key)
              warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
              Map.put(acc, index, value)
      
            {{index, _ctx, _idx, _keydir, _key, _file_id, _offset, _raw_value}, {:error, reason}},
            acc ->
              Map.put(acc, index, {:waraft_read_error, reason})
          end)
        end
      
        defp waraft_segment_results(result_by_index, entries) do
          entries
          |> Enum.with_index()
          |> Enum.map(fn {{ctx, idx, keydir, key, file_id, offset, value_size}, index} ->
            case Map.fetch(result_by_index, index) do
              {:ok, {:waraft_read_error, _reason}} ->
                retry_changed_waraft_segment_value_result(
                  ctx,
                  idx,
                  keydir,
                  key,
                  {file_id, offset, value_size},
                  HLC.now_ms()
                )
      
              {:ok, value} ->
                value
      
              :error ->
                retry_changed_waraft_segment_value_result(
                  ctx,
                  idx,
                  keydir,
                  key,
                  {file_id, offset, value_size},
                  HLC.now_ms()
                )
            end
          end)
        end
      
        defp read_waraft_segment_values(ctx, idx, file_id, keys, count) do
          case Ferricstore.Raft.WARaftSegmentReader.read_values_from_location(ctx, idx, file_id, keys) do
            {:ok, values} when is_map(values) ->
              {:ok, values}
      
            {:error, reason} ->
              emit_waraft_segment_read_error(ctx, idx, file_id, reason, count)
              {:error, reason}
      
            _unexpected ->
              emit_waraft_segment_read_error(ctx, idx, file_id, :bad_segment_read_result, count)
              {:error, :bad_segment_read_result}
          end
        end
      
        defp put_waraft_read_errors(acc, indexed_entries, reason) do
          Enum.reduce(indexed_entries, acc, fn {_entry, index}, acc ->
            Map.put(acc, index, {:waraft_read_error, reason})
          end)
        end
      
        defp emit_waraft_segment_read_error(ctx, idx, file_id, reason, count) do
          path = waraft_segment_read_path(ctx, idx, file_id)
          reason = cold_batch_read_error_reason({:error, reason})
          emit_batch_cold_read_corruption(%{{path, reason} => count})
        end
      
        defp waraft_segment_read_path(ctx, idx, {tag, index})
             when tag in [:waraft_segment, :waraft_projection, :waraft_apply_projection] do
          Path.join([
            ctx.data_dir,
            "waraft",
            "ferricstore_waraft_backend.#{idx + 1}",
            "#{tag}-#{index}"
          ])
        end
      
        defp waraft_segment_read_path(ctx, idx, file_id) do
          Path.join([
            ctx.data_dir,
            "waraft",
            "ferricstore_waraft_backend.#{idx + 1}",
            inspect(file_id)
          ])
        end
      
        defp dedupe_cold_batch_entries(entries) do
          {unique_entries, _index_by_location, value_indexes} =
            Enum.reduce(entries, {[], %{}, []}, fn entry, {unique_acc, index_acc, value_index_acc} ->
              location = cold_batch_entry_location(entry)
      
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
      
        defp cold_batch_entry_location({_ctx, _idx, _keydir, key, path, _file_id, offset, _value_size}) do
          {path, offset, key}
        end
      
        defp read_unique_cold_batch_async(entries, now) do
          locations =
            Enum.map(entries, fn {_ctx, _idx, _keydir, key, path, _file_id, offset, _value_size} ->
              {path, offset, key}
            end)
      
          values =
            case router_pread_batch_keyed(locations, @cold_batch_read_timeout_ms) do
              {:ok, values} when is_list(values) ->
                if length(values) == length(entries) do
                  values
                else
                  List.duplicate({:error, :batch_result_length_mismatch}, length(entries))
                end
      
              {:error, reason} ->
                List.duplicate({:error, reason}, length(entries))
            end
      
          entry_values = Enum.zip(entries, values)
      
          corrupt_by_path =
            Enum.reduce(entry_values, %{}, fn
              {{_ctx, _idx, _keydir, _key, _path, _file_id, _offset, _value_size}, value},
              corrupt_by_path
              when is_binary(value) ->
                corrupt_by_path
      
              {{_ctx, _idx, _keydir, _key, path, _file_id, _offset, _value_size}, value},
              corrupt_by_path ->
                reason = cold_batch_read_error_reason(value)
                Map.update(corrupt_by_path, {path, reason}, 1, &(&1 + 1))
            end)
      
          emit_batch_cold_read_corruption(corrupt_by_path)
      
          entry_values = materialize_cold_batch_values(entry_values)
      
          Enum.map(entry_values, fn
            {{ctx, idx, keydir, key, _path, file_id, offset, _value_size}, {:ok, value}}
            when is_binary(value) ->
              Stats.record_cold_read(ctx, key)
              warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
              value
      
            {{ctx, idx, keydir, key, _path, file_id, offset, value_size}, _value_or_error} ->
              case retry_changed_cold_value(ctx, idx, keydir, key, {file_id, offset, value_size}, now) do
                {:cold, value, retry_file_id, retry_offset} ->
                  Stats.record_cold_read(ctx, key)
                  warm_ets_after_cold_read(ctx, idx, keydir, key, value, retry_file_id, retry_offset)
                  value
      
                {:hot, value} ->
                  value
      
                :miss ->
                  record_keyspace_miss(ctx, key)
                  nil
              end
          end)
        end
      
        defp materialize_cold_batch_values(entry_values) do
          {groups, results} =
            entry_values
            |> Enum.with_index()
            |> Enum.reduce({%{}, %{}}, fn
              {{{ctx, idx, _keydir, _key, _path, _file_id, _offset, _value_size}, value}, index},
              {groups, results}
              when is_binary(value) ->
                group_key = {ctx.data_dir, idx, BlobValue.threshold(ctx)}
                groups = Map.update(groups, group_key, [{index, value}], &[{index, value} | &1])
                {groups, results}
      
              {{_entry, value_or_error}, index}, {groups, results} ->
                {groups, Map.put(results, index, value_or_error)}
            end)
      
          results =
            Enum.reduce(groups, results, fn {{data_dir, idx, threshold}, indexed_values}, acc ->
              indexed_values = Enum.reverse(indexed_values)
              {indexes, values} = Enum.unzip(indexed_values)
              materialized = BlobValue.maybe_materialize_many(data_dir, idx, threshold, values)
      
              indexes
              |> Enum.zip(materialized)
              |> Enum.reduce(acc, fn {index, result}, acc -> Map.put(acc, index, result) end)
            end)
      
          entry_values
          |> Enum.with_index()
          |> Enum.map(fn {{entry, _value}, index} ->
            {entry, Map.fetch!(results, index)}
          end)
        end
      
        defp read_unique_cold_batch_file_ref_async(
               entries,
               now,
               min_file_ref_size,
               validate_blob_ref?
             ) do
          locations =
            Enum.map(entries, fn {_ctx, _idx, _keydir, key, path, _file_id, offset, _value_size} ->
              {path, offset, key}
            end)
      
          values =
            case router_pread_batch_keyed(locations, @cold_batch_read_timeout_ms) do
              {:ok, values} when is_list(values) ->
                if length(values) == length(entries) do
                  values
                else
                  List.duplicate({:error, :batch_result_length_mismatch}, length(entries))
                end
      
              {:error, reason} ->
                List.duplicate({:error, reason}, length(entries))
            end
      
          entry_values = Enum.zip(entries, values)
      
          corrupt_by_path =
            Enum.reduce(entry_values, %{}, fn
              {{_ctx, _idx, _keydir, _key, _path, _file_id, _offset, _value_size}, value},
              corrupt_by_path
              when is_binary(value) ->
                corrupt_by_path
      
              {{_ctx, _idx, _keydir, _key, path, _file_id, _offset, _value_size}, value},
              corrupt_by_path ->
                reason = cold_batch_read_error_reason(value)
                Map.update(corrupt_by_path, {path, reason}, 1, &(&1 + 1))
            end)
      
          emit_batch_cold_read_corruption(corrupt_by_path)
      
          blob_file_ref_results =
            batch_blob_file_ref_results(entry_values, min_file_ref_size, validate_blob_ref?)
      
          entry_values
          |> Enum.with_index()
          |> Enum.map(fn
            {{entry, _value}, index} when is_map_key(blob_file_ref_results, index) ->
              cold_batch_preloaded_blob_file_ref_value(
                entry,
                Map.fetch!(blob_file_ref_results, index),
                now
              )
      
            {{entry, value}, _index} when is_binary(value) ->
              cold_batch_file_ref_value(entry, value, min_file_ref_size, validate_blob_ref?, now)
      
            {{{ctx, idx, keydir, key, _path, file_id, offset, value_size}, _value}, _index} ->
              retry_cold_batch_materialized_value(
                ctx,
                idx,
                keydir,
                key,
                {file_id, offset, value_size},
                now
              )
          end)
        end
      
        defp batch_blob_file_ref_results(entry_values, min_file_ref_size, _validate_blob_ref?) do
          grouped =
            entry_values
            |> Enum.with_index()
            |> Enum.reduce(%{}, fn
              {{{ctx, idx, _keydir, _key, _path, _file_id, _offset, value_size}, value}, index}, acc
              when is_binary(value) ->
                with true <- blob_ref_candidate?(ctx, value_size),
                     {:ok, %BlobRef{size: blob_size} = ref} when blob_size >= min_file_ref_size <-
                       BlobRef.decode(value) do
                  Map.update(acc, {ctx.data_dir, idx}, [{index, ref}], &[{index, ref} | &1])
                else
                  _ -> acc
                end
      
              _other, acc ->
                acc
            end)
      
          Enum.reduce(grouped, %{}, fn {{data_dir, idx}, indexed_refs}, acc ->
            indexed_refs = Enum.reverse(indexed_refs)
            {indexes, refs} = Enum.unzip(indexed_refs)
      
            results = BlobStore.file_refs_many(data_dir, idx, refs)
      
            indexes
            |> Enum.zip(results)
            |> Enum.reduce(acc, fn {index, result}, acc -> Map.put(acc, index, result) end)
          end)
        end
      
        defp cold_batch_preloaded_blob_file_ref_value(
               {ctx, _idx, _keydir, key, _path, _file_id, _offset, _value_size},
               {:ok, {path, value_offset, size}},
               _now
             ) do
          Stats.record_cold_read(ctx, key)
          {:file_ref, path, value_offset, size}
        end
      
        defp cold_batch_preloaded_blob_file_ref_value(
               {ctx, idx, keydir, key, _path, file_id, offset, value_size},
               {:error, _reason},
               now
             ) do
          retry_cold_batch_materialized_value(
            ctx,
            idx,
            keydir,
            key,
            {file_id, offset, value_size},
            now
          )
        end
      
        defp cold_batch_file_ref_value(
               {ctx, idx, keydir, key, _path, file_id, offset, value_size},
               value,
               min_file_ref_size,
               validate_blob_ref?,
               now
             ) do
          if blob_ref_candidate?(ctx, value_size) do
            case BlobRef.decode(value) do
              {:ok, %BlobRef{size: blob_size} = ref} when blob_size >= min_file_ref_size ->
                case blob_ref_file_ref(ctx, idx, ref, validate_blob_ref?) do
                  {:ok, {path, value_offset, size}} ->
                    Stats.record_cold_read(ctx, key)
                    {:file_ref, path, value_offset, size}
      
                  {:error, _reason} ->
                    retry_cold_batch_materialized_value(
                      ctx,
                      idx,
                      keydir,
                      key,
                      {file_id, offset, value_size},
                      now
                    )
                end
      
              {:ok, %BlobRef{} = ref} ->
                case BlobStore.get(ctx.data_dir, idx, ref) do
                  {:ok, materialized} ->
                    Stats.record_cold_read(ctx, key)
                    warm_ets_after_cold_read(ctx, idx, keydir, key, materialized, file_id, offset)
                    materialized
      
                  {:error, _reason} ->
                    retry_cold_batch_materialized_value(
                      ctx,
                      idx,
                      keydir,
                      key,
                      {file_id, offset, value_size},
                      now
                    )
                end
      
              :error ->
                Stats.record_cold_read(ctx, key)
                warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
                value
            end
          else
            Stats.record_cold_read(ctx, key)
            warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
            value
          end
        end
      
        defp retry_cold_batch_materialized_value(ctx, idx, keydir, key, original_location, now) do
          case retry_changed_cold_value(ctx, idx, keydir, key, original_location, now) do
            {:cold, value, retry_file_id, retry_offset} ->
              Stats.record_cold_read(ctx, key)
              warm_ets_after_cold_read(ctx, idx, keydir, key, value, retry_file_id, retry_offset)
              value
      
            {:hot, value} ->
              value
      
            :miss ->
              record_keyspace_miss(ctx, key)
              nil
          end
        end
      
        defp router_pread_batch_keyed(locations, timeout_ms) do
          case Process.get(:ferricstore_router_pread_batch_keyed_result) do
            nil -> Ferricstore.Store.ColdRead.pread_batch_keyed(locations, timeout_ms)
            forced_result -> forced_result
          end
        end
      
        defp emit_batch_cold_read_corruption(corrupt_by_path) when map_size(corrupt_by_path) == 0,
          do: :ok
      
        defp emit_batch_cold_read_corruption(corrupt_by_path) do
          Enum.each(corrupt_by_path, fn {{path, reason}, count} ->
            :telemetry.execute(
              [:ferricstore, :bitcask, :pread_corrupt],
              %{count: count},
              %{path: path, reason: reason}
            )
          end)
        end
      
        defp cold_batch_read_error_reason({:error, reason}) when is_binary(reason) do
          downcased = String.downcase(reason)
      
          if String.contains?(downcased, "missing_file") or
               String.contains?(downcased, "no such file") do
            :missing_file
          else
            :corrupt_record
          end
        end
      
        defp cold_batch_read_error_reason({:error, reason}) when reason in [:missing_file, :enoent],
          do: :missing_file
      
        defp cold_batch_read_error_reason({:error, :timeout}), do: :timeout
      
        defp cold_batch_read_error_reason({:error, :batch_result_length_mismatch}),
          do: :batch_result_length_mismatch
      
        defp cold_batch_read_error_reason({:error, :segment_entry_not_found}),
          do: :missing_segment_entry
      
        defp cold_batch_read_error_reason({:error, _reason}), do: :corrupt_record
      
        defp cold_batch_read_error_reason(_value), do: :nil_from_cold_location
      
        defp read_cold_async(path, offset, expected_key) do
          Ferricstore.Store.ColdRead.pread_keyed(
            path,
            offset,
            expected_key,
            @cold_batch_read_timeout_ms
          )
        end
      
        defp read_cold_materialized(ctx, idx, path, offset, expected_key) do
          with {:ok, value} <- read_cold_async(path, offset, expected_key),
               {:ok, materialized} <- materialize_blob_value(ctx, idx, value) do
            {:ok, materialized}
          end
        end
      
        defp read_waraft_segment_materialized(ctx, idx, file_id, key) do
          case Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(ctx, idx, file_id, key) do
            {:ok, value} ->
              materialize_blob_value(ctx, idx, value)
      
            :not_found ->
              :not_found
      
            {:error, reason} = error ->
              emit_waraft_segment_read_error(ctx, idx, file_id, reason, 1)
              error
          end
        end
      
        defp read_waraft_segment_file_ref_or_value(ctx, idx, file_id, key, validate_blob_ref?) do
          case Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(ctx, idx, file_id, key) do
            {:ok, value} ->
              case waraft_segment_blob_file_ref(ctx, idx, value, validate_blob_ref?) do
                {:ok, {path, value_offset, size}} ->
                  {:cold_ref, path, value_offset, size}
      
                :not_blob ->
                  case materialize_blob_value(ctx, idx, value) do
                    {:ok, materialized} -> {:cold_value, materialized}
                    {:error, reason} -> {:error, reason}
                  end
      
                {:error, reason} ->
                  {:error, reason}
              end
      
            :not_found ->
              :not_found
      
            {:error, reason} ->
              emit_waraft_segment_read_error(ctx, idx, file_id, reason, 1)
              {:error, reason}
          end
        end
      
        defp waraft_segment_blob_file_ref(ctx, idx, value, validate_blob_ref?) when is_binary(value) do
          if blob_ref_candidate?(ctx, byte_size(value)) do
            case BlobRef.decode(value) do
              {:ok, %BlobRef{} = ref} -> blob_ref_file_ref(ctx, idx, ref, validate_blob_ref?)
              :error -> :not_blob
            end
          else
            :not_blob
          end
        end
      
        defp waraft_segment_blob_file_ref(_ctx, _idx, _value, _validate_blob_ref?), do: :not_blob
      
        defp materialize_blob_value(ctx, idx, value) do
          BlobValue.maybe_materialize(ctx.data_dir, idx, BlobValue.threshold(ctx), value)
        end
      
        defp blob_ref_candidate?(ctx, value_size) do
          BlobValue.threshold(ctx) > 0 and BlobRef.encoded_size?(value_size)
        end
    end
  end
end
