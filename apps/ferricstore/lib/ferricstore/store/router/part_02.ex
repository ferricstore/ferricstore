defmodule Ferricstore.Store.Router.Part02 do
  @moduledoc false

  # Extracted from Router: do_get_with_file_ref .. batch_get_with_deferred_blob_file_refs_planned_and_presence
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
        defp do_get_with_file_ref(ctx, key, validate_blob_ref?) do
          idx = shard_for(ctx, key)
          keydir = resolve_keydir(ctx, idx)
          now = HLC.now_ms()
      
          case ets_get_full(ctx, idx, keydir, key, now) do
            {:hit, value, lfu} ->
              sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
              {:hot, value}
      
            {:cold, file_id, offset, value_size}
            when valid_cold_location(file_id, offset, value_size) ->
              path = cold_file_path(ctx, idx, file_id)
      
              case file_ref_from_cold_location(
                     ctx,
                     idx,
                     path,
                     offset,
                     key,
                     value_size,
                     validate_blob_ref?
                   ) do
                {:ok, {file_ref_path, value_offset, size}} ->
                  Stats.record_cold_read(ctx, key)
                  {:cold_ref, file_ref_path, value_offset, size}
      
                nil ->
                  case retry_changed_file_ref(ctx, idx, keydir, key, {file_id, offset, value_size}, now) do
                    {:cold_ref, retry_path, value_offset, retry_size} ->
                      Stats.record_cold_read(ctx, key)
                      {:cold_ref, retry_path, value_offset, retry_size}
      
                    {:hot, value} ->
                      {:hot, value}
      
                    :miss ->
                      record_keyspace_miss(ctx, key)
                      :miss
                  end
              end
      
            {:cold, file_id, offset, value_size}
            when valid_waraft_segment_location(file_id, offset, value_size) ->
              case read_waraft_segment_file_ref_or_value(ctx, idx, file_id, key, validate_blob_ref?) do
                {:cold_ref, path, value_offset, size} ->
                  Stats.record_cold_read(ctx, key)
                  {:cold_ref, path, value_offset, size}
      
                {:cold_value, value} when is_binary(value) ->
                  Stats.record_cold_read(ctx, key)
                  warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
                  {:cold_value, value}
      
                :not_found ->
                  retry_changed_waraft_segment_file_ref_result(
                    ctx,
                    idx,
                    keydir,
                    key,
                    {file_id, offset, value_size},
                    now,
                    validate_blob_ref?
                  )
      
                {:error, _reason} ->
                  retry_changed_waraft_segment_file_ref_result(
                    ctx,
                    idx,
                    keydir,
                    key,
                    {file_id, offset, value_size},
                    now,
                    validate_blob_ref?
                  )
              end
      
            {:cold, _file_id, _offset, _value_size} ->
              # Cold entry but no valid file ref. Ask the shard to flush pending
              # writes and return a file ref before falling back to materialization.
              shard_file_ref_or_value(ctx, idx, key)
      
            :expired ->
              record_keyspace_miss(ctx, key)
              :miss
      
            :miss ->
              if compound_data_structure_key?(ctx, keydir, key) do
                {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
              else
                # Key not in ETS = doesn't exist. No GenServer needed.
                record_keyspace_miss(ctx, key)
                :miss
              end
      
            :no_table ->
              # ETS table unavailable (shard restarting). Fall back to GenServer.
              shard_file_ref_or_value(ctx, idx, key)
          end
        end
      
        defp shard_file_ref_or_value(ctx, idx, key) do
          case safe_read_call(ctx, idx, {:get_file_ref, key}) do
            {:ok, {path, value_offset, value_size}}
            when is_binary(path) and is_integer(value_offset) and is_integer(value_size) ->
              Stats.record_cold_read(ctx, key)
              {:cold_ref, path, value_offset, value_size}
      
            _ ->
              result =
                case safe_read_call(ctx, idx, {:get, key}) do
                  {:ok, value} -> value
                  :unavailable -> nil
                end
      
              if result != nil do
                Stats.record_cold_read(ctx, key)
                {:cold_value, result}
              else
                record_keyspace_miss(ctx, key)
                :miss
              end
          end
        end
      
        defp compound_data_structure_key?(ctx, keydir, key) do
          case :ets.lookup(keydir, CompoundKey.type_key(key)) do
            [] -> false
            _ -> TypeRegistry.get_type(key, ctx) != "none"
          end
        rescue
          _ -> false
        end
      
        defp file_ref_from_cold_location(ctx, idx, path, offset, key, value_size, validate_blob_ref?) do
          if blob_ref_candidate?(ctx, value_size) do
            case cold_blob_file_ref_from_location(ctx, idx, path, offset, key, validate_blob_ref?) do
              {:ok, _file_ref} = ok ->
                ok
      
              :not_blob ->
                bitcask_file_ref_from_location(path, offset, key, value_size)
      
              {:error, _reason} ->
                nil
            end
          else
            bitcask_file_ref_from_location(path, offset, key, value_size)
          end
        end
      
        defp bitcask_file_ref_from_location(path, offset, key, value_size) do
          case validated_file_ref(path, offset, key, value_size) do
            {^path, value_offset, ^value_size} -> {:ok, {path, value_offset, value_size}}
            nil -> nil
          end
        end
      
        defp cold_blob_file_ref_from_location(ctx, idx, path, offset, key, validate_blob_ref?) do
          with {:ok, encoded_ref} <- read_cold_async(path, offset, key),
               {:ok, ref} <- BlobRef.decode(encoded_ref) do
            blob_ref_file_ref(ctx, idx, ref, validate_blob_ref?)
          else
            :error -> :not_blob
            {:error, reason} -> {:error, reason}
          end
        end
      
        defp blob_ref_file_ref(ctx, idx, %BlobRef{} = ref, _validate_blob_ref?) do
          BlobStore.file_ref(ctx.data_dir, idx, ref)
        end
      
        defp validated_file_ref(path, record_offset, key, value_size) do
          case Ferricstore.Bitcask.NIF.v2_validate_value_ref(path, record_offset, key, value_size) do
            {:ok, {value_offset, ^value_size}} ->
              {path, value_offset, value_size}
      
            _ ->
              maybe_run_validate_file_ref_miss_hook()
              nil
          end
        end
      
        defp retry_changed_file_ref(
               ctx,
               idx,
               keydir,
               key,
               original_location,
               now,
               validate_blob_ref? \\ true
             ) do
          case retry_changed_file_ref_once(
                 ctx,
                 idx,
                 keydir,
                 key,
                 original_location,
                 now,
                 validate_blob_ref?
               ) do
            :unchanged_cold ->
              retry_after_unchanged_cold_location(
                fn ->
                  retry_changed_file_ref_once(
                    ctx,
                    idx,
                    keydir,
                    key,
                    original_location,
                    now,
                    validate_blob_ref?
                  )
                end,
                cold_retry_metadata(ctx, idx, key, :file_ref)
              )
      
            result ->
              result
          end
        end
      
        defp retry_changed_file_ref_once(
               ctx,
               idx,
               keydir,
               key,
               original_location,
               now,
               validate_blob_ref?
             ) do
          case ets_get_full(ctx, idx, keydir, key, now) do
            {:hit, value, lfu} ->
              sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
              {:hot, value}
      
            {:cold, file_id, offset, value_size}
            when valid_cold_location(file_id, offset, value_size) and
                   {file_id, offset, value_size} != original_location ->
              path = cold_file_path(ctx, idx, file_id)
      
              case file_ref_from_cold_location(ctx, idx, path, offset, key, value_size, true) do
                {:ok, {file_ref_path, value_offset, size}} ->
                  {:cold_ref, file_ref_path, value_offset, size}
      
                nil ->
                  :miss
              end
      
            {:cold, file_id, offset, value_size}
            when valid_waraft_segment_location(file_id, offset, value_size) and
                   {file_id, offset, value_size} != original_location ->
              case read_waraft_segment_file_ref_or_value(ctx, idx, file_id, key, validate_blob_ref?) do
                {:cold_ref, file_ref_path, value_offset, size} ->
                  {:cold_ref, file_ref_path, value_offset, size}
      
                {:cold_value, value} when is_binary(value) ->
                  Stats.record_cold_read(ctx, key)
                  warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
                  {:hot, value}
      
                _ ->
                  :miss
              end
      
            {:cold, file_id, offset, value_size}
            when valid_cold_location(file_id, offset, value_size) and
                   {file_id, offset, value_size} == original_location ->
              :unchanged_cold
      
            {:cold, file_id, offset, value_size}
            when valid_waraft_segment_location(file_id, offset, value_size) and
                   {file_id, offset, value_size} == original_location ->
              :unchanged_cold
      
            _ ->
              :miss
          end
        end
      
        defp retry_changed_waraft_segment_file_ref_result(
               ctx,
               idx,
               keydir,
               key,
               original_location,
               now,
               validate_blob_ref?
             ) do
          case retry_changed_file_ref(ctx, idx, keydir, key, original_location, now, validate_blob_ref?) do
            {:cold_ref, retry_path, value_offset, retry_size} ->
              Stats.record_cold_read(ctx, key)
              {:cold_ref, retry_path, value_offset, retry_size}
      
            {:hot, value} ->
              {:hot, value}
      
            :miss ->
              record_keyspace_miss(ctx, key)
              :miss
          end
        end
      
        defp cold_file_path(ctx, idx, {:flow_history, file_id}) do
          ctx.data_dir
          |> Ferricstore.DataDir.shard_data_path(idx)
          |> Ferricstore.Flow.HistoryProjector.history_file_path(file_id)
        end
      
        defp cold_file_path(ctx, idx, file_id) do
          shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
          Path.join(shard_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
        end
      
        defp maybe_run_validate_file_ref_miss_hook do
          case Process.get(:ferricstore_router_validate_file_ref_miss_hook) do
            fun when is_function(fun, 0) -> fun.()
            _ -> :ok
          end
        end
      
        # Like ets_get but returns file ref info for cold entries and LFU counter for hits.
        # Single lookup provides everything needed — no second ETS read for bookkeeping.
        defp ets_get_full(ctx, idx, keydir, key, now) do
          try do
            case :ets.lookup(keydir, key) do
              [{^key, value, 0, lfu, _fid, _off, _vsize}] when value != nil ->
                {:hit, value, lfu}
      
              [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
                {:cold, fid, off, vsize}
      
              [{^key, nil, 0, _lfu, {:flow_history, file_id} = fid, off, vsize}]
              when is_integer(file_id) and file_id >= 0 and is_integer(off) and off >= 0 and
                     is_integer(vsize) and vsize >= 0 ->
                {:cold, fid, off, vsize}
      
              [{^key, nil, 0, _lfu, fid, off, vsize}]
              when valid_waraft_segment_location(fid, off, vsize) ->
                {:cold, fid, off, vsize}
      
              [{^key, nil, 0, _lfu, :pending, off, vsize}] ->
                {:cold, :pending, off, vsize}
      
              [{^key, value, exp, lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
                {:hit, value, lfu}
      
              [{^key, nil, exp, _lfu, fid, off, vsize}]
              when exp > now and valid_cold_location(fid, off, vsize) ->
                {:cold, fid, off, vsize}
      
              [{^key, nil, exp, _lfu, {:flow_history, file_id} = fid, off, vsize}]
              when exp > now and is_integer(file_id) and file_id >= 0 and is_integer(off) and
                     off >= 0 and is_integer(vsize) and vsize >= 0 ->
                {:cold, fid, off, vsize}
      
              [{^key, nil, exp, _lfu, fid, off, vsize}]
              when exp > now and valid_waraft_segment_location(fid, off, vsize) ->
                {:cold, fid, off, vsize}
      
              [{^key, nil, exp, _lfu, :pending, off, vsize}] when exp > now ->
                {:cold, :pending, off, vsize}
      
              [{^key, value, _exp, _lfu, _fid, _off, _vsize}] ->
                track_keydir_binary_delete_known(ctx, idx, key, value)
                :ets.delete(keydir, key)
                :expired
      
              [] ->
                :miss
            end
          rescue
            ArgumentError -> :no_table
          end
        end
      
        defp ets_get_meta_full(ctx, idx, keydir, key, now) do
          try do
            case :ets.lookup(keydir, key) do
              [{^key, value, exp, lfu, _fid, _off, _vsize}]
              when value != nil and (exp == 0 or exp > now) ->
                {:hit, value, exp, lfu}
      
              [{^key, nil, exp, _lfu, fid, off, vsize}]
              when (exp == 0 or exp > now) and valid_cold_location(fid, off, vsize) ->
                {:cold, fid, off, vsize, exp}
      
              [{^key, nil, exp, _lfu, {:flow_history, file_id} = fid, off, vsize}]
              when (exp == 0 or exp > now) and is_integer(file_id) and file_id >= 0 and
                     is_integer(off) and off >= 0 and is_integer(vsize) and vsize >= 0 ->
                {:cold, fid, off, vsize, exp}
      
              [{^key, nil, exp, _lfu, fid, off, vsize}]
              when (exp == 0 or exp > now) and valid_waraft_segment_location(fid, off, vsize) ->
                {:cold, fid, off, vsize, exp}
      
              [{^key, nil, exp, _lfu, :pending, off, vsize}] when exp == 0 or exp > now ->
                {:cold, :pending, off, vsize, exp}
      
              [{^key, value, _exp, _lfu, _fid, _off, _vsize}] ->
                track_keydir_binary_delete_known(ctx, idx, key, value)
                :ets.delete(keydir, key)
                :expired
      
              [] ->
                :miss
            end
          rescue
            ArgumentError -> :no_table
          end
        end
      
        @doc """
        Retrieves the value for `key`, or `nil` if the key does not exist or is
        expired.
      
        Hot path: reads directly from ETS (no GenServer roundtrip for cached keys).
        Falls back to a GenServer call for cache misses or when the ETS table is
        temporarily unavailable (e.g. during a shard restart).
      
        Each successful read is recorded as either *hot* (ETS hit) or *cold*
        (Bitcask fallback) in `Ferricstore.Stats` for the `FERRICSTORE.HOTNESS`
        command and the `INFO stats` hot/cold fields.
        """
        @spec get(FerricStore.Instance.t(), binary()) :: binary() | nil
        def get(ctx, key) do
          idx = shard_for(ctx, key)
          keydir = resolve_keydir(ctx, idx)
          now = HLC.now_ms()
      
          case ets_get_full(ctx, idx, keydir, key, now) do
            {:hit, value, lfu} ->
              sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
              value
      
            {:cold, file_id, offset, value_size}
            when valid_cold_location(file_id, offset, value_size) ->
              # Cold key — value evicted from ETS but disk location known.
              # Read directly from Bitcask via NIF, bypassing the Shard GenServer.
              # The ETS entry has valid file_id/offset from when the write committed,
              # so pread works without flushing pending writes.
              path = cold_file_path(ctx, idx, file_id)
      
              case read_cold_materialized(ctx, idx, path, offset, key) do
                {:ok, value} when is_binary(value) ->
                  Stats.record_cold_read(ctx, key)
                  # Warm ETS: promote back to hot if value fits in cache
                  warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
                  value
      
                _ ->
                  case retry_changed_cold_value(
                         ctx,
                         idx,
                         keydir,
                         key,
                         {file_id, offset, value_size},
                         now
                       ) do
                    {:cold, value, retry_file_id, retry_offset} ->
                      Stats.record_cold_read(ctx, key)
      
                      warm_ets_after_cold_read(
                        ctx,
                        idx,
                        keydir,
                        key,
                        value,
                        retry_file_id,
                        retry_offset
                      )
      
                      value
      
                    {:hot, value} ->
                      value
      
                    :miss ->
                      record_keyspace_miss(ctx, key)
                      nil
                  end
              end
      
            {:cold, file_id, offset, value_size}
            when valid_waraft_segment_location(file_id, offset, value_size) ->
              case read_waraft_segment_materialized(ctx, idx, file_id, key) do
                {:ok, value} when is_binary(value) ->
                  Stats.record_cold_read(ctx, key)
                  warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
                  value
      
                :not_found ->
                  retry_changed_waraft_segment_value_result(
                    ctx,
                    idx,
                    keydir,
                    key,
                    {file_id, offset, value_size},
                    now
                  )
      
                {:error, _reason} ->
                  retry_changed_waraft_segment_value_result(
                    ctx,
                    idx,
                    keydir,
                    key,
                    {file_id, offset, value_size},
                    now
                  )
              end
      
            {:cold, _file_id, _offset, _value_size} ->
              # Cold entry but invalid file ref — ask GenServer.
              result =
                case safe_read_call(ctx, idx, {:get, key}) do
                  {:ok, value} -> value
                  :unavailable -> nil
                end
      
              if result != nil do
                Stats.record_cold_read(ctx, key)
              else
                record_keyspace_miss(ctx, key)
              end
      
              result
      
            :expired ->
              record_keyspace_miss(ctx, key)
              nil
      
            :miss ->
              # Key not in ETS at all — doesn't exist. No GenServer needed.
              record_keyspace_miss(ctx, key)
              nil
      
            :no_table ->
              # ETS table unavailable (shard restarting). Fall back to GenServer.
              result =
                case safe_read_call(ctx, idx, {:get, key}) do
                  {:ok, value} -> value
                  :unavailable -> nil
                end
      
              if result != nil do
                Stats.record_cold_read(ctx, key)
              else
                record_keyspace_miss(ctx, key)
              end
      
              result
          end
        end
      
        @doc false
        @spec get_with_deferred_blob_file_ref(FerricStore.Instance.t(), binary()) ::
                {:hot, binary()}
                | {:cold_ref, binary(), non_neg_integer(), non_neg_integer()}
                | {:cold_value, binary()}
                | {:error, binary()}
                | :miss
        def get_with_deferred_blob_file_ref(ctx, key), do: do_get_with_file_ref(ctx, key, false)
      
        @spec batch_get(FerricStore.Instance.t(), [binary()]) :: [binary() | nil]
        def batch_get(ctx, keys) do
          now = HLC.now_ms()
      
          {results, {cold_entries, _cold_count, waraft_entries, _waraft_count, hot_hits}} =
            Enum.map_reduce(keys, {[], 0, [], 0, []}, fn key,
                                                         {cold_entries, cold_count, waraft_entries,
                                                          waraft_count, hot_hits} ->
              idx = shard_for(ctx, key)
              keydir = resolve_keydir(ctx, idx)
      
              case ets_get_full(ctx, idx, keydir, key, now) do
                {:hit, value, lfu} ->
                  {{:value, value},
                   {cold_entries, cold_count, waraft_entries, waraft_count,
                    [{keydir, key, lfu} | hot_hits]}}
      
                {:cold, file_id, offset, value_size}
                when valid_cold_location(file_id, offset, value_size) ->
                  path = cold_file_path(ctx, idx, file_id)
      
                  entry = {ctx, idx, keydir, key, path, file_id, offset, value_size}
      
                  {{:cold, cold_count},
                   {[entry | cold_entries], cold_count + 1, waraft_entries, waraft_count, hot_hits}}
      
                {:cold, file_id, offset, value_size}
                when valid_waraft_segment_location(file_id, offset, value_size) ->
                  entry = {ctx, idx, keydir, key, file_id, offset, value_size}
      
                  {{:waraft, waraft_count},
                   {cold_entries, cold_count, [entry | waraft_entries], waraft_count + 1, hot_hits}}
      
                {:cold, _file_id, _offset, _value_size} ->
                  result =
                    case safe_read_call(ctx, idx, {:get, key}) do
                      {:ok, value} -> value
                      :unavailable -> nil
                    end
      
                  if result != nil do
                    Stats.record_cold_read(ctx, key)
                  else
                    record_keyspace_miss(ctx, key)
                  end
      
                  {{:value, result}, {cold_entries, cold_count, waraft_entries, waraft_count, hot_hits}}
      
                :expired ->
                  record_keyspace_miss(ctx, key)
                  {{:value, nil}, {cold_entries, cold_count, waraft_entries, waraft_count, hot_hits}}
      
                :miss ->
                  record_keyspace_miss(ctx, key)
                  {{:value, nil}, {cold_entries, cold_count, waraft_entries, waraft_count, hot_hits}}
      
                :no_table ->
                  result =
                    case safe_read_call(ctx, idx, {:get, key}) do
                      {:ok, value} -> value
                      :unavailable -> nil
                    end
      
                  if result != nil do
                    Stats.record_cold_read(ctx, key)
                  else
                    record_keyspace_miss(ctx, key)
                  end
      
                  {{:value, result}, {cold_entries, cold_count, waraft_entries, waraft_count, hot_hits}}
              end
            end)
      
          sampled_read_bookkeeping_batch(ctx, Enum.reverse(hot_hits), length(hot_hits))
      
          cold_values =
            cold_entries
            |> Enum.reverse()
            |> read_cold_batch_async(now)
            |> List.to_tuple()
      
          waraft_values =
            waraft_entries
            |> Enum.reverse()
            |> read_waraft_segment_batch_materialized()
            |> List.to_tuple()
      
          Enum.map(results, fn
            {:value, value} -> value
            {:cold, index} -> elem(cold_values, index)
            {:waraft, index} -> elem(waraft_values, index)
          end)
        end
      
        @doc false
        @spec batch_get_planned(FerricStore.Instance.t(), [tuple()]) :: [binary() | nil]
        def batch_get_planned(ctx, planned_keys) do
          keys = Enum.map(planned_keys, &planned_lookup_key/1)
          batch_get(ctx, keys)
        end
      
        @doc """
        Batch GET variant for TCP large-value streaming.
      
        It performs the same single ETS pass as `batch_get/2`, but cold entries whose
        value size is at least `min_file_ref_size` are returned as validated
        `{:file_ref, path, value_offset, size}` tuples instead of being materialized
        into BEAM binaries. Stale or invalid refs fall back to the normal batched cold
        pread path.
        """
        @spec batch_get_with_file_refs(FerricStore.Instance.t(), [binary()], non_neg_integer()) :: [
                binary() | nil | {:file_ref, binary(), non_neg_integer(), non_neg_integer()}
              ]
        def batch_get_with_file_refs(ctx, keys, min_file_ref_size) do
          do_batch_get_with_file_refs(ctx, keys, min_file_ref_size, true)
        end
      
        defp do_batch_get_with_file_refs(ctx, keys, min_file_ref_size, validate_blob_ref?) do
          now = HLC.now_ms()
      
          {results, {cold_entries, _cold_count, waraft_entries, _waraft_count, hot_hits}} =
            Enum.map_reduce(keys, {[], 0, [], 0, []}, fn key,
                                                         {cold_entries, cold_count, waraft_entries,
                                                          waraft_count, hot_hits} ->
              idx = shard_for(ctx, key)
              keydir = resolve_keydir(ctx, idx)
      
              case ets_get_full(ctx, idx, keydir, key, now) do
                {:hit, value, lfu} ->
                  {{:value, value},
                   {cold_entries, cold_count, waraft_entries, waraft_count,
                    [{keydir, key, lfu} | hot_hits]}}
      
                {:cold, file_id, offset, value_size}
                when valid_cold_location(file_id, offset, value_size) ->
                  path = cold_file_path(ctx, idx, file_id)
      
                  maybe_file_ref_or_cold_entry(
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
      
                {:cold, file_id, offset, value_size}
                when valid_waraft_segment_location(file_id, offset, value_size) ->
                  entry = {ctx, idx, keydir, key, file_id, offset, value_size}
      
                  {{:waraft, waraft_count},
                   {cold_entries, cold_count, [entry | waraft_entries], waraft_count + 1, hot_hits}}
      
                {:cold, _file_id, _offset, _value_size} ->
                  result =
                    case safe_read_call(ctx, idx, {:get, key}) do
                      {:ok, value} -> value
                      :unavailable -> nil
                    end
      
                  if result != nil do
                    Stats.record_cold_read(ctx, key)
                  else
                    record_keyspace_miss(ctx, key)
                  end
      
                  {{:value, result}, {cold_entries, cold_count, waraft_entries, waraft_count, hot_hits}}
      
                :expired ->
                  record_keyspace_miss(ctx, key)
                  {{:value, nil}, {cold_entries, cold_count, waraft_entries, waraft_count, hot_hits}}
      
                :miss ->
                  record_keyspace_miss(ctx, key)
                  {{:value, nil}, {cold_entries, cold_count, waraft_entries, waraft_count, hot_hits}}
      
                :no_table ->
                  result =
                    case safe_read_call(ctx, idx, {:get, key}) do
                      {:ok, value} -> value
                      :unavailable -> nil
                    end
      
                  if result != nil do
                    Stats.record_cold_read(ctx, key)
                  else
                    record_keyspace_miss(ctx, key)
                  end
      
                  {{:value, result}, {cold_entries, cold_count, waraft_entries, waraft_count, hot_hits}}
              end
            end)
      
          sampled_read_bookkeeping_batch(ctx, Enum.reverse(hot_hits), length(hot_hits))
      
          cold_values =
            cold_entries
            |> Enum.reverse()
            |> read_cold_batch_file_ref_async(now, min_file_ref_size, validate_blob_ref?)
            |> List.to_tuple()
      
          waraft_values =
            waraft_entries
            |> Enum.reverse()
            |> read_waraft_segment_batch_file_ref_or_materialized(
              min_file_ref_size,
              validate_blob_ref?
            )
            |> List.to_tuple()
      
          Enum.map(results, fn
            {:value, value} -> value
            {:file_ref, path, offset, size} -> {:file_ref, path, offset, size}
            {:cold, index} -> elem(cold_values, index)
            {:waraft, index} -> elem(waraft_values, index)
          end)
        end
      
        @doc false
        @spec batch_get_with_deferred_blob_file_refs(
                FerricStore.Instance.t(),
                [binary()],
                non_neg_integer()
              ) :: [binary() | nil | {:file_ref, binary(), non_neg_integer(), non_neg_integer()}]
        def batch_get_with_deferred_blob_file_refs(ctx, keys, min_file_ref_size) do
          do_batch_get_with_file_refs(ctx, keys, min_file_ref_size, false)
        end
      
        @doc false
        @spec batch_get_with_deferred_blob_file_refs_and_presence(
                FerricStore.Instance.t(),
                [binary()],
                non_neg_integer()
              ) ::
                {[binary() | nil | {:file_ref, binary(), non_neg_integer(), non_neg_integer()}],
                 boolean()}
        def batch_get_with_deferred_blob_file_refs_and_presence(ctx, keys, min_file_ref_size) do
          results = do_batch_get_with_file_refs(ctx, keys, min_file_ref_size, false)
          {results, Enum.any?(results, &file_ref_read_result?/1)}
        end
      
        @doc false
        @spec batch_get_with_deferred_blob_file_refs_planned_and_presence(
                FerricStore.Instance.t(),
                [tuple()],
                non_neg_integer()
              ) ::
                {[binary() | nil | {:file_ref, binary(), non_neg_integer(), non_neg_integer()}],
                 boolean()}
        def batch_get_with_deferred_blob_file_refs_planned_and_presence(
              ctx,
              planned_keys,
              min_file_ref_size
            ) do
          keys = Enum.map(planned_keys, &planned_lookup_key/1)
          batch_get_with_deferred_blob_file_refs_and_presence(ctx, keys, min_file_ref_size)
        end
    end
  end
end
