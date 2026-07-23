defmodule Ferricstore.Store.Router.Part02 do
  @moduledoc false

  # Extracted from Router: do_get_with_file_ref .. batch_get_with_deferred_blob_file_refs_planned_and_presence
  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.CommandTime
      alias Ferricstore.ErrorReasons
      alias Ferricstore.ExpiryContext
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
      alias Ferricstore.Store.ExpiryTracker
      alias Ferricstore.Store.Keydir
      alias Ferricstore.Store.LFU
      alias Ferricstore.Store.ListOps
      alias Ferricstore.Store.PublicationEpoch
      alias Ferricstore.Store.ReadResult
      alias Ferricstore.Store.Router
      alias Ferricstore.Store.SlotMap
      alias Ferricstore.Store.TypeRegistry

      defp do_get_with_file_ref(ctx, key, validate_blob_ref?) do
        idx = shard_for(ctx, key)
        keydir = resolve_keydir(ctx, idx)
        expiry_context = ExpiryContext.capture()

        case ets_get_full(ctx, idx, keydir, key, expiry_context) do
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
                case retry_changed_file_ref(
                       ctx,
                       idx,
                       keydir,
                       key,
                       {file_id, offset, value_size},
                       expiry_context
                     ) do
                  {:cold_ref, retry_path, value_offset, retry_size} ->
                    Stats.record_cold_read(ctx, key)
                    {:cold_ref, retry_path, value_offset, retry_size}

                  {:hot, value} ->
                    {:hot, value}

                  :miss ->
                    record_keyspace_miss(ctx, key)
                    :miss

                  {:error, {:storage_read_failed, _reason}} = failure ->
                    failure
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
                  expiry_context,
                  validate_blob_ref?
                )

              {:error, _reason} ->
                retry_changed_waraft_segment_file_ref_result(
                  ctx,
                  idx,
                  keydir,
                  key,
                  {file_id, offset, value_size},
                  expiry_context,
                  validate_blob_ref?
                )
            end

          {:cold, _file_id, _offset, _value_size} ->
            # Cold entry but no valid file ref. Ask the shard to flush pending
            # writes and return a file ref before falling back to materialization.
            shard_file_ref_or_value(ctx, idx, key)

          {:invalid, entry} ->
            ReadResult.failure({:invalid_keydir_entry, entry})

          :hlc_drift_exceeded ->
            ReadResult.failure(:hlc_drift_exceeded)

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

      defp file_ref_from_cold_location(
             ctx,
             idx,
             path,
             offset,
             key,
             value_size,
             validate_blob_ref?
           ) do
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
          {file_ref_path, value_offset, ^value_size} ->
            {:ok, {file_ref_path, value_offset, value_size}}

          nil ->
            nil
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
        case validate_file_ref_at_path(path, record_offset, key, value_size) do
          {_path, _value_offset, _value_size} = file_ref ->
            file_ref

          nil ->
            maybe_run_validate_file_ref_miss_hook()

            path
            |> Ferricstore.Store.ColdRead.compaction_backup_path()
            |> case do
              nil -> nil
              backup -> validate_file_ref_at_path(backup, record_offset, key, value_size)
            end
        end
      end

      defp validate_file_ref_at_path(path, record_offset, key, value_size) do
        case Ferricstore.Bitcask.NIF.v2_validate_value_ref(path, record_offset, key, value_size) do
          {:ok, {value_offset, ^value_size}} -> {path, value_offset, value_size}
          _ -> nil
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
        case retry_changed_file_ref_raw(
               ctx,
               idx,
               keydir,
               key,
               original_location,
               now,
               validate_blob_ref?,
               :miss
             ) do
          {:read_error, _reason} -> :miss
          result -> result
        end
      end

      defp retry_changed_file_ref_result(
             ctx,
             idx,
             keydir,
             key,
             original_location,
             now,
             exhausted_failure,
             validate_blob_ref? \\ true
           ) do
        case retry_changed_file_ref_raw(
               ctx,
               idx,
               keydir,
               key,
               original_location,
               now,
               validate_blob_ref?,
               exhausted_failure
             ) do
          {:read_error, reason} -> storage_read_failure(:cold_file_ref_unavailable, reason)
          result -> result
        end
      end

      defp retry_changed_file_ref_raw(
             ctx,
             idx,
             keydir,
             key,
             original_location,
             now,
             validate_blob_ref?,
             exhausted_result
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
              cold_retry_metadata(ctx, idx, key, :file_ref),
              exhausted_result
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
                {:read_error, :invalid_cold_file_ref}
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

              read_error ->
                {:read_error, storage_read_reason(read_error)}
            end

          {:cold, file_id, offset, value_size}
          when valid_cold_location(file_id, offset, value_size) and
                 {file_id, offset, value_size} == original_location ->
            :unchanged_cold

          {:cold, file_id, offset, value_size}
          when valid_waraft_segment_location(file_id, offset, value_size) and
                 {file_id, offset, value_size} == original_location ->
            :unchanged_cold

          {:invalid, entry} ->
            {:read_error, {:invalid_keydir_entry, entry}}

          :hlc_drift_exceeded ->
            ReadResult.failure(:hlc_drift_exceeded)

          :no_table ->
            {:read_error, :keydir_unavailable}

          :expired ->
            :miss

          :miss ->
            :miss

          {:cold, file_id, offset, value_size} ->
            {:read_error, {:invalid_cold_location, file_id, offset, value_size}}
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
        case retry_changed_file_ref(
               ctx,
               idx,
               keydir,
               key,
               original_location,
               now,
               validate_blob_ref?
             ) do
          {:cold_ref, retry_path, value_offset, retry_size} ->
            Stats.record_cold_read(ctx, key)
            {:cold_ref, retry_path, value_offset, retry_size}

          {:hot, value} ->
            {:hot, value}

          :miss ->
            record_keyspace_miss(ctx, key)
            :miss

          {:error, {:storage_read_failed, _reason}} = failure ->
            failure
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
      defp ets_get_full(ctx, idx, keydir, key, expiry_context) do
        expiry_context = ExpiryContext.normalize(expiry_context)
        now = ExpiryContext.now_ms(expiry_context)

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

            [{^key, _value, exp, _lfu, _fid, _off, _vsize} = expired_entry]
            when is_integer(exp) and exp > 0 and exp <= now ->
              case ExpiryContext.classify(expiry_context, exp) do
                {:unsafe, :hlc_drift_exceeded} ->
                  :hlc_drift_exceeded

                :expired ->
                  delete_observed_keydir_entry(ctx, idx, keydir, expired_entry)
                  :expired
              end

            [entry] ->
              {:invalid, entry}

            [] ->
              :miss
          end
        rescue
          ArgumentError -> :no_table
        end
      end

      defp ets_get_meta_full(ctx, idx, keydir, key, expiry_context) do
        expiry_context = ExpiryContext.normalize(expiry_context)
        now = ExpiryContext.now_ms(expiry_context)

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

            [{^key, _value, exp, _lfu, _fid, _off, _vsize} = expired_entry]
            when is_integer(exp) and exp > 0 and exp <= now ->
              case ExpiryContext.classify(expiry_context, exp) do
                {:unsafe, :hlc_drift_exceeded} ->
                  :hlc_drift_exceeded

                :expired ->
                  delete_observed_keydir_entry(ctx, idx, keydir, expired_entry)
                  :expired
              end

            [entry] ->
              {:invalid, entry}

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
      @spec get(FerricStore.Instance.t(), binary()) :: binary() | nil | ReadResult.failure()
      def get(ctx, key) do
        idx = shard_for(ctx, key)
        keydir = resolve_keydir(ctx, idx)
        expiry_context = ExpiryContext.capture()

        case ets_get_full(ctx, idx, keydir, key, expiry_context) do
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

              read_error ->
                failure = storage_read_failure(:cold_value_unavailable, read_error)

                case retry_changed_cold_value_result(
                       ctx,
                       idx,
                       keydir,
                       key,
                       {file_id, offset, value_size},
                       expiry_context,
                       failure
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

                  {:error, {:storage_read_failed, _reason}} = failure ->
                    failure
                end
            end

          {:cold, file_id, offset, value_size}
          when valid_waraft_segment_location(file_id, offset, value_size) ->
            case read_waraft_segment_materialized(ctx, idx, file_id, key) do
              {:ok, value} when is_binary(value) ->
                Stats.record_cold_read(ctx, key)
                warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
                value

              :not_found = read_error ->
                retry_changed_waraft_segment_value_read_result(
                  ctx,
                  idx,
                  keydir,
                  key,
                  {file_id, offset, value_size},
                  expiry_context,
                  read_error
                )

              {:error, _reason} = read_error ->
                retry_changed_waraft_segment_value_read_result(
                  ctx,
                  idx,
                  keydir,
                  key,
                  {file_id, offset, value_size},
                  expiry_context,
                  read_error
                )
            end

          {:cold, file_id, offset, value_size} ->
            ReadResult.failure({:invalid_cold_location, {file_id, offset, value_size}})

          {:invalid, entry} ->
            ReadResult.failure({:invalid_keydir_entry, entry})

          :hlc_drift_exceeded ->
            ReadResult.failure(:hlc_drift_exceeded)

          :expired ->
            record_keyspace_miss(ctx, key)
            nil

          :miss ->
            # Key not in ETS at all — doesn't exist. No GenServer needed.
            record_keyspace_miss(ctx, key)
            nil

          :no_table ->
            # ETS table unavailable (shard restarting). Fall back to GenServer.
            case safe_read_call(ctx, idx, {:get, key}) do
              {:ok, value} ->
                if value != nil do
                  Stats.record_cold_read(ctx, key)
                else
                  record_keyspace_miss(ctx, key)
                end

                value

              :unavailable ->
                ReadResult.failure(:keydir_unavailable)
            end
        end
      end

      defp storage_read_failure(operation, read_result) do
        ReadResult.failure({operation, storage_read_reason(read_result)})
      end

      defp storage_read_reason({:error, reason}), do: reason
      defp storage_read_reason(reason), do: reason

      @doc false
      @spec get_with_deferred_blob_file_ref(FerricStore.Instance.t(), binary()) ::
              {:hot, binary()}
              | {:cold_ref, binary(), non_neg_integer(), non_neg_integer()}
              | {:cold_value, binary()}
              | {:error, binary()}
              | :miss
      def get_with_deferred_blob_file_ref(ctx, key), do: do_get_with_file_ref(ctx, key, false)

      @spec batch_get(FerricStore.Instance.t(), [binary()]) ::
              [binary() | nil | ReadResult.failure()]
      def batch_get(ctx, keys) do
        PublicationEpoch.read(ctx, publication_shards(ctx, keys), fn ->
          {:ok, values} = do_batch_get(ctx, keys, :unlimited)
          values
        end)
      end

      @doc false
      @spec get_bounded(FerricStore.Instance.t(), binary(), non_neg_integer() | :unlimited) ::
              {:ok, binary() | nil} | {:error, :response_byte_limit} | ReadResult.failure()
      def get_bounded(ctx, key, :unlimited) when is_binary(key) do
        case get(ctx, key) do
          {:error, {:storage_read_failed, _reason}} = failure -> failure
          value -> {:ok, value}
        end
      end

      def get_bounded(ctx, key, max_value_bytes)
          when is_binary(key) and is_integer(max_value_bytes) and max_value_bytes >= 0 do
        case batch_get_bounded(ctx, [key], max_value_bytes) do
          {:ok, [value]} -> {:ok, value}
          {:error, :response_byte_limit} = error -> error
          {:error, {:storage_read_failed, _reason}} = failure -> failure
        end
      end

      @doc false
      @spec batch_get_bounded(FerricStore.Instance.t(), [binary()], non_neg_integer()) ::
              {:ok, [binary() | nil]} | {:error, :response_byte_limit} | ReadResult.failure()
      def batch_get_bounded(ctx, keys, max_value_bytes)
          when is_list(keys) and is_integer(max_value_bytes) and max_value_bytes >= 0 do
        PublicationEpoch.read(ctx, publication_shards(ctx, keys), fn ->
          expiry_context = ExpiryContext.capture()
          read_plan = do_batch_get_with_file_refs(ctx, keys, 0, :bounded, expiry_context)

          with {:ok, _planned_reads?} <- batch_get_preflight(read_plan, max_value_bytes) do
            values = materialize_bounded_batch_values(read_plan, expiry_context)

            case batch_get_preflight(values, max_value_bytes) do
              {:ok, _file_refs?} -> {:ok, values}
              {:error, :response_byte_limit} = error -> error
              {:error, {:storage_read_failed, _reason}} = failure -> failure
            end
          end
        end)
      end

      @doc false
      @spec batch_get_each_bounded(FerricStore.Instance.t(), [binary()], non_neg_integer()) ::
              {:ok, [binary() | nil]} | {:error, :response_byte_limit} | ReadResult.failure()
      def batch_get_each_bounded(ctx, keys, max_value_bytes)
          when is_list(keys) and is_integer(max_value_bytes) and max_value_bytes >= 0 do
        PublicationEpoch.read(ctx, publication_shards(ctx, keys), fn ->
          expiry_context = ExpiryContext.capture()
          read_plan = do_batch_get_with_file_refs(ctx, keys, 0, :bounded, expiry_context)

          with :ok <- batch_get_each_preflight(read_plan, max_value_bytes) do
            values = materialize_bounded_batch_values(read_plan, expiry_context)

            case batch_get_each_preflight(values, max_value_bytes) do
              :ok -> {:ok, values}
              {:error, :response_byte_limit} = error -> error
              {:error, {:storage_read_failed, _reason}} = failure -> failure
            end
          end
        end)
      end

      defp publication_shards(ctx, keys), do: Enum.map(keys, &shard_for(ctx, &1))

      defp batch_get_each_preflight(values, max_value_bytes) do
        Enum.reduce_while(values, :ok, fn value, :ok ->
          case value do
            {:error, {:storage_read_failed, _reason}} ->
              {:cont, :ok}

            _value ->
              {value_bytes, _file_ref?} = batch_get_preflight_value(value)

              if value_bytes > max_value_bytes,
                do: {:halt, {:error, :response_byte_limit}},
                else: {:cont, :ok}
          end
        end)
      end

      defp batch_get_preflight(values, max_value_bytes) do
        Enum.reduce_while(values, {:ok, 0, false}, fn value, {:ok, bytes, file_refs?} ->
          case value do
            {:error, {:storage_read_failed, _reason}} = failure ->
              {:halt, failure}

            _value ->
              {value_bytes, file_ref?} = batch_get_preflight_value(value)
              next_bytes = bytes + value_bytes

              if next_bytes > max_value_bytes do
                {:halt, {:error, :response_byte_limit}}
              else
                {:cont, {:ok, next_bytes, file_refs? or file_ref?}}
              end
          end
        end)
        |> case do
          {:ok, _bytes, file_refs?} -> {:ok, file_refs?}
          {:error, :response_byte_limit} = error -> error
          {:error, {:storage_read_failed, _reason}} = failure -> failure
        end
      end

      defp batch_get_preflight_value({:file_ref, _path, _offset, size})
           when is_integer(size) and size >= 0,
           do: {size, true}

      defp batch_get_preflight_value(
             {:bounded_cold, {_ctx, _idx, _keydir, _key, _path, _file_id, _offset, value_size}}
           )
           when is_integer(value_size) and value_size >= 0,
           do: {value_size, true}

      defp batch_get_preflight_value(
             {:bounded_waraft, {_ctx, _idx, _keydir, _key, _file_id, _offset, value_size}}
           )
           when is_integer(value_size) and value_size >= 0,
           do: {value_size, true}

      defp batch_get_preflight_value(
             {:bounded_blob, _ctx, _idx, _keydir, _key, _file_id, _offset, _value_size,
              %BlobRef{size: blob_size}}
           )
           when is_integer(blob_size) and blob_size >= 0,
           do: {blob_size, true}

      defp batch_get_preflight_value(value) when is_binary(value), do: {byte_size(value), false}
      defp batch_get_preflight_value(nil), do: {0, false}

      defp materialize_bounded_batch_values(read_plan, expiry_context) do
        {cold_plans, blob_plans} =
          read_plan
          |> Enum.with_index()
          |> Enum.reduce({[], []}, fn
            {{:bounded_cold, entry}, index}, {cold_plans, blob_plans} ->
              {[{index, entry} | cold_plans], blob_plans}

            {{:bounded_blob, _ctx, _idx, _keydir, _key, _file_id, _offset, _value_size,
              %BlobRef{}} = plan, index},
            {cold_plans, blob_plans} ->
              {cold_plans, [{index, plan} | blob_plans]}

            {_value, _index}, plans ->
              plans
          end)

        waraft_plans =
          read_plan
          |> Enum.with_index()
          |> Enum.flat_map(fn
            {{:bounded_waraft, entry}, index} -> [{index, entry}]
            {_value, _index} -> []
          end)

        cold_results = materialize_bounded_cold_plans(Enum.reverse(cold_plans), expiry_context)
        waraft_results = materialize_bounded_waraft_plans(waraft_plans, expiry_context)
        blob_results = materialize_bounded_blob_plans(Enum.reverse(blob_plans), expiry_context)

        read_plan
        |> Enum.with_index()
        |> Enum.map(fn
          {{:bounded_cold, _entry}, index} ->
            Map.fetch!(cold_results, index)

          {{:bounded_waraft, _entry}, index} ->
            Map.fetch!(waraft_results, index)

          {{:bounded_blob, _ctx, _idx, _keydir, _key, _file_id, _offset, _value_size, _ref},
           index} ->
            Map.fetch!(blob_results, index)

          {value, _index} ->
            value
        end)
      end

      defp materialize_bounded_cold_plans([], _now), do: %{}

      defp materialize_bounded_cold_plans(indexed_entries, now) do
        {indexes, entries} = Enum.unzip(indexed_entries)
        values = read_cold_batch_async(entries, now)
        indexes |> Enum.zip(values) |> Map.new()
      end

      defp materialize_bounded_waraft_plans([], _expiry_context), do: %{}

      defp materialize_bounded_waraft_plans(indexed_entries, expiry_context) do
        {indexes, entries} = Enum.unzip(indexed_entries)
        values = read_waraft_segment_batch_materialized(entries, expiry_context)
        indexes |> Enum.zip(values) |> Map.new()
      end

      defp materialize_bounded_blob_plans([], _now), do: %{}

      defp materialize_bounded_blob_plans(indexed_plans, now) do
        indexed_plans
        |> Enum.group_by(fn
          {_index, {:bounded_blob, ctx, idx, _keydir, _key, _file_id, _offset, _value_size, _ref}} ->
            {ctx.data_dir, idx}
        end)
        |> Enum.reduce(%{}, fn {{data_dir, idx}, group}, results_by_index ->
          refs = Enum.map(group, fn {_index, plan} -> elem(plan, 8) end)
          loaded = BlobStore.get_many(data_dir, idx, refs)

          group
          |> Enum.zip(loaded)
          |> Enum.reduce(results_by_index, fn {{index, plan}, result}, acc ->
            Map.put(acc, index, bounded_blob_result(plan, result, now))
          end)
        end)
      end

      defp bounded_blob_result(
             {:bounded_blob, ctx, idx, keydir, key, file_id, offset, _value_size, _ref},
             {:ok, value},
             _now
           )
           when is_binary(value) do
        warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
        value
      end

      defp bounded_blob_result(
             {:bounded_blob, ctx, idx, keydir, key, file_id, offset, value_size, _ref},
             _error,
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

      defp do_batch_get(ctx, keys, byte_limit),
        do: do_batch_get(ctx, keys, byte_limit, nil)

      defp do_batch_get_from_shard(ctx, idx, keys, byte_limit)
           when is_integer(idx) and idx >= 0 do
        do_batch_get(ctx, keys, byte_limit, idx, :values)
      end

      defp do_batch_get_entries_from_shard(ctx, idx, keys, byte_limit)
           when is_integer(idx) and idx >= 0 do
        do_batch_get(ctx, keys, byte_limit, idx, :entries)
      end

      defp do_batch_get(ctx, keys, byte_limit, fixed_shard_index) do
        do_batch_get(ctx, keys, byte_limit, fixed_shard_index, :values)
      end

      defp do_batch_get(ctx, keys, byte_limit, fixed_shard_index, result_mode) do
        expiry_context = ExpiryContext.capture()
        bookkeeping = hot_read_bookkeeping_start(ctx)

        plan =
          Enum.reduce_while(
            keys,
            {[], [], 0, [], 0, bookkeeping, 0},
            fn key,
               {results, cold_entries, cold_count, waraft_entries, waraft_count, bookkeeping,
                response_bytes} ->
              idx =
                if is_integer(fixed_shard_index),
                  do: fixed_shard_index,
                  else: shard_for(ctx, key)

              keydir = resolve_keydir(ctx, idx)

              {result, cold_entries, cold_count, waraft_entries, waraft_count, bookkeeping,
               value_bytes} =
                case ets_get_meta_full(ctx, idx, keydir, key, expiry_context) do
                  {:hit, value, expire_at_ms, lfu} ->
                    bookkeeping = hot_read_bookkeeping_add(bookkeeping, keydir, key, lfu)

                    {{:value, batch_get_result(value, expire_at_ms, result_mode)}, cold_entries,
                     cold_count, waraft_entries, waraft_count, bookkeeping,
                     batch_get_value_bytes(value)}

                  {:cold, file_id, offset, value_size, expire_at_ms}
                  when valid_cold_location(file_id, offset, value_size) ->
                    path = cold_file_path(ctx, idx, file_id)
                    entry = {ctx, idx, keydir, key, path, file_id, offset, value_size}

                    {{:cold, cold_count, expire_at_ms}, [entry | cold_entries], cold_count + 1,
                     waraft_entries, waraft_count, bookkeeping, value_size}

                  {:cold, file_id, offset, value_size, expire_at_ms}
                  when valid_waraft_segment_location(file_id, offset, value_size) ->
                    entry = {ctx, idx, keydir, key, file_id, offset, value_size}

                    {{:waraft, waraft_count, expire_at_ms}, cold_entries, cold_count,
                     [entry | waraft_entries], waraft_count + 1, bookkeeping, value_size}

                  {:cold, _file_id, _offset, _value_size, _expire_at_ms} ->
                    result = batch_get_fallback_value(ctx, idx, key, result_mode)

                    {{:value, result}, cold_entries, cold_count, waraft_entries, waraft_count,
                     bookkeeping, batch_get_value_bytes(result)}

                  {:invalid, entry} ->
                    failure = ReadResult.failure({:invalid_keydir_entry, entry})

                    {{:value, failure}, cold_entries, cold_count, waraft_entries, waraft_count,
                     bookkeeping, 0}

                  :hlc_drift_exceeded ->
                    failure = ReadResult.failure(:hlc_drift_exceeded)

                    {{:value, failure}, cold_entries, cold_count, waraft_entries, waraft_count,
                     bookkeeping, 0}

                  :expired ->
                    record_keyspace_miss(ctx, key)

                    {{:value, nil}, cold_entries, cold_count, waraft_entries, waraft_count,
                     bookkeeping, 0}

                  :miss ->
                    record_keyspace_miss(ctx, key)

                    {{:value, nil}, cold_entries, cold_count, waraft_entries, waraft_count,
                     bookkeeping, 0}

                  :no_table ->
                    result = batch_get_fallback_value(ctx, idx, key, result_mode)

                    {{:value, result}, cold_entries, cold_count, waraft_entries, waraft_count,
                     bookkeeping, batch_get_value_bytes(result)}
                end

              next_response_bytes = response_bytes + value_bytes

              if batch_get_byte_limit_exceeded?(byte_limit, next_response_bytes) do
                {:halt, {:byte_limit, bookkeeping}}
              else
                {:cont,
                 {[result | results], cold_entries, cold_count, waraft_entries, waraft_count,
                  bookkeeping, next_response_bytes}}
              end
            end
          )

        case plan do
          {:byte_limit, bookkeeping} ->
            hot_read_bookkeeping_finish(ctx, bookkeeping)
            {:error, :response_byte_limit}

          {results, cold_entries, _cold_count, waraft_entries, _waraft_count, bookkeeping,
           _response_bytes} ->
            hot_read_bookkeeping_finish(ctx, bookkeeping)

            cold_values =
              cold_entries
              |> Enum.reverse()
              |> read_cold_batch_async(expiry_context, result_mode)
              |> List.to_tuple()

            waraft_values =
              waraft_entries
              |> Enum.reverse()
              |> read_waraft_segment_batch_materialized(expiry_context, result_mode)
              |> List.to_tuple()

            values =
              results
              |> Enum.reverse()
              |> Enum.map(fn
                {:value, value} ->
                  value

                {:cold, index, expire_at_ms} ->
                  batch_get_result(elem(cold_values, index), expire_at_ms, result_mode)

                {:waraft, index, expire_at_ms} ->
                  batch_get_result(elem(waraft_values, index), expire_at_ms, result_mode)
              end)

            {:ok, values}
        end
      end

      defp batch_get_fallback_value(ctx, idx, key),
        do: batch_get_fallback_value(ctx, idx, key, :values)

      defp batch_get_fallback_value(ctx, idx, key, result_mode) do
        request = if result_mode == :entries, do: {:get_meta, key}, else: {:get, key}

        result =
          case safe_read_call(ctx, idx, request) do
            {:ok, value} -> value
            :unavailable -> ReadResult.failure(:shard_unavailable)
          end

        case result do
          {:error, {:storage_read_failed, _reason}} -> :ok
          nil -> record_keyspace_miss(ctx, key)
          _value -> Stats.record_cold_read(ctx, key)
        end

        result
      end

      defp batch_get_value_bytes(value) when is_binary(value), do: byte_size(value)

      defp batch_get_value_bytes({value, expire_at_ms})
           when is_binary(value) and is_integer(expire_at_ms),
           do: byte_size(value)

      defp batch_get_value_bytes(nil), do: 0
      defp batch_get_value_bytes({:error, {:storage_read_failed, _reason}}), do: 0

      defp batch_get_result(value, _expire_at_ms, :values), do: value

      defp batch_get_result({:batch_entry, value, expire_at_ms}, _planned_expire_at_ms, :entries)
           when is_binary(value) and is_integer(expire_at_ms),
           do: {value, expire_at_ms}

      defp batch_get_result(value, expire_at_ms, :entries)
           when is_binary(value) and is_integer(expire_at_ms),
           do: {value, expire_at_ms}

      defp batch_get_result(value, _expire_at_ms, :entries), do: value

      defp batch_get_byte_limit_exceeded?(:unlimited, _response_bytes), do: false
      defp batch_get_byte_limit_exceeded?(limit, response_bytes), do: response_bytes > limit

      @doc false
      @spec batch_get_on_route_keys(FerricStore.Instance.t(), [{binary(), binary()}]) :: [
              binary() | nil
            ]
      def batch_get_on_route_keys(ctx, route_lookup_pairs) do
        expiry_context = ExpiryContext.capture()
        bookkeeping = hot_read_bookkeeping_start(ctx)

        {results, bookkeeping} =
          Enum.map_reduce(route_lookup_pairs, bookkeeping, fn {route_key, lookup_key},
                                                              bookkeeping ->
            idx = shard_for(ctx, route_key)
            keydir = resolve_keydir(ctx, idx)

            case ets_get_full(ctx, idx, keydir, lookup_key, expiry_context) do
              {:hit, value, lfu} ->
                bookkeeping = hot_read_bookkeeping_add(bookkeeping, keydir, lookup_key, lfu)
                {value, bookkeeping}

              {:cold, _file_id, _offset, _value_size} ->
                {routed_get_fallback(ctx, idx, lookup_key), bookkeeping}

              :hlc_drift_exceeded ->
                {ReadResult.failure(:hlc_drift_exceeded), bookkeeping}

              :expired ->
                record_keyspace_miss(ctx, lookup_key)
                {nil, bookkeeping}

              :miss ->
                record_keyspace_miss(ctx, lookup_key)
                {nil, bookkeeping}

              :no_table ->
                {routed_get_fallback(ctx, idx, lookup_key), bookkeeping}
            end
          end)

        hot_read_bookkeeping_finish(ctx, bookkeeping)
        results
      end

      defp routed_get_fallback(ctx, idx, key) do
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
              binary()
              | nil
              | {:file_ref, binary(), non_neg_integer(), non_neg_integer()}
              | ReadResult.failure()
            ]
      def batch_get_with_file_refs(ctx, keys, min_file_ref_size) do
        do_batch_get_with_file_refs(ctx, keys, min_file_ref_size, true)
      end

      defp do_batch_get_with_file_refs(ctx, keys, min_file_ref_size, validate_blob_ref?) do
        expiry_context = ExpiryContext.capture()

        do_batch_get_with_file_refs(
          ctx,
          keys,
          min_file_ref_size,
          validate_blob_ref?,
          expiry_context
        )
      end

      defp do_batch_get_with_file_refs(
             ctx,
             keys,
             min_file_ref_size,
             validate_blob_ref?,
             expiry_context
           ) do
        bookkeeping = hot_read_bookkeeping_start(ctx)

        {results, {cold_entries, _cold_count, waraft_entries, _waraft_count, bookkeeping}} =
          Enum.map_reduce(keys, {[], 0, [], 0, bookkeeping}, fn key,
                                                                {cold_entries, cold_count,
                                                                 waraft_entries, waraft_count,
                                                                 bookkeeping} ->
            idx = shard_for(ctx, key)
            keydir = resolve_keydir(ctx, idx)

            case ets_get_full(ctx, idx, keydir, key, expiry_context) do
              {:hit, value, lfu} ->
                bookkeeping = hot_read_bookkeeping_add(bookkeeping, keydir, key, lfu)

                {{:value, value},
                 {cold_entries, cold_count, waraft_entries, waraft_count, bookkeeping}}

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
                  bookkeeping,
                  expiry_context,
                  validate_blob_ref?
                )

              {:cold, file_id, offset, value_size}
              when valid_waraft_segment_location(file_id, offset, value_size) ->
                entry = {ctx, idx, keydir, key, file_id, offset, value_size}

                if validate_blob_ref? == :bounded and
                     not blob_ref_candidate?(ctx, value_size) do
                  {{:bounded_waraft, entry},
                   {cold_entries, cold_count, waraft_entries, waraft_count, bookkeeping}}
                else
                  {{:waraft, waraft_count},
                   {cold_entries, cold_count, [entry | waraft_entries], waraft_count + 1,
                    bookkeeping}}
                end

              {:cold, _file_id, _offset, _value_size} ->
                result = batch_get_fallback_value(ctx, idx, key)

                {{:value, result},
                 {cold_entries, cold_count, waraft_entries, waraft_count, bookkeeping}}

              {:invalid, entry} ->
                failure = ReadResult.failure({:invalid_keydir_entry, entry})

                {{:value, failure},
                 {cold_entries, cold_count, waraft_entries, waraft_count, bookkeeping}}

              :hlc_drift_exceeded ->
                failure = ReadResult.failure(:hlc_drift_exceeded)

                {{:value, failure},
                 {cold_entries, cold_count, waraft_entries, waraft_count, bookkeeping}}

              :expired ->
                record_keyspace_miss(ctx, key)

                {{:value, nil},
                 {cold_entries, cold_count, waraft_entries, waraft_count, bookkeeping}}

              :miss ->
                record_keyspace_miss(ctx, key)

                {{:value, nil},
                 {cold_entries, cold_count, waraft_entries, waraft_count, bookkeeping}}

              :no_table ->
                result = batch_get_fallback_value(ctx, idx, key)

                {{:value, result},
                 {cold_entries, cold_count, waraft_entries, waraft_count, bookkeeping}}
            end
          end)

        hot_read_bookkeeping_finish(ctx, bookkeeping)

        cold_values =
          cold_entries
          |> Enum.reverse()
          |> read_cold_batch_file_ref_async(
            expiry_context,
            min_file_ref_size,
            validate_blob_ref?
          )
          |> List.to_tuple()

        waraft_values =
          waraft_entries
          |> Enum.reverse()
          |> read_waraft_segment_batch_file_ref_or_materialized(
            min_file_ref_size,
            validate_blob_ref?,
            expiry_context
          )
          |> List.to_tuple()

        Enum.map(results, fn
          {:value, value} -> value
          {:file_ref, path, offset, size} -> {:file_ref, path, offset, size}
          {:bounded_cold, entry} -> {:bounded_cold, entry}
          {:bounded_waraft, entry} -> {:bounded_waraft, entry}
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
