defmodule Ferricstore.Store.Router.Part04 do
  @moduledoc false

  # Extracted from Router: retry_changed_cold_value .. sampled_read_bookkeeping_fast
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
      alias Ferricstore.Store.LFU
      alias Ferricstore.Store.ListOps
      alias Ferricstore.Store.ReadResult
      alias Ferricstore.Store.Router
      alias Ferricstore.Store.SlotMap
      alias Ferricstore.Store.TypeRegistry

      @doc false
      def __pread_file_range_for_test__(path, offset, count),
        do: pread_file_range(path, offset, count)

      defp retry_changed_cold_value(ctx, idx, keydir, key, original_location, now) do
        case retry_changed_cold_value_raw(
               ctx,
               idx,
               keydir,
               key,
               original_location,
               now,
               :miss
             ) do
          {:read_error, _reason} -> :miss
          result -> result
        end
      end

      defp retry_changed_cold_value_result(
             ctx,
             idx,
             keydir,
             key,
             original_location,
             now,
             exhausted_failure
           ) do
        case retry_changed_cold_value_raw(
               ctx,
               idx,
               keydir,
               key,
               original_location,
               now,
               exhausted_failure
             ) do
          {:read_error, reason} -> storage_read_failure(:cold_value_unavailable, reason)
          result -> result
        end
      end

      defp retry_changed_cold_value_raw(
             ctx,
             idx,
             keydir,
             key,
             original_location,
             now,
             exhausted_result
           ) do
        case retry_changed_cold_value_once(ctx, idx, keydir, key, original_location, now) do
          :unchanged_cold ->
            retry_after_unchanged_cold_location(
              fn ->
                retry_changed_cold_value_once(ctx, idx, keydir, key, original_location, now)
              end,
              cold_retry_metadata(ctx, idx, key, :value),
              exhausted_result
            )

          result ->
            result
        end
      end

      defp retry_changed_cold_value_once(ctx, idx, keydir, key, original_location, now) do
        case ets_get_full(ctx, idx, keydir, key, now) do
          {:hit, value, lfu} ->
            sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
            {:hot, value}

          {:cold, file_id, offset, value_size}
          when valid_cold_location(file_id, offset, value_size) and
                 {file_id, offset, value_size} != original_location ->
            path = cold_file_path(ctx, idx, file_id)

            case read_cold_materialized(ctx, idx, path, offset, key) do
              {:ok, value} when is_binary(value) -> {:cold, value, file_id, offset}
              read_error -> {:read_error, storage_read_reason(read_error)}
            end

          {:cold, file_id, offset, value_size}
          when valid_waraft_segment_location(file_id, offset, value_size) and
                 {file_id, offset, value_size} != original_location ->
            case read_waraft_segment_materialized(ctx, idx, file_id, key) do
              {:ok, value} when is_binary(value) -> {:cold, value, file_id, offset}
              read_error -> {:read_error, storage_read_reason(read_error)}
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

          _ ->
            :miss
        end
      end

      defp retry_changed_waraft_segment_value_result(
             ctx,
             idx,
             keydir,
             key,
             original_location,
             now
           ) do
        case retry_changed_cold_value(ctx, idx, keydir, key, original_location, now) do
          {:cold, value, retry_file_id, retry_offset} ->
            Stats.record_cold_read(ctx, key)
            warm_ets_after_cold_read(ctx, idx, keydir, key, value, retry_file_id, retry_offset)
            value

          {:hot, value} ->
            value

          :miss ->
            nil

          {:error, {:storage_read_failed, _reason}} = failure ->
            failure
        end
      end

      defp retry_changed_waraft_segment_value_read_result(
             ctx,
             idx,
             keydir,
             key,
             original_location,
             now,
             read_error
           ) do
        failure = storage_read_failure(:waraft_value_unavailable, read_error)

        case retry_changed_cold_value_result(
               ctx,
               idx,
               keydir,
               key,
               original_location,
               now,
               failure
             ) do
          {:cold, value, retry_file_id, retry_offset} ->
            Stats.record_cold_read(ctx, key)
            warm_ets_after_cold_read(ctx, idx, keydir, key, value, retry_file_id, retry_offset)
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

      defp retry_changed_cold_meta(ctx, idx, keydir, key, original_location, now) do
        case retry_changed_cold_meta_raw(
               ctx,
               idx,
               keydir,
               key,
               original_location,
               now,
               :miss
             ) do
          {:read_error, _reason} -> :miss
          result -> result
        end
      end

      defp retry_changed_cold_meta_result(
             ctx,
             idx,
             keydir,
             key,
             original_location,
             now,
             exhausted_failure
           ) do
        case retry_changed_cold_meta_raw(
               ctx,
               idx,
               keydir,
               key,
               original_location,
               now,
               exhausted_failure
             ) do
          {:read_error, reason} -> storage_read_failure(:cold_meta_unavailable, reason)
          result -> result
        end
      end

      defp retry_changed_cold_meta_raw(
             ctx,
             idx,
             keydir,
             key,
             original_location,
             now,
             exhausted_result
           ) do
        case retry_changed_cold_meta_once(ctx, idx, keydir, key, original_location, now) do
          :unchanged_cold ->
            retry_after_unchanged_cold_location(
              fn ->
                retry_changed_cold_meta_once(ctx, idx, keydir, key, original_location, now)
              end,
              cold_retry_metadata(ctx, idx, key, :meta),
              exhausted_result
            )

          result ->
            result
        end
      end

      defp retry_changed_cold_meta_once(ctx, idx, keydir, key, original_location, now) do
        case ets_get_meta_full(ctx, idx, keydir, key, now) do
          {:hit, value, expire_at_ms, lfu} ->
            sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
            {:hot, value, expire_at_ms}

          {:cold, file_id, offset, value_size, expire_at_ms}
          when valid_cold_location(file_id, offset, value_size) and
                 {file_id, offset, value_size} != original_location ->
            path = cold_file_path(ctx, idx, file_id)

            case read_cold_materialized(ctx, idx, path, offset, key) do
              {:ok, value} when is_binary(value) -> {:cold, value, expire_at_ms, file_id, offset}
              read_error -> {:read_error, storage_read_reason(read_error)}
            end

          {:cold, file_id, offset, value_size, expire_at_ms}
          when valid_waraft_segment_location(file_id, offset, value_size) and
                 {file_id, offset, value_size} != original_location ->
            case read_waraft_segment_materialized(ctx, idx, file_id, key) do
              {:ok, value} when is_binary(value) -> {:cold, value, expire_at_ms, file_id, offset}
              read_error -> {:read_error, storage_read_reason(read_error)}
            end

          {:cold, file_id, offset, value_size, _expire_at_ms}
          when valid_cold_location(file_id, offset, value_size) and
                 {file_id, offset, value_size} == original_location ->
            :unchanged_cold

          {:cold, file_id, offset, value_size, _expire_at_ms}
          when valid_waraft_segment_location(file_id, offset, value_size) and
                 {file_id, offset, value_size} == original_location ->
            :unchanged_cold

          {:invalid, entry} ->
            {:read_error, {:invalid_keydir_entry, entry}}

          :hlc_drift_exceeded ->
            ReadResult.failure(:hlc_drift_exceeded)

          :no_table ->
            {:read_error, :keydir_unavailable}

          _ ->
            :miss
        end
      end

      defp retry_changed_waraft_segment_meta_result(ctx, idx, keydir, key, original_location, now) do
        case retry_changed_cold_meta(ctx, idx, keydir, key, original_location, now) do
          {:cold, value, retry_expire_at_ms, retry_file_id, retry_offset} ->
            Stats.record_cold_read(ctx, key)
            warm_ets_after_cold_read(ctx, idx, keydir, key, value, retry_file_id, retry_offset)
            {value, retry_expire_at_ms}

          {:hot, value, retry_expire_at_ms} ->
            {value, retry_expire_at_ms}

          :miss ->
            nil

          {:error, {:storage_read_failed, _reason}} = failure ->
            failure
        end
      end

      defp retry_changed_waraft_segment_meta_read_result(
             ctx,
             idx,
             keydir,
             key,
             original_location,
             now,
             read_error
           ) do
        failure = storage_read_failure(:waraft_meta_unavailable, read_error)

        case retry_changed_cold_meta_result(
               ctx,
               idx,
               keydir,
               key,
               original_location,
               now,
               failure
             ) do
          {:cold, value, retry_expire_at_ms, retry_file_id, retry_offset} ->
            Stats.record_cold_read(ctx, key)
            warm_ets_after_cold_read(ctx, idx, keydir, key, value, retry_file_id, retry_offset)
            {value, retry_expire_at_ms}

          {:hot, value, retry_expire_at_ms} ->
            {value, retry_expire_at_ms}

          :miss ->
            record_keyspace_miss(ctx, key)
            nil

          {:error, {:storage_read_failed, _reason}} = failure ->
            failure
        end
      end

      defp retry_after_unchanged_cold_location(retry_fun, metadata)
           when is_function(retry_fun, 0) do
        retry_after_unchanged_cold_location(retry_fun, metadata, :miss)
      end

      defp retry_after_unchanged_cold_location(retry_fun, metadata, exhausted_result)
           when is_function(retry_fun, 0) do
        retry_after_unchanged_cold_location(
          retry_fun,
          metadata,
          exhausted_result,
          @cold_location_retry_attempts
        )
      end

      defp retry_after_unchanged_cold_location(
             _retry_fun,
             metadata,
             exhausted_result,
             0
           ) do
        emit_cold_retry_exhausted(metadata)
        exhausted_result
      end

      defp retry_after_unchanged_cold_location(
             retry_fun,
             metadata,
             exhausted_result,
             attempts_left
           ) do
        maybe_run_cold_location_miss_hook()
        Process.sleep(@cold_location_retry_sleep_ms)

        case retry_fun.() do
          :unchanged_cold ->
            retry_after_unchanged_cold_location(
              retry_fun,
              metadata,
              exhausted_result,
              attempts_left - 1
            )

          result ->
            result
        end
      end

      defp cold_retry_metadata(ctx, idx, key, operation) do
        %{
          instance: ctx.name,
          shard_index: idx,
          operation: operation,
          reason: :unchanged_cold_location,
          redis_key_hash: :erlang.phash2(key)
        }
      end

      defp emit_cold_retry_exhausted(nil), do: :ok

      defp emit_cold_retry_exhausted(metadata) do
        :telemetry.execute(
          [:ferricstore, :store, :cold_read_retry_exhausted],
          %{count: 1, attempts: @cold_location_retry_attempts},
          metadata
        )
      end

      defp maybe_run_cold_location_miss_hook do
        case Process.get(:ferricstore_router_cold_location_miss_hook) do
          fun when is_function(fun, 0) -> fun.()
          _ -> :ok
        end
      end

      @doc """
      Returns `{value, expire_at_ms}` for a live key, or `nil` if the key does
      not exist or is expired.

      Hot path: reads directly from ETS for cached keys. Each read is recorded
      as hot or cold in `Ferricstore.Stats`.
      """
      @spec get_meta(FerricStore.Instance.t(), binary()) ::
              {binary(), non_neg_integer()} | nil | Ferricstore.Store.ReadResult.failure()
      def get_meta(ctx, key) do
        idx = shard_for(ctx, key)
        keydir = resolve_keydir(ctx, idx)
        expiry_context = ExpiryContext.capture()

        case ets_get_meta_full(ctx, idx, keydir, key, expiry_context) do
          {:hit, value, expire_at_ms, lfu} ->
            sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
            {value, expire_at_ms}

          {:cold, file_id, offset, value_size, expire_at_ms}
          when valid_cold_location(file_id, offset, value_size) ->
            # Cold key — read value from disk directly, return with expire_at_ms.
            path = cold_file_path(ctx, idx, file_id)

            case read_cold_materialized(ctx, idx, path, offset, key) do
              {:ok, value} when is_binary(value) ->
                Stats.record_cold_read(ctx, key)
                warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
                {value, expire_at_ms}

              read_error ->
                failure = storage_read_failure(:cold_meta_unavailable, read_error)

                case retry_changed_cold_meta_result(
                       ctx,
                       idx,
                       keydir,
                       key,
                       {file_id, offset, value_size},
                       expiry_context,
                       failure
                     ) do
                  {:cold, value, retry_expire_at_ms, retry_file_id, retry_offset} ->
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

                    {value, retry_expire_at_ms}

                  {:hot, value, retry_expire_at_ms} ->
                    {value, retry_expire_at_ms}

                  :miss ->
                    record_keyspace_miss(ctx, key)
                    nil

                  {:error, {:storage_read_failed, _reason}} = failure ->
                    failure
                end
            end

          {:cold, file_id, offset, value_size, expire_at_ms}
          when valid_waraft_segment_location(file_id, offset, value_size) ->
            case read_waraft_segment_materialized(ctx, idx, file_id, key) do
              {:ok, value} when is_binary(value) ->
                Stats.record_cold_read(ctx, key)
                warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
                {value, expire_at_ms}

              :not_found = read_error ->
                retry_changed_waraft_segment_meta_read_result(
                  ctx,
                  idx,
                  keydir,
                  key,
                  {file_id, offset, value_size},
                  expiry_context,
                  read_error
                )

              {:error, _reason} = read_error ->
                retry_changed_waraft_segment_meta_read_result(
                  ctx,
                  idx,
                  keydir,
                  key,
                  {file_id, offset, value_size},
                  expiry_context,
                  read_error
                )
            end

          {:cold, file_id, offset, value_size, _expire_at_ms} ->
            Ferricstore.Store.ReadResult.failure(
              {:invalid_cold_location, {file_id, offset, value_size}}
            )

          {:invalid, entry} ->
            Ferricstore.Store.ReadResult.failure({:invalid_keydir_entry, entry})

          :hlc_drift_exceeded ->
            Ferricstore.Store.ReadResult.failure(:hlc_drift_exceeded)

          :expired ->
            record_keyspace_miss(ctx, key)
            nil

          :miss ->
            record_keyspace_miss(ctx, key)
            nil

          :no_table ->
            case safe_read_call(ctx, idx, {:get_meta, key}) do
              {:ok, result} ->
                if result != nil do
                  Stats.record_cold_read(ctx, key)
                else
                  record_keyspace_miss(ctx, key)
                end

                result

              :unavailable ->
                Ferricstore.Store.ReadResult.failure(:keydir_unavailable)
            end
        end
      end

      @doc """
      Returns the expiry timestamp for a live plain key without reading its value.

      This is used by expiry-time commands so cold large values do not pay a
      Bitcask pread just to report TTL metadata.
      """
      @spec expire_at_ms(FerricStore.Instance.t(), binary()) ::
              non_neg_integer() | nil | ReadResult.failure()
      def expire_at_ms(ctx, key) do
        idx = shard_for(ctx, key)
        keydir = resolve_keydir(ctx, idx)
        expiry_context = ExpiryContext.capture()
        now = ExpiryContext.now_ms(expiry_context)

        try do
          case :ets.lookup(keydir, key) do
            [{^key, _value, exp, _lfu, _fid, _off, _vsize}]
            when is_integer(exp) and (exp == 0 or exp > now) ->
              exp

            [{^key, _value, exp, _lfu, _fid, _off, _vsize} = entry]
            when is_integer(exp) and exp > 0 and exp <= now ->
              case ExpiryContext.classify(expiry_context, exp) do
                {:unsafe, :hlc_drift_exceeded} ->
                  ReadResult.failure(:hlc_drift_exceeded)

                :expired ->
                  delete_observed_keydir_entry(ctx, idx, keydir, entry)
                  nil
              end

            [] ->
              nil

            [_malformed_live_entry] ->
              nil
          end
        rescue
          ArgumentError -> keydir_unavailable(ctx, idx, :expire_at_ms, nil)
        end
      end

      @doc """
      Returns the live plain key value size without reading a cold value.

      Hot entries use the in-memory value size; cold entries use the keydir
      `value_size` field populated by Bitcask append/recovery.
      """
      @spec value_size(FerricStore.Instance.t(), binary()) ::
              non_neg_integer() | nil | ReadResult.failure()
      def value_size(ctx, key) do
        idx = shard_for(ctx, key)
        keydir = resolve_keydir(ctx, idx)
        expiry_context = ExpiryContext.capture()
        now = ExpiryContext.now_ms(expiry_context)

        try do
          case :ets.lookup(keydir, key) do
            [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
              stored_value_size(value)

            [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
              cold_logical_value_size(ctx, idx, key, fid, off, vsize)

            [{^key, nil, 0, _lfu, fid, off, vsize}]
            when valid_waraft_segment_location(fid, off, vsize) ->
              waraft_logical_value_size(ctx, idx, key, fid, vsize)

            [{^key, nil, 0, _lfu, :pending, _off, vsize}]
            when valid_pending_value_size(vsize) ->
              vsize

            [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
              stored_value_size(value)

            [{^key, nil, exp, _lfu, fid, off, vsize}]
            when exp > now and valid_cold_location(fid, off, vsize) ->
              cold_logical_value_size(ctx, idx, key, fid, off, vsize)

            [{^key, nil, exp, _lfu, fid, off, vsize}]
            when exp > now and valid_waraft_segment_location(fid, off, vsize) ->
              waraft_logical_value_size(ctx, idx, key, fid, vsize)

            [{^key, nil, exp, _lfu, :pending, _off, vsize}]
            when exp > now and valid_pending_value_size(vsize) ->
              vsize

            [{^key, nil, exp, _lfu, _fid, :pending_offset, vsize}]
            when is_integer(exp) and (exp == 0 or exp > now) and is_integer(vsize) and
                   vsize >= 0 ->
              vsize

            [{^key, _value, exp, _lfu, _fid, _off, _vsize} = entry]
            when is_integer(exp) and exp > 0 and exp <= now ->
              case ExpiryContext.classify(expiry_context, exp) do
                {:unsafe, :hlc_drift_exceeded} ->
                  ReadResult.failure(:hlc_drift_exceeded)

                :expired ->
                  delete_observed_keydir_entry(ctx, idx, keydir, entry)
                  nil
              end

            [] ->
              nil

            [malformed_live_entry] ->
              ReadResult.failure({:invalid_keydir_entry, malformed_live_entry})
          end
        rescue
          ArgumentError -> keydir_unavailable(ctx, idx, :value_size, nil)
        end
      end

      @doc false
      @spec object_lfu(FerricStore.Instance.t(), binary()) ::
              non_neg_integer() | nil | ReadResult.failure()
      def object_lfu(ctx, key) do
        idx = shard_for(ctx, key)
        keydir = resolve_keydir(ctx, idx)
        expiry_context = ExpiryContext.capture()
        now = ExpiryContext.now_ms(expiry_context)

        try do
          case :ets.lookup(keydir, key) do
            [{^key, _value, exp, lfu, _fid, _off, _vsize}]
            when is_integer(exp) and (exp == 0 or exp > now) and is_integer(lfu) and lfu >= 0 ->
              lfu

            [{^key, _value, exp, _lfu, _fid, _off, _vsize} = entry]
            when is_integer(exp) and exp > 0 and exp <= now ->
              case ExpiryContext.classify(expiry_context, exp) do
                {:unsafe, :hlc_drift_exceeded} ->
                  ReadResult.failure(:hlc_drift_exceeded)

                :expired ->
                  delete_observed_keydir_entry(ctx, idx, keydir, entry)
                  nil
              end

            [] ->
              nil

            [_malformed_live_entry] ->
              nil
          end
        rescue
          ArgumentError -> keydir_unavailable(ctx, idx, :object_lfu, nil)
        end
      end

      @doc """
      Returns a byte range for a live plain key without reading the full cold value.

      Hot entries slice the in-memory value. Cold entries validate the Bitcask
      location once, then read only the requested value bytes from the data file.
      Missing or expired keys return `nil`, matching `get/2`.
      """
      @spec getrange(FerricStore.Instance.t(), binary(), integer(), integer()) ::
              binary() | nil | ReadResult.failure()
      def getrange(ctx, key, start_idx, end_idx) do
        idx = shard_for(ctx, key)
        keydir = resolve_keydir(ctx, idx)
        expiry_context = ExpiryContext.capture()

        case ets_get_full(ctx, idx, keydir, key, expiry_context) do
          {:hit, value, lfu} ->
            sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
            range_from_value(value, start_idx, end_idx)

          {:cold, file_id, offset, value_size}
          when valid_cold_location(file_id, offset, value_size) ->
            cold_range_from_location(
              ctx,
              idx,
              keydir,
              key,
              file_id,
              offset,
              value_size,
              start_idx,
              end_idx,
              expiry_context
            )

          {:cold, file_id, offset, value_size}
          when valid_waraft_segment_location(file_id, offset, value_size) ->
            case read_waraft_segment_materialized(ctx, idx, file_id, key) do
              {:ok, value} when is_binary(value) ->
                Stats.record_cold_read(ctx, key)
                warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
                range_from_value(value, start_idx, end_idx)

              :not_found ->
                retry_getrange_after_ref_miss(
                  ctx,
                  idx,
                  keydir,
                  key,
                  {file_id, offset, value_size},
                  start_idx,
                  end_idx,
                  expiry_context,
                  :not_found
                )

              {:error, reason} ->
                retry_getrange_after_ref_miss(
                  ctx,
                  idx,
                  keydir,
                  key,
                  {file_id, offset, value_size},
                  start_idx,
                  end_idx,
                  expiry_context,
                  reason
                )
            end

          {:cold, _file_id, _offset, _value_size} ->
            fallback_getrange(ctx, idx, key, start_idx, end_idx)

          {:invalid, entry} ->
            ReadResult.failure({:invalid_keydir_entry, entry})

          :hlc_drift_exceeded ->
            ReadResult.failure(:hlc_drift_exceeded)

          :expired ->
            record_keyspace_miss(ctx, key)
            nil

          :miss ->
            record_keyspace_miss(ctx, key)
            nil

          :no_table ->
            fallback_getrange(ctx, idx, key, start_idx, end_idx)
        end
      end

      defp cold_range_from_location(
             ctx,
             idx,
             keydir,
             key,
             file_id,
             offset,
             value_size,
             start_idx,
             end_idx,
             now
           ) do
        if blob_ref_candidate?(ctx, value_size) do
          path = cold_file_path(ctx, idx, file_id)

          case cold_blob_range_from_location(ctx, idx, path, offset, key, start_idx, end_idx) do
            {:ok, value} ->
              Stats.record_cold_read(ctx, key)
              value

            :not_blob ->
              cold_bitcask_range_from_location(
                ctx,
                idx,
                keydir,
                key,
                file_id,
                offset,
                value_size,
                start_idx,
                end_idx,
                now
              )

            {:error, _reason} ->
              retry_getrange_after_ref_miss(
                ctx,
                idx,
                keydir,
                key,
                {file_id, offset, value_size},
                start_idx,
                end_idx,
                now,
                :blob_range_read_failed
              )
          end
        else
          cold_bitcask_range_from_location(
            ctx,
            idx,
            keydir,
            key,
            file_id,
            offset,
            value_size,
            start_idx,
            end_idx,
            now
          )
        end
      end

      defp cold_bitcask_range_from_location(
             ctx,
             idx,
             keydir,
             key,
             file_id,
             offset,
             value_size,
             start_idx,
             end_idx,
             now
           ) do
        case normalize_byte_range(value_size, start_idx, end_idx) do
          :empty ->
            ""

          {relative_offset, count} ->
            path = cold_file_path(ctx, idx, file_id)

            case validated_file_ref(path, offset, key, value_size) do
              {read_path, value_offset, ^value_size} ->
                case read_validated_value_range(
                       ctx,
                       key,
                       read_path,
                       value_offset + relative_offset,
                       count
                     ) do
                  {:ok, value} ->
                    value

                  :error ->
                    retry_getrange_after_ref_miss(
                      ctx,
                      idx,
                      keydir,
                      key,
                      {file_id, offset, value_size},
                      start_idx,
                      end_idx,
                      now,
                      :range_pread_failed
                    )
                end

              nil ->
                retry_getrange_after_ref_miss(
                  ctx,
                  idx,
                  keydir,
                  key,
                  {file_id, offset, value_size},
                  start_idx,
                  end_idx,
                  now,
                  :invalid_value_ref
                )
            end
        end
      end

      defp cold_blob_range_from_location(ctx, idx, path, offset, key, start_idx, end_idx) do
        with {:ok, encoded_ref} <- read_cold_async(path, offset, key),
             {:ok, %BlobRef{} = ref} <- BlobRef.decode(encoded_ref) do
          case normalize_byte_range(ref.size, start_idx, end_idx) do
            :empty ->
              {:ok, ""}

            {relative_offset, count} ->
              BlobStore.get_range(ctx.data_dir, idx, ref, relative_offset, count)
          end
        else
          :error -> :not_blob
          {:error, reason} -> {:error, reason}
        end
      end

      defp retry_getrange_after_ref_miss(
             ctx,
             idx,
             keydir,
             key,
             original_location,
             start_idx,
             end_idx,
             now,
             read_error
           ) do
        exhausted_failure = storage_read_failure(:cold_range_unavailable, read_error)

        case retry_changed_file_ref_result(
               ctx,
               idx,
               keydir,
               key,
               original_location,
               now,
               exhausted_failure
             ) do
          {:cold_ref, path, value_offset, value_size} ->
            case normalize_byte_range(value_size, start_idx, end_idx) do
              :empty ->
                ""

              {relative_offset, count} ->
                case read_validated_value_range(
                       ctx,
                       key,
                       path,
                       value_offset + relative_offset,
                       count
                     ) do
                  {:ok, value} ->
                    value

                  :error ->
                    storage_read_failure(:cold_range_unavailable, :range_pread_failed)
                end
            end

          {:hot, value} ->
            range_from_value(value, start_idx, end_idx)

          :miss ->
            record_keyspace_miss(ctx, key)
            nil

          {:error, {:storage_read_failed, _reason}} = failure ->
            failure
        end
      end

      defp read_validated_value_range(ctx, key, path, offset, count) do
        maybe_run_cold_range_pread_miss_hook()

        case pread_file_range(path, offset, count) do
          {:ok, value} ->
            Stats.record_cold_read(ctx, key)
            {:ok, value}

          :error ->
            :error
        end
      end

      defp maybe_run_cold_range_pread_miss_hook do
        case Process.get(:ferricstore_router_cold_range_pread_miss_hook) do
          fun when is_function(fun, 0) -> fun.()
          _ -> :ok
        end
      end

      defp fallback_getrange(ctx, idx, key, start_idx, end_idx) do
        case safe_read_call(ctx, idx, {:get, key}) do
          {:ok, {:error, {:storage_read_failed, _reason}} = failure} ->
            failure

          {:ok, nil} ->
            record_keyspace_miss(ctx, key)
            nil

          {:ok, value} ->
            Stats.record_cold_read(ctx, key)
            range_from_value(value, start_idx, end_idx)

          :unavailable ->
            ReadResult.failure(:shard_unavailable)
        end
      end

      defp pread_file_range(_path, _offset, 0), do: {:ok, ""}

      defp pread_file_range(path, offset, count) do
        with {:ok, %File.Stat{type: :regular} = expected_stat} <- File.lstat(path),
             :ok <- maybe_run_cold_range_open_hook(),
             {:ok, fd} <- :file.open(path, [:read, :raw, :binary]) do
          try do
            with :ok <- verify_cold_range_file_identity(fd, expected_stat),
                 {:ok, value} when is_binary(value) and byte_size(value) == count <-
                   :file.pread(fd, offset, count) do
              {:ok, value}
            else
              _ -> :error
            end
          after
            :file.close(fd)
          end
        else
          _ -> :error
        end
      end

      defp maybe_run_cold_range_open_hook do
        case Process.get(:ferricstore_router_cold_range_open_hook) do
          fun when is_function(fun, 0) -> fun.()
          _ -> :ok
        end
      end

      defp verify_cold_range_file_identity(
             fd,
             %File.Stat{
               major_device: major_device,
               minor_device: minor_device,
               inode: inode
             }
           ) do
        case :file.read_file_info(fd) do
          {:ok, info}
          when elem(info, 2) == :regular and elem(info, 9) == major_device and
                 elem(info, 10) == minor_device and elem(info, 11) == inode ->
            :ok

          _ ->
            :error
        end
      end

      defp range_from_value(value, start_idx, end_idx) when is_binary(value),
        do: slice_binary_range(value, start_idx, end_idx)

      defp range_from_value(value, start_idx, end_idx) when is_integer(value),
        do: value |> Integer.to_string() |> slice_binary_range(start_idx, end_idx)

      defp range_from_value(value, start_idx, end_idx) when is_float(value),
        do: value |> Float.to_string() |> slice_binary_range(start_idx, end_idx)

      defp range_from_value(value, start_idx, end_idx),
        do: value |> to_string() |> slice_binary_range(start_idx, end_idx)

      defp slice_binary_range(value, start_idx, end_idx) do
        case normalize_byte_range(byte_size(value), start_idx, end_idx) do
          :empty -> ""
          {offset, count} -> binary_part(value, offset, count)
        end
      end

      defp normalize_byte_range(0, _start_idx, _end_idx), do: :empty

      defp normalize_byte_range(size, start_idx, end_idx) when size > 0 do
        start_norm = if start_idx < 0, do: max(size + start_idx, 0), else: start_idx
        end_norm = if end_idx < 0, do: size + end_idx, else: end_idx

        start_clamped = min(start_norm, size)
        end_clamped = min(end_norm, size - 1)

        if start_clamped > end_clamped do
          :empty
        else
          {start_clamped, end_clamped - start_clamped + 1}
        end
      end

      # Sampling rate for read-side bookkeeping (LFU touch + hot/cold stats).
      # 1 in N reads performs the ETS writes. Reduces write contention at high
      # concurrency with negligible impact on LFU accuracy (logarithmic counter)
      # and stats precision (ratio stays the same).
      # Default 100 = sample 1 in 100 reads. Set to 1 to disable sampling.

      # LFU counter already available from the initial ets_get_full lookup.
      # Eliminates the second ETS lookup that sampled_read_bookkeeping does.
      defp sampled_read_bookkeeping_fast(ctx, keydir, key, lfu) do
        sampled = Stats.sample_keyspace_hits_for_key(ctx, key)

        if sampled > 0 do
          LFU.touch(ctx, keydir, key, lfu)
          Stats.record_hot_read(ctx, key)
        end
      end
    end
  end
end
