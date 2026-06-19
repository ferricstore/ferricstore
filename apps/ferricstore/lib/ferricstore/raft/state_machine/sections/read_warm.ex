defmodule Ferricstore.Raft.StateMachine.Sections.ReadWarm do
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

      defp ets_lookup_committed(state, key) do
        now = apply_now_ms()

        case committed_keydir_lookup(state, key) do
          [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
            {:hit, value, 0}

          [{^key, nil, 0, _lfu, _fid, _off, _vsize}] ->
            # Cold key -- try Bitcask
            warm_from_bitcask(state, key)

          [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
            {:hit, value, exp}

          [{^key, nil, exp, _lfu, _fid, _off, _vsize}] when exp > now ->
            # Cold key with valid TTL -- try Bitcask
            warm_from_bitcask_with_exp(state, key, exp)

          [{^key, value, _exp, _lfu, _fid, _off, _vsize}] ->
            track_keydir_binary_remove_known(state, key, value)
            safe_ets_delete(state.ets, key)
            :expired

          [] ->
            # ETS miss -- try Bitcask for keys not yet in keydir
            warm_from_bitcask(state, key)
        end
      end

      # v2: warms a cold key from disk using the location stored in the ETS
      # 7-tuple. If the key has a cold entry (value=nil, fid/off known), reads
      # the value via pread_at and updates ETS. For truly missing keys (not in
      # ETS at all after recover_keydir), returns :miss.
      defp warm_from_bitcask(state, key) do
        case committed_keydir_lookup(state, key) do
          [{^key, nil, _exp, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
            warm_from_disk(state, key, 0, fid, off, vsize)

          [{^key, nil, _exp, _lfu, fid, off, vsize}]
          when valid_waraft_segment_location(fid, off, vsize) ->
            warm_from_waraft_segment(state, key, 0, fid, off, vsize)

          [{^key, nil, _exp, _lfu, _fid, _off, _vsize}] ->
            track_keydir_binary_remove_known(state, key, nil)
            safe_ets_delete(state.ets, key)
            :miss

          _ ->
            # :pending fid or truly missing -- cannot warm from disk.
            :miss
        end
      end

      defp committed_keydir_lookup(state, key) do
        :ets.lookup(state.ets, key)
      rescue
        ArgumentError -> []
      end

      defp warm_from_bitcask_with_exp(state, key, exp) do
        case committed_keydir_lookup(state, key) do
          [{^key, nil, _exp, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
            warm_from_disk(state, key, exp, fid, off, vsize)

          [{^key, nil, _exp, _lfu, fid, off, vsize}]
          when valid_waraft_segment_location(fid, off, vsize) ->
            warm_from_waraft_segment(state, key, exp, fid, off, vsize)

          [{^key, nil, _exp, _lfu, _fid, _off, _vsize}] ->
            track_keydir_binary_remove_known(state, key, nil)
            safe_ets_delete(state.ets, key)
            :miss

          _ ->
            # :pending fid or truly missing -- cannot warm from disk.
            :miss
        end
      end

      # Reads a value from disk at the given file_id + offset, warms ETS, and
      # returns {:hit, value, expire_at_ms}.
      # Applies the hot_cache_max_value_size threshold when re-warming ETS.
      defp warm_from_disk(state, key, expire_at_ms, fid, off, vsize) do
        path = sm_file_path(state, fid)
        original_location = {fid, off, vsize}

        case read_cold_async(path, off, key) do
          {:ok, value} when is_binary(value) ->
            case materialize_cold_blob_value(state, value) do
              {:ok, ^value} ->
                v = value_for_ets(value, hot_cache_threshold(state))
                # Cold -> warm: previous ETS value was nil, only new value bytes matter.
                track_keydir_binary_warm(state, v)

                safe_ets_insert(
                  state.ets,
                  {key, v, expire_at_ms, LFU.initial(), fid, off, byte_size(value)}
                )

                {:hit, value, expire_at_ms}

              {:ok, materialized} ->
                {:hit, materialized, expire_at_ms}

              {:error, _reason} ->
                retry_warm_from_changed_cold_location(
                  state,
                  key,
                  original_location,
                  @cold_location_retry_attempts
                )
            end

          _ ->
            retry_warm_from_changed_cold_location(
              state,
              key,
              original_location,
              @cold_location_retry_attempts
            )
        end
      end

      defp retry_warm_from_changed_cold_location(_state, _key, _original_location, 0), do: :miss

      defp retry_warm_from_changed_cold_location(state, key, original_location, attempts_left) do
        maybe_run_cold_location_miss_hook()
        Process.sleep(@cold_location_retry_sleep_ms)
        now = apply_now_ms()

        case committed_keydir_lookup(state, key) do
          [{^key, value, exp, _lfu, _fid, _off, _vsize}]
          when value != nil and (exp == 0 or exp > now) ->
            {:hit, value, exp}

          [{^key, nil, exp, _lfu, fid, off, vsize}]
          when (exp == 0 or exp > now) and valid_cold_location(fid, off, vsize) ->
            if {fid, off, vsize} == original_location do
              retry_warm_from_changed_cold_location(
                state,
                key,
                original_location,
                attempts_left - 1
              )
            else
              warm_from_disk(state, key, exp, fid, off, vsize)
            end

          [{^key, nil, exp, _lfu, fid, off, vsize}]
          when (exp == 0 or exp > now) and valid_waraft_segment_location(fid, off, vsize) ->
            if {fid, off, vsize} == original_location do
              retry_warm_from_changed_cold_location(
                state,
                key,
                original_location,
                attempts_left - 1
              )
            else
              warm_from_waraft_segment(state, key, exp, fid, off, vsize)
            end

          _ ->
            :miss
        end
      end

      defp warm_from_waraft_segment(state, key, expire_at_ms, fid, off, vsize) do
        original_location = {fid, off, vsize}

        case Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
               instance_ctx_for_state(state),
               state.shard_index,
               fid,
               key
             ) do
          {:ok, value} when is_binary(value) ->
            case materialize_cold_blob_value(state, value) do
              {:ok, ^value} ->
                v = value_for_ets(value, hot_cache_threshold(state))
                track_keydir_binary_warm(state, v)

                safe_ets_insert(
                  state.ets,
                  {key, v, expire_at_ms, LFU.initial(), fid, off, byte_size(value)}
                )

                {:hit, value, expire_at_ms}

              {:ok, materialized} ->
                {:hit, materialized, expire_at_ms}

              {:error, _reason} ->
                retry_warm_from_changed_cold_location(
                  state,
                  key,
                  original_location,
                  @cold_location_retry_attempts
                )
            end

          _ ->
            retry_warm_from_changed_cold_location(
              state,
              key,
              original_location,
              @cold_location_retry_attempts
            )
        end
      end

      defp maybe_run_cold_location_miss_hook do
        case Process.get(:ferricstore_state_machine_cold_location_miss_hook) do
          fun when is_function(fun, 0) -> fun.()
          _ -> :ok
        end
      end

      defp read_cold_async(path, offset, key) do
        Ferricstore.Store.ColdRead.pread_keyed(path, offset, key, @cold_read_timeout_ms)
      end

      defp materialize_cold_blob_value(state, value) do
        ctx = blob_apply_ctx(state)

        BlobValue.maybe_materialize(
          Map.get(ctx, :data_dir),
          state.shard_index,
          BlobValue.threshold(ctx),
          value
        )
      end

      # Returns the full file path for a log file within this shard's data dir.
      defp sm_file_path(state, file_id) do
        Path.join(
          state.shard_data_path,
          "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log"
        )
      end

      # ---------------------------------------------------------------------------
      # Private: list operations (read-modify-write via ListOps)
      # ---------------------------------------------------------------------------

      # Performs a complete read-modify-write for a list operation within a single
      # Raft apply. The get/put/delete closures operate directly on ETS and Bitcask
      # (the same stores available to the state machine) so the entire operation is
      # atomic from the Raft log's perspective.
      defp do_checked_list_op(state, key, operation) do
        store = build_compound_store(state)

        type_store =
          Map.put(store, :exists?, fn k ->
            live_key?(state, k)
          end)

        case ensure_list_type_for_operation(key, operation, type_store) do
          :ok -> ListOps.execute(key, store, operation)
          {:error, _} = err -> err
        end
      end

      defp do_checked_lmove(state, source, destination, from_dir, to_dir) do
        store = build_compound_store(state)

        with :ok <- Ferricstore.Store.TypeRegistry.check_type(source, :list, store) do
          case ListOps.read_meta(source, store) do
            nil ->
              nil

            {0, _, _} ->
              nil

            _meta ->
              with :ok <- Ferricstore.Store.TypeRegistry.check_or_set(destination, :list, store) do
                ListOps.execute_lmove(source, destination, store, from_dir, to_dir)
              end
          end
        end
      end

      defp ensure_list_type_for_operation(key, operation, store)

      defp ensure_list_type_for_operation(key, {:lpush, _elements}, store),
        do: Ferricstore.Store.TypeRegistry.check_or_set(key, :list, store)

      defp ensure_list_type_for_operation(key, {:rpush, _elements}, store),
        do: Ferricstore.Store.TypeRegistry.check_or_set(key, :list, store)

      defp ensure_list_type_for_operation(key, _operation, store),
        do: Ferricstore.Store.TypeRegistry.check_type(key, :list, store)

      defp build_string_value_store(state) do
        %{
          get: fn key -> do_get(state, key) end,
          get_meta: fn key -> do_get_meta(state, key) end,
          batch_get: fn keys -> Enum.map(keys, &do_get(state, &1)) end,
          put: fn key, value, expire_at_ms -> do_put(state, key, value, expire_at_ms) end,
          delete: fn key -> do_delete(state, key) end,
          exists?: fn key -> live_key?(state, key) end,
          compound_get: fn _redis_key, compound_key -> do_get(state, compound_key) end
        }
      end

      defp do_pfmerge(state, dest_key, source_sketches) when is_list(source_sketches) do
        source_keys =
          source_sketches
          |> Enum.with_index()
          |> Enum.map(fn {_sketch, idx} -> "\0__pfmerge_source__:" <> Integer.to_string(idx) end)

        source_values = Map.new(Enum.zip(source_keys, source_sketches))
        base_store = build_string_value_store(state)

        store = %{
          base_store
          | batch_get: fn keys ->
              Enum.map(keys, fn key ->
                Map.get(source_values, key) || do_get(state, key)
              end)
            end,
            compound_get: fn redis_key, compound_key ->
              if Map.has_key?(source_values, redis_key) do
                nil
              else
                do_get(state, compound_key)
              end
            end
        }

        HyperLogLog.handle_ast({:pfmerge, [dest_key | source_keys]}, store)
      end

      # Builds a compound store for list/hash/set operations inside the state
      # machine. Uses do_put/do_delete/do_get directly (already inside apply context,
      # writes accumulate in pending_writes buffer for batch NIF flush).
      defp build_compound_store(state) do
        %{
          compound_get: fn _redis_key, compound_key ->
            do_get(state, compound_key)
          end,
          compound_put: fn _redis_key, compound_key, value, expire_at_ms ->
            do_put(state, compound_key, value, expire_at_ms)
          end,
          compound_batch_put: fn _redis_key, entries ->
            Enum.each(entries, fn {compound_key, value, expire_at_ms} ->
              :ok = do_put(state, compound_key, value, expire_at_ms)
            end)

            :ok
          end,
          compound_delete: fn _redis_key, compound_key ->
            do_delete(state, compound_key)
          end,
          compound_batch_delete: fn _redis_key, compound_keys ->
            Enum.reduce_while(compound_keys, :ok, fn compound_key, :ok ->
              case do_delete(state, compound_key) do
                :ok -> {:cont, :ok}
                {:error, _} = error -> {:halt, error}
              end
            end)
          end,
          compound_scan: fn redis_key, prefix ->
            data_path =
              Ferricstore.Store.Shard.Compound.Promoted.promoted_store(state, redis_key) ||
                state.shard_data_path

            Ferricstore.Store.Shard.ETS.prefix_scan_entries(
              shard_ets_state(state),
              prefix,
              data_path
            )
            |> Enum.sort_by(fn {field, _} -> field end)
          end,
          compound_count: fn _redis_key, prefix ->
            Ferricstore.Store.Shard.ETS.prefix_count_entries(shard_ets_state(state), prefix)
          end,
          exists?: fn key ->
            live_key?(state, key)
          end
        }
      end

      defp build_zset_compound_store(state) do
        %{
          build_compound_store(state)
          | compound_put: fn redis_key, compound_key, value, expire_at_ms ->
              case do_put(state, compound_key, value, expire_at_ms) do
                :ok ->
                  maybe_queue_zset_ready_empty_after_flush(state, redis_key, compound_key, value)
                  :ok

                other ->
                  other
              end
            end,
            compound_batch_put: fn redis_key, entries ->
              do_compound_batch_put(state, redis_key, entries)
            end,
            compound_delete: fn redis_key, compound_key ->
              do_compound_delete(state, redis_key, compound_key)
            end,
            compound_batch_delete: fn redis_key, compound_keys ->
              do_compound_batch_delete(state, redis_key, compound_keys)
            end
        }
      end

      defp shard_ets_state(state) do
        %{keydir: state.ets, index: state.shard_index, instance_ctx: state.instance_ctx}
      end

      defp live_key?(state, key) do
        # Type checks use this for plain-key existence. Raw ETS presence is
        # incorrect because expired keys can stay unswept until the next read.
        case ets_lookup(state, key) do
          {:hit, _value, _expire_at_ms} -> true
          _ -> false
        end
      end

      # ---------------------------------------------------------------------------
      # Private: compound delete prefix (scan + batch delete)
      # ---------------------------------------------------------------------------

      # Scans ETS for all keys matching the given prefix and deletes each from
      # both ETS and Bitcask. Used by DEL on hashes, sets, and sorted sets to
      # remove all compound fields belonging to a data structure.
      #
      # Uses :ets.select with a match spec for O(matching) prefix lookup instead
      # of :ets.foldl which would scan every key in the entire keydir.
      defp do_delete_prefix(state, prefix) do
        prefix_len = byte_size(prefix)

        match_spec = [
          {{:"$1", :_, :_, :_, :_, :_, :_},
           [
             {:andalso, {:is_binary, :"$1"},
              {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
               {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
           ], [:"$1"]}
        ]

        keys_to_delete = :ets.select(state.ets, match_spec)

        Enum.each(keys_to_delete, fn key ->
          do_delete(state, key)
        end)

        :ok
      end

      defp do_compound_put(state, redis_key, compound_key, value, expire_at_ms) do
        dedicated_path = promoted_compound_path(state, redis_key, compound_key)

        result =
          case dedicated_path do
            nil ->
              do_put(state, compound_key, value, expire_at_ms)

            path ->
              do_promoted_compound_put(state, redis_key, compound_key, value, expire_at_ms, path)
          end

        if result == :ok do
          if dedicated_path == nil do
            maybe_queue_compound_promotion_after_flush(state, redis_key, compound_key, 1)
          end

          zset_index_put(state, redis_key, compound_key, value)
        end

        result
      end

      defp do_compound_batch_put(_state, _redis_key, []), do: :ok

      defp do_compound_batch_put(state, redis_key, entries) do
        case compound_batch_put_target(state, redis_key, entries) do
          :shared ->
            do_shared_compound_batch_put_fast(state, redis_key, entries)

          {:promoted, dedicated_path} ->
            do_promoted_compound_batch_put(state, redis_key, entries, dedicated_path)

          :mixed ->
            do_compound_batch_put_generic(state, redis_key, entries)
        end
      end

      defp compound_batch_put_target(state, redis_key, [
             {compound_key, _value, _expire_at_ms} | rest
           ]) do
        first_path = promoted_compound_path(state, redis_key, compound_key)

        if Enum.all?(rest, fn {key, _value, _expire_at_ms} ->
             promoted_compound_path(state, redis_key, key) == first_path
           end) do
          case first_path do
            nil -> :shared
            dedicated_path -> {:promoted, dedicated_path}
          end
        else
          :mixed
        end
      end

      defp do_compound_batch_put_generic(state, redis_key, entries) do
        Enum.reduce_while(entries, :ok, fn {compound_key, value, expire_at_ms}, :ok ->
          case do_compound_put(state, redis_key, compound_key, value, expire_at_ms) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      # Shared compound batches use the same publish-after-append contract as
      # put_batch: do not install visible ETS rows until Bitcask returns ordered
      # locations for the whole batch. ZSET side indexes are queued and flushed
      # only after the append succeeds.
      defp do_shared_compound_batch_put_fast(state, redis_key, entries) do
        if compound_shared_fast_path?(state) do
          case List.last(entries) do
            {compound_key, _value, _expire_at_ms} ->
              maybe_queue_compound_promotion_after_flush(
                state,
                redis_key,
                compound_key,
                length(entries)
              )

            nil ->
              :ok
          end

          pending = Process.get(:sm_pending_writes, [])
          pending_values = Process.get(:sm_pending_values, %{})
          fast_publish? = fast_put_publish_possible?(pending, pending_values)

          pending =
            Enum.reduce(entries, pending, fn {compound_key, value, expire_at_ms}, acc ->
              disk_val = to_disk_binary(value)
              queue_zset_index_put_after_flush(state, redis_key, compound_key, disk_val)
              [{:put, compound_key, disk_val, expire_at_ms} | acc]
            end)

          Process.put(:sm_pending_writes, pending)
          Process.put(:sm_pending_fast_put_batch, fast_publish?)
          :ok
        else
          do_compound_batch_put_generic(state, redis_key, entries)
        end
      end

      defp compound_shared_fast_path?(_state) do
        not cross_shard_pending_active?() and not standalone_staged_apply?()
      end

      defp maybe_queue_compound_promotion_after_flush(state, redis_key, compound_key, write_count) do
        threshold = Promotion.threshold(state.instance_ctx)

        if threshold > 0 and
             compound_promotion_candidate?(state, redis_key, compound_key, write_count) do
          pending = Process.get(:sm_pending_compound_promotions, MapSet.new())

          Process.put(
            :sm_pending_compound_promotions,
            MapSet.put(pending, {redis_key, compound_key})
          )
        end

        :ok
      end

      defp compound_promotion_candidate?(state, redis_key, compound_key, write_count) do
        case compound_prefix_from_key(redis_key, compound_key) do
          nil ->
            false

          prefix ->
            threshold = Promotion.threshold(state.instance_ctx)

            Ferricstore.Store.Shard.ETS.prefix_count_entries(shard_ets_state(state), prefix) +
              write_count > threshold
        end
      end

      defp run_pending_compound_promotions(state) do
        promotions = Process.get(:sm_pending_compound_promotions, MapSet.new())

        Enum.reduce(promotions, state, fn {redis_key, compound_key}, acc ->
          maybe_promote_compound_collection(acc, redis_key, compound_key)
        end)
      end

      defp maybe_promote_compound_collection(state, redis_key, compound_key) do
        threshold = Promotion.threshold(state.instance_ctx)

        cond do
          threshold == 0 ->
            state

          promoted_compound_path(state, redis_key, compound_key) != nil ->
            state

          true ->
            case {compound_type_from_key(compound_key),
                  compound_prefix_from_key(redis_key, compound_key)} do
              {nil, _prefix} ->
                state

              {_type, nil} ->
                state

              {type, prefix} ->
                if Ferricstore.Store.Shard.ETS.prefix_count_entries(
                     shard_ets_state(state),
                     prefix
                   ) >
                     threshold do
                  promote_compound_collection!(state, redis_key, type)
                else
                  state
                end
            end
        end
      end

      defp promote_compound_collection!(state, redis_key, type) do
        {:ok, dedicated_store} =
          Promotion.promote_collection!(
            type,
            redis_key,
            state.shard_data_path,
            state.ets,
            promoted_data_dir(state),
            state.shard_index,
            state.instance_ctx
          )

        total_bytes = Ferricstore.Store.Shard.Compound.promoted_dir_size(dedicated_store)

        promoted_instances =
          Map.put(Map.get(state, :promoted_instances, %{}), redis_key, %{
            path: dedicated_store,
            writes: 0,
            total_bytes: total_bytes,
            dead_bytes: 0,
            last_compacted_at: nil
          })

        pending_state = apply_state_get(:pending_state, state)

        apply_state_put(
          :pending_state,
          Map.put(pending_state, :promoted_instances, promoted_instances)
        )

        Map.put(state, :promoted_instances, promoted_instances)
      end

      defp compound_prefix_from_key(redis_key, <<"H:", _rest::binary>>),
        do: CompoundKey.hash_prefix(redis_key)

      defp compound_prefix_from_key(redis_key, <<"S:", _rest::binary>>),
        do: CompoundKey.set_prefix(redis_key)

      defp compound_prefix_from_key(redis_key, <<"Z:", _rest::binary>>),
        do: CompoundKey.zset_prefix(redis_key)

      defp compound_prefix_from_key(_redis_key, _compound_key), do: nil

      defp do_promoted_compound_batch_put(state, redis_key, entries, dedicated_path) do
        Promotion.await_compaction_latch(state, redis_key)

        active = Promotion.find_active(dedicated_path)
        fid = parse_fid_from_path(active)

        disk_entries =
          Enum.map(entries, fn {compound_key, value, expire_at_ms} ->
            {compound_key, to_disk_binary(value), expire_at_ms}
          end)

        case NIF.v2_append_batch(active, disk_entries) do
          {:ok, locations} when length(locations) == length(entries) ->
            entries
            |> Enum.zip(disk_entries)
            |> Enum.zip(locations)
            |> Enum.each(fn {{{compound_key, value, expire_at_ms}, {_key, disk_val, _exp}},
                             {offset, value_size}} ->
              ets_val = value_for_ets(value, hot_cache_threshold(state))
              track_keydir_binary_delta(state, compound_key, ets_val, expire_at_ms)

              :ets.insert(
                state.ets,
                {compound_key, ets_val, expire_at_ms, LFU.initial(), fid, offset, value_size}
              )

              sm_tx_put_pending(compound_key, value, expire_at_ms)

              deleted = Process.get(:tx_deleted_keys, MapSet.new())

              if MapSet.member?(deleted, compound_key) do
                Process.put(:tx_deleted_keys, MapSet.delete(deleted, compound_key))
              end

              zset_index_put(state, redis_key, compound_key, disk_val)
            end)

            :ok

          {:ok, locations} ->
            {:error, {:batch_result_mismatch, length(entries), locations}}

          {:error, _reason} = error ->
            error
        end
      end

      defp do_compound_delete(state, redis_key, compound_key) do
        result =
          case promoted_compound_path(state, redis_key, compound_key) do
            nil ->
              do_delete(state, compound_key)

            dedicated_path ->
              do_promoted_compound_delete(state, redis_key, compound_key, dedicated_path)
          end

        if result == :ok do
          zset_index_delete(state, redis_key, compound_key)
        end

        result
      end

      defp do_compound_batch_delete(_state, _redis_key, []), do: :ok

      defp do_compound_batch_delete(state, redis_key, compound_keys) do
        compound_keys
        |> Enum.chunk_by(&promoted_compound_path(state, redis_key, &1))
        |> Enum.reduce_while(:ok, fn keys, :ok ->
          result =
            case promoted_compound_path(state, redis_key, hd(keys)) do
              nil ->
                do_shared_compound_batch_delete(state, redis_key, keys)

              dedicated_path ->
                do_promoted_compound_batch_delete(state, redis_key, keys, dedicated_path)
            end

          case result do
            :ok -> {:cont, :ok}
            {:error, _} = error -> {:halt, error}
          end
        end)
      end

      defp do_shared_compound_batch_delete(state, redis_key, compound_keys) do
        case do_shared_compound_batch_delete_fast(state, redis_key, compound_keys) do
          :fallback ->
            do_shared_compound_batch_delete_generic(state, redis_key, compound_keys)

          result ->
            result
        end
      end

      defp do_shared_compound_batch_delete_fast(state, redis_key, compound_keys) do
        with true <- compound_shared_fast_path?(state),
             {:ok, prepared} <- maybe_prepare_delete_batch_fast(state, compound_keys),
             true <- Process.get(:sm_pending_writes, []) == [],
             true <- Process.get(:sm_pending_values, %{}) == %{} do
          Enum.each(Enum.reverse(prepared), fn {compound_key, _prob_path} ->
            queue_zset_index_delete_after_flush(state, redis_key, compound_key)
            queue_pending_delete_fast(compound_key, nil)
          end)

          Process.put(:sm_pending_fast_delete_batch, true)
          :ok
        else
          _ -> :fallback
        end
      end

      defp do_shared_compound_batch_delete_generic(state, redis_key, compound_keys) do
        Enum.reduce_while(compound_keys, :ok, fn compound_key, :ok ->
          case do_delete(state, compound_key) do
            :ok ->
              zset_index_delete(state, redis_key, compound_key)
              {:cont, :ok}

            {:error, _} = error ->
              {:halt, error}
          end
        end)
      end

      defp do_promoted_compound_batch_delete(state, redis_key, compound_keys, dedicated_path) do
        Promotion.await_compaction_latch(state, redis_key)

        active = Promotion.find_active(dedicated_path)
        ops = Enum.map(compound_keys, &{:delete, &1})

        case NIF.v2_append_ops_batch_nosync(active, ops) do
          {:ok, locations} ->
            with :ok <- validate_promoted_tombstone_batch(locations, length(compound_keys)),
                 :ok <- NIF.v2_fsync(active) do
              deleted =
                Enum.reduce(compound_keys, Process.get(:tx_deleted_keys, MapSet.new()), fn
                  compound_key, acc ->
                    track_keydir_binary_remove(state, compound_key)
                    :ets.delete(state.ets, compound_key)
                    sm_tx_drop_pending(compound_key)
                    zset_index_delete(state, redis_key, compound_key)
                    MapSet.put(acc, compound_key)
                end)

              Process.put(:tx_deleted_keys, deleted)
              :ok
            end

          {:error, _reason} = err ->
            err
        end
      end

      defp validate_promoted_tombstone_batch(locations, expected_count)
           when length(locations) == expected_count do
        if Enum.all?(locations, &valid_promoted_tombstone_location?/1) do
          :ok
        else
          {:error, {:tombstone_batch_result_mismatch, expected_count, locations}}
        end
      end

      defp validate_promoted_tombstone_batch(locations, expected_count),
        do: {:error, {:tombstone_batch_result_mismatch, expected_count, locations}}

      defp valid_promoted_tombstone_location?({:delete, offset, record_size})
           when is_integer(offset) and offset >= 0 and is_integer(record_size) and
                  record_size >= 0,
           do: true

      defp valid_promoted_tombstone_location?(_location), do: false

      defp maybe_delete_empty_compound_type_key_after_pop(
             _state,
             _redis_key,
             _total_member_count,
             0
           ),
           do: :ok

      defp maybe_delete_empty_compound_type_key_after_pop(
             state,
             redis_key,
             total_member_count,
             selected_count
           ) do
        if selected_count >= total_member_count do
          do_compound_delete(state, redis_key, CompoundKey.type_key(redis_key))
        else
          :ok
        end
      end

      defp do_compound_delete_prefix(state, redis_key, prefix) do
        result =
          case promoted_compound_path(state, redis_key, prefix) do
            nil ->
              do_delete_prefix(state, prefix)

            _dedicated_path ->
              Promotion.await_compaction_latch(state, redis_key)
              delete_compound_prefix_from_ets(state, prefix)

              Promotion.cleanup_promoted!(
                redis_key,
                state.shard_data_path,
                state.ets,
                state.data_dir,
                state.shard_index,
                Map.get(state, :instance_ctx) || Map.get(state, :instance_name)
              )
          end

        if result == :ok do
          zset_index_clear(state, redis_key)
        end

        result
      end

      defp do_promoted_compound_put(
             state,
             redis_key,
             compound_key,
             value,
             expire_at_ms,
             dedicated_path
           ) do
        Promotion.await_compaction_latch(state, redis_key)

        value_for = value_for_ets(value, hot_cache_threshold(state))
        disk_val = to_disk_binary(value)
        active = Promotion.find_active(dedicated_path)
        fid = parse_fid_from_path(active)

        case NIF.v2_append_record(active, compound_key, disk_val, expire_at_ms) do
          {:ok, {offset, _record_size}} ->
            value_size = byte_size(disk_val)

            track_keydir_binary_delta(state, compound_key, value_for, expire_at_ms)

            :ets.insert(
              state.ets,
              {compound_key, value_for, expire_at_ms, LFU.initial(), fid, offset, value_size}
            )

            sm_tx_put_pending(compound_key, value, expire_at_ms)
            deleted = Process.get(:tx_deleted_keys, MapSet.new())

            if MapSet.member?(deleted, compound_key) do
              Process.put(:tx_deleted_keys, MapSet.delete(deleted, compound_key))
            end

            :ok

          {:error, _reason} = err ->
            err
        end
      end
    end
  end
end
