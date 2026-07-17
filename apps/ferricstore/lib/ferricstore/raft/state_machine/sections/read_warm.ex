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
      alias Ferricstore.ExpiryContext
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
        Keydir,
        LFU,
        ListOps,
        Promotion,
        Router,
        ValueCodec
      }

      alias Ferricstore.Store.Shard.{CompoundMemberIndex, ZSetIndex}
      alias Ferricstore.Store.Shard.Transaction, as: ShardTransaction
      alias Ferricstore.Store.Shard.Flush, as: ShardFlush
      alias Ferricstore.Transaction.Ast, as: TxAst

      defp ets_lookup_committed(state, key) do
        expiry_context = ExpiryContext.capture()

        case committed_keydir_lookup(state, key) do
          [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
            {:hit, value, 0}

          [{^key, nil, 0, _lfu, _fid, _off, _vsize}] ->
            # Cold key -- try Bitcask
            warm_from_bitcask(state, key, expiry_context)

          [{^key, value, exp, _lfu, _fid, _off, _vsize} = entry]
          when is_integer(exp) and exp > 0 ->
            case ExpiryContext.classify(expiry_context, exp) do
              :live when value != nil ->
                {:hit, value, exp}

              :live ->
                # Cold key with valid TTL -- try Bitcask
                warm_from_bitcask_with_exp(state, key, exp, expiry_context)

              :expired ->
                delete_expired_committed_entry(state, entry)
                :expired

              {:unsafe, reason} ->
                record_state_read_failure(reason)
            end

          [] ->
            # ETS miss -- try Bitcask for keys not yet in keydir
            warm_from_bitcask(state, key, expiry_context)

          [entry] ->
            record_state_read_failure({:invalid_keydir_entry, key, entry})
        end
      end

      defp delete_expired_committed_entry(state, entry) do
        if Keydir.delete_exact(state.ets, entry) do
          track_keydir_binary_remove_entry(state, entry)
          true
        else
          false
        end
      end

      @doc false
      def __delete_expired_committed_entry_for_test__(state, entry),
        do: delete_expired_committed_entry(state, entry)

      # v2: warms a cold key from disk using the location stored in the ETS
      # 7-tuple. If the key has a cold entry (value=nil, fid/off known), reads
      # the value via pread_at and updates ETS. For truly missing keys (not in
      # ETS at all after recover_keydir), returns :miss.
      defp warm_from_bitcask(state, key, expiry_context) do
        case committed_keydir_lookup(state, key) do
          [{^key, nil, exp, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
            warm_from_disk(
              state,
              key,
              exp,
              fid,
              off,
              vsize,
              expiry_context,
              @cold_location_retry_attempts
            )

          [{^key, nil, exp, _lfu, fid, off, vsize}]
          when valid_waraft_segment_location(fid, off, vsize) ->
            warm_from_waraft_segment(
              state,
              key,
              exp,
              fid,
              off,
              vsize,
              expiry_context,
              @cold_location_retry_attempts
            )

          [{^key, nil, _exp, _lfu, fid, off, vsize}] ->
            record_state_read_failure({:invalid_cold_location, {fid, off, vsize}})

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

      defp warm_from_bitcask_with_exp(state, key, exp, expiry_context) do
        case committed_keydir_lookup(state, key) do
          [{^key, nil, _exp, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
            warm_from_disk(
              state,
              key,
              exp,
              fid,
              off,
              vsize,
              expiry_context,
              @cold_location_retry_attempts
            )

          [{^key, nil, _exp, _lfu, fid, off, vsize}]
          when valid_waraft_segment_location(fid, off, vsize) ->
            warm_from_waraft_segment(
              state,
              key,
              exp,
              fid,
              off,
              vsize,
              expiry_context,
              @cold_location_retry_attempts
            )

          [{^key, nil, _exp, _lfu, fid, off, vsize}] ->
            record_state_read_failure({:invalid_cold_location, {fid, off, vsize}})

          _ ->
            # :pending fid or truly missing -- cannot warm from disk.
            :miss
        end
      end

      # Reads a value from disk at the given file_id + offset, warms ETS, and
      # returns {:hit, value, expire_at_ms}.
      # Applies the hot_cache_max_value_size threshold when re-warming ETS.
      defp warm_from_disk(
             state,
             key,
             expire_at_ms,
             fid,
             off,
             vsize,
             expiry_context,
             attempts_left
           ) do
        path = sm_file_path(state, fid)
        original_location = {fid, off, vsize}

        observed_entry =
          observed_cold_entry(state, key, expire_at_ms, fid, off, vsize)

        case read_cold_async(path, off, key) do
          {:ok, value} when is_binary(value) ->
            maybe_run_cold_read_success_hook(state, key)

            case materialize_cold_blob_value(state, value) do
              {:ok, materialized} ->
                accept_current_cold_read(
                  state,
                  key,
                  observed_entry,
                  value,
                  materialized,
                  original_location,
                  expiry_context,
                  attempts_left
                )

              {:error, reason} ->
                retry_warm_from_changed_cold_location(
                  state,
                  key,
                  original_location,
                  expiry_context,
                  attempts_left - 1,
                  {:blob_ref_unavailable, reason}
                )
            end

          {:error, reason} ->
            retry_warm_from_changed_cold_location(
              state,
              key,
              original_location,
              expiry_context,
              attempts_left - 1,
              reason
            )

          other ->
            retry_warm_from_changed_cold_location(
              state,
              key,
              original_location,
              expiry_context,
              attempts_left - 1,
              {:invalid_cold_read_result, other}
            )
        end
      end

      defp observed_cold_entry(state, key, expire_at_ms, fid, off, vsize) do
        case committed_keydir_lookup(state, key) do
          [
            {^key, nil, ^expire_at_ms, _lfu, ^fid, ^off, ^vsize} = entry
          ] ->
            entry

          _changed_or_missing ->
            nil
        end
      end

      defp accept_current_cold_read(
             state,
             key,
             {key, nil, expire_at_ms, _lfu, _fid, _off, _vsize} = observed_entry,
             value,
             value,
             original_location,
             expiry_context,
             attempts_left
           ) do
        ets_value = value_for_ets(value, hot_cache_threshold(state))
        replacement = put_elem(observed_entry, 1, ets_value)

        if Keydir.replace_exact(state.ets, observed_entry, replacement) do
          track_keydir_binary_warm(state, ets_value)
          {:hit, value, expire_at_ms}
        else
          retry_after_changed_successful_cold_read(
            state,
            key,
            observed_entry,
            original_location,
            expiry_context,
            attempts_left - 1
          )
        end
      end

      defp accept_current_cold_read(
             state,
             key,
             {key, nil, expire_at_ms, _lfu, _fid, _off, _vsize} = observed_entry,
             _encoded_value,
             materialized,
             original_location,
             expiry_context,
             attempts_left
           ) do
        if Keydir.replace_exact(state.ets, observed_entry, observed_entry) do
          {:hit, materialized, expire_at_ms}
        else
          retry_after_changed_successful_cold_read(
            state,
            key,
            observed_entry,
            original_location,
            expiry_context,
            attempts_left - 1
          )
        end
      end

      defp accept_current_cold_read(
             state,
             key,
             nil,
             _encoded_value,
             _materialized,
             original_location,
             expiry_context,
             attempts_left
           ) do
        retry_after_changed_successful_cold_read(
          state,
          key,
          nil,
          original_location,
          expiry_context,
          attempts_left - 1
        )
      end

      defp retry_after_changed_successful_cold_read(
             state,
             key,
             _observed_entry,
             original_location,
             expiry_context,
             attempts_left
           ) do
        retry_warm_from_changed_cold_location(
          state,
          key,
          original_location,
          expiry_context,
          attempts_left,
          :cold_location_changed_after_read
        )
      end

      defp retry_warm_from_changed_cold_location(
             _state,
             _key,
             original_location,
             _expiry_context,
             attempts_left,
             reason
           )
           when attempts_left <= 0 do
        record_state_read_failure({:cold_value_unavailable, original_location, reason})
      end

      defp retry_warm_from_changed_cold_location(
             state,
             key,
             original_location,
             expiry_context,
             attempts_left,
             reason
           ) do
        maybe_run_cold_location_miss_hook()
        Process.sleep(@cold_location_retry_sleep_ms)

        case committed_keydir_lookup(state, key) do
          [{^key, _value, exp, _lfu, _fid, _off, _vsize} = entry]
          when is_integer(exp) and exp >= 0 ->
            case ExpiryContext.classify(expiry_context, exp) do
              :live ->
                retry_warm_from_live_entry(
                  state,
                  key,
                  entry,
                  original_location,
                  expiry_context,
                  attempts_left,
                  reason
                )

              :expired ->
                :miss

              {:unsafe, unsafe_reason} ->
                record_state_read_failure(unsafe_reason)
            end

          _ ->
            :miss
        end
      end

      defp retry_warm_from_live_entry(
             _state,
             key,
             {key, value, exp, _lfu, _fid, _off, _vsize},
             _original_location,
             _expiry_context,
             _attempts_left,
             _reason
           )
           when value != nil do
        {:hit, value, exp}
      end

      defp retry_warm_from_live_entry(
             state,
             key,
             {key, nil, exp, _lfu, fid, off, vsize},
             _original_location,
             expiry_context,
             attempts_left,
             _reason
           )
           when valid_cold_location(fid, off, vsize) do
        warm_from_disk(
          state,
          key,
          exp,
          fid,
          off,
          vsize,
          expiry_context,
          attempts_left
        )
      end

      defp retry_warm_from_live_entry(
             state,
             key,
             {key, nil, exp, _lfu, fid, off, vsize},
             _original_location,
             expiry_context,
             attempts_left,
             _reason
           )
           when valid_waraft_segment_location(fid, off, vsize) do
        warm_from_waraft_segment(
          state,
          key,
          exp,
          fid,
          off,
          vsize,
          expiry_context,
          attempts_left
        )
      end

      defp retry_warm_from_live_entry(
             _state,
             _key,
             _entry,
             _original_location,
             _expiry_context,
             _attempts_left,
             _reason
           ),
           do: :miss

      defp warm_from_waraft_segment(
             state,
             key,
             expire_at_ms,
             fid,
             off,
             vsize,
             expiry_context,
             attempts_left
           ) do
        original_location = {fid, off, vsize}

        observed_entry =
          observed_cold_entry(state, key, expire_at_ms, fid, off, vsize)

        case Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
               instance_ctx_for_state(state),
               state.shard_index,
               fid,
               key
             ) do
          {:ok, value} when is_binary(value) ->
            maybe_run_cold_read_success_hook(state, key)

            case materialize_cold_blob_value(state, value) do
              {:ok, materialized} ->
                accept_current_cold_read(
                  state,
                  key,
                  observed_entry,
                  value,
                  materialized,
                  original_location,
                  expiry_context,
                  attempts_left
                )

              {:error, reason} ->
                retry_warm_from_changed_cold_location(
                  state,
                  key,
                  original_location,
                  expiry_context,
                  attempts_left - 1,
                  {:blob_ref_unavailable, reason}
                )
            end

          {:error, reason} ->
            retry_warm_from_changed_cold_location(
              state,
              key,
              original_location,
              expiry_context,
              attempts_left - 1,
              reason
            )

          other ->
            retry_warm_from_changed_cold_location(
              state,
              key,
              original_location,
              expiry_context,
              attempts_left - 1,
              {:invalid_waraft_read_result, other}
            )
        end
      end

      defp maybe_run_cold_location_miss_hook do
        case Process.get(:ferricstore_state_machine_cold_location_miss_hook) do
          fun when is_function(fun, 0) -> fun.()
          _ -> :ok
        end
      end

      defp maybe_run_cold_read_success_hook(state, key) do
        case Process.get(:ferricstore_state_machine_cold_read_success_hook) do
          fun when is_function(fun, 2) -> fun.(state, key)
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

            {:error, _reason} = error ->
              error

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

      # Shared collections retain the pending-write fast path; promoted
      # collections are resolved to their dedicated log before any access.
      defp build_compound_store(state) do
        %{
          compound_get: fn redis_key, compound_key ->
            sm_store_compound_get(state, redis_key, compound_key)
          end,
          compound_put: fn redis_key, compound_key, value, expire_at_ms ->
            do_compound_put(state, redis_key, compound_key, value, expire_at_ms)
          end,
          compound_batch_put: fn redis_key, entries ->
            do_compound_batch_put(state, redis_key, entries)
          end,
          compound_delete: fn redis_key, compound_key ->
            do_compound_delete(state, redis_key, compound_key)
          end,
          compound_batch_delete: fn redis_key, compound_keys ->
            do_compound_batch_delete(state, redis_key, compound_keys)
          end,
          compound_batch_mutate: fn redis_key, compound_keys, entries ->
            with :ok <- do_compound_batch_delete(state, redis_key, compound_keys),
                 :ok <- do_compound_batch_put(state, redis_key, entries) do
              :ok
            end
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
            |> Ferricstore.Store.ReadResult.map_success(
              &Enum.sort_by(&1, fn {field, _} -> field end)
            )
          end,
          compound_scan_slice: fn redis_key, prefix, start, count, total ->
            data_path =
              Ferricstore.Store.Shard.Compound.Promoted.promoted_store(state, redis_key) ||
                state.shard_data_path

            Ferricstore.Store.Shard.ETS.prefix_scan_entries_slice(
              shard_ets_state(state),
              prefix,
              data_path,
              start,
              count,
              total
            )
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
              case do_compound_put(state, redis_key, compound_key, value, expire_at_ms) do
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
        %{
          keydir: state.ets,
          index: state.shard_index,
          instance_ctx: state.instance_ctx,
          compound_member_index: Map.get(state, :compound_member_index_name)
        }
      end

      defp live_key?(state, key) do
        # Type checks use this for plain-key existence. Raw ETS presence is
        # incorrect because expired keys can stay unswept until the next read.
        case ets_lookup(state, key) do
          {:hit, _value, _expire_at_ms} -> true
          _ -> false
        end
      end

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

        state.ets
        |> :ets.select(match_spec)
        |> Enum.reduce_while(:ok, fn key, :ok ->
          case do_delete(state, key) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      # Collection deletion must use the exact ordered catalog. Falling back to
      # the keydir would turn one collection delete into a full-shard apply stall.
      defp do_compound_member_prefix_delete(state, redis_key, prefix) do
        case CompoundMemberIndex.keys_for_prefix(
               Map.get(state, :compound_member_index_name),
               prefix,
               state.apply_context.compound_delete_member_budget
             ) do
          {:ok, compound_keys} ->
            do_compound_batch_delete(state, redis_key, compound_keys)

          {:error, :limit_exceeded} ->
            {:error, :compound_delete_budget_exceeded}

          :unavailable ->
            {:error, :compound_member_index_unavailable}
        end
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
            queue_compound_promotion_after_flush(state, redis_key, compound_key)
          end

          zset_index_put(state, redis_key, compound_key, value)
        end

        result
      end

      defp sm_store_compound_get(state, redis_key, compound_key) do
        case sm_store_compound_get_meta(state, redis_key, compound_key) do
          {:error, _reason} = error -> error
          {value, _expire_at_ms} -> value
          nil -> nil
        end
      end

      defp do_compound_batch_put(_state, _redis_key, []), do: :ok

      defp do_compound_batch_put(state, redis_key, entries) do
        case compound_batch_put_target(state, redis_key, entries) do
          :shared ->
            do_shared_compound_batch_put_fast(state, redis_key, entries)

          {:promoted, dedicated_path} ->
            do_promoted_compound_batch_put(state, redis_key, entries, dedicated_path)

          :mixed ->
            {:error, :mixed_compound_batch_targets}
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
        pending = Process.get(:sm_pending_writes, [])
        pending_values = Process.get(:sm_pending_values, %{})

        if compound_shared_fast_path?(state) and
             fast_put_publish_possible?(pending, pending_values) do
          case List.last(entries) do
            {compound_key, _value, _expire_at_ms} ->
              queue_compound_promotion_after_flush(state, redis_key, compound_key)

            nil ->
              :ok
          end

          pending =
            Enum.reduce(entries, pending, fn {compound_key, value, expire_at_ms}, acc ->
              disk_val = to_disk_binary(value)
              queue_zset_index_put_after_flush(state, redis_key, compound_key, disk_val)
              [{:put, compound_key, disk_val, expire_at_ms} | acc]
            end)

          Process.put(:sm_pending_writes, pending)
          Process.put(:sm_pending_fast_put_batch, true)
          :ok
        else
          do_compound_batch_put_generic(state, redis_key, entries)
        end
      end

      defp compound_shared_fast_path?(_state) do
        not cross_shard_pending_active?() and not standalone_staged_apply?()
      end

      # Promotion is structural maintenance, not replicated state-machine work.
      # Keep only one lightweight hint per collection and dispatch it after the
      # durable write batch succeeds. Carry the exact replicated threshold that
      # admitted the write so the async worker cannot observe newer process state.
      defp queue_compound_promotion_after_flush(state, redis_key, compound_key) do
        threshold = Map.fetch!(state, :apply_context).promotion_threshold

        case Process.get(:sm_pending_compound_promotions) do
          %{} = pending ->
            Process.put(
              :sm_pending_compound_promotions,
              Map.put(pending, redis_key, {compound_key, threshold})
            )

          _not_in_apply ->
            :ok
        end

        :ok
      end

      defp promoted_put_maintenance(state, compound_key, disk_value)
           when is_binary(compound_key) and is_binary(disk_value) do
        new_record_size =
          @bitcask_record_header_size + byte_size(compound_key) + byte_size(disk_value)

        %{
          appended_bytes: new_record_size,
          reclaimable_bytes: promoted_existing_record_size(state, compound_key),
          writes: 1
        }
      end

      defp promoted_batch_put_maintenance(state, disk_entries) do
        {maintenance, _latest_sizes} =
          Enum.reduce(disk_entries, {empty_promoted_maintenance(), %{}}, fn
            {compound_key, disk_value, _expire_at_ms}, {maintenance, latest_sizes} ->
              old_record_size =
                Map.get_lazy(latest_sizes, compound_key, fn ->
                  promoted_existing_record_size(state, compound_key)
                end)

              new_record_size =
                @bitcask_record_header_size + byte_size(compound_key) + byte_size(disk_value)

              {
                add_promoted_maintenance(
                  maintenance,
                  new_record_size,
                  old_record_size,
                  1
                ),
                Map.put(latest_sizes, compound_key, new_record_size)
              }
          end)

        maintenance
      end

      defp promoted_delete_maintenance(state, compound_key) when is_binary(compound_key) do
        tombstone_size = @bitcask_record_header_size + byte_size(compound_key)

        %{
          appended_bytes: tombstone_size,
          reclaimable_bytes: promoted_existing_record_size(state, compound_key) + tombstone_size,
          writes: 1
        }
      end

      defp promoted_batch_delete_maintenance(state, compound_keys) do
        {maintenance, _deleted} =
          Enum.reduce(compound_keys, {empty_promoted_maintenance(), MapSet.new()}, fn
            compound_key, {maintenance, deleted} ->
              tombstone_size = @bitcask_record_header_size + byte_size(compound_key)

              old_record_size =
                if MapSet.member?(deleted, compound_key),
                  do: 0,
                  else: promoted_existing_record_size(state, compound_key)

              {
                add_promoted_maintenance(
                  maintenance,
                  tombstone_size,
                  old_record_size + tombstone_size,
                  1
                ),
                MapSet.put(deleted, compound_key)
              }
          end)

        maintenance
      end

      defp promoted_existing_record_size(state, compound_key) do
        case safe_ets_lookup(state.ets, compound_key) do
          [{^compound_key, _value, _expire_at_ms, _lfu, _file_id, _offset, value_size}]
          when is_integer(value_size) and value_size >= 0 ->
            @bitcask_record_header_size + byte_size(compound_key) + value_size

          _other ->
            0
        end
      end

      defp empty_promoted_maintenance do
        %{appended_bytes: 0, reclaimable_bytes: 0, writes: 0}
      end

      defp add_promoted_maintenance(
             maintenance,
             appended_bytes,
             reclaimable_bytes,
             writes
           ) do
        %{
          appended_bytes: maintenance.appended_bytes + appended_bytes,
          reclaimable_bytes: maintenance.reclaimable_bytes + reclaimable_bytes,
          writes: maintenance.writes + writes
        }
      end

      defp queue_promoted_maintenance_after_flush(redis_key, maintenance) do
        case Process.get(:sm_pending_promoted_maintenance) do
          pending when is_map(pending) ->
            merged =
              Map.update(pending, redis_key, maintenance, fn current ->
                add_promoted_maintenance(
                  current,
                  maintenance.appended_bytes,
                  maintenance.reclaimable_bytes,
                  maintenance.writes
                )
              end)

            Process.put(:sm_pending_promoted_maintenance, merged)

          _not_in_apply ->
            :ok
        end

        :ok
      end

      defp queue_promoted_revision_puts_after_flush(table, keys) when is_list(keys) do
        revision = promoted_logical_revision()
        ops = Process.get(:sm_pending_compound_revision_ops, [])

        Process.put(
          :sm_pending_compound_revision_ops,
          Enum.reduce(keys, ops, fn key, acc -> [{:put, table, key, revision} | acc] end)
        )

        :ok
      end

      defp queue_promoted_revision_put_after_flush(table, key),
        do: queue_promoted_revision_puts_after_flush(table, [key])

      defp queue_promoted_revision_deletes_after_flush(table, keys) when is_list(keys) do
        ops = Process.get(:sm_pending_compound_revision_ops, [])

        Process.put(
          :sm_pending_compound_revision_ops,
          Enum.reduce(keys, ops, fn key, acc -> [{:delete, table, key} | acc] end)
        )

        :ok
      end

      defp queue_promoted_revision_delete_after_flush(table, key),
        do: queue_promoted_revision_deletes_after_flush(table, [key])

      defp promoted_logical_revision do
        current_ra_index() ||
          Process.get(:sm_compound_revision_fallback) ||
          promoted_fallback_revision()
      end

      defp promoted_fallback_revision do
        revision = :erlang.unique_integer([:monotonic, :positive])
        Process.put(:sm_compound_revision_fallback, revision)
        revision
      end

      defp do_promoted_compound_batch_put(state, redis_key, entries, dedicated_path) do
        Promotion.await_compaction_latch(state, redis_key)

        active = Promotion.find_active(dedicated_path)
        fid = parse_fid_from_path(active)

        disk_entries =
          Enum.map(entries, fn {compound_key, value, expire_at_ms} ->
            {compound_key, to_disk_binary(value), expire_at_ms}
          end)

        maintenance = promoted_batch_put_maintenance(state, disk_entries)

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

              CompoundMemberIndex.put(Map.get(state, :compound_member_index_name), compound_key)
              sm_tx_put_pending(compound_key, value, expire_at_ms)

              deleted = Process.get(:tx_deleted_keys, MapSet.new())

              if MapSet.member?(deleted, compound_key) do
                Process.put(:tx_deleted_keys, MapSet.delete(deleted, compound_key))
              end

              zset_index_put(state, redis_key, compound_key, disk_val)
            end)

            queue_promoted_maintenance_after_flush(redis_key, maintenance)

            queue_promoted_revision_puts_after_flush(
              Map.get(state, :compound_revision_index_name),
              Enum.map(entries, fn {compound_key, _value, _expire_at_ms} -> compound_key end)
            )

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
        first_path = promoted_compound_path(state, redis_key, hd(compound_keys))

        if Enum.all?(compound_keys, fn compound_key ->
             promoted_compound_path(state, redis_key, compound_key) == first_path
           end) do
          case first_path do
            nil ->
              do_shared_compound_batch_delete(state, redis_key, compound_keys)

            dedicated_path ->
              do_promoted_compound_batch_delete(
                state,
                redis_key,
                compound_keys,
                dedicated_path
              )
          end
        else
          {:error, :mixed_compound_batch_targets}
        end
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
             {:ok, prepared} <- maybe_prepare_compound_delete_batch_fast(state, compound_keys),
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

      defp maybe_prepare_compound_delete_batch_fast(state, keys) do
        now_ms = apply_now_ms()

        Enum.reduce_while(keys, {:ok, []}, fn key, {:ok, acc} ->
          case safe_ets_lookup(state.ets, key) do
            [{^key, _value, _expire_at_ms, _lfu, :pending, _offset, _value_size}] ->
              {:halt, :fallback}

            [{^key, _value, expire_at_ms, _lfu, _file_id, _offset, _value_size}]
            when expire_at_ms != 0 and expire_at_ms <= now_ms ->
              {:halt, :fallback}

            [{^key, nil, _expire_at_ms, _lfu, file_id, offset, value_size}]
            when valid_cold_location(file_id, offset, value_size) or
                   valid_waraft_segment_location(file_id, offset, value_size) ->
              {:cont, {:ok, [{key, nil} | acc]}}

            [{^key, nil, _expire_at_ms, _lfu, file_id, offset, value_size}] ->
              record_state_read_failure({:invalid_cold_location, {file_id, offset, value_size}})
              {:halt, {:error, :invalid_cold_location}}

            [{^key, value, _expire_at_ms, _lfu, _file_id, _offset, _value_size}]
            when is_binary(value) ->
              {:cont, {:ok, [{key, nil} | acc]}}

            [] ->
              {:cont, {:ok, [{key, nil} | acc]}}

            invalid ->
              record_state_read_failure({:invalid_keydir_entry, key, invalid})
              {:halt, {:error, :invalid_keydir_entry}}
          end
        end)
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
        maintenance = promoted_batch_delete_maintenance(state, compound_keys)

        case NIF.v2_append_ops_batch(active, ops) do
          {:ok, locations} ->
            with :ok <- validate_promoted_tombstone_batch(locations, length(compound_keys)) do
              deleted =
                Enum.reduce(compound_keys, Process.get(:tx_deleted_keys, MapSet.new()), fn
                  compound_key, acc ->
                    track_keydir_binary_remove(state, compound_key)
                    :ets.delete(state.ets, compound_key)

                    CompoundMemberIndex.delete(
                      Map.get(state, :compound_member_index_name),
                      compound_key
                    )

                    sm_tx_drop_pending(compound_key)
                    zset_index_delete(state, redis_key, compound_key)
                    MapSet.put(acc, compound_key)
                end)

              Process.put(:tx_deleted_keys, deleted)
              queue_promoted_maintenance_after_flush(redis_key, maintenance)

              queue_promoted_revision_deletes_after_flush(
                Map.get(state, :compound_revision_index_name),
                compound_keys
              )

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

      defp do_compound_delete_prefix(state, redis_key, prefix) do
        cleanup_target = promoted_prefix_cleanup_target(state, redis_key, prefix)
        result = do_compound_member_prefix_delete(state, redis_key, prefix)

        if result == :ok do
          cleanup_promoted_prefix!(state, redis_key, cleanup_target)
          zset_index_clear(state, redis_key)
        end

        result
      end

      defp promoted_prefix_cleanup_target(state, redis_key, prefix) do
        with type when type in [:hash, :set, :zset] <-
               exact_compound_prefix_type(redis_key, prefix),
             dedicated_path when is_binary(dedicated_path) <-
               promoted_compound_path(state, redis_key, prefix) do
          {type, dedicated_path}
        else
          _not_an_exact_promoted_prefix -> nil
        end
      end

      defp exact_compound_prefix_type(redis_key, prefix) do
        cond do
          prefix == CompoundKey.hash_prefix(redis_key) -> :hash
          prefix == CompoundKey.set_prefix(redis_key) -> :set
          prefix == CompoundKey.zset_prefix(redis_key) -> :zset
          true -> nil
        end
      end

      defp cleanup_promoted_prefix!(_state, _redis_key, nil), do: :ok

      defp cleanup_promoted_prefix!(state, redis_key, {type, dedicated_path}) do
        :ok =
          Promotion.cleanup_promoted!(
            redis_key,
            type,
            dedicated_path,
            state.shard_data_path,
            state.ets,
            state.data_dir,
            state.shard_index,
            state.instance_ctx
          )

        record_promoted_instance_removal(redis_key)
        queue_compound_promotion_removal_after_flush(Promotion.marker_key(redis_key))
      end

      defp record_promoted_instance_removal(redis_key) do
        removals = apply_state_get(:promoted_instance_removals, MapSet.new())
        apply_state_put(:promoted_instance_removals, MapSet.put(removals, redis_key))
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
        maintenance = promoted_put_maintenance(state, compound_key, disk_val)

        case NIF.v2_append_record(active, compound_key, disk_val, expire_at_ms) do
          {:ok, {offset, _record_size}} ->
            value_size = byte_size(disk_val)

            track_keydir_binary_delta(state, compound_key, value_for, expire_at_ms)

            :ets.insert(
              state.ets,
              {compound_key, value_for, expire_at_ms, LFU.initial(), fid, offset, value_size}
            )

            CompoundMemberIndex.put(Map.get(state, :compound_member_index_name), compound_key)
            sm_tx_put_pending(compound_key, value, expire_at_ms)
            deleted = Process.get(:tx_deleted_keys, MapSet.new())

            if MapSet.member?(deleted, compound_key) do
              Process.put(:tx_deleted_keys, MapSet.delete(deleted, compound_key))
            end

            queue_promoted_maintenance_after_flush(redis_key, maintenance)

            queue_promoted_revision_put_after_flush(
              Map.get(state, :compound_revision_index_name),
              compound_key
            )

            :ok

          {:error, _reason} = err ->
            err
        end
      end
    end
  end
end
