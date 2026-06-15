defmodule Ferricstore.Raft.StateMachine.Sections.ApplyDispatch do
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

      def apply(%{index: idx} = meta, _command, %{skip_below_index: skip} = state)
          when skip > 0 and idx <= skip do
        old_count = state.applied_count
        new_state = %{state | applied_count: old_count + 1}

        # Clear skip_below_index once we've passed it — no need to check on every apply
        new_state =
          if idx == skip, do: %{new_state | skip_below_index: 0}, else: new_state

        maybe_release_cursor(meta, old_count, new_state, :ok)
      end

      # Unwrap pre-serialized commands produced by the write Batcher.
      def apply(meta, {:ttb, binary}, state) when is_binary(binary) do
        __MODULE__.apply(meta, :erlang.binary_to_term(binary, [:safe]), state)
      end

      def apply(meta, {:ferricstore_latency_trace, inner_command}, state) do
        previous_trace = Ferricstore.LatencyTrace.start(%{})

        try do
          result =
            Ferricstore.LatencyTrace.span("server_apply_us", fn ->
              __MODULE__.apply(meta, inner_command, state)
            end)

          trace = Ferricstore.LatencyTrace.finish(previous_trace)
          wrap_latency_trace_apply_result(result, trace)
        rescue
          error ->
            _ = Ferricstore.LatencyTrace.finish(previous_trace)
            reraise error, __STACKTRACE__
        catch
          kind, reason ->
            _ = Ferricstore.LatencyTrace.finish(previous_trace)
            :erlang.raise(kind, reason, __STACKTRACE__)
        end
      end

      # Async commands. Router on the origin node has already persisted the write
      # Async single-command path. Delegates to apply_single which handles
      # origin-skip via the embedded origin node tag.
      def apply(meta, {:async, _origin, _inner_cmd} = cmd, state) do
        apply_pending_with_time(meta, state, fn -> apply_single(state, cmd) end)
      end

      # Backward-compat for 2-tuple async commands written by older binaries.
      # Treat as origin-unknown — apply unconditionally. Idempotent for put/delete,
      # may over-count repeated RMW on the same key (acceptable for one-time WAL
      # recovery; new writes use the 3-tuple form below).
      def apply(meta, {:async, _inner_cmd} = cmd, state) do
        apply_pending_with_time(meta, state, fn -> apply_single(state, cmd) end)
      end

      def apply(meta, {:put, key, value, expire_at_ms}, state) do
        with_apply_time(meta, fn ->
          redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

          result =
            case check_key_lock(state, redis_key, nil) do
              :ok ->
                with_pending_writes(state, fn -> do_put(state, key, value, expire_at_ms) end)

              {:error, :key_locked} ->
                {:error, :key_locked}
            end

          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:put_blob_ref, key, encoded_ref, expire_at_ms}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_checked_put_blob_ref_ref_only(state, key, encoded_ref, expire_at_ms)
        end)
      end

      def apply(meta, {:set, key, value, expire_at_ms, opts}, state) do
        with_apply_time(meta, fn ->
          redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

          result =
            case check_key_lock(state, redis_key, nil) do
              :ok ->
                with_pending_writes(state, fn -> do_set(state, key, value, expire_at_ms, opts) end)

              {:error, :key_locked} ->
                {:error, :key_locked}
            end

          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:set_blob_ref, key, encoded_ref, expire_at_ms, opts}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_checked_set_blob_ref(state, key, encoded_ref, expire_at_ms, opts)
        end)
      end

      def apply(meta, {:delete, key}, state) do
        with_apply_time(meta, fn ->
          redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

          result =
            case check_key_lock(state, redis_key, nil) do
              :ok ->
                with_pending_writes(state, fn -> do_delete(state, key) end)

              {:error, :key_locked} ->
                {:error, :key_locked}
            end

          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:put_batch, entries}, state) when is_list(entries) do
        with_apply_time(meta, fn ->
          old_count = state.applied_count

          write_result =
            with_pending_writes(state, fn ->
              apply_put_batch_entries(state, entries)
            end)

          finish_hot_batch_apply(meta, old_count, state, length(entries), write_result)
        end)
      end

      def apply(meta, {:put_blob_batch, entries}, state) when is_list(entries) do
        with_apply_time(meta, fn ->
          old_count = state.applied_count

          write_result =
            with_pending_writes(state, fn ->
              apply_put_blob_batch_entries(state, entries)
            end)

          finish_hot_batch_apply(meta, old_count, state, length(entries), write_result)
        end)
      end

      def apply(meta, {:delete_batch, keys}, state) when is_list(keys) do
        with_apply_time(meta, fn ->
          old_count = state.applied_count

          write_result =
            with_pending_writes(state, fn ->
              apply_delete_batch_keys(state, keys)
            end)

          finish_hot_batch_apply(meta, old_count, state, length(keys), write_result)
        end)
      end

      def apply(meta, {:delete_prefix, prefix}, state) when is_binary(prefix) do
        with_apply_time(meta, fn ->
          result = with_pending_writes(state, fn -> do_delete_prefix(state, prefix) end)

          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:batch, commands}, state) do
        with_apply_time(meta, fn ->
          commands = normalize_generic_batch_commands(commands)
          old_count = state.applied_count
          applied_increment = length(commands)

          # All commands in a batch share one pending-writes buffer so they
          # are flushed in a single v2_append_batch_nosync NIF call.
          write_result =
            case prepare_apply_blob_command(state, {:batch, commands}) do
              {:ok, {:batch, prepared_commands}} ->
                with_pending_writes(state, fn ->
                  Enum.map_reduce(prepared_commands, old_count, fn cmd, count ->
                    result = apply_single(state, cmd)
                    {result, count + 1}
                  end)
                end)

              {:ok, prepared_command} ->
                with_pending_writes(state, fn ->
                  result = apply_single(state, prepared_command)
                  {List.wrap(result), old_count + applied_increment}
                end)

              {:error, _reason} = error ->
                error
            end

          case write_result do
            {:error, _reason} = error ->
              new_state = %{state | applied_count: old_count + applied_increment}
              maybe_release_cursor(meta, old_count, new_state, error)

            {results, new_count} ->
              new_state = %{state | applied_count: new_count}
              maybe_release_cursor(meta, old_count, new_state, {:ok, results})
          end
        end)
      end

      def apply(meta, {:cross_shard_tx, shard_batches, watched_keys}, state)
          when is_map(watched_keys) do
        apply_cross_shard_tx(meta, shard_batches, watched_keys, state)
      end

      def apply(meta, {:cross_shard_tx, shard_batches}, state) do
        apply_cross_shard_tx(meta, shard_batches, %{}, state)
      end

      defp wrap_latency_trace_apply_result({state, result}, trace) do
        {state, Ferricstore.LatencyTrace.wrap_result(result, trace)}
      end

      defp wrap_latency_trace_apply_result({state, result, effects}, trace) do
        {state, Ferricstore.LatencyTrace.wrap_result(result, trace), effects}
      end

      # Legacy: list operations used to be sent as a single {:list_op} Raft entry
      # containing the entire operation. Now lists use compound keys (L:key\0pos)
      # and individual {:put}/{:delete} entries. This handler remains for WAL
      # replay of entries written before the compound-key migration.
      def apply(meta, {:list_op, key, operation}, state) do
        with_apply_time(meta, fn ->
          result = with_pending_writes(state, fn -> do_checked_list_op(state, key, operation) end)

          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:list_op_lmove, src_key, dst_key, from_dir, to_dir}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_checked_lmove(state, src_key, dst_key, from_dir, to_dir)
        end)
      end

      def apply(meta, {:hset_single, key, field, value}, state) do
        apply_pending_with_time(meta, state, fn ->
          apply_single(state, {:hset_single, key, field, value})
        end)
      end

      def apply(meta, {:lpush_single, key, value}, state) do
        apply_pending_with_time(meta, state, fn ->
          apply_single(state, {:lpush_single, key, value})
        end)
      end

      def apply(meta, {:rpush_single, key, value}, state) do
        apply_pending_with_time(meta, state, fn ->
          apply_single(state, {:rpush_single, key, value})
        end)
      end

      def apply(meta, {:sadd_single, key, member}, state) do
        apply_pending_with_time(meta, state, fn ->
          apply_single(state, {:sadd_single, key, member})
        end)
      end

      def apply(meta, {:srem_single, key, member}, state) do
        apply_pending_with_time(meta, state, fn ->
          apply_single(state, {:srem_single, key, member})
        end)
      end

      def apply(meta, {:zadd_single, key, score, member}, state) do
        apply_pending_with_time(meta, state, fn ->
          apply_single(state, {:zadd_single, key, score, member})
        end)
      end

      def apply(meta, {:zadd_many_single, entries}, state) when is_list(entries) do
        with_apply_time(meta, fn ->
          old_count = state.applied_count

          write_result =
            with_pending_writes(state, fn ->
              {apply_zadd_many_single_entries(state, entries), old_count + length(entries)}
            end)

          case write_result do
            {:error, _reason} = error ->
              new_state = %{state | applied_count: old_count + length(entries)}
              maybe_release_cursor(meta, old_count, new_state, error)

            {results, new_count} ->
              new_state = %{state | applied_count: new_count}
              maybe_release_cursor(meta, old_count, new_state, {:ok, results})
          end
        end)
      end

      def apply(meta, {:zrem_single, key, member}, state) do
        apply_pending_with_time(meta, state, fn ->
          apply_single(state, {:zrem_single, key, member})
        end)
      end

      def apply(meta, {:compound_put, compound_key, value, expire_at_ms}, state) do
        with_apply_time(meta, fn ->
          redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(compound_key)

          result =
            case check_key_lock(state, redis_key, nil) do
              :ok ->
                with_pending_writes(state, fn ->
                  do_compound_put(state, redis_key, compound_key, value, expire_at_ms)
                end)

              {:error, :key_locked} ->
                {:error, :key_locked}
            end

          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:compound_put_blob_ref, compound_key, encoded_ref, expire_at_ms}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_checked_compound_put_blob_ref(state, compound_key, encoded_ref, expire_at_ms)
        end)
      end

      def apply(meta, {:compound_batch_put, redis_key, entries}, state)
          when is_binary(redis_key) and is_list(entries) do
        with_apply_time(meta, fn ->
          old_count = state.applied_count

          write_result =
            with_pending_writes(state, fn ->
              apply_compound_batch_put_entries(state, redis_key, entries)
            end)

          finish_hot_batch_apply(meta, old_count, state, length(entries), write_result)
        end)
      end

      def apply(meta, {:compound_blob_batch_put, redis_key, entries}, state)
          when is_binary(redis_key) and is_list(entries) do
        with_apply_time(meta, fn ->
          old_count = state.applied_count

          write_result =
            with_pending_writes(state, fn ->
              apply_compound_blob_batch_put_entries(state, redis_key, entries)
            end)

          finish_hot_batch_apply(meta, old_count, state, length(entries), write_result)
        end)
      end

      def apply(meta, {:compound_delete, compound_key}, state) do
        with_apply_time(meta, fn ->
          redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(compound_key)

          result =
            case check_key_lock(state, redis_key, nil) do
              :ok ->
                with_pending_writes(state, fn ->
                  do_compound_delete(state, redis_key, compound_key)
                end)

              {:error, :key_locked} ->
                {:error, :key_locked}
            end

          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:compound_batch_delete, redis_key, compound_keys}, state)
          when is_binary(redis_key) and is_list(compound_keys) do
        with_apply_time(meta, fn ->
          old_count = state.applied_count

          write_result =
            with_pending_writes(state, fn ->
              apply_compound_batch_delete_keys(state, redis_key, compound_keys)
            end)

          finish_hot_batch_apply(meta, old_count, state, length(compound_keys), write_result)
        end)
      end

      def apply(meta, {:compound_delete_prefix, prefix}, state) do
        with_apply_time(meta, fn ->
          redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(prefix)

          result =
            case check_key_lock(state, redis_key, nil) do
              :ok ->
                with_pending_writes(state, fn ->
                  do_compound_delete_prefix(state, redis_key, prefix)
                end)

              {:error, :key_locked} ->
                {:error, :key_locked}
            end

          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:incr, key, delta}, state) do
        apply_pending_with_time(meta, state, fn -> do_incr(state, key, delta) end)
      end

      def apply(meta, {:incr_float, key, delta}, state) do
        apply_pending_with_time(meta, state, fn -> do_incr_float(state, key, delta) end)
      end

      def apply(meta, {:append, key, suffix}, state) do
        apply_pending_with_time(meta, state, fn -> do_append(state, key, suffix) end)
      end

      def apply(meta, {:append_blob_ref, key, encoded_ref}, state) do
        apply_pending_with_time(meta, state, fn -> do_append_blob_ref(state, key, encoded_ref) end)
      end

      def apply(meta, {:getset, key, new_value}, state) do
        apply_pending_with_time(meta, state, fn -> do_getset(state, key, new_value) end)
      end

      def apply(meta, {:getset_blob_ref, key, encoded_ref}, state) do
        apply_pending_with_time(meta, state, fn -> do_getset_blob_ref(state, key, encoded_ref) end)
      end

      def apply(meta, {:getdel, key}, state) do
        apply_pending_with_time(meta, state, fn -> do_getdel(state, key) end)
      end

      def apply(meta, {:getex, key, expire_at_ms}, state) do
        apply_pending_with_time(meta, state, fn -> do_getex(state, key, expire_at_ms) end)
      end

      def apply(meta, {:setrange, key, offset, value}, state) do
        apply_pending_with_time(meta, state, fn -> do_setrange(state, key, offset, value) end)
      end

      def apply(meta, {:setrange_blob_ref, key, offset, encoded_ref}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_setrange_blob_ref(state, key, offset, encoded_ref)
        end)
      end

      # Atomic SETBIT — read bitmap blob, mutate one bit, write back. Previously
      # the read+compute+write ran in the caller process (FerricStore.setbit/3),
      # losing updates under concurrent writes on the same key.
      def apply(meta, {:setbit, key, offset, bit_val}, state) do
        apply_pending_with_time(meta, state, fn -> do_setbit(state, key, offset, bit_val) end)
      end

      # Atomic HINCRBY / HINCRBYFLOAT — read compound field, add delta, write back.
      # Previously ran in caller process and lost updates under concurrent hincrby
      # on the same field.
      def apply(meta, {:hincrby, key, field, delta}, state) do
        apply_pending_with_time(meta, state, fn -> do_hincrby(state, key, field, delta) end)
      end

      def apply(meta, {:hincrbyfloat, key, field, delta}, state) do
        apply_pending_with_time(meta, state, fn -> do_hincrbyfloat(state, key, field, delta) end)
      end

      # Atomic ZINCRBY — read zset member's score, add increment, write back.
      # Also sets the type metadata atomically if absent (first write to the key).
      def apply(meta, {:zincrby, key, increment, member}, state) do
        apply_pending_with_time(meta, state, fn -> do_zincrby(state, key, increment, member) end)
      end

      def apply(meta, {:pfadd, key, elements}, state) do
        apply_pending_with_time(meta, state, fn ->
          HyperLogLog.handle_ast({:pfadd, [key | elements]}, build_string_value_store(state))
        end)
      end

      def apply(meta, {:pfmerge, dest_key, source_sketches}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_pfmerge(state, dest_key, source_sketches)
        end)
      end

      def apply(meta, {:pfmerge, dest_key, _source_keys, source_sketches}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_pfmerge(state, dest_key, source_sketches)
        end)
      end

      def apply(meta, {:spop, key, count}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_spop(state, key, count, Map.get(meta, :index, 0))
        end)
      end

      def apply(meta, {:zpop, key, count, direction}, state) do
        apply_pending_with_time(meta, state, fn -> do_zpop(state, key, count, direction) end)
      end

      def apply(meta, {:cas, key, expected, new_value, ttl_ms}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_cas(state, key, expected, new_value, ttl_ms)
        end)
      end

      def apply(meta, {:cas_blob_ref, key, expected, encoded_ref, ttl_ms}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_cas_blob_ref(state, key, expected, encoded_ref, ttl_ms)
        end)
      end

      def apply(meta, {:lock, key, owner, ttl_ms}, state) do
        apply_pending_with_time(meta, state, fn -> do_lock(state, key, owner, ttl_ms) end)
      end

      def apply(meta, {:unlock, key, owner}, state) do
        apply_pending_with_time(meta, state, fn -> do_unlock(state, key, owner) end)
      end

      def apply(meta, {:extend, key, owner, ttl_ms}, state) do
        apply_pending_with_time(meta, state, fn -> do_extend(state, key, owner, ttl_ms) end)
      end

      def apply(meta, {:ratelimit_add, key, window_ms, max, count}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_ratelimit_add(state, key, window_ms, max, count, nil)
        end)
      end

      # Legacy 6-tuple variant: older submitters embedded now_ms before commands
      # were HLC-stamped at the Raft boundary. Stamped wrappers normalize this back
      # to the 5-tuple so the single log-entry timestamp wins.
      def apply(meta, {:ratelimit_add, key, window_ms, max, count, now_ms}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_ratelimit_add(state, key, window_ms, max, count, now_ms)
        end)
      end

      # ---------------------------------------------------------------------------
      # Cross-shard operation commands (mini-percolator)
      #
      # These commands support the CrossShardOp protocol: per-key locking through
      # Raft consensus, intent records for crash recovery, and locked writes.
      # ---------------------------------------------------------------------------

      def apply(meta, {:lock_keys, keys, owner_ref, expire_at_ms}, state) do
        with_apply_time(meta, fn ->
          {new_state, result} = do_lock_keys(state, keys, owner_ref, expire_at_ms)
          old_count = state.applied_count
          new_state = %{new_state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:unlock_keys, keys, owner_ref}, state) do
        apply_control_with_time(meta, state, fn -> do_unlock_keys(state, keys, owner_ref) end)
      end

      def apply(meta, {:cross_shard_intent, owner_ref, intent_map}, state) do
        apply_control_with_time(meta, state, fn ->
          do_write_intent(state, owner_ref, intent_map)
        end)
      end

      def apply(meta, {:delete_intent, owner_ref}, state) do
        apply_control_with_time(meta, state, fn -> do_delete_intent(state, owner_ref) end)
      end

      def apply(meta, {:get_intents}, state) do
        apply_control_with_time(meta, state, fn -> {state, do_get_intents(state)} end)
      end

      def apply(meta, {:get_lock_count}, state) do
        apply_control_with_time(meta, state, fn ->
          {state, map_size(Map.get(state, :cross_shard_locks, %{}))}
        end)
      end

      def apply(meta, {:clear_locks}, state) do
        apply_control_with_time(meta, state, fn ->
          {state |> Map.put(:cross_shard_locks, %{}) |> Map.put(:cross_shard_intents, %{}), :ok}
        end)
      end

      def apply(meta, {:locked_put, key, value, expire_at_ms, owner_ref}, state) do
        with_apply_time(meta, fn ->
          redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

          result =
            case check_key_lock(state, redis_key, owner_ref) do
              :ok ->
                with_pending_writes(state, fn -> do_put(state, key, value, expire_at_ms) end)

              {:error, _} = err ->
                err
            end

          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:locked_put_blob_ref, key, encoded_ref, expire_at_ms, owner_ref}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_locked_put_blob_ref(state, key, encoded_ref, expire_at_ms, owner_ref)
        end)
      end

      def apply(meta, {:locked_delete, key, owner_ref}, state) do
        with_apply_time(meta, fn ->
          redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

          result =
            case check_key_lock(state, redis_key, owner_ref) do
              :ok ->
                with_pending_writes(state, fn -> do_delete(state, key) end)

              {:error, _} = err ->
                err
            end

          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:locked_delete_prefix, prefix, owner_ref}, state) do
        with_apply_time(meta, fn ->
          redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(prefix)

          result =
            case check_key_lock(state, redis_key, owner_ref) do
              :ok ->
                with_pending_writes(state, fn -> do_delete_prefix(state, prefix) end)

              {:error, _} = err ->
                err
            end

          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      # ---------------------------------------------------------------------------
      # Probabilistic data structure commands (bloom, CMS, cuckoo, TopK)
      #
      # These commands replicate prob mutations through Raft so that followers
      # apply the same NIF writes to their local prob files. Read commands
      # (BF.EXISTS, CMS.QUERY, etc.) bypass Raft and go directly to the local
      # stateless pread NIF.
      # ---------------------------------------------------------------------------

      # -- Bloom --

      def apply(meta, {:bloom_create, key, num_bits, num_hashes, prob_meta}, state) do
        apply_prob_with_time(meta, state, fn ->
          create_bloom_metadata(state, key, num_bits, num_hashes, prob_meta)
        end)
      end

      def apply(meta, {:bloom_add, key, element, auto_create_params}, state) do
        apply_prob_with_time(meta, state, fn ->
          path = prob_path(state, key, "bloom")

          with :ok <- ensure_prob_dir(state) do
            case auto_create_bloom_if_needed(state, path, key, auto_create_params) do
              :ok -> NIF.bloom_file_add(path, element)
              {:error, _reason} = error -> error
            end
          end
        end)
      end

      def apply(meta, {:bloom_madd, key, elements, auto_create_params}, state) do
        apply_prob_with_time(meta, state, fn ->
          path = prob_path(state, key, "bloom")

          with :ok <- ensure_prob_dir(state) do
            case auto_create_bloom_if_needed(state, path, key, auto_create_params) do
              :ok -> NIF.bloom_file_madd(path, elements)
              {:error, _reason} = error -> error
            end
          end
        end)
      end

      # -- CMS --

      def apply(meta, {:cms_create, key, width, depth}, state) do
        apply_prob_with_time(meta, state, fn ->
          create_cms_metadata(state, key, width, depth)
        end)
      end

      def apply(meta, {:cms_incrby, key, items}, state) do
        apply_prob_with_time(meta, state, fn ->
          path = prob_path(state, key, "cms")
          NIF.cms_file_incrby(path, items)
        end)
      end

      def apply(meta, {:cms_merge, dst_key, src_keys, weights, create_params}, state) do
        apply_prob_with_time(meta, state, fn ->
          dst_path = prob_path(state, dst_key, "cms")
          src_paths = cms_source_paths(state, src_keys)

          with :ok <- ensure_prob_dir(state) do
            case maybe_create_cms_merge_dst(state, dst_path, dst_key, create_params) do
              :ok -> NIF.cms_file_merge(dst_path, src_paths, weights)
              {:error, _reason} = error -> error
            end
          end
        end)
      end

      # -- Cuckoo --

      def apply(meta, {:cuckoo_create, key, capacity, bucket_size}, state) do
        apply_prob_with_time(meta, state, fn ->
          create_cuckoo_metadata(state, key, capacity, bucket_size)
        end)
      end

      def apply(meta, {:cuckoo_add, key, element, auto_create_params}, state) do
        apply_prob_with_time(meta, state, fn ->
          path = prob_path(state, key, "cuckoo")

          with :ok <- ensure_prob_dir(state) do
            case auto_create_cuckoo_if_needed(state, path, key, auto_create_params) do
              :ok -> NIF.cuckoo_file_add(path, element)
              {:error, _reason} = error -> error
            end
          end
        end)
      end

      def apply(meta, {:cuckoo_addnx, key, element, auto_create_params}, state) do
        apply_prob_with_time(meta, state, fn ->
          path = prob_path(state, key, "cuckoo")

          with :ok <- ensure_prob_dir(state) do
            case auto_create_cuckoo_if_needed(state, path, key, auto_create_params) do
              :ok -> NIF.cuckoo_file_addnx(path, element)
              {:error, _reason} = error -> error
            end
          end
        end)
      end

      def apply(meta, {:cuckoo_del, key, element}, state) do
        apply_prob_with_time(meta, state, fn ->
          path = prob_path(state, key, "cuckoo")
          NIF.cuckoo_file_del(path, element)
        end)
      end

      # -- TopK --

      def apply(meta, {:topk_create, key, k, width, depth, decay}, state) do
        apply_prob_with_time(meta, state, fn ->
          create_topk_metadata(state, key, k, width, depth, decay)
        end)
      end

      def apply(meta, {:topk_add, key, elements}, state) do
        apply_prob_with_time(meta, state, fn ->
          path = prob_path(state, key, "topk")
          NIF.topk_file_add_v2(path, elements)
        end)
      end

      def apply(meta, {:topk_incrby, key, pairs}, state) do
        apply_prob_with_time(meta, state, fn ->
          path = prob_path(state, key, "topk")
          NIF.topk_file_incrby_v2(path, pairs)
        end)
      end

      def apply(meta, {:tx_execute, queue, sandbox_namespace}, state) when is_list(queue) do
        apply_pending_with_time(meta, state, fn ->
          do_tx_execute(state, queue, sandbox_namespace)
        end)
      end

      # -- Flow --

      def apply(meta, {:flow_create, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_create, attrs, fn ->
          do_flow_create(state, attrs)
        end)
      end

      def apply(meta, {:flow_create_many, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_create_many, attrs, fn ->
          do_flow_create_many(state, attrs)
        end)
      end

      def apply(meta, {:flow_create_pipeline_batch, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_create_pipeline_batch, attrs, fn ->
          do_flow_create_pipeline_batch(state, attrs)
        end)
      end

      def apply(meta, {:flow_start_and_claim_pipeline_batch, _key, attrs}, state)
          when is_map(attrs) do
        apply_flow_pending_with_time(
          meta,
          state,
          :flow_start_and_claim_pipeline_batch,
          attrs,
          fn ->
            do_flow_start_and_claim_pipeline_batch(state, attrs)
          end
        )
      end

      def apply(meta, {:flow_named_value_put, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_named_value_put, attrs, fn ->
          do_flow_named_value_put(state, attrs)
        end)
      end

      def apply(meta, {:flow_named_value_put_pipeline_batch, _key, attrs}, state)
          when is_map(attrs) do
        apply_flow_pending_with_time(
          meta,
          state,
          :flow_named_value_put_pipeline_batch,
          attrs,
          fn ->
            do_flow_named_value_put_pipeline_batch(state, attrs)
          end
        )
      end

      def apply(meta, {:flow_signal, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_signal, attrs, fn ->
          do_flow_signal(state, attrs)
        end)
      end

      def apply(meta, {:flow_signal_many, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_signal_many, attrs, fn ->
          do_flow_signal_many(state, attrs)
        end)
      end

      def apply(meta, {:flow_spawn_children, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_spawn_children, attrs, fn ->
          do_flow_spawn_children(state, attrs)
        end)
      end

      def apply(meta, {:flow_claim_due, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_claim_due, attrs, fn ->
          do_flow_claim_due(state, attrs)
        end)
      end

      def apply(meta, {:flow_extend_lease, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_extend_lease, attrs, fn ->
          do_flow_extend_lease(state, attrs)
        end)
      end

      def apply(meta, {:flow_complete, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_complete, attrs, fn ->
          do_flow_complete(state, attrs)
        end)
      end

      def apply(meta, {:flow_complete_many, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_complete_many, attrs, fn ->
          do_flow_complete_many(state, attrs)
        end)
      end

      def apply(meta, {:flow_terminal_pipeline_batch, op, _key, attrs}, state)
          when op in [:complete, :retry, :fail, :cancel] and is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_terminal_pipeline_batch, attrs, fn ->
          do_flow_terminal_pipeline_batch(state, op, attrs)
        end)
      end

      def apply(meta, {:flow_transition, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_transition, attrs, fn ->
          do_flow_transition(state, attrs)
        end)
      end

      def apply(meta, {:flow_start_and_claim, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_start_and_claim, attrs, fn ->
          do_flow_start_and_claim(state, attrs)
        end)
      end

      def apply(meta, {:flow_run_steps_many, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_run_steps_many, attrs, fn ->
          do_flow_run_steps_many(state, attrs)
        end)
      end

      def apply(meta, {:flow_step_continue, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_step_continue, attrs, fn ->
          do_flow_step_continue(state, attrs)
        end)
      end

      def apply(meta, {:flow_step_continue_many, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_step_continue_many, attrs, fn ->
          do_flow_step_continue_many(state, attrs)
        end)
      end

      def apply(meta, {:flow_transition_many, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_transition_many, attrs, fn ->
          do_flow_transition_many(state, attrs)
        end)
      end

      def apply(meta, {:flow_retry, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_retry, attrs, fn ->
          do_flow_retry(state, attrs)
        end)
      end

      def apply(meta, {:flow_retry_many, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_retry_many, attrs, fn ->
          do_flow_retry_many(state, attrs)
        end)
      end

      def apply(meta, {:flow_fail, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_fail, attrs, fn ->
          do_flow_fail(state, attrs)
        end)
      end

      def apply(meta, {:flow_fail_many, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_fail_many, attrs, fn ->
          do_flow_fail_many(state, attrs)
        end)
      end

      def apply(meta, {:flow_cancel, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_cancel, attrs, fn ->
          do_flow_cancel(state, attrs)
        end)
      end

      def apply(meta, {:flow_cancel_many, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_cancel_many, attrs, fn ->
          do_flow_cancel_many(state, attrs)
        end)
      end

      def apply(meta, {:flow_retention_cleanup, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_retention_cleanup, attrs, fn ->
          do_flow_retention_cleanup(state, attrs)
        end)
      end

      def apply(meta, {:flow_rewind, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_rewind, attrs, fn ->
          do_flow_rewind(state, attrs)
        end)
      end

      # ---------------------------------------------------------------------------
      # HLC-wrapped commands (spec 2G.6)
      #
      # Raft submit paths stamp commands before they enter the log. During apply,
      # the stamped physical HLC time is installed through `CommandTime`, so command
      # modules compute relative expiries and other time-derived values from the
      # same log-entry timestamp on every replica.
      # ---------------------------------------------------------------------------

      # Generic server command hook — allows server apps to replicate their own
      # commands through Raft without the library knowing what they are.
      # The server registers a raft_apply_hook callback on the Instance struct.
      def apply(meta, {:server_command, command}, state) do
        with_apply_time(meta, fn ->
          hook = raft_apply_hook(state)
          result = if hook, do: hook.(command), else: {:error, :no_hook}
          bump_applied(meta, state, result)
        end)
      end

      def apply(meta, {inner_command, %{hlc_ts: {physical_ms, _logical} = remote_ts}}, state)
          when is_tuple(inner_command) and is_integer(physical_ms) do
        merge_hlc(remote_ts)

        __MODULE__.apply(
          Map.put(meta, :system_time, physical_ms),
          normalize_stamped_command(inner_command),
          state
        )
      end

      # Catch-all: unknown commands should not crash the ra state machine.
      # Log the unrecognized command and return an error result so the caller
      # gets a meaningful error instead of ra crashing with FunctionClauseError.
      def apply(_meta, unknown_command, state) do
        require Logger
        Logger.error("StateMachine: unrecognized command: #{inspect(unknown_command)}")
        {state, {:error, {:unknown_command, unknown_command}}}
      end
    end
  end
end
