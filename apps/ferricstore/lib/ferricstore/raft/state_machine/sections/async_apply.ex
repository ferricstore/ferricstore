defmodule Ferricstore.Raft.StateMachine.Sections.AsyncApply do
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

      defp valid_waraft_segment_location_value?(file_id, offset, value_size) do
        is_tuple(file_id) and tuple_size(file_id) == 2 and
          elem(file_id, 0) in [:waraft_segment, :waraft_projection, :waraft_apply_projection] and
          is_integer(elem(file_id, 1)) and elem(file_id, 1) > 0 and is_integer(offset) and
          offset >= 0 and is_integer(value_size) and value_size >= 0
      end

      defp cross_shard_delete_keydir_entry(ctx, key, value) do
        ref = keydir_binary_ref(ctx)

        if ref do
          bytes = binary_byte_size(key) + binary_byte_size(value)
          if bytes > 0, do: :atomics.sub(ref, ctx.index + 1, bytes)
        end

        :ets.delete(ctx.keydir, key)
      end

      defp parse_fid_from_path(path) do
        path
        |> Path.basename()
        |> String.trim_trailing(".log")
        |> String.to_integer()
      end

      # ---------------------------------------------------------------------------
      # Private: async origin-skip detection
      # ---------------------------------------------------------------------------

      # Returns true when the local ETS already contains an entry for the key
      # targeted by the inner async command. This is how each node decides
      # whether it was the origin (Router already wrote) or a replica (empty
      # ETS, needs to apply). Deterministic per-node because it reads the
      # node's own local ETS state.
      defp async_key_present?(state, {:put, key, _value, _exp}), do: ets_has?(state.ets, key)
      # Delete/getdel: Router deletes from ETS before Raft submit, so ets_has?
      # always returns false on origin. Always apply — tombstone writes are idempotent.
      defp async_key_present?(_state, {:delete, _key}), do: false
      defp async_key_present?(state, {:incr, key, _delta}), do: ets_has?(state.ets, key)
      defp async_key_present?(state, {:incr_float, key, _delta}), do: ets_has?(state.ets, key)
      defp async_key_present?(state, {:append, key, _suffix}), do: ets_has?(state.ets, key)
      defp async_key_present?(state, {:getset, key, _v}), do: ets_has?(state.ets, key)
      defp async_key_present?(_state, {:getdel, _key}), do: false
      defp async_key_present?(state, {:getex, key, _exp}), do: ets_has?(state.ets, key)
      defp async_key_present?(state, {:setrange, key, _off, _v}), do: ets_has?(state.ets, key)
      defp async_key_present?(state, {:setbit, key, _off, _bit}), do: ets_has?(state.ets, key)

      defp async_key_present?(state, {:hincrby, key, field, _delta}) do
        ets_has?(state.ets, Ferricstore.Store.CompoundKey.hash_field(key, field))
      end

      defp async_key_present?(state, {:hincrbyfloat, key, field, _delta}) do
        ets_has?(state.ets, Ferricstore.Store.CompoundKey.hash_field(key, field))
      end

      defp async_key_present?(state, {:zincrby, key, _incr, member}) do
        ets_has?(state.ets, Ferricstore.Store.CompoundKey.zset_member(key, member))
      end

      # List ops check the canonical type marker written by the origin before
      # submit. On replicas the marker is absent, so they apply the inner op.
      defp async_key_present?(state, {:list_op, key, _op}) do
        ets_has?(state.ets, Ferricstore.Store.CompoundKey.type_key(key))
      end

      defp async_key_present?(state, {:list_op_lmove, src_key, _dst, _from, _to}) do
        ets_has?(state.ets, Ferricstore.Store.CompoundKey.type_key(src_key))
      end

      # Unknown inner command shape — conservative fallback: apply it (treat as replica).
      defp async_key_present?(_state, _other), do: false

      defp ets_has?(ets, key) do
        case :ets.lookup(ets, key) do
          [] -> false
          _ -> true
        end
      end

      # ---------------------------------------------------------------------------
      # Private: command execution
      # ---------------------------------------------------------------------------

      defp do_tx_execute(state, queue, sandbox_namespace) do
        case ShardTransaction.handle_tx_execute(queue, sandbox_namespace, state) do
          {:reply, results, _state} -> results
        end
      end

      # 3-tuple async clauses (current shape, with origin node tag).
      #
      # Origin node decides skip vs apply: each peer compares the embedded
      # `origin` against its own `node()`. Deterministic and correct even when
      # the same key receives multiple RMW commands in rapid succession.
      #
      # Single-node mode (no Erlang distribution) reports `node() == :nonode@nohost`,
      # which equals the originating node by the same name — so the origin-skip
      # still fires correctly and avoids the double-write.

      defp apply_single(state, {:ferricstore_latency_trace, inner_command}) do
        previous_trace = Ferricstore.LatencyTrace.start(%{})

        try do
          result =
            Ferricstore.LatencyTrace.span("server_apply_us", fn ->
              apply_single(state, inner_command)
            end)

          trace = Ferricstore.LatencyTrace.finish(previous_trace)
          Ferricstore.LatencyTrace.wrap_result(result, trace)
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

      # Async PUT, origin: skip ETS (Router already inserted) but accumulate
      # disk write only for small values (file_id == :pending means Router
      # deferred disk write to us). Large values already have a real file_id
      # and offset from Router's synchronous NIF write — skip disk too.
      defp apply_single(state, {:async, origin, {:put, key, value, expire_at_ms} = _inner})
           when origin == node() do
        apply_origin_async_put(state, key, value, expire_at_ms)
      end

      # Async PUT, replica: apply normally (both ETS + disk).
      defp apply_single(state, {:async, _origin, {:put, key, value, expire_at_ms}}) do
        apply_single(state, {:put, key, value, expire_at_ms})
      end

      # DELETE/GETDEL are idempotent and must persist an accepted tombstone on the
      # origin even when Router already removed the ETS row. Router's local
      # BitcaskWriter tombstone is asynchronous and can fail independently; the Ra
      # entry is the authoritative repair path.
      defp apply_single(state, {:async, origin, {:delete, key}}) when origin == node() do
        apply_single(state, {:delete, key})
      end

      defp apply_single(state, {:async, origin, {:getdel, key}}) when origin == node() do
        if ets_has?(state.ets, key) do
          apply_single(state, {:getdel, key})
        else
          _ = apply_single(state, {:delete, key})
          nil
        end
      end

      defp apply_single(
             state,
             {:async, origin,
              {:origin_checked, key, inner_cmd, before_value, before_expire_at_ms, expected_value,
               expire_at_ms}}
           )
           when origin == node() do
        case origin_replay_decision(
               state,
               key,
               inner_cmd,
               before_value,
               before_expire_at_ms,
               expected_value,
               expire_at_ms
             ) do
          :already_applied ->
            maybe_queue_already_applied_origin_put(
              state,
              key,
              inner_cmd,
              expected_value,
              expire_at_ms
            )

          :apply ->
            apply_single(state, inner_cmd)

          :apply_expected ->
            apply_origin_checked_expected(
              state,
              key,
              inner_cmd,
              before_value,
              expected_value,
              expire_at_ms
            )

          :newer_local_value ->
            :ok
        end
      end

      defp apply_single(
             state,
             {:async, origin, {:origin_checked, key, inner_cmd, expected_value, expire_at_ms}}
           )
           when origin == node() do
        if origin_command_already_applied?(state, key, inner_cmd, expected_value, expire_at_ms) do
          :ok
        else
          apply_single(state, inner_cmd)
        end
      end

      # Other async commands, origin: skip when Router already applied locally.
      # If recovery has no local marker/value, apply the accepted Ra entry so an
      # origin crash after Ra acceptance cannot lose the command.
      defp apply_single(state, {:async, origin, inner_cmd}) when origin == node() do
        if async_key_present?(state, inner_cmd), do: :ok, else: apply_single(state, inner_cmd)
      end

      defp apply_single(
             state,
             {:async, _origin,
              {:origin_checked, _key, inner_cmd, _before_value, _before_exp, _value, _exp}}
           ) do
        apply_single(state, inner_cmd)
      end

      defp apply_single(
             state,
             {:async, _origin, {:origin_checked, _key, inner_cmd, _value, _exp}}
           ) do
        apply_single(state, inner_cmd)
      end

      # Other async commands, replica: apply.
      defp apply_single(state, {:async, _origin, inner_cmd}) do
        apply_single(state, inner_cmd)
      end

      # 2-tuple async clauses (legacy shape from binaries before origin tagging).
      # Kept for WAL backward compatibility — replays still work. New writes use
      # the 3-tuple form. Falls back to the ETS-presence heuristic which is
      # imperfect for repeated RMW on the same key but correct for the common
      # case (single put/delete/incr per key per batch).
      defp apply_single(state, {:async, {:put, key, value, expire_at_ms} = _inner}) do
        if async_key_present?(state, {:put, key, value, expire_at_ms}) do
          maybe_queue_origin_pending_put(state, key, value, expire_at_ms)
          :ok
        else
          apply_single(state, {:put, key, value, expire_at_ms})
        end
      end

      defp apply_single(state, {:async, inner_cmd}) do
        if async_key_present?(state, inner_cmd) do
          :ok
        else
          apply_single(state, inner_cmd)
        end
      end

      defp apply_single(state, {:put, key, value, expire_at_ms}) do
        redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

        case check_key_lock(state, redis_key, nil) do
          :ok -> do_put(state, key, value, expire_at_ms)
          {:error, :key_locked} -> {:error, :key_locked}
        end
      end

      defp apply_single(state, {:put_blob_ref, key, encoded_ref, expire_at_ms}) do
        do_checked_put_blob_ref(state, key, encoded_ref, expire_at_ms)
      end

      defp apply_single(state, {:set, key, value, expire_at_ms, opts}) do
        redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

        case check_key_lock(state, redis_key, nil) do
          :ok -> do_set(state, key, value, expire_at_ms, opts)
          {:error, :key_locked} -> {:error, :key_locked}
        end
      end

      defp apply_single(state, {:set_blob_ref, key, encoded_ref, expire_at_ms, opts}) do
        do_checked_set_blob_ref(state, key, encoded_ref, expire_at_ms, opts)
      end

      defp apply_single(state, {:delete, key}) do
        redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

        case check_key_lock(state, redis_key, nil) do
          :ok -> do_delete(state, key)
          {:error, :key_locked} -> {:error, :key_locked}
        end
      end

      defp apply_single(state, {:delete_prefix, prefix}) do
        do_delete_prefix(state, prefix)
      end

      defp apply_single(state, {:list_op, key, operation}) do
        do_checked_list_op(state, key, operation)
      end

      # When a `:list_op_lmove` arrives on a replica wrapped as `{:async, origin, cmd}`,
      # the 3-tuple async clause unwraps and re-dispatches via apply_single. We need
      # to handle the inner shape here so followers re-execute and converge.
      defp apply_single(state, {:list_op_lmove, src_key, dst_key, from_dir, to_dir}) do
        do_checked_lmove(state, src_key, dst_key, from_dir, to_dir)
      end

      defp apply_single(state, {:compound_put, compound_key, value, expire_at_ms}) do
        redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(compound_key)

        case check_key_lock(state, redis_key, nil) do
          :ok -> do_compound_put(state, redis_key, compound_key, value, expire_at_ms)
          {:error, :key_locked} -> {:error, :key_locked}
        end
      end

      defp apply_single(state, {:compound_put_blob_ref, compound_key, encoded_ref, expire_at_ms}) do
        do_checked_compound_put_blob_ref(state, compound_key, encoded_ref, expire_at_ms)
      end

      defp apply_single(state, {:compound_batch_put, redis_key, entries}) do
        case apply_compound_batch_put_entries(state, redis_key, entries) do
          results when is_list(results) -> :ok
          {:error, _reason} = error -> error
        end
      end

      defp apply_single(state, {:hset_single, key, field, value}) do
        case check_key_lock(state, key, nil) do
          :ok ->
            apply_hset_single(state, key, field, value)

          {:error, :key_locked} ->
            {:error, :key_locked}
        end
      end

      defp apply_hset_single(state, key, field, value) do
        type_key = CompoundKey.type_key(key)
        field_key = CompoundKey.hash_field(key, field)

        case do_get(state, type_key) do
          nil ->
            apply_hset_single_new_hash(state, key, type_key, field_key, value)

          "hash" ->
            apply_hset_single_existing_hash(state, field_key, value)

          _other_type ->
            {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
        end
      end

      defp apply_hset_single_new_hash(state, key, type_key, field_key, value) do
        if live_key?(state, key) do
          {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
        else
          with :ok <- do_put(state, type_key, "hash", 0) do
            case do_put(state, field_key, value, 0) do
              :ok ->
                1

              {:error, _reason} = error ->
                rollback_hset_single_type_marker(state, type_key, error)
            end
          end
        end
      end

      defp apply_hset_single_existing_hash(state, field_key, value) do
        existed? = do_get(state, field_key) != nil

        case do_put(state, field_key, value, 0) do
          :ok -> if existed?, do: 0, else: 1
          {:error, _reason} = error -> error
        end
      end

      defp rollback_hset_single_type_marker(state, type_key, write_error) do
        case do_delete(state, type_key) do
          :ok ->
            write_error

          {:error, _reason} = rollback_error ->
            {:error, {:hash_type_marker_rollback_failed, write_error, rollback_error}}
        end
      end

      defp apply_single(state, {:lpush_single, key, value}) do
        case check_key_lock(state, key, nil) do
          :ok ->
            apply_list_push_single(state, key, value, :left)

          {:error, :key_locked} ->
            {:error, :key_locked}
        end
      end

      defp apply_single(state, {:rpush_single, key, value}) do
        case check_key_lock(state, key, nil) do
          :ok ->
            apply_list_push_single(state, key, value, :right)

          {:error, :key_locked} ->
            {:error, :key_locked}
        end
      end

      defp apply_list_push_single(state, key, value, direction) do
        type_key = CompoundKey.type_key(key)

        case do_get(state, type_key) do
          nil ->
            apply_list_push_single_new_type(state, key, type_key, value, direction)

          "list" ->
            apply_list_push_single_with_meta(
              state,
              key,
              value,
              direction,
              decode_list_push_meta(do_get(state, CompoundKey.list_meta_key(key))),
              false,
              type_key
            )

          _other_type ->
            {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
        end
      end

      defp apply_list_push_single_new_type(state, key, type_key, value, direction) do
        if live_key?(state, key) do
          {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
        else
          with :ok <- do_put(state, type_key, "list", 0) do
            apply_list_push_single_with_meta(state, key, value, direction, nil, true, type_key)
          end
        end
      end

      defp apply_list_push_single_with_meta(
             state,
             key,
             value,
             _direction,
             nil,
             type_created?,
             type_key
           ) do
        apply_list_push_single_write(
          state,
          key,
          value,
          0,
          {1, -1_000_000_000, 1_000_000_000},
          type_created?,
          type_key
        )
      end

      defp apply_list_push_single_with_meta(
             state,
             key,
             value,
             :left,
             {len, left_pos, right_pos},
             type_created?,
             type_key
           ) do
        apply_list_push_single_write(
          state,
          key,
          value,
          left_pos,
          {len + 1, left_pos - 1_000_000_000, right_pos},
          type_created?,
          type_key
        )
      end

      defp apply_list_push_single_with_meta(
             state,
             key,
             value,
             :right,
             {len, left_pos, right_pos},
             type_created?,
             type_key
           ) do
        apply_list_push_single_write(
          state,
          key,
          value,
          right_pos,
          {len + 1, left_pos, right_pos + 1_000_000_000},
          type_created?,
          type_key
        )
      end

      defp apply_list_push_single_write(
             state,
             key,
             value,
             pos,
             {_len, _left_pos, _right_pos} = meta,
             type_created?,
             type_key
           ) do
        element_key = CompoundKey.list_element(key, pos)
        meta_key = CompoundKey.list_meta_key(key)

        with :ok <- do_put(state, element_key, value, 0) do
          case do_put(state, meta_key, ListOps.encode_meta(meta), 0) do
            :ok ->
              elem(meta, 0)

            {:error, _reason} = error ->
              _ = do_delete(state, element_key)
              maybe_rollback_list_push_type_marker(state, type_created?, type_key, error)
          end
        else
          {:error, _reason} = error ->
            maybe_rollback_list_push_type_marker(state, type_created?, type_key, error)
        end
      end

      defp maybe_rollback_list_push_type_marker(state, true, type_key, write_error) do
        case do_delete(state, type_key) do
          :ok ->
            write_error

          {:error, _reason} = rollback_error ->
            {:error, {:list_type_marker_rollback_failed, write_error, rollback_error}}
        end
      end

      defp maybe_rollback_list_push_type_marker(_state, false, _type_key, write_error),
        do: write_error

      defp decode_list_push_meta(value), do: ListOps.decode_meta(value)

      defp apply_single(state, {:sadd_single, key, member}) do
        case check_key_lock(state, key, nil) do
          :ok ->
            Ferricstore.Commands.Set.handle_ast(
              {:sadd, [key, member]},
              build_compound_store(state)
            )

          {:error, :key_locked} ->
            {:error, :key_locked}
        end
      end

      defp apply_single(state, {:srem_single, key, member}) do
        case check_key_lock(state, key, nil) do
          :ok ->
            apply_srem_single(state, key, member)

          {:error, :key_locked} ->
            {:error, :key_locked}
        end
      end

      defp apply_single(state, {:zadd_single, key, score, member}) do
        case check_key_lock(state, key, nil) do
          :ok ->
            apply_zadd_single(state, key, score, member)

          {:error, :key_locked} ->
            {:error, :key_locked}
        end
      end

      defp apply_single(state, {:zrem_single, key, member}) do
        case check_key_lock(state, key, nil) do
          :ok ->
            apply_zrem_single(state, key, member)

          {:error, :key_locked} ->
            {:error, :key_locked}
        end
      end

      defp apply_srem_single(state, key, member) do
        type_key = CompoundKey.type_key(key)

        case pending_aware_get(state, type_key) do
          "set" ->
            member_key = CompoundKey.set_member(key, member)

            case pending_aware_get(state, member_key) do
              nil ->
                0

              _present ->
                with :ok <- do_delete(state, member_key),
                     :ok <- maybe_delete_empty_compound_type(state, key, type_key, :set) do
                  1
                end
            end

          nil ->
            if live_key?(state, key),
              do: {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"},
              else: 0

          _other_type ->
            {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
        end
      end

      defp apply_zadd_single(state, key, score, member) do
        type_key = CompoundKey.type_key(key)
        member_key = CompoundKey.zset_member(key, member)
        score_str = Float.to_string(score * 1.0)

        case do_get(state, type_key) do
          nil ->
            apply_zadd_single_new_zset(state, key, type_key, member_key, member, score, score_str)

          "zset" ->
            apply_zadd_single_existing_zset(state, key, member_key, score_str)

          _other_type ->
            {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
        end
      end

      defp apply_zadd_many_single_entries(state, entries) do
        Enum.map(entries, fn {key, score, member} ->
          case check_key_lock(state, key, nil) do
            :ok ->
              apply_zadd_single(state, key, score, member)

            {:error, :key_locked} ->
              {:error, :key_locked}
          end
        end)
      end

      defp apply_zadd_single_new_zset(
             state,
             key,
             type_key,
             member_key,
             member,
             score,
             score_str
           ) do
        if live_key?(state, key) do
          {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
        else
          with :ok <- do_put(state, type_key, "zset", 0),
               :ok <- do_put(state, member_key, score_str, 0) do
            queue_zset_index_new_put_after_flush(state, key, member, score)
            1
          end
        end
      end

      defp apply_zadd_single_existing_zset(state, key, member_key, score_str) do
        current_score = do_get(state, member_key)

        if current_score == score_str do
          0
        else
          case do_put(state, member_key, score_str, 0) do
            :ok ->
              queue_zset_index_put_after_flush(state, key, member_key, score_str)

              if current_score == nil, do: 1, else: 0

            {:error, _reason} = error ->
              error
          end
        end
      end

      defp apply_zrem_single(state, key, member) do
        type_key = CompoundKey.type_key(key)

        case pending_aware_get(state, type_key) do
          "zset" ->
            member_key = CompoundKey.zset_member(key, member)

            case pending_aware_get(state, member_key) do
              nil ->
                0

              _score ->
                with :ok <- do_delete(state, member_key),
                     :ok <- queue_zset_index_delete_after_flush_ok(state, key, member_key),
                     :ok <- maybe_delete_empty_compound_type(state, key, type_key, :zset) do
                  1
                end
            end

          nil ->
            if live_key?(state, key),
              do: {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"},
              else: 0

          _other_type ->
            {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
        end
      end

      defp pending_aware_get(state, key) do
        if pending_deleted?(key), do: nil, else: do_get(state, key)
      end

      defp pending_deleted?(key) do
        Process.get(:sm_pending_values, %{}) |> Map.get(key) == :deleted
      end

      defp queue_zset_index_delete_after_flush_ok(state, key, member_key) do
        _pending = queue_zset_index_delete_after_flush(state, key, member_key)
        :ok
      end

      defp maybe_delete_empty_compound_type(state, key, type_key, :set) do
        maybe_delete_empty_compound_type(state, type_key, CompoundKey.set_prefix(key))
      end

      defp maybe_delete_empty_compound_type(state, key, type_key, :zset) do
        maybe_delete_empty_compound_type(state, type_key, CompoundKey.zset_prefix(key))
      end

      defp maybe_delete_empty_compound_type(state, type_key, prefix) do
        case Ferricstore.Store.Shard.CompoundMemberIndex.any_live?(
               Map.get(state, :compound_member_index_name),
               shard_ets_state(state),
               prefix,
               Process.get(:sm_pending_values, %{})
             ) do
          false ->
            do_delete(state, type_key)

          true ->
            :ok

          :unavailable ->
            maybe_delete_empty_compound_type_fallback(state, type_key, prefix)
        end
      end

      defp maybe_delete_empty_compound_type_fallback(state, type_key, prefix) do
        if Ferricstore.Store.Shard.ETS.prefix_count_entries(shard_ets_state(state), prefix) == 0 do
          do_delete(state, type_key)
        else
          :ok
        end
      end

      defp apply_single(state, {:compound_blob_batch_put, redis_key, entries}) do
        case apply_compound_blob_batch_put_entries(state, redis_key, entries) do
          results when is_list(results) -> :ok
          {:error, _reason} = error -> error
        end
      end

      defp apply_single(state, {:compound_delete, compound_key}) do
        redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(compound_key)

        case check_key_lock(state, redis_key, nil) do
          :ok -> do_compound_delete(state, redis_key, compound_key)
          {:error, :key_locked} -> {:error, :key_locked}
        end
      end

      defp apply_single(state, {:compound_batch_delete, redis_key, compound_keys}) do
        case apply_compound_batch_delete_keys(state, redis_key, compound_keys) do
          results when is_list(results) -> :ok
          {:error, _reason} = error -> error
        end
      end

      defp apply_single(state, {:compound_delete_prefix, prefix}) do
        redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(prefix)

        case check_key_lock(state, redis_key, nil) do
          :ok -> do_compound_delete_prefix(state, redis_key, prefix)
          {:error, :key_locked} -> {:error, :key_locked}
        end
      end

      defp apply_single(state, {:pfadd, key, elements}) do
        HyperLogLog.handle_ast({:pfadd, [key | elements]}, build_string_value_store(state))
      end

      defp apply_single(state, {:pfmerge, dest_key, source_sketches}) do
        do_pfmerge(state, dest_key, source_sketches)
      end

      defp apply_single(state, {:pfmerge, dest_key, _source_keys, source_sketches}) do
        do_pfmerge(state, dest_key, source_sketches)
      end

      defp apply_single(state, {:incr, key, delta}) do
        do_incr(state, key, delta)
      end

      defp apply_single(state, {:incr_float, key, delta}) do
        do_incr_float(state, key, delta)
      end

      defp apply_single(state, {:append, key, suffix}) do
        do_append(state, key, suffix)
      end

      defp apply_single(state, {:append_blob_ref, key, encoded_ref}) do
        do_append_blob_ref(state, key, encoded_ref)
      end

      defp apply_single(state, {:getset, key, new_value}) do
        do_getset(state, key, new_value)
      end

      defp apply_single(state, {:getset_blob_ref, key, encoded_ref}) do
        do_getset_blob_ref(state, key, encoded_ref)
      end

      defp apply_single(state, {:getdel, key}) do
        do_getdel(state, key)
      end

      defp apply_single(state, {:getex, key, expire_at_ms}) do
        do_getex(state, key, expire_at_ms)
      end

      defp apply_single(state, {:setrange, key, offset, value}) do
        do_setrange(state, key, offset, value)
      end

      defp apply_single(state, {:setrange_blob_ref, key, offset, encoded_ref}) do
        do_setrange_blob_ref(state, key, offset, encoded_ref)
      end

      defp apply_single(state, {:setbit, key, offset, bit_val}) do
        do_setbit(state, key, offset, bit_val)
      end

      defp apply_single(state, {:hincrby, key, field, delta}) do
        do_hincrby(state, key, field, delta)
      end

      defp apply_single(state, {:hincrbyfloat, key, field, delta}) do
        do_hincrbyfloat(state, key, field, delta)
      end

      defp apply_single(state, {:zincrby, key, increment, member}) do
        do_zincrby(state, key, increment, member)
      end

      defp apply_single(state, {:spop, key, count}) do
        do_spop(state, key, count, 0)
      end

      defp apply_single(state, {:zpop, key, count, direction}) do
        do_zpop(state, key, count, direction)
      end

      defp apply_single(state, {:cas, key, expected, new_value, ttl_ms}) do
        do_cas(state, key, expected, new_value, ttl_ms)
      end

      defp apply_single(state, {:cas_blob_ref, key, expected, encoded_ref, ttl_ms}) do
        do_cas_blob_ref(state, key, expected, encoded_ref, ttl_ms)
      end

      defp apply_single(state, {:lock, key, owner, ttl_ms}) do
        do_lock(state, key, owner, ttl_ms)
      end

      defp apply_single(state, {:unlock, key, owner}) do
        do_unlock(state, key, owner)
      end

      defp apply_single(state, {:extend, key, owner, ttl_ms}) do
        do_extend(state, key, owner, ttl_ms)
      end

      defp apply_single(state, {:ratelimit_add, key, window_ms, max, count}) do
        do_ratelimit_add(state, key, window_ms, max, count, nil)
      end

      defp apply_single(state, {:ratelimit_add, key, window_ms, max, count, now_ms}) do
        do_ratelimit_add(state, key, window_ms, max, count, now_ms)
      end

      defp apply_single(state, {:tx_execute, queue, sandbox_namespace}) when is_list(queue) do
        do_tx_execute(state, queue, sandbox_namespace)
      end

      defp apply_single(state, {:locked_put, key, value, expire_at_ms, owner_ref}) do
        redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

        case check_key_lock(state, redis_key, owner_ref) do
          :ok -> do_put(state, key, value, expire_at_ms)
          {:error, _reason} = error -> error
        end
      end

      defp apply_single(state, {:locked_put_blob_ref, key, encoded_ref, expire_at_ms, owner_ref}) do
        do_locked_put_blob_ref(state, key, encoded_ref, expire_at_ms, owner_ref)
      end

      defp apply_single(state, {:flow_create, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_create, attrs, fn ->
          do_flow_create(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_create_many, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_create_many, attrs, fn ->
          do_flow_create_many(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_create_pipeline_batch, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_create_pipeline_batch, attrs, fn ->
          do_flow_create_pipeline_batch(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_start_and_claim_pipeline_batch, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_start_and_claim_pipeline_batch, attrs, fn ->
          do_flow_start_and_claim_pipeline_batch(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_run_steps_many, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_run_steps_many, attrs, fn ->
          do_flow_run_steps_many(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_named_value_put, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_named_value_put, attrs, fn ->
          do_flow_named_value_put(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_named_value_put_pipeline_batch, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_named_value_put_pipeline_batch, attrs, fn ->
          do_flow_named_value_put_pipeline_batch(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_signal, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_signal, attrs, fn ->
          do_flow_signal(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_signal_many, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_signal_many, attrs, fn ->
          do_flow_signal_many(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_spawn_children, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_spawn_children, attrs, fn ->
          do_flow_spawn_children(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_claim_due, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_claim_due, attrs, fn ->
          do_flow_claim_due(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_extend_lease, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_extend_lease, attrs, fn ->
          do_flow_extend_lease(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_complete, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_complete, attrs, fn ->
          do_flow_complete(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_complete_many, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_complete_many, attrs, fn ->
          do_flow_complete_many(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_terminal_pipeline_batch, op, _key, attrs})
           when op in [:complete, :retry, :fail, :cancel] do
        apply_flow_single_with_telemetry(state, :flow_terminal_pipeline_batch, attrs, fn ->
          do_flow_terminal_pipeline_batch(state, op, attrs)
        end)
      end

      defp apply_single(state, {:flow_transition, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_transition, attrs, fn ->
          do_flow_transition(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_start_and_claim, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_start_and_claim, attrs, fn ->
          do_flow_start_and_claim(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_step_continue, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_step_continue, attrs, fn ->
          do_flow_step_continue(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_step_continue_many, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_step_continue_many, attrs, fn ->
          do_flow_step_continue_many(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_transition_many, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_transition_many, attrs, fn ->
          do_flow_transition_many(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_retry, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_retry, attrs, fn ->
          do_flow_retry(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_retry_many, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_retry_many, attrs, fn ->
          do_flow_retry_many(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_fail, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_fail, attrs, fn ->
          do_flow_fail(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_fail_many, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_fail_many, attrs, fn ->
          do_flow_fail_many(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_cancel, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_cancel, attrs, fn ->
          do_flow_cancel(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_cancel_many, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_cancel_many, attrs, fn ->
          do_flow_cancel_many(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_retention_cleanup, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_retention_cleanup, attrs, fn ->
          do_flow_retention_cleanup(state, attrs)
        end)
      end

      defp apply_single(state, {:flow_rewind, _key, attrs}) do
        apply_flow_single_with_telemetry(state, :flow_rewind, attrs, fn ->
          do_flow_rewind(state, attrs)
        end)
      end

      # -- Probabilistic data structure commands in batch/cross_shard_tx --

      defp apply_single(state, {:bloom_create, key, num_bits, num_hashes, prob_meta}) do
        do_prob_command(state, fn ->
          create_bloom_metadata(state, key, num_bits, num_hashes, prob_meta)
        end)
      end

      defp apply_single(state, {:bloom_add, key, element, auto_create_params}) do
        do_prob_command(state, fn ->
          path = prob_path(state, key, "bloom")

          with :ok <- ensure_prob_dir(state) do
            case auto_create_bloom_if_needed(state, path, key, auto_create_params) do
              :ok -> NIF.bloom_file_add(path, element)
              {:error, _reason} = error -> error
            end
          end
        end)
      end

      defp apply_single(state, {:bloom_madd, key, elements, auto_create_params}) do
        do_prob_command(state, fn ->
          path = prob_path(state, key, "bloom")

          with :ok <- ensure_prob_dir(state) do
            case auto_create_bloom_if_needed(state, path, key, auto_create_params) do
              :ok -> NIF.bloom_file_madd(path, elements)
              {:error, _reason} = error -> error
            end
          end
        end)
      end

      defp apply_single(state, {:cms_create, key, width, depth}) do
        do_prob_command(state, fn ->
          create_cms_metadata(state, key, width, depth)
        end)
      end

      defp apply_single(state, {:cms_incrby, key, items}) do
        do_prob_command(state, fn ->
          path = prob_path(state, key, "cms")
          NIF.cms_file_incrby(path, items)
        end)
      end

      defp apply_single(state, {:cms_merge, dst_key, src_keys, weights, create_params}) do
        do_prob_command(state, fn ->
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

      defp apply_single(state, {:cuckoo_create, key, capacity, bucket_size}) do
        do_prob_command(state, fn ->
          create_cuckoo_metadata(state, key, capacity, bucket_size)
        end)
      end

      defp apply_single(state, {:cuckoo_add, key, element, auto_create_params}) do
        do_prob_command(state, fn ->
          path = prob_path(state, key, "cuckoo")

          with :ok <- ensure_prob_dir(state) do
            case auto_create_cuckoo_if_needed(state, path, key, auto_create_params) do
              :ok -> NIF.cuckoo_file_add(path, element)
              {:error, _reason} = error -> error
            end
          end
        end)
      end

      defp apply_single(state, {:cuckoo_addnx, key, element, auto_create_params}) do
        do_prob_command(state, fn ->
          path = prob_path(state, key, "cuckoo")

          with :ok <- ensure_prob_dir(state) do
            case auto_create_cuckoo_if_needed(state, path, key, auto_create_params) do
              :ok -> NIF.cuckoo_file_addnx(path, element)
              {:error, _reason} = error -> error
            end
          end
        end)
      end

      defp apply_single(state, {:cuckoo_del, key, element}) do
        do_prob_command(state, fn ->
          path = prob_path(state, key, "cuckoo")
          NIF.cuckoo_file_del(path, element)
        end)
      end

      defp apply_single(state, {:topk_create, key, k, width, depth, decay}) do
        do_prob_command(state, fn ->
          create_topk_metadata(state, key, k, width, depth, decay)
        end)
      end

      defp apply_single(state, {:topk_add, key, elements}) do
        do_prob_command(state, fn ->
          path = prob_path(state, key, "topk")
          NIF.topk_file_add_v2(path, elements)
        end)
      end

      defp apply_single(state, {:topk_incrby, key, pairs}) do
        do_prob_command(state, fn ->
          path = prob_path(state, key, "topk")
          NIF.topk_file_incrby_v2(path, pairs)
        end)
      end

      defp apply_single(_state, unknown_command) do
        require Logger
        Logger.error("StateMachine: unrecognized batch command: #{inspect(unknown_command)}")
        {:error, {:unknown_command, unknown_command}}
      end

      defp apply_put_batch_entries(state, entries) do
        case prepare_apply_blob_command(state, {:put_batch, entries}) do
          {:ok, {:put_blob_batch, prepared_entries}} ->
            apply_put_blob_batch_entries(state, prepared_entries)

          {:ok, {:put_batch, ^entries}} ->
            apply_plain_put_batch_entries(state, entries)

          {:ok, other} ->
            apply_single(state, other)

          {:error, _reason} = error ->
            error
        end
      end

      defp apply_plain_put_batch_entries(state, entries) do
        case Map.get(state, :cross_shard_locks, %{}) do
          locks when map_size(locks) == 0 ->
            if put_batch_fast_path?(state, entries) do
              apply_put_batch_entries_fast(state, entries)
            else
              Enum.map(entries, fn {key, value, expire_at_ms} ->
                do_put(state, key, value, expire_at_ms)
              end)
            end

          _locks ->
            Enum.map(entries, fn {key, value, expire_at_ms} ->
              redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

              case check_key_lock(state, redis_key, nil) do
                :ok -> do_put(state, key, value, expire_at_ms)
                {:error, :key_locked} -> {:error, :key_locked}
              end
            end)
        end
      end

      defp apply_put_blob_batch_entries(state, entries) do
        with {:ok, prepared_entries} <- prepare_put_blob_batch_entries(state, entries) do
          Enum.map(prepared_entries, fn
            {:value, key, value, expire_at_ms} ->
              do_put(state, key, value, expire_at_ms)

            {:blob_ref, key, encoded_ref, expire_at_ms, _ref} ->
              redis_key = CompoundKey.extract_redis_key(key)

              case check_key_lock(state, redis_key, nil) do
                :ok -> do_put_blob_ref_ref_only_validated(state, key, encoded_ref, expire_at_ms)
                {:error, :key_locked} -> {:error, :key_locked}
              end
          end)
        end
      end

      defp prepare_put_blob_batch_entries(state, entries) do
        with {:ok, prepared, refs} <- decode_put_blob_batch_entries(entries),
             :ok <- verify_blob_refs_for_apply(state, refs) do
          {:ok, prepared}
        end
      end

      defp decode_put_blob_batch_entries(entries) do
        entries
        |> Enum.reduce_while({:ok, [], []}, fn
          {key, value, expire_at_ms, :value}, {:ok, acc, refs}
          when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) ->
            {:cont, {:ok, [{:value, key, value, expire_at_ms} | acc], refs}}

          {key, encoded_ref, expire_at_ms, :blob_ref}, {:ok, acc, refs}
          when is_binary(key) and is_binary(encoded_ref) and is_integer(expire_at_ms) ->
            case BlobRef.decode(encoded_ref) do
              {:ok, ref} ->
                entry = {:blob_ref, key, encoded_ref, expire_at_ms, ref}
                {:cont, {:ok, [entry | acc], [ref | refs]}}

              :error ->
                {:halt, {:error, {:blob_ref_unavailable, :invalid_blob_ref}}}
            end

          _entry, {:ok, _acc, _refs} ->
            {:halt, {:error, :invalid_put_blob_batch_entry}}
        end)
        |> case do
          {:ok, prepared, refs} -> {:ok, Enum.reverse(prepared), Enum.reverse(refs)}
          {:error, _reason} = error -> error
        end
      end

      defp verify_blob_refs_for_apply(_state, []), do: :ok

      defp verify_blob_refs_for_apply(state, refs) do
        case BlobStore.verify_many(state.data_dir, state.shard_index, refs) do
          :ok -> :ok
          {:error, reason} -> {:error, {:blob_ref_unavailable, reason}}
        end
      end

      defp do_put_blob_ref_ref_only_validated(state, key, encoded_ref, expire_at_ms) do
        maybe_clear_compound_data_structure_for_string_put(state, key)
        raw_put_blob_ref_ref_only(state, key, encoded_ref, expire_at_ms)
      end

      defp put_batch_fast_path?(state, entries) do
        not cross_shard_pending_active?() and
          not standalone_staged_apply?() and
          Enum.all?(entries, fn {key, _value, _expire_at_ms} ->
            put_batch_plain_string_key?(state, key)
          end)
      end

      defp put_batch_plain_string_key?(state, key) do
        if CompoundKey.internal_key?(key) do
          false
        else
          case :ets.lookup(state.ets, CompoundKey.type_key(key)) do
            [] -> true
            _marker -> false
          end
        end
      end

      # Specialized Ra term contract:
      #
      # `{:put_batch, entries}` is homogeneous and write-only. It does not publish
      # temporary ETS `:pending` rows or fill the generic pending-value map because
      # no later command inside the same Ra entry can read the staged values. The
      # append batch is recorded in `:sm_pending_writes`, and
      # `apply_fast_put_pending_locations/5` publishes the final ETS rows only after
      # the NIF returns ordered append locations.
      #
      # If a future compact term needs read-your-own-write inside the same Ra entry,
      # use the generic `{:batch, commands}` machinery or add a dedicated equivalent
      # with rollback, ordering, and mixed-result tests.
      defp apply_put_batch_entries_fast(_state, entries) do
        pending = Process.get(:sm_pending_writes, [])
        pending_values = Process.get(:sm_pending_values, %{})
        fast_publish? = fast_put_publish_possible?(pending, pending_values)

        {results, pending} =
          Enum.reduce(entries, {[], pending}, fn
            {key, value, expire_at_ms}, {results, pending_acc} ->
              disk_val = to_disk_binary(value)

              if FlowKeys.policy_key?(key) do
                queue_pending_lmdb_mirror_put(key, disk_val, expire_at_ms)
              end

              {
                [:ok | results],
                [{:put, key, disk_val, expire_at_ms} | pending_acc]
              }
          end)

        Process.put(:sm_pending_writes, pending)
        Process.put(:sm_pending_fast_put_batch, fast_publish?)

        Enum.reverse(results)
      end

      defp apply_delete_batch_keys_fast(state, keys) do
        with true <- delete_batch_fast_path?(state),
             {:ok, prepared} <- maybe_prepare_delete_batch_fast(state, keys),
             true <- Process.get(:sm_pending_writes, []) == [],
             true <- Process.get(:sm_pending_values, %{}) == %{} do
          Enum.each(Enum.reverse(prepared), fn {key, prob_path} ->
            queue_pending_delete_fast(key, prob_path)
          end)

          Process.put(:sm_pending_fast_delete_batch, true)

          Enum.map(keys, fn _key -> :ok end)
        else
          _ -> :fallback
        end
      end

      defp fast_put_publish_possible?(pending, pending_values) do
        pending_values == %{} and
          (pending == [] or
             (Process.get(:sm_pending_fast_put_batch) == true and put_only_pending_batch?(pending)))
      end

      defp delete_batch_fast_path?(state) do
        not cross_shard_pending_active?() and
          not standalone_staged_apply?() and Map.get(state, :cross_shard_locks, %{}) == %{}
      end
    end
  end
end
