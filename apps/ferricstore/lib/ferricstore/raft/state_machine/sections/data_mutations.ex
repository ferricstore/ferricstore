defmodule Ferricstore.Raft.StateMachine.Sections.DataMutations do
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
      alias Ferricstore.FetchOrCompute.Outcome, as: FetchOrComputeOutcome
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
        RateLimit,
        Router,
        ValueCodec
      }

      alias Ferricstore.Store.Shard.{CompoundMemberIndex, ZSetIndex}
      alias Ferricstore.Store.Shard.Transaction, as: ShardTransaction
      alias Ferricstore.Store.Shard.Flush, as: ShardFlush
      alias Ferricstore.Transaction.Ast, as: TxAst

      defp do_getset(state, key, new_value) do
        with :ok <- ensure_string_key(state, key) do
          old = do_get(state, key)
          do_put(state, key, new_value, 0)
          old
        end
      end

      # Atomic GETDEL: reads value, deletes key, returns value directly (not
      # wrapped in {:ok, ...}). Returns nil if key does not exist.
      defp do_getdel(state, key) do
        with :ok <- ensure_string_key(state, key) do
          old = do_get(state, key)

          if old != nil do
            case do_delete(state, key) do
              :ok -> old
              {:error, _reason} = error -> error
            end
          else
            old
          end
        end
      end

      # Atomic GETEX: reads value, re-writes with new expire_at_ms, returns value
      # directly (not wrapped). Returns nil if key does not exist or is expired.
      defp do_getex(state, key, expire_at_ms) do
        with :ok <- ensure_string_key(state, key) do
          case do_get_meta(state, key) do
            nil ->
              nil

            {value, _old_exp} ->
              do_put(state, key, value, expire_at_ms)
              value
          end
        end
      end

      # Atomic SETRANGE: reads current value, pads with zero bytes if needed,
      # replaces bytes at offset, writes back. Preserves expire_at_ms.
      # A bad offset is an error, not a raise: a raise here crashes storage,
      # and again on every replay of the entry.
      defp do_setrange(state, key, offset, value) do
        with :ok <- ensure_string_key(state, key) do
          {old_val, expire_at_ms} =
            case do_get_meta(state, key) do
              nil -> {"", 0}
              {v, exp} -> {to_disk_binary(v), exp}
            end

          with :ok <- check_setrange_offset(offset, value, byte_size(old_val)),
               target_size =
                 Ferricstore.Raft.ApplyLimits.setrange_size(
                   byte_size(old_val),
                   offset,
                   byte_size(value)
                 ),
               :ok <- Ferricstore.Raft.ApplyLimits.validate_value_size(state, target_size),
               new_val = sm_apply_setrange(old_val, offset, value),
               :ok <- do_put(state, key, new_val, expire_at_ms) do
            {:ok, target_size}
          end
        end
      end

      # Rejects exactly the offsets the apply raised on. An empty value at a
      # numeric offset up to the string's end left it unchanged, and an older
      # release logged such entries; replaying one must agree with it.
      defp check_setrange_offset(offset, _value, _old_len)
           when is_integer(offset) and offset >= 0,
           do: :ok

      defp check_setrange_offset(offset, "", old_len)
           when is_number(offset) and offset <= old_len,
           do: :ok

      defp check_setrange_offset(_offset, _value, _old_len),
        do: {:error, "ERR offset is out of range"}

      # Atomic SETBIT: read bitmap, extend with zeros to include byte_index if
      # needed, flip the single bit, write back. Preserves expire_at_ms.
      # Returns the OLD bit at that offset (Redis semantics).
      defp do_setbit(state, key, offset, bit_val) do
        with :ok <- check_setbit_args(offset, bit_val),
             :ok <- ensure_string_key(state, key) do
          {old_val, expire_at_ms} =
            case do_get_meta(state, key) do
              nil -> {<<>>, 0}
              {v, exp} -> {to_disk_binary(v), exp}
            end

          byte_index = div(offset, 8)
          bit_position = 7 - rem(offset, 8)
          target_size = Ferricstore.Raft.ApplyLimits.setbit_size(byte_size(old_val), offset)

          with :ok <- Ferricstore.Raft.ApplyLimits.validate_value_size(state, target_size) do
            extended =
              if byte_size(old_val) >= byte_index + 1 do
                old_val
              else
                old_val <> :binary.copy(<<0>>, byte_index + 1 - byte_size(old_val))
              end

            old_byte = :binary.at(extended, byte_index)
            old_bit = old_byte >>> bit_position &&& 1

            new_byte =
              case bit_val do
                1 -> old_byte ||| 1 <<< bit_position
                0 -> old_byte &&& bnot(1 <<< bit_position)
              end

            <<prefix::binary-size(^byte_index), _old::8, suffix::binary>> = extended
            new_value = <<prefix::binary, new_byte::8, suffix::binary>>

            case do_put(state, key, new_value, expire_at_ms) do
              :ok -> old_bit
              {:error, _reason} = error -> error
            end
          end
        end
      end

      # A field, member or HINCRBY delta of the wrong type raised while the
      # entry was applied, crash-looping the shard; the router rejects them,
      # and these clauses keep an entry that reached the log from raising.
      defp do_hincrby(_state, _redis_key, field, _delta) when not is_binary(field),
        do: {:error, "ERR hash field must be a string"}

      defp do_hincrby(_state, _redis_key, _field, delta) when not is_integer(delta),
        do: {:error, "ERR value is not an integer or out of range"}

      # Atomic HINCRBY: read compound key (H:<redis_key>\0<field>), parse integer,
      # add delta, write back. Returns new integer value, or {:error, reason}.
      defp do_hincrby(state, redis_key, field, delta) do
        compound_key = Ferricstore.Store.CompoundKey.hash_field(redis_key, field)

        with :ok <- check_member_write_type(state, redis_key, [compound_key]) do
          do_checked_hincrby(state, redis_key, compound_key, delta)
        end
      end

      defp do_checked_hincrby(state, redis_key, compound_key, delta) do
        case sm_store_compound_get_meta(state, redis_key, compound_key) do
          {:error, _reason} = error ->
            error

          nil ->
            if delta > @int64_max or delta < @int64_min do
              {:error, "ERR increment or decrement would overflow"}
            else
              expire_at_ms = new_member_expire_at(state, redis_key)

              case do_compound_put(
                     state,
                     redis_key,
                     compound_key,
                     Integer.to_string(delta),
                     expire_at_ms
                   ) do
                :ok -> delta
                {:error, _reason} = error -> error
              end
            end

          {value, expire_at_ms} ->
            case coerce_incr_integer(state, value) do
              {:ok, cur} ->
                new_val = cur + delta

                if new_val > @int64_max or new_val < @int64_min do
                  {:error, "ERR increment or decrement would overflow"}
                else
                  case do_compound_put(
                         state,
                         redis_key,
                         compound_key,
                         Integer.to_string(new_val),
                         expire_at_ms
                       ) do
                    :ok -> new_val
                    {:error, _reason} = error -> error
                  end
                end

              :error ->
                {:error, "ERR hash value is not an integer"}
            end
        end
      end

      defp do_hincrbyfloat(_state, _redis_key, field, _delta) when not is_binary(field),
        do: {:error, "ERR hash field must be a string"}

      # Atomic HINCRBYFLOAT: same as HINCRBY but for floats.
      defp do_hincrbyfloat(state, redis_key, field, delta) do
        compound_key = Ferricstore.Store.CompoundKey.hash_field(redis_key, field)

        with :ok <- check_member_write_type(state, redis_key, [compound_key]) do
          do_checked_hincrbyfloat(state, redis_key, compound_key, delta)
        end
      end

      defp do_checked_hincrbyfloat(state, redis_key, compound_key, delta) do
        case sm_store_compound_get_meta(state, redis_key, compound_key) do
          {:error, _reason} = error ->
            error

          nil ->
            expire_at_ms = new_member_expire_at(state, redis_key)
            do_hincrbyfloat_put(state, redis_key, compound_key, 0.0, delta, expire_at_ms)

          {value, expire_at_ms} ->
            case coerce_float(value) do
              {:ok, cur} ->
                do_hincrbyfloat_put(
                  state,
                  redis_key,
                  compound_key,
                  cur,
                  delta,
                  expire_at_ms
                )

              :error ->
                {:error, "ERR hash value is not a float"}
            end
        end
      end

      defp do_hincrbyfloat_put(
             state,
             redis_key,
             compound_key,
             current,
             delta,
             expire_at_ms
           ) do
        case ValueCodec.checked_float_add(current, delta) do
          {:ok, new_val} ->
            new_str =
              if feature_level_4?(state),
                do: ValueCodec.format_float_plain(new_val),
                else: Float.to_string(new_val)

            case do_compound_put(state, redis_key, compound_key, new_str, expire_at_ms) do
              :ok -> new_str
              {:error, _reason} = error -> error
            end

          :overflow ->
            {:error, "ERR increment would produce NaN or Infinity"}

          :error ->
            {:error, "ERR hash value is not a float"}
        end
      end

      # Atomic ZINCRBY: check/set type metadata, read member score, add delta,
      # write back. Returns the new score as a string. Returns {:error, ...} on
      # wrong type.
      defp do_zincrby(_state, _redis_key, _increment, member) when not is_binary(member),
        do: {:error, "ERR zincrby member must be a string"}

      defp do_zincrby(state, redis_key, increment, member) do
        type_key = Ferricstore.Store.CompoundKey.type_key(redis_key)
        expected_type = Ferricstore.Store.CompoundKey.encode_type(:zset)

        with :ok <- check_or_set_type(state, redis_key, type_key, expected_type) do
          compound_key = Ferricstore.Store.CompoundKey.zset_member(redis_key, member)

          case sm_store_compound_get_meta(state, redis_key, compound_key) do
            {:error, _reason} = error ->
              error

            current ->
              case current_zset_score(current) do
                {:ok, current_score} ->
                  case ValueCodec.checked_float_add(current_score, increment) do
                    {:ok, new_score} ->
                      new_str = Float.to_string(new_score)
                      expire_at_ms = zincrby_expire_at(state, redis_key, current)

                      case do_compound_put(state, redis_key, compound_key, new_str, expire_at_ms) do
                        :ok -> new_str
                        {:error, _reason} = error -> error
                      end

                    :overflow ->
                      {:error, "ERR resulting score is not a number (NaN)"}

                    :error ->
                      {:error, "ERR value is not a valid float"}
                  end

                :error ->
                  {:error, "ERR value is not a valid float"}
              end
          end
        end
      end

      # From feature level 6 an incremented member keeps its TTL (the key's) and a
      # new one takes the key's; older releases write none.
      defp zincrby_expire_at(state, redis_key, nil), do: new_member_expire_at(state, redis_key)

      defp zincrby_expire_at(state, _redis_key, {_score, expire_at_ms}) do
        if feature_level_6?(state), do: expire_at_ms, else: 0
      end

      defp current_zset_score(nil), do: {:ok, 0.0}

      defp current_zset_score({score_value, _expire_at_ms}) do
        score_string =
          case score_value do
            value when is_binary(value) -> value
            value -> to_string(value)
          end

        ValueCodec.parse_float(score_string)
      end

      defp do_spop(_state, _redis_key, count, _seed)
           when not (is_nil(count) or is_integer(count)),
           do: {:error, "ERR value is not an integer or out of range"}

      defp do_spop(_state, _redis_key, count, _seed) when is_integer(count) and count < 0,
        do: {:error, "ERR value is not an integer or out of range"}

      defp do_spop(state, redis_key, count, seed) do
        store = build_compound_store(state)

        with :ok <- Ferricstore.Store.TypeRegistry.check_type(redis_key, :set, store) do
          pop_count = if is_nil(count), do: 1, else: count

          if pop_count == 0 do
            []
          else
            prefix = CompoundKey.set_prefix(redis_key)
            cursor = deterministic_member_cursor({redis_key, seed})

            case CompoundMemberIndex.member_slice(
                   Map.get(state, :compound_member_index_name),
                   shard_ets_state(state),
                   prefix,
                   cursor,
                   pop_count,
                   ExpiryContext.capture(),
                   Process.get(:sm_pending_values, %{})
                 ) do
              {:ok, selected} ->
                selected_delete_keys =
                  Enum.map(selected, &CompoundKey.set_member(redis_key, &1))

                type_key = CompoundKey.type_key(redis_key)

                with :ok <- do_compound_batch_delete(state, redis_key, selected_delete_keys),
                     :ok <-
                       maybe_delete_empty_compound_type(
                         state,
                         redis_key,
                         type_key,
                         :set,
                         selected_delete_keys
                       ) do
                  if is_nil(count), do: List.first(selected), else: selected
                end

              :unavailable ->
                pop_state_read_failure(:compound_member_index_unavailable)

              {:error, reason} ->
                pop_state_read_failure(reason)
            end
          end
        end
      end

      defp do_zpop(_state, _redis_key, count, _direction)
           when not is_integer(count) or count < 0,
           do: {:error, "ERR value is not an integer or out of range"}

      defp do_zpop(state, redis_key, count, direction) when direction in [:min, :max] do
        store = build_compound_store(state)

        with :ok <- Ferricstore.Store.TypeRegistry.check_type(redis_key, :zset, store) do
          if count == 0 do
            []
          else
            case zset_tree_path(state, redis_key) do
              nil -> do_zpop_keydir(state, redis_key, count, direction)
              path -> do_zpop_tree(state, redis_key, count, direction, path)
            end
          end
        end
      end

      defp do_zpop_tree(state, redis_key, count, direction, path) do
        op = if direction == :max, do: {:pop_max, count}, else: {:pop_min, count}
        type_key = CompoundKey.type_key(redis_key)

        with {:ok, [popped]} <- zset_tree_write(state, path, [op]),
             :ok <- maybe_delete_empty_compound_type(state, redis_key, type_key, :zset, []) do
          Enum.flat_map(popped, fn {member, score} -> [member, format_zset_score(score)] end)
        end
      end

      defp do_zpop_keydir(state, redis_key, count, direction) do
        prefix = CompoundKey.zset_prefix(redis_key)

        with :ok <- ensure_zpop_score_index(state, redis_key, prefix) do
          selected =
            ZSetIndex.rank_range(
              state.zset_score_index_name,
              redis_key,
              0,
              count - 1,
              direction == :max
            )

          selected_delete_keys =
            Enum.map(selected, fn {member, _score} ->
              CompoundKey.zset_member(redis_key, member)
            end)

          type_key = CompoundKey.type_key(redis_key)

          with :ok <- do_compound_batch_delete(state, redis_key, selected_delete_keys),
               :ok <-
                 maybe_delete_empty_compound_type(
                   state,
                   redis_key,
                   type_key,
                   :zset,
                   selected_delete_keys
                 ) do
            Enum.flat_map(selected, fn {member, score} ->
              [member, format_zset_score(score)]
            end)
          end
        end
      end

      defp do_zpop(_state, _redis_key, _count, _direction),
        do: {:error, "ERR syntax error"}

      defp deterministic_member_cursor(seed) do
        :crypto.hash(:sha256, :erlang.term_to_binary(seed, [:deterministic]))
      end

      defp ensure_zpop_score_index(state, redis_key, prefix) do
        cond do
          not zset_index_tables?(state) ->
            pop_state_read_failure(:zset_index_unavailable)

          ZSetIndex.ready?(state.zset_score_lookup_name, redis_key) ->
            :ok

          true ->
            instance_ctx = cross_shard_instance_ctx(state)
            ctx = cross_shard_ctx(state, state.shard_index, state.data_dir, instance_ctx)
            entries = cross_shard_compound_scan(ctx, redis_key, prefix)

            case Process.get(:sm_state_read_failure) do
              nil ->
                case ZSetIndex.rebuild_key(
                       state.zset_score_index_name,
                       state.zset_score_lookup_name,
                       redis_key,
                       entries
                     ) do
                  :ok -> :ok
                  {:error, reason} -> pop_state_read_failure(reason)
                end

              reason ->
                {:error, {:state_read_failed, reason}}
            end
        end
      end

      defp pop_state_read_failure(reason) do
        record_state_read_failure(reason)
        {:error, {:state_read_failed, reason}}
      end

      # The reply only: popped members leave nothing stored.
      defp format_zset_score(score) when is_float(score),
        do: Ferricstore.Store.ValueCodec.format_double(score)

      defp sm_store_compound_get_meta(state, redis_key, compound_key) do
        case zset_tree_route(state, redis_key, [compound_key]) do
          {:tree, path, [_member], []} ->
            {:ok, member} = Ferricstore.Store.ZSetTree.member(redis_key, compound_key)

            case Ferricstore.Store.ZSetTree.score(path, member) do
              {:ok, nil} -> nil
              {:ok, score} -> {score, 0}
              {:error, reason} -> {:error, {:zset_tree_read_failed, reason}}
            end

          :keydir ->
            sm_store_compound_get_meta_keydir(state, redis_key, compound_key)
        end
      end

      defp sm_store_compound_get_meta_keydir(state, redis_key, compound_key) do
        result =
          case sm_store_compound_path_fun(state, redis_key, compound_key) do
            nil ->
              do_get_meta(state, compound_key)

            path_fun ->
              case sm_store_batch_get(state, [compound_key], path_fun) do
                [value] when is_binary(value) ->
                  case :ets.lookup(state.ets, compound_key) do
                    [{^compound_key, _ets_value, exp, _lfu, _fid, _off, _vsize}] ->
                      {value, exp}

                    _ ->
                      nil
                  end

                _ ->
                  nil
              end
          end

        case Process.get(:sm_state_read_failure) do
          reason when reason not in [nil, :outside_apply] ->
            {:error, {:state_read_failed, reason}}

          _no_failure ->
            result
        end
      end

      defp sm_store_compound_path_fun(state, redis_key, compound_key_or_prefix) do
        case promoted_compound_path(state, redis_key, compound_key_or_prefix) do
          nil -> nil
          dedicated_path -> fn _state, fid -> sm_file_path_from_path(dedicated_path, fid) end
        end
      end

      defp sm_store_batch_get(state, keys, path_fun) do
        expiry_context = ExpiryContext.capture()

        sm_store_batch_get(
          state,
          keys,
          path_fun,
          expiry_context,
          @cold_location_retry_attempts
        )
      end

      defp sm_store_batch_get(state, keys, path_fun, expiry_context, attempts_left) do
        {local_results, cold_reads, remote_entries} =
          keys
          |> Enum.with_index()
          |> Enum.reduce({%{}, [], []}, fn {key, index}, {results, cold, remote} ->
            sm_store_collect_batch_get(
              state,
              key,
              index,
              expiry_context,
              results,
              cold,
              remote
            )
          end)

        results =
          sm_store_read_cold_batch(
            state,
            local_results,
            Enum.reverse(cold_reads),
            path_fun,
            expiry_context,
            attempts_left
          )

        remote_entries
        |> Enum.reverse()
        |> sm_store_batch_remote_get(instance_ctx_for_state(state), results)
        |> values_for_indexes(keys)
      end

      defp sm_store_collect_batch_get(
             state,
             key,
             index,
             expiry_context,
             results,
             cold,
             remote
           ) do
        case sm_pending_value_meta(key) do
          {:hit, value, _exp} ->
            {Map.put(results, index, value), cold, remote}

          :miss ->
            sm_store_collect_committed_batch_get(
              state,
              key,
              index,
              expiry_context,
              results,
              cold,
              remote
            )
        end
      end

      defp sm_store_collect_committed_batch_get(
             state,
             key,
             index,
             expiry_context,
             results,
             cold,
             remote
           ) do
        case :ets.lookup(state.ets, key) do
          [{^key, value, exp, _lfu, fid, off, vsize} = entry]
          when is_integer(exp) and exp >= 0 ->
            case ExpiryContext.classify(expiry_context, exp) do
              :live when value != nil ->
                {Map.put(results, index, value), cold, remote}

              :live
              when valid_cold_location(fid, off, vsize) or
                     valid_waraft_segment_location(fid, off, vsize) ->
                {results, [{index, entry} | cold], remote}

              :live ->
                record_state_read_failure({:invalid_cold_location, {fid, off, vsize}})
                {results, cold, remote}

              :expired ->
                delete_expired_committed_entry(state, entry)
                {results, cold, [{index, key} | remote]}

              {:unsafe, reason} ->
                record_state_read_failure(reason)
                {results, cold, remote}
            end

          [] ->
            {results, cold, [{index, key} | remote]}

          [entry] ->
            record_state_read_failure({:invalid_keydir_entry, key, entry})
            {results, cold, remote}
        end
      rescue
        ArgumentError ->
          record_state_read_failure(:keydir_unavailable)
          {results, cold, remote}
      end

      defp sm_store_read_cold_batch(
             _state,
             results,
             [],
             _path_fun,
             _expiry_context,
             _attempts_left
           ),
           do: results

      defp sm_store_read_cold_batch(
             state,
             results,
             cold_reads,
             path_fun,
             expiry_context,
             attempts_left
           ) do
        {segment_reads, file_reads} =
          Enum.split_with(cold_reads, fn {_index, {_key, _value, _exp, _lfu, fid, off, _vsize}} ->
            valid_waraft_segment_location(fid, off, 0)
          end)

        results =
          sm_store_read_bitcask_cold_batch(
            state,
            results,
            file_reads,
            path_fun,
            expiry_context,
            attempts_left
          )

        sm_store_read_waraft_segment_batch(
          state,
          results,
          segment_reads,
          path_fun,
          expiry_context,
          attempts_left
        )
      end

      defp sm_store_read_bitcask_cold_batch(
             _state,
             results,
             [],
             _path_fun,
             _expiry_context,
             _attempts_left
           ),
           do: results

      defp sm_store_read_bitcask_cold_batch(
             state,
             results,
             cold_reads,
             path_fun,
             expiry_context,
             attempts_left
           ) do
        reads =
          Enum.map(cold_reads, fn {_index, {key, nil, _exp, _lfu, fid, off, _vsize} = entry} ->
            {path_fun.(state, fid), off, key, entry}
          end)

        {:ok, current_results} =
          Ferricstore.Store.ColdRead.pread_batch_keyed_current(
            reads,
            fn key, entry ->
              sm_store_resolve_current_bitcask(
                state,
                path_fun,
                key,
                entry,
                expiry_context
              )
            end,
            @cold_read_timeout_ms
          )

        values = Enum.map(current_results, &sm_store_current_read_value/1)

        emit_state_machine_batch_cold_errors(cold_reads, values, fn
          {_index, {_key, _value, _exp, _lfu, fid, _off, _vsize}} ->
            path_fun.(state, fid)
        end)

        materialized_values = materialize_state_machine_batch_values(state, values)

        [cold_reads, current_results, materialized_values]
        |> Enum.zip()
        |> Enum.reduce(results, fn
          {{index, _observed_entry}, {:value, value, current_entry}, materialized}, acc
          when is_binary(value) and is_binary(materialized) ->
            maybe_run_cold_read_success_hook(state, elem(current_entry, 0))

            case sm_store_accept_current_batch_value(
                   state,
                   current_entry,
                   value,
                   materialized,
                   path_fun,
                   expiry_context,
                   attempts_left
                 ) do
              {:ok, current} -> Map.put(acc, index, current)
              :missing -> acc
            end

          {{_index, _observed_entry}, {:error, reason}, _materialized}, acc ->
            record_state_read_failure({:cold_value_unavailable, reason})
            acc

          {{index, _observed_entry}, {:value, _value, current_entry}, {:error, reason}}, acc ->
            case sm_store_retry_failed_current_batch_value(
                   state,
                   current_entry,
                   path_fun,
                   expiry_context,
                   attempts_left,
                   reason
                 ) do
              {:ok, current} -> Map.put(acc, index, current)
              :missing -> acc
            end

          {_read, _current_result, _materialized}, acc ->
            acc
        end)
      end

      defp sm_store_current_read_value({:value, value, _entry}), do: value
      defp sm_store_current_read_value({:error, reason}), do: {:error, reason}
      defp sm_store_current_read_value(:missing), do: nil

      defp sm_store_resolve_current_bitcask(
             state,
             path_fun,
             key,
             _observed_entry,
             expiry_context
           ) do
        case :ets.lookup(state.ets, key) do
          [{^key, value, exp, _lfu, fid, off, vsize} = entry]
          when is_integer(exp) and exp >= 0 ->
            case ExpiryContext.classify(expiry_context, exp) do
              :live when is_binary(value) ->
                {:hot, value, entry}

              :live when valid_cold_location(fid, off, vsize) ->
                {:cold, path_fun.(state, fid), off, entry}

              :live ->
                :missing

              :expired ->
                :missing

              {:unsafe, reason} ->
                record_state_read_failure(reason)
                :missing
            end

          _ ->
            :missing
        end
      rescue
        ArgumentError -> :missing
      end

      defp sm_store_accept_current_batch_value(
             state,
             {key, nil, _exp, _lfu, _fid, _off, _vsize} = current_entry,
             encoded_value,
             encoded_value,
             path_fun,
             expiry_context,
             attempts_left
           ) do
        ets_value = value_for_ets(encoded_value, hot_cache_threshold(state))
        replacement = put_elem(current_entry, 1, ets_value)

        if Keydir.replace_exact(state.ets, current_entry, replacement) do
          track_keydir_binary_warm(state, ets_value)
          {:ok, encoded_value}
        else
          sm_store_retry_changed_batch_value(
            state,
            key,
            path_fun,
            expiry_context,
            attempts_left - 1
          )
        end
      end

      defp sm_store_accept_current_batch_value(
             state,
             {key, _value, _exp, _lfu, _fid, _off, _vsize} = current_entry,
             _encoded_value,
             materialized,
             path_fun,
             expiry_context,
             attempts_left
           ) do
        if Keydir.replace_exact(state.ets, current_entry, current_entry) do
          {:ok, materialized}
        else
          sm_store_retry_changed_batch_value(
            state,
            key,
            path_fun,
            expiry_context,
            attempts_left - 1
          )
        end
      end

      defp sm_store_retry_failed_current_batch_value(
             state,
             {key, _value, _exp, _lfu, _fid, _off, _vsize} = current_entry,
             path_fun,
             expiry_context,
             attempts_left,
             reason
           ) do
        if Keydir.replace_exact(state.ets, current_entry, current_entry) do
          record_state_read_failure(reason)
          :missing
        else
          sm_store_retry_changed_batch_value(
            state,
            key,
            path_fun,
            expiry_context,
            attempts_left - 1
          )
        end
      end

      defp sm_store_retry_changed_batch_value(
             _state,
             _key,
             _path_fun,
             _expiry_context,
             attempts_left
           )
           when attempts_left <= 0 do
        record_state_read_failure({:cold_value_unavailable, :cold_location_changed_after_read})

        :missing
      end

      defp sm_store_retry_changed_batch_value(
             state,
             key,
             path_fun,
             expiry_context,
             attempts_left
           ) do
        Process.sleep(@cold_location_retry_sleep_ms)

        case sm_store_batch_get(
               state,
               [key],
               path_fun,
               expiry_context,
               attempts_left
             ) do
          [value] when is_binary(value) -> {:ok, value}
          _missing_or_failed -> :missing
        end
      end

      defp sm_store_read_waraft_segment_batch(
             _state,
             results,
             [],
             _path_fun,
             _expiry_context,
             _attempts_left
           ),
           do: results

      defp sm_store_read_waraft_segment_batch(
             state,
             results,
             segment_reads,
             path_fun,
             expiry_context,
             attempts_left
           ) do
        ctx = instance_ctx_for_state(state)

        Enum.reduce(segment_reads, results, fn
          {index, {key, nil, _exp, _lfu, fid, _off, _vsize} = entry}, acc ->
            case Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
                   ctx,
                   state.shard_index,
                   fid,
                   key
                 ) do
              {:ok, value} when is_binary(value) ->
                maybe_run_cold_read_success_hook(state, key)

                sm_store_merge_segment_value(
                  state,
                  acc,
                  index,
                  entry,
                  value,
                  path_fun,
                  expiry_context,
                  attempts_left
                )

              {:error, reason} ->
                record_state_read_failure({:waraft_value_unavailable, reason})
                acc

              :not_found ->
                record_state_read_failure({:waraft_value_unavailable, :not_found})
                acc

              invalid ->
                record_state_read_failure({:invalid_waraft_read_result, invalid})
                acc
            end
        end)
      end

      defp sm_store_merge_segment_value(
             state,
             acc,
             index,
             entry,
             value,
             path_fun,
             expiry_context,
             attempts_left
           ) do
        case BlobValue.maybe_materialize(
               Map.get(blob_apply_ctx(state), :data_dir),
               state.shard_index,
               BlobValue.threshold(blob_apply_ctx(state)),
               value
             ) do
          {:ok, materialized} when is_binary(materialized) ->
            case sm_store_accept_current_batch_value(
                   state,
                   entry,
                   value,
                   materialized,
                   path_fun,
                   expiry_context,
                   attempts_left
                 ) do
              {:ok, current} -> Map.put(acc, index, current)
              :missing -> acc
            end

          {:error, reason} ->
            case sm_store_retry_failed_current_batch_value(
                   state,
                   entry,
                   path_fun,
                   expiry_context,
                   attempts_left,
                   {:blob_ref_unavailable, reason}
                 ) do
              {:ok, current} -> Map.put(acc, index, current)
              :missing -> acc
            end
        end
      end

      defp materialize_state_machine_batch_values(state, values) do
        ctx = blob_apply_ctx(state)
        threshold = BlobValue.threshold(ctx)

        if threshold > 0 do
          binary_values = Enum.filter(values, &is_binary/1)

          materialized =
            BlobValue.maybe_materialize_many(
              Map.get(ctx, :data_dir),
              state.shard_index,
              threshold,
              binary_values
            )

          {inflated, _remaining} =
            Enum.map_reduce(values, materialized, fn
              value, [{:ok, materialized_value} | rest] when is_binary(value) ->
                {materialized_value, rest}

              value, [{:error, reason} | rest] when is_binary(value) ->
                {{:error, {:blob_ref_unavailable, reason}}, rest}

              value, rest ->
                {value, rest}
            end)

          inflated
        else
          values
        end
      end

      # Raft apply must be a pure function of replicated state. A local keydir
      # miss is therefore a missing value; consulting Router here could observe
      # a different Raft group or replica and make peers apply different data.
      defp sm_store_batch_remote_get(_entries, _ctx, results), do: results

      defp merge_indexed_values(results, entries, values) do
        entries
        |> Enum.zip(values)
        |> Enum.reduce(results, fn {entry, value}, acc ->
          merge_indexed_value(acc, entry, value)
        end)
      end

      defp merge_indexed_value(acc, {index, _key}, value) when is_integer(index),
        do: Map.put(acc, index, value)

      defp merge_indexed_value(acc, {_key, index}, value), do: Map.put(acc, index, value)

      defp values_for_indexes(results, keys) do
        keys
        |> Enum.with_index()
        |> Enum.map(fn {_key, index} -> Map.get(results, index) end)
      end

      defp instance_ctx_for_state(%{instance_ctx: %FerricStore.Instance{} = ctx}), do: ctx

      # A reused instance name must not bind recovered state to a different data root.
      defp instance_ctx_for_state(%{instance_name: name} = state) when is_atom(name) do
        case instance_ctx_by_name(name) do
          %FerricStore.Instance{} = ctx ->
            if instance_data_path?(ctx, state), do: ctx, else: nil

          _missing ->
            nil
        end
      end

      defp instance_ctx_for_state(_state), do: nil

      defp instance_ctx_by_name(name) do
        FerricStore.Instance.get(name)
      rescue
        _ -> nil
      catch
        :exit, _ -> nil
      end

      # Mirror of TypeRegistry.check_or_set but operates on state machine state.
      # Writes type metadata on first use, returns :ok or wrongtype error.
      # Feature level 4 (Ferricstore.Cluster.FeatureLevel): a write of hash, set
      # or sorted set members checks the key's type in its own entry, as ZINCRBY
      # always has. Callers check the type before they write, so a SET or DEL
      # applied in between left the members under a string or without a type,
      # to come back when the key was recreated. Read through do_get so a hot
      # or cold marker reads the same on every replica.
      defp check_member_write_type(state, redis_key, compound_keys) do
        with type when is_binary(type) <-
               Ferricstore.Store.CompoundKey.member_write_type(redis_key, compound_keys),
             true <- feature_level_4?(state) do
          check_or_set_type(
            state,
            redis_key,
            Ferricstore.Store.CompoundKey.type_key(redis_key),
            type
          )
        else
          _unchecked -> :ok
        end
      end

      # Feature level 4: a command that reads one hash or sorted set, decides
      # and writes it runs its handler here, on the state machine's store, so
      # concurrent clients each see one outcome (one HDEL removes a field, one
      # ZADD NX adds a member). Older releases do not apply the entry, so it
      # is refused below the level on every release that knows it.
      defp do_compound_command(state, redis_key, ast) do
        with {:ok, handler} <- compound_command_handler(redis_key, ast),
             :ok <- require_compound_command_level(state, ast),
             :ok <- check_fetch_or_compute_lock(state, redis_key, nil) do
          run_compound_command(handler, ast, compound_command_store(state, handler))
        end
      end

      # The member index learns an entry's own deletes when the entry flushes,
      # and a delete that is the entry's first write only queues its keydir
      # rows' removal. So a read later in the same entry saw the deleted
      # members: HDEL of a hash's last field counted one left and kept the
      # type, and the key then refused a list or string as the wrong type.
      # The handler's reads apply the queued removals first, and a count under
      # a prefix this entry deleted from counts the members still in the
      # keydir. (Only collections in the shared file queue deletes; those of a
      # promoted collection reach the index as they are written.)
      @compound_command_reads [
        :compound_get,
        :compound_get_meta,
        :compound_batch_get,
        :compound_scan,
        :compound_scan_page,
        :compound_scan_slice,
        :exists?
      ]

      # A TDIGEST digest is a plain value: its handler reads and writes the key
      # itself, as the HyperLogLog handlers do (`build_string_value_store/1`).
      defp compound_command_store(state, Ferricstore.Commands.TDigest) do
        value_store = build_string_value_store(state)

        state
        |> compound_command_store()
        |> Map.merge(%{
          get: materializing_read(state, value_store.get),
          get_meta: materializing_read(state, value_store.get_meta),
          put: value_store.put,
          delete: value_store.delete,
          level_5_writes?: feature_level_5?(state)
        })
      end

      # DEL of one key deletes whatever it holds: its value, or its
      # collection's members, metadata and type key. The prob file of a
      # bloom/cms/cuckoo/topk key goes with its value's delete, as for every
      # replicated delete (`do_delete/2`).
      defp compound_command_store(state, Ferricstore.Commands.Strings) do
        value_store = build_string_value_store(state)

        state
        |> compound_command_store()
        |> Map.merge(%{
          get: materializing_read(state, value_store.get),
          get_meta: materializing_read(state, value_store.get_meta),
          delete: value_store.delete,
          compound_delete_prefix: fn redis_key, prefix ->
            do_compound_delete_prefix(state, redis_key, prefix)
          end,
          prob_file_lifecycle: :replicated
        })
      end

      defp compound_command_store(state, _handler), do: compound_command_store(state)

      defp compound_command_store(state) do
        store = build_compound_store(state)

        store =
          Enum.reduce(@compound_command_reads, store, fn name, store ->
            case Map.fetch(store, name) do
              {:ok, read} -> Map.put(store, name, materializing_read(state, read))
              :error -> store
            end
          end)

        case Map.fetch(store, :compound_count) do
          {:ok, count} -> Map.put(store, :compound_count, entry_aware_count(state, count))
          :error -> store
        end
      end

      defp entry_aware_count(state, count) do
        fn redis_key, prefix ->
          :ok = materialize_pending_fast_deletes(state)

          with true <- pending_delete_under?(prefix),
               {:ok, live} <-
                 CompoundMemberIndex.count_live(
                   Map.get(state, :compound_member_index_name),
                   shard_ets_state(state),
                   prefix
                 ) do
            live
          else
            _indexed_count_is_current -> count.(redis_key, prefix)
          end
        end
      end

      defp pending_delete_under?(prefix) do
        Enum.any?(Process.get(:sm_pending_writes, []), fn
          {:delete, key, _prob_path} when is_binary(key) -> String.starts_with?(key, prefix)
          _write -> false
        end)
      end

      defp materializing_read(state, read) when is_function(read, 1),
        do: fn a -> materialize_then(state, fn -> read.(a) end) end

      defp materializing_read(state, read) when is_function(read, 2),
        do: fn a, b -> materialize_then(state, fn -> read.(a, b) end) end

      defp materializing_read(state, read) when is_function(read, 3),
        do: fn a, b, c -> materialize_then(state, fn -> read.(a, b, c) end) end

      defp materializing_read(state, read) when is_function(read, 5),
        do: fn a, b, c, d, e -> materialize_then(state, fn -> read.(a, b, c, d, e) end) end

      defp materializing_read(state, read) when is_function(read, 6),
        do: fn a, b, c, d, e, f -> materialize_then(state, fn -> read.(a, b, c, d, e, f) end) end

      defp materializing_read(_state, read), do: read

      defp materialize_then(state, read) do
        :ok = materialize_pending_fast_deletes(state)
        read.()
      end

      # A handler that raised would stop the shard's storage on every replay.
      # Every replica runs the same code on the same entry, so each answers the
      # same error instead, and the entry's pending writes roll back.
      defp run_compound_command(handler, ast, store) do
        handler.handle_ast(ast, store)
      rescue
        error ->
          Logger.error(
            "compound command #{inspect(elem(ast, 0))} raised in apply: " <>
              Exception.format(:error, error, __STACKTRACE__)
          )

          {:error, "ERR internal error applying #{elem(ast, 0)}"}
      end

      defp compound_command_handler(key, {:hdel, [key | fields]})
           when is_binary(key) and fields != [],
           do: binaries_handler(fields, Ferricstore.Commands.Hash)

      defp compound_command_handler(key, {:hsetnx, key, field, value})
           when is_binary(key) and is_binary(field) and is_binary(value),
           do: {:ok, Ferricstore.Commands.Hash}

      defp compound_command_handler(key, {:hgetdel, key, fields})
           when is_binary(key) and is_list(fields) and fields != [],
           do: binaries_handler(fields, Ferricstore.Commands.Hash)

      defp compound_command_handler(key, {:zadd, key, opts, pairs})
           when is_binary(key) and is_list(opts) and opts != [] and is_list(pairs),
           do: {:ok, Ferricstore.Commands.SortedSet}

      defp compound_command_handler(key, {:geoadd, key, flags, pairs})
           when is_binary(key) and is_list(flags) and flags != [] and is_list(pairs),
           do: {:ok, Ferricstore.Commands.Geo}

      defp compound_command_handler(key, {:hsetex, key, {condition, _expiry}, pairs})
           when is_binary(key) and condition in [nil, :fnx, :fxx] and is_list(pairs) and
                  pairs != [],
           do: binaries_handler(pairs, Ferricstore.Commands.Hash)

      defp compound_command_handler(key, {:del, [key]}) when is_binary(key),
        do: {:ok, Ferricstore.Commands.Strings}

      defp compound_command_handler(key, {:tdigest_create, key, compression})
           when is_binary(key) and (is_nil(compression) or is_integer(compression)),
           do: {:ok, Ferricstore.Commands.TDigest}

      defp compound_command_handler(key, {:tdigest_add, key, values})
           when is_binary(key) and is_list(values) and values != [] do
        if Enum.all?(values, &is_float/1),
          do: {:ok, Ferricstore.Commands.TDigest},
          else: {:error, "ERR invalid compound command"}
      end

      defp compound_command_handler(key, {:tdigest_reset, [key]}) when is_binary(key),
        do: {:ok, Ferricstore.Commands.TDigest}

      defp compound_command_handler(key, {:tdigest_merge, key, src_keys, opts})
           when is_binary(key) and is_list(src_keys) and src_keys != [] and is_list(opts),
           do: binaries_handler(src_keys, Ferricstore.Commands.TDigest)

      defp compound_command_handler(_key, _ast), do: {:error, "ERR invalid compound command"}

      defp binaries_handler(values, handler) do
        if Enum.all?(values, &is_binary/1),
          do: {:ok, handler},
          else: {:error, "ERR invalid compound command"}
      end

      # HSETEX and ZADD INCR joined the entry at level 5; releases before it
      # refuse them.
      defp require_compound_command_level(state, {:hsetex, _key, _options, _pairs}),
        do: require_feature_level_5(state)

      defp require_compound_command_level(state, {:zadd, _key, opts, _pairs})
           when is_list(opts) do
        if :incr in opts,
          do: require_feature_level_5(state),
          else: require_feature_level_4(state)
      end

      # DEL and TDIGEST writes joined the entry at level 6.
      defp require_compound_command_level(state, ast)
           when elem(ast, 0) in [
                  :del,
                  :tdigest_create,
                  :tdigest_add,
                  :tdigest_reset,
                  :tdigest_merge
                ] do
        if feature_level_6?(state),
          do: :ok,
          else: {:error, "ERR this command requires cluster feature level 6"}
      end

      defp require_compound_command_level(state, _ast), do: require_feature_level_4(state)

      defp require_feature_level_5(state) do
        if feature_level_5?(state),
          do: :ok,
          else: {:error, "ERR this command requires cluster feature level 5"}
      end

      defp require_feature_level_4(state) do
        if feature_level_4?(state),
          do: :ok,
          else: {:error, "ERR this command requires cluster feature level 4"}
      end

      defp feature_level_4?(state),
        do:
          do_get(state, Ferricstore.Cluster.FeatureLevel.key(4)) ==
            Ferricstore.Cluster.FeatureLevel.marker()

      defp feature_level_5?(state),
        do:
          do_get(state, Ferricstore.Cluster.FeatureLevel.key(5)) ==
            Ferricstore.Cluster.FeatureLevel.marker()

      defp feature_level_6?(state),
        do:
          do_get(state, Ferricstore.Cluster.FeatureLevel.key(6)) ==
            Ferricstore.Cluster.FeatureLevel.marker()

      defp feature_level_7?(state),
        do:
          do_get(state, Ferricstore.Cluster.FeatureLevel.key(7)) ==
            Ferricstore.Cluster.FeatureLevel.marker()

      defp feature_level_8?(state),
        do:
          do_get(state, Ferricstore.Cluster.FeatureLevel.key(8)) ==
            Ferricstore.Cluster.FeatureLevel.marker()

      # A key's TTL is stored on each of its members. From feature level 6 a
      # member written into a collection that has one takes it (its type key's
      # expiry, while in the future), so members added after EXPIRE expire with
      # the rest; older releases write none.
      defp new_member_expire_at(state, redis_key) do
        if feature_level_6?(state) do
          type_key = Ferricstore.Store.CompoundKey.type_key(redis_key)

          case sm_store_compound_get_meta(state, redis_key, type_key) do
            {_type, expire_at_ms} when is_integer(expire_at_ms) and expire_at_ms > 0 ->
              if expire_at_ms > CommandTime.now_ms(), do: expire_at_ms, else: 0

            _none_or_failure ->
              0
          end
        else
          0
        end
      end

      # Feature level 4 also stores INCRBYFLOAT and HINCRBYFLOAT results as
      # Redis does ("3", "5200"); older releases store Float.to_string/1
      # ("3.0", "5.2e3"), which INCR and HINCRBY then refuse.
      defp stored_float_increment(state, value) when is_float(value) do
        if feature_level_4?(state),
          do: ValueCodec.format_float_plain(value),
          else: value
      end

      defp compound_entry_keys(entries) when is_list(entries) do
        Enum.map(entries, fn
          {compound_key, _value, _expire_at_ms} -> compound_key
          _invalid -> nil
        end)
      end

      defp compound_entry_keys(_entries), do: []

      defp check_or_set_type(state, redis_key, type_key, expected_type) do
        case sm_store_compound_get(state, redis_key, type_key) do
          {:error, _reason} = error ->
            error

          nil ->
            check_or_create_type_for_plain_key(state, redis_key, type_key, expected_type)

          existing when is_binary(existing) and existing == expected_type ->
            :ok

          _other ->
            {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
        end
      end

      defp check_or_create_type_for_plain_key(state, redis_key, type_key, expected_type) do
        case ets_lookup(state, redis_key) do
          {:hit, _value, _expire_at_ms} ->
            {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

          :expired ->
            do_compound_put(state, redis_key, type_key, expected_type, 0)

          :miss ->
            case Process.get(:sm_state_read_failure) do
              nil -> do_compound_put(state, redis_key, type_key, expected_type, 0)
              reason -> {:error, {:state_read_failed, reason}}
            end
        end
      end

      # Rejects exactly what the apply raised on (div/rem of a non-integer, a
      # bit other than 0 or 1). A negative offset applied in older releases, so
      # it still does here; the API and commands reject it before the log.
      defp check_setbit_args(offset, bit_val) when is_integer(offset) and bit_val in [0, 1],
        do: :ok

      defp check_setbit_args(offset, _bit_val) when is_integer(offset),
        do: {:error, "ERR bit is not an integer or out of range"}

      defp check_setbit_args(_offset, _bit_val),
        do: {:error, "ERR bit offset is not an integer or out of range"}

      # The logical key projection rejects a non-integer expiry after the value
      # is written, which crashes storage on every replay of the entry.
      defp check_expire_at(expire_at_ms) when is_integer(expire_at_ms) and expire_at_ms >= 0,
        do: :ok

      defp check_expire_at(_expire_at_ms),
        do: {:error, "ERR value is not an integer or out of range"}

      defp check_optional_expire_at(nil), do: :ok
      defp check_optional_expire_at(expire_at_ms), do: check_expire_at(expire_at_ms)

      # Overwrites bytes at `offset` with `value`, zero-padding if the original
      # string is shorter than offset. Mirrors shard.ex apply_setrange/3.
      defp sm_apply_setrange(old, offset, value) do
        old_len = byte_size(old)
        val_len = byte_size(value)

        cond do
          val_len == 0 ->
            if offset > old_len do
              old <> :binary.copy(<<0>>, offset - old_len)
            else
              old
            end

          offset >= old_len ->
            padding = :binary.copy(<<0>>, offset - old_len)
            old <> padding <> value

          offset + val_len >= old_len ->
            binary_part(old, 0, offset) <> value

          true ->
            binary_part(old, 0, offset) <>
              value <>
              binary_part(old, offset + val_len, old_len - offset - val_len)
        end
      end

      # ---------------------------------------------------------------------------
      # Private: compare-and-swap
      # ---------------------------------------------------------------------------

      # Reads the current value from ETS (with Bitcask fallback), compares it
      # against `expected`. If match, writes `new_value` with optional TTL.
      # Returns 1 (swapped), 0 (mismatch), or nil (missing/expired).
      #
      # Replicates the exact shard.ex handle_cas_direct logic.
      # NOTE: The caller (shard.ex) pre-computes expire_at_ms as an absolute
      # timestamp before entering Raft to keep the state machine deterministic
      # (no System.os_time calls). So the 5th arg is already absolute, not relative.
      defp do_cas(state, key, expected, new_value, expire_at_ms) do
        with :ok <- check_optional_expire_at(expire_at_ms) do
          case ets_lookup(state, key) do
            {:hit, ^expected, old_exp} ->
              expire = if expire_at_ms, do: expire_at_ms, else: old_exp
              do_put(state, key, new_value, expire)
              1

            {:hit, _other, _exp} ->
              0

            :expired ->
              nil

            :miss ->
              nil
          end
        end
      end

      # ---------------------------------------------------------------------------
      # Private: distributed lock operations
      # ---------------------------------------------------------------------------

      # Acquires a lock. If the key doesn't exist, is expired, or is already held
      # by the same owner, sets {owner, ttl}. Returns :ok or {:error, reason}.
      #
      # Replicates the exact shard.ex handle_lock_direct logic.
      # NOTE: The caller (shard.ex) pre-computes expire_at_ms as an absolute
      # timestamp before entering Raft to keep the state machine deterministic.
      defp do_lock(state, key, owner, expire_at_ms) do
        with :ok <- check_expire_at(expire_at_ms), do: apply_lock(state, key, owner, expire_at_ms)
      end

      defp apply_lock(state, key, owner, expire_at_ms) do
        case ets_lookup(state, key) do
          {:hit, ^owner, _exp} ->
            # Same owner -- re-acquire (idempotent)
            do_put(state, key, owner, expire_at_ms)
            :ok

          {:hit, _other, _exp} ->
            {:error, "DISTLOCK lock is held by another owner"}

          _ ->
            # Missing or expired -- acquire
            do_put(state, key, owner, expire_at_ms)
            :ok
        end
      end

      # Releases a lock. If the key exists and the owner matches, deletes the key.
      # Returns 1 on success, {:error, reason} on owner mismatch.
      #
      # Replicates the exact shard.ex handle_unlock_direct logic.
      defp do_unlock(state, key, owner) do
        case ets_lookup(state, key) do
          {:hit, ^owner, _exp} ->
            do_delete(state, key)
            1

          {:hit, _other, _exp} ->
            {:error, "DISTLOCK caller is not the lock owner"}

          _ ->
            # Missing or expired -- treat as already unlocked
            1
        end
      end

      # Extends a lock's TTL. If the key exists and the owner matches, updates
      # the TTL. Returns 1 on success, {:error, reason} on mismatch or missing.
      #
      # Replicates the exact shard.ex handle_extend_direct logic.
      # NOTE: The caller (shard.ex) pre-computes expire_at_ms as an absolute
      # timestamp before entering Raft to keep the state machine deterministic.
      defp do_extend(state, key, owner, expire_at_ms) do
        with :ok <- check_expire_at(expire_at_ms),
             do: apply_extend(state, key, owner, expire_at_ms)
      end

      defp apply_extend(state, key, owner, expire_at_ms) do
        case ets_lookup(state, key) do
          {:hit, ^owner, _exp} ->
            do_put(state, key, owner, expire_at_ms)
            1

          {:hit, _other, _exp} ->
            {:error, "DISTLOCK caller is not the lock owner"}

          _ ->
            {:error, "DISTLOCK lock does not exist or has expired"}
        end
      end

      # ---------------------------------------------------------------------------
      # Private: fetch-or-compute ownership locking
      # ---------------------------------------------------------------------------

      @fetch_or_compute_lock_prune_batch 256

      # Locks all keys atomically. If any key is already locked by a different
      # owner (and not expired), rejects the entire batch.
      # Returns {new_state, result} — locks are persisted in Raft state.
      defp do_acquire_fetch_or_compute_locks(state, keys, owner_ref, expire_at_ms) do
        locks = Map.get(state, :fetch_or_compute_locks, %{})
        {state, expiry_index} = ensure_fetch_or_compute_lock_expiry_index(state, locks)
        expiry_context = ExpiryContext.capture()

        case prune_fetch_or_compute_lock_expiries(
               locks,
               expiry_index,
               expiry_context,
               @fetch_or_compute_lock_prune_batch
             ) do
          {:error, reason} ->
            {state, {:error, reason}}

          {:ok, pruned_locks, pruned_expiry_index} ->
            indexed_state =
              put_fetch_or_compute_lock_state(state, pruned_locks, pruned_expiry_index)

            case fetch_or_compute_lock_conflict(
                   keys,
                   pruned_locks,
                   owner_ref,
                   expiry_context
                 ) do
              {:error, reason} ->
                {state, {:error, reason}}

              :conflict ->
                {indexed_state, {:error, :keys_locked}}

              :ok ->
                {new_locks, new_expiry_index} =
                  Enum.reduce(
                    keys,
                    {pruned_locks, pruned_expiry_index},
                    fn key, {lock_acc, index_acc} ->
                      index_acc =
                        remove_fetch_or_compute_lock_expiry(
                          index_acc,
                          key,
                          Map.get(lock_acc, key)
                        )

                      {
                        Map.put(lock_acc, key, {owner_ref, expire_at_ms}),
                        :gb_trees.enter({expire_at_ms, key}, owner_ref, index_acc)
                      }
                    end
                  )

                {put_fetch_or_compute_lock_state(state, new_locks, new_expiry_index), :ok}
            end
        end
      end

      defp fetch_or_compute_lock_conflict(keys, locks, owner_ref, expiry_context) do
        Enum.reduce_while(keys, :ok, fn key, :ok ->
          case Map.get(locks, key) do
            nil ->
              {:cont, :ok}

            {^owner_ref, _expire_at_ms} ->
              {:cont, :ok}

            {_other_owner, expire_at_ms} ->
              case ExpiryContext.classify(expiry_context, expire_at_ms) do
                :live -> {:halt, :conflict}
                :expired -> {:cont, :ok}
                {:unsafe, reason} -> {:halt, {:error, reason}}
              end
          end
        end)
      end

      # Unlocks keys owned by the given owner_ref.
      # Returns {new_state, :ok}.
      defp do_release_fetch_or_compute_locks(state, keys, owner_ref) do
        locks = Map.get(state, :fetch_or_compute_locks, %{})
        {state, expiry_index} = ensure_fetch_or_compute_lock_expiry_index(state, locks)

        {new_locks, new_expiry_index} =
          Enum.reduce(keys, {locks, expiry_index}, fn key, {lock_acc, index_acc} ->
            case Map.get(lock_acc, key) do
              {^owner_ref, _exp} = lock ->
                {
                  Map.delete(lock_acc, key),
                  remove_fetch_or_compute_lock_expiry(index_acc, key, lock)
                }

              _missing_or_other_owner ->
                {lock_acc, index_acc}
            end
          end)

        {put_fetch_or_compute_lock_state(state, new_locks, new_expiry_index), :ok}
      end

      defp do_release_fetch_or_compute_locks_owned(state, keys, owner_ref) do
        locks = Map.get(state, :fetch_or_compute_locks, %{})
        {state, expiry_index} = ensure_fetch_or_compute_lock_expiry_index(state, locks)
        expiry_context = ExpiryContext.capture()

        case fetch_or_compute_owned_lock_status(keys, locks, owner_ref, expiry_context) do
          :ok ->
            {new_locks, new_expiry_index} =
              Enum.reduce(keys, {locks, expiry_index}, fn key, {lock_acc, index_acc} ->
                lock = Map.fetch!(lock_acc, key)

                {
                  Map.delete(lock_acc, key),
                  remove_fetch_or_compute_lock_expiry(index_acc, key, lock)
                }
              end)

            {put_fetch_or_compute_lock_state(state, new_locks, new_expiry_index), :ok}

          {:error, reason} ->
            {state, {:error, reason}}
        end
      end

      defp fetch_or_compute_owned_lock_status(keys, locks, owner_ref, expiry_context) do
        Enum.reduce_while(keys, :ok, fn key, :ok ->
          case Map.get(locks, key) do
            {^owner_ref, expire_at_ms} ->
              case ExpiryContext.classify(expiry_context, expire_at_ms) do
                :live -> {:cont, :ok}
                :expired -> {:halt, {:error, :not_lock_owner}}
                {:unsafe, reason} -> {:halt, {:error, reason}}
              end

            _missing_or_other_owner ->
              {:halt, {:error, :not_lock_owner}}
          end
        end)
      end

      defp ensure_fetch_or_compute_lock_expiry_index(state, locks) do
        case Map.fetch(state, :fetch_or_compute_lock_expiries) do
          {:ok, expiry_index} ->
            {state, expiry_index}

          :error ->
            expiry_index =
              Enum.reduce(locks, :gb_trees.empty(), fn {key, {owner_ref, expire_at_ms}}, acc ->
                :gb_trees.enter({expire_at_ms, key}, owner_ref, acc)
              end)

            {Map.put(state, :fetch_or_compute_lock_expiries, expiry_index), expiry_index}
        end
      end

      defp prune_fetch_or_compute_lock_expiries(locks, expiry_index, _expiry_context, 0),
        do: {:ok, locks, expiry_index}

      defp prune_fetch_or_compute_lock_expiries(
             locks,
             expiry_index,
             expiry_context,
             remaining
           ) do
        if :gb_trees.is_empty(expiry_index) do
          {:ok, locks, expiry_index}
        else
          {{expire_at_ms, key}, indexed_owner} = :gb_trees.smallest(expiry_index)

          case Map.get(locks, key) do
            {^indexed_owner, ^expire_at_ms} ->
              case ExpiryContext.classify(expiry_context, expire_at_ms) do
                :live ->
                  {:ok, locks, expiry_index}

                :expired ->
                  prune_fetch_or_compute_lock_expiries(
                    Map.delete(locks, key),
                    :gb_trees.delete({expire_at_ms, key}, expiry_index),
                    expiry_context,
                    remaining - 1
                  )

                {:unsafe, _reason} ->
                  {:ok, locks, expiry_index}
              end

            _missing_or_renewed ->
              prune_fetch_or_compute_lock_expiries(
                locks,
                :gb_trees.delete({expire_at_ms, key}, expiry_index),
                expiry_context,
                remaining - 1
              )
          end
        end
      end

      defp remove_fetch_or_compute_lock_expiry(expiry_index, _key, nil), do: expiry_index

      defp remove_fetch_or_compute_lock_expiry(expiry_index, key, {_owner_ref, expire_at_ms}) do
        :gb_trees.delete_any({expire_at_ms, key}, expiry_index)
      end

      defp put_fetch_or_compute_lock_state(state, locks, expiry_index) do
        state
        |> Map.put(:fetch_or_compute_locks, locks)
        |> Map.put(:fetch_or_compute_lock_expiries, expiry_index)
      end

      defp do_fetch_or_compute_lock(state, key, outcome_key, owner_ref, expire_at_ms) do
        with :ok <- validate_fetch_or_compute_outcome_key(key, outcome_key) do
          case do_acquire_fetch_or_compute_locks(state, [key], owner_ref, expire_at_ms) do
            {locked_state, :ok} ->
              case clear_fetch_or_compute_outcome(locked_state, outcome_key) do
                :ok -> {locked_state, :ok}
                {:error, _reason} = error -> {state, error}
              end

            {indexed_state, {:error, _reason} = error} ->
              {indexed_state, error}
          end
        else
          {:error, _reason} = error -> {state, error}
        end
      end

      defp do_fetch_or_compute_fail(
             state,
             key,
             outcome_key,
             encoded_error,
             outcome_expire_at_ms,
             owner_ref
           ) do
        with :ok <- validate_fetch_or_compute_outcome_key(key, outcome_key),
             {:ok, _error} <- FetchOrComputeOutcome.decode_error(encoded_error),
             :ok <- check_fetch_or_compute_lock(state, key, owner_ref) do
          case with_pending_writes(state, fn ->
                 do_put(state, outcome_key, encoded_error, outcome_expire_at_ms)
               end) do
            :ok -> do_release_fetch_or_compute_locks(state, [key], owner_ref)
            {:error, _reason} = error -> {state, error}
            other -> {state, {:error, {:invalid_fetch_or_compute_write_result, other}}}
          end
        else
          {:error, _reason} = error -> {state, error}
        end
      end

      defp do_fetch_or_compute_publish(state, key, value, expire_at_ms, owner_ref) do
        with :ok <- check_fetch_or_compute_lock(state, key, owner_ref) do
          case with_pending_writes(state, fn -> do_put(state, key, value, expire_at_ms) end) do
            :ok -> do_release_fetch_or_compute_locks(state, [key], owner_ref)
            {:error, _reason} = error -> {state, error}
            other -> {state, {:error, {:invalid_fetch_or_compute_write_result, other}}}
          end
        else
          {:error, _reason} = error -> {state, error}
        end
      end

      defp validate_fetch_or_compute_outcome_key(key, outcome_key) do
        if outcome_key == FetchOrComputeOutcome.key(key) do
          :ok
        else
          {:error, "ERR invalid fetch_or_compute outcome key"}
        end
      end

      defp clear_fetch_or_compute_outcome(state, outcome_key) do
        case ets_lookup(state, outcome_key) do
          {:hit, _value, _expire_at_ms} ->
            case with_pending_writes(state, fn -> do_delete(state, outcome_key) end) do
              result when result in [:ok, 0, 1] -> :ok
              {:error, _reason} = error -> error
              other -> {:error, {:invalid_fetch_or_compute_delete_result, other}}
            end

          _missing_or_expired ->
            :ok
        end
      end

      # Ordinary mutations may proceed when no live lock exists. Fenced
      # mutations must prove that their exact owner still holds a live lock.
      defp check_fetch_or_compute_lock(state, key, nil) do
        locks = Map.get(state, :fetch_or_compute_locks, %{})

        case Map.get(locks, key) do
          nil ->
            :ok

          {_owner_ref, expire_at_ms} ->
            expiry_context = ExpiryContext.capture()

            case ExpiryContext.classify(expiry_context, expire_at_ms) do
              :live -> {:error, :key_locked}
              :expired -> :ok
              {:unsafe, reason} -> {:error, reason}
            end
        end
      end

      defp check_fetch_or_compute_lock(state, key, owner_ref) do
        locks = Map.get(state, :fetch_or_compute_locks, %{})

        case Map.get(locks, key) do
          nil ->
            {:error, :key_not_locked}

          {lock_owner, expire_at_ms} ->
            expiry_context = ExpiryContext.capture()

            case ExpiryContext.classify(expiry_context, expire_at_ms) do
              :live when lock_owner == owner_ref -> :ok
              :live -> {:error, :key_locked}
              :expired -> {:error, :key_lock_expired}
              {:unsafe, reason} -> {:error, reason}
            end
        end
      end

      # ---------------------------------------------------------------------------
      # Private: sliding window rate limiter
      # ---------------------------------------------------------------------------

      # Implements a sliding window rate limiter. Reads current counters from ETS,
      # rotates windows as needed, computes the effective count using a weighted
      # sliding window approximation, and updates the stored state.
      # Returns [status, count, remaining, ms_until_reset].
      #
      # Replicates the exact shard.ex handle_ratelimit_add_direct logic.
      defp do_ratelimit_add(state, key, window_ms, max, count) do
        with :ok <- RateLimit.validate_apply_args(window_ms, max, count),
             do: apply_ratelimit_add(state, key, window_ms, max, count)
      end

      defp apply_ratelimit_add(state, key, window_ms, max, count) do
        now = apply_now_ms()

        {{cur_count, cur_start, prv_count}, original} =
          case ets_lookup(state, key) do
            {:hit, value, expire_at_ms} ->
              {decode_ratelimit(value, now), {:existing, value, expire_at_ms}}

            _ ->
              {{0, now, 0}, :missing}
          end

        # Rotate windows
        {cur_count, cur_start, prv_count} =
          cond do
            now - cur_start >= window_ms * 2 -> {0, now, 0}
            now - cur_start >= window_ms -> {0, now, cur_count}
            true -> {cur_count, cur_start, prv_count}
          end

        elapsed = now - cur_start
        effective = RateLimit.effective_count(cur_count, prv_count, elapsed, window_ms)
        expire_at_ms = cur_start + window_ms * 2

        {status, final_count, remaining, value} =
          if effective + count > max do
            value = encode_ratelimit(cur_count, cur_start, prv_count)
            {"denied", effective, max(0, max - effective), value}
          else
            new_cur = cur_count + count
            new_eff = effective + count
            value = encode_ratelimit(new_cur, cur_start, prv_count)
            {"allowed", new_eff, max(0, max - new_eff), value}
          end

        unless status == "denied" and
                 rate_limit_state_unchanged?(original, value, expire_at_ms) do
          do_put(state, key, value, expire_at_ms)
        end

        ms_until_reset = max(0, cur_start + window_ms - now)
        [status, final_count, remaining, ms_until_reset]
      end

      defp rate_limit_state_unchanged?(
             {:existing, value, expire_at_ms},
             value,
             expire_at_ms
           ),
           do: true

      defp rate_limit_state_unchanged?(_original, _value, _expire_at_ms), do: false

      # Delegates to the shared ValueCodec to avoid duplication with shard.ex.
      defp encode_ratelimit(cur, start, prev), do: ValueCodec.encode_ratelimit(cur, start, prev)

      defp decode_ratelimit(value, fallback_start_ms),
        do: ValueCodec.decode_ratelimit(value, fallback_start_ms)

      # ---------------------------------------------------------------------------
      # Private: ETS lookup with expiry checking
      # ---------------------------------------------------------------------------

      # Reads a key from ETS, checking expiry. Falls back to Bitcask for cold
      # keys. Returns {:hit, value, expire_at_ms}, :expired, or :miss.
      # Mirrors the shard's `ets_lookup/2` logic with Bitcask fallback for
      # keys that may not yet be warmed into ETS.
      defp ets_lookup(state, key) do
        case sm_pending_value_meta(key) do
          {:hit, value, exp} ->
            {:hit, value, exp}

          :miss ->
            ets_lookup_committed(state, key)
        end
      end

      defp sm_pending_value_meta(key) do
        pending = Process.get(:sm_pending_values)
        expiry_context = ExpiryContext.capture()

        case pending && Map.get(pending, key) do
          :deleted ->
            :miss

          {value, exp} when is_integer(exp) and exp >= 0 ->
            case ExpiryContext.classify(expiry_context, exp) do
              :live ->
                {:hit, value, exp}

              :expired ->
                Process.put(:sm_pending_values, Map.delete(pending, key))
                :miss

              {:unsafe, reason} ->
                record_state_read_failure(reason)
                :miss
            end

          _ ->
            :miss
        end
      end

      @doc false
      def __sm_pending_value_meta_for_test__(key), do: sm_pending_value_meta(key)
    end
  end
end
