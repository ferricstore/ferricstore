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
      defp do_setrange(state, key, offset, value) do
        with :ok <- ensure_string_key(state, key) do
          {old_val, expire_at_ms} =
            case do_get_meta(state, key) do
              nil -> {"", 0}
              {v, exp} -> {to_disk_binary(v), exp}
            end

          target_size =
            Ferricstore.Raft.ApplyLimits.setrange_size(
              byte_size(old_val),
              offset,
              byte_size(value)
            )

          with :ok <- Ferricstore.Raft.ApplyLimits.validate_value_size(state, target_size),
               new_val = sm_apply_setrange(old_val, offset, value),
               :ok <- do_put(state, key, new_val, expire_at_ms) do
            {:ok, target_size}
          end
        end
      end

      # Atomic SETBIT: read bitmap, extend with zeros to include byte_index if
      # needed, flip the single bit, write back. Preserves expire_at_ms.
      # Returns the OLD bit at that offset (Redis semantics).
      defp do_setbit(state, key, offset, bit_val) do
        with :ok <- ensure_string_key(state, key) do
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

            <<prefix::binary-size(byte_index), _old::8, suffix::binary>> = extended
            new_value = <<prefix::binary, new_byte::8, suffix::binary>>

            case do_put(state, key, new_value, expire_at_ms) do
              :ok -> old_bit
              {:error, _reason} = error -> error
            end
          end
        end
      end

      # Atomic HINCRBY: read compound key (H:<redis_key>\0<field>), parse integer,
      # add delta, write back. Returns new integer value, or {:error, reason}.
      defp do_hincrby(state, redis_key, field, delta) do
        compound_key = Ferricstore.Store.CompoundKey.hash_field(redis_key, field)

        case sm_store_compound_get_meta(state, redis_key, compound_key) do
          nil ->
            if delta > @int64_max or delta < @int64_min do
              {:error, "ERR increment or decrement would overflow"}
            else
              case do_compound_put(state, redis_key, compound_key, Integer.to_string(delta), 0) do
                :ok -> delta
                {:error, _reason} = error -> error
              end
            end

          {value, expire_at_ms} ->
            case coerce_integer(value) do
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

      # Atomic HINCRBYFLOAT: same as HINCRBY but for floats.
      defp do_hincrbyfloat(state, redis_key, field, delta) do
        compound_key = Ferricstore.Store.CompoundKey.hash_field(redis_key, field)

        case sm_store_compound_get_meta(state, redis_key, compound_key) do
          nil ->
            new_val = delta * 1.0
            new_str = Float.to_string(new_val)

            case do_compound_put(state, redis_key, compound_key, new_str, 0) do
              :ok -> new_str
              {:error, _reason} = error -> error
            end

          {value, expire_at_ms} ->
            case coerce_float(value) do
              {:ok, cur} ->
                new_val = cur + delta
                new_str = Float.to_string(new_val)

                case do_compound_put(state, redis_key, compound_key, new_str, expire_at_ms) do
                  :ok -> new_str
                  {:error, _reason} = error -> error
                end

              :error ->
                {:error, "ERR hash value is not a valid float"}
            end
        end
      end

      # Atomic ZINCRBY: check/set type metadata, read member score, add delta,
      # write back. Returns the new score as a string. Returns {:error, ...} on
      # wrong type.
      defp do_zincrby(state, redis_key, increment, member) do
        type_key = Ferricstore.Store.CompoundKey.type_key(redis_key)
        expected_type = Ferricstore.Store.CompoundKey.encode_type(:zset)

        with :ok <- check_or_set_type(state, redis_key, type_key, expected_type) do
          compound_key = Ferricstore.Store.CompoundKey.zset_member(redis_key, member)

          current_score =
            case sm_store_compound_get_meta(state, redis_key, compound_key) do
              nil ->
                0.0

              {score_val, _expire_at_ms} ->
                score_str =
                  case score_val do
                    v when is_binary(v) -> v
                    v -> to_string(v)
                  end

                case Float.parse(score_str) do
                  {s, ""} -> s
                  _ -> 0.0
                end
            end

          new_score = current_score + increment * 1.0
          new_str = Float.to_string(new_score)

          case do_compound_put(state, redis_key, compound_key, new_str, 0) do
            :ok -> new_str
            {:error, _reason} = error -> error
          end
        end
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
                   apply_now_ms(),
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

      defp format_zset_score(score) when is_float(score) do
        :erlang.float_to_binary(score, [:compact, decimals: 17])
      end

      defp sm_store_compound_get_meta(state, redis_key, compound_key) do
        case sm_store_compound_path_fun(state, redis_key, compound_key) do
          nil ->
            do_get_meta(state, compound_key)

          path_fun ->
            case sm_store_batch_get(state, [compound_key], path_fun) do
              [value] when is_binary(value) ->
                case :ets.lookup(state.ets, compound_key) do
                  [{^compound_key, _ets_value, exp, _lfu, _fid, _off, _vsize}] -> {value, exp}
                  _ -> nil
                end

              _ ->
                nil
            end
        end
      end

      defp sm_store_compound_path_fun(state, redis_key, compound_key_or_prefix) do
        case promoted_compound_path(state, redis_key, compound_key_or_prefix) do
          nil -> nil
          dedicated_path -> fn _state, fid -> sm_file_path_from_path(dedicated_path, fid) end
        end
      end

      defp sm_store_batch_get(state, keys, path_fun) do
        {local_results, cold_reads, remote_entries} =
          keys
          |> Enum.with_index()
          |> Enum.reduce({%{}, [], []}, fn {key, index}, {results, cold, remote} ->
            sm_store_collect_batch_get(state, key, index, results, cold, remote)
          end)

        results =
          sm_store_read_cold_batch(state, local_results, Enum.reverse(cold_reads), path_fun)

        remote_entries
        |> Enum.reverse()
        |> sm_store_batch_remote_get(instance_ctx_for_state(state), results)
        |> values_for_indexes(keys)
      end

      defp sm_store_collect_batch_get(state, key, index, results, cold, remote) do
        case sm_pending_value_meta(key) do
          {:hit, value, _exp} ->
            {Map.put(results, index, value), cold, remote}

          :miss ->
            sm_store_collect_committed_batch_get(state, key, index, results, cold, remote)
        end
      end

      defp sm_store_collect_committed_batch_get(state, key, index, results, cold, remote) do
        now = apply_now_ms()

        case :ets.lookup(state.ets, key) do
          [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
            {Map.put(results, index, value), cold, remote}

          [{^key, nil, 0, _lfu, fid, off, vsize} = entry]
          when valid_cold_location(fid, off, vsize) ->
            {results, [{index, entry} | cold], remote}

          [{^key, nil, 0, _lfu, fid, off, vsize} = entry]
          when valid_waraft_segment_location(fid, off, vsize) ->
            {results, [{index, entry} | cold], remote}

          [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
            {Map.put(results, index, value), cold, remote}

          [{^key, nil, exp, _lfu, fid, off, vsize} = entry]
          when exp > now and valid_cold_location(fid, off, vsize) ->
            {results, [{index, entry} | cold], remote}

          [{^key, nil, exp, _lfu, fid, off, vsize} = entry]
          when exp > now and valid_waraft_segment_location(fid, off, vsize) ->
            {results, [{index, entry} | cold], remote}

          [{^key, value, exp, _lfu, _fid, _off, _vsize}]
          when exp != 0 and exp <= now ->
            track_keydir_binary_remove_known(state, key, value)
            :ets.delete(state.ets, key)
            {results, cold, [{index, key} | remote]}

          [{^key, nil, _exp, _lfu, fid, off, vsize}] ->
            record_state_read_failure({:invalid_cold_location, {fid, off, vsize}})
            {results, cold, remote}

          [] ->
            {results, cold, [{index, key} | remote]}
        end
      rescue
        ArgumentError ->
          record_state_read_failure(:keydir_unavailable)
          {results, cold, remote}
      end

      defp sm_store_read_cold_batch(_state, results, [], _path_fun), do: results

      defp sm_store_read_cold_batch(state, results, cold_reads, path_fun) do
        {segment_reads, file_reads} =
          Enum.split_with(cold_reads, fn {_index, {_key, _value, _exp, _lfu, fid, off, _vsize}} ->
            valid_waraft_segment_location(fid, off, 0)
          end)

        results =
          sm_store_read_bitcask_cold_batch(state, results, file_reads, path_fun)

        sm_store_read_waraft_segment_batch(state, results, segment_reads)
      end

      defp sm_store_read_bitcask_cold_batch(_state, results, [], _path_fun), do: results

      defp sm_store_read_bitcask_cold_batch(state, results, cold_reads, path_fun) do
        reads =
          Enum.map(cold_reads, fn {_index, {key, nil, _exp, _lfu, fid, off, _vsize} = entry} ->
            {path_fun.(state, fid), off, key, entry}
          end)

        {:ok, current_results} =
          Ferricstore.Store.ColdRead.pread_batch_keyed_current(
            reads,
            fn key, entry -> sm_store_resolve_current_bitcask(state, path_fun, key, entry) end,
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
            sm_store_maybe_warm_current_entry(state, current_entry, value)
            Map.put(acc, index, materialized)

          {{_index, _observed_entry}, {:error, reason}, _materialized}, acc ->
            record_state_read_failure({:cold_value_unavailable, reason})
            acc

          {{_index, _observed_entry}, {:value, _value, _current_entry}, {:error, reason}}, acc ->
            record_state_read_failure(reason)
            acc

          {_read, _current_result, _materialized}, acc ->
            acc
        end)
      end

      defp sm_store_current_read_value({:value, value, _entry}), do: value
      defp sm_store_current_read_value({:error, reason}), do: {:error, reason}
      defp sm_store_current_read_value(:missing), do: nil

      defp sm_store_resolve_current_bitcask(state, path_fun, key, _observed_entry) do
        now = apply_now_ms()

        case :ets.lookup(state.ets, key) do
          [{^key, value, 0, _lfu, _fid, _off, _vsize} = entry] when is_binary(value) ->
            {:hot, value, entry}

          [{^key, value, exp, _lfu, _fid, _off, _vsize} = entry]
          when exp > now and is_binary(value) ->
            {:hot, value, entry}

          [{^key, nil, 0, _lfu, fid, off, vsize} = entry]
          when valid_cold_location(fid, off, vsize) ->
            {:cold, path_fun.(state, fid), off, entry}

          [{^key, nil, exp, _lfu, fid, off, vsize} = entry]
          when exp > now and valid_cold_location(fid, off, vsize) ->
            {:cold, path_fun.(state, fid), off, entry}

          _ ->
            :missing
        end
      rescue
        ArgumentError -> :missing
      end

      defp sm_store_maybe_warm_current_entry(
             state,
             {key, nil, _exp, _lfu, _fid, _off, _vsize} = current_entry,
             value
           ) do
        ets_value = value_for_ets(value, hot_cache_threshold(state))
        replacement = put_elem(current_entry, 1, ets_value)

        if Keydir.replace_exact(state.ets, current_entry, replacement) do
          track_keydir_binary_warm(state, ets_value)
        end

        :ok
      end

      defp sm_store_maybe_warm_current_entry(_state, _current_entry, _value), do: :ok

      defp sm_store_read_waraft_segment_batch(_state, results, []), do: results

      defp sm_store_read_waraft_segment_batch(state, results, segment_reads) do
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
                sm_store_merge_segment_value(state, acc, index, entry, value)

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

      defp sm_store_merge_segment_value(state, acc, index, entry, value) do
        case BlobValue.maybe_materialize(
               Map.get(blob_apply_ctx(state), :data_dir),
               state.shard_index,
               BlobValue.threshold(blob_apply_ctx(state)),
               value
             ) do
          {:ok, ^value} ->
            sm_store_maybe_warm_current_entry(state, entry, value)
            Map.put(acc, index, value)

          {:ok, materialized} when is_binary(materialized) ->
            Map.put(acc, index, materialized)

          {:error, reason} ->
            record_state_read_failure({:blob_ref_unavailable, reason})
            acc
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
      defp check_or_set_type(state, redis_key, type_key, expected_type) do
        case sm_store_compound_get(state, redis_key, type_key) do
          nil ->
            # No type metadata yet. Reject if the key already exists as a plain string.
            if ets_has?(state.ets, redis_key) do
              {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
            else
              do_compound_put(state, redis_key, type_key, expected_type, 0)
            end

          existing when is_binary(existing) and existing == expected_type ->
            :ok

          _other ->
            {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
        end
      end

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
        now = apply_now_ms()

        {locks, expiry_index} =
          prune_fetch_or_compute_lock_expiries(
            locks,
            expiry_index,
            now,
            @fetch_or_compute_lock_prune_batch
          )

        indexed_state = put_fetch_or_compute_lock_state(state, locks, expiry_index)

        conflict =
          Enum.find(keys, fn key ->
            case Map.get(locks, key) do
              nil -> false
              {^owner_ref, _exp} -> false
              {_other, exp} -> exp > now
            end
          end)

        if conflict do
          {indexed_state, {:error, :keys_locked}}
        else
          {new_locks, new_expiry_index} =
            Enum.reduce(keys, {locks, expiry_index}, fn key, {lock_acc, index_acc} ->
              index_acc =
                remove_fetch_or_compute_lock_expiry(index_acc, key, Map.get(lock_acc, key))

              {
                Map.put(lock_acc, key, {owner_ref, expire_at_ms}),
                :gb_trees.enter({expire_at_ms, key}, owner_ref, index_acc)
              }
            end)

          {put_fetch_or_compute_lock_state(state, new_locks, new_expiry_index), :ok}
        end
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
        now = apply_now_ms()

        owns_all? =
          Enum.all?(keys, fn key ->
            case Map.get(locks, key) do
              {^owner_ref, expire_at_ms} when expire_at_ms > now -> true
              _missing_expired_or_other_owner -> false
            end
          end)

        if owns_all? do
          {new_locks, new_expiry_index} =
            Enum.reduce(keys, {locks, expiry_index}, fn key, {lock_acc, index_acc} ->
              lock = Map.fetch!(lock_acc, key)

              {
                Map.delete(lock_acc, key),
                remove_fetch_or_compute_lock_expiry(index_acc, key, lock)
              }
            end)

          {put_fetch_or_compute_lock_state(state, new_locks, new_expiry_index), :ok}
        else
          {state, {:error, :not_lock_owner}}
        end
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

      defp prune_fetch_or_compute_lock_expiries(locks, expiry_index, _now, 0),
        do: {locks, expiry_index}

      defp prune_fetch_or_compute_lock_expiries(locks, expiry_index, now, remaining) do
        if :gb_trees.is_empty(expiry_index) do
          {locks, expiry_index}
        else
          {{expire_at_ms, key}, indexed_owner} = :gb_trees.smallest(expiry_index)

          if expire_at_ms > now do
            {locks, expiry_index}
          else
            expiry_index = :gb_trees.delete({expire_at_ms, key}, expiry_index)

            locks =
              case Map.get(locks, key) do
                {^indexed_owner, ^expire_at_ms} -> Map.delete(locks, key)
                _missing_or_renewed -> locks
              end

            prune_fetch_or_compute_lock_expiries(locks, expiry_index, now, remaining - 1)
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
        now = apply_now_ms()

        case Map.get(locks, key) do
          nil -> :ok
          {_owner_ref, expire_at_ms} when expire_at_ms <= now -> :ok
          {_owner_ref, _expire_at_ms} -> {:error, :key_locked}
        end
      end

      defp check_fetch_or_compute_lock(state, key, owner_ref) do
        locks = Map.get(state, :fetch_or_compute_locks, %{})
        now = apply_now_ms()

        case Map.get(locks, key) do
          {^owner_ref, expire_at_ms} when expire_at_ms > now ->
            :ok

          {^owner_ref, _expired_at_ms} ->
            {:error, :key_lock_expired}

          nil ->
            {:error, :key_not_locked}

          {_other_owner, expire_at_ms} when expire_at_ms <= now ->
            {:error, :key_lock_expired}

          {_other_owner, _expire_at_ms} ->
            {:error, :key_locked}
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
        now = apply_now_ms()

        {cur_count, cur_start, prv_count} =
          case ets_lookup(state, key) do
            {:hit, value, _exp} -> decode_ratelimit(value, now)
            _ -> {0, now, 0}
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

        do_put(state, key, value, expire_at_ms)
        ms_until_reset = max(0, cur_start + window_ms - now)
        [status, final_count, remaining, ms_until_reset]
      end

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

        case pending && Map.get(pending, key) do
          :deleted ->
            :miss

          {value, 0} ->
            {:hit, value, 0}

          {value, exp} ->
            if exp > apply_now_ms() do
              {:hit, value, exp}
            else
              Process.put(:sm_pending_values, Map.delete(pending, key))
              :miss
            end

          _ ->
            :miss
        end
      end
    end
  end
end
