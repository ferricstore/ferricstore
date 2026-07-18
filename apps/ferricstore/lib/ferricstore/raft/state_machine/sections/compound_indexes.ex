defmodule Ferricstore.Raft.StateMachine.Sections.CompoundIndexes do
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
      alias Ferricstore.Commands.ProbParameters
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

      alias Ferricstore.Store.Shard.{CompoundMemberIndex, ZSetIndex}
      alias Ferricstore.Store.Shard.Transaction, as: ShardTransaction
      alias Ferricstore.Store.Shard.Flush, as: ShardFlush
      alias Ferricstore.Transaction.Ast, as: TxAst

      defp do_promoted_compound_delete(state, redis_key, compound_key, dedicated_path) do
        Promotion.await_compaction_latch(state, redis_key)

        active = Promotion.find_active(dedicated_path)
        maintenance = promoted_delete_maintenance(state, compound_key)

        case validate_promoted_append_location(append_promoted_tombstone(active, compound_key)) do
          {:ok, _location} ->
            track_keydir_binary_remove(state, compound_key)
            :ets.delete(state.ets, compound_key)

            CompoundMemberIndex.delete(
              Map.get(state, :compound_member_index_name),
              compound_key
            )

            sm_tx_mark_deleted(compound_key)
            queue_promoted_maintenance_after_flush(redis_key, maintenance)

            queue_promoted_revision_delete_after_flush(
              Map.get(state, :compound_revision_index_name),
              compound_key
            )

            :ok

          {:error, _reason} = err ->
            err
        end
      end

      defp zset_index_put(
             %{zset_score_index_name: index, zset_score_lookup_name: lookup},
             redis_key,
             key,
             value
           )
           when index != nil and lookup != nil do
        if standalone_staged_apply?() do
          queue_pending_zset_index_op(
            {:put, index, lookup, redis_key, key, to_disk_binary(value)}
          )
        else
          apply_zset_index_put(index, lookup, redis_key, key, to_disk_binary(value))
        end

        :ok
      end

      defp zset_index_put(_state, _redis_key, _key, _value), do: :ok

      defp zset_index_delete(
             %{zset_score_index_name: index, zset_score_lookup_name: lookup},
             redis_key,
             key
           )
           when index != nil and lookup != nil do
        operation = zset_index_delete_op(index, lookup, redis_key, key)

        if standalone_staged_apply?() do
          queue_pending_zset_index_op(operation)
        else
          apply_pending_zset_index_op(operation)
        end

        :ok
      end

      defp zset_index_delete(_state, _redis_key, _key), do: :ok

      defp queue_zset_index_put_after_flush(
             %{zset_score_index_name: index, zset_score_lookup_name: lookup},
             redis_key,
             key,
             value
           )
           when index != nil and lookup != nil do
        queue_pending_zset_index_op({:put, index, lookup, redis_key, key, to_disk_binary(value)})
      end

      defp queue_zset_index_put_after_flush(_state, _redis_key, _key, _value), do: :ok

      defp queue_zset_index_delete_after_flush(
             %{zset_score_index_name: index, zset_score_lookup_name: lookup},
             redis_key,
             key
           )
           when index != nil and lookup != nil do
        queue_pending_zset_index_op(zset_index_delete_op(index, lookup, redis_key, key))
      end

      defp queue_zset_index_delete_after_flush(_state, _redis_key, _key), do: :ok

      defp zset_index_delete_op(index, lookup, redis_key, key) do
        if key == CompoundKey.type_key(redis_key) do
          {:clear, index, lookup, redis_key}
        else
          {:delete, index, lookup, redis_key, key}
        end
      end

      defp queue_compound_indexes_put_after_flush(
             state,
             redis_key,
             compound_key,
             value,
             expire_at_ms
           ) do
        _ = queue_compound_member_index_op(state, {:put, compound_key, expire_at_ms})
        _ = maybe_queue_zset_ready_empty_after_flush(state, redis_key, compound_key, value)
        _ = queue_zset_index_put_after_flush(state, redis_key, compound_key, value)
        :ok
      end

      defp queue_compound_indexes_delete_after_flush(state, redis_key, compound_key) do
        _ = queue_compound_member_index_op(state, {:delete, compound_key})
        _ = queue_zset_index_delete_after_flush(state, redis_key, compound_key)
        :ok
      end

      defp queue_compound_member_index_op(
             %{compound_member_index_name: index},
             {:put, compound_key, expire_at_ms}
           )
           when index != nil and is_integer(expire_at_ms) and expire_at_ms >= 0 do
        pending = Process.get(:sm_pending_compound_member_index_ops, [])

        Process.put(:sm_pending_compound_member_index_ops, [
          {:put, index, compound_key, expire_at_ms} | pending
        ])

        :ok
      end

      defp queue_compound_member_index_op(
             %{compound_member_index_name: index},
             {:delete, compound_key}
           )
           when index != nil do
        pending = Process.get(:sm_pending_compound_member_index_ops, [])

        Process.put(:sm_pending_compound_member_index_ops, [
          {:delete, index, compound_key} | pending
        ])

        :ok
      end

      defp queue_compound_member_index_op(_state, _operation), do: :ok

      defp flush_pending_compound_member_indexes do
        :sm_pending_compound_member_index_ops
        |> Process.put([])
        |> Enum.reverse()
        |> Enum.each(fn
          {:put, index, compound_key, expire_at_ms} ->
            CompoundMemberIndex.put(index, compound_key, expire_at_ms)

          {:delete, index, compound_key} ->
            CompoundMemberIndex.delete(index, compound_key)
        end)

        :ok
      end

      defp maybe_queue_zset_ready_empty_after_flush(state, redis_key, compound_key, value) do
        if compound_key == CompoundKey.type_key(redis_key) and to_disk_binary(value) == "zset" do
          queue_zset_index_ready_empty_after_flush(state, redis_key)
        end

        :ok
      end

      defp queue_zset_index_ready_empty_after_flush(
             %{zset_score_index_name: index, zset_score_lookup_name: lookup},
             redis_key
           )
           when index != nil and lookup != nil do
        queue_pending_zset_index_op({:ready_empty, index, lookup, redis_key})
      end

      defp queue_zset_index_ready_empty_after_flush(_state, _redis_key), do: :ok

      defp queue_zset_index_new_put_after_flush(
             %{zset_score_index_name: index, zset_score_lookup_name: lookup},
             redis_key,
             member,
             score
           )
           when index != nil and lookup != nil do
        queue_pending_zset_index_op({:new_put, index, lookup, redis_key, member, score})
        :ok
      end

      defp queue_zset_index_new_put_after_flush(_state, _redis_key, _member, _score), do: :ok

      defp zset_index_clear(
             %{zset_score_index_name: index, zset_score_lookup_name: lookup},
             redis_key
           )
           when index != nil and lookup != nil do
        if standalone_staged_apply?() do
          queue_pending_zset_index_op({:clear, index, lookup, redis_key})
        else
          apply_zset_index_clear(index, lookup, redis_key)
        end

        :ok
      end

      defp zset_index_clear(_state, _redis_key), do: :ok

      defp queue_pending_zset_index_op(op) do
        pending = Process.get(:sm_pending_zset_index_ops, [])
        Process.put(:sm_pending_zset_index_ops, [op | pending])
      end

      defp flush_pending_zset_indexes(_state) do
        case Process.put(:sm_pending_zset_index_ops, []) do
          [] ->
            :ok

          pending when is_list(pending) ->
            pending
            |> Enum.reverse()
            |> Enum.each(&apply_pending_zset_index_op/1)

            :ok
        end
      end

      defp queue_stream_cache_cleanup({_scope, key} = cache_key) when is_binary(key) do
        pending = Process.get(:sm_pending_stream_cache_cleanups, MapSet.new())
        Process.put(:sm_pending_stream_cache_cleanups, MapSet.put(pending, cache_key))
        :ok
      end

      defp flush_pending_stream_cache_cleanups do
        pending = Process.put(:sm_pending_stream_cache_cleanups, MapSet.new())

        Enum.each(pending, fn {scope, key} ->
          Ferricstore.Commands.Strings.Delete.cleanup_stream_metadata(key, %{cache_scope: scope})
        end)

        :ok
      end

      defp apply_pending_zset_index_op({:put, index, lookup, redis_key, key, value}) do
        apply_zset_index_put(index, lookup, redis_key, key, value)
      end

      defp apply_pending_zset_index_op({:delete, index, lookup, redis_key, key}) do
        apply_zset_index_delete(index, lookup, redis_key, key)
      end

      defp apply_pending_zset_index_op({:ready_empty, index, lookup, redis_key}) do
        apply_zset_index_ready_empty(index, lookup, redis_key)
      end

      defp apply_pending_zset_index_op({:new_put, index, lookup, redis_key, member, score}) do
        apply_zset_index_new_put(index, lookup, redis_key, member, score)
      end

      defp apply_pending_zset_index_op({:clear, index, lookup, redis_key}) do
        apply_zset_index_clear(index, lookup, redis_key)
      end

      defp apply_zset_index_put(index, lookup, redis_key, key, value) do
        if zset_index_tables?(%{zset_score_index_name: index, zset_score_lookup_name: lookup}) do
          ZSetIndex.apply_put_to_tables(index, lookup, redis_key, key, value)
        end
      end

      defp apply_zset_index_delete(index, lookup, redis_key, key) do
        if zset_index_tables?(%{zset_score_index_name: index, zset_score_lookup_name: lookup}) do
          ZSetIndex.apply_delete_to_tables(index, lookup, redis_key, key)
        end
      end

      defp apply_zset_index_ready_empty(index, lookup, redis_key) do
        if zset_index_tables?(%{zset_score_index_name: index, zset_score_lookup_name: lookup}) do
          ZSetIndex.mark_ready_empty(index, lookup, redis_key)
        end
      end

      defp apply_zset_index_new_put(index, lookup, redis_key, member, score) do
        if zset_index_tables?(%{zset_score_index_name: index, zset_score_lookup_name: lookup}) do
          ZSetIndex.mark_ready_empty(index, lookup, redis_key)
          ZSetIndex.put_new_member(index, lookup, redis_key, member, score)
        end
      end

      defp apply_zset_index_clear(index, lookup, redis_key) do
        if zset_index_tables?(%{zset_score_index_name: index, zset_score_lookup_name: lookup}) do
          ZSetIndex.clear_key(index, lookup, redis_key)
        end
      end

      defp maybe_clear_compound_data_structure_for_string_put(state, key) do
        if Process.get(:sm_prob_metadata_put?, false) or
             Ferricstore.Store.CompoundKey.internal_key?(key) do
          :ok
        else
          type_key = Ferricstore.Store.CompoundKey.type_key(key)

          case string_put_compound_marker(state, type_key) do
            nil ->
              :ok

            type_marker ->
              type = CompoundKey.type_name(type_marker)

              with :ok <- clear_compound_prefix_for_string_put(state, key, type) do
                do_delete(state, type_key)
              end
          end
        end
      end

      defp string_put_compound_marker(state, type_key) do
        case sm_pending_value_meta(type_key) do
          {:hit, type, _exp} ->
            type

          :miss ->
            case :ets.lookup(state.ets, type_key) do
              [] ->
                nil

              [{^type_key, type, 0, _lfu, _fid, _off, _vsize}] when type != nil ->
                type

              _entry ->
                do_get(state, type_key)
            end
        end
      end

      defp compound_data_structure_key?(state, key) do
        not Ferricstore.Store.CompoundKey.internal_key?(key) and
          do_get(state, Ferricstore.Store.CompoundKey.type_key(key)) != nil
      end

      defp ensure_string_key(state, key) do
        if compound_data_structure_key?(state, key),
          do: {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"},
          else: :ok
      end

      defp clear_compound_prefix_for_string_put(state, key, "hash"),
        do:
          do_compound_delete_prefix(
            state,
            key,
            Ferricstore.Store.CompoundKey.hash_prefix(key)
          )

      defp clear_compound_prefix_for_string_put(state, key, "list") do
        with :ok <-
               do_compound_delete_prefix(
                 state,
                 key,
                 Ferricstore.Store.CompoundKey.list_prefix(key)
               ) do
          do_compound_delete(state, key, Ferricstore.Store.CompoundKey.list_meta_key(key))
        end
      end

      defp clear_compound_prefix_for_string_put(state, key, "set"),
        do:
          do_compound_delete_prefix(
            state,
            key,
            Ferricstore.Store.CompoundKey.set_prefix(key)
          )

      defp clear_compound_prefix_for_string_put(state, key, "zset"),
        do:
          do_compound_delete_prefix(
            state,
            key,
            Ferricstore.Store.CompoundKey.zset_prefix(key)
          )

      defp clear_compound_prefix_for_string_put(state, key, "stream") do
        with :ok <-
               do_compound_delete_prefix(
                 state,
                 key,
                 Ferricstore.Store.CompoundKey.stream_prefix(key)
               ),
             :ok <-
               do_compound_delete_prefix(
                 state,
                 key,
                 Ferricstore.Store.CompoundKey.stream_group_prefix(key)
               ),
             :ok <-
               do_compound_delete(
                 state,
                 key,
                 Ferricstore.Store.CompoundKey.stream_meta_key(key)
               ) do
          queue_stream_cache_cleanup({state.instance_name, key})
        end
      end

      defp clear_compound_prefix_for_string_put(state, key, type)
           when type in ["bloom", "cms", "cuckoo", "topk"] do
        queue_pending_prob_delete(prob_path(state, key, prob_extension(type)))
      end

      defp clear_compound_prefix_for_string_put(_state, _key, _type), do: :ok

      # ---------------------------------------------------------------------------
      # Private: read from ETS with Bitcask fallback
      # ---------------------------------------------------------------------------

      # Reads a value from ETS, falling back to Bitcask for cold keys. Mirrors
      # the shard's `do_get/2` logic so that list operations can read current
      # state within the state machine.
      defp do_get(state, key) do
        case ets_lookup(state, key) do
          {:hit, value, _exp} -> value
          :expired -> nil
          :miss -> nil
        end
      end

      # Reads a value + expire_at_ms from ETS, falling back to Bitcask for cold
      # keys. Returns `{value, expire_at_ms}` or `nil`.
      defp do_get_meta(state, key) do
        case ets_lookup(state, key) do
          {:hit, value, exp} -> {value, exp}
          :expired -> nil
          :miss -> nil
        end
      end

      # ---------------------------------------------------------------------------
      # Private: HLC merging (spec 2G.6)
      # ---------------------------------------------------------------------------

      # Merges a remote HLC timestamp into the local node's HLC. This is a
      # side-effect that does not affect the deterministic state machine output.
      #
      # The merge is wrapped in a try/catch because the HLC GenServer may not be
      # running in unit tests that exercise the state machine in isolation.
      @spec merge_hlc(HLC.timestamp()) :: :ok
      defp merge_hlc(remote_ts) do
        HLC.update(remote_ts)
      rescue
        # HLC GenServer not running (e.g. unit tests without full app)
        _error -> :ok
      catch
        :exit, _reason -> :ok
      end

      # ---------------------------------------------------------------------------
      # Private: keydir binary memory tracking
      # ---------------------------------------------------------------------------

      # Tracks off-heap binary bytes when inserting/updating a key in ETS.
      # Computes delta: new_bytes - old_bytes (if key existed before).
      defp track_keydir_binary_delta(state, key, new_ets_val, new_expire_at_ms) do
        previous = safe_ets_lookup(state.ets, key)

        track_keydir_binary_delta_from_previous(
          state,
          key,
          previous,
          new_ets_val,
          new_expire_at_ms
        )
      end

      defp track_keydir_binary_delta_from_previous(
             state,
             key,
             previous,
             new_ets_val,
             new_expire_at_ms
           ) do
        ref = keydir_binary_ref(state)

        ExpiryTracker.adjust(
          expiry_instance_ctx(state),
          state.shard_index,
          ExpiryTracker.entry_expire_at(previous),
          new_expire_at_ms
        )

        if ref do
          new_bytes = binary_byte_size(key) + binary_byte_size(new_ets_val)

          old_bytes =
            case previous do
              [{^key, old_val, _, _, _, _, _}] ->
                binary_byte_size(key) + binary_byte_size(old_val)

              _ ->
                0
            end

          delta = new_bytes - old_bytes
          if delta != 0, do: :atomics.add(ref, state.shard_index + 1, delta)
        end
      end

      defp track_keydir_binary_delta_from_missing(state, key, new_ets_val, new_expire_at_ms) do
        ExpiryTracker.adjust(expiry_instance_ctx(state), state.shard_index, 0, new_expire_at_ms)

        if ref = keydir_binary_ref(state) do
          delta = binary_byte_size(key) + binary_byte_size(new_ets_val)
          if delta != 0, do: :atomics.add(ref, state.shard_index + 1, delta)
        end
      end

      # Tracks off-heap binary bytes when deleting a key from ETS.
      defp track_keydir_binary_remove(state, key) do
        ref = keydir_binary_ref(state)
        previous = safe_ets_lookup(state.ets, key)

        ExpiryTracker.adjust(
          expiry_instance_ctx(state),
          state.shard_index,
          ExpiryTracker.entry_expire_at(previous),
          0
        )

        if ref do
          bytes =
            case previous do
              [{^key, val, _, _, _, _, _}] ->
                binary_byte_size(key) + binary_byte_size(val)

              _ ->
                0
            end

          if bytes > 0, do: :atomics.sub(ref, state.shard_index + 1, bytes)
        end
      end

      # Tracks off-heap binary bytes when deleting a key whose value is already known.
      defp track_keydir_binary_remove_known(state, key, value) do
        ref = keydir_binary_ref(state)
        previous = safe_ets_lookup(state.ets, key)

        ExpiryTracker.adjust(
          expiry_instance_ctx(state),
          state.shard_index,
          ExpiryTracker.entry_expire_at(previous),
          0
        )

        if ref do
          bytes = binary_byte_size(key) + binary_byte_size(value)
          if bytes > 0, do: :atomics.sub(ref, state.shard_index + 1, bytes)
        end
      end

      defp track_keydir_binary_remove_entry(
             state,
             {key, value, expire_at_ms, _lfu, _file_id, _offset, _value_size}
           ) do
        ExpiryTracker.adjust(
          expiry_instance_ctx(state),
          state.shard_index,
          expire_at_ms,
          0
        )

        if ref = keydir_binary_ref(state) do
          bytes = binary_byte_size(key) + binary_byte_size(value)
          if bytes > 0, do: :atomics.sub(ref, state.shard_index + 1, bytes)
        end
      end

      # Tracks off-heap binary bytes when warming a cold key (nil -> value).
      defp track_keydir_binary_warm(state, new_ets_val) do
        ref = keydir_binary_ref(state)

        if ref do
          new_bytes = binary_byte_size(new_ets_val)
          if new_bytes > 0, do: :atomics.add(ref, state.shard_index + 1, new_bytes)
        end
      end

      defp keydir_binary_ref(%{instance_ctx: ctx} = state) when is_map(ctx) do
        keydir_binary_ref_from_ctx(ctx, metrics_shard_index(state))
      end

      defp keydir_binary_ref(%{instance_name: name} = state) when is_atom(name) do
        state
        |> instance_ctx_for_state()
        |> keydir_binary_ref_from_ctx(metrics_shard_index(state))
      end

      defp keydir_binary_ref(_state), do: nil

      defp expiry_instance_ctx(state), do: instance_ctx_for_state(state)

      defp metrics_shard_index(%{shard_index: shard_index}), do: shard_index
      defp metrics_shard_index(%{index: index}), do: index

      defp keydir_binary_ref_from_ctx(
             %{keydir_binary_bytes: ref, shard_count: count},
             shard_index
           )
           when ref != nil and shard_index < count do
        ref
      end

      defp keydir_binary_ref_from_ctx(_ctx, _shard_index), do: nil

      defp binary_byte_size(v) when is_binary(v) and byte_size(v) > 64, do: byte_size(v)
      defp binary_byte_size(_), do: 0

      # ---------------------------------------------------------------------------
      # Private: probabilistic data structure helpers
      # ---------------------------------------------------------------------------

      defp next_prob_mutation_token(state) do
        mutation_index = current_ra_index() || Map.get(state, :applied_count, 0) + 1
        mutation_ordinal = Process.get(:sm_prob_mutation_ordinal, 0) + 1

        if mutation_index in 0..18_446_744_073_709_551_615 and
             mutation_ordinal in 1..18_446_744_073_709_551_615 do
          Process.put(:sm_prob_mutation_ordinal, mutation_ordinal)
          {:ok, mutation_index, mutation_ordinal}
        else
          {:error, :invalid_probabilistic_mutation_token}
        end
      end

      # Shorthand for the common prob command pattern: bump applied count +
      # maybe release cursor.
      defp bump_applied(meta, state, result) do
        old_count = state.applied_count
        new_state = %{state | applied_count: old_count + 1}
        maybe_release_cursor(meta, old_count, new_state, result)
      end

      # Prob commands don't write to Bitcask log (they write to their own files),
      # so they use with_pending_writes to ensure any metadata puts are batched.
      defp do_prob_command(state, fun) do
        if Process.get(:sm_pending_writes, :undefined) == :undefined do
          with_pending_writes(state, fun)
        else
          fun.()
        end
      end

      defp apply_prob_lifecycle_locally(state, command, store_builder)
           when is_function(store_builder, 0) do
        store = store_builder.()
        lifecycle_id = prob_lifecycle_id(state, command)

        case prob_lifecycle_replay_type(store, command, lifecycle_id) do
          {:ok, type} ->
            replay_prob_lifecycle(state, store, command, type)

          :not_replay ->
            apply_new_prob_lifecycle(state, store, command, lifecycle_id)

          {:error, _reason} = error ->
            error
        end
      end

      defp apply_new_prob_lifecycle(state, store, command, lifecycle_id) do
        source = elem(command, 1)

        case prob_lifecycle_source(store, source) do
          {:ok, type, encoded_metadata, expire_at_ms} ->
            execute_prob_lifecycle(
              state,
              store,
              command,
              type,
              encoded_metadata,
              expire_at_ms,
              lifecycle_id
            )

          :not_prob ->
            :not_prob

          {:error, _reason} = error ->
            error
        end
      end

      defp prob_lifecycle_source(store, source) do
        case Ferricstore.Store.TypeRegistry.get_type(source, store) do
          type when type in ["bloom", "cms", "cuckoo", "topk"] ->
            with {:ok, type} <- prob_type_atom(type) do
              case Ferricstore.Store.Ops.get_meta(store, source) do
                {encoded_metadata, expire_at_ms}
                when is_binary(encoded_metadata) and is_integer(expire_at_ms) ->
                  {:ok, type, encoded_metadata, expire_at_ms}

                nil ->
                  {:error, {:prob_sidecar_apply_failed, :lifecycle, :missing_metadata}}

                {:error, _reason} = error ->
                  error

                _invalid ->
                  {:error, {:prob_sidecar_apply_failed, :lifecycle, :invalid_metadata}}
              end
            end

          {:error, {:storage_read_failed, _reason}} = failure ->
            Ferricstore.Store.ReadResult.command_error(failure)

          _non_prob ->
            :not_prob
        end
      end

      defp execute_prob_lifecycle(
             state,
             store,
             {:rename, source, destination} = command,
             type,
             encoded_metadata,
             expire_at_ms,
             lifecycle_id
           ) do
        cond do
          source == destination ->
            :ok

          true ->
            with :ok <- replace_prob_lifecycle_destination(store, destination, true),
                 {:ok, destination_path} <-
                   stage_prob_lifecycle_sidecar(state, store, source, destination, type, :rename),
                 :ok <-
                   write_prob_lifecycle_metadata(
                     state,
                     store,
                     destination,
                     type,
                     encoded_metadata,
                     expire_at_ms,
                     destination_path,
                     lifecycle_id
                   ),
                 :ok <- delete_prob_lifecycle_source(store, source) do
              :ok
            else
              {:error, _reason} = error -> normalize_prob_lifecycle_error(command, error)
            end
        end
      end

      defp execute_prob_lifecycle(
             state,
             store,
             {:renamenx, source, destination} = command,
             type,
             encoded_metadata,
             expire_at_ms,
             lifecycle_id
           ) do
        cond do
          source == destination ->
            0

          true ->
            case prob_lifecycle_destination_exists?(store, destination) do
              {:ok, true} ->
                0

              {:ok, false} ->
                with {:ok, destination_path} <-
                       stage_prob_lifecycle_sidecar(
                         state,
                         store,
                         source,
                         destination,
                         type,
                         :rename
                       ),
                     :ok <-
                       write_prob_lifecycle_metadata(
                         state,
                         store,
                         destination,
                         type,
                         encoded_metadata,
                         expire_at_ms,
                         destination_path,
                         lifecycle_id
                       ),
                     :ok <- delete_prob_lifecycle_source(store, source) do
                  1
                else
                  {:error, _reason} = error -> normalize_prob_lifecycle_error(command, error)
                end

              {:error, _reason} = error ->
                error
            end
        end
      end

      defp execute_prob_lifecycle(
             state,
             store,
             {:copy, source, destination, replace?} = command,
             type,
             encoded_metadata,
             expire_at_ms,
             lifecycle_id
           )
           when is_boolean(replace?) do
        cond do
          source == destination ->
            if replace?, do: 1, else: 0

          true ->
            case prob_lifecycle_destination_exists?(store, destination) do
              {:ok, true} when not replace? ->
                0

              {:ok, destination_exists?} ->
                with :ok <-
                       replace_prob_lifecycle_destination(
                         store,
                         destination,
                         destination_exists?
                       ),
                     {:ok, destination_path} <-
                       stage_prob_lifecycle_sidecar(
                         state,
                         store,
                         source,
                         destination,
                         type,
                         :copy
                       ),
                     :ok <-
                       write_prob_lifecycle_metadata(
                         state,
                         store,
                         destination,
                         type,
                         encoded_metadata,
                         expire_at_ms,
                         destination_path,
                         lifecycle_id
                       ) do
                  1
                else
                  {:error, _reason} = error -> normalize_prob_lifecycle_error(command, error)
                end

              {:error, _reason} = error ->
                error
            end
        end
      end

      defp replace_prob_lifecycle_destination(_store, _destination, false), do: :ok

      defp replace_prob_lifecycle_destination(store, destination, true) do
        case Ferricstore.Commands.Strings.Delete.do_del_key(destination, store) do
          result when result in [true, false, :ok] -> :ok
          {:error, _reason} = error -> error
        end
      end

      defp delete_prob_lifecycle_source(store, source) do
        case Ferricstore.Commands.Strings.Delete.do_del_key(source, store) do
          result when result in [true, :ok] -> :ok
          false -> {:error, {:prob_sidecar_apply_failed, :rename, :source_disappeared}}
          {:error, _reason} = error -> error
        end
      end

      defp prob_lifecycle_destination_exists?(store, destination) do
        case Ferricstore.Store.TypeRegistry.get_type(destination, store) do
          "none" ->
            {:ok, false}

          {:error, {:storage_read_failed, _reason}} = failure ->
            Ferricstore.Store.ReadResult.command_error(failure)

          _type ->
            {:ok, true}
        end
      end

      defp stage_prob_lifecycle_sidecar(state, store, source, destination, type, mode) do
        extension = prob_extension(type)
        source_path = prob_store_path(store, source, extension)
        destination_path = prob_store_canonical_path(store, destination, extension)
        create_path = pending_prob_create_path(destination_path)

        with :ok <- ensure_prob_lifecycle_dir(state, Path.dirname(destination_path)),
             :ok <- copy_prob_lifecycle_sidecar(source_path, create_path, mode) do
          record_pending_prob_lifecycle_create(create_path, destination_path)
          {:ok, destination_path}
        end
      end

      defp ensure_prob_lifecycle_dir(state, dir) do
        if Ferricstore.FS.exists?(dir) do
          :ok
        else
          case Ferricstore.FS.mkdir_p(dir) do
            :ok -> prob_fsync_dir(Path.dirname(dir), :create_prob_dir)
            {:error, reason} -> {:error, {:prob_dir_create_failed, reason}}
          end
        end
      end

      defp copy_prob_lifecycle_sidecar(source_path, create_path, :rename) do
        case NIF.fs_hard_link_replace_sync_nofollow(source_path, create_path) do
          :ok ->
            :ok

          {:error, {:cross_device, _reason}} ->
            copy_prob_lifecycle_sidecar(source_path, create_path, :copy)

          {:error, _reason} = error ->
            error
        end
      end

      defp copy_prob_lifecycle_sidecar(source_path, create_path, :copy) do
        NIF.fs_copy_replace_sync_nofollow(source_path, create_path)
      end

      defp write_prob_lifecycle_metadata(
             state,
             store,
             destination,
             type,
             encoded_metadata,
             expire_at_ms,
             destination_path,
             lifecycle_id
           ) do
        with {:ok, copied_metadata} <-
               encode_prob_lifecycle_metadata(
                 state,
                 encoded_metadata,
                 type,
                 destination_path,
                 lifecycle_id
               ),
             :ok <- Ferricstore.Store.Ops.put(store, destination, copied_metadata, expire_at_ms) do
          Ferricstore.Store.Ops.compound_put(
            store,
            destination,
            CompoundKey.type_key(destination),
            CompoundKey.encode_prob_type(type, prob_create_token(state)),
            0
          )
        end
      end

      defp encode_prob_lifecycle_metadata(
             state,
             encoded_metadata,
             type,
             destination_path,
             lifecycle_id
           ) do
        with {:ok, {tag, metadata} = decoded} <- Ferricstore.TermCodec.decode(encoded_metadata),
             true <- is_atom(tag) and is_map(metadata),
             ^type <- Ferricstore.Commands.ProbType.metadata_type(decoded) do
          metadata =
            metadata
            |> Map.put(:create_token, prob_create_token(state))
            |> Map.put(:lifecycle_id, lifecycle_id)
            |> maybe_replace_prob_metadata_path(destination_path)

          {:ok, Ferricstore.TermCodec.encode({tag, metadata})}
        else
          _invalid -> {:error, {:prob_sidecar_apply_failed, :lifecycle, :invalid_metadata}}
        end
      end

      defp maybe_replace_prob_metadata_path(metadata, destination_path) do
        if Map.has_key?(metadata, :path) do
          Map.put(metadata, :path, destination_path)
        else
          metadata
        end
      end

      defp prob_lifecycle_replay_type(store, command, lifecycle_id) do
        destination = elem(command, 2)

        case Ferricstore.Store.Ops.compound_get(
               store,
               destination,
               CompoundKey.type_key(destination)
             ) do
          {:error, {:storage_read_failed, _reason}} = failure ->
            Ferricstore.Store.ReadResult.command_error(failure)

          marker when is_binary(marker) ->
            prob_lifecycle_replay_metadata(store, destination, marker, lifecycle_id)

          _missing_or_invalid_marker ->
            :not_replay
        end
      end

      defp prob_lifecycle_replay_metadata(store, destination, marker, lifecycle_id) do
        with {:ok, {type, _token}} <- CompoundKey.decode_prob_type(marker),
             true <- type in [:bloom, :cms, :cuckoo, :topk] do
          case Ferricstore.Store.Ops.get_meta(store, destination) do
            {:error, {:storage_read_failed, _reason}} = failure ->
              Ferricstore.Store.ReadResult.command_error(failure)

            {encoded_metadata, expire_at_ms}
            when is_binary(encoded_metadata) and is_integer(expire_at_ms) ->
              decode_prob_lifecycle_replay_type(encoded_metadata, lifecycle_id, type)

            _missing_or_invalid_metadata ->
              :not_replay
          end
        else
          _invalid_marker -> :not_replay
        end
      end

      defp decode_prob_lifecycle_replay_type(encoded_metadata, lifecycle_id, type) do
        with {:ok, {_tag, metadata}} <- Ferricstore.TermCodec.decode(encoded_metadata),
             true <- is_map(metadata),
             ^lifecycle_id <- Map.get(metadata, :lifecycle_id) do
          {:ok, type}
        else
          _not_replay -> :not_replay
        end
      end

      if Mix.env() == :test do
        def __prob_lifecycle_replay_type_for_test__(store, command, lifecycle_id) do
          prob_lifecycle_replay_type(store, command, lifecycle_id)
        end
      end

      defp replay_prob_lifecycle(state, store, command, type) do
        source = elem(command, 1)
        destination = elem(command, 2)
        mode = if elem(command, 0) in [:rename, :renamenx], do: :rename, else: :copy

        with :ok <-
               repair_prob_lifecycle_destination(state, store, source, destination, type, mode),
             :ok <- maybe_replay_prob_source_delete(store, command, source, type) do
          case elem(command, 0) do
            :rename -> :ok
            :renamenx -> 1
            :copy -> 1
          end
        end
      end

      defp repair_prob_lifecycle_destination(state, store, source, destination, type, mode) do
        extension = prob_extension(type)
        destination_path = prob_store_canonical_path(store, destination, extension)
        create_path = pending_prob_create_path(destination_path)

        cond do
          Ferricstore.FS.exists?(create_path) ->
            record_pending_prob_lifecycle_create(create_path, destination_path)
            :ok

          Ferricstore.FS.exists?(destination_path) ->
            queue_pending_prob_delete_path(prob_mutation_receipt_path(destination_path))
            prob_fsync_dir(Path.dirname(destination_path), :publish_prob_file)

          true ->
            source_path = prob_store_path(store, source, extension)

            with :ok <- ensure_prob_lifecycle_dir(state, Path.dirname(destination_path)),
                 :ok <- copy_prob_lifecycle_sidecar(source_path, create_path, mode) do
              record_pending_prob_lifecycle_create(create_path, destination_path)
              :ok
            end
        end
      end

      defp maybe_replay_prob_source_delete(
             store,
             {operation, _source, _destination},
             source,
             type
           )
           when operation in [:rename, :renamenx] do
        source_path = prob_store_canonical_path(store, source, prob_extension(type))
        queue_pending_prob_delete(source_path)
        queue_pending_prob_delete(pending_prob_create_path(source_path))

        :ok
      end

      defp maybe_replay_prob_source_delete(_store, _command, _source, _type), do: :ok

      defp prob_store_path(store, key, extension) do
        final_path = prob_store_canonical_path(store, key, extension)

        case Process.get(:sm_pending_prob_paths) do
          %{by_final: paths} -> Map.get(paths, final_path, final_path)
          _not_pending -> final_path
        end
      end

      defp prob_store_canonical_path(store, key, extension) do
        prob_dir = store.prob_dir_for.(key)
        Ferricstore.ProbFile.path(prob_dir, key, extension)
      end

      defp prob_lifecycle_id(state, command) do
        digest =
          command
          |> :erlang.term_to_binary(minor_version: 2)
          |> then(&:crypto.hash(:sha256, &1))
          |> binary_part(0, 16)

        {prob_create_token(state), digest}
      end

      defp prob_type_atom("bloom"), do: {:ok, :bloom}
      defp prob_type_atom("cms"), do: {:ok, :cms}
      defp prob_type_atom("cuckoo"), do: {:ok, :cuckoo}
      defp prob_type_atom("topk"), do: {:ok, :topk}
      defp prob_type_atom(_type), do: {:error, :invalid_probabilistic_type}

      defp normalize_prob_lifecycle_error(command, {:error, reason}) do
        if Ferricstore.Raft.ApplyFailure.storage_reason?(reason) do
          {:error, reason}
        else
          {:error, {:prob_sidecar_apply_failed, elem(command, 0), reason}}
        end
      end

      defp prob_path(state, key, ext) do
        final_path = prob_canonical_path(state, key, ext)

        case Process.get(:sm_pending_prob_paths) do
          %{by_final: paths} -> Map.get(paths, final_path, final_path)
          _not_in_pending_apply -> final_path
        end
      end

      defp prob_canonical_path(state, key, ext),
        do: Ferricstore.ProbFile.path(prob_dir(state), key, ext)

      defp pending_prob_create_path(final_path), do: final_path <> ".pending-create"

      defp cms_source_paths(state, src_keys) do
        Enum.map(src_keys, &prob_path(state, &1, "cms"))
      end

      defp validate_cms_merge_locality(
             state,
             dst_key,
             src_keys,
             weights,
             create_params
           ) do
        case instance_ctx_for_state(state) do
          %FerricStore.Instance{} = ctx ->
            command = {:cms_merge, dst_key, src_keys, weights, create_params}

            case Router.prob_write_route(ctx, command) do
              {:ok, ^dst_key, idx} when idx == state.shard_index ->
                :ok

              {:ok, ^dst_key, _other_idx} ->
                {:error, "CROSSSLOT CMS.MERGE keys must hash to the same shard"}

              {:error, _reason} = error ->
                error
            end

          _single_shard_state ->
            :ok
        end
      end

      # Returns the prob directory for this shard.
      defp prob_dir(%{shard_data_path: shard_data_path}) do
        Path.join(shard_data_path, "prob")
      end

      # Ensures the prob directory exists. Fsyncs parent on first create
      # so the new dir's entry survives kernel panic.
      defp ensure_prob_dir(state) do
        dir = prob_dir(state)

        if Ferricstore.FS.exists?(dir) do
          :ok
        else
          case Ferricstore.FS.mkdir_p(dir) do
            :ok -> prob_fsync_dir(Path.dirname(dir), :create_prob_dir)
            {:error, reason} -> {:error, {:prob_dir_create_failed, reason}}
          end
        end
      end

      defp prob_fsync_dir(path, phase) do
        result =
          case Process.get(:ferricstore_prob_fsync_dir_hook) do
            fun when is_function(fun, 1) -> fun.(path)
            _ -> NIF.v2_fsync_dir(path)
          end

        case result do
          :ok ->
            :ok

          {:error, reason} ->
            Logger.error(
              "StateMachine probabilistic directory fsync failed during #{phase} for #{path}: #{inspect(reason)}"
            )

            {:error, {:fsync_dir_failed, phase, reason}}
        end
      end

      defp create_bloom_metadata(state, key, num_bits, num_hashes, prob_meta) do
        final_path = prob_canonical_path(state, key, "bloom")
        create_path = pending_prob_create_path(final_path)

        with :ok <- validate_bloom_dimensions(num_bits, num_hashes),
             :ok <-
               ensure_prob_create_allowed(state, key, :bloom, "ERR item exists", create_path),
             :ok <- ensure_prob_dir(state),
             :ok <-
               prob_create_and_fsync(
                 state,
                 create_path,
                 final_path,
                 NIF.bloom_file_create(create_path, num_bits, num_hashes)
               ) do
          put_prob_metadata(
            state,
            key,
            :bloom,
            bloom_meta_with_path(prob_meta, final_path),
            0
          )
        end
      end

      defp bloom_meta_with_path({:bloom_meta, meta}, path) when is_map(meta) do
        {:bloom_meta, Map.put(meta, :path, path)}
      end

      defp bloom_meta_with_path(_prob_meta, path), do: {:bloom_meta, %{path: path}}

      defp put_prob_metadata(state, key, type, metadata, expire_at_ms) do
        previous = Process.get(:sm_prob_metadata_put?, :undefined)
        Process.put(:sm_prob_metadata_put?, true)
        encoded_metadata = encode_prob_metadata(state, metadata)

        try do
          with :ok <- do_put(state, key, encoded_metadata, expire_at_ms) do
            do_compound_put(
              state,
              key,
              CompoundKey.type_key(key),
              CompoundKey.encode_prob_type(type, prob_create_token(state)),
              0
            )
          end
        after
          case previous do
            :undefined -> Process.delete(:sm_prob_metadata_put?)
            value -> Process.put(:sm_prob_metadata_put?, value)
          end
        end
      end

      defp encode_prob_metadata(state, {tag, metadata}) when is_atom(tag) and is_map(metadata) do
        metadata
        |> Map.put(:create_token, prob_create_token(state))
        |> then(&Ferricstore.TermCodec.encode({tag, &1}))
      end

      defp prob_create_token(state) do
        current_ra_index() || -(Map.get(state, :applied_count, 0) + 1)
      end

      defp ensure_prob_create_allowed(
             state,
             key,
             expected_type,
             exists_error,
             _pending_path
           ) do
        case classify_prob_key(state, key, expected_type) do
          {:ok, :missing} ->
            :ok

          {:ok, :existing} ->
            {:error, exists_error}

          {:ok, :replay} ->
            :ok

          {:error, _reason} = error ->
            error
        end
      end

      defp classify_prob_key(state, key, expected_type) do
        case prob_type_marker_with_token(state, key) do
          nil ->
            classify_untyped_prob_key(state, key)

          {^expected_type, create_token} ->
            cond do
              pending_prob_create?(state, key, expected_type) ->
                {:ok, :existing}

              create_token == prob_create_token(state) ->
                {:ok, :replay}

              true ->
                {:ok, :existing}
            end

          _other ->
            {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
        end
      end

      defp pending_prob_create?(state, key, type) do
        final_path = prob_canonical_path(state, key, prob_extension(type))

        case Process.get(:sm_pending_prob_paths) do
          %{by_final: %{^final_path => _create_path}} -> true
          _not_pending -> false
        end
      end

      defp classify_untyped_prob_key(state, key) do
        if is_nil(do_get(state, key)) do
          {:ok, :missing}
        else
          {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
        end
      end

      defp require_existing_prob_key(state, key, expected_type) do
        case classify_prob_key(state, key, expected_type) do
          {:ok, status} when status in [:existing, :replay] -> :ok
          {:ok, :missing} -> {:error, :enoent}
          {:error, _reason} = error -> error
        end
      end

      defp require_existing_prob_keys(state, keys, expected_type) do
        Enum.reduce_while(keys, :ok, fn key, :ok ->
          case require_existing_prob_key(state, key, expected_type) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp prob_type_marker(state, key) do
        case prob_type_marker_with_token(state, key) do
          {type, _create_token} -> type
          missing_or_other -> missing_or_other
        end
      end

      defp prob_type_marker_with_token(state, key) do
        state
        |> do_get(CompoundKey.type_key(key))
        |> prob_type_marker_data()
      end

      defp prob_type_marker_data(nil), do: nil

      defp prob_type_marker_data(marker) do
        case CompoundKey.decode_prob_type(marker) do
          {:ok, type_and_token} -> type_and_token
          :error -> :other
        end
      end

      defp prob_type_from_marker(marker) do
        case prob_type_marker_data(marker) do
          {type, _create_token} -> type
          missing_or_other -> missing_or_other
        end
      end

      defp validate_bloom_dimensions(num_bits, num_hashes) do
        case ProbParameters.validate_bloom_dimensions(num_bits, num_hashes) do
          :ok -> :ok
          {:error, _reason} -> {:error, :invalid_bloom_dimensions}
        end
      end

      defp create_cms_metadata(state, key, width, depth) do
        final_path = prob_canonical_path(state, key, "cms")
        create_path = pending_prob_create_path(final_path)

        with :ok <- validate_cms_dimensions(width, depth),
             :ok <-
               ensure_prob_create_allowed(
                 state,
                 key,
                 :cms,
                 "ERR item already exists",
                 create_path
               ),
             :ok <- ensure_prob_dir(state),
             :ok <-
               prob_create_and_fsync(
                 state,
                 create_path,
                 final_path,
                 NIF.cms_file_create(create_path, width, depth)
               ) do
          meta_val = {:cms_meta, %{width: width, depth: depth}}
          put_prob_metadata(state, key, :cms, meta_val, 0)
        end
      end

      defp maybe_create_cms_merge_dst(
             _state,
             dst_path,
             _dst_key,
             :existing,
             _width,
             _depth
           ) do
        if Ferricstore.FS.exists?(dst_path), do: {:ok, dst_path}, else: {:error, :enoent}
      end

      defp maybe_create_cms_merge_dst(
             state,
             final_path,
             dst_key,
             :replay,
             width,
             depth
           ) do
        if Ferricstore.FS.exists?(final_path) do
          {:ok, final_path}
        else
          maybe_create_cms_merge_dst(state, final_path, dst_key, :missing, width, depth)
        end
      end

      defp maybe_create_cms_merge_dst(state, final_path, dst_key, status, width, depth)
           when status == :missing do
        create_path = pending_prob_create_path(final_path)

        with :ok <-
               prob_create_and_fsync(
                 state,
                 create_path,
                 final_path,
                 NIF.cms_file_create(create_path, width, depth)
               ) do
          meta_val = {:cms_meta, %{width: width, depth: depth}}

          with :ok <-
                 put_prob_metadata(
                   state,
                   dst_key,
                   :cms,
                   meta_val,
                   0
                 ) do
            {:ok, create_path}
          end
        end
      end

      defp validate_cms_create_params(%{width: width, depth: depth}) do
        with :ok <- validate_cms_dimensions(width, depth) do
          {:ok, width, depth}
        end
      end

      defp validate_cms_create_params(_invalid),
        do: {:error, :invalid_cms_dimensions}

      defp validate_cms_merge_apply_work(src_keys, width, depth) when is_list(src_keys) do
        ProbParameters.validate_cms_merge_work(length(src_keys), width, depth)
      end

      defp validate_cms_merge_apply_work(_src_keys, _width, _depth),
        do: {:error, :cms_merge_source_limit_exceeded}

      defp validate_cms_dimensions(width, depth) do
        case ProbParameters.validate_cms_dimensions(width, depth) do
          :ok -> :ok
          {:error, _reason} -> {:error, :invalid_cms_dimensions}
        end
      end

      defp create_cuckoo_metadata(state, key, capacity, bucket_size) do
        final_path = prob_canonical_path(state, key, "cuckoo")
        create_path = pending_prob_create_path(final_path)

        with :ok <- validate_cuckoo_parameters(capacity, bucket_size),
             :ok <-
               ensure_prob_create_allowed(
                 state,
                 key,
                 :cuckoo,
                 "ERR item exists",
                 create_path
               ),
             :ok <- ensure_prob_dir(state),
             :ok <-
               prob_create_and_fsync(
                 state,
                 create_path,
                 final_path,
                 NIF.cuckoo_file_create(create_path, capacity, bucket_size)
               ) do
          meta_val = {:cuckoo_meta, %{capacity: capacity}}
          put_prob_metadata(state, key, :cuckoo, meta_val, 0)
        end
      end

      defp validate_cuckoo_parameters(capacity, bucket_size) do
        case ProbParameters.validate_cuckoo_parameters(capacity, bucket_size) do
          :ok -> :ok
          {:error, _reason} -> {:error, :invalid_cuckoo_parameters}
        end
      end

      defp create_topk_metadata(state, key, k, width, depth) do
        final_path = prob_canonical_path(state, key, "topk")
        create_path = pending_prob_create_path(final_path)

        with :ok <- validate_topk_parameters(k, width, depth),
             :ok <-
               ensure_prob_create_allowed(
                 state,
                 key,
                 :topk,
                 "ERR item already exists",
                 create_path
               ),
             :ok <- ensure_prob_dir(state),
             :ok <-
               prob_create_and_fsync(
                 state,
                 create_path,
                 final_path,
                 NIF.topk_file_create_v2(create_path, k, width, depth)
               ) do
          meta_val = {:topk_meta, %{path: final_path, k: k, width: width, depth: depth}}
          put_prob_metadata(state, key, :topk, meta_val, 0)
        end
      end

      defp validate_topk_parameters(k, width, depth) do
        case ProbParameters.validate_topk_parameters(k, width, depth) do
          :ok -> :ok
          {:error, _reason} -> {:error, :invalid_topk_parameters}
        end
      end

      defp prob_create_and_fsync(state, create_path, final_path, create_result) do
        case normalize_prob_create_result(create_result) do
          :ok ->
            # Native sidecar creation syncs both the file and its parent before
            # returning. The apply boundary only needs to sync the later rename.
            record_pending_prob_create(create_path, final_path)
            :ok

          {:error, reason} ->
            cleanup_created_prob_file(state, create_path)
            {:error, {:prob_sidecar_create_failed, reason}}
        end
      end

      if Mix.env() == :test do
        def __prob_create_and_fsync_for_test__(state, create_path, final_path, create_result) do
          prob_create_and_fsync(state, create_path, final_path, create_result)
        end
      end

      defp record_pending_prob_create(create_path, final_path)
           when is_binary(create_path) and is_binary(final_path) do
        creates = Process.get(:sm_pending_prob_creates, %{})
        Process.put(:sm_pending_prob_creates, Map.put(creates, final_path, create_path))

        paths = Process.get(:sm_pending_prob_paths, %{by_final: %{}, by_create: %{}})

        Process.put(:sm_pending_prob_paths, %{
          by_final: Map.put(paths.by_final, final_path, create_path),
          by_create: Map.put(paths.by_create, create_path, final_path)
        })

        case Process.get(:sm_pending_prob_deletes) do
          %MapSet{} = deletes ->
            deletes =
              deletes
              |> MapSet.delete(final_path)
              |> MapSet.delete(create_path)
              |> MapSet.delete(prob_mutation_receipt_path(final_path))
              |> MapSet.delete(prob_mutation_receipt_path(create_path))

            Process.put(:sm_pending_prob_deletes, deletes)

          _not_in_pending_apply ->
            :ok
        end
      end

      defp record_pending_prob_lifecycle_create(create_path, final_path) do
        record_pending_prob_create(create_path, final_path)
        queue_pending_prob_delete_path(prob_mutation_receipt_path(final_path))
      end

      defp queue_pending_prob_delete(nil), do: :ok

      defp queue_pending_prob_delete(path) when is_binary(path) do
        case {
          Process.get(:sm_pending_prob_deletes),
          Process.get(:sm_pending_prob_creates, %{}),
          Process.get(:sm_pending_prob_paths)
        } do
          {%MapSet{} = pending, %{} = creates, %{by_create: by_create, by_final: by_final}} ->
            case Map.get(by_create, path) do
              final_path when is_binary(final_path) ->
                Process.put(:sm_pending_prob_creates, Map.delete(creates, final_path))

                pending =
                  put_pending_prob_delete_paths(pending, [
                    path,
                    final_path,
                    prob_mutation_receipt_path(path),
                    prob_mutation_receipt_path(final_path)
                  ])

                Process.put(:sm_pending_prob_deletes, pending)

              nil ->
                if Map.has_key?(by_final, path) do
                  # The tombstone captured the old canonical path before a
                  # replacement was staged. Publishing the replacement owns
                  # that path now; only generation-specific cleanup may remain.
                  :ok
                else
                  pending =
                    put_pending_prob_delete_paths(pending, [
                      path,
                      prob_mutation_receipt_path(path)
                    ])

                  Process.put(:sm_pending_prob_deletes, pending)
                end
            end

          _not_in_pending_apply ->
            :ok
        end

        :ok
      end

      defp put_pending_prob_delete_paths(pending, paths) do
        Enum.reduce(paths, pending, &MapSet.put(&2, &1))
      end

      defp queue_pending_prob_delete_path(path) when is_binary(path) do
        case Process.get(:sm_pending_prob_deletes) do
          %MapSet{} = pending -> Process.put(:sm_pending_prob_deletes, MapSet.put(pending, path))
          _not_in_pending_apply -> :ok
        end

        :ok
      end

      defp prob_mutation_receipt_path(path) when is_binary(path), do: path <> ".mutation"

      defp publish_pending_prob_creates do
        creates =
          :sm_pending_prob_creates
          |> Process.get(%{})
          |> Enum.sort_by(&elem(&1, 0))

        with :ok <- rename_pending_prob_creates(creates) do
          fsync_pending_prob_create_dirs(creates)
        end
      end

      defp rename_pending_prob_creates(creates) do
        Enum.reduce_while(creates, :ok, fn {final_path, create_path}, :ok ->
          with :ok <- Ferricstore.FS.rename(create_path, final_path),
               :ok <- publish_pending_prob_receipt(create_path, final_path) do
            {:cont, :ok}
          else
            {:error, reason} ->
              {:halt, prob_sidecar_publish_error(final_path, create_path, reason)}
          end
        end)
      end

      defp publish_pending_prob_receipt(create_path, final_path) do
        create_receipt = prob_mutation_receipt_path(create_path)
        final_receipt = prob_mutation_receipt_path(final_path)

        case Ferricstore.FS.rename(create_receipt, final_receipt) do
          :ok ->
            :ok

          {:error, {:not_found, _message}} ->
            case Ferricstore.FS.rm(final_receipt) do
              :ok -> :ok
              {:error, {:not_found, _message}} -> :ok
              {:error, reason} -> {:error, {:delete_stale_prob_receipt_failed, reason}}
            end

          {:error, reason} ->
            {:error, {:publish_prob_receipt_failed, reason}}
        end
      end

      defp fsync_pending_prob_create_dirs(creates) do
        creates
        |> Enum.reduce(%{}, fn {final_path, create_path}, dirs ->
          Map.put_new(dirs, Path.dirname(final_path), {final_path, create_path})
        end)
        |> Enum.sort_by(&elem(&1, 0))
        |> Enum.reduce_while(:ok, fn {dir, {final_path, create_path}}, :ok ->
          case prob_fsync_dir(dir, :publish_prob_file) do
            :ok ->
              {:cont, :ok}

            {:error, reason} ->
              {:halt, prob_sidecar_publish_error(final_path, create_path, reason)}
          end
        end)
      end

      defp prob_sidecar_publish_error(final_path, create_path, reason) do
        {:error, {:prob_sidecar_publish_failed, final_path, create_path, reason}}
      end

      defp publish_pending_prob_deletes(state) do
        deletes =
          :sm_pending_prob_deletes
          |> Process.get(MapSet.new())
          |> Enum.sort()

        with :ok <- remove_pending_prob_files(state, deletes) do
          fsync_pending_prob_delete_dirs(state, deletes)
        end
      end

      defp remove_pending_prob_files(state, deletes) do
        Enum.reduce_while(deletes, :ok, fn path, :ok ->
          case Ferricstore.FS.rm(path) do
            :ok ->
              {:cont, :ok}

            {:error, {:not_found, _message}} ->
              {:cont, :ok}

            {:error, reason} ->
              {:halt, prob_sidecar_delete_error(state, path, {:delete_prob_file_failed, reason})}
          end
        end)
      rescue
        error ->
          path = List.first(deletes)
          prob_sidecar_delete_error(state, path, {:delete_prob_file_exception, error})
      catch
        :exit, reason ->
          path = List.first(deletes)
          prob_sidecar_delete_error(state, path, {:delete_prob_file_exit, reason})
      end

      defp fsync_pending_prob_delete_dirs(state, deletes) do
        deletes
        |> Enum.reduce(%{}, fn path, dirs ->
          Map.put_new(dirs, Path.dirname(path), path)
        end)
        |> Enum.sort_by(&elem(&1, 0))
        |> Enum.reduce_while(:ok, fn {dir, path}, :ok ->
          case prob_fsync_dir(dir, :delete_prob_files) do
            :ok ->
              {:cont, :ok}

            {:error, reason} ->
              {:halt, prob_sidecar_delete_error(state, path, reason)}
          end
        end)
      end

      defp prob_sidecar_delete_error(state, path, reason) do
        emit_prob_sidecar_delete_failed(state, path, reason)
        {:error, {:prob_sidecar_delete_failed, path, reason}}
      end

      defp cleanup_created_prob_file(state, path) when is_binary(path) do
        try do
          case Ferricstore.FS.rm(path) do
            :ok ->
              _ = prob_fsync_dir(Path.dirname(path), :rollback_prob_file_create)
              :ok

            {:error, {:not_found, _}} ->
              :ok

            {:error, _reason} = error ->
              error
          end
        catch
          kind, reason -> {:error, {kind, reason}}
        end
        |> case do
          :ok ->
            :ok

          {:error, reason} ->
            Logger.warning(
              "StateMachine probabilistic sidecar rollback failed for #{path}: #{inspect(reason)}"
            )

            emit_prob_sidecar_delete_failed(state, path, {:rollback_prob_file_create, reason})
            :ok
        end
      end

      defp normalize_prob_create_result(:ok), do: :ok
      defp normalize_prob_create_result({:ok, :ok}), do: :ok
      defp normalize_prob_create_result({:error, _reason} = error), do: error
      defp normalize_prob_create_result(other), do: {:error, {:unexpected_prob_nif_result, other}}

      defp normalize_prob_mutation_result(operation, {:error, reason} = error) do
        if prob_mutation_semantic_error?(operation, reason) do
          error
        else
          {:error, {:prob_sidecar_apply_failed, operation, reason}}
        end
      end

      defp normalize_prob_mutation_result(_operation, result), do: result

      defp prob_mutation_semantic_error?(operation, reason)
           when operation in [:cuckoo_add, :cuckoo_addnx] do
        reason == "filter is full"
      end

      defp prob_mutation_semantic_error?(:cms_incrby, reason) when is_binary(reason) do
        String.starts_with?(reason, ["CMS counter overflow", "CMS total count overflow"])
      end

      defp prob_mutation_semantic_error?(:cms_merge, reason) when is_binary(reason) do
        reason in [
          "src_paths and weights must have the same length",
          "width/depth mismatch"
        ] or String.starts_with?(reason, ["CMS counter overflow", "CMS total count overflow"])
      end

      defp prob_mutation_semantic_error?(operation, reason)
           when operation in [:topk_add, :topk_incrby] and is_binary(reason) do
        reason == "TopK increment must be positive" or
          String.starts_with?(reason, [
            "TopK element length ",
            "TopK CMS counter overflow"
          ])
      end

      defp prob_mutation_semantic_error?(_operation, _reason), do: false

      # Auto-creates a bloom filter file if it doesn't exist.
      defp validate_bloom_auto_create_params(nil), do: {:ok, nil}

      defp validate_bloom_auto_create_params(
             %{num_bits: num_bits, num_hashes: num_hashes} = params
           ) do
        with :ok <- validate_bloom_dimensions(num_bits, num_hashes) do
          {:ok, {num_bits, num_hashes, params}}
        end
      end

      defp validate_bloom_auto_create_params(_invalid),
        do: {:error, :invalid_bloom_dimensions}

      defp auto_create_bloom_if_needed(_state, path, _key, :existing, _validated_params) do
        if Ferricstore.FS.exists?(path), do: {:ok, path}, else: {:error, :enoent}
      end

      defp auto_create_bloom_if_needed(_state, _path, _key, :missing, nil),
        do: {:error, :enoent}

      defp auto_create_bloom_if_needed(
             state,
             final_path,
             key,
             :replay,
             validated_params
           ) do
        if Ferricstore.FS.exists?(final_path) do
          {:ok, final_path}
        else
          auto_create_bloom_if_needed(state, final_path, key, :missing, validated_params)
        end
      end

      defp auto_create_bloom_if_needed(
             state,
             final_path,
             key,
             status,
             {num_bits, num_hashes, params}
           )
           when status == :missing do
        create_path = pending_prob_create_path(final_path)

        with :ok <-
               prob_create_and_fsync(
                 state,
                 create_path,
                 final_path,
                 NIF.bloom_file_create(create_path, num_bits, num_hashes)
               ) do
          meta_val = {:bloom_meta, Map.put(params, :path, final_path)}

          with :ok <-
                 put_prob_metadata(
                   state,
                   key,
                   :bloom,
                   meta_val,
                   0
                 ) do
            {:ok, create_path}
          end
        end
      end

      # Applies a prob command locally (used in cross-shard tx context where
      # the state machine is already running inside Raft apply).
      defp apply_prob_locally(instance_ctx, command) do
        # In cross-shard tx, prob commands go through Router.prob_write
        # which routes to the correct shard's Raft group.
        Router.prob_write(instance_ctx, command)
      end

      # Auto-creates a cuckoo filter file if it doesn't exist.
      defp validate_cuckoo_auto_create_params(nil), do: {:ok, nil}

      defp validate_cuckoo_auto_create_params(%{capacity: capacity, bucket_size: bucket_size}) do
        with :ok <- validate_cuckoo_parameters(capacity, bucket_size) do
          {:ok, {capacity, bucket_size}}
        end
      end

      defp validate_cuckoo_auto_create_params(_invalid),
        do: {:error, :invalid_cuckoo_parameters}

      defp auto_create_cuckoo_if_needed(_state, path, _key, :existing, _validated_params) do
        if Ferricstore.FS.exists?(path), do: {:ok, path}, else: {:error, :enoent}
      end

      defp auto_create_cuckoo_if_needed(_state, _path, _key, :missing, nil),
        do: {:error, :enoent}

      defp auto_create_cuckoo_if_needed(
             state,
             final_path,
             key,
             :replay,
             validated_params
           ) do
        if Ferricstore.FS.exists?(final_path) do
          {:ok, final_path}
        else
          auto_create_cuckoo_if_needed(state, final_path, key, :missing, validated_params)
        end
      end

      defp auto_create_cuckoo_if_needed(
             state,
             final_path,
             key,
             :missing,
             {capacity, bucket_size}
           ) do
        create_path = pending_prob_create_path(final_path)

        with :ok <-
               prob_create_and_fsync(
                 state,
                 create_path,
                 final_path,
                 NIF.cuckoo_file_create(create_path, capacity, bucket_size)
               ) do
          meta_val = {:cuckoo_meta, %{capacity: capacity}}

          with :ok <-
                 put_prob_metadata(
                   state,
                   key,
                   :cuckoo,
                   meta_val,
                   0
                 ) do
            {:ok, create_path}
          end
        end
      end

      defp prob_file_path_for_delete(state, key) do
        if CompoundKey.internal_key?(key) do
          nil
        else
          case prob_type_marker(state, key) do
            type when type in [:bloom, :cms, :cuckoo, :topk] ->
              prob_path(state, key, prob_extension(type))

            _missing_or_non_prob ->
              nil
          end
        end
      end

      defp prob_extension(:bloom), do: "bloom"
      defp prob_extension(:cms), do: "cms"
      defp prob_extension(:cuckoo), do: "cuckoo"
      defp prob_extension(:topk), do: "topk"
      defp prob_extension(type) when is_binary(type), do: type

      defp maybe_delete_prob_file_path(_state, path), do: queue_pending_prob_delete(path)

      defp emit_prob_sidecar_delete_failed(state, path, reason) do
        :telemetry.execute(
          [:ferricstore, :prob, :sidecar_delete_failed],
          %{count: 1},
          %{shard_index: state.shard_index, path: path, reason: reason}
        )
      end
    end
  end
end
