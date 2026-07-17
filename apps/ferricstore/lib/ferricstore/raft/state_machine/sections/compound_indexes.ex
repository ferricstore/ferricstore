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

            sm_tx_drop_pending(compound_key)
            deleted = Process.get(:tx_deleted_keys, MapSet.new())
            Process.put(:tx_deleted_keys, MapSet.put(deleted, compound_key))
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

      defp queue_compound_indexes_put_after_flush(state, redis_key, compound_key, value) do
        _ = queue_compound_member_index_op(state, {:put, compound_key})
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
             {operation, compound_key}
           )
           when index != nil and operation in [:put, :delete] do
        pending = Process.get(:sm_pending_compound_member_index_ops, [])

        Process.put(:sm_pending_compound_member_index_ops, [
          {operation, index, compound_key} | pending
        ])

        :ok
      end

      defp queue_compound_member_index_op(_state, _operation), do: :ok

      defp flush_pending_compound_member_indexes do
        :sm_pending_compound_member_index_ops
        |> Process.put([])
        |> Enum.reverse()
        |> Enum.each(fn
          {:put, index, compound_key} -> CompoundMemberIndex.put(index, compound_key)
          {:delete, index, compound_key} -> CompoundMemberIndex.delete(index, compound_key)
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
        if Ferricstore.Store.CompoundKey.internal_key?(key) do
          :ok
        else
          type_key = Ferricstore.Store.CompoundKey.type_key(key)

          case string_put_compound_marker(state, type_key) do
            nil ->
              :ok

            type ->
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

      defp prob_path(state, key, ext) do
        Ferricstore.ProbFile.path(prob_dir(state), key, ext)
      end

      defp cms_source_paths(state, src_keys) do
        Enum.map(src_keys, &prob_path_for_key(state, &1, "cms"))
      end

      defp prob_path_for_key(state, key, ext) do
        prob_dir =
          case instance_ctx_for_state(state) do
            %FerricStore.Instance{} = ctx ->
              idx = Router.shard_for(ctx, key)
              shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
              Path.join(shard_path, "prob")

            _ ->
              prob_dir(state)
          end

        Ferricstore.ProbFile.path(prob_dir, key, ext)
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
          Ferricstore.FS.mkdir_p!(dir)
          prob_fsync_dir(Path.dirname(dir), :create_prob_dir)
        end
      end

      # Called immediately after a `*_file_create` NIF to make the new
      # filename entry durable. The NIF already fsynced the file's data;
      # this fsyncs the directory so the entry itself is durable.
      defp prob_fsync_dir(state) do
        prob_fsync_dir(prob_dir(state), :prob_file_dir)
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
        path = prob_path(state, key, "bloom")

        with :ok <- ensure_prob_dir(state),
             :ok <-
               prob_create_and_fsync(
                 state,
                 path,
                 NIF.bloom_file_create(path, num_bits, num_hashes)
               ) do
          do_put(
            state,
            key,
            Ferricstore.TermCodec.encode(bloom_meta_with_path(prob_meta, path)),
            0
          )

          :ok
        end
      end

      defp bloom_meta_with_path({:bloom_meta, meta}, path) when is_map(meta) do
        {:bloom_meta, Map.put(meta, :path, path)}
      end

      defp bloom_meta_with_path(_prob_meta, path), do: {:bloom_meta, %{path: path}}

      defp create_cms_metadata(state, key, width, depth) do
        path = prob_path(state, key, "cms")

        with :ok <- ensure_prob_dir(state),
             :ok <- prob_create_and_fsync(state, path, NIF.cms_file_create(path, width, depth)) do
          meta_val = {:cms_meta, %{width: width, depth: depth}}
          do_put(state, key, Ferricstore.TermCodec.encode(meta_val), 0)
          :ok
        end
      end

      defp maybe_create_cms_merge_dst(state, dst_path, dst_key, create_params) do
        if Ferricstore.FS.exists?(dst_path) do
          :ok
        else
          %{width: width, depth: depth} = create_params

          with :ok <-
                 prob_create_and_fsync(
                   state,
                   dst_path,
                   NIF.cms_file_create(dst_path, width, depth)
                 ) do
            meta_val = {:cms_meta, %{width: width, depth: depth}}
            do_put(state, dst_key, Ferricstore.TermCodec.encode(meta_val), 0)
            :ok
          end
        end
      end

      defp create_cuckoo_metadata(state, key, capacity, bucket_size) do
        path = prob_path(state, key, "cuckoo")

        with :ok <- ensure_prob_dir(state),
             :ok <-
               prob_create_and_fsync(
                 state,
                 path,
                 NIF.cuckoo_file_create(path, capacity, bucket_size)
               ) do
          meta_val = {:cuckoo_meta, %{capacity: capacity}}
          do_put(state, key, Ferricstore.TermCodec.encode(meta_val), 0)
          :ok
        end
      end

      defp create_topk_metadata(state, key, k, width, depth) do
        path = prob_path(state, key, "topk")

        with :ok <- ensure_prob_dir(state),
             :ok <-
               prob_create_and_fsync(
                 state,
                 path,
                 NIF.topk_file_create_v2(path, k, width, depth)
               ) do
          meta_val = {:topk_meta, %{path: path, k: k, width: width, depth: depth}}
          do_put(state, key, Ferricstore.TermCodec.encode(meta_val), 0)
          :ok
        end
      end

      defp prob_create_and_fsync(state, path, create_result) do
        case normalize_prob_create_result(create_result) do
          :ok ->
            case normalize_prob_create_result(prob_fsync_dir(state)) do
              :ok ->
                record_pending_prob_create(path)
                :ok

              {:error, _reason} = error ->
                cleanup_created_prob_file(state, path)
                error
            end

          {:error, _reason} = error ->
            error
        end
      end

      defp record_pending_prob_create(path) when is_binary(path) do
        pending = Process.get(:sm_pending_prob_creates, [])
        Process.put(:sm_pending_prob_creates, [path | pending])
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

      # Auto-creates a bloom filter file if it doesn't exist.
      defp auto_create_bloom_if_needed(state, path, key, auto_create_params) do
        cond do
          Ferricstore.FS.exists?(path) ->
            :ok

          auto_create_params ->
            %{num_bits: nb, num_hashes: nh} = auto_create_params

            with :ok <- prob_create_and_fsync(state, path, NIF.bloom_file_create(path, nb, nh)) do
              meta_val = {:bloom_meta, Map.merge(auto_create_params, %{path: path})}
              do_put(state, key, Ferricstore.TermCodec.encode(meta_val), 0)
              :ok
            end

          true ->
            :ok
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
      defp auto_create_cuckoo_if_needed(state, path, key, auto_create_params) do
        cond do
          Ferricstore.FS.exists?(path) ->
            :ok

          auto_create_params ->
            %{capacity: cap, bucket_size: bs} = auto_create_params

            with :ok <- prob_create_and_fsync(state, path, NIF.cuckoo_file_create(path, cap, bs)) do
              meta_val = {:cuckoo_meta, %{capacity: cap}}
              do_put(state, key, Ferricstore.TermCodec.encode(meta_val), 0)
              :ok
            end

          true ->
            :ok
        end
      end

      # Enhanced do_delete that cleans up prob files.
      # When a key's value is a prob metadata marker, delete the associated file.
      defp prob_file_path_for_delete(state, key) do
        if CompoundKey.internal_key?(key) do
          nil
        else
          case do_get(state, key) do
            nil ->
              nil

            value when is_binary(value) ->
              prob_file_path_from_delete_value(state, key, value)

            _ ->
              nil
          end
        end
      end

      defp maybe_delete_prob_file_path(_state, nil), do: :ok

      defp maybe_delete_prob_file_path(state, path) do
        result =
          try do
            case Ferricstore.FS.rm(path) do
              :ok -> prob_fsync_dir(state)
              {:error, {:not_found, _}} -> :ok
              {:error, reason} -> {:error, {:delete_prob_file_failed, reason}}
              other -> {:error, {:unexpected_delete_prob_file_result, other}}
            end
          rescue
            error -> {:error, {:delete_prob_file_exception, error}}
          catch
            :exit, reason -> {:error, {:delete_prob_file_exit, reason}}
          end

        case result do
          :ok ->
            :ok

          {:error, reason} ->
            Logger.warning(
              "StateMachine probabilistic sidecar delete failed for #{path}: #{inspect(reason)}"
            )

            emit_prob_sidecar_delete_failed(state, path, reason)
            :ok
        end
      end

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
