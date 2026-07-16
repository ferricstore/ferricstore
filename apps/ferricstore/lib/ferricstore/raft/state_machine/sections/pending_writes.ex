defmodule Ferricstore.Raft.StateMachine.Sections.PendingWrites do
  @moduledoc false

  import Kernel, except: [apply: 3]

  defmacro __using__(_opts) do
    quote do
      import Kernel, except: [apply: 3]
      import Bitwise

      require Logger
      require Ferricstore.LatencyTrace

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
      alias Ferricstore.Store.Shard.CompoundMemberIndex
      alias Ferricstore.Store.Shard.Transaction, as: ShardTransaction
      alias Ferricstore.Store.Shard.Flush, as: ShardFlush
      alias Ferricstore.Transaction.Ast, as: TxAst

      defp do_compound_put_blob_ref(state, redis_key, compound_key, encoded_ref, expire_at_ms) do
        with {:ok, materialized} <- materialize_blob_ref(state, encoded_ref) do
          result =
            case promoted_compound_path(state, redis_key, compound_key) do
              nil ->
                raw_put_blob_ref(state, compound_key, encoded_ref, expire_at_ms, materialized)

              dedicated_path ->
                do_promoted_compound_put(
                  state,
                  redis_key,
                  compound_key,
                  encoded_ref,
                  expire_at_ms,
                  dedicated_path
                )
            end

          if result == :ok do
            zset_index_put(state, redis_key, compound_key, materialized)
          end

          result
        end
      end

      defp raw_put_blob_ref(state, key, encoded_ref, expire_at_ms, materialized_value) do
        raw_put_blob_ref(state, key, encoded_ref, expire_at_ms, materialized_value, LFU.initial())
      end

      defp raw_put_flow_blob_ref(state, key, encoded_ref, expire_at_ms) do
        lfu = LFU.initial()
        raw_put_blob_ref_ref_only(state, key, encoded_ref, expire_at_ms, lfu)
      end

      defp raw_put_blob_ref_ref_only(state, key, encoded_ref, expire_at_ms) do
        raw_put_blob_ref_ref_only(state, key, encoded_ref, expire_at_ms, LFU.initial())
      end

      defp raw_put_blob_ref_ref_only(state, key, encoded_ref, expire_at_ms, lfu) do
        disk_val = to_disk_binary(encoded_ref)

        if cross_shard_pending_active?() do
          cross_shard_raw_put(state, key, nil, disk_val, expire_at_ms, lfu)
          maybe_queue_lmdb_policy_put(key, disk_val, expire_at_ms)
          maybe_queue_lmdb_flow_blob_value_put(state, key, encoded_ref, expire_at_ms)
          :ok
        else
          record_pending_original(state, key)

          unless standalone_staged_apply?() do
            track_keydir_binary_delta(state, key, nil, expire_at_ms)

            safe_ets_insert(
              state.ets,
              {key, nil, expire_at_ms, lfu, :pending, 0, byte_size(disk_val)}
            )
          end

          queue_pending_put_cold(key, disk_val, expire_at_ms, lfu)
          Process.put(:sm_pending_fast_staged_put_batch, true)
          maybe_queue_lmdb_policy_put(key, disk_val, expire_at_ms)
          maybe_queue_lmdb_flow_blob_value_put(state, key, encoded_ref, expire_at_ms)
          :ok
        end
      end

      defp raw_put_blob_ref(state, key, encoded_ref, expire_at_ms, materialized_value, lfu) do
        disk_val = to_disk_binary(encoded_ref)

        if cross_shard_pending_active?() do
          cross_shard_raw_put(state, key, nil, disk_val, expire_at_ms, lfu)
          put_pending_value(key, materialized_value, expire_at_ms)
          maybe_queue_lmdb_policy_put(key, disk_val, expire_at_ms)
          maybe_queue_lmdb_flow_blob_value_put(state, key, encoded_ref, expire_at_ms)
          :ok
        else
          record_pending_original(state, key)

          unless standalone_staged_apply?() do
            track_keydir_binary_delta(state, key, nil, expire_at_ms)

            safe_ets_insert(
              state.ets,
              {key, nil, expire_at_ms, lfu, :pending, 0, byte_size(disk_val)}
            )
          end

          queue_pending_put_cold(key, disk_val, expire_at_ms, lfu)
          put_pending_value(key, materialized_value, expire_at_ms)
          Process.put(:sm_pending_fast_staged_put_batch, true)
          maybe_queue_lmdb_policy_put(key, disk_val, expire_at_ms)
          maybe_queue_lmdb_flow_blob_value_put(state, key, encoded_ref, expire_at_ms)
          :ok
        end
      end

      defp put_pending_value(key, value, expire_at_ms) do
        pending_values = Process.get(:sm_pending_values, %{})
        Process.put(:sm_pending_values, Map.put(pending_values, key, {value, expire_at_ms}))
      end

      defp materialize_blob_ref(state, encoded_ref) when is_binary(encoded_ref) do
        case BlobRef.decode(encoded_ref) do
          {:ok, ref} ->
            with :ok <- Ferricstore.Raft.ApplyLimits.validate_value_size(state, ref.size) do
              case BlobStore.get(state.data_dir, state.shard_index, ref) do
                {:ok, value} -> {:ok, value}
                {:error, reason} -> {:error, {:blob_ref_unavailable, reason}}
              end
            end

          :error ->
            {:error, {:blob_ref_unavailable, :invalid_blob_ref}}
        end
      end

      defp materialize_blob_ref(_state, _encoded_ref),
        do: {:error, {:blob_ref_unavailable, :invalid_blob_ref}}

      defp decode_blob_ref(encoded_ref) when is_binary(encoded_ref) do
        case BlobRef.decode(encoded_ref) do
          {:ok, ref} -> {:ok, ref}
          :error -> {:error, {:blob_ref_unavailable, :invalid_blob_ref}}
        end
      end

      defp decode_blob_ref(_encoded_ref), do: {:error, {:blob_ref_unavailable, :invalid_blob_ref}}

      # Flushes all accumulated disk writes in a single NIF call, then updates
      # ETS entries with real file_id/offset. Called at the end of every apply/3
      # — no :pending entries remain after this returns.
      defp flush_pending_writes(state) do
        :ok = flush_pending_lmdb(state)

        case Process.put(:sm_pending_writes, []) do
          [] ->
            Ferricstore.LatencyTrace.maybe_span "server_flow_index_update_us" do
              flush_pending_flow_native_indexes(state)
            end

          pending when is_list(pending) ->
            batch = Enum.reverse(pending)
            {batch_bytes, record_bytes, delete_count} = bitcask_batch_stats(batch)

            case Process.get(@sm_waraft_projection_writer_key) do
              projection_writer when is_function(projection_writer, 1) ->
                flush_pending_waraft_projection(state, batch, projection_writer)

              _none ->
                flush_pending_bitcask_batch(state, batch, batch_bytes, record_bytes, delete_count)
            end

          _ ->
            :ok
        end
      end

      defp flush_pending_bitcask_batch(state, batch, batch_bytes, record_bytes, delete_count) do
        case resolve_active_file(state) do
          :stale ->
            emit_bitcask_append_telemetry(
              state,
              System.monotonic_time(),
              length(batch),
              batch_bytes,
              delete_count,
              :stale
            )

            set_disk_pressure(state)
            rollback_pending_writes(state)
            {:error, :active_file_unavailable}

          {file_path, file_id} ->
            started_at = System.monotonic_time()

            append_result =
              Ferricstore.LatencyTrace.maybe_span "server_bitcask_append_us" do
                append_pending_batch(file_path, batch)
              end

            validated_append_result = validate_append_result(batch, append_result)

            emit_bitcask_append_telemetry(
              state,
              started_at,
              length(batch),
              batch_bytes,
              delete_count,
              validated_append_result
            )

            case validated_append_result do
              {:ok, locations} ->
                clear_disk_pressure(state)
                publish_pending_batch(state, file_id, batch, locations)

                observe_pending_lmdb_mirror_enqueue(state, enqueue_pending_lmdb_mirror(state))
                state = track_bitcask_append_bytes(state, file_path, file_id, record_bytes)
                apply_state_put(:pending_state, state)
                :ok

              {:error, reason} ->
                set_disk_pressure(state)
                rollback_pending_writes(state)
                {:error, {:bitcask_append_failed, reason}}
            end
        end
      end

      defp flush_pending_waraft_projection(state, batch, projection_writer) do
        projection_result =
          Ferricstore.LatencyTrace.maybe_span "server_bitcask_append_us" do
            projection_writer.(batch)
          end

        case projection_result do
          {:ok, file_id, locations} ->
            case validate_append_result(batch, {:ok, locations}) do
              {:ok, ^locations} ->
                clear_disk_pressure(state)
                publish_pending_batch(state, file_id, batch, locations)

                observe_pending_lmdb_mirror_enqueue(state, enqueue_pending_lmdb_mirror(state))
                :ok

              {:error, reason} ->
                set_disk_pressure(state)
                rollback_pending_writes(state)
                {:error, {:waraft_projection_failed, reason}}
            end

          {:error, reason} ->
            set_disk_pressure(state)
            rollback_pending_writes(state)
            {:error, {:waraft_projection_failed, reason}}

          other ->
            set_disk_pressure(state)
            rollback_pending_writes(state)
            {:error, {:waraft_projection_failed, {:unexpected_result, other}}}
        end
      end

      defp publish_pending_batch(state, file_id, batch, locations) do
        ctx = Map.get(state, :instance_ctx, %{})

        Ferricstore.Store.PublicationEpoch.with_write(ctx, state.shard_index, fn ->
          Ferricstore.LatencyTrace.maybe_span "server_pending_locations_us" do
            apply_pending_locations(state, file_id, batch, locations)
          end

          Ferricstore.LatencyTrace.maybe_span "server_flow_index_update_us" do
            flush_pending_flow_native_indexes(state)
          end

          Ferricstore.LatencyTrace.maybe_span "server_zset_index_update_us" do
            flush_pending_zset_indexes(state)
          end
        end)
      end

      defp flush_pending_flow_native_indexes(state) do
        case Process.put(:sm_pending_flow_native_ops, []) do
          [] ->
            :ok

          ops when is_list(ops) ->
            batches =
              ops
              |> Enum.reverse()
              |> normalize_flow_native_ops(state)
              |> coalesce_flow_native_ops()

            if batches != [] do
              Process.put(:sm_pending_flow_native_flush?, true)
            end

            batches
            |> Enum.each(fn {native, batch_ops} ->
              NativeFlowIndex.apply_batch(native, batch_ops)
              after_flow_native_apply_batch_hook(native, batch_ops)
            end)

            :ok

          _ ->
            :ok
        end
      end

      if Mix.env() == :test do
        defp after_flow_native_apply_batch_hook(native, batch_ops) do
          case Process.get(:ferricstore_state_machine_after_flow_native_apply_batch_hook) do
            hook when is_function(hook, 2) -> hook.(native, batch_ops)
            _ -> :ok
          end
        end
      else
        defp after_flow_native_apply_batch_hook(_native, _batch_ops), do: :ok
      end

      @doc false
      def __coalesce_flow_native_ops_for_test__(ops), do: coalesce_flow_native_ops(ops)

      @doc false
      def __flow_history_projection_shards_for_test__(ctx, state, entries) do
        Enum.map(entries, &flow_history_projection_shard(ctx, state, &1))
      end

      @doc false
      def __flow_history_projection_same_shard_for_test__(ctx, state, entries) do
        flow_history_projection_same_shard?(ctx, state, entries)
      end

      @doc false
      def __observe_tagged_lmdb_enqueue_failure_for_test__(state, ops, after_flush \\ []) do
        previous = Process.get(:sm_pending_lmdb_mirror_tagged, :undefined)
        Process.put(:sm_pending_lmdb_mirror_tagged, true)

        try do
          result = enqueue_lmdb_mirror_groups(state, ops, after_flush)
          observe_pending_lmdb_mirror_enqueue(state, result)
          result
        after
          case previous do
            :undefined -> Process.delete(:sm_pending_lmdb_mirror_tagged)
            value -> Process.put(:sm_pending_lmdb_mirror_tagged, value)
          end
        end
      end

      @doc false
      def __safe_ets_select_page_for_test__(table, match_spec, limit) do
        safe_ets_select_page(table, match_spec, limit)
      end

      defp normalize_flow_native_ops([], _state), do: []

      defp normalize_flow_native_ops(ops, state) do
        fallback_native = flow_native_index(state)

        Enum.flat_map(ops, fn
          {native, op} when is_reference(native) ->
            [{native, op}]

          op ->
            case fallback_native do
              nil -> []
              native -> [{native, op}]
            end
        end)
      end

      defp coalesce_flow_native_ops([]), do: []

      defp coalesce_flow_native_ops([{native, op} | rest]) do
        rest
        |> Enum.reduce([{native, flow_native_op_batch_class(op), [op]}], fn {next_native, next_op},
                                                                            [
                                                                              {current_native,
                                                                               current_class,
                                                                               current_ops}
                                                                              | tail
                                                                            ] = acc ->
          next_class = flow_native_op_batch_class(next_op)

          if flow_native_ops_batchable?(current_native, current_class, next_native, next_class) do
            [{current_native, current_class, [next_op | current_ops]} | tail]
          else
            [{next_native, next_class, [next_op]} | acc]
          end
        end)
        |> Enum.reverse()
        |> Enum.map(fn {batch_native, _class, reversed_ops} ->
          {batch_native, Enum.reverse(reversed_ops)}
        end)
      end

      defp flow_native_ops_batchable?(native, class, native, class), do: class != :barrier
      defp flow_native_ops_batchable?(_native, _class, _next_native, _next_class), do: false

      defp flow_native_op_batch_class({:put_entries, _entries}), do: :put_entries
      defp flow_native_op_batch_class({:put_new_entries, _entries}), do: :put_new_entries
      defp flow_native_op_batch_class({:move_entries, _entries}), do: :move_entries
      defp flow_native_op_batch_class({:delete_members, _key, _members}), do: :delete_members
      defp flow_native_op_batch_class({:apply_claim_entries, _entries}), do: :apply_claim_entries
      defp flow_native_op_batch_class(_op), do: :barrier

      defp bitcask_batch_stats(batch) do
        Enum.reduce(batch, {0, 0, 0}, fn
          {:put, key, value, _expire_at_ms}, {batch_bytes, record_bytes, delete_count} ->
            bytes = byte_size(key) + byte_size(value)

            {batch_bytes + bytes, record_bytes + @bitcask_record_header_size + bytes,
             delete_count}

          {:put_cold, key, value, _expire_at_ms, _lfu},
          {batch_bytes, record_bytes, delete_count} ->
            bytes = byte_size(key) + byte_size(value)

            {batch_bytes + bytes, record_bytes + @bitcask_record_header_size + bytes,
             delete_count}

          {:delete, key, _prob_path}, {batch_bytes, record_bytes, delete_count} ->
            bytes = byte_size(key)

            {batch_bytes + bytes, record_bytes + @bitcask_record_header_size + bytes,
             delete_count + 1}
        end)
      end

      defp bitcask_record_bytes(batch) do
        {_batch_bytes, record_bytes, _delete_count} = bitcask_batch_stats(batch)
        record_bytes
      end

      defp track_bitcask_append_bytes(state, file_path, file_id, written_bytes)
           when written_bytes > 0 do
        state = %{state | active_file_path: file_path, active_file_id: file_id}
        fid = state.active_file_id
        {total, dead} = Map.get(state.file_stats, fid, {0, 0})

        state
        |> Map.put(:active_file_size, state.active_file_size + written_bytes)
        |> Map.put(:file_stats, Map.put(state.file_stats, fid, {total + written_bytes, dead}))
        |> maybe_rotate_state_machine_active_file()
      end

      defp track_bitcask_append_bytes(state, _file_path, _file_id, _written_bytes), do: state

      defp track_cross_shard_append_bytes(state, shard_index, file_path, file_id, written_bytes) do
        cond do
          shard_index == state.shard_index and file_path == state.active_file_path and
              file_id == state.active_file_id ->
            track_bitcask_append_bytes(state, file_path, file_id, written_bytes)

          shard_index != state.shard_index ->
            maybe_rotate_remote_cross_shard_active_file(
              state,
              shard_index,
              file_path,
              file_id,
              written_bytes
            )

            state

          true ->
            # Dedicated collection files are checkpoint dependencies, but they
            # are not the shard's rotatable shared active file.
            state
        end
      end

      defp maybe_rotate_remote_cross_shard_active_file(
             state,
             shard_index,
             file_path,
             file_id,
             written_bytes
           )
           when written_bytes > 0 do
        ctx = checkpoint_ctx_for_state(state)

        with %{keydir_refs: keydir_refs} <- ctx,
             true <- is_tuple(keydir_refs),
             true <- shard_index >= 0 and shard_index < tuple_size(keydir_refs),
             keydir <- elem(keydir_refs, shard_index),
             {^file_id, ^file_path, shard_data_path} <-
               Ferricstore.Store.ActiveFile.get(ctx, shard_index),
             {:ok, %{size: active_file_size}} <- File.stat(file_path) do
          max_active_file_size =
            Map.get(ctx, :max_active_file_size, Map.get(state, :max_active_file_size))

          rotated =
            %{
              state
              | shard_index: shard_index,
                shard_data_path: shard_data_path,
                shard_data_path_expanded: Path.expand(shard_data_path),
                active_file_id: file_id,
                active_file_path: file_path,
                active_file_size: active_file_size,
                file_stats: %{file_id => {active_file_size, 0}},
                max_active_file_size: max_active_file_size,
                ets: keydir
            }
            |> maybe_rotate_state_machine_active_file()

          if rotated.active_file_id != file_id or rotated.active_file_path != file_path do
            notify_cross_shard_active_file_sync(ctx, shard_index)
          end
        end

        :ok
      rescue
        _ -> :ok
      end

      defp maybe_rotate_remote_cross_shard_active_file(
             _state,
             _shard_index,
             _file_path,
             _file_id,
             _written_bytes
           ),
           do: :ok

      defp notify_cross_shard_active_file_sync(%{name: _name} = ctx, shard_index) do
        ctx
        |> Router.shard_name(shard_index)
        |> GenServer.cast(:sync_active_file_from_registry)
      rescue
        _ -> :ok
      catch
        :exit, _ -> :ok
      end

      defp notify_cross_shard_active_file_sync(_ctx, _shard_index), do: :ok

      defp mark_cross_shard_checkpoint_dirty(state, shard_index) do
        case checkpoint_ctx_for_state(state) do
          nil ->
            if shard_index == state.shard_index do
              clear_disk_pressure(state)
            end

          ctx ->
            flag_idx = shard_index + 1

            if flag_idx <= :atomics.info(ctx.checkpoint_flags).size do
              if shard_index == state.shard_index do
                remember_checkpoint_clean_before_write(state, ctx)
              end

              remember_checkpoint_dependencies_clean_before_write(state)
              :atomics.put(ctx.checkpoint_flags, flag_idx, 1)
              record_checkpoint_dirty_index(shard_index)
            end

            Ferricstore.Store.DiskPressure.clear(ctx, shard_index)
        end

        state
      rescue
        _ -> state
      end

      defp maybe_rotate_state_machine_active_file(state) do
        rotated =
          state
          |> Map.put(:index, state.shard_index)
          |> Map.put(:keydir, state.ets)
          |> ShardFlush.maybe_rotate_file()

        %{
          state
          | active_file_id: rotated.active_file_id,
            active_file_path: rotated.active_file_path,
            active_file_size: rotated.active_file_size,
            file_stats: rotated.file_stats
        }
      end

      defp append_pending_batch(file_path, batch) do
        has_delete? = pending_batch_has_delete?(batch)

        if standalone_staged_apply?() do
          append_pending_batch_sync(file_path, batch, has_delete?)
        else
          append_pending_batch_nosync(file_path, batch, has_delete?)
        end
      end

      defp append_pending_batch(file_path, batch, has_delete?) do
        if standalone_staged_apply?() do
          append_pending_batch_sync(file_path, batch, has_delete?)
        else
          append_pending_batch_nosync(file_path, batch, has_delete?)
        end
      end

      defp append_pending_batch_sync(file_path, batch, has_delete?) do
        case standalone_durability_hook(file_path, batch) do
          :passthrough ->
            do_append_pending_batch_sync(file_path, batch, has_delete?)

          {:error, _reason} = error ->
            error

          {:ok, _locations} = ok ->
            ok

          other ->
            other
        end
      end

      defp do_append_pending_batch_sync(file_path, batch, has_delete?) do
        if has_delete? do
          ops =
            Enum.map(batch, fn
              {:put, key, value, expire_at_ms} -> {:put, key, value, expire_at_ms}
              {:put_cold, key, value, expire_at_ms, _lfu} -> {:put, key, value, expire_at_ms}
              {:delete, key, _prob_path} -> {:delete, key}
            end)

          NIF.v2_append_ops_batch(file_path, ops)
        else
          puts =
            Enum.map(batch, fn
              {:put, key, value, expire_at_ms} -> {key, value, expire_at_ms}
              {:put_cold, key, value, expire_at_ms, _lfu} -> {key, value, expire_at_ms}
            end)

          case NIF.v2_append_batch(file_path, puts) do
            {:ok, locations} ->
              tagged_locations =
                Enum.map(locations, fn {offset, value_size} ->
                  {:put, offset, value_size}
                end)

              {:ok, tagged_locations}

            {:error, _reason} = error ->
              error
          end
        end
      end

      defp append_pending_batch_nosync(file_path, batch, has_delete?) do
        if has_delete? do
          ops =
            Enum.map(batch, fn
              {:put, key, value, expire_at_ms} -> {:put, key, value, expire_at_ms}
              {:put_cold, key, value, expire_at_ms, _lfu} -> {:put, key, value, expire_at_ms}
              {:delete, key, _prob_path} -> {:delete, key}
            end)

          NIF.v2_append_ops_batch_nosync(file_path, ops)
        else
          puts =
            Enum.map(batch, fn
              {:put, key, value, expire_at_ms} -> {key, value, expire_at_ms}
              {:put_cold, key, value, expire_at_ms, _lfu} -> {key, value, expire_at_ms}
            end)

          case NIF.v2_append_batch_nosync(file_path, puts) do
            {:ok, locations} ->
              tagged_locations =
                Enum.map(locations, fn {offset, value_size} ->
                  {:put, offset, value_size}
                end)

              {:ok, tagged_locations}

            {:error, _reason} = error ->
              error
          end
        end
      end

      defp pending_batch_has_delete?(batch) do
        case Process.get(:sm_pending_has_delete, :unknown) do
          true -> true
          false -> false
          _ -> batch_contains_delete?(batch)
        end
      end

      defp batch_contains_delete?(batch), do: Enum.any?(batch, &match?({:delete, _, _}, &1))

      defp standalone_durability_hook(file_path, batch) do
        case Application.get_env(:ferricstore, :standalone_durability_hook) do
          hook when is_function(hook, 2) -> hook.(file_path, batch)
          _ -> :passthrough
        end
      end

      defp validate_append_result(batch, {:ok, locations}) do
        case validate_pending_locations(batch, locations) do
          :ok -> {:ok, locations}
          {:error, reason} -> {:error, reason}
        end
      end

      defp validate_append_result(_batch, append_result), do: append_result

      defp validate_pending_locations(batch, locations) do
        validate_pending_locations(batch, locations, 0)
      end

      defp validate_pending_locations([], [], _index), do: :ok

      defp validate_pending_locations([], locations, index) do
        {:error,
         {:bitcask_append_result_mismatch, {:length_mismatch, index, index + length(locations)}}}
      end

      defp validate_pending_locations(entries, [], index) do
        {:error,
         {:bitcask_append_result_mismatch, {:length_mismatch, index + length(entries), index}}}
      end

      defp validate_pending_locations([entry | entries], [location | locations], index) do
        expected = pending_entry_op(entry)
        actual = pending_location_op(location)

        cond do
          expected != actual ->
            {:error, {:bitcask_append_result_mismatch, {:op_mismatch, index, expected, actual}}}

          not valid_pending_location?(location) ->
            {:error, {:bitcask_append_result_mismatch, {:invalid_location, index, location}}}

          true ->
            validate_pending_locations(entries, locations, index + 1)
        end
      end

      defp pending_entry_op({:put, _key, _value, _expire_at_ms}), do: :put
      defp pending_entry_op({:put_cold, _key, _value, _expire_at_ms, _lfu}), do: :put
      defp pending_entry_op({:delete, _key, _prob_path}), do: :delete

      defp pending_location_op({:put, _offset, _value_size}), do: :put
      defp pending_location_op({:delete, _offset, _record_size}), do: :delete
      defp pending_location_op(_location), do: :unknown

      defp valid_pending_location?({:put, offset, value_size}),
        do: non_negative_integer?(offset) and non_negative_integer?(value_size)

      defp valid_pending_location?({:delete, offset, record_size}),
        do: non_negative_integer?(offset) and non_negative_integer?(record_size)

      defp valid_pending_location?(_location), do: false

      defp non_negative_integer?(value), do: is_integer(value) and value >= 0

      defp apply_pending_locations(state, file_id, batch, locations) do
        cond do
          Process.get(:sm_pending_fast_put_batch) == true and put_only_pending_batch?(batch) ->
            apply_fast_put_pending_locations(
              state,
              file_id,
              batch,
              locations,
              hot_cache_threshold(state)
            )

          Process.get(:sm_pending_fast_delete_batch) == true and delete_only_pending_batch?(batch) ->
            apply_fast_delete_pending_locations(state, batch, locations)

          Process.get(:sm_pending_fast_staged_put_batch) == true and
              put_or_put_cold_pending_batch?(batch) ->
            apply_fast_staged_put_pending_locations(
              state,
              file_id,
              batch,
              locations,
              hot_cache_threshold(state)
            )

          true ->
            apply_pending_locations(state, file_id, batch, locations, standalone_staged_apply?())
        end
      end

      defp put_only_pending_batch?(batch) do
        Enum.all?(batch, fn
          {:put, _key, _value, _expire_at_ms} -> true
          _entry -> false
        end)
      end

      defp delete_only_pending_batch?(batch) do
        Enum.all?(batch, fn
          {:delete, _key, _prob_path} -> true
          _entry -> false
        end)
      end

      defp put_or_put_cold_pending_batch?(batch) do
        Enum.all?(batch, fn
          {:put, _key, _value, _expire_at_ms} -> true
          {:put_cold, _key, _value, _expire_at_ms, _lfu} -> true
          _entry -> false
        end)
      end

      defp apply_fast_put_pending_locations(_state, _file_id, [], [], _hot_threshold), do: :ok

      defp apply_fast_put_pending_locations(
             state,
             file_id,
             [{:put, key, value, expire_at_ms} | batch],
             [{:put, offset, value_size} | locations],
             hot_threshold
           ) do
        ets_val = value_for_ets(value, hot_threshold)
        previous = :ets.lookup(state.ets, key)

        track_keydir_binary_delta_from_previous(state, key, previous, ets_val, expire_at_ms)

        :ets.insert(
          state.ets,
          {key, ets_val, expire_at_ms, LFU.initial(), file_id, offset, value_size}
        )

        CompoundMemberIndex.put(Map.get(state, :compound_member_index_name), key)
        logical_key_index_put(state, key, value, expire_at_ms)
        apply_fast_put_pending_locations(state, file_id, batch, locations, hot_threshold)
      end

      defp apply_fast_delete_pending_locations(_state, [], []), do: :ok

      defp apply_fast_delete_pending_locations(
             state,
             [{:delete, key, prob_path} | batch],
             [{:delete, _offset, _record_size} | locations]
           ) do
        delete_apply_projection_cache_for_pending_original(state, key)
        track_keydir_binary_remove(state, key)
        :ets.delete(state.ets, key)
        CompoundMemberIndex.delete(Map.get(state, :compound_member_index_name), key)
        logical_key_index_delete(state, key)
        maybe_queue_lmdb_state_delete_after_publish(state, key)
        maybe_delete_prob_file_path(state, prob_path)

        apply_fast_delete_pending_locations(state, batch, locations)
      end

      defp apply_fast_staged_put_pending_locations(
             state,
             file_id,
             batch,
             locations,
             hot_threshold
           ) do
        cond do
          batch_has_duplicate_put_key?(batch) ->
            apply_final_staged_put_pending_locations(
              state,
              file_id,
              batch,
              locations,
              hot_threshold
            )

          true ->
            do_apply_fast_staged_put_pending_locations(
              state,
              file_id,
              batch,
              locations,
              hot_threshold
            )
        end
      end
    end
  end
end
