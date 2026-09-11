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
        AppendResult,
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
      alias Ferricstore.Store.Shard.NamespaceUsageIndex
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
          materialize_pending_fast_deletes(state)
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
          materialize_pending_fast_deletes(state)
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

      defp materialize_pending_fast_deletes(state) do
        if Process.get(:sm_pending_fast_delete_batch) == true do
          :sm_pending_unmaterialized_fast_delete_keys
          |> Process.put([])
          |> Enum.each(fn key ->
            record_pending_original(state, key)
            track_keydir_binary_remove(state, key)
            :ets.delete(state.ets, key)
            maybe_queue_lmdb_state_delete(state, key)
          end)

          Process.put(:sm_pending_fast_delete_batch, false)
        end

        :ok
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
      defp flush_pending_writes(state, publication) do
        case prepare_pending_flow_native_batches(state) do
          :ok ->
            do_flush_pending_writes(state, publication)

          {:error, _reason} = error ->
            rollback_pending_writes(state)
            error
        end
      end

      defp do_flush_pending_writes(state, publication) do
        :ok = flush_pending_lmdb(state)

        case Process.put(:sm_pending_writes, []) do
          [] ->
            Ferricstore.LatencyTrace.maybe_span "server_flow_index_update_us" do
              flush_pending_flow_native_indexes(state)
            end

          pending when is_list(pending) ->
            batch =
              pending
              |> Enum.reverse()
              |> maybe_compact_pending_stream_meta_writes(publication)

            {batch_bytes, record_bytes, delete_count} = bitcask_batch_stats(batch)

            case Process.get(@sm_waraft_projection_writer_key) do
              projection_writer when is_function(projection_writer, 1) ->
                flush_pending_waraft_projection(state, batch, projection_writer, publication)

              _none ->
                flush_pending_bitcask_batch(
                  state,
                  batch,
                  batch_bytes,
                  record_bytes,
                  delete_count,
                  publication
                )
            end

          _ ->
            :ok
        end
      end

      defp flush_pending_bitcask_batch(
             state,
             batch,
             batch_bytes,
             record_bytes,
             delete_count,
             publication
           ) do
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
                Process.put(:sm_pending_storage_published?, true)
                publish_pending_batch(state, file_id, batch, locations, publication)

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

      defp flush_pending_waraft_projection(state, batch, projection_writer, publication) do
        projection_result =
          Ferricstore.LatencyTrace.maybe_span "server_bitcask_append_us" do
            projection_writer.(batch)
          end

        case projection_result do
          {:ok, file_id, locations} ->
            case validate_append_result(batch, {:ok, locations}) do
              {:ok, ^locations} ->
                clear_disk_pressure(state)
                Process.put(:sm_pending_storage_published?, true)
                publish_pending_batch(state, file_id, batch, locations, publication)

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

      defp publish_pending_batch(state, file_id, batch, locations, publication) do
        ctx = Map.get(state, :instance_ctx, %{})

        Ferricstore.Store.PublicationEpoch.with_write(ctx, state.shard_index, fn ->
          Ferricstore.LatencyTrace.maybe_span "server_pending_locations_us" do
            apply_pending_batch_locations(state, file_id, batch, locations, publication)
          end

          Ferricstore.LatencyTrace.maybe_span "server_flow_index_update_us" do
            flush_pending_flow_native_indexes(state)
          end

          Ferricstore.LatencyTrace.maybe_span "server_zset_index_update_us" do
            flush_pending_zset_indexes(state)
          end

          Ferricstore.LatencyTrace.maybe_span "server_stream_cache_publish_us" do
            flush_pending_stream_cache_cleanups()
            publish_terminal_stream_cache(publication)
          end
        end)
      end

      defp flush_pending_flow_native_indexes(state) do
        Process.put(:sm_pending_flow_native_ops, [])

        case Process.put(:sm_pending_flow_native_batches, []) do
          [] ->
            :ok

          batches when is_list(batches) ->
            Process.put(:sm_pending_flow_native_flush?, true)

            case apply_flow_native_batches(batches) do
              :ok ->
                flow_flush_pending_due_catalog_keys(state)

              {:error, reason} ->
                Logger.error(
                  "Flow native index apply failed; rebuilding from committed keydir: #{inspect(reason)}"
                )

                with :ok <- reset_flow_native_index_from_keydir(state) do
                  flow_flush_pending_due_catalog_keys(state)
                end
            end

          _ ->
            :ok
        end
      end

      defp prepare_pending_flow_native_batches(state) do
        ops = Process.get(:sm_pending_flow_native_ops, [])

        with true <- is_list(ops),
             batches <-
               ops
               |> Enum.reverse()
               |> normalize_flow_native_ops(state)
               |> coalesce_flow_native_ops(),
             {:ok, prepared} <- chunk_flow_native_batches(batches),
             :ok <- before_flow_native_prepare_commit_hook(prepared) do
          Process.put(:sm_pending_flow_native_batches, prepared)
          Process.put(:sm_pending_flow_native_ops, [])
          :ok
        else
          false -> {:error, :invalid_pending_flow_native_ops}
          {:error, _reason} = error -> error
        end
      end

      defp chunk_flow_native_batches(batches) do
        Enum.reduce_while(batches, {:ok, []}, fn {native, batch_ops}, {:ok, prepared} ->
          case NativeFlowIndex.chunk_batch_ops(batch_ops) do
            {:ok, chunks} ->
              next =
                Enum.reduce(chunks, prepared, fn chunk_ops, acc ->
                  [{native, chunk_ops} | acc]
                end)

              {:cont, {:ok, next}}

            {:error, _reason} = error ->
              {:halt, error}
          end
        end)
        |> case do
          {:ok, prepared} -> {:ok, Enum.reverse(prepared)}
          {:error, _reason} = error -> error
        end
      end

      defp apply_flow_native_batches(batches) do
        Enum.reduce_while(batches, :ok, fn {native, batch_ops}, :ok ->
          case apply_flow_native_batch(native, batch_ops) do
            :ok ->
              after_flow_native_apply_batch_hook(native, batch_ops)
              {:cont, :ok}

            {:error, _reason} = error ->
              {:halt, error}

            invalid ->
              {:halt, {:error, {:invalid_flow_native_apply_result, invalid}}}
          end
        end)
      end

      if Mix.env() == :test do
        defp before_flow_native_prepare_commit_hook(prepared) do
          case Process.get(:ferricstore_state_machine_flow_native_prepare_hook) do
            hook when is_function(hook, 1) -> hook.(prepared)
            _ -> :ok
          end
        end

        defp apply_flow_native_batch(native, batch_ops) do
          case Process.get(:ferricstore_state_machine_flow_native_apply_hook) do
            hook when is_function(hook, 2) -> hook.(native, batch_ops)
            _ -> NativeFlowIndex.apply_batch(native, batch_ops)
          end
        end

        defp after_flow_native_apply_batch_hook(native, batch_ops) do
          case Process.get(:ferricstore_state_machine_after_flow_native_apply_batch_hook) do
            hook when is_function(hook, 2) -> hook.(native, batch_ops)
            _ -> :ok
          end
        end
      else
        defp before_flow_native_prepare_commit_hook(_prepared), do: :ok

        defp apply_flow_native_batch(native, batch_ops),
          do: NativeFlowIndex.apply_batch(native, batch_ops)

        defp after_flow_native_apply_batch_hook(_native, _batch_ops), do: :ok
      end

      @doc false
      def __coalesce_flow_native_ops_for_test__(ops), do: coalesce_flow_native_ops(ops)

      @doc false
      def __prepare_pending_flow_native_batches_for_test__(state),
        do: prepare_pending_flow_native_batches(state)

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

      @doc false
      def __materialize_pending_fast_deletes_for_test__(
            state,
            pending_writes,
            unmaterialized_keys
          )
          when is_list(pending_writes) and is_list(unmaterialized_keys) do
        Process.put(:sm_pending_writes, pending_writes)
        Process.put(:sm_pending_unmaterialized_fast_delete_keys, unmaterialized_keys)
        Process.put(:sm_pending_fast_delete_batch, true)
        Process.put(:sm_pending_originals, %{})

        try do
          :ok = materialize_pending_fast_deletes(state)
          {:ok, map_size(Process.get(:sm_pending_originals, %{}))}
        after
          Process.delete(:sm_pending_writes)
          Process.delete(:sm_pending_unmaterialized_fast_delete_keys)
          Process.delete(:sm_pending_fast_delete_batch)
          Process.delete(:sm_pending_originals)
        end
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

      # A generic Raft batch can contain many XADDs for the same Stream. Each
      # append must observe the metadata produced by the previous append while
      # IDs and replies are computed, but only the final metadata value is part
      # of the committed projection. Avoid writing every intermediate XM row to
      # Bitcask; the Raft WAL still retains the complete ordered command batch.
      @doc false
      def compact_pending_stream_meta_writes_for_test(batch),
        do: compact_pending_stream_meta_writes(batch)

      # The compact Stream command emits one final metadata row by construction,
      # so there are no intermediate XM rows to remove. Generic command batches
      # still require compaction because each XADD is applied sequentially.
      defp maybe_compact_pending_stream_meta_writes(
             batch,
             %Ferricstore.Commands.Stream.AtomicAppend.Publication{}
           ),
           do: batch

      defp maybe_compact_pending_stream_meta_writes(
             batch,
             [%Ferricstore.Commands.Stream.AtomicAppend.Publication{} | _rest]
           ),
           do: batch

      defp maybe_compact_pending_stream_meta_writes(batch, _publication),
        do: compact_pending_stream_meta_writes(batch)

      defp compact_pending_stream_meta_writes(batch) do
        {compacted, _seen} =
          batch
          |> Enum.reverse()
          |> Enum.reduce({[], MapSet.new()}, fn entry, {acc, seen} ->
            case pending_stream_meta_key(entry) do
              nil ->
                {[entry | acc], seen}

              key ->
                if MapSet.member?(seen, key) do
                  {acc, seen}
                else
                  {[entry | acc], MapSet.put(seen, key)}
                end
            end
          end)

        compacted
      end

      defp pending_stream_meta_key({:put, <<"XM:", _rest::binary>> = key, _value, _expiry}),
        do: key

      defp pending_stream_meta_key(
             {:put_cold, <<"XM:", _rest::binary>> = key, _value, _expiry, _lfu}
           ),
           do: key

      defp pending_stream_meta_key({:delete, <<"XM:", _rest::binary>> = key, _path}), do: key
      defp pending_stream_meta_key(_entry), do: nil

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

      if Mix.env() == :test do
        defp append_pending_batch(file_path, batch) do
          case pending_append_test_hook(file_path, batch) do
            :passthrough ->
              append_pending_batch_by_mode(file_path, batch, pending_batch_has_delete?(batch))

            result ->
              result
          end
        end

        defp append_pending_batch(file_path, batch, has_delete?) do
          case pending_append_test_hook(file_path, batch) do
            :passthrough -> append_pending_batch_by_mode(file_path, batch, has_delete?)
            result -> result
          end
        end

        defp pending_append_test_hook(file_path, batch) do
          case Application.get_env(:ferricstore, :pending_append_hook) do
            hook when is_function(hook, 2) -> hook.(file_path, batch)
            _missing -> :passthrough
          end
        end

        defp append_pending_batch_by_mode(file_path, batch, has_delete?) do
          if standalone_staged_apply?(),
            do: append_pending_batch_sync(file_path, batch, has_delete?),
            else: append_pending_batch_nosync(file_path, batch, has_delete?)
        end
      else
        defp append_pending_batch(file_path, batch) do
          has_delete? = pending_batch_has_delete?(batch)

          if standalone_staged_apply?(),
            do: append_pending_batch_sync(file_path, batch, has_delete?),
            else: append_pending_batch_nosync(file_path, batch, has_delete?)
        end

        defp append_pending_batch(file_path, batch, has_delete?) do
          if standalone_staged_apply?(),
            do: append_pending_batch_sync(file_path, batch, has_delete?),
            else: append_pending_batch_nosync(file_path, batch, has_delete?)
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
              tag_put_append_locations(locations, length(puts))

            {:error, _reason} = error ->
              error

            other ->
              {:error, {:bitcask_append_result_mismatch, {:unexpected_result, other}}}
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
          if Process.get(:sm_pending_fast_put_batch) == true and
               put_only_pending_batch?(batch) do
            # The native mixed-operation writer accepts these ready {:put, ...}
            # rows and already returns tagged locations. Avoid allocating one
            # transformed request list and one transformed result list for the
            # common Stream/put-only Raft projection.
            NIF.v2_append_ops_batch_nosync(file_path, batch)
          else
            puts =
              Enum.map(batch, fn
                {:put, key, value, expire_at_ms} -> {key, value, expire_at_ms}
                {:put_cold, key, value, expire_at_ms, _lfu} -> {key, value, expire_at_ms}
              end)

            case NIF.v2_append_batch_nosync(file_path, puts) do
              {:ok, locations} ->
                tag_put_append_locations(locations, length(puts))

              {:error, _reason} = error ->
                error

              other ->
                {:error, {:bitcask_append_result_mismatch, {:unexpected_result, other}}}
            end
          end
        end
      end

      defp tag_put_append_locations(locations, expected_count) do
        case AppendResult.validate_locations(locations, expected_count) do
          :ok ->
            {:ok,
             Enum.map(locations, fn {offset, value_size} ->
               {:put, offset, value_size}
             end)}

          {:error, reason} ->
            {:error, {:bitcask_append_result_mismatch, reason}}
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

      defp validate_append_result(_batch, {:error, _reason} = error), do: error

      defp validate_append_result(_batch, append_result) do
        {:error, {:bitcask_append_result_mismatch, {:unexpected_result, append_result}}}
      end

      defp validate_pending_locations(batch, locations)
           when is_list(batch) and is_list(locations) do
        validate_pending_locations(batch, locations, 0)
      end

      defp validate_pending_locations(_batch, locations) do
        {:error, {:bitcask_append_result_mismatch, {:invalid_locations, locations}}}
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
        apply_pending_batch_locations(state, file_id, batch, locations, nil)
      end

      defp apply_pending_batch_locations(state, file_id, batch, locations, publication) do
        cond do
          Process.get(:sm_pending_fast_put_batch) == true and
              (stream_append_publication?(publication) or
                 put_only_pending_batch?(batch)) ->
            apply_fast_put_pending_locations(
              state,
              file_id,
              batch,
              locations,
              hot_cache_threshold(state),
              publication
            )

          Process.get(:sm_pending_fast_delete_batch) == true and delete_only_pending_batch?(batch) ->
            apply_fast_delete_pending_locations(state, batch, locations)

          Process.get(:sm_pending_fast_put_batch) == true ->
            apply_mixed_fast_pending_locations(
              state,
              file_id,
              batch,
              locations,
              hot_cache_threshold(state)
            )

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

      defp stream_append_publication?(%Ferricstore.Commands.Stream.AtomicAppend.Publication{}),
        do: true

      defp stream_append_publication?([
             %Ferricstore.Commands.Stream.AtomicAppend.Publication{} | _rest
           ]),
           do: true

      defp stream_append_publication?(_publication), do: false

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

      # A generic batch may start on the invisible put-only fast path and then
      # add a delete or another staged operation. Once durable, publish only the
      # final mutation for each key. The ordinary mixed publisher expects every
      # put to have an ETS :pending row, which intentionally is not true for the
      # invisible prefix of such a batch.
      defp apply_mixed_fast_pending_locations(state, file_id, batch, locations, hot_threshold) do
        final_indexes = final_pending_key_indexes(batch)

        {puts, put_locations, cold_puts, deletes} =
          batch
          |> Enum.zip(locations)
          |> Enum.with_index()
          |> Enum.reduce({[], [], [], []}, fn
            {{{:put, key, value, expire_at_ms} = entry, location}, index},
            {puts, put_locations, cold_puts, deletes} ->
              if Map.get(final_indexes, key) == index do
                {[entry | puts], [location | put_locations], cold_puts, deletes}
              else
                {puts, put_locations, cold_puts, deletes}
              end

            {{{:put_cold, key, value, expire_at_ms, lfu}, {:put, offset, value_size}}, index},
            {puts, put_locations, cold_puts, deletes} ->
              if Map.get(final_indexes, key) == index do
                cold_put = {key, value, expire_at_ms, lfu, offset, value_size}
                {puts, put_locations, [cold_put | cold_puts], deletes}
              else
                {puts, put_locations, cold_puts, deletes}
              end

            {{{:delete, key, prob_path}, {:delete, _offset, _record_size}}, index},
            {puts, put_locations, cold_puts, deletes} ->
              maybe_delete_prob_file_path(state, prob_path)

              if Map.get(final_indexes, key) == index do
                {puts, put_locations, cold_puts, [key | deletes]}
              else
                {puts, put_locations, cold_puts, deletes}
              end
          end)

        apply_fast_put_pending_locations(
          state,
          file_id,
          Enum.reverse(puts),
          Enum.reverse(put_locations),
          hot_threshold,
          nil
        )

        Enum.each(Enum.reverse(cold_puts), fn
          {key, value, expire_at_ms, lfu, offset, value_size} ->
            apply_put_cold_pending_location(
              state,
              key,
              value,
              expire_at_ms,
              lfu,
              file_id,
              offset,
              value_size
            )
        end)

        Enum.each(deletes, fn key ->
          delete_apply_projection_cache_for_pending_original(state, key)
          track_keydir_binary_remove(state, key)
          :ets.delete(state.ets, key)
          CompoundMemberIndex.delete(Map.get(state, :compound_member_index_name), key)
          logical_key_index_delete(state, key)
          maybe_queue_lmdb_state_delete_after_publish(state, key)
        end)

        :ok
      end

      defp final_pending_key_indexes(batch) do
        batch
        |> Enum.with_index()
        |> Enum.reduce(%{}, fn
          {{:put, key, _value, _expire_at_ms}, index}, acc ->
            Map.put(acc, key, index)

          {{:put_cold, key, _value, _expire_at_ms, _lfu}, index}, acc ->
            Map.put(acc, key, index)

          {{:delete, key, _prob_path}, index}, acc ->
            Map.put(acc, key, index)
        end)
      end

      defp apply_fast_put_pending_locations(
             state,
             file_id,
             batch,
             locations,
             hot_threshold,
             publication
           ) do
        {terminal_stream_publications, terminal_stream_members} =
          case publication do
            %Ferricstore.Commands.Stream.AtomicAppend.Publication{} = stream_publication ->
              {[stream_publication], stream_publication.member_entries}

            [%Ferricstore.Commands.Stream.AtomicAppend.Publication{} | _rest] =
                stream_publications ->
              members = Enum.flat_map(stream_publications, & &1.member_entries)
              {stream_publications, members}

            _generic ->
              {[], nil}
          end

        collect_stream_usage? =
          is_list(terminal_stream_members) and namespace_usage_index_active?(state)

        terminal_stream_batch? = terminal_stream_publications != []
        initial_lfu = LFU.initial()

        {refs, compound_entries, stream_usage_entries, stream_ets_entries, stream_binary_bytes} =
          Ferricstore.LatencyTrace.maybe_span "server_stream_publish_prepare_us" do
            do_apply_fast_put_pending_locations(
              state,
              file_id,
              batch,
              locations,
              hot_threshold,
              terminal_stream_members || [],
              collect_stream_usage?,
              terminal_stream_batch?,
              initial_lfu,
              {[], [], [], [], 0}
            )
          end

        # These rows are unique, newly assigned Stream IDs. ETS publication and
        # namespace accounting are order-independent, so retain the accumulator
        # order instead of copying both batches merely to restore append order.
        # The compact Stream member catalog below receives ready keyed rows from
        # the explicit publication descriptor; generic compound rows retain the
        # accumulator reversal used by the ordinary publication path.
        Ferricstore.LatencyTrace.maybe_span "server_stream_publish_keydir_us" do
          add_keydir_binary_bytes(state, stream_binary_bytes)
          safe_ets_insert_many(state.ets, stream_ets_entries)
        end

        Ferricstore.LatencyTrace.maybe_span "server_stream_publish_usage_us" do
          entries =
            maybe_collect_late_stream_usage_entries(
              state,
              batch,
              terminal_stream_members,
              collect_stream_usage?,
              stream_usage_entries
            )

          namespace_usage_index_put_many(state, entries)
        end

        Ferricstore.LatencyTrace.maybe_span "server_stream_publish_members_us" do
          member_index = Map.get(state, :compound_member_index_name)

          case terminal_stream_publications do
            [stream_publication] ->
              CompoundMemberIndex.publish_stream_append(
                member_index,
                stream_publication.member_prefix,
                stream_publication.member_entries,
                stream_publication.member_count
              )

            [_first, _second | _rest] = stream_publications ->
              stream_counts =
                Enum.map(stream_publications, fn stream_publication ->
                  {stream_publication.member_prefix, stream_publication.member_count}
                end)

              CompoundMemberIndex.publish_stream_appends(
                member_index,
                terminal_stream_members,
                stream_counts
              )

            [] ->
              CompoundMemberIndex.put_many(member_index, Enum.reverse(compound_entries))
          end
        end

        Ferricstore.LatencyTrace.maybe_span "server_stream_publish_cache_refs_us" do
          delete_apply_projection_cache_refs(state, refs)
        end
      end

      defp do_apply_fast_put_pending_locations(
             _state,
             _file_id,
             [],
             [],
             _hot_threshold,
             _terminal_stream_members,
             _collect_stream_usage?,
             _terminal_stream_batch?,
             _initial_lfu,
             acc
           ),
           do: acc

      defp do_apply_fast_put_pending_locations(
             state,
             file_id,
             [{:put, key, value, expire_at_ms} | batch],
             [{:put, offset, value_size} | locations],
             hot_threshold,
             terminal_stream_members,
             collect_stream_usage?,
             terminal_stream_batch?,
             initial_lfu,
             {refs, compound_entries, stream_usage_entries, stream_ets_entries,
              stream_binary_bytes}
           ) do
        ets_val = value_for_ets(value, hot_threshold)

        {new_stream_member?, remaining_stream_members} =
          take_terminal_stream_member(key, terminal_stream_members)

        {previous, refs} =
          if new_stream_member? do
            {[], refs}
          else
            {safe_ets_lookup(state.ets, key),
             maybe_prepend_apply_projection_cache_ref(state, key, refs, file_id)}
          end

        unless new_stream_member? do
          track_keydir_binary_delta_from_previous(state, key, previous, ets_val, expire_at_ms)
        end

        ets_entry =
          {key, ets_val, expire_at_ms, initial_lfu, file_id, offset, value_size}

        {stream_usage_entries, stream_ets_entries, stream_binary_bytes} =
          if new_stream_member? do
            stream_usage_entries =
              if collect_stream_usage? do
                [{key, value, expire_at_ms} | stream_usage_entries]
              else
                stream_usage_entries
              end

            {
              stream_usage_entries,
              [ets_entry | stream_ets_entries],
              stream_binary_bytes + missing_keydir_binary_bytes(key, ets_val)
            }
          else
            if terminal_stream_batch? do
              # The terminal Stream plan is already durable and no command reads
              # during publication. Publish its type/meta rows with the member
              # rows in one ETS operation instead of exposing a partial batch.
              terminal_stream_auxiliary_logical_index_put(state, key, value, expire_at_ms)

              stream_usage_entries =
                if collect_stream_usage? do
                  [{key, value, expire_at_ms} | stream_usage_entries]
                else
                  stream_usage_entries
                end

              {stream_usage_entries, [ets_entry | stream_ets_entries], stream_binary_bytes}
            else
              safe_ets_insert(state.ets, ets_entry)
              logical_key_index_put(state, key, value, expire_at_ms)
              {stream_usage_entries, stream_ets_entries, stream_binary_bytes}
            end
          end

        next_compound_entries =
          if new_stream_member? or terminal_stream_batch? do
            # Terminal plans contain only explicitly published X: members plus
            # T:/XM: auxiliary rows. The latter have no compound separator and
            # are intentionally absent from CompoundMemberIndex, so do not
            # accumulate and re-scan them through its generic fallback.
            compound_entries
          else
            [{key, expire_at_ms} | compound_entries]
          end

        do_apply_fast_put_pending_locations(
          state,
          file_id,
          batch,
          locations,
          hot_threshold,
          remaining_stream_members,
          collect_stream_usage?,
          terminal_stream_batch?,
          initial_lfu,
          {
            refs,
            next_compound_entries,
            stream_usage_entries,
            stream_ets_entries,
            stream_binary_bytes
          }
        )
      end

      defp take_terminal_stream_member(
             key,
             [{{_member_prefix, {_ms, _seq}}, key} | remaining]
           ),
           do: {true, remaining}

      defp take_terminal_stream_member(_key, members), do: {false, members}

      # Compact append metadata is an internal row and is intentionally absent
      # from the logical-key projection. Namespace usage for all terminal rows
      # is published by one bulk call after the keydir becomes visible.
      defp terminal_stream_auxiliary_logical_index_put(
             _state,
             <<"XM:", _rest::binary>> = key,
             _value,
             _expire_at_ms
           )
           when is_binary(key),
           do: :ok

      defp terminal_stream_auxiliary_logical_index_put(state, key, value, expire_at_ms) do
        :ok =
          Ferricstore.Store.Shard.LogicalKeyIndex.put(
            Map.get(state, :logical_key_index_name),
            Map.get(state, :logical_key_slots_name),
            key,
            value,
            expire_at_ms
          )
      end

      defp namespace_usage_index_active?(state) do
        state
        |> Map.get(:namespace_usage_index_name)
        |> NamespaceUsageIndex.active?()
      end

      defp maybe_collect_late_stream_usage_entries(
             state,
             batch,
             members,
             false,
             []
           )
           when is_list(members) do
        # Scope activation rebuilds from the visible keydir. Recheck only after
        # the bulk member rows are visible: if activation raced the early check,
        # either this append accounts them here or the rebuild observes them.
        if namespace_usage_index_active?(state) do
          collect_terminal_stream_usage_entries(batch, [])
        else
          []
        end
      end

      defp maybe_collect_late_stream_usage_entries(
             _state,
             _batch,
             _members,
             _collected?,
             entries
           ),
           do: entries

      defp collect_terminal_stream_usage_entries([], entries), do: entries

      defp collect_terminal_stream_usage_entries(
             [{:put, key, value, expire_at_ms} | batch],
             entries
           ),
           do:
             collect_terminal_stream_usage_entries(
               batch,
               [{key, value, expire_at_ms} | entries]
             )

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
