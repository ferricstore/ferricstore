defmodule Ferricstore.Raft.WARaftStorage.Sections.ApplyResult do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.CommandTime
      alias Ferricstore.HLC
      alias Ferricstore.Flow.HistoryProjector
      alias Ferricstore.Flow.Keys, as: FlowKeys
      alias Ferricstore.Flow.LMDB, as: FlowLMDB
      alias Ferricstore.Raft.StateMachine
      alias Ferricstore.Raft.CommandStamp
      alias Ferricstore.Raft.WARaftSegmentReader
      alias Ferricstore.Raft.WARaftSegmentReader.CommandValues
      alias Ferricstore.Store.BlobRef
      alias Ferricstore.Store.BlobStore
      alias Ferricstore.Store.BlobValue
      alias Ferricstore.Store.ColdRead
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.Promotion
      alias Ferricstore.Store.Shard.ETS, as: ShardETS
      alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
      alias Ferricstore.Store.Shard.ZSetIndex

      defp meta_from_position({:raft_log_pos, index, term})
           when is_integer(index) and is_integer(term) do
        %{index: index, term: term}
      end

      defp meta_from_position(_position), do: %{}

      defp unwrap_applied_result({:applied_at, _index, result}), do: result
      defp unwrap_applied_result(result), do: result

      defp finish_apply_result(command, position, result, old_handle, new_handle) do
        # Command-level errors such as WRONGTYPE or compare failures are still
        # deterministic Raft outcomes and may advance the replay cursor. Storage
        # infrastructure failures are different: if Bitcask/blob/projection apply
        # did not durably match the committed log entry, keep the old position so
        # restart recovery replays the entry instead of acknowledging a skipped
        # local materialization.
        if storage_apply_failure?(result) do
          {result,
           block_storage(old_handle, storage_block_reason(result), position, :apply_failure)}
        else
          new_handle =
            maybe_clear_replay_safe_noop_dirty(command, result, old_handle, new_handle)

          persist_position(position, result, old_handle, new_handle)
        end
      end

      defp maybe_clear_replay_safe_noop_dirty(
             command,
             result,
             %{bitcask_dirty?: false},
             %{bitcask_dirty?: true} = new_handle
           ) do
        if replay_safe_noop_result?(decoded_replay_command(command), result) do
          %{new_handle | bitcask_dirty?: false}
        else
          new_handle
        end
      end

      defp maybe_clear_replay_safe_noop_dirty(_command, _result, _old_handle, new_handle),
        do: new_handle

      defp decoded_replay_command(command), do: CommandValues.decode_replay_command(command)

      defp replay_safe_noop_result?({:cas, _key, _expected, _new_value, _ttl_ms}, result)
           when result in [0, nil],
           do: true

      defp replay_safe_noop_result?({:set, _key, _value, _expire_at_ms, opts}, nil)
           when is_map(opts) do
        Map.get(opts, :nx, false) or Map.get(opts, :xx, false)
      end

      defp replay_safe_noop_result?({:set_blob_ref, _key, _encoded_ref, _expire_at_ms, opts}, nil)
           when is_map(opts) do
        Map.get(opts, :nx, false) or Map.get(opts, :xx, false)
      end

      defp replay_safe_noop_result?(_command, _result), do: false

      defp storage_block_reason({:error, reason}), do: reason
      defp storage_block_reason(reason), do: reason

      defp storage_apply_failure?({:error, reason}), do: storage_apply_failure_reason?(reason)
      defp storage_apply_failure?(_result), do: false

      defp storage_apply_failure_reason?(:active_file_unavailable), do: true
      defp storage_apply_failure_reason?(:invalid_preencoded_command), do: true
      defp storage_apply_failure_reason?({:bitcask_append_failed, _reason}), do: true
      defp storage_apply_failure_reason?({:bitcask_append_result_mismatch, _reason}), do: true
      defp storage_apply_failure_reason?({:bitcask_writer_flush_failed, _reason}), do: true
      defp storage_apply_failure_reason?({:blob_externalize_failed, _reason}), do: true
      defp storage_apply_failure_reason?({:blob_ref_unavailable, _reason}), do: true
      defp storage_apply_failure_reason?({:state_read_failed, _reason}), do: true
      defp storage_apply_failure_reason?({:cross_shard_compensation_failed, _reason}), do: true
      defp storage_apply_failure_reason?({:flow_history_projection_failed, _reason}), do: true
      defp storage_apply_failure_reason?({:flush_shard_apply_failed, _reason}), do: true
      defp storage_apply_failure_reason?({:batch_result_mismatch, _expected, _actual}), do: true

      defp storage_apply_failure_reason?({:tombstone_batch_result_mismatch, _expected, _actual}),
        do: true

      defp storage_apply_failure_reason?({:fsync_dir_failed, _phase, _reason}), do: true
      defp storage_apply_failure_reason?({:delete_prob_file_failed, _reason}), do: true
      defp storage_apply_failure_reason?(_reason), do: false

      defp persist_position(position, result, old_handle, handle) do
        new_handle =
          handle
          |> Map.put(:position, position)
          |> register_segment_projection_context()

        case profile_storage_apply_phase(new_handle, :apply_projection_cache, fn ->
               maybe_compact_apply_projection_cache(new_handle)
             end) do
          {:ok, compacted_handle} ->
            case profile_storage_apply_phase(compacted_handle, :recovery_projection, fn ->
                   {:ok, compacted_handle}
                 end) do
              {:ok, projected_handle} ->
                case profile_storage_apply_phase(projected_handle, :storage_metadata, fn ->
                       persist_metadata_for_hot_position(old_handle, projected_handle)
                     end) do
                  {:ok, persisted_handle} ->
                    {tag_applied_result(position, result),
                     maybe_start_segment_projection_checkpoint(persisted_handle)}

                  :skipped ->
                    {tag_applied_result(position, result),
                     projected_handle
                     |> maybe_mark_clean_position()
                     |> maybe_start_segment_projection_checkpoint()}

                  {:error, reason} ->
                    {{:error, reason},
                     block_storage(old_handle, reason, position, :metadata_failure)}
                end

              {:error, reason} ->
                {{:error, reason},
                 block_storage(old_handle, reason, position, :segment_projection_failure)}
            end

          {:error, reason} ->
            {{:error, reason},
             block_storage(old_handle, reason, position, :apply_projection_cache_compaction)}
        end
      end

      defp tag_applied_result(
             {:raft_log_pos, index, term} = position,
             result
           )
           when is_integer(index) and index > 0 and is_integer(term) and term > 0,
           do: {:waraft_applied_at, position, result}

      defp tag_applied_result(_position, result), do: result

      defp maybe_compact_apply_projection_cache(handle) do
        entry_limit = normalize_apply_projection_cache_limit(apply_projection_cache_max_entries())
        byte_limit = normalize_apply_projection_cache_limit(apply_projection_cache_max_bytes())

        cond do
          Map.has_key?(handle, :apply_projection_cache_compaction) ->
            {:ok, handle}

          entry_limit == :infinity and byte_limit == :infinity ->
            {:ok, handle}

          true ->
            count =
              Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(
                handle.ctx.data_dir,
                handle.shard_index
              )

            bytes =
              Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_bytes(
                handle.ctx.data_dir,
                handle.shard_index
              )

            spill_count = apply_projection_cache_spill_amount(count, entry_limit)
            spill_bytes = apply_projection_cache_spill_amount(bytes, byte_limit)

            if spill_count > 0 or spill_bytes > 0 do
              start_apply_projection_cache_compaction(
                handle,
                count,
                bytes,
                entry_limit,
                byte_limit,
                spill_count,
                spill_bytes
              )
            else
              {:ok, handle}
            end
        end
      end

      defp start_apply_projection_cache_compaction(
             handle,
             count,
             limit
           ) do
        entry_limit = normalize_apply_projection_cache_limit(limit)

        bytes =
          Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_bytes(
            handle.ctx.data_dir,
            handle.shard_index
          )

        start_apply_projection_cache_compaction(
          handle,
          count,
          bytes,
          entry_limit,
          :infinity,
          apply_projection_cache_spill_amount(count, entry_limit),
          0
        )
      end

      defp start_apply_projection_cache_compaction(
             %{position: position} = handle,
             count,
             bytes,
             entry_limit,
             byte_limit,
             spill_count,
             spill_bytes
           ) do
        index = position_index(position)

        cond do
          index <= 0 ->
            {:ok, handle}

          spill_count <= 0 and spill_bytes <= 0 ->
            {:ok, handle}

          true ->
            ref = make_ref()
            started_at = System.monotonic_time()
            storage_name = Map.fetch!(handle.options, :storage_name)

            metadata = %{
              shard_index: handle.shard_index,
              position: position,
              root_dir: handle.root_dir,
              count: count,
              bytes: bytes,
              limit: entry_limit,
              byte_limit: byte_limit,
              spill_count: spill_count,
              spill_bytes: spill_bytes
            }

            case Task.start(fn ->
                   result =
                     run_apply_projection_cache_compaction(
                       handle.ctx.data_dir,
                       handle.shard_index,
                       spill_count,
                       spill_bytes,
                       metadata
                     )

                   send_storage_info(
                     storage_name,
                     {:ferricstore_waraft_apply_projection_cache_compact_done, ref,
                      {started_at, metadata, result}}
                   )
                 end) do
              {:ok, pid} ->
                monitor = Process.monitor(pid)

                {:ok,
                 Map.put(handle, :apply_projection_cache_compaction, %{
                   ref: ref,
                   pid: pid,
                   monitor: monitor,
                   started_at: started_at,
                   metadata: metadata,
                   count: count,
                   bytes: bytes,
                   limit: entry_limit,
                   byte_limit: byte_limit,
                   spill_count: spill_count,
                   spill_bytes: spill_bytes
                 })}

              {:error, reason} ->
                emit_apply_projection_cache_compaction(
                  metadata,
                  started_at,
                  {:error, {:task_start_failed, reason}}
                )

                {:ok,
                 Map.put(handle, :apply_projection_cache_last_error, {:task_start_failed, reason})}
            end
        end
      end

      defp normalize_apply_projection_cache_limit(:infinity), do: :infinity

      defp normalize_apply_projection_cache_limit(limit)
           when is_integer(limit) and limit >= 0,
           do: limit

      defp normalize_apply_projection_cache_limit(_invalid), do: :infinity

      defp apply_projection_cache_spill_amount(_current, :infinity), do: 0

      defp apply_projection_cache_spill_amount(current, limit)
           when is_integer(current) and current > limit and is_integer(limit) and limit >= 0 do
        max(current - div(limit, 2), 1)
      end

      defp apply_projection_cache_spill_amount(_current, _limit), do: 0

      defp apply_projection_cache_max_entries do
        Ferricstore.MemoryBudget.limit(:waraft_apply_projection_cache_max_entries, 16_384)
      end

      defp apply_projection_cache_max_bytes do
        Ferricstore.MemoryBudget.limit(:waraft_apply_projection_cache_max_bytes, 8_388_608)
      end

      defp run_apply_projection_cache_compaction(
             data_dir,
             shard_index,
             spill_count,
             spill_bytes,
             metadata
           ) do
        call_apply_projection_cache_compact_hook(:before_spill, metadata)

        Ferricstore.Raft.WARaftSegmentReader.spill_apply_projection_cache(
          data_dir,
          shard_index,
          spill_count,
          spill_bytes
        )
      rescue
        error -> {:error, {:apply_projection_cache_compact_failed, error}}
      catch
        kind, reason -> {:error, {:apply_projection_cache_compact_failed, {kind, reason}}}
      end

      defp finish_apply_projection_cache_compaction(
             ref,
             {started_at, metadata, result},
             %{apply_projection_cache_compaction: %{ref: ref}} = handle
           ) do
        emit_apply_projection_cache_compaction(metadata, started_at, result)

        handle =
          handle
          |> clear_apply_projection_cache_compaction()
          |> update_apply_projection_cache_compaction_error(result)

        maybe_continue_apply_projection_cache_compaction(handle, result)
      end

      defp finish_apply_projection_cache_compaction(
             _ref,
             {started_at, metadata, result},
             handle
           ) do
        emit_apply_projection_cache_compaction(metadata, started_at, result)
        {:ok, handle}
      end

      defp finish_apply_projection_cache_compaction(_ref, _result, handle), do: {:ok, handle}

      defp finish_apply_projection_cache_compaction_down(
             monitor,
             pid,
             reason,
             %{
               apply_projection_cache_compaction: %{
                 monitor: monitor,
                 pid: pid,
                 started_at: started_at,
                 metadata: metadata
               }
             } = handle
           ) do
        result = {:error, {:task_down, reason}}
        emit_apply_projection_cache_compaction(metadata, started_at, result)

        handle =
          handle
          |> clear_apply_projection_cache_compaction()
          |> update_apply_projection_cache_compaction_error(result)

        {:ok, handle}
      end

      defp finish_apply_projection_cache_compaction_down(
             _monitor,
             _pid,
             _reason,
             handle
           ),
           do: {:ok, handle}

      defp clear_apply_projection_cache_compaction(handle) do
        case Map.get(handle, :apply_projection_cache_compaction) do
          %{monitor: monitor} when is_reference(monitor) ->
            Process.demonitor(monitor, [:flush])

          _missing_or_legacy ->
            :ok
        end

        Map.delete(handle, :apply_projection_cache_compaction)
      end

      defp maybe_continue_apply_projection_cache_compaction(handle, :ok),
        do: maybe_compact_apply_projection_cache(handle)

      defp maybe_continue_apply_projection_cache_compaction(handle, {:ok, _removed}),
        do: maybe_compact_apply_projection_cache(handle)

      defp maybe_continue_apply_projection_cache_compaction(handle, _failed), do: {:ok, handle}

      defp update_apply_projection_cache_compaction_error(handle, :ok),
        do: Map.delete(handle, :apply_projection_cache_last_error)

      defp update_apply_projection_cache_compaction_error(handle, {:ok, _removed}),
        do: Map.delete(handle, :apply_projection_cache_last_error)

      defp update_apply_projection_cache_compaction_error(handle, {:error, reason}),
        do: Map.put(handle, :apply_projection_cache_last_error, reason)

      defp update_apply_projection_cache_compaction_error(handle, other),
        do: Map.put(handle, :apply_projection_cache_last_error, other)

      defp persist_metadata_for_hot_position(_old_handle, new_handle) do
        cond do
          not storage_metadata_persist_due?(new_handle) ->
            :skipped

          replay_dependencies_ready?(new_handle) ->
            persist_ready_hot_metadata(new_handle)

          true ->
            # Keep the release/persist boundary behind undurable projection data,
            # but do not spill that data synchronously from the WARaft apply path.
            {:ok, request_replay_dependencies_async(new_handle)}
        end
      end

      defp persist_ready_hot_metadata(new_handle) do
        new_handle = clear_replay_dependencies(new_handle)

        case persist_hot_metadata(new_handle) do
          :ok -> {:ok, mark_hot_metadata_persisted(new_handle)}
          {:error, _reason} = error -> error
        end
      end

      defp mark_hot_metadata_persisted(%{position: position} = handle) do
        handle
        |> Map.put(:persisted_position, position)
        |> Map.put(:last_clean_position, position)
        |> clear_replay_dependencies()
      end

      defp mark_metadata_persisted(%{position: position} = handle) do
        handle
        |> mark_hot_metadata_persisted()
        |> Map.put(:segment_projection_position, position)
      end

      defp maybe_mark_clean_position(%{bitcask_dirty?: false, position: position} = handle) do
        if replay_dependencies_ready?(handle) do
          handle
          |> Map.put(:last_clean_position, position)
          |> clear_replay_dependencies()
        else
          handle
        end
      end

      defp maybe_mark_clean_position(handle), do: handle

      defp storage_metadata_persist_due?(new_handle) do
        position_gap_due?(
          storage_metadata_persist_every(),
          Map.get(new_handle, :position),
          Map.get(new_handle, :persisted_position)
        )
      end

      defp maybe_start_segment_projection_checkpoint(handle) do
        cond do
          Map.has_key?(handle, :segment_projection_checkpoint) ->
            handle

          not segment_projection_checkpoint_due?(handle) ->
            handle

          true ->
            start_segment_projection_checkpoint(handle)
        end
      end

      defp segment_projection_checkpoint_due?(handle) do
        position_gap_due?(
          segment_projection_checkpoint_every(),
          Map.get(handle, :position),
          Map.get(handle, :segment_projection_position, @zero_pos)
        ) and segment_projection_checkpoint_interval_due?(handle)
      end

      defp segment_projection_checkpoint_interval_due?(handle) do
        interval_ms = segment_projection_checkpoint_min_interval_ms()
        last_ms = Map.get(handle, :segment_projection_checkpoint_started_at_ms, 0)

        cond do
          interval_ms <= 0 ->
            true

          last_ms <= 0 ->
            true

          System.monotonic_time(:millisecond) - last_ms >= interval_ms ->
            true

          true ->
            false
        end
      end

      defp start_segment_projection_checkpoint(%{sm_state: %{ets: keydir}} = handle) do
        if :ets.info(keydir) == :undefined do
          handle
        else
          position = Map.fetch!(handle, :position)
          ref = make_ref()
          started_at = System.monotonic_time()
          started_at_ms = System.monotonic_time(:millisecond)
          storage_name = Map.fetch!(handle.options, :storage_name)

          {:ok, pid} =
            Task.start(fn ->
              {metadata, result} =
                run_segment_projection_checkpoint(
                  handle.root_dir,
                  handle.ctx,
                  handle.shard_index,
                  position,
                  keydir
                )

              send(
                storage_name,
                {:ferricstore_waraft_segment_projection_checkpoint_done, ref,
                 {started_at, metadata, result}}
              )
            end)

          handle
          |> Map.put(:segment_projection_checkpoint, %{
            ref: ref,
            pid: pid,
            position: position,
            started_at: started_at
          })
          |> Map.put(:segment_projection_checkpoint_started_at_ms, started_at_ms)
        end
      rescue
        _ -> handle
      end

      defp start_segment_projection_checkpoint(handle), do: handle

      defp finish_segment_projection_checkpoint(
             ref,
             {started_at, metadata, result},
             %{segment_projection_checkpoint: %{ref: ref, position: position}} = handle
           ) do
        duration_us =
          System.convert_time_unit(System.monotonic_time() - started_at, :native, :microsecond)

        handle =
          handle
          |> Map.delete(:segment_projection_checkpoint)
          |> finish_segment_projection_checkpoint_result(position, result)

        emit_segment_projection_checkpoint_stop(metadata, duration_us, result)

        {:ok, handle}
      end

      defp finish_segment_projection_checkpoint(_ref, {_started_at, metadata, result}, handle) do
        emit_segment_projection_checkpoint_stale(metadata, result)
        {:ok, handle}
      end

      defp finish_segment_projection_checkpoint(_ref, _result, handle), do: {:ok, handle}

      defp finish_segment_projection_checkpoint_result(handle, position, :ok) do
        if position_index(position) >=
             position_index(Map.get(handle, :segment_projection_position, @zero_pos)) do
          Map.put(handle, :segment_projection_position, position)
        else
          handle
        end
      end

      defp finish_segment_projection_checkpoint_result(handle, position, {:ok, :stale}) do
        finish_segment_projection_checkpoint_result(handle, position, :ok)
      end

      defp finish_segment_projection_checkpoint_result(handle, _position, {:error, _reason}),
        do: handle

      defp finish_segment_projection_checkpoint_result(handle, _position, _other), do: handle

      defp run_segment_projection_checkpoint(root_dir, ctx, shard_index, position, keydir) do
        with_segment_projection_lock(root_dir, fn ->
          now = HLC.now_ms()

          case segment_projection_entries_from_keydir(keydir, ctx, shard_index, now) do
            :unavailable ->
              metadata = %{shard_index: shard_index, position: position, entries: 0}
              {metadata, {:error, {:segment_keydir_unavailable, shard_index}}}

            {:ok, {entries, entry_count}} ->
              metadata = %{shard_index: shard_index, position: position, entries: entry_count}
              emit_segment_projection_checkpoint_start(metadata)
              call_segment_projection_checkpoint_hook(:before_write, metadata)

              result =
                with :ok <-
                       write_segment_projection_checkpoint_unlocked(root_dir, position, entries) do
                  :ok
                end

              {metadata, result}

            {:error, reason} ->
              metadata = %{shard_index: shard_index, position: position, entries: 0}
              {metadata, {:error, reason}}
          end
        end)
        |> case do
          {%{} = metadata, result} ->
            {metadata, result}

          {:error, reason} ->
            {%{shard_index: shard_index, position: position, entries: 0},
             {:error, {:segment_projection_checkpoint_failed, reason}}}

          other ->
            {%{shard_index: shard_index, position: position, entries: 0},
             {:error, {:segment_projection_checkpoint_failed, other}}}
        end
      rescue
        error ->
          {%{shard_index: shard_index, position: position, entries: 0},
           {:error, {:segment_projection_checkpoint_failed, error}}}
      end

      defp segment_projection_entries_from_keydir(keydir, ctx, shard_index, now) do
        keydir
        |> reduce_keydir_rows_while({[], 0}, fn row, {entries, count} ->
          case segment_projection_entry_from_keydir_row(row, ctx, shard_index, now) do
            {:ok, entry} -> {:cont, {[entry | entries], count + 1}}
            :skip -> {:cont, {entries, count}}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end)
        |> case do
          {:ok, {entries, count}} ->
            {:ok, {Enum.sort_by(entries, fn {key, _value, _expire_at_ms} -> key end), count}}

          {:error, _reason} = error ->
            error

          :unavailable ->
            :unavailable
        end
      end

      defp write_segment_projection_checkpoint_unlocked(root_dir, position, entries) do
        checkpoint_root = segment_projection_checkpoint_root(root_dir)

        result =
          case read_segment_projection_log(checkpoint_root) do
            {:ok, %{position: existing_position}} ->
              if position_index(existing_position) >= position_index(position) do
                {:ok, :stale}
              else
                write_segment_projection(checkpoint_root, position, entries)
              end

            {:ok, _projection} ->
              write_segment_projection(checkpoint_root, position, entries)

            {:error, :enoent} ->
              write_segment_projection(checkpoint_root, position, entries)

            {:error, reason} ->
              {:error, {:read_existing_segment_projection_checkpoint, reason}}
          end

        case result do
          :ok -> :ok
          {:ok, :stale} -> {:ok, :stale}
          {:error, _reason} = error -> error
          other -> {:error, {:write_segment_projection_checkpoint, other}}
        end
      end

      defp segment_projection_checkpoint_every do
        Application.get_env(
          :ferricstore,
          :waraft_segment_projection_checkpoint_every,
          @default_segment_projection_checkpoint_every
        )
      end

      defp segment_projection_checkpoint_min_interval_ms do
        case Application.get_env(
               :ferricstore,
               :waraft_segment_projection_checkpoint_min_interval_ms,
               @default_segment_projection_checkpoint_min_interval_ms
             ) do
          value when is_integer(value) and value >= 0 -> value
          _ -> @default_segment_projection_checkpoint_min_interval_ms
        end
      end

      defp call_segment_projection_checkpoint_hook(phase, metadata) do
        case Application.get_env(:ferricstore, :waraft_segment_projection_checkpoint_hook) do
          fun when is_function(fun, 2) -> fun.(phase, metadata)
          _ -> :ok
        end
      catch
        _, _ -> :ok
      end

      defp call_apply_projection_cache_compact_hook(phase, metadata) do
        case Application.get_env(:ferricstore, :waraft_apply_projection_cache_compact_hook) do
          fun when is_function(fun, 2) -> fun.(phase, metadata)
          _ -> :ok
        end
      catch
        _, _ -> :ok
      end

      defp emit_segment_projection_checkpoint_start(metadata) do
        :telemetry.execute(
          [:ferricstore, :waraft, :segment_projection_checkpoint, :start],
          %{entries: Map.get(metadata, :entries, 0)},
          metadata
        )
      catch
        _, _ -> :ok
      end

      defp emit_segment_projection_checkpoint_stop(metadata, duration_us, result) do
        :telemetry.execute(
          [:ferricstore, :waraft, :segment_projection_checkpoint, :stop],
          %{duration_us: duration_us, entries: Map.get(metadata, :entries, 0)},
          metadata
          |> Map.put(:result, segment_projection_checkpoint_result(result))
          |> maybe_put_telemetry_reason(segment_projection_checkpoint_reason(result))
        )
      catch
        _, _ -> :ok
      end

      defp emit_segment_projection_checkpoint_stale(metadata, result) do
        :telemetry.execute(
          [:ferricstore, :waraft, :segment_projection_checkpoint, :stale],
          %{entries: Map.get(metadata, :entries, 0)},
          metadata
          |> Map.put(:result, segment_projection_checkpoint_result(result))
          |> maybe_put_telemetry_reason(segment_projection_checkpoint_reason(result))
        )
      catch
        _, _ -> :ok
      end

      defp emit_segment_projection_trim_checkpoint_reuse(metadata) do
        :telemetry.execute(
          [:ferricstore, :waraft, :segment_projection_trim, :checkpoint_reuse],
          %{
            relocations: Map.get(metadata, :relocations, 0),
            value_pin_relocations: Map.get(metadata, :value_pin_relocations, 0)
          },
          metadata
        )
      catch
        _, _ -> :ok
      end

      defp segment_projection_checkpoint_result(:ok), do: :ok
      defp segment_projection_checkpoint_result({:ok, :stale}), do: :stale
      defp segment_projection_checkpoint_result({:error, _reason}), do: :error
      defp segment_projection_checkpoint_result(_other), do: :error

      defp segment_projection_checkpoint_reason({:error, reason}), do: reason

      defp segment_projection_checkpoint_reason(other) when other not in [:ok, {:ok, :stale}],
        do: other

      defp segment_projection_checkpoint_reason(_result), do: nil

      defp maybe_put_telemetry_reason(metadata, nil), do: metadata
      defp maybe_put_telemetry_reason(metadata, reason), do: Map.put(metadata, :reason, reason)

      defp position_gap_due?(:never, _position, _persisted_position), do: false

      defp position_gap_due?(
             interval,
             {:raft_log_pos, index, _term},
             {:raft_log_pos, persisted_index, _persisted_term}
           )
           when is_integer(interval) and interval > 0 and is_integer(index) and
                  is_integer(persisted_index) do
        index - persisted_index >= interval
      end

      defp position_gap_due?(interval, {:raft_log_pos, index, _term}, _persisted_position)
           when is_integer(interval) and interval > 0 and is_integer(index) do
        index >= interval
      end

      defp position_gap_due?(_interval, _position, _persisted_position), do: true

      defp storage_metadata_persist_every do
        Application.get_env(
          :ferricstore,
          :waraft_storage_metadata_persist_every,
          @default_storage_metadata_persist_every
        )
      end

      defp register_segment_projection_context(%{root_dir: root_dir} = handle) do
        ensure_segment_projection_registry!()
        {key, handle} = segment_projection_registry_key(handle, root_dir)

        case :ets.lookup(@segment_projection_registry, {key, :context}) do
          [] ->
            true =
              :ets.insert(
                @segment_projection_registry,
                {{key, :context},
                 %{
                   ctx: Map.fetch!(handle, :ctx),
                   shard_index: Map.fetch!(handle, :shard_index)
                 }}
              )

          _context ->
            :ok
        end

        true =
          :ets.insert(
            @segment_projection_registry,
            {{key, :position}, Map.fetch!(handle, :position)}
          )

        handle
      end

      defp register_segment_projection_context(handle), do: handle

      defp unregister_segment_projection_context(%{root_dir: root_dir}) do
        case :ets.whereis(@segment_projection_registry) do
          :undefined ->
            :ok

          _tid ->
            key = segment_projection_registry_key(root_dir)
            :ets.delete(@segment_projection_registry, {key, :context})
            :ets.delete(@segment_projection_registry, {key, :position})
        end

        :ok
      rescue
        ArgumentError -> :ok
      end

      defp unregister_segment_projection_context(_handle), do: :ok

      defp lookup_segment_projection_context(root_dir) do
        case :ets.whereis(@segment_projection_registry) do
          :undefined ->
            {:error, {:segment_projection_registry_missing, root_dir}}

          _tid ->
            key = segment_projection_registry_key(root_dir)

            case {
              :ets.lookup(@segment_projection_registry, {key, :context}),
              :ets.lookup(@segment_projection_registry, {key, :position})
            } do
              {[{_context_key, context}], [{_position_key, position}]} ->
                {:ok, Map.put(context, :position, position)}

              {[], _position} ->
                {:error, {:segment_projection_context_missing, root_dir}}

              {_context, []} ->
                {:error, {:segment_projection_position_missing, root_dir}}
            end
        end
      rescue
        ArgumentError -> {:error, {:segment_projection_registry_unavailable, root_dir}}
      end

      defp validate_segment_projection_trim_position({:raft_log_pos, index, _term}, trim_index)
           when is_integer(index) and index >= trim_index,
           do: :ok

      defp validate_segment_projection_trim_position(position, trim_index),
        do: {:error, {:segment_projection_position_before_trim, position, trim_index}}

      defp ensure_segment_projection_registry! do
        case :ets.whereis(@segment_projection_registry) do
          :undefined ->
            try do
              :ets.new(@segment_projection_registry, [
                :set,
                :public,
                :named_table,
                {:read_concurrency, true},
                {:write_concurrency, true}
              ])
            rescue
              ArgumentError -> :ok
            end

          _tid ->
            :ok
        end

        :ok
      end
    end
  end
end
