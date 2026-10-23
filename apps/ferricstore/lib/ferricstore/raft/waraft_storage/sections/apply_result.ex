defmodule Ferricstore.Raft.WARaftStorage.Sections.ApplyResult do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.CommandTime
      alias Ferricstore.HLC
      alias Ferricstore.Flow.HistoryProjector
      alias Ferricstore.Flow.Keys, as: FlowKeys
      alias Ferricstore.Flow.LMDB, as: FlowLMDB
      alias Ferricstore.Raft.PromotedFileSync
      alias Ferricstore.Raft.StateMachine
      alias Ferricstore.Raft.CommandStamp
      alias Ferricstore.Raft.ApplyFailure
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

      defp storage_apply_failure?(result), do: ApplyFailure.storage_result?(result)

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

            {:ok, pid} =
              Task.start(fn ->
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
              end)

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
        if replay_dependencies_ready?(handle) and
             PromotedFileSync.synced?(Map.get(handle, :promoted_sync)) do
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

      # A deferred trim asks for a checkpoint whatever the position cadence is.
      defp segment_projection_checkpoint_due?(handle) do
        (Map.get(handle, :segment_projection_checkpoint_wanted?, false) or
           position_gap_due?(
             segment_projection_checkpoint_every(),
             Map.get(handle, :position),
             Map.get(handle, :segment_projection_position, @zero_pos)
           )) and segment_projection_checkpoint_interval_due?(handle)
      end

      defp segment_projection_checkpoint_interval_due?(handle) do
        interval_ms = segment_projection_checkpoint_min_interval_ms()
        last_ms = Map.get(handle, :segment_projection_checkpoint_started_at_ms)
        now_ms = System.monotonic_time(:millisecond)
        retry_after_ms = Map.get(handle, :segment_projection_checkpoint_retry_after_ms)

        cond do
          is_integer(retry_after_ms) and now_ms < retry_after_ms ->
            false

          interval_ms <= 0 ->
            true

          not is_integer(last_ms) ->
            true

          now_ms - last_ms >= interval_ms ->
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

          job = %{
            root_dir: handle.root_dir,
            ctx: handle.ctx,
            shard_index: handle.shard_index,
            position: position,
            keydir: keydir,
            ref: ref,
            storage_name: storage_name,
            promoted_sync_table:
              case Map.get(handle, :promoted_sync) do
                %{table: table} -> table
                nil -> nil
              end
          }

          {:ok, pid} =
            Task.start(fn ->
              {metadata, result} = run_bounded_segment_projection_checkpoint(job)

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
            started_at: started_at,
            wanted?: Map.get(handle, :segment_projection_checkpoint_wanted?, false),
            previous_started_at_ms: Map.get(handle, :segment_projection_checkpoint_started_at_ms)
          })
          |> Map.put(:segment_projection_checkpoint_started_at_ms, started_at_ms)
          |> Map.delete(:segment_projection_checkpoint_wanted?)
          |> Map.delete(:segment_projection_checkpoint_retry_after_ms)
        end
      rescue
        _ -> handle
      end

      defp start_segment_projection_checkpoint(handle), do: handle

      defp finish_segment_projection_checkpoint(
             ref,
             {started_at, metadata, result},
             %{segment_projection_checkpoint: %{ref: ref, position: position} = checkpoint} =
               handle
           ) do
        duration_us =
          System.convert_time_unit(System.monotonic_time() - started_at, :native, :microsecond)

        handle =
          handle
          |> Map.delete(:segment_projection_checkpoint)
          |> finish_segment_projection_checkpoint_result(
            Map.get(metadata, :position, position),
            result
          )
          |> requeue_unfinished_segment_projection_checkpoint(checkpoint, result)

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

      defp finish_segment_projection_checkpoint_result(handle, _position, _other), do: handle

      # A checkpoint that never got to write (another shard held the slot, or a
      # trim moved rows after its cut) did no real work: retry soon without
      # spending the minimum interval. A failed one keeps a trim's request but
      # waits out the interval.
      defp requeue_unfinished_segment_projection_checkpoint(handle, _checkpoint, :ok), do: handle

      defp requeue_unfinished_segment_projection_checkpoint(handle, checkpoint, {:ok, unfinished})
           when unfinished in [:busy, :stale_cut, :history_pending] do
        handle
        |> restore_segment_projection_checkpoint_request(checkpoint)
        |> Map.put(
          :segment_projection_checkpoint_started_at_ms,
          checkpoint.previous_started_at_ms
        )
        |> Map.put(
          :segment_projection_checkpoint_retry_after_ms,
          System.monotonic_time(:millisecond) + 1_000
        )
      end

      defp requeue_unfinished_segment_projection_checkpoint(handle, checkpoint, _failed),
        do: restore_segment_projection_checkpoint_request(handle, checkpoint)

      defp restore_segment_projection_checkpoint_request(handle, %{wanted?: true}),
        do: Map.put(handle, :segment_projection_checkpoint_wanted?, true)

      defp restore_segment_projection_checkpoint_request(handle, _checkpoint), do: handle

      # A shard has its own pending guard, but checkpoints on different shards
      # must not all copy and write their keydirs at once: at most
      # segment_projection_checkpoint_concurrency() run on a node. An attempt that
      # finds no free slot is busy and retries a second later. Waiting shards get
      # slots in the order they first asked, so one that keeps losing the race
      # cannot fall behind until its trims reach the deferral bound.
      defp run_bounded_segment_projection_checkpoint(job) do
        slots = segment_projection_checkpoint_concurrency()

        if segment_projection_checkpoint_turn?(job.root_dir, slots),
          do: run_in_segment_projection_checkpoint_slot(job, 1, slots),
          else: busy_segment_projection_checkpoint(job)
      end

      defp run_in_segment_projection_checkpoint_slot(job, slot, slots) when slot > slots,
        do: busy_segment_projection_checkpoint(job)

      defp run_in_segment_projection_checkpoint_slot(job, slot, slots) do
        lock = {{__MODULE__, :background_segment_projection_checkpoint, slot}, self()}

        run = fn ->
          stop_waiting_for_segment_projection_checkpoint(job.root_dir)
          run_segment_projection_checkpoint(job)
        end

        case :global.trans(lock, run, [node()], 0) do
          :aborted -> run_in_segment_projection_checkpoint_slot(job, slot + 1, slots)
          result -> result
        end
      end

      defp busy_segment_projection_checkpoint(job),
        do: {%{shard_index: job.shard_index, position: job.position, entries: 0}, {:ok, :busy}}

      defp segment_projection_checkpoint_concurrency do
        case Application.get_env(:ferricstore, :waraft_segment_projection_checkpoint_concurrency) do
          value when is_integer(value) and value > 0 -> value
          _ -> max(1, div(System.schedulers_online(), 4))
        end
      end

      defp segment_projection_checkpoint_turn?(root_dir, slots),
        do: segment_projection_checkpoint_waiters_ahead(root_dir) < slots

      # Records this shard's place in line (taken on its first ask) and that it
      # is still asking, and counts the shards still asking that are ahead of
      # it. A shard that stopped retrying drops out of the order. Places come
      # from a strictly increasing counter, so shards that first asked within
      # the same millisecond are still ordered.
      defp segment_projection_checkpoint_waiters_ahead(root_dir) do
        ensure_segment_projection_registry!()
        now = System.monotonic_time(:millisecond)
        registry_key = segment_projection_registry_key(root_dir)
        key = {registry_key, :checkpoint_waiting}

        place =
          case :ets.lookup(@segment_projection_registry, key) do
            [{^key, {place, _last_asked_at}}] -> place
            [] -> :erlang.unique_integer([:monotonic])
          end

        true = :ets.insert(@segment_projection_registry, {key, {place, now}})

        :ets.select_count(@segment_projection_registry, [
          {{{:"$1", :checkpoint_waiting}, {:"$2", :"$3"}},
           [
             {:"/=", :"$1", registry_key},
             {:<, :"$2", place},
             {:>=, :"$3", now - @segment_projection_checkpoint_waiter_ttl_ms}
           ], [true]}
        ])
      end

      defp stop_waiting_for_segment_projection_checkpoint(root_dir) do
        key = {segment_projection_registry_key(root_dir), :checkpoint_waiting}
        :ets.delete(@segment_projection_registry, key)
        :ok
      end

      # The checkpoint must be exactly the state at its position: recovery
      # re-executes the commands after it, and some are not idempotent. So the
      # keydir is copied while storage holds its applies (the cut), and values are
      # resolved and written afterwards, off the commit path.
      defp run_segment_projection_checkpoint(job) do
        metadata = %{shard_index: job.shard_index, position: job.position, entries: 0}
        call_segment_projection_checkpoint_hook(:before_scan, metadata)

        case take_segment_projection_cut(job) do
          {:ok, cut, cut_position, generation, history_dependencies} ->
            metadata = %{metadata | position: cut_position}

            if await_cut_history(job, history_dependencies) do
              job.root_dir
              |> with_segment_projection_lock(fn ->
                write_segment_projection_checkpoint_from_cut(job, cut, generation, metadata)
              end)
              |> case do
                {%{} = metadata, result} ->
                  {metadata, result}

                {:error, reason} ->
                  {metadata, {:error, {:segment_projection_checkpoint_failed, reason}}}
              end
            else
              :ets.delete(cut)
              {metadata, {:ok, :history_pending}}
            end

          :stale ->
            {metadata, {:ok, :stale_cut}}
        end
      rescue
        error ->
          {%{shard_index: job.shard_index, position: job.position, entries: 0},
           {:error, {:segment_projection_checkpoint_failed, error}}}
      end

      # Recovery replays only the entries after a projection, and Flow history
      # is not in the keydir: the projector must have written the history of
      # every entry the cut covers before the cut may become that floor.
      defp await_cut_history(job, history_dependencies) do
        request_history_dependencies(job, history_dependencies)

        deadline =
          System.monotonic_time(:millisecond) +
            segment_projection_checkpoint_history_timeout_ms()

        do_await_cut_history(job, history_dependencies, deadline)
      end

      defp do_await_cut_history(job, history_dependencies, deadline) do
        cond do
          history_dependencies_durable?(job, history_dependencies) ->
            true

          System.monotonic_time(:millisecond) >= deadline ->
            false

          true ->
            Process.sleep(10)
            do_await_cut_history(job, history_dependencies, deadline)
        end
      end

      defp segment_projection_checkpoint_history_timeout_ms do
        case Application.get_env(
               :ferricstore,
               :waraft_segment_projection_checkpoint_history_timeout_ms,
               @default_segment_projection_checkpoint_history_timeout_ms
             ) do
          value when is_integer(value) and value >= 0 -> value
          _ -> @default_segment_projection_checkpoint_history_timeout_ms
        end
      end

      # Storage answers with its position and then waits for this copy before it
      # applies anything else (see info/2). The trim generation is read first: a
      # trim that relocates rows during or after the copy changes it.
      defp take_segment_projection_cut(job) do
        %{ref: ref, storage_name: storage_name} = job

        send_storage_info(
          storage_name,
          {:ferricstore_waraft_segment_projection_checkpoint_cut, self(), ref}
        )

        receive do
          {:ferricstore_waraft_segment_projection_checkpoint_cut, ^ref,
           {:cut, position, storage, history_dependencies}} ->
            generation = segment_projection_trim_generation(job.root_dir)
            cut = copy_keydir_for_cut(job.keydir)
            send(storage, {:ferricstore_waraft_segment_projection_checkpoint_cut_copied, ref})

            receive do
              {:ferricstore_waraft_segment_projection_checkpoint_cut, ^ref, :confirmed} ->
                {:ok, cut, position, generation, history_dependencies}

              {:ferricstore_waraft_segment_projection_checkpoint_cut, ^ref, :expired} ->
                :ets.delete(cut)
                :stale
            after
              @segment_projection_cut_timeout_ms ->
                :ets.delete(cut)
                :stale
            end

          {:ferricstore_waraft_segment_projection_checkpoint_cut, ^ref, :stale} ->
            :stale
        after
          @segment_projection_cut_timeout_ms -> :stale
        end
      end

      # Copies the keydir in chunks into a table this task owns, so no single
      # list of every row is built while storage waits. The keydir is fixed so
      # the chunked traversal sees each row once.
      defp copy_keydir_for_cut(keydir) do
        cut = :ets.new(:segment_projection_cut, [:set, :private])
        :ets.safe_fixtable(keydir, true)

        try do
          copy_keydir_chunks(:ets.select(keydir, [{:_, [], [:"$_"]}], 2_000), cut)
        after
          :ets.safe_fixtable(keydir, false)
        end

        cut
      end

      defp copy_keydir_chunks(:"$end_of_table", _cut), do: :ok

      defp copy_keydir_chunks({rows, continuation}, cut) do
        true = :ets.insert(cut, rows)
        copy_keydir_chunks(:ets.select(continuation), cut)
      end

      # Runs under the projection lock, so no trim relocates rows or replaces
      # projection files while cold values are read from the cut's locations.
      defp write_segment_projection_checkpoint_from_cut(job, cut, generation, metadata) do
        if segment_projection_trim_generation(job.root_dir) != generation do
          # A trim moved rows after the cut; the cut's locations may be stale.
          {metadata, {:ok, :stale_cut}}
        else
          now = storage_expiry_cutoff_ms()

          case segment_projection_entries_from_keydir(cut, job.ctx, job.shard_index, now) do
            {:ok, {entries, entry_count}} ->
              metadata = %{metadata | entries: entry_count}
              emit_segment_projection_checkpoint_start(metadata)
              call_segment_projection_checkpoint_hook(:before_write, metadata)

              # Recovery may start from this checkpoint, so the dedicated-file
              # writes up to its cut must be on disk first.
              case PromotedFileSync.sync_tracked(Map.get(job, :promoted_sync_table)) do
                :ok ->
                  {metadata,
                   write_and_adopt_segment_projection_checkpoint(job, metadata, entries)}

                {:error, reason} ->
                  {metadata, {:error, reason}}
              end

            :unavailable ->
              {metadata, {:error, {:segment_keydir_unavailable, job.shard_index}}}

            {:error, reason} ->
              {metadata, {:error, reason}}
          end
        end
      end

      # Still under the projection lock: the checkpoint is written, then adopted
      # (see adopt_segment_projection_checkpoint/3), off the Raft commit path.
      defp write_and_adopt_segment_projection_checkpoint(job, metadata, entries) do
        locations = :ets.new(:segment_projection_checkpoint_locations, [:set, :private])

        try do
          with :ok <- write_segment_projection_checkpoint(job, metadata, entries, locations) do
            call_segment_projection_checkpoint_hook(:before_adopt, metadata)
            adopt_segment_projection_checkpoint(job, metadata.position, locations)
          end
        after
          :ets.delete(locations)
        end
      end

      # The write's own verify pass records each entry's location.
      defp write_segment_projection_checkpoint(job, metadata, entries, locations) do
        record_location = fn
          index, {0, {:ferricstore_segment_projection_entry, key, value, expire_at_ms}}, location
          when index > 0 ->
            {_ordinal, offset, _size} = location

            true =
              :ets.insert(
                locations,
                {key, index, offset, expire_at_ms, segment_projected_value_size(value)}
              )

          _index, _entry, _location ->
            true
        end

        case :ferricstore_waraft_spike_segment_log.write_projection(
               job.root_dir |> segment_projection_checkpoint_root() |> to_charlist(),
               metadata.position,
               entries,
               record_location
             ) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
          other -> {:error, {:write_segment_projection_log, other}}
        end
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

      defp emit_segment_projection_checkpoint_adopt(metadata) do
        :telemetry.execute(
          [:ferricstore, :waraft, :segment_projection_checkpoint, :adopt],
          %{relocations: Map.get(metadata, :relocations, 0)},
          Map.delete(metadata, :relocations)
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
      defp segment_projection_checkpoint_result({:ok, :busy}), do: :busy
      defp segment_projection_checkpoint_result({:ok, :stale_cut}), do: :stale_cut
      defp segment_projection_checkpoint_result({:ok, :history_pending}), do: :history_pending
      defp segment_projection_checkpoint_result({:error, _reason}), do: :error
      defp segment_projection_checkpoint_result(_other), do: :error

      defp segment_projection_checkpoint_reason({:error, reason}), do: reason

      defp segment_projection_checkpoint_reason(other)
           when other not in [
                  :ok,
                  {:ok, :stale},
                  {:ok, :busy},
                  {:ok, :stale_cut},
                  {:ok, :history_pending}
                ],
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
                   shard_index: Map.fetch!(handle, :shard_index),
                   storage_name: handle |> Map.get(:options, %{}) |> Map.get(:storage_name)
                 }}
              )

          _context ->
            :ok
        end

        # A trim that rebuilds the projection at this position needs the Flow
        # history of the entries up to it durable first.
        true =
          :ets.insert(
            @segment_projection_registry,
            {{key, :position}, Map.fetch!(handle, :position), history_replay_dependencies(handle)}
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
            :ets.delete(@segment_projection_registry, {key, :adopted_checkpoint})
            :ets.delete(@segment_projection_registry, {key, :checkpoint_waiting})
        end

        :ok
      rescue
        ArgumentError -> :ok
      end

      defp unregister_segment_projection_context(_handle), do: :ok

      # Bumped, under the projection lock, after every trim that may have moved
      # keydir rows or replaced projection files. A checkpoint whose cut saw an
      # older generation cannot be adopted or finished.
      defp segment_projection_trim_generation(root_dir) do
        ensure_segment_projection_registry!()
        key = {segment_projection_registry_key(root_dir), :trim_generation}

        case :ets.lookup(@segment_projection_registry, key) do
          [{^key, generation}] -> generation
          [] -> 0
        end
      end

      defp bump_segment_projection_trim_generation(root_dir) do
        ensure_segment_projection_registry!()
        key = {segment_projection_registry_key(root_dir), :trim_generation}
        _ = :ets.update_counter(@segment_projection_registry, key, 1, {key, 0})
        :ok
      end

      defp register_adopted_segment_projection_checkpoint(root_dir, adopted) do
        ensure_segment_projection_registry!()
        key = {segment_projection_registry_key(root_dir), :adopted_checkpoint}
        true = :ets.insert(@segment_projection_registry, {key, adopted})
        :ok
      end

      defp lookup_adopted_segment_projection_checkpoint(root_dir) do
        ensure_segment_projection_registry!()
        key = {segment_projection_registry_key(root_dir), :adopted_checkpoint}

        case :ets.lookup(@segment_projection_registry, key) do
          [{^key, adopted}] -> {:ok, adopted}
          [] -> :none
        end
      end

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
              {[{_context_key, context}], [{_position_key, position, history_dependencies}]} ->
                {:ok,
                 context
                 |> Map.put(:position, position)
                 |> Map.put(:history_dependencies, history_dependencies)}

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
