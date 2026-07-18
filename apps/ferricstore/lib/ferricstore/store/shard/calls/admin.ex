defmodule Ferricstore.Store.Shard.Calls.Admin do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.ExpiryContext
      alias Ferricstore.Store.CompactionPlan
      alias Ferricstore.Store.SegmentFilename
      alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
      require Logger

      # -------------------------------------------------------------------
      # handle_call — stats, merge, admin
      # -------------------------------------------------------------------
      def handle_call(:shard_stats, _from, state) do
        state = await_in_flight(state)
        state = flush_pending_sync(state)
        sp = state.shard_data_path
        key_count = :ets.info(state.keydir, :size)
        # Compute file-level stats for merge scheduler
        {total_bytes, dead_bytes, file_count} =
          case Ferricstore.FS.ls(sp) do
            {:ok, files} ->
              Enum.reduce(files, {0, 0, 0}, fn name, {total, dead, count} = acc ->
                case admin_log_file_id(name) do
                  nil ->
                    acc

                  file_id ->
                    case File.lstat(Path.join(sp, name)) do
                      {:ok, %{type: :regular, size: size}} ->
                        {_tracked_total, tracked_dead} =
                          Map.get(Map.get(state, :file_stats, %{}), file_id, {size, 0})

                        tracked_dead =
                          if is_integer(tracked_dead),
                            do: tracked_dead |> max(0) |> min(size),
                            else: 0

                        {total + size, dead + tracked_dead, count + 1}

                      _ ->
                        acc
                    end
                end
              end)

            _ ->
              {0, 0, 0}
          end

        live_bytes = total_bytes - dead_bytes
        frag = if total_bytes > 0, do: dead_bytes / total_bytes, else: 0.0
        {:reply, {:ok, {total_bytes, live_bytes, dead_bytes, file_count, key_count, frag}}, state}
      end

      def handle_call(:file_sizes, _from, state) do
        state = await_in_flight(state)
        state = flush_pending_sync(state)
        sp = state.shard_data_path

        sizes =
          case Ferricstore.FS.ls(sp) do
            {:ok, files} ->
              files
              |> Enum.flat_map(fn name ->
                case admin_log_file_id(name) do
                  nil ->
                    []

                  fid ->
                    case File.lstat(Path.join(sp, name)) do
                      {:ok, %{type: :regular, size: size}} -> [{fid, size}]
                      _ -> []
                    end
                end
              end)

            _ ->
              []
          end

        {:reply, {:ok, sizes}, state}
      end

      def handle_call({:run_compaction, _file_ids}, _from, %{writes_paused: true} = state) do
        {:reply, {:error, "ERR shard writes paused for sync"}, state}
      end

      def handle_call(
            {:run_compaction, _file_ids},
            _from,
            %{compaction_worker: worker} = state
          )
          when worker != nil do
        {:reply, {:error, :compaction_in_progress}, state}
      end

      def handle_call({:run_compaction, file_ids}, _from, state) do
        case prepare_compaction_state(state) do
          {:ok, prepared_state} ->
            start_compaction_worker(file_ids, _from, prepared_state)

          {:error, reason, failed_state} ->
            {:reply, {:error, reason}, failed_state}
        end
      end

      defp prepare_compaction_state(state) do
        state = await_in_flight(state)
        state = sync_active_file_from_registry(state)
        state = flush_pending_sync(state)

        if Map.get(state, :last_flush_error) != nil do
          {:error, {:pending_flush_failed, state.last_flush_error}, state}
        else
          # Router async/RMW paths can leave small values queued in BitcaskWriter with
          # ETS file_id=:pending. Drain those writes before compaction snapshots ETS,
          # otherwise a source file can be removed while the writer still targets it.
          case Ferricstore.Store.BitcaskWriter.flush(state.instance_ctx, state.index) do
            :ok ->
              {:ok, sync_active_file_from_registry(state)}

            {:error, reason} ->
              Logger.warning(
                "Shard #{state.index}: compaction aborted because BitcaskWriter flush failed: #{inspect(reason)}"
              )

              {:error, {:bitcask_writer_flush_failed, reason}, state}
          end
        end
      end

      defp start_compaction_worker(file_ids, from, state) do
        parent = self()
        job_ref = make_ref()

        {pid, monitor_ref} =
          :erlang.spawn_opt(
            fn ->
              result = run_compaction_worker(file_ids, state)
              send(parent, {:shard_compaction_complete, job_ref, self(), result})
            end,
            [:link, :monitor]
          )

        worker = %{
          file_ids: file_ids,
          from: from,
          job_ref: job_ref,
          monitor_ref: monitor_ref,
          pid: pid
        }

        {:noreply, %{state | compaction_worker: worker}}
      end

      defp run_compaction_worker(file_ids, state) do
        sp = state.shard_data_path
        expiry_cutoff_ms = ExpiryContext.capture() |> ExpiryContext.safe_expiry_cutoff_ms()

        {total_written, total_dropped, total_reclaimed, compacted_file_ids, skipped_file_ids,
         failures} =
          Enum.reduce(file_ids, {0, 0, 0, [], [], []}, fn fid,
                                                          {written, dropped, reclaimed, compacted,
                                                           skipped, failures} ->
            if fid == state.active_file_id do
              {written, dropped, reclaimed, compacted, skipped, failures}
            else
              case compact_inactive_segment(state, fid, expiry_cutoff_ms) do
                {:ok, copied, reclaimed_bytes} ->
                  {written + copied, dropped, reclaimed + reclaimed_bytes, [fid | compacted],
                   skipped, failures}

                :skipped ->
                  {written, dropped, reclaimed, compacted, [fid | skipped], failures}

                {:committed_error, copied, reclaimed_bytes, reason} ->
                  failure = {fid, :compaction_finalize_failed, reason}

                  {written + copied, dropped, reclaimed + reclaimed_bytes, [fid | compacted],
                   skipped, [failure | failures]}

                {:error, phase, reason} ->
                  failure =
                    case phase do
                      :publication -> {fid, :compaction_publication_failed, reason}
                      _other -> {fid, reason}
                    end

                  {written, dropped, reclaimed, compacted, skipped, [failure | failures]}
              end
            end
          end)

        # Dir fsync makes rename/rm entries durable so a kernel panic after
        # compaction doesn't resurrect pre-merge filenames. If this fails, the
        # namespace was changed in this running process, so keep file_stats in
        # sync with reality but return an error to the scheduler/operator.
        dir_fsync_failure =
          if compacted_file_ids == [] do
            nil
          else
            case compaction_fsync_dir(state, sp) do
              :ok ->
                nil

              {:error, reason} ->
                Logger.error(
                  "Shard #{state.index}: compaction directory fsync failed for #{sp}: #{inspect(reason)}"
                )

                {:dir_fsync_failed, reason}
            end
          end

        reply =
          case {dir_fsync_failure, failures} do
            {nil, []} ->
              if compacted_file_ids == [] and skipped_file_ids != [] do
                {:error, {:no_compactable_files, Enum.reverse(skipped_file_ids)}}
              else
                {:ok, {total_written, total_dropped, total_reclaimed}}
              end

            {nil, [_ | _]} ->
              {:error, {:compaction_failed, Enum.reverse(failures)}}

            {failure, []} ->
              {:error, {:compaction_failed, [failure]}}

            {failure, [_ | _]} ->
              {:error, {:compaction_failed, Enum.reverse(failures, [failure])}}
          end

        {reply, compacted_file_ids}
      end

      defp compact_inactive_segment(state, fid, expiry_cutoff_ms) do
        source = file_path(state.shard_data_path, fid)
        dest = Path.join(state.shard_data_path, "compact_#{fid}.log")
        old_size = compaction_file_size(source)

        if match?({:ok, %File.Stat{type: :directory}}, File.lstat(source)) do
          finalize_empty_compaction_segment(state, fid, source, old_size, 0)
        else
          case build_compaction_plan(state, fid, source, dest, expiry_cutoff_ms) do
            {:ok, %{plan_path: plan_path, live_count: 0, tombstone_count: tombstone_count}} ->
              remove_compaction_temp(state, dest)

              with :ok <- CompactionPlan.remove(plan_path) do
                finalize_empty_compaction_segment(
                  state,
                  fid,
                  source,
                  old_size,
                  tombstone_count
                )
              else
                {:error, reason} -> {:error, :planning, reason}
              end

            {:ok, %{plan_path: plan_path, live_count: live_count}} ->
              case publish_compacted_segment(state, fid, source, dest, plan_path) do
                :ok ->
                  finalize_published_compaction(state, fid, source, old_size, live_count)

                {:committed_error, reason} ->
                  new_size = compaction_file_size(source)
                  hint_result = remove_hint_for_file(state.shard_data_path, fid)

                  combined_reason =
                    case hint_result do
                      :ok ->
                        reason

                      {:error, hint_reason} ->
                        {reason, {:post_publish_hint_remove_failed, hint_reason}}
                    end

                  {:committed_error, live_count, max(old_size - new_size, 0), combined_reason}

                {:error, reason} ->
                  remove_compaction_temp(state, dest)
                  _ = CompactionPlan.remove(plan_path)

                  Logger.error(
                    "Shard #{state.index}: compaction publication failed for #{source}: #{inspect(reason)}"
                  )

                  {:error, :publication, reason}
              end

            {:error, reason} ->
              maybe_emit_compaction_crc_mismatch(state, fid, source, dest, reason)

              Logger.error(
                "Shard #{state.index}: compaction planning failed for #{source}: #{inspect(reason)}"
              )

              normalized_reason =
                case reason do
                  {:copy_failed, copy_reason} -> {:copy_failed, copy_reason}
                  other -> {:compaction_plan_failed, other}
                end

              {:error, :planning, normalized_reason}
          end
        end
      end

      defp finalize_published_compaction(state, fid, source, old_size, live_count) do
        new_size = compaction_file_size(source)
        reclaimed = max(old_size - new_size, 0)

        case remove_hint_for_file(state.shard_data_path, fid) do
          :ok ->
            {:ok, live_count, reclaimed}

          {:error, reason} ->
            {:committed_error, live_count, reclaimed, {:post_publish_hint_remove_failed, reason}}
        end
      end

      defp finalize_empty_compaction_segment(
             state,
             fid,
             source,
             old_size,
             needed_tombstone_count
           ) do
        with :ok <- remove_hint_for_file(state.shard_data_path, fid) do
          if needed_tombstone_count > 0 do
            :skipped
          else
            case remove_compacted_source(state, source) do
              :ok -> {:ok, 0, old_size}
              {:error, reason} -> {:error, :removal, {:remove_failed, reason}}
            end
          end
        else
          {:error, reason} -> {:error, :removal, {:hint_remove_failed, reason}}
        end
      end

      defp compaction_file_size(path) do
        case File.lstat(path) do
          {:ok, %File.Stat{type: :regular, size: size}} -> size
          _ -> 0
        end
      end

      defp compaction_copy_records(state, source, dest, offsets, tombstone_offsets) do
        case Map.get(state, :compaction_copy_fun) do
          fun when is_function(fun, 4) ->
            fun.(source, dest, offsets, tombstone_offsets)

          _ when tombstone_offsets == [] ->
            NIF.v2_copy_records(source, dest, offsets)

          _ ->
            NIF.v2_copy_records_preserve_tombstones(
              source,
              dest,
              offsets,
              tombstone_offsets
            )
        end
      end

      defp refresh_compacted_file_stats(state, file_ids) do
        file_stats =
          Enum.reduce(file_ids, state.file_stats, fn fid, file_stats ->
            case File.lstat(file_path(state.shard_data_path, fid)) do
              {:ok, %File.Stat{type: :regular, size: new_size}} ->
                Map.put(file_stats, fid, {new_size, 0})

              _ ->
                Map.delete(file_stats, fid)
            end
          end)

        %{state | file_stats: file_stats}
      end

      defp cancel_compaction_worker(%{compaction_worker: nil} = state), do: state

      defp cancel_compaction_worker(%{compaction_worker: worker} = state) do
        Process.unlink(worker.pid)
        Process.exit(worker.pid, :kill)
        await_compaction_worker_down(worker)
        GenServer.reply(worker.from, {:error, :shard_stopping})
        %{state | compaction_worker: nil}
      end

      defp await_compaction_worker_down(worker) do
        receive do
          {:DOWN, ref, :process, pid, _reason}
          when ref == worker.monitor_ref and pid == worker.pid ->
            :ok
        end
      end

      def handle_call(:available_disk_space, _from, state) do
        {:reply, NIF.v2_available_disk_space(state.shard_data_path), state}
      end

      # Synchronous flush — used by tests and by delete to ensure durability.
      def handle_call(:flush, _from, state) do
        state =
          state
          |> flush_standalone_batch()
          |> await_standalone_flush()
          |> await_in_flight()

        state = flush_pending_sync(state)

        case Map.get(state, :last_flush_error) do
          nil -> {:reply, :ok, state}
          reason -> {:reply, {:error, {:flush_failed, reason}}, state}
        end
      end

      # Synchronous expiry sweep — used by tests to trigger a sweep and wait for
      # completion before making assertions.
      def handle_call(:expiry_sweep, _from, state) do
        state = ShardLifecycle.do_expiry_sweep(state)
        {:reply, :ok, state}
      end

      defp admin_log_file_id(name) when is_binary(name) do
        case SegmentFilename.parse(name) do
          {:ok, fid} -> fid
          _invalid_or_unrelated -> nil
        end
      end

      # -------------------------------------------------------------------
      # handle_call — catch-all for unhandled commands
      #
      # MUST be the LAST handle_call clause.
      # -------------------------------------------------------------------
      # Catch-all for commands not handled above. Routes through Batcher → Raft when Raft is
      # enabled, or directly to state machine when Raft is disabled.
      def handle_call(
            {:flush_shard_paused, flush_epoch},
            _from,
            %{writes_paused: true} = state
          ) do
        state = prepare_promoted_flush_state(state)
        sm_state = direct_sm_state(state)

        case Ferricstore.Raft.StateMachine.apply_standalone_command(
               {:flush_shard, flush_epoch},
               sm_state
             ) do
          {new_sm_state, result} ->
            new_state =
              state
              |> apply_direct_sm_state(new_sm_state)
              |> record_standalone_flush_result(result)

            {:reply, result, new_state}

          {new_sm_state, result, _effects} ->
            new_state =
              state
              |> apply_direct_sm_state(new_sm_state)
              |> record_standalone_flush_result(result)

            {:reply, result, new_state}
        end
      end

      defp record_standalone_flush_result(state, {:error, reason}) do
        state
        |> Map.put(:last_flush_error, reason)
        |> Map.put(:writes_paused, true)
      end

      defp record_standalone_flush_result(state, _success),
        do: Map.put(state, :last_flush_error, nil)

      def handle_call({:flush_shard_paused, _flush_epoch}, _from, state) do
        {:reply, {:error, :flush_shard_requires_paused_writes}, state}
      end

      def handle_call(
            {:prepare_promoted_flush, _flush_epoch},
            _from,
            %{writes_paused: true} = state
          ) do
        {:reply, :ok, prepare_promoted_flush_state(state)}
      end

      def handle_call({:prepare_promoted_flush, _flush_epoch}, _from, state) do
        {:reply, {:error, :flush_shard_requires_paused_writes}, state}
      end

      def handle_call(
            {:prepare_promoted_flush_from_raft, _flush_epoch},
            {caller_pid, _tag},
            state
          ) do
        if waraft_storage_caller?(state.index, caller_pid) do
          {:reply, :ok, prepare_promoted_flush_state(state)}
        else
          {:reply, {:error, :invalid_flush_shard_caller}, state}
        end
      end

      defp waraft_storage_caller?(shard_index, caller_pid)
           when is_integer(shard_index) and shard_index >= 0 and is_pid(caller_pid) do
        storage =
          :wa_raft_storage.registered_name(
            :ferricstore_waraft_backend,
            shard_index + 1
          )

        Process.whereis(storage) == caller_pid
      end

      defp waraft_storage_caller?(_shard_index, _caller_pid), do: false

      def handle_call(command, _from, %{writes_paused: true} = state)
          when is_tuple(command) do
        {:reply, {:error, "ERR shard writes paused for sync"}, state}
      end

      def handle_call(command, from, state) when is_tuple(command) do
        cond do
          default_waraft_write_state?(state) ->
            reply_default_waraft_write(command, state)

          state.raft? ->
            # Forward through Batcher for Raft consensus, same as put/delete.
            Ferricstore.Raft.Batcher.write_async(state.index, command, from)
            {:noreply, state}

          true ->
            # No Raft — apply directly via state machine.
            sm_state = direct_sm_state(state)

            case Ferricstore.Raft.StateMachine.apply(%{}, command, sm_state) do
              {new_sm_state, result} ->
                {:reply, result, apply_direct_sm_state(state, new_sm_state)}

              {new_sm_state, result, _effects} ->
                {:reply, result, apply_direct_sm_state(state, new_sm_state)}
            end
        end
      end
    end
  end
end
