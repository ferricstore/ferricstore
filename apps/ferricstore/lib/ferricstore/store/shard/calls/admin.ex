defmodule Ferricstore.Store.Shard.Calls.Admin do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
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
        {total_bytes, live_bytes, dead_bytes, file_count} =
          case Ferricstore.FS.ls(sp) do
            {:ok, files} ->
              log_files = Enum.filter(files, &String.ends_with?(&1, ".log"))
              fc = length(log_files)

              total =
                Enum.reduce(log_files, 0, fn name, acc ->
                  case File.stat(Path.join(sp, name)) do
                    {:ok, %{size: s}} -> acc + s
                    _ -> acc
                  end
                end)

              # Estimate: live = total / file_count (single active), dead = total - live
              live = if fc > 0, do: div(total, fc), else: 0
              dead = total - live
              {total, live, dead, fc}

            _ ->
              {0, 0, 0, 0}
          end

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
              |> Enum.filter(&String.ends_with?(&1, ".log"))
              |> Enum.flat_map(fn name ->
                fid = name |> String.trim_trailing(".log") |> String.to_integer()

                case File.stat(Path.join(sp, name)) do
                  {:ok, %{size: size}} -> [{fid, size}]
                  {:error, _} -> []
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

      def handle_call({:run_compaction, file_ids}, _from, state) do
        try do
          state = await_in_flight(state)
          state = sync_active_file_from_registry(state)
          state = flush_pending_sync(state)
          # Router async/RMW paths can leave small values queued in BitcaskWriter with
          # ETS file_id=:pending. Drain those writes before compaction snapshots ETS,
          # otherwise a source file can be removed while the writer still targets it.
          case Ferricstore.Store.BitcaskWriter.flush(state.instance_ctx, state.index) do
            :ok ->
              :ok

            {:error, reason} ->
              Logger.warning(
                "Shard #{state.index}: compaction aborted because BitcaskWriter flush failed: #{inspect(reason)}"
              )

              throw({:bitcask_writer_flush_failed, reason, state})
          end

          state = sync_active_file_from_registry(state)
          sp = state.shard_data_path
          # v2 compaction: for each file_id, collect live key offsets from ETS,
          # copy them to a new file, then replace the old file.
          # Track statistics for the merge scheduler.
          live_entries_by_fid = group_compaction_live_entries(state, file_ids)

          {total_written, total_dropped, total_reclaimed, compacted_file_ids, skipped_file_ids,
           failures} =
            Enum.reduce(file_ids, {0, 0, 0, [], [], []}, fn fid,
                                                            {written, dropped, reclaimed,
                                                             compacted, skipped, failures} ->
              source = file_path(sp, fid)
              live_entries = Map.get(live_entries_by_fid, fid, [])

              cond do
                fid == state.active_file_id ->
                  {written, dropped, reclaimed, compacted, skipped, failures}

                live_entries != [] ->
                  offsets = Enum.map(live_entries, &compaction_entry_offset/1)

                  old_size =
                    case File.stat(source) do
                      {:ok, %{size: s}} -> s
                      _ -> 0
                    end

                  dest = Path.join(sp, "compact_#{fid}.log")
                  tombstone_offsets = needed_tombstone_offsets(sp, fid, source)

                  copy_result =
                    case prepare_compaction_temp(dest) do
                      :ok ->
                        if tombstone_offsets == [] do
                          NIF.v2_copy_records(source, dest, offsets)
                        else
                          NIF.v2_copy_records_preserve_tombstones(
                            source,
                            dest,
                            offsets,
                            tombstone_offsets
                          )
                        end

                      {:error, reason} ->
                        {:error, {:temp_remove_failed, reason}}
                    end

                  case copy_result do
                    {:ok, results} when length(results) == length(live_entries) ->
                      case remove_hint_for_file(sp, fid) do
                        :ok ->
                          Ferricstore.FS.rename!(dest, source)
                          update_compacted_ets_locations(state.keydir, fid, live_entries, results)

                          cold_update =
                            update_compacted_flow_cold_locations(state, live_entries, results)

                          case cold_update do
                            :ok ->
                              new_size =
                                case File.stat(source) do
                                  {:ok, %{size: s}} -> s
                                  _ -> 0
                                end

                              {written + length(live_entries), dropped,
                               reclaimed + max(old_size - new_size, 0), [fid | compacted],
                               skipped, failures}

                            {:error, reason} ->
                              failure = {fid, :cold_flow_locator_update_failed, reason}

                              Logger.error(
                                "Shard #{state.index}: compaction cold Flow locator update failed for #{source}: #{inspect(reason)}"
                              )

                              {written, dropped, reclaimed, compacted, skipped,
                               [failure | failures]}
                          end

                        {:error, reason} ->
                          remove_compaction_temp(state, dest)

                          {written, dropped, reclaimed, compacted, skipped,
                           [{fid, {:hint_remove_failed, reason}} | failures]}
                      end

                    {:ok, results} ->
                      Logger.error(
                        "Shard #{state.index}: compaction copy_records result mismatch for #{source}: expected #{length(live_entries)}, got #{length(results)}"
                      )

                      remove_compaction_temp(state, dest)

                      failure =
                        {fid, {:copy_result_mismatch, length(live_entries), length(results)}}

                      {written, dropped, reclaimed, compacted, skipped, [failure | failures]}

                    {:error, reason} ->
                      maybe_emit_compaction_crc_mismatch(state, fid, source, dest, reason)

                      Logger.error(
                        "Shard #{state.index}: compaction copy_records failed for #{source}: #{inspect(reason)}"
                      )

                      remove_compaction_temp(state, dest)

                      {written, dropped, reclaimed, compacted, skipped,
                       [{fid, {:copy_failed, reason}} | failures]}
                  end

                true ->
                  # Tombstones are not represented in ETS, but they can still be
                  # semantically live because they suppress older values in lower file
                  # ids. Per-file compaction cannot prove those older values are gone,
                  # so keep tombstone-only files for correctness.
                  old_size =
                    case File.stat(source) do
                      {:ok, %{size: s}} -> s
                      _ -> 0
                    end

                  if tombstone_file?(source) do
                    case remove_hint_for_file(sp, fid) do
                      :ok ->
                        if tombstone_file_still_needed?(sp, fid, source) do
                          {written, dropped, reclaimed, compacted, [fid | skipped], failures}
                        else
                          case remove_compacted_source(state, source) do
                            :ok ->
                              {written, dropped, reclaimed + old_size, [fid | compacted], skipped,
                               failures}

                            {:error, reason} ->
                              {written, dropped, reclaimed, compacted, skipped,
                               [{fid, {:remove_failed, reason}} | failures]}
                          end
                        end

                      {:error, reason} ->
                        {written, dropped, reclaimed, compacted, skipped,
                         [{fid, {:hint_remove_failed, reason}} | failures]}
                    end
                  else
                    case remove_hint_for_file(sp, fid) do
                      :ok ->
                        case remove_compacted_source(state, source) do
                          :ok ->
                            {written, dropped, reclaimed + old_size, [fid | compacted], skipped,
                             failures}

                          {:error, reason} ->
                            {written, dropped, reclaimed, compacted, skipped,
                             [{fid, {:remove_failed, reason}} | failures]}
                        end

                      {:error, reason} ->
                        {written, dropped, reclaimed, compacted, skipped,
                         [{fid, {:hint_remove_failed, reason}} | failures]}
                    end
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

          # Reset file_stats for compacted files: dead bytes are now gone,
          # total bytes reflect the new compacted file size.
          new_file_stats =
            Enum.reduce(compacted_file_ids, state.file_stats, fn fid, fs ->
              case File.stat(file_path(sp, fid)) do
                {:ok, %{size: new_size}} ->
                  Map.put(fs, fid, {new_size, 0})

                _ ->
                  # File was deleted entirely (all dead)
                  Map.delete(fs, fid)
              end
            end)

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

          {:reply, reply, %{state | file_stats: new_file_stats}}
        catch
          {:bitcask_writer_flush_failed, reason, abort_state} ->
            {:reply, {:error, {:bitcask_writer_flush_failed, reason}}, abort_state}
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

      # -------------------------------------------------------------------
      # handle_call — catch-all for unhandled commands
      #
      # MUST be the LAST handle_call clause.
      # -------------------------------------------------------------------
      # Catch-all for commands not handled above (prob commands, server_command,
      # raft_apply_hook, etc.). Routes through Batcher → Raft when Raft is
      # enabled, or directly to state machine when Raft is disabled.
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
