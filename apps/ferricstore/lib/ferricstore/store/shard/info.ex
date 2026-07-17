defmodule Ferricstore.Store.Shard.Info do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Flow.Hibernation
      alias Ferricstore.Flow.LMDB
      alias Ferricstore.Flow.Locator
      alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
      alias Ferricstore.Raft.Backend, as: RaftBackend
      alias Ferricstore.Store.BlobValue
      alias Ferricstore.Store.ColdRead
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.Promotion
      alias Ferricstore.Store.ReadResult
      alias Ferricstore.Store.Router
      alias Ferricstore.Store.Shard.Compound, as: ShardCompound
      alias Ferricstore.Store.Shard.ETS, as: ShardETS
      alias Ferricstore.Store.Shard.Flush, as: ShardFlush
      alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
      alias Ferricstore.Store.Shard.NativeOps, as: ShardNativeOps
      alias Ferricstore.Store.Shard.Reads, as: ShardReads
      alias Ferricstore.Store.Shard.Transaction, as: ShardTransaction
      alias Ferricstore.Store.Shard.Writes, as: ShardWrites
      alias Ferricstore.Store.Shard.ZSetIndex
      require Logger

      def handle_info(
            {:tx_pending_compound_write, redis_key, compound_key, value, expire_at_ms},
            state
          ) do
        case ShardCompound.handle_compound_put(
               redis_key,
               compound_key,
               value,
               expire_at_ms,
               state
             ) do
          {:reply, :ok, new_state} ->
            {:noreply, new_state}

          {:reply, {:error, reason}, new_state} ->
            Logger.error(
              "Shard #{state.index}: tx promoted compound write failed: #{inspect(reason)}"
            )

            {:noreply, new_state}

          {:reply, _other, new_state} ->
            {:noreply, new_state}
        end
      end

      def handle_info({:tx_pending_compound_delete, redis_key, compound_key}, state) do
        case ShardCompound.handle_compound_delete(redis_key, compound_key, state) do
          {:reply, :ok, new_state} ->
            {:noreply, new_state}

          {:reply, {:error, reason}, new_state} ->
            Logger.error(
              "Shard #{state.index}: tx promoted compound delete failed: #{inspect(reason)}"
            )

            {:noreply, new_state}

          {:reply, _other, new_state} ->
            {:noreply, new_state}
        end
      end

      def handle_info(
            {:maybe_promote_after_commit, redis_key, compound_key, threshold},
            state
          ) do
        {:noreply, ShardCompound.maybe_promote(state, redis_key, compound_key, threshold)}
      end

      def handle_info({:start_compound_promotion, redis_key, type}, state) do
        case Map.get(state.compound_promotion_pending, redis_key) do
          ^type -> {:noreply, maybe_start_compound_promotion(state, redis_key, type)}
          _cancelled_or_stale -> {:noreply, state}
        end
      end

      def handle_info(
            {:compound_promotion_complete, job_ref, pid, result},
            %{compound_promotion_worker: %{job_ref: job_ref, pid: pid} = worker} = state
          ) do
        Process.demonitor(worker.monitor_ref, [:flush])

        state =
          state
          |> sync_active_file_from_registry()
          |> Map.put(:compound_promotion_worker, nil)
          |> refresh_active_file_size_after_compound_promotion(worker)

        case result do
          {:ok, dedicated_path} ->
            state = install_promoted_instance(state, worker.redis_key, dedicated_path)
            :ok = Promotion.clear_compound_promotion_fence(state, worker.redis_key)
            acknowledge_compound_promotion_worker(worker)

            state =
              state
              |> reply_compound_promotion_waiters(worker.redis_key)
              |> maybe_start_pending_compound_promotion()

            {:noreply, state}

          {:error, reason} ->
            Logger.error(
              "Shard #{state.index}: compound promotion failed for " <>
                "#{inspect(worker.redis_key)}: #{inspect(reason)}"
            )

            state =
              state
              |> Map.put(:promotion_recovery_required, true)
              |> reply_compound_promotion_waiters(worker.redis_key, false)

            acknowledge_compound_promotion_worker(worker)
            {:stop, {:compound_promotion_failed, reason}, state}

          unexpected ->
            reason = {:unexpected_promotion_result, unexpected}

            state =
              state
              |> Map.put(:promotion_recovery_required, true)
              |> reply_compound_promotion_waiters(worker.redis_key, false)

            acknowledge_compound_promotion_worker(worker)
            {:stop, {:compound_promotion_failed, reason}, state}
        end
      end

      def handle_info({:compound_promotion_complete, _job_ref, _pid, _result}, state) do
        {:noreply, state}
      end

      def handle_info({:remove_promoted_after_commit, redis_key}, state) do
        state =
          state
          |> cancel_promoted_compaction_retry(redis_key)
          |> Map.update!(:promoted_compaction_pending, &MapSet.delete(&1, redis_key))

        {:noreply, %{state | promoted_instances: Map.delete(state.promoted_instances, redis_key)}}
      end

      def handle_info({:promoted_maintenance_after_commit, redis_key, maintenance}, state) do
        state = ShardCompound.apply_promoted_maintenance(state, redis_key, maintenance)
        {:noreply, ShardCompound.bump_promoted_writes(state, redis_key)}
      end

      def handle_info({:maybe_compact_promoted, redis_key}, state) do
        if promoted_compaction_retry_scheduled?(state, redis_key) do
          {:noreply, state}
        else
          {:noreply, maybe_start_promoted_compaction(state, redis_key)}
        end
      end

      def handle_info({:retry_promoted_compaction, redis_key, retry_tag}, state) do
        case Map.get(state.promoted_compaction_retry_timers, redis_key) do
          %{tag: ^retry_tag} ->
            state =
              Map.update!(state, :promoted_compaction_retry_timers, &Map.delete(&1, redis_key))

            {:noreply, maybe_start_promoted_compaction(state, redis_key)}

          _stale_or_cancelled ->
            {:noreply, state}
        end
      end

      def handle_info(
            {:promoted_compaction_complete, job_ref, pid, result},
            %{promoted_compaction_worker: %{job_ref: job_ref, pid: pid} = worker} = state
          ) do
        Process.demonitor(worker.monitor_ref, [:flush])

        state =
          state
          |> finish_promoted_compaction(worker, result)
          |> Map.put(:promoted_compaction_worker, nil)
          |> maybe_schedule_failed_promoted_compaction(worker.redis_key, result)
          |> maybe_start_pending_promoted_compaction()

        {:noreply, state}
      end

      def handle_info({:promoted_compaction_complete, _job_ref, _pid, _result}, state) do
        {:noreply, state}
      end

      def handle_info({:tx_pending_write, key, value, expire_at_ms}, state) do
        new_pending = [{key, value, expire_at_ms} | state.pending]
        new_pending_count = Map.get(state, :pending_count, length(state.pending)) + 1
        new_version = state.write_version + 1

        new_state = %{
          state
          | pending: new_pending,
            pending_count: new_pending_count,
            write_version: new_version
        }

        new_state =
          if state.flush_in_flight == nil,
            do: flush_pending(new_state),
            else: new_state

        {:noreply, new_state}
      end

      def handle_info({:tx_pending_delete, key}, state) do
        if state.raft? do
          raft_write(state, {:delete, key})
          new_version = state.write_version + 1
          {:noreply, %{state | write_version: new_version}}
        else
          state = await_in_flight(state)
          state = flush_pending_sync(state)

          case NIF.v2_append_tombstone(state.active_file_path, key) do
            {:ok, _} ->
              state = track_delete_dead_bytes(state, key)
              new_pending = Enum.reject(state.pending, fn {k, _, _} -> k == key end)
              new_version = state.write_version + 1

              {:noreply,
               %{
                 state
                 | pending: new_pending,
                   pending_count: length(new_pending),
                   write_version: new_version
               }}

            {:error, reason} ->
              Logger.error(
                "Shard #{state.index}: tombstone write failed for tx_pending_delete: #{inspect(reason)}"
              )

              {:noreply, state}
          end
        end
      end

      @impl true
      def handle_cast(:sync_active_file_from_registry, state) do
        {:noreply, sync_active_file_from_registry(state)}
      end

      def handle_cast(
            {:refresh_flush_marker_accounting, file_id, file_path},
            state
          ) do
        {:noreply, refresh_active_file_size(state, file_id, file_path)}
      end

      def handle_info(:drain_pending, state) do
        # Drain any pending writes from BEAM memory to the active file
        # (page cache only — NO fsync). BitcaskCheckpointer is responsible
        # for actual disk durability on its own, longer tick.
        state = flush_pending(state)
        schedule_drain_pending(Process.get(:flush_interval_ms, @flush_interval_ms))
        {:noreply, state}
      end

      def handle_info(:standalone_commit_flush, state) do
        {:noreply, flush_standalone_batch(state)}
      end

      def handle_info({:standalone_commit_flushed, ref, result}, state) do
        {:noreply, handle_standalone_flush_result(ref, result, state)}
      end

      def handle_info({:shard_get_many_complete, job_ref, result}, state) do
        {:noreply, ShardReads.handle_get_many_complete(job_ref, result, state)}
      end

      def handle_info({:shard_get_many_timeout, job_ref}, state) do
        {:noreply, ShardReads.handle_get_many_timeout(job_ref, state)}
      end

      def handle_info(
            {:shard_compaction_complete, job_ref, pid, {reply, compacted_file_ids}},
            %{
              compaction_worker: %{job_ref: job_ref, pid: pid} = worker
            } = state
          ) do
        Process.demonitor(worker.monitor_ref, [:flush])
        GenServer.reply(worker.from, reply)

        state =
          state
          |> refresh_compacted_file_stats(compacted_file_ids)
          |> Map.put(:compaction_worker, nil)

        {:noreply, state}
      end

      def handle_info({:shard_compaction_complete, _job_ref, _pid, _result}, state) do
        {:noreply, state}
      end

      def handle_info(
            {:DOWN, monitor_ref, :process, pid, reason},
            %{compaction_worker: %{monitor_ref: monitor_ref, pid: pid} = worker} = state
          ) do
        GenServer.reply(worker.from, {:error, {:compaction_worker_failed, reason}})
        {:noreply, %{state | compaction_worker: nil}}
      end

      def handle_info(
            {:DOWN, monitor_ref, :process, pid, reason},
            %{
              compound_promotion_worker:
                %{monitor_ref: monitor_ref, pid: pid, redis_key: redis_key} = worker
            } = state
          ) do
        Logger.error(
          "Shard #{state.index}: compound promotion worker failed for " <>
            "#{inspect(redis_key)}: #{inspect(reason)}"
        )

        state =
          state
          |> Map.put(:compound_promotion_worker, nil)
          |> Map.put(:promotion_recovery_required, true)
          |> sync_active_file_from_registry()
          |> refresh_active_file_size_after_compound_promotion(worker)
          |> reply_compound_promotion_waiters(redis_key, false)

        release_compound_promotion_worker_latches(worker)
        {:stop, {:compound_promotion_worker_failed, reason}, state}
      end

      def handle_info(
            {:DOWN, monitor_ref, :process, pid, reason},
            %{
              promoted_compaction_worker:
                %{
                  monitor_ref: monitor_ref,
                  pid: pid,
                  redis_key: redis_key
                } = worker
            } = state
          ) do
        release_promoted_compaction_latch_if_owned(
          Map.get(worker, :latch_token, :none),
          worker.pid
        )

        Logger.error(
          "Shard #{state.index}: promoted compaction worker failed for " <>
            "#{inspect(redis_key)}: #{inspect(reason)}"
        )

        state =
          state
          |> Map.put(:promoted_compaction_worker, nil)
          |> schedule_promoted_compaction_retry(redis_key)
          |> maybe_start_pending_promoted_compaction()

        {:noreply, state}
      end

      def handle_info(
            {:DOWN, monitor_ref, :process, owner_pid, _reason},
            %{
              standalone_write_barrier: %{
                monitor_ref: monitor_ref,
                owner_pid: owner_pid
              }
            } = state
          ) do
        {:noreply, release_standalone_write_barrier(state)}
      end

      def handle_info({:DOWN, monitor_ref, :process, owner_pid, _reason}, state) do
        case Map.get(state.write_pause_monitors, monitor_ref) do
          nil ->
            case ShardReads.handle_get_many_down(monitor_ref, state) do
              {:handled, state} -> {:noreply, state}
              :unhandled -> {:noreply, state}
            end

          lease_ref ->
            state =
              release_write_pause_lease(
                state,
                {owner_pid, lease_ref},
                false
              )

            {:noreply, state}
        end
      end

      defp maybe_start_compound_promotion(
             %{compound_promotion_worker: nil} = state,
             redis_key,
             type
           ) do
        state =
          Map.update!(state, :compound_promotion_pending, &Map.delete(&1, redis_key))

        case ShardCompound.promoted_store(state, redis_key) do
          path when is_binary(path) ->
            state
            |> install_promoted_instance(redis_key, path)
            |> reply_compound_promotion_waiters(redis_key)
            |> maybe_start_pending_compound_promotion()

          nil ->
            state = ShardFlush.await_in_flight(state)
            state = ShardFlush.flush_pending_sync(state)
            parent = self()
            job_ref = make_ref()
            :ok = Promotion.clear_compound_promotion_fence(state, redis_key)
            latch_token = Promotion.acquire_compaction_latch(state, redis_key)
            :ok = Promotion.record_compound_promotion_running(state, redis_key)
            shared_log_latch_token = Promotion.acquire_shared_log_latch(state)
            state = sync_active_file_from_registry(state)

            {pid, monitor_ref} =
              spawn_compound_promotion_worker(
                state,
                redis_key,
                type,
                parent,
                job_ref,
                latch_token,
                shared_log_latch_token
              )

            worker = %{
              job_ref: job_ref,
              monitor_ref: monitor_ref,
              pid: pid,
              redis_key: redis_key,
              type: type,
              latch_token: latch_token,
              shared_log_latch_token: shared_log_latch_token,
              active_file_id: state.active_file_id,
              active_file_path: state.active_file_path
            }

            %{state | compound_promotion_worker: worker}
        end
      end

      defp maybe_start_compound_promotion(state, _redis_key, _type), do: state

      defp spawn_compound_promotion_worker(
             state,
             redis_key,
             type,
             parent,
             job_ref,
             latch_token,
             shared_log_latch_token
           ) do
        {pid, monitor_ref} =
          try do
            :erlang.spawn_opt(
              fn ->
                try do
                  receive do
                    {:start_compound_promotion_worker, ^job_ref} ->
                      result =
                        try do
                          Promotion.promote_collection!(
                            type,
                            redis_key,
                            state.shard_data_path,
                            state.keydir,
                            state.data_dir,
                            state.index,
                            state.instance_ctx,
                            state.compound_member_index
                          )
                        rescue
                          error ->
                            {:error,
                             {:exception, error.__struct__, Exception.message(error),
                              __STACKTRACE__}}
                        catch
                          kind, reason -> {:error, {kind, reason, __STACKTRACE__}}
                        end

                      case result do
                        {:ok, _dedicated_path} ->
                          Promotion.record_compound_promotion_success(state, redis_key)

                        {:error, reason} ->
                          Promotion.record_compound_promotion_failure(
                            state,
                            redis_key,
                            reason
                          )

                        unexpected ->
                          Promotion.record_compound_promotion_failure(
                            state,
                            redis_key,
                            {:unexpected_promotion_result, unexpected}
                          )
                      end

                      release_promoted_compaction_latch_if_owned(latch_token, self())

                      release_promoted_compaction_latch_if_owned(
                        shared_log_latch_token,
                        self()
                      )

                      send(parent, {:compound_promotion_complete, job_ref, self(), result})
                  after
                    5_000 -> :ok
                  end
                after
                  release_promoted_compaction_latch_if_owned(latch_token, self())

                  release_promoted_compaction_latch_if_owned(
                    shared_log_latch_token,
                    self()
                  )
                end
              end,
              [:link, :monitor]
            )
          catch
            kind, reason ->
              release_promoted_compaction_latch_if_owned(latch_token, self())

              release_promoted_compaction_latch_if_owned(
                shared_log_latch_token,
                self()
              )

              :erlang.raise(kind, reason, __STACKTRACE__)
          end

        try do
          transfer_promoted_compaction_latch(latch_token, pid)
          transfer_promoted_compaction_latch(shared_log_latch_token, pid)
          send(pid, {:start_compound_promotion_worker, job_ref})
          {pid, monitor_ref}
        catch
          kind, reason ->
            Process.exit(pid, :kill)
            Process.demonitor(monitor_ref, [:flush])
            release_promoted_compaction_latch_if_owned(latch_token, self())
            release_promoted_compaction_latch_if_owned(latch_token, pid)

            release_promoted_compaction_latch_if_owned(
              shared_log_latch_token,
              self()
            )

            release_promoted_compaction_latch_if_owned(shared_log_latch_token, pid)
            :erlang.raise(kind, reason, __STACKTRACE__)
        end
      end

      defp acknowledge_compound_promotion_worker(worker) do
        release_compound_promotion_worker_latches(worker)
        :ok
      end

      defp release_compound_promotion_worker_latches(worker) do
        release_promoted_compaction_latch_if_owned(
          Map.get(worker, :latch_token, :none),
          worker.pid
        )

        release_promoted_compaction_latch_if_owned(
          Map.get(worker, :shared_log_latch_token, :none),
          worker.pid
        )
      end

      defp refresh_active_file_size_after_compound_promotion(state, worker) do
        file_id = Map.get(worker, :active_file_id, state.active_file_id)
        file_path = Map.get(worker, :active_file_path, state.active_file_path)
        refresh_active_file_size(state, file_id, file_path)
      end

      defp refresh_active_file_size(state, file_id, file_path) do
        case File.lstat(file_path) do
          {:ok, %File.Stat{type: :regular, size: size}}
          when is_integer(size) and size >= 0 ->
            {_total, dead} =
              Map.get(state.file_stats, file_id, {state.active_file_size, 0})

            state = %{state | file_stats: Map.put(state.file_stats, file_id, {size, dead})}

            if state.active_file_id == file_id and state.active_file_path == file_path do
              %{state | active_file_size: size}
            else
              state
            end

          stat_error ->
            Logger.error(
              "Shard #{state.index}: failed to reconcile active log size after promotion: " <>
                inspect(stat_error)
            )

            state
        end
      end

      defp install_promoted_instance(state, redis_key, dedicated_path) do
        info = %{
          path: dedicated_path,
          writes: 0,
          total_bytes: ShardCompound.promoted_dir_size(dedicated_path),
          dead_bytes: 0,
          last_compacted_at: nil
        }

        %{state | promoted_instances: Map.put(state.promoted_instances, redis_key, info)}
      end

      defp maybe_start_pending_compound_promotion(state) do
        case Enum.at(state.compound_promotion_pending, 0) do
          {redis_key, type} -> maybe_start_compound_promotion(state, redis_key, type)
          nil -> state
        end
      end

      defp prepare_promoted_flush_state(state) do
        state
        |> cancel_compound_promotion_worker_for_flush()
        |> cancel_promoted_compaction_worker_and_retries()
        |> reply_all_compound_promotion_waiters(false)
        |> Map.put(:compound_promotion_pending, %{})
        |> Map.put(:promoted_compaction_pending, MapSet.new())
        |> Map.put(:promoted_instances, %{})
        |> tap(&Promotion.clear_compound_promotion_fences/1)
      end

      defp cancel_compound_promotion_worker_for_flush(%{compound_promotion_worker: nil} = state),
        do: state

      defp cancel_compound_promotion_worker_for_flush(
             %{compound_promotion_worker: worker} = state
           ) do
        if Process.alive?(worker.pid) do
          Process.unlink(worker.pid)
          Process.exit(worker.pid, :kill)
        end

        await_compaction_worker_down(worker)
        release_compound_promotion_worker_latches(worker)

        state
        |> sync_active_file_from_registry()
        |> refresh_active_file_size_after_compound_promotion(worker)
        |> Map.put(:compound_promotion_worker, nil)
      end

      defp reply_all_compound_promotion_waiters(state, promoted?) when is_boolean(promoted?) do
        state.compound_promotion_waiters
        |> Map.values()
        |> List.flatten()
        |> Enum.each(&GenServer.reply(&1, promoted?))

        %{state | compound_promotion_waiters: %{}}
      end

      defp reply_compound_promotion_waiters(state, redis_key) do
        promoted? = ShardCompound.promoted_store(state, redis_key) != nil
        reply_compound_promotion_waiters(state, redis_key, promoted?)
      end

      defp reply_compound_promotion_waiters(state, redis_key, promoted?)
           when is_boolean(promoted?) do
        {waiters, remaining} =
          Map.pop(Map.get(state, :compound_promotion_waiters, %{}), redis_key, [])

        Enum.each(waiters, &GenServer.reply(&1, promoted?))
        Map.put(state, :compound_promotion_waiters, remaining)
      end

      defp maybe_start_promoted_compaction(
             %{promoted_compaction_worker: nil} = state,
             redis_key
           ) do
        case Map.get(state.promoted_instances, redis_key) do
          %{path: path, dead_bytes: baseline_dead}
          when is_binary(path) and is_integer(baseline_dead) and baseline_dead >= 0 ->
            if ShardCompound.promoted_compaction_due?(state, redis_key) do
              parent = self()
              job_ref = make_ref()
              latch_token = Promotion.acquire_compaction_latch(state, redis_key)

              {pid, monitor_ref} =
                spawn_promoted_compaction_worker(
                  state,
                  redis_key,
                  path,
                  parent,
                  job_ref,
                  latch_token
                )

              worker = %{
                job_ref: job_ref,
                monitor_ref: monitor_ref,
                pid: pid,
                redis_key: redis_key,
                path: path,
                baseline_dead: baseline_dead,
                latch_token: latch_token
              }

              %{state | promoted_compaction_worker: worker}
            else
              state
            end

          _missing ->
            state
        end
      end

      defp maybe_start_promoted_compaction(state, redis_key) do
        Map.update!(state, :promoted_compaction_pending, &MapSet.put(&1, redis_key))
      end

      defp spawn_promoted_compaction_worker(
             state,
             redis_key,
             path,
             parent,
             job_ref,
             latch_token
           ) do
        {pid, monitor_ref} =
          try do
            spawn_monitor(fn ->
              receive do
                {:start_promoted_compaction, ^job_ref} ->
                  {status, _worker_state} =
                    try do
                      ShardCompound.compact_dedicated_result_latched(state, redis_key, path)
                    after
                      Promotion.release_compaction_latch(latch_token)
                    end

                  send(parent, {:promoted_compaction_complete, job_ref, self(), status})
              after
                5_000 -> :ok
              end
            end)
          catch
            kind, reason ->
              release_promoted_compaction_latch_if_owned(latch_token, self())
              :erlang.raise(kind, reason, __STACKTRACE__)
          end

        try do
          transfer_promoted_compaction_latch(latch_token, pid)
          send(pid, {:start_promoted_compaction, job_ref})
          {pid, monitor_ref}
        catch
          kind, reason ->
            Process.exit(pid, :kill)
            Process.demonitor(monitor_ref, [:flush])
            release_promoted_compaction_latch_if_owned(latch_token, self())
            release_promoted_compaction_latch_if_owned(latch_token, pid)
            :erlang.raise(kind, reason, __STACKTRACE__)
        end
      end

      defp transfer_promoted_compaction_latch(:none, _pid), do: :ok

      defp transfer_promoted_compaction_latch({table, latch_key}, pid) when is_pid(pid) do
        case :ets.lookup(table, latch_key) do
          [{^latch_key, owner}] when owner == self() ->
            true = :ets.update_element(table, latch_key, {2, pid})
            :ok

          other ->
            raise "promoted compaction latch transfer failed: #{inspect(other)}"
        end
      end

      defp release_promoted_compaction_latch_if_owned(:none, _owner), do: :ok

      defp release_promoted_compaction_latch_if_owned({table, latch_key}, owner) do
        :ets.delete_object(table, {latch_key, owner})
        :ok
      rescue
        ArgumentError -> :ok
      end

      defp finish_promoted_compaction(state, worker, :ok) do
        case Map.get(state.promoted_instances, worker.redis_key) do
          %{dead_bytes: current_dead} = info when is_integer(current_dead) ->
            updated = %{
              info
              | total_bytes: ShardCompound.promoted_dir_size(worker.path),
                dead_bytes: max(current_dead - worker.baseline_dead, 0),
                last_compacted_at: System.monotonic_time(:millisecond)
            }

            %{
              state
              | promoted_instances: Map.put(state.promoted_instances, worker.redis_key, updated)
            }

          _removed ->
            state
        end
      end

      defp finish_promoted_compaction(state, _worker, :error), do: state

      defp maybe_schedule_failed_promoted_compaction(state, redis_key, :error),
        do: schedule_promoted_compaction_retry(state, redis_key)

      defp maybe_schedule_failed_promoted_compaction(state, _redis_key, :ok), do: state

      defp schedule_promoted_compaction_retry(state, redis_key) do
        state =
          Map.update!(state, :promoted_compaction_pending, &MapSet.delete(&1, redis_key))

        if promoted_compaction_retry_scheduled?(state, redis_key) do
          state
        else
          retry_tag = make_ref()

          timer_ref =
            Process.send_after(
              self(),
              {:retry_promoted_compaction, redis_key, retry_tag},
              state.promoted_compaction_retry_ms
            )

          retry = %{tag: retry_tag, timer_ref: timer_ref}

          Map.update!(state, :promoted_compaction_retry_timers, &Map.put(&1, redis_key, retry))
        end
      end

      defp cancel_promoted_compaction_retry(state, redis_key) do
        case Map.pop(state.promoted_compaction_retry_timers, redis_key) do
          {nil, _timers} ->
            state

          {%{timer_ref: timer_ref}, timers} ->
            _ = Process.cancel_timer(timer_ref, async: false, info: false)
            %{state | promoted_compaction_retry_timers: timers}
        end
      end

      defp promoted_compaction_retry_scheduled?(state, redis_key) do
        Map.has_key?(state.promoted_compaction_retry_timers, redis_key)
      end

      defp maybe_start_pending_promoted_compaction(state) do
        case Enum.at(state.promoted_compaction_pending, 0) do
          nil ->
            state

          redis_key ->
            state =
              Map.update!(state, :promoted_compaction_pending, &MapSet.delete(&1, redis_key))

            case maybe_start_promoted_compaction(state, redis_key) do
              %{promoted_compaction_worker: nil} = state ->
                maybe_start_pending_promoted_compaction(state)

              state ->
                state
            end
        end
      end

      # Periodic fragmentation re-evaluation for idle shards.
      # Catches shards that accumulated dead data then stopped receiving writes.
      # Disk pressure is intentionally not cleared here; only a successful append or
      # fsync proves that the shard can accept writes again.
      def handle_info(:frag_check, state) do
        state = maybe_notify_fragmentation(state)
        ShardLifecycle.schedule_frag_check()
        {:noreply, state}
      end

      # Active expiry sweep: scan ETS for expired keys and delete them.
      # When the sweep finds nothing to expire and there are no pending writes
      # or in-flight flushes, hibernate the GenServer to trigger a full GC
      # and shrink the heap. This reclaims memory accumulated during busy periods
      # on idle shards (memory audit L1).
      def handle_info(:expiry_sweep, state) do
        state = ShardLifecycle.do_expiry_sweep(state)
        ShardLifecycle.schedule_expiry_sweep()

        if state.sweep_at_ceiling_count == 0 and
             state.pending == [] and
             state.pending_count == 0 and
             state.flush_in_flight == nil do
          {:noreply, state, :hibernate}
        else
          {:noreply, state}
        end
      end

      # Handle async io_uring completion message from the NIF background thread.
      def handle_info({:io_complete, op_id, result}, state) do
        if state.flush_in_flight == op_id do
          case result do
            :ok ->
              {:noreply, %{state | flush_in_flight: nil}}

            {:error, reason} ->
              # The async flush failed. Log the error but clear in-flight so
              # the next timer tick can attempt another flush. The keydir was
              # updated optimistically by prepare_batch_for_async — on the next
              # store open, log replay will reconcile.
              Logger.error(
                "Shard #{state.index}: async flush failed for op #{op_id}: #{inspect(reason)}"
              )

              {:noreply, %{state | flush_in_flight: nil}}
          end
        else
          # Stale or unknown op_id — ignore.
          {:noreply, state}
        end
      end

      # Handle Tokio async completion with correlation ID.
      # Dispatches to fsync completion (flush_in_flight match) or read completion
      # (pending_reads lookup).
      def handle_info({:cold_read_timeout, corr_id}, state) do
        case pop_pending_read(state.pending_reads, corr_id, :keep_timer) do
          {{from, _key, _exp, _fid, _off, _vsize} = pending_entry, rest_pending} ->
            emit_pending_read_error(state, pending_entry, :timeout)
            GenServer.reply(from, ReadResult.failure({:cold_read_failed, :timeout}))
            {:noreply, %{state | pending_reads: rest_pending}}

          {{from, _key, :meta, _exp, _fid, _off, _vsize} = pending_entry, rest_pending} ->
            emit_pending_read_error(state, pending_entry, :timeout)
            GenServer.reply(from, ReadResult.failure({:cold_read_failed, :timeout}))
            {:noreply, %{state | pending_reads: rest_pending}}

          {nil, _} ->
            {:noreply, state}
        end
      end

      def handle_info({:cold_read_retry, pending_entry, attempts_left}, state) do
        retry_or_reply_nil_cold_read(state, pending_entry, attempts_left)
      end

      def handle_info({:tokio_complete, corr_id, :ok, value}, state) do
        cond do
          # Async fsync completion — value is :ok for fsync
          corr_id == state.flush_in_flight ->
            {:noreply, %{state | flush_in_flight: nil}}

          # Async read completion — look up in pending_reads
          true ->
            case pop_pending_read(state.pending_reads, corr_id, :cancel_timer) do
              {{from, key, exp, fid, off, vsize}, rest_pending} ->
                # Simple GET cold-read completion. Warm only if the ETS entry still
                # points at the same disk location read by this request.
                if value != nil do
                  case materialize_pending_cold_value(state, value) do
                    {:ok, materialized} ->
                      ShardETS.cold_read_warm_ets(state, key, materialized, exp, fid, off, vsize)
                      GenServer.reply(from, materialized)
                      {:noreply, %{state | pending_reads: rest_pending}}

                    {:error, _reason} ->
                      retry_or_reply_nil_cold_read(
                        %{state | pending_reads: rest_pending},
                        {from, key, exp, fid, off, vsize},
                        @cold_read_compaction_retry_attempts
                      )
                  end
                else
                  retry_or_reply_nil_cold_read(
                    %{state | pending_reads: rest_pending},
                    {from, key, exp, fid, off, vsize},
                    @cold_read_compaction_retry_attempts
                  )
                end

              {{from, key, :meta, exp, fid, off, vsize}, rest_pending} ->
                # GET_META cold-read completion. The reply may linearize before a
                # later overwrite, but ETS warming must still be location-checked.
                if value != nil do
                  case materialize_pending_cold_value(state, value) do
                    {:ok, materialized} ->
                      ShardETS.cold_read_warm_ets(state, key, materialized, exp, fid, off, vsize)
                      GenServer.reply(from, {materialized, exp})
                      {:noreply, %{state | pending_reads: rest_pending}}

                    {:error, _reason} ->
                      retry_or_reply_nil_cold_read(
                        %{state | pending_reads: rest_pending},
                        {from, key, :meta, exp, fid, off, vsize},
                        @cold_read_compaction_retry_attempts
                      )
                  end
                else
                  retry_or_reply_nil_cold_read(
                    %{state | pending_reads: rest_pending},
                    {from, key, :meta, exp, fid, off, vsize},
                    @cold_read_compaction_retry_attempts
                  )
                end

              {nil, _} ->
                # Unknown correlation_id — could be a stale fsync or read. Ignore.
                {:noreply, state}
            end
        end
      end

      def handle_info({:tokio_complete, corr_id, :error, reason}, state) do
        if corr_id == state.flush_in_flight do
          # Async fsync error completion.
          Logger.error(
            "Shard #{state.index}: async fsync failed for corr_id #{corr_id}: #{inspect(reason)}"
          )

          {:noreply, %{state | flush_in_flight: nil}}
        else
          case pop_pending_read(state.pending_reads, corr_id, :cancel_timer) do
            {{from, _key, _exp, _fid, _off, _vsize} = pending_entry, rest_pending} ->
              emit_pending_read_error(state, pending_entry, reason)
              GenServer.reply(from, ReadResult.failure({:cold_read_failed, reason}))
              {:noreply, %{state | pending_reads: rest_pending}}

            {{from, _key, :meta, _exp, _fid, _off, _vsize} = pending_entry, rest_pending} ->
              emit_pending_read_error(state, pending_entry, reason)
              GenServer.reply(from, ReadResult.failure({:cold_read_failed, reason}))
              {:noreply, %{state | pending_reads: rest_pending}}

            {nil, _} ->
              {:noreply, state}
          end
        end
      end

      # Catch-all for unexpected messages. Without this, any unmatched message
      # (stale timer, DOWN from a linked process, etc.) would crash the shard
      # GenServer, causing a restart and temporary unavailability.
      def handle_info(_msg, state) do
        {:noreply, state}
      end

      defp pop_pending_read(pending_reads, corr_id, timer_action) do
        case Map.pop(pending_reads, corr_id) do
          {{:pending_read, entry, timer_ref}, rest_pending} ->
            maybe_cancel_pending_timer(timer_action, timer_ref)
            {entry, rest_pending}

          {nil, rest_pending} ->
            {nil, rest_pending}

          {_invalid_entry, rest_pending} ->
            {nil, rest_pending}
        end
      end

      defp maybe_cancel_pending_timer(:cancel_timer, timer_ref) when is_reference(timer_ref) do
        _ = Process.cancel_timer(timer_ref, async: false, info: false)
        :ok
      end

      defp maybe_cancel_pending_timer(_timer_action, _timer_ref), do: :ok

      defp materialize_pending_cold_value(
             %{data_dir: data_dir, index: shard_index} = state,
             value
           ) do
        BlobValue.maybe_materialize(
          data_dir,
          shard_index,
          BlobValue.threshold(Map.get(state, :instance_ctx)),
          value
        )
      end

      defp materialize_pending_cold_value(_state, value), do: {:ok, value}

      defp emit_pending_read_error(state, {_from, _key, _exp, fid, _off, _vsize}, reason) do
        emit_pending_read_error_for_fid(state, fid, reason)
      end

      defp emit_pending_read_error(state, {_from, _key, :meta, _exp, fid, _off, _vsize}, reason) do
        emit_pending_read_error_for_fid(state, fid, reason)
      end

      defp emit_pending_read_error(_state, _pending_entry, _reason), do: :ok

      defp emit_pending_read_error_for_fid(%{shard_data_path: shard_data_path}, fid, reason)
           when is_binary(shard_data_path) and is_integer(fid) and fid >= 0 do
        shard_data_path
        |> ShardETS.file_path(fid)
        |> ColdRead.emit_pread_error(reason)
      rescue
        _ -> :ok
      end

      defp emit_pending_read_error_for_fid(_state, _fid, _reason), do: :ok

      defp retry_or_reply_nil_cold_read(state, pending_entry, attempts_left) do
        case resolve_nil_cold_read(state, pending_entry, attempts_left) do
          {:reply, from, reply} ->
            GenServer.reply(from, reply)
            {:noreply, state}

          {:retry_later, pending_entry, next_attempts_left} ->
            Process.send_after(
              self(),
              {:cold_read_retry, pending_entry, next_attempts_left},
              @cold_read_compaction_retry_delay_ms
            )

            {:noreply, state}

          {:resubmit, pending_entry} ->
            resubmit_cold_read(state, pending_entry)
        end
      end

      defp resolve_nil_cold_read(
             state,
             {from, key, _exp, fid, off, vsize} = pending_entry,
             attempts_left
           ) do
        case ShardETS.ets_lookup(state, key) do
          {:hit, value, _expire_at_ms} ->
            {:reply, from, value}

          {:cold, ^fid, ^off, ^vsize, _exp} when attempts_left > 0 ->
            {:retry_later, pending_entry, attempts_left - 1}

          {:cold, ^fid, ^off, ^vsize, _exp} ->
            emit_cold_retry_exhausted(state, pending_entry, :get, :missing_live_cold_entry)
            emit_pending_read_error(state, pending_entry, :missing_live_cold_entry)

            {:reply, from, ReadResult.failure({:cold_read_failed, :missing_live_cold_entry})}

          {:cold, new_fid, new_off, new_vsize, new_exp} ->
            {:resubmit, {from, key, new_exp, new_fid, new_off, new_vsize}}

          :expired ->
            {:reply, from, nil}

          :miss ->
            {:reply, from, nil}

          {:error, :invalid_keydir_entry} ->
            {:reply, from, ReadResult.failure(:invalid_keydir_entry)}

          {:error, {:storage_read_failed, _reason}} = failure ->
            {:reply, from, failure}
        end
      end

      defp resolve_nil_cold_read(
             state,
             {from, key, :meta, _exp, fid, off, vsize} = pending_entry,
             attempts_left
           ) do
        case ShardETS.ets_lookup(state, key) do
          {:hit, value, expire_at_ms} ->
            {:reply, from, {value, expire_at_ms}}

          {:cold, ^fid, ^off, ^vsize, _exp} when attempts_left > 0 ->
            {:retry_later, pending_entry, attempts_left - 1}

          {:cold, ^fid, ^off, ^vsize, _exp} ->
            emit_cold_retry_exhausted(state, pending_entry, :get_meta, :missing_live_cold_entry)
            emit_pending_read_error(state, pending_entry, :missing_live_cold_entry)

            {:reply, from, ReadResult.failure({:cold_read_failed, :missing_live_cold_entry})}

          {:cold, new_fid, new_off, new_vsize, new_exp} ->
            {:resubmit, {from, key, :meta, new_exp, new_fid, new_off, new_vsize}}

          :expired ->
            {:reply, from, nil}

          :miss ->
            {:reply, from, nil}

          {:error, :invalid_keydir_entry} ->
            {:reply, from, ReadResult.failure(:invalid_keydir_entry)}

          {:error, {:storage_read_failed, _reason}} = failure ->
            {:reply, from, failure}
        end
      end

      defp emit_cold_retry_exhausted(
             %{index: shard_index, shard_data_path: shard_data_path},
             {_from, key, _exp, fid, off, vsize},
             operation,
             reason
           )
           when is_binary(shard_data_path) and is_integer(fid) and fid >= 0 do
        emit_cold_retry_exhausted_for_location(
          shard_index,
          shard_data_path,
          key,
          fid,
          off,
          vsize,
          operation,
          reason
        )
      end

      defp emit_cold_retry_exhausted(
             %{index: shard_index, shard_data_path: shard_data_path},
             {_from, key, :meta, _exp, fid, off, vsize},
             operation,
             reason
           )
           when is_binary(shard_data_path) and is_integer(fid) and fid >= 0 do
        emit_cold_retry_exhausted_for_location(
          shard_index,
          shard_data_path,
          key,
          fid,
          off,
          vsize,
          operation,
          reason
        )
      end

      defp emit_cold_retry_exhausted(_state, _pending_entry, _operation, _reason), do: :ok

      defp emit_cold_retry_exhausted_for_location(
             shard_index,
             shard_data_path,
             key,
             fid,
             off,
             vsize,
             operation,
             reason
           ) do
        path = ShardETS.file_path(shard_data_path, fid)

        :telemetry.execute(
          [:ferricstore, :store, :cold_read_retry_exhausted],
          %{count: 1, attempts: @cold_read_compaction_retry_attempts},
          %{
            source: :shard,
            operation: operation,
            shard_index: shard_index,
            redis_key_hash: :erlang.phash2(key),
            path: path,
            file_id: fid,
            offset: off,
            value_size: vsize,
            reason: reason
          }
        )
      rescue
        _ -> :ok
      end

      defp resubmit_cold_read(state, {from, key, _exp, fid, off, _vsize} = pending_entry) do
        path = ShardETS.file_path(state.shard_data_path, fid)
        corr_id = state.next_correlation_id + 1

        case NIF.v2_pread_at_key_async(self(), corr_id, path, off, key) do
          :ok ->
            timer_ref =
              Process.send_after(self(), {:cold_read_timeout, corr_id}, @cold_read_timeout_ms)

            {:noreply,
             %{
               state
               | next_correlation_id: corr_id,
                 pending_reads:
                   Map.put(
                     state.pending_reads,
                     corr_id,
                     {:pending_read, pending_entry, timer_ref}
                   )
             }}

          {:error, reason} ->
            emit_pending_read_error(state, pending_entry, reason)
            GenServer.reply(from, ReadResult.failure({:cold_read_failed, reason}))
            {:noreply, state}
        end
      end

      defp resubmit_cold_read(state, {from, key, :meta, _exp, fid, off, _vsize} = pending_entry) do
        path = ShardETS.file_path(state.shard_data_path, fid)
        corr_id = state.next_correlation_id + 1

        case NIF.v2_pread_at_key_async(self(), corr_id, path, off, key) do
          :ok ->
            timer_ref =
              Process.send_after(self(), {:cold_read_timeout, corr_id}, @cold_read_timeout_ms)

            {:noreply,
             %{
               state
               | next_correlation_id: corr_id,
                 pending_reads:
                   Map.put(
                     state.pending_reads,
                     corr_id,
                     {:pending_read, pending_entry, timer_ref}
                   )
             }}

          {:error, reason} ->
            emit_pending_read_error(state, pending_entry, reason)
            GenServer.reply(from, ReadResult.failure({:cold_read_failed, reason}))
            {:noreply, state}
        end
      end

      # -------------------------------------------------------------------
      # Graceful shutdown (spec 2C.6, step 8)
      #
      # OTP calls terminate/2 when the supervisor stops this child during
      # application shutdown (children are stopped in reverse start order).
      # We flush pending writes, write the Bitcask hint file, and emit
      # telemetry so operators can observe shutdown timing.
      # -------------------------------------------------------------------

      @impl true
      def terminate(reason, state) do
        state
        |> cancel_compaction_worker()
        |> cancel_compound_promotion_worker()
        |> cancel_promoted_compaction_worker_and_retries()
        |> flush_standalone_batch()
        |> await_standalone_flush()
        |> then(&ShardLifecycle.do_terminate(reason, &1))
      end

      defp cancel_compound_promotion_worker(state) do
        case state.compound_promotion_worker do
          nil ->
            %{state | compound_promotion_pending: %{}}

          worker ->
            Process.exit(worker.pid, :kill)
            await_compaction_worker_down(worker)
            release_compound_promotion_worker_latches(worker)

            %{
              state
              | compound_promotion_worker: nil,
                compound_promotion_pending: %{}
            }
            |> Map.put(:promotion_recovery_required, true)
        end
      end

      defp cancel_promoted_compaction_worker_and_retries(state) do
        Enum.each(state.promoted_compaction_retry_timers, fn {_redis_key, %{timer_ref: timer_ref}} ->
          _ = Process.cancel_timer(timer_ref, async: false, info: false)
        end)

        case state.promoted_compaction_worker do
          nil ->
            %{state | promoted_compaction_retry_timers: %{}}

          worker ->
            Process.exit(worker.pid, :kill)
            await_compaction_worker_down(worker)

            release_promoted_compaction_latch_if_owned(
              Map.get(worker, :latch_token, :none),
              worker.pid
            )

            %{
              state
              | promoted_compaction_worker: nil,
                promoted_compaction_retry_timers: %{}
            }
        end
      end

      # -------------------------------------------------------------------
      # Private: flush
      # -------------------------------------------------------------------

      defp flush_pending(state), do: ShardFlush.flush_pending(state)
      defp flush_pending_sync(state), do: ShardFlush.flush_pending_sync(state)
      defp await_in_flight(state), do: ShardFlush.await_in_flight(state)
      defp track_delete_dead_bytes(state, key), do: ShardFlush.track_delete_dead_bytes(state, key)
      defp maybe_notify_fragmentation(state), do: ShardFlush.maybe_notify_fragmentation(state)

      defp compute_file_stats(shard_path, keydir),
        do: ShardFlush.compute_file_stats(shard_path, keydir)

      defp compaction_fsync_dir(state, path) do
        case Map.get(state, :compaction_fsync_dir_fun) do
          fun when is_function(fun, 1) -> fun.(path)
          _ -> NIF.v2_fsync_dir(path)
        end
      end

      defp schedule_drain_pending(ms), do: ShardFlush.schedule_drain_pending(ms)

      # -------------------------------------------------------------------
      # Private: read helpers (delegates to Shard.Reads / Shard.ETS)
      # -------------------------------------------------------------------

      defp prefix_scan_entries(state_or_keydir, prefix, shard_data_path),
        do: ShardETS.prefix_scan_entries(state_or_keydir, prefix, shard_data_path)

      defp prefix_count_entries(state_or_keydir, prefix),
        do: ShardETS.prefix_count_entries(state_or_keydir, prefix)

      # -------------------------------------------------------------------
      # Private: Raft write helpers
      # -------------------------------------------------------------------

      # Submits a write command through Raft via the Batcher (group commit).
      defp raft_write(%__MODULE__{index: index}, command) do
        Ferricstore.Raft.Batcher.write(index, command)
      end
    end
  end
end
