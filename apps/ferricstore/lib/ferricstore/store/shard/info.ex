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

      def handle_info({:tx_pending_write, key, value, expire_at_ms}, state) do
        new_pending = [{key, value, expire_at_ms} | state.pending]
        new_version = state.write_version + 1
        new_state = %{state | pending: new_pending, write_version: new_version}

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
          state = track_delete_dead_bytes(state, key)

          case NIF.v2_append_tombstone(state.active_file_path, key) do
            {:ok, _} ->
              new_pending = Enum.reject(state.pending, fn {k, _, _} -> k == key end)
              new_version = state.write_version + 1
              {:noreply, %{state | pending: new_pending, write_version: new_version}}

            {:error, reason} ->
              Logger.error(
                "Shard #{state.index}: tombstone write failed for tx_pending_delete: #{inspect(reason)}"
              )

              {:noreply, state}
          end
        end
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
            GenServer.reply(from, nil)
            {:noreply, %{state | pending_reads: rest_pending}}

          {{from, _key, :meta, _exp, _fid, _off, _vsize} = pending_entry, rest_pending} ->
            emit_pending_read_error(state, pending_entry, :timeout)
            GenServer.reply(from, nil)
            {:noreply, %{state | pending_reads: rest_pending}}

          {{from, _key}, rest_pending} ->
            GenServer.reply(from, nil)
            {:noreply, %{state | pending_reads: rest_pending}}

          {{from, _key, :meta, _exp}, rest_pending} ->
            GenServer.reply(from, nil)
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

              {{from, _key}, rest_pending} ->
                # Legacy in-memory pending entry without disk location. Reply but
                # do not warm, because the current ETS location may be newer than
                # the value returned by the async read.
                GenServer.reply(from, value)
                {:noreply, %{state | pending_reads: rest_pending}}

              {{from, _key, :meta, exp}, rest_pending} ->
                # Legacy in-memory pending entry without disk location. Reply but
                # skip warming for the same stale-completion reason as simple GET.
                GenServer.reply(from, if(value != nil, do: {value, exp}, else: nil))
                {:noreply, %{state | pending_reads: rest_pending}}

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
              GenServer.reply(from, nil)
              {:noreply, %{state | pending_reads: rest_pending}}

            {{from, _key, :meta, _exp, _fid, _off, _vsize} = pending_entry, rest_pending} ->
              emit_pending_read_error(state, pending_entry, reason)
              GenServer.reply(from, nil)
              {:noreply, %{state | pending_reads: rest_pending}}

            {{from, _key}, rest_pending} ->
              GenServer.reply(from, nil)
              {:noreply, %{state | pending_reads: rest_pending}}

            {{from, _key, :meta, _exp}, rest_pending} ->
              GenServer.reply(from, nil)
              {:noreply, %{state | pending_reads: rest_pending}}

            {nil, _} ->
              {:noreply, state}
          end
        end
      end

      # Legacy v1 3-tuple format (no correlation ID) — keep for backward compat
      # during rolling upgrades. Once all async NIFs use correlation IDs, remove.
      def handle_info({:tokio_complete, :ok, _value}, state) do
        {:noreply, state}
      end

      def handle_info({:tokio_complete, :error, _reason}, state) do
        {:noreply, state}
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

          other ->
            other
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
            {:reply, from, nil}

          {:cold, new_fid, new_off, new_vsize, new_exp} ->
            {:resubmit, {from, key, new_exp, new_fid, new_off, new_vsize}}

          :expired ->
            {:reply, from, nil}

          :miss ->
            {:reply, from, nil}

          _ ->
            {:reply, from, nil}
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
            {:reply, from, nil}

          {:cold, new_fid, new_off, new_vsize, new_exp} ->
            {:resubmit, {from, key, :meta, new_exp, new_fid, new_off, new_vsize}}

          :expired ->
            {:reply, from, nil}

          :miss ->
            {:reply, from, nil}

          _ ->
            {:reply, from, nil}
        end
      end

      defp resolve_nil_cold_read(_state, {from, _key}, _attempts_left), do: {:reply, from, nil}

      defp resolve_nil_cold_read(_state, {from, _key, :meta, _exp}, _attempts_left),
        do: {:reply, from, nil}

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
            GenServer.reply(from, nil)
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
            GenServer.reply(from, nil)
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
        |> flush_standalone_batch()
        |> await_standalone_flush()
        |> then(&ShardLifecycle.do_terminate(reason, &1))
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
