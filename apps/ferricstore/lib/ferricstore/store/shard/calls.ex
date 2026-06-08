defmodule Ferricstore.Store.Shard.Calls do
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
      @impl true
      def handle_continue({:flush_interval, ms}, state) do
        # Store flush interval in process dictionary so handle_info can reschedule.
        Process.put(:flush_interval_ms, ms)
        {:noreply, state}
      end

      # -------------------------------------------------------------------
      # handle_call — reads
      # -------------------------------------------------------------------
      @impl true
      def handle_call({:get, key}, from, state), do: ShardReads.handle_get(key, from, state)

      def handle_call({:get_file_ref, key}, _from, state),
        do: ShardReads.handle_get_file_ref(key, state)

      def handle_call({:get_meta, key}, from, state),
        do: ShardReads.handle_get_meta(key, from, state)

      # Compound key scan: returns all live entries matching a prefix.
      # Used by HSCAN, SSCAN, ZSCAN via the compound_scan store callback.
      # Uses :ets.select match spec instead of :ets.foldl full-table scan.
      def handle_call({:scan_prefix, prefix}, _from, state) do
        state =
          if ShardETS.prefix_has_pending_cold?(state.keydir, prefix) do
            ShardFlush.flush_pending_for_read(state)
          else
            state
          end

        results = prefix_scan_entries(state, prefix, state.shard_data_path)
        {:reply, Enum.sort_by(results, fn {field, _} -> field end), state}
      end

      # Count entries matching a compound key prefix.
      # Uses :ets.select match spec instead of :ets.foldl full-table scan.
      def handle_call({:count_prefix, prefix}, _from, state) do
        {:reply, prefix_count_entries(state, prefix), state}
      end

      def handle_call({:exists, key}, _from, state), do: ShardReads.handle_exists(key, state)
      def handle_call(:keys, _from, state), do: ShardReads.handle_keys(state)
      # Returns the active file info (file_id + path). Used by callers that
      # need to reach the shard's current Bitcask file without copying the
      # entire Shard state via :sys.get_state.
      def handle_call(:get_active_file, _from, state) do
        {:reply, {state.active_file_id, state.active_file_path}, state}
      end

      # Returns the current write_version for WATCH support.
      # Combines the Shard's internal counter (incremented for async/non-raft writes)
      # with the shared atomic counter (incremented by Router for quorum bypass writes).
      # This ensures WATCH detects mutations regardless of which write path was used.
      def handle_call({:get_version, _key}, _from, state) do
        {:reply, max(state.write_version, shared_write_version(state)), state}
      end

      # -------------------------------------------------------------------
      # handle_call — writes
      # -------------------------------------------------------------------
      # Delete all entries matching a compound key prefix.
      # Uses :ets.select match spec instead of :ets.foldl full-table scan.
      def handle_call({:delete_prefix, prefix}, _from, state) do
        maybe_route_default_waraft_write({:delete_prefix, prefix}, state, fn ->
          prefix
          |> ShardWrites.handle_delete_prefix(state)
          |> track_write_version_result(state)
        end)
      end

      def handle_call({:put, _key, _value, _expire_at_ms}, _from, %{writes_paused: true} = state) do
        {:reply, {:error, "ERR shard writes paused for sync"}, state}
      end

      def handle_call(command, _from, %{writes_paused: true} = state)
          when is_tuple(command) and command != {:pause_writes} and command != {:resume_writes} do
        {:reply, {:error, "ERR shard writes paused for sync"}, state}
      end

      def handle_call({:standalone_commit, command}, from, state) do
        if default_waraft_write_state?(state) do
          reply_default_waraft_write(command, state)
        else
          state =
            state
            |> enqueue_standalone_commit(from, command)
            |> maybe_flush_full_standalone_batch()

          {:noreply, state}
        end
      end

      def handle_call(:standalone_cross_shard_barrier_acquire, _from, state) do
        state = drain_standalone_commits_for_sync(state)

        cond do
          state.standalone_write_barrier ->
            {:reply, {:error, :standalone_cross_shard_barrier_busy}, state}

          state.writes_paused ->
            {:reply, {:error, :prior_standalone_write_failed}, state}

          state.last_flush_error != nil ->
            {:reply, {:error, state.last_flush_error}, %{state | writes_paused: true}}

          true ->
            {:reply, :ok, %{state | standalone_write_barrier: true}}
        end
      end

      def handle_call(:standalone_cross_shard_barrier_release, _from, state) do
        state =
          cond do
            state.writes_paused ->
              state
              |> clear_standalone_write_barrier()
              |> reject_queued_standalone_commits(
                {:standalone_durability_failed, :prior_standalone_write_failed}
              )

            state.last_flush_error != nil ->
              state
              |> clear_standalone_write_barrier()
              |> reject_queued_standalone_commits(
                {:standalone_durability_failed, state.last_flush_error}
              )
              |> Map.put(:writes_paused, true)

            true ->
              state
              |> clear_standalone_write_barrier()
              |> drain_standalone_waiting()
              |> flush_ready_standalone_batch()
          end

        {:reply, :ok, state}
      end

      def handle_call(
            {:standalone_commit_sync, {:cross_shard_tx, _shard_batches} = command},
            _from,
            state
          ) do
        if default_waraft_write_state?(state) do
          reply_default_waraft_write(command, state)
        else
          state = drain_standalone_commits_for_sync(state)

          cond do
            state.writes_paused ->
              {:reply, {:error, {:standalone_durability_failed, :prior_standalone_write_failed}},
               state}

            state.last_flush_error != nil ->
              {:reply, {:error, {:standalone_durability_failed, state.last_flush_error}},
               %{state | writes_paused: true}}

            true ->
              sm_state = direct_sm_state(state)

              case run_standalone_command(command, sm_state) do
                {:ok, new_sm_state, result} ->
                  {:reply, result, apply_direct_sm_state(state, new_sm_state)}

                {:error, new_sm_state, reason} ->
                  state =
                    if new_sm_state do
                      apply_direct_sm_state(state, new_sm_state)
                    else
                      state
                    end

                  {:reply, {:error, {:standalone_durability_failed, reason}},
                   %{state | writes_paused: true}}
              end
          end
        end
      end

      def handle_call(:standalone_commit_debug, _from, state) do
        {:reply,
         %{
           batch_count: length(state.standalone_batch),
           waiting_count: length(state.standalone_waiting),
           inflight_count: length(state.standalone_flush_entries),
           inflight_keys: MapSet.to_list(state.standalone_inflight_keys)
         }, state}
      end

      def handle_call(:write_status, _from, state) do
        {:reply,
         %{
           writes_paused: state.writes_paused,
           last_flush_error: state.last_flush_error,
           pending_count: state.pending_count,
           standalone_batch_count: length(state.standalone_batch),
           standalone_waiting_count: length(state.standalone_waiting),
           standalone_flush_inflight: state.standalone_flush_ref != nil
         }, state}
      end

      def handle_call({:forwarded_quorum, origin_node, command}, from, state) do
        forwarded_from = Ferricstore.Raft.Batcher.remote_origin_from(origin_node, from)
        previous_origin = Process.get(:ferricstore_forward_origin)
        Process.put(:ferricstore_forward_origin, origin_node)

        try do
          if default_waraft_write_state?(state) do
            reply_default_waraft_write(command, state)
          else
            handle_forwarded_quorum(command, forwarded_from, state)
          end
        after
          if previous_origin == nil do
            Process.delete(:ferricstore_forward_origin)
          else
            Process.put(:ferricstore_forward_origin, previous_origin)
          end
        end
      end

      def handle_call({:put, key, value, expire_at_ms}, from, state) do
        maybe_route_default_waraft_write({:put, key, value, expire_at_ms}, state, fn ->
          key
          |> ShardWrites.handle_put(value, expire_at_ms, from, state)
          |> track_write_version_result(state)
        end)
      end

      # Atomic increment: reads current value, parses as integer, adds delta, writes back.
      # Returns {:ok, new_integer} or {:error, reason}.
      def handle_call({:incr, key, delta}, from, state) do
        maybe_route_default_waraft_write({:incr, key, delta}, state, fn ->
          key
          |> ShardWrites.handle_incr(delta, from, state)
          |> track_write_version_result(state)
        end)
      end

      # Atomic float increment: reads current value, parses as float, adds delta, writes back.
      # Returns {:ok, new_float_string} or {:error, reason}.
      def handle_call({:incr_float, key, delta}, from, state) do
        maybe_route_default_waraft_write({:incr_float, key, delta}, state, fn ->
          key
          |> ShardWrites.handle_incr_float(delta, from, state)
          |> track_write_version_result(state)
        end)
      end

      # Atomic append: reads current value (or ""), appends suffix, writes back.
      # Returns {:ok, new_byte_length}.
      def handle_call({:append, key, suffix}, from, state) do
        maybe_route_default_waraft_write({:append, key, suffix}, state, fn ->
          key
          |> ShardWrites.handle_append(suffix, from, state)
          |> track_write_version_result(state)
        end)
      end

      # Atomic get-and-set: returns old value (or nil), sets new value.
      def handle_call({:getset, key, new_value}, from, state) do
        maybe_route_default_waraft_write({:getset, key, new_value}, state, fn ->
          key
          |> ShardWrites.handle_getset(new_value, from, state)
          |> track_write_version_result(state)
        end)
      end

      # Atomic get-and-delete: returns value (or nil), deletes key.
      def handle_call({:getdel, key}, from, state) do
        maybe_route_default_waraft_write({:getdel, key}, state, fn ->
          key
          |> ShardWrites.handle_getdel(from, state)
          |> track_write_version_result(state)
        end)
      end

      # Atomic get-and-update-expiry: returns value, updates TTL.
      # expire_at_ms = 0 means PERSIST (remove expiry).
      def handle_call({:getex, key, expire_at_ms}, from, state) do
        maybe_route_default_waraft_write({:getex, key, expire_at_ms}, state, fn ->
          key
          |> ShardWrites.handle_getex(expire_at_ms, from, state)
          |> track_write_version_result(state)
        end)
      end

      # Atomic set-range: overwrites portion of string at offset with value.
      # Zero-pads if key doesn't exist or string is shorter than offset.
      # Returns {:ok, new_byte_length}.
      def handle_call({:setrange, key, offset, value}, from, state) do
        maybe_route_default_waraft_write({:setrange, key, offset, value}, state, fn ->
          key
          |> ShardWrites.handle_setrange(offset, value, from, state)
          |> track_write_version_result(state)
        end)
      end

      def handle_call({:delete, key}, from, state) do
        maybe_route_default_waraft_write({:delete, key}, state, fn ->
          key
          |> ShardWrites.handle_delete(from, state)
          |> track_write_version_result(state)
        end)
      end

      # -------------------------------------------------------------------
      # handle_call — compound operations (promotion-aware)
      # -------------------------------------------------------------------
      def handle_call({:compound_get, redis_key, compound_key}, _from, state) do
        ShardCompound.handle_compound_get(redis_key, compound_key, state)
      end

      def handle_call({:compound_batch_get, redis_key, compound_keys}, _from, state) do
        ShardCompound.handle_compound_batch_get(redis_key, compound_keys, state)
      end

      def handle_call({:compound_get_meta, redis_key, compound_key}, _from, state) do
        ShardCompound.handle_compound_get_meta(redis_key, compound_key, state)
      end

      def handle_call({:compound_batch_get_meta, redis_key, compound_keys}, _from, state) do
        ShardCompound.handle_compound_batch_get_meta(redis_key, compound_keys, state)
      end

      def handle_call({:compound_put, redis_key, compound_key, value, expire_at_ms}, _from, state) do
        maybe_route_default_waraft_write(
          {:compound_put, redis_key, compound_key, value, expire_at_ms},
          state,
          fn ->
            redis_key
            |> ShardCompound.handle_compound_put(compound_key, value, expire_at_ms, state)
            |> track_write_version_result(state)
          end
        )
      end

      def handle_call({:compound_batch_put, redis_key, entries}, _from, state) do
        maybe_route_default_waraft_write({:compound_batch_put, redis_key, entries}, state, fn ->
          redis_key
          |> ShardCompound.handle_compound_batch_put(entries, state)
          |> track_write_version_result(state)
        end)
      end

      def handle_call({:compound_delete, redis_key, compound_key}, _from, state) do
        maybe_route_default_waraft_write({:compound_delete, redis_key, compound_key}, state, fn ->
          redis_key
          |> ShardCompound.handle_compound_delete(compound_key, state)
          |> track_write_version_result(state)
        end)
      end

      def handle_call({:compound_batch_delete, redis_key, compound_keys}, _from, state) do
        maybe_route_default_waraft_write(
          {:compound_batch_delete, redis_key, compound_keys},
          state,
          fn ->
            redis_key
            |> ShardCompound.handle_compound_batch_delete(compound_keys, state)
            |> track_write_version_result(state)
          end
        )
      end

      def handle_call({:compound_delete_prefix, redis_key, prefix}, _from, state) do
        maybe_route_default_waraft_write(
          {:compound_delete_prefix, redis_key, prefix},
          state,
          fn ->
            redis_key
            |> ShardCompound.handle_compound_delete_prefix(prefix, state)
            |> track_write_version_result(state)
          end
        )
      end

      def handle_call({:compound_scan, redis_key, prefix}, _from, state) do
        ShardCompound.handle_compound_scan(redis_key, prefix, state)
      end

      def handle_call({:compound_fields, redis_key, prefix}, _from, state) do
        ShardCompound.handle_compound_fields(redis_key, prefix, state)
      end

      def handle_call({:compound_count, redis_key, prefix}, _from, state) do
        ShardCompound.handle_compound_count(redis_key, prefix, state)
      end

      def handle_call(
            {:zset_score_range, redis_key, min_bound, max_bound, reverse?},
            _from,
            state
          ) do
        ShardCompound.handle_zset_score_range(redis_key, min_bound, max_bound, reverse?, state)
      end

      def handle_call(
            {:zset_score_range_slice, redis_key, min_bound, max_bound, reverse?, offset, count},
            _from,
            state
          ) do
        ShardCompound.handle_zset_score_range_slice(
          redis_key,
          min_bound,
          max_bound,
          reverse?,
          offset,
          count,
          state
        )
      end

      def handle_call({:zset_score_count, redis_key, min_bound, max_bound}, _from, state) do
        ShardCompound.handle_zset_score_count(redis_key, min_bound, max_bound, state)
      end

      def handle_call({:zset_score_count_many, queries}, _from, state) do
        ShardCompound.handle_zset_score_count_many(queries, state)
      end

      def handle_call({:zset_score_count_all_many_no_build, keys}, _from, state) do
        ShardCompound.handle_zset_score_count_all_many_no_build(keys, state)
      end

      def handle_call({:zset_rank_range, redis_key, start_idx, stop_idx, reverse?}, _from, state) do
        ShardCompound.handle_zset_rank_range(redis_key, start_idx, stop_idx, reverse?, state)
      end

      def handle_call({:zset_member_rank, redis_key, member, reverse?}, _from, state) do
        ShardCompound.handle_zset_member_rank(redis_key, member, reverse?, state)
      end

      def handle_call(
            {:flow_index_score_range_slice, key, min_bound, max_bound, reverse?, offset, count},
            _from,
            state
          ) do
        reply =
          case native_flow_index_for_read(state) do
            {:ok, native} ->
              {:ok,
               NativeFlowIndex.range_slice(
                 native,
                 key,
                 min_bound,
                 max_bound,
                 reverse?,
                 offset,
                 count
               )}

            :unavailable ->
              :unavailable
          end

        {:reply, reply, state}
      end

      def handle_call({:flow_index_rank_range, key, start_idx, stop_idx, reverse?}, _from, state) do
        reply =
          case native_flow_index_for_read(state) do
            {:ok, native} ->
              {:ok, NativeFlowIndex.rank_range(native, key, start_idx, stop_idx, reverse?)}

            :unavailable ->
              :unavailable
          end

        {:reply, reply, state}
      end

      def handle_call({:flow_index_rank_range_many, requests}, _from, state) do
        reply =
          Enum.reduce_while(requests, {:ok, []}, fn {key, start_idx, stop_idx, reverse?},
                                                    {:ok, acc} ->
            case native_flow_index_for_read(state) do
              {:ok, native} ->
                result = NativeFlowIndex.rank_range(native, key, start_idx, stop_idx, reverse?)
                {:cont, {:ok, [result | acc]}}

              :unavailable ->
                {:halt, :unavailable}
            end
          end)
          |> case do
            {:ok, results} -> {:ok, Enum.reverse(results)}
            :unavailable -> :unavailable
          end

        {:reply, reply, state}
      end

      def handle_call({:flow_index_count_all, key}, _from, state) do
        reply =
          case native_flow_index_for_read(state) do
            {:ok, native} -> {:ok, NativeFlowIndex.count_all(native, key)}
            :unavailable -> :unavailable
          end

        {:reply, reply, state}
      end

      def handle_call({:flow_index_count_all_many, keys}, _from, state) do
        reply =
          case keys do
            [] ->
              {:ok, []}

            _ ->
              case native_flow_index_for_count_many(state, keys) do
                {:ok, native} ->
                  {:ok, NativeFlowIndex.count_many(native, keys)}

                :unavailable ->
                  :unavailable
              end
          end

        {:reply, reply, state}
      end

      def handle_call(:flow_due_count_keys, _from, state) do
        reply =
          case native_flow_index_for_read(state) do
            {:ok, native} -> {:ok, NativeFlowIndex.due_count_keys(native)}
            :unavailable -> :unavailable
          end

        {:reply, reply, state}
      end

      # -------------------------------------------------------------------
      # handle_call — native commands: CAS, LOCK, UNLOCK, EXTEND, RATELIMIT.ADD
      # -------------------------------------------------------------------
      def handle_call({:cas, key, expected, new_value, ttl_ms}, _from, state) do
        maybe_route_default_waraft_write({:cas, key, expected, new_value, ttl_ms}, state, fn ->
          key
          |> ShardNativeOps.handle_cas(expected, new_value, ttl_ms, state)
          |> track_write_version_result(state)
        end)
      end

      def handle_call({:lock, key, owner, ttl_ms}, _from, state) do
        maybe_route_default_waraft_write({:lock, key, owner, ttl_ms}, state, fn ->
          key
          |> ShardNativeOps.handle_lock(owner, ttl_ms, state)
          |> track_write_version_result(state)
        end)
      end

      def handle_call({:unlock, key, owner}, _from, state) do
        maybe_route_default_waraft_write({:unlock, key, owner}, state, fn ->
          key
          |> ShardNativeOps.handle_unlock(owner, state)
          |> track_write_version_result(state)
        end)
      end

      def handle_call({:extend, key, owner, ttl_ms}, _from, state) do
        maybe_route_default_waraft_write({:extend, key, owner, ttl_ms}, state, fn ->
          key
          |> ShardNativeOps.handle_extend(owner, ttl_ms, state)
          |> track_write_version_result(state)
        end)
      end

      def handle_call({:ratelimit_add, key, window_ms, max, count}, _from, state) do
        maybe_route_default_waraft_write(
          {:ratelimit_add, key, window_ms, max, count},
          state,
          fn ->
            key
            |> ShardNativeOps.handle_ratelimit_add(window_ms, max, count, state)
            |> track_write_version_result(state)
          end
        )
      end

      # 6-tuple variant: includes pre-computed now_ms from Router.raft_write.
      # In cluster mode this MUST go through Raft (replicated) just like the
      # 5-tuple variant — otherwise a follower's ratelimit-add lands locally
      # only and other nodes never see the increment. Falls back to direct in
      # non-Raft mode.
      def handle_call({:ratelimit_add, key, window_ms, max, count, _now_ms}, _from, state) do
        maybe_route_default_waraft_write(
          {:ratelimit_add, key, window_ms, max, count},
          state,
          fn ->
            key
            |> ShardNativeOps.handle_ratelimit_add(window_ms, max, count, state)
            |> track_write_version_result(state)
          end
        )
      end

      # -------------------------------------------------------------------
      # handle_call — list operations
      # -------------------------------------------------------------------
      def handle_call({:list_op, key, operation}, _from, state) do
        maybe_route_default_waraft_write({:list_op, key, operation}, state, fn ->
          key
          |> ShardNativeOps.handle_list_op(operation, state)
          |> track_write_version_result(state)
        end)
      end

      def handle_call({:list_op_lmove, src_key, dst_key, from_dir, to_dir}, _from, state) do
        maybe_route_default_waraft_write(
          {:list_op_lmove, src_key, dst_key, from_dir, to_dir},
          state,
          fn ->
            src_key
            |> ShardNativeOps.handle_list_op_lmove(dst_key, from_dir, to_dir, state)
            |> track_write_version_result(state)
          end
        )
      end

      # -------------------------------------------------------------------
      # handle_call — transaction execution (single-shard atomic batch)
      # -------------------------------------------------------------------
      def handle_call({:tx_execute, queue, sandbox_namespace}, _from, state) do
        maybe_route_default_waraft_write({:tx_execute, queue, sandbox_namespace}, state, fn ->
          queue
          |> ShardTransaction.handle_tx_execute(sandbox_namespace, state)
          |> track_write_version_result(state)
        end)
      end

      # Check if a redis_key has been promoted to dedicated storage.
      def handle_call({:promoted?, redis_key}, _from, state) do
        {:reply, ShardCompound.promoted_store(state, redis_key) != nil, state}
      end

      # -------------------------------------------------------------------
      # handle_call — pause/resume writes (cluster data sync)
      # -------------------------------------------------------------------
      def handle_call({:pause_writes}, _from, state) do
        state = drain_standalone_commits_for_sync(state)
        state = await_in_flight(state)
        state = flush_pending_sync(state)
        {:reply, :ok, %{state | writes_paused: true}}
      end

      def handle_call({:resume_writes}, _from, state) do
        {:reply, :ok, %{state | writes_paused: false}}
      end

      def handle_call(:enable_raft, _from, state) do
        {:reply, :ok, %{state | raft?: true}}
      end

      def handle_call(:start_raft, _from, %{raft?: true} = state) do
        {:reply, :ok, state}
      end

      def handle_call(:start_raft, _from, state) do
        result =
          ShardLifecycle.start_raft_if_available(
            state.index,
            state.shard_data_path,
            state.active_file_id,
            state.active_file_path,
            state.ets,
            if(state.instance_ctx, do: state.instance_ctx.name, else: :default),
            wait_for_leader: false,
            blob_side_channel_threshold_bytes:
              if(state.instance_ctx,
                do: state.instance_ctx.blob_side_channel_threshold_bytes,
                else: 0
              ),
            active_file_preallocated_to: state.active_file_preallocated_to,
            zset_score_index_name: state.zset_score_index,
            zset_score_lookup_name: state.zset_score_lookup,
            flow_index_name: state.flow_index,
            flow_lookup_name: state.flow_lookup
          )

        if result do
          {:reply, :ok, %{state | raft?: true}}
        else
          {:reply, {:error, :batcher_not_started}, state}
        end
      catch
        kind, reason ->
          {:reply, {:error, {kind, reason}}, state}
      end

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
