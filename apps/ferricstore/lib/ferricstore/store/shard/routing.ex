defmodule Ferricstore.Store.Shard.Routing do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Flow.DueCatalog
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
      defp native_flow_index_for_count_many(state, _keys), do: native_flow_index_for_read(state)

      defp native_flow_index_for_read(state) do
        case NativeFlowIndex.get(state.flow_index, state.flow_lookup) do
          nil -> :unavailable
          native -> {:ok, native}
        end
      end

      defp maybe_emit_compaction_crc_mismatch(state, fid, source, dest, reason) do
        if compaction_crc_mismatch?(reason) do
          :telemetry.execute(
            [:ferricstore, :bitcask, :compaction_crc_mismatch],
            %{count: 1},
            %{
              shard_index: state.index,
              file_id: fid,
              path: source,
              dest: dest,
              reason: inspect(reason)
            }
          )
        end
      end

      defp compaction_crc_mismatch?(reason) do
        reason
        |> inspect()
        |> String.downcase()
        |> String.contains?("crc mismatch")
      end

      defp apply_direct_sm_state(state, sm_state) do
        %{
          state
          | active_file_id: Map.get(sm_state, :active_file_id, state.active_file_id),
            active_file_path: Map.get(sm_state, :active_file_path, state.active_file_path),
            active_file_size: Map.get(sm_state, :active_file_size, state.active_file_size),
            file_stats: Map.get(sm_state, :file_stats, state.file_stats),
            apply_context: Map.get(sm_state, :apply_context, state.apply_context),
            apply_context_encoded:
              Map.get(sm_state, :apply_context_encoded, state.apply_context_encoded),
            flow_due_catalog:
              direct_flow_due_catalog(
                Map.get(sm_state, :flow_due_catalog),
                state.flow_due_catalog
              ),
            promoted_instances: Map.get(sm_state, :promoted_instances, state.promoted_instances),
            fetch_or_compute_locks:
              Map.get(sm_state, :fetch_or_compute_locks, state.fetch_or_compute_locks),
            fetch_or_compute_lock_expiries:
              Map.get(
                sm_state,
                :fetch_or_compute_lock_expiries,
                state.fetch_or_compute_lock_expiries
              )
        }
      end

      defp direct_sm_state(state) do
        %{
          shard_index: state.index,
          data_dir: state.data_dir,
          data_dir_expanded: Path.expand(state.data_dir),
          shard_data_path: state.shard_data_path,
          shard_data_path_expanded: Path.expand(state.shard_data_path),
          active_file_id: state.active_file_id,
          active_file_path: state.active_file_path,
          active_file_size: state.active_file_size,
          file_stats: state.file_stats,
          merge_config: state.merge_config,
          max_active_file_size: state.max_active_file_size,
          ets: state.ets,
          apply_context: state.apply_context,
          apply_context_encoded: state.apply_context_encoded,
          applied_count: 0,
          release_cursor_interval: state.release_cursor_interval,
          fetch_or_compute_locks: state.fetch_or_compute_locks,
          fetch_or_compute_lock_expiries: state.fetch_or_compute_lock_expiries,
          instance_ctx: state.instance_ctx,
          instance_name: if(state.instance_ctx, do: state.instance_ctx.name, else: :default),
          compound_member_index_name: state.compound_member_index,
          zset_score_index_name: state.zset_score_index,
          zset_score_lookup_name: state.zset_score_lookup,
          logical_key_index_name: state.logical_key_index,
          logical_key_slots_name: state.logical_key_slots,
          flow_index_name: state.flow_index,
          flow_lookup_name: state.flow_lookup,
          flow_due_catalog: direct_flow_due_catalog(state.flow_due_catalog, DueCatalog.new()),
          flow_lmdb_path: Ferricstore.Flow.LMDB.path(state.shard_data_path),
          flow_async_history: state.flow_async_history,
          promoted_instances: state.promoted_instances
        }
      end

      defp direct_flow_due_catalog(catalog, fallback) do
        if DueCatalog.valid?(catalog), do: catalog, else: fallback
      end

      defp flow_async_history_enabled? do
        case Application.get_env(:ferricstore, :flow_async_history, true) do
          value when value in [true, "1", "true"] -> true
          value when value in [false, "0", "false"] -> false
          _ -> true
        end
      end

      defp enqueue_standalone_commit(state, from, command) do
        if standalone_queued_count(state) >= state.standalone_commit_max_queued_ops do
          GenServer.reply(from, {:error, "BUSY standalone commit queue is full"})
          state
        else
          command_bytes = :erlang.external_size(command)

          if standalone_retained_bytes(state) + command_bytes >
               state.standalone_commit_max_queued_bytes do
            GenServer.reply(from, {:error, "BUSY standalone commit queue is full"})
            state
          else
            entry = {from, command, standalone_command_keys(command), command_bytes}

            if standalone_write_barrier_active?(state) or
                 standalone_entry_conflicts?(entry, state.standalone_inflight_keys) or
                 standalone_entry_conflicts?(entry, state.standalone_batch_keys) or
                 standalone_entry_conflicts?(entry, state.standalone_waiting_keys) do
              {_from, _command, keys, _command_bytes} = entry

              %{
                state
                | standalone_waiting: :queue.in(entry, state.standalone_waiting),
                  standalone_waiting_count: state.standalone_waiting_count + 1,
                  standalone_waiting_bytes: state.standalone_waiting_bytes + command_bytes,
                  standalone_waiting_keys: MapSet.union(state.standalone_waiting_keys, keys)
              }
            else
              enqueue_ready_standalone_commit(state, entry)
            end
          end
        end
      end

      defp enqueue_standalone_barrier_write(state, from, request) do
        request_bytes = :erlang.external_size(request)

        cond do
          standalone_queued_count(state) >= state.standalone_commit_max_queued_ops ->
            GenServer.reply(from, {:error, "BUSY standalone commit queue is full"})
            state

          standalone_retained_bytes(state) + request_bytes >
              state.standalone_commit_max_queued_bytes ->
            GenServer.reply(from, {:error, "BUSY standalone commit queue is full"})
            state

          true ->
            %{
              state
              | standalone_barrier_waiting:
                  :queue.in(
                    {from, request, request_bytes},
                    state.standalone_barrier_waiting
                  ),
                standalone_barrier_waiting_count: state.standalone_barrier_waiting_count + 1,
                standalone_barrier_waiting_bytes:
                  state.standalone_barrier_waiting_bytes + request_bytes
            }
        end
      end

      defp standalone_queued_count(state) do
        state.standalone_batch_count + state.standalone_waiting_count +
          state.standalone_barrier_waiting_count
      end

      defp standalone_retained_bytes(state) do
        state.standalone_batch_bytes + state.standalone_waiting_bytes +
          state.standalone_flush_bytes + state.standalone_barrier_waiting_bytes
      end

      defp enqueue_ready_standalone_commit(state, entry) do
        {_from, _command, keys, command_bytes} = entry

        timer =
          if state.standalone_batch_timer == nil and state.standalone_flush_ref == nil do
            Process.send_after(self(), :standalone_commit_flush, state.standalone_commit_delay_ms)
          else
            state.standalone_batch_timer
          end

        %{
          state
          | standalone_batch: :queue.in(entry, state.standalone_batch),
            standalone_batch_count: state.standalone_batch_count + 1,
            standalone_batch_bytes: state.standalone_batch_bytes + command_bytes,
            standalone_batch_keys: MapSet.union(state.standalone_batch_keys, keys),
            standalone_batch_timer: timer
        }
      end

      defp maybe_flush_full_standalone_batch(state) do
        if state.standalone_batch_count >= state.standalone_commit_max_ops do
          flush_standalone_batch(state)
        else
          state
        end
      end

      defp flush_standalone_batch(%{standalone_batch_count: 0} = state) do
        %{state | standalone_batch_timer: nil}
      end

      defp flush_standalone_batch(%{standalone_flush_ref: ref} = state) when ref != nil do
        %{state | standalone_batch_timer: nil}
      end

      defp flush_standalone_batch(state) do
        if timer = state.standalone_batch_timer do
          Process.cancel_timer(timer)
        end

        flush_count = min(state.standalone_batch_count, state.standalone_commit_max_ops)
        {flush_queue, remaining_batch} = :queue.split(flush_count, state.standalone_batch)
        entries = :queue.to_list(flush_queue)
        flush_bytes = standalone_entries_bytes(entries)
        flush_keys = standalone_entries_keys(entries)
        sm_state = direct_sm_state(state)
        ref = make_ref()
        parent = self()

        _pid =
          spawn_link(fn ->
            send(
              parent,
              {:standalone_commit_flushed, ref, run_standalone_batch(entries, sm_state)}
            )
          end)

        %{
          state
          | standalone_batch: remaining_batch,
            standalone_batch_count: state.standalone_batch_count - flush_count,
            standalone_batch_bytes: state.standalone_batch_bytes - flush_bytes,
            standalone_batch_keys:
              Enum.reduce(flush_keys, state.standalone_batch_keys, &MapSet.delete(&2, &1)),
            standalone_batch_timer: nil,
            standalone_flush_ref: ref,
            standalone_flush_entries: entries,
            standalone_flush_bytes: flush_bytes,
            standalone_inflight_keys: flush_keys
        }
      end

      defp run_standalone_batch(entries, sm_state) do
        commands = Enum.map(entries, fn {_from, command, _keys, _bytes} -> command end)

        try do
          if Enum.any?(commands, &match?({:cross_shard_tx, _}, &1)) do
            run_standalone_commands_sequential(commands, sm_state)
          else
            case Ferricstore.Raft.StateMachine.apply_standalone_batch(commands, sm_state) do
              {new_sm_state, {:ok, results}} when is_list(results) ->
                {:ok, new_sm_state, results}

              {new_sm_state, {:error, reason}} ->
                {:error, new_sm_state, reason}

              {new_sm_state, result} ->
                {:ok, new_sm_state, List.wrap(result)}
            end
          end
        rescue
          error ->
            {:error, nil, {:exception, error}}
        catch
          kind, reason ->
            {:error, nil, {kind, reason}}
        end
      end

      defp run_standalone_commands_sequential(commands, sm_state) do
        Enum.reduce_while(commands, {:ok, sm_state, []}, fn command, {:ok, acc_state, results} ->
          case run_standalone_command(command, acc_state) do
            {:ok, new_state, result} ->
              case flatten_standalone_command_result(command, result) do
                {:ok, command_results} ->
                  {:cont, {:ok, new_state, Enum.reverse(command_results, results)}}

                {:error, reason} ->
                  {:halt, {:error, new_state, reason}}
              end

            {:error, new_state, reason} ->
              {:halt, {:error, new_state, reason}}
          end
        end)
        |> case do
          {:ok, new_state, results} -> {:ok, new_state, Enum.reverse(results)}
          {:error, new_state, reason} -> {:error, new_state, reason}
        end
      end

      defp run_standalone_command({:cross_shard_tx, _shard_batches} = command, sm_state) do
        case Ferricstore.Raft.StateMachine.apply_standalone_command(command, sm_state) do
          {new_sm_state, {:error, reason}} -> {:error, new_sm_state, reason}
          {new_sm_state, result} -> {:ok, new_sm_state, result}
          {new_sm_state, {:error, reason}, _effects} -> {:error, new_sm_state, reason}
          {new_sm_state, result, _effects} -> {:ok, new_sm_state, result}
        end
      end

      defp run_standalone_command(command, sm_state) do
        case Ferricstore.Raft.StateMachine.apply_standalone_batch([command], sm_state) do
          {new_sm_state, {:ok, [result]}} -> {:ok, new_sm_state, result}
          {new_sm_state, {:ok, results}} when is_list(results) -> {:ok, new_sm_state, results}
          {new_sm_state, {:error, reason}} -> {:error, new_sm_state, reason}
          {new_sm_state, result} -> {:ok, new_sm_state, result}
        end
      end

      defp handle_standalone_flush_result(ref, result, %{standalone_flush_ref: ref} = state) do
        entries = state.standalone_flush_entries

        state =
          %{
            state
            | standalone_flush_ref: nil,
              standalone_flush_entries: [],
              standalone_flush_bytes: 0,
              standalone_inflight_keys: MapSet.new()
          }

        case result do
          {:ok, new_sm_state, results} ->
            reply_result = reply_standalone_results(entries, results)

            mutation_count =
              case reply_result do
                {:ok, replies} -> standalone_mutation_count(entries, replies)
                :mismatch -> length(entries)
              end

            state =
              state
              |> apply_direct_sm_state(new_sm_state)
              |> bump_standalone_write_version(mutation_count)

            state
            |> drain_standalone_waiting()
            |> flush_ready_standalone_batch()

          {:error, new_sm_state, reason} ->
            state =
              if new_sm_state do
                apply_direct_sm_state(state, new_sm_state)
              else
                state
              end

            reply_standalone_error(entries, {:standalone_durability_failed, reason})

            reply_standalone_error(
              state.standalone_batch,
              {:standalone_durability_failed, reason}
            )

            reply_standalone_error(
              state.standalone_waiting,
              {:standalone_durability_failed, reason}
            )

            %{
              state
              | writes_paused: true,
                standalone_batch: :queue.new(),
                standalone_batch_count: 0,
                standalone_batch_bytes: 0,
                standalone_batch_keys: MapSet.new(),
                standalone_batch_timer: nil,
                standalone_waiting: :queue.new(),
                standalone_waiting_count: 0,
                standalone_waiting_bytes: 0,
                standalone_waiting_keys: MapSet.new()
            }
        end
      end

      defp handle_standalone_flush_result(_ref, _result, state), do: state

      defp flush_ready_standalone_batch(%{standalone_batch_count: 0} = state), do: state
      defp flush_ready_standalone_batch(state), do: flush_standalone_batch(state)

      defp coordinate_standalone_cross_shard(participant_indices, execute_fn, state) do
        state = drain_standalone_commits_for_sync(state)

        cond do
          standalone_write_barrier_active?(state) ->
            {{:error, {:standalone_cross_shard_busy, :coordinator_barrier_busy}}, state}

          state.writes_paused ->
            {{:error,
              {:standalone_cross_shard_busy,
               {:standalone_durability_failed, :prior_standalone_write_failed}}}, state}

          state.last_flush_error != nil ->
            {{:error,
              {:standalone_cross_shard_busy,
               {:standalone_durability_failed, state.last_flush_error}}},
             %{state | writes_paused: true}}

          not valid_standalone_participants?(participant_indices, state) ->
            {{:error, {:standalone_cross_shard_busy, :invalid_participant_order}}, state}

          true ->
            owner_token = make_ref()

            state =
              install_standalone_write_barrier(state, owner_token, self(), false)

            case acquire_standalone_participant_barriers(
                   state.instance_ctx,
                   participant_indices,
                   owner_token,
                   []
                 ) do
              {:ok, acquired} ->
                {reply, state} = apply_standalone_cross_shard(execute_fn, state)
                log_standalone_barrier_release_errors(state, acquired, owner_token)
                state = maybe_bump_cross_shard_write_versions(state, participant_indices, reply)
                {reply, release_standalone_write_barrier(state)}

              {:error, reason, acquired} ->
                log_standalone_barrier_release_errors(state, acquired, owner_token)

                {{:error, {:standalone_cross_shard_busy, reason}},
                 release_standalone_write_barrier(state)}
            end
        end
      end

      defp valid_standalone_participants?(participant_indices, state) do
        participant_indices == Enum.sort(Enum.uniq(participant_indices)) and
          Enum.all?(participant_indices, fn
            index when is_integer(index) -> index > state.index
            _invalid -> false
          end) and not is_nil(state.instance_ctx)
      end

      defp acquire_standalone_participant_barriers(
             _ctx,
             [],
             _owner_token,
             acquired
           ),
           do: {:ok, acquired}

      defp acquire_standalone_participant_barriers(
             ctx,
             [shard_index | rest],
             owner_token,
             acquired
           ) do
        result =
          try do
            ctx
            |> Router.shard_name(shard_index)
            |> GenServer.call(
              {:standalone_cross_shard_barrier_acquire, owner_token},
              :infinity
            )
          catch
            :exit, reason -> {:error, reason}
          end

        case result do
          :ok ->
            acquire_standalone_participant_barriers(
              ctx,
              rest,
              owner_token,
              [shard_index | acquired]
            )

          {:error, reason} ->
            {:error, reason, acquired}

          other ->
            {:error, {:unexpected_barrier_reply, other}, acquired}
        end
      end

      defp log_standalone_barrier_release_errors(state, acquired, owner_token) do
        errors =
          Enum.reduce(acquired, [], fn shard_index, errors ->
            result =
              try do
                state.instance_ctx
                |> Router.shard_name(shard_index)
                |> GenServer.call(
                  {:standalone_cross_shard_barrier_release, owner_token},
                  :infinity
                )
              catch
                :exit, reason -> {:error, reason}
              end

            case result do
              :ok -> errors
              {:error, reason} -> [{shard_index, reason} | errors]
              other -> [{shard_index, {:unexpected_barrier_reply, other}} | errors]
            end
          end)

        if errors != [] do
          Logger.error(
            "Shard #{state.index}: standalone cross-shard barrier release failed: " <>
              inspect(Enum.reverse(errors))
          )
        end

        :ok
      end

      defp apply_standalone_cross_shard(execute_fn, state) do
        sm_state = direct_sm_state(state)

        case Ferricstore.Raft.StateMachine.apply_standalone_cross_shard(execute_fn, sm_state) do
          {result, %{} = new_sm_state} ->
            {result, apply_direct_sm_state(state, new_sm_state)}

          {:error, reason, new_sm_state} ->
            state =
              state
              |> apply_direct_sm_state(new_sm_state)
              |> Map.put(:writes_paused, true)
              |> Map.put(:last_flush_error, reason)

            {{:error, {:standalone_durability_failed, reason}}, state}

          {:error, reason} ->
            state =
              state
              |> Map.put(:writes_paused, true)
              |> Map.put(:last_flush_error, reason)

            {{:error, {:standalone_durability_failed, reason}}, state}
        end
      end

      defp standalone_write_barrier_active?(state),
        do: state.standalone_write_barrier != false

      defp standalone_write_barrier_owner?(
             %{
               standalone_write_barrier: %{
                 token: owner_token,
                 owner_pid: owner_pid
               }
             },
             owner_token,
             owner_pid
           ),
           do: true

      defp standalone_write_barrier_owner?(_state, _owner_token, _owner_pid), do: false

      defp install_standalone_write_barrier(
             state,
             owner_token,
             owner_pid,
             monitor_owner?
           ) do
        monitor_ref = if monitor_owner?, do: Process.monitor(owner_pid), else: nil

        %{
          state
          | standalone_write_barrier: %{
              token: owner_token,
              owner_pid: owner_pid,
              monitor_ref: monitor_ref
            }
        }
      end

      defp release_standalone_write_barrier(state) do
        state = clear_standalone_write_barrier(state)

        cond do
          state.writes_paused ->
            state
            |> reject_barrier_waiting_writes(
              {:standalone_durability_failed, :prior_standalone_write_failed}
            )
            |> reject_queued_standalone_commits(
              {:standalone_durability_failed, :prior_standalone_write_failed}
            )

          state.last_flush_error != nil ->
            state
            |> reject_barrier_waiting_writes(
              {:standalone_durability_failed, state.last_flush_error}
            )
            |> reject_queued_standalone_commits(
              {:standalone_durability_failed, state.last_flush_error}
            )
            |> Map.put(:writes_paused, true)

          true ->
            state
            |> drain_standalone_barrier_waiting()
            |> drain_standalone_waiting()
            |> flush_ready_standalone_batch()
        end
      end

      defp drain_standalone_barrier_waiting(state) do
        case :queue.out(state.standalone_barrier_waiting) do
          {{:value, {from, request, request_bytes}}, remaining} ->
            state = %{
              state
              | standalone_barrier_waiting: remaining,
                standalone_barrier_waiting_count: state.standalone_barrier_waiting_count - 1,
                standalone_barrier_waiting_bytes:
                  state.standalone_barrier_waiting_bytes - request_bytes
            }

            state =
              case handle_call(request, from, state) do
                {:reply, reply, new_state} ->
                  GenServer.reply(from, reply)
                  new_state

                {:noreply, new_state} ->
                  new_state

                {:stop, reason, reply, new_state} ->
                  GenServer.reply(from, {:error, {:standalone_write_failed, reason, reply}})
                  new_state

                {:stop, reason, new_state} ->
                  GenServer.reply(from, {:error, {:standalone_write_failed, reason}})
                  new_state
              end

            drain_standalone_barrier_waiting(state)

          {:empty, _queue} ->
            %{
              state
              | standalone_barrier_waiting_count: 0,
                standalone_barrier_waiting_bytes: 0
            }
        end
      end

      defp reject_barrier_waiting_writes(state, reason) do
        state.standalone_barrier_waiting
        |> :queue.to_list()
        |> Enum.each(fn {from, _request, _request_bytes} ->
          GenServer.reply(from, {:error, reason})
        end)

        %{
          state
          | standalone_barrier_waiting: :queue.new(),
            standalone_barrier_waiting_count: 0,
            standalone_barrier_waiting_bytes: 0
        }
      end

      defp clear_standalone_write_barrier(
             %{standalone_write_barrier: %{monitor_ref: monitor_ref}} = state
           ) do
        if is_reference(monitor_ref) do
          Process.demonitor(monitor_ref, [:flush])
        end

        %{state | standalone_write_barrier: false}
      end

      defp clear_standalone_write_barrier(state),
        do: %{state | standalone_write_barrier: false}

      defp bump_standalone_write_version(state, delta) when is_integer(delta) and delta > 0 do
        state = %{state | write_version: state.write_version + delta}
        bump_shared_write_version(state, delta)
        state
      end

      defp bump_standalone_write_version(state, _delta), do: state

      defp maybe_bump_cross_shard_write_versions(state, _participant_indices, {:error, _reason}),
        do: state

      defp maybe_bump_cross_shard_write_versions(state, participant_indices, _result) do
        state = bump_standalone_write_version(state, 1)

        Enum.each(participant_indices, fn shard_index ->
          bump_instance_write_version(state.instance_ctx, shard_index, 1)
        end)

        state
      end

      defp bump_instance_write_version(
             %{write_version: write_version},
             shard_index,
             delta
           )
           when is_integer(shard_index) and shard_index >= 0 and is_integer(delta) and delta > 0 do
        size = :counters.info(write_version).size
        if shard_index < size, do: :counters.add(write_version, shard_index + 1, delta)
        :ok
      rescue
        _ -> :ok
      end

      defp bump_instance_write_version(_ctx, _shard_index, _delta), do: :ok

      defp reject_queued_standalone_commits(state, reason) do
        if timer = state.standalone_batch_timer do
          Process.cancel_timer(timer)
        end

        reply_standalone_error(state.standalone_batch, reason)
        reply_standalone_error(state.standalone_waiting, reason)

        %{
          state
          | standalone_batch: :queue.new(),
            standalone_batch_count: 0,
            standalone_batch_bytes: 0,
            standalone_batch_keys: MapSet.new(),
            standalone_batch_timer: nil,
            standalone_waiting: :queue.new(),
            standalone_waiting_count: 0,
            standalone_waiting_bytes: 0,
            standalone_waiting_keys: MapSet.new()
        }
      end

      defp await_standalone_flush(%{standalone_flush_ref: nil} = state), do: state

      defp await_standalone_flush(%{standalone_flush_ref: ref} = state) do
        receive do
          {:standalone_commit_flushed, ^ref, result} ->
            ref
            |> handle_standalone_flush_result(result, state)
            |> await_standalone_flush()
        after
          30_000 ->
            %{state | last_flush_error: {:standalone_commit_flush_timeout, ref}}
        end
      end

      defp drain_standalone_commits_for_sync(state) do
        state
        |> drain_standalone_waiting()
        |> flush_ready_standalone_batch()
        |> await_standalone_flush()
      end

      defp drain_standalone_waiting(%{standalone_write_barrier: barrier} = state)
           when barrier != false,
           do: state

      defp drain_standalone_waiting(%{standalone_waiting_count: 0} = state) do
        %{state | standalone_waiting_bytes: 0, standalone_waiting_keys: MapSet.new()}
      end

      defp drain_standalone_waiting(state) do
        {ready, waiting, waiting_keys, ready_keys, ready_count, ready_bytes} =
          state.standalone_waiting
          |> :queue.to_list()
          |> Enum.reduce(
            {[], [], MapSet.new(), state.standalone_batch_keys, 0, 0},
            fn entry, {ready, waiting, blocked_keys, ready_keys, ready_count, ready_bytes} ->
              {_from, _command, keys, command_bytes} = entry

              if standalone_entry_conflicts?(entry, state.standalone_inflight_keys) or
                   standalone_entry_conflicts?(entry, blocked_keys) or
                   standalone_entry_conflicts?(entry, ready_keys) do
                {ready, [entry | waiting], MapSet.union(blocked_keys, keys), ready_keys,
                 ready_count, ready_bytes}
              else
                {[entry | ready], waiting, blocked_keys, MapSet.union(ready_keys, keys),
                 ready_count + 1, ready_bytes + command_bytes}
              end
            end
          )

        ready = Enum.reverse(ready)
        waiting = Enum.reverse(waiting)

        %{
          state
          | standalone_batch: :queue.join(state.standalone_batch, :queue.from_list(ready)),
            standalone_batch_count: state.standalone_batch_count + ready_count,
            standalone_batch_bytes: state.standalone_batch_bytes + ready_bytes,
            standalone_batch_keys: ready_keys,
            standalone_waiting: :queue.from_list(waiting),
            standalone_waiting_count: state.standalone_waiting_count - ready_count,
            standalone_waiting_bytes: state.standalone_waiting_bytes - ready_bytes,
            standalone_waiting_keys: waiting_keys
        }
      end

      defp standalone_entry_conflicts?({_from, _command, keys, _bytes}, inflight_keys) do
        (standalone_global_keys?(keys) and MapSet.size(inflight_keys) > 0) or
          standalone_global_keys?(inflight_keys) or
          not MapSet.disjoint?(keys, inflight_keys)
      end

      defp standalone_global_keys?(keys), do: MapSet.member?(keys, @standalone_global_key)

      defp standalone_entries_keys(entries) do
        Enum.reduce(entries, MapSet.new(), fn {_from, _command, keys, _bytes}, acc ->
          MapSet.union(acc, keys)
        end)
      end

      defp standalone_entries_bytes(entries) do
        Enum.reduce(entries, 0, fn {_from, _command, _keys, bytes}, acc -> acc + bytes end)
      end

      defp standalone_command_keys({:batch, commands}) when is_list(commands) do
        commands
        |> Enum.reduce(MapSet.new(), fn command, acc ->
          MapSet.union(acc, standalone_command_keys(command))
        end)
        |> standalone_nonempty_keys()
      end

      defp standalone_command_keys({operation, entries})
           when operation in [:mset, :msetnx, :mset_blob_batch, :msetnx_blob_batch] and
                  is_list(entries) do
        entries
        |> Enum.reduce(MapSet.new(), fn
          entry, acc when is_tuple(entry) and tuple_size(entry) >= 3 ->
            case elem(entry, 0) do
              key when is_binary(key) -> MapSet.put(acc, standalone_lock_key(key))
              _invalid -> MapSet.put(acc, @standalone_global_key)
            end

          _invalid, acc ->
            MapSet.put(acc, @standalone_global_key)
        end)
        |> standalone_nonempty_keys()
      end

      defp standalone_command_keys({:async, _origin, command}) when is_tuple(command) do
        standalone_command_keys(command)
      end

      defp standalone_command_keys({command, %{hlc_ts: _remote_ts, wall_time_ms: wall_time_ms}})
           when is_tuple(command) and is_integer(wall_time_ms) do
        standalone_command_keys(command)
      end

      defp standalone_command_keys({:cross_shard_tx, shard_batches})
           when is_list(shard_batches) do
        shard_batches
        |> Enum.reduce(MapSet.new(), fn
          {_shard_idx, entries, _namespace}, acc when is_list(entries) ->
            Enum.reduce(entries, acc, fn entry, acc ->
              MapSet.union(acc, standalone_cross_shard_entry_keys(entry))
            end)

          _other, acc ->
            MapSet.put(acc, @standalone_global_key)
        end)
        |> standalone_nonempty_keys()
      end

      defp standalone_command_keys({:cross_shard_tx, _shard_batches}) do
        standalone_global_keys()
      end

      defp standalone_command_keys(
             {:server_catalog_mutate, namespace, subject, _expected_encoded, _expected_revision,
              _value, _max_live_entries}
           )
           when is_binary(namespace) and is_binary(subject) do
        MapSet.new([
          standalone_lock_key(Ferricstore.ServerCatalog.entry_key(namespace, subject)),
          standalone_lock_key(Ferricstore.ServerCatalog.revision_key(namespace)),
          standalone_lock_key(Ferricstore.ServerCatalog.live_count_key(namespace))
        ])
      rescue
        ArgumentError -> standalone_global_keys()
      end

      defp standalone_command_keys(
             {:server_catalog_replace, namespace, _expected_revision, mutations,
              _expected_live_count, _max_live_entries}
           )
           when is_binary(namespace) and is_list(mutations) do
        mutations
        |> Enum.reduce(
          MapSet.new([
            standalone_lock_key(Ferricstore.ServerCatalog.revision_key(namespace)),
            standalone_lock_key(Ferricstore.ServerCatalog.live_count_key(namespace))
          ]),
          fn
            {subject, _value}, acc when is_binary(subject) ->
              MapSet.put(
                acc,
                standalone_lock_key(Ferricstore.ServerCatalog.entry_key(namespace, subject))
              )

            _invalid, _acc ->
              throw(:invalid_server_catalog_replacement)
          end
        )
      rescue
        ArgumentError -> standalone_global_keys()
      catch
        :invalid_server_catalog_replacement -> standalone_global_keys()
      end

      defp standalone_command_keys({:list_op_lmove, source, destination, _from_dir, _to_dir}) do
        MapSet.new([standalone_lock_key(source), standalone_lock_key(destination)])
      end

      defp standalone_command_keys({:watch_tokens, keys}) when is_list(keys) do
        standalone_lock_keys(keys)
      end

      defp standalone_command_keys({:pfmerge, destination, sources}) when is_list(sources) do
        standalone_lock_keys([destination | sources])
      end

      defp standalone_command_keys({:pfmerge, destination, source_keys, _source_sketches})
           when is_list(source_keys) do
        standalone_lock_keys([destination | source_keys])
      end

      defp standalone_command_keys({:cms_merge, destination, sources, _weights, _create_params})
           when is_list(sources) do
        standalone_lock_keys([destination | sources])
      end

      defp standalone_command_keys(
             {:fetch_or_compute_lock, key, outcome_key, _owner_ref, _expire_at_ms}
           ) do
        standalone_lock_keys([key, outcome_key])
      end

      defp standalone_command_keys(
             {:fetch_or_compute_fail, key, outcome_key, _encoded_error, _expire_at_ms, _owner_ref}
           ) do
        standalone_lock_keys([key, outcome_key])
      end

      defp standalone_command_keys(
             {:fetch_or_compute_publish, key, _value, _expire_at_ms, _owner_ref}
           ) do
        MapSet.new([standalone_lock_key(key)])
      end

      defp standalone_command_keys(
             {:fetch_or_compute_publish_blob_ref, key, _encoded_ref, _expire_at_ms, _owner_ref}
           ) do
        MapSet.new([standalone_lock_key(key)])
      end

      defp standalone_command_keys({:fetch_or_compute_release, key, _owner_ref}) do
        MapSet.new([standalone_lock_key(key)])
      end

      defp standalone_command_keys(
             {:compound_put, redis_key, _compound_key, _value, _expire_at_ms}
           ) do
        MapSet.new([standalone_lock_key(redis_key)])
      end

      defp standalone_command_keys({:compound_delete, redis_key, _compound_key}) do
        MapSet.new([standalone_lock_key(redis_key)])
      end

      defp standalone_command_keys({:compound_delete_prefix, redis_key, _prefix}) do
        MapSet.new([standalone_lock_key(redis_key)])
      end

      defp standalone_command_keys({:compound_put, compound_key, _value, _expire_at_ms}) do
        MapSet.new([standalone_lock_key(compound_key)])
      end

      defp standalone_command_keys({:compound_delete, compound_key}) do
        MapSet.new([standalone_lock_key(compound_key)])
      end

      defp standalone_command_keys({:compound_delete_prefix, prefix}) do
        MapSet.new([standalone_lock_key(prefix)])
      end

      defp standalone_command_keys({command, route_key, %{records: records}})
           when command in @standalone_flow_many_commands and is_list(records) do
        standalone_flow_record_keys(route_key, records)
      end

      defp standalone_command_keys({:flow_spawn_children, route_key, %{children: children}})
           when is_list(children) do
        standalone_flow_record_keys(route_key, children)
      end

      defp standalone_command_keys({:flow_claim_due, _route_key, _attrs}) do
        # Claims discover and mutate record keys by scanning due indexes, so the
        # exact write set is not knowable before the state machine runs.
        standalone_global_keys()
      end

      defp standalone_command_keys(command) when is_tuple(command) and tuple_size(command) > 1 do
        case elem(command, 1) do
          key when is_binary(key) -> MapSet.new([standalone_lock_key(key)])
          _other -> standalone_global_keys()
        end
      end

      defp standalone_command_keys(_command), do: standalone_global_keys()

      defp standalone_lock_keys(keys) do
        keys
        |> Enum.reduce(MapSet.new(), fn key, acc ->
          if is_binary(key) do
            MapSet.put(acc, standalone_lock_key(key))
          else
            MapSet.put(acc, @standalone_global_key)
          end
        end)
        |> standalone_nonempty_keys()
      end

      defp standalone_lock_key(key) when is_binary(key) do
        Ferricstore.Store.CompoundKey.extract_redis_key(key)
      end

      defp standalone_nonempty_keys(keys) do
        if MapSet.size(keys) == 0 do
          standalone_global_keys()
        else
          keys
        end
      end

      defp standalone_global_keys, do: MapSet.new([@standalone_global_key])

      defp standalone_cross_shard_entry_keys({_pos, tx_entry}),
        do: standalone_tx_entry_keys(tx_entry)

      defp standalone_cross_shard_entry_keys(_entry), do: standalone_global_keys()

      defp standalone_tx_entry_keys({_name, args, command}) do
        command
        |> standalone_tx_command_keys(args)
        |> standalone_lock_keys()
      end

      defp standalone_tx_entry_keys(_entry), do: standalone_global_keys()

      defp standalone_tx_command_keys({:msetnx, args}, _args), do: every_other_arg(args)

      defp standalone_tx_command_keys({:copy, source, destination, _replace?}, _args),
        do: [source, destination]

      defp standalone_tx_command_keys({:rename, source, destination}, _args),
        do: [source, destination]

      defp standalone_tx_command_keys({:renamenx, source, destination}, _args),
        do: [source, destination]

      defp standalone_tx_command_keys({:lmove, source, destination, _from_dir, _to_dir}, _args),
        do: [source, destination]

      defp standalone_tx_command_keys({:smove, source, destination, _member}, _args),
        do: [source, destination]

      defp standalone_tx_command_keys({command, [destination | sources]}, _args)
           when command in [:sdiffstore, :sinterstore, :sunionstore] and is_list(sources),
           do: [destination | sources]

      defp standalone_tx_command_keys(_command, args) when is_list(args),
        do: standalone_tx_args_keys(args)

      defp standalone_tx_command_keys(_command, _args), do: [@standalone_global_key]

      defp standalone_tx_args_keys(["MSETNX" | args]), do: every_other_arg(args)
      defp standalone_tx_args_keys([source, destination | _rest]), do: [source, destination]
      defp standalone_tx_args_keys(_args), do: [@standalone_global_key]

      defp every_other_arg(args) when is_list(args) do
        args
        |> Enum.with_index()
        |> Enum.filter(fn {_arg, index} -> rem(index, 2) == 0 end)
        |> Enum.map(fn {arg, _index} -> arg end)
      end

      defp every_other_arg(_args), do: [@standalone_global_key]

      defp standalone_flow_record_keys(route_key, records) do
        records
        |> Enum.reduce_while([route_key], fn
          %{id: id} = attrs, acc when is_binary(id) ->
            key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))
            {:cont, [key | acc]}

          _record, _acc ->
            {:halt, :global}
        end)
        |> case do
          :global -> standalone_global_keys()
          keys -> standalone_lock_keys(keys)
        end
      end

      defp reply_standalone_results(entries, results) do
        case partition_standalone_results(entries, results, []) do
          {:ok, replies} ->
            entries
            |> Enum.zip(replies)
            |> Enum.each(fn {{from, _command, _keys, _bytes}, reply} ->
              GenServer.reply(from, reply)
            end)

            {:ok, replies}

          :mismatch ->
            reply_standalone_error(entries, {:standalone_result_mismatch, length(results)})
            :mismatch
        end
      end

      defp standalone_mutation_count(entries, replies) do
        entries
        |> Enum.zip(replies)
        |> Enum.count(fn {{_from, command, _keys, _bytes}, reply} ->
          standalone_command_mutated?(command, reply)
        end)
      end

      defp standalone_command_mutated?(_command, {:error, _reason}), do: false
      defp standalone_command_mutated?({:msetnx, _args}, 0), do: false

      defp standalone_command_mutated?(
             {:set, _key, _value, _expire_at_ms, %{get: false, nx: nx, xx: xx}},
             nil
           )
           when nx or xx,
           do: false

      defp standalone_command_mutated?({:expire_if_batch, _entries}, results)
           when is_list(results),
           do: Enum.any?(results, &(&1 == true))

      defp standalone_command_mutated?({:batch, commands}, {:ok, replies})
           when is_list(commands) and is_list(replies) do
        commands
        |> Enum.zip(replies)
        |> Enum.any?(fn {command, reply} -> standalone_command_mutated?(command, reply) end)
      end

      defp standalone_command_mutated?(_command, _reply), do: true

      defp partition_standalone_results([], [], replies),
        do: {:ok, Enum.reverse(replies)}

      defp partition_standalone_results(
             [{_from, command, _keys, _bytes} | entries],
             results,
             replies
           ) do
        width = standalone_command_reply_width(command)

        case take_standalone_results(results, width, []) do
          {:ok, command_results, remaining_results} ->
            reply =
              if standalone_multi_reply_command?(command) do
                {:ok, command_results}
              else
                case command_results do
                  [result] -> result
                  _invalid -> :standalone_result_mismatch
                end
              end

            if reply == :standalone_result_mismatch do
              :mismatch
            else
              partition_standalone_results(entries, remaining_results, [reply | replies])
            end

          :mismatch ->
            :mismatch
        end
      end

      defp partition_standalone_results(_entries, _results, _replies), do: :mismatch

      defp take_standalone_results(results, 0, acc),
        do: {:ok, Enum.reverse(acc), results}

      defp take_standalone_results([result | results], remaining, acc) when remaining > 0 do
        take_standalone_results(results, remaining - 1, [result | acc])
      end

      defp take_standalone_results(_results, _remaining, _acc), do: :mismatch

      defp flatten_standalone_command_result(command, result) do
        if standalone_multi_reply_command?(command) do
          width = standalone_command_reply_width(command)

          cond do
            width == 0 and result == [] -> {:ok, []}
            width == 1 -> {:ok, [result]}
            is_list(result) and exact_standalone_result_count?(result, width) -> {:ok, result}
            true -> {:error, {:standalone_result_mismatch, result}}
          end
        else
          {:ok, [result]}
        end
      end

      defp exact_standalone_result_count?([], 0), do: true

      defp exact_standalone_result_count?([_result | results], remaining) when remaining > 0,
        do: exact_standalone_result_count?(results, remaining - 1)

      defp exact_standalone_result_count?(_results, _remaining), do: false

      defp standalone_command_reply_width({:batch, commands}) when is_list(commands) do
        Enum.reduce(commands, 0, fn command, count ->
          count + standalone_command_reply_width(command)
        end)
      end

      defp standalone_command_reply_width({command, entries})
           when command in [:put_batch, :delete_batch] and is_list(entries),
           do: length(entries)

      defp standalone_command_reply_width({:put_blob_batch, entries}) when is_list(entries),
        do: length(entries)

      defp standalone_command_reply_width(_command), do: 1

      defp standalone_multi_reply_command?({:batch, commands}) when is_list(commands), do: true

      defp standalone_multi_reply_command?({command, entries})
           when command in [:put_batch, :delete_batch] and is_list(entries),
           do: true

      defp standalone_multi_reply_command?({:put_blob_batch, entries}) when is_list(entries),
        do: true

      defp standalone_multi_reply_command?(_command), do: false

      defp reply_standalone_error({rear, front} = entries, reason)
           when is_list(rear) and is_list(front) do
        entries
        |> :queue.to_list()
        |> reply_standalone_error(reason)
      end

      defp reply_standalone_error(entries, reason) do
        Enum.each(entries, fn {from, _command, _keys, _bytes} ->
          GenServer.reply(from, {:error, reason})
        end)
      end

      @doc false
      def __standalone_command_keys_for_test__(command) do
        command
        |> standalone_command_keys()
        |> MapSet.to_list()
        |> Enum.sort()
      end

      @doc false
      def __standalone_command_keys_conflict_for_test__(left, right) do
        left_keys = standalone_command_keys(left)
        right_keys = standalone_command_keys(right)

        standalone_entry_conflicts?({nil, left, left_keys, 0}, right_keys)
      end

      defp maybe_route_default_waraft_write(
             _command,
             %{writes_paused: true} = state,
             _local_fun
           ) do
        {:reply, {:error, "ERR shard writes paused for sync"}, state}
      end

      defp maybe_route_default_waraft_write(command, state, local_fun) do
        if default_waraft_write_state?(state) do
          reply_default_waraft_write(command, state)
        else
          local_fun.()
        end
      end

      defp default_waraft_write_state?(%{instance_ctx: %{name: :default}}), do: true
      defp default_waraft_write_state?(_state), do: false

      defp reply_default_waraft_write(command, state) do
        result =
          case command do
            {:batch, commands} when is_list(commands) ->
              RaftBackend.write_batch(state.index, commands)

            _other ->
              RaftBackend.write(state.index, command)
          end

        case result do
          {:error, _reason} ->
            {:reply, result, state}

          _ok ->
            new_state = %{state | write_version: state.write_version + 1}
            bump_shared_write_version(new_state, 1)
            {:reply, result, new_state}
        end
      end

      defp shared_write_version(%{instance_ctx: %{write_version: write_version}, index: index}) do
        counter_value(write_version, index)
      end

      defp shared_write_version(%{instance_ctx: nil, index: index}) do
        Ferricstore.Store.WriteVersion.get(index)
      rescue
        _ -> 0
      end

      defp counter_value(write_version, index) do
        size = :counters.info(write_version).size
        if index < size, do: :counters.get(write_version, index + 1), else: 0
      rescue
        _ -> 0
      end

      defp track_write_version_result({:reply, reply, new_state}, old_state) do
        {:reply, reply, mirror_direct_write_version(new_state, old_state)}
      end

      defp track_write_version_result({:noreply, new_state}, old_state) do
        {:noreply, mirror_direct_write_version(new_state, old_state)}
      end

      defp track_write_version_result(other, _old_state), do: other

      # Custom/direct shards do not go through Router's quorum bump, so mirror
      # their local WATCH token into the instance counter that survives shard restart.
      defp mirror_direct_write_version(
             %{instance_ctx: %{name: :default}, raft?: false} = new_state,
             _old_state
           ) do
        new_state
      end

      defp mirror_direct_write_version(
             %{write_version: new_version} = new_state,
             %{raft?: false, write_version: old_version}
           )
           when new_version > old_version do
        bump_shared_write_version(new_state, new_version - old_version)
        new_state
      end

      defp mirror_direct_write_version(new_state, _old_state), do: new_state

      defp bump_shared_write_version(
             %{instance_ctx: %{write_version: write_version}, index: index},
             delta
           ) do
        size = :counters.info(write_version).size
        if index < size, do: :counters.add(write_version, index + 1, delta)
        :ok
      rescue
        _ -> :ok
      end

      defp bump_shared_write_version(%{instance_ctx: nil, index: index}, delta) do
        ref = :persistent_term.get(:ferricstore_write_versions)
        size = :counters.info(ref).size
        if index < size, do: :counters.add(ref, index + 1, delta)
        :ok
      rescue
        _ -> :ok
      end

      defp sync_active_file_from_registry(%{instance_ctx: nil} = state), do: state

      defp sync_active_file_from_registry(state) do
        case Ferricstore.Store.ActiveFile.get(state.instance_ctx, state.index) do
          {fid, path, shard_path}
          when fid != state.active_file_id or path != state.active_file_path ->
            active_size = active_file_size(path)

            %{
              state
              | active_file_id: fid,
                active_file_path: path,
                active_file_size: active_size,
                shard_data_path: shard_path,
                file_stats: Map.put(state.file_stats, fid, {active_size, 0})
            }

          _current ->
            state
        end
      rescue
        _ -> state
      end

      defp active_file_size(path) do
        case File.lstat(path) do
          {:ok, %File.Stat{type: :regular, size: size}} -> size
          _invalid_or_missing -> 0
        end
      end

      defp handle_forwarded_quorum(
             {:put, _key, _value, _expire_at_ms},
             _from,
             %{writes_paused: true} = state
           ) do
        {:reply, {:error, "ERR shard writes paused for sync"}, state}
      end

      defp handle_forwarded_quorum({:put, key, value, expire_at_ms}, from, state) do
        ShardWrites.handle_put(key, value, expire_at_ms, from, state)
      end

      defp handle_forwarded_quorum({:delete, key}, from, state) do
        ShardWrites.handle_delete(key, from, state)
      end

      defp handle_forwarded_quorum({:batch, commands}, _from, state) when is_list(commands) do
        {:reply, Ferricstore.Raft.Batcher.write(state.index, {:batch, commands}), state}
      end

      defp handle_forwarded_quorum({:incr, key, delta}, from, state) do
        ShardWrites.handle_incr(key, delta, from, state)
      end

      defp handle_forwarded_quorum({:incr_float, key, delta}, from, state) do
        ShardWrites.handle_incr_float(key, delta, from, state)
      end

      defp handle_forwarded_quorum({:append, key, suffix}, from, state) do
        ShardWrites.handle_append(key, suffix, from, state)
      end

      defp handle_forwarded_quorum({:getset, key, new_value}, from, state) do
        ShardWrites.handle_getset(key, new_value, from, state)
      end

      defp handle_forwarded_quorum({:getdel, key}, from, state) do
        ShardWrites.handle_getdel(key, from, state)
      end

      defp handle_forwarded_quorum({:getex, key, expire_at_ms}, from, state) do
        ShardWrites.handle_getex(key, expire_at_ms, from, state)
      end

      defp handle_forwarded_quorum({:setrange, key, offset, value}, from, state) do
        ShardWrites.handle_setrange(key, offset, value, from, state)
      end

      defp handle_forwarded_quorum({:compound_type_claim, redis_key, type}, _from, state) do
        ShardNativeOps.handle_type_claim(redis_key, type, state)
      end

      defp handle_forwarded_quorum(
             {:compound_put, redis_key, compound_key, value, expire_at_ms},
             _from,
             state
           ) do
        ShardCompound.handle_compound_put(redis_key, compound_key, value, expire_at_ms, state)
      end

      defp handle_forwarded_quorum({:compound_delete, redis_key, compound_key}, _from, state) do
        ShardCompound.handle_compound_delete(redis_key, compound_key, state)
      end

      defp handle_forwarded_quorum(
             {:compound_batch_delete, redis_key, compound_keys},
             _from,
             state
           ) do
        ShardCompound.handle_compound_batch_delete(redis_key, compound_keys, state)
      end

      defp handle_forwarded_quorum({:compound_delete_prefix, redis_key, prefix}, _from, state) do
        ShardCompound.handle_compound_delete_prefix(redis_key, prefix, state)
      end

      defp handle_forwarded_quorum({:cas, key, expected, new_value, ttl_ms}, _from, state) do
        ShardNativeOps.handle_cas(key, expected, new_value, ttl_ms, state)
      end

      defp handle_forwarded_quorum({:lock, key, owner, ttl_ms}, _from, state) do
        ShardNativeOps.handle_lock(key, owner, ttl_ms, state)
      end

      defp handle_forwarded_quorum({:unlock, key, owner}, _from, state) do
        ShardNativeOps.handle_unlock(key, owner, state)
      end

      defp handle_forwarded_quorum({:extend, key, owner, ttl_ms}, _from, state) do
        ShardNativeOps.handle_extend(key, owner, ttl_ms, state)
      end

      defp handle_forwarded_quorum({:ratelimit_add, key, window_ms, max, count}, _from, state) do
        ShardNativeOps.handle_ratelimit_add(key, window_ms, max, count, state)
      end

      defp handle_forwarded_quorum({:list_op, key, operation}, _from, state) do
        ShardNativeOps.handle_list_op(key, operation, state)
      end

      defp handle_forwarded_quorum(
             {:list_op_lmove, src_key, dst_key, from_dir, to_dir},
             _from,
             state
           ) do
        ShardNativeOps.handle_list_op_lmove(src_key, dst_key, from_dir, to_dir, state)
      end

      defp handle_forwarded_quorum(command, from, state) when is_tuple(command) do
        handle_call(command, from, state)
      end
    end
  end
end
