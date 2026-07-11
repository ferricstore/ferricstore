defmodule Ferricstore.Store.Shard.Routing do
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
            file_stats: Map.get(sm_state, :file_stats, state.file_stats)
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
          applied_count: 0,
          release_cursor_interval:
            Application.get_env(:ferricstore, :release_cursor_interval, 200_000),
          cross_shard_locks: %{},
          cross_shard_intents: %{},
          instance_ctx: state.instance_ctx,
          instance_name: if(state.instance_ctx, do: state.instance_ctx.name, else: :default),
          compound_member_index_name: state.compound_member_index,
          zset_score_index_name: state.zset_score_index,
          zset_score_lookup_name: state.zset_score_lookup,
          flow_index_name: state.flow_index,
          flow_lookup_name: state.flow_lookup,
          flow_lmdb_path: Ferricstore.Flow.LMDB.path(state.shard_data_path),
          flow_async_history: flow_async_history_enabled?()
        }
      end

      defp flow_async_history_enabled? do
        case Application.get_env(:ferricstore, :flow_async_history, true) do
          value when value in [true, "1", "true"] -> true
          value when value in [false, "0", "false"] -> false
          _ -> true
        end
      end

      defp enqueue_standalone_commit(state, from, command) do
        entry = {from, command, standalone_command_keys(command)}

        if state.standalone_write_barrier or
             standalone_entry_conflicts?(entry, state.standalone_inflight_keys) do
          %{state | standalone_waiting: state.standalone_waiting ++ [entry]}
        else
          enqueue_ready_standalone_commit(state, entry)
        end
      end

      defp enqueue_ready_standalone_commit(state, entry) do
        timer =
          if state.standalone_batch_timer == nil and state.standalone_flush_ref == nil do
            Process.send_after(self(), :standalone_commit_flush, standalone_commit_delay_ms())
          else
            state.standalone_batch_timer
          end

        %{
          state
          | standalone_batch: [entry | state.standalone_batch],
            standalone_batch_timer: timer
        }
      end

      defp maybe_flush_full_standalone_batch(state) do
        if length(state.standalone_batch) >= standalone_commit_max_ops() do
          flush_standalone_batch(state)
        else
          state
        end
      end

      defp standalone_commit_delay_ms do
        :ferricstore
        |> Application.get_env(:standalone_fsync_max_delay_ms, @flush_interval_ms)
        |> normalize_positive_integer(@flush_interval_ms)
      end

      defp standalone_commit_max_ops do
        :ferricstore
        |> Application.get_env(:standalone_fsync_max_ops, 1024)
        |> normalize_positive_integer(1024)
      end

      defp normalize_positive_integer(value, _default) when is_integer(value) and value > 0,
        do: value

      defp normalize_positive_integer(_value, default), do: default

      defp flush_standalone_batch(%{standalone_batch: []} = state) do
        %{state | standalone_batch_timer: nil}
      end

      defp flush_standalone_batch(%{standalone_flush_ref: ref} = state) when ref != nil do
        %{state | standalone_batch_timer: nil}
      end

      defp flush_standalone_batch(state) do
        if timer = state.standalone_batch_timer do
          Process.cancel_timer(timer)
        end

        entries = Enum.reverse(state.standalone_batch)
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
          | standalone_batch: [],
            standalone_batch_timer: nil,
            standalone_flush_ref: ref,
            standalone_flush_entries: entries,
            standalone_inflight_keys: standalone_entries_keys(entries)
        }
      end

      defp run_standalone_batch(entries, sm_state) do
        commands = Enum.map(entries, fn {_from, command, _keys} -> command end)

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
              {:cont, {:ok, new_state, [result | results]}}

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
              standalone_inflight_keys: MapSet.new()
          }

        case result do
          {:ok, new_sm_state, results} ->
            state = apply_direct_sm_state(state, new_sm_state)
            reply_standalone_results(entries, results)

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
                standalone_batch: [],
                standalone_batch_timer: nil,
                standalone_waiting: []
            }
        end
      end

      defp handle_standalone_flush_result(_ref, _result, state), do: state

      defp flush_ready_standalone_batch(%{standalone_batch: []} = state), do: state
      defp flush_ready_standalone_batch(state), do: flush_standalone_batch(state)

      defp clear_standalone_write_barrier(state), do: %{state | standalone_write_barrier: false}

      defp reject_queued_standalone_commits(state, reason) do
        if timer = state.standalone_batch_timer do
          Process.cancel_timer(timer)
        end

        reply_standalone_error(state.standalone_batch, reason)
        reply_standalone_error(state.standalone_waiting, reason)

        %{state | standalone_batch: [], standalone_batch_timer: nil, standalone_waiting: []}
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

      defp drain_standalone_waiting(%{standalone_write_barrier: true} = state), do: state

      defp drain_standalone_waiting(state) do
        {ready, waiting} =
          Enum.split_with(state.standalone_waiting, fn entry ->
            not standalone_entry_conflicts?(entry, state.standalone_inflight_keys)
          end)

        %{
          state
          | standalone_batch: Enum.reverse(ready) ++ state.standalone_batch,
            standalone_waiting: waiting
        }
      end

      defp standalone_entry_conflicts?({_from, _command, keys}, inflight_keys) do
        (standalone_global_keys?(keys) and MapSet.size(inflight_keys) > 0) or
          standalone_global_keys?(inflight_keys) or
          not MapSet.disjoint?(keys, inflight_keys)
      end

      defp standalone_global_keys?(keys), do: MapSet.member?(keys, @standalone_global_key)

      defp standalone_entries_keys(entries) do
        Enum.reduce(entries, MapSet.new(), fn {_from, _command, keys}, acc ->
          MapSet.union(acc, keys)
        end)
      end

      defp standalone_command_keys({:batch, commands}) when is_list(commands) do
        commands
        |> Enum.reduce(MapSet.new(), fn command, acc ->
          MapSet.union(acc, standalone_command_keys(command))
        end)
        |> standalone_nonempty_keys()
      end

      defp standalone_command_keys({:async, _origin, command}) when is_tuple(command) do
        standalone_command_keys(command)
      end

      defp standalone_command_keys({command, %{hlc_ts: _remote_ts}}) when is_tuple(command) do
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

      defp standalone_command_keys({:server_command, _command}) do
        standalone_global_keys()
      end

      defp standalone_command_keys({:list_op_lmove, source, destination, _from_dir, _to_dir}) do
        MapSet.new([standalone_lock_key(source), standalone_lock_key(destination)])
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

      defp standalone_command_keys({:lock_keys, keys, _owner_ref, _expire_at_ms})
           when is_list(keys) do
        standalone_lock_keys(keys)
      end

      defp standalone_command_keys({:unlock_keys, keys, _owner_ref}) when is_list(keys) do
        standalone_lock_keys(keys)
      end

      defp standalone_command_keys({:locked_put, key, _value, _expire_at_ms, _owner_ref}) do
        MapSet.new([standalone_lock_key(key)])
      end

      defp standalone_command_keys({:locked_delete, key, _owner_ref}) do
        MapSet.new([standalone_lock_key(key)])
      end

      defp standalone_command_keys({:locked_delete_prefix, prefix, _owner_ref}) do
        MapSet.new([standalone_lock_key(prefix)])
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

      defp reply_standalone_results(entries, results) when length(entries) == length(results) do
        Enum.zip(entries, results)
        |> Enum.each(fn {{from, _command, _keys}, result} -> GenServer.reply(from, result) end)
      end

      defp reply_standalone_results([{from, {:batch, _commands}, _keys}], results) do
        GenServer.reply(from, {:ok, results})
      end

      defp reply_standalone_results(entries, [result]) do
        Enum.each(entries, fn {from, _command, _keys} -> GenServer.reply(from, result) end)
      end

      defp reply_standalone_results(entries, results) do
        reply_standalone_error(entries, {:standalone_result_mismatch, length(results)})
      end

      defp reply_standalone_error(entries, reason) do
        Enum.each(entries, fn {from, _command, _keys} ->
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

        standalone_entry_conflicts?({nil, left, left_keys}, right_keys)
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
        case File.stat(path) do
          {:ok, %{size: size}} -> size
          {:error, _reason} -> 0
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

      defp handle_forwarded_quorum(
             {:ratelimit_add, key, window_ms, max, count, _now_ms},
             _from,
             state
           ) do
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
