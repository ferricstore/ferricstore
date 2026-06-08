defmodule Ferricstore.Raft.WARaftStorage.Sections.Recovery do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.CommandTime
      alias Ferricstore.HLC
      alias Ferricstore.Flow.HistoryProjector
      alias Ferricstore.Flow.Keys, as: FlowKeys
      alias Ferricstore.Flow.LMDB, as: FlowLMDB
      alias Ferricstore.Raft.StateMachine
      alias Ferricstore.Raft.WARaftSegmentReader
      alias Ferricstore.Store.BlobRef
      alias Ferricstore.Store.BlobStore
      alias Ferricstore.Store.BlobValue
      alias Ferricstore.Store.ColdRead
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.Promotion
      alias Ferricstore.Store.Shard.ETS, as: ShardETS
      alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
      alias Ferricstore.Store.Shard.ZSetIndex

      defp segment_projection_registry_key(root_dir), do: root_dir |> Path.expand() |> to_string()

      defp segment_projection_registry_key(
             %{segment_projection_registry_key: key} = handle,
             _root_dir
           )
           when is_binary(key) do
        {key, handle}
      end

      defp segment_projection_registry_key(handle, root_dir) do
        key = segment_projection_registry_key(root_dir)
        {key, Map.put(handle, :segment_projection_registry_key, key)}
      end

      defp maybe_fsync_payload_before_metadata(%{bitcask_dirty?: true} = handle) do
        start = System.monotonic_time()
        result = fsync_payload_dirs(handle)
        duration = System.monotonic_time() - start

        emit_payload_fsync(handle, result, duration)
        result
      end

      defp maybe_fsync_payload_before_metadata(_handle), do: :ok

      defp emit_payload_fsync(handle, result, duration) do
        :telemetry.execute(
          [:ferricstore, :waraft, :storage, :payload_fsync],
          %{count: 1, duration: duration},
          %{
            shard_index: Map.get(handle, :shard_index),
            position: Map.get(handle, :position),
            result: payload_fsync_result(result),
            reason: payload_fsync_reason(result),
            root_dir: Map.get(handle, :root_dir)
          }
        )
      rescue
        _ -> :ok
      end

      defp payload_fsync_result(:ok), do: :ok
      defp payload_fsync_result({:error, _reason}), do: :error
      defp payload_fsync_result(_other), do: :unknown

      defp payload_fsync_reason(:ok), do: nil
      defp payload_fsync_reason({:error, reason}), do: reason
      defp payload_fsync_reason(other), do: other

      defp block_storage(handle, reason, attempted_position, operation) do
        emit_storage_blocked(handle, reason, attempted_position, operation)
        Map.put(handle, :blocked_error, reason)
      end

      defp emit_storage_blocked(handle, reason, attempted_position, operation) do
        :telemetry.execute(
          [:ferricstore, :waraft, :storage_blocked],
          %{count: 1},
          %{
            operation: operation,
            reason: reason,
            shard_index: Map.get(handle, :shard_index),
            attempted_position: attempted_position,
            durable_position: Map.get(handle, :position),
            root_dir: Map.get(handle, :root_dir)
          }
        )
      end

      defp send_storage_info(storage_name, message) do
        send(storage_name, message)
        :ok
      catch
        _, _ -> :ok
      end

      defp emit_apply_projection_cache_compaction(metadata, started_at, result) do
        duration_us =
          System.convert_time_unit(System.monotonic_time() - started_at, :native, :microsecond)

        :telemetry.execute(
          [:ferricstore, :waraft, :apply_projection_cache, :compact],
          %{
            count: Map.get(metadata, :count, 0),
            limit: Map.get(metadata, :limit, 0),
            spill_count: Map.get(metadata, :spill_count, 0),
            duration_us: duration_us
          },
          %{
            result: apply_projection_cache_compaction_result(result),
            reason: apply_projection_cache_compaction_reason(result),
            shard_index: Map.get(metadata, :shard_index),
            position: Map.get(metadata, :position),
            root_dir: Map.get(metadata, :root_dir)
          }
        )
      rescue
        _ -> :ok
      end

      defp apply_projection_cache_compaction_result(:ok), do: :ok
      defp apply_projection_cache_compaction_result({:ok, _removed}), do: :ok
      defp apply_projection_cache_compaction_result({:error, _reason}), do: :error
      defp apply_projection_cache_compaction_result(_other), do: :error

      defp apply_projection_cache_compaction_reason(:ok), do: nil
      defp apply_projection_cache_compaction_reason({:ok, _removed}), do: nil
      defp apply_projection_cache_compaction_reason({:error, reason}), do: reason
      defp apply_projection_cache_compaction_reason(other), do: other

      defp build_sm_state(ctx, shard_index) do
        data_dir = ctx.data_dir
        shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        Ferricstore.FS.mkdir_p!(shard_data_path)

        # WARaft storage may be opened by snapshot/bootstrap paths after the
        # original setup caller exits, so the named ETS owner must exist here too.
        Ferricstore.Store.ActiveFile.init(ctx.shard_count)

        {active_file_id, active_file_size} = ShardLifecycle.discover_active_file(shard_data_path)
        active_file_path = ShardETS.file_path(shard_data_path, active_file_id)

        unless Ferricstore.FS.exists?(active_file_path) do
          Ferricstore.FS.touch!(active_file_path)
        end

        keydir = elem(ctx.keydir_refs, shard_index)
        reset_keydir!(ctx, shard_index, keydir)
        ShardLifecycle.recover_keydir(shard_data_path, keydir, shard_index, ctx)

        instance_name = ctx.name
        {zset_score_index, zset_score_lookup} = ZSetIndex.table_names(instance_name, shard_index)
        ensure_ets_table!(zset_score_index, :ordered_set)
        ensure_ets_table!(zset_score_lookup, :set)

        {flow_index, flow_lookup} =
          Ferricstore.Flow.NativeOrderedIndex.table_names(instance_name, shard_index)

        ensure_native_flow_index!(flow_index, flow_lookup)

        Ferricstore.Flow.LMDBRebuilder.reconcile_startup_shard(
          shard_data_path,
          keydir,
          shard_index,
          ctx,
          zset_score_index,
          zset_score_lookup,
          flow_index,
          flow_lookup
        )

        Ferricstore.Store.ActiveFile.publish(
          ctx,
          shard_index,
          active_file_id,
          active_file_path,
          shard_data_path
        )

        StateMachine.init(%{
          shard_index: shard_index,
          data_dir: data_dir,
          shard_data_path: shard_data_path,
          active_file_id: active_file_id,
          active_file_path: active_file_path,
          active_file_size: active_file_size,
          ets: keydir,
          instance_ctx: ctx,
          instance_name: instance_name,
          zset_score_index_name: zset_score_index,
          zset_score_lookup_name: zset_score_lookup,
          flow_index_name: flow_index,
          flow_lookup_name: flow_lookup
        })
      end

      defp maybe_recover_segment_projected!(sm_state, root_dir, metadata) do
        metadata_position = Map.get(metadata, :position, @zero_pos)

        with {:ok, projected_sm_state, replay_after_index, base_position} <-
               recover_segment_projection_log(root_dir, sm_state, metadata_position),
             target_position = segment_recovery_target_position(root_dir, metadata, base_position),
             {:ok, recovered_sm_state, recovered_position, replay_dependencies} <-
               recover_segment_projected_keydir(
                 root_dir,
                 projected_sm_state,
                 target_position,
                 replay_after_index
               ) do
          {recovered_sm_state, recovered_position, replay_dependencies}
        else
          {:error, reason} ->
            raise "failed to recover WARaft segment-backed keydir: #{inspect(reason)}"
        end
      end

      defp rebuild_indexes_from_segment_keydir(
             %{ets: keydir, shard_data_path: shard_data_path} = sm_state,
             ctx,
             shard_index
           ) do
        Ferricstore.Flow.LMDBRebuilder.reconcile_startup_shard(
          shard_data_path,
          keydir,
          shard_index,
          ctx,
          sm_state.zset_score_index_name,
          sm_state.zset_score_lookup_name,
          sm_state.flow_index_name,
          sm_state.flow_lookup_name,
          force_full_reconcile?: true,
          reason: :segment_replay
        )

        sm_state
      end

      defp recover_segment_projection_log(root_dir, sm_state, metadata_position) do
        projection_root = segment_projection_root(root_dir)

        case read_segment_projection_log(projection_root) do
          {:ok, projection} ->
            with {:ok, entries} <- validate_segment_projection_entries(projection) do
              {:ok, apply_segment_projection_entries(sm_state, projection_root, entries),
               position_index(projection.position),
               max_raft_position(metadata_position, projection.position)}
            end

          {:error, :enoent} ->
            {:ok, sm_state, 0, metadata_position}

          {:error, reason} ->
            {:error, reason}
        end
      end

      defp segment_recovery_target_position(_root_dir, metadata, base_position) do
        cond do
          snapshot_boundary_position?(metadata, base_position) ->
            base_position

          single_member_config?(Map.get(metadata, :config)) ->
            {:latest, base_position}

          true ->
            base_position
        end
      end

      defp snapshot_boundary_position?(metadata, base_position) do
        case {Map.get(metadata, :position), Map.get(metadata, :snapshot_boundary_position)} do
          {position, position} -> position == base_position
          _other -> false
        end
      end

      defp snapshot_boundary_metadata?(%{position: position} = metadata),
        do: snapshot_boundary_position?(metadata, position)

      defp snapshot_boundary_metadata?(_metadata), do: false

      defp single_member_config?({_position, config}), do: single_member_config?(config)

      defp single_member_config?(%{participants: participants, witness: witness})
           when is_list(participants) do
        length(participants) == 1 and witness in [nil, []]
      end

      defp single_member_config?(%{membership: membership, witness: witness})
           when is_list(membership) do
        length(membership) == 1 and witness in [nil, []]
      end

      defp single_member_config?(_config), do: false

      defp recover_segment_projected_keydir(
             _root_dir,
             sm_state,
             {:raft_log_pos, index, _term},
             _after
           )
           when is_integer(index) and index <= 0,
           do: {:ok, sm_state, @zero_pos, %{history: %{}}}

      defp recover_segment_projected_keydir(
             root_dir,
             sm_state,
             target_position,
             replay_after_index
           ) do
        target_index = recovery_target_index(target_position)

        if target_index <= replay_after_index do
          {:ok, sm_state, recovery_base_position(target_position), %{history: %{}}}
        else
          initial = %{
            sm_state: sm_state,
            position: target_position_for_replay_start(target_position, replay_after_index),
            target_index: target_index,
            replay_after_index: replay_after_index,
            replay_dependencies: %{history: %{}},
            error: nil
          }

          case :ferricstore_waraft_spike_segment_log.fold_disk(
                 root_dir,
                 &recover_segment_projected_keydir_record/3,
                 initial
               ) do
            {:ok, %{error: nil} = acc} ->
              case validate_recovered_target_position(acc, target_position) do
                :ok ->
                  {:ok, acc.sm_state, acc.position, acc.replay_dependencies}

                {:error, reason} ->
                  {:error, reason}
              end

            {:ok, %{error: reason}} ->
              {:error, reason}

            {:error, _reason} = error ->
              error
          end
        end
      end

      defp recovery_target_index({:latest, _base_position}), do: :infinity
      defp recovery_target_index(target_position), do: position_index(target_position)

      defp recovery_base_position({:latest, base_position}), do: base_position
      defp recovery_base_position(position), do: position

      defp recover_segment_projected_keydir_record(_index, _entry, %{error: reason} = acc)
           when not is_nil(reason),
           do: acc

      defp recover_segment_projected_keydir_record(index, _entry, acc)
           when index <= acc.replay_after_index,
           do: acc

      defp recover_segment_projected_keydir_record(index, _entry, acc)
           when index > acc.target_index,
           do: acc

      defp recover_segment_projected_keydir_record(index, {term, _op} = entry, acc) do
        case command_from_segment_log_entry(entry) do
          {:ok, command} ->
            position = {:raft_log_pos, index, term}

            case recover_segment_projected_command(command, position, acc.sm_state) do
              {:ok, next_sm_state, replay_dependencies} ->
                %{
                  acc
                  | sm_state: next_sm_state,
                    position: position,
                    replay_dependencies:
                      merge_recovery_replay_dependencies(
                        acc.replay_dependencies,
                        replay_dependencies
                      )
                }

              {:error, reason} ->
                %{acc | error: {:segment_projected_keydir_recovery_failed, position, reason}}
            end

          :skip ->
            %{acc | position: {:raft_log_pos, index, term}}
        end
      end

      defp target_position_for_replay_start({:latest, base_position}, _after), do: base_position

      defp target_position_for_replay_start(_target_position, replay_after_index)
           when is_integer(replay_after_index) and replay_after_index > 0,
           do: {:raft_log_pos, replay_after_index, 0}

      defp target_position_for_replay_start(_target_position, _after), do: @zero_pos

      defp validate_recovered_target_position(_acc, {:latest, _base_position}), do: :ok

      defp validate_recovered_target_position(%{position: position}, target_position) do
        if recovered_position_reaches_target?(position, target_position) do
          :ok
        else
          {:error, {:segment_projected_keydir_recovery_incomplete, target_position, position}}
        end
      end

      defp recovered_position_reaches_target?(
             {:raft_log_pos, recovered_index, recovered_term},
             {:raft_log_pos, target_index, target_term}
           )
           when is_integer(recovered_index) and is_integer(target_index) do
        recovered_index > target_index or
          (recovered_index == target_index and recovered_term == target_term)
      end

      defp recovered_position_reaches_target?(_position, target_position),
        do: position_index(target_position) <= 0

      defp recover_segment_projected_command(command, _position, sm_state)
           when command in [:noop, :noop_omitted, :undefined],
           do: {:ok, sm_state, %{history: %{}}}

      defp recover_segment_projected_command(command, position, sm_state) do
        case segment_project_command(decoded_replay_command(command), position, sm_state) do
          {:ok, next_sm_state, _result, applied_increment} ->
            {:ok, bump_segment_projected_applied_count(next_sm_state, applied_increment),
             %{history: %{}}}

          :unsupported ->
            recover_segment_projected_state_machine_command(command, position, sm_state)
        end
      end

      defp recover_segment_projected_state_machine_command(command, position, sm_state) do
        apply_result =
          StateMachine.apply_waraft_segment_command(
            command,
            meta_from_position(position),
            sm_state,
            fn batch ->
              recover_segment_projection_batch(position, batch)
            end
          )

        replay_dependencies = StateMachine.consume_waraft_replay_dependencies()

        case apply_result do
          {next_sm_state, result} ->
            finish_recovered_state_machine_result(next_sm_state, result, replay_dependencies)

          {next_sm_state, result, _effects} ->
            finish_recovered_state_machine_result(next_sm_state, result, replay_dependencies)
        end
      end

      defp recover_segment_projection_batch(position, batch) do
        index = position_index(position)

        if index > 0 do
          {:ok, {:waraft_segment, index}, apply_projection_locations(batch, 0)}
        else
          {:error, {:bad_waraft_recovery_projection_position, position}}
        end
      end

      defp finish_recovered_state_machine_result(next_sm_state, result, replay_dependencies) do
        result = unwrap_applied_result(result)

        if storage_apply_failure?(result) do
          {:error, storage_block_reason(result)}
        else
          {:ok, next_sm_state, replay_dependencies}
        end
      end

      defp merge_recovery_replay_dependencies(left, right) when is_map(right) do
        history =
          right
          |> Map.get(:history, %{})
          |> normalize_replay_dependency_map()

        if map_size(history) == 0 do
          left
        else
          Map.update(left || %{}, :history, history, fn existing ->
            merge_replay_dependency_maps(existing, history)
          end)
        end
      end

      defp merge_recovery_replay_dependencies(left, _right), do: left || %{history: %{}}

      defp command_from_segment_log_entry({_term, {:default, {corr, command}}})
           when is_reference(corr),
           do: {:ok, command}

      defp command_from_segment_log_entry({_term, {corr, command}}) when is_reference(corr),
        do: {:ok, command}

      defp command_from_segment_log_entry({_term, command}) when is_tuple(command),
        do: {:ok, command}

      defp command_from_segment_log_entry(_entry), do: :skip

      defp reset_keydir!(ctx, shard_index, keydir) do
        case :ets.whereis(keydir) do
          :undefined ->
            :ets.new(keydir, [
              :set,
              :public,
              :named_table,
              {:read_concurrency, true},
              {:write_concurrency, :auto},
              {:decentralized_counters, true}
            ])

          _tid ->
            :ets.delete_all_objects(keydir)
        end

        if is_reference(ctx.keydir_binary_bytes) do
          :atomics.put(ctx.keydir_binary_bytes, shard_index + 1, 0)
        end

        if is_reference(ctx.expiry_key_counts) do
          :atomics.put(ctx.expiry_key_counts, shard_index + 1, 0)
        end

        if is_reference(ctx.expiry_next_due_at) do
          :atomics.put(ctx.expiry_next_due_at, shard_index + 1, 0)
        end
      end

      defp ensure_ets_table!(table_name, table_type) do
        case :ets.whereis(table_name) do
          :undefined ->
            :ets.new(table_name, [
              table_type,
              :public,
              :named_table,
              {:read_concurrency, true},
              {:write_concurrency, :auto}
            ])

          _tid ->
            :ets.delete_all_objects(table_name)
            table_name
        end
      end

      defp ensure_native_flow_index!(flow_index, flow_lookup) do
        Ferricstore.Flow.NativeOrderedIndex.reset(flow_index, flow_lookup)
        :ok
      end

      defp persist_metadata(%{root_dir: root_dir} = handle, mode) do
        with :ok <- maybe_persist_segment_projection(handle) do
          persist_storage_metadata(root_dir, storage_metadata(handle), mode)
        end
      end

      defp persist_hot_metadata(%{root_dir: root_dir} = handle) do
        persist_storage_metadata(root_dir, storage_metadata(handle), :normal)
      end

      defp storage_metadata(handle) do
        metadata = %{
          version: @version,
          position: handle.position,
          label: handle.label,
          config: handle.config
        }

        case Map.get(handle, :snapshot_boundary_position) do
          nil -> metadata
          position -> Map.put(metadata, :snapshot_boundary_position, position)
        end
      end

      defp maybe_persist_segment_projection(%{
             position: position,
             segment_projection_position: position
           }),
           do: :ok

      defp maybe_persist_segment_projection(%{
             root_dir: root_dir,
             position: position,
             sm_state: sm_state
           }) do
        with_segment_projection_lock(root_dir, fn ->
          with {:ok, entries} <- collect_segment_projected_entries_strict(sm_state) do
            root_dir
            |> segment_projection_root()
            |> write_segment_projection(position, entries)
          end
        end)
      end

      defp maybe_persist_segment_projection(_handle), do: :ok

      defp with_segment_projection_lock(root_dir, fun) when is_function(fun, 0) do
        # :global lock ids are {resource_id, requester_id}. The resource must be
        # the shard projection root so checkpoint/trim serialize across processes;
        # the requester must remain process-specific, otherwise :global treats a
        # second caller as the same requester and allows reentrant acquisition.
        lock = {{__MODULE__, :segment_projection, root_dir}, self()}

        case :global.trans(lock, fun, [node()]) do
          :aborted -> {:error, :segment_projection_lock_busy}
          result -> result
        end
      end

      defp initial_storage_metadata do
        %{
          version: @version,
          position: @zero_pos,
          label: nil,
          config: nil
        }
      end

      defp ensure_initial_storage_metadata!(metadata, root_dir) when map_size(metadata) == 0 do
        metadata = initial_storage_metadata()

        case persist_storage_metadata(root_dir, metadata, :compact) do
          :ok ->
            metadata

          {:error, reason} ->
            raise "failed to publish initial WARaft storage metadata: #{inspect(reason)}"
        end
      end

      defp ensure_initial_storage_metadata!(metadata, _root_dir), do: metadata

      defp persist_storage_metadata(root_dir, metadata, mode) do
        path = metadata_path(root_dir)

        with {:ok, payload} <- encode_storage_metadata(metadata) do
          if mode == :compact or storage_metadata_compaction_due?(metadata) do
            compact_storage_metadata(path, payload)
          else
            append_metadata_journal_payload(path, payload)
          end
        end
      end

      defp compact_storage_metadata(path, payload) do
        with :ok <- atomic_write_binary(path, payload) do
          case delete_metadata_journal(path) do
            :ok ->
              :ok

            {:error, _reason} = error ->
              _ = restore_previous_metadata_after_publish(path)
              error
          end
        end
      end

      defp encode_storage_metadata(metadata) do
        payload = metadata |> encode_persisted_metadata_term() |> :erlang.term_to_binary()

        if byte_size(payload) <= @max_storage_metadata_bytes do
          {:ok, payload}
        else
          {:error,
           {:storage_metadata_term_too_large, byte_size(payload), @max_storage_metadata_bytes}}
        end
      end

      defp storage_metadata_compaction_due?(metadata) do
        interval =
          Application.get_env(
            :ferricstore,
            :waraft_storage_metadata_compact_every,
            @default_metadata_compact_every
          )

        case {interval, Map.get(metadata, :position)} do
          {:never, _position} ->
            false

          {interval, {:raft_log_pos, index, _term}} when is_integer(interval) and interval > 0 ->
            rem(index, interval) == 0

          {_other, _position} ->
            false
        end
      end

      defp read_metadata!(path, ctx, shard_index) do
        case read_storage_metadata_file(path, :storage_metadata_file_too_large) do
          {:ok, binary} ->
            case persisted_binary_to_term(binary) do
              {:ok, %{version: @version} = metadata} ->
                case validate_storage_metadata(metadata) do
                  {:ok, validated} ->
                    prefer_newest_storage_metadata(path, validated)

                  {:error, reason} ->
                    raise "bad WARaft storage metadata in #{path}: #{inspect(reason)}"
                end

              {:ok, other} ->
                raise "bad WARaft storage metadata in #{path}: #{inspect(other)}"

              {:error, reason} ->
                recover_or_empty_metadata!(
                  path,
                  {:decode_storage_metadata, reason},
                  ctx,
                  shard_index
                )
            end

          {:error, :enoent} ->
            recover_or_empty_metadata!(
              path,
              :missing_current_storage_metadata,
              ctx,
              shard_index
            )

          {:error, {:storage_metadata_file_too_large, _size, _max} = reason} ->
            recover_or_empty_metadata!(
              path,
              {:read_storage_metadata, reason},
              ctx,
              shard_index
            )

          {:error, reason} ->
            raise "failed to read WARaft storage metadata in #{path}: #{inspect(reason)}"
        end
      end

      defp profile_startup_phase(shard_index, root_dir, phase, fun) when is_function(fun, 0) do
        started_at = System.monotonic_time(:microsecond)

        try do
          fun.()
        after
          duration_us = System.monotonic_time(:microsecond) - started_at

          :telemetry.execute(
            [:ferricstore, :waraft, :storage, :startup_phase],
            %{duration_us: duration_us},
            %{shard_index: shard_index, phase: phase, root_dir: root_dir}
          )
        end
      end

      defp profile_storage_apply_phase(handle, phase, fun) when is_function(fun, 0) do
        started_at = System.monotonic_time(:microsecond)
        result = fun.()
        duration_us = System.monotonic_time(:microsecond) - started_at

        :telemetry.execute(
          [:ferricstore, :waraft, :storage, :apply_phase],
          %{duration_us: duration_us},
          %{
            shard_index: Map.get(handle, :shard_index),
            position: Map.get(handle, :position),
            phase: phase,
            result: storage_apply_phase_result(result)
          }
        )

        result
      end

      defp storage_apply_phase_result({:ok, _handle}), do: :ok
      defp storage_apply_phase_result(:ok), do: :ok
      defp storage_apply_phase_result(:skipped), do: :skipped
      defp storage_apply_phase_result({:error, reason}), do: {:error, reason}
      defp storage_apply_phase_result(_other), do: :unknown

      defp recover_or_empty_metadata!(path, reason, ctx, shard_index) do
        case recover_storage_metadata(path, reason) do
          {:ok, metadata} ->
            metadata

          {:error, recovery_reason} ->
            case live_storage_payload_empty?(Path.dirname(path), ctx, shard_index) do
              {:ok, true} ->
                %{}

              {:ok, false} ->
                raise "failed to recover WARaft storage metadata in #{path}: #{inspect(reason)}; recovery failed: #{inspect(recovery_reason)}"

              {:error, payload_reason} ->
                raise "failed to recover WARaft storage metadata in #{path}: #{inspect(reason)}; payload check failed: #{inspect(payload_reason)}; recovery failed: #{inspect(recovery_reason)}"
            end
        end
      end

      defp recover_storage_metadata(path, reason) do
        case read_recovery_metadata_candidates(path) do
          {[], errors} ->
            {:error, errors}

          {candidates, _errors} ->
            {source_path, metadata} = recovery_storage_metadata_candidate(path, candidates)
            emit_storage_metadata_recovered(path, source_path, reason)
            {:ok, metadata}
        end
      end

      defp prefer_newest_storage_metadata(path, current_metadata) do
        if snapshot_boundary_metadata?(current_metadata) do
          current_metadata
        else
          {candidates, _errors} = read_recovery_metadata_candidates(path)

          {source_path, newest_metadata} =
            newest_storage_metadata_candidate([{path, current_metadata} | candidates])

          if source_path == path do
            current_metadata
          else
            emit_storage_metadata_recovered(path, source_path, :stale_current_storage_metadata)
            newest_metadata
          end
        end
      end
    end
  end
end
