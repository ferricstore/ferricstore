defmodule Ferricstore.Raft.WARaftStorage.Sections.Lifecycle do
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

      @spec validate_supported_apply_mode!() :: :ok
      def validate_supported_apply_mode!, do: :ok

      @spec finalize_segment_log_snapshot_boundary(charlist() | binary(), tuple()) ::
              :ok | {:error, term()}
      def finalize_segment_log_snapshot_boundary(root_dir, position) do
        root_dir = to_path(root_dir)

        with :ok <- clear_snapshot_boundary_metadata(root_dir, position) do
          finalize_snapshot_install_marker_if_matching(root_dir, position)
        end
      end

      @spec open(map(), charlist() | binary()) :: handle()
      def open(options, root_dir) do
        root_dir = to_path(root_dir)
        ctx = Ferricstore.Raft.WARaftBackend.context!(Map.fetch!(options, :table))
        shard_index = Map.fetch!(options, :partition) - 1

        case profile_startup_phase(shard_index, root_dir, :recover_pending_snapshot_install, fn ->
               recover_pending_snapshot_install(root_dir, ctx, shard_index)
             end) do
          :ok ->
            :ok

          {:error, reason} ->
            raise "failed to recover WARaft snapshot install: #{inspect(reason)}"
        end

        metadata =
          profile_startup_phase(shard_index, root_dir, :read_metadata, fn ->
            root_dir
            |> metadata_path()
            |> read_metadata!(ctx, shard_index)
            |> ensure_initial_storage_metadata!(root_dir)
          end)

        profile_startup_phase(shard_index, root_dir, :ensure_apply_projection_log, fn ->
          ensure_apply_projection_segment_log_ready!(root_dir)
        end)

        Ferricstore.Raft.WARaftBackend.cache_config(shard_index, Map.get(metadata, :config))

        sm_state =
          profile_startup_phase(shard_index, root_dir, :build_state, fn ->
            build_sm_state(ctx, shard_index)
          end)

        {sm_state, recovered_position, replay_dependencies} =
          profile_startup_phase(shard_index, root_dir, :recover_segment_projected_keydir, fn ->
            maybe_recover_segment_projected!(sm_state, root_dir, metadata)
          end)

        sm_state =
          profile_startup_phase(shard_index, root_dir, :rebuild_segment_indexes, fn ->
            rebuild_indexes_from_segment_keydir(sm_state, ctx, shard_index)
          end)

        metadata_position = Map.get(metadata, :position, @zero_pos)

        handle = %{
          options: options,
          ctx: ctx,
          root_dir: root_dir,
          shard_index: shard_index,
          sm_state: sm_state,
          position: recovered_position,
          persisted_position: metadata_position,
          segment_projection_position: metadata_position,
          last_clean_position: metadata_position,
          replay_dependencies: replay_dependencies,
          label: Map.get(metadata, :label),
          config: Map.get(metadata, :config),
          bitcask_dirty?: false
        }

        last_clean_position =
          if replay_dependencies_ready?(handle), do: recovered_position, else: metadata_position

        handle
        |> Map.put(:last_clean_position, last_clean_position)
        |> register_segment_projection_context()
      end

      @spec close(handle()) :: :ok | {:error, term()}
      def close(handle) do
        try do
          handle = flush_replay_dependencies_before_close(handle)

          with :ok <- maybe_fsync_payload_before_metadata(handle) do
            clean_handle =
              handle
              |> Map.put(:bitcask_dirty?, false)
              |> maybe_mark_clean_position()

            cond do
              not segment_keydir_available?(clean_handle) ->
                :ok

              replay_dependencies_ready?(clean_handle) ->
                clean_handle
                |> clear_replay_dependencies()
                |> persist_metadata(:compact)

              true ->
                request_replay_dependencies(clean_handle)
                :ok
            end
          end
        after
          unregister_segment_projection_context(handle)
        end
      end

      @spec position(handle()) :: tuple()
      def position(%{position: position}), do: position

      @spec status(handle()) :: keyword()
      def status(handle) do
        [
          applied_position: Map.get(handle, :position),
          durable_position: durable_position(handle),
          segment_projection_position: Map.get(handle, :segment_projection_position, @zero_pos),
          segment_projection_checkpoint_pending?:
            Map.has_key?(handle, :segment_projection_checkpoint),
          apply_projection_cache_compaction_pending?:
            Map.has_key?(handle, :apply_projection_cache_compaction),
          payload_dirty?: Map.get(handle, :bitcask_dirty?, false),
          blocked?: Map.has_key?(handle, :blocked_error)
        ]
        |> maybe_put_status(:blocked_error, Map.get(handle, :blocked_error))
      end

      @spec label(handle()) :: {:ok, term()}
      def label(%{label: label}), do: {:ok, label}

      @spec config(handle()) :: {:ok, tuple(), term()} | :undefined
      def config(%{config: nil}), do: :undefined
      def config(%{config: {position, config}}), do: {:ok, position, config}

      @spec prepare_segment_projection_for_trim(charlist() | binary(), non_neg_integer()) ::
              :ok | {:error, term()}
      def prepare_segment_projection_for_trim(root_dir, trim_index)
          when is_integer(trim_index) and trim_index >= 0 do
        root_dir = to_path(root_dir)

        with_segment_projection_lock(root_dir, fn ->
          projection_root = segment_projection_root(root_dir)

          case lookup_segment_projection_context(root_dir) do
            {:ok, context} ->
              with :ok <- validate_segment_projection_trim_position(context.position, trim_index) do
                case prepare_segment_projection_from_checkpoint(
                       root_dir,
                       projection_root,
                       context,
                       trim_index
                     ) do
                  :ok ->
                    :ok

                  :not_available ->
                    rebuild_segment_projection_for_trim(
                      root_dir,
                      projection_root,
                      context,
                      trim_index
                    )

                  {:error, _reason} = error ->
                    error
                end
              end

            {:error, reason}
            when reason in [
                   {:segment_projection_registry_missing, root_dir},
                   {:segment_projection_context_missing, root_dir}
                 ] ->
              # The segment log provider is reused by small WARaft spike storage
              # modules that do not project Bitcask/keydir data from the log. Only
              # skip projection prep when no projection/checkpoint files exist.
              # If any projection state is present, missing registry context is a
              # production consistency error and trim must fail closed.
              if segment_projection_files_present?(root_dir) do
                {:error, reason}
              else
                :ok
              end

            {:error, _reason} = error ->
              error
          end
        end)
      end

      def prepare_segment_projection_for_trim(_root_dir, trim_index),
        do: {:error, {:bad_trim_index, trim_index}}

      @doc false
      def __prepare_segment_value_pins_for_trim_for_test__(
            root_dir,
            ctx,
            shard_index,
            trim_index,
            page_limit
          )
          when is_integer(page_limit) and page_limit > 0 do
        prepare_segment_value_pins_for_trim(
          to_path(root_dir),
          ctx,
          shard_index,
          trim_index,
          page_limit
        )
      end

      defp prepare_segment_projection_from_checkpoint(
             root_dir,
             projection_root,
             context,
             trim_index
           ) do
        checkpoint_root = segment_projection_checkpoint_root(root_dir)

        with {:ok, projection} <- read_segment_projection_log(checkpoint_root),
             true <- position_index(projection.position) >= trim_index,
             {:ok, entries} <- validate_segment_projection_entries(projection),
             {:ok, relocations} <-
               segment_projection_checkpoint_relocations(
                 context.ctx,
                 context.shard_index,
                 entries,
                 trim_index
               ),
             :ok <- write_segment_projection(projection_root, projection.position, entries),
             {:ok, value_pin_relocation_count} <-
               prepare_segment_value_pins_for_trim(
                 root_dir,
                 context.ctx,
                 context.shard_index,
                 trim_index
               ),
             :ok <-
               relocate_segment_projection_keydir_from_checkpoint(
                 context.ctx,
                 context.shard_index,
                 projection_root,
                 relocations
               ),
             :ok <-
               prune_apply_projection_cache_after_segment_projection(
                 context.ctx,
                 context.shard_index,
                 trim_index,
                 relocations
               ) do
          emit_segment_projection_trim_checkpoint_reuse(%{
            shard_index: context.shard_index,
            trim_index: trim_index,
            checkpoint_index: position_index(projection.position),
            relocations: length(relocations),
            value_pin_relocations: value_pin_relocation_count
          })

          :ok
        else
          {:error, :enoent} -> :not_available
          false -> :not_available
          {:error, _reason} = error -> error
          _other -> :not_available
        end
      end

      defp rebuild_segment_projection_for_trim(root_dir, projection_root, context, trim_index) do
        with {:ok, relocations} <-
               collect_segment_projection_relocations(context.ctx, context.shard_index),
             entries = segment_projection_entries_from_relocations(relocations),
             :ok <-
               write_segment_projection(
                 projection_root,
                 context.position,
                 entries
               ),
             {:ok, _value_pin_relocation_count} <-
               prepare_segment_value_pins_for_trim(
                 root_dir,
                 context.ctx,
                 context.shard_index,
                 trim_index
               ),
             :ok <-
               relocate_segment_projection_keydir(
                 context.ctx,
                 context.shard_index,
                 projection_root,
                 relocations
               ),
             :ok <-
               prune_apply_projection_cache_after_segment_projection(
                 context.ctx,
                 context.shard_index,
                 trim_index,
                 relocations
               ) do
          :ok
        end
      end

      @spec apply(term(), tuple(), handle()) :: {term(), handle()}
      def apply(command, position, handle) do
        apply_command(command, position, handle, :keep_label)
      end

      @spec apply(term(), tuple(), term(), handle()) :: {term(), handle()}
      def apply(command, position, label, handle) do
        apply_command(command, position, handle, {:replace_label, label})
      end

      @spec apply_config(term(), tuple(), handle()) :: {:ok | {:error, term()}, handle()}
      def apply_config(_config, position, %{blocked_error: reason} = handle) do
        emit_storage_blocked(handle, reason, position, :blocked_config)
        {{:error, {:storage_blocked, reason}}, handle}
      end

      def apply_config(config, position, handle) do
        new_handle = %{handle | config: {position, config}, position: position}

        with :ok <- maybe_fsync_payload_before_metadata(new_handle),
             clean_handle = Map.put(new_handle, :bitcask_dirty?, false),
             :ok <- persist_metadata(clean_handle, :compact) do
          Ferricstore.Raft.WARaftBackend.cache_config(handle.shard_index, config)

          {:ok,
           clean_handle |> mark_metadata_persisted() |> register_segment_projection_context()}
        else
          {:error, reason} ->
            {{:error, reason}, block_storage(handle, reason, position, :metadata_failure)}
        end
      end

      @spec read(term(), tuple(), handle()) :: term()
      def read(:position, _position, handle), do: handle.position

      def read({:get, key}, _position, %{ctx: ctx}) when is_binary(key) do
        case Ferricstore.Store.Router.get(ctx, key) do
          nil -> :not_found
          value -> {:ok, value}
        end
      end

      def read(_command, _position, _handle), do: :ok

      @spec info(term(), handle()) :: {:ok, handle()} | :ignore
      def info({:ferricstore_waraft_segment_projection_checkpoint_done, ref, result}, handle) do
        finish_segment_projection_checkpoint(ref, result, handle)
      end

      def info({:ferricstore_waraft_apply_projection_cache_compact_done, ref, result}, handle) do
        finish_apply_projection_cache_compaction(ref, result, handle)
      end

      def info(_info, _handle), do: :ignore

      @spec create_snapshot(charlist() | binary(), handle()) :: :ok | {:error, term()}
      def create_snapshot(_snapshot_path, %{blocked_error: reason} = handle) do
        emit_storage_blocked(handle, reason, handle.position, :blocked_snapshot)
        {:error, {:storage_blocked, reason}}
      end

      def create_snapshot(snapshot_path, handle) do
        snapshot_path = to_path(snapshot_path)

        with :ok <- reset_dir(snapshot_path),
             :ok <- copy_shard_dirs_to_snapshot(snapshot_path, handle),
             :ok <- drain_apply_projection_cache_compaction_for_snapshot(handle),
             :ok <- flush_apply_projection_snapshot_payload(handle),
             :ok <- copy_storage_dirs_to_snapshot(snapshot_path, handle),
             {:ok, segment_projection} <-
               maybe_write_snapshot_segment_projection(snapshot_path, handle),
             :ok <- write_snapshot_metadata(snapshot_path, handle, segment_projection),
             :ok <- fsync_dir(snapshot_path) do
          :ok
        end
      end

      @spec create_witness_snapshot(charlist() | binary(), handle()) :: :ok | {:error, term()}
      def create_witness_snapshot(_snapshot_path, %{blocked_error: reason} = handle) do
        emit_storage_blocked(handle, reason, handle.position, :blocked_witness_snapshot)
        {:error, {:storage_blocked, reason}}
      end

      def create_witness_snapshot(snapshot_path, handle) do
        snapshot_path = to_path(snapshot_path)

        with :ok <- reset_dir(snapshot_path),
             :ok <- create_empty_snapshot_payload_dirs(snapshot_path),
             :ok <- create_empty_snapshot_storage_payload_dirs(snapshot_path),
             :ok <- write_snapshot_metadata(snapshot_path, handle),
             :ok <- fsync_dir(snapshot_path) do
          :ok
        end
      end

      @spec open_snapshot(charlist() | binary(), tuple(), handle()) ::
              {:ok, handle()} | {:error, term()}
      def open_snapshot(snapshot_path, expected_position, handle) do
        snapshot_path = to_path(snapshot_path)

        with {:ok, metadata} <- read_snapshot_metadata(snapshot_path),
             :ok <- verify_snapshot_position(metadata, expected_position),
             :ok <- verify_snapshot_payload_dirs(metadata, snapshot_path, handle),
             {:ok, segment_projection_entries} <-
               read_snapshot_segment_projection(snapshot_path, metadata, expected_position),
             {:ok, install} <-
               copy_snapshot_to_shard_dirs(
                 snapshot_path,
                 handle,
                 Map.fetch!(metadata, :position),
                 metadata
               ) do
          position = Map.fetch!(metadata, :position)

          projection_source =
            segment_projection_apply_source(handle.root_dir, position, segment_projection_entries)

          sm_state =
            handle.ctx
            |> build_sm_state(handle.shard_index)
            |> apply_segment_projection_entries(projection_source, segment_projection_entries)

          new_handle =
            handle
            |> Map.put(:sm_state, sm_state)
            |> Map.put(:position, position)
            |> Map.put(:persisted_position, position)
            |> Map.put(:last_clean_position, position)
            |> Map.put(:snapshot_boundary_position, position)
            |> Map.put(:label, Map.get(metadata, :label))
            |> Map.put(:config, Map.get(metadata, :config))
            |> Map.put(:bitcask_dirty?, false)

          case ensure_apply_projection_segment_log_ready(handle.root_dir) do
            :ok ->
              case persist_metadata(new_handle, :compact) do
                :ok ->
                  _ =
                    Ferricstore.Raft.WARaftSegmentReader.clear_apply_projection_cache(
                      handle.ctx.data_dir,
                      handle.shard_index
                    )

                  new_handle = Map.delete(new_handle, :snapshot_boundary_position)

                  Ferricstore.Raft.WARaftBackend.cache_config(
                    handle.shard_index,
                    Map.get(metadata, :config)
                  )

                  case finish_persisted_snapshot_install(install) do
                    :ok ->
                      {:ok,
                       new_handle
                       |> mark_metadata_persisted()
                       |> register_segment_projection_context()}

                    {:error, reason} ->
                      blocked_handle =
                        block_storage(
                          new_handle,
                          {:finalize_snapshot_install_failed, reason},
                          position,
                          :snapshot_install_finalize_failure
                        )

                      {:ok,
                       blocked_handle
                       |> mark_metadata_persisted()
                       |> register_segment_projection_context()}
                  end

                {:error, reason} ->
                  _ = rollback_snapshot_install_and_restore_runtime(install, handle)
                  {:error, reason}
              end

            {:error, reason} ->
              _ = rollback_snapshot_install_and_restore_runtime(install, handle)
              {:error, {:apply_projection_log_init_failed, reason}}
          end
        end
      end

      defp segment_projection_apply_source(_root_dir, position, []), do: position

      defp segment_projection_apply_source(root_dir, _position, _entries),
        do: segment_projection_root(root_dir)

      @spec make_empty_snapshot(map(), charlist() | binary(), tuple(), term(), term()) ::
              :ok | {:error, term()}
      def make_empty_snapshot(_options, snapshot_path, position, config, _data) do
        snapshot_path = to_path(snapshot_path)

        metadata = %{
          version: @version,
          position: position,
          label: nil,
          config: {position, config},
          payload_dirs: snapshot_payload_kinds(),
          empty_payload_dirs: snapshot_payload_kinds(),
          storage_payload_dirs: snapshot_storage_payload_kinds(),
          empty_storage_payload_dirs: snapshot_storage_payload_kinds()
        }

        with :ok <- reset_dir(snapshot_path),
             :ok <- create_empty_snapshot_payload_dirs(snapshot_path),
             :ok <- create_empty_snapshot_storage_payload_dirs(snapshot_path),
             :ok <- atomic_write_snapshot_metadata(snapshot_path, metadata),
             :ok <- fsync_dir(snapshot_path) do
          :ok
        end
      end

      defp apply_command(_command, position, %{blocked_error: reason} = handle, _label_update) do
        emit_storage_blocked(handle, reason, position, :blocked_apply)
        {{:error, {:storage_blocked, reason}}, handle}
      end

      defp apply_command(:noop, position, handle, label_update),
        do: persist_position(position, :ok, handle, maybe_update_label(handle, label_update))

      defp apply_command(:noop_omitted, position, handle, label_update),
        do: persist_position(position, :ok, handle, maybe_update_label(handle, label_update))

      defp apply_command(command, position, handle, label_update) do
        case apply_segment_projected_command(command, position, handle, label_update) do
          :unsupported ->
            apply_state_machine_command_and_persist(command, position, handle, label_update)

          result ->
            result
        end
      end

      defp apply_state_machine_command_and_persist(
             command,
             position,
             %{sm_state: sm_state} = handle,
             label_update
           ) do
        clear_apply_projection_replay_dependencies()

        try do
          apply_result = apply_state_machine_command(command, position, sm_state)

          replay_dependencies =
            StateMachine.consume_waraft_replay_dependencies()
            |> merge_apply_projection_replay_dependencies(
              consume_apply_projection_replay_dependencies()
            )

          case apply_result do
            {new_sm_state, result} ->
              finish_apply_result(
                command,
                position,
                unwrap_applied_result(result),
                handle,
                %{
                  handle
                  | sm_state: new_sm_state
                }
                |> merge_replay_dependencies(replay_dependencies)
                |> maybe_mark_bitcask_dirty()
                |> maybe_update_label(label_update)
              )

            {new_sm_state, result, _effects} ->
              finish_apply_result(
                command,
                position,
                unwrap_applied_result(result),
                handle,
                %{
                  handle
                  | sm_state: new_sm_state
                }
                |> merge_replay_dependencies(replay_dependencies)
                |> maybe_mark_bitcask_dirty()
                |> maybe_update_label(label_update)
              )
          end
        after
          clear_apply_projection_replay_dependencies()
        end
      end

      defp apply_segment_projected_command(
             command,
             position,
             handle,
             label_update
           ) do
        decoded_command = decoded_replay_command(command)

        with_segment_projection_command_time(command, fn ->
          do_apply_segment_projected_command(
            decoded_command,
            command,
            position,
            handle,
            label_update
          )
        end)
      end

      defp do_apply_segment_projected_command(
             decoded_command,
             command,
             position,
             handle,
             label_update
           ) do
        case segment_project_command(decoded_command, position, handle.sm_state) do
          {:ok, new_sm_state, result, applied_increment} ->
            finish_apply_result(
              command,
              position,
              result,
              handle,
              %{
                handle
                | sm_state: bump_segment_projected_applied_count(new_sm_state, applied_increment)
              }
              |> maybe_update_label(label_update)
            )

          :unsupported ->
            :unsupported
        end
      end

      defp segment_projection_locks_present?(sm_state) do
        case Map.get(sm_state, :cross_shard_locks, %{}) do
          locks when is_map(locks) -> map_size(locks) > 0
          _other -> false
        end
      end
    end
  end
end
