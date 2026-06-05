defmodule Ferricstore.Raft.WARaftStorage do
  @moduledoc false

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

  @metadata_file "ferricstore_storage.term"
  @snapshot_metadata_file "ferricstore_snapshot.term"
  @segment_projection_dir "segment_projection_log"
  @segment_projection_checkpoint_dir "segment_projection_checkpoint_log"
  @apply_projection_dir "apply_projection_log"
  @snapshot_install_marker_file "snapshot_install.term"
  @metadata_previous_suffix ".previous"
  @metadata_journal_suffix ".journal"
  @metadata_journal_magic "FSMJ1"
  @max_storage_metadata_bytes 1_048_576
  @max_metadata_journal_record_bytes @max_storage_metadata_bytes
  @max_snapshot_metadata_bytes @max_storage_metadata_bytes
  @max_snapshot_install_marker_bytes @max_storage_metadata_bytes
  @version 1
  @default_storage_metadata_persist_every 1_024
  @default_segment_projection_checkpoint_every 1_000_000
  @default_segment_projection_checkpoint_min_interval_ms 30_000
  @default_metadata_compact_every 1024
  @default_snapshot_compaction_drain_timeout_ms 30_000
  @cold_read_timeout_ms 10_000
  @zero_pos {:raft_log_pos, 0, 0}
  @encoded_peer_tag :ferricstore_waraft_peer
  @segment_projection_registry :ferricstore_waraft_segment_projection_registry
  @storage_root "ferricstore_waraft_backend"
  @segment_value_pin_scan_limit 100_000
  @apply_projection_replay_dependencies_key :ferricstore_waraft_apply_projection_replay_dependencies

  defguardp valid_segment_backed_file_id(file_id)
            when is_tuple(file_id) and tuple_size(file_id) == 2 and
                   (elem(file_id, 0) == :waraft_segment or
                      elem(file_id, 0) == :waraft_projection or
                      elem(file_id, 0) == :waraft_apply_projection) and
                   is_integer(elem(file_id, 1)) and elem(file_id, 1) > 0

  @type handle :: map()

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
      :ok -> :ok
      {:error, reason} -> raise "failed to recover WARaft snapshot install: #{inspect(reason)}"
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

  defp prepare_segment_projection_from_checkpoint(root_dir, projection_root, context, trim_index) do
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
      {:ok, clean_handle |> mark_metadata_persisted() |> register_segment_projection_context()}
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
      do_apply_segment_projected_command(decoded_command, command, position, handle, label_update)
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

  defp segment_project_command({:put, key, value, expire_at_ms}, position, sm_state) do
    redis_key = if is_binary(key), do: CompoundKey.extract_redis_key(key)

    with :ok <- segment_project_check_key_lock(sm_state, redis_key, nil),
         true <- segment_projectable_put?(sm_state, key, value, expire_at_ms) do
      {:ok, segment_project_put(sm_state, key, value, expire_at_ms, position), :ok, 1}
    else
      {:error, _reason} = error -> {:ok, sm_state, error, 0}
      false -> :unsupported
    end
  end

  defp segment_project_command(
         {:put_blob_ref, key, encoded_ref, expire_at_ms},
         position,
         sm_state
       ) do
    redis_key = if is_binary(key), do: CompoundKey.extract_redis_key(key)

    with :ok <- segment_project_check_key_lock(sm_state, redis_key, nil),
         true <- segment_projectable_blob_ref_put?(key, encoded_ref, expire_at_ms) do
      case verify_segment_blob_refs(sm_state, [encoded_ref]) do
        :ok ->
          {:ok, segment_project_put_blob_ref(sm_state, key, encoded_ref, expire_at_ms, position),
           :ok, 1}

        {:error, _reason} = error ->
          {:ok, sm_state, error, 0}
      end
    else
      {:error, _reason} = error -> {:ok, sm_state, error, 0}
      false -> :unsupported
    end
  end

  defp segment_project_command(
         {:locked_put, key, value, expire_at_ms, owner_ref},
         position,
         sm_state
       ) do
    redis_key = if is_binary(key), do: CompoundKey.extract_redis_key(key)

    with :ok <- segment_project_check_key_lock(sm_state, redis_key, owner_ref),
         true <- segment_projectable_put?(sm_state, key, value, expire_at_ms) do
      {:ok, segment_project_put(sm_state, key, value, expire_at_ms, position), :ok, 1}
    else
      {:error, _reason} = error -> {:ok, sm_state, error, 0}
      false -> :unsupported
    end
  end

  defp segment_project_command(
         {:locked_put_blob_ref, key, encoded_ref, expire_at_ms, owner_ref},
         position,
         sm_state
       ) do
    redis_key = if is_binary(key), do: CompoundKey.extract_redis_key(key)

    with :ok <- segment_project_check_key_lock(sm_state, redis_key, owner_ref),
         true <- segment_projectable_blob_ref_put?(key, encoded_ref, expire_at_ms),
         :ok <- verify_segment_blob_refs(sm_state, [encoded_ref]) do
      {:ok, segment_project_put_blob_ref(sm_state, key, encoded_ref, expire_at_ms, position), :ok,
       1}
    else
      {:error, _reason} = error -> {:ok, sm_state, error, 0}
      false -> :unsupported
    end
  end

  defp segment_project_command({:delete, key}, _position, sm_state) when is_binary(key) do
    redis_key = CompoundKey.extract_redis_key(key)

    case segment_project_check_key_lock(sm_state, redis_key, nil) do
      :ok -> {:ok, segment_project_delete(sm_state, key), :ok, 1}
      {:error, _reason} = error -> {:ok, sm_state, error, 0}
    end
  end

  defp segment_project_command({:locked_delete, key, owner_ref}, _position, sm_state)
       when is_binary(key) do
    redis_key = CompoundKey.extract_redis_key(key)

    case segment_project_check_key_lock(sm_state, redis_key, owner_ref) do
      :ok -> {:ok, segment_project_delete(sm_state, key), :ok, 1}
      {:error, _reason} = error -> {:ok, sm_state, error, 0}
    end
  end

  defp segment_project_command(
         {:compound_put, compound_key, value, expire_at_ms},
         position,
         sm_state
       ) do
    redis_key = if is_binary(compound_key), do: CompoundKey.extract_redis_key(compound_key)

    with :ok <- segment_project_check_key_lock(sm_state, redis_key, nil),
         true <-
           segment_projectable_compound_put?(
             sm_state,
             redis_key,
             compound_key,
             value,
             expire_at_ms
           ) do
      new_sm_state =
        sm_state
        |> segment_project_put(compound_key, value, expire_at_ms, position)
        |> segment_project_zset_put(redis_key, compound_key, value)

      {:ok, new_sm_state, :ok, 1}
    else
      {:error, _reason} = error -> {:ok, sm_state, error, 0}
      false -> :unsupported
    end
  end

  defp segment_project_command(
         {:compound_put_blob_ref, compound_key, encoded_ref, expire_at_ms},
         position,
         sm_state
       ) do
    redis_key = if is_binary(compound_key), do: CompoundKey.extract_redis_key(compound_key)

    if segment_projectable_compound_blob_ref_put?(
         sm_state,
         redis_key,
         compound_key,
         encoded_ref,
         expire_at_ms
       ) and segment_project_check_key_lock(sm_state, redis_key, nil) == :ok do
      case verify_segment_blob_refs(sm_state, [encoded_ref]) do
        :ok ->
          new_sm_state =
            sm_state
            |> segment_project_put_blob_ref(compound_key, encoded_ref, expire_at_ms, position)
            |> segment_project_zset_put(redis_key, compound_key, encoded_ref)

          {:ok, new_sm_state, :ok, 1}

        {:error, _reason} = error ->
          {:ok, sm_state, error, 0}
      end
    else
      case segment_project_check_key_lock(sm_state, redis_key, nil) do
        {:error, _reason} = error -> {:ok, sm_state, error, 0}
        _other -> :unsupported
      end
    end
  end

  defp segment_project_command({:compound_delete, compound_key}, _position, sm_state)
       when is_binary(compound_key) do
    redis_key = CompoundKey.extract_redis_key(compound_key)

    with :ok <- segment_project_check_key_lock(sm_state, redis_key, nil),
         true <- segment_shared_compound_projection_safe?(sm_state, redis_key) do
        new_sm_state =
          sm_state
          |> segment_project_delete(compound_key)
          |> segment_project_zset_delete(redis_key, compound_key)

        {:ok, new_sm_state, :ok, 1}
    else
      {:error, _reason} = error ->
        {:ok, sm_state, error, 0}

      false ->
        :unsupported
    end
  end

  defp segment_project_command({:put_batch, entries}, position, sm_state) when is_list(entries) do
    cond do
      segment_projection_locks_present?(sm_state) ->
      case put_batch_entry_commands(entries) do
        {:ok, commands} -> segment_project_generic_batch(commands, position, sm_state)
        :error -> :unsupported
      end

      segment_project_batch_has_blob_candidate?(sm_state, entries) ->
        :unsupported

      true ->
      file_id = {:waraft_segment, position_index(position)}
      offset = segment_record_offset(sm_state, position)
      shard_state = shard_ets_state_from_sm(sm_state)
      threshold = ShardETS.hot_cache_threshold(shard_state)

      case segment_project_batch_hot_cache_threshold(entries, threshold) do
        {:ok, batch_threshold} ->
          case ShardETS.ets_insert_fresh_no_expiry_many_with_location(
                 shard_state,
                 entries,
                 file_id,
                 offset,
                 batch_threshold
               ) do
            {:ok, count} ->
              {:ok, sm_state, {:ok, List.duplicate(:ok, count)}, count}

            :fallback ->
              apply_segment_put_batch_entries(
                entries,
                sm_state,
                shard_state,
                threshold,
                file_id,
                offset,
                0
              )
          end

        :per_key ->
          apply_segment_put_batch_entries(
            entries,
            sm_state,
            shard_state,
            threshold,
            file_id,
            offset,
            0
          )
      end
    end
  end

  defp segment_project_command({:put_blob_batch, entries}, position, sm_state)
       when is_list(entries) do
    with {:ok, prepared, encoded_refs} <- prepare_segment_blob_batch_entries(entries),
         :ok <- verify_segment_blob_refs(sm_state, encoded_refs) do
      new_sm_state =
        Enum.reduce(prepared, sm_state, fn
          {:value, key, value, expire_at_ms}, acc ->
            segment_project_put(acc, key, value, expire_at_ms, position)

          {:blob_ref, key, encoded_ref, expire_at_ms}, acc ->
            segment_project_put_blob_ref(acc, key, encoded_ref, expire_at_ms, position)
        end)

      {:ok, new_sm_state, {:ok, List.duplicate(:ok, length(entries))}, length(entries)}
    else
      {:unsupported, _reason} ->
        :unsupported

      {:error, _reason} = error ->
        {:ok, sm_state, error, 0}
    end
  end

  defp segment_project_command(
         {:compound_batch_put, redis_key, entries},
         position,
         sm_state
       )
       when is_binary(redis_key) and is_list(entries) do
    if segment_projectable_compound_batch_put?(sm_state, redis_key, entries) do
      new_sm_state =
        Enum.reduce(entries, sm_state, fn {compound_key, value, expire_at_ms}, acc ->
          acc
          |> segment_project_put(compound_key, value, expire_at_ms, position)
          |> segment_project_zset_put(redis_key, compound_key, value)
        end)

      {:ok, new_sm_state, {:ok, List.duplicate(:ok, length(entries))}, length(entries)}
    else
      :unsupported
    end
  end

  defp segment_project_command(
         {:compound_blob_batch_put, redis_key, entries},
         position,
         sm_state
       )
       when is_binary(redis_key) and is_list(entries) do
    with {:ok, prepared, encoded_refs} <-
           prepare_segment_compound_blob_batch_entries(redis_key, entries),
         true <- segment_projectable_prepared_compound_blob_batch?(sm_state, redis_key, prepared),
         :ok <- verify_segment_blob_refs(sm_state, encoded_refs) do
      new_sm_state =
        Enum.reduce(prepared, sm_state, fn
          {:value, compound_key, value, expire_at_ms}, acc ->
            acc
            |> segment_project_put(compound_key, value, expire_at_ms, position)
            |> segment_project_zset_put(redis_key, compound_key, value)

          {:blob_ref, compound_key, encoded_ref, expire_at_ms}, acc ->
            acc
            |> segment_project_put_blob_ref(compound_key, encoded_ref, expire_at_ms, position)
            |> segment_project_zset_put(redis_key, compound_key, encoded_ref)
        end)

      {:ok, new_sm_state, {:ok, List.duplicate(:ok, length(entries))}, length(entries)}
    else
      {:unsupported, _reason} ->
        :unsupported

      false ->
        :unsupported

      {:error, _reason} = error ->
        {:ok, sm_state, error, 0}
    end
  end

  defp segment_project_command({:delete_batch, keys}, position, sm_state) when is_list(keys) do
    if segment_projection_locks_present?(sm_state) do
      case delete_batch_entry_commands(keys) do
        {:ok, commands} -> segment_project_generic_batch(commands, position, sm_state)
        :error -> :unsupported
      end
    else
      if Enum.all?(keys, &is_binary/1) do
        new_sm_state = Enum.reduce(keys, sm_state, &segment_project_delete(&2, &1))
        {:ok, new_sm_state, {:ok, List.duplicate(:ok, length(keys))}, length(keys)}
      else
        :unsupported
      end
    end
  end

  defp segment_project_command(
         {:compound_batch_delete, redis_key, compound_keys},
         _position,
         sm_state
       )
       when is_binary(redis_key) and is_list(compound_keys) do
    if segment_shared_compound_projection_safe?(sm_state, redis_key) and
         Enum.all?(compound_keys, &compound_key_for_redis_key?(redis_key, &1)) do
      new_sm_state =
        Enum.reduce(compound_keys, sm_state, fn compound_key, acc ->
          acc
          |> segment_project_delete(compound_key)
          |> segment_project_zset_delete(redis_key, compound_key)
        end)

      {:ok, new_sm_state, {:ok, List.duplicate(:ok, length(compound_keys))},
       length(compound_keys)}
    else
      :unsupported
    end
  end

  defp segment_project_command({:compound_delete_prefix, prefix}, _position, sm_state)
       when is_binary(prefix) do
    redis_key = CompoundKey.extract_redis_key(prefix)

    with :ok <- segment_project_check_key_lock(sm_state, redis_key, nil),
         true <- segment_shared_compound_projection_safe?(sm_state, redis_key) do
        new_sm_state = segment_project_delete_prefix(sm_state, redis_key, prefix)
        {:ok, new_sm_state, :ok, 1}
    else
      {:error, _reason} = error ->
        {:ok, sm_state, error, 0}

      false ->
        :unsupported
    end
  end

  defp segment_project_command({:locked_delete_prefix, prefix, owner_ref}, _position, sm_state)
       when is_binary(prefix) do
    redis_key = CompoundKey.extract_redis_key(prefix)

    case segment_project_check_key_lock(sm_state, redis_key, owner_ref) do
      :ok ->
        {:ok, segment_project_delete_prefix(sm_state, redis_key, prefix), :ok, 1}

      {:error, _reason} = error ->
        {:ok, sm_state, error, 0}
    end
  end

  defp segment_project_command({:batch, commands}, position, sm_state) when is_list(commands) do
    case segment_project_decode_batch(commands, :unknown, [], []) do
      {:put_batch, entries} ->
        segment_project_command({:put_batch, entries}, position, sm_state)

      {:delete_batch, keys} ->
        segment_project_command({:delete_batch, keys}, position, sm_state)

      {:generic, commands} ->
        segment_project_generic_batch(commands, position, sm_state)
    end
  end

  defp segment_project_command(_command, _position, _sm_state), do: :unsupported

  defp put_batch_entry_commands(entries) do
    Enum.reduce_while(entries, {:ok, []}, fn
      {key, value, expire_at_ms}, {:ok, acc}
      when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) and
             expire_at_ms >= 0 ->
        {:cont, {:ok, [{:put, key, value, expire_at_ms} | acc]}}

      _entry, {:ok, _acc} ->
        {:halt, :error}
    end)
    |> case do
      {:ok, commands} -> {:ok, Enum.reverse(commands)}
      :error -> :error
    end
  end

  defp delete_batch_entry_commands(keys) do
    Enum.reduce_while(keys, {:ok, []}, fn
      key, {:ok, acc} when is_binary(key) ->
        {:cont, {:ok, [{:delete, key} | acc]}}

      _key, {:ok, _acc} ->
        {:halt, :error}
    end)
    |> case do
      {:ok, commands} -> {:ok, Enum.reverse(commands)}
      :error -> :error
    end
  end

  defp segment_project_decode_batch([], :put, _decoded_acc, entries) do
    {:put_batch, Enum.reverse(entries)}
  end

  defp segment_project_decode_batch([], :delete, _decoded_acc, keys) do
    {:delete_batch, Enum.reverse(keys)}
  end

  defp segment_project_decode_batch([], _kind, decoded_acc, _fast_acc) do
    {:generic, Enum.reverse(decoded_acc)}
  end

  defp segment_project_decode_batch([command | rest], kind, decoded_acc, fast_acc) do
    decoded = decoded_replay_command(command)

    case {kind, decoded} do
      {:unknown, {:put, key, value, expire_at_ms}}
      when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) and expire_at_ms >= 0 ->
        segment_project_decode_batch(rest, :put, [decoded | decoded_acc], [
          {key, value, expire_at_ms} | fast_acc
        ])

      {:put, {:put, key, value, expire_at_ms}}
      when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) and expire_at_ms >= 0 ->
        segment_project_decode_batch(rest, :put, [decoded | decoded_acc], [
          {key, value, expire_at_ms} | fast_acc
        ])

      {:unknown, {:delete, key}} when is_binary(key) ->
        segment_project_decode_batch(rest, :delete, [decoded | decoded_acc], [key | fast_acc])

      {:delete, {:delete, key}} when is_binary(key) ->
        segment_project_decode_batch(rest, :delete, [decoded | decoded_acc], [key | fast_acc])

      {:generic, _decoded} ->
        segment_project_decode_batch(rest, :generic, [decoded | decoded_acc], [])

      {_homogeneous, _decoded} ->
        segment_project_decode_batch(rest, :generic, [decoded | decoded_acc], [])
    end
  end

  defp segment_project_generic_batch(commands, position, sm_state) do
    if Enum.all?(commands, &segment_projectable_batch_command?(sm_state, &1)) do
      Enum.reduce_while(commands, {sm_state, [], 0}, fn command,
                                                        {acc_state, acc_results, acc_count} ->
        {:ok, next_state, result, count} = segment_project_command(command, position, acc_state)

        if storage_apply_failure?(result) do
          {:halt, {:storage_error, next_state, result, acc_count + count}}
        else
          {:cont,
           {next_state, [single_segment_project_result(result) | acc_results], acc_count + count}}
        end
      end)
      |> case do
        {:storage_error, new_sm_state, result, applied_increment} ->
          {:ok, new_sm_state, result, applied_increment}

        {new_sm_state, results, applied_increment} ->
          {:ok, new_sm_state, {:ok, Enum.reverse(results)}, applied_increment}
      end
    else
      :unsupported
    end
  end

  defp segment_projectable_batch_command?(sm_state, {:put, key, value, expire_at_ms}),
    do: segment_projectable_put?(sm_state, key, value, expire_at_ms)

  defp segment_projectable_batch_command?(_sm_state, {:delete, key}), do: is_binary(key)

  defp segment_projectable_batch_command?(
         sm_state,
         {:compound_put, compound_key, value, expire_at_ms}
       ) do
    redis_key = if is_binary(compound_key), do: CompoundKey.extract_redis_key(compound_key)

    segment_projectable_compound_put?(
      sm_state,
      redis_key,
      compound_key,
      value,
      expire_at_ms
    )
  end

  defp segment_projectable_batch_command?(sm_state, {:compound_delete, compound_key}) do
    redis_key = if is_binary(compound_key), do: CompoundKey.extract_redis_key(compound_key)

    is_binary(compound_key) and segment_shared_compound_projection_safe?(sm_state, redis_key)
  end

  defp segment_projectable_batch_command?(sm_state, {:compound_batch_put, redis_key, entries})
       when is_binary(redis_key) and is_list(entries),
       do: segment_projectable_compound_batch_put?(sm_state, redis_key, entries)

  defp segment_projectable_batch_command?(sm_state, {:compound_batch_delete, redis_key, keys})
       when is_binary(redis_key) and is_list(keys),
       do:
         segment_shared_compound_projection_safe?(sm_state, redis_key) and
           Enum.all?(keys, &compound_key_for_redis_key?(redis_key, &1))

  defp segment_projectable_batch_command?(sm_state, {:compound_delete_prefix, prefix}) do
    redis_key = if is_binary(prefix), do: CompoundKey.extract_redis_key(prefix)

    is_binary(prefix) and segment_shared_compound_projection_safe?(sm_state, redis_key)
  end

  defp segment_projectable_batch_command?(_sm_state, _command), do: false

  defp segment_projectable_put?(sm_state, key, value, expire_at_ms) do
    is_binary(key) and is_binary(value) and non_neg_integer?(expire_at_ms) and
      segment_projection_fast_key?(key) and
      not segment_blob_candidate?(sm_state, value)
  end

  # Flow policy writes are issued as raw PUTs but carry LMDB projection side
  # effects. Keep them on the full state-machine path; policy writes are cold.
  defp segment_projection_fast_key?(key), do: not FlowKeys.policy_key?(key)

  defp segment_blob_candidate?(sm_state, value) when is_binary(value) do
    threshold = BlobValue.threshold(Map.get(sm_state, :instance_ctx))
    threshold > 0 and (byte_size(value) >= threshold or BlobRef.encoded_size?(byte_size(value)))
  end

  defp segment_blob_candidate?(_sm_state, _value), do: false

  defp segment_project_batch_has_blob_candidate?(sm_state, entries) do
    Enum.any?(entries, fn
      {_key, value, _expire_at_ms} -> segment_blob_candidate?(sm_state, value)
      _entry -> false
    end)
  end

  defp apply_segment_put_batch_entries(
         [],
         sm_state,
         _shard_state,
         _threshold,
         _file_id,
         _offset,
         count
       ) do
    {:ok, sm_state, {:ok, List.duplicate(:ok, count)}, count}
  end

  defp apply_segment_put_batch_entries(
         [{key, value, expire_at_ms} | rest],
         sm_state,
         shard_state,
         threshold,
         file_id,
         offset,
         count
       )
       when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) and
              expire_at_ms >= 0 do
    entry_threshold = segment_project_hot_cache_threshold(shard_state, key, threshold)

    sm_state =
      segment_project_put_at_location(
        sm_state,
        shard_state,
        entry_threshold,
        key,
        value,
        expire_at_ms,
        file_id,
        offset
      )

    apply_segment_put_batch_entries(
      rest,
      sm_state,
      shard_state,
      threshold,
      file_id,
      offset,
      count + 1
    )
  end

  defp apply_segment_put_batch_entries(
         _invalid,
         _sm_state,
         _shard_state,
         _threshold,
         _file_id,
         _offset,
         _count
       ) do
    :unsupported
  end

  defp segment_projectable_compound_put?(
         sm_state,
         redis_key,
         compound_key,
         value,
         expire_at_ms
       ) do
    compound_key_for_redis_key?(redis_key, compound_key) and is_binary(value) and
      non_neg_integer?(expire_at_ms) and
      segment_shared_compound_projection_safe?(sm_state, redis_key, compound_key, 1)
  end

  defp segment_projectable_compound_batch_put?(sm_state, redis_key, entries) do
    case List.last(entries) do
      {compound_key, _value, _expire_at_ms} ->
        Enum.all?(entries, &segment_projectable_compound_put_shape?(redis_key, &1)) and
          segment_shared_compound_projection_safe?(sm_state, redis_key, compound_key, length(entries))

      nil ->
        true

      _entry ->
        false
    end
  end

  defp segment_projectable_compound_put_shape?(redis_key, {compound_key, value, expire_at_ms}) do
    compound_key_for_redis_key?(redis_key, compound_key) and is_binary(value) and
      non_neg_integer?(expire_at_ms)
  end

  defp segment_projectable_compound_put_shape?(_redis_key, _entry), do: false

  defp segment_projectable_prepared_compound_blob_batch?(sm_state, redis_key, prepared) do
    case List.last(prepared) do
      {:value, compound_key, _value, _expire_at_ms} ->
        segment_shared_compound_projection_safe?(sm_state, redis_key, compound_key, length(prepared))

      {:blob_ref, compound_key, _encoded_ref, _expire_at_ms} ->
        segment_shared_compound_projection_safe?(sm_state, redis_key, compound_key, length(prepared))

      nil ->
        true

      _entry ->
        false
    end
  end

  defp segment_projectable_blob_ref_put?(key, encoded_ref, expire_at_ms) do
    is_binary(key) and is_binary(encoded_ref) and non_neg_integer?(expire_at_ms)
  end

  defp segment_projectable_compound_blob_ref_put?(
         sm_state,
         redis_key,
         compound_key,
         encoded_ref,
         expire_at_ms
       ) do
    compound_key_for_redis_key?(redis_key, compound_key) and is_binary(encoded_ref) and
      non_neg_integer?(expire_at_ms) and
      segment_shared_compound_projection_safe?(sm_state, redis_key, compound_key, 1)
  end

  defp segment_shared_compound_projection_safe?(sm_state, redis_key) do
    is_binary(redis_key) and not segment_compound_promoted?(sm_state, redis_key)
  end

  defp segment_shared_compound_projection_safe?(sm_state, redis_key, compound_key, write_count) do
    segment_shared_compound_projection_safe?(sm_state, redis_key) and
      not segment_compound_promotion_candidate?(sm_state, redis_key, compound_key, write_count)
  end

  defp segment_compound_promoted?(sm_state, redis_key) do
    Map.has_key?(Map.get(sm_state, :promoted_instances, %{}), redis_key)
  end

  defp segment_compound_promotion_candidate?(sm_state, redis_key, compound_key, write_count) do
    threshold = Promotion.threshold(Map.get(sm_state, :instance_ctx))

    cond do
      threshold == 0 ->
        false

      not is_integer(write_count) or write_count <= 0 ->
        false

      true ->
        case segment_compound_prefix(redis_key, compound_key) do
          nil ->
            false

          prefix ->
            shard_state = shard_ets_state_from_sm(sm_state)
            ShardETS.prefix_count_entries(shard_state, prefix) + write_count > threshold
        end
    end
  end

  defp segment_compound_prefix(redis_key, <<"H:", _rest::binary>>),
    do: CompoundKey.hash_prefix(redis_key)

  defp segment_compound_prefix(redis_key, <<"S:", _rest::binary>>),
    do: CompoundKey.set_prefix(redis_key)

  defp segment_compound_prefix(redis_key, <<"Z:", _rest::binary>>),
    do: CompoundKey.zset_prefix(redis_key)

  defp segment_compound_prefix(_redis_key, _compound_key), do: nil

  defp compound_key_for_redis_key?(redis_key, compound_key)
       when is_binary(redis_key) and is_binary(compound_key),
       do: CompoundKey.extract_redis_key(compound_key) == redis_key

  defp compound_key_for_redis_key?(_redis_key, _compound_key), do: false

  defp non_neg_integer?(value), do: is_integer(value) and value >= 0

  defp segment_project_batch_hot_cache_threshold(entries, default_threshold) do
    if Enum.any?(entries, fn
         {key, _value, _expire_at_ms} -> segment_project_cold_flow_key?(key)
         _entry -> false
       end) do
      :per_key
    else
      {:ok, default_threshold}
    end
  end

  defp segment_project_hot_cache_threshold(shard_state, key) do
    segment_project_hot_cache_threshold(
      shard_state,
      key,
      ShardETS.hot_cache_threshold(shard_state)
    )
  end

  defp segment_project_hot_cache_threshold(_shard_state, key, default_threshold)
       when is_binary(key) do
    if segment_project_cold_flow_key?(key), do: 0, else: default_threshold
  end

  defp segment_project_hot_cache_threshold(_shard_state, _key, default_threshold),
    do: default_threshold

  defp segment_project_cold_flow_key?(key) when is_binary(key),
    do: FlowKeys.value_key?(key) or FlowKeys.history_key?(key) or FlowKeys.registry_key?(key)

  defp segment_project_cold_flow_key?(_key), do: false

  defp segment_project_check_key_lock(_sm_state, nil, _owner_ref), do: {:error, :key_locked}

  defp segment_project_check_key_lock(sm_state, key, owner_ref) when is_binary(key) do
    locks = Map.get(sm_state, :cross_shard_locks, %{})

    if map_size(locks) == 0 do
      :ok
    else
      now = CommandTime.now_ms()

      case Map.get(locks, key) do
        nil ->
          :ok

        {^owner_ref, _expires_at_ms} ->
          :ok

        {_other_owner, expires_at_ms} when is_integer(expires_at_ms) and expires_at_ms <= now ->
          :ok

        {_other_owner, _expires_at_ms} ->
          {:error, :key_locked}
      end
    end
  end

  defp segment_project_check_key_lock(_sm_state, _key, _owner_ref), do: {:error, :key_locked}

  defp with_segment_projection_command_time({:ttb, binary}, fun) when is_binary(binary) do
    try do
      binary
      |> :erlang.binary_to_term([:safe])
      |> with_segment_projection_command_time(fun)
    rescue
      _ -> fun.()
    end
  end

  defp with_segment_projection_command_time(
         {_inner_command, %{hlc_ts: {physical_ms, logical} = remote_ts}},
         fun
       )
       when is_integer(physical_ms) and is_integer(logical) do
    _ = HLC.update(remote_ts)
    CommandTime.with_now_ms(physical_ms, fun)
  rescue
    _ -> CommandTime.with_now_ms(physical_ms, fun)
  end

  defp with_segment_projection_command_time(_command, fun), do: fun.()

  defp prepare_segment_blob_batch_entries(entries) do
    entries
    |> Enum.reduce_while({:ok, [], []}, fn
      {key, value, expire_at_ms, :value}, {:ok, prepared, encoded_refs}
      when is_binary(key) and is_binary(value) ->
        if non_neg_integer?(expire_at_ms) do
          {:cont, {:ok, [{:value, key, value, expire_at_ms} | prepared], encoded_refs}}
        else
          {:halt, {:unsupported, :invalid_blob_batch_entry}}
        end

      {key, encoded_ref, expire_at_ms, :blob_ref}, {:ok, prepared, encoded_refs}
      when is_binary(key) and is_binary(encoded_ref) ->
        if non_neg_integer?(expire_at_ms) do
          {:cont,
           {:ok, [{:blob_ref, key, encoded_ref, expire_at_ms} | prepared],
            [encoded_ref | encoded_refs]}}
        else
          {:halt, {:unsupported, :invalid_blob_batch_entry}}
        end

      _entry, {:ok, _prepared, _encoded_refs} ->
        {:halt, {:unsupported, :invalid_blob_batch_entry}}
    end)
    |> case do
      {:ok, prepared, encoded_refs} -> {:ok, Enum.reverse(prepared), Enum.reverse(encoded_refs)}
      other -> other
    end
  end

  defp prepare_segment_compound_blob_batch_entries(redis_key, entries) do
    entries
    |> Enum.reduce_while({:ok, [], []}, fn
      {compound_key, value, expire_at_ms, :value}, {:ok, prepared, encoded_refs}
      when is_binary(compound_key) and is_binary(value) ->
        if compound_key_for_redis_key?(redis_key, compound_key) and non_neg_integer?(expire_at_ms) do
          {:cont, {:ok, [{:value, compound_key, value, expire_at_ms} | prepared], encoded_refs}}
        else
          {:halt, {:unsupported, :invalid_compound_blob_batch_entry}}
        end

      {compound_key, encoded_ref, expire_at_ms, :blob_ref}, {:ok, prepared, encoded_refs}
      when is_binary(compound_key) and is_binary(encoded_ref) ->
        if compound_key_for_redis_key?(redis_key, compound_key) and non_neg_integer?(expire_at_ms) do
          {:cont,
           {:ok, [{:blob_ref, compound_key, encoded_ref, expire_at_ms} | prepared],
            [encoded_ref | encoded_refs]}}
        else
          {:halt, {:unsupported, :invalid_compound_blob_batch_entry}}
        end

      _entry, {:ok, _prepared, _encoded_refs} ->
        {:halt, {:unsupported, :invalid_compound_blob_batch_entry}}
    end)
    |> case do
      {:ok, prepared, encoded_refs} -> {:ok, Enum.reverse(prepared), Enum.reverse(encoded_refs)}
      other -> other
    end
  end

  defp verify_segment_blob_refs(_sm_state, []), do: :ok

  defp verify_segment_blob_refs(sm_state, encoded_refs) do
    with {:ok, refs} <- decode_segment_blob_refs(encoded_refs),
         :ok <- BlobStore.verify_many(sm_state.data_dir, sm_state.shard_index, refs) do
      :ok
    else
      {:error, reason} -> {:error, {:blob_ref_unavailable, reason}}
    end
  end

  defp decode_segment_blob_refs(encoded_refs) do
    Enum.reduce_while(encoded_refs, {:ok, []}, fn encoded_ref, {:ok, refs} ->
      case BlobRef.decode(encoded_ref) do
        {:ok, ref} -> {:cont, {:ok, [ref | refs]}}
        :error -> {:halt, {:error, :invalid_blob_ref}}
      end
    end)
    |> case do
      {:ok, refs} -> {:ok, Enum.reverse(refs)}
      {:error, _reason} = error -> error
    end
  end

  defp segment_project_put(sm_state, key, value, expire_at_ms, position) do
    file_id = {:waraft_segment, position_index(position)}
    offset = segment_record_offset(sm_state, position)
    segment_project_put_at_location(sm_state, key, value, expire_at_ms, file_id, offset)
  end

  defp segment_project_put_at_location(sm_state, key, value, expire_at_ms, file_id, offset) do
    shard_state = shard_ets_state_from_sm(sm_state)
    threshold = segment_project_hot_cache_threshold(shard_state, key)

    segment_project_put_at_location(
      sm_state,
      shard_state,
      threshold,
      key,
      value,
      expire_at_ms,
      file_id,
      offset
    )
  end

  defp segment_project_put_at_location(
         sm_state,
         shard_state,
         threshold,
         key,
         value,
         expire_at_ms,
         file_id,
         offset
       ) do
    previous = :ets.lookup(shard_state.keydir, key)
    sm_state = segment_project_clear_compound_for_string_put(sm_state, key, previous)

    true =
      ShardETS.ets_insert_with_location(
        shard_state,
        key,
        value,
        expire_at_ms,
        file_id,
        offset,
        byte_size(value),
        previous,
        threshold
      )

    sm_state
  end

  defp segment_project_put_blob_ref(sm_state, key, encoded_ref, expire_at_ms, position) do
    shard_state = shard_ets_state_from_sm(sm_state)
    previous = :ets.lookup(shard_state.keydir, key)
    sm_state = segment_project_clear_compound_for_string_put(sm_state, key, previous)
    file_id = {:waraft_segment, position_index(position)}
    offset = segment_record_offset(sm_state, position)
    value_size = blob_ref_logical_size(encoded_ref)

    true =
      ShardETS.ets_insert_with_location(
        shard_state,
        key,
        nil,
        expire_at_ms,
        file_id,
        offset,
        value_size,
        previous
      )

    sm_state
  end

  defp blob_ref_logical_size(encoded_ref) do
    case BlobRef.decode(encoded_ref) do
      {:ok, %BlobRef{size: size}} -> size
      :error -> byte_size(encoded_ref)
    end
  end

  defp segment_project_clear_compound_for_string_put(sm_state, key, previous)
       when is_binary(key) do
    cond do
      CompoundKey.internal_key?(key) ->
        sm_state

      # Existing plain string row means this SET cannot be overwriting a compound value.
      # Reuse the lookup needed for ETS accounting and skip the marker probe.
      match?([{^key, _value, _expire_at_ms, _lfu, _fid, _offset, _value_size}], previous) ->
        sm_state

      true ->
        segment_project_clear_compound_for_string_put(sm_state, key)
    end
  end

  defp segment_project_clear_compound_for_string_put(sm_state, _key, _previous), do: sm_state

  defp segment_project_clear_compound_for_string_put(sm_state, key) when is_binary(key) do
    if CompoundKey.internal_key?(key) do
      sm_state
    else
      marker_key = CompoundKey.type_key(key)

      case segment_project_live_value(sm_state, marker_key) do
        "hash" ->
          sm_state
          |> segment_project_delete_prefix(key, CompoundKey.hash_prefix(key))
          |> segment_project_delete(marker_key)

        "list" ->
          sm_state
          |> segment_project_delete_prefix(key, CompoundKey.list_prefix(key))
          |> segment_project_delete(CompoundKey.list_meta_key(key))
          |> segment_project_delete(marker_key)

        "set" ->
          sm_state
          |> segment_project_delete_prefix(key, CompoundKey.set_prefix(key))
          |> segment_project_delete(marker_key)

        "zset" ->
          sm_state
          |> segment_project_delete_prefix(key, CompoundKey.zset_prefix(key))
          |> segment_project_delete(marker_key)

        _none_or_unknown ->
          sm_state
      end
    end
  end

  defp segment_project_clear_compound_for_string_put(sm_state, _key), do: sm_state

  defp segment_project_live_value(
         %{ets: keydir, instance_ctx: ctx, shard_index: shard_index},
         key
       )
       when is_binary(key) do
    now = HLC.now_ms()

    case :ets.lookup(keydir, key) do
      [{^key, value, expire_at_ms, _lfu, _file_id, _offset, _value_size}]
      when is_binary(value) ->
        if live_expire_at?(expire_at_ms, now), do: value, else: nil

      [
        {^key, nil, expire_at_ms, _lfu, file_id, _offset, _value_size}
      ]
      when valid_segment_backed_file_id(file_id) ->
        if live_expire_at?(expire_at_ms, now) do
          case Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
                 ctx,
                 shard_index,
                 file_id,
                 key
               ) do
            {:ok, value} when is_binary(value) -> value
            _other -> nil
          end
        else
          nil
        end

      _other ->
        nil
    end
  end

  defp segment_project_delete(sm_state, key) do
    true = ShardETS.ets_delete_key(shard_ets_state_from_sm(sm_state), key)
    sm_state
  end

  defp segment_project_delete_prefix(sm_state, redis_key, prefix) do
    sm_state.ets
    |> segment_project_prefix_keys(prefix)
    |> Enum.reduce(sm_state, fn key, acc ->
      segment_project_delete(acc, key)
    end)
    |> ZSetIndex.clear_ready_key(redis_key)
  end

  defp segment_project_prefix_keys(keydir, prefix) do
    prefix_len = byte_size(prefix)

    match_spec = [
      {{:"$1", :_, :_, :_, :_, :_, :_},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
       ], [:"$1"]}
    ]

    :ets.select(keydir, match_spec)
  end

  defp segment_project_zset_put(sm_state, redis_key, compound_key, value),
    do: ZSetIndex.apply_put(sm_state, redis_key, compound_key, value)

  defp segment_project_zset_delete(sm_state, redis_key, compound_key),
    do: ZSetIndex.apply_delete(sm_state, redis_key, compound_key)

  defp apply_segment_projection_entries(sm_state, _position, []), do: sm_state

  defp apply_segment_projection_entries(sm_state, projection_root, entries)
       when is_binary(projection_root) do
    now = HLC.now_ms()

    entries
    |> Enum.with_index(1)
    |> Enum.reduce(sm_state, fn {{key, value, expire_at_ms}, projection_index}, acc ->
      if live_expire_at?(expire_at_ms, now) do
        segment_project_recovered_projection_entry(
          acc,
          projection_root,
          projection_index,
          key,
          value,
          expire_at_ms
        )
      else
        acc
      end
    end)
  end

  defp apply_segment_projection_entries(sm_state, position, entries) do
    now = HLC.now_ms()

    Enum.reduce(entries, sm_state, fn {key, value, expire_at_ms}, acc ->
      if live_expire_at?(expire_at_ms, now) do
        segment_project_recovered_entry(acc, key, value, expire_at_ms, position)
      else
        acc
      end
    end)
  end

  defp segment_project_recovered_projection_entry(
         sm_state,
         projection_root,
         projection_index,
         key,
         value,
         expire_at_ms
       ) do
    offset = projection_record_offset(projection_root, projection_index)
    sm_state = segment_project_clear_compound_for_string_put(sm_state, key)
    shard_state = shard_ets_state_from_sm(sm_state)
    threshold = segment_project_hot_cache_threshold(shard_state, key)
    previous = :ets.lookup(shard_state.keydir, key)

    if segment_blob_ref_value?(value) do
      true =
        ShardETS.ets_insert_with_location(
          shard_state,
          key,
          nil,
          expire_at_ms,
          {:waraft_projection, projection_index},
          offset,
          segment_projected_value_size(value),
          previous
        )
    else
      true =
        ShardETS.ets_insert_with_location(
          shard_state,
          key,
          value,
          expire_at_ms,
          {:waraft_projection, projection_index},
          offset,
          byte_size(value),
          previous,
          threshold
        )
    end

    sm_state
  end

  defp segment_project_recovered_entry(sm_state, key, value, expire_at_ms, position) do
    if segment_blob_ref_value?(value) do
      segment_project_put_blob_ref(sm_state, key, value, expire_at_ms, position)
    else
      segment_project_put(sm_state, key, value, expire_at_ms, position)
    end
  end

  defp segment_blob_ref_value?(value) when is_binary(value) do
    BlobRef.encoded_size?(byte_size(value)) and BlobRef.ref?(value)
  end

  defp segment_blob_ref_value?(_value), do: false

  defp segment_projected_value_size(value) when is_binary(value) do
    if segment_blob_ref_value?(value), do: blob_ref_logical_size(value), else: byte_size(value)
  end

  defp segment_projected_value_size(_value), do: 0

  defp single_segment_project_result({:ok, [result]}), do: result
  defp single_segment_project_result(result), do: result

  defp bump_segment_projected_applied_count(sm_state, 0), do: sm_state

  defp bump_segment_projected_applied_count(sm_state, count)
       when is_integer(count) and count > 0 do
    Map.update(sm_state, :applied_count, count, &(&1 + count))
  end

  defp shard_ets_state_from_sm(sm_state) do
    %{
      keydir: sm_state.ets,
      index: sm_state.shard_index,
      instance_ctx: sm_state.instance_ctx
    }
  end

  defp position_index({:raft_log_pos, index, _term}) when is_integer(index), do: index
  defp position_index(_position), do: 0

  defp segment_record_offset(
         %{data_dir: data_dir, shard_index: shard_index},
         {:raft_log_pos, index, _term}
       )
       when is_integer(index) and index > 0 do
    root = Path.join([data_dir, "waraft", "#{@storage_root}.#{shard_index + 1}"])

    case :ferricstore_waraft_spike_segment_log.location_for_index(to_charlist(root), index) do
      {:ok, {_ordinal, offset, _encoded_size}} when is_integer(offset) and offset >= 0 -> offset
      _missing_or_error -> 0
    end
  end

  defp segment_record_offset(_sm_state, _position), do: 0

  defp projection_record_offset(projection_root, projection_index)
       when is_binary(projection_root) and is_integer(projection_index) and projection_index > 0 do
    case projection_record_location(projection_root, projection_index) do
      {:ok, offset} -> offset
      {:error, _reason} -> 0
    end
  end

  defp projection_record_offset(_projection_root, _projection_index), do: 0

  defp projection_record_location(projection_root, projection_index)
       when is_binary(projection_root) and is_integer(projection_index) and projection_index > 0 do
    case :ferricstore_waraft_spike_segment_log.location_for_index(
           to_charlist(projection_root),
           projection_index
         ) do
      {:ok, {_ordinal, offset, _encoded_size}} when is_integer(offset) and offset >= 0 ->
        {:ok, offset}

      :not_found ->
        {:error, {:missing_segment_projection_offset, projection_index}}

      {:error, reason} ->
        {:error, {:segment_projection_offset_failed, projection_index, reason}}
    end
  end

  defp projection_record_location(_projection_root, projection_index),
    do: {:error, {:bad_segment_projection_index, projection_index}}

  defp apply_state_machine_command(command, position, sm_state) do
    meta = meta_from_position(position)

    StateMachine.apply_waraft_segment_command(command, meta, sm_state, fn batch ->
      write_apply_projection_batch(sm_state, position, batch)
    end)
  end

  defp write_apply_projection_batch(sm_state, position, batch) do
    index = position_index(position)

    if index > 0 do
      file_id = {:waraft_apply_projection, index}

      :ok =
        WARaftSegmentReader.put_apply_projection(
          sm_state.instance_ctx.data_dir,
          sm_state.shard_index,
          index,
          apply_projection_entries(batch)
        )

      :ok =
        Ferricstore.FaultInjection.maybe_pause(:after_waraft_apply_projection_write, %{
          shard_index: sm_state.shard_index,
          index: index,
          entry_count: length(batch)
        })

      {:ok, file_id, apply_projection_locations(batch, 0)}
    else
      {:error, {:bad_waraft_projection_position, position}}
    end
  end

  defp consume_apply_projection_replay_dependencies do
    dependencies =
      Process.get(@apply_projection_replay_dependencies_key, %{})
      |> normalize_replay_dependency_map()

    clear_apply_projection_replay_dependencies()
    dependencies
  end

  defp clear_apply_projection_replay_dependencies do
    Process.delete(@apply_projection_replay_dependencies_key)
    :ok
  end

  defp merge_apply_projection_replay_dependencies(dependencies, apply_projection)
       when is_map(dependencies) do
    apply_projection = normalize_replay_dependency_map(apply_projection)

    if map_size(apply_projection) == 0 do
      dependencies
    else
      Map.update(dependencies, :apply_projection, apply_projection, fn existing ->
        merge_replay_dependency_maps(existing, apply_projection)
      end)
    end
  end

  defp merge_apply_projection_replay_dependencies(dependencies, _apply_projection),
    do: dependencies

  defp spill_apply_projection_replay_dependencies(%{ctx: %{data_dir: data_dir}} = handle) do
    handle
    |> Map.get(:replay_dependencies, replay_dependency_defaults())
    |> Map.get(:apply_projection, %{})
    |> normalize_replay_dependency_map()
    |> Enum.each(fn {shard_index, index} ->
      unless WARaftSegmentReader.apply_projection_dependency_ready?(data_dir, shard_index, index) do
        _ = WARaftSegmentReader.spill_apply_projection_cache(data_dir, shard_index)
      end
    end)

    :ok
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp spill_apply_projection_replay_dependencies(_handle), do: :ok

  defp apply_projection_replay_dependencies_ready?(%{ctx: %{data_dir: data_dir}} = handle) do
    handle
    |> Map.get(:replay_dependencies, replay_dependency_defaults())
    |> Map.get(:apply_projection, %{})
    |> normalize_replay_dependency_map()
    |> Enum.all?(fn {shard_index, index} ->
      WARaftSegmentReader.apply_projection_dependency_ready?(data_dir, shard_index, index)
    end)
  end

  defp apply_projection_replay_dependencies_ready?(_handle), do: true

  defp history_replay_dependencies_ready?(handle) do
    handle
    |> Map.get(:replay_dependencies, replay_dependency_defaults())
    |> Map.get(:history, %{})
    |> normalize_replay_dependency_map()
    |> Enum.all?(fn {shard_index, index} ->
      HistoryProjector.durable?(
        Map.get(handle, :ctx),
        shard_index,
        replay_dependency_shard_data_path(handle, shard_index),
        index
      )
    end)
  end

  defp request_history_replay_dependencies(handle) do
    handle
    |> Map.get(:replay_dependencies, replay_dependency_defaults())
    |> Map.get(:history, %{})
    |> normalize_replay_dependency_map()
    |> Enum.each(fn {shard_index, index} ->
      HistoryProjector.request(
        Map.get(handle, :ctx),
        shard_index,
        replay_dependency_shard_data_path(handle, shard_index),
        index
      )
    end)

    :ok
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp flush_history_replay_dependencies(handle, timeout_ms) do
    handle
    |> Map.get(:replay_dependencies, replay_dependency_defaults())
    |> Map.get(:history, %{})
    |> normalize_replay_dependency_map()
    |> Enum.each(fn {shard_index, _index} ->
      _ =
        HistoryProjector.flush(
          Map.get(handle, :ctx),
          shard_index,
          timeout_ms
        )
    end)

    :ok
  end

  defp replay_dependency_defaults, do: %{history: %{}, apply_projection: %{}}

  defp replay_dependency_defaults(kind, dependencies)
       when kind in [:history, :apply_projection] do
    replay_dependency_defaults()
    |> Map.put(kind, dependencies)
  end

  defp replay_dependency_defaults(_kind, _dependencies), do: replay_dependency_defaults()

  defp apply_projection_locations(batch, offset) do
    Enum.map(batch, fn
      {:put, _key, value, _expire_at_ms} -> {:put, offset, byte_size(value)}
      {:put_cold, _key, value, _expire_at_ms, _lfu} -> {:put, offset, byte_size(value)}
      {:delete, key, _prob_path} -> {:delete, offset, byte_size(key)}
    end)
  end

  defp apply_projection_entries(batch) do
    Enum.flat_map(batch, fn
      {:put_cold, key, value, expire_at_ms, _lfu}
      when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) ->
        [{key, value, expire_at_ms}]

      _delete_or_invalid ->
        []
    end)
  end

  defp maybe_mark_bitcask_dirty(handle), do: handle

  defp maybe_update_label(handle, :keep_label), do: handle
  defp maybe_update_label(handle, {:replace_label, label}), do: %{handle | label: label}

  defp maybe_put_status(status, _key, nil), do: status
  defp maybe_put_status(status, key, value), do: [{key, value} | status]

  defp durable_position(%{bitcask_dirty?: true} = handle) do
    last_clean_position(handle)
  end

  defp durable_position(handle) do
    if replay_dependencies_ready?(handle) do
      Map.get(handle, :position)
    else
      last_clean_position(handle)
    end
  end

  defp last_clean_position(handle) do
    Map.get(
      handle,
      :last_clean_position,
      Map.get(handle, :persisted_position, Map.get(handle, :position))
    )
  end

  defp merge_replay_dependencies(handle, dependencies) when is_map(dependencies) do
    Enum.reduce([:history, :apply_projection], handle, fn kind, acc ->
      dependency_map =
        dependencies
        |> Map.get(kind, %{})
        |> normalize_replay_dependency_map()

      if map_size(dependency_map) == 0 do
        acc
      else
        Map.update(acc, :replay_dependencies, replay_dependency_defaults(kind, dependency_map), fn
          existing ->
            existing = existing || replay_dependency_defaults()

            Map.update(existing, kind, dependency_map, fn existing_map ->
              merge_replay_dependency_maps(existing_map, dependency_map)
            end)
        end)
      end
    end)
  end

  defp merge_replay_dependencies(handle, _dependencies), do: handle

  defp clear_replay_dependencies(handle),
    do: Map.put(handle, :replay_dependencies, replay_dependency_defaults())

  defp replay_dependencies_ready?(handle) do
    history_replay_dependencies_ready?(handle) and
      apply_projection_replay_dependencies_ready?(handle)
  end

  defp request_replay_dependencies(handle) do
    request_history_replay_dependencies(handle)
    spill_apply_projection_replay_dependencies(handle)
    :ok
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp request_replay_dependencies_async(handle) do
    request_history_replay_dependencies(handle)
    maybe_start_apply_projection_replay_spill(handle)
  rescue
    _ -> handle
  catch
    _, _ -> handle
  end

  defp maybe_start_apply_projection_replay_spill(
         %{apply_projection_cache_compaction: _} = handle
       ),
       do: handle

  defp maybe_start_apply_projection_replay_spill(%{ctx: %{data_dir: data_dir}} = handle) do
    dependencies =
      handle
      |> Map.get(:replay_dependencies, replay_dependency_defaults())
      |> Map.get(:apply_projection, %{})
      |> normalize_replay_dependency_map()

    cond do
      map_size(dependencies) == 0 ->
        handle

      apply_projection_replay_dependencies_ready?(handle) ->
        handle

      true ->
        count =
          WARaftSegmentReader.apply_projection_cache_count(
            data_dir,
            handle.shard_index
          )

        {:ok, requested_handle} = start_apply_projection_cache_compaction(handle, count, 0)
        requested_handle
    end
  end

  defp maybe_start_apply_projection_replay_spill(handle), do: handle

  defp flush_replay_dependencies_before_close(handle) do
    if replay_dependencies_ready?(handle) do
      handle
    else
      timeout_ms = replay_dependency_close_flush_timeout_ms()

      flush_history_replay_dependencies(handle, timeout_ms)
      request_replay_dependencies(handle)
      wait_replay_dependencies_ready(handle, timeout_ms)
    end
  rescue
    _ -> handle
  catch
    _, _ -> handle
  end

  defp wait_replay_dependencies_ready(handle, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + max(timeout_ms, 0)
    do_wait_replay_dependencies_ready(handle, deadline)
  end

  defp do_wait_replay_dependencies_ready(handle, deadline) do
    cond do
      replay_dependencies_ready?(handle) ->
        handle

      System.monotonic_time(:millisecond) >= deadline ->
        handle

      true ->
        Process.sleep(10)
        do_wait_replay_dependencies_ready(handle, deadline)
    end
  end

  defp replay_dependency_close_flush_timeout_ms do
    Application.get_env(:ferricstore, :waraft_replay_dependency_close_flush_timeout_ms, 10_000)
  end

  defp replay_dependency_shard_data_path(%{ctx: %{data_dir: data_dir}}, shard_index)
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
  end

  defp replay_dependency_shard_data_path(%{sm_state: %{shard_data_path: path}}, _shard_index)
       when is_binary(path),
       do: path

  defp replay_dependency_shard_data_path(_handle, shard_index),
    do: Path.join(["data", "shard_#{shard_index}"])

  defp merge_replay_dependency_maps(left, right) do
    right
    |> normalize_replay_dependency_map()
    |> Enum.reduce(normalize_replay_dependency_map(left), fn {shard_index, index}, acc ->
      Map.update(acc, shard_index, index, &max(&1, index))
    end)
  end

  defp normalize_replay_dependency_map(dependencies) when is_map(dependencies) do
    dependencies
    |> Enum.reduce(%{}, fn
      {shard_index, index}, acc
      when is_integer(shard_index) and shard_index >= 0 and is_integer(index) and index > 0 ->
        Map.update(acc, shard_index, index, &max(&1, index))

      _other, acc ->
        acc
    end)
  end

  defp normalize_replay_dependency_map(_dependencies), do: %{}

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
      {result, block_storage(old_handle, storage_block_reason(result), position, :apply_failure)}
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

  defp decoded_replay_command({:ttb, binary}) when is_binary(binary) do
    try do
      binary
      |> :erlang.binary_to_term([:safe])
      |> decoded_replay_command()
    rescue
      _ -> {:ttb, binary}
    end
  end

  defp decoded_replay_command({inner_command, %{hlc_ts: {physical_ms, logical}}})
       when is_tuple(inner_command) and is_integer(physical_ms) and is_integer(logical) do
    decoded_replay_command(inner_command)
  end

  defp decoded_replay_command(command), do: command

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

  defp storage_apply_failure?({:error, reason}), do: storage_apply_failure_reason?(reason)
  defp storage_apply_failure?(_result), do: false

  defp storage_apply_failure_reason?(:active_file_unavailable), do: true
  defp storage_apply_failure_reason?({:bitcask_append_failed, _reason}), do: true
  defp storage_apply_failure_reason?({:bitcask_append_result_mismatch, _reason}), do: true
  defp storage_apply_failure_reason?({:bitcask_writer_flush_failed, _reason}), do: true
  defp storage_apply_failure_reason?({:blob_externalize_failed, _reason}), do: true
  defp storage_apply_failure_reason?({:blob_ref_unavailable, _reason}), do: true
  defp storage_apply_failure_reason?({:cross_shard_compensation_failed, _reason}), do: true
  defp storage_apply_failure_reason?({:flow_history_projection_failed, _reason}), do: true
  defp storage_apply_failure_reason?({:batch_result_mismatch, _expected, _actual}), do: true

  defp storage_apply_failure_reason?({:tombstone_batch_result_mismatch, _expected, _actual}),
    do: true

  defp storage_apply_failure_reason?({:fsync_dir_failed, _phase, _reason}), do: true
  defp storage_apply_failure_reason?({:delete_prob_file_failed, _reason}), do: true
  defp storage_apply_failure_reason?(_reason), do: false

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
                {result, maybe_start_segment_projection_checkpoint(persisted_handle)}

              :skipped ->
                {result,
                 projected_handle
                 |> maybe_mark_clean_position()
                 |> maybe_start_segment_projection_checkpoint()}

              {:error, reason} ->
                {{:error, reason}, block_storage(old_handle, reason, position, :metadata_failure)}
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

  defp maybe_compact_apply_projection_cache(handle) do
    limit = apply_projection_cache_max_entries()

    cond do
      Map.has_key?(handle, :apply_projection_cache_compaction) ->
        {:ok, handle}

      limit == :infinity ->
        {:ok, handle}

      not (is_integer(limit) and limit >= 0) ->
        {:ok, handle}

      true ->
        count =
          Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(
            handle.ctx.data_dir,
            handle.shard_index
          )

        if count > limit do
          start_apply_projection_cache_compaction(handle, count, limit)
        else
          {:ok, handle}
        end
    end
  end

  defp start_apply_projection_cache_compaction(
         %{position: position} = handle,
         count,
         limit
       ) do
    index = position_index(position)
    spill_count = apply_projection_cache_spill_count(count, limit)

    cond do
      index <= 0 ->
        {:ok, handle}

      spill_count <= 0 ->
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
          limit: limit,
          spill_count: spill_count
        }

        case Task.start(fn ->
               result =
                 run_apply_projection_cache_compaction(
                   handle.ctx.data_dir,
                   handle.shard_index,
                   spill_count,
                   metadata
                 )

               send_storage_info(
                 storage_name,
                 {:ferricstore_waraft_apply_projection_cache_compact_done, ref,
                  {started_at, metadata, result}}
               )
             end) do
          {:ok, pid} ->
            {:ok,
             Map.put(handle, :apply_projection_cache_compaction, %{
               ref: ref,
               pid: pid,
               started_at: started_at,
               count: count,
               limit: limit,
               spill_count: spill_count
             })}

          {:error, reason} ->
            emit_apply_projection_cache_compaction(
              metadata,
              started_at,
              {:error, {:task_start_failed, reason}}
            )

            {:ok,
             Map.put(handle, :apply_projection_cache_last_error, {:task_start_failed, reason})}
        end
    end
  end

  defp apply_projection_cache_spill_count(count, limit)
       when is_integer(count) and is_integer(limit) and count > 0 and limit <= 0,
       do: count

  defp apply_projection_cache_spill_count(count, limit)
       when is_integer(count) and is_integer(limit) and count > 0 do
    target_count = div(limit, 2)
    max(count - target_count, 1)
  end

  defp apply_projection_cache_spill_count(_count, _limit), do: 0

  defp apply_projection_cache_max_entries do
    Ferricstore.MemoryBudget.limit(:waraft_apply_projection_cache_max_entries, 16_384)
  end

  defp run_apply_projection_cache_compaction(data_dir, shard_index, spill_count, metadata) do
    call_apply_projection_cache_compact_hook(:before_spill, metadata)

    Ferricstore.Raft.WARaftSegmentReader.spill_apply_projection_cache(
      data_dir,
      shard_index,
      spill_count
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
      |> Map.delete(:apply_projection_cache_compaction)
      |> update_apply_projection_cache_compaction_error(result)

    {:ok, handle}
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
    if replay_dependencies_ready?(handle) do
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

  defp segment_projection_checkpoint_due?(handle) do
    position_gap_due?(
      segment_projection_checkpoint_every(),
      Map.get(handle, :position),
      Map.get(handle, :segment_projection_position, @zero_pos)
    ) and segment_projection_checkpoint_interval_due?(handle)
  end

  defp segment_projection_checkpoint_interval_due?(handle) do
    interval_ms = segment_projection_checkpoint_min_interval_ms()
    last_ms = Map.get(handle, :segment_projection_checkpoint_started_at_ms, 0)

    cond do
      interval_ms <= 0 ->
        true

      last_ms <= 0 ->
        true

      System.monotonic_time(:millisecond) - last_ms >= interval_ms ->
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

      {:ok, pid} =
        Task.start(fn ->
          {metadata, result} =
            run_segment_projection_checkpoint(
              handle.root_dir,
              handle.ctx,
              handle.shard_index,
              position,
              keydir
            )

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
        started_at: started_at
      })
      |> Map.put(:segment_projection_checkpoint_started_at_ms, started_at_ms)
    end
  rescue
    _ -> handle
  end

  defp start_segment_projection_checkpoint(handle), do: handle

  defp finish_segment_projection_checkpoint(
         ref,
         {started_at, metadata, result},
         %{segment_projection_checkpoint: %{ref: ref, position: position}} = handle
       ) do
    duration_us =
      System.convert_time_unit(System.monotonic_time() - started_at, :native, :microsecond)

    handle =
      handle
      |> Map.delete(:segment_projection_checkpoint)
      |> finish_segment_projection_checkpoint_result(position, result)

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

  defp finish_segment_projection_checkpoint_result(handle, position, {:ok, :stale}) do
    finish_segment_projection_checkpoint_result(handle, position, :ok)
  end

  defp finish_segment_projection_checkpoint_result(handle, _position, {:error, _reason}),
    do: handle

  defp finish_segment_projection_checkpoint_result(handle, _position, _other), do: handle

  defp run_segment_projection_checkpoint(root_dir, ctx, shard_index, position, keydir) do
    with_segment_projection_lock(root_dir, fn ->
      now = HLC.now_ms()

      case segment_projection_entries_from_keydir(keydir, ctx, shard_index, now) do
        :unavailable ->
          metadata = %{shard_index: shard_index, position: position, entries: 0}
          {metadata, {:error, {:segment_keydir_unavailable, shard_index}}}

        {:ok, {entries, entry_count}} ->
          metadata = %{shard_index: shard_index, position: position, entries: entry_count}
          emit_segment_projection_checkpoint_start(metadata)
          call_segment_projection_checkpoint_hook(:before_write, metadata)

          result =
            with :ok <- write_segment_projection_checkpoint_unlocked(root_dir, position, entries) do
              :ok
            end

          {metadata, result}

        {:error, reason} ->
          metadata = %{shard_index: shard_index, position: position, entries: 0}
          {metadata, {:error, reason}}
      end
    end)
    |> case do
      {%{} = metadata, result} ->
        {metadata, result}

      {:error, reason} ->
        {%{shard_index: shard_index, position: position, entries: 0},
         {:error, {:segment_projection_checkpoint_failed, reason}}}

      other ->
        {%{shard_index: shard_index, position: position, entries: 0},
         {:error, {:segment_projection_checkpoint_failed, other}}}
    end
  rescue
    error ->
      {%{shard_index: shard_index, position: position, entries: 0},
       {:error, {:segment_projection_checkpoint_failed, error}}}
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

  defp write_segment_projection_checkpoint_unlocked(root_dir, position, entries) do
    checkpoint_root = segment_projection_checkpoint_root(root_dir)

    result =
      case read_segment_projection_log(checkpoint_root) do
        {:ok, %{position: existing_position}} ->
          if position_index(existing_position) >= position_index(position) do
            {:ok, :stale}
          else
            write_segment_projection(checkpoint_root, position, entries)
          end

        {:ok, _projection} ->
          write_segment_projection(checkpoint_root, position, entries)

        {:error, :enoent} ->
          write_segment_projection(checkpoint_root, position, entries)

        {:error, reason} ->
          {:error, {:read_existing_segment_projection_checkpoint, reason}}
      end

    case result do
      :ok -> :ok
      {:ok, :stale} -> {:ok, :stale}
      {:error, _reason} = error -> error
      other -> {:error, {:write_segment_projection_checkpoint, other}}
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
  defp segment_projection_checkpoint_result({:error, _reason}), do: :error
  defp segment_projection_checkpoint_result(_other), do: :error

  defp segment_projection_checkpoint_reason({:error, reason}), do: reason

  defp segment_projection_checkpoint_reason(other) when other not in [:ok, {:ok, :stale}],
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
               shard_index: Map.fetch!(handle, :shard_index)
             }}
          )

      _context ->
        :ok
    end

    true =
      :ets.insert(
        @segment_projection_registry,
        {{key, :position}, Map.fetch!(handle, :position)}
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
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  defp unregister_segment_projection_context(_handle), do: :ok

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
          {[{_context_key, context}], [{_position_key, position}]} ->
            {:ok, Map.put(context, :position, position)}

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

  defp command_from_segment_log_entry({_term, command}) when is_tuple(command), do: {:ok, command}
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

  defp recovery_storage_metadata_candidate(path, candidates) do
    journal_path = metadata_journal_path(path)

    case Enum.find(candidates, fn
           {^journal_path, metadata} -> snapshot_boundary_metadata?(metadata)
           _candidate -> false
         end) do
      nil -> newest_storage_metadata_candidate(candidates)
      boundary_candidate -> boundary_candidate
    end
  end

  defp read_recovery_metadata_candidates(path) do
    previous_path = metadata_previous_path(path)
    journal_path = metadata_journal_path(path)

    [
      {previous_path, read_previous_storage_metadata(path), :previous},
      {journal_path, read_latest_storage_metadata_journal(path), :journal}
    ]
    |> Enum.reduce({[], %{}}, fn
      {source_path, {:ok, metadata}, source_key}, {candidates, errors} ->
        {[{source_path, metadata} | candidates], Map.delete(errors, source_key)}

      {_source_path, {:error, reason}, source_key}, {candidates, errors} ->
        {candidates, Map.put(errors, source_key, reason)}
    end)
  end

  defp newest_storage_metadata_candidate(candidates) do
    Enum.max_by(candidates, fn {_source_path, metadata} ->
      storage_metadata_position_key(metadata)
    end)
  end

  defp storage_metadata_position_key(%{position: {:raft_log_pos, index, term}})
       when is_integer(index) and is_integer(term),
       do: {index, term}

  defp read_metadata_if_present(path) do
    case read_storage_metadata_file(path, :storage_metadata_file_too_large) do
      {:ok, binary} ->
        case persisted_binary_to_term(binary) do
          {:ok, %{version: @version} = metadata} ->
            case validate_storage_metadata(metadata) do
              {:ok, validated} -> validated
              {:error, reason} -> {:error, reason}
            end

          {:ok, other} ->
            {:error, {:bad_storage_metadata, other}}

          {:error, reason} ->
            {:error, {:decode_storage_metadata, reason}}
        end

      {:error, :enoent} ->
        %{}

      {:error, reason} ->
        {:error, {:read_storage_metadata, reason}}
    end
  end

  defp read_storage_metadata_file(path, too_large_reason) do
    read_bounded_metadata_file(path, @max_storage_metadata_bytes, too_large_reason)
  end

  defp validate_storage_metadata(%{position: position, config: config} = metadata)
       when is_map(metadata) do
    with :ok <- validate_raft_position(position),
         :ok <- validate_storage_config(config),
         :ok <- validate_storage_snapshot_boundary(metadata) do
      {:ok, metadata}
    else
      {:error, reason} -> {:error, {:bad_storage_metadata, reason}}
    end
  end

  defp validate_storage_metadata(%{position: position} = metadata) when is_map(metadata) do
    with :ok <- validate_raft_position(position),
         :ok <- validate_storage_snapshot_boundary(metadata) do
      {:ok, metadata}
    else
      {:error, reason} -> {:error, {:bad_storage_metadata, reason}}
    end
  end

  defp validate_storage_metadata(metadata) when is_map(metadata),
    do: {:error, {:bad_storage_metadata, :missing_position}}

  defp validate_storage_metadata(other), do: {:error, {:bad_storage_metadata, other}}

  defp validate_storage_config(nil), do: :ok

  defp validate_storage_config({position, config}) when is_map(config) do
    validate_raft_position(position)
  end

  defp validate_storage_config(other), do: {:error, {:bad_config, other}}

  defp validate_storage_snapshot_boundary(%{snapshot_boundary_position: position}) do
    validate_raft_position(position)
  end

  defp validate_storage_snapshot_boundary(_metadata), do: :ok

  defp validate_raft_position({:raft_log_pos, index, term})
       when is_integer(index) and index >= 0 and is_integer(term) and term >= 0,
       do: :ok

  defp validate_raft_position(other), do: {:error, {:bad_position, other}}

  defp persisted_binary_to_term(binary) do
    term =
      binary
      |> :erlang.binary_to_term([:safe])
      |> decode_persisted_metadata_term()

    {:ok, term}
  rescue
    error -> {:error, error}
  end

  defp encode_persisted_metadata_term(%{} = metadata) do
    Map.update(metadata, :config, nil, &encode_persisted_storage_config/1)
  end

  defp encode_persisted_metadata_term(other), do: other

  defp decode_persisted_metadata_term(%{} = metadata) do
    Map.update(metadata, :config, nil, &decode_persisted_storage_config/1)
  end

  defp decode_persisted_metadata_term(other), do: other

  defp encode_persisted_storage_config({position, config}) when is_map(config),
    do: {position, encode_persisted_waraft_config(config)}

  defp encode_persisted_storage_config(other), do: other

  defp decode_persisted_storage_config({position, config}) when is_map(config),
    do: {position, decode_persisted_waraft_config(config)}

  defp decode_persisted_storage_config(other), do: other

  defp encode_persisted_waraft_config(config) do
    Enum.reduce([:membership, :participants, :witness, :witnesses], config, fn key, acc ->
      if Map.has_key?(acc, key) do
        Map.update!(acc, key, &encode_persisted_waraft_peers/1)
      else
        acc
      end
    end)
  end

  defp decode_persisted_waraft_config(config) do
    Enum.reduce([:membership, :participants, :witness, :witnesses], config, fn key, acc ->
      if Map.has_key?(acc, key) do
        Map.update!(acc, key, &decode_persisted_waraft_peers/1)
      else
        acc
      end
    end)
  end

  defp encode_persisted_waraft_peers(peers) when is_list(peers),
    do: Enum.map(peers, &encode_persisted_waraft_peer/1)

  defp encode_persisted_waraft_peers(other), do: other

  defp decode_persisted_waraft_peers(peers) when is_list(peers),
    do: Enum.map(peers, &decode_persisted_waraft_peer/1)

  defp decode_persisted_waraft_peers(other), do: other

  defp encode_persisted_waraft_peer({server, node_name})
       when is_atom(server) and is_atom(node_name) do
    {@encoded_peer_tag, Atom.to_string(server), Atom.to_string(node_name)}
  end

  defp encode_persisted_waraft_peer(other), do: other

  defp decode_persisted_waraft_peer({@encoded_peer_tag, server, node_name})
       when is_binary(server) and is_binary(node_name) do
    {String.to_atom(server), String.to_atom(node_name)}
  end

  defp decode_persisted_waraft_peer(other), do: other

  defp metadata_path(root_dir), do: Path.join(root_dir, @metadata_file)

  defp metadata_previous_path(path), do: path <> @metadata_previous_suffix

  defp metadata_journal_path(path), do: path <> @metadata_journal_suffix

  defp segment_projection_root(root_dir), do: Path.join(root_dir, @segment_projection_dir)

  defp segment_projection_checkpoint_root(root_dir),
    do: Path.join(root_dir, @segment_projection_checkpoint_dir)

  defp segment_projection_files_present?(root_dir) do
    Ferricstore.FS.exists?(segment_projection_root(root_dir)) or
      Ferricstore.FS.exists?(segment_projection_checkpoint_root(root_dir))
  end

  defp apply_projection_root(root_dir), do: Path.join(root_dir, @apply_projection_dir)

  defp ensure_apply_projection_segment_log_ready!(root_dir) do
    case ensure_apply_projection_segment_log_ready(root_dir) do
      :ok ->
        :ok

      {:error, reason} ->
        raise "failed to initialize WARaft apply-projection segment log: #{inspect(reason)}"
    end
  end

  defp ensure_apply_projection_segment_log_ready(root_dir) do
    case :ferricstore_waraft_spike_segment_log.ensure_segment_config(
           root_dir
           |> apply_projection_root()
           |> to_charlist()
         ) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error, reason}

      other ->
        {:error, other}
    end
  end

  defp maybe_write_snapshot_segment_projection(snapshot_path, handle) do
    with {:ok, entries} <- collect_segment_projected_entries_strict(handle.sm_state) do
      case entries do
        [] ->
          {:ok, nil}

        _ ->
          projection_root = Path.join(snapshot_path, @segment_projection_dir)

          case write_segment_projection(projection_root, handle.position, entries) do
            :ok ->
              {:ok,
               %{
                 dir: @segment_projection_dir,
                 format: :segment_log,
                 count: length(entries)
               }}

            {:error, reason} ->
              {:error, {:write_segment_projection_snapshot, reason}}
          end
      end
    else
      {:error, reason} -> {:error, {:collect_segment_projection_snapshot, reason}}
    end
  end

  defp write_segment_projection(projection_root, position, entries) do
    case :ferricstore_waraft_spike_segment_log.write_projection(
           to_charlist(projection_root),
           position,
           entries
         ) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
      other -> {:error, {:write_segment_projection_log, other}}
    end
  end

  defp collect_segment_projected_entries_strict(%{
         ets: keydir,
         instance_ctx: ctx,
         shard_index: shard_index
       }) do
    now = HLC.now_ms()

    case segment_projection_entries_from_keydir(keydir, ctx, shard_index, now) do
      :unavailable ->
        {:error, {:segment_keydir_unavailable, shard_index}}

      {:ok, {entries, _count}} ->
        {:ok, entries}

      {:error, _reason} = error ->
        error
    end
  rescue
    error -> {:error, {:collect_segment_projection_entries_failed, error}}
  end

  defp collect_segment_projected_entries_strict(_sm_state),
    do: {:error, :bad_segment_projection_state}

  defp collect_segment_projection_relocations(ctx, shard_index) do
    keydir = elem(ctx.keydir_refs, shard_index)

    collect_segment_projection_relocations(%{
      ets: keydir,
      instance_ctx: ctx,
      shard_index: shard_index
    })
  end

  defp collect_segment_projection_relocations(%{
         ets: keydir,
         instance_ctx: ctx,
         shard_index: shard_index
       }) do
    now = HLC.now_ms()

    case segment_projection_relocations_from_keydir(keydir, ctx, shard_index, now) do
      :unavailable ->
        {:error, {:segment_keydir_unavailable, shard_index}}

      {:ok, relocations} ->
        {:ok, relocations}

      {:error, _reason} = error ->
        error
    end
  rescue
    error -> {:error, {:collect_segment_projection_relocations_failed, error}}
  end

  defp collect_segment_projection_relocations(_sm_state),
    do: {:error, :bad_segment_projection_state}

  defp segment_projection_relocations_from_keydir(keydir, ctx, shard_index, now) do
    keydir
    |> reduce_keydir_rows_while([], fn row, acc ->
      case segment_projection_entry_from_keydir_row(row, ctx, shard_index, now) do
        {:ok, entry} -> {:cont, [{entry, row} | acc]}
        :skip -> {:cont, acc}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, relocations} ->
        {:ok, Enum.sort_by(relocations, fn {{key, _value, _expire_at_ms}, _row} -> key end)}

      {:error, _reason} = error ->
        error

      :unavailable ->
        :unavailable
    end
  end

  defp segment_projection_entries_from_relocations(relocations) do
    Enum.map(relocations, fn {entry, _row} -> entry end)
  end

  defp segment_projection_checkpoint_relocations(ctx, shard_index, entries, trim_index) do
    keydir = elem(ctx.keydir_refs, shard_index)
    now = HLC.now_ms()

    entry_by_key =
      entries
      |> Enum.with_index(1)
      |> Map.new(fn {{key, value, expire_at_ms}, projection_index} ->
        {key, {projection_index, value, expire_at_ms}}
      end)

    case reduce_keydir_rows_while(keydir, [], fn row, acc ->
           case segment_projection_checkpoint_relocation(row, entry_by_key, trim_index, now) do
             {:ok, relocation} -> {:cont, [relocation | acc]}
             :skip -> {:cont, acc}
             {:error, reason} -> {:halt, {:error, reason}}
           end
         end) do
      :unavailable ->
        {:error, {:segment_keydir_unavailable, shard_index}}

      {:ok, relocations} ->
        {:ok, Enum.reverse(relocations)}

      {:error, _reason} = error ->
        error
    end
  rescue
    error -> {:error, {:segment_projection_checkpoint_relocations_failed, error}}
  end

  defp segment_projection_checkpoint_relocation(
         {key, _value, expire_at_ms, _lfu, file_id, _offset, _value_size} = row,
         entry_by_key,
         trim_index,
         now
       )
       when is_binary(key) do
    cond do
      not live_expire_at?(expire_at_ms, now) ->
        :skip

      not segment_projection_relocatable_file_id?(file_id, trim_index) ->
        :skip

      true ->
        case Map.fetch(entry_by_key, key) do
          {:ok, {projection_index, projected_value, ^expire_at_ms}} ->
            {:ok, {projection_index, {{key, projected_value, expire_at_ms}, row}}}

          {:ok, {_projection_index, _projected_value, projected_expire_at_ms}} ->
            {:error,
             {:segment_projection_checkpoint_expire_mismatch, key, expire_at_ms,
              projected_expire_at_ms}}

          :error ->
            {:error, {:segment_projection_checkpoint_missing_key, key, file_id}}
        end
    end
  end

  defp segment_projection_checkpoint_relocation(_row, _entry_by_key, _trim_index, _now),
    do: :skip

  defp segment_projection_relocatable_file_id?({:waraft_segment, index}, trim_index)
       when is_integer(index),
       do: index < trim_index

  defp segment_projection_relocatable_file_id?({:waraft_apply_projection, index}, trim_index)
       when is_integer(index),
       do: index < trim_index

  defp segment_projection_relocatable_file_id?({:waraft_projection, index}, _trim_index)
       when is_integer(index),
       do: true

  defp segment_projection_relocatable_file_id?(_file_id, _trim_index), do: false

  defp prepare_segment_value_pins_for_trim(root_dir, ctx, shard_index, trim_index) do
    prepare_segment_value_pins_for_trim(
      root_dir,
      ctx,
      shard_index,
      trim_index,
      @segment_value_pin_scan_limit
    )
  end

  defp prepare_segment_value_pins_for_trim(root_dir, ctx, shard_index, trim_index, page_limit) do
    lmdb_path = flow_lmdb_path(ctx, shard_index)

    do_prepare_segment_value_pins_for_trim(
      root_dir,
      ctx,
      shard_index,
      lmdb_path,
      trim_index,
      <<>>,
      page_limit,
      0
    )
  rescue
    error -> {:error, {:prepare_segment_value_pins_for_trim_failed, error}}
  end

  defp do_prepare_segment_value_pins_for_trim(
         root_dir,
         ctx,
         shard_index,
         lmdb_path,
         trim_index,
         after_key,
         page_limit,
         count
       ) do
    case FlowLMDB.segment_value_pin_entries_before_page(
           lmdb_path,
           trim_index,
           after_key,
           page_limit
         ) do
      {:ok, pins, next_after_key, done?} ->
        with {:ok, relocations} <- segment_value_pin_relocations_from_pins(ctx, shard_index, pins),
             :ok <- write_apply_projection_value_pins(root_dir, relocations),
             :ok <- relocate_segment_value_pins(ctx, shard_index, relocations) do
          next_count = count + length(relocations)

          if done? do
            {:ok, next_count}
          else
            do_prepare_segment_value_pins_for_trim(
              root_dir,
              ctx,
              shard_index,
              lmdb_path,
              trim_index,
              next_after_key,
              page_limit,
              next_count
            )
          end
        end

      {:error, reason} ->
        {:error, {:collect_segment_value_pin_relocations_failed, reason}}
    end
  end

  defp segment_value_pin_relocations_from_pins(ctx, shard_index, pins) do
    pins
    |> Enum.reduce_while({:ok, []}, fn pin, {:ok, acc} ->
      case segment_value_pin_relocation_from_pin(ctx, shard_index, pin) do
        {:ok, relocation} -> {:cont, {:ok, [relocation | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, relocations} -> {:ok, Enum.reverse(relocations)}
      {:error, _reason} = error -> error
    end
  end

  defp segment_value_pin_relocation_from_pin(
         ctx,
         shard_index,
         %{
           key: key,
           expire_at_ms: expire_at_ms,
           file_id: file_id,
           offset: offset,
           value_size: value_size,
           pin_key: pin_key
         }
       )
       when is_binary(key) and valid_segment_backed_file_id(file_id) and is_integer(offset) and
              offset >= 0 and is_integer(value_size) and value_size >= 0 and
              is_binary(pin_key) do
    if expired_segment_value_pin?(expire_at_ms) do
      {:ok,
       %{
         key: key,
         expire_at_ms: expire_at_ms,
         source_file_id: file_id,
         source_offset: offset,
         source_value_size: value_size,
         source_pin_key: pin_key,
         stale?: true
       }}
    else
      segment_value_pin_relocation_from_live_pin(
        ctx,
        shard_index,
        key,
        expire_at_ms,
        file_id,
        offset,
        value_size,
        pin_key
      )
    end
  end

  defp segment_value_pin_relocation_from_pin(_ctx, _shard_index, pin),
    do: {:error, {:bad_segment_value_pin, pin}}

  defp segment_value_pin_relocation_from_live_pin(
         ctx,
         shard_index,
         key,
         expire_at_ms,
         file_id,
         offset,
         value_size,
         pin_key
       ) do
    case WARaftSegmentReader.read_value_from_location(ctx, shard_index, file_id, key) do
      {:ok, value} when is_binary(value) ->
        {:ok,
         %{
           key: key,
           value: value,
           expire_at_ms: expire_at_ms,
           source_file_id: file_id,
           source_offset: offset,
           source_value_size: value_size,
           source_pin_key: pin_key
         }}

      :not_found ->
        {:error, {:segment_value_pin_missing_live_value, key, file_id}}

      {:error, reason} ->
        {:error, {:segment_value_pin_read_failed, key, file_id, reason}}
    end
  end

  defp expired_segment_value_pin?(expire_at_ms)
       when is_integer(expire_at_ms) and expire_at_ms > 0,
       do: not live_expire_at?(expire_at_ms, HLC.now_ms())

  defp expired_segment_value_pin?(_expire_at_ms), do: false

  defp write_apply_projection_value_pins(_root_dir, []), do: :ok

  defp write_apply_projection_value_pins(root_dir, relocations) do
    relocations = Enum.reject(relocations, &Map.get(&1, :stale?, false))

    if relocations == [] do
      :ok
    else
      batches =
        relocations
        |> Enum.group_by(fn %{source_file_id: {_tag, index}} -> index end)
        |> Enum.map(fn {index, index_relocations} ->
          entries =
            Enum.map(index_relocations, fn %{key: key, value: value, expire_at_ms: expire_at_ms} ->
              {key, value, expire_at_ms}
            end)

          {{:raft_log_pos, index, 0}, entries}
        end)

      case :ferricstore_waraft_spike_segment_log.write_projection_batches_sync(
             root_dir
             |> apply_projection_root()
             |> to_charlist(),
             batches
           ) do
        :ok -> :ok
        {:error, reason} -> {:error, {:write_apply_projection_value_pins_failed, reason}}
        other -> {:error, {:write_apply_projection_value_pins_failed, other}}
      end
    end
  end

  defp relocate_segment_projection_keydir(_ctx, _shard_index, _projection_root, []), do: :ok

  defp relocate_segment_projection_keydir(ctx, shard_index, projection_root, relocations) do
    keydir = elem(ctx.keydir_refs, shard_index)

    with :ok <-
           maybe_run_segment_projection_before_relocate_hook(
             shard_index,
             projection_root,
             relocations
           ) do
      relocations
      |> Enum.with_index(1)
      |> Enum.reduce_while(:ok, fn {relocation, projection_index}, :ok ->
        case relocate_segment_projection_row(
               keydir,
               projection_root,
               projection_index,
               relocation
             ) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end
  rescue
    error -> {:error, {:relocate_segment_projection_keydir_failed, error}}
  end

  defp relocate_segment_projection_keydir_from_checkpoint(
         _ctx,
         _shard_index,
         _projection_root,
         []
       ),
       do: :ok

  defp relocate_segment_projection_keydir_from_checkpoint(
         ctx,
         shard_index,
         projection_root,
         relocations
       ) do
    keydir = elem(ctx.keydir_refs, shard_index)

    with :ok <-
           maybe_run_segment_projection_before_relocate_hook(
             shard_index,
             projection_root,
             relocations
           ) do
      Enum.reduce_while(relocations, :ok, fn {projection_index, relocation}, :ok ->
        case relocate_segment_projection_row(
               keydir,
               projection_root,
               projection_index,
               relocation
             ) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end
  rescue
    error -> {:error, {:relocate_segment_projection_keydir_from_checkpoint_failed, error}}
  end

  defp relocate_segment_value_pins(_ctx, _shard_index, []), do: :ok

  defp relocate_segment_value_pins(
         ctx,
         shard_index,
         relocations
       ) do
    lmdb_path = flow_lmdb_path(ctx, shard_index)

    ops =
      relocations
      |> Enum.reduce_while({:ok, []}, fn relocation, {:ok, acc} ->
        case segment_value_pin_relocation_ops(lmdb_path, relocation) do
          {:ok, relocation_ops} -> {:cont, {:ok, [relocation_ops | acc]}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)

    with {:ok, op_groups} <- ops do
      lmdb_ops =
        op_groups
        |> Enum.reverse()
        |> List.flatten()
        |> Enum.uniq()

      FlowLMDB.write_batch(lmdb_path, lmdb_ops)
    end
  rescue
    error -> {:error, {:relocate_segment_value_pins_failed, error}}
  end

  defp segment_value_pin_relocation_ops(
         lmdb_path,
         %{
           key: key,
           expire_at_ms: expire_at_ms,
           source_file_id: source_file_id,
           source_offset: source_offset,
           source_value_size: source_value_size,
           source_pin_key: source_pin_key
         }
       ) do
    with :current <-
           current_segment_value_pin_locator(
             lmdb_path,
             key,
             source_file_id,
             source_offset,
             source_value_size
           ) do
      target_file_id = apply_projection_pin_target(source_file_id)

      {:ok,
       [
         {:put, key,
          FlowLMDB.encode_value_locator(
            expire_at_ms,
            target_file_id,
            apply_projection_pin_target_offset(source_file_id, source_offset),
            source_value_size
          )},
         {:delete, source_pin_key}
       ]}
    else
      :changed_or_deleted ->
        {:ok, [{:delete, source_pin_key}]}

      {:error, _reason} = error ->
        error
    end
  end

  defp apply_projection_pin_target({:waraft_segment, index}),
    do: {:waraft_apply_projection, index}

  defp apply_projection_pin_target({:waraft_apply_projection, index}),
    do: {:waraft_apply_projection, index}

  defp apply_projection_pin_target(file_id), do: file_id

  defp apply_projection_pin_target_offset({:waraft_segment, _index}, _source_offset), do: 0

  defp apply_projection_pin_target_offset({:waraft_apply_projection, _index}, source_offset),
    do: source_offset

  defp apply_projection_pin_target_offset(_file_id, source_offset), do: source_offset

  defp current_segment_value_pin_locator(
         lmdb_path,
         key,
         source_file_id,
         source_offset,
         source_value_size
       ) do
    case FlowLMDB.get(lmdb_path, key) do
      {:ok, blob} ->
        case FlowLMDB.decode_value_locator(blob, HLC.now_ms()) do
          {:ok, {^source_file_id, ^source_offset, ^source_value_size}} -> :current
          _expired_or_changed -> :changed_or_deleted
        end

      :not_found ->
        :changed_or_deleted

      {:error, reason} ->
        {:error, {:read_segment_value_pin_locator_failed, key, reason}}
    end
  end

  defp prune_apply_projection_cache_after_segment_projection(
         ctx,
         shard_index,
         trim_index,
         relocations
       ) do
    keydir = elem(ctx.keydir_refs, shard_index)

    relocated_refs = apply_projection_refs_from_relocations(relocations)

    before_trim_refs =
      Ferricstore.Raft.WARaftSegmentReader.apply_projection_refs_before(
        ctx.data_dir,
        shard_index,
        trim_index
      )

    refs =
      relocated_refs
      |> Enum.concat(before_trim_refs)
      |> Enum.uniq()
      |> Enum.reject(&apply_projection_ref_still_referenced?(keydir, &1))

    Ferricstore.Raft.WARaftSegmentReader.delete_apply_projection_entries(
      ctx.data_dir,
      shard_index,
      refs
    )

    :ok
  end

  defp apply_projection_refs_from_relocations(relocations) do
    Enum.flat_map(relocations, fn
      {projection_index, relocation} when is_integer(projection_index) ->
        apply_projection_refs_from_relocations([relocation])

      {{key, _value, _expire_at_ms},
       {row_key, _ets_value, _ets_expire_at_ms, _lfu, {:waraft_apply_projection, index}, _offset,
        _value_size}}
      when key == row_key and is_integer(index) and index > 0 ->
        [{index, key}]

      _relocation ->
        []
    end)
  end

  defp apply_projection_ref_still_referenced?(keydir, {index, key})
       when is_integer(index) and index > 0 and is_binary(key) do
    case :ets.lookup(keydir, key) do
      [
        {^key, _value, _expire_at_ms, _lfu, {:waraft_apply_projection, ^index}, _offset,
         _value_size}
      ] ->
        true

      _not_current ->
        false
    end
  rescue
    ArgumentError -> false
  end

  defp apply_projection_ref_still_referenced?(_keydir, _ref), do: false

  defp maybe_run_segment_projection_before_relocate_hook(
         shard_index,
         projection_root,
         relocations
       ) do
    case Application.get_env(:ferricstore, :waraft_segment_projection_before_relocate_hook) do
      hook when is_function(hook, 3) ->
        case hook.(shard_index, projection_root, relocations) do
          :ok -> :ok
          other -> {:error, {:segment_projection_before_relocate_hook, other}}
        end

      _ ->
        :ok
    end
  end

  defp relocate_segment_projection_row(
         keydir,
         projection_root,
         projection_index,
         {{key, value, expire_at_ms}, original_row}
       ) do
    with {:ok, projection_offset} <- projection_record_location(projection_root, projection_index) do
      compare_and_relocate_segment_projection_row(
        keydir,
        key,
        value,
        expire_at_ms,
        original_row,
        projection_index,
        projection_offset
      )
    end
  end

  defp compare_and_relocate_segment_projection_row(
         keydir,
         key,
         projected_value,
         expire_at_ms,
         {key, original_value, expire_at_ms, _original_lfu, original_file_id, original_offset,
          original_value_size},
         projection_index,
         projection_offset
       ) do
    case :ets.lookup(keydir, key) do
      [
        {^key, current_value, ^expire_at_ms, current_lfu, ^original_file_id, ^original_offset,
         ^original_value_size}
      ] ->
        if original_value == nil or current_value == original_value do
          :ets.insert(
            keydir,
            {key, current_value, expire_at_ms, current_lfu,
             {:waraft_projection, projection_index}, projection_offset,
             segment_projected_value_size(projected_value)}
          )
        end

        :ok

      _changed_or_deleted ->
        :ok
    end
  end

  defp compare_and_relocate_segment_projection_row(
         _keydir,
         key,
         _projected_value,
         _expire_at_ms,
         original_row,
         _projection_index,
         _projection_offset
       ),
       do: {:error, {:bad_segment_projection_relocation_row, key, original_row}}

  defp segment_projection_entry_from_keydir_row(
         {key, value, expire_at_ms, _lfu, _file_id, _offset, _value_size},
         _ctx,
         _shard_index,
         now
       )
       when is_binary(key) and is_binary(value) do
    if live_expire_at?(expire_at_ms, now) do
      {:ok, {key, value, expire_at_ms}}
    else
      :skip
    end
  end

  defp segment_projection_entry_from_keydir_row(
         {key, nil, expire_at_ms, _lfu, file_id, offset, _value_size},
         ctx,
         shard_index,
         now
       )
       when is_binary(key) do
    if live_expire_at?(expire_at_ms, now) do
      case read_keydir_cold_value(ctx, shard_index, key, file_id, offset) do
        {:ok, value} when is_binary(value) ->
          {:ok, {key, value, expire_at_ms}}

        :not_found ->
          {:error, {:segment_projection_missing_live_value, key, file_id}}

        {:error, reason} ->
          {:error, {:segment_projection_read_failed, key, file_id, reason}}
      end
    else
      :skip
    end
  end

  defp segment_projection_entry_from_keydir_row(_row, _ctx, _shard_index, _now), do: :skip

  defp read_keydir_cold_value(ctx, shard_index, key, file_id, _offset)
       when valid_segment_backed_file_id(file_id) do
    Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(ctx, shard_index, file_id, key)
  end

  defp read_keydir_cold_value(ctx, shard_index, key, file_id, offset)
       when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 do
    path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> ShardETS.file_path(file_id)

    ColdRead.pread_keyed(path, offset, key, @cold_read_timeout_ms)
  end

  defp read_keydir_cold_value(ctx, shard_index, key, {:flow_history, file_id} = location, offset)
       when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 do
    path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> ShardETS.file_path(location)

    ColdRead.pread_keyed(path, offset, key, @cold_read_timeout_ms)
  end

  defp read_keydir_cold_value(_ctx, _shard_index, _key, file_id, _offset),
    do: {:error, {:unsupported_segment_projection_location, file_id}}

  defp flow_lmdb_path(%{data_dir: data_dir}, shard_index) do
    data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> FlowLMDB.path()
  end

  defp segment_keydir_available?(%{sm_state: %{ets: keydir}}), do: :ets.info(keydir) != :undefined
  defp segment_keydir_available?(_handle), do: false

  defp reduce_keydir_rows_while(keydir, acc, fun) when is_function(fun, 2) do
    case :ets.info(keydir) do
      :undefined ->
        :unavailable

      _info ->
        {:ok,
         :ets.foldl(
           fn row, next_acc ->
             case fun.(row, next_acc) do
               {:cont, reduced} -> reduced
               {:halt, result} -> throw({:keydir_reduce_halt, result})
             end
           end,
           acc,
           keydir
         )}
    end
  rescue
    ArgumentError -> :unavailable
  catch
    {:keydir_reduce_halt, result} -> result
  end

  defp live_expire_at?(0, _now), do: true
  defp live_expire_at?(expire_at_ms, now) when is_integer(expire_at_ms), do: expire_at_ms > now
  defp live_expire_at?(_expire_at_ms, _now), do: false

  defp write_snapshot_metadata(snapshot_path, handle, segment_projection \\ nil) do
    with {:ok, empty_payload_dirs} <- empty_snapshot_payload_kinds(snapshot_path),
         {:ok, empty_storage_payload_dirs} <- empty_snapshot_storage_payload_kinds(snapshot_path) do
      metadata =
        %{
          version: @version,
          position: handle.position,
          label: handle.label,
          config: handle.config,
          payload_dirs: snapshot_payload_kinds(),
          empty_payload_dirs: empty_payload_dirs,
          storage_payload_dirs: snapshot_storage_payload_kinds(),
          empty_storage_payload_dirs: empty_storage_payload_dirs
        }
        |> maybe_put_segment_projection_metadata(segment_projection)

      atomic_write_snapshot_metadata(snapshot_path, metadata)
    end
  end

  defp maybe_put_segment_projection_metadata(metadata, nil), do: metadata

  defp maybe_put_segment_projection_metadata(metadata, segment_projection),
    do: Map.put(metadata, :segment_projection, segment_projection)

  defp atomic_write_snapshot_metadata(snapshot_path, metadata) do
    with {:ok, payload} <- encode_snapshot_metadata(metadata) do
      atomic_write_binary(Path.join(snapshot_path, @snapshot_metadata_file), payload)
    end
  end

  defp encode_snapshot_metadata(metadata) do
    payload = metadata |> encode_persisted_metadata_term() |> :erlang.term_to_binary()

    if byte_size(payload) <= @max_snapshot_metadata_bytes do
      {:ok, payload}
    else
      {:error,
       {:snapshot_metadata_term_too_large, byte_size(payload), @max_snapshot_metadata_bytes}}
    end
  end

  defp read_snapshot_metadata(snapshot_path) do
    path = Path.join(snapshot_path, @snapshot_metadata_file)

    case read_snapshot_metadata_file(path) do
      {:ok, binary} ->
        case persisted_binary_to_term(binary) do
          {:ok, %{version: @version, position: _position} = metadata} ->
            validate_snapshot_metadata(metadata)

          {:ok, other} ->
            {:error, {:bad_snapshot_metadata, other}}

          {:error, reason} ->
            {:error, {:decode_snapshot_metadata, reason}}
        end

      {:error, reason} ->
        {:error, {:read_snapshot_metadata, reason}}
    end
  end

  defp read_snapshot_metadata_file(path) do
    read_bounded_metadata_file(
      path,
      @max_snapshot_metadata_bytes,
      :snapshot_metadata_file_too_large
    )
  end

  defp read_snapshot_segment_projection(_snapshot_path, %{segment_projection: nil}, _position),
    do: {:ok, []}

  defp read_snapshot_segment_projection(
         snapshot_path,
         %{segment_projection: projection_metadata},
         position
       )
       when is_map(projection_metadata) do
    projection_root = Path.join(snapshot_path, Map.fetch!(projection_metadata, :dir))
    expected_count = Map.get(projection_metadata, :count)

    with {:ok, projection} <- read_segment_projection_log(projection_root),
         :ok <- verify_segment_projection_position(projection, position),
         {:ok, entries} <- validate_segment_projection_entries(projection),
         :ok <- verify_segment_projection_count(entries, expected_count) do
      {:ok, entries}
    else
      {:error, reason} -> {:error, {:read_segment_projection_snapshot, reason}}
    end
  end

  defp read_snapshot_segment_projection(_snapshot_path, _metadata, _position), do: {:ok, []}

  defp read_segment_projection_log(projection_root) do
    case :ferricstore_waraft_spike_segment_log.fold_disk(
           to_charlist(projection_root),
           &fold_segment_projection_record/3,
           %{header: nil, entries: [], invalid: []}
         ) do
      {:ok, %{invalid: [invalid | _]}} ->
        {:error, {:bad_segment_projection_record, invalid}}

      {:ok, %{header: nil, entries: []}} ->
        {:error, :enoent}

      {:ok, %{header: nil}} ->
        {:error, :missing_segment_projection_header}

      {:ok, %{header: {position, count}, entries: entries}} ->
        entries = Enum.reverse(entries)

        with :ok <- verify_segment_projection_count(entries, count) do
          {:ok, %{version: @version, position: position, entries: entries}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp fold_segment_projection_record(
         0,
         {0, {:ferricstore_segment_projection_header, position, count}},
         acc
       )
       when is_integer(count) and count >= 0 do
    %{acc | header: {position, count}}
  end

  defp fold_segment_projection_record(
         index,
         {0, {:ferricstore_segment_projection_entry, key, value, expire_at_ms}},
         acc
       )
       when is_integer(index) and index > 0 do
    %{acc | entries: [{key, value, expire_at_ms} | acc.entries]}
  end

  defp fold_segment_projection_record(index, entry, acc) do
    %{acc | invalid: [{index, entry} | acc.invalid]}
  end

  defp verify_segment_projection_position(%{position: expected}, expected), do: :ok

  defp verify_segment_projection_position(%{position: actual}, expected),
    do: {:error, {:bad_segment_projection_position, actual, expected}}

  defp validate_segment_projection_entries(%{entries: entries}) when is_list(entries) do
    if Enum.all?(entries, &valid_segment_projection_entry?/1) do
      {:ok, entries}
    else
      {:error, {:bad_segment_projection_entries, entries}}
    end
  end

  defp validate_segment_projection_entries(projection),
    do: {:error, {:bad_segment_projection_entries, projection}}

  defp valid_segment_projection_entry?({key, value, expire_at_ms})
       when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) and expire_at_ms >= 0,
       do: true

  defp valid_segment_projection_entry?(_entry), do: false

  defp verify_segment_projection_count(entries, count) when is_integer(count) do
    if length(entries) == count do
      :ok
    else
      {:error, {:bad_segment_projection_count, count, length(entries)}}
    end
  end

  defp verify_segment_projection_count(_entries, _count), do: :ok

  defp read_bounded_metadata_file(path, max_bytes, too_large_reason) do
    case File.lstat(path) do
      {:ok, %{type: :regular, size: size}} when size <= max_bytes ->
        File.read(path)

      {:ok, %{type: :regular, size: size}} ->
        {:error, {too_large_reason, size, max_bytes}}

      {:ok, %{type: type}} ->
        {:error, {:unsafe_metadata_path, path, type}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp validate_snapshot_metadata(%{position: position, config: config} = metadata)
       when is_map(metadata) do
    with :ok <- validate_raft_position(position),
         :ok <- validate_storage_config(config),
         :ok <- validate_snapshot_payload_metadata(metadata) do
      {:ok, metadata}
    else
      {:error, reason} -> {:error, {:bad_snapshot_metadata, reason}}
    end
  end

  defp validate_snapshot_metadata(%{position: position} = metadata) when is_map(metadata) do
    validation =
      with :ok <- validate_raft_position(position),
           :ok <- validate_snapshot_payload_metadata(metadata) do
        :ok
      end

    case validation do
      :ok ->
        {:ok, metadata}

      {:error, reason} ->
        {:error, {:bad_snapshot_metadata, reason}}
    end
  end

  defp validate_snapshot_payload_metadata(metadata) do
    with :ok <-
           validate_snapshot_payload_dir_list(
             :payload_dirs,
             Map.get(metadata, :payload_dirs, snapshot_payload_kinds())
           ),
         :ok <-
           validate_snapshot_payload_dir_list(
             :empty_payload_dirs,
             Map.get(metadata, :empty_payload_dirs, [])
           ),
         :ok <-
           validate_snapshot_storage_payload_dir_list(
             :storage_payload_dirs,
             Map.get(metadata, :storage_payload_dirs, [])
           ),
         :ok <-
           validate_snapshot_storage_payload_dir_list(
             :empty_storage_payload_dirs,
             Map.get(metadata, :empty_storage_payload_dirs, [])
           ),
         :ok <- validate_segment_projection_metadata(Map.get(metadata, :segment_projection)) do
      :ok
    end
  end

  defp validate_segment_projection_metadata(nil), do: :ok

  defp validate_segment_projection_metadata(%{
         dir: @segment_projection_dir,
         format: :segment_log,
         count: count
       })
       when is_integer(count) and count >= 0,
       do: :ok

  defp validate_segment_projection_metadata(other),
    do: {:error, {:bad_segment_projection, other}}

  defp validate_snapshot_payload_dir_list(field, dirs) when is_list(dirs) do
    allowed = snapshot_payload_kinds()

    if Enum.all?(dirs, &(&1 in allowed)) do
      :ok
    else
      {:error, {:bad_payload_dirs, field, dirs}}
    end
  end

  defp validate_snapshot_payload_dir_list(field, other),
    do: {:error, {:bad_payload_dirs, field, other}}

  defp validate_snapshot_storage_payload_dir_list(field, dirs) when is_list(dirs) do
    allowed = snapshot_storage_payload_kinds()

    if Enum.all?(dirs, &(&1 in allowed)) do
      :ok
    else
      {:error, {:bad_payload_dirs, field, dirs}}
    end
  end

  defp validate_snapshot_storage_payload_dir_list(field, other),
    do: {:error, {:bad_payload_dirs, field, other}}

  defp verify_snapshot_position(%{position: expected}, expected), do: :ok

  defp verify_snapshot_position(%{position: actual}, expected),
    do: {:error, {:bad_position, actual, expected}}

  defp verify_snapshot_payload_dirs(metadata, snapshot_path, handle) do
    payload_dirs = Map.get(metadata, :payload_dirs, snapshot_payload_kinds())

    with :ok <-
           verify_snapshot_dirs(
             metadata,
             snapshot_path,
             handle,
             shard_dir_specs(handle, payload_dirs),
             :empty_payload_dirs
           ),
         :ok <-
           verify_snapshot_dirs(
             metadata,
             snapshot_path,
             handle,
             storage_payload_dir_specs(handle, Map.get(metadata, :storage_payload_dirs, [])),
             :empty_storage_payload_dirs
           ) do
      :ok
    end
  end

  defp verify_snapshot_dirs(_metadata, _snapshot_path, _handle, [], _empty_field), do: :ok

  defp verify_snapshot_dirs(metadata, snapshot_path, handle, specs, empty_field) do
    empty_payload_dirs = Map.get(metadata, empty_field, [])

    missing =
      Enum.reject(specs, fn {kind, _dest} ->
        Ferricstore.FS.dir?(Path.join(snapshot_path, Atom.to_string(kind)))
      end)

    forbidden_missing =
      Enum.reject(missing, fn {kind, _dest} ->
        kind in empty_payload_dirs
      end)

    cond do
      forbidden_missing == [] ->
        :ok

      bootstrap_empty_snapshot?(missing, specs, handle) ->
        :ok

      true ->
        {kind, _dest} = hd(forbidden_missing)
        {:error, {:missing_snapshot_dir, kind, Path.join(snapshot_path, Atom.to_string(kind))}}
    end
  end

  defp empty_snapshot_payload_kinds(snapshot_path) do
    empty_snapshot_payload_kinds(snapshot_path, snapshot_payload_kinds())
  end

  defp empty_snapshot_storage_payload_kinds(snapshot_path) do
    empty_snapshot_payload_kinds(snapshot_path, snapshot_storage_payload_kinds())
  end

  defp empty_snapshot_payload_kinds(snapshot_path, kinds) do
    kinds
    |> Enum.reduce_while({:ok, []}, fn kind, {:ok, acc} ->
      path = Path.join(snapshot_path, Atom.to_string(kind))

      case dir_payload_empty(path) do
        {:ok, true} -> {:cont, {:ok, [kind | acc]}}
        {:ok, false} -> {:cont, {:ok, acc}}
        {:error, reason} -> {:halt, {:error, {:snapshot_payload_empty, kind, reason}}}
      end
    end)
    |> case do
      {:ok, kinds} -> {:ok, Enum.reverse(kinds)}
      {:error, _reason} = error -> error
    end
  end

  # WARaft can create a metadata-only witness snapshot while bootstrapping an empty
  # member. Any non-empty/opened storage position must include payload dirs, because
  # installing a metadata-only snapshot would reset Bitcask/blob/probability files.
  defp bootstrap_empty_snapshot?(missing, specs, %{position: @zero_pos}) do
    length(missing) == length(specs) and live_snapshot_payload_empty?(specs) == {:ok, true}
  end

  defp bootstrap_empty_snapshot?(_missing, _specs, _handle), do: false

  defp live_snapshot_payload_empty?(specs) do
    Enum.reduce_while(specs, {:ok, true}, fn {_kind, dest}, {:ok, true} ->
      case dir_payload_empty(dest) do
        {:ok, true} -> {:cont, {:ok, true}}
        {:ok, false} -> {:halt, {:ok, false}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp dir_payload_empty(path) do
    case File.lstat(path) do
      {:ok, %{type: :directory}} ->
        case Ferricstore.FS.ls(path) do
          {:ok, children} -> payload_children_empty(path, children)
          {:error, reason} -> {:error, {:list_dir, path, reason}}
        end

      {:ok, %{type: type}} ->
        {:error, {:not_directory, path, type}}

      {:error, :enoent} ->
        {:ok, true}

      {:error, reason} ->
        {:error, {:stat, path, reason}}
    end
  end

  defp live_payload_empty?(ctx, shard_index) do
    %{ctx: ctx, shard_index: shard_index}
    |> shard_dir_specs()
    |> live_snapshot_payload_empty?()
  end

  defp live_storage_payload_empty?(storage_root, ctx, shard_index) do
    with {:ok, true} <- live_payload_empty?(ctx, shard_index),
         {:ok, true} <- segment_log_payload_empty?(storage_root) do
      {:ok, true}
    else
      {:ok, false} -> {:ok, false}
      {:error, _reason} = error -> error
    end
  end

  defp segment_log_payload_empty?(storage_root) do
    storage_root
    |> Path.join("segment_log")
    |> dir_payload_empty()
  end

  defp payload_children_empty(_path, []), do: {:ok, true}

  defp payload_children_empty(path, [child | rest]) do
    child_path = Path.join(path, child)

    cond do
      ignorable_payload_dir?(child_path) ->
        payload_children_empty(path, rest)

      true ->
        payload_child_empty(path, child_path, rest)
    end
  end

  defp payload_child_empty(path, child_path, rest) do
    case File.lstat(child_path) do
      {:ok, %{type: :directory}} ->
        case dir_payload_empty(child_path) do
          {:ok, true} -> payload_children_empty(path, rest)
          {:ok, false} -> {:ok, false}
          {:error, _reason} = error -> error
        end

      {:ok, %{type: :regular, size: 0}} ->
        payload_children_empty(path, rest)

      {:ok, %{type: :regular}} ->
        if ignorable_payload_marker?(child_path) do
          payload_children_empty(path, rest)
        else
          {:ok, false}
        end

      {:ok, %{type: type}} ->
        {:error, {:unsafe_snapshot_payload_path, child_path, type}}

      {:error, :enoent} ->
        payload_children_empty(path, rest)

      {:error, reason} ->
        {:error, {:stat, child_path, reason}}
    end
  end

  defp ignorable_payload_dir?(path) do
    # LMDB is a lagged/cold Flow projection. It is rebuilt from durable Flow
    # records and must not make a fresh WARaft storage bootstrap look unsafe.
    Path.basename(path) == "flow_lmdb" and Ferricstore.Flow.LMDB.env_present?(path)
  end

  defp ignorable_payload_marker?(path) do
    Path.basename(path) == "flow_history_projected.index" and
      Ferricstore.Flow.HistoryProjectedIndex.read(Path.dirname(path)) == 0
  end

  defp atomic_write_term(path, term) do
    atomic_write_binary(path, :erlang.term_to_binary(term))
  end

  defp atomic_write_binary(path, payload) do
    tmp = "#{path}.tmp.#{System.unique_integer([:positive])}"
    previous = maybe_metadata_previous_path(path)

    with :ok <- Ferricstore.FS.mkdir_p(Path.dirname(path)),
         :ok <- File.write(tmp, payload),
         :ok <- fsync_metadata_file(tmp),
         :ok <- stage_previous_metadata(path, previous),
         :ok <- Ferricstore.FS.rename(tmp, path),
         :ok <- fsync_dir(Path.dirname(path)) do
      :ok
    else
      {:error, reason} = error ->
        _ = Ferricstore.FS.rm(tmp)
        _ = restore_previous_metadata(path, previous)
        {:error, reason || error}
    end
  end

  defp maybe_metadata_previous_path(path) do
    if Path.basename(path) == @metadata_file do
      metadata_previous_path(path)
    else
      nil
    end
  end

  defp stage_previous_metadata(_path, nil), do: :ok

  defp stage_previous_metadata(path, previous) do
    case Ferricstore.FS.rename(path, previous) do
      :ok -> :ok
      {:error, {:not_found, _}} -> :ok
      {:error, reason} -> {:error, {:stage_previous_metadata, path, previous, reason}}
    end
  end

  defp restore_previous_metadata(_path, nil), do: :ok

  defp restore_previous_metadata(path, previous) do
    cond do
      Ferricstore.FS.exists?(path) ->
        :ok

      Ferricstore.FS.exists?(previous) ->
        Ferricstore.FS.rename(previous, path)

      true ->
        :ok
    end
  end

  defp restore_previous_metadata_after_publish(path) do
    previous = maybe_metadata_previous_path(path)

    cond do
      is_nil(previous) ->
        :ok

      not Ferricstore.FS.exists?(previous) ->
        :ok

      true ->
        do_restore_previous_metadata_after_publish(path, previous)
    end
  end

  defp do_restore_previous_metadata_after_publish(path, previous) do
    failed = "#{path}.failed.#{System.unique_integer([:positive])}"

    case Ferricstore.FS.rename(path, failed) do
      :ok ->
        case Ferricstore.FS.rename(previous, path) do
          :ok ->
            _ = fsync_dir(Path.dirname(path))
            _ = Ferricstore.FS.rm(failed)
            :ok

          {:error, reason} ->
            _ = Ferricstore.FS.rename(failed, path)
            {:error, {:restore_previous_metadata_after_publish, path, previous, reason}}
        end

      {:error, {:not_found, _}} ->
        case Ferricstore.FS.rename(previous, path) do
          :ok ->
            _ = fsync_dir(Path.dirname(path))
            :ok

          {:error, reason} ->
            {:error, {:restore_previous_metadata_after_publish, path, previous, reason}}
        end

      {:error, reason} ->
        {:error, {:stage_failed_metadata_after_publish, path, failed, reason}}
    end
  end

  defp read_previous_storage_metadata(path) do
    previous = metadata_previous_path(path)

    case read_storage_metadata_file(previous, :previous_storage_metadata_file_too_large) do
      {:ok, binary} ->
        case persisted_binary_to_term(binary) do
          {:ok, %{version: @version} = metadata} -> validate_storage_metadata(metadata)
          {:ok, other} -> {:error, {:bad_previous_storage_metadata, other}}
          {:error, reason} -> {:error, {:decode_previous_storage_metadata, reason}}
        end

      {:error, reason} ->
        {:error, {:read_previous_storage_metadata, reason}}
    end
  end

  defp append_metadata_journal_payload(path, payload) do
    journal_path = metadata_journal_path(path)

    record =
      <<@metadata_journal_magic, byte_size(payload)::32, :erlang.crc32(payload)::32,
        payload::binary>>

    new_file? = not Ferricstore.FS.exists?(journal_path)

    case metadata_journal_size(journal_path) do
      {:ok, previous_size} ->
        with :ok <- Ferricstore.FS.mkdir_p(Path.dirname(journal_path)),
             :ok <- File.write(journal_path, record, [:append, :binary]),
             :ok <- fsync_metadata_file(journal_path),
             :ok <- maybe_fsync_new_metadata_journal_dir(journal_path, new_file?) do
          :ok
        else
          {:error, _reason} = error ->
            _ = rollback_metadata_journal_append(journal_path, new_file?, previous_size)
            error

          other ->
            _ = rollback_metadata_journal_append(journal_path, new_file?, previous_size)
            {:error, other}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp metadata_journal_size(journal_path) do
    case File.lstat(journal_path) do
      {:ok, %{type: :regular, size: size}} -> {:ok, size}
      {:ok, %{type: type}} -> {:error, {:unsafe_metadata_path, journal_path, type}}
      {:error, :enoent} -> {:ok, 0}
      {:error, reason} -> {:error, {:stat_metadata_journal, reason}}
    end
  end

  defp rollback_metadata_journal_append(journal_path, true, 0) do
    case Ferricstore.FS.rm(journal_path) do
      :ok -> :ok
      {:error, {:not_found, _}} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp rollback_metadata_journal_append(journal_path, _new_file?, previous_size)
       when is_integer(previous_size) and previous_size >= 0 do
    case File.open(journal_path, [:read, :write, :binary]) do
      {:ok, io} ->
        try do
          with {:ok, _pos} <- :file.position(io, previous_size),
               :ok <- :file.truncate(io) do
            :ok
          end
        after
          File.close(io)
        end

      {:error, :enoent} ->
        :ok

      {:error, _reason} = error ->
        error
    end
  end

  defp delete_metadata_journal(path) do
    journal_path = metadata_journal_path(path)

    case Ferricstore.FS.rm(journal_path) do
      :ok -> fsync_dir(Path.dirname(journal_path))
      {:error, {:not_found, _}} -> :ok
      {:error, reason} -> {:error, {:delete_storage_metadata_journal, reason}}
    end
  end

  defp maybe_fsync_new_metadata_journal_dir(journal_path, true),
    do: fsync_dir(Path.dirname(journal_path))

  defp maybe_fsync_new_metadata_journal_dir(_journal_path, false), do: :ok

  defp read_latest_storage_metadata_journal(path) do
    journal_path = metadata_journal_path(path)

    case File.lstat(journal_path) do
      {:ok, %{type: :regular}} ->
        case File.open(journal_path, [:read, :binary]) do
          {:ok, io} ->
            try do
              read_metadata_journal_record(io, nil)
            after
              File.close(io)
            end

          {:error, reason} ->
            {:error, {:read_storage_metadata_journal, reason}}
        end

      {:ok, %{type: type}} ->
        {:error, {:read_storage_metadata_journal, {:unsafe_metadata_path, journal_path, type}}}

      {:error, reason} ->
        {:error, {:read_storage_metadata_journal, reason}}
    end
  end

  defp read_metadata_journal_record(io, latest) do
    case :file.read(io, byte_size(@metadata_journal_magic) + 8) do
      :eof ->
        latest_or_empty_journal_error(latest)

      {:ok, <<@metadata_journal_magic, size::32, crc::32>>} ->
        read_metadata_journal_payload(io, size, crc, latest)

      {:ok, _partial_or_bad_header} ->
        latest_or_journal_error(latest)

      {:error, reason} ->
        {:error, {:read_storage_metadata_journal, reason}}
    end
  end

  defp read_metadata_journal_payload(io, size, crc, latest) do
    if size > @max_metadata_journal_record_bytes do
      oversized_metadata_journal_error(size, latest)
    else
      read_metadata_journal_payload_bytes(io, size, crc, latest)
    end
  end

  defp read_metadata_journal_payload_bytes(io, size, crc, latest) do
    case :file.read(io, size) do
      {:ok, payload} when byte_size(payload) == size ->
        decode_metadata_journal_payload(io, payload, crc, latest)

      {:ok, _partial} ->
        latest_or_journal_error(latest)

      :eof ->
        latest_or_journal_error(latest)

      {:error, reason} ->
        {:error, {:read_storage_metadata_journal, reason}}
    end
  end

  defp oversized_metadata_journal_error(size, nil) do
    {:error,
     {:bad_storage_metadata_journal_record,
      {:metadata_journal_record_too_large, size, @max_metadata_journal_record_bytes}}}
  end

  defp oversized_metadata_journal_error(_size, metadata), do: {:ok, metadata}

  defp decode_metadata_journal_payload(io, payload, crc, latest) do
    if :erlang.crc32(payload) == crc do
      case persisted_binary_to_term(payload) do
        {:ok, %{version: @version} = metadata} ->
          case validate_storage_metadata(metadata) do
            {:ok, validated} -> read_metadata_journal_record(io, validated)
            {:error, reason} -> {:error, {:bad_storage_metadata_journal_record, reason}}
          end

        {:ok, other} ->
          {:error, {:bad_storage_metadata_journal_record, other}}

        {:error, _reason} ->
          latest_or_journal_error(latest)
      end
    else
      latest_or_journal_error(latest)
    end
  end

  defp latest_or_empty_journal_error(nil), do: {:error, :empty_storage_metadata_journal}
  defp latest_or_empty_journal_error(metadata), do: {:ok, metadata}

  defp latest_or_journal_error(nil), do: {:error, :no_valid_storage_metadata_journal_record}
  defp latest_or_journal_error(metadata), do: {:ok, metadata}

  defp emit_storage_metadata_recovered(path, previous_path, reason) do
    :telemetry.execute(
      [:ferricstore, :waraft, :storage, :metadata_recovered],
      %{count: 1},
      %{path: path, previous_path: previous_path, reason: reason}
    )
  rescue
    _ -> :ok
  end

  defp fsync_metadata_file(path) do
    fsync_file(path, :waraft_storage_metadata_fsync_file_hook)
  end

  defp copy_shard_dirs_to_snapshot(snapshot_path, handle) do
    Enum.reduce_while(shard_dir_specs(handle), :ok, fn {kind, source}, :ok ->
      dest = Path.join(snapshot_path, Atom.to_string(kind))

      with :ok <- copy_dir(source, dest),
           :ok <- maybe_run_snapshot_create_hook({:copied, kind}) do
        {:cont, :ok}
      else
        {:error, {:snapshot_create_hook, _reason}} = error -> {:halt, error}
        {:error, reason} -> {:halt, {:error, {kind, reason}}}
      end
    end)
  end

  defp copy_storage_dirs_to_snapshot(snapshot_path, handle) do
    Enum.reduce_while(storage_payload_dir_specs(handle), :ok, fn {kind, source}, :ok ->
      dest = Path.join(snapshot_path, Atom.to_string(kind))

      with :ok <- copy_dir(source, dest),
           :ok <- maybe_run_snapshot_create_hook({:copied, kind}) do
        {:cont, :ok}
      else
        {:error, {:snapshot_create_hook, _reason}} = error -> {:halt, error}
        {:error, reason} -> {:halt, {:error, {kind, reason}}}
      end
    end)
  end

  defp flush_apply_projection_snapshot_payload(%{ctx: ctx, shard_index: shard_index}) do
    flush_apply_projection_snapshot_payload(
      ctx.data_dir,
      shard_index,
      apply_projection_snapshot_spill_chunk_entries()
    )
  end

  defp flush_apply_projection_snapshot_payload(data_dir, shard_index, chunk_entries) do
    case Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, shard_index) do
      0 ->
        :ok

      _remaining ->
        case Ferricstore.Raft.WARaftSegmentReader.spill_apply_projection_cache(
               data_dir,
               shard_index,
               chunk_entries
             ) do
          {:ok, removed} when is_integer(removed) and removed > 0 ->
            flush_apply_projection_snapshot_payload(data_dir, shard_index, chunk_entries)

          {:ok, 0} ->
            {:error, {:flush_apply_projection_snapshot_payload, :no_progress}}

          {:error, reason} ->
            {:error, {:flush_apply_projection_snapshot_payload, reason}}

          other ->
            {:error, {:flush_apply_projection_snapshot_payload, other}}
        end
    end
  end

  defp apply_projection_snapshot_spill_chunk_entries do
    case Application.get_env(:ferricstore, :waraft_apply_projection_snapshot_spill_chunk_entries) do
      value when is_integer(value) and value > 0 ->
        value

      _other ->
        case apply_projection_cache_max_entries() do
          value when is_integer(value) and value > 0 -> value
          _disabled_or_invalid -> 16_384
        end
    end
  end

  defp drain_apply_projection_cache_compaction_for_snapshot(%{
         apply_projection_cache_compaction: %{pid: pid}
       })
       when is_pid(pid) do
    if Process.alive?(pid) do
      ref = Process.monitor(pid)

      receive do
        {:DOWN, ^ref, :process, ^pid, :normal} ->
          :ok

        {:DOWN, ^ref, :process, ^pid, reason} ->
          {:error, {:apply_projection_cache_compaction_snapshot_drain_failed, reason}}
      after
        snapshot_compaction_drain_timeout_ms() ->
          Process.demonitor(ref, [:flush])
          {:error, :apply_projection_cache_compaction_snapshot_drain_timeout}
      end
    else
      :ok
    end
  end

  defp drain_apply_projection_cache_compaction_for_snapshot(_handle), do: :ok

  defp snapshot_compaction_drain_timeout_ms do
    Application.get_env(
      :ferricstore,
      :waraft_snapshot_compaction_drain_timeout_ms,
      @default_snapshot_compaction_drain_timeout_ms
    )
  end

  defp create_empty_snapshot_payload_dirs(snapshot_path) do
    Enum.reduce_while(snapshot_payload_kinds(), :ok, fn kind, :ok ->
      case Ferricstore.FS.mkdir_p(Path.join(snapshot_path, Atom.to_string(kind))) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:mkdir_snapshot_dir, kind, reason}}}
      end
    end)
  end

  defp create_empty_snapshot_storage_payload_dirs(snapshot_path) do
    Enum.reduce_while(snapshot_storage_payload_kinds(), :ok, fn kind, :ok ->
      case Ferricstore.FS.mkdir_p(Path.join(snapshot_path, Atom.to_string(kind))) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:mkdir_snapshot_dir, kind, reason}}}
      end
    end)
  end

  defp copy_snapshot_to_shard_dirs(
         snapshot_path,
         handle,
         snapshot_position,
         metadata
       ) do
    install_id = System.unique_integer([:positive])
    payload_dirs = Map.get(metadata, :payload_dirs, snapshot_payload_kinds())
    storage_payload_dirs = Map.get(metadata, :storage_payload_dirs, [])
    specs = snapshot_install_dir_specs(handle, payload_dirs, storage_payload_dirs)

    empty_payload_dirs =
      Map.get(metadata, :empty_payload_dirs, []) ++
        Map.get(metadata, :empty_storage_payload_dirs, [])

    install = %{
      root_dir: handle.root_dir,
      snapshot_position: snapshot_position,
      staging_root: Path.join(handle.root_dir, "snapshot_install_staging.#{install_id}"),
      backup_root: Path.join(handle.root_dir, "snapshot_install_backup.#{install_id}"),
      payload_dirs: payload_dirs,
      storage_payload_dirs: storage_payload_dirs
    }

    result =
      with :ok <-
             stage_snapshot_dirs(snapshot_path, install.staging_root, specs, empty_payload_dirs),
           :ok <- write_snapshot_install_marker(install),
           :ok <-
             swap_staged_snapshot_dirs(
               install.staging_root,
               install.backup_root,
               handle,
               specs
             ) do
        {:ok, install}
      end

    case result do
      {:ok, _install} ->
        result

      {:error, _reason} = error ->
        if Ferricstore.FS.exists?(snapshot_install_marker_path(install.root_dir)) do
          _ = rollback_snapshot_install(install, handle)
        else
          _ = cleanup_snapshot_install(install)
        end

        error
    end
  end

  defp recover_pending_snapshot_install(root_dir, ctx, shard_index) do
    case read_snapshot_install_marker(root_dir) do
      :none ->
        :ok

      {:ok, install} ->
        metadata = read_snapshot_install_recovery_metadata(root_dir, ctx, shard_index)

        case metadata do
          %{position: position} when position == install.snapshot_position ->
            finish_persisted_snapshot_install(install)

          %{position: position} ->
            handle = %{ctx: ctx, shard_index: shard_index, root_dir: root_dir}

            case snapshot_install_backup_status(install, handle) do
              status when status in [:complete, :partial_recoverable] ->
                rollback_snapshot_install(install, handle)

              :not_started ->
                if snapshot_install_staging_present?(install) do
                  finalize_snapshot_install(install)
                else
                  {:error,
                   {:snapshot_install_position_mismatch, position, install.snapshot_position}}
                end

              :incomplete ->
                {:error,
                 {:snapshot_install_position_mismatch, position, install.snapshot_position}}

              {:error, reason} ->
                {:error, reason}
            end

          empty when is_map(empty) and map_size(empty) == 0 ->
            handle = %{ctx: ctx, shard_index: shard_index, root_dir: root_dir}

            case snapshot_install_backup_status(install, handle) do
              status when status in [:complete, :partial_recoverable] ->
                rollback_snapshot_install(install, handle)

              status when status in [:incomplete, :not_started] ->
                {:error,
                 {:snapshot_install_missing_metadata_without_backup, install.snapshot_position}}

              {:error, reason} ->
                {:error, reason}
            end

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_snapshot_install_recovery_metadata(root_dir, ctx, shard_index) do
    path = metadata_path(root_dir)

    case read_storage_metadata_file(path, :storage_metadata_file_too_large) do
      {:error, :enoent} ->
        case recover_storage_metadata(path, :missing_current_storage_metadata) do
          {:ok, metadata} -> metadata
          {:error, _reason} -> %{}
        end

      _other ->
        read_metadata!(path, ctx, shard_index)
    end
  rescue
    error -> {:error, error}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp write_snapshot_install_marker(install) do
    marker = %{
      version: @version,
      snapshot_position: install.snapshot_position,
      staging_root: install.staging_root,
      backup_root: install.backup_root,
      payload_dirs: Map.get(install, :payload_dirs, snapshot_payload_kinds()),
      storage_payload_dirs: Map.get(install, :storage_payload_dirs, [])
    }

    atomic_write_term(snapshot_install_marker_path(install.root_dir), marker)
  end

  defp read_snapshot_install_marker(root_dir) do
    path = snapshot_install_marker_path(root_dir)

    case read_snapshot_install_marker_file(path) do
      {:ok, binary} ->
        case persisted_binary_to_term(binary) do
          {:ok,
           %{
             version: @version,
             snapshot_position: position,
             staging_root: staging_root,
             backup_root: backup_root
           } = marker}
          when is_binary(staging_root) and is_binary(backup_root) ->
            with :ok <- validate_raft_position(position),
                 :ok <-
                   validate_snapshot_payload_dir_list(
                     :payload_dirs,
                     Map.get(marker, :payload_dirs, snapshot_payload_kinds())
                   ),
                 :ok <-
                   validate_snapshot_storage_payload_dir_list(
                     :storage_payload_dirs,
                     Map.get(marker, :storage_payload_dirs, [])
                   ),
                 :ok <-
                   validate_snapshot_install_marker_path(
                     root_dir,
                     staging_root,
                     "snapshot_install_staging."
                   ),
                 :ok <-
                   validate_snapshot_install_marker_path(
                     root_dir,
                     backup_root,
                     "snapshot_install_backup."
                   ) do
              {:ok,
               %{
                 root_dir: root_dir,
                 snapshot_position: position,
                 staging_root: staging_root,
                 backup_root: backup_root,
                 payload_dirs: Map.get(marker, :payload_dirs, snapshot_payload_kinds()),
                 storage_payload_dirs: Map.get(marker, :storage_payload_dirs, [])
               }}
            else
              {:error, reason} -> {:error, {:bad_snapshot_install_marker, reason}}
            end

          {:ok, other} ->
            {:error, {:bad_snapshot_install_marker, other}}

          {:error, reason} ->
            {:error, {:decode_snapshot_install_marker, reason}}
        end

      {:error, :enoent} ->
        :none

      {:error, reason} ->
        {:error, {:read_snapshot_install_marker, reason}}
    end
  end

  defp read_snapshot_install_marker_file(path) do
    read_bounded_metadata_file(
      path,
      @max_snapshot_install_marker_bytes,
      :snapshot_install_marker_file_too_large
    )
  end

  defp snapshot_install_marker_path(root_dir),
    do: Path.join(root_dir, @snapshot_install_marker_file)

  defp validate_snapshot_install_marker_path(root_dir, path, prefix) do
    root_dir = Path.expand(root_dir)
    path = Path.expand(path)

    if Path.dirname(path) == root_dir and String.starts_with?(Path.basename(path), prefix) do
      :ok
    else
      {:error, {:bad_snapshot_install_path, path}}
    end
  end

  defp finalize_snapshot_install(install) do
    with :ok <- cleanup_snapshot_install(install) do
      case Ferricstore.FS.rm(snapshot_install_marker_path(install.root_dir)) do
        :ok -> fsync_dir(install.root_dir)
        {:error, {:not_found, _}} -> :ok
        {:error, reason} -> {:error, {:remove_snapshot_install_marker, reason}}
      end
    end
  end

  defp finish_persisted_snapshot_install(install) do
    with :ok <-
           reset_segment_log_to_snapshot_boundary(install.root_dir, install.snapshot_position),
         :ok <- clear_snapshot_boundary_metadata(install.root_dir, install.snapshot_position) do
      finalize_snapshot_install(install)
    end
  end

  defp reset_segment_log_to_snapshot_boundary(root_dir, position) do
    case :ferricstore_waraft_spike_segment_log.reset_disk_to_position(
           to_charlist(root_dir),
           position
         ) do
      :ok -> :ok
      {:error, reason} -> {:error, {:reset_segment_log_to_snapshot_boundary, reason}}
      other -> {:error, {:reset_segment_log_to_snapshot_boundary, other}}
    end
  end

  defp clear_snapshot_boundary_metadata(root_dir, position) do
    path = metadata_path(root_dir)

    case read_metadata_if_present(path) do
      %{position: ^position, snapshot_boundary_position: ^position} = metadata ->
        with :ok <-
               persist_storage_metadata(
                 root_dir,
                 Map.delete(metadata, :snapshot_boundary_position),
                 :compact
               ) do
          delete_metadata_journal(path)
        end

      %{position: ^position} ->
        delete_metadata_journal(path)

      %{position: _position} ->
        :ok

      empty when is_map(empty) and map_size(empty) == 0 ->
        :ok

      {:error, reason} ->
        {:error, {:clear_snapshot_boundary_metadata, reason}}
    end
  end

  defp finalize_snapshot_install_marker_if_matching(root_dir, position) do
    case read_snapshot_install_marker(root_dir) do
      {:ok, %{snapshot_position: ^position} = install} -> finalize_snapshot_install(install)
      {:ok, _install} -> :ok
      :none -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp snapshot_install_staging_present?(install) do
    case File.lstat(install.staging_root) do
      {:ok, %{type: :directory}} -> true
      _other -> false
    end
  end

  defp cleanup_snapshot_install(install) do
    with :ok <- cleanup_snapshot_install_path(:staging, install.staging_root),
         :ok <- cleanup_snapshot_install_path(:backup, install.backup_root) do
      :ok
    end
  end

  defp cleanup_snapshot_install_path(kind, path) do
    with :ok <- maybe_run_snapshot_cleanup_hook({:remove, kind, path}) do
      case Ferricstore.FS.rm_rf(path) do
        :ok -> :ok
        {:error, reason} -> {:error, {:cleanup_snapshot_install, kind, path, reason}}
      end
    end
  end

  defp rollback_snapshot_install(install, handle) do
    specs =
      snapshot_install_dir_specs(
        handle,
        Map.get(install, :payload_dirs, snapshot_payload_kinds()),
        Map.get(install, :storage_payload_dirs, [])
      )

    with :ok <- rollback_snapshot_swap(specs, install.backup_root),
         :ok <- restore_segment_projection_from_backup(install.root_dir, install.backup_root),
         :ok <- fsync_snapshot_parent_dirs(specs) do
      finalize_snapshot_install(install)
    end
  end

  defp rollback_snapshot_install_and_restore_runtime(install, handle) do
    result = rollback_snapshot_install(install, handle)
    _ = rebuild_runtime_after_snapshot_rollback(handle)
    result
  end

  defp rebuild_runtime_after_snapshot_rollback(%{ctx: ctx, shard_index: shard_index} = handle) do
    metadata =
      handle
      |> Map.get(:root_dir)
      |> rollback_rebuild_metadata(handle)

    _ =
      ctx
      |> build_sm_state(shard_index)
      |> maybe_recover_segment_projected!(Map.get(handle, :root_dir), metadata)

    :ok
  rescue
    _ -> :ok
  end

  defp rebuild_runtime_after_snapshot_rollback(_handle), do: :ok

  defp rollback_rebuild_metadata(nil, handle),
    do: %{position: Map.get(handle, :position, @zero_pos)}

  defp rollback_rebuild_metadata(root_dir, handle) do
    case read_metadata_if_present(metadata_path(root_dir)) do
      %{position: _position} = metadata ->
        metadata

      _missing_or_bad ->
        %{position: latest_segment_log_position(root_dir, Map.get(handle, :position, @zero_pos))}
    end
  end

  defp latest_segment_log_position(root_dir, fallback) do
    case :ferricstore_waraft_spike_segment_log.fold_disk(
           to_charlist(root_dir),
           fn
             index, {term, _entry}, acc when is_integer(index) and is_integer(term) ->
               max_raft_position(acc, {:raft_log_pos, index, term})

             _index, _entry, acc ->
               acc
           end,
           fallback
         ) do
      {:ok, position} -> position
      {:error, _reason} -> fallback
    end
  end

  defp max_raft_position(
         {:raft_log_pos, left_index, left_term} = left,
         {:raft_log_pos, right_index, right_term} = right
       )
       when is_integer(left_index) and is_integer(right_index) and is_integer(left_term) and
              is_integer(right_term) do
    if {right_index, right_term} > {left_index, left_term}, do: right, else: left
  end

  defp max_raft_position(_left, right), do: right

  defp snapshot_install_backup_status(install, handle) do
    specs =
      snapshot_install_dir_specs(
        handle,
        Map.get(install, :payload_dirs, snapshot_payload_kinds()),
        Map.get(install, :storage_payload_dirs, [])
      )

    case File.lstat(install.backup_root) do
      {:error, :enoent} ->
        if snapshot_live_dirs_intact?(specs), do: :not_started, else: :incomplete

      {:ok, %{type: :directory}} ->
        snapshot_install_backup_dir_status(install, specs)

      {:ok, %{type: type}} ->
        {:error, {:unsafe_snapshot_backup_root, install.backup_root, type}}

      {:error, reason} ->
        {:error, {:stat_snapshot_backup_root, install.backup_root, reason}}
    end
  end

  defp snapshot_install_backup_dir_status(install, specs) do
    specs
    |> Enum.reduce_while({0, 0, 0}, fn {kind, dest}, {present, missing, unrecoverable} ->
      path = Path.join(install.backup_root, Atom.to_string(kind))

      case File.lstat(path) do
        {:ok, %{type: :directory}} ->
          {:cont, {present + 1, missing, unrecoverable}}

        {:ok, %{type: type}} ->
          {:halt, {:error, {:unsafe_snapshot_payload_path, path, type}}}

        {:error, :enoent} ->
          if snapshot_live_dir_intact?(dest) do
            {:cont, {present, missing + 1, unrecoverable}}
          else
            {:cont, {present, missing + 1, unrecoverable + 1}}
          end

        {:error, reason} ->
          {:halt, {:error, {:stat_snapshot_backup_dir, kind, path, reason}}}
      end
    end)
    |> case do
      {present, 0, 0} when present > 0 ->
        :complete

      {0, _missing, 0} ->
        :not_started

      {present, _missing, 0} when present > 0 ->
        :partial_recoverable

      {:error, _reason} = error ->
        error

      {_present, _missing, _unrecoverable} ->
        :incomplete
    end
  end

  defp snapshot_live_dirs_intact?(specs) do
    Enum.all?(specs, fn {_kind, dest} -> snapshot_live_dir_intact?(dest) end)
  end

  defp snapshot_live_dir_intact?(dest) do
    case File.lstat(dest) do
      {:ok, %{type: :directory}} -> true
      _other -> false
    end
  end

  defp shard_dir_specs(%{ctx: ctx, shard_index: shard_index}) do
    [
      data: Ferricstore.DataDir.shard_data_path(ctx.data_dir, shard_index),
      blob: Ferricstore.DataDir.blob_shard_path(ctx.data_dir, shard_index),
      dedicated: Path.join([ctx.data_dir, "dedicated", "shard_#{shard_index}"]),
      prob: Path.join([ctx.data_dir, "prob", "shard_#{shard_index}"])
    ]
  end

  defp shard_dir_specs(handle, kinds) do
    specs = shard_dir_specs(handle)
    Enum.filter(specs, fn {kind, _dest} -> kind in kinds end)
  end

  defp snapshot_payload_kinds, do: [:data, :blob, :dedicated, :prob]

  defp snapshot_storage_payload_kinds, do: [:segment_projection_log, :apply_projection_log]

  defp storage_payload_dir_specs(%{root_dir: root_dir}) do
    [
      segment_projection_log: segment_projection_root(root_dir),
      apply_projection_log: apply_projection_root(root_dir)
    ]
  end

  defp storage_payload_dir_specs(_handle, []), do: []

  defp storage_payload_dir_specs(handle, kinds) do
    specs = storage_payload_dir_specs(handle)
    Enum.filter(specs, fn {kind, _dest} -> kind in kinds end)
  end

  defp snapshot_install_dir_specs(handle, payload_dirs, storage_payload_dirs),
    do:
      shard_dir_specs(handle, payload_dirs) ++
        storage_payload_dir_specs(handle, storage_payload_dirs)

  defp stage_snapshot_dirs(snapshot_path, staging_root, specs, empty_payload_dirs) do
    empty_payload_dirs = MapSet.new(empty_payload_dirs)

    with :ok <- reset_dir(staging_root),
         :ok <-
           Enum.reduce_while(specs, :ok, fn {kind, _dest}, :ok ->
             with :ok <-
                    stage_snapshot_dir(
                      snapshot_path,
                      staging_root,
                      kind,
                      MapSet.member?(empty_payload_dirs, kind)
                    ),
                  :ok <- maybe_run_snapshot_install_hook({:staged, kind}) do
               {:cont, :ok}
             else
               {:error, {:snapshot_install_hook, _reason}} = error -> {:halt, error}
               {:error, reason} -> {:halt, {:error, {kind, reason}}}
             end
           end),
         :ok <- fsync_dir(staging_root) do
      :ok
    end
  end

  defp stage_snapshot_dir(snapshot_path, staging_root, kind, allow_missing_empty?) do
    source = Path.join(snapshot_path, Atom.to_string(kind))
    staged = Path.join(staging_root, Atom.to_string(kind))

    case File.lstat(source) do
      {:ok, %{type: :directory}} ->
        copy_dir(source, staged)

      {:ok, %{type: type}} ->
        {:error, {:source_not_directory, source, type}}

      {:error, :enoent} when allow_missing_empty? ->
        with :ok <- reset_dir(staged) do
          fsync_dir(staged)
        end

      {:error, reason} ->
        {:error, {:stat_source_dir, source, reason}}
    end
  end

  defp swap_staged_snapshot_dirs(staging_root, backup_root, handle, specs) do
    with :ok <- reset_dir(backup_root),
         :ok <- maybe_backup_segment_projection(handle.root_dir, backup_root, specs),
         :ok <- move_live_dirs_to_backup(specs, backup_root),
         :ok <- move_staged_dirs_live(specs, staging_root) do
      fsync_snapshot_parent_dirs(specs)
    else
      {:error, reason} = error ->
        {:error, reason || error}
    end
  end

  defp backup_segment_projection(root_dir, backup_root) do
    source = segment_projection_root(root_dir)
    backup = segment_projection_backup_path(backup_root)

    copy_dir(source, backup)
  end

  defp maybe_backup_segment_projection(root_dir, backup_root, specs) do
    if Enum.any?(specs, fn {kind, _dest} -> kind == :segment_projection_log end) do
      :ok
    else
      backup_segment_projection(root_dir, backup_root)
    end
  end

  defp restore_segment_projection_from_backup(root_dir, backup_root) do
    backup = segment_projection_backup_path(backup_root)
    dest = segment_projection_root(root_dir)

    case File.lstat(backup) do
      {:ok, %{type: :directory}} ->
        with :ok <- Ferricstore.FS.rm_rf(dest),
             :ok <- copy_dir(backup, dest) do
          fsync_dir(root_dir)
        else
          {:error, reason} -> {:error, {:rollback_segment_projection, reason}}
        end

      {:ok, %{type: type}} ->
        {:error, {:rollback_segment_projection, {:unsafe_backup_path, backup, type}}}

      {:error, :enoent} ->
        :ok

      {:error, reason} ->
        {:error, {:rollback_segment_projection, {:stat_backup_path, backup, reason}}}
    end
  end

  defp segment_projection_backup_path(backup_root),
    do: Path.join(backup_root, @segment_projection_dir)

  defp move_live_dirs_to_backup(specs, backup_root) do
    Enum.reduce_while(specs, :ok, fn {kind, dest}, :ok ->
      backup = Path.join(backup_root, Atom.to_string(kind))

      case File.lstat(dest) do
        {:ok, %{type: :directory}} ->
          case Ferricstore.FS.rename(dest, backup) do
            :ok -> {:cont, :ok}
            {:error, reason} -> {:halt, {:error, {:backup_live_dir, kind, reason}}}
          end

        {:ok, %{type: :symlink}} ->
          {:halt, {:error, {:unsafe_snapshot_payload_path, dest, :symlink}}}

        {:ok, _stat} ->
          {:halt, {:error, {:backup_live_dir, kind, :not_directory}}}

        {:error, :enoent} ->
          {:cont, :ok}

        {:error, reason} ->
          {:halt, {:error, {:backup_live_dir, kind, reason}}}
      end
    end)
  end

  defp move_staged_dirs_live(specs, staging_root) do
    Enum.reduce_while(specs, :ok, fn {kind, dest}, :ok ->
      staged = Path.join(staging_root, Atom.to_string(kind))

      with :ok <- Ferricstore.FS.mkdir_p(Path.dirname(dest)),
           :ok <- Ferricstore.FS.rename(staged, dest) do
        {:cont, :ok}
      else
        {:error, reason} -> {:halt, {:error, {:promote_staged_dir, kind, reason}}}
      end
    end)
  end

  defp rollback_snapshot_swap(specs, backup_root) do
    Enum.reduce_while(specs, :ok, fn {kind, dest}, :ok ->
      backup = Path.join(backup_root, Atom.to_string(kind))

      case File.lstat(backup) do
        {:ok, %{type: :directory}} ->
          with :ok <- Ferricstore.FS.rm_rf(dest),
               :ok <- Ferricstore.FS.mkdir_p(Path.dirname(dest)),
               :ok <- Ferricstore.FS.rename(backup, dest) do
            {:cont, :ok}
          else
            {:error, reason} -> {:halt, {:error, {:rollback_snapshot_dir, kind, reason}}}
          end

        {:ok, %{type: type}} ->
          {:halt, {:error, {:unsafe_snapshot_payload_path, backup, type}}}

        {:error, :enoent} ->
          {:cont, :ok}

        {:error, reason} ->
          {:halt, {:error, {:rollback_snapshot_dir, kind, reason}}}
      end
    end)
  end

  defp fsync_snapshot_parent_dirs(specs) do
    specs
    |> Enum.map(fn {_kind, dest} -> Path.dirname(dest) end)
    |> Enum.uniq()
    |> Enum.reduce_while(:ok, fn parent, :ok ->
      case fsync_dir(parent) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp maybe_run_snapshot_install_hook(event) do
    case Process.get(:ferricstore_waraft_snapshot_install_hook) do
      fun when is_function(fun, 1) ->
        case fun.(event) do
          :ok -> :ok
          nil -> :ok
          {:error, reason} -> {:error, {:snapshot_install_hook, reason}}
          other -> {:error, {:snapshot_install_hook, other}}
        end

      _other ->
        :ok
    end
  end

  defp maybe_run_snapshot_create_hook(event) do
    case Application.get_env(:ferricstore, :waraft_snapshot_create_hook) do
      fun when is_function(fun, 1) ->
        case fun.(event) do
          :ok -> :ok
          nil -> :ok
          {:error, reason} -> {:error, {:snapshot_create_hook, reason}}
          other -> {:error, {:snapshot_create_hook, other}}
        end

      _other ->
        :ok
    end
  end

  defp maybe_run_snapshot_cleanup_hook(event) do
    case Application.get_env(:ferricstore, :waraft_snapshot_cleanup_hook) do
      fun when is_function(fun, 1) ->
        case fun.(event) do
          :ok -> :ok
          nil -> :ok
          {:error, reason} -> {:error, {:snapshot_cleanup_hook, reason}}
          other -> {:error, {:snapshot_cleanup_hook, other}}
        end

      _other ->
        :ok
    end
  end

  defp copy_dir(source, dest) do
    with :ok <- reset_dir(dest) do
      case File.lstat(source) do
        {:ok, %{type: :directory}} ->
          with {:ok, children} <- Ferricstore.FS.ls(source),
               :ok <-
                 Enum.reduce_while(children, :ok, fn child, :ok ->
                   case copy_snapshot_payload_entry(
                          Path.join(source, child),
                          Path.join(dest, child)
                        ) do
                     :ok -> {:cont, :ok}
                     {:error, _reason} = error -> {:halt, error}
                   end
                 end),
               :ok <- fsync_copied_tree(dest) do
            :ok
          else
            {:error, reason} -> {:error, reason}
          end

        {:ok, %{type: type}} ->
          {:error, {:source_not_directory, source, type}}

        {:error, :enoent} ->
          :ok

        {:error, reason} ->
          {:error, {:stat_source_dir, source, reason}}
      end
    end
  end

  defp copy_snapshot_payload_entry(source, dest) do
    case File.lstat(source) do
      {:ok, %{type: :directory}} ->
        copy_dir(source, dest)

      {:ok, %{type: :regular}} ->
        case File.cp(source, dest) do
          :ok -> :ok
          {:error, reason} -> {:error, {:copy_file, source, reason}}
        end

      {:ok, %{type: type}} ->
        {:error, {:unsafe_snapshot_payload_path, source, type}}

      {:error, reason} ->
        {:error, {:stat_snapshot_payload_path, source, reason}}
    end
  end

  defp fsync_copied_tree(path) do
    case File.lstat(path) do
      {:ok, %{type: :directory}} ->
        with {:ok, children} <- Ferricstore.FS.ls(path),
             :ok <-
               Enum.reduce_while(children, :ok, fn child, :ok ->
                 case fsync_copied_tree(Path.join(path, child)) do
                   :ok -> {:cont, :ok}
                   {:error, _reason} = error -> {:halt, error}
                 end
               end) do
          fsync_dir(path)
        else
          {:error, reason} -> {:error, reason}
        end

      {:ok, %{type: :regular}} ->
        fsync_file(path)

      {:ok, %{type: type}} ->
        {:error, {:unsafe_snapshot_payload_path, path, type}}

      {:error, reason} ->
        {:error, {:stat_copied_path, path, reason}}
    end
  end

  defp fsync_payload_dirs(handle) do
    handle
    |> shard_dir_specs()
    |> Enum.reduce_while(:ok, fn {kind, path}, :ok ->
      case fsync_payload_tree(path) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:fsync_payload, kind, reason}}}
      end
    end)
  end

  defp fsync_payload_tree(path) do
    case File.lstat(path) do
      {:ok, %{type: :directory}} ->
        with {:ok, children} <- Ferricstore.FS.ls(path),
             :ok <-
               Enum.reduce_while(children, :ok, fn child, :ok ->
                 case fsync_payload_tree(Path.join(path, child)) do
                   :ok -> {:cont, :ok}
                   {:error, _reason} = error -> {:halt, error}
                 end
               end) do
          fsync_dir(path)
        else
          {:error, reason} -> {:error, reason}
        end

      {:ok, %{type: :regular}} ->
        fsync_file(path, :waraft_bitcask_payload_fsync_file_hook)

      {:ok, %{type: type}} ->
        {:error, {:unsafe_snapshot_payload_path, path, type}}

      {:error, :enoent} ->
        :ok

      {:error, reason} ->
        {:error, {:stat_payload_path, path, reason}}
    end
  end

  defp fsync_file(path) do
    fsync_file(path, :waraft_snapshot_fsync_file_hook)
  end

  defp fsync_file(path, hook_key) do
    result =
      case Application.get_env(:ferricstore, hook_key) do
        fun when is_function(fun, 1) -> fun.(path)
        _other -> Ferricstore.Bitcask.NIF.v2_fsync(path)
      end

    case result do
      :ok -> :ok
      {:error, reason} -> {:error, {:fsync_file, path, reason}}
      other -> {:error, {:fsync_file, path, other}}
    end
  rescue
    error -> {:error, {:fsync_file_exception, path, error}}
  end

  defp reset_dir(path) do
    case Ferricstore.FS.rm_rf(path) do
      :ok -> Ferricstore.FS.mkdir_p(path)
      {:error, reason} -> {:error, reason}
    end
  end

  defp fsync_dir(path) do
    result =
      case Application.get_env(:ferricstore, :waraft_storage_fsync_dir_hook) do
        fun when is_function(fun, 1) -> fun.(path)
        _other -> Ferricstore.Bitcask.NIF.v2_fsync_dir(path)
      end

    case result do
      :ok -> :ok
      {:error, reason} -> {:error, {:fsync_dir, path, reason}}
      other -> {:error, {:fsync_dir, path, other}}
    end
  rescue
    error -> {:error, {:fsync_dir_exception, path, error}}
  end

  defp to_path(path) when is_binary(path), do: path
  defp to_path(path) when is_list(path), do: List.to_string(path)
end
