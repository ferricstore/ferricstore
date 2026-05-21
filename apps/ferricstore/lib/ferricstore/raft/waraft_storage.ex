defmodule Ferricstore.Raft.WARaftStorage do
  @moduledoc false

  alias Ferricstore.HLC
  alias Ferricstore.Raft.StateMachine
  alias Ferricstore.Store.BlobRef
  alias Ferricstore.Store.BlobStore
  alias Ferricstore.Store.ColdRead
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
  alias Ferricstore.Store.Shard.ZSetIndex

  @metadata_file "ferricstore_storage.term"
  @snapshot_metadata_file "ferricstore_snapshot.term"
  @segment_projection_dir "segment_projection_log"
  @snapshot_install_marker_file "snapshot_install.term"
  @metadata_previous_suffix ".previous"
  @metadata_journal_suffix ".journal"
  @metadata_journal_magic "FSMJ1"
  @max_storage_metadata_bytes 1_048_576
  @max_metadata_journal_record_bytes @max_storage_metadata_bytes
  @max_snapshot_metadata_bytes @max_storage_metadata_bytes
  @max_snapshot_install_marker_bytes @max_storage_metadata_bytes
  @version 1
  @default_metadata_persist_every 128
  @default_metadata_compact_every 1024
  @default_payload_fsync_every 20_000
  @cold_read_timeout_ms 10_000
  @zero_pos {:raft_log_pos, 0, 0}
  @encoded_peer_tag :ferricstore_waraft_peer
  @replay_safe_nosync_apply :replay_safe_nosync
  @bitcask_keydir_apply :bitcask_keydir
  @segment_keydir_apply :segment_keydir
  @default_storage_apply @bitcask_keydir_apply
  @segment_projection_registry :ferricstore_waraft_segment_projection_registry
  @storage_root "ferricstore_waraft_backend"

  defguardp valid_segment_backed_file_id(file_id)
            when is_tuple(file_id) and tuple_size(file_id) == 2 and
                   (elem(file_id, 0) == :waraft_segment or
                      elem(file_id, 0) == :waraft_projection) and
                   is_integer(elem(file_id, 1)) and elem(file_id, 1) > 0

  @type handle :: map()

  @spec validate_supported_apply_mode!() :: :ok
  def validate_supported_apply_mode! do
    _ = storage_apply_mode()
    :ok
  end

  @spec open(map(), charlist() | binary()) :: handle()
  def open(options, root_dir) do
    root_dir = to_path(root_dir)
    ctx = Ferricstore.Raft.WARaftBackend.context!(Map.fetch!(options, :table))
    shard_index = Map.fetch!(options, :partition) - 1

    case recover_pending_snapshot_install(root_dir, ctx, shard_index) do
      :ok -> :ok
      {:error, reason} -> raise "failed to recover WARaft snapshot install: #{inspect(reason)}"
    end

    metadata =
      root_dir
      |> metadata_path()
      |> read_metadata!(ctx, shard_index)
      |> ensure_initial_storage_metadata!(root_dir)

    Ferricstore.Raft.WARaftBackend.cache_config(shard_index, Map.get(metadata, :config))

    sm_state =
      ctx
      |> build_sm_state(shard_index)
      |> maybe_recover_segment_projected!(root_dir, metadata)

    %{
      options: options,
      ctx: ctx,
      root_dir: root_dir,
      shard_index: shard_index,
      sm_state: sm_state,
      position: Map.get(metadata, :position, @zero_pos),
      persisted_position: Map.get(metadata, :position, @zero_pos),
      last_clean_position: Map.get(metadata, :position, @zero_pos),
      label: Map.get(metadata, :label),
      config: Map.get(metadata, :config),
      bitcask_dirty?: false
    }
    |> register_segment_projection_context()
  end

  @spec close(handle()) :: :ok | {:error, term()}
  def close(handle) do
    try do
      with :ok <- maybe_fsync_payload_before_metadata(handle) do
        clean_handle = Map.put(handle, :bitcask_dirty?, false)

        if segment_bitcask_apply?() and not segment_keydir_available?(clean_handle) do
          :ok
        else
          persist_metadata(clean_handle, :compact)
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

    if segment_bitcask_apply?() do
      projection_root = segment_projection_root(root_dir)

      with {:ok, context} <- lookup_segment_projection_context(root_dir),
           :ok <- validate_segment_projection_trim_position(context.position, trim_index),
           {:ok, relocations} <-
             collect_segment_projection_relocations(context.ctx, context.shard_index),
           entries = segment_projection_entries_from_relocations(relocations),
           :ok <-
             write_segment_projection(
               projection_root,
               context.position,
               entries
             ),
           :ok <-
             relocate_segment_projection_keydir(
               context.ctx,
               context.shard_index,
               projection_root,
               relocations
             ) do
        :ok
      end
    else
      :ok
    end
  end

  def prepare_segment_projection_for_trim(_root_dir, trim_index),
    do: {:error, {:bad_trim_index, trim_index}}

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

  @spec create_snapshot(charlist() | binary(), handle()) :: :ok | {:error, term()}
  def create_snapshot(_snapshot_path, %{blocked_error: reason} = handle) do
    emit_storage_blocked(handle, reason, handle.position, :blocked_snapshot)
    {:error, {:storage_blocked, reason}}
  end

  def create_snapshot(snapshot_path, handle) do
    snapshot_path = to_path(snapshot_path)

    with :ok <- reset_dir(snapshot_path),
         :ok <- copy_shard_dirs_to_snapshot(snapshot_path, handle),
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
             Map.get(metadata, :empty_payload_dirs, [])
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
        |> Map.put(:label, Map.get(metadata, :label))
        |> Map.put(:config, Map.get(metadata, :config))
        |> Map.put(:bitcask_dirty?, false)

      case persist_metadata(new_handle, :compact) do
        :ok ->
          Ferricstore.Raft.WARaftBackend.cache_config(
            handle.shard_index,
            Map.get(metadata, :config)
          )

          case finalize_snapshot_install(install) do
            :ok ->
              {:ok,
               new_handle |> mark_metadata_persisted() |> register_segment_projection_context()}

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
      empty_payload_dirs: snapshot_payload_kinds()
    }

    with :ok <- reset_dir(snapshot_path),
         :ok <- create_empty_snapshot_payload_dirs(snapshot_path),
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
    if segment_bitcask_apply?() do
      case apply_segment_projected_command(command, position, handle, label_update) do
        :unsupported ->
          apply_state_machine_command_and_persist(command, position, handle, label_update)

        result ->
          result
      end
    else
      apply_state_machine_command_and_persist(command, position, handle, label_update)
    end
  end

  defp apply_state_machine_command_and_persist(
         command,
         position,
         %{sm_state: sm_state} = handle,
         label_update
       ) do
    case apply_state_machine_command(command, position, sm_state) do
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
          |> maybe_mark_bitcask_dirty()
          |> maybe_update_label(label_update)
        )
    end
  end

  defp apply_segment_projected_command(
         command,
         position,
         %{sm_state: sm_state} = handle,
         label_update
       ) do
    case segment_project_command(decoded_replay_command(command), position, sm_state) do
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

  defp segment_project_command({:put, key, value, expire_at_ms}, position, sm_state) do
    if segment_projectable_put?(sm_state, key, value, expire_at_ms) do
      {:ok, segment_project_put(sm_state, key, value, expire_at_ms, position), :ok, 1}
    else
      :unsupported
    end
  end

  defp segment_project_command(
         {:put_blob_ref, key, encoded_ref, expire_at_ms},
         position,
         sm_state
       ) do
    if segment_projectable_blob_ref_put?(key, encoded_ref, expire_at_ms) do
      case verify_segment_blob_refs(sm_state, [encoded_ref]) do
        :ok ->
          {:ok, segment_project_put_blob_ref(sm_state, key, encoded_ref, expire_at_ms, position),
           :ok, 1}

        {:error, _reason} = error ->
          {:ok, sm_state, error, 0}
      end
    else
      :unsupported
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
    {:ok, segment_project_delete(sm_state, key), :ok, 1}
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

    if segment_projectable_compound_put?(redis_key, compound_key, value, expire_at_ms) do
      new_sm_state =
        sm_state
        |> segment_project_put(compound_key, value, expire_at_ms, position)
        |> segment_project_zset_put(redis_key, compound_key, value)

      {:ok, new_sm_state, :ok, 1}
    else
      :unsupported
    end
  end

  defp segment_project_command(
         {:compound_put_blob_ref, compound_key, encoded_ref, expire_at_ms},
         position,
         sm_state
       ) do
    redis_key = if is_binary(compound_key), do: CompoundKey.extract_redis_key(compound_key)

    if segment_projectable_compound_blob_ref_put?(
         redis_key,
         compound_key,
         encoded_ref,
         expire_at_ms
       ) do
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
      :unsupported
    end
  end

  defp segment_project_command({:compound_delete, compound_key}, _position, sm_state)
       when is_binary(compound_key) do
    redis_key = CompoundKey.extract_redis_key(compound_key)

    new_sm_state =
      sm_state
      |> segment_project_delete(compound_key)
      |> segment_project_zset_delete(redis_key, compound_key)

    {:ok, new_sm_state, :ok, 1}
  end

  defp segment_project_command({:put_batch, entries}, position, sm_state) when is_list(entries) do
    if Enum.all?(entries, fn {key, value, expire_at_ms} ->
         segment_projectable_put?(sm_state, key, value, expire_at_ms)
       end) do
      new_sm_state =
        Enum.reduce(entries, sm_state, fn {key, value, expire_at_ms}, acc ->
          segment_project_put(acc, key, value, expire_at_ms, position)
        end)

      {:ok, new_sm_state, {:ok, List.duplicate(:ok, length(entries))}, length(entries)}
    else
      :unsupported
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
    if Enum.all?(entries, &segment_projectable_compound_put?(redis_key, &1)) do
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

      {:error, _reason} = error ->
        {:ok, sm_state, error, 0}
    end
  end

  defp segment_project_command({:delete_batch, keys}, _position, sm_state) when is_list(keys) do
    if Enum.all?(keys, &is_binary/1) do
      new_sm_state = Enum.reduce(keys, sm_state, &segment_project_delete(&2, &1))
      {:ok, new_sm_state, {:ok, List.duplicate(:ok, length(keys))}, length(keys)}
    else
      :unsupported
    end
  end

  defp segment_project_command(
         {:compound_batch_delete, redis_key, compound_keys},
         _position,
         sm_state
       )
       when is_binary(redis_key) and is_list(compound_keys) do
    if Enum.all?(compound_keys, &compound_key_for_redis_key?(redis_key, &1)) do
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
    new_sm_state = segment_project_delete_prefix(sm_state, redis_key, prefix)
    {:ok, new_sm_state, :ok, 1}
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
    commands = Enum.map(commands, &decoded_replay_command/1)

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

  defp segment_project_command(_command, _position, _sm_state), do: :unsupported

  defp segment_projectable_batch_command?(sm_state, {:put, key, value, expire_at_ms}),
    do: segment_projectable_put?(sm_state, key, value, expire_at_ms)

  defp segment_projectable_batch_command?(_sm_state, {:delete, key}), do: is_binary(key)

  defp segment_projectable_batch_command?(
         _sm_state,
         {:compound_put, compound_key, value, expire_at_ms}
       ) do
    redis_key = if is_binary(compound_key), do: CompoundKey.extract_redis_key(compound_key)
    segment_projectable_compound_put?(redis_key, compound_key, value, expire_at_ms)
  end

  defp segment_projectable_batch_command?(_sm_state, {:compound_delete, compound_key}),
    do: is_binary(compound_key)

  defp segment_projectable_batch_command?(_sm_state, {:compound_batch_put, redis_key, entries})
       when is_binary(redis_key) and is_list(entries),
       do: Enum.all?(entries, &segment_projectable_compound_put?(redis_key, &1))

  defp segment_projectable_batch_command?(_sm_state, {:compound_batch_delete, redis_key, keys})
       when is_binary(redis_key) and is_list(keys),
       do: Enum.all?(keys, &compound_key_for_redis_key?(redis_key, &1))

  defp segment_projectable_batch_command?(_sm_state, {:compound_delete_prefix, prefix}),
    do: is_binary(prefix)

  defp segment_projectable_batch_command?(_sm_state, _command), do: false

  defp segment_projectable_put?(_sm_state, key, value, expire_at_ms) do
    is_binary(key) and is_binary(value) and non_neg_integer?(expire_at_ms)
  end

  defp segment_projectable_compound_put?(redis_key, {compound_key, value, expire_at_ms}),
    do: segment_projectable_compound_put?(redis_key, compound_key, value, expire_at_ms)

  defp segment_projectable_compound_put?(redis_key, compound_key, value, expire_at_ms) do
    compound_key_for_redis_key?(redis_key, compound_key) and is_binary(value) and
      non_neg_integer?(expire_at_ms)
  end

  defp segment_projectable_blob_ref_put?(key, encoded_ref, expire_at_ms) do
    is_binary(key) and is_binary(encoded_ref) and non_neg_integer?(expire_at_ms)
  end

  defp segment_projectable_compound_blob_ref_put?(
         redis_key,
         compound_key,
         encoded_ref,
         expire_at_ms
       ) do
    compound_key_for_redis_key?(redis_key, compound_key) and is_binary(encoded_ref) and
      non_neg_integer?(expire_at_ms)
  end

  defp compound_key_for_redis_key?(redis_key, compound_key)
       when is_binary(redis_key) and is_binary(compound_key),
       do: CompoundKey.extract_redis_key(compound_key) == redis_key

  defp compound_key_for_redis_key?(_redis_key, _compound_key), do: false

  defp non_neg_integer?(value), do: is_integer(value) and value >= 0

  defp segment_project_check_key_lock(_sm_state, nil, _owner_ref), do: {:error, :key_locked}

  defp segment_project_check_key_lock(sm_state, key, owner_ref) when is_binary(key) do
    locks = Map.get(sm_state, :cross_shard_locks, %{})

    if map_size(locks) == 0 do
      :ok
    else
      now = HLC.now_ms()

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
    sm_state = segment_project_clear_compound_for_string_put(sm_state, key)
    shard_state = shard_ets_state_from_sm(sm_state)
    true = ShardETS.ets_insert(shard_state, key, value, expire_at_ms)
    install_main_segment_location(sm_state, key, value, position)
    sm_state
  end

  defp segment_project_put_blob_ref(sm_state, key, encoded_ref, expire_at_ms, position) do
    sm_state = segment_project_clear_compound_for_string_put(sm_state, key)
    shard_state = shard_ets_state_from_sm(sm_state)
    file_id = {:waraft_segment, position_index(position)}
    offset = segment_record_offset(sm_state, position)

    true =
      ShardETS.ets_insert_with_location(
        shard_state,
        key,
        nil,
        expire_at_ms,
        file_id,
        offset,
        byte_size(encoded_ref)
      )

    sm_state
  end

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

    if segment_blob_ref_value?(value) do
      true =
        ShardETS.ets_insert_with_location(
          shard_state,
          key,
          nil,
          expire_at_ms,
          {:waraft_projection, projection_index},
          offset,
          byte_size(value)
        )
    else
      true = ShardETS.ets_insert(shard_state, key, value, expire_at_ms)

      install_segment_location(
        sm_state,
        key,
        value,
        {:waraft_projection, projection_index},
        offset
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

  defp install_main_segment_location(sm_state, key, value, position) do
    file_id = {:waraft_segment, position_index(position)}
    offset = segment_record_offset(sm_state, position)
    install_segment_location(sm_state, key, value, file_id, offset)
  end

  defp install_segment_location(%{ets: keydir}, key, value, file_id, offset)
       when valid_segment_backed_file_id(file_id) and is_integer(offset) and offset >= 0 do
    case :ets.lookup(keydir, key) do
      [{^key, ets_value, expire_at_ms, lfu, _file_id, _offset, _value_size}] ->
        :ets.insert(
          keydir,
          {key, ets_value, expire_at_ms, lfu, file_id, offset, byte_size(value)}
        )

      [] ->
        true
    end
  end

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

    if replay_safe_nosync_apply?() do
      StateMachine.apply_waraft_storage_command(command, meta, sm_state)
    else
      StateMachine.apply_standalone_command(command, meta, sm_state)
    end
  end

  defp maybe_mark_bitcask_dirty(handle) do
    if replay_safe_nosync_apply?() do
      %{handle | bitcask_dirty?: true}
    else
      handle
    end
  end

  defp maybe_update_label(handle, :keep_label), do: handle
  defp maybe_update_label(handle, {:replace_label, label}), do: %{handle | label: label}

  defp maybe_put_status(status, _key, nil), do: status
  defp maybe_put_status(status, key, value), do: [{key, value} | status]

  defp durable_position(%{bitcask_dirty?: true} = handle) do
    Map.get(
      handle,
      :last_clean_position,
      Map.get(handle, :persisted_position, Map.get(handle, :position))
    )
  end

  defp durable_position(handle), do: Map.get(handle, :position)

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

    case persist_metadata_for_hot_position(old_handle, new_handle) do
      {:ok, persisted_handle} ->
        {result, mark_metadata_persisted(persisted_handle)}

      :skipped ->
        {result, maybe_mark_clean_position(new_handle)}

      {:error, reason} ->
        {{:error, reason}, block_storage(old_handle, reason, position, :metadata_failure)}
    end
  end

  defp persist_metadata_for_hot_position(_old_handle, new_handle) do
    cond do
      replay_safe_nosync_apply?() ->
        persist_replay_safe_nosync_position(new_handle)

      storage_metadata_persist_due?(new_handle) ->
        case persist_metadata(new_handle) do
          :ok -> {:ok, new_handle}
          {:error, _reason} = error -> error
        end

      true ->
        :skipped
    end
  end

  defp persist_replay_safe_nosync_position(%{bitcask_dirty?: true} = new_handle) do
    if replay_safe_nosync_frontier_due?(new_handle) do
      with :ok <- maybe_fsync_payload_before_metadata(new_handle),
           persisted_handle = Map.put(new_handle, :bitcask_dirty?, false),
           :ok <- persist_metadata(persisted_handle) do
        {:ok, persisted_handle}
      end
    else
      :skipped
    end
  end

  defp persist_replay_safe_nosync_position(_new_handle), do: :skipped

  defp mark_metadata_persisted(%{position: position} = handle),
    do: %{handle | persisted_position: position, last_clean_position: position}

  defp maybe_mark_clean_position(%{bitcask_dirty?: false, position: position} = handle),
    do: %{handle | last_clean_position: position}

  defp maybe_mark_clean_position(handle), do: handle

  defp storage_metadata_persist_due?(new_handle) do
    interval =
      Application.get_env(
        :ferricstore,
        :waraft_storage_metadata_persist_every,
        @default_metadata_persist_every
      )

    case {interval, Map.get(new_handle, :position)} do
      {:never, _position} ->
        false

      {interval, {:raft_log_pos, index, _term}} when is_integer(interval) and interval > 0 ->
        rem(index, interval) == 0

      {_other, _position} ->
        true
    end
  end

  defp replay_safe_nosync_apply? do
    storage_apply_mode() in [@bitcask_keydir_apply, @replay_safe_nosync_apply]
  end

  defp segment_bitcask_apply?, do: storage_apply_mode() == @segment_keydir_apply

  defp storage_apply_mode do
    case Application.get_env(:ferricstore, :waraft_storage_apply_mode, @default_storage_apply) do
      :segment_bitcask ->
        raise ArgumentError,
              "WARaft segment projection storage mode was removed; use :bitcask_keydir"

      :segment_projected ->
        raise ArgumentError,
              "WARaft segment projection storage mode was removed; use :bitcask_keydir"

      @segment_keydir_apply ->
        @segment_keydir_apply

      @bitcask_keydir_apply ->
        @bitcask_keydir_apply

      @replay_safe_nosync_apply ->
        @replay_safe_nosync_apply

      mode ->
        raise ArgumentError,
              "unsupported WARaft storage apply mode #{inspect(mode)}; expected :bitcask_keydir, :replay_safe_nosync, or :segment_keydir"
    end
  end

  defp register_segment_projection_context(%{root_dir: root_dir} = handle) do
    if segment_bitcask_apply?() do
      ensure_segment_projection_registry!()
      key = segment_projection_registry_key(root_dir)

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
    end

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

  defp replay_safe_nosync_frontier_due?(new_handle) do
    interval =
      Application.get_env(
        :ferricstore,
        :waraft_bitcask_payload_fsync_every,
        @default_payload_fsync_every
      )

    case {interval, Map.get(new_handle, :position)} do
      {:never, _position} ->
        false

      {interval, {:raft_log_pos, index, _term}} when is_integer(interval) and interval > 0 ->
        rem(index, interval) == 0

      {_other, _position} ->
        true
    end
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

    {flow_index, flow_lookup} = Ferricstore.Flow.Index.table_names(instance_name, shard_index)
    ensure_ets_table!(flow_index, :ordered_set)
    ensure_ets_table!(flow_lookup, :set)
    ensure_native_flow_index!(flow_index, flow_lookup)

    Ferricstore.Flow.LMDBRebuilder.rebuild_active_indexes_from_keydir(
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
    if segment_bitcask_apply?() do
      position = Map.get(metadata, :position, @zero_pos)

      with {:ok, projected_sm_state, replay_after_index} <-
             recover_segment_projection_log(root_dir, sm_state, position),
           {:ok, recovered_sm_state} <-
             recover_segment_projected_keydir(
               root_dir,
               projected_sm_state,
               position,
               replay_after_index
             ) do
        recovered_sm_state
      else
        {:error, reason} ->
          raise "failed to recover WARaft segment-backed keydir: #{inspect(reason)}"
      end
    else
      sm_state
    end
  end

  defp recover_segment_projection_log(root_dir, sm_state, metadata_position) do
    projection_root = segment_projection_root(root_dir)

    case read_segment_projection_log(projection_root) do
      {:ok, projection} ->
        with {:ok, entries} <- validate_segment_projection_entries(projection) do
          case compare_positions(projection.position, metadata_position) do
            :gt ->
              {:ok, sm_state, 0}

            _order ->
              {:ok, apply_segment_projection_entries(sm_state, projection_root, entries),
               position_index(projection.position)}
          end
        end

      {:error, :enoent} ->
        {:ok, sm_state, 0}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp recover_segment_projected_keydir(
         _root_dir,
         sm_state,
         {:raft_log_pos, index, _term},
         _after
       )
       when is_integer(index) and index <= 0,
       do: {:ok, sm_state}

  defp recover_segment_projected_keydir(
         root_dir,
         sm_state,
         {:raft_log_pos, durable_index, _term},
         replay_after_index
       )
       when is_integer(durable_index) and durable_index > 0 do
    :ferricstore_waraft_spike_segment_log.fold_disk(
      root_dir,
      fn
        index, _entry, acc when index > durable_index ->
          acc

        index, _entry, acc when index <= replay_after_index ->
          acc

        index, {term, _op} = entry, acc ->
          case command_from_segment_log_entry(entry) do
            {:ok, command} ->
              position = {:raft_log_pos, index, term}

              case segment_project_command(decoded_replay_command(command), position, acc) do
                {:ok, next_acc, _result, _applied_increment} -> next_acc
                :unsupported -> acc
              end

            :skip ->
              acc
          end
      end,
      sm_state
    )
  end

  defp recover_segment_projected_keydir(_root_dir, sm_state, _position, _after),
    do: {:ok, sm_state}

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
    case Ferricstore.Flow.NativeOrderedIndex.get(flow_index, flow_lookup) do
      nil ->
        Ferricstore.Flow.NativeOrderedIndex.register(
          flow_index,
          flow_lookup,
          Ferricstore.Flow.NativeOrderedIndex.new()
        )

      _resource ->
        :ok
    end
  end

  defp persist_metadata(handle, mode \\ :normal)

  defp persist_metadata(%{root_dir: root_dir} = handle, mode) do
    with :ok <- maybe_persist_segment_projection(handle) do
      persist_storage_metadata(root_dir, storage_metadata(handle), mode)
    end
  end

  defp storage_metadata(handle) do
    %{
      version: @version,
      position: handle.position,
      label: handle.label,
      config: handle.config
    }
  end

  defp maybe_persist_segment_projection(%{
         root_dir: root_dir,
         position: position,
         sm_state: sm_state
       }) do
    if segment_bitcask_apply?() do
      with {:ok, entries} <- collect_segment_projected_entries_strict(sm_state) do
        root_dir
        |> segment_projection_root()
        |> write_segment_projection(position, entries)
      end
    else
      :ok
    end
  end

  defp maybe_persist_segment_projection(_handle), do: :ok

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

    with {:ok, payload} <- encode_storage_metadata(metadata),
         :ok <- append_metadata_journal_payload(path, payload) do
      if mode == :compact or storage_metadata_compaction_due?(metadata) do
        atomic_write_binary(path, payload)
      else
        :ok
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
        {source_path, metadata} = newest_storage_metadata_candidate(candidates)
        emit_storage_metadata_recovered(path, source_path, reason)
        {:ok, metadata}
    end
  end

  defp prefer_newest_storage_metadata(path, current_metadata) do
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

  defp compare_positions(
         {:raft_log_pos, left_index, left_term},
         {:raft_log_pos, right_index, right_term}
       )
       when is_integer(left_index) and is_integer(left_term) and is_integer(right_index) and
              is_integer(right_term) do
    cond do
      {left_index, left_term} < {right_index, right_term} -> :lt
      {left_index, left_term} == {right_index, right_term} -> :eq
      true -> :gt
    end
  end

  defp compare_positions(_left, _right), do: :gt

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
         :ok <- validate_storage_config(config) do
      {:ok, metadata}
    else
      {:error, reason} -> {:error, {:bad_storage_metadata, reason}}
    end
  end

  defp validate_storage_metadata(%{position: position} = metadata) when is_map(metadata) do
    case validate_raft_position(position) do
      :ok -> {:ok, metadata}
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

  defp maybe_write_snapshot_segment_projection(snapshot_path, handle) do
    if segment_bitcask_apply?() do
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
    else
      {:ok, nil}
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

    case keydir_rows(keydir) do
      :unavailable ->
        {:error, {:segment_keydir_unavailable, shard_index}}

      rows ->
        rows
        |> Enum.reduce_while({:ok, []}, fn
          row, {:ok, acc} ->
            case segment_projection_entry_from_keydir_row(row, ctx, shard_index, now) do
              {:ok, entry} -> {:cont, {:ok, [entry | acc]}}
              :skip -> {:cont, {:ok, acc}}
              {:error, reason} -> {:halt, {:error, reason}}
            end
        end)
        |> case do
          {:ok, entries} ->
            {:ok, Enum.sort_by(entries, fn {key, _value, _expire_at_ms} -> key end)}

          {:error, _reason} = error ->
            error
        end
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

    case keydir_rows(keydir) do
      :unavailable ->
        {:error, {:segment_keydir_unavailable, shard_index}}

      rows ->
        rows
        |> Enum.reduce_while({:ok, []}, fn row, {:ok, acc} ->
          case segment_projection_entry_from_keydir_row(row, ctx, shard_index, now) do
            {:ok, entry} -> {:cont, {:ok, [{entry, row} | acc]}}
            :skip -> {:cont, {:ok, acc}}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end)
        |> case do
          {:ok, relocations} ->
            {:ok, Enum.sort_by(relocations, fn {{key, _value, _expire_at_ms}, _row} -> key end)}

          {:error, _reason} = error ->
            error
        end
    end
  rescue
    error -> {:error, {:collect_segment_projection_relocations_failed, error}}
  end

  defp collect_segment_projection_relocations(_sm_state),
    do: {:error, :bad_segment_projection_state}

  defp segment_projection_entries_from_relocations(relocations) do
    Enum.map(relocations, fn {entry, _row} -> entry end)
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
             byte_size(projected_value)}
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

    ColdRead.pread_at(path, offset, key, @cold_read_timeout_ms)
  end

  defp read_keydir_cold_value(ctx, shard_index, key, {:flow_history, file_id} = location, offset)
       when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 do
    path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> ShardETS.file_path(location)

    ColdRead.pread_at(path, offset, key, @cold_read_timeout_ms)
  end

  defp read_keydir_cold_value(_ctx, _shard_index, _key, file_id, _offset),
    do: {:error, {:unsupported_segment_projection_location, file_id}}

  defp segment_keydir_available?(%{sm_state: %{ets: keydir}}), do: :ets.info(keydir) != :undefined
  defp segment_keydir_available?(_handle), do: false

  defp keydir_rows(keydir) do
    case :ets.info(keydir) do
      :undefined -> :unavailable
      _info -> :ets.tab2list(keydir)
    end
  end

  defp live_expire_at?(0, _now), do: true
  defp live_expire_at?(expire_at_ms, now) when is_integer(expire_at_ms), do: expire_at_ms > now
  defp live_expire_at?(_expire_at_ms, _now), do: false

  defp write_snapshot_metadata(snapshot_path, handle, segment_projection \\ nil) do
    with {:ok, empty_payload_dirs} <- empty_snapshot_payload_kinds(snapshot_path) do
      metadata =
        %{
          version: @version,
          position: handle.position,
          label: handle.label,
          config: handle.config,
          payload_dirs: snapshot_payload_kinds(),
          empty_payload_dirs: empty_payload_dirs
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

  defp verify_snapshot_position(%{position: expected}, expected), do: :ok

  defp verify_snapshot_position(%{position: actual}, expected),
    do: {:error, {:bad_position, actual, expected}}

  defp verify_snapshot_payload_dirs(metadata, snapshot_path, handle) do
    specs = shard_dir_specs(handle)
    empty_payload_dirs = Map.get(metadata, :empty_payload_dirs, [])

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
    snapshot_payload_kinds()
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
    if segment_bitcask_apply?() do
      storage_root
      |> Path.join("segment_log")
      |> dir_payload_empty()
    else
      {:ok, true}
    end
  end

  defp payload_children_empty(_path, []), do: {:ok, true}

  defp payload_children_empty(path, [child | rest]) do
    child_path = Path.join(path, child)

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

  defp create_empty_snapshot_payload_dirs(snapshot_path) do
    Enum.reduce_while(snapshot_payload_kinds(), :ok, fn kind, :ok ->
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
         empty_payload_dirs
       ) do
    install_id = System.unique_integer([:positive])

    install = %{
      root_dir: handle.root_dir,
      snapshot_position: snapshot_position,
      staging_root: Path.join(handle.root_dir, "snapshot_install_staging.#{install_id}"),
      backup_root: Path.join(handle.root_dir, "snapshot_install_backup.#{install_id}")
    }

    result =
      with :ok <-
             stage_snapshot_dirs(snapshot_path, install.staging_root, handle, empty_payload_dirs),
           :ok <- write_snapshot_install_marker(install),
           :ok <- swap_staged_snapshot_dirs(install.staging_root, install.backup_root, handle) do
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
        metadata = read_metadata_if_present(metadata_path(root_dir))

        case metadata do
          %{position: position} when position == install.snapshot_position ->
            finalize_snapshot_install(install)

          %{position: position} ->
            handle = %{ctx: ctx, shard_index: shard_index}

            case snapshot_install_backup_status(install, handle) do
              :complete ->
                rollback_snapshot_install(install, handle)

              :incomplete ->
                {:error,
                 {:snapshot_install_position_mismatch, position, install.snapshot_position}}

              {:error, reason} ->
                {:error, reason}
            end

          empty when is_map(empty) and map_size(empty) == 0 ->
            handle = %{ctx: ctx, shard_index: shard_index}

            case snapshot_install_backup_status(install, handle) do
              :complete ->
                rollback_snapshot_install(install, handle)

              :incomplete ->
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

  defp write_snapshot_install_marker(install) do
    marker = %{
      version: @version,
      snapshot_position: install.snapshot_position,
      staging_root: install.staging_root,
      backup_root: install.backup_root
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
           }}
          when is_binary(staging_root) and is_binary(backup_root) ->
            with :ok <- validate_raft_position(position),
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
                 backup_root: backup_root
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
    specs = shard_dir_specs(handle)

    with :ok <- rollback_snapshot_swap(specs, install.backup_root),
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
    handle
    |> shard_dir_specs()
    |> Enum.reduce_while(:complete, fn {kind, _dest}, :complete ->
      path = Path.join(install.backup_root, Atom.to_string(kind))

      case File.lstat(path) do
        {:ok, %{type: :directory}} ->
          {:cont, :complete}

        {:ok, %{type: type}} ->
          {:halt, {:error, {:unsafe_snapshot_payload_path, path, type}}}

        {:error, :enoent} ->
          {:halt, :incomplete}

        {:error, reason} ->
          {:halt, {:error, {:stat_snapshot_backup_dir, kind, path, reason}}}
      end
    end)
  end

  defp shard_dir_specs(%{ctx: ctx, shard_index: shard_index}) do
    base = [
      data: Ferricstore.DataDir.shard_data_path(ctx.data_dir, shard_index),
      blob: Ferricstore.DataDir.blob_shard_path(ctx.data_dir, shard_index)
    ]

    dedicated =
      if copy_dedicated_payload?() do
        [dedicated: Path.join([ctx.data_dir, "dedicated", "shard_#{shard_index}"])]
      else
        []
      end

    base ++ dedicated ++ [prob: Path.join([ctx.data_dir, "prob", "shard_#{shard_index}"])]
  end

  defp snapshot_payload_kinds do
    if copy_dedicated_payload?() do
      [:data, :blob, :dedicated, :prob]
    else
      [:data, :blob, :prob]
    end
  end

  defp copy_dedicated_payload?, do: not segment_bitcask_apply?()

  defp stage_snapshot_dirs(snapshot_path, staging_root, handle, empty_payload_dirs) do
    empty_payload_dirs = MapSet.new(empty_payload_dirs)

    with :ok <- reset_dir(staging_root),
         :ok <-
           Enum.reduce_while(shard_dir_specs(handle), :ok, fn {kind, _dest}, :ok ->
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

  defp swap_staged_snapshot_dirs(staging_root, backup_root, handle) do
    specs = shard_dir_specs(handle)

    with :ok <- reset_dir(backup_root),
         :ok <- move_live_dirs_to_backup(specs, backup_root),
         :ok <- move_staged_dirs_live(specs, staging_root) do
      fsync_snapshot_parent_dirs(specs)
    else
      {:error, reason} = error ->
        {:error, reason || error}
    end
  end

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
