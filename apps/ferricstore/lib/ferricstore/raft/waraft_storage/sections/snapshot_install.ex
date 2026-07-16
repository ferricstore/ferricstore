defmodule Ferricstore.Raft.WARaftStorage.Sections.SnapshotInstall do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.CommandTime
      alias Ferricstore.HLC
      alias Ferricstore.Flow.HistoryProjector
      alias Ferricstore.Flow.Keys, as: FlowKeys
      alias Ferricstore.Flow.LMDB, as: FlowLMDB
      alias Ferricstore.Flow.LMDBWriter
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

      defp with_flow_lmdb_snapshot_install(handle, fun) when is_function(fun, 0) do
        with :ok <- prepare_flow_lmdb_snapshot_install(handle) do
          result =
            try do
              fun.()
            catch
              kind, reason ->
                _ = resume_flow_lmdb_after_snapshot_install(handle)
                :erlang.raise(kind, reason, __STACKTRACE__)
            end

          case resume_flow_lmdb_after_snapshot_install(handle) do
            :ok -> result
            {:error, reason} -> {:error, {:flow_lmdb_snapshot_resume_failed, reason}}
          end
        end
      end

      defp prepare_flow_lmdb_snapshot_install(%{ctx: ctx, shard_index: shard_index} = handle) do
        instance_name = Map.get(ctx, :name, :default)

        with :ok <- LMDBWriter.prepare_snapshot_install(instance_name, shard_index) do
          lmdb_path = flow_lmdb_path(ctx, shard_index)

          release_result =
            with :ok <- Ferricstore.FS.mkdir_p(lmdb_path) do
              FlowLMDB.release(lmdb_path)
            end

          case release_result do
            :ok ->
              :ok

            {:error, reason} ->
              _ = resume_flow_lmdb_after_snapshot_install(handle)
              {:error, {:flow_lmdb_snapshot_release_failed, reason}}
          end
        end
      end

      defp resume_flow_lmdb_after_snapshot_install(%{ctx: ctx, shard_index: shard_index}) do
        LMDBWriter.resume_after_snapshot_install(Map.get(ctx, :name, :default), shard_index)
      end

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
               :ok <- maybe_run_snapshot_install_hook({:before_promote, kind, dest}),
               :ok <- promote_staged_dir(staged, dest) do
            {:cont, :ok}
          else
            {:error, reason} -> {:halt, {:error, {:promote_staged_dir, kind, reason}}}
          end
        end)
      end

      defp promote_staged_dir(staged, dest) do
        case Ferricstore.FS.rename(staged, dest) do
          :ok ->
            :ok

          {:error, reason} = error when reason in [:directory_not_empty, :not_empty] ->
            replace_recreated_snapshot_dir(staged, dest, error)

          {:error, {:directory_not_empty, _}} = error ->
            replace_recreated_snapshot_dir(staged, dest, error)

          {:error, {:not_empty, _}} = error ->
            replace_recreated_snapshot_dir(staged, dest, error)

          {:error, _reason} = error ->
            error
        end
      end

      defp replace_recreated_snapshot_dir(staged, dest, original_error) do
        case File.lstat(dest) do
          {:ok, %{type: :directory}} ->
            with :ok <- Ferricstore.FS.rm_rf(dest),
                 :ok <- Ferricstore.FS.rename(staged, dest) do
              :ok
            end

          {:ok, %{type: type}} ->
            {:error, {:unsafe_recreated_snapshot_target, dest, type}}

          {:error, :enoent} ->
            Ferricstore.FS.rename(staged, dest)

          {:error, _reason} ->
            original_error
        end
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
            case Ferricstore.FS.copy_sync_nofollow(source, dest) do
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
  end
end
