defmodule Ferricstore.Raft.WARaftStorage.Sections.SnapshotMetadata do
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
                 :ok <- run_storage_metadata_fsync_hook(journal_path),
                 :ok <-
                   Ferricstore.FS.append_sync_nofollow_bounded(
                     journal_path,
                     record,
                     @max_metadata_journal_bytes
                   ),
                 :ok <- maybe_fsync_new_metadata_journal_dir(journal_path, new_file?) do
              :ok
            else
              {:error, {:too_large, _reason}} ->
                compact_storage_metadata(path, payload)

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
          {:ok, %{type: :regular, size: size}} when size <= @max_metadata_journal_bytes ->
            {:ok, size}

          {:ok, %{type: :regular, size: size}} ->
            {:error,
             {:metadata_journal_too_large, journal_path, size, @max_metadata_journal_bytes}}

          {:ok, %{type: type}} ->
            {:error, {:unsafe_metadata_path, journal_path, type}}

          {:error, :enoent} ->
            {:ok, 0}

          {:error, reason} ->
            {:error, {:stat_metadata_journal, reason}}
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
        case open_verified_metadata_journal(journal_path, [:read, :write, :binary]) do
          {:ok, io} ->
            try do
              with {:ok, _pos} <- :file.position(io, previous_size),
                   :ok <- :file.truncate(io) do
                :ok
              end
            after
              :file.close(io)
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

        case metadata_journal_size(journal_path) do
          {:ok, _size} ->
            case open_verified_metadata_journal(journal_path, [:read, :binary]) do
              {:ok, io} ->
                try do
                  read_metadata_journal_record(io, nil)
                after
                  :file.close(io)
                end

              {:error, reason} ->
                {:error, {:read_storage_metadata_journal, reason}}
            end

          {:error, reason} ->
            {:error, {:read_storage_metadata_journal, reason}}
        end
      end

      defp open_verified_metadata_journal(path, modes) do
        case File.lstat(path) do
          {:ok,
           %File.Stat{
             type: :regular,
             major_device: major_device,
             minor_device: minor_device,
             inode: inode
           }} ->
            open_verified_metadata_journal_file(
              path,
              modes,
              major_device,
              minor_device,
              inode
            )

          {:ok, %File.Stat{type: type}} ->
            {:error, {:unsafe_metadata_path, path, type}}

          {:error, reason} ->
            {:error, reason}
        end
      end

      defp open_verified_metadata_journal_file(
             path,
             modes,
             major_device,
             minor_device,
             inode
           ) do
        case :file.open(String.to_charlist(path), [:raw | modes]) do
          {:ok, io} ->
            case :file.read_file_info(io) do
              {:ok,
               {:file_info, size, :regular, _access, _atime, _mtime, _ctime, _mode, _links,
                ^major_device, ^minor_device, ^inode, _uid, _gid}}
              when size <= @max_metadata_journal_bytes ->
                {:ok, io}

              {:ok,
               {:file_info, size, :regular, _access, _atime, _mtime, _ctime, _mode, _links,
                ^major_device, ^minor_device, ^inode, _uid, _gid}} ->
                :ok = :file.close(io)

                {:error, {:metadata_journal_too_large, path, size, @max_metadata_journal_bytes}}

              {:ok, _other} ->
                :ok = :file.close(io)
                {:error, {:metadata_journal_identity_mismatch, path}}

              {:error, reason} ->
                :ok = :file.close(io)
                {:error, {:read_open_metadata_journal_info, reason}}
            end

          {:error, reason} ->
            {:error, reason}
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

      defp run_storage_metadata_fsync_hook(path) do
        case Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook) do
          fun when is_function(fun, 1) ->
            case fun.(path) do
              :ok -> :ok
              {:error, reason} -> {:error, {:fsync_file, path, reason}}
              other -> {:error, {:fsync_file, path, other}}
            end

          _other ->
            :ok
        end
      rescue
        error -> {:error, {:fsync_file_exception, path, error}}
      end

      defp with_stable_flow_projection_snapshot(
             %{ctx: ctx, shard_index: shard_index} = handle,
             fun
           )
           when is_function(fun, 0) do
        case HistoryProjector.flush(ctx, shard_index, snapshot_compaction_drain_timeout_ms()) do
          :ok ->
            with_flow_lmdb_snapshot_install(handle, fun)

          {:error, reason} ->
            {:error, {:flow_history_snapshot_flush_failed, reason}}

          other ->
            {:error, {:flow_history_snapshot_flush_failed, other}}
        end
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

      defp copy_compacted_storage_dirs_to_snapshot(
             snapshot_path,
             %{
               ctx: ctx,
               root_dir: root_dir,
               shard_index: shard_index,
               position: position
             } = handle
           ) do
        Ferricstore.Raft.WARaftSegmentReader.with_apply_projection_disk_lock(
          ctx.data_dir,
          shard_index,
          fn ->
            with :ok <- flush_apply_projection_snapshot_payload(handle),
                 :ok <-
                   compact_apply_projection_log(
                     root_dir,
                     ctx,
                     shard_index,
                     apply_projection_snapshot_trim_index(position),
                     snapshot_flow_lmdb_path(snapshot_path)
                   ),
                 :ok <- copy_storage_dirs_to_snapshot(snapshot_path, handle) do
              :ok
            end
          end
        )
      end

      defp snapshot_flow_lmdb_path(snapshot_path),
        do: snapshot_path |> Path.join("data") |> FlowLMDB.path()

      defp apply_projection_snapshot_trim_index({:raft_log_pos, index, _term})
           when is_integer(index) and index >= 0,
           do: index + 1

      defp apply_projection_snapshot_trim_index(_position), do: 1

      defp flush_apply_projection_snapshot_payload(data_dir, shard_index, chunk_entries) do
        case Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(
               data_dir,
               shard_index
             ) do
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
        case Application.get_env(
               :ferricstore,
               :waraft_apply_projection_snapshot_spill_chunk_entries
             ) do
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
                 stage_snapshot_dirs(
                   snapshot_path,
                   install.staging_root,
                   specs,
                   empty_payload_dirs
                 ),
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
                     {:snapshot_install_missing_metadata_without_backup,
                      install.snapshot_position}}

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
          build_sm_state(ctx, shard_index, Map.get(metadata, :apply_context))
          |> maybe_recover_segment_projected!(Map.get(handle, :root_dir), metadata)

        :ok
      rescue
        _ -> :ok
      end

      defp rebuild_runtime_after_snapshot_rollback(_handle), do: :ok

      defp rollback_rebuild_metadata(nil, handle),
        do: %{
          position: Map.get(handle, :position, @zero_pos),
          apply_context: storage_apply_context(handle)
        }

      defp rollback_rebuild_metadata(root_dir, handle) do
        case read_metadata_if_present(metadata_path(root_dir)) do
          %{position: _position} = metadata ->
            metadata

          _missing_or_bad ->
            %{
              position:
                latest_segment_log_position(root_dir, Map.get(handle, :position, @zero_pos)),
              apply_context: storage_apply_context(handle)
            }
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
    end
  end
end
