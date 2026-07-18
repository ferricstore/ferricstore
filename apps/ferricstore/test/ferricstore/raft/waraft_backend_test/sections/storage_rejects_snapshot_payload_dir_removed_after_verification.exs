defmodule Ferricstore.Raft.WARaftBackendTest.Sections.StorageRejectsSnapshotPayloadDirRemovedAfterVerification do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      import ExUnit.CaptureLog

      alias Ferricstore.ErrorReasons
      alias Ferricstore.Raft.Cluster, as: RaftCluster
      alias Ferricstore.Raft.WARaftBackend
      alias Ferricstore.Raft.WARaftStorage
      alias Ferricstore.Store.{BlobRef, BlobStore}
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.Router
      alias Ferricstore.Raft.WARaftBackendTest.LabelCounter
      alias Ferricstore.Raft.WARaftBackendTest.OversizedLabel

      @tag :snapshot_swap_phase_durability
      test "snapshot install syncs rollback backups before promoting staged payload", %{
        root: root,
        ctx: ctx
      } do
        snapshot_path = Path.join(root, "snapshot-backup-sync")
        position = {:raft_log_pos, 0, 0}
        File.mkdir_p!(Path.join(snapshot_path, "data"))

        File.write!(
          Path.join(snapshot_path, "ferricstore_snapshot.term"),
          :erlang.term_to_binary(%{
            version: 1,
            position: position,
            label: nil,
            config: nil,
            apply_context: ctx.apply_context,
            payload_dirs: [:data],
            empty_payload_dirs: [:data]
          })
        )

        handle = %{
          ctx: ctx,
          shard_index: 0,
          root_dir: Path.join([root, "waraft", "ferricstore_waraft_backend.1"]),
          sm_state: nil,
          position: position,
          label: nil,
          config: nil
        }

        previous_fsync_hook =
          Application.get_env(:ferricstore, :waraft_storage_fsync_dir_hook)

        Process.put(:snapshot_backup_root_synced, false)

        try do
          Application.put_env(:ferricstore, :waraft_storage_fsync_dir_hook, fn path ->
            if String.starts_with?(Path.basename(path), "snapshot_install_backup.") do
              Process.put(:snapshot_backup_root_synced, true)
            end

            :ok
          end)

          Process.put(:ferricstore_waraft_snapshot_install_hook, fn
            {:before_promote, _kind, _dest} ->
              if Process.get(:snapshot_backup_root_synced, false) do
                :ok
              else
                {:error, :rollback_backup_not_durable}
              end

            _event ->
              :ok
          end)

          assert {:ok, _handle} =
                   WARaftStorage.open_snapshot(snapshot_path, position, handle)
        after
          restore_env(:waraft_storage_fsync_dir_hook, previous_fsync_hook)
          Process.delete(:ferricstore_waraft_snapshot_install_hook)
          Process.delete(:snapshot_backup_root_synced)
        end
      end

      @tag :snapshot_missing_dir_rollback
      test "failed snapshot install removes staged data for an initially absent directory", %{
        root: root,
        ctx: ctx
      } do
        snapshot_path = Path.join(root, "snapshot-missing-dir-rollback")
        snapshot_prob_path = Path.join(snapshot_path, "prob")
        position = {:raft_log_pos, 0, 0}
        File.mkdir_p!(snapshot_prob_path)
        File.write!(Path.join(snapshot_prob_path, "snapshot-only.marker"), "staged")

        File.write!(
          Path.join(snapshot_path, "ferricstore_snapshot.term"),
          :erlang.term_to_binary(%{
            version: 1,
            position: position,
            label: nil,
            config: nil,
            apply_context: ctx.apply_context,
            payload_dirs: [:prob],
            empty_payload_dirs: []
          })
        )

        root_dir = Path.join([root, "waraft", "ferricstore_waraft_backend.1"])
        live_prob_path = Path.join([root, "prob", "shard_0"])
        File.rm_rf!(live_prob_path)

        handle = %{
          ctx: ctx,
          shard_index: 0,
          root_dir: root_dir,
          sm_state: nil,
          position: position,
          label: nil,
          config: nil
        }

        previous_fsync_hook =
          Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)

        try do
          Application.put_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook, fn path ->
            if String.starts_with?(Path.basename(path), "ferricstore_storage.term.tmp.") do
              {:error, :injected_metadata_publish_failure}
            else
              :ok
            end
          end)

          assert {:error, {:fsync_file, _path, :injected_metadata_publish_failure}} =
                   WARaftStorage.open_snapshot(snapshot_path, position, handle)
        after
          restore_env(:waraft_storage_metadata_fsync_file_hook, previous_fsync_hook)
        end

        refute File.exists?(Path.join(live_prob_path, "snapshot-only.marker"))
      end

      test "storage rejects snapshot payload dir removed after verification", %{
        root: root,
        ctx: ctx
      } do
        snapshot_path = Path.join(root, "payload-dir-race-snapshot")
        position = {:raft_log_pos, 0, 0}
        File.mkdir_p!(snapshot_path)

        Enum.each([:data, :blob, :prob], fn kind ->
          File.mkdir_p!(Path.join(snapshot_path, Atom.to_string(kind)))
        end)

        File.write!(
          Path.join(snapshot_path, "ferricstore_snapshot.term"),
          :erlang.term_to_binary(%{
            version: 1,
            position: position,
            label: nil,
            config: nil,
            apply_context: ctx.apply_context,
            payload_dirs: [:data, :blob, :prob],
            empty_payload_dirs: []
          })
        )

        handle = %{
          ctx: ctx,
          shard_index: 0,
          root_dir: Path.join([root, "waraft", "ferricstore_waraft_backend.1"]),
          sm_state: nil,
          position: position,
          label: nil,
          config: nil
        }

        Process.put(:ferricstore_waraft_snapshot_install_hook, fn
          {:staged, :data} ->
            File.rm_rf!(Path.join(snapshot_path, "blob"))
            :ok

          _event ->
            :ok
        end)

        try do
          assert {:error, {:blob, {:stat_source_dir, _path, :enoent}}} =
                   Ferricstore.Raft.WARaftStorage.open_snapshot(snapshot_path, position, handle)
        after
          Process.delete(:ferricstore_waraft_snapshot_install_hook)
        end
      end

      test "storage replaces projection dir recreated before staged promotion", %{root: root} do
        source_root = Path.join(root, "recreated-projection-source")
        target_root = Path.join(root, "recreated-projection-target")
        File.mkdir_p!(source_root)
        File.mkdir_p!(target_root)

        source_ctx = build_ctx(source_root)

        assert :ok =
                 WARaftBackend.start(source_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert :ok = WARaftBackend.write(0, {:put, "snapshot:recreated-projection", "new", 0})
        assert {:ok, {:raft_log_pos, index, term} = position} = WARaftBackend.create_snapshot(0)

        snapshot_path =
          Path.join([
            source_root,
            "waraft",
            "ferricstore_waraft_backend.1",
            "snapshot.#{index}.#{term}"
          ])

        assert :ok = WARaftBackend.stop()
        FerricStore.Instance.cleanup(source_ctx.name)

        target_ctx = build_ctx(target_root)

        assert :ok =
                 WARaftBackend.start(target_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert :ok = WARaftBackend.write(0, {:put, "snapshot:recreated-projection", "old", 0})
        assert :ok = WARaftBackend.stop()

        root_dir = Path.join([target_root, "waraft", "ferricstore_waraft_backend.1"])
        target_projection = Path.join(root_dir, "segment_projection_log")
        stale_file = Path.join(target_projection, "stale-recreated")

        handle = %{
          ctx: target_ctx,
          shard_index: 0,
          root_dir: root_dir,
          sm_state: nil,
          position: {:raft_log_pos, 1, 1},
          label: nil,
          config: nil
        }

        Process.put(:ferricstore_waraft_snapshot_install_hook, fn
          {:before_promote, :segment_projection_log, ^target_projection} ->
            File.mkdir_p!(target_projection)
            File.write!(stale_file, "stale")
            :ok

          _event ->
            :ok
        end)

        try do
          assert {:ok, _handle} =
                   Ferricstore.Raft.WARaftStorage.open_snapshot(snapshot_path, position, handle)
        after
          Process.delete(:ferricstore_waraft_snapshot_install_hook)
        end

        assert File.dir?(target_projection)
        refute File.exists?(stale_file)
      end

      test "storage rejects oversized snapshot metadata before install", %{
        root: root,
        ctx: ctx
      } do
        snapshot_path = Path.join(root, "oversized-snapshot")
        position = {:raft_log_pos, 10, 1}

        File.mkdir_p!(snapshot_path)

        for kind <- [:data, :blob, :prob] do
          File.mkdir_p!(Path.join(snapshot_path, Atom.to_string(kind)))
        end

        metadata = %{
          version: 1,
          position: position,
          label: :binary.copy("x", 1_048_576),
          config: nil,
          apply_context: ctx.apply_context,
          payload_dirs: [:data, :blob, :prob],
          empty_payload_dirs: [:data, :blob, :prob]
        }

        File.write!(
          Path.join(snapshot_path, "ferricstore_snapshot.term"),
          :erlang.term_to_binary(metadata)
        )

        handle = %{
          ctx: ctx,
          shard_index: 0,
          root_dir: Path.join([root, "waraft", "ferricstore_waraft_backend.1"]),
          sm_state: nil,
          position: {:raft_log_pos, 2, 1},
          label: nil,
          config: nil
        }

        assert {:error, {:read_snapshot_metadata, {:snapshot_metadata_file_too_large, size, max}}} =
                 Ferricstore.Raft.WARaftStorage.open_snapshot(snapshot_path, position, handle)

        assert size > max
        assert max == 1_048_576
        refute File.exists?(Path.join(handle.root_dir, "snapshot_install.term"))
      end

      test "snapshot creation fails before writing oversized metadata", %{ctx: ctx} do
        previous_every = Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)

        try do
          Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, :never)

          assert :ok =
                   WARaftBackend.start(ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     label_module: OversizedLabel
                   )

          assert :ok = WARaftBackend.write(0, {:put, "snapshot:metadata-too-large", "v", 0})

          assert {:error, reason} = WARaftBackend.create_snapshot(0)
          assert inspect(reason) =~ "snapshot_metadata_term_too_large"
        after
          WARaftBackend.stop()
          restore_env(:waraft_storage_metadata_persist_every, previous_every)
        end
      end

      test "storage rejects atom-bearing local snapshot metadata without interning atoms", %{
        root: root,
        ctx: ctx
      } do
        snapshot_path = Path.join(root, "unsafe-snapshot")
        File.mkdir_p!(snapshot_path)

        atom_name = "ferricstore_waraft_snapshot_metadata_#{System.unique_integer([:positive])}"
        refute existing_atom?(atom_name)

        File.write!(
          Path.join(snapshot_path, "ferricstore_snapshot.term"),
          unknown_atom_payload(atom_name)
        )

        handle = %{
          ctx: ctx,
          shard_index: 0,
          root_dir: Path.join([root, "waraft", "ferricstore_waraft_backend.1"]),
          sm_state: nil,
          position: {:raft_log_pos, 2, 1},
          label: nil,
          config: nil
        }

        assert {:error, {:decode_snapshot_metadata, :invalid_external_term}} =
                 Ferricstore.Raft.WARaftStorage.open_snapshot(
                   snapshot_path,
                   {:raft_log_pos, 10, 1},
                   handle
                 )

        refute existing_atom?(atom_name)
      end

      @tag :snapshot_metadata_nofollow
      test "storage rejects snapshot metadata symlinks before install", %{root: root} do
        snapshot_path = Path.join(root, "snapshot-metadata-symlink")
        File.mkdir_p!(snapshot_path)

        Enum.each([:data, :blob, :prob], fn kind ->
          File.mkdir_p!(Path.join(snapshot_path, Atom.to_string(kind)))
        end)

        position = {:raft_log_pos, 0, 0}
        outside_metadata = Path.join(root, "outside-snapshot-metadata.term")

        File.write!(
          outside_metadata,
          :erlang.term_to_binary(%{
            version: 1,
            position: position,
            config: nil,
            payload_dirs: [:data, :blob, :prob],
            empty_payload_dirs: [:data, :blob, :prob]
          })
        )

        metadata_path = Path.join(snapshot_path, "ferricstore_snapshot.term")
        assert :ok = File.ln_s(outside_metadata, metadata_path)

        target_root = Path.join(root, "snapshot-metadata-symlink-target")
        File.mkdir_p!(target_root)
        target_ctx = build_ctx(target_root)

        handle = %{
          ctx: target_ctx,
          shard_index: 0,
          root_dir: Path.join([target_root, "waraft", "ferricstore_waraft_backend.1"]),
          sm_state: nil,
          position: position,
          label: nil,
          config: nil
        }

        assert {:error,
                {:read_snapshot_metadata, {:unsafe_metadata_path, ^metadata_path, :symlink}}} =
                 Ferricstore.Raft.WARaftStorage.open_snapshot(snapshot_path, position, handle)

        source = Ferricstore.Test.SourceFiles.waraft_storage_source()
        assert source =~ "Ferricstore.FS.read_nofollow(path, max_bytes)"
      end

      test "storage rejects snapshot metadata missing position without crashing", %{
        root: root,
        ctx: ctx
      } do
        snapshot_path = Path.join(root, "bad-shape-snapshot")
        File.mkdir_p!(snapshot_path)

        File.write!(
          Path.join(snapshot_path, "ferricstore_snapshot.term"),
          :erlang.term_to_binary(%{version: 1})
        )

        handle = %{
          ctx: ctx,
          shard_index: 0,
          root_dir: Path.join([root, "waraft", "ferricstore_waraft_backend.1"]),
          sm_state: nil,
          position: {:raft_log_pos, 2, 1},
          label: nil,
          config: nil
        }

        assert {:error, {:bad_snapshot_metadata, %{version: 1}}} =
                 Ferricstore.Raft.WARaftStorage.open_snapshot(
                   snapshot_path,
                   {:raft_log_pos, 10, 1},
                   handle
                 )
      end

      test "storage rejects snapshot metadata with malformed config", %{
        root: root,
        ctx: ctx
      } do
        snapshot_path = Path.join(root, "bad-config-snapshot")
        File.mkdir_p!(snapshot_path)
        position = {:raft_log_pos, 10, 1}

        File.write!(
          Path.join(snapshot_path, "ferricstore_snapshot.term"),
          :erlang.term_to_binary(%{
            version: 1,
            position: position,
            config: {:bad_config_position, %{membership: []}},
            payload_dirs: [:data, :blob, :prob],
            empty_payload_dirs: [:data, :blob, :prob]
          })
        )

        handle = %{
          ctx: ctx,
          shard_index: 0,
          root_dir: Path.join([root, "waraft", "ferricstore_waraft_backend.1"]),
          sm_state: nil,
          position: {:raft_log_pos, 2, 1},
          label: nil,
          config: nil
        }

        assert {:error, {:bad_snapshot_metadata, {:bad_position, :bad_config_position}}} =
                 Ferricstore.Raft.WARaftStorage.open_snapshot(snapshot_path, position, handle)
      end

      test "storage rejects snapshot metadata with malformed payload dir declarations", %{
        root: root,
        ctx: ctx
      } do
        snapshot_path = Path.join(root, "bad-payload-dirs-snapshot")
        File.mkdir_p!(snapshot_path)
        position = {:raft_log_pos, 10, 1}

        File.write!(
          Path.join(snapshot_path, "ferricstore_snapshot.term"),
          :erlang.term_to_binary(%{
            version: 1,
            position: position,
            config: nil,
            apply_context: ctx.apply_context,
            payload_dirs: [:data, :blob, :prob],
            empty_payload_dirs: :all
          })
        )

        handle = %{
          ctx: ctx,
          shard_index: 0,
          root_dir: Path.join([root, "waraft", "ferricstore_waraft_backend.1"]),
          sm_state: nil,
          position: {:raft_log_pos, 2, 1},
          label: nil,
          config: nil
        }

        assert {:error, {:bad_snapshot_metadata, {:bad_payload_dirs, :empty_payload_dirs, :all}}} =
                 Ferricstore.Raft.WARaftStorage.open_snapshot(snapshot_path, position, handle)
      end

      test "snapshot creation fails closed when copied payload emptiness cannot be scanned", %{
        root: root
      } do
        source_root = Path.join(root, "scan-error-source")
        File.mkdir_p!(source_root)

        source_ctx = build_ctx(source_root)
        test_pid = self()
        previous_hook = Application.get_env(:ferricstore, :waraft_snapshot_create_hook)

        try do
          assert :ok =
                   WARaftBackend.start(source_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert :ok = WARaftBackend.write(0, {:put, "snapshot:scan-error", "value", 0})

          Application.put_env(:ferricstore, :waraft_snapshot_create_hook, fn
            {:copied, :data} ->
              snapshot_root =
                Path.join([
                  source_root,
                  "waraft",
                  "ferricstore_waraft_backend.1"
                ])

              [snapshot_path | _] = Path.wildcard(Path.join(snapshot_root, "snapshot.*"))
              data_dir = Path.join(snapshot_path, "data")
              File.chmod!(data_dir, 0)
              send(test_pid, {:snapshot_payload_dir_chmod, data_dir})
              :ok

            _event ->
              :ok
          end)

          assert {:error, {:snapshot_payload_empty, :data, _reason}} =
                   WARaftBackend.create_snapshot(0)
        after
          restore_chmoded_snapshot_dirs()
          restore_env(:waraft_snapshot_create_hook, previous_hook)
        end
      end

      test "snapshot creation rejects payload paths that are not directories", %{root: root} do
        source_root = Path.join(root, "non-dir-payload-source")
        File.mkdir_p!(source_root)

        source_ctx = build_ctx(source_root)

        assert :ok =
                 WARaftBackend.start(source_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert :ok = WARaftBackend.write(0, {:put, "snapshot:non-dir-payload", "value", 0})

        blob_path = Ferricstore.DataDir.blob_shard_path(source_ctx.data_dir, 0)
        File.rm_rf!(blob_path)
        File.write!(blob_path, "not-a-directory")

        assert {:error, {:blob, {:source_not_directory, ^blob_path, :regular}}} =
                 WARaftBackend.create_snapshot(0)
      end

      test "storage rejects snapshot payload symlinks without wiping live shard data", %{
        root: root
      } do
        source_root = Path.join(root, "symlink-source")
        target_root = Path.join(root, "symlink-target")
        File.mkdir_p!(source_root)
        File.mkdir_p!(target_root)

        source_ctx = build_ctx(source_root)

        assert :ok =
                 WARaftBackend.start(source_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert :ok = WARaftBackend.write(0, {:put, "snapshot:symlink", "new", 0})
        assert {:ok, {:raft_log_pos, index, term} = position} = WARaftBackend.create_snapshot(0)

        snapshot_path =
          Path.join([
            source_root,
            "waraft",
            "ferricstore_waraft_backend.1",
            "snapshot.#{index}.#{term}"
          ])

        outside_path = Path.join(root, "snapshot-symlink-outside")
        snapshot_link_path = Path.join([snapshot_path, "blob", "unsafe-link"])
        File.write!(outside_path, "outside")
        assert :ok = File.ln_s(outside_path, snapshot_link_path)

        assert :ok = WARaftBackend.stop()
        FerricStore.Instance.cleanup(source_ctx.name)

        target_ctx = build_ctx(target_root)

        assert :ok =
                 WARaftBackend.start(target_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert :ok = WARaftBackend.write(0, {:put, "snapshot:symlink", "old", 0})
        assert "old" == Router.get(target_ctx, "snapshot:symlink")
        assert :ok = WARaftBackend.stop()

        handle = %{
          ctx: target_ctx,
          shard_index: 0,
          root_dir: Path.join([target_root, "waraft", "ferricstore_waraft_backend.1"]),
          sm_state: nil,
          position: {:raft_log_pos, 2, 1},
          label: nil,
          config: nil
        }

        assert {:error, {:blob, {:unsafe_snapshot_payload_path, ^snapshot_link_path, :symlink}}} =
                 Ferricstore.Raft.WARaftStorage.open_snapshot(snapshot_path, position, handle)

        FerricStore.Instance.cleanup(target_ctx.name)
        recovered_ctx = build_ctx(target_root)

        assert :ok =
                 WARaftBackend.start(recovered_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert "old" == Router.get(recovered_ctx, "snapshot:symlink")
      end

      test "snapshot install copy failure leaves live shard data untouched", %{root: root} do
        source_root = Path.join(root, "partial-source")
        target_root = Path.join(root, "partial-target")
        File.mkdir_p!(source_root)
        File.mkdir_p!(target_root)

        source_ctx = build_ctx(source_root)

        assert :ok =
                 WARaftBackend.start(source_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert :ok = WARaftBackend.write(0, {:put, "snapshot:partial", "new", 0})
        assert "new" == Router.get(source_ctx, "snapshot:partial")
        assert {:ok, {:raft_log_pos, index, term} = position} = WARaftBackend.create_snapshot(0)

        snapshot_path =
          Path.join([
            source_root,
            "waraft",
            "ferricstore_waraft_backend.1",
            "snapshot.#{index}.#{term}"
          ])

        assert :ok = WARaftBackend.stop()
        FerricStore.Instance.cleanup(source_ctx.name)

        target_ctx = build_ctx(target_root)

        assert :ok =
                 WARaftBackend.start(target_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert :ok = WARaftBackend.write(0, {:put, "snapshot:partial", "old", 0})
        assert "old" == Router.get(target_ctx, "snapshot:partial")
        assert :ok = WARaftBackend.stop()

        handle = %{
          ctx: target_ctx,
          shard_index: 0,
          root_dir: Path.join([target_root, "waraft", "ferricstore_waraft_backend.1"]),
          sm_state: nil,
          position: {:raft_log_pos, 2, 1},
          label: nil,
          config: nil
        }

        Process.put(:ferricstore_waraft_snapshot_install_hook, fn {:staged, :data} ->
          {:error, :injected_snapshot_copy_failure}
        end)

        try do
          assert {:error, {:snapshot_install_hook, :injected_snapshot_copy_failure}} =
                   Ferricstore.Raft.WARaftStorage.open_snapshot(snapshot_path, position, handle)
        after
          Process.delete(:ferricstore_waraft_snapshot_install_hook)
        end

        FerricStore.Instance.cleanup(target_ctx.name)
        recovered_ctx = build_ctx(target_root)

        assert :ok =
                 WARaftBackend.start(recovered_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert "old" == Router.get(recovered_ctx, "snapshot:partial")
      end

      test "snapshot install swap failure preserves live dirs that were not backed up", %{
        root: root
      } do
        source_root = Path.join(root, "swap-fail-source")
        target_root = Path.join(root, "swap-fail-target")
        File.mkdir_p!(source_root)
        File.mkdir_p!(target_root)

        source_ctx = build_ctx(source_root)

        assert :ok =
                 WARaftBackend.start(source_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert :ok = WARaftBackend.write(0, {:put, "snapshot:swap-fail", "new", 0})
        assert {:ok, {:raft_log_pos, index, term} = position} = WARaftBackend.create_snapshot(0)

        snapshot_path =
          Path.join([
            source_root,
            "waraft",
            "ferricstore_waraft_backend.1",
            "snapshot.#{index}.#{term}"
          ])

        assert :ok = WARaftBackend.stop()
        FerricStore.Instance.cleanup(source_ctx.name)

        target_ctx = build_ctx(target_root)

        assert :ok =
                 WARaftBackend.start(target_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert :ok = WARaftBackend.write(0, {:put, "snapshot:swap-fail", "old", 0})
        assert "old" == Router.get(target_ctx, "snapshot:swap-fail")
        assert :ok = WARaftBackend.stop()

        specs = shard_dir_specs(target_ctx, 0)
        data_dest = Keyword.fetch!(specs, :data)
        blob_dest = Keyword.fetch!(specs, :blob)
        prob_dest = Keyword.fetch!(specs, :prob)
        prob_sentinel = Path.join(prob_dest, "sentinel")

        File.rm_rf!(blob_dest)
        File.mkdir_p!(Path.dirname(blob_dest))
        File.write!(blob_dest, "not-a-directory")
        File.mkdir_p!(prob_dest)
        File.write!(prob_sentinel, "must-survive")

        handle = %{
          ctx: target_ctx,
          shard_index: 0,
          root_dir: Path.join([target_root, "waraft", "ferricstore_waraft_backend.1"]),
          sm_state: nil,
          position: {:raft_log_pos, 2, 1},
          label: nil,
          config: nil
        }

        assert {:error, {:backup_live_dir, :blob, :not_directory}} =
                 Ferricstore.Raft.WARaftStorage.open_snapshot(snapshot_path, position, handle)

        assert File.exists?(prob_sentinel)
        assert File.dir?(data_dest)
        assert File.regular?(blob_dest)
      end

      @tag :snapshot_metadata_rollback
      test "snapshot install metadata failure rolls live ETS back with disk", %{root: root} do
        source_root = Path.join(root, "metadata-fail-source")
        target_root = Path.join(root, "metadata-fail-target")
        File.mkdir_p!(source_root)
        File.mkdir_p!(target_root)

        source_ctx = build_ctx(source_root)

        assert :ok =
                 WARaftBackend.start(source_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert :ok = WARaftBackend.write(0, {:put, "snapshot:metadata-fail", "new", 0})
        assert {:ok, {:raft_log_pos, index, term} = position} = WARaftBackend.create_snapshot(0)

        snapshot_path =
          Path.join([
            source_root,
            "waraft",
            "ferricstore_waraft_backend.1",
            "snapshot.#{index}.#{term}"
          ])

        assert :ok = WARaftBackend.stop()
        FerricStore.Instance.cleanup(source_ctx.name)

        target_ctx = build_ctx(target_root)

        assert :ok =
                 WARaftBackend.start(target_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert :ok =
                 WARaftBackend.write(0, {:put, "snapshot:metadata-fail", "old-value-longer", 0})

        assert "old-value-longer" == Router.get(target_ctx, "snapshot:metadata-fail")
        assert :ok = WARaftBackend.stop()

        root_dir = Path.join([target_root, "waraft", "ferricstore_waraft_backend.1"])
        metadata_path = Path.join(root_dir, "ferricstore_storage.term")
        File.rm!(metadata_path)
        File.mkdir_p!(metadata_path)

        handle = %{
          ctx: target_ctx,
          shard_index: 0,
          root_dir: root_dir,
          sm_state: nil,
          position: {:raft_log_pos, 2, 1},
          label: nil,
          config: nil
        }

        assert {:error, reason} =
                 Ferricstore.Raft.WARaftStorage.open_snapshot(snapshot_path, position, handle)

        assert "old-value-longer" == Router.get(target_ctx, "snapshot:metadata-fail"),
               inspect(reason)
      end

      @tag :snapshot_journal_cleanup_failure
      test "snapshot install journal cleanup failure restores previous storage metadata", %{
        root: root
      } do
        source_root = Path.join(root, "metadata-journal-cleanup-fail-source")
        target_root = Path.join(root, "metadata-journal-cleanup-fail-target")
        File.mkdir_p!(source_root)
        File.mkdir_p!(target_root)

        source_ctx = build_ctx(source_root)

        assert :ok =
                 WARaftBackend.start(source_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert :ok = WARaftBackend.write(0, {:put, "snapshot:journal-cleanup-fail", "new", 0})

        assert :ok =
                 WARaftBackend.write(0, {:put, "snapshot:journal-cleanup-fail:extra", "newer", 0})

        assert {:ok, {:raft_log_pos, index, term} = position} = WARaftBackend.create_snapshot(0)

        snapshot_path =
          Path.join([
            source_root,
            "waraft",
            "ferricstore_waraft_backend.1",
            "snapshot.#{index}.#{term}"
          ])

        assert :ok = WARaftBackend.stop()
        FerricStore.Instance.cleanup(source_ctx.name)

        target_ctx = build_ctx(target_root)

        assert :ok =
                 WARaftBackend.start(target_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert :ok = WARaftBackend.write(0, {:put, "snapshot:journal-cleanup-fail", "old", 0})
        assert "old" == Router.get(target_ctx, "snapshot:journal-cleanup-fail")
        assert :ok = WARaftBackend.stop()

        root_dir = Path.join([target_root, "waraft", "ferricstore_waraft_backend.1"])
        metadata_path = Path.join(root_dir, "ferricstore_storage.term")
        journal_path = metadata_path <> ".journal"
        old_position = Map.fetch!(waraft_storage_metadata(target_root, 0), :position)

        File.rm_rf!(journal_path)
        File.mkdir_p!(journal_path)

        handle = %{
          ctx: target_ctx,
          shard_index: 0,
          root_dir: root_dir,
          sm_state: nil,
          position: old_position,
          label: nil,
          config: nil
        }

        assert {:error, {:delete_storage_metadata_journal, _reason}} =
                 Ferricstore.Raft.WARaftStorage.open_snapshot(snapshot_path, position, handle)

        assert %{position: ^old_position} = waraft_storage_metadata(target_root, 0)
        assert "old" == Router.get(target_ctx, "snapshot:journal-cleanup-fail")

        FerricStore.Instance.cleanup(target_ctx.name)
        recovered_ctx = build_ctx(target_root)

        assert :ok =
                 WARaftBackend.start(recovered_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert "old" == Router.get(recovered_ctx, "snapshot:journal-cleanup-fail")
      end

      test "startup rolls back interrupted snapshot swap before metadata persisted", %{root: root} do
        source_root = Path.join(root, "swap-source")
        target_root = Path.join(root, "swap-target")
        File.mkdir_p!(source_root)
        File.mkdir_p!(target_root)

        source_ctx = build_ctx(source_root)

        assert :ok =
                 WARaftBackend.start(source_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert :ok = WARaftBackend.write(0, {:put, "snapshot:swap", "new", 0})
        assert {:ok, {:raft_log_pos, index, term} = position} = WARaftBackend.create_snapshot(0)

        snapshot_path =
          Path.join([
            source_root,
            "waraft",
            "ferricstore_waraft_backend.1",
            "snapshot.#{index}.#{term}"
          ])

        assert :ok = WARaftBackend.stop()
        FerricStore.Instance.cleanup(source_ctx.name)

        target_ctx = build_ctx(target_root)

        assert :ok =
                 WARaftBackend.start(target_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert :ok = WARaftBackend.write(0, {:put, "snapshot:swap", "old", 0})
        assert :ok = WARaftBackend.write(0, {:put, "snapshot:swap:target-only", "keep", 0})
        assert "old" == Router.get(target_ctx, "snapshot:swap")
        assert "keep" == Router.get(target_ctx, "snapshot:swap:target-only")
        assert :ok = WARaftBackend.stop()

        root_dir = Path.join([target_root, "waraft", "ferricstore_waraft_backend.1"])
        backup_root = Path.join(root_dir, "snapshot_install_backup.injected")
        staging_root = Path.join(root_dir, "snapshot_install_staging.injected")

        File.rm_rf!(backup_root)
        File.rm_rf!(staging_root)
        File.mkdir_p!(backup_root)
        File.mkdir_p!(staging_root)

        for {kind, dest} <- shard_dir_specs(target_ctx, 0) do
          backup = Path.join(backup_root, Atom.to_string(kind))
          source = Path.join(snapshot_path, Atom.to_string(kind))

          File.mkdir_p!(Path.dirname(backup))
          File.rename!(dest, backup)
          {:ok, _copied} = File.cp_r(source, dest)
        end

        marker = %{
          version: 1,
          snapshot_position: position,
          backup_root: backup_root,
          staging_root: staging_root
        }

        File.write!(Path.join(root_dir, "snapshot_install.term"), :erlang.term_to_binary(marker))

        FerricStore.Instance.cleanup(target_ctx.name)
        recovered_ctx = build_ctx(target_root)

        assert :ok =
                 WARaftBackend.start(recovered_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert "old" == Router.get(recovered_ctx, "snapshot:swap")
        assert "keep" == Router.get(recovered_ctx, "snapshot:swap:target-only")
      end

      test "startup aborts snapshot install marker written before backup starts", %{root: root} do
        target_root = Path.join(root, "marker-before-backup-target")
        File.mkdir_p!(target_root)

        target_ctx = build_ctx(target_root)

        assert :ok =
                 WARaftBackend.start(target_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert :ok = WARaftBackend.write(0, {:put, "snapshot:marker-before-backup", "old", 0})
        assert "old" == Router.get(target_ctx, "snapshot:marker-before-backup")
        assert :ok = WARaftBackend.stop()

        root_dir = Path.join([target_root, "waraft", "ferricstore_waraft_backend.1"])
        backup_root = Path.join(root_dir, "snapshot_install_backup.before_backup")
        staging_root = Path.join(root_dir, "snapshot_install_staging.before_backup")

        File.rm_rf!(backup_root)
        File.rm_rf!(staging_root)
        File.mkdir_p!(staging_root)
        File.write!(Path.join(staging_root, "sentinel"), "staged-but-not-swapped")

        marker = %{
          version: 1,
          snapshot_position: {:raft_log_pos, 100, 1},
          backup_root: backup_root,
          staging_root: staging_root
        }

        File.write!(Path.join(root_dir, "snapshot_install.term"), :erlang.term_to_binary(marker))

        FerricStore.Instance.cleanup(target_ctx.name)
        recovered_ctx = build_ctx(target_root)

        assert :ok =
                 WARaftBackend.start(recovered_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert "old" == Router.get(recovered_ctx, "snapshot:marker-before-backup")
        refute File.exists?(Path.join(root_dir, "snapshot_install.term"))
        refute File.exists?(staging_root)
        refute File.exists?(backup_root)
      end

      test "startup rolls back interrupted snapshot install with partial backup", %{root: root} do
        target_root = Path.join(root, "partial-backup-target")
        File.mkdir_p!(target_root)

        target_ctx = build_ctx(target_root)

        assert :ok =
                 WARaftBackend.start(target_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert :ok = WARaftBackend.write(0, {:put, "snapshot:partial-backup", "old", 0})
        assert "old" == Router.get(target_ctx, "snapshot:partial-backup")
        assert :ok = WARaftBackend.stop()

        root_dir = Path.join([target_root, "waraft", "ferricstore_waraft_backend.1"])
        backup_root = Path.join(root_dir, "snapshot_install_backup.partial")
        staging_root = Path.join(root_dir, "snapshot_install_staging.partial")

        specs = shard_dir_specs(target_ctx, 0)
        data_dest = Keyword.fetch!(specs, :data)
        data_backup = Path.join(backup_root, "data")

        File.rm_rf!(backup_root)
        File.rm_rf!(staging_root)
        File.mkdir_p!(Path.dirname(data_backup))
        File.mkdir_p!(staging_root)
        File.write!(Path.join(staging_root, "sentinel"), "staged-but-not-promoted")
        File.rename!(data_dest, data_backup)

        marker = %{
          version: 1,
          snapshot_position: {:raft_log_pos, 100, 1},
          backup_root: backup_root,
          staging_root: staging_root
        }

        File.write!(Path.join(root_dir, "snapshot_install.term"), :erlang.term_to_binary(marker))

        FerricStore.Instance.cleanup(target_ctx.name)
        recovered_ctx = build_ctx(target_root)

        assert :ok =
                 WARaftBackend.start(recovered_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log,
                   bootstrap: false
                 )

        assert "old" == Router.get(recovered_ctx, "snapshot:partial-backup")
        refute File.exists?(Path.join(root_dir, "snapshot_install.term"))
        refute File.exists?(staging_root)
        refute File.exists?(backup_root)
      end
    end
  end
end
