defmodule Ferricstore.Raft.WARaftBackendTest.Sections.RestartRecoversMissingCurrentStorageMetadataUsingPreviousDurable do
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

      test "restart recovers from missing current storage metadata using previous durable metadata",
           %{
             root: root,
             ctx: ctx
           } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "metadata:missing-current:before", "v1", 0})
        assert "v1" == Router.get(ctx, "metadata:missing-current:before")
        assert :ok = WARaftBackend.write(0, {:put, "metadata:missing-current:after", "v2", 0})
        assert "v2" == Router.get(ctx, "metadata:missing-current:after")
        assert :ok = WARaftBackend.stop()

        assert File.exists?(waraft_storage_metadata_previous_path(root, 0))
        assert :ok = File.rm(waraft_storage_metadata_path(root, 0))

        FerricStore.Instance.cleanup(ctx.name)
        restarted_ctx = build_ctx(root)

        assert :ok =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert_eventually(
          fn -> Router.get(restarted_ctx, "metadata:missing-current:before") end,
          "v1"
        )

        assert_eventually(
          fn -> Router.get(restarted_ctx, "metadata:missing-current:after") end,
          "v2"
        )
      end

      test "restart prefers journal metadata when current storage metadata is valid but stale", %{
        root: root,
        ctx: ctx
      } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "metadata:stale-current", "value", 0})
        assert "value" == Router.get(ctx, "metadata:stale-current")
        assert :ok = WARaftBackend.stop()

        metadata_path = waraft_storage_metadata_path(root, 0)

        assert %{position: {:raft_log_pos, expected_index, _term}} =
                 waraft_storage_metadata(root, 0)

        assert expected_index >= 2

        File.write!(
          metadata_path,
          :erlang.term_to_binary(%{
            version: 1,
            position: {:raft_log_pos, 0, 0},
            label: nil,
            config: nil,
            apply_context: ctx.apply_context
          })
        )

        FerricStore.Instance.cleanup(ctx.name)
        restarted_ctx = build_ctx(root)

        assert :ok =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert {:ok, {:raft_log_pos, recovered_index, _recovered_term}} =
                 WARaftBackend.storage_position(0)

        assert recovered_index >= expected_index
        assert_eventually(fn -> Router.get(restarted_ctx, "metadata:stale-current") end, "value")
      end

      test "restart recovers from torn current and previous storage metadata using journal", %{
        root: root,
        ctx: ctx
      } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "metadata:journal:before", "v1", 0})
        assert "v1" == Router.get(ctx, "metadata:journal:before")
        assert :ok = WARaftBackend.write(0, {:put, "metadata:journal:after", "v2", 0})
        assert "v2" == Router.get(ctx, "metadata:journal:after")
        assert :ok = WARaftBackend.stop()

        metadata_path = waraft_storage_metadata_path(root, 0)
        previous_path = waraft_storage_metadata_previous_path(root, 0)
        journal_path = waraft_storage_metadata_journal_path(root, 0)
        write_storage_metadata_journal!(journal_path, waraft_storage_metadata(root, 0))
        assert File.exists?(journal_path)
        File.write!(metadata_path, <<131, 116, 0, 0, 0, 3, "torn">>)
        File.write!(previous_path, <<131, 116, 0, 0, 0, 3, "also-torn">>)

        FerricStore.Instance.cleanup(ctx.name)
        restarted_ctx = build_ctx(root)

        assert :ok =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert_eventually(fn -> Router.get(restarted_ctx, "metadata:journal:before") end, "v1")
        assert_eventually(fn -> Router.get(restarted_ctx, "metadata:journal:after") end, "v2")
      end

      test "restart fails closed when all storage metadata artifacts are missing but shard payload exists",
           %{
             root: root,
             ctx: ctx
           } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "metadata:missing-file", "value", 0})
        assert "value" == Router.get(ctx, "metadata:missing-file")
        assert :ok = WARaftBackend.stop()

        assert :ok = File.rm(waraft_storage_metadata_path(root, 0))
        assert :ok = File.rm(waraft_storage_metadata_previous_path(root, 0))
        File.rm(waraft_storage_metadata_journal_path(root, 0))
        FerricStore.Instance.cleanup(ctx.name)
        restarted_ctx = build_ctx(root)

        assert {:error, _reason} =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert shard_payload_present?(restarted_ctx, 0)
      end

      test "restart fails closed on snapshot install marker without metadata or backup", %{
        root: root,
        ctx: ctx
      } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "metadata:marker-no-backup", "value", 0})
        assert "value" == Router.get(ctx, "metadata:marker-no-backup")
        assert :ok = WARaftBackend.stop()

        root_dir = Path.join([root, "waraft", "ferricstore_waraft_backend.1"])

        marker = %{
          version: 1,
          snapshot_position: {:raft_log_pos, 10, 1},
          staging_root: Path.join(root_dir, "snapshot_install_staging.no_backup"),
          backup_root: Path.join(root_dir, "snapshot_install_backup.no_backup")
        }

        for path <- [
              waraft_storage_metadata_path(root, 0),
              waraft_storage_metadata_previous_path(root, 0),
              waraft_storage_metadata_journal_path(root, 0)
            ] do
          case File.rm(path) do
            :ok -> :ok
            {:error, :enoent} -> :ok
            {:error, reason} -> flunk("failed to remove #{path}: #{inspect(reason)}")
          end
        end

        File.write!(Path.join(root_dir, "snapshot_install.term"), :erlang.term_to_binary(marker))
        FerricStore.Instance.cleanup(ctx.name)
        restarted_ctx = build_ctx(root)

        assert {:error, reason} =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert inspect(reason) =~ "snapshot_install_missing_metadata_without_backup"
        assert shard_payload_present?(restarted_ctx, 0)
      end

      test "restart rejects pending snapshot install with symlink backup payload", %{
        root: root,
        ctx: ctx
      } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "metadata:marker-symlink-backup", "old", 0})
        assert "old" == Router.get(ctx, "metadata:marker-symlink-backup")
        assert :ok = WARaftBackend.stop()

        root_dir = Path.join([root, "waraft", "ferricstore_waraft_backend.1"])
        backup_root = Path.join(root_dir, "snapshot_install_backup.symlink")
        staging_root = Path.join(root_dir, "snapshot_install_staging.symlink")
        outside_data = Path.join(root, "outside-backup-data")

        File.mkdir_p!(backup_root)
        File.mkdir_p!(staging_root)
        File.mkdir_p!(outside_data)
        assert :ok = File.ln_s(outside_data, Path.join(backup_root, "data"))

        for kind <- ["blob", "prob"] do
          File.mkdir_p!(Path.join(backup_root, kind))
        end

        marker = %{
          version: 1,
          snapshot_position: {:raft_log_pos, 10, 1},
          staging_root: staging_root,
          backup_root: backup_root
        }

        File.write!(Path.join(root_dir, "snapshot_install.term"), :erlang.term_to_binary(marker))
        FerricStore.Instance.cleanup(ctx.name)
        restarted_ctx = build_ctx(root)

        assert {:error, reason} =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert inspect(reason) =~ "unsafe_snapshot_payload_path"

        assert {:ok, %{type: :directory}} =
                 File.lstat(Ferricstore.DataDir.shard_data_path(root, 0))

        assert shard_payload_present?(restarted_ctx, 0)
      end

      test "restart rejects pending snapshot install marker symlink", %{
        root: root,
        ctx: ctx
      } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "metadata:marker-symlink-file", "value", 0})
        assert "value" == Router.get(ctx, "metadata:marker-symlink-file")
        assert :ok = WARaftBackend.stop()

        root_dir = Path.join([root, "waraft", "ferricstore_waraft_backend.1"])
        marker_path = Path.join(root_dir, "snapshot_install.term")
        outside_marker = Path.join(root, "outside-snapshot-install-marker.term")

        marker = %{
          version: 1,
          snapshot_position: {:raft_log_pos, 10, 1},
          staging_root: Path.join(root_dir, "snapshot_install_staging.symlink_marker"),
          backup_root: Path.join(root_dir, "snapshot_install_backup.symlink_marker")
        }

        File.write!(outside_marker, :erlang.term_to_binary(marker))
        assert :ok = File.ln_s(outside_marker, marker_path)

        FerricStore.Instance.cleanup(ctx.name)
        restarted_ctx = build_ctx(root)

        assert {:error, reason} =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert inspect(reason) =~ "unsafe_metadata_path"
        assert {:ok, %{type: :symlink}} = File.lstat(marker_path)
      end

      test "restart fails closed on storage metadata with invalid position", %{
        root: root,
        ctx: ctx
      } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "metadata:bad-position", "value", 0})
        assert "value" == Router.get(ctx, "metadata:bad-position")
        assert :ok = WARaftBackend.stop()

        root
        |> waraft_storage_metadata(0)
        |> Map.put(:position, {:not_a_raft_log_pos, "bad"})
        |> then(&:erlang.term_to_binary/1)
        |> then(&File.write!(waraft_storage_metadata_path(root, 0), &1))

        FerricStore.Instance.cleanup(ctx.name)
        restarted_ctx = build_ctx(root)

        assert {:error, _reason} =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert shard_payload_present?(restarted_ctx, 0)
      end

      @tag :snapshot_boundary_mismatch
      test "restart fails closed when snapshot boundary differs from storage position", %{
        root: root,
        ctx: ctx
      } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "metadata:bad-boundary", "value", 0})
        assert :ok = WARaftBackend.stop()

        metadata = waraft_storage_metadata(root, 0)
        {:raft_log_pos, index, term} = metadata.position

        metadata
        |> Map.put(:snapshot_boundary_position, {:raft_log_pos, index + 1, term})
        |> then(&:erlang.term_to_binary/1)
        |> then(&File.write!(waraft_storage_metadata_path(root, 0), &1))

        FerricStore.Instance.cleanup(ctx.name)
        restarted_ctx = build_ctx(root)

        assert {:error, reason} =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert inspect(reason) =~ "snapshot_boundary_position_mismatch"
        assert shard_payload_present?(restarted_ctx, 0)
      end

      test "restart fails closed on storage metadata missing position", %{root: root, ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "metadata:missing-position", "value", 0})
        assert "value" == Router.get(ctx, "metadata:missing-position")
        assert :ok = WARaftBackend.stop()

        root
        |> waraft_storage_metadata(0)
        |> Map.delete(:position)
        |> then(&:erlang.term_to_binary/1)
        |> then(&File.write!(waraft_storage_metadata_path(root, 0), &1))

        FerricStore.Instance.cleanup(ctx.name)
        restarted_ctx = build_ctx(root)

        assert {:error, _reason} =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert shard_payload_present?(restarted_ctx, 0)
      end

      @tag :strict_apply_context_metadata
      test "restart fails closed on storage metadata missing replicated apply context", %{
        root: root,
        ctx: ctx
      } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "metadata:missing-context", "value", 0})
        assert "value" == Router.get(ctx, "metadata:missing-context")
        assert :ok = WARaftBackend.stop()

        root
        |> waraft_storage_metadata(0)
        |> Map.delete(:apply_context)
        |> then(&:erlang.term_to_binary/1)
        |> then(&File.write!(waraft_storage_metadata_path(root, 0), &1))

        FerricStore.Instance.cleanup(ctx.name)
        restarted_ctx = build_ctx(root)

        assert {:error, reason} =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert inspect(reason) =~ "missing_apply_context"
        assert shard_payload_present?(restarted_ctx, 0)
      end

      test "restart fails closed on storage metadata with invalid config", %{root: root, ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "metadata:bad-config", "value", 0})
        assert "value" == Router.get(ctx, "metadata:bad-config")
        assert :ok = WARaftBackend.stop()

        root
        |> waraft_storage_metadata(0)
        |> Map.put(:config, {:bad_config_position, %{membership: []}})
        |> then(&:erlang.term_to_binary/1)
        |> then(&File.write!(waraft_storage_metadata_path(root, 0), &1))

        FerricStore.Instance.cleanup(ctx.name)
        restarted_ctx = build_ctx(root)

        assert {:error, _reason} =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert shard_payload_present?(restarted_ctx, 0)
      end

      test "snapshot install restores real Bitcask state into a stalled backend member", %{
        root: root
      } do
        source_root = Path.join(root, "source")
        target_root = Path.join(root, "target")
        File.mkdir_p!(source_root)
        File.mkdir_p!(target_root)

        source_ctx = build_ctx(source_root)

        assert :ok =
                 WARaftBackend.start(source_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert :ok = WARaftBackend.write(0, {:put, "snap:k", "snap:v", 0})
        assert "snap:v" == Router.get(source_ctx, "snap:k")

        assert {:ok, {:raft_log_pos, index, term} = position} = WARaftBackend.create_snapshot(0)

        snapshot_path =
          Path.join([
            source_root,
            "waraft",
            "ferricstore_waraft_backend.1",
            "snapshot.#{index}.#{term}"
          ])

        assert File.dir?(snapshot_path)
        assert :ok = WARaftBackend.stop()
        FerricStore.Instance.cleanup(source_ctx.name)

        target_ctx = build_ctx(target_root)

        assert :ok =
                 WARaftBackend.start(target_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log,
                   bootstrap: false
                 )

        assert :ok = WARaftBackend.install_snapshot(0, snapshot_path, position)
        assert_eventually(fn -> Router.get(target_ctx, "snap:k") end, "snap:v")
      end

      test "snapshot install carries promoted dedicated shard payload directories", %{root: root} do
        source_root = Path.join(root, "dedicated-source")
        target_root = Path.join(root, "dedicated-target")
        File.mkdir_p!(source_root)
        File.mkdir_p!(target_root)

        source_ctx = build_ctx(source_root)

        assert :ok =
                 WARaftBackend.start(source_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        promoted_key = "snapshot:dedicated:hash"

        source_dedicated_dir =
          Ferricstore.Store.Promotion.dedicated_path(source_ctx.data_dir, 0, :hash, promoted_key)

        File.mkdir_p!(source_dedicated_dir)

        File.write!(
          Path.join(source_dedicated_dir, "00000000000000000001.log"),
          "dedicated-payload"
        )

        assert {:ok, {:raft_log_pos, index, term} = position} = WARaftBackend.create_snapshot(0)

        snapshot_path =
          Path.join([
            source_root,
            "waraft",
            "ferricstore_waraft_backend.1",
            "snapshot.#{index}.#{term}"
          ])

        assert File.exists?(
                 Path.join([
                   snapshot_path,
                   "dedicated",
                   Path.basename(source_dedicated_dir),
                   "00000000000000000001.log"
                 ])
               )

        assert :ok = WARaftBackend.stop()
        FerricStore.Instance.cleanup(source_ctx.name)

        target_ctx = build_ctx(target_root)

        assert :ok =
                 WARaftBackend.start(target_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log,
                   bootstrap: false
                 )

        assert :ok = WARaftBackend.install_snapshot(0, snapshot_path, position)

        target_dedicated_dir =
          Ferricstore.Store.Promotion.dedicated_path(target_ctx.data_dir, 0, :hash, promoted_key)

        assert File.read(Path.join(target_dedicated_dir, "00000000000000000001.log")) ==
                 {:ok, "dedicated-payload"}
      end

      test "snapshot install persists installed segment projection across restart", %{root: root} do
        source_root = Path.join(root, "projection-source")
        target_root = Path.join(root, "projection-target")
        File.mkdir_p!(source_root)
        File.mkdir_p!(target_root)

        source_ctx = build_ctx(source_root)

        assert :ok =
                 WARaftBackend.start(source_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert :ok = WARaftBackend.write(0, {:put, "snapshot:projection:new", "new", 0})
        assert {:ok, {:raft_log_pos, index, term} = position} = WARaftBackend.create_snapshot(0)

        snapshot_path =
          Path.join([
            source_root,
            "waraft",
            "ferricstore_waraft_backend.1",
            "snapshot.#{index}.#{term}"
          ])

        assert {:ok, {^position, count}} = read_snapshot_segment_projection_header(snapshot_path)
        assert count > 0

        assert :ok = WARaftBackend.stop()
        FerricStore.Instance.cleanup(source_ctx.name)

        target_ctx = build_ctx(target_root)

        assert :ok =
                 WARaftBackend.start(target_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log,
                   bootstrap: false
                 )

        assert :ok = WARaftBackend.install_snapshot(0, snapshot_path, position)
        assert_eventually(fn -> Router.get(target_ctx, "snapshot:projection:new") end, "new")
        assert :ok = WARaftBackend.stop()
        FerricStore.Instance.cleanup(target_ctx.name)

        restarted_ctx = build_ctx(target_root)

        try do
          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     bootstrap: false
                   )

          assert_eventually(fn -> Router.get(restarted_ctx, "snapshot:projection:new") end, "new")
          assert {:ok, {^position, ^count}} = read_segment_projection_header(target_root, 0)
        after
          FerricStore.Instance.cleanup(restarted_ctx.name)
        end
      end

      test "snapshot install preserves logical value size for blob-backed projection rows", %{
        root: root
      } do
        source_root = Path.join(root, "projection-blob-source")
        target_root = Path.join(root, "projection-blob-target")
        File.mkdir_p!(source_root)
        File.mkdir_p!(target_root)

        source_ctx = build_ctx(source_root)
        key = "snapshot:projection:blob:size"
        payload = :binary.copy("projected-blob-payload", 16_384)

        assert byte_size(payload) > source_ctx.blob_side_channel_threshold_bytes

        assert :ok =
                 WARaftBackend.start(source_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert :ok = WARaftBackend.write(0, {:put, key, payload, 0})
        assert byte_size(payload) == Router.value_size(source_ctx, key)
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

        try do
          assert :ok =
                   WARaftBackend.start(target_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     bootstrap: false
                   )

          assert :ok = WARaftBackend.install_snapshot(0, snapshot_path, position)
          assert payload == Router.get(target_ctx, key)
          assert byte_size(payload) == Router.value_size(target_ctx, key)

          assert [
                   {^key, nil, 0, _lfu, {:waraft_projection, projection_index}, _offset,
                    value_size}
                 ] = :ets.lookup(elem(target_ctx.keydir_refs, 0), key)

          assert is_integer(projection_index) and projection_index > 0
          assert value_size == byte_size(payload)
        after
          WARaftBackend.stop()
          FerricStore.Instance.cleanup(target_ctx.name)
        end
      end

      test "snapshot install carries Flow apply-projection value log locators", %{root: root} do
        source_root = Path.join(root, "apply-projection-source")
        target_root = Path.join(root, "apply-projection-target")
        File.mkdir_p!(source_root)
        File.mkdir_p!(target_root)

        clear_apply_projection_cache!()

        source_ctx = build_ctx(source_root)

        assert :ok =
                 WARaftBackend.start(source_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        payload_ref = "f:{snapshot-apply-projection}:v:p:payload:1"
        payload = :binary.copy("snapshot-apply-projection-payload", 128)
        encoded_payload = Ferricstore.Flow.encode_value(payload)

        {_log, value_index} =
          append_waraft_fence!("snapshot-apply-projection:fence", "v")

        {_lfu, value_offset, value_size} =
          insert_apply_projection_ref!(
            source_root,
            source_ctx,
            value_index,
            payload_ref,
            encoded_payload
          )

        lmdb_path =
          source_ctx.data_dir
          |> Ferricstore.DataDir.shard_data_path(0)
          |> Ferricstore.Flow.LMDB.path()

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(
                   lmdb_path,
                   Ferricstore.Flow.LMDB.segment_value_pin_batch_put_ops([
                     {payload_ref, 0, {:waraft_apply_projection, value_index}, value_offset,
                      value_size}
                   ])
                 )

        :ets.delete(elem(source_ctx.keydir_refs, 0), payload_ref)
        assert {:ok, [^payload]} = Ferricstore.Flow.value_mget(source_ctx, [payload_ref])

        assert {:ok, {:raft_log_pos, index, term} = position} = WARaftBackend.create_snapshot(0)

        snapshot_path =
          Path.join([
            source_root,
            "waraft",
            "ferricstore_waraft_backend.1",
            "snapshot.#{index}.#{term}"
          ])

        assert [_ | _] =
                 snapshot_path
                 |> Path.join("apply_projection_log")
                 |> apply_projection_segment_files()

        assert :ok = WARaftBackend.stop()
        FerricStore.Instance.cleanup(source_ctx.name)
        clear_apply_projection_cache!()

        target_ctx = build_ctx(target_root)

        assert :ok =
                 WARaftBackend.start(target_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log,
                   bootstrap: false
                 )

        assert :ok = WARaftBackend.install_snapshot(0, snapshot_path, position)

        assert [_ | _] =
                 target_root
                 |> waraft_apply_projection_root(0)
                 |> apply_projection_segment_files()

        assert :ok = WARaftBackend.stop()
        FerricStore.Instance.cleanup(target_ctx.name)
        clear_apply_projection_cache!()

        restarted_ctx = build_ctx(target_root)

        assert :ok =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log,
                   bootstrap: false
                 )

        assert [_ | _] =
                 target_root
                 |> waraft_apply_projection_root(0)
                 |> apply_projection_segment_files()

        assert {:ok, [^payload]} = Ferricstore.Flow.value_mget(restarted_ctx, [payload_ref])
      end

      test "snapshot apply-projection cache flush uses bounded spill chunks", %{root: root} do
        source_root = Path.join(root, "apply-projection-chunked-source")
        File.mkdir_p!(source_root)

        clear_apply_projection_cache!()

        previous_chunk =
          Application.get_env(:ferricstore, :waraft_apply_projection_snapshot_spill_chunk_entries)

        previous_hook = Application.get_env(:ferricstore, :waraft_apply_projection_spill_hook)
        test_pid = self()

        try do
          Application.put_env(
            :ferricstore,
            :waraft_apply_projection_snapshot_spill_chunk_entries,
            2
          )

          Application.put_env(:ferricstore, :waraft_apply_projection_spill_hook, fn batches ->
            count =
              Enum.reduce(batches, 0, fn {_position, entries}, acc ->
                acc + length(entries)
              end)

            send(test_pid, {:apply_projection_snapshot_spill_chunk, count})

            if count <= 2 do
              :ok
            else
              {:error, {:oversized_apply_projection_snapshot_spill, count}}
            end
          end)

          source_ctx = build_ctx(source_root)

          assert :ok =
                   WARaftBackend.start(source_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          for n <- 1..5 do
            {_log, index} =
              append_waraft_fence!("snapshot-apply-projection-chunk:fence:#{n}", "v")

            insert_apply_projection_ref!(
              source_root,
              source_ctx,
              index,
              "f:{snapshot-apply-projection-chunk}:v:p:payload:#{n}",
              Ferricstore.Flow.encode_value("payload-#{n}")
            )
          end

          assert {:ok, _position} = WARaftBackend.create_snapshot(0)

          assert_receive {:apply_projection_snapshot_spill_chunk, count1}, 1_000
          assert count1 <= 2
          assert_receive {:apply_projection_snapshot_spill_chunk, count2}, 1_000
          assert count2 <= 2
          assert_receive {:apply_projection_snapshot_spill_chunk, count3}, 1_000
          assert count3 <= 2
          refute_receive {:apply_projection_snapshot_spill_chunk, _count}, 50
          assert apply_projection_cache_rows(source_root, 0) == 0
        after
          clear_apply_projection_cache!()
          restore_env(:waraft_apply_projection_snapshot_spill_chunk_entries, previous_chunk)
          restore_env(:waraft_apply_projection_spill_hook, previous_hook)
        end
      end

      test "snapshot payload copy fsyncs copied files before publishing", %{root: root} do
        source_root = Path.join(root, "fsync-source")
        target_root = Path.join(root, "fsync-target")
        File.mkdir_p!(source_root)
        File.mkdir_p!(target_root)

        source_ctx = build_ctx(source_root)
        test_pid = self()
        previous_hook = Application.get_env(:ferricstore, :waraft_snapshot_fsync_file_hook)

        try do
          assert :ok =
                   WARaftBackend.start(source_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert :ok = WARaftBackend.write(0, {:put, "snapshot:fsync", "value", 0})

          Application.put_env(:ferricstore, :waraft_snapshot_fsync_file_hook, fn path ->
            send(test_pid, {:snapshot_payload_fsync, path})
            :ok
          end)

          assert {:ok, {:raft_log_pos, index, term} = position} = WARaftBackend.create_snapshot(0)

          snapshot_path =
            Path.join([
              source_root,
              "waraft",
              "ferricstore_waraft_backend.1",
              "snapshot.#{index}.#{term}"
            ])

          assert_receive_snapshot_payload_fsync(fn create_path ->
            String.starts_with?(create_path, snapshot_path)
          end)

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(source_ctx.name)

          target_ctx = build_ctx(target_root)

          assert :ok =
                   WARaftBackend.start(target_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     bootstrap: false
                   )

          assert :ok = WARaftBackend.install_snapshot(0, snapshot_path, position)

          install_path =
            assert_receive_snapshot_payload_fsync(fn install_path ->
              String.starts_with?(install_path, target_root) and
                String.contains?(install_path, "snapshot_install_staging")
            end)

          assert install_path =~ "snapshot_install_staging"
          assert_eventually(fn -> Router.get(target_ctx, "snapshot:fsync") end, "value")
        after
          restore_env(:waraft_snapshot_fsync_file_hook, previous_hook)
        end
      end

      test "storage rejects incomplete snapshots without wiping live shard data", %{
        root: root,
        ctx: ctx
      } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "snapshot:keep", "old", 0})
        assert "old" == Router.get(ctx, "snapshot:keep")
        assert :ok = WARaftBackend.stop()
        assert shard_payload_present?(ctx, 0)

        snapshot_path = Path.join(root, "incomplete-snapshot")
        position = {:raft_log_pos, 10, 1}
        File.mkdir_p!(snapshot_path)

        File.write!(
          Path.join(snapshot_path, "ferricstore_snapshot.term"),
          :erlang.term_to_binary(%{
            version: 1,
            position: position,
            label: nil,
            config: nil,
            apply_context: ctx.apply_context
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

        assert {:error, {:missing_snapshot_dir, :data, _path}} =
                 Ferricstore.Raft.WARaftStorage.open_snapshot(snapshot_path, position, handle)

        assert shard_payload_present?(ctx, 0)
        FerricStore.Instance.cleanup(ctx.name)
        recovered_ctx = build_ctx(root)

        assert :ok =
                 WARaftBackend.start(recovered_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert "old" == Router.get(recovered_ctx, "snapshot:keep")
      end

      @tag :waraft_noncanonical_snapshot_metadata
      test "storage rejects noncanonical snapshot metadata terms", %{
        root: root,
        ctx: ctx
      } do
        position = {:raft_log_pos, 10, 1}

        metadata = %{
          version: 1,
          position: position,
          label: nil,
          config: nil,
          apply_context: ctx.apply_context
        }

        canonical = :erlang.term_to_binary(metadata, [:deterministic])

        encodings = [
          :erlang.term_to_binary(metadata, compressed: 9),
          canonical <> <<0>>
        ]

        handle = %{
          ctx: ctx,
          shard_index: 0,
          root_dir: Path.join([root, "waraft", "ferricstore_waraft_backend.1"]),
          sm_state: nil,
          position: {:raft_log_pos, 2, 1},
          label: nil,
          config: nil
        }

        Enum.with_index(encodings, fn encoded, index ->
          snapshot_path = Path.join(root, "noncanonical-snapshot-#{index}")
          File.mkdir_p!(snapshot_path)
          File.write!(Path.join(snapshot_path, "ferricstore_snapshot.term"), encoded)

          assert {:error, {:decode_snapshot_metadata, _reason}} =
                   Ferricstore.Raft.WARaftStorage.open_snapshot(
                     snapshot_path,
                     position,
                     handle
                   )
        end)
      end

      test "storage recreates missing snapshot dirs declared empty during install", %{
        root: root,
        ctx: ctx
      } do
        snapshot_path = Path.join(root, "metadata-only-empty-snapshot")
        position = {:raft_log_pos, 0, 0}
        File.mkdir_p!(snapshot_path)

        File.write!(
          Path.join(snapshot_path, "ferricstore_snapshot.term"),
          :erlang.term_to_binary(%{
            version: 1,
            position: position,
            label: nil,
            config: nil,
            apply_context: ctx.apply_context,
            payload_dirs: [:data, :blob, :prob],
            empty_payload_dirs: [:data, :blob, :prob]
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

        assert {:ok, new_handle} =
                 Ferricstore.Raft.WARaftStorage.open_snapshot(snapshot_path, position, handle)

        assert new_handle.position == position

        Enum.each(Keyword.take(shard_dir_specs(ctx, 0), [:data, :blob, :prob]), fn {_kind, path} ->
          assert File.dir?(path)
        end)

        refute File.exists?(Path.join(handle.root_dir, "snapshot_install.term"))
      end

      @tag :empty_snapshot_apply_context
      test "empty snapshots persist and install the backend apply context", %{
        root: root,
        ctx: ctx
      } do
        persisted_context =
          Ferricstore.Raft.ApplyContext.new(flow_default_history_max_events: 23)

        local_context =
          Ferricstore.Raft.ApplyContext.new(flow_default_history_max_events: 99)

        snapshot_ctx = %{ctx | apply_context: persisted_context}
        install_ctx = %{ctx | apply_context: local_context}
        table = :ferricstore_empty_snapshot_apply_context_test
        context_key = {{WARaftBackend, :context}, table}
        previous_context = :persistent_term.get(context_key, :missing)

        :persistent_term.put(context_key, snapshot_ctx)

        on_exit(fn ->
          case previous_context do
            :missing -> :persistent_term.erase(context_key)
            previous -> :persistent_term.put(context_key, previous)
          end
        end)

        snapshot_path = Path.join(root, "empty-snapshot-with-apply-context")
        position = {:raft_log_pos, 0, 0}

        assert :ok =
                 WARaftStorage.make_empty_snapshot(
                   %{table: table},
                   snapshot_path,
                   position,
                   %{},
                   nil
                 )

        metadata =
          snapshot_path
          |> Path.join("ferricstore_snapshot.term")
          |> File.read!()
          |> :erlang.binary_to_term([:safe])

        assert {:ok, ^persisted_context} =
                 Ferricstore.Raft.ApplyContext.decode(metadata.apply_context)

        handle = %{
          ctx: install_ctx,
          shard_index: 0,
          root_dir: Path.join([root, "waraft", "ferricstore_waraft_backend.1"]),
          sm_state: nil,
          position: position,
          label: nil,
          config: nil
        }

        assert {:ok, new_handle} =
                 WARaftStorage.open_snapshot(snapshot_path, position, handle)

        assert new_handle.sm_state.apply_context == persisted_context
      end
    end
  end
end
