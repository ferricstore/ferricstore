defmodule Ferricstore.Raft.WARaftBackendTest.Sections.WaraftStorageRecoveryReusesSegmentLocationsFlowReplay do
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

      test "WARaft storage recovery reuses segment locations for Flow replay", %{
        root: root,
        ctx: ctx
      } do
        {id, partition_key, key} = flow_key_for_shard(ctx, 0, "storage-flow-fast-forward")

        attrs = %{
          id: id,
          type: "storage-flow-fast-forward",
          state: "queued",
          partition_key: partition_key,
          run_at_ms: 1,
          now_ms: 1,
          payload: "payload-v1"
        }

        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:flow_create, key, attrs})

        assert {:ok, {:raft_log_pos, applied_index, _term} = applied_position} =
                 WARaftBackend.storage_position(0)

        assert applied_index > 1
        assert :ok = WARaftBackend.stop()

        storage_root = waraft_storage_root(root, 0)
        metadata = waraft_latest_storage_metadata(root, 0)
        File.rm_rf!(Path.join(storage_root, "segment_projection_log"))

        write_waraft_storage_metadata!(
          root,
          0,
          Map.put(metadata, :position, {:raft_log_pos, 1, 1})
        )

        FerricStore.Instance.cleanup(ctx.name)
        restarted_ctx = build_ctx(root)
        context_key = {{WARaftBackend, :context}, :ferricstore_waraft_backend}
        :persistent_term.put(context_key, restarted_ctx)

        try do
          handle =
            WARaftStorage.open(%{table: :ferricstore_waraft_backend, partition: 1}, storage_root)

          try do
            keydir_rows = :ets.tab2list(elem(restarted_ctx.keydir_refs, 0))

            assert Enum.any?(keydir_rows, fn
                     {^key, _value, _expire_at_ms, _lfu, {:waraft_segment, ^applied_index},
                      _offset, _value_size} ->
                       true

                     _row ->
                       false
                   end)

            refute Enum.any?(keydir_rows, fn
                     {_key, _value, _expire_at_ms, _lfu, {:waraft_apply_projection, _index},
                      _offset, _value_size} ->
                       true

                     _row ->
                       false
                   end)

            assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(
                     restarted_ctx.data_dir,
                     0
                   ) == 0

            assert WARaftStorage.position(handle) == applied_position
          after
            WARaftStorage.close(handle)
          end
        after
          :persistent_term.erase(context_key)
          FerricStore.Instance.cleanup(restarted_ctx.name)
        end
      end

      test "WARaft storage emits startup phase telemetry", %{ctx: ctx} do
        parent = self()
        handler_id = "waraft-storage-startup-phase-#{System.unique_integer([:positive])}"

        :telemetry.attach(
          handler_id,
          [:ferricstore, :waraft, :storage, :startup_phase],
          &__MODULE__.handle_storage_startup_phase_telemetry/4,
          parent
        )

        try do
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert_receive {:waraft_storage_startup_phase,
                          [:ferricstore, :waraft, :storage, :startup_phase],
                          %{duration_us: duration_us}, %{phase: :read_metadata, shard_index: 0}},
                         1_000

          assert is_integer(duration_us)
          assert duration_us >= 0

          assert_receive {:waraft_storage_startup_phase,
                          [:ferricstore, :waraft, :storage, :startup_phase],
                          %{duration_us: build_us}, %{phase: :build_state, shard_index: 0}},
                         1_000

          assert is_integer(build_us)
          assert build_us >= 0
        after
          :telemetry.detach(handler_id)
        end
      end

      test "segment-keydir storage persists replay cursor on hot write interval by default",
           %{root: root, ctx: ctx} do
        parent = self()
        previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)
        previous_every = Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)

        previous_hook =
          Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)

        try do
          Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_keydir)
          Application.delete_env(:ferricstore, :waraft_storage_metadata_persist_every)

          Application.put_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook, fn path ->
            send(parent, {:storage_metadata_fsync, path})
            :ok
          end)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          drain_storage_metadata_fsyncs()

          for index <- 1..1_100 do
            assert :ok =
                     WARaftBackend.write(0, {:put, "segment-metadata-default:#{index}", "v", 0})
          end

          assert_receive {:storage_metadata_fsync, path}, 1_000
          assert String.ends_with?(path, "ferricstore_storage.term.journal")

          assert %{position: {:raft_log_pos, persisted_index, _term}} =
                   waraft_latest_storage_metadata(root, 0)

          assert persisted_index >= 1_024
        after
          restore_env(:waraft_storage_apply_mode, previous_mode)
          restore_env(:waraft_storage_metadata_persist_every, previous_every)
          restore_env(:waraft_storage_metadata_fsync_file_hook, previous_hook)
        end
      end

      test "segment-native WARaft apply bypasses standalone Bitcask fsync hook", %{ctx: ctx} do
        parent = self()
        previous_hook = Application.get_env(:ferricstore, :standalone_durability_hook)

        previous_metadata_hook =
          Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)

        try do
          Application.put_env(:ferricstore, :standalone_durability_hook, fn _file_path, batch ->
            send(parent, {:unexpected_standalone_sync, batch})
            {:error, :standalone_sync_used}
          end)

          Application.put_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook, fn path ->
            send(parent, {:storage_metadata_fsync, path})
            :ok
          end)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          drain_storage_metadata_fsyncs()

          assert :ok = WARaftBackend.write(0, {:put, "nosync-apply:k", "v", 0})
          assert "v" == Router.get(ctx, "nosync-apply:k")
          refute_receive {:unexpected_standalone_sync, _batch}, 100
          refute_receive {:storage_metadata_fsync, _path}, 100
        after
          restore_env(:standalone_durability_hook, previous_hook)
          restore_env(:waraft_storage_metadata_fsync_file_hook, previous_metadata_hook)
        end
      end

      test "segment-native WARaft close preserves values without separate Bitcask payload fsync",
           %{root: root, ctx: ctx} do
        parent = self()
        previous_hook = Application.get_env(:ferricstore, :waraft_bitcask_payload_fsync_file_hook)

        try do
          Application.put_env(:ferricstore, :waraft_bitcask_payload_fsync_file_hook, fn path ->
            send(parent, {:waraft_payload_fsync, path})
            :ok
          end)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          assert :ok = WARaftBackend.write(0, {:put, "nosync-close:k", "v", 0})
          assert "v" == Router.get(ctx, "nosync-close:k")

          assert :ok = WARaftBackend.stop()
          refute_receive {:waraft_payload_fsync, _path}, 100

          FerricStore.Instance.cleanup(ctx.name)
          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert_eventually(fn -> Router.get(restarted_ctx, "nosync-close:k") end, "v")
        after
          restore_env(:waraft_bitcask_payload_fsync_file_hook, previous_hook)
        end
      end

      test "segment-native WARaft metadata advances from WAL position without payload fsync frontier",
           %{root: root, ctx: ctx} do
        parent = self()

        previous_payload_hook =
          Application.get_env(:ferricstore, :waraft_bitcask_payload_fsync_file_hook)

        previous_metadata_hook =
          Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)

        try do
          Application.put_env(:ferricstore, :waraft_bitcask_payload_fsync_file_hook, fn path ->
            send(parent, {:waraft_payload_fsync, path})
            :ok
          end)

          Application.put_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook, fn path ->
            send(parent, {:storage_metadata_fsync, path})
            :ok
          end)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          drain_storage_metadata_fsyncs()

          assert :ok = WARaftBackend.write(0, {:put, "nosync-frontier:k1", "v1", 0})
          refute_receive {:waraft_payload_fsync, _path}, 100
          refute_receive {:storage_metadata_fsync, _path}, 100

          assert :ok = WARaftBackend.write(0, {:put, "nosync-frontier:k2", "v2", 0})
          refute_receive {:waraft_payload_fsync, _path}, 100
          refute_receive {:storage_metadata_fsync, _path}, 100

          assert :ok = WARaftBackend.stop()
          assert_receive {:storage_metadata_fsync, metadata_path}, 1_000
          assert String.contains?(metadata_path, "ferricstore_storage.term")

          assert %{position: {:raft_log_pos, durable_index, _term}} =
                   waraft_latest_storage_metadata(root, 0)

          assert durable_index >= 4
          FerricStore.Instance.cleanup(ctx.name)
          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert_eventually(fn -> Router.get(restarted_ctx, "nosync-frontier:k1") end, "v1")
          assert_eventually(fn -> Router.get(restarted_ctx, "nosync-frontier:k2") end, "v2")
        after
          restore_env(:waraft_bitcask_payload_fsync_file_hook, previous_payload_hook)
          restore_env(:waraft_storage_metadata_fsync_file_hook, previous_metadata_hook)
        end
      end

      test "segment-native WARaft status reports WAL-backed durable position", %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert {:ok, pre_write_position} = WARaftBackend.storage_position(0)

        assert :ok = WARaftBackend.write(0, {:put, "nosync-status:k", "v", 0})
        assert {:ok, post_write_position} = WARaftBackend.storage_position(0)
        assert post_write_position != pre_write_position

        status = waraft_storage_status(0)

        assert Keyword.fetch!(status, :applied_position) == post_write_position
        assert Keyword.fetch!(status, :durable_position) == post_write_position
        assert Keyword.fetch!(status, :payload_dirty?) == false
      end

      test "segment-native WARaft config metadata persists without separate Bitcask payload fsync",
           %{ctx: ctx} do
        parent = self()

        previous_payload_hook =
          Application.get_env(:ferricstore, :waraft_bitcask_payload_fsync_file_hook)

        previous_metadata_hook =
          Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)

        try do
          Application.put_env(:ferricstore, :waraft_bitcask_payload_fsync_file_hook, fn path ->
            send(parent, {:waraft_payload_fsync, path})
            :ok
          end)

          Application.put_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook, fn path ->
            send(parent, {:storage_metadata_fsync, path})
            :ok
          end)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          drain_storage_metadata_fsyncs()

          assert :ok = WARaftBackend.write(0, {:put, "nosync-config:k", "v", 0})
          refute_receive {:waraft_payload_fsync, _path}, 100
          refute_receive {:storage_metadata_fsync, _path}, 100

          assert {:ok, {:raft_log_pos, _index, _term}} =
                   WARaftBackend.adjust_membership(0, :add_participant, :config_payload@node)

          refute_receive {:waraft_payload_fsync, _path}, 100
          assert_receive {:storage_metadata_fsync, metadata_path}, 1_000
          assert String.contains?(metadata_path, "ferricstore_storage.term")

          status = waraft_storage_status(0)
          assert Keyword.fetch!(status, :payload_dirty?) == false
        after
          restore_env(:waraft_bitcask_payload_fsync_file_hook, previous_payload_hook)
          restore_env(:waraft_storage_metadata_fsync_file_hook, previous_metadata_hook)
        end
      end

      test "segment-native WARaft deterministic no-ops do not dirty payload",
           %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "nosync-wrongtype:k", "v", 0})
        assert :ok = WARaftBackend.write(0, {:put, "nosync-wrongtype:flush", "v", 0})
        assert Keyword.fetch!(waraft_storage_status(0), :payload_dirty?) == false

        assert {:error, wrongtype} =
                 Ferricstore.Commands.Hash.handle_ast(
                   {:hset, ["nosync-wrongtype:k", "field", "value"]},
                   ctx
                 )

        assert wrongtype =~ "WRONGTYPE"
        assert Keyword.fetch!(waraft_storage_status(0), :payload_dirty?) == false
        assert "v" == Router.get(ctx, "nosync-wrongtype:k")

        assert 0 = WARaftBackend.write(0, {:cas, "nosync-wrongtype:k", "not-v", "new", 0})
        assert Keyword.fetch!(waraft_storage_status(0), :payload_dirty?) == false

        assert is_nil(
                 WARaftBackend.write(0, {:set, "nosync-wrongtype:k", "nx-skip", 0, %{nx: true}})
               )

        assert Keyword.fetch!(waraft_storage_status(0), :payload_dirty?) == false

        assert is_nil(
                 WARaftBackend.write(
                   0,
                   {:set, "nosync-wrongtype:missing", "xx-skip", 0,
                    %{
                      xx: true
                    }}
                 )
               )

        assert Keyword.fetch!(waraft_storage_status(0), :payload_dirty?) == false

        encoded_ref = BlobRef.encode!(BlobRef.from_segment("blob-ref-skip", 0, 0))

        assert is_nil(
                 WARaftBackend.write(
                   0,
                   {:set_blob_ref, "nosync-wrongtype:blob-missing", encoded_ref, 0, %{xx: true}}
                 )
               )

        assert Keyword.fetch!(waraft_storage_status(0), :payload_dirty?) == false
      end

      test "WARaft snapshot exports segment-projected payload without source Bitcask fsync",
           %{root: root, ctx: ctx} do
        parent = self()

        previous_payload_hook =
          Application.get_env(:ferricstore, :waraft_bitcask_payload_fsync_file_hook)

        previous_snapshot_hook =
          Application.get_env(:ferricstore, :waraft_snapshot_fsync_file_hook)

        try do
          Application.put_env(:ferricstore, :waraft_bitcask_payload_fsync_file_hook, fn path ->
            send(parent, {:source_payload_fsync, path})
            :ok
          end)

          Application.put_env(:ferricstore, :waraft_snapshot_fsync_file_hook, fn path ->
            send(parent, {:snapshot_payload_fsync, path})
            :ok
          end)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          assert :ok = WARaftBackend.write(0, {:put, "nosync-snapshot:k", "v", 0})
          assert Keyword.fetch!(waraft_storage_status(0), :payload_dirty?) == false

          assert {:ok, {:raft_log_pos, index, term} = position} = WARaftBackend.create_snapshot(0)

          snapshot_path =
            Path.join([
              root,
              "waraft",
              "ferricstore_waraft_backend.1",
              "snapshot.#{index}.#{term}"
            ])

          refute_receive {:source_payload_fsync, _path}, 100

          metadata =
            Path.join(snapshot_path, "ferricstore_snapshot.term")
            |> File.read!()
            |> :erlang.binary_to_term([:safe])

          assert %{segment_projection: %{format: :segment_log, count: count}} = metadata
          assert count > 0

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          target_root = Path.join(root, "nosync-snapshot-target")
          File.mkdir_p!(target_root)
          target_ctx = build_ctx(target_root)

          assert :ok =
                   WARaftBackend.start(target_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     bootstrap: false
                   )

          assert :ok = WARaftBackend.install_snapshot(0, snapshot_path, position)
          assert_eventually(fn -> Router.get(target_ctx, "nosync-snapshot:k") end, "v")
          FerricStore.Instance.cleanup(target_ctx.name)
        after
          restore_env(:waraft_bitcask_payload_fsync_file_hook, previous_payload_hook)
          restore_env(:waraft_snapshot_fsync_file_hook, previous_snapshot_hook)
        end
      end

      test "storage metadata fsync failure does not advance replay position", %{
        root: root,
        ctx: ctx
      } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert {:ok, pre_write_position} = WARaftBackend.storage_position(0)

        previous_hook =
          Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)

        previous_every = Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)

        previous_payload_every =
          Application.get_env(:ferricstore, :waraft_bitcask_payload_fsync_every)

        try do
          Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 1)
          Application.put_env(:ferricstore, :waraft_bitcask_payload_fsync_every, 1)

          Application.put_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook, fn _path ->
            {:error, :forced_metadata_fsync_failure}
          end)

          assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
                   WARaftBackend.write(0, {:put, "metadata-fsync-fail:k", "v", 0})

          assert {:ok, ^pre_write_position} = WARaftBackend.storage_position(0)

          restore_env(:waraft_storage_metadata_fsync_file_hook, previous_hook)

          assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
                   WARaftBackend.write(0, {:put, "metadata-fsync-fail:after", "v2", 0})

          assert {:ok, ^pre_write_position} = WARaftBackend.storage_position(0)

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert_eventually(fn -> Router.get(restarted_ctx, "metadata-fsync-fail:k") end, "v")
        after
          restore_env(:waraft_storage_metadata_fsync_file_hook, previous_hook)
          restore_env(:waraft_storage_metadata_persist_every, previous_every)
          restore_env(:waraft_bitcask_payload_fsync_every, previous_payload_every)
        end
      end

      test "storage metadata directory fsync failure does not advance replay position", %{
        ctx: ctx
      } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert {:ok, pre_write_position} = WARaftBackend.storage_position(0)
        previous_hook = Application.get_env(:ferricstore, :waraft_storage_fsync_dir_hook)
        previous_every = Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)

        previous_payload_every =
          Application.get_env(:ferricstore, :waraft_bitcask_payload_fsync_every)

        previous_compact_every =
          Application.get_env(:ferricstore, :waraft_storage_metadata_compact_every)

        try do
          Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 1)
          Application.put_env(:ferricstore, :waraft_bitcask_payload_fsync_every, 1)
          Application.put_env(:ferricstore, :waraft_storage_metadata_compact_every, 1)

          Application.put_env(:ferricstore, :waraft_storage_fsync_dir_hook, fn path ->
            if String.ends_with?(path, "ferricstore_waraft_backend.1") do
              {:error, :forced_metadata_dir_fsync_failure}
            else
              :ok
            end
          end)

          assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
                   WARaftBackend.write(0, {:put, "metadata-dir-fsync-fail:k", "v", 0})

          assert {:ok, ^pre_write_position} = WARaftBackend.storage_position(0)
        after
          restore_env(:waraft_storage_fsync_dir_hook, previous_hook)
          restore_env(:waraft_storage_metadata_persist_every, previous_every)
          restore_env(:waraft_bitcask_payload_fsync_every, previous_payload_every)
          restore_env(:waraft_storage_metadata_compact_every, previous_compact_every)
        end
      end

      test "membership metadata fsync failure returns unknown outcome and replays after restart",
           %{
             root: root,
             ctx: ctx
           } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        previous_hook =
          Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)

        try do
          Application.put_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook, fn _path ->
            {:error, :forced_membership_metadata_fsync_failure}
          end)

          assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
                   WARaftBackend.adjust_membership(0, :add_participant, :missing@node)

          restore_env(:waraft_storage_metadata_fsync_file_hook, previous_hook)

          assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
                   WARaftBackend.write(0, {:put, "membership-block:after", "v", 0})

          assert nil == Router.get(ctx, "membership-block:after")

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert_eventually(fn -> Router.get(restarted_ctx, "membership-block:after") end, "v")

          assert eventually(fn ->
                   case WARaftBackend.status(0) do
                     status when is_list(status) ->
                       config = Keyword.get(status, :config, %{})

                       participants =
                         Map.get(config, :participants, Map.get(config, :membership, []))

                       {:raft_server_ferricstore_waraft_backend_1, :missing@node} in participants

                     _other ->
                       false
                   end
                 end)
        after
          restore_env(:waraft_storage_metadata_fsync_file_hook, previous_hook)
        end
      end

      test "storage label persists across restart when WARaft labels are enabled", %{
        root: root,
        ctx: ctx
      } do
        assert :ok =
                 WARaftBackend.start(ctx,
                   log_module: :ferricstore_waraft_spike_segment_log,
                   label_module: LabelCounter
                 )

        assert :ok = WARaftBackend.write(0, {:put, "label:k1", "v1", 0})
        assert "v1" == Router.get(ctx, "label:k1")
        assert {:ok, label_before_stop} = waraft_storage_label(0)
        assert is_integer(label_before_stop)

        assert :ok = WARaftBackend.stop()
        FerricStore.Instance.cleanup(ctx.name)

        restarted_ctx = build_ctx(root)

        assert :ok =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log,
                   label_module: LabelCounter
                 )

        assert_eventually(fn -> Router.get(restarted_ctx, "label:k1") end, "v1")
        assert {:ok, ^label_before_stop} = waraft_storage_label(0)

        assert :ok = WARaftBackend.write(0, {:put, "label:k2", "v2", 0})
        assert {:ok, label_after} = waraft_storage_label(0)
        assert label_after > label_before_stop
      end

      test "storage label is persisted with the applied position before graceful close", %{
        root: root,
        ctx: ctx
      } do
        previous_every = Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)

        previous_payload_every =
          Application.get_env(:ferricstore, :waraft_bitcask_payload_fsync_every)

        try do
          Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 1)
          Application.put_env(:ferricstore, :waraft_bitcask_payload_fsync_every, 1)

          assert :ok =
                   WARaftBackend.start(ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     label_module: LabelCounter
                   )

          assert :ok = WARaftBackend.write(0, {:put, "label:crash", "v1", 0})
          assert {:ok, label_after_apply} = waraft_storage_label(0)
          assert is_integer(label_after_apply)

          metadata = waraft_latest_storage_metadata(root, 0)
          assert %{position: {:raft_log_pos, index, _term}, label: ^label_after_apply} = metadata
          assert index >= 3
        after
          restore_env(:waraft_storage_metadata_persist_every, previous_every)
          restore_env(:waraft_bitcask_payload_fsync_every, previous_payload_every)
        end
      end

      test "oversized storage label fails before persisting metadata", %{root: root, ctx: ctx} do
        previous_every = Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)

        previous_payload_every =
          Application.get_env(:ferricstore, :waraft_bitcask_payload_fsync_every)

        try do
          Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 1)
          Application.put_env(:ferricstore, :waraft_bitcask_payload_fsync_every, 1)

          assert :ok =
                   WARaftBackend.start(ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     label_module: OversizedLabel
                   )

          assert {:error, reason} = WARaftBackend.write(0, {:put, "label:too-large", "v", 0})
          assert inspect(reason) =~ "storage_metadata_term_too_large"

          metadata = waraft_latest_storage_metadata(root, 0)
          assert Map.get(metadata, :label) == nil
        after
          WARaftBackend.stop()
          restore_env(:waraft_storage_metadata_persist_every, previous_every)
          restore_env(:waraft_bitcask_payload_fsync_every, previous_payload_every)
        end
      end

      test "metadata fsync failure does not advance storage label", %{root: root, ctx: ctx} do
        previous_every = Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)

        previous_hook =
          Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)

        previous_payload_every =
          Application.get_env(:ferricstore, :waraft_bitcask_payload_fsync_every)

        try do
          Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 1)
          Application.put_env(:ferricstore, :waraft_bitcask_payload_fsync_every, 1)

          assert :ok =
                   WARaftBackend.start(ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     label_module: LabelCounter
                   )

          assert :ok = WARaftBackend.write(0, {:put, "label:failure:before", "v1", 0})
          assert {:ok, label_before_failure} = waraft_storage_label(0)
          assert is_integer(label_before_failure)
          assert %{label: ^label_before_failure} = waraft_latest_storage_metadata(root, 0)

          Application.put_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook, fn _path ->
            {:error, :forced_label_fsync_failure}
          end)

          assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
                   WARaftBackend.write(0, {:put, "label:failure:after", "v2", 0})

          assert {:ok, ^label_before_failure} = waraft_storage_label(0)
          assert %{label: ^label_before_failure} = waraft_latest_storage_metadata(root, 0)
        after
          restore_env(:waraft_storage_metadata_fsync_file_hook, previous_hook)
          restore_env(:waraft_storage_metadata_persist_every, previous_every)
          restore_env(:waraft_bitcask_payload_fsync_every, previous_payload_every)
        end
      end

      test "restart fails closed on bad storage metadata", %{root: root, ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "metadata:keep", "value", 0})
        assert "value" == Router.get(ctx, "metadata:keep")
        assert :ok = WARaftBackend.stop()

        metadata_path =
          Path.join([
            root,
            "waraft",
            "ferricstore_waraft_backend.1",
            "ferricstore_storage.term"
          ])

        File.write!(metadata_path, :erlang.term_to_binary(%{version: 999, position: :bad}))
        FerricStore.Instance.cleanup(ctx.name)
        restarted_ctx = build_ctx(root)

        assert {:error, _reason} =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert shard_payload_present?(restarted_ctx, 0)
      end

      test "restart fails closed when missing metadata sees special payload files", %{
        root: root,
        ctx: ctx
      } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.stop()

        File.rm(waraft_storage_metadata_path(root, 0))
        File.rm(waraft_storage_metadata_previous_path(root, 0))
        File.rm(waraft_storage_metadata_journal_path(root, 0))

        fifo_path = Path.join(Ferricstore.DataDir.shard_data_path(root, 0), "unexpected.fifo")
        assert {_output, 0} = System.cmd("mkfifo", [fifo_path])
        assert {:ok, %{type: :other, size: 0}} = File.lstat(fifo_path)

        FerricStore.Instance.cleanup(ctx.name)
        restarted_ctx = build_ctx(root)

        assert {:error, reason} =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert inspect(reason) =~ "unsafe_snapshot_payload_path"
      end

      test "cluster-member start publishes zero storage metadata before payload apply", %{
        root: root,
        ctx: ctx
      } do
        assert :ok =
                 WARaftBackend.start(ctx,
                   bootstrap: false,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert %{
                 position: {:raft_log_pos, 0, 0},
                 label: nil,
                 config: nil
               } = waraft_storage_metadata(root, 0)

        assert File.exists?(waraft_storage_metadata_path(root, 0))
      end

      test "fresh start ignores rebuildable LMDB projection files when storage metadata is missing",
           %{
             root: root,
             ctx: ctx
           } do
        lmdb_path =
          root
          |> Ferricstore.DataDir.shard_data_path(0)
          |> Ferricstore.Flow.LMDB.path()

        :ok = Ferricstore.FS.mkdir_p(lmdb_path)

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(lmdb_path, [{:put, "projection-only", "v"}])

        assert Ferricstore.Flow.LMDB.env_present?(lmdb_path)

        assert :ok =
                 WARaftBackend.start(ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert %{version: 1, position: {:raft_log_pos, index, _term}} =
                 waraft_storage_metadata(root, 0)

        assert is_integer(index) and index >= 0
      end

      test "restart recovers from torn current storage metadata using previous durable metadata",
           %{
             root: root,
             ctx: ctx
           } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "metadata:torn:before", "v1", 0})
        assert "v1" == Router.get(ctx, "metadata:torn:before")
        assert :ok = WARaftBackend.write(0, {:put, "metadata:torn:after", "v2", 0})
        assert "v2" == Router.get(ctx, "metadata:torn:after")
        assert :ok = WARaftBackend.stop()

        metadata_path = waraft_storage_metadata_path(root, 0)
        assert File.exists?(waraft_storage_metadata_previous_path(root, 0))
        File.write!(metadata_path, <<131, 116, 0, 0, 0, 3, "torn">>)

        FerricStore.Instance.cleanup(ctx.name)
        restarted_ctx = build_ctx(root)

        assert :ok =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert_eventually(fn -> Router.get(restarted_ctx, "metadata:torn:before") end, "v1")
        assert_eventually(fn -> Router.get(restarted_ctx, "metadata:torn:after") end, "v2")
      end
    end
  end
end
