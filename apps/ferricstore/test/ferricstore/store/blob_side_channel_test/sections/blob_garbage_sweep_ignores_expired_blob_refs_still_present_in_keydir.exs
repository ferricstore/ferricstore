defmodule Ferricstore.Store.BlobSideChannelTest.Sections.BlobGarbageSweepIgnoresExpiredBlobRefsStillPresentInKeydir do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Store.{
        BlobRef,
        BlobStore,
        ColdRead,
        CompoundKey,
        LFU,
        LocalTxStore,
        Ops,
        Router
      }

      alias Ferricstore.Store.Shard.Compound, as: ShardCompound
      alias Ferricstore.Store.Shard.ETS, as: ShardETS
      alias Ferricstore.Raft.StateMachine
      alias Ferricstore.Test.IsolatedInstance

      test "blob garbage sweep ignores expired blob refs still present in keydir", %{
        ctx: ctx,
        keydir: keydir
      } do
        Process.put(:ferricstore_blob_store_segment_gc_grace_ms, 0)

        on_exit(fn ->
          Process.delete(:ferricstore_blob_store_segment_gc_grace_ms)
        end)

        key = "blob:gc:expired-ref"
        payload = :binary.copy("E", 1024)

        assert {:ok, blob_ref} = BlobStore.put(ctx.data_dir, 0, payload)
        encoded_ref = BlobRef.encode!(blob_ref)
        assert {:ok, {path, _offset, _size}} = BlobStore.file_ref(ctx.data_dir, 0, blob_ref)
        assert File.exists?(path)

        expired_at = Ferricstore.HLC.now_ms() - 1_000

        :ets.insert(
          keydir,
          {key, encoded_ref, expired_at, LFU.initial(), :memory, 0, byte_size(encoded_ref)}
        )

        assert {:ok, %{deleted_files: 1}} = Router.sweep_blob_garbage(ctx)
        refute File.exists?(path)
      end

      @tag :hlc_drift_guard
      test "blob garbage sweep preserves wall-live refs during unsafe HLC drift", %{
        ctx: ctx,
        keydir: keydir
      } do
        Process.put(:ferricstore_blob_store_segment_gc_grace_ms, 0)
        hlc_ref = :persistent_term.get(:ferricstore_hlc_ref)
        previous_hlc = :atomics.get(hlc_ref, 1)

        try do
          key = "blob:gc:unsafe-drift-ref"
          payload = :binary.copy("D", 1024)
          wall_ms = System.os_time(:millisecond)
          expire_at_ms = wall_ms + 30_000

          assert {:ok, blob_ref} = BlobStore.put(ctx.data_dir, 0, payload)
          encoded_ref = BlobRef.encode!(blob_ref)
          path = BlobRef.path(ctx.data_dir, 0, blob_ref)
          assert File.exists?(path)

          :ets.insert(
            keydir,
            {key, encoded_ref, expire_at_ms, LFU.initial(), :memory, 0, byte_size(encoded_ref)}
          )

          :atomics.put(hlc_ref, 1, Bitwise.bsl(wall_ms + 60_000, 16))

          assert {:ok, %{deleted_files: 0, kept_files: 1}} =
                   Router.sweep_blob_garbage(ctx)

          assert File.exists?(path)
        after
          :atomics.put(hlc_ref, 1, previous_hlc)
          Process.delete(:ferricstore_blob_store_segment_gc_grace_ms)
        end
      end

      test "blob garbage sweep preserves live refs stored behind WARaft segment locations" do
        ctx =
          IsolatedInstance.checkout(
            shard_count: 1,
            hot_cache_max_value_size: 0,
            blob_side_channel_threshold_bytes: 128,
            start_shards: false
          )

        key = "blob:gc:waraft-segment-live"
        payload = :binary.copy("W", 1536)

        try do
          Process.put(:ferricstore_blob_store_segment_gc_grace_ms, 0)

          assert {:ok, _shard} =
                   Ferricstore.Store.Shard.start_link(
                     index: 0,
                     data_dir: ctx.data_dir,
                     instance_ctx: ctx,
                     flow_shared_ref_backfill?: false
                   )

          assert :ok =
                   Ferricstore.Raft.WARaftBackend.start(ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert :ok = Ferricstore.Raft.WARaftBackend.write(0, {:put, key, payload, 0})

          assert [
                   {^key, nil, 0, _lfu, {:waraft_segment, index}, offset, value_size}
                 ] = :ets.lookup(elem(ctx.keydir_refs, 0), key)

          assert is_integer(index) and index > 0
          assert is_integer(offset) and offset >= 0
          assert value_size == byte_size(payload)
          assert payload == Router.get(ctx, key)

          assert {:ok, _stats} = Router.sweep_blob_garbage(ctx)
          assert payload == Router.get(ctx, key)
        after
          Process.delete(:ferricstore_blob_store_segment_gc_grace_ms)
          Ferricstore.Raft.WARaftBackend.stop()
          IsolatedInstance.checkin(ctx)
        end
      end

      test "blob garbage sweep fails closed without matching WARaft storage", %{
        ctx: ctx,
        shard: shard
      } do
        payload = "dead-without-waraft-storage"
        assert {:ok, ref} = BlobStore.put(ctx.data_dir, 0, payload)
        path = BlobRef.path(ctx.data_dir, 0, ref)
        original_state = :sys.get_state(shard)

        :sys.replace_state(shard, fn state -> %{state | raft?: true} end)

        try do
          assert {:ok,
                  %{
                    deleted_files: 0,
                    deleted_bytes: 0,
                    kept_files: 0,
                    skipped: true,
                    reason: :missing_waraft_storage_metrics
                  }} = Router.sweep_blob_garbage(ctx)

          assert File.exists?(path)
        after
          :sys.replace_state(shard, fn _state -> original_state end)
        end
      end

      test "blob garbage sweep fails closed when a live cold location cannot be read", %{
        ctx: ctx,
        shard: shard,
        keydir: keydir
      } do
        attach_blob_gc_handler()

        payload = "dead-but-live-ref-scan-fails"
        assert {:ok, ref} = BlobStore.put(ctx.data_dir, 0, payload)
        path = BlobRef.path(ctx.data_dir, 0, ref)
        live_key = "blob:gc:missing-live-location"

        :ets.insert(keydir, {live_key, nil, 0, LFU.initial(), 999_999, 0, BlobRef.encoded_size()})

        try do
          assert {:error, {0, {:blob_gc_live_ref_scan_failed, ^live_key, _reason}}} =
                   Router.sweep_blob_garbage(ctx)

          assert_receive {:blob_gc_failed, [:ferricstore, :blob, :gc, :failed], %{count: 1},
                          %{
                            shard_index: 0,
                            reason: {:blob_gc_live_ref_scan_failed, ^live_key, _reason}
                          }}

          assert File.exists?(path)
        after
          :ets.delete(keydir, live_key)
          :sys.replace_state(shard, fn state -> state end)
        end
      end

      test "blob garbage sweep skips cold values whose stored size cannot be a blob ref", %{
        ctx: ctx,
        keydir: keydir
      } do
        Process.put(:ferricstore_blob_store_segment_gc_grace_ms, 0)

        on_exit(fn ->
          Process.delete(:ferricstore_blob_store_segment_gc_grace_ms)
        end)

        payload = "dead-blob-with-unrelated-cold-value"
        assert {:ok, ref} = BlobStore.put(ctx.data_dir, 0, payload)
        path = BlobRef.path(ctx.data_dir, 0, ref)
        cold_key = "blob:gc:non-ref-cold-value"

        :ets.insert(
          keydir,
          {cold_key, nil, 0, LFU.initial(), 999_999, 0, BlobRef.encoded_size() + 1}
        )

        try do
          assert {:ok, %{deleted_files: 1}} = Router.sweep_blob_garbage(ctx)
          refute File.exists?(path)
        after
          :ets.delete(keydir, cold_key)
        end
      end

      test "blob garbage sweep does not delete a blob written after live-ref scan starts", %{
        ctx: ctx,
        keydir: keydir
      } do
        parent = self()
        key = "blob:gc:concurrent-large-write"
        payload = :binary.copy("W", 1024)

        Process.put(:ferricstore_blob_gc_after_live_refs_hook, fn _ctx, 0, _live_refs ->
          task =
            Task.async(fn ->
              result = Router.put(ctx, key, payload, 0)
              send(parent, {:blob_gc_writer_done, result})
              result
            end)

          send(parent, {:blob_gc_writer_task, task})

          refute_receive {:blob_gc_writer_done, :ok}, 50

          :ok
        end)

        on_exit(fn -> Process.delete(:ferricstore_blob_gc_after_live_refs_hook) end)

        assert {:ok, _stats} = Router.sweep_blob_garbage(ctx)
        assert_receive {:blob_gc_writer_task, task}, 1_000
        assert :ok = Task.await(task, 1_000)
        assert {:ok, _encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, key)
        assert File.exists?(BlobRef.path(ctx.data_dir, 0, ref))
        assert payload == Router.get(ctx, key)
      end

      test "blob garbage sweep fails closed when the active Bitcask file cannot fsync", %{
        ctx: ctx,
        shard: shard,
        keydir: keydir
      } do
        attach_blob_gc_handler()

        dead_key = "blob:gc:fsync-fail"
        payload = :binary.copy("D", 1024)

        assert :ok = Router.put(ctx, dead_key, payload, 0)
        assert {:ok, _dead_encoded, dead_ref} = raw_disk_blob_ref(ctx, keydir, dead_key)
        dead_path = BlobRef.path(ctx.data_dir, 0, dead_ref)

        assert :ok = GenServer.call(shard, {:delete, dead_key})

        original_state = :sys.get_state(shard)

        :sys.replace_state(shard, fn state ->
          missing_path = Path.join(state.shard_data_path, "missing-active-for-blob-gc.log")
          %{state | active_file_path: missing_path}
        end)

        try do
          assert {:error, {0, {:blob_gc_active_fsync_failed, _missing_path, _reason}}} =
                   Router.sweep_blob_garbage(ctx)

          assert_receive {:blob_gc_failed, [:ferricstore, :blob, :gc, :failed], %{count: 1},
                          %{
                            shard_index: 0,
                            reason: {:blob_gc_active_fsync_failed, _missing_path, _reason}
                          }}

          assert File.exists?(dead_path)
        after
          :sys.replace_state(shard, fn state ->
            %{
              state
              | active_file_path: original_state.active_file_path,
                active_file_id: original_state.active_file_id,
                active_file_size: original_state.active_file_size
            }
          end)
        end
      end

      test "blob storage stats count complete blob files and bytes", %{ctx: ctx} do
        payload = :binary.copy("S", 1024)

        assert :ok = Router.put(ctx, "blob:stats:one", payload, 0)
        assert :ok = Router.put(ctx, "blob:stats:two", payload <> "2", 0)

        assert {:ok,
                %{
                  files: 1,
                  bytes: bytes,
                  tmp_files: 0,
                  tmp_bytes: 0
                }} = BlobStore.storage_stats(ctx.data_dir)

        assert bytes >= 2048
      end
    end
  end
end
