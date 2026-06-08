defmodule Ferricstore.Raft.WARaftBackendTest.Sections.RejectsVolatileWaraftEtsLogModule do
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

      test "rejects volatile WARaft ETS log module", %{ctx: ctx} do
        assert_raise ArgumentError, ~r/does not support volatile ETS log/, fn ->
          WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)
        end
      end

      test "default WARaft storage uses segment-backed keydir locations",
           %{root: root, ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        key = "default-segment-keydir:k1"
        assert :ok = WARaftBackend.write(0, {:put, key, "v1", 0})
        assert "v1" == Router.get(ctx, key)

        assert [
                 {^key, "v1", 0, _lfu, {:waraft_segment, index}, offset, 2}
               ] = :ets.lookup(elem(ctx.keydir_refs, 0), key)

        assert is_integer(index)
        assert index > 0
        assert is_integer(offset)
        assert offset >= 0

        bitcask_file = Path.join([root, "data", "shard_0", "00000.log"])
        assert File.stat!(bitcask_file).size == 0

        assert :ok = WARaftBackend.stop()

        restarted_ctx = build_ctx(root)

        try do
          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert "v1" == Router.get(restarted_ctx, key)

          assert [{^key, "v1", 0, _lfu, restarted_file_id, restarted_offset, 2}] =
                   :ets.lookup(elem(restarted_ctx.keydir_refs, 0), key)

          case restarted_file_id do
            {:waraft_segment, ^index} ->
              :ok

            {:waraft_projection, projection_index} ->
              assert is_integer(projection_index)
              assert projection_index > 0
          end

          assert is_integer(restarted_offset)
          assert restarted_offset >= 0
          assert File.stat!(bitcask_file).size == 0
        after
          FerricStore.Instance.cleanup(restarted_ctx.name)
        end
      end

      test "pause_writes_for_sync blocks new WARaft writes until resume", %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        key = "sync-pause:k"
        assert :ok = Ferricstore.Raft.Batcher.pause_writes_for_sync(0, 1_000)

        task =
          Task.async(fn ->
            WARaftBackend.write(0, {:put, key, "v1", 0})
          end)

        refute Task.yield(task, 50)

        assert :ok = Ferricstore.Raft.Batcher.resume_writes_for_sync(0, 1_000)
        assert {:ok, :ok} == Task.yield(task, 1_000)
        assert "v1" == Router.get(ctx, key)
      end

      test "overlapping sync pauses require matching resumes before writes continue", %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        key = "sync-pause-overlap:k"
        assert :ok = Ferricstore.Raft.Batcher.pause_writes_for_sync(0, 1_000)
        assert :ok = Ferricstore.Raft.Batcher.pause_writes_for_sync(0, 1_000)

        task =
          Task.async(fn ->
            WARaftBackend.write(0, {:put, key, "v1", 0})
          end)

        refute Task.yield(task, 50)

        assert :ok = Ferricstore.Raft.Batcher.resume_writes_for_sync(0, 1_000)
        refute Task.yield(task, 50)

        assert :ok = Ferricstore.Raft.Batcher.resume_writes_for_sync(0, 1_000)
        assert {:ok, :ok} == Task.yield(task, 1_000)
        assert "v1" == Router.get(ctx, key)
      end

      test "pause_writes_for_sync waits for writes admitted before pause publication", %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        parent = self()

        admitted_writer =
          Task.async(fn ->
            assert {:ok, token} = Ferricstore.Raft.WARaftBackend.SyncGate.enter(0)
            send(parent, :admitted_writer_entered)

            receive do
              :release_admitted_writer -> :ok
            after
              5_000 -> exit(:admitted_writer_release_timeout)
            end

            Ferricstore.Raft.WARaftBackend.SyncGate.leave(token)
          end)

        assert_receive :admitted_writer_entered, 1_000

        pause_task =
          Task.async(fn ->
            Ferricstore.Raft.Batcher.pause_writes_for_sync(0, 5_000)
          end)

        refute Task.yield(pause_task, 50)

        send(admitted_writer.pid, :release_admitted_writer)
        assert :ok = Task.await(admitted_writer, 1_000)
        assert {:ok, :ok} == Task.yield(pause_task, 2_000)
        assert :ok = Ferricstore.Raft.Batcher.resume_writes_for_sync(0, 1_000)
      end

      test "pause_writes_for_sync waits for async batch casts admitted before pause", %{ctx: ctx} do
        previous_window = Application.get_env(:ferricstore, :waraft_hot_batch_window_ms)
        previous_hook = Application.get_env(:ferricstore, :waraft_backend_batcher_cast_hook)

        try do
          Application.put_env(:ferricstore, :waraft_hot_batch_window_ms, 60_000)
          Application.put_env(:ferricstore, :waraft_backend_batcher_cast_hook, {:defer, self()})

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          reply_ref = make_ref()

          assert :ok =
                   WARaftBackend.write_put_batch_async(
                     0,
                     [{"sync-pause-async-cast:k", "v1", 0}],
                     {self(), reply_ref}
                   )

          assert_receive {:waraft_backend_batcher_cast_deferred, cast_ref, cast_worker}, 1_000

          pause_task =
            Task.async(fn ->
              Ferricstore.Raft.Batcher.pause_writes_for_sync(0, 5_000)
            end)

          refute Task.yield(pause_task, 50),
                 "pause must wait for an already-admitted async batch cast to reach the batcher"

          send(cast_worker, {cast_ref, :continue})
          assert_receive {^reply_ref, {:ok, [:ok]}}, 5_000
          assert {:ok, :ok} == Task.yield(pause_task, 5_000)
          assert "v1" == Router.get(ctx, "sync-pause-async-cast:k")

          assert :ok = Ferricstore.Raft.Batcher.resume_writes_for_sync(0, 1_000)
        after
          restore_env(:waraft_hot_batch_window_ms, previous_window)
          restore_env(:waraft_backend_batcher_cast_hook, previous_hook)
        end
      end

      test "pause_writes_for_sync resumes new writes after drain timeout", %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        assert {:ok, token} = Ferricstore.Raft.WARaftBackend.SyncGate.enter(0)

        try do
          assert {:error, :sync_pause_drain_timeout} =
                   Ferricstore.Raft.Batcher.pause_writes_for_sync(0, 10)

          key = "sync-pause-timeout-resume:k"
          assert :ok = WARaftBackend.write(0, {:put, key, "v1", 0})
          assert "v1" == Router.get(ctx, key)
        after
          Ferricstore.Raft.WARaftBackend.SyncGate.leave(token)
          _ = Ferricstore.Raft.Batcher.resume_writes_for_sync(0, 1_000)
        end
      end

      test "pause_writes_for_sync flushes queued sync hot batches before waiting for drain", %{
        ctx: ctx
      } do
        previous_window = Application.get_env(:ferricstore, :waraft_hot_batch_window_ms)
        previous_max = Application.get_env(:ferricstore, :waraft_hot_batch_max)

        try do
          Application.put_env(:ferricstore, :waraft_hot_batch_window_ms, 60_000)
          Application.put_env(:ferricstore, :waraft_hot_batch_max, 10_000)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          key = "sync-pause-sync-hot-batch:k"

          writer =
            Task.async(fn ->
              WARaftBackend.write_put_batch(0, [{key, "v1", 0}])
            end)

          try do
            batcher_name = Ferricstore.Raft.WARaftBackend.Batcher.name(0)
            batcher_pid = Process.whereis(batcher_name)
            assert is_pid(batcher_pid)

            assert eventually(fn ->
                     case :sys.get_state(batcher_pid) do
                       %{put_slot: %{count: count}} when count > 0 -> true
                       _ -> false
                     end
                   end)

            assert :ok = Ferricstore.Raft.Batcher.pause_writes_for_sync(0, 1_000)
            assert {:ok, {:ok, [:ok]}} == Task.yield(writer, 1_000)
            assert "v1" == Router.get(ctx, key)
            assert :ok = Ferricstore.Raft.Batcher.resume_writes_for_sync(0, 1_000)
          after
            _ = Ferricstore.Raft.WARaftBackend.Batcher.flush(0, 1_000)
            _ = Task.shutdown(writer, :brutal_kill)
          end
        after
          restore_env(:waraft_hot_batch_window_ms, previous_window)
          restore_env(:waraft_hot_batch_max, previous_max)
        end
      end

      test "sync gate releases async batch tokens left in a terminating batcher mailbox", %{
        ctx: ctx
      } do
        previous_window = Application.get_env(:ferricstore, :waraft_hot_batch_window_ms)
        previous_trap = Process.flag(:trap_exit, true)

        try do
          Application.put_env(:ferricstore, :waraft_hot_batch_window_ms, 60_000)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          batcher_name = Ferricstore.Raft.WARaftBackend.Batcher.name(0)
          batcher_pid = Process.whereis(batcher_name)
          assert is_pid(batcher_pid)

          assert :ok = :sys.suspend(batcher_pid)
          assert {:ok, put_sync_token} = Ferricstore.Raft.WARaftBackend.SyncGate.enter(0)
          assert {:ok, delete_sync_token} = Ferricstore.Raft.WARaftBackend.SyncGate.enter(0)

          put_reply_ref = make_ref()
          delete_reply_ref = make_ref()

          GenServer.cast(
            batcher_pid,
            {:write_put_batch, [{"sync-token-mailbox-leak:k", "v1", 0}], 60_000,
             {self(), put_reply_ref}, put_sync_token}
          )

          GenServer.cast(
            batcher_pid,
            {:write_delete_batch, ["sync-token-mailbox-leak:k"], 60_000,
             {self(), delete_reply_ref}, delete_sync_token}
          )

          ref = Process.monitor(batcher_pid)
          :sys.terminate(batcher_pid, :shutdown)
          assert_receive {:DOWN, ^ref, :process, ^batcher_pid, :shutdown}, 1_000

          assert :ok = Ferricstore.Raft.Batcher.pause_writes_for_sync(0, 1_000)
          assert :ok = Ferricstore.Raft.Batcher.resume_writes_for_sync(0, 1_000)
          assert_receive {^put_reply_ref, {:error, :shutting_down}}, 1_000
          assert_receive {^delete_reply_ref, {:error, :shutting_down}}, 1_000
        after
          Process.flag(:trap_exit, previous_trap)
          restore_env(:waraft_hot_batch_window_ms, previous_window)
        end
      end

      test "async batch cast lost to dead batcher replies and releases sync token", %{ctx: ctx} do
        previous_window = Application.get_env(:ferricstore, :waraft_hot_batch_window_ms)
        previous_hook = Application.get_env(:ferricstore, :waraft_backend_batcher_cast_hook)
        previous_trap = Process.flag(:trap_exit, true)

        try do
          Application.put_env(:ferricstore, :waraft_hot_batch_window_ms, 60_000)
          Application.put_env(:ferricstore, :waraft_backend_batcher_cast_hook, {:defer, self()})

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          put_reply_ref = make_ref()
          delete_reply_ref = make_ref()

          assert :ok =
                   WARaftBackend.write_put_batch_async(
                     0,
                     [{"sync-token-lost-cast:k", "v1", 0}],
                     {self(), put_reply_ref}
                   )

          assert :ok =
                   WARaftBackend.write_delete_batch_async(
                     0,
                     ["sync-token-lost-cast:k"],
                     {self(), delete_reply_ref}
                   )

          assert_receive {:waraft_backend_batcher_cast_deferred, put_cast_ref, put_cast_worker},
                         1_000

          assert_receive {:waraft_backend_batcher_cast_deferred, delete_cast_ref,
                          delete_cast_worker},
                         1_000

          batcher_name = Ferricstore.Raft.WARaftBackend.Batcher.name(0)
          batcher_pid = Process.whereis(batcher_name)
          assert is_pid(batcher_pid)

          ref = Process.monitor(batcher_pid)
          Process.exit(batcher_pid, :kill)
          assert_receive {:DOWN, ^ref, :process, ^batcher_pid, :killed}, 1_000

          send(put_cast_worker, {put_cast_ref, :continue})
          send(delete_cast_worker, {delete_cast_ref, :continue})

          assert_receive {^put_reply_ref, {:error, :shutting_down}}, 1_000
          assert_receive {^delete_reply_ref, {:error, :shutting_down}}, 1_000
          assert :ok = Ferricstore.Raft.Batcher.pause_writes_for_sync(0, 1_000)
          assert :ok = Ferricstore.Raft.Batcher.resume_writes_for_sync(0, 1_000)
        after
          Process.flag(:trap_exit, previous_trap)
          restore_env(:waraft_hot_batch_window_ms, previous_window)
          restore_env(:waraft_backend_batcher_cast_hook, previous_hook)
        end
      end

      test "unified segment storage uses the Raft segment as the value location", %{
        root: root,
        ctx: ctx
      } do
        previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

        try do
          Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_keydir)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          key = "segment-keydir:k1"
          assert :ok = WARaftBackend.write(0, {:put, key, "v1", 0})
          assert "v1" == Router.get(ctx, key)

          assert [
                   {^key, "v1", 0, _lfu, {:waraft_segment, index}, offset, 2}
                 ] = :ets.lookup(elem(ctx.keydir_refs, 0), key)

          assert is_integer(index) and index > 0
          assert is_integer(offset) and offset >= 0
          assert File.stat!(Path.join([root, "data", "shard_0", "00000.log"])).size == 0

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert "v1" == Router.get(restarted_ctx, key)

          assert [{^key, "v1", 0, _lfu, restarted_file_id, restarted_offset, 2}] =
                   :ets.lookup(elem(restarted_ctx.keydir_refs, 0), key)

          case restarted_file_id do
            {:waraft_segment, ^index} ->
              :ok

            {:waraft_projection, projection_index} ->
              assert is_integer(projection_index) and projection_index > 0
          end

          assert is_integer(restarted_offset) and restarted_offset >= 0
        after
          restore_env(:waraft_storage_apply_mode, previous_mode)
        end
      end

      test "unified segment storage records physical segment offsets in keydir", %{
        ctx: ctx
      } do
        previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

        try do
          Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_keydir)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert :ok = WARaftBackend.write(0, {:put, "segment-keydir:offset-fence", "v1", 0})

          key = "segment-keydir:offset-large"
          value = :binary.copy("v", ctx.hot_cache_max_value_size + 1)

          assert :ok = WARaftBackend.write(0, {:put, key, value, 0})

          assert [
                   {^key, nil, 0, _lfu, {:waraft_segment, index}, offset, value_size}
                 ] = :ets.lookup(elem(ctx.keydir_refs, 0), key)

          assert is_integer(index) and index > 0
          assert offset > 0
          assert value_size == byte_size(value)
          assert value == Router.get(ctx, key)
        after
          restore_env(:waraft_storage_apply_mode, previous_mode)
        end
      end

      test "unified segment put_batch publishes final segment locations directly", %{ctx: ctx} do
        previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

        try do
          Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_keydir)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          small_key = "segment-keydir:batch-small"
          large_key = "segment-keydir:batch-large"
          large_value = :binary.copy("v", ctx.hot_cache_max_value_size + 1)

          assert {:ok, [:ok, :ok]} =
                   WARaftBackend.write_put_batch(0, [
                     {small_key, "small", 0},
                     {large_key, large_value, 0}
                   ])

          assert [{^small_key, "small", 0, _small_lfu, {:waraft_segment, index}, offset, 5}] =
                   :ets.lookup(elem(ctx.keydir_refs, 0), small_key)

          assert [
                   {^large_key, nil, 0, _large_lfu, {:waraft_segment, ^index}, ^offset,
                    large_size}
                 ] = :ets.lookup(elem(ctx.keydir_refs, 0), large_key)

          assert large_size == byte_size(large_value)
          assert is_integer(index) and index > 0
          assert is_integer(offset) and offset >= 0
          assert "small" == Router.get(ctx, small_key)
          assert large_value == Router.get(ctx, large_key)
        after
          restore_env(:waraft_storage_apply_mode, previous_mode)
        end
      end

      test "unified segment fast paths route through lock-aware apply when keys are locked", %{
        ctx: ctx
      } do
        previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

        try do
          Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_keydir)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          owner = make_ref()
          expires_at = Ferricstore.HLC.now_ms() + 60_000

          put_key = "segment-lock:put"
          delete_key = "segment-lock:delete"
          batch_put_locked = "segment-lock:batch-put-locked"
          batch_put_free = "segment-lock:batch-put-free"
          batch_delete_locked = "segment-lock:batch-delete-locked"
          batch_delete_free = "segment-lock:batch-delete-free"

          for key <- [
                put_key,
                delete_key,
                batch_put_locked,
                batch_delete_locked,
                batch_delete_free
              ] do
            assert :ok = WARaftBackend.write(0, {:put, key, "old", 0})
          end

          assert :ok =
                   WARaftBackend.write(0, {
                     :lock_keys,
                     [put_key, delete_key, batch_put_locked, batch_delete_locked],
                     owner,
                     expires_at
                   })

          assert {:error, :key_locked} = WARaftBackend.write(0, {:put, put_key, "new", 0})
          assert {:error, :key_locked} = WARaftBackend.write(0, {:delete, delete_key})

          assert {:ok, [{:error, :key_locked}, :ok]} =
                   WARaftBackend.__commit_put_batch_direct__(0, [
                     {batch_put_locked, "new", 0},
                     {batch_put_free, "new", 0}
                   ])

          assert {:ok, [{:error, :key_locked}, :ok]} =
                   WARaftBackend.__commit_delete_batch_direct__(0, [
                     batch_delete_locked,
                     batch_delete_free
                   ])

          assert "old" == Router.get(ctx, put_key)
          assert "old" == Router.get(ctx, delete_key)
          assert "old" == Router.get(ctx, batch_put_locked)
          assert "new" == Router.get(ctx, batch_put_free)
          assert "old" == Router.get(ctx, batch_delete_locked)
          assert nil == Router.get(ctx, batch_delete_free)

          assert [{^batch_put_free, _value, 0, _lfu, {:waraft_segment, _index}, _offset, _size}] =
                   :ets.lookup(elem(ctx.keydir_refs, 0), batch_put_free)
        after
          restore_env(:waraft_storage_apply_mode, previous_mode)
        end
      end

      test "unified segment fast path stays enabled for keys unrelated to active locks", %{
        ctx: ctx
      } do
        previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

        try do
          Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_keydir)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          owner = make_ref()
          expires_at = Ferricstore.HLC.now_ms() + 60_000
          locked_key = "segment-lock:unrelated-locked"
          free_key = "segment-lock:unrelated-free"

          assert :ok = WARaftBackend.write(0, {:lock_keys, [locked_key], owner, expires_at})
          assert :ok = WARaftBackend.write(0, {:put, free_key, "free", 0})

          assert [{^free_key, _value, 0, _lfu, {:waraft_segment, _index}, _offset, _size}] =
                   :ets.lookup(elem(ctx.keydir_refs, 0), free_key)

          assert "free" == Router.get(ctx, free_key)
        after
          restore_env(:waraft_storage_apply_mode, previous_mode)
        end
      end

      test "unified segment lock expiry uses stamped command time on replay", %{ctx: ctx} do
        previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

        try do
          Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_keydir)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          key = "segment-lock:stamped-expiry"
          owner = make_ref()
          local_now = Ferricstore.HLC.now_ms()
          stamped_now = local_now - 20_000
          expires_after_stamp_before_local_now = stamped_now + 10_000

          assert :ok = WARaftBackend.write(0, {:put, key, "old", 0})

          assert :ok =
                   WARaftBackend.write(0, {
                     :lock_keys,
                     [key],
                     owner,
                     expires_after_stamp_before_local_now
                   })

          assert {:error, :key_locked} =
                   WARaftBackend.write(0, {
                     {:put, key, "new", 0},
                     %{hlc_ts: {stamped_now, 0}}
                   })

          assert "old" == Router.get(ctx, key)
        after
          restore_env(:waraft_storage_apply_mode, previous_mode)
        end
      end

      test "unified segment storage keeps Flow value and history payloads cold", %{ctx: ctx} do
        previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

        try do
          Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_keydir)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          normal_key = "segment-keydir:hot-normal"

          flow_value_key =
            Ferricstore.Flow.Keys.value_key("cold-flow", :payload, 1, "tenant-cold")

          flow_history_key = Ferricstore.Flow.Keys.history_key("cold-flow", "tenant-cold")

          assert {:ok, [:ok, :ok, :ok]} =
                   WARaftBackend.write_put_batch(0, [
                     {normal_key, "normal", 0},
                     {flow_value_key, "small-payload-value", 0},
                     {flow_history_key, "small-history-value", 0}
                   ])

          assert [{^normal_key, "normal", 0, _lfu, {:waraft_segment, _index}, _offset, 6}] =
                   :ets.lookup(elem(ctx.keydir_refs, 0), normal_key)

          assert [
                   {^flow_value_key, nil, 0, _lfu, {:waraft_segment, value_index}, value_offset,
                    value_size}
                 ] = :ets.lookup(elem(ctx.keydir_refs, 0), flow_value_key)

          assert is_integer(value_index) and value_index > 0
          assert is_integer(value_offset) and value_offset >= 0
          assert value_size == byte_size("small-payload-value")

          assert [
                   {^flow_history_key, nil, 0, _lfu, {:waraft_segment, history_index},
                    history_offset, history_size}
                 ] = :ets.lookup(elem(ctx.keydir_refs, 0), flow_history_key)

          assert is_integer(history_index) and history_index > 0
          assert is_integer(history_offset) and history_offset >= 0
          assert history_size == byte_size("small-history-value")

          assert "small-payload-value" == Router.get(ctx, flow_value_key)
          assert "small-history-value" == Router.get(ctx, flow_history_key)
        after
          restore_env(:waraft_storage_apply_mode, previous_mode)
        end
      end

      test "unified segment string put reuses the keydir lookup before compound clear" do
        source = Ferricstore.Test.SourceFiles.waraft_storage_source()

        assert source =~ "previous = :ets.lookup(shard_state.keydir, key)"
        assert source =~ "segment_project_clear_compound_for_string_put(sm_state, key, previous)"
        assert source =~ "previous\n      )"
      end

      test "unified segment put_batch validates and applies entries in one pass" do
        source = Ferricstore.Test.SourceFiles.waraft_storage_source()

        assert Regex.match?(~r/apply_segment_put_batch_entries\(\s*entries,/, source)
        refute source =~ "Enum.all?(entries, fn {key, value, expire_at_ms} ->"
      end

      test "unified segment put_batch hoists keydir state and threshold out of the per-entry loop" do
        source = Ferricstore.Test.SourceFiles.waraft_storage_source()

        assert source =~ "shard_state = shard_ets_state_from_sm(sm_state)"
        assert source =~ "threshold = ShardETS.hot_cache_threshold(shard_state)"

        assert Regex.match?(
                 ~r/apply_segment_put_batch_entries\(\s*entries,\s*sm_state,\s*shard_state,\s*threshold,\s*file_id,\s*offset,\s*0\s*\)/,
                 source
               )
      end

      test "unified segment put_batch tries a fresh no-ttl ETS batch insert before per-key apply" do
        source = Ferricstore.Test.SourceFiles.waraft_storage_source()
        ets_source = File.read!("lib/ferricstore/store/shard/ets.ex")

        assert source =~ "ShardETS.ets_insert_fresh_no_expiry_many_with_location("
        assert ets_source =~ ":ets.insert(state.keydir, records)"
      end

      test "unified segment batch decoder routes homogeneous put and delete commands directly" do
        source = Ferricstore.Test.SourceFiles.waraft_storage_source()

        assert source =~ "{:put_batch, entries} ->"
        assert source =~ "{:delete_batch, keys} ->"

        refute source =~ "segment_project_batch_fast_path(commands, position, sm_state)",
               "the one-pass decoder already handles homogeneous batches; mixed batches should not rescan the list through the old fast-path"
      end

      test "unified segment generic batch projection decodes and classifies in one pass" do
        source = Ferricstore.Test.SourceFiles.waraft_storage_source()

        assert source =~ "segment_project_decode_batch(commands, :unknown, [], [])"

        refute source =~ "commands = Enum.map(commands, &decoded_replay_command/1)",
               "projection should not decode once and then scan again for homogeneous batches"
      end

      test "WARaft segment batch reader decodes replay command once per segment entry" do
        source =
          File.read!("lib/ferricstore/raft/waraft_segment_reader.ex") <>
            File.read!("lib/ferricstore/raft/waraft_segment_reader/command_values.ex")

        assert source =~ "values_from_command(decode_replay_command(command), keys)"

        refute source =~ "Enum.reduce(Enum.uniq(keys), %{}, fn key, acc ->",
               "batch cold reads should not decode and rescan the same segment command once per requested key"
      end

      test "keydir location insert skips expiry accounting for no-ttl string puts" do
        source = File.read!("lib/ferricstore/store/shard/ets.ex")

        assert source =~ "adjust_expiry_for_insert(state, previous, expire_at_ms)"
        assert source =~ "defp adjust_expiry_for_insert(_state, [], 0), do: :ok"
        assert source =~ "defp adjust_expiry_for_insert(_state, [{_key, _value, 0"
      end

      test "unified segment storage reads cold large values from the Raft segment", %{
        root: root,
        ctx: ctx
      } do
        previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

        try do
          Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_keydir)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          key = "segment-keydir:large"
          value = :binary.copy("v", ctx.hot_cache_max_value_size + 1)

          assert :ok = WARaftBackend.write(0, {:put, key, value, 0})

          assert [
                   {^key, nil, 0, _lfu, {:waraft_segment, index}, offset, value_size}
                 ] = :ets.lookup(elem(ctx.keydir_refs, 0), key)

          assert value_size == byte_size(value)
          assert is_integer(index) and index > 0
          assert is_integer(offset) and offset >= 0
          assert value == Router.get(ctx, key)

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert value == Router.get(restarted_ctx, key)
          assert File.stat!(Path.join([root, "data", "shard_0", "00000.log"])).size == 0
        after
          restore_env(:waraft_storage_apply_mode, previous_mode)
        end
      end

      test "unified segment storage keeps cold values readable after segment trim", %{
        root: root,
        ctx: ctx
      } do
        previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

        try do
          Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_keydir)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          key = "segment-keydir:trim-large"
          value = :binary.copy("v", ctx.hot_cache_max_value_size + 1)

          assert :ok = WARaftBackend.write(0, {:put, key, value, 0})

          assert [
                   {^key, nil, 0, _lfu, {:waraft_segment, value_index}, value_offset, value_size}
                 ] = :ets.lookup(elem(ctx.keydir_refs, 0), key)

          assert value_size == byte_size(value)
          assert is_integer(value_offset) and value_offset >= 0
          assert :ok = WARaftBackend.write(0, {:put, "segment-keydir:trim-fence", "v2", 0})

          log = waraft_segment_log_record(0)

          assert {:ok, _state} =
                   :ferricstore_waraft_spike_segment_log.trim(log, value_index + 1, %{})

          assert :not_found = :ferricstore_waraft_spike_segment_log.get(log, value_index)

          assert [
                   {^key, nil, 0, _lfu_after, {:waraft_projection, projection_index},
                    projection_offset, ^value_size}
                 ] = :ets.lookup(elem(ctx.keydir_refs, 0), key)

          assert is_integer(projection_index) and projection_index > 0
          assert is_integer(projection_offset) and projection_offset > 0
          assert value == Router.get(ctx, key)

          assert {:error, :segment_entry_not_found} =
                   Ferricstore.Raft.WARaftSegmentReader.read_value(ctx, 0, value_index, key)

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)
          on_exit(fn -> FerricStore.Instance.cleanup(restarted_ctx.name) end)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert [
                   {^key, nil, 0, _lfu_restarted,
                    {:waraft_projection, restarted_projection_index}, restarted_projection_offset,
                    ^value_size}
                 ] = :ets.lookup(elem(restarted_ctx.keydir_refs, 0), key)

          assert restarted_projection_index == projection_index
          assert restarted_projection_offset == projection_offset
          assert value == Router.get(restarted_ctx, key)
        after
          restore_env(:waraft_storage_apply_mode, previous_mode)
        end
      end
    end
  end
end
