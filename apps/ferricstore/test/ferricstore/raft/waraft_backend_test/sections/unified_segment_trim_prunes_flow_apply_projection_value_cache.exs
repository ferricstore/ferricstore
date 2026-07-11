defmodule Ferricstore.Raft.WARaftBackendTest.Sections.UnifiedSegmentTrimPrunesFlowApplyProjectionValueCache do
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

      test "unified segment trim prunes Flow apply-projection value cache", %{
        root: root,
        ctx: ctx
      } do
        previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

        try do
          Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_keydir)
          clear_apply_projection_cache!()

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          payload_ref = "f:{segment-keydir-flow-trim-cache}:v:p:segment-keydir-flow-trim-cache:1"
          payload = :binary.copy("p", 1_024)
          encoded_payload = Ferricstore.Flow.encode_value(payload)

          {_log, value_index} =
            append_waraft_fence!("segment-keydir:flow-trim-cache-index", "v")

          {_lfu, value_offset, value_size} =
            insert_apply_projection_ref!(root, ctx, value_index, payload_ref, encoded_payload)

          assert value_offset == 0
          assert value_size == byte_size(encoded_payload)
          assert apply_projection_cache_contains?(root, 0, value_index, payload_ref)
          assert apply_projection_cache_value_bytes(root, 0, value_index) >= value_size

          newer_payload_ref =
            "f:{segment-keydir-flow-trim-cache}:v:p:segment-keydir-flow-trim-cache-newer:1"

          newer_payload = :binary.copy("n", 1_024)
          newer_encoded_payload = Ferricstore.Flow.encode_value(newer_payload)

          {log, newer_value_index} =
            append_waraft_fence!("segment-keydir:flow-trim-cache-newer-index", "v")

          {_newer_lfu, _newer_value_offset, newer_value_size} =
            insert_apply_projection_ref!(
              root,
              ctx,
              newer_value_index,
              newer_payload_ref,
              newer_encoded_payload
            )

          assert newer_value_index > value_index
          assert newer_value_size == byte_size(newer_encoded_payload)
          assert apply_projection_cache_contains?(root, 0, newer_value_index, newer_payload_ref)

          assert {:ok, _state} =
                   :ferricstore_waraft_spike_segment_log.trim(log, value_index + 1, %{})

          refute apply_projection_cache_contains?(root, 0, value_index, payload_ref)
          assert apply_projection_cache_value_bytes(root, 0, value_index) == 0
          refute apply_projection_cache_contains?(root, 0, newer_value_index, newer_payload_ref)
          assert apply_projection_cache_value_bytes(root, 0, newer_value_index) == 0

          assert [
                   {^payload_ref, nil, 0, _lfu_after, {:waraft_projection, projection_index},
                    projection_offset, ^value_size}
                 ] = :ets.lookup(elem(ctx.keydir_refs, 0), payload_ref)

          assert is_integer(projection_index) and projection_index > 0
          assert is_integer(projection_offset) and projection_offset > 0

          assert [
                   {^newer_payload_ref, nil, 0, _newer_lfu_after,
                    {:waraft_projection, newer_projection_index}, newer_projection_offset,
                    ^newer_value_size}
                 ] = :ets.lookup(elem(ctx.keydir_refs, 0), newer_payload_ref)

          assert is_integer(newer_projection_index) and newer_projection_index > 0
          assert is_integer(newer_projection_offset) and newer_projection_offset > 0

          assert {:ok, stored_payload} =
                   Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
                     ctx,
                     0,
                     {:waraft_projection, projection_index},
                     payload_ref
                   )

          assert Ferricstore.Flow.decode_value(stored_payload) == payload

          assert {:ok, stored_newer_payload} =
                   Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
                     ctx,
                     0,
                     {:waraft_projection, newer_projection_index},
                     newer_payload_ref
                   )

          assert Ferricstore.Flow.decode_value(stored_newer_payload) == newer_payload
        after
          restore_env(:waraft_storage_apply_mode, previous_mode)
        end
      end

      test "unified segment trim keeps Flow apply-projection cache while keydir still references it",
           %{
             root: root,
             ctx: ctx
           } do
        previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

        previous_hook =
          Application.get_env(:ferricstore, :waraft_segment_projection_before_relocate_hook)

        try do
          Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_keydir)
          clear_apply_projection_cache!()

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          payload_ref =
            "f:{segment-keydir-flow-trim-cache}:v:p:segment-keydir-flow-trim-cache-still-referenced:1"

          payload = :binary.copy("r", 1_024)
          encoded_payload = Ferricstore.Flow.encode_value(payload)

          {_log, value_index} =
            append_waraft_fence!("segment-keydir:flow-trim-cache-still-referenced-index", "v")

          {lfu, value_offset, value_size} =
            insert_apply_projection_ref!(root, ctx, value_index, payload_ref, encoded_payload)

          assert apply_projection_cache_contains?(root, 0, value_index, payload_ref)
          test_pid = self()

          Application.put_env(:ferricstore, :waraft_segment_projection_before_relocate_hook, fn
            0, _projection_root, relocations ->
              assert Enum.any?(relocations, fn {{relocated_key, _value, _expire_at_ms}, _row} ->
                       relocated_key == payload_ref
                     end)

              :ets.insert(
                elem(ctx.keydir_refs, 0),
                {payload_ref, nil, 9_999_999_999_999, lfu,
                 {:waraft_apply_projection, value_index}, value_offset, value_size}
              )

              send(test_pid, :apply_projection_ref_kept_referenced)
              :ok
          end)

          assert :ok =
                   WARaftBackend.write(
                     0,
                     {:put, "segment-keydir:flow-trim-cache-still-referenced-fence", "v", 0}
                   )

          log = waraft_segment_log_record(0)

          assert {:ok, _state} =
                   :ferricstore_waraft_spike_segment_log.trim(log, value_index + 1, %{})

          assert_receive :apply_projection_ref_kept_referenced

          assert [
                   {^payload_ref, nil, 9_999_999_999_999, ^lfu,
                    {:waraft_apply_projection, ^value_index}, ^value_offset, ^value_size}
                 ] = :ets.lookup(elem(ctx.keydir_refs, 0), payload_ref)

          assert apply_projection_cache_contains?(root, 0, value_index, payload_ref)

          assert {:ok, stored_payload} =
                   Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
                     ctx,
                     0,
                     {:waraft_apply_projection, value_index},
                     payload_ref
                   )

          assert Ferricstore.Flow.decode_value(stored_payload) == payload
        after
          restore_env(:waraft_segment_projection_before_relocate_hook, previous_hook)
          restore_env(:waraft_storage_apply_mode, previous_mode)
        end
      end

      test "unified segment trim relocates LMDB-pinned Flow value locators", %{
        root: root,
        ctx: ctx
      } do
        previous_sync_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)

        clear_apply_projection_cache!()

        try do
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          payload_ref =
            "f:{segment-keydir-flow-trim-pin}:v:p:segment-keydir-flow-trim-pin-relocated:1"

          payload = :binary.copy("p", 1_024)
          encoded_payload = Ferricstore.Flow.encode_value(payload)

          {log, value_index} =
            append_waraft_fence!("segment-keydir:flow-trim-pin-relocated-index", "v")

          {_lfu, value_offset, value_size} =
            insert_apply_projection_ref!(root, ctx, value_index, payload_ref, encoded_payload)

          lmdb_path =
            ctx.data_dir
            |> Ferricstore.DataDir.shard_data_path(0)
            |> Ferricstore.Flow.LMDB.path()

          assert :ok =
                   Ferricstore.Flow.LMDB.write_batch(
                     lmdb_path,
                     Ferricstore.Flow.LMDB.segment_value_pin_put_ops(
                       payload_ref,
                       0,
                       {:waraft_apply_projection, value_index},
                       value_offset,
                       value_size
                     )
                   )

          :ets.delete(elem(ctx.keydir_refs, 0), payload_ref)

          assert apply_projection_cache_contains?(root, 0, value_index, payload_ref)

          assert :ok =
                   WARaftBackend.write(
                     0,
                     {:put, "segment-keydir:flow-trim-pin-relocated-fence", "v", 0}
                   )

          Application.put_env(
            :ferricstore,
            :waraft_segment_log_file_sync_hook,
            {:notify_with_method, self()}
          )

          assert {:ok, _state} =
                   :ferricstore_waraft_spike_segment_log.trim(log, value_index + 1, %{})

          assert_receive_apply_projection_sync()

          assert {:ok, []} =
                   Ferricstore.Flow.LMDB.segment_value_pin_entries_before(
                     lmdb_path,
                     value_index + 1,
                     100
                   )

          assert {:ok, lmdb_value} = Ferricstore.Flow.LMDB.get(lmdb_path, payload_ref)

          assert {:ok, {{:waraft_apply_projection, ^value_index}, ^value_offset, ^value_size}} =
                   Ferricstore.Flow.LMDB.decode_value_locator(lmdb_value, 1_000)

          refute apply_projection_cache_contains?(root, 0, value_index, payload_ref)

          assert {:ok, stored_payload} =
                   Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
                     ctx,
                     0,
                     {:waraft_apply_projection, value_index},
                     payload_ref
                   )

          assert Ferricstore.Flow.decode_value(stored_payload) == payload
          assert [] = :ets.lookup(elem(ctx.keydir_refs, 0), payload_ref)

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert [] = :ets.lookup(elem(restarted_ctx.keydir_refs, 0), payload_ref)
          assert {:ok, [^payload]} = Ferricstore.Flow.value_mget(restarted_ctx, [payload_ref])
        after
          restore_env(:waraft_segment_log_file_sync_hook, previous_sync_hook)
        end
      end

      test "unified segment trim prepares LMDB-pinned Flow values in pages", %{
        root: root,
        ctx: ctx
      } do
        clear_apply_projection_cache!()

        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        lmdb_path =
          ctx.data_dir
          |> Ferricstore.DataDir.shard_data_path(0)
          |> Ferricstore.Flow.LMDB.path()

        {last_index, refs_and_payloads} =
          Enum.reduce(1..3, {0, []}, fn n, {_last_index, acc} ->
            payload_ref = "f:{segment-keydir-flow-trim-pin-page}:v:p:page-#{n}:1"
            payload = "paged-payload-#{n}"
            encoded_payload = Ferricstore.Flow.encode_value(payload)

            {_log, value_index} =
              append_waraft_fence!("segment-keydir:flow-trim-pin-page-index-#{n}", "v")

            {_lfu, value_offset, value_size} =
              insert_apply_projection_ref!(root, ctx, value_index, payload_ref, encoded_payload)

            assert :ok =
                     Ferricstore.Flow.LMDB.write_batch(
                       lmdb_path,
                       Ferricstore.Flow.LMDB.segment_value_pin_put_ops(
                         payload_ref,
                         0,
                         {:waraft_apply_projection, value_index},
                         value_offset,
                         value_size
                       )
                     )

            :ets.delete(elem(ctx.keydir_refs, 0), payload_ref)

            {value_index, [{payload_ref, payload} | acc]}
          end)

        assert {:ok, 3} =
                 WARaftStorage.__prepare_segment_value_pins_for_trim_for_test__(
                   root,
                   ctx,
                   0,
                   last_index + 1,
                   2
                 )

        assert {:ok, []} =
                 Ferricstore.Flow.LMDB.segment_value_pin_entries_before(
                   lmdb_path,
                   last_index + 1,
                   10
                 )

        refs_and_payloads
        |> Enum.reverse()
        |> Enum.each(fn {payload_ref, payload} ->
          assert {:ok, [^payload]} = Ferricstore.Flow.value_mget(ctx, [payload_ref])
        end)
      end

      test "unified segment trim drops expired LMDB-pinned Flow apply-projection locators", %{
        root: root,
        ctx: ctx
      } do
        clear_apply_projection_cache!()

        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        payload_ref = "f:{segment-keydir-flow-trim-expired-pin}:v:p:expired:1"
        payload = Ferricstore.Flow.encode_value(:binary.copy("x", 1_024))
        expired_at_ms = 1

        {log, value_index} =
          append_waraft_fence!("segment-keydir:flow-trim-expired-pin-index", "v")

        lfu = Ferricstore.Store.LFU.initial()
        value_size = byte_size(payload)

        :ets.insert(
          elem(ctx.keydir_refs, 0),
          {payload_ref, nil, expired_at_ms, lfu, {:waraft_apply_projection, value_index}, 0,
           value_size}
        )

        assert :ok =
                 Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(root, 0, value_index, [
                   {payload_ref, payload, expired_at_ms}
                 ])

        lmdb_path =
          ctx.data_dir
          |> Ferricstore.DataDir.shard_data_path(0)
          |> Ferricstore.Flow.LMDB.path()

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(
                   lmdb_path,
                   Ferricstore.Flow.LMDB.segment_value_pin_put_ops(
                     payload_ref,
                     expired_at_ms,
                     {:waraft_apply_projection, value_index},
                     0,
                     value_size
                   )
                 )

        :ets.delete(elem(ctx.keydir_refs, 0), payload_ref)

        assert apply_projection_cache_contains?(root, 0, value_index, payload_ref)

        assert :ok =
                 WARaftBackend.write(
                   0,
                   {:put, "segment-keydir:flow-trim-expired-pin-fence", "v", 0}
                 )

        assert {:ok, _state} =
                 :ferricstore_waraft_spike_segment_log.trim(log, value_index + 1, %{})

        assert {:ok, []} =
                 Ferricstore.Flow.LMDB.segment_value_pin_entries_before(
                   lmdb_path,
                   value_index + 1,
                   100
                 )

        refute apply_projection_cache_contains?(root, 0, value_index, payload_ref)
      end

      test "unified segment trim does not relocate a keydir row changed after projection", %{
        ctx: ctx
      } do
        previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

        previous_hook =
          Application.get_env(:ferricstore, :waraft_segment_projection_before_relocate_hook)

        try do
          Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_keydir)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          key = "segment-keydir:trim-race"
          old_value = :binary.copy("o", ctx.hot_cache_max_value_size + 1)

          assert :ok = WARaftBackend.write(0, {:put, key, old_value, 0})

          assert [
                   {^key, nil, 0, _lfu, {:waraft_segment, value_index}, _value_offset,
                    old_value_size}
                 ] = :ets.lookup(elem(ctx.keydir_refs, 0), key)

          test_pid = self()

          Application.put_env(:ferricstore, :waraft_segment_projection_before_relocate_hook, fn
            0, _projection_root, relocations ->
              assert Enum.any?(relocations, fn {{relocated_key, _value, _expire_at_ms}, _row} ->
                       relocated_key == key
                     end)

              :ets.insert(elem(ctx.keydir_refs, 0), {key, "new-hot", 0, 0, :pending, nil, 0})
              send(test_pid, :projection_relocation_hook_ran)
              :ok
          end)

          assert :ok = WARaftBackend.write(0, {:put, "segment-keydir:trim-race-fence", "v2", 0})

          log = waraft_segment_log_record(0)

          assert {:ok, _state} =
                   :ferricstore_waraft_spike_segment_log.trim(log, value_index + 1, %{})

          assert_receive :projection_relocation_hook_ran

          assert [{^key, "new-hot", 0, _lfu_after, :pending, nil, 0}] =
                   :ets.lookup(elem(ctx.keydir_refs, 0), key)

          assert old_value_size == byte_size(old_value)
        after
          restore_env(:waraft_segment_projection_before_relocate_hook, previous_hook)
          restore_env(:waraft_storage_apply_mode, previous_mode)
        end
      end

      test "unified segment storage reads cold values from a batched Raft segment record", %{
        root: root,
        ctx: ctx
      } do
        previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

        try do
          Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_keydir)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          bitcask_file = Path.join([root, "data", "shard_0", "00000.log"])
          bitcask_size = File.stat!(bitcask_file).size
          key1 = "segment-keydir:batch-large:1"
          key2 = "segment-keydir:batch-large:2"
          value1 = :binary.copy("a", ctx.hot_cache_max_value_size + 1)
          value2 = :binary.copy("b", ctx.hot_cache_max_value_size + 2)

          assert {:ok, [:ok, :ok]} =
                   WARaftBackend.write_put_batch(0, [{key1, value1, 0}, {key2, value2, 0}])

          assert [
                   {^key1, nil, 0, _lfu1, {:waraft_segment, index}, offset, value1_size}
                 ] = :ets.lookup(elem(ctx.keydir_refs, 0), key1)

          assert [
                   {^key2, nil, 0, _lfu2, {:waraft_segment, ^index}, ^offset, value2_size}
                 ] = :ets.lookup(elem(ctx.keydir_refs, 0), key2)

          assert is_integer(offset) and offset >= 0
          assert value1_size == byte_size(value1)
          assert value2_size == byte_size(value2)
          assert value1 == Router.get(ctx, key1)
          assert value2 == Router.get(ctx, key2)
          assert File.stat!(bitcask_file).size == bitcask_size

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert value1 == Router.get(restarted_ctx, key1)
          assert value2 == Router.get(restarted_ctx, key2)
        after
          restore_env(:waraft_storage_apply_mode, previous_mode)
        end
      end

      test "unified segment storage replays batched deletes without Bitcask data writes", %{
        root: root,
        ctx: ctx
      } do
        previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

        try do
          Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_keydir)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          bitcask_file = Path.join([root, "data", "shard_0", "00000.log"])
          bitcask_size = File.stat!(bitcask_file).size
          key1 = "segment-keydir:delete-batch:1"
          key2 = "segment-keydir:delete-batch:2"

          assert {:ok, [:ok, :ok]} =
                   WARaftBackend.write_put_batch(0, [{key1, "v1", 0}, {key2, "v2", 0}])

          assert {:ok, [:ok, :ok]} = WARaftBackend.write_delete_batch(0, [key1, key2])
          assert nil == Router.get(ctx, key1)
          assert nil == Router.get(ctx, key2)
          assert [] == :ets.lookup(elem(ctx.keydir_refs, 0), key1)
          assert [] == :ets.lookup(elem(ctx.keydir_refs, 0), key2)
          assert File.stat!(bitcask_file).size == bitcask_size

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert nil == Router.get(restarted_ctx, key1)
          assert nil == Router.get(restarted_ctx, key2)
        after
          restore_env(:waraft_storage_apply_mode, previous_mode)
        end
      end

      test "committed SET and DEL use the real FerricStore state machine", %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        assert :ok = WARaftBackend.write(0, {:put, "wk1", "wv1", 0})
        assert "wv1" == Router.get(ctx, "wk1")
        assert [{_, "wv1", 0, _lfu, _fid, _off, 3}] = :ets.lookup(elem(ctx.keydir_refs, 0), "wk1")

        assert :ok = WARaftBackend.write(0, {:delete, "wk1"})
        assert nil == Router.get(ctx, "wk1")
        assert [] == :ets.lookup(elem(ctx.keydir_refs, 0), "wk1")
      end

      test "single-member writes avoid per-command WARaft status calls", %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        test_pid = self()

        Process.put(:ferricstore_waraft_backend_status_hook, fn shard_index ->
          send(test_pid, {:waraft_status_call, shard_index})
        end)

        try do
          _ = WARaftBackend.status(0)
          assert_receive {:waraft_status_call, 0}, 100

          assert :ok = WARaftBackend.write(0, {:put, "status-cache:key", "v", 0})

          refute_receive {:waraft_status_call, 0}, 50
        after
          Process.delete(:ferricstore_waraft_backend_status_hook)
        end
      end

      test "default namespace window keeps WARaft writes on the direct path", %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        parent = self()
        handler_id = {__MODULE__, :default_namespace_direct_path, make_ref()}

        :telemetry.attach(
          handler_id,
          [:ferricstore, :waraft, :batcher, :slot_flush],
          &__MODULE__.handle_namespace_batcher_telemetry/4,
          parent
        )

        try do
          assert :ok = WARaftBackend.write(0, {:put, "defaultwin:key", "v", 0})
          assert "v" == Router.get(ctx, "defaultwin:key")
          refute_receive {:waraft_namespace_batcher_flush, _event, _measurements, _metadata}, 50
        after
          :telemetry.detach(handler_id)
        end
      end

      test "start options configure WARaft throughput batch knobs", %{ctx: ctx} do
        previous_entries =
          Application.get_env(:ferricstore_waraft_backend, :raft_max_log_entries_per_heartbeat)

        previous_heartbeat =
          Application.get_env(:ferricstore_waraft_backend, :raft_max_heartbeat_size)

        previous_apply =
          Application.get_env(:ferricstore_waraft_backend, :raft_apply_log_batch_size)

        previous_apply_bytes =
          Application.get_env(:ferricstore_waraft_backend, :raft_apply_batch_max_bytes)

        previous_rotation_interval =
          Application.get_env(:ferricstore_waraft_backend, :raft_max_log_records_per_file)

        previous_rotation_keep =
          Application.get_env(:ferricstore_waraft_backend, :raft_max_log_records)

        previous_max_retained =
          Application.get_env(:ferricstore_waraft_backend, :raft_max_retained_entries)

        try do
          assert :ok =
                   WARaftBackend.start(ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     max_log_entries_per_heartbeat: 2048,
                     max_heartbeat_size: 32 * 1024 * 1024,
                     apply_log_batch_size: 2048,
                     apply_batch_max_bytes: 32 * 1024 * 1024,
                     log_rotation_interval: 64,
                     log_rotation_keep: 128,
                     max_retained_entries: 128
                   )

          assert 2048 ==
                   Application.get_env(
                     :ferricstore_waraft_backend,
                     :raft_max_log_entries_per_heartbeat
                   )

          assert 32 * 1024 * 1024 ==
                   Application.get_env(:ferricstore_waraft_backend, :raft_max_heartbeat_size)

          assert 2048 ==
                   Application.get_env(:ferricstore_waraft_backend, :raft_apply_log_batch_size)

          assert 32 * 1024 * 1024 ==
                   Application.get_env(:ferricstore_waraft_backend, :raft_apply_batch_max_bytes)

          assert 64 ==
                   Application.get_env(
                     :ferricstore_waraft_backend,
                     :raft_max_log_records_per_file
                   )

          assert 128 == Application.get_env(:ferricstore_waraft_backend, :raft_max_log_records)

          assert 128 ==
                   Application.get_env(:ferricstore_waraft_backend, :raft_max_retained_entries)
        after
          restore_waraft_app_env(:raft_max_log_entries_per_heartbeat, previous_entries)
          restore_waraft_app_env(:raft_max_heartbeat_size, previous_heartbeat)
          restore_waraft_app_env(:raft_apply_log_batch_size, previous_apply)
          restore_waraft_app_env(:raft_apply_batch_max_bytes, previous_apply_bytes)
          restore_waraft_app_env(:raft_max_log_records_per_file, previous_rotation_interval)
          restore_waraft_app_env(:raft_max_log_records, previous_rotation_keep)
          restore_waraft_app_env(:raft_max_retained_entries, previous_max_retained)
        end
      end

      test "FerricStore app env configures WARaft throughput batch knobs", %{ctx: ctx} do
        previous_public = Application.get_env(:ferricstore, :waraft_max_log_entries_per_heartbeat)
        previous_rotation_public = Application.get_env(:ferricstore, :waraft_log_rotation_keep)

        previous_waraft =
          Application.get_env(:ferricstore_waraft_backend, :raft_max_log_entries_per_heartbeat)

        previous_rotation_waraft =
          Application.get_env(:ferricstore_waraft_backend, :raft_max_log_records)

        try do
          Application.put_env(:ferricstore, :waraft_max_log_entries_per_heartbeat, 4096)
          Application.put_env(:ferricstore, :waraft_log_rotation_keep, 8192)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert 4096 ==
                   Application.get_env(
                     :ferricstore_waraft_backend,
                     :raft_max_log_entries_per_heartbeat
                   )

          assert 8192 == Application.get_env(:ferricstore_waraft_backend, :raft_max_log_records)
        after
          restore_env(:waraft_max_log_entries_per_heartbeat, previous_public)
          restore_env(:waraft_log_rotation_keep, previous_rotation_public)
          restore_waraft_app_env(:raft_max_log_entries_per_heartbeat, previous_waraft)
          restore_waraft_app_env(:raft_max_log_records, previous_rotation_waraft)
        end
      end

      test "invalid WARaft throughput batch knobs fail closed before partition start", %{ctx: ctx} do
        previous_heartbeat = Application.get_env(:ferricstore, :waraft_max_heartbeat_size)

        try do
          assert_raise ArgumentError, ~r/max_log_entries_per_heartbeat/, fn ->
            WARaftBackend.start(ctx,
              log_module: :ferricstore_waraft_spike_segment_log,
              max_log_entries_per_heartbeat: 0
            )
          end

          refute Process.whereis(:raft_sup_ferricstore_waraft_backend_1)

          Application.put_env(:ferricstore, :waraft_max_heartbeat_size, "bad")

          assert_raise ArgumentError, ~r/waraft_max_heartbeat_size/, fn ->
            WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          end

          refute Process.whereis(:raft_sup_ferricstore_waraft_backend_1)
        after
          restore_env(:waraft_max_heartbeat_size, previous_heartbeat)
        end
      end

      test "invalid WARaft throughput config does not partially publish backend app env", %{
        ctx: ctx
      } do
        previous_public = Application.get_env(:ferricstore, :waraft_max_heartbeat_size)
        previous_database = Application.get_env(:ferricstore_waraft_backend, :raft_database)

        previous_entries =
          Application.get_env(:ferricstore_waraft_backend, :raft_max_log_entries_per_heartbeat)

        try do
          Application.put_env(:ferricstore, :waraft_max_heartbeat_size, 0)
          Application.put_env(:ferricstore_waraft_backend, :raft_database, ~c"sentinel-waraft-db")

          Application.put_env(
            :ferricstore_waraft_backend,
            :raft_max_log_entries_per_heartbeat,
            777
          )

          assert_raise ArgumentError, ~r/waraft_max_heartbeat_size/, fn ->
            WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          end

          assert ~c"sentinel-waraft-db" ==
                   Application.get_env(:ferricstore_waraft_backend, :raft_database)

          assert 777 ==
                   Application.get_env(
                     :ferricstore_waraft_backend,
                     :raft_max_log_entries_per_heartbeat
                   )
        after
          restore_env(:waraft_max_heartbeat_size, previous_public)
          restore_waraft_app_env(:raft_database, previous_database)
          restore_waraft_app_env(:raft_max_log_entries_per_heartbeat, previous_entries)
        end
      end

      test "small WARaft log retention trims in-memory segment log without losing reads", %{
        ctx: ctx
      } do
        assert :ok =
                 WARaftBackend.start(ctx,
                   log_module: :ferricstore_waraft_spike_segment_log,
                   log_rotation_interval: 4,
                   log_rotation_keep: 4,
                   max_retained_entries: 4,
                   apply_log_batch_size: 8
                 )

        for n <- 1..32 do
          assert :ok = WARaftBackend.write(0, {:put, "trim:key:#{n}", "value:#{n}", 0})
        end

        log_table = waraft_log_table(0)

        assert_eventually(
          fn ->
            case :ets.info(log_table, :size) do
              size when is_integer(size) and size <= 8 -> :trimmed
              _ -> :not_trimmed
            end
          end,
          :trimmed,
          100
        )

        assert "value:1" == Router.get(ctx, "trim:key:1")
        assert "value:32" == Router.get(ctx, "trim:key:32")
      end

      test "invalid WARaft election timeout bounds fail closed before partition start", %{
        ctx: ctx
      } do
        previous_min = Application.get_env(:ferricstore, :waraft_election_timeout_ms)
        previous_max = Application.get_env(:ferricstore, :waraft_election_timeout_ms_max)

        try do
          Application.put_env(:ferricstore, :waraft_election_timeout_ms, 500)
          Application.put_env(:ferricstore, :waraft_election_timeout_ms_max, 250)

          assert_raise ArgumentError, ~r/waraft_election_timeout_ms_max.*>=/, fn ->
            WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          end

          refute Process.whereis(:raft_sup_ferricstore_waraft_backend_1)
        after
          restore_env(:waraft_election_timeout_ms, previous_min)
          restore_env(:waraft_election_timeout_ms_max, previous_max)
        end
      end

      test "invalid WARaft queue and commit knobs fail closed before partition start", %{ctx: ctx} do
        previous_pending = Application.get_env(:ferricstore, :waraft_max_pending_reads)
        existing_sup = Process.whereis(:raft_sup_ferricstore_waraft_backend_1)

        try do
          assert_raise ArgumentError, ~r/max_pending_high_priority_commits/, fn ->
            WARaftBackend.start(ctx,
              log_module: :ferricstore_waraft_spike_segment_log,
              max_pending_high_priority_commits: -1
            )
          end

          assert Process.whereis(:raft_sup_ferricstore_waraft_backend_1) == existing_sup

          assert_raise ArgumentError, ~r/commit_batch_max/, fn ->
            WARaftBackend.start(ctx,
              log_module: :ferricstore_waraft_spike_segment_log,
              commit_batch_max: 0
            )
          end

          assert Process.whereis(:raft_sup_ferricstore_waraft_backend_1) == existing_sup

          Application.put_env(:ferricstore, :waraft_max_pending_reads, "bad")

          assert_raise ArgumentError, ~r/waraft_max_pending_reads/, fn ->
            WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          end

          assert Process.whereis(:raft_sup_ferricstore_waraft_backend_1) == existing_sup

          Application.delete_env(:ferricstore, :waraft_max_pending_reads)
        after
          restore_env(:waraft_max_pending_reads, previous_pending)
        end
      end
    end
  end
end
