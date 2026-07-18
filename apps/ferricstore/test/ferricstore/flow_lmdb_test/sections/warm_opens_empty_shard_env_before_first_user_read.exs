defmodule Ferricstore.Flow.LMDBTest.Sections.WarmOpensEmptyShardEnvBeforeFirstUserRead do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Flow.LMDBTest.FlushProbeWriter

      test "warm opens an empty shard env before first user read" do
        path =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_#{System.unique_integer([:positive])}"
          )

        on_exit(fn -> File.rm_rf!(path) end)

        refute File.exists?(path)
        assert :ok = Ferricstore.Flow.LMDB.warm(path)
        assert File.dir?(path)
        assert :not_found = Ferricstore.Flow.LMDB.get(path, "missing")
      end

      test "release_all drops cached envs without losing persisted data" do
        path =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_release_#{System.unique_integer([:positive])}"
          )

        on_exit(fn ->
          Ferricstore.Flow.LMDB.release_all(30_000)
          File.rm_rf!(path)
        end)

        assert :ok = Ferricstore.Flow.LMDB.write_batch(path, [{:put, "key", "value"}])
        assert {:ok, "value"} = Ferricstore.Flow.LMDB.get(path, "key")

        assert {:ok, released} = Ferricstore.Flow.LMDB.release_all()
        assert released >= 1

        assert {:ok, "value"} = Ferricstore.Flow.LMDB.get(path, "key")
      end

      test "clear reopens a cached env after its directory was replaced" do
        path =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_replaced_#{System.unique_integer([:positive])}"
          )

        on_exit(fn ->
          Ferricstore.Flow.LMDB.release_all(30_000)
          File.rm_rf!(path)
        end)

        assert :ok = Ferricstore.Flow.LMDB.write_batch(path, [{:put, "old", "value"}])
        assert Ferricstore.Flow.LMDB.env_present?(path)

        File.rm_rf!(path)
        File.mkdir_p!(path)
        refute Ferricstore.Flow.LMDB.env_present?(path)

        assert :ok = Ferricstore.Flow.LMDB.clear(path)
        assert Ferricstore.Flow.LMDB.env_present?(path)
        assert :ok = Ferricstore.Flow.LMDB.write_batch(path, [{:put, "new", "value"}])
        assert {:ok, "value"} = Ferricstore.Flow.LMDB.get(path, "new")
      end

      test "mode parser normalizes every legacy value to lagged" do
        assert Ferricstore.Flow.LMDB.normalize_mode("off") == :lagged
        assert Ferricstore.Flow.LMDB.normalize_mode("false") == :lagged
        assert Ferricstore.Flow.LMDB.normalize_mode("mirror") == :lagged
        assert Ferricstore.Flow.LMDB.normalize_mode("lagged") == :lagged
        assert Ferricstore.Flow.LMDB.normalize_mode("true") == :lagged
      end

      test "read-only operations do not open a missing LMDB env" do
        path =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_read_only_missing_#{System.unique_integer([:positive])}"
          )

        on_exit(fn -> File.rm_rf!(path) end)

        refute File.exists?(path)
        assert :not_found = Ferricstore.Flow.LMDB.get(path, "missing")
        assert {:ok, [:not_found, :not_found]} = Ferricstore.Flow.LMDB.get_many(path, ["a", "b"])
        assert {:ok, []} = Ferricstore.Flow.LMDB.prefix_entries(path, "prefix", 10)
        assert {:ok, []} = Ferricstore.Flow.LMDB.prefix_entries(path, "prefix", 10, true)
        assert {:ok, 0} = Ferricstore.Flow.LMDB.prefix_count(path, "prefix")
        refute File.exists?(path)
      end

      test "flush marker check does not open empty pre-created shard dirs" do
        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_empty_dirs_#{System.unique_integer([:positive])}"
          )

        on_exit(fn -> File.rm_rf!(data_dir) end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 2)
        assert :ok = Ferricstore.Flow.LMDB.ensure_shard_dirs(data_dir, 2)

        for shard_index <- 0..1 do
          path =
            data_dir
            |> Ferricstore.DataDir.shard_data_path(shard_index)
            |> Ferricstore.Flow.LMDB.path()

          assert File.dir?(path)
          refute Ferricstore.Flow.LMDB.env_present?(path)
          refute Ferricstore.Flow.LMDB.flush_in_progress?(path)
          refute Ferricstore.Flow.LMDB.env_present?(path)
        end
      end

      test "LMDB replay-safe marker never moves backward" do
        path =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_replay_safe_marker_#{System.unique_integer([:positive])}"
          )

        on_exit(fn -> File.rm_rf!(path) end)

        assert :ok = Ferricstore.Flow.LMDBReplaySafeIndex.persist(path, 42)
        assert :ok = Ferricstore.Flow.LMDBReplaySafeIndex.persist(path, 7)
        assert Ferricstore.Flow.LMDBReplaySafeIndex.read(path) == 42
      end

      test "prefix entries after resumes from the last returned LMDB key" do
        path =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_prefix_after_#{System.unique_integer([:positive])}"
          )

        on_exit(fn -> File.rm_rf!(path) end)

        prefix = "flow-active-index:test:"

        keys = [
          prefix <> "001",
          prefix <> "002",
          prefix <> "003"
        ]

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(
                   path,
                   Enum.map(keys, fn key -> {:put, key, "value:" <> key} end) ++
                     [{:put, "flow-active-index:other:001", "skip"}]
                 )

        assert {:ok, [{first_key, _first_value}, {second_key, _second_value}]} =
                 Ferricstore.Flow.LMDB.prefix_entries_after(path, prefix, <<>>, 2)

        assert [first_key, second_key] == Enum.take(keys, 2)

        assert {:ok, [{third_key, _third_value}]} =
                 Ferricstore.Flow.LMDB.prefix_entries_after(path, prefix, second_key, 10)

        assert third_key == List.last(keys)

        assert {:ok, []} =
                 Ferricstore.Flow.LMDB.prefix_entries_after(path, prefix, third_key, 10)
      end

      test "bounded prefix pages honor item and cumulative byte limits without stalling" do
        path =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_prefix_bounded_#{System.unique_integer([:positive])}"
          )

        on_exit(fn -> File.rm_rf!(path) end)

        prefix = "flow-bounded:test:"

        rows = [
          {prefix <> "001", "one"},
          {prefix <> "002", "two-two"},
          {prefix <> "003", String.duplicate("x", 128)}
        ]

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(
                   path,
                   Enum.map(rows, fn {key, value} -> {:put, key, value} end) ++
                     [{:put, "flow-bounded:other:001", "skip"}]
                 )

        [{first_key, first_value}, {second_key, second_value}, {third_key, third_value}] = rows
        first_bytes = byte_size(first_key) + byte_size(first_value)
        second_bytes = byte_size(second_key) + byte_size(second_value)

        assert {:ok, [^first_key, ^second_key]} =
                 Ferricstore.Flow.LMDB.prefix_entries_after_bounded(
                   path,
                   prefix,
                   <<>>,
                   2,
                   first_bytes + second_bytes
                 )
                 |> then(fn {:ok, entries} -> {:ok, Enum.map(entries, &elem(&1, 0))} end)

        assert {:ok, [{^first_key, ^first_value}]} =
                 Ferricstore.Flow.LMDB.prefix_entries_after_bounded(
                   path,
                   prefix,
                   <<>>,
                   10,
                   first_bytes
                 )

        assert {:ok, [{^second_key, ^second_value}]} =
                 Ferricstore.Flow.LMDB.prefix_entries_after_bounded(
                   path,
                   prefix,
                   first_key,
                   10,
                   second_bytes
                 )

        assert {:ok, [{^third_key, ^third_value}]} =
                 Ferricstore.Flow.LMDB.prefix_entries_after_bounded(
                   path,
                   prefix,
                   second_key,
                   10,
                   10_000
                 )

        assert {:ok, []} =
                 Ferricstore.Flow.LMDB.prefix_entries_after_bounded(
                   path,
                   prefix,
                   third_key,
                   10,
                   10_000
                 )

        assert {:ok, [{^first_key, ^first_value}]} =
                 Ferricstore.Flow.LMDB.prefix_entries_after_bounded(
                   path,
                   prefix,
                   <<>>,
                   10,
                   1
                 )

        assert {:ok, []} =
                 Ferricstore.Flow.LMDB.prefix_entries_after_bounded(
                   path,
                   prefix,
                   <<>>,
                   0,
                   1_000
                 )
      end

      test "segment value pin scan accepts exact limit only when a future pin bounds it" do
        path =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_segment_pin_scan_#{System.unique_integer([:positive])}"
          )

        on_exit(fn -> File.rm_rf!(path) end)

        ops =
          Ferricstore.Flow.LMDB.segment_value_pin_batch_put_ops([
            {"flow-pin-old-1", 0, {:waraft_apply_projection, 1}, 0, 10},
            {"flow-pin-old-2", 0, {:waraft_apply_projection, 2}, 0, 10},
            {"flow-pin-future", 0, {:waraft_apply_projection, 3}, 0, 10}
          ])

        assert :ok = Ferricstore.Flow.LMDB.write_batch(path, ops)

        assert {:ok, pins} = Ferricstore.Flow.LMDB.segment_value_pin_entries_before(path, 3, 2)

        assert Enum.map(pins, fn %{key: key, file_id: file_id} -> {key, file_id} end) ==
                 [
                   {"flow-pin-old-1", {:waraft_apply_projection, 1}},
                   {"flow-pin-old-2", {:waraft_apply_projection, 2}}
                 ]

        assert {:error, {:flow_segment_value_pin_scan_limit, 2}} =
                 Ferricstore.Flow.LMDB.segment_value_pin_entries_before(path, 4, 2)
      end

      test "segment value pins batch multiple values from the same apply index" do
        path =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_segment_pin_batch_#{System.unique_integer([:positive])}"
          )

        on_exit(fn -> File.rm_rf!(path) end)

        ops =
          Ferricstore.Flow.LMDB.segment_value_pin_batch_put_ops([
            {"flow-pin-same-1", 0, {:waraft_apply_projection, 7}, 10, 11},
            {"flow-pin-same-2", 0, {:waraft_apply_projection, 7}, 20, 22},
            {"flow-pin-other", 0, {:waraft_apply_projection, 8}, 30, 33}
          ])

        pin_prefix = Ferricstore.Flow.LMDB.segment_value_pin_prefix(:waraft_apply_projection)
        assert :ok = Ferricstore.Flow.LMDB.write_batch(path, ops)
        assert {:ok, 2} = Ferricstore.Flow.LMDB.prefix_count(path, pin_prefix)

        assert {:ok, pins} = Ferricstore.Flow.LMDB.segment_value_pin_entries_before(path, 8, 10)

        assert Enum.map(pins, fn pin ->
                 {pin.key, pin.file_id, pin.offset, pin.value_size, is_binary(pin.pin_key)}
               end) == [
                 {"flow-pin-same-1", {:waraft_apply_projection, 7}, 10, 11, true},
                 {"flow-pin-same-2", {:waraft_apply_projection, 7}, 20, 22, true}
               ]
      end

      test "value_mget materializes direct LMDB blob-ref values" do
        ctx =
          Ferricstore.Test.IsolatedInstance.checkout(
            shard_count: 1,
            blob_side_channel_threshold_bytes: 128
          )

        on_exit(fn ->
          Ferricstore.Test.IsolatedInstance.checkin(ctx)
        end)

        ref =
          Ferricstore.Flow.Keys.value_key(
            "flow-lmdb-direct-blob",
            :payload,
            1,
            "lmdb-direct-blob"
          )

        payload = String.duplicate("payload-", 64)
        encoded_payload = Ferricstore.Flow.encode_value(payload)

        assert {:ok, blob_ref} = Ferricstore.Store.BlobStore.put(ctx.data_dir, 0, encoded_payload)
        encoded_blob_ref = Ferricstore.Store.BlobRef.encode!(blob_ref)

        lmdb_path =
          ctx.data_dir
          |> Ferricstore.DataDir.shard_data_path(0)
          |> Ferricstore.Flow.LMDB.path()

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(lmdb_path, [
                   {:put, ref, Ferricstore.Flow.LMDB.encode_value(encoded_blob_ref, 0)}
                 ])

        assert {:ok, [^payload]} = Ferricstore.Flow.value_mget(ctx, [ref])
      end

      test "blob garbage sweep preserves Flow value refs projected only in LMDB" do
        ctx =
          Ferricstore.Test.IsolatedInstance.checkout(
            shard_count: 1,
            blob_side_channel_threshold_bytes: 128
          )

        Process.put(:ferricstore_blob_store_segment_gc_grace_ms, 0)

        on_exit(fn ->
          Process.delete(:ferricstore_blob_store_segment_gc_grace_ms)
          Ferricstore.Test.IsolatedInstance.checkin(ctx)
        end)

        ref =
          Ferricstore.Flow.Keys.value_key(
            "flow-lmdb-blob-gc",
            :payload,
            1,
            "lmdb-blob-gc"
          )

        payload = String.duplicate("payload-", 64)
        encoded_payload = Ferricstore.Flow.encode_value(payload)

        assert {:ok, blob_ref} = Ferricstore.Store.BlobStore.put(ctx.data_dir, 0, encoded_payload)
        encoded_blob_ref = Ferricstore.Store.BlobRef.encode!(blob_ref)

        assert {:ok, {path, _offset, _size}} =
                 Ferricstore.Store.BlobStore.file_ref(ctx.data_dir, 0, blob_ref)

        lmdb_path =
          ctx.data_dir
          |> Ferricstore.DataDir.shard_data_path(0)
          |> Ferricstore.Flow.LMDB.path()

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(lmdb_path, [
                   {:put, ref, Ferricstore.Flow.LMDB.encode_value(encoded_blob_ref, 0)}
                 ])

        assert {:ok, [^payload]} = Ferricstore.Flow.value_mget(ctx, [ref])
        assert [] = :ets.lookup(elem(ctx.keydir_refs, 0), ref)

        assert {:ok, %{deleted_files: 0}} = Ferricstore.Store.Router.sweep_blob_garbage(ctx)
        assert File.exists?(path)
        assert {:ok, [^payload]} = Ferricstore.Flow.value_mget(ctx, [ref])
      end

      test "value_mget batches LMDB locators that point to the same WARaft apply projection" do
        ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1)

        on_exit(fn ->
          Process.delete(:ferricstore_waraft_apply_projection_disk_read_hook)
          Ferricstore.Test.IsolatedInstance.checkin(ctx)
        end)

        ref1 =
          Ferricstore.Flow.Keys.value_key(
            "flow-lmdb-locator-batch-1",
            :payload,
            1,
            "lmdb-locator-batch"
          )

        ref2 =
          Ferricstore.Flow.Keys.value_key(
            "flow-lmdb-locator-batch-2",
            :payload,
            1,
            "lmdb-locator-batch"
          )

        encoded1 = Ferricstore.Flow.encode_value("value-one")
        encoded2 = Ferricstore.Flow.encode_value("value-two")
        index = 12_345
        file_id = {:waraft_apply_projection, index}

        :ok =
          Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(ctx.data_dir, 0, index, [
            {ref1, encoded1, 0},
            {ref2, encoded2, 0}
          ])

        assert {:ok, 2} =
                 Ferricstore.Raft.WARaftSegmentReader.spill_apply_projection_cache(
                   ctx.data_dir,
                   0
                 )

        assert 0 =
                 Ferricstore.Raft.WARaftSegmentReader.clear_apply_projection_cache(
                   ctx.data_dir,
                   0
                 )

        lmdb_path =
          ctx.data_dir
          |> Ferricstore.DataDir.shard_data_path(0)
          |> Ferricstore.Flow.LMDB.path()

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(lmdb_path, [
                   {:put, ref1,
                    Ferricstore.Flow.LMDB.encode_value_locator(0, file_id, 0, byte_size(encoded1))},
                   {:put, ref2,
                    Ferricstore.Flow.LMDB.encode_value_locator(0, file_id, 0, byte_size(encoded2))}
                 ])

        parent = self()

        Process.put(:ferricstore_waraft_apply_projection_disk_read_hook, fn _root,
                                                                            ^index,
                                                                            source ->
          send(parent, {:apply_projection_disk_read, source})
        end)

        assert {:ok, ["value-one", "value-two"]} = Ferricstore.Flow.value_mget(ctx, [ref1, ref2])
        assert [:latest] = collect_apply_projection_disk_reads()
      end

      test "writer does not open LMDB until projection data is flushed" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_lazy_writer_#{System.unique_integer([:positive])}"
          )

        shard_index = 0
        instance_name = :"flow_lmdb_lazy_writer_#{System.unique_integer([:positive])}"
        key = "flow:{flow:test}:state:lazy"

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)

        start_supervised!(
          {Ferricstore.Flow.LMDBWriter,
           shard_index: shard_index, data_dir: data_dir, instance_name: instance_name}
        )

        path =
          data_dir
          |> Ferricstore.DataDir.shard_data_path(shard_index)
          |> Ferricstore.Flow.LMDB.path()

        refute File.exists?(path)

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
                   {:put, key, "v1"}
                 ])

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)
        assert File.dir?(path)
        assert {:ok, "v1"} = Ferricstore.Flow.LMDB.get(path, key)
      end

      test "writer flush waits for async enqueue work reserved by another process" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)

        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
        Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_async_flush_barrier_#{System.unique_integer([:positive])}"
          )

        shard_index = 0
        instance_name = :"flow_lmdb_async_flush_barrier_#{System.unique_integer([:positive])}"
        key = "flow:{flow:test}:state:async-flush-barrier"

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)

        writer =
          start_supervised!(
            {Ferricstore.Flow.LMDBWriter,
             shard_index: shard_index, data_dir: data_dir, instance_name: instance_name}
          )

        path =
          data_dir
          |> Ferricstore.DataDir.shard_data_path(shard_index)
          |> Ferricstore.Flow.LMDB.path()

        seq_ref =
          :persistent_term.get(
            {Ferricstore.Flow.LMDBWriter, :enqueue_seq, instance_name, shard_index}
          )

        seq = :atomics.add_get(seq_ref, 1, 1)

        flush =
          Task.async(fn -> Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index) end)

        refute Task.yield(flush, 50),
               "flush replied before the async enqueue reservation reached the writer"

        GenServer.cast(writer, {:enqueue, seq, [{:put, key, "v1"}], [], {seq_ref, 0}})

        assert Task.await(flush, 1_000) == :ok
        assert {:ok, "v1"} = Ferricstore.Flow.LMDB.get(path, key)
      end

      test "writer discard rejects projection work reserved before the reset" do
        old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
        Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_async_discard_barrier_#{System.unique_integer([:positive])}"
          )

        shard_index = 0
        instance_name = :"flow_lmdb_async_discard_barrier_#{System.unique_integer([:positive])}"
        key = "flow:{flow:test}:state:async-discard-barrier"
        sync_key = "flow:{flow:test}:state:sync-discard-barrier"
        outbox_key = "flow:{flow:test}:state:outbox-discard-barrier"
        degraded = :atomics.new(1, signed: false)

        on_exit(fn ->
          restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)

        writer =
          start_supervised!(
            {Ferricstore.Flow.LMDBWriter,
             shard_index: shard_index,
             data_dir: data_dir,
             instance_ctx: %{
               name: instance_name,
               flow_lmdb_mirror_degraded: degraded
             }}
          )

        path =
          data_dir
          |> Ferricstore.DataDir.shard_data_path(shard_index)
          |> Ferricstore.Flow.LMDB.path()

        seq_ref =
          :persistent_term.get(
            {Ferricstore.Flow.LMDBWriter, :enqueue_seq, instance_name, shard_index}
          )

        seq = :atomics.add_get(seq_ref, 1, 1)

        assert :ok = Ferricstore.Flow.LMDBWriter.discard(instance_name, shard_index)

        assert {:error, :writer_restarted} =
                 GenServer.call(
                   writer,
                   {:enqueue, [{:put, sync_key, "stale"}], [], {seq_ref, 0}}
                 )

        GenServer.cast(writer, {:enqueue, seq, [{:put, key, "stale"}], [], {seq_ref, 0}})

        outbox = Ferricstore.Flow.LMDBWriter.projection_outbox_name(instance_name, shard_index)

        true =
          :ets.insert(
            outbox,
            {System.unique_integer([:monotonic, :positive]), seq_ref, outbox_key, 1}
          )

        GenServer.cast(writer, {:projection_outbox_available, seq_ref})

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)
        assert :not_found = Ferricstore.Flow.LMDB.get(path, key)
        assert :not_found = Ferricstore.Flow.LMDB.get(path, sync_key)
        assert :ets.info(outbox, :size) == 0
        assert :atomics.get(degraded, 1) == 0

        true = :ets.insert(outbox, {:dirty, seq_ref, 1})
        GenServer.cast(writer, {:projection_dirty, seq_ref})

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)
        assert :ets.info(outbox, :size) == 0
        assert :atomics.get(degraded, 1) == 0
      end

      test "writer async enqueue preserves after-flush callbacks without LMDB ops" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)

        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
        Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_after_flush_only_#{System.unique_integer([:positive])}"
          )

        shard_index = 0
        instance_name = :"flow_lmdb_after_flush_only_#{System.unique_integer([:positive])}"
        ets = :ets.new(:flow_lmdb_after_flush_only_tombstones, [:set, :public])
        key = "flow:{flow:test}:state:deleted"

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)

        start_supervised!(
          {Ferricstore.Flow.LMDBWriter,
           shard_index: shard_index, data_dir: data_dir, instance_name: instance_name}
        )

        :ets.insert(ets, {key, nil, 0, :flow_state_deleted, :deleted, 0, 0})

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue_async(instance_name, shard_index, [], [
                   {:delete_flow_tombstone, ets, key}
                 ])

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)
        assert :ets.lookup(ets, key) == []
      end

      test "lagged mode keeps LMDB projection enabled but uses larger writer batches" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
        old_max_batch_ops = Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops)

        Application.put_env(:ferricstore, :flow_lmdb_mode, :lagged)
        Application.delete_env(:ferricstore, :flow_lmdb_flush_interval_ms)
        Application.delete_env(:ferricstore, :flow_lmdb_max_batch_ops)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_lagged_writer_#{System.unique_integer([:positive])}"
          )

        shard_index = 0
        instance_name = :"flow_lmdb_lagged_writer_#{System.unique_integer([:positive])}"
        key = "flow:{flow:test}:state:lagged"

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
          restore_env(:flow_lmdb_max_batch_ops, old_max_batch_ops)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)

        writer_pid =
          start_supervised!(
            {Ferricstore.Flow.LMDBWriter,
             shard_index: shard_index, data_dir: data_dir, instance_name: instance_name}
          )

        writer_state = :sys.get_state(writer_pid)

        assert Ferricstore.Flow.LMDB.mode() == :lagged
        assert Ferricstore.Flow.LMDB.projection_enabled?()
        assert writer_state.flush_interval_ms == 500
        assert writer_state.flush_jitter_ms == 250
        assert writer_state.max_ops == 25_000
        assert writer_state.flush_chunk_ops == 5_000
        assert writer_state.flush_chunk_pause_ms == 1

        path =
          data_dir
          |> Ferricstore.DataDir.shard_data_path(shard_index)
          |> Ferricstore.Flow.LMDB.path()

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
                   {:put, key, "v1"}
                 ])

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)
        assert {:ok, "v1"} = Ferricstore.Flow.LMDB.get(path, key)
      end

      test "LMDB mode cannot be disabled for Flow production projection" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        old_enabled = Application.get_env(:ferricstore, :flow_lmdb_enabled)

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          restore_env(:flow_lmdb_enabled, old_enabled)
        end)

        Application.delete_env(:ferricstore, :flow_lmdb_mode)
        Application.delete_env(:ferricstore, :flow_lmdb_enabled)
        assert Ferricstore.Flow.LMDB.mode() == :lagged
        assert Ferricstore.Flow.LMDB.projection_enabled?()

        for off <- [:off, false, "false", "FALSE", "0", "off", nil] do
          Application.put_env(:ferricstore, :flow_lmdb_mode, off)
          assert Ferricstore.Flow.LMDB.mode() == :lagged
          assert Ferricstore.Flow.LMDB.projection_enabled?()
        end
      end

      test "source projection reads the durable shard file instead of ETS cached value" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        Application.put_env(:ferricstore, :flow_lmdb_mode, :lagged)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_source_projection_#{System.unique_integer([:positive])}"
          )

        shard_index = 0
        instance_name = :"flow_lmdb_source_projection_#{System.unique_integer([:positive])}"
        key = "flow:{flow:test}:policy:source"
        disk_value = "from-durable-file"

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)
        shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        file_path = Path.join(shard_data_path, "00000.log")

        {:ok, [{offset, value_size}]} =
          Ferricstore.Bitcask.NIF.v2_append_batch(file_path, [{key, disk_value, 0}])

        keydir = :ets.new(:flow_lmdb_source_projection_keydir, [:set, :public])
        :ets.insert(keydir, {key, "wrong-ets-cache", 0, 0, 0, offset, value_size})

        instance_ctx = %{
          name: instance_name,
          keydir_refs: {keydir},
          disk_pressure: :atomics.new(1, signed: false),
          write_version: :counters.new(1, [:write_concurrency])
        }

        start_supervised!(
          {Ferricstore.Flow.LMDBWriter,
           shard_index: shard_index, data_dir: data_dir, instance_ctx: instance_ctx}
        )

        path = Ferricstore.Flow.LMDB.path(shard_data_path)

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
                   {:project_kv_from_source, key}
                 ])

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)
        assert {:ok, wrapped} = Ferricstore.Flow.LMDB.get(path, key)
        assert {:ok, ^disk_value} = Ferricstore.Flow.LMDB.decode_value(wrapped, 0)
      end

      test "source projection treats deleted keydir rows as deletes, not mirror degradation" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_deleted_source_#{System.unique_integer([:positive])}"
          )

        shard_index = 0
        instance_name = :"flow_lmdb_deleted_source_#{System.unique_integer([:positive])}"
        key = "flow:{flow:test}:policy:deleted-source"
        degraded = :atomics.new(1, signed: false)
        flush_failures = :atomics.new(1, signed: false)

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)
        shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        path = Ferricstore.Flow.LMDB.path(shard_data_path)

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(path, [
                   {:put, key, Ferricstore.Flow.LMDB.encode_value("stale", 0)}
                 ])

        keydir = :ets.new(:flow_lmdb_deleted_source_keydir, [:set, :public])
        :ets.insert(keydir, {key, nil, 0, 0, :deleted, 0, 0})

        instance_ctx = %{
          name: instance_name,
          keydir_refs: {keydir},
          flow_lmdb_mirror_degraded: degraded,
          flow_lmdb_writer_flush_failures: flush_failures
        }

        start_supervised!(
          {Ferricstore.Flow.LMDBWriter,
           shard_index: shard_index, data_dir: data_dir, instance_ctx: instance_ctx}
        )

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
                   {:project_kv_from_source, key}
                 ])

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)
        assert :not_found = Ferricstore.Flow.LMDB.get(path, key)
        assert :atomics.get(degraded, 1) == 0
        assert :atomics.get(flush_failures, 1) == 0
      end

      test "source projection waits for pending keydir locations to publish" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        Application.put_env(:ferricstore, :flow_lmdb_mode, :lagged)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_pending_source_#{System.unique_integer([:positive])}"
          )

        shard_index = 0
        instance_name = :"flow_lmdb_pending_source_#{System.unique_integer([:positive])}"
        key = "flow:{flow:test}:policy:pending"
        disk_value = "from-published-location"

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)
        shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        file_path = Path.join(shard_data_path, "00000.log")

        {:ok, [{offset, value_size}]} =
          Ferricstore.Bitcask.NIF.v2_append_batch(file_path, [{key, disk_value, 0}])

        keydir = :ets.new(:flow_lmdb_pending_source_keydir, [:set, :public])
        :ets.insert(keydir, {key, "wrong-ets-cache", 0, 0, :pending, 0, value_size})

        instance_ctx = %{
          name: instance_name,
          keydir_refs: {keydir},
          disk_pressure: :atomics.new(1, signed: false),
          write_version: :counters.new(1, [:write_concurrency])
        }

        start_supervised!(
          {Ferricstore.Flow.LMDBWriter,
           shard_index: shard_index, data_dir: data_dir, instance_ctx: instance_ctx}
        )

        path = Ferricstore.Flow.LMDB.path(shard_data_path)
        parent = self()

        publisher =
          spawn(fn ->
            Process.sleep(10)
            :ets.insert(keydir, {key, "wrong-ets-cache", 0, 0, 0, offset, value_size})
            send(parent, :pending_location_published)
          end)

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
                   {:project_kv_from_source, key}
                 ])

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)
        assert_receive :pending_location_published, 500
        refute Process.alive?(publisher)
        assert {:ok, wrapped} = Ferricstore.Flow.LMDB.get(path, key)
        assert {:ok, ^disk_value} = Ferricstore.Flow.LMDB.decode_value(wrapped, 0)
      end

      test "source projection reads WARaft segment-backed keydir locations" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)

        Application.put_env(:ferricstore, :flow_lmdb_mode, :lagged)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_segment_source_#{System.unique_integer([:positive])}"
          )

        instance_name = :"flow_lmdb_segment_source_#{System.unique_integer([:positive])}"
        key = "flow:{flow:test}:policy:segment"
        disk_value = :binary.copy("segment-durable-value", 32)

        ctx =
          FerricStore.Instance.build(instance_name,
            data_dir: data_dir,
            shard_count: 1,
            max_memory_bytes: 256 * 1024 * 1024,
            keydir_max_ram: 64 * 1024 * 1024,
            hot_cache_max_value_size: 16,
            blob_side_channel_threshold_bytes: 1024 * 1024,
            max_active_file_size: 64 * 1024 * 1024,
            read_sample_rate: 100,
            lfu_decay_time: 1,
            lfu_log_factor: 10
          )

        on_exit(fn ->
          Ferricstore.Raft.WARaftBackend.stop()
          FerricStore.Instance.cleanup(instance_name)
          restore_env(:flow_lmdb_mode, old_mode)
          File.rm_rf!(data_dir)
        end)

        assert :ok =
                 Ferricstore.Raft.WARaftBackend.start(ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert :ok = Ferricstore.Raft.WARaftBackend.write(0, {:put, key, disk_value, 0})

        assert [
                 {^key, nil, 0, _lfu, {:waraft_segment, index}, offset, value_size}
               ] = :ets.lookup(elem(ctx.keydir_refs, 0), key)

        assert is_integer(index) and index > 0
        assert is_integer(offset) and offset >= 0
        assert value_size == byte_size(disk_value)

        start_supervised!(
          {Ferricstore.Flow.LMDBWriter, shard_index: 0, data_dir: data_dir, instance_ctx: ctx}
        )

        path =
          data_dir
          |> Ferricstore.DataDir.shard_data_path(0)
          |> Ferricstore.Flow.LMDB.path()

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, 0, [
                   {:project_kv_from_source, key}
                 ])

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, 0)
        assert {:ok, wrapped} = Ferricstore.Flow.LMDB.get(path, key)
        assert {:ok, ^disk_value} = Ferricstore.Flow.LMDB.decode_value(wrapped, 0)
      end
    end
  end
end
