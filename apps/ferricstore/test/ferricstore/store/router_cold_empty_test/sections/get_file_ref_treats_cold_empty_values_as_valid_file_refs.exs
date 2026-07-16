defmodule Ferricstore.Store.RouterColdEmptyTest.Sections.GetFileRefTreatsColdEmptyValuesAsValidFileRefs do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Store.{CompoundKey, LFU}
      alias Ferricstore.Store.Router
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.WARaftSegmentReader
      alias Ferricstore.Stats
      alias Ferricstore.Test.IsolatedInstance

      test "get_with_file_ref treats cold empty values as valid file refs", %{
        ctx: ctx,
        shard: shard,
        keydir: keydir
      } do
        key = "cold_empty:" <> Integer.to_string(:erlang.unique_integer([:positive]))

        :ok = GenServer.call(shard, {:put, key, "", 0})
        :ok = GenServer.call(shard, :flush)

        assert [{^key, "", exp, lfu, fid, off, 0}] = :ets.lookup(keydir, key)
        :ets.insert(keydir, {key, nil, exp, lfu, fid, off, 0})

        assert {:cold_ref, path, value_offset, 0} = Router.get_with_file_ref(ctx, key)
        assert File.exists?(path)
        assert is_integer(value_offset)
      end

      test "get_with_file_ref rejects live cold rows with invalid offsets", %{
        ctx: ctx,
        keydir: keydir
      } do
        key = "cold_invalid_offset:" <> Integer.to_string(:erlang.unique_integer([:positive]))
        :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 5})

        assert {:error, {:storage_read_failed, {:invalid_keydir_entry, _entry}}} =
                 Router.get_with_file_ref(ctx, key)

        assert [{^key, nil, 0, _lfu, 0, :pending_offset, 5}] = :ets.lookup(keydir, key)
      end

      test "get_with_file_ref retries when compaction changes the cold row after validation misses",
           %{
             ctx: ctx,
             keydir: keydir
           } do
        key = "cold_sendfile_compacted:" <> Integer.to_string(:erlang.unique_integer([:positive]))
        other_key = key <> ":other"
        value = "compacted-value"
        value_size = byte_size(value)
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
        stale_path = Path.join(shard_path, "00000.log")
        current_path = Path.join(shard_path, "00001.log")

        {:ok, {stale_offset, _stale_record_size}} =
          NIF.v2_append_record(stale_path, other_key, "wrong", 0)

        {:ok, {current_offset, _current_record_size}} =
          NIF.v2_append_record(current_path, key, value, 0)

        :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, stale_offset, value_size})

        Process.put(:ferricstore_router_validate_file_ref_miss_hook, fn ->
          :ets.insert(keydir, {key, nil, 0, LFU.initial(), 1, current_offset, value_size})
        end)

        try do
          assert {:cold_ref, ^current_path, value_offset, ^value_size} =
                   Router.get_with_file_ref(ctx, key)

          assert is_integer(value_offset)
        after
          Process.delete(:ferricstore_router_validate_file_ref_miss_hook)
        end
      end

      test "get_with_file_ref flushes pending cold rows as file refs instead of materializing",
           %{
             ctx: ctx,
             shard: shard,
             keydir: keydir
           } do
        key = "cold_pending_sendfile:" <> Integer.to_string(:erlang.unique_integer([:positive]))
        value = :binary.copy("p", 2048)
        value_size = byte_size(value)

        :sys.replace_state(shard, fn state ->
          :ets.insert(keydir, {key, nil, 0, LFU.initial(), :pending, 0, value_size})

          %{
            state
            | pending: [{key, value, 0} | state.pending],
              pending_count: state.pending_count + 1
          }
        end)

        assert [{^key, nil, 0, _lfu, :pending, 0, ^value_size}] = :ets.lookup(keydir, key)

        assert {:cold_ref, path, value_offset, ^value_size} = Router.get_with_file_ref(ctx, key)
        assert File.exists?(path)
        assert is_integer(value_offset)
      end

      test "watch_token fingerprints cold rows without warming large values", %{
        ctx: ctx,
        shard: shard,
        keydir: keydir
      } do
        key = "cold_watch_token:" <> Integer.to_string(:erlang.unique_integer([:positive]))
        value = :binary.copy("w", 2048)
        value_size = byte_size(value)

        assert :ok = GenServer.call(shard, {:put, key, value, 0})
        :ok = GenServer.call(shard, :flush)
        assert [{^key, nil, 0, lfu, fid, off, ^value_size}] = :ets.lookup(keydir, key)

        assert {:watch, {:sha256, digest}, 0} = Router.watch_token(ctx, key)
        assert digest == :crypto.hash(:sha256, value)
        assert [{^key, nil, 0, ^lfu, ^fid, ^off, ^value_size}] = :ets.lookup(keydir, key)
      end

      test "batch_get_with_file_refs retries file refs after validation misses instead of materializing",
           %{
             ctx: ctx,
             keydir: keydir
           } do
        key =
          "cold_batch_sendfile_compacted:" <>
            Integer.to_string(:erlang.unique_integer([:positive]))

        other_key = key <> ":other"
        value = "compacted-batch-file-ref"
        value_size = byte_size(value)
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
        stale_path = Path.join(shard_path, "00000.log")
        current_path = Path.join(shard_path, "00001.log")

        {:ok, {stale_offset, _stale_record_size}} =
          NIF.v2_append_record(stale_path, other_key, "wrong", 0)

        {:ok, {current_offset, _current_record_size}} =
          NIF.v2_append_record(current_path, key, value, 0)

        :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, stale_offset, value_size})

        Process.put(:ferricstore_router_validate_file_ref_miss_hook, fn ->
          :ets.insert(keydir, {key, nil, 0, LFU.initial(), 1, current_offset, value_size})
        end)

        try do
          assert [{:file_ref, ^current_path, value_offset, ^value_size}] =
                   Router.batch_get_with_file_refs(ctx, [key], 1)

          assert is_integer(value_offset)
        after
          Process.delete(:ferricstore_router_validate_file_ref_miss_hook)
        end
      end

      test "batch_get reads one WARaft apply projection record once for multiple keys", %{
        ctx: ctx,
        keydir: keydir
      } do
        index = 10_000 + System.unique_integer([:positive])
        key1 = "waraft_projection_batch:{same}:a"
        key2 = "waraft_projection_batch:{same}:b"
        value1 = "projection-value-a"
        value2 = "projection-value-b"
        file_id = {:waraft_apply_projection, index}

        assert :ok =
                 WARaftSegmentReader.put_apply_projection(ctx.data_dir, 0, index, [
                   {key1, value1, 0},
                   {key2, value2, 0}
                 ])

        assert {:ok, 2} = WARaftSegmentReader.spill_apply_projection_cache(ctx.data_dir, 0)
        WARaftSegmentReader.clear_apply_projection_cache(ctx.data_dir, 0)

        :ets.insert(keydir, {key1, nil, 0, LFU.initial(), file_id, 0, byte_size(value1)})
        :ets.insert(keydir, {key2, nil, 0, LFU.initial(), file_id, 0, byte_size(value2)})

        parent = self()

        Process.put(:ferricstore_waraft_apply_projection_disk_read_hook, fn _root,
                                                                            ^index,
                                                                            source ->
          send(parent, {:apply_projection_disk_read, source})
        end)

        try do
          assert [^value1, ^value2] = Router.batch_get(ctx, [key1, key2])
        after
          Process.delete(:ferricstore_waraft_apply_projection_disk_read_hook)
        end

        reads = collect_apply_projection_disk_reads([])
        assert reads == [:latest]
      end

      test "get_with_deferred_blob_file_ref streams WARaft segment-backed blob refs", %{
        ctx: ctx,
        keydir: keydir
      } do
        ctx = %{ctx | blob_side_channel_threshold_bytes: 1}
        index = 11_000 + System.unique_integer([:positive])
        key = "waraft_blob_ref:{same}:large"
        payload = String.duplicate("blob-payload-", 24_000)

        assert {:ok, blob_ref} = Ferricstore.Store.BlobStore.put(ctx.data_dir, 0, payload)
        encoded_ref = Ferricstore.Store.BlobRef.encode!(blob_ref)
        file_id = {:waraft_apply_projection, index}

        assert :ok =
                 WARaftSegmentReader.put_apply_projection(ctx.data_dir, 0, index, [
                   {key, encoded_ref, 0}
                 ])

        :ets.insert(keydir, {key, nil, 0, LFU.initial(), file_id, 0, byte_size(encoded_ref)})

        assert {:cold_ref, path, offset, size} = Router.get_with_deferred_blob_file_ref(ctx, key)
        assert path == Ferricstore.Store.BlobRef.path(ctx.data_dir, 0, blob_ref)
        assert offset == blob_ref.offset
        assert size == byte_size(payload)
      end

      test "get_with_deferred_blob_file_ref retries when WARaft location relocates before disk read",
           %{
             ctx: ctx,
             keydir: keydir
           } do
        stale_index = 11_500 + System.unique_integer([:positive])
        key = "waraft_ref_relocated:{same}:" <> Integer.to_string(stale_index)
        value = "relocated-waraft-value"
        file_id = {:waraft_apply_projection, stale_index}

        :ets.insert(keydir, {key, nil, 0, LFU.initial(), file_id, 0, byte_size(value)})

        Process.put(:ferricstore_waraft_apply_projection_disk_read_hook, fn _root,
                                                                            ^stale_index,
                                                                            _source ->
          :ets.insert(keydir, {key, value, 0, LFU.initial(), :pending, 0, byte_size(value)})
        end)

        try do
          assert {:hot, ^value} = Router.get_with_deferred_blob_file_ref(ctx, key)
        after
          Process.delete(:ferricstore_waraft_apply_projection_disk_read_hook)
        end
      end

      test "get_with_deferred_blob_file_ref retries to newer WARaft blob location",
           %{
             ctx: ctx,
             keydir: keydir
           } do
        ctx = %{ctx | blob_side_channel_threshold_bytes: 1}
        stale_index = 11_700 + System.unique_integer([:positive])
        current_index = stale_index + 1
        key = "waraft_ref_relocated_blob:{same}:" <> Integer.to_string(stale_index)
        payload = String.duplicate("relocated-blob-", 8_000)
        stale_file_id = {:waraft_apply_projection, stale_index}
        current_file_id = {:waraft_apply_projection, current_index}

        assert {:ok, blob_ref} = Ferricstore.Store.BlobStore.put(ctx.data_dir, 0, payload)
        encoded_ref = Ferricstore.Store.BlobRef.encode!(blob_ref)

        assert :ok =
                 WARaftSegmentReader.put_apply_projection(ctx.data_dir, 0, current_index, [
                   {key, encoded_ref, 0}
                 ])

        assert {:ok, 1} = WARaftSegmentReader.spill_apply_projection_cache(ctx.data_dir, 0)
        WARaftSegmentReader.clear_apply_projection_cache(ctx.data_dir, 0)

        :ets.insert(
          keydir,
          {key, nil, 0, LFU.initial(), stale_file_id, 0, byte_size(encoded_ref)}
        )

        Process.put(:ferricstore_waraft_apply_projection_disk_read_hook, fn
          _root, ^stale_index, _source ->
            :ets.insert(
              keydir,
              {key, nil, 0, LFU.initial(), current_file_id, 0, byte_size(encoded_ref)}
            )

          _root, _index, _source ->
            :ok
        end)

        try do
          assert {:cold_ref, path, offset, size} =
                   Router.get_with_deferred_blob_file_ref(ctx, key)

          assert path == Ferricstore.Store.BlobRef.path(ctx.data_dir, 0, blob_ref)
          assert offset == blob_ref.offset
          assert size == byte_size(payload)
        after
          Process.delete(:ferricstore_waraft_apply_projection_disk_read_hook)
        end
      end

      test "batch_get_with_deferred_blob_file_refs streams WARaft segment-backed blob refs", %{
        ctx: ctx,
        keydir: keydir
      } do
        ctx = %{ctx | blob_side_channel_threshold_bytes: 1}
        index = 12_000 + System.unique_integer([:positive])
        key = "waraft_blob_ref:{same}:batch-large"
        payload = String.duplicate("batch-blob-payload-", 18_000)

        assert {:ok, blob_ref} = Ferricstore.Store.BlobStore.put(ctx.data_dir, 0, payload)
        encoded_ref = Ferricstore.Store.BlobRef.encode!(blob_ref)
        file_id = {:waraft_apply_projection, index}

        assert :ok =
                 WARaftSegmentReader.put_apply_projection(ctx.data_dir, 0, index, [
                   {key, encoded_ref, 0}
                 ])

        :ets.insert(keydir, {key, nil, 0, LFU.initial(), file_id, 0, byte_size(encoded_ref)})

        assert [{:file_ref, path, offset, size}] =
                 Router.batch_get_with_deferred_blob_file_refs(ctx, [key], 1024)

        assert path == Ferricstore.Store.BlobRef.path(ctx.data_dir, 0, blob_ref)
        assert offset == blob_ref.offset
        assert size == byte_size(payload)
      end

      test "metadata reads use logical payload size for WARaft blob refs", %{
        ctx: ctx,
        keydir: keydir
      } do
        ctx = %{ctx | blob_side_channel_threshold_bytes: 1}
        index = 12_500 + System.unique_integer([:positive])
        key = "waraft_blob_ref:{same}:metadata-size"
        payload = String.duplicate("metadata-blob-payload-", 12_000)
        range_start = 4_096
        range_len = 64
        range_end = range_start + range_len - 1

        assert {:ok, blob_ref} = Ferricstore.Store.BlobStore.put(ctx.data_dir, 0, payload)
        encoded_ref = Ferricstore.Store.BlobRef.encode!(blob_ref)
        file_id = {:waraft_apply_projection, index}

        assert :ok =
                 WARaftSegmentReader.put_apply_projection(ctx.data_dir, 0, index, [
                   {key, encoded_ref, 0}
                 ])

        :ets.insert(keydir, {key, nil, 0, LFU.initial(), file_id, 0, byte_size(encoded_ref)})

        assert Router.value_size(ctx, key) == byte_size(payload)

        tx = %Ferricstore.Store.LocalTxStore{
          instance_ctx: ctx,
          shard_index: 0,
          shard_state: %{
            instance_ctx: ctx,
            keydir: keydir,
            index: 0,
            shard_data_path: Path.join([ctx.data_dir, "data", "shard_0"]),
            data_dir: ctx.data_dir
          }
        }

        assert Ferricstore.Store.Ops.value_size(tx, key) == byte_size(payload)

        assert binary_part(payload, range_start, range_len) ==
                 Ferricstore.Commands.Strings.handle(
                   "GETRANGE",
                   [key, Integer.to_string(range_start), Integer.to_string(range_end)],
                   ctx
                 )
      end

      test "direct cold reads reject live cold rows with invalid offsets", %{
        ctx: ctx,
        keydir: keydir
      } do
        key = "cold_invalid_get:" <> Integer.to_string(:erlang.unique_integer([:positive]))
        :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 5})

        assert {:error, {:storage_read_failed, {:invalid_keydir_entry, _entry}}} =
                 Router.get(ctx, key)

        assert {:error, {:storage_read_failed, {:invalid_keydir_entry, _entry}}} =
                 Router.get_meta(ctx, key)

        assert {:error, {:storage_read_failed, {:invalid_keydir_entry, _entry}}} =
                 Router.getrange(ctx, key, 0, 2)

        assert {:error, "ERR storage read failed"} =
                 Ferricstore.Commands.Strings.handle("GETRANGE", [key, "0", "2"], ctx)

        assert [{^key, nil, 0, _lfu, 0, :pending_offset, 5}] = :ets.lookup(keydir, key)
      end

      test "compound_get reads a valid shared cold row without the shard GenServer", %{
        ctx: ctx,
        shard: shard,
        keydir: keydir
      } do
        redis_key =
          "cold_compound_direct:" <> Integer.to_string(:erlang.unique_integer([:positive]))

        compound_key = CompoundKey.hash_field(redis_key, "field")
        value = "compound-cold-value"
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
        path = Path.join(shard_path, "00000.log")

        {:ok, {offset, _record_size}} = NIF.v2_append_record(path, compound_key, value, 0)
        :ets.insert(keydir, {compound_key, nil, 0, LFU.initial(), 0, offset, byte_size(value)})

        shard_name = elem(ctx.shard_names, 0)
        Process.unregister(shard_name)

        try do
          assert [{^compound_key, nil, 0, _lfu, 0, ^offset, _vsize}] =
                   :ets.lookup(keydir, compound_key)

          assert value == Router.compound_get(ctx, redis_key, compound_key)
        after
          if Process.alive?(shard) and Process.whereis(shard_name) == nil do
            Process.register(shard, shard_name)
          end
        end
      end

      test "compound_get_meta reads a valid shared cold row without the shard GenServer", %{
        ctx: ctx,
        shard: shard,
        keydir: keydir
      } do
        redis_key =
          "cold_compound_meta_direct:" <> Integer.to_string(:erlang.unique_integer([:positive]))

        compound_key = CompoundKey.hash_field(redis_key, "field")
        value = "compound-cold-meta-value"
        expire_at_ms = Ferricstore.HLC.now_ms() + 60_000
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
        path = Path.join(shard_path, "00000.log")

        {:ok, {offset, _record_size}} =
          NIF.v2_append_record(path, compound_key, value, expire_at_ms)

        :ets.insert(
          keydir,
          {compound_key, nil, expire_at_ms, LFU.initial(), 0, offset, byte_size(value)}
        )

        shard_name = elem(ctx.shard_names, 0)
        Process.unregister(shard_name)

        try do
          assert [{^compound_key, nil, ^expire_at_ms, _lfu, 0, ^offset, _vsize}] =
                   :ets.lookup(keydir, compound_key)

          assert {value, expire_at_ms} == Router.compound_get_meta(ctx, redis_key, compound_key)
        after
          if Process.alive?(shard) and Process.whereis(shard_name) == nil do
            Process.register(shard, shard_name)
          end
        end
      end

      test "compound_get waits through a delayed compaction ETS update without shard fallback", %{
        ctx: ctx,
        shard: shard,
        keydir: keydir
      } do
        redis_key =
          "cold_compound_gap:" <> Integer.to_string(:erlang.unique_integer([:positive]))

        compound_key = CompoundKey.hash_field(redis_key, "field")
        value = "compound-delayed-compacted-value"

        {_old_offset, new_offset, value_size} =
          insert_replaced_cold_compound_row(ctx, keydir, compound_key, value, 0)

        Process.put(:ferricstore_router_cold_location_miss_hook, fn ->
          misses = Process.get(:compound_get_compaction_misses, 0) + 1
          Process.put(:compound_get_compaction_misses, misses)

          if misses == 2 do
            :ets.insert(keydir, {compound_key, nil, 0, LFU.initial(), 0, new_offset, value_size})
          end
        end)

        try do
          with_unregistered_shard(ctx, shard, fn ->
            assert ^value = Router.compound_get(ctx, redis_key, compound_key)
          end)
        after
          Process.delete(:ferricstore_router_cold_location_miss_hook)
          Process.delete(:compound_get_compaction_misses)
        end
      end

      test "compound_batch_get waits through a delayed compaction ETS update without shard fallback",
           %{
             ctx: ctx,
             shard: shard,
             keydir: keydir
           } do
        redis_key =
          "cold_compound_batch_gap:" <> Integer.to_string(:erlang.unique_integer([:positive]))

        compound_key = CompoundKey.hash_field(redis_key, "field")
        value = "compound-batch-delayed-compacted-value"

        {_old_offset, new_offset, value_size} =
          insert_replaced_cold_compound_row(ctx, keydir, compound_key, value, 0)

        Process.put(:ferricstore_router_cold_location_miss_hook, fn ->
          misses = Process.get(:compound_batch_get_compaction_misses, 0) + 1
          Process.put(:compound_batch_get_compaction_misses, misses)

          if misses == 2 do
            :ets.insert(keydir, {compound_key, nil, 0, LFU.initial(), 0, new_offset, value_size})
          end
        end)

        try do
          with_unregistered_shard(ctx, shard, fn ->
            assert [^value] = Router.compound_batch_get(ctx, redis_key, [compound_key])
          end)
        after
          Process.delete(:ferricstore_router_cold_location_miss_hook)
          Process.delete(:compound_batch_get_compaction_misses)
        end
      end

      test "compound_get_meta waits through a delayed compaction ETS update without shard fallback",
           %{
             ctx: ctx,
             shard: shard,
             keydir: keydir
           } do
        redis_key =
          "cold_compound_meta_gap:" <> Integer.to_string(:erlang.unique_integer([:positive]))

        compound_key = CompoundKey.hash_field(redis_key, "field")
        value = "compound-meta-delayed-compacted-value"
        expire_at_ms = Ferricstore.HLC.now_ms() + 60_000

        {_old_offset, new_offset, value_size} =
          insert_replaced_cold_compound_row(ctx, keydir, compound_key, value, expire_at_ms)

        Process.put(:ferricstore_router_cold_location_miss_hook, fn ->
          misses = Process.get(:compound_get_meta_compaction_misses, 0) + 1
          Process.put(:compound_get_meta_compaction_misses, misses)

          if misses == 2 do
            :ets.insert(
              keydir,
              {compound_key, nil, expire_at_ms, LFU.initial(), 0, new_offset, value_size}
            )
          end
        end)

        try do
          with_unregistered_shard(ctx, shard, fn ->
            assert {^value, ^expire_at_ms} =
                     Router.compound_get_meta(ctx, redis_key, compound_key)
          end)
        after
          Process.delete(:ferricstore_router_cold_location_miss_hook)
          Process.delete(:compound_get_meta_compaction_misses)
        end
      end

      test "compound_batch_get_meta waits through a delayed compaction ETS update without shard fallback",
           %{
             ctx: ctx,
             shard: shard,
             keydir: keydir
           } do
        redis_key =
          "cold_compound_batch_meta_gap:" <>
            Integer.to_string(:erlang.unique_integer([:positive]))

        compound_key = CompoundKey.hash_field(redis_key, "field")
        value = "compound-batch-meta-delayed-compacted-value"
        expire_at_ms = Ferricstore.HLC.now_ms() + 60_000

        {_old_offset, new_offset, value_size} =
          insert_replaced_cold_compound_row(ctx, keydir, compound_key, value, expire_at_ms)

        Process.put(:ferricstore_router_cold_location_miss_hook, fn ->
          misses = Process.get(:compound_batch_get_meta_compaction_misses, 0) + 1
          Process.put(:compound_batch_get_meta_compaction_misses, misses)

          if misses == 2 do
            :ets.insert(
              keydir,
              {compound_key, nil, expire_at_ms, LFU.initial(), 0, new_offset, value_size}
            )
          end
        end)

        try do
          with_unregistered_shard(ctx, shard, fn ->
            assert [{^value, ^expire_at_ms}] =
                     Router.compound_batch_get_meta(ctx, redis_key, [compound_key])
          end)
        after
          Process.delete(:ferricstore_router_cold_location_miss_hook)
          Process.delete(:compound_batch_get_meta_compaction_misses)
        end
      end

      test "string batch install clears compound data when cold type marker moves during compaction",
           %{
             ctx: ctx,
             keydir: keydir
           } do
        redis_key =
          "cold_compound_marker_gap:" <> Integer.to_string(:erlang.unique_integer([:positive]))

        marker_key = CompoundKey.type_key(redis_key)
        field_key = CompoundKey.hash_field(redis_key, "field")
        value_size = byte_size("hash")
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
        current_path = Path.join(shard_path, "00001.log")

        {:ok, {current_offset, _record_size}} =
          NIF.v2_append_record(current_path, marker_key, "hash", 0)

        :ets.insert(keydir, {marker_key, nil, 0, LFU.initial(), 9, 0, value_size})
        :ets.insert(keydir, {field_key, "old-field-value", 0, LFU.initial(), :pending, 0, 15})

        Process.put(:ferricstore_router_cold_location_miss_hook, fn ->
          misses = Process.get(:compound_marker_compaction_misses, 0) + 1
          Process.put(:compound_marker_compaction_misses, misses)

          if misses == 2 do
            :ets.insert(
              keydir,
              {marker_key, nil, 0, LFU.initial(), 1, current_offset, value_size}
            )
          end
        end)

        try do
          Router.__install_batch_entries_for_test__(
            ctx,
            0,
            [{redis_key, "string-value", "string-value"}],
            %{}
          )

          assert [] == :ets.lookup(keydir, marker_key)
          assert [] == :ets.lookup(keydir, field_key)

          assert [{^redis_key, "string-value", 0, _lfu, :pending, 0, 12}] =
                   :ets.lookup(keydir, redis_key)
        after
          Process.delete(:ferricstore_router_cold_location_miss_hook)
          Process.delete(:compound_marker_compaction_misses)
        end
      end

      test "failed direct cold GET does not increment keyspace misses", %{
        ctx: ctx,
        keydir: keydir
      } do
        key = "cold_missing_get_stats:" <> Integer.to_string(:erlang.unique_integer([:positive]))
        :ets.insert(keydir, {key, nil, 0, LFU.initial(), 99, 0, 5})

        before_misses = Stats.keyspace_misses(ctx)

        assert {:error, {:storage_read_failed, _reason}} = Router.get(ctx, key)
        assert Stats.keyspace_misses(ctx) == before_misses
      end

      test "failed direct cold GET_META does not increment misses or cold-read success", %{
        ctx: ctx,
        keydir: keydir
      } do
        key =
          "cold_missing_get_meta_stats:" <> Integer.to_string(:erlang.unique_integer([:positive]))

        :ets.insert(keydir, {key, nil, 0, LFU.initial(), 99, 0, 5})

        before_misses = Stats.keyspace_misses(ctx)
        before_cold_reads = Stats.total_cold_reads(ctx)

        assert {:error, {:storage_read_failed, _reason}} = Router.get_meta(ctx, key)
        assert Stats.keyspace_misses(ctx) == before_misses
        assert Stats.total_cold_reads(ctx) == before_cold_reads
      end

      test "direct cold read retries when compaction replaces file before ETS offset update", %{
        ctx: ctx,
        keydir: keydir
      } do
        key = "cold_compaction_gap:" <> Integer.to_string(:erlang.unique_integer([:positive]))
        value = "compacted-gap-value"
        value_size = byte_size(value)
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
        path = Path.join(shard_path, "00000.log")

        {:ok, {_dead_offset, _dead_record_size}} = NIF.v2_append_record(path, "dead", "old", 0)
        {:ok, {old_offset, _old_record_size}} = NIF.v2_append_record(path, key, value, 0)

        File.rm!(path)
        {:ok, {new_offset, _new_record_size}} = NIF.v2_append_record(path, key, value, 0)

        :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, old_offset, value_size})

        Process.put(:ferricstore_router_cold_location_miss_hook, fn ->
          :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, new_offset, value_size})
        end)

        try do
          assert ^value = Router.get(ctx, key)
        after
          Process.delete(:ferricstore_router_cold_location_miss_hook)
        end
      end

      test "getrange retries when compaction removes file after value ref validation", %{
        ctx: ctx,
        keydir: keydir
      } do
        key = "cold_getrange_pread_gap:" <> Integer.to_string(:erlang.unique_integer([:positive]))
        old_value = "0123456789abcdef"
        current_value = "ABCDEFGHIJKLMNOP"
        value_size = byte_size(current_value)
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
        stale_path = Path.join(shard_path, "00001.log")
        current_path = Path.join(shard_path, "00002.log")
        test_pid = self()

        {:ok, {stale_offset, _stale_record_size}} =
          NIF.v2_append_record(stale_path, key, old_value, 0)

        {:ok, {current_offset, _current_record_size}} =
          NIF.v2_append_record(current_path, key, current_value, 0)

        :ets.insert(keydir, {key, nil, 0, LFU.initial(), 1, stale_offset, value_size})

        Process.put(:ferricstore_router_cold_range_pread_miss_hook, fn ->
          send(test_pid, :cold_range_pread_gap)
          File.rm(stale_path)
          :ets.insert(keydir, {key, nil, 0, LFU.initial(), 2, current_offset, value_size})
        end)

        try do
          assert "EFGH" == Router.getrange(ctx, key, 4, 7)
          assert_receive :cold_range_pread_gap, 500
        after
          Process.delete(:ferricstore_router_cold_range_pread_miss_hook)
        end
      end

      test "direct cold read waits through a delayed compaction ETS update", %{
        ctx: ctx,
        keydir: keydir
      } do
        key = "cold_compaction_delayed:" <> Integer.to_string(:erlang.unique_integer([:positive]))
        value = "delayed-compacted-value"
        value_size = byte_size(value)
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
        path = Path.join(shard_path, "00000.log")

        {:ok, {_dead_offset, _dead_record_size}} = NIF.v2_append_record(path, "dead", "old", 0)
        {:ok, {old_offset, _old_record_size}} = NIF.v2_append_record(path, key, value, 0)

        File.rm!(path)
        {:ok, {new_offset, _new_record_size}} = NIF.v2_append_record(path, key, value, 0)

        :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, old_offset, value_size})

        Process.put(:ferricstore_router_cold_location_miss_hook, fn ->
          misses = Process.get(:delayed_compaction_misses, 0) + 1
          Process.put(:delayed_compaction_misses, misses)

          if misses == 2 do
            :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, new_offset, value_size})
          end
        end)

        try do
          assert ^value = Router.get(ctx, key)
        after
          Process.delete(:ferricstore_router_cold_location_miss_hook)
          Process.delete(:delayed_compaction_misses)
        end
      end

      test "batch cold read waits through a delayed compaction ETS update", %{
        ctx: ctx,
        keydir: keydir
      } do
        key =
          "cold_batch_compaction_delayed:" <>
            Integer.to_string(:erlang.unique_integer([:positive]))

        value = "delayed-batch-compacted-value"
        value_size = byte_size(value)
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
        path = Path.join(shard_path, "00000.log")

        {:ok, {_dead_offset, _dead_record_size}} = NIF.v2_append_record(path, "dead", "old", 0)
        {:ok, {old_offset, _old_record_size}} = NIF.v2_append_record(path, key, value, 0)

        File.rm!(path)
        {:ok, {new_offset, _new_record_size}} = NIF.v2_append_record(path, key, value, 0)

        :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, old_offset, value_size})

        Process.put(:ferricstore_router_cold_location_miss_hook, fn ->
          misses = Process.get(:delayed_batch_compaction_misses, 0) + 1
          Process.put(:delayed_batch_compaction_misses, misses)

          if misses == 2 do
            :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, new_offset, value_size})
          end
        end)

        try do
          assert [^value] = Router.batch_get(ctx, [key])
        after
          Process.delete(:ferricstore_router_cold_location_miss_hook)
          Process.delete(:delayed_batch_compaction_misses)
        end
      end
    end
  end
end
