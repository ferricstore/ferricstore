defmodule Ferricstore.Store.BlobSideChannelTest.Sections.Part01 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Store.{BlobRef, BlobStore, ColdRead, CompoundKey, LFU, LocalTxStore, Ops, Router}
      alias Ferricstore.Store.Shard.Compound, as: ShardCompound
      alias Ferricstore.Store.Shard.ETS, as: ShardETS
      alias Ferricstore.Raft.StateMachine
      alias Ferricstore.Test.IsolatedInstance

  test "blob garbage sweep streams keydir refs without copying the full ETS table" do
    source = File.read!(@router_source_path)

    [function_source] =
      Regex.run(
        ~r/defp blob_gc_keydir_live_refs\(ctx, idx, state, keydir, now\) do.*?^  end/ms,
        source
      )

    refute function_source =~ ":ets.tab2list",
           "blob GC must not materialize the whole keydir while collecting live refs"

    assert function_source =~ ":ets.foldl"
  end

  test "large direct values are persisted as blob refs and materialized on reads", %{
    ctx: ctx,
    shard: shard,
    keydir: keydir
  } do
    key = "blob:auto:large"
    payload = :binary.copy("L", 1024)

    assert :ok = Router.put(ctx, key, payload, 0)

    assert {:ok, encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, key)
    assert byte_size(encoded_ref) == BlobRef.encoded_size()
    assert {:ok, ^payload} = BlobStore.get(ctx.data_dir, 0, ref)

    assert payload == Router.get(ctx, key)
    assert [payload] == Router.batch_get(ctx, [key])
    assert {payload, 0} == Router.get_meta(ctx, key)
    assert byte_size(payload) == Router.value_size(ctx, key)
    assert binary_part(payload, 128, 64) == Router.getrange(ctx, key, 128, 191)

    assert {:ok, {blob_path, blob_offset, blob_size}} = BlobStore.file_ref(ctx.data_dir, 0, ref)
    assert blob_size == byte_size(payload)
    payload_size = byte_size(payload)

    assert {blob_path, blob_offset, payload_size} == Router.get_file_ref(ctx, key)

    assert {:cold_ref, ^blob_path, ^blob_offset, ^payload_size} =
             Router.get_with_file_ref(ctx, key)

    assert [{:file_ref, ^blob_path, ^blob_offset, ^payload_size}] =
             Router.batch_get_with_file_refs(ctx, [key], 64)

    assert payload == GenServer.call(shard, {:get, key})
  end

  test "deferred file-ref lookup returns validated blob segment ref", %{ctx: ctx} do
    key = "blob:auto:deferred-file-ref"
    payload = :binary.copy("D", 1024)

    assert :ok = Router.put(ctx, key, payload, 0)

    assert {:cold_ref, blob_path, blob_offset, 1024} =
             Router.get_with_deferred_blob_file_ref(ctx, key)

    assert Path.extname(blob_path) == ".bloblog"
    assert is_integer(blob_offset) and blob_offset >= 0
  end

  test "deferred batch file-ref lookup returns validated blob segment refs", %{ctx: ctx} do
    key = "blob:auto:deferred-batch-file-ref"
    payload = :binary.copy("B", 1024)

    assert :ok = Router.put(ctx, key, payload, 0)

    assert [{:file_ref, blob_path, blob_offset, 1024}] =
             Router.batch_get_with_deferred_blob_file_refs(ctx, [key], 64)

    assert {[{:file_ref, ^blob_path, ^blob_offset, 1024}], true} =
             Router.batch_get_with_deferred_blob_file_refs_and_presence(ctx, [key], 64)

    assert Path.extname(blob_path) == ".bloblog"
    assert is_integer(blob_offset) and blob_offset >= 0
  end

  test "deferred file-ref lookup rejects corrupt blob segment headers", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "blob:auto:deferred-corrupt-segment-header"
    payload = :binary.copy("H", 1024)

    assert :ok = Router.put(ctx, key, payload, 0)
    assert {:ok, _encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, key)

    corrupt_segment_header!(ctx.data_dir, 0, ref)

    refute match?(
             {:cold_ref, _path, _offset, _size},
             Router.get_with_deferred_blob_file_ref(ctx, key)
           )

    refute match?(
             [{:file_ref, _path, _offset, _size}],
             Router.batch_get_with_deferred_blob_file_refs(ctx, [key], 64)
           )
  end

  test "file-ref reads keep streaming fast path while materialized reads reject corrupt blobs", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "blob:auto:corrupt-file-ref"
    payload = :binary.copy("Z", 1024)

    assert :ok = Router.put(ctx, key, payload, 0)
    assert {:ok, _encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, key)

    blob_path = BlobRef.path(ctx.data_dir, 0, ref)
    overwrite_segment_payload!(ctx.data_dir, 0, ref, :binary.copy("x", byte_size(payload)))

    assert {^blob_path, blob_offset, 1024} = Router.get_file_ref(ctx, key)
    assert blob_offset == ref.offset
    assert {:cold_ref, ^blob_path, ^blob_offset, 1024} = Router.get_with_file_ref(ctx, key)

    assert [{:file_ref, ^blob_path, ^blob_offset, 1024}] =
             Router.batch_get_with_file_refs(ctx, [key], 64)

    assert nil == Router.get(ctx, key)
  end

  test "large duplicate values share one append segment file", %{
    ctx: ctx,
    keydir: keydir
  } do
    payload = :binary.copy("D", 2048)

    assert :ok = Router.put(ctx, "blob:auto:a", payload, 0)
    assert :ok = Router.put(ctx, "blob:auto:b", payload, 0)

    assert {:ok, encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, "blob:auto:a")
    assert {:ok, second_encoded_ref, second_ref} = raw_disk_blob_ref(ctx, keydir, "blob:auto:b")

    refute second_encoded_ref == encoded_ref
    assert second_ref.checksum == ref.checksum
    assert second_ref.offset != ref.offset
    assert [blob_path] = Path.wildcard(Path.join(ctx.data_dir, "blob/shard_0/segments/*.bloblog"))
    assert BlobRef.path(ctx.data_dir, 0, ref) == blob_path
    assert {:ok, ^payload} = BlobStore.get(ctx.data_dir, 0, ref)
    assert {:ok, ^payload} = BlobStore.get(ctx.data_dir, 0, second_ref)
  end

  test "batch_get_with_file_refs batches encoded blob ref reads before file-ref validation", %{
    ctx: ctx,
    keydir: keydir
  } do
    key_a = "blob:auto:batch-ref-a"
    key_b = "blob:auto:batch-ref-b"
    payload_a = :binary.copy("A", 1024)
    payload_b = :binary.copy("B", 2048)

    assert {:ok, ref_a} = BlobStore.put(ctx.data_dir, 0, payload_a)
    assert {:ok, ref_b} = BlobStore.put(ctx.data_dir, 0, payload_b)

    encoded_a = BlobRef.encode!(ref_a)
    encoded_b = BlobRef.encode!(ref_b)

    :ets.insert(keydir, {
      key_a,
      nil,
      0,
      LFU.initial(),
      0,
      111_111,
      BlobRef.encoded_size()
    })

    :ets.insert(keydir, {
      key_b,
      nil,
      0,
      LFU.initial(),
      0,
      222_222,
      BlobRef.encoded_size()
    })

    Process.put(:ferricstore_router_pread_batch_keyed_result, {:ok, [encoded_a, encoded_b]})

    try do
      assert [
               {:file_ref, path_a, offset_a, 1024},
               {:file_ref, path_b, offset_b, 2048}
             ] = Router.batch_get_with_file_refs(ctx, [key_a, key_b], 64)

      assert {:ok, {^path_a, ^offset_a, 1024}} = BlobStore.file_ref(ctx.data_dir, 0, ref_a)
      assert {:ok, {^path_b, ^offset_b, 2048}} = BlobStore.file_ref(ctx.data_dir, 0, ref_b)
    after
      Process.delete(:ferricstore_router_pread_batch_keyed_result)
    end
  end

  test "batch_get_with_file_refs validates encoded blob refs with one segment open", %{
    ctx: ctx,
    keydir: keydir
  } do
    key_a = "blob:auto:batch-ref-open-a"
    key_b = "blob:auto:batch-ref-open-b"
    payload_a = :binary.copy("A", 1024)
    payload_b = :binary.copy("B", 2048)

    assert {:ok, [ref_a, ref_b]} = BlobStore.put_many(ctx.data_dir, 0, [payload_a, payload_b])
    assert BlobRef.path(ctx.data_dir, 0, ref_a) == BlobRef.path(ctx.data_dir, 0, ref_b)

    encoded_a = BlobRef.encode!(ref_a)
    encoded_b = BlobRef.encode!(ref_b)

    :ets.insert(keydir, {
      key_a,
      nil,
      0,
      LFU.initial(),
      0,
      444_444,
      BlobRef.encoded_size()
    })

    :ets.insert(keydir, {
      key_b,
      nil,
      0,
      LFU.initial(),
      0,
      555_555,
      BlobRef.encoded_size()
    })

    parent = self()
    segment_path = BlobRef.path(ctx.data_dir, 0, ref_a)

    Process.put(:ferricstore_router_pread_batch_keyed_result, {:ok, [encoded_a, encoded_b]})

    Process.put(:ferricstore_blob_store_open_read_hook, fn path, modes ->
      send(parent, {:blob_open_read, path})
      File.open(path, modes)
    end)

    try do
      assert [
               {:file_ref, ^segment_path, offset_a, 1024},
               {:file_ref, ^segment_path, offset_b, 2048}
             ] = Router.batch_get_with_file_refs(ctx, [key_a, key_b], 64)

      assert offset_a == ref_a.offset
      assert offset_b == ref_b.offset
      assert_received {:blob_open_read, ^segment_path}
      refute_received {:blob_open_read, _}
    after
      Process.delete(:ferricstore_router_pread_batch_keyed_result)
      Process.delete(:ferricstore_blob_store_open_read_hook)
    end
  end

  test "batch_get_with_file_refs keeps fixed-size non-ref cold values inline", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "blob:auto:batch-normal-encoded-size"
    value = :binary.copy("N", BlobRef.encoded_size())

    :ets.insert(keydir, {
      key,
      nil,
      0,
      LFU.initial(),
      0,
      333_333,
      byte_size(value)
    })

    Process.put(:ferricstore_router_pread_batch_keyed_result, {:ok, [value]})

    try do
      assert [^value] = Router.batch_get_with_file_refs(ctx, [key], 64)
    after
      Process.delete(:ferricstore_router_pread_batch_keyed_result)
    end
  end

  test "large direct hot-cache values still publish blob disk locations" do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 4096,
        blob_side_channel_threshold_bytes: 128
      )

    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    keydir = elem(ctx.keydir_refs, 0)
    key = "blob:auto:hot-pending"
    payload = :binary.copy("H", 1024)

    assert :ok = Router.put(ctx, key, payload, 0)

    assert [{^key, value, 0, _lfu, fid, off, vsize}] = :ets.lookup(keydir, key)
    assert value == payload
    assert is_integer(fid) and fid >= 0
    assert is_integer(off) and off >= 0
    assert vsize == BlobRef.encoded_size()

    assert {:ok, _encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, key)
    assert {:ok, ^payload} = BlobStore.get(ctx.data_dir, 0, ref)

    assert payload == Router.get(ctx, key)
  end

  test "small direct values stay inline in Bitcask", %{ctx: ctx, keydir: keydir} do
    key = "blob:auto:small"
    payload = "small-value"

    assert :ok = Router.put(ctx, key, payload, 0)
    assert {:ok, ^payload} = raw_disk_value(ctx, keydir, key)
    assert :error == BlobRef.decode(payload)
    assert payload == Router.get(ctx, key)
  end

  test "BlobRef-shaped user values round trip as normal payload bytes", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "blob:auto:literal-ref-shaped"

    payload =
      BlobRef.encode!(%BlobRef{
        size: 123,
        checksum: :binary.copy(<<1>>, 32)
      })

    assert byte_size(payload) < ctx.blob_side_channel_threshold_bytes
    assert :ok = Router.put(ctx, key, payload, 0)

    assert {:ok, _encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, key)
    assert {:ok, ^payload} = BlobStore.get(ctx.data_dir, 0, ref)
    assert payload == Router.get(ctx, key)
  end

  test "BlobRef-shaped user values round trip when blob side-channel is disabled" do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 16,
        blob_side_channel_threshold_bytes: 0
      )

    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    key = "blob:auto:literal-ref-disabled"

    payload =
      BlobRef.encode!(%BlobRef{
        size: 123,
        checksum: :binary.copy(<<2>>, 32)
      })

    assert :ok = Router.put(ctx, key, payload, 0)
    assert payload == Router.get(ctx, key)
    assert [payload] == Router.batch_get(ctx, [key])
    assert {payload, 0} == Router.get_meta(ctx, key)
  end

  test "Ra apply persists large values as blob refs while keeping logical reads intact", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "blob:auto:raft"
    payload = :binary.copy("R", 1536)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    active_file_path = ShardETS.file_path(shard_path, 0)

    state =
      StateMachine.init(%{
        shard_index: 0,
        shard_data_path: shard_path,
        active_file_id: 0,
        active_file_path: active_file_path,
        ets: keydir,
        data_dir: ctx.data_dir,
        instance_ctx: ctx,
        instance_name: ctx.name
      })

    assert_state_machine_result(
      :ok,
      StateMachine.apply(%{index: 1}, {:put, key, payload, 0}, state)
    )

    assert {:ok, encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, key)
    assert byte_size(encoded_ref) == BlobRef.encoded_size()
    assert {:ok, ^payload} = BlobStore.get(ctx.data_dir, 0, ref)
    assert payload == Router.get(ctx, key)
    assert byte_size(payload) == Router.value_size(ctx, key)
    assert binary_part(payload, 256, 32) == Router.getrange(ctx, key, 256, 287)
  end

  test "Ra put_batch externalizes large values with one blob segment fsync", %{
    ctx: ctx,
    keydir: keydir
  } do
    parent = self()

    Process.put(:ferricstore_blob_store_fsync_file_hook, fn path ->
      send(parent, {:blob_fsync_file, path})
      :ok
    end)

    key_a = "blob:auto:raft-put-batch-a"
    key_b = "blob:auto:raft-put-batch-b"
    payload_a = :binary.copy("A", 1536)
    payload_b = :binary.copy("B", 2048)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    active_file_path = ShardETS.file_path(shard_path, 0)

    state =
      StateMachine.init(%{
        shard_index: 0,
        shard_data_path: shard_path,
        active_file_id: 0,
        active_file_path: active_file_path,
        ets: keydir,
        data_dir: ctx.data_dir,
        instance_ctx: ctx,
        instance_name: ctx.name
      })

    try do
      assert_state_machine_result(
        [:ok, :ok],
        StateMachine.apply(
          %{index: 1},
          {:put_batch, [{key_a, payload_a, 0}, {key_b, payload_b, 0}]},
          state
        )
      )

      assert payload_a == Router.get(ctx, key_a)
      assert payload_b == Router.get(ctx, key_b)
      assert {:ok, _encoded_a, ref_a} = raw_disk_blob_ref(ctx, keydir, key_a)
      assert {:ok, _encoded_b, ref_b} = raw_disk_blob_ref(ctx, keydir, key_b)

      assert {:ok, {segment_path, _offset_a, _size_a}} =
               BlobStore.file_ref(ctx.data_dir, 0, ref_a)

      assert {:ok, {^segment_path, _offset_b, _size_b}} =
               BlobStore.file_ref(ctx.data_dir, 0, ref_b)

      assert_received {:blob_fsync_file, ^segment_path}
      refute_received {:blob_fsync_file, _}
    after
      Process.delete(:ferricstore_blob_store_fsync_file_hook)
    end
  end

  test "Ra apply accepts pre-externalized blob refs without double externalizing", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "blob:auto:raft-ref-only"
    payload = :binary.copy("B", 1536)
    assert {:ok, ref} = BlobStore.put(ctx.data_dir, 0, payload)
    encoded_ref = BlobRef.encode!(ref)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    active_file_path = ShardETS.file_path(shard_path, 0)

    state =
      StateMachine.init(%{
        shard_index: 0,
        shard_data_path: shard_path,
        active_file_id: 0,
        active_file_path: active_file_path,
        ets: keydir,
        data_dir: ctx.data_dir,
        instance_ctx: ctx,
        instance_name: ctx.name
      })

    assert_state_machine_result(
      :ok,
      StateMachine.apply(%{index: 1}, {:put_blob_ref, key, encoded_ref, 0}, state)
    )

    assert {:ok, ^encoded_ref, ^ref} = raw_disk_blob_ref(ctx, keydir, key)
    assert payload == Router.get(ctx, key)
  end

  test "Ra apply verifies pre-externalized put refs without materializing payload", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "blob:auto:raft-ref-verify-only"
    payload = :binary.copy("V", 1536)
    assert {:ok, ref} = BlobStore.put(ctx.data_dir, 0, payload)
    encoded_ref = BlobRef.encode!(ref)
    parent = self()
    data_dir = ctx.data_dir

    Process.put(:ferricstore_blob_store_verify_hook, fn ^data_dir, 0, ^ref ->
      send(parent, {:blob_verify, ref})
      :ok
    end)

    Process.put(:ferricstore_blob_store_open_read_hook, fn _path, _modes ->
      raise "unexpected full blob materialization"
    end)

    on_exit(fn ->
      Process.delete(:ferricstore_blob_store_verify_hook)
      Process.delete(:ferricstore_blob_store_open_read_hook)
    end)

    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    active_file_path = ShardETS.file_path(shard_path, 0)

    state =
      StateMachine.init(%{
        shard_index: 0,
        shard_data_path: shard_path,
        active_file_id: 0,
        active_file_path: active_file_path,
        ets: keydir,
        data_dir: ctx.data_dir,
        instance_ctx: ctx,
        instance_name: ctx.name
      })

    assert_state_machine_result(
      :ok,
      StateMachine.apply(%{index: 1}, {:put_blob_ref, key, encoded_ref, 0}, state)
    )

    assert_received {:blob_verify, ^ref}
    assert {:ok, ^encoded_ref, ^ref} = raw_disk_blob_ref(ctx, keydir, key)
  end

  test "Ra apply rejects a pre-externalized blob ref when the blob is missing", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "blob:auto:raft-ref-missing"
    payload = :binary.copy("M", 1536)
    ref = BlobRef.from_payload(payload)
    encoded_ref = BlobRef.encode!(ref)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    active_file_path = ShardETS.file_path(shard_path, 0)

    state =
      StateMachine.init(%{
        shard_index: 0,
        shard_data_path: shard_path,
        active_file_id: 0,
        active_file_path: active_file_path,
        ets: keydir,
        data_dir: ctx.data_dir,
        instance_ctx: ctx,
        instance_name: ctx.name
      })

    assert_state_machine_result(
      {:error, {:blob_ref_unavailable, :enoent}},
      StateMachine.apply(%{index: 1}, {:put_blob_ref, key, encoded_ref, 0}, state)
    )

    assert [] == :ets.lookup(keydir, key)
  end

  test "Ra apply rejects a same-size corrupt pre-externalized blob ref", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "blob:auto:raft-ref-corrupt"
    payload = :binary.copy("C", 1536)
    assert {:ok, ref} = BlobStore.put(ctx.data_dir, 0, payload)
    encoded_ref = BlobRef.encode!(ref)
    overwrite_segment_payload!(ctx.data_dir, 0, ref, :binary.copy("x", byte_size(payload)))

    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    active_file_path = ShardETS.file_path(shard_path, 0)

    state =
      StateMachine.init(%{
        shard_index: 0,
        shard_data_path: shard_path,
        active_file_id: 0,
        active_file_path: active_file_path,
        ets: keydir,
        data_dir: ctx.data_dir,
        instance_ctx: ctx,
        instance_name: ctx.name
      })

    assert_state_machine_result(
      {:error, {:blob_ref_unavailable, :checksum_mismatch}},
      StateMachine.apply(%{index: 1}, {:put_blob_ref, key, encoded_ref, 0}, state)
    )

    assert [] == :ets.lookup(keydir, key)
  end

  test "Ra apply accepts pre-externalized blob refs in batch without double externalizing", %{
    ctx: ctx,
    keydir: keydir
  } do
    small_key = "blob:auto:raft-ref-batch-small"
    blob_key = "blob:auto:raft-ref-batch-large"
    small = "small"
    payload = :binary.copy("C", 1536)
    assert {:ok, ref} = BlobStore.put(ctx.data_dir, 0, payload)
    encoded_ref = BlobRef.encode!(ref)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    active_file_path = ShardETS.file_path(shard_path, 0)

    state =
      StateMachine.init(%{
        shard_index: 0,
        shard_data_path: shard_path,
        active_file_id: 0,
        active_file_path: active_file_path,
        ets: keydir,
        data_dir: ctx.data_dir,
        instance_ctx: ctx,
        instance_name: ctx.name
      })

    assert_state_machine_result(
      [:ok, :ok],
      StateMachine.apply(
        %{index: 1},
        {:put_blob_batch,
         [
           {small_key, small, 0, :value},
           {blob_key, encoded_ref, 0, :blob_ref}
         ]},
        state
      )
    )

    assert small == Router.get(ctx, small_key)
    assert payload == Router.get(ctx, blob_key)
    assert {:ok, ^encoded_ref, ^ref} = raw_disk_blob_ref(ctx, keydir, blob_key)
  end

  test "Ra apply validates duplicate pre-externalized batch refs once", %{
    ctx: ctx,
    keydir: keydir
  } do
    first_key = "blob:auto:raft-ref-batch-shared-a"
    second_key = "blob:auto:raft-ref-batch-shared-b"
    payload = :binary.copy("S", 1536)
    assert {:ok, ref} = BlobStore.put(ctx.data_dir, 0, payload)
    encoded_ref = BlobRef.encode!(ref)
    parent = self()
    data_dir = ctx.data_dir

    Process.put(:ferricstore_blob_store_verify_hook, fn ^data_dir, 0, ^ref ->
      send(parent, {:blob_verify, ref})
      :ok
    end)

    Process.put(:ferricstore_blob_store_open_read_hook, fn _path, _modes ->
      raise "unexpected full blob materialization"
    end)

    on_exit(fn ->
      Process.delete(:ferricstore_blob_store_verify_hook)
      Process.delete(:ferricstore_blob_store_open_read_hook)
    end)

    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    active_file_path = ShardETS.file_path(shard_path, 0)

    state =
      StateMachine.init(%{
        shard_index: 0,
        shard_data_path: shard_path,
        active_file_id: 0,
        active_file_path: active_file_path,
        ets: keydir,
        data_dir: ctx.data_dir,
        instance_ctx: ctx,
        instance_name: ctx.name
      })

    assert_state_machine_result(
      [:ok, :ok],
      StateMachine.apply(
        %{index: 1},
        {:put_blob_batch,
         [
           {first_key, encoded_ref, 0, :blob_ref},
           {second_key, encoded_ref, 0, :blob_ref}
         ]},
        state
      )
    )

    assert_received {:blob_verify, ^ref}
    refute_received {:blob_verify, _}
    Process.delete(:ferricstore_blob_store_open_read_hook)
    assert payload == Router.get(ctx, first_key)
    assert payload == Router.get(ctx, second_key)
  end

  test "Ra apply publishes the last same-key pre-externalized blob ref in batch", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "blob:auto:raft-ref-batch-duplicate"
    first_payload = :binary.copy("1", 1024)
    second_payload = :binary.copy("2", 1024)
    assert {:ok, first_ref} = BlobStore.put(ctx.data_dir, 0, first_payload)
    assert {:ok, second_ref} = BlobStore.put(ctx.data_dir, 0, second_payload)
    first_encoded = BlobRef.encode!(first_ref)
    second_encoded = BlobRef.encode!(second_ref)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    active_file_path = ShardETS.file_path(shard_path, 0)

    state =
      StateMachine.init(%{
        shard_index: 0,
        shard_data_path: shard_path,
        active_file_id: 0,
        active_file_path: active_file_path,
        ets: keydir,
        data_dir: ctx.data_dir,
        instance_ctx: ctx,
        instance_name: ctx.name
      })

    assert_state_machine_result(
      [:ok, :ok],
      StateMachine.apply(
        %{index: 1},
        {:put_blob_batch,
         [
           {key, first_encoded, 0, :blob_ref},
           {key, second_encoded, 0, :blob_ref}
         ]},
        state
      )
    )

    assert second_payload == Router.get(ctx, key)
    assert {:ok, ^second_encoded, ^second_ref} = raw_disk_blob_ref(ctx, keydir, key)
  end

  test "Ra apply rejects pre-externalized blob ref batch before partial writes", %{
    ctx: ctx,
    keydir: keydir
  } do
    small_key = "blob:auto:raft-ref-batch-reject-small"
    blob_key = "blob:auto:raft-ref-batch-reject-large"
    payload = :binary.copy("X", 1536)
    ref = BlobRef.from_payload(payload)
    encoded_ref = BlobRef.encode!(ref)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    active_file_path = ShardETS.file_path(shard_path, 0)

    state =
      StateMachine.init(%{
        shard_index: 0,
        shard_data_path: shard_path,
        active_file_id: 0,
        active_file_path: active_file_path,
        ets: keydir,
        data_dir: ctx.data_dir,
        instance_ctx: ctx,
        instance_name: ctx.name
      })

    assert_state_machine_result(
      {:error, {:blob_ref_unavailable, :enoent}},
      StateMachine.apply(
        %{index: 1},
        {:put_blob_batch,
         [
           {small_key, "small", 0, :value},
           {blob_key, encoded_ref, 0, :blob_ref}
         ]},
        state
      )
    )

    assert [] == :ets.lookup(keydir, small_key)
    assert [] == :ets.lookup(keydir, blob_key)
  end
    end
  end
end
