defmodule Ferricstore.Store.BlobSideChannelTest do
  use ExUnit.Case, async: false

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

  @router_source_path Path.expand("../../../lib/ferricstore/store/router.ex", __DIR__)

  setup do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 64,
        blob_side_channel_threshold_bytes: 128
      )

    original_threshold = Application.get_env(:ferricstore, :promotion_threshold)

    original_persistent_threshold =
      try do
        :persistent_term.get(:ferricstore_promotion_threshold)
      rescue
        ArgumentError -> :not_set
      end

    Application.put_env(:ferricstore, :promotion_threshold, 1)
    :persistent_term.put(:ferricstore_promotion_threshold, 1)

    on_exit(fn ->
      case original_threshold do
        nil -> Application.delete_env(:ferricstore, :promotion_threshold)
        value -> Application.put_env(:ferricstore, :promotion_threshold, value)
      end

      case original_persistent_threshold do
        :not_set -> :persistent_term.erase(:ferricstore_promotion_threshold)
        value -> :persistent_term.put(:ferricstore_promotion_threshold, value)
      end

      IsolatedInstance.checkin(ctx)
    end)

    %{ctx: ctx, shard: elem(ctx.shard_names, 0), keydir: elem(ctx.keydir_refs, 0)}
  end

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

  test "Ra generic batch accepts pre-externalized blob refs", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "blob:auto:raft-ref-generic-batch"
    payload = :binary.copy("G", 1536)
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
      {:ok, [:ok]},
      StateMachine.apply(%{index: 1}, {:batch, [{:put_blob_ref, key, encoded_ref, 0}]}, state)
    )

    assert payload == Router.get(ctx, key)
    assert {:ok, ^encoded_ref, ^ref} = raw_disk_blob_ref(ctx, keydir, key)
  end

  test "Ra generic batch RMW sees preceding pre-externalized blob ref", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "blob:auto:raft-ref-generic-rmw"
    payload = :binary.copy("R", 1536)
    suffix = "!"
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
      {:ok, [:ok, {:ok, byte_size(payload) + byte_size(suffix)}]},
      StateMachine.apply(
        %{index: 1},
        {:batch, [{:put_blob_ref, key, encoded_ref, 0}, {:append, key, suffix}]},
        state
      )
    )

    assert payload <> suffix == Router.get(ctx, key)
  end

  test "Ra apply returns an error and rolls back staged writes when blob persistence fails", %{
    ctx: ctx,
    keydir: keydir
  } do
    small_key = "blob:auto:raft-blob-fail-small"
    large_key = "blob:auto:raft-blob-fail-large"
    payload = :binary.copy("F", 1536)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    active_file_path = ShardETS.file_path(shard_path, 0)

    Process.put(:ferricstore_blob_store_fsync_dir_hook, fn _path -> {:error, :eio} end)

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
        {:error, {:blob_externalize_failed, :eio}},
        StateMachine.apply(
          %{index: 1},
          {:batch, [{:put, small_key, "small", 0}, {:put, large_key, payload, 0}]},
          state
        )
      )

      assert [] == :ets.lookup(keydir, small_key)
      assert [] == :ets.lookup(keydir, large_key)
    after
      Process.delete(:ferricstore_blob_store_fsync_dir_hook)
    end
  end

  test "Ra read-modify-write materializes blob refs before mutation", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "blob:auto:raft:append"
    payload = :binary.copy("A", 1536)
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

    assert_state_machine_result(
      byte_size(payload) + 1,
      StateMachine.apply(%{index: 2}, {:append, key, "!"}, state)
    )

    expected = payload <> "!"
    assert expected == Router.get(ctx, key)
    assert {:ok, _encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, key)
    assert {:ok, ^expected} = BlobStore.get(ctx.data_dir, 0, ref)
  end

  test "transaction-local GET materializes cold blob refs", %{
    ctx: ctx,
    shard: shard
  } do
    key = "blob:auto:local-tx-get"
    payload = :binary.copy("T", 1536)

    assert :ok = Router.put(ctx, key, payload, 0)
    tx = LocalTxStore.new(:sys.get_state(shard))

    assert payload == Ops.get(tx, key)
    assert {payload, 0} == Ops.get_meta(tx, key)
    assert [payload] == Ops.batch_get(tx, [key])
  end

  test "transaction-local RMW reads cold blob refs before mutation", %{
    ctx: ctx,
    shard: shard
  } do
    key = "blob:auto:local-tx-append"
    payload = :binary.copy("A", 1536)
    suffix = "!"

    assert :ok = Router.put(ctx, key, payload, 0)
    tx = LocalTxStore.new(:sys.get_state(shard))

    try do
      assert {:ok, byte_size(payload) + byte_size(suffix)} == Ops.append(tx, key, suffix)
      assert_receive {:tx_pending_write, ^key, written, 0}
      assert written == payload <> suffix
    after
      Process.delete(:tx_pending_values)
      Process.delete(:tx_deleted_keys)
    end
  end

  test "transaction-local value_size and GETRANGE use logical blob size", %{
    ctx: ctx,
    shard: shard
  } do
    key = "blob:auto:local-tx-getrange"
    payload = :binary.copy("A", 128) <> :binary.copy("B", 128)

    assert :ok = Router.put(ctx, key, payload, 0)
    tx = LocalTxStore.new(:sys.get_state(shard))

    assert byte_size(payload) == Ops.value_size(tx, key)
    assert binary_part(payload, 128, 8) == Ops.getrange(tx, key, 128, 135)

    assert [binary_part(payload, 128, 8)] ==
             GenServer.call(
               shard,
               {:tx_execute, [{"GETRANGE", [key, "128", "135"], {:getrange, key, 128, 135}}], nil}
             )
  end

  test "transaction-local promoted compound GET materializes cold blob refs", %{
    shard: shard
  } do
    redis_key = "blob:auto:local-tx-promoted-hash"
    field = CompoundKey.hash_field(redis_key, "large")
    field_b = CompoundKey.hash_field(redis_key, "small")
    payload = :binary.copy("P", 1536)

    assert :ok = GenServer.call(shard, {:compound_put, redis_key, field, payload, 0})
    assert :ok = GenServer.call(shard, {:compound_put, redis_key, field_b, "small", 0})

    tx = LocalTxStore.new(:sys.get_state(shard))

    assert payload == Ops.compound_get(tx, redis_key, field)
    assert [payload] == Ops.compound_batch_get(tx, redis_key, [field])
  end

  test "origin replay pending PUT persists large hot values as blob refs" do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 4096,
        blob_side_channel_threshold_bytes: 128
      )

    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    keydir = elem(ctx.keydir_refs, 0)
    key = "blob:auto:origin-replay"
    payload = :binary.copy("O", 1024)
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

    :ets.insert(keydir, {key, payload, 0, 1, :pending, 0, 0})

    assert_state_machine_result(
      :ok,
      StateMachine.apply(%{index: 1}, {:async, node(), {:put, key, payload, 0}}, state)
    )

    assert {:ok, _encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, key)
    assert {:ok, ^payload} = BlobStore.get(ctx.data_dir, 0, ref)
    assert payload == Router.get(ctx, key)
  end

  test "Flow-owned large payload values are persisted as blob refs", %{
    ctx: ctx,
    keydir: keydir
  } do
    id = "blob-flow-payload"
    partition_key = "tenant-blob"
    payload = :binary.copy("P", 1024)
    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
    payload_key = Ferricstore.Flow.Keys.value_key(id, :payload, 1, partition_key)
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
      StateMachine.apply(
        %{index: 1, system_time: 1_000},
        {:flow_create, state_key,
         %{
           id: id,
           type: "blob-flow",
           state: "queued",
           partition_key: partition_key,
           payload: payload,
           now_ms: 1_000
         }},
        state
      )
    )

    assert [] = :ets.lookup(keydir, payload_key)

    lmdb_path = Ferricstore.Flow.LMDB.path(shard_path)
    assert {:ok, lmdb_value} = Ferricstore.Flow.LMDB.get(lmdb_path, payload_key)
    assert {:ok, encoded_ref} = Ferricstore.Flow.LMDB.decode_value(lmdb_value, 1_000)
    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert {:ok, encoded_payload} = BlobStore.get(ctx.data_dir, 0, ref)
    assert Ferricstore.Flow.decode_value(encoded_payload) == payload
    assert {:ok, [^payload]} = Ferricstore.Flow.value_mget(ctx, [payload_key])
  end

  test "owner-scoped Flow named values store large values as live blob refs", %{ctx: ctx} do
    id = "blob-flow-named-value"
    partition_key = "tenant-blob-named-value"
    payload = :binary.copy("N", 512)

    Process.put(:ferricstore_blob_store_segment_gc_grace_ms, 0)

    try do
      assert :ok =
               Ferricstore.Flow.create(ctx, id,
                 type: "blob-flow-named-value",
                 partition_key: partition_key,
                 run_at_ms: 1,
                 now_ms: 1
               )

      assert {:ok, %{ref: value_ref}} =
               Ferricstore.Flow.value_put(ctx, payload,
                 partition_key: partition_key,
                 owner_flow_id: id,
                 name: "doc",
                 now_ms: 2
               )

      assert {:ok, [^payload]} = Ferricstore.Flow.value_mget(ctx, [value_ref])
      assert {:ok, %{deleted_files: 0}} = Router.sweep_blob_garbage(ctx)
      assert {:ok, [^payload]} = Ferricstore.Flow.value_mget(ctx, [value_ref])
    after
      Process.delete(:ferricstore_blob_store_segment_gc_grace_ms)
    end
  end

  test "active Flow state records with large metadata are persisted as blob refs", %{
    ctx: ctx,
    keydir: keydir
  } do
    id = "blob-flow-active-state"
    partition_key = "tenant-blob-active"
    correlation_id = :binary.copy("c", 256)
    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: "blob-active",
               partition_key: partition_key,
               correlation_id: correlation_id,
               run_at_ms: 1,
               now_ms: 1
             )

    assert {:ok, _encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, state_key)
    assert {:ok, encoded_state} = BlobStore.get(ctx.data_dir, 0, ref)

    assert %{id: ^id, state: "queued", correlation_id: ^correlation_id} =
             Ferricstore.Flow.decode_record(encoded_state)

    assert {:ok, %{id: ^id, state: "queued", correlation_id: ^correlation_id}} =
             Ferricstore.Flow.get(ctx, id, partition_key: partition_key)
  end

  test "Flow retention cleanup decodes terminal state stored as a blob ref", %{
    ctx: ctx,
    keydir: keydir
  } do
    Process.put(:ferricstore_blob_store_segment_gc_grace_ms, 0)

    on_exit(fn ->
      Process.delete(:ferricstore_blob_store_segment_gc_grace_ms)
    end)

    id = "blob-flow-retention"
    partition_key = "tenant-blob-retention"
    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: "blob-retention",
               partition_key: partition_key,
               correlation_id: :binary.copy("c", 256),
               retention_ttl_ms: 1,
               run_at_ms: 1,
               now_ms: 1
             )

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, "blob-retention",
               partition_key: partition_key,
               worker: "blob-worker",
               limit: 1,
               now_ms: 1
             )

    assert :ok =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               now_ms: 2
             )

    assert {:ok, _encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, state_key)
    assert {:ok, encoded_state} = BlobStore.get(ctx.data_dir, 0, ref)
    assert %{id: ^id, state: "completed"} = Ferricstore.Flow.decode_record(encoded_state)

    assert {:ok, %{deleted_files: 0}} = Router.sweep_blob_garbage(ctx)
    assert {:ok, _encoded_state} = BlobStore.get(ctx.data_dir, 0, ref)

    cleanup_now = System.system_time(:millisecond) + 10_000

    assert {:ok, cleaned} =
             Ferricstore.Flow.retention_cleanup(ctx, limit: 10, now_ms: cleanup_now)

    assert cleaned.flows >= 1
    assert {:ok, nil} = Ferricstore.Flow.get(ctx, id, partition_key: partition_key)
  end

  test "Flow LMDB rebuild decodes terminal state stored as a blob ref", %{
    ctx: ctx,
    shard: shard,
    keydir: keydir
  } do
    id = "blob-flow-lmdb-rebuild"
    partition_key = "tenant-blob-lmdb-rebuild"
    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
    flow_type = "blob-lmdb-rebuild"

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: flow_type,
               partition_key: partition_key,
               correlation_id: :binary.copy("c", 256),
               run_at_ms: 1,
               now_ms: 1
             )

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, flow_type,
               partition_key: partition_key,
               worker: "blob-lmdb-worker",
               limit: 1,
               now_ms: 1
             )

    assert :ok =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               now_ms: 2
             )

    assert {:ok, _encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, state_key)
    assert {:ok, encoded_state} = BlobStore.get(ctx.data_dir, 0, ref)
    assert %{id: ^id, state: "completed"} = Ferricstore.Flow.decode_record(encoded_state)

    state = :sys.get_state(shard)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    lmdb_path = Ferricstore.Flow.LMDB.path(shard_path)

    if pid = Process.whereis(Ferricstore.Flow.LMDBWriter.name(ctx.name, 0)) do
      GenServer.stop(pid, :normal, 5_000)
    end

    File.rm_rf!(lmdb_path)

    assert :ok =
             Ferricstore.Flow.LMDBRebuilder.reconcile_shard(
               shard_path,
               keydir,
               0,
               ctx,
               state.zset_score_index,
               state.zset_score_lookup,
               state.flow_index,
               state.flow_lookup
             )

    assert [] == :ets.lookup(keydir, state_key)
    assert {:ok, lmdb_blob} = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)
    assert {:ok, rebuilt_state} = Ferricstore.Flow.LMDB.decode_value(lmdb_blob, 10)
    assert %{id: ^id, state: "completed"} = Ferricstore.Flow.decode_record(rebuilt_state)
  end

  test "Ra compound batch apply persists large values as blob refs", %{
    ctx: ctx,
    keydir: keydir
  } do
    redis_key = "blob:auto:raft-hash"
    field = CompoundKey.hash_field(redis_key, "large")
    payload = :binary.copy("C", 1536)
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
      [:ok],
      StateMachine.apply(
        %{index: 1, system_time: 1_000},
        {:compound_batch_put, redis_key, [{field, payload, 0}]},
        state
      )
    )

    assert {:ok, _encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, field)
    assert {:ok, ^payload} = BlobStore.get(ctx.data_dir, 0, ref)
    assert payload == Router.compound_get(ctx, redis_key, field)
  end

  test "cross-shard SET apply persists large values as blob refs", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "blob:auto:cross-shard"
    payload = :binary.copy("X", 2048)
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
      %{0 => [:ok]},
      StateMachine.apply(
        %{index: 1, system_time: 1_000},
        {:cross_shard_tx, [{0, [{"SET", [key, payload]}], nil}]},
        state
      )
    )

    assert {:ok, _encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, key)
    assert {:ok, ^payload} = BlobStore.get(ctx.data_dir, 0, ref)

    assert_state_machine_result(
      %{0 => [payload]},
      StateMachine.apply(
        %{index: 2, system_time: 1_001},
        {:cross_shard_tx, [{0, [{"GET", [key]}], nil}]},
        state
      )
    )

    assert payload == Router.get(ctx, key)
  end

  test "cross-shard apply returns an error and rolls back when blob persistence fails", %{
    ctx: ctx,
    keydir: keydir
  } do
    small_key = "blob:auto:cross-shard-fail-small"
    large_key = "blob:auto:cross-shard-fail-large"
    payload = :binary.copy("X", 2048)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    active_file_path = ShardETS.file_path(shard_path, 0)

    Process.put(:ferricstore_blob_store_fsync_dir_hook, fn _path -> {:error, :eio} end)

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
        {:error, {:blob_externalize_failed, :eio}},
        StateMachine.apply(
          %{index: 1, system_time: 1_000},
          {:cross_shard_tx,
           [{0, [{"SET", [small_key, "small"]}, {"SET", [large_key, payload]}], nil}]},
          state
        )
      )

      assert [] == :ets.lookup(keydir, small_key)
      assert [] == :ets.lookup(keydir, large_key)
    after
      Process.delete(:ferricstore_blob_store_fsync_dir_hook)
    end
  end

  test "shared compound cold batch reads materialize blob refs", %{
    ctx: ctx,
    keydir: keydir
  } do
    redis_key = "blob:auto:hash"
    field_a = CompoundKey.hash_field(redis_key, "a")
    field_b = CompoundKey.hash_field(redis_key, "b")
    payload_a = :binary.copy("A", 1024)
    payload_b = :binary.copy("B", 1536)

    assert :ok = Router.compound_put(ctx, redis_key, field_a, payload_a, 0)
    assert :ok = Router.compound_put(ctx, redis_key, field_b, payload_b, 0)

    assert {:ok, _encoded_ref_a, ref_a} = raw_disk_blob_ref(ctx, keydir, field_a)
    assert {:ok, _encoded_ref_b, ref_b} = raw_disk_blob_ref(ctx, keydir, field_b)
    assert {:ok, ^payload_a} = BlobStore.get(ctx.data_dir, 0, ref_a)
    assert {:ok, ^payload_b} = BlobStore.get(ctx.data_dir, 0, ref_b)

    assert payload_a == Router.compound_get(ctx, redis_key, field_a)
    assert [payload_a, payload_b] == Router.compound_batch_get(ctx, redis_key, [field_a, field_b])

    assert [{^payload_a, 0}, {^payload_b, 0}] =
             Router.compound_batch_get_meta(ctx, redis_key, [field_a, field_b])
  end

  test "direct native list writes persist large elements as blob refs", %{
    ctx: ctx,
    shard: shard,
    keydir: keydir
  } do
    key = "blob:auto:list"
    element_key = CompoundKey.list_element(key, 0)
    payload = :binary.copy("L", 2048)

    assert 1 == GenServer.call(shard, {:list_op, key, {:rpush, [payload]}})

    assert {:ok, _encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, element_key)
    assert {:ok, ^payload} = BlobStore.get(ctx.data_dir, 0, ref)
    assert [payload] == GenServer.call(shard, {:list_op, key, {:lrange, 0, -1}})
  end

  test "shared Bitcask compaction rewrites only blob refs and leaves blob bytes untouched", %{
    ctx: ctx,
    shard: shard
  } do
    payload = :binary.copy("A", 1024)
    assert {:ok, ref} = BlobStore.put(ctx.data_dir, 0, payload)
    blob_path = BlobRef.path(ctx.data_dir, 0, ref)
    blob_ref = BlobRef.encode!(ref)

    assert :ok = GenServer.call(shard, {:put, "blob:shared", blob_ref, 0})
    assert :ok = GenServer.call(shard, {:put, "blob:dead", "dead", 0})
    assert :ok = GenServer.call(shard, :flush)
    assert :ok = GenServer.call(shard, {:delete, "blob:dead"})

    force_rotate_active_file(shard)

    assert {:ok, {_written, _dropped, _reclaimed}} =
             GenServer.call(shard, {:run_compaction, [0]})

    assert blob_ref == GenServer.call(shard, {:get, "blob:shared"})
    assert File.exists?(blob_path)
    assert {:ok, ^payload} = BlobStore.get(ctx.data_dir, 0, ref)
  end

  test "promoted dedicated compaction rewrites blob refs without owning blob bytes", %{
    ctx: ctx,
    shard: shard
  } do
    first_payload = :binary.copy("B", 2048)
    second_payload = :binary.copy("C", 4096)

    assert {:ok, first_blob_ref} = BlobStore.put(ctx.data_dir, 0, first_payload)
    assert {:ok, second_blob_ref} = BlobStore.put(ctx.data_dir, 0, second_payload)

    blob_path = BlobRef.path(ctx.data_dir, 0, first_blob_ref)
    first_ref = BlobRef.encode!(first_blob_ref)
    second_ref = BlobRef.encode!(second_blob_ref)

    redis_key = "blob_hash"
    field_a = CompoundKey.hash_field(redis_key, "a")
    field_b = CompoundKey.hash_field(redis_key, "b")

    assert :ok = GenServer.call(shard, {:compound_put, redis_key, field_a, first_ref, 0})
    assert :ok = GenServer.call(shard, {:compound_put, redis_key, field_b, second_ref, 0})

    state = :sys.get_state(shard)
    dedicated_path = state.promoted_instances[redis_key].path

    refute String.starts_with?(
             dedicated_path,
             Ferricstore.DataDir.blob_shard_path(ctx.data_dir, 0)
           )

    :sys.replace_state(shard, fn state ->
      ShardCompound.compact_dedicated(state, redis_key, dedicated_path)
    end)

    assert first_ref == GenServer.call(shard, {:compound_get, redis_key, field_a})
    assert second_ref == GenServer.call(shard, {:compound_get, redis_key, field_b})
    assert File.exists?(blob_path)
    assert {:ok, ^first_payload} = BlobStore.get(ctx.data_dir, 0, first_blob_ref)
    assert {:ok, ^second_payload} = BlobStore.get(ctx.data_dir, 0, second_blob_ref)
  end

  test "promoted compound batch put externalizes large values with one blob segment fsync", %{
    shard: shard
  } do
    redis_key = "blob:promoted:batch-put"
    seed_a = CompoundKey.hash_field(redis_key, "seed-a")
    seed_b = CompoundKey.hash_field(redis_key, "seed-b")

    assert :ok = GenServer.call(shard, {:compound_put, redis_key, seed_a, "small-a", 0})
    assert :ok = GenServer.call(shard, {:compound_put, redis_key, seed_b, "small-b", 0})

    state = :sys.get_state(shard)
    assert Map.has_key?(state.promoted_instances, redis_key)

    parent = self()

    Process.put(:ferricstore_blob_store_fsync_file_hook, fn path ->
      send(parent, {:blob_fsync_file, path})
      Ferricstore.Bitcask.NIF.v2_fsync(path)
    end)

    on_exit(fn ->
      Process.delete(:ferricstore_blob_store_fsync_file_hook)
    end)

    field_a = CompoundKey.hash_field(redis_key, "large-a")
    field_b = CompoundKey.hash_field(redis_key, "large-b")
    payload_a = :binary.copy("A", 1024)
    payload_b = :binary.copy("B", 1024)

    assert {:reply, :ok, _new_state} =
             ShardCompound.handle_compound_batch_put(
               redis_key,
               [{field_a, payload_a, 0}, {field_b, payload_b, 0}],
               state
             )

    assert payload_a == GenServer.call(shard, {:compound_get, redis_key, field_a})
    assert payload_b == GenServer.call(shard, {:compound_get, redis_key, field_b})

    assert_receive {:blob_fsync_file, first_path}, 1000
    refute_receive {:blob_fsync_file, _second_path}, 100
    assert String.ends_with?(first_path, ".bloblog")
  end

  test "blob garbage sweep removes deleted direct blobs and preserves promoted live refs", %{
    ctx: ctx,
    shard: shard,
    keydir: keydir
  } do
    attach_blob_gc_handler()

    live_key = "blob:gc:live"
    dead_key = "blob:gc:dead"
    redis_key = "blob_gc_hash"
    field = CompoundKey.hash_field(redis_key, "field")
    field_b = CompoundKey.hash_field(redis_key, "field-b")

    assert :ok = Router.put(ctx, live_key, :binary.copy("L", 1024), 0)
    assert :ok = Router.put(ctx, dead_key, :binary.copy("D", 1024), 0)

    assert :ok =
             GenServer.call(shard, {:compound_put, redis_key, field, :binary.copy("P", 1024), 0})

    assert :ok = GenServer.call(shard, {:compound_put, redis_key, field_b, "small", 0})

    assert {:ok, _live_encoded, live_ref} = raw_disk_blob_ref(ctx, keydir, live_key)
    assert {:ok, _dead_encoded, dead_ref} = raw_disk_blob_ref(ctx, keydir, dead_key)

    assert {:ok, _promoted_encoded, promoted_ref} =
             promoted_disk_blob_ref(shard, keydir, redis_key, field)

    live_path = BlobRef.path(ctx.data_dir, 0, live_ref)
    dead_path = BlobRef.path(ctx.data_dir, 0, dead_ref)
    promoted_path = BlobRef.path(ctx.data_dir, 0, promoted_ref)

    assert :ok = GenServer.call(shard, {:delete, dead_key})

    assert {:ok, %{deleted_files: 0} = stats} = Router.sweep_blob_garbage(ctx)

    assert_receive {:blob_gc, [:ferricstore, :blob, :gc], measurements,
                    %{result: :ok, shard_count: 1}}

    assert measurements.deleted_files == stats.deleted_files
    assert measurements.deleted_bytes == stats.deleted_bytes
    assert measurements.kept_files == stats.kept_files

    assert File.exists?(live_path)
    assert File.exists?(dead_path)
    assert File.exists?(promoted_path)
    assert :binary.copy("L", 1024) == Router.get(ctx, live_key)
    assert :binary.copy("P", 1024) == GenServer.call(shard, {:compound_get, redis_key, field})
  end

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

  test "blob garbage sweep preserves live refs stored behind WARaft segment locations" do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 0,
        blob_side_channel_threshold_bytes: 128
      )

    key = "blob:gc:waraft-segment-live"
    payload = :binary.copy("W", 1536)

    try do
      Process.put(:ferricstore_blob_store_segment_gc_grace_ms, 0)

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

  test "blob garbage sweep skips deletion while Ra replay cursor still covers possible blob refs",
       %{ctx: ctx, shard: shard} do
    payload = "dead-but-possibly-still-in-raft-log"
    ref = BlobRef.from_payload(payload)
    path = write_legacy_blob!(ctx.data_dir, 0, ref, payload)

    :sys.replace_state(shard, fn state -> %{state | raft?: true} end)
    :atomics.put(ctx.last_applied_index, 1, 10)
    :atomics.put(ctx.last_released_cursor_index, 1, 9)
    state = :sys.get_state(shard)
    assert state.raft?
    assert :atomics.get(state.instance_ctx.last_applied_index, 1) == 10
    assert :atomics.get(state.instance_ctx.last_released_cursor_index, 1) == 9

    assert {:ok,
            %{
              deleted_files: 0,
              deleted_bytes: 0,
              kept_files: 0,
              skipped: true,
              reason: {:raft_replay_gap, 10, 9}
            }} = Router.sweep_blob_garbage(ctx)

    assert File.exists?(path)
  end

  test "blob garbage sweep skips deletion while WARaft replay cursor still covers possible blob refs",
       %{ctx: ctx, shard: shard} do
    payload = "dead-but-possibly-still-in-waraft-log"
    ref = BlobRef.from_payload(payload)
    path = write_legacy_blob!(ctx.data_dir, 0, ref, payload)

    :atomics.put(ctx.last_applied_index, 1, 10)
    :atomics.put(ctx.last_released_cursor_index, 1, 9)
    state = :sys.get_state(shard)
    refute state.raft?
    assert :atomics.get(state.instance_ctx.last_applied_index, 1) == 10
    assert :atomics.get(state.instance_ctx.last_released_cursor_index, 1) == 9

    assert {:ok,
            %{
              deleted_files: 0,
              deleted_bytes: 0,
              kept_files: 0,
              skipped: true,
              reason: {:raft_replay_gap, 10, 9}
            }} = Router.sweep_blob_garbage(ctx)

    assert File.exists?(path)
  end

  test "blob garbage sweep continues safe shards when another shard is replay-unsafe" do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 2,
        hot_cache_max_value_size: 64,
        blob_side_channel_threshold_bytes: 128
      )

    try do
      safe_payload = "safe-shard-orphan-blob"
      unsafe_payload = "unsafe-shard-orphan-blob"
      safe_ref = BlobRef.from_payload(safe_payload)
      unsafe_ref = BlobRef.from_payload(unsafe_payload)
      safe_path = write_legacy_blob!(ctx.data_dir, 0, safe_ref, safe_payload)
      unsafe_path = write_legacy_blob!(ctx.data_dir, 1, unsafe_ref, unsafe_payload)

      :atomics.put(ctx.last_applied_index, 2, 10)
      :atomics.put(ctx.last_released_cursor_index, 2, 9)

      assert {:ok, %{deleted_files: 1, skipped: true, reason: {:raft_replay_gap, 10, 9}}} =
               Router.sweep_blob_garbage(ctx)

      refute File.exists?(safe_path)
      assert File.exists?(unsafe_path)
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  test "blob garbage sweep fails closed when Ra replay metrics are unavailable", %{
    ctx: ctx,
    shard: shard
  } do
    payload = "dead-but-unknown-raft-replay-gap"
    ref = BlobRef.from_payload(payload)
    path = write_legacy_blob!(ctx.data_dir, 0, ref, payload)
    original_state = :sys.get_state(shard)

    :sys.replace_state(shard, fn state -> %{state | raft?: true, instance_ctx: nil} end)

    try do
      assert {:ok,
              %{
                deleted_files: 0,
                deleted_bytes: 0,
                kept_files: 0,
                skipped: true,
                reason: :missing_raft_replay_metrics
              }} = Router.sweep_blob_garbage(ctx)

      assert File.exists?(path)
    after
      :sys.replace_state(shard, fn _state -> original_state end)
    end
  end

  test "blob garbage sweep fails closed when Ra replay metrics are invalid", %{
    ctx: ctx,
    shard: shard
  } do
    payload = "dead-but-invalid-raft-replay-metrics"
    ref = BlobRef.from_payload(payload)
    path = write_legacy_blob!(ctx.data_dir, 0, ref, payload)
    original_state = :sys.get_state(shard)

    :sys.replace_state(shard, fn state ->
      %{
        state
        | raft?: true,
          instance_ctx: %{
            state.instance_ctx
            | last_applied_index: {:not, :atomics},
              last_released_cursor_index: {:not, :atomics}
          }
      }
    end)

    try do
      assert {:ok,
              %{
                deleted_files: 0,
                deleted_bytes: 0,
                kept_files: 0,
                skipped: true,
                reason: :missing_raft_replay_metrics
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
    ref = BlobRef.from_payload(payload)
    path = write_legacy_blob!(ctx.data_dir, 0, ref, payload)
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
              legacy_files: 0,
              legacy_bytes: 0,
              segment_files: 1,
              segment_bytes: segment_bytes,
              tmp_files: 0,
              tmp_bytes: 0
            }} = BlobStore.storage_stats(ctx.data_dir)

    assert bytes >= 2048
    assert segment_bytes == bytes
  end

  defp force_rotate_active_file(shard) do
    :sys.replace_state(shard, fn state ->
      new_id = state.active_file_id + 1
      shard_path = state.shard_data_path
      new_path = Ferricstore.Store.Shard.ETS.file_path(shard_path, new_id)

      Ferricstore.FS.touch!(new_path)

      Ferricstore.Store.ActiveFile.publish(
        state.instance_ctx,
        state.index,
        new_id,
        new_path,
        shard_path
      )

      %{
        state
        | active_file_id: new_id,
          active_file_path: new_path,
          active_file_size: 0,
          file_stats: Map.put(state.file_stats, new_id, {0, 0})
      }
    end)
  end

  defp raw_disk_blob_ref(ctx, keydir, key) do
    with {:ok, value} <- raw_disk_value(ctx, keydir, key),
         {:ok, ref} <- BlobRef.decode(value) do
      {:ok, value, ref}
    else
      other -> other
    end
  end

  defp raw_disk_value(ctx, keydir, key) do
    case :ets.lookup(keydir, key) do
      [{^key, nil, _exp, _lfu, fid, off, _vsize}] when is_integer(fid) and fid >= 0 ->
        path = ShardETS.file_path(Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0), fid)
        ColdRead.pread_at(path, off, key, 5_000)

      [{^key, value, _exp, _lfu, fid, off, _vsize}] when is_integer(fid) and fid >= 0 ->
        path = ShardETS.file_path(Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0), fid)

        case ColdRead.pread_at(path, off, key, 5_000) do
          {:ok, disk_value} -> {:ok, disk_value}
          _ -> {:ok, value}
        end

      other ->
        {:error, {:unexpected_keydir_entry, other}}
    end
  end

  defp promoted_disk_blob_ref(shard, keydir, redis_key, compound_key) do
    state = :sys.get_state(shard)
    dedicated_path = state.promoted_instances[redis_key].path

    with [{^compound_key, _value, _exp, _lfu, fid, off, _vsize}] <-
           :ets.lookup(keydir, compound_key),
         path <- ShardCompound.dedicated_file_path(dedicated_path, fid),
         {:ok, value} <- ColdRead.pread_at(path, off, compound_key, 5_000),
         {:ok, ref} <- BlobRef.decode(value) do
      {:ok, value, ref}
    else
      other -> {:error, {:unexpected_promoted_blob_ref, other}}
    end
  end

  defp assert_state_machine_result(expected, result)
       when is_tuple(result) and tuple_size(result) >= 2 do
    case elem(result, 1) do
      ^expected -> :ok
      {:ok, ^expected} -> :ok
      {:applied_at, _index, ^expected} -> :ok
      {:applied_at, _index, {:ok, ^expected}} -> :ok
      other -> flunk("unexpected state machine result #{inspect(other)}")
    end
  end

  defp attach_blob_gc_handler do
    handler_id = {__MODULE__, self(), make_ref()}
    parent = self()

    :telemetry.attach_many(
      handler_id,
      [[:ferricstore, :blob, :gc], [:ferricstore, :blob, :gc, :failed]],
      fn
        [:ferricstore, :blob, :gc] = event, measurements, metadata, _config ->
          send(parent, {:blob_gc, event, measurements, metadata})

        [:ferricstore, :blob, :gc, :failed] = event, measurements, metadata, _config ->
          send(parent, {:blob_gc_failed, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp write_legacy_blob!(data_dir, shard_index, %BlobRef{} = ref, payload) do
    path = BlobRef.path(data_dir, shard_index, ref)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, payload)
    path
  end

  defp overwrite_segment_payload!(data_dir, shard_index, ref, payload) do
    assert {:ok, {path, offset, size}} = BlobStore.file_ref(data_dir, shard_index, ref)
    assert byte_size(payload) == size

    {:ok, io} = File.open(path, [:read, :write, :raw, :binary])

    try do
      assert :ok = :file.pwrite(io, offset, payload)
    after
      :file.close(io)
    end
  end

  defp corrupt_segment_header!(data_dir, shard_index, ref) do
    assert {:ok, {path, offset, _size}} = BlobStore.file_ref(data_dir, shard_index, ref)
    header_offset = offset - 48
    assert header_offset >= 0

    {:ok, io} = File.open(path, [:read, :write, :raw, :binary])

    try do
      assert :ok = :file.pwrite(io, header_offset, :binary.copy(<<0>>, 48))
    after
      :file.close(io)
    end
  end
end
