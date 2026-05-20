defmodule Ferricstore.Raft.WARaftBackendTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Ferricstore.Raft.Cluster, as: RaftCluster
  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Store.BlobRef
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Router

  defmodule LabelCounter do
    @moduledoc false

    def new_label(nil, _command), do: 1
    def new_label(:undefined, _command), do: 1
    def new_label(label, _command) when is_integer(label), do: label + 1
  end

  defmodule OversizedLabel do
    @moduledoc false

    def new_label(_label, _command), do: :binary.copy("x", 1_048_576)
  end

  def handle_test_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_storage_blocked, event, measurements, metadata})
  end

  def handle_segment_log_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_segment_log_telemetry, event, measurements, metadata})
  end

  def handle_namespace_batcher_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_namespace_batcher_flush, event, measurements, metadata})
  end

  def handle_payload_fsync_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_payload_fsync_telemetry, event, measurements, metadata})
  end

  def handle_blob_prepare_failed_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_blob_prepare_failed, event, measurements, metadata})
  end

  def handle_store_unavailable_telemetry(event, measurements, metadata, parent) do
    send(parent, {:store_unavailable, event, measurements, metadata})
  end

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-backend-#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(root)
    Ferricstore.DataDir.ensure_layout!(root, 1)
    Ferricstore.Store.ActiveFile.init(1)

    ctx = build_ctx(root)

    on_exit(fn ->
      WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)
      File.rm_rf!(root)
    end)

    %{root: root, ctx: ctx}
  end

  test "committed SET and DEL use the real FerricStore state machine", %{ctx: ctx} do
    assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

    assert :ok = WARaftBackend.write(0, {:put, "wk1", "wv1", 0})
    assert "wv1" == Router.get(ctx, "wk1")
    assert [{_, "wv1", 0, _lfu, _fid, _off, 3}] = :ets.lookup(elem(ctx.keydir_refs, 0), "wk1")

    assert :ok = WARaftBackend.write(0, {:delete, "wk1"})
    assert nil == Router.get(ctx, "wk1")
    assert [] == :ets.lookup(elem(ctx.keydir_refs, 0), "wk1")
  end

  test "segment-projected storage uses the WARaft segment as the simple KV durability source",
       %{root: root, ctx: ctx} do
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_projected)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert :ok = WARaftBackend.write(0, {:put, "unified-segment:k1", "v1", 0})
      assert :ok = WARaftBackend.write(0, {:put, "unified-segment:gone", "old", 0})
      assert :ok = WARaftBackend.write(0, {:delete, "unified-segment:gone"})

      assert "v1" == Router.get(ctx, "unified-segment:k1")
      assert nil == Router.get(ctx, "unified-segment:gone")

      bitcask_file = Path.join([root, "data", "shard_0", "00000.log"])
      assert File.stat!(bitcask_file).size == 0

      assert :ok = WARaftBackend.stop()

      restarted_ctx = build_ctx(root)

      try do
        assert :ok =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert "v1" == Router.get(restarted_ctx, "unified-segment:k1")
        assert nil == Router.get(restarted_ctx, "unified-segment:gone")
        assert File.stat!(bitcask_file).size == 0
      after
        FerricStore.Instance.cleanup(restarted_ctx.name)
      end
    after
      restore_env(:waraft_storage_apply_mode, previous_mode)
    end
  end

  test "segment-projected cold ETS entries read back from the WARaft segment", %{ctx: ctx} do
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_projected)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      key = "unified-segment-cold:k1"
      assert :ok = WARaftBackend.write(0, {:put, key, "v1", 0})

      force_segment_projected_key_cold!(ctx, key)

      assert "v1" == Router.get(ctx, key)
      assert {"v1", 0} == Router.get_meta(ctx, key)
      assert 2 == Router.value_size(ctx, key)
      assert 0 == Router.expire_at_ms(ctx, key)
    after
      restore_env(:waraft_storage_apply_mode, previous_mode)
    end
  end

  test "segment-projected compound batch stores hash records in the WARaft segment", %{
    root: root,
    ctx: ctx
  } do
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)
    previous_backend = Application.get_env(:ferricstore, :raft_backend)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_projected)
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      redis_key = key_for_shard(ctx, 0)
      marker_key = CompoundKey.type_key(redis_key)
      field_key = CompoundKey.hash_field(redis_key, "field")

      assert {:ok, [:ok, :ok]} =
               WARaftBackend.write(0, {
                 :compound_batch_put,
                 redis_key,
                 [{marker_key, "hash", 0}, {field_key, "value", 0}]
               })

      assert "value" == Router.compound_get(ctx, redis_key, field_key)

      assert [
               {^field_key, "value", 0, _lfu, {:waraft_segment, index}, 0, 5}
             ] = :ets.lookup(elem(ctx.keydir_refs, 0), field_key)

      assert is_integer(index) and index > 0

      force_segment_projected_key_cold!(ctx, field_key)
      assert "value" == Router.compound_get(ctx, redis_key, field_key)

      force_segment_projected_key_cold!(ctx, field_key)
      prefix = CompoundKey.hash_prefix(redis_key)
      assert [{"field", "value"}] == Router.compound_scan(ctx, redis_key, prefix)
      ensure_segment_projected_key_cold!(ctx, field_key)
      assert ["field"] == Router.compound_fields(ctx, redis_key, prefix)
      ensure_segment_projected_key_cold!(ctx, field_key)
      assert 1 == Router.compound_count(ctx, redis_key, prefix)

      ensure_segment_projected_key_cold!(ctx, field_key)
      assert {"value", 0} == Router.compound_get_meta(ctx, redis_key, field_key)

      ensure_segment_projected_key_cold!(ctx, field_key)
      assert [{"value", 0}] == Router.compound_batch_get_meta(ctx, redis_key, [field_key])

      bitcask_file = Path.join([root, "data", "shard_0", "00000.log"])
      assert File.stat!(bitcask_file).size == 0

      assert :ok = WARaftBackend.stop()

      restarted_ctx = build_ctx(root)

      try do
        assert :ok =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert "hash" == Router.compound_get(restarted_ctx, redis_key, marker_key)
        assert "value" == Router.compound_get(restarted_ctx, redis_key, field_key)
        assert File.stat!(bitcask_file).size == 0
      after
        FerricStore.Instance.cleanup(restarted_ctx.name)
      end
    after
      restore_backend(previous_backend)
      restore_env(:waraft_storage_apply_mode, previous_mode)
    end
  end

  test "segment-projected compound batch stores blob-backed hash records in the WARaft segment",
       %{
         root: root,
         ctx: ctx
       } do
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)
    previous_backend = Application.get_env(:ferricstore, :raft_backend)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_projected)
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      redis_key = key_for_shard(ctx, 0)
      marker_key = CompoundKey.type_key(redis_key)
      field_key = CompoundKey.hash_field(redis_key, "large")
      value = :binary.copy("large-hash-field", 20_000)

      assert byte_size(value) > ctx.blob_side_channel_threshold_bytes

      assert {:ok, [:ok, :ok]} =
               WARaftBackend.write(0, {
                 :compound_batch_put,
                 redis_key,
                 [{marker_key, "hash", 0}, {field_key, value, 0}]
               })

      assert value == Router.compound_get(ctx, redis_key, field_key)

      bitcask_file = Path.join([root, "data", "shard_0", "00000.log"])
      assert File.stat!(bitcask_file).size == 0

      assert [{^field_key, nil, 0, _lfu, {:waraft_segment, index}, 0, value_size}] =
               :ets.lookup(elem(ctx.keydir_refs, 0), field_key)

      assert is_integer(index) and index > 0
      assert BlobRef.encoded_size?(value_size)

      assert :ok = WARaftBackend.stop()

      restarted_ctx = build_ctx(root)

      try do
        assert :ok =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert "hash" == Router.compound_get(restarted_ctx, redis_key, marker_key)
        assert value == Router.compound_get(restarted_ctx, redis_key, field_key)
        assert File.stat!(bitcask_file).size == 0
      after
        FerricStore.Instance.cleanup(restarted_ctx.name)
      end
    after
      restore_backend(previous_backend)
      restore_env(:waraft_storage_apply_mode, previous_mode)
    end
  end

  test "segment-projected compound delete commands remove hash records without Bitcask writes", %{
    root: root,
    ctx: ctx
  } do
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)
    previous_backend = Application.get_env(:ferricstore, :raft_backend)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_projected)
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      redis_key = key_for_shard(ctx, 0)
      marker_key = CompoundKey.type_key(redis_key)
      field_a = CompoundKey.hash_field(redis_key, "a")
      field_b = CompoundKey.hash_field(redis_key, "b")

      assert {:ok, [:ok, :ok, :ok]} =
               WARaftBackend.write(0, {
                 :compound_batch_put,
                 redis_key,
                 [{marker_key, "hash", 0}, {field_a, "va", 0}, {field_b, "vb", 0}]
               })

      assert {:ok, [:ok]} =
               WARaftBackend.write(0, {:compound_batch_delete, redis_key, [field_a]})

      assert nil == Router.compound_get(ctx, redis_key, field_a)
      assert "vb" == Router.compound_get(ctx, redis_key, field_b)

      assert :ok =
               WARaftBackend.write(
                 0,
                 {:compound_delete_prefix, CompoundKey.hash_prefix(redis_key)}
               )

      assert "hash" == Router.compound_get(ctx, redis_key, marker_key)
      assert nil == Router.compound_get(ctx, redis_key, field_b)

      bitcask_file = Path.join([root, "data", "shard_0", "00000.log"])
      assert File.stat!(bitcask_file).size == 0
    after
      restore_backend(previous_backend)
      restore_env(:waraft_storage_apply_mode, previous_mode)
    end
  end

  test "segment-projected string put overwrites compound records without dedicated files", %{
    root: root,
    ctx: ctx
  } do
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)
    previous_backend = Application.get_env(:ferricstore, :raft_backend)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_projected)
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      redis_key = key_for_shard(ctx, 0)
      marker_key = CompoundKey.type_key(redis_key)
      field_key = CompoundKey.hash_field(redis_key, "field")

      assert {:ok, [:ok, :ok]} =
               WARaftBackend.write(0, {
                 :compound_batch_put,
                 redis_key,
                 [{marker_key, "hash", 0}, {field_key, "value", 0}]
               })

      force_segment_projected_key_cold!(ctx, marker_key)
      assert :ok = WARaftBackend.write(0, {:put, redis_key, "string-value", 0})

      assert "string-value" == Router.get(ctx, redis_key)
      assert nil == Router.compound_get(ctx, redis_key, marker_key)
      assert nil == Router.compound_get(ctx, redis_key, field_key)
      assert [] == Router.compound_scan(ctx, redis_key, CompoundKey.hash_prefix(redis_key))

      bitcask_file = Path.join([root, "data", "shard_0", "00000.log"])
      assert File.stat!(bitcask_file).size == 0
      refute File.exists?(Path.join([root, "dedicated", "shard_0", "00000.log"]))
    after
      restore_backend(previous_backend)
      restore_env(:waraft_storage_apply_mode, previous_mode)
    end
  end

  test "segment-projected cold reads use bounded segment point lookup", %{root: root, ctx: ctx} do
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)
    previous_records = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_projected)
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      for n <- 0..5 do
        assert :ok = WARaftBackend.write(0, {:put, "segment-point:k#{n}", "v#{n}", 0})
      end

      key = "segment-point:k4"
      force_segment_projected_key_cold!(ctx, key)

      [{^key, nil, _expire_at_ms, _lfu, {:waraft_segment, index}, _offset, _value_size}] =
        :ets.lookup(elem(ctx.keydir_refs, 0), key)

      true = :ets.delete(:raft_log_ferricstore_waraft_backend_1, index)

      target_ordinal = div(index, 2)
      corrupt_ordinal = if target_ordinal == 0, do: 1, else: 0

      corrupt_path = Path.join(waraft_segment_log_dir(root, 0), "#{corrupt_ordinal}.seg")
      assert File.exists?(corrupt_path)
      File.write!(corrupt_path, "corrupt unrelated segment")

      assert "v4" == Router.get(ctx, key)
    after
      WARaftBackend.stop()
      restore_env(:waraft_storage_apply_mode, previous_mode)
      restore_env(:waraft_segment_log_records_per_segment, previous_records)
    end
  end

  test "segment-projected cold entries survive WARaft log trim", %{ctx: ctx} do
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)
    previous_records = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_projected)
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      for n <- 0..5 do
        assert :ok = WARaftBackend.write(0, {:put, "segment-trim:k#{n}", "v#{n}", 0})
      end

      key = "segment-trim:k4"
      force_segment_projected_key_cold!(ctx, key)

      [{^key, nil, _expire_at_ms, _lfu, {:waraft_segment, index}, _offset, _value_size}] =
        :ets.lookup(elem(ctx.keydir_refs, 0), key)

      log = waraft_segment_log_record(0)
      assert {:ok, _state} = :ferricstore_waraft_spike_segment_log.trim(log, index + 1, %{})

      assert "v4" == Router.get(ctx, key)
    after
      WARaftBackend.stop()
      restore_env(:waraft_storage_apply_mode, previous_mode)
      restore_env(:waraft_segment_log_records_per_segment, previous_records)
    end
  end

  test "segment-projected compound cold entries survive WARaft log trim", %{
    root: root,
    ctx: ctx
  } do
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    previous_records = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_projected)
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      redis_key = key_for_shard(ctx, 0)
      marker_key = CompoundKey.type_key(redis_key)
      field_key = CompoundKey.hash_field(redis_key, "trimmed")

      assert {:ok, [:ok, :ok]} =
               WARaftBackend.write(0, {
                 :compound_batch_put,
                 redis_key,
                 [{marker_key, "hash", 0}, {field_key, "value", 0}]
               })

      assert :ok = WARaftBackend.write(0, {:put, "compound-trim:tail:1", "v1", 0})
      assert :ok = WARaftBackend.write(0, {:put, "compound-trim:tail:2", "v2", 0})

      force_segment_projected_key_cold!(ctx, field_key)

      [{^field_key, nil, _expire_at_ms, _lfu, {:waraft_segment, index}, _offset, _value_size}] =
        :ets.lookup(elem(ctx.keydir_refs, 0), field_key)

      log = waraft_segment_log_record(0)
      assert {:ok, _state} = :ferricstore_waraft_spike_segment_log.trim(log, index + 1, %{})

      assert "value" == Router.compound_get(ctx, redis_key, field_key)

      bitcask_file = Path.join([root, "data", "shard_0", "00000.log"])
      assert File.stat!(bitcask_file).size == 0
    after
      WARaftBackend.stop()
      restore_backend(previous_backend)
      restore_env(:waraft_storage_apply_mode, previous_mode)
      restore_env(:waraft_segment_log_records_per_segment, previous_records)
    end
  end

  test "segment-projected cold entries work through batch, file-ref, and range reads", %{
    ctx: ctx
  } do
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_projected)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      small_key = "unified-segment-read-paths:small"
      large_key = "unified-segment-read-paths:large"
      large_value = :binary.copy("0123456789abcdef", 6_000)

      assert byte_size(large_value) > ctx.hot_cache_max_value_size
      assert byte_size(large_value) < ctx.blob_side_channel_threshold_bytes

      assert {:ok, [:ok, :ok]} =
               WARaftBackend.write_put_batch(0, [
                 {small_key, "small-value", 0},
                 {large_key, large_value, 0}
               ])

      ensure_segment_projected_key_cold!(ctx, small_key)
      ensure_segment_projected_key_cold!(ctx, large_key)

      assert ["small-value", large_value, nil] ==
               Router.batch_get(ctx, [small_key, large_key, "unified-segment-read-paths:miss"])

      ensure_segment_projected_key_cold!(ctx, small_key)

      assert ["small-value", large_value] ==
               Router.batch_get_with_file_refs(ctx, [small_key, large_key], 1)

      ensure_segment_projected_key_cold!(ctx, small_key)

      assert {:cold_value, "small-value"} ==
               Router.get_with_deferred_blob_file_ref(ctx, small_key)

      assert binary_part(large_value, 16, 32) == Router.getrange(ctx, large_key, 16, 47)
    after
      restore_env(:waraft_storage_apply_mode, previous_mode)
    end
  end

  test "segment reader uses the final mutation from one projected batch entry", %{ctx: ctx} do
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_projected)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      key = "unified-segment-ordered-batch:k"

      assert {:ok, [:ok, :ok, :ok]} =
               WARaftBackend.write_batch(0, [
                 {:put, key, "old", 0},
                 {:delete, key},
                 {:put, key, "new", 0}
               ])

      ensure_segment_projected_key_cold!(ctx, key)
      assert "new" == Router.get(ctx, key)

      gone_key = "unified-segment-ordered-batch:gone"

      assert {:ok, [:ok, :ok]} =
               WARaftBackend.write_batch(0, [
                 {:put, gone_key, "old", 0},
                 {:delete, gone_key}
               ])

      assert nil == Router.get(ctx, gone_key)
    after
      restore_env(:waraft_storage_apply_mode, previous_mode)
    end
  end

  defp ensure_segment_projected_key_cold!(ctx, key, shard_index \\ 0) do
    keydir = elem(ctx.keydir_refs, shard_index)

    case :ets.lookup(keydir, key) do
      [{^key, value, _expire_at_ms, _lfu, {:waraft_segment, index}, _offset, _value_size}]
      when is_binary(value) and is_integer(index) and index > 0 ->
        force_segment_projected_key_cold!(ctx, key, shard_index)

      _ ->
        assert_segment_projected_key_cold!(ctx, key, shard_index)
    end
  end

  defp assert_segment_projected_key_cold!(ctx, key, shard_index) do
    keydir = elem(ctx.keydir_refs, shard_index)

    assert [
             {^key, nil, _expire_at_ms, _lfu, {:waraft_segment, index}, offset, value_size}
           ] = :ets.lookup(keydir, key)

    assert is_integer(index) and index > 0
    assert offset == 0
    assert is_integer(value_size) and value_size > 0
  end

  test "segment-projected storage keeps large non-blob values in the WARaft segment", %{
    root: root,
    ctx: ctx
  } do
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_projected)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      key = "unified-segment-large:k1"
      value = :binary.copy("large-value", 8_000)

      assert byte_size(value) > ctx.hot_cache_max_value_size
      assert byte_size(value) < ctx.blob_side_channel_threshold_bytes

      assert :ok = WARaftBackend.write(0, {:put, key, value, 0})
      assert value == Router.get(ctx, key)
      assert byte_size(value) == Router.value_size(ctx, key)

      bitcask_file = Path.join([root, "data", "shard_0", "00000.log"])
      assert File.stat!(bitcask_file).size == 0

      assert [{^key, nil, 0, _lfu, {:waraft_segment, index}, 0, value_size}] =
               :ets.lookup(elem(ctx.keydir_refs, 0), key)

      assert is_integer(index) and index > 0
      assert value_size == byte_size(value)

      assert :ok = WARaftBackend.stop()

      restarted_ctx = build_ctx(root)

      try do
        assert :ok =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert value == Router.get(restarted_ctx, key)
        assert File.stat!(bitcask_file).size == 0
      after
        FerricStore.Instance.cleanup(restarted_ctx.name)
      end
    after
      restore_env(:waraft_storage_apply_mode, previous_mode)
    end
  end

  test "segment-projected storage keeps blob-backed put batches in the WARaft segment", %{
    root: root,
    ctx: ctx
  } do
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_projected)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      key = "unified-segment-blob:k1"
      value = :binary.copy("blob-backed-value", 20_000)

      assert byte_size(value) > ctx.blob_side_channel_threshold_bytes

      assert {:ok, [:ok]} = WARaftBackend.write_put_batch(0, [{key, value, 0}])
      assert value == Router.get(ctx, key)

      bitcask_file = Path.join([root, "data", "shard_0", "00000.log"])
      assert File.stat!(bitcask_file).size == 0

      assert [{^key, nil, 0, _lfu, {:waraft_segment, index}, 0, value_size}] =
               :ets.lookup(elem(ctx.keydir_refs, 0), key)

      assert is_integer(index) and index > 0
      assert BlobRef.encoded_size?(value_size)

      assert :ok = WARaftBackend.stop()

      restarted_ctx = build_ctx(root)

      try do
        assert :ok =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert value == Router.get(restarted_ctx, key)
        assert File.stat!(bitcask_file).size == 0
      after
        FerricStore.Instance.cleanup(restarted_ctx.name)
      end
    after
      restore_env(:waraft_storage_apply_mode, previous_mode)
    end
  end

  test "segment-projected storage keeps put/delete batches in the WARaft segment", %{
    root: root,
    ctx: ctx
  } do
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_projected)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert {:ok, [:ok, :ok, :ok]} =
               WARaftBackend.write_put_batch(0, [
                 {"unified-batch:k1", "v1", 0},
                 {"unified-batch:k2", "v2", 0},
                 {"unified-batch:gone", "old", 0}
               ])

      assert {:ok, [:ok]} = WARaftBackend.write_delete_batch(0, ["unified-batch:gone"])

      assert "v1" == Router.get(ctx, "unified-batch:k1")
      assert "v2" == Router.get(ctx, "unified-batch:k2")
      assert nil == Router.get(ctx, "unified-batch:gone")

      bitcask_file = Path.join([root, "data", "shard_0", "00000.log"])
      assert File.stat!(bitcask_file).size == 0

      assert :ok = WARaftBackend.stop()

      restarted_ctx = build_ctx(root)

      try do
        assert :ok =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert "v1" == Router.get(restarted_ctx, "unified-batch:k1")
        assert "v2" == Router.get(restarted_ctx, "unified-batch:k2")
        assert nil == Router.get(restarted_ctx, "unified-batch:gone")
        assert File.stat!(bitcask_file).size == 0
      after
        FerricStore.Instance.cleanup(restarted_ctx.name)
      end
    after
      restore_env(:waraft_storage_apply_mode, previous_mode)
    end
  end

  test "segment-projected storage replays acknowledged entries after storage metadata lag", %{
    root: root,
    ctx: ctx
  } do
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

    previous_persist_every =
      Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_projected)
      Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, :never)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert {:ok, pre_write_position} = WARaftBackend.storage_position(0)
      assert :ok = WARaftBackend.write(0, {:put, "unified-lag:k1", "v1", 0})
      assert "v1" == Router.get(ctx, "unified-lag:k1")

      bitcask_file = Path.join([root, "data", "shard_0", "00000.log"])
      assert File.stat!(bitcask_file).size == 0

      assert :ok = WARaftBackend.stop()
      rewind_waraft_storage_position!(root, 0, pre_write_position)

      restarted_ctx = build_ctx(root)

      try do
        assert :ok =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert "v1" == Router.get(restarted_ctx, "unified-lag:k1")
        assert File.stat!(bitcask_file).size == 0
      after
        FerricStore.Instance.cleanup(restarted_ctx.name)
      end
    after
      restore_env(:waraft_storage_apply_mode, previous_mode)
      restore_env(:waraft_storage_metadata_persist_every, previous_persist_every)
    end
  end

  test "segment-projected snapshot transfer preserves projected values", %{root: root} do
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)
    source_root = Path.join(root, "segment-projected-snapshot-source")
    target_root = Path.join(root, "segment-projected-snapshot-target")
    File.mkdir_p!(source_root)
    File.mkdir_p!(target_root)
    source_ctx = build_ctx(source_root)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_projected)

      assert :ok =
               WARaftBackend.start(source_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert :ok = WARaftBackend.write(0, {:put, "unified-snapshot:k", "v1", 0})
      assert "v1" == Router.get(source_ctx, "unified-snapshot:k")
      force_segment_projected_key_cold!(source_ctx, "unified-snapshot:k")

      source_bitcask_file = Path.join([source_root, "data", "shard_0", "00000.log"])
      assert File.stat!(source_bitcask_file).size == 0

      assert {:ok, {:raft_log_pos, index, term} = position} = WARaftBackend.create_snapshot(0)

      snapshot_path =
        Path.join([
          source_root,
          "waraft",
          "ferricstore_waraft_backend.1",
          "snapshot.#{index}.#{term}"
        ])

      refute File.exists?(Path.join(snapshot_path, "segment_projected_keydir.term"))
      assert File.dir?(Path.join([snapshot_path, "segment_projection_log", "segment_log"]))
      refute File.exists?(Path.join(snapshot_path, "dedicated"))

      snapshot_metadata =
        snapshot_path
        |> Path.join("ferricstore_snapshot.term")
        |> File.read!()
        |> :erlang.binary_to_term()

      refute :dedicated in Map.fetch!(snapshot_metadata, :payload_dirs)
      refute :dedicated in Map.fetch!(snapshot_metadata, :empty_payload_dirs)

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
        assert_eventually(fn -> Router.get(target_ctx, "unified-snapshot:k") end, "v1")

        target_bitcask_file = Path.join([target_root, "data", "shard_0", "00000.log"])
        assert File.stat!(target_bitcask_file).size == 0

        target_storage_root =
          Path.join([target_root, "waraft", "ferricstore_waraft_backend.1"])

        refute File.exists?(Path.join(target_storage_root, "segment_projected_keydir.term"))

        assert File.dir?(
                 Path.join([target_storage_root, "segment_projection_log", "segment_log"])
               )

        assert :ok = WARaftBackend.stop()
        FerricStore.Instance.cleanup(target_ctx.name)

        restarted_target_ctx = build_ctx(target_root)

        try do
          assert :ok =
                   WARaftBackend.start(restarted_target_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert_eventually(
            fn -> Router.get(restarted_target_ctx, "unified-snapshot:k") end,
            "v1"
          )

          assert File.stat!(target_bitcask_file).size == 0
        after
          WARaftBackend.stop()
          FerricStore.Instance.cleanup(restarted_target_ctx.name)
        end
      after
        WARaftBackend.stop()
        FerricStore.Instance.cleanup(target_ctx.name)
      end
    after
      WARaftBackend.stop()
      FerricStore.Instance.cleanup(source_ctx.name)
      restore_env(:waraft_storage_apply_mode, previous_mode)
    end
  end

  test "single-member writes avoid per-command WARaft status calls", %{ctx: ctx} do
    assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)
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
    assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)
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

    try do
      assert :ok =
               WARaftBackend.start(ctx,
                 log_module: :wa_raft_log_ets,
                 max_log_entries_per_heartbeat: 2048,
                 max_heartbeat_size: 32 * 1024 * 1024,
                 apply_log_batch_size: 2048,
                 apply_batch_max_bytes: 32 * 1024 * 1024
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
    after
      restore_waraft_app_env(:raft_max_log_entries_per_heartbeat, previous_entries)
      restore_waraft_app_env(:raft_max_heartbeat_size, previous_heartbeat)
      restore_waraft_app_env(:raft_apply_log_batch_size, previous_apply)
      restore_waraft_app_env(:raft_apply_batch_max_bytes, previous_apply_bytes)
    end
  end

  test "FerricStore app env configures WARaft throughput batch knobs", %{ctx: ctx} do
    previous_public = Application.get_env(:ferricstore, :waraft_max_log_entries_per_heartbeat)

    previous_waraft =
      Application.get_env(:ferricstore_waraft_backend, :raft_max_log_entries_per_heartbeat)

    try do
      Application.put_env(:ferricstore, :waraft_max_log_entries_per_heartbeat, 4096)

      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

      assert 4096 ==
               Application.get_env(
                 :ferricstore_waraft_backend,
                 :raft_max_log_entries_per_heartbeat
               )
    after
      restore_env(:waraft_max_log_entries_per_heartbeat, previous_public)
      restore_waraft_app_env(:raft_max_log_entries_per_heartbeat, previous_waraft)
    end
  end

  test "invalid WARaft throughput batch knobs fail closed before partition start", %{ctx: ctx} do
    previous_heartbeat = Application.get_env(:ferricstore, :waraft_max_heartbeat_size)

    try do
      assert_raise ArgumentError, ~r/max_log_entries_per_heartbeat/, fn ->
        WARaftBackend.start(ctx, log_module: :wa_raft_log_ets, max_log_entries_per_heartbeat: 0)
      end

      refute Process.whereis(:raft_sup_ferricstore_waraft_backend_1)

      Application.put_env(:ferricstore, :waraft_max_heartbeat_size, "bad")

      assert_raise ArgumentError, ~r/waraft_max_heartbeat_size/, fn ->
        WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)
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
      Application.put_env(:ferricstore_waraft_backend, :raft_max_log_entries_per_heartbeat, 777)

      assert_raise ArgumentError, ~r/waraft_max_heartbeat_size/, fn ->
        WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)
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

  test "invalid WARaft election timeout bounds fail closed before partition start", %{ctx: ctx} do
    previous_min = Application.get_env(:ferricstore, :waraft_election_timeout_ms)
    previous_max = Application.get_env(:ferricstore, :waraft_election_timeout_ms_max)

    try do
      Application.put_env(:ferricstore, :waraft_election_timeout_ms, 500)
      Application.put_env(:ferricstore, :waraft_election_timeout_ms_max, 250)

      assert_raise ArgumentError, ~r/waraft_election_timeout_ms_max.*>=/, fn ->
        WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)
      end

      refute Process.whereis(:raft_sup_ferricstore_waraft_backend_1)
    after
      restore_env(:waraft_election_timeout_ms, previous_min)
      restore_env(:waraft_election_timeout_ms_max, previous_max)
    end
  end

  test "invalid WARaft queue and commit knobs fail closed before partition start", %{ctx: ctx} do
    previous_pending = Application.get_env(:ferricstore, :waraft_max_pending_reads)

    try do
      assert_raise ArgumentError, ~r/max_pending_high_priority_commits/, fn ->
        WARaftBackend.start(ctx,
          log_module: :wa_raft_log_ets,
          max_pending_high_priority_commits: -1
        )
      end

      refute Process.whereis(:raft_sup_ferricstore_waraft_backend_1)

      assert_raise ArgumentError, ~r/commit_batch_max/, fn ->
        WARaftBackend.start(ctx, log_module: :wa_raft_log_ets, commit_batch_max: 0)
      end

      refute Process.whereis(:raft_sup_ferricstore_waraft_backend_1)

      Application.put_env(:ferricstore, :waraft_max_pending_reads, "bad")

      assert_raise ArgumentError, ~r/waraft_max_pending_reads/, fn ->
        WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)
      end

      refute Process.whereis(:raft_sup_ferricstore_waraft_backend_1)
    after
      restore_env(:waraft_max_pending_reads, previous_pending)
    end
  end

  test "invalid WARaft in-flight bytes config does not partially publish backend app env", %{
    ctx: ctx
  } do
    previous_public = Application.get_env(:ferricstore, :waraft_max_inflight_commit_bytes)
    previous_database = Application.get_env(:ferricstore_waraft_backend, :raft_database)

    try do
      Application.put_env(:ferricstore, :waraft_max_inflight_commit_bytes, :bad)
      Application.put_env(:ferricstore_waraft_backend, :raft_database, ~c"sentinel-waraft-db")

      assert_raise ArgumentError, ~r/waraft_max_inflight_commit_bytes/, fn ->
        WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)
      end

      assert ~c"sentinel-waraft-db" ==
               Application.get_env(:ferricstore_waraft_backend, :raft_database)

      assert_raise ArgumentError, fn ->
        WARaftBackend.context!(:ferricstore_waraft_backend)
      end

      refute Process.whereis(:raft_sup_ferricstore_waraft_backend_1)
    after
      restore_env(:waraft_max_inflight_commit_bytes, previous_public)
      restore_waraft_app_env(:raft_database, previous_database)
    end
  end

  test "WARaft info cleanup tolerates the app info table already being stopped" do
    assert {:ok, _apps} = Application.ensure_all_started(:wa_raft)
    assert :undefined != :ets.whereis(:wa_raft_info)
    assert :ok = Application.stop(:wa_raft)
    assert :undefined == :ets.whereis(:wa_raft_info)

    try do
      assert true = :wa_raft_info.clear(:ferricstore_waraft_backend, 1, :raft_server_test)
    after
      assert {:ok, _apps} = Application.ensure_all_started(:wa_raft)
    end
  end

  test "invalid WARaft module options fail closed before publishing backend state", %{ctx: ctx} do
    previous_database = Application.get_env(:ferricstore_waraft_backend, :raft_database)

    try do
      Application.put_env(:ferricstore_waraft_backend, :raft_database, ~c"sentinel-waraft-db")

      assert_raise ArgumentError, ~r/log_module/, fn ->
        WARaftBackend.start(ctx, log_module: :missing_waraft_log_provider)
      end

      assert ~c"sentinel-waraft-db" ==
               Application.get_env(:ferricstore_waraft_backend, :raft_database)

      assert_raise ArgumentError, fn ->
        WARaftBackend.context!(:ferricstore_waraft_backend)
      end

      refute Process.whereis(:raft_sup_ferricstore_waraft_backend_1)

      assert_raise ArgumentError, ~r/label_module/, fn ->
        WARaftBackend.start(ctx,
          log_module: :wa_raft_log_ets,
          label_module: :missing_label_provider
        )
      end

      assert ~c"sentinel-waraft-db" ==
               Application.get_env(:ferricstore_waraft_backend, :raft_database)

      assert_raise ArgumentError, fn ->
        WARaftBackend.context!(:ferricstore_waraft_backend)
      end

      refute Process.whereis(:raft_sup_ferricstore_waraft_backend_1)
    after
      restore_waraft_app_env(:raft_database, previous_database)
    end
  end

  test "invalid WARaft module options do not stop an already running backend", %{ctx: ctx} do
    assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)
    assert :ok = WARaftBackend.write(0, {:put, "module-preflight:live", "v1", 0})
    assert "v1" == Router.get(ctx, "module-preflight:live")

    assert_raise ArgumentError, ~r/log_module/, fn ->
      WARaftBackend.start(ctx, log_module: :missing_waraft_log_provider)
    end

    assert %FerricStore.Instance{} = WARaftBackend.context!(:ferricstore_waraft_backend)
    assert Process.whereis(:raft_sup_ferricstore_waraft_backend_1)
    assert "v1" == Router.get(ctx, "module-preflight:live")
    assert :ok = WARaftBackend.write(0, {:put, "module-preflight:after", "v2", 0})
    assert "v2" == Router.get(ctx, "module-preflight:after")
  end

  test "bootstrap storage failure during start fails closed and clears backend context", %{
    ctx: ctx
  } do
    previous_hook = Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)

    try do
      Application.put_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook, fn _path ->
        {:error, :forced_bootstrap_metadata_fsync_failure}
      end)

      assert {:error, _reason} =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert_raise ArgumentError, fn ->
        WARaftBackend.context!(:ferricstore_waraft_backend)
      end

      refute Process.whereis(:raft_sup_ferricstore_waraft_backend_1)
    after
      restore_env(:waraft_storage_metadata_fsync_file_hook, previous_hook)
    end
  end

  test "FerricStore app env configures WARaft queue and commit knobs", %{ctx: ctx} do
    previous_pending_reads = Application.get_env(:ferricstore, :waraft_max_pending_reads)
    previous_commit_interval = Application.get_env(:ferricstore, :waraft_commit_batch_interval_ms)
    previous_commit_max = Application.get_env(:ferricstore, :waraft_commit_batch_max)

    previous_backend_pending_reads =
      Application.get_env(:ferricstore_waraft_backend, :raft_max_pending_reads)

    previous_backend_commit_interval =
      Application.get_env(:ferricstore_waraft_backend, :raft_commit_batch_interval_ms)

    previous_backend_commit_max =
      Application.get_env(:ferricstore_waraft_backend, :raft_commit_batch_max)

    try do
      Application.put_env(:ferricstore, :waraft_max_pending_reads, 12_345)
      Application.put_env(:ferricstore, :waraft_commit_batch_interval_ms, 7)
      Application.put_env(:ferricstore, :waraft_commit_batch_max, 2048)

      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

      assert 12_345 ==
               Application.get_env(:ferricstore_waraft_backend, :raft_max_pending_reads)

      assert 7 ==
               Application.get_env(:ferricstore_waraft_backend, :raft_commit_batch_interval_ms)

      assert 2048 ==
               Application.get_env(:ferricstore_waraft_backend, :raft_commit_batch_max)
    after
      restore_env(:waraft_max_pending_reads, previous_pending_reads)
      restore_env(:waraft_commit_batch_interval_ms, previous_commit_interval)
      restore_env(:waraft_commit_batch_max, previous_commit_max)
      restore_waraft_app_env(:raft_max_pending_reads, previous_backend_pending_reads)
      restore_waraft_app_env(:raft_commit_batch_interval_ms, previous_backend_commit_interval)
      restore_waraft_app_env(:raft_commit_batch_max, previous_backend_commit_max)
    end
  end

  test "WARaft redirect timeouts keep unknown-outcome semantics" do
    assert {:error, :timeout} ==
             WARaftBackend.__redirect_write_failure_for_test__(:exit, {:erpc, :timeout})

    assert {:error, :leader_unavailable} ==
             WARaftBackend.__redirect_write_failure_for_test__(:exit, {:erpc, :noconnection})

    assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
             WARaftBackend.__redirect_membership_failure_for_test__(:exit, {:erpc, :timeout})

    assert {:error, :leader_unavailable} ==
             WARaftBackend.__redirect_membership_failure_for_test__(
               :exit,
               {:erpc, :noconnection}
             )

    assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
             WARaftBackend.__redirect_transfer_failure_for_test__(:exit, {:erpc, :timeout})

    assert {:error, :leader_unavailable} ==
             WARaftBackend.__redirect_transfer_failure_for_test__(:exit, {:erpc, :noconnection})
  end

  test "WARaft redirect peer normalization rejects boolean pseudo-nodes" do
    assert :valid_peer@nohost == WARaftBackend.__peer_node_for_test__(:valid_peer@nohost)

    assert :valid_peer@nohost ==
             WARaftBackend.__peer_node_for_test__(
               {:raft_identity, :raft_server_ferricstore_waraft_backend_1, :valid_peer@nohost}
             )

    assert nil == WARaftBackend.__peer_node_for_test__(nil)
    assert nil == WARaftBackend.__peer_node_for_test__(true)
    assert nil == WARaftBackend.__peer_node_for_test__(false)
    assert nil == WARaftBackend.__peer_node_for_test__({:server, true})
    assert nil == WARaftBackend.__peer_node_for_test__({:raft_identity, :server, false})
  end

  test "bootstrap_cluster rejects empty and malformed membership before publishing config", %{
    ctx: ctx
  } do
    assert :ok = WARaftBackend.start(ctx, bootstrap: false, log_module: :wa_raft_log_ets)

    assert {:error, :empty_cluster} = WARaftBackend.bootstrap_cluster([])

    assert {:error, {:invalid_node, "not-a-node"}} =
             WARaftBackend.bootstrap_cluster(["not-a-node"])

    assert {:error, {:invalid_node, nil}} = WARaftBackend.bootstrap_cluster([nil])
    current_node = node()

    assert {:error, {:duplicate_node, ^current_node}} =
             WARaftBackend.bootstrap_cluster([current_node, current_node])

    status = WARaftBackend.status(0)
    assert Keyword.get(status, :state) == :stalled
    assert {:ok, {:raft_log_pos, 0, 0}} = WARaftBackend.storage_position(0)
  end

  test "add_member rejects invalid timeouts before membership mutation", %{ctx: ctx} do
    assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)
    original_membership = WARaftBackend.membership(0)
    target_node = :waraft_invalid_timeout_target@nohost

    assert {:error, {:invalid_timeout_ms, -1}} =
             WARaftBackend.add_member(0, target_node, timeout_ms: -1)

    assert {:error, {:invalid_timeout_ms, :bad}} =
             WARaftBackend.add_participant(0, target_node, timeout_ms: :bad)

    assert original_membership == WARaftBackend.membership(0)
  end

  test "membership mutations reject non-node atoms before config append", %{ctx: ctx} do
    assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)
    original_membership = WARaftBackend.membership(0)

    assert {:error, {:invalid_node, nil}} = WARaftBackend.add_member(0, nil)
    assert {:error, {:invalid_node, false}} = WARaftBackend.add_participant(0, false)

    assert {:error, {:invalid_node, true}} =
             WARaftBackend.adjust_membership(0, :add_participant, true)

    assert original_membership == WARaftBackend.membership(0)
  end

  test "membership mutations reject unknown actions before config append", %{ctx: ctx} do
    assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)
    original_membership = WARaftBackend.membership(0)

    assert {:error, {:invalid_membership_action, :replace_everyone}} =
             WARaftBackend.adjust_membership(0, :replace_everyone, node())

    assert {:error, {:invalid_membership_action, :replace_everyone}} =
             WARaftBackend.adjust_membership_redirected(
               0,
               :replace_everyone,
               node(),
               10_000,
               0
             )

    assert original_membership == WARaftBackend.membership(0)
  end

  test "redirected public WARaft APIs reject malformed redirect counts", %{ctx: ctx} do
    assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)
    original_membership = WARaftBackend.membership(0)

    assert {:error, {:invalid_redirects_left, -1}} =
             WARaftBackend.write_redirected(0, {:put, "redirect-count:write", "v", 0}, -1)

    assert {:error, {:invalid_redirects_left, :bad}} =
             WARaftBackend.transfer_leadership_redirected(0, node(), :bad)

    assert {:error, {:invalid_redirects_left, -1}} =
             WARaftBackend.adjust_membership_redirected(0, :add_participant, node(), 10_000, -1)

    assert {:error, {:invalid_redirects_left, :bad}} =
             WARaftBackend.add_member_redirected(0, node(), 10_000, :bad)

    assert {:error, {:invalid_redirects_left, -1}} =
             WARaftBackend.add_participant_redirected(0, node(), 10_000, -1)

    assert original_membership == WARaftBackend.membership(0)
    assert nil == Router.get(ctx, "redirect-count:write")
  end

  test "bootstrap_cluster does not cache a conflicting config after bootstrap", %{ctx: ctx} do
    assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)
    original_membership = WARaftBackend.membership(0)

    assert {:error, {:already_bootstrapped, _actual_nodes}} =
             WARaftBackend.bootstrap_cluster([
               node(),
               :waraft_conflicting_bootstrap_1@nohost,
               :waraft_conflicting_bootstrap_2@nohost
             ])

    assert original_membership == WARaftBackend.membership(0)
    assert :ok = WARaftBackend.write(0, {:put, "bootstrap-conflict:still-live", "v", 0})
    assert "v" == Router.get(ctx, "bootstrap-conflict:still-live")
  end

  test "cached voter extraction ignores non-node atoms from malformed metadata", %{ctx: ctx} do
    assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

    WARaftBackend.cache_config(0, %{
      membership: [
        {:raft_server_ferricstore_waraft_backend_1, nil},
        {:raft_server_ferricstore_waraft_backend_1, false},
        {:raft_server_ferricstore_waraft_backend_1, node()}
      ]
    })

    assert :ok = WARaftBackend.write(0, {:put, "cache-config:valid-voter", "v", 0})
    assert "v" == Router.get(ctx, "cache-config:valid-voter")
  end

  test "public WARaft APIs reject invalid shard indices without crashing", %{ctx: ctx} do
    assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

    assert {:error, {:invalid_shard_index, -1}} =
             WARaftBackend.write(-1, {:put, "invalid-shard:write", "v", 0})

    assert [
             {:error, {:invalid_shard_index, -1}},
             :ok
           ] =
             WARaftBackend.write_many([
               {-1, {:put, "invalid-shard:batch-bad", "v", 0}},
               {0, {:put, "invalid-shard:batch-good", "v", 0}}
             ])

    assert "v" == Router.get(ctx, "invalid-shard:batch-good")

    assert {:error, {:invalid_shard_index, -1}} =
             WARaftBackend.write_batch(-1, [{:put, "invalid-shard:write-batch", "v", 0}])

    assert {:error, {:invalid_shard_index, -1}} =
             WARaftBackend.write_put_batch(-1, [{"invalid-shard:put-batch", "v", 0}])

    assert {:error, {:invalid_shard_index, -1}} =
             WARaftBackend.write_delete_batch(-1, ["invalid-shard:delete-batch"])

    assert {:error, {:invalid_shard_index, -1}} =
             WARaftBackend.local_get(-1, "invalid-shard:local-get")

    assert {:error, {:invalid_shard_index, -1}} = WARaftBackend.status(-1)
    assert {:error, {:invalid_shard_index, -1}} = WARaftBackend.membership(-1)
    assert {:error, {:invalid_shard_index, -1}} = WARaftBackend.storage_position(-1)
    assert {:error, {:invalid_shard_index, -1}} = WARaftBackend.create_snapshot(-1)

    assert {:error, {:invalid_shard_index, -1}} =
             WARaftBackend.install_snapshot(-1, "/tmp/no-snapshot", {:raft_log_pos, 0, 0})

    assert {:error, {:invalid_snapshot_path, nil}} =
             WARaftBackend.install_snapshot(0, nil, {:raft_log_pos, 0, 0})

    assert {:error, {:invalid_snapshot_position, :bad_position}} =
             WARaftBackend.install_snapshot(0, "/tmp/no-snapshot", :bad_position)

    assert {:error, {:invalid_shard_index, -1}} = WARaftBackend.trigger_election(-1)

    assert {:error, {:invalid_shard_index, -1}} =
             WARaftBackend.transfer_leadership(-1, node())

    assert {:error, {:invalid_shard_index, -1}} =
             WARaftBackend.add_member(-1, :invalid_shard_target@nohost)

    assert {:error, {:invalid_shard_index, -1}} =
             WARaftBackend.add_participant(-1, :invalid_shard_target@nohost)

    assert {:error, {:invalid_shard_index, -1}} =
             WARaftBackend.adjust_membership(-1, :add_participant, :invalid_shard_target@nohost)

    assert {:error, {:invalid_shard_index, -1}} = WARaftBackend.peer_ready(-1, node())
    assert 0 == WARaftBackend.inflight_commit_bytes(-1)
  end

  test "out-of-range shard write fails closed when in-flight byte cap is enabled", %{ctx: ctx} do
    assert :ok =
             WARaftBackend.start(ctx,
               log_module: :wa_raft_log_ets,
               max_inflight_commit_bytes: 128
             )

    assert {:error, {:invalid_shard_index, 1}} =
             WARaftBackend.write(1, {:put, "invalid-shard:over-cap", "v", 0})

    assert 0 == WARaftBackend.inflight_commit_bytes(1)
    assert :ok = WARaftBackend.write(0, {:put, "invalid-shard:cap-good", "v", 0})
    assert "v" == Router.get(ctx, "invalid-shard:cap-good")
  end

  test "public WARaft batch write APIs reject malformed payloads without crashing", %{ctx: ctx} do
    assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

    assert {:error, {:invalid_write_many_entries, :not_a_list}} =
             WARaftBackend.write_many(:not_a_list)

    assert [
             {:error, {:invalid_write_many_entry, :bad_entry}},
             :ok
           ] =
             WARaftBackend.write_many([
               :bad_entry,
               {0, {:put, "invalid-payload:write-many-good", "v", 0}}
             ])

    assert "v" == Router.get(ctx, "invalid-payload:write-many-good")

    assert {:error, {:invalid_command_batch, :not_a_list}} =
             WARaftBackend.write_batch(0, :not_a_list)

    assert {:error, {:invalid_put_batch, :not_a_list}} =
             WARaftBackend.write_put_batch(0, :not_a_list)

    assert {:error, {:invalid_delete_batch, :not_a_list}} =
             WARaftBackend.write_delete_batch(0, :not_a_list)

    assert {:error, {:invalid_key, :not_a_key}} =
             WARaftBackend.local_get(0, :not_a_key)
  end

  test "configured namespace windows coalesce WARaft writes before apply", %{ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    previous_hook = Application.get_env(:ferricstore, :standalone_durability_hook)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      assert :ok = Ferricstore.NamespaceConfig.set("waraftns", "window_ms", "25")
      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

      parent = self()

      Application.put_env(:ferricstore, :standalone_durability_hook, fn _file_path, batch ->
        send(parent, {:waraft_namespace_window_batch, batch})
        :passthrough
      end)

      tasks =
        for i <- 1..2 do
          Task.async(fn ->
            WARaftBackend.write(0, {:put, "waraftns:key#{i}", "v#{i}", 0})
          end)
        end

      assert [:ok, :ok] = Enum.map(tasks, &Task.await(&1, 5_000))

      assert_receive {:waraft_namespace_window_batch, batch}, 1_000
      keys = Enum.map(batch, fn {:put, key, _value, _expire_at_ms} -> key end)
      assert "waraftns:key1" in keys
      assert "waraftns:key2" in keys

      assert "v1" == Router.get(ctx, "waraftns:key1")
      assert "v2" == Router.get(ctx, "waraftns:key2")
    after
      restore_env(:standalone_durability_hook, previous_hook)
      restore_backend(previous_backend)
      Ferricstore.NamespaceConfig.reset("waraftns")
    end
  end

  test "configured namespace windows ignore stale flush messages", %{ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      assert :ok = Ferricstore.NamespaceConfig.set("stale-win", "window_ms", "200")
      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

      task =
        Task.async(fn ->
          WARaftBackend.write(0, {:put, "stale-win:key", "v", 0})
        end)

      Process.sleep(10)
      send(Process.whereis(Ferricstore.Raft.WARaftBackend.Batcher.name(0)), {:flush, "stale-win"})

      ref = task.ref
      refute_receive {^ref, _reply}, 50
      assert :ok = Task.await(task, 1_000)
      assert "v" == Router.get(ctx, "stale-win:key")
    after
      restore_backend(previous_backend)
      Ferricstore.NamespaceConfig.reset("stale-win")
    end
  end

  test "hot put batches ignore stale flush messages", %{ctx: ctx} do
    previous_window = Application.get_env(:ferricstore, :waraft_hot_batch_window_ms)

    try do
      Application.put_env(:ferricstore, :waraft_hot_batch_window_ms, 200)
      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

      task =
        Task.async(fn ->
          WARaftBackend.write_put_batch(0, [{"stale-hot:key", "v", 0}])
        end)

      Process.sleep(10)
      send(Process.whereis(Ferricstore.Raft.WARaftBackend.Batcher.name(0)), :flush_hot_put_batch)

      ref = task.ref
      refute_receive {^ref, _reply}, 50
      assert {:ok, [:ok]} = Task.await(task, 1_000)
      assert "v" == Router.get(ctx, "stale-hot:key")
    after
      restore_env(:waraft_hot_batch_window_ms, previous_window)
    end
  end

  test "hot delete batches ignore stale flush messages", %{ctx: ctx} do
    previous_window = Application.get_env(:ferricstore, :waraft_hot_batch_window_ms)

    try do
      Application.put_env(:ferricstore, :waraft_hot_batch_window_ms, 200)
      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)
      assert {:ok, [:ok]} = WARaftBackend.write_put_batch(0, [{"stale-hot-delete:key", "v", 0}])

      task =
        Task.async(fn ->
          WARaftBackend.write_delete_batch(0, ["stale-hot-delete:key"])
        end)

      Process.sleep(10)

      send(
        Process.whereis(Ferricstore.Raft.WARaftBackend.Batcher.name(0)),
        :flush_hot_delete_batch
      )

      ref = task.ref
      refute_receive {^ref, _reply}, 50
      assert {:ok, [:ok]} = Task.await(task, 1_000)
      assert nil == Router.get(ctx, "stale-hot-delete:key")
    after
      restore_env(:waraft_hot_batch_window_ms, previous_window)
    end
  end

  test "committed hot batch terms keep ordered per-command replies", %{ctx: ctx} do
    assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

    assert {:ok, [:ok, :ok]} =
             WARaftBackend.write_put_batch(0, [
               {"batch:k1", "v1", 0},
               {"batch:k2", "v2", 0}
             ])

    assert "v1" == Router.get(ctx, "batch:k1")
    assert "v2" == Router.get(ctx, "batch:k2")

    assert {:ok, [:ok, :ok]} = WARaftBackend.write_delete_batch(0, ["batch:k1", "batch:k2"])
    assert nil == Router.get(ctx, "batch:k1")
    assert nil == Router.get(ctx, "batch:k2")
  end

  test "generic batches flatten nested hot batch terms before apply", %{ctx: ctx} do
    assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

    assert {:ok, [:ok, :ok, :ok, :ok]} =
             WARaftBackend.write_batch(0, [
               {:put_batch, [{"nested-batch:k1", "v1", 0}, {"nested-batch:k2", "v2", 0}]},
               {:delete_batch, ["nested-batch:k1"]},
               {:put, "nested-batch:k3", "v3", 0}
             ])

    assert nil == Router.get(ctx, "nested-batch:k1")
    assert "v2" == Router.get(ctx, "nested-batch:k2")
    assert "v3" == Router.get(ctx, "nested-batch:k3")
  end

  test "single-member WARaft externalizes large put batches to blob refs", %{ctx: ctx} do
    assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

    payload = :binary.copy("large-waraft-value", 20_000)
    assert byte_size(payload) > ctx.blob_side_channel_threshold_bytes

    assert {:ok, [:ok]} = WARaftBackend.write_put_batch(0, [{"blob:large", payload, 0}])
    assert payload == Router.get(ctx, "blob:large")

    assert [{_, nil, 0, _lfu, _fid, _off, value_size}] =
             :ets.lookup(elem(ctx.keydir_refs, 0), "blob:large")

    assert BlobRef.encoded_size?(value_size)
    assert value_size < byte_size(payload)
  end

  test "single-member WARaft fails closed when blob preparation raises", %{ctx: ctx} do
    assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

    payload = :binary.copy("large-waraft-prepare-fails", 20_000)
    assert byte_size(payload) > ctx.blob_side_channel_threshold_bytes
    parent = self()
    handler_id = {__MODULE__, :blob_prepare_failed, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :waraft, :blob_prepare_failed],
      &__MODULE__.handle_blob_prepare_failed_telemetry/4,
      parent
    )

    write_hook = fn _io, _iodata ->
      raise RuntimeError, "blob write failed before raft submit"
    end

    previous_write_hook = Application.get_env(:ferricstore, :blob_store_write_hook)
    Process.put(:ferricstore_blob_store_write_hook, write_hook)
    Application.put_env(:ferricstore, :blob_store_write_hook, write_hook)

    on_exit(fn ->
      :telemetry.detach(handler_id)
      Process.delete(:ferricstore_blob_store_write_hook)
      restore_env(:blob_store_write_hook, previous_write_hook)
    end)

    assert {:error,
            {:blob_prepare_failed, {RuntimeError, "blob write failed before raft submit"}}} =
             WARaftBackend.write_put_batch(0, [{"blob:prepare-fails", payload, 0}])

    assert_receive {:waraft_blob_prepare_failed, [:ferricstore, :waraft, :blob_prepare_failed],
                    %{count: 1},
                    %{
                      shard_index: 0,
                      reason: {RuntimeError, "blob write failed before raft submit"},
                      command_shape: :put_batch
                    }}

    assert nil == Router.get(ctx, "blob:prepare-fails")
  end

  test "restart reopens real Bitcask state at the last durable apply position", %{
    root: root,
    ctx: ctx
  } do
    assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
    assert :ok = WARaftBackend.write(0, {:put, "restart:k", "restart:v", 0})
    assert "restart:v" == Router.get(ctx, "restart:k")

    assert :ok = WARaftBackend.stop()
    FerricStore.Instance.cleanup(ctx.name)

    restarted_ctx = build_ctx(root)

    assert :ok =
             WARaftBackend.start(restarted_ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert "restart:v" == Router.get(restarted_ctx, "restart:k")
    assert {:ok, position} = WARaftBackend.storage_position(0)
    assert elem(position, 1) >= 2
  end

  @tag :shard_kill
  test "acked writes survive WARaft server kill during active write load", %{
    root: root,
    ctx: ctx
  } do
    assert :ok =
             WARaftBackend.start(ctx,
               log_module: :ferricstore_waraft_spike_segment_log,
               commit_batch_interval_ms: 5,
               commit_batch_max: 32
             )

    parent = self()

    writer =
      Task.async(fn ->
        for i <- 1..80 do
          key = "kill-load:#{i}"
          value = "v#{i}"
          result = WARaftBackend.write(0, {:put, key, value, 0})
          send(parent, {:waraft_kill_load_result, key, value, result})
        end
      end)

    acked_before_kill = wait_for_kill_load_acks(5, [])
    kill_waraft_server!(0)

    _ = Task.yield(writer, 5_000) || Task.shutdown(writer, :brutal_kill)
    acked = drain_kill_load_results(acked_before_kill)

    assert length(acked) >= 5

    assert :ok = WARaftBackend.stop()
    FerricStore.Instance.cleanup(ctx.name)

    restarted_ctx = build_ctx(root)

    assert :ok =
             WARaftBackend.start(restarted_ctx,
               log_module: :ferricstore_waraft_spike_segment_log
             )

    for {key, value} <- acked do
      assert_eventually(fn -> Router.get(restarted_ctx, key) end, value)
    end
  end

  @tag :shard_kill
  test "acked writes survive one WARaft server kill during multi-shard write load", %{
    root: root
  } do
    shard_count = 4
    victim_shard = 2
    multi_root = Path.join(root, "multi-shard-kill")
    File.mkdir_p!(multi_root)
    Ferricstore.DataDir.ensure_layout!(multi_root, shard_count)
    Ferricstore.Store.ActiveFile.init(shard_count)
    ctx = build_ctx(multi_root, shard_count: shard_count)

    assert :ok =
             WARaftBackend.start(ctx,
               log_module: :ferricstore_waraft_spike_segment_log,
               commit_batch_interval_ms: 5,
               commit_batch_max: 32
             )

    parent = self()

    writers =
      for shard_idx <- 0..(shard_count - 1) do
        Task.async(fn ->
          for i <- 1..40 do
            key = key_for_shard(ctx, shard_idx, "multi-kill:#{shard_idx}:#{i}")
            value = "v#{shard_idx}:#{i}"
            result = WARaftBackend.write(shard_idx, {:put, key, value, 0})
            send(parent, {:waraft_multi_kill_result, shard_idx, key, value, result})
          end
        end)
      end

    acked_before_kill = wait_for_multi_kill_shard_acks(victim_shard, 3, [])
    kill_waraft_server!(victim_shard)

    Enum.each(writers, fn writer ->
      _ = Task.yield(writer, 5_000) || Task.shutdown(writer, :brutal_kill)
    end)

    acked = drain_multi_kill_results(acked_before_kill)
    assert Enum.any?(acked, fn {shard_idx, _key, _value} -> shard_idx == victim_shard end)
    assert Enum.any?(acked, fn {shard_idx, _key, _value} -> shard_idx != victim_shard end)

    assert :ok = WARaftBackend.stop()
    FerricStore.Instance.cleanup(ctx.name)

    Ferricstore.Store.ActiveFile.init(shard_count)
    restarted_ctx = build_ctx(multi_root, shard_count: shard_count)

    assert :ok =
             WARaftBackend.start(restarted_ctx,
               log_module: :ferricstore_waraft_spike_segment_log
             )

    for {shard_idx, key, value} <- acked do
      assert_eventually(fn -> Router.get(restarted_ctx, key) end, value)
      assert Router.shard_for(restarted_ctx, key) == shard_idx
    end

    FerricStore.Instance.cleanup(restarted_ctx.name)
  end

  test "restart replay does not double-apply non-idempotent RMW after storage-position lag", %{
    root: root,
    ctx: ctx
  } do
    assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
    assert {:ok, pre_write_position} = WARaftBackend.storage_position(0)

    assert {:ok, 1} = WARaftBackend.write(0, {:incr, "rmw:lag", 1})
    assert "1" == Router.get(ctx, "rmw:lag")

    assert :ok = WARaftBackend.stop()
    rewind_waraft_storage_position!(root, 0, pre_write_position)
    FerricStore.Instance.cleanup(ctx.name)

    restarted_ctx = build_ctx(root)

    assert :ok =
             WARaftBackend.start(restarted_ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert "1" == Router.get(restarted_ctx, "rmw:lag")
  end

  test "failed Bitcask apply does not advance WARaft storage replay position", %{
    root: root,
    ctx: ctx
  } do
    assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
    assert {:ok, pre_write_position} = WARaftBackend.storage_position(0)

    previous_hook = Application.get_env(:ferricstore, :standalone_durability_hook)

    try do
      Application.put_env(:ferricstore, :standalone_durability_hook, fn _file_path, _batch ->
        {:error, :forced_bitcask_failure}
      end)

      assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
               WARaftBackend.write(0, {:put, "apply-fail:k", "apply-fail:v", 0})

      assert nil == Router.get(ctx, "apply-fail:k")
      assert {:ok, ^pre_write_position} = WARaftBackend.storage_position(0)

      assert :ok = WARaftBackend.stop()
      restore_env(:standalone_durability_hook, previous_hook)
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert_eventually(fn -> Router.get(restarted_ctx, "apply-fail:k") end, "apply-fail:v")
    after
      restore_env(:standalone_durability_hook, previous_hook)
    end
  end

  test "segment log append failure returns unknown outcome without applying before restart", %{
    root: root,
    ctx: ctx
  } do
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_append_hook)

    try do
      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      Application.put_env(
        :ferricstore,
        :waraft_segment_log_append_hook,
        {:fail_once_after_write, self()}
      )

      assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
               WARaftBackend.write(0, {:put, "log-append-fail:k", "v1", 0})

      assert_receive {:waraft_segment_log_append_hook, :after_write}, 1_000
      assert nil == Router.get(ctx, "log-append-fail:k")

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert nil == Router.get(restarted_ctx, "log-append-fail:k")
    after
      restore_env(:waraft_segment_log_append_hook, previous_hook)
    end
  end

  test "segment log file fsync failure returns unknown outcome without applying before restart",
       %{
         root: root,
         ctx: ctx
       } do
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)

    try do
      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      Application.put_env(:ferricstore, :waraft_segment_log_file_sync_hook, {:fail_once, self()})

      assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
               WARaftBackend.write(0, {:put, "log-file-sync-fail:k", "v1", 0})

      assert_receive {:waraft_segment_log_file_sync, _path}, 1_000
      assert nil == Router.get(ctx, "log-file-sync-fail:k")

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert nil == Router.get(restarted_ctx, "log-file-sync-fail:k")
    after
      restore_env(:waraft_segment_log_file_sync_hook, previous_hook)
    end
  end

  test "segment log restart truncates torn oversized tail header", %{root: root, ctx: ctx} do
    assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
    assert :ok = WARaftBackend.write(0, {:put, "log-torn-tail:k", "v1", 0})
    assert "v1" == Router.get(ctx, "log-torn-tail:k")
    assert :ok = WARaftBackend.stop()

    segment_path = Path.join(waraft_segment_log_dir(root, 0), "0.seg")
    size_before_tail = File.stat!(segment_path).size

    File.write!(segment_path, <<2_147_483_648::32, 0::32>>, [:append, :binary])
    assert File.stat!(segment_path).size == size_before_tail + 8

    FerricStore.Instance.cleanup(ctx.name)
    restarted_ctx = build_ctx(root)

    assert :ok =
             WARaftBackend.start(restarted_ctx,
               log_module: :ferricstore_waraft_spike_segment_log
             )

    assert_eventually(fn -> Router.get(restarted_ctx, "log-torn-tail:k") end, "v1")
    segment = File.read!(segment_path)
    refute binary_part(segment, size_before_tail, 8) == <<2_147_483_648::32, 0::32>>
    <<valid_len::32, _valid_crc::32>> = binary_part(segment, size_before_tail, 8)
    assert valid_len < 1_073_741_824
    assert byte_size(segment) >= size_before_tail + 8 + valid_len
  end

  test "segment log recovery streams records instead of reading whole segment files" do
    source =
      Path.expand("../../../src/ferricstore_waraft_spike_segment_log.erl", __DIR__)
      |> File.read!()

    assert [_, recovery_source] =
             String.split(
               source,
               "load_segment(Ordinal, Path, Name, PreviousIndex, RecordsPerSegment) ->",
               parts: 2
             )

    assert [recovery_source, _] =
             String.split(recovery_source, "insert_recovered_record(", parts: 2)

    refute recovery_source =~ "file:read_file(",
           "segment recovery must not materialize a full segment file in BEAM memory"

    assert recovery_source =~ "file:open(Path, [read, raw, binary])"
    assert recovery_source =~ "file:read(Fd, ?RECORD_HEADER_SIZE)"
  end

  test "segment log trim and truncate stream kept records into rewrite staging" do
    source =
      Path.expand("../../../src/ferricstore_waraft_spike_segment_log.erl", __DIR__)
      |> File.read!()

    refute source =~ "kept_records_from(Name, Index)",
           "truncate rewrite must not materialize every kept record before staging"

    refute source =~ "kept_records_at_or_after(Name, Index)",
           "trim rewrite must not materialize every kept record before staging"
  end

  test "segment log config lookup walks backward instead of folding the full log" do
    source =
      Path.expand("../../../src/ferricstore_waraft_spike_segment_log.erl", __DIR__)
      |> File.read!()

    assert [_, config_source] =
             String.split(source, "config(#raft_log{name = Name}) ->", parts: 2)

    assert [config_source, _] =
             String.split(config_source, "config_from_entry", parts: 2)

    refute config_source =~ "ets:foldl",
           "config lookup must not scan every log entry when the latest config is near the tail"

    assert config_source =~ "ets:last(Name)"
    assert source =~ "ets:prev(Name, Index)"
  end

  test "segment log append grouping stays linear for monotonic Raft batches" do
    source =
      Path.expand("../../../src/ferricstore_waraft_spike_segment_log.erl", __DIR__)
      |> File.read!()

    assert [_, grouping_source] =
             String.split(source, "\ngroup_records(Records, RecordsPerSegment) ->", parts: 2)

    assert [grouping_source, _] =
             String.split(grouping_source, "write_record_group_list", parts: 2)

    refute grouping_source =~ "maps:update",
           "append grouping should not allocate a map for already-monotonic Raft entries"

    refute grouping_source =~ "maps:to_list",
           "append grouping should not sort a map on the append hot path"
  end

  test "segment log records per segment is configurable", %{root: root, ctx: ctx} do
    previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      assert :ok = WARaftBackend.write(0, {:put, "log-config-segment:k", "v1", 0})

      segment_files =
        root
        |> waraft_segment_log_dir(0)
        |> File.ls!()
        |> Enum.filter(&String.ends_with?(&1, ".seg"))
        |> Enum.sort()

      assert "0.seg" in segment_files
      assert "1.seg" in segment_files
    after
      WARaftBackend.stop()
      restore_env(:waraft_segment_log_records_per_segment, previous)
    end
  end

  test "segment log keeps its original segment sizing across config changes", %{ctx: ctx} do
    previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      assert :ok = WARaftBackend.write(0, {:put, "log-config-stable:first", "v1", 0})

      assert File.exists?(
               Path.join(waraft_segment_log_dir(ctx.data_dir, 0), "segment_config.term")
             )

      WARaftBackend.stop()

      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 4096)

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      assert :ok = WARaftBackend.write(0, {:put, "log-config-stable:second", "v2", 0})
      WARaftBackend.stop()

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      assert "v1" == Router.get(ctx, "log-config-stable:first")
      assert "v2" == Router.get(ctx, "log-config-stable:second")
    after
      WARaftBackend.stop()
      restore_env(:waraft_segment_log_records_per_segment, previous)
    end
  end

  test "segment log rewrite preserves its original segment sizing after config changes", %{
    root: root,
    ctx: ctx
  } do
    previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      assert :ok = WARaftBackend.write(0, {:put, "log-config-rewrite:first", "v1", 0})
      assert :ok = WARaftBackend.write(0, {:put, "log-config-rewrite:second", "v2", 0})

      segment_dir = waraft_segment_log_dir(root, 0)
      assert %{records_per_segment: 2} = read_segment_config(segment_dir)

      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 4096)

      log = waraft_segment_log_record(0)
      assert {:ok, _state} = :ferricstore_waraft_spike_segment_log.trim(log, 2, %{})

      assert %{records_per_segment: 2} = read_segment_config(segment_dir)
    after
      WARaftBackend.stop()
      restore_env(:waraft_segment_log_records_per_segment, previous)
    end
  end

  test "segment log close clears its segment sizing cache", %{root: root, ctx: ctx} do
    previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      assert :ok = WARaftBackend.write(0, {:put, "log-config-cache:k", "v", 0})

      segment_dir = waraft_segment_log_dir(root, 0)

      cache_key =
        {:ferricstore_waraft_spike_segment_log, :records_per_segment,
         segment_dir |> Path.absname() |> String.to_charlist()}

      assert :persistent_term.get(cache_key, :missing) == 2

      assert :ok = WARaftBackend.stop()
      assert :persistent_term.get(cache_key, :missing) == :missing
    after
      WARaftBackend.stop()
      restore_env(:waraft_segment_log_records_per_segment, previous)
    end
  end

  test "segment log fails closed when persisted segment sizing metadata is corrupt", %{
    root: root,
    ctx: ctx
  } do
    previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      assert :ok = WARaftBackend.write(0, {:put, "log-config-corrupt:k", "v", 0})
      WARaftBackend.stop()

      root
      |> waraft_segment_log_dir(0)
      |> Path.join("segment_config.term")
      |> File.write!("not-an-erlang-term")

      assert {:error, _reason} =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
    after
      WARaftBackend.stop()
      restore_env(:waraft_segment_log_records_per_segment, previous)
    end
  end

  test "segment log fails closed when persisted segment sizing metadata is oversized", %{
    root: root,
    ctx: ctx
  } do
    previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      assert :ok = WARaftBackend.write(0, {:put, "log-config-oversized:k", "v", 0})
      WARaftBackend.stop()

      root
      |> waraft_segment_log_dir(0)
      |> Path.join("segment_config.term")
      |> File.write!(
        :erlang.term_to_binary(%{
          version: 1,
          records_per_segment: 2,
          label: :binary.copy("x", 1_048_576)
        })
      )

      assert {:error, reason} =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert inspect(reason) =~ "segment_config_file_too_large"
    after
      WARaftBackend.stop()
      restore_env(:waraft_segment_log_records_per_segment, previous)
    end
  end

  test "segment log rejects persisted segment sizing metadata symlink", %{
    root: root,
    ctx: ctx
  } do
    previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      assert :ok = WARaftBackend.write(0, {:put, "log-config-symlink:k", "v", 0})
      WARaftBackend.stop()

      segment_dir = waraft_segment_log_dir(root, 0)
      segment_config = Path.join(segment_dir, "segment_config.term")
      outside_config = Path.join(root, "outside-segment-config.term")

      File.write!(
        outside_config,
        :erlang.term_to_binary(%{version: 1, records_per_segment: 2})
      )

      File.rm!(segment_config)
      assert :ok = File.ln_s(outside_config, segment_config)

      assert {:error, reason} =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert inspect(reason) =~ "unsafe_segment_metadata_path"
      assert {:ok, %{type: :symlink}} = File.lstat(segment_config)
    after
      WARaftBackend.stop()
      restore_env(:waraft_segment_log_records_per_segment, previous)
    end
  end

  test "segment log rejects symlinked segment files during restart", %{
    root: root,
    ctx: ctx
  } do
    previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 16)

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      assert :ok = WARaftBackend.write(0, {:put, "log-segment-symlink:k", "v", 0})
      WARaftBackend.stop()

      segment_dir = waraft_segment_log_dir(root, 0)
      [segment_path | _] = Path.wildcard(Path.join(segment_dir, "*.seg"))
      outside_segment = Path.join(root, "outside-segment.seg")

      File.cp!(segment_path, outside_segment)
      File.rm!(segment_path)
      assert :ok = File.ln_s(outside_segment, segment_path)

      assert {:error, reason} =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert inspect(reason) =~ "unsafe_segment_path"
      assert {:ok, %{type: :symlink}} = File.lstat(segment_path)
    after
      WARaftBackend.stop()
      restore_env(:waraft_segment_log_records_per_segment, previous)
    end
  end

  test "segment log rejects symlinked segment log directory during restart", %{
    root: root,
    ctx: ctx
  } do
    previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 16)

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      assert :ok = WARaftBackend.write(0, {:put, "log-dir-symlink:k", "v", 0})
      WARaftBackend.stop()

      segment_dir = waraft_segment_log_dir(root, 0)
      outside_dir = Path.join(root, "outside-segment-log")

      File.cp_r!(segment_dir, outside_dir)
      File.rm_rf!(segment_dir)
      assert :ok = File.ln_s(outside_dir, segment_dir)

      assert {:error, reason} =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert inspect(reason) =~ "unsafe_segment_log_dir"
      assert {:ok, %{type: :symlink}} = File.lstat(segment_dir)
    after
      WARaftBackend.stop()
      restore_env(:waraft_segment_log_records_per_segment, previous)
    end
  end

  test "segment log rejects appending through symlinked segment files", %{
    root: root,
    ctx: ctx
  } do
    previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 16)

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      segment_dir = waraft_segment_log_dir(root, 0)
      [segment_path | _] = Path.wildcard(Path.join(segment_dir, "*.seg"))
      outside_segment = Path.join(root, "outside-live-append-segment.seg")

      File.cp!(segment_path, outside_segment)
      outside_size = File.stat!(outside_segment).size
      File.rm!(segment_path)
      assert :ok = File.ln_s(outside_segment, segment_path)

      assert {:error, reason} =
               WARaftBackend.write(0, {:put, "log-segment-live-symlink:k", "v", 0})

      assert inspect(reason) =~ "unsafe_segment_path" or
               reason in [:unknown_outcome, :timeout, {:timeout, :unknown_outcome}]

      assert File.stat!(outside_segment).size == outside_size
      assert nil == Router.get(ctx, "log-segment-live-symlink:k")
    after
      WARaftBackend.stop()
      restore_env(:waraft_segment_log_records_per_segment, previous)
    end
  end

  test "segment log rejects appending through a symlinked segment log directory", %{
    root: root,
    ctx: ctx
  } do
    previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 16)

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      segment_dir = waraft_segment_log_dir(root, 0)
      outside_dir = Path.join(root, "outside-live-append-segment-log")
      outside_segment = Path.join(outside_dir, "0.seg")

      File.rename!(segment_dir, outside_dir)
      assert :ok = File.ln_s(outside_dir, segment_dir)
      outside_size = File.stat!(outside_segment).size

      assert {:error, reason} =
               WARaftBackend.write(0, {:put, "log-dir-live-symlink:k", "v", 0})

      assert inspect(reason) =~ "unsafe_segment_log_dir" or
               reason in [:unknown_outcome, :timeout, {:timeout, :unknown_outcome}]

      assert File.stat!(outside_segment).size == outside_size
      assert nil == Router.get(ctx, "log-dir-live-symlink:k")
    after
      WARaftBackend.stop()
      restore_env(:waraft_segment_log_records_per_segment, previous)
    end
  end

  test "segment log rejects records stored under the wrong segment ordinal", %{
    root: root,
    ctx: ctx
  } do
    previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      assert :ok = WARaftBackend.write(0, {:put, "log-ordinal-mismatch:k", "v", 0})
      WARaftBackend.stop()

      segment_dir = waraft_segment_log_dir(root, 0)
      segment_one = Path.join(segment_dir, "1.seg")
      wrong_segment = Path.join(segment_dir, "9.seg")

      assert File.exists?(segment_one)
      File.rename!(segment_one, wrong_segment)

      assert {:error, reason} =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert inspect(reason) =~ "segment_ordinal_mismatch"
    after
      WARaftBackend.stop()
      restore_env(:waraft_segment_log_records_per_segment, previous)
    end
  end

  test "segment log rejects duplicate numeric segment ordinals", %{
    root: root,
    ctx: ctx
  } do
    previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 16)

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      assert :ok = WARaftBackend.write(0, {:put, "log-duplicate-ordinal:k", "v", 0})
      WARaftBackend.stop()

      segment_dir = waraft_segment_log_dir(root, 0)
      segment_zero = Path.join(segment_dir, "0.seg")
      duplicate_zero = Path.join(segment_dir, "00.seg")

      assert File.exists?(segment_zero)
      File.write!(duplicate_zero, "")

      assert {:error, reason} =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert inspect(reason) =~ "duplicate_segment_ordinal" or
               inspect(reason) =~ "noncanonical_segment_filename"
    after
      WARaftBackend.stop()
      restore_env(:waraft_segment_log_records_per_segment, previous)
    end
  end

  test "segment log rejects non-canonical numeric segment filenames", %{
    root: root,
    ctx: ctx
  } do
    previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      assert :ok = WARaftBackend.write(0, {:put, "log-noncanonical-segment:k", "v", 0})
      WARaftBackend.stop()

      segment_dir = waraft_segment_log_dir(root, 0)
      canonical = Path.join(segment_dir, "1.seg")
      noncanonical = Path.join(segment_dir, "01.seg")

      assert File.exists?(canonical)
      File.rename!(canonical, noncanonical)

      assert {:error, reason} =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert inspect(reason) =~ "noncanonical_segment_filename"
    after
      WARaftBackend.stop()
      restore_env(:waraft_segment_log_records_per_segment, previous)
    end
  end

  test "segment log fails closed when pending rewrite marker is oversized", %{
    root: root,
    ctx: ctx
  } do
    assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
    assert :ok = WARaftBackend.write(0, {:put, "log-rewrite-marker-oversized:k", "v", 0})
    WARaftBackend.stop()

    segment_dir = waraft_segment_log_dir(root, 0)
    log_root = Path.dirname(segment_dir)
    staging = Path.join(log_root, ".rewrite.staging.too_large")
    backup = Path.join(log_root, ".rewrite.backup.too_large")
    marker_path = Path.join(log_root, "segment_log.rewrite.term")

    marker = %{
      version: 1,
      dir: String.to_charlist(segment_dir),
      staging: String.to_charlist(staging),
      backup: String.to_charlist(backup),
      label: :binary.copy("x", 1_048_576)
    }

    File.write!(marker_path, :erlang.term_to_binary(marker))

    assert {:error, reason} =
             WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert inspect(reason) =~ "rewrite_marker_file_too_large"
    assert File.exists?(marker_path)
  after
    WARaftBackend.stop()
  end

  test "segment log rejects pending rewrite marker with symlink backup", %{
    root: root,
    ctx: ctx
  } do
    assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
    assert :ok = WARaftBackend.write(0, {:put, "log-rewrite-marker-symlink:k", "v", 0})
    WARaftBackend.stop()

    segment_dir = waraft_segment_log_dir(root, 0)
    log_root = Path.dirname(segment_dir)
    staging = Path.join(log_root, "segment_log.rewrite.staging.symlink")
    backup = Path.join(log_root, "segment_log.rewrite.backup.symlink")
    marker_path = Path.join(log_root, "segment_log.rewrite.term")
    outside_backup = Path.join(root, "outside-rewrite-backup")

    File.mkdir_p!(staging)
    File.mkdir_p!(outside_backup)
    assert :ok = File.ln_s(outside_backup, backup)

    marker = %{
      version: 1,
      dir: String.to_charlist(segment_dir),
      staging: String.to_charlist(staging),
      backup: String.to_charlist(backup)
    }

    File.write!(marker_path, :erlang.term_to_binary(marker))

    assert {:error, reason} =
             WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert inspect(reason) =~ "unsafe_rewrite_path"
    assert {:ok, %{type: :directory}} = File.lstat(segment_dir)
    assert File.exists?(marker_path)
  after
    WARaftBackend.stop()
  end

  test "segment log startup errors are returned without MatchError noise", %{
    root: root,
    ctx: ctx
  } do
    assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
    assert :ok = WARaftBackend.write(0, {:put, "log-open-error-noise:k", "v", 0})
    WARaftBackend.stop()

    segment_dir = waraft_segment_log_dir(root, 0)
    log_root = Path.dirname(segment_dir)
    staging = Path.join(log_root, "segment_log.rewrite.staging.noise")
    backup = Path.join(log_root, "segment_log.rewrite.backup.noise")
    marker_path = Path.join(log_root, "segment_log.rewrite.term")
    outside_backup = Path.join(root, "outside-rewrite-backup-noise")

    File.mkdir_p!(staging)
    File.mkdir_p!(outside_backup)
    assert :ok = File.ln_s(outside_backup, backup)

    marker = %{
      version: 1,
      dir: String.to_charlist(segment_dir),
      staging: String.to_charlist(staging),
      backup: String.to_charlist(backup)
    }

    File.write!(marker_path, :erlang.term_to_binary(marker))

    logs =
      capture_log(fn ->
        assert {:error, reason} =
                 WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        assert inspect(reason) =~ "unsafe_rewrite_path"
        Process.sleep(50)
      end)

    refute logs =~ "MatchError"
    refute logs =~ "no match of right hand side value"
  after
    WARaftBackend.stop()
  end

  test "segment log fails closed when segment sizing metadata is missing for existing segments",
       %{root: root, ctx: ctx} do
    previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      assert :ok = WARaftBackend.write(0, {:put, "log-config-missing:k", "v", 0})
      WARaftBackend.stop()

      root
      |> waraft_segment_log_dir(0)
      |> Path.join("segment_config.term")
      |> File.rm!()

      assert {:error, _reason} =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
    after
      WARaftBackend.stop()
      restore_env(:waraft_segment_log_records_per_segment, previous)
    end
  end

  test "single-member WARaft batches concurrent commits before segment fsync", %{ctx: ctx} do
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)

    try do
      assert :ok =
               WARaftBackend.start(ctx,
                 log_module: :ferricstore_waraft_spike_segment_log,
                 commit_batch_interval_ms: 50,
                 commit_batch_max: 10
               )

      Application.put_env(:ferricstore, :waraft_segment_log_file_sync_hook, {:notify, self()})

      tasks =
        for i <- 1..2 do
          Task.async(fn ->
            WARaftBackend.write(0, {:put, "single-member-batch:k#{i}", "v#{i}", 0})
          end)
        end

      assert [:ok, :ok] = Enum.map(tasks, &Task.await(&1, 5_000))

      assert_receive {:waraft_segment_log_file_sync, path}, 1_000
      refute_receive {:waraft_segment_log_file_sync, _path}, 100
      assert String.ends_with?(path, ".seg")
      assert "v1" == Router.get(ctx, "single-member-batch:k1")
      assert "v2" == Router.get(ctx, "single-member-batch:k2")
    after
      restore_env(:waraft_segment_log_file_sync_hook, previous_hook)
    end
  end

  test "default WARaft hot put batches coalesce before storage fsync", %{ctx: ctx} do
    previous_hook = Application.get_env(:ferricstore, :standalone_durability_hook)
    previous_window = Application.get_env(:ferricstore, :waraft_hot_batch_window_ms)

    try do
      Application.put_env(:ferricstore, :waraft_hot_batch_window_ms, 25)

      assert :ok =
               WARaftBackend.start(ctx,
                 log_module: :wa_raft_log_ets,
                 commit_batch_interval_ms: 1,
                 commit_batch_max: 10_000
               )

      parent = self()

      Application.put_env(:ferricstore, :standalone_durability_hook, fn _file_path, batch ->
        send(parent, {:waraft_hot_put_storage_batch, batch})
        :passthrough
      end)

      tasks =
        for i <- 1..2 do
          Task.async(fn ->
            WARaftBackend.write_put_batch(0, [{"hot-put-batch:#{i}", "v#{i}", 0}])
          end)
        end

      assert [{:ok, [:ok]}, {:ok, [:ok]}] = Enum.map(tasks, &Task.await(&1, 5_000))

      assert_receive {:waraft_hot_put_storage_batch, batch}, 1_000
      keys = Enum.map(batch, fn {:put, key, _value, _expire_at_ms} -> key end)
      assert "hot-put-batch:1" in keys
      assert "hot-put-batch:2" in keys
      refute_receive {:waraft_hot_put_storage_batch, _batch}, 100
    after
      restore_env(:standalone_durability_hook, previous_hook)
      restore_env(:waraft_hot_batch_window_ms, previous_window)
    end
  end

  test "Router multi-shard WARaft put batches still use hot per-shard coalescing", %{
    root: root
  } do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    previous_hook = Application.get_env(:ferricstore, :standalone_durability_hook)
    previous_window = Application.get_env(:ferricstore, :waraft_hot_batch_window_ms)
    multi_root = Path.join(root, "router-multishard-hot-put")
    Ferricstore.DataDir.ensure_layout!(multi_root, 2)
    Ferricstore.Store.ActiveFile.init(2)
    multi_ctx = build_ctx(multi_root, shard_count: 2)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      Application.put_env(:ferricstore, :waraft_hot_batch_window_ms, 25)

      assert :ok =
               WARaftBackend.start(multi_ctx,
                 log_module: :wa_raft_log_ets,
                 commit_batch_interval_ms: 1,
                 commit_batch_max: 10_000
               )

      k0a = key_for_shard(multi_ctx, 0, "router-hot-put-a")
      k0b = key_for_shard(multi_ctx, 0, "router-hot-put-b")
      k1a = key_for_shard(multi_ctx, 1, "router-hot-put-a")
      k1b = key_for_shard(multi_ctx, 1, "router-hot-put-b")

      parent = self()

      Application.put_env(:ferricstore, :standalone_durability_hook, fn _file_path, batch ->
        send(parent, {:waraft_router_hot_put_storage_batch, batch})
        :passthrough
      end)

      tasks = [
        Task.async(fn ->
          Router.__forwarded_batch_quorum_put_entries__(
            multi_ctx,
            [{k0a, "v0a", 0}, {k1a, "v1a", 0}],
            nil
          )
        end),
        Task.async(fn ->
          Router.__forwarded_batch_quorum_put_entries__(
            multi_ctx,
            [{k0b, "v0b", 0}, {k1b, "v1b", 0}],
            nil
          )
        end)
      ]

      assert [[:ok, :ok], [:ok, :ok]] = Enum.map(tasks, &Task.await(&1, 5_000))

      batches =
        for _ <- 1..2 do
          assert_receive {:waraft_router_hot_put_storage_batch, batch}, 1_000
          batch
        end

      refute_receive {:waraft_router_hot_put_storage_batch, _batch}, 100

      key_sets =
        Enum.map(batches, fn batch ->
          batch
          |> Enum.map(fn {:put, key, _value, _expire_at_ms} -> key end)
          |> MapSet.new()
        end)

      assert MapSet.new([k0a, k0b]) in key_sets
      assert MapSet.new([k1a, k1b]) in key_sets

      assert "v0a" == Router.get(multi_ctx, k0a)
      assert "v0b" == Router.get(multi_ctx, k0b)
      assert "v1a" == Router.get(multi_ctx, k1a)
      assert "v1b" == Router.get(multi_ctx, k1b)
    after
      restore_backend(previous_backend)
      restore_env(:standalone_durability_hook, previous_hook)
      restore_env(:waraft_hot_batch_window_ms, previous_window)
      FerricStore.Instance.cleanup(multi_ctx.name)
      File.rm_rf!(multi_root)
    end
  end

  test "default WARaft hot delete batches coalesce before storage fsync", %{ctx: ctx} do
    previous_hook = Application.get_env(:ferricstore, :standalone_durability_hook)
    previous_window = Application.get_env(:ferricstore, :waraft_hot_batch_window_ms)

    try do
      Application.put_env(:ferricstore, :waraft_hot_batch_window_ms, 25)

      assert :ok =
               WARaftBackend.start(ctx,
                 log_module: :wa_raft_log_ets,
                 commit_batch_interval_ms: 1,
                 commit_batch_max: 10_000
               )

      assert {:ok, [:ok, :ok]} =
               WARaftBackend.write_put_batch(0, [
                 {"hot-delete-batch:1", "v1", 0},
                 {"hot-delete-batch:2", "v2", 0}
               ])

      assert "v1" == Router.get(ctx, "hot-delete-batch:1")
      assert "v2" == Router.get(ctx, "hot-delete-batch:2")

      parent = self()

      Application.put_env(:ferricstore, :standalone_durability_hook, fn _file_path, batch ->
        send(parent, {:waraft_hot_delete_storage_batch, batch})
        :passthrough
      end)

      tasks =
        for i <- 1..2 do
          Task.async(fn ->
            WARaftBackend.write_delete_batch(0, ["hot-delete-batch:#{i}"])
          end)
        end

      assert [{:ok, [:ok]}, {:ok, [:ok]}] = Enum.map(tasks, &Task.await(&1, 5_000))

      assert_receive {:waraft_hot_delete_storage_batch, batch}, 1_000
      keys = Enum.map(batch, fn {:delete, key, _prob_path} -> key end)
      assert "hot-delete-batch:1" in keys
      assert "hot-delete-batch:2" in keys
      refute_receive {:waraft_hot_delete_storage_batch, _batch}, 100

      assert nil == Router.get(ctx, "hot-delete-batch:1")
      assert nil == Router.get(ctx, "hot-delete-batch:2")
    after
      restore_env(:standalone_durability_hook, previous_hook)
      restore_env(:waraft_hot_batch_window_ms, previous_window)
    end
  end

  test "single-member WARaft commit batch timer is anchored under continuous load", %{
    ctx: ctx
  } do
    assert :ok =
             WARaftBackend.start(ctx,
               log_module: :wa_raft_log_ets,
               commit_batch_interval_ms: 30,
               commit_batch_max: 10_000
             )

    acceptor = :wa_raft_acceptor.registered_name(:ferricstore_waraft_backend, 1)
    first_ref = make_ref()
    started_ms = System.monotonic_time(:millisecond)

    :ok = raw_waraft_async_put(acceptor, first_ref, "batch-timer:first", "v0")

    producer =
      Task.async(fn ->
        for i <- 1..80 do
          Process.sleep(2)
          :ok = raw_waraft_async_put(acceptor, make_ref(), "batch-timer:#{i}", "v#{i}")
        end
      end)

    assert_receive {^first_ref, :ok}, 120
    assert System.monotonic_time(:millisecond) - started_ms < 120
    assert List.duplicate(:ok, 80) == Task.await(producer, 5_000)
    assert_eventually(fn -> Router.get(ctx, "batch-timer:first") end, "v0")
  end

  test "segment log append emits success telemetry with record count and bytes", %{ctx: ctx} do
    parent = self()
    handler_id = {__MODULE__, :segment_log_append_success, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :waraft, :segment_log, :append],
      &__MODULE__.handle_segment_log_telemetry/4,
      parent
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert :ok =
             WARaftBackend.start(ctx,
               log_module: :ferricstore_waraft_spike_segment_log
             )

    assert {:ok, [:ok, :ok]} =
             WARaftBackend.write_put_batch(0, [
               {"segment-log-telemetry:k1", "v1", 0},
               {"segment-log-telemetry:k2", "v2", 0}
             ])

    assert_receive {:waraft_segment_log_telemetry, [:ferricstore, :waraft, :segment_log, :append],
                    %{count: count, bytes: bytes, duration: duration},
                    %{path: path, result: :ok, new_segment: new_segment}},
                   1_000

    assert count >= 1
    assert bytes > 0
    assert duration >= 0
    assert String.ends_with?(path, ".seg")
    assert is_boolean(new_segment)
  end

  test "segment log append emits error telemetry on fsync failure", %{ctx: ctx} do
    parent = self()
    handler_id = {__MODULE__, :segment_log_append_error, make_ref()}
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)

    :telemetry.attach(
      handler_id,
      [:ferricstore, :waraft, :segment_log, :append],
      &__MODULE__.handle_segment_log_telemetry/4,
      parent
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    try do
      assert :ok =
               WARaftBackend.start(ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      Application.put_env(:ferricstore, :waraft_segment_log_file_sync_hook, {:fail_once, self()})

      assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
               WARaftBackend.write(0, {:put, "segment-log-telemetry-fail:k", "v1", 0})

      assert_receive {:waraft_segment_log_telemetry,
                      [:ferricstore, :waraft, :segment_log, :append],
                      %{count: count, bytes: bytes, duration: duration},
                      %{
                        path: path,
                        result: :error,
                        reason: {_, _}
                      }},
                     1_000

      assert count >= 1
      assert bytes > 0
      assert duration >= 0
      assert String.ends_with?(path, ".seg")
    after
      restore_env(:waraft_segment_log_file_sync_hook, previous_hook)
    end
  end

  test "Router WARaft batch put submits shard groups concurrently", %{root: root} do
    Ferricstore.DataDir.ensure_layout!(root, 2)
    Ferricstore.Store.ActiveFile.init(2)
    ctx = build_ctx(root, shard_count: 2)

    on_exit(fn ->
      FerricStore.Instance.cleanup(ctx.name)
    end)

    assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

    key0 = key_for_shard(ctx, 0, "waraft-parallel-batch")
    key1 = key_for_shard(ctx, 1, "waraft-parallel-batch")
    previous_hook = Application.get_env(:ferricstore, :standalone_durability_hook)
    previous_backend = Application.get_env(:ferricstore, :raft_backend)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      Application.put_env(:ferricstore, :standalone_durability_hook, fn _file_path, batch ->
        if Enum.any?(batch, &delayed_parallel_batch_record?/1), do: Process.sleep(200)
        :passthrough
      end)

      started_ms = System.monotonic_time(:millisecond)
      assert [:ok, :ok] = Router.batch_quorum_put(ctx, [{key0, "v0"}, {key1, "v1"}])
      elapsed_ms = System.monotonic_time(:millisecond) - started_ms

      assert elapsed_ms < 320
      assert "v0" == Router.get(ctx, key0)
      assert "v1" == Router.get(ctx, key1)
    after
      restore_env(:standalone_durability_hook, previous_hook)
      restore_backend(previous_backend)
    end
  end

  test "storage apply failure blocks later positions until restart", %{root: root, ctx: ctx} do
    assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
    assert {:ok, pre_write_position} = WARaftBackend.storage_position(0)
    previous_hook = Application.get_env(:ferricstore, :standalone_durability_hook)

    try do
      Application.put_env(:ferricstore, :standalone_durability_hook, fn _file_path, batch ->
        if Enum.any?(batch, &match?({:put, "apply-block:k", _value, _expire_at_ms}, &1)) do
          {:error, :forced_bitcask_failure}
        else
          :passthrough
        end
      end)

      assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
               WARaftBackend.write(0, {:put, "apply-block:k", "v1", 0})

      restore_env(:standalone_durability_hook, previous_hook)

      assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
               WARaftBackend.write(0, {:put, "apply-block:after", "v2", 0})

      assert {:ok, ^pre_write_position} = WARaftBackend.storage_position(0)
      assert nil == Router.get(ctx, "apply-block:k")
      assert nil == Router.get(ctx, "apply-block:after")

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert_eventually(fn -> Router.get(restarted_ctx, "apply-block:k") end, "v1")
      assert_eventually(fn -> Router.get(restarted_ctx, "apply-block:after") end, "v2")
    after
      restore_env(:standalone_durability_hook, previous_hook)
    end
  end

  test "storage blocked state emits telemetry for first failure and later rejected applies", %{
    ctx: ctx
  } do
    assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
    parent = self()
    handler_id = {__MODULE__, :storage_blocked, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :waraft, :storage_blocked],
      &__MODULE__.handle_test_telemetry/4,
      parent
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    previous_hook = Application.get_env(:ferricstore, :standalone_durability_hook)

    try do
      Application.put_env(:ferricstore, :standalone_durability_hook, fn _file_path, batch ->
        if Enum.any?(batch, &match?({:put, "telemetry-block:k", _value, _expire_at_ms}, &1)) do
          {:error, :forced_bitcask_failure}
        else
          :passthrough
        end
      end)

      assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
               WARaftBackend.write(0, {:put, "telemetry-block:k", "v1", 0})

      assert_receive {:waraft_storage_blocked, [:ferricstore, :waraft, :storage_blocked],
                      %{count: 1},
                      %{
                        operation: :apply_failure,
                        reason: {:bitcask_append_failed, :forced_bitcask_failure},
                        shard_index: 0
                      }}

      restore_env(:standalone_durability_hook, previous_hook)

      assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
               WARaftBackend.write(0, {:put, "telemetry-block:after", "v2", 0})

      assert_receive {:waraft_storage_blocked, [:ferricstore, :waraft, :storage_blocked],
                      %{count: 1},
                      %{
                        operation: :blocked_apply,
                        reason: {:bitcask_append_failed, :forced_bitcask_failure},
                        shard_index: 0
                      }}
    after
      restore_env(:standalone_durability_hook, previous_hook)
    end
  end

  test "blocked storage refuses snapshot creation instead of exporting a newer volatile position",
       %{
         ctx: ctx
       } do
    assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
    previous_hook = Application.get_env(:ferricstore, :standalone_durability_hook)

    try do
      Application.put_env(:ferricstore, :standalone_durability_hook, fn _file_path, batch ->
        if Enum.any?(batch, &match?({:put, "snapshot-block:k", _value, _expire_at_ms}, &1)) do
          {:error, :forced_bitcask_failure}
        else
          :passthrough
        end
      end)

      assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
               WARaftBackend.write(0, {:put, "snapshot-block:k", "v1", 0})

      assert {:error, {:storage_blocked, {:bitcask_append_failed, :forced_bitcask_failure}}} =
               WARaftBackend.create_snapshot(0)
    after
      restore_env(:standalone_durability_hook, previous_hook)
    end
  end

  test "deterministic command errors still advance WARaft storage replay position", %{ctx: ctx} do
    assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
    assert {:ok, {:raft_log_pos, pre_index, _pre_term}} = WARaftBackend.storage_position(0)

    bad_command = {:unknown_for_replay_position_test, "k"}

    assert {:error, {:unknown_command, ^bad_command}} = WARaftBackend.write(0, bad_command)
    assert {:ok, {:raft_log_pos, post_index, _post_term}} = WARaftBackend.storage_position(0)
    assert post_index > pre_index
  end

  test "storage metadata hot writes fsync journal without rewriting current metadata", %{ctx: ctx} do
    test_pid = self()
    previous_hook = Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)
    previous_every = Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)

    try do
      Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 1)

      Application.put_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook, fn path ->
        send(test_pid, {:storage_metadata_fsync, path})
        :ok
      end)

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      drain_storage_metadata_fsyncs()

      assert :ok = WARaftBackend.write(0, {:put, "metadata-fsync:k", "v", 0})

      assert_receive {:storage_metadata_fsync, path}, 1_000
      paths = [path | collect_storage_metadata_fsyncs()]

      assert Enum.any?(paths, &String.ends_with?(&1, "ferricstore_storage.term.journal"))
      refute Enum.any?(paths, &String.contains?(&1, "ferricstore_storage.term.tmp."))
    after
      restore_env(:waraft_storage_metadata_fsync_file_hook, previous_hook)
      restore_env(:waraft_storage_metadata_persist_every, previous_every)
    end
  end

  test "storage metadata journal symlink does not append outside shard root", %{
    root: root,
    ctx: ctx
  } do
    test_pid = self()
    previous_hook = Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)
    previous_every = Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)

    try do
      Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 1)

      Application.put_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook, fn path ->
        send(test_pid, {:storage_metadata_fsync, path})
        :ok
      end)

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      assert :ok = WARaftBackend.write(0, {:put, "metadata-journal-symlink:warmup", "v", 0})
      _ = collect_storage_metadata_fsyncs()

      journal_path = waraft_storage_metadata_journal_path(root, 0)
      outside_path = Path.join(root, "outside-storage-metadata-journal")
      sentinel = "outside-sentinel"

      File.rm(journal_path)
      File.write!(outside_path, sentinel)
      assert :ok = File.ln_s(outside_path, journal_path)
      assert {:ok, %{type: :symlink}} = File.lstat(journal_path)

      assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
               WARaftBackend.write(0, {:put, "metadata-journal-symlink:k", "v", 0})

      assert File.read!(outside_path) == sentinel
    after
      restore_env(:waraft_storage_metadata_fsync_file_hook, previous_hook)
      restore_env(:waraft_storage_metadata_persist_every, previous_every)
    end
  end

  test "storage metadata journal recovery streams records instead of reading whole journal" do
    source =
      Path.expand("../../../lib/ferricstore/raft/waraft_storage.ex", __DIR__)
      |> File.read!()

    refute source =~ "File.read(journal_path)",
           "metadata journal recovery must not materialize the full journal in BEAM memory"

    assert source =~ "File.open(journal_path, [:read, :binary])"
    assert source =~ "read_metadata_journal_record"
  end

  test "restart fails closed on oversized storage metadata journal record", %{
    root: root,
    ctx: ctx
  } do
    assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
    assert :ok = WARaftBackend.write(0, {:put, "metadata:oversized-journal", "value", 0})
    assert "value" == Router.get(ctx, "metadata:oversized-journal")

    metadata =
      root
      |> waraft_latest_storage_metadata(0)
      |> Map.put(:label, :binary.copy("x", 2 * 1024 * 1024))

    assert :ok = WARaftBackend.stop()

    payload = :erlang.term_to_binary(metadata)
    record = <<"FSMJ1", byte_size(payload)::32, :erlang.crc32(payload)::32, payload::binary>>

    File.write!(waraft_storage_metadata_path(root, 0), <<131, 116, 0, 0, 0, 3, "torn">>)

    File.write!(
      waraft_storage_metadata_previous_path(root, 0),
      <<131, 116, 0, 0, 0, 3, "also-torn">>
    )

    File.write!(waraft_storage_metadata_journal_path(root, 0), record)

    FerricStore.Instance.cleanup(ctx.name)
    restarted_ctx = build_ctx(root)

    assert {:error, reason} =
             WARaftBackend.start(restarted_ctx,
               log_module: :ferricstore_waraft_spike_segment_log
             )

    assert inspect(reason) =~ "metadata_journal_record_too_large"
    assert shard_payload_present?(restarted_ctx, 0)
  end

  test "restart fails closed on oversized current storage metadata file", %{
    root: root,
    ctx: ctx
  } do
    assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
    assert :ok = WARaftBackend.write(0, {:put, "metadata:oversized-current", "value", 0})
    assert "value" == Router.get(ctx, "metadata:oversized-current")

    metadata =
      root
      |> waraft_latest_storage_metadata(0)
      |> Map.put(:label, :binary.copy("x", 2 * 1024 * 1024))

    assert :ok = WARaftBackend.stop()

    File.write!(waraft_storage_metadata_path(root, 0), :erlang.term_to_binary(metadata))
    File.rm(waraft_storage_metadata_previous_path(root, 0))
    File.rm(waraft_storage_metadata_journal_path(root, 0))

    FerricStore.Instance.cleanup(ctx.name)
    restarted_ctx = build_ctx(root)

    assert {:error, reason} =
             WARaftBackend.start(restarted_ctx,
               log_module: :ferricstore_waraft_spike_segment_log
             )

    assert inspect(reason) =~ "storage_metadata_file_too_large"
    assert shard_payload_present?(restarted_ctx, 0)
  end

  test "storage metadata hot persistence can lag without blocking acknowledged replay", %{
    root: root,
    ctx: ctx
  } do
    test_pid = self()
    previous_hook = Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)
    previous_every = Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)

    try do
      Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 128)

      Application.put_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook, fn path ->
        send(test_pid, {:storage_metadata_fsync, path})
        :ok
      end)

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      drain_storage_metadata_fsyncs()
      assert {:ok, persisted_before} = WARaftBackend.storage_position(0)

      assert :ok = WARaftBackend.write(0, {:put, "metadata-lag:k1", "v1", 0})
      assert :ok = WARaftBackend.write(0, {:put, "metadata-lag:k2", "v2", 0})

      assert {:ok, {:raft_log_pos, live_index, _live_term}} = WARaftBackend.storage_position(0)
      assert {:raft_log_pos, persisted_index, _persisted_term} = persisted_before
      assert live_index > persisted_index
      refute_receive {:storage_metadata_fsync, _path}, 100

      assert :ok = WARaftBackend.stop()
      rewind_waraft_storage_position!(root, 0, persisted_before)
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert_eventually(fn -> Router.get(restarted_ctx, "metadata-lag:k1") end, "v1")
      assert_eventually(fn -> Router.get(restarted_ctx, "metadata-lag:k2") end, "v2")
    after
      restore_env(:waraft_storage_metadata_fsync_file_hook, previous_hook)
      restore_env(:waraft_storage_metadata_persist_every, previous_every)
    end
  end

  test "experimental WARaft replay-safe no-sync apply bypasses standalone Bitcask fsync hook",
       %{ctx: ctx} do
    parent = self()
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)
    previous_hook = Application.get_env(:ferricstore, :standalone_durability_hook)

    previous_metadata_hook =
      Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :replay_safe_nosync)

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
      restore_env(:waraft_storage_apply_mode, previous_mode)
      restore_env(:standalone_durability_hook, previous_hook)
      restore_env(:waraft_storage_metadata_fsync_file_hook, previous_metadata_hook)
    end
  end

  test "experimental WARaft replay-safe no-sync apply fsyncs payload before closing metadata",
       %{root: root, ctx: ctx} do
    parent = self()
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)
    previous_hook = Application.get_env(:ferricstore, :waraft_bitcask_payload_fsync_file_hook)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :replay_safe_nosync)

      Application.put_env(:ferricstore, :waraft_bitcask_payload_fsync_file_hook, fn path ->
        send(parent, {:waraft_payload_fsync, path})
        :ok
      end)

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      assert :ok = WARaftBackend.write(0, {:put, "nosync-close:k", "v", 0})
      assert "v" == Router.get(ctx, "nosync-close:k")

      assert :ok = WARaftBackend.stop()
      assert_receive {:waraft_payload_fsync, path}, 1_000
      assert String.ends_with?(path, ".log")

      FerricStore.Instance.cleanup(ctx.name)
      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert_eventually(fn -> Router.get(restarted_ctx, "nosync-close:k") end, "v")
    after
      restore_env(:waraft_storage_apply_mode, previous_mode)
      restore_env(:waraft_bitcask_payload_fsync_file_hook, previous_hook)
    end
  end

  test "experimental WARaft replay-safe no-sync apply advances metadata after payload fsync frontier",
       %{root: root, ctx: ctx} do
    parent = self()
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)
    previous_frontier = Application.get_env(:ferricstore, :waraft_bitcask_payload_fsync_every)

    previous_payload_hook =
      Application.get_env(:ferricstore, :waraft_bitcask_payload_fsync_file_hook)

    previous_metadata_hook =
      Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :replay_safe_nosync)
      Application.put_env(:ferricstore, :waraft_bitcask_payload_fsync_every, 2)

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
      assert_receive {:waraft_payload_fsync, payload_path}, 1_000
      assert String.ends_with?(payload_path, ".log")
      assert_receive {:storage_metadata_fsync, metadata_path}, 1_000
      assert String.ends_with?(metadata_path, "ferricstore_storage.term.journal")

      assert %{position: {:raft_log_pos, durable_index, _term}} =
               waraft_latest_storage_metadata(root, 0)

      assert durable_index >= 4

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)
      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert_eventually(fn -> Router.get(restarted_ctx, "nosync-frontier:k1") end, "v1")
      assert_eventually(fn -> Router.get(restarted_ctx, "nosync-frontier:k2") end, "v2")
    after
      restore_env(:waraft_storage_apply_mode, previous_mode)
      restore_env(:waraft_bitcask_payload_fsync_every, previous_frontier)
      restore_env(:waraft_bitcask_payload_fsync_file_hook, previous_payload_hook)
      restore_env(:waraft_storage_metadata_fsync_file_hook, previous_metadata_hook)
    end
  end

  test "experimental WARaft replay-safe no-sync status reports persisted durable position",
       %{ctx: ctx} do
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)
    previous_frontier = Application.get_env(:ferricstore, :waraft_bitcask_payload_fsync_every)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :replay_safe_nosync)
      Application.put_env(:ferricstore, :waraft_bitcask_payload_fsync_every, :never)

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      assert {:ok, pre_write_position} = WARaftBackend.storage_position(0)

      assert :ok = WARaftBackend.write(0, {:put, "nosync-status:k", "v", 0})
      assert {:ok, post_write_position} = WARaftBackend.storage_position(0)
      assert post_write_position != pre_write_position

      status = waraft_storage_status(0)

      assert Keyword.fetch!(status, :applied_position) == post_write_position
      assert Keyword.fetch!(status, :durable_position) == pre_write_position
      assert Keyword.fetch!(status, :payload_dirty?) == true
    after
      restore_env(:waraft_storage_apply_mode, previous_mode)
      restore_env(:waraft_bitcask_payload_fsync_every, previous_frontier)
    end
  end

  test "experimental WARaft replay-safe no-sync fsyncs dirty payload before config metadata",
       %{ctx: ctx} do
    parent = self()
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)
    previous_frontier = Application.get_env(:ferricstore, :waraft_bitcask_payload_fsync_every)

    previous_payload_hook =
      Application.get_env(:ferricstore, :waraft_bitcask_payload_fsync_file_hook)

    previous_metadata_hook =
      Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :replay_safe_nosync)
      Application.put_env(:ferricstore, :waraft_bitcask_payload_fsync_every, :never)

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

      assert_receive {:waraft_payload_fsync, payload_path}, 1_000
      assert String.ends_with?(payload_path, ".log")
      assert_receive {:storage_metadata_fsync, metadata_path}, 1_000
      assert String.contains?(metadata_path, "ferricstore_storage.term")

      status = waraft_storage_status(0)
      assert Keyword.fetch!(status, :payload_dirty?) == false
    after
      restore_env(:waraft_storage_apply_mode, previous_mode)
      restore_env(:waraft_bitcask_payload_fsync_every, previous_frontier)
      restore_env(:waraft_bitcask_payload_fsync_file_hook, previous_payload_hook)
      restore_env(:waraft_storage_metadata_fsync_file_hook, previous_metadata_hook)
    end
  end

  test "experimental WARaft replay-safe no-sync emits telemetry for payload frontier fsync",
       %{ctx: ctx} do
    parent = self()
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)
    previous_frontier = Application.get_env(:ferricstore, :waraft_bitcask_payload_fsync_every)
    handler_id = {__MODULE__, :waraft_payload_fsync_frontier, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :waraft, :storage, :payload_fsync],
      &__MODULE__.handle_payload_fsync_telemetry/4,
      parent
    )

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :replay_safe_nosync)
      Application.put_env(:ferricstore, :waraft_bitcask_payload_fsync_every, 2)

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      assert :ok = WARaftBackend.write(0, {:put, "nosync-telemetry:k1", "v1", 0})
      refute_receive {:waraft_payload_fsync_telemetry, _event, _measurements, _metadata}, 100

      assert :ok = WARaftBackend.write(0, {:put, "nosync-telemetry:k2", "v2", 0})

      assert_receive {:waraft_payload_fsync_telemetry,
                      [:ferricstore, :waraft, :storage, :payload_fsync], measurements, metadata},
                     1_000

      assert %{count: 1, duration: duration} = measurements
      assert is_integer(duration) and duration >= 0
      assert %{shard_index: 0, result: :ok, position: {:raft_log_pos, _index, _term}} = metadata
    after
      :telemetry.detach(handler_id)
      restore_env(:waraft_storage_apply_mode, previous_mode)
      restore_env(:waraft_bitcask_payload_fsync_every, previous_frontier)
    end
  end

  test "experimental WARaft replay-safe no-sync deterministic no-ops do not dirty payload",
       %{ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)
    previous_frontier = Application.get_env(:ferricstore, :waraft_bitcask_payload_fsync_every)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :replay_safe_nosync)
      Application.put_env(:ferricstore, :waraft_bitcask_payload_fsync_every, 2)

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      assert :ok = WARaftBackend.write(0, {:put, "nosync-wrongtype:k", "v", 0})
      assert :ok = WARaftBackend.write(0, {:put, "nosync-wrongtype:flush", "v", 0})
      assert Keyword.fetch!(waraft_storage_status(0), :payload_dirty?) == false

      Application.put_env(:ferricstore, :waraft_bitcask_payload_fsync_every, :never)

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
    after
      restore_backend(previous_backend)
      restore_env(:waraft_storage_apply_mode, previous_mode)
      restore_env(:waraft_bitcask_payload_fsync_every, previous_frontier)
    end
  end

  test "experimental WARaft replay-safe no-sync snapshot exports dirty payload without source fsync",
       %{root: root, ctx: ctx} do
    parent = self()
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)
    previous_frontier = Application.get_env(:ferricstore, :waraft_bitcask_payload_fsync_every)

    previous_payload_hook =
      Application.get_env(:ferricstore, :waraft_bitcask_payload_fsync_file_hook)

    previous_snapshot_hook = Application.get_env(:ferricstore, :waraft_snapshot_fsync_file_hook)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :replay_safe_nosync)
      Application.put_env(:ferricstore, :waraft_bitcask_payload_fsync_every, :never)

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
      assert Keyword.fetch!(waraft_storage_status(0), :payload_dirty?) == true

      assert {:ok, {:raft_log_pos, index, term} = position} = WARaftBackend.create_snapshot(0)

      snapshot_path =
        Path.join([
          root,
          "waraft",
          "ferricstore_waraft_backend.1",
          "snapshot.#{index}.#{term}"
        ])

      refute_receive {:source_payload_fsync, _path}, 100
      assert_receive {:snapshot_payload_fsync, snapshot_payload_path}, 1_000
      assert String.starts_with?(snapshot_payload_path, snapshot_path)

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
      restore_env(:waraft_storage_apply_mode, previous_mode)
      restore_env(:waraft_bitcask_payload_fsync_every, previous_frontier)
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
    previous_hook = Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)
    previous_every = Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)

    try do
      Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 1)

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
    end
  end

  test "storage metadata directory fsync failure does not advance replay position", %{
    ctx: ctx
  } do
    assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
    assert {:ok, pre_write_position} = WARaftBackend.storage_position(0)
    previous_hook = Application.get_env(:ferricstore, :waraft_storage_fsync_dir_hook)
    previous_every = Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)

    previous_compact_every =
      Application.get_env(:ferricstore, :waraft_storage_metadata_compact_every)

    try do
      Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 1)
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
      restore_env(:waraft_storage_metadata_compact_every, previous_compact_every)
    end
  end

  test "membership metadata fsync failure returns unknown outcome and replays after restart", %{
    root: root,
    ctx: ctx
  } do
    assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
    previous_hook = Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)

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
                   participants = Map.get(config, :participants, Map.get(config, :membership, []))
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

    try do
      Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 1)

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
    end
  end

  test "oversized storage label fails before persisting metadata", %{root: root, ctx: ctx} do
    previous_every = Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)

    try do
      Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 1)

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
    end
  end

  test "metadata fsync failure does not advance storage label", %{root: root, ctx: ctx} do
    previous_every = Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)
    previous_hook = Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)

    try do
      Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 1)

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

    assert File.exists?(waraft_storage_metadata_journal_path(root, 0))
  end

  test "restart recovers from torn current storage metadata using previous durable metadata", %{
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

    assert_eventually(fn -> Router.get(restarted_ctx, "metadata:missing-current:after") end, "v2")
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
    assert %{position: {:raft_log_pos, expected_index, _term}} = waraft_storage_metadata(root, 0)
    assert expected_index >= 2

    File.write!(
      metadata_path,
      :erlang.term_to_binary(%{
        version: 1,
        position: {:raft_log_pos, 0, 0},
        label: nil,
        config: nil
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
    assert File.exists?(waraft_storage_metadata_journal_path(root, 0))
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
    assert :ok = File.rm(waraft_storage_metadata_journal_path(root, 0))
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

    File.rm!(waraft_storage_metadata_path(root, 0))
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

    for kind <- ["blob", "dedicated", "prob"] do
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
    assert {:ok, %{type: :directory}} = File.lstat(Ferricstore.DataDir.shard_data_path(root, 0))
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

  test "restart fails closed on storage metadata with invalid position", %{root: root, ctx: ctx} do
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

  test "snapshot install restores real Bitcask state into a stalled backend member", %{root: root} do
    source_root = Path.join(root, "source")
    target_root = Path.join(root, "target")
    File.mkdir_p!(source_root)
    File.mkdir_p!(target_root)

    source_ctx = build_ctx(source_root)

    assert :ok =
             WARaftBackend.start(source_ctx, log_module: :ferricstore_waraft_spike_segment_log)

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
               WARaftBackend.start(source_ctx, log_module: :ferricstore_waraft_spike_segment_log)

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

      assert_receive {:snapshot_payload_fsync, create_path}, 1_000
      assert String.starts_with?(create_path, snapshot_path)

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(source_ctx.name)

      target_ctx = build_ctx(target_root)

      assert :ok =
               WARaftBackend.start(target_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log,
                 bootstrap: false
               )

      assert :ok = WARaftBackend.install_snapshot(0, snapshot_path, position)

      assert_receive {:snapshot_payload_fsync, install_path}, 1_000
      assert String.starts_with?(install_path, target_root)
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
        config: nil
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
             WARaftBackend.start(recovered_ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert "old" == Router.get(recovered_ctx, "snapshot:keep")
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
        payload_dirs: [:data, :blob, :dedicated, :prob],
        empty_payload_dirs: [:data, :blob, :dedicated, :prob]
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

    Enum.each(shard_dir_specs(ctx, 0), fn {_kind, path} ->
      assert File.dir?(path)
    end)

    refute File.exists?(Path.join(handle.root_dir, "snapshot_install.term"))
  end

  test "storage rejects snapshot payload dir removed after verification", %{
    root: root,
    ctx: ctx
  } do
    snapshot_path = Path.join(root, "payload-dir-race-snapshot")
    position = {:raft_log_pos, 0, 0}
    File.mkdir_p!(snapshot_path)

    Enum.each([:data, :blob, :dedicated, :prob], fn kind ->
      File.mkdir_p!(Path.join(snapshot_path, Atom.to_string(kind)))
    end)

    File.write!(
      Path.join(snapshot_path, "ferricstore_snapshot.term"),
      :erlang.term_to_binary(%{
        version: 1,
        position: position,
        label: nil,
        config: nil,
        payload_dirs: [:data, :blob, :dedicated, :prob],
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

  test "storage rejects oversized snapshot metadata before install", %{
    root: root,
    ctx: ctx
  } do
    snapshot_path = Path.join(root, "oversized-snapshot")
    position = {:raft_log_pos, 10, 1}

    File.mkdir_p!(snapshot_path)

    for kind <- [:data, :blob, :dedicated, :prob] do
      File.mkdir_p!(Path.join(snapshot_path, Atom.to_string(kind)))
    end

    metadata = %{
      version: 1,
      position: position,
      label: :binary.copy("x", 1_048_576),
      config: nil,
      payload_dirs: [:data, :blob, :dedicated, :prob],
      empty_payload_dirs: [:data, :blob, :dedicated, :prob]
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

  test "storage validates atom-bearing local snapshot metadata shape", %{
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

    assert {:error, {:bad_snapshot_metadata, _created_atom}} =
             Ferricstore.Raft.WARaftStorage.open_snapshot(
               snapshot_path,
               {:raft_log_pos, 10, 1},
               handle
             )

    assert existing_atom?(atom_name)
  end

  test "storage rejects snapshot metadata symlinks before install", %{root: root} do
    snapshot_path = Path.join(root, "snapshot-metadata-symlink")
    File.mkdir_p!(snapshot_path)

    Enum.each([:data, :blob, :dedicated, :prob], fn kind ->
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
        payload_dirs: [:data, :blob, :dedicated, :prob],
        empty_payload_dirs: [:data, :blob, :dedicated, :prob]
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

    assert {:error, {:read_snapshot_metadata, {:unsafe_metadata_path, ^metadata_path, :symlink}}} =
             Ferricstore.Raft.WARaftStorage.open_snapshot(snapshot_path, position, handle)
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
        payload_dirs: [:data, :blob, :dedicated, :prob],
        empty_payload_dirs: [:data, :blob, :dedicated, :prob]
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
        payload_dirs: [:data, :blob, :dedicated, :prob],
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
             WARaftBackend.start(source_ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert :ok = WARaftBackend.write(0, {:put, "snapshot:non-dir-payload", "value", 0})

    blob_path = Ferricstore.DataDir.blob_shard_path(source_ctx.data_dir, 0)
    File.rm_rf!(blob_path)
    File.write!(blob_path, "not-a-directory")

    assert {:error, {:blob, {:source_not_directory, ^blob_path, :regular}}} =
             WARaftBackend.create_snapshot(0)
  end

  test "storage rejects snapshots missing non-empty declared payload dirs", %{root: root} do
    source_root = Path.join(root, "declared-missing-source")
    target_root = Path.join(root, "declared-missing-target")
    File.mkdir_p!(source_root)
    File.mkdir_p!(target_root)

    source_ctx = build_ctx(source_root)

    assert :ok =
             WARaftBackend.start(source_ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert :ok = WARaftBackend.write(0, {:put, "snapshot:declared-missing", "new", 0})
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
             WARaftBackend.start(target_ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert :ok = WARaftBackend.write(0, {:put, "snapshot:declared-missing", "old", 0})
    assert "old" == Router.get(target_ctx, "snapshot:declared-missing")
    assert :ok = WARaftBackend.stop()

    File.rm_rf!(Path.join(snapshot_path, "data"))

    handle = %{
      ctx: target_ctx,
      shard_index: 0,
      root_dir: Path.join([target_root, "waraft", "ferricstore_waraft_backend.1"]),
      sm_state: nil,
      position: {:raft_log_pos, 2, 1},
      label: nil,
      config: nil
    }

    assert {:error, {:missing_snapshot_dir, :data, _path}} =
             Ferricstore.Raft.WARaftStorage.open_snapshot(snapshot_path, position, handle)

    FerricStore.Instance.cleanup(target_ctx.name)
    recovered_ctx = build_ctx(target_root)

    assert :ok =
             WARaftBackend.start(recovered_ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert "old" == Router.get(recovered_ctx, "snapshot:declared-missing")
  end

  test "storage rejects snapshot payload symlinks without wiping live shard data", %{root: root} do
    source_root = Path.join(root, "symlink-source")
    target_root = Path.join(root, "symlink-target")
    File.mkdir_p!(source_root)
    File.mkdir_p!(target_root)

    source_ctx = build_ctx(source_root)

    assert :ok =
             WARaftBackend.start(source_ctx, log_module: :ferricstore_waraft_spike_segment_log)

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
             WARaftBackend.start(target_ctx, log_module: :ferricstore_waraft_spike_segment_log)

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
             WARaftBackend.start(recovered_ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert "old" == Router.get(recovered_ctx, "snapshot:symlink")
  end

  test "snapshot install copy failure leaves live shard data untouched", %{root: root} do
    source_root = Path.join(root, "partial-source")
    target_root = Path.join(root, "partial-target")
    File.mkdir_p!(source_root)
    File.mkdir_p!(target_root)

    source_ctx = build_ctx(source_root)

    assert :ok =
             WARaftBackend.start(source_ctx, log_module: :ferricstore_waraft_spike_segment_log)

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
             WARaftBackend.start(target_ctx, log_module: :ferricstore_waraft_spike_segment_log)

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
             WARaftBackend.start(recovered_ctx, log_module: :ferricstore_waraft_spike_segment_log)

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
             WARaftBackend.start(source_ctx, log_module: :ferricstore_waraft_spike_segment_log)

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
             WARaftBackend.start(target_ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert :ok = WARaftBackend.write(0, {:put, "snapshot:swap-fail", "old", 0})
    assert "old" == Router.get(target_ctx, "snapshot:swap-fail")
    assert :ok = WARaftBackend.stop()

    specs = shard_dir_specs(target_ctx, 0)
    dedicated_dest = Keyword.fetch!(specs, :dedicated)
    prob_dest = Keyword.fetch!(specs, :prob)
    prob_sentinel = Path.join(prob_dest, "sentinel")

    File.rm_rf!(dedicated_dest)
    File.mkdir_p!(Path.dirname(dedicated_dest))
    File.write!(dedicated_dest, "not-a-directory")
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

    assert {:error, {:backup_live_dir, :dedicated, :not_directory}} =
             Ferricstore.Raft.WARaftStorage.open_snapshot(snapshot_path, position, handle)

    assert File.exists?(prob_sentinel)
    assert File.regular?(dedicated_dest)
  end

  test "snapshot install metadata failure rolls live ETS back with disk", %{root: root} do
    source_root = Path.join(root, "metadata-fail-source")
    target_root = Path.join(root, "metadata-fail-target")
    File.mkdir_p!(source_root)
    File.mkdir_p!(target_root)

    source_ctx = build_ctx(source_root)

    assert :ok =
             WARaftBackend.start(source_ctx, log_module: :ferricstore_waraft_spike_segment_log)

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
             WARaftBackend.start(target_ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert :ok = WARaftBackend.write(0, {:put, "snapshot:metadata-fail", "old-value-longer", 0})
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

    assert {:error, _reason} =
             Ferricstore.Raft.WARaftStorage.open_snapshot(snapshot_path, position, handle)

    assert "old-value-longer" == Router.get(target_ctx, "snapshot:metadata-fail")
  end

  test "startup rolls back interrupted snapshot swap before metadata persisted", %{root: root} do
    source_root = Path.join(root, "swap-source")
    target_root = Path.join(root, "swap-target")
    File.mkdir_p!(source_root)
    File.mkdir_p!(target_root)

    source_ctx = build_ctx(source_root)

    assert :ok =
             WARaftBackend.start(source_ctx, log_module: :ferricstore_waraft_spike_segment_log)

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
             WARaftBackend.start(target_ctx, log_module: :ferricstore_waraft_spike_segment_log)

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
             WARaftBackend.start(recovered_ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert "old" == Router.get(recovered_ctx, "snapshot:swap")
    assert "keep" == Router.get(recovered_ctx, "snapshot:swap:target-only")
  end

  test "startup finalizes interrupted snapshot swap after metadata persisted", %{root: root} do
    source_root = Path.join(root, "finalize-source")
    target_root = Path.join(root, "finalize-target")
    File.mkdir_p!(source_root)
    File.mkdir_p!(target_root)

    source_ctx = build_ctx(source_root)

    assert :ok =
             WARaftBackend.start(source_ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert :ok = WARaftBackend.write(0, {:put, "snapshot:finalize", "new", 0})
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
             WARaftBackend.start(target_ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert :ok = WARaftBackend.write(0, {:put, "snapshot:finalize", "old", 0})
    assert :ok = WARaftBackend.write(0, {:put, "snapshot:finalize:target-only", "drop", 0})
    assert "old" == Router.get(target_ctx, "snapshot:finalize")
    assert :ok = WARaftBackend.stop()

    root_dir = Path.join([target_root, "waraft", "ferricstore_waraft_backend.1"])
    backup_root = Path.join(root_dir, "snapshot_install_backup.finalize")
    staging_root = Path.join(root_dir, "snapshot_install_staging.finalize")

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

    metadata_path = Path.join(root_dir, "ferricstore_storage.term")

    storage_metadata =
      metadata_path
      |> File.read!()
      |> :erlang.binary_to_term([:safe])
      |> Map.put(:position, position)

    File.write!(metadata_path, :erlang.term_to_binary(storage_metadata))

    FerricStore.Instance.cleanup(target_ctx.name)
    recovered_ctx = build_ctx(target_root)

    assert :ok =
             WARaftBackend.start(recovered_ctx,
               log_module: :ferricstore_waraft_spike_segment_log,
               bootstrap: false
             )

    assert "new" == Router.get(recovered_ctx, "snapshot:finalize")
    assert nil == Router.get(recovered_ctx, "snapshot:finalize:target-only")
    refute File.exists?(Path.join(root_dir, "snapshot_install.term"))
    refute File.exists?(backup_root)
  end

  test "startup keeps snapshot install marker when finalize cleanup fails", %{root: root} do
    target_root = Path.join(root, "finalize-cleanup-fails")
    File.mkdir_p!(target_root)

    target_ctx = build_ctx(target_root)

    assert :ok =
             WARaftBackend.start(target_ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert :ok = WARaftBackend.write(0, {:put, "snapshot:cleanup-fails", "value", 0})
    assert :ok = WARaftBackend.stop()

    root_dir = Path.join([target_root, "waraft", "ferricstore_waraft_backend.1"])
    marker_path = Path.join(root_dir, "snapshot_install.term")
    backup_root = Path.join(root_dir, "snapshot_install_backup.cleanup_fails")
    staging_root = Path.join(root_dir, "snapshot_install_staging.cleanup_fails")

    metadata =
      root_dir
      |> Path.join("ferricstore_storage.term")
      |> File.read!()
      |> :erlang.binary_to_term([:safe])

    File.mkdir_p!(backup_root)
    File.mkdir_p!(staging_root)

    marker = %{
      version: 1,
      snapshot_position: Map.fetch!(metadata, :position),
      backup_root: backup_root,
      staging_root: staging_root
    }

    File.write!(marker_path, :erlang.term_to_binary(marker))

    previous_hook = Application.get_env(:ferricstore, :waraft_snapshot_cleanup_hook)

    try do
      Application.put_env(:ferricstore, :waraft_snapshot_cleanup_hook, fn
        {:remove, :backup, ^backup_root} -> {:error, :injected_cleanup_failure}
        _event -> :ok
      end)

      FerricStore.Instance.cleanup(target_ctx.name)
      recovered_ctx = build_ctx(target_root)

      assert {:error, reason} =
               WARaftBackend.start(recovered_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log,
                 bootstrap: false
               )

      assert inspect(reason) =~ "injected_cleanup_failure"
      assert File.exists?(marker_path)
    after
      restore_env(:waraft_snapshot_cleanup_hook, previous_hook)
    end
  end

  test "snapshot install blocks storage when final cleanup fails after metadata persists", %{
    root: root,
    ctx: ctx
  } do
    snapshot_path = Path.join(root, "finalize-fails-live-snapshot")
    position = {:raft_log_pos, 10, 1}
    File.mkdir_p!(snapshot_path)

    File.write!(
      Path.join(snapshot_path, "ferricstore_snapshot.term"),
      :erlang.term_to_binary(%{
        version: 1,
        position: position,
        label: nil,
        config: nil,
        payload_dirs: [:data, :blob, :dedicated, :prob],
        empty_payload_dirs: [:data, :blob, :dedicated, :prob]
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

    previous_hook = Application.get_env(:ferricstore, :waraft_snapshot_cleanup_hook)
    parent = self()
    handler_id = {__MODULE__, :snapshot_finalize_storage_blocked, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :waraft, :storage_blocked],
      &__MODULE__.handle_test_telemetry/4,
      parent
    )

    try do
      Application.put_env(:ferricstore, :waraft_snapshot_cleanup_hook, fn
        {:remove, :backup, _path} -> {:error, :injected_finalize_cleanup_failure}
        _event -> :ok
      end)

      assert {:ok, new_handle} =
               Ferricstore.Raft.WARaftStorage.open_snapshot(snapshot_path, position, handle)

      assert {:finalize_snapshot_install_failed,
              {:snapshot_cleanup_hook, :injected_finalize_cleanup_failure}} =
               Map.fetch!(new_handle, :blocked_error)

      assert_receive {:waraft_storage_blocked, [:ferricstore, :waraft, :storage_blocked],
                      %{count: 1},
                      %{
                        operation: :snapshot_install_finalize_failure,
                        reason:
                          {:finalize_snapshot_install_failed,
                           {:snapshot_cleanup_hook, :injected_finalize_cleanup_failure}},
                        attempted_position: ^position,
                        shard_index: 0
                      }},
                     500

      assert {{:error, {:storage_blocked, _reason}}, ^new_handle} =
               Ferricstore.Raft.WARaftStorage.apply(
                 {:put, "snapshot:after-finalize-failure", "unsafe", 0},
                 {:raft_log_pos, 11, 1},
                 new_handle
               )

      assert File.exists?(Path.join(handle.root_dir, "snapshot_install.term"))
    after
      :telemetry.detach(handler_id)
      restore_env(:waraft_snapshot_cleanup_hook, previous_hook)
    end
  end

  test "startup fails closed on oversized snapshot install marker", %{root: root} do
    target_root = Path.join(root, "oversized-install-marker")
    File.mkdir_p!(target_root)

    target_ctx = build_ctx(target_root)

    assert :ok =
             WARaftBackend.start(target_ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert :ok = WARaftBackend.write(0, {:put, "snapshot:marker-too-large", "value", 0})
    assert :ok = WARaftBackend.stop()

    root_dir = Path.join([target_root, "waraft", "ferricstore_waraft_backend.1"])
    marker_path = Path.join(root_dir, "snapshot_install.term")
    backup_root = Path.join(root_dir, "snapshot_install_backup.too_large")
    staging_root = Path.join(root_dir, "snapshot_install_staging.too_large")

    metadata =
      root_dir
      |> Path.join("ferricstore_storage.term")
      |> File.read!()
      |> :erlang.binary_to_term([:safe])

    File.mkdir_p!(backup_root)
    File.mkdir_p!(staging_root)

    marker = %{
      version: 1,
      snapshot_position: Map.fetch!(metadata, :position),
      backup_root: backup_root,
      staging_root: staging_root,
      label: :binary.copy("x", 1_048_576)
    }

    File.write!(marker_path, :erlang.term_to_binary(marker))

    FerricStore.Instance.cleanup(target_ctx.name)
    recovered_ctx = build_ctx(target_root)

    assert {:error, reason} =
             WARaftBackend.start(recovered_ctx,
               log_module: :ferricstore_waraft_spike_segment_log,
               bootstrap: false
             )

    assert inspect(reason) =~ "snapshot_install_marker_file_too_large"
    assert File.exists?(marker_path)
    assert File.exists?(backup_root)
  end

  test "startup fails closed when pending snapshot install metadata is unreadable", %{root: root} do
    source_root = Path.join(root, "unreadable-source")
    target_root = Path.join(root, "unreadable-target")
    File.mkdir_p!(source_root)
    File.mkdir_p!(target_root)

    source_ctx = build_ctx(source_root)

    assert :ok =
             WARaftBackend.start(source_ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert :ok = WARaftBackend.write(0, {:put, "snapshot:unreadable", "new", 0})
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
             WARaftBackend.start(target_ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert :ok = WARaftBackend.write(0, {:put, "snapshot:unreadable", "old", 0})
    assert :ok = WARaftBackend.stop()

    root_dir = Path.join([target_root, "waraft", "ferricstore_waraft_backend.1"])
    marker_path = Path.join(root_dir, "snapshot_install.term")
    backup_root = Path.join(root_dir, "snapshot_install_backup.unreadable")
    staging_root = Path.join(root_dir, "snapshot_install_staging.unreadable")

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

    File.write!(marker_path, :erlang.term_to_binary(marker))

    metadata_path = Path.join(root_dir, "ferricstore_storage.term")

    storage_metadata =
      metadata_path
      |> File.read!()
      |> :erlang.binary_to_term([:safe])
      |> Map.put(:position, position)

    File.write!(metadata_path, :erlang.term_to_binary(storage_metadata))
    File.chmod!(metadata_path, 0)

    try do
      FerricStore.Instance.cleanup(target_ctx.name)
      recovered_ctx = build_ctx(target_root)

      assert {:error, _reason} =
               WARaftBackend.start(recovered_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log,
                 bootstrap: false
               )

      assert File.exists?(marker_path)
      assert File.exists?(backup_root)
    after
      _ = File.chmod(metadata_path, 0o600)
    end
  end

  test "startup validates atom-bearing local pending snapshot install marker shape", %{
    root: root,
    ctx: ctx
  } do
    root_dir = Path.join([root, "waraft", "ferricstore_waraft_backend.1"])
    File.mkdir_p!(root_dir)

    atom_name = "ferricstore_waraft_snapshot_marker_#{System.unique_integer([:positive])}"
    refute existing_atom?(atom_name)

    File.write!(Path.join(root_dir, "snapshot_install.term"), unknown_atom_payload(atom_name))

    assert {:error, reason} =
             WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert inspect(reason) =~ "bad_snapshot_install_marker"
    assert existing_atom?(atom_name)
  end

  test "startup rejects pending snapshot install marker paths outside storage root", %{
    root: root,
    ctx: ctx
  } do
    root_dir = Path.join([root, "waraft", "ferricstore_waraft_backend.1"])
    File.mkdir_p!(root_dir)

    outside_staging = Path.join(root, "outside_snapshot_install_staging")
    outside_backup = Path.join(root, "outside_snapshot_install_backup")
    File.mkdir_p!(outside_staging)
    File.mkdir_p!(outside_backup)
    File.write!(Path.join(outside_staging, "sentinel"), "keep")

    marker = %{
      version: 1,
      snapshot_position: {:raft_log_pos, 3, 1},
      staging_root: outside_staging,
      backup_root: outside_backup
    }

    File.write!(Path.join(root_dir, "snapshot_install.term"), :erlang.term_to_binary(marker))

    assert {:error, reason} =
             WARaftBackend.start(ctx,
               log_module: :ferricstore_waraft_spike_segment_log,
               bootstrap: false
             )

    assert inspect(reason) =~ "bad_snapshot_install_marker"
    assert File.exists?(Path.join(outside_staging, "sentinel"))
    assert File.dir?(outside_backup)
  end

  test "startup fails closed when pending snapshot install marker position differs from metadata",
       %{
         root: root,
         ctx: ctx
       } do
    root_dir = Path.join([root, "waraft", "ferricstore_waraft_backend.1"])
    File.mkdir_p!(root_dir)

    current_position = {:raft_log_pos, 2, 1}
    snapshot_position = {:raft_log_pos, 3, 1}

    marker = %{
      version: 1,
      snapshot_position: snapshot_position,
      staging_root: Path.join(root_dir, "snapshot_install_staging.mismatch"),
      backup_root: Path.join(root_dir, "snapshot_install_backup.mismatch")
    }

    metadata = %{
      version: 1,
      position: current_position,
      label: nil,
      config: nil
    }

    File.write!(Path.join(root_dir, "snapshot_install.term"), :erlang.term_to_binary(marker))
    File.write!(Path.join(root_dir, "ferricstore_storage.term"), :erlang.term_to_binary(metadata))

    assert {:error, reason} =
             WARaftBackend.start(ctx,
               log_module: :ferricstore_waraft_spike_segment_log,
               bootstrap: false
             )

    assert inspect(reason) =~ "snapshot_install_position_mismatch"
    assert File.exists?(Path.join(root_dir, "snapshot_install.term"))
  end

  test "snapshot creation excludes writes that arrive while snapshot is in progress", %{
    root: root
  } do
    source_root = Path.join(root, "concurrent-source")
    target_root = Path.join(root, "concurrent-target")
    File.mkdir_p!(source_root)
    File.mkdir_p!(target_root)

    source_ctx = build_ctx(source_root)
    test_pid = self()
    previous_hook = Application.get_env(:ferricstore, :waraft_snapshot_create_hook)

    try do
      assert :ok =
               WARaftBackend.start(source_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert :ok = WARaftBackend.write(0, {:put, "snapshot:base", "base", 0})

      Application.put_env(:ferricstore, :waraft_snapshot_create_hook, fn
        {:copied, :data} ->
          send(test_pid, {:snapshot_create_paused, self()})

          receive do
            :resume_snapshot_create -> :ok
          after
            2_000 -> {:error, :snapshot_create_hook_timeout}
          end

        _event ->
          :ok
      end)

      snapshot_task = Task.async(fn -> WARaftBackend.create_snapshot(0) end)
      assert_receive {:snapshot_create_paused, storage_pid}, 1_000

      write_task =
        Task.async(fn ->
          result = WARaftBackend.write(0, {:put, "snapshot:late", "late", 0})
          send(test_pid, {:late_write_done, result})
          result
        end)

      refute_receive {:late_write_done, _result}, 50
      send(storage_pid, :resume_snapshot_create)

      assert {:ok, {:raft_log_pos, index, term} = position} = Task.await(snapshot_task, 5_000)
      assert :ok = Task.await(write_task, 5_000)
      assert_receive {:late_write_done, :ok}, 1_000

      snapshot_path =
        Path.join([
          source_root,
          "waraft",
          "ferricstore_waraft_backend.1",
          "snapshot.#{index}.#{term}"
        ])

      assert "late" == Router.get(source_ctx, "snapshot:late")
      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(source_ctx.name)

      target_ctx = build_ctx(target_root)

      assert :ok =
               WARaftBackend.start(target_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log,
                 bootstrap: false
               )

      assert :ok = WARaftBackend.install_snapshot(0, snapshot_path, position)
      assert_eventually(fn -> Router.get(target_ctx, "snapshot:base") end, "base")
      assert nil == Router.get(target_ctx, "snapshot:late")
    after
      restore_env(:waraft_snapshot_create_hook, previous_hook)
    end
  end

  test "backend maps WARaft commit backpressure to Ra-compatible overload", %{ctx: ctx} do
    assert :ok =
             WARaftBackend.start(ctx,
               log_module: :wa_raft_log_ets,
               max_pending_low_priority_commits: 0
             )

    assert {:error, :overloaded} = WARaftBackend.write(0, {:put, "blocked:k", "v", 0})
    assert nil == Router.get(ctx, "blocked:k")
  end

  test "backend maps async WARaft commit backpressure to Ra-compatible overload", %{ctx: ctx} do
    assert :ok =
             WARaftBackend.start(ctx,
               log_module: :wa_raft_log_ets,
               max_pending_low_priority_commits: 0,
               max_inflight_commit_bytes: 256
             )

    assert [{:error, :overloaded}] =
             WARaftBackend.write_many([{0, {:put, "blocked:async", "v", 0}}])

    assert nil == Router.get(ctx, "blocked:async")
    assert 0 == WARaftBackend.inflight_commit_bytes(0)
  end

  test "backend async submit does not assert on commit_async after reserving bytes" do
    source =
      Path.expand("../../../lib/ferricstore/raft/waraft_backend.ex", __DIR__)
      |> File.read!()

    assert [_, submit_source] =
             String.split(source, "defp submit_acquired_commit_async", parts: 2)

    assert [submit_source, _] = String.split(submit_source, "defp await_commit_async", parts: 2)

    refute submit_source =~ ":ok =\n              :wa_raft_acceptor.commit_async",
           "async submit must not crash after reserving in-flight bytes"

    assert submit_source =~ "release_commit_bytes(shard_index, acquired_bytes)"
    assert source =~ "defp commit_async_safely"
    assert source =~ "catch"
  end

  test "backend sync submit wraps acceptor exits after reserving bytes" do
    source =
      Path.expand("../../../lib/ferricstore/raft/waraft_backend.ex", __DIR__)
      |> File.read!()

    assert [_, commit_source] =
             String.split(source, "defp commit(shard_index, command)", parts: 2)

    assert [commit_source, _] =
             String.split(commit_source, "defp submit_commit_async", parts: 2)

    assert commit_source =~ "commit_safely(",
           "sync submit must not leak an acceptor exit after reserving in-flight bytes"

    assert source =~ "defp commit_safely"
    assert source =~ "catch"
  end

  test "backend async await flushes timed-out reply aliases" do
    source =
      Path.expand("../../../lib/ferricstore/raft/waraft_backend.ex", __DIR__)
      |> File.read!()

    assert [_, await_source] = String.split(source, "defp await_commit_async", parts: 2)

    assert [await_source, _] =
             String.split(await_source, "defp normalize_commit_transport_result", parts: 2)

    assert await_source =~ "flush_reply_alias(reply_alias, reply_ref)",
           "timed-out async commits must flush late alias replies instead of leaving mailbox junk"

    assert source =~ "defp flush_reply_alias"
    assert source =~ "{^reply_ref, _late_result} -> :ok"
  end

  test "backend maps in-flight byte rejection to Ra-compatible overload", %{ctx: ctx} do
    assert :ok =
             WARaftBackend.start(ctx,
               log_module: :wa_raft_log_ets,
               max_inflight_commit_bytes: 256
             )

    assert {:error, :overloaded} =
             WARaftBackend.write(0, {:put, "blocked:bytes", String.duplicate("x", 1024), 0})

    assert nil == Router.get(ctx, "blocked:bytes")
    assert 0 == WARaftBackend.inflight_commit_bytes(0)
  end

  test "backend rejects over byte cap before blob side-channel writes", %{ctx: ctx} do
    assert :ok =
             WARaftBackend.start(ctx,
               log_module: :wa_raft_log_ets,
               max_inflight_commit_bytes: 0
             )

    parent = self()
    payload = :binary.copy("blocked-large-blob", 300_000)
    assert byte_size(payload) > ctx.blob_side_channel_threshold_bytes

    Process.put(:ferricstore_blob_store_write_hook, fn _io, _iodata ->
      send(parent, :unexpected_blob_write)
      :ok
    end)

    on_exit(fn -> Process.delete(:ferricstore_blob_store_write_hook) end)

    assert {:error, :overloaded} = WARaftBackend.write(0, {:put, "blocked:blob", payload, 0})
    refute_received :unexpected_blob_write
    assert nil == Router.get(ctx, "blocked:blob")
    assert 0 == WARaftBackend.inflight_commit_bytes(0)
  end

  test "backend maps missing local acceptor to shard unavailable" do
    assert :ok = WARaftBackend.stop()

    assert {:error, "ERR shard not available"} =
             WARaftBackend.write(0, {:put, "stopped:k", "v", 0})

    assert [{:error, "ERR shard not available"}] =
             WARaftBackend.write_many([{0, {:put, "stopped:many", "v", 0}}])

    assert {:error, "ERR shard not available"} =
             WARaftBackend.write_batch(0, [{:put, "stopped:batch", "v", 0}])

    assert {:error, "ERR shard not available"} =
             WARaftBackend.write_put_batch(0, [{"stopped:put-batch", "v", 0}])

    assert {:error, "ERR shard not available"} =
             WARaftBackend.write_delete_batch(0, ["stopped:delete-batch"])
  end

  test "backend admin APIs fail closed when WARaft is stopped" do
    assert :ok = WARaftBackend.stop()

    assert {:error, :backend_unavailable} = WARaftBackend.status(0)
    assert {:error, :backend_unavailable} = WARaftBackend.membership(0)
    assert {:error, :backend_unavailable} = WARaftBackend.storage_position(0)
    assert {:error, :backend_unavailable} = WARaftBackend.create_snapshot(0)
    assert {:error, :backend_unavailable} = WARaftBackend.trigger_election(0)
    assert {:error, :backend_unavailable} = WARaftBackend.peer_ready(0, node())
    assert {:error, "ERR shard not available"} = WARaftBackend.transfer_leadership(0, node())

    assert {:error, :backend_unavailable} =
             WARaftBackend.adjust_membership(0, :add_participant, :stopped_target@nohost)

    assert {:error, :backend_unavailable} =
             WARaftBackend.add_participant(0, :stopped_target@nohost)

    assert {:error, :backend_unavailable} =
             WARaftBackend.add_member(0, :stopped_target@nohost)

    assert {:error, :backend_unavailable} =
             WARaftBackend.install_snapshot(0, "/tmp/no-snapshot", {:raft_log_pos, 0, 0})

    assert {:error, :backend_unavailable} = WARaftBackend.local_get(0, "stopped:local")
    assert {:error, :backend_unavailable} = WARaftBackend.bootstrap_cluster([node()])
  end

  test "bootstrap fails closed when context exists but WARaft server is missing", %{ctx: ctx} do
    assert :ok = WARaftBackend.stop()

    context_key = {{WARaftBackend, :context}, :ferricstore_waraft_backend}
    :persistent_term.put(context_key, ctx)

    on_exit(fn -> :persistent_term.erase(context_key) end)

    assert {:error, :backend_unavailable} = WARaftBackend.bootstrap_cluster([node()])
  end

  test "membership storage config polling wraps WARaft exits" do
    source =
      Path.expand("../../../lib/ferricstore/raft/waraft_backend.ex", __DIR__)
      |> File.read!()

    assert [_, participant_source] = String.split(source, "defp storage_participant?", parts: 2)

    assert [participant_source, member_and_rest] =
             String.split(participant_source, "defp wait_storage_member", parts: 2)

    assert [_, member_source] = String.split(member_and_rest, "defp storage_member?", parts: 2)

    assert [member_source, _rest] =
             String.split(member_source, "defp create_transfer_snapshot", parts: 2)

    assert participant_source =~ "backend_call(fn -> :wa_raft_storage.config(storage) end)"
    assert member_source =~ "backend_call(fn -> :wa_raft_storage.config(storage) end)"
  end

  test "startup promotion wraps WARaft exits" do
    source =
      Path.expand("../../../lib/ferricstore/raft/waraft_backend.ex", __DIR__)
      |> File.read!()

    assert [_, finish_source] = String.split(source, "defp finish_start_status", parts: 2)
    assert [finish_source, _rest] = String.split(finish_source, "defp bootstrap", parts: 2)

    assert finish_source =~ "backend_call(fn -> :wa_raft_server.promote(server, :next, true) end)"
  end

  test "snapshot transfer wraps WARaft transport exits" do
    source =
      Path.expand("../../../lib/ferricstore/raft/waraft_backend.ex", __DIR__)
      |> File.read!()

    assert [_, transfer_source] = String.split(source, "defp transfer_snapshot", parts: 2)

    assert [transfer_source, _rest] =
             String.split(transfer_source, "defp wait_peer_ready", parts: 2)

    assert transfer_source =~ "backend_call(fn ->"
    assert transfer_source =~ ":wa_raft_transport.transfer_snapshot("
  end

  test "startup status polling wraps WARaft exits" do
    source =
      Path.expand("../../../lib/ferricstore/raft/waraft_backend.ex", __DIR__)
      |> File.read!()

    assert [_, wait_source] = String.split(source, "defp wait_status", parts: 2)

    assert [wait_source, _rest] =
             String.split(wait_source, "defp maybe_cache_current_config", parts: 2)

    assert wait_source =~ "backend_call(fn -> :wa_raft_server.status(server) end)"
  end

  test "storage durable-position polling wraps WARaft exits" do
    source =
      Path.expand("../../../lib/ferricstore/raft/waraft_backend.ex", __DIR__)
      |> File.read!()

    assert [_, storage_source] = String.split(source, "defp storage_status", parts: 2)

    assert [storage_source, _rest] =
             String.split(storage_source, "defp position_reached?", parts: 2)

    assert storage_source =~ "backend_call(fn -> :wa_raft_storage.status(storage) end)"
  end

  test "transfer leadership wraps WARaft exits" do
    source =
      Path.expand("../../../lib/ferricstore/raft/waraft_backend.ex", __DIR__)
      |> File.read!()

    assert [_, transfer_source] = String.split(source, "defp local_transfer_leadership", parts: 2)

    assert [transfer_source, _rest] =
             String.split(transfer_source, "defp maybe_redirect_transfer", parts: 2)

    assert transfer_source =~ "backend_call(fn ->"
    assert transfer_source =~ ":wa_raft_server.handover(target_node)"
  end

  test "backend stop cleanup tracks configured shard counts above 64" do
    names = WARaftBackend.__registered_names_for_test__(65)

    assert :raft_server_ferricstore_waraft_backend_65 in names
    assert :raft_storage_ferricstore_waraft_backend_65 in names
    assert :raft_acceptor_ferricstore_waraft_backend_65 in names
  end

  test "backend releases in-flight byte accounting after commit replies", %{ctx: ctx} do
    assert :ok =
             WARaftBackend.start(ctx,
               log_module: :wa_raft_log_ets,
               max_inflight_commit_bytes: 256
             )

    assert :ok = WARaftBackend.write(0, {:put, "bytes:k1", "v1", 0})
    assert 0 == WARaftBackend.inflight_commit_bytes(0)
    assert :ok = WARaftBackend.write(0, {:put, "bytes:k2", "v2", 0})
    assert 0 == WARaftBackend.inflight_commit_bytes(0)

    assert "v1" == Router.get(ctx, "bytes:k1")
    assert "v2" == Router.get(ctx, "bytes:k2")
  end

  test "Router can use WARaft as the selected durable write backend", %{ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

      assert :ok = Router.put(ctx, "router:k", "router:v", 0)
      assert "router:v" == Router.get(ctx, "router:k")

      assert [:ok, :ok] = Router.batch_quorum_put(ctx, [{"router:b1", "v1"}, {"router:b2", "v2"}])
      assert "v1" == Router.get(ctx, "router:b1")
      assert "v2" == Router.get(ctx, "router:b2")

      assert [:ok, :ok] = Router.batch_quorum_delete(ctx, ["router:b1", "router:b2"])
      assert nil == Router.get(ctx, "router:b1")
      assert nil == Router.get(ctx, "router:b2")
    after
      restore_backend(previous_backend)
    end
  end

  test "Router forced-quorum commands use WARaft as the selected backend", %{ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

      assert 1 = Router.pfadd(ctx, "router:pf", ["a"])
      assert 0 = Router.pfadd(ctx, "router:pf", ["a"])
      assert is_binary(Router.get(ctx, "router:pf"))
    after
      restore_backend(previous_backend)
    end
  end

  test "Router get_version uses WARaft shared counters without shard fallback telemetry", %{
    ctx: ctx
  } do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    handler_id = {__MODULE__, :waraft_get_version_no_shard_fallback, make_ref()}
    parent = self()

    :telemetry.attach(
      handler_id,
      [:ferricstore, :store, :shard_unavailable],
      &__MODULE__.handle_store_unavailable_telemetry/4,
      parent
    )

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

      assert :ok = Router.put(ctx, "router:version:k", "v", 0)
      assert Router.get_version(ctx, "router:version:k") > 0

      refute_receive {:store_unavailable, _event, _measurements, _metadata}, 50
    after
      :telemetry.detach(handler_id)
      restore_backend(previous_backend)
    end
  end

  test "key expiry and persist commands survive WARaft restart", %{root: root, ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      key = "router:ttl:#{System.unique_integer([:positive])}"

      assert :ok = Ferricstore.Commands.Strings.handle_ast({:set, key, "ttl-value"}, ctx)
      assert 1 = Ferricstore.Commands.Expiry.handle_ast({:pexpire, key, 60_000}, ctx)
      ttl_before = Ferricstore.Commands.Expiry.handle_ast({:pttl, key}, ctx)
      assert ttl_before > 0

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert "ttl-value" == Router.get(restarted_ctx, key)

      ttl_after = Ferricstore.Commands.Expiry.handle_ast({:pttl, key}, restarted_ctx)
      assert ttl_after > 0
      assert ttl_after <= ttl_before

      assert 1 = Ferricstore.Commands.Expiry.handle_ast({:persist, key}, restarted_ctx)
      assert -1 = Ferricstore.Commands.Expiry.handle_ast({:pttl, key}, restarted_ctx)

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(restarted_ctx.name)

      persisted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(persisted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert "ttl-value" == Router.get(persisted_ctx, key)
      assert -1 = Ferricstore.Commands.Expiry.handle_ast({:pttl, key}, persisted_ctx)
    after
      restore_backend(previous_backend)
    end
  end

  test "Router compound commands use WARaft as the selected backend", %{ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

      redis_key = "router:hash"
      field_key = Ferricstore.Store.CompoundKey.hash_field(redis_key, "field")

      assert :ok = Router.compound_put(ctx, redis_key, field_key, "value", 0)
      assert "value" == Router.compound_get(ctx, redis_key, field_key)

      assert :ok = Router.compound_delete(ctx, redis_key, field_key)
      assert nil == Router.compound_get(ctx, redis_key, field_key)
    after
      restore_backend(previous_backend)
    end
  end

  test "Router JSON bitmap and native commands use WARaft as the selected backend", %{ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

      assert :ok = Router.json_set(ctx, "router:json", "$", ~s({"n":1}), [])

      assert ~s({"n":1}) ==
               Ferricstore.Commands.Json.handle_ast({:json_get, "router:json", []}, ctx)

      assert 0 = Router.setbit(ctx, "router:bitmap", 7, 1)
      assert <<1>> == Router.get(ctx, "router:bitmap")

      assert ["allowed", 1, 2, reset_ms] = Router.ratelimit_add(ctx, "router:rl", 1_000, 3, 1)
      assert is_integer(reset_ms)
      assert ["allowed", 3, 0, _] = Router.ratelimit_add(ctx, "router:rl", 1_000, 3, 2)
      assert ["denied", 3, 0, _] = Router.ratelimit_add(ctx, "router:rl", 1_000, 3, 1)
    after
      restore_backend(previous_backend)
    end
  end

  test "JSON and bitmap mutations survive WARaft restart", %{root: root, ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    suffix = System.unique_integer([:positive])
    json_key = "router:json-restart:#{suffix}"
    bitmap_a = "router:bitmap-restart:a:#{suffix}"
    bitmap_b = "router:bitmap-restart:b:#{suffix}"
    bitmap_dest = "router:bitmap-restart:dest:#{suffix}"

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert :ok =
               Ferricstore.Commands.Json.handle_ast(
                 {:json_set, json_key, "$", ~s({"n":1,"arr":[],"flag":true}), []},
                 ctx
               )

      assert "3" =
               Ferricstore.Commands.Json.handle_ast(
                 {:json_numincrby, json_key, "$.n", 2},
                 ctx
               )

      assert 2 =
               Ferricstore.Commands.Json.handle_ast(
                 {:json_arrappend, json_key, "$.arr", ["1", "2"]},
                 ctx
               )

      assert "false" =
               Ferricstore.Commands.Json.handle_ast({:json_toggle, json_key, "$.flag"}, ctx)

      assert 0 = Router.setbit(ctx, bitmap_a, 1, 1)
      assert 0 = Router.setbit(ctx, bitmap_b, 7, 1)

      assert 1 =
               Ferricstore.Commands.Bitmap.handle_ast(
                 {:bitop, :bor, bitmap_dest, [bitmap_a, bitmap_b]},
                 ctx
               )

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert %{"arr" => [1, 2], "flag" => false, "n" => 3} =
               {:json_get, json_key, []}
               |> Ferricstore.Commands.Json.handle_ast(restarted_ctx)
               |> Jason.decode!()

      assert 2 =
               Ferricstore.Commands.Bitmap.handle_ast(
                 {:bitcount, bitmap_dest},
                 restarted_ctx
               )
    after
      restore_backend(previous_backend)
    end
  end

  test "Flow writes claims and transitions use WARaft as the selected backend", %{ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    flow_type = "router-flow-#{System.unique_integer([:positive])}"
    flow_id = "router-flow-id-#{System.unique_integer([:positive])}"

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

      assert :ok =
               Ferricstore.Flow.create(ctx, flow_id,
                 type: flow_type,
                 partition_key: "tenant-a",
                 run_at_ms: 1,
                 now_ms: 1,
                 payload: "payload"
               )

      assert {:ok, created} = Ferricstore.Flow.get(ctx, flow_id, partition_key: "tenant-a")
      assert created.id == flow_id
      assert created.state == "queued"

      assert {:ok, [claimed]} =
               Ferricstore.Flow.claim_due(ctx, flow_type,
                 partition_key: "tenant-a",
                 worker: "worker-a",
                 limit: 1,
                 now_ms: 2,
                 lease_ms: 10_000
               )

      assert claimed.id == flow_id
      assert claimed.state == "running"
      assert is_binary(claimed.lease_token)
      assert is_integer(claimed.fencing_token)

      assert :ok =
               Ferricstore.Flow.transition(
                 ctx,
                 flow_id,
                 "running",
                 "waiting",
                 partition_key: "tenant-a",
                 lease_token: claimed.lease_token,
                 fencing_token: claimed.fencing_token,
                 now_ms: 3,
                 payload: "next-payload"
               )

      assert {:ok, transitioned} = Ferricstore.Flow.get(ctx, flow_id, partition_key: "tenant-a")
      assert transitioned.state == "waiting"
      assert transitioned.version >= claimed.version + 1
    after
      restore_backend(previous_backend)
    end
  end

  test "Flow create_many and transition_many use WARaft as the selected backend", %{ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    flow_type = "router-flow-many-#{System.unique_integer([:positive])}"
    partition = "tenant-many-#{System.unique_integer([:positive])}"
    ids = for n <- 1..3, do: "router-flow-many-#{n}-#{System.unique_integer([:positive])}"

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

      assert :ok =
               Ferricstore.Flow.create_many(
                 ctx,
                 partition,
                 Enum.map(ids, &%{id: &1, payload: "payload:#{&1}"}),
                 type: flow_type,
                 state: "queued",
                 run_at_ms: 1_000,
                 now_ms: 900
               )

      assert :ok =
               Ferricstore.Flow.transition_many(
                 ctx,
                 partition,
                 "queued",
                 "ready",
                 Enum.map(ids, &%{id: &1, fencing_token: 0}),
                 run_at_ms: 2_000,
                 now_ms: 1_000
               )

      assert {:ok, claimed} =
               Ferricstore.Flow.claim_due(ctx, flow_type,
                 partition_key: partition,
                 state: "ready",
                 worker: "worker-many",
                 limit: 10,
                 now_ms: 2_000
               )

      assert claimed |> Enum.map(& &1.id) |> MapSet.new() == MapSet.new(ids)
      assert Enum.all?(claimed, &(&1.state == "running"))
    after
      restore_backend(previous_backend)
    end
  end

  test "WARaft apply passes log index to Flow history projection", %{ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    flow_type = "router-flow-history-#{System.unique_integer([:positive])}"
    flow_id = "router-flow-history-id-#{System.unique_integer([:positive])}"
    partition = "tenant-history-#{System.unique_integer([:positive])}"
    shard_data_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

      assert :ok =
               Ferricstore.Flow.create(ctx, flow_id,
                 type: flow_type,
                 partition_key: partition,
                 run_at_ms: 1_000,
                 now_ms: 900
               )

      assert {:ok, [claim]} =
               Ferricstore.Flow.claim_due(ctx, flow_type,
                 partition_key: partition,
                 worker: "worker-history",
                 limit: 1,
                 now_ms: 1_000
               )

      assert claim.id == flow_id
      assert {:ok, {:raft_log_pos, applied_index, _term}} = WARaftBackend.storage_position(0)

      assert Ferricstore.Flow.HistoryProjectedIndex.read(shard_data_path) >= applied_index
    after
      restore_backend(previous_backend)
    end
  end

  test "async Flow history projection is durable before WARaft storage position advances", %{
    ctx: ctx
  } do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    previous_async_history = Application.get_env(:ferricstore, :flow_async_history)

    previous_history_flush =
      Application.get_env(:ferricstore, :flow_history_projector_flush_interval_ms)

    previous_history_batch = Application.get_env(:ferricstore, :flow_history_projector_batch_size)
    flow_type = "router-flow-history-sync-#{System.unique_integer([:positive])}"
    flow_id = "router-flow-history-sync-id-#{System.unique_integer([:positive])}"
    partition = "tenant-history-sync-#{System.unique_integer([:positive])}"
    shard_data_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      Application.put_env(:ferricstore, :flow_async_history, true)
      Application.put_env(:ferricstore, :flow_history_projector_flush_interval_ms, 60_000)
      Application.put_env(:ferricstore, :flow_history_projector_batch_size, 10_000)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      start_supervised!(
        {Ferricstore.Flow.HistoryProjector,
         [
           shard_index: 0,
           shard_data_path: shard_data_path,
           instance_ctx: ctx,
           recover_on_init: false
         ]}
      )

      assert :ok =
               Ferricstore.Flow.create(ctx, flow_id,
                 type: flow_type,
                 partition_key: partition,
                 run_at_ms: 1_000,
                 now_ms: 900
               )

      assert {:ok, {:raft_log_pos, applied_index, _term}} = WARaftBackend.storage_position(0)
      assert Ferricstore.Flow.HistoryProjectedIndex.read(shard_data_path) >= applied_index
    after
      restore_backend(previous_backend)
      restore_env(:flow_async_history, previous_async_history)
      restore_env(:flow_history_projector_flush_interval_ms, previous_history_flush)
      restore_env(:flow_history_projector_batch_size, previous_history_batch)
    end
  end

  test "failed async Flow history projection does not advance WARaft storage position", %{
    ctx: ctx
  } do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    previous_async_history = Application.get_env(:ferricstore, :flow_async_history)
    flow_type = "router-flow-history-fail-#{System.unique_integer([:positive])}"
    flow_id = "router-flow-history-fail-id-#{System.unique_integer([:positive])}"
    partition = "tenant-history-fail-#{System.unique_integer([:positive])}"
    shard_data_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    history_path = Ferricstore.Flow.HistoryProjector.history_file_path(shard_data_path, 0)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      Application.put_env(:ferricstore, :flow_async_history, true)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert {:ok, pre_position} = WARaftBackend.storage_position(0)

      File.rm_rf!(history_path)
      File.mkdir_p!(Path.dirname(history_path))
      File.mkdir!(history_path)

      assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
               Ferricstore.Flow.create(ctx, flow_id,
                 type: flow_type,
                 partition_key: partition,
                 run_at_ms: 1_000,
                 now_ms: 900
               )

      assert {:ok, ^pre_position} = WARaftBackend.storage_position(0)
    after
      restore_backend(previous_backend)
      restore_env(:flow_async_history, previous_async_history)
    end
  end

  test "flow due indexes survive WARaft restart without shard process reads", %{
    root: root,
    ctx: ctx
  } do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    flow_type = "router-flow-restart-#{System.unique_integer([:positive])}"
    flow_id = "router-flow-restart-id-#{System.unique_integer([:positive])}"
    running_id = "router-flow-running-restart-id-#{System.unique_integer([:positive])}"
    partition = "tenant-restart-#{System.unique_integer([:positive])}"

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert :ok =
               Ferricstore.Flow.create(ctx, flow_id,
                 type: flow_type,
                 partition_key: partition,
                 run_at_ms: 1_000,
                 now_ms: 900
               )

      assert :ok =
               Ferricstore.Flow.create(ctx, running_id,
                 type: flow_type,
                 partition_key: partition,
                 run_at_ms: 800,
                 now_ms: 700
               )

      assert {:ok, [%{id: ^running_id}]} =
               Ferricstore.Flow.claim_due(ctx, flow_type,
                 partition_key: partition,
                 worker: "worker-before-restart",
                 lease_ms: 50,
                 limit: 1,
                 now_ms: 800
               )

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert {:ok, recovered} =
               Ferricstore.Flow.get(restarted_ctx, flow_id, partition_key: partition)

      assert recovered.id == flow_id

      assert {:ok, info} =
               Ferricstore.Flow.info(restarted_ctx, flow_type, partition_key: partition)

      assert info.queued == 1
      assert info.running == 1
      assert info.inflight == 1

      assert {:ok, [stuck]} =
               Ferricstore.Flow.stuck(restarted_ctx, flow_type,
                 partition_key: partition,
                 older_than_ms: 0,
                 count: 10,
                 now_ms: 900
               )

      assert stuck.id == running_id

      assert {:ok, [listed]} =
               Ferricstore.Flow.list(restarted_ctx, flow_type,
                 state: "queued",
                 partition_key: partition,
                 count: 10
               )

      assert listed.id == flow_id

      assert {:ok, [claim]} =
               Ferricstore.Flow.claim_due(restarted_ctx, flow_type,
                 partition_key: partition,
                 worker: "worker-restart",
                 limit: 1,
                 now_ms: 1_000,
                 reclaim_expired: false
               )

      assert claim.id == flow_id
    after
      restore_backend(previous_backend)
    end
  end

  test "Flow cross-shard spawn_children uses WARaft as the selected backend", %{root: root} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    ctx = build_ctx(Path.join(root, "flow-cross-shard"), shard_count: 2)
    parent_id = "router-flow-parent-#{System.unique_integer([:positive])}"
    child_id = "router-flow-child-#{System.unique_integer([:positive])}"
    parent_partition = flow_partition_for_shard(ctx, parent_id, 0)
    child_partition = flow_partition_for_shard(ctx, child_id, 1)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

      assert :ok =
               Ferricstore.Flow.create(ctx, parent_id,
                 type: "parent",
                 state: "dispatch",
                 partition_key: parent_partition,
                 now_ms: 1_000
               )

      assert {:ok, created_parent} =
               Ferricstore.Flow.get(ctx, parent_id, partition_key: parent_partition)

      assert :ok =
               Ferricstore.Flow.spawn_children(
                 ctx,
                 parent_id,
                 [%{id: child_id, type: "child", partition_key: child_partition}],
                 group_id: "fanout",
                 wait: :all,
                 wait_state: "waiting_children",
                 on_child_failed: :ignore,
                 on_parent_closed: :abandon_children,
                 exhaust_to: %{success: "children_done", failure: "children_failed"},
                 partition_key: parent_partition,
                 from_state: "dispatch",
                 fencing_token: created_parent.fencing_token,
                 now_ms: 1_010
               )

      assert {:ok, waiting_parent} =
               Ferricstore.Flow.get(ctx, parent_id, partition_key: parent_partition)

      assert waiting_parent.state == "waiting_children"
      assert waiting_parent.child_groups["fanout"]["children"][child_id] == "running"

      assert {:ok, child} = Ferricstore.Flow.get(ctx, child_id, partition_key: child_partition)
      assert child.parent_flow_id == parent_id
      assert child.parent_partition_key == parent_partition
      assert child.partition_key == child_partition
    after
      restore_backend(previous_backend)
      FerricStore.Instance.cleanup(ctx.name)
    end
  end

  test "Flow cross-shard child completion resolves parent through WARaft", %{root: root} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    ctx = build_ctx(Path.join(root, "flow-cross-shard-complete"), shard_count: 2)
    parent_id = "router-flow-parent-complete-#{System.unique_integer([:positive])}"
    child_id = "router-flow-child-complete-#{System.unique_integer([:positive])}"
    parent_partition = flow_partition_for_shard(ctx, parent_id, 0)
    child_partition = flow_partition_for_shard(ctx, child_id, 1)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

      assert {:ok, _waiting_parent} =
               setup_cross_shard_flow_child(
                 ctx,
                 parent_id,
                 child_id,
                 parent_partition,
                 child_partition,
                 group_id: "complete-fanout"
               )

      claimed = claim_flow_child!(ctx, child_id, child_partition, "worker-complete")

      assert :ok =
               Ferricstore.Flow.complete(ctx, child_id, claimed.lease_token,
                 partition_key: child_partition,
                 fencing_token: claimed.fencing_token,
                 result: "ok",
                 now_ms: 2_000
               )

      assert {:ok, completed_child} =
               Ferricstore.Flow.get(ctx, child_id, partition_key: child_partition)

      assert completed_child.state == "completed"

      assert {:ok, done_parent} =
               Ferricstore.Flow.get(ctx, parent_id, partition_key: parent_partition)

      assert done_parent.state == "children_done"
      assert done_parent.child_groups["complete-fanout"]["children"][child_id] == "completed"
      assert done_parent.child_groups["complete-fanout"]["summary"]["completed"] == 1
    after
      restore_backend(previous_backend)
      FerricStore.Instance.cleanup(ctx.name)
    end
  end

  test "Flow cross-shard retry exhaustion resolves parent through WARaft", %{root: root} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    ctx = build_ctx(Path.join(root, "flow-cross-shard-retry"), shard_count: 2)
    parent_id = "router-flow-parent-retry-#{System.unique_integer([:positive])}"
    child_id = "router-flow-child-retry-#{System.unique_integer([:positive])}"
    parent_partition = flow_partition_for_shard(ctx, parent_id, 0)
    child_partition = flow_partition_for_shard(ctx, child_id, 1)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

      assert {:ok, _waiting_parent} =
               setup_cross_shard_flow_child(
                 ctx,
                 parent_id,
                 child_id,
                 parent_partition,
                 child_partition,
                 group_id: "retry-fanout",
                 on_child_failed: :fail_parent
               )

      claimed = claim_flow_child!(ctx, child_id, child_partition, "worker-retry")

      assert :ok =
               Ferricstore.Flow.retry(ctx, child_id, claimed.lease_token,
                 partition_key: child_partition,
                 fencing_token: claimed.fencing_token,
                 now_ms: 2_000,
                 retry: [max_retries: 0, exhausted_to: "failed"]
               )

      assert {:ok, failed_child} =
               Ferricstore.Flow.get(ctx, child_id, partition_key: child_partition)

      assert failed_child.state == "failed"

      assert {:ok, failed_parent} =
               Ferricstore.Flow.get(ctx, parent_id, partition_key: parent_partition)

      assert failed_parent.state == "children_failed"
      assert failed_parent.child_groups["retry-fanout"]["children"][child_id] == "failed"
      assert failed_parent.child_groups["retry-fanout"]["summary"]["failed"] == 1
    after
      restore_backend(previous_backend)
      FerricStore.Instance.cleanup(ctx.name)
    end
  end

  test "Flow cross-shard fail and cancel propagate parent policy through WARaft", %{root: root} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    ctx = build_ctx(Path.join(root, "flow-cross-shard-fail-cancel"), shard_count: 2)
    fail_parent = "router-flow-parent-fail-#{System.unique_integer([:positive])}"
    fail_child = "router-flow-child-fail-#{System.unique_integer([:positive])}"
    cancel_parent = "router-flow-parent-cancel-#{System.unique_integer([:positive])}"
    cancel_child = "router-flow-child-cancel-#{System.unique_integer([:positive])}"
    parent_partition = flow_partition_for_shard(ctx, fail_parent, 0)
    child_partition = flow_partition_for_shard(ctx, fail_child, 1)
    cancel_parent_partition = flow_partition_for_shard(ctx, cancel_parent, 0)
    cancel_child_partition = flow_partition_for_shard(ctx, cancel_child, 1)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

      assert {:ok, _waiting_parent} =
               setup_cross_shard_flow_child(
                 ctx,
                 fail_parent,
                 fail_child,
                 parent_partition,
                 child_partition,
                 group_id: "fail-fanout",
                 on_child_failed: :fail_parent
               )

      claimed = claim_flow_child!(ctx, fail_child, child_partition, "worker-fail")

      assert :ok =
               Ferricstore.Flow.fail(ctx, fail_child, claimed.lease_token,
                 partition_key: child_partition,
                 fencing_token: claimed.fencing_token,
                 error: "boom",
                 now_ms: 2_000
               )

      assert {:ok, failed_parent} =
               Ferricstore.Flow.get(ctx, fail_parent, partition_key: parent_partition)

      assert failed_parent.state == "children_failed"
      assert failed_parent.child_groups["fail-fanout"]["children"][fail_child] == "failed"

      assert {:ok, waiting_cancel_parent} =
               setup_cross_shard_flow_child(
                 ctx,
                 cancel_parent,
                 cancel_child,
                 cancel_parent_partition,
                 cancel_child_partition,
                 group_id: "cancel-fanout",
                 on_parent_closed: :cancel_children
               )

      assert :ok =
               Ferricstore.Flow.cancel(ctx, cancel_parent,
                 partition_key: cancel_parent_partition,
                 fencing_token: waiting_cancel_parent.fencing_token,
                 now_ms: 3_000
               )

      assert {:ok, cancelled_child} =
               Ferricstore.Flow.get(ctx, cancel_child, partition_key: cancel_child_partition)

      assert cancelled_child.state == "cancelled"

      assert {:ok, cancelled_parent} =
               Ferricstore.Flow.get(ctx, cancel_parent, partition_key: cancel_parent_partition)

      assert cancelled_parent.child_groups["cancel-fanout"]["children"][cancel_child] ==
               "cancelled"
    after
      restore_backend(previous_backend)
      FerricStore.Instance.cleanup(ctx.name)
    end
  end

  test "Flow retention cleanup scans all WARaft shards", %{root: root} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    ctx = build_ctx(Path.join(root, "flow-retention-cleanup"), shard_count: 2)
    flow_type = "router-flow-retention-#{System.unique_integer([:positive])}"
    flow_a = "router-flow-retention-a-#{System.unique_integer([:positive])}"
    flow_b = "router-flow-retention-b-#{System.unique_integer([:positive])}"
    partition_a = flow_partition_for_shard(ctx, flow_a, 0)
    partition_b = flow_partition_for_shard(ctx, flow_b, 1)
    now_ms = System.system_time(:millisecond)
    cleanup_now_ms = now_ms + 1_000

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

      for {id, partition, worker} <- [
            {flow_a, partition_a, "worker-retention-a"},
            {flow_b, partition_b, "worker-retention-b"}
          ] do
        assert :ok =
                 Ferricstore.Flow.create(ctx, id,
                   type: flow_type,
                   state: "queued",
                   partition_key: partition,
                   payload: %{id: id},
                   retention_ttl_ms: 10,
                   run_at_ms: now_ms,
                   now_ms: now_ms
                 )

        assert {:ok, [claimed]} =
                 Ferricstore.Flow.claim_due(ctx, flow_type,
                   partition_key: partition,
                   worker: worker,
                   limit: 1,
                   now_ms: now_ms
                 )

        assert :ok =
                 Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
                   partition_key: partition,
                   fencing_token: claimed.fencing_token,
                   result: %{ok: true},
                   now_ms: now_ms + 10
                 )
      end

      assert {:ok, cleaned} =
               Ferricstore.Flow.retention_cleanup(ctx, limit: 10, now_ms: cleanup_now_ms)

      assert cleaned.flows == 2
      assert cleaned.history >= 2
      assert cleaned.values >= 4

      assert {:ok, nil} = Ferricstore.Flow.get(ctx, flow_a, partition_key: partition_a)
      assert {:ok, nil} = Ferricstore.Flow.get(ctx, flow_b, partition_key: partition_b)
    after
      restore_backend(previous_backend)
      FerricStore.Instance.cleanup(ctx.name)
    end
  end

  test "Flow cross-shard terminal many commands resolve parents through WARaft", %{root: root} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    ctx = build_ctx(Path.join(root, "flow-cross-terminal-many"), shard_count: 2)
    complete_parent = "router-flow-many-parent-complete-#{System.unique_integer([:positive])}"
    complete_child = "router-flow-many-child-complete-#{System.unique_integer([:positive])}"
    retry_parent = "router-flow-many-parent-retry-#{System.unique_integer([:positive])}"
    retry_child = "router-flow-many-child-retry-#{System.unique_integer([:positive])}"
    fail_parent = "router-flow-many-parent-fail-#{System.unique_integer([:positive])}"
    fail_child = "router-flow-many-child-fail-#{System.unique_integer([:positive])}"
    cancel_parent = "router-flow-many-parent-cancel-#{System.unique_integer([:positive])}"
    cancel_child = "router-flow-many-child-cancel-#{System.unique_integer([:positive])}"

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

      {complete_parent_partition, complete_child_partition} =
        setup_cross_shard_child_for_many!(ctx, complete_parent, complete_child, "many-complete")

      complete_claim = claim_flow_child!(ctx, complete_child, complete_child_partition, "many-c")

      assert :ok =
               Ferricstore.Flow.complete_many(
                 ctx,
                 nil,
                 [
                   %{
                     id: complete_child,
                     partition_key: complete_child_partition,
                     lease_token: complete_claim.lease_token,
                     fencing_token: complete_claim.fencing_token
                   }
                 ],
                 now_ms: 2_000
               )

      assert {:ok, complete_done} =
               Ferricstore.Flow.get(ctx, complete_parent,
                 partition_key: complete_parent_partition
               )

      assert complete_done.state == "children_done"

      assert complete_done.child_groups["many-complete"]["children"][complete_child] ==
               "completed"

      {retry_parent_partition, retry_child_partition} =
        setup_cross_shard_child_for_many!(ctx, retry_parent, retry_child, "many-retry",
          on_child_failed: :fail_parent
        )

      retry_claim = claim_flow_child!(ctx, retry_child, retry_child_partition, "many-r")

      assert :ok =
               Ferricstore.Flow.retry_many(
                 ctx,
                 nil,
                 [
                   %{
                     id: retry_child,
                     partition_key: retry_child_partition,
                     lease_token: retry_claim.lease_token,
                     fencing_token: retry_claim.fencing_token
                   }
                 ],
                 now_ms: 2_000,
                 retry: [max_retries: 0, exhausted_to: "failed"]
               )

      assert {:ok, retry_failed} =
               Ferricstore.Flow.get(ctx, retry_parent, partition_key: retry_parent_partition)

      assert retry_failed.state == "children_failed"
      assert retry_failed.child_groups["many-retry"]["children"][retry_child] == "failed"

      {fail_parent_partition, fail_child_partition} =
        setup_cross_shard_child_for_many!(ctx, fail_parent, fail_child, "many-fail",
          on_child_failed: :fail_parent
        )

      fail_claim = claim_flow_child!(ctx, fail_child, fail_child_partition, "many-f")

      assert :ok =
               Ferricstore.Flow.fail_many(
                 ctx,
                 nil,
                 [
                   %{
                     id: fail_child,
                     partition_key: fail_child_partition,
                     lease_token: fail_claim.lease_token,
                     fencing_token: fail_claim.fencing_token
                   }
                 ],
                 error: "boom",
                 now_ms: 2_000
               )

      assert {:ok, fail_done} =
               Ferricstore.Flow.get(ctx, fail_parent, partition_key: fail_parent_partition)

      assert fail_done.state == "children_failed"
      assert fail_done.child_groups["many-fail"]["children"][fail_child] == "failed"

      {cancel_parent_partition, cancel_child_partition} =
        setup_cross_shard_child_for_many!(ctx, cancel_parent, cancel_child, "many-cancel",
          on_parent_closed: :cancel_children
        )

      assert {:ok, waiting_cancel_parent} =
               Ferricstore.Flow.get(ctx, cancel_parent, partition_key: cancel_parent_partition)

      assert :ok =
               Ferricstore.Flow.cancel_many(
                 ctx,
                 nil,
                 [
                   %{
                     id: cancel_parent,
                     partition_key: cancel_parent_partition,
                     fencing_token: waiting_cancel_parent.fencing_token
                   }
                 ],
                 now_ms: 2_000
               )

      assert {:ok, cancelled_child} =
               Ferricstore.Flow.get(ctx, cancel_child, partition_key: cancel_child_partition)

      assert cancelled_child.state == "cancelled"
    after
      restore_backend(previous_backend)
      FerricStore.Instance.cleanup(ctx.name)
    end
  end

  test "file-backed probabilistic commands use WARaft as the selected backend", %{ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

      meta = {:bloom_meta, %{capacity: 128, error_rate: 0.01}}
      assert :ok = Router.prob_write(ctx, {:bloom_create, "router:bf", 128, 3, meta})
      assert {:ok, 1} = Router.prob_write(ctx, {:bloom_add, "router:bf", "a", nil})

      assert File.exists?(
               Path.join(
                 Path.join(
                   Ferricstore.DataDir.shard_data_path(
                     ctx.data_dir,
                     Router.shard_for(ctx, "router:bf")
                   ),
                   "prob"
                 ),
                 "#{Base.url_encode64("router:bf", padding: false)}.bloom"
               )
             )
    after
      restore_backend(previous_backend)
    end
  end

  test "CMS Cuckoo TopK and TDigest commands use WARaft as the selected backend", %{ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

      assert :ok =
               Ferricstore.Commands.CMS.handle_ast({:cms_initbydim, "router:cms", 64, 4}, ctx)

      assert [3] =
               Ferricstore.Commands.CMS.handle_ast(
                 {:cms_incrby, "router:cms", [{"hot", 3}]},
                 ctx
               )

      assert [3] = Ferricstore.Commands.CMS.handle_ast({:cms_query, ["router:cms", "hot"]}, ctx)

      assert :ok = Ferricstore.Commands.Cuckoo.handle_ast({:cf_reserve, "router:cf", 128}, ctx)
      assert 1 = Ferricstore.Commands.Cuckoo.handle_ast({:cf_add, ["router:cf", "seen"]}, ctx)
      assert 1 = Ferricstore.Commands.Cuckoo.handle_ast({:cf_exists, ["router:cf", "seen"]}, ctx)
      assert 1 = Ferricstore.Commands.Cuckoo.handle_ast({:cf_del, ["router:cf", "seen"]}, ctx)
      assert 0 = Ferricstore.Commands.Cuckoo.handle_ast({:cf_exists, ["router:cf", "seen"]}, ctx)

      assert :ok =
               Ferricstore.Commands.TopK.handle_ast(
                 {:topk_reserve, "router:topk", 3, 8, 4, 0.9},
                 ctx
               )

      assert [nil, nil] =
               Ferricstore.Commands.TopK.handle_ast({:topk_add, ["router:topk", "a", "b"]}, ctx)

      assert [nil] =
               Ferricstore.Commands.TopK.handle_ast(
                 {:topk_incrby, "router:topk", [{"a", 5}]},
                 ctx
               )

      assert [6] = Ferricstore.Commands.TopK.handle_ast({:topk_count, ["router:topk", "a"]}, ctx)

      assert :ok =
               Ferricstore.Commands.TDigest.handle_ast(
                 {:tdigest_create, "router:td", 100},
                 ctx
               )

      assert :ok =
               Ferricstore.Commands.TDigest.handle_ast(
                 {:tdigest_add, "router:td", [1.0, 2.0, 3.0]},
                 ctx
               )

      assert [median] =
               Ferricstore.Commands.TDigest.handle_ast(
                 {:tdigest_quantile, "router:td", [0.5]},
                 ctx
               )

      assert median != "nan"
    after
      restore_backend(previous_backend)
    end
  end

  test "file-backed probabilistic commands survive WARaft restart", %{root: root, ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    suffix = System.unique_integer([:positive])

    bloom_key = "router:bf-restart:#{suffix}"
    cms_key = "router:cms-restart:#{suffix}"
    cuckoo_key = "router:cf-restart:#{suffix}"
    topk_key = "router:topk-restart:#{suffix}"
    tdigest_key = "router:td-restart:#{suffix}"

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert :ok =
               Ferricstore.Commands.Bloom.handle_ast({:bf_reserve, bloom_key, 0.01, 128}, ctx)

      assert 1 = Ferricstore.Commands.Bloom.handle_ast({:bf_add, [bloom_key, "sensor-a"]}, ctx)
      assert 1 = Ferricstore.Commands.Bloom.handle_ast({:bf_exists, [bloom_key, "sensor-a"]}, ctx)

      assert :ok = Ferricstore.Commands.CMS.handle_ast({:cms_initbydim, cms_key, 64, 4}, ctx)

      assert [7] =
               Ferricstore.Commands.CMS.handle_ast({:cms_incrby, cms_key, [{"hot", 7}]}, ctx)

      assert :ok = Ferricstore.Commands.Cuckoo.handle_ast({:cf_reserve, cuckoo_key, 128}, ctx)
      assert 1 = Ferricstore.Commands.Cuckoo.handle_ast({:cf_add, [cuckoo_key, "seen"]}, ctx)

      assert :ok =
               Ferricstore.Commands.TopK.handle_ast(
                 {:topk_reserve, topk_key, 3, 8, 4, 0.9},
                 ctx
               )

      assert [nil] = Ferricstore.Commands.TopK.handle_ast({:topk_add, [topk_key, "a"]}, ctx)

      assert :ok =
               Ferricstore.Commands.TDigest.handle_ast(
                 {:tdigest_create, tdigest_key, 100},
                 ctx
               )

      assert :ok =
               Ferricstore.Commands.TDigest.handle_ast(
                 {:tdigest_add, tdigest_key, [1.0, 2.0, 3.0]},
                 ctx
               )

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert 1 =
               Ferricstore.Commands.Bloom.handle_ast(
                 {:bf_exists, [bloom_key, "sensor-a"]},
                 restarted_ctx
               )

      assert [7] =
               Ferricstore.Commands.CMS.handle_ast(
                 {:cms_query, [cms_key, "hot"]},
                 restarted_ctx
               )

      assert 1 =
               Ferricstore.Commands.Cuckoo.handle_ast(
                 {:cf_exists, [cuckoo_key, "seen"]},
                 restarted_ctx
               )

      assert [1] =
               Ferricstore.Commands.TopK.handle_ast(
                 {:topk_count, [topk_key, "a"]},
                 restarted_ctx
               )

      assert [median] =
               Ferricstore.Commands.TDigest.handle_ast(
                 {:tdigest_quantile, tdigest_key, [0.5]},
                 restarted_ctx
               )

      assert median != "nan"
      FerricStore.Instance.cleanup(restarted_ctx.name)
    after
      restore_backend(previous_backend)
    end
  end

  test "probabilistic merge commands survive WARaft restart", %{root: root, ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    suffix = System.unique_integer([:positive])

    hll_src1 = "router:hll-merge-src1:#{suffix}"
    hll_src2 = "router:hll-merge-src2:#{suffix}"
    hll_dest = "router:hll-merge-dest:#{suffix}"

    cms_src1 = "router:cms-merge-src1:#{suffix}"
    cms_src2 = "router:cms-merge-src2:#{suffix}"
    cms_dest = "router:cms-merge-dest:#{suffix}"

    td_src1 = "router:td-merge-src1:#{suffix}"
    td_src2 = "router:td-merge-src2:#{suffix}"
    td_dest = "router:td-merge-dest:#{suffix}"

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert 1 =
               Ferricstore.Commands.HyperLogLog.handle_ast(
                 {:pfadd, [hll_src1, "a", "b"]},
                 ctx
               )

      assert 1 =
               Ferricstore.Commands.HyperLogLog.handle_ast(
                 {:pfadd, [hll_src2, "c", "d"]},
                 ctx
               )

      assert :ok =
               Ferricstore.Commands.HyperLogLog.handle_ast(
                 {:pfmerge, [hll_dest, hll_src1, hll_src2]},
                 ctx
               )

      for key <- [cms_src1, cms_src2, cms_dest] do
        assert :ok = Ferricstore.Commands.CMS.handle_ast({:cms_initbydim, key, 64, 4}, ctx)
      end

      assert [2] =
               Ferricstore.Commands.CMS.handle_ast({:cms_incrby, cms_src1, [{"hot", 2}]}, ctx)

      assert [4] =
               Ferricstore.Commands.CMS.handle_ast({:cms_incrby, cms_src2, [{"hot", 4}]}, ctx)

      assert :ok =
               Ferricstore.Commands.CMS.handle_ast(
                 {:cms_merge, cms_dest, [cms_src1, cms_src2], [1, 1]},
                 ctx
               )

      for {key, values} <- [{td_src1, [1.0, 2.0]}, {td_src2, [9.0, 10.0]}] do
        assert :ok = Ferricstore.Commands.TDigest.handle_ast({:tdigest_create, key, 100}, ctx)
        assert :ok = Ferricstore.Commands.TDigest.handle_ast({:tdigest_add, key, values}, ctx)
      end

      assert :ok =
               Ferricstore.Commands.TDigest.handle_ast(
                 {:tdigest_merge, td_dest, [td_src1, td_src2], []},
                 ctx
               )

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      hll_count =
        Ferricstore.Commands.HyperLogLog.handle_ast(
          {:pfcount, [hll_dest]},
          restarted_ctx
        )

      assert hll_count >= 3

      assert [6] =
               Ferricstore.Commands.CMS.handle_ast(
                 {:cms_query, [cms_dest, "hot"]},
                 restarted_ctx
               )

      assert [median] =
               Ferricstore.Commands.TDigest.handle_ast(
                 {:tdigest_quantile, td_dest, [0.5]},
                 restarted_ctx
               )

      assert median != "nan"
    after
      restore_backend(previous_backend)
    end
  end

  test "stream XADD uses WARaft-backed compound writes", %{ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

      key = "router:stream:#{System.unique_integer([:positive])}"

      assert "1-0" =
               Ferricstore.Commands.Stream.handle_ast(
                 {:xadd, key, {{:explicit, 1, 0}, ["f", "v"], nil, false}},
                 ctx
               )

      assert 1 = Ferricstore.Commands.Stream.handle_ast({:xlen, key}, ctx)
    after
      restore_backend(previous_backend)
    end
  end

  test "stream consumer group state survives WARaft restart", %{root: root, ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      key = "router:stream-group:#{System.unique_integer([:positive])}"

      assert "1-0" =
               Ferricstore.Commands.Stream.handle_ast(
                 {:xadd, key, {{:explicit, 1, 0}, ["f", "v"], nil, false}},
                 ctx
               )

      assert :ok =
               Ferricstore.Commands.Stream.handle_ast(
                 {:xgroup_create, key, "group-a", "0", false},
                 ctx
               )

      assert [[^key, [["1-0", "f", "v"]]]] =
               Ferricstore.Commands.Stream.handle_ast(
                 {:xreadgroup, "group-a", "consumer-a", {10, :no_block, [{key, ">"}]}},
                 ctx
               )

      assert :ok = WARaftBackend.stop()
      Ferricstore.Commands.Stream.clear_local_state()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert [[^key, [["1-0", "f", "v"]]]] =
               Ferricstore.Commands.Stream.handle_ast(
                 {:xreadgroup, "group-a", "consumer-a", {10, :no_block, [{key, "0"}]}},
                 restarted_ctx
               )

      assert 1 =
               Ferricstore.Commands.Stream.handle_ast(
                 {:xack, key, "group-a", ["1-0"]},
                 restarted_ctx
               )

      assert :ok = WARaftBackend.stop()
      Ferricstore.Commands.Stream.clear_local_state()
      FerricStore.Instance.cleanup(restarted_ctx.name)

      acked_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(acked_ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert [] =
               Ferricstore.Commands.Stream.handle_ast(
                 {:xreadgroup, "group-a", "consumer-a", {10, :no_block, [{key, "0"}]}},
                 acked_ctx
               )
    after
      restore_backend(previous_backend)
    end
  end

  test "stream XDEL and XTRIM mutations survive WARaft restart", %{root: root, ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      key = "router:stream-trim:#{System.unique_integer([:positive])}"

      for id <- ["1-0", "2-0", "3-0", "4-0", "5-0"] do
        assert ^id =
                 Ferricstore.Commands.Stream.handle_ast(
                   {:xadd, key,
                    {{:explicit, parse_stream_ms(id), 0}, ["f", "v:#{id}"], nil, false}},
                   ctx
                 )
      end

      assert 1 = Ferricstore.Commands.Stream.handle_ast({:xdel, key, ["2-0"]}, ctx)
      assert 2 = Ferricstore.Commands.Stream.handle_ast({:xtrim, key, {:maxlen, false, 2}}, ctx)

      assert [["4-0", "f", "v:4-0"], ["5-0", "f", "v:5-0"]] =
               Ferricstore.Commands.Stream.handle_ast({:xrange, key, "-", "+", nil}, ctx)

      assert :ok = WARaftBackend.stop()
      Ferricstore.Commands.Stream.clear_local_state()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      prefix = Ferricstore.Store.CompoundKey.stream_prefix(key)
      assert 2 = Router.compound_count(restarted_ctx, key, prefix)
      assert ["4-0", "5-0"] = Router.compound_fields(restarted_ctx, key, prefix)
      assert 2 = Ferricstore.Commands.Stream.handle_ast({:xlen, key}, restarted_ctx)

      assert [["4-0", "f", "v:4-0"], ["5-0", "f", "v:5-0"]] =
               Ferricstore.Commands.Stream.handle_ast(
                 {:xrange, key, "-", "+", nil},
                 restarted_ctx
               )
    after
      restore_backend(previous_backend)
    end
  end

  test "hash field metadata reads survive WARaft restart", %{root: root, ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      key = "router:hash-ttl:#{System.unique_integer([:positive])}"

      assert 1 =
               Ferricstore.Commands.Hash.handle_ast(
                 {:hsetex, key, 60, ["field", "value"]},
                 ctx
               )

      assert [ttl_before] = Ferricstore.Commands.Hash.handle_ast({:hpttl, key, ["field"]}, ctx)
      assert ttl_before > 0

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert ["value"] =
               Ferricstore.Commands.Hash.handle_ast({:hmget, [key, "field"]}, restarted_ctx)

      assert [ttl_after] =
               Ferricstore.Commands.Hash.handle_ast({:hpttl, key, ["field"]}, restarted_ctx)

      assert ttl_after > 0
      assert ttl_after <= ttl_before

      assert ["value"] =
               Ferricstore.Commands.Hash.handle_ast(
                 {:hgetex, key, {:px, 120_000}, ["field"]},
                 restarted_ctx
               )

      assert [extended_ttl] =
               Ferricstore.Commands.Hash.handle_ast({:hpttl, key, ["field"]}, restarted_ctx)

      assert extended_ttl > ttl_after
    after
      restore_backend(previous_backend)
    end
  end

  test "advanced hash mutations survive WARaft restart", %{root: root, ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    key = "router:hash-advanced:#{System.unique_integer([:positive])}"

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert 3 =
               Ferricstore.Commands.Hash.handle_ast(
                 {:hset, [key, "int", "1", "float", "1.5", "delete", "gone"]},
                 ctx
               )

      assert 4 = Ferricstore.Commands.Hash.handle_ast({:hincrby, key, "int", 3}, ctx)

      assert "2.0" =
               Ferricstore.Commands.Hash.handle_ast({:hincrbyfloat, key, "float", 0.5}, ctx)

      assert 1 =
               Ferricstore.Commands.Hash.handle_ast(
                 {:hsetnx, key, "created-once", "first"},
                 ctx
               )

      assert 0 =
               Ferricstore.Commands.Hash.handle_ast(
                 {:hsetnx, key, "created-once", "second"},
                 ctx
               )

      assert ["gone", nil] =
               Ferricstore.Commands.Hash.handle_ast({:hgetdel, key, ["delete", "missing"]}, ctx)

      assert [1] = Ferricstore.Commands.Hash.handle_ast({:hpexpire, key, 60_000, ["int"]}, ctx)
      assert [ttl_before] = Ferricstore.Commands.Hash.handle_ast({:hpttl, key, ["int"]}, ctx)
      assert ttl_before > 0
      assert [1] = Ferricstore.Commands.Hash.handle_ast({:hpersist, key, ["int"]}, ctx)

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert ["4", "2.0", "first", nil] =
               Ferricstore.Commands.Hash.handle_ast(
                 {:hmget, [key, "int", "float", "created-once", "delete"]},
                 restarted_ctx
               )

      assert [cursor, flat_fields] =
               Ferricstore.Commands.Hash.handle_ast({:hscan, key, 0, []}, restarted_ctx)

      assert cursor in ["0", 0]
      assert "int" in flat_fields
      assert "float" in flat_fields
      assert "created-once" in flat_fields
      refute "delete" in flat_fields

      assert [expiretime] =
               Ferricstore.Commands.Hash.handle_ast({:hexpiretime, key, ["int"]}, restarted_ctx)

      assert expiretime == -1
    after
      restore_backend(previous_backend)
    end
  end

  test "zset index helpers read directly from WARaft keydir after restart", %{
    root: root,
    ctx: ctx
  } do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      key = "router:zset-index:#{System.unique_integer([:positive])}"

      assert 3 =
               Ferricstore.Commands.SortedSet.handle_ast(
                 {:zadd, key, [], [{2.0, "b"}, {1.0, "a"}, {3.0, "c"}]},
                 ctx
               )

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert {:ok, [{"a", 1.0}, {"b", 2.0}]} =
               Router.zset_rank_range(restarted_ctx, key, 0, 1, false)

      assert {:ok, [{"c", 3.0}, {"b", 2.0}]} =
               Router.zset_rank_range(restarted_ctx, key, 0, 1, true)

      assert {:ok, 1} = Router.zset_member_rank(restarted_ctx, key, "b", false)
      assert {:ok, 2} = Router.zset_score_count(restarted_ctx, key, {:inclusive, 1.5}, :inf)

      assert {:ok, [{"b", 2.0}, {"c", 3.0}]} =
               Router.zset_score_range(restarted_ctx, key, {:inclusive, 2.0}, :inf, false)

      assert {:ok, [{"b", 2.0}]} =
               Router.zset_score_range_slice(restarted_ctx, key, :neg_inf, :inf, false, 1, 1)
    after
      restore_backend(previous_backend)
    end
  end

  test "zset update and pop mutations survive WARaft restart", %{root: root, ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    key = "router:zset-mutate:#{System.unique_integer([:positive])}"

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert 3 =
               Ferricstore.Commands.SortedSet.handle(
                 "ZADD",
                 [key, "1.0", "a", "2.0", "b", "3.0", "c"],
                 ctx
               )

      assert 1 = Ferricstore.Commands.SortedSet.handle("ZREM", [key, "a"], ctx)
      incr_result = Ferricstore.Commands.SortedSet.handle("ZINCRBY", [key, "2.0", "b"], ctx)
      {score, ""} = Float.parse(incr_result)
      assert_in_delta 4.0, score, 0.001
      assert ["b", score_text] = Ferricstore.Commands.SortedSet.handle("ZPOPMAX", [key], ctx)
      {popped_score, ""} = Float.parse(score_text)
      assert_in_delta 4.0, popped_score, 0.001

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert 1 = Ferricstore.Commands.SortedSet.handle("ZCARD", [key], restarted_ctx)
      assert nil == Ferricstore.Commands.SortedSet.handle("ZSCORE", [key, "a"], restarted_ctx)
      assert nil == Ferricstore.Commands.SortedSet.handle("ZSCORE", [key, "b"], restarted_ctx)

      assert ["c", "3.0"] =
               Ferricstore.Commands.SortedSet.handle(
                 "ZRANGE",
                 [key, "0", "-1", "WITHSCORES"],
                 restarted_ctx
               )
    after
      restore_backend(previous_backend)
    end
  end

  test "advanced zset range and pop mutations survive WARaft restart", %{root: root, ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    key = "router:zset-advanced:#{System.unique_integer([:positive])}"

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert 4 =
               Ferricstore.Commands.SortedSet.handle_ast(
                 {:zadd, key, [], [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}]},
                 ctx
               )

      assert ["a", "1.0"] =
               Ferricstore.Commands.SortedSet.handle_ast({:zpopmin, key}, ctx)

      assert ["d", "4.0"] =
               Ferricstore.Commands.SortedSet.handle_ast({:zpopmax, key}, ctx)

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert ["b", "2.0", "c", "3.0"] =
               Ferricstore.Commands.SortedSet.handle(
                 "ZRANGEBYSCORE",
                 [key, "2", "3", "WITHSCORES"],
                 restarted_ctx
               )

      assert ["c", "3.0", "b", "2.0"] =
               Ferricstore.Commands.SortedSet.handle(
                 "ZREVRANGEBYSCORE",
                 [key, "3", "2", "WITHSCORES"],
                 restarted_ctx
               )

      assert ["2.0", nil] =
               Ferricstore.Commands.SortedSet.handle_ast(
                 {:zmscore, [key, "b", "missing"]},
                 restarted_ctx
               )

      assert [cursor, scanned] =
               Ferricstore.Commands.SortedSet.handle_ast(
                 {:zscan, key, 0, []},
                 restarted_ctx
               )

      assert cursor in ["0", 0]
      assert "b" in scanned
      assert "2.0" in scanned
      assert "c" in scanned
      assert "3.0" in scanned
      refute "a" in scanned
      refute "d" in scanned
    after
      restore_backend(previous_backend)
    end
  end

  test "list commands survive WARaft restart without shard process reads", %{root: root, ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      key = "router:list:#{System.unique_integer([:positive])}"

      assert 3 = Ferricstore.Commands.List.handle_ast({:rpush, [key, "a", "b", "c"]}, ctx)
      assert ["a", "b", "c"] = Ferricstore.Commands.List.handle_ast({:lrange, key, 0, -1}, ctx)
      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert 3 = Ferricstore.Commands.List.handle_ast({:llen, key}, restarted_ctx)

      assert ["a", "b", "c"] =
               Ferricstore.Commands.List.handle_ast({:lrange, key, 0, -1}, restarted_ctx)

      assert "a" = Ferricstore.Commands.List.handle_ast({:lpop, key}, restarted_ctx)
      assert 2 = Ferricstore.Commands.List.handle_ast({:llen, key}, restarted_ctx)

      assert ["b", "c"] =
               Ferricstore.Commands.List.handle_ast({:lrange, key, 0, -1}, restarted_ctx)
    after
      restore_backend(previous_backend)
    end
  end

  test "advanced list mutations survive WARaft restart", %{root: root, ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    key = "router:list-advanced:#{System.unique_integer([:positive])}"
    missing_key = "router:list-advanced:missing:#{System.unique_integer([:positive])}"

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert 4 = Ferricstore.Commands.List.handle_ast({:rpush, [key, "a", "b", "c", "d"]}, ctx)
      assert :ok = Ferricstore.Commands.List.handle_ast({:lset, key, 1, "B"}, ctx)
      assert 5 = Ferricstore.Commands.List.handle_ast({:linsert, key, :after, "B", "mid"}, ctx)
      assert 1 = Ferricstore.Commands.List.handle_ast({:lrem, key, 1, "c"}, ctx)
      assert :ok = Ferricstore.Commands.List.handle_ast({:ltrim, key, 1, 2}, ctx)
      assert 3 = Ferricstore.Commands.List.handle_ast({:lpushx, [key, "L"]}, ctx)
      assert 4 = Ferricstore.Commands.List.handle_ast({:rpushx, [key, "R"]}, ctx)
      assert 0 = Ferricstore.Commands.List.handle_ast({:lpushx, [missing_key, "x"]}, ctx)

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert ["L", "B", "mid", "R"] =
               Ferricstore.Commands.List.handle_ast({:lrange, key, 0, -1}, restarted_ctx)

      assert "mid" = Ferricstore.Commands.List.handle_ast({:lindex, key, 2}, restarted_ctx)
      assert 1 = Ferricstore.Commands.List.handle("LPOS", [key, "B"], restarted_ctx)
      assert 0 = Ferricstore.Commands.List.handle_ast({:llen, missing_key}, restarted_ctx)
    after
      restore_backend(previous_backend)
    end
  end

  test "set commands survive WARaft restart without shard process reads", %{root: root, ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      key = "router:set:#{System.unique_integer([:positive])}"

      assert 3 = Ferricstore.Commands.Set.handle_ast({:sadd, [key, "a", "b", "c"]}, ctx)
      assert 1 = Ferricstore.Commands.Set.handle_ast({:sismember, key, "b"}, ctx)
      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert 3 = Ferricstore.Commands.Set.handle_ast({:scard, key}, restarted_ctx)
      assert 1 = Ferricstore.Commands.Set.handle_ast({:sismember, key, "b"}, restarted_ctx)

      assert [1, 0, 1] =
               Ferricstore.Commands.Set.handle_ast(
                 {:smismember, [key, "a", "x", "c"]},
                 restarted_ctx
               )

      assert ["a", "b", "c"] =
               {:smembers, key}
               |> Ferricstore.Commands.Set.handle_ast(restarted_ctx)
               |> Enum.sort()

      assert 1 = Ferricstore.Commands.Set.handle_ast({:srem, [key, "b"]}, restarted_ctx)
      assert 2 = Ferricstore.Commands.Set.handle_ast({:scard, key}, restarted_ctx)
      assert 0 = Ferricstore.Commands.Set.handle_ast({:sismember, key, "b"}, restarted_ctx)
    after
      restore_backend(previous_backend)
    end
  end

  test "advanced set store and pop mutations survive WARaft restart", %{root: root, ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    suffix = System.unique_integer([:positive])
    left = "router:set-advanced:left:#{suffix}"
    right = "router:set-advanced:right:#{suffix}"
    inter = "router:set-advanced:inter:#{suffix}"
    diff = "router:set-advanced:diff:#{suffix}"
    pop = "router:set-advanced:pop:#{suffix}"

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert 3 = Ferricstore.Commands.Set.handle_ast({:sadd, [left, "a", "b", "c"]}, ctx)
      assert 3 = Ferricstore.Commands.Set.handle_ast({:sadd, [right, "b", "c", "d"]}, ctx)
      assert 1 = Ferricstore.Commands.Set.handle_ast({:sadd, [pop, "only"]}, ctx)
      assert 2 = Ferricstore.Commands.Set.handle_ast({:sinterstore, [inter, left, right]}, ctx)
      assert 1 = Ferricstore.Commands.Set.handle_ast({:sdiffstore, [diff, left, right]}, ctx)
      assert "only" = Ferricstore.Commands.Set.handle_ast({:spop, pop}, ctx)

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert ["b", "c"] =
               Ferricstore.Commands.Set.handle_ast({:smembers, inter}, restarted_ctx)
               |> Enum.sort()

      assert ["a"] =
               Ferricstore.Commands.Set.handle_ast({:smembers, diff}, restarted_ctx)
               |> Enum.sort()

      assert 0 = Ferricstore.Commands.Set.handle_ast({:scard, pop}, restarted_ctx)

      assert 2 =
               Ferricstore.Commands.Set.handle_ast(
                 {:sintercard, [left, right], 0},
                 restarted_ctx
               )

      assert [cursor, scanned] =
               Ferricstore.Commands.Set.handle_ast({:sscan, inter, 0, []}, restarted_ctx)

      assert cursor in ["0", 0]
      assert "b" in scanned
      assert "c" in scanned
      refute "a" in scanned
    after
      restore_backend(previous_backend)
    end
  end

  test "cross-shard list and set mutations survive WARaft restart", %{root: root} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    ctx = build_ctx(Path.join(root, "compound-cross-shard"), shard_count: 2)

    list_src = key_for_shard(ctx, 0, "router:list-cross:src")
    list_dst = key_for_shard(ctx, 1, "router:list-cross:dst")
    set_src = key_for_shard(ctx, 0, "router:set-cross:src")
    set_dst = key_for_shard(ctx, 1, "router:set-cross:dst")
    set_union_dst = key_for_shard(ctx, 1, "router:set-cross:union")

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert 2 = Ferricstore.Commands.List.handle_ast({:rpush, [list_src, "a", "b"]}, ctx)
      assert 1 = Ferricstore.Commands.List.handle_ast({:rpush, [list_dst, "x"]}, ctx)

      assert "b" =
               Ferricstore.Commands.List.handle_ast(
                 {:lmove, list_src, list_dst, :right, :left},
                 ctx
               )

      assert 2 = Ferricstore.Commands.Set.handle_ast({:sadd, [set_src, "a", "b"]}, ctx)
      assert 1 = Ferricstore.Commands.Set.handle_ast({:sadd, [set_dst, "x"]}, ctx)
      assert 1 = Ferricstore.Commands.Set.handle_ast({:smove, set_src, set_dst, "b"}, ctx)

      assert 3 =
               Ferricstore.Commands.Set.handle_ast(
                 {:sunionstore, [set_union_dst, set_src, set_dst]},
                 ctx
               )

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(Path.join(root, "compound-cross-shard"), shard_count: 2)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert ["a"] =
               Ferricstore.Commands.List.handle_ast({:lrange, list_src, 0, -1}, restarted_ctx)

      assert ["b", "x"] =
               Ferricstore.Commands.List.handle_ast({:lrange, list_dst, 0, -1}, restarted_ctx)

      assert 0 = Ferricstore.Commands.Set.handle_ast({:sismember, set_src, "b"}, restarted_ctx)
      assert 1 = Ferricstore.Commands.Set.handle_ast({:sismember, set_dst, "b"}, restarted_ctx)

      assert ["a", "b", "x"] =
               {:smembers, set_union_dst}
               |> Ferricstore.Commands.Set.handle_ast(restarted_ctx)
               |> Enum.sort()
    after
      WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)
      restore_backend(previous_backend)
    end
  end

  test "blocking list immediate mutations survive WARaft restart", %{root: root} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    ctx = build_ctx(Path.join(root, "blocking-list"), shard_count: 2)

    blpop_key = key_for_shard(ctx, 0, "router:blocking:blpop")
    brpop_key = key_for_shard(ctx, 0, "router:blocking:brpop")
    move_src = key_for_shard(ctx, 0, "router:blocking:move-src")
    move_dst = key_for_shard(ctx, 1, "router:blocking:move-dst")

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert 2 = Ferricstore.Commands.List.handle_ast({:rpush, [blpop_key, "a", "b"]}, ctx)
      assert 2 = Ferricstore.Commands.List.handle_ast({:rpush, [brpop_key, "c", "d"]}, ctx)
      assert 2 = Ferricstore.Commands.List.handle_ast({:rpush, [move_src, "x", "y"]}, ctx)

      assert [^blpop_key, "a"] =
               Ferricstore.Commands.Blocking.handle("BLPOP", [blpop_key, "0"], ctx)

      assert [^brpop_key, "d"] =
               Ferricstore.Commands.Blocking.handle("BRPOP", [brpop_key, "0"], ctx)

      assert "y" =
               Ferricstore.Commands.Blocking.handle(
                 "BLMOVE",
                 [move_src, move_dst, "RIGHT", "LEFT", "0"],
                 ctx
               )

      assert [^brpop_key, ["c"]] =
               Ferricstore.Commands.Blocking.handle(
                 "BLMPOP",
                 ["0", "2", "router:blocking:missing", brpop_key, "LEFT", "COUNT", "1"],
                 ctx
               )

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(Path.join(root, "blocking-list"), shard_count: 2)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert ["b"] =
               Ferricstore.Commands.List.handle_ast({:lrange, blpop_key, 0, -1}, restarted_ctx)

      assert 0 = Ferricstore.Commands.List.handle_ast({:llen, brpop_key}, restarted_ctx)

      assert ["x"] =
               Ferricstore.Commands.List.handle_ast({:lrange, move_src, 0, -1}, restarted_ctx)

      assert ["y"] =
               Ferricstore.Commands.List.handle_ast({:lrange, move_dst, 0, -1}, restarted_ctx)
    after
      WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)
      restore_backend(previous_backend)
    end
  end

  test "geo commands survive WARaft restart without shard process reads", %{root: root, ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      key = "router:geo:#{System.unique_integer([:positive])}"

      assert 2 =
               Ferricstore.Commands.Geo.handle_ast(
                 {:geoadd, key, [],
                  [
                    {13.361389, 38.115556, "Palermo"},
                    {15.087269, 37.502669, "Catania"}
                  ]},
                 ctx
               )

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert [[_lng, _lat], [_lng2, _lat2]] =
               Ferricstore.Commands.Geo.handle_ast(
                 {:geopos, [key, "Palermo", "Catania"]},
                 restarted_ctx
               )

      assert distance_km_string =
               Ferricstore.Commands.Geo.handle_ast(
                 {:geodist, key, "Palermo", "Catania", "KM"},
                 restarted_ctx
               )

      {distance_km, ""} = Float.parse(distance_km_string)
      assert distance_km > 100.0
      assert distance_km < 300.0

      assert ["Palermo", "Catania"] =
               Ferricstore.Commands.Geo.handle_ast(
                 {:geosearch, key,
                  [
                    center: {:lonlat, 13.5, 38.0},
                    shape: {:radius, 200_000.0},
                    unit: "KM",
                    sort: :asc
                  ]},
                 restarted_ctx
               )
    after
      restore_backend(previous_backend)
    end
  end

  test "server KEYS sees WARaft keydir state without shard process reads", %{
    root: root,
    ctx: ctx
  } do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert :ok = Router.put(ctx, "router:keys:plain", "value", 0)

      assert 1 =
               Ferricstore.Commands.Hash.handle_ast(
                 {:hset, ["router:keys:hash", "field", "value"]},
                 ctx
               )

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert ["router:keys:hash", "router:keys:plain"] =
               "KEYS"
               |> Ferricstore.Commands.Server.handle(["router:keys:*"], restarted_ctx)
               |> Enum.sort()

      assert 2 = Ferricstore.Commands.Server.handle("DBSIZE", [], restarted_ctx)
    after
      restore_backend(previous_backend)
    end
  end

  test "server FLUSHDB clears WARaft-backed keys and stays clear after restart", %{
    root: root,
    ctx: ctx
  } do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    suffix = System.unique_integer([:positive])

    plain_key = "router:flush:plain:#{suffix}"
    hash_key = "router:flush:hash:#{suffix}"
    list_key = "router:flush:list:#{suffix}"

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert :ok = Router.put(ctx, plain_key, "value", 0)

      assert 1 =
               Ferricstore.Commands.Hash.handle_ast(
                 {:hset, [hash_key, "field", "value"]},
                 ctx
               )

      assert 1 = Ferricstore.Commands.List.handle_ast({:lpush, [list_key, "item"]}, ctx)
      assert 3 = Ferricstore.Commands.Server.handle("DBSIZE", [], ctx)

      assert :ok = Ferricstore.Commands.Server.handle("FLUSHDB", [], ctx)
      assert 0 = Ferricstore.Commands.Server.handle("DBSIZE", [], ctx)
      assert nil == Router.get(ctx, plain_key)
      assert 0 = Ferricstore.Commands.Hash.handle_ast({:hlen, hash_key}, ctx)
      assert 0 = Ferricstore.Commands.List.handle_ast({:llen, list_key}, ctx)

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert 0 = Ferricstore.Commands.Server.handle("DBSIZE", [], restarted_ctx)
      assert nil == Router.get(restarted_ctx, plain_key)
      assert 0 = Ferricstore.Commands.Hash.handle_ast({:hlen, hash_key}, restarted_ctx)
      assert 0 = Ferricstore.Commands.List.handle_ast({:llen, list_key}, restarted_ctx)
    after
      restore_backend(previous_backend)
    end
  end

  test "generic key mutations survive WARaft restart", %{root: root, ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert :ok = Router.put(ctx, "router:generic:source", "source-value", 0)
      assert :ok = Router.put(ctx, "router:generic:rename-src", "rename-value", 0)
      assert :ok = Router.put(ctx, "router:generic:unlink", "delete-me", 0)

      assert 1 =
               Ferricstore.Commands.Generic.handle_ast(
                 {:copy, "router:generic:source", "router:generic:copy", false},
                 ctx
               )

      assert :ok =
               Ferricstore.Commands.Generic.handle_ast(
                 {:rename, "router:generic:rename-src", "router:generic:renamed"},
                 ctx
               )

      assert 0 =
               Ferricstore.Commands.Generic.handle_ast(
                 {:renamenx, "router:generic:renamed", "router:generic:copy"},
                 ctx
               )

      assert 1 =
               Ferricstore.Commands.Generic.handle_ast({:unlink, ["router:generic:unlink"]}, ctx)

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert "source-value" == Router.get(restarted_ctx, "router:generic:source")
      assert "source-value" == Router.get(restarted_ctx, "router:generic:copy")
      assert nil == Router.get(restarted_ctx, "router:generic:rename-src")
      assert "rename-value" == Router.get(restarted_ctx, "router:generic:renamed")
      assert nil == Router.get(restarted_ctx, "router:generic:unlink")
    after
      restore_backend(previous_backend)
    end
  end

  test "cross-shard generic key mutations survive WARaft restart", %{root: root} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    ctx = build_ctx(Path.join(root, "generic-cross-shard"), shard_count: 2)

    source = key_for_shard(ctx, 0, "router:generic-cross:source")
    copy_dest = key_for_shard(ctx, 1, "router:generic-cross:copy")
    rename_src = key_for_shard(ctx, 0, "router:generic-cross:rename-src")
    renamed = key_for_shard(ctx, 1, "router:generic-cross:renamed")

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert :ok = Router.put(ctx, source, "source-value", 0)
      assert :ok = Router.put(ctx, rename_src, "rename-value", 0)

      assert 1 =
               Ferricstore.Commands.Generic.handle_ast(
                 {:copy, source, copy_dest, false},
                 ctx
               )

      assert :ok =
               Ferricstore.Commands.Generic.handle_ast(
                 {:rename, rename_src, renamed},
                 ctx
               )

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(Path.join(root, "generic-cross-shard"), shard_count: 2)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert "source-value" == Router.get(restarted_ctx, source)
      assert "source-value" == Router.get(restarted_ctx, copy_dest)
      assert nil == Router.get(restarted_ctx, rename_src)
      assert "rename-value" == Router.get(restarted_ctx, renamed)
    after
      WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)
      restore_backend(previous_backend)
    end
  end

  test "cross-shard generic key mutations preserve blob-backed values after WARaft restart", %{
    root: root
  } do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    ctx = build_ctx(Path.join(root, "generic-cross-shard-blob"), shard_count: 2)

    copy_source = key_for_shard(ctx, 0, "router:generic-cross-blob:copy-source")
    copy_dest = key_for_shard(ctx, 1, "router:generic-cross-blob:copy-dest")
    rename_source = key_for_shard(ctx, 0, "router:generic-cross-blob:rename-source")
    rename_dest = key_for_shard(ctx, 1, "router:generic-cross-blob:rename-dest")

    copy_payload = :binary.copy("copy-blob-payload", 30_000)
    rename_payload = :binary.copy("rename-blob-payload", 30_000)

    assert byte_size(copy_payload) > ctx.blob_side_channel_threshold_bytes
    assert byte_size(rename_payload) > ctx.blob_side_channel_threshold_bytes

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert :ok = Router.put(ctx, copy_source, copy_payload, 0)
      assert :ok = Router.put(ctx, rename_source, rename_payload, 0)

      assert 1 =
               Ferricstore.Commands.Generic.handle_ast(
                 {:copy, copy_source, copy_dest, false},
                 ctx
               )

      assert :ok =
               Ferricstore.Commands.Generic.handle_ast({:rename, rename_source, rename_dest}, ctx)

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(Path.join(root, "generic-cross-shard-blob"), shard_count: 2)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert copy_payload == Router.get(restarted_ctx, copy_source)
      assert copy_payload == Router.get(restarted_ctx, copy_dest)
      assert nil == Router.get(restarted_ctx, rename_source)
      assert rename_payload == Router.get(restarted_ctx, rename_dest)

      assert [{_, nil, 0, _lfu, _fid, _off, copy_value_size}] =
               :ets.lookup(elem(restarted_ctx.keydir_refs, 1), copy_dest)

      assert [{_, nil, 0, _lfu, _fid, _off, rename_value_size}] =
               :ets.lookup(elem(restarted_ctx.keydir_refs, 1), rename_dest)

      assert BlobRef.encoded_size?(copy_value_size)
      assert BlobRef.encoded_size?(rename_value_size)
    after
      WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)
      restore_backend(previous_backend)
    end
  end

  test "native CAS lock and ratelimit mutations survive WARaft restart", %{root: root, ctx: ctx} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    suffix = System.unique_integer([:positive])
    cas_key = "router:native:cas:#{suffix}"
    lock_key = "router:native:lock:#{suffix}"
    ratelimit_key = "router:native:rl:#{suffix}"

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert :ok = Router.put(ctx, cas_key, "old", 0)

      assert 1 =
               Ferricstore.Commands.Native.handle_ast({:cas, cas_key, "old", "new", 60_000}, ctx)

      assert :ok =
               Ferricstore.Commands.Native.handle_ast({:lock, lock_key, "owner-a", 60_000}, ctx)

      assert 1 =
               Ferricstore.Commands.Native.handle_ast(
                 {:extend, lock_key, "owner-a", 120_000},
                 ctx
               )

      assert ["allowed", 2, 1, _] =
               Ferricstore.Commands.Native.handle_ast(
                 {:ratelimit_add, ratelimit_key, 60_000, 3, 2},
                 ctx
               )

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert "new" == Router.get(restarted_ctx, cas_key)
      assert Ferricstore.Commands.Expiry.handle_ast({:pttl, cas_key}, restarted_ctx) > 0

      assert {:error, msg} =
               Ferricstore.Commands.Native.handle_ast(
                 {:unlock, lock_key, "wrong-owner"},
                 restarted_ctx
               )

      assert msg =~ "DISTLOCK"

      assert 1 =
               Ferricstore.Commands.Native.handle_ast(
                 {:unlock, lock_key, "owner-a"},
                 restarted_ctx
               )

      assert nil == Router.get(restarted_ctx, lock_key)

      assert ["denied", 2, 1, _] =
               Ferricstore.Commands.Native.handle_ast(
                 {:ratelimit_add, ratelimit_key, 60_000, 3, 2},
                 restarted_ctx
               )
    after
      restore_backend(previous_backend)
    end
  end

  test "extended string RMW commands survive WARaft restart", %{root: root} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    ctx = build_ctx(Path.join(root, "strings-rmw"), shard_count: 2)

    getset_key = key_for_shard(ctx, 0, "router:strings-rmw:getset")
    getdel_key = key_for_shard(ctx, 0, "router:strings-rmw:getdel")
    getex_key = key_for_shard(ctx, 0, "router:strings-rmw:getex")
    setrange_key = key_for_shard(ctx, 0, "router:strings-rmw:setrange")
    msetnx_a = key_for_shard(ctx, 0, "router:strings-rmw:msetnx-a")
    msetnx_b = key_for_shard(ctx, 1, "router:strings-rmw:msetnx-b")

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert :ok = Ferricstore.Commands.Strings.handle_ast({:set, getset_key, "old"}, ctx)
      assert "old" = Ferricstore.Commands.Strings.handle_ast({:getset, getset_key, "new"}, ctx)

      assert :ok = Ferricstore.Commands.Strings.handle_ast({:set, getdel_key, "delete-me"}, ctx)
      assert "delete-me" = Ferricstore.Commands.Strings.handle_ast({:getdel, getdel_key}, ctx)

      assert :ok = Ferricstore.Commands.Strings.handle_ast({:set, getex_key, "ttl-me"}, ctx)

      assert "ttl-me" =
               Ferricstore.Commands.Strings.handle_ast({:getex, getex_key, {:px, 60_000}}, ctx)

      assert :ok =
               Ferricstore.Commands.Strings.handle_ast({:set, setrange_key, "Hello World"}, ctx)

      assert 11 =
               Ferricstore.Commands.Strings.handle_ast(
                 {:setrange, setrange_key, 6, "Redis"},
                 ctx
               )

      assert 1 =
               Ferricstore.Commands.Strings.handle_ast(
                 {:msetnx, [msetnx_a, "v0", msetnx_b, "v1"]},
                 ctx
               )

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(Path.join(root, "strings-rmw"), shard_count: 2)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert "new" == Router.get(restarted_ctx, getset_key)
      assert nil == Router.get(restarted_ctx, getdel_key)
      assert "ttl-me" == Router.get(restarted_ctx, getex_key)
      assert Ferricstore.Commands.Expiry.handle_ast({:pttl, getex_key}, restarted_ctx) > 0
      assert "Hello Redis" == Router.get(restarted_ctx, setrange_key)
      assert "v0" == Router.get(restarted_ctx, msetnx_a)
      assert "v1" == Router.get(restarted_ctx, msetnx_b)
    after
      WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)
      restore_backend(previous_backend)
    end
  end

  test "numeric append and expiring string commands survive WARaft restart", %{root: root} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    ctx = build_ctx(Path.join(root, "strings-numeric"), shard_count: 2)

    int_key = key_for_shard(ctx, 0, "router:strings-numeric:int")
    float_key = key_for_shard(ctx, 0, "router:strings-numeric:float")
    append_key = key_for_shard(ctx, 1, "router:strings-numeric:append")
    setex_key = key_for_shard(ctx, 1, "router:strings-numeric:setex")
    psetex_key = key_for_shard(ctx, 1, "router:strings-numeric:psetex")

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)

      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert {:ok, 3} = Ferricstore.Commands.Strings.handle_ast({:incrby, int_key, 3}, ctx)

      float_result =
        Ferricstore.Commands.Strings.handle_ast({:incrbyfloat, float_key, 1.5}, ctx)

      {float_score, ""} = Float.parse(float_result)
      assert_in_delta 1.5, float_score, 0.001

      assert 5 = Ferricstore.Commands.Strings.handle_ast({:append, append_key, "hello"}, ctx)
      assert 11 = Ferricstore.Commands.Strings.handle_ast({:append, append_key, " world"}, ctx)

      assert :ok =
               Ferricstore.Commands.Strings.handle_ast({:setex, setex_key, 60, "seconds"}, ctx)

      assert :ok =
               Ferricstore.Commands.Strings.handle_ast(
                 {:psetex, psetex_key, 60_000, "millis"},
                 ctx
               )

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(Path.join(root, "strings-numeric"), shard_count: 2)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert "3" == Router.get(restarted_ctx, int_key)
      assert "1.5" == Router.get(restarted_ctx, float_key)
      assert "hello world" == Router.get(restarted_ctx, append_key)

      assert "hello" ==
               Ferricstore.Commands.Strings.handle_ast(
                 {:getrange, append_key, 0, 4},
                 restarted_ctx
               )

      assert "seconds" == Router.get(restarted_ctx, setex_key)
      assert "millis" == Router.get(restarted_ctx, psetex_key)
      assert Ferricstore.Commands.Expiry.handle_ast({:pttl, setex_key}, restarted_ctx) > 0
      assert Ferricstore.Commands.Expiry.handle_ast({:pttl, psetex_key}, restarted_ctx) > 0
    after
      WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)
      restore_backend(previous_backend)
    end
  end

  test "Router WARaft backend keeps shard partitions isolated", %{root: root} do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    ctx = build_ctx(Path.join(root, "multi"), shard_count: 4)

    try do
      Application.put_env(:ferricstore, :raft_backend, :waraft)
      assert :ok = WARaftBackend.start(ctx, log_module: :wa_raft_log_ets)

      entries =
        for shard_idx <- 0..3 do
          key = key_for_shard(ctx, shard_idx)
          {key, "value:#{shard_idx}"}
        end

      assert [:ok, :ok, :ok, :ok] = Router.batch_quorum_put(ctx, entries)

      for {key, value} <- entries do
        assert value == Router.get(ctx, key)
      end

      for shard_idx <- 0..3 do
        assert {:ok, {:raft_log_pos, index, _term}} = WARaftBackend.storage_position(shard_idx)
        assert index >= 2
      end
    after
      restore_backend(previous_backend)
      FerricStore.Instance.cleanup(ctx.name)
    end
  end

  @tag :cluster
  test "three peer backend nodes commit through the real FerricStore state machine" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader =
      start_waraft_backend_peer_cluster!(nodes, unique,
        election_timeout_ms: 200,
        election_timeout_ms_max: 300
      )

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster:k", "v", 0}
             ])

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [0, "backend-cluster:k"]) == "v"
             end)
           end)
  end

  @tag :cluster
  test "backend write through a follower redirects to the WARaft leader" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader = start_waraft_backend_peer_cluster!(nodes, unique)
    follower = Enum.find(names, &(&1 != leader))

    assert :ok =
             :rpc.call(follower, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster:follower-write", "v", 0}
             ])

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster:follower-write"
               ]) == "v"
             end)
           end)
  end

  @tag :cluster
  test "backend follower redirect submits read-modify-write commands once" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader = start_waraft_backend_peer_cluster!(nodes, unique)
    follower = Enum.find(names, &(&1 != leader))

    assert {:ok, 1} =
             :rpc.call(follower, WARaftBackend, :write, [
               0,
               {:incr, "backend-cluster:follower-incr-once", 1}
             ])

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster:follower-incr-once"
               ]) == "1"
             end)
           end)
  end

  @tag :cluster
  test "three peer backend cluster replicates multiple FerricStore shards" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)
    instance_name = waraft_backend_peer_instance_name(unique)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader = start_waraft_backend_peer_cluster!(nodes, unique, shard_count: 2)
    shard1_leader = wait_for_waraft_backend_leader(names, 1)
    follower = Enum.find(names, &(&1 != shard1_leader))
    leader_ctx = :rpc.call(leader, FerricStore.Instance, :get, [instance_name])

    key0 = key_for_shard(leader_ctx, 0, "backend-cluster:multi")
    key1 = key_for_shard(leader_ctx, 1, "backend-cluster:multi")

    assert :ok = :rpc.call(leader, WARaftBackend, :write, [0, {:put, key0, "v0", 0}])
    assert :ok = :rpc.call(follower, WARaftBackend, :write, [1, {:put, key1, "v1", 0}])

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [0, key0]) == "v0" and
                 :rpc.call(node, WARaftBackend, :local_get, [1, key1]) == "v1"
             end)
           end)
  end

  @tag :cluster
  test "three peer backend cluster restarts after an acknowledged write" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader = start_waraft_backend_peer_cluster!(nodes, unique)

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-restart:k", "v1", 0}
             ])

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [0, "backend-cluster-restart:k"]) ==
                 "v1"
             end)
           end)

    stop_waraft_backend_peer_cluster!(nodes, unique)
    restarted_leader = start_waraft_backend_peer_cluster!(nodes, unique)

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [0, "backend-cluster-restart:k"]) ==
                 "v1"
             end)
           end)

    assert :ok =
             :rpc.call(restarted_leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-restart:k2", "v2", 0}
             ])

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [0, "backend-cluster-restart:k2"]) ==
                 "v2"
             end)
           end)
  end

  @tag :cluster
  @tag :shard_kill
  test "single peer no-sync backend replays acknowledged write after OS process kill" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    [node] = nodes = start_waraft_backend_peers(unique, 1)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    :rpc.call(node.name, Application, :put_env, [
      :ferricstore,
      :waraft_storage_apply_mode,
      :replay_safe_nosync
    ])

    :rpc.call(node.name, Application, :put_env, [
      :ferricstore,
      :waraft_bitcask_payload_fsync_every,
      :never
    ])

    leader =
      start_waraft_backend_peer_cluster!(nodes, unique,
        election_timeout_ms: 200,
        election_timeout_ms_max: 300
      )

    assert leader == node.name

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-nosync-kill:k", "v", 0}
             ])

    assert "v" == :rpc.call(leader, WARaftBackend, :local_get, [0, "backend-nosync-kill:k"])

    kill_peer_os_process!(node)

    restarted = restart_waraft_backend_peer!(node)

    on_exit(fn ->
      try do
        :peer.stop(restarted.peer)
      catch
        _, _ -> :ok
      end
    end)

    start_peer_runtime_apps!(restarted.name)

    :rpc.call(restarted.name, Application, :put_env, [
      :ferricstore,
      :waraft_storage_apply_mode,
      :replay_safe_nosync
    ])

    :rpc.call(restarted.name, Application, :put_env, [
      :ferricstore,
      :waraft_bitcask_payload_fsync_every,
      :never
    ])

    restarted_leader =
      start_waraft_backend_peer_cluster!([restarted], unique,
        election_timeout_ms: 200,
        election_timeout_ms_max: 300
      )

    assert restarted_leader == restarted.name

    assert_eventually(
      fn ->
        :rpc.call(restarted.name, WARaftBackend, :local_get, [0, "backend-nosync-kill:k"])
      end,
      "v"
    )
  end

  @tag :cluster
  @tag :shard_kill
  test "three peer backend cluster preserves acked writes when leader server is killed during load" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader = start_waraft_backend_peer_cluster!(nodes, unique)
    parent = self()

    writer =
      Task.async(fn ->
        for i <- 1..80 do
          key = "backend-cluster-kill:#{i}"
          value = "v#{i}"
          result = :rpc.call(leader, WARaftBackend, :write, [0, {:put, key, value, 0}])
          send(parent, {:waraft_cluster_kill_result, key, value, result})
        end
      end)

    acked_before_kill = wait_for_cluster_kill_acks(5, [])
    kill_waraft_server!(leader, 0)

    _ = Task.yield(writer, 10_000) || Task.shutdown(writer, :brutal_kill)
    acked = drain_cluster_kill_results(acked_before_kill)

    assert length(acked) >= 5

    recovered_leader = wait_for_waraft_backend_leader(names, 0, 200)

    assert :ok =
             :rpc.call(recovered_leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-kill:after", "after", 0}
             ])

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-kill:after"
               ]) == "after"
             end)
           end)

    for {key, value} <- acked do
      assert eventually(fn ->
               Enum.all?(names, fn node ->
                 :rpc.call(node, WARaftBackend, :local_get, [0, key]) == value
               end)
             end)
    end
  end

  @tag :cluster
  @tag :shard_kill
  test "three peer backend cluster catches up follower node crash during active write load" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader =
      start_waraft_backend_peer_cluster!(nodes, unique,
        election_timeout_ms: 200,
        election_timeout_ms_max: 300
      )

    crashed = Enum.find(nodes, &(&1.name != leader))
    live_names = names -- [crashed.name]
    parent = self()

    writer =
      Task.async(fn ->
        for i <- 1..80 do
          key = "backend-cluster-node-crash:#{i}"
          value = "v#{i}"
          result = :rpc.call(leader, WARaftBackend, :write, [0, {:put, key, value, 0}])
          send(parent, {:waraft_cluster_kill_result, key, value, result})
        end
      end)

    acked_before_crash = wait_for_cluster_kill_acks(5, [])
    :peer.stop(crashed.peer)

    _ = Task.yield(writer, 10_000) || Task.shutdown(writer, :brutal_kill)
    acked = drain_cluster_kill_results(acked_before_crash)

    assert length(acked) >= 5

    assert eventually(fn ->
             Enum.all?(acked, fn {key, value} ->
               Enum.all?(live_names, fn node ->
                 :rpc.call(node, WARaftBackend, :local_get, [0, key]) == value
               end)
             end)
           end)

    restarted = restart_waraft_backend_peer!(crashed)

    on_exit(fn ->
      try do
        :peer.stop(restarted.peer)
      catch
        _, _ -> :ok
      end
    end)

    start_peer_runtime_apps!(restarted.name)

    for live <- live_names do
      :rpc.call(restarted.name, Node, :connect, [live])
      :rpc.call(live, Node, :connect, [restarted.name])
    end

    start_waraft_backend_peer!(restarted, unique,
      election_timeout_ms: 200,
      election_timeout_ms_max: 300
    )

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-node-crash:after", "after", 0}
             ])

    assert eventually(fn ->
             :rpc.call(restarted.name, WARaftBackend, :local_get, [
               0,
               "backend-cluster-node-crash:after"
             ]) == "after"
           end)

    for {key, value} <- acked do
      assert eventually(fn ->
               :rpc.call(restarted.name, WARaftBackend, :local_get, [0, key]) == value
             end)
    end
  end

  @tag :cluster
  @tag :shard_kill
  test "three peer backend cluster re-elects after leader node crash during active write load" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader =
      start_waraft_backend_peer_cluster!(nodes, unique,
        election_timeout_ms: 200,
        election_timeout_ms_max: 300
      )

    crashed = Enum.find(nodes, &(&1.name == leader))
    live_names = names -- [leader]
    parent = self()

    writer =
      Task.async(fn ->
        for i <- 1..80 do
          key = "backend-cluster-leader-node-crash:#{i}"
          value = "v#{i}"
          result = :rpc.call(leader, WARaftBackend, :write, [0, {:put, key, value, 0}])
          send(parent, {:waraft_cluster_kill_result, key, value, result})
        end
      end)

    acked_before_crash = wait_for_cluster_kill_acks(5, [])
    :peer.stop(crashed.peer)

    _ = Task.yield(writer, 10_000) || Task.shutdown(writer, :brutal_kill)
    acked = drain_cluster_kill_results(acked_before_crash)

    assert length(acked) >= 5

    recovered_leader = wait_for_waraft_backend_leader(live_names, 0, 200)

    assert :ok =
             :rpc.call(recovered_leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-leader-node-crash:after", "after", 0}
             ])

    assert eventually(fn ->
             Enum.all?(live_names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-leader-node-crash:after"
               ]) == "after"
             end)
           end)

    assert eventually(fn ->
             Enum.all?(acked, fn {key, value} ->
               Enum.all?(live_names, fn node ->
                 :rpc.call(node, WARaftBackend, :local_get, [0, key]) == value
               end)
             end)
           end)

    restarted = restart_waraft_backend_peer!(crashed)

    on_exit(fn ->
      try do
        :peer.stop(restarted.peer)
      catch
        _, _ -> :ok
      end
    end)

    start_peer_runtime_apps!(restarted.name)

    for live <- live_names do
      :rpc.call(restarted.name, Node, :connect, [live])
      :rpc.call(live, Node, :connect, [restarted.name])
    end

    start_waraft_backend_peer!(restarted, unique,
      election_timeout_ms: 200,
      election_timeout_ms_max: 300
    )

    assert :ok =
             :rpc.call(recovered_leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-leader-node-crash:after-restart", "after-restart", 0}
             ])

    assert eventually(fn ->
             :rpc.call(restarted.name, WARaftBackend, :local_get, [
               0,
               "backend-cluster-leader-node-crash:after-restart"
             ]) == "after-restart"
           end)

    for {key, value} <- acked do
      assert eventually(fn ->
               :rpc.call(restarted.name, WARaftBackend, :local_get, [0, key]) == value
             end)
    end
  end

  @tag :cluster
  @tag :shard_kill
  test "three peer backend cluster recovers after leader OS process kill during active write load" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader =
      start_waraft_backend_peer_cluster!(nodes, unique,
        election_timeout_ms: 200,
        election_timeout_ms_max: 300
      )

    killed = Enum.find(nodes, &(&1.name == leader))
    live_names = names -- [leader]
    parent = self()

    writer =
      Task.async(fn ->
        for i <- 1..80 do
          key = "backend-cluster-leader-kill9:#{i}"
          value = "v#{i}"
          result = :rpc.call(leader, WARaftBackend, :write, [0, {:put, key, value, 0}])
          send(parent, {:waraft_cluster_kill_result, key, value, result})
        end
      end)

    acked_before_kill = wait_for_cluster_kill_acks(5, [])
    kill_peer_os_process!(killed)

    _ = Task.yield(writer, 10_000) || Task.shutdown(writer, :brutal_kill)
    acked = drain_cluster_kill_results(acked_before_kill)

    assert length(acked) >= 5

    recovered_leader = wait_for_waraft_backend_leader(live_names, 0, 200)

    assert :ok =
             :rpc.call(recovered_leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-leader-kill9:after", "after", 0}
             ])

    assert eventually(fn ->
             Enum.all?(live_names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-leader-kill9:after"
               ]) == "after"
             end)
           end)

    assert eventually(fn ->
             Enum.all?(acked, fn {key, value} ->
               Enum.all?(live_names, fn node ->
                 :rpc.call(node, WARaftBackend, :local_get, [0, key]) == value
               end)
             end)
           end)

    restarted = restart_waraft_backend_peer!(killed)

    on_exit(fn ->
      try do
        :peer.stop(restarted.peer)
      catch
        _, _ -> :ok
      end
    end)

    start_peer_runtime_apps!(restarted.name)

    for live <- live_names do
      :rpc.call(restarted.name, Node, :connect, [live])
      :rpc.call(live, Node, :connect, [restarted.name])
    end

    start_waraft_backend_peer!(restarted, unique,
      election_timeout_ms: 200,
      election_timeout_ms_max: 300
    )

    assert :ok =
             :rpc.call(recovered_leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-leader-kill9:after-restart", "after-restart", 0}
             ])

    assert eventually(fn ->
             :rpc.call(restarted.name, WARaftBackend, :local_get, [
               0,
               "backend-cluster-leader-kill9:after-restart"
             ]) == "after-restart"
           end)

    for {key, value} <- acked do
      assert eventually(fn ->
               :rpc.call(restarted.name, WARaftBackend, :local_get, [0, key]) == value
             end)
    end
  end

  @tag :cluster
  @tag :shard_kill
  test "three peer backend cluster catches up follower OS process kill during active write load" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader =
      start_waraft_backend_peer_cluster!(nodes, unique,
        election_timeout_ms: 200,
        election_timeout_ms_max: 300
      )

    killed = Enum.find(nodes, &(&1.name != leader))
    live_names = names -- [killed.name]
    parent = self()

    writer =
      Task.async(fn ->
        for i <- 1..80 do
          key = "backend-cluster-follower-kill9:#{i}"
          value = "v#{i}"
          result = :rpc.call(leader, WARaftBackend, :write, [0, {:put, key, value, 0}])
          send(parent, {:waraft_cluster_kill_result, key, value, result})
        end
      end)

    acked_before_kill = wait_for_cluster_kill_acks(5, [])
    kill_peer_os_process!(killed)

    _ = Task.yield(writer, 10_000) || Task.shutdown(writer, :brutal_kill)
    acked = drain_cluster_kill_results(acked_before_kill)

    assert length(acked) >= 5

    assert eventually(fn ->
             Enum.all?(acked, fn {key, value} ->
               Enum.all?(live_names, fn node ->
                 :rpc.call(node, WARaftBackend, :local_get, [0, key]) == value
               end)
             end)
           end)

    restarted = restart_waraft_backend_peer!(killed)

    on_exit(fn ->
      try do
        :peer.stop(restarted.peer)
      catch
        _, _ -> :ok
      end
    end)

    start_peer_runtime_apps!(restarted.name)

    for live <- live_names do
      :rpc.call(restarted.name, Node, :connect, [live])
      :rpc.call(live, Node, :connect, [restarted.name])
    end

    start_waraft_backend_peer!(restarted, unique,
      election_timeout_ms: 200,
      election_timeout_ms_max: 300
    )

    live_leader = wait_for_waraft_backend_leader(live_names, 0, 200)

    assert :ok =
             :rpc.call(live_leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-follower-kill9:after-restart", "after-restart", 0}
             ])

    assert eventually(fn ->
             :rpc.call(restarted.name, WARaftBackend, :local_get, [
               0,
               "backend-cluster-follower-kill9:after-restart"
             ]) == "after-restart"
           end)

    for {key, value} <- acked do
      assert eventually(fn ->
               :rpc.call(restarted.name, WARaftBackend, :local_get, [0, key]) == value
             end)
    end
  end

  @tag :cluster
  @tag :shard_kill
  test "three peer backend cluster rejects writes without quorum after two OS kills and recovers" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader =
      start_waraft_backend_peer_cluster!(nodes, unique,
        election_timeout_ms: 200,
        election_timeout_ms_max: 300
      )

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-two-kill9:before", "before", 0}
             ])

    [first_killed, second_killed] =
      nodes
      |> Enum.reject(&(&1.name == leader))
      |> Enum.take(2)

    kill_peer_os_process!(first_killed)
    kill_peer_os_process!(second_killed)

    assert {:error, :no_quorum} =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-two-kill9:no-quorum", "no-quorum", 0}
             ])

    restarted_first = restart_waraft_backend_peer!(first_killed)

    on_exit(fn ->
      try do
        :peer.stop(restarted_first.peer)
      catch
        _, _ -> :ok
      end
    end)

    start_peer_runtime_apps!(restarted_first.name)
    :rpc.call(restarted_first.name, Node, :connect, [leader])
    :rpc.call(leader, Node, :connect, [restarted_first.name])

    start_waraft_backend_peer!(restarted_first, unique,
      election_timeout_ms: 200,
      election_timeout_ms_max: 300
    )

    quorum_names = [leader, restarted_first.name]
    quorum_leader = wait_for_waraft_backend_leader(quorum_names, 0, 200)

    assert :ok =
             :rpc.call(quorum_leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-two-kill9:after-quorum", "after-quorum", 0}
             ])

    restarted_second = restart_waraft_backend_peer!(second_killed)

    on_exit(fn ->
      try do
        :peer.stop(restarted_second.peer)
      catch
        _, _ -> :ok
      end
    end)

    start_peer_runtime_apps!(restarted_second.name)

    for live <- quorum_names do
      :rpc.call(restarted_second.name, Node, :connect, [live])
      :rpc.call(live, Node, :connect, [restarted_second.name])
    end

    start_waraft_backend_peer!(restarted_second, unique,
      election_timeout_ms: 200,
      election_timeout_ms_max: 300
    )

    assert eventually(fn ->
             Enum.all?([leader, restarted_first.name, restarted_second.name], fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-two-kill9:before"
               ]) == "before" and
                 :rpc.call(node, WARaftBackend, :local_get, [
                   0,
                   "backend-cluster-two-kill9:after-quorum"
                 ]) == "after-quorum" and
                 :rpc.call(node, WARaftBackend, :local_get, [
                   0,
                   "backend-cluster-two-kill9:no-quorum"
                 ]) == nil
             end)
           end)
  end

  @tag :cluster
  @tag :shard_kill
  test "three peer backend cluster catches up isolated follower after network partition heal" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader =
      start_waraft_backend_peer_cluster!(nodes, unique,
        election_timeout_ms: 200,
        election_timeout_ms_max: 300
      )

    isolated = Enum.find(nodes, &(&1.name != leader))
    majority_names = names -- [isolated.name]
    real_cookie = partition_waraft_peer!(isolated, nodes)

    assert {:error, _reason} =
             :rpc.call(isolated.name, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-partition:minority", "minority", 0}
             ])

    majority_leader = wait_for_waraft_backend_leader(majority_names, 0, 200)

    for i <- 1..20 do
      assert :ok =
               :rpc.call(majority_leader, WARaftBackend, :write, [
                 0,
                 {:put, "backend-cluster-partition:#{i}", "v#{i}", 0}
               ])
    end

    assert eventually(fn ->
             Enum.all?(majority_names, fn node ->
               Enum.all?(1..20, fn i ->
                 :rpc.call(node, WARaftBackend, :local_get, [
                   0,
                   "backend-cluster-partition:#{i}"
                 ]) == "v#{i}"
               end)
             end)
           end)

    heal_waraft_peer_partition!(isolated, nodes, real_cookie)

    assert eventually(fn ->
             Enum.all?(1..20, fn i ->
               :rpc.call(isolated.name, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-partition:#{i}"
               ]) == "v#{i}"
             end)
           end)
  end

  @tag :cluster
  @tag :shard_kill
  test "three peer backend cluster rejects isolated leader writes and catches up after heal" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader =
      start_waraft_backend_peer_cluster!(nodes, unique,
        election_timeout_ms: 200,
        election_timeout_ms_max: 300
      )

    isolated = Enum.find(nodes, &(&1.name == leader))
    majority_names = names -- [leader]
    real_cookie = partition_waraft_peer!(isolated, nodes)

    assert {:error, _reason} =
             :rpc.call(isolated.name, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-leader-partition:minority", "minority", 0}
             ])

    majority_leader = wait_for_waraft_backend_leader(majority_names, 0, 200)

    for i <- 1..20 do
      assert :ok =
               :rpc.call(majority_leader, WARaftBackend, :write, [
                 0,
                 {:put, "backend-cluster-leader-partition:#{i}", "v#{i}", 0}
               ])
    end

    assert eventually(fn ->
             Enum.all?(majority_names, fn node ->
               Enum.all?(1..20, fn i ->
                 :rpc.call(node, WARaftBackend, :local_get, [
                   0,
                   "backend-cluster-leader-partition:#{i}"
                 ]) == "v#{i}"
               end)
             end)
           end)

    heal_waraft_peer_partition!(isolated, nodes, real_cookie)

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               Enum.all?(1..20, fn i ->
                 :rpc.call(node, WARaftBackend, :local_get, [
                   0,
                   "backend-cluster-leader-partition:#{i}"
                 ]) == "v#{i}"
               end)
             end)
           end)

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-leader-partition:minority"
               ]) == nil
             end)
           end)
  end

  @tag :cluster
  @tag :shard_kill
  test "three peer backend cluster survives repeated partition and heal cycles" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    _leader =
      start_waraft_backend_peer_cluster!(nodes, unique,
        election_timeout_ms: 200,
        election_timeout_ms_max: 300
      )

    expected =
      Enum.reduce(1..3, [], fn cycle, acc ->
        current_leader = wait_for_waraft_backend_leader(names, 0, 200)

        isolated =
          if rem(cycle, 2) == 1 do
            Enum.find(nodes, &(&1.name == current_leader))
          else
            Enum.find(nodes, &(&1.name != current_leader))
          end

        majority_names = names -- [isolated.name]
        real_cookie = partition_waraft_peer!(isolated, nodes)

        assert {:error, _reason} =
                 :rpc.call(isolated.name, WARaftBackend, :write, [
                   0,
                   {:put, "backend-cluster-flap:minority:#{cycle}", "minority", 0}
                 ])

        majority_leader = wait_for_waraft_backend_leader(majority_names, 0, 200)

        cycle_expected =
          for i <- 1..5 do
            key = "backend-cluster-flap:#{cycle}:#{i}"
            value = "v#{cycle}:#{i}"

            assert :ok =
                     :rpc.call(majority_leader, WARaftBackend, :write, [
                       0,
                       {:put, key, value, 0}
                     ])

            {key, value}
          end

        assert eventually(fn ->
                 Enum.all?(majority_names, fn node ->
                   Enum.all?(cycle_expected, fn {key, value} ->
                     :rpc.call(node, WARaftBackend, :local_get, [0, key]) == value
                   end)
                 end)
               end)

        heal_waraft_peer_partition!(isolated, nodes, real_cookie)

        assert eventually(fn ->
                 Enum.all?(names, fn node ->
                   Enum.all?(cycle_expected, fn {key, value} ->
                     :rpc.call(node, WARaftBackend, :local_get, [0, key]) == value
                   end)
                 end)
               end)

        assert eventually(fn ->
                 Enum.all?(names, fn node ->
                   :rpc.call(node, WARaftBackend, :local_get, [
                     0,
                     "backend-cluster-flap:minority:#{cycle}"
                   ]) == nil
                 end)
               end)

        cycle_expected ++ acc
      end)

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               Enum.all?(expected, fn {key, value} ->
                 :rpc.call(node, WARaftBackend, :local_get, [0, key]) == value
               end)
             end)
           end)
  end

  @tag :cluster
  @tag :shard_kill
  test "three peer backend cluster preserves acked writes across partition heal and follower OS kill" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader =
      start_waraft_backend_peer_cluster!(nodes, unique,
        election_timeout_ms: 200,
        election_timeout_ms_max: 300
      )

    isolated = Enum.find(nodes, &(&1.name != leader))
    majority_names = names -- [isolated.name]
    real_cookie = partition_waraft_peer!(isolated, nodes)
    majority_leader = wait_for_waraft_backend_leader(majority_names, 0, 200)
    parent = self()

    partition_writer =
      Task.async(fn ->
        for i <- 1..40 do
          key = "backend-cluster-mixed-chaos:partition:#{i}"
          value = "pv#{i}"
          result = :rpc.call(majority_leader, WARaftBackend, :write, [0, {:put, key, value, 0}])
          send(parent, {:waraft_cluster_kill_result, key, value, result})
        end
      end)

    partition_acked_before_heal = wait_for_cluster_kill_acks(5, [])
    heal_waraft_peer_partition!(isolated, nodes, real_cookie)

    _ = Task.yield(partition_writer, 10_000) || Task.shutdown(partition_writer, :brutal_kill)
    partition_acked = drain_cluster_kill_results(partition_acked_before_heal)

    assert length(partition_acked) >= 5

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               Enum.all?(partition_acked, fn {key, value} ->
                 :rpc.call(node, WARaftBackend, :local_get, [0, key]) == value
               end)
             end)
           end)

    live_leader = wait_for_waraft_backend_leader(names, 0, 200)
    killed = Enum.find(nodes, &(&1.name != live_leader))
    live_names = names -- [killed.name]

    kill_writer =
      Task.async(fn ->
        for i <- 1..40 do
          key = "backend-cluster-mixed-chaos:kill:#{i}"
          value = "kv#{i}"
          result = :rpc.call(live_leader, WARaftBackend, :write, [0, {:put, key, value, 0}])
          send(parent, {:waraft_cluster_kill_result, key, value, result})
        end
      end)

    kill_acked_before_os_kill = wait_for_cluster_kill_acks(5, [])
    kill_peer_os_process!(killed)

    _ = Task.yield(kill_writer, 10_000) || Task.shutdown(kill_writer, :brutal_kill)
    kill_acked = drain_cluster_kill_results(kill_acked_before_os_kill)

    assert length(kill_acked) >= 5

    assert eventually(fn ->
             Enum.all?(live_names, fn node ->
               Enum.all?(kill_acked, fn {key, value} ->
                 :rpc.call(node, WARaftBackend, :local_get, [0, key]) == value
               end)
             end)
           end)

    restarted = restart_waraft_backend_peer!(killed)

    on_exit(fn ->
      try do
        :peer.stop(restarted.peer)
      catch
        _, _ -> :ok
      end
    end)

    start_peer_runtime_apps!(restarted.name)

    for live <- live_names do
      :rpc.call(restarted.name, Node, :connect, [live])
      :rpc.call(live, Node, :connect, [restarted.name])
    end

    start_waraft_backend_peer!(restarted, unique,
      election_timeout_ms: 200,
      election_timeout_ms_max: 300
    )

    assert eventually(fn ->
             Enum.all?(partition_acked ++ kill_acked, fn {key, value} ->
               :rpc.call(restarted.name, WARaftBackend, :local_get, [0, key]) == value
             end)
           end)
  end

  @tag :cluster
  test "three peer backend cluster removes a member through the backend API" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader = start_waraft_backend_peer_cluster!(nodes, unique)
    removed = Enum.find(names, &(&1 != leader))
    kept = Enum.reject(names, &(&1 == removed))
    removed_peer = {:raft_server_ferricstore_waraft_backend_1, removed}

    assert {:ok, {:raft_log_pos, _, _}} =
             :rpc.call(leader, WARaftBackend, :adjust_membership, [0, :remove, removed])

    assert eventually(fn ->
             membership = :rpc.call(leader, WARaftBackend, :membership, [0])
             is_list(membership) and removed_peer not in membership
           end)

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-membership:k", "v", 0}
             ])

    assert eventually(fn ->
             Enum.all?(kept, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [0, "backend-cluster-membership:k"]) ==
                 "v"
             end)
           end)
  end

  @tag :cluster
  test "backend cluster adds a new member and catches up real Bitcask state" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 4)
    initial_nodes = Enum.take(nodes, 3)
    joining_node = List.last(nodes)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader = start_waraft_backend_peer_cluster!(initial_nodes, unique)

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-add:before", "v1", 0}
             ])

    assert eventually(fn ->
             Enum.all?(Enum.map(initial_nodes, & &1.name), fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [0, "backend-cluster-add:before"]) ==
                 "v1"
             end)
           end)

    start_waraft_backend_peer!(joining_node, unique)
    joining_peer = {:raft_server_ferricstore_waraft_backend_1, joining_node.name}

    assert {:ok, {:raft_log_pos, _, _}} =
             :rpc.call(leader, WARaftBackend, :add_member, [0, joining_node.name])

    assert eventually(fn ->
             membership = :rpc.call(leader, WARaftBackend, :membership, [0])
             is_list(membership) and joining_peer in membership
           end)

    assert eventually(fn ->
             :rpc.call(joining_node.name, WARaftBackend, :local_get, [
               0,
               "backend-cluster-add:before"
             ]) == "v1"
           end)

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-add:after", "v2", 0}
             ])

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [0, "backend-cluster-add:after"]) ==
                 "v2"
             end)
           end)
  end

  @tag :cluster
  test "Raft.Cluster add_member delegates to WARaft backend when selected" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 4)
    initial_nodes = Enum.take(nodes, 3)
    joining_node = List.last(nodes)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    for node <- names do
      :rpc.call(node, Application, :put_env, [:ferricstore, :raft_backend, :waraft])
    end

    leader = start_waraft_backend_peer_cluster!(initial_nodes, unique)

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-api-add:before", "v1", 0}
             ])

    start_waraft_backend_peer!(joining_node, unique)
    joining_peer = {:raft_server_ferricstore_waraft_backend_1, joining_node.name}

    assert :ok = :rpc.call(leader, RaftCluster, :add_member, [0, joining_node.name, :voter])

    assert eventually(fn ->
             membership = :rpc.call(leader, WARaftBackend, :membership, [0])
             is_list(membership) and joining_peer in membership
           end)

    assert eventually(fn ->
             :rpc.call(joining_node.name, WARaftBackend, :local_get, [
               0,
               "backend-cluster-api-add:before"
             ]) == "v1"
           end)
  end

  @tag :cluster
  test "Raft.Cluster add_member redirects from WARaft follower to leader" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 4)
    initial_nodes = Enum.take(nodes, 3)
    joining_node = List.last(nodes)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    for node <- names do
      :rpc.call(node, Application, :put_env, [:ferricstore, :raft_backend, :waraft])
    end

    leader = start_waraft_backend_peer_cluster!(initial_nodes, unique)
    follower = initial_nodes |> Enum.map(& &1.name) |> Enum.find(&(&1 != leader))

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-api-follower-add:before", "v1", 0}
             ])

    start_waraft_backend_peer!(joining_node, unique)
    joining_peer = {:raft_server_ferricstore_waraft_backend_1, joining_node.name}

    assert :ok = :rpc.call(follower, RaftCluster, :add_member, [0, joining_node.name, :voter])

    assert eventually(fn ->
             membership = :rpc.call(leader, WARaftBackend, :membership, [0])
             is_list(membership) and joining_peer in membership
           end)

    assert eventually(fn ->
             :rpc.call(joining_node.name, WARaftBackend, :local_get, [
               0,
               "backend-cluster-api-follower-add:before"
             ]) == "v1"
           end)
  end

  @tag :cluster
  test "Raft.Cluster promotable WARaft member catches up from snapshot without becoming voter" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 4)
    initial_nodes = Enum.take(nodes, 3)
    joining_node = List.last(nodes)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    for node <- names do
      :rpc.call(node, Application, :put_env, [:ferricstore, :raft_backend, :waraft])
    end

    leader = start_waraft_backend_peer_cluster!(initial_nodes, unique)
    follower = initial_nodes |> Enum.map(& &1.name) |> Enum.find(&(&1 != leader))

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-api-participant:before", "v1", 0}
             ])

    start_waraft_backend_peer!(joining_node, unique)
    joining_peer = {:raft_server_ferricstore_waraft_backend_1, joining_node.name}

    assert :ok =
             :rpc.call(follower, RaftCluster, :add_member, [0, joining_node.name, :promotable])

    assert eventually(fn ->
             membership = :rpc.call(leader, WARaftBackend, :membership, [0])
             is_list(membership) and joining_peer not in membership
           end)

    assert eventually(fn ->
             :rpc.call(joining_node.name, WARaftBackend, :local_get, [
               0,
               "backend-cluster-api-participant:before"
             ]) == "v1"
           end)

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-api-participant:after", "v2", 0}
             ])

    assert eventually(fn ->
             :rpc.call(joining_node.name, WARaftBackend, :local_get, [
               0,
               "backend-cluster-api-participant:after"
             ]) == "v2"
           end)
  end

  @tag :cluster
  test "Raft.Cluster demotes an existing WARaft voter from a follower caller" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    for node <- names do
      :rpc.call(node, Application, :put_env, [:ferricstore, :raft_backend, :waraft])
    end

    leader = start_waraft_backend_peer_cluster!(nodes, unique)
    target = Enum.find(names, &(&1 != leader))
    caller = Enum.find(names, &(&1 not in [leader, target]))
    target_peer = {:raft_server_ferricstore_waraft_backend_1, target}

    assert target_peer in :rpc.call(leader, WARaftBackend, :membership, [0])
    assert :ok = :rpc.call(caller, RaftCluster, :add_member, [0, target, :promotable])

    assert eventually(fn ->
             membership = :rpc.call(leader, WARaftBackend, :membership, [0])
             is_list(membership) and target_peer not in membership
           end)

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-api-demote:k", "v", 0}
             ])

    assert eventually(fn ->
             :rpc.call(target, WARaftBackend, :local_get, [
               0,
               "backend-cluster-api-demote:k"
             ]) == "v"
           end)
  end

  @tag :cluster
  test "Raft.Cluster remove_member redirects from WARaft follower to leader" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    for node <- names do
      :rpc.call(node, Application, :put_env, [:ferricstore, :raft_backend, :waraft])
    end

    leader = start_waraft_backend_peer_cluster!(nodes, unique)
    removed = Enum.find(names, &(&1 != leader))
    kept = Enum.reject(names, &(&1 == removed))
    caller = Enum.find(kept, &(&1 != leader))
    removed_peer = {:raft_server_ferricstore_waraft_backend_1, removed}

    assert :ok = :rpc.call(caller, RaftCluster, :remove_member, [0, removed])

    assert eventually(fn ->
             membership = :rpc.call(leader, WARaftBackend, :membership, [0])
             is_list(membership) and removed_peer not in membership
           end)

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-api-remove:k", "v", 0}
             ])

    assert eventually(fn ->
             Enum.all?(kept, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-api-remove:k"
               ]) == "v"
             end)
           end)
  end

  @tag :cluster
  test "Raft.Cluster members reports WARaft leader through shared API" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    for node <- names do
      :rpc.call(node, Application, :put_env, [:ferricstore, :raft_backend, :waraft])
    end

    leader = start_waraft_backend_peer_cluster!(nodes, unique)
    leader_peer = {:raft_server_ferricstore_waraft_backend_1, leader}
    follower = Enum.find(names, &(&1 != leader))

    assert {:ok, members, ^leader_peer} = :rpc.call(leader, RaftCluster, :members, [0])
    assert leader_peer in members
    assert {:ok, follower_members, ^leader_peer} = :rpc.call(follower, RaftCluster, :members, [0])
    assert leader_peer in follower_members
  end

  @tag :cluster
  test "Raft.Cluster transfer_leadership delegates to WARaft handover when selected" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    for node <- names do
      :rpc.call(node, Application, :put_env, [:ferricstore, :raft_backend, :waraft])
    end

    leader = start_waraft_backend_peer_cluster!(nodes, unique)
    target = Enum.find(names, &(&1 != leader))
    caller = Enum.find(names, &(&1 not in [leader, target]))

    assert :ok = :rpc.call(caller, RaftCluster, :transfer_leadership, [0, target])

    assert eventually(fn ->
             case :rpc.call(target, WARaftBackend, :status, [0]) do
               status when is_list(status) -> Keyword.get(status, :state) == :leader
               _other -> false
             end
           end)

    assert :ok =
             :rpc.call(target, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-api-failover:k", "v", 0}
             ])

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-api-failover:k"
               ]) == "v"
             end)
           end)
  end

  @tag :cluster
  test "backend cluster keeps added member and data after full restart" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 4)
    initial_nodes = Enum.take(nodes, 3)
    joining_node = List.last(nodes)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)
    initial_names = Enum.map(initial_nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader = start_waraft_backend_peer_cluster!(initial_nodes, unique)

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-add-restart:before", "v1", 0}
             ])

    assert eventually(fn ->
             Enum.all?(initial_names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-add-restart:before"
               ]) == "v1"
             end)
           end)

    start_waraft_backend_peer!(joining_node, unique)
    joining_peer = {:raft_server_ferricstore_waraft_backend_1, joining_node.name}

    assert {:ok, {:raft_log_pos, _, _}} =
             :rpc.call(leader, WARaftBackend, :add_member, [0, joining_node.name])

    assert eventually(fn ->
             membership = :rpc.call(leader, WARaftBackend, :membership, [0])
             is_list(membership) and joining_peer in membership
           end)

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-add-restart:after", "v2", 0}
             ])

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-add-restart:after"
               ]) == "v2"
             end)
           end)

    stop_waraft_backend_peer_cluster!(nodes, unique)
    restarted_leader = start_waraft_backend_peer_cluster!(nodes, unique)

    assert eventually(fn ->
             membership = :rpc.call(restarted_leader, WARaftBackend, :membership, [0])
             is_list(membership) and joining_peer in membership
           end)

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-add-restart:before"
               ]) == "v1" and
                 :rpc.call(node, WARaftBackend, :local_get, [
                   0,
                   "backend-cluster-add-restart:after"
                 ]) == "v2"
             end)
           end)

    assert :ok =
             :rpc.call(restarted_leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-add-restart:after-restart", "v3", 0}
             ])

    assert eventually(fn ->
             Enum.all?(names, fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-add-restart:after-restart"
               ]) == "v3"
             end)
           end)
  end

  @tag :cluster
  test "backend add_member catches up large blob-backed values" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 4)
    initial_nodes = Enum.take(nodes, 3)
    joining_node = List.last(nodes)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader = start_waraft_backend_peer_cluster!(initial_nodes, unique)
    large_value = :binary.copy("blob-value", 40_000)

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-add-blob:large", large_value, 0}
             ])

    assert eventually(fn ->
             Enum.all?(Enum.map(initial_nodes, & &1.name), fn node ->
               :rpc.call(node, WARaftBackend, :local_get, [
                 0,
                 "backend-cluster-add-blob:large"
               ]) == large_value
             end)
           end)

    start_waraft_backend_peer!(joining_node, unique)

    assert {:ok, {:raft_log_pos, _, _}} =
             :rpc.call(leader, WARaftBackend, :add_member, [0, joining_node.name])

    assert eventually(fn ->
             :rpc.call(joining_node.name, WARaftBackend, :local_get, [
               0,
               "backend-cluster-add-blob:large"
             ]) == large_value
           end)
  end

  @tag :cluster
  test "one-node cluster disables blob side-channel while a participant is staged" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 2)
    [leader_node, joining_node] = nodes

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    for left <- Enum.map(nodes, & &1.name),
        right <- Enum.map(nodes, & &1.name),
        left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader = start_waraft_backend_peer_cluster!([leader_node], unique)
    start_waraft_backend_peer!(joining_node, unique)

    joining_peer = {:raft_server_ferricstore_waraft_backend_1, joining_node.name}

    assert {:ok, {:raft_log_pos, _, _}} =
             :rpc.call(leader, WARaftBackend, :add_participant, [0, joining_node.name])

    assert eventually(fn ->
             case :rpc.call(leader, WARaftBackend, :status, [0]) do
               status when is_list(status) ->
                 config = Keyword.get(status, :config, %{})
                 participants = Map.get(config, :participants, Map.get(config, :membership, []))
                 membership = Map.get(config, :membership, [])

                 joining_peer in participants and joining_peer not in membership

               _other ->
                 false
             end
           end)

    large_value = :binary.copy("participant-window-blob", 30_000)

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-participant-window:large", large_value, 0}
             ])

    assert eventually(fn ->
             :rpc.call(joining_node.name, WARaftBackend, :local_get, [
               0,
               "backend-cluster-participant-window:large"
             ]) == large_value
           end)
  end

  @tag :cluster
  test "backend add_member retries from staged participant after failed transfer" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft backend cluster test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_backend_peers(unique, 4)
    initial_nodes = Enum.take(nodes, 3)
    joining_node = List.last(nodes)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :rpc.call(node.name, WARaftBackend, :stop, [])
        catch
          _, _ -> :ok
        end

        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    leader = start_waraft_backend_peer_cluster!(initial_nodes, unique)
    joining_peer = {:raft_server_ferricstore_waraft_backend_1, joining_node.name}

    assert :ok =
             :rpc.call(leader, WARaftBackend, :write, [
               0,
               {:put, "backend-cluster-add-retry:before", "v1", 0}
             ])

    assert {:error, _reason} =
             :rpc.call(leader, WARaftBackend, :add_member, [
               0,
               joining_node.name,
               [timeout_ms: 1_000]
             ])

    assert eventually(fn ->
             membership = :rpc.call(leader, WARaftBackend, :membership, [0])
             is_list(membership) and joining_peer not in membership
           end)

    start_waraft_backend_peer!(joining_node, unique)

    assert {:ok, {:raft_log_pos, _, _}} =
             :rpc.call(leader, WARaftBackend, :add_member, [0, joining_node.name])

    assert eventually(fn ->
             membership = :rpc.call(leader, WARaftBackend, :membership, [0])
             is_list(membership) and joining_peer in membership
           end)

    assert eventually(fn ->
             :rpc.call(joining_node.name, WARaftBackend, :local_get, [
               0,
               "backend-cluster-add-retry:before"
             ]) == "v1"
           end)
  end

  defp build_ctx(root, opts \\ []) do
    shard_count = Keyword.get(opts, :shard_count, 1)

    FerricStore.Instance.build(
      :"waraft_backend_test_#{System.unique_integer([:positive])}",
      instance_opts(root, shard_count: shard_count)
    )
  end

  defp instance_opts(root, opts) do
    [
      data_dir: root,
      shard_count: Keyword.get(opts, :shard_count, 1),
      max_memory_bytes: 256 * 1024 * 1024,
      keydir_max_ram: 64 * 1024 * 1024,
      hot_cache_max_value_size: 65_536,
      blob_side_channel_threshold_bytes: 256 * 1024,
      max_active_file_size: 64 * 1024 * 1024,
      read_sample_rate: 100,
      lfu_decay_time: 1,
      lfu_log_factor: 10
    ]
  end

  defp restore_backend(nil), do: Application.delete_env(:ferricstore, :raft_backend)
  defp restore_backend(value), do: Application.put_env(:ferricstore, :raft_backend, value)

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  defp restore_waraft_app_env(key, nil),
    do: Application.delete_env(:ferricstore_waraft_backend, key)

  defp restore_waraft_app_env(key, value),
    do: Application.put_env(:ferricstore_waraft_backend, key, value)

  defp waraft_storage_label(shard_index) do
    :ferricstore_waraft_backend
    |> :wa_raft_storage.registered_name(shard_index + 1)
    |> :wa_raft_storage.label()
  end

  defp waraft_storage_status(shard_index) do
    :ferricstore_waraft_backend
    |> :wa_raft_storage.registered_name(shard_index + 1)
    |> :wa_raft_storage.status()
  end

  defp waraft_storage_metadata(root, shard_index) do
    root
    |> waraft_storage_metadata_path(shard_index)
    |> File.read!()
    |> :erlang.binary_to_term([:safe])
  end

  defp waraft_latest_storage_metadata(root, shard_index) do
    path = waraft_storage_metadata_path(root, shard_index)
    current = File.read!(path) |> :erlang.binary_to_term([:safe])

    case latest_storage_metadata_journal(path <> ".journal") do
      nil ->
        current

      journal ->
        Enum.max_by([current, journal], &storage_metadata_position_key/1)
    end
  end

  defp latest_storage_metadata_journal(path) do
    case File.read(path) do
      {:ok, binary} -> scan_storage_metadata_journal(binary, nil)
      {:error, :enoent} -> nil
    end
  end

  defp scan_storage_metadata_journal(<<>>, latest), do: latest

  defp scan_storage_metadata_journal(<<"FSMJ1", size::32, crc::32, rest::binary>>, latest)
       when byte_size(rest) >= size do
    <<payload::binary-size(size), tail::binary>> = rest

    if :erlang.crc32(payload) == crc do
      scan_storage_metadata_journal(tail, :erlang.binary_to_term(payload, [:safe]))
    else
      latest
    end
  end

  defp scan_storage_metadata_journal(_partial_or_corrupt_tail, latest), do: latest

  defp storage_metadata_position_key(%{position: {:raft_log_pos, index, term}}),
    do: {index, term}

  defp waraft_storage_metadata_path(root, shard_index) do
    Path.join([
      root,
      "waraft",
      "ferricstore_waraft_backend.#{shard_index + 1}",
      "ferricstore_storage.term"
    ])
  end

  defp waraft_storage_metadata_previous_path(root, shard_index) do
    waraft_storage_metadata_path(root, shard_index) <> ".previous"
  end

  defp waraft_storage_metadata_journal_path(root, shard_index) do
    waraft_storage_metadata_path(root, shard_index) <> ".journal"
  end

  defp waraft_segment_log_dir(root, shard_index) do
    Path.join([
      root,
      "waraft",
      "ferricstore_waraft_backend.#{shard_index + 1}",
      "segment_log"
    ])
  end

  defp read_segment_config(segment_dir) do
    segment_dir
    |> Path.join("segment_config.term")
    |> File.read!()
    |> :erlang.binary_to_term([:safe])
  end

  defp waraft_segment_log_record(shard_index) do
    partition = shard_index + 1

    {:raft_log, :"raft_log_ferricstore_waraft_backend_#{partition}", :ferricstore_waraft_backend,
     :ferricstore_waraft_backend, partition, :ferricstore_waraft_spike_segment_log}
  end

  defp key_for_shard(ctx, shard_idx) do
    Stream.iterate(0, &(&1 + 1))
    |> Enum.find_value(fn n ->
      key = "shard:#{shard_idx}:#{n}"
      if Router.shard_for(ctx, key) == shard_idx, do: key
    end)
  end

  defp force_segment_projected_key_cold!(ctx, key, shard_index \\ 0) do
    keydir = elem(ctx.keydir_refs, shard_index)

    case :ets.lookup(keydir, key) do
      [{^key, value, expire_at_ms, lfu, {:waraft_segment, index}, offset, value_size}]
      when is_binary(value) ->
        assert is_integer(index) and index > 0

        true =
          :ets.insert(
            keydir,
            {key, nil, expire_at_ms, lfu, {:waraft_segment, index}, offset, value_size}
          )

      [{^key, nil, _expire_at_ms, _lfu, {:waraft_segment, index}, _offset, _value_size}]
      when is_integer(index) and index > 0 ->
        assert_segment_projected_key_cold!(ctx, key, shard_index)
    end
  end

  defp parse_stream_ms(id) do
    [ms, "0"] = String.split(id, "-", parts: 2)
    String.to_integer(ms)
  end

  defp flow_partition_for_shard(ctx, id, shard_idx) do
    Stream.iterate(0, &(&1 + 1))
    |> Enum.find_value(fn n ->
      partition = "flow-partition:#{shard_idx}:#{n}"
      key = Ferricstore.Flow.Keys.state_key(id, partition)
      if Router.shard_for(ctx, key) == shard_idx, do: partition
    end)
  end

  defp setup_cross_shard_flow_child(
         ctx,
         parent_id,
         child_id,
         parent_partition,
         child_partition,
         opts
       ) do
    group_id = Keyword.fetch!(opts, :group_id)
    on_child_failed = Keyword.get(opts, :on_child_failed, :ignore)
    on_parent_closed = Keyword.get(opts, :on_parent_closed, :abandon_children)

    with :ok <-
           Ferricstore.Flow.create(ctx, parent_id,
             type: "parent",
             state: "dispatch",
             partition_key: parent_partition,
             now_ms: 1_000
           ),
         {:ok, created_parent} <-
           Ferricstore.Flow.get(ctx, parent_id, partition_key: parent_partition),
         :ok <-
           Ferricstore.Flow.spawn_children(
             ctx,
             parent_id,
             [%{id: child_id, type: "child", partition_key: child_partition}],
             group_id: group_id,
             wait: :all,
             wait_state: "waiting_children",
             on_child_failed: on_child_failed,
             on_parent_closed: on_parent_closed,
             exhaust_to: %{success: "children_done", failure: "children_failed"},
             partition_key: parent_partition,
             from_state: "dispatch",
             fencing_token: created_parent.fencing_token,
             now_ms: 1_010
           ) do
      Ferricstore.Flow.get(ctx, parent_id, partition_key: parent_partition)
    end
  end

  defp claim_flow_child!(ctx, id, partition_key, worker) do
    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, "child",
               partition_key: partition_key,
               worker: worker,
               limit: 1,
               now_ms: 9_000_000_000_000
             )

    assert claimed.id == id
    claimed
  end

  defp setup_cross_shard_child_for_many!(ctx, parent_id, child_id, group_id, opts \\ []) do
    parent_partition = flow_partition_for_shard(ctx, parent_id, 0)
    child_partition = flow_partition_for_shard(ctx, child_id, 1)

    assert {:ok, _waiting_parent} =
             setup_cross_shard_flow_child(
               ctx,
               parent_id,
               child_id,
               parent_partition,
               child_partition,
               Keyword.merge(opts, group_id: group_id)
             )

    {parent_partition, child_partition}
  end

  defp rewind_waraft_storage_position!(root, shard_index, position) do
    metadata_path =
      Path.join([
        root,
        "waraft",
        "ferricstore_waraft_backend.#{shard_index + 1}",
        "ferricstore_storage.term"
      ])

    metadata =
      metadata_path
      |> File.read!()
      |> :erlang.binary_to_term([:safe])
      |> Map.put(:position, position)

    File.write!(metadata_path, :erlang.term_to_binary(metadata))
  end

  defp kill_waraft_server!(shard_index) do
    server =
      :ferricstore_waraft_backend
      |> :wa_raft_server.registered_name(shard_index + 1)

    pid = Process.whereis(server)
    assert is_pid(pid), "expected live WARaft server #{inspect(server)}"

    Process.exit(pid, :kill)
  end

  defp kill_waraft_server!(node, shard_index) do
    server =
      :ferricstore_waraft_backend
      |> :wa_raft_server.registered_name(shard_index + 1)

    pid = :rpc.call(node, Process, :whereis, [server])
    assert is_pid(pid), "expected live WARaft server #{inspect(server)} on #{inspect(node)}"

    :rpc.call(node, Process, :exit, [pid, :kill])
  end

  defp wait_for_kill_load_acks(0, acked), do: acked

  defp wait_for_kill_load_acks(remaining, acked) do
    receive do
      {:waraft_kill_load_result, key, value, :ok} ->
        wait_for_kill_load_acks(remaining - 1, [{key, value} | acked])

      {:waraft_kill_load_result, _key, _value, _error} ->
        wait_for_kill_load_acks(remaining, acked)
    after
      5_000 ->
        flunk("expected #{remaining} more acknowledged writes before killing WARaft server")
    end
  end

  defp drain_kill_load_results(acked) do
    receive do
      {:waraft_kill_load_result, key, value, :ok} ->
        drain_kill_load_results([{key, value} | acked])

      {:waraft_kill_load_result, _key, _value, _error} ->
        drain_kill_load_results(acked)
    after
      0 ->
        acked
    end
  end

  defp wait_for_multi_kill_shard_acks(_shard_index, 0, acked), do: acked

  defp wait_for_multi_kill_shard_acks(shard_index, remaining, acked) do
    receive do
      {:waraft_multi_kill_result, ^shard_index, key, value, :ok} ->
        wait_for_multi_kill_shard_acks(shard_index, remaining - 1, [
          {shard_index, key, value} | acked
        ])

      {:waraft_multi_kill_result, other_shard_index, key, value, :ok} ->
        wait_for_multi_kill_shard_acks(shard_index, remaining, [
          {other_shard_index, key, value} | acked
        ])

      {:waraft_multi_kill_result, _shard_index, _key, _value, _error} ->
        wait_for_multi_kill_shard_acks(shard_index, remaining, acked)
    after
      5_000 ->
        flunk("expected #{remaining} more acknowledged writes on shard #{shard_index}")
    end
  end

  defp drain_multi_kill_results(acked) do
    receive do
      {:waraft_multi_kill_result, shard_index, key, value, :ok} ->
        drain_multi_kill_results([{shard_index, key, value} | acked])

      {:waraft_multi_kill_result, _shard_index, _key, _value, _error} ->
        drain_multi_kill_results(acked)
    after
      0 ->
        Enum.uniq(acked)
    end
  end

  defp wait_for_cluster_kill_acks(0, acked), do: acked

  defp wait_for_cluster_kill_acks(remaining, acked) do
    receive do
      {:waraft_cluster_kill_result, key, value, :ok} ->
        wait_for_cluster_kill_acks(remaining - 1, [{key, value} | acked])

      {:waraft_cluster_kill_result, _key, _value, _error} ->
        wait_for_cluster_kill_acks(remaining, acked)
    after
      10_000 ->
        flunk("expected #{remaining} more acknowledged cluster writes before kill")
    end
  end

  defp drain_cluster_kill_results(acked) do
    receive do
      {:waraft_cluster_kill_result, key, value, :ok} ->
        drain_cluster_kill_results([{key, value} | acked])

      {:waraft_cluster_kill_result, _key, _value, _error} ->
        drain_cluster_kill_results(acked)
    after
      0 ->
        Enum.uniq(acked)
    end
  end

  defp shard_dir_specs(ctx, shard_index) do
    [
      data: Ferricstore.DataDir.shard_data_path(ctx.data_dir, shard_index),
      blob: Ferricstore.DataDir.blob_shard_path(ctx.data_dir, shard_index),
      dedicated: Path.join([ctx.data_dir, "dedicated", "shard_#{shard_index}"]),
      prob: Path.join([ctx.data_dir, "prob", "shard_#{shard_index}"])
    ]
  end

  defp shard_payload_present?(ctx, shard_index) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> Path.join("*.log")
    |> Path.wildcard()
    |> Enum.any?(fn path ->
      case File.stat(path) do
        {:ok, %{size: size}} when size > 0 -> true
        _other -> false
      end
    end)
  end

  defp unknown_atom_payload(atom_name) when is_binary(atom_name) do
    <<131, 100, byte_size(atom_name)::16, atom_name::binary>>
  end

  defp existing_atom?(atom_name) when is_binary(atom_name) do
    _ = String.to_existing_atom(atom_name)
    true
  rescue
    ArgumentError -> false
  end

  defp restore_chmoded_snapshot_dirs do
    receive do
      {:snapshot_payload_dir_chmod, path} ->
        _ = File.chmod(path, 0o700)
        restore_chmoded_snapshot_dirs()
    after
      0 -> :ok
    end
  end

  defp drain_storage_metadata_fsyncs do
    receive do
      {:storage_metadata_fsync, _path} -> drain_storage_metadata_fsyncs()
    after
      0 -> :ok
    end
  end

  defp collect_storage_metadata_fsyncs(acc \\ []) do
    receive do
      {:storage_metadata_fsync, path} -> collect_storage_metadata_fsyncs([path | acc])
    after
      50 -> Enum.reverse(acc)
    end
  end

  defp assert_eventually(fun, expected, attempts \\ 50)

  defp assert_eventually(_fun, expected, 0),
    do: flunk("expected eventual value #{inspect(expected)}")

  defp assert_eventually(fun, expected, attempts) do
    case fun.() do
      ^expected ->
        :ok

      _other ->
        Process.sleep(20)
        assert_eventually(fun, expected, attempts - 1)
    end
  end

  defp raw_waraft_async_put(acceptor, reply_ref, key, value) do
    stamped = Ferricstore.Raft.CommandClock.to_ttb({:put, key, value, 0})
    :wa_raft_acceptor.commit_async(acceptor, {self(), reply_ref}, {make_ref(), stamped}, :low)
  end

  defp start_waraft_backend_peers(unique, count) do
    for i <- 1..count do
      name = :"waraft_backend_#{unique}_#{i}"
      data_dir = Path.join(System.tmp_dir!(), "ferricstore-waraft-backend-peer-#{unique}-#{i}")
      File.rm_rf!(data_dir)
      File.mkdir_p!(data_dir)

      node = start_waraft_backend_peer_node!(name, data_dir)
      start_peer_runtime_apps!(node.name)
      node
    end
  end

  defp restart_waraft_backend_peer!(%{name: node_name, data_dir: data_dir}) do
    node_name
    |> peer_local_name()
    |> start_waraft_backend_peer_node!(data_dir)
  end

  defp kill_peer_os_process!(%{name: node_name, peer: peer}) do
    pid = :rpc.call(node_name, System, :pid, [])
    assert is_binary(pid) or is_list(pid)

    previous_trap_exit = Process.flag(:trap_exit, true)
    monitor_ref = Process.monitor(peer)
    Node.monitor(node_name, true)

    try do
      assert {"", 0} = System.cmd("kill", ["-9", to_string(pid)])
      wait_for_node_down!(node_name)

      receive do
        {:DOWN, ^monitor_ref, :process, ^peer, _reason} -> :ok
      after
        1_000 -> :ok
      end

      receive do
        {:EXIT, ^peer, _reason} -> :ok
      after
        0 -> :ok
      end
    after
      Process.demonitor(monitor_ref, [:flush])
      Node.monitor(node_name, false)
      Process.flag(:trap_exit, previous_trap_exit)
    end
  end

  defp wait_for_node_down!(node_name, attempts \\ 100)

  defp wait_for_node_down!(node_name, 0),
    do: flunk("expected #{inspect(node_name)} to go down after OS kill")

  defp wait_for_node_down!(node_name, attempts) do
    if Node.ping(node_name) == :pang do
      :ok
    else
      receive do
        {:nodedown, ^node_name} ->
          :ok

        {:nodedown, ^node_name, _info} ->
          :ok
      after
        50 ->
          wait_for_node_down!(node_name, attempts - 1)
      end
    end
  end

  defp partition_waraft_peer!(node, all_nodes) do
    others = Enum.reject(all_nodes, &(&1.name == node.name))
    real_cookie = :rpc.call(node.name, :erlang, :get_cookie, [])
    assert is_atom(real_cookie)

    assert true = :rpc.call(node.name, :erlang, :set_cookie, [node.name, :waraft_partitioned])

    for other <- others do
      :rpc.call(node.name, :erlang, :disconnect_node, [other.name])
      :rpc.call(other.name, :erlang, :disconnect_node, [node.name])
    end

    assert eventually(
             fn ->
               Enum.all?(others, fn other ->
                 :rpc.call(other.name, Node, :ping, [node.name]) == :pang and
                   :rpc.call(node.name, Node, :ping, [other.name]) == :pang
               end)
             end,
             40
           )

    real_cookie
  end

  defp heal_waraft_peer_partition!(node, all_nodes, real_cookie) do
    others = Enum.reject(all_nodes, &(&1.name == node.name))
    assert true = :rpc.call(node.name, :erlang, :set_cookie, [node.name, real_cookie])

    assert eventually(
             fn ->
               Enum.each(others, fn other ->
                 :rpc.call(other.name, Node, :connect, [node.name])
                 :rpc.call(node.name, Node, :connect, [other.name])
               end)

               Enum.all?(others, fn other ->
                 node_sees_other =
                   case :rpc.call(node.name, Node, :list, []) do
                     peers when is_list(peers) -> other.name in peers
                     _other -> false
                   end

                 other_sees_node =
                   case :rpc.call(other.name, Node, :list, []) do
                     peers when is_list(peers) -> node.name in peers
                     _other -> false
                   end

                 node_sees_other and other_sees_node
               end)
             end,
             40
           )

    :ok
  end

  defp start_waraft_backend_peer_node!(name, data_dir) do
    code_paths = Enum.flat_map(:code.get_path(), fn path -> [~c"-pa", path] end)
    cookie = Atom.to_charlist(Node.get_cookie())

    {:ok, peer, node_name} =
      :peer.start(%{
        name: name,
        args: code_paths ++ [~c"-connect_all", ~c"false", ~c"-setcookie", cookie],
        wait_boot: 120_000
      })

    %{name: node_name, peer: peer, data_dir: data_dir}
  end

  defp peer_local_name(node_name) when is_atom(node_name) do
    node_name
    |> Atom.to_string()
    |> String.split("@", parts: 2)
    |> hd()
    |> String.to_atom()
  end

  defp start_waraft_backend_peer_cluster!(nodes, unique, opts \\ []) do
    names = Enum.map(nodes, & &1.name)
    shard_count = Keyword.get(opts, :shard_count, 1)
    backend_opts = Keyword.take(opts, [:election_timeout_ms, :election_timeout_ms_max])

    for node <- nodes do
      start_waraft_backend_peer!(node, unique, [shard_count: shard_count] ++ backend_opts)
    end

    for node <- names do
      assert :ok = :rpc.call(node, WARaftBackend, :bootstrap_cluster, [names])
    end

    leaders =
      for shard_index <- 0..(shard_count - 1) do
        assert :ok = :rpc.call(hd(names), WARaftBackend, :trigger_election, [shard_index])
        wait_for_waraft_backend_leader(names, shard_index)
      end

    hd(leaders)
  end

  defp stop_waraft_backend_peer_cluster!(nodes, unique) do
    instance_name = waraft_backend_peer_instance_name(unique)

    for node <- nodes do
      assert :ok = :rpc.call(node.name, WARaftBackend, :stop, [])
      _ = :rpc.call(node.name, FerricStore.Instance, :cleanup, [instance_name])
    end

    :ok
  end

  defp waraft_backend_peer_instance_name(unique), do: :"waraft_backend_peer_#{unique}"

  defp start_waraft_backend_peer!(node, unique, opts \\ []) do
    shard_count = Keyword.get(opts, :shard_count, 1)
    backend_opts = Keyword.take(opts, [:election_timeout_ms, :election_timeout_ms_max])

    ctx =
      :rpc.call(node.name, FerricStore.Instance, :build, [
        waraft_backend_peer_instance_name(unique),
        instance_opts(node.data_dir, shard_count: shard_count)
      ])

    assert %FerricStore.Instance{} = ctx

    assert :ok =
             :rpc.call(node.name, WARaftBackend, :start, [
               ctx,
               [bootstrap: false, log_module: :ferricstore_waraft_spike_segment_log] ++
                 backend_opts
             ])
  end

  defp key_for_shard(ctx, shard_index, prefix) do
    1..10_000
    |> Enum.map(&"#{prefix}:#{shard_index}:#{&1}")
    |> Enum.find(&(Router.shard_for(ctx, &1) == shard_index))
  end

  defp delayed_parallel_batch_record?(
         {:put, "waraft-parallel-batch:" <> _, _value, _expire_at_ms}
       ),
       do: true

  defp delayed_parallel_batch_record?(_other), do: false

  defp start_peer_runtime_apps!(node_name) do
    assert {:ok, _} = :rpc.call(node_name, Application, :ensure_all_started, [:telemetry])
    assert {:ok, _} = :rpc.call(node_name, Application, :ensure_all_started, [:os_mon])
  end

  defp wait_for_waraft_backend_leader(names, shard_index),
    do: wait_for_waraft_backend_leader(names, shard_index, 100)

  defp wait_for_waraft_backend_leader(_names, shard_index, 0),
    do: flunk("WARaft backend leader was not elected for shard #{shard_index}")

  defp wait_for_waraft_backend_leader(names, shard_index, attempts) do
    case Enum.find(names, fn node ->
           case :rpc.call(node, WARaftBackend, :status, [shard_index]) do
             status when is_list(status) -> Keyword.get(status, :state) == :leader
             _other -> false
           end
         end) do
      nil ->
        Process.sleep(50)
        wait_for_waraft_backend_leader(names, shard_index, attempts - 1)

      leader ->
        leader
    end
  end

  defp eventually(fun, attempts \\ 100)
  defp eventually(_fun, 0), do: false

  defp eventually(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(50)
      eventually(fun, attempts - 1)
    end
  end

  defp ensure_distribution! do
    case Node.self() do
      :nonode@nohost ->
        node_name = :"waraft_backend_runner_#{:erlang.unique_integer([:positive])}"
        assert {:ok, _} = Node.start(node_name, :shortnames)

      _node ->
        :ok
    end

    Node.set_cookie(:ferricstore_waraft_backend_test)
  end
end
