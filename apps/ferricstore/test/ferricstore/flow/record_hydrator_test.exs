defmodule Ferricstore.Flow.RecordHydratorTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.{Codec, Keys, Locator, RecordHydrator, StorageScope}
  alias Ferricstore.Raft.WARaftSegmentReader
  alias Ferricstore.Raft.WARaftSegmentReader.DiskReader
  alias Ferricstore.Store.{BlobRef, BlobStore}

  test "hydrates an ordered batch grouped across Bitcask and WARaft locations" do
    root = tmp_root("mixed")
    shard_path = Ferricstore.DataDir.shard_data_path(root, 0)
    File.mkdir_p!(shard_path)
    bitcask_path = Path.join(shard_path, "00003.log")

    first = record("run-1", 1)
    second = record("run-2", 2)
    third = record("run-3", 3)
    first_key = Keys.state_key(first.id, first.partition_key)
    second_key = Keys.state_key(second.id, second.partition_key)
    third_key = Keys.state_key(third.id, third.partition_key)
    first_encoded = Ferricstore.Flow.encode_record(first)
    second_encoded = Ferricstore.Flow.encode_record(second)
    third_encoded = Ferricstore.Flow.encode_record(third)

    assert {:ok, [{first_offset, _}, {second_offset, _}]} =
             NIF.v2_append_batch(bitcask_path, [
               {first_key, first_encoded, 0},
               {second_key, second_encoded, 0}
             ])

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, 9, [
               {third_key, third_encoded, 0}
             ])

    requests = [
      {second_key, locator(second, 3, second_offset, byte_size(second_encoded), raft_index: 2)},
      {third_key, durable_apply_locator(root, third_key, third, 9, byte_size(third_encoded))},
      {first_key, locator(first, 3, first_offset, byte_size(first_encoded), raft_index: 1)}
    ]

    assert {:ok, hydrated} =
             RecordHydrator.read_many(%{data_dir: root}, 0, requests, max_bytes: 1_000_000)

    assert Enum.map(hydrated, &{&1.id, &1.version}) == [
             {second.id, second.version},
             {third.id, third.version},
             {first.id, first.version}
           ]
  end

  test "returns authoritative encoded bytes after applying the same validation" do
    root = tmp_root("encoded")
    record = record("run-encoded", 4)
    state_key = Keys.state_key(record.id, record.partition_key)
    encoded = Ferricstore.Flow.encode_record(record)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, 12, [
               {state_key, encoded, 0}
             ])

    locator = durable_apply_locator(root, state_key, record, 12, byte_size(encoded))

    assert {:ok, [^encoded]} =
             RecordHydrator.read_encoded_many(
               %{data_dir: root},
               0,
               [{state_key, locator}],
               max_bytes: 1_000_000
             )
  end

  test "decodes a hydrated page with one bounded batch NIF call" do
    root = tmp_root("batch_decode")
    records = Enum.map(1..100, &record("run-batch-#{&1}", &1))

    entries =
      Enum.map(records, fn record ->
        state_key = Keys.state_key(record.id, record.partition_key)
        {state_key, Ferricstore.Flow.encode_record(record), 0}
      end)

    assert :ok = WARaftSegmentReader.put_apply_projection(root, 0, 30, entries)

    keys = Enum.map(entries, &elem(&1, 0))
    {ordinal, offset, frame_size} = durable_apply_location!(root, 30, keys)

    requests =
      Enum.map(records, fn record ->
        state_key = Keys.state_key(record.id, record.partition_key)
        encoded = Ferricstore.Flow.encode_record(record)

        {state_key,
         locator(record, {:waraft_apply_projection, 30}, offset, byte_size(encoded),
           raft_index: 30,
           segment_generation: ordinal,
           frame_size: frame_size
         )}
      end)

    parent = self()
    trace_tag = make_ref()
    tracer = spawn(fn -> forward_traces(parent, trace_tag) end)
    assert :erlang.trace_pattern({Codec, :decode_records, 1}, true, []) > 0
    assert :erlang.trace_pattern({Codec, :decode_record, 1}, true, []) > 0
    assert :erlang.trace(self(), true, [:call, {:tracer, tracer}]) == 1

    try do
      assert {:ok, hydrated} =
               RecordHydrator.read_many(%{data_dir: root}, 0, requests, max_bytes: 1_000_000)

      assert Enum.map(hydrated, & &1.id) == Enum.map(records, & &1.id)
      delivered = :erlang.trace_delivered(self())
      assert_receive {:trace_delivered, _pid, ^delivered}, 1_000
      calls = traced_calls(trace_tag)
      assert Enum.count(calls, &match?({Codec, :decode_records, [_values]}, &1)) == 1
      assert Enum.count(calls, &match?({Codec, :decode_record, [_value]}, &1)) == 0
    after
      :erlang.trace(self(), false, [:call])
      :erlang.trace_pattern({Codec, :decode_records, 1}, false, [])
      :erlang.trace_pattern({Codec, :decode_record, 1}, false, [])
      send(tracer, :stop)
    end
  end

  test "hydrates distinct Raft indexes with one bounded disk-reader batch" do
    root = tmp_root("batch_disk_read")
    records = Enum.map(1..12, &record("run-disk-batch-#{&1}", &1))

    requests =
      Enum.map(records, fn record ->
        index = 500 + record.version
        state_key = Keys.state_key(record.id, record.partition_key)
        encoded = Ferricstore.Flow.encode_record(record)

        assert :ok =
                 WARaftSegmentReader.put_apply_projection(root, 0, index, [
                   {state_key, encoded, 0}
                 ])

        assert {:ok, 1} =
                 WARaftSegmentReader.ensure_apply_projection_entries_durable(
                   root,
                   0,
                   [{index, state_key}]
                 )

        assert {:ok, {ordinal, offset, frame_size}} =
                 WARaftSegmentReader.physical_location(
                   %{data_dir: root},
                   0,
                   {:waraft_apply_projection, index}
                 )

        {state_key,
         locator(record, {:waraft_apply_projection, index}, offset, byte_size(encoded),
           raft_index: index,
           segment_generation: ordinal,
           frame_size: frame_size
         )}
      end)

    _removed = WARaftSegmentReader.clear_apply_projection_cache(root, 0)
    parent = self()
    trace_tag = make_ref()
    tracer = spawn(fn -> forward_traces(parent, trace_tag) end)
    assert :erlang.trace_pattern({DiskReader, :read_many_at, 4}, true, []) > 0
    assert :erlang.trace_pattern({DiskReader, :read, 3}, true, []) > 0
    assert :erlang.trace(self(), true, [:call, {:tracer, tracer}]) == 1

    try do
      assert {:ok, hydrated} =
               RecordHydrator.read_many(%{data_dir: root}, 0, requests, max_bytes: 1_000_000)

      assert Enum.map(hydrated, & &1.id) == Enum.map(records, & &1.id)
      delivered = :erlang.trace_delivered(self())
      assert_receive {:trace_delivered, _pid, ^delivered}, 1_000
      calls = traced_calls(trace_tag)

      assert Enum.count(
               calls,
               &match?(
                 {DiskReader, :read_many_at, [_root, _log_root, _requests, _timeout_ms]},
                 &1
               )
             ) == 1

      assert Enum.count(calls, &match?({DiskReader, :read, [_root, _index, _location]}, &1)) == 0
    after
      :erlang.trace(self(), false, [:call])
      :erlang.trace_pattern({DiskReader, :read_many_at, 4}, false, [])
      :erlang.trace_pattern({DiskReader, :read, 3}, false, [])
      send(tracer, :stop)
    end
  end

  test "hydrates checkpoint projection frames with one vector read per segment" do
    root = tmp_root("checkpoint_projection_batch")

    records = [record("run-checkpoint-1", 1), record("run-checkpoint-2", 2)]

    entries =
      Enum.map(records, fn record ->
        state_key = Keys.state_key(record.id, record.partition_key)
        {state_key, Ferricstore.Flow.encode_record(record), 0}
      end)

    projection_root =
      Path.join([
        root,
        "waraft",
        "ferricstore_waraft_backend.1",
        "segment_projection_log"
      ])

    assert :ok =
             :ferricstore_waraft_spike_segment_log.write_projection(
               to_charlist(projection_root),
               {:raft_log_pos, 900, 1},
               entries
             )

    requests =
      records
      |> Enum.with_index(1)
      |> Enum.map(fn {record, projection_index} ->
        state_key = Keys.state_key(record.id, record.partition_key)
        encoded = Ferricstore.Flow.encode_record(record)
        file_id = {:waraft_projection, projection_index}

        assert {:ok, {ordinal, offset, frame_size}} =
                 WARaftSegmentReader.physical_location(%{data_dir: root}, 0, file_id)

        {state_key,
         locator(record, file_id, offset, byte_size(encoded),
           raft_index: projection_index,
           segment_generation: ordinal,
           frame_size: frame_size
         )}
      end)

    parent = self()
    trace_tag = make_ref()
    tracer = spawn(fn -> forward_traces(parent, trace_tag) end)

    assert :erlang.trace_pattern({DiskReader, :read_many_at, 4}, true, []) > 0

    assert :erlang.trace(self(), true, [:call, {:tracer, tracer}]) == 1

    try do
      assert {:ok, hydrated} =
               RecordHydrator.read_many(%{data_dir: root}, 0, requests, max_bytes: 1_000_000)

      assert Enum.map(hydrated, & &1.id) == Enum.map(records, & &1.id)
      delivered = :erlang.trace_delivered(self())
      assert_receive {:trace_delivered, _pid, ^delivered}, 1_000

      calls = traced_calls(trace_tag)

      assert [
               {DiskReader, :read_many_at, [_root, log_root, batch, timeout_ms]}
             ] =
               Enum.filter(
                 calls,
                 &match?(
                   {DiskReader, :read_many_at, [_root, _log_root, _requests, _timeout_ms]},
                   &1
                 )
               )

      assert log_root == projection_root
      assert length(batch) == 2
      assert is_integer(timeout_ms) and timeout_ms > 0
    after
      :erlang.trace(self(), false, [:call])
      :erlang.trace_pattern({DiskReader, :read_many_at, 4}, false, [])
      send(tracer, :stop)
    end
  end

  test "hydrates the recorded physical WARaft frame and fails closed on a stale offset" do
    root = tmp_root("physical_frame")
    record = record("run-physical-frame", 18)
    state_key = Keys.state_key(record.id, record.partition_key)
    encoded = Ferricstore.Flow.encode_record(record)
    index = 718

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, index, [
               {state_key, encoded, 0}
             ])

    assert {:ok, 1} =
             WARaftSegmentReader.ensure_apply_projection_entries_durable(
               root,
               0,
               [{index, state_key}]
             )

    assert {:ok, {ordinal, offset, frame_size}} =
             WARaftSegmentReader.physical_location(
               %{data_dir: root},
               0,
               {:waraft_apply_projection, index}
             )

    physical =
      locator(record, {:waraft_apply_projection, index}, offset, byte_size(encoded),
        raft_index: index,
        segment_generation: ordinal,
        frame_size: frame_size
      )

    _removed = WARaftSegmentReader.clear_apply_projection_cache(root, 0)

    assert {:ok, [%{id: "run-physical-frame"}]} =
             RecordHydrator.read_many(%{data_dir: root}, 0, [{state_key, physical}],
               max_bytes: 1_000_000
             )

    stale = %{physical | offset: physical.offset + 1}

    assert {:error, _reason} =
             RecordHydrator.read_many(%{data_dir: root}, 0, [{state_key, stale}],
               max_bytes: 1_000_000
             )
  end

  test "rejects a checksumless state locator before issuing a storage read" do
    record = record("run-checksumless", 1)
    state_key = Keys.state_key(record.id, record.partition_key)

    checksumless =
      locator(record, {:waraft_apply_projection, 999}, 0, 128,
        raft_index: 999,
        checksum: nil
      )

    assert {:error, :invalid_hydration_request} =
             RecordHydrator.read_many(
               %{data_dir: "/path-that-must-not-be-read"},
               0,
               [{state_key, checksumless}],
               max_bytes: 1_000_000
             )
  end

  test "applies one aggregate timeout across independent WARaft groups" do
    root = tmp_root("aggregate_timeout")
    first = record("run-timeout-1", 1)
    second = record("run-timeout-2", 2)
    first_key = Keys.state_key(first.id, first.partition_key)
    second_key = Keys.state_key(second.id, second.partition_key)
    first_encoded = Ferricstore.Flow.encode_record(first)
    second_encoded = Ferricstore.Flow.encode_record(second)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, 20, [
               {first_key, first_encoded, 0}
             ])

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, 21, [
               {second_key, second_encoded, 0}
             ])

    requests = [
      {first_key, durable_apply_locator(root, first_key, first, 20, byte_size(first_encoded))},
      {second_key, durable_apply_locator(root, second_key, second, 21, byte_size(second_encoded))}
    ]

    counter = :counters.new(1, [])

    clock_ms = fn ->
      call = :counters.get(counter, 1)
      :counters.add(counter, 1, 1)
      if call < 2, do: 0, else: 6
    end

    assert {:error, :hydration_timeout} =
             RecordHydrator.read_many(%{data_dir: root}, 0, requests,
               max_bytes: 1_000_000,
               timeout_ms: 5,
               clock_ms: clock_ms
             )
  end

  test "normalizes a physical reader deadline while compaction owns the disk latch" do
    root = tmp_root("compaction_deadline")
    record = record("run-compaction-timeout", 1)
    state_key = Keys.state_key(record.id, record.partition_key)
    encoded = Ferricstore.Flow.encode_record(record)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, 22, [
               {state_key, encoded, 0}
             ])

    locator = durable_apply_locator(root, state_key, record, 22, byte_size(encoded))
    parent = self()

    writer =
      Task.async(fn ->
        WARaftSegmentReader.with_apply_projection_disk_lock(root, 0, fn ->
          send(parent, :hydration_disk_latch_acquired)

          receive do
            :release_hydration_disk_latch -> :ok
          end
        end)
      end)

    assert_receive :hydration_disk_latch_acquired, 1_000

    reader =
      Task.async(fn ->
        RecordHydrator.read_many(%{data_dir: root}, 0, [{state_key, locator}],
          max_bytes: 1_000_000,
          timeout_ms: 20
        )
      end)

    yielded = Task.yield(reader, 250)
    send(writer.pid, :release_hydration_disk_latch)
    assert :ok = Task.await(writer, 1_000)

    if is_nil(yielded), do: Task.shutdown(reader, :brutal_kill)

    assert {:ok, {:error, :hydration_timeout}} = yielded
  end

  test "retention hydration can explicitly include an expired apply projection" do
    root = tmp_root("expired")
    record = record("run-expired", 5)
    state_key = Keys.state_key(record.id, record.partition_key)
    encoded = Ferricstore.Flow.encode_record(record)
    expired_at_ms = Ferricstore.HLC.now_ms() - 1

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, 13, [
               {state_key, encoded, expired_at_ms}
             ])

    locator =
      durable_apply_locator(root, state_key, record, 13, byte_size(encoded),
        expire_at_ms: expired_at_ms
      )

    assert {:ok, [nil]} =
             RecordHydrator.read_many(%{data_dir: root}, 0, [{state_key, locator}],
               max_bytes: 1_000_000
             )

    assert {:ok, [%{id: "run-expired", version: 5}]} =
             RecordHydrator.read_many(%{data_dir: root}, 0, [{state_key, locator}],
               max_bytes: 1_000_000,
               include_expired: true
             )
  end

  test "filters an expired Bitcask locator before raw offset hydration" do
    root = tmp_root("expired_bitcask")
    shard_path = Ferricstore.DataDir.shard_data_path(root, 0)
    File.mkdir_p!(shard_path)
    path = Path.join(shard_path, "00003.log")
    record = record("run-expired-bitcask", 6)
    state_key = Keys.state_key(record.id, record.partition_key)
    encoded = Ferricstore.Flow.encode_record(record)

    assert {:ok, [{offset, _bytes}]} =
             NIF.v2_append_batch(path, [{state_key, encoded, 500}])

    locator =
      locator(record, 3, offset, byte_size(encoded),
        raft_index: 6,
        expire_at_ms: 500
      )

    assert {:ok, [nil]} =
             RecordHydrator.read_many(%{data_dir: root}, 0, [{state_key, locator}],
               max_bytes: 1_000_000,
               now_ms: 1_000
             )

    assert {:ok, [hydrated]} =
             RecordHydrator.read_many(%{data_dir: root}, 0, [{state_key, locator}],
               max_bytes: 1_000_000,
               now_ms: 1_000,
               include_expired: true
             )

    assert hydrated.id == record.id
    assert hydrated.version == record.version
  end

  test "accounts for referenced blob bytes before loading them" do
    root = tmp_root("blob_budget")
    state_key = Keys.state_key("run-1", "tenant-a")
    payload = :binary.copy("x", 4_096)
    assert {:ok, ref} = BlobStore.put(root, 0, payload)
    encoded_ref = BlobRef.encode!(ref)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, 10, [
               {state_key, encoded_ref, 0}
             ])

    {ordinal, offset, frame_size} = durable_apply_location!(root, 10, [state_key])

    locator =
      Locator.new!(
        flow_id: "run-1",
        kind: :state,
        version: 1,
        raft_index: 10,
        file_id: {:waraft_apply_projection, 10},
        offset: offset,
        value_size: byte_size(encoded_ref),
        frame_size: frame_size,
        segment_generation: ordinal,
        checksum: :binary.copy(<<1>>, 32)
      )

    ctx = %{data_dir: root, blob_side_channel_threshold_bytes: 64}

    assert {:error, :hydration_byte_budget_exceeded} =
             RecordHydrator.read_many(ctx, 0, [{state_key, locator}], max_bytes: 1_024)
  end

  test "returns exact stored blob-reference bytes after validating the materialized record" do
    root = tmp_root("stored_blob_ref")
    record = record("run-stored-ref", 7)
    state_key = Keys.state_key(record.id, record.partition_key)
    encoded = Ferricstore.Flow.encode_record(record)
    assert {:ok, ref} = BlobStore.put(root, 0, encoded)
    encoded_ref = BlobRef.encode!(ref)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, 107, [
               {state_key, encoded_ref, 0}
             ])

    locator =
      durable_apply_locator(root, state_key, record, 107, byte_size(encoded_ref),
        checksum: :crypto.hash(:sha256, encoded)
      )

    ctx = %{data_dir: root, blob_side_channel_threshold_bytes: 64}

    assert {:ok, [^encoded]} =
             RecordHydrator.read_encoded_many(ctx, 0, [{state_key, locator}],
               max_bytes: 1_000_000
             )

    assert {:ok, [^encoded_ref]} =
             RecordHydrator.read_stored_many(ctx, 0, [{state_key, locator}], max_bytes: 1_000_000)
  end

  test "reads checksum-validated storage refs without materializing blob payloads" do
    root = tmp_root("storage_ref_without_payload_read")
    record = record("run-storage-ref", 8)
    state_key = Keys.state_key(record.id, record.partition_key)
    encoded = Ferricstore.Flow.encode_record(record)
    assert {:ok, ref} = BlobStore.put(root, 0, encoded)
    encoded_ref = BlobRef.encode!(ref)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, 108, [
               {state_key, encoded_ref, 0}
             ])

    locator =
      durable_apply_locator(root, state_key, record, 108, byte_size(encoded_ref),
        checksum: ref.checksum
      )

    Process.put(:ferricstore_blob_store_open_read_hook, fn _path, _modes ->
      flunk("storage-ref hydration must not open the blob payload")
    end)

    on_exit(fn -> Process.delete(:ferricstore_blob_store_open_read_hook) end)

    ctx = %{data_dir: root, blob_side_channel_threshold_bytes: 64}

    assert {:ok, [^encoded_ref]} =
             RecordHydrator.read_storage_refs_many(ctx, 0, [{state_key, locator}],
               max_bytes: 1_000_000
             )

    invalid = %{locator | checksum: :binary.copy(<<0>>, 32)}

    assert {:error, :hydrated_record_identity_mismatch} =
             RecordHydrator.read_storage_refs_many(ctx, 0, [{state_key, invalid}],
               max_bytes: 1_000_000
             )
  end

  test "fails closed when the located record does not match key identity or generation" do
    root = tmp_root("identity")
    state_key = Keys.state_key("run-1", "tenant-a")
    wrong = record("other-run", 2)
    encoded = Ferricstore.Flow.encode_record(wrong)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, 11, [
               {state_key, encoded, 0}
             ])

    {ordinal, offset, frame_size} = durable_apply_location!(root, 11, [state_key])

    locator =
      Locator.new!(
        flow_id: "run-1",
        kind: :state,
        version: 1,
        raft_index: 11,
        file_id: {:waraft_apply_projection, 11},
        offset: offset,
        value_size: byte_size(encoded),
        frame_size: frame_size,
        segment_generation: ordinal,
        checksum: :crypto.hash(:sha256, encoded)
      )

    assert {:error, :hydrated_record_identity_mismatch} =
             RecordHydrator.read_many(%{data_dir: root}, 0, [{state_key, locator}],
               max_bytes: 1_000_000
             )
  end

  test "fails closed when authoritative bytes do not match the locator checksum" do
    root = tmp_root("checksum_mismatch")
    record = record("run-checksum-mismatch", 3)
    state_key = Keys.state_key(record.id, record.partition_key)
    encoded = Ferricstore.Flow.encode_record(record)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, 16, [
               {state_key, encoded, 0}
             ])

    locator =
      durable_apply_locator(root, state_key, record, 16, byte_size(encoded),
        checksum: :binary.copy(<<0>>, 32)
      )

    assert {:error, :hydrated_record_identity_mismatch} =
             RecordHydrator.read_many(%{data_dir: root}, 0, [{state_key, locator}],
               max_bytes: 1_000_000
             )
  end

  test "fails closed when the physical value size differs from the admitted locator size" do
    root = tmp_root("size_mismatch")
    record = record("run-size-mismatch", 3)
    state_key = Keys.state_key(record.id, record.partition_key)
    encoded = Ferricstore.Flow.encode_record(record)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, 17, [
               {state_key, encoded, 0}
             ])

    locator = durable_apply_locator(root, state_key, record, 17, byte_size(encoded) - 1)

    assert {:error, :hydrated_record_size_mismatch} =
             RecordHydrator.read_many(%{data_dir: root}, 0, [{state_key, locator}],
               max_bytes: 1_000_000
             )
  end

  test "binds hydrated shared records to their sealed physical scope" do
    root = tmp_root("shared_scope")
    scope = <<42::unsigned-big-64>>
    logical = record("run-shared", 6)

    assert {:ok, physical_partition} =
             StorageScope.physical_partition_key(logical.partition_key, scope)

    valid =
      logical
      |> Map.put(:partition_key, physical_partition)
      |> Map.put(:system_metadata, scope_metadata(42))

    state_key = Keys.state_key(valid.id, physical_partition)
    valid_encoded = Ferricstore.Flow.encode_record(valid)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, 14, [
               {state_key, valid_encoded, 0}
             ])

    valid_locator =
      durable_apply_locator(root, state_key, valid, 14, byte_size(valid_encoded))

    assert {:ok, [%{id: "run-shared", version: 6}]} =
             RecordHydrator.read_many(%{data_dir: root}, 0, [{state_key, valid_locator}],
               max_bytes: 1_000_000
             )

    forged = Map.put(valid, :system_metadata, scope_metadata(99))
    forged_encoded = Ferricstore.Flow.encode_record(forged)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(root, 0, 15, [
               {state_key, forged_encoded, 0}
             ])

    forged_locator =
      durable_apply_locator(root, state_key, forged, 15, byte_size(forged_encoded))

    assert {:error, :hydrated_record_identity_mismatch} =
             RecordHydrator.read_many(%{data_dir: root}, 0, [{state_key, forged_locator}],
               max_bytes: 1_000_000
             )
  end

  test "rejects admitted locator bytes before issuing storage reads" do
    state_key = Keys.state_key("run-1", "tenant-a")
    locator = locator(record("run-1", 1), 3, 0, 2_000)

    assert {:error, :hydration_byte_budget_exceeded} =
             RecordHydrator.read_many(%{data_dir: "/missing"}, 0, [{state_key, locator}],
               max_bytes: 1_000
             )
  end

  defp record(id, version) do
    %{
      id: id,
      version: version,
      type: "job",
      state: "waiting",
      partition_key: "tenant-a",
      created_at_ms: 100,
      updated_at_ms: 100 + version,
      next_run_at_ms: 1_000,
      priority: 0,
      attempts: 0,
      payload_ref: nil,
      result_ref: nil,
      error_ref: nil
    }
  end

  defp locator(record, file_id, offset, value_size, overrides \\ []) do
    Locator.new!(
      Keyword.merge(
        [
          flow_id: record.id,
          kind: :state,
          version: record.version,
          raft_index: record.version,
          file_id: file_id,
          offset: offset,
          value_size: value_size,
          checksum: :crypto.hash(:sha256, Ferricstore.Flow.encode_record(record))
        ],
        overrides
      )
    )
  end

  defp durable_apply_locator(root, state_key, record, index, value_size, overrides \\ []) do
    {ordinal, offset, frame_size} = durable_apply_location!(root, index, [state_key])

    locator(
      record,
      {:waraft_apply_projection, index},
      offset,
      value_size,
      Keyword.merge(
        [raft_index: index, segment_generation: ordinal, frame_size: frame_size],
        overrides
      )
    )
  end

  defp durable_apply_location!(root, index, keys) do
    assert {:ok, _removed} =
             WARaftSegmentReader.ensure_apply_projection_entries_durable(
               root,
               0,
               Enum.map(keys, &{index, &1})
             )

    assert {:ok, location} =
             WARaftSegmentReader.physical_location(
               %{data_dir: root},
               0,
               {:waraft_apply_projection, index}
             )

    location
  end

  defp scope_metadata(value), do: %{0x8001 => {1, :uint64, :isolation_scope, value}}

  defp forward_traces(parent, tag) do
    receive do
      :stop ->
        :ok

      message ->
        send(parent, {tag, message})
        forward_traces(parent, tag)
    end
  end

  defp traced_calls(tag, acc \\ []) do
    receive do
      {^tag, {:trace, _pid, :call, {module, function, args}}} ->
        traced_calls(tag, [{module, function, args} | acc])

      {^tag, _other} ->
        traced_calls(tag, acc)
    after
      10 -> Enum.reverse(acc)
    end
  end

  defp tmp_root(suffix) do
    root =
      Path.join(
        System.tmp_dir!(),
        "record_hydrator_#{suffix}_#{System.unique_integer([:positive])}"
      )

    on_exit(fn ->
      WARaftSegmentReader.clear_apply_projection_cache(root, 0)
      File.rm_rf!(root)
    end)

    root
  end
end
