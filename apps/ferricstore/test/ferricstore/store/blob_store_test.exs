Code.require_file(
  "blob_store_test/sections/put_appends_blob_segment_record_returns_small_ref.exs",
  __DIR__
)

defmodule Ferricstore.Store.BlobStoreTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.{BlobRef, BlobStore}

  setup do
    root =
      Path.join(System.tmp_dir!(), "ferricstore_blob_store_#{System.unique_integer([:positive])}")

    Ferricstore.DataDir.ensure_layout!(root, 1)
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root}
  end

  test "put rotates append segments when the active segment crosses the size cap", %{
    root: root
  } do
    with_blob_segment_max_bytes(600)

    assert {:ok, first_ref} = BlobStore.put(root, 0, :binary.copy("a", 400))
    assert {:ok, second_ref} = BlobStore.put(root, 0, :binary.copy("b", 400))

    assert first_ref.segment_id == 0
    assert second_ref.segment_id == 1
    assert {:ok, {first_path, _offset, _size}} = BlobStore.file_ref(root, 0, first_ref)
    assert {:ok, {second_path, _offset, _size}} = BlobStore.file_ref(root, 0, second_ref)
    assert first_path != second_path
  end

  test "recover_shard rejects symlinked append segments without mutating the target", %{
    root: root
  } do
    segment_dir = Path.join(Ferricstore.DataDir.blob_shard_path(root, 0), "segments")
    File.mkdir_p!(segment_dir)

    outside_path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_blob_symlink_target_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm(outside_path) end)

    File.write!(outside_path, "sentinel")
    segment_path = Path.join(segment_dir, "00000000000000000000.bloblog")
    assert :ok = File.ln_s(outside_path, segment_path)

    assert {:error, {:unsafe_blob_segment_path, ^segment_path, :symlink}} =
             BlobStore.recover_shard(root, 0)

    assert File.read!(outside_path) == "sentinel"
  end

  test "recover_shard never truncates a segment swapped after its safety check", %{root: root} do
    assert {:ok, ref} = BlobStore.put(root, 0, :binary.copy("payload", 32))
    segment_path = BlobRef.path(root, 0, ref)
    File.write!(segment_path, "torn", [:append])

    original_path = segment_path <> ".original"
    outside_path = segment_path <> ".outside"
    corrupt_bytes = File.read!(segment_path)
    File.write!(outside_path, corrupt_bytes)

    Process.put(:ferricstore_blob_store_open_recovery_hook, fn path, modes ->
      File.rename!(path, original_path)
      File.ln_s!(outside_path, path)
      File.open(path, modes)
    end)

    try do
      assert {:error, _reason} = BlobStore.recover_shard(root, 0)
    after
      Process.delete(:ferricstore_blob_store_open_recovery_hook)
    end

    assert File.read!(outside_path) == corrupt_bytes
  end

  test "reads reject a segment swapped after the path safety check", %{root: root} do
    payload = :binary.copy("race-safe", 64)
    assert {:ok, ref} = BlobStore.put(root, 0, payload)
    segment_path = BlobRef.path(root, 0, ref)
    original_path = segment_path <> ".original"
    replacement_path = segment_path <> ".replacement"
    File.cp!(segment_path, replacement_path)

    Process.put(:ferricstore_blob_store_open_read_hook, fn path, modes ->
      File.rename!(path, original_path)
      File.ln_s!(replacement_path, path)
      File.open(path, modes)
    end)

    on_exit(fn -> Process.delete(:ferricstore_blob_store_open_read_hook) end)

    assert {:error, {:unsafe_blob_file_identity, ^segment_path}} =
             BlobStore.get(root, 0, ref)

    assert File.read!(replacement_path) == File.read!(original_path)
  end

  test "put rejects a symlinked next-segment marker without touching its target", %{root: root} do
    segment_dir = Path.join(Ferricstore.DataDir.blob_shard_path(root, 0), "segments")
    File.mkdir_p!(segment_dir)

    outside_path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_blob_next_id_target_#{System.unique_integer([:positive])}"
      )

    marker_path = Path.join(segment_dir, "next_segment_id")
    on_exit(fn -> File.rm(outside_path) end)
    File.write!(outside_path, "999\n")
    assert :ok = File.ln_s(outside_path, marker_path)

    assert {:error, {:blob_segment_next_id_read_failed, ^marker_path, {:symlink, _message}}} =
             BlobStore.put(root, 0, "payload")

    assert File.read!(outside_path) == "999\n"

    refute File.exists?(Path.join(segment_dir, "00000000000000000999.bloblog"))
  end

  test "GC replaces a symlinked next-segment temp file without touching its target", %{
    root: root
  } do
    with_blob_segment_gc_grace_ms(0)
    assert {:ok, _ref} = BlobStore.put(root, 0, :binary.copy("blob", 128))

    segment_dir = Path.join(Ferricstore.DataDir.blob_shard_path(root, 0), "segments")
    marker_path = Path.join(segment_dir, "next_segment_id")
    tmp_path = marker_path <> ".tmp"

    outside_path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_blob_next_id_tmp_target_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm(outside_path) end)
    File.write!(outside_path, "sentinel")
    assert :ok = File.ln_s(outside_path, tmp_path)

    assert {:ok, %{deleted_files: 1}} = BlobStore.sweep_unreferenced(root, 0, [])

    assert File.read!(outside_path) == "sentinel"
    assert File.read!(marker_path) == "1\n"
    assert File.lstat!(marker_path).type == :regular
  end

  test "recover_shard invalidates cached append offsets after truncating a corrupt record", %{
    root: root
  } do
    first_payload = :binary.copy("a", 128)
    second_payload = :binary.copy("b", 128)
    third_payload = :binary.copy("c", 128)

    assert {:ok, first_ref} = BlobStore.put(root, 0, first_payload)
    assert {:ok, second_ref} = BlobStore.put(root, 0, second_payload)
    overwrite_segment_payload!(root, 0, second_ref, :binary.copy("x", 128))

    assert {:ok, %{truncated_segments: 1}} = BlobStore.recover_shard(root, 0)
    assert {:ok, ^first_payload} = BlobStore.get(root, 0, first_ref)
    assert {:error, _reason} = BlobStore.get(root, 0, second_ref)

    assert {:ok, third_ref} = BlobStore.put(root, 0, third_payload)
    assert third_ref.offset == second_ref.offset
    assert {:ok, ^third_payload} = BlobStore.get(root, 0, third_ref)
  end

  test "get and verify reject corrupt segment headers even when payload bytes match", %{
    root: root
  } do
    payload = "correct"

    assert {:ok, ref} = BlobStore.put(root, 0, payload)
    overwrite_segment_header!(root, 0, ref, :binary.copy(<<0>>, 48))

    assert {:error, :segment_header_mismatch} = BlobStore.get(root, 0, ref)
    assert {:error, :segment_header_mismatch} = BlobStore.verify(root, 0, ref)
  end

  test "sweep_unreferenced deletes an append segment when no live ref points to it", %{
    root: root
  } do
    with_blob_segment_gc_grace_ms(0)

    assert {:ok, refs} =
             BlobStore.put_many(root, 0, [
               :binary.copy("a", 256),
               :binary.copy("b", 512)
             ])

    assert [segment_path] =
             refs
             |> Enum.map(fn ref ->
               assert {:ok, {path, _offset, _size}} = BlobStore.file_ref(root, 0, ref)
               path
             end)
             |> Enum.uniq()

    segment_bytes = File.stat!(segment_path).size

    assert {:ok, %{deleted_files: 1, deleted_bytes: ^segment_bytes, kept_files: 0}} =
             BlobStore.sweep_unreferenced(root, 0, [])

    refute File.exists?(segment_path)
  end

  test "sweep_unreferenced preserves a fresh append segment even before keydir refs appear", %{
    root: root
  } do
    with_blob_segment_gc_grace_ms(60_000)

    assert {:ok, ref} = BlobStore.put(root, 0, :binary.copy("pending", 256))
    assert {:ok, {segment_path, _offset, _size}} = BlobStore.file_ref(root, 0, ref)
    segment_bytes = File.stat!(segment_path).size

    assert {:ok, %{deleted_files: 0, deleted_bytes: 0, kept_files: 1}} =
             BlobStore.sweep_unreferenced(root, 0, [])

    assert File.exists?(segment_path)
    assert File.stat!(segment_path).size == segment_bytes
    assert {:ok, :binary.copy("pending", 256)} == BlobStore.get(root, 0, ref)
  end

  test "sweep_unreferenced preserves an append segment while any ref in it is live", %{
    root: root
  } do
    live_payload = :binary.copy("l", 256)
    dead_payload = :binary.copy("d", 512)
    assert {:ok, [live_ref, dead_ref]} = BlobStore.put_many(root, 0, [live_payload, dead_payload])

    assert {:ok, {segment_path, _offset, _size}} = BlobStore.file_ref(root, 0, live_ref)
    assert {:ok, {^segment_path, _offset, _size}} = BlobStore.file_ref(root, 0, dead_ref)

    assert {:ok, %{deleted_files: 0, deleted_bytes: 0, kept_files: 1}} =
             BlobStore.sweep_unreferenced(root, 0, MapSet.new([live_ref]))

    assert File.exists?(segment_path)
    assert {:ok, ^live_payload} = BlobStore.get(root, 0, live_ref)
  end

  test "sweep_unreferenced does not let stale legacy protection rows pin dead segments", %{
    root: root
  } do
    with_blob_segment_gc_grace_ms(0)

    assert {:ok, ref} = BlobStore.put(root, 0, :binary.copy("stale-protected", 256))
    assert {:ok, {segment_path, _offset, _size}} = BlobStore.file_ref(root, 0, ref)
    relative_path = BlobRef.relative_path(ref)
    segment_bytes = File.stat!(segment_path).size

    BlobStore.init_tables()
    :ets.insert(:ferricstore_blob_store_protected_refs, {{root, 0, relative_path}, 1})

    assert {:ok, %{deleted_files: 1, deleted_bytes: ^segment_bytes, kept_files: 0}} =
             BlobStore.sweep_unreferenced(root, 0, [])

    refute File.exists?(segment_path)
    assert [] = :ets.lookup(:ferricstore_blob_store_protected_refs, {root, 0, relative_path})
  end

  test "sweep_unreferenced expires modern protection rows from uncertain submits", %{
    root: root
  } do
    with_blob_segment_gc_grace_ms(0)
    with_blob_protection_ttl_ms(0)

    assert {:ok, ref, token} =
             BlobStore.put_protected(root, 0, :binary.copy("modern-protected", 256))

    relative_path = BlobRef.relative_path(ref)

    assert {:blob_store_protection, ^root, 0, [^relative_path]} = token
    assert {:ok, {segment_path, _offset, _size}} = BlobStore.file_ref(root, 0, ref)
    segment_bytes = File.stat!(segment_path).size

    assert [{_, 1, deadline_ms}] =
             :ets.lookup(:ferricstore_blob_store_protected_refs, {root, 0, relative_path})

    assert is_integer(deadline_ms)

    assert {:ok, %{deleted_files: 1, deleted_bytes: ^segment_bytes, kept_files: 0}} =
             BlobStore.sweep_unreferenced(root, 0, [])

    refute File.exists?(segment_path)
    assert [] = :ets.lookup(:ferricstore_blob_store_protected_refs, {root, 0, relative_path})
  end

  test "hardened unknown-outcome protection is not expired by the wall-clock TTL", %{
    root: root
  } do
    with_blob_segment_gc_grace_ms(0)
    with_blob_protection_ttl_ms(0)

    assert {:ok, ref, token} =
             BlobStore.put_protected(root, 0, :binary.copy("unknown-outcome", 256))

    relative_path = BlobRef.relative_path(ref)
    assert {:ok, {segment_path, _offset, _size}} = BlobStore.file_ref(root, 0, ref)

    assert :ok = BlobStore.harden_protection(token)

    assert [{_, 1, :infinity}] =
             :ets.lookup(:ferricstore_blob_store_protected_refs, {root, 0, relative_path})

    assert {:ok, %{deleted_files: 0, deleted_bytes: 0, kept_files: 1}} =
             BlobStore.sweep_unreferenced(root, 0, [])

    assert File.exists?(segment_path)

    assert :ok = BlobStore.unprotect(token)

    assert {:ok, %{deleted_files: 1}} = BlobStore.sweep_unreferenced(root, 0, [])
    refute File.exists?(segment_path)
  end

  test "hardened protection pages return the oldest matching rows with an exact limit", %{
    root: root
  } do
    BlobStore.init_tables()
    table = :ferricstore_blob_store_hardened_protections
    now_ms = System.monotonic_time(:millisecond)
    [oldest_id, middle_id, newest_id, other_shard_id] = Enum.map(1..4, fn _ -> make_ref() end)

    rows = [
      {newest_id, root, 0, ["newest"], now_ms - 10, %{}},
      {oldest_id, root, 0, ["oldest"], now_ms - 30, %{}},
      {middle_id, root, 0, ["middle"], now_ms - 20, %{}},
      {other_shard_id, root, 1, ["other"], now_ms - 40, %{}}
    ]

    :ets.insert(table, rows)

    on_exit(fn -> Enum.each(rows, &:ets.delete(table, elem(&1, 0))) end)

    assert [^oldest_id, ^middle_id] = BlobStore.hardened_protection_ids(root, 0, 2)
    assert [] = BlobStore.hardened_protection_ids(root, 0, 0)
    assert %{count: 3, oldest_age_ms: shard_age_ms} = BlobStore.hardened_protection_stats(root, 0)
    assert shard_age_ms >= 30
    assert %{count: 4, oldest_age_ms: all_age_ms} = BlobStore.hardened_protection_stats(root)
    assert all_age_ms >= 40
  end

  test "storage stats do not traverse symlinked blob shard directories", %{root: root} do
    outside = root <> "-outside"
    outside_segments = Path.join(outside, "segments")
    File.mkdir_p!(outside_segments)
    outside_segment = Path.join(outside_segments, "00000000000000000000.bloblog")
    File.write!(outside_segment, :binary.copy("x", 1_024))

    blob_root = Path.join(root, "blob")
    File.mkdir_p!(blob_root)
    File.ln_s!(outside, Path.join(blob_root, "shard_99"))
    on_exit(fn -> File.rm_rf!(outside) end)

    assert {:ok, %{files: 0, bytes: 0, tmp_files: 0, tmp_bytes: 0}} =
             BlobStore.storage_stats(root)

    assert File.read!(outside_segment) == :binary.copy("x", 1_024)
  end

  test "sweep_unreferenced can reclaim a dead rotated segment while newer segment stays live", %{
    root: root
  } do
    with_blob_segment_max_bytes(600)
    with_blob_segment_gc_grace_ms(0)

    first_payload = :binary.copy("a", 400)
    second_payload = :binary.copy("b", 400)
    assert {:ok, first_ref} = BlobStore.put(root, 0, first_payload)
    assert {:ok, second_ref} = BlobStore.put(root, 0, second_payload)

    assert first_ref.segment_id == 0
    assert second_ref.segment_id == 1
    assert {:ok, {first_path, _offset, _size}} = BlobStore.file_ref(root, 0, first_ref)
    assert {:ok, {second_path, _offset, _size}} = BlobStore.file_ref(root, 0, second_ref)

    first_bytes = File.stat!(first_path).size

    assert {:ok, %{deleted_files: 1, deleted_bytes: ^first_bytes, kept_files: 1}} =
             BlobStore.sweep_unreferenced(root, 0, MapSet.new([second_ref]))

    refute File.exists?(first_path)
    assert File.exists?(second_path)
    assert {:ok, ^second_payload} = BlobStore.get(root, 0, second_ref)
  end

  test "sweep_unreferenced persists next segment id before deleting the last segment", %{
    root: root
  } do
    with_blob_segment_max_bytes(600)
    with_blob_segment_gc_grace_ms(0)

    assert {:ok, first_ref} = BlobStore.put(root, 0, :binary.copy("a", 400))
    assert {:ok, {first_path, _offset, _size}} = BlobStore.file_ref(root, 0, first_ref)

    assert {:ok, %{deleted_files: 1}} = BlobStore.sweep_unreferenced(root, 0, [])
    refute File.exists?(first_path)

    assert {:ok, second_ref} = BlobStore.put(root, 0, :binary.copy("b", 400))
    assert second_ref.segment_id == first_ref.segment_id + 1
    assert {:ok, {second_path, _offset, _size}} = BlobStore.file_ref(root, 0, second_ref)
    assert second_path != first_path
  end

  use Ferricstore.Store.BlobStoreTest.Sections.PutAppendsBlobSegmentRecordReturnsSmallRef

  defp tmp_files(root, shard_index) do
    root
    |> Ferricstore.DataDir.blob_shard_path(shard_index)
    |> Path.join("**/*.tmp")
    |> Path.wildcard(match_dot: true)
  end

  defp with_blob_segment_max_bytes(bytes) do
    Process.put(:ferricstore_blob_store_segment_max_bytes, bytes)
    on_exit(fn -> Process.delete(:ferricstore_blob_store_segment_max_bytes) end)
  end

  defp with_blob_segment_gc_grace_ms(ms) do
    Process.put(:ferricstore_blob_store_segment_gc_grace_ms, ms)
    on_exit(fn -> Process.delete(:ferricstore_blob_store_segment_gc_grace_ms) end)
  end

  defp with_blob_protection_ttl_ms(ms) do
    Process.put(:ferricstore_blob_store_protection_ttl_ms, ms)
    on_exit(fn -> Process.delete(:ferricstore_blob_store_protection_ttl_ms) end)
  end

  defp count_regular_files(path) do
    path
    |> Path.join("**/*")
    |> Path.wildcard()
    |> Enum.count(&File.regular?/1)
  end

  defp overwrite_segment_payload!(root, shard_index, ref, payload) do
    assert {:ok, {path, offset, size}} = BlobStore.file_ref(root, shard_index, ref)
    assert byte_size(payload) == size

    {:ok, io} = File.open(path, [:read, :write, :raw, :binary])

    try do
      assert :ok = :file.pwrite(io, offset, payload)
    after
      :file.close(io)
    end
  end

  defp overwrite_segment_header!(root, shard_index, ref, header) do
    assert {:ok, {path, offset, _size}} = BlobStore.file_ref(root, shard_index, ref)
    assert byte_size(header) == 48

    {:ok, io} = File.open(path, [:read, :write, :raw, :binary])

    try do
      assert :ok = :file.pwrite(io, offset - 48, header)
    after
      :file.close(io)
    end
  end

  defp truncate_segment_payload!(root, shard_index, ref, keep_bytes) do
    assert {:ok, {path, offset, size}} = BlobStore.file_ref(root, shard_index, ref)
    assert keep_bytes < size

    {:ok, io} = File.open(path, [:read, :write, :raw, :binary])

    try do
      assert {:ok, _} = :file.position(io, offset + keep_bytes)
      assert :ok = :file.truncate(io)
    after
      :file.close(io)
    end
  end
end
