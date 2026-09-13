defmodule Ferricstore.Store.BlobStoreTailRecoveryTest do
  use ExUnit.Case, async: false

  import Bitwise

  alias Ferricstore.Store.{BlobRef, BlobStore}

  @segment_header_bytes 48
  @tail_probe_bytes 1_048_576
  @tail_probe_read_bytes @tail_probe_bytes - @segment_header_bytes + 1
  @hash_candidate_bytes 524_288

  setup do
    root = new_root("blob-tail-recovery")

    on_exit(fn ->
      File.rm_rf!(root)
      Process.delete(:ferricstore_blob_store_segment_max_bytes)
      Process.delete(:ferricstore_blob_store_open_recovery_hook)
    end)

    %{root: root}
  end

  test "preserves a complete record buried behind a partial large body", %{root: root} do
    assert {:ok, prefix_ref} = BlobStore.put(root, 0, "prefix")
    path = BlobRef.path(root, 0, prefix_ref)
    prefix_bytes = File.read!(path)

    donor = new_root("blob-tail-donor")
    on_exit(fn -> File.rm_rf!(donor) end)
    partial_record = donor_record(donor, :binary.copy("p", 2_048))
    later_payload = "later-record"
    later_record = donor_record(donor, later_payload)

    partial_body_bytes = 64
    partial_bytes = binary_part(partial_record, 0, @segment_header_bytes + partial_body_bytes)
    partial_offset = byte_size(prefix_bytes)
    later_offset = byte_size(prefix_bytes) + byte_size(partial_bytes) + @segment_header_bytes

    later_ref =
      ref_at_offset(donor_record_ref(donor, later_payload), prefix_ref.segment_id, later_offset)

    File.write!(path, [prefix_bytes, partial_bytes, later_record])
    before = File.read!(path)

    assert {:ok, ^later_payload} = BlobStore.get(root, 0, later_ref)

    assert {:error, {:corrupt_blob_segment, ^path, ^partial_offset, :complete_record_found}} =
             BlobStore.recover_shard(root, 0)

    assert File.read!(path) == before
    assert {:ok, ^later_payload} = BlobStore.get(root, 0, later_ref)
  end

  test "checks multiple magic candidates before refusing a destructive repair", %{root: root} do
    assert {:ok, prefix_ref} = BlobStore.put(root, 0, "prefix")
    path = BlobRef.path(root, 0, prefix_ref)
    prefix_bytes = File.read!(path)

    donor = new_root("blob-tail-candidates")
    on_exit(fn -> File.rm_rf!(donor) end)
    partial_record = donor_record(donor, :binary.copy("p", 2_048))
    invalid_record = corrupt_record(donor_record(donor, "invalid-candidate"))
    later_payload = "valid-candidate"
    later_record = donor_record(donor, later_payload)

    partial_bytes = binary_part(partial_record, 0, @segment_header_bytes + 64)
    partial_offset = byte_size(prefix_bytes)

    later_offset =
      byte_size(prefix_bytes) + byte_size(partial_bytes) + byte_size(invalid_record) +
        @segment_header_bytes

    later_ref =
      ref_at_offset(donor_record_ref(donor, later_payload), prefix_ref.segment_id, later_offset)

    File.write!(path, [prefix_bytes, partial_bytes, invalid_record, later_record])
    before = File.read!(path)

    assert {:ok, ^later_payload} = BlobStore.get(root, 0, later_ref)

    assert {:error, {:corrupt_blob_segment, ^path, ^partial_offset, :complete_record_found}} =
             BlobStore.recover_shard(root, 0)

    assert File.read!(path) == before
  end

  test "repairs an ordinary partial body larger than the probe buffer", %{root: root} do
    assert {:ok, prefix_ref} = BlobStore.put(root, 0, "prefix")
    path = BlobRef.path(root, 0, prefix_ref)
    prefix_bytes = File.read!(path)

    donor = new_root("blob-tail-budget")
    on_exit(fn -> File.rm_rf!(donor) end)
    large_payload_size = @tail_probe_bytes * 2 + 128

    partial_header =
      donor_record(donor, :binary.copy("p", large_payload_size))
      |> binary_part(0, @segment_header_bytes)

    partial_body = :binary.copy("z", @tail_probe_bytes + 1)
    partial_bytes = partial_header <> partial_body
    File.write!(path, [prefix_bytes, partial_bytes])

    assert {:ok, %{truncated_segments: 1, truncated_bytes: truncated_bytes}} =
             BlobStore.recover_shard(root, 0)

    assert truncated_bytes == byte_size(partial_bytes)
    assert File.stat!(path).size == byte_size(prefix_bytes)
    assert {:ok, "prefix"} = BlobStore.get(root, 0, prefix_ref)
  end

  test "preserves a complete record beyond the probe buffer", %{root: root} do
    assert {:ok, prefix_ref} = BlobStore.put(root, 0, "prefix")
    path = BlobRef.path(root, 0, prefix_ref)
    prefix_bytes = File.read!(path)

    donor = new_root("blob-tail-beyond-buffer")
    on_exit(fn -> File.rm_rf!(donor) end)

    partial_header =
      donor_record(donor, :binary.copy("p", @tail_probe_bytes * 2 + 128))
      |> binary_part(0, @segment_header_bytes)

    partial_body = :binary.copy("z", @tail_probe_bytes + 128)
    later_payload = "later-beyond-buffer"
    later_record = donor_record(donor, later_payload)
    partial_bytes = partial_header <> partial_body
    later_offset = byte_size(prefix_bytes) + byte_size(partial_bytes) + @segment_header_bytes

    later_ref =
      ref_at_offset(donor_record_ref(donor, later_payload), prefix_ref.segment_id, later_offset)

    File.write!(path, [prefix_bytes, partial_bytes, later_record])
    before = File.read!(path)

    assert {:ok, ^later_payload} = BlobStore.get(root, 0, later_ref)

    assert {:error, {:corrupt_blob_segment, ^path, _offset, :complete_record_found}} =
             BlobStore.recover_shard(root, 0)

    assert File.read!(path) == before
    assert {:ok, ^later_payload} = BlobStore.get(root, 0, later_ref)
  end

  test "detects a magic and header split across probe windows", %{root: root} do
    assert {:ok, prefix_ref} = BlobStore.put(root, 0, "prefix")
    path = BlobRef.path(root, 0, prefix_ref)
    prefix_bytes = File.read!(path)

    donor = new_root("blob-tail-window-boundary")
    on_exit(fn -> File.rm_rf!(donor) end)

    partial_header =
      donor_record(donor, :binary.copy("p", @tail_probe_bytes * 2 + 128))
      |> binary_part(0, @segment_header_bytes)

    body_before_later_header =
      :binary.copy("z", @tail_probe_read_bytes - @segment_header_bytes - 4)

    later_payload = "split-window-record"
    later_record = donor_record(donor, later_payload)
    partial_bytes = partial_header <> body_before_later_header
    later_header_offset = byte_size(prefix_bytes) + byte_size(partial_bytes)

    assert rem(later_header_offset - byte_size(prefix_bytes), @tail_probe_read_bytes) ==
             @tail_probe_read_bytes - 4

    later_offset = later_header_offset + @segment_header_bytes

    later_ref =
      ref_at_offset(donor_record_ref(donor, later_payload), prefix_ref.segment_id, later_offset)

    File.write!(path, [prefix_bytes, partial_bytes, later_record])
    before = File.read!(path)

    assert {:ok, ^later_payload} = BlobStore.get(root, 0, later_ref)

    assert {:error, {:corrupt_blob_segment, ^path, _offset, :complete_record_found}} =
             BlobStore.recover_shard(root, 0)

    assert File.read!(path) == before
    assert {:ok, ^later_payload} = BlobStore.get(root, 0, later_ref)
  end

  test "fails closed when the bounded probe hash budget is exhausted", %{root: root} do
    assert {:ok, prefix_ref} = BlobStore.put(root, 0, "prefix")
    path = BlobRef.path(root, 0, prefix_ref)
    prefix_bytes = File.read!(path)

    donor = new_root("blob-tail-hash-budget")
    on_exit(fn -> File.rm_rf!(donor) end)

    initial_header =
      donor_record(donor, :binary.copy("i", @tail_probe_bytes + 1))
      |> binary_part(0, @segment_header_bytes)

    candidate_header =
      donor_record(donor, :binary.copy("p", @hash_candidate_bytes))
      |> binary_part(0, @segment_header_bytes)

    tail = [
      initial_header,
      List.duplicate(candidate_header, 65),
      :binary.copy("q", @hash_candidate_bytes)
    ]

    assert IO.iodata_length(tail) < @tail_probe_bytes
    File.write!(path, [prefix_bytes, tail])
    before = File.read!(path)

    assert {:error, {:corrupt_blob_segment, ^path, _offset, :hash_budget_exceeded}} =
             BlobStore.recover_shard(root, 0)

    assert File.read!(path) == before
  end

  test "repairs an ordinary partial body when no later record exists", %{root: root} do
    assert {:ok, prefix_ref} = BlobStore.put(root, 0, "prefix")
    path = BlobRef.path(root, 0, prefix_ref)
    prefix_bytes = File.read!(path)

    donor = new_root("blob-tail-partial-body")
    on_exit(fn -> File.rm_rf!(donor) end)
    partial_record = donor_record(donor, :binary.copy("p", 256))
    partial_bytes = binary_part(partial_record, 0, @segment_header_bytes + 16)
    File.write!(path, [prefix_bytes, partial_bytes])

    assert {:ok, %{truncated_segments: 1, truncated_bytes: truncated_bytes}} =
             BlobStore.recover_shard(root, 0)

    assert truncated_bytes == byte_size(partial_bytes)
    assert File.stat!(path).size == byte_size(prefix_bytes)
    assert {:ok, "prefix"} = BlobStore.get(root, 0, prefix_ref)
  end

  test "repairs an ordinary partial header", %{root: root} do
    assert {:ok, prefix_ref} = BlobStore.put(root, 0, "prefix")
    path = BlobRef.path(root, 0, prefix_ref)
    prefix_bytes = File.read!(path)

    donor = new_root("blob-tail-partial-header")
    on_exit(fn -> File.rm_rf!(donor) end)
    partial_header = donor_record(donor, "header") |> binary_part(0, @segment_header_bytes - 1)
    File.write!(path, [prefix_bytes, partial_header])

    assert {:ok, %{truncated_segments: 1, truncated_bytes: truncated_bytes}} =
             BlobStore.recover_shard(root, 0)

    assert truncated_bytes == byte_size(partial_header)
    assert File.stat!(path).size == byte_size(prefix_bytes)
    assert {:ok, "prefix"} = BlobStore.get(root, 0, prefix_ref)
  end

  test "never truncates a torn tail in a sealed segment", %{root: root} do
    Process.put(:ferricstore_blob_store_segment_max_bytes, 200)
    assert {:ok, sealed_ref} = BlobStore.put(root, 0, :binary.copy("s", 128))
    assert {:ok, active_ref} = BlobStore.put(root, 0, :binary.copy("a", 128))

    sealed_path = BlobRef.path(root, 0, sealed_ref)
    sealed_bytes = File.read!(sealed_path)
    donor = new_root("blob-tail-sealed")
    on_exit(fn -> File.rm_rf!(donor) end)
    partial_record = donor_record(donor, :binary.copy("p", 256))
    partial_bytes = binary_part(partial_record, 0, @segment_header_bytes + 16)
    File.write!(sealed_path, [sealed_bytes, partial_bytes])
    before = File.read!(sealed_path)

    assert {:error, {:corrupt_immutable_blob_segment, ^sealed_path}} =
             BlobStore.recover_shard(root, 0)

    assert File.read!(sealed_path) == before
    sealed_payload = :binary.copy("s", 128)
    active_payload = :binary.copy("a", 128)
    assert {:ok, ^sealed_payload} = BlobStore.get(root, 0, sealed_ref)
    assert {:ok, ^active_payload} = BlobStore.get(root, 0, active_ref)
  end

  defp new_root(prefix) do
    root =
      Path.join(
        System.tmp_dir!(),
        "#{prefix}-#{System.pid()}-#{System.unique_integer([:positive])}"
      )

    Ferricstore.DataDir.ensure_layout!(root, 1)
    root
  end

  defp donor_record(donor, payload) do
    assert {:ok, ref} = BlobStore.put(donor, 0, payload)
    record_bytes(BlobRef.path(donor, 0, ref), ref)
  end

  defp donor_record_ref(donor, payload) do
    assert {:ok, ref} = BlobStore.put(donor, 0, payload)
    ref
  end

  defp record_bytes(path, %BlobRef{offset: offset, size: size}) do
    bytes = File.read!(path)
    binary_part(bytes, offset - @segment_header_bytes, @segment_header_bytes + size)
  end

  defp ref_at_offset(%BlobRef{} = ref, segment_id, offset),
    do: %{ref | segment_id: segment_id, offset: offset}

  defp corrupt_record(record) do
    <<header::binary-size(@segment_header_bytes), first, rest::binary>> = record
    <<header::binary, bxor(first, 1), rest::binary>>
  end
end
