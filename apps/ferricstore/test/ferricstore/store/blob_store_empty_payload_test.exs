defmodule Ferricstore.Store.BlobStoreEmptyPayloadTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.{BlobRef, BlobStore}

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "blob-empty-payload-#{System.pid()}-#{System.unique_integer([:positive])}"
      )

    Ferricstore.DataDir.ensure_layout!(root, 1)

    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root}
  end

  test "reads an empty payload through scalar and batch APIs", %{root: root} do
    assert {:ok, ref} = BlobStore.put(root, 0, "")

    assert {:ok, ""} = BlobStore.get(root, 0, ref)
    assert [{:ok, ""}] = BlobStore.get_many(root, 0, [ref])
    assert :ok = BlobStore.verify(root, 0, ref)
    assert :ok = BlobStore.verify_many(root, 0, [ref])
    assert {:ok, %{truncated_bytes: 0}} = BlobStore.recover_shard(root, 0)
    assert {:ok, ""} = BlobStore.get(root, 0, ref)
  end

  test "mixed batches preserve empty values, duplicate refs, and result order", %{root: root} do
    assert {:ok, [full_ref, empty_ref]} = BlobStore.put_many(root, 0, ["payload", ""])

    assert [{:ok, ""}, {:ok, "payload"}, {:ok, ""}] =
             BlobStore.get_many(root, 0, [empty_ref, full_ref, empty_ref])
  end

  test "an empty ref still requires its segment to exist", %{root: root} do
    assert {:ok, ref} = BlobStore.put(root, 0, "")
    File.rm!(BlobRef.path(root, 0, ref))

    assert {:error, :enoent} = BlobStore.get(root, 0, ref)
    assert [{:error, :enoent}] = BlobStore.get_many(root, 0, [ref])
  end

  test "an empty ref still requires a complete matching header", %{root: root} do
    assert {:ok, ref} = BlobStore.put(root, 0, "")
    path = BlobRef.path(root, 0, ref)
    bytes = File.read!(path)
    File.write!(path, binary_part(bytes, 0, byte_size(bytes) - 1))

    assert {:error, _reason} = BlobStore.get(root, 0, ref)
    assert [{:error, _reason}] = BlobStore.get_many(root, 0, [ref])
  end

  test "matching empty ref and header checksums must still match the actual payload", %{
    root: root
  } do
    assert {:ok, ref} = BlobStore.put(root, 0, "")
    path = BlobRef.path(root, 0, ref)
    bad_checksum = :binary.copy(<<0>>, 32)
    <<header_prefix::binary-size(16), _checksum::binary-size(32)>> = File.read!(path)
    File.write!(path, header_prefix <> bad_checksum)
    forged_ref = %{ref | checksum: bad_checksum}

    assert {:error, :checksum_mismatch} = BlobStore.get(root, 0, forged_ref)
    assert [{:error, :checksum_mismatch}] = BlobStore.get_many(root, 0, [forged_ref])
  end
end
