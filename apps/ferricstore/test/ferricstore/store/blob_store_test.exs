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

  test "put writes a content-addressed blob and returns a small ref", %{root: root} do
    payload = :binary.copy("abc", 1024)

    assert {:ok, ref} = BlobStore.put(root, 0, payload)
    encoded = BlobRef.encode!(ref)

    assert byte_size(encoded) == BlobRef.encoded_size()
    assert File.read!(BlobRef.path(root, 0, ref)) == payload
    assert {:ok, ^payload} = BlobStore.get(root, 0, ref)
    assert [] == tmp_files(root, 0)
  end

  test "put is idempotent for the same payload", %{root: root} do
    payload = "dedupe-me"

    assert {:ok, ref} = BlobStore.put(root, 0, payload)
    path = BlobRef.path(root, 0, ref)
    assert {:ok, %{mtime: first_mtime}} = File.stat(path)

    assert {:ok, ^ref} = BlobStore.put(root, 0, payload)
    assert {:ok, %{mtime: second_mtime}} = File.stat(path)

    assert first_mtime == second_mtime
    assert count_regular_files(Ferricstore.DataDir.blob_shard_path(root, 0)) == 1
  end

  test "put replaces an incomplete existing blob", %{root: root} do
    payload = "complete-payload"
    ref = BlobRef.from_payload(payload)
    path = BlobRef.path(root, 0, ref)

    File.mkdir_p!(Path.dirname(path))
    File.write!(path, "partial")

    assert {:ok, ^ref} = BlobStore.put(root, 0, payload)
    assert {:ok, ^payload} = BlobStore.get(root, 0, ref)
  end

  test "put replaces same-size corrupt existing blob", %{root: root} do
    payload = "complete-payload"
    ref = BlobRef.from_payload(payload)
    path = BlobRef.path(root, 0, ref)

    File.mkdir_p!(Path.dirname(path))
    File.write!(path, :binary.copy("x", byte_size(payload)))

    assert {:ok, ^ref} = BlobStore.put(root, 0, payload)
    assert File.read!(path) == payload
    assert {:ok, ^payload} = BlobStore.get(root, 0, ref)
  end

  test "get detects same-size corrupt blob bytes", %{root: root} do
    payload = "correct"

    assert {:ok, ref} = BlobStore.put(root, 0, payload)
    File.write!(BlobRef.path(root, 0, ref), "corrupt")

    assert {:error, :checksum_mismatch} = BlobStore.get(root, 0, ref)
  end

  test "get detects truncated blob bytes", %{root: root} do
    payload = "correct"

    assert {:ok, ref} = BlobStore.put(root, 0, payload)
    File.write!(BlobRef.path(root, 0, ref), "short")

    assert {:error, :size_mismatch} = BlobStore.get(root, 0, ref)
  end

  defp tmp_files(root, shard_index) do
    root
    |> Ferricstore.DataDir.blob_shard_path(shard_index)
    |> Path.join("**/*.tmp")
    |> Path.wildcard()
  end

  defp count_regular_files(path) do
    path
    |> Path.join("**/*")
    |> Path.wildcard()
    |> Enum.count(&File.regular?/1)
  end
end
