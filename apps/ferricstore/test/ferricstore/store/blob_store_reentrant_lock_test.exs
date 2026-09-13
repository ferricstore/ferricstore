defmodule Ferricstore.Store.BlobStoreReentrantLockTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.BlobStore

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "blob-reentrant-lock-#{System.pid()}-#{System.unique_integer([:positive])}"
      )

    Ferricstore.DataDir.ensure_layout!(root, 1)

    on_exit(fn ->
      Process.delete(:ferricstore_blob_store_write_hook)
      Process.delete(:blob_reentrant_protection_token)
      Process.delete(:blob_reentrant_nested_ref)
      File.rm_rf!(root)
    end)

    assert {:ok, sentinel} = BlobStore.put(root, 0, "sentinel")
    %{root: root, sentinel: sentinel}
  end

  test "reentrant GC recovers an exceptional append before a protected append", %{
    root: root,
    sentinel: sentinel
  } do
    live_refs = fn ->
      Process.put(:ferricstore_blob_store_write_hook, fn io, data ->
        bytes = IO.iodata_to_binary(data)
        :ok = :file.write(io, binary_part(bytes, 0, 16))
        :ok = :file.sync(io)
        exit(:interrupted_blob_append)
      end)

      try do
        BlobStore.put(root, 0, String.duplicate("x", 128))
      catch
        :exit, :interrupted_blob_append -> :ok
      after
        Process.delete(:ferricstore_blob_store_write_hook)
      end

      assert {:ok, nested_ref, protection} = BlobStore.put_protected(root, 0, "nested")
      Process.put(:blob_reentrant_nested_ref, nested_ref)
      Process.put(:blob_reentrant_protection_token, protection)
      {:ok, [sentinel, nested_ref]}
    end

    assert {:ok, %{deleted_files: 0}} =
             BlobStore.sweep_unreferenced_with_live_refs(root, 0, live_refs)

    assert {:ok, "sentinel"} = BlobStore.get(root, 0, sentinel)
    nested_ref = Process.get(:blob_reentrant_nested_ref)
    assert {:ok, "nested"} = BlobStore.get(root, 0, nested_ref)
    protection = Process.get(:blob_reentrant_protection_token)
    assert is_tuple(protection)
    assert :ok = BlobStore.unprotect(protection)
  end
end
