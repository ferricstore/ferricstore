defmodule Ferricstore.Store.BlobValueTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.{BlobRef, BlobStore, BlobValue}

  setup do
    root =
      Path.join(System.tmp_dir!(), "ferricstore_blob_value_#{System.unique_integer([:positive])}")

    Ferricstore.DataDir.ensure_layout!(root, 1)
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root}
  end

  test "maybe_materialize_many loads duplicate encoded refs once" do
    parent = self()
    payload = :binary.copy("shared", 32)
    ref = BlobRef.from_segment(payload, 0, 48)
    encoded_ref = BlobRef.encode!(ref)

    loader = fn "root", 0, ^ref ->
      send(parent, :blob_loaded)
      {:ok, payload}
    end

    assert [
             {:ok, ^payload},
             {:ok, "inline"},
             {:ok, ^payload}
           ] =
             BlobValue.maybe_materialize_many(
               "root",
               0,
               1,
               [encoded_ref, "inline", encoded_ref],
               loader
             )

    assert_received :blob_loaded
    refute_received :blob_loaded
  end

  test "maybe_materialize_many fans duplicate load errors out consistently" do
    parent = self()
    payload = "missing"
    ref = BlobRef.from_segment(payload, 1, 48)
    encoded_ref = BlobRef.encode!(ref)

    loader = fn "root", 0, ^ref ->
      send(parent, :blob_loaded)
      {:error, :enoent}
    end

    assert [
             {:error, :enoent},
             {:error, :enoent}
           ] =
             BlobValue.maybe_materialize_many("root", 0, 1, [encoded_ref, encoded_ref], loader)

    assert_received :blob_loaded
    refute_received :blob_loaded
  end

  test "maybe_materialize_many default loader batches append-segment reads", %{root: root} do
    payloads = [
      :binary.copy("a", 512),
      :binary.copy("b", 768),
      :binary.copy("c", 1024)
    ]

    assert {:ok, refs} = BlobStore.put_many(root, 0, payloads)
    encoded_refs = Enum.map(refs, &BlobRef.encode!/1)
    assert {:ok, {segment_path, _offset, _size}} = BlobStore.file_ref(root, 0, hd(refs))
    parent = self()

    Process.put(:ferricstore_blob_store_open_read_hook, fn path, modes ->
      send(parent, {:blob_open_read, path})
      File.open(path, modes)
    end)

    on_exit(fn -> Process.delete(:ferricstore_blob_store_open_read_hook) end)

    assert [
             {:ok, Enum.at(payloads, 0)},
             {:ok, "inline"},
             {:ok, Enum.at(payloads, 1)},
             {:ok, Enum.at(payloads, 2)}
           ] ==
             BlobValue.maybe_materialize_many(
               root,
               0,
               1,
               [
                 Enum.at(encoded_refs, 0),
                 "inline",
                 Enum.at(encoded_refs, 1),
                 Enum.at(encoded_refs, 2)
               ]
             )

    assert_received {:blob_open_read, ^segment_path}
    refute_received {:blob_open_read, _}
  end
end
