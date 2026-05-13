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

  test "put fsyncs checksum-prefix parent only when creating that directory", %{root: root} do
    parent = self()
    {first_payload, second_payload} = same_prefix_payloads()

    Process.put(:ferricstore_blob_store_fsync_dir_hook, fn path ->
      send(parent, {:blob_fsync_dir, path})
      :ok
    end)

    on_exit(fn -> Process.delete(:ferricstore_blob_store_fsync_dir_hook) end)

    assert {:ok, first_ref} = BlobStore.put(root, 0, first_payload)
    first_dir = Path.dirname(BlobRef.path(root, 0, first_ref))
    shard_blob_dir = Ferricstore.DataDir.blob_shard_path(root, 0)

    assert_received {:blob_fsync_dir, ^shard_blob_dir}
    assert_received {:blob_fsync_dir, ^first_dir}
    refute_received {:blob_fsync_dir, _}

    assert {:ok, second_ref} = BlobStore.put(root, 0, second_payload)
    second_dir = Path.dirname(BlobRef.path(root, 0, second_ref))

    assert second_dir == first_dir
    assert_received {:blob_fsync_dir, ^second_dir}
    refute_received {:blob_fsync_dir, ^shard_blob_dir}
    refute_received {:blob_fsync_dir, _}
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

  test "get emits blob error telemetry when checksum verification fails", %{root: root} do
    parent = self()
    handler_id = {:blob_store_error_telemetry, self(), make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :blob, :error],
      fn event, measurements, metadata, _config ->
        send(parent, {:blob_error, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert {:ok, ref} = BlobStore.put(root, 0, "correct")
    path = BlobRef.path(root, 0, ref)
    File.write!(path, "corrupt")

    assert {:error, :checksum_mismatch} = BlobStore.get(root, 0, ref)

    assert_receive {:blob_error, [:ferricstore, :blob, :error], %{count: 1, bytes: 7},
                    %{operation: :get, shard_index: 0, reason: :checksum_mismatch, path: ^path}}
  end

  test "file_ref is stat-only while materialized get still verifies checksum", %{root: root} do
    payload = "correct"

    assert {:ok, ref} = BlobStore.put(root, 0, payload)
    path = BlobRef.path(root, 0, ref)
    File.write!(path, "corrupt")

    assert {:ok, {^path, 0, 7}} = BlobStore.file_ref(root, 0, ref)
    assert {:error, :checksum_mismatch} = BlobStore.get(root, 0, ref)
  end

  test "verify hashes an existing blob without returning its payload", %{root: root} do
    payload = "correct"

    assert {:ok, ref} = BlobStore.put(root, 0, payload)
    assert :ok = BlobStore.verify(root, 0, ref)

    File.write!(BlobRef.path(root, 0, ref), "corrupt")

    assert {:error, :checksum_mismatch} = BlobStore.verify(root, 0, ref)
  end

  test "file_ref emits blob error telemetry when the blob file is missing", %{root: root} do
    parent = self()
    handler_id = {:blob_store_file_ref_telemetry, self(), make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :blob, :error],
      fn event, measurements, metadata, _config ->
        send(parent, {:blob_error, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert {:ok, ref} = BlobStore.put(root, 0, "payload")
    path = BlobRef.path(root, 0, ref)
    File.rm!(path)

    assert {:error, :enoent} = BlobStore.file_ref(root, 0, ref)

    assert_receive {:blob_error, [:ferricstore, :blob, :error], %{count: 1, bytes: 7},
                    %{operation: :file_ref, shard_index: 0, reason: :enoent, path: ^path}}
  end

  test "get detects truncated blob bytes", %{root: root} do
    payload = "correct"

    assert {:ok, ref} = BlobStore.put(root, 0, payload)
    File.write!(BlobRef.path(root, 0, ref), "short")

    assert {:error, :size_mismatch} = BlobStore.get(root, 0, ref)
  end

  test "sweep_unreferenced deletes only blobs absent from the live ref set", %{root: root} do
    assert {:ok, live_ref} = BlobStore.put(root, 0, "live-payload")
    assert {:ok, dead_ref} = BlobStore.put(root, 0, "dead-payload")
    live_path = BlobRef.path(root, 0, live_ref)
    dead_path = BlobRef.path(root, 0, dead_ref)

    assert {:ok, %{deleted_files: 1, deleted_bytes: 12, kept_files: 1}} =
             BlobStore.sweep_unreferenced(root, 0, MapSet.new([live_ref]))

    assert File.exists?(live_path)
    refute File.exists?(dead_path)
  end

  test "sweep_unreferenced deletes stale atomic-write tmp files", %{root: root} do
    ref = BlobRef.from_payload("crashed-write")
    blob_path = BlobRef.path(root, 0, ref)
    tmp_path = Path.join(Path.dirname(blob_path), ".#{Path.basename(blob_path)}.123.tmp")

    File.mkdir_p!(Path.dirname(tmp_path))
    File.write!(tmp_path, "partial-crashed-write")
    File.touch!(tmp_path, {{2000, 1, 1}, {0, 0, 0}})

    assert [^tmp_path] = tmp_files(root, 0)

    assert {:ok, %{deleted_tmp_files: 1, deleted_tmp_bytes: 21}} =
             BlobStore.sweep_unreferenced(root, 0, [])

    assert [] == tmp_files(root, 0)
  end

  test "sweep_unreferenced preserves fresh atomic-write tmp files", %{root: root} do
    ref = BlobRef.from_payload("active-write")
    blob_path = BlobRef.path(root, 0, ref)
    tmp_path = Path.join(Path.dirname(blob_path), ".#{Path.basename(blob_path)}.456.tmp")

    File.mkdir_p!(Path.dirname(tmp_path))
    File.write!(tmp_path, "active-write-in-progress")

    assert {:ok, %{deleted_tmp_files: 0, deleted_tmp_bytes: 0}} =
             BlobStore.sweep_unreferenced(root, 0, [])

    assert [^tmp_path] = tmp_files(root, 0)
  end

  defp tmp_files(root, shard_index) do
    root
    |> Ferricstore.DataDir.blob_shard_path(shard_index)
    |> Path.join("**/*.tmp")
    |> Path.wildcard(match_dot: true)
  end

  defp count_regular_files(path) do
    path
    |> Path.join("**/*")
    |> Path.wildcard()
    |> Enum.count(&File.regular?/1)
  end

  defp same_prefix_payloads do
    Stream.iterate(0, &(&1 + 1))
    |> Enum.reduce_while(%{}, fn n, seen ->
      payload = "blob-prefix-payload-#{n}"
      ref = BlobRef.from_payload(payload)
      prefix = binary_part(Base.encode16(ref.checksum, case: :lower), 0, 2)

      case Map.fetch(seen, prefix) do
        {:ok, previous} -> {:halt, {previous, payload}}
        :error -> {:cont, Map.put(seen, prefix, payload)}
      end
    end)
  end
end
