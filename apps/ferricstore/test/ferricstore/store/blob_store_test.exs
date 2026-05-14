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

  test "put appends a blob segment record and returns a small ref", %{root: root} do
    payload = :binary.copy("abc", 1024)

    assert {:ok, ref} = BlobStore.put(root, 0, payload)
    encoded = BlobRef.encode!(ref)

    assert byte_size(encoded) == BlobRef.encoded_size()
    assert %{version: 2, segment_id: 0, offset: offset} = ref
    assert offset > 0
    assert {:ok, {segment_path, ^offset, size}} = BlobStore.file_ref(root, 0, ref)
    assert size == byte_size(payload)
    assert Path.basename(segment_path) == "00000000000000000000.bloblog"

    assert [] ==
             Path.wildcard(Path.join(Ferricstore.DataDir.blob_shard_path(root, 0), "**/*.blob"))

    assert {:ok, ^payload} = BlobStore.get(root, 0, ref)
    assert [] == tmp_files(root, 0)
  end

  test "put_many appends a batch to the same segment with one file fsync", %{root: root} do
    parent = self()

    Process.put(:ferricstore_blob_store_fsync_file_hook, fn path ->
      send(parent, {:blob_fsync_file, path})
      :ok
    end)

    on_exit(fn -> Process.delete(:ferricstore_blob_store_fsync_file_hook) end)

    payloads = [
      :binary.copy("a", 512),
      :binary.copy("b", 768),
      :binary.copy("c", 1024)
    ]

    assert {:ok, refs} = BlobStore.put_many(root, 0, payloads)
    assert length(refs) == length(payloads)

    file_refs = Enum.map(refs, &BlobStore.file_ref(root, 0, &1))
    assert Enum.all?(file_refs, &match?({:ok, {_path, _offset, _size}}, &1))

    paths =
      Enum.map(file_refs, fn {:ok, {path, _offset, _size}} -> path end)
      |> Enum.uniq()

    assert [segment_path] = paths
    assert_received {:blob_fsync_file, ^segment_path}
    refute_received {:blob_fsync_file, _}

    assert Enum.zip(payloads, refs)
           |> Enum.all?(fn {payload, ref} ->
             BlobStore.get(root, 0, ref) == {:ok, payload}
           end)
  end

  test "put_many writes a batch with one file write", %{root: root} do
    parent = self()

    Process.put(:ferricstore_blob_store_write_hook, fn io, iodata ->
      send(parent, {:blob_write, IO.iodata_length(iodata)})
      :file.write(io, iodata)
    end)

    on_exit(fn -> Process.delete(:ferricstore_blob_store_write_hook) end)

    payloads = [
      :binary.copy("a", 512),
      :binary.copy("b", 768),
      :binary.copy("c", 1024)
    ]

    assert {:ok, refs} = BlobStore.put_many(root, 0, payloads)

    expected_bytes = Enum.sum(Enum.map(payloads, &byte_size/1)) + length(payloads) * 48
    assert_received {:blob_write, ^expected_bytes}
    refute_received {:blob_write, _}

    assert Enum.zip(payloads, refs)
           |> Enum.all?(fn {payload, ref} ->
             BlobStore.get(root, 0, ref) == {:ok, payload}
           end)
  end

  test "put_many stores duplicate payloads once within the same append batch", %{root: root} do
    payload = :binary.copy("same", 512)

    assert {:ok, [first_ref, second_ref]} = BlobStore.put_many(root, 0, [payload, payload])
    assert first_ref == second_ref

    assert {:ok, {segment_path, offset, size}} = BlobStore.file_ref(root, 0, first_ref)
    assert size == byte_size(payload)
    assert offset == 48
    assert File.stat!(segment_path).size == 48 + byte_size(payload)
    assert {:ok, ^payload} = BlobStore.get(root, 0, second_ref)
  end

  test "blob ETS caches survive short-lived writer processes", %{root: root} do
    parent = self()
    ref = make_ref()

    pid =
      spawn(fn ->
        assert {:ok, _blob_ref} = BlobStore.put(root, 0, :binary.copy("x", 512))
        send(parent, {ref, :writer_done})
      end)

    monitor = Process.monitor(pid)
    assert_receive {^ref, :writer_done}
    assert_receive {:DOWN, ^monitor, :process, ^pid, _reason}

    refute :ets.whereis(:ferricstore_blob_store_dirs) == :undefined
    refute :ets.whereis(:ferricstore_blob_store_segments) == :undefined
    refute :ets.whereis(:ferricstore_blob_store_locks) == :undefined

    assert {:ok, _blob_ref} = BlobStore.put(root, 0, :binary.copy("y", 512))
  end

  test "verify_many validates duplicate refs once", %{root: root} do
    payload = :binary.copy("same-ref", 512)
    assert {:ok, ref} = BlobStore.put(root, 0, payload)
    parent = self()

    verifier = fn ^root, 0, ^ref ->
      send(parent, {:verified, ref})
      :ok
    end

    assert :ok = BlobStore.verify_many(root, 0, [ref, ref], verifier)
    assert_received {:verified, ^ref}
    refute_received {:verified, _}
  end

  test "verify_many opens each append segment once for unique refs", %{root: root} do
    payloads = [
      :binary.copy("a", 512),
      :binary.copy("b", 768),
      :binary.copy("c", 1024)
    ]

    assert {:ok, refs} = BlobStore.put_many(root, 0, payloads)
    assert {:ok, {segment_path, _offset, _size}} = BlobStore.file_ref(root, 0, hd(refs))
    parent = self()

    Process.put(:ferricstore_blob_store_open_read_hook, fn path, modes ->
      send(parent, {:blob_open_read, path})
      File.open(path, modes)
    end)

    on_exit(fn -> Process.delete(:ferricstore_blob_store_open_read_hook) end)

    assert :ok = BlobStore.verify_many(root, 0, refs)
    assert_received {:blob_open_read, ^segment_path}
    refute_received {:blob_open_read, _}
  end

  test "verify_many rejects corrupt refs inside grouped segment verification", %{root: root} do
    assert {:ok, [good_ref, bad_ref]} =
             BlobStore.put_many(root, 0, [:binary.copy("g", 128), :binary.copy("b", 128)])

    overwrite_segment_payload!(root, 0, bad_ref, :binary.copy("x", 128))

    assert {:error, :checksum_mismatch} = BlobStore.verify_many(root, 0, [good_ref, bad_ref])
  end

  test "get_many opens each append segment once for unique refs", %{root: root} do
    payloads = [
      :binary.copy("a", 512),
      :binary.copy("b", 768),
      :binary.copy("c", 1024)
    ]

    assert {:ok, refs} = BlobStore.put_many(root, 0, payloads)
    assert {:ok, {segment_path, _offset, _size}} = BlobStore.file_ref(root, 0, hd(refs))
    parent = self()

    Process.put(:ferricstore_blob_store_open_read_hook, fn path, modes ->
      send(parent, {:blob_open_read, path})
      File.open(path, modes)
    end)

    on_exit(fn -> Process.delete(:ferricstore_blob_store_open_read_hook) end)

    assert Enum.map(payloads, &{:ok, &1}) == BlobStore.get_many(root, 0, refs)
    assert_received {:blob_open_read, ^segment_path}
    refute_received {:blob_open_read, _}
  end

  test "file_refs_many opens each append segment once for unique refs", %{root: root} do
    payloads = [
      :binary.copy("a", 512),
      :binary.copy("b", 768),
      :binary.copy("c", 1024)
    ]

    assert {:ok, refs} = BlobStore.put_many(root, 0, payloads)
    [first_ref | _] = refs
    segment_path = BlobRef.path(root, 0, first_ref)
    parent = self()

    Process.put(:ferricstore_blob_store_open_read_hook, fn path, modes ->
      send(parent, {:blob_open_read, path})
      File.open(path, modes)
    end)

    on_exit(fn -> Process.delete(:ferricstore_blob_store_open_read_hook) end)

    assert Enum.map(refs, &{:ok, {segment_path, &1.offset, &1.size}}) ==
             BlobStore.file_refs_many(root, 0, refs)

    assert_received {:blob_open_read, ^segment_path}
    refute_received {:blob_open_read, _}
  end

  test "file_ref uses the shared blob read-open path for segment refs", %{root: root} do
    assert {:ok, [ref]} = BlobStore.put_many(root, 0, [:binary.copy("f", 512)])
    segment_path = BlobRef.path(root, 0, ref)
    parent = self()

    Process.put(:ferricstore_blob_store_open_read_hook, fn path, modes ->
      send(parent, {:blob_open_read, path})
      File.open(path, modes)
    end)

    on_exit(fn -> Process.delete(:ferricstore_blob_store_open_read_hook) end)

    assert {:ok, {^segment_path, _offset, 512}} = BlobStore.file_ref(root, 0, ref)
    assert_received {:blob_open_read, ^segment_path}
    refute_received {:blob_open_read, _}
  end

  test "blob APIs reject malformed refs without raising", %{root: root} do
    invalid_segment_ref = %BlobRef{
      version: 2,
      checksum: :binary.copy(<<0>>, 32),
      size: 1,
      segment_id: nil,
      offset: 48
    }

    invalid_legacy_ref = %BlobRef{
      version: 1,
      checksum: "short",
      size: 1
    }

    for ref <- [invalid_segment_ref, invalid_legacy_ref] do
      assert {:error, :invalid_blob_ref} = BlobStore.get(root, 0, ref)
      assert {:error, :invalid_blob_ref} = BlobStore.verify(root, 0, ref)
      assert {:error, :invalid_blob_ref} = BlobStore.file_ref(root, 0, ref)
      assert {:error, :invalid_blob_ref} = BlobStore.get_range(root, 0, ref, 0, 1)
      assert [{:error, :invalid_blob_ref}] = BlobStore.get_many(root, 0, [ref])
      assert [{:error, :invalid_blob_ref}] = BlobStore.file_refs_many(root, 0, [ref])
      assert {:error, :invalid_blob_ref} = BlobStore.verify_many(root, 0, [ref])
    end
  end

  test "file_refs_many isolates corrupt segment headers", %{root: root} do
    first_payload = :binary.copy("a", 128)
    second_payload = :binary.copy("b", 128)
    third_payload = :binary.copy("c", 128)

    assert {:ok, [first_ref, second_ref, third_ref]} =
             BlobStore.put_many(root, 0, [first_payload, second_payload, third_payload])

    overwrite_segment_header!(root, 0, second_ref, :binary.copy(<<0>>, 48))

    assert [
             {:ok, {_first_path, _first_offset, 128}},
             {:error, :segment_header_mismatch},
             {:ok, {_third_path, _third_offset, 128}}
           ] = BlobStore.file_refs_many(root, 0, [first_ref, second_ref, third_ref])
  end

  test "get_many isolates corrupt refs inside grouped segment reads", %{root: root} do
    first_payload = :binary.copy("a", 128)
    second_payload = :binary.copy("b", 128)
    third_payload = :binary.copy("c", 128)

    assert {:ok, [first_ref, second_ref, third_ref]} =
             BlobStore.put_many(root, 0, [first_payload, second_payload, third_payload])

    overwrite_segment_payload!(root, 0, second_ref, :binary.copy("x", 128))

    assert [
             {:ok, ^first_payload},
             {:error, :checksum_mismatch},
             {:ok, ^third_payload}
           ] = BlobStore.get_many(root, 0, [first_ref, second_ref, third_ref])
  end

  test "put_many rejects invalid payload without opening the segment", %{root: root} do
    parent = self()

    Process.put(:ferricstore_blob_store_write_hook, fn io, iodata ->
      send(parent, :unexpected_blob_write)
      :file.write(io, iodata)
    end)

    on_exit(fn -> Process.delete(:ferricstore_blob_store_write_hook) end)

    assert {:error, :invalid_blob_payload} = BlobStore.put_many(root, 0, ["valid", :invalid])
    refute_received :unexpected_blob_write
  end

  test "single-entry put_many rejects invalid payload without opening the segment", %{root: root} do
    parent = self()

    Process.put(:ferricstore_blob_store_write_hook, fn io, iodata ->
      send(parent, :unexpected_blob_write)
      :file.write(io, iodata)
    end)

    on_exit(fn -> Process.delete(:ferricstore_blob_store_write_hook) end)

    assert {:error, :invalid_blob_payload} = BlobStore.put_many(root, 0, [:invalid])
    refute_received :unexpected_blob_write
  end

  test "put rolls back unacknowledged segment bytes when fsync fails", %{root: root} do
    first_payload = :binary.copy("a", 512)
    failed_payload = :binary.copy("b", 768)
    next_payload = :binary.copy("c", 256)

    assert {:ok, first_ref} = BlobStore.put(root, 0, first_payload)

    assert {:ok, {segment_path, _first_offset, _first_size}} =
             BlobStore.file_ref(root, 0, first_ref)

    before_size = File.stat!(segment_path).size

    Process.put(:ferricstore_blob_store_fsync_file_hook, fn ^segment_path ->
      {:error, :test_fsync_failed}
    end)

    try do
      assert {:error, :test_fsync_failed} = BlobStore.put(root, 0, failed_payload)
      assert File.stat!(segment_path).size == before_size
    after
      Process.delete(:ferricstore_blob_store_fsync_file_hook)
    end

    assert {:ok, next_ref} = BlobStore.put(root, 0, next_payload)
    assert next_ref.offset == before_size + 48
    assert {:ok, ^next_payload} = BlobStore.get(root, 0, next_ref)
  end

  test "file_ref rejects a segment ref whose header does not match", %{root: root} do
    payload_a = :binary.copy("a", 512)
    payload_b = :binary.copy("b", 512)

    assert {:ok, [ref_a, ref_b]} = BlobStore.put_many(root, 0, [payload_a, payload_b])

    wrong_ref = %{ref_a | offset: ref_b.offset}

    assert {:error, :segment_header_mismatch} = BlobStore.file_ref(root, 0, wrong_ref)
  end

  test "get_range reads only the requested segment blob slice", %{root: root} do
    payload = :binary.copy("a", 2048) <> "target-slice" <> :binary.copy("z", 2048)

    assert {:ok, ref} = BlobStore.put(root, 0, payload)
    assert {:ok, "target"} = BlobStore.get_range(root, 0, ref, 2048, 6)
  end

  test "get_range rejects an oversized legacy blob file", %{root: root} do
    payload = "legacy-payload"
    ref = BlobRef.from_payload(payload)
    write_legacy_blob!(root, 0, ref, payload <> "-trailing-corruption")

    assert {:error, :size_mismatch} = BlobStore.get_range(root, 0, ref, 0, 6)
  end

  test "get_range rejects a segment ref whose header does not match", %{root: root} do
    payload_a = :binary.copy("a", 512)
    payload_b = :binary.copy("b", 512)

    assert {:ok, [ref_a, ref_b]} = BlobStore.put_many(root, 0, [payload_a, payload_b])

    wrong_ref = %{ref_a | offset: ref_b.offset}

    assert {:error, :segment_header_mismatch} = BlobStore.get_range(root, 0, wrong_ref, 0, 1)
  end

  test "same-shard blob appends serialize while preserving offsets", %{root: root} do
    parent = self()

    first =
      Task.async(fn ->
        Process.put(:ferricstore_blob_store_write_hook, fn io, iodata ->
          send(parent, :first_blob_write_started)

          receive do
            :release_first_blob_write -> :file.write(io, iodata)
          after
            5_000 -> {:error, :test_timeout}
          end
        end)

        BlobStore.put(root, 0, :binary.copy("a", 128))
      end)

    assert_receive :first_blob_write_started, 1_000

    second =
      Task.async(fn ->
        Process.put(:ferricstore_blob_store_write_hook, fn io, iodata ->
          send(parent, :second_blob_write_started)
          :file.write(io, iodata)
        end)

        BlobStore.put(root, 0, :binary.copy("b", 128))
      end)

    refute_receive :second_blob_write_started, 100
    send(first.pid, :release_first_blob_write)

    assert {:ok, first_ref} = Task.await(first, 5_000)
    assert_receive :second_blob_write_started, 1_000
    assert {:ok, second_ref} = Task.await(second, 5_000)

    assert first_ref.segment_id == second_ref.segment_id
    assert first_ref.offset != second_ref.offset
    assert {:ok, :binary.copy("a", 128)} == BlobStore.get(root, 0, first_ref)
    assert {:ok, :binary.copy("b", 128)} == BlobStore.get(root, 0, second_ref)
  end

  test "same-shard blob latch recovers when the holder process dies", %{root: root} do
    parent = self()

    blocked =
      Task.async(fn ->
        Process.put(:ferricstore_blob_store_write_hook, fn _io, _iodata ->
          send(parent, :blocked_blob_write_started)

          receive do
            :never -> :ok
          end
        end)

        BlobStore.put(root, 0, :binary.copy("x", 128))
      end)

    assert_receive :blocked_blob_write_started, 1_000
    Task.shutdown(blocked, :brutal_kill)

    assert {:ok, ref} = BlobStore.put(root, 0, :binary.copy("y", 128))
    assert {:ok, :binary.copy("y", 128)} == BlobStore.get(root, 0, ref)
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

  test "recover_shard truncates a partial segment tail and preserves prior blobs", %{root: root} do
    payload = :binary.copy("safe", 256)

    assert {:ok, ref} = BlobStore.put(root, 0, payload)
    assert {:ok, {segment_path, _offset, _size}} = BlobStore.file_ref(root, 0, ref)

    File.write!(segment_path, "partial-tail", [:append, :binary])
    dirty_size = File.stat!(segment_path).size

    assert {:ok, %{truncated_segments: 1, truncated_bytes: 12}} =
             BlobStore.recover_shard(root, 0)

    assert File.stat!(segment_path).size == dirty_size - 12
    assert {:ok, ^payload} = BlobStore.get(root, 0, ref)
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

  test "recover_shard refuses to truncate corrupt non-latest segments", %{root: root} do
    with_blob_segment_max_bytes(600)

    first_payload = :binary.copy("a", 400)
    second_payload = :binary.copy("b", 400)

    assert {:ok, first_ref} = BlobStore.put(root, 0, first_payload)
    assert {:ok, second_ref} = BlobStore.put(root, 0, second_payload)
    assert first_ref.segment_id == 0
    assert second_ref.segment_id == 1

    assert {:ok, {first_path, _first_offset, _first_size}} =
             BlobStore.file_ref(root, 0, first_ref)

    first_size = File.stat!(first_path).size
    overwrite_segment_payload!(root, 0, first_ref, :binary.copy("x", 400))

    assert {:error, {:corrupt_immutable_blob_segment, ^first_path}} =
             BlobStore.recover_shard(root, 0)

    assert File.stat!(first_path).size == first_size
  end

  test "put appends duplicate payloads without creating per-value files", %{root: root} do
    payload = "dedupe-me"

    assert {:ok, first_ref} = BlobStore.put(root, 0, payload)
    assert {:ok, second_ref} = BlobStore.put(root, 0, payload)

    assert first_ref.checksum == second_ref.checksum
    assert first_ref.offset != second_ref.offset
    assert {:ok, ^payload} = BlobStore.get(root, 0, first_ref)
    assert {:ok, ^payload} = BlobStore.get(root, 0, second_ref)
    assert count_regular_files(Ferricstore.DataDir.blob_shard_path(root, 0)) == 1
  end

  test "put fsyncs segment parent only when creating segment storage", %{root: root} do
    parent = self()
    first_payload = "segment-parent-a"
    second_payload = "segment-parent-b"

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
    refute_received {:blob_fsync_dir, ^shard_blob_dir}
    refute_received {:blob_fsync_dir, _}
  end

  test "put recreates segment directory if it disappears after being cached", %{root: root} do
    assert {:ok, first_ref} = BlobStore.put(root, 0, "first")
    segment_dir = Path.dirname(BlobRef.path(root, 0, first_ref))
    File.rm_rf!(segment_dir)

    assert {:ok, second_ref} = BlobStore.put(root, 0, "second")
    assert {:ok, "second"} = BlobStore.get(root, 0, second_ref)
    assert File.dir?(segment_dir)
  end

  test "legacy content-addressed refs remain readable", %{root: root} do
    payload = "complete-payload"
    ref = BlobRef.from_payload(payload)

    write_legacy_blob!(root, 0, ref, payload)

    assert {:ok, ^payload} = BlobStore.get(root, 0, ref)
    assert {:ok, {path, 0, 16}} = BlobStore.file_ref(root, 0, ref)
    assert path == BlobRef.path(root, 0, ref)
  end

  test "legacy content-addressed corrupt refs are rejected", %{root: root} do
    payload = "complete-payload"
    ref = BlobRef.from_payload(payload)

    write_legacy_blob!(root, 0, ref, :binary.copy("x", byte_size(payload)))

    assert {:error, :checksum_mismatch} = BlobStore.get(root, 0, ref)
    assert {:error, :checksum_mismatch} = BlobStore.verify(root, 0, ref)
  end

  test "get detects same-size corrupt blob bytes", %{root: root} do
    payload = "correct"

    assert {:ok, ref} = BlobStore.put(root, 0, payload)
    overwrite_segment_payload!(root, 0, ref, "corrupt")

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
    overwrite_segment_payload!(root, 0, ref, "corrupt")

    assert {:error, :checksum_mismatch} = BlobStore.get(root, 0, ref)

    assert_receive {:blob_error, [:ferricstore, :blob, :error], %{count: 1, bytes: 7},
                    %{operation: :get, shard_index: 0, reason: :checksum_mismatch, path: ^path}}
  end

  test "file_ref is stat-only while materialized get still verifies checksum", %{root: root} do
    payload = "correct"

    assert {:ok, ref} = BlobStore.put(root, 0, payload)
    path = BlobRef.path(root, 0, ref)
    overwrite_segment_payload!(root, 0, ref, "corrupt")

    assert {:ok, {^path, offset, 7}} = BlobStore.file_ref(root, 0, ref)
    assert offset == ref.offset
    assert {:error, :checksum_mismatch} = BlobStore.get(root, 0, ref)
  end

  test "verify hashes an existing blob without returning its payload", %{root: root} do
    payload = "correct"

    assert {:ok, ref} = BlobStore.put(root, 0, payload)
    assert :ok = BlobStore.verify(root, 0, ref)

    overwrite_segment_payload!(root, 0, ref, "corrupt")

    assert {:error, :checksum_mismatch} = BlobStore.verify(root, 0, ref)
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
    truncate_segment_payload!(root, 0, ref, 5)

    assert {:error, :size_mismatch} = BlobStore.get(root, 0, ref)
  end

  test "sweep_unreferenced deletes only blobs absent from the live ref set", %{root: root} do
    live_ref = BlobRef.from_payload("live-payload")
    dead_ref = BlobRef.from_payload("dead-payload")
    live_path = BlobRef.path(root, 0, live_ref)
    dead_path = BlobRef.path(root, 0, dead_ref)

    write_legacy_blob!(root, 0, live_ref, "live-payload")
    write_legacy_blob!(root, 0, dead_ref, "dead-payload")

    assert {:ok, %{deleted_files: 1, deleted_bytes: 12, kept_files: 1}} =
             BlobStore.sweep_unreferenced(root, 0, MapSet.new([live_ref]))

    assert File.exists?(live_path)
    refute File.exists?(dead_path)
  end

  test "sweep_unreferenced deletes an append segment when no live ref points to it", %{
    root: root
  } do
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

  test "sweep_unreferenced can reclaim a dead rotated segment while newer segment stays live", %{
    root: root
  } do
    with_blob_segment_max_bytes(600)

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

    assert {:ok, first_ref} = BlobStore.put(root, 0, :binary.copy("a", 400))
    assert {:ok, {first_path, _offset, _size}} = BlobStore.file_ref(root, 0, first_ref)

    assert {:ok, %{deleted_files: 1}} = BlobStore.sweep_unreferenced(root, 0, [])
    refute File.exists?(first_path)

    assert {:ok, second_ref} = BlobStore.put(root, 0, :binary.copy("b", 400))
    assert second_ref.segment_id == first_ref.segment_id + 1
    assert {:ok, {second_path, _offset, _size}} = BlobStore.file_ref(root, 0, second_ref)
    assert second_path != first_path
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

  defp with_blob_segment_max_bytes(bytes) do
    Process.put(:ferricstore_blob_store_segment_max_bytes, bytes)
    on_exit(fn -> Process.delete(:ferricstore_blob_store_segment_max_bytes) end)
  end

  defp count_regular_files(path) do
    path
    |> Path.join("**/*")
    |> Path.wildcard()
    |> Enum.count(&File.regular?/1)
  end

  defp write_legacy_blob!(root, shard_index, ref, payload) do
    path = BlobRef.path(root, shard_index, ref)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, payload)
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
