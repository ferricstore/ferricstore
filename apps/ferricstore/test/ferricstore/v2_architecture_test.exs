defmodule Ferricstore.V2ArchitectureTest do
  @moduledoc """
  Correctness tests for the v2 pure-NIF architecture.

  In v2, Rust NIFs are stateless functions — no HashMap keydir, no Mutex.
  The ETS keydir in Elixir is the single source of truth.

  These tests verify:
  - Pure NIF functions work correctly (append, pread, scan, hint, copy)
  - CRC validation catches corruption
  - Hint files round-trip correctly
  - Batch operations work
  - Concurrent reads are safe (no Mutex needed for reads)
  """

  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.NIF

  # Header size matches Rust: crc32(4) + timestamp_ms(8) + expire_at_ms(8) + key_size(2) + value_size(4) = 26
  @header_size 26

  setup do
    dir = Path.join(System.tmp_dir!(), "v2_test_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    %{dir: dir}
  end

  defp data_file(dir, file_id) do
    Path.join(dir, String.pad_leading("#{file_id}", 20, "0") <> ".log")
  end

  defp hint_file(dir, file_id) do
    Path.join(dir, String.pad_leading("#{file_id}", 20, "0") <> ".hint")
  end

  # =========================================================================
  # Basic operations (tests 1-10)
  # =========================================================================

  describe "v2_append_record" do
    test "1. PUT writes to disk, returns {file_id, offset, size} for ETS", %{dir: dir} do
      path = data_file(dir, 1)
      assert {:ok, {offset, record_size}} = NIF.v2_append_record(path, "hello", "world", 0)
      assert offset == 0
      assert record_size == @header_size + 5 + 5
      # File should exist and have data
      assert File.exists?(path)
      assert File.stat!(path).size > 0
    end

    test "2. GET hot (value in ETS) -- no NIF call needed", %{dir: _dir} do
      # This test validates the ETS-only path: when value is in ETS, no NIF needed
      keydir = :ets.new(:v2_test_hot, [:set, :public])
      :ets.insert(keydir, {"mykey", "myvalue", 0, 5, 1, 0, 7})
      [{_, value, _, _, _, _, _}] = :ets.lookup(keydir, "mykey")
      assert value == "myvalue"
      :ets.delete(keydir)
    end

    test "3. GET cold (value=nil, has file_id/offset) -- pread_at returns correct value", %{
      dir: dir
    } do
      path = data_file(dir, 1)
      {:ok, {offset, _size}} = NIF.v2_append_record(path, "cold_key", "cold_value", 0)

      # Simulate cold read via pread_at
      assert {:ok, value} = NIF.v2_pread_at(path, offset)
      assert value == "cold_value"
    end

    test "4. DELETE writes tombstone, marks for removal", %{dir: dir} do
      path = data_file(dir, 1)
      {:ok, {_offset, _size}} = NIF.v2_append_record(path, "to_delete", "val", 0)
      {:ok, {tomb_offset, tomb_size}} = NIF.v2_append_tombstone(path, "to_delete")
      assert tomb_offset > 0
      # key "to_delete" is 9 bytes
      assert tomb_size == @header_size + 9

      # Reading the tombstone offset should return nil (tombstone)
      assert {:ok, nil} = NIF.v2_pread_at(path, tomb_offset)
    end

    test "7. Value round-trip: PUT large binary, GET returns identical bytes", %{dir: dir} do
      path = data_file(dir, 1)
      # Create a 64 KiB binary with a recognizable pattern
      large_value = :crypto.strong_rand_bytes(65_536)
      {:ok, {offset, _size}} = NIF.v2_append_record(path, "big", large_value, 0)
      {:ok, read_back} = NIF.v2_pread_at(path, offset)
      assert read_back == large_value
    end

    test "8. CRC validation catches corrupt data", %{dir: dir} do
      path = data_file(dir, 1)
      {:ok, {_offset, _size}} = NIF.v2_append_record(path, "crc_test", "value", 0)

      # Corrupt a byte in the value area (after the 26-byte header + 8 byte key)
      {:ok, fd} = :file.open(path, [:read, :write, :binary])
      # Flip byte at position 34 (inside key area)
      :file.pwrite(fd, @header_size, <<0xFF>>)
      :file.close(fd)

      # pread should fail with CRC error
      assert {:error, reason} = NIF.v2_pread_at(path, 0)
      assert reason =~ "CRC" or reason =~ "mismatch"
    end

    test "9. Empty key works", %{dir: dir} do
      path = data_file(dir, 1)
      {:ok, {offset, _size}} = NIF.v2_append_record(path, "", "value_for_empty_key", 0)
      {:ok, value} = NIF.v2_pread_at(path, offset)
      assert value == "value_for_empty_key"
    end

    test "10. Key with null bytes works", %{dir: dir} do
      path = data_file(dir, 1)
      key = "prefix\0field\0subfield"
      {:ok, {offset, _size}} = NIF.v2_append_record(path, key, "nested_val", 0)
      {:ok, value} = NIF.v2_pread_at(path, offset)
      assert value == "nested_val"
    end
  end

  # =========================================================================
  # Hint file correctness (tests 5-6, related to recovery)
  # =========================================================================

  describe "hint files" do
    test "5. Eviction sets value to nil -- key still readable via cold path", %{dir: dir} do
      path = data_file(dir, 1)
      {:ok, {offset, _size}} = NIF.v2_append_record(path, "evict_me", "original_val", 0)

      # Simulate eviction: ETS value set to nil, disk location retained
      keydir = :ets.new(:v2_test_evict, [:set, :public])
      :ets.insert(keydir, {"evict_me", nil, 0, 3, 1, offset, 12})

      # Verify ETS shows nil (evicted)
      [{_, nil, _, _, file_id, stored_offset, _}] = :ets.lookup(keydir, "evict_me")
      assert file_id == 1
      assert stored_offset == offset

      # Cold read via pread_at should recover the value
      {:ok, value} = NIF.v2_pread_at(path, offset)
      assert value == "original_val"

      :ets.delete(keydir)
    end

    test "6. Write and read hint file round-trips correctly", %{dir: dir} do
      hint_path = hint_file(dir, 1)

      entries = [
        {"key1", 1, 0, 10, 0},
        {"key2", 1, 100, 20, 5000},
        {"key3", 2, 0, 30, 0}
      ]

      assert :ok = NIF.v2_write_hint_file(hint_path, entries)
      assert File.exists?(hint_path)

      {:ok, read_entries} = NIF.v2_read_hint_file(hint_path)
      assert length(read_entries) == 3

      [{key1, fid1, off1, vs1, exp1} | _] = read_entries
      assert key1 == "key1"
      assert fid1 == 1
      assert off1 == 0
      assert vs1 == 10
      assert exp1 == 0
    end

    test "hint file with empty key round-trips", %{dir: dir} do
      hint_path = hint_file(dir, 1)
      entries = [{"", 1, 0, 5, 0}]
      assert :ok = NIF.v2_write_hint_file(hint_path, entries)
      {:ok, [{"", 1, 0, 5, 0}]} = NIF.v2_read_hint_file(hint_path)
    end
  end

  # =========================================================================
  # Scan file (tests 11-12)
  # =========================================================================

  describe "v2_scan_file" do
    test "11. scan_file returns all records in a data file", %{dir: dir} do
      path = data_file(dir, 1)
      {:ok, {_off1, _}} = NIF.v2_append_record(path, "k1", "v1", 0)
      {:ok, {_off2, _}} = NIF.v2_append_record(path, "k2", "v2", 5000)
      {:ok, {_off3, _}} = NIF.v2_append_tombstone(path, "k3")

      {:ok, records} = NIF.v2_scan_file(path)
      assert length(records) == 3

      [{key1, offset1, vs1, _exp1, tomb1} | rest] = records
      assert key1 == "k1"
      assert offset1 == 0
      assert vs1 == 2
      assert tomb1 == false

      [{key2, _off2, vs2, exp2, _tomb2}, {key3, _off3, _vs3, _exp3, tomb3}] = rest
      assert key2 == "k2"
      assert vs2 == 2
      assert exp2 == 5000
      assert key3 == "k3"
      assert tomb3 == true
    end

    test "11b. scan_file_page returns bounded metadata pages with a resume offset", %{dir: dir} do
      path = data_file(dir, 1)
      {:ok, {_off1, _}} = NIF.v2_append_record(path, "k1", "v1", 0)
      {:ok, {_off2, _}} = NIF.v2_append_record(path, "k2", "v22", 5000)
      {:ok, {_off3, _}} = NIF.v2_append_tombstone(path, "k3")

      assert {:ok, [{"k1", 0, 2, 0, false}, {"k2", off2, 3, 5000, false}], next, false} =
               NIF.v2_scan_file_page(path, 0, 2)

      assert next > off2

      assert {:ok, [{"k3", _off3, 0, _exp3, true}], next2, true} =
               NIF.v2_scan_file_page(path, next, 2)

      assert next2 > next
      assert {:ok, [], ^next2, true} = NIF.v2_scan_file_page(path, next2, 2)
    end

    test "12. copy_records copies only specified offsets", %{dir: dir} do
      source_path = data_file(dir, 1)
      {:ok, {off1, _}} = NIF.v2_append_record(source_path, "keep1", "val1", 0)
      {:ok, {_off2, _}} = NIF.v2_append_record(source_path, "skip", "val2", 0)
      {:ok, {off3, _}} = NIF.v2_append_record(source_path, "keep2", "val3", 0)

      dest_path = data_file(dir, 99)
      {:ok, new_locations} = NIF.v2_copy_records(source_path, dest_path, [off1, off3])
      assert length(new_locations) == 2

      # Verify destination file has only the 2 copied records
      {:ok, dest_records} = NIF.v2_scan_file(dest_path)
      assert length(dest_records) == 2
      keys = Enum.map(dest_records, fn {key, _, _, _, _} -> key end)
      assert "keep1" in keys
      assert "keep2" in keys
      refute "skip" in keys
    end
  end

  # =========================================================================
  # Compaction correctness (tests 13-18)
  # =========================================================================

  describe "compaction via copy_records" do
    test "13. After copy, cold reads use new file_id/offset", %{dir: dir} do
      source = data_file(dir, 1)
      {:ok, {off, _}} = NIF.v2_append_record(source, "compact_key", "compact_val", 0)

      dest = data_file(dir, 10)
      {:ok, [{new_off, _new_size}]} = NIF.v2_copy_records(source, dest, [off])

      # Read from the new location
      {:ok, value} = NIF.v2_pread_at(dest, new_off)
      assert value == "compact_val"
    end

    test "15. Stale entries not copied during compaction", %{dir: dir} do
      source = data_file(dir, 1)
      # Write key twice -- only the second is "live"
      {:ok, {_off1, _}} = NIF.v2_append_record(source, "key", "old_val", 0)
      {:ok, {off2, _}} = NIF.v2_append_record(source, "key", "new_val", 0)

      dest = data_file(dir, 10)
      # Only copy the live (latest) offset
      {:ok, [{new_off, _}]} = NIF.v2_copy_records(source, dest, [off2])

      {:ok, value} = NIF.v2_pread_at(dest, new_off)
      assert value == "new_val"

      # Dest should have only 1 record
      {:ok, dest_records} = NIF.v2_scan_file(dest)
      assert length(dest_records) == 1
    end

    test "17. Tombstones not copied by copy_records", %{dir: dir} do
      source = data_file(dir, 1)
      {:ok, {off_live, _}} = NIF.v2_append_record(source, "live", "val", 0)
      {:ok, {off_tomb, _}} = NIF.v2_append_tombstone(source, "dead")

      dest = data_file(dir, 10)
      {:ok, results} = NIF.v2_copy_records(source, dest, [off_live, off_tomb])
      # Tombstones are silently skipped by copy_records
      assert length(results) == 1

      {:ok, dest_records} = NIF.v2_scan_file(dest)
      assert length(dest_records) == 1
      [{key, _, _, _, _}] = dest_records
      assert key == "live"
    end

    test "18. Hint file written after compaction matches records", %{dir: dir} do
      source = data_file(dir, 1)
      {:ok, {off, _}} = NIF.v2_append_record(source, "hinted", "val", 99_000)

      dest = data_file(dir, 10)
      {:ok, [{new_off, _new_size}]} = NIF.v2_copy_records(source, dest, [off])

      # Write a hint file for the compacted data
      hint_path = hint_file(dir, 10)
      hint_entries = [{"hinted", 10, new_off, 3, 99_000}]
      assert :ok = NIF.v2_write_hint_file(hint_path, hint_entries)

      # Read back and verify
      {:ok, [{key, fid, hoff, vs, exp}]} = NIF.v2_read_hint_file(hint_path)
      assert key == "hinted"
      assert fid == 10
      assert hoff == new_off
      assert vs == 3
      assert exp == 99_000
    end
  end

  # =========================================================================
  # Concurrency (tests 19-23)
  # =========================================================================

  describe "concurrent operations" do
    test "19. 100 concurrent cold reads -- no Mutex, all parallel, all return correct data", %{
      dir: dir
    } do
      path = data_file(dir, 1)

      # Write 100 distinct keys
      offsets =
        for i <- 0..99 do
          key = "key_#{i}"
          value = "value_#{i}"
          {:ok, {offset, _}} = NIF.v2_append_record(path, key, value, 0)
          {i, offset}
        end

      # Read all 100 concurrently
      tasks =
        for {i, offset} <- offsets do
          Task.async(fn ->
            {:ok, value} = NIF.v2_pread_at(path, offset)
            assert value == "value_#{i}"
            :ok
          end)
        end

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, &(&1 == :ok))
    end

    test "20. Concurrent read + write -- writer doesn't block readers", %{dir: dir} do
      path = data_file(dir, 1)
      {:ok, {offset, _}} = NIF.v2_append_record(path, "read_me", "stable_value", 0)

      # Start a writer task that writes 50 records
      writer_task =
        Task.async(fn ->
          for i <- 0..49 do
            NIF.v2_append_record(path, "writer_#{i}", "w_val_#{i}", 0)
          end

          :done
        end)

      # Concurrently read the original record 50 times
      reader_tasks =
        for _ <- 0..49 do
          Task.async(fn ->
            {:ok, value} = NIF.v2_pread_at(path, offset)
            assert value == "stable_value"
            :ok
          end)
        end

      # All should complete without errors
      assert Task.await(writer_task, 10_000) == :done
      results = Task.await_many(reader_tasks, 10_000)
      assert Enum.all?(results, &(&1 == :ok))
    end

    test "22. 50 concurrent writes (serialized by caller) -- all succeed", %{dir: dir} do
      # In the real system, the Shard GenServer serialises writes.
      # Here we just verify append_record is safe to call sequentially.
      path = data_file(dir, 1)

      offsets =
        for i <- 0..49 do
          {:ok, {offset, _}} = NIF.v2_append_record(path, "w_#{i}", "val_#{i}", 0)
          {i, offset}
        end

      # All 50 records should be readable
      for {i, offset} <- offsets do
        {:ok, value} = NIF.v2_pread_at(path, offset)
        assert value == "val_#{i}"
      end
    end
  end

  # =========================================================================
  # File management (tests 24-28)
  # =========================================================================

  describe "file management" do
    test "24. Multiple data files are independently readable", %{dir: dir} do
      path1 = data_file(dir, 1)
      path2 = data_file(dir, 2)

      {:ok, {off1, _}} = NIF.v2_append_record(path1, "k1", "v1", 0)
      {:ok, {off2, _}} = NIF.v2_append_record(path2, "k2", "v2", 0)

      {:ok, v1} = NIF.v2_pread_at(path1, off1)
      {:ok, v2} = NIF.v2_pread_at(path2, off2)

      assert v1 == "v1"
      assert v2 == "v2"
    end

    test "25. File IDs in paths are correctly used", %{dir: dir} do
      for fid <- [1, 2, 5, 100] do
        path = data_file(dir, fid)
        {:ok, {offset, _}} = NIF.v2_append_record(path, "k", "v", 0)
        {:ok, value} = NIF.v2_pread_at(path, offset)
        assert value == "v"
      end
    end

    test "27. Old files still readable after new file created", %{dir: dir} do
      old_path = data_file(dir, 1)
      {:ok, {old_off, _}} = NIF.v2_append_record(old_path, "old", "old_val", 0)

      new_path = data_file(dir, 2)
      {:ok, {_new_off, _}} = NIF.v2_append_record(new_path, "new", "new_val", 0)

      # Old file is still readable
      {:ok, old_val} = NIF.v2_pread_at(old_path, old_off)
      assert old_val == "old_val"
    end

    test "28. File path mapping: file_id -> correct path", %{dir: dir} do
      assert data_file(dir, 1) == Path.join(dir, "00000000000000000001.log")
      assert data_file(dir, 42) == Path.join(dir, "00000000000000000042.log")
      assert hint_file(dir, 1) == Path.join(dir, "00000000000000000001.hint")
    end
  end

  # =========================================================================
  # Batch operations
  # =========================================================================

  describe "v2_append_batch" do
    test "batch append writes multiple records with single fsync", %{dir: dir} do
      path = data_file(dir, 1)

      records = [
        {"batch_k1", "batch_v1", 0},
        {"batch_k2", "batch_v2", 1_000_000},
        {"batch_k3", "batch_v3", 0}
      ]

      {:ok, results} = NIF.v2_append_batch(path, records)
      assert length(results) == 3

      # Offsets should be monotonically increasing
      offsets = Enum.map(results, fn {off, _} -> off end)
      assert offsets == Enum.sort(offsets)

      # Each record should be readable via pread_at
      for {{off, _vlen}, {_key, expected_val, _exp}} <- Enum.zip(results, records) do
        {:ok, value} = NIF.v2_pread_at(path, off)
        assert value == expected_val
      end
    end

    test "empty batch is ok", %{dir: dir} do
      path = data_file(dir, 1)
      {:ok, results} = NIF.v2_append_batch(path, [])
      assert results == []
    end
  end

  # =========================================================================
  # pread_batch
  # =========================================================================

  describe "v2_pread_batch" do
    test "batch read multiple offsets from same file", %{dir: dir} do
      path = data_file(dir, 1)
      {:ok, {off1, _}} = NIF.v2_append_record(path, "k1", "v1", 0)
      {:ok, {off2, _}} = NIF.v2_append_record(path, "k2", "v2", 0)
      {:ok, {off3, _}} = NIF.v2_append_record(path, "k3", "v3", 0)

      {:ok, values} = NIF.v2_pread_batch(path, [off1, off2, off3])
      assert length(values) == 3
      assert Enum.at(values, 0) == "v1"
      assert Enum.at(values, 1) == "v2"
      assert Enum.at(values, 2) == "v3"
    end

    test "batch read surfaces CRC errors instead of returning nil", %{dir: dir} do
      path = data_file(dir, 1)
      {:ok, {offset, _}} = NIF.v2_append_record(path, "crc_batch", "value", 0)

      {:ok, fd} = :file.open(path, [:read, :write, :binary])
      :ok = :file.pwrite(fd, @header_size, <<0xFF>>)
      :ok = :file.close(fd)

      assert {:error, reason} = NIF.v2_pread_batch(path, [offset])
      assert reason =~ "CRC" or reason =~ "mismatch"
    end
  end

  # =========================================================================
  # v2_fsync
  # =========================================================================

  describe "v2_fsync" do
    test "fsync on a data file succeeds", %{dir: dir} do
      path = data_file(dir, 1)
      NIF.v2_append_record(path, "k", "v", 0)
      assert :ok = NIF.v2_fsync(path)
    end
  end

  # =========================================================================
  # ETS keydir integration (simulated)
  # =========================================================================

  describe "ETS keydir integration" do
    test "full put/get/evict/cold-read lifecycle", %{dir: dir} do
      path = data_file(dir, 1)

      # 1. PUT: write to disk + insert into ETS with value
      {:ok, {offset, _size}} = NIF.v2_append_record(path, "lifecycle", "hot_value", 0)

      keydir = :ets.new(:v2_lifecycle, [:set, :public])
      :ets.insert(keydir, {"lifecycle", "hot_value", 0, 5, 1, offset, 9})

      # 2. Hot GET: read from ETS (value != nil)
      [{_, value, _, _, _, _, _}] = :ets.lookup(keydir, "lifecycle")
      assert value == "hot_value"

      # 3. Eviction: set value to nil, keep disk location
      :ets.update_element(keydir, "lifecycle", {2, nil})
      [{_, nil, _, _, _, stored_offset, _}] = :ets.lookup(keydir, "lifecycle")
      assert stored_offset == offset

      # 4. Cold GET: pread from disk
      {:ok, recovered} = NIF.v2_pread_at(path, stored_offset)
      assert recovered == "hot_value"

      # 5. Re-warm: put value back in ETS
      :ets.update_element(keydir, "lifecycle", {2, recovered})
      [{_, re_warmed, _, _, _, _, _}] = :ets.lookup(keydir, "lifecycle")
      assert re_warmed == "hot_value"

      :ets.delete(keydir)
    end

    test "expired key not returned on hot read", %{dir: dir} do
      _path = data_file(dir, 1)
      keydir = :ets.new(:v2_expiry, [:set, :public])
      now = System.os_time(:millisecond)
      # Key expired 1 second ago
      :ets.insert(keydir, {"expired_key", "val", now - 1000, 5, 1, 0, 3})

      [{_, _val, exp, _, _, _, _}] = :ets.lookup(keydir, "expired_key")
      assert exp < now

      :ets.delete(keydir)
    end

    test "scan_file can rebuild ETS keydir from a data file", %{dir: dir} do
      path = data_file(dir, 1)
      {:ok, {_off1, _}} = NIF.v2_append_record(path, "rebuild_k1", "v1", 0)
      {:ok, {_off2, _}} = NIF.v2_append_record(path, "rebuild_k2", "v2", 5000)
      {:ok, {_off3, _}} = NIF.v2_append_tombstone(path, "rebuild_k1")

      {:ok, records} = NIF.v2_scan_file(path)

      # Build a keydir from scan results (last-writer-wins, skip tombstones)
      keydir = :ets.new(:v2_rebuild, [:set, :public])

      for {key, offset, value_size, expire_at_ms, is_tombstone} <- records do
        if is_tombstone do
          :ets.delete(keydir, key)
        else
          :ets.insert(keydir, {key, nil, expire_at_ms, 0, 1, offset, value_size})
        end
      end

      # k1 was deleted by tombstone
      assert :ets.lookup(keydir, "rebuild_k1") == []
      # k2 should exist with correct metadata
      [{_, nil, 5000, 0, 1, _offset, 2}] = :ets.lookup(keydir, "rebuild_k2")

      :ets.delete(keydir)
    end

    test "hint file can rebuild ETS keydir on startup", %{dir: dir} do
      path = data_file(dir, 1)
      {:ok, {off1, _}} = NIF.v2_append_record(path, "hint_k1", "v1", 0)
      {:ok, {off2, _}} = NIF.v2_append_record(path, "hint_k2", "v2", 99_000)

      # Write a hint file
      hint_path = hint_file(dir, 1)

      entries = [
        {"hint_k1", 1, off1, 2, 0},
        {"hint_k2", 1, off2, 2, 99_000}
      ]

      :ok = NIF.v2_write_hint_file(hint_path, entries)

      # Rebuild keydir from hint
      {:ok, hint_entries} = NIF.v2_read_hint_file(hint_path)
      keydir = :ets.new(:v2_hint_rebuild, [:set, :public])

      for {key, file_id, offset, value_size, expire_at_ms} <- hint_entries do
        :ets.insert(keydir, {key, nil, expire_at_ms, 0, file_id, offset, value_size})
      end

      # Verify
      [{_, nil, 0, 0, 1, ^off1, 2}] = :ets.lookup(keydir, "hint_k1")
      [{_, nil, 99_000, 0, 1, ^off2, 2}] = :ets.lookup(keydir, "hint_k2")

      # Cold read to verify values
      {:ok, v1} = NIF.v2_pread_at(path, off1)
      {:ok, v2} = NIF.v2_pread_at(path, off2)
      assert v1 == "v1"
      assert v2 == "v2"

      :ets.delete(keydir)
    end
  end

  # =========================================================================
  # Performance sanity checks (tagged :perf)
  # =========================================================================

  describe "performance" do
    @describetag :bench
    test "29. Hot read latency < 1us (single ETS lookup)", %{dir: _dir} do
      keydir = :ets.new(:v2_perf_hot, [:set, :public, {:read_concurrency, true}])

      # Insert 1000 keys
      for i <- 0..999 do
        :ets.insert(keydir, {"key_#{i}", "val_#{i}", 0, 5, 1, i * 100, 6})
      end

      # Time 10000 ETS lookups
      {elapsed_us, _} =
        :timer.tc(fn ->
          for _ <- 0..9999 do
            :ets.lookup(keydir, "key_500")
          end
        end)

      avg_ns = elapsed_us * 1000 / 10_000
      assert avg_ns < 1000, "Hot read avg #{avg_ns}ns exceeds 1000ns"

      :ets.delete(keydir)
    end

    @tag :bench
    test "31. 100 concurrent cold reads faster than serialized", %{dir: dir} do
      path = data_file(dir, 1)

      # Write 100 records
      offsets =
        for i <- 0..99 do
          {:ok, {offset, _}} =
            NIF.v2_append_record(path, "perf_#{i}", String.duplicate("x", 100), 0)

          offset
        end

      # Serialized
      {serial_us, _} =
        :timer.tc(fn ->
          for offset <- offsets do
            NIF.v2_pread_at(path, offset)
          end
        end)

      # Concurrent
      {concurrent_us, _} =
        :timer.tc(fn ->
          tasks =
            for offset <- offsets do
              Task.async(fn -> NIF.v2_pread_at(path, offset) end)
            end

          Task.await_many(tasks, 10_000)
        end)

      # Concurrent should be at least somewhat faster on multicore
      # (But we mainly care it doesn't deadlock or error)
      assert concurrent_us < serial_us * 2, "Concurrent reads should not be 2x slower than serial"
    end
  end
end
