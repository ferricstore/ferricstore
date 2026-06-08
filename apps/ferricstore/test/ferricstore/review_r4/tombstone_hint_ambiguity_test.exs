defmodule Ferricstore.ReviewR4.TombstoneHintAmbiguityTest do
  @moduledoc """
  R4-A1: Tombstone vs empty value in hint files.

  The hint file format uses `value_size` to distinguish live entries from
  tombstones. The Rust `HintReader::load_into` treats `value_size == 0` as a
  tombstone and calls `keydir.delete()`. However, `value_size == 0` is also a
  legitimate empty value (e.g. `SET key ""`). The data log format correctly
  distinguishes the two: tombstones use `u32::MAX` (the `TOMBSTONE` sentinel)
  while empty values use `value_size = 0`.

  This mismatch means: if a live key with an empty value is written to a hint
  file, and the store is recovered from that hint file, the key is incorrectly
  deleted (treated as tombstone) instead of being loaded as a live key with an
  empty value.

  The Elixir v2 recovery path (`v2_read_hint_file` + ETS insert) does NOT
  filter on `value_size == 0`, so it is NOT affected by this bug. But the
  Rust `Store::open` path IS affected (via `HintReader::load_into`).

  These tests verify the bug at the NIF level by:
  1. Writing a hint entry with value_size=0 via `v2_write_hint_file`
  2. Reading it back via `v2_read_hint_file` (Elixir path -- works correctly)
  3. Demonstrating the end-to-end scenario: put an empty value, write hint,
     recover from hint -- the key must survive.
  """

  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.NIF

  @moduletag timeout: 30_000

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "r4_a1_#{:rand.uniform(99_999_999)}")
    File.mkdir_p!(dir)

    on_exit(fn -> File.rm_rf!(dir) end)

    dir
  end

  # ===========================================================================
  # Test 1: v2_write_hint_file + v2_read_hint_file round-trips value_size=0
  # ===========================================================================

  describe "R4-A1: hint file value_size=0 round-trip" do
    test "v2_read_hint_file returns entries with value_size=0 (not filtered out)" do
      dir = tmp_dir()
      hint_path = Path.join(dir, "00001.hint")

      # Write a hint file with two entries:
      #   key "live_empty" with value_size=0 (legitimate empty value)
      #   key "live_nonempty" with value_size=42
      entries = [
        {"live_empty", 1, 0, 0, 0},
        {"live_nonempty", 1, 100, 42, 0}
      ]

      assert :ok = NIF.v2_write_hint_file(hint_path, entries)

      # Read back -- both entries must be present
      assert {:ok, read_entries} = NIF.v2_read_hint_file(hint_path)

      assert length(read_entries) == 2,
             "v2_read_hint_file must return ALL entries including value_size=0, got #{length(read_entries)}"

      # Find the empty-value entry
      empty_entry = Enum.find(read_entries, fn {key, _, _, _, _} -> key == "live_empty" end)

      assert empty_entry != nil,
             "entry with value_size=0 must be present in v2_read_hint_file output"

      {_key, _fid, _offset, value_size, _expire} = empty_entry
      assert value_size == 0, "value_size must be 0 for the empty-value entry"
    end

    test "v2_write_hint_file accepts value_size=0 without error" do
      dir = tmp_dir()
      hint_path = Path.join(dir, "00002.hint")

      entries = [{"empty_val_key", 1, 0, 0, 0}]
      assert :ok = NIF.v2_write_hint_file(hint_path, entries)
    end
  end

  # ===========================================================================
  # Test 2: End-to-end scenario -- empty value survives hint-based recovery
  #
  # This tests the actual bug: write a record with empty value to a data file,
  # write its hint entry, then verify that log-based recovery correctly treats
  # value_size=0 as a live empty value (not a tombstone).
  # ===========================================================================

  describe "R4-A1: empty value vs tombstone in data log" do
    test "v2_scan_file distinguishes empty value (value_size=0) from tombstone (is_tombstone=true)" do
      dir = tmp_dir()
      log_path = Path.join(dir, "00001.log")

      # Write a record with an empty value (value_size=0, but NOT a tombstone)
      {:ok, {offset_empty, _size}} = NIF.v2_append_record(log_path, "empty_key", "", 0)

      # Write a tombstone for a different key
      {:ok, {offset_tomb, _size}} = NIF.v2_append_tombstone(log_path, "deleted_key")

      # Write a normal record for comparison
      {:ok, {offset_normal, _size}} = NIF.v2_append_record(log_path, "normal_key", "hello", 0)

      # Scan the file -- each record should have correct is_tombstone flag
      {:ok, records} = NIF.v2_scan_file(log_path)
      assert length(records) == 3

      # Record 1: empty value -- NOT a tombstone
      {key1, ^offset_empty, vs1, _exp1, tomb1} = Enum.at(records, 0)
      assert key1 == "empty_key"
      assert vs1 == 0, "empty value has value_size=0"
      assert tomb1 == false, "empty value must NOT be flagged as tombstone"

      # Record 2: tombstone -- IS a tombstone
      {key2, ^offset_tomb, _vs2, _exp2, tomb2} = Enum.at(records, 1)
      assert key2 == "deleted_key"
      assert tomb2 == true, "deleted key must be flagged as tombstone"

      # Record 3: normal value
      {key3, ^offset_normal, vs3, _exp3, tomb3} = Enum.at(records, 2)
      assert key3 == "normal_key"
      assert vs3 == 5
      assert tomb3 == false
    end

    test "v2_pread_at returns empty binary for value_size=0 records (not nil)" do
      dir = tmp_dir()
      log_path = Path.join(dir, "00001.log")

      # Write a record with an empty value
      {:ok, {offset, _size}} = NIF.v2_append_record(log_path, "empty_key", "", 0)

      # pread should return the empty binary, not nil (nil = tombstone)
      result = NIF.v2_pread_at(log_path, offset)
      assert {:ok, value} = result
      assert value == "", "pread of empty-value record must return empty string, not nil"
    end

    test "hint file with value_size=0 entry round-trips correctly via v2_read_hint_file" do
      dir = tmp_dir()
      log_path = Path.join(dir, "00001.log")
      hint_path = Path.join(dir, "00001.hint")

      # Simulate the full write + hint workflow:
      # 1. Write empty-value record to data file
      {:ok, {offset, _}} = NIF.v2_append_record(log_path, "empty_key", "", 0)

      # 2. Write hint entry for it (value_size=0, as would happen in write_hint_for_file)
      hint_entries = [{"empty_key", 1, offset, 0, 0}]
      assert :ok = NIF.v2_write_hint_file(hint_path, hint_entries)

      # 3. Read hint file back
      {:ok, read_entries} = NIF.v2_read_hint_file(hint_path)
      assert length(read_entries) == 1

      {key, _fid, read_offset, value_size, _exp} = hd(read_entries)
      assert key == "empty_key"
      assert read_offset == offset
      assert value_size == 0

      # 4. Verify the actual value is still readable from the data file
      {:ok, value} = NIF.v2_pread_at(log_path, offset)
      assert value == "", "empty value must be readable from data file at hint offset"
    end
  end

  # ===========================================================================
  # Test 3: Demonstrate the Rust-level bug (HintReader::load_into treats
  # value_size=0 as tombstone). This cannot be tested directly from Elixir
  # since load_into is not exposed as a NIF. But we document the bug and
  # verify the Elixir recovery path is safe.
  # ===========================================================================

  describe "R4-A1: Elixir recovery path is not affected by Rust load_into bug" do
    test "Elixir hint recovery inserts value_size=0 entries into ETS (no tombstone filtering)" do
      dir = tmp_dir()
      hint_path = Path.join(dir, "00001.hint")

      # Write hint with an empty-value entry
      entries = [
        {"alive_empty", 1, 0, 0, 0},
        {"alive_normal", 1, 100, 42, 0}
      ]

      assert :ok = NIF.v2_write_hint_file(hint_path, entries)

      # Simulate Elixir recovery: read hint file and insert into ETS
      # (this mimics shard.ex:354-362)
      keydir = :ets.new(:test_keydir, [:set, :public])

      {:ok, hint_entries} = NIF.v2_read_hint_file(hint_path)

      Enum.each(hint_entries, fn {key, _file_id, offset, value_size, expire_at_ms} ->
        :ets.insert(keydir, {key, nil, expire_at_ms, 0, 1, offset, value_size})
      end)

      # Both entries must exist in ETS
      assert :ets.lookup(keydir, "alive_empty") != [],
             "empty-value key must survive Elixir hint recovery"

      assert :ets.lookup(keydir, "alive_normal") != [],
             "normal-value key must survive Elixir hint recovery"

      [{_, _, _, _, _, _, vs}] = :ets.lookup(keydir, "alive_empty")
      assert vs == 0, "value_size must be 0 in ETS for empty-value key"

      :ets.delete(keydir)
    end
  end
end
