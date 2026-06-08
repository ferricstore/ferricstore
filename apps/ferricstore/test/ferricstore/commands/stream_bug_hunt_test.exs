defmodule Ferricstore.Commands.StreamBugHuntTest do
  @moduledoc """
  Targeted bug-hunt tests for Stream commands.

  Each `describe` block probes one specific corner case from the bug-hunt
  checklist. Tests document current behaviour and flag deviations from Redis
  semantics with `@tag :bug` so they can be filtered with
  `mix test --only bug` once the fixes land.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Stream
  alias Ferricstore.Test.MockStore

  # Unique stream key per call to avoid inter-test interference.
  defp ustream, do: "bh_#{System.unique_integer([:positive, :monotonic])}"

  # Wipe stream ETS tables before every test.
  setup do
    for table <- [Ferricstore.Stream.Meta, Ferricstore.Stream.Groups] do
      if :ets.whereis(table) != :undefined do
        :ets.delete_all_objects(table)
      end
    end

    :ok
  end

  # ===========================================================================
  # 1. XADD with auto-generated ID (*) — monotonic
  # ===========================================================================

  describe "XADD auto-ID monotonicity" do
    test "auto-generated IDs are strictly increasing across rapid successive adds" do
      store = MockStore.make()
      key = ustream()

      ids =
        for _ <- 1..50 do
          id = Stream.handle("XADD", [key, "*", "k", "v"], store)
          assert is_binary(id), "expected binary id, got: #{inspect(id)}"
          id
        end

      parsed =
        Enum.map(ids, fn id ->
          [ms, seq] = String.split(id, "-")
          {String.to_integer(ms), String.to_integer(seq)}
        end)

      # Every successive pair must be strictly greater.
      parsed
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.each(fn [{ms1, seq1}, {ms2, seq2}] ->
        assert {ms2, seq2} > {ms1, seq1},
               "IDs not monotonic: #{ms1}-#{seq1} >= #{ms2}-#{seq2}"
      end)
    end

    test "auto-IDs stay monotonic even when clock would go backwards" do
      # We cannot mock System.os_time, but we CAN force last_ms to be far
      # in the future via an explicit ID, then rely on auto-ID.
      store = MockStore.make()
      key = ustream()

      # Plant an entry with a timestamp far in the future.
      future_ms = System.os_time(:millisecond) + 100_000
      explicit_id = "#{future_ms}-0"
      assert ^explicit_id = Stream.handle("XADD", [key, explicit_id, "a", "1"], store)

      # Auto-ID must be > future_ms-0 despite the wall clock being behind.
      auto_id = Stream.handle("XADD", [key, "*", "b", "2"], store)
      assert is_binary(auto_id)

      {auto_ms, auto_seq} =
        auto_id |> String.split("-") |> Enum.map(&String.to_integer/1) |> List.to_tuple()

      assert {auto_ms, auto_seq} > {future_ms, 0},
             "auto-ID #{auto_id} did not exceed future explicit ID #{explicit_id}"
    end
  end

  # ===========================================================================
  # 2. XADD with explicit ID lower than last — error
  # ===========================================================================

  describe "XADD explicit ID ordering" do
    test "explicit ID equal to last entry is rejected" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "100-5", "a", "1"], store)
      result = Stream.handle("XADD", [key, "100-5", "b", "2"], store)

      assert {:error, msg} = result
      assert msg =~ "equal or smaller"
    end

    test "explicit ID with lower ms is rejected" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "200-0", "a", "1"], store)
      result = Stream.handle("XADD", [key, "100-0", "b", "2"], store)

      assert {:error, msg} = result
      assert msg =~ "equal or smaller"
    end

    test "explicit ID with same ms but lower seq is rejected" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "100-5", "a", "1"], store)
      result = Stream.handle("XADD", [key, "100-3", "b", "2"], store)

      assert {:error, msg} = result
      assert msg =~ "equal or smaller"
    end

    test "explicit ID with 0-0 on non-empty stream is rejected" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-1", "a", "1"], store)
      result = Stream.handle("XADD", [key, "0-0", "b", "2"], store)

      assert {:error, msg} = result
      assert msg =~ "equal or smaller"
    end
  end

  # ===========================================================================
  # 3. XLEN on non-existent stream — 0
  # ===========================================================================

  describe "XLEN on non-existent stream" do
    test "returns 0 for a key that was never written" do
      store = MockStore.make()
      assert 0 == Stream.handle("XLEN", ["totally_missing_#{System.unique_integer()}"], store)
    end

    test "returns 0 for a key after all entries are deleted via XDEL" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "f", "v"], store)
      Stream.handle("XADD", [key, "2-0", "f", "v"], store)
      Stream.handle("XDEL", [key, "1-0", "2-0"], store)

      assert 0 == Stream.handle("XLEN", [key], store)
    end

    test "returns 0 for a key after all entries are removed via XTRIM MAXLEN 0" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "f", "v"], store)
      Stream.handle("XADD", [key, "2-0", "g", "w"], store)
      Stream.handle("XTRIM", [key, "MAXLEN", "0"], store)

      # Redis returns 0 here — the stream key still exists but is empty.
      assert 0 == Stream.handle("XLEN", [key], store)
    end
  end

  # ===========================================================================
  # 4. XRANGE with - + returns all entries
  # ===========================================================================

  describe "XRANGE - + returns all entries" do
    test "single entry" do
      store = MockStore.make()
      key = ustream()

      id = Stream.handle("XADD", [key, "42-0", "hello", "world"], store)
      entries = Stream.handle("XRANGE", [key, "-", "+"], store)

      assert length(entries) == 1
      assert hd(entries) == [id, "hello", "world"]
    end

    test "many entries are returned in chronological order" do
      store = MockStore.make()
      key = ustream()

      expected_ids =
        for i <- 1..20 do
          id = Stream.handle("XADD", [key, "#{i}-0", "n", "#{i}"], store)
          id
        end

      entries = Stream.handle("XRANGE", [key, "-", "+"], store)
      assert length(entries) == 20
      assert Enum.map(entries, &hd/1) == expected_ids
    end

    test "entries with the same ms but different seq are included" do
      store = MockStore.make()
      key = ustream()

      for seq <- 0..4 do
        Stream.handle("XADD", [key, "100-#{seq}", "s", "#{seq}"], store)
      end

      entries = Stream.handle("XRANGE", [key, "-", "+"], store)
      assert length(entries) == 5
      ids = Enum.map(entries, &hd/1)
      assert ids == ["100-0", "100-1", "100-2", "100-3", "100-4"]
    end
  end

  # ===========================================================================
  # 5. XREVRANGE returns reverse order
  # ===========================================================================

  describe "XREVRANGE returns reverse order" do
    test "full range is reversed" do
      store = MockStore.make()
      key = ustream()

      for i <- 1..5, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)

      fwd = Stream.handle("XRANGE", [key, "-", "+"], store)
      rev = Stream.handle("XREVRANGE", [key, "+", "-"], store)

      assert rev == Enum.reverse(fwd)
    end

    test "sub-range is reversed" do
      store = MockStore.make()
      key = ustream()

      for i <- 1..10, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)

      fwd = Stream.handle("XRANGE", [key, "3-0", "7-0"], store)
      rev = Stream.handle("XREVRANGE", [key, "7-0", "3-0"], store)

      assert rev == Enum.reverse(fwd)
    end

    test "empty result on non-existent stream" do
      store = MockStore.make()

      assert [] ==
               Stream.handle("XREVRANGE", ["nope_#{System.unique_integer()}", "+", "-"], store)
    end
  end

  # ===========================================================================
  # 6. XREAD COUNT 0 — returns nothing or error?
  #
  # BUG: parse_xread_count has guard `n > 0`, so COUNT 0 is silently ignored
  # and all entries are returned.  Redis returns an empty array for COUNT 0.
  # ===========================================================================

  describe "XREAD COUNT 0" do
    @tag :bug
    test "COUNT 0 should return empty result, not all entries" do
      store = MockStore.make()
      key = ustream()

      for i <- 1..5, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)

      result = Stream.handle("XREAD", ["COUNT", "0", "STREAMS", key, "0"], store)

      # Redis behaviour: COUNT 0 means "give me zero entries" -> empty.
      # Current behaviour (bug): COUNT 0 falls through the guard, count
      # becomes :infinity, and all 5 entries are returned.
      assert result == [],
             "XREAD COUNT 0 should return [], got #{length(get_entries(result))} entries. " <>
               "Bug: parse_xread_count guard `n > 0` drops COUNT 0 and defaults to :infinity."
    end
  end

  # ===========================================================================
  # 7. XREAD BLOCK — now implemented
  #
  # BLOCK is parsed by the stream command handler. When data is immediately
  # available, it returns the result directly. When no data is available,
  # it returns {:block, timeout_ms, stream_ids, count} so the connection
  # layer can handle actual blocking.
  # ===========================================================================

  describe "XREAD BLOCK" do
    test "BLOCK with data available returns entries immediately" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "f", "v"], store)

      # XREAD BLOCK 0 STREAMS key 0 — data exists, should return immediately.
      result = Stream.handle("XREAD", ["BLOCK", "0", "STREAMS", key, "0"], store)

      assert is_list(result)
      assert length(result) == 1
      [[^key, entries]] = result
      assert length(entries) == 1
    end

    test "BLOCK with timeout and data returns entries immediately" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "f", "v"], store)

      # XREAD BLOCK 5000 STREAMS key 0 — data exists, should return immediately.
      result = Stream.handle("XREAD", ["BLOCK", "5000", "STREAMS", key, "0"], store)

      assert is_list(result)
      assert length(result) == 1
    end

    test "BLOCK with no data signals blocking to connection layer" do
      store = MockStore.make()
      key = ustream()

      # No data in this stream — should return {:block, ...} tuple.
      result = Stream.handle("XREAD", ["BLOCK", "100", "STREAMS", key, "0"], store)

      assert {:block, 100, [{^key, "0"}], :infinity} = result
    end
  end

  # ===========================================================================
  # 8. XGROUP CREATE on non-existent stream without MKSTREAM — error
  # ===========================================================================

  describe "XGROUP CREATE without MKSTREAM on non-existent stream" do
    test "returns error about the key needing to exist" do
      store = MockStore.make()
      key = ustream()

      result = Stream.handle("XGROUP", ["CREATE", key, "mygroup", "0"], store)

      assert {:error, msg} = result
      assert msg =~ "requires the key to exist"
      assert msg =~ "MKSTREAM"
    end

    test "MKSTREAM creates the stream and the group succeeds" do
      store = MockStore.make()
      key = ustream()

      result = Stream.handle("XGROUP", ["CREATE", key, "g1", "0", "MKSTREAM"], store)
      assert :ok == result

      # Stream exists (empty).
      assert 0 == Stream.handle("XLEN", [key], store)

      # Group is usable: XREADGROUP should not crash.
      read_result =
        Stream.handle("XREADGROUP", ["GROUP", "g1", "c1", "STREAMS", key, ">"], store)

      assert read_result == []
    end

    test "MKSTREAM is case-insensitive" do
      store = MockStore.make()
      key = ustream()

      result = Stream.handle("XGROUP", ["CREATE", key, "g1", "0", "mkstream"], store)
      assert :ok == result
    end
  end

  # ===========================================================================
  # 9. XREADGROUP with > returns new entries only
  # ===========================================================================

  describe "XREADGROUP with > returns new entries only" do
    test "first read with > after group at 0 delivers all existing entries" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      Stream.handle("XADD", [key, "2-0", "b", "2"], store)
      Stream.handle("XADD", [key, "3-0", "c", "3"], store)
      Stream.handle("XGROUP", ["CREATE", key, "g1", "0"], store)

      [[^key, entries]] =
        Stream.handle("XREADGROUP", ["GROUP", "g1", "c1", "STREAMS", key, ">"], store)

      assert length(entries) == 3
      assert Enum.map(entries, &hd/1) == ["1-0", "2-0", "3-0"]
    end

    test "second read with > only returns entries added after the first read" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      Stream.handle("XGROUP", ["CREATE", key, "g1", "0"], store)

      # First read consumes 1-0.
      Stream.handle("XREADGROUP", ["GROUP", "g1", "c1", "STREAMS", key, ">"], store)

      # Add more entries.
      Stream.handle("XADD", [key, "2-0", "b", "2"], store)
      Stream.handle("XADD", [key, "3-0", "c", "3"], store)

      [[^key, entries]] =
        Stream.handle("XREADGROUP", ["GROUP", "g1", "c1", "STREAMS", key, ">"], store)

      ids = Enum.map(entries, &hd/1)
      assert ids == ["2-0", "3-0"], "Expected only new entries, got #{inspect(ids)}"
    end

    test "group created with $ only sees entries added after creation" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "old", "data"], store)
      Stream.handle("XGROUP", ["CREATE", key, "g1", "$"], store)

      # No new entries yet.
      assert [] ==
               Stream.handle("XREADGROUP", ["GROUP", "g1", "c1", "STREAMS", key, ">"], store)

      # Add a new entry.
      Stream.handle("XADD", [key, "2-0", "new", "data"], store)

      [[^key, entries]] =
        Stream.handle("XREADGROUP", ["GROUP", "g1", "c1", "STREAMS", key, ">"], store)

      assert length(entries) == 1
      assert hd(hd(entries)) == "2-0"
    end
  end

  # ===========================================================================
  # 10. XACK on already-acked entry — returns 0
  # ===========================================================================

  describe "XACK on already-acked entry" do
    test "first XACK returns 1, second XACK returns 0" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      Stream.handle("XGROUP", ["CREATE", key, "g1", "0"], store)
      Stream.handle("XREADGROUP", ["GROUP", "g1", "c1", "STREAMS", key, ">"], store)

      assert 1 == Stream.handle("XACK", [key, "g1", "1-0"], store)
      assert 0 == Stream.handle("XACK", [key, "g1", "1-0"], store)
    end

    test "XACK on entry that was never delivered returns 0" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      Stream.handle("XGROUP", ["CREATE", key, "g1", "0"], store)

      # Never read via XREADGROUP — 1-0 is not in the pending list.
      assert 0 == Stream.handle("XACK", [key, "g1", "1-0"], store)
    end

    test "XACK on non-existent group returns 0" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)

      assert 0 == Stream.handle("XACK", [key, "no_such_group", "1-0"], store)
    end

    test "XACK with mixture of pending and already-acked IDs" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      Stream.handle("XADD", [key, "2-0", "b", "2"], store)
      Stream.handle("XADD", [key, "3-0", "c", "3"], store)
      Stream.handle("XGROUP", ["CREATE", key, "g1", "0"], store)
      Stream.handle("XREADGROUP", ["GROUP", "g1", "c1", "STREAMS", key, ">"], store)

      # Ack 1-0 first.
      assert 1 == Stream.handle("XACK", [key, "g1", "1-0"], store)

      # Now ack 1-0 (already acked) + 2-0 (still pending) + 99-0 (never existed).
      acked = Stream.handle("XACK", [key, "g1", "1-0", "2-0", "99-0"], store)
      assert acked == 1, "only 2-0 should be newly acked, got #{acked}"
    end
  end

  # ===========================================================================
  # 11. XTRIM MAXLEN 0 — removes all entries
  #
  # BUG: After XTRIM MAXLEN 0, update_meta_after_trim([]) calls
  # :ets.delete(@meta_table, key), which erases the stream entirely.
  # A subsequent XADD * will create a brand-new stream whose auto-ID
  # does not respect the old last-generated-id. This breaks monotonicity.
  # ===========================================================================

  describe "XTRIM MAXLEN 0 removes all entries" do
    test "all entries are removed" do
      store = MockStore.make()
      key = ustream()

      for i <- 1..5, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)

      deleted = Stream.handle("XTRIM", [key, "MAXLEN", "0"], store)
      assert deleted == 5
      assert 0 == Stream.handle("XLEN", [key], store)

      entries = Stream.handle("XRANGE", [key, "-", "+"], store)
      assert entries == []
    end

    @tag :bug
    test "XADD after XTRIM MAXLEN 0 respects previous last-generated-id" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "500-0", "a", "1"], store)
      Stream.handle("XADD", [key, "500-1", "b", "2"], store)
      Stream.handle("XTRIM", [key, "MAXLEN", "0"], store)

      # Auto-ID after trimming should still be > 500-1.
      new_id = Stream.handle("XADD", [key, "*", "c", "3"], store)

      assert is_binary(new_id), "expected a valid ID, got #{inspect(new_id)}"

      {new_ms, new_seq} =
        new_id |> String.split("-") |> Enum.map(&String.to_integer/1) |> List.to_tuple()

      assert {new_ms, new_seq} > {500, 1},
             "Bug: XTRIM MAXLEN 0 erases metadata via :ets.delete, so XADD * " <>
               "generates #{new_id} which does not respect the previous last ID 500-1. " <>
               "Fix: update_meta_after_trim([]) should preserve {key, 0, \"0-0\", last, ms, seq}."
    end

    @tag :bug
    test "explicit ID <= previous last after XTRIM MAXLEN 0 should be rejected" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "500-0", "a", "1"], store)
      Stream.handle("XTRIM", [key, "MAXLEN", "0"], store)

      # This should be rejected because the stream's last-generated-id was 500-0.
      result = Stream.handle("XADD", [key, "100-0", "b", "2"], store)

      assert {:error, _msg} = result,
             "Bug: after XTRIM MAXLEN 0 the stream metadata is gone, so XADD with " <>
               "an old ID #{inspect(result)} succeeds when it should fail."
    end

    @tag :bug
    test "XINFO STREAM after XTRIM MAXLEN 0 should report empty stream, not missing key" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "f", "v"], store)
      Stream.handle("XTRIM", [key, "MAXLEN", "0"], store)

      info = Stream.handle("XINFO", ["STREAM", key], store)

      # Redis: the stream still exists, length=0.
      # Bug: update_meta_after_trim([]) deletes the key from ETS, so
      # XINFO returns {:error, "ERR no such key"}.
      assert is_map(info),
             "Bug: XINFO returns #{inspect(info)} instead of a map. " <>
               "The stream should still exist with length 0 after XTRIM MAXLEN 0."
    end
  end

  # ===========================================================================
  # 12. XDEL non-existent entry — returns 0
  # ===========================================================================

  describe "XDEL non-existent entry" do
    test "returns 0 when the entry ID does not exist" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "f", "v"], store)

      assert 0 == Stream.handle("XDEL", [key, "999-0"], store)
    end

    test "returns 0 when the stream key does not exist at all" do
      store = MockStore.make()
      assert 0 == Stream.handle("XDEL", ["no_such_stream", "1-0"], store)
    end

    test "returns 0 when deleting an already-deleted entry" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "f", "v"], store)
      assert 1 == Stream.handle("XDEL", [key, "1-0"], store)

      # Second delete of same ID.
      assert 0 == Stream.handle("XDEL", [key, "1-0"], store)
    end
  end

  # ===========================================================================
  # 13. XINFO STREAM returns correct metadata
  # ===========================================================================

  describe "XINFO STREAM returns correct metadata" do
    test "basic metadata fields" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "10-0", "name", "alice"], store)
      Stream.handle("XADD", [key, "20-0", "name", "bob"], store)
      Stream.handle("XADD", [key, "30-0", "name", "carol"], store)

      info = Stream.handle("XINFO", ["STREAM", key], store)

      assert is_map(info)
      assert info["length"] == 3
      assert info["first-entry"] == ["10-0", "name", "alice"]
      assert info["last-entry"] == ["30-0", "name", "carol"]
      assert info["last-generated-id"] == "30-0"
      assert info["groups"] == 0
    end

    test "group count reflects created groups" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "f", "v"], store)
      Stream.handle("XGROUP", ["CREATE", key, "g1", "0"], store)
      Stream.handle("XGROUP", ["CREATE", key, "g2", "0"], store)
      Stream.handle("XGROUP", ["CREATE", key, "g3", "0"], store)

      info = Stream.handle("XINFO", ["STREAM", key], store)
      assert info["groups"] == 3
    end

    test "first-entry and last-entry contain correct field-value data" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "color", "red", "size", "large"], store)
      Stream.handle("XADD", [key, "2-0", "color", "blue", "size", "small"], store)

      info = Stream.handle("XINFO", ["STREAM", key], store)

      assert info["first-entry"] == ["1-0", "color", "red", "size", "large"]
      assert info["last-entry"] == ["2-0", "color", "blue", "size", "small"]
    end

    test "metadata updates after XDEL" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      Stream.handle("XADD", [key, "2-0", "b", "2"], store)
      Stream.handle("XADD", [key, "3-0", "c", "3"], store)

      # Delete the first entry.
      Stream.handle("XDEL", [key, "1-0"], store)

      info = Stream.handle("XINFO", ["STREAM", key], store)
      assert info["length"] == 2
      assert hd(info["first-entry"]) == "2-0"
      assert hd(info["last-entry"]) == "3-0"
    end

    test "metadata updates after XTRIM" do
      store = MockStore.make()
      key = ustream()

      for i <- 1..5, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)

      Stream.handle("XTRIM", [key, "MAXLEN", "2"], store)

      info = Stream.handle("XINFO", ["STREAM", key], store)
      assert info["length"] == 2
      assert hd(info["first-entry"]) == "4-0"
      assert hd(info["last-entry"]) == "5-0"
      assert info["last-generated-id"] == "5-0"
    end

    test "XINFO on non-existent key returns error" do
      store = MockStore.make()
      result = Stream.handle("XINFO", ["STREAM", "no_such_key_#{System.unique_integer()}"], store)
      assert {:error, "ERR no such key"} = result
    end
  end

  # ===========================================================================
  # Helpers
  # ===========================================================================

  # Extract entries from an XREAD-style result for assertion messages.
  defp get_entries(result) when is_list(result) do
    Enum.flat_map(result, fn
      [_key, entries] -> entries
      _ -> []
    end)
  end

  defp get_entries(_), do: []
end
