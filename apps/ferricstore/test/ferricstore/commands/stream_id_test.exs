defmodule Ferricstore.Commands.StreamIdTest do
  @moduledoc """
  Tests for stream ID generation and sequence counter logic.

  Verifies that:
  - Auto-generated IDs (`*`) are monotonically increasing
  - Multiple XADDs within the same millisecond increment the sequence counter
  - IDs follow the `ms-seq` format
  - Explicit IDs work when they are greater than the last ID
  - Explicit IDs are rejected when they are less than or equal to the last ID
  - Partial IDs (ms only) auto-assign the sequence number
  - Clock-backward scenarios maintain monotonicity
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Stream
  alias Ferricstore.Commands.Stream.ID
  alias Ferricstore.Test.MockStore

  defp ustream, do: "sid_#{System.unique_integer([:positive, :monotonic])}"

  setup do
    for table <- [Ferricstore.Stream.Meta, Ferricstore.Stream.Groups] do
      if :ets.whereis(table) != :undefined do
        :ets.delete_all_objects(table)
      end
    end

    :ok
  end

  # ===========================================================================
  # Auto-ID format and monotonicity
  # ===========================================================================

  describe "auto-ID format" do
    test "XADD with * generates ID in ms-seq format" do
      store = MockStore.make()
      key = ustream()

      id = Stream.handle("XADD", [key, "*", "k", "v"], store)

      assert is_binary(id)
      assert id =~ ~r/^\d+-\d+$/
      [ms_str, seq_str] = String.split(id, "-")
      ms = String.to_integer(ms_str)
      seq = String.to_integer(seq_str)
      assert ms > 0
      assert seq >= 0
    end

    test "first auto-ID starts with sequence 0" do
      store = MockStore.make()
      key = ustream()

      id = Stream.handle("XADD", [key, "*", "k", "v"], store)

      [_ms_str, seq_str] = String.split(id, "-")
      assert String.to_integer(seq_str) == 0
    end

    test "auto-IDs are monotonically increasing across 100 rapid adds" do
      store = MockStore.make()
      key = ustream()

      ids =
        for _ <- 1..100 do
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
  end

  # ===========================================================================
  # Same-millisecond sequence increment
  # ===========================================================================

  describe "same-millisecond sequence increment" do
    test "multiple XADDs within same millisecond increment sequence" do
      store = MockStore.make()
      key = ustream()

      # Force entries to the same millisecond using explicit partial IDs.
      id1 = Stream.handle("XADD", [key, "5000", "a", "1"], store)
      id2 = Stream.handle("XADD", [key, "5000", "b", "2"], store)
      id3 = Stream.handle("XADD", [key, "5000", "c", "3"], store)

      assert id1 == "5000-0"
      assert id2 == "5000-1"
      assert id3 == "5000-2"
    end

    test "sequence resets to 0 when millisecond advances" do
      store = MockStore.make()
      key = ustream()

      id1 = Stream.handle("XADD", [key, "5000-5", "a", "1"], store)
      assert id1 == "5000-5"

      # Move to a new millisecond with partial ID.
      id2 = Stream.handle("XADD", [key, "5001", "b", "2"], store)
      assert id2 == "5001-0"
    end

    test "explicit IDs at same millisecond with incrementing sequences" do
      store = MockStore.make()
      key = ustream()

      for seq <- 0..9 do
        id = Stream.handle("XADD", [key, "1000-#{seq}", "n", "#{seq}"], store)
        assert id == "1000-#{seq}"
      end

      assert 10 == Stream.handle("XLEN", [key], store)
    end
  end

  # ===========================================================================
  # Explicit ID validation
  # ===========================================================================

  describe "explicit IDs" do
    test "rejects negative and overflowing unsigned components" do
      max_u64 = 18_446_744_073_709_551_615

      assert {:ok, {^max_u64, ^max_u64}} = ID.parse_full_id("#{max_u64}-#{max_u64}")

      for invalid <- ["1--1", "#{max_u64 + 1}-0", "0-#{max_u64 + 1}"] do
        assert {:error, "ERR Invalid stream ID specified as stream command argument"} =
                 ID.parse_full_id(invalid)
      end
    end

    test "auto and partial IDs do not overflow the sequence component" do
      max_u64 = 18_446_744_073_709_551_615

      assert {:error, _message} = ID.resolve(:auto, max_u64, max_u64)
      assert {:error, _message} = ID.resolve({:partial, max_u64}, max_u64, max_u64)
    end

    test "explicit ID greater than last is accepted" do
      store = MockStore.make()
      key = ustream()

      id1 = Stream.handle("XADD", [key, "100-0", "f", "v"], store)
      assert id1 == "100-0"

      id2 = Stream.handle("XADD", [key, "200-0", "g", "w"], store)
      assert id2 == "200-0"
    end

    test "explicit ID with higher sequence at same ms is accepted" do
      store = MockStore.make()
      key = ustream()

      id1 = Stream.handle("XADD", [key, "100-5", "f", "v"], store)
      assert id1 == "100-5"

      id2 = Stream.handle("XADD", [key, "100-10", "g", "w"], store)
      assert id2 == "100-10"
    end

    test "explicit ID equal to last is rejected" do
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

    test "explicit ID 0-0 on non-empty stream is rejected" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-1", "a", "1"], store)
      result = Stream.handle("XADD", [key, "0-0", "b", "2"], store)

      assert {:error, msg} = result
      assert msg =~ "equal or smaller"
    end

    test "explicit ID below last after XDEL is rejected (monotonicity preserved)" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "100-0", "a", "1"], store)
      Stream.handle("XDEL", [key, "100-0"], store)

      # Stream is empty but last_id was 100-0 -- must still reject IDs <= 100-0.
      result = Stream.handle("XADD", [key, "50-0", "b", "2"], store)
      assert {:error, _} = result

      # Must also reject equal ID.
      result2 = Stream.handle("XADD", [key, "100-0", "c", "3"], store)
      assert {:error, _} = result2

      # ID strictly greater should be accepted.
      id = Stream.handle("XADD", [key, "101-0", "d", "4"], store)
      assert id == "101-0"
    end
  end

  # ===========================================================================
  # Clock-backward handling
  # ===========================================================================

  describe "clock-backward handling" do
    test "auto-ID stays monotonic when last_ms is in the future" do
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

    test "multiple auto-IDs after future explicit ID increment sequence" do
      store = MockStore.make()
      key = ustream()

      future_ms = System.os_time(:millisecond) + 200_000
      Stream.handle("XADD", [key, "#{future_ms}-0", "a", "1"], store)

      id1 = Stream.handle("XADD", [key, "*", "b", "2"], store)
      id2 = Stream.handle("XADD", [key, "*", "c", "3"], store)

      {ms1, seq1} = id1 |> String.split("-") |> Enum.map(&String.to_integer/1) |> List.to_tuple()
      {ms2, seq2} = id2 |> String.split("-") |> Enum.map(&String.to_integer/1) |> List.to_tuple()

      # Both should use future_ms since wall clock is behind, with incrementing sequences.
      assert ms1 == future_ms
      assert seq1 == 1
      assert ms2 == future_ms
      assert seq2 == 2
    end
  end

  # ===========================================================================
  # Partial IDs
  # ===========================================================================

  describe "partial IDs (ms only)" do
    test "partial ID gets sequence 0 when ms is new" do
      store = MockStore.make()
      key = ustream()

      id = Stream.handle("XADD", [key, "500", "f", "v"], store)
      assert id == "500-0"
    end

    test "partial ID at same ms as last increments sequence" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "500-3", "a", "1"], store)

      id = Stream.handle("XADD", [key, "500", "b", "2"], store)
      assert id == "500-4"
    end

    test "partial ID at lower ms than last is rejected" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "500-0", "a", "1"], store)

      result = Stream.handle("XADD", [key, "400", "b", "2"], store)
      assert {:error, msg} = result
      assert msg =~ "equal or smaller"
    end
  end

  # ===========================================================================
  # ID after XTRIM and XDEL
  # ===========================================================================

  describe "ID generation after XTRIM" do
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
             "XTRIM MAXLEN 0 should preserve last-generated-id for monotonicity"
    end

    test "explicit ID <= previous last after XTRIM MAXLEN 0 is rejected" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "500-0", "a", "1"], store)
      Stream.handle("XTRIM", [key, "MAXLEN", "0"], store)

      result = Stream.handle("XADD", [key, "100-0", "b", "2"], store)
      assert {:error, _} = result
    end

    test "XADD after XDEL of all entries preserves monotonicity" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "300-0", "a", "1"], store)
      Stream.handle("XADD", [key, "300-1", "b", "2"], store)
      Stream.handle("XDEL", [key, "300-0", "300-1"], store)

      # Stream is empty, but last_id=300-1 should be preserved.
      result = Stream.handle("XADD", [key, "200-0", "c", "3"], store)
      assert {:error, _} = result

      id = Stream.handle("XADD", [key, "301-0", "d", "4"], store)
      assert id == "301-0"
    end
  end
end
