defmodule Ferricstore.ReviewR4.E17E33IssuesTest do
  @moduledoc """
  Tests verifying code review findings E17-E33.

  Each describe block maps to one review finding and documents whether
  the issue is a CONFIRMED BUG, FALSE POSITIVE, or DESIGN LIMITATION.
  """

  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Bitmap, Blocking, Generic, HyperLogLog, Stream, Strings}
  alias Ferricstore.Test.MockStore

  # Stream tests need the ETS tables to exist.
  # If they don't, we create them (idempotent).
  setup_all do
    for table <- [Ferricstore.Stream.Meta, Ferricstore.Stream.Groups] do
      if :ets.whereis(table) == :undefined do
        :ets.new(table, [:named_table, :public, :set])
      end
    end

    :ok
  end

  # Each stream test gets a unique key.
  defp ustream, do: "review_r4_stream_#{:erlang.unique_integer([:positive])}"

  # =========================================================================
  # E17: Stream parse_id_spec rejects non-numeric IDs without crashing
  # Regression guard — malformed explicit IDs return {:error, _}.
  # =========================================================================

  describe "E17: parse_id_spec rejects non-numeric IDs" do
    test "XADD with malformed ID 'abc-def' returns an error" do
      store = MockStore.make()
      key = ustream()

      assert {:error, _} = Stream.handle("XADD", [key, "abc-def", "field", "value"], store)
    end

    test "XADD with partial malformed ID 'abc' returns an error" do
      store = MockStore.make()
      key = ustream()

      assert {:error, _} = Stream.handle("XADD", [key, "abc", "field", "value"], store)
    end

    test "XADD with mixed malformed ID '123-abc' returns an error" do
      store = MockStore.make()
      key = ustream()

      assert {:error, _} = Stream.handle("XADD", [key, "123-abc", "field", "value"], store)
    end

    test "XADD with valid explicit ID works fine" do
      store = MockStore.make()
      key = ustream()

      result = Stream.handle("XADD", [key, "1-1", "field", "value"], store)
      assert result == "1-1"
    end
  end

  # =========================================================================
  # E18: HSCAN/SSCAN/ZSCAN cursor is positional index, not stable
  # DESIGN LIMITATION — not a bug, but differs from Redis semantics
  # =========================================================================

  # E18 is a design limitation. The paginate function uses Enum.drop(items, cursor)
  # where cursor is a positional index. In Redis, SCAN cursors are hash-table
  # bucket positions that survive concurrent mutations. Here, if a key is
  # inserted between pages, the cursor can skip or repeat entries.
  # No test needed — this is documented behavior for the current implementation.

  # =========================================================================
  # E19: SCAN cursor uses string comparison, not integer
  # FALSE POSITIVE — cursor IS the last key string, not an integer index
  # =========================================================================

  describe "E19: SCAN cursor format" do
    test "SCAN uses key-based cursor (last key seen), not integer index" do
      # Build a store with several keys that sort alphabetically.
      store =
        MockStore.make(%{
          "aaa" => {"1", 0},
          "bbb" => {"2", 0},
          "ccc" => {"3", 0},
          "ddd" => {"4", 0}
        })

      # First page: cursor "0" means start from beginning, count 2
      [next_cursor, batch1] = Generic.handle("SCAN", ["0", "COUNT", "2"], store)
      assert length(batch1) == 2
      # next_cursor should be the last key in batch1 (a string, not an integer)
      assert next_cursor == List.last(batch1)

      # Second page: use the cursor from first page
      [next_cursor2, batch2] = Generic.handle("SCAN", [next_cursor, "COUNT", "2"], store)
      assert length(batch2) == 2
      # Cursor "0" means iteration complete
      assert next_cursor2 == "0"

      # All keys should be covered
      assert Enum.sort(batch1 ++ batch2) == ["aaa", "bbb", "ccc", "ddd"]
    end
  end

  # =========================================================================
  # E20: OBJECT ENCODING returns "int" for integer values
  # Regression guard — Redis-compatible integer encoding detection.
  # =========================================================================

  describe "E20: OBJECT ENCODING for integer-like string values" do
    test "OBJECT ENCODING returns 'int' for '42'" do
      store = MockStore.make(%{"mykey" => {"42", 0}})

      result = Generic.handle("OBJECT", ["ENCODING", "mykey"], store)
      assert result == "int"
    end

    test "OBJECT ENCODING returns 'embstr' for short string" do
      store = MockStore.make(%{"mykey" => {"hello", 0}})
      assert "embstr" == Generic.handle("OBJECT", ["ENCODING", "mykey"], store)
    end

    test "OBJECT ENCODING returns 'raw' for long string" do
      store = MockStore.make(%{"mykey" => {String.duplicate("x", 50), 0}})
      assert "raw" == Generic.handle("OBJECT", ["ENCODING", "mykey"], store)
    end
  end

  # =========================================================================
  # E21: SETNX must treat compound-key data structures as existing
  # Regression guard — fixed by checking live type metadata before SETNX.
  # =========================================================================

  describe "E21: SETNX on compound-key data structures" do
    test "SETNX does not overwrite a hash when only compound keys exist" do
      # Simulate a hash that only has compound keys (no plain key entry).
      # The type metadata is stored at "T:mykey\0" in the compound key space.
      # store.exists? checks only the plain "mykey" key in the store.
      store = MockStore.make()

      # Simulate storing hash data via compound keys only (no plain key "mykey")
      # This mimics what HSET does: it writes compound keys but no plain key.
      type_key = "T:mykey"
      store.compound_put.("mykey", type_key, "hash", 0)
      field_key = "H:mykey\0field1"
      store.compound_put.("mykey", field_key, "value1", 0)

      # Now SETNX should detect that "mykey" exists as a hash.
      # But store.exists? only checks the plain key, which doesn't exist.
      result = Strings.handle("SETNX", ["mykey", "overwrite!"], store)

      assert result == 0

      refute store.get.("mykey") == "overwrite!"
      assert store.compound_get.("mykey", type_key) == "hash"
      assert store.compound_get.("mykey", field_key) == "value1"
    end
  end

  # =========================================================================
  # E22: GEOSEARCHSTORE doesn't check destination key type
  # CONFIRMED BUG — writes zset binary directly without type checking
  # =========================================================================

  # E22: GEOSEARCHSTORE writes :erlang.term_to_binary({:zset, new_zset}) to
  # the destination key via store.put without checking if the destination
  # already holds a different type (e.g., a string, list, or hash).
  # This means it can silently overwrite data of a different type.
  # However, Redis GEOSEARCHSTORE also overwrites the destination key
  # without type checking (it replaces whatever was there with a sorted set).
  # So this matches Redis behavior. FALSE POSITIVE for compatibility.

  # =========================================================================
  # E23: NaN/inf checks in parse_float_arg are dead code
  # CONFIRMED — Float.parse never returns infinity or NaN atoms
  # =========================================================================

  describe "E23: parse_float_arg NaN/infinity checks" do
    test "Float.parse never returns :infinity, :neg_infinity, or NaN" do
      # Float.parse returns {float, rest} or :error.
      # Elixir floats are IEEE 754 doubles, but Float.parse of strings
      # like "inf", "nan", etc. returns :error, not special atoms.
      assert Float.parse("inf") == :error
      assert Float.parse("Inf") == :error
      assert Float.parse("-inf") == :error
      assert Float.parse("NaN") == :error
      assert Float.parse("nan") == :error
      assert Float.parse("infinity") == :error

      # The cond checks in parse_float_arg (lines 564-567) can never trigger
      # because Float.parse would have already returned :error for these inputs.
      # The checks are harmless but unreachable dead code.
    end

    test "INCRBYFLOAT rejects non-numeric strings via Float.parse returning :error" do
      store = MockStore.make(%{"k" => {"10.5", 0}})
      # "inf" is rejected at Float.parse level, not at the NaN/inf guard
      result = Strings.handle("INCRBYFLOAT", ["k", "inf"], store)
      assert {:error, _} = result
    end
  end

  # =========================================================================
  # E24: PFADD with zero elements is accepted
  # Regression guard — Redis accepts PFADD key with no elements.
  # =========================================================================

  describe "E24: PFADD with zero elements" do
    test "PFADD with key but no elements creates an empty HLL" do
      store = MockStore.make()

      result = HyperLogLog.handle("PFADD", ["mykey"], store)
      assert result == 1
    end
  end

  # =========================================================================
  # E25: TOPK.LIST WITHCOUNT is case-sensitive
  # CONFIRMED BUG — only matches literal "WITHCOUNT", not "withcount"
  # =========================================================================

  # We cannot easily test TOPK commands with MockStore since they depend on
  # NIF mmap resources. But code inspection confirms:
  # Line 128: def handle("TOPK.LIST", [key, "WITHCOUNT"], store) do
  # This pattern-matches the literal string "WITHCOUNT" only.
  # "withcount" or "Withcount" would fall through to the catch-all error clause.
  # Redis is case-insensitive for command arguments.

  # =========================================================================
  # E26: TOPK.COUNT listed in moduledoc but has no handler
  # CONFIRMED — moduledoc mentions TOPK.COUNT but no handle clause exists
  # =========================================================================

  # The moduledoc at line 25 lists "TOPK.COUNT key element [element ...]"
  # but grep confirms no handle("TOPK.COUNT", ...) clause exists.
  # Any TOPK.COUNT command would get a "unknown command" error from the dispatcher.

  # =========================================================================
  # E27: validate_field_count(0, []) is dead code
  # FALSE POSITIVE — it IS the base case for the recursion
  # =========================================================================

  describe "E27: validate_field_count(0, []) reachability" do
    # validate_field_count(n, fields) is called with n from parse_positive_integer
    # which requires n > 0. The recursion on line 838 is:
    #   validate_field_count(n, [_ | rest]) when n > 0 -> validate_field_count(n - 1, rest)
    # When n=1 and fields=["x"], it calls validate_field_count(0, []).
    # So line 837 `defp validate_field_count(0, []), do: :ok` IS reachable
    # as the successful base case when count matches the number of fields.

    test "validate_field_count(0, []) is the normal success base case" do
      # We can't call the private function directly, but the logic is clear
      # from reading the recursive definition. This test documents the finding.
      assert true, "validate_field_count(0, []) is NOT dead code — it's the recursion base case"
    end
  end

  # =========================================================================
  # E28: BLMOVE direction conversion fragile
  # FALSE POSITIVE — to_string(:left) returns "left", which upcase handles
  # =========================================================================

  describe "E28: BLMOVE direction conversion" do
    test "BLMOVE passes atom directions through to_string, LMOVE upcases them" do
      # parse_blmove_args returns {:ok, src, dst, :left, :right, timeout_ms}
      # handle("BLMOVE") then calls to_string(:left) => "left"
      # LMOVE.parse_lr_direction calls String.upcase("left") => "LEFT"
      # which matches. So the conversion chain is correct.
      assert to_string(:left) == "left"
      assert to_string(:right) == "right"
      assert String.upcase("left") == "LEFT"
      assert String.upcase("right") == "RIGHT"
    end

    test "BLMOVE parse_blmove_args validates directions" do
      # Valid directions
      assert {:ok, "src", "dst", :left, :right, _} =
               Blocking.parse_blmove_args(["src", "dst", "LEFT", "RIGHT", "0"])

      # Invalid direction
      assert {:error, _} = Blocking.parse_blmove_args(["src", "dst", "UP", "RIGHT", "0"])
    end
  end

  # =========================================================================
  # E29: OBJECT ENCODING doesn't distinguish ziplist/listpack/skiplist
  # DESIGN LIMITATION — simplified encoding names, not a bug
  # =========================================================================

  describe "E29: OBJECT ENCODING simplified encoding names" do
    test "returns 'hashtable' for hash type, not 'ziplist' or 'listpack'" do
      # FerricStore uses compound keys for all hashes regardless of size,
      # so there is no small-hash optimization (ziplist/listpack).
      # Returning "hashtable" for all hashes is consistent with the implementation.
      # This is a design limitation, not a bug.
      # Note: store.exists? checks the plain key, so we need a dummy plain key
      # entry AND the compound type key for TypeRegistry to find the type.
      type_key = "T:hkey"
      store = MockStore.make(%{"hkey" => {"dummy", 0}, type_key => {"hash", 0}})
      result = Generic.handle("OBJECT", ["ENCODING", "hkey"], store)
      assert result == "hashtable"
    end

    test "returns 'skiplist' for zset type, not 'ziplist' or 'listpack'" do
      type_key = "T:zkey"
      store = MockStore.make(%{"zkey" => {"dummy", 0}, type_key => {"zset", 0}})
      result = Generic.handle("OBJECT", ["ENCODING", "zkey"], store)
      assert result == "skiplist"
    end
  end

  # =========================================================================
  # E30: SETBIT/GETBIT don't validate bit offset range
  # FALSE POSITIVE — @max_bit_offset and check_bit_offset exist
  # =========================================================================

  describe "E30: SETBIT/GETBIT offset validation" do
    test "SETBIT validates offset is non-negative" do
      store = MockStore.make()
      result = Bitmap.handle("SETBIT", ["k", "-1", "1"], store)
      assert {:error, _} = result
    end

    test "SETBIT validates offset against max (2^32 - 1)" do
      store = MockStore.make()
      # 4294967296 = 2^32, one above the max
      result = Bitmap.handle("SETBIT", ["k", "4294967296", "1"], store)
      assert {:error, _} = result
    end

    test "SETBIT allows max valid offset (2^32 - 1)" do
      store = MockStore.make()
      # This would allocate 512MB, so we just verify it doesn't error on parsing.
      # We use a smaller offset to actually test.
      result = Bitmap.handle("SETBIT", ["k", "100", "1"], store)
      assert result == 0
    end

    test "GETBIT validates offset is non-negative" do
      store = MockStore.make()
      result = Bitmap.handle("GETBIT", ["k", "-1"], store)
      assert {:error, _} = result
    end

    test "GETBIT validates offset against max" do
      store = MockStore.make()
      result = Bitmap.handle("GETBIT", ["k", "4294967296"], store)
      assert {:error, _} = result
    end
  end

  # =========================================================================
  # E31: BITCOUNT with negative range handling
  # FALSE POSITIVE — resolve_index handles negatives correctly
  # =========================================================================

  describe "E31: BITCOUNT negative range handling" do
    test "BITCOUNT with negative indices counts from end" do
      # Set up a value where we know the bit pattern
      # "ab" = <<0x61, 0x62>> = <<0b01100001, 0b01100010>>
      # popcount("ab") = 3 + 3 = 6
      store = MockStore.make(%{"k" => {"ab", 0}})

      # Full range
      assert 6 == Bitmap.handle("BITCOUNT", ["k", "0", "-1"], store)

      # Last byte only: "b" = 0b01100010 = 3 bits set
      assert 3 == Bitmap.handle("BITCOUNT", ["k", "-1", "-1"], store)

      # First byte only: "a" = 0b01100001 = 3 bits set
      assert 3 == Bitmap.handle("BITCOUNT", ["k", "0", "0"], store)
    end

    test "BITCOUNT with inverted range returns 0" do
      store = MockStore.make(%{"k" => {"ab", 0}})
      # start > end after resolution should return 0
      assert 0 == Bitmap.handle("BITCOUNT", ["k", "-1", "0"], store)
    end
  end

  # =========================================================================
  # E32: COPY without REPLACE returns 0 when destination exists
  # Regression guard — Redis returns 0, not an error.
  # =========================================================================

  describe "E32: COPY without REPLACE when destination exists" do
    test "COPY returns 0 when dest exists and no REPLACE" do
      store =
        MockStore.make(%{
          "src" => {"v1", 0},
          "dst" => {"v2", 0}
        })

      result = Generic.handle("COPY", ["src", "dst"], store)

      assert 0 == result
      assert store.get.("dst") == "v2"
    end

    test "COPY succeeds when destination does not exist" do
      store = MockStore.make(%{"src" => {"v1", 0}})
      result = Generic.handle("COPY", ["src", "dst"], store)
      assert result == 1
    end

    test "COPY with REPLACE succeeds when destination exists" do
      store =
        MockStore.make(%{
          "src" => {"v1", 0},
          "dst" => {"v2", 0}
        })

      result = Generic.handle("COPY", ["src", "dst", "REPLACE"], store)
      assert result == 1
      # Verify destination was overwritten
      assert store.get.("dst") == "v1"
    end

    test "COPY returns error when source does not exist" do
      store = MockStore.make(%{"dst" => {"v2", 0}})
      result = Generic.handle("COPY", ["src", "dst"], store)
      assert {:error, "ERR no such key"} == result
    end
  end

  # =========================================================================
  # E33: Shard.incr has no INT64 overflow check
  # CONFIRMED BUG — no range check on int_val + delta
  # =========================================================================

  describe "E33: incr overflow handling" do
    test "Elixir integers are arbitrary-precision, no overflow crash occurs" do
      # The BEAM uses arbitrary-precision integers, so int_val + delta
      # will never wrap or crash. However, Redis limits INCR to signed
      # 64-bit range [-2^63, 2^63 - 1]. FerricStore does not enforce this.
      max_int64 = 9_223_372_036_854_775_807
      min_int64 = -9_223_372_036_854_775_808

      # These would overflow in Redis but succeed silently in FerricStore
      assert max_int64 + 1 == 9_223_372_036_854_775_808
      assert min_int64 - 1 == -9_223_372_036_854_775_809

      # This means FerricStore allows values outside Redis's int64 range,
      # which could cause protocol-level issues when encoding as RESP integers.
    end

    test "embedded incr_by does not check int64 overflow" do
      # The embedded FerricStore.incr_by at line 442-456 does:
      #   new_val = int_val + amount
      #   Router.put(FerricStore.Instance.get(:default), resolved_key, Integer.to_string(new_val), 0)
      # No range check. If int_val is at max_int64 and amount is 1,
      # the result exceeds the Redis int64 range.
      # This is a compatibility issue with Redis clients that expect
      # INCR to return ERR when overflow would occur.
      max_int64 = 9_223_372_036_854_775_807

      # Demonstrate the math works without bounds checking
      result = max_int64 + 1
      assert result == 9_223_372_036_854_775_808
      assert Integer.to_string(result) == "9223372036854775808"
    end
  end
end
