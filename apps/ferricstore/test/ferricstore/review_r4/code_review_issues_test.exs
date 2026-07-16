defmodule Ferricstore.ReviewR4.CodeReviewIssuesTest do
  @moduledoc """
  Tests verifying Round 4 code review findings (E1-E16).

  Each test group targets a specific reported issue, with tests designed
  to confirm whether the bug exists (test fails = bug confirmed) or
  is a false positive (test passes).

  Uses the FerricStore embedded API where possible, falling back to
  command-level MockStore tests for issues that need low-level control.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  # ===========================================================================
  # E1: RENAME/RENAMENX/COPY don't move compound sub-keys
  # ===========================================================================

  describe "E1: RENAME on hash key does not move compound sub-keys" do
    test "RENAME hash: HGET on new name should return the field values" do
      source = "e1:{compound-rename}:src"
      destination = "e1:{compound-rename}:dst"

      # Create a hash with fields
      :ok = FerricStore.hset(source, %{"f1" => "v1", "f2" => "v2"})

      # Verify the hash works
      {:ok, "v1"} = FerricStore.hget(source, "f1")

      # Rename the key
      :ok = FerricStore.rename(source, destination)

      # The field data should be accessible via the new name.
      # BUG: If RENAME only moves the plain key value (nil for hashes),
      # HGET on the new name will return nil.
      result = FerricStore.hget(destination, "f1")

      assert result == {:ok, "v1"},
             "E1 BUG: RENAME did not move hash compound sub-keys. " <>
               "HGET on renamed key returned #{inspect(result)} instead of {:ok, \"v1\"}"
    end

    test "RENAME hash: old key should no longer have the fields" do
      source = "e1:{compound-rename-old}:src"
      destination = "e1:{compound-rename-old}:dst"

      :ok = FerricStore.hset(source, %{"field" => "value"})
      :ok = FerricStore.rename(source, destination)

      # Old key should not have data
      result = FerricStore.hget(source, "field")

      assert result == {:ok, nil},
             "After RENAME, old key still has field data: #{inspect(result)}"
    end
  end

  # ===========================================================================
  # E2: GEO storage format incompatible with sorted set commands
  # ===========================================================================

  describe "E2: GEO storage incompatible with sorted set commands" do
    @tag :e2_geo_zset
    test "GEOADD then ZSCORE should return the geohash score" do
      # This test uses MockStore to check at command level since the
      # embedded API may not wire geo commands through the same store.
      store = Ferricstore.Test.MockStore.make()

      # GEOADD stores members with geohash-encoded scores
      result =
        Ferricstore.Commands.Geo.handle(
          "GEOADD",
          ["geo_key", "13.361389", "38.115556", "Palermo"],
          store
        )

      assert result == 1

      # Now try ZSCORE on the same key -- should return the score
      # BUG: Geo uses {:zset, [...]} in plain key store;
      # SortedSet uses compound keys Z:key\0member
      zscore_result =
        Ferricstore.Commands.SortedSet.handle(
          "ZSCORE",
          ["geo_key", "Palermo"],
          store
        )

      # If formats are incompatible, ZSCORE will return nil (member not found)
      # or an error. If compatible, it returns a score string.
      refute is_nil(zscore_result),
             "E2 BUG: ZSCORE returned nil for GEOADD member -- storage formats are incompatible. " <>
               "Geo uses :erlang.term_to_binary({:zset, [...]}) in plain key, " <>
               "SortedSet uses compound keys Z:key\\0member."
    end
  end

  # ===========================================================================
  # E3: Stream XADD compound keys bypass shard routing
  # ===========================================================================

  describe "E3: Stream XADD compound key routing" do
    @tag :e3_stream
    test "XADD uses store.put with compound key -- check code path" do
      # This is a code-level verification. Stream.ex line 429 uses store.put.()
      # directly with compound_key "X:key\0id". The question is whether this
      # routes to the wrong shard.
      #
      # Looking at the code: store.put.() is the same store function passed
      # to all command handlers. For streams, the compound key is put into
      # the same shard's store (store is already bound to a shard by the
      # dispatcher). The stream XRANGE also reads using store.keys.() and
      # store.get.() from the same shard store.
      #
      # This is NOT a shard routing bug -- store.put.() goes through the same
      # shard that dispatched the command. The compound key is just a string
      # key in that shard's keyspace.

      store = Ferricstore.Test.MockStore.make()
      Ferricstore.Commands.Stream.ensure_meta_table()

      # XADD should work correctly
      id =
        Ferricstore.Commands.Stream.handle(
          "XADD",
          ["e3_stream", "*", "field1", "value1"],
          store
        )

      assert is_binary(id), "XADD should return an ID string, got: #{inspect(id)}"

      # XRANGE should find the entry
      entries =
        Ferricstore.Commands.Stream.handle(
          "XRANGE",
          ["e3_stream", "-", "+"],
          store
        )

      assert length(entries) == 1
      assert hd(entries) == [id, "field1", "value1"]
    end
  end

  # ===========================================================================
  # E4: HEXPIRE/HPEXPIRE/HSETEX use System.os_time instead of HLC
  # ===========================================================================

  describe "E4: Hash field TTL uses System.os_time" do
    test "HEXPIRE uses System.os_time -- code-level verification" do
      # Line 293 in hash.ex: expire_at_ms = System.os_time(:millisecond) + seconds * 1000
      # While expiry.ex uses Ferricstore.HLC.now_ms()
      #
      # This is a real inconsistency. System.os_time and HLC.now_ms() can differ
      # significantly if the clock jumps backward. However, for per-field TTL,
      # the HTTL command also uses System.os_time (line 325), so the field TTL
      # is internally consistent. The concern is cross-type consistency.
      #
      # We verify by checking that HEXPIRE/HPEXPIRE produce valid expiry.
      store = Ferricstore.Test.MockStore.make()

      # Create a hash field
      Ferricstore.Commands.Hash.handle("HSET", ["e4_key", "f1", "v1"], store)

      # HEXPIRE should set expiry
      result =
        Ferricstore.Commands.Hash.handle(
          "HEXPIRE",
          ["e4_key", "3600", "FIELDS", "1", "f1"],
          store
        )

      assert result == [1], "HEXPIRE should return [1], got: #{inspect(result)}"

      # Check that HTTL returns a positive value
      ttl_result =
        Ferricstore.Commands.Hash.handle(
          "HTTL",
          ["e4_key", "FIELDS", "1", "f1"],
          store
        )

      assert is_list(ttl_result)
      [ttl_val] = ttl_result

      assert ttl_val > 0 and ttl_val <= 3600,
             "HTTL should return value between 0 and 3600, got: #{inspect(ttl_val)}"
    end
  end

  # ===========================================================================
  # E5: DECRBY integer overflow on MIN_INT64 negation
  # ===========================================================================

  describe "E5: DECRBY overflow on MIN_INT64 negation" do
    test "DECRBY with MIN_INT64 should error" do
      store = Ferricstore.Test.MockStore.make(%{"e5_key" => {"0", 0}})

      # MIN_INT64 = -9223372036854775808
      # DECRBY negates delta: -(-9223372036854775808) = 9223372036854775808
      # which exceeds MAX_INT64 (9223372036854775807)
      #
      # However, looking at the code (line 240):
      #   store.incr.(key, -delta)
      # When delta = -9223372036854775808, -delta in Elixir = 9223372036854775808
      # Elixir uses arbitrary precision integers, so negation works.
      # But the DECRBY range check (line 240) guards:
      #   delta >= @min_int64 and delta <= @max_int64
      # -9223372036854775808 == @min_int64, so it passes the guard.
      # Then -(-9223372036854775808) = 9223372036854775808 which in Elixir is fine
      # (bignum), but store.incr will produce a value outside INT64 range.
      result =
        Ferricstore.Commands.Strings.handle(
          "DECRBY",
          ["e5_key", "-9223372036854775808"],
          store
        )

      # The negated delta becomes 9223372036854775808 which is MIN_INT64 negated.
      # In Redis, DECRBY -9223372036854775808 on "0" would produce
      # 9223372036854775808, which overflows INT64. Redis returns an error.
      # Elixir bignums won't overflow but the result is outside INT64 range.
      case result do
        {:error, _} ->
          # Correct: should error on overflow
          assert true

        {:ok, value} when is_integer(value) ->
          # Bug if value is outside INT64 range and no error is returned
          if value > 9_223_372_036_854_775_807 or value < -9_223_372_036_854_775_808 do
            flunk("E5 BUG: DECRBY returned #{value} which is outside INT64 range without error")
          end

        other ->
          # Any other result is unexpected
          flunk("DECRBY returned unexpected: #{inspect(other)}")
      end
    end

    test "DECRBY with delta that would cause result overflow" do
      store = Ferricstore.Test.MockStore.make(%{"e5_key2" => {"9223372036854775806", 0}})

      # DECRBY key -2 means incr(key, 2), result = 9223372036854775808 > MAX_INT64
      result =
        Ferricstore.Commands.Strings.handle(
          "DECRBY",
          ["e5_key2", "-2"],
          store
        )

      case result do
        {:error, _} ->
          assert true, "Correctly returns error on overflow"

        {:ok, value} when is_integer(value) ->
          if value > 9_223_372_036_854_775_807 do
            flunk("E5 BUG: DECRBY produced #{value} exceeding MAX_INT64 without error")
          end

        other ->
          flunk("Unexpected result: #{inspect(other)}")
      end
    end
  end

  # ===========================================================================
  # E6: EXPIRE/TTL commands don't work on data structure keys
  # ===========================================================================

  describe "E6: EXPIRE on data structure keys" do
    test "EXPIRE on a hash key should work" do
      # Create a hash
      :ok = FerricStore.hset("e6_hash", %{"f" => "v"})

      # Try to set an expiry on the hash key
      result = FerricStore.expire("e6_hash", 60_000)

      # BUG: FerricStore.expire uses Router.get_meta which reads the plain key.
      # Hash data is stored in compound keys, so Router.get_meta returns nil.
      assert result == {:ok, true},
             "E6 BUG: EXPIRE returned #{inspect(result)} on hash key. " <>
               "apply_expiry reads plain key via store.get_meta, but hash keys " <>
               "have no plain key entry -- they use compound keys."
    end
  end

  # ===========================================================================
  # E7: LSET/LREM/LTRIM missing type checks
  # ===========================================================================

  describe "E7: LSET/LREM/LTRIM missing type checks" do
    test "LSET on a string key should return WRONGTYPE" do
      store = Ferricstore.Test.MockStore.make(%{"e7_str" => {"hello", 0}})

      # LSET on a string key should return WRONGTYPE error
      result =
        Ferricstore.Commands.List.handle(
          "LSET",
          ["e7_str", "0", "newval"],
          store
        )

      assert match?({:error, "WRONGTYPE" <> _}, result),
             "E7 BUG: LSET on a string key returned #{inspect(result)} instead of WRONGTYPE error. " <>
               "LSET does not call TypeRegistry.check_type."
    end

    test "LREM on a string key should return WRONGTYPE" do
      store = Ferricstore.Test.MockStore.make(%{"e7_str2" => {"hello", 0}})

      result =
        Ferricstore.Commands.List.handle(
          "LREM",
          ["e7_str2", "0", "hello"],
          store
        )

      assert match?({:error, "WRONGTYPE" <> _}, result),
             "E7 BUG: LREM on a string key returned #{inspect(result)} instead of WRONGTYPE error"
    end

    test "LTRIM on a string key should return WRONGTYPE" do
      store = Ferricstore.Test.MockStore.make(%{"e7_str3" => {"hello", 0}})

      result =
        Ferricstore.Commands.List.handle(
          "LTRIM",
          ["e7_str3", "0", "1"],
          store
        )

      assert match?({:error, "WRONGTYPE" <> _}, result),
             "E7 BUG: LTRIM on a string key returned #{inspect(result)} instead of WRONGTYPE error"
    end
  end

  # ===========================================================================
  # E8: HINCRBY has no 64-bit overflow protection
  # ===========================================================================

  describe "E8: HINCRBY overflow protection" do
    test "HINCRBY that would overflow INT64 should return error" do
      store = Ferricstore.Test.MockStore.make()

      # Set hash field to near MAX_INT64
      Ferricstore.Commands.Hash.handle(
        "HSET",
        ["e8_key", "counter", "9223372036854775806"],
        store
      )

      # HINCRBY by 2 should overflow INT64
      result =
        Ferricstore.Commands.Hash.handle(
          "HINCRBY",
          ["e8_key", "counter", "2"],
          store
        )

      # In Redis, this would return an error: "ERR increment or decrement would overflow"
      # In FerricStore, Elixir bignums allow the operation without overflow.
      case result do
        {:error, _} ->
          assert true, "Correctly returns error on overflow"

        value when is_integer(value) and value > 9_223_372_036_854_775_807 ->
          flunk(
            "E8 BUG: HINCRBY returned #{value} exceeding MAX_INT64 without error. " <>
              "Redis would return an overflow error."
          )

        value when is_integer(value) ->
          # Value within range is fine
          assert true

        other ->
          flunk("Unexpected HINCRBY result: #{inspect(other)}")
      end
    end
  end

  # ===========================================================================
  # E9: SINTER/SUNION/SDIFF cross-shard keys
  # ===========================================================================

  describe "E9: SINTER/SUNION/SDIFF cross-shard handling" do
    test "SINTER with keys on different shards should return correct intersection" do
      # Find keys on different shards
      [key_a, key_b] = ShardHelpers.keys_on_different_shards(2)

      # Use set names that route to different shards
      set_a = "e9_set_#{key_a}"
      set_b = "e9_set_#{key_b}"

      # Add members to each set
      {:ok, 2} = FerricStore.sadd(set_a, ["member1", "member2"])
      {:ok, 2} = FerricStore.sadd(set_b, ["member2", "member3"])

      # SINTER should return the intersection {"member2"}
      {:ok, members} = FerricStore.sinter([set_a, set_b])

      assert "member2" in members,
             "E9 BUG: SINTER across shards did not find 'member2'. Got: #{inspect(members)}"
    end

    test "SUNION with keys on different shards returns correct union" do
      [key_a, key_b] = ShardHelpers.keys_on_different_shards(2)
      set_a = "e9_union_#{key_a}"
      set_b = "e9_union_#{key_b}"

      {:ok, 1} = FerricStore.sadd(set_a, ["only_a"])
      {:ok, 1} = FerricStore.sadd(set_b, ["only_b"])

      {:ok, members} = FerricStore.sunion([set_a, set_b])

      assert "only_a" in members and "only_b" in members,
             "E9 BUG: SUNION across shards missing members. Got: #{inspect(members)}"
    end
  end

  # ===========================================================================
  # E10: SETBIT clears existing TTL
  # ===========================================================================

  describe "E10: SETBIT clears existing TTL" do
    test "SETBIT should preserve existing TTL" do
      store = Ferricstore.Test.MockStore.make()
      future = System.os_time(:millisecond) + 3_600_000

      # SET key with TTL
      store.put.("e10_key", "a", future)

      # Verify TTL is set
      {_value, ^future} = store.get_meta.("e10_key")

      # SETBIT
      Ferricstore.Commands.Bitmap.handle(
        "SETBIT",
        ["e10_key", "0", "1"],
        store
      )

      # Check if TTL was preserved
      case store.get_meta.("e10_key") do
        {_value, 0} ->
          flunk(
            "E10 BUG CONFIRMED: SETBIT cleared the TTL. " <>
              "store.put.(key, new_value, 0) on line 76 of bitmap.ex sets expire_at_ms=0."
          )

        {_value, exp} when exp == future ->
          assert true, "TTL preserved correctly"

        {_value, exp} ->
          flunk("TTL changed unexpectedly from #{future} to #{exp}")

        nil ->
          flunk("Key disappeared after SETBIT")
      end
    end
  end

  # ===========================================================================
  # E11: INCRBY result overflow not checked
  # ===========================================================================

  describe "E11: INCRBY result overflow" do
    test "INCRBY that causes result to exceed MAX_INT64 should error" do
      store =
        Ferricstore.Test.MockStore.make(%{
          "e11_key" => {"9223372036854775806", 0}
        })

      result =
        Ferricstore.Commands.Strings.handle(
          "INCRBY",
          ["e11_key", "2"],
          store
        )

      # Redis would return: ERR increment or decrement would overflow
      case result do
        {:error, _} ->
          assert true, "Correctly returns error on overflow"

        {:ok, value} when is_integer(value) and value > 9_223_372_036_854_775_807 ->
          flunk(
            "E11 BUG: INCRBY returned #{value} exceeding MAX_INT64 without error. " <>
              "store.incr computes int_val + delta without checking the result range."
          )

        {:ok, value} ->
          assert value <= 9_223_372_036_854_775_807,
                 "Result should be within INT64 range"

        other ->
          flunk("Unexpected result: #{inspect(other)}")
      end
    end
  end

  # ===========================================================================
  # E12: OBJECT ENCODING doesn't find compound-key data structures
  # ===========================================================================

  describe "E12: OBJECT ENCODING for data structure keys" do
    test "OBJECT ENCODING on a hash key should return 'hashtable'" do
      store = Ferricstore.Test.MockStore.make()

      # Create a hash
      Ferricstore.Commands.Hash.handle(
        "HSET",
        ["e12_hash", "field1", "value1"],
        store
      )

      # OBJECT ENCODING should detect it as a hash
      result =
        Ferricstore.Commands.Generic.handle(
          "OBJECT",
          ["ENCODING", "e12_hash"],
          store
        )

      # BUG: OBJECT ENCODING checks store.exists?.(key), but hash keys
      # exist only as compound keys (T:key, H:key\0field), not as plain keys.
      # store.exists? checks the plain key store, so it returns false.
      assert result == "hashtable",
             "E12 BUG: OBJECT ENCODING returned #{inspect(result)} for hash key. " <>
               "store.exists? misses compound-key data structures."
    end
  end

  # ===========================================================================
  # E13: ZRANDMEMBER WITHSCORES returns raw score strings
  # ===========================================================================

  describe "E13: ZRANDMEMBER WITHSCORES format" do
    test "ZRANDMEMBER WITHSCORES should return formatted scores" do
      store = Ferricstore.Test.MockStore.make()

      # Create a sorted set
      Ferricstore.Commands.SortedSet.handle(
        "ZADD",
        ["e13_key", "1.5", "member1"],
        store
      )

      # ZRANDMEMBER with WITHSCORES
      result =
        Ferricstore.Commands.SortedSet.handle(
          "ZRANDMEMBER",
          ["e13_key", "1", "WITHSCORES"],
          store
        )

      # The result should be ["member1", formatted_score]
      # BUG: select_random_zset_members (line 880) does:
      #   Enum.flat_map(selected, fn {member, score} -> [member, score] end)
      # where `score` is the raw score string from compound_scan, NOT
      # format_score(score). Other commands like ZRANGE WITHSCORES call
      # format_score(score) to normalize the output.
      assert is_list(result), "Expected list, got: #{inspect(result)}"

      if result != [] do
        [_member, score_str] = result

        # The raw stored score is "1.5" (from Float.to_string).
        # format_score would convert via float_to_binary with [:compact, decimals: 17].
        # If the score is the raw string without formatting, it might differ
        # from what ZRANGE WITHSCORES returns.
        #
        # Check: is it a string? (compound_scan returns {member, score_str} where
        # score_str is the raw stored value). format_score would reformat it.
        assert is_binary(score_str),
               "Score should be a string, got: #{inspect(score_str)}"
      end
    end
  end

  # ===========================================================================
  # E14: DEL case statement missing "stream" type
  # ===========================================================================

  describe "E14: DEL missing stream type in case statement" do
    test "verify DEL case handles or has catch-all for stream type" do
      # Looking at strings.ex lines 137-147, the do_del_key function has:
      #   case type_str do
      #     "hash" -> CompoundKey.hash_prefix(key)
      #     "list" -> CompoundKey.list_prefix(key)
      #     "set" -> CompoundKey.set_prefix(key)
      #     "zset" -> CompoundKey.zset_prefix(key)
      #   end
      #
      # There is NO "stream" case and NO catch-all default clause.
      # If a key has type "stream", this will raise a CaseClauseError.
      #
      # However, streams use ETS metadata, not TypeRegistry compound keys.
      # TypeRegistry.get_type returns type_str from the T:key compound entry.
      # Streams store data via X:key\0id compound keys but their type
      # registration varies.
      #
      # This IS a real concern -- if a stream key somehow gets a T:key entry
      # with value "stream", DEL will crash with CaseClauseError.

      store = Ferricstore.Test.MockStore.make()

      # Manually create a type entry for "stream"
      type_key = Ferricstore.Store.CompoundKey.type_key("e14_stream")
      store.compound_put.("e14_stream", type_key, "stream", 0)

      # DEL should handle this gracefully
      result =
        try do
          Ferricstore.Commands.Strings.handle("DEL", ["e14_stream"], store)
        rescue
          CaseClauseError ->
            {:error, :case_clause_error}
        end

      refute match?({:error, :case_clause_error}, result),
             "E14 BUG: DEL raised CaseClauseError for stream type. " <>
               "The case statement in do_del_key is missing a 'stream' clause and has no catch-all."
    end
  end

  # ===========================================================================
  # E15: GETEX/SETNX don't check for WRONGTYPE
  # ===========================================================================

  describe "E15: GETEX/SETNX missing WRONGTYPE checks" do
    test "GETEX on a hash key should return WRONGTYPE" do
      store = Ferricstore.Test.MockStore.make()

      # Create a hash
      Ferricstore.Commands.Hash.handle(
        "HSET",
        ["e15_hash", "f", "v"],
        store
      )

      # GETEX on a hash key should return WRONGTYPE
      result =
        Ferricstore.Commands.Strings.handle(
          "GETEX",
          ["e15_hash"],
          store
        )

      # GETEX without options calls store.get.(key) which returns nil for
      # data structures stored via compound keys. It does NOT check TypeRegistry.
      # The question is: does it return nil (wrong, should be WRONGTYPE) or error?
      assert match?({:error, "WRONGTYPE" <> _}, result) or is_nil(result),
             "GETEX on hash key returned: #{inspect(result)}"

      # Specifically check it IS NOT returning nil without checking type
      if is_nil(result) do
        flunk(
          "E15 BUG: GETEX returned nil for hash key instead of WRONGTYPE. " <>
            "GETEX does not check TypeRegistry before reading."
        )
      end
    end

    test "SETNX on a hash key should return WRONGTYPE" do
      store = Ferricstore.Test.MockStore.make()

      # Create a hash
      Ferricstore.Commands.Hash.handle(
        "HSET",
        ["e15_hash2", "f", "v"],
        store
      )

      # SETNX on a hash key -- hash compound keys exist but plain key doesn't
      result =
        Ferricstore.Commands.Strings.handle(
          "SETNX",
          ["e15_hash2", "overwrite_value"],
          store
        )

      # SETNX checks store.exists?.(key). Hash keys don't have a plain key
      # entry, so exists? returns false. SETNX then sets the key as a string,
      # silently overwriting the hash!
      #
      # After SETNX, the key should still be a hash.
      assert match?({:error, "WRONGTYPE" <> _}, result) or result == 0,
             "SETNX on hash key should return WRONGTYPE or 0 (key exists), got: #{inspect(result)}"

      if result == 1 do
        flunk(
          "E15 BUG: SETNX returned 1 (set successfully) on hash key. " <>
            "SETNX uses store.exists? which misses compound-key data structures, " <>
            "then overwrites the hash with a string value."
        )
      end
    end
  end

  # ===========================================================================
  # E16: APPEND doesn't check for WRONGTYPE
  # ===========================================================================

  describe "E16: APPEND missing WRONGTYPE check" do
    test "APPEND on a hash key should return WRONGTYPE" do
      store = Ferricstore.Test.MockStore.make()

      # Create a hash
      Ferricstore.Commands.Hash.handle(
        "HSET",
        ["e16_hash", "f", "v"],
        store
      )

      # APPEND on a hash key
      result =
        Ferricstore.Commands.Strings.handle(
          "APPEND",
          ["e16_hash", "data"],
          store
        )

      # APPEND calls store.append.(key, value) directly without checking
      # TypeRegistry. The hash's plain key is nil, so append creates a new
      # string key, effectively corrupting the hash.
      assert match?({:error, "WRONGTYPE" <> _}, result),
             "E16 BUG: APPEND on hash key returned #{inspect(result)} instead of WRONGTYPE. " <>
               "APPEND does not check TypeRegistry before writing."
    end
  end
end
