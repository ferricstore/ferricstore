defmodule Ferricstore.Commands.RedisCompatHashTest do
  @moduledoc """
  Redis compatibility tests for data structure edge cases,
  extracted from RedisCompatTest.

  Covers: HSCAN/SSCAN/ZSCAN correctness, sorted set edge cases,
  set operation edge cases, hash edge cases, list edge cases.
  """

  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Hash, List, Set, SortedSet}
  alias Ferricstore.Test.MockStore

  # ===========================================================================
  # HASH/SET/ZSET SCAN CORRECTNESS
  #
  # HSCAN/SSCAN/ZSCAN should iterate correctly and return cursor 0 when done.
  # ===========================================================================

  describe "HSCAN correctness" do
    test "HSCAN iterates all fields" do
      store = MockStore.make()

      for i <- 1..15 do
        Hash.handle("HSET", ["myhash", "field#{i}", "val#{i}"], store)
      end

      # Iterate with small count
      fields = hscan_all(store, "myhash", "0", 3)
      assert length(Enum.uniq(fields)) == 15
    end

    test "HSCAN with MATCH pattern" do
      store = MockStore.make()

      Hash.handle("HSET", ["h", "name", "alice", "age", "30", "city", "nyc"], store)

      [cursor, elements] = Hash.handle("HSCAN", ["h", "0", "MATCH", "a*", "COUNT", "100"], store)
      assert cursor == "0"

      # Elements come as [field1, val1, field2, val2, ...]
      field_names = for i <- 0..(div(length(elements), 2) - 1), do: Enum.at(elements, i * 2)
      assert "age" in field_names
      refute "name" in field_names
      refute "city" in field_names
    end

    test "HSCAN on empty hash returns cursor 0 and empty list" do
      store = MockStore.make()

      [cursor, elements] = Hash.handle("HSCAN", ["nosuchhash", "0"], store)
      assert cursor == "0"
      assert elements == []
    end
  end

  describe "SSCAN correctness" do
    test "SSCAN iterates all members" do
      store = MockStore.make()

      for i <- 1..12 do
        Set.handle("SADD", ["myset", "member#{i}"], store)
      end

      members = sscan_all(store, "myset", "0", 3)
      assert length(Enum.uniq(members)) == 12
    end

    test "SSCAN on empty set returns cursor 0 and empty list" do
      store = MockStore.make()

      [cursor, members] = Set.handle("SSCAN", ["nosuchset", "0"], store)
      assert cursor == "0"
      assert members == []
    end
  end

  describe "ZSCAN correctness" do
    test "ZSCAN iterates all members with scores" do
      store = MockStore.make()

      for i <- 1..10 do
        SortedSet.handle("ZADD", ["myzset", "#{i}.0", "member#{i}"], store)
      end

      {members, _scores} = zscan_all(store, "myzset", "0", 3)
      assert length(Enum.uniq(members)) == 10
    end

    test "ZSCAN on empty sorted set returns cursor 0 and empty list" do
      store = MockStore.make()

      [cursor, elements] = SortedSet.handle("ZSCAN", ["nosuchzset", "0"], store)
      assert cursor == "0"
      assert elements == []
    end
  end

  # ===========================================================================
  # SORTED SET EDGE CASES
  #
  # Score parsing, NX/XX/GT/LT flags, +inf/-inf handling.
  # ===========================================================================

  describe "sorted set edge cases" do
    test "ZADD NX does not update existing members" do
      store = MockStore.make()

      SortedSet.handle("ZADD", ["z", "1.0", "a"], store)
      result = SortedSet.handle("ZADD", ["z", "NX", "5.0", "a"], store)

      # NX: only add new elements, don't update
      assert result == 0

      # Score should still be 1.0
      assert "1.0" = SortedSet.handle("ZSCORE", ["z", "a"], store)
    end

    test "ZADD XX does not add new members" do
      store = MockStore.make()

      result = SortedSet.handle("ZADD", ["z", "XX", "1.0", "newmember"], store)
      assert result == 0

      # Member should not exist
      assert nil == SortedSet.handle("ZSCORE", ["z", "newmember"], store)
    end

    test "ZADD GT only updates if new score is greater" do
      store = MockStore.make()

      SortedSet.handle("ZADD", ["z", "5.0", "a"], store)

      # Try to set lower score with GT
      SortedSet.handle("ZADD", ["z", "GT", "3.0", "a"], store)
      assert "5.0" = SortedSet.handle("ZSCORE", ["z", "a"], store)

      # Try to set higher score with GT
      SortedSet.handle("ZADD", ["z", "GT", "10.0", "a"], store)
      assert "10.0" = SortedSet.handle("ZSCORE", ["z", "a"], store)
    end

    test "ZADD LT only updates if new score is lower" do
      store = MockStore.make()

      SortedSet.handle("ZADD", ["z", "5.0", "a"], store)

      # Try to set higher score with LT
      SortedSet.handle("ZADD", ["z", "LT", "10.0", "a"], store)
      assert "5.0" = SortedSet.handle("ZSCORE", ["z", "a"], store)

      # Try to set lower score with LT
      SortedSet.handle("ZADD", ["z", "LT", "1.0", "a"], store)
      assert "1.0" = SortedSet.handle("ZSCORE", ["z", "a"], store)
    end

    test "ZADD CH flag changes return value to count of changed elements" do
      store = MockStore.make()

      SortedSet.handle("ZADD", ["z", "1.0", "a"], store)

      # CH: return count of changed (added + updated) instead of just added
      result = SortedSet.handle("ZADD", ["z", "CH", "2.0", "a", "3.0", "b"], store)
      # "a" updated (score changed 1.0 -> 2.0) + "b" added = 2
      assert result == 2
    end

    test "ZCOUNT with +inf and -inf" do
      store = MockStore.make()

      SortedSet.handle("ZADD", ["z", "1.0", "a", "2.0", "b", "3.0", "c"], store)

      assert 3 = SortedSet.handle("ZCOUNT", ["z", "-inf", "+inf"], store)
      assert 2 = SortedSet.handle("ZCOUNT", ["z", "1.5", "+inf"], store)
      assert 2 = SortedSet.handle("ZCOUNT", ["z", "-inf", "2.0"], store)
    end

    test "ZCOUNT with exclusive bounds" do
      store = MockStore.make()

      SortedSet.handle("ZADD", ["z", "1.0", "a", "2.0", "b", "3.0", "c"], store)

      # (1 to (3 means > 1 and < 3, so only "b" (score 2.0)
      assert 1 = SortedSet.handle("ZCOUNT", ["z", "(1", "(3"], store)
    end

    test "ZPOPMIN on empty sorted set returns empty list" do
      store = MockStore.make()

      result = SortedSet.handle("ZPOPMIN", ["nosuchzset"], store)
      assert result == []
    end

    test "ZPOPMAX on empty sorted set returns empty list" do
      store = MockStore.make()

      result = SortedSet.handle("ZPOPMAX", ["nosuchzset"], store)
      assert result == []
    end
  end

  # ===========================================================================
  # SET OPERATION EDGE CASES
  # ===========================================================================

  describe "set operation edge cases" do
    test "SINTER with missing key returns empty set" do
      store = MockStore.make()

      Set.handle("SADD", ["s1", "a", "b", "c"], store)

      result = Set.handle("SINTER", ["s1", "nosuchkey"], store)
      assert result == []
    end

    test "SUNION with missing key returns the existing set" do
      store = MockStore.make()

      Set.handle("SADD", ["s1", "a", "b"], store)

      result = Set.handle("SUNION", ["s1", "nosuchkey"], store)
      assert Enum.sort(result) == ["a", "b"]
    end

    test "SDIFF with missing subtrahend returns the entire first set" do
      store = MockStore.make()

      Set.handle("SADD", ["s1", "a", "b", "c"], store)

      result = Set.handle("SDIFF", ["s1", "nosuchkey"], store)
      assert Enum.sort(result) == ["a", "b", "c"]
    end

    test "SMOVE with missing source member returns 0" do
      store = MockStore.make()

      Set.handle("SADD", ["src", "a"], store)
      Set.handle("SADD", ["dst", "b"], store)

      assert 0 = Set.handle("SMOVE", ["src", "dst", "nonexistent"], store)
    end

    test "SPOP count larger than set size returns all members" do
      store = MockStore.make()

      Set.handle("SADD", ["s", "a", "b", "c"], store)

      result = Set.handle("SPOP", ["s", "100"], store)
      assert length(result) == 3
      assert Enum.sort(result) == ["a", "b", "c"]
    end

    test "SRANDMEMBER with negative count allows duplicates" do
      store = MockStore.make()

      Set.handle("SADD", ["s", "only"], store)

      # Negative count: return abs(count) elements, may repeat
      result = Set.handle("SRANDMEMBER", ["s", "-5"], store)
      assert length(result) == 5
      assert Enum.all?(result, &(&1 == "only"))
    end
  end

  # ===========================================================================
  # HASH EDGE CASES
  # ===========================================================================

  describe "hash edge cases" do
    test "HINCRBY on non-integer field returns error" do
      store = MockStore.make()

      Hash.handle("HSET", ["h", "name", "alice"], store)

      result = Hash.handle("HINCRBY", ["h", "name", "1"], store)
      assert {:error, "ERR hash value is not an integer"} = result
    end

    test "HINCRBYFLOAT on non-numeric field returns error" do
      store = MockStore.make()

      Hash.handle("HSET", ["h", "name", "alice"], store)

      result = Hash.handle("HINCRBYFLOAT", ["h", "name", "1.5"], store)
      assert {:error, "ERR hash value is not a valid float"} = result
    end

    test "HINCRBY initializes missing field to 0 before incrementing" do
      store = MockStore.make()

      # HINCRBY on missing field in new hash
      result = Hash.handle("HINCRBY", ["h", "counter", "5"], store)
      assert result == 5

      assert "5" = Hash.handle("HGET", ["h", "counter"], store)
    end

    test "HSETNX does not overwrite existing field" do
      store = MockStore.make()

      Hash.handle("HSET", ["h", "f", "original"], store)

      assert 0 = Hash.handle("HSETNX", ["h", "f", "new"], store)
      assert "original" = Hash.handle("HGET", ["h", "f"], store)
    end

    test "HSETNX creates new field if it doesn't exist" do
      store = MockStore.make()

      assert 1 = Hash.handle("HSETNX", ["h", "f", "value"], store)
      assert "value" = Hash.handle("HGET", ["h", "f"], store)
    end

    test "HDEL on all fields removes type metadata (empty hash cleanup)" do
      store = MockStore.make()

      Hash.handle("HSET", ["h", "f1", "v1", "f2", "v2"], store)
      assert {:simple, "hash"} = Ferricstore.Commands.Generic.handle("TYPE", ["h"], store)

      Hash.handle("HDEL", ["h", "f1", "f2"], store)

      # After deleting all fields, the hash should be gone
      assert {:simple, "none"} = Ferricstore.Commands.Generic.handle("TYPE", ["h"], store)
    end
  end

  # ===========================================================================
  # LIST EDGE CASES
  # ===========================================================================

  describe "list edge cases" do
    test "LPOP on empty list returns nil" do
      store = MockStore.make()
      assert nil == List.handle("LPOP", ["nosuchlist"], store)
    end

    test "RPOP on empty list returns nil" do
      store = MockStore.make()
      assert nil == List.handle("RPOP", ["nosuchlist"], store)
    end

    test "LPOP with count 0 on existing list returns empty list" do
      store = MockStore.make()
      List.handle("LPUSH", ["mylist", "a", "b"], store)

      result = List.handle("LPOP", ["mylist", "0"], store)
      assert result == []
    end

    test "LPOP with count 0 on missing list returns nil" do
      store = MockStore.make()

      result = List.handle("LPOP", ["nosuchlist", "0"], store)
      assert result == nil
    end

    test "LRANGE on missing list returns empty list" do
      store = MockStore.make()
      result = List.handle("LRANGE", ["nosuchlist", "0", "-1"], store)
      assert result == []
    end

    test "LLEN on missing list returns 0" do
      store = MockStore.make()
      assert 0 = List.handle("LLEN", ["nosuchlist"], store)
    end

    test "LINDEX out of range returns nil" do
      store = MockStore.make()
      List.handle("LPUSH", ["l", "a"], store)

      assert nil == List.handle("LINDEX", ["l", "100"], store)
      assert nil == List.handle("LINDEX", ["l", "-100"], store)
    end

    test "LPUSHX on missing list returns 0" do
      store = MockStore.make()
      assert 0 = List.handle("LPUSHX", ["nosuchlist", "a"], store)
    end

    test "RPUSHX on missing list returns 0" do
      store = MockStore.make()
      assert 0 = List.handle("RPUSHX", ["nosuchlist", "a"], store)
    end
  end

  # ===========================================================================
  # Helpers
  # ===========================================================================

  # Iterates HSCAN until cursor returns "0", collecting all field names.
  defp hscan_all(store, key, cursor, count) do
    [next_cursor, elements] =
      Hash.handle("HSCAN", [key, cursor, "COUNT", "#{count}"], store)

    # Elements are [field1, val1, field2, val2, ...]
    fields =
      for i <- 0..(div(max(length(elements), 2), 2) - 1),
          elements != [],
          do: Enum.at(elements, i * 2)

    if next_cursor == "0" do
      fields
    else
      fields ++ hscan_all(store, key, next_cursor, count)
    end
  end

  # Iterates SSCAN until cursor returns "0", collecting all members.
  defp sscan_all(store, key, cursor, count) do
    [next_cursor, members] =
      Set.handle("SSCAN", [key, cursor, "COUNT", "#{count}"], store)

    if next_cursor == "0" do
      members
    else
      members ++ sscan_all(store, key, next_cursor, count)
    end
  end

  # Iterates ZSCAN until cursor returns "0", collecting members and scores.
  defp zscan_all(store, key, cursor, count) do
    [next_cursor, elements] =
      SortedSet.handle("ZSCAN", [key, cursor, "COUNT", "#{count}"], store)

    # Elements are [member1, score1, member2, score2, ...]
    pairs = Enum.chunk_every(elements, 2)
    members = Enum.map(pairs, fn [m, _s] -> m end)
    scores = Enum.map(pairs, fn [_m, s] -> s end)

    if next_cursor == "0" do
      {members, scores}
    else
      {more_members, more_scores} = zscan_all(store, key, next_cursor, count)
      {members ++ more_members, scores ++ more_scores}
    end
  end
end
