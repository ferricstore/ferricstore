defmodule Ferricstore.Commands.ZrangebyscoreTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Hash, Set, SortedSet}
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # Helper: seed a sorted set with the given score/member pairs
  # ---------------------------------------------------------------------------

  defp seed(store, key, pairs) do
    args = Enum.flat_map(pairs, fn {score, member} -> [Float.to_string(score / 1), member] end)
    SortedSet.handle("ZADD", [key | args], store)
    store
  end

  # ---------------------------------------------------------------------------
  # ZRANGEBYSCORE basic range queries
  # ---------------------------------------------------------------------------

  describe "ZRANGEBYSCORE basic range" do
    test "returns members with scores in the inclusive range" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}, {5.0, "e"}])

      result = SortedSet.handle("ZRANGEBYSCORE", ["zs", "2", "4"], store)
      assert result == ["b", "c", "d"]
    end

    test "returns empty list when no members in range" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])

      result = SortedSet.handle("ZRANGEBYSCORE", ["zs", "5", "10"], store)
      assert result == []
    end

    test "returns empty list for nonexistent key" do
      store = MockStore.make()
      result = SortedSet.handle("ZRANGEBYSCORE", ["nonexistent", "-inf", "+inf"], store)
      assert result == []
    end

    test "returns members sorted by score ascending" do
      store = MockStore.make()
      # Insert in arbitrary order
      seed(store, "zs", [{3.0, "c"}, {1.0, "a"}, {2.0, "b"}])

      result = SortedSet.handle("ZRANGEBYSCORE", ["zs", "-inf", "+inf"], store)
      assert result == ["a", "b", "c"]
    end
  end

  # ---------------------------------------------------------------------------
  # ZRANGEBYSCORE WITHSCORES
  # ---------------------------------------------------------------------------

  describe "ZRANGEBYSCORE WITHSCORES" do
    test "returns member-score pairs interleaved" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])

      result = SortedSet.handle("ZRANGEBYSCORE", ["zs", "1", "3", "WITHSCORES"], store)
      # Result is [member, score, member, score, ...]
      assert length(result) == 6
      pairs = Enum.chunk_every(result, 2)
      members = Enum.map(pairs, fn [m, _s] -> m end)
      assert members == ["a", "b", "c"]
      # Verify scores are valid float strings
      Enum.each(pairs, fn [_m, s] ->
        assert is_binary(s)
        {_, ""} = Float.parse(s)
      end)
    end

    test "WITHSCORES is case-insensitive" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}])

      result = SortedSet.handle("ZRANGEBYSCORE", ["zs", "-inf", "+inf", "WITHSCORES"], store)
      assert length(result) == 2
    end
  end

  # ---------------------------------------------------------------------------
  # ZRANGEBYSCORE -inf and +inf
  # ---------------------------------------------------------------------------

  describe "ZRANGEBYSCORE infinity bounds" do
    test "-inf to +inf returns all members" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])

      result = SortedSet.handle("ZRANGEBYSCORE", ["zs", "-inf", "+inf"], store)
      assert result == ["a", "b", "c"]
    end

    test "-inf as min returns all up to max" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}])

      result = SortedSet.handle("ZRANGEBYSCORE", ["zs", "-inf", "2"], store)
      assert result == ["a", "b"]
    end

    test "+inf as max returns all from min" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}])

      result = SortedSet.handle("ZRANGEBYSCORE", ["zs", "3", "+inf"], store)
      assert result == ["c", "d"]
    end

    test "bare 'inf' works as +inf" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}])

      result = SortedSet.handle("ZRANGEBYSCORE", ["zs", "-inf", "inf"], store)
      assert result == ["a", "b"]
    end
  end

  # ---------------------------------------------------------------------------
  # ZRANGEBYSCORE exclusive ranges with ( prefix
  # ---------------------------------------------------------------------------

  describe "ZRANGEBYSCORE exclusive ranges" do
    test "exclusive min excludes exact match" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])

      result = SortedSet.handle("ZRANGEBYSCORE", ["zs", "(1", "3"], store)
      assert result == ["b", "c"]
    end

    test "exclusive max excludes exact match" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])

      result = SortedSet.handle("ZRANGEBYSCORE", ["zs", "1", "(3"], store)
      assert result == ["a", "b"]
    end

    test "both exclusive bounds" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}])

      result = SortedSet.handle("ZRANGEBYSCORE", ["zs", "(1", "(4"], store)
      assert result == ["b", "c"]
    end

    test "exclusive bound with float" do
      store = MockStore.make()
      seed(store, "zs", [{1.5, "a"}, {2.5, "b"}, {3.5, "c"}])

      result = SortedSet.handle("ZRANGEBYSCORE", ["zs", "(1.5", "(3.5"], store)
      assert result == ["b"]
    end
  end

  # ---------------------------------------------------------------------------
  # ZRANGEBYSCORE LIMIT offset count
  # ---------------------------------------------------------------------------

  describe "ZRANGEBYSCORE LIMIT" do
    test "LIMIT offset count paginates results" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}, {5.0, "e"}])

      result = SortedSet.handle("ZRANGEBYSCORE", ["zs", "-inf", "+inf", "LIMIT", "1", "2"], store)
      assert result == ["b", "c"]
    end

    test "LIMIT with offset 0 returns from start" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])

      result = SortedSet.handle("ZRANGEBYSCORE", ["zs", "-inf", "+inf", "LIMIT", "0", "2"], store)
      assert result == ["a", "b"]
    end

    test "LIMIT with count exceeding remaining returns all remaining" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])

      result =
        SortedSet.handle("ZRANGEBYSCORE", ["zs", "-inf", "+inf", "LIMIT", "1", "100"], store)

      assert result == ["b", "c"]
    end

    test "LIMIT with offset exceeding size returns empty" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}])

      result =
        SortedSet.handle("ZRANGEBYSCORE", ["zs", "-inf", "+inf", "LIMIT", "10", "5"], store)

      assert result == []
    end

    test "LIMIT combined with WITHSCORES" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}])

      result =
        SortedSet.handle(
          "ZRANGEBYSCORE",
          ["zs", "-inf", "+inf", "WITHSCORES", "LIMIT", "1", "2"],
          store
        )

      assert length(result) == 4
      pairs = Enum.chunk_every(result, 2)
      members = Enum.map(pairs, fn [m, _s] -> m end)
      assert members == ["b", "c"]
    end

    test "LIMIT with negative count returns all from offset (Redis compat)" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}])

      result =
        SortedSet.handle("ZRANGEBYSCORE", ["zs", "-inf", "+inf", "LIMIT", "2", "-1"], store)

      assert result == ["c", "d"]
    end
  end

  # ---------------------------------------------------------------------------
  # ZRANGEBYSCORE edge cases
  # ---------------------------------------------------------------------------

  describe "ZRANGEBYSCORE edge cases" do
    test "min > max returns empty list" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])

      result = SortedSet.handle("ZRANGEBYSCORE", ["zs", "5", "1"], store)
      assert result == []
    end

    test "single member in range" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])

      result = SortedSet.handle("ZRANGEBYSCORE", ["zs", "2", "2"], store)
      assert result == ["b"]
    end

    test "members with same score are returned in lexicographic order" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "charlie"}, {1.0, "alice"}, {1.0, "bob"}])

      result = SortedSet.handle("ZRANGEBYSCORE", ["zs", "-inf", "+inf"], store)
      assert result == ["alice", "bob", "charlie"]
    end

    test "invalid min returns error" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}])

      assert {:error, msg} = SortedSet.handle("ZRANGEBYSCORE", ["zs", "abc", "+inf"], store)
      assert msg =~ "not a float"
    end

    test "invalid max returns error" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}])

      assert {:error, msg} = SortedSet.handle("ZRANGEBYSCORE", ["zs", "-inf", "xyz"], store)
      assert msg =~ "not a float"
    end

    test "wrong number of arguments returns error" do
      store = MockStore.make()
      assert {:error, msg} = SortedSet.handle("ZRANGEBYSCORE", ["zs"], store)
      assert msg =~ "wrong number of arguments"

      assert {:error, msg2} = SortedSet.handle("ZRANGEBYSCORE", ["zs", "0"], store)
      assert msg2 =~ "wrong number of arguments"

      assert {:error, msg3} = SortedSet.handle("ZRANGEBYSCORE", [], store)
      assert msg3 =~ "wrong number of arguments"
    end

    test "WRONGTYPE on non-zset key returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["mykey", "field", "value"], store)

      assert {:error, "WRONGTYPE" <> _} =
               SortedSet.handle("ZRANGEBYSCORE", ["mykey", "-inf", "+inf"], store)
    end

    test "negative scores work correctly" do
      store = MockStore.make()
      seed(store, "zs", [{-3.0, "a"}, {-1.0, "b"}, {0.0, "c"}, {2.0, "d"}])

      result = SortedSet.handle("ZRANGEBYSCORE", ["zs", "-2", "1"], store)
      assert result == ["b", "c"]
    end
  end

  # ---------------------------------------------------------------------------
  # ZREVRANGEBYSCORE
  # ---------------------------------------------------------------------------

  describe "ZREVRANGEBYSCORE basic" do
    test "returns members in reverse (descending) order" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}, {5.0, "e"}])

      # Note: ZREVRANGEBYSCORE takes max first, then min
      result = SortedSet.handle("ZREVRANGEBYSCORE", ["zs", "4", "2"], store)
      assert result == ["d", "c", "b"]
    end

    test "returns empty list when no members in range" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])

      result = SortedSet.handle("ZREVRANGEBYSCORE", ["zs", "10", "5"], store)
      assert result == []
    end

    test "+inf to -inf returns all in descending order" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])

      result = SortedSet.handle("ZREVRANGEBYSCORE", ["zs", "+inf", "-inf"], store)
      assert result == ["c", "b", "a"]
    end

    test "exclusive bounds work" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}])

      result = SortedSet.handle("ZREVRANGEBYSCORE", ["zs", "(4", "(1"], store)
      assert result == ["c", "b"]
    end

    test "WITHSCORES returns score-member pairs in reverse order" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])

      result = SortedSet.handle("ZREVRANGEBYSCORE", ["zs", "+inf", "-inf", "WITHSCORES"], store)
      assert length(result) == 6
      pairs = Enum.chunk_every(result, 2)
      members = Enum.map(pairs, fn [m, _s] -> m end)
      assert members == ["c", "b", "a"]
    end

    test "LIMIT with reverse order" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}, {5.0, "e"}])

      result =
        SortedSet.handle("ZREVRANGEBYSCORE", ["zs", "+inf", "-inf", "LIMIT", "1", "2"], store)

      assert result == ["d", "c"]
    end

    test "LIMIT with WITHSCORES in reverse" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}])

      result =
        SortedSet.handle(
          "ZREVRANGEBYSCORE",
          ["zs", "+inf", "-inf", "WITHSCORES", "LIMIT", "0", "2"],
          store
        )

      assert length(result) == 4
      pairs = Enum.chunk_every(result, 2)
      members = Enum.map(pairs, fn [m, _s] -> m end)
      assert members == ["d", "c"]
    end

    test "returns empty for nonexistent key" do
      store = MockStore.make()
      result = SortedSet.handle("ZREVRANGEBYSCORE", ["nonexistent", "+inf", "-inf"], store)
      assert result == []
    end

    test "max < min returns empty (reversed args compared to forward)" do
      store = MockStore.make()
      seed(store, "zs", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])

      # In ZREVRANGEBYSCORE, first arg is max, second is min
      # If max < min that's an empty range
      result = SortedSet.handle("ZREVRANGEBYSCORE", ["zs", "1", "5"], store)
      assert result == []
    end

    test "wrong number of arguments returns error" do
      store = MockStore.make()
      assert {:error, msg} = SortedSet.handle("ZREVRANGEBYSCORE", ["zs"], store)
      assert msg =~ "wrong number of arguments"

      assert {:error, msg2} = SortedSet.handle("ZREVRANGEBYSCORE", ["zs", "0"], store)
      assert msg2 =~ "wrong number of arguments"
    end

    test "WRONGTYPE on non-zset key returns error" do
      store = MockStore.make()
      Set.handle("SADD", ["mykey", "member"], store)

      assert {:error, "WRONGTYPE" <> _} =
               SortedSet.handle("ZREVRANGEBYSCORE", ["mykey", "+inf", "-inf"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # Stress test: 10000 members, range query < 100ms
  # ---------------------------------------------------------------------------

  describe "ZRANGEBYSCORE stress" do
    @tag timeout: 10_000
    test "range query on 10000 members completes under 100ms" do
      store = MockStore.make()

      # Insert 10000 members with sequential scores
      args =
        Enum.flat_map(1..10_000, fn i ->
          [Float.to_string(i * 1.0), "member:#{String.pad_leading(Integer.to_string(i), 5, "0")}"]
        end)

      SortedSet.handle("ZADD", ["zs" | args], store)

      # Query a subset (scores 5000-5100)
      {time_us, result} =
        :timer.tc(fn ->
          SortedSet.handle("ZRANGEBYSCORE", ["zs", "5000", "5100"], store)
        end)

      assert length(result) == 101
      assert time_us < 100_000, "Query took #{time_us}us, expected < 100ms"
    end

    @tag timeout: 10_000
    test "ZREVRANGEBYSCORE on 10000 members completes under 100ms" do
      store = MockStore.make()

      args =
        Enum.flat_map(1..10_000, fn i ->
          [Float.to_string(i * 1.0), "member:#{String.pad_leading(Integer.to_string(i), 5, "0")}"]
        end)

      SortedSet.handle("ZADD", ["zs" | args], store)

      {time_us, result} =
        :timer.tc(fn ->
          SortedSet.handle("ZREVRANGEBYSCORE", ["zs", "5100", "5000"], store)
        end)

      assert length(result) == 101
      assert time_us < 100_000, "Query took #{time_us}us, expected < 100ms"
    end
  end
end
