defmodule Ferricstore.Commands.SortedSetTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Hash, Set, SortedSet}
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # ZADD
  # ---------------------------------------------------------------------------

  describe "ZADD" do
    test "ZADD adds new members and returns count" do
      store = MockStore.make()
      assert 3 == SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], store)
    end

    test "ZADD updating existing member returns 0" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert 0 == SortedSet.handle("ZADD", ["zs", "5.0", "a"], store)
    end

    test "ZADD with NX only adds new members" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert 1 == SortedSet.handle("ZADD", ["zs", "NX", "5.0", "a", "2.0", "b"], store)
      # a should still have score 1.0
      assert "1.0" == SortedSet.handle("ZSCORE", ["zs", "a"], store)
    end

    test "ZADD with XX only updates existing members" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert 0 == SortedSet.handle("ZADD", ["zs", "XX", "5.0", "a", "2.0", "b"], store)
      # a should be updated to 5.0
      assert "5.0" == SortedSet.handle("ZSCORE", ["zs", "a"], store)
      # b should not exist
      assert nil == SortedSet.handle("ZSCORE", ["zs", "b"], store)
    end

    test "ZADD with CH returns count of changed (added + updated)" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      # Adding b (new) + updating a (changed) = 2
      assert 2 == SortedSet.handle("ZADD", ["zs", "CH", "5.0", "a", "2.0", "b"], store)
    end

    test "ZADD with GT only updates if new score > current" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "5.0", "a"], store)
      SortedSet.handle("ZADD", ["zs", "GT", "3.0", "a"], store)
      # Should stay at 5.0 since 3.0 < 5.0
      assert "5.0" == SortedSet.handle("ZSCORE", ["zs", "a"], store)
      SortedSet.handle("ZADD", ["zs", "GT", "10.0", "a"], store)
      assert "10.0" == SortedSet.handle("ZSCORE", ["zs", "a"], store)
    end

    test "ZADD with integer scores" do
      store = MockStore.make()
      assert 1 == SortedSet.handle("ZADD", ["zs", "5", "a"], store)
    end

    test "ZADD with invalid score returns error" do
      store = MockStore.make()
      assert {:error, _} = SortedSet.handle("ZADD", ["zs", "abc", "a"], store)
    end

    test "ZADD with odd score/member count returns error" do
      store = MockStore.make()
      assert {:error, _} = SortedSet.handle("ZADD", ["zs", "1.0"], store)
    end

    test "ZADD with no args returns error" do
      assert {:error, _} = SortedSet.handle("ZADD", [], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # ZSCORE
  # ---------------------------------------------------------------------------

  describe "ZSCORE" do
    test "ZSCORE returns score string" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "3.14", "pi"], store)
      # ZSCORE now formats scores consistently with ZRANGE WITHSCORES
      assert "3.14000000000000012" == SortedSet.handle("ZSCORE", ["zs", "pi"], store)
    end

    test "ZSCORE returns nil for missing member" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert nil == SortedSet.handle("ZSCORE", ["zs", "missing"], store)
    end

    test "ZSCORE returns nil for nonexistent key" do
      assert nil == SortedSet.handle("ZSCORE", ["nonexistent", "a"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # ZRANK
  # ---------------------------------------------------------------------------

  describe "ZRANK" do
    test "ZRANK returns rank (0-indexed) by ascending score" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "3.0", "c", "1.0", "a", "2.0", "b"], store)
      assert 0 == SortedSet.handle("ZRANK", ["zs", "a"], store)
      assert 1 == SortedSet.handle("ZRANK", ["zs", "b"], store)
      assert 2 == SortedSet.handle("ZRANK", ["zs", "c"], store)
    end

    test "ZRANK returns nil for missing member" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert nil == SortedSet.handle("ZRANK", ["zs", "missing"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # ZRANGE
  # ---------------------------------------------------------------------------

  describe "ZRANGE" do
    test "ZRANGE returns members by rank" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "3.0", "c", "1.0", "a", "2.0", "b"], store)
      assert ["a", "b", "c"] == SortedSet.handle("ZRANGE", ["zs", "0", "-1"], store)
    end

    test "ZRANGE with sub-range" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c", "4.0", "d"], store)
      assert ["b", "c"] == SortedSet.handle("ZRANGE", ["zs", "1", "2"], store)
    end

    test "ZRANGE WITHSCORES returns members and scores" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.5", "a", "2.5", "b"], store)
      result = SortedSet.handle("ZRANGE", ["zs", "0", "-1", "WITHSCORES"], store)
      assert length(result) == 4
      assert Enum.at(result, 0) == "a"
      assert Enum.at(result, 2) == "b"
    end

    test "ZRANGE on nonexistent key returns empty list" do
      assert [] == SortedSet.handle("ZRANGE", ["nonexistent", "0", "-1"], MockStore.make())
    end

    test "ZRANGE with start > stop returns empty list" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert [] == SortedSet.handle("ZRANGE", ["zs", "5", "1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # ZREVRANGE
  # ---------------------------------------------------------------------------

  describe "ZREVRANGE" do
    test "ZREVRANGE returns members in descending score order" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], store)
      assert ["c", "b", "a"] == SortedSet.handle("ZREVRANGE", ["zs", "0", "-1"], store)
    end

    test "ZREVRANGE with sub-range" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], store)
      assert ["c", "b"] == SortedSet.handle("ZREVRANGE", ["zs", "0", "1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # ZCARD
  # ---------------------------------------------------------------------------

  describe "ZCARD" do
    test "ZCARD returns sorted set cardinality" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], store)
      assert 3 == SortedSet.handle("ZCARD", ["zs"], store)
    end

    test "ZCARD returns 0 for nonexistent key" do
      assert 0 == SortedSet.handle("ZCARD", ["nonexistent"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # ZREM
  # ---------------------------------------------------------------------------

  describe "ZREM" do
    test "ZREM removes member and returns 1" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b"], store)
      assert 1 == SortedSet.handle("ZREM", ["zs", "a"], store)
      assert nil == SortedSet.handle("ZSCORE", ["zs", "a"], store)
    end

    test "ZREM on missing member returns 0" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert 0 == SortedSet.handle("ZREM", ["zs", "missing"], store)
    end

    test "ZREM cleans up type metadata when sorted set becomes empty" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      SortedSet.handle("ZREM", ["zs", "a"], store)
      assert nil == store.compound_get.("zs", "T:zs")
    end
  end

  # ---------------------------------------------------------------------------
  # ZINCRBY
  # ---------------------------------------------------------------------------

  describe "ZINCRBY" do
    test "ZINCRBY increments score" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "5.0", "a"], store)
      result = SortedSet.handle("ZINCRBY", ["zs", "3.0", "a"], store)
      {val, ""} = Float.parse(result)
      assert_in_delta 8.0, val, 0.001
    end

    test "ZINCRBY creates member if missing" do
      store = MockStore.make()
      result = SortedSet.handle("ZINCRBY", ["zs", "5.0", "a"], store)
      {val, ""} = Float.parse(result)
      assert_in_delta 5.0, val, 0.001
    end

    test "ZINCRBY with integer increment" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "5.0", "a"], store)
      result = SortedSet.handle("ZINCRBY", ["zs", "3", "a"], store)
      {val, ""} = Float.parse(result)
      assert_in_delta 8.0, val, 0.001
    end

    test "ZINCRBY with invalid increment returns error" do
      store = MockStore.make()
      assert {:error, _} = SortedSet.handle("ZINCRBY", ["zs", "abc", "a"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # ZCOUNT
  # ---------------------------------------------------------------------------

  describe "ZCOUNT" do
    test "ZCOUNT with inclusive range" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c", "4.0", "d"], store)
      assert 3 == SortedSet.handle("ZCOUNT", ["zs", "1.0", "3.0"], store)
    end

    test "ZCOUNT with -inf to +inf returns all" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b"], store)
      assert 2 == SortedSet.handle("ZCOUNT", ["zs", "-inf", "+inf"], store)
    end

    test "ZCOUNT with exclusive bounds" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], store)
      # (1.0 to (3.0 = only score 2.0
      assert 1 == SortedSet.handle("ZCOUNT", ["zs", "(1.0", "(3.0"], store)
    end

    test "ZCOUNT on nonexistent key returns 0" do
      assert 0 == SortedSet.handle("ZCOUNT", ["nonexistent", "-inf", "+inf"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # ZPOPMIN and ZPOPMAX
  # ---------------------------------------------------------------------------

  describe "ZPOPMIN" do
    test "ZPOPMIN returns member with lowest score" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "3.0", "c", "1.0", "a", "2.0", "b"], store)
      result = SortedSet.handle("ZPOPMIN", ["zs"], store)
      assert Enum.at(result, 0) == "a"
    end

    test "ZPOPMIN removes the member" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b"], store)
      SortedSet.handle("ZPOPMIN", ["zs"], store)
      assert nil == SortedSet.handle("ZSCORE", ["zs", "a"], store)
    end

    test "ZPOPMIN with count returns multiple members" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], store)
      result = SortedSet.handle("ZPOPMIN", ["zs", "2"], store)
      # Should return [member1, score1, member2, score2]
      assert length(result) == 4
      assert Enum.at(result, 0) == "a"
      assert Enum.at(result, 2) == "b"
    end

    test "ZPOPMIN on empty key returns empty list" do
      store = MockStore.make()
      assert [] == SortedSet.handle("ZPOPMIN", ["nonexistent"], store)
    end
  end

  describe "ZPOPMAX" do
    test "ZPOPMAX returns member with highest score" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], store)
      result = SortedSet.handle("ZPOPMAX", ["zs"], store)
      assert Enum.at(result, 0) == "c"
    end

    test "ZPOPMAX removes the member" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "3.0", "c"], store)
      SortedSet.handle("ZPOPMAX", ["zs"], store)
      assert nil == SortedSet.handle("ZSCORE", ["zs", "c"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # ZSCAN
  # ---------------------------------------------------------------------------

  describe "ZSCAN" do
    test "ZSCAN with cursor 0 returns all members with scores" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], store)
      [cursor, elements] = SortedSet.handle("ZSCAN", ["zs", "0"], store)
      assert cursor == "0"
      # Elements is flat [member, score, member, score, ...]
      assert length(elements) == 6
      pairs = elements |> Enum.chunk_every(2) |> Map.new(fn [m, s] -> {m, s} end)
      assert pairs["a"] == "1.0"
      assert pairs["b"] == "2.0"
      assert pairs["c"] == "3.0"
    end

    test "ZSCAN with COUNT limits batch size" do
      store = MockStore.make()

      for i <- 1..20 do
        SortedSet.handle("ZADD", ["zs", "#{i}.0", "member#{String.pad_leading(Integer.to_string(i), 2, "0")}"], store)
      end

      [cursor, elements] = SortedSet.handle("ZSCAN", ["zs", "0", "COUNT", "5"], store)
      assert cursor != "0"
      # 5 members * 2 (member + score) = 10 elements
      assert length(elements) == 10
    end

    test "ZSCAN full iteration collects all members exactly once" do
      store = MockStore.make()
      expected = for i <- 1..12, into: %{}, do: {"m#{String.pad_leading(Integer.to_string(i), 2, "0")}", "#{i}.0"}

      for {m, s} <- expected do
        SortedSet.handle("ZADD", ["zs", s, m], store)
      end

      all_members = collect_zscan_members(store, "zs", "0", 4)
      result_map = Map.new(all_members, fn {m, s} -> {m, s} end)
      assert result_map == expected
    end

    test "ZSCAN with MATCH filters members by pattern" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "alpha", "2.0", "beta", "3.0", "alpaca"], store)
      [cursor, elements] = SortedSet.handle("ZSCAN", ["zs", "0", "MATCH", "al*"], store)
      assert cursor == "0"
      members = elements |> Enum.chunk_every(2) |> Enum.map(fn [m, _s] -> m end)
      assert Enum.sort(members) == ["alpaca", "alpha"]
    end

    test "ZSCAN on nonexistent key returns cursor 0 and empty list" do
      store = MockStore.make()
      [cursor, elements] = SortedSet.handle("ZSCAN", ["nonexistent", "0"], store)
      assert cursor == "0"
      assert elements == []
    end

    test "ZSCAN with invalid cursor returns error" do
      store = MockStore.make()
      assert {:error, _} = SortedSet.handle("ZSCAN", ["zs", "notanumber"], store)
    end

    test "ZSCAN with wrong number of arguments returns error" do
      store = MockStore.make()
      assert {:error, _} = SortedSet.handle("ZSCAN", ["key"], store)
      assert {:error, _} = SortedSet.handle("ZSCAN", [], store)
    end

    test "ZSCAN on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      store.compound_put.("mykey", "T:mykey", "hash", 0)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZSCAN", ["mykey", "0"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # ZRANDMEMBER
  # ---------------------------------------------------------------------------

  describe "ZRANDMEMBER" do
    test "ZRANDMEMBER returns a single random member" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], store)
      result = SortedSet.handle("ZRANDMEMBER", ["zs"], store)
      assert result in ["a", "b", "c"]
    end

    test "ZRANDMEMBER with positive count returns unique members" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c", "4.0", "d"], store)
      result = SortedSet.handle("ZRANDMEMBER", ["zs", "2"], store)
      assert is_list(result)
      assert length(result) == 2
      assert length(Enum.uniq(result)) == 2
      assert Enum.all?(result, &(&1 in ["a", "b", "c", "d"]))
    end

    test "ZRANDMEMBER with count > zset size returns all members" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b"], store)
      result = SortedSet.handle("ZRANDMEMBER", ["zs", "10"], store)
      assert Enum.sort(result) == ["a", "b"]
    end

    test "ZRANDMEMBER with negative count allows duplicates" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "only"], store)
      result = SortedSet.handle("ZRANDMEMBER", ["zs", "-5"], store)
      assert is_list(result)
      assert length(result) == 5
      assert Enum.all?(result, &(&1 == "only"))
    end

    test "ZRANDMEMBER with count 0 returns empty list" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      result = SortedSet.handle("ZRANDMEMBER", ["zs", "0"], store)
      assert result == []
    end

    test "ZRANDMEMBER with WITHSCORES returns member-score pairs" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], store)
      result = SortedSet.handle("ZRANDMEMBER", ["zs", "2", "WITHSCORES"], store)
      assert is_list(result)
      # 2 members * 2 (member + score) = 4
      assert length(result) == 4
      pairs = result |> Enum.chunk_every(2)
      # Verify each pair has a valid member and score
      Enum.each(pairs, fn [m, s] ->
        assert m in ["a", "b", "c"]
        assert is_binary(s)
        {_, ""} = Float.parse(s)
      end)
    end

    test "ZRANDMEMBER with negative count and WITHSCORES" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "only"], store)
      result = SortedSet.handle("ZRANDMEMBER", ["zs", "-3", "WITHSCORES"], store)
      assert length(result) == 6
      pairs = result |> Enum.chunk_every(2)
      assert Enum.all?(pairs, fn [m, _s] -> m == "only" end)
    end

    test "ZRANDMEMBER on nonexistent key returns nil" do
      store = MockStore.make()
      result = SortedSet.handle("ZRANDMEMBER", ["nonexistent"], store)
      assert result == nil
    end

    test "ZRANDMEMBER with count on nonexistent key returns empty list" do
      store = MockStore.make()
      result = SortedSet.handle("ZRANDMEMBER", ["nonexistent", "5"], store)
      assert result == []
    end

    test "ZRANDMEMBER with non-integer count returns error" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert {:error, _} = SortedSet.handle("ZRANDMEMBER", ["zs", "abc"], store)
    end

    test "ZRANDMEMBER with wrong arity returns error" do
      store = MockStore.make()
      assert {:error, _} = SortedSet.handle("ZRANDMEMBER", [], store)
    end

    test "ZRANDMEMBER on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      store.compound_put.("mykey", "T:mykey", "hash", 0)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZRANDMEMBER", ["mykey"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # ZMSCORE
  # ---------------------------------------------------------------------------

  describe "ZMSCORE" do
    test "ZMSCORE returns scores for existing members" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.5", "a", "2.5", "b", "3.5", "c"], store)
      result = SortedSet.handle("ZMSCORE", ["zs", "a", "b", "c"], store)
      assert result == ["1.5", "2.5", "3.5"]
    end

    test "ZMSCORE returns nil for missing members" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "3.0", "c"], store)
      result = SortedSet.handle("ZMSCORE", ["zs", "a", "b", "c"], store)
      assert result == ["1.0", nil, "3.0"]
    end

    test "ZMSCORE on nonexistent key returns all nils" do
      store = MockStore.make()
      result = SortedSet.handle("ZMSCORE", ["nonexistent", "a", "b"], store)
      assert result == [nil, nil]
    end

    test "ZMSCORE with single member" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "5.0", "only"], store)
      result = SortedSet.handle("ZMSCORE", ["zs", "only"], store)
      assert result == ["5.0"]
    end

    test "ZMSCORE with wrong number of arguments returns error" do
      store = MockStore.make()
      assert {:error, _} = SortedSet.handle("ZMSCORE", ["key"], store)
      assert {:error, _} = SortedSet.handle("ZMSCORE", [], store)
    end

    test "ZMSCORE on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      store.compound_put.("mykey", "T:mykey", "hash", 0)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZMSCORE", ["mykey", "a"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # Type enforcement
  # ---------------------------------------------------------------------------

  describe "type enforcement" do
    test "ZADD on a key used as hash returns WRONGTYPE" do
      store = MockStore.make()
      Hash.handle("HSET", ["mykey", "field", "value"], store)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZADD", ["mykey", "1.0", "a"], store)
    end

    test "ZRANGE on a key used as set returns WRONGTYPE" do
      store = MockStore.make()
      store.compound_put.("mykey", "T:mykey", "set", 0)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZRANGE", ["mykey", "0", "-1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # Private test helpers
  # ---------------------------------------------------------------------------

  # ---------------------------------------------------------------------------
  # Edge cases: arity, WRONGTYPE, score parsing, ZADD NX+XX conflict
  # ---------------------------------------------------------------------------

  describe "arity edge cases" do
    test "ZSCORE with no args returns error" do
      assert {:error, msg} = SortedSet.handle("ZSCORE", [], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "ZSCORE with only key returns error" do
      assert {:error, msg} = SortedSet.handle("ZSCORE", ["zs"], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "ZSCORE with extra args returns error" do
      store = MockStore.make()
      assert {:error, msg} = SortedSet.handle("ZSCORE", ["zs", "m", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "ZRANK with no args returns error" do
      assert {:error, msg} = SortedSet.handle("ZRANK", [], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "ZRANK with only key returns error" do
      assert {:error, msg} = SortedSet.handle("ZRANK", ["zs"], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "ZCARD with no args returns error" do
      assert {:error, msg} = SortedSet.handle("ZCARD", [], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "ZCARD with extra args returns error" do
      store = MockStore.make()
      assert {:error, msg} = SortedSet.handle("ZCARD", ["zs", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "ZINCRBY with no args returns error" do
      assert {:error, msg} = SortedSet.handle("ZINCRBY", [], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "ZINCRBY with only key returns error" do
      assert {:error, msg} = SortedSet.handle("ZINCRBY", ["zs"], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "ZCOUNT with no args returns error" do
      assert {:error, msg} = SortedSet.handle("ZCOUNT", [], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "ZCOUNT with only key returns error" do
      assert {:error, msg} = SortedSet.handle("ZCOUNT", ["zs"], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "ZREM with no members returns error" do
      assert {:error, msg} = SortedSet.handle("ZREM", ["zs"], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "ZPOPMIN with extra args returns error" do
      store = MockStore.make()
      assert {:error, msg} = SortedSet.handle("ZPOPMIN", ["zs", "1", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "ZPOPMAX with extra args returns error" do
      store = MockStore.make()
      assert {:error, msg} = SortedSet.handle("ZPOPMAX", ["zs", "1", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "ZRANGE with only key returns error" do
      assert {:error, msg} = SortedSet.handle("ZRANGE", ["zs"], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "ZRANGE with key and start but no stop returns error" do
      assert {:error, msg} = SortedSet.handle("ZRANGE", ["zs", "0"], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end
  end

  describe "WRONGTYPE enforcement for sorted set commands" do
    test "ZSCORE on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      store.compound_put.("mykey", "T:mykey", "hash", 0)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZSCORE", ["mykey", "m"], store)
    end

    test "ZRANK on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      store.compound_put.("mykey", "T:mykey", "set", 0)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZRANK", ["mykey", "m"], store)
    end

    test "ZCARD on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      store.compound_put.("mykey", "T:mykey", "hash", 0)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZCARD", ["mykey"], store)
    end

    test "ZINCRBY on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["mykey", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZINCRBY", ["mykey", "1.0", "m"], store)
    end

    test "ZCOUNT on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      store.compound_put.("mykey", "T:mykey", "hash", 0)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZCOUNT", ["mykey", "-inf", "+inf"], store)
    end

    test "ZPOPMIN on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      store.compound_put.("mykey", "T:mykey", "list", 0)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZPOPMIN", ["mykey"], store)
    end

    test "ZPOPMAX on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      store.compound_put.("mykey", "T:mykey", "list", 0)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZPOPMAX", ["mykey"], store)
    end

    test "ZREM on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      store.compound_put.("mykey", "T:mykey", "hash", 0)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZREM", ["mykey", "m"], store)
    end

    test "ZREVRANGE on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      store.compound_put.("mykey", "T:mykey", "set", 0)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZREVRANGE", ["mykey", "0", "-1"], store)
    end
  end

  describe "ZADD option conflict edge cases" do
    test "ZADD with NX and XX returns error (mutually exclusive flags)" do
      store = MockStore.make()
      # Redis rejects NX+XX
      assert {:error, "ERR XX and NX options at the same time are not compatible"} =
               SortedSet.handle("ZADD", ["zs", "NX", "XX", "1.0", "a"], store)
    end

    test "ZADD with NX and XX on existing member returns error" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      # Redis rejects NX+XX regardless of member existence
      assert {:error, "ERR XX and NX options at the same time are not compatible"} =
               SortedSet.handle("ZADD", ["zs", "NX", "XX", "5.0", "a"], store)
    end
  end

  describe "ZADD score parsing edge cases" do
    test "ZADD with negative score" do
      store = MockStore.make()
      assert 1 == SortedSet.handle("ZADD", ["zs", "-5.0", "a"], store)
      assert "-5.0" == SortedSet.handle("ZSCORE", ["zs", "a"], store)
    end

    test "ZADD with zero score" do
      store = MockStore.make()
      assert 1 == SortedSet.handle("ZADD", ["zs", "0", "a"], store)
    end

    test "ZADD with very large score" do
      store = MockStore.make()
      assert 1 == SortedSet.handle("ZADD", ["zs", "999999999999999", "a"], store)
    end

    test "ZADD with empty string score returns error" do
      store = MockStore.make()
      assert {:error, msg} = SortedSet.handle("ZADD", ["zs", "", "a"], store)
      assert msg =~ "not a valid float"
    end
  end

  describe "ZCOUNT score bound edge cases" do
    test "ZCOUNT with 'inf' (no plus) as max" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b"], store)
      assert 2 == SortedSet.handle("ZCOUNT", ["zs", "-inf", "inf"], store)
    end

    test "ZCOUNT with invalid min returns error" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert {:error, msg} = SortedSet.handle("ZCOUNT", ["zs", "abc", "+inf"], store)
      assert msg =~ "not a float"
    end

    test "ZCOUNT with invalid max returns error" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert {:error, msg} = SortedSet.handle("ZCOUNT", ["zs", "-inf", "abc"], store)
      assert msg =~ "not a float"
    end
  end

  describe "ZSCAN cursor edge cases" do
    test "ZSCAN with negative cursor returns error" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert {:error, "ERR invalid cursor"} = SortedSet.handle("ZSCAN", ["zs", "-1"], store)
    end

    test "ZSCAN with very large cursor returns cursor 0 and empty list" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      [cursor, elements] = SortedSet.handle("ZSCAN", ["zs", "999999"], store)
      assert cursor == "0"
      assert elements == []
    end

    test "ZSCAN with COUNT 0 returns error" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert {:error, _} = SortedSet.handle("ZSCAN", ["zs", "0", "COUNT", "0"], store)
    end

    test "ZSCAN with unknown option returns error" do
      store = MockStore.make()
      assert {:error, "ERR syntax error"} = SortedSet.handle("ZSCAN", ["zs", "0", "BOGUS", "val"], store)
    end
  end

  describe "ZPOPMIN/ZPOPMAX edge cases" do
    test "ZPOPMIN with count 0 returns empty list" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert [] == SortedSet.handle("ZPOPMIN", ["zs", "0"], store)
    end

    test "ZPOPMAX with count 0 returns empty list" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert [] == SortedSet.handle("ZPOPMAX", ["zs", "0"], store)
    end

    test "ZPOPMIN with negative count returns error" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert {:error, msg} = SortedSet.handle("ZPOPMIN", ["zs", "-1"], store)
      assert msg =~ "not an integer"
    end

    test "ZPOPMAX with negative count returns error" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert {:error, msg} = SortedSet.handle("ZPOPMAX", ["zs", "-1"], store)
      assert msg =~ "not an integer"
    end

    test "ZPOPMIN with non-integer count returns error" do
      store = MockStore.make()
      assert {:error, msg} = SortedSet.handle("ZPOPMIN", ["zs", "abc"], store)
      assert msg =~ "not an integer"
    end

    test "ZPOPMAX with non-integer count returns error" do
      store = MockStore.make()
      assert {:error, msg} = SortedSet.handle("ZPOPMAX", ["zs", "abc"], store)
      assert msg =~ "not an integer"
    end
  end

  describe "ZRANDMEMBER edge cases" do
    test "ZRANDMEMBER with invalid WITHSCORES arg returns syntax error" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert {:error, "ERR syntax error"} = SortedSet.handle("ZRANDMEMBER", ["zs", "1", "BOGUS"], store)
    end

    test "ZRANDMEMBER with negative count on empty set returns empty list" do
      store = MockStore.make()
      result = SortedSet.handle("ZRANDMEMBER", ["zs", "-5"], store)
      assert result == []
    end
  end

  defp collect_zscan_members(store, key, cursor, count) do
    collect_zscan_members(store, key, cursor, count, [])
  end

  defp collect_zscan_members(store, key, cursor, count, acc) do
    [next_cursor, elements] = SortedSet.handle("ZSCAN", [key, cursor, "COUNT", Integer.to_string(count)], store)
    pairs = elements |> Enum.chunk_every(2) |> Enum.map(fn [m, s] -> {m, s} end)
    new_acc = acc ++ pairs

    if next_cursor == "0" do
      new_acc
    else
      collect_zscan_members(store, key, next_cursor, count, new_acc)
    end
  end
end
