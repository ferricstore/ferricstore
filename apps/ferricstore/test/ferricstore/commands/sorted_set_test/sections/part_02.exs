defmodule Ferricstore.Commands.SortedSetTest.Sections.Part02 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.{Hash, List, Set, SortedSet}
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Test.MockStore

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

    test "ZPOPMIN with count batches member deletes" do
      parent = self()
      type_key = CompoundKey.type_key("zs")

      member_keys = [
        CompoundKey.zset_member("zs", "a"),
        CompoundKey.zset_member("zs", "b")
      ]

      store = %{
        compound_get: fn
          "zs", ^type_key -> "zset"
          "zs", _compound_key -> nil
        end,
        compound_scan: fn "zs", _prefix ->
          [{"b", "2.0"}, {"a", "1.0"}, {"c", "3.0"}]
        end,
        compound_batch_delete: fn "zs", compound_keys ->
          send(parent, {:compound_batch_delete, compound_keys})
          :ok
        end,
        compound_delete: fn "zs", compound_key ->
          flunk(
            "ZPOPMIN should use compound_batch_delete, got per-member delete #{inspect(compound_key)}"
          )
        end,
        compound_count: fn "zs", _prefix -> 1 end
      }

      assert ["a", "1.0", "b", "2.0"] == SortedSet.handle("ZPOPMIN", ["zs", "2"], store)
      assert_received {:compound_batch_delete, ^member_keys}
      refute_received {:compound_batch_delete, _}
    end

    test "ZPOPMIN uses score-index rank range when available" do
      parent = self()
      base_store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], base_store)

      store =
        base_store
        |> Map.put(:zset_rank_range, fn redis_key, start_idx, stop_idx, reverse? ->
          send(parent, {:zset_rank_range, redis_key, start_idx, stop_idx, reverse?})
          {:ok, [{"a", 1.0}, {"b", 2.0}]}
        end)
        |> Map.put(:compound_scan, fn _redis_key, _prefix ->
          flunk("ZPOPMIN should not scan the full zset when rank index exists")
        end)

      assert ["a", "1.0", "b", "2.0"] == SortedSet.handle("ZPOPMIN", ["zs", "2"], store)
      assert_receive {:zset_rank_range, "zs", 0, 1, false}
    end

    test "ZPOPMIN on empty key returns empty list" do
      store = MockStore.make()
      assert [] == SortedSet.handle("ZPOPMIN", ["nonexistent"], store)
    end

    test "ZPOPMIN returns type cleanup errors after removing the last member" do
      store = zset_cleanup_failure_store()

      assert {:error, :disk_full} == SortedSet.handle("ZPOPMIN", ["zs"], store)
    end

    test "ZPOPMIN preserves the last member when type cleanup fails" do
      base = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1", "only"], base)
      type_key = CompoundKey.type_key("zs")

      store =
        Map.put(base, :compound_delete, fn
          "zs", ^type_key -> {:error, :disk_full}
          key, compound_key -> base.compound_delete.(key, compound_key)
        end)

      assert {:error, :disk_full} == SortedSet.handle("ZPOPMIN", ["zs"], store)
      assert "1.0" == SortedSet.handle("ZSCORE", ["zs", "only"], base)
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

    test "ZPOPMAX with count batches member deletes" do
      parent = self()
      type_key = CompoundKey.type_key("zs")

      member_keys = [
        CompoundKey.zset_member("zs", "c"),
        CompoundKey.zset_member("zs", "b")
      ]

      store = %{
        compound_get: fn
          "zs", ^type_key -> "zset"
          "zs", _compound_key -> nil
        end,
        compound_scan: fn "zs", _prefix ->
          [{"b", "2.0"}, {"a", "1.0"}, {"c", "3.0"}]
        end,
        compound_batch_delete: fn "zs", compound_keys ->
          send(parent, {:compound_batch_delete, compound_keys})
          :ok
        end,
        compound_delete: fn "zs", compound_key ->
          flunk(
            "ZPOPMAX should use compound_batch_delete, got per-member delete #{inspect(compound_key)}"
          )
        end,
        compound_count: fn "zs", _prefix -> 1 end
      }

      assert ["c", "3.0", "b", "2.0"] == SortedSet.handle("ZPOPMAX", ["zs", "2"], store)
      assert_received {:compound_batch_delete, ^member_keys}
      refute_received {:compound_batch_delete, _}
    end

    test "ZPOPMAX uses score-index rank range when available" do
      parent = self()
      base_store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], base_store)

      store =
        base_store
        |> Map.put(:zset_rank_range, fn redis_key, start_idx, stop_idx, reverse? ->
          send(parent, {:zset_rank_range, redis_key, start_idx, stop_idx, reverse?})
          {:ok, [{"c", 3.0}, {"b", 2.0}]}
        end)
        |> Map.put(:compound_scan, fn _redis_key, _prefix ->
          flunk("ZPOPMAX should not scan the full zset when rank index exists")
        end)

      assert ["c", "3.0", "b", "2.0"] == SortedSet.handle("ZPOPMAX", ["zs", "2"], store)
      assert_receive {:zset_rank_range, "zs", 0, 1, true}
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
        SortedSet.handle(
          "ZADD",
          ["zs", "#{i}.0", "member#{String.pad_leading(Integer.to_string(i), 2, "0")}"],
          store
        )
      end

      [cursor, elements] = SortedSet.handle("ZSCAN", ["zs", "0", "COUNT", "5"], store)
      assert cursor != "0"
      # 5 members * 2 (member + score) = 10 elements
      assert length(elements) == 10
    end

    test "ZSCAN full iteration collects all members exactly once" do
      store = MockStore.make()

      expected =
        for i <- 1..12,
            into: %{},
            do: {"m#{String.pad_leading(Integer.to_string(i), 2, "0")}", "#{i}.0"}

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
      Hash.handle("HSET", ["mykey", "field", "value"], store)
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

    test "ZRANDMEMBER with count 0 does not scan members" do
      type_key = CompoundKey.type_key("zs")

      store = %{
        compound_get: fn "zs", ^type_key -> "zset" end,
        compound_scan: fn "zs", _prefix ->
          flunk("ZRANDMEMBER count 0 should not scan members")
        end
      }

      assert [] == SortedSet.handle("ZRANDMEMBER", ["zs", "0"], store)
    end

    test "ZRANDMEMBER with count 0 WITHSCORES does not scan members" do
      type_key = CompoundKey.type_key("zs")

      store = %{
        compound_get: fn "zs", ^type_key -> "zset" end,
        compound_scan: fn "zs", _prefix ->
          flunk("ZRANDMEMBER count 0 WITHSCORES should not scan members")
        end
      }

      assert [] == SortedSet.handle("ZRANDMEMBER", ["zs", "0", "WITHSCORES"], store)
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
      Hash.handle("HSET", ["mykey", "field", "value"], store)
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

    test "ZMSCORE uses compound_batch_get when the store provides it" do
      type_key = CompoundKey.type_key("zs")

      member_keys = [
        CompoundKey.zset_member("zs", "a"),
        CompoundKey.zset_member("zs", "missing"),
        CompoundKey.zset_member("zs", "c")
      ]

      store = %{
        compound_get: fn
          "zs", ^type_key ->
            nil

          "zs", compound_key ->
            flunk(
              "ZMSCORE should use compound_batch_get, got per-member lookup #{inspect(compound_key)}"
            )
        end,
        compound_batch_get: fn "zs", ^member_keys -> ["1.0", nil, "3.0"] end
      }

      assert ["1.0", nil, "3.0"] ==
               SortedSet.handle("ZMSCORE", ["zs", "a", "missing", "c"], store)
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
      Hash.handle("HSET", ["mykey", "field", "value"], store)
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
      Set.handle("SADD", ["mykey", "member"], store)
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
      Hash.handle("HSET", ["mykey", "field", "value"], store)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZSCORE", ["mykey", "m"], store)
    end

    test "ZRANK on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["mykey", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZRANK", ["mykey", "m"], store)
    end

    test "ZCARD on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      Hash.handle("HSET", ["mykey", "field", "value"], store)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZCARD", ["mykey"], store)
    end

    test "ZINCRBY on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["mykey", "member"], store)

      assert {:error, "WRONGTYPE" <> _} =
               SortedSet.handle("ZINCRBY", ["mykey", "1.0", "m"], store)
    end

    test "ZCOUNT on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      Hash.handle("HSET", ["mykey", "field", "value"], store)

      assert {:error, "WRONGTYPE" <> _} =
               SortedSet.handle("ZCOUNT", ["mykey", "-inf", "+inf"], store)
    end

    test "ZPOPMIN on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      List.handle("LPUSH", ["mykey", "elem"], store)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZPOPMIN", ["mykey"], store)
    end

    test "ZPOPMAX on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      List.handle("LPUSH", ["mykey", "elem"], store)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZPOPMAX", ["mykey"], store)
    end

    test "ZREM on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      Hash.handle("HSET", ["mykey", "field", "value"], store)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZREM", ["mykey", "m"], store)
    end

    test "ZREVRANGE on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["mykey", "member"], store)

      assert {:error, "WRONGTYPE" <> _} =
               SortedSet.handle("ZREVRANGE", ["mykey", "0", "-1"], store)
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

    test "AST ZSCAN with negative cursor returns invalid cursor" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)

      assert {:error, "ERR invalid cursor"} =
               SortedSet.handle_ast({:zscan, "zs", -1, []}, store)
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

      assert {:error, "ERR syntax error"} =
               SortedSet.handle("ZSCAN", ["zs", "0", "BOGUS", "val"], store)
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

      assert {:error, "ERR syntax error"} =
               SortedSet.handle("ZRANDMEMBER", ["zs", "1", "BOGUS"], store)
    end

    test "ZRANDMEMBER with negative count on empty set returns empty list" do
      store = MockStore.make()
      result = SortedSet.handle("ZRANDMEMBER", ["zs", "-5"], store)
      assert result == []
    end
  end
    end
  end
end
