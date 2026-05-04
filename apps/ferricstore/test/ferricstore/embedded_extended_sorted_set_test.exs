defmodule Ferricstore.EmbeddedExtendedSortedSetTest do
  @moduledoc """
  Sorted set and stream operation tests extracted from EmbeddedExtendedTest.

  Covers: zrank, zrevrank, zrangebyscore, zcount, zincrby,
  zrandmember, zpopmin, zpopmax, zmscore, xadd, xlen, xrange,
  xrevrange, xtrim.
  """
  use ExUnit.Case, async: false

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    :ok
  end

  # ===========================================================================
  # SORTED SET extended: zrank, zrevrank, zrangebyscore, zcount, zincrby,
  # zrandmember, zpopmin, zpopmax, zmscore
  # ===========================================================================

  describe "zrank/2" do
    test "returns rank of existing member (ascending)" do
      FerricStore.zadd("zr:key", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, 0} = FerricStore.zrank("zr:key", "a")
      assert {:ok, 1} = FerricStore.zrank("zr:key", "b")
      assert {:ok, 2} = FerricStore.zrank("zr:key", "c")
    end

    test "returns nil for missing member" do
      FerricStore.zadd("zr:key2", [{1.0, "a"}])
      assert {:ok, nil} = FerricStore.zrank("zr:key2", "z")
    end
  end

  describe "zrevrank/2" do
    test "returns reverse rank (descending)" do
      FerricStore.zadd("zrr:key", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, 2} = FerricStore.zrevrank("zrr:key", "a")
      assert {:ok, 1} = FerricStore.zrevrank("zrr:key", "b")
      assert {:ok, 0} = FerricStore.zrevrank("zrr:key", "c")
    end

    test "returns nil for missing member" do
      FerricStore.zadd("zrr:key2", [{1.0, "a"}])
      assert {:ok, nil} = FerricStore.zrevrank("zrr:key2", "z")
    end
  end

  describe "zrangebyscore/4" do
    test "returns members within score range" do
      FerricStore.zadd("zbs:key", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}])
      assert {:ok, members} = FerricStore.zrangebyscore("zbs:key", "2", "3")
      assert members == ["b", "c"]
    end

    test "supports -inf and +inf" do
      FerricStore.zadd("zbs:key2", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, members} = FerricStore.zrangebyscore("zbs:key2", "-inf", "+inf")
      assert members == ["a", "b", "c"]
    end

    test "returns empty for out-of-range scores" do
      FerricStore.zadd("zbs:key3", [{1.0, "a"}])
      assert {:ok, []} = FerricStore.zrangebyscore("zbs:key3", "10", "20")
    end

    test "score index stays current after zadd updates and zrem" do
      key = "zbs:index_mutation"
      FerricStore.zadd(key, [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])

      assert {:ok, ["a", "b", "c"]} = FerricStore.zrangebyscore(key, "-inf", "+inf")

      FerricStore.zadd(key, [{10.0, "b"}, {0.5, "d"}])
      assert {:ok, ["d", "a"]} = FerricStore.zrangebyscore(key, "-inf", "2")

      FerricStore.zrem(key, ["a"])
      assert {:ok, ["d"]} = FerricStore.zrangebyscore(key, "-inf", "2")
    end
  end

  describe "zcount/3" do
    test "counts members in score range" do
      FerricStore.zadd("zc:key", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}])
      assert {:ok, 2} = FerricStore.zcount("zc:key", "2", "3")
    end

    test "supports -inf and +inf" do
      FerricStore.zadd("zc:key2", [{1.0, "a"}, {2.0, "b"}])
      assert {:ok, 2} = FerricStore.zcount("zc:key2", "-inf", "+inf")
    end

    test "returns 0 for out-of-range" do
      FerricStore.zadd("zc:key3", [{1.0, "a"}])
      assert {:ok, 0} = FerricStore.zcount("zc:key3", "10", "20")
    end

    test "score index count stays current after zincrby" do
      key = "zc:index_mutation"
      FerricStore.zadd(key, [{1.0, "a"}, {2.0, "b"}])

      assert {:ok, 2} = FerricStore.zcount(key, "-inf", "2")

      assert {:ok, _score} = FerricStore.zincrby(key, 5.0, "a")
      assert {:ok, 1} = FerricStore.zcount(key, "-inf", "2")
    end
  end

  describe "zincrby/3" do
    test "increments member score" do
      FerricStore.zadd("zi:key", [{10.0, "alice"}])
      assert {:ok, result} = FerricStore.zincrby("zi:key", 5.0, "alice")
      {score, _} = Float.parse(result)
      assert_in_delta score, 15.0, 0.001
    end

    test "creates member if it does not exist" do
      assert {:ok, result} = FerricStore.zincrby("zi:key2", 3.0, "bob")
      {score, _} = Float.parse(result)
      assert_in_delta score, 3.0, 0.001
    end
  end

  describe "zrandmember/2" do
    test "returns a random member" do
      FerricStore.zadd("zrm:key", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, member} = FerricStore.zrandmember("zrm:key")
      assert member in ["a", "b", "c"]
    end

    test "returns multiple random members with count" do
      FerricStore.zadd("zrm:key2", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, members} = FerricStore.zrandmember("zrm:key2", 2)
      assert is_list(members) and length(members) == 2
    end

    test "returns nil for nonexistent key" do
      assert {:ok, nil} = FerricStore.zrandmember("zrm:missing")
    end
  end

  describe "zpopmin/2" do
    test "pops member with lowest score" do
      FerricStore.zadd("zpm:key", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, [{member, score}]} = FerricStore.zpopmin("zpm:key", 1)
      assert member == "a"
      assert_in_delta score, 1.0, 0.001
    end

    test "pops multiple members with lowest scores" do
      FerricStore.zadd("zpm:key2", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, pairs} = FerricStore.zpopmin("zpm:key2", 2)
      assert length(pairs) == 2
      [{m1, _}, {m2, _}] = pairs
      assert m1 == "a"
      assert m2 == "b"
    end

    test "returns empty list for nonexistent key" do
      assert {:ok, []} = FerricStore.zpopmin("zpm:missing", 1)
    end
  end

  describe "zpopmax/2" do
    test "pops member with highest score" do
      FerricStore.zadd("zpx:key", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, [{member, score}]} = FerricStore.zpopmax("zpx:key", 1)
      assert member == "c"
      assert_in_delta score, 3.0, 0.001
    end

    test "pops multiple members with highest scores" do
      FerricStore.zadd("zpx:key2", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, pairs} = FerricStore.zpopmax("zpx:key2", 2)
      assert length(pairs) == 2
      [{m1, _}, {m2, _}] = pairs
      assert m1 == "c"
      assert m2 == "b"
    end

    test "returns empty list for nonexistent key" do
      assert {:ok, []} = FerricStore.zpopmax("zpx:missing", 1)
    end
  end

  describe "zmscore/2" do
    test "returns scores for multiple members" do
      FerricStore.zadd("zms:key", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, scores} = FerricStore.zmscore("zms:key", ["a", "c", "z"])
      assert length(scores) == 3
      assert_in_delta Enum.at(scores, 0), 1.0, 0.001
      assert_in_delta Enum.at(scores, 1), 3.0, 0.001
      assert Enum.at(scores, 2) == nil
    end
  end

  # ===========================================================================
  # STREAMS: xadd, xlen, xrange, xrevrange, xtrim
  # ===========================================================================

  describe "xadd/2 and xlen/1" do
    test "adds entries and reports length" do
      assert {:ok, id1} = FerricStore.xadd("xs:key", ["name", "alice"])
      assert is_binary(id1)
      assert {:ok, id2} = FerricStore.xadd("xs:key", ["name", "bob"])
      assert is_binary(id2)
      assert {:ok, 2} = FerricStore.xlen("xs:key")
    end

    test "xlen returns 0 for nonexistent stream" do
      assert {:ok, 0} = FerricStore.xlen("xs:missing")
    end
  end

  describe "xrange/4" do
    test "returns entries in forward order" do
      FerricStore.xadd("xr:key", ["k", "v1"])
      FerricStore.xadd("xr:key", ["k", "v2"])
      assert {:ok, entries} = FerricStore.xrange("xr:key", "-", "+")
      assert is_list(entries)
      assert length(entries) >= 2
    end

    test "supports COUNT option" do
      FerricStore.xadd("xr:key2", ["k", "v1"])
      FerricStore.xadd("xr:key2", ["k", "v2"])
      FerricStore.xadd("xr:key2", ["k", "v3"])
      assert {:ok, entries} = FerricStore.xrange("xr:key2", "-", "+", count: 2)
      assert length(entries) == 2
    end
  end

  describe "xrevrange/4" do
    test "returns entries in reverse order" do
      FerricStore.xadd("xrr:key", ["k", "v1"])
      FerricStore.xadd("xrr:key", ["k", "v2"])
      assert {:ok, entries} = FerricStore.xrevrange("xrr:key", "+", "-")
      assert is_list(entries) and length(entries) >= 2
    end
  end

  describe "xtrim/2" do
    test "trims stream to maxlen" do
      for i <- 1..10 do
        FerricStore.xadd("xt:key", ["i", to_string(i)])
      end

      assert {:ok, trimmed} = FerricStore.xtrim("xt:key", maxlen: 5)
      assert is_integer(trimmed) and trimmed >= 0
      assert {:ok, len} = FerricStore.xlen("xt:key")
      assert len <= 5
    end
  end
end
