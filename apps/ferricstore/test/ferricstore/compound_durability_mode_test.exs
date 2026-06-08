defmodule Ferricstore.CompoundDurabilityModeTest do
  @moduledoc """
  Tests that compound operations (list, hash, set, sorted set) work through
  the quorum write path.

  Verifies that data written through each command family is readable after
  write.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state
  @moduletag timeout: 60_000

  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  # ---------------------------------------------------------------------------
  # List operations
  # ---------------------------------------------------------------------------

  describe "list operations (quorum mode)" do
    test "LPUSH + LRANGE through quorum" do
      assert {:ok, 3} = FerricStore.lpush("q:list1", ["c", "b", "a"])
      assert {:ok, ["a", "b", "c"]} = FerricStore.lrange("q:list1", 0, -1)
    end

    test "RPUSH + LPOP through quorum" do
      FerricStore.rpush("q:list2", ["x", "y", "z"])
      assert {:ok, "x"} = FerricStore.lpop("q:list2")
      assert {:ok, ["y", "z"]} = FerricStore.lrange("q:list2", 0, -1)
    end

    test "LINSERT through quorum" do
      FerricStore.rpush("q:list3", ["a", "c"])
      FerricStore.linsert("q:list3", :before, "c", "b")
      assert {:ok, ["a", "b", "c"]} = FerricStore.lrange("q:list3", 0, -1)
    end

    test "LLEN through quorum" do
      FerricStore.rpush("q:list4", ["a", "b", "c"])
      assert {:ok, 3} = FerricStore.llen("q:list4")
    end
  end

  # ---------------------------------------------------------------------------
  # Hash operations
  # ---------------------------------------------------------------------------

  describe "hash operations (quorum mode)" do
    test "HSET + HGET through quorum" do
      FerricStore.hset("q:hash1", %{"field1" => "val1", "field2" => "val2"})
      assert {:ok, "val1"} = FerricStore.hget("q:hash1", "field1")
      assert {:ok, "val2"} = FerricStore.hget("q:hash1", "field2")
    end

    test "HDEL through quorum" do
      FerricStore.hset("q:hash2", %{"a" => "1", "b" => "2"})
      FerricStore.hdel("q:hash2", ["a"])
      assert {:ok, nil} = FerricStore.hget("q:hash2", "a")
      assert {:ok, "2"} = FerricStore.hget("q:hash2", "b")
    end

    test "HGETALL through quorum" do
      FerricStore.hset("q:hash3", %{"x" => "1", "y" => "2"})
      {:ok, all} = FerricStore.hgetall("q:hash3")
      assert all == %{"x" => "1", "y" => "2"}
    end

    test "HLEN through quorum" do
      FerricStore.hset("q:hash4", %{"a" => "1", "b" => "2", "c" => "3"})
      assert {:ok, 3} = FerricStore.hlen("q:hash4")
    end
  end

  # ---------------------------------------------------------------------------
  # Set operations
  # ---------------------------------------------------------------------------

  describe "set operations (quorum mode)" do
    test "SADD + SMEMBERS through quorum" do
      FerricStore.sadd("q:set1", ["a", "b", "c"])
      {:ok, members} = FerricStore.smembers("q:set1")
      assert Enum.sort(members) == ["a", "b", "c"]
    end

    test "SREM through quorum" do
      FerricStore.sadd("q:set2", ["a", "b", "c"])
      FerricStore.srem("q:set2", ["b"])
      {:ok, members} = FerricStore.smembers("q:set2")
      assert Enum.sort(members) == ["a", "c"]
    end

    test "SCARD through quorum" do
      FerricStore.sadd("q:set3", ["x", "y", "z"])
      assert {:ok, 3} = FerricStore.scard("q:set3")
    end

    test "SISMEMBER through quorum" do
      FerricStore.sadd("q:set4", ["a", "b"])
      assert {:ok, true} = FerricStore.sismember("q:set4", "a")
      assert {:ok, false} = FerricStore.sismember("q:set4", "z")
    end
  end

  # ---------------------------------------------------------------------------
  # Sorted set operations
  # ---------------------------------------------------------------------------

  describe "sorted set operations (quorum mode)" do
    test "ZADD + ZRANGE through quorum" do
      FerricStore.zadd("q:zset1", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      {:ok, members} = FerricStore.zrange("q:zset1", 0, -1)
      assert members == ["a", "b", "c"]
    end

    test "ZSCORE through quorum" do
      FerricStore.zadd("q:zset2", [{42.5, "x"}])
      {:ok, score} = FerricStore.zscore("q:zset2", "x")
      assert score != nil
    end

    test "ZCARD through quorum" do
      FerricStore.zadd("q:zset3", [{1.0, "a"}, {2.0, "b"}])
      assert {:ok, 2} = FerricStore.zcard("q:zset3")
    end

    test "ZREM through quorum" do
      FerricStore.zadd("q:zset4", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      FerricStore.zrem("q:zset4", ["b"])
      {:ok, members} = FerricStore.zrange("q:zset4", 0, -1)
      assert members == ["a", "c"]
    end
  end

  # ---------------------------------------------------------------------------
  # Simple string operations for comparison
  # ---------------------------------------------------------------------------

  describe "string operations (quorum mode baseline)" do
    test "SET + GET through quorum" do
      FerricStore.set("q:str1", "hello")
      assert {:ok, "hello"} = FerricStore.get("q:str1")
    end

    test "INCR through quorum" do
      FerricStore.set("q:counter", "10")
      assert {:ok, 11} = FerricStore.incr("q:counter")
    end
  end

  # ---------------------------------------------------------------------------
  # All operations survive write → read cycle
  # This verifies the data path is complete (ETS + disk)
  # ---------------------------------------------------------------------------

  describe "compound operations data integrity" do
    test "all compound types written and readable in same test" do
      # Write all types
      FerricStore.rpush("integrity:list", ["a", "b", "c"])
      FerricStore.hset("integrity:hash", %{"f1" => "v1", "f2" => "v2"})
      FerricStore.sadd("integrity:set", ["x", "y", "z"])
      FerricStore.zadd("integrity:zset", [{1.0, "m1"}, {2.0, "m2"}])
      FerricStore.set("integrity:str", "plain")

      # Read all back — must be consistent
      assert {:ok, ["a", "b", "c"]} = FerricStore.lrange("integrity:list", 0, -1)
      assert {:ok, "v1"} = FerricStore.hget("integrity:hash", "f1")
      {:ok, members} = FerricStore.smembers("integrity:set")
      assert Enum.sort(members) == ["x", "y", "z"]
      {:ok, zranged} = FerricStore.zrange("integrity:zset", 0, -1)
      assert zranged == ["m1", "m2"]
      assert {:ok, "plain"} = FerricStore.get("integrity:str")
    end
  end
end
