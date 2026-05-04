defmodule Ferricstore.EmbeddedTest do
  @moduledoc """
  Comprehensive tests for the FerricStore embedded mode direct Elixir API.

  Tests cover strings (set/get/del), INCR/INCRBY, hash operations (hset/hget/hgetall),
  TTL management (expire/ttl), and pipeline batching. Tests use flush_all_keys
  for isolation and run with async: false.
  """
  use ExUnit.Case, async: false

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    :ok
  end

  # ===========================================================================
  # SET / GET — basic string operations
  # ===========================================================================

  describe "set/3 and get/1" do
    test "sets and gets a simple string value" do
      assert :ok = FerricStore.set("user:42:name", "alice")
      assert {:ok, "alice"} = FerricStore.get("user:42:name")
    end

    test "get returns {:ok, nil} for nonexistent key" do
      assert {:ok, nil} = FerricStore.get("nonexistent")
    end

    test "set overwrites existing value" do
      assert :ok = FerricStore.set("key", "first")
      assert :ok = FerricStore.set("key", "second")
      assert {:ok, "second"} = FerricStore.get("key")
    end

    test "set with empty string key and value" do
      assert :ok = FerricStore.set("", "")
      assert {:ok, ""} = FerricStore.get("")
    end

    test "set preserves binary data with null bytes" do
      value = <<0, 1, 2, 255, 0, 128>>
      assert :ok = FerricStore.set("binary_key", value)
      assert {:ok, ^value} = FerricStore.get("binary_key")
    end

    test "set with large value (100KB)" do
      large = String.duplicate("x", 100_000)
      assert :ok = FerricStore.set("large", large)
      assert {:ok, ^large} = FerricStore.get("large")
    end

    test "set with TTL option" do
      assert :ok = FerricStore.set("ttl_key", "value", ttl: :timer.hours(1))
      assert {:ok, "value"} = FerricStore.get("ttl_key")
    end

    test "set with TTL 0 means no expiry" do
      assert :ok = FerricStore.set("no_expire", "value", ttl: 0)
      assert {:ok, "value"} = FerricStore.get("no_expire")
    end

    test "set returns :ok" do
      result = FerricStore.set("k", "v")
      assert result == :ok
    end

    test "multiple keys can be set and retrieved independently" do
      assert :ok = FerricStore.set("a", "1")
      assert :ok = FerricStore.set("b", "2")
      assert :ok = FerricStore.set("c", "3")

      assert {:ok, "1"} = FerricStore.get("a")
      assert {:ok, "2"} = FerricStore.get("b")
      assert {:ok, "3"} = FerricStore.get("c")
    end
  end

  # ===========================================================================
  # DEL — key deletion
  # ===========================================================================

  describe "del/1" do
    test "deletes an existing key" do
      FerricStore.set("to_delete", "value")
      result = FerricStore.del("to_delete")
      assert result == :ok or result == {:ok, 1}
      assert {:ok, nil} = FerricStore.get("to_delete")
    end

    test "del on nonexistent key returns :ok" do
      result = FerricStore.del("never_existed")
      assert result == :ok or result == {:ok, 0}
    end

    test "del then set creates fresh key" do
      FerricStore.set("cycle", "first")
      FerricStore.del("cycle")
      FerricStore.set("cycle", "second")
      assert {:ok, "second"} = FerricStore.get("cycle")
    end
  end

  # ===========================================================================
  # INCR / INCRBY — integer increment
  # ===========================================================================

  describe "incr/1" do
    test "increments nonexistent key from 0 to 1" do
      assert {:ok, 1} = FerricStore.incr("counter:new")
    end

    test "increments existing integer value" do
      FerricStore.set("counter:existing", "5")
      assert {:ok, 6} = FerricStore.incr("counter:existing")
    end

    test "multiple increments accumulate" do
      assert {:ok, 1} = FerricStore.incr("counter:multi")
      assert {:ok, 2} = FerricStore.incr("counter:multi")
      assert {:ok, 3} = FerricStore.incr("counter:multi")
    end

    test "incr on non-integer value returns error" do
      FerricStore.set("not_a_number", "abc")
      assert {:error, msg} = FerricStore.incr("not_a_number")
      assert msg =~ "not an integer"
    end

    test "incr on negative integer works" do
      FerricStore.set("negative", "-3")
      assert {:ok, -2} = FerricStore.incr("negative")
    end

    test "incr on zero works" do
      FerricStore.set("zero", "0")
      assert {:ok, 1} = FerricStore.incr("zero")
    end

    test "incr preserves string representation after increment" do
      FerricStore.incr("str_counter")
      assert {:ok, "1"} = FerricStore.get("str_counter")
    end
  end

  describe "incr_by/2" do
    test "increments by specified amount" do
      assert {:ok, 10} = FerricStore.incr_by("counter:by", 10)
    end

    test "increments by negative amount (decrement)" do
      FerricStore.set("counter:dec", "100")
      assert {:ok, 95} = FerricStore.incr_by("counter:dec", -5)
    end

    test "increments by zero returns current value" do
      FerricStore.set("counter:zero", "42")
      assert {:ok, 42} = FerricStore.incr_by("counter:zero", 0)
    end

    test "incr_by on nonexistent key initializes from 0" do
      assert {:ok, 50} = FerricStore.incr_by("counter:fresh", 50)
    end

    test "incr_by on non-integer value returns error" do
      FerricStore.set("bad_val", "hello")
      assert {:error, _msg} = FerricStore.incr_by("bad_val", 5)
    end

    test "incr_by with large amount works" do
      assert {:ok, 1_000_000} = FerricStore.incr_by("big_counter", 1_000_000)
    end
  end

  # ===========================================================================
  # HSET / HGET / HGETALL — hash operations
  # ===========================================================================

  describe "hset/2 and hget/2" do
    test "sets and gets a hash field" do
      assert :ok = FerricStore.hset("user:1", %{"name" => "alice"})
      assert {:ok, "alice"} = FerricStore.hget("user:1", "name")
    end

    test "sets multiple fields at once" do
      assert :ok = FerricStore.hset("user:2", %{"name" => "bob", "age" => "25"})
      assert {:ok, "bob"} = FerricStore.hget("user:2", "name")
      assert {:ok, "25"} = FerricStore.hget("user:2", "age")
    end

    test "hget on nonexistent hash returns {:ok, nil}" do
      assert {:ok, nil} = FerricStore.hget("nonexistent_hash", "field")
    end

    test "hget on nonexistent field returns {:ok, nil}" do
      FerricStore.hset("partial_hash", %{"exists" => "yes"})
      assert {:ok, nil} = FerricStore.hget("partial_hash", "missing")
    end

    test "hset merges with existing fields" do
      FerricStore.hset("merge_hash", %{"a" => "1"})
      FerricStore.hset("merge_hash", %{"b" => "2"})
      assert {:ok, "1"} = FerricStore.hget("merge_hash", "a")
      assert {:ok, "2"} = FerricStore.hget("merge_hash", "b")
    end

    test "hset overwrites existing field value" do
      FerricStore.hset("overwrite_hash", %{"field" => "old"})
      FerricStore.hset("overwrite_hash", %{"field" => "new"})
      assert {:ok, "new"} = FerricStore.hget("overwrite_hash", "field")
    end

    test "hset with atom keys converts to strings" do
      FerricStore.hset("atom_hash", %{name: "alice", age: 30})
      assert {:ok, "alice"} = FerricStore.hget("atom_hash", "name")
      assert {:ok, "30"} = FerricStore.hget("atom_hash", "age")
    end
  end

  describe "hgetall/1" do
    test "returns all fields and values" do
      FerricStore.hset("full_hash", %{"a" => "1", "b" => "2", "c" => "3"})
      assert {:ok, map} = FerricStore.hgetall("full_hash")
      assert map == %{"a" => "1", "b" => "2", "c" => "3"}
    end

    test "returns empty map for nonexistent hash" do
      assert {:ok, %{}} = FerricStore.hgetall("nope")
    end

    test "returns merged result after multiple hset calls" do
      FerricStore.hset("merged", %{"x" => "1"})
      FerricStore.hset("merged", %{"y" => "2"})
      assert {:ok, %{"x" => "1", "y" => "2"}} = FerricStore.hgetall("merged")
    end
  end

  # ===========================================================================
  # Hash storage via compound keys
  # ===========================================================================
  # encode_hash/decode_hash were removed in favour of compound-key storage.
  # The HSET/HGET/HGETALL tests above cover the new path.

  # ===========================================================================
  # TTL — expire and ttl
  # ===========================================================================

  describe "expire/2 and ttl/1" do
    test "sets TTL on existing key" do
      FerricStore.set("ttl_test", "value")
      assert {:ok, true} = FerricStore.expire("ttl_test", :timer.minutes(30))
      assert {:ok, ms} = FerricStore.ttl("ttl_test")
      assert is_integer(ms)
      assert ms > 0
      assert ms <= :timer.minutes(30)
    end

    test "expire on nonexistent key returns {:ok, false}" do
      assert {:ok, false} = FerricStore.expire("no_such_key", :timer.seconds(10))
    end

    test "ttl on key without expiry returns {:ok, nil}" do
      FerricStore.set("no_ttl", "value")
      assert {:ok, nil} = FerricStore.ttl("no_ttl")
    end

    test "ttl on nonexistent key returns {:ok, nil}" do
      assert {:ok, nil} = FerricStore.ttl("never_set")
    end

    test "set with ttl then check ttl" do
      FerricStore.set("ttl_via_set", "value", ttl: :timer.seconds(60))
      assert {:ok, ms} = FerricStore.ttl("ttl_via_set")
      assert is_integer(ms)
      assert ms > 0
      assert ms <= 60_000
    end
  end

  # ===========================================================================
  # Pipeline — batching
  # ===========================================================================

  describe "pipeline/1" do
    test "executes multiple SET commands" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.set("p_key1", "val1")
                 |> FerricStore.Pipe.set("p_key2", "val2")
               end)

      assert results == [:ok, :ok]
      assert {:ok, "val1"} = FerricStore.get("p_key1")
      assert {:ok, "val2"} = FerricStore.get("p_key2")
    end

    test "executes mixed SET and GET commands" do
      FerricStore.set("existing", "hello")

      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.set("new_key", "world")
                 |> FerricStore.Pipe.get("existing")
                 |> FerricStore.Pipe.get("new_key")
               end)

      assert results == [:ok, {:ok, "hello"}, {:ok, "world"}]
    end

    test "pipeline with INCR" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.incr("pipe_counter")
                 |> FerricStore.Pipe.incr("pipe_counter")
                 |> FerricStore.Pipe.incr("pipe_counter")
               end)

      assert results == [{:ok, 1}, {:ok, 2}, {:ok, 3}]
    end

    test "pipeline with DEL" do
      FerricStore.set("del_target", "value")

      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.del("del_target")
                 |> FerricStore.Pipe.get("del_target")
               end)

      assert results == [:ok, {:ok, nil}]
    end

    test "pipeline with HSET and HGET" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.hset("pipe_hash", %{"field" => "value"})
                 |> FerricStore.Pipe.hget("pipe_hash", "field")
               end)

      assert results == [:ok, {:ok, "value"}]
    end

    test "empty pipeline returns empty results" do
      assert {:ok, []} =
               FerricStore.pipeline(fn pipe ->
                 pipe
               end)
    end

    test "pipeline with INCR_BY" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.incr_by("pipe_incrby", 10)
                 |> FerricStore.Pipe.incr_by("pipe_incrby", 5)
               end)

      assert results == [{:ok, 10}, {:ok, 15}]
    end

    test "pipeline commands execute in order" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.set("order_test", "first")
                 |> FerricStore.Pipe.get("order_test")
                 |> FerricStore.Pipe.set("order_test", "second")
                 |> FerricStore.Pipe.get("order_test")
               end)

      assert results == [:ok, {:ok, "first"}, :ok, {:ok, "second"}]
    end

    test "pipeline with SET with TTL option" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.set("ttl_pipe", "value", ttl: :timer.hours(1))
               end)

      assert results == [:ok]
      assert {:ok, "value"} = FerricStore.get("ttl_pipe")
    end
  end

  # ===========================================================================
  # Named caches (option passthrough)
  # ===========================================================================

  describe "named cache option" do
    test "set accepts cache option without error" do
      # The :cache option is accepted but currently routes to the default cache.
      # This test verifies the option does not cause an error.
      assert :ok = FerricStore.set("cached_key", "value", cache: :sessions)
    end

    test "get accepts cache option without error" do
      FerricStore.set("cached_key2", "value")
      assert {:ok, _} = FerricStore.get("cached_key2", cache: :sessions)
    end
  end

  # ===========================================================================
  # LPUSH / RPUSH — list push operations
  # ===========================================================================

  describe "lpush/2" do
    test "creates a new list and pushes elements to the left" do
      assert {:ok, 3} = FerricStore.lpush("list:lpush", ["a", "b", "c"])
    end

    test "pushes to an existing list" do
      FerricStore.lpush("list:lpush2", ["a"])
      assert {:ok, 3} = FerricStore.lpush("list:lpush2", ["b", "c"])
    end

    test "elements are pushed in Redis LPUSH order (last arg becomes leftmost)" do
      FerricStore.lpush("list:order", ["a", "b", "c"])
      assert {:ok, ["c", "b", "a"]} = FerricStore.lrange("list:order", 0, -1)
    end

    test "lpush with single element" do
      assert {:ok, 1} = FerricStore.lpush("list:single", ["x"])
    end
  end

  describe "rpush/2" do
    test "creates a new list and pushes elements to the right" do
      assert {:ok, 3} = FerricStore.rpush("list:rpush", ["a", "b", "c"])
    end

    test "pushes to an existing list" do
      FerricStore.rpush("list:rpush2", ["a"])
      assert {:ok, 3} = FerricStore.rpush("list:rpush2", ["b", "c"])
    end

    test "elements are pushed in order (first arg is leftmost)" do
      FerricStore.rpush("list:rorder", ["a", "b", "c"])
      assert {:ok, ["a", "b", "c"]} = FerricStore.lrange("list:rorder", 0, -1)
    end
  end

  # ===========================================================================
  # LPOP / RPOP — list pop operations
  # ===========================================================================

  describe "lpop/2" do
    test "pops single element from the left" do
      FerricStore.rpush("list:lpop", ["a", "b", "c"])
      assert {:ok, "a"} = FerricStore.lpop("list:lpop")
    end

    test "pops multiple elements from the left" do
      FerricStore.rpush("list:lpop2", ["a", "b", "c", "d"])
      assert {:ok, ["a", "b"]} = FerricStore.lpop("list:lpop2", 2)
    end

    test "lpop on nonexistent key returns {:ok, nil}" do
      assert {:ok, nil} = FerricStore.lpop("list:nonexistent")
    end

    test "lpop count exceeding list length returns all elements" do
      FerricStore.rpush("list:lpop3", ["a", "b"])
      assert {:ok, ["a", "b"]} = FerricStore.lpop("list:lpop3", 5)
    end

    test "lpop empties the list and subsequent lpop returns nil" do
      FerricStore.rpush("list:lpop4", ["a"])
      assert {:ok, "a"} = FerricStore.lpop("list:lpop4")
      assert {:ok, nil} = FerricStore.lpop("list:lpop4")
    end
  end

  describe "rpop/2" do
    test "pops single element from the right" do
      FerricStore.rpush("list:rpop", ["a", "b", "c"])
      assert {:ok, "c"} = FerricStore.rpop("list:rpop")
    end

    test "pops multiple elements from the right" do
      FerricStore.rpush("list:rpop2", ["a", "b", "c", "d"])
      assert {:ok, ["d", "c"]} = FerricStore.rpop("list:rpop2", 2)
    end

    test "rpop on nonexistent key returns {:ok, nil}" do
      assert {:ok, nil} = FerricStore.rpop("list:nonexistent")
    end
  end

  # ===========================================================================
  # LRANGE — list range
  # ===========================================================================

  describe "lrange/3" do
    test "returns full list with 0..-1" do
      FerricStore.rpush("list:range", ["a", "b", "c", "d"])
      assert {:ok, ["a", "b", "c", "d"]} = FerricStore.lrange("list:range", 0, -1)
    end

    test "returns a sub-range" do
      FerricStore.rpush("list:range2", ["a", "b", "c", "d"])
      assert {:ok, ["b", "c"]} = FerricStore.lrange("list:range2", 1, 2)
    end

    test "returns empty list for out-of-range" do
      FerricStore.rpush("list:range3", ["a"])
      assert {:ok, []} = FerricStore.lrange("list:range3", 5, 10)
    end

    test "returns empty list for nonexistent key" do
      assert {:ok, []} = FerricStore.lrange("list:noexist", 0, -1)
    end

    test "negative indices work correctly" do
      FerricStore.rpush("list:range4", ["a", "b", "c", "d"])
      assert {:ok, ["c", "d"]} = FerricStore.lrange("list:range4", -2, -1)
    end
  end

  # ===========================================================================
  # LLEN — list length
  # ===========================================================================

  describe "llen/1" do
    test "returns the length of a list" do
      FerricStore.rpush("list:len", ["a", "b", "c"])
      assert {:ok, 3} = FerricStore.llen("list:len")
    end

    test "returns 0 for nonexistent key" do
      assert {:ok, 0} = FerricStore.llen("list:noexist")
    end

    test "returns 0 after all elements are popped" do
      FerricStore.rpush("list:len2", ["a"])
      FerricStore.lpop("list:len2")
      assert {:ok, 0} = FerricStore.llen("list:len2")
    end
  end

  # ===========================================================================
  # SADD — set add
  # ===========================================================================

  describe "sadd/2" do
    test "adds new members and returns count" do
      assert {:ok, 3} = FerricStore.sadd("set:add", ["a", "b", "c"])
    end

    test "ignores duplicate members" do
      FerricStore.sadd("set:dup", ["a", "b"])
      assert {:ok, 1} = FerricStore.sadd("set:dup", ["b", "c"])
    end

    test "all duplicates returns 0" do
      FerricStore.sadd("set:alldup", ["a"])
      assert {:ok, 0} = FerricStore.sadd("set:alldup", ["a"])
    end
  end

  # ===========================================================================
  # SREM — set remove
  # ===========================================================================

  describe "srem/2" do
    test "removes existing members" do
      FerricStore.sadd("set:rem", ["a", "b", "c"])
      assert {:ok, 2} = FerricStore.srem("set:rem", ["a", "c"])
    end

    test "ignores nonexistent members" do
      FerricStore.sadd("set:rem2", ["a"])
      assert {:ok, 0} = FerricStore.srem("set:rem2", ["z"])
    end

    test "removing from nonexistent set returns 0" do
      assert {:ok, 0} = FerricStore.srem("set:noexist", ["a"])
    end
  end

  # ===========================================================================
  # SMEMBERS — set members
  # ===========================================================================

  describe "smembers/1" do
    test "returns all members of a set" do
      FerricStore.sadd("set:members", ["a", "b", "c"])
      {:ok, members} = FerricStore.smembers("set:members")
      assert Enum.sort(members) == ["a", "b", "c"]
    end

    test "returns empty list for nonexistent set" do
      assert {:ok, []} = FerricStore.smembers("set:noexist")
    end

    test "returns correct members after add and remove" do
      FerricStore.sadd("set:addrem", ["a", "b", "c"])
      FerricStore.srem("set:addrem", ["b"])
      {:ok, members} = FerricStore.smembers("set:addrem")
      assert Enum.sort(members) == ["a", "c"]
    end
  end

  # ===========================================================================
  # SISMEMBER — set membership check
  # ===========================================================================

  describe "sismember/2" do
    test "returns true for existing member" do
      FerricStore.sadd("set:ism", ["a", "b"])
      result = FerricStore.sismember("set:ism", "a")
      assert result == true or result == {:ok, true}
    end

    test "returns false for missing member" do
      FerricStore.sadd("set:ism2", ["a"])
      result = FerricStore.sismember("set:ism2", "z")
      assert result == false or result == {:ok, false}
    end

    test "returns false for nonexistent key" do
      result = FerricStore.sismember("set:noexist", "a")
      assert result == false or result == {:ok, false}
    end
  end

  # ===========================================================================
  # SCARD — set cardinality
  # ===========================================================================

  describe "scard/1" do
    test "returns the number of members" do
      FerricStore.sadd("set:card", ["a", "b", "c"])
      assert {:ok, 3} = FerricStore.scard("set:card")
    end

    test "returns 0 for nonexistent set" do
      assert {:ok, 0} = FerricStore.scard("set:noexist")
    end
  end

  # ===========================================================================
  # ZADD — sorted set add
  # ===========================================================================

  describe "zadd/2" do
    test "adds new members with scores" do
      assert {:ok, 2} = FerricStore.zadd("zset:add", [{1.0, "a"}, {2.0, "b"}])
    end

    test "updating existing member score does not count as new" do
      FerricStore.zadd("zset:upd", [{1.0, "a"}])
      assert {:ok, 0} = FerricStore.zadd("zset:upd", [{5.0, "a"}])
    end

    test "mixed add and update" do
      FerricStore.zadd("zset:mix", [{1.0, "a"}])
      assert {:ok, 1} = FerricStore.zadd("zset:mix", [{2.0, "a"}, {3.0, "b"}])
    end
  end

  # ===========================================================================
  # ZRANGE — sorted set range
  # ===========================================================================

  describe "zrange/4" do
    test "returns members in score order" do
      FerricStore.zadd("zset:range", [{3.0, "c"}, {1.0, "a"}, {2.0, "b"}])
      assert {:ok, ["a", "b", "c"]} = FerricStore.zrange("zset:range", 0, -1)
    end

    test "returns sub-range" do
      FerricStore.zadd("zset:range2", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, ["b"]} = FerricStore.zrange("zset:range2", 1, 1)
    end

    test "returns empty list for nonexistent key" do
      assert {:ok, []} = FerricStore.zrange("zset:noexist", 0, -1)
    end

    test "withscores option returns member-score tuples" do
      FerricStore.zadd("zset:ws", [{1.0, "a"}, {2.0, "b"}])
      {:ok, pairs} = FerricStore.zrange("zset:ws", 0, -1, withscores: true)
      assert length(pairs) == 2
      assert {first_member, first_score} = hd(pairs)
      assert first_member == "a"
      assert first_score == 1.0
    end

    test "returns empty list for out-of-range indices" do
      FerricStore.zadd("zset:oor", [{1.0, "a"}])
      assert {:ok, []} = FerricStore.zrange("zset:oor", 5, 10)
    end
  end

  # ===========================================================================
  # ZSCORE — sorted set score lookup
  # ===========================================================================

  describe "zscore/2" do
    test "returns the score of an existing member" do
      FerricStore.zadd("zset:score", [{42.5, "alice"}])
      assert {:ok, 42.5} = FerricStore.zscore("zset:score", "alice")
    end

    test "returns nil for missing member" do
      FerricStore.zadd("zset:score2", [{1.0, "a"}])
      assert {:ok, nil} = FerricStore.zscore("zset:score2", "z")
    end

    test "returns nil for nonexistent key" do
      assert {:ok, nil} = FerricStore.zscore("zset:noexist", "a")
    end

    test "score updates are reflected" do
      FerricStore.zadd("zset:score3", [{1.0, "a"}])
      FerricStore.zadd("zset:score3", [{99.0, "a"}])
      assert {:ok, 99.0} = FerricStore.zscore("zset:score3", "a")
    end
  end

  # ===========================================================================
  # ZCARD — sorted set cardinality
  # ===========================================================================

  describe "zcard/1" do
    test "returns the number of members" do
      FerricStore.zadd("zset:card", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, 3} = FerricStore.zcard("zset:card")
    end

    test "returns 0 for nonexistent key" do
      assert {:ok, 0} = FerricStore.zcard("zset:noexist")
    end
  end

  # ===========================================================================
  # ZREM — sorted set remove
  # ===========================================================================

  describe "zrem/2" do
    test "removes existing members" do
      FerricStore.zadd("zset:rem", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, 2} = FerricStore.zrem("zset:rem", ["a", "c"])
    end

    test "ignores nonexistent members" do
      FerricStore.zadd("zset:rem2", [{1.0, "a"}])
      assert {:ok, 0} = FerricStore.zrem("zset:rem2", ["z"])
    end

    test "removing from nonexistent key returns 0" do
      assert {:ok, 0} = FerricStore.zrem("zset:noexist", ["a"])
    end

    test "zrange reflects removal" do
      FerricStore.zadd("zset:rem3", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      FerricStore.zrem("zset:rem3", ["b"])
      assert {:ok, ["a", "c"]} = FerricStore.zrange("zset:rem3", 0, -1)
    end
  end

  # ===========================================================================
  # CAS — compare-and-swap
  # ===========================================================================

  describe "cas/4" do
    test "swaps when value matches" do
      FerricStore.set("cas:key", "old")
      assert {:ok, true} = FerricStore.cas("cas:key", "old", "new")
      assert {:ok, "new"} = FerricStore.get("cas:key")
    end

    test "does not swap when value does not match" do
      FerricStore.set("cas:key2", "current")
      assert {:ok, false} = FerricStore.cas("cas:key2", "wrong", "new")
      assert {:ok, "current"} = FerricStore.get("cas:key2")
    end

    test "returns nil when key does not exist" do
      assert {:ok, nil} = FerricStore.cas("cas:noexist", "expected", "new")
    end

    test "cas with TTL option" do
      FerricStore.set("cas:ttl", "old")
      assert {:ok, true} = FerricStore.cas("cas:ttl", "old", "new", ttl: :timer.hours(1))
      assert {:ok, "new"} = FerricStore.get("cas:ttl")
    end
  end

  # ===========================================================================
  # EXISTS — key existence check
  # ===========================================================================

  describe "exists/1" do
    test "returns true for existing key" do
      FerricStore.set("exists:key", "value")
      assert FerricStore.exists("exists:key") == true
    end

    test "returns false for nonexistent key" do
      assert FerricStore.exists("exists:noexist") == false
    end

    test "returns false after deletion" do
      FerricStore.set("exists:del", "value")
      FerricStore.del("exists:del")
      assert FerricStore.exists("exists:del") == false
    end
  end

  # ===========================================================================
  # KEYS — key listing
  # ===========================================================================

  describe "keys/1" do
    test "returns all keys with wildcard pattern" do
      FerricStore.set("keys:a", "1")
      FerricStore.set("keys:b", "2")
      {:ok, result} = FerricStore.keys("keys:*")
      assert "keys:a" in result
      assert "keys:b" in result
    end

    test "returns empty list when no keys match" do
      {:ok, result} = FerricStore.keys("nomatch:*")
      assert result == []
    end

    test "default pattern returns all keys" do
      FerricStore.set("kall:x", "1")
      {:ok, result} = FerricStore.keys()
      assert "kall:x" in result
    end
  end

  # ===========================================================================
  # DBSIZE — key count
  # ===========================================================================

  describe "dbsize/0" do
    test "returns the number of keys" do
      FerricStore.set("db:a", "1")
      FerricStore.set("db:b", "2")
      {:ok, size} = FerricStore.dbsize()
      assert size >= 2
    end
  end

  # ===========================================================================
  # FLUSHDB — delete all keys
  # ===========================================================================

  describe "flushdb/0" do
    test "deletes all keys in the sandbox" do
      FerricStore.set("flush:a", "1")
      FerricStore.set("flush:b", "2")
      assert :ok = FerricStore.flushdb()

      # Keys should be gone
      assert {:ok, nil} = FerricStore.get("flush:a")
      assert {:ok, nil} = FerricStore.get("flush:b")
    end

    test "removes stale probabilistic sidecar files" do
      ctx = FerricStore.Instance.get(:default)
      prob_dir = Path.join(Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0), "prob")
      stale_path = Path.join(prob_dir, "embedded_stale.bloom")

      File.mkdir_p!(prob_dir)
      File.write!(stale_path, "bits")

      assert :ok = FerricStore.flushdb()
      refute Ferricstore.FS.exists?(stale_path)
    end

    test "surfaces probabilistic directory fsync failures" do
      ctx = FerricStore.Instance.get(:default)
      prob_dir = Path.join(Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0), "prob")

      File.mkdir_p!(prob_dir)
      File.write!(Path.join(prob_dir, "embedded_fsync.bloom"), "bits")

      Process.put(:ferricstore_prob_command_fsync_dir_hook, fn
        ^prob_dir -> {:error, :eio}
        _path -> :ok
      end)

      on_exit(fn ->
        Process.delete(:ferricstore_prob_command_fsync_dir_hook)
      end)

      assert {:error, {:fsync_dir_failed, :flush_prob_dir, :eio}} = FerricStore.flushdb()
    end
  end

  # ===========================================================================
  # Pipeline — new commands
  # ===========================================================================

  describe "pipeline with new commands" do
    test "pipeline with LPUSH and RPUSH" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.rpush("pipe_list", ["a", "b"])
                 |> FerricStore.Pipe.lpush("pipe_list", ["c"])
               end)

      assert [{:ok, 2}, {:ok, 3}] = results
    end

    test "pipeline with SADD" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.sadd("pipe_set", ["a", "b", "c"])
               end)

      assert [{:ok, 3}] = results
    end

    test "pipeline with ZADD" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.zadd("pipe_zset", [{1.0, "a"}, {2.0, "b"}])
               end)

      assert [{:ok, 2}] = results
    end

    test "pipeline with EXPIRE" do
      FerricStore.set("pipe_expire", "value")

      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.expire("pipe_expire", :timer.hours(1))
               end)

      assert [{:ok, true}] = results
    end

    test "pipeline with mixed new and old commands" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.set("mixed:key", "val")
                 |> FerricStore.Pipe.rpush("mixed:list", ["a"])
                 |> FerricStore.Pipe.sadd("mixed:set", ["x"])
                 |> FerricStore.Pipe.get("mixed:key")
               end)

      assert [:ok, {:ok, 1}, {:ok, 1}, {:ok, "val"}] = results
    end
  end

  # ===========================================================================
  # Integration — combined data type operations
  # ===========================================================================

  describe "combined operations" do
    test "list push-pop cycle preserves FIFO ordering" do
      FerricStore.rpush("fifo", ["first", "second", "third"])
      assert {:ok, "first"} = FerricStore.lpop("fifo")
      assert {:ok, "second"} = FerricStore.lpop("fifo")
      assert {:ok, "third"} = FerricStore.lpop("fifo")
      assert {:ok, nil} = FerricStore.lpop("fifo")
    end

    test "list push-pop cycle preserves LIFO ordering" do
      FerricStore.rpush("lifo", ["first", "second", "third"])
      assert {:ok, "third"} = FerricStore.rpop("lifo")
      assert {:ok, "second"} = FerricStore.rpop("lifo")
      assert {:ok, "first"} = FerricStore.rpop("lifo")
    end

    test "sorted set maintains score ordering across operations" do
      FerricStore.zadd("zorder", [{10.0, "ten"}, {1.0, "one"}, {5.0, "five"}])
      assert {:ok, ["one", "five", "ten"]} = FerricStore.zrange("zorder", 0, -1)

      # Update a score to reorder
      FerricStore.zadd("zorder", [{100.0, "one"}])
      assert {:ok, ["five", "ten", "one"]} = FerricStore.zrange("zorder", 0, -1)
    end

    test "set operations are idempotent for adds" do
      FerricStore.sadd("idem", ["a"])
      FerricStore.sadd("idem", ["a"])
      assert {:ok, 1} = FerricStore.scard("idem")
    end

    test "different data types can coexist with different keys" do
      FerricStore.set("coexist:str", "hello")
      FerricStore.rpush("coexist:list", ["a"])
      FerricStore.sadd("coexist:set", ["x"])
      FerricStore.zadd("coexist:zset", [{1.0, "m"}])

      assert {:ok, "hello"} = FerricStore.get("coexist:str")
      assert {:ok, ["a"]} = FerricStore.lrange("coexist:list", 0, -1)
      {:ok, members} = FerricStore.smembers("coexist:set")
      assert members == ["x"]
      assert {:ok, ["m"]} = FerricStore.zrange("coexist:zset", 0, -1)
    end
  end
end
