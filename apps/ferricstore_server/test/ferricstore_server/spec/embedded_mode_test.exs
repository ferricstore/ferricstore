defmodule FerricstoreServer.Spec.EmbeddedModeTest do
  @moduledoc """
  Spec section 10: Embedded Mode tests.

  Verifies the direct Elixir API (`FerricStore.*`) works without any TCP
  connection. All operations route through `Ferricstore.Store.Router` in-process.

  Covers:
    - Embedded.get/put/delete work without TCP connection
    - Embedded.keys returns all keys
    - Embedded.dbsize returns correct count
    - Embedded.flush clears all keys
    - Embedded.incr/decr work on numeric values
    - Embedded pipeline executes multiple commands atomically
    - Hash operations (hset/hget/hgetall) work in embedded mode
    - List operations (lpush/rpush/lrange) work in embedded mode
    - Set operations (sadd/smembers/scard) work in embedded mode
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    :ok
  end

  # ---------------------------------------------------------------------------
  # get/put/delete — basic string operations without TCP
  # ---------------------------------------------------------------------------

  describe "get/set/del work without TCP connection" do
    test "set and get a string value" do
      assert :ok = FerricStore.set("emb:key", "hello")
      assert {:ok, "hello"} = FerricStore.get("emb:key")
    end

    test "get returns {:ok, nil} for nonexistent key" do
      assert {:ok, nil} = FerricStore.get("emb:nonexistent")
    end

    test "del removes an existing key" do
      FerricStore.set("emb:del_me", "value")
      result = FerricStore.del("emb:del_me")
      assert result == :ok or result == {:ok, 1}
      assert {:ok, nil} = FerricStore.get("emb:del_me")
    end

    test "del on nonexistent key returns :ok" do
      result = FerricStore.del("emb:never_existed")
      assert result == :ok or result == {:ok, 0}
    end

    test "set overwrites previous value" do
      FerricStore.set("emb:overwrite", "first")
      FerricStore.set("emb:overwrite", "second")
      assert {:ok, "second"} = FerricStore.get("emb:overwrite")
    end

    test "set with TTL option stores and retrieves value" do
      assert :ok = FerricStore.set("emb:ttl", "val", ttl: :timer.hours(1))
      assert {:ok, "val"} = FerricStore.get("emb:ttl")
    end

    test "binary values are preserved" do
      binary = <<0, 1, 2, 128, 255>>
      assert :ok = FerricStore.set("emb:bin", binary)
      assert {:ok, ^binary} = FerricStore.get("emb:bin")
    end

    test "empty string key and value work" do
      assert :ok = FerricStore.set("", "")
      assert {:ok, ""} = FerricStore.get("")
    end
  end

  # ---------------------------------------------------------------------------
  # keys — returns all keys
  # ---------------------------------------------------------------------------

  describe "keys returns all keys" do
    test "returns matching keys with wildcard pattern" do
      FerricStore.set("emb:keys:a", "1")
      FerricStore.set("emb:keys:b", "2")
      FerricStore.set("emb:keys:c", "3")

      {:ok, result} = FerricStore.keys("emb:keys:*")
      assert "emb:keys:a" in result
      assert "emb:keys:b" in result
      assert "emb:keys:c" in result
    end

    test "returns empty list when no keys match" do
      {:ok, result} = FerricStore.keys("emb:nomatch:*")
      assert result == []
    end

    test "default pattern returns all keys in sandbox" do
      FerricStore.set("emb:allk:x", "1")
      {:ok, result} = FerricStore.keys()
      assert "emb:allk:x" in result
    end

    test "keys are returned as strings" do
      FerricStore.set("emb:strkey", "v")
      {:ok, result} = FerricStore.keys("emb:strkey")
      assert Enum.all?(result, &is_binary/1)
    end
  end

  # ---------------------------------------------------------------------------
  # dbsize — returns correct count
  # ---------------------------------------------------------------------------

  describe "dbsize returns correct count" do
    test "returns 0 when no keys exist in sandbox" do
      {:ok, size} = FerricStore.dbsize()
      assert size == 0
    end

    test "returns correct count after adding keys" do
      FerricStore.set("emb:db:a", "1")
      FerricStore.set("emb:db:b", "2")
      FerricStore.set("emb:db:c", "3")

      {:ok, size} = FerricStore.dbsize()
      assert size == 3
    end

    test "count decreases after deleting keys" do
      FerricStore.set("emb:dbd:x", "1")
      FerricStore.set("emb:dbd:y", "2")
      {:ok, before_size} = FerricStore.dbsize()

      FerricStore.del("emb:dbd:x")
      {:ok, after_size} = FerricStore.dbsize()

      assert after_size == before_size - 1
    end

    test "dbsize returns a non-negative integer" do
      {:ok, size} = FerricStore.dbsize()
      assert is_integer(size)
      assert size >= 0
    end
  end

  # ---------------------------------------------------------------------------
  # flush — clears all keys
  # ---------------------------------------------------------------------------

  describe "flushdb clears all keys" do
    test "removes all keys in sandbox" do
      FerricStore.set("emb:flush:a", "1")
      FerricStore.set("emb:flush:b", "2")
      FerricStore.set("emb:flush:c", "3")

      {:ok, before_size} = FerricStore.dbsize()
      assert before_size == 3

      assert :ok = FerricStore.flushdb()

      {:ok, after_size} = FerricStore.dbsize()
      assert after_size == 0
    end

    test "get returns nil for flushed keys" do
      FerricStore.set("emb:flush:x", "value")
      FerricStore.flushdb()

      assert {:ok, nil} = FerricStore.get("emb:flush:x")
    end

    test "flushdb returns :ok" do
      assert :ok = FerricStore.flushdb()
    end

    test "can set new keys after flush" do
      FerricStore.set("emb:postflush", "before")
      FerricStore.flushdb()
      FerricStore.set("emb:postflush", "after")

      assert {:ok, "after"} = FerricStore.get("emb:postflush")
    end
  end

  # ---------------------------------------------------------------------------
  # incr/decr — numeric value operations
  # ---------------------------------------------------------------------------

  describe "incr/decr work on numeric values" do
    test "incr initializes nonexistent key from 0 to 1" do
      assert {:ok, 1} = FerricStore.incr("emb:ctr:new")
    end

    test "incr increments existing integer value" do
      FerricStore.set("emb:ctr:existing", "5")
      assert {:ok, 6} = FerricStore.incr("emb:ctr:existing")
    end

    test "multiple increments accumulate" do
      assert {:ok, 1} = FerricStore.incr("emb:ctr:multi")
      assert {:ok, 2} = FerricStore.incr("emb:ctr:multi")
      assert {:ok, 3} = FerricStore.incr("emb:ctr:multi")
    end

    test "incr on non-integer value returns error" do
      FerricStore.set("emb:ctr:bad", "not_a_number")
      assert {:error, msg} = FerricStore.incr("emb:ctr:bad")
      assert msg =~ "not an integer"
    end

    test "decrement via incr_by with negative amount" do
      FerricStore.set("emb:ctr:decr", "10")
      assert {:ok, 7} = FerricStore.incr_by("emb:ctr:decr", -3)
    end

    test "incr_by initializes from 0" do
      assert {:ok, 50} = FerricStore.incr_by("emb:ctr:fresh", 50)
    end

    test "incr_by with zero returns current value" do
      FerricStore.set("emb:ctr:zero", "42")
      assert {:ok, 42} = FerricStore.incr_by("emb:ctr:zero", 0)
    end

    test "incr on negative integer works" do
      FerricStore.set("emb:ctr:neg", "-5")
      assert {:ok, -4} = FerricStore.incr("emb:ctr:neg")
    end

    test "incr stores result as string representation" do
      FerricStore.incr("emb:ctr:str")
      assert {:ok, "1"} = FerricStore.get("emb:ctr:str")
    end
  end

  # ---------------------------------------------------------------------------
  # pipeline — executes multiple commands atomically
  # ---------------------------------------------------------------------------

  describe "pipeline executes multiple commands" do
    test "executes multiple SET commands" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.set("emb:pipe:a", "val_a")
                 |> FerricStore.Pipe.set("emb:pipe:b", "val_b")
               end)

      assert results == [:ok, :ok]
      assert {:ok, "val_a"} = FerricStore.get("emb:pipe:a")
      assert {:ok, "val_b"} = FerricStore.get("emb:pipe:b")
    end

    test "executes mixed SET and GET commands" do
      FerricStore.set("emb:pipe:existing", "hello")

      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.set("emb:pipe:new", "world")
                 |> FerricStore.Pipe.get("emb:pipe:existing")
                 |> FerricStore.Pipe.get("emb:pipe:new")
               end)

      assert results == [:ok, {:ok, "hello"}, {:ok, "world"}]
    end

    test "pipeline with INCR" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.incr("emb:pipe:ctr")
                 |> FerricStore.Pipe.incr("emb:pipe:ctr")
                 |> FerricStore.Pipe.incr("emb:pipe:ctr")
               end)

      assert results == [{:ok, 1}, {:ok, 2}, {:ok, 3}]
    end

    test "pipeline with DEL" do
      FerricStore.set("emb:pipe:target", "value")

      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.del("emb:pipe:target")
                 |> FerricStore.Pipe.get("emb:pipe:target")
               end)

      assert results == [:ok, {:ok, nil}]
    end

    test "empty pipeline returns empty results" do
      assert {:ok, []} =
               FerricStore.pipeline(fn pipe ->
                 pipe
               end)
    end

    test "pipeline commands execute in order" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.set("emb:pipe:order", "first")
                 |> FerricStore.Pipe.get("emb:pipe:order")
                 |> FerricStore.Pipe.set("emb:pipe:order", "second")
                 |> FerricStore.Pipe.get("emb:pipe:order")
               end)

      assert results == [:ok, {:ok, "first"}, :ok, {:ok, "second"}]
    end

    test "pipeline with HSET and HGET" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.hset("emb:pipe:hash", %{"field" => "value"})
                 |> FerricStore.Pipe.hget("emb:pipe:hash", "field")
               end)

      assert results == [:ok, {:ok, "value"}]
    end

    test "pipeline with mixed data structure commands" do
      assert {:ok, results} =
               FerricStore.pipeline(fn pipe ->
                 pipe
                 |> FerricStore.Pipe.set("emb:pipe:mix:str", "val")
                 |> FerricStore.Pipe.rpush("emb:pipe:mix:list", ["a"])
                 |> FerricStore.Pipe.sadd("emb:pipe:mix:set", ["x"])
                 |> FerricStore.Pipe.get("emb:pipe:mix:str")
               end)

      assert [:ok, {:ok, 1}, {:ok, 1}, {:ok, "val"}] = results
    end
  end

  # ---------------------------------------------------------------------------
  # Hash operations — hset/hget/hgetall
  # ---------------------------------------------------------------------------

  describe "hash operations work in embedded mode" do
    test "hset and hget a single field" do
      assert :ok = FerricStore.hset("emb:hash:1", %{"name" => "alice"})
      assert {:ok, "alice"} = FerricStore.hget("emb:hash:1", "name")
    end

    test "hset multiple fields at once" do
      assert :ok = FerricStore.hset("emb:hash:2", %{"name" => "bob", "age" => "25"})
      assert {:ok, "bob"} = FerricStore.hget("emb:hash:2", "name")
      assert {:ok, "25"} = FerricStore.hget("emb:hash:2", "age")
    end

    test "hget on nonexistent hash returns {:ok, nil}" do
      assert {:ok, nil} = FerricStore.hget("emb:hash:missing", "field")
    end

    test "hget on nonexistent field returns {:ok, nil}" do
      FerricStore.hset("emb:hash:partial", %{"exists" => "yes"})
      assert {:ok, nil} = FerricStore.hget("emb:hash:partial", "missing")
    end

    test "hgetall returns all fields and values" do
      FerricStore.hset("emb:hash:all", %{"a" => "1", "b" => "2", "c" => "3"})
      assert {:ok, map} = FerricStore.hgetall("emb:hash:all")
      assert map == %{"a" => "1", "b" => "2", "c" => "3"}
    end

    test "hgetall returns empty map for nonexistent hash" do
      assert {:ok, %{}} = FerricStore.hgetall("emb:hash:nope")
    end

    test "hset merges with existing fields" do
      FerricStore.hset("emb:hash:merge", %{"a" => "1"})
      FerricStore.hset("emb:hash:merge", %{"b" => "2"})
      assert {:ok, "1"} = FerricStore.hget("emb:hash:merge", "a")
      assert {:ok, "2"} = FerricStore.hget("emb:hash:merge", "b")
    end

    test "hset overwrites existing field value" do
      FerricStore.hset("emb:hash:overwrite", %{"field" => "old"})
      FerricStore.hset("emb:hash:overwrite", %{"field" => "new"})
      assert {:ok, "new"} = FerricStore.hget("emb:hash:overwrite", "field")
    end

    test "hset with atom keys converts to strings" do
      FerricStore.hset("emb:hash:atoms", %{name: "alice", age: 30})
      assert {:ok, "alice"} = FerricStore.hget("emb:hash:atoms", "name")
      assert {:ok, "30"} = FerricStore.hget("emb:hash:atoms", "age")
    end
  end

  # ---------------------------------------------------------------------------
  # List operations — lpush/rpush/lrange
  # ---------------------------------------------------------------------------

  describe "list operations work in embedded mode" do
    test "lpush creates a new list" do
      assert {:ok, 3} = FerricStore.lpush("emb:list:lpush", ["a", "b", "c"])
    end

    test "rpush creates a new list" do
      assert {:ok, 3} = FerricStore.rpush("emb:list:rpush", ["a", "b", "c"])
    end

    test "lpush elements appear in reverse order (Redis LPUSH semantics)" do
      FerricStore.lpush("emb:list:lorder", ["a", "b", "c"])
      assert {:ok, ["c", "b", "a"]} = FerricStore.lrange("emb:list:lorder", 0, -1)
    end

    test "rpush elements appear in insertion order" do
      FerricStore.rpush("emb:list:rorder", ["a", "b", "c"])
      assert {:ok, ["a", "b", "c"]} = FerricStore.lrange("emb:list:rorder", 0, -1)
    end

    test "lrange returns sub-range" do
      FerricStore.rpush("emb:list:range", ["a", "b", "c", "d"])
      assert {:ok, ["b", "c"]} = FerricStore.lrange("emb:list:range", 1, 2)
    end

    test "lrange returns empty list for nonexistent key" do
      assert {:ok, []} = FerricStore.lrange("emb:list:noexist", 0, -1)
    end

    test "lrange with negative indices works" do
      FerricStore.rpush("emb:list:neg", ["a", "b", "c", "d"])
      assert {:ok, ["c", "d"]} = FerricStore.lrange("emb:list:neg", -2, -1)
    end

    test "lpop removes and returns from the left" do
      FerricStore.rpush("emb:list:lpop", ["a", "b", "c"])
      assert {:ok, "a"} = FerricStore.lpop("emb:list:lpop")
      assert {:ok, ["b", "c"]} = FerricStore.lrange("emb:list:lpop", 0, -1)
    end

    test "rpop removes and returns from the right" do
      FerricStore.rpush("emb:list:rpop", ["a", "b", "c"])
      assert {:ok, "c"} = FerricStore.rpop("emb:list:rpop")
      assert {:ok, ["a", "b"]} = FerricStore.lrange("emb:list:rpop", 0, -1)
    end

    test "llen returns the length of a list" do
      FerricStore.rpush("emb:list:len", ["a", "b", "c"])
      assert {:ok, 3} = FerricStore.llen("emb:list:len")
    end

    test "llen returns 0 for nonexistent key" do
      assert {:ok, 0} = FerricStore.llen("emb:list:noexist")
    end
  end

  # ---------------------------------------------------------------------------
  # Set operations — sadd/smembers/scard
  # ---------------------------------------------------------------------------

  describe "set operations work in embedded mode" do
    test "sadd adds new members and returns count" do
      assert {:ok, 3} = FerricStore.sadd("emb:set:add", ["a", "b", "c"])
    end

    test "sadd ignores duplicate members" do
      FerricStore.sadd("emb:set:dup", ["a", "b"])
      assert {:ok, 1} = FerricStore.sadd("emb:set:dup", ["b", "c"])
    end

    test "smembers returns all members" do
      FerricStore.sadd("emb:set:members", ["a", "b", "c"])
      {:ok, members} = FerricStore.smembers("emb:set:members")
      assert Enum.sort(members) == ["a", "b", "c"]
    end

    test "smembers returns empty list for nonexistent set" do
      assert {:ok, []} = FerricStore.smembers("emb:set:noexist")
    end

    test "scard returns the number of members" do
      FerricStore.sadd("emb:set:card", ["a", "b", "c"])
      assert {:ok, 3} = FerricStore.scard("emb:set:card")
    end

    test "scard returns 0 for nonexistent set" do
      assert {:ok, 0} = FerricStore.scard("emb:set:noexist")
    end

    test "sismember returns true for existing member" do
      FerricStore.sadd("emb:set:ism", ["a", "b"])
      result = FerricStore.sismember("emb:set:ism", "a")
      assert result == true or result == {:ok, true}
    end

    test "sismember returns false for missing member" do
      FerricStore.sadd("emb:set:ism2", ["a"])
      result = FerricStore.sismember("emb:set:ism2", "z")
      assert result == false or result == {:ok, false}
    end

    test "srem removes members and returns count" do
      FerricStore.sadd("emb:set:rem", ["a", "b", "c"])
      assert {:ok, 2} = FerricStore.srem("emb:set:rem", ["a", "c"])

      {:ok, members} = FerricStore.smembers("emb:set:rem")
      assert members == ["b"]
    end

    test "srem on nonexistent members returns 0" do
      FerricStore.sadd("emb:set:rem2", ["a"])
      assert {:ok, 0} = FerricStore.srem("emb:set:rem2", ["z"])
    end
  end
end
