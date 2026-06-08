defmodule Ferricstore.Raft.DataStructureWritePathTest do
  @moduledoc """
  Raft edge-case tests for data structure write commands.

  Verifies that Hash, List, Set, and Sorted Set commands operate correctly
  through the full Raft path: Shard GenServer -> Batcher -> ra -> StateMachine
  -> ETS + Bitcask.

  Each test dispatches commands via the store map, which routes compound
  operations through the Shard GenServer. The Shard delegates writes to
  the Raft Batcher, which batches them into ra log entries. The StateMachine
  `apply/3` callback then writes to ETS and Bitcask deterministically.
  """

  use ExUnit.Case, async: false
  @moduletag :raft

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_default_pipeline_ready()

    :ok
  end

  setup do
    ShardHelpers.wait_default_pipeline_ready()

    on_exit(fn -> ShardHelpers.wait_default_pipeline_ready() end)
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Generate unique keys to avoid cross-test pollution
  defp ukey(base), do: "raft_ds_#{base}_#{:rand.uniform(9_999_999)}"

  # Build the store map that routes through the Shard GenServer,
  # which in turn routes writes through Raft.
  defp build_store(redis_key) do
    shard = Router.shard_name(FerricStore.Instance.get(:default), Router.shard_for(FerricStore.Instance.get(:default), redis_key))

    %{
      get: fn k -> Router.get(FerricStore.Instance.get(:default), k) end,
      get_meta: fn k -> Router.get_meta(FerricStore.Instance.get(:default), k) end,
      put: fn k, v, e -> Router.put(FerricStore.Instance.get(:default), k, v, e) end,
      delete: fn k -> Router.delete(FerricStore.Instance.get(:default), k) end,
      exists?: fn k -> Router.exists?(FerricStore.Instance.get(:default), k) end,
      keys: fn -> Router.keys(FerricStore.Instance.get(:default)) end,
      list_op: fn k, op -> Router.list_op(FerricStore.Instance.get(:default), k, op) end,
      compound_get: fn _redis_key, compound_key ->
        GenServer.call(shard, {:compound_get, redis_key, compound_key})
      end,
      compound_get_meta: fn _redis_key, compound_key ->
        GenServer.call(shard, {:compound_get_meta, redis_key, compound_key})
      end,
      compound_put: fn _redis_key, compound_key, value, expire_at_ms ->
        GenServer.call(shard, {:compound_put, redis_key, compound_key, value, expire_at_ms})
      end,
      compound_delete: fn _redis_key, compound_key ->
        GenServer.call(shard, {:compound_delete, redis_key, compound_key})
      end,
      compound_scan: fn _redis_key, prefix ->
        GenServer.call(shard, {:compound_scan, redis_key, prefix})
      end,
      compound_count: fn _redis_key, prefix ->
        GenServer.call(shard, {:compound_count, redis_key, prefix})
      end,
      compound_delete_prefix: fn _redis_key, prefix ->
        GenServer.call(shard, {:compound_delete_prefix, redis_key, prefix})
      end
    }
  end

  # Build a store map where compound operations route to the redis_key's
  # owning shard. Unlike build_store/1, the redis_key in the callbacks is
  # passed through from the caller (the command module), so multi-key
  # operations that query the type registry with the actual redis_key
  # still route correctly.
  defp build_dynamic_store do
    %{
      get: fn k -> Router.get(FerricStore.Instance.get(:default), k) end,
      get_meta: fn k -> Router.get_meta(FerricStore.Instance.get(:default), k) end,
      put: fn k, v, e -> Router.put(FerricStore.Instance.get(:default), k, v, e) end,
      delete: fn k -> Router.delete(FerricStore.Instance.get(:default), k) end,
      exists?: fn k -> Router.exists?(FerricStore.Instance.get(:default), k) end,
      keys: fn -> Router.keys(FerricStore.Instance.get(:default)) end,
      list_op: fn k, op -> Router.list_op(FerricStore.Instance.get(:default), k, op) end,
      compound_get: fn redis_key, compound_key ->
        shard = Router.shard_name(FerricStore.Instance.get(:default), Router.shard_for(FerricStore.Instance.get(:default), redis_key))
        GenServer.call(shard, {:compound_get, redis_key, compound_key})
      end,
      compound_get_meta: fn redis_key, compound_key ->
        shard = Router.shard_name(FerricStore.Instance.get(:default), Router.shard_for(FerricStore.Instance.get(:default), redis_key))
        GenServer.call(shard, {:compound_get_meta, redis_key, compound_key})
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        shard = Router.shard_name(FerricStore.Instance.get(:default), Router.shard_for(FerricStore.Instance.get(:default), redis_key))
        GenServer.call(shard, {:compound_put, redis_key, compound_key, value, expire_at_ms})
      end,
      compound_delete: fn redis_key, compound_key ->
        shard = Router.shard_name(FerricStore.Instance.get(:default), Router.shard_for(FerricStore.Instance.get(:default), redis_key))
        GenServer.call(shard, {:compound_delete, redis_key, compound_key})
      end,
      compound_scan: fn redis_key, prefix ->
        shard = Router.shard_name(FerricStore.Instance.get(:default), Router.shard_for(FerricStore.Instance.get(:default), redis_key))
        GenServer.call(shard, {:compound_scan, redis_key, prefix})
      end,
      compound_count: fn redis_key, prefix ->
        shard = Router.shard_name(FerricStore.Instance.get(:default), Router.shard_for(FerricStore.Instance.get(:default), redis_key))
        GenServer.call(shard, {:compound_count, redis_key, prefix})
      end,
      compound_delete_prefix: fn redis_key, prefix ->
        shard = Router.shard_name(FerricStore.Instance.get(:default), Router.shard_for(FerricStore.Instance.get(:default), redis_key))
        GenServer.call(shard, {:compound_delete_prefix, redis_key, prefix})
      end
    }
  end

  alias Ferricstore.Commands.Hash
  alias Ferricstore.Commands.List, as: ListCmd
  alias Ferricstore.Commands.Set, as: SetCmd
  alias Ferricstore.Commands.SortedSet

  # ===========================================================================
  # Hash through Raft
  # ===========================================================================

  describe "HSET multiple fields in one call -- all persisted through Raft" do
    test "all fields from a single HSET with multiple field-value pairs are readable" do
      key = ukey("hset_multi")
      store = build_store(key)

      # HSET key f1 v1 f2 v2 f3 v3 -- returns count of newly added fields
      result = Hash.handle("HSET", [key, "f1", "v1", "f2", "v2", "f3", "v3"], store)
      assert result == 3

      # All fields should be readable
      assert Hash.handle("HGET", [key, "f1"], store) == "v1"
      assert Hash.handle("HGET", [key, "f2"], store) == "v2"
      assert Hash.handle("HGET", [key, "f3"], store) == "v3"

      # HLEN should report 3 fields
      assert Hash.handle("HLEN", [key], store) == 3
    end

    test "HSET overwrites existing fields and counts only new ones" do
      key = ukey("hset_overwrite")
      store = build_store(key)

      # Set initial fields
      assert 2 = Hash.handle("HSET", [key, "f1", "v1", "f2", "v2"], store)

      # Overwrite f1, add f3 -- only f3 is new
      assert 1 = Hash.handle("HSET", [key, "f1", "updated", "f3", "v3"], store)

      assert Hash.handle("HGET", [key, "f1"], store) == "updated"
      assert Hash.handle("HGET", [key, "f2"], store) == "v2"
      assert Hash.handle("HGET", [key, "f3"], store) == "v3"
    end
  end

  describe "HDEL field that doesn't exist -- returns 0 through Raft" do
    test "deleting a non-existent field returns 0" do
      key = ukey("hdel_missing")
      store = build_store(key)

      # Create hash with one field
      Hash.handle("HSET", [key, "exists", "val"], store)

      # Try to delete a field that does not exist
      assert Hash.handle("HDEL", [key, "nope"], store) == 0

      # Original field should still be there
      assert Hash.handle("HGET", [key, "exists"], store) == "val"
    end

    test "deleting mix of existing and non-existing fields returns correct count" do
      key = ukey("hdel_mixed")
      store = build_store(key)

      Hash.handle("HSET", [key, "a", "1", "b", "2", "c", "3"], store)

      # Delete a (exists), x (not), b (exists) -> should return 2
      assert Hash.handle("HDEL", [key, "a", "x", "b"], store) == 2

      assert Hash.handle("HGET", [key, "a"], store) == nil
      assert Hash.handle("HGET", [key, "b"], store) == nil
      assert Hash.handle("HGET", [key, "c"], store) == "3"
    end
  end

  describe "HINCRBY through Raft -- atomic field increment" do
    test "HINCRBY on non-existent field initializes to increment" do
      key = ukey("hincrby_new")
      store = build_store(key)

      assert Hash.handle("HINCRBY", [key, "counter", "5"], store) == 5
      assert Hash.handle("HGET", [key, "counter"], store) == "5"
    end

    test "HINCRBY on existing field increments correctly" do
      key = ukey("hincrby_existing")
      store = build_store(key)

      Hash.handle("HSET", [key, "counter", "10"], store)
      assert Hash.handle("HINCRBY", [key, "counter", "3"], store) == 13
      assert Hash.handle("HGET", [key, "counter"], store) == "13"
    end

    test "HINCRBY with negative value decrements" do
      key = ukey("hincrby_negative")
      store = build_store(key)

      Hash.handle("HSET", [key, "counter", "100"], store)
      assert Hash.handle("HINCRBY", [key, "counter", "-7"], store) == 93
      assert Hash.handle("HGET", [key, "counter"], store) == "93"
    end

    test "sequential HINCRBY calls produce correct total" do
      key = ukey("hincrby_seq")
      store = build_store(key)

      for _ <- 1..10 do
        Hash.handle("HINCRBY", [key, "counter", "1"], store)
      end

      assert Hash.handle("HGET", [key, "counter"], store) == "10"
    end
  end

  describe "HGETALL after multiple HSET calls -- returns all fields through Raft" do
    test "HGETALL returns all field-value pairs in flat list" do
      key = ukey("hgetall_multi")
      store = build_store(key)

      Hash.handle("HSET", [key, "name", "alice"], store)
      Hash.handle("HSET", [key, "age", "30"], store)
      Hash.handle("HSET", [key, "city", "nyc"], store)

      result = Hash.handle("HGETALL", [key], store)

      # Result is a flat list: [field1, value1, field2, value2, ...]
      result_map = result |> Enum.chunk_every(2) |> Map.new(fn [k, v] -> {k, v} end)

      assert result_map == %{"name" => "alice", "age" => "30", "city" => "nyc"}
    end

    test "HGETALL on non-existent key returns empty list" do
      key = ukey("hgetall_empty")
      store = build_store(key)

      assert Hash.handle("HGETALL", [key], store) == []
    end
  end

  describe "DEL on hash cleans up all compound keys through Raft" do
    test "deleting a hash key removes all fields and type metadata" do
      key = ukey("del_hash")
      store = build_store(key)

      # Build a hash with multiple fields
      Hash.handle("HSET", [key, "f1", "v1", "f2", "v2", "f3", "v3"], store)

      # Verify fields exist
      assert Hash.handle("HLEN", [key], store) == 3

      # DEL the key via the compound_delete_prefix path
      prefix = Ferricstore.Store.CompoundKey.hash_prefix(key)
      store.compound_delete_prefix.(key, prefix)

      # Also delete the type registry entry
      type_key = Ferricstore.Store.CompoundKey.type_key(key)
      store.compound_delete.(key, type_key)

      # All fields should be gone
      assert Hash.handle("HGETALL", [key], store) == []
      assert Hash.handle("HLEN", [key], store) == 0
    end
  end

  # ===========================================================================
  # List through Raft
  # ===========================================================================

  describe "LPUSH multiple elements -- all added in correct order through Raft" do
    test "LPUSH multiple elements are stored in reverse push order" do
      key = ukey("lpush_multi")
      store = build_dynamic_store()

      # LPUSH pushes elements one at a time from left, so LPUSH a b c -> [c, b, a]
      result = ListCmd.handle("LPUSH", [key, "a", "b", "c"], store)
      assert result == 3

      # LRANGE 0 -1 should return all elements
      assert ListCmd.handle("LRANGE", [key, "0", "-1"], store) == ["c", "b", "a"]
    end

    test "sequential LPUSH calls prepend correctly" do
      key = ukey("lpush_seq")
      store = build_dynamic_store()

      ListCmd.handle("LPUSH", [key, "first"], store)
      ListCmd.handle("LPUSH", [key, "second"], store)
      ListCmd.handle("LPUSH", [key, "third"], store)

      assert ListCmd.handle("LRANGE", [key, "0", "-1"], store) == ["third", "second", "first"]
    end
  end

  describe "RPUSH then LPOP -- FIFO behavior preserved through Raft" do
    test "RPUSH then LPOP produces FIFO ordering" do
      key = ukey("fifo")
      store = build_dynamic_store()

      # RPUSH appends to tail
      ListCmd.handle("RPUSH", [key, "first", "second", "third"], store)

      # LPOP removes from head -- FIFO
      assert ListCmd.handle("LPOP", [key], store) == "first"
      assert ListCmd.handle("LPOP", [key], store) == "second"
      assert ListCmd.handle("LPOP", [key], store) == "third"
    end

    test "interleaved RPUSH and LPOP maintain FIFO order" do
      key = ukey("fifo_interleaved")
      store = build_dynamic_store()

      ListCmd.handle("RPUSH", [key, "a", "b"], store)
      assert ListCmd.handle("LPOP", [key], store) == "a"

      ListCmd.handle("RPUSH", [key, "c"], store)
      assert ListCmd.handle("LPOP", [key], store) == "b"
      assert ListCmd.handle("LPOP", [key], store) == "c"
    end
  end

  describe "LRANGE after multiple pushes -- correct range returned through Raft" do
    test "LRANGE returns the correct subrange" do
      key = ukey("lrange")
      store = build_dynamic_store()

      ListCmd.handle("RPUSH", [key, "a", "b", "c", "d", "e"], store)

      # LRANGE 1 3 -> elements at indices 1, 2, 3
      assert ListCmd.handle("LRANGE", [key, "1", "3"], store) == ["b", "c", "d"]
    end

    test "LRANGE with negative indices" do
      key = ukey("lrange_neg")
      store = build_dynamic_store()

      ListCmd.handle("RPUSH", [key, "a", "b", "c", "d"], store)

      # LRANGE -2 -1 -> last two elements
      assert ListCmd.handle("LRANGE", [key, "-2", "-1"], store) == ["c", "d"]
    end

    test "LRANGE on non-existent key returns empty list" do
      key = ukey("lrange_empty")
      store = build_dynamic_store()

      assert ListCmd.handle("LRANGE", [key, "0", "-1"], store) == []
    end
  end

  describe "LLEN accurate after push/pop through Raft" do
    test "LLEN reflects pushes and pops" do
      key = ukey("llen")
      store = build_dynamic_store()

      assert ListCmd.handle("LLEN", [key], store) == 0

      ListCmd.handle("RPUSH", [key, "a", "b", "c"], store)
      assert ListCmd.handle("LLEN", [key], store) == 3

      ListCmd.handle("LPOP", [key], store)
      assert ListCmd.handle("LLEN", [key], store) == 2

      ListCmd.handle("RPUSH", [key, "d", "e"], store)
      assert ListCmd.handle("LLEN", [key], store) == 4
    end
  end

  describe "empty list after all elements popped -- key removed through Raft" do
    test "popping all elements removes the key" do
      key = ukey("list_auto_del")
      store = build_dynamic_store()

      ListCmd.handle("RPUSH", [key, "only"], store)
      assert ListCmd.handle("LLEN", [key], store) == 1

      assert ListCmd.handle("LPOP", [key], store) == "only"

      # Key should no longer exist
      assert ListCmd.handle("LLEN", [key], store) == 0
      assert Router.get(FerricStore.Instance.get(:default), key) == nil
    end

    test "popping from already-empty key returns nil" do
      key = ukey("list_pop_empty")
      store = build_dynamic_store()

      assert ListCmd.handle("LPOP", [key], store) == nil
    end
  end

  # ===========================================================================
  # Set through Raft
  # ===========================================================================

  describe "SADD duplicate member -- returns 0 through Raft" do
    test "adding a duplicate member returns 0 (not added)" do
      key = ukey("sadd_dup")
      store = build_store(key)

      # First add -- new member
      assert SetCmd.handle("SADD", [key, "member1"], store) == 1

      # Second add of same member -- duplicate
      assert SetCmd.handle("SADD", [key, "member1"], store) == 0
    end

    test "adding mix of new and duplicate members returns count of new only" do
      key = ukey("sadd_mix")
      store = build_store(key)

      assert SetCmd.handle("SADD", [key, "a", "b", "c"], store) == 3

      # a is duplicate, d is new
      assert SetCmd.handle("SADD", [key, "a", "d"], store) == 1
    end
  end

  describe "SREM non-existent member -- returns 0 through Raft" do
    test "removing a non-existent member returns 0" do
      key = ukey("srem_missing")
      store = build_store(key)

      SetCmd.handle("SADD", [key, "a", "b"], store)

      # Remove a member that doesn't exist
      assert SetCmd.handle("SREM", [key, "z"], store) == 0
    end

    test "removing mix of existing and non-existing returns correct count" do
      key = ukey("srem_mix")
      store = build_store(key)

      SetCmd.handle("SADD", [key, "a", "b", "c"], store)

      # Remove a (exists), x (not), b (exists) -> should return 2
      assert SetCmd.handle("SREM", [key, "a", "x", "b"], store) == 2
    end
  end

  describe "SMEMBERS after SADD/SREM -- correct membership through Raft" do
    test "SMEMBERS reflects adds and removes" do
      key = ukey("smembers")
      store = build_store(key)

      SetCmd.handle("SADD", [key, "a", "b", "c", "d"], store)
      SetCmd.handle("SREM", [key, "b", "d"], store)

      members = SetCmd.handle("SMEMBERS", [key], store)
      assert Enum.sort(members) == ["a", "c"]
    end

    test "SMEMBERS on non-existent key returns empty list" do
      key = ukey("smembers_empty")
      store = build_store(key)

      assert SetCmd.handle("SMEMBERS", [key], store) == []
    end

    test "SISMEMBER reflects current membership" do
      key = ukey("sismember")
      store = build_store(key)

      SetCmd.handle("SADD", [key, "x", "y"], store)

      assert SetCmd.handle("SISMEMBER", [key, "x"], store) == 1
      assert SetCmd.handle("SISMEMBER", [key, "z"], store) == 0

      SetCmd.handle("SREM", [key, "x"], store)
      assert SetCmd.handle("SISMEMBER", [key, "x"], store) == 0
    end
  end

  describe "SCARD accurate through Raft" do
    test "SCARD reflects adds and removes" do
      key = ukey("scard")
      store = build_store(key)

      assert SetCmd.handle("SCARD", [key], store) == 0

      SetCmd.handle("SADD", [key, "a", "b", "c"], store)
      assert SetCmd.handle("SCARD", [key], store) == 3

      SetCmd.handle("SREM", [key, "b"], store)
      assert SetCmd.handle("SCARD", [key], store) == 2

      # Adding a duplicate should not change cardinality
      SetCmd.handle("SADD", [key, "a"], store)
      assert SetCmd.handle("SCARD", [key], store) == 2
    end
  end

  # ===========================================================================
  # Sorted Set through Raft
  # ===========================================================================

  describe "ZADD with NX flag -- only adds new members through Raft" do
    test "NX adds members that don't exist" do
      key = ukey("zadd_nx")
      store = build_store(key)

      # Add initial members
      assert SortedSet.handle("ZADD", [key, "1.0", "a", "2.0", "b"], store) == 2

      # NX: only add new members, skip existing ones
      assert SortedSet.handle("ZADD", [key, "NX", "99.0", "a", "3.0", "c"], store) == 1

      # "a" should still have score 1.0 (not updated to 99.0)
      assert SortedSet.handle("ZSCORE", [key, "a"], store) == "1.0"
      # "c" should have been added with score 3.0
      assert SortedSet.handle("ZSCORE", [key, "c"], store) == "3.0"
    end

    test "NX with all existing members returns 0" do
      key = ukey("zadd_nx_all_existing")
      store = build_store(key)

      SortedSet.handle("ZADD", [key, "1.0", "a", "2.0", "b"], store)

      # Both a and b already exist -- nothing added
      assert SortedSet.handle("ZADD", [key, "NX", "10.0", "a", "20.0", "b"], store) == 0

      # Scores unchanged
      assert SortedSet.handle("ZSCORE", [key, "a"], store) == "1.0"
      assert SortedSet.handle("ZSCORE", [key, "b"], store) == "2.0"
    end
  end

  describe "ZADD with XX flag -- only updates existing members through Raft" do
    test "XX updates existing members but does not add new ones" do
      key = ukey("zadd_xx")
      store = build_store(key)

      SortedSet.handle("ZADD", [key, "1.0", "a", "2.0", "b"], store)

      # XX: only update existing, skip new
      # "a" exists -> update score; "c" doesn't exist -> skip
      assert SortedSet.handle("ZADD", [key, "XX", "10.0", "a", "3.0", "c"], store) == 0

      # "a" score should be updated
      assert SortedSet.handle("ZSCORE", [key, "a"], store) == "10.0"
      # "c" should NOT exist
      assert SortedSet.handle("ZSCORE", [key, "c"], store) == nil
      # Only 2 members total
      assert SortedSet.handle("ZCARD", [key], store) == 2
    end

    test "XX with all new members returns 0 and adds nothing" do
      key = ukey("zadd_xx_all_new")
      store = build_store(key)

      SortedSet.handle("ZADD", [key, "1.0", "existing"], store)

      assert SortedSet.handle("ZADD", [key, "XX", "5.0", "new1", "6.0", "new2"], store) == 0
      assert SortedSet.handle("ZCARD", [key], store) == 1
    end
  end

  describe "ZINCRBY through Raft -- atomic score increment" do
    test "ZINCRBY on non-existent member initializes score" do
      key = ukey("zincrby_new")
      store = build_store(key)

      result = SortedSet.handle("ZINCRBY", [key, "5", "player1"], store)
      assert result == "5.0"

      assert SortedSet.handle("ZSCORE", [key, "player1"], store) == "5.0"
    end

    test "ZINCRBY on existing member increments score" do
      key = ukey("zincrby_existing")
      store = build_store(key)

      SortedSet.handle("ZADD", [key, "10.0", "player1"], store)

      result = SortedSet.handle("ZINCRBY", [key, "3.5", "player1"], store)
      assert result == "13.5"
    end

    test "ZINCRBY with negative value decrements score" do
      key = ukey("zincrby_negative")
      store = build_store(key)

      SortedSet.handle("ZADD", [key, "100.0", "player1"], store)

      result = SortedSet.handle("ZINCRBY", [key, "-25", "player1"], store)
      assert result == "75.0"
    end

    test "sequential ZINCRBY calls produce correct total" do
      key = ukey("zincrby_seq")
      store = build_store(key)

      for _ <- 1..5 do
        SortedSet.handle("ZINCRBY", [key, "2", "player1"], store)
      end

      assert SortedSet.handle("ZSCORE", [key, "player1"], store) == "10.0"
    end
  end

  describe "ZRANGE after multiple ZADD -- correct sort order through Raft" do
    test "ZRANGE returns members sorted by score ascending" do
      key = ukey("zrange_order")
      store = build_store(key)

      SortedSet.handle("ZADD", [key, "3.0", "charlie"], store)
      SortedSet.handle("ZADD", [key, "1.0", "alice"], store)
      SortedSet.handle("ZADD", [key, "2.0", "bob"], store)

      result = SortedSet.handle("ZRANGE", [key, "0", "-1"], store)
      assert result == ["alice", "bob", "charlie"]
    end

    test "ZRANGE with WITHSCORES includes scores" do
      key = ukey("zrange_scores")
      store = build_store(key)

      SortedSet.handle("ZADD", [key, "1.5", "a", "2.5", "b", "3.5", "c"], store)

      result = SortedSet.handle("ZRANGE", [key, "0", "-1", "WITHSCORES"], store)
      assert result == ["a", "1.5", "b", "2.5", "c", "3.5"]
    end

    test "ZRANGE subrange returns correct slice" do
      key = ukey("zrange_sub")
      store = build_store(key)

      SortedSet.handle("ZADD", [key, "1.0", "a", "2.0", "b", "3.0", "c", "4.0", "d", "5.0", "e"], store)

      assert SortedSet.handle("ZRANGE", [key, "1", "3"], store) == ["b", "c", "d"]
    end

    test "members with equal scores are sorted lexicographically" do
      key = ukey("zrange_lex")
      store = build_store(key)

      SortedSet.handle("ZADD", [key, "1.0", "banana", "1.0", "apple", "1.0", "cherry"], store)

      result = SortedSet.handle("ZRANGE", [key, "0", "-1"], store)
      assert result == ["apple", "banana", "cherry"]
    end

    test "ZRANGE on empty/non-existent key returns empty list" do
      key = ukey("zrange_empty")
      store = build_store(key)

      assert SortedSet.handle("ZRANGE", [key, "0", "-1"], store) == []
    end
  end

  describe "ZREM then ZCARD -- count decremented through Raft" do
    test "ZREM decrements ZCARD by the number of removed members" do
      key = ukey("zrem_card")
      store = build_store(key)

      SortedSet.handle("ZADD", [key, "1.0", "a", "2.0", "b", "3.0", "c", "4.0", "d"], store)
      assert SortedSet.handle("ZCARD", [key], store) == 4

      # Remove two members
      assert SortedSet.handle("ZREM", [key, "b", "d"], store) == 2
      assert SortedSet.handle("ZCARD", [key], store) == 2

      # Remaining members should be a and c
      assert SortedSet.handle("ZRANGE", [key, "0", "-1"], store) == ["a", "c"]
    end

    test "ZREM of non-existent member returns 0 and does not change ZCARD" do
      key = ukey("zrem_missing")
      store = build_store(key)

      SortedSet.handle("ZADD", [key, "1.0", "a", "2.0", "b"], store)
      assert SortedSet.handle("ZCARD", [key], store) == 2

      assert SortedSet.handle("ZREM", [key, "nonexistent"], store) == 0
      assert SortedSet.handle("ZCARD", [key], store) == 2
    end

    test "ZREM mix of existing and non-existing returns correct count" do
      key = ukey("zrem_mix")
      store = build_store(key)

      SortedSet.handle("ZADD", [key, "1.0", "a", "2.0", "b", "3.0", "c"], store)

      # Remove a (exists), z (not), c (exists) -> 2
      assert SortedSet.handle("ZREM", [key, "a", "z", "c"], store) == 2
      assert SortedSet.handle("ZCARD", [key], store) == 1
    end

    test "ZREM all members cleans up the sorted set" do
      key = ukey("zrem_all")
      store = build_store(key)

      SortedSet.handle("ZADD", [key, "1.0", "a", "2.0", "b"], store)
      SortedSet.handle("ZREM", [key, "a", "b"], store)

      assert SortedSet.handle("ZCARD", [key], store) == 0
      assert SortedSet.handle("ZRANGE", [key, "0", "-1"], store) == []
    end
  end
end
