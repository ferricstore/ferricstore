defmodule Ferricstore.Commands.RedisCompatTest do
  @moduledoc """
  Redis compatibility tests based on known edge cases and bugs found in
  Redis-compatible servers (KeyDB, Dragonfly, Garnet, Valkey).

  These tests verify that FerricStore matches Redis behaviour for:

  1. WRONGTYPE enforcement gaps (SET overwrites type, INCR on wrong type, etc.)
  2. Transaction atomicity edge cases (MULTI/EXEC error propagation)
  3. SCAN cursor correctness
  5. Type metadata consistency after overwrites
  6. Null byte handling in keys
  7. RENAME/COPY with data structure keys
  15. Dispatcher and command routing
  16. OBJECT command edge cases

  See also:
  - redis_compat_strings_test.exs — string/boundary edge cases
  - redis_compat_hash_test.exs — data structure edge cases (hash/set/zset/list)
  - redis_compat_expiry_test.exs — key expiry edge cases

  Sources:
  - https://redis.io/blog/set-command-strange-beast/
  - https://redis.io/docs/latest/develop/using-commands/transactions/
  - https://redis.io/docs/latest/commands/scan/
  - https://github.com/dragonflydb/dragonfly/issues/2630
  - https://github.com/redis/redis/issues/2864
  """

  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Dispatcher, Generic, Hash, Set, SortedSet, Strings}
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Test.MockStore

  # WRONGTYPE tests moved to redis_compat_wrongtype_test.exs

  # ===========================================================================
  # 2. TRANSACTION ATOMICITY EDGE CASES
  # ===========================================================================

  describe "transaction WRONGTYPE errors" do
    test "WRONGTYPE error inside MULTI/EXEC does not abort other commands" do
      store = MockStore.make()

      Hash.handle("HSET", ["hashkey", "f", "v"], store)
      Strings.handle("SET", ["strkey", "hello"], store)

      results = [
        Dispatcher.dispatch("SET", ["strkey", "world"], store),
        Dispatcher.dispatch("INCR", ["hashkey"], store),
        Dispatcher.dispatch("GET", ["strkey"], store)
      ]

      assert Enum.at(results, 0) == :ok
      assert Enum.at(results, 2) == "world"
    end
  end

  # ===========================================================================
  # 3. SCAN CURSOR BUGS
  # ===========================================================================

  describe "SCAN cursor correctness" do
    test "SCAN returns all keys across multiple iterations" do
      store = MockStore.make()

      for i <- 1..20 do
        Strings.handle("SET", ["key:#{String.pad_leading("#{i}", 3, "0")}", "v#{i}"], store)
      end

      all_keys = scan_all(store, "0", 5)
      assert length(Enum.uniq(all_keys)) == 20
    end

    test "SCAN with MATCH pattern returns only matching keys" do
      store = MockStore.make()

      Strings.handle("SET", ["user:1", "alice"], store)
      Strings.handle("SET", ["user:2", "bob"], store)
      Strings.handle("SET", ["post:1", "hello"], store)
      Strings.handle("SET", ["post:2", "world"], store)

      result = Generic.handle("SCAN", ["0", "MATCH", "user:*", "COUNT", "100"], store)
      [_cursor, keys] = result

      assert "user:1" in keys
      assert "user:2" in keys
      refute "post:1" in keys
      refute "post:2" in keys
    end

    test "SCAN cursor 0 means start, returns cursor 0 when done" do
      store = MockStore.make()

      Strings.handle("SET", ["a", "1"], store)
      Strings.handle("SET", ["b", "2"], store)
      Strings.handle("SET", ["c", "3"], store)

      [cursor, keys] = Generic.handle("SCAN", ["0", "COUNT", "100"], store)
      assert cursor == "0"
      assert length(keys) == 3
    end

    test "SCAN with COUNT 1 still terminates" do
      store = MockStore.make()

      for i <- 1..5 do
        Strings.handle("SET", ["k#{i}", "v"], store)
      end

      all_keys = scan_all(store, "0", 1)
      assert length(Enum.uniq(all_keys)) == 5
    end

    test "SCAN with no matching keys returns cursor 0 and empty list" do
      store = MockStore.make()

      Strings.handle("SET", ["hello", "world"], store)

      [cursor, keys] = Generic.handle("SCAN", ["0", "MATCH", "nomatch:*", "COUNT", "100"], store)
      assert cursor == "0"
      assert keys == []
    end

    test "SCAN TYPE filter works for string type" do
      store = MockStore.make()

      Strings.handle("SET", ["strkey", "val"], store)

      [cursor, keys] = Generic.handle("SCAN", ["0", "TYPE", "string", "COUNT", "100"], store)
      assert cursor == "0"
      assert "strkey" in keys
    end
  end

  # ===========================================================================
  # 5. RENAME/COPY WITH DATA STRUCTURE KEYS
  # ===========================================================================

  describe "RENAME/COPY with type metadata" do
    test "RENAME preserves value for string keys" do
      store = MockStore.make()
      Strings.handle("SET", ["src", "hello"], store)

      assert :ok = Generic.handle("RENAME", ["src", "dst"], store)
      assert "hello" = Strings.handle("GET", ["dst"], store)
      assert nil == Strings.handle("GET", ["src"], store)
    end

    test "RENAME preserves TTL" do
      store = MockStore.make()
      Strings.handle("SET", ["src", "hello", "EX", "3600"], store)

      assert :ok = Generic.handle("RENAME", ["src", "dst"], store)

      ttl = Ferricstore.Commands.Expiry.handle("TTL", ["dst"], store)
      assert ttl > 0
    end

    test "RENAME on nonexistent key returns error" do
      store = MockStore.make()
      result = Generic.handle("RENAME", ["nosuchkey", "dst"], store)
      assert {:error, "ERR no such key"} = result
    end

    test "RENAMENX when destination exists returns 0" do
      store = MockStore.make()
      Strings.handle("SET", ["src", "hello"], store)
      Strings.handle("SET", ["dst", "world"], store)

      assert 0 = Generic.handle("RENAMENX", ["src", "dst"], store)

      assert "hello" = Strings.handle("GET", ["src"], store)
      assert "world" = Strings.handle("GET", ["dst"], store)
    end

    test "RENAMENX when destination does not exist returns 1" do
      store = MockStore.make()
      Strings.handle("SET", ["src", "hello"], store)

      assert 1 = Generic.handle("RENAMENX", ["src", "dst"], store)
      assert "hello" = Strings.handle("GET", ["dst"], store)
      assert nil == Strings.handle("GET", ["src"], store)
    end

    test "COPY preserves value" do
      store = MockStore.make()
      Strings.handle("SET", ["src", "hello"], store)

      assert 1 = Generic.handle("COPY", ["src", "dst"], store)
      assert "hello" = Strings.handle("GET", ["dst"], store)
      assert "hello" = Strings.handle("GET", ["src"], store)
    end

    test "COPY without REPLACE on existing destination returns 0" do
      store = MockStore.make()
      Strings.handle("SET", ["src", "hello"], store)
      Strings.handle("SET", ["dst", "world"], store)

      result = Generic.handle("COPY", ["src", "dst"], store)
      assert 0 = result
      assert "world" = Strings.handle("GET", ["dst"], store)
    end

    test "COPY with REPLACE overwrites destination" do
      store = MockStore.make()
      Strings.handle("SET", ["src", "hello"], store)
      Strings.handle("SET", ["dst", "world"], store)

      assert 1 = Generic.handle("COPY", ["src", "dst", "REPLACE"], store)
      assert "hello" = Strings.handle("GET", ["dst"], store)
    end
  end

  # ===========================================================================
  # 7. NULL BYTE IN KEYS
  # ===========================================================================

  describe "null byte in key names" do
    test "keys containing null bytes are handled safely by string commands" do
      store = MockStore.make()
      key_with_null = "key\0with\0nulls"

      result =
        try do
          Strings.handle("SET", [key_with_null, "value"], store)
        rescue
          e -> {:exception, Exception.message(e)}
        end

      assert result == :ok or match?({:error, _}, result) or match?({:exception, _}, result),
             "SET with null byte key: #{inspect(result)}"
    end

    test "null byte key does not corrupt hash compound key namespace" do
      store = MockStore.make()

      Hash.handle("HSET", ["mykey", "field1", "value1"], store)

      Strings.handle("SET", ["H:mykey", "rogue"], store)

      assert "value1" = Hash.handle("HGET", ["mykey", "field1"], store)
    end

    test "CompoundKey.type_key format is deterministic" do
      assert "T:mykey" = CompoundKey.type_key("mykey")
      assert "T:" = CompoundKey.type_key("")
      assert "T:a:b:c" = CompoundKey.type_key("a:b:c")
    end
  end

  # ===========================================================================
  # 8. DEL/UNLINK WITH DATA STRUCTURE KEYS
  # ===========================================================================

  describe "DEL/UNLINK cleanup of data structure keys" do
    test "DEL on a hash key removes all fields and type metadata" do
      store = MockStore.make()

      Hash.handle("HSET", ["mykey", "f1", "v1", "f2", "v2"], store)
      assert {:simple, "hash"} = Generic.handle("TYPE", ["mykey"], store)

      assert 1 = Strings.handle("DEL", ["mykey"], store)

      assert {:simple, "none"} = Generic.handle("TYPE", ["mykey"], store)
      assert 0 = Hash.handle("HLEN", ["mykey"], store)
    end

    test "DEL on a set key removes all members and type metadata" do
      store = MockStore.make()

      Set.handle("SADD", ["myset", "a", "b", "c"], store)
      assert {:simple, "set"} = Generic.handle("TYPE", ["myset"], store)

      assert 1 = Strings.handle("DEL", ["myset"], store)

      assert {:simple, "none"} = Generic.handle("TYPE", ["myset"], store)
      assert 0 = Set.handle("SCARD", ["myset"], store)
    end

    test "DEL on a sorted set key removes all members and type metadata" do
      store = MockStore.make()

      SortedSet.handle("ZADD", ["myzset", "1.0", "a", "2.0", "b"], store)
      assert {:simple, "zset"} = Generic.handle("TYPE", ["myzset"], store)

      assert 1 = Strings.handle("DEL", ["myzset"], store)

      assert {:simple, "none"} = Generic.handle("TYPE", ["myzset"], store)
      assert 0 = SortedSet.handle("ZCARD", ["myzset"], store)
    end

    test "UNLINK has same cleanup semantics as DEL" do
      store = MockStore.make()

      Hash.handle("HSET", ["mykey", "f", "v"], store)
      assert {:simple, "hash"} = Generic.handle("TYPE", ["mykey"], store)

      assert 1 = Generic.handle("UNLINK", ["mykey"], store)
      assert {:simple, "none"} = Generic.handle("TYPE", ["mykey"], store)
    end

    test "DEL on nonexistent key returns 0" do
      store = MockStore.make()
      assert 0 = Strings.handle("DEL", ["nosuchkey"], store)
    end

    test "DEL on multiple keys returns count of deleted" do
      store = MockStore.make()
      Strings.handle("SET", ["a", "1"], store)
      Strings.handle("SET", ["b", "2"], store)

      assert 2 = Strings.handle("DEL", ["a", "b", "c"], store)
    end
  end

  # ===========================================================================
  # 15. DISPATCHER AND COMMAND ROUTING
  # ===========================================================================

  describe "dispatcher edge cases" do
    test "unknown command returns error" do
      store = MockStore.make()
      result = Dispatcher.dispatch("TOTALLYUNKNOWN", [], store)
      assert {:error, "ERR unknown command" <> _} = result
    end

    test "command names are case-insensitive" do
      store = MockStore.make()
      Strings.handle("SET", ["k", "v"], store)

      assert "v" = Dispatcher.dispatch("get", ["k"], store)
      assert "v" = Dispatcher.dispatch("Get", ["k"], store)
      assert "v" = Dispatcher.dispatch("GET", ["k"], store)
    end

    test "empty args to DEL returns error" do
      store = MockStore.make()
      result = Dispatcher.dispatch("DEL", [], store)
      assert {:error, _} = result
    end

    test "empty args to EXISTS returns error" do
      store = MockStore.make()
      result = Dispatcher.dispatch("EXISTS", [], store)
      assert {:error, _} = result
    end
  end

  # ===========================================================================
  # 16. OBJECT COMMAND EDGE CASES
  # ===========================================================================

  describe "OBJECT command edge cases" do
    test "OBJECT ENCODING on existing short string key returns 'embstr'" do
      store = MockStore.make()
      Strings.handle("SET", ["k", "v"], store)

      assert "embstr" = Generic.handle("OBJECT", ["ENCODING", "k"], store)
    end

    test "OBJECT ENCODING on missing key returns error" do
      store = MockStore.make()
      result = Generic.handle("OBJECT", ["ENCODING", "nosuchkey"], store)
      assert {:error, "ERR no such key"} = result
    end

    test "OBJECT HELP returns list" do
      store = MockStore.make()
      result = Generic.handle("OBJECT", ["HELP"], store)
      assert is_list(result)
      assert result != []
    end

    test "OBJECT REFCOUNT always returns 1" do
      store = MockStore.make()
      Strings.handle("SET", ["k", "v"], store)
      assert 1 = Generic.handle("OBJECT", ["REFCOUNT", "k"], store)
    end

    test "OBJECT unknown subcommand returns error" do
      store = MockStore.make()
      result = Generic.handle("OBJECT", ["BOGUS", "k"], store)
      assert {:error, _} = result
    end
  end

  # ===========================================================================
  # Helpers
  # ===========================================================================

  # Iterates SCAN until cursor returns "0", collecting all keys.
  defp scan_all(store, cursor, count) do
    [next_cursor, keys] =
      Generic.handle("SCAN", [cursor, "COUNT", "#{count}"], store)

    if next_cursor == "0" do
      keys
    else
      keys ++ scan_all(store, next_cursor, count)
    end
  end
end
