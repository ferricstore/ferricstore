defmodule Ferricstore.Commands.HashTest.Sections.WrongtypeEnforcementEdgeCases do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.Generic
      alias Ferricstore.Commands.Hash
      alias Ferricstore.Commands.List
      alias Ferricstore.Commands.Set
      alias Ferricstore.Commands.SortedSet
      alias Ferricstore.Commands.Strings
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Test.MockStore

  describe "WRONGTYPE enforcement edge cases" do
    test "HDEL on a key used as list returns WRONGTYPE" do
      store = MockStore.make()
      List.handle("LPUSH", ["mykey", "elem"], store)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HDEL", ["mykey", "f1"], store)
    end

    test "HMGET on a key used as set returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["mykey", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HMGET", ["mykey", "f1"], store)
    end

    test "HLEN on a key used as list returns WRONGTYPE" do
      store = MockStore.make()
      List.handle("LPUSH", ["mykey", "elem"], store)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HLEN", ["mykey"], store)
    end

    test "HEXISTS on a key used as set returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["mykey", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HEXISTS", ["mykey", "f"], store)
    end

    test "HKEYS on a key used as zset returns WRONGTYPE" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["mykey", "1.0", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HKEYS", ["mykey"], store)
    end

    test "HVALS on a key used as list returns WRONGTYPE" do
      store = MockStore.make()
      List.handle("LPUSH", ["mykey", "elem"], store)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HVALS", ["mykey"], store)
    end

    test "HSETNX on a key used as set returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["mykey", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HSETNX", ["mykey", "f", "v"], store)
    end

    test "HINCRBY on a key used as list returns WRONGTYPE" do
      store = MockStore.make()
      List.handle("LPUSH", ["mykey", "elem"], store)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HINCRBY", ["mykey", "f", "1"], store)
    end

    test "HINCRBYFLOAT on a key used as zset returns WRONGTYPE" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["mykey", "1.0", "member"], store)

      assert {:error, "WRONGTYPE" <> _} =
               Hash.handle("HINCRBYFLOAT", ["mykey", "f", "1.0"], store)
    end
  end
  describe "empty field handling" do
    test "HSET with empty string field name works" do
      store = MockStore.make()
      assert 1 == Hash.handle("HSET", ["hash", "", "value"], store)
      assert "value" == Hash.handle("HGET", ["hash", ""], store)
    end

    test "HGET with empty string field on existing hash returns nil" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert nil == Hash.handle("HGET", ["hash", ""], store)
    end
  end
  describe "HSCAN cursor edge cases" do
    test "HSCAN with negative cursor returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert {:error, "ERR invalid cursor"} = Hash.handle("HSCAN", ["hash", "-1"], store)
    end

    test "HSCAN with very large cursor beyond set size returns cursor 0 and empty list" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      [cursor, elements] = Hash.handle("HSCAN", ["hash", "999999"], store)
      assert cursor == "0"
      assert elements == []
    end

    test "HSCAN with odd trailing option returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert {:error, "ERR syntax error"} = Hash.handle("HSCAN", ["hash", "0", "MATCH"], store)
    end

    test "HSCAN with unknown option returns error" do
      store = MockStore.make()

      assert {:error, "ERR syntax error"} =
               Hash.handle("HSCAN", ["hash", "0", "BOGUS", "val"], store)
    end
  end
  describe "HRANDFIELD edge cases" do
    test "HRANDFIELD with invalid WITHVALUES string returns syntax error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "a", "1"], store)

      assert {:error, "ERR syntax error"} =
               Hash.handle("HRANDFIELD", ["hash", "1", "BOGUS"], store)
    end

    test "HRANDFIELD with negative count on empty hash returns empty list" do
      store = MockStore.make()
      result = Hash.handle("HRANDFIELD", ["hash", "-5"], store)
      assert result == []
    end
  end
    end
  end
end
