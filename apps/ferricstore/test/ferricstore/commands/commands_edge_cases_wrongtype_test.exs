defmodule Ferricstore.Commands.CommandsEdgeCasesWrongtypeTest do
  @moduledoc """
  Edge cases for WRONGTYPE enforcement across data structure boundaries.
  Split from CommandsEdgeCasesTest.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Hash, List, Set, SortedSet, Strings}
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Test.MockStore

  defp app_path(path), do: Path.expand("../../../#{path}", __DIR__)

  # ===========================================================================
  # WRONGTYPE enforcement — string commands on data structure keys
  # ===========================================================================

  describe "WRONGTYPE enforcement: string commands on hash keys" do
    setup do
      store = MockStore.make()
      Hash.handle("HSET", ["mykey", "field", "value"], store)
      {:ok, store: store}
    end

    test "GET treats ETF-looking hash payload as a string without type marker", %{store: _store} do
      hash_val = :erlang.term_to_binary({:hash, %{"f" => "v"}})
      store_with_hash = MockStore.make(%{"mykey" => {hash_val, 0}})
      result = Strings.handle("GET", ["mykey"], store_with_hash)
      assert result == hash_val
    end

    test "GET treats ETF-looking list payload as a string without type marker" do
      list_val = :erlang.term_to_binary({:list, ["a", "b"]})
      store = MockStore.make(%{"mylist" => {list_val, 0}})
      result = Strings.handle("GET", ["mylist"], store)
      assert result == list_val
    end

    test "MGET returns ETF-looking plain binaries as string values" do
      list_val = :erlang.term_to_binary({:list, ["a", "b"]})
      store = MockStore.make(%{"mylist" => {list_val, 0}, "string" => {"value", 0}})

      assert [list_val, "value", nil] ==
               Strings.handle("MGET", ["mylist", "string", "missing"], store)
    end

    test "GET treats ETF-looking set payload as a string without type marker" do
      set_val = :erlang.term_to_binary({:set, MapSet.new(["a"])})
      store = MockStore.make(%{"myset" => {set_val, 0}})
      result = Strings.handle("GET", ["myset"], store)
      assert result == set_val
    end

    test "GET treats ETF-looking zset payload as a string without type marker" do
      zset_val = :erlang.term_to_binary({:zset, %{"a" => 1.0}})
      store = MockStore.make(%{"myzset" => {zset_val, 0}})
      result = Strings.handle("GET", ["myzset"], store)
      assert result == zset_val
    end

    test "GET on key with Erlang-encoded but non-struct binary returns value as-is" do
      # A value that starts with <<131>> (ETF marker) but is not a known type
      # should be returned as the raw binary.
      other_val = :erlang.term_to_binary({:something_else, 42})
      store = MockStore.make(%{"other" => {other_val, 0}})
      result = Strings.handle("GET", ["other"], store)
      assert result == other_val
    end

    test "string read path does not infer type from ETF-looking payloads" do
      source = File.read!(app_path("lib/ferricstore/commands/strings.ex"))

      refute source =~ "maybe_check_type"
      refute source =~ "extract_etf_atom_name"
    end

    test "GETEX on hash key returns WRONGTYPE and does not create a string", %{store: store} do
      assert {:error, "WRONGTYPE" <> _} = Strings.handle("GETEX", ["mykey"], store)
      assert {:error, "WRONGTYPE" <> _} = Strings.handle("GETEX", ["mykey", "PX", "1000"], store)
      assert store.get.("mykey") == nil
    end

    test "SETNX on hash key treats compound type metadata as existing", %{store: store} do
      assert 0 = Strings.handle("SETNX", ["mykey", "overwrite"], store)
      assert store.get.("mykey") == nil
    end

    test "APPEND on hash key returns WRONGTYPE and does not create a string", %{store: store} do
      assert {:error, "WRONGTYPE" <> _} = Strings.handle("APPEND", ["mykey", "suffix"], store)
      assert store.get.("mykey") == nil
    end

    test "read-only string commands on hash key return WRONGTYPE", %{store: store} do
      assert {:error, "WRONGTYPE" <> _} = Strings.handle("STRLEN", ["mykey"], store)
      assert {:error, "WRONGTYPE" <> _} = Strings.handle("GETRANGE", ["mykey", "0", "-1"], store)
    end

    test "mutating string commands on hash key return WRONGTYPE and do not create a string", %{
      store: store
    } do
      assert {:error, "WRONGTYPE" <> _} = Strings.handle("GETSET", ["mykey", "overwrite"], store)
      assert {:error, "WRONGTYPE" <> _} = Strings.handle("GETDEL", ["mykey"], store)

      assert {:error, "WRONGTYPE" <> _} =
               Strings.handle("SETRANGE", ["mykey", "0", "overwrite"], store)

      assert store.get.("mykey") == nil
      assert store.compound_get.("mykey", "T:mykey") == "hash"
    end

    test "numeric string commands on hash key return WRONGTYPE and do not create a string", %{
      store: store
    } do
      assert {:error, "WRONGTYPE" <> _} = Strings.handle("INCR", ["mykey"], store)
      assert {:error, "WRONGTYPE" <> _} = Strings.handle("DECR", ["mykey"], store)
      assert {:error, "WRONGTYPE" <> _} = Strings.handle("INCRBY", ["mykey", "2"], store)
      assert {:error, "WRONGTYPE" <> _} = Strings.handle("DECRBY", ["mykey", "2"], store)
      assert {:error, "WRONGTYPE" <> _} = Strings.handle("INCRBYFLOAT", ["mykey", "1.5"], store)
      assert store.get.("mykey") == nil
    end

    test "MSETNX treats live compound data as existing", %{store: store} do
      assert 0 = Strings.handle("MSETNX", ["other", "value", "mykey", "overwrite"], store)
      assert store.get.("other") == nil
      assert store.get.("mykey") == nil
    end
  end

  describe "WRONGTYPE enforcement: HSET on string key" do
    test "HSET on a key already holding a set returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["mykey", "member"], store)

      result = Hash.handle("HSET", ["mykey", "field", "value"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end

    test "HSET on a key already holding a list returns WRONGTYPE" do
      store = MockStore.make()
      List.handle("LPUSH", ["mykey", "elem"], store)

      result = Hash.handle("HSET", ["mykey", "field", "value"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end

    test "HSET on a key already holding a zset returns WRONGTYPE" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["mykey", "1.0", "member"], store)

      result = Hash.handle("HSET", ["mykey", "field", "value"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end
  end

  describe "string SET replacement clears old compound data" do
    setup do
      store = MockStore.make()
      store.compound_put.("mykey", CompoundKey.type_key("mykey"), "hash", 0)
      store.compound_put.("mykey", CompoundKey.hash_field("mykey", "field"), "old", 0)
      {:ok, store: store}
    end

    test "SET replaces hash key with string and clears stale hash metadata", %{store: store} do
      assert :ok = Strings.handle("SET", ["mykey", "string"], store)
      assert store.get.("mykey") == "string"
      assert store.compound_get.("mykey", CompoundKey.type_key("mykey")) == nil
      assert store.compound_get.("mykey", CompoundKey.hash_field("mykey", "field")) == nil
      assert {:simple, "string"} = Strings.handle("TYPE", ["mykey"], store)
    end

    test "MSET replaces hash key with string and clears stale hash metadata", %{store: store} do
      assert :ok = Strings.handle("MSET", ["mykey", "string", "other", "value"], store)
      assert store.get.("mykey") == "string"
      assert store.get.("other") == "value"
      assert store.compound_get.("mykey", CompoundKey.type_key("mykey")) == nil
      assert store.compound_get.("mykey", CompoundKey.hash_field("mykey", "field")) == nil
    end

    test "SETEX and PSETEX replace hash key with string and clear stale hash metadata", %{
      store: store
    } do
      assert :ok = Strings.handle("SETEX", ["mykey", "10", "string"], store)
      assert store.get.("mykey") == "string"
      assert store.compound_get.("mykey", CompoundKey.type_key("mykey")) == nil
      assert store.compound_get.("mykey", CompoundKey.hash_field("mykey", "field")) == nil

      store.compound_put.("mykey", CompoundKey.type_key("mykey"), "hash", 0)
      store.compound_put.("mykey", CompoundKey.hash_field("mykey", "field"), "old", 0)
      assert :ok = Strings.handle("PSETEX", ["mykey", "10", "string2"], store)
      assert store.get.("mykey") == "string2"
      assert store.compound_get.("mykey", CompoundKey.type_key("mykey")) == nil
      assert store.compound_get.("mykey", CompoundKey.hash_field("mykey", "field")) == nil
    end
  end

  describe "WRONGTYPE: after DEL, key can be reused as any type" do
    test "DEL hash key then SET as string succeeds" do
      alias Ferricstore.Commands.Hash

      store = MockStore.make()
      # Create as hash
      Hash.handle("HSET", ["reuse", "f1", "v1"], store)
      # Delete via Strings DEL
      Strings.handle("DEL", ["reuse"], store)
      # Reuse as string
      assert :ok = Strings.handle("SET", ["reuse", "now_a_string"], store)
      assert "now_a_string" == store.get.("reuse")
    end

    test "DEL set key then HSET as hash succeeds" do
      alias Ferricstore.Commands.{Hash, Set}

      store = MockStore.make()
      # Create as set
      Set.handle("SADD", ["reuse", "member1"], store)
      # Delete via Strings DEL
      Strings.handle("DEL", ["reuse"], store)
      # Reuse as hash
      result = Hash.handle("HSET", ["reuse", "field1", "val1"], store)
      assert result == 1
    end
  end

  # ===========================================================================
  # WRONGTYPE across all data structure boundaries
  # ===========================================================================

  describe "WRONGTYPE across data structure boundaries" do
    test "LPUSH on hash key returns WRONGTYPE" do
      store = MockStore.make()
      Hash.handle("HSET", ["mykey", "field", "value"], store)

      result = List.handle("LPUSH", ["mykey", "elem"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end

    test "SADD on hash key returns WRONGTYPE" do
      store = MockStore.make()
      Hash.handle("HSET", ["mykey", "field", "value"], store)

      result = Set.handle("SADD", ["mykey", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end

    test "ZADD on hash key returns WRONGTYPE" do
      store = MockStore.make()
      Hash.handle("HSET", ["mykey", "field", "value"], store)

      result = SortedSet.handle("ZADD", ["mykey", "1.0", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end

    test "HSET on list key returns WRONGTYPE" do
      store = MockStore.make()
      List.handle("LPUSH", ["mykey", "elem"], store)

      result = Hash.handle("HSET", ["mykey", "f", "v"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end

    test "SADD on zset key returns WRONGTYPE" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["mykey", "1.0", "member"], store)

      result = Set.handle("SADD", ["mykey", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end

    test "ZADD on set key returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["mykey", "member"], store)

      result = SortedSet.handle("ZADD", ["mykey", "1.0", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end

    test "LPUSH on set key returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["mykey", "member"], store)

      result = List.handle("LPUSH", ["mykey", "elem"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end
  end
end
