defmodule Ferricstore.Commands.JsonTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Json
  alias Ferricstore.Commands.Hash
  alias Ferricstore.Test.MockStore

  # ===========================================================================
  # Helpers
  # ===========================================================================

  # Stores a JSON document and returns the store.
  defp store_with_json(key \\ "doc", json_value) do
    store = MockStore.make()
    json_str = Jason.encode!(json_value)
    raw = :erlang.term_to_binary({:json, json_str})
    store.put.(key, raw, 0)
    store
  end

  # ===========================================================================
  # JSON.SET
  # ===========================================================================

  describe "JSON.SET" do
    test "sets a new key at root path $" do
      store = MockStore.make()
      assert :ok = Json.handle("JSON.SET", ["doc", "$", ~s({"name":"Alice"})], store)
      # Verify the stored value is readable
      assert ~s({"name":"Alice"}) == Json.handle("JSON.GET", ["doc"], store)
    end

    test "sets a scalar value at root" do
      store = MockStore.make()
      assert :ok = Json.handle("JSON.SET", ["doc", "$", "42"], store)
      assert "42" == Json.handle("JSON.GET", ["doc"], store)
    end

    test "sets a string value at root" do
      store = MockStore.make()
      assert :ok = Json.handle("JSON.SET", ["doc", "$", ~s("hello")], store)
      assert ~s("hello") == Json.handle("JSON.GET", ["doc"], store)
    end

    test "sets a null value at root" do
      store = MockStore.make()
      assert :ok = Json.handle("JSON.SET", ["doc", "$", "null"], store)
      assert "null" == Json.handle("JSON.GET", ["doc"], store)
    end

    test "sets a boolean value at root" do
      store = MockStore.make()
      assert :ok = Json.handle("JSON.SET", ["doc", "$", "true"], store)
      assert "true" == Json.handle("JSON.GET", ["doc"], store)
    end

    test "sets an array value at root" do
      store = MockStore.make()
      assert :ok = Json.handle("JSON.SET", ["doc", "$", "[1,2,3]"], store)
      assert "[1,2,3]" == Json.handle("JSON.GET", ["doc"], store)
    end

    test "overwrites existing key at root" do
      store = store_with_json(%{"old" => true})
      assert :ok = Json.handle("JSON.SET", ["doc", "$", ~s({"new":true})], store)
      assert ~s({"new":true}) == Json.handle("JSON.GET", ["doc"], store)
    end

    test "sets nested field on existing object" do
      store = store_with_json(%{"name" => "Alice", "age" => 30})
      assert :ok = Json.handle("JSON.SET", ["doc", "$.name", ~s("Bob")], store)
      result = Jason.decode!(Json.handle("JSON.GET", ["doc"], store))
      assert result["name"] == "Bob"
      assert result["age"] == 30
    end

    test "sets deeply nested field" do
      store = store_with_json(%{"a" => %{"b" => %{"c" => 1}}})
      assert :ok = Json.handle("JSON.SET", ["doc", "$.a.b.c", "99"], store)
      result = Jason.decode!(Json.handle("JSON.GET", ["doc"], store))
      assert result["a"]["b"]["c"] == 99
    end

    test "sets array element by index" do
      store = store_with_json(%{"arr" => [1, 2, 3]})
      assert :ok = Json.handle("JSON.SET", ["doc", "$.arr[1]", "42"], store)
      result = Jason.decode!(Json.handle("JSON.GET", ["doc"], store))
      assert result["arr"] == [1, 42, 3]
    end

    test "NX flag: sets when key does not exist" do
      store = MockStore.make()
      assert :ok = Json.handle("JSON.SET", ["doc", "$", ~s({"x":1}), "NX"], store)
      assert ~s({"x":1}) == Json.handle("JSON.GET", ["doc"], store)
    end

    test "NX flag: does not overwrite when key exists" do
      store = store_with_json(%{"x" => 1})
      assert nil == Json.handle("JSON.SET", ["doc", "$", ~s({"x":2}), "NX"], store)
      result = Jason.decode!(Json.handle("JSON.GET", ["doc"], store))
      assert result["x"] == 1
    end

    test "NX flag on nested path: sets when path does not exist" do
      store = store_with_json(%{"a" => 1})
      assert :ok = Json.handle("JSON.SET", ["doc", "$.b", "2", "NX"], store)
      result = Jason.decode!(Json.handle("JSON.GET", ["doc"], store))
      assert result["b"] == 2
    end

    test "NX flag on nested path: does not overwrite when path exists" do
      store = store_with_json(%{"a" => 1})
      assert nil == Json.handle("JSON.SET", ["doc", "$.a", "2", "NX"], store)
      result = Jason.decode!(Json.handle("JSON.GET", ["doc"], store))
      assert result["a"] == 1
    end

    test "XX flag: sets when key exists" do
      store = store_with_json(%{"x" => 1})
      assert :ok = Json.handle("JSON.SET", ["doc", "$", ~s({"x":2}), "XX"], store)
      result = Jason.decode!(Json.handle("JSON.GET", ["doc"], store))
      assert result["x"] == 2
    end

    test "XX flag: returns nil when key does not exist" do
      store = MockStore.make()
      assert nil == Json.handle("JSON.SET", ["doc", "$", ~s({"x":1}), "XX"], store)
      assert nil == Json.handle("JSON.GET", ["doc"], store)
    end

    test "XX flag on nested path: sets when path exists" do
      store = store_with_json(%{"a" => 1})
      assert :ok = Json.handle("JSON.SET", ["doc", "$.a", "2", "XX"], store)
      result = Jason.decode!(Json.handle("JSON.GET", ["doc"], store))
      assert result["a"] == 2
    end

    test "XX flag on nested path: returns nil when path does not exist" do
      store = store_with_json(%{"a" => 1})
      assert nil == Json.handle("JSON.SET", ["doc", "$.b", "2", "XX"], store)
      result = Jason.decode!(Json.handle("JSON.GET", ["doc"], store))
      refute Map.has_key?(result, "b")
    end

    test "invalid JSON value returns error" do
      store = MockStore.make()
      assert {:error, msg} = Json.handle("JSON.SET", ["doc", "$", "not json{"], store)
      assert msg =~ "invalid JSON"
    end

    test "wrong number of arguments returns error" do
      assert {:error, _} = Json.handle("JSON.SET", [], MockStore.make())
      assert {:error, _} = Json.handle("JSON.SET", ["key"], MockStore.make())
      assert {:error, _} = Json.handle("JSON.SET", ["key", "$"], MockStore.make())
    end

    test "unrecognized flag returns error" do
      assert {:error, msg} = Json.handle("JSON.SET", ["k", "$", "1", "BOGUS"], MockStore.make())
      assert msg =~ "syntax error"
    end

    test "root set rejects an existing plain string key" do
      store = MockStore.make()
      store.put.("doc", "plain", 0)

      assert {:error, msg} = Json.handle("JSON.SET", ["doc", "$", ~s({"x":1})], store)
      assert msg =~ "not a JSON value"
      assert store.get.("doc") == "plain"
    end

    test "root set rejects an existing compound key" do
      store = MockStore.make()
      assert 1 == Hash.handle("HSET", ["doc", "field", "value"], store)

      assert {:error, msg} = Json.handle("JSON.SET", ["doc", "$", ~s({"x":1})], store)
      assert msg =~ "WRONGTYPE"
      assert {:error, "WRONGTYPE" <> _} = Json.handle("JSON.GET", ["doc"], store)
      assert "value" == Hash.handle("HGET", ["doc", "field"], store)
    end

    test "creates nested path on non-existing key" do
      store = MockStore.make()
      assert :ok = Json.handle("JSON.SET", ["doc", "$.a.b", ~s("deep")], store)
      result = Jason.decode!(Json.handle("JSON.GET", ["doc"], store))
      assert result["a"]["b"] == "deep"
    end

    test "returns write error when root write fails" do
      store =
        MockStore.make()
        |> Map.put(:put, fn "doc", _raw, 0 -> {:error, :disk_full} end)

      assert {:error, :disk_full} =
               Json.handle("JSON.SET", ["doc", "$", ~s({"name":"Alice"})], store)
    end
  end

  # ===========================================================================
  # JSON.GET
  # ===========================================================================

  describe "JSON.GET" do
    test "gets entire document with no path" do
      store = store_with_json(%{"name" => "Alice"})
      assert ~s({"name":"Alice"}) == Json.handle("JSON.GET", ["doc"], store)
    end

    test "gets value at root path $" do
      store = store_with_json(%{"name" => "Alice"})
      assert ~s({"name":"Alice"}) == Json.handle("JSON.GET", ["doc", "$"], store)
    end

    test "gets nested field" do
      store = store_with_json(%{"name" => "Alice", "age" => 30})
      assert ~s("Alice") == Json.handle("JSON.GET", ["doc", "$.name"], store)
    end

    test "gets deeply nested field" do
      store = store_with_json(%{"a" => %{"b" => %{"c" => 42}}})
      assert "42" == Json.handle("JSON.GET", ["doc", "$.a.b.c"], store)
    end

    test "gets array element by index" do
      store = store_with_json(%{"arr" => [10, 20, 30]})
      assert "20" == Json.handle("JSON.GET", ["doc", "$.arr[1]"], store)
    end

    test "gets nested field within array element" do
      store = store_with_json(%{"users" => [%{"name" => "Alice"}, %{"name" => "Bob"}]})
      assert ~s("Bob") == Json.handle("JSON.GET", ["doc", "$.users[1].name"], store)
    end

    test "returns nil for missing key" do
      store = MockStore.make()
      assert nil == Json.handle("JSON.GET", ["missing"], store)
    end

    test "returns nil for missing path" do
      store = store_with_json(%{"a" => 1})
      assert nil == Json.handle("JSON.GET", ["doc", "$.b"], store)
    end

    test "returns nil for out-of-bounds array index" do
      store = store_with_json(%{"arr" => [1, 2, 3]})
      assert nil == Json.handle("JSON.GET", ["doc", "$.arr[10]"], store)
    end

    test "multiple paths return JSON object" do
      store = store_with_json(%{"name" => "Alice", "age" => 30, "city" => "NY"})
      result = Json.handle("JSON.GET", ["doc", "$.name", "$.age"], store)
      decoded = Jason.decode!(result)
      assert decoded["$.name"] == "Alice"
      assert decoded["$.age"] == 30
    end

    test "multiple paths with missing path include null" do
      store = store_with_json(%{"a" => 1})
      result = Json.handle("JSON.GET", ["doc", "$.a", "$.b"], store)
      decoded = Jason.decode!(result)
      assert decoded["$.a"] == 1
      assert decoded["$.b"] == nil
    end

    test "wrong number of arguments returns error" do
      assert {:error, _} = Json.handle("JSON.GET", [], MockStore.make())
    end

    test "error for non-JSON value stored at key" do
      store = MockStore.make()
      store.put.("doc", "not a json tagged value", 0)
      assert {:error, msg} = Json.handle("JSON.GET", ["doc"], store)
      assert msg =~ "not a JSON value"
    end

    test "returns WRONGTYPE for compound keys" do
      store = MockStore.make()
      assert 1 == Hash.handle("HSET", ["doc", "field", "value"], store)

      assert {:error, msg} = Json.handle("JSON.GET", ["doc"], store)
      assert msg =~ "WRONGTYPE"
      assert "value" == Hash.handle("HGET", ["doc", "field"], store)
    end
  end

  # ===========================================================================
  # JSON.DEL
  # ===========================================================================

  describe "JSON.DEL" do
    test "deletes entire key at root $" do
      store = store_with_json(%{"a" => 1})
      assert 1 == Json.handle("JSON.DEL", ["doc", "$"], store)
      assert nil == Json.handle("JSON.GET", ["doc"], store)
    end

    test "deletes entire key with no path (default)" do
      store = store_with_json(%{"a" => 1})
      assert 1 == Json.handle("JSON.DEL", ["doc"], store)
      assert nil == Json.handle("JSON.GET", ["doc"], store)
    end

    test "deletes nested field" do
      store = store_with_json(%{"name" => "Alice", "age" => 30})
      assert 1 == Json.handle("JSON.DEL", ["doc", "$.age"], store)
      result = Jason.decode!(Json.handle("JSON.GET", ["doc"], store))
      assert result == %{"name" => "Alice"}
    end

    test "deletes deeply nested field" do
      store = store_with_json(%{"a" => %{"b" => %{"c" => 1, "d" => 2}}})
      assert 1 == Json.handle("JSON.DEL", ["doc", "$.a.b.c"], store)
      result = Jason.decode!(Json.handle("JSON.GET", ["doc"], store))
      assert result == %{"a" => %{"b" => %{"d" => 2}}}
    end

    test "deletes array element" do
      store = store_with_json(%{"arr" => [1, 2, 3]})
      assert 1 == Json.handle("JSON.DEL", ["doc", "$.arr[1]"], store)
      result = Jason.decode!(Json.handle("JSON.GET", ["doc"], store))
      assert result["arr"] == [1, 3]
    end

    test "returns 0 for missing key" do
      store = MockStore.make()
      assert 0 == Json.handle("JSON.DEL", ["missing"], store)
    end

    test "returns 0 for missing path" do
      store = store_with_json(%{"a" => 1})
      assert 0 == Json.handle("JSON.DEL", ["doc", "$.b"], store)
    end

    test "wrong number of arguments returns error" do
      assert {:error, _} = Json.handle("JSON.DEL", [], MockStore.make())
    end

    test "returns error for non-JSON value stored at key" do
      store = MockStore.make()
      store.put.("doc", "not a json tagged value", 0)

      assert {:error, msg} = Json.handle("JSON.DEL", ["doc"], store)
      assert msg =~ "not a JSON value"
      assert store.get.("doc") == "not a json tagged value"
    end

    test "returns WRONGTYPE for compound keys" do
      store = MockStore.make()
      assert 1 == Hash.handle("HSET", ["doc", "field", "value"], store)

      assert {:error, msg} = Json.handle("JSON.DEL", ["doc"], store)
      assert msg =~ "WRONGTYPE"
      assert "value" == Hash.handle("HGET", ["doc", "field"], store)
    end

    test "returns delete error when root delete fails" do
      store =
        store_with_json(%{"a" => 1})
        |> Map.put(:delete, fn "doc" -> {:error, :disk_full} end)

      assert {:error, :disk_full} = Json.handle("JSON.DEL", ["doc"], store)
      assert ~s({"a":1}) == Json.handle("JSON.GET", ["doc"], store)
    end

    test "returns write error when nested delete rewrite fails" do
      store =
        store_with_json(%{"a" => 1, "b" => 2})
        |> Map.put(:put, fn "doc", _raw, 0 -> {:error, :disk_full} end)

      assert {:error, :disk_full} = Json.handle("JSON.DEL", ["doc", "$.b"], store)
      assert ~s({"a":1,"b":2}) == Json.handle("JSON.GET", ["doc"], store)
    end
  end

  # ===========================================================================
  # JSON.NUMINCRBY
  # ===========================================================================

  describe "JSON.NUMINCRBY" do
    test "increments integer at path" do
      store = store_with_json(%{"count" => 10})
      result = Json.handle("JSON.NUMINCRBY", ["doc", "$.count", "5"], store)
      assert result == "15"
      assert "15" == Json.handle("JSON.GET", ["doc", "$.count"], store)
    end

    test "decrements with negative value" do
      store = store_with_json(%{"count" => 10})
      result = Json.handle("JSON.NUMINCRBY", ["doc", "$.count", "-3"], store)
      assert result == "7"
    end

    test "increments float at path" do
      store = store_with_json(%{"val" => 1.5})
      result = Json.handle("JSON.NUMINCRBY", ["doc", "$.val", "0.5"], store)
      assert result == "2.0"
    end

    test "increments integer with float produces float" do
      store = store_with_json(%{"val" => 10})
      result = Json.handle("JSON.NUMINCRBY", ["doc", "$.val", "0.5"], store)
      assert result == "10.5"
    end

    test "increments nested number" do
      store = store_with_json(%{"a" => %{"b" => 100}})
      result = Json.handle("JSON.NUMINCRBY", ["doc", "$.a.b", "1"], store)
      assert result == "101"
    end

    test "returns error for non-number value" do
      store = store_with_json(%{"name" => "Alice"})
      assert {:error, msg} = Json.handle("JSON.NUMINCRBY", ["doc", "$.name", "1"], store)
      assert msg =~ "not a number"
    end

    test "returns error for missing key" do
      store = MockStore.make()
      assert {:error, _} = Json.handle("JSON.NUMINCRBY", ["missing", "$.x", "1"], store)
    end

    test "returns error for missing path" do
      store = store_with_json(%{"a" => 1})
      assert {:error, msg} = Json.handle("JSON.NUMINCRBY", ["doc", "$.b", "1"], store)
      assert msg =~ "path does not exist"
    end

    test "returns error for invalid increment" do
      store = store_with_json(%{"a" => 1})
      assert {:error, msg} = Json.handle("JSON.NUMINCRBY", ["doc", "$.a", "abc"], store)
      assert msg =~ "not a number"
    end

    test "wrong number of arguments returns error" do
      assert {:error, _} = Json.handle("JSON.NUMINCRBY", [], MockStore.make())
      assert {:error, _} = Json.handle("JSON.NUMINCRBY", ["key"], MockStore.make())
    end

    test "returns write error when increment rewrite fails" do
      store =
        store_with_json(%{"count" => 10})
        |> Map.put(:put, fn "doc", _raw, 0 -> {:error, :disk_full} end)

      assert {:error, :disk_full} =
               Json.handle("JSON.NUMINCRBY", ["doc", "$.count", "5"], store)

      assert "10" == Json.handle("JSON.GET", ["doc", "$.count"], store)
    end
  end

  # ===========================================================================
  # JSON.TYPE
  # ===========================================================================

  describe "JSON.TYPE" do
    test "returns 'object' for map" do
      store = store_with_json(%{"a" => 1})
      assert "object" == Json.handle("JSON.TYPE", ["doc"], store)
    end

    test "returns 'array' for list" do
      store = store_with_json([1, 2, 3])
      assert "array" == Json.handle("JSON.TYPE", ["doc"], store)
    end

    test "returns 'string' for string" do
      store = store_with_json("hello")
      assert "string" == Json.handle("JSON.TYPE", ["doc"], store)
    end

    test "returns 'integer' for integer" do
      store = store_with_json(42)
      assert "integer" == Json.handle("JSON.TYPE", ["doc"], store)
    end

    test "returns 'number' for float" do
      store = store_with_json(3.14)
      assert "number" == Json.handle("JSON.TYPE", ["doc"], store)
    end

    test "returns 'boolean' for boolean" do
      store = store_with_json(true)
      assert "boolean" == Json.handle("JSON.TYPE", ["doc"], store)
    end

    test "returns 'null' for null" do
      store = store_with_json(nil)
      assert "null" == Json.handle("JSON.TYPE", ["doc"], store)
    end

    test "returns type at nested path" do
      store = store_with_json(%{"name" => "Alice", "age" => 30, "tags" => ["a"]})
      assert "string" == Json.handle("JSON.TYPE", ["doc", "$.name"], store)
      assert "integer" == Json.handle("JSON.TYPE", ["doc", "$.age"], store)
      assert "array" == Json.handle("JSON.TYPE", ["doc", "$.tags"], store)
    end

    test "returns nil for missing key" do
      store = MockStore.make()
      assert nil == Json.handle("JSON.TYPE", ["missing"], store)
    end

    test "returns nil for missing path" do
      store = store_with_json(%{"a" => 1})
      assert nil == Json.handle("JSON.TYPE", ["doc", "$.b"], store)
    end

    test "wrong number of arguments returns error" do
      assert {:error, _} = Json.handle("JSON.TYPE", [], MockStore.make())
    end

    test "returns WRONGTYPE for compound keys" do
      store = MockStore.make()
      assert 1 == Hash.handle("HSET", ["doc", "field", "value"], store)

      assert {:error, msg} = Json.handle("JSON.TYPE", ["doc"], store)
      assert msg =~ "WRONGTYPE"
      assert "value" == Hash.handle("HGET", ["doc", "field"], store)
    end
  end

  # ===========================================================================
  # JSON.STRLEN
  # ===========================================================================

  describe "JSON.STRLEN" do
    test "returns length of root string" do
      store = store_with_json("hello")
      assert 5 == Json.handle("JSON.STRLEN", ["doc"], store)
    end

    test "returns length of nested string" do
      store = store_with_json(%{"name" => "Alice"})
      assert 5 == Json.handle("JSON.STRLEN", ["doc", "$.name"], store)
    end

    test "returns error for non-string at root" do
      store = store_with_json(42)
      assert {:error, msg} = Json.handle("JSON.STRLEN", ["doc"], store)
      assert msg =~ "not a string"
    end

    test "returns error for non-string at path" do
      store = store_with_json(%{"age" => 30})
      assert {:error, msg} = Json.handle("JSON.STRLEN", ["doc", "$.age"], store)
      assert msg =~ "not a string"
    end

    test "returns nil for missing key" do
      assert nil == Json.handle("JSON.STRLEN", ["missing"], MockStore.make())
    end

    test "returns nil for missing path" do
      store = store_with_json(%{"a" => "x"})
      assert nil == Json.handle("JSON.STRLEN", ["doc", "$.b"], store)
    end

    test "returns length for unicode string" do
      store = store_with_json(%{"text" => "cafe\u0301"})
      # "cafe\u0301" = "café" — 4 grapheme clusters (e + combining accent = 1 grapheme)
      assert 4 == Json.handle("JSON.STRLEN", ["doc", "$.text"], store)
    end

    test "returns 0 for empty string" do
      store = store_with_json(%{"empty" => ""})
      assert 0 == Json.handle("JSON.STRLEN", ["doc", "$.empty"], store)
    end

    test "wrong number of arguments returns error" do
      assert {:error, _} = Json.handle("JSON.STRLEN", [], MockStore.make())
    end
  end

  # ===========================================================================
  # JSON.OBJKEYS
  # ===========================================================================

  describe "JSON.OBJKEYS" do
    test "returns keys of root object" do
      store = store_with_json(%{"b" => 2, "a" => 1})
      keys = Json.handle("JSON.OBJKEYS", ["doc"], store)
      assert is_list(keys)
      assert Enum.sort(keys) == ["a", "b"]
    end

    test "returns keys of nested object" do
      store = store_with_json(%{"inner" => %{"x" => 1, "y" => 2}})
      keys = Json.handle("JSON.OBJKEYS", ["doc", "$.inner"], store)
      assert Enum.sort(keys) == ["x", "y"]
    end

    test "returns error for non-object at root" do
      store = store_with_json([1, 2, 3])
      assert {:error, msg} = Json.handle("JSON.OBJKEYS", ["doc"], store)
      assert msg =~ "not an object"
    end

    test "returns error for non-object at path" do
      store = store_with_json(%{"arr" => [1, 2]})
      assert {:error, msg} = Json.handle("JSON.OBJKEYS", ["doc", "$.arr"], store)
      assert msg =~ "not an object"
    end

    test "returns nil for missing key" do
      assert nil == Json.handle("JSON.OBJKEYS", ["missing"], MockStore.make())
    end

    test "returns nil for missing path" do
      store = store_with_json(%{"a" => 1})
      assert nil == Json.handle("JSON.OBJKEYS", ["doc", "$.b"], store)
    end

    test "returns empty list for empty object" do
      store = store_with_json(%{})
      assert [] == Json.handle("JSON.OBJKEYS", ["doc"], store)
    end

    test "wrong number of arguments returns error" do
      assert {:error, _} = Json.handle("JSON.OBJKEYS", [], MockStore.make())
    end
  end

  # ===========================================================================
  # JSON.OBJLEN
  # ===========================================================================

  describe "JSON.OBJLEN" do
    test "returns size of root object" do
      store = store_with_json(%{"a" => 1, "b" => 2, "c" => 3})
      assert 3 == Json.handle("JSON.OBJLEN", ["doc"], store)
    end

    test "returns size of nested object" do
      store = store_with_json(%{"inner" => %{"x" => 1}})
      assert 1 == Json.handle("JSON.OBJLEN", ["doc", "$.inner"], store)
    end

    test "returns 0 for empty object" do
      store = store_with_json(%{})
      assert 0 == Json.handle("JSON.OBJLEN", ["doc"], store)
    end

    test "returns error for non-object" do
      store = store_with_json([1, 2])
      assert {:error, _} = Json.handle("JSON.OBJLEN", ["doc"], store)
    end

    test "returns nil for missing key" do
      assert nil == Json.handle("JSON.OBJLEN", ["missing"], MockStore.make())
    end

    test "returns nil for missing path" do
      store = store_with_json(%{"a" => 1})
      assert nil == Json.handle("JSON.OBJLEN", ["doc", "$.b"], store)
    end

    test "wrong number of arguments returns error" do
      assert {:error, _} = Json.handle("JSON.OBJLEN", [], MockStore.make())
    end
  end

  # ===========================================================================
  # JSON.ARRAPPEND
  # ===========================================================================

  describe "JSON.ARRAPPEND" do
    test "appends single value to array" do
      store = store_with_json(%{"arr" => [1, 2]})
      assert 3 == Json.handle("JSON.ARRAPPEND", ["doc", "$.arr", "3"], store)
      result = Jason.decode!(Json.handle("JSON.GET", ["doc", "$.arr"], store))
      assert result == [1, 2, 3]
    end

    test "appends multiple values to array" do
      store = store_with_json(%{"arr" => [1]})
      assert 4 == Json.handle("JSON.ARRAPPEND", ["doc", "$.arr", "2", "3", "4"], store)
      result = Jason.decode!(Json.handle("JSON.GET", ["doc", "$.arr"], store))
      assert result == [1, 2, 3, 4]
    end

    test "appends object to array" do
      store = store_with_json(%{"arr" => []})
      assert 1 == Json.handle("JSON.ARRAPPEND", ["doc", "$.arr", ~s({"x":1})], store)
      result = Jason.decode!(Json.handle("JSON.GET", ["doc", "$.arr"], store))
      assert result == [%{"x" => 1}]
    end

    test "appends string to array" do
      store = store_with_json(%{"arr" => []})
      assert 1 == Json.handle("JSON.ARRAPPEND", ["doc", "$.arr", ~s("hello")], store)
      result = Jason.decode!(Json.handle("JSON.GET", ["doc", "$.arr"], store))
      assert result == ["hello"]
    end

    test "appends to nested array" do
      store = store_with_json(%{"a" => %{"b" => [10]}})
      assert 2 == Json.handle("JSON.ARRAPPEND", ["doc", "$.a.b", "20"], store)
      result = Jason.decode!(Json.handle("JSON.GET", ["doc", "$.a.b"], store))
      assert result == [10, 20]
    end

    test "returns error for non-array" do
      store = store_with_json(%{"obj" => %{}})
      assert {:error, msg} = Json.handle("JSON.ARRAPPEND", ["doc", "$.obj", "1"], store)
      assert msg =~ "not an array"
    end

    test "returns error for missing key" do
      store = MockStore.make()
      assert {:error, _} = Json.handle("JSON.ARRAPPEND", ["missing", "$.arr", "1"], store)
    end

    test "returns error for missing path" do
      store = store_with_json(%{"a" => 1})
      assert {:error, msg} = Json.handle("JSON.ARRAPPEND", ["doc", "$.arr", "1"], store)
      assert msg =~ "path does not exist"
    end

    test "returns error for invalid JSON value" do
      store = store_with_json(%{"arr" => []})
      assert {:error, msg} = Json.handle("JSON.ARRAPPEND", ["doc", "$.arr", "not{json"], store)
      assert msg =~ "invalid JSON"
    end

    test "wrong number of arguments returns error" do
      assert {:error, _} = Json.handle("JSON.ARRAPPEND", [], MockStore.make())
      assert {:error, _} = Json.handle("JSON.ARRAPPEND", ["key"], MockStore.make())
      assert {:error, _} = Json.handle("JSON.ARRAPPEND", ["key", "$.arr"], MockStore.make())
    end

    test "returns write error when append rewrite fails" do
      store =
        store_with_json(%{"arr" => [1, 2]})
        |> Map.put(:put, fn "doc", _raw, 0 -> {:error, :disk_full} end)

      assert {:error, :disk_full} =
               Json.handle("JSON.ARRAPPEND", ["doc", "$.arr", "3"], store)

      assert [1, 2] == Jason.decode!(Json.handle("JSON.GET", ["doc", "$.arr"], store))
    end
  end

  # ===========================================================================
  # JSON.ARRLEN
  # ===========================================================================

  describe "JSON.ARRLEN" do
    test "returns length of root array" do
      store = store_with_json([1, 2, 3])
      assert 3 == Json.handle("JSON.ARRLEN", ["doc"], store)
    end

    test "returns length of nested array" do
      store = store_with_json(%{"arr" => [10, 20]})
      assert 2 == Json.handle("JSON.ARRLEN", ["doc", "$.arr"], store)
    end

    test "returns 0 for empty array" do
      store = store_with_json([])
      assert 0 == Json.handle("JSON.ARRLEN", ["doc"], store)
    end

    test "returns error for non-array at root" do
      store = store_with_json(%{"a" => 1})
      assert {:error, _} = Json.handle("JSON.ARRLEN", ["doc"], store)
    end

    test "returns nil for missing key" do
      assert nil == Json.handle("JSON.ARRLEN", ["missing"], MockStore.make())
    end

    test "returns nil for missing path" do
      store = store_with_json(%{"a" => 1})
      assert nil == Json.handle("JSON.ARRLEN", ["doc", "$.b"], store)
    end

    test "wrong number of arguments returns error" do
      assert {:error, _} = Json.handle("JSON.ARRLEN", [], MockStore.make())
    end
  end

  # ===========================================================================
  # JSON.TOGGLE
  # ===========================================================================

  describe "JSON.TOGGLE" do
    test "toggles true to false" do
      store = store_with_json(%{"active" => true})
      result = Json.handle("JSON.TOGGLE", ["doc", "$.active"], store)
      assert result == "false"

      got = Json.handle("JSON.GET", ["doc", "$.active"], store)
      assert got == "false"
    end

    test "toggles false to true" do
      store = store_with_json(%{"active" => false})
      result = Json.handle("JSON.TOGGLE", ["doc", "$.active"], store)
      assert result == "true"

      got = Json.handle("JSON.GET", ["doc", "$.active"], store)
      assert got == "true"
    end

    test "toggles root boolean" do
      store = store_with_json(true)
      result = Json.handle("JSON.TOGGLE", ["doc", "$"], store)
      assert result == "false"
    end

    test "returns error for non-boolean" do
      store = store_with_json(%{"count" => 1})
      assert {:error, msg} = Json.handle("JSON.TOGGLE", ["doc", "$.count"], store)
      assert msg =~ "not a boolean"
    end

    test "returns error for missing key" do
      assert {:error, _} = Json.handle("JSON.TOGGLE", ["missing", "$.x"], MockStore.make())
    end

    test "returns error for missing path" do
      store = store_with_json(%{"a" => true})
      assert {:error, msg} = Json.handle("JSON.TOGGLE", ["doc", "$.b"], store)
      assert msg =~ "path does not exist"
    end

    test "wrong number of arguments returns error" do
      assert {:error, _} = Json.handle("JSON.TOGGLE", [], MockStore.make())
      assert {:error, _} = Json.handle("JSON.TOGGLE", ["key"], MockStore.make())
    end

    test "returns write error when toggle rewrite fails" do
      store =
        store_with_json(%{"active" => true})
        |> Map.put(:put, fn "doc", _raw, 0 -> {:error, :disk_full} end)

      assert {:error, :disk_full} = Json.handle("JSON.TOGGLE", ["doc", "$.active"], store)
      assert "true" == Json.handle("JSON.GET", ["doc", "$.active"], store)
    end
  end

  # ===========================================================================
  # JSON.CLEAR
  # ===========================================================================

  describe "JSON.CLEAR" do
    test "clears root object to empty object" do
      store = store_with_json(%{"a" => 1, "b" => 2})
      assert 1 == Json.handle("JSON.CLEAR", ["doc"], store)
      assert "{}" == Json.handle("JSON.GET", ["doc"], store)
    end

    test "clears root array to empty array" do
      store = store_with_json([1, 2, 3])
      assert 1 == Json.handle("JSON.CLEAR", ["doc"], store)
      assert "[]" == Json.handle("JSON.GET", ["doc"], store)
    end

    test "clears root number to 0" do
      store = store_with_json(42)
      assert 1 == Json.handle("JSON.CLEAR", ["doc"], store)
      assert "0" == Json.handle("JSON.GET", ["doc"], store)
    end

    test "clears nested object" do
      store = store_with_json(%{"inner" => %{"x" => 1, "y" => 2}})
      assert 1 == Json.handle("JSON.CLEAR", ["doc", "$.inner"], store)
      result = Jason.decode!(Json.handle("JSON.GET", ["doc"], store))
      assert result["inner"] == %{}
    end

    test "clears nested array" do
      store = store_with_json(%{"arr" => [1, 2, 3]})
      assert 1 == Json.handle("JSON.CLEAR", ["doc", "$.arr"], store)
      result = Jason.decode!(Json.handle("JSON.GET", ["doc"], store))
      assert result["arr"] == []
    end

    test "clears nested number to 0" do
      store = store_with_json(%{"count" => 99})
      assert 1 == Json.handle("JSON.CLEAR", ["doc", "$.count"], store)
      result = Jason.decode!(Json.handle("JSON.GET", ["doc"], store))
      assert result["count"] == 0
    end

    test "does not change string or boolean" do
      store = store_with_json("hello")
      assert 1 == Json.handle("JSON.CLEAR", ["doc"], store)
      # String stays as string (no clear semantics for string)
      assert ~s("hello") == Json.handle("JSON.GET", ["doc"], store)
    end

    test "returns 0 for missing key" do
      assert 0 == Json.handle("JSON.CLEAR", ["missing"], MockStore.make())
    end

    test "returns 0 for missing path" do
      store = store_with_json(%{"a" => 1})
      assert 0 == Json.handle("JSON.CLEAR", ["doc", "$.b"], store)
    end

    test "wrong number of arguments returns error" do
      assert {:error, _} = Json.handle("JSON.CLEAR", [], MockStore.make())
    end

    test "returns write error when root clear rewrite fails" do
      store =
        store_with_json(%{"a" => 1})
        |> Map.put(:put, fn "doc", _raw, 0 -> {:error, :disk_full} end)

      assert {:error, :disk_full} = Json.handle("JSON.CLEAR", ["doc"], store)
      assert ~s({"a":1}) == Json.handle("JSON.GET", ["doc"], store)
    end

    test "returns write error when nested clear rewrite fails" do
      store =
        store_with_json(%{"nested" => %{"a" => 1}})
        |> Map.put(:put, fn "doc", _raw, 0 -> {:error, :disk_full} end)

      assert {:error, :disk_full} = Json.handle("JSON.CLEAR", ["doc", "$.nested"], store)
      assert %{"a" => 1} == Jason.decode!(Json.handle("JSON.GET", ["doc", "$.nested"], store))
    end
  end

  # ===========================================================================
  # JSON.MGET
  # ===========================================================================

  describe "JSON.MGET" do
    test "gets same path from multiple keys" do
      store = MockStore.make()
      # Store two JSON documents
      raw1 = :erlang.term_to_binary({:json, Jason.encode!(%{"name" => "Alice"})})
      raw2 = :erlang.term_to_binary({:json, Jason.encode!(%{"name" => "Bob"})})
      store.put.("user:1", raw1, 0)
      store.put.("user:2", raw2, 0)

      result = Json.handle("JSON.MGET", ["user:1", "user:2", "$.name"], store)
      assert result == [~s("Alice"), ~s("Bob")]
    end

    test "returns nil for missing keys" do
      store = store_with_json("doc", %{"x" => 1})

      result = Json.handle("JSON.MGET", ["doc", "missing", "$.x"], store)
      assert result == ["1", nil]
    end

    test "returns nil for keys where path doesn't exist" do
      store = MockStore.make()
      raw1 = :erlang.term_to_binary({:json, Jason.encode!(%{"a" => 1})})
      raw2 = :erlang.term_to_binary({:json, Jason.encode!(%{"b" => 2})})
      store.put.("k1", raw1, 0)
      store.put.("k2", raw2, 0)

      result = Json.handle("JSON.MGET", ["k1", "k2", "$.a"], store)
      assert result == ["1", nil]
    end

    test "uses batch_get when the store provides it" do
      raw1 = :erlang.term_to_binary({:json, Jason.encode!(%{"name" => "Ada"})})
      raw2 = :erlang.term_to_binary({:json, Jason.encode!(%{"name" => "Linus"})})

      store = %{
        batch_get: fn keys ->
          Enum.map(keys, fn
            "user:1" -> raw1
            "user:2" -> raw2
            _ -> nil
          end)
        end,
        get: fn key ->
          flunk("JSON.MGET should use batch_get, got per-key GET for #{inspect(key)}")
        end
      }

      assert [~s("Ada"), ~s("Linus"), nil] ==
               Json.handle("JSON.MGET", ["user:1", "user:2", "missing", "$.name"], store)
    end

    test "wrong number of arguments returns error" do
      assert {:error, _} = Json.handle("JSON.MGET", [], MockStore.make())
      assert {:error, _} = Json.handle("JSON.MGET", ["key"], MockStore.make())
    end
  end

  # ===========================================================================
  # JSONPath edge cases
  # ===========================================================================

  describe "JSONPath edge cases" do
    test "bracket notation for object keys: $[\"field\"]" do
      store = store_with_json(%{"field" => "value"})
      assert ~s("value") == Json.handle("JSON.GET", ["doc", ~s($["field"])], store)
    end

    test "bracket notation with single quotes: $['field']" do
      store = store_with_json(%{"field" => "value"})
      assert ~s("value") == Json.handle("JSON.GET", ["doc", "$['field']"], store)
    end

    test "mixed dot and bracket notation" do
      store = store_with_json(%{"a" => [%{"b" => "found"}]})
      assert ~s("found") == Json.handle("JSON.GET", ["doc", "$.a[0].b"], store)
    end

    test "array index 0" do
      store = store_with_json(%{"arr" => ["first", "second"]})
      assert ~s("first") == Json.handle("JSON.GET", ["doc", "$.arr[0]"], store)
    end

    test "negative array index" do
      store = store_with_json(%{"arr" => [1, 2, 3]})
      # -1 means last element
      assert "3" == Json.handle("JSON.GET", ["doc", "$.arr[-1]"], store)
    end

    test "deep nesting (5 levels)" do
      store = store_with_json(%{"a" => %{"b" => %{"c" => %{"d" => %{"e" => 42}}}}})
      assert "42" == Json.handle("JSON.GET", ["doc", "$.a.b.c.d.e"], store)
    end

    test "set on deep nested path" do
      store = store_with_json(%{"a" => %{"b" => %{"c" => 1}}})
      assert :ok = Json.handle("JSON.SET", ["doc", "$.a.b.c", "2"], store)
      assert "2" == Json.handle("JSON.GET", ["doc", "$.a.b.c"], store)
    end

    test "get root path $ returns full document" do
      store = store_with_json(%{"x" => 1})
      result = Json.handle("JSON.GET", ["doc", "$"], store)
      assert Jason.decode!(result) == %{"x" => 1}
    end

    test "empty path segments are ignored gracefully" do
      store = store_with_json(%{"a" => 1})
      # $ with nothing after should return root
      assert ~s({"a":1}) == Json.handle("JSON.GET", ["doc", "$"], store)
    end
  end

  # ===========================================================================
  # Storage format edge cases
  # ===========================================================================

  describe "storage format" do
    test "stored value uses :erlang.term_to_binary with {:json, string} tag" do
      store = MockStore.make()
      Json.handle("JSON.SET", ["key", "$", ~s({"a":1})], store)

      raw = store.get.("key")
      assert {:json, json_str} = :erlang.binary_to_term(raw, [:safe])
      assert is_binary(json_str)
      assert Jason.decode!(json_str) == %{"a" => 1}
    end

    test "non-JSON tagged value returns error on read" do
      store = MockStore.make()
      # Store a plain string (not tagged)
      store.put.("key", "just a string", 0)
      assert {:error, _} = Json.handle("JSON.GET", ["key"], store)
    end

    test "corrupt tagged value returns error on read" do
      store = MockStore.make()
      # Store a valid term_to_binary but with corrupt JSON inside
      raw = :erlang.term_to_binary({:json, "not valid json{"})
      store.put.("key", raw, 0)
      assert {:error, msg} = Json.handle("JSON.GET", ["key"], store)
      assert msg =~ "corrupt JSON"
    end
  end

  # ===========================================================================
  # Cross-command workflows
  # ===========================================================================

  describe "cross-command workflows" do
    test "SET then GET roundtrip" do
      store = MockStore.make()
      obj = %{"users" => [%{"name" => "Alice", "active" => true}]}
      Json.handle("JSON.SET", ["doc", "$", Jason.encode!(obj)], store)

      assert ~s("Alice") == Json.handle("JSON.GET", ["doc", "$.users[0].name"], store)
      assert "true" == Json.handle("JSON.GET", ["doc", "$.users[0].active"], store)
    end

    test "SET, NUMINCRBY, GET" do
      store = MockStore.make()
      Json.handle("JSON.SET", ["doc", "$", ~s({"score":0})], store)
      Json.handle("JSON.NUMINCRBY", ["doc", "$.score", "10"], store)
      Json.handle("JSON.NUMINCRBY", ["doc", "$.score", "5"], store)
      assert "15" == Json.handle("JSON.GET", ["doc", "$.score"], store)
    end

    test "SET, ARRAPPEND, ARRLEN" do
      store = MockStore.make()
      Json.handle("JSON.SET", ["doc", "$", ~s({"tags":[]})], store)
      Json.handle("JSON.ARRAPPEND", ["doc", "$.tags", ~s("elixir")], store)
      Json.handle("JSON.ARRAPPEND", ["doc", "$.tags", ~s("beam")], store)
      assert 2 == Json.handle("JSON.ARRLEN", ["doc", "$.tags"], store)
    end

    test "SET, TOGGLE, GET" do
      store = MockStore.make()
      Json.handle("JSON.SET", ["doc", "$", ~s({"enabled":false})], store)
      Json.handle("JSON.TOGGLE", ["doc", "$.enabled"], store)
      assert "true" == Json.handle("JSON.GET", ["doc", "$.enabled"], store)
    end

    test "SET, DEL nested, GET parent" do
      store = MockStore.make()
      Json.handle("JSON.SET", ["doc", "$", ~s({"a":1,"b":2,"c":3})], store)
      Json.handle("JSON.DEL", ["doc", "$.b"], store)
      result = Jason.decode!(Json.handle("JSON.GET", ["doc"], store))
      assert result == %{"a" => 1, "c" => 3}
    end

    test "SET, CLEAR, TYPE" do
      store = MockStore.make()
      Json.handle("JSON.SET", ["doc", "$", ~s({"nested":{"x":1}})], store)
      Json.handle("JSON.CLEAR", ["doc", "$.nested"], store)
      assert "object" == Json.handle("JSON.TYPE", ["doc", "$.nested"], store)
      assert 0 == Json.handle("JSON.OBJLEN", ["doc", "$.nested"], store)
    end
  end
end
