defmodule Ferricstore.Commands.JsonBugHuntTest do
  @moduledoc """
  Bug-hunt test suite for Ferricstore.Commands.Json.

  Each `describe` block targets a specific edge case or potential bug in the
  JSON command implementation.  Tests that expose actual bugs are annotated
  with comments explaining the root cause.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Json
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Stores a JSON document under `key` (default "doc") and returns the store.
  defp store_with_json(key \\ "doc", json_value) do
    store = MockStore.make()
    json_str = Jason.encode!(json_value)
    raw = :erlang.term_to_binary({:json, json_str})
    store.put.(key, raw, 0)
    store
  end

  # ===========================================================================
  # 1. JSON.SET with invalid JSON — must return error
  # ===========================================================================

  describe "JSON.SET with invalid JSON" do
    test "bare word is rejected" do
      store = MockStore.make()
      assert {:error, msg} = Json.handle("JSON.SET", ["k", "$", "not_json"], store)
      assert msg =~ "invalid JSON"
    end

    test "trailing comma in object is rejected" do
      store = MockStore.make()
      assert {:error, msg} = Json.handle("JSON.SET", ["k", "$", ~s({"a":1,})], store)
      assert msg =~ "invalid JSON"
    end

    test "single quotes are rejected (JSON requires double quotes)" do
      store = MockStore.make()
      assert {:error, msg} = Json.handle("JSON.SET", ["k", "$", "{'a':1}"], store)
      assert msg =~ "invalid JSON"
    end

    test "empty string is rejected" do
      store = MockStore.make()
      assert {:error, msg} = Json.handle("JSON.SET", ["k", "$", ""], store)
      assert msg =~ "invalid JSON"
    end

    test "unclosed brace is rejected" do
      store = MockStore.make()
      assert {:error, msg} = Json.handle("JSON.SET", ["k", "$", "{\"a\":1"], store)
      assert msg =~ "invalid JSON"
    end

    test "unclosed bracket is rejected" do
      store = MockStore.make()
      assert {:error, msg} = Json.handle("JSON.SET", ["k", "$", "[1,2"], store)
      assert msg =~ "invalid JSON"
    end

    test "invalid JSON does not create the key" do
      store = MockStore.make()
      Json.handle("JSON.SET", ["k", "$", "not{json"], store)
      assert nil == Json.handle("JSON.GET", ["k"], store)
    end
  end

  # ===========================================================================
  # 2. JSON.SET nested path "$.a.b.c" — creates nested structure
  # ===========================================================================

  describe "JSON.SET nested path creates nested structure" do
    test "creates $.a.b.c on a new key (no existing document)" do
      store = MockStore.make()
      assert :ok = Json.handle("JSON.SET", ["k", "$.a.b.c", "42"], store)

      doc = Jason.decode!(Json.handle("JSON.GET", ["k"], store))
      assert doc == %{"a" => %{"b" => %{"c" => 42}}}
    end

    test "creates missing intermediate keys on existing document" do
      # Document has {"a": {}}, set $.a.b.c should auto-vivify "b" inside "a"
      store = store_with_json(%{"a" => %{}})
      assert :ok = Json.handle("JSON.SET", ["doc", "$.a.b.c", ~s("deep")], store)

      doc = Jason.decode!(Json.handle("JSON.GET", ["doc"], store))
      assert doc["a"]["b"]["c"] == "deep"
    end

    test "creates nested path two levels deep on new key" do
      store = MockStore.make()
      assert :ok = Json.handle("JSON.SET", ["k", "$.x.y", "true"], store)

      doc = Jason.decode!(Json.handle("JSON.GET", ["k"], store))
      assert doc == %{"x" => %{"y" => true}}
    end

    test "preserves sibling keys when vivifying nested path" do
      store = store_with_json(%{"a" => %{"existing" => 1}})
      assert :ok = Json.handle("JSON.SET", ["doc", "$.a.new_key", "2"], store)

      doc = Jason.decode!(Json.handle("JSON.GET", ["doc"], store))
      assert doc["a"]["existing"] == 1
      assert doc["a"]["new_key"] == 2
    end

    test "deeply nested path (4 levels) on new key" do
      store = MockStore.make()
      assert :ok = Json.handle("JSON.SET", ["k", "$.a.b.c.d", ~s("leaf")], store)

      doc = Jason.decode!(Json.handle("JSON.GET", ["k"], store))
      assert doc == %{"a" => %{"b" => %{"c" => %{"d" => "leaf"}}}}
    end
  end

  # ===========================================================================
  # 3. JSON.GET non-existent path — nil or empty
  # ===========================================================================

  describe "JSON.GET non-existent path" do
    test "returns nil for missing top-level field" do
      store = store_with_json(%{"a" => 1})
      assert nil == Json.handle("JSON.GET", ["doc", "$.missing"], store)
    end

    test "returns nil for missing nested field" do
      store = store_with_json(%{"a" => %{"b" => 1}})
      assert nil == Json.handle("JSON.GET", ["doc", "$.a.missing.deep"], store)
    end

    test "returns nil for missing key entirely" do
      store = MockStore.make()
      assert nil == Json.handle("JSON.GET", ["no_such_key"], store)
    end

    test "returns nil for out-of-bounds positive array index" do
      store = store_with_json(%{"arr" => [1, 2, 3]})
      assert nil == Json.handle("JSON.GET", ["doc", "$.arr[99]"], store)
    end

    test "returns nil for out-of-bounds negative array index" do
      store = store_with_json(%{"arr" => [1, 2, 3]})
      assert nil == Json.handle("JSON.GET", ["doc", "$.arr[-99]"], store)
    end

    test "returns nil when traversing through a scalar" do
      store = store_with_json(%{"a" => 42})
      assert nil == Json.handle("JSON.GET", ["doc", "$.a.b"], store)
    end

    test "returns nil when indexing into a non-array" do
      store = store_with_json(%{"a" => "hello"})
      assert nil == Json.handle("JSON.GET", ["doc", "$.a[0]"], store)
    end

    test "multi-path GET includes nil for missing paths" do
      store = store_with_json(%{"a" => 1})
      result = Jason.decode!(Json.handle("JSON.GET", ["doc", "$.a", "$.nope"], store))
      assert result["$.a"] == 1
      assert result["$.nope"] == nil
    end
  end

  # ===========================================================================
  # 4. JSON.DEL root path "$" — deletes entire key
  # ===========================================================================

  describe "JSON.DEL root path deletes entire key" do
    test "DEL with explicit $ removes the key" do
      store = store_with_json(%{"a" => 1})
      assert 1 == Json.handle("JSON.DEL", ["doc", "$"], store)
      assert nil == Json.handle("JSON.GET", ["doc"], store)
    end

    test "DEL with no path (default) removes the key" do
      store = store_with_json(%{"a" => 1})
      assert 1 == Json.handle("JSON.DEL", ["doc"], store)
      assert nil == Json.handle("JSON.GET", ["doc"], store)
    end

    test "DEL root on array value removes the key" do
      store = store_with_json([1, 2, 3])
      assert 1 == Json.handle("JSON.DEL", ["doc", "$"], store)
      assert nil == Json.handle("JSON.GET", ["doc"], store)
    end

    test "DEL root on scalar value removes the key" do
      store = store_with_json(42)
      assert 1 == Json.handle("JSON.DEL", ["doc", "$"], store)
      assert nil == Json.handle("JSON.GET", ["doc"], store)
    end

    test "DEL root on null value removes the key" do
      store = store_with_json(nil)
      assert 1 == Json.handle("JSON.DEL", ["doc", "$"], store)
      assert nil == Json.handle("JSON.GET", ["doc"], store)
    end

    test "DEL root on boolean value removes the key" do
      store = store_with_json(true)
      assert 1 == Json.handle("JSON.DEL", ["doc", "$"], store)
      assert nil == Json.handle("JSON.GET", ["doc"], store)
    end

    test "DEL root on string value removes the key" do
      store = store_with_json("hello")
      assert 1 == Json.handle("JSON.DEL", ["doc", "$"], store)
      assert nil == Json.handle("JSON.GET", ["doc"], store)
    end

    test "DEL returns 0 for non-existent key" do
      store = MockStore.make()
      assert 0 == Json.handle("JSON.DEL", ["nope", "$"], store)
    end

    test "after DEL root, subsequent commands see nil" do
      store = store_with_json(%{"x" => 1})
      Json.handle("JSON.DEL", ["doc", "$"], store)
      assert nil == Json.handle("JSON.TYPE", ["doc"], store)
      assert nil == Json.handle("JSON.STRLEN", ["doc"], store)
      assert nil == Json.handle("JSON.OBJKEYS", ["doc"], store)
    end
  end

  # ===========================================================================
  # 5. JSON.NUMINCRBY on non-number — error
  # ===========================================================================

  describe "JSON.NUMINCRBY on non-number" do
    test "error when value is a string" do
      store = store_with_json(%{"name" => "Alice"})
      assert {:error, msg} = Json.handle("JSON.NUMINCRBY", ["doc", "$.name", "1"], store)
      assert msg =~ "not a number"
    end

    test "error when value is a boolean" do
      store = store_with_json(%{"flag" => true})
      assert {:error, msg} = Json.handle("JSON.NUMINCRBY", ["doc", "$.flag", "1"], store)
      assert msg =~ "not a number"
    end

    test "error when value is null" do
      store = store_with_json(%{"val" => nil})
      assert {:error, msg} = Json.handle("JSON.NUMINCRBY", ["doc", "$.val", "1"], store)
      assert msg =~ "not a number"
    end

    test "error when value is an array" do
      store = store_with_json(%{"arr" => [1, 2, 3]})
      assert {:error, msg} = Json.handle("JSON.NUMINCRBY", ["doc", "$.arr", "1"], store)
      assert msg =~ "not a number"
    end

    test "error when value is an object" do
      store = store_with_json(%{"obj" => %{"a" => 1}})
      assert {:error, msg} = Json.handle("JSON.NUMINCRBY", ["doc", "$.obj", "1"], store)
      assert msg =~ "not a number"
    end

    test "error when path does not exist" do
      store = store_with_json(%{"a" => 1})
      assert {:error, msg} = Json.handle("JSON.NUMINCRBY", ["doc", "$.missing", "1"], store)
      assert msg =~ "path does not exist"
    end

    test "error when key does not exist" do
      store = MockStore.make()
      assert {:error, msg} = Json.handle("JSON.NUMINCRBY", ["nope", "$.a", "1"], store)
      assert msg =~ "key does not exist"
    end

    test "error when increment is not a valid number" do
      store = store_with_json(%{"n" => 1})
      assert {:error, msg} = Json.handle("JSON.NUMINCRBY", ["doc", "$.n", "abc"], store)
      assert msg =~ "not a number"
    end
  end

  # ===========================================================================
  # 6. JSON.TYPE on array vs object vs string vs number
  # ===========================================================================

  describe "JSON.TYPE distinguishes all JSON types" do
    test "object" do
      store = store_with_json(%{"a" => 1})
      assert "object" == Json.handle("JSON.TYPE", ["doc"], store)
    end

    test "array" do
      store = store_with_json([1, 2, 3])
      assert "array" == Json.handle("JSON.TYPE", ["doc"], store)
    end

    test "string" do
      store = store_with_json("hello")
      assert "string" == Json.handle("JSON.TYPE", ["doc"], store)
    end

    test "integer" do
      store = store_with_json(42)
      assert "integer" == Json.handle("JSON.TYPE", ["doc"], store)
    end

    test "float returns 'number'" do
      store = store_with_json(3.14)
      assert "number" == Json.handle("JSON.TYPE", ["doc"], store)
    end

    test "boolean true" do
      store = store_with_json(true)
      assert "boolean" == Json.handle("JSON.TYPE", ["doc"], store)
    end

    test "boolean false" do
      store = store_with_json(false)
      assert "boolean" == Json.handle("JSON.TYPE", ["doc"], store)
    end

    test "null" do
      store = store_with_json(nil)
      assert "null" == Json.handle("JSON.TYPE", ["doc"], store)
    end

    test "empty object" do
      store = store_with_json(%{})
      assert "object" == Json.handle("JSON.TYPE", ["doc"], store)
    end

    test "empty array" do
      store = store_with_json([])
      assert "array" == Json.handle("JSON.TYPE", ["doc"], store)
    end

    test "empty string" do
      store = store_with_json("")
      assert "string" == Json.handle("JSON.TYPE", ["doc"], store)
    end

    test "zero" do
      store = store_with_json(0)
      assert "integer" == Json.handle("JSON.TYPE", ["doc"], store)
    end

    test "negative integer" do
      store = store_with_json(-5)
      assert "integer" == Json.handle("JSON.TYPE", ["doc"], store)
    end

    test "nested type detection via path" do
      store =
        store_with_json(%{
          "str" => "hello",
          "num" => 42,
          "flt" => 1.5,
          "bool" => true,
          "nil" => nil,
          "arr" => [],
          "obj" => %{}
        })

      assert "string" == Json.handle("JSON.TYPE", ["doc", "$.str"], store)
      assert "integer" == Json.handle("JSON.TYPE", ["doc", "$.num"], store)
      assert "number" == Json.handle("JSON.TYPE", ["doc", "$.flt"], store)
      assert "boolean" == Json.handle("JSON.TYPE", ["doc", "$.bool"], store)
      assert "null" == Json.handle("JSON.TYPE", ["doc", "$.nil"], store)
      assert "array" == Json.handle("JSON.TYPE", ["doc", "$.arr"], store)
      assert "object" == Json.handle("JSON.TYPE", ["doc", "$.obj"], store)
    end

    test "returns nil for non-existent key" do
      store = MockStore.make()
      assert nil == Json.handle("JSON.TYPE", ["no_key"], store)
    end

    test "returns nil for non-existent path" do
      store = store_with_json(%{"a" => 1})
      assert nil == Json.handle("JSON.TYPE", ["doc", "$.missing"], store)
    end
  end

  # ===========================================================================
  # 7. JSON.STRLEN on non-string — error or nil
  # ===========================================================================

  describe "JSON.STRLEN on non-string" do
    test "error when root is an integer" do
      store = store_with_json(42)
      assert {:error, msg} = Json.handle("JSON.STRLEN", ["doc"], store)
      assert msg =~ "not a string"
    end

    test "error when root is a float" do
      store = store_with_json(3.14)
      assert {:error, msg} = Json.handle("JSON.STRLEN", ["doc"], store)
      assert msg =~ "not a string"
    end

    test "error when root is a boolean" do
      store = store_with_json(true)
      assert {:error, msg} = Json.handle("JSON.STRLEN", ["doc"], store)
      assert msg =~ "not a string"
    end

    test "error when root is an object" do
      store = store_with_json(%{"a" => 1})
      assert {:error, msg} = Json.handle("JSON.STRLEN", ["doc"], store)
      assert msg =~ "not a string"
    end

    test "error when root is an array" do
      store = store_with_json([1, 2])
      assert {:error, msg} = Json.handle("JSON.STRLEN", ["doc"], store)
      assert msg =~ "not a string"
    end

    test "error when root is null" do
      store = store_with_json(nil)
      assert {:error, msg} = Json.handle("JSON.STRLEN", ["doc"], store)
      assert msg =~ "not a string"
    end

    test "error when nested path value is a number" do
      store = store_with_json(%{"count" => 99})
      assert {:error, msg} = Json.handle("JSON.STRLEN", ["doc", "$.count"], store)
      assert msg =~ "not a string"
    end

    test "error when nested path value is a boolean" do
      store = store_with_json(%{"active" => false})
      assert {:error, msg} = Json.handle("JSON.STRLEN", ["doc", "$.active"], store)
      assert msg =~ "not a string"
    end

    test "error when nested path value is an object" do
      store = store_with_json(%{"inner" => %{"x" => 1}})
      assert {:error, msg} = Json.handle("JSON.STRLEN", ["doc", "$.inner"], store)
      assert msg =~ "not a string"
    end

    test "error when nested path value is an array" do
      store = store_with_json(%{"tags" => ["a", "b"]})
      assert {:error, msg} = Json.handle("JSON.STRLEN", ["doc", "$.tags"], store)
      assert msg =~ "not a string"
    end

    test "nil for missing key" do
      store = MockStore.make()
      assert nil == Json.handle("JSON.STRLEN", ["nope"], store)
    end

    test "nil for missing path" do
      store = store_with_json(%{"a" => "hello"})
      assert nil == Json.handle("JSON.STRLEN", ["doc", "$.missing"], store)
    end
  end

  # ===========================================================================
  # 8. JSON.OBJKEYS on non-object — error
  # ===========================================================================

  describe "JSON.OBJKEYS on non-object" do
    test "error when root is an array" do
      store = store_with_json([1, 2, 3])
      assert {:error, msg} = Json.handle("JSON.OBJKEYS", ["doc"], store)
      assert msg =~ "not an object"
    end

    test "error when root is a string" do
      store = store_with_json("hello")
      assert {:error, msg} = Json.handle("JSON.OBJKEYS", ["doc"], store)
      assert msg =~ "not an object"
    end

    test "error when root is a number" do
      store = store_with_json(42)
      assert {:error, msg} = Json.handle("JSON.OBJKEYS", ["doc"], store)
      assert msg =~ "not an object"
    end

    test "error when root is a boolean" do
      store = store_with_json(true)
      assert {:error, msg} = Json.handle("JSON.OBJKEYS", ["doc"], store)
      assert msg =~ "not an object"
    end

    test "error when root is null" do
      store = store_with_json(nil)
      assert {:error, msg} = Json.handle("JSON.OBJKEYS", ["doc"], store)
      assert msg =~ "not an object"
    end

    test "error when nested path value is an array" do
      store = store_with_json(%{"arr" => [1, 2]})
      assert {:error, msg} = Json.handle("JSON.OBJKEYS", ["doc", "$.arr"], store)
      assert msg =~ "not an object"
    end

    test "error when nested path value is a string" do
      store = store_with_json(%{"name" => "Alice"})
      assert {:error, msg} = Json.handle("JSON.OBJKEYS", ["doc", "$.name"], store)
      assert msg =~ "not an object"
    end

    test "error when nested path value is a number" do
      store = store_with_json(%{"count" => 42})
      assert {:error, msg} = Json.handle("JSON.OBJKEYS", ["doc", "$.count"], store)
      assert msg =~ "not an object"
    end

    test "nil for missing key" do
      store = MockStore.make()
      assert nil == Json.handle("JSON.OBJKEYS", ["nope"], store)
    end

    test "nil for missing path" do
      store = store_with_json(%{"a" => %{"x" => 1}})
      assert nil == Json.handle("JSON.OBJKEYS", ["doc", "$.missing"], store)
    end
  end

  # ===========================================================================
  # 9. JSON.ARRAPPEND to non-array — error
  # ===========================================================================

  describe "JSON.ARRAPPEND to non-array" do
    test "error when target is an object" do
      store = store_with_json(%{"obj" => %{"a" => 1}})
      assert {:error, msg} = Json.handle("JSON.ARRAPPEND", ["doc", "$.obj", "1"], store)
      assert msg =~ "not an array"
    end

    test "error when target is a string" do
      store = store_with_json(%{"name" => "Alice"})
      assert {:error, msg} = Json.handle("JSON.ARRAPPEND", ["doc", "$.name", "1"], store)
      assert msg =~ "not an array"
    end

    test "error when target is a number" do
      store = store_with_json(%{"count" => 42})
      assert {:error, msg} = Json.handle("JSON.ARRAPPEND", ["doc", "$.count", "1"], store)
      assert msg =~ "not an array"
    end

    test "error when target is a boolean" do
      store = store_with_json(%{"flag" => true})
      assert {:error, msg} = Json.handle("JSON.ARRAPPEND", ["doc", "$.flag", "1"], store)
      assert msg =~ "not an array"
    end

    test "error when target is null" do
      store = store_with_json(%{"val" => nil})
      assert {:error, msg} = Json.handle("JSON.ARRAPPEND", ["doc", "$.val", "1"], store)
      assert msg =~ "not an array"
    end

    test "error when target root is a scalar (via $ path)" do
      store = store_with_json(42)
      assert {:error, msg} = Json.handle("JSON.ARRAPPEND", ["doc", "$", "1"], store)
      assert msg =~ "not an array"
    end

    test "error when path does not exist" do
      store = store_with_json(%{"a" => 1})
      assert {:error, msg} = Json.handle("JSON.ARRAPPEND", ["doc", "$.missing", "1"], store)
      assert msg =~ "path does not exist"
    end

    test "error when key does not exist" do
      store = MockStore.make()
      assert {:error, msg} = Json.handle("JSON.ARRAPPEND", ["nope", "$.arr", "1"], store)
      assert msg =~ "key does not exist"
    end

    test "error when appending invalid JSON value" do
      store = store_with_json(%{"arr" => [1]})
      assert {:error, msg} = Json.handle("JSON.ARRAPPEND", ["doc", "$.arr", "{bad"], store)
      assert msg =~ "invalid JSON"
    end
  end

  # ===========================================================================
  # 10. JSON.TOGGLE on non-boolean — error
  # ===========================================================================

  describe "JSON.TOGGLE on non-boolean" do
    test "error when value is a string" do
      store = store_with_json(%{"name" => "Alice"})
      assert {:error, msg} = Json.handle("JSON.TOGGLE", ["doc", "$.name"], store)
      assert msg =~ "not a boolean"
    end

    test "error when value is a number" do
      store = store_with_json(%{"count" => 0})
      assert {:error, msg} = Json.handle("JSON.TOGGLE", ["doc", "$.count"], store)
      assert msg =~ "not a boolean"
    end

    test "error when value is null" do
      store = store_with_json(%{"val" => nil})
      assert {:error, msg} = Json.handle("JSON.TOGGLE", ["doc", "$.val"], store)
      assert msg =~ "not a boolean"
    end

    test "error when value is an array" do
      store = store_with_json(%{"arr" => [true, false]})
      assert {:error, msg} = Json.handle("JSON.TOGGLE", ["doc", "$.arr"], store)
      assert msg =~ "not a boolean"
    end

    test "error when value is an object" do
      store = store_with_json(%{"obj" => %{"flag" => true}})
      assert {:error, msg} = Json.handle("JSON.TOGGLE", ["doc", "$.obj"], store)
      assert msg =~ "not a boolean"
    end

    test "error when path does not exist" do
      store = store_with_json(%{"a" => true})
      assert {:error, msg} = Json.handle("JSON.TOGGLE", ["doc", "$.missing"], store)
      assert msg =~ "path does not exist"
    end

    test "error when key does not exist" do
      store = MockStore.make()
      assert {:error, msg} = Json.handle("JSON.TOGGLE", ["nope", "$.x"], store)
      assert msg =~ "key does not exist"
    end

    test "toggling number 1 (truthy in some languages) returns error" do
      store = store_with_json(%{"val" => 1})
      assert {:error, msg} = Json.handle("JSON.TOGGLE", ["doc", "$.val"], store)
      assert msg =~ "not a boolean"
    end

    test "toggling empty string returns error" do
      store = store_with_json(%{"val" => ""})
      assert {:error, msg} = Json.handle("JSON.TOGGLE", ["doc", "$.val"], store)
      assert msg =~ "not a boolean"
    end
  end

  # ===========================================================================
  # 11. JSON.CLEAR resets to empty object/array/0
  # ===========================================================================

  describe "JSON.CLEAR resets containers and numbers" do
    test "clears object to empty object" do
      store = store_with_json(%{"a" => 1, "b" => 2})
      assert 1 == Json.handle("JSON.CLEAR", ["doc"], store)
      assert "{}" == Json.handle("JSON.GET", ["doc"], store)
    end

    test "clears array to empty array" do
      store = store_with_json([1, 2, 3])
      assert 1 == Json.handle("JSON.CLEAR", ["doc"], store)
      assert "[]" == Json.handle("JSON.GET", ["doc"], store)
    end

    test "clears integer to 0" do
      store = store_with_json(42)
      assert 1 == Json.handle("JSON.CLEAR", ["doc"], store)
      assert "0" == Json.handle("JSON.GET", ["doc"], store)
    end

    test "clears float to 0" do
      store = store_with_json(3.14)
      assert 1 == Json.handle("JSON.CLEAR", ["doc"], store)
      assert "0" == Json.handle("JSON.GET", ["doc"], store)
    end

    test "clears negative number to 0" do
      store = store_with_json(-99)
      assert 1 == Json.handle("JSON.CLEAR", ["doc"], store)
      assert "0" == Json.handle("JSON.GET", ["doc"], store)
    end

    test "does not change string (leaves unchanged)" do
      store = store_with_json("hello")
      assert 1 == Json.handle("JSON.CLEAR", ["doc"], store)
      # Strings are not clearable; the value stays as is
      assert ~s("hello") == Json.handle("JSON.GET", ["doc"], store)
    end

    test "does not change boolean (leaves unchanged)" do
      store = store_with_json(true)
      assert 1 == Json.handle("JSON.CLEAR", ["doc"], store)
      assert "true" == Json.handle("JSON.GET", ["doc"], store)
    end

    test "does not change null (leaves unchanged)" do
      store = store_with_json(nil)
      assert 1 == Json.handle("JSON.CLEAR", ["doc"], store)
      assert "null" == Json.handle("JSON.GET", ["doc"], store)
    end

    test "clears nested object to empty" do
      store = store_with_json(%{"inner" => %{"x" => 1, "y" => 2}})
      assert 1 == Json.handle("JSON.CLEAR", ["doc", "$.inner"], store)
      doc = Jason.decode!(Json.handle("JSON.GET", ["doc"], store))
      assert doc["inner"] == %{}
    end

    test "clears nested array to empty" do
      store = store_with_json(%{"arr" => [1, 2, 3]})
      assert 1 == Json.handle("JSON.CLEAR", ["doc", "$.arr"], store)
      doc = Jason.decode!(Json.handle("JSON.GET", ["doc"], store))
      assert doc["arr"] == []
    end

    test "clears nested number to 0" do
      store = store_with_json(%{"count" => 100})
      assert 1 == Json.handle("JSON.CLEAR", ["doc", "$.count"], store)
      doc = Jason.decode!(Json.handle("JSON.GET", ["doc"], store))
      assert doc["count"] == 0
    end

    test "returns 0 for non-existent key" do
      store = MockStore.make()
      assert 0 == Json.handle("JSON.CLEAR", ["nope"], store)
    end

    test "returns 0 for non-existent path" do
      store = store_with_json(%{"a" => 1})
      assert 0 == Json.handle("JSON.CLEAR", ["doc", "$.missing"], store)
    end

    test "clears already-empty object is idempotent" do
      store = store_with_json(%{})
      assert 1 == Json.handle("JSON.CLEAR", ["doc"], store)
      assert "{}" == Json.handle("JSON.GET", ["doc"], store)
    end

    test "clears already-empty array is idempotent" do
      store = store_with_json([])
      assert 1 == Json.handle("JSON.CLEAR", ["doc"], store)
      assert "[]" == Json.handle("JSON.GET", ["doc"], store)
    end

    test "clears zero is idempotent" do
      store = store_with_json(0)
      assert 1 == Json.handle("JSON.CLEAR", ["doc"], store)
      assert "0" == Json.handle("JSON.GET", ["doc"], store)
    end
  end

  # ===========================================================================
  # 12. JSON.MGET across multiple keys
  # ===========================================================================

  describe "JSON.MGET across multiple keys" do
    test "retrieves same path from multiple keys" do
      store = MockStore.make()
      store_json(store, "k1", %{"name" => "Alice"})
      store_json(store, "k2", %{"name" => "Bob"})
      store_json(store, "k3", %{"name" => "Charlie"})

      result = Json.handle("JSON.MGET", ["k1", "k2", "k3", "$.name"], store)
      assert result == [~s("Alice"), ~s("Bob"), ~s("Charlie")]
    end

    test "returns nil for keys that don't exist" do
      store = MockStore.make()
      store_json(store, "k1", %{"a" => 1})

      result = Json.handle("JSON.MGET", ["k1", "missing", "$.a"], store)
      assert result == ["1", nil]
    end

    test "returns nil for keys where the path doesn't match" do
      store = MockStore.make()
      store_json(store, "k1", %{"a" => 1})
      store_json(store, "k2", %{"b" => 2})

      result = Json.handle("JSON.MGET", ["k1", "k2", "$.a"], store)
      assert result == ["1", nil]
    end

    test "works with root path $" do
      store = MockStore.make()
      store_json(store, "k1", %{"x" => 1})
      store_json(store, "k2", %{"y" => 2})

      result = Json.handle("JSON.MGET", ["k1", "k2", "$"], store)
      assert length(result) == 2
      assert Jason.decode!(Enum.at(result, 0)) == %{"x" => 1}
      assert Jason.decode!(Enum.at(result, 1)) == %{"y" => 2}
    end

    test "works with nested path" do
      store = MockStore.make()
      store_json(store, "k1", %{"a" => %{"b" => 10}})
      store_json(store, "k2", %{"a" => %{"b" => 20}})

      result = Json.handle("JSON.MGET", ["k1", "k2", "$.a.b"], store)
      assert result == ["10", "20"]
    end

    test "single key still returns a list" do
      store = MockStore.make()
      store_json(store, "k1", %{"val" => 42})

      result = Json.handle("JSON.MGET", ["k1", "$.val"], store)
      assert result == ["42"]
    end

    test "all keys missing returns list of nils" do
      store = MockStore.make()
      result = Json.handle("JSON.MGET", ["a", "b", "c", "$.x"], store)
      assert result == [nil, nil, nil]
    end

    test "non-JSON key returns nil for that position" do
      store = MockStore.make()
      store_json(store, "k1", %{"a" => 1})
      # Store a plain (non-JSON) value at k2
      store.put.("k2", "plain string", 0)

      result = Json.handle("JSON.MGET", ["k1", "k2", "$.a"], store)
      assert result == ["1", nil]
    end

    test "wrong number of arguments returns error" do
      store = MockStore.make()
      assert {:error, _} = Json.handle("JSON.MGET", [], store)
      assert {:error, _} = Json.handle("JSON.MGET", ["single"], store)
    end
  end

  # Helper to store JSON in an existing store without creating a new one
  defp store_json(store, key, value) do
    json_str = Jason.encode!(value)
    raw = :erlang.term_to_binary({:json, json_str})
    store.put.(key, raw, 0)
  end
end
