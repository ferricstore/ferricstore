defmodule Ferricstore.Commands.JsonTest.Sections.JsonClear do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.Json
      alias Ferricstore.Commands.Hash
      alias Ferricstore.Test.MockStore

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
  end
end
