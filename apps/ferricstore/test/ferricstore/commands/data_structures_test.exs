defmodule Ferricstore.Commands.DataStructuresTest do
  @moduledoc """
  Cross-cutting tests for data structure commands: TYPE enforcement,
  DEL cleanup, and dispatcher routing.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Bitmap, Dispatcher, Hash, Json, List, Set, SortedSet, Strings}
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # TYPE command
  # ---------------------------------------------------------------------------

  describe "TYPE command" do
    test "TYPE returns 'none' for nonexistent key" do
      store = MockStore.make()
      assert {:simple, "none"} == Strings.handle("TYPE", ["missing"], store)
    end

    test "TYPE returns 'string' for string key" do
      store = MockStore.make()
      Strings.handle("SET", ["k", "v"], store)
      assert {:simple, "string"} == Strings.handle("TYPE", ["k"], store)
    end

    test "TYPE returns 'hash' for hash key" do
      store = MockStore.make()
      Hash.handle("HSET", ["h", "f", "v"], store)
      assert {:simple, "hash"} == Strings.handle("TYPE", ["h"], store)
    end

    test "TYPE returns 'list' for list key" do
      store = MockStore.make()
      List.handle("RPUSH", ["l", "elem"], store)
      assert {:simple, "list"} == Strings.handle("TYPE", ["l"], store)
    end

    test "TYPE returns 'set' for set key" do
      store = MockStore.make()
      Set.handle("SADD", ["s", "member"], store)
      assert {:simple, "set"} == Strings.handle("TYPE", ["s"], store)
    end

    test "TYPE returns 'zset' for sorted set key" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["z", "1.0", "member"], store)
      assert {:simple, "zset"} == Strings.handle("TYPE", ["z"], store)
    end

    test "TYPE with wrong arity returns error" do
      assert {:error, _} = Strings.handle("TYPE", [], MockStore.make())
    end

    test "TYPE ignores orphan list metadata without a type marker" do
      store = MockStore.make()

      :ok =
        store.compound_put.(
          "orphan",
          CompoundKey.list_meta_key("orphan"),
          :erlang.term_to_binary({1, 0, 1}),
          0
        )

      :ok = store.compound_put.("orphan", CompoundKey.list_element("orphan", 0), "value", 0)

      assert {:simple, "none"} == Strings.handle("TYPE", ["orphan"], store)
      assert nil == Json.handle("JSON.GET", ["orphan"], store)
      assert 0 == Bitmap.handle("GETBIT", ["orphan", "0"], store)
    end

    test "read paths do not use list metadata as a fallback type marker" do
      # LM:key is list payload metadata. Only T:key is a type marker, so read
      # commands must not probe LM:key as a shortcut for WRONGTYPE detection.
      checks = [
        {"lib/ferricstore/store/type_registry.ex", ~r/def get_type.*?case Ops\.get/s},
        {"lib/ferricstore/commands/json.ex", ~r/defp compound_type_marker\?.*?end/s},
        {"lib/ferricstore/commands/bitmap.ex", ~r/defp compound_type_marker\?.*?end/s},
        {"lib/ferricstore/commands/hyperloglog.ex", ~r/defp compound_type_marker\?.*?end/s}
      ]

      findings =
        checks
        |> Enum.flat_map(fn {path, pattern} ->
          source = File.read!(app_path(path))

          case Regex.run(pattern, source) do
            [body] ->
              if body =~ "list_meta_key" do
                [path]
              else
                []
              end

            nil ->
              [path <> " missing expected function body"]
          end
        end)

      assert findings == [],
             "read paths must rely on T:key only, found LM fallback probes in:\n" <>
               Enum.join(findings, "\n")
    end
  end

  defp app_path(path), do: Path.expand("../../../#{path}", __DIR__)

  # ---------------------------------------------------------------------------
  # DEL with data structures
  # ---------------------------------------------------------------------------

  describe "DEL with data structures" do
    test "DEL on hash key removes all fields" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1", "f2", "v2", "f3", "v3"], store)
      assert 1 == Strings.handle("DEL", ["hash"], store)
      # All fields should be gone
      assert 0 == Hash.handle("HLEN", ["hash"], store)
      assert {:simple, "none"} == Strings.handle("TYPE", ["hash"], store)
    end

    test "DEL on list key removes all elements" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)
      assert 1 == Strings.handle("DEL", ["mylist"], store)
      assert 0 == List.handle("LLEN", ["mylist"], store)
      assert {:simple, "none"} == Strings.handle("TYPE", ["mylist"], store)
    end

    test "DEL on set key removes all members" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b", "c"], store)
      assert 1 == Strings.handle("DEL", ["myset"], store)
      assert 0 == Set.handle("SCARD", ["myset"], store)
      assert {:simple, "none"} == Strings.handle("TYPE", ["myset"], store)
    end

    test "DEL on sorted set key removes all members" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b"], store)
      assert 1 == Strings.handle("DEL", ["zs"], store)
      assert 0 == SortedSet.handle("ZCARD", ["zs"], store)
      assert {:simple, "none"} == Strings.handle("TYPE", ["zs"], store)
    end

    test "DEL on nonexistent key returns 0" do
      store = MockStore.make()
      assert 0 == Strings.handle("DEL", ["nonexistent"], store)
    end

    test "DEL on string key still works" do
      store = MockStore.make()
      Strings.handle("SET", ["str", "value"], store)
      assert 1 == Strings.handle("DEL", ["str"], store)
      assert nil == Strings.handle("GET", ["str"], store)
    end

    test "DEL multiple keys of mixed types" do
      store = MockStore.make()
      Strings.handle("SET", ["str", "v"], store)
      Hash.handle("HSET", ["hash", "f", "v"], store)
      Set.handle("SADD", ["set", "m"], store)
      assert 3 == Strings.handle("DEL", ["str", "hash", "set"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # WRONGTYPE enforcement across types
  # ---------------------------------------------------------------------------

  describe "WRONGTYPE enforcement" do
    test "HSET on set key returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["key", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HSET", ["key", "f", "v"], store)
    end

    test "SADD on hash key returns WRONGTYPE" do
      store = MockStore.make()
      Hash.handle("HSET", ["key", "f", "v"], store)
      assert {:error, "WRONGTYPE" <> _} = Set.handle("SADD", ["key", "member"], store)
    end

    test "RPUSH on hash key returns WRONGTYPE" do
      store = MockStore.make()
      Hash.handle("HSET", ["key", "f", "v"], store)
      assert {:error, "WRONGTYPE" <> _} = List.handle("RPUSH", ["key", "elem"], store)
    end

    test "ZADD on list key returns WRONGTYPE" do
      store = MockStore.make()
      List.handle("RPUSH", ["key", "elem"], store)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZADD", ["key", "1.0", "m"], store)
    end

    test "read-only commands on wrong type also return WRONGTYPE" do
      store = MockStore.make()
      Hash.handle("HSET", ["key", "f", "v"], store)
      assert {:error, "WRONGTYPE" <> _} = Set.handle("SMEMBERS", ["key"], store)
      assert {:error, "WRONGTYPE" <> _} = List.handle("LLEN", ["key"], store)
      assert {:error, "WRONGTYPE" <> _} = SortedSet.handle("ZCARD", ["key"], store)
    end

    test "after DEL, key can be reused with different type" do
      store = MockStore.make()
      Hash.handle("HSET", ["key", "f", "v"], store)
      Strings.handle("DEL", ["key"], store)
      # Now it should work as a set
      assert 1 == Set.handle("SADD", ["key", "member"], store)
      assert {:simple, "set"} == Strings.handle("TYPE", ["key"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # Dispatcher routing for new commands
  # ---------------------------------------------------------------------------

  describe "Dispatcher routing" do
    test "dispatches HSET to Hash handler" do
      store = MockStore.make()
      assert 1 == Dispatcher.dispatch("HSET", ["h", "f", "v"], store)
    end

    test "dispatches HGET to Hash handler" do
      store = MockStore.make()
      Dispatcher.dispatch("HSET", ["h", "f", "v"], store)
      assert "v" == Dispatcher.dispatch("HGET", ["h", "f"], store)
    end

    test "dispatches RPUSH to List handler" do
      store = MockStore.make()
      assert 1 == Dispatcher.dispatch("RPUSH", ["l", "a"], store)
    end

    test "dispatches LPUSH to List handler" do
      store = MockStore.make()
      assert 1 == Dispatcher.dispatch("LPUSH", ["l", "a"], store)
    end

    test "dispatches SADD to Set handler" do
      store = MockStore.make()
      assert 1 == Dispatcher.dispatch("SADD", ["s", "a"], store)
    end

    test "dispatches SMEMBERS to Set handler" do
      store = MockStore.make()
      Dispatcher.dispatch("SADD", ["s", "a", "b"], store)
      members = Dispatcher.dispatch("SMEMBERS", ["s"], store)
      assert Enum.sort(members) == ["a", "b"]
    end

    test "dispatches ZADD to SortedSet handler" do
      store = MockStore.make()
      assert 1 == Dispatcher.dispatch("ZADD", ["z", "1.0", "a"], store)
    end

    test "dispatches ZRANGE to SortedSet handler" do
      store = MockStore.make()
      Dispatcher.dispatch("ZADD", ["z", "2.0", "b", "1.0", "a"], store)
      assert ["a", "b"] == Dispatcher.dispatch("ZRANGE", ["z", "0", "-1"], store)
    end

    test "dispatches TYPE to Strings handler" do
      store = MockStore.make()
      Dispatcher.dispatch("SET", ["k", "v"], store)
      assert {:simple, "string"} == Dispatcher.dispatch("TYPE", ["k"], store)
    end

    test "data structure commands are case insensitive" do
      store = MockStore.make()
      assert 1 == Dispatcher.dispatch("hset", ["h", "f", "v"], store)
      assert "v" == Dispatcher.dispatch("hget", ["h", "f"], store)
    end

    test "KEYS does not return internal compound keys" do
      store = MockStore.make()
      Dispatcher.dispatch("SET", ["visible", "v"], store)
      Dispatcher.dispatch("HSET", ["hash", "f", "v"], store)
      keys = Dispatcher.dispatch("KEYS", ["*"], store)
      # Only "visible" should appear — not compound keys like H:hash\0f or T:hash
      assert "visible" in keys
      refute Enum.any?(keys, &String.starts_with?(&1, "H:"))
      refute Enum.any?(keys, &String.starts_with?(&1, "T:"))
    end

    test "DBSIZE does not count internal compound keys" do
      store = MockStore.make()
      base_size = Dispatcher.dispatch("DBSIZE", [], store)
      Dispatcher.dispatch("HSET", ["hash", "f1", "v1", "f2", "v2"], store)
      # HSET creates compound keys (H:hash\0f1, H:hash\0f2, T:hash) but
      # DBSIZE should only count the logical hash key, not the internal fields.
      assert base_size + 1 == Dispatcher.dispatch("DBSIZE", [], store)
    end
  end
end
