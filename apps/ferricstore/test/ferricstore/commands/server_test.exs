defmodule Ferricstore.Commands.ServerTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Server
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # PING
  # ---------------------------------------------------------------------------

  describe "PING" do
    test "PING with no args returns {:simple, \"PONG\"}" do
      assert {:simple, "PONG"} == Server.handle("PING", [], MockStore.make())
    end

    test "PING with message returns bulk string echo" do
      assert "hello" == Server.handle("PING", ["hello"], MockStore.make())
    end

    test "PING with too many args returns error" do
      assert {:error, _} = Server.handle("PING", ["a", "b"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # ECHO
  # ---------------------------------------------------------------------------

  describe "ECHO" do
    test "ECHO message returns the message" do
      assert "hello world" == Server.handle("ECHO", ["hello world"], MockStore.make())
    end

    test "ECHO no args returns error" do
      assert {:error, _} = Server.handle("ECHO", [], MockStore.make())
    end

    test "ECHO too many args returns error" do
      assert {:error, _} = Server.handle("ECHO", ["a", "b"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # DBSIZE
  # ---------------------------------------------------------------------------

  describe "DBSIZE" do
    test "DBSIZE returns count of all keys" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}, "c" => {"3", 0}})
      assert 3 == Server.handle("DBSIZE", [], store)
    end

    test "DBSIZE returns 0 for empty store" do
      assert 0 == Server.handle("DBSIZE", [], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # KEYS
  # ---------------------------------------------------------------------------

  describe "KEYS" do
    test "KEYS * returns all keys" do
      store = MockStore.make(%{"hello" => {"v", 0}, "world" => {"v", 0}})
      result = Server.handle("KEYS", ["*"], store)
      assert Enum.sort(result) == ["hello", "world"]
    end

    test "KEYS with prefix* returns matching keys" do
      store = MockStore.make(%{"pre_a" => {"v", 0}, "pre_b" => {"v", 0}, "other" => {"v", 0}})
      result = Server.handle("KEYS", ["pre_*"], store)
      assert Enum.sort(result) == ["pre_a", "pre_b"]
    end

    test "KEYS with ? matches single character" do
      store = MockStore.make(%{"a" => {"v", 0}, "ab" => {"v", 0}, "b" => {"v", 0}})
      result = Server.handle("KEYS", ["?"], store)
      assert Enum.sort(result) == ["a", "b"]
    end

    test "KEYS with exact match returns single key" do
      store = MockStore.make(%{"hello" => {"v", 0}, "world" => {"v", 0}})
      result = Server.handle("KEYS", ["hello"], store)
      assert result == ["hello"]
    end

    test "KEYS no args returns error" do
      assert {:error, _} = Server.handle("KEYS", [], MockStore.make())
    end

    test "KEYS too many args returns error" do
      assert {:error, _} = Server.handle("KEYS", ["*", "extra"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # FLUSHDB
  # ---------------------------------------------------------------------------

  describe "FLUSHDB" do
    test "FLUSHDB clears all keys" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}})
      assert :ok = Server.handle("FLUSHDB", [], store)
      assert 0 == store.dbsize.()
    end

    test "FLUSHDB with ASYNC flag works" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert :ok = Server.handle("FLUSHDB", ["ASYNC"], store)
      assert 0 == store.dbsize.()
    end

    test "FLUSHDB with SYNC flag works" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert :ok = Server.handle("FLUSHDB", ["SYNC"], store)
      assert 0 == store.dbsize.()
    end

    test "FLUSHDB mode is case-insensitive" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert :ok = Server.handle("FLUSHDB", ["async"], store)
      assert 0 == store.dbsize.()
    end

    test "FLUSHDB then KEYS * returns empty list" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}})
      assert :ok = Server.handle("FLUSHDB", [], store)
      assert [] == Server.handle("KEYS", ["*"], store)
    end

    test "FLUSHDB then DBSIZE returns 0" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}})
      assert :ok = Server.handle("FLUSHDB", [], store)
      assert 0 == Server.handle("DBSIZE", [], store)
    end

    test "FLUSHDB is idempotent (double flush)" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert :ok = Server.handle("FLUSHDB", [], store)
      assert :ok = Server.handle("FLUSHDB", [], store)
      assert 0 == store.dbsize.()
    end

    test "FLUSHDB with invalid flag returns error" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert {:error, _} = Server.handle("FLUSHDB", ["INVALID"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # KEYS — additional edge cases
  # ---------------------------------------------------------------------------

  describe "KEYS edge cases" do
    test "KEYS with [abc] character class matches any of a, b, or c" do
      store =
        MockStore.make(%{
          "a" => {"v", 0},
          "b" => {"v", 0},
          "c" => {"v", 0},
          "d" => {"v", 0}
        })

      # The GlobMatcher supports [abc] character classes, matching Redis
      # behaviour where [abc] matches any single char in the set.
      result = Server.handle("KEYS", ["[abc]"], store) |> Enum.sort()
      assert result == ["a", "b", "c"]
    end

    test "KEYS with dot in pattern matches literal dot" do
      store =
        MockStore.make(%{
          "hello.world" => {"v", 0},
          "helloxworld" => {"v", 0}
        })

      # In the glob-to-regex, '.' is escaped by Regex.escape, so it matches
      # only a literal dot, not any character.
      result = Server.handle("KEYS", ["hello.world"], store)
      assert result == ["hello.world"]
    end

    test "KEYS with no matching pattern returns empty list" do
      store = MockStore.make(%{"hello" => {"v", 0}, "world" => {"v", 0}})
      assert [] == Server.handle("KEYS", ["zzz*"], store)
    end

    test "KEYS returns empty list for empty store" do
      store = MockStore.make()
      assert [] == Server.handle("KEYS", ["*"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # DBSIZE — additional edge cases
  # ---------------------------------------------------------------------------

  describe "DBSIZE edge cases" do
    test "DBSIZE counts each unique key once (overwrites do not inflate count)" do
      store = MockStore.make()
      # Put the same key 5 times
      for _ <- 1..5, do: store.put.("k", "v", 0)
      assert 1 == Server.handle("DBSIZE", [], store)
    end

    test "DBSIZE excludes expired keys" do
      past = System.os_time(:millisecond) - 1000
      future = System.os_time(:millisecond) + 60_000

      store =
        MockStore.make(%{
          "live1" => {"v", 0},
          "live2" => {"v", future},
          "live3" => {"v", 0},
          "exp1" => {"v", past},
          "exp2" => {"v", past}
        })

      assert 3 == Server.handle("DBSIZE", [], store)
    end

    test "DBSIZE with args returns error" do
      assert {:error, _} = Server.handle("DBSIZE", ["extra"], MockStore.make())
    end
  end
end
