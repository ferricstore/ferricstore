defmodule Ferricstore.Commands.CommandsEdgeCasesServerTest do
  @moduledoc """
  Edge cases for server commands (PING, ECHO, KEYS, DBSIZE, FLUSHDB) and Dispatcher.
  Split from CommandsEdgeCasesTest.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Dispatcher, Server}
  alias Ferricstore.Test.MockStore

  # ===========================================================================
  # Server — PING edge cases
  # ===========================================================================

  describe "PING edge cases (not covered elsewhere)" do
    test "PING with empty string arg echoes empty string" do
      assert "" == Server.handle("PING", [""], MockStore.make())
    end

    test "PING with 3 args returns error" do
      assert {:error, _} = Server.handle("PING", ["a", "b", "c"], MockStore.make())
    end
  end

  # ===========================================================================
  # Server — ECHO edge cases
  # ===========================================================================

  describe "ECHO edge cases (not covered elsewhere)" do
    test "ECHO with empty string returns empty string" do
      assert "" == Server.handle("ECHO", [""], MockStore.make())
    end

    test "ECHO with 3 args returns error" do
      assert {:error, _} = Server.handle("ECHO", ["a", "b", "c"], MockStore.make())
    end

    test "ECHO preserves binary content" do
      msg = <<0, 1, 2, 255>>
      assert ^msg = Server.handle("ECHO", [msg], MockStore.make())
    end
  end

  # ===========================================================================
  # Server — KEYS edge cases
  # ===========================================================================

  describe "KEYS edge cases (not covered elsewhere)" do
    test "KEYS '*suf' matches suffix" do
      store =
        MockStore.make(%{
          "hello_suf" => {"v", 0},
          "world_suf" => {"v", 0},
          "no_match" => {"v", 0}
        })

      result = Server.handle("KEYS", ["*suf"], store)
      assert Enum.sort(result) == ["hello_suf", "world_suf"]
    end

    test "KEYS '?' does not match empty string or multi-char keys" do
      store =
        MockStore.make(%{
          "" => {"v", 0},
          "a" => {"v", 0},
          "ab" => {"v", 0},
          "abc" => {"v", 0}
        })

      result = Server.handle("KEYS", ["?"], store)
      assert result == ["a"]
    end

    test "KEYS '??' matches exactly two-char keys" do
      store =
        MockStore.make(%{
          "a" => {"v", 0},
          "ab" => {"v", 0},
          "abc" => {"v", 0}
        })

      result = Server.handle("KEYS", ["??"], store)
      assert result == ["ab"]
    end

    test "KEYS 'exact' matches only that exact key" do
      store =
        MockStore.make(%{
          "exact" => {"v", 0},
          "exactly" => {"v", 0},
          "inexact" => {"v", 0}
        })

      result = Server.handle("KEYS", ["exact"], store)
      assert result == ["exact"]
    end

    test "KEYS '*' on store with special-char keys returns them" do
      store =
        MockStore.make(%{
          "key.with.dots" => {"v", 0},
          "key:with:colons" => {"v", 0}
        })

      result = Server.handle("KEYS", ["*"], store)
      assert length(result) == 2
    end

    test "KEYS 'pre*suf' matches keys with prefix and suffix" do
      store =
        MockStore.make(%{
          "pre_middle_suf" => {"v", 0},
          "pre_suf" => {"v", 0},
          "pre_no" => {"v", 0},
          "no_suf" => {"v", 0}
        })

      result = Server.handle("KEYS", ["pre*suf"], store)
      assert Enum.sort(result) == ["pre_middle_suf", "pre_suf"]
    end

    test "KEYS does not return expired keys" do
      past = System.os_time(:millisecond) - 1_000

      store =
        MockStore.make(%{
          "live" => {"v", 0},
          "dead" => {"v", past}
        })

      result = Server.handle("KEYS", ["*"], store)
      assert result == ["live"]
    end
  end

  # ===========================================================================
  # Server — DBSIZE edge cases
  # ===========================================================================

  describe "DBSIZE edge cases (not covered elsewhere)" do
    test "DBSIZE with 2 args returns error" do
      assert {:error, _} = Server.handle("DBSIZE", ["a", "b"], MockStore.make())
    end
  end

  # ===========================================================================
  # Server — FLUSHDB edge cases
  # ===========================================================================

  describe "FLUSHDB edge cases (not covered elsewhere)" do
    test "FLUSHDB accepts lowercase flags case-insensitively" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert :ok = Server.handle("FLUSHDB", ["async"], store)
    end

    test "FLUSHDB with multiple flags returns error" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert {:error, _} = Server.handle("FLUSHDB", ["ASYNC", "SYNC"], store)
    end

    test "FLUSHDB on empty store returns :ok" do
      store = MockStore.make()
      assert :ok = Server.handle("FLUSHDB", [], store)
    end
  end

  # ===========================================================================
  # Dispatcher — case normalisation
  # ===========================================================================

  describe "Dispatcher case normalisation" do
    test "dispatches fully lowercase command" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert "v" == Dispatcher.dispatch("get", ["k"], store)
    end

    test "dispatches mixed case 'gEt'" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert "v" == Dispatcher.dispatch("gEt", ["k"], store)
    end

    test "dispatches 'pInG' mixed case" do
      assert {:simple, "PONG"} == Dispatcher.dispatch("pInG", [], MockStore.make())
    end

    test "dispatches 'expire' lowercase" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Dispatcher.dispatch("expire", ["k", "10"], store)
    end

    test "dispatches 'Ttl' mixed case" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert -1 == Dispatcher.dispatch("Ttl", ["k"], store)
    end

    test "dispatches 'dBsIzE' mixed case" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert 1 == Dispatcher.dispatch("dBsIzE", [], store)
    end
  end

  # ===========================================================================
  # Dispatcher — unknown commands
  # ===========================================================================

  describe "Dispatcher unknown commands" do
    test "completely unknown command returns error with command name" do
      assert {:error, msg} = Dispatcher.dispatch("FOOBAR", ["x"], MockStore.make())
      assert msg =~ "unknown command"
      assert msg =~ "foobar"
    end

    test "empty string command returns error" do
      assert {:error, msg} = Dispatcher.dispatch("", [], MockStore.make())
      assert msg =~ "unknown command"
    end

    test "command with spaces returns error" do
      assert {:error, _} = Dispatcher.dispatch("GET SET", ["k"], MockStore.make())
    end

    test "numeric command name returns error" do
      assert {:error, _} = Dispatcher.dispatch("12345", [], MockStore.make())
    end
  end

  # ===========================================================================
  # Dispatcher — error propagation
  # ===========================================================================

  describe "Dispatcher error propagation" do
    test "propagates argument errors from handler through dispatcher" do
      assert {:error, msg} = Dispatcher.dispatch("GET", [], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "propagates SET option errors through dispatcher" do
      assert {:error, _} =
               Dispatcher.dispatch("SET", ["k", "v", "EX", "0"], MockStore.make())
    end

    test "propagates EXPIRE parse errors through dispatcher" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert {:error, _} = Dispatcher.dispatch("EXPIRE", ["k", "abc"], store)
    end
  end

  # ===========================================================================
  # Dispatcher — FLUSHALL routing
  # ===========================================================================

  describe "Dispatcher FLUSHALL routing" do
    test "FLUSHALL is routed to Server handler" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert :ok = Dispatcher.dispatch("FLUSHALL", [], store)
    end
  end

  # ===========================================================================
  # Cross-command interactions via dispatcher
  # ===========================================================================

  describe "cross-command interactions" do
    test "SET then GET returns the set value" do
      store = MockStore.make()
      assert :ok = Dispatcher.dispatch("SET", ["k", "hello"], store)
      assert "hello" == Dispatcher.dispatch("GET", ["k"], store)
    end

    test "SET then DEL then GET returns nil" do
      store = MockStore.make()
      assert :ok = Dispatcher.dispatch("SET", ["k", "v"], store)
      assert 1 == Dispatcher.dispatch("DEL", ["k"], store)
      assert nil == Dispatcher.dispatch("GET", ["k"], store)
    end

    test "MSET then MGET returns all values" do
      store = MockStore.make()
      assert :ok = Dispatcher.dispatch("MSET", ["a", "1", "b", "2", "c", "3"], store)
      assert ["1", "2", "3"] == Dispatcher.dispatch("MGET", ["a", "b", "c"], store)
    end

    test "SET with EX then TTL returns positive seconds" do
      store = MockStore.make()
      assert :ok = Dispatcher.dispatch("SET", ["k", "v", "EX", "60"], store)
      ttl = Dispatcher.dispatch("TTL", ["k"], store)
      assert ttl > 0
      assert ttl <= 60
    end

    test "SET then EXPIRE then PERSIST removes expiry" do
      store = MockStore.make()
      assert :ok = Dispatcher.dispatch("SET", ["k", "v"], store)
      assert 1 == Dispatcher.dispatch("EXPIRE", ["k", "30"], store)
      ttl = Dispatcher.dispatch("TTL", ["k"], store)
      assert ttl > 0
      assert 1 == Dispatcher.dispatch("PERSIST", ["k"], store)
      assert -1 == Dispatcher.dispatch("TTL", ["k"], store)
    end

    test "FLUSHDB then EXISTS returns 0" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}})
      assert :ok = Dispatcher.dispatch("FLUSHDB", [], store)
      assert 0 == Dispatcher.dispatch("EXISTS", ["a", "b"], store)
    end

    test "FLUSHDB then DBSIZE returns 0" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert :ok = Dispatcher.dispatch("FLUSHDB", [], store)
      assert 0 == Dispatcher.dispatch("DBSIZE", [], store)
    end
  end
end
