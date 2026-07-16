defmodule Ferricstore.Commands.ServerStubsTest do
  @moduledoc """
  Tests for server stub commands: MODULE, WAITAOF, DEBUG subcommands,
  FLUSHALL, and CLIENT subcommands.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Server
  alias Ferricstore.Test.MockStore

  # ===========================================================================
  # MODULE
  # ===========================================================================

  describe "MODULE LIST" do
    test "returns empty list" do
      assert [] == Server.handle("MODULE", ["LIST"], MockStore.make())
    end

    test "subcommand is case-insensitive" do
      assert [] == Server.handle("MODULE", ["list"], MockStore.make())
    end
  end

  describe "MODULE LOAD" do
    test "returns error about unsupported modules" do
      assert {:error, msg} =
               Server.handle("MODULE", ["LOAD", "/path/to/module.so"], MockStore.make())

      assert msg =~ "does not support modules"
    end

    test "returns error with path and args" do
      assert {:error, _} = Server.handle("MODULE", ["LOAD", "/path.so", "arg1"], MockStore.make())
    end
  end

  describe "MODULE UNLOAD" do
    test "returns error about unsupported modules" do
      assert {:error, msg} = Server.handle("MODULE", ["UNLOAD", "mymodule"], MockStore.make())
      assert msg =~ "does not support modules"
    end
  end

  describe "MODULE unknown subcommand" do
    test "returns error for unknown subcommand" do
      assert {:error, msg} = Server.handle("MODULE", ["UNKNOWN"], MockStore.make())
      assert msg =~ "unknown subcommand"
    end

    test "returns error for no args" do
      assert {:error, _} = Server.handle("MODULE", [], MockStore.make())
    end
  end

  # ===========================================================================
  # WAITAOF
  # ===========================================================================

  describe "WAITAOF" do
    test "returns [0, 0] with valid arguments" do
      assert [0, 0] == Server.handle("WAITAOF", ["0", "0", "100"], MockStore.make())
    end

    test "returns [0, 0] regardless of argument values" do
      assert [0, 0] == Server.handle("WAITAOF", ["1", "2", "5000"], MockStore.make())
    end

    test "returns error with wrong number of arguments" do
      assert {:error, msg} = Server.handle("WAITAOF", ["0", "0"], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with no arguments" do
      assert {:error, _} = Server.handle("WAITAOF", [], MockStore.make())
    end

    test "returns error with too many arguments" do
      assert {:error, _} = Server.handle("WAITAOF", ["0", "0", "100", "extra"], MockStore.make())
    end
  end

  # ===========================================================================
  # DEBUG subcommands
  # ===========================================================================

  describe "DEBUG RELOAD" do
    test "returns OK" do
      assert :ok = Server.handle("DEBUG", ["RELOAD"], MockStore.make())
    end
  end

  describe "DEBUG FLUSHALL" do
    test "flushes all keys (same as FLUSHALL)" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}})
      assert :ok = Server.handle("DEBUG", ["FLUSHALL"], store)
      assert 0 == store.dbsize.()
    end

    test "accepts ASYNC flag" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert :ok = Server.handle("DEBUG", ["FLUSHALL", "ASYNC"], store)
      assert 0 == store.dbsize.()
    end

    test "rejects invalid options without flushing" do
      store = MockStore.make(%{"a" => {"1", 0}})

      assert {:error, _reason} = Server.handle("DEBUG", ["FLUSHALL", "BOGUS"], store)
      assert 1 == store.dbsize.()
    end
  end

  describe "DEBUG SET-ACTIVE-EXPIRE" do
    test "returns OK with 0" do
      assert :ok = Server.handle("DEBUG", ["SET-ACTIVE-EXPIRE", "0"], MockStore.make())
    end

    test "returns OK with 1" do
      assert :ok = Server.handle("DEBUG", ["SET-ACTIVE-EXPIRE", "1"], MockStore.make())
    end
  end

  describe "DEBUG CHANGE-REPL-ID" do
    test "returns OK" do
      assert :ok = Server.handle("DEBUG", ["CHANGE-REPL-ID"], MockStore.make())
    end
  end

  describe "DEBUG QUICKLIST-PACKED-THRESHOLD" do
    test "returns OK" do
      assert :ok =
               Server.handle("DEBUG", ["QUICKLIST-PACKED-THRESHOLD", "1024"], MockStore.make())
    end

    test "returns OK with no value arg" do
      assert :ok = Server.handle("DEBUG", ["QUICKLIST-PACKED-THRESHOLD"], MockStore.make())
    end
  end

  describe "DEBUG AOFSTAT" do
    test "returns empty map" do
      assert %{} == Server.handle("DEBUG", ["AOFSTAT"], MockStore.make())
    end
  end

  describe "DEBUG SFLAGS" do
    test "returns empty map" do
      assert %{} == Server.handle("DEBUG", ["SFLAGS"], MockStore.make())
    end
  end

  describe "DEBUG unknown subcommand" do
    test "returns error for unknown subcommand" do
      assert {:error, msg} = Server.handle("DEBUG", ["UNKNOWN"], MockStore.make())
      assert msg =~ "unknown subcommand"
    end

    test "returns error with no args" do
      assert {:error, _} = Server.handle("DEBUG", [], MockStore.make())
    end
  end

  # ===========================================================================
  # FLUSHALL
  # ===========================================================================

  describe "FLUSHALL" do
    test "clears all keys" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}})
      assert :ok = Server.handle("FLUSHALL", [], store)
      assert 0 == store.dbsize.()
    end

    test "with ASYNC flag works" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert :ok = Server.handle("FLUSHALL", ["ASYNC"], store)
      assert 0 == store.dbsize.()
    end

    test "with SYNC flag works" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert :ok = Server.handle("FLUSHALL", ["SYNC"], store)
      assert 0 == store.dbsize.()
    end

    test "with invalid flag returns error" do
      assert {:error, _} = Server.handle("FLUSHALL", ["INVALID"], MockStore.make())
    end
  end

  # CLIENT subcommands moved to apps/ferricstore_server/test/ferricstore_server/commands/client_test.exs
end
