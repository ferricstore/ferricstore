defmodule Ferricstore.Commands.CommandTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Server
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # COMMAND (no subcommand)
  # ---------------------------------------------------------------------------

  describe "COMMAND" do
    test "returns list of command info tuples" do
      result = Server.handle("COMMAND", [], MockStore.make())

      assert is_list(result)
      assert result != []

      # Each entry should be a list of 6 elements: [name, arity, flags, first_key, last_key, step]
      Enum.each(result, fn entry ->
        assert is_list(entry)
        assert length(entry) == 6
        [name, arity, flags, first_key, last_key, step] = entry
        assert is_binary(name)
        assert is_integer(arity)
        assert is_list(flags)
        assert is_integer(first_key)
        assert is_integer(last_key)
        assert is_integer(step)
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # COMMAND COUNT
  # ---------------------------------------------------------------------------

  describe "COMMAND COUNT" do
    test "returns a positive integer" do
      result = Server.handle("COMMAND", ["COUNT"], MockStore.make())

      assert is_integer(result)
      assert result > 0
    end

    test "subcommand is case-insensitive" do
      assert Server.handle("COMMAND", ["count"], MockStore.make()) ==
               Server.handle("COMMAND", ["COUNT"], MockStore.make())
    end

    test "count matches number of entries from COMMAND" do
      all = Server.handle("COMMAND", [], MockStore.make())
      count = Server.handle("COMMAND", ["COUNT"], MockStore.make())

      assert count == length(all)
    end
  end

  # ---------------------------------------------------------------------------
  # COMMAND LIST
  # ---------------------------------------------------------------------------

  describe "COMMAND LIST" do
    test "returns a list of strings" do
      result = Server.handle("COMMAND", ["LIST"], MockStore.make())

      assert is_list(result)
      assert result != []
      assert Enum.all?(result, &is_binary/1)
    end

    test "includes known commands" do
      result = Server.handle("COMMAND", ["LIST"], MockStore.make())

      assert "get" in result
      assert "set" in result
      assert "ping" in result
      assert "info" in result
      assert "client" in result
      assert "command" in result
    end

    test "LIST subcommand is case-insensitive" do
      assert Server.handle("COMMAND", ["list"], MockStore.make()) ==
               Server.handle("COMMAND", ["LIST"], MockStore.make())
    end

    test "all names are lowercase" do
      result = Server.handle("COMMAND", ["LIST"], MockStore.make())

      Enum.each(result, fn name ->
        assert name == String.downcase(name),
               "Expected lowercase command name, got: #{inspect(name)}"
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # COMMAND INFO
  # ---------------------------------------------------------------------------

  describe "COMMAND INFO" do
    test "returns info for a known command" do
      result = Server.handle("COMMAND", ["INFO", "get"], MockStore.make())

      assert is_list(result)
      assert length(result) == 1

      [entry] = result
      assert is_list(entry)
      [name, arity, flags, first_key, _last_key, _step] = entry
      assert name == "get"
      assert arity == 2
      assert is_list(flags)
      assert first_key == 1
    end

    test "returns info for multiple commands" do
      result = Server.handle("COMMAND", ["INFO", "get", "set", "ping"], MockStore.make())

      assert is_list(result)
      assert length(result) == 3

      names = Enum.map(result, fn [name | _] -> name end)
      assert "get" in names
      assert "set" in names
      assert "ping" in names
    end

    test "returns nil for unknown command" do
      result = Server.handle("COMMAND", ["INFO", "nonexistent"], MockStore.make())

      assert is_list(result)
      assert [nil] == result
    end

    test "mixed known and unknown commands" do
      result = Server.handle("COMMAND", ["INFO", "get", "bogus"], MockStore.make())

      assert length(result) == 2
      [get_info, bogus_info] = result
      assert is_list(get_info)
      assert bogus_info == nil
    end

    test "with no command names returns error" do
      assert {:error, _} = Server.handle("COMMAND", ["INFO"], MockStore.make())
    end

    test "is case-insensitive" do
      result = Server.handle("COMMAND", ["INFO", "GET"], MockStore.make())
      assert [[_name | _rest]] = result
    end
  end

  # ---------------------------------------------------------------------------
  # COMMAND DOCS
  # ---------------------------------------------------------------------------

  describe "COMMAND DOCS" do
    test "returns name and summary for a known command" do
      result = Server.handle("COMMAND", ["DOCS", "get"], MockStore.make())

      assert is_list(result)
      # Format: [name, [summary]]
      assert "get" in result
    end

    test "returns empty list for unknown command" do
      result = Server.handle("COMMAND", ["DOCS", "nonexistent"], MockStore.make())
      assert result == []
    end

    test "with no command name returns error" do
      assert {:error, _} = Server.handle("COMMAND", ["DOCS"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # COMMAND GETKEYS
  # ---------------------------------------------------------------------------

  describe "COMMAND GETKEYS" do
    test "returns keys for GET command" do
      result = Server.handle("COMMAND", ["GETKEYS", "GET", "mykey"], MockStore.make())
      assert result == ["mykey"]
    end

    test "returns keys for SET command" do
      result = Server.handle("COMMAND", ["GETKEYS", "SET", "mykey", "myval"], MockStore.make())
      assert result == ["mykey"]
    end

    test "returns multiple keys for DEL command" do
      result = Server.handle("COMMAND", ["GETKEYS", "DEL", "k1", "k2", "k3"], MockStore.make())
      assert result == ["k1", "k2", "k3"]
    end

    test "returns keys at step=2 for MSET command" do
      result =
        Server.handle("COMMAND", ["GETKEYS", "MSET", "k1", "v1", "k2", "v2"], MockStore.make())

      assert result == ["k1", "k2"]
    end

    test "returns empty list for PING (no keys)" do
      result = Server.handle("COMMAND", ["GETKEYS", "PING"], MockStore.make())
      assert result == []
    end

    test "returns error for unknown command" do
      assert {:error, _} =
               Server.handle("COMMAND", ["GETKEYS", "NONEXISTENT", "arg"], MockStore.make())
    end

    test "with no command name returns error" do
      assert {:error, _} = Server.handle("COMMAND", ["GETKEYS"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # SELECT
  # ---------------------------------------------------------------------------

  describe "SELECT" do
    test "returns error about not being supported" do
      result = Server.handle("SELECT", ["0"], MockStore.make())
      assert {:error, msg} = result
      assert msg =~ "SELECT not supported"
    end

    test "returns error for any database number" do
      assert {:error, _} = Server.handle("SELECT", ["1"], MockStore.make())
      assert {:error, _} = Server.handle("SELECT", ["15"], MockStore.make())
    end

    test "with no args returns wrong number of arguments error" do
      assert {:error, msg} = Server.handle("SELECT", [], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end
  end

  # ---------------------------------------------------------------------------
  # LOLWUT
  # ---------------------------------------------------------------------------

  describe "LOLWUT" do
    test "returns ASCII art string" do
      result = Server.handle("LOLWUT", [], MockStore.make())

      assert is_binary(result)
      assert result =~ "v0.1.0"
      # Contains multi-line ASCII art (figlet-style banner)
      assert result =~ "___|"
    end

    test "with VERSION 1 returns art" do
      result = Server.handle("LOLWUT", ["VERSION", "1"], MockStore.make())
      assert is_binary(result)
      assert result =~ "v0.1.0"
    end

    test "VERSION option is case-insensitive" do
      result = Server.handle("LOLWUT", ["version", "1"], MockStore.make())
      assert is_binary(result)
      assert result =~ "v0.1.0"
    end

    test "with VERSION 99 returns art (all versions show same art)" do
      result = Server.handle("LOLWUT", ["VERSION", "99"], MockStore.make())
      assert is_binary(result)
      assert result =~ "v0.1.0"
    end
  end

  # ---------------------------------------------------------------------------
  # DEBUG SLEEP
  # ---------------------------------------------------------------------------

  describe "DEBUG SLEEP" do
    test "DEBUG SLEEP 0 returns immediately" do
      start = System.monotonic_time(:millisecond)
      result = Server.handle("DEBUG", ["SLEEP", "0"], MockStore.make())
      elapsed = System.monotonic_time(:millisecond) - start

      assert result == :ok
      assert elapsed < 500
    end

    test "DEBUG subcommand is case-insensitive" do
      assert :ok == Server.handle("DEBUG", ["sleep", "0"], MockStore.make())
    end

    test "DEBUG SLEEP with invalid number returns error" do
      assert {:error, _} = Server.handle("DEBUG", ["SLEEP", "abc"], MockStore.make())
    end

    test "DEBUG with no subcommand returns error" do
      assert {:error, _} = Server.handle("DEBUG", [], MockStore.make())
    end

    test "DEBUG with unknown subcommand returns error" do
      assert {:error, msg} = Server.handle("DEBUG", ["BOGUS"], MockStore.make())
      assert msg =~ "unknown subcommand"
    end
  end

  # ---------------------------------------------------------------------------
  # FLUSHALL
  # ---------------------------------------------------------------------------

  describe "FLUSHALL" do
    test "clears all keys" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}})
      assert :ok = Server.handle("FLUSHALL", [], store)
      assert 0 == store.dbsize.()
    end

    test "with ASYNC flag works" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert :ok = Server.handle("FLUSHALL", ["ASYNC"], store)
    end

    test "with SYNC flag works" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert :ok = Server.handle("FLUSHALL", ["SYNC"], store)
    end

    test "with invalid flag returns error" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert {:error, _} = Server.handle("FLUSHALL", ["INVALID"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # COMMAND unknown subcommand
  # ---------------------------------------------------------------------------

  describe "COMMAND unknown subcommand" do
    test "returns error with subcommand name" do
      result = Server.handle("COMMAND", ["BOGUS"], MockStore.make())
      assert {:error, msg} = result
      assert msg =~ "unknown subcommand"
      assert msg =~ "BOGUS"
    end
  end
end
