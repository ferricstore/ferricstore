defmodule Ferricstore.Commands.CatalogTest do
  @moduledoc """
  Unit tests for the Ferricstore.Commands.Catalog module.

  Tests the catalog registry of all commands, verifying that every entry
  has valid metadata, the lookup/count/names functions work correctly, and
  that key extraction via `get_keys/2` returns the correct arguments for
  various command shapes (single-key, multi-key, variadic, no-key, step > 1).
  """

  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Catalog

  # ---------------------------------------------------------------------------
  # all/0 — structural validation of every entry
  # ---------------------------------------------------------------------------

  describe "all/0" do
    test "returns a non-empty list" do
      commands = Catalog.all()
      assert is_list(commands)
      refute Enum.empty?(commands)
    end

    test "every entry has a non-empty name" do
      for cmd <- Catalog.all() do
        assert is_binary(cmd.name), "Expected binary name, got: #{inspect(cmd.name)}"
        assert byte_size(cmd.name) > 0, "Command name must not be empty"
      end
    end

    test "every entry has a valid arity (integer)" do
      for cmd <- Catalog.all() do
        assert is_integer(cmd.arity),
               "Expected integer arity for #{cmd.name}, got: #{inspect(cmd.arity)}"
      end
    end

    test "every entry has a non-empty summary" do
      for cmd <- Catalog.all() do
        assert is_binary(cmd.summary),
               "Expected binary summary for #{cmd.name}, got: #{inspect(cmd.summary)}"
        assert byte_size(cmd.summary) > 0,
               "Summary for #{cmd.name} must not be empty"
      end
    end

    test "every entry has flags as a list of strings" do
      for cmd <- Catalog.all() do
        assert is_list(cmd.flags),
               "Expected list of flags for #{cmd.name}, got: #{inspect(cmd.flags)}"
        for flag <- cmd.flags do
          assert is_binary(flag),
                 "Expected string flag for #{cmd.name}, got: #{inspect(flag)}"
        end
      end
    end

    test "every entry has integer first_key, last_key, step" do
      for cmd <- Catalog.all() do
        assert is_integer(cmd.first_key),
               "Expected integer first_key for #{cmd.name}"
        assert is_integer(cmd.last_key),
               "Expected integer last_key for #{cmd.name}"
        assert is_integer(cmd.step),
               "Expected integer step for #{cmd.name}"
      end
    end

    test "no-key commands have first_key == 0, last_key == 0, step == 0" do
      for cmd <- Catalog.all(), cmd.first_key == 0 do
        assert cmd.last_key == 0,
               "#{cmd.name} has first_key=0 but last_key=#{cmd.last_key}"
        assert cmd.step == 0,
               "#{cmd.name} has first_key=0 but step=#{cmd.step}"
      end
    end

    test "all names are lowercase" do
      for cmd <- Catalog.all() do
        assert cmd.name == String.downcase(cmd.name),
               "Expected lowercase name, got: #{inspect(cmd.name)}"
      end
    end

    test "no duplicate names" do
      names = Enum.map(Catalog.all(), & &1.name)
      assert length(names) == length(Enum.uniq(names)),
             "Duplicate command names detected"
    end
  end

  # ---------------------------------------------------------------------------
  # count/0
  # ---------------------------------------------------------------------------

  describe "count/0" do
    test "returns a positive integer" do
      count = Catalog.count()
      assert is_integer(count)
      assert count > 0
    end

    test "matches the length of all/0" do
      assert Catalog.count() == length(Catalog.all())
    end
  end

  # ---------------------------------------------------------------------------
  # names/0
  # ---------------------------------------------------------------------------

  describe "names/0" do
    test "returns a list of strings" do
      names = Catalog.names()
      assert is_list(names)
      assert Enum.all?(names, &is_binary/1)
    end

    test "length matches count/0" do
      assert length(Catalog.names()) == Catalog.count()
    end

    test "includes major commands" do
      names = Catalog.names()
      expected = ["get", "set", "del", "exists", "mget", "mset",
                  "ping", "echo", "info", "keys", "dbsize",
                  "expire", "ttl", "persist", "command", "client",
                  "hello", "quit", "reset", "flushdb", "flushall"]

      for cmd <- expected do
        assert cmd in names,
               "Expected #{cmd} to be in Catalog.names(), but it was not found"
      end
    end

    test "all names are lowercase" do
      for name <- Catalog.names() do
        assert name == String.downcase(name)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # lookup/1
  # ---------------------------------------------------------------------------

  describe "lookup/1" do
    test "finds a known command by lowercase name" do
      assert {:ok, cmd} = Catalog.lookup("get")
      assert cmd.name == "get"
      assert cmd.arity == 2
    end

    test "is case-insensitive" do
      assert {:ok, cmd_lower} = Catalog.lookup("get")
      assert {:ok, cmd_upper} = Catalog.lookup("GET")
      assert {:ok, cmd_mixed} = Catalog.lookup("Get")

      assert cmd_lower.name == cmd_upper.name
      assert cmd_lower.name == cmd_mixed.name
    end

    test "returns :error for unknown command" do
      assert :error = Catalog.lookup("nonexistent")
    end

    test "returns :error for empty string" do
      assert :error = Catalog.lookup("")
    end

    test "returns correct metadata for SET" do
      assert {:ok, cmd} = Catalog.lookup("set")
      assert cmd.name == "set"
      assert cmd.arity == -3
      assert cmd.first_key == 1
      assert cmd.last_key == 1
      assert cmd.step == 1
      assert "write" in cmd.flags
    end

    test "returns correct metadata for PING" do
      assert {:ok, cmd} = Catalog.lookup("ping")
      assert cmd.name == "ping"
      assert cmd.first_key == 0
      assert cmd.last_key == 0
      assert cmd.step == 0
    end

    test "returns correct metadata for DEL (variadic multi-key)" do
      assert {:ok, cmd} = Catalog.lookup("del")
      assert cmd.arity == -2
      assert cmd.first_key == 1
      assert cmd.last_key == -1
      assert cmd.step == 1
    end

    test "returns correct metadata for MSET (step == 2)" do
      assert {:ok, cmd} = Catalog.lookup("mset")
      assert cmd.arity == -3
      assert cmd.first_key == 1
      assert cmd.last_key == -1
      assert cmd.step == 2
    end
  end

  # ---------------------------------------------------------------------------
  # info_tuple/1
  # ---------------------------------------------------------------------------

  describe "info_tuple/1" do
    test "returns a 6-element list for a command entry" do
      {:ok, cmd} = Catalog.lookup("get")
      tuple = Catalog.info_tuple(cmd)

      assert is_list(tuple)
      assert length(tuple) == 6
    end

    test "elements are in correct order: name, arity, flags, first_key, last_key, step" do
      {:ok, cmd} = Catalog.lookup("get")
      [name, arity, flags, first_key, last_key, step] = Catalog.info_tuple(cmd)

      assert name == "get"
      assert arity == 2
      assert is_list(flags)
      assert first_key == 1
      assert last_key == 1
      assert step == 1
    end

    test "works for every registered command" do
      for cmd <- Catalog.all() do
        tuple = Catalog.info_tuple(cmd)
        assert length(tuple) == 6,
               "info_tuple for #{cmd.name} should have 6 elements"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # get_keys/2
  # ---------------------------------------------------------------------------

  describe "get_keys/2" do
    test "returns single key for GET" do
      assert {:ok, ["mykey"]} = Catalog.get_keys("GET", ["mykey"])
    end

    test "returns single key for SET" do
      assert {:ok, ["mykey"]} = Catalog.get_keys("SET", ["mykey", "myvalue"])
    end

    test "returns single key for SET with options" do
      assert {:ok, ["mykey"]} = Catalog.get_keys("SET", ["mykey", "myvalue", "EX", "60"])
    end

    test "returns multiple keys for DEL" do
      assert {:ok, ["k1", "k2", "k3"]} = Catalog.get_keys("DEL", ["k1", "k2", "k3"])
    end

    test "returns multiple keys for EXISTS" do
      assert {:ok, ["k1", "k2"]} = Catalog.get_keys("EXISTS", ["k1", "k2"])
    end

    test "returns keys at step=2 for MSET (keys interleaved with values)" do
      assert {:ok, ["k1", "k2"]} = Catalog.get_keys("MSET", ["k1", "v1", "k2", "v2"])
    end

    test "returns keys at step=2 for MSET with three pairs" do
      assert {:ok, ["k1", "k2", "k3"]} =
               Catalog.get_keys("MSET", ["k1", "v1", "k2", "v2", "k3", "v3"])
    end

    test "returns keys for MGET (variadic, all args are keys)" do
      assert {:ok, ["k1", "k2", "k3"]} = Catalog.get_keys("MGET", ["k1", "k2", "k3"])
    end

    test "returns empty list for PING (no key command)" do
      assert {:ok, []} = Catalog.get_keys("PING", [])
    end

    test "returns empty list for PING with argument" do
      assert {:ok, []} = Catalog.get_keys("PING", ["hello"])
    end

    test "returns empty list for ECHO (no key command)" do
      assert {:ok, []} = Catalog.get_keys("ECHO", ["hello"])
    end

    test "returns empty list for DBSIZE" do
      assert {:ok, []} = Catalog.get_keys("DBSIZE", [])
    end

    test "returns empty list for INFO" do
      assert {:ok, []} = Catalog.get_keys("INFO", [])
    end

    test "returns error for unknown command" do
      assert {:error, msg} = Catalog.get_keys("NONEXISTENT", ["arg"])
      assert msg =~ "Invalid command"
    end

    test "is case-insensitive" do
      assert {:ok, keys_upper} = Catalog.get_keys("GET", ["mykey"])
      assert {:ok, keys_lower} = Catalog.get_keys("get", ["mykey"])
      assert keys_upper == keys_lower
    end

    test "returns single key for single-key expiry commands" do
      assert {:ok, ["k"]} = Catalog.get_keys("EXPIRE", ["k", "60"])
      assert {:ok, ["k"]} = Catalog.get_keys("TTL", ["k"])
      assert {:ok, ["k"]} = Catalog.get_keys("PERSIST", ["k"])
      assert {:ok, ["k"]} = Catalog.get_keys("PTTL", ["k"])
      assert {:ok, ["k"]} = Catalog.get_keys("PEXPIRE", ["k", "1000"])
    end

    test "returns empty list for server commands" do
      assert {:ok, []} = Catalog.get_keys("FLUSHDB", [])
      assert {:ok, []} = Catalog.get_keys("FLUSHALL", [])
      assert {:ok, []} = Catalog.get_keys("LOLWUT", [])
      assert {:ok, []} = Catalog.get_keys("SELECT", ["0"])
    end

    test "handles single key for DEL" do
      assert {:ok, ["k1"]} = Catalog.get_keys("DEL", ["k1"])
    end

    test "handles empty args for no-key command" do
      assert {:ok, []} = Catalog.get_keys("COMMAND", [])
    end
  end
end
