defmodule Ferricstore.Commands.ConfigLocalTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Server
  alias Ferricstore.Config
  alias Ferricstore.Config.Local, as: ConfigLocal
  alias Ferricstore.MemoryGuard
  alias Ferricstore.Test.MockStore

  # Ensure the local config ETS table is clean before/after each test,
  # and restore the original Logger level.
  setup do
    original_level = Logger.level()
    original_eviction = Application.get_env(:ferricstore, :eviction_policy)
    original_memory_guard_state = :sys.get_state(MemoryGuard)
    original_reject = MemoryGuard.reject_writes?()
    original_keydir_full = MemoryGuard.keydir_full?()

    # Reset local config table if it exists
    ConfigLocal.reset_all()

    on_exit(fn ->
      ConfigLocal.reset_all()
      Logger.configure(level: original_level)
      restore_env(:eviction_policy, original_eviction)
      :sys.replace_state(MemoryGuard, fn _ -> original_memory_guard_state end)
      MemoryGuard.set_reject_writes(original_reject)
      MemoryGuard.set_keydir_full(original_keydir_full)
    end)

    :ok
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  # ===========================================================================
  # CONFIG SET LOCAL — happy paths
  # ===========================================================================

  describe "CONFIG SET LOCAL log_level" do
    test "sets log_level to debug" do
      result = Server.handle("CONFIG", ["SET", "LOCAL", "log_level", "debug"], MockStore.make())
      assert result == :ok
    end

    test "sets log_level to info" do
      result = Server.handle("CONFIG", ["SET", "LOCAL", "log_level", "info"], MockStore.make())
      assert result == :ok
    end

    test "sets log_level to warning" do
      result = Server.handle("CONFIG", ["SET", "LOCAL", "log_level", "warning"], MockStore.make())
      assert result == :ok
    end

    test "sets log_level to error" do
      result = Server.handle("CONFIG", ["SET", "LOCAL", "log_level", "error"], MockStore.make())
      assert result == :ok
    end

    test "setting log_level actually configures the Logger" do
      Server.handle("CONFIG", ["SET", "LOCAL", "log_level", "debug"], MockStore.make())
      assert Logger.level() == :debug
    end

    test "setting log_level to warning configures the Logger" do
      Server.handle("CONFIG", ["SET", "LOCAL", "log_level", "warning"], MockStore.make())
      assert Logger.level() == :warning
    end
  end

  # ===========================================================================
  # CONFIG GET LOCAL — happy paths
  # ===========================================================================

  describe "CONFIG GET LOCAL" do
    test "returns set value for log_level" do
      Server.handle("CONFIG", ["SET", "LOCAL", "log_level", "debug"], MockStore.make())
      result = Server.handle("CONFIG", ["GET", "LOCAL", "log_level"], MockStore.make())
      assert result == ["log_level", "debug"]
    end

    test "returns current Logger level when log_level not explicitly set" do
      result = Server.handle("CONFIG", ["GET", "LOCAL", "log_level"], MockStore.make())
      assert ["log_level", level_str] = result
      assert level_str == Atom.to_string(Logger.level())
    end

    test "reflects the most recent SET LOCAL value" do
      Server.handle("CONFIG", ["SET", "LOCAL", "log_level", "debug"], MockStore.make())
      Server.handle("CONFIG", ["SET", "LOCAL", "log_level", "error"], MockStore.make())
      result = Server.handle("CONFIG", ["GET", "LOCAL", "log_level"], MockStore.make())
      assert result == ["log_level", "error"]
    end
  end

  # ===========================================================================
  # CONFIG SET LOCAL — error cases
  # ===========================================================================

  describe "CONFIG SET LOCAL error cases" do
    test "rejects unknown local parameter" do
      result = Server.handle("CONFIG", ["SET", "LOCAL", "unknown_param", "value"], MockStore.make())
      assert {:error, msg} = result
      assert msg =~ "Unsupported" or msg =~ "unknown"
    end

    test "rejects invalid log_level value" do
      result = Server.handle("CONFIG", ["SET", "LOCAL", "log_level", "invalid_level"], MockStore.make())
      assert {:error, msg} = result
      assert msg =~ "Invalid" or msg =~ "invalid"
    end

    test "CONFIG SET LOCAL with wrong number of args returns error" do
      result = Server.handle("CONFIG", ["SET", "LOCAL", "log_level"], MockStore.make())
      assert {:error, msg} = result
      assert msg =~ "wrong number of arguments"
    end

    test "CONFIG SET LOCAL with no key or value returns error" do
      result = Server.handle("CONFIG", ["SET", "LOCAL"], MockStore.make())
      assert {:error, msg} = result
      assert msg =~ "wrong number of arguments"
    end
  end

  # ===========================================================================
  # CONFIG GET LOCAL — error cases
  # ===========================================================================

  describe "CONFIG GET LOCAL error cases" do
    test "rejects unknown local parameter" do
      result = Server.handle("CONFIG", ["GET", "LOCAL", "unknown_param"], MockStore.make())
      assert {:error, msg} = result
      assert msg =~ "Unsupported" or msg =~ "unknown"
    end

    test "CONFIG GET LOCAL with no key returns error" do
      result = Server.handle("CONFIG", ["GET", "LOCAL"], MockStore.make())
      assert {:error, msg} = result
      assert msg =~ "wrong number of arguments"
    end
  end

  # ===========================================================================
  # Regular CONFIG SET still works (no regression)
  # ===========================================================================

  describe "regular CONFIG SET is unaffected" do
    test "CONFIG SET hz still works as before" do
      result = Server.handle("CONFIG", ["SET", "hz", "50"], MockStore.make())
      assert result == :ok
      assert ["hz", "50"] = Server.handle("CONFIG", ["GET", "hz"], MockStore.make())
    end

    test "CONFIG SET maxmemory-policy still validates" do
      result = Server.handle("CONFIG", ["SET", "maxmemory-policy", "noeviction"], MockStore.make())
      assert result == :ok
    end
  end

  # ===========================================================================
  # Local settings do NOT appear in CONFIG REWRITE output
  # ===========================================================================

  describe "local settings excluded from CONFIG REWRITE" do
    test "CONFIG REWRITE does not include local settings" do
      Server.handle("CONFIG", ["SET", "LOCAL", "log_level", "debug"], MockStore.make())
      Server.handle("CONFIG", ["REWRITE"], MockStore.make())

      path = Config.config_file_path()

      if File.exists?(path) do
        content = File.read!(path)
        refute content =~ "log_level"
        File.rm(path)
      end
    end
  end

  # ===========================================================================
  # Local settings do NOT appear in regular CONFIG GET *
  # ===========================================================================

  describe "local settings isolation from global config" do
    test "CONFIG GET * does not include local-only parameters" do
      Server.handle("CONFIG", ["SET", "LOCAL", "log_level", "debug"], MockStore.make())
      result = Server.handle("CONFIG", ["GET", "*"], MockStore.make())
      keys = extract_keys(result)
      refute "log_level" in keys
    end

    test "CONFIG GET log_level (without LOCAL) returns empty" do
      Server.handle("CONFIG", ["SET", "LOCAL", "log_level", "debug"], MockStore.make())
      result = Server.handle("CONFIG", ["GET", "log_level"], MockStore.make())
      assert result == []
    end
  end

  # ===========================================================================
  # ConfigLocal module direct API
  # ===========================================================================

  describe "Config.Local module API" do
    test "set/2 stores and get/1 retrieves log_level" do
      assert :ok = ConfigLocal.set("log_level", "debug")
      assert {:ok, "debug"} = ConfigLocal.get("log_level")
    end

    test "set/2 returns error for unknown param" do
      assert {:error, _} = ConfigLocal.set("bogus", "value")
    end

    test "get/1 returns error for unknown param" do
      assert {:error, _} = ConfigLocal.get("bogus")
    end

    test "get_all/0 returns all local settings" do
      ConfigLocal.set("log_level", "debug")
      all = ConfigLocal.get_all()
      assert is_map(all)
      assert Map.get(all, "log_level") == "debug"
    end

    test "reset_all/0 clears all local settings" do
      ConfigLocal.set("log_level", "debug")
      ConfigLocal.reset_all()
      # After reset, get falls back to current Logger level
      {:ok, val} = ConfigLocal.get("log_level")
      assert val == Atom.to_string(Logger.level())
    end
  end

  # ===========================================================================
  # Helpers
  # ===========================================================================

  defp extract_keys(flat_list) when is_list(flat_list) do
    flat_list
    |> Enum.drop(0)
    |> Enum.take_every(2)
  end
end
