defmodule Ferricstore.Bugs.ListWrongtypeTest do
  @moduledoc """
  Bug #4: Missing WRONGTYPE Checks on 7 List Commands

  LSET, LREM, LTRIM, LPOS, LINSERT, LPUSHX, RPUSHX don't check key type
  before operating. Using these on a string key should return WRONGTYPE
  error but doesn't. Other list commands (LPUSH, RPUSH, LPOP, LRANGE, etc.)
  correctly check type.

  File: commands/list.ex
  """

  use ExUnit.Case, async: false
  @moduletag :global_state
  @moduletag timeout: 30_000

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    :ok
  end

  describe "LSET on wrong type" do
    test "LSET on a string key should return WRONGTYPE error" do
      FerricStore.set("wt:lset", "string_value")
      result = FerricStore.lset("wt:lset", 0, "x")

      # Redis: (error) WRONGTYPE Operation against a key holding the wrong kind of value
      # BUG: Does not check type, may succeed or return a different error
      assert {:error, msg} = result,
             "LSET on string key should return WRONGTYPE error, got: #{inspect(result)}"

      assert msg =~ "WRONGTYPE",
             "Expected WRONGTYPE error message, got: #{inspect(msg)}"
    end
  end

  describe "LREM on wrong type" do
    test "LREM on a string key should return WRONGTYPE error" do
      FerricStore.set("wt:lrem", "string_value")
      result = FerricStore.lrem("wt:lrem", 0, "x")

      # Redis: (error) WRONGTYPE ...
      # BUG: Does not check type
      assert {:error, msg} = result,
             "LREM on string key should return WRONGTYPE error, got: #{inspect(result)}"

      assert msg =~ "WRONGTYPE",
             "Expected WRONGTYPE error message, got: #{inspect(msg)}"
    end
  end

  describe "LPOS on wrong type" do
    test "LPOS on a string key should return WRONGTYPE error" do
      FerricStore.set("wt:lpos", "string_value")
      result = FerricStore.lpos("wt:lpos", "x")

      # Redis: (error) WRONGTYPE ...
      # BUG: Does not check type
      assert {:error, msg} = result,
             "LPOS on string key should return WRONGTYPE error, got: #{inspect(result)}"

      assert msg =~ "WRONGTYPE",
             "Expected WRONGTYPE error message, got: #{inspect(msg)}"
    end
  end

  describe "LINSERT on wrong type" do
    test "LINSERT on a string key should return WRONGTYPE error" do
      FerricStore.set("wt:linsert", "string_value")
      result = FerricStore.linsert("wt:linsert", :before, "x", "y")

      # Redis: (error) WRONGTYPE ...
      # BUG: Does not check type
      assert {:error, msg} = result,
             "LINSERT on string key should return WRONGTYPE error, got: #{inspect(result)}"

      assert msg =~ "WRONGTYPE",
             "Expected WRONGTYPE error message, got: #{inspect(msg)}"
    end
  end

  describe "LTRIM on wrong type (via Dispatcher)" do
    test "LTRIM on a string key should return WRONGTYPE error" do
      FerricStore.set("wt:ltrim", "string_value")

      # No embedded API for LTRIM — call the handler directly via MockStore
      store = Ferricstore.Test.MockStore.make(%{"wt:ltrim" => {"string_value", 0}})
      result = Ferricstore.Commands.List.handle("LTRIM", ["wt:ltrim", "0", "-1"], store)

      # Redis: (error) WRONGTYPE ...
      # BUG: Does not check type
      assert {:error, msg} = result,
             "LTRIM on string key should return WRONGTYPE error, got: #{inspect(result)}"

      assert msg =~ "WRONGTYPE",
             "Expected WRONGTYPE error message, got: #{inspect(msg)}"
    end
  end

  describe "LPUSHX on wrong type (via Dispatcher)" do
    test "LPUSHX on a string key should return WRONGTYPE error" do
      store = Ferricstore.Test.MockStore.make(%{"wt:lpushx" => {"string_value", 0}})
      result = Ferricstore.Commands.List.handle("LPUSHX", ["wt:lpushx", "x"], store)

      # Redis: (error) WRONGTYPE ...
      # BUG: Does not check type
      assert {:error, msg} = result,
             "LPUSHX on string key should return WRONGTYPE error, got: #{inspect(result)}"

      assert msg =~ "WRONGTYPE",
             "Expected WRONGTYPE error message, got: #{inspect(msg)}"
    end
  end

  describe "RPUSHX on wrong type (via Dispatcher)" do
    test "RPUSHX on a string key should return WRONGTYPE error" do
      store = Ferricstore.Test.MockStore.make(%{"wt:rpushx" => {"string_value", 0}})
      result = Ferricstore.Commands.List.handle("RPUSHX", ["wt:rpushx", "x"], store)

      # Redis: (error) WRONGTYPE ...
      # BUG: Does not check type
      assert {:error, msg} = result,
             "RPUSHX on string key should return WRONGTYPE error, got: #{inspect(result)}"

      assert msg =~ "WRONGTYPE",
             "Expected WRONGTYPE error message, got: #{inspect(msg)}"
    end
  end
end
