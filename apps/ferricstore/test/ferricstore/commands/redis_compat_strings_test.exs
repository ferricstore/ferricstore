defmodule Ferricstore.Commands.RedisCompatStringsTest do
  @moduledoc """
  Redis compatibility tests for string command edge cases,
  extracted from RedisCompatTest.

  Covers: large value/boundary handling, MSET/MSETNX edge cases,
  SETNX/SETEX/PSETEX edge cases, GETDEL edge cases.
  """

  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Strings}
  alias Ferricstore.Test.MockStore

  # ===========================================================================
  # LARGE VALUE AND BOUNDARY HANDLING
  #
  # Known issues from Dragonfly/KeyDB:
  # - Pipeline buffer overflow with large payloads
  # - Integer overflow in INCR/INCRBY
  # ===========================================================================

  describe "large value and boundary handling" do
    test "INCR at integer max boundary" do
      store = MockStore.make()
      # Set to near max 64-bit signed integer
      # Long.MAX_VALUE - 1
      max_val = 9_223_372_036_854_775_806
      store.put.("counter", Integer.to_string(max_val), 0)

      result = Strings.handle("INCR", ["counter"], store)
      assert {:ok, 9_223_372_036_854_775_807} = result
    end

    test "DECRBY with very large negative delta" do
      store = MockStore.make()
      store.put.("counter", "0", 0)

      result = Strings.handle("DECRBY", ["counter", "9223372036854775807"], store)
      assert {:ok, -9_223_372_036_854_775_807} = result
    end

    test "SET with very large value succeeds" do
      store = MockStore.make()
      # 1 MB value
      large_value = String.duplicate("x", 1_048_576)
      assert :ok = Strings.handle("SET", ["bigkey", large_value], store)
      assert ^large_value = Strings.handle("GET", ["bigkey"], store)
    end

    test "GETRANGE with reversed indices returns empty string" do
      store = MockStore.make()
      Strings.handle("SET", ["mykey", "hello"], store)

      # start > end should return empty
      assert "" = Strings.handle("GETRANGE", ["mykey", "3", "1"], store)
    end

    test "GETRANGE with negative indices works" do
      store = MockStore.make()
      Strings.handle("SET", ["mykey", "hello"], store)

      # -3 to -1 should return "llo"
      assert "llo" = Strings.handle("GETRANGE", ["mykey", "-3", "-1"], store)
    end

    test "GETRANGE on missing key returns empty string" do
      store = MockStore.make()
      assert "" = Strings.handle("GETRANGE", ["nosuchkey", "0", "-1"], store)
    end

    test "SETRANGE with offset beyond current length zero-pads" do
      store = MockStore.make()
      Strings.handle("SET", ["mykey", "hello"], store)

      result = Strings.handle("SETRANGE", ["mykey", "10", "world"], store)
      # New length should be 10 + 5 = 15
      assert result == 15

      value = Strings.handle("GET", ["mykey"], store)
      # Should have null bytes between "hello" and "world"
      assert byte_size(value) == 15
      assert binary_part(value, 0, 5) == "hello"
      assert binary_part(value, 10, 5) == "world"
    end

    test "SETRANGE on missing key creates zero-padded value" do
      store = MockStore.make()

      result = Strings.handle("SETRANGE", ["newkey", "5", "hello"], store)
      assert result == 10

      value = Strings.handle("GET", ["newkey"], store)
      assert byte_size(value) == 10
      assert binary_part(value, 5, 5) == "hello"
    end

    test "INCRBYFLOAT with inf returns error" do
      store = MockStore.make()
      store.put.("counter", "0", 0)

      result = Strings.handle("INCRBYFLOAT", ["counter", "inf"], store)
      assert {:error, _} = result
    end

    test "INCRBYFLOAT with nan returns error" do
      store = MockStore.make()
      store.put.("counter", "0", 0)

      result = Strings.handle("INCRBYFLOAT", ["counter", "nan"], store)
      assert {:error, _} = result
    end
  end

  # ===========================================================================
  # MSET/MSETNX EDGE CASES
  # ===========================================================================

  describe "MSET/MSETNX edge cases" do
    test "MSET with odd number of arguments returns error" do
      store = MockStore.make()
      result = Strings.handle("MSET", ["k1", "v1", "k2"], store)
      assert {:error, _} = result
    end

    test "MSETNX returns 0 if any key exists, sets none" do
      store = MockStore.make()

      Strings.handle("SET", ["k2", "existing"], store)

      result = Strings.handle("MSETNX", ["k1", "v1", "k2", "v2", "k3", "v3"], store)
      assert result == 0

      # None of the new keys should have been set
      assert nil == Strings.handle("GET", ["k1"], store)
      assert "existing" = Strings.handle("GET", ["k2"], store)
      assert nil == Strings.handle("GET", ["k3"], store)
    end

    test "MSETNX returns 1 if no keys exist, sets all" do
      store = MockStore.make()

      result = Strings.handle("MSETNX", ["k1", "v1", "k2", "v2"], store)
      assert result == 1

      assert "v1" = Strings.handle("GET", ["k1"], store)
      assert "v2" = Strings.handle("GET", ["k2"], store)
    end

    test "MGET returns nil for missing keys" do
      store = MockStore.make()

      Strings.handle("SET", ["a", "1"], store)

      result = Strings.handle("MGET", ["a", "missing", "also_missing"], store)
      assert result == ["1", nil, nil]
    end
  end

  # ===========================================================================
  # SETNX/SETEX/PSETEX EDGE CASES
  # ===========================================================================

  describe "SETNX/SETEX/PSETEX edge cases" do
    test "SETNX returns 0 when key exists" do
      store = MockStore.make()
      Strings.handle("SET", ["k", "v"], store)

      assert 0 = Strings.handle("SETNX", ["k", "new"], store)
      assert "v" = Strings.handle("GET", ["k"], store)
    end

    test "SETNX returns 1 when key does not exist" do
      store = MockStore.make()

      assert 1 = Strings.handle("SETNX", ["k", "v"], store)
      assert "v" = Strings.handle("GET", ["k"], store)
    end

    test "SETEX with 0 or negative seconds returns error" do
      store = MockStore.make()

      result = Strings.handle("SETEX", ["k", "0", "v"], store)
      assert {:error, _} = result

      result = Strings.handle("SETEX", ["k", "-1", "v"], store)
      assert {:error, _} = result
    end

    test "PSETEX with 0 or negative milliseconds returns error" do
      store = MockStore.make()

      result = Strings.handle("PSETEX", ["k", "0", "v"], store)
      assert {:error, _} = result

      result = Strings.handle("PSETEX", ["k", "-1", "v"], store)
      assert {:error, _} = result
    end
  end

  # ===========================================================================
  # GETDEL EDGE CASES
  # ===========================================================================

  describe "GETDEL edge cases" do
    test "GETDEL returns value and deletes key" do
      store = MockStore.make()
      Strings.handle("SET", ["k", "hello"], store)

      assert "hello" = Strings.handle("GETDEL", ["k"], store)
      assert nil == Strings.handle("GET", ["k"], store)
    end

    test "GETDEL on missing key returns nil" do
      store = MockStore.make()
      assert nil == Strings.handle("GETDEL", ["nosuchkey"], store)
    end
  end
end
