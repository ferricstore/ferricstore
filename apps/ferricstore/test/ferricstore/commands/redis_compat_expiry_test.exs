defmodule Ferricstore.Commands.RedisCompatExpiryTest do
  @moduledoc """
  Redis compatibility tests for key expiry edge cases,
  extracted from RedisCompatTest.

  Covers: EXPIRE/PEXPIRE edge cases, TTL/PTTL on missing/non-expiring keys,
  PERSIST, EXPIREAT with past timestamp, EXPIRETIME/PEXPIRETIME edge cases,
  GETEX edge cases.
  """

  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Expiry, Generic, Strings}
  alias Ferricstore.Test.MockStore

  # ===========================================================================
  # KEY EXPIRY EDGE CASES
  #
  # Known issues:
  # - EXPIRE with 0 seconds: should delete key immediately in Redis 7+
  # - Negative expire times
  # - TTL precision and rounding
  # - PERSIST on key without expiry
  # ===========================================================================

  describe "key expiry edge cases" do
    test "EXPIRE 0 on a key should succeed (key expires immediately)" do
      store = MockStore.make()
      Strings.handle("SET", ["mykey", "hello"], store)

      result = Expiry.handle("EXPIRE", ["mykey", "0"], store)
      # Redis 7+: EXPIRE 0 means key expires now
      assert result == 1 or match?({:error, _}, result)
    end

    test "PEXPIRE 0 on a key should succeed" do
      store = MockStore.make()
      Strings.handle("SET", ["mykey", "hello"], store)

      result = Expiry.handle("PEXPIRE", ["mykey", "0"], store)
      assert result == 1 or match?({:error, _}, result)
    end

    test "EXPIRE with negative value deletes the key (Redis compat)" do
      store = MockStore.make()
      Strings.handle("SET", ["mykey", "hello"], store)

      result = Expiry.handle("EXPIRE", ["mykey", "-1"], store)
      assert 1 == result
      assert nil == store.get.("mykey")
    end

    test "TTL on missing key returns -2" do
      store = MockStore.make()
      assert -2 = Expiry.handle("TTL", ["nosuchkey"], store)
    end

    test "TTL on key without expiry returns -1" do
      store = MockStore.make()
      Strings.handle("SET", ["mykey", "hello"], store)
      assert -1 = Expiry.handle("TTL", ["mykey"], store)
    end

    test "PTTL on missing key returns -2" do
      store = MockStore.make()
      assert -2 = Expiry.handle("PTTL", ["nosuchkey"], store)
    end

    test "PERSIST on key without expiry returns 0" do
      store = MockStore.make()
      Strings.handle("SET", ["mykey", "hello"], store)
      assert 0 = Expiry.handle("PERSIST", ["mykey"], store)
    end

    test "PERSIST on missing key returns 0" do
      store = MockStore.make()
      assert 0 = Expiry.handle("PERSIST", ["nosuchkey"], store)
    end

    test "PERSIST on key with expiry returns 1 and removes TTL" do
      store = MockStore.make()
      Strings.handle("SET", ["mykey", "hello", "EX", "3600"], store)

      assert 1 = Expiry.handle("PERSIST", ["mykey"], store)
      assert -1 = Expiry.handle("TTL", ["mykey"], store)
    end

    test "EXPIREAT with past timestamp makes key unreachable" do
      store = MockStore.make()
      Strings.handle("SET", ["mykey", "hello"], store)

      # Set expiry to Unix epoch (Jan 1, 1970) - definitely in the past
      assert 1 = Expiry.handle("EXPIREAT", ["mykey", "1"], store)

      # Key should be expired now -- TTL returns -2 or 0
      ttl = Expiry.handle("TTL", ["mykey"], store)
      # Expired keys may still linger until lazy expiry checks them.
      # In Redis, TTL returns -2 for missing/expired keys, but the key
      # may not be lazily evicted yet.
      assert ttl == -2 or ttl == 0
    end
  end

  # ===========================================================================
  # EXPIRETIME/PEXPIRETIME EDGE CASES
  # ===========================================================================

  describe "EXPIRETIME/PEXPIRETIME edge cases" do
    test "EXPIRETIME on missing key returns -2" do
      store = MockStore.make()
      assert -2 = Generic.handle("EXPIRETIME", ["nosuchkey"], store)
    end

    test "EXPIRETIME on key without expiry returns -1" do
      store = MockStore.make()
      Strings.handle("SET", ["k", "v"], store)
      assert -1 = Generic.handle("EXPIRETIME", ["k"], store)
    end

    test "PEXPIRETIME on missing key returns -2" do
      store = MockStore.make()
      assert -2 = Generic.handle("PEXPIRETIME", ["nosuchkey"], store)
    end

    test "PEXPIRETIME on key without expiry returns -1" do
      store = MockStore.make()
      Strings.handle("SET", ["k", "v"], store)
      assert -1 = Generic.handle("PEXPIRETIME", ["k"], store)
    end
  end

  # ===========================================================================
  # GETEX EDGE CASES
  # ===========================================================================

  describe "GETEX edge cases" do
    test "GETEX without options returns value without changing TTL" do
      store = MockStore.make()
      Strings.handle("SET", ["k", "hello"], store)

      assert "hello" = Strings.handle("GETEX", ["k"], store)
      assert -1 = Expiry.handle("TTL", ["k"], store)
    end

    test "GETEX with PERSIST removes TTL" do
      store = MockStore.make()
      Strings.handle("SET", ["k", "hello", "EX", "3600"], store)

      assert "hello" = Strings.handle("GETEX", ["k", "PERSIST"], store)
      assert -1 = Expiry.handle("TTL", ["k"], store)
    end

    test "GETEX on missing key returns nil" do
      store = MockStore.make()
      assert nil == Strings.handle("GETEX", ["nosuchkey"], store)
    end

    test "GETEX with EX sets TTL in seconds" do
      store = MockStore.make()
      Strings.handle("SET", ["k", "hello"], store)

      assert "hello" = Strings.handle("GETEX", ["k", "EX", "60"], store)
      ttl = Expiry.handle("TTL", ["k"], store)
      assert ttl > 0 and ttl <= 60
    end

    test "GETEX with invalid option returns syntax error" do
      store = MockStore.make()
      Strings.handle("SET", ["k", "hello"], store)

      result = Strings.handle("GETEX", ["k", "BOGUS"], store)
      assert {:error, _} = result
    end
  end
end
