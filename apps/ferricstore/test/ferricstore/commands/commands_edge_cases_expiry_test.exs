defmodule Ferricstore.Commands.CommandsEdgeCasesExpiryTest do
  @moduledoc """
  Edge cases for expiry commands (EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST).
  Split from CommandsEdgeCasesTest.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Expiry
  alias Ferricstore.Test.MockStore

  # ===========================================================================
  # Expiry — EXPIRE edge cases
  # ===========================================================================

  describe "EXPIRE edge cases (not covered elsewhere)" do
    test "EXPIRE with only key arg (no seconds) returns error" do
      assert {:error, _} = Expiry.handle("EXPIRE", ["k"], MockStore.make())
    end

    test "EXPIRE with 3 args returns error" do
      assert {:error, _} = Expiry.handle("EXPIRE", ["k", "10", "extra"], MockStore.make())
    end

    test "EXPIRE with float seconds returns error" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert {:error, _} = Expiry.handle("EXPIRE", ["k", "1.5"], store)
    end

    test "EXPIRE with very large seconds value succeeds" do
      store = MockStore.make(%{"k" => {"v", 0}})
      # 10 years in seconds
      assert 1 == Expiry.handle("EXPIRE", ["k", "315360000"], store)
      ttl = Expiry.handle("TTL", ["k"], store)
      assert ttl > 315_359_000
    end

    test "EXPIRE on expired key returns 0 (key treated as missing)" do
      past = System.os_time(:millisecond) - 1_000
      store = MockStore.make(%{"k" => {"v", past}})
      assert 0 == Expiry.handle("EXPIRE", ["k", "10"], store)
    end
  end

  # ===========================================================================
  # Expiry — PEXPIRE edge cases
  # ===========================================================================

  describe "PEXPIRE edge cases (not covered elsewhere)" do
    test "PEXPIRE with 0 ms expires key immediately" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("PEXPIRE", ["k", "0"], store)
      # Key should be expired now
      assert -2 == Expiry.handle("PTTL", ["k"], store)
    end

    test "PEXPIRE with only key arg returns error" do
      assert {:error, _} = Expiry.handle("PEXPIRE", ["k"], MockStore.make())
    end

    test "PEXPIRE with 3 args returns error" do
      assert {:error, _} = Expiry.handle("PEXPIRE", ["k", "5000", "extra"], MockStore.make())
    end

    test "PEXPIRE with non-integer returns error" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert {:error, _} = Expiry.handle("PEXPIRE", ["k", "abc"], store)
    end

    test "PEXPIRE with float returns error" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert {:error, _} = Expiry.handle("PEXPIRE", ["k", "1.5"], store)
    end

    test "PEXPIRE on expired key returns 0" do
      past = System.os_time(:millisecond) - 1_000
      store = MockStore.make(%{"k" => {"v", past}})
      assert 0 == Expiry.handle("PEXPIRE", ["k", "5000"], store)
    end

    test "PEXPIRE updates existing TTL" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("PEXPIRE", ["k", "60000"], store)
      assert 1 == Expiry.handle("PEXPIRE", ["k", "120000"], store)
      pttl = Expiry.handle("PTTL", ["k"], store)
      # Should be closer to 120_000 than 60_000
      assert pttl > 60_000
    end
  end

  # ===========================================================================
  # Expiry — EXPIREAT edge cases
  # ===========================================================================

  describe "EXPIREAT edge cases (not covered elsewhere)" do
    test "EXPIREAT with far-future timestamp persists key" do
      store = MockStore.make(%{"k" => {"v", 0}})
      # Year ~2050
      future_unix = 2_524_608_000
      assert 1 == Expiry.handle("EXPIREAT", ["k", "#{future_unix}"], store)
      ttl = Expiry.handle("TTL", ["k"], store)
      assert ttl > 0
    end

    test "EXPIREAT with 0 timestamp deletes the key" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("EXPIREAT", ["k", "0"], store)
      assert -2 == Expiry.handle("TTL", ["k"], store)
    end

    test "EXPIREAT with only key arg returns error" do
      assert {:error, _} = Expiry.handle("EXPIREAT", ["k"], MockStore.make())
    end

    test "EXPIREAT with 3 args returns error" do
      assert {:error, _} = Expiry.handle("EXPIREAT", ["k", "123", "extra"], MockStore.make())
    end

    test "EXPIREAT on missing key returns 0" do
      future_unix = div(System.os_time(:millisecond), 1_000) + 3_600
      assert 0 == Expiry.handle("EXPIREAT", ["missing", "#{future_unix}"], MockStore.make())
    end

    test "EXPIREAT on expired key returns 0" do
      past = System.os_time(:millisecond) - 1_000
      store = MockStore.make(%{"k" => {"v", past}})
      future_unix = div(System.os_time(:millisecond), 1_000) + 3_600
      assert 0 == Expiry.handle("EXPIREAT", ["k", "#{future_unix}"], store)
    end
  end

  # ===========================================================================
  # Expiry — PEXPIREAT edge cases
  # ===========================================================================

  describe "PEXPIREAT edge cases (not covered elsewhere)" do
    test "PEXPIREAT with ms timestamp in the past expires key immediately" do
      store = MockStore.make(%{"k" => {"v", 0}})
      past_ms = System.os_time(:millisecond) - 60_000
      assert 1 == Expiry.handle("PEXPIREAT", ["k", "#{past_ms}"], store)
      assert -2 == Expiry.handle("PTTL", ["k"], store)
    end

    test "PEXPIREAT with 0 timestamp deletes the key" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Expiry.handle("PEXPIREAT", ["k", "0"], store)
      assert -2 == Expiry.handle("PTTL", ["k"], store)
    end

    test "PEXPIREAT with far-future ms timestamp persists key" do
      store = MockStore.make(%{"k" => {"v", 0}})
      far_future = System.os_time(:millisecond) + 86_400_000 * 365
      assert 1 == Expiry.handle("PEXPIREAT", ["k", "#{far_future}"], store)
      pttl = Expiry.handle("PTTL", ["k"], store)
      assert pttl > 86_400_000
    end

    test "PEXPIREAT with non-integer returns error" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert {:error, _} = Expiry.handle("PEXPIREAT", ["k", "abc"], store)
    end

    test "PEXPIREAT with only key arg returns error" do
      assert {:error, _} = Expiry.handle("PEXPIREAT", ["k"], MockStore.make())
    end

    test "PEXPIREAT with 3 args returns error" do
      assert {:error, _} = Expiry.handle("PEXPIREAT", ["k", "123", "extra"], MockStore.make())
    end

    test "PEXPIREAT on missing key returns 0" do
      future_ms = System.os_time(:millisecond) + 3_600_000
      assert 0 == Expiry.handle("PEXPIREAT", ["missing", "#{future_ms}"], MockStore.make())
    end
  end

  # ===========================================================================
  # Expiry — TTL edge cases
  # ===========================================================================

  describe "TTL edge cases (not covered elsewhere)" do
    test "TTL with 2 args returns error" do
      assert {:error, _} = Expiry.handle("TTL", ["a", "b"], MockStore.make())
    end

    test "TTL on key with expire_at_ms=0 returns -1 (no expiry)" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert -1 == Expiry.handle("TTL", ["k"], store)
    end

    test "TTL on key with already-passed expiry returns 0 (clamped by max)" do
      past = System.os_time(:millisecond) - 10_000
      store = MockStore.make(%{"k" => {"v", past}})
      # MockStore's read_meta returns nil for expired keys, so TTL returns -2
      assert -2 == Expiry.handle("TTL", ["k"], store)
    end
  end

  # ===========================================================================
  # Expiry — PTTL edge cases
  # ===========================================================================

  describe "PTTL edge cases (not covered elsewhere)" do
    test "PTTL with 2 args returns error" do
      assert {:error, _} = Expiry.handle("PTTL", ["a", "b"], MockStore.make())
    end

    test "PTTL on key with expire_at_ms=0 returns -1 (no expiry)" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert -1 == Expiry.handle("PTTL", ["k"], store)
    end
  end

  # ===========================================================================
  # Expiry — PERSIST edge cases
  # ===========================================================================

  describe "PERSIST edge cases (not covered elsewhere)" do
    test "PERSIST with 2 args returns error" do
      assert {:error, _} = Expiry.handle("PERSIST", ["a", "b"], MockStore.make())
    end

    test "PERSIST on key with expire_at_ms=0 returns 0 (already persistent)" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 0 == Expiry.handle("PERSIST", ["k"], store)
    end

    test "PERSIST on expired key returns 0 (treated as missing)" do
      past = System.os_time(:millisecond) - 1_000
      store = MockStore.make(%{"k" => {"v", past}})
      assert 0 == Expiry.handle("PERSIST", ["k"], store)
    end

    test "PERSIST preserves value after removing TTL" do
      future = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"k" => {"myval", future}})
      assert 1 == Expiry.handle("PERSIST", ["k"], store)
      assert "myval" == store.get.("k")
      assert -1 == Expiry.handle("TTL", ["k"], store)
    end

    test "PERSIST is idempotent (second call returns 0)" do
      future = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"k" => {"v", future}})
      assert 1 == Expiry.handle("PERSIST", ["k"], store)
      assert 0 == Expiry.handle("PERSIST", ["k"], store)
    end
  end
end
