defmodule Ferricstore.Commands.ExpiryConditionalImmediateTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Expiry
  alias Ferricstore.Test.MockStore

  test "conditional relative expiries evaluate flags before immediate deletion" do
    for command <- ["EXPIRE", "PEXPIRE"] do
      persistent = MockStore.make(%{"key" => {"value", 0}})
      assert 0 == Expiry.handle(command, ["key", "0", "XX"], persistent)
      assert "value" == persistent.get.("key")

      expiring = MockStore.make(%{"key" => {"value", System.os_time(:millisecond) + 60_000}})
      assert 0 == Expiry.handle(command, ["key", "0", "NX"], expiring)
      assert "value" == expiring.get.("key")

      allowed = MockStore.make(%{"key" => {"value", 0}})
      assert 1 == Expiry.handle(command, ["key", "0", "NX"], allowed)
      assert nil == allowed.get.("key")
    end
  end

  test "conditional absolute expiries evaluate flags before past-deadline deletion" do
    for command <- ["EXPIREAT", "PEXPIREAT"] do
      persistent = MockStore.make(%{"key" => {"value", 0}})
      assert 0 == Expiry.handle(command, ["key", "1", "XX"], persistent)
      assert "value" == persistent.get.("key")

      expiring = MockStore.make(%{"key" => {"value", System.os_time(:millisecond) + 60_000}})
      assert 0 == Expiry.handle(command, ["key", "1", "NX"], expiring)
      assert "value" == expiring.get.("key")

      allowed = MockStore.make(%{"key" => {"value", 0}})
      assert 1 == Expiry.handle(command, ["key", "1", "NX"], allowed)
      assert nil == allowed.get.("key")
    end
  end
end
