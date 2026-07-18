defmodule Ferricstore.Store.TypeRegistryTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.{CompoundKey, TypeRegistry}

  test "check_or_set propagates type marker write errors" do
    store = %{
      exists?: fn "hash" -> false end,
      compound_get: fn "hash", _compound_key -> nil end,
      compound_put: fn "hash", _compound_key, "hash", 0 -> {:error, :disk_full} end
    }

    assert {:error, :disk_full} == TypeRegistry.check_or_set("hash", :hash, store)
  end

  test "check_or_set uses the store's atomic first-claim contract" do
    parent = self()

    store = %{
      compound_type_claim: fn "shared", type ->
        send(parent, {:claimed, type})
        {:ok, :created}
      end,
      compound_get: fn _redis_key, _compound_key ->
        flunk("an atomic type claim must not perform a separate marker read")
      end,
      compound_put: fn _redis_key, _compound_key, _value, _expire_at_ms ->
        flunk("an atomic type claim must not perform a separate marker write")
      end
    }

    assert {:ok, :created} = TypeRegistry.check_or_set_status("shared", :hash, store)
    assert_receive {:claimed, :hash}
  end

  test "probabilistic type checks accept replay-stamped type markers" do
    marker = CompoundKey.encode_prob_type(:bloom, 42)

    store = %{
      compound_get: fn "filter", compound_key ->
        assert compound_key == CompoundKey.type_key("filter")
        marker
      end
    }

    assert :ok = TypeRegistry.check_or_set_status("filter", :bloom, store)
    assert :ok = TypeRegistry.serialized_claim_status("filter", :bloom, store)
    assert :ok = TypeRegistry.check_type("filter", :bloom, store)
  end
end
