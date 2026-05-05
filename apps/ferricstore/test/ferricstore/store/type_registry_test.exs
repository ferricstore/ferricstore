defmodule Ferricstore.Store.TypeRegistryTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.TypeRegistry

  test "check_or_set propagates type marker write errors" do
    store = %{
      exists?: fn "hash" -> false end,
      compound_get: fn "hash", _compound_key -> nil end,
      compound_put: fn "hash", _compound_key, "hash", 0 -> {:error, :disk_full} end
    }

    assert {:error, :disk_full} == TypeRegistry.check_or_set("hash", :hash, store)
  end
end
