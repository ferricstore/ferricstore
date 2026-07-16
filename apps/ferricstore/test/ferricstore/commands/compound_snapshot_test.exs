defmodule Ferricstore.Commands.CompoundSnapshotTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.CompoundSnapshot
  alias Ferricstore.Store.CompoundKey

  test "scan member suffixes are always qualified even when they begin with the prefix" do
    key = "hash"
    prefix = CompoundKey.hash_prefix(key)
    field = prefix <> "field"

    store = %{
      compound_scan: fn ^key, ^prefix -> [{field, "value"}] end
    }

    assert {:ok, entries} = CompoundSnapshot.value_snapshot(key, "hash", store)
    assert {prefix <> field, "value", 0} in entries
  end
end
