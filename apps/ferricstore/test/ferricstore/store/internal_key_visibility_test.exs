defmodule Ferricstore.Store.InternalKeyVisibilityTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.CompoundKey

  @state_key "f:{f}:s:flow-1"
  @history_entry "X:f:{f}:h:flow-1\0" <> "123-4"

  test "user-visible normalization hides canonical Flow keys after type-key extraction" do
    raw_keys = [
      "ordinary",
      @state_key,
      @history_entry,
      "T:" <> @state_key,
      "T:user-hash",
      "H:user-hash\0field"
    ]

    assert ["ordinary", "user-hash"] ==
             raw_keys |> CompoundKey.user_visible_keys() |> Enum.sort()
  end

  test "storage cleanup normalization retains Flow primary keys" do
    raw_keys = ["ordinary", @state_key, @history_entry, "T:user-hash", "H:user-hash\0field"]

    assert [@state_key, "ordinary", "user-hash"] ==
             raw_keys |> CompoundKey.storage_logical_keys() |> Enum.sort()
  end
end
