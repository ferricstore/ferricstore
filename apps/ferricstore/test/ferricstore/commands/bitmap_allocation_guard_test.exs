defmodule Ferricstore.Commands.BitmapAllocationGuardTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Bitmap
  alias Ferricstore.Test.MockStore

  test "SETBIT rejects an oversized derived value before reading or allocating it" do
    parent = self()

    store =
      MockStore.make()
      |> Map.put(:max_value_size, 8)
      |> Map.put(:get_meta, fn key ->
        send(parent, {:read, key})
        nil
      end)
      |> Map.put(:put, fn key, value, expire_at_ms ->
        send(parent, {:write, key, value, expire_at_ms})
        :ok
      end)

    assert {:error, "ERR value too large (9 bytes, max 8 bytes)"} =
             Bitmap.handle("SETBIT", ["key", "64", "1"], store)

    refute_received {:read, _key}
    refute_received {:write, _key, _value, _expire_at_ms}
  end
end
