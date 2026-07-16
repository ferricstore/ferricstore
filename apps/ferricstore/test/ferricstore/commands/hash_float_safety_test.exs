defmodule Ferricstore.Commands.HashFloatSafetyTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Hash
  alias Ferricstore.Test.MockStore

  test "HINCRBYFLOAT rejects integer-shaped input outside the float range without raising" do
    huge = String.duplicate("9", 1_000)

    assert {:error, "ERR value is not a valid float"} =
             Hash.handle("HINCRBYFLOAT", ["hash", "field", huge], MockStore.make())
  end

  test "HINCRBYFLOAT rejects a stored value outside the float range without writing" do
    base = MockStore.make()
    huge = String.duplicate("9", 1_000)
    assert 1 == Hash.handle("HSET", ["hash", "field", huge], base)

    store =
      Map.put(base, :compound_put, fn _key, _compound_key, _value, _expire_at_ms ->
        flunk("an invalid stored float must not be replaced")
      end)

    assert {:error, "ERR hash value is not a valid float"} =
             Hash.handle("HINCRBYFLOAT", ["hash", "field", "1"], store)
  end

  test "HINCRBYFLOAT rejects finite operands whose sum overflows without writing" do
    base = MockStore.make()
    assert 1 == Hash.handle("HSET", ["hash", "field", "1e308"], base)

    store =
      Map.put(base, :compound_put, fn _key, _compound_key, _value, _expire_at_ms ->
        flunk("an overflowing float sum must not be written")
      end)

    assert {:error, "ERR increment would produce NaN or Infinity"} =
             Hash.handle("HINCRBYFLOAT", ["hash", "field", "1e308"], store)
  end
end
