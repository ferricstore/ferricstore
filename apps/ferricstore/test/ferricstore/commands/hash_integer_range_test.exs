defmodule Ferricstore.Commands.HashIntegerRangeTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Hash
  alias Ferricstore.Test.MockStore

  test "HINCRBY rejects an existing field outside the signed 64-bit range" do
    store = MockStore.make()
    oversized = "9223372036854775808"

    assert 1 == Hash.handle("HSET", ["hash", "field", oversized], store)

    assert {:error, "ERR hash value is not an integer"} ==
             Hash.handle("HINCRBY", ["hash", "field", "-1"], store)

    assert oversized == Hash.handle("HGET", ["hash", "field"], store)
  end
end
