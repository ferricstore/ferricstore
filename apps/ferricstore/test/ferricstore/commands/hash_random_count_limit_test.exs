defmodule Ferricstore.Commands.HashRandomCountLimitTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Hash
  alias Ferricstore.Test.MockStore

  @limit_error {:error, "ERR count exceeds maximum allowed response size"}

  test "HRANDFIELD bounds replacement sampling before allocating the result" do
    store = MockStore.make()
    assert 1 == Hash.handle("HSET", ["hash", "field", "value"], store)

    assert @limit_error == Hash.handle("HRANDFIELD", ["hash", "-10001"], store)
    assert @limit_error == Hash.handle_ast({:hrandfield, "hash", -10_001}, store)
  end
end
