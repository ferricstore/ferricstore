defmodule Ferricstore.Commands.ZaddEmptyWriteCleanupTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.SortedSet
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Test.MockStore

  test "ZADD XX does not leave a type marker when no member can be written" do
    type_key = CompoundKey.type_key("zset")
    raw_store = MockStore.make()
    assert 0 == SortedSet.handle("ZADD", ["zset", "XX", "1", "member"], raw_store)
    assert nil == raw_store.compound_get.("zset", type_key)

    prepared_store = MockStore.make()

    assert 0 ==
             SortedSet.handle_ast({:zadd, "zset", [:xx], [{1.0, "member"}]}, prepared_store)

    assert nil == prepared_store.compound_get.("zset", type_key)
  end
end
