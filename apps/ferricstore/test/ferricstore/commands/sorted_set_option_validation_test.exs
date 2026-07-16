defmodule Ferricstore.Commands.SortedSetOptionValidationTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.SortedSet
  alias Ferricstore.Test.MockStore

  @syntax_error {:error, "ERR syntax error"}

  setup do
    store = MockStore.make()
    assert 2 == SortedSet.handle("ZADD", ["zset", "1", "one", "2", "two"], store)
    %{store: store}
  end

  test "rank ranges accept lowercase WITHSCORES and reject every other tail", %{store: store} do
    assert ["one", "1.0", "two", "2.0"] ==
             SortedSet.handle("ZRANGE", ["zset", "0", "-1", "withscores"], store)

    assert @syntax_error == SortedSet.handle("ZRANGE", ["zset", "0", "-1", "bogus"], store)

    assert @syntax_error ==
             SortedSet.handle("ZREVRANGE", ["zset", "0", "-1", "bogus"], store)
  end

  test "score ranges reject unknown and incomplete options", %{store: store} do
    assert @syntax_error ==
             SortedSet.handle("ZRANGEBYSCORE", ["zset", "-inf", "+inf", "bogus"], store)

    assert @syntax_error ==
             SortedSet.handle("ZRANGEBYSCORE", ["zset", "-inf", "+inf", "LIMIT", "0"], store)

    assert @syntax_error ==
             SortedSet.handle("ZREVRANGEBYSCORE", ["zset", "+inf", "-inf", "LIMIT"], store)
  end
end
