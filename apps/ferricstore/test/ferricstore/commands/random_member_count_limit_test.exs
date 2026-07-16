defmodule Ferricstore.Commands.RandomMemberCountLimitTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Set, SortedSet}
  alias Ferricstore.Test.MockStore

  @limit_error {:error, "ERR count exceeds maximum allowed response size"}

  test "SRANDMEMBER bounds replacement sampling before allocating the result" do
    store = MockStore.make()
    assert 1 == Set.handle("SADD", ["set", "member"], store)

    assert @limit_error == Set.handle("SRANDMEMBER", ["set", "-10001"], store)
    assert @limit_error == Set.handle_ast({:srandmember, "set", -10_001}, store)
  end

  test "ZRANDMEMBER bounds replacement sampling before allocating the result" do
    store = MockStore.make()
    assert 1 == SortedSet.handle("ZADD", ["zset", "1", "member"], store)

    assert @limit_error == SortedSet.handle("ZRANDMEMBER", ["zset", "-10001"], store)

    assert @limit_error ==
             SortedSet.handle("ZRANDMEMBER", ["zset", "-10001", "WITHSCORES"], store)

    assert @limit_error ==
             SortedSet.handle_ast({:zrandmember, "zset", -10_001, false}, store)

    assert @limit_error ==
             SortedSet.handle_ast({:zrandmember, "zset", -10_001, true}, store)
  end
end
