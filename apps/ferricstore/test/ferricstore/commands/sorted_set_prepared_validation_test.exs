defmodule Ferricstore.Commands.SortedSetPreparedValidationTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Dispatcher, NativeAstParser, SortedSet}
  alias Ferricstore.Test.MockStore

  test "prepared ZADD rejects conflicts and invalid scores without mutating storage" do
    store = MockStore.make()

    assert {:error, "ERR XX and NX options at the same time are not compatible"} =
             parse_and_dispatch("ZADD", ["zset", "NX", "XX", "1", "member"], store)

    assert {:error, "ERR value is not a valid float"} =
             parse_and_dispatch("ZADD", ["zset", "not-a-score", "member"], store)

    assert 0 == SortedSet.handle("ZCARD", ["zset"], store)
  end

  test "prepared ZPOPMIN rejects a negative count without removing a member" do
    store = MockStore.make()
    assert 1 == SortedSet.handle("ZADD", ["zset", "1", "member"], store)

    assert {:error, "ERR value is not an integer or out of range"} =
             parse_and_dispatch("ZPOPMIN", ["zset", "-1"], store)

    assert 1 == SortedSet.handle("ZCARD", ["zset"], store)
  end

  test "prepared sorted-set options are case-insensitive" do
    store = MockStore.make()

    assert 1 == parse_and_dispatch("ZADD", ["zset", "nx", "1", "member"], store)

    assert ["member", "1.0"] ==
             parse_and_dispatch("ZRANGE", ["zset", "0", "-1", "withscores"], store)

    assert ["member", "1.0"] ==
             parse_and_dispatch("ZRANDMEMBER", ["zset", "1", "withscores"], store)
  end

  defp parse_and_dispatch(command, args, store) do
    assert {:ok, ^command, ^args, ast, _keys} = NativeAstParser.parse(command, args)
    Dispatcher.dispatch_ast(ast, store)
  end
end
