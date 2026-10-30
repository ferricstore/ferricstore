defmodule FerricStore.API.SortedSetRangeTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  setup do
    tag = "{zrapi:#{System.unique_integer([:positive])}}"
    k = &(tag <> &1)
    {:ok, 4} = FerricStore.zadd(k.("lex"), [{0, "a"}, {0, "b"}, {0, "c"}, {0, "d"}])
    {:ok, 3} = FerricStore.zadd(k.("z"), [{1, "one"}, {2, "two"}, {3, "three"}])
    %{k: k}
  end

  test "lex reads", %{k: k} do
    assert {:ok, ["b", "c"]} = FerricStore.zrangebylex(k.("lex"), "[b", "[c")
    assert {:ok, ["b"]} = FerricStore.zrangebylex(k.("lex"), "-", "+", limit: {1, 1})
    assert {:ok, ["d", "c"]} = FerricStore.zrevrangebylex(k.("lex"), "+", "[c")
    assert {:ok, 3} = FerricStore.zlexcount(k.("lex"), "(a", "+")

    assert {:error, "ERR min or max not valid string range item"} =
             FerricStore.zlexcount(k.("lex"), "a", "+")
  end

  test "range removals and ZMPOP", %{k: k} do
    assert {:ok, 1} = FerricStore.zremrangebyrank(k.("z"), 0, 0)
    assert {:ok, 1} = FerricStore.zremrangebyscore(k.("z"), "(2", "+inf")
    assert {:ok, 2} = FerricStore.zremrangebylex(k.("lex"), "[c", "+")

    assert {:ok, {k.("lex"), [{"a", 0.0}, {"b", 0.0}]}} ==
             FerricStore.zmpop([k.("missing"), k.("lex")], :min, count: 2)

    assert {:ok, {k.("z"), [{"two", 2.0}]}} == FerricStore.zmpop([k.("z")], :max)
    assert {:ok, nil} = FerricStore.zmpop([k.("z")], :min)
  end
end
