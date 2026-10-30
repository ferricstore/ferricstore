defmodule FerricStore.API.SortedSetAlgebraTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  setup do
    tag = "{zapi:#{System.unique_integer([:positive])}}"
    k = &(tag <> &1)
    {:ok, 2} = FerricStore.zadd(k.("z1"), [{1, "a"}, {2, "b"}])
    {:ok, 2} = FerricStore.zadd(k.("z2"), [{3, "b"}, {4, "c"}])
    %{k: k}
  end

  test "store forms answer the stored count", %{k: k} do
    assert {:ok, 3} = FerricStore.zunionstore(k.("u"), [k.("z1"), k.("z2")])

    assert {:ok, [{"a", 1.0}, {"c", 4.0}, {"b", 5.0}]} =
             FerricStore.zrange(k.("u"), 0, -1, withscores: true)

    assert {:ok, 1} =
             FerricStore.zinterstore(k.("i"), [k.("z1"), k.("z2")],
               weights: [2, 3],
               aggregate: :max
             )

    assert {:ok, [{"b", 9.0}]} = FerricStore.zrange(k.("i"), 0, -1, withscores: true)
    assert {:ok, 1} = FerricStore.zdiffstore(k.("d"), [k.("z1"), k.("z2")])
    assert {:ok, 1} = FerricStore.zrangestore(k.("r"), k.("u"), 0, 0, rev: true)
    assert {:ok, ["b"]} = FerricStore.zrange(k.("r"), 0, -1)
    assert {:ok, 2} = FerricStore.zrangestore(k.("r"), k.("u"), "(1", "+inf", by: :score)

    assert {:ok, 1} =
             FerricStore.zrangestore(k.("r"), k.("u"), "[a", "[a", by: :lex, limit: {0, 1})
  end

  test "read forms answer members, with scores on request", %{k: k} do
    assert {:ok, ["a", "c", "b"]} = FerricStore.zunion([k.("z1"), k.("z2")])
    assert {:ok, [{"b", 5.0}]} = FerricStore.zinter([k.("z1"), k.("z2")], withscores: true)

    assert {:ok, [{"b", 2.0}]} =
             FerricStore.zinter([k.("z1"), k.("z2")], aggregate: :min, withscores: true)

    assert {:ok, ["a"]} = FerricStore.zdiff([k.("z1"), k.("z2")])
    assert {:ok, 1} = FerricStore.zintercard([k.("z1"), k.("z2")])
    assert {:ok, 1} = FerricStore.zintercard([k.("z1"), k.("z2")], limit: 1)
  end

  test "errors come back as error tuples", %{k: k} do
    :ok = FerricStore.set(k.("str"), "x")
    assert {:error, "WRONGTYPE" <> _} = FerricStore.zunion([k.("z1"), k.("str")])
    assert {:error, _reason} = FerricStore.zunionstore(k.("u"), [])
  end
end
