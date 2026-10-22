defmodule Ferricstore.Commands.PromotedColdMarkerTest do
  @moduledoc """
  Memory pressure can turn any keydir row cold, including the marker row of a
  promoted collection. Each node evicts on its own, so applying a write must
  read a cold marker the same way it reads a hot one.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.MemoryGuard
  alias Ferricstore.Store.{CompoundKey, Router}
  alias Ferricstore.Test.ShardHelpers

  setup do
    %{
      ctx: FerricStore.Instance.get(:default),
      key: "{cold-marker:#{System.unique_integer([:positive])}}"
    }
  end

  test "a write to a promoted hash applies after its marker row is evicted", %{
    ctx: ctx,
    key: key
  } do
    fields = Enum.flat_map(1..150, &["f#{&1}", "v#{&1}"])
    assert 150 == Dispatcher.dispatch("HSET", [key | fields], ctx)

    ShardHelpers.eventually(
      fn -> promoted?(ctx, key) end,
      "expected #{inspect(key)} to finish promotion"
    )

    evict_marker!(ctx, key)

    assert 0 == Dispatcher.dispatch("HSET", [key, "f7", "new"], ctx)
    assert 1 == Dispatcher.dispatch("HSET", [key, "f151", "v151"], ctx)
    assert "new" == Dispatcher.dispatch("HGET", [key, "f7"], ctx)
    assert 151 == Dispatcher.dispatch("HLEN", [key], ctx)
  end

  test "a promoted hash reads its fields after its marker and members are evicted", %{
    ctx: ctx,
    key: key
  } do
    fields = Enum.flat_map(1..150, &["f#{&1}", "v#{&1}"])
    assert 150 == Dispatcher.dispatch("HSET", [key | fields], ctx)
    await_promoted!(ctx, key)

    evict_collection!(ctx, key)

    assert "v9" == Dispatcher.dispatch("HGET", [key, "f9"], ctx)
    assert ["v1", nil, "v150"] == Dispatcher.dispatch("HMGET", [key, "f1", "nope", "f150"], ctx)
    assert 1 == Dispatcher.dispatch("HEXISTS", [key, "f2"], ctx)
    assert 150 == Dispatcher.dispatch("HLEN", [key], ctx)
    assert 1 == Dispatcher.dispatch("HDEL", [key, "f3"], ctx)
    assert 1 == Dispatcher.dispatch("DEL", [key], ctx)
    assert 0 == Dispatcher.dispatch("EXISTS", [key], ctx)
  end

  test "a promoted set and sorted set read their members after eviction", %{ctx: ctx, key: key} do
    set = key <> ":s"
    zset = key <> ":z"
    assert 150 == Dispatcher.dispatch("SADD", [set | Enum.map(1..150, &"m#{&1}")], ctx)

    assert 150 ==
             Dispatcher.dispatch(
               "ZADD",
               [zset | Enum.flat_map(1..150, &["#{&1}", "m#{&1}"])],
               ctx
             )

    await_promoted!(ctx, set)
    await_promoted!(ctx, zset)

    evict_collection!(ctx, set)
    evict_collection!(ctx, zset)

    assert 1 == Dispatcher.dispatch("SISMEMBER", [set, "m4"], ctx)
    assert 1 == Dispatcher.dispatch("SREM", [set, "m4"], ctx)
    assert 149 == Dispatcher.dispatch("SCARD", [set], ctx)
    assert "2" == Dispatcher.dispatch("ZSCORE", [zset, "m2"], ctx)
    assert ["m1", "m2"] == Dispatcher.dispatch("ZRANGE", [zset, "0", "1"], ctx)
    assert 1 == Dispatcher.dispatch("ZREM", [zset, "m1"], ctx)
  end

  defp await_promoted!(ctx, key) do
    ShardHelpers.eventually(
      fn -> promoted?(ctx, key) end,
      "expected #{inspect(key)} to finish promotion"
    )
  end

  # Evicts every keydir row of the key: its marker, type and members.
  defp evict_collection!(ctx, key) do
    keydir = elem(ctx.keydir_refs, Router.shard_for(ctx, key))
    marker = CompoundKey.promotion_marker_key(key)

    evicted =
      keydir
      |> :ets.tab2list()
      |> Enum.filter(fn {row_key, value, _exp, _lfu, fid, _off, _size} ->
        value != nil and fid != :pending and
          (row_key == marker or CompoundKey.extract_redis_key(row_key) == key)
      end)
      |> Enum.map(&MemoryGuard.coldify_candidate(keydir, &1))

    assert Enum.count(evicted, &match?({:evicted, _bytes, _size}, &1)) > 150
    assert [{^marker, nil, _exp, _lfu, _fid, _off, _size}] = :ets.lookup(keydir, marker)
  end

  defp evict_marker!(ctx, key) do
    keydir = elem(ctx.keydir_refs, Router.shard_for(ctx, key))
    [row] = :ets.lookup(keydir, CompoundKey.promotion_marker_key(key))
    assert {:evicted, _bytes, _size} = MemoryGuard.coldify_candidate(keydir, row)
    assert [{_key, nil, _exp, _lfu, _fid, _off, _size}] = :ets.lookup(keydir, elem(row, 0))
  end

  defp promoted?(ctx, key),
    do: GenServer.call(Router.shard_name(ctx, Router.shard_for(ctx, key)), {:promoted?, key})
end
