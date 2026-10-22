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

  defp evict_marker!(ctx, key) do
    keydir = elem(ctx.keydir_refs, Router.shard_for(ctx, key))
    [row] = :ets.lookup(keydir, CompoundKey.promotion_marker_key(key))
    assert {:evicted, _bytes, _size} = MemoryGuard.coldify_candidate(keydir, row)
    assert [{_key, nil, _exp, _lfu, _fid, _off, _size}] = :ets.lookup(keydir, elem(row, 0))
  end

  defp promoted?(ctx, key),
    do: GenServer.call(Router.shard_name(ctx, Router.shard_for(ctx, key)), {:promoted?, key})
end
