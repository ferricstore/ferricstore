defmodule Ferricstore.Commands.ZSetScoreIndexProjectionTest do
  @moduledoc """
  A sorted set first written by the state machine's single-member ZADD entry
  (the native pipeline's ZADD path) has a ready score index. Writes applied
  through the WARaft segment projection, and writes to a promoted sorted set's
  own file, must keep that index current, or ZRANGE and ZCARD read a stale
  index.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    %{
      ctx: FerricStore.Instance.get(:default),
      key: "{zidx:#{System.unique_integer([:positive])}}"
    }
  end

  test "members added by a multi-member ZADD are ranged and counted", %{ctx: ctx, key: key} do
    assert [1] = zadd_single(ctx, key, 0.0, "b")
    assert 2 == Dispatcher.dispatch("ZADD", [key, "0", "a", "0", "c"], ctx)
    assert 1 == Dispatcher.dispatch("ZADD", [key, "5", "solo"], ctx)

    assert ["a", "b", "c", "solo"] == Dispatcher.dispatch("ZRANGE", [key, "0", "-1"], ctx)
    assert 4 == Dispatcher.dispatch("ZCARD", [key], ctx)
  end

  test "a member scored -0.0 counts as a tie at a bound of 0", %{ctx: ctx, key: key} do
    assert 3 == Dispatcher.dispatch("ZADD", [key, "-0.0", "zero", "1", "b", "-1", "neg"], ctx)

    assert ["b"] == Dispatcher.dispatch("ZRANGEBYSCORE", [key, "(0", "+inf"], ctx)
    assert ["zero", "neg"] == Dispatcher.dispatch("ZREVRANGEBYSCORE", [key, "0", "-inf"], ctx)
    assert ["zero", "b"] == Dispatcher.dispatch("ZRANGEBYSCORE", [key, "0", "+inf"], ctx)
    assert ["neg"] == Dispatcher.dispatch("ZREVRANGEBYSCORE", [key, "(0", "-inf"], ctx)
  end

  test "members removed by a multi-member ZREM leave the range", %{ctx: ctx, key: key} do
    assert [1] = zadd_single(ctx, key, 0.0, "a")
    assert [1] = zadd_single(ctx, key, 1.0, "b")
    assert [1] = zadd_single(ctx, key, 2.0, "c")
    assert 2 == Dispatcher.dispatch("ZREM", [key, "a", "b"], ctx)

    assert ["c"] == Dispatcher.dispatch("ZRANGE", [key, "0", "-1"], ctx)
    assert 1 == Dispatcher.dispatch("ZCARD", [key], ctx)
  end

  test "a deleted sorted set leaves no members behind", %{ctx: ctx, key: key} do
    assert [1] = zadd_single(ctx, key, 0.0, "old")
    assert 1 == Dispatcher.dispatch("DEL", [key], ctx)
    assert 2 == Dispatcher.dispatch("ZADD", [key, "1", "new", "2", "newer"], ctx)

    assert ["new", "newer"] == Dispatcher.dispatch("ZRANGE", [key, "0", "-1"], ctx)
    assert 2 == Dispatcher.dispatch("ZCARD", [key], ctx)
  end

  test "a sorted set overwritten by SET leaves no members behind", %{ctx: ctx, key: key} do
    assert [1] = zadd_single(ctx, key, 0.0, "old")
    assert :ok = Dispatcher.dispatch("SET", [key, "string"], ctx)
    assert 1 == Dispatcher.dispatch("DEL", [key], ctx)
    assert 1 == Dispatcher.dispatch("ZADD", [key, "1", "new"], ctx)

    assert ["new"] == Dispatcher.dispatch("ZRANGE", [key, "0", "-1"], ctx)
    assert 1 == Dispatcher.dispatch("ZCARD", [key], ctx)
  end

  test "a promoted sorted set's index follows batched adds and removes", %{ctx: ctx, key: key} do
    assert List.duplicate(1, 150) == zadd_single_batch(ctx, key, 1..150)

    ShardHelpers.eventually(
      fn -> promoted?(ctx, key) end,
      "expected #{inspect(key)} to finish promotion"
    )

    assert List.duplicate(1, 10) == zadd_single_batch(ctx, key, 151..160)
    assert 160 == Dispatcher.dispatch("ZCARD", [key], ctx)
    assert "m160" == List.last(Dispatcher.dispatch("ZRANGE", [key, "0", "-1"], ctx))

    removed =
      Router.pipeline_write_batch(ctx, Enum.map(1..5, &{key, {:zrem_single, key, "m#{&1}"}}))

    assert List.duplicate(1, 5) == removed
    assert 155 == Dispatcher.dispatch("ZCARD", [key], ctx)
    assert "m6" == hd(Dispatcher.dispatch("ZRANGE", [key, "0", "-1"], ctx))
  end

  defp zadd_single(ctx, key, score, member),
    do: Router.pipeline_write_batch(ctx, [{key, {:zadd_single, key, score, member}}])

  defp zadd_single_batch(ctx, key, range) do
    Router.pipeline_write_batch(
      ctx,
      Enum.map(range, &{key, {:zadd_single, key, &1 * 1.0, "m#{&1}"}})
    )
  end

  defp promoted?(ctx, key),
    do: GenServer.call(Router.shard_name(ctx, Router.shard_for(ctx, key)), {:promoted?, key})
end
