defmodule Ferricstore.Store.RouterCustomInstanceWritesTest do
  @moduledoc """
  PFADD, SPOP and ZPOPMIN/ZPOPMAX through the router write to the custom
  embedded instance they are given, not to the default instance's shards.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.IsolatedInstance

  setup do
    ctx = IsolatedInstance.checkout()
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)
    %{ctx: ctx, key: "custom-instance:#{System.unique_integer([:positive])}"}
  end

  test "PFADD adds to the instance's sketch", %{ctx: ctx, key: key} do
    assert 1 == Router.pfadd(ctx, key, ["x", "y"])
    assert 2 == Dispatcher.dispatch("PFCOUNT", [key], ctx)
  end

  test "single-member SADD, SREM and ZREM write to the instance", %{ctx: ctx, key: key} do
    assert 1 == Dispatcher.dispatch("SADD", [key <> ":s", "m"], ctx)
    assert 0 == Dispatcher.dispatch("SADD", [key <> ":s", "m"], ctx)
    assert ["m"] == Dispatcher.dispatch("SMEMBERS", [key <> ":s"], ctx)
    assert 1 == Dispatcher.dispatch("SREM", [key <> ":s", "m"], ctx)
    assert 0 == Dispatcher.dispatch("SCARD", [key <> ":s"], ctx)

    assert 1 == Dispatcher.dispatch("ZADD", [key <> ":z", "1", "m"], ctx)
    assert 1 == Dispatcher.dispatch("ZREM", [key <> ":z", "m"], ctx)
    assert 0 == Dispatcher.dispatch("ZCARD", [key <> ":z"], ctx)

    default = FerricStore.Instance.get(:default)
    assert 0 == Dispatcher.dispatch("EXISTS", [key <> ":s"], default)
  end

  test "SPOP and ZPOP pop from the instance's keys", %{ctx: ctx, key: key} do
    assert 1 == Dispatcher.dispatch("SADD", [key <> ":set", "only"], ctx)
    assert "only" == Router.spop(ctx, key <> ":set", nil)
    assert 0 == Dispatcher.dispatch("SCARD", [key <> ":set"], ctx)

    assert 2 == Dispatcher.dispatch("ZADD", [key <> ":zset", "1", "a", "2", "b"], ctx)
    assert ["a", "1"] == Router.zpopmin(ctx, key <> ":zset", 1)
    assert ["b", "2"] == Router.zpopmax(ctx, key <> ":zset", 1)
    assert 0 == Dispatcher.dispatch("ZCARD", [key <> ":zset"], ctx)
  end
end
