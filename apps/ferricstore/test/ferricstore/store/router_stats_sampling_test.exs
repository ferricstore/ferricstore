defmodule Ferricstore.Store.RouterStatsSamplingTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Stats
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.IsolatedInstance

  test "normal keyspace misses are sampled by read_sample_rate" do
    ctx = IsolatedInstance.checkout(shard_count: 1, read_sample_rate: 10)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    before = Stats.keyspace_misses(ctx)

    for i <- 1..9 do
      assert Router.get(ctx, "sampled-miss-#{i}") == nil
    end

    assert Stats.keyspace_misses(ctx) - before == 0

    assert Router.get(ctx, "sampled-miss-10") == nil
    assert Stats.keyspace_misses(ctx) - before == 1
  end

  test "read_sample_rate one keeps keyspace misses exact" do
    ctx = IsolatedInstance.checkout(shard_count: 1, read_sample_rate: 1)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    before = Stats.keyspace_misses(ctx)

    for i <- 1..3 do
      assert Router.get(ctx, "exact-miss-#{i}") == nil
    end

    assert Stats.keyspace_misses(ctx) - before == 3
  end

  test "normal keyspace hits are batched by read_sample_rate" do
    ctx = IsolatedInstance.checkout(shard_count: 1, read_sample_rate: 3)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    key = "sampled-hit"
    assert :ok = Router.put(ctx, key, "value")

    before = Stats.keyspace_hits(ctx)

    :rand.seed(:exsss, {1, 2, 3})

    assert Router.get(ctx, key) == "value"
    assert Router.get(ctx, key) == "value"
    assert Stats.keyspace_hits(ctx) - before == 0

    assert Router.get(ctx, key) == "value"
    assert Stats.keyspace_hits(ctx) - before == 1
  end

  test "read_sample_rate one keeps keyspace hits exact" do
    ctx = IsolatedInstance.checkout(shard_count: 1, read_sample_rate: 1)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    key = "exact-hit"
    assert :ok = Router.put(ctx, key, "value")

    before = Stats.keyspace_hits(ctx)

    for _ <- 1..3 do
      assert Router.get(ctx, key) == "value"
    end

    assert Stats.keyspace_hits(ctx) - before == 3
  end
end
