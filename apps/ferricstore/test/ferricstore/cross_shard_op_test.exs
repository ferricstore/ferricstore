defmodule Ferricstore.CrossShardOpTest do
  use ExUnit.Case, async: false

  @moduletag :global_state

  alias Ferricstore.Commands.Generic
  alias Ferricstore.Commands.Set
  alias Ferricstore.CrossShardOp
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  @crossslot {:error, "CROSSSLOT Keys in request don't hash to the same slot"}

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  test "same-shard SMOVE keeps the direct fast path" do
    ctx = FerricStore.Instance.get(:default)
    {source, destination} = ShardHelpers.keys_on_same_shard()

    assert Router.shard_for(ctx, source) == Router.shard_for(ctx, destination)
    assert 2 == Set.handle("SADD", [source, "a", "b"], ctx)
    assert 1 == Set.handle("SMOVE", [source, destination, "a"], ctx)
    assert Set.handle("SMEMBERS", [source], ctx) == ["b"]
    assert Set.handle("SMEMBERS", [destination], ctx) == ["a"]
  end

  test "same-shard RENAME keeps the direct fast path" do
    ctx = FerricStore.Instance.get(:default)
    {source, destination} = ShardHelpers.keys_on_same_shard()

    assert :ok == Router.put(ctx, source, "value", 0)
    assert :ok == Generic.handle("RENAME", [source, destination], ctx)
    assert Router.get(ctx, source) == nil
    assert Router.get(ctx, destination) == "value"
  end

  test "durable cross-group execution rejects before invoking the callback" do
    ctx = FerricStore.Instance.get(:default)
    [first, second] = ShardHelpers.keys_on_different_shards(2)

    assert @crossslot ==
             CrossShardOp.execute(
               [{first, :write}, {second, :write}],
               fn _store -> flunk("durable cross-group callback must not run") end,
               instance: ctx
             )
  end

  test "SMOVE rejects independent Raft groups without mutation" do
    ctx = FerricStore.Instance.get(:default)
    [source, destination] = ShardHelpers.keys_on_different_shards(2)

    assert 2 == Set.handle("SADD", [source, "a", "b"], ctx)
    assert @crossslot == Set.handle("SMOVE", [source, destination, "a"], ctx)
    assert Enum.sort(Set.handle("SMEMBERS", [source], ctx)) == ["a", "b"]
    assert Set.handle("SMEMBERS", [destination], ctx) == []
  end

  test "RENAME and RENAMENX reject independent Raft groups without mutation" do
    ctx = FerricStore.Instance.get(:default)
    [source, destination] = ShardHelpers.keys_on_different_shards(2)

    assert :ok == Router.put(ctx, source, "value", 0)
    assert @crossslot == Generic.handle("RENAME", [source, destination], ctx)
    assert @crossslot == Generic.handle("RENAMENX", [source, destination], ctx)
    assert Router.get(ctx, source) == "value"
    assert Router.get(ctx, destination) == nil
  end

  test "COPY rejects independent Raft groups even when the source is missing" do
    ctx = FerricStore.Instance.get(:default)
    [source, destination] = ShardHelpers.keys_on_different_shards(2)

    assert @crossslot == Generic.handle("COPY", [source, destination], ctx)
    assert Router.get(ctx, source) == nil
    assert Router.get(ctx, destination) == nil
  end

  test "COPY rejects independent Raft groups without creating the destination" do
    ctx = FerricStore.Instance.get(:default)
    [source, destination] = ShardHelpers.keys_on_different_shards(2)

    assert :ok == Router.put(ctx, source, "value", 0)
    assert @crossslot == Generic.handle("COPY", [source, destination], ctx)
    assert Router.get(ctx, source) == "value"
    assert Router.get(ctx, destination) == nil
  end

  test "set store commands reject a write group with independent read groups" do
    ctx = FerricStore.Instance.get(:default)
    [destination, source_a, source_b] = ShardHelpers.keys_on_different_shards(3)

    assert 2 == Set.handle("SADD", [source_a, "a", "shared"], ctx)
    assert 2 == Set.handle("SADD", [source_b, "b", "shared"], ctx)

    for command <- ["SDIFFSTORE", "SINTERSTORE", "SUNIONSTORE"] do
      assert @crossslot == Set.handle(command, [destination, source_a, source_b], ctx)
      assert Set.handle("SMEMBERS", [destination], ctx) == []
      assert Enum.sort(Set.handle("SMEMBERS", [source_a], ctx)) == ["a", "shared"]
      assert Enum.sort(Set.handle("SMEMBERS", [source_b], ctx)) == ["b", "shared"]
    end
  end
end
