defmodule Ferricstore.Store.PipelinePlannerTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.PipelinePlanner
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.IsolatedInstance

  setup do
    ctx = IsolatedInstance.checkout(shard_count: 4)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)
    {:ok, ctx: ctx}
  end

  test "plans lookup keys with the same shard and keydir as Router", %{ctx: ctx} do
    keys = ["plain-a", "plain-b", "tag:{42}:a", "tag:{42}:b"]
    plan = PipelinePlanner.plan_lookup_keys(ctx, keys)

    assert PipelinePlanner.original_keys(plan) == keys
    assert PipelinePlanner.lookup_keys(plan) == keys

    for {entry, key} <- Enum.zip(plan, keys) do
      shard_index = Router.shard_for(ctx, key)

      assert PipelinePlanner.original_key(entry) == key
      assert PipelinePlanner.lookup_key(entry) == key
      assert PipelinePlanner.shard_index(entry) == shard_index
      assert PipelinePlanner.keydir(entry) == Router.resolve_keydir(ctx, shard_index)
    end

    [_, _, tagged_a, tagged_b] = plan
    assert PipelinePlanner.shard_index(tagged_a) == PipelinePlanner.shard_index(tagged_b)
  end

  test "plans namespaced client keys while routing by the stored lookup key", %{ctx: ctx} do
    namespace = "tenant:alpha:"
    keys = ["plain-a", "tag:{same}:1", "tag:{same}:2"]
    plan = PipelinePlanner.plan_keys(ctx, keys, namespace)

    assert PipelinePlanner.original_keys(plan) == keys
    assert PipelinePlanner.lookup_keys(plan) == Enum.map(keys, &(namespace <> &1))

    for {entry, key} <- Enum.zip(plan, keys) do
      lookup_key = namespace <> key
      shard_index = Router.shard_for(ctx, lookup_key)

      assert PipelinePlanner.original_key(entry) == key
      assert PipelinePlanner.lookup_key(entry) == lookup_key
      assert PipelinePlanner.shard_index(entry) == shard_index
      assert PipelinePlanner.keydir(entry) == Router.resolve_keydir(ctx, shard_index)
    end
  end

  test "planned batch GET matches the existing key-list API for hot hits and misses", %{ctx: ctx} do
    assert :ok = Router.put(ctx, "tenant:exists", "value-1")
    assert :ok = Router.put(ctx, "tenant:other", "value-2")

    keys = ["exists", "missing", "other"]
    lookup_keys = Enum.map(keys, &("tenant:" <> &1))
    plan = PipelinePlanner.plan_keys(ctx, keys, "tenant:")

    assert Router.batch_get_planned(ctx, plan) == ["value-1", nil, "value-2"]
    assert Router.batch_get_planned(ctx, plan) == Router.batch_get(ctx, lookup_keys)
  end

  test "planned file-ref batch GET matches the existing key-list API for hot hits and misses", %{
    ctx: ctx
  } do
    assert :ok = Router.put(ctx, "tenant:fileref-exists", "value-1")
    assert :ok = Router.put(ctx, "tenant:fileref-other", "value-2")

    keys = ["fileref-exists", "fileref-missing", "fileref-other"]
    lookup_keys = Enum.map(keys, &("tenant:" <> &1))
    plan = PipelinePlanner.plan_keys(ctx, keys, "tenant:")

    assert Router.batch_get_with_deferred_blob_file_refs_planned(ctx, plan, 1024) ==
             ["value-1", nil, "value-2"]

    assert Router.batch_get_with_deferred_blob_file_refs_planned(ctx, plan, 1024) ==
             Router.batch_get_with_deferred_blob_file_refs(ctx, lookup_keys, 1024)
  end
end
