defmodule Ferricstore.Store.RouterInstanceContextTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.IsolatedInstance

  setup do
    ctx = IsolatedInstance.checkout(shard_count: 2)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)
    {:ok, ctx: ctx}
  end

  test "Router LMOVE uses the caller instance context", %{ctx: ctx} do
    {source, destination} = same_shard_keys(ctx)

    assert 2 = Router.list_op(ctx, source, {:rpush, ["first", "second"]})

    assert "first" = Router.list_op(ctx, source, {:lmove, destination, :left, :right})
    assert ["second"] = Router.list_op(ctx, source, {:lrange, 0, -1})
    assert ["first"] = Router.list_op(ctx, destination, {:lrange, 0, -1})
  end

  test "Router cross-shard LMOVE works inside a non-Raft instance", %{ctx: ctx} do
    {source, destination} = different_shard_keys(ctx)

    assert 2 = Router.list_op(ctx, source, {:rpush, ["first", "second"]})

    assert "first" = Router.list_op(ctx, source, {:lmove, destination, :left, :right})
    assert ["second"] = Router.list_op(ctx, source, {:lrange, 0, -1})
    assert ["first"] = Router.list_op(ctx, destination, {:lrange, 0, -1})
  end

  test "direct list creation stamps type metadata before later compound commands", %{ctx: ctx} do
    key = "router:instance:type:list:#{System.unique_integer([:positive])}"

    assert 1 = Router.list_op(ctx, key, {:rpush, ["first"]})

    assert {:error, "WRONGTYPE" <> _} =
             Ferricstore.Commands.Hash.handle("HSET", [key, "field", "value"], ctx)
  end

  test "custom batch async put does not use the default Raft batcher", %{ctx: ctx} do
    key = "router:instance:async-batch:#{System.unique_integer([:positive])}"
    custom_idx = Router.shard_for(ctx, key)

    on_exit(fn -> Batcher.reset_pending(custom_idx) end)
    fill_default_async_pending(custom_idx, key)

    assert :ok = Router.batch_async_put(ctx, [{key, "custom"}])
    assert "custom" == Router.get(ctx, key)
  end

  defp same_shard_keys(ctx) do
    base = System.unique_integer([:positive])
    default_ctx = FerricStore.Instance.get(:default)

    keys =
      for i <- 1..200 do
        "router:instance:#{base}:#{i}"
      end

    Enum.find_value(keys, fn source ->
      Enum.find_value(keys, fn
        ^source ->
          nil

        destination ->
          same_in_ctx? = Router.shard_for(ctx, source) == Router.shard_for(ctx, destination)

          same_in_default? =
            Router.shard_for(default_ctx, source) == Router.shard_for(default_ctx, destination)

          if same_in_ctx? and same_in_default?, do: {source, destination}
      end)
    end)
  end

  defp different_shard_keys(ctx) do
    base = System.unique_integer([:positive])

    keys =
      for i <- 1..200 do
        "router:instance:cross:#{base}:#{i}"
      end

    Enum.find_value(keys, fn source ->
      Enum.find_value(keys, fn
        ^source ->
          nil

        destination ->
          if Router.shard_for(ctx, source) != Router.shard_for(ctx, destination),
            do: {source, destination}
      end)
    end)
  end

  defp fill_default_async_pending(idx, key) do
    for _ <- 1..64 do
      Batcher.__inject_async_pending__(
        idx,
        make_ref(),
        [{:async, node(), {:put, key, "pending", 0}}],
        0
      )
    end
  end
end
