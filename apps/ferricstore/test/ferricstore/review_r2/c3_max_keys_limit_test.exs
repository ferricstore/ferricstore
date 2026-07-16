defmodule Ferricstore.ReviewR2.C3MaxKeysLimitTest do
  use ExUnit.Case, async: false

  alias Ferricstore.CrossShardOp
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.IsolatedInstance

  setup do
    ctx = IsolatedInstance.checkout(shard_count: 2)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)
    {:ok, ctx: ctx}
  end

  test "standalone coordination rejects more than 20 keys", %{ctx: ctx} do
    keys_with_roles = keys_across_shards(ctx, 21)

    assert {:error, message} =
             CrossShardOp.execute(
               keys_with_roles,
               fn _store -> flunk("oversized standalone callback must not run") end,
               instance: ctx
             )

    assert message =~ "max key limit"
  end

  test "standalone coordination accepts 20 keys", %{ctx: ctx} do
    keys_with_roles = keys_across_shards(ctx, 20)

    assert :executed ==
             CrossShardOp.execute(
               keys_with_roles,
               fn _store -> :executed end,
               instance: ctx
             )
  end

  test "same-shard operations bypass the standalone limit", %{ctx: ctx} do
    keys_with_roles =
      for index <- 1..30 do
        {key_for_shard(ctx, 0, "same-#{index}"), :write}
      end

    assert :same_shard_ok ==
             CrossShardOp.execute(
               keys_with_roles,
               fn _store -> :same_shard_ok end,
               instance: ctx
             )
  end

  defp keys_across_shards(ctx, count) do
    for index <- 1..count do
      shard_index = rem(index, 2)
      {key_for_shard(ctx, shard_index, "cross-#{index}"), :read}
    end
  end

  defp key_for_shard(ctx, shard_index, suffix) do
    Enum.find_value(0..10_000, fn candidate ->
      key = "cross-shard-limit:#{suffix}:#{candidate}"
      if Router.shard_for(ctx, key) == shard_index, do: key
    end)
  end
end
