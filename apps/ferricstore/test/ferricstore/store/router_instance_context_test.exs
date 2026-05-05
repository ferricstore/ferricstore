defmodule Ferricstore.Store.RouterInstanceContextTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Raft.Batcher
  alias Ferricstore.CommandTime
  alias Ferricstore.Store.Router
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Promotion
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

    assert :ok = Router.batch_put(ctx, [{key, "custom"}])
    assert "custom" == Router.get(ctx, key)
  end

  test "custom compound put does not use the default Raft batcher", %{ctx: ctx} do
    key = "router:instance:async-compound:#{System.unique_integer([:positive])}"
    field_key = CompoundKey.hash_field(key, "field")
    custom_idx = Router.shard_for(ctx, key)

    on_exit(fn -> Batcher.reset_pending(custom_idx) end)
    fill_default_async_pending(custom_idx, field_key)

    assert :ok = Router.compound_put(ctx, key, field_key, "custom", 0)
    assert "custom" == Router.compound_get(ctx, key, field_key)
  end

  test "custom compound delete does not use the default Raft batcher", %{ctx: ctx} do
    key = "router:instance:async-compound-del:#{System.unique_integer([:positive])}"
    field_key = CompoundKey.hash_field(key, "field")
    custom_idx = Router.shard_for(ctx, key)

    assert :ok = Router.compound_put(ctx, key, field_key, "before", 0)

    on_exit(fn -> Batcher.reset_pending(custom_idx) end)
    fill_default_async_pending(custom_idx, field_key)

    assert :ok = Router.compound_delete(ctx, key, field_key)
    assert nil == Router.compound_get(ctx, key, field_key)
  end

  test "custom write version survives shard process restart", %{ctx: ctx} do
    key = "router:instance:version-restart:#{System.unique_integer([:positive])}"
    idx = Router.shard_for(ctx, key)
    shard_name = elem(ctx.shard_names, idx)

    assert :ok = Router.put(ctx, key, "before")
    version_before = Router.get_version(ctx, key)
    assert version_before > 0

    shard_name
    |> Process.whereis()
    |> GenServer.stop(:normal, 5_000)

    {:ok, _pid} =
      Ferricstore.Store.Shard.start_link(
        index: idx,
        data_dir: ctx.data_dir,
        instance_ctx: ctx
      )

    assert version_before == Router.get_version(ctx, key)

    assert :ok = Router.put(ctx, key, "after")
    assert Router.get_version(ctx, key) > version_before
  end

  test "promoted routing uses stamped command time", %{ctx: ctx} do
    key = "router:instance:promoted-time:#{System.unique_integer([:positive])}"
    idx = Router.shard_for(ctx, key)
    marker = Promotion.marker_key(key)
    stamped_now = Ferricstore.HLC.now_ms() - 60_000
    marker_expire_at = stamped_now + 30_000

    assert marker_expire_at < Ferricstore.HLC.now_ms()

    :ets.insert(elem(ctx.keydir_refs, idx), {marker, "hash", marker_expire_at, 1, 0, 0, 0})
    :atomics.put(ctx.disk_pressure, idx + 1, 1)

    routed_ctx = %{ctx | name: :default}
    field_key = CompoundKey.hash_field(key, "field")

    assert :ok =
             CommandTime.with_now_ms(stamped_now, fn ->
               Router.compound_put(routed_ctx, key, field_key, "value", 0)
             end)
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
