defmodule Ferricstore.Store.RouterPromotedReadPathTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Store.{CompoundKey, Router}
  alias Ferricstore.Test.IsolatedInstance

  setup do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 1,
        promotion_threshold: 1
      )

    handler_id = {:router_promoted_read_path, self(), make_ref()}
    parent = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :bitcask, :pread_corrupt],
        fn event, measurements, metadata, _config ->
          send(parent, {:pread_corrupt, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      IsolatedInstance.checkin(ctx)
    end)

    {:ok, ctx: ctx}
  end

  test "compound_get skips the shared-log cold probe for promoted data fields", %{ctx: ctx} do
    redis_key = unique_key("router-promoted-get")
    field = CompoundKey.hash_field(redis_key, "f2")
    promote_hash(ctx, redis_key)
    assert :ok = Router.compound_put(ctx, redis_key, field, "cc", 0)

    without_shared_log(ctx, fn ->
      assert "cc" == Router.compound_get(ctx, redis_key, field)

      refute_received {:pread_corrupt, [:ferricstore, :bitcask, :pread_corrupt], _measurements,
                       _metadata}
    end)
  end

  test "compound_get_meta skips the shared-log cold probe for promoted data fields", %{ctx: ctx} do
    redis_key = unique_key("router-promoted-get-meta")
    field = CompoundKey.hash_field(redis_key, "f2")
    promote_hash(ctx, redis_key)
    assert :ok = Router.compound_put(ctx, redis_key, field, "cc", 0)

    without_shared_log(ctx, fn ->
      assert {"cc", 0} == Router.compound_get_meta(ctx, redis_key, field)

      refute_received {:pread_corrupt, [:ferricstore, :bitcask, :pread_corrupt], _measurements,
                       _metadata}
    end)
  end

  test "compound_batch_get skips the shared-log cold probe for promoted data fields", %{ctx: ctx} do
    redis_key = unique_key("router-promoted-batch-get")
    fields = [CompoundKey.hash_field(redis_key, "f1"), CompoundKey.hash_field(redis_key, "f2")]
    promote_hash(ctx, redis_key)
    assert :ok = Router.compound_put(ctx, redis_key, Enum.at(fields, 0), "cc", 0)
    assert :ok = Router.compound_put(ctx, redis_key, Enum.at(fields, 1), "dd", 0)

    without_shared_log(ctx, fn ->
      assert ["cc", "dd"] == Router.compound_batch_get(ctx, redis_key, fields)

      refute_received {:pread_corrupt, [:ferricstore, :bitcask, :pread_corrupt], _measurements,
                       _metadata}
    end)
  end

  test "routed compound batches preserve order and use promoted cold storage", %{ctx: ctx} do
    redis_key = unique_key("router-promoted-routed-batch-get")
    type_key = CompoundKey.type_key(redis_key)
    field = CompoundKey.hash_field(redis_key, "f2")
    promote_hash(ctx, redis_key)
    assert :ok = Router.compound_put(ctx, redis_key, field, "cc", 0)

    without_shared_log(ctx, fn ->
      assert ["hash", "cc", "hash"] ==
               Router.compound_batch_get_on_route_keys(ctx, [
                 {redis_key, type_key},
                 {redis_key, field},
                 {redis_key, type_key}
               ])

      refute_received {:pread_corrupt, [:ferricstore, :bitcask, :pread_corrupt], _measurements,
                       _metadata}
    end)
  end

  test "compound scan pages read a bounded promoted cold window", %{ctx: ctx} do
    redis_key = unique_key("router-promoted-scan-page")
    promote_hash(ctx, redis_key)

    without_shared_log(ctx, fn ->
      assert {:ok, {{:after, "f1"}, [{"f1", "aa"}]}} =
               Router.compound_scan_page(
                 ctx,
                 redis_key,
                 CompoundKey.hash_prefix(redis_key),
                 0,
                 1,
                 nil,
                 false
               )

      assert {:ok, {0, [{"f2", "bb"}]}} =
               Router.compound_scan_page(
                 ctx,
                 redis_key,
                 CompoundKey.hash_prefix(redis_key),
                 {:after, "f1"},
                 1,
                 nil,
                 false
               )

      refute_received {:pread_corrupt, [:ferricstore, :bitcask, :pread_corrupt], _measurements,
                       _metadata}
    end)
  end

  test "compound_batch_get_meta skips the shared-log cold probe for promoted data fields", %{
    ctx: ctx
  } do
    redis_key = unique_key("router-promoted-batch-get-meta")
    fields = [CompoundKey.hash_field(redis_key, "f1"), CompoundKey.hash_field(redis_key, "f2")]
    promote_hash(ctx, redis_key)
    assert :ok = Router.compound_put(ctx, redis_key, Enum.at(fields, 0), "cc", 0)
    assert :ok = Router.compound_put(ctx, redis_key, Enum.at(fields, 1), "dd", 0)

    without_shared_log(ctx, fn ->
      assert [{"cc", 0}, {"dd", 0}] == Router.compound_batch_get_meta(ctx, redis_key, fields)

      refute_received {:pread_corrupt, [:ferricstore, :bitcask, :pread_corrupt], _measurements,
                       _metadata}
    end)
  end

  defp promote_hash(ctx, redis_key) do
    assert :ok =
             Router.compound_put(
               ctx,
               redis_key,
               CompoundKey.type_key(redis_key),
               "hash",
               0
             )

    assert :ok =
             Router.compound_put(
               ctx,
               redis_key,
               CompoundKey.hash_field(redis_key, "f1"),
               "aa",
               0
             )

    assert :ok =
             Router.compound_put(
               ctx,
               redis_key,
               CompoundKey.hash_field(redis_key, "f2"),
               "bb",
               0
             )

    shard = elem(ctx.shard_names, Router.shard_for(ctx, redis_key))

    Ferricstore.Test.ShardHelpers.eventually(
      fn -> Map.has_key?(:sys.get_state(shard).promoted_instances, redis_key) end,
      "expected #{inspect(redis_key)} to be promoted"
    )
  end

  defp without_shared_log(ctx, fun) do
    Process.sleep(200)
    drain_pread_corrupt()

    shared_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(0)
      |> Path.join("00000.log")

    moved_path = shared_path <> ".hidden"
    File.rename!(shared_path, moved_path)

    try do
      fun.()
    after
      File.rename!(moved_path, shared_path)
    end
  end

  defp drain_pread_corrupt do
    receive do
      {:pread_corrupt, [:ferricstore, :bitcask, :pread_corrupt], _measurements, _metadata} ->
        drain_pread_corrupt()
    after
      0 -> :ok
    end
  end

  defp unique_key(prefix), do: "#{prefix}:#{System.unique_integer([:positive])}"
end
