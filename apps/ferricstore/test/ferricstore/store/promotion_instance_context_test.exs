defmodule Ferricstore.Store.PromotionInstanceContextTest do
  @moduledoc false

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.HLC
  alias Ferricstore.Store.{CompoundKey, Router}
  alias Ferricstore.Store.Shard.Compound, as: ShardCompound
  alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
  alias Ferricstore.Test.IsolatedInstance

  setup do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 1_000_000,
        promotion_threshold: 1
      )

    on_exit(fn ->
      IsolatedInstance.checkin(ctx)
    end)

    {:ok, ctx: ctx}
  end

  test "promotion in custom instances does not mutate default instance accounting", %{ctx: ctx} do
    default_ctx = FerricStore.Instance.get(:default)
    default_before = keydir_binary_total(default_ctx)
    custom_before = keydir_binary_total(ctx)

    redis_key =
      "promoted_custom_instance_" <>
        String.duplicate("k", 80) <> "_#{System.unique_integer([:positive])}"

    assert :ok = put_hash_type(ctx, redis_key)

    assert :ok =
             Router.compound_put(
               ctx,
               redis_key,
               CompoundKey.hash_field(redis_key, "f1"),
               String.duplicate("a", 80),
               0
             )

    assert :ok =
             Router.compound_put(
               ctx,
               redis_key,
               CompoundKey.hash_field(redis_key, "f2"),
               String.duplicate("b", 80),
               0
             )

    shard = elem(ctx.shard_names, Router.shard_for(ctx, redis_key))
    assert GenServer.call(shard, {:promoted?, redis_key}, 30_000)
    state = :sys.get_state(shard)
    assert Map.has_key?(state.promoted_instances, redis_key)

    assert keydir_binary_total(default_ctx) == default_before
    assert keydir_binary_total(ctx) > custom_before
  end

  test "orphan compound members above the threshold do not crash or promote the shard", %{
    ctx: ctx
  } do
    redis_key = "orphan_promotion_#{System.unique_integer([:positive])}"
    first = CompoundKey.hash_field(redis_key, "f1")
    second = CompoundKey.hash_field(redis_key, "f2")
    shard = elem(ctx.shard_names, Router.shard_for(ctx, redis_key))
    original_pid = Process.whereis(shard)
    monitor_ref = Process.monitor(original_pid)

    assert :ok = Router.compound_put(ctx, redis_key, first, "value1", 0)
    assert :ok = Router.compound_put(ctx, redis_key, second, "value2", 0)
    refute GenServer.call(shard, {:promoted?, redis_key}, 5_000)
    refute_receive {:DOWN, ^monitor_ref, :process, ^original_pid, _reason}, 1_000
    assert Process.whereis(shard) == original_pid
  end

  test "new promoted instance records its initial dedicated byte size", %{ctx: ctx} do
    redis_key = "promoted_initial_size_#{System.unique_integer([:positive])}"

    assert :ok = put_hash_type(ctx, redis_key)

    assert :ok =
             Router.compound_put(
               ctx,
               redis_key,
               CompoundKey.hash_field(redis_key, "f1"),
               "value1",
               0
             )

    assert :ok =
             Router.compound_put(
               ctx,
               redis_key,
               CompoundKey.hash_field(redis_key, "f2"),
               "value2",
               0
             )

    shard = elem(ctx.shard_names, Router.shard_for(ctx, redis_key))
    assert GenServer.call(shard, {:promoted?, redis_key}, 30_000)
    state = :sys.get_state(shard)
    info = Map.fetch!(state.promoted_instances, redis_key)

    assert info.total_bytes == ShardCompound.promoted_dir_size(info.path)
    assert info.total_bytes > 0
    assert info.dead_bytes == 0
  end

  test "expiry sweep tracks dead bytes for promoted compound entries", %{ctx: ctx} do
    redis_key = "promoted_expiry_accounting_#{System.unique_integer([:positive])}"

    assert :ok = put_hash_type(ctx, redis_key)

    assert :ok =
             Router.compound_put(
               ctx,
               redis_key,
               CompoundKey.hash_field(redis_key, "f1"),
               "value1",
               0
             )

    assert :ok =
             Router.compound_put(
               ctx,
               redis_key,
               CompoundKey.hash_field(redis_key, "f2"),
               "value2",
               0
             )

    expired_key = CompoundKey.hash_field(redis_key, "expired")
    shard = elem(ctx.shard_names, Router.shard_for(ctx, redis_key))

    assert :ok =
             Router.compound_put(
               ctx,
               redis_key,
               expired_key,
               "gone",
               HLC.now_ms() - 1
             )

    assert GenServer.call(shard, {:promoted?, redis_key}, 30_000)
    assert :ets.lookup(elem(ctx.latch_refs, 0), {:promoted_compaction, redis_key}) == []
    assert :ets.lookup(elem(ctx.latch_refs, 0), :compound_promotion_shared_log) == []
    state = :sys.get_state(shard)
    before_info = Map.fetch!(state.promoted_instances, redis_key)

    after_state = ShardLifecycle.do_expiry_sweep(state)
    after_info = Map.fetch!(after_state.promoted_instances, redis_key)

    assert after_info.dead_bytes > before_info.dead_bytes
    assert ShardCompound.promoted_dir_size(after_info.path) == before_info.total_bytes
    assert :ets.lookup(state.keydir, expired_key) == []
  end

  test "all-dead promoted compaction reclaims dedicated log bytes", %{ctx: ctx} do
    redis_key = "promoted_all_dead_compaction_#{System.unique_integer([:positive])}"
    value = String.duplicate("x", 600_000)
    field1 = CompoundKey.hash_field(redis_key, "f1")
    field2 = CompoundKey.hash_field(redis_key, "f2")

    assert :ok = put_hash_type(ctx, redis_key)
    assert :ok = Router.compound_put(ctx, redis_key, field1, value, 0)
    assert :ok = Router.compound_put(ctx, redis_key, field2, value, 0)

    shard = elem(ctx.shard_names, Router.shard_for(ctx, redis_key))
    assert GenServer.call(shard, {:promoted?, redis_key}, 30_000)
    promoted_before = :sys.get_state(shard).promoted_instances |> Map.fetch!(redis_key)
    size_before = ShardCompound.promoted_dir_size(promoted_before.path)

    assert size_before > 1_000_000

    assert :ok = Router.compound_delete(ctx, redis_key, field1)
    assert :ok = Router.compound_delete(ctx, redis_key, field2)

    promoted_after = await_compacted_promoted_info(shard, redis_key)
    size_after = ShardCompound.promoted_dir_size(promoted_after.path)

    assert promoted_after.dead_bytes == 0
    assert size_after < div(size_before, 10)
  end

  defp put_hash_type(ctx, redis_key) do
    Router.compound_put(
      ctx,
      redis_key,
      CompoundKey.type_key(redis_key),
      CompoundKey.encode_type(:hash),
      0
    )
  end

  defp await_compacted_promoted_info(shard, redis_key, attempts \\ 100)

  defp await_compacted_promoted_info(shard, redis_key, attempts) when attempts > 0 do
    info = :sys.get_state(shard).promoted_instances |> Map.fetch!(redis_key)

    if info.dead_bytes == 0 do
      info
    else
      Process.sleep(20)
      await_compacted_promoted_info(shard, redis_key, attempts - 1)
    end
  end

  defp await_compacted_promoted_info(shard, redis_key, 0) do
    :sys.get_state(shard).promoted_instances |> Map.fetch!(redis_key)
  end

  defp keydir_binary_total(ctx) do
    1..ctx.shard_count
    |> Enum.reduce(0, fn idx, acc -> acc + :atomics.get(ctx.keydir_binary_bytes, idx) end)
  end
end
