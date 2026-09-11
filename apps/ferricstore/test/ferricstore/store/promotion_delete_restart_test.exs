defmodule Ferricstore.Store.PromotionDeleteRestartTest do
  use ExUnit.Case, async: false

  @moduletag :global_state
  @moduletag :shard_kill
  @moduletag timeout: 180_000

  alias Ferricstore.Commands.{Hash, Set, SortedSet, Strings}
  alias Ferricstore.LatencyTrace
  alias Ferricstore.Raft.Backend
  alias Ferricstore.Store.{CompoundKey, Promotion, Router}
  alias Ferricstore.Test.ShardHelpers

  setup do
    old_threshold = Application.get_env(:ferricstore, :promotion_threshold)
    old_hook = Application.get_env(:ferricstore, :compound_promotion_worker_test_hook)
    Application.put_env(:ferricstore, :promotion_threshold, 1)

    isolated = ShardHelpers.setup_isolated_data_dir()

    on_exit(fn ->
      restore_env(:compound_promotion_worker_test_hook, old_hook)
      restore_env(:promotion_threshold, old_threshold)
      ShardHelpers.teardown_isolated_data_dir(isolated)
    end)

    {:ok, isolated: isolated}
  end

  test "public DEL waits for an active promotion and remains deleted after restart", %{
    isolated: isolated
  } do
    store = ShardHelpers.router_store()
    redis_key = "promotion-delete-restart-#{System.unique_integer([:positive])}"
    ctx = FerricStore.Instance.get(:default)
    shard_index = Router.shard_for(ctx, redis_key)
    shard = Router.shard_name(ctx, shard_index)
    shard_pid = Process.whereis(shard)
    shard_monitor = Process.monitor(shard_pid)
    test_pid = self()

    Application.put_env(:ferricstore, :compound_promotion_worker_test_hook, fn
      ^redis_key ->
        send(test_pid, {:promotion_worker_paused, self()})

        receive do
          :continue_promotion -> :ok
        end

      _other_key ->
        :ok
    end)

    assert 2 == Hash.handle("HSET", [redis_key, "first", "one", "second", "two"], store)
    assert_receive {:promotion_worker_paused, worker_pid}, 5_000

    latch_table = elem(ctx.latch_refs, shard_index)

    assert %{
             compound_promotion_worker: %{
               pid: ^worker_pid,
               latch_token: {^latch_table, {:promoted_compaction, ^redis_key}}
             }
           } = :sys.get_state(shard)

    assert [{{:promoted_compaction, ^redis_key}, ^worker_pid}] =
             :ets.lookup(latch_table, {:promoted_compaction, redis_key})

    delete_task = Task.async(fn -> Strings.handle("DEL", [redis_key], store) end)

    try do
      assert nil == Task.yield(delete_task, 50)
    after
      send(worker_pid, :continue_promotion)
    end

    assert 1 == Task.await(delete_task, 30_000)
    Application.delete_env(:ferricstore, :compound_promotion_worker_test_hook)

    assert :ok =
             ShardHelpers.eventually(
               fn ->
                 state = :sys.get_state(shard)

                 state.compound_promotion_worker == nil and
                   not Map.has_key?(state.compound_promotion_pending, redis_key) and
                   not Map.has_key?(state.promoted_instances, redis_key) and
                   :ets.lookup(state.keydir, Promotion.marker_key(redis_key)) == []
               end,
               "DEL should retire the completed promotion"
             )

    assert 0 == Hash.handle("HLEN", [redis_key], store)
    refute_receive {:DOWN, ^shard_monitor, :process, ^shard_pid, _reason}, 100

    dedicated_path =
      Promotion.dedicated_path(isolated.tmp_dir, shard_index, :hash, redis_key)

    refute File.dir?(dedicated_path)

    :ok = ShardHelpers.restart_current_data_dir(isolated)

    restarted_ctx = FerricStore.Instance.get(:default)
    restarted_store = ShardHelpers.router_store()
    restarted_shard = Router.shard_name(restarted_ctx, shard_index)
    restarted_state = :sys.get_state(restarted_shard)

    assert 0 == Hash.handle("HLEN", [redis_key], restarted_store)
    assert [] == :ets.lookup(restarted_state.keydir, CompoundKey.type_key(redis_key))
    assert [] == :ets.lookup(restarted_state.keydir, Promotion.marker_key(redis_key))
    refute File.dir?(dedicated_path)
  end

  test "traced compound batch waits for an active promotion", %{isolated: isolated} do
    store = ShardHelpers.router_store()
    redis_key = "promotion-traced-batch-#{System.unique_integer([:positive])}"
    ctx = FerricStore.Instance.get(:default)
    shard_index = Router.shard_for(ctx, redis_key)
    shard = Router.shard_name(ctx, shard_index)
    shard_pid = Process.whereis(shard)
    shard_monitor = Process.monitor(shard_pid)
    test_pid = self()

    Application.put_env(:ferricstore, :compound_promotion_worker_test_hook, fn
      ^redis_key ->
        send(test_pid, {:promotion_worker_paused, self()})

        receive do
          :continue_promotion -> :ok
        end

      _other_key ->
        :ok
    end)

    assert 2 == Hash.handle("HSET", [redis_key, "first", "one", "second", "two"], store)
    assert_receive {:promotion_worker_paused, worker_pid}, 5_000

    batch_task =
      Task.async(fn ->
        previous_trace = LatencyTrace.start(%{})

        try do
          Backend.write_batch(shard_index, [
            {:compound_delete_prefix, CompoundKey.hash_prefix(redis_key)},
            {:compound_delete, CompoundKey.type_key(redis_key)}
          ])
        after
          LatencyTrace.finish(previous_trace)
        end
      end)

    early_result = Task.yield(batch_task, 50)
    send(worker_pid, :continue_promotion)

    batch_result =
      case early_result do
        nil -> Task.await(batch_task, 30_000)
        {:ok, result} -> result
      end

    assert nil == early_result
    assert {:ok, [:ok, :ok]} == batch_result
    Application.delete_env(:ferricstore, :compound_promotion_worker_test_hook)

    assert :ok =
             ShardHelpers.eventually(
               fn ->
                 state = :sys.get_state(shard)

                 state.compound_promotion_worker == nil and
                   not Map.has_key?(state.compound_promotion_pending, redis_key) and
                   not Map.has_key?(state.promoted_instances, redis_key) and
                   :ets.lookup(state.keydir, Promotion.marker_key(redis_key)) == []
               end,
               "traced batch should retire the completed promotion"
             )

    assert 0 == Hash.handle("HLEN", [redis_key], store)
    refute_receive {:DOWN, ^shard_monitor, :process, ^shard_pid, _reason}, 100

    dedicated_path =
      Promotion.dedicated_path(isolated.tmp_dir, shard_index, :hash, redis_key)

    refute File.dir?(dedicated_path)
  end

  @tag :final_promoted_collection_delete
  test "deleting the final promoted hash fields retires storage before restart", %{
    isolated: isolated
  } do
    store = ShardHelpers.router_store()
    redis_key = "promotion-hdel-restart-#{System.unique_integer([:positive])}"
    ctx = FerricStore.Instance.get(:default)
    shard_index = Router.shard_for(ctx, redis_key)
    shard = Router.shard_name(ctx, shard_index)
    marker_key = Promotion.marker_key(redis_key)
    dedicated_path = Promotion.dedicated_path(isolated.tmp_dir, shard_index, :hash, redis_key)

    assert 2 == Hash.handle("HSET", [redis_key, "first", "one", "second", "two"], store)
    assert GenServer.call(shard, {:promoted?, redis_key}, 30_000)
    assert File.dir?(dedicated_path)

    assert 2 == Hash.handle("HDEL", [redis_key, "first", "second"], store)

    assert :ok =
             ShardHelpers.eventually(
               fn ->
                 state = :sys.get_state(shard)

                 Hash.handle("HLEN", [redis_key], store) == 0 and
                   :ets.lookup(state.keydir, marker_key) == [] and
                   not Map.has_key?(state.promoted_instances, redis_key) and
                   not File.dir?(dedicated_path)
               end,
               "final HDEL should retire its promoted collection"
             )

    :ok = ShardHelpers.restart_current_data_dir(isolated)

    restarted_store = ShardHelpers.router_store()
    assert 0 == Hash.handle("HLEN", [redis_key], restarted_store)
    assert {:ok, "none"} = FerricStore.type(redis_key)
    refute File.dir?(dedicated_path)
  end

  @tag :final_promoted_collection_delete
  test "deleting final promoted set and sorted-set members retires storage before restart", %{
    isolated: isolated
  } do
    store = ShardHelpers.router_store()
    suffix = System.unique_integer([:positive])
    set_key = "promotion-srem-restart-#{suffix}"
    zset_key = "promotion-zrem-restart-#{suffix}"
    ctx = FerricStore.Instance.get(:default)

    assert 2 == Set.handle("SADD", [set_key, "first", "second"], store)
    assert 2 == SortedSet.handle("ZADD", [zset_key, "1", "first", "2", "second"], store)

    collections = [{set_key, :set}, {zset_key, :zset}]

    Enum.each(collections, fn {redis_key, type} ->
      shard_index = Router.shard_for(ctx, redis_key)
      shard = Router.shard_name(ctx, shard_index)
      assert GenServer.call(shard, {:promoted?, redis_key}, 30_000)

      dedicated_path = Promotion.dedicated_path(isolated.tmp_dir, shard_index, type, redis_key)
      assert File.dir?(dedicated_path)
    end)

    assert 2 == Set.handle("SREM", [set_key, "first", "second"], store)
    assert 2 == SortedSet.handle("ZREM", [zset_key, "first", "second"], store)

    Enum.each(collections, fn {redis_key, type} ->
      shard_index = Router.shard_for(ctx, redis_key)
      shard = Router.shard_name(ctx, shard_index)
      marker_key = Promotion.marker_key(redis_key)
      dedicated_path = Promotion.dedicated_path(isolated.tmp_dir, shard_index, type, redis_key)

      assert :ok =
               ShardHelpers.eventually(
                 fn ->
                   state = :sys.get_state(shard)

                   :ets.lookup(state.keydir, marker_key) == [] and
                     not Map.has_key?(state.promoted_instances, redis_key) and
                     not File.dir?(dedicated_path)
                 end,
                 "final #{type} removal should retire its promoted collection"
               )
    end)

    :ok = ShardHelpers.restart_current_data_dir(isolated)
    restarted_store = ShardHelpers.router_store()

    assert 0 == Set.handle("SCARD", [set_key], restarted_store)
    assert 0 == SortedSet.handle("ZCARD", [zset_key], restarted_store)
    assert {:ok, "none"} = FerricStore.type(set_key)
    assert {:ok, "none"} = FerricStore.type(zset_key)
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
