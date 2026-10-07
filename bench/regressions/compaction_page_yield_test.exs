defmodule Ferricstore.Store.CompactionPageYieldTest do
  @moduledoc "Benchmark-only regression coverage for the rejected yielding compactor."
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricStore.Impl
  alias Ferricstore.Commands.Hash
  alias Ferricstore.Store.{CompoundKey, Promotion, Router}
  alias Ferricstore.Store.Shard.Compound
  alias Ferricstore.Test.ShardHelpers

  setup do
    snapshot = ShardHelpers.replace_default_apply_context(promotion_threshold: 5)
    ShardHelpers.flush_all_keys()
    ctx = FerricStore.Instance.get(:default)
    key = "page-yield:#{System.unique_integer([:positive])}"
    seed(ctx, key)
    shard = Router.shard_name(ctx, Router.shard_for(ctx, key))
    ShardHelpers.eventually(fn -> GenServer.call(shard, {:promoted?, key}) end, "not promoted")
    state = :sys.get_state(shard)
    path = state.promoted_instances[key].path

    on_exit(fn ->
      ShardHelpers.restore_default_apply_context(snapshot)
      ShardHelpers.flush_all_keys()
    end)

    %{ctx: ctx, key: key, state: state, path: path}
  end

  test "writes, deletion and insertion between pages survive durable recovery", fixture do
    task = paused_compactor(fixture)

    try do
      assert_receive {:page_yielded, pid}, 5_000
      assert pid == task.pid
      store = ShardHelpers.router_store(fixture.ctx)
      assert {:ok, 0} = Impl.hset(fixture.ctx, fixture.key, %{"seed-1" => "newer"})
      assert Hash.handle("HDEL", [fixture.key, "seed-999"], store) == 1

      assert {:ok, 2} =
               Impl.hset(fixture.ctx, fixture.key, %{
                 "aaa" => "before-cursor",
                 "zzz" => "after-cursor"
               })

      send(task.pid, :resume)
      assert {:ok, _} = Task.await(task, 10_000)
      refute File.exists?(Path.join(fixture.path, "00000.log"))
      assert recovered_value(fixture, "seed-1") == "newer"
      assert recovered_value(fixture, "seed-999") == nil
      assert recovered_value(fixture, "aaa") == "before-cursor"
      assert recovered_value(fixture, "zzz") == "after-cursor"
      assert recovered_value(fixture, "seed-1200") == "seed"
    after
      Task.shutdown(task, :brutal_kill)
    end
  end

  test "interrupted compaction retains sources and newer writes recover", fixture do
    task = paused_compactor(fixture)

    try do
      assert_receive {:page_yielded, _}, 5_000

      assert {:ok, 0} =
               Impl.hset(fixture.ctx, fixture.key, %{"seed-1" => "acked-before-interruption"})

      assert Hash.handle(
               "HDEL",
               [fixture.key, "seed-999"],
               ShardHelpers.router_store(fixture.ctx)
             ) == 1

      token = Promotion.acquire_compaction_latch(fixture.state, fixture.key)

      try do
        Task.shutdown(task, :brutal_kill)
        {table, latch_key} = token
        assert :ets.lookup(table, latch_key) == [{latch_key, self()}]
      after
        Promotion.release_compaction_latch(token)
      end

      assert File.exists?(Path.join(fixture.path, "00000.log"))
      assert recovered_value(fixture, "seed-1") == "acked-before-interruption"
      assert recovered_value(fixture, "seed-999") == nil
      assert recovered_value(fixture, "seed-1200") == "seed"
    after
      Task.shutdown(task, :brutal_kill)
    end
  end

  test "delete and recreate between pages invalidates the old collection generation", fixture do
    task = paused_compactor(fixture)

    try do
      assert_receive {:page_yielded, _}, 5_000
      old_marker = marker(fixture)
      assert {:ok, 1} = Impl.del(fixture.ctx, [fixture.key])
      seed(fixture.ctx, fixture.key)
      shard = Router.shard_name(fixture.ctx, fixture.state.index)

      ShardHelpers.eventually(
        fn -> GenServer.call(shard, {:promoted?, fixture.key}) end,
        "not repromoted"
      )

      assert marker(fixture) != old_marker
      assert {:ok, 0} = Impl.hset(fixture.ctx, fixture.key, %{"seed-1" => "new-generation"})
      send(task.pid, :resume)
      assert {:error, _} = Task.await(task, 10_000)
      assert {:ok, "new-generation"} = Impl.hget(fixture.ctx, fixture.key, "seed-1")
      assert recovered_value(fixture, "seed-1") == "new-generation"
      assert recovered_value(fixture, "seed-1200") == "seed"
    after
      Task.shutdown(task, :brutal_kill)
    end
  end

  test "a competing rotation between pages cannot strand or remove the newer target", fixture do
    task = paused_compactor(fixture)

    try do
      assert_receive {:page_yielded, _}, 5_000

      assert {:ok, _} =
               Compound.compact_dedicated_result(fixture.state, fixture.key, fixture.path)

      active = Promotion.find_active(fixture.path)
      send(task.pid, :resume)
      assert {:error, _} = Task.await(task, 10_000)
      assert File.exists?(active)
      assert recovered_value(fixture, "seed-1") == "seed"
      assert recovered_value(fixture, "seed-1200") == "seed"
    after
      Task.shutdown(task, :brutal_kill)
    end
  end

  defp seed(ctx, key) do
    for fields <- Enum.chunk_every(1..1200, 64) do
      {:ok, _} = Impl.hset(ctx, key, Map.new(fields, &{"seed-#{&1}", "seed"}))
    end
  end

  defp paused_compactor(fixture) do
    parent = self()

    Task.async(fn ->
      token = Promotion.acquire_compaction_latch(fixture.state, fixture.key)

      Process.put(:ferricstore_promoted_compaction_page_yield_hook, fn _, _ ->
        unless Process.get(:yielded_once, false) do
          Process.put(:yielded_once, true)
          send(parent, {:page_yielded, self()})

          receive do
            :resume -> :ok
          after
            10_000 -> raise("page yield was not resumed")
          end
        end
      end)

      try do
        Compound.compact_dedicated_result_latched(fixture.state, fixture.key, fixture.path, token)
      after
        {table, latch_key} = token
        :ets.delete_object(table, {latch_key, self()})
      end
    end)
  end

  defp marker(fixture) do
    [row] = :ets.lookup(fixture.state.keydir, Promotion.marker_key(fixture.key))
    elem(row, 1)
  end

  defp recovered_value(fixture, field) do
    keydir = :ets.new(:recovered_compaction_page, [:set, :public])

    try do
      marker_key = Promotion.marker_key(fixture.key)
      :ets.insert(keydir, :ets.lookup(fixture.state.keydir, marker_key))

      recovered =
        Promotion.recover_promoted(
          fixture.state.shard_data_path,
          keydir,
          fixture.ctx.data_dir,
          fixture.state.index
        )

      assert Map.has_key?(recovered, fixture.key)

      case :ets.lookup(keydir, CompoundKey.hash_field(fixture.key, field)) do
        [] -> nil
        [row] -> elem(row, 1)
      end
    after
      :ets.delete(keydir)
    end
  end
end
