defmodule Ferricstore.Store.ZSetTreeMaintenanceTest do
  @moduledoc """
  A tree-encoded sorted set's file goes once no set names it and Raft will
  replay nothing that reads it, and a file grown by writes is compacted once
  its frames are behind the replay floor.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state
  @moduletag timeout: 300_000

  import Ferricstore.Test.Eventually

  alias Ferricstore.Cluster.FeatureLevel
  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Store.{Promotion, Router}
  alias Ferricstore.Test.ShardHelpers

  # Checkpoints, and so the replay floor and the maintenance pass, follow
  # every write.
  @env [
    waraft_segment_projection_checkpoint_every: 1,
    waraft_segment_projection_checkpoint_min_interval_ms: 0,
    zset_tree_maintenance_interval_ms: 0,
    zset_tree_compact_min_bytes: 0
  ]

  setup do
    previous = for {key, _value} <- @env, do: {key, Application.fetch_env(:ferricstore, key)}
    Enum.each(@env, fn {key, value} -> Application.put_env(:ferricstore, key, value) end)

    isolated = ShardHelpers.setup_isolated_data_dir()
    snapshot = ShardHelpers.replace_default_apply_context(promotion_threshold: 16)

    on_exit(fn ->
      ShardHelpers.restore_default_apply_context(snapshot)
      ShardHelpers.teardown_isolated_data_dir(isolated)

      Enum.each(previous, fn
        {key, :error} -> Application.delete_env(:ferricstore, key)
        {key, {:ok, value}} -> Application.put_env(:ferricstore, key, value)
      end)
    end)

    ctx = FerricStore.Instance.get(:default)
    assert {:ok, %{active: 9}} = FeatureLevel.raise(ctx, 9)
    {:ok, ctx: ctx}
  end

  defp cmd(ctx, name, args), do: Dispatcher.dispatch(name, args, ctx)

  defp pairs(prefix, count),
    do: Enum.flat_map(1..count, fn i -> ["#{i}", "#{prefix}#{i}"] end)

  defp tree_dir(ctx, key),
    do: Promotion.dedicated_path(ctx.data_dir, Router.shard_for(ctx, key), :zset_tree, key)

  defp files(ctx, key) do
    ctx
    |> tree_dir(key)
    |> Path.join("*")
    |> Path.wildcard()
    |> Enum.map(&Path.basename/1)
    |> Enum.sort()
  end

  defp trees(ctx, key), do: Enum.filter(files(ctx, key), &String.ends_with?(&1, ".tree"))

  # A clean stop persists each shard's replay projection at its applied
  # position, so the replay floor moves up to it.
  defp restart_storage(ctx) do
    assert :ok = WARaftBackend.stop()
    assert :ok = WARaftBackend.start(ctx)
    assert :ok = ShardHelpers.wait_default_pipeline_ready()
  end

  defp floor(ctx, key) do
    {:raft_log_pos, index, _term} =
      :ferricstore_waraft_backend
      |> :wa_raft_storage.registered_name(Router.shard_for(ctx, key) + 1)
      |> :wa_raft_storage.status()
      |> Keyword.fetch!(:segment_projection_position)

    index
  end

  # A write to the key's shard runs the next maintenance pass.
  defp nudge(ctx, key) do
    :ok = cmd(ctx, "SET", [key <> ":nudge", "x"])
    # Leave the checkpoint room to finish between writes.
    Process.sleep(50)
  end

  test "a deleted set's file goes once the replay floor passes the delete", %{ctx: ctx} do
    key = "{gc}"
    assert 30 = cmd(ctx, "ZADD", [key | pairs("m", 30)])
    assert [_tree] = trees(ctx, key)

    assert 1 = cmd(ctx, "DEL", [key])
    restart_storage(ctx)

    assert_eventually(
      fn ->
        nudge(ctx, key)
        assert files(ctx, key) == []
        refute File.exists?(tree_dir(ctx, key))
      end,
      timeout: 60_000
    )

    assert 0 = cmd(ctx, "ZCARD", [key])
  end

  test "the file of a set that expired goes once the key holds a new set", %{ctx: ctx} do
    key = "{expired}"
    assert 30 = cmd(ctx, "ZADD", [key | pairs("old", 30)])
    assert [old] = trees(ctx, key)
    assert 1 = cmd(ctx, "PEXPIRE", [key, "100"])
    Process.sleep(250)

    assert 30 = cmd(ctx, "ZADD", [key | pairs("new", 30)])

    # A pass finds the old file no set names and records the position; once a
    # restart has moved the floor past that, the next pass removes it.
    for _ <- 1..2 do
      restart_storage(ctx)
      nudge(ctx, key)
    end

    assert_eventually(
      fn ->
        nudge(ctx, key)
        assert [new] = trees(ctx, key)
        assert new != old
        refute Enum.any?(files(ctx, key), &String.ends_with?(&1, ".removed"))
      end,
      timeout: 60_000
    )

    assert 30 = cmd(ctx, "ZCARD", [key])
    assert ["new1", "new2"] = cmd(ctx, "ZRANGE", [key, "0", "1"])
  end

  # Each write appends a frame of the pages it changed; compaction folds them
  # without waiting for the replay floor, so the file stays near the size of
  # the set (150 frames would take over a megabyte).
  test "a set written many times keeps a small file", %{ctx: ctx} do
    key = "{grow}"
    assert 30 = cmd(ctx, "ZADD", [key | pairs("m", 30)])
    assert [tree] = trees(ctx, key)
    path = Path.join(tree_dir(ctx, key), tree)

    for _ <- 1..150, do: cmd(ctx, "ZINCRBY", [key, "1", "m1"])

    assert_eventually(fn ->
      nudge(ctx, key)
      assert File.stat!(path).size < 100_000
    end)

    assert "151" = cmd(ctx, "ZSCORE", [key, "m1"])
    assert 30 = cmd(ctx, "ZCARD", [key])
    assert ["m2", "m3"] = cmd(ctx, "ZRANGE", [key, "0", "1"])
  end

  test "a compacted set comes back whole after the storage is killed", %{ctx: ctx} do
    key = "{crash}"
    assert 30 = cmd(ctx, "ZADD", [key | pairs("m", 30)])
    assert [tree] = trees(ctx, key)
    path = Path.join(tree_dir(ctx, key), tree)
    for _ <- 1..150, do: cmd(ctx, "ZINCRBY", [key, "1", "m1"])

    assert_eventually(fn ->
      nudge(ctx, key)
      assert File.stat!(path).size < 100_000
    end)

    # The floor moves past everything so far and stays there. Compaction folds
    # the writes below into bases ahead of it, so after the kill each replayed
    # entry rewinds through the undo log.
    Application.put_env(:ferricstore, :waraft_segment_projection_checkpoint_every, 1_000_000_000)
    restart_storage(ctx)
    floor = floor(ctx, key)
    assert floor > 150

    for i <- 1..40 do
      cmd(ctx, "ZINCRBY", [key, "1", "m2"])
      cmd(ctx, "ZADD", [key, "#{i}", "late#{i}"])
      if rem(i, 10) == 0, do: cmd(ctx, "ZREM", [key, "late#{i - 1}"])
    end

    assert floor(ctx, key) == floor
    expected = cmd(ctx, "ZRANGE", [key, "0", "-1", "WITHSCORES"])

    storage =
      :wa_raft_storage.registered_name(
        :ferricstore_waraft_backend,
        Router.shard_for(ctx, key) + 1
      )

    pid = Process.whereis(storage)
    monitor = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :killed}, 5_000

    assert_eventually(
      fn ->
        assert is_pid(Process.whereis(storage))
        assert ^expected = cmd(ctx, "ZRANGE", [key, "0", "-1", "WITHSCORES"])
      end,
      timeout: 30_000
    )

    # Once the shard takes writes again, its replay is done.
    assert_eventually(
      fn ->
        _ = cmd(ctx, "ZADD", [key, "1", "after-crash"])
        assert "1" = cmd(ctx, "ZSCORE", [key, "after-crash"])
      end,
      timeout: 30_000
    )

    assert 67 = cmd(ctx, "ZCARD", [key])

    assert ^expected =
             ctx
             |> cmd("ZRANGE", [key, "0", "-1", "WITHSCORES"])
             |> Enum.chunk_every(2)
             |> Enum.reject(&match?(["after-crash", _score], &1))
             |> List.flatten()
  end
end
