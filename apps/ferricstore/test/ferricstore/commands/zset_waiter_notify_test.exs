defmodule Ferricstore.Commands.ZSetWaiterNotifyTest do
  @moduledoc """
  BZPOPMIN, BZPOPMAX and BZMPOP wait in `Ferricstore.Waiters`. Every node's
  state machine wakes them when it applies a write that gives the sorted set
  members, as it does for list pushes.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.ShardHelpers
  alias Ferricstore.Waiters

  defp wait(key) do
    :ok = Waiters.register(key, self(), 0)
    on_exit(fn -> Waiters.unregister(key, self()) end)
  end

  test "ZADD, ZINCRBY and a STORE into the key wake its waiter" do
    ctx = FerricStore.Instance.get(:default)
    tag = "{zwake:#{System.unique_integer([:positive])}}"
    d = fn command, args -> Dispatcher.dispatch(command, args, ctx) end

    for {key, write} <- [
          {tag <> "zadd", fn key -> d.("ZADD", [key, "1", "a"]) end},
          {tag <> "zincrby", fn key -> d.("ZINCRBY", [key, "2", "a"]) end},
          {tag <> "store",
           fn key ->
             1 = d.("ZADD", [tag <> "source", "1", "a"])
             1 = d.("ZUNIONSTORE", [key, "1", tag <> "source"])
           end}
        ] do
      wait(key)
      write.(key)
      assert_receive {:waiter_notify, ^key}, 1_000
    end
  end

  # A transaction applies through the cross-shard pending path, which must
  # wake waiters once it publishes, as single writes do.
  test "pushes, an LMOVE and a ZADD inside a transaction wake their waiters" do
    tag = "{zwake-tx:#{System.unique_integer([:positive])}}"
    list = tag <> "list"
    zset = tag <> "zset"
    moved = tag <> "moved"
    for key <- [list, zset, moved], do: wait(key)

    queue =
      for {command, args} <- [
            {"LPUSH", [list, "a"]},
            {"ZADD", [zset, "1", "a"]},
            {"RPUSH", [tag <> "source", "x"]},
            {"LMOVE", [tag <> "source", moved, "LEFT", "LEFT"]}
          ] do
        {:ok, prepared} = Ferricstore.Commands.PreparedCommand.prepare(command, args)
        prepared
      end

    assert [1, 1, 1, "x"] == Ferricstore.Transaction.Coordinator.execute(queue, %{}, nil)
    for key <- [list, zset, moved], do: assert_receive({:waiter_notify, ^key}, 1_000)
  end

  test "a removal does not wake a waiter" do
    ctx = FerricStore.Instance.get(:default)
    key = "{zwake:#{System.unique_integer([:positive])}}rem"
    2 = Dispatcher.dispatch("ZADD", [key, "1", "a", "2", "b"], ctx)
    wait(key)
    1 = Dispatcher.dispatch("ZREM", [key, "a"], ctx)
    refute_receive {:waiter_notify, ^key}, 200
  end

  describe "from feature level 10" do
    setup do
      isolated = ShardHelpers.setup_isolated_data_dir()
      on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
      ctx = FerricStore.Instance.get(:default)
      assert {:ok, %{active: 10}} = Ferricstore.Cluster.FeatureLevel.raise(ctx, 10)
      %{ctx: ctx}
    end

    # The STORE forms run as one-command transactions there.
    test "a STORE into the key wakes its waiter", %{ctx: ctx} do
      tag = "{zwake10:#{System.unique_integer([:positive])}}"
      key = tag <> "store"
      1 = Dispatcher.dispatch("ZADD", [tag <> "source", "1", "a"], ctx)
      wait(key)
      1 = Dispatcher.dispatch("ZUNIONSTORE", [key, "1", tag <> "source"], ctx)
      assert_receive {:waiter_notify, ^key}, 1_000
    end
  end

  describe "a tree-encoded set" do
    setup do
      isolated = ShardHelpers.setup_isolated_data_dir()
      snapshot = ShardHelpers.replace_default_apply_context(promotion_threshold: 16)

      on_exit(fn ->
        ShardHelpers.restore_default_apply_context(snapshot)
        ShardHelpers.teardown_isolated_data_dir(isolated)
      end)

      ctx = FerricStore.Instance.get(:default)
      assert {:ok, %{active: 9}} = Ferricstore.Cluster.FeatureLevel.raise(ctx, 9)
      %{ctx: ctx}
    end

    test "a member write wakes its waiter", %{ctx: ctx} do
      key = "{zwake-tree:#{System.unique_integer([:positive])}}"
      pairs = Enum.flat_map(1..30, fn i -> ["#{i}", "m#{i}"] end)
      30 = Dispatcher.dispatch("ZADD", [key | pairs], ctx)
      wait(key)
      1 = Dispatcher.dispatch("ZADD", [key, "99", "late"], ctx)
      assert_receive {:waiter_notify, ^key}, 1_000
    end
  end
end
