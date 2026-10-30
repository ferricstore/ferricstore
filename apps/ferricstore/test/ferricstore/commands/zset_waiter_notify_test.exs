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

  test "a removal does not wake a waiter" do
    ctx = FerricStore.Instance.get(:default)
    key = "{zwake:#{System.unique_integer([:positive])}}rem"
    2 = Dispatcher.dispatch("ZADD", [key, "1", "a", "2", "b"], ctx)
    wait(key)
    1 = Dispatcher.dispatch("ZREM", [key, "a"], ctx)
    refute_receive {:waiter_notify, ^key}, 200
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
