defmodule Ferricstore.Commands.SetRandomPopTest do
  @moduledoc """
  SPOP removes a random member, as Redis does. The pop is one replicated
  entry, so every replica must pick the same one: the choice comes from the
  entry's Raft index. From feature level 11 it is a rank drawn from that
  index, so members are picked evenly whatever their names.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.ShardHelpers

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    assert {:ok, %{active: 11}} = FerricStore.raise_cluster_feature_level(11)
    ctx = FerricStore.Instance.get(:default)
    key = "spop:{#{System.unique_integer([:positive])}}"
    members = for i <- 1..200, do: "m#{i}"
    assert 200 == Dispatcher.dispatch("SADD", [key | members], ctx)
    %{ctx: ctx, key: key}
  end

  test "SPOP picks members across the set", %{ctx: ctx, key: key} do
    popped =
      for _ <- 1..40 do
        member = Dispatcher.dispatch("SPOP", [key], ctx)
        assert 1 == Dispatcher.dispatch("SADD", [key, member], ctx)
        member
      end

    # 40 even draws from 200 members repeat few; the old pick took the
    # set's first member every time.
    assert popped |> Enum.uniq() |> length() >= 15, inspect(popped)
    assert 200 == Dispatcher.dispatch("SCARD", [key], ctx)
  end

  test "SPOP with a count removes that many distinct members", %{ctx: ctx, key: key} do
    popped = Dispatcher.dispatch("SPOP", [key, "50"], ctx)

    assert 50 == length(popped)
    assert 50 == popped |> Enum.uniq() |> length()
    assert 150 == Dispatcher.dispatch("SCARD", [key], ctx)
    assert [0] == Enum.uniq(Dispatcher.dispatch("SMISMEMBER", [key | popped], ctx))
    # Not the 50 that come first in the set's order.
    first_50 = for(i <- 1..200, do: "m#{i}") |> Enum.sort() |> Enum.take(50)
    refute Enum.sort(popped) == first_50
  end
end
