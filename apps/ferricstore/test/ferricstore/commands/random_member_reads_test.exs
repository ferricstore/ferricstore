defmodule Ferricstore.Commands.RandomMemberReadsTest do
  @moduledoc """
  SRANDMEMBER, HRANDFIELD and ZRANDMEMBER on the instance pick with positional
  reads (`RandomPick`): the replies keep Redis's forms, picks spread over the
  collection, and counts past the pick limit still read it whole.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Dispatcher, RandomPick}
  alias Ferricstore.Test.ShardHelpers

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    isolated = ShardHelpers.setup_isolated_data_dir()
    # A low threshold so the sorted set is stored as a tree (level 9).
    snapshot = ShardHelpers.replace_default_apply_context(promotion_threshold: 16)

    on_exit(fn ->
      ShardHelpers.restore_default_apply_context(snapshot)
      ShardHelpers.teardown_isolated_data_dir(isolated)
    end)

    assert {:ok, _} = FerricStore.raise_cluster_feature_level(9)
    ctx = FerricStore.Instance.get(:default)
    tag = "rand:{#{System.unique_integer([:positive])}}"
    d = fn command, args -> Dispatcher.dispatch(command, args, ctx) end
    members = for i <- 1..200, do: "m#{i}"

    200 = d.("SADD", [tag <> "s" | members])
    200 = d.("HSET", [tag <> "h" | Enum.flat_map(members, &[&1, "v" <> &1])])
    200 = d.("ZADD", [tag <> "z" | Enum.flat_map(1..200, &["#{&1}", "m#{&1}"])])
    %{d: d, k: &(tag <> &1), members: MapSet.new(members)}
  end

  test "one member at a time spreads over the collection", %{d: d, k: k, members: members} do
    for {command, key} <- [{"SRANDMEMBER", "s"}, {"HRANDFIELD", "h"}, {"ZRANDMEMBER", "z"}] do
      picked = for _ <- 1..40, do: d.(command, [k.(key)])
      assert Enum.all?(picked, &MapSet.member?(members, &1)), "#{command}: #{inspect(picked)}"
      assert picked |> Enum.uniq() |> length() >= 15, "#{command}: #{inspect(picked)}"
    end

    assert nil == d.("SRANDMEMBER", [k.("missing")])
  end

  test "counts keep Redis's forms", %{d: d, k: k, members: members} do
    limit = RandomPick.max_positional_picks("Z:")

    for count <- [3, limit, limit + 5] do
      picked = d.("SRANDMEMBER", [k.("s"), "#{count}"])
      assert count == picked |> Enum.uniq() |> length()
      assert Enum.all?(picked, &MapSet.member?(members, &1))
    end

    assert 4 == length(d.("SRANDMEMBER", [k.("s"), "-4"]))

    [field, value | _] = flat = d.("HRANDFIELD", [k.("h"), "3", "WITHVALUES"])
    assert 6 == length(flat)
    assert value == "v" <> field

    [member, score | _] = flat = d.("ZRANDMEMBER", [k.("z"), "3", "WITHSCORES"])
    assert 6 == length(flat)
    assert score == d.("ZSCORE", [k.("z"), member])

    for count <- [2, limit, limit + 3] do
      picked = d.("ZRANDMEMBER", [k.("z"), "#{count}"])
      assert count == picked |> Enum.uniq() |> length()
      assert Enum.all?(picked, &MapSet.member?(members, &1))
    end

    # As many as the collection holds, past it.
    assert 200 == length(d.("ZRANDMEMBER", [k.("z"), "500"]))
  end
end
