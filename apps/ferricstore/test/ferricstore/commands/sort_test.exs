defmodule Ferricstore.Commands.SortTest do
  @moduledoc "SORT and SORT_RO, as Redis in cluster mode answers them."
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Dispatcher, PreparedCommand}
  alias Ferricstore.Transaction.ReplicaCompat

  setup do
    ctx = FerricStore.Instance.get(:default)
    tag = "sort#{System.unique_integer([:positive])}"

    %{
      ctx: ctx,
      d: fn command, args -> Dispatcher.dispatch(command, args, ctx) end,
      k: &"{#{tag}}#{&1}"
    }
  end

  test "numbers, ALPHA, DESC and LIMIT over a list, set and sorted set", %{d: d, k: k} do
    3 = d.("RPUSH", [k.("l"), "3", "10", "2"])
    3 = d.("SADD", [k.("s"), "3", "10", "2"])
    3 = d.("ZADD", [k.("z"), "1", "c", "2", "a", "3", "b"])

    assert ["2", "3", "10"] == d.("SORT", [k.("l")])
    assert ["10", "3", "2"] == d.("SORT", [k.("l"), "DESC"])
    assert ["10", "2", "3"] == d.("SORT", [k.("l"), "ALPHA"])
    assert ["3", "10"] == d.("SORT", [k.("l"), "LIMIT", "1", "5"])
    assert [] == d.("SORT", [k.("l"), "LIMIT", "5", "1"])
    assert ["2", "3", "10"] == d.("SORT", [k.("s")])
    assert ["a", "b", "c"] == d.("SORT_RO", [k.("z"), "ALPHA"])

    assert {:error, "ERR One or more scores can't be converted into double"} ==
             d.("SORT", [k.("z")])

    assert [] == d.("SORT", [k.("missing")])
  end

  test "BY without a pattern keeps a list's and a sorted set's order", %{d: d, k: k} do
    3 = d.("RPUSH", [k.("l"), "b", "c", "a"])
    3 = d.("ZADD", [k.("z"), "1", "c", "2", "a", "3", "b"])

    assert ["b", "c", "a"] == d.("SORT", [k.("l"), "BY", "nosort"])
    assert ["a", "c", "b"] == d.("SORT", [k.("l"), "BY", "nosort", "DESC"])
    assert ["c", "a", "b"] == d.("SORT", [k.("z"), "BY", "nosort"])
    assert ["a"] == d.("SORT", [k.("z"), "BY", "nosort", "DESC", "LIMIT", "1", "1"])
  end

  test "BY and GET read keys and hash fields named by a pattern", %{d: d, k: k} do
    3 = d.("RPUSH", [k.("l"), "1", "2", "3"])

    for {id, w} <- [{"1", "30"}, {"2", "10"}, {"3", "20"}],
        do: :ok = d.("SET", [k.("w_" <> id), w])

    :ok = d.("SET", [k.("o_1"), "one"])
    1 = d.("HSET", [k.("h_2"), "name", "two"])

    assert ["2", "3", "1"] == d.("SORT", [k.("l"), "BY", k.("w_*")])

    assert ["2", nil, "two", "3", nil, nil, "1", "one", nil] ==
             d.("SORT", [
               k.("l"),
               "BY",
               k.("w_*"),
               "GET",
               "#",
               "GET",
               k.("o_*"),
               "GET",
               k.("h_*->name")
             ])

    # A missing weight counts as 0.
    4 = d.("RPUSH", [k.("l"), "4"])
    assert ["4", "2", "3", "1"] == d.("SORT", [k.("l"), "BY", k.("w_*")])
  end

  test "patterns that may name keys in other slots are refused", %{d: d, k: k} do
    1 = d.("RPUSH", [k.("l"), "1"])

    assert {:error,
            "ERR BY option of SORT denied in Cluster mode when keys formed by the pattern " <>
              "may be in different slots."} == d.("SORT", [k.("l"), "BY", "w_*"])

    assert {:error,
            "ERR GET option of SORT denied in Cluster mode when keys formed by the pattern " <>
              "may be in different slots."} == d.("SORT", [k.("l"), "GET", "o_*"])

    assert {:error, "ERR GET option of SORT denied" <> _} =
             d.("SORT", [k.("l"), "GET", "{other}o_*"])
  end

  test "errors", %{d: d, k: k} do
    :ok = d.("SET", [k.("str"), "v"])
    assert {:error, "WRONGTYPE" <> _} = d.("SORT", [k.("str")])
    assert {:error, "ERR syntax error"} == d.("SORT", [k.("l"), "FOO"])
    assert {:error, "ERR syntax error"} == d.("SORT_RO", [k.("l"), "STORE", k.("d")])

    assert {:error, "ERR value is not an integer or out of range"} ==
             d.("SORT", [k.("l"), "LIMIT", "x", "1"])

    assert {:error, "ERR wrong number of arguments for 'sort' command"} == d.("SORT", [])
  end

  test "STORE needs feature level 12", %{d: d, k: k} do
    1 = d.("RPUSH", [k.("l"), "1"])
    assert {:error, message} = d.("SORT", [k.("l"), "STORE", k.("d")])
    assert message =~ "needs cluster feature level 12"
  end

  test "keys, ACL keys and MULTI", %{ctx: ctx, k: k} do
    {:ok, plain} = PreparedCommand.prepare("SORT", [k.("l"), "GET", "#", "STORE", k.("d")])
    assert [k.("l"), k.("d")] == plain.command_keys
    assert [k.("l"), k.("d")] == plain.acl_keys

    # A pattern reads keys the command does not name: as in Redis, only a
    # user with access to every key may run it.
    {:ok, by} = PreparedCommand.prepare("SORT_RO", [k.("l"), "BY", k.("w_*")])
    assert [k.("l")] == by.command_keys
    assert "*" in by.acl_keys

    for prepared <- [plain, by] do
      assert 12 == ReplicaCompat.required_level(prepared.ast)
      assert ReplicaCompat.queue_error(prepared.command, prepared.ast, prepared.command_keys, ctx)
    end
  end
end

defmodule Ferricstore.Commands.SortLevel12Test do
  @moduledoc "From feature level 12, SORT ... STORE replaces the destination in one entry."
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Dispatcher, PreparedCommand}
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    {:ok, %{active: 12}} = FerricStore.raise_cluster_feature_level(12)
    :ok
  end

  test "STORE writes a list, replaces any type, and an empty result deletes it" do
    ctx = FerricStore.Instance.get(:default)
    d = fn command, args -> Dispatcher.dispatch(command, args, ctx) end
    tag = "sort12#{System.unique_integer([:positive])}"
    k = &"{#{tag}}#{&1}"
    l = k.("l")
    dest = k.("d")

    3 = d.("RPUSH", [l, "3", "1", "2"])
    :ok = d.("SET", [dest, "old"])
    assert 3 == d.("SORT", [l, "STORE", dest])
    assert ["1", "2", "3"] == d.("LRANGE", [dest, "0", "-1"])

    # A missing GET value is stored as an empty string.
    assert 3 == d.("SORT", [l, "GET", k.("none_*"), "STORE", dest])
    assert ["", "", ""] == d.("LRANGE", [dest, "0", "-1"])

    assert 0 == d.("SORT", [k.("missing"), "STORE", dest])
    assert 0 == d.("EXISTS", [dest])

    {:ok, prepared} = PreparedCommand.prepare("SORT", [l, "DESC", "STORE", dest])
    assert [3] == Ferricstore.Transaction.Coordinator.execute([prepared], %{}, nil)
    assert ["3", "2", "1"] == d.("LRANGE", [dest, "0", "-1"])
  end
end
