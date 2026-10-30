defmodule Ferricstore.Commands.ZSetLexRemRangeTest do
  @moduledoc """
  ZRANGEBYLEX, ZREVRANGEBYLEX, ZLEXCOUNT, ZREMRANGEBYRANK, ZREMRANGEBYSCORE,
  ZREMRANGEBYLEX and ZMPOP, as Redis answers them.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.{Dispatcher, PreparedCommand}
  alias Ferricstore.Test.ShardHelpers
  alias Ferricstore.Transaction.{Coordinator, ReplicaCompat}

  @wrongtype "WRONGTYPE Operation against a key holding the wrong kind of value"

  setup do
    ctx = FerricStore.Instance.get(:default)
    tag = "{zlex:#{System.unique_integer([:positive])}}"
    d = fn command, args -> Dispatcher.dispatch(command, args, ctx) end
    k = &(tag <> &1)
    # Equal scores, so lex ranges follow member order (as Redis requires).
    5 = d.("ZADD", [k.("lex"), "0", "a", "0", "b", "0", "c", "0", "d", "0", "e"])
    4 = d.("ZADD", [k.("z"), "1", "one", "2", "two", "3", "three", "4", "four"])
    %{ctx: ctx, d: d, k: k}
  end

  defp members(d, key), do: d.("ZRANGE", [key, "0", "-1"])

  describe "lex reads" do
    test "ZRANGEBYLEX and ZREVRANGEBYLEX select by member, with LIMIT", %{d: d, k: k} do
      assert ["b", "c", "d"] == d.("ZRANGEBYLEX", [k.("lex"), "[b", "(e"])
      assert ["a", "b", "c", "d", "e"] == d.("ZRANGEBYLEX", [k.("lex"), "-", "+"])
      assert ["c", "d"] == d.("ZRANGEBYLEX", [k.("lex"), "-", "+", "LIMIT", "2", "2"])
      assert ["d", "c", "b"] == d.("ZREVRANGEBYLEX", [k.("lex"), "(e", "[b"])
      assert ["e", "d"] == d.("ZREVRANGEBYLEX", [k.("lex"), "+", "-", "limit", "0", "2"])
      assert [] == d.("ZRANGEBYLEX", [k.("missing"), "-", "+"])
    end

    test "ZLEXCOUNT counts the members in a range", %{d: d, k: k} do
      assert 3 == d.("ZLEXCOUNT", [k.("lex"), "[b", "[d"])
      assert 5 == d.("ZLEXCOUNT", [k.("lex"), "-", "+"])
      assert 0 == d.("ZLEXCOUNT", [k.("missing"), "-", "+"])
    end

    test "lex reads refuse bad ranges, options and types", %{d: d, k: k} do
      :ok = d.("SET", [k.("str"), "x"])
      bad_range = {:error, "ERR min or max not valid string range item"}

      assert bad_range == d.("ZRANGEBYLEX", [k.("lex"), "b", "+"])
      assert bad_range == d.("ZLEXCOUNT", [k.("lex"), "-", "c"])

      assert {:error, "ERR syntax error"} ==
               d.("ZRANGEBYLEX", [k.("lex"), "-", "+", "WITHSCORES"])

      assert {:error, "ERR syntax error"} ==
               d.("ZRANGEBYLEX", [k.("lex"), "-", "+", "LIMIT", "1"])

      assert {:error, @wrongtype} == d.("ZRANGEBYLEX", [k.("str"), "-", "+"])
      assert {:error, @wrongtype} == d.("ZLEXCOUNT", [k.("str"), "-", "+"])
    end
  end

  describe "ZREMRANGEBY*" do
    test "ZREMRANGEBYRANK removes a rank range", %{d: d, k: k} do
      assert 2 == d.("ZREMRANGEBYRANK", [k.("z"), "0", "1"])
      assert ["three", "four"] == members(d, k.("z"))
      assert 1 == d.("ZREMRANGEBYRANK", [k.("z"), "-1", "-1"])
      assert ["three"] == members(d, k.("z"))
      assert 0 == d.("ZREMRANGEBYRANK", [k.("z"), "5", "10"])

      assert {:error, "ERR value is not an integer or out of range"} ==
               d.("ZREMRANGEBYRANK", [k.("z"), "a", "1"])
    end

    test "ZREMRANGEBYSCORE removes a score range", %{d: d, k: k} do
      assert 2 == d.("ZREMRANGEBYSCORE", [k.("z"), "(1", "3"])
      assert ["one", "four"] == members(d, k.("z"))
      assert 2 == d.("ZREMRANGEBYSCORE", [k.("z"), "-inf", "+inf"])
      assert 0 == d.("EXISTS", [k.("z")])

      assert {:error, "ERR min or max is not a float"} ==
               d.("ZREMRANGEBYSCORE", [k.("z"), "x", "1"])
    end

    test "ZREMRANGEBYLEX removes a member range", %{d: d, k: k} do
      assert 3 == d.("ZREMRANGEBYLEX", [k.("lex"), "[b", "[d"])
      assert ["a", "e"] == members(d, k.("lex"))

      assert {:error, "ERR min or max not valid string range item"} ==
               d.("ZREMRANGEBYLEX", [k.("lex"), "b", "+"])
    end

    test "a missing key removes nothing and a wrong type is refused", %{d: d, k: k} do
      :ok = d.("SET", [k.("str"), "x"])
      assert 0 == d.("ZREMRANGEBYRANK", [k.("missing"), "0", "-1"])
      assert 0 == d.("EXISTS", [k.("missing")])
      assert {:error, @wrongtype} == d.("ZREMRANGEBYSCORE", [k.("str"), "-inf", "+inf"])
    end
  end

  describe "ZMPOP" do
    test "pops from the first non-empty key", %{d: d, k: k} do
      assert [k.("z"), [["one", "1"]]] == d.("ZMPOP", ["2", k.("missing"), k.("z"), "MIN"])

      assert [k.("z"), [["four", "4"], ["three", "3"]]] ==
               d.("ZMPOP", ["1", k.("z"), "max", "COUNT", "2"])

      assert nil == d.("ZMPOP", ["1", k.("missing"), "MIN"])
    end

    test "refuses bad arguments as Redis does", %{d: d, k: k} do
      assert {:error, "ERR numkeys should be greater than 0"} ==
               d.("ZMPOP", ["0", k.("z"), "MIN"])

      assert {:error, "ERR syntax error"} == d.("ZMPOP", ["1", k.("z"), "SIDEWAYS"])

      assert {:error, "ERR count should be greater than 0"} ==
               d.("ZMPOP", ["1", k.("z"), "MIN", "COUNT", "0"])

      assert {:error, "ERR syntax error"} == d.("ZMPOP", ["3", k.("z"), "MIN"])
    end
  end

  test "COMMAND GETKEYS names every key", %{d: d} do
    assert ["key"] == d.("COMMAND", ["GETKEYS", "ZRANGEBYLEX", "key", "-", "+"])
    assert ["key"] == d.("COMMAND", ["GETKEYS", "ZREMRANGEBYSCORE", "key", "0", "1"])
    assert ["a", "b"] == d.("COMMAND", ["GETKEYS", "ZMPOP", "2", "a", "b", "MIN"])
  end

  test "below feature level 10 MULTI refuses them", %{ctx: ctx, k: k} do
    for [command | args] <- [
          ["ZRANGEBYLEX", k.("lex"), "-", "+"],
          ["ZLEXCOUNT", k.("lex"), "-", "+"],
          ["ZREMRANGEBYRANK", k.("z"), "0", "0"],
          ["ZMPOP", "1", k.("z"), "MIN"]
        ] do
      {:ok, prepared} = PreparedCommand.prepare(command, args)

      assert ReplicaCompat.queue_error(command, prepared.ast, prepared.command_keys, ctx) =~
               "needs cluster feature level 10"
    end
  end

  describe "at feature level 10" do
    setup do
      isolated = ShardHelpers.setup_isolated_data_dir()
      on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
      assert {:ok, %{active: 10}} = FerricStore.raise_cluster_feature_level(10)
      ctx = FerricStore.Instance.get(:default)
      tag = "{zlex10:#{System.unique_integer([:positive])}}"
      d = fn command, args -> Dispatcher.dispatch(command, args, ctx) end
      4 = d.("ZADD", [tag <> "z", "1", "one", "2", "two", "3", "three", "4", "four"])
      %{ctx: ctx, d: d, k: &(tag <> &1)}
    end

    test "a transaction runs them", %{d: d, k: k} do
      queue =
        for [command | args] <- [
              ["ZLEXCOUNT", k.("z"), "-", "+"],
              ["ZREMRANGEBYSCORE", k.("z"), "-inf", "1"],
              ["ZMPOP", "1", k.("z"), "MAX"],
              ["ZRANGEBYLEX", k.("z"), "-", "+"]
            ] do
          {:ok, prepared} = PreparedCommand.prepare(command, args)
          prepared
        end

      key = k.("z")
      assert [4, 1, [^key, [["four", "4"]]], members] = Coordinator.execute(queue, %{}, nil)
      assert Enum.sort(members) == ["three", "two"]
      assert ["two", "three"] == d.("ZRANGE", [k.("z"), "0", "-1"])
    end

    # The shard decides what to remove or pop where the write applies.
    test "the writes run as one transaction", %{d: d, k: k} do
      parent = self()

      Process.put(:ferricstore_tx_backend_write_hook, fn shard, command ->
        send(parent, :transaction_used)
        Ferricstore.Raft.Backend.write(shard, command)
      end)

      on_exit(fn -> Process.delete(:ferricstore_tx_backend_write_hook) end)

      assert 1 == d.("ZREMRANGEBYRANK", [k.("z"), "0", "0"])
      assert_received :transaction_used
      assert [k.("z"), [["four", "4"]]] == d.("ZMPOP", ["1", k.("z"), "MAX"])
      assert_received :transaction_used
      assert ["two", "three"] == d.("ZRANGE", [k.("z"), "0", "-1"])
    end
  end
end
