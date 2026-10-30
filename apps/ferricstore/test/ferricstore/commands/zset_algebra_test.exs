defmodule Ferricstore.Commands.ZSetAlgebraTest do
  @moduledoc """
  ZUNION, ZINTER, ZDIFF, their STORE forms, ZINTERCARD and ZRANGESTORE, as
  Redis answers them: sets count as sorted sets of score 1, a missing key as
  an empty set, and an empty result deletes the destination.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.{Dispatcher, PreparedCommand}
  alias Ferricstore.Test.ShardHelpers
  alias Ferricstore.Transaction.{Coordinator, ReplicaCompat}

  setup do
    ctx = FerricStore.Instance.get(:default)
    tag = "{zalg:#{System.unique_integer([:positive])}}"
    d = fn command, args -> Dispatcher.dispatch(command, args, ctx) end
    k = fn name -> tag <> name end

    2 = d.("ZADD", [k.("z1"), "1", "a", "2", "b"])
    2 = d.("ZADD", [k.("z2"), "3", "b", "4", "c"])
    %{ctx: ctx, d: d, k: k}
  end

  defp range(d, key), do: d.("ZRANGE", [key, "0", "-1", "WITHSCORES"])

  describe "ZUNIONSTORE" do
    test "sums the scores of every member", %{d: d, k: k} do
      assert 3 == d.("ZUNIONSTORE", [k.("out"), "2", k.("z1"), k.("z2")])
      assert ["a", "1", "c", "4", "b", "5"] == range(d, k.("out"))
    end

    test "weights the sources and aggregates by MIN or MAX", %{d: d, k: k} do
      args = [k.("out"), "2", k.("z1"), k.("z2"), "WEIGHTS", "2", "0.5"]
      assert 3 == d.("ZUNIONSTORE", args)
      assert ["a", "2", "c", "2", "b", "5.5"] == range(d, k.("out"))

      assert 3 == d.("ZUNIONSTORE", args ++ ["AGGREGATE", "MIN"])
      assert ["b", "1.5", "a", "2", "c", "2"] == range(d, k.("out"))

      assert 3 == d.("ZUNIONSTORE", args ++ ["aggregate", "max"])
      assert ["a", "2", "c", "2", "b", "4"] == range(d, k.("out"))
    end

    test "counts a set's members with score 1 and a missing key as empty", %{d: d, k: k} do
      2 = d.("SADD", [k.("s"), "a", "c"])

      assert 3 == d.("ZUNIONSTORE", [k.("out"), "3", k.("z1"), k.("s"), k.("missing")])
      assert ["c", "1", "a", "2", "b", "2"] == range(d, k.("out"))
    end

    test "replaces a destination of any type", %{d: d, k: k} do
      :ok = d.("SET", [k.("out"), "old"])
      assert 2 == d.("ZUNIONSTORE", [k.("out"), "1", k.("z1")])
      assert {:simple, "zset"} == d.("TYPE", [k.("out")])
      assert ["a", "1", "b", "2"] == range(d, k.("out"))

      1 = d.("HSET", [k.("hash"), "f", "v"])
      assert 2 == d.("ZUNIONSTORE", [k.("hash"), "1", k.("z2")])
      assert ["b", "3", "c", "4"] == range(d, k.("hash"))
    end

    test "may store into one of its sources", %{d: d, k: k} do
      assert 3 == d.("ZUNIONSTORE", [k.("z1"), "2", k.("z1"), k.("z2")])
      assert ["a", "1", "c", "4", "b", "5"] == range(d, k.("z1"))
    end

    test "refuses bad arguments as Redis does", %{d: d, k: k} do
      :ok = d.("SET", [k.("str"), "x"])

      assert {:error, "ERR at least 1 input key is needed for 'zunionstore' command"} ==
               d.("ZUNIONSTORE", [k.("out"), "0", k.("z1")])

      assert {:error, "ERR value is not an integer or out of range"} ==
               d.("ZUNIONSTORE", [k.("out"), "x", k.("z1")])

      assert {:error, "ERR syntax error"} == d.("ZUNIONSTORE", [k.("out"), "3", k.("z1")])

      assert {:error, "ERR weight value is not a float"} ==
               d.("ZUNIONSTORE", [k.("out"), "1", k.("z1"), "WEIGHTS", "x"])

      assert {:error, "ERR syntax error"} ==
               d.("ZUNIONSTORE", [k.("out"), "1", k.("z1"), "AGGREGATE", "AVG"])

      assert {:error, "ERR syntax error"} ==
               d.("ZUNIONSTORE", [k.("out"), "1", k.("z1"), "WITHSCORES"])

      assert {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"} ==
               d.("ZUNIONSTORE", [k.("out"), "2", k.("z1"), k.("str")])

      assert {:error, "ERR wrong number of arguments for 'zunionstore' command"} ==
               d.("ZUNIONSTORE", [k.("out"), "1"])
    end

    test "refuses a score past the largest double", %{d: d, k: k} do
      assert {:error, "ERR resulting score is not a number (NaN)"} ==
               d.("ZUNIONSTORE", [k.("out"), "1", k.("z2"), "WEIGHTS", "1e308"])

      assert 0 == d.("EXISTS", [k.("out")])
    end

    test "refuses keys on different shards" do
      ctx = FerricStore.Instance.get(:default)

      assert {:error, "CROSSSLOT" <> _} =
               Dispatcher.dispatch("ZUNIONSTORE", ["{x}out", "2", "{y}a", "{z}b"], ctx)
    end
  end

  describe "ZINTERSTORE and ZDIFFSTORE" do
    test "ZINTERSTORE keeps the members in every source", %{d: d, k: k} do
      assert 1 == d.("ZINTERSTORE", [k.("out"), "2", k.("z1"), k.("z2"), "WEIGHTS", "2", "3"])
      assert ["b", "13"] == range(d, k.("out"))
    end

    test "an empty result deletes the destination", %{d: d, k: k} do
      :ok = d.("SET", [k.("out"), "old"])
      assert 0 == d.("ZINTERSTORE", [k.("out"), "2", k.("z1"), k.("missing")])
      assert 0 == d.("EXISTS", [k.("out")])
    end

    test "ZDIFFSTORE keeps the first source's members the others lack", %{d: d, k: k} do
      assert 1 == d.("ZDIFFSTORE", [k.("out"), "2", k.("z1"), k.("z2")])
      assert ["a", "1"] == range(d, k.("out"))

      assert {:error, "ERR syntax error"} ==
               d.("ZDIFFSTORE", [k.("out"), "2", k.("z1"), k.("z2"), "WEIGHTS", "1", "1"])

      assert {:error, "ERR at least 1 input key is needed for 'zdiffstore' command"} ==
               d.("ZDIFFSTORE", [k.("out"), "0", k.("z1")])
    end
  end

  describe "ZUNION, ZINTER, ZDIFF and ZINTERCARD" do
    test "answer members by score, with scores on request", %{d: d, k: k} do
      assert ["a", "c", "b"] == d.("ZUNION", ["2", k.("z1"), k.("z2")])

      assert ["a", "1", "c", "4", "b", "5"] ==
               d.("ZUNION", ["2", k.("z1"), k.("z2"), "WITHSCORES"])

      assert ["b", "5"] == d.("ZINTER", ["2", k.("z1"), k.("z2"), "WITHSCORES"])
      assert ["a"] == d.("ZDIFF", ["2", k.("z1"), k.("z2")])
      assert ["a", "1"] == d.("ZDIFF", ["2", k.("z1"), k.("z2"), "withscores"])
      assert [] == d.("ZINTER", ["2", k.("z1"), k.("missing")])
      assert 0 == d.("EXISTS", [k.("missing")])

      assert {:error, "ERR at least 1 input key is needed for 'zunion' command"} ==
               d.("ZUNION", ["0", k.("z1")])
    end

    test "ZINTERCARD counts the intersection up to LIMIT", %{d: d, k: k} do
      2 = d.("ZADD", [k.("z3"), "9", "b", "9", "c"])

      assert 1 == d.("ZINTERCARD", ["2", k.("z1"), k.("z2")])
      assert 2 == d.("ZINTERCARD", ["2", k.("z2"), k.("z3")])
      assert 1 == d.("ZINTERCARD", ["2", k.("z2"), k.("z3"), "LIMIT", "1"])
      assert 2 == d.("ZINTERCARD", ["2", k.("z2"), k.("z3"), "LIMIT", "0"])

      assert {:error, "ERR numkeys should be greater than 0"} == d.("ZINTERCARD", ["0", k.("z1")])

      assert {:error, "ERR Number of keys can't be greater than number of args"} ==
               d.("ZINTERCARD", ["3", k.("z1"), k.("z2")])

      assert {:error, "ERR LIMIT can't be negative"} ==
               d.("ZINTERCARD", ["1", k.("z1"), "LIMIT", "-1"])
    end
  end

  describe "ZRANGESTORE" do
    setup %{d: d, k: k} do
      4 = d.("ZADD", [k.("src"), "1", "a", "2", "b", "3", "c", "4", "d"])
      :ok
    end

    test "stores a range by rank, score or member", %{d: d, k: k} do
      assert 2 == d.("ZRANGESTORE", [k.("out"), k.("src"), "1", "2"])
      assert ["b", "2", "c", "3"] == range(d, k.("out"))

      assert 2 == d.("ZRANGESTORE", [k.("out"), k.("src"), "0", "1", "REV"])
      assert ["c", "3", "d", "4"] == range(d, k.("out"))

      assert 2 ==
               d.("ZRANGESTORE", [
                 k.("out"),
                 k.("src"),
                 "(1",
                 "+inf",
                 "BYSCORE",
                 "LIMIT",
                 "0",
                 "2"
               ])

      assert ["b", "2", "c", "3"] == range(d, k.("out"))

      assert 3 == d.("ZRANGESTORE", [k.("out"), k.("src"), "[b", "+", "BYLEX"])
      assert ["b", "2", "c", "3", "d", "4"] == range(d, k.("out"))
    end

    test "an empty range deletes the destination", %{d: d, k: k} do
      assert 0 == d.("ZRANGESTORE", [k.("out"), k.("src"), "10", "20"])
      assert 0 == d.("EXISTS", [k.("out")])
      assert 0 == d.("ZRANGESTORE", [k.("out"), k.("missing"), "0", "-1"])
    end

    test "refuses WITHSCORES and a set source", %{d: d, k: k} do
      1 = d.("SADD", [k.("s"), "a"])

      assert {:error, "ERR syntax error"} ==
               d.("ZRANGESTORE", [k.("out"), k.("src"), "0", "-1", "WITHSCORES"])

      assert {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"} ==
               d.("ZRANGESTORE", [k.("out"), k.("s"), "0", "-1"])
    end
  end

  test "COMMAND GETKEYS names every key", %{d: d} do
    assert ["dst", "a", "b"] == d.("COMMAND", ["GETKEYS", "ZUNIONSTORE", "dst", "2", "a", "b"])
    assert ["a", "b"] == d.("COMMAND", ["GETKEYS", "ZINTER", "2", "a", "b", "WITHSCORES"])
    assert ["a", "b"] == d.("COMMAND", ["GETKEYS", "ZINTERCARD", "2", "a", "b"])
    assert ["dst", "src"] == d.("COMMAND", ["GETKEYS", "ZRANGESTORE", "dst", "src", "0", "1"])
  end

  describe "inside MULTI" do
    test "below feature level 10 the commands are refused", %{ctx: ctx, k: k} do
      assert {:ok, %{active: active}} = FerricStore.cluster_feature_level()
      assert active < 10
      {:ok, prepared} = PreparedCommand.prepare("ZUNIONSTORE", [k.("out"), "1", k.("z1")])

      assert ReplicaCompat.queue_error(prepared.command, prepared.ast, prepared.command_keys, ctx) =~
               "needs cluster feature level 10"
    end
  end

  describe "at feature level 10" do
    setup do
      isolated = ShardHelpers.setup_isolated_data_dir()
      on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
      assert {:ok, %{active: 10}} = FerricStore.raise_cluster_feature_level(10)
      ctx = FerricStore.Instance.get(:default)
      tag = "{zalg10:#{System.unique_integer([:positive])}}"
      d = fn command, args -> Dispatcher.dispatch(command, args, ctx) end
      2 = d.("ZADD", [tag <> "z1", "1", "a", "2", "b"])
      2 = d.("ZADD", [tag <> "z2", "3", "b", "4", "c"])
      %{ctx: ctx, d: d, k: &(tag <> &1)}
    end

    test "a transaction runs them", %{ctx: ctx, d: d, k: k} do
      queue =
        for [command | args] <- [
              ["ZUNIONSTORE", k.("out"), "2", k.("z1"), k.("z2")],
              ["ZINTERCARD", "2", k.("z1"), k.("z2")],
              ["ZRANGESTORE", k.("top"), k.("out"), "0", "0", "REV"],
              ["ZDIFF", "2", k.("z1"), k.("z2"), "WITHSCORES"]
            ] do
          {:ok, prepared} = PreparedCommand.prepare(command, args)

          assert nil ==
                   ReplicaCompat.queue_error(command, prepared.ast, prepared.command_keys, ctx)

          prepared
        end

      assert [3, 1, 1, ["a", "1"]] == Coordinator.execute(queue, %{}, nil)
      assert ["b", "5"] == range(d, k.("top"))
    end

    test "storing into a source runs as one transaction", %{d: d, k: k} do
      parent = self()

      Process.put(:ferricstore_tx_backend_write_hook, fn shard, command ->
        send(parent, :transaction_used)
        Ferricstore.Raft.Backend.write(shard, command)
      end)

      on_exit(fn -> Process.delete(:ferricstore_tx_backend_write_hook) end)

      assert 1 == d.("ZINTERSTORE", [k.("z1"), "2", k.("z1"), k.("z2")])
      assert_received :transaction_used
      assert ["b", "5"] == range(d, k.("z1"))
    end
  end
end
