defmodule Ferricstore.Cluster.FeatureLevelMemberTypesTest do
  @moduledoc """
  Feature level 4: the state machine checks the key's type when it writes
  hash, set or sorted set members, as ZINCRBY always has. Commands that check
  the type before writing (HINCRBY, HINCRBYFLOAT, HSETNX, HSETEX, ZADD and
  GEOADD with options, SMOVE) can then no longer leave members under a string
  that a SET wrote between the check and the write.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.ShardHelpers

  @rounds 30
  @wrongtype {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

  setup_all do
    # Feature levels survive FLUSHDB, so raise them only on a throwaway data dir.
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    assert {:ok, %{active: 4}} = FerricStore.raise_cluster_feature_level(4)
    :ok
  end

  setup do
    %{
      ctx: FerricStore.Instance.get(:default),
      key: "{level4:#{System.unique_integer([:positive])}}"
    }
  end

  for {name, emptying} <- [
        {"HDEL", [["HDEL", "f"]]},
        {"HDEL of several fields", [["HSET", "g", "2"], ["HDEL", "f", "g", "missing"]]},
        {"HGETDEL", [["HGETDEL", "FIELDS", "1", "f"]]}
      ] do
    @emptying emptying

    test "a hash emptied by #{name} leaves no type behind", %{ctx: ctx, key: key} do
      assert 1 == Dispatcher.dispatch("HSET", [key, "f", "1"], ctx)

      for [command | args] <- @emptying, do: Dispatcher.dispatch(command, [key | args], ctx)

      # Nothing that reads the key's type runs first: TYPE and EXISTS drop a
      # marker whose collection is empty, and would hide one left behind.
      assert 1 == Dispatcher.dispatch("RPUSH", [key, "x"], ctx)
      assert ["x"] == Dispatcher.dispatch("LRANGE", [key, "0", "-1"], ctx)
    end
  end

  for {name, setup, write, recreate, check} <- [
        {"HINCRBY", nil, ["HINCRBY", "m", "1"], ["HSET", "o", "1"], ["HEXISTS", "m"]},
        {"HINCRBYFLOAT", nil, ["HINCRBYFLOAT", "m", "1.5"], ["HSET", "o", "1"], ["HEXISTS", "m"]},
        {"HSETNX", nil, ["HSETNX", "m", "1"], ["HSET", "o", "1"], ["HEXISTS", "m"]},
        {"HSETEX", nil, ["HSETEX", "100", "m", "1"], ["HSET", "o", "1"], ["HEXISTS", "m"]},
        {"ZADD NX", nil, ["ZADD", "NX", "1", "m"], ["ZADD", "1", "o"], ["ZSCORE", "m"]},
        {"ZADD CH", nil, ["ZADD", "CH", "1", "m", "2", "n"], ["ZADD", "1", "o"], ["ZSCORE", "m"]},
        {"GEOADD NX", nil, ["GEOADD", "NX", "13.36", "38.11", "m"], ["ZADD", "1", "o"],
         ["ZSCORE", "m"]},
        {"SMOVE", :source, ["SMOVE", "m"], ["SADD", "o"], ["SISMEMBER", "m"]}
      ] do
    @setup setup
    @write write
    @recreate recreate
    @check check

    test "a SET racing #{name} leaves no member behind", %{ctx: ctx, key: key} do
      [command | args] = @write
      [recreate_command | recreate_args] = @recreate
      [check_command | check_args] = @check

      resurrected =
        Enum.count(1..@rounds, fn round ->
          round_key = "#{key}:#{round}"
          source = round_key <> ":source"

          args =
            if @setup == :source do
              assert 1 == Dispatcher.dispatch("SADD", [source, "m"], ctx)
              [source, round_key | args]
            else
              [round_key | args]
            end

          writer = Task.async(fn -> Dispatcher.dispatch(command, args, ctx) end)
          setter = Task.async(fn -> Dispatcher.dispatch("SET", [round_key, "string"], ctx) end)
          Task.await(writer, 30_000)
          Task.await(setter, 30_000)

          string? = Dispatcher.dispatch("TYPE", [round_key], ctx) == {:simple, "string"}
          Dispatcher.dispatch("DEL", [round_key], ctx)
          Dispatcher.dispatch(recreate_command, [round_key | recreate_args], ctx)

          string? and
            Dispatcher.dispatch(check_command, [round_key | check_args], ctx) not in [0, nil]
        end)

      assert resurrected == 0
    end
  end

  test "member writes keep their replies, create keys and refuse strings", %{ctx: ctx, key: key} do
    assert 5 == Dispatcher.dispatch("HINCRBY", [key <> ":h", "n", "5"], ctx)
    assert {:simple, "hash"} == Dispatcher.dispatch("TYPE", [key <> ":h"], ctx)
    assert 1 == Dispatcher.dispatch("HSETNX", [key <> ":h", "f", "v"], ctx)
    assert 0 == Dispatcher.dispatch("HSETNX", [key <> ":h", "f", "w"], ctx)
    assert 1 == Dispatcher.dispatch("HSETEX", [key <> ":h", "100", "g", "v"], ctx)
    assert "v" == Dispatcher.dispatch("HGET", [key <> ":h", "f"], ctx)

    assert 1 == Dispatcher.dispatch("ZADD", [key <> ":z", "NX", "1", "a"], ctx)
    assert 1 == Dispatcher.dispatch("ZADD", [key <> ":z", "XX", "CH", "2", "a"], ctx)
    assert "2.0" == Dispatcher.dispatch("ZSCORE", [key <> ":z", "a"], ctx)

    assert 1 == Dispatcher.dispatch("SADD", [key <> ":s", "m"], ctx)
    assert 1 == Dispatcher.dispatch("SMOVE", [key <> ":s", key <> ":t", "m"], ctx)
    assert ["m"] == Dispatcher.dispatch("SMEMBERS", [key <> ":t"], ctx)
    assert 0 == Dispatcher.dispatch("EXISTS", [key <> ":s"], ctx)

    assert :ok = Dispatcher.dispatch("SET", [key <> ":str", "v"], ctx)

    for args <- [
          ["HINCRBY", "n", "1"],
          ["HINCRBYFLOAT", "n", "1.5"],
          ["HSETNX", "f", "v"],
          ["HSETEX", "100", "f", "v"],
          ["ZADD", "NX", "1", "a"],
          ["GEOADD", "XX", "13.36", "38.11", "m"]
        ] do
      [command | rest] = args
      assert @wrongtype == Dispatcher.dispatch(command, [key <> ":str" | rest], ctx)
    end

    assert "v" == Dispatcher.dispatch("GET", [key <> ":str"], ctx)
  end

  test "float increments store and answer plain decimals, as Redis does", %{ctx: ctx, key: key} do
    d = fn command, args -> Dispatcher.dispatch(command, args, ctx) end

    assert "2.5" == d.("INCRBYFLOAT", [key <> ":f", "2.5"])
    assert "3" == d.("INCRBYFLOAT", [key <> ":f", "0.5"])
    assert "3" == d.("GET", [key <> ":f"])
    assert {:ok, 4} == d.("INCR", [key <> ":f"])
    assert "5200" == d.("INCRBYFLOAT", [key <> ":big", "5200"])
    assert "5200" == d.("GET", [key <> ":big"])
    assert "0.00000015" == d.("INCRBYFLOAT", [key <> ":tiny", "0.00000015"])
    assert "0.00000015" == d.("GET", [key <> ":tiny"])
    assert "-2.5" == d.("INCRBYFLOAT", [key <> ":neg", "-2.5"])

    assert "2.5" == d.("HINCRBYFLOAT", [key <> ":h", "f", "2.5"])
    assert "3" == d.("HINCRBYFLOAT", [key <> ":h", "f", "0.5"])
    assert "3" == d.("HGET", [key <> ":h", "f"])
    assert 4 == d.("HINCRBY", [key <> ":h", "f", "1"])
    assert "5200" == d.("HINCRBYFLOAT", [key <> ":h", "g", "5200"])
    assert "5200" == d.("HGET", [key <> ":h", "g"])

    queue =
      for args <- [
            ["INCRBYFLOAT", key <> ":m", "2.5"],
            ["INCRBYFLOAT", key <> ":m", "0.5"],
            ["HINCRBYFLOAT", key <> ":mh", "f", "5200"]
          ] do
        [command | rest] = args
        {:ok, prepared} = Ferricstore.Commands.PreparedCommand.prepare(command, rest)
        prepared
      end

    assert ["2.5", "3", "5200"] == Ferricstore.Transaction.Coordinator.execute(queue, %{}, nil)
    assert "3" == d.("GET", [key <> ":m"])
    assert "5200" == d.("HGET", [key <> ":mh", "f"])

    assert {:ok, 2.5} = FerricStore.incr_by_float(key <> ":e", 2.5)
    assert {:ok, 3.0} = FerricStore.incr_by_float(key <> ":e", 0.5)
    assert {:ok, "3"} = FerricStore.get(key <> ":e")
  end

  test "read-decide-write commands have one winner under concurrency", %{ctx: ctx, key: key} do
    d = fn command, args -> Dispatcher.dispatch(command, args, ctx) end

    assert 2 == d.("HSET", [key <> ":h", "f", "v", "g", "w"])
    assert 1 == winners(fn -> d.("HDEL", [key <> ":h", "f"]) end, &(&1 == 1))

    assert 1 ==
             winners(fn -> d.("HGETDEL", [key <> ":h", "FIELDS", "1", "g"]) end, &(&1 == ["w"]))

    assert 0 == d.("EXISTS", [key <> ":h"])

    assert 1 == winners(fn -> d.("HSETNX", [key <> ":nx", "f", "v"]) end, &(&1 == 1))
    assert 1 == winners(fn -> d.("ZADD", [key <> ":z", "NX", "1", "m"]) end, &(&1 == 1))
    assert 1 == winners(fn -> d.("ZADD", [key <> ":z", "CH", "2", "m"]) end, &(&1 == 1))
    assert 1 == winners(fn -> d.("ZADD", [key <> ":z", "GT", "CH", "3", "m"]) end, &(&1 == 1))
    assert "3.0" == d.("ZSCORE", [key <> ":z", "m"])

    assert 1 ==
             winners(
               fn -> d.("GEOADD", [key <> ":g", "NX", "13.36", "38.11", "m"]) end,
               &(&1 == 1)
             )
  end

  test "read-decide-write commands keep their replies", %{ctx: ctx, key: key} do
    d = fn command, args -> Dispatcher.dispatch(command, args, ctx) end

    assert 0 == d.("HDEL", [key <> ":missing", "f"])
    assert 3 == d.("HSET", [key <> ":h", "a", "1", "b", "2", "c", "3"])
    assert 2 == d.("HDEL", [key <> ":h", "a", "b", "a", "x"])
    assert [nil, "3"] == d.("HGETDEL", [key <> ":h", "FIELDS", "2", "a", "c"])
    assert 0 == d.("EXISTS", [key <> ":h"])
    assert 2 == d.("ZADD", [key <> ":z", "NX", "1", "a", "2", "b"])
    assert 1 == d.("ZADD", [key <> ":z", "XX", "CH", "5", "a", "1", "new"])
    assert ["b", "a"] == d.("ZRANGE", [key <> ":z", "0", "-1"])
    assert :ok = d.("SET", [key <> ":str", "v"])
    assert @wrongtype == d.("HDEL", [key <> ":str", "f"])
    assert @wrongtype == d.("ZADD", [key <> ":str", "NX", "1", "a"])
    assert @wrongtype == d.("HGETDEL", [key <> ":str", "FIELDS", "1", "f"])
  end

  defp winners(fun, winner?) do
    1..20
    |> Task.async_stream(fn _client -> fun.() end, max_concurrency: 20, timeout: 30_000)
    |> Enum.count(fn {:ok, result} -> winner?.(result) end)
  end

  test "MULTI queues the forms older releases apply differently", %{ctx: ctx, key: key} do
    alias Ferricstore.Transaction.ReplicaCompat

    assert ReplicaCompat.level_4_form?({:expire, key, 100, [:xx, :gt]})
    assert ReplicaCompat.level_4_form?({:bitcount, [key, "0", "-1"]})
    refute ReplicaCompat.level_4_form?({:expire, key, 100, :gt})
    assert nil == ReplicaCompat.queue_error("EXPIRE", {:expire, key, 100, [:xx, :gt]}, [key], ctx)
    assert nil == ReplicaCompat.queue_error("BITPOS", {:bitpos, [key, "1"]}, [key], ctx)
  end

  test "a hash keeps its members through the level-4 write paths", %{ctx: ctx, key: key} do
    fields = Enum.flat_map(1..150, &["f#{&1}", "v"])
    assert 150 == Dispatcher.dispatch("HSET", [key | fields], ctx)
    assert 3 == Dispatcher.dispatch("HINCRBY", [key, "f1x", "3"], ctx)
    assert 1 == Dispatcher.dispatch("HSETEX", [key, "100", "late", "v"], ctx)
    assert 152 == Dispatcher.dispatch("HLEN", [key], ctx)
  end
end
