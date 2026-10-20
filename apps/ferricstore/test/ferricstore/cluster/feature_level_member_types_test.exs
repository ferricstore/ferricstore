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

  test "a hash keeps its members through the level-4 write paths", %{ctx: ctx, key: key} do
    fields = Enum.flat_map(1..150, &["f#{&1}", "v"])
    assert 150 == Dispatcher.dispatch("HSET", [key | fields], ctx)
    assert 3 == Dispatcher.dispatch("HINCRBY", [key, "f1x", "3"], ctx)
    assert 1 == Dispatcher.dispatch("HSETEX", [key, "100", "late", "v"], ctx)
    assert 152 == Dispatcher.dispatch("HLEN", [key], ctx)
  end
end
