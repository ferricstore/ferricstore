defmodule Ferricstore.Commands.MemberBatchAtomicTest do
  @moduledoc """
  SADD, SREM, ZREM and HSET with several members, and ZADD and GEOADD without
  options, write through the state machine's single-member entries, which
  check the key's type and report their own change. Concurrent clients then
  count each member once, and a SET racing the write leaves no member hidden
  under the string, to come back when the key is recreated.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  @clients 20
  @rounds 30
  @wrongtype {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

  setup do
    %{
      ctx: FerricStore.Instance.get(:default),
      key: "{batch:#{System.unique_integer([:positive])}}"
    }
  end

  test "concurrent multi-member writes count each member once", %{ctx: ctx, key: key} do
    assert 2 == replies_sum(fn -> Dispatcher.dispatch("SADD", [key <> ":s", "a", "b"], ctx) end)
    assert 2 == replies_sum(fn -> Dispatcher.dispatch("SREM", [key <> ":s", "a", "b"], ctx) end)

    assert 2 ==
             replies_sum(fn ->
               Dispatcher.dispatch("ZADD", [key <> ":z", "1", "a", "2", "b"], ctx)
             end)

    assert 1 == replies_sum(fn -> Dispatcher.dispatch("ZADD", [key <> ":z", "3", "c"], ctx) end)
    assert 2 == replies_sum(fn -> Dispatcher.dispatch("ZREM", [key <> ":z", "a", "b"], ctx) end)

    assert 2 ==
             replies_sum(fn ->
               Dispatcher.dispatch("HSET", [key <> ":h", "f", "1", "g", "2"], ctx)
             end)

    assert 1 ==
             replies_sum(fn ->
               Dispatcher.dispatch("GEOADD", [key <> ":g", "13.36", "38.11", "m"], ctx)
             end)
  end

  test "the embedded API counts each member once", %{key: key} do
    assert 2 == embedded_sum(fn -> FerricStore.sadd(key <> ":s", ["a", "b"]) end)
    assert 2 == embedded_sum(fn -> FerricStore.srem(key <> ":s", ["a", "b"]) end)
    assert 2 == embedded_sum(fn -> FerricStore.zadd(key <> ":z", [{1.0, "a"}, {2.0, "b"}]) end)
    assert 2 == embedded_sum(fn -> FerricStore.zrem(key <> ":z", ["a", "b"]) end)
  end

  for {name, write, recreate, check} <- [
        {"SADD", ["SADD", "m", "n"], ["SADD", "o"], ["SISMEMBER", "m"]},
        {"SREM", ["SREM", "m", "n"], ["SADD", "o"], ["SISMEMBER", "m"]},
        {"ZADD", ["ZADD", "1", "m", "2", "n"], ["ZADD", "1", "o"], ["ZSCORE", "m"]},
        {"ZADD one member", ["ZADD", "1", "m"], ["ZADD", "1", "o"], ["ZSCORE", "m"]},
        {"HSET", ["HSET", "m", "1", "n", "2"], ["HSET", "o", "1"], ["HEXISTS", "m"]},
        {"GEOADD", ["GEOADD", "13.36", "38.11", "m"], ["ZADD", "1", "o"], ["ZSCORE", "m"]}
      ] do
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

          if command == "SREM",
            do: Dispatcher.dispatch("SADD", [round_key, "m", "n"], ctx)

          writer = Task.async(fn -> Dispatcher.dispatch(command, [round_key | args], ctx) end)
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

  test "replies, duplicates and type errors keep their meaning", %{ctx: ctx, key: key} do
    assert 2 == Dispatcher.dispatch("SADD", [key <> ":s", "a", "a", "b"], ctx)
    assert 0 == Dispatcher.dispatch("SADD", [key <> ":s", "a", "b"], ctx)
    assert 1 == Dispatcher.dispatch("SREM", [key <> ":s", "a", "a", "x"], ctx)
    assert ["b"] == Dispatcher.dispatch("SMEMBERS", [key <> ":s"], ctx)

    assert 1 == Dispatcher.dispatch("ZADD", [key <> ":z", "1", "a", "2", "a"], ctx)
    assert "2.0" == Dispatcher.dispatch("ZSCORE", [key <> ":z", "a"], ctx)
    assert 1 == Dispatcher.dispatch("ZADD", [key <> ":z", "5", "a", "1", "b"], ctx)
    assert ["b", "a"] == Dispatcher.dispatch("ZRANGE", [key <> ":z", "0", "-1"], ctx)
    assert 1 == Dispatcher.dispatch("ZREM", [key <> ":z", "a", "x"], ctx)
    assert 1 == Dispatcher.dispatch("ZCARD", [key <> ":z"], ctx)

    assert 1 == Dispatcher.dispatch("HSET", [key <> ":h", "f", "1", "f", "2"], ctx)
    assert "2" == Dispatcher.dispatch("HGET", [key <> ":h", "f"], ctx)
    assert 1 == Dispatcher.dispatch("HSET", [key <> ":h", "f", "3", "g", "4"], ctx)
    assert %{"f" => "3", "g" => "4"} == hgetall(ctx, key <> ":h")

    assert 1 == Dispatcher.dispatch("GEOADD", [key <> ":g", "13.36", "38.11", "m"], ctx)
    assert 0 == Dispatcher.dispatch("GEOADD", [key <> ":g", "13.36", "38.11", "m"], ctx)
    assert [[_lon, _lat]] = Dispatcher.dispatch("GEOPOS", [key <> ":g", "m"], ctx)

    assert :ok = Dispatcher.dispatch("SET", [key <> ":str", "v"], ctx)

    for args <- [
          ["SADD", "a", "b"],
          ["SREM", "a", "b"],
          ["ZADD", "1", "a", "2", "b"],
          ["ZADD", "1", "a"],
          ["ZREM", "a", "b"],
          ["HSET", "f", "1", "g", "2"],
          ["GEOADD", "13.36", "38.11", "m"]
        ] do
      [command | rest] = args
      assert @wrongtype == Dispatcher.dispatch(command, [key <> ":str" | rest], ctx)
    end

    assert "v" == Dispatcher.dispatch("GET", [key <> ":str"], ctx)
    assert 0 == Dispatcher.dispatch("SREM", [key <> ":missing", "a", "b"], ctx)
    assert 0 == Dispatcher.dispatch("ZREM", [key <> ":missing", "a", "b"], ctx)
  end

  test "large member lists are written in full", %{ctx: ctx, key: key} do
    members = Enum.map(1..700, &"m#{&1}")

    assert 700 == Dispatcher.dispatch("SADD", [key <> ":s" | members], ctx)
    assert 700 == Dispatcher.dispatch("SCARD", [key <> ":s"], ctx)
    assert 350 == Dispatcher.dispatch("SREM", [key <> ":s" | Enum.take(members, 350)], ctx)
    assert 350 == Dispatcher.dispatch("SCARD", [key <> ":s"], ctx)

    zadd_args = Enum.flat_map(Enum.with_index(members), fn {m, i} -> ["#{i}", m] end)
    assert 700 == Dispatcher.dispatch("ZADD", [key <> ":z" | zadd_args], ctx)
    assert 700 == Dispatcher.dispatch("ZCARD", [key <> ":z"], ctx)
    assert ["m1", "m2"] == Dispatcher.dispatch("ZRANGE", [key <> ":z", "0", "1"], ctx)
    assert 350 == Dispatcher.dispatch("ZREM", [key <> ":z" | Enum.take(members, 350)], ctx)
    assert 350 == Dispatcher.dispatch("ZCARD", [key <> ":z"], ctx)

    hset_args = Enum.flat_map(members, &[&1, "v"])
    assert 700 == Dispatcher.dispatch("HSET", [key <> ":h" | hset_args], ctx)
    assert 700 == Dispatcher.dispatch("HLEN", [key <> ":h"], ctx)
  end

  test "a batch on a promoted collection ends as the single-member entries one by one",
       %{ctx: ctx, key: key} do
    seed = Enum.map(1..150, &"seed#{&1}")
    seed_scores = Enum.flat_map(Enum.with_index(seed), fn {m, i} -> ["#{i}", m] end)

    for target <- [key <> ":a", key <> ":b"] do
      assert 150 == Dispatcher.dispatch("ZADD", [target <> ":z", "CH" | seed_scores], ctx)

      assert "1.0e20" ==
               Dispatcher.dispatch("ZINCRBY", [target <> ":z", "1e20", "big"], ctx) |> canonical()

      assert 150 == Dispatcher.dispatch("SADD", [target <> ":s" | seed], ctx)
    end

    for target <- [key <> ":a", key <> ":b"] do
      wait_promoted(ctx, target <> ":z")
      wait_promoted(ctx, target <> ":s")
    end

    zadds = [
      {1.0, "new"},
      {2.0, "new"},
      {3.0, "seed5"},
      {6.0, "seed6"},
      {1.0e20, "big"},
      {4.0, "other"},
      {4.0, "other"}
    ]

    grouped =
      Dispatcher.dispatch(
        "ZADD",
        [key <> ":a:z" | Enum.flat_map(zadds, fn {s, m} -> [Float.to_string(s), m] end)],
        ctx
      )

    one_by_one =
      Enum.map(zadds, fn {score, member} ->
        [reply] =
          Router.pipeline_write_batch(ctx, [
            {key <> ":b:z", {:zadd_single, key <> ":b:z", score, member}}
          ])

        reply
      end)

    assert grouped == Enum.sum(one_by_one)
    assert zrange(ctx, key <> ":a:z") == zrange(ctx, key <> ":b:z")
    assert Dispatcher.dispatch("ZCARD", [key <> ":a:z"], ctx) == 153

    sadds = ["seed1", "fresh", "fresh", "seed2", "another"]
    grouped = Dispatcher.dispatch("SADD", [key <> ":a:s" | sadds], ctx)

    one_by_one =
      Enum.map(sadds, fn member ->
        [reply] =
          Router.pipeline_write_batch(ctx, [
            {key <> ":b:s", {:sadd_single, key <> ":b:s", member}}
          ])

        reply
      end)

    assert grouped == 2 and Enum.sum(one_by_one) == 2

    assert Enum.sort(Dispatcher.dispatch("SMEMBERS", [key <> ":a:s"], ctx)) ==
             Enum.sort(Dispatcher.dispatch("SMEMBERS", [key <> ":b:s"], ctx))
  end

  defp zrange(ctx, key), do: Dispatcher.dispatch("ZRANGE", [key, "0", "-1", "WITHSCORES"], ctx)

  defp canonical(score), do: score |> String.to_float() |> Float.to_string()

  defp wait_promoted(ctx, key) do
    ShardHelpers.eventually(
      fn ->
        GenServer.call(Router.shard_name(ctx, Router.shard_for(ctx, key)), {:promoted?, key})
      end,
      "expected #{inspect(key)} to finish promotion"
    )
  end

  defp replies_sum(fun) do
    1..@clients
    |> Task.async_stream(fn _client -> fun.() end, max_concurrency: @clients, timeout: 30_000)
    |> Enum.map(fn {:ok, result} -> result end)
    |> Enum.sum()
  end

  defp embedded_sum(fun) do
    1..@clients
    |> Task.async_stream(fn _client -> fun.() end, max_concurrency: @clients, timeout: 30_000)
    |> Enum.map(fn {:ok, {:ok, count}} -> count end)
    |> Enum.sum()
  end

  defp hgetall(ctx, key) do
    ctx
    |> then(&Dispatcher.dispatch("HGETALL", [key], &1))
    |> Enum.chunk_every(2)
    |> Map.new(fn [field, value] -> {field, value} end)
  end
end
