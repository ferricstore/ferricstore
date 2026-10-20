defmodule Ferricstore.Commands.SetMemberAtomicTest do
  @moduledoc """
  Single-member SADD, SREM and ZREM report whether they changed the key from
  one replicated entry, so concurrent clients see exactly one winner, as Redis
  clients relying on SADD for deduplication expect.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher

  @clients 20
  @wrongtype {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

  setup do
    %{
      ctx: FerricStore.Instance.get(:default),
      key: "set-member-atomic:#{System.unique_integer([:positive])}"
    }
  end

  test "concurrent SADD of one member has one winner", %{ctx: ctx, key: key} do
    results = concurrently(fn -> Dispatcher.dispatch("SADD", [key, "m"], ctx) end)

    assert 1 == Enum.count(results, &(&1 == 1))
    assert ["m"] == Dispatcher.dispatch("SMEMBERS", [key], ctx)
  end

  test "concurrent SREM of one member has one winner", %{ctx: ctx, key: key} do
    assert 2 == Dispatcher.dispatch("SADD", [key, "m", "other"], ctx)

    results = concurrently(fn -> Dispatcher.dispatch("SREM", [key, "m"], ctx) end)

    assert 1 == Enum.count(results, &(&1 == 1))
    assert ["other"] == Dispatcher.dispatch("SMEMBERS", [key], ctx)
  end

  test "concurrent ZREM of one member has one winner", %{ctx: ctx, key: key} do
    assert 2 == Dispatcher.dispatch("ZADD", [key, "1", "m", "2", "other"], ctx)

    results = concurrently(fn -> Dispatcher.dispatch("ZREM", [key, "m"], ctx) end)

    assert 1 == Enum.count(results, &(&1 == 1))
    assert ["other"] == Dispatcher.dispatch("ZRANGE", [key, "0", "-1"], ctx)
  end

  test "the embedded sadd, srem and zrem have one winner", %{key: key} do
    results = concurrently(fn -> FerricStore.sadd(key, ["m"]) end)
    assert 1 == Enum.count(results, &(&1 == {:ok, 1}))

    results = concurrently(fn -> FerricStore.srem(key, ["m"]) end)
    assert 1 == Enum.count(results, &(&1 == {:ok, 1}))

    assert {:ok, 1} = FerricStore.zadd(key <> ":z", [{1.0, "m"}])
    results = concurrently(fn -> FerricStore.zrem(key <> ":z", ["m"]) end)
    assert 1 == Enum.count(results, &(&1 == {:ok, 1}))
  end

  test "single-member writes keep their replies and type errors", %{ctx: ctx, key: key} do
    assert :ok = Dispatcher.dispatch("SET", [key <> ":string", "v"], ctx)
    assert @wrongtype == Dispatcher.dispatch("SADD", [key <> ":string", "m"], ctx)
    assert @wrongtype == Dispatcher.dispatch("SREM", [key <> ":string", "m"], ctx)
    assert @wrongtype == Dispatcher.dispatch("ZREM", [key <> ":string", "m"], ctx)
    assert "v" == Dispatcher.dispatch("GET", [key <> ":string"], ctx)

    assert 0 == Dispatcher.dispatch("SREM", [key <> ":missing", "m"], ctx)
    assert 0 == Dispatcher.dispatch("ZREM", [key <> ":missing", "m"], ctx)

    assert 1 == Dispatcher.dispatch("SADD", [key, "m"], ctx)
    assert 0 == Dispatcher.dispatch("SADD", [key, "m"], ctx)
    assert {:simple, "set"} == Dispatcher.dispatch("TYPE", [key], ctx)
    assert 1 == Dispatcher.dispatch("SREM", [key, "m"], ctx)
    assert 0 == Dispatcher.dispatch("EXISTS", [key], ctx)

    assert 1 == Dispatcher.dispatch("ZADD", [key <> ":z", "1", "m"], ctx)
    assert @wrongtype == Dispatcher.dispatch("SADD", [key <> ":z", "m"], ctx)
    assert 1 == Dispatcher.dispatch("ZREM", [key <> ":z", "m"], ctx)
    assert {:simple, "none"} == Dispatcher.dispatch("TYPE", [key <> ":z"], ctx)
  end

  defp concurrently(fun) do
    1..@clients
    |> Task.async_stream(fn _client -> fun.() end, max_concurrency: @clients, timeout: 30_000)
    |> Enum.map(fn {:ok, result} -> result end)
  end
end
