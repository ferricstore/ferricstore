defmodule Ferricstore.Commands.ReadDecideWriteLatchTest do
  @moduledoc """
  Commands that read a key and decide their write from it, through the command
  path the TCP and HTTP gateways use: one of many concurrent clients wins.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher

  setup do
    tag = "{latched:#{System.unique_integer([:positive])}}"
    %{ctx: FerricStore.Instance.get(:default), key: tag}
  end

  test "concurrent HGETDEL hands the field to one client", %{ctx: ctx, key: key} do
    assert 1 == Dispatcher.dispatch("HSET", [key, "f", "v"], ctx)

    results =
      concurrently(fn _ -> Dispatcher.dispatch("HGETDEL", [key, "FIELDS", "1", "f"], ctx) end)

    assert 1 == Enum.count(results, &(&1 == ["v"]))
  end

  test "concurrent SMOVE of one member has one winner", %{ctx: ctx, key: key} do
    assert 1 == Dispatcher.dispatch("SADD", [key <> ":from", "only"], ctx)

    results =
      concurrently(fn _ ->
        Dispatcher.dispatch("SMOVE", [key <> ":from", key <> ":to", "only"], ctx)
      end)

    assert 1 == Enum.count(results, &(&1 == 1))
    assert 1 == Dispatcher.dispatch("SCARD", [key <> ":to"], ctx)
  end

  # SMOVE latches its source, but SPOP and SREM do not take that latch: a
  # member SPOP took between SMOVE's read and its write still reached the
  # destination, and a later SPOP there handed it out a second time.
  test "concurrent SMOVE and SPOP hand out each member once", %{ctx: ctx, key: key} do
    for round <- 1..3 do
      from = "#{key}:from#{round}"
      to = "#{key}:to#{round}"
      members = for i <- 1..100, do: "m#{i}"
      assert 100 == Dispatcher.dispatch("SADD", [from | members], ctx)

      popped =
        1..32
        |> Task.async_stream(
          fn client ->
            Enum.flat_map(1..10, fn i ->
              if rem(client + i, 2) == 0 do
                [Dispatcher.dispatch("SPOP", [from], ctx), Dispatcher.dispatch("SPOP", [to], ctx)]
              else
                _ = Dispatcher.dispatch("SMOVE", [from, to, Enum.random(members)], ctx)
                []
              end
            end)
          end,
          max_concurrency: 32,
          timeout: 60_000
        )
        |> Enum.flat_map(fn {:ok, result} -> result end)
        |> Enum.reject(&is_nil/1)

      left =
        Dispatcher.dispatch("SMEMBERS", [from], ctx) ++ Dispatcher.dispatch("SMEMBERS", [to], ctx)

      all = popped ++ left

      assert [] == all -- Enum.uniq(all), "handed out twice in round #{round}"
      assert Enum.sort(members) == Enum.sort(all)
    end
  end

  # SETBIT read the whole value, changed one bit and wrote the value back, so
  # concurrent clients setting different bits overwrote each other's.
  test "concurrent SETBIT of different bits keeps every bit", %{ctx: ctx, key: key} do
    assert :ok = Dispatcher.dispatch("SET", [key, "", "EX", "100"], ctx)

    1..16
    |> Task.async_stream(
      fn client ->
        for i <- 1..40, do: Dispatcher.dispatch("SETBIT", [key, "#{client * 40 + i}", "1"], ctx)
      end,
      max_concurrency: 16,
      timeout: 60_000
    )
    |> Stream.run()

    assert 640 == Dispatcher.dispatch("BITCOUNT", [key], ctx)
    assert Dispatcher.dispatch("TTL", [key], ctx) in 95..100
  end

  test "concurrent EXPIRE NX has one winner", %{ctx: ctx, key: key} do
    assert :ok = Dispatcher.dispatch("SET", [key, "v"], ctx)

    results =
      concurrently(fn client ->
        Dispatcher.dispatch("EXPIRE", [key, "#{100 + client}", "NX"], ctx)
      end)

    assert 1 == Enum.count(results, &(&1 == 1))
  end

  test "concurrent GEOADD NX has one winner", %{ctx: ctx, key: key} do
    results =
      concurrently(fn client ->
        Dispatcher.dispatch("GEOADD", [key, "NX", "13.#{client}", "38.1", "m"], ctx)
      end)

    assert 1 == Enum.count(results, &(&1 == 1))
  end

  defp concurrently(fun) do
    1..20
    |> Task.async_stream(fun, max_concurrency: 20, timeout: 60_000)
    |> Enum.map(fn {:ok, result} -> result end)
  end
end
