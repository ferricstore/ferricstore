defmodule Ferricstore.Commands.PopMoveAtomicTest do
  @moduledoc """
  SPOP, ZPOPMIN and LMOVE through the command path the TCP and HTTP gateways
  use: concurrent clients each take a different element and none is lost.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher

  @members for i <- 1..40, do: "m#{i}"

  setup do
    tag = "{pop-move:#{System.unique_integer([:positive])}}"
    %{ctx: FerricStore.Instance.get(:default), key: tag}
  end

  test "concurrent SPOP pops each member once", %{ctx: ctx, key: key} do
    assert 40 == Dispatcher.dispatch("SADD", [key | @members], ctx)
    popped = concurrently(fn -> Dispatcher.dispatch("SPOP", [key], ctx) end)

    assert Enum.sort(popped) == Enum.sort(@members)
    assert 0 == Dispatcher.dispatch("SCARD", [key], ctx)
  end

  test "concurrent ZPOPMIN pops each member once", %{ctx: ctx, key: key} do
    pairs = @members |> Enum.with_index() |> Enum.flat_map(fn {m, i} -> ["#{i}", m] end)
    assert 40 == Dispatcher.dispatch("ZADD", [key | pairs], ctx)

    popped =
      concurrently(fn ->
        [member, _score] = Dispatcher.dispatch("ZPOPMIN", [key], ctx)
        member
      end)

    assert Enum.sort(popped) == Enum.sort(@members)
    assert 0 == Dispatcher.dispatch("ZCARD", [key], ctx)
  end

  test "concurrent LMOVE moves each element once and loses none", %{ctx: ctx, key: key} do
    source = key <> ":source"
    destination = key <> ":destination"
    assert 40 == Dispatcher.dispatch("RPUSH", [source | @members], ctx)

    moved =
      concurrently(fn ->
        Dispatcher.dispatch("LMOVE", [source, destination, "LEFT", "RIGHT"], ctx)
      end)

    assert Enum.sort(moved) == Enum.sort(@members)
    assert 0 == Dispatcher.dispatch("LLEN", [source], ctx)

    assert Enum.sort(Dispatcher.dispatch("LRANGE", [destination, "0", "-1"], ctx)) ==
             Enum.sort(@members)
  end

  defp concurrently(fun) do
    1..40
    |> Task.async_stream(fn _ -> fun.() end, max_concurrency: 40, timeout: 60_000)
    |> Enum.map(fn {:ok, result} -> result end)
  end
end
