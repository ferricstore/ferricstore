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

  test "concurrent EXPIRE NX has one winner", %{ctx: ctx, key: key} do
    assert :ok = Dispatcher.dispatch("SET", [key, "v"], ctx)

    results =
      concurrently(fn client ->
        Dispatcher.dispatch("EXPIRE", [key, "#{100 + client}", "NX"], ctx)
      end)

    assert 1 == Enum.count(results, &(&1 == 1))
  end

  defp concurrently(fun) do
    1..20
    |> Task.async_stream(fun, max_concurrency: 20, timeout: 60_000)
    |> Enum.map(fn {:ok, result} -> result end)
  end
end
