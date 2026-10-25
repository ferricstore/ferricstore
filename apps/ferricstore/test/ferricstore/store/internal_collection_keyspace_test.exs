defmodule Ferricstore.Store.InternalCollectionKeyspaceTest do
  @moduledoc """
  KEYS hid the invocation definition catalog, an internal hash, but SCAN and
  RANDOMKEY returned it and DBSIZE counted it; clients are refused access to
  it.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher

  @catalog "f:{f}:invocation:definitions:1"

  setup do
    ctx = FerricStore.Instance.get(:default)
    Dispatcher.dispatch("FLUSHDB", [], ctx)
    on_exit(fn -> Dispatcher.dispatch("FLUSHDB", [], ctx) end)
    %{ctx: ctx}
  end

  test "SCAN, RANDOMKEY and DBSIZE leave out an internal collection", %{ctx: ctx} do
    definition = Jason.encode!(%{"name" => "keyspace-check", "enabled" => true})
    assert "OK" == Dispatcher.dispatch("INVOCATION.DEFINITION.PUT", [definition], ctx)
    assert :ok == Dispatcher.dispatch("SET", ["user:key", "v"], ctx)

    assert ["user:key"] == Dispatcher.dispatch("KEYS", ["*"], ctx)
    assert ["user:key"] == scan_all(ctx)
    assert 1 == Dispatcher.dispatch("DBSIZE", [], ctx)

    for _ <- 1..50, do: assert("user:key" == Dispatcher.dispatch("RANDOMKEY", [], ctx))
    refute @catalog in Dispatcher.dispatch("KEYS", ["*"], ctx)
  end

  defp scan_all(ctx, cursor \\ "0", acc \\ []) do
    [next, keys] = Dispatcher.dispatch("SCAN", [cursor, "COUNT", "1000"], ctx)
    acc = acc ++ keys
    if next == "0", do: Enum.sort(acc), else: scan_all(ctx, next, acc)
  end
end
