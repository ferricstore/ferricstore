defmodule Ferricstore.Commands.ZsetHllAtomicTest do
  @moduledoc """
  ZINCRBY and PFADD through the command path the TCP and HTTP gateways use,
  with concurrent clients on one key.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher

  @clients 10
  @increments 20

  setup do
    %{
      ctx: FerricStore.Instance.get(:default),
      key: "zset-hll-atomic:#{System.unique_integer([:positive])}"
    }
  end

  test "concurrent ZINCRBY increments are all applied", %{ctx: ctx, key: key} do
    run_concurrently(fn _client, _n -> Dispatcher.dispatch("ZINCRBY", [key, "1", "m"], ctx) end)

    assert "#{@clients * @increments}.0" == Dispatcher.dispatch("ZSCORE", [key, "m"], ctx)
    assert ["m"] == Dispatcher.dispatch("ZRANGE", [key, "0", "-1"], ctx)
  end

  test "ZINCRBY keeps its reply format and errors", %{ctx: ctx, key: key} do
    assert "0.1" == Dispatcher.dispatch("ZINCRBY", [key, "0.1", "m"], ctx)
    assert "0.30000000000000004" == Dispatcher.dispatch("ZINCRBY", [key, "0.2", "m"], ctx)
    assert :ok = FerricStore.set(key <> ":string", "v")

    assert {:error, "WRONGTYPE" <> _} =
             Dispatcher.dispatch("ZINCRBY", [key <> ":string", "1", "m"], ctx)
  end

  test "concurrent PFADD keeps every element", %{ctx: ctx, key: key} do
    run_concurrently(fn client, n ->
      Dispatcher.dispatch("PFADD", [key, "e#{client}-#{n}"], ctx)
    end)

    count = Dispatcher.dispatch("PFCOUNT", [key], ctx)
    expected = @clients * @increments

    assert count >= expected * 0.95 and count <= expected * 1.05,
           "PFCOUNT #{count} for #{expected} elements"
  end

  defp run_concurrently(fun) do
    1..@clients
    |> Task.async_stream(fn client -> for n <- 1..@increments, do: fun.(client, n) end,
      max_concurrency: @clients,
      timeout: 60_000
    )
    |> Stream.run()
  end
end
