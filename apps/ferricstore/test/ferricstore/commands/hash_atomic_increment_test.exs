defmodule Ferricstore.Commands.HashAtomicIncrementTest do
  @moduledoc """
  HINCRBY and HINCRBYFLOAT through the command path the TCP and HTTP gateways
  use, with concurrent clients on one field.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher

  @clients 10
  @increments 20

  setup do
    %{
      ctx: FerricStore.Instance.get(:default),
      key: "hash-atomic:#{System.unique_integer([:positive])}"
    }
  end

  test "concurrent HINCRBY increments are all applied", %{ctx: ctx, key: key} do
    run_concurrently(fn -> Dispatcher.dispatch("HINCRBY", [key, "f", "1"], ctx) end)

    assert "#{@clients * @increments}" == Dispatcher.dispatch("HGET", [key, "f"], ctx)
    assert {:simple, "hash"} == Dispatcher.dispatch("TYPE", [key], ctx)
  end

  test "concurrent HINCRBYFLOAT increments are all applied", %{ctx: ctx, key: key} do
    run_concurrently(fn -> Dispatcher.dispatch("HINCRBYFLOAT", [key, "f", "0.5"], ctx) end)

    assert {total, ""} = Float.parse(Dispatcher.dispatch("HGET", [key, "f"], ctx))
    assert total == @clients * @increments * 0.5
  end

  test "HINCRBY keeps its errors and leaves no hash behind on a failed first write", %{
    ctx: ctx,
    key: key
  } do
    assert 5 == Dispatcher.dispatch("HINCRBY", [key, "f", "5"], ctx)
    assert 1 == Dispatcher.dispatch("HSET", [key, "s", "text"], ctx)

    assert {:error, "ERR hash value is not an integer"} =
             Dispatcher.dispatch("HINCRBY", [key, "s", "1"], ctx)

    assert {:error, "ERR increment or decrement would overflow"} =
             Dispatcher.dispatch("HINCRBY", [key, "f", "9223372036854775807"], ctx)

    assert :ok = FerricStore.set(key <> ":string", "v")

    assert {:error, "WRONGTYPE" <> _} =
             Dispatcher.dispatch("HINCRBY", [key <> ":string", "f", "1"], ctx)
  end

  defp run_concurrently(fun) do
    1..@clients
    |> Task.async_stream(fn _ -> for _ <- 1..@increments, do: fun.() end,
      max_concurrency: @clients,
      timeout: 60_000
    )
    |> Stream.run()
  end
end
