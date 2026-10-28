defmodule Ferricstore.Commands.MemoryUsageCollectionsTest do
  @moduledoc """
  MEMORY USAGE estimates a collection from its member count and a sample of
  its members (SAMPLES, default 5; 0 reads them all), as Redis does.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher

  setup do
    %{ctx: FerricStore.Instance.get(:default), id: System.unique_integer([:positive])}
  end

  test "every collection type has a size that grows with its members", %{ctx: ctx, id: id} do
    for {type, fill} <- [
          hash: &Dispatcher.dispatch("HSET", [&1 | Enum.flat_map(&2, fn m -> [m, m] end)], &3),
          list: &Dispatcher.dispatch("RPUSH", [&1 | &2], &3),
          set: &Dispatcher.dispatch("SADD", [&1 | &2], &3),
          zset: &Dispatcher.dispatch("ZADD", [&1 | Enum.flat_map(&2, fn m -> ["1", m] end)], &3),
          stream: fn key, members, ctx ->
            for m <- members, do: Dispatcher.dispatch("XADD", [key, "*", "f", m], ctx)
          end
        ] do
      small = "{mem:#{id}}:#{type}:small"
      large = "{mem:#{id}}:#{type}:large"
      fill.(small, members(10), ctx)
      fill.(large, members(100), ctx)

      small_usage = Dispatcher.dispatch("MEMORY", ["USAGE", small], ctx)
      large_usage = Dispatcher.dispatch("MEMORY", ["USAGE", large], ctx)

      assert is_integer(small_usage) and small_usage > 10 * 32, "#{type}: #{inspect(small_usage)}"
      assert is_integer(large_usage) and large_usage > 5 * small_usage, "#{type}: #{large_usage}"

      assert is_integer(Dispatcher.dispatch("MEMORY", ["USAGE", large, "SAMPLES", "0"], ctx)),
             "#{type} with SAMPLES 0"
    end
  end

  test "SAMPLES 0 measures every member", %{ctx: ctx, id: id} do
    key = "{mem:#{id}}:skewed"
    big = String.duplicate("x", 10_000)
    members = members(9)
    assert 10 == Dispatcher.dispatch("SADD", [key, big | members], ctx)

    exact = Dispatcher.dispatch("MEMORY", ["USAGE", key, "SAMPLES", "0"], ctx)
    assert exact > 10_000
    assert exact < 10_000 + 10 * 1_000
  end

  defp members(count), do: for(i <- 1..count, do: "member-#{String.pad_leading("#{i}", 24, "0")}")
end
