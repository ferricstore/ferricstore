defmodule Ferricstore.Commands.TopKAccuracyTest do
  @moduledoc """
  TOPK.RESERVE without dimensions sizes its Count-Min sketch for k, so the
  heavy hitters of a skewed stream come out on top with counts close to the
  truth.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher

  @heavy Enum.map(1..10, &{"heavy-#{&1}", 100 + 10 * &1})
  @light Enum.map(1..1_000, &{"light-#{&1}", 2})

  setup do
    %{
      ctx: FerricStore.Instance.get(:default),
      key: "{topk:#{System.unique_integer([:positive])}}"
    }
  end

  test "a default TOPK.RESERVE finds the heavy hitters of a skewed stream", %{ctx: ctx, key: key} do
    assert :ok == Dispatcher.dispatch("TOPK.RESERVE", [key, "10"], ctx)
    feed(fn items -> Dispatcher.dispatch("TOPK.ADD", [key | items], ctx) end)

    listed = Dispatcher.dispatch("TOPK.LIST", [key, "WITHCOUNT"], ctx) |> pairs()

    assert Enum.sort(Map.keys(listed)) == Enum.sort(Enum.map(@heavy, &elem(&1, 0)))

    for {item, count} <- @heavy do
      assert listed[item] >= count and listed[item] <= count * 1.1,
             "#{item}: true #{count}, estimated #{listed[item]}"
    end
  end

  test "the embedded topk_reserve sizes the sketch the same way", %{key: key} do
    assert :ok == FerricStore.topk_reserve(key, 10)
    feed(fn items -> FerricStore.topk_add(key, items) end)

    assert {:ok, listed} = FerricStore.topk_list(key)
    assert Enum.sort(listed) == Enum.sort(Enum.map(@heavy, &elem(&1, 0)))
  end

  test "explicit dimensions are kept", %{ctx: ctx, key: key} do
    assert :ok == Dispatcher.dispatch("TOPK.RESERVE", [key, "50", "2000", "7"], ctx)

    assert ["k", 50, "width", 2000, "depth", 7] ==
             Dispatcher.dispatch("TOPK.INFO", [key], ctx)
  end

  defp feed(add) do
    (@heavy ++ @light)
    |> Enum.flat_map(fn {item, count} -> List.duplicate(item, count) end)
    |> Enum.shuffle()
    |> Enum.chunk_every(500)
    |> Enum.each(add)
  end

  defp pairs(flat) do
    flat
    |> Enum.chunk_every(2)
    |> Map.new(fn [item, count] -> {item, count} end)
  end
end
