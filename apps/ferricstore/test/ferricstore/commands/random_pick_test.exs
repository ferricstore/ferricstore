defmodule Ferricstore.Commands.RandomPickTest do
  @moduledoc """
  SRANDMEMBER, HRANDFIELD and ZRANDMEMBER pick members with the collection's
  count and one positional read per pick, not by reading every member, up to
  `RandomPick`'s pick limit; past it, or when a read finds nothing at its
  position, the caller reads the collection once.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.RandomPick

  defp reader(total) do
    test = self()

    slice = fn position ->
      send(test, {:slice, position})
      if position < total, do: [{"m#{position}", "v#{position}"}], else: []
    end

    {fn -> total end, slice}
  end

  defp slices do
    receive do
      {:slice, position} -> [position | slices()]
    after
      0 -> []
    end
  end

  test "one member takes one positional read" do
    {count, slice} = reader(1_000)

    assert {:ok, [{"m" <> _, "v" <> _}]} = RandomPick.pick(count, slice, nil, 8)
    assert [_one] = slices()
  end

  test "a positive count takes that many distinct members" do
    {count, slice} = reader(1_000)

    assert {:ok, pairs} = RandomPick.pick(count, slice, 5, 8)
    assert 5 == pairs |> Enum.uniq() |> length()
    assert 5 == slices() |> Enum.uniq() |> length()
  end

  test "a negative count takes that many, repeats allowed" do
    {count, slice} = reader(1)

    assert {:ok, [pair, pair, pair]} = RandomPick.pick(count, slice, -3, 8)
    assert [0] = slices()
  end

  test "sorted sets take several positional picks, sets and hashes one" do
    assert 8 == RandomPick.max_positional_picks("Z:key\0")
    assert 1 == RandomPick.max_positional_picks("S:key\0")
    assert 1 == RandomPick.max_positional_picks("H:key\0")

    {count, slice} = reader(1_000)
    assert {:ok, [_pair]} = RandomPick.pick(count, slice, nil, 1)
    assert :scan = RandomPick.pick(count, slice, 2, 1)
  end

  test "an empty collection gives nothing without a read" do
    {count, slice} = reader(0)

    assert {:ok, []} = RandomPick.pick(count, slice, nil, 8)
    assert {:ok, []} = RandomPick.pick(count, slice, 3, 8)
    assert [] = slices()
  end

  test "the caller reads the collection past the pick limit or for all of it" do
    {count, slice} = reader(1_000)

    assert :scan = RandomPick.pick(count, slice, 9, 8)
    assert :scan = RandomPick.pick(count, slice, -9, 8)

    {count, slice} = reader(4)
    assert :scan = RandomPick.pick(count, slice, 4, 8)
    assert [] = slices()
  end

  test "a read that finds nothing at its position leaves it to the caller" do
    # The count says 10, but the collection shrank to 0 before the read.
    {_count, slice} = reader(0)

    assert :scan = RandomPick.pick(fn -> 10 end, slice, nil, 8)
  end
end
