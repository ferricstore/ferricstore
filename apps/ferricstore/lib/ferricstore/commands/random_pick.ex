defmodule Ferricstore.Commands.RandomPick do
  @moduledoc false

  # SRANDMEMBER, HRANDFIELD and ZRANDMEMBER read every member to pick one: on
  # a 20,000-member set or hash about 25 ms, and 3.3 ms on a sorted set. On
  # the instance they take the collection's count (O(1)) and one positional
  # read per pick instead. A sorted set stored as a tree answers a position in
  # O(log N) (ZRANDMEMBER fell to 83 us), so up to @max_tree_picks picks read
  # that way. A set's or hash's read walks the member catalog to its position,
  # about a quarter of the collection on average (SRANDMEMBER fell to 4.9 ms),
  # and costs about what reading every member does per member, so only a
  # single pick reads that way. Past those, or when a positive count covers
  # the whole collection, the caller reads the collection once (`:scan`).

  alias Ferricstore.Store.Ops

  @max_tree_picks 8

  @doc false
  @spec max_positional_picks(binary()) :: pos_integer()
  def max_positional_picks(<<"Z:", _rest::binary>>), do: @max_tree_picks
  def max_positional_picks(_prefix), do: 1

  @doc false
  # The picked {member, value} pairs for `count`: nil for one member, a
  # positive count for that many distinct members, a negative one for that
  # many with repeats, as Redis picks them. :scan when the caller should read
  # the collection instead.
  @spec pairs(term(), binary(), binary(), integer() | nil) ::
          {:ok, [{binary(), binary()}]} | :scan
  def pairs(%FerricStore.Instance{} = ctx, key, prefix, count) do
    pick(
      fn -> Ops.compound_count(ctx, key, prefix) end,
      fn position, total -> Ops.compound_scan_slice(ctx, key, prefix, position, 1, total) end,
      count,
      max_positional_picks(prefix)
    )
  end

  def pairs(_store, _key, _prefix, _count), do: :scan

  @doc false
  @spec pick((-> term()), function(), integer() | nil, pos_integer()) ::
          {:ok, [{binary(), binary()}]} | :scan
  def pick(count_fun, slice_fun, count, max_picks) do
    if picks(count) > max_picks do
      :scan
    else
      case count_fun.() do
        0 -> {:ok, []}
        total when is_integer(total) and total > 0 -> pick_positions(slice_fun, count, total)
        # A failed count: the caller's read reports it.
        _failure -> :scan
      end
    end
  end

  defp picks(nil), do: 1
  defp picks(count), do: abs(count)

  defp pick_positions(_slice_fun, count, total) when is_integer(count) and count >= total,
    do: :scan

  defp pick_positions(slice_fun, count, total) do
    positions = positions(count, total)

    positions
    |> Enum.uniq()
    |> Enum.reduce_while(%{}, fn position, found ->
      case read_at(slice_fun, position, total) do
        [{_member, _value} = pair] -> {:cont, Map.put(found, position, pair)}
        # Nothing there (the collection shrank since the count) or a failed
        # read: the caller reads the collection and reports any failure.
        _other -> {:halt, :scan}
      end
    end)
    |> case do
      :scan -> :scan
      found -> {:ok, Enum.map(positions, &Map.fetch!(found, &1))}
    end
  end

  defp read_at(slice_fun, position, _total) when is_function(slice_fun, 1),
    do: slice_fun.(position)

  defp read_at(slice_fun, position, total), do: slice_fun.(position, total)

  defp positions(nil, total), do: [:rand.uniform(total) - 1]

  defp positions(count, total) when count < 0,
    do: for(_ <- 1..abs(count), do: :rand.uniform(total) - 1)

  # `count` distinct positions below `total` (Floyd's sampling), in random
  # order.
  defp positions(count, total) do
    (total - count)..(total - 1)//1
    |> Enum.reduce(MapSet.new(), fn bound, picked ->
      position = :rand.uniform(bound + 1) - 1
      MapSet.put(picked, if(MapSet.member?(picked, position), do: bound, else: position))
    end)
    |> Enum.shuffle()
  end
end
