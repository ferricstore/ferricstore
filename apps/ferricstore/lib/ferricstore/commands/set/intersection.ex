defmodule Ferricstore.Commands.Set.Intersection do
  @moduledoc false

  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops

  def sinter_set(keys, store) do
    keys
    |> count_sets(store)
    |> intersection_from_counted_keys(store)
  end

  def sinter_count(keys, limit, store) do
    counted = count_sets(keys, store)

    if Enum.any?(counted, fn {_key, count} -> count == 0 end) do
      0
    else
      counted
      |> pop_smallest_set()
      |> count_intersection_candidates(limit, store)
    end
  end

  defp count_sets(keys, store) do
    Enum.map(keys, fn key ->
      {key, Ops.compound_count(store, key, CompoundKey.set_prefix(key))}
    end)
  end

  defp intersection_from_counted_keys([], _store), do: MapSet.new()

  defp intersection_from_counted_keys(counted, store) do
    if Enum.any?(counted, fn {_key, count} -> count == 0 end) do
      MapSet.new()
    else
      {{base_key, _count}, rest} = pop_smallest_set(counted)

      base_key
      |> get_members_list(store)
      |> filter_members_in_all_sets(rest, store)
      |> MapSet.new()
    end
  end

  defp pop_smallest_set([{_key, _count} | _] = counted) do
    smallest_index =
      counted
      |> Enum.with_index()
      |> Enum.min_by(fn {{_key, count}, _index} -> count end)
      |> elem(1)

    List.pop_at(counted, smallest_index)
  end

  defp count_intersection_candidates({{base_key, _count}, rest}, limit, store) do
    members = get_members_list(base_key, store)

    if limit > 0 do
      members
      |> Enum.chunk_every(128)
      |> Enum.reduce_while(0, fn chunk, count ->
        matched_count = chunk |> filter_members_in_all_sets(rest, store) |> length()
        next_count = count + matched_count

        if next_count >= limit do
          {:halt, limit}
        else
          {:cont, next_count}
        end
      end)
    else
      members
      |> filter_members_in_all_sets(rest, store)
      |> length()
    end
  end

  defp filter_members_in_all_sets([], _counted_keys, _store), do: []
  defp filter_members_in_all_sets(members, [], _store), do: members

  defp filter_members_in_all_sets(members, counted_keys, store) do
    Enum.reduce_while(counted_keys, members, fn
      {_key, _count}, [] ->
        {:halt, []}

      {key, _count}, candidates ->
        compound_keys = Enum.map(candidates, &CompoundKey.set_member(key, &1))
        values = Ops.compound_batch_get(store, key, compound_keys)

        next_candidates =
          candidates
          |> Enum.zip(values)
          |> Enum.reduce([], fn
            {member, value}, acc when not is_nil(value) -> [member | acc]
            {_member, nil}, acc -> acc
          end)
          |> Enum.reverse()

        {:cont, next_candidates}
    end)
  end

  defp get_members_list(key, store) do
    prefix = CompoundKey.set_prefix(key)
    pairs = Ops.compound_scan(store, key, prefix)
    Enum.map(pairs, fn {member, _} -> member end)
  end
end
