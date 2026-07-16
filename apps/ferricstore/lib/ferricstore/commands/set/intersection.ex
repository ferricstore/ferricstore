defmodule Ferricstore.Commands.Set.Intersection do
  @moduledoc false

  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.ReadResult

  def sinter_set(keys, store) do
    with {:ok, counted} <- count_sets(keys, store) do
      intersection_from_counted_keys(counted, store)
    end
  end

  def sinter_count(keys, limit, store) do
    with {:ok, counted} <- count_sets(keys, store) do
      if Enum.any?(counted, fn {_key, count} -> count == 0 end) do
        {:ok, 0}
      else
        counted
        |> pop_smallest_set()
        |> count_intersection_candidates(limit, store)
      end
    end
  end

  defp count_sets(keys, store) do
    keys
    |> Enum.reduce_while({:ok, []}, fn key, {:ok, counted} ->
      case Ops.compound_count(store, key, CompoundKey.set_prefix(key)) do
        count when is_integer(count) and count >= 0 ->
          {:cont, {:ok, [{key, count} | counted]}}

        {:error, {:storage_read_failed, _reason}} = failure ->
          {:halt, ReadResult.command_error(failure)}

        invalid ->
          {:invalid_compound_count_result, invalid}
          |> ReadResult.failure()
          |> ReadResult.command_error()
          |> then(&{:halt, &1})
      end
    end)
    |> case do
      {:ok, counted} -> {:ok, Enum.reverse(counted)}
      error -> error
    end
  end

  defp intersection_from_counted_keys([], _store), do: {:ok, MapSet.new()}

  defp intersection_from_counted_keys(counted, store) do
    if Enum.any?(counted, fn {_key, count} -> count == 0 end) do
      {:ok, MapSet.new()}
    else
      {{base_key, _count}, rest} = pop_smallest_set(counted)

      with {:ok, members} <- get_members_list(base_key, store),
           {:ok, members} <- filter_members_in_all_sets(members, rest, store) do
        {:ok, MapSet.new(members)}
      end
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
    with {:ok, members} <- get_members_list(base_key, store) do
      if limit > 0 do
        members
        |> Enum.chunk_every(128)
        |> Enum.reduce_while({:ok, 0}, fn chunk, {:ok, count} ->
          case filter_members_in_all_sets(chunk, rest, store) do
            {:ok, matched} ->
              next_count = count + length(matched)

              if next_count >= limit do
                {:halt, {:ok, limit}}
              else
                {:cont, {:ok, next_count}}
              end

            error ->
              {:halt, error}
          end
        end)
      else
        with {:ok, matched} <- filter_members_in_all_sets(members, rest, store) do
          {:ok, length(matched)}
        end
      end
    end
  end

  defp filter_members_in_all_sets([], _counted_keys, _store), do: {:ok, []}
  defp filter_members_in_all_sets(members, [], _store), do: {:ok, members}

  defp filter_members_in_all_sets(members, counted_keys, store) do
    Enum.reduce_while(counted_keys, {:ok, members}, fn
      {_key, _count}, {:ok, []} ->
        {:halt, {:ok, []}}

      {key, _count}, {:ok, candidates} ->
        compound_keys = Enum.map(candidates, &CompoundKey.set_member(key, &1))
        values = Ops.compound_batch_get(store, key, compound_keys)

        case ReadResult.first_failure(values) do
          nil ->
            next_candidates =
              candidates
              |> Enum.zip(values)
              |> Enum.reduce([], fn
                {member, value}, acc when not is_nil(value) -> [member | acc]
                {_member, nil}, acc -> acc
              end)
              |> Enum.reverse()

            {:cont, {:ok, next_candidates}}

          failure ->
            {:halt, ReadResult.command_error(failure)}
        end
    end)
  end

  defp get_members_list(key, store) do
    prefix = CompoundKey.set_prefix(key)

    case Ops.compound_scan(store, key, prefix) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      pairs when is_list(pairs) ->
        {:ok, Enum.map(pairs, fn {member, _} -> member end)}
    end
  end
end
