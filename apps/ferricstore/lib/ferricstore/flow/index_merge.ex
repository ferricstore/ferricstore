defmodule Ferricstore.Flow.IndexMerge do
  @moduledoc false

  def ids_from_scored_entries(ram_entries, lmdb_entries, count, reverse?) do
    ram_ids = MapSet.new(ram_entries, fn {id, _score} -> id end)
    lmdb_entries = Enum.reject(lmdb_entries, fn {id, _score} -> MapSet.member?(ram_ids, id) end)

    (ram_entries ++ lmdb_entries)
    |> Enum.sort_by(fn {id, score} -> {score, id} end)
    |> maybe_reverse(reverse?)
    |> Enum.uniq_by(fn {id, _score} -> id end)
    |> Enum.map(fn {id, _score} -> id end)
    |> Enum.take(count)
  end

  def ids_from_query_entries(ram_entries, lmdb_entries, count, reverse?) do
    lmdb_scored =
      Enum.map(lmdb_entries, fn {id, updated_at_ms, _state_key} ->
        {id, updated_at_ms}
      end)

    ids_from_scored_entries(ram_entries, lmdb_scored, count, reverse?)
  end

  def terminal_entries_from_chunks(chunks, count, reverse?) do
    chunks
    |> flatten_chunks()
    |> Enum.sort_by(fn {id, updated_at_ms} -> {updated_at_ms, id} end)
    |> maybe_reverse(reverse?)
    |> Enum.take(count)
  end

  def query_entries_from_chunks(chunks) do
    chunks
    |> flatten_chunks()
    |> Enum.sort_by(fn {id, updated_at_ms, _state_key} -> {updated_at_ms, id} end)
  end

  defp maybe_reverse(entries, true), do: Enum.reverse(entries)
  defp maybe_reverse(entries, false), do: entries

  defp flatten_chunks(chunks), do: Enum.flat_map(chunks, & &1)
end
