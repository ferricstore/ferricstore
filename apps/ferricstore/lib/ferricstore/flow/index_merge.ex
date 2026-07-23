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

  def ids_from_priority_lists(_ram_ids, _lmdb_ids, count) when count <= 0, do: []

  def ids_from_priority_lists(ram_ids, lmdb_ids, count) do
    merge_priority_ids(ram_ids, lmdb_ids, count, MapSet.new(), [])
  end

  def ids_from_ordered_scored_entries(_ram_entries, _lmdb_entries, count, _reverse?)
      when count <= 0,
      do: []

  def ids_from_ordered_scored_entries(ram_entries, lmdb_entries, count, reverse?) do
    ram_ids = MapSet.new(ram_entries, fn {id, _score} -> id end)

    merge_ordered_ids(
      ram_entries,
      lmdb_entries,
      count,
      reverse?,
      ram_ids,
      MapSet.new(),
      []
    )
  end

  def ids_from_query_entries(ram_entries, lmdb_entries, count, reverse?) do
    lmdb_scored =
      Enum.map(lmdb_entries, fn {id, updated_at_ms, _state_key} ->
        {id, updated_at_ms}
      end)

    lmdb_scored = maybe_reverse(lmdb_scored, reverse?)

    ids_from_ordered_scored_entries(ram_entries, lmdb_scored, count, reverse?)
  end

  def terminal_entries_from_chunks([entries], count, reverse?) do
    entries
    |> Enum.sort_by(fn {id, updated_at_ms} -> {updated_at_ms, id} end)
    |> maybe_reverse(reverse?)
    |> Enum.take(count)
  end

  def terminal_entries_from_chunks(chunks, count, reverse?) do
    chunks
    |> flatten_chunks()
    |> Enum.sort_by(fn {id, updated_at_ms} -> {updated_at_ms, id} end)
    |> maybe_reverse(reverse?)
    |> Enum.take(count)
  end

  def query_entries_from_chunks([entries]) do
    Enum.sort_by(entries, fn {id, updated_at_ms, _state_key} -> {updated_at_ms, id} end)
  end

  def query_entries_from_chunks(chunks) do
    chunks
    |> flatten_chunks()
    |> Enum.sort_by(fn {id, updated_at_ms, _state_key} -> {updated_at_ms, id} end)
  end

  defp maybe_reverse(entries, true), do: Enum.reverse(entries)
  defp maybe_reverse(entries, false), do: entries

  defp merge_priority_ids(_ram, _lmdb, 0, _seen, acc), do: Enum.reverse(acc)
  defp merge_priority_ids([], [], _count, _seen, acc), do: Enum.reverse(acc)

  defp merge_priority_ids([], [id | lmdb], count, seen, acc),
    do: emit_priority_id(id, [], lmdb, count, seen, acc)

  defp merge_priority_ids([id | ram], lmdb, count, seen, acc),
    do: emit_priority_id(id, ram, lmdb, count, seen, acc)

  defp emit_priority_id(id, ram, lmdb, count, seen, acc) do
    if MapSet.member?(seen, id) do
      merge_priority_ids(ram, lmdb, count, seen, acc)
    else
      merge_priority_ids(ram, lmdb, count - 1, MapSet.put(seen, id), [id | acc])
    end
  end

  defp merge_ordered_ids(_ram, _lmdb, 0, _reverse?, _ram_ids, _seen, acc),
    do: Enum.reverse(acc)

  defp merge_ordered_ids([], [], _count, _reverse?, _ram_ids, _seen, acc),
    do: Enum.reverse(acc)

  defp merge_ordered_ids([entry | ram], [], count, reverse?, ram_ids, seen, acc) do
    emit_ordered_id(entry, :ram, ram, [], count, reverse?, ram_ids, seen, acc)
  end

  defp merge_ordered_ids([], [entry | lmdb], count, reverse?, ram_ids, seen, acc) do
    emit_ordered_id(entry, :lmdb, [], lmdb, count, reverse?, ram_ids, seen, acc)
  end

  defp merge_ordered_ids(
         [ram_entry | ram] = ram_entries,
         [lmdb_entry | lmdb] = lmdb_entries,
         count,
         reverse?,
         ram_ids,
         seen,
         acc
       ) do
    if ordered_before?(ram_entry, lmdb_entry, reverse?) do
      emit_ordered_id(
        ram_entry,
        :ram,
        ram,
        lmdb_entries,
        count,
        reverse?,
        ram_ids,
        seen,
        acc
      )
    else
      emit_ordered_id(
        lmdb_entry,
        :lmdb,
        ram_entries,
        lmdb,
        count,
        reverse?,
        ram_ids,
        seen,
        acc
      )
    end
  end

  defp emit_ordered_id(
         {id, _score},
         source,
         ram,
         lmdb,
         count,
         reverse?,
         ram_ids,
         seen,
         acc
       ) do
    if MapSet.member?(seen, id) or (source == :lmdb and MapSet.member?(ram_ids, id)) do
      merge_ordered_ids(ram, lmdb, count, reverse?, ram_ids, seen, acc)
    else
      merge_ordered_ids(
        ram,
        lmdb,
        count - 1,
        reverse?,
        ram_ids,
        MapSet.put(seen, id),
        [id | acc]
      )
    end
  end

  defp ordered_before?({left_id, left_score}, {right_id, right_score}, true),
    do: {left_score, left_id} >= {right_score, right_id}

  defp ordered_before?({left_id, left_score}, {right_id, right_score}, false),
    do: {left_score, left_id} <= {right_score, right_id}

  defp flatten_chunks(chunks), do: Enum.flat_map(chunks, & &1)
end
