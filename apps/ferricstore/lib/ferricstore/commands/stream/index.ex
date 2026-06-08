defmodule Ferricstore.Commands.Stream.Index do
  @moduledoc false

  alias Ferricstore.Commands.Stream.{Entries, ID}

  @index_table Ferricstore.Stream.Index
  @max_int64 9_223_372_036_854_775_807

  @spec ready?(binary()) :: boolean()
  def ready?(stream_key) do
    :ets.lookup(@index_table, {:ready, stream_key}) != []
  end

  @spec ensure(binary(), map()) :: :ok
  def ensure(stream_key, store) do
    unless ready?(stream_key) do
      rebuild(stream_key, store)
    end

    :ok
  end

  @spec mark_ready(binary()) :: true
  def mark_ready(stream_key) do
    :ets.insert(@index_table, {{:ready, stream_key}, true})
  end

  @spec clear(binary()) :: true
  def clear(stream_key) do
    :ets.select_delete(@index_table, [{{{stream_key, :_, :_}, :_, :_}, [], [true]}])
    :ets.delete(@index_table, {:ready, stream_key})
  end

  @spec insert_entry(binary(), binary(), binary()) :: true
  def insert_entry(stream_key, id_str, compound_key) do
    {ms, seq} = ID.parse_id!(id_str)
    :ets.insert(@index_table, {{stream_key, ms, seq}, id_str, compound_key})
  end

  @spec delete_ids(binary(), [binary()]) :: :ok
  def delete_ids(stream_key, ids) do
    Enum.each(ids, fn id_str ->
      {ms, seq} = ID.parse_id!(id_str)
      :ets.delete(@index_table, {stream_key, ms, seq})
    end)
  end

  @spec slice(
          binary(),
          :min | ID.stream_id(),
          :max | ID.stream_id(),
          non_neg_integer() | :infinity,
          boolean()
        ) ::
          [{binary(), binary()}]
  def slice(_stream_key, _range_start, _range_end, 0, _reverse?), do: []

  def slice(stream_key, range_start, range_end, count, false) do
    stream_key
    |> forward_first(range_start)
    |> collect(stream_key, range_start, range_end, count, &next_key/1, [])
  end

  def slice(stream_key, range_start, range_end, count, true) do
    stream_key
    |> reverse_first(range_end)
    |> collect(stream_key, range_start, range_end, count, &prev_key/1, [])
  end

  @spec ids(binary(), non_neg_integer() | :infinity) :: [binary()]
  def ids(stream_key, count) do
    stream_key
    |> slice(:min, :max, count, false)
    |> Enum.map(fn {id_str, _compound_key} -> id_str end)
  end

  @spec first_last(binary()) :: {binary(), binary()} | nil
  def first_last(stream_key) do
    first_key = forward_first(stream_key, :min)
    last_key = reverse_first(stream_key, :max)

    with {^stream_key, _first_ms, _first_seq} <- first_key,
         {^stream_key, _last_ms, _last_seq} <- last_key,
         [{^first_key, first_id, _first_compound_key}] <- :ets.lookup(@index_table, first_key),
         [{^last_key, last_id, _last_compound_key}] <- :ets.lookup(@index_table, last_key) do
      {first_id, last_id}
    else
      _ -> nil
    end
  end

  defp rebuild(stream_key, store) do
    clear(stream_key)

    store
    |> Entries.fields_for(stream_key)
    |> Enum.each(fn id_str ->
      insert_entry(stream_key, id_str, Entries.entry_key(stream_key, id_str))
    end)

    mark_ready(stream_key)
  end

  defp forward_first(stream_key, :min) do
    :ets.next(@index_table, {stream_key, -1, -1})
  end

  defp forward_first(stream_key, {ms, seq}) do
    :ets.next(@index_table, {stream_key, ms, seq - 1})
  end

  defp reverse_first(stream_key, :max) do
    :ets.prev(@index_table, {stream_key, @max_int64, @max_int64})
  end

  defp reverse_first(stream_key, {ms, seq}) do
    key = {stream_key, ms, seq}

    case :ets.lookup(@index_table, key) do
      [{^key, _id_str, _compound_key}] -> key
      [] -> :ets.prev(@index_table, key)
    end
  end

  defp next_key(:"$end_of_table"), do: :"$end_of_table"
  defp next_key(key), do: :ets.next(@index_table, key)

  defp prev_key(:"$end_of_table"), do: :"$end_of_table"
  defp prev_key(key), do: :ets.prev(@index_table, key)

  defp collect(:"$end_of_table", _stream_key, _start, _end, _count, _next, acc),
    do: Enum.reverse(acc)

  defp collect({stream_key, ms, seq} = key, stream_key, range_start, range_end, count, next, acc) do
    id = {ms, seq}

    cond do
      count == 0 ->
        Enum.reverse(acc)

      not ID.in_range?(id, range_start, range_end) ->
        Enum.reverse(acc)

      true ->
        case :ets.lookup(@index_table, key) do
          [{^key, id_str, compound_key}] ->
            collect(
              next.(key),
              stream_key,
              range_start,
              range_end,
              decrement_count(count),
              next,
              [{id_str, compound_key} | acc]
            )

          [] ->
            collect(next.(key), stream_key, range_start, range_end, count, next, acc)
        end
    end
  end

  defp collect(_other_key, _stream_key, _start, _end, _count, _next, acc),
    do: Enum.reverse(acc)

  defp decrement_count(:infinity), do: :infinity
  defp decrement_count(count), do: count - 1
end
