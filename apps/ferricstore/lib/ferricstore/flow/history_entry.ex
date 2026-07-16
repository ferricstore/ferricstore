defmodule Ferricstore.Flow.HistoryEntry do
  @moduledoc false

  def to_tuple({event_id, fields}) when is_list(fields) do
    {event_id, fields_to_map(fields)}
  end

  def to_tuple([event_id | fields]) when is_list(fields) do
    {event_id, fields_to_map(fields)}
  end

  def fields_to_map(fields) when is_list(fields) do
    fields
    |> Enum.chunk_every(2)
    |> Enum.reduce(%{}, fn
      [key, value], acc when is_binary(key) -> Map.put(acc, key, value)
      _invalid, acc -> acc
    end)
  end
end
