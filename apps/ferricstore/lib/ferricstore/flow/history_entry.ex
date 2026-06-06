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
    |> Map.new(fn [key, value] -> {key, value} end)
  end
end
