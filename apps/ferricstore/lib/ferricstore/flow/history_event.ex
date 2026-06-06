defmodule Ferricstore.Flow.HistoryEvent do
  @moduledoc false

  def ms(event_id) when is_binary(event_id) do
    case Integer.parse(event_id) do
      {ms, "-" <> _rest} -> ms
      {ms, ""} -> ms
      _ -> 0
    end
  end

  def ms(_event_id), do: 0
end
