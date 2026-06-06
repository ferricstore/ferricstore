defmodule Ferricstore.Flow.HistoryProjector.KeyCodec do
  @moduledoc false

  @prefix "X:"
  @separator <<0>>

  def parse_history_entry_key(@prefix <> rest) do
    case :binary.split(rest, @separator) do
      [history_key, event_id] ->
        case parse_event_ms(event_id) do
          {:ok, event_ms} -> {:ok, history_key, event_id, event_ms}
          :error -> :error
        end

      _ ->
        :error
    end
  end

  def parse_history_entry_key(_key), do: :error

  def history_entry_key(history_key, event_id), do: @prefix <> history_key <> @separator <> event_id

  def parse_event_ms(event_id) do
    case :binary.split(event_id, "-") do
      [ms, _version] ->
        case Integer.parse(ms) do
          {event_ms, ""} -> {:ok, event_ms}
          _ -> :error
        end

      _ ->
        :error
    end
  end

  def parse_event_version(event_id) do
    case :binary.split(event_id, "-") do
      [_ms, version] ->
        case Integer.parse(version) do
          {parsed, ""} -> {:ok, parsed}
          _ -> :error
        end

      _ ->
        :error
    end
  end
end
