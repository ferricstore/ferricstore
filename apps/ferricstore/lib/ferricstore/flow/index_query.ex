defmodule Ferricstore.Flow.IndexQuery do
  @moduledoc false

  def fetch_count(%{count: count} = query, scan_count_fun)
      when is_function(scan_count_fun, 1) do
    if filtering?(query), do: scan_count_fun.(count), else: count
  end

  def filtering?(%{
        from_ms: nil,
        to_ms: nil,
        rev?: false,
        state: nil,
        terminal_only?: false
      }),
      do: false

  def filtering?(_query), do: true

  def filter_records(records, field, value, query, terminal_states) do
    Enum.filter(records, fn record ->
      Map.get(record, field) == value and record_matches?(record, query, terminal_states)
    end)
  end

  def record_matches?(record, query, terminal_states) do
    updated_at_ms = Map.get(record, :updated_at_ms, 0)
    state = Map.get(record, :state)

    ms_after?(updated_at_ms, query.from_ms) and
      ms_before?(updated_at_ms, query.to_ms) and
      state_matches?(state, query.state) and
      terminal_matches?(state, query.terminal_only?, terminal_states)
  end

  defp ms_after?(_event_ms, nil), do: true
  defp ms_after?(event_ms, from_ms), do: event_ms >= from_ms

  defp ms_before?(_event_ms, nil), do: true
  defp ms_before?(event_ms, to_ms), do: event_ms <= to_ms

  defp state_matches?(_state, nil), do: true
  defp state_matches?(state, expected), do: state == expected

  defp terminal_matches?(_state, false, _terminal_states), do: true
  defp terminal_matches?(state, true, terminal_states), do: state in terminal_states
end
