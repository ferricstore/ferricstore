defmodule Ferricstore.Flow.IndexQuery do
  @moduledoc false

  def fetch_count(%{count: count} = query, scan_count_fun)
      when is_function(scan_count_fun, 1) do
    if filtering?(query), do: scan_count_fun.(count), else: count
  end

  def filtering?(query) when is_map(query) do
    Map.get(query, :from_ms) != nil or Map.get(query, :to_ms) != nil or
      Map.get(query, :rev?, false) or Map.get(query, :after_id) != nil or
      Map.get(query, :before_id) != nil or Map.get(query, :state) != nil or
      Map.get(query, :terminal_only?, false)
  end

  def filter_records(records, field, value, query, terminal_states) do
    Enum.filter(records, fn record ->
      Map.get(record, field) == value and record_matches?(record, query, terminal_states)
    end)
  end

  def record_matches?(record, query, terminal_states) do
    updated_at_ms = Map.get(record, :updated_at_ms, 0)
    id = Map.get(record, :id)
    state = Map.get(record, :state)

    ms_after?(updated_at_ms, query.from_ms) and
      ms_before?(updated_at_ms, query.to_ms) and
      after_cursor?(updated_at_ms, id, query) and
      before_cursor?(updated_at_ms, id, query) and
      state_matches?(state, query.state) and
      terminal_matches?(state, query.terminal_only?, terminal_states)
  end

  def entry_before_cursor?({id, updated_at_ms}, query),
    do: before_cursor?(updated_at_ms, id, query)

  def entry_after_cursor?({id, updated_at_ms}, query),
    do: after_cursor?(updated_at_ms, id, query)

  defp ms_after?(_event_ms, nil), do: true
  defp ms_after?(event_ms, from_ms), do: event_ms >= from_ms

  defp ms_before?(_event_ms, nil), do: true
  defp ms_before?(event_ms, to_ms), do: event_ms <= to_ms

  defp after_cursor?(updated_at_ms, id, %{rev?: false, from_ms: from_ms, after_id: after_id})
       when is_integer(from_ms) and is_binary(after_id) and after_id != "" do
    updated_at_ms > from_ms or (updated_at_ms == from_ms and is_binary(id) and id > after_id)
  end

  defp after_cursor?(_updated_at_ms, _id, _query), do: true

  defp before_cursor?(updated_at_ms, id, %{rev?: true, to_ms: to_ms, before_id: before_id})
       when is_integer(to_ms) and is_binary(before_id) and before_id != "" do
    updated_at_ms < to_ms or (updated_at_ms == to_ms and is_binary(id) and id < before_id)
  end

  defp before_cursor?(_updated_at_ms, _id, _query), do: true

  defp state_matches?(_state, nil), do: true
  defp state_matches?(state, expected), do: state == expected

  defp terminal_matches?(_state, false, _terminal_states), do: true
  defp terminal_matches?(state, true, terminal_states), do: state in terminal_states
end
