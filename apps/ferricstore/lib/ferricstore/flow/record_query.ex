defmodule Ferricstore.Flow.RecordQuery do
  @moduledoc false

  def fetch_count(count, nil, nil, _scan_count_fun), do: count

  def fetch_count(count, _from_ms, _to_ms, scan_count_fun) when is_function(scan_count_fun, 1),
    do: scan_count_fun.(count)

  def filter_by_ms(records, from_ms, to_ms) do
    Enum.filter(records, fn record ->
      updated_at_ms = Map.get(record, :updated_at_ms, 0)
      ms_after?(updated_at_ms, from_ms) and ms_before?(updated_at_ms, to_ms)
    end)
  end

  def sort_by_update(records) do
    Enum.sort_by(records, fn record ->
      {Map.get(record, :updated_at_ms, 0), Map.get(record, :id, "")}
    end)
  end

  def maybe_reverse(records, true), do: Enum.reverse(records)
  def maybe_reverse(records, false), do: records

  def prepend_chunk(chunk, chunks), do: [chunk | chunks]
  def flatten_chunks(chunks), do: Enum.flat_map(chunks, & &1)

  defp ms_after?(_event_ms, nil), do: true
  defp ms_after?(event_ms, from_ms), do: event_ms >= from_ms

  defp ms_before?(_event_ms, nil), do: true
  defp ms_before?(event_ms, to_ms), do: event_ms <= to_ms
end
