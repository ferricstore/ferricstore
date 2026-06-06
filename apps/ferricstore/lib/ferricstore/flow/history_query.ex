defmodule Ferricstore.Flow.HistoryQuery do
  @moduledoc false

  @filter_keys [
    :from_event,
    :to_event,
    :from_ms,
    :to_ms,
    :from_version,
    :to_version,
    :event,
    :worker
  ]

  def fetch_count(%{count: count} = query, scan_count_fun)
      when is_function(scan_count_fun, 2) do
    if filtering?(query) do
      scan_count_fun.(count, Map.get(query, :rev?, false))
    else
      count
    end
  end

  def filtering?(query) when is_map(query) do
    Enum.any?(@filter_keys, &(Map.get(query, &1) != nil))
  end

  def apply(events, query, event_ms_fun \\ &event_ms/1)
      when is_function(event_ms_fun, 1) do
    filtered = Enum.filter(events, &event_matches?(&1, query, event_ms_fun))

    cond do
      query.rev? ->
        filtered
        |> Enum.reverse()
        |> Enum.take(query.count)

      filtering?(query) ->
        Enum.take(filtered, -query.count)

      true ->
        Enum.take(filtered, query.count)
    end
  end

  def validate_ms_range(nil, _to_ms), do: :ok
  def validate_ms_range(_from_ms, nil), do: :ok
  def validate_ms_range(from_ms, to_ms) when from_ms <= to_ms, do: :ok
  def validate_ms_range(_from_ms, _to_ms), do: {:error, "ERR flow from_ms must be <= to_ms"}

  def validate_version_range(nil, _to_version), do: :ok
  def validate_version_range(_from_version, nil), do: :ok
  def validate_version_range(from_version, to_version) when from_version <= to_version, do: :ok

  def validate_version_range(_from_version, _to_version),
    do: {:error, "ERR flow from_version must be <= to_version"}

  def validate_event_range(from_event, to_event, event_ms_fun \\ &event_ms/1)

  def validate_event_range(nil, _to_event, _event_ms_fun), do: :ok
  def validate_event_range(_from_event, nil, _event_ms_fun), do: :ok

  def validate_event_range(from_event, to_event, event_ms_fun)
      when is_function(event_ms_fun, 1) do
    if event_key(from_event, event_ms_fun) <= event_key(to_event, event_ms_fun) do
      :ok
    else
      {:error, "ERR flow from_event must be <= to_event"}
    end
  end

  def event_ms(event_id) when is_integer(event_id), do: event_id

  def event_ms(event_id) when is_binary(event_id) do
    event_id
    |> :binary.split("-", [])
    |> List.first()
    |> parse_event_ms()
  end

  def event_ms(_event_id), do: 0

  defp event_matches?({event_id, fields}, query, event_ms_fun) do
    event_ms = event_ms_fun.(event_id)
    event_key = {event_ms, event_id}
    version = field_int(fields, "version")

    event_after?(event_key, query.from_event, event_ms_fun) and
      event_before?(event_key, query.to_event, event_ms_fun) and
      ms_after?(event_ms, query.from_ms) and
      ms_before?(event_ms, query.to_ms) and
      version_after?(version, query.from_version) and
      version_before?(version, query.to_version) and
      field_matches?(fields, "event", query.event) and
      field_matches?(fields, "lease_owner", query.worker)
  end

  defp event_after?(_event_key, nil, _event_ms_fun), do: true

  defp event_after?(event_key, from_event, event_ms_fun),
    do: event_key >= event_key(from_event, event_ms_fun)

  defp event_before?(_event_key, nil, _event_ms_fun), do: true

  defp event_before?(event_key, to_event, event_ms_fun),
    do: event_key <= event_key(to_event, event_ms_fun)

  defp event_key(event_id, event_ms_fun), do: {event_ms_fun.(event_id), event_id}

  defp ms_after?(_event_ms, nil), do: true
  defp ms_after?(event_ms, from_ms), do: event_ms >= from_ms

  defp ms_before?(_event_ms, nil), do: true
  defp ms_before?(event_ms, to_ms), do: event_ms <= to_ms

  defp version_after?(_version, nil), do: true
  defp version_after?(version, from_version), do: version >= from_version

  defp version_before?(_version, nil), do: true
  defp version_before?(version, to_version), do: version <= to_version

  defp field_int(fields, key) do
    case Map.get(fields, key) do
      value when is_integer(value) ->
        value

      value when is_binary(value) ->
        case Integer.parse(value) do
          {int, ""} -> int
          _ -> 0
        end

      _ ->
        0
    end
  end

  defp field_matches?(_fields, _key, nil), do: true
  defp field_matches?(fields, key, value), do: Map.get(fields, key) == value

  defp parse_event_ms(nil), do: 0

  defp parse_event_ms(value) do
    case Integer.parse(value) do
      {ms, ""} -> ms
      _ -> 0
    end
  end
end
