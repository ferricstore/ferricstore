defmodule Ferricstore.Flow.LMDB.HistoryScan do
  @moduledoc false

  # A Flow's history index keys sort by millisecond, then by event id bytes,
  # which puts one millisecond's "1000-10" before "1000-9". Each scan reads on
  # until the last millisecond it reached is complete, then returns `limit`
  # entries in history order (`Ferricstore.Flow.HistoryEvent.sort_key/1`):
  # oldest first for first/from_ms/after_event, newest first for the others.

  alias Ferricstore.Flow.{HistoryEvent, LMDB, LMDBQueryWindow}

  @ms_page 256
  @ms_page_bytes 1_048_576

  def first(path, history_key, limit),
    do: scan(path, LMDB.history_index_prefix(history_key), nil, limit, :asc)

  def last(path, history_key, limit),
    do: scan(path, LMDB.history_index_prefix(history_key), nil, limit, :desc)

  def from_ms(path, history_key, ms, limit) do
    prefix = LMDB.history_index_prefix(history_key)
    scan(path, prefix, LMDBQueryWindow.time_seek_key(prefix, ms), limit, :asc)
  end

  def to_ms(path, history_key, ms, limit) do
    prefix = LMDB.history_index_prefix(history_key)
    scan(path, prefix, LMDBQueryWindow.time_upper_seek_key(prefix, ms), limit, :desc)
  end

  def after_event(path, history_key, event_id, limit),
    do: resume(path, history_key, event_id, limit, :asc)

  def before_event(path, history_key, event_id, limit),
    do: resume(path, history_key, event_id, limit, :desc)

  # The cursor's millisecond, read whole, holds the events on both sides of
  # it; the scan continues past that millisecond.
  defp resume(_path, _history_key, _event_id, 0, _direction), do: {:ok, []}

  defp resume(path, history_key, event_id, limit, direction) do
    prefix = LMDB.history_index_prefix(history_key)
    ms = HistoryEvent.ms(event_id)
    cursor = sort_key(prefix, LMDB.history_index_key(history_key, event_id, ms))
    seek = LMDBQueryWindow.time_seek_key(prefix, ms)

    {beyond_seek, beyond?} =
      case direction do
        :asc -> {LMDBQueryWindow.time_upper_seek_key(prefix, ms), &(&1 > cursor)}
        :desc -> {seek, &(&1 < cursor)}
      end

    with {:ok, same_ms} <- rest_of_ms(path, prefix, seek, :asc),
         {:ok, beyond} <- scan(path, prefix, beyond_seek, limit, direction) do
      same_ms
      |> Enum.filter(&beyond?.(sort_key(prefix, &1)))
      |> sort(prefix, direction)
      |> Kernel.++(beyond)
      |> Enum.take(limit)
      |> then(&{:ok, &1})
    end
  end

  defp scan(_path, _prefix, _seek, 0, _direction), do: {:ok, []}

  defp scan(path, prefix, seek, limit, direction) do
    with {:ok, entries} <- read(path, prefix, seek, limit, direction) do
      if length(entries) < limit do
        {:ok, sort(entries, prefix, direction)}
      else
        {last_key, _value} = List.last(entries)

        with {:ok, rest} <- rest_of_ms(path, prefix, last_key, direction) do
          {:ok, (entries ++ rest) |> sort(prefix, direction) |> Enum.take(limit)}
        end
      end
    end
  end

  defp read(path, prefix, nil, limit, :asc), do: LMDB.prefix_entries(path, prefix, limit)
  defp read(path, prefix, nil, limit, :desc), do: LMDB.prefix_entries(path, prefix, limit, true)

  defp read(path, prefix, seek, limit, :asc),
    do: LMDB.prefix_entries_after(path, prefix, seek, limit)

  defp read(path, prefix, seek, limit, :desc),
    do: LMDB.prefix_entries_reverse_before(path, prefix, seek, limit)

  # The entries past `key`, in `direction`, that share its millisecond: a read
  # bounded to that millisecond, so it is empty unless events share it.
  defp rest_of_ms(path, prefix, key, direction) do
    case key_ms(prefix, key) do
      nil ->
        {:ok, []}

      ms ->
        ms_start = prefix <> ms

        case direction do
          :asc -> ms_range(path, prefix, key, ms_start <> <<255>>, [])
          :desc -> ms_range(path, prefix, ms_start, key, [])
        end
    end
  end

  defp ms_range(path, prefix, after_key, before_key, acc) do
    case LMDB.range_entries_bounded(path, prefix, after_key, before_key, @ms_page, @ms_page_bytes) do
      {:ok, entries, false, _bytes} when entries != [] ->
        {next_key, _value} = List.last(entries)
        ms_range(path, prefix, next_key, before_key, [entries | acc])

      {:ok, entries, _complete?, _bytes} ->
        {:ok, [entries | acc] |> Enum.reverse() |> Enum.concat()}

      {:error, _reason} = error ->
        error
    end
  end

  # Byte order is history order unless two neighbours share a millisecond.
  defp sort(entries, prefix, direction) do
    if shared_ms?(entries, prefix),
      do: Enum.sort_by(entries, &sort_key(prefix, &1), direction),
      else: entries
  end

  defp shared_ms?([{key, _value} | [{next_key, _next_value} | _] = rest], prefix),
    do: key_ms(prefix, key) == key_ms(prefix, next_key) or shared_ms?(rest, prefix)

  defp shared_ms?(_entries, _prefix), do: false

  defp sort_key(prefix, {key, _value}), do: sort_key(prefix, key)

  # The padded millisecond sorts as its number does.
  defp sort_key(prefix, key) do
    size = byte_size(prefix)

    case key do
      <<^prefix::binary-size(^size), ms::binary-size(20), 0, event_id::binary>> ->
        {ms, HistoryEvent.version(event_id), event_id}

      _other ->
        {"", 0, key}
    end
  end

  defp key_ms(prefix, key) do
    size = byte_size(prefix)

    case key do
      <<^prefix::binary-size(^size), ms::binary-size(20), _rest::binary>> -> ms
      _other -> nil
    end
  end
end
