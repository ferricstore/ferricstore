defmodule Ferricstore.Flow.Query.CompositeRangeReader do
  @moduledoc false

  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.Query.{CompositeIndex, CompositeRange}

  @type page :: %{
          entries: [map()],
          cursor: binary() | nil,
          exhausted: boolean(),
          scanned_entries: non_neg_integer(),
          scanned_bytes: non_neg_integer()
        }

  @type query_row_page :: %{
          entries: [map()],
          query_row_values: [binary() | nil],
          query_row_bytes: non_neg_integer(),
          cursor: binary() | nil,
          exhausted: boolean(),
          scanned_entries: non_neg_integer(),
          scanned_bytes: non_neg_integer()
        }

  @spec read(binary(), CompositeRange.t(), binary() | nil, pos_integer(), pos_integer()) ::
          {:ok, page()} | {:error, atom() | term()}
  def read(path, %CompositeRange{} = range, cursor, max_entries, max_bytes)
      when is_binary(path) and is_integer(max_entries) and max_entries > 0 and
             is_integer(max_bytes) and max_bytes > 0 do
    with :ok <- CompositeRange.validate(range),
         {:ok, after_key} <- effective_after_key(range, cursor),
         {:ok, rows, exhausted, scanned_bytes} <-
           LMDB.composite_range_entries_bounded(
             path,
             range.prefix,
             after_key,
             range.before_key,
             max_entries,
             max_bytes
           ),
         {:ok, entries} <- materialize_rows(rows) do
      {:ok,
       %{
         entries: entries,
         cursor: next_cursor(rows, exhausted),
         exhausted: exhausted,
         scanned_entries: length(rows),
         scanned_bytes: scanned_bytes
       }}
    end
  end

  def read(_path, %CompositeRange{}, _cursor, _max_entries, _max_bytes),
    do: {:error, :invalid_composite_range_budget}

  @spec read_query_rows(
          binary(),
          CompositeRange.t(),
          binary(),
          binary() | nil,
          pos_integer(),
          pos_integer(),
          pos_integer()
        ) :: {:ok, query_row_page()} | {:error, atom() | term()}
  def read_query_rows(
        path,
        %CompositeRange{} = range,
        state_key_prefix,
        cursor,
        max_entries,
        max_scan_bytes,
        max_query_row_bytes
      )
      when is_binary(path) and is_binary(state_key_prefix) and state_key_prefix != "" and
             byte_size(state_key_prefix) <= 511 and is_integer(max_entries) and max_entries > 0 and
             is_integer(max_scan_bytes) and max_scan_bytes > 0 and
             is_integer(max_query_row_bytes) and max_query_row_bytes > 0 do
    with :ok <- CompositeRange.validate(range),
         {:ok, after_key} <- effective_after_key(range, cursor),
         {:ok, rows, exhausted, scanned_bytes, query_row_bytes} <-
           LMDB.composite_range_query_rows_bounded(
             path,
             range.prefix,
             state_key_prefix,
             after_key,
             range.before_key,
             max_entries,
             max_scan_bytes,
             max_query_row_bytes
           ),
         {:ok, entries, query_row_values, ^query_row_bytes} <- materialize_query_rows(rows) do
      {:ok,
       %{
         entries: entries,
         query_row_values: query_row_values,
         query_row_bytes: query_row_bytes,
         cursor: next_entry_cursor(entries, exhausted),
         exhausted: exhausted,
         scanned_entries: length(entries),
         scanned_bytes: scanned_bytes
       }}
    end
  end

  def read_query_rows(
        _path,
        %CompositeRange{},
        _state_key_prefix,
        _cursor,
        _max_entries,
        _max_scan_bytes,
        _max_query_row_bytes
      ),
      do: {:error, :invalid_composite_range_budget}

  defp effective_after_key(%CompositeRange{after_key: after_key}, nil), do: {:ok, after_key}

  defp effective_after_key(%CompositeRange{} = range, cursor) when is_binary(cursor) do
    if byte_size(cursor) <= 511 and String.starts_with?(cursor, range.prefix) and
         cursor > range.after_key and
         (range.before_key == "" or cursor < range.before_key),
       do: {:ok, cursor},
       else: {:error, :invalid_composite_cursor}
  end

  defp effective_after_key(%CompositeRange{}, _cursor),
    do: {:error, :invalid_composite_cursor}

  defp materialize_rows(rows) when is_list(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn row, {:ok, acc} ->
      case materialize_row(row) do
        {:ok, entry} -> {:cont, {:ok, [entry | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp materialize_row(
         {key, id, state_key, record_version, expire_at_ms, storage_bytes, encoded_covering}
       )
       when is_binary(key) and is_binary(id) and is_binary(state_key) and
              is_integer(record_version) and record_version >= 0 and
              is_integer(expire_at_ms) and expire_at_ms >= 0 and is_integer(storage_bytes) and
              storage_bytes > 0 do
    case decode_covering(encoded_covering, id, record_version) do
      {:ok, covering_record} ->
        {:ok,
         %{
           id: id,
           state_key: state_key,
           record_version: record_version,
           expire_at_ms: expire_at_ms,
           storage_key: key,
           storage_bytes: storage_bytes,
           covering_record: covering_record
         }}

      :error ->
        {:error, :invalid_composite_entry}
    end
  end

  defp materialize_row(_invalid), do: {:error, :invalid_composite_entry}

  defp decode_covering(nil, _id, _record_version), do: {:ok, nil}

  defp decode_covering(encoded, id, record_version) when is_binary(encoded),
    do: CompositeIndex.decode_covering_record(encoded, id, record_version)

  defp decode_covering(_encoded, _id, _record_version), do: :error

  defp materialize_query_rows(rows) when is_list(rows) do
    rows
    |> Enum.reduce_while({:ok, [], [], 0}, fn
      {composite, value}, {:ok, entries, values, bytes} ->
        with true <- is_nil(value) or is_binary(value),
             {:ok, entry} <- materialize_row(composite) do
          value_bytes = if is_binary(value), do: byte_size(value), else: 0
          {:cont, {:ok, [entry | entries], [value | values], bytes + value_bytes}}
        else
          false -> {:halt, {:error, :invalid_query_row_read}}
          {:error, _reason} = error -> {:halt, error}
        end

      _invalid, _acc ->
        {:halt, {:error, :invalid_query_row_read}}
    end)
    |> case do
      {:ok, entries, values, bytes} ->
        {:ok, Enum.reverse(entries), Enum.reverse(values), bytes}

      {:error, _reason} = error ->
        error
    end
  end

  defp materialize_query_rows(_rows), do: {:error, :invalid_query_row_read}

  defp next_entry_cursor(_entries, true), do: nil
  defp next_entry_cursor([], false), do: nil
  defp next_entry_cursor(entries, false), do: entries |> List.last() |> Map.fetch!(:storage_key)

  defp next_cursor(_rows, true), do: nil
  defp next_cursor([], false), do: nil
  defp next_cursor(rows, false), do: rows |> List.last() |> elem(0)
end
