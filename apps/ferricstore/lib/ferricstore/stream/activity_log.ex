defmodule Ferricstore.Stream.ActivityLog do
  @moduledoc """
  Metadata-only ring buffer for recent Stream mutations.

  Stream entries can contain application payloads. This log intentionally keeps
  only command shape, stream key, entry ID/counts, and safe option metadata.
  """

  use GenServer

  @table :ferricstore_stream_activity_log
  @counter_key :ferricstore_stream_activity_log_counter
  @max_len_key :ferricstore_stream_activity_log_max_len
  @default_max_len 512
  @default_read_count 128
  @max_read_count 500
  @max_metadata_bytes 256

  @type entry :: %{
          id: non_neg_integer(),
          timestamp_us: integer(),
          command: binary(),
          role: :producer | :consumer | :maintenance,
          key: binary(),
          result: binary(),
          entry_id: binary() | nil,
          count: non_neg_integer() | nil,
          field_pairs: non_neg_integer() | nil,
          trim: binary() | nil,
          group: binary() | nil,
          consumer: binary() | nil,
          nomkstream: boolean() | nil
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec record_xadd(binary(), binary(), non_neg_integer(), term(), boolean()) :: :ok
  def record_xadd(key, entry_id, field_pair_count, trim_opts, nomkstream) do
    record(%{
      command: "XADD",
      role: :producer,
      key: normalize_binary(key),
      result: "ok",
      entry_id: normalize_binary(entry_id),
      count: nil,
      field_pairs: max(field_pair_count, 0),
      trim: trim_label(trim_opts),
      group: nil,
      consumer: nil,
      nomkstream: nomkstream
    })
  end

  @spec record_xread([{binary(), binary()}], term()) :: :ok
  def record_xread(stream_ids, result) when is_list(stream_ids) do
    Enum.each(stream_read_counts(result), fn {key, count, last_id} ->
      record(%{
        command: "XREAD",
        role: :consumer,
        key: normalize_binary(key),
        result: "ok",
        entry_id: normalize_optional_binary(last_id),
        count: count,
        field_pairs: nil,
        trim: nil,
        group: nil,
        consumer: nil,
        nomkstream: nil
      })
    end)

    :ok
  end

  @spec record_xreadgroup(binary(), binary(), [{binary(), binary()}], term()) :: :ok
  def record_xreadgroup(group, consumer, stream_ids, result) when is_list(stream_ids) do
    Enum.each(stream_read_counts(result), fn {key, count, last_id} ->
      record(%{
        command: "XREADGROUP",
        role: :consumer,
        key: normalize_binary(key),
        result: "ok",
        entry_id: normalize_optional_binary(last_id),
        count: count,
        field_pairs: nil,
        trim: nil,
        group: normalize_binary(group),
        consumer: normalize_binary(consumer),
        nomkstream: nil
      })
    end)

    :ok
  end

  @spec record_xack(binary(), binary(), non_neg_integer()) :: :ok
  def record_xack(key, group, acked_count) when is_integer(acked_count) do
    record(%{
      command: "XACK",
      role: :consumer,
      key: normalize_binary(key),
      result: "ok",
      entry_id: nil,
      count: max(acked_count, 0),
      field_pairs: nil,
      trim: nil,
      group: normalize_binary(group),
      consumer: nil,
      nomkstream: nil
    })
  end

  @spec record_xtrim(binary(), non_neg_integer(), term()) :: :ok
  def record_xtrim(key, deleted_count, trim_opts) when is_integer(deleted_count) do
    record(%{
      command: "XTRIM",
      role: :maintenance,
      key: normalize_binary(key),
      result: "ok",
      entry_id: nil,
      count: max(deleted_count, 0),
      field_pairs: nil,
      trim: trim_label(trim_opts),
      group: nil,
      consumer: nil,
      nomkstream: nil
    })
  end

  @spec record_xdel(binary(), non_neg_integer()) :: :ok
  def record_xdel(key, deleted_count) when is_integer(deleted_count) do
    record(%{
      command: "XDEL",
      role: :maintenance,
      key: normalize_binary(key),
      result: "ok",
      entry_id: nil,
      count: max(deleted_count, 0),
      field_pairs: nil,
      trim: nil,
      group: nil,
      consumer: nil,
      nomkstream: nil
    })
  end

  @spec get(non_neg_integer() | nil) :: [entry()]
  def get(count \\ @default_read_count) do
    count = bounded_count(count)

    if table_ready?() do
      newest_entries(:ets.last(@table), count, [])
    else
      []
    end
  rescue
    _ -> []
  catch
    :exit, _ -> []
  end

  @spec len() :: non_neg_integer()
  def len do
    case :ets.info(@table, :size) do
      n when is_integer(n) -> n
      _ -> 0
    end
  rescue
    ArgumentError -> 0
  end

  @spec reset() :: :ok
  def reset do
    if table_ready?() do
      :ets.delete_all_objects(@table)
    end

    reset_counter()
    :ok
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  @impl true
  def init(_opts) do
    table =
      :ets.new(@table, [
        :ordered_set,
        :public,
        :named_table,
        {:read_concurrency, true},
        {:write_concurrency, true}
      ])

    :persistent_term.put(@counter_key, :atomics.new(1, signed: false))

    :persistent_term.put(
      @max_len_key,
      Application.get_env(:ferricstore, :stream_activity_log_max_len, @default_max_len)
    )

    {:ok, %{table: table}}
  end

  defp record(entry) do
    max_len = max_len()

    if max_len > 0 and table_ready?() do
      id = next_id()

      :ets.insert(
        @table,
        {id, Map.put(entry, :timestamp_us, System.os_time(:microsecond))}
      )

      maybe_evict_overflow(id, max_len)
    end

    :ok
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  defp max_len do
    case :persistent_term.get(@max_len_key, @default_max_len) do
      n when is_integer(n) and n >= 0 -> n
      _ -> @default_max_len
    end
  end

  defp next_id do
    case :persistent_term.get(@counter_key, nil) do
      counter when is_reference(counter) -> :atomics.add_get(counter, 1, 1) - 1
      _ -> System.unique_integer([:positive, :monotonic])
    end
  end

  defp reset_counter do
    case :persistent_term.get(@counter_key, nil) do
      counter when is_reference(counter) -> :atomics.put(counter, 1, 0)
      _ -> :ok
    end
  end

  defp maybe_evict_overflow(id, max_len) do
    interval = min(max(max_len, 1), 16)

    if rem(id + 1, interval) == 0 do
      evict_overflow(max_len)
    end
  end

  defp evict_overflow(max_len) do
    case :ets.info(@table, :size) do
      size when is_integer(size) and size > max_len -> delete_oldest(size - max_len)
      _ -> :ok
    end
  end

  defp delete_oldest(remaining) when remaining <= 0, do: :ok

  defp delete_oldest(remaining) do
    case :ets.first(@table) do
      :"$end_of_table" ->
        :ok

      id ->
        :ets.delete(@table, id)
        delete_oldest(remaining - 1)
    end
  end

  defp newest_entries(:"$end_of_table", _remaining, acc), do: Enum.reverse(acc)
  defp newest_entries(_id, 0, acc), do: Enum.reverse(acc)

  defp newest_entries(id, remaining, acc) do
    previous_id = :ets.prev(@table, id)

    case :ets.lookup(@table, id) do
      [{^id, entry}] ->
        newest_entries(previous_id, remaining - 1, [Map.put(entry, :id, id) | acc])

      [] ->
        newest_entries(previous_id, remaining, acc)
    end
  end

  defp bounded_count(nil), do: @default_read_count
  defp bounded_count(count) when is_integer(count), do: count |> max(0) |> min(@max_read_count)
  defp bounded_count(_count), do: @default_read_count

  defp table_ready?, do: :ets.whereis(@table) != :undefined

  defp normalize_binary(value) when not is_binary(value),
    do: value |> to_string() |> normalize_binary()

  defp normalize_binary(value) when byte_size(value) <= @max_metadata_bytes do
    :binary.copy(value)
  end

  defp normalize_binary(value) do
    omitted = byte_size(value) - @max_metadata_bytes
    prefix = value |> binary_part(0, @max_metadata_bytes) |> :binary.copy()
    prefix <> "...[#{omitted} more bytes]"
  end

  defp normalize_optional_binary(nil), do: nil
  defp normalize_optional_binary(value), do: normalize_binary(value)

  defp stream_read_counts(result) when is_list(result) do
    Enum.flat_map(result, fn
      [key, entries] when is_binary(key) and is_list(entries) ->
        count = length(entries)

        if count > 0 do
          [{key, count, last_entry_id(entries)}]
        else
          []
        end

      _other ->
        []
    end)
  end

  defp stream_read_counts(_result), do: []

  defp last_entry_id(entries) do
    entries
    |> List.last()
    |> case do
      [entry_id | _fields] when is_binary(entry_id) -> entry_id
      _other -> nil
    end
  end

  defp trim_label(nil), do: nil
  defp trim_label({:maxlen, approx, max_len}), do: "MAXLEN #{exact_label(approx)} #{max_len}"
  defp trim_label({:minid, approx, min_id}), do: "MINID #{exact_label(approx)} #{min_id}"
  defp trim_label(other), do: inspect(other, limit: 5, printable_limit: 80)

  defp exact_label(true), do: "~"
  defp exact_label(_), do: "="
end
