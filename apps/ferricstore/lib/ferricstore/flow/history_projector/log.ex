defmodule Ferricstore.Flow.HistoryProjector.Log do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.CommandTime
  alias Ferricstore.Flow.HistoryProjector.Files

  @scan_page_records 4_096

  @spec read_value(binary(), {:flow_history, non_neg_integer()}, non_neg_integer()) ::
          {:ok, binary()} | {:error, term()}
  def read_value(shard_data_path, {:flow_history, file_id}, offset) do
    shard_data_path
    |> history_file_path(file_id)
    |> NIF.v2_pread_at(offset)
  end

  def read_value(_shard_data_path, _file_id, _offset), do: {:error, :not_flow_history}

  @doc false
  # The values at `offsets` of one history log, in order, with the log opened
  # once; the whole read fails if any record does.
  @spec read_values(binary(), {:flow_history, non_neg_integer()}, [non_neg_integer()]) ::
          {:ok, [binary() | nil]} | {:error, term()}
  def read_values(shard_data_path, {:flow_history, file_id}, offsets) when is_list(offsets) do
    shard_data_path
    |> history_file_path(file_id)
    |> NIF.v2_pread_batch(offsets)
  end

  def read_values(_shard_data_path, _file_id, _offsets), do: {:error, :not_flow_history}

  # The latest record of `target_key` across the history files, read in replay
  # order.
  @spec scan_event_value(binary(), binary()) :: {:ok, binary()} | :miss | {:error, term()}
  def scan_event_value(shard_data_path, target_key) do
    now_ms = CommandTime.now_ms()

    with {:ok, file_ids} <- Files.list(shard_data_path),
         {:ok, location} <- scan_event_location(shard_data_path, file_ids, target_key, now_ms) do
      case location do
        {:live, file_path, offset} -> NIF.v2_pread_at(file_path, offset)
        :miss -> :miss
      end
    end
  end

  defp scan_event_location(shard_data_path, file_ids, target_key, now_ms) do
    Enum.reduce_while(file_ids, {:ok, :miss}, fn file_id, {:ok, location} ->
      file_path = history_file_path(shard_data_path, file_id)

      case reduce_metadata_pages(file_path, location, fn record, location ->
             latest_scanned_event_location(record, target_key, file_path, location, now_ms)
           end) do
        {:ok, next_location} -> {:cont, {:ok, next_location}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  @doc false
  def reduce_metadata_pages(file_path, initial, reducer)
      when is_binary(file_path) and is_function(reducer, 2) do
    reduce_metadata_pages(file_path, 0, initial, reducer)
  end

  @doc false
  def reduce_metadata_pages(file_path, offset, acc, reducer)
      when is_binary(file_path) and is_integer(offset) and offset >= 0 and
             is_function(reducer, 2) do
    reduce_metadata_pages_with(file_path, offset, acc, reducer, &NIF.v2_scan_file_page/3)
  end

  @doc false
  def reduce_metadata_pages_buffered(file_path, offset, acc, reducer)
      when is_binary(file_path) and is_integer(offset) and offset >= 0 and
             is_function(reducer, 2) do
    reduce_metadata_pages_with(file_path, offset, acc, reducer, &NIF.v2_scan_file_page_buffered/3)
  end

  defp reduce_metadata_pages_with(file_path, offset, acc, reducer, scanner) do
    case scanner.(file_path, offset, @scan_page_records) do
      {:ok, records, next_offset, done}
      when is_list(records) and is_integer(next_offset) and next_offset >= offset and
             is_boolean(done) ->
        next_acc = Enum.reduce(records, acc, reducer)

        cond do
          done ->
            {:ok, next_acc}

          next_offset > offset ->
            reduce_metadata_pages_with(file_path, next_offset, next_acc, reducer, scanner)

          true ->
            {:error, {:history_scan_stalled, offset}}
        end

      {:error, reason} ->
        {:error, reason}

      invalid ->
        {:error, {:invalid_history_scan_page, invalid}}
    end
  end

  defp latest_scanned_event_location(
         {target_key, _offset, _value_size, _expire_at_ms, true},
         target_key,
         _file_path,
         _location,
         _now_ms
       ),
       do: :miss

  defp latest_scanned_event_location(
         {target_key, _offset, _value_size, expire_at_ms, false},
         target_key,
         _file_path,
         _location,
         now_ms
       )
       when is_integer(expire_at_ms) and expire_at_ms > 0 and expire_at_ms <= now_ms,
       do: :miss

  defp latest_scanned_event_location(
         {target_key, offset, _value_size, expire_at_ms, false},
         target_key,
         file_path,
         _location,
         _now_ms
       )
       when is_integer(expire_at_ms),
       do: {:live, file_path, offset}

  defp latest_scanned_event_location(_record, _target_key, _file_path, location, _now_ms),
    do: location

  defp history_file_path(shard_data_path, file_id), do: Files.path(shard_data_path, file_id)
end
