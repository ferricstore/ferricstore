defmodule Ferricstore.Flow.HistoryProjector.Log do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.CommandTime

  @scan_page_records 4_096

  @spec read_value(binary(), {:flow_history, non_neg_integer()}, non_neg_integer()) ::
          {:ok, binary()} | {:error, term()}
  def read_value(shard_data_path, {:flow_history, file_id}, offset) do
    shard_data_path
    |> history_file_path(file_id)
    |> NIF.v2_pread_at(offset)
  end

  def read_value(_shard_data_path, _file_id, _offset), do: {:error, :not_flow_history}

  @spec scan_event_value(binary(), binary()) :: {:ok, binary()} | :miss | {:error, term()}
  def scan_event_value(shard_data_path, target_key) do
    file_path = history_file_path(shard_data_path, 0)
    now_ms = CommandTime.now_ms()

    with {:ok, location} <-
           reduce_metadata_pages(file_path, :miss, fn record, location ->
             latest_scanned_event_location(record, target_key, location, now_ms)
           end) do
      case location do
        {:live, offset} -> NIF.v2_pread_at(file_path, offset)
        :miss -> :miss
      end
    end
  end

  @doc false
  def reduce_metadata_pages(file_path, initial, reducer)
      when is_binary(file_path) and is_function(reducer, 2) do
    reduce_metadata_pages(file_path, 0, initial, reducer)
  end

  defp reduce_metadata_pages(file_path, offset, acc, reducer) do
    case NIF.v2_scan_file_page(file_path, offset, @scan_page_records) do
      {:ok, records, next_offset, done}
      when is_list(records) and is_integer(next_offset) and next_offset >= offset and
             is_boolean(done) ->
        next_acc = Enum.reduce(records, acc, reducer)

        cond do
          done ->
            {:ok, next_acc}

          next_offset > offset ->
            reduce_metadata_pages(file_path, next_offset, next_acc, reducer)

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
         _location,
         _now_ms
       ),
       do: :miss

  defp latest_scanned_event_location(
         {target_key, _offset, _value_size, expire_at_ms, false},
         target_key,
         _location,
         now_ms
       )
       when is_integer(expire_at_ms) and expire_at_ms > 0 and expire_at_ms <= now_ms,
       do: :miss

  defp latest_scanned_event_location(
         {target_key, offset, _value_size, expire_at_ms, false},
         target_key,
         _location,
         _now_ms
       )
       when is_integer(expire_at_ms),
       do: {:live, offset}

  defp latest_scanned_event_location(_record, _target_key, location, _now_ms), do: location

  defp history_file_path(shard_data_path, file_id) do
    Path.join(
      Path.join(shard_data_path, "history"),
      "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log"
    )
  end
end
