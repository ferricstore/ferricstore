defmodule Ferricstore.Flow.HistoryProjector.Log do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF

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

    with {:ok, records} <- NIF.v2_scan_file(file_path) do
      case latest_scanned_event_location(records, target_key) do
        {:live, offset} -> NIF.v2_pread_at(file_path, offset)
        :miss -> :miss
      end
    end
  end

  defp latest_scanned_event_location(records, target_key) do
    Enum.reduce(records, :miss, fn
      {^target_key, _offset, _value_size, _expire_at_ms, true}, _acc ->
        :miss

      {^target_key, offset, _value_size, _expire_at_ms, false}, _acc ->
        {:live, offset}

      _record, acc ->
        acc
    end)
  end

  defp history_file_path(shard_data_path, file_id) do
    Path.join(
      Path.join(shard_data_path, "history"),
      "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log"
    )
  end
end
