defmodule Ferricstore.Flow.RecordLoader do
  @moduledoc false

  alias Ferricstore.Flow.Codec
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.ReadResult
  alias Ferricstore.Store.Router

  def records_for_ids(ctx, ids, partition_key) do
    keys = Enum.map(ids, &Keys.state_key(&1, partition_key))

    case Enum.find(keys, &(byte_size(&1) > Router.max_key_size())) do
      nil ->
        ctx
        |> Router.flow_batch_get(ids, partition_key)
        |> decode_values(&safe_decode_record/1)

      _too_large ->
        {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
    end
  end

  def records_for_partitioned_entries(ctx, entries) when is_list(entries) do
    keys =
      Enum.map(entries, fn {id, _score, partition_key} ->
        Keys.state_key(id, partition_key)
      end)

    case Enum.find(keys, &(byte_size(&1) > Router.max_key_size())) do
      nil ->
        ctx
        |> Router.batch_get(keys)
        |> decode_values(&safe_decode_record/1)

      _too_large ->
        {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
    end
  end

  def decode_values(values, decode_fun) when is_function(decode_fun, 1) do
    if is_list(values) do
      values
      |> Enum.reduce_while({:ok, []}, fn
        nil, {:ok, acc} ->
          {:cont, {:ok, acc}}

        value, {:ok, acc} when is_binary(value) ->
          case decode_fun.(value) do
            {:ok, record} when is_map(record) -> {:cont, {:ok, [record | acc]}}
            {:error, _reason} = error -> {:halt, error}
            _invalid -> {:halt, {:error, "ERR invalid flow record"}}
          end

        {:error, _reason} = error, {:ok, _acc} ->
          {:halt, normalize_read_error(error)}

        _invalid, {:ok, _acc} ->
          {:halt, {:error, "ERR storage read failed"}}
      end)
      |> case do
        {:ok, records} -> {:ok, Enum.reverse(records)}
        {:error, _reason} = error -> error
      end
    else
      {:error, "ERR storage read failed"}
    end
  end

  defp safe_decode_record(value) when is_binary(value) do
    {:ok, Codec.decode_record(value)}
  rescue
    _ -> {:error, "ERR invalid flow record"}
  end

  defp normalize_read_error({:error, {:storage_read_failed, _reason}} = failure),
    do: ReadResult.command_error(failure)

  defp normalize_read_error({:error, message}) when is_binary(message), do: {:error, message}
  defp normalize_read_error(_error), do: {:error, "ERR storage read failed"}
end
