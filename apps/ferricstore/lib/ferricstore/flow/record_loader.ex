defmodule Ferricstore.Flow.RecordLoader do
  @moduledoc false

  alias Ferricstore.Flow.Codec
  alias Ferricstore.Flow.Keys
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
    case Enum.find(values, &match?({:error, _reason}, &1)) do
      nil ->
        records =
          values
          |> Enum.reduce([], fn
            nil, acc -> acc
            value, acc when is_binary(value) -> prepend_decoded_record(value, acc, decode_fun)
          end)
          |> Enum.reverse()

        {:ok, records}

      {:error, _reason} = error ->
        error
    end
  end

  defp prepend_decoded_record(value, acc, decode_fun) when is_binary(value) do
    case decode_fun.(value) do
      {:ok, nil} -> acc
      {:ok, record} -> [record | acc]
    end
  end

  defp safe_decode_record(value) when is_binary(value) do
    {:ok, Codec.decode_record(value)}
  rescue
    _ -> {:ok, nil}
  end
end
