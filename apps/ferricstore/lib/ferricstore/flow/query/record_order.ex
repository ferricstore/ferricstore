defmodule Ferricstore.Flow.Query.RecordOrder do
  @moduledoc false

  alias Ferricstore.Flow.Query.{Field, Limits, TupleCodec}

  @entry_identity_tag 0x60
  @identity_bytes 33
  @max_sort_key_bytes Limits.max_sort_key_bytes()
  @max_field_bytes @max_sort_key_bytes - @identity_bytes

  @spec sort([map()], [{Field.t(), :asc | :desc}]) ::
          {:ok, [map()]} | {:error, :unsupported_query_order_value}
  def sort(records, order_by) when is_list(records) and is_list(order_by) do
    records
    |> Enum.reduce_while({:ok, []}, fn record, {:ok, acc} ->
      case sort_key(record, order_by) do
        {:ok, key} -> {:cont, {:ok, [{key, record} | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, keyed} ->
        {:ok,
         keyed
         |> Enum.sort_by(&elem(&1, 0))
         |> Enum.map(&elem(&1, 1))}

      {:error, _reason} = error ->
        error
    end
  end

  def sort(_records, _order_by), do: {:error, :unsupported_query_order_value}

  @spec sort_key(map(), [{Field.t(), :asc | :desc}]) ::
          {:ok, binary()} | {:error, :unsupported_query_order_value}
  def sort_key(record, order_by) when is_map(record) and is_list(order_by) do
    with {:ok, components} <- encode_fields(record, order_by, [], 0),
         {:ok, id} <- run_id(record) do
      {:ok,
       IO.iodata_to_binary([
         Enum.reverse(components),
         <<@entry_identity_tag, :crypto.hash(:sha256, id)::binary-size(32)>>
       ])}
    end
  end

  def sort_key(_record, _order_by), do: {:error, :unsupported_query_order_value}

  @spec run_ref(binary()) :: <<_::256>>
  def run_ref(id) when is_binary(id) and id != "", do: :crypto.hash(:sha256, id)

  defp encode_fields(_record, [], acc, _bytes), do: {:ok, acc}

  defp encode_fields(record, [{field, direction} | rest], acc, bytes)
       when direction in [:asc, :desc] do
    value =
      case Field.fetch(record, field) do
        {:ok, value} -> value
        :missing -> Field.missing()
      end

    with :ok <- validate_value_size(value),
         {:ok, encoded} <- TupleCodec.encode_component_safe(value, direction),
         next_bytes = bytes + byte_size(encoded),
         true <- next_bytes <= @max_field_bytes do
      encode_fields(record, rest, [encoded | acc], next_bytes)
    else
      _invalid -> {:error, :unsupported_query_order_value}
    end
  end

  defp encode_fields(_record, _order_by, _acc, _bytes),
    do: {:error, :unsupported_query_order_value}

  defp validate_value_size(value) when is_binary(value) and byte_size(value) > @max_field_bytes,
    do: {:error, :unsupported_query_order_value}

  defp validate_value_size(_value), do: :ok

  defp run_id(record) do
    case Field.fetch(record, :run_id) do
      {:ok, id} when is_binary(id) and id != "" -> {:ok, id}
      _invalid -> {:error, :unsupported_query_order_value}
    end
  end
end
