defmodule Ferricstore.Commands.Strings.Range do
  @moduledoc false

  alias Ferricstore.Commands.Strings.Compound
  alias Ferricstore.Store.{Ops, ReadResult}

  def getrange_parsed(key, start_idx, end_idx, store) do
    case metadata_value_size(store, key) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      size when is_integer(size) ->
        if getrange_empty_for_size?(size, start_idx, end_idx) do
          empty_range_result(key, store)
        else
          read_getrange_value(key, start_idx, end_idx, store)
        end

      _unknown_or_missing ->
        read_getrange_value(key, start_idx, end_idx, store)
    end
  end

  defp metadata_value_size(%FerricStore.Instance{} = store, key), do: Ops.value_size(store, key)

  defp metadata_value_size(%Ferricstore.Store.LocalTxStore{} = store, key),
    do: Ops.value_size(store, key)

  defp metadata_value_size(%{value_size: value_size}, key) when is_function(value_size, 1),
    do: value_size.(key)

  defp metadata_value_size(_store, _key), do: :unknown

  defp read_getrange_value(key, start_idx, end_idx, store) do
    case Ops.getrange(store, key, start_idx, end_idx) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      nil ->
        empty_range_result(key, store)

      value ->
        value
    end
  end

  defp getrange_empty_for_size?(size, start_idx, end_idx) do
    start_norm = if start_idx < 0, do: max(size + start_idx, 0), else: start_idx
    end_norm = if end_idx < 0, do: size + end_idx, else: end_idx

    start_clamped = min(start_norm, size)
    end_clamped = min(end_norm, size - 1)

    start_clamped > end_clamped
  end

  defp empty_range_result(key, store) do
    case Compound.data_structure_status(key, store) do
      :compound -> {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
      :plain -> ""
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
    end
  end
end
