defmodule Ferricstore.Store.AppendResult do
  @moduledoc false

  @spec validate_locations(term(), non_neg_integer()) :: :ok | {:error, term()}
  def validate_locations(locations, expected_count)
      when is_list(locations) and is_integer(expected_count) and expected_count >= 0 do
    validate_locations(locations, expected_count, 0)
  end

  def validate_locations(locations, _expected_count),
    do: {:error, {:invalid_locations, locations}}

  @spec validate_operation_locations(term(), list()) :: :ok | {:error, term()}
  def validate_operation_locations(locations, operations)
      when is_list(locations) and is_list(operations) do
    validate_operation_locations(locations, operations, 0)
  end

  def validate_operation_locations(locations, _operations),
    do: {:error, {:invalid_locations, locations}}

  defp validate_locations([], expected_count, expected_count), do: :ok

  defp validate_locations([], expected_count, seen),
    do: {:error, {:location_count_mismatch, expected_count, seen}}

  defp validate_locations(locations, expected_count, seen) when seen >= expected_count do
    {:error, {:location_count_mismatch, expected_count, seen + length(locations)}}
  end

  defp validate_locations([{offset, record_size} | locations], expected_count, seen)
       when is_integer(offset) and offset >= 0 and is_integer(record_size) and record_size >= 0 do
    validate_locations(locations, expected_count, seen + 1)
  end

  defp validate_locations([invalid | _locations], _expected_count, seen),
    do: {:error, {:invalid_location, seen, invalid}}

  defp validate_operation_locations([], [], _index), do: :ok

  defp validate_operation_locations([], operations, index),
    do: {:error, {:location_count_mismatch, index + length(operations), index}}

  defp validate_operation_locations(locations, [], index),
    do: {:error, {:location_count_mismatch, index, index + length(locations)}}

  defp validate_operation_locations([location | locations], [operation | operations], index) do
    expected_tag = operation_tag(operation)

    case location do
      {^expected_tag, offset, record_size}
      when expected_tag in [:put, :delete] and is_integer(offset) and offset >= 0 and
             is_integer(record_size) and record_size >= 0 ->
        validate_operation_locations(locations, operations, index + 1)

      _invalid ->
        {:error, {:operation_location_mismatch, index, expected_tag, location}}
    end
  end

  defp operation_tag(operation)
       when is_tuple(operation) and tuple_size(operation) > 0 and
              elem(operation, 0) in [:put, :delete],
       do: elem(operation, 0)

  defp operation_tag(_operation), do: :invalid
end
