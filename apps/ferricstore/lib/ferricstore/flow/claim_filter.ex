defmodule Ferricstore.Flow.ClaimFilter do
  @moduledoc false

  @max_footprint 64
  @footprint_error "ERR flow claim filter footprint exceeds maximum #{@max_footprint}"

  @spec validate_footprint(term(), term()) :: :ok | {:error, binary()}
  def validate_footprint(state_filter, partition_filter) do
    state_count = dimension_count(state_filter)
    partition_count = dimension_count(partition_filter)

    if state_count <= div(@max_footprint, partition_count) do
      :ok
    else
      {:error, @footprint_error}
    end
  end

  defp dimension_count(values) when is_list(values), do: max(length(values), 1)
  defp dimension_count(_value), do: 1
end
