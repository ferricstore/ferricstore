defmodule Ferricstore.Flow.Schedule.Limits do
  @moduledoc false

  alias Ferricstore.Flow.Codec

  @runtime_capacity_error "ERR flow schedule definition leaves insufficient room for runtime state; " <>
                            "reduce target id/id_prefix or use payload_ref/value_refs"

  @spec validate_definition(map(), map(), pos_integer(), pos_integer()) ::
          :ok | {:error, binary()}
  def validate_definition(
        definition,
        max_runtime_definition,
        definition_max_bytes,
        hydration_max_bytes
      )
      when is_map(definition) and is_map(max_runtime_definition) and
             is_integer(definition_max_bytes) and definition_max_bytes > 0 and
             is_integer(hydration_max_bytes) and hydration_max_bytes > 0 do
    cond do
      encoded_size(definition) > definition_max_bytes ->
        {:error, "ERR flow schedule definition too large; use payload_ref/value_refs"}

      encoded_size(max_runtime_definition) > hydration_max_bytes ->
        {:error, @runtime_capacity_error}

      true ->
        :ok
    end
  end

  defp encoded_size(definition), do: definition |> Codec.encode_value() |> byte_size()
end
