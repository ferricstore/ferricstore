defmodule Ferricstore.Flow.PartitionKey do
  @moduledoc false

  @prefix "fpk:"

  @spec encode_components([binary(), ...]) :: binary()
  def encode_components([_first | _rest] = components) do
    [@prefix | Enum.map(components, &encode_component/1)]
    |> IO.iodata_to_binary()
  end

  defp encode_component(component) when is_binary(component) do
    [Integer.to_string(byte_size(component)), ?:, component]
  end
end
