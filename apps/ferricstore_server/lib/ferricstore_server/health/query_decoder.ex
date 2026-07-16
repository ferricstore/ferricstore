defmodule FerricstoreServer.Health.QueryDecoder do
  @moduledoc false

  @spec decode(binary()) :: map()
  def decode(query) when is_binary(query) do
    params = URI.decode_query(query)

    if Enum.all?(params, &valid_pair?/1), do: params, else: %{}
  rescue
    _error -> %{}
  end

  def decode(_query), do: %{}

  @spec decode_component(binary()) :: {:ok, binary()} | :error
  def decode_component(encoded) when is_binary(encoded) do
    decoded = URI.decode(encoded)

    if String.valid?(decoded), do: {:ok, decoded}, else: :error
  rescue
    _error -> :error
  end

  def decode_component(_encoded), do: :error

  defp valid_pair?({key, value}) do
    is_binary(key) and is_binary(value) and String.valid?(key) and String.valid?(value)
  end
end
