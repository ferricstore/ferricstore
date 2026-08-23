defmodule FerricstoreHttp.BinaryEnvelope do
  @moduledoc """
  Binary-safe JSON encoding used by FerricStore HTTP SDKs.
  """

  @encoding "ferricstore-json-v1"
  @bytes_tag "$ferricstore_bytes"
  @map_tag "$ferricstore_map"

  @spec encoding() :: binary()
  def encoding, do: @encoding

  @spec decode(term()) :: {:ok, term()} | {:error, :malformed_binary_envelope}
  def decode(value) do
    {:ok, decode!(value)}
  catch
    :throw, :malformed_binary_envelope -> {:error, :malformed_binary_envelope}
  end

  @spec encode(term()) :: term()
  def encode(value) when is_binary(value), do: %{@bytes_tag => Base.encode64(value)}
  def encode(value) when is_list(value), do: Enum.map(value, &encode/1)

  def encode(%{} = value) do
    pairs = Enum.map(value, fn {key, item} -> [encode(key), encode(item)] end)
    %{@map_tag => pairs}
  end

  def encode(value) when is_tuple(value), do: value |> Tuple.to_list() |> encode()
  def encode(value), do: value

  defp decode!(%{@bytes_tag => encoded} = marker) when map_size(marker) == 1 do
    if is_binary(encoded) do
      case Base.decode64(encoded) do
        {:ok, bytes} -> bytes
        :error -> throw(:malformed_binary_envelope)
      end
    else
      throw(:malformed_binary_envelope)
    end
  end

  defp decode!(%{@map_tag => pairs} = marker) when map_size(marker) == 1 do
    if is_list(pairs) do
      Enum.reduce(pairs, %{}, fn
        [key, value], acc -> Map.put(acc, decode!(key), decode!(value))
        _invalid_pair, _acc -> throw(:malformed_binary_envelope)
      end)
    else
      throw(:malformed_binary_envelope)
    end
  end

  defp decode!(value) when is_list(value), do: Enum.map(value, &decode!/1)
  defp decode!(%{} = value), do: Map.new(value, fn {key, item} -> {key, decode!(item)} end)
  defp decode!(value), do: value
end
