defmodule Ferricstore.TermMemory do
  @moduledoc false

  import Bitwise

  @word_size :erlang.system_info(:wordsize)
  @small_integer_bits @word_size * 8 - 5
  @small_integer_min -(1 <<< (@small_integer_bits - 1))
  @small_integer_max (1 <<< (@small_integer_bits - 1)) - 1

  @spec bytes(term()) :: non_neg_integer()
  def bytes(term), do: estimate(term)

  defp estimate(value) when is_atom(value), do: 0

  defp estimate(value)
       when is_integer(value) and value >= @small_integer_min and value <= @small_integer_max,
       do: 0

  defp estimate(value) when is_integer(value),
    do: :erlang.external_size(value, minor_version: 2) + 3 * @word_size

  defp estimate(value) when is_float(value), do: 3 * @word_size

  defp estimate(value) when is_binary(value) do
    size = byte_size(value)

    if size <= 64,
      do: (3 + div(size + @word_size - 1, @word_size)) * @word_size,
      else: size + 8 * @word_size
  end

  defp estimate(value) when is_bitstring(value),
    do: :erlang.external_size(value, minor_version: 2) + 8 * @word_size

  defp estimate([]), do: 0

  defp estimate([head | tail]),
    do: 2 * @word_size + estimate(head) + estimate(tail)

  defp estimate(value) when is_tuple(value) do
    tuple_bytes = (tuple_size(value) + 2) * @word_size

    value
    |> Tuple.to_list()
    |> Enum.reduce(tuple_bytes, fn item, bytes -> bytes + estimate(item) end)
  end

  defp estimate(value) when is_map(value) do
    map_bytes = (8 + 4 * map_size(value)) * @word_size

    value
    |> Map.to_list()
    |> Enum.reduce(map_bytes, fn {key, item}, bytes ->
      bytes + estimate(key) + estimate(item)
    end)
  end

  defp estimate(value),
    do: 8 * @word_size + 4 * :erlang.external_size(value, minor_version: 2)
end
