defmodule Ferricstore.NativeValueCodec do
  @moduledoc false

  @minimum_integer -0x8000_0000_0000_0000
  @maximum_integer 0x7FFF_FFFF_FFFF_FFFF

  @spec encode(term()) :: binary()
  def encode(nil), do: <<0>>
  def encode(true), do: <<1>>
  def encode(false), do: <<2>>

  def encode(value)
      when is_integer(value) and value >= @minimum_integer and value <= @maximum_integer,
      do: <<3, value::signed-64>>

  def encode(value) when is_integer(value),
    do: raise(ArgumentError, "native integers must fit in signed 64 bits")

  def encode(value) when is_binary(value) do
    length = byte_size(value)
    <<4, length::unsigned-32, value::binary>>
  end

  def encode(value) when is_atom(value), do: value |> Atom.to_string() |> encode()

  def encode(values) when is_list(values) do
    body = values |> Enum.map(&encode/1) |> IO.iodata_to_binary()
    <<5, length(values)::unsigned-32, body::binary>>
  end

  def encode(%_{} = struct), do: struct |> Map.from_struct() |> encode()

  def encode(values) when is_map(values) do
    entries =
      values
      |> Enum.map(fn {key, value} ->
        key = encode_key(key)
        [<<byte_size(key)::unsigned-32>>, key, encode(value)]
      end)
      |> IO.iodata_to_binary()

    <<6, map_size(values)::unsigned-32, entries::binary>>
  end

  def encode(value) when is_float(value), do: <<7, value::float-64>>
  def encode(value) when is_tuple(value), do: value |> Tuple.to_list() |> encode()
  def encode(value), do: value |> inspect(limit: 50) |> encode()

  @spec encoded_size(term()) :: non_neg_integer()
  def encoded_size(value), do: encoded_size(value, 0)

  @spec fits?(term(), non_neg_integer()) :: boolean()
  def fits?(value, maximum_bytes) when is_integer(maximum_bytes) and maximum_bytes >= 0,
    do: match?({:ok, _remaining}, consume(value, maximum_bytes))

  def fits?(_value, _maximum_bytes), do: false

  defp encoded_size(nil, size), do: size + 1
  defp encoded_size(value, size) when is_boolean(value), do: size + 1

  defp encoded_size(value, size)
       when is_integer(value) and value >= @minimum_integer and value <= @maximum_integer,
       do: size + 9

  defp encoded_size(value, _size) when is_integer(value),
    do: raise(ArgumentError, "native integers must fit in signed 64 bits")

  defp encoded_size(value, size) when is_binary(value), do: size + 5 + byte_size(value)

  defp encoded_size(value, size) when is_atom(value),
    do: encoded_size(Atom.to_string(value), size)

  defp encoded_size(value, size) when is_float(value), do: size + 9
  defp encoded_size(%_{} = struct, size), do: encoded_size(Map.from_struct(struct), size)

  defp encoded_size(values, size) when is_list(values),
    do: Enum.reduce(values, size + 5, &encoded_size/2)

  defp encoded_size(values, size) when is_map(values) do
    Enum.reduce(values, size + 5, fn {key, value}, total ->
      key = encode_key(key)
      encoded_size(value, total + 4 + byte_size(key))
    end)
  end

  defp encoded_size(value, size) when is_tuple(value),
    do: encoded_tuple_size(value, 0, tuple_size(value), size + 5)

  defp encoded_size(value, size), do: value |> inspect(limit: 50) |> encoded_size(size)

  defp encoded_tuple_size(_tuple, index, count, size) when index == count, do: size

  defp encoded_tuple_size(tuple, index, count, size),
    do: encoded_tuple_size(tuple, index + 1, count, encoded_size(elem(tuple, index), size))

  defp consume(_value, remaining) when remaining < 0, do: :error
  defp consume(nil, remaining), do: consume_bytes(remaining, 1)
  defp consume(value, remaining) when is_boolean(value), do: consume_bytes(remaining, 1)

  defp consume(value, remaining)
       when is_integer(value) and value >= @minimum_integer and value <= @maximum_integer,
       do: consume_bytes(remaining, 9)

  defp consume(value, _remaining) when is_integer(value), do: :error

  defp consume(value, remaining) when is_binary(value),
    do: consume_bytes(remaining, 5 + byte_size(value))

  defp consume(value, remaining) when is_atom(value),
    do: consume(Atom.to_string(value), remaining)

  defp consume(value, remaining) when is_float(value), do: consume_bytes(remaining, 9)
  defp consume(%_{} = struct, remaining), do: consume(Map.from_struct(struct), remaining)

  defp consume(values, remaining) when is_list(values) do
    with {:ok, remaining} <- consume_bytes(remaining, 5) do
      Enum.reduce_while(values, {:ok, remaining}, fn value, {:ok, available} ->
        case consume(value, available) do
          {:ok, _remaining} = result -> {:cont, result}
          :error -> {:halt, :error}
        end
      end)
    end
  end

  defp consume(values, remaining) when is_map(values) do
    with {:ok, remaining} <- consume_bytes(remaining, 5) do
      Enum.reduce_while(values, {:ok, remaining}, fn {key, value}, {:ok, available} ->
        key = encode_key(key)

        with {:ok, available} <- consume_bytes(available, 4 + byte_size(key)),
             {:ok, _remaining} = result <- consume(value, available) do
          {:cont, result}
        else
          :error -> {:halt, :error}
        end
      end)
    end
  end

  defp consume(value, remaining) when is_tuple(value) do
    with {:ok, remaining} <- consume_bytes(remaining, 5) do
      consume_tuple(value, 0, tuple_size(value), remaining)
    end
  end

  defp consume(value, remaining), do: value |> inspect(limit: 50) |> consume(remaining)

  defp consume_tuple(_tuple, index, count, remaining) when index == count,
    do: {:ok, remaining}

  defp consume_tuple(tuple, index, count, remaining) do
    case consume(elem(tuple, index), remaining) do
      {:ok, remaining} -> consume_tuple(tuple, index + 1, count, remaining)
      :error -> :error
    end
  end

  defp consume_bytes(remaining, bytes) when bytes <= remaining,
    do: {:ok, remaining - bytes}

  defp consume_bytes(_remaining, _bytes), do: :error

  defp encode_key(key) when is_binary(key), do: key
  defp encode_key(key) when is_atom(key), do: Atom.to_string(key)
  defp encode_key(key), do: to_string(key)
end
