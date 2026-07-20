defmodule Ferricstore.Flow.Query.TupleCodec do
  @moduledoc false

  import Bitwise

  alias Ferricstore.Flow.Query.Field

  @null_tag 0xFE
  @missing_tag 0xFF
  @false_tag 0x20
  @true_tag 0x21
  @integer_tag 0x30
  @float_tag 0x31
  @binary_tag 0x40
  @sign_bit 0x8000_0000_0000_0000
  @u64_mask 0xFFFF_FFFF_FFFF_FFFF
  @min_i64 -0x8000_0000_0000_0000
  @max_i64 0x7FFF_FFFF_FFFF_FFFF

  @type direction :: :asc | :desc
  @type value ::
          {:ferric_query, :missing}
          | nil
          | boolean()
          | integer()
          | float()
          | binary()

  @spec encode([value()], [{Field.t(), direction()}]) ::
          {:ok, binary()} | {:error, atom()}
  def encode(values, fields) when is_list(values) and is_list(fields) do
    if length(values) == length(fields) do
      encode_pairs(Enum.zip(values, fields), [])
    else
      {:error, :invalid_tuple_arity}
    end
  end

  def encode(_values, _fields), do: {:error, :invalid_tuple_arity}

  @spec encode_prefix([value()], [{Field.t(), direction()}]) ::
          {:ok, binary()} | {:error, atom()}
  def encode_prefix(values, fields) when is_list(values) and is_list(fields) do
    if length(values) <= length(fields) do
      encode_pairs(Enum.zip(values, Enum.take(fields, length(values))), [])
    else
      {:error, :invalid_tuple_arity}
    end
  end

  def encode_prefix(_values, _fields), do: {:error, :invalid_tuple_arity}

  @spec encode_component(value(), direction()) :: binary()
  def encode_component(value, direction) do
    case encode_component_result(value, direction) do
      {:ok, encoded} -> encoded
      {:error, reason} -> raise ArgumentError, "cannot encode query tuple value: #{reason}"
    end
  end

  @spec encode_component_safe(value(), direction()) :: {:ok, binary()} | {:error, atom()}
  def encode_component_safe(value, direction), do: encode_component_result(value, direction)

  @spec decode_component(binary(), direction()) ::
          {:ok, value(), binary()} | {:error, atom()}
  def decode_component(<<@null_tag, rest::binary>>, direction) when direction in [:asc, :desc],
    do: {:ok, nil, rest}

  def decode_component(<<@missing_tag, rest::binary>>, direction) when direction in [:asc, :desc],
    do: {:ok, Field.missing(), rest}

  def decode_component(encoded, direction)
      when is_binary(encoded) and direction in [:asc, :desc] do
    with {:ok, tag, rest} <- take_byte(encoded, direction) do
      decode_tag(tag, rest, direction)
    end
  end

  def decode_component(_encoded, _direction), do: {:error, :invalid_tuple_encoding}

  @spec compare_values(value(), value()) :: :lt | :eq | :gt
  def compare_values(left, right), do: compare_values(left, right, :asc)

  @spec compare_values(value(), value(), direction()) :: :lt | :eq | :gt
  def compare_values(left, right, direction) when direction in [:asc, :desc] do
    left_encoded = encode_component(left, direction)
    right_encoded = encode_component(right, direction)

    cond do
      left_encoded < right_encoded -> :lt
      left_encoded > right_encoded -> :gt
      true -> :eq
    end
  end

  defp encode_pairs([], acc), do: {:ok, acc |> Enum.reverse() |> IO.iodata_to_binary()}

  defp encode_pairs([{value, {field, direction}} | rest], acc)
       when direction in [:asc, :desc] do
    if Field.valid?(field) do
      case encode_component_result(value, direction) do
        {:ok, encoded} -> encode_pairs(rest, [encoded | acc])
        {:error, _reason} = error -> error
      end
    else
      {:error, :unsupported_index_field}
    end
  end

  defp encode_pairs(_pairs, _acc), do: {:error, :invalid_index_direction}

  defp encode_component_result(value, direction)
       when direction in [:asc, :desc] and (is_nil(value) or value == {:ferric_query, :missing}),
       do: encode_ascending(value)

  defp encode_component_result(value, direction) when direction in [:asc, :desc] do
    with {:ok, ascending} <- encode_ascending(value) do
      {:ok, maybe_invert(ascending, direction)}
    end
  end

  defp encode_component_result(_value, _direction), do: {:error, :invalid_index_direction}

  defp encode_ascending({:ferric_query, :missing}), do: {:ok, <<@missing_tag>>}
  defp encode_ascending(nil), do: {:ok, <<@null_tag>>}
  defp encode_ascending(false), do: {:ok, <<@false_tag>>}
  defp encode_ascending(true), do: {:ok, <<@true_tag>>}

  defp encode_ascending(value)
       when is_integer(value) and value >= @min_i64 and value <= @max_i64 do
    ordered = bxor(value &&& @u64_mask, @sign_bit)
    {:ok, <<@integer_tag, ordered::unsigned-big-64>>}
  end

  defp encode_ascending(value) when is_integer(value), do: {:error, :integer_out_of_range}

  defp encode_ascending(value) when is_float(value) do
    <<bits::unsigned-big-64>> = <<value::float-big-64>>
    bits = if bits == @sign_bit, do: 0, else: bits
    exponent = bits >>> 52 &&& 0x7FF

    if exponent == 0x7FF do
      {:error, :non_finite_float}
    else
      ordered =
        if (bits &&& @sign_bit) == 0,
          do: bxor(bits, @sign_bit),
          else: bnot(bits) &&& @u64_mask

      {:ok, <<@float_tag, ordered::unsigned-big-64>>}
    end
  end

  defp encode_ascending(value) when is_binary(value) do
    escaped = for <<byte <- value>>, do: if(byte == 0, do: <<0, 0xFF>>, else: <<byte>>)
    {:ok, IO.iodata_to_binary([<<@binary_tag>>, escaped, <<0, 0>>])}
  end

  defp encode_ascending(_value), do: {:error, :unsupported_index_value}

  defp decode_tag(@missing_tag, rest, _direction), do: {:ok, Field.missing(), rest}
  defp decode_tag(@null_tag, rest, _direction), do: {:ok, nil, rest}
  defp decode_tag(@false_tag, rest, _direction), do: {:ok, false, rest}
  defp decode_tag(@true_tag, rest, _direction), do: {:ok, true, rest}

  defp decode_tag(@integer_tag, rest, direction) do
    with {:ok, ordered, tail} <- take_u64(rest, direction) do
      bits = bxor(ordered, @sign_bit)
      value = if (bits &&& @sign_bit) == 0, do: bits, else: bits - (@u64_mask + 1)
      {:ok, value, tail}
    end
  end

  defp decode_tag(@float_tag, rest, direction) do
    with {:ok, ordered, tail} <- take_u64(rest, direction) do
      bits =
        if (ordered &&& @sign_bit) == 0,
          do: bnot(ordered) &&& @u64_mask,
          else: bxor(ordered, @sign_bit)

      <<value::float-big-64>> = <<bits::unsigned-big-64>>
      {:ok, value, tail}
    end
  end

  defp decode_tag(@binary_tag, rest, direction), do: take_binary(rest, direction, [])
  defp decode_tag(_tag, _rest, _direction), do: {:error, :invalid_tuple_encoding}

  defp take_byte(<<byte, rest::binary>>, :asc), do: {:ok, byte, rest}

  defp take_byte(<<byte, rest::binary>>, :desc), do: {:ok, bxor(byte, 0xFF), rest}
  defp take_byte(_encoded, _direction), do: {:error, :invalid_tuple_encoding}

  defp take_u64(<<bytes::binary-size(8), rest::binary>>, direction) do
    decoded = maybe_invert(bytes, direction)
    <<value::unsigned-big-64>> = decoded
    {:ok, value, rest}
  end

  defp take_u64(_encoded, _direction), do: {:error, :invalid_tuple_encoding}

  defp take_binary(encoded, direction, acc) do
    with {:ok, byte, rest} <- take_byte(encoded, direction) do
      if byte == 0 do
        with {:ok, escaped, tail} <- take_byte(rest, direction) do
          case escaped do
            0 -> {:ok, acc |> Enum.reverse() |> :erlang.list_to_binary(), tail}
            0xFF -> take_binary(tail, direction, [0 | acc])
            _other -> {:error, :invalid_tuple_encoding}
          end
        end
      else
        take_binary(rest, direction, [byte | acc])
      end
    end
  end

  defp maybe_invert(binary, :asc), do: binary

  defp maybe_invert(binary, :desc),
    do: for(<<byte <- binary>>, into: <<>>, do: <<bxor(byte, 0xFF)>>)
end
