defmodule Ferricstore.Flow.Query.QueryRowPrimitives do
  @moduledoc false

  import Bitwise

  @nil_tag 0
  @false_tag 1
  @true_tag 2
  @integer_tag 3
  @float_tag 4
  @binary_tag 5
  @list_tag 6

  @max_u64 0xFFFF_FFFF_FFFF_FFFF
  @min_i64 -0x8000_0000_0000_0000
  @max_i64 0x7FFF_FFFF_FFFF_FFFF
  @maximum_list_values 16

  @spec encode_u64(non_neg_integer()) :: {:ok, binary()} | :error
  def encode_u64(value) when is_integer(value) and value >= 0 and value <= @max_u64,
    do: {:ok, value |> encode_varint([]) |> IO.iodata_to_binary()}

  def encode_u64(_value), do: :error

  @spec decode_u64(binary()) :: {:ok, non_neg_integer(), binary()} | :error
  def decode_u64(encoded) when is_binary(encoded), do: decode_varint(encoded, 0, 0, 0)
  def decode_u64(_encoded), do: :error

  @spec encode(term(), pos_integer(), boolean()) ::
          {:ok, binary(), non_neg_integer()} | :error
  def encode(nil, _maximum_binary_bytes, _allow_list?), do: {:ok, <<@nil_tag>>, 0}
  def encode(false, _maximum_binary_bytes, _allow_list?), do: {:ok, <<@false_tag>>, 1}
  def encode(true, _maximum_binary_bytes, _allow_list?), do: {:ok, <<@true_tag>>, 1}

  def encode(value, _maximum_binary_bytes, _allow_list?)
      when is_integer(value) and value >= @min_i64 and value <= @max_i64 do
    encoded = zigzag_encode(value)

    case encode_u64(encoded) do
      {:ok, bytes} -> {:ok, <<@integer_tag, bytes::binary>>, 8}
      :error -> :error
    end
  end

  def encode(value, _maximum_binary_bytes, _allow_list?) when is_float(value) do
    <<bits::unsigned-big-64>> = <<value::float-big-64>>

    if finite_float_bits?(bits),
      do: {:ok, <<@float_tag, bits::unsigned-big-64>>, 8},
      else: :error
  end

  def encode(value, maximum_binary_bytes, _allow_list?)
      when is_binary(value) and is_integer(maximum_binary_bytes) and maximum_binary_bytes > 0 and
             byte_size(value) <= maximum_binary_bytes do
    with {:ok, length} <- encode_u64(byte_size(value)) do
      {:ok, <<@binary_tag, length::binary, value::binary>>, byte_size(value)}
    end
  end

  def encode(values, maximum_binary_bytes, true)
      when is_list(values) and values != [] and length(values) <= @maximum_list_values and
             is_integer(maximum_binary_bytes) and maximum_binary_bytes > 0 do
    values
    |> Enum.reduce_while({:ok, 0, []}, fn value, {:ok, semantic_bytes, acc} ->
      if is_binary(value) and byte_size(value) <= maximum_binary_bytes do
        case encode_u64(byte_size(value)) do
          {:ok, length} ->
            {:cont, {:ok, semantic_bytes + byte_size(value), [[length, value] | acc]}}

          :error ->
            {:halt, :error}
        end
      else
        {:halt, :error}
      end
    end)
    |> case do
      {:ok, semantic_bytes, reversed} ->
        {:ok, IO.iodata_to_binary([<<@list_tag, length(values)>>, Enum.reverse(reversed)]),
         semantic_bytes}

      :error ->
        :error
    end
  end

  def encode(_value, _maximum_binary_bytes, _allow_list?), do: :error

  @spec decode(binary(), pos_integer(), boolean()) ::
          {:ok, term(), binary(), non_neg_integer()} | :error
  def decode(<<@nil_tag, rest::binary>>, _maximum_binary_bytes, _allow_list?),
    do: {:ok, nil, rest, 0}

  def decode(<<@false_tag, rest::binary>>, _maximum_binary_bytes, _allow_list?),
    do: {:ok, false, rest, 1}

  def decode(<<@true_tag, rest::binary>>, _maximum_binary_bytes, _allow_list?),
    do: {:ok, true, rest, 1}

  def decode(<<@integer_tag, encoded::binary>>, _maximum_binary_bytes, _allow_list?) do
    with {:ok, value, rest} <- decode_u64(encoded) do
      {:ok, zigzag_decode(value), rest, 8}
    end
  end

  def decode(
        <<@float_tag, bits::unsigned-big-64, rest::binary>>,
        _maximum_binary_bytes,
        _allow_list?
      ) do
    if finite_float_bits?(bits) do
      <<value::float-big-64>> = <<bits::unsigned-big-64>>
      {:ok, value, rest, 8}
    else
      :error
    end
  end

  def decode(<<@binary_tag, encoded::binary>>, maximum_binary_bytes, _allow_list?)
      when is_integer(maximum_binary_bytes) and maximum_binary_bytes > 0 do
    with {:ok, bytes, rest} <- decode_u64(encoded),
         true <- bytes <= maximum_binary_bytes,
         <<value::binary-size(bytes), tail::binary>> <- rest do
      {:ok, :binary.copy(value), tail, bytes}
    else
      _invalid -> :error
    end
  end

  def decode(<<@list_tag, count, encoded::binary>>, maximum_binary_bytes, true)
      when count > 0 and count <= @maximum_list_values and is_integer(maximum_binary_bytes) and
             maximum_binary_bytes > 0 do
    decode_binary_list(count, encoded, maximum_binary_bytes, [], 0)
  end

  def decode(_encoded, _maximum_binary_bytes, _allow_list?), do: :error

  @doc false
  @spec skip(binary(), pos_integer(), boolean()) ::
          {:ok, binary(), non_neg_integer()} | :error
  def skip(<<@nil_tag, rest::binary>>, _maximum_binary_bytes, _allow_list?),
    do: {:ok, rest, 0}

  def skip(<<@false_tag, rest::binary>>, _maximum_binary_bytes, _allow_list?),
    do: {:ok, rest, 1}

  def skip(<<@true_tag, rest::binary>>, _maximum_binary_bytes, _allow_list?),
    do: {:ok, rest, 1}

  def skip(<<@integer_tag, encoded::binary>>, _maximum_binary_bytes, _allow_list?) do
    with {:ok, _value, rest} <- decode_u64(encoded), do: {:ok, rest, 8}
  end

  def skip(
        <<@float_tag, bits::unsigned-big-64, rest::binary>>,
        _maximum_binary_bytes,
        _allow_list?
      ) do
    if finite_float_bits?(bits), do: {:ok, rest, 8}, else: :error
  end

  def skip(<<@binary_tag, encoded::binary>>, maximum_binary_bytes, _allow_list?)
      when is_integer(maximum_binary_bytes) and maximum_binary_bytes > 0 do
    with {:ok, bytes, rest} <- decode_u64(encoded),
         true <- bytes <= maximum_binary_bytes,
         <<_value::binary-size(bytes), tail::binary>> <- rest do
      {:ok, tail, bytes}
    else
      _invalid -> :error
    end
  end

  def skip(<<@list_tag, count, encoded::binary>>, maximum_binary_bytes, true)
      when count > 0 and count <= @maximum_list_values and is_integer(maximum_binary_bytes) and
             maximum_binary_bytes > 0 do
    skip_binary_list(count, encoded, maximum_binary_bytes, MapSet.new(), 0)
  end

  def skip(_encoded, _maximum_binary_bytes, _allow_list?), do: :error

  defp encode_varint(value, acc) when value < 0x80,
    do: Enum.reverse([<<value>> | acc])

  defp encode_varint(value, acc),
    do: encode_varint(value >>> 7, [<<(value &&& 0x7F) ||| 0x80>> | acc])

  defp decode_varint(<<byte, rest::binary>>, acc, shift, group) when group < 10 do
    payload = byte &&& 0x7F
    continuation? = (byte &&& 0x80) != 0

    cond do
      group == 9 and (continuation? or payload != 1) ->
        :error

      not continuation? and group > 0 and payload == 0 ->
        :error

      not continuation? ->
        {:ok, acc ||| payload <<< shift, rest}

      true ->
        decode_varint(rest, acc ||| payload <<< shift, shift + 7, group + 1)
    end
  end

  defp decode_varint(_encoded, _acc, _shift, _group), do: :error

  defp decode_binary_list(0, rest, _maximum_binary_bytes, acc, semantic_bytes),
    do: {:ok, Enum.reverse(acc), rest, semantic_bytes}

  defp decode_binary_list(count, encoded, maximum_binary_bytes, acc, semantic_bytes)
       when count > 0 do
    with {:ok, bytes, rest} <- decode_u64(encoded),
         true <- bytes <= maximum_binary_bytes,
         <<value::binary-size(bytes), tail::binary>> <- rest do
      decode_binary_list(
        count - 1,
        tail,
        maximum_binary_bytes,
        [:binary.copy(value) | acc],
        semantic_bytes + bytes
      )
    else
      _invalid -> :error
    end
  end

  defp skip_binary_list(0, rest, _maximum_binary_bytes, _seen, semantic_bytes),
    do: {:ok, rest, semantic_bytes}

  defp skip_binary_list(count, encoded, maximum_binary_bytes, seen, semantic_bytes)
       when count > 0 do
    with {:ok, bytes, rest} <- decode_u64(encoded),
         true <- bytes <= maximum_binary_bytes,
         <<value::binary-size(bytes), tail::binary>> <- rest,
         false <- MapSet.member?(seen, value) do
      skip_binary_list(
        count - 1,
        tail,
        maximum_binary_bytes,
        MapSet.put(seen, value),
        semantic_bytes + bytes
      )
    else
      _invalid -> :error
    end
  end

  defp zigzag_encode(value), do: bxor(value <<< 1, value >>> 63)
  defp zigzag_decode(value), do: bxor(value >>> 1, -(value &&& 1))

  defp finite_float_bits?(bits), do: (bits >>> 52 &&& 0x7FF) != 0x7FF
end
