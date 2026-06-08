defmodule Ferricstore.Flow.Codec.Primitives do
  @moduledoc false

  import Bitwise

  def encode_int(value) when is_integer(value) and value >= 0 and value < 127,
    do: <<value + 1>>

  def encode_int(value) when is_integer(value) and value >= 0, do: encode_varint(value + 1)

  def encode_int(_value), do: <<0>>

  def decode_int(<<0, rest::binary>>), do: {:ok, nil, rest}

  def decode_int(<<encoded, rest::binary>>) when encoded < 128,
    do: {:ok, encoded - 1, rest}

  def decode_int(binary) do
    with {:ok, encoded, rest} <- decode_varint(binary) do
      case encoded do
        0 -> {:ok, nil, rest}
        value -> {:ok, value - 1, rest}
      end
    end
  end

  def encode_bin(value) when is_binary(value) and byte_size(value) < 127,
    do: [<<byte_size(value) + 1>>, value]

  def encode_bin(value) when is_binary(value),
    do: [encode_varint(byte_size(value) + 1), value]

  def encode_bin(_value), do: <<0>>

  def decode_bin(<<0, rest::binary>>), do: {:ok, nil, rest}

  def decode_bin(<<encoded, rest::binary>>) when encoded < 128 do
    len = encoded - 1

    case rest do
      <<value::binary-size(len), tail::binary>> -> {:ok, value, tail}
      _ -> :error
    end
  end

  def decode_bin(binary) do
    with {:ok, encoded, rest} <- decode_varint(binary) do
      case encoded do
        0 ->
          {:ok, nil, rest}

        size when size > 0 ->
          len = size - 1

          case rest do
            <<value::binary-size(len), tail::binary>> -> {:ok, value, tail}
            _ -> :error
          end
      end
    end
  end

  defp encode_varint(value) when value < 128, do: <<value>>

  defp encode_varint(value) when value < 16_384 do
    <<(value &&& 0x7F) ||| 0x80, value >>> 7>>
  end

  defp encode_varint(value) when value < 2_097_152 do
    <<(value &&& 0x7F) ||| 0x80, (value >>> 7 &&& 0x7F) ||| 0x80, value >>> 14>>
  end

  defp encode_varint(value) when value < 268_435_456 do
    <<(value &&& 0x7F) ||| 0x80, (value >>> 7 &&& 0x7F) ||| 0x80,
      (value >>> 14 &&& 0x7F) ||| 0x80, value >>> 21>>
  end

  defp encode_varint(value) when value < 34_359_738_368 do
    <<(value &&& 0x7F) ||| 0x80, (value >>> 7 &&& 0x7F) ||| 0x80,
      (value >>> 14 &&& 0x7F) ||| 0x80, (value >>> 21 &&& 0x7F) ||| 0x80, value >>> 28>>
  end

  defp encode_varint(value) when value < 4_398_046_511_104 do
    <<(value &&& 0x7F) ||| 0x80, (value >>> 7 &&& 0x7F) ||| 0x80,
      (value >>> 14 &&& 0x7F) ||| 0x80, (value >>> 21 &&& 0x7F) ||| 0x80,
      (value >>> 28 &&& 0x7F) ||| 0x80, value >>> 35>>
  end

  defp encode_varint(value) when value >= 128 do
    <<(value &&& 0x7F) ||| 0x80>> <> encode_varint(value >>> 7)
  end

  defp decode_varint(binary), do: decode_varint(binary, 0, 0)

  defp decode_varint(<<byte, rest::binary>>, acc, shift) when shift < 70 do
    value = acc ||| (byte &&& 0x7F) <<< shift

    if (byte &&& 0x80) == 0 do
      {:ok, value, rest}
    else
      decode_varint(rest, value, shift + 7)
    end
  end

  defp decode_varint(_binary, _acc, _shift), do: :error
end
