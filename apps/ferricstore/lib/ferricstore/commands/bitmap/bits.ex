defmodule Ferricstore.Commands.Bitmap.Bits do
  @moduledoc false

  import Bitwise

  @spec extend_binary(binary(), non_neg_integer()) :: binary()
  def extend_binary(bin, min_size) when byte_size(bin) >= min_size, do: bin

  def extend_binary(bin, min_size) do
    padding_size = min_size - byte_size(bin)
    <<bin::binary, 0::size(padding_size * 8)>>
  end

  @spec pad_binary(binary(), non_neg_integer()) :: binary()
  def pad_binary(bin, target_size) when byte_size(bin) >= target_size, do: bin

  def pad_binary(bin, target_size) do
    padding = target_size - byte_size(bin)
    <<bin::binary, 0::size(padding * 8)>>
  end

  @spec popcount(binary()) :: non_neg_integer()
  def popcount(<<>>), do: 0

  def popcount(binary) do
    for <<byte::8 <- binary>>, reduce: 0 do
      acc -> acc + byte_popcount(byte)
    end
  end

  @spec bitcount_byte_range(binary(), integer(), integer()) :: non_neg_integer()
  def bitcount_byte_range(<<>>, _start, _stop), do: 0

  def bitcount_byte_range(bin, start_idx, end_idx) do
    len = byte_size(bin)
    s = resolve_index(start_idx, len)
    e = resolve_index(end_idx, len)

    if s > e or s >= len or e < 0 do
      0
    else
      s = max(s, 0)
      e = min(e, len - 1)
      slice_size = e - s + 1
      <<_::binary-size(s), slice::binary-size(slice_size), _::binary>> = bin
      popcount(slice)
    end
  end

  @spec bitcount_bit_range(binary(), integer(), integer()) :: non_neg_integer()
  def bitcount_bit_range(<<>>, _start, _stop), do: 0

  def bitcount_bit_range(bin, start_idx, end_idx) do
    total_bits = byte_size(bin) * 8
    s = resolve_index(start_idx, total_bits)
    e = resolve_index(end_idx, total_bits)

    if s > e or s >= total_bits or e < 0 do
      0
    else
      s = max(s, 0)
      e = min(e, total_bits - 1)

      s..e
      |> Enum.count(fn bit_offset ->
        byte_idx = div(bit_offset, 8)
        bit_pos = 7 - rem(bit_offset, 8)
        byte = :binary.at(bin, byte_idx)
        (byte >>> bit_pos &&& 1) == 1
      end)
    end
  end

  @spec resolve_index(integer(), non_neg_integer()) :: integer()
  def resolve_index(idx, _len) when idx >= 0, do: idx
  def resolve_index(idx, len), do: len + idx

  @spec bitpos_byte_range(binary(), 0 | 1, integer(), integer(), boolean()) :: integer()
  def bitpos_byte_range(<<>>, 1, _start, _stop, _explicit_end), do: -1
  def bitpos_byte_range(<<>>, 0, _start, _stop, _explicit_end), do: 0

  def bitpos_byte_range(bin, bit_val, start_byte, end_byte, explicit_end) do
    len = byte_size(bin)
    s = max(start_byte, 0)
    e = min(end_byte, len - 1)

    if s > e or s >= len do
      if bit_val == 0 and not explicit_end, do: len * 8, else: -1
    else
      bin
      |> scan_bytes_for_bit(bit_val, s, e)
      |> bitpos_not_found_fallback(bit_val, e, explicit_end)
    end
  end

  @spec bitpos_bit_range(binary(), 0 | 1, integer(), integer()) :: integer()
  def bitpos_bit_range(bin, bit_val, start_bit, end_bit) do
    total_bits = byte_size(bin) * 8
    s = max(start_bit, 0)
    e = min(end_bit, total_bits - 1)

    if s > e or s >= total_bits do
      -1
    else
      Enum.find(s..e, -1, fn bit_offset ->
        byte_idx = div(bit_offset, 8)
        bit_pos = 7 - rem(bit_offset, 8)
        byte = :binary.at(bin, byte_idx)
        (byte >>> bit_pos &&& 1) == bit_val
      end)
    end
  end

  @spec bitop_not(binary()) :: binary()
  def bitop_not(bin) do
    for <<byte::8 <- bin>>, into: <<>> do
      <<Bitwise.bnot(byte) &&& 0xFF::8>>
    end
  end

  @spec bitop_combine([binary()], (byte(), byte() -> byte())) :: binary()
  def bitop_combine([], _op_fn), do: <<>>

  def bitop_combine([first | rest], op_fn) do
    Enum.reduce(rest, first, fn bin, acc ->
      combine_binaries(acc, bin, op_fn)
    end)
  end

  defp byte_popcount(byte) do
    do_byte_popcount(byte, 0)
  end

  defp do_byte_popcount(0, count), do: count

  defp do_byte_popcount(byte, count) do
    do_byte_popcount(byte &&& byte - 1, count + 1)
  end

  defp bitpos_not_found_fallback(pos, _bit_val, _end_byte, _explicit_end) when pos >= 0, do: pos

  defp bitpos_not_found_fallback(-1, 0 = _bit_val, end_byte, false = _explicit_end) do
    (end_byte + 1) * 8
  end

  defp bitpos_not_found_fallback(-1, _bit_val, _end_byte, _explicit_end), do: -1

  @spec scan_bytes_for_bit(binary(), 0 | 1, non_neg_integer(), non_neg_integer()) :: integer()
  def scan_bytes_for_bit(bin, bit_val, byte_from, byte_to) do
    skip_byte = if bit_val == 1, do: 0x00, else: 0xFF

    Enum.reduce_while(byte_from..byte_to, -1, fn byte_idx, _acc ->
      byte = :binary.at(bin, byte_idx)

      if byte == skip_byte do
        {:cont, -1}
      else
        {:halt, byte_idx * 8 + first_bit_in_byte(byte, bit_val)}
      end
    end)
  end

  defp first_bit_in_byte(byte, bit_val) do
    Enum.find(0..7, fn bit_pos ->
      (byte >>> (7 - bit_pos) &&& 1) == bit_val
    end)
  end

  defp combine_binaries(<<>>, <<>>, _op_fn), do: <<>>

  defp combine_binaries(a, b, op_fn) do
    len = byte_size(a)

    for i <- 0..(len - 1), into: <<>> do
      <<op_fn.(:binary.at(a, i), :binary.at(b, i))::8>>
    end
  end
end
