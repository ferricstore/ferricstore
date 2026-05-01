defmodule Ferricstore.Commands.Bitmap do
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops

  @moduledoc """
  Handles Redis bitmap commands: SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP.

  Each handler takes the uppercased command name, a list of string arguments,
  and an injected store map. Returns plain Elixir terms — the connection layer
  handles RESP encoding.

  Bitmap commands operate on string values at the bit level. Bits are numbered
  from the most significant bit (MSB) of the first byte: bit 0 is the MSB of
  byte 0 (value 128), bit 7 is the LSB of byte 0 (value 1), bit 8 is the MSB
  of byte 1, and so on. This matches Redis bit ordering.

  Since FerricStore uses an append-only Bitcask storage engine, all write
  operations (SETBIT, BITOP) perform a read-modify-write cycle.

  ## Supported commands

    * `SETBIT key offset value` — set or clear the bit at `offset`; returns old bit
    * `GETBIT key offset` — returns the bit value at `offset`
    * `BITCOUNT key [start end [BYTE|BIT]]` — count set bits in a range
    * `BITPOS key bit [start [end [BYTE|BIT]]]` — find first 0 or 1 bit
    * `BITOP operation destkey key [key ...]` — bitwise AND/OR/XOR/NOT
  """

  import Bitwise

  # Redis limits bit offset to 2^32 - 1 (512MB value max)
  @max_bit_offset 4_294_967_295
  @wrongtype_error {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

  @doc """
  Handles a bitmap command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"SETBIT"`, `"GETBIT"`)
    - `args` - List of string arguments
    - `store` - Injected store map with `get`, `put` callbacks

  ## Returns

  Plain Elixir term: integer, or `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # SETBIT key offset value
  # ---------------------------------------------------------------------------

  def handle("SETBIT", [key, offset_str, bit_str], store) do
    with :ok <- ensure_string_key(key, store),
         {:ok, offset} <- parse_non_negative_integer(offset_str, "bit offset"),
         :ok <- check_bit_offset(offset),
         {:ok, bit_val} <- parse_bit_value(bit_str) do
      {current, expire_at_ms} =
        case Ops.get_meta(store, key) do
          nil -> {<<>>, 0}
          {value, exp} -> {value, exp}
        end

      byte_index = div(offset, 8)
      # Extend the binary with zero bytes if needed
      extended = extend_binary(current, byte_index + 1)

      # Read the old bit
      old_byte = :binary.at(extended, byte_index)
      bit_position = 7 - rem(offset, 8)
      old_bit = old_byte >>> bit_position &&& 1

      # Set the new bit
      new_byte =
        case bit_val do
          1 -> old_byte ||| 1 <<< bit_position
          0 -> old_byte &&& Bitwise.bnot(1 <<< bit_position)
        end

      # Replace the byte in the binary
      <<prefix::binary-size(byte_index), _old::8, suffix::binary>> = extended
      new_value = <<prefix::binary, new_byte::8, suffix::binary>>

      Ops.put(store, key, new_value, expire_at_ms)
      old_bit
    end
  end

  def handle("SETBIT", _args, _store) do
    {:error, "ERR wrong number of arguments for 'setbit' command"}
  end

  # ---------------------------------------------------------------------------
  # GETBIT key offset
  # ---------------------------------------------------------------------------

  def handle("GETBIT", [key, offset_str], store) do
    with :ok <- ensure_string_key(key, store),
         {:ok, offset} <- parse_non_negative_integer(offset_str, "bit offset"),
         :ok <- check_bit_offset(offset) do
      current = Ops.get(store, key) || <<>>
      byte_index = div(offset, 8)

      if byte_index >= byte_size(current) do
        0
      else
        byte = :binary.at(current, byte_index)
        bit_position = 7 - rem(offset, 8)
        byte >>> bit_position &&& 1
      end
    end
  end

  def handle("GETBIT", _args, _store) do
    {:error, "ERR wrong number of arguments for 'getbit' command"}
  end

  # ---------------------------------------------------------------------------
  # BITCOUNT key [start end [BYTE|BIT]]
  # ---------------------------------------------------------------------------

  def handle("BITCOUNT", [key], store) do
    with :ok <- ensure_string_key(key, store) do
      current = Ops.get(store, key) || <<>>
      popcount(current)
    end
  end

  def handle("BITCOUNT", [key, start_str, end_str | rest], store) do
    mode = parse_bitcount_mode(rest)

    with :ok <- ensure_string_key(key, store),
         {:ok, mode} <- mode,
         {:ok, start_idx} <- parse_integer(start_str),
         {:ok, end_idx} <- parse_integer(end_str) do
      current = Ops.get(store, key) || <<>>

      case mode do
        :byte -> bitcount_byte_range(current, start_idx, end_idx)
        :bit -> bitcount_bit_range(current, start_idx, end_idx)
      end
    end
  end

  def handle("BITCOUNT", [], _store) do
    {:error, "ERR wrong number of arguments for 'bitcount' command"}
  end

  def handle("BITCOUNT", [_key, _start], _store) do
    {:error, "ERR syntax error"}
  end

  # ---------------------------------------------------------------------------
  # BITPOS key bit [start [end [BYTE|BIT]]]
  # ---------------------------------------------------------------------------

  def handle("BITPOS", [key, bit_str], store) do
    with :ok <- ensure_string_key(key, store),
         {:ok, bit_val} <- parse_bit_value(bit_str) do
      current = Ops.get(store, key) || <<>>
      bitpos_byte_range(current, bit_val, 0, byte_size(current) - 1, false)
    end
  end

  def handle("BITPOS", [key, bit_str, start_str], store) do
    with :ok <- ensure_string_key(key, store),
         {:ok, bit_val} <- parse_bit_value(bit_str),
         {:ok, start_idx} <- parse_integer(start_str) do
      current = Ops.get(store, key) || <<>>
      len = byte_size(current)
      start_resolved = resolve_index(start_idx, len)
      bitpos_byte_range(current, bit_val, start_resolved, len - 1, false)
    end
  end

  def handle("BITPOS", [key, bit_str, start_str, end_str | rest], store) do
    mode = parse_bitcount_mode(rest)

    with :ok <- ensure_string_key(key, store),
         {:ok, mode} <- mode,
         {:ok, bit_val} <- parse_bit_value(bit_str),
         {:ok, start_idx} <- parse_integer(start_str),
         {:ok, end_idx} <- parse_integer(end_str) do
      current = Ops.get(store, key) || <<>>

      case mode do
        :byte ->
          len = byte_size(current)
          s = resolve_index(start_idx, len)
          e = resolve_index(end_idx, len)
          bitpos_byte_range(current, bit_val, s, e, true)

        :bit ->
          total_bits = byte_size(current) * 8
          s = resolve_index(start_idx, total_bits)
          e = resolve_index(end_idx, total_bits)
          bitpos_bit_range(current, bit_val, s, e)
      end
    end
  end

  def handle("BITPOS", [], _store) do
    {:error, "ERR wrong number of arguments for 'bitpos' command"}
  end

  def handle("BITPOS", [_key], _store) do
    {:error, "ERR wrong number of arguments for 'bitpos' command"}
  end

  # ---------------------------------------------------------------------------
  # BITOP operation destkey key [key ...]
  # ---------------------------------------------------------------------------

  def handle("BITOP", [op_str, destkey | source_keys], store) when source_keys != [] do
    op = String.upcase(op_str)

    with {:ok, result} <- execute_bitop(op, source_keys, store) do
      clear_compound_data_structure(destkey, store)
      Ops.put(store, destkey, result, 0)
      byte_size(result)
    end
  end

  def handle("BITOP", _args, _store) do
    {:error, "ERR wrong number of arguments for 'bitop' command"}
  end

  # ===========================================================================
  # Private helpers
  # ===========================================================================

  # --- Parsing helpers -------------------------------------------------------

  defp check_bit_offset(offset) when offset > @max_bit_offset do
    {:error, "ERR bit offset is not an integer or out of range"}
  end

  defp check_bit_offset(_offset), do: :ok

  @spec parse_non_negative_integer(binary(), binary()) ::
          {:ok, non_neg_integer()} | {:error, binary()}
  defp parse_non_negative_integer(str, label) do
    case Integer.parse(str) do
      {n, ""} when n >= 0 -> {:ok, n}
      {_n, ""} -> {:error, "ERR #{label} is not an integer or out of range"}
      _ -> {:error, "ERR #{label} is not an integer or out of range"}
    end
  end

  @spec parse_integer(binary()) :: {:ok, integer()} | {:error, binary()}
  defp parse_integer(str) do
    case Integer.parse(str) do
      {n, ""} -> {:ok, n}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  @spec parse_bit_value(binary()) :: {:ok, 0 | 1} | {:error, binary()}
  defp parse_bit_value("0"), do: {:ok, 0}
  defp parse_bit_value("1"), do: {:ok, 1}

  defp parse_bit_value(_) do
    {:error, "ERR bit is not an integer or out of range"}
  end

  @spec parse_bitcount_mode([binary()]) :: {:ok, :byte | :bit} | {:error, binary()}
  defp parse_bitcount_mode([]), do: {:ok, :byte}

  defp parse_bitcount_mode([mode_str]) do
    case String.upcase(mode_str) do
      "BYTE" -> {:ok, :byte}
      "BIT" -> {:ok, :bit}
      _ -> {:error, "ERR syntax error"}
    end
  end

  defp parse_bitcount_mode(_), do: {:error, "ERR syntax error"}

  # --- Type guards ----------------------------------------------------------

  defp ensure_string_key(key, store) do
    if compound_data_structure_key?(key, store), do: @wrongtype_error, else: :ok
  end

  defp compound_data_structure_key?(key, store) do
    Ops.has_compound?(store) and
      (Ops.compound_get(store, key, CompoundKey.type_key(key)) != nil or
         Ops.compound_get(store, key, CompoundKey.list_meta_key(key)) != nil)
  end

  defp clear_compound_data_structure(key, store) do
    if Ops.has_compound?(store) do
      Ops.compound_delete(store, key, CompoundKey.type_key(key))
      Ops.compound_delete(store, key, CompoundKey.list_meta_key(key))

      for prefix <- [
            CompoundKey.hash_prefix(key),
            CompoundKey.list_prefix(key),
            CompoundKey.set_prefix(key),
            CompoundKey.zset_prefix(key)
          ] do
        Ops.compound_delete_prefix(store, key, prefix)
      end
    end

    :ok
  end

  # --- Binary manipulation --------------------------------------------------

  @spec extend_binary(binary(), non_neg_integer()) :: binary()
  defp extend_binary(bin, min_size) when byte_size(bin) >= min_size, do: bin

  defp extend_binary(bin, min_size) do
    padding_size = min_size - byte_size(bin)
    <<bin::binary, 0::size(padding_size * 8)>>
  end

  @spec pad_binary(binary(), non_neg_integer()) :: binary()
  defp pad_binary(bin, target_size) when byte_size(bin) >= target_size, do: bin

  defp pad_binary(bin, target_size) do
    padding = target_size - byte_size(bin)
    <<bin::binary, 0::size(padding * 8)>>
  end

  # --- Popcount (count set bits) ---------------------------------------------

  @spec popcount(binary()) :: non_neg_integer()
  defp popcount(<<>>), do: 0

  defp popcount(binary) do
    for <<byte::8 <- binary>>, reduce: 0 do
      acc -> acc + byte_popcount(byte)
    end
  end

  @spec byte_popcount(byte()) :: non_neg_integer()
  defp byte_popcount(byte) do
    # Kernighan's bit counting: clear lowest set bit each iteration
    do_byte_popcount(byte, 0)
  end

  defp do_byte_popcount(0, count), do: count

  defp do_byte_popcount(byte, count) do
    do_byte_popcount(byte &&& byte - 1, count + 1)
  end

  # --- BITCOUNT with byte range ----------------------------------------------

  @spec bitcount_byte_range(binary(), integer(), integer()) :: non_neg_integer()
  defp bitcount_byte_range(<<>>, _start, _stop), do: 0

  defp bitcount_byte_range(bin, start_idx, end_idx) do
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

  # --- BITCOUNT with bit range -----------------------------------------------

  @spec bitcount_bit_range(binary(), integer(), integer()) :: non_neg_integer()
  defp bitcount_bit_range(<<>>, _start, _stop), do: 0

  defp bitcount_bit_range(bin, start_idx, end_idx) do
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

  # --- Index resolution (supports negative indexing) -------------------------

  @spec resolve_index(integer(), non_neg_integer()) :: integer()
  defp resolve_index(idx, _len) when idx >= 0, do: idx
  defp resolve_index(idx, len), do: len + idx

  # --- BITPOS helpers --------------------------------------------------------

  @spec bitpos_byte_range(binary(), 0 | 1, integer(), integer(), boolean()) :: integer()
  defp bitpos_byte_range(<<>>, 1, _start, _stop, _explicit_end), do: -1
  defp bitpos_byte_range(<<>>, 0, _start, _stop, _explicit_end), do: 0

  defp bitpos_byte_range(bin, bit_val, start_byte, end_byte, explicit_end) do
    len = byte_size(bin)
    s = max(start_byte, 0)
    e = min(end_byte, len - 1)

    if s > e or s >= len do
      # Redis: when searching for 0 without explicit end, virtual bits past
      # the string are all zeros. Return length * 8 as the first virtual 0.
      if bit_val == 0 and not explicit_end, do: len * 8, else: -1
    else
      bin
      |> scan_bytes_for_bit(bit_val, s, e)
      |> bitpos_not_found_fallback(bit_val, e, explicit_end)
    end
  end

  # When the scan found a bit, return its position.
  defp bitpos_not_found_fallback(pos, _bit_val, _end_byte, _explicit_end) when pos >= 0, do: pos

  # When looking for 0 without an explicit end range, Redis considers the
  # first bit past the string as a virtual 0 bit.
  defp bitpos_not_found_fallback(-1, 0 = _bit_val, end_byte, false = _explicit_end) do
    (end_byte + 1) * 8
  end

  defp bitpos_not_found_fallback(-1, _bit_val, _end_byte, _explicit_end), do: -1

  @spec bitpos_bit_range(binary(), 0 | 1, integer(), integer()) :: integer()
  defp bitpos_bit_range(bin, bit_val, start_bit, end_bit) do
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

  @spec scan_bytes_for_bit(binary(), 0 | 1, non_neg_integer(), non_neg_integer()) :: integer()
  defp scan_bytes_for_bit(bin, bit_val, byte_from, byte_to) do
    # Determine which byte value means "all target bits absent"
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

  # Finds the position (0-7) of the first bit matching `bit_val` within a byte.
  @spec first_bit_in_byte(byte(), 0 | 1) :: 0..7
  defp first_bit_in_byte(byte, bit_val) do
    Enum.find(0..7, fn bit_pos ->
      (byte >>> (7 - bit_pos) &&& 1) == bit_val
    end)
  end

  # --- BITOP dispatch --------------------------------------------------------

  @spec execute_bitop(binary(), [binary()], map()) :: {:ok, binary()} | {:error, binary()}
  defp execute_bitop("NOT", [src_key], store) do
    with {:ok, src} <- read_source(src_key, store) do
      {:ok, bitop_not(src)}
    end
  end

  defp execute_bitop("NOT", _keys, _store) do
    {:error, "ERR BITOP NOT requires one and only one key"}
  end

  defp execute_bitop(op, source_keys, store) when op in ~w(AND OR XOR) do
    with {:ok, values} <- read_sources(source_keys, store) do
      max_len = values |> Enum.map(&byte_size/1) |> Enum.max()
      padded = Enum.map(values, &pad_binary(&1, max_len))

      result =
        case op do
          "AND" -> bitop_combine(padded, &Bitwise.band/2)
          "OR" -> bitop_combine(padded, &Bitwise.bor/2)
          "XOR" -> bitop_combine(padded, &Bitwise.bxor/2)
        end

      {:ok, result}
    end
  end

  defp execute_bitop(_op, _keys, _store), do: {:error, "ERR syntax error"}

  defp read_sources(source_keys, store) do
    Enum.reduce_while(source_keys, {:ok, []}, fn key, {:ok, acc} ->
      case read_source(key, store) do
        {:ok, value} -> {:cont, {:ok, [value | acc]}}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      {:error, _} = err -> err
    end
  end

  defp read_source(key, store) do
    case ensure_string_key(key, store) do
      :ok -> {:ok, Ops.get(store, key) || <<>>}
      @wrongtype_error -> @wrongtype_error
    end
  end

  # --- BITOP helpers ---------------------------------------------------------

  @spec bitop_not(binary()) :: binary()
  defp bitop_not(bin) do
    for <<byte::8 <- bin>>, into: <<>> do
      <<Bitwise.bnot(byte) &&& 0xFF::8>>
    end
  end

  @spec bitop_combine([binary()], (byte(), byte() -> byte())) :: binary()
  defp bitop_combine([], _op_fn), do: <<>>

  defp bitop_combine([first | rest], op_fn) do
    Enum.reduce(rest, first, fn bin, acc ->
      combine_binaries(acc, bin, op_fn)
    end)
  end

  @spec combine_binaries(binary(), binary(), (byte(), byte() -> byte())) :: binary()
  defp combine_binaries(<<>>, <<>>, _op_fn), do: <<>>

  defp combine_binaries(a, b, op_fn) do
    len = byte_size(a)

    for i <- 0..(len - 1), into: <<>> do
      <<op_fn.(:binary.at(a, i), :binary.at(b, i))::8>>
    end
  end
end
