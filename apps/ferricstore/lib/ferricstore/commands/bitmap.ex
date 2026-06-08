defmodule Ferricstore.Commands.Bitmap do
  alias Ferricstore.Commands.Bitmap.Args
  alias Ferricstore.Commands.Bitmap.Bits
  alias Ferricstore.Commands.Bitmap.Destination
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

  @wrongtype_error {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
  @bitcount_chunk_bytes 64 * 1024

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
    with :ok <- Destination.ensure_string_key(key, store),
         {:ok, offset} <- Args.parse_non_negative_integer(offset_str, "bit offset"),
         :ok <- Args.check_bit_offset(offset),
         {:ok, bit_val} <- Args.parse_bit_value(bit_str) do
      case setbit_noop_from_store(store, key, offset, bit_val) do
        {:ok, old_bit} -> old_bit
        :unknown -> setbit_rewrite(key, offset, bit_val, store)
      end
    end
  end

  def handle("SETBIT", _args, _store) do
    {:error, "ERR wrong number of arguments for 'setbit' command"}
  end

  # ---------------------------------------------------------------------------
  # GETBIT key offset
  # ---------------------------------------------------------------------------

  def handle("GETBIT", [key, offset_str], store) do
    with :ok <- Destination.ensure_string_key(key, store),
         {:ok, offset} <- Args.parse_non_negative_integer(offset_str, "bit offset"),
         :ok <- Args.check_bit_offset(offset) do
      byte_index = div(offset, 8)

      if byte_index_outside_value?(store, key, byte_index) do
        0
      else
        case byte_at(store, key, byte_index) do
          nil ->
            0

          byte ->
            bit_position = 7 - rem(offset, 8)
            byte >>> bit_position &&& 1
        end
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
    with :ok <- Destination.ensure_string_key(key, store) do
      case bitcount_all_from_store(store, key) do
        {:ok, count} ->
          count

        :unknown ->
          current = Ops.get(store, key) || <<>>
          Bits.popcount(current)
      end
    end
  end

  def handle("BITCOUNT", [key, start_str, end_str | rest], store) do
    mode = Args.parse_bitcount_mode(rest)

    with :ok <- Destination.ensure_string_key(key, store),
         {:ok, mode} <- mode,
         {:ok, start_idx} <- Args.parse_integer(start_str),
         {:ok, end_idx} <- Args.parse_integer(end_str) do
      if bitcount_range_empty_without_value?(store, key, mode, start_idx, end_idx) do
        0
      else
        case bitcount_range_from_store(store, key, mode, start_idx, end_idx) do
          {:ok, count} ->
            count

          :unknown ->
            current = Ops.get(store, key) || <<>>

            case mode do
              :byte -> Bits.bitcount_byte_range(current, start_idx, end_idx)
              :bit -> Bits.bitcount_bit_range(current, start_idx, end_idx)
            end
        end
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
    with :ok <- Destination.ensure_string_key(key, store),
         {:ok, bit_val} <- Args.parse_bit_value(bit_str) do
      case bitpos_all_from_store(store, key, bit_val) do
        {:ok, pos} ->
          pos

        :unknown ->
          current = Ops.get(store, key) || <<>>
          Bits.bitpos_byte_range(current, bit_val, 0, byte_size(current) - 1, false)
      end
    end
  end

  def handle("BITPOS", [key, bit_str, start_str], store) do
    with :ok <- Destination.ensure_string_key(key, store),
         {:ok, bit_val} <- Args.parse_bit_value(bit_str),
         {:ok, start_idx} <- Args.parse_integer(start_str) do
      case bitpos_byte_range_from_size(store, key, bit_val, start_idx, nil, false) do
        {:ok, result} ->
          result

        :unknown ->
          case bitpos_byte_range_from_store(store, key, bit_val, start_idx, nil, false) do
            {:ok, result} ->
              result

            :unknown ->
              current = Ops.get(store, key) || <<>>
              len = byte_size(current)
              start_resolved = Bits.resolve_index(start_idx, len)
              Bits.bitpos_byte_range(current, bit_val, start_resolved, len - 1, false)
          end
      end
    end
  end

  def handle("BITPOS", [key, bit_str, start_str, end_str | rest], store) do
    mode = Args.parse_bitcount_mode(rest)

    with :ok <- Destination.ensure_string_key(key, store),
         {:ok, mode} <- mode,
         {:ok, bit_val} <- Args.parse_bit_value(bit_str),
         {:ok, start_idx} <- Args.parse_integer(start_str),
         {:ok, end_idx} <- Args.parse_integer(end_str) do
      case mode do
        :byte ->
          case bitpos_byte_range_from_size(store, key, bit_val, start_idx, end_idx, true) do
            {:ok, result} ->
              result

            :unknown ->
              case bitpos_byte_range_from_store(store, key, bit_val, start_idx, end_idx, true) do
                {:ok, result} ->
                  result

                :unknown ->
                  current = Ops.get(store, key) || <<>>
                  len = byte_size(current)
                  s = Bits.resolve_index(start_idx, len)
                  e = Bits.resolve_index(end_idx, len)
                  Bits.bitpos_byte_range(current, bit_val, s, e, true)
              end
          end

        :bit ->
          case bitpos_bit_range_from_size(store, key, start_idx, end_idx) do
            {:ok, result} ->
              result

            :unknown ->
              case bitpos_bit_range_from_store(store, key, bit_val, start_idx, end_idx) do
                {:ok, result} ->
                  result

                :unknown ->
                  current = Ops.get(store, key) || <<>>
                  total_bits = byte_size(current) * 8
                  s = Bits.resolve_index(start_idx, total_bits)
                  e = Bits.resolve_index(end_idx, total_bits)
                  Bits.bitpos_bit_range(current, bit_val, s, e)
              end
          end
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
      write_bitop_result(store, destkey, result)
    end
  end

  def handle("BITOP", _args, _store) do
    {:error, "ERR wrong number of arguments for 'bitop' command"}
  end

  @spec handle_ast(term(), map()) :: term()
  def handle_ast(ast, store)

  def handle_ast({:setbit, _key, {:error, reason}, _bit}, _store), do: {:error, reason}
  def handle_ast({:setbit, _key, _offset, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:getbit, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:bitcount, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:bitpos, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:bitpos, _key, _bit, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:bitop, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:setbit, key, offset, bit_val}, store)
      when is_integer(offset) and offset >= 0 and bit_val in [0, 1] do
    with :ok <- Destination.ensure_string_key(key, store),
         :ok <- Args.check_bit_offset(offset) do
      case setbit_noop_from_store(store, key, offset, bit_val) do
        {:ok, old_bit} -> old_bit
        :unknown -> setbit_rewrite(key, offset, bit_val, store)
      end
    end
  end

  def handle_ast({:getbit, key, offset}, store) when is_integer(offset) and offset >= 0 do
    with :ok <- Destination.ensure_string_key(key, store),
         :ok <- Args.check_bit_offset(offset) do
      byte_index = div(offset, 8)

      if byte_index_outside_value?(store, key, byte_index) do
        0
      else
        case byte_at(store, key, byte_index) do
          nil ->
            0

          byte ->
            bit_position = 7 - rem(offset, 8)
            byte >>> bit_position &&& 1
        end
      end
    end
  end

  def handle_ast({:bitcount, key}, store) do
    with :ok <- Destination.ensure_string_key(key, store) do
      case bitcount_all_from_store(store, key) do
        {:ok, count} ->
          count

        :unknown ->
          current = Ops.get(store, key) || <<>>
          Bits.popcount(current)
      end
    end
  end

  def handle_ast({:bitcount, key, {start_idx, end_idx, mode}}, store)
      when is_integer(start_idx) and is_integer(end_idx) and mode in [:byte, :bit] do
    with :ok <- Destination.ensure_string_key(key, store) do
      if bitcount_range_empty_without_value?(store, key, mode, start_idx, end_idx) do
        0
      else
        case bitcount_range_from_store(store, key, mode, start_idx, end_idx) do
          {:ok, count} ->
            count

          :unknown ->
            current = Ops.get(store, key) || <<>>

            case mode do
              :byte -> Bits.bitcount_byte_range(current, start_idx, end_idx)
              :bit -> Bits.bitcount_bit_range(current, start_idx, end_idx)
            end
        end
      end
    end
  end

  def handle_ast({:bitpos, key, bit_val, :all}, store) when bit_val in [0, 1] do
    with :ok <- Destination.ensure_string_key(key, store) do
      case bitpos_all_from_store(store, key, bit_val) do
        {:ok, pos} ->
          pos

        :unknown ->
          current = Ops.get(store, key) || <<>>
          Bits.bitpos_byte_range(current, bit_val, 0, byte_size(current) - 1, false)
      end
    end
  end

  def handle_ast({:bitpos, key, bit_val, {:start, start_idx}}, store)
      when bit_val in [0, 1] and is_integer(start_idx) do
    with :ok <- Destination.ensure_string_key(key, store) do
      case bitpos_byte_range_from_size(store, key, bit_val, start_idx, nil, false) do
        {:ok, result} ->
          result

        :unknown ->
          case bitpos_byte_range_from_store(store, key, bit_val, start_idx, nil, false) do
            {:ok, result} ->
              result

            :unknown ->
              current = Ops.get(store, key) || <<>>
              len = byte_size(current)
              start_resolved = Bits.resolve_index(start_idx, len)
              Bits.bitpos_byte_range(current, bit_val, start_resolved, len - 1, false)
          end
      end
    end
  end

  def handle_ast({:bitpos, key, bit_val, {start_idx, end_idx, mode}}, store)
      when bit_val in [0, 1] and is_integer(start_idx) and is_integer(end_idx) and
             mode in [:byte, :bit] do
    with :ok <- Destination.ensure_string_key(key, store) do
      case mode do
        :byte ->
          case bitpos_byte_range_from_size(store, key, bit_val, start_idx, end_idx, true) do
            {:ok, result} ->
              result

            :unknown ->
              case bitpos_byte_range_from_store(store, key, bit_val, start_idx, end_idx, true) do
                {:ok, result} ->
                  result

                :unknown ->
                  current = Ops.get(store, key) || <<>>
                  len = byte_size(current)
                  s = Bits.resolve_index(start_idx, len)
                  e = Bits.resolve_index(end_idx, len)
                  Bits.bitpos_byte_range(current, bit_val, s, e, true)
              end
          end

        :bit ->
          case bitpos_bit_range_from_size(store, key, start_idx, end_idx) do
            {:ok, result} ->
              result

            :unknown ->
              case bitpos_bit_range_from_store(store, key, bit_val, start_idx, end_idx) do
                {:ok, result} ->
                  result

                :unknown ->
                  current = Ops.get(store, key) || <<>>
                  total_bits = byte_size(current) * 8
                  s = Bits.resolve_index(start_idx, total_bits)
                  e = Bits.resolve_index(end_idx, total_bits)
                  Bits.bitpos_bit_range(current, bit_val, s, e)
              end
          end
      end
    end
  end

  def handle_ast({:bitop, op, destkey, source_keys}, store)
      when op in [:band, :bor, :bxor, :bnot] and is_list(source_keys) and source_keys != [] do
    with {:ok, result} <- execute_bitop_ast(op, source_keys, store) do
      write_bitop_result(store, destkey, result)
    end
  end

  def handle_ast(_ast, _store), do: {:error, "ERR unsupported bitmap command AST"}

  # ===========================================================================
  # Private helpers
  # ===========================================================================

  defp byte_index_outside_value?(store, key, byte_index) do
    case Destination.metadata_value_size(store, key) do
      size when is_integer(size) -> byte_index >= size
      _ -> false
    end
  end

  defp byte_at(store, key, byte_index) do
    case Ops.getrange(store, key, byte_index, byte_index) do
      <<byte>> -> byte
      _ -> nil
    end
  end

  defp setbit_noop_from_store(store, key, offset, bit_val) do
    byte_index = div(offset, 8)

    with size when is_integer(size) and byte_index < size <-
           setbit_noop_candidate_size(store, key),
         byte when is_integer(byte) <- byte_at(store, key, byte_index) do
      bit_position = 7 - rem(offset, 8)
      old_bit = byte >>> bit_position &&& 1

      if old_bit == bit_val, do: {:ok, old_bit}, else: :unknown
    else
      _ -> :unknown
    end
  end

  defp setbit_noop_candidate_size(%FerricStore.Instance{} = store, key) do
    case Ferricstore.Store.Router.get_keydir_file_ref(store, key) do
      {:ok, {_fid, _offset, value_size}} -> value_size
      :miss -> :unknown
    end
  end

  defp setbit_noop_candidate_size(%{value_size: value_size}, key) when is_function(value_size, 1),
    do: value_size.(key)

  defp setbit_noop_candidate_size(_store, _key), do: :unknown

  defp setbit_rewrite(key, offset, bit_val, store) do
    {current, expire_at_ms} =
      case Ops.get_meta(store, key) do
        nil -> {<<>>, 0}
        {value, exp} -> {value, exp}
      end

    byte_index = div(offset, 8)
    # Extend the binary with zero bytes if needed
    extended = Bits.extend_binary(current, byte_index + 1)

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

    write_setbit_result(store, key, new_value, expire_at_ms, old_bit)
  end

  defp write_setbit_result(store, key, value, expire_at_ms, old_bit) do
    case Ops.put(store, key, value, expire_at_ms) do
      :ok -> old_bit
      {:error, _} = error -> error
    end
  end

  defp write_bitop_result(store, destkey, result) do
    case Destination.bitop_compound_destination_type(destkey, store) do
      nil ->
        case Ops.put(store, destkey, result, 0) do
          :ok -> byte_size(result)
          {:error, _} = error -> error
        end

      type ->
        backup = Destination.compound_destination_backup(destkey, type, store)

        case Destination.clear_compound_data_structure(destkey, store) do
          :ok ->
            case Ops.put(store, destkey, result, 0) do
              :ok ->
                byte_size(result)

              {:error, _} = error ->
                Destination.restore_bitop_destination(store, destkey, backup, error)
            end

          {:error, _} = error ->
            Destination.restore_bitop_destination(store, destkey, backup, error)
        end
    end
  end

  defp bitcount_range_empty_without_value?(_store, _key, _mode, start_idx, end_idx)
       when start_idx >= 0 and end_idx >= 0 and start_idx > end_idx,
       do: true

  defp bitcount_range_empty_without_value?(store, key, :byte, start_idx, _end_idx)
       when start_idx >= 0 do
    case Destination.metadata_value_size(store, key) do
      size when is_integer(size) -> start_idx >= size
      _ -> false
    end
  end

  defp bitcount_range_empty_without_value?(store, key, :bit, start_idx, _end_idx)
       when start_idx >= 0 do
    case Destination.metadata_value_size(store, key) do
      size when is_integer(size) -> start_idx >= size * 8
      _ -> false
    end
  end

  defp bitcount_range_empty_without_value?(_store, _key, _mode, _start_idx, _end_idx), do: false

  defp bitcount_all_from_store(store, key) do
    case Destination.metadata_value_size(store, key) do
      0 ->
        {:ok, 0}

      size when is_integer(size) and size > 0 ->
        bitcount_all_chunks(store, key, size, 0, 0)

      _unknown_or_missing ->
        :unknown
    end
  end

  defp bitcount_all_chunks(_store, _key, size, offset, acc) when offset >= size,
    do: {:ok, acc}

  defp bitcount_all_chunks(store, key, size, offset, acc) do
    last = min(offset + @bitcount_chunk_bytes - 1, size - 1)
    expected_size = last - offset + 1

    case Ops.getrange(store, key, offset, last) do
      slice when is_binary(slice) and byte_size(slice) == expected_size ->
        bitcount_all_chunks(store, key, size, last + 1, acc + Bits.popcount(slice))

      _missing_or_short ->
        :unknown
    end
  end

  defp bitpos_all_from_store(store, key, bit_val) do
    case Destination.metadata_value_size(store, key) do
      0 when bit_val == 0 ->
        {:ok, 0}

      0 ->
        {:ok, -1}

      size when is_integer(size) and size > 0 ->
        bitpos_all_chunks(store, key, bit_val, size, 0)

      _unknown_or_missing ->
        :unknown
    end
  end

  defp bitpos_all_chunks(_store, _key, 0, size, offset) when offset >= size,
    do: {:ok, size * 8}

  defp bitpos_all_chunks(_store, _key, 1, size, offset) when offset >= size,
    do: {:ok, -1}

  defp bitpos_all_chunks(store, key, bit_val, size, offset) do
    last = min(offset + @bitcount_chunk_bytes - 1, size - 1)
    expected_size = last - offset + 1

    case Ops.getrange(store, key, offset, last) do
      slice when is_binary(slice) and byte_size(slice) == expected_size ->
        case Bits.scan_bytes_for_bit(slice, bit_val, 0, byte_size(slice) - 1) do
          pos when pos >= 0 -> {:ok, offset * 8 + pos}
          -1 -> bitpos_all_chunks(store, key, bit_val, size, last + 1)
        end

      _missing_or_short ->
        :unknown
    end
  end

  defp bitcount_range_from_store(store, key, :byte, start_idx, end_idx) do
    with size when is_integer(size) <- Destination.metadata_value_size(store, key),
         {:ok, start_byte, end_byte} <- resolve_range(start_idx, end_idx, size) do
      bitcount_byte_range_chunks(store, key, start_byte, end_byte, 0)
    else
      :empty -> {:ok, 0}
      _ -> :unknown
    end
  end

  defp bitcount_range_from_store(store, key, :bit, start_idx, end_idx) do
    with size when is_integer(size) <- Destination.metadata_value_size(store, key),
         total_bits = size * 8,
         {:ok, start_bit, end_bit} <- resolve_range(start_idx, end_idx, total_bits) do
      start_byte = div(start_bit, 8)
      end_byte = div(end_bit, 8)
      bitcount_bit_range_chunks(store, key, start_byte, end_byte, start_bit, end_bit, 0)
    else
      :empty -> {:ok, 0}
      _ -> :unknown
    end
  end

  defp bitcount_bit_range_chunks(
         _store,
         _key,
         offset,
         end_byte,
         _start_bit,
         _end_bit,
         acc
       )
       when offset > end_byte,
       do: {:ok, acc}

  defp bitcount_bit_range_chunks(store, key, offset, end_byte, start_bit, end_bit, acc) do
    last = min(offset + @bitcount_chunk_bytes - 1, end_byte)
    expected_size = last - offset + 1

    case Ops.getrange(store, key, offset, last) do
      slice when is_binary(slice) and byte_size(slice) == expected_size ->
        chunk_start_bit = offset * 8
        local_start_bit = max(start_bit - chunk_start_bit, 0)
        local_end_bit = min(end_bit - chunk_start_bit, byte_size(slice) * 8 - 1)
        count = Bits.bitcount_bit_range(slice, local_start_bit, local_end_bit)

        bitcount_bit_range_chunks(
          store,
          key,
          last + 1,
          end_byte,
          start_bit,
          end_bit,
          acc + count
        )

      _missing_or_short ->
        :unknown
    end
  end

  defp bitcount_byte_range_chunks(_store, _key, offset, end_byte, acc) when offset > end_byte,
    do: {:ok, acc}

  defp bitcount_byte_range_chunks(store, key, offset, end_byte, acc) do
    last = min(offset + @bitcount_chunk_bytes - 1, end_byte)
    expected_size = last - offset + 1

    case Ops.getrange(store, key, offset, last) do
      slice when is_binary(slice) and byte_size(slice) == expected_size ->
        bitcount_byte_range_chunks(store, key, last + 1, end_byte, acc + Bits.popcount(slice))

      _missing_or_short ->
        :unknown
    end
  end

  defp resolve_range(_start_idx, _end_idx, len) when len <= 0, do: :empty

  defp resolve_range(start_idx, end_idx, len) do
    start_resolved = Bits.resolve_index(start_idx, len)
    end_resolved = Bits.resolve_index(end_idx, len)

    if start_resolved > end_resolved or start_resolved >= len or end_resolved < 0 do
      :empty
    else
      {:ok, max(start_resolved, 0), min(end_resolved, len - 1)}
    end
  end

  defp bitpos_byte_range_from_size(store, key, bit_val, start_idx, end_idx, explicit_end) do
    case Destination.metadata_value_size(store, key) do
      size when is_integer(size) ->
        start_resolved = Bits.resolve_index(start_idx, size)
        end_resolved = if end_idx == nil, do: size - 1, else: Bits.resolve_index(end_idx, size)
        start_byte = max(start_resolved, 0)
        end_byte = min(end_resolved, size - 1)

        cond do
          start_byte > end_byte or start_byte >= size ->
            {:ok, bitpos_empty_byte_range_result(bit_val, size, explicit_end)}

          true ->
            :unknown
        end

      _ ->
        :unknown
    end
  end

  defp bitpos_empty_byte_range_result(0, size, false), do: size * 8
  defp bitpos_empty_byte_range_result(_bit_val, _size, _explicit_end), do: -1

  defp bitpos_bit_range_from_size(store, key, start_idx, end_idx) do
    case Destination.metadata_value_size(store, key) do
      size when is_integer(size) ->
        total_bits = size * 8
        start_resolved = Bits.resolve_index(start_idx, total_bits)
        end_resolved = Bits.resolve_index(end_idx, total_bits)
        start_bit = max(start_resolved, 0)
        end_bit = min(end_resolved, total_bits - 1)

        if start_bit > end_bit or start_bit >= total_bits do
          {:ok, -1}
        else
          :unknown
        end

      _ ->
        :unknown
    end
  end

  defp bitpos_byte_range_from_store(store, key, bit_val, start_idx, end_idx, explicit_end) do
    with size when is_integer(size) <- Destination.metadata_value_size(store, key),
         end_idx <- if(end_idx == nil, do: size - 1, else: end_idx),
         {:ok, start_byte, end_byte} <- resolve_range(start_idx, end_idx, size) do
      bitpos_byte_range_chunks(store, key, bit_val, start_byte, end_byte, size, explicit_end)
    else
      :empty ->
        case Destination.metadata_value_size(store, key) do
          size when is_integer(size) ->
            {:ok, bitpos_empty_byte_range_result(bit_val, size, explicit_end)}

          _ ->
            :unknown
        end

      _ ->
        :unknown
    end
  end

  defp bitpos_byte_range_chunks(_store, _key, bit_val, offset, end_byte, size, explicit_end)
       when offset > end_byte do
    {:ok, bitpos_empty_byte_range_result(bit_val, size, explicit_end)}
  end

  defp bitpos_byte_range_chunks(store, key, bit_val, offset, end_byte, size, explicit_end) do
    last = min(offset + @bitcount_chunk_bytes - 1, end_byte)
    expected_size = last - offset + 1

    case Ops.getrange(store, key, offset, last) do
      slice when is_binary(slice) and byte_size(slice) == expected_size ->
        case Bits.scan_bytes_for_bit(slice, bit_val, 0, byte_size(slice) - 1) do
          pos when pos >= 0 ->
            {:ok, offset * 8 + pos}

          -1 ->
            bitpos_byte_range_chunks(
              store,
              key,
              bit_val,
              last + 1,
              end_byte,
              size,
              explicit_end
            )
        end

      _missing_or_short ->
        :unknown
    end
  end

  defp bitpos_bit_range_from_store(store, key, bit_val, start_idx, end_idx) do
    with size when is_integer(size) <- Destination.metadata_value_size(store, key),
         total_bits = size * 8,
         {:ok, start_bit, end_bit} <- resolve_range(start_idx, end_idx, total_bits) do
      start_byte = div(start_bit, 8)
      end_byte = div(end_bit, 8)
      bitpos_bit_range_chunks(store, key, bit_val, start_byte, end_byte, start_bit, end_bit)
    else
      :empty -> {:ok, -1}
      _ -> :unknown
    end
  end

  defp bitpos_bit_range_chunks(_store, _key, _bit_val, offset, end_byte, _start_bit, _end_bit)
       when offset > end_byte,
       do: {:ok, -1}

  defp bitpos_bit_range_chunks(store, key, bit_val, offset, end_byte, start_bit, end_bit) do
    last = min(offset + @bitcount_chunk_bytes - 1, end_byte)
    expected_size = last - offset + 1

    case Ops.getrange(store, key, offset, last) do
      slice when is_binary(slice) and byte_size(slice) == expected_size ->
        chunk_start_bit = offset * 8
        local_start_bit = max(start_bit - chunk_start_bit, 0)
        local_end_bit = min(end_bit - chunk_start_bit, byte_size(slice) * 8 - 1)

        case Bits.bitpos_bit_range(slice, bit_val, local_start_bit, local_end_bit) do
          pos when pos >= 0 ->
            {:ok, chunk_start_bit + pos}

          -1 ->
            bitpos_bit_range_chunks(store, key, bit_val, last + 1, end_byte, start_bit, end_bit)
        end

      _missing_or_short ->
        :unknown
    end
  end

  # --- BITOP dispatch --------------------------------------------------------

  @spec execute_bitop(binary(), [binary()], map()) :: {:ok, binary()} | {:error, binary()}
  defp execute_bitop("NOT", [src_key], store) do
    with {:ok, src} <- read_source(src_key, store) do
      {:ok, Bits.bitop_not(src)}
    end
  end

  defp execute_bitop("NOT", _keys, _store) do
    {:error, "ERR BITOP NOT requires one and only one key"}
  end

  defp execute_bitop("AND", source_keys, store) do
    with :ok <- ensure_string_keys(source_keys, store) do
      case and_zero_result_from_missing_source(source_keys, store) do
        {:ok, result} -> {:ok, result}
        :unknown -> read_sources_unchecked(source_keys, store, "AND")
      end
    end
  end

  defp execute_bitop(op, source_keys, store) when op in ~w(OR XOR) do
    with {:ok, values} <- read_sources(source_keys, store) do
      combine_bitop_sources(op, values)
    end
  end

  defp execute_bitop(_op, _keys, _store), do: {:error, "ERR syntax error"}

  defp execute_bitop_ast(:bnot, [src_key], store) do
    with {:ok, src} <- read_source(src_key, store) do
      {:ok, Bits.bitop_not(src)}
    end
  end

  defp execute_bitop_ast(:bnot, _keys, _store) do
    {:error, "ERR BITOP NOT requires one and only one key"}
  end

  defp execute_bitop_ast(:band, source_keys, store) do
    with :ok <- ensure_string_keys(source_keys, store) do
      case and_zero_result_from_missing_source(source_keys, store) do
        {:ok, result} -> {:ok, result}
        :unknown -> read_sources_unchecked(source_keys, store, "AND")
      end
    end
  end

  defp execute_bitop_ast(op, source_keys, store) when op in [:bor, :bxor] do
    with {:ok, values} <- read_sources(source_keys, store) do
      combine_bitop_sources(op, values)
    end
  end

  defp execute_bitop_ast(_op, _keys, _store), do: {:error, "ERR syntax error"}

  defp read_sources(source_keys, store) do
    with :ok <- ensure_string_keys(source_keys, store) do
      values_from_batch_get(source_keys, store)
    end
  end

  defp read_sources_unchecked(source_keys, store, op) do
    {:ok, values} = values_from_batch_get(source_keys, store)
    combine_bitop_sources(op, values)
  end

  defp values_from_batch_get(source_keys, store) do
    values =
      store
      |> Ops.batch_get(source_keys)
      |> Enum.map(fn
        nil -> <<>>
        value -> value
      end)

    {:ok, values}
  end

  defp combine_bitop_sources(op, values) do
    max_len = values |> Enum.map(&byte_size/1) |> Enum.max()
    padded = Enum.map(values, &Bits.pad_binary(&1, max_len))

    result =
      case op do
        "AND" -> Bits.bitop_combine(padded, &Bitwise.band/2)
        "OR" -> Bits.bitop_combine(padded, &Bitwise.bor/2)
        "XOR" -> Bits.bitop_combine(padded, &Bitwise.bxor/2)
        :bor -> Bits.bitop_combine(padded, &Bitwise.bor/2)
        :bxor -> Bits.bitop_combine(padded, &Bitwise.bxor/2)
      end

    {:ok, result}
  end

  defp and_zero_result_from_missing_source(source_keys, store) do
    case bitop_source_sizes(source_keys, store) do
      :unknown ->
        :unknown

      sizes ->
        if Enum.any?(sizes, &is_nil/1) do
          max_size =
            sizes
            |> Enum.reject(&is_nil/1)
            |> Enum.max(fn -> 0 end)

          {:ok, :binary.copy(<<0>>, max_size)}
        else
          :unknown
        end
    end
  end

  defp bitop_source_sizes(source_keys, store) do
    Enum.reduce_while(source_keys, [], fn key, acc ->
      case Destination.metadata_value_size(store, key) do
        :unknown -> {:halt, :unknown}
        size when is_integer(size) or is_nil(size) -> {:cont, [size | acc]}
      end
    end)
    |> case do
      :unknown -> :unknown
      sizes -> Enum.reverse(sizes)
    end
  end

  defp ensure_string_keys(keys, store) do
    Enum.reduce_while(keys, :ok, fn key, :ok ->
      case Destination.ensure_string_key(key, store) do
        :ok -> {:cont, :ok}
        @wrongtype_error -> {:halt, @wrongtype_error}
      end
    end)
  end

  defp read_source(key, store) do
    case Destination.ensure_string_key(key, store) do
      :ok -> {:ok, Ops.get(store, key) || <<>>}
      @wrongtype_error -> @wrongtype_error
    end
  end
end
