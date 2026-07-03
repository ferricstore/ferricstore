defmodule FerricStore.SDK.Native.Codec do
  @moduledoc false

  import Bitwise

  @magic "FSNP"
  @version 1
  @response_direction 0x80
  @header_size 24
  @flag_custom_payload 0x02
  @flag_compressed 0x08
  @flag_more_chunks 0x20

  @compact_ok_list 0x81
  @compact_kv_get 0x82
  @compact_kv_mget 0x83
  @compact_kv_mget_fixed 0x89

  @status %{
    0 => :ok,
    1 => :error,
    2 => :auth,
    3 => :noperm,
    4 => :busy,
    5 => :reroute,
    6 => :bad_request
  }

  @spec encode_frame(non_neg_integer(), non_neg_integer(), non_neg_integer(), term()) :: binary()
  def encode_frame(opcode, lane_id, request_id, payload) do
    body = encode_value(payload || %{})

    <<@magic, @version, 0, lane_id::unsigned-32, opcode::unsigned-16, request_id::unsigned-64,
      byte_size(body)::unsigned-32, body::binary>>
  end

  @spec decode_frames(binary(), pos_integer()) ::
          {:ok, [tuple()], binary()} | {:error, term()}
  def decode_frames(buffer, max_frame_bytes) when is_binary(buffer) do
    scan_frames(buffer, max_frame_bytes, [])
  end

  @spec decode_response(non_neg_integer(), non_neg_integer(), binary()) ::
          {:ok, term()} | {:error, term()} | {:auth, term()} | {:noperm, term()} | {:busy, term()}
  def decode_response(opcode, flags, body) do
    with {:ok, body} <- maybe_decompress(flags, body),
         {:ok, status, payload} <- split_response_body(body),
         {:ok, value} <- decode_response_payload(opcode, flags, payload) do
      case Map.get(@status, status, :unknown) do
        :ok -> {:ok, value}
        other -> {other, value}
      end
    end
  end

  @spec custom_payload?(non_neg_integer()) :: boolean()
  def custom_payload?(flags), do: band(flags, @flag_custom_payload) != 0

  @spec more_chunks?(non_neg_integer()) :: boolean()
  def more_chunks?(flags), do: band(flags, @flag_more_chunks) != 0

  defp scan_frames(buffer, _max_frame_bytes, acc) when byte_size(buffer) < @header_size,
    do: {:ok, Enum.reverse(acc), buffer}

  defp scan_frames(
         <<@magic, version_byte, flags, lane_id::unsigned-32, opcode::unsigned-16,
           request_id::unsigned-64, body_len::unsigned-32, rest::binary>> = buffer,
         max_frame_bytes,
         acc
       ) do
    version = band(version_byte, 0x7F)

    cond do
      version != @version ->
        {:error, {:unsupported_version, version}}

      band(version_byte, @response_direction) == 0 ->
        {:error, :not_a_response_frame}

      body_len > max_frame_bytes ->
        {:error, :frame_too_large}

      byte_size(rest) < body_len ->
        {:ok, Enum.reverse(acc), buffer}

      true ->
        <<body::binary-size(body_len), next::binary>> = rest
        raw_len = @header_size + body_len
        raw = binary_part(buffer, 0, raw_len)
        frame = {lane_id, opcode, request_id, flags, body, raw}
        scan_frames(next, max_frame_bytes, [frame | acc])
    end
  end

  defp scan_frames(_buffer, _max_frame_bytes, _acc), do: {:error, :invalid_magic}

  defp split_response_body(<<status::unsigned-16, payload::binary>>), do: {:ok, status, payload}
  defp split_response_body(_body), do: {:error, :truncated_response}

  defp decode_response_payload(opcode, flags, payload) do
    if custom_payload?(flags) do
      decode_custom_response_payload(opcode, payload)
    else
      decode_typed_payload(payload)
    end
  end

  defp decode_custom_response_payload(0x0101, <<@compact_kv_get, 0>>), do: {:ok, nil}

  defp decode_custom_response_payload(
         0x0101,
         <<@compact_kv_get, 1, len::unsigned-32, rest::binary>>
       )
       when byte_size(rest) == len,
       do: {:ok, rest}

  defp decode_custom_response_payload(
         opcode,
         <<@compact_kv_mget, count::unsigned-32, rest::binary>>
       )
       when opcode in [0x0104, 0x020C],
       do: decode_compact_mget(count, rest, [])

  defp decode_custom_response_payload(
         opcode,
         <<@compact_kv_mget_fixed, 0::unsigned-32, 0::unsigned-32>>
       )
       when opcode in [0x0104, 0x020C],
       do: {:ok, []}

  defp decode_custom_response_payload(
         opcode,
         <<@compact_kv_mget_fixed, count::unsigned-32, len::unsigned-32, rest::binary>>
       )
       when opcode in [0x0104, 0x020C] and byte_size(rest) == count * len do
    values =
      for offset <- 0..(count - 1)//1 do
        binary_part(rest, offset * len, len)
      end

    {:ok, values}
  end

  defp decode_custom_response_payload(opcode, <<@compact_ok_list, count::unsigned-32>>)
       when opcode in [0x0102, 0x0105] and count >= 1,
       do: {:ok, "OK"}

  defp decode_custom_response_payload(_opcode, payload), do: decode_typed_payload(payload)

  defp decode_compact_mget(0, "", acc), do: {:ok, Enum.reverse(acc)}

  defp decode_compact_mget(count, <<0, rest::binary>>, acc) when count > 0,
    do: decode_compact_mget(count - 1, rest, [nil | acc])

  defp decode_compact_mget(count, <<1, len::unsigned-32, rest::binary>>, acc)
       when count > 0 and byte_size(rest) >= len do
    <<value::binary-size(len), next::binary>> = rest
    decode_compact_mget(count - 1, next, [value | acc])
  end

  defp decode_compact_mget(_count, _rest, _acc), do: {:error, :invalid_compact_mget}

  defp decode_typed_payload(payload) do
    case decode_value(payload) do
      {:ok, value, ""} -> {:ok, value}
      {:ok, _value, _rest} -> {:error, :trailing_response_bytes}
      {:error, reason} -> {:error, reason}
    end
  end

  defp maybe_decompress(flags, body) do
    if band(flags, @flag_compressed) != 0 do
      {:ok, :zlib.uncompress(body)}
    else
      {:ok, body}
    end
  rescue
    _ -> {:error, :invalid_compressed_payload}
  end

  @spec encode_value(term()) :: binary()
  def encode_value(nil), do: <<0>>
  def encode_value(true), do: <<1>>
  def encode_value(false), do: <<2>>
  def encode_value(value) when is_integer(value), do: <<3, value::signed-64>>

  def encode_value(value) when is_binary(value),
    do: <<4, byte_size(value)::unsigned-32, value::binary>>

  def encode_value(value) when is_atom(value), do: value |> Atom.to_string() |> encode_value()

  def encode_value(values) when is_list(values) do
    body = values |> Enum.map(&encode_value/1) |> IO.iodata_to_binary()
    <<5, length(values)::unsigned-32, body::binary>>
  end

  def encode_value(values) when is_map(values) do
    entries =
      values
      |> Enum.map(fn {key, value} ->
        key = encode_key(key)
        [<<byte_size(key)::unsigned-32>>, key, encode_value(value)]
      end)
      |> IO.iodata_to_binary()

    <<6, map_size(values)::unsigned-32, entries::binary>>
  end

  def encode_value(value) when is_float(value), do: <<7, value::float-64>>
  def encode_value(value), do: value |> inspect(limit: 50) |> encode_value()

  @spec decode_value(binary()) :: {:ok, term(), binary()} | {:error, term()}
  def decode_value(<<0, rest::binary>>), do: {:ok, nil, rest}
  def decode_value(<<1, rest::binary>>), do: {:ok, true, rest}
  def decode_value(<<2, rest::binary>>), do: {:ok, false, rest}
  def decode_value(<<3, value::signed-64, rest::binary>>), do: {:ok, value, rest}
  def decode_value(<<4, len::unsigned-32, rest::binary>>), do: decode_binary(len, rest)
  def decode_value(<<5, count::unsigned-32, rest::binary>>), do: decode_array(count, rest, [])
  def decode_value(<<6, count::unsigned-32, rest::binary>>), do: decode_map(count, rest, %{})
  def decode_value(<<7, value::float-64, rest::binary>>), do: {:ok, value, rest}
  def decode_value(<<>>), do: {:error, :empty_value}
  def decode_value(_), do: {:error, :unknown_or_truncated_value}

  defp decode_binary(len, rest) when byte_size(rest) >= len do
    <<value::binary-size(len), next::binary>> = rest
    {:ok, value, next}
  end

  defp decode_binary(_len, _rest), do: {:error, :truncated_binary}

  defp decode_array(0, rest, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_array(count, rest, acc) do
    case decode_value(rest) do
      {:ok, value, next} -> decode_array(count - 1, next, [value | acc])
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_map(0, rest, acc), do: {:ok, acc, rest}

  defp decode_map(count, <<key_len::unsigned-32, rest::binary>>, acc) do
    with {:ok, key, after_key} <- decode_binary(key_len, rest),
         {:ok, value, after_value} <- decode_value(after_key) do
      decode_map(count - 1, after_value, Map.put(acc, key, value))
    end
  end

  defp decode_map(_count, _rest, _acc), do: {:error, :truncated_map}

  defp encode_key(key) when is_binary(key), do: key
  defp encode_key(key) when is_atom(key), do: Atom.to_string(key)
  defp encode_key(key), do: to_string(key)
end
