defmodule FerricstoreServer.Native.Connection.Chunks do
  @moduledoc false

  @flag_compressed 0x08
  @flag_more_chunks 0x20

  def reassemble(frame, state) do
    key = chunk_key(frame)
    more? = Bitwise.band(flags(frame), @flag_more_chunks) != 0

    case {Map.fetch(state.chunk_buffers, key), more?} do
      {:error, false} ->
        {:ready, frame, state}

      {:error, true} ->
        if map_size(state.chunk_buffers) >= state.max_pending_chunks do
          {:error, "ERR native pending chunk stream limit exceeded", state}
        else
          put_pending_chunk(state, key, flags(frame), [body(frame)], byte_size(body(frame)))
        end

      {{:ok, {stored_flags, chunks, total_size}}, true} ->
        previous_size = total_size
        total_size = total_size + byte_size(body(frame))

        if total_size > state.max_frame_bytes do
          state = drop_chunk(state, key, previous_size)
          {:error, "ERR native chunked request exceeds max_frame_bytes", state}
        else
          put_pending_chunk(
            state,
            key,
            stored_flags,
            [body(frame) | chunks],
            total_size,
            previous_size
          )
        end

      {{:ok, {stored_flags, chunks, total_size}}, false} ->
        previous_size = total_size
        total_size = total_size + byte_size(body(frame))

        if total_size > state.max_frame_bytes do
          state = drop_chunk(state, key, previous_size)
          {:error, "ERR native chunked request exceeds max_frame_bytes", state}
        else
          body = chunks |> Enum.reverse() |> IO.iodata_to_binary() |> Kernel.<>(body(frame))
          state = drop_chunk(state, key, previous_size)

          flags =
            Bitwise.band(Bitwise.bor(stored_flags, flags(frame)), Bitwise.bnot(@flag_more_chunks))

          {:ready, put_frame(frame, flags, body), state}
        end
    end
  end

  def maybe_uncompress(frame, state) do
    if Bitwise.band(flags(frame), @flag_compressed) != 0 do
      try do
        body = :zlib.uncompress(body(frame))

        if byte_size(body) > state.max_frame_bytes do
          {:error, "ERR native decompressed frame exceeds max_frame_bytes"}
        else
          {:ok,
           put_frame(frame, Bitwise.band(flags(frame), Bitwise.bnot(@flag_compressed)), body)}
        end
      rescue
        _ -> {:error, "ERR native compressed frame body is invalid"}
      end
    else
      {:ok, frame}
    end
  end

  defp put_pending_chunk(state, key, flags, chunks, total_size, previous_size \\ 0) do
    pending_chunk_bytes = state.pending_chunk_bytes - previous_size + total_size

    if pending_chunk_bytes > state.max_pending_chunk_bytes do
      state = drop_chunk(state, key, previous_size)
      {:error, "ERR native pending chunk bytes limit exceeded", state}
    else
      state = %{
        state
        | chunk_buffers: Map.put(state.chunk_buffers, key, {flags, chunks, total_size}),
          pending_chunk_bytes: pending_chunk_bytes
      }

      {:pending, state}
    end
  end

  defp drop_chunk(state, key, size) do
    %{
      state
      | chunk_buffers: Map.delete(state.chunk_buffers, key),
        pending_chunk_bytes: max(state.pending_chunk_bytes - size, 0)
    }
  end

  defp lane_id({lane_id, _opcode, _request_id, _flags, _body}), do: lane_id
  defp opcode({_lane_id, opcode, _request_id, _flags, _body}), do: opcode
  defp request_id({_lane_id, _opcode, request_id, _flags, _body}), do: request_id
  defp flags({_lane_id, _opcode, _request_id, flags, _body}), do: flags
  defp body({_lane_id, _opcode, _request_id, _flags, body}), do: body

  defp put_frame({lane_id, opcode, request_id, _flags, _body}, flags, body),
    do: {lane_id, opcode, request_id, flags, body}

  defp chunk_key(frame), do: {lane_id(frame), opcode(frame), request_id(frame)}
end
