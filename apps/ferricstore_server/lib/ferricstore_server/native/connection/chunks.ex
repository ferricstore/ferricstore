defmodule FerricstoreServer.Native.Connection.Chunks do
  @moduledoc false

  alias FerricstoreServer.Native.ResourceBudget

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
          start_pending_chunk(state, key, flags(frame), body(frame))
        end

      {{:ok, {stored_flags, chunks, total_size, stream_token, bytes_token, deadline_ms}}, true} ->
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
            previous_size,
            stream_token,
            bytes_token,
            deadline_ms
          )
        end

      {{:ok, {stored_flags, chunks, total_size, _stream_token, bytes_token, _deadline_ms}}, false} ->
        previous_size = total_size
        total_size = total_size + byte_size(body(frame))

        cond do
          total_size > state.max_frame_bytes ->
            state = drop_chunk(state, key, previous_size)
            {:error, "ERR native chunked request exceeds max_frame_bytes", state}

          ResourceBudget.resize(resource_budget(state), bytes_token, total_size) != :ok ->
            state = drop_chunk(state, key, previous_size)
            {:error, "ERR native global pending chunk bytes limit exceeded", state}

          true ->
            body = chunks |> Enum.reverse() |> IO.iodata_to_binary() |> Kernel.<>(body(frame))
            state = drop_chunk(state, key, previous_size)

            flags =
              Bitwise.band(
                Bitwise.bor(stored_flags, flags(frame)),
                Bitwise.bnot(@flag_more_chunks)
              )

            {:ready, put_frame(frame, flags, body), state}
        end
    end
  end

  def maybe_uncompress(frame, state) do
    if Bitwise.band(flags(frame), @flag_compressed) != 0 do
      case bounded_uncompress(body(frame), state.max_frame_bytes) do
        {:ok, body} ->
          {:ok,
           put_frame(frame, Bitwise.band(flags(frame), Bitwise.bnot(@flag_compressed)), body)}

        {:error, :too_large} ->
          {:error, "ERR native decompressed frame exceeds max_frame_bytes"}

        {:error, :invalid} ->
          {:error, "ERR native compressed frame body is invalid"}
      end
    else
      {:ok, frame}
    end
  end

  defp bounded_uncompress(compressed, max_bytes) do
    stream = :zlib.open()

    try do
      :ok = :zlib.inflateInit(stream)

      case bounded_inflate(stream, compressed, max_bytes, 0, []) do
        {:ok, body} ->
          :ok = :zlib.inflateEnd(stream)
          {:ok, body}

        {:error, _reason} = error ->
          error
      end
    rescue
      _error -> {:error, :invalid}
    catch
      _kind, _reason -> {:error, :invalid}
    after
      :zlib.close(stream)
    end
  end

  defp bounded_inflate(stream, input, max_bytes, total_bytes, chunks) do
    case :zlib.safeInflate(stream, input) do
      {status, output} when status in [:continue, :finished] ->
        output_bytes = IO.iodata_length(output)
        next_total = total_bytes + output_bytes

        cond do
          next_total > max_bytes ->
            {:error, :too_large}

          status == :finished ->
            {:ok, chunks |> Enum.reverse([output]) |> IO.iodata_to_binary()}

          true ->
            bounded_inflate(stream, [], max_bytes, next_total, [output | chunks])
        end

      {:need_dictionary, _adler, _output} ->
        {:error, :invalid}
    end
  end

  defp start_pending_chunk(state, key, flags, body) do
    total_size = byte_size(body)
    pending_chunk_bytes = state.pending_chunk_bytes + total_size

    deadline_ms =
      System.monotonic_time(:millisecond) + Map.get(state, :frame_assembly_timeout_ms, 15_000)

    if pending_chunk_bytes > state.max_pending_chunk_bytes do
      {:error, "ERR native pending chunk bytes limit exceeded", state}
    else
      budget = resource_budget(state)

      case ResourceBudget.acquire(budget, :chunk_streams, self(), 1) do
        {:ok, stream_token} ->
          case ResourceBudget.acquire(budget, :chunk_bytes, self(), total_size) do
            {:ok, bytes_token} ->
              {:pending,
               put_chunk_state(
                 state,
                 key,
                 flags,
                 [body],
                 total_size,
                 0,
                 stream_token,
                 bytes_token,
                 deadline_ms
               )}

            {:error, _reason} ->
              ResourceBudget.release(budget, stream_token)
              {:error, "ERR native global pending chunk bytes limit exceeded", state}
          end

        {:error, _reason} ->
          {:error, "ERR native global pending chunk stream limit exceeded", state}
      end
    end
  end

  defp put_pending_chunk(
         state,
         key,
         flags,
         chunks,
         total_size,
         previous_size,
         stream_token,
         bytes_token,
         deadline_ms
       ) do
    pending_chunk_bytes = state.pending_chunk_bytes - previous_size + total_size

    cond do
      pending_chunk_bytes > state.max_pending_chunk_bytes ->
        state = drop_chunk(state, key, previous_size)
        {:error, "ERR native pending chunk bytes limit exceeded", state}

      ResourceBudget.resize(resource_budget(state), bytes_token, total_size) != :ok ->
        state = drop_chunk(state, key, previous_size)
        {:error, "ERR native global pending chunk bytes limit exceeded", state}

      true ->
        {:pending,
         put_chunk_state(
           state,
           key,
           flags,
           chunks,
           total_size,
           previous_size,
           stream_token,
           bytes_token,
           deadline_ms
         )}
    end
  end

  defp put_chunk_state(
         state,
         key,
         flags,
         chunks,
         total_size,
         previous_size,
         stream_token,
         bytes_token,
         deadline_ms
       ) do
    chunk_assembly_deadline_ms =
      case Map.get(state, :chunk_assembly_deadline_ms) do
        current when is_integer(current) -> min(current, deadline_ms)
        _missing -> deadline_ms
      end

    %{
      state
      | chunk_buffers:
          Map.put(
            state.chunk_buffers,
            key,
            {flags, chunks, total_size, stream_token, bytes_token, deadline_ms}
          ),
        pending_chunk_bytes: state.pending_chunk_bytes - previous_size + total_size,
        chunk_assembly_deadline_ms: chunk_assembly_deadline_ms
    }
  end

  defp drop_chunk(state, key, size) do
    dropped_deadline =
      case Map.get(state.chunk_buffers, key) do
        {_flags, _chunks, _total_size, stream_token, bytes_token, deadline_ms} ->
          budget = resource_budget(state)
          ResourceBudget.release(budget, bytes_token)
          ResourceBudget.release(budget, stream_token)
          deadline_ms

        nil ->
          nil
      end

    chunk_buffers = Map.delete(state.chunk_buffers, key)

    %{
      state
      | chunk_buffers: chunk_buffers,
        pending_chunk_bytes: max(state.pending_chunk_bytes - size, 0),
        chunk_assembly_deadline_ms:
          next_chunk_assembly_deadline(state, chunk_buffers, dropped_deadline)
    }
  end

  defp next_chunk_assembly_deadline(_state, chunk_buffers, _dropped_deadline)
       when map_size(chunk_buffers) == 0,
       do: nil

  defp next_chunk_assembly_deadline(state, chunk_buffers, dropped_deadline) do
    case Map.get(state, :chunk_assembly_deadline_ms) do
      current when is_integer(current) and current != dropped_deadline ->
        current

      _removed_or_missing ->
        Enum.reduce(chunk_buffers, nil, fn
          {_key, {_flags, _chunks, _total_size, _stream_token, _bytes_token, deadline_ms}}, nil ->
            deadline_ms

          {_key, {_flags, _chunks, _total_size, _stream_token, _bytes_token, deadline_ms}},
          earliest ->
            min(earliest, deadline_ms)
        end)
    end
  end

  defp resource_budget(state), do: Map.get(state, :resource_budget, ResourceBudget)

  defp lane_id({lane_id, _opcode, _request_id, _flags, _body}), do: lane_id
  defp opcode({_lane_id, opcode, _request_id, _flags, _body}), do: opcode
  defp request_id({_lane_id, _opcode, request_id, _flags, _body}), do: request_id
  defp flags({_lane_id, _opcode, _request_id, flags, _body}), do: flags
  defp body({_lane_id, _opcode, _request_id, _flags, body}), do: body

  defp put_frame({lane_id, opcode, request_id, _flags, _body}, flags, body),
    do: {lane_id, opcode, request_id, flags, body}

  defp chunk_key(frame), do: {lane_id(frame), opcode(frame), request_id(frame)}
end
