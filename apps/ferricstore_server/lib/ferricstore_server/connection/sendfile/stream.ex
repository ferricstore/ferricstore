defmodule FerricstoreServer.Connection.Sendfile.Stream do
  @moduledoc false

  alias FerricstoreServer.Connection.Send, as: ConnSend
  alias FerricstoreServer.Connection.Sendfile.IO, as: SendIO

  @file_stream_chunk_bytes Application.compile_env(
                             :ferricstore_server,
                             :file_stream_chunk_bytes,
                             65_536
                           )

  def do_stream_file_get(path, offset, size, validator, state) do
    case SendIO.file_open(path) do
      {:ok, fd} ->
        try do
          do_stream_file_get_open(path, fd, offset, size, validator, state)
        after
          :file.close(fd)
        end

      {:error, _reason} ->
        :fallback
    end
  end

  def do_stream_file_get_open(path, fd, offset, size, validator, state) do
    case SendIO.validate_open_file_range(path, fd, offset, size, validator, :file_stream) do
      :ok -> stream_file_get_open(fd, offset, size, state)
      :mismatch -> :fallback
    end
  end

  def do_stream_file_ref_get(key, path, offset, size, state) do
    case SendIO.file_open(path) do
      {:ok, fd} ->
        try do
          do_stream_file_ref_get_open(key, path, fd, offset, size, state)
        after
          :file.close(fd)
        end

      {:error, _reason} ->
        :fallback
    end
  end

  def do_stream_file_ref_get_open(key, path, fd, offset, size, state) do
    case SendIO.validate_open_file_ref(path, fd, key, offset, size, :file_stream) do
      :ok -> stream_file_get_open(fd, offset, size, state)
      :mismatch -> :fallback
    end
  end

  def stream_file_get_open(fd, offset, size, state) do
    header = [?$, Integer.to_string(size), "\r\n"]

    case ConnSend.send(state.socket, state.transport, header, :file_stream_header, %{
           client_id: state.client_id
         }) do
      :ok ->
        case stream_file_chunks(fd, offset, size, state, 0) do
          {:ok, chunks} ->
            case ConnSend.send(state.socket, state.transport, "\r\n", :file_stream_trailer, %{
                   client_id: state.client_id
                 }) do
              :ok -> {:sent, state, chunks}
              {:error, reason} -> {:error_after_header, reason, chunks}
            end

          {:error, reason, chunks} ->
            {:error_after_header, reason, chunks}
        end

      {:error, _reason} ->
        :fallback
    end
  end

  def stream_file_chunks(_fd, _offset, 0, _state, chunks), do: {:ok, chunks}

  def stream_file_chunks(fd, offset, remaining, state, chunks) do
    read_size = min(remaining, @file_stream_chunk_bytes)

    case SendIO.file_pread(fd, offset, read_size) do
      {:ok, data} when is_binary(data) and byte_size(data) > 0 ->
        sent = byte_size(data)

        case ConnSend.send(state.socket, state.transport, data, :file_stream_chunk, %{
               client_id: state.client_id
             }) do
          :ok -> stream_file_chunks(fd, offset + sent, remaining - sent, state, chunks + 1)
          {:error, reason} -> {:error, reason, chunks}
        end

      {:ok, _empty} ->
        {:error, :eof, chunks}

      :eof ->
        {:error, :eof, chunks}

      {:error, reason} ->
        {:error, reason, chunks}
    end
  end
end
