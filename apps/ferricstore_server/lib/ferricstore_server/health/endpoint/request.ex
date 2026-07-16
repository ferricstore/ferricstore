defmodule FerricstoreServer.Health.Endpoint.Request do
  @moduledoc false

  @max_request_bytes 8_192
  @request_recv_timeout_ms 5_000
  @header_name_pattern ~r/^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/

  @spec read_request(:inet.socket(), module()) ::
          {:ok, String.t(), String.t(), map(), binary()} | :error
  def read_request(socket, transport) do
    deadline_ms = System.monotonic_time(:millisecond) + @request_recv_timeout_ms

    with :ok <- transport.setopts(socket, buffer: @max_request_bytes) do
      read_request(socket, transport, <<>>, deadline_ms)
    else
      _ -> :error
    end
  end

  defp read_request(_socket, _transport, data, _deadline_ms)
       when byte_size(data) > @max_request_bytes do
    :error
  end

  defp read_request(socket, transport, data, deadline_ms) do
    with {:ok, remaining_ms} <- remaining_timeout(deadline_ms) do
      if :binary.match(data, "\r\n\r\n") != :nomatch do
        case parse_request_line(data) do
          {:ok, method, path, headers, body} ->
            read_request_body(
              socket,
              transport,
              method,
              path,
              headers,
              body,
              deadline_ms,
              request_head_bytes(data)
            )

          :error ->
            :error
        end
      else
        recv_request_headers(socket, transport, data, deadline_ms, remaining_ms)
      end
    else
      :error -> :error
    end
  end

  defp recv_request_headers(socket, transport, data, deadline_ms, remaining_ms) do
    remaining_bytes = @max_request_bytes - byte_size(data)

    if remaining_bytes > 0 do
      case transport.recv(socket, 0, remaining_ms) do
        {:ok, chunk} when byte_size(chunk) > 0 and byte_size(chunk) <= remaining_bytes ->
          read_request(socket, transport, data <> chunk, deadline_ms)

        {:ok, _chunk} ->
          :error

        {:error, _reason} ->
          :error
      end
    else
      :error
    end
  end

  @spec parse_request_line(binary()) ::
          {:ok, String.t(), String.t(), map(), binary()} | :error
  def parse_request_line(data) do
    case String.split(data, "\r\n\r\n", parts: 2) do
      [head, body] ->
        with true <- String.valid?(head),
             [request_line | header_lines] <- String.split(head, "\r\n"),
             [method, path, _version] <- String.split(request_line, " ", parts: 3),
             {:ok, headers} <- parse_headers(header_lines) do
          {:ok, method, path, headers, body}
        else
          _ -> :error
        end

      _ ->
        :error
    end
  end

  def read_request_body(socket, transport, method, path, headers, body) do
    deadline_ms = System.monotonic_time(:millisecond) + @request_recv_timeout_ms
    read_request_body(socket, transport, method, path, headers, body, deadline_ms, 0)
  end

  defp read_request_body(
         socket,
         transport,
         method,
         path,
         headers,
         body,
         deadline_ms,
         head_bytes
       ) do
    with {:ok, content_length} <- request_content_length(headers),
         true <- head_bytes in 0..@max_request_bytes,
         true <- content_length <= @max_request_bytes - head_bytes,
         {:ok, _remaining_ms} <- remaining_timeout(deadline_ms) do
      cond do
        content_length == 0 ->
          {:ok, method, path, headers, <<>>}

        byte_size(body) >= content_length ->
          {:ok, method, path, headers, binary_part(body, 0, content_length)}

        true ->
          recv_request_body(
            socket,
            transport,
            method,
            path,
            headers,
            body,
            content_length,
            deadline_ms
          )
      end
    else
      _ -> :error
    end
  end

  defp recv_request_body(
         _socket,
         _transport,
         _method,
         _path,
         _headers,
         body,
         content_length,
         _deadline_ms
       )
       when byte_size(body) > content_length or byte_size(body) > @max_request_bytes do
    :error
  end

  defp recv_request_body(
         socket,
         transport,
         method,
         path,
         headers,
         body,
         content_length,
         deadline_ms
       ) do
    with {:ok, remaining_ms} <- remaining_timeout(deadline_ms) do
      if byte_size(body) == content_length do
        {:ok, method, path, headers, body}
      else
        needed = content_length - byte_size(body)

        case transport.recv(socket, needed, remaining_ms) do
          {:ok, chunk} when byte_size(chunk) > 0 and byte_size(chunk) <= needed ->
            recv_request_body(
              socket,
              transport,
              method,
              path,
              headers,
              body <> chunk,
              content_length,
              deadline_ms
            )

          {:ok, _chunk} ->
            :error

          {:error, _reason} ->
            :error
        end
      end
    else
      :error ->
        :error
    end
  end

  defp remaining_timeout(deadline_ms) do
    case deadline_ms - System.monotonic_time(:millisecond) do
      remaining_ms when remaining_ms > 0 -> {:ok, remaining_ms}
      _ -> :error
    end
  end

  def request_content_length(headers) do
    case Map.get(headers, "content-length") do
      nil ->
        {:ok, 0}

      value ->
        case Integer.parse(value) do
          {n, ""} when n >= 0 -> {:ok, n}
          _ -> :error
        end
    end
  end

  def peer_addr(socket, transport) do
    case transport.peername(socket) do
      {:ok, {addr, _port}} -> addr
      _ -> :unknown
    end
  end

  def parse_headers(lines) when is_list(lines) do
    Enum.reduce_while(lines, {:ok, %{}}, fn line, {:ok, headers} ->
      with [name, value] <- String.split(line, ":", parts: 2),
           true <- Regex.match?(@header_name_pattern, name),
           normalized_name = String.downcase(name),
           false <- normalized_name == "transfer-encoding",
           false <- Map.has_key?(headers, normalized_name) do
        {:cont, {:ok, Map.put(headers, normalized_name, String.trim(value))}}
      else
        _ -> {:halt, :error}
      end
    end)
  rescue
    _error -> :error
  end

  def parse_headers(_lines), do: :error

  defp request_head_bytes(data) do
    case :binary.match(data, "\r\n\r\n") do
      {offset, 4} -> offset + 4
      :nomatch -> @max_request_bytes + 1
    end
  end
end
