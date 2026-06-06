defmodule FerricstoreServer.Health.Endpoint.Request do
  @moduledoc false

  @max_request_bytes 8_192
  @request_recv_timeout_ms 5_000

  @spec read_request(:inet.socket(), module()) ::
          {:ok, String.t(), String.t(), map(), binary()} | :error
  def read_request(socket, transport) do
    read_request(socket, transport, <<>>)
  end

  defp read_request(_socket, _transport, data) when byte_size(data) > @max_request_bytes do
    :error
  end

  defp read_request(socket, transport, data) do
    if :binary.match(data, "\r\n\r\n") != :nomatch do
      case parse_request_line(data) do
        {:ok, method, path, headers, body} ->
          read_request_body(socket, transport, method, path, headers, body)

        :error ->
          :error
      end
    else
      case transport.recv(socket, 0, @request_recv_timeout_ms) do
        {:ok, chunk} -> read_request(socket, transport, data <> chunk)
        {:error, _reason} -> :error
      end
    end
  end

  @spec parse_request_line(binary()) ::
          {:ok, String.t(), String.t(), map(), binary()} | :error
  def parse_request_line(data) do
    case String.split(data, "\r\n\r\n", parts: 2) do
      [head, body] ->
        [request_line | header_lines] = String.split(head, "\r\n")

        case String.split(request_line, " ", parts: 3) do
          [method, path, _version] -> {:ok, method, path, parse_headers(header_lines), body}
          _ -> :error
        end

      _ ->
        :error
    end
  end

  def read_request_body(socket, transport, method, path, headers, body) do
    with {:ok, content_length} <- request_content_length(headers),
         true <- content_length <= @max_request_bytes do
      cond do
        content_length == 0 ->
          {:ok, method, path, headers, <<>>}

        byte_size(body) >= content_length ->
          {:ok, method, path, headers, binary_part(body, 0, content_length)}

        true ->
          read_request_body(socket, transport, method, path, headers, body, content_length)
      end
    else
      _ -> :error
    end
  end

  defp read_request_body(_socket, _transport, _method, _path, _headers, body, content_length)
       when byte_size(body) > content_length or byte_size(body) > @max_request_bytes do
    :error
  end

  defp read_request_body(_socket, _transport, method, path, headers, body, content_length)
       when byte_size(body) == content_length do
    {:ok, method, path, headers, body}
  end

  defp read_request_body(socket, transport, method, path, headers, body, content_length) do
    needed = content_length - byte_size(body)

    case transport.recv(socket, needed, @request_recv_timeout_ms) do
      {:ok, chunk} ->
        read_request_body(socket, transport, method, path, headers, body <> chunk, content_length)

      {:error, _reason} ->
        :error
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

  def parse_headers(lines) do
    Enum.reduce(lines, %{}, fn line, acc ->
      case String.split(line, ":", parts: 2) do
        [name, value] ->
          Map.put(acc, String.downcase(String.trim(name)), String.trim(value))

        _ ->
          acc
      end
    end)
  end
end
