defmodule FerricstoreServer.Health.Endpoint.Response do
  @moduledoc false

  alias FerricstoreServer.Connection.Send, as: ConnSend

  @spec send_response(:inet.socket(), module(), pos_integer(), String.t(), String.t()) :: :ok
  def send_response(socket, transport, status_code, status_text, body) do
    send_response(socket, transport, status_code, status_text, "application/json", body)
  end

  @spec send_html_response(:inet.socket(), module(), pos_integer(), String.t(), String.t()) :: :ok
  def send_html_response(socket, transport, status_code, status_text, body) do
    send_response(socket, transport, status_code, status_text, "text/html; charset=utf-8", body)
  end

  @spec send_text_response(:inet.socket(), module(), pos_integer(), String.t(), String.t()) :: :ok
  def send_text_response(socket, transport, status_code, status_text, body) do
    send_response(socket, transport, status_code, status_text, "text/plain; charset=utf-8", body)
  end

  @spec send_redirect_response(:inet.socket(), module(), binary()) :: :ok
  def send_redirect_response(socket, transport, location) do
    send_redirect_response(socket, transport, location, [])
  end

  @spec send_redirect_response(:inet.socket(), module(), binary(), [{binary(), binary()}]) :: :ok
  def send_redirect_response(socket, transport, location, extra_headers) do
    body = ""
    extra = encode_http_headers(extra_headers)

    response =
      "HTTP/1.1 302 Found\r\n" <>
        "Location: #{location}\r\n" <>
        extra <>
        "Content-Length: #{byte_size(body)}\r\n" <>
        "Connection: close\r\n" <>
        "\r\n" <>
        body

    ConnSend.send(socket, transport, response, :health_response)
    :ok
  end

  @spec send_response(
          :inet.socket(),
          module(),
          pos_integer(),
          String.t(),
          String.t(),
          String.t()
        ) :: :ok
  def send_response(socket, transport, status_code, status_text, content_type, body) do
    content_length = byte_size(body)

    response =
      "HTTP/1.1 #{status_code} #{status_text}\r\n" <>
        "Content-Type: #{content_type}\r\n" <>
        "Content-Length: #{content_length}\r\n" <>
        "Connection: close\r\n" <>
        "\r\n" <>
        body

    ConnSend.send(socket, transport, response, :health_response)
    :ok
  end

  defp encode_http_headers(headers) do
    Enum.map_join(headers, "", fn {name, value} -> "#{name}: #{value}\r\n" end)
  end
end
