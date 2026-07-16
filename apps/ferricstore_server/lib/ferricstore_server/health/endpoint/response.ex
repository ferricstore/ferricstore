defmodule FerricstoreServer.Health.Endpoint.Response do
  @moduledoc false

  alias FerricstoreServer.Connection.Send, as: ConnSend
  alias FerricstoreServer.Health.Endpoint.Session

  @common_security_headers [
    {"Cache-Control", "no-store"},
    {"X-Content-Type-Options", "nosniff"},
    {"X-Frame-Options", "DENY"},
    {"Referrer-Policy", "no-referrer"},
    {"Permissions-Policy", "camera=(), geolocation=(), microphone=()"},
    {"Cross-Origin-Opener-Policy", "same-origin"}
  ]

  @dashboard_content_security_policy Enum.join(
                                       [
                                         "default-src 'none'",
                                         "script-src 'self' 'unsafe-inline'",
                                         "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
                                         "font-src 'self' https://fonts.gstatic.com",
                                         "img-src 'self' data:",
                                         "connect-src 'self'",
                                         "frame-ancestors 'none'",
                                         "base-uri 'none'",
                                         "form-action 'self'"
                                       ],
                                       "; "
                                     )

  @header_name_punctuation ~c"!#$%&'*+-.^_`|~"

  @spec send_response(:inet.socket(), module(), pos_integer(), String.t(), String.t()) :: :ok
  def send_response(socket, transport, status_code, status_text, body) do
    send_response(socket, transport, status_code, status_text, "application/json", body)
  end

  @spec send_html_response(:inet.socket(), module(), pos_integer(), String.t(), String.t()) :: :ok
  def send_html_response(socket, transport, status_code, status_text, body) do
    send_html_response(socket, transport, status_code, status_text, body, [])
  end

  @spec send_html_response(
          :inet.socket(),
          module(),
          pos_integer(),
          String.t(),
          String.t(),
          [{binary(), binary()}]
        ) :: :ok
  def send_html_response(socket, transport, status_code, status_text, body, extra_headers) do
    {body, csrf_header} = Session.protect_html(body)

    send_response(
      socket,
      transport,
      status_code,
      status_text,
      "text/html; charset=utf-8",
      body,
      [csrf_header | extra_headers]
    )
  end

  @spec send_text_response(:inet.socket(), module(), pos_integer(), String.t(), String.t()) :: :ok
  def send_text_response(socket, transport, status_code, status_text, body) do
    send_response(socket, transport, status_code, status_text, "text/plain; charset=utf-8", body)
  end

  @spec send_live_json_response(:inet.socket(), module(), map()) :: :ok
  def send_live_json_response(socket, transport, payload) when is_map(payload) do
    {payload, csrf_header} = Session.protect_live_payload(payload)

    send_response(
      socket,
      transport,
      200,
      "OK",
      "application/json; charset=utf-8",
      Jason.encode!(payload),
      [csrf_header]
    )
  end

  @spec send_redirect_response(:inet.socket(), module(), binary()) :: :ok
  def send_redirect_response(socket, transport, location) do
    send_redirect_response(socket, transport, location, [])
  end

  @spec send_redirect_response(:inet.socket(), module(), binary(), [{binary(), binary()}]) :: :ok
  def send_redirect_response(socket, transport, location, extra_headers) do
    location = validate_header_value!(location)
    body = ""
    headers = encode_http_headers(common_security_headers() ++ extra_headers)

    response =
      "HTTP/1.1 302 Found\r\n" <>
        "Location: #{location}\r\n" <>
        headers <>
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
    send_response(socket, transport, status_code, status_text, content_type, body, [])
  end

  defp send_response(
         socket,
         transport,
         status_code,
         status_text,
         content_type,
         body,
         extra_headers
       ) do
    status_text = validate_header_value!(status_text)
    content_type = validate_header_value!(content_type)
    content_length = byte_size(body)
    headers = encode_http_headers(security_headers(content_type) ++ extra_headers)

    response =
      "HTTP/1.1 #{status_code} #{status_text}\r\n" <>
        "Content-Type: #{content_type}\r\n" <>
        headers <>
        "Content-Length: #{content_length}\r\n" <>
        "Connection: close\r\n" <>
        "\r\n" <>
        body

    ConnSend.send(socket, transport, response, :health_response)
    :ok
  end

  defp encode_http_headers(headers) do
    Enum.map_join(headers, "", fn {name, value} ->
      name = validate_header_name!(name)
      value = validate_header_value!(value)
      "#{name}: #{value}\r\n"
    end)
  end

  defp validate_header_name!(name) when is_binary(name) and byte_size(name) > 0 do
    if valid_header_name?(name), do: name, else: raise(ArgumentError, "invalid HTTP header name")
  end

  defp validate_header_name!(_name), do: raise(ArgumentError, "invalid HTTP header name")

  defp valid_header_name?(<<>>), do: true

  defp valid_header_name?(<<byte, rest::binary>>)
       when byte in ?0..?9 or byte in ?A..?Z or byte in ?a..?z or
              byte in @header_name_punctuation,
       do: valid_header_name?(rest)

  defp valid_header_name?(_name), do: false

  defp validate_header_value!(value) when is_binary(value) do
    if valid_header_value?(value),
      do: value,
      else: raise(ArgumentError, "invalid HTTP header value")
  end

  defp validate_header_value!(_value), do: raise(ArgumentError, "invalid HTTP header value")

  defp valid_header_value?(<<>>), do: true

  defp valid_header_value?(<<byte, rest::binary>>)
       when byte == ?\t or (byte >= 32 and byte != 127),
       do: valid_header_value?(rest)

  defp valid_header_value?(_value), do: false

  defp security_headers("text/html" <> _rest) do
    [
      {"Content-Security-Policy", @dashboard_content_security_policy}
      | common_security_headers()
    ]
  end

  defp security_headers(_content_type), do: common_security_headers()

  defp common_security_headers do
    if Session.current_request_secure?() do
      [
        {"Strict-Transport-Security", "max-age=31536000; includeSubDomains"}
        | @common_security_headers
      ]
    else
      @common_security_headers
    end
  end
end
