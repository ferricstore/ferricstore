defmodule FerricstoreHttp.HttpConnectionTest do
  use ExUnit.Case, async: false

  alias FerricstoreHttp.Test.HttpHelpers

  @health_request "GET /health HTTP/1.1\r\nhost: localhost\r\n\r\n"

  defmodule SlowBackend do
    @behaviour FerricstoreHttp.Backend

    @impl FerricstoreHttp.Backend
    def authenticate("worker", "secret", _opts), do: {:ok, :session}
    def authenticate(_username, _password, _opts), do: {:error, :unauthenticated}

    @impl FerricstoreHttp.Backend
    def execute_batch(:session, _commands, _opts) do
      Process.sleep(150)
      {:ok, [%{status: :ok, value: "PONG"}]}
    end

    @impl FerricstoreHttp.Backend
    def ready?, do: true
  end

  test "closes a keep-alive connection after the configured request budget" do
    HttpHelpers.start_server(max_keepalive_requests: 1)
    {:ok, socket} = HttpHelpers.connect()

    assert {:ok, 200, headers, ~s({"status":"ok"})} =
             HttpHelpers.raw_request(socket, @health_request)

    assert {"connection", "close"} in headers
    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
  end

  test "closes idle keep-alive connections within the request-header timeout" do
    HttpHelpers.start_server(request_header_timeout_ms: 40)
    {:ok, socket} = HttpHelpers.connect()

    assert {:ok, 200, _headers, _body} = HttpHelpers.raw_request(socket, @health_request)

    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
  end

  test "bounds incomplete request headers" do
    HttpHelpers.start_server(request_header_timeout_ms: 40)
    {:ok, socket} = HttpHelpers.connect()

    assert {:ok, 408, _headers, ""} =
             HttpHelpers.raw_request(socket, "GET /health HTTP/1.1\r\nhost: localhost")

    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
  end

  test "closes a connection when an active stream exceeds the idle timeout" do
    HttpHelpers.start_server(backend: SlowBackend, idle_timeout_ms: 40)
    {:ok, socket} = HttpHelpers.connect()
    authorization = HttpHelpers.basic("worker", "secret") |> elem(1)
    body = ~s({"commands":[["PING"]]})

    request =
      "POST /v1/commands HTTP/1.1\r\n" <>
        "host: localhost\r\n" <>
        "authorization: #{authorization}\r\n" <>
        "content-type: application/json\r\n" <>
        "content-length: #{byte_size(body)}\r\n\r\n" <>
        body

    assert :ok = :gen_tcp.send(socket, request)
    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1_000)
  end

  test "rejects excessive header counts and request lines before dispatch" do
    HttpHelpers.start_server(max_headers: 2, max_request_line_bytes: 32)

    {:ok, headers_socket} = HttpHelpers.connect()

    too_many_headers =
      "GET /health HTTP/1.1\r\n" <>
        "host: localhost\r\n" <>
        "x-first: one\r\n" <>
        "x-second: two\r\n\r\n"

    assert {:ok, 431, headers, body} =
             HttpHelpers.raw_request(headers_socket, too_many_headers)

    assert {"content-length", Integer.to_string(byte_size(body))} in headers

    :ok = :gen_tcp.close(headers_socket)

    {:ok, line_socket} = HttpHelpers.connect()
    long_path = "/" <> String.duplicate("x", 64)

    assert {:ok, 414, headers, body} =
             HttpHelpers.raw_request(
               line_socket,
               "GET #{long_path} HTTP/1.1\r\nhost: localhost\r\n\r\n"
             )

    assert {"content-length", Integer.to_string(byte_size(body))} in headers

    :ok = :gen_tcp.close(line_socket)
  end

  test "returns a framed request-line rejection to a normal HTTP client" do
    HttpHelpers.start_server(max_request_line_bytes: 32)
    :ok = ensure_inets()
    path = "/" <> String.duplicate("x", 64)
    url = String.to_charlist("http://127.0.0.1:#{FerricstoreHttp.Listener.port()}#{path}")

    assert {:ok, {{_version, 414, _reason}, headers, body}} =
             :httpc.request(:get, {url, []}, [timeout: 500], body_format: :binary)

    assert {~c"content-length", String.to_charlist(Integer.to_string(byte_size(body)))} in headers
  end

  test "rejects excessive header name and value lengths before dispatch" do
    HttpHelpers.start_server(max_header_name_bytes: 8, max_header_value_bytes: 8)

    {:ok, name_socket} = HttpHelpers.connect()

    assert {:ok, 431, _headers, _body} =
             HttpHelpers.raw_request(
               name_socket,
               "GET /health HTTP/1.1\r\nhost: local\r\nx-header-too-long: ok\r\n\r\n"
             )

    :ok = :gen_tcp.close(name_socket)

    {:ok, value_socket} = HttpHelpers.connect()

    assert {:ok, 431, _headers, _body} =
             HttpHelpers.raw_request(
               value_socket,
               "GET /health HTTP/1.1\r\nhost: local\r\nx-test: value-too-long\r\n\r\n"
             )

    :ok = :gen_tcp.close(value_socket)
  end

  defp ensure_inets do
    case :inets.start() do
      :ok -> :ok
      {:error, {:already_started, :inets}} -> :ok
    end
  end
end
