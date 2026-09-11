defmodule FerricstoreHttp.HttpServerTest do
  use ExUnit.Case, async: false

  alias FerricstoreHttp.BinaryEnvelope
  alias FerricstoreHttp.Test.HttpHelpers

  test "serves authenticated JSON command batches and binary values" do
    HttpHelpers.start_server()
    bytes = <<0, 255, 1>>

    envelope = %{
      "encoding" => BinaryEnvelope.encoding(),
      "commands" => BinaryEnvelope.encode([["PING"], ["ECHO", bytes]])
    }

    {status, _headers, body} =
      HttpHelpers.request(
        :post,
        "/v1/commands",
        "application/json",
        [HttpHelpers.basic("worker", "secret")],
        Jason.encode!(envelope)
      )

    assert status == 200

    assert %{
             "encoding" => "ferricstore-json-v1",
             "results" => [
               %{"status" => "ok", "value" => pong},
               %{"status" => "ok", "value" => encoded_bytes}
             ]
           } = Jason.decode!(body)

    assert {:ok, "PONG"} = BinaryEnvelope.decode(pong)
    assert {:ok, ^bytes} = BinaryEnvelope.decode(encoded_bytes)
  end

  test "supports the SDK MessagePack command format without base64" do
    HttpHelpers.start_server()
    bytes = <<0, 255, 1>>

    envelope = %{
      "encoding" => "ferricstore-msgpack-v1",
      "commands" => [["ECHO", Msgpax.Bin.new(bytes)]]
    }

    {status, headers, body} =
      HttpHelpers.request(
        :post,
        "/v1/commands",
        "application/vnd.ferricstore.commands+msgpack",
        [HttpHelpers.basic("worker", "secret")],
        Msgpax.pack!(envelope)
      )

    assert status == 200
    assert {"content-type", "application/vnd.ferricstore.commands+msgpack"} in headers
    assert {:ok, %{"results" => [%{"status" => "ok", "value" => ^bytes}]}} = Msgpax.unpack(body)
  end

  test "returns stable authentication, method, and body-limit errors" do
    HttpHelpers.start_server()

    assert {401, headers, body} =
             HttpHelpers.request(
               :post,
               "/v1/commands",
               "application/json",
               [],
               ~s({"commands":[["PING"]]})
             )

    assert {"www-authenticate", ~s(Basic realm="FerricStore")} in headers
    assert %{"error" => %{"code" => "unauthenticated"}} = Jason.decode!(body)

    assert {405, headers, _body} = HttpHelpers.request(:get, "/v1/commands")
    assert {"allow", "POST"} in headers

    oversized = Jason.encode!(%{"commands" => [["ECHO", String.duplicate("x", 2_000)]]})

    assert {413, _headers, body} =
             HttpHelpers.request(
               :post,
               "/v1/commands",
               "application/json",
               [HttpHelpers.basic("worker", "secret")],
               oversized
             )

    assert %{"error" => %{"code" => "body_too_large"}} = Jason.decode!(body)
  end

  test "keeps invocation routes disabled unless configured" do
    HttpHelpers.start_server()

    assert {404, _headers, _body} =
             HttpHelpers.request(
               :post,
               "/v1/invocations/send-email",
               "application/json",
               [HttpHelpers.basic("worker", "secret")],
               ~s({"payload":{"to":"test@example.com"}})
             )
  end

  test "serves invocation, result, and scoped value routes when enabled" do
    HttpHelpers.start_server(invocations_enabled: true)
    auth = [HttpHelpers.basic("worker", "secret")]

    assert {202, _headers, body} =
             HttpHelpers.request(
               :post,
               "/v1/invocations/send-email",
               "application/json",
               auth,
               ~s({"payload":{"to":"test@example.com"}})
             )

    assert %{"invocation_id" => "inv-test", "state" => "queued"} = Jason.decode!(body)

    assert {200, _headers, body} =
             HttpHelpers.request(
               :get,
               "/v1/invocations/inv-test/result",
               "application/json",
               auth
             )

    assert %{
             "invocation_id" => "inv-test",
             "state" => "completed",
             "result" => %{"delivered" => true}
           } = Jason.decode!(body)

    assert {200, _headers, body} =
             HttpHelpers.request(
               :get,
               "/v1/invocations/inv-test/values/receipt",
               "application/json",
               auth
             )

    assert %{"name" => "receipt", "json" => %{"accepted" => true}} = Jason.decode!(body)

    assert {400, _headers, body} =
             HttpHelpers.request(
               :post,
               "/v1/invocations/inv-test/values/batch",
               "application/json",
               auth,
               ~s({"names":"receipt"})
             )

    assert %{"error" => %{"code" => "value_names_required"}} = Jason.decode!(body)

    assert {400, _headers, body} =
             HttpHelpers.request(
               :post,
               "/v1/invocations/inv-test/values",
               "application/json",
               auth,
               ~s({"name":"receipt","bytes_base64":"not base64"})
             )

    assert %{"error" => %{"code" => "invalid_value_encoding"}} = Jason.decode!(body)
  end

  test "serves health, readiness, bounded metrics, and reuses an HTTP/1.1 connection" do
    HttpHelpers.start_server()

    assert {200, _headers, ~s({"status":"ok"})} = HttpHelpers.request(:get, "/health")
    assert {200, _headers, ~s({"status":"ready"})} = HttpHelpers.request(:get, "/ready")

    assert {200, _headers, metrics} = HttpHelpers.request(:get, "/metrics")
    assert metrics =~ "ferricstore_http_requests_total"
    assert metrics =~ "ferricstore_http_in_flight_request_limit 1024"

    {:ok, socket} =
      :gen_tcp.connect({127, 0, 0, 1}, FerricstoreHttp.Listener.port(), [:binary, active: false])

    request = "GET /health HTTP/1.1\r\nhost: localhost\r\n\r\n"

    assert :ok = :gen_tcp.send(socket, request)
    assert {:ok, 200, _body} = receive_http_response(socket)
    assert :ok = :gen_tcp.send(socket, request)
    assert {:ok, 200, _body} = receive_http_response(socket)
    :ok = :gen_tcp.close(socket)
  end

  test "does not expose metrics when metrics are disabled" do
    HttpHelpers.start_server(metrics_enabled: false)

    assert {404, _headers, _body} = HttpHelpers.request(:get, "/metrics")
    refute Process.whereis(FerricstoreHttp.Metrics)
  end

  test "rejects excess in-flight requests and releases admission on disconnect" do
    HttpHelpers.start_server(max_in_flight_requests: 1)

    {:ok, blocked_socket} =
      :gen_tcp.connect({127, 0, 0, 1}, FerricstoreHttp.Listener.port(), [:binary, active: false])

    authorization = HttpHelpers.basic("worker", "secret") |> elem(1)

    partial_request =
      "POST /v1/commands HTTP/1.1\r\n" <>
        "host: localhost\r\n" <>
        "authorization: #{authorization}\r\n" <>
        "content-type: application/json\r\n" <>
        "content-length: 64\r\n\r\n"

    assert :ok = :gen_tcp.send(blocked_socket, partial_request)
    assert_eventually(fn -> FerricstoreHttp.Admission.stats().in_flight == 1 end)

    assert {503, headers, body} = HttpHelpers.request(:get, "/health")
    assert {"retry-after", "1"} in headers
    assert {"cache-control", "no-store"} in headers
    assert {"content-length", Integer.to_string(byte_size(body))} in headers
    assert %{"error" => %{"code" => "server_overloaded"}} = Jason.decode!(body)

    assert :ok = :gen_tcp.close(blocked_socket)
    assert_eventually(fn -> FerricstoreHttp.Admission.stats().in_flight == 0 end)
  end

  defp receive_http_response(socket) do
    with {:ok, headers, remaining} <- receive_headers(socket, ""),
         [status_line | header_lines] <- String.split(headers, "\r\n", trim: true),
         [_, status, _reason] <- String.split(status_line, " ", parts: 3),
         {content_length, ""} <- Integer.parse(header_value(header_lines, "content-length")),
         {:ok, body} <- receive_bytes(socket, remaining, content_length) do
      {:ok, String.to_integer(status), body}
    end
  end

  defp receive_headers(socket, buffer) do
    case :binary.match(buffer, "\r\n\r\n") do
      {index, 4} ->
        headers = binary_part(buffer, 0, index)
        remaining = binary_part(buffer, index + 4, byte_size(buffer) - index - 4)
        {:ok, headers, remaining}

      :nomatch ->
        with {:ok, bytes} <- :gen_tcp.recv(socket, 0, 2_000) do
          receive_headers(socket, buffer <> bytes)
        end
    end
  end

  defp receive_bytes(_socket, buffer, length) when byte_size(buffer) >= length,
    do: {:ok, binary_part(buffer, 0, length)}

  defp receive_bytes(socket, buffer, length) do
    with {:ok, bytes} <- :gen_tcp.recv(socket, length - byte_size(buffer), 2_000) do
      {:ok, buffer <> bytes}
    end
  end

  defp header_value(lines, expected_name) do
    Enum.find_value(lines, fn line ->
      case String.split(line, ":", parts: 2) do
        [name, value] -> matching_header_value(name, value, expected_name)
        _invalid -> nil
      end
    end)
  end

  defp matching_header_value(name, value, expected_name) do
    if String.downcase(name) == expected_name, do: String.trim(value)
  end

  defp assert_eventually(predicate, attempts \\ 100)

  defp assert_eventually(predicate, attempts) when attempts > 0 do
    if predicate.() do
      :ok
    else
      Process.sleep(10)
      assert_eventually(predicate, attempts - 1)
    end
  end

  defp assert_eventually(_predicate, 0), do: flunk("condition did not become true")
end
