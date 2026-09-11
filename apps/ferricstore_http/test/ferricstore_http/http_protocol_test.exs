defmodule FerricstoreHttp.HttpProtocolTest do
  use ExUnit.Case, async: false

  alias FerricstoreHttp.BinaryEnvelope
  alias FerricstoreHttp.ControlledTestBackend
  alias FerricstoreHttp.Test.HttpHelpers

  @msgpack_content_type "application/vnd.ferricstore.commands+msgpack"

  setup do
    start_supervised!(ControlledTestBackend)
    :ok
  end

  test "reuses one successful Basic authentication session across HTTP requests" do
    HttpHelpers.start_server(backend: ControlledTestBackend)
    authorization = HttpHelpers.basic("worker", "secret:with:colon")
    body = Jason.encode!(%{"commands" => [["PING"]]})

    assert {200, _headers, _body} =
             HttpHelpers.request(:post, "/v1/commands", "application/json", [authorization], body)

    assert {200, _headers, _body} =
             HttpHelpers.request(:post, "/v1/commands", "application/json", [authorization], body)

    assert Enum.count(ControlledTestBackend.calls(), &match?({:authenticate, _, _, _}, &1)) == 1
  end

  test "can disable authenticated session reuse" do
    HttpHelpers.start_server(backend: ControlledTestBackend, auth_cache_enabled: false)
    authorization = HttpHelpers.basic("worker", "secret:with:colon")
    body = Jason.encode!(%{"commands" => [["PING"]]})

    for _request <- 1..2 do
      assert {200, _headers, _body} =
               HttpHelpers.request(
                 :post,
                 "/v1/commands",
                 "application/json",
                 [authorization],
                 body
               )
    end

    assert Enum.count(ControlledTestBackend.calls(), &match?({:authenticate, _, _, _}, &1)) == 2
  end

  test "supports legacy JSON and preserves ordered per-command results" do
    HttpHelpers.start_server(backend: ControlledTestBackend, max_batch_commands: 17)

    body =
      Jason.encode!(%{
        "commands" => [
          ["PING"],
          ["ERROR", "ERR expected"],
          ["ATOM_ERROR", "wrongtype"],
          ["OPAQUE_ERROR"]
        ]
      })

    assert {200, headers, response_body} = request_json(body)
    assert header(headers, "content-type") == "application/json; charset=utf-8"

    assert %{
             "results" => [
               %{"status" => "ok", "value" => "PONG"},
               %{
                 "status" => "error",
                 "error" => %{"code" => "upstream_error", "message" => "ERR expected"}
               },
               %{
                 "status" => "error",
                 "error" => %{"code" => "error", "message" => "wrongtype"}
               },
               %{
                 "status" => "error",
                 "error" => %{"code" => "error", "message" => "FerricStore command failed"}
               }
             ]
           } = Jason.decode!(response_body)

    assert [
             {:authenticate, "worker", "secret:with:colon", peer_options},
             {:execute_batch, {:session, peer}, _commands, execute_options}
           ] = ControlledTestBackend.calls()

    assert Keyword.fetch!(peer_options, :peer) == peer
    assert execute_options[:max_commands] == 17
    assert execute_options[:deadline_ms] > System.system_time(:millisecond)
  end

  test "round-trips nested binary JSON values and non-string map keys" do
    HttpHelpers.start_server(backend: ControlledTestBackend)
    value = %{<<0, 255>> => [<<128>>, %{7 => true}]}

    body =
      Jason.encode!(%{
        "encoding" => BinaryEnvelope.encoding(),
        "commands" => BinaryEnvelope.encode([["ECHO", value]])
      })

    assert {200, _headers, response_body} = request_json(body)

    assert %{
             "encoding" => "ferricstore-json-v1",
             "results" => [%{"status" => "ok", "value" => encoded}]
           } = Jason.decode!(response_body)

    assert {:ok, ^value} = BinaryEnvelope.decode(encoded)
  end

  test "accepts case-insensitive MessagePack media types with parameters" do
    HttpHelpers.start_server(backend: ControlledTestBackend)
    bytes = <<0, 255, 128>>

    envelope = %{
      "encoding" => "ferricstore-msgpack-v1",
      "commands" => [["ECHO", Msgpax.Bin.new(bytes)]]
    }

    assert {200, headers, response_body} =
             HttpHelpers.request(
               :post,
               "/v1/commands",
               "Application/Vnd.FerricStore.Commands+MsgPack; version=1",
               [HttpHelpers.basic("worker", "secret:with:colon")],
               Msgpax.pack!(envelope)
             )

    assert header(headers, "content-type") == @msgpack_content_type

    assert {:ok,
            %{
              "encoding" => "ferricstore-msgpack-v1",
              "results" => [%{"status" => "ok", "value" => ^bytes}]
            }} = Msgpax.unpack(response_body)
  end

  test "returns format-matched stable errors for malformed payloads" do
    HttpHelpers.start_server(backend: ControlledTestBackend)

    json_cases = [
      {"{", "malformed_json"},
      {"[]", "malformed_json"},
      {~s({"not_commands":[]}), "malformed_envelope"},
      {~s({"encoding":"ferricstore-json-v1","commands":{"$ferricstore_bytes":"bad"}}),
       "malformed_binary_envelope"}
    ]

    Enum.each(json_cases, fn {body, code} ->
      assert {400, headers, response_body} = request_json(body)
      assert header(headers, "cache-control") == "no-store"
      assert %{"error" => %{"code" => ^code}} = Jason.decode!(response_body)
    end)

    for body <- [<<0xC1>>, Msgpax.pack!([])] do
      assert {400, headers, response_body} = request_msgpack(body)
      assert header(headers, "content-type") == @msgpack_content_type
      assert header(headers, "cache-control") == "no-store"

      assert {:ok, %{"error" => %{"code" => "malformed_msgpack"}}} =
               Msgpax.unpack(response_body)
    end
  end

  test "maps backend request failures without dispatching a partial success" do
    HttpHelpers.start_server(backend: ControlledTestBackend)

    cases = [
      {{:error, {:too_many_commands, 1}}, 413, "too_many_commands"},
      {{:error, {:malformed_command, 1}}, 400, "malformed_command"},
      {{:error, {:unsupported_command, 0, "MULTI"}}, 400, "unsupported_command"},
      {{:error, {:busy, :execution_budget}}, 503, "server_overloaded"},
      {{:error, :reauthentication_required}, 401, "unauthenticated"},
      {{:error, {:private_failure, "do not expose"}}, 500, "internal_error"}
    ]

    Enum.each(cases, fn {backend_result, expected_status, expected_code} ->
      ControlledTestBackend.put(:execute_result, backend_result)

      assert {^expected_status, headers, response_body} =
               request_json(~s({"commands":[["PING"]]}))

      assert header(headers, "cache-control") == "no-store"
      assert %{"error" => %{"code" => ^expected_code}} = Jason.decode!(response_body)
      refute response_body =~ "do not expose"
    end)
  end

  test "handles authentication outcomes and Basic passwords containing colons" do
    HttpHelpers.start_server(backend: ControlledTestBackend, auth_cache_enabled: false)

    assert {200, _headers, _body} = request_json(~s({"commands":[["PING"]]}))

    for auth_result <- [
          {:error, :unauthenticated},
          {:error, {:invalid_credentials, :disabled}},
          {:error, :reauthentication_required}
        ] do
      ControlledTestBackend.put(:auth_result, auth_result)

      assert {401, headers, response_body} = request_json(~s({"commands":[["PING"]]}))
      assert header(headers, "www-authenticate") == ~s(Basic realm="FerricStore")
      assert header(headers, "cache-control") == "no-store"
      assert %{"error" => %{"code" => "unauthenticated"}} = Jason.decode!(response_body)
    end

    ControlledTestBackend.put(:auth_result, {:error, {:rate_limited, 1_250}})
    assert {429, headers, _body} = request_json(~s({"commands":[["PING"]]}))
    assert header(headers, "retry-after") == "2"
  end

  test "rejects declared and streamed bodies above the configured limit" do
    HttpHelpers.start_server(backend: ControlledTestBackend, max_body_bytes: 48)
    oversized = Jason.encode!(%{"commands" => [["ECHO", String.duplicate("x", 64)]]})

    assert {413, headers, body} = request_json(oversized)
    assert header(headers, "cache-control") == "no-store"
    assert %{"error" => %{"code" => "body_too_large"}} = Jason.decode!(body)

    {:ok, socket} = HttpHelpers.connect()
    authorization = HttpHelpers.basic("worker", "secret:with:colon") |> elem(1)
    chunk = String.duplicate("x", 64)

    request =
      "POST /v1/commands HTTP/1.1\r\n" <>
        "host: localhost\r\n" <>
        "authorization: #{authorization}\r\n" <>
        "content-type: application/json\r\n" <>
        "transfer-encoding: chunked\r\n\r\n" <>
        "40\r\n#{chunk}\r\n0\r\n\r\n"

    assert {:ok, 413, headers, body} = HttpHelpers.raw_request(socket, request)
    assert header(headers, "cache-control") == "no-store"
    assert %{"error" => %{"code" => "body_too_large"}} = Jason.decode!(body)
    :ok = :gen_tcp.close(socket)
  end

  test "applies one request deadline while reading a stalled body" do
    HttpHelpers.start_server(backend: ControlledTestBackend, request_timeout_ms: 40)
    {:ok, socket} = HttpHelpers.connect()
    authorization = HttpHelpers.basic("worker", "secret:with:colon") |> elem(1)

    partial_request =
      "POST /v1/commands HTTP/1.1\r\n" <>
        "host: localhost\r\n" <>
        "authorization: #{authorization}\r\n" <>
        "content-type: application/json\r\n" <>
        "content-length: 64\r\n\r\n"

    assert {:ok, 408, headers, body} =
             HttpHelpers.raw_request(socket, partial_request, 1_000)

    assert header(headers, "cache-control") == "no-store"
    assert %{"error" => %{"code" => "request_timeout"}} = Jason.decode!(body)
    :ok = :gen_tcp.close(socket)
  end

  test "returns 405 with an explicit method contract and no-store" do
    HttpHelpers.start_server(backend: ControlledTestBackend)

    assert {405, headers, body} = HttpHelpers.request(:get, "/v1/commands")
    assert header(headers, "allow") == "POST"
    assert header(headers, "cache-control") == "no-store"
    assert %{"error" => %{"code" => "method_not_allowed"}} = Jason.decode!(body)
  end

  defp request_json(body) do
    HttpHelpers.request(
      :post,
      "/v1/commands",
      "application/json",
      [HttpHelpers.basic("worker", "secret:with:colon")],
      body
    )
  end

  defp request_msgpack(body) do
    HttpHelpers.request(
      :post,
      "/v1/commands",
      @msgpack_content_type,
      [HttpHelpers.basic("worker", "secret:with:colon")],
      body
    )
  end

  defp header(headers, name) do
    case List.keyfind(headers, name, 0) do
      {^name, value} -> value
      nil -> nil
    end
  end
end
