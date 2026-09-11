defmodule FerricstoreHttp.OssIntegrationTest do
  use ExUnit.Case, async: false

  @moduletag :oss_integration

  alias FerricstoreHttp.BinaryEnvelope
  alias FerricstoreHttp.Test.HttpHelpers
  alias FerricstoreServer.{Acl.CatalogProjector, AuthRateLimiter}

  test "authenticates and executes through the in-process OSS gateways" do
    :ok = CatalogProjector.mark_ready()
    username = "http-integration-#{System.unique_integer([:positive, :monotonic])}"
    password = "integration-secret"
    key = "http:integration:#{System.unique_integer([:positive, :monotonic])}"

    assert :ok =
             FerricstoreServer.Acl.set_user(username, [
               "on",
               ">#{password}",
               "+GET",
               "+SET",
               "~http:integration:*"
             ])

    HttpHelpers.start_server(backend: FerricstoreHttp.Backends.Ferricstore)

    envelope = %{
      "encoding" => BinaryEnvelope.encoding(),
      "commands" => BinaryEnvelope.encode([["SET", key, "value"], ["GET", key]])
    }

    assert {200, _headers, body} =
             HttpHelpers.request(
               :post,
               "/v1/commands",
               "application/json",
               [HttpHelpers.basic(username, password)],
               Jason.encode!(envelope)
             )

    assert %{"results" => [%{"status" => "ok"}, %{"status" => "ok", "value" => encoded_value}]} =
             Jason.decode!(body)

    assert {:ok, "value"} = BinaryEnvelope.decode(encoded_value)
  end

  test "enforces FerricStore ACL key scopes while preserving ordered results" do
    :ok = CatalogProjector.mark_ready()
    unique = System.unique_integer([:positive, :monotonic])
    username = "http-acl-#{unique}"
    password = "integration-secret"
    allowed_key = "http:allowed:#{unique}"
    denied_key = "http:denied:#{unique}"

    assert :ok =
             FerricstoreServer.Acl.set_user(username, [
               "on",
               ">#{password}",
               "+GET",
               "+SET",
               "~http:allowed:*"
             ])

    HttpHelpers.start_server(backend: FerricstoreHttp.Backends.Ferricstore)

    response =
      command_request(username, password, [
        ["SET", allowed_key, "value"],
        ["GET", allowed_key],
        ["GET", denied_key]
      ])

    assert {200, _headers, body} = response

    assert %{
             "results" => [
               %{"status" => "ok"},
               %{"status" => "ok", "value" => encoded_value},
               %{"status" => "error", "error" => %{"code" => "noperm", "message" => message}}
             ]
           } = Jason.decode!(body)

    assert {:ok, "value"} = BinaryEnvelope.decode(encoded_value)
    assert String.starts_with?(message, "NOPERM")
  end

  test "executes a blocking command inside one ordered HTTP pipeline" do
    :ok = CatalogProjector.mark_ready()
    unique = System.unique_integer([:positive, :monotonic])
    username = "http-blocking-#{unique}"
    password = "integration-secret"
    key = "http:blocking:#{unique}"
    configure_all_command_user(username, password, "~http:blocking:*")
    assert {:ok, 1} = FerricStore.rpush(key, ["ready"])
    HttpHelpers.start_server(backend: FerricstoreHttp.Backends.Ferricstore)

    assert {200, _headers, body} =
             command_request(username, password, [
               ["PING"],
               ["BLPOP", key, "0"],
               ["PING"]
             ])

    assert %{
             "results" => [
               %{"status" => "ok", "value" => encoded_ping_before},
               %{"status" => "ok", "value" => encoded_pop},
               %{"status" => "ok", "value" => encoded_ping_after}
             ]
           } = Jason.decode!(body)

    assert {:ok, "PONG"} = BinaryEnvelope.decode(encoded_ping_before)
    assert {:ok, [^key, "ready"]} = BinaryEnvelope.decode(encoded_pop)
    assert {:ok, "PONG"} = BinaryEnvelope.decode(encoded_ping_after)
  end

  test "rejects an invalid stateless batch before executing any command" do
    :ok = CatalogProjector.mark_ready()
    unique = System.unique_integer([:positive, :monotonic])
    username = "http-stateless-#{unique}"
    password = "integration-secret"
    key = "http:stateless:#{unique}"
    configure_all_command_user(username, password, "~http:stateless:*")
    HttpHelpers.start_server(backend: FerricstoreHttp.Backends.Ferricstore)

    assert {400, _headers, body} =
             command_request(username, password, [["SET", key, "must-not-exist"], ["MULTI"]])

    assert %{
             "error" => %{
               "code" => "unsupported_command",
               "index" => 1,
               "command" => "MULTI"
             }
           } = Jason.decode!(body)

    assert {200, _headers, body} = command_request(username, password, [["GET", key]])

    assert %{"results" => [%{"status" => "ok", "value" => encoded_nil}]} =
             Jason.decode!(body)

    assert {:ok, nil} = BinaryEnvelope.decode(encoded_nil)
  end

  test "enforces the configured batch limit before execution" do
    :ok = CatalogProjector.mark_ready()
    unique = System.unique_integer([:positive, :monotonic])
    username = "http-batch-#{unique}"
    password = "integration-secret"
    key = "http:batch:#{unique}"
    configure_all_command_user(username, password, "~http:batch:*")

    HttpHelpers.start_server(
      backend: FerricstoreHttp.Backends.Ferricstore,
      max_batch_commands: 1
    )

    assert {413, _headers, body} =
             command_request(username, password, [
               ["SET", key, "first"],
               ["SET", key, "second"]
             ])

    assert %{"error" => %{"code" => "too_many_commands", "max" => 1}} = Jason.decode!(body)

    assert {200, _headers, body} = command_request(username, password, [["GET", key]])
    assert %{"results" => [%{"status" => "ok", "value" => encoded_nil}]} = Jason.decode!(body)
    assert {:ok, nil} = BinaryEnvelope.decode(encoded_nil)
  end

  test "rejects ambiguous structured command envelopes at the HTTP backend boundary" do
    :ok = CatalogProjector.mark_ready()
    unique = System.unique_integer([:positive, :monotonic])
    username = "http-structured-#{unique}"
    password = "integration-secret"
    configure_all_command_user(username, password, "~*")
    HttpHelpers.start_server(backend: FerricstoreHttp.Backends.Ferricstore)

    descriptor = %{
      "command" => "FLOW.START_AND_CLAIM",
      "opcode" => 0x0223,
      "payload" => %{
        "id" => "http:structured:#{unique}",
        "type" => "http-integration",
        "initial_state" => "queued",
        "worker" => "http-worker",
        "partition_key" => "http:structured:#{unique}"
      },
      "ignored" => true
    }

    assert {400, _headers, body} = command_request(username, password, [descriptor])

    assert %{"error" => %{"code" => "malformed_command", "index" => 0}} =
             Jason.decode!(body)
  end

  test "revalidates password rotation on the next HTTP request" do
    :ok = CatalogProjector.mark_ready()
    unique = System.unique_integer([:positive, :monotonic])
    username = "http-rotation-#{unique}"
    old_password = "old-secret"
    new_password = "new-secret"
    configure_all_command_user(username, old_password, "~*")
    HttpHelpers.start_server(backend: FerricstoreHttp.Backends.Ferricstore)

    assert {200, _headers, _body} = command_request(username, old_password, [["PING"]])

    assert :ok =
             FerricstoreServer.Acl.set_user(username, [
               "resetpass",
               ">#{new_password}"
             ])

    assert {401, _headers, body} = command_request(username, old_password, [["PING"]])
    assert %{"error" => %{"code" => "unauthenticated"}} = Jason.decode!(body)
    assert {200, _headers, _body} = command_request(username, new_password, [["PING"]])
  end

  test "creates and reads a real Flow-backed invocation through HTTP" do
    :ok = CatalogProjector.mark_ready()
    unique = System.unique_integer([:positive, :monotonic])
    name = "http-invocation-#{unique}"
    password = "integration-secret"
    admin = "http-invocation-admin-#{unique}"

    assert :ok =
             FerricstoreServer.Acl.set_user(admin, [
               "on",
               ">#{password}",
               "-@all",
               "+INVOCATION.DEFINITION.PUT",
               "~invocation:definition:*"
             ])

    assert {:ok, admin_session} =
             FerricstoreServer.AuthenticationGateway.authenticate(admin, password,
               peer: {{127, 0, 0, 1}, 40_001}
             )

    definition = %{
      "name" => name,
      "enabled" => true,
      "flow_type" => "invocation:#{name}",
      "initial_state" => "queued",
      "target" => %{"kind" => "http_endpoint", "url" => "https://hooks.example.test"},
      "limits" => %{"idempotency_required" => true},
      "partition" => %{"key" => "invocation:{name}:{subject}"}
    }

    assert {:ok, [%{status: :ok, value: "OK"}]} =
             FerricstoreServer.CommandGateway.execute_batch(admin_session, [
               ["INVOCATION.DEFINITION.PUT", Jason.encode!(definition)]
             ])

    caller = "http-invocation-caller-#{unique}"

    assert :ok =
             FerricstoreServer.Acl.set_user(caller, [
               "on",
               ">#{password}",
               "-@all",
               "+INVOCATION.CREATE",
               "+INVOCATION.GET",
               "+FLOW.GET",
               "~invocation:#{name}",
               "~invocation:#{name}:*"
             ])

    HttpHelpers.start_server(
      backend: FerricstoreHttp.Backends.Ferricstore,
      invocations_enabled: true
    )

    headers = [HttpHelpers.basic(caller, password)]
    expected_partition = "invocation:#{name}:#{caller}"

    assert {202, _headers, body} =
             HttpHelpers.request(
               :post,
               "/v1/invocations/#{name}",
               "application/json",
               [{"idempotency-key", "request-1"} | headers],
               Jason.encode!(%{"payload" => %{"message" => "hello"}})
             )

    assert %{
             "invocation_id" => invocation_id,
             "state" => "queued",
             "partition_key" => ^expected_partition,
             "subject" => ^caller
           } = Jason.decode!(body)

    assert length(String.split(invocation_id, ".")) == 4

    assert {200, _headers, body} =
             HttpHelpers.request(
               :get,
               "/v1/invocations/#{invocation_id}",
               "application/json",
               headers
             )

    assert %{
             "id" => ^invocation_id,
             "state" => "queued",
             "payload" => %{"message" => "hello"},
             "attributes" => %{"invocation_name" => ^name, "invocation_subject" => ^caller}
           } = Jason.decode!(body)
  end

  test "uses a narrow invocation value-policy projection without granting definition access" do
    :ok = CatalogProjector.mark_ready()
    unique = System.unique_integer([:positive, :monotonic])
    name = "http-values-#{unique}"
    password = "integration-secret"
    admin = "http-values-admin-#{unique}"

    assert :ok =
             FerricstoreServer.Acl.set_user(admin, [
               "on",
               ">#{password}",
               "-@all",
               "+INVOCATION.DEFINITION.PUT",
               "~invocation:definition:*"
             ])

    assert {:ok, admin_session} =
             FerricstoreServer.AuthenticationGateway.authenticate(admin, password,
               peer: {{127, 0, 0, 1}, 40_002}
             )

    allowed_names = ["receipt" | Enum.map(1..24, &"allowed-result-value-#{&1}")]

    definition = %{
      "name" => name,
      "enabled" => true,
      "flow_type" => "invocation:#{name}",
      "initial_state" => "queued",
      "target" => %{"kind" => "http_endpoint", "url" => "https://hooks.example.test"},
      "refs" => %{
        "allowed_read_names" => allowed_names,
        "allowed_write_names" => allowed_names,
        "private_marker" => "must-not-cross-the-invocation-boundary"
      },
      "partition" => %{"key" => "invocation:{name}:{subject}"}
    }

    assert {:ok, [%{status: :ok, value: "OK"}]} =
             FerricstoreServer.CommandGateway.execute_batch(admin_session, [
               ["INVOCATION.DEFINITION.PUT", Jason.encode!(definition)]
             ])

    caller = "http-values-caller-#{unique}"

    assert :ok =
             FerricstoreServer.Acl.set_user(caller, [
               "on",
               ">#{password}",
               "-@all",
               "+INVOCATION.CREATE",
               "+INVOCATION.GET",
               "+FLOW.GET",
               "+FLOW.VALUE.PUT",
               "+FLOW.VALUE.MGET",
               "~*"
             ])

    HttpHelpers.start_server(
      backend: FerricstoreHttp.Backends.Ferricstore,
      invocations_enabled: true
    )

    headers = [HttpHelpers.basic(caller, password)]

    assert {202, _headers, body} =
             HttpHelpers.request(
               :post,
               "/v1/invocations/#{name}",
               "application/json",
               headers,
               Jason.encode!(%{"payload" => %{"message" => "hello"}})
             )

    %{"invocation_id" => invocation_id} = Jason.decode!(body)

    assert {201, _headers, body} =
             HttpHelpers.request(
               :post,
               "/v1/invocations/#{invocation_id}/values",
               "application/json",
               headers,
               Jason.encode!(%{"name" => "receipt", "json" => %{"accepted" => true}})
             )

    assert %{"name" => "receipt", "ref" => ref} = Jason.decode!(body)
    assert is_binary(ref)

    assert {200, _headers, body} =
             HttpHelpers.request(
               :get,
               "/v1/invocations/#{invocation_id}/values/receipt",
               "application/json",
               headers
             )

    assert %{
             "name" => "receipt",
             "content_type" => "application/json",
             "json" => %{"accepted" => true}
           } = Jason.decode!(body)

    assert {200, _headers, body} =
             command_request(caller, password, [["INVOCATION.GET", invocation_id]])

    assert %{"results" => [%{"status" => "ok", "value" => encoded_metadata}]} =
             Jason.decode!(body)

    assert {:ok, metadata} = BinaryEnvelope.decode(encoded_metadata)
    refute get_in(metadata, ["value_policy", "refs", "private_marker"])
    refute Map.has_key?(metadata, "target")

    assert {200, _headers, body} =
             command_request(caller, password, [["INVOCATION.DEFINITION.GET", name]])

    assert %{"results" => [%{"status" => "error", "error" => %{"code" => "noperm"}}]} =
             Jason.decode!(body)
  end

  test "shares the OSS authentication limiter and returns retry metadata" do
    previous_max_attempts = Application.get_env(:ferricstore, :auth_rate_limit_max_attempts)
    previous_window_ms = Application.get_env(:ferricstore, :auth_rate_limit_window_ms)

    Application.put_env(:ferricstore, :auth_rate_limit_max_attempts, 1)
    Application.put_env(:ferricstore, :auth_rate_limit_window_ms, 60_000)
    :ok = AuthRateLimiter.reset()

    on_exit(fn ->
      restore_env(:auth_rate_limit_max_attempts, previous_max_attempts)
      restore_env(:auth_rate_limit_window_ms, previous_window_ms)
      AuthRateLimiter.reset()
    end)

    :ok = CatalogProjector.mark_ready()
    unique = System.unique_integer([:positive, :monotonic])
    username = "http-rate-limit-#{unique}"
    password = "integration-secret"
    configure_all_command_user(username, password, "~*")
    HttpHelpers.start_server(backend: FerricstoreHttp.Backends.Ferricstore)

    assert {401, _headers, body} =
             command_request(username, "#{password}-invalid", [["PING"]])

    assert %{"error" => %{"code" => "unauthenticated"}} = Jason.decode!(body)

    assert {429, headers, body} = command_request(username, password, [["PING"]])
    assert List.keyfind(headers, "retry-after", 0) == {"retry-after", "60"}
    assert %{"error" => %{"code" => "rate_limited"}} = Jason.decode!(body)
  end

  defp command_request(username, password, commands) do
    envelope = %{
      "encoding" => BinaryEnvelope.encoding(),
      "commands" => BinaryEnvelope.encode(commands)
    }

    HttpHelpers.request(
      :post,
      "/v1/commands",
      "application/json",
      [HttpHelpers.basic(username, password)],
      Jason.encode!(envelope)
    )
  end

  defp configure_all_command_user(username, password, key_pattern) do
    assert :ok =
             FerricstoreServer.Acl.set_user(username, [
               "on",
               "resetpass",
               ">#{password}",
               "resetkeys",
               "+@all",
               key_pattern
             ])
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
