defmodule FerricstoreServer.Spec.HttpEndpointsTest do
  @moduledoc """
  Spec section 9: HTTP Endpoints tests.

  Verifies the HTTP health, liveness, metrics, and dashboard endpoints
  exposed by `FerricstoreServer.Health.Endpoint`. All requests are made over
  raw TCP to avoid adding an HTTP client dependency.

  Covers:
    - GET /health/ready returns 200 with JSON {"status":"ok"} when ready
    - GET /health/ready returns 503 when not ready
    - GET /health/live returns 200 always
    - GET /metrics returns Prometheus text format with key metrics
    - GET /metrics contains ferricstore_connected_clients
    - GET /metrics contains ferricstore_used_memory_bytes
    - GET /dashboard returns HTML with <!DOCTYPE html>
    - GET /unknown returns 404
    - Response headers include Content-Type
  """

  use ExUnit.Case, async: false

  alias FerricstoreServer.Health.Endpoint, as: HealthEndpoint

  setup do
    protected_mode = Application.get_env(:ferricstore, :protected_mode)
    Application.put_env(:ferricstore, :protected_mode, false)

    on_exit(fn ->
      Ferricstore.Health.set_ready(true)

      if is_nil(protected_mode) do
        Application.delete_env(:ferricstore, :protected_mode)
      else
        Application.put_env(:ferricstore, :protected_mode, protected_mode)
      end
    end)

    :ok
  end

  # ---------------------------------------------------------------------------
  # HTTP helpers
  # ---------------------------------------------------------------------------

  defp http_get(port, path) do
    {:ok, conn} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    :ok =
      :gen_tcp.send(
        conn,
        "GET #{path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
      )

    response = recv_all(conn, "")
    :gen_tcp.close(conn)
    response
  end

  defp recv_all(conn, acc) do
    case :gen_tcp.recv(conn, 0, 5_000) do
      {:ok, data} -> recv_all(conn, acc <> data)
      {:error, :closed} -> acc
    end
  end

  defp extract_headers(response) do
    case String.split(response, "\r\n\r\n", parts: 2) do
      [headers, _body] -> headers
      _ -> response
    end
  end

  defp extract_body(response) do
    case String.split(response, "\r\n\r\n", parts: 2) do
      [_headers, body] -> body
      _ -> response
    end
  end

  defp extract_status_code(response) do
    case String.split(response, "\r\n", parts: 2) do
      [status_line, _rest] ->
        case Regex.run(~r/HTTP\/1\.\d\s+(\d+)/, status_line) do
          [_, code] -> String.to_integer(code)
          _ -> nil
        end

      _ ->
        nil
    end
  end

  # ---------------------------------------------------------------------------
  # GET /health/ready
  # ---------------------------------------------------------------------------

  describe "GET /health/ready" do
    test "returns 200 with JSON {\"status\":\"ok\"} when ready" do
      # Ensure readiness is true (should be the default after app start)
      Ferricstore.Health.set_ready(true)

      port = HealthEndpoint.port()
      response = http_get(port, "/health/ready")

      assert extract_status_code(response) == 200
      assert response =~ "HTTP/1.1 200 OK"

      body = extract_body(response)
      {:ok, decoded} = Jason.decode(body)
      assert decoded["status"] == "ok"
    end

    test "returns 503 when not ready" do
      # Set not ready, test, then restore
      Ferricstore.Health.set_ready(false)

      port = HealthEndpoint.port()
      response = http_get(port, "/health/ready")

      assert extract_status_code(response) == 503
      assert response =~ "HTTP/1.1 503 Service Unavailable"

      body = extract_body(response)
      {:ok, decoded} = Jason.decode(body)
      assert decoded["status"] == "starting"

      # Restore ready state
      Ferricstore.Health.set_ready(true)
    end

    test "response has application/json Content-Type header" do
      port = HealthEndpoint.port()
      response = http_get(port, "/health/ready")

      headers = extract_headers(response)
      assert String.contains?(headers, "Content-Type: application/json")
    end

    test "response body is valid JSON with expected fields" do
      port = HealthEndpoint.port()
      response = http_get(port, "/health/ready")
      body = extract_body(response)

      {:ok, decoded} = Jason.decode(body)
      assert Map.has_key?(decoded, "status")
      assert Map.has_key?(decoded, "shard_count")
      assert Map.has_key?(decoded, "shards")
      assert Map.has_key?(decoded, "uptime_seconds")
    end
  end

  # ---------------------------------------------------------------------------
  # GET /health/live
  # ---------------------------------------------------------------------------

  describe "GET /health/live" do
    test "returns 200 always" do
      port = HealthEndpoint.port()
      response = http_get(port, "/health/live")

      assert extract_status_code(response) == 200
      assert response =~ "HTTP/1.1 200 OK"
    end

    test "returns 200 even when readiness is false" do
      Ferricstore.Health.set_ready(false)

      port = HealthEndpoint.port()
      response = http_get(port, "/health/live")

      assert extract_status_code(response) == 200
      assert response =~ "HTTP/1.1 200 OK"

      body = extract_body(response)
      {:ok, decoded} = Jason.decode(body)
      assert decoded["status"] == "alive"

      Ferricstore.Health.set_ready(true)
    end

    test "response has application/json Content-Type header" do
      port = HealthEndpoint.port()
      response = http_get(port, "/health/live")

      headers = extract_headers(response)
      assert String.contains?(headers, "Content-Type: application/json")
    end

    test "response body contains status alive" do
      port = HealthEndpoint.port()
      response = http_get(port, "/health/live")
      body = extract_body(response)

      {:ok, decoded} = Jason.decode(body)
      assert decoded == %{"status" => "alive"}
    end
  end

  # ---------------------------------------------------------------------------
  # GET /metrics
  # ---------------------------------------------------------------------------

  describe "GET /metrics" do
    test "returns 200 with Prometheus text format" do
      port = HealthEndpoint.port()
      response = http_get(port, "/metrics")

      assert extract_status_code(response) == 200
      assert response =~ "HTTP/1.1 200 OK"

      body = extract_body(response)
      # Prometheus text format has HELP and TYPE lines
      assert String.contains?(body, "# HELP")
      assert String.contains?(body, "# TYPE")
    end

    test "contains ferricstore_connected_clients metric" do
      port = HealthEndpoint.port()
      response = http_get(port, "/metrics")
      body = extract_body(response)

      assert String.contains?(body, "ferricstore_connected_clients")
      assert String.contains?(body, "# HELP ferricstore_connected_clients")
      assert String.contains?(body, "# TYPE ferricstore_connected_clients gauge")
    end

    test "contains ferricstore_used_memory_bytes metric" do
      port = HealthEndpoint.port()
      response = http_get(port, "/metrics")
      body = extract_body(response)

      assert String.contains?(body, "ferricstore_used_memory_bytes")
      assert String.contains?(body, "# HELP ferricstore_used_memory_bytes")
      assert String.contains?(body, "# TYPE ferricstore_used_memory_bytes gauge")
    end

    test "response has text/plain Content-Type header for Prometheus scraping" do
      port = HealthEndpoint.port()
      response = http_get(port, "/metrics")

      headers = extract_headers(response)
      # Prometheus expects text/plain or text/plain; version=0.0.4
      assert String.contains?(headers, "Content-Type: text/plain")
    end

    test "metrics body contains all core metric families" do
      port = HealthEndpoint.port()
      response = http_get(port, "/metrics")
      body = extract_body(response)

      core_metrics = [
        "ferricstore_connected_clients",
        "ferricstore_total_connections_received",
        "ferricstore_total_commands_processed",
        "ferricstore_used_memory_bytes",
        "ferricstore_uptime_seconds"
      ]

      for metric <- core_metrics do
        assert String.contains?(body, metric),
               "Expected metric #{metric} in /metrics response"
      end
    end

    test "metrics body ends with a trailing newline" do
      port = HealthEndpoint.port()
      response = http_get(port, "/metrics")
      body = extract_body(response)

      assert String.ends_with?(body, "\n")
    end
  end

  # ---------------------------------------------------------------------------
  # GET /dashboard
  # ---------------------------------------------------------------------------

  describe "GET /dashboard" do
    test "returns HTML with <!DOCTYPE html>" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard")

      assert extract_status_code(response) == 200
      body = extract_body(response)
      assert String.starts_with?(body, "<!DOCTYPE html>")
    end

    test "response has text/html Content-Type header" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard")

      headers = extract_headers(response)
      assert String.contains?(headers, "Content-Type: text/html; charset=utf-8")
    end

    test "response body is a complete HTML document" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard")
      body = extract_body(response)

      assert String.contains?(body, "<html")
      assert String.contains?(body, "</html>")
      assert String.contains?(body, "<title>FerricStore Dashboard</title>")
    end

    test "overview dashboard is a live shell without full-page meta refresh" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard")
      body = extract_body(response)

      refute String.contains?(body, ~s(http-equiv="refresh"))
      assert String.contains?(body, ~s(data-dashboard-live-page="overview"))
      assert String.contains?(body, ~s(data-dashboard-live-url="/dashboard/api/overview"))
      assert String.contains?(body, "dashboard-live.js")
    end

    test "flow dashboard is a live shell without full-page meta refresh" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard/flow")
      body = extract_body(response)

      refute String.contains?(body, ~s(http-equiv="refresh"))
      assert String.contains?(body, ~s(data-dashboard-live-page="flow"))
      assert String.contains?(body, ~s(data-dashboard-live-url="/dashboard/api/flow"))
      assert String.contains?(body, "dashboard-live.js")
    end
  end

  describe "GET /dashboard/api" do
    test "overview live endpoint returns component JSON" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard/api/overview")

      assert extract_status_code(response) == 200
      assert extract_headers(response) =~ "Content-Type: application/json"

      {:ok, decoded} = Jason.decode(extract_body(response))
      assert is_integer(decoded["generated_at_ms"])
      assert is_map(decoded["components"])
      assert is_binary(decoded["components"]["top_bar"])
      assert is_binary(decoded["components"]["content"])
      assert is_binary(decoded["components"]["footer"])
    end

    test "flow live endpoint returns component JSON" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard/api/flow")

      assert extract_status_code(response) == 200
      assert extract_headers(response) =~ "Content-Type: application/json"

      {:ok, decoded} = Jason.decode(extract_body(response))
      assert is_integer(decoded["generated_at_ms"])
      assert is_map(decoded["components"])
      assert is_binary(decoded["components"]["flow_overview"])
      assert is_binary(decoded["components"]["flow_recent_records"])
    end

    test "all live dashboard component endpoints return JSON payloads" do
      port = HealthEndpoint.port()

      for path <- [
            "/dashboard/api/slowlog",
            "/dashboard/api/merge",
            "/dashboard/api/raft",
            "/dashboard/api/clients",
            "/dashboard/api/storage",
            "/dashboard/api/prefixes",
            "/dashboard/api/flow/states",
            "/dashboard/api/flow/workers",
            "/dashboard/api/flow/due"
          ] do
        response = http_get(port, path)

        assert extract_status_code(response) == 200,
               "expected #{path} to return 200"

        assert extract_headers(response) =~ "Content-Type: application/json"

        {:ok, decoded} = Jason.decode(extract_body(response))
        assert is_integer(decoded["generated_at_ms"])
        assert is_map(decoded["components"])
        assert map_size(decoded["components"]) > 0
      end
    end
  end

  # ---------------------------------------------------------------------------
  # GET /unknown — 404 handling
  # ---------------------------------------------------------------------------

  describe "GET /unknown" do
    test "returns 404 for unknown paths" do
      port = HealthEndpoint.port()
      response = http_get(port, "/unknown")

      assert extract_status_code(response) == 404
      assert response =~ "HTTP/1.1 404 Not Found"
    end

    test "returns 404 for /nonexistent path" do
      port = HealthEndpoint.port()
      response = http_get(port, "/nonexistent")

      assert extract_status_code(response) == 404
    end

    test "404 response has application/json Content-Type" do
      port = HealthEndpoint.port()
      response = http_get(port, "/unknown")

      headers = extract_headers(response)
      assert String.contains?(headers, "Content-Type: application/json")
    end

    test "404 response body is valid JSON" do
      port = HealthEndpoint.port()
      response = http_get(port, "/unknown")
      body = extract_body(response)

      {:ok, decoded} = Jason.decode(body)
      assert Map.has_key?(decoded, "error")
    end
  end

  # ---------------------------------------------------------------------------
  # Response headers — Content-Type always present
  # ---------------------------------------------------------------------------

  describe "response headers include Content-Type" do
    test "all JSON endpoints include Content-Type header" do
      port = HealthEndpoint.port()

      for path <- ["/health/ready", "/health/live"] do
        response = http_get(port, path)
        headers = extract_headers(response)

        assert String.contains?(headers, "Content-Type:"),
               "Expected Content-Type header in response for #{path}"
      end
    end

    test "dashboard endpoint includes Content-Type header" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard")
      headers = extract_headers(response)

      assert String.contains?(headers, "Content-Type:"),
             "Expected Content-Type header in response for /dashboard"
    end

    test "metrics endpoint includes Content-Type header" do
      port = HealthEndpoint.port()
      response = http_get(port, "/metrics")
      headers = extract_headers(response)

      assert String.contains?(headers, "Content-Type:"),
             "Expected Content-Type header in response for /metrics"
    end

    test "favicon request does not produce browser console 404 noise" do
      port = HealthEndpoint.port()
      response = http_get(port, "/favicon.ico")

      assert response =~ "HTTP/1.1 204 No Content"
      assert response =~ "Content-Length: 0"
    end

    test "all responses include Content-Length header" do
      port = HealthEndpoint.port()

      for path <- ["/health/ready", "/health/live", "/dashboard", "/metrics"] do
        response = http_get(port, path)
        headers = extract_headers(response)

        assert String.contains?(headers, "Content-Length:"),
               "Expected Content-Length header in response for #{path}"
      end
    end
  end
end
