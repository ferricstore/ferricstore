defmodule FerricstoreServer.Health.DashboardTest do
  @moduledoc """
  Tests for the built-in HTML dashboard (spec 7.3).

  Covers:
    - Dashboard data collection from all subsystems
    - HTML rendering with all expected sections
    - HTTP endpoint serving at /dashboard
    - Live component updates without full-page refresh
    - Content-Type header is text/html
    - Graceful degradation when subsystems are unavailable
  """

  use ExUnit.Case, async: false
  @moduletag timeout: 60_000

  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Endpoint, as: HealthEndpoint
  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Test.ShardHelpers

  setup do
    protected_mode = Application.get_env(:ferricstore, :protected_mode)

    Application.put_env(:ferricstore, :protected_mode, false)
    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      restore_env(:protected_mode, protected_mode)
      Ferricstore.NamespaceConfig.reset_all()
      FerricstoreServer.Acl.reset!()
    end)

    :ok
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  def handle_dashboard_flow_lookup_event(event, measurements, metadata, parent) do
    send(parent, {:dashboard_flow_lookup, event, measurements, metadata})
  end

  # ---------------------------------------------------------------------------
  # HTTP helpers
  # ---------------------------------------------------------------------------

  defp http_get(port, path) do
    http_get(port, path, [])
  end

  defp http_get(port, path, headers) do
    {:ok, conn} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    header_lines =
      headers
      |> Enum.map(fn {name, value} -> "#{name}: #{value}\r\n" end)
      |> Enum.join()

    :ok =
      :gen_tcp.send(
        conn,
        "GET #{path} HTTP/1.1\r\nHost: localhost\r\n" <>
          header_lines <>
          "Connection: close\r\n\r\n"
      )

    response = recv_all(conn, "")
    :gen_tcp.close(conn)
    response
  end

  defp http_post_form(port, path, params) do
    http_post_form(port, path, params, [])
  end

  defp http_post_form(port, path, params, headers) do
    body = URI.encode_query(params)

    {:ok, conn} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    header_lines =
      headers
      |> Enum.map(fn {name, value} -> "#{name}: #{value}\r\n" end)
      |> Enum.join()

    request =
      "POST #{path} HTTP/1.1\r\n" <>
        "Host: localhost\r\n" <>
        header_lines <>
        "Content-Type: application/x-www-form-urlencoded\r\n" <>
        "Content-Length: #{byte_size(body)}\r\n" <>
        "Connection: close\r\n" <>
        "\r\n" <>
        body

    :ok = :gen_tcp.send(conn, request)

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

  defp extract_body(response) do
    case String.split(response, "\r\n\r\n", parts: 2) do
      [_headers, body] -> body
      _ -> response
    end
  end

  defp extract_headers(response) do
    case String.split(response, "\r\n\r\n", parts: 2) do
      [headers, _body] -> headers
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

  defp extract_header(response, name) do
    downcased = String.downcase(name)

    response
    |> extract_headers()
    |> String.split("\r\n")
    |> Enum.find_value(fn line ->
      case String.split(line, ":", parts: 2) do
        [header, value] ->
          if String.downcase(header) == downcased, do: String.trim(value), else: nil

        _ ->
          nil
      end
    end)
  end

  defp dashboard_session_cookie(response) do
    response
    |> extract_header("set-cookie")
    |> String.split(";", parts: 2)
    |> hd()
  end

  # ---------------------------------------------------------------------------
  # Dashboard.collect/0
  # ---------------------------------------------------------------------------

  describe "collect/0" do
    test "returns a map with all expected keys" do
      data = Dashboard.collect()

      assert is_map(data)
      assert Map.has_key?(data, :overview)
      assert Map.has_key?(data, :shards)
      assert Map.has_key?(data, :hotcold)
      assert Map.has_key?(data, :memory)
      assert Map.has_key?(data, :connections)
      assert Map.has_key?(data, :slowlog)
      assert Map.has_key?(data, :merge)
    end

    test "overview contains status, uptime, keys, and memory" do
      data = Dashboard.collect()
      overview = data.overview

      assert overview.status in [:ok, :starting]
      assert is_integer(overview.uptime_seconds)
      assert overview.uptime_seconds >= 0
      assert is_integer(overview.total_keys)
      assert overview.total_keys >= 0
      assert is_integer(overview.memory_bytes)
      assert overview.memory_bytes > 0
      assert is_binary(overview.run_id)
      assert byte_size(overview.run_id) == 40
    end

    test "shards is a list matching shard_count" do
      data = Dashboard.collect()
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      assert length(data.shards) == shard_count

      for shard <- data.shards do
        assert is_integer(shard.index)
        assert shard.status in ["ok", "down"]
        assert is_integer(shard.keys)
        assert is_integer(shard.ets_memory_bytes)
      end
    end

    test "hotcold contains read metrics" do
      data = Dashboard.collect()
      hc = data.hotcold

      assert is_float(hc.hot_read_pct)
      assert hc.hot_read_pct >= 0.0 and hc.hot_read_pct <= 100.0
      assert is_float(hc.cold_reads_per_sec)
      assert is_integer(hc.total_hot)
      assert is_integer(hc.total_cold)
      assert is_list(hc.top_prefixes)
    end

    test "memory contains pressure level and eviction policy" do
      data = Dashboard.collect()
      mem = data.memory

      assert mem.pressure_level in [:ok, :warning, :pressure, :reject]
      assert is_atom(mem.eviction_policy)
      assert is_integer(mem.total_bytes)
      assert is_integer(mem.max_bytes)
      assert is_float(mem.ratio)
      assert is_map(mem.shards)
    end

    test "connections contains active, blocked, and tracking counts" do
      data = Dashboard.collect()
      conns = data.connections

      assert is_integer(conns.active)
      assert conns.active >= 0
      assert is_integer(conns.blocked)
      assert conns.blocked >= 0
      assert is_integer(conns.tracking)
      assert conns.tracking >= 0
    end

    test "slowlog is a list of entry maps" do
      data = Dashboard.collect()
      assert is_list(data.slowlog)

      for entry <- data.slowlog do
        assert is_integer(entry.id)
        assert is_integer(entry.timestamp_us)
        assert is_integer(entry.duration_us)
        assert is_list(entry.command)
      end
    end

    test "merge is a list of status maps per shard" do
      data = Dashboard.collect()
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      assert length(data.merge) == shard_count

      for m <- data.merge do
        assert is_integer(m.shard_index)
        assert is_atom(m.mode)
        assert is_boolean(m.merging)
        assert is_integer(m.merge_count)
        assert is_integer(m.total_bytes_reclaimed)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Dashboard.render/1
  # ---------------------------------------------------------------------------

  describe "render/1" do
    setup do
      data = Dashboard.collect()
      html = Dashboard.render(data)
      %{data: data, html: html}
    end

    test "returns a valid HTML document", %{html: html} do
      assert is_binary(html)
      assert String.starts_with?(html, "<!DOCTYPE html>")
      assert String.contains?(html, "<html")
      assert String.contains?(html, "</html>")
    end

    test "contains live component shell without meta refresh", %{html: html} do
      refute String.contains?(html, ~s(http-equiv="refresh"))
      assert String.contains?(html, ~s(data-dashboard-live-page="overview"))
      assert String.contains?(html, ~s(data-dashboard-live-url="/dashboard/api/overview"))
      assert String.contains?(html, "dashboard-live.js")
      assert String.contains?(html, ~s(data-live-component="content"))
    end

    test "contains page title", %{html: html} do
      assert String.contains?(html, "<title>FerricStore Dashboard</title>")
    end

    test "contains overview purpose text", %{html: html} do
      assert String.contains?(html, ~s(class="page-intro"))
      assert String.contains?(html, "Fast health snapshot")
    end

    test "uses a semantic main landmark", %{html: html} do
      assert String.contains?(html, ~s(<main class="main-content">))
    end

    test "contains top bar with key metrics", %{html: html} do
      assert String.contains?(html, "top-bar")
      assert String.contains?(html, "FerricStore")
      assert String.contains?(html, "Node")
      assert String.contains?(html, "Status")
      assert String.contains?(html, "Memory")
      assert String.contains?(html, "Keys")
    end

    test "contains shards section", %{html: html} do
      assert String.contains?(html, "Shards")
      assert String.contains?(html, "<th>Shard</th>")
      assert String.contains?(html, "<th>Status</th>")
      assert String.contains?(html, "<th>Memory</th>")
    end

    test "contains cache performance section", %{html: html} do
      assert String.contains?(html, "Cache Performance")
      assert String.contains?(html, "Hit Rate")
      assert String.contains?(html, "RAM")
      assert String.contains?(html, "Disk")
    end

    test "contains memory info in top bar", %{html: html} do
      # Memory is shown in the top bar; the full pressure section only appears
      # when pressure != :ok. Verify the top bar memory metric is present.
      assert String.contains?(html, "Memory")
      assert String.contains?(html, "mem-bar-wrap")
    end

    test "contains connections section", %{html: html} do
      assert String.contains?(html, "Connections")
      assert String.contains?(html, "Active")
      assert String.contains?(html, "Blocked")
      assert String.contains?(html, "Tracking")
    end

    test "contains slow log nav link to sub-page", %{html: html} do
      assert String.contains?(html, ~s(href="/dashboard/slowlog"))
      assert String.contains?(html, "Slow Log")
    end

    test "contains merge status nav link to sub-page", %{html: html} do
      assert String.contains?(html, ~s(href="/dashboard/merge"))
      assert String.contains?(html, "Merge Status")
    end

    test "contains Flow nav link to sub-page", %{html: html} do
      assert String.contains?(html, ~s(href="/dashboard/flow"))
      assert String.contains?(html, "FerricFlow")
    end

    test "contains run ID in footer", %{data: data, html: html} do
      # Footer shows first 8 characters of the run_id
      short_id = String.slice(data.overview.run_id, 0, 8)
      assert String.contains?(html, short_id)
    end

    test "escapes HTML entities in rendered output" do
      # The render function should safely escape any user-controlled data.
      # Verify by checking that the escape function works correctly.
      data = Dashboard.collect()
      html = Dashboard.render(data)

      # The run_id is hex so no entities, but the structure should not contain
      # raw unescaped angle brackets from data.
      refute String.contains?(html, "<script>")
    end
  end

  # ---------------------------------------------------------------------------
  # HTTP endpoint: GET /dashboard
  # ---------------------------------------------------------------------------

  describe "GET /dashboard HTTP endpoint" do
    test "returns 200 with HTML content" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard")

      assert response =~ "HTTP/1.1 200 OK"
      assert response =~ "text/html"
    end

    test "response body contains valid HTML" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard")
      body = extract_body(response)

      assert String.contains?(body, "<!DOCTYPE html>")
      assert String.contains?(body, "<title>FerricStore Dashboard</title>")
      assert String.contains?(body, "</html>")
    end

    test "response contains live component shell without meta refresh" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard")
      body = extract_body(response)

      refute String.contains?(body, ~s(http-equiv="refresh"))
      assert String.contains?(body, ~s(data-dashboard-live-page="overview"))
      assert String.contains?(body, ~s(data-dashboard-live-url="/dashboard/api/overview"))
      assert String.contains?(body, "dashboard-live.js")
    end

    test "response contains all dashboard sections" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard")
      body = extract_body(response)

      # Top bar with key metrics
      assert String.contains?(body, "top-bar")
      assert String.contains?(body, "FerricStore")
      # Main content sections
      assert String.contains?(body, "Cache Performance")
      assert String.contains?(body, "Shards")
      assert String.contains?(body, "Connections")
      # Sidebar navigation links
      assert String.contains?(body, "Slow Log")
      assert String.contains?(body, "Merge Status")
      assert String.contains?(body, "Config")
    end

    test "Content-Type header is text/html" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard")

      [headers, _body] = String.split(response, "\r\n\r\n", parts: 2)
      assert String.contains?(headers, "Content-Type: text/html; charset=utf-8")
    end

    test "Content-Length header is present and correct" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard")

      [headers, body] = String.split(response, "\r\n\r\n", parts: 2)

      # Extract Content-Length from headers
      content_length =
        headers
        |> String.split("\r\n")
        |> Enum.find_value(fn line ->
          case String.split(line, ": ", parts: 2) do
            ["Content-Length", value] -> String.to_integer(String.trim(value))
            _ -> nil
          end
        end)

      assert content_length == byte_size(body)
    end
  end

  describe "dashboard ACL login" do
    setup do
      FerricstoreServer.Acl.reset!()
      :ok
    end

    test "protected mode off leaves dashboard open without a session" do
      Application.put_env(:ferricstore, :protected_mode, false)

      response = http_get(HealthEndpoint.port(), "/dashboard/flow")

      assert extract_status_code(response) == 200
      assert extract_body(response) =~ "FerricFlow"
    end

    test "protected mode redirects dashboard pages to ACL login" do
      Application.put_env(:ferricstore, :protected_mode, true)

      response = http_get(HealthEndpoint.port(), "/dashboard/flow")

      assert extract_status_code(response) == 302

      assert extract_header(response, "location") ==
               "/dashboard/login?next=%2Fdashboard%2Fflow"
    end

    test "protected dashboard API replies with JSON login error" do
      Application.put_env(:ferricstore, :protected_mode, true)

      response = http_get(HealthEndpoint.port(), "/dashboard/api/overview")

      assert extract_status_code(response) == 401
      assert response =~ "application/json"
      assert Jason.decode!(extract_body(response)) == %{"error" => "login required"}
    end

    test "invalid dashboard login keeps the user on a readable error page" do
      Application.put_env(:ferricstore, :protected_mode, true)

      response =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "missing",
          "password" => "wrong",
          "next" => "/dashboard/flow"
        })

      assert extract_status_code(response) == 401
      body = extract_body(response)
      assert body =~ "FerricStore Dashboard"
      assert body =~ "WRONGPASS invalid username-password pair or user is disabled."
      assert body =~ ~s(name="next" value="/dashboard/flow")
    end

    test "dashboard login rejects control bytes in next before writing Location" do
      Application.put_env(:ferricstore, :protected_mode, true)
      :ok = FerricstoreServer.Acl.set_user("dash", ["on", ">secret"])

      response =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "dash",
          "password" => "secret",
          "next" => "/dashboard\r\nX-Injected: yes"
        })

      assert extract_status_code(response) == 302
      assert extract_header(response, "location") == "/dashboard"
      refute extract_header(response, "x-injected")
    end

    test "ACL login creates a dashboard session for permitted commands" do
      Application.put_env(:ferricstore, :protected_mode, true)

      :ok =
        FerricstoreServer.Acl.set_user("dash", [
          "on",
          ">secret",
          "~*",
          "-@all",
          "+FLOW.LIST"
        ])

      login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "dash",
          "password" => "secret",
          "next" => "/dashboard/flow"
        })

      assert extract_status_code(login) == 302
      assert extract_header(login, "location") == "/dashboard/flow"

      response =
        http_get(HealthEndpoint.port(), "/dashboard/flow", [
          {"Cookie", dashboard_session_cookie(login)}
        ])

      assert extract_status_code(response) == 200
      assert extract_body(response) =~ "FerricFlow"
    end

    test "dashboard actions enforce Redis command permissions" do
      Application.put_env(:ferricstore, :protected_mode, true)

      :ok =
        FerricstoreServer.Acl.set_user("flow-read", [
          "on",
          ">secret",
          "~*",
          "-@all",
          "+FLOW.LIST",
          "+FLOW.POLICY.GET"
        ])

      login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "flow-read",
          "password" => "secret"
        })

      response =
        http_post_form(
          HealthEndpoint.port(),
          "/dashboard/flow/policies",
          %{"type" => "email", "max_attempts" => "3"},
          [{"Cookie", dashboard_session_cookie(login)}]
        )

      assert extract_status_code(response) == 403
      assert extract_body(response) =~ "FLOW.POLICY.SET"
      assert extract_body(response) =~ "+FLOW.POLICY.SET"
      assert extract_body(response) =~ "Required ACL command"

      retention_response =
        http_post_form(
          HealthEndpoint.port(),
          "/dashboard/flow/retention",
          %{"action" => "cleanup", "limit" => "1"},
          [{"Cookie", dashboard_session_cookie(login)}]
        )

      assert extract_status_code(retention_response) == 403
      assert extract_body(retention_response) =~ "FLOW.RETENTION_CLEANUP"
      assert extract_body(retention_response) =~ "+FLOW.RETENTION_CLEANUP"
      assert extract_body(retention_response) =~ "Required ACL command"
    end

    test "dashboard rewind action reports missing command and write key permission" do
      Application.put_env(:ferricstore, :protected_mode, true)

      :ok =
        FerricstoreServer.Acl.set_user("flow-reader-only", [
          "on",
          ">secret",
          "~tenant-a:*",
          "-@all",
          "+FLOW.GET"
        ])

      login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "flow-reader-only",
          "password" => "secret"
        })

      response =
        http_post_form(
          HealthEndpoint.port(),
          "/dashboard/flow/tenant-b%3Aflow-1/rewind",
          %{"to_event" => "1"},
          [{"Cookie", dashboard_session_cookie(login)}]
        )

      assert extract_status_code(response) == 403
      assert extract_body(response) =~ "FLOW.REWIND"
      assert extract_body(response) =~ "+FLOW.REWIND"
      assert extract_body(response) =~ "%W~tenant-b:flow-1"
      assert extract_body(response) =~ "write"
    end

    test "flow detail pages enforce ACL key patterns" do
      Application.put_env(:ferricstore, :protected_mode, true)

      :ok =
        FerricstoreServer.Acl.set_user("tenant-reader", [
          "on",
          ">secret",
          "~tenant-a:*",
          "-@all",
          "+FLOW.GET"
        ])

      login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "tenant-reader",
          "password" => "secret"
        })

      response =
        http_get(HealthEndpoint.port(), "/dashboard/flow/tenant-b%3Aflow-1", [
          {"Cookie", dashboard_session_cookie(login)}
        ])

      assert extract_status_code(response) == 403
      assert extract_body(response) =~ "FLOW.GET"
      assert extract_body(response) =~ "+FLOW.GET"
      assert extract_body(response) =~ "~tenant-b:flow-1"
      assert extract_body(response) =~ "read"
      assert extract_body(response) =~ "key"
    end

    test "dashboard API forbidden replies include required ACL command and key details" do
      Application.put_env(:ferricstore, :protected_mode, true)

      :ok =
        FerricstoreServer.Acl.set_user("tenant-api-reader", [
          "on",
          ">secret",
          "~tenant-a:*",
          "-@all",
          "+FLOW.GET"
        ])

      login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "tenant-api-reader",
          "password" => "secret"
        })

      response =
        http_get(HealthEndpoint.port(), "/dashboard/api/flow/tenant-b%3Aflow-1", [
          {"Cookie", dashboard_session_cookie(login)}
        ])

      assert extract_status_code(response) == 403

      assert Jason.decode!(extract_body(response)) == %{
               "error" => "forbidden",
               "reason" =>
                 "NOPERM this user has no permissions to access one of the keys mentioned in the command (FLOW.GET key)",
               "required_acl_rule" => "+FLOW.GET",
               "required_command" => "FLOW.GET",
               "required_key" => "tenant-b:flow-1",
               "required_key_access" => "read",
               "required_key_rule" => "%R~tenant-b:flow-1"
             }
    end

    test "keyspace inspector reports GET key permission when protected" do
      Application.put_env(:ferricstore, :protected_mode, true)

      :ok =
        FerricstoreServer.Acl.set_user("scan-only", [
          "on",
          ">secret",
          "~tenant-a:*",
          "-@all",
          "+SCAN"
        ])

      login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "scan-only",
          "password" => "secret"
        })

      response =
        http_get(HealthEndpoint.port(), "/dashboard/keyspace?key=tenant-b%3A1", [
          {"Cookie", dashboard_session_cookie(login)}
        ])

      assert extract_status_code(response) == 403
      assert extract_body(response) =~ "GET"
      assert extract_body(response) =~ "+GET"
      assert extract_body(response) =~ "%R~tenant-b:1"
    end

    test "removed Flow projections live endpoint is not authorized as a Flow id" do
      Application.put_env(:ferricstore, :protected_mode, true)

      :ok =
        FerricstoreServer.Acl.set_user("flow-list", [
          "on",
          ">secret",
          "~*",
          "-@all",
          "+FLOW.LIST"
        ])

      login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "flow-list",
          "password" => "secret"
        })

      response =
        http_get(HealthEndpoint.port(), "/dashboard/api/flow/projections", [
          {"Cookie", dashboard_session_cookie(login)}
        ])

      assert extract_status_code(response) == 404
      assert Jason.decode!(extract_body(response)) == %{"error" => "not found"}

      :ok =
        FerricstoreServer.Acl.set_user("no-flow-list", [
          "on",
          ">secret",
          "~*",
          "-@all",
          "+FLOW.GET"
        ])

      denied_login =
        http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
          "username" => "no-flow-list",
          "password" => "secret"
        })

      denied =
        http_get(HealthEndpoint.port(), "/dashboard/api/flow/projections", [
          {"Cookie", dashboard_session_cookie(denied_login)}
        ])

      assert extract_status_code(denied) == 403
      assert Jason.decode!(extract_body(denied))["required_acl_rule"] == "+FLOW.LIST"
    end
  end

  # ---------------------------------------------------------------------------
  # Existing endpoints still work after dashboard addition
  # ---------------------------------------------------------------------------

  describe "existing endpoints unaffected" do
    test "/health/ready still returns JSON" do
      port = HealthEndpoint.port()
      response = http_get(port, "/health/ready")

      assert response =~ "HTTP/1.1 200 OK"
      assert response =~ "application/json"
      assert response =~ ~s("status":"ok")
    end

    test "unknown paths still return 404" do
      port = HealthEndpoint.port()
      response = http_get(port, "/nonexistent")

      assert response =~ "HTTP/1.1 404 Not Found"
    end
  end

  # ---------------------------------------------------------------------------
  # Dashboard reflects live data changes
  # ---------------------------------------------------------------------------

  describe "dashboard reflects live data" do
    test "shows shard status as ok when shards are running" do
      data = Dashboard.collect()

      # At least one shard should be ok since the app is running
      assert Enum.any?(data.shards, fn s -> s.status == "ok" end)
    end

    test "memory data shows non-zero max_bytes" do
      data = Dashboard.collect()

      assert data.memory.max_bytes > 0
    end

    test "eviction policy matches configuration" do
      data = Dashboard.collect()

      # Read the authoritative value directly from MemoryGuard (the same source
      # Dashboard.collect/0 uses) instead of Application.get_env, which can be
      # stale when other tests modify it via CONFIG SET without updating the
      # MemoryGuard GenServer state.
      configured = Ferricstore.MemoryGuard.eviction_policy()

      assert data.memory.eviction_policy == configured
    end
  end

  # ---------------------------------------------------------------------------
  # Namespace Config section (Dashboard Page 9)
  # ---------------------------------------------------------------------------

  describe "collect/0 namespace_config" do
    test "includes namespace_config key in collected data" do
      data = Dashboard.collect()

      assert Map.has_key?(data, :namespace_config)
      assert is_list(data.namespace_config)
    end

    test "returns empty list when no overrides are configured" do
      NamespaceConfig.reset_all()
      data = Dashboard.collect()

      assert data.namespace_config == []
    end

    test "returns configured namespaces after set" do
      NamespaceConfig.reset_all()
      :ok = NamespaceConfig.set("rate", "window_ms", "10")

      data = Dashboard.collect()

      assert length(data.namespace_config) == 1
      [entry] = data.namespace_config
      assert entry.prefix == "rate"
      assert entry.window_ms == 10
      assert is_integer(entry.changed_at)
      assert is_binary(entry.changed_by)

      NamespaceConfig.reset_all()
    end

    test "returns multiple configured namespaces sorted by prefix" do
      NamespaceConfig.reset_all()
      :ok = NamespaceConfig.set("zeta", "window_ms", "50")
      :ok = NamespaceConfig.set("alpha", "window_ms", "20")

      data = Dashboard.collect()

      assert length(data.namespace_config) == 2
      prefixes = Enum.map(data.namespace_config, & &1.prefix)
      assert prefixes == ["alpha", "zeta"]

      NamespaceConfig.reset_all()
    end
  end

  describe "render_config_page/1 namespace config sub-page" do
    test "config sub-page contains Namespace Config heading" do
      data = Dashboard.collect_config_page()
      html = Dashboard.render_config_page(data)

      assert String.contains?(html, "Namespace Config")
    end

    test "config sub-page shows supported configuration commands" do
      data = Dashboard.collect_config_page()
      html = Dashboard.render_config_page(data)

      assert String.contains?(html, "Configuration Commands")
      assert String.contains?(html, "CONFIG GET &lt;pattern&gt;")
      assert String.contains?(html, "CONFIG SET &lt;key&gt; &lt;value&gt;")
      assert String.contains?(html, "CONFIG SET LOCAL log_level &lt;level&gt;")
      assert String.contains?(html, "CONFIG RESETSTAT")
      assert String.contains?(html, "CONFIG REWRITE")
      assert String.contains?(html, "FERRICSTORE.CONFIG SET &lt;prefix&gt; window_ms &lt;ms&gt;")
      assert String.contains?(html, "FERRICSTORE.CONFIG RESET [prefix]")
    end

    test "config sub-page shows supported config parameters by scope" do
      data = Dashboard.collect_config_page()
      html = Dashboard.render_config_page(data)

      assert String.contains?(html, "Runtime Parameters")
      assert String.contains?(html, "maxmemory-policy")
      assert String.contains?(html, "keydir-max-ram")
      assert String.contains?(html, "hot-cache-max-value-size")
      assert String.contains?(html, "maxmemory")
      assert String.contains?(html, "data-dir")
      assert String.contains?(html, "log_level")
      assert String.contains?(html, "read-write")
      assert String.contains?(html, "read-only")
      assert String.contains?(html, "node-local")
    end

    test "shows built-in defaults message when no namespaces configured" do
      NamespaceConfig.reset_all()
      data = Dashboard.collect_config_page()
      html = Dashboard.render_config_page(data)

      assert String.contains?(html, "All namespaces using built-in default window (1ms)")
    end

    test "shows table with prefix and window when namespaces configured" do
      NamespaceConfig.reset_all()
      :ok = NamespaceConfig.set("session", "window_ms", "5")

      data = Dashboard.collect_config_page()
      html = Dashboard.render_config_page(data)

      assert String.contains?(html, "session")
      assert String.contains?(html, "5")
      refute String.contains?(html, "quorum")
      # Table headers
      assert String.contains?(html, "Prefix")
      assert String.contains?(html, "Window (ms)")
      refute String.contains?(html, "Durability")
      assert String.contains?(html, "Changed At")
      assert String.contains?(html, "Changed By")

      NamespaceConfig.reset_all()
    end

    test "does not render removed namespace durability state" do
      NamespaceConfig.reset_all()
      assert {:error, _} = NamespaceConfig.set("ephemeral", "durability", "async")

      data = Dashboard.collect_config_page()
      html = Dashboard.render_config_page(data)

      [_before, ns_section] = String.split(html, "Namespace Config", parts: 2)

      ns_html =
        case String.split(ns_section, "section-title", parts: 2) do
          [section, _rest] -> section
          [section] -> section
        end

      refute String.contains?(ns_html, "ephemeral")
      refute String.contains?(ns_html, ">async<")

      NamespaceConfig.reset_all()
    end

    test "does not render removed durability column state" do
      NamespaceConfig.reset_all()
      :ok = NamespaceConfig.set("safe", "window_ms", "2")

      data = Dashboard.collect_config_page()
      html = Dashboard.render_config_page(data)

      # Extract the config table area to check
      [_before, ns_section] = String.split(html, "Namespace Config", parts: 2)
      # Take until the next section or end of body
      ns_html =
        case String.split(ns_section, "section-title", parts: 2) do
          [section, _rest] -> section
          [section] -> section
        end

      refute String.contains?(ns_html, "Durability")
      refute String.contains?(ns_html, "quorum")
      refute String.contains?(ns_html, "c-yellow")

      NamespaceConfig.reset_all()
    end

    test "shows overrides count badge when namespaces are configured" do
      NamespaceConfig.reset_all()
      :ok = NamespaceConfig.set("metrics", "window_ms", "100")

      data = Dashboard.collect_config_page()
      html = Dashboard.render_config_page(data)

      assert String.contains?(html, "1 overrides")

      NamespaceConfig.reset_all()
    end

    test "shows defaults badge when all defaults" do
      NamespaceConfig.reset_all()

      data = Dashboard.collect_config_page()
      html = Dashboard.render_config_page(data)

      assert String.contains?(html, "defaults")

      NamespaceConfig.reset_all()
    end

    test "config sidebar link appears after merge status in sidebar" do
      data = Dashboard.collect()
      html = Dashboard.render(data)

      merge_pos = :binary.match(html, "Merge Status") |> elem(0)
      config_pos = :binary.match(html, "Config") |> elem(0)

      assert config_pos > merge_pos
    end
  end

  describe "non-Flow operational sub-pages" do
    test "slowlog page renders at-a-glance latency summary" do
      html =
        Dashboard.render_slowlog_page(%{
          slowlog: [
            %{
              id: 2,
              timestamp_us: 1_700_000_000_100_000,
              duration_us: 12_000,
              command: ["SET", "a", "b"]
            },
            %{
              id: 1,
              timestamp_us: 1_700_000_000_000_000,
              duration_us: 4_000,
              command: ["GET", "a"]
            }
          ]
        })

      assert String.contains?(html, "Slow Log Summary")
      assert String.contains?(html, "Worst")
      assert String.contains?(html, "Avg")
      assert String.contains?(html, "Total Time")
      assert String.contains?(html, "12.0 ms")
    end

    test "merge page renders compact merge pressure summary" do
      html =
        Dashboard.render_merge_page(%{
          merge: [
            %{
              shard_index: 0,
              mode: :idle,
              merging: false,
              last_merge_at: nil,
              merge_count: 2,
              total_bytes_reclaimed: 1_024
            },
            %{
              shard_index: 1,
              mode: :compact,
              merging: true,
              last_merge_at: 1_700_000_000_000,
              merge_count: 3,
              total_bytes_reclaimed: 2_048
            }
          ]
        })

      assert String.contains?(html, "Merge Summary")
      assert String.contains?(html, "Active Shards")
      assert String.contains?(html, "Total Reclaimed")
      assert String.contains?(html, "Total Merges")
      assert String.contains?(html, "1 / 2")
    end

    test "consensus page renders leader and apply-lag summary" do
      html =
        Dashboard.render_raft_page(%{
          cluster: %{
            node_name: :nonode@nohost,
            cluster_mode: :standalone,
            cluster_size: 1,
            nodes: [:nonode@nohost]
          },
          raft_shards: [
            %{
              shard: 0,
              status: :ok,
              leader: {:raft_server_0, :nonode@nohost},
              current_term: 4,
              commit_index: 1_100,
              last_applied: 1_000,
              log_size: 64,
              members: []
            },
            %{
              shard: 1,
              status: :down,
              leader: nil,
              current_term: 0,
              commit_index: 0,
              last_applied: 0,
              log_size: 0,
              members: []
            }
          ]
        })

      assert String.contains?(html, "Consensus Summary")
      assert String.contains?(html, "Healthy Shards")
      assert String.contains?(html, "Max Apply Lag")
      assert String.contains?(html, "single-member WARaft")
    end

    test "clients page summarizes flags and oldest client age" do
      html =
        Dashboard.render_clients_page(%{
          connections: %{active: 3, blocked: 1, tracking: 1},
          clients: [
            %{pid: self(), peer: "127.0.0.1:6379", age_seconds: 65, flags: "ST"},
            %{pid: self(), peer: "127.0.0.1:6380", age_seconds: 2, flags: "M"}
          ]
        })

      assert String.contains?(html, "Oldest")
      assert String.contains?(html, "Pub/Sub")
      assert String.contains?(html, "Transactions")
      assert String.contains?(html, "Tracking")
    end

    test "overview renders idle and unlimited memory as neutral states" do
      data =
        Dashboard.collect()
        |> put_in([:hotcold, :total_lookups], 0)
        |> put_in([:hotcold, :total_hits], 0)
        |> put_in([:hotcold, :total_misses], 0)
        |> put_in([:hotcold, :hit_ratio], 0.0)
        |> put_in([:memory, :max_bytes], 0)
        |> put_in([:memory, :ratio], 0.0)

      html = Dashboard.render(data)

      assert String.contains?(html, "No read samples")
      assert String.contains?(html, "unlimited")
      refute String.contains?(html, "/ 0 B")
    end

    test "consensus page uses WARaft-ready labels and omits unavailable zero log metrics" do
      html =
        Dashboard.render_raft_page(%{
          cluster: %{
            node_name: :nonode@nohost,
            cluster_mode: :standalone,
            cluster_size: 1,
            nodes: [:nonode@nohost]
          },
          raft_shards: [
            %{
              shard: 16,
              status: :ok,
              leader: {:raft_server_ferricstore_waraft_backend_16, :nonode@nohost},
              current_term: 4,
              commit_index: 16_000,
              last_applied: 15_900,
              log_size: 0,
              members: [{:raft_server_ferricstore_waraft_backend_16, :nonode@nohost}]
            }
          ]
        })

      assert String.contains?(html, "Consensus")
      assert String.contains?(html, "WARaft")
      assert String.contains?(html, "shard-16")
      refute String.contains?(html, "Raft Consensus - FerricStore")
      refute String.contains?(html, ">Raft Consensus<")
      refute String.contains?(html, "Per-Shard Raft State")
      refute String.contains?(html, "Total Log Entries")
      refute String.contains?(html, "<td>0</td>")
    end

    test "storage page shows total files and largest shard summary" do
      html =
        Dashboard.render_storage_page(%{
          total_disk_bytes: 10_240,
          total_files: 6,
          shards: [
            %{index: 0, disk_bytes: 2_048, data_file_count: 1, hint_file_count: 0},
            %{index: 1, disk_bytes: 8_192, data_file_count: 3, hint_file_count: 2}
          ]
        })

      assert String.contains?(html, "Largest Shard")
      assert String.contains?(html, "Shard 1")
      assert String.contains?(html, "Data Files")
      assert String.contains?(html, "Hint Files")
    end

    test "keyspace page renders bounded key metadata and inspector purpose" do
      html =
        Dashboard.render_keyspace_page(%{
          filters: %{key: "user:1", prefix: "", include_internal: false, limit: 50},
          rows: [
            %{
              key: "user:1",
              physical_key: "user:1",
              shard: 0,
              type: "string",
              ttl: "none",
              size: "5 B",
              location: "hot",
              internal?: false
            }
          ],
          inspected: %{
            key: "user:1",
            found?: true,
            type: "string",
            ttl: "none",
            size: "5 B",
            location: "hot",
            shard: 0
          },
          total_sampled: 1
        })

      assert String.contains?(html, "Keyspace")
      assert String.contains?(html, "Find keys, inspect metadata")
      assert String.contains?(html, "user:1")
      assert String.contains?(html, "Requires +SCAN")
      assert String.contains?(html, "Requires +GET")
    end

    test "commands page explains KV command health from counters and slowlog" do
      html =
        Dashboard.render_commands_page(%{
          summary: %{
            total_commands: 120,
            ops_per_sec: 12.5,
            slowlog_entries: 2,
            slowest_us: 15_000
          },
          slow_by_command: [
            %{command: "SET", count: 1, worst_us: 15_000, avg_us: 15_000},
            %{command: "GET", count: 1, worst_us: 4_000, avg_us: 4_000}
          ],
          command_groups: []
        })

      assert String.contains?(html, "Commands")
      assert String.contains?(html, "Understand command traffic")
      assert String.contains?(html, "Slow Log By Command")
      assert String.contains?(html, "15.0 ms")
    end

    test "read path page explains hot/cold and prefix read pressure" do
      html =
        Dashboard.render_reads_page(%{
          hotcold: %{
            hit_ratio: 98.5,
            total_hits: 985,
            total_misses: 15,
            hot_reads: 900,
            cold_reads: 85,
            hot_read_pct: 91.4,
            cold_reads_per_sec: 3.2,
            sample_rate: 100,
            total_lookups: 1_000,
            top_prefixes: []
          },
          prefixes: [
            %{prefix: "tenant", hot_reads: 900, cold_reads: 85, cold_pct: 8.6}
          ]
        })

      assert String.contains?(html, "Read Path")
      assert String.contains?(html, "Track hot-cache efficiency")
      assert String.contains?(html, "Prefix Read Pressure")
      assert String.contains?(html, "tenant")
      assert String.contains?(html, ~s(<span class="sampled-tag"))
      refute String.contains?(html, "&lt;span")
    end

    test "prefixes page summarizes sample size and read totals" do
      html =
        Dashboard.render_prefixes_page(%{
          total_sampled: 100,
          prefixes: [
            %{prefix: "tenant-a", keys: 70, pct: 70.0, hot_reads: 12, cold_reads: 3},
            %{prefix: "tenant-b", keys: 30, pct: 30.0, hot_reads: 2, cold_reads: 5}
          ]
        })

      assert String.contains?(html, "Prefix Summary")
      assert String.contains?(html, "Sampled Keys")
      assert String.contains?(html, "Indexed Keys")
      assert String.contains?(html, "Hot Reads")
      assert String.contains?(html, "Cold Reads")
    end

    test "non-Flow sub-pages include purpose text" do
      pages = [
        Dashboard.render_slowlog_page(%{slowlog: []}),
        Dashboard.render_merge_page(%{merge: []}),
        Dashboard.render_config_page(%{
          namespace_config: [],
          config_commands: [],
          config_parameters: []
        }),
        Dashboard.render_raft_page(%{
          cluster: %{
            node_name: :nonode@nohost,
            cluster_mode: :standalone,
            cluster_size: 1,
            nodes: []
          },
          raft_shards: []
        }),
        Dashboard.render_clients_page(%{
          connections: %{active: 0, blocked: 0, tracking: 0},
          clients: []
        }),
        Dashboard.render_storage_page(%{total_disk_bytes: 0, total_files: 0, shards: []}),
        Dashboard.render_prefixes_page(%{total_sampled: 0, prefixes: []})
      ]

      for html <- pages do
        assert String.contains?(html, ~s(class="page-intro"))
      end
    end
  end

  describe "GET /dashboard namespace config sidebar link" do
    test "HTTP response body contains Config sidebar link" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard")
      body = extract_body(response)

      assert String.contains?(body, "Config")
      assert String.contains?(body, "/dashboard/config")
    end

    test "HTTP response sidebar contains config link regardless of overrides" do
      NamespaceConfig.reset_all()
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard")
      body = extract_body(response)

      assert String.contains?(body, "/dashboard/config")
    end
  end

  describe "dashboard UX hardening" do
    test "/dashboard/consensus redirects to the consensus page" do
      response = http_get(HealthEndpoint.port(), "/dashboard/consensus")

      assert extract_status_code(response) == 302
      assert extract_header(response, "location") == "/dashboard/raft"
    end

    test "subpage shell exposes semantic heading and active navigation state" do
      html = Dashboard.collect_flow_page() |> Dashboard.render_flow_page()

      assert String.contains?(html, ~s(<h1 class="subpage-title">FerricFlow</h1>))
      assert String.contains?(html, ~s(<nav class="sidebar" aria-label="Dashboard sections">))
      assert String.contains?(html, ~s(aria-current="page"))
    end

    test "clients page uses explicit connection registry metadata" do
      client_id = System.unique_integer([:positive])
      parent = self()

      pid =
        spawn(fn ->
          send(parent, {:client_fixture_ready, self()})

          receive do
            :stop -> :ok
          end
        end)

      assert_receive {:client_fixture_ready, ^pid}

      FerricstoreServer.Connection.Registry.register(client_id, pid, %{
        client_name: "dashboard-client",
        username: "alice",
        peer: "127.0.0.1:6380",
        created_at_ms: System.monotonic_time(:millisecond) - 65_000,
        flags: "ST"
      })

      on_exit(fn ->
        FerricstoreServer.Connection.Registry.unregister(client_id, pid)
        send(pid, :stop)
      end)

      html = Dashboard.collect_clients_page() |> Dashboard.render_clients_page()

      assert String.contains?(html, Integer.to_string(client_id))
      assert String.contains?(html, "dashboard-client")
      assert String.contains?(html, "alice")
      assert String.contains?(html, "127.0.0.1:6380")
      refute String.contains?(html, "unknown:0")
    end

    test "live Flow payloads update charts and tables together" do
      assert {:ok, states_payload} = Dashboard.live_payload("flow/states")
      assert Map.has_key?(states_payload.components, "flow_states_chart")
      assert Map.has_key?(states_payload.components, "flow_states_table")

      assert {:ok, workers_payload} = Dashboard.live_payload("flow/workers")
      assert Map.has_key?(workers_payload.components, "flow_workers_chart")
      assert Map.has_key?(workers_payload.components, "flow_workers")

      assert {:ok, due_payload} = Dashboard.live_payload("flow/due")
      assert Map.has_key?(due_payload.components, "flow_due_chart")
      assert Map.has_key?(due_payload.components, "flow_due_now")
    end

    test "signals live refresh preserves scan history mode" do
      html =
        Dashboard.render_flow_signals_page(%{
          signals: [],
          filters: %{type: "email", signal: nil, q: nil, limit: 40, scan_history: true},
          available_types: ["email"],
          total_sampled: 0,
          filtered_sampled: 0,
          sample_limit: 400
        })

      assert String.contains?(html, ~s(data-dashboard-live-url="/dashboard/api/flow/signals?))
      assert String.contains?(html, "scan=true")
    end

    test "retention cleanup requires explicit confirmation" do
      assert {:error, reason} =
               Dashboard.apply_flow_retention_form(%{"action" => "cleanup", "limit" => "1"})

      assert reason =~ "confirm"

      html =
        Dashboard.collect_flow_retention_page(limit: 1)
        |> Dashboard.render_flow_retention_page()

      assert String.contains?(html, ~s(name="confirm_cleanup"))
      assert String.contains?(html, "Requires +FLOW.RETENTION_CLEANUP")
      assert String.contains?(html, "Sample Preview")
    end

    test "failure recovery requires explicit reclaim confirmation" do
      assert {:error, reason} =
               Dashboard.apply_flow_failures_form(%{
                 "action" => "reclaim",
                 "type" => "email",
                 "limit" => "1",
                 "lease_ms" => "30000"
               })

      assert reason =~ "confirm"

      html =
        Dashboard.collect_flow_failures_page(type: "email")
        |> Dashboard.render_flow_failures_page()

      assert String.contains?(html, ~s(name="confirm_reclaim"))
      assert String.contains?(html, "Requires +FLOW.RECLAIM")
    end

    test "detail rewind requires explicit confirmation" do
      assert {:error, reason} =
               Dashboard.apply_flow_rewind_form(%{"id" => "flow-1", "to_event" => "event-1"})

      assert reason =~ "confirm"
    end
  end

  # ---------------------------------------------------------------------------
  # FerricFlow dashboard pages
  # ---------------------------------------------------------------------------

  describe "collect_flow_page/0" do
    test "discovers Flow types from durable state records" do
      id = "dashboard-flow-#{System.unique_integer([:positive])}"

      assert :ok =
               FerricStore.flow_create(id,
                 type: "dashboard-order",
                 state: "queued",
                 run_at_ms: 1_000
               )

      data = Dashboard.collect_flow_page()

      assert data.total_sampled >= 1
      assert Enum.any?(data.types, fn type -> type.type == "dashboard-order" end)

      type = Enum.find(data.types, fn type -> type.type == "dashboard-order" end)
      assert type.active >= 1
      assert type.queued >= 1
    end

    test "uses sampled counts when global Flow info misses partitioned records" do
      flow_type = "dashboard-partitioned-counts-#{System.unique_integer([:positive])}"
      partition_key = "tenant-counts-#{System.unique_integer([:positive])}"
      queued_id = "dashboard-queued-#{System.unique_integer([:positive])}"
      running_id = "dashboard-running-#{System.unique_integer([:positive])}"

      assert :ok =
               FerricStore.flow_create(queued_id,
                 type: flow_type,
                 state: "queued",
                 partition_key: partition_key,
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )

      assert :ok =
               FerricStore.flow_create(running_id,
                 type: flow_type,
                 state: "queued",
                 partition_key: partition_key,
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )

      assert {:ok, [%{lease_owner: "dashboard-worker"}]} =
               FerricStore.flow_claim_due(flow_type,
                 partition_key: partition_key,
                 worker: "dashboard-worker",
                 limit: 1,
                 now_ms: 1_000,
                 payload: false
               )

      data = Dashboard.collect_flow_page()
      type = Enum.find(data.types, &(&1.type == flow_type))

      assert type
      assert type.exact == false
      assert type.total >= 2
      assert type.active >= 2
      assert type.queued >= 1
      assert type.running >= 1
      assert data.summary.active >= 2
    end
  end

  describe "render_flow_page/1" do
    test "renders overview, state breakdown, and explorer links" do
      data = Dashboard.collect_flow_page()
      html = Dashboard.render_flow_page(data)

      assert String.contains?(html, "FerricFlow")
      assert String.contains?(html, "Flow Overview")
      assert String.contains?(html, "Projection Health")
      assert String.contains?(html, "State Breakdown")
      assert String.contains?(html, "Recent Flow Records")
      assert String.contains?(html, ~s(href="/dashboard/flow/states"))
      assert String.contains?(html, ~s(href="/dashboard/flow/workers"))
      assert String.contains?(html, ~s(href="/dashboard/flow/due"))
      assert String.contains?(html, ~s(href="/dashboard/flow/failures"))
      assert String.contains?(html, ~s(href="/dashboard/flow/lineage"))
      assert String.contains?(html, ~s(href="/dashboard/flow/query"))
      assert String.contains?(html, ~s(href="/dashboard/flow/policies"))
      assert String.contains?(html, ~s(href="/dashboard/flow/retention"))
      refute String.contains?(html, ~s(href="/dashboard/flow/config"))
      refute String.contains?(html, ~s(href="/dashboard/flow/projections"))
    end

    test "renders lease_owner as the running worker" do
      html =
        Dashboard.render_flow_page(%{
          summary: %{},
          types: [],
          workers: [],
          records: [
            %{
              id: "dashboard-running-worker",
              type: "email",
              state: "running",
              partition_key: "tenant-a",
              lease_owner: "worker-a",
              run_at_ms: 1_000,
              lease_expires_at_ms: System.system_time(:millisecond) + 30_000,
              updated_at_ms: 1_000
            }
          ],
          total_sampled: 1,
          sample_limit: 400
        })

      assert String.contains?(html, "worker-a")
      assert String.contains?(html, "leased by worker-a")
      refute String.contains?(html, "running without worker metadata")
    end

    test "Flow overview value refs link to the detail inspector" do
      html =
        Dashboard.render_flow_page(%{
          summary: %{},
          types: [],
          workers: [],
          records: [
            %{
              id: "dashboard-value-link",
              type: "email",
              state: "queued",
              partition_key: "tenant-a",
              payload_ref: "flow-value:payload",
              run_at_ms: 1_000,
              updated_at_ms: 1_000
            }
          ],
          total_sampled: 1,
          sample_limit: 400
        })

      assert String.contains?(
               html,
               ~s(href="/dashboard/flow/dashboard-value-link?partition_key=tenant-a#flow-value-)
             )

      assert String.contains?(html, ~s(title="Open payload value"))
      assert String.contains?(html, ~s(aria-label="Open payload value"))
    end

    test "Flow signals page renders signal history rows" do
      html =
        Dashboard.render_flow_signals_page(%{
          signals: [
            %{
              id: "dashboard-signal-row",
              partition_key: "tenant-a",
              type: "email",
              event_id: "1100-2",
              time_ms: 1_100,
              signal: "payment_received",
              from_state: "waiting_payment",
              to_state: "verify_payment",
              fields: %{
                "value_refs" => ~s({"payment_event":{"ref":"ref:payment","version":1}})
              },
              record: %{
                id: "dashboard-signal-row",
                partition_key: "tenant-a",
                type: "email"
              }
            }
          ],
          filters: %{type: nil, signal: nil, q: nil, limit: 40},
          available_types: ["email"],
          total_sampled: 1,
          filtered_sampled: 1,
          sample_limit: 400
        })

      assert String.contains?(html, "Flow Signals")
      assert String.contains?(html, "payment_received")
      assert String.contains?(html, "waiting_payment -> verify_payment")
      assert String.contains?(html, "payment_event")
      assert String.contains?(html, ~s(title="Filter by signal name substring"))
      assert String.contains?(html, ~s(title="Apply Flow signal filters"))

      assert String.contains?(
               html,
               ~s(href="/dashboard/flow/dashboard-signal-row?partition_key=tenant-a)
             )
    end

    test "does not depend on external chart scripts" do
      pages = [
        Dashboard.collect_flow_page() |> Dashboard.render_flow_page(),
        Dashboard.collect_flow_states_page() |> Dashboard.render_flow_states_page(),
        Dashboard.collect_flow_workers_page() |> Dashboard.render_flow_workers_page(),
        Dashboard.collect_flow_due_page() |> Dashboard.render_flow_due_page(),
        "dashboard-flow-chart-#{System.unique_integer([:positive])}"
        |> Dashboard.collect_flow_detail_page()
        |> Dashboard.render_flow_detail_page()
      ]

      for html <- pages do
        refute String.contains?(html, "cdn.jsdelivr")
        refute String.contains?(html, "https://")
        refute String.contains?(html, "new Chart")
        refute String.contains?(html, "<canvas")
      end
    end
  end

  describe "render_flow_states_page/1" do
    test "renders state-centric Flow page" do
      data = Dashboard.collect_flow_states_page()
      html = Dashboard.render_flow_states_page(data)

      assert String.contains?(html, "Flow States")
      assert String.contains?(html, "Oldest Due")
      assert String.contains?(html, "Due Now")
    end

    test "filters state-centric Flow page by workflow type" do
      email_type = "dashboard-email-#{System.unique_integer([:positive])}"
      sms_type = "dashboard-sms-#{System.unique_integer([:positive])}"

      assert :ok =
               FerricStore.flow_create("dashboard-email-id-#{System.unique_integer([:positive])}",
                 type: email_type,
                 state: "queued",
                 run_at_ms: 1_000
               )

      assert :ok =
               FerricStore.flow_create("dashboard-sms-id-#{System.unique_integer([:positive])}",
                 type: sms_type,
                 state: "queued",
                 run_at_ms: 1_000
               )

      data = Dashboard.collect_flow_states_page(type: email_type)
      html = Dashboard.render_flow_states_page(data)

      assert data.type_filter == email_type
      assert Enum.all?(data.states, &(&1.type == email_type))
      assert String.contains?(html, ~s(name="type"))
      assert String.contains?(html, ~s(value="#{email_type}" selected))
      assert String.contains?(html, email_type)
      assert String.contains?(html, ~s(data-dashboard-live-url="/dashboard/api/flow/states?type=))
    end

    test "filters state-centric Flow page by type, state, name, updated range, and limit" do
      flow_type = "dashboard-filter-#{System.unique_integer([:positive])}"
      keep_old_id = "dashboard-filter-keep-old-#{System.unique_integer([:positive])}"
      keep_new_id = "dashboard-filter-keep-new-#{System.unique_integer([:positive])}"
      drop_id = "dashboard-filter-drop-#{System.unique_integer([:positive])}"
      other_state_id = "dashboard-filter-other-#{System.unique_integer([:positive])}"
      now_ms = System.system_time(:millisecond)

      assert :ok =
               FerricStore.flow_create(keep_old_id,
                 type: flow_type,
                 state: "queued",
                 run_at_ms: now_ms,
                 now_ms: now_ms
               )

      assert :ok =
               FerricStore.flow_create(keep_new_id,
                 type: flow_type,
                 state: "queued",
                 run_at_ms: now_ms + 10,
                 now_ms: now_ms + 10
               )

      assert :ok =
               FerricStore.flow_create(drop_id,
                 type: flow_type,
                 state: "queued",
                 run_at_ms: now_ms,
                 now_ms: now_ms
               )

      assert :ok =
               FerricStore.flow_create(other_state_id,
                 type: flow_type,
                 state: "waiting",
                 run_at_ms: now_ms,
                 now_ms: now_ms
               )

      data =
        Dashboard.collect_flow_states_page(
          type: flow_type,
          state: "queued",
          q: "keep",
          from_ms: now_ms - 1_000,
          to_ms: now_ms + 1_000,
          limit: 1
        )

      html = Dashboard.render_flow_states_page(data)

      assert data.type_filter == flow_type
      assert data.state_filter == "queued"
      assert data.name_filter == "keep"
      assert data.limit == 1
      assert data.filtered_sampled == 2
      assert Enum.map(data.records, &Map.fetch!(&1, :id)) == [keep_new_id]
      assert Enum.all?(data.states, &(&1.type == flow_type and &1.state == "queued"))
      assert Enum.map(data.states, & &1.count) == [2]
      assert String.contains?(html, ~s(name="state"))
      assert String.contains?(html, ~s(name="q"))
      assert String.contains?(html, ~s(name="from"))
      assert String.contains?(html, ~s(name="to"))
      assert String.contains?(html, ~s(name="limit"))
      assert String.contains?(html, "Showing #{flow_type} / queued / id contains keep")
      assert String.contains?(html, "recent limit 1")
      assert String.contains?(html, "Recent Flow Records")
      assert String.contains?(html, "limit 1")
      assert String.contains?(html, keep_new_id)
      refute String.contains?(html, keep_old_id)
      refute String.contains?(html, drop_id)
      refute String.contains?(html, other_state_id)
    end

    test "terminal state filter reads cold projected list records when hot sample misses them" do
      flow_type = "dashboard-cold-terminal-#{System.unique_integer([:positive])}"
      hot_id = "dashboard-cold-terminal-hot-#{System.unique_integer([:positive])}"
      terminal_id = "dashboard-cold-terminal-done-#{System.unique_integer([:positive])}"
      previous_list = Application.get_env(:ferricstore, :flow_dashboard_flow_list_fun)
      test_pid = self()

      assert :ok =
               FerricStore.flow_create(hot_id,
                 type: flow_type,
                 state: "queued",
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )

      Application.put_env(:ferricstore, :flow_dashboard_flow_list_fun, fn ^flow_type, opts ->
        send(test_pid, {:terminal_list_dashboard_opts, opts})

        {:ok,
         [
           %{
             id: terminal_id,
             type: flow_type,
             state: "completed",
             partition_key: "tenant-cold-terminal",
             run_at_ms: 2_000,
             updated_at_ms: 2_000,
             attempts: 0
           }
         ]}
      end)

      on_exit(fn -> restore_env(:flow_dashboard_flow_list_fun, previous_list) end)

      data = Dashboard.collect_flow_states_page(state: "completed", limit: 25)

      assert_receive {:terminal_list_dashboard_opts, opts}
      assert opts[:state] == "completed"
      assert opts[:include_cold] == true
      assert opts[:consistent_projection] == true
      assert opts[:count] >= 25

      assert Enum.any?(data.records, &(Map.get(&1, :id) == terminal_id))
      assert Enum.any?(data.states, &(&1.state == "completed" and &1.count >= 1))
      assert "completed" in data.available_states
      assert data.filtered_sampled <= data.total_sampled
    end

    test "terminal state filter tolerates slower cold projection list reads" do
      flow_type = "dashboard-slow-terminal-#{System.unique_integer([:positive])}"
      hot_id = "dashboard-slow-terminal-hot-#{System.unique_integer([:positive])}"
      terminal_id = "dashboard-slow-terminal-done-#{System.unique_integer([:positive])}"
      previous_list = Application.get_env(:ferricstore, :flow_dashboard_flow_list_fun)

      previous_detail_timeout =
        Application.get_env(:ferricstore, :flow_dashboard_detail_fetch_timeout_ms)

      assert :ok =
               FerricStore.flow_create(hot_id,
                 type: flow_type,
                 state: "queued",
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )

      Application.put_env(:ferricstore, :flow_dashboard_detail_fetch_timeout_ms, 10)

      Application.put_env(:ferricstore, :flow_dashboard_flow_list_fun, fn ^flow_type, _opts ->
        Process.sleep(50)

        {:ok,
         [
           %{
             id: terminal_id,
             type: flow_type,
             state: "completed",
             partition_key: "tenant-slow-terminal",
             run_at_ms: 2_000,
             updated_at_ms: 2_000,
             attempts: 0
           }
         ]}
      end)

      on_exit(fn ->
        restore_env(:flow_dashboard_flow_list_fun, previous_list)
        restore_env(:flow_dashboard_detail_fetch_timeout_ms, previous_detail_timeout)
      end)

      data = Dashboard.collect_flow_states_page(state: "completed", limit: 25)

      assert Enum.any?(data.records, &(Map.get(&1, :id) == terminal_id))
    end

    test "type-filtered state page includes terminal records projected out of hot keydir" do
      flow_type = "dashboard-terminal-#{System.unique_integer([:positive])}"
      partition_key = "tenant-terminal-#{System.unique_integer([:positive])}"
      complete_id = "dashboard-terminal-complete-#{System.unique_integer([:positive])}"
      fail_id = "dashboard-terminal-fail-#{System.unique_integer([:positive])}"

      assert :ok =
               FerricStore.flow_create(complete_id,
                 type: flow_type,
                 partition_key: partition_key,
                 state: "queued",
                 run_at_ms: 1_000,
                 retention_ttl_ms: 60_000,
                 now_ms: 1_000
               )

      assert :ok =
               FerricStore.flow_create(fail_id,
                 type: flow_type,
                 partition_key: partition_key,
                 state: "queued",
                 run_at_ms: 1_000,
                 retention_ttl_ms: 60_000,
                 now_ms: 1_000
               )

      assert {:ok, [first_claim, second_claim]} =
               FerricStore.flow_claim_due(flow_type,
                 partition_key: partition_key,
                 worker: "dashboard-worker",
                 lease_ms: 30_000,
                 limit: 2,
                 now_ms: 1_000
               )

      claims_by_id = Map.new([first_claim, second_claim], &{Map.fetch!(&1, :id), &1})
      complete_claim = Map.fetch!(claims_by_id, complete_id)
      fail_claim = Map.fetch!(claims_by_id, fail_id)

      assert :ok =
               FerricStore.flow_complete(complete_id, complete_claim.lease_token,
                 partition_key: partition_key,
                 fencing_token: complete_claim.fencing_token,
                 result: "ok",
                 now_ms: 1_100
               )

      assert :ok =
               FerricStore.flow_fail(fail_id, fail_claim.lease_token,
                 partition_key: partition_key,
                 fencing_token: fail_claim.fencing_token,
                 error: "bad",
                 now_ms: 1_100
               )

      assert {:ok, %{state: "completed"}} =
               FerricStore.flow_get(complete_id, partition_key: partition_key)

      assert {:ok, %{state: "failed"}} =
               FerricStore.flow_get(fail_id, partition_key: partition_key)

      data = Dashboard.collect_flow_states_page(type: flow_type)

      assert Enum.any?(data.records, &(Map.get(&1, :id) == complete_id))
      assert Enum.any?(data.records, &(Map.get(&1, :id) == fail_id))
      assert Enum.any?(data.states, &(&1.state == "completed" and &1.count >= 1))
      assert Enum.any?(data.states, &(&1.state == "failed" and &1.count >= 1))
    end

    test "Flow states filter renders friendly time controls and parses datetime-local input" do
      from_ms =
        DateTime.to_unix(DateTime.from_naive!(~N[2026-05-25 12:30:00], "Etc/UTC"), :millisecond)

      to_ms =
        DateTime.to_unix(DateTime.from_naive!(~N[2026-05-25 13:00:00], "Etc/UTC"), :millisecond)

      opts =
        Dashboard.flow_states_opts_from_query(
          URI.encode_query(%{"from" => "2026-05-25T12:30", "to" => "2026-05-25T13:00"})
        )

      assert opts[:from_ms] == from_ms
      assert opts[:to_ms] == to_ms

      html =
        Dashboard.collect_flow_states_page(from_ms: from_ms, to_ms: to_ms)
        |> Dashboard.render_flow_states_page()

      assert String.contains?(html, ~s(name="range"))
      assert String.contains?(html, ~s(type="datetime-local"))
      assert String.contains?(html, ~s(title="Filter by workflow type"))
      assert String.contains?(html, ~s(title="Use a quick sliding window or Custom for From/To"))
      assert String.contains?(html, ~s(title="Apply Flow state filters"))
      assert String.contains?(html, ~s(value="2026-05-25T12:30"))
      assert String.contains?(html, ~s(value="2026-05-25T13:00"))
      refute String.contains?(html, ~s(value="#{from_ms}"))
    end

    test "Flow states quick range keeps a sliding live query" do
      before_ms = System.system_time(:millisecond)
      data = Dashboard.collect_flow_states_page(range: "1h", limit: 1)
      after_ms = System.system_time(:millisecond)
      html = Dashboard.render_flow_states_page(data)

      assert data.filters.range == "1h"
      assert data.from_ms >= before_ms - 3_600_000
      assert data.from_ms <= after_ms - 3_600_000
      assert data.to_ms == nil
      assert String.contains?(html, ~s(name="range"))
      assert String.contains?(html, ~s(value="1h" selected))
      assert Regex.match?(~r/id="flow-state-from-filter"[^>]*value=""/, html)
      assert Regex.match?(~r/id="flow-state-to-filter"[^>]*value=""/, html)
      assert String.contains?(html, "updated last 1 hour")
      assert String.contains?(html, "range=1h")
    end

    test "renders retry, failed, and expired lease help in state rows" do
      data = %{
        states: [
          %{
            type: "email",
            state: "queued",
            count: 3,
            due_now: 2,
            running: 0,
            retrying: 1,
            failed: 0,
            expired_leases: 0,
            max_attempts_reached: 0,
            oldest_due_ms: 1_000
          },
          %{
            type: "email",
            state: "running",
            count: 1,
            due_now: 0,
            running: 1,
            retrying: 0,
            failed: 0,
            expired_leases: 1,
            max_attempts_reached: 0,
            oldest_due_ms: 0
          },
          %{
            type: "email",
            state: "failed",
            count: 1,
            due_now: 0,
            running: 0,
            retrying: 0,
            failed: 1,
            expired_leases: 0,
            max_attempts_reached: 1,
            oldest_due_ms: 0
          }
        ],
        records: [],
        available_types: ["email"],
        type_filter: "email",
        total_sampled: 5,
        filtered_sampled: 5,
        sample_limit: 400
      }

      html = Dashboard.render_flow_states_page(data)

      assert String.contains?(html, "Retrying")
      assert String.contains?(html, "Failed")
      assert String.contains?(html, "Maxed")
      assert String.contains?(html, "lease deadline passed")
      assert String.contains?(html, "terminal failed")
      assert String.contains?(html, "attempts")
      assert String.contains?(html, ~s(role="img"))
      assert String.contains?(html, ~s(data-tooltip="Running flows whose lease deadline passed))
      assert String.contains?(html, ~s(data-tooltip="Updated quick ranges are sliding windows))
    end
  end

  describe "render_flow_workers_page/1" do
    test "renders workers and lease health" do
      data = Dashboard.collect_flow_workers_page()
      html = Dashboard.render_flow_workers_page(data)

      assert String.contains?(html, "Flow Workers")
      assert String.contains?(html, "Running")
      assert String.contains?(html, "Expired")
    end
  end

  describe "render_flow_due_page/1" do
    test "renders due and scheduled work" do
      data = Dashboard.collect_flow_due_page()
      html = Dashboard.render_flow_due_page(data)

      assert String.contains?(html, "Due / Scheduled")
      assert String.contains?(html, "Why Waiting")
      assert String.contains?(html, "Run At")
    end
  end

  describe "Flow failures, lineage, and query pages" do
    test "renders failures page with recovery controls and bounded records" do
      now_ms = System.system_time(:millisecond)

      html =
        Dashboard.render_flow_failures_page(%{
          filters: %{type: "email", partition_key: nil, q: nil, limit: 40, scan_exact: false},
          available_types: ["email"],
          flash: nil,
          summary: %{total: 1, failed: 1, expired_leases: 0, maxed: 1},
          total_sampled: 1,
          filtered_sampled: 1,
          sample_limit: 400,
          candidates: [
            %{
              id: "failed-flow",
              type: "email",
              state: "failed",
              attempts: 3,
              max_attempts: 3,
              updated_at_ms: now_ms
            }
          ]
        })

      assert String.contains?(html, "Flow Failures")
      assert String.contains?(html, "Recovery Actions")
      assert String.contains?(html, "FLOW.RECLAIM")
      assert String.contains?(html, "failed-flow")
      assert String.contains?(html, "terminal failed")
    end

    test "failures page does not run exact failure/stuck queries unless requested" do
      previous_failures = Application.get_env(:ferricstore, :flow_dashboard_flow_failures_fun)
      previous_stuck = Application.get_env(:ferricstore, :flow_dashboard_flow_stuck_fun)
      test_pid = self()

      Application.put_env(:ferricstore, :flow_dashboard_flow_failures_fun, fn type, _opts ->
        send(test_pid, {:exact_failure_scan, type})
        {:ok, []}
      end)

      Application.put_env(:ferricstore, :flow_dashboard_flow_stuck_fun, fn type, _opts ->
        send(test_pid, {:exact_stuck_scan, type})
        {:ok, []}
      end)

      on_exit(fn ->
        restore_env(:flow_dashboard_flow_failures_fun, previous_failures)
        restore_env(:flow_dashboard_flow_stuck_fun, previous_stuck)
      end)

      _data = Dashboard.collect_flow_failures_page(type: "email")
      refute_received {:exact_failure_scan, "email"}
      refute_received {:exact_stuck_scan, "email"}

      _data = Dashboard.collect_flow_failures_page(type: "email", scan_exact: true)
      assert_received {:exact_failure_scan, "email"}
      assert_received {:exact_stuck_scan, "email"}
    end

    test "exact failure scan errors are visible and not collapsed to authoritative zero" do
      previous_failures = Application.get_env(:ferricstore, :flow_dashboard_flow_failures_fun)
      previous_stuck = Application.get_env(:ferricstore, :flow_dashboard_flow_stuck_fun)

      Application.put_env(:ferricstore, :flow_dashboard_flow_failures_fun, fn _type, _opts ->
        {:error, :backend_down}
      end)

      Application.put_env(:ferricstore, :flow_dashboard_flow_stuck_fun, fn _type, _opts ->
        {:ok, []}
      end)

      on_exit(fn ->
        restore_env(:flow_dashboard_flow_failures_fun, previous_failures)
        restore_env(:flow_dashboard_flow_stuck_fun, previous_stuck)
      end)

      data = Dashboard.collect_flow_failures_page(type: "email", scan_exact: true)
      html = Dashboard.render_flow_failures_page(data)

      assert data.exact_scan_status.failures == {:error, :backend_down}
      assert data.exact_scan_status.stuck == :ok
      assert String.contains?(html, "Exact scan issue")
      assert String.contains?(html, "FLOW.FAILURES")
      assert String.contains?(html, "backend_down")
    end

    test "signals page does not read histories unless scan is requested" do
      flow_type = "dashboard-signal-scan-#{System.unique_integer([:positive])}"
      id = "dashboard-signal-scan-flow-#{System.unique_integer([:positive])}"
      previous_history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)
      test_pid = self()

      assert :ok =
               FerricStore.flow_create(id,
                 type: flow_type,
                 state: "queued",
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )

      Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn ^id, _opts ->
        send(test_pid, {:signal_history_scan, id})
        {:ok, []}
      end)

      on_exit(fn -> restore_env(:flow_dashboard_flow_history_fun, previous_history) end)

      _data = Dashboard.collect_flow_signals_page(type: flow_type)
      refute_received {:signal_history_scan, ^id}

      _data = Dashboard.collect_flow_signals_page(type: flow_type, scan_history: true)
      assert_received {:signal_history_scan, ^id}
    end

    test "lineage page uses injected root query and renders graph/table components" do
      previous = Application.get_env(:ferricstore, :flow_dashboard_flow_by_root_fun)
      test_pid = self()

      Application.put_env(:ferricstore, :flow_dashboard_flow_by_root_fun, fn "root-1", opts ->
        send(test_pid, {:lineage_opts, opts})

        {:ok,
         [
           %{
             id: "child-1",
             type: "order",
             state: "queued",
             root_flow_id: "root-1",
             parent_flow_id: "root-1",
             correlation_id: "order-1",
             updated_at_ms: 1_000
           }
         ]}
      end)

      on_exit(fn -> restore_env(:flow_dashboard_flow_by_root_fun, previous) end)

      data = Dashboard.collect_flow_lineage_page(mode: "root", target: "root-1", limit: 10)
      html = Dashboard.render_flow_lineage_page(data)

      assert_receive {:lineage_opts, opts}
      assert opts[:include_cold] == true
      assert opts[:consistent_projection] == true
      assert opts[:count] == 10
      assert data.result.command == "FLOW.BY_ROOT"
      assert String.contains?(html, "Flow Lineage")
      assert String.contains?(html, "Lineage Map")
      assert String.contains?(html, "child-1")
      assert String.contains?(html, "order-1")
    end

    test "query explorer renders idle and result states without payload hydration" do
      idle =
        Dashboard.collect_flow_query_page(kind: "history")
        |> Dashboard.render_flow_query_page()

      assert String.contains?(idle, "Flow Query Explorer")
      assert String.contains?(idle, "Enter an id")
      assert String.contains?(idle, "FLOW.HISTORY")
      assert String.contains?(idle, "Flow ID")
      assert String.contains?(idle, ~s(data-flow-query-form))
      assert String.contains?(idle, ~s(data-flow-query-kind))
      assert idle =~ ~r/data-flow-query-field="type"[^>]*hidden/
      assert idle =~ ~r/name="type"[^>]*disabled/
      assert idle =~ ~r/data-flow-query-field="from"[^>]*hidden/
      assert idle =~ ~r/name="from"[^>]*disabled/

      html =
        Dashboard.render_flow_query_page(%{
          filters: %{
            kind: "list",
            type: "email",
            state: nil,
            id: nil,
            partition_key: nil,
            limit: 40,
            from_ms: nil,
            to_ms: nil,
            rev: false
          },
          available_types: ["email"],
          total_sampled: 1,
          sample_limit: 400,
          result: %{
            status: :ok,
            command: "FLOW.LIST",
            message: "1 row",
            rows: [
              %{
                id: "query-flow",
                type: "email",
                state: "queued",
                updated_at_ms: 1_000
              }
            ]
          }
        })

      assert String.contains?(html, "Safe Query Explorer")
      assert String.contains?(html, "FLOW.LIST")
      assert String.contains?(html, "Workflow Type")
      assert String.contains?(html, "State")
      assert String.contains?(html, "From UTC")
      assert html =~ ~r/data-flow-query-field="id"[^>]*hidden/
      assert html =~ ~r/name="id"[^>]*disabled/
      assert String.contains?(html, "query-flow")
    end

    test "query explorer only renders fields relevant to lineage query kind" do
      html =
        Dashboard.collect_flow_query_page(kind: "by_correlation", id: "corr-1")
        |> Dashboard.render_flow_query_page()

      assert String.contains?(html, "FLOW.BY_CORRELATION")
      assert String.contains?(html, "Correlation ID")
      assert String.contains?(html, "Partition")
      assert String.contains?(html, "From UTC")
      assert html =~ ~r/data-flow-query-field="type"[^>]*hidden/
      assert html =~ ~r/name="type"[^>]*disabled/
      assert html =~ ~r/data-flow-query-field="state"[^>]*hidden/
      assert html =~ ~r/name="state"[^>]*disabled/
    end
  end

  describe "Flow overview projection health" do
    test "renders projection health inside overview instead of a separate page" do
      data = Dashboard.collect_flow_page()
      html = Dashboard.render_flow_page(data)

      assert String.contains?(html, "Projection Health")
      assert String.contains?(html, "LMDB")
      assert String.contains?(html, "lagged")
      assert String.contains?(html, "Pending")
      refute String.contains?(html, "LMDB Mode")
      refute String.contains?(String.downcase(html), "mirror")
      refute String.contains?(html, ~s(href="/dashboard/flow/projections"))
    end

    test "renders grouped projection health instead of raw metric names" do
      html =
        Dashboard.render_flow_page(%{
          summary: %{},
          types: [],
          workers: [],
          records: [],
          total_sampled: 0,
          sample_limit: 400,
          projection: %{
            lmdb_projection: :lagged,
            lmdb_flush_interval_ms: 1_000,
            history_flush_interval_ms: 0,
            metrics: [
              %{name: ~s(ferricstore_flow_lmdb_replay_safe_lag{shard_index="0"}), value: "7"},
              %{name: ~s(ferricstore_flow_lmdb_writer_pending_ops{shard_index="0"}), value: "3"},
              %{
                name:
                  ~s(ferricstore_flow_lmdb_projection_enqueue_failures_total{shard_index="0"}),
                value: "1"
              }
            ]
          }
        })

      assert String.contains?(html, "Health")
      assert String.contains?(html, "failures")
      assert String.contains?(html, ">7<")
      refute String.contains?(html, "ferricstore_flow_lmdb_replay_safe_lag")
    end
  end

  describe "collect_flow_detail_page/1" do
    test "renders Flow detail with waiting reason and history timeline" do
      id = "dashboard-flow-detail-#{System.unique_integer([:positive])}"

      assert :ok =
               FerricStore.flow_create(id,
                 type: "dashboard-detail",
                 state: "queued",
                 run_at_ms: System.system_time(:millisecond) + 60_000
               )

      data = Dashboard.collect_flow_detail_page(id)
      html = Dashboard.render_flow_detail_page(data)

      assert String.contains?(html, id)
      assert String.contains?(html, "Why waiting")
      assert String.contains?(html, "Timeline")
      assert String.contains?(html, "scheduled for future")
    end

    test "Flow detail timeline renders transition, retry, and terminal events" do
      flow_type = "dashboard-history-#{System.unique_integer([:positive])}"
      partition_key = "tenant-history-#{System.unique_integer([:positive])}"
      id = "dashboard-history-flow-#{System.unique_integer([:positive])}"

      assert :ok =
               FerricStore.flow_create(id,
                 type: flow_type,
                 partition_key: partition_key,
                 state: "queued",
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )

      assert :ok =
               FerricStore.flow_transition(id, "queued", "ready",
                 partition_key: partition_key,
                 fencing_token: 0,
                 run_at_ms: 1_100,
                 now_ms: 1_100
               )

      assert {:ok, [first_claim]} =
               FerricStore.flow_claim_due(flow_type,
                 state: "ready",
                 partition_key: partition_key,
                 worker: "worker-a",
                 lease_ms: 30_000,
                 limit: 1,
                 now_ms: 1_200
               )

      assert :ok =
               FerricStore.flow_retry(id, first_claim.lease_token,
                 partition_key: partition_key,
                 fencing_token: first_claim.fencing_token,
                 error: "temporary failure",
                 run_at_ms: 1_300,
                 now_ms: 1_250
               )

      assert {:ok, retried} = FerricStore.flow_get(id, partition_key: partition_key)

      assert {:ok, [second_claim]} =
               FerricStore.flow_claim_due(flow_type,
                 state: retried.state,
                 partition_key: partition_key,
                 worker: "worker-a",
                 lease_ms: 30_000,
                 limit: 1,
                 now_ms: 1_400
               )

      assert :ok =
               FerricStore.flow_complete(id, second_claim.lease_token,
                 partition_key: partition_key,
                 fencing_token: second_claim.fencing_token,
                 result: "ok",
                 now_ms: 1_500
               )

      html =
        id
        |> Dashboard.collect_flow_detail_page(partition_key: partition_key)
        |> Dashboard.render_flow_detail_page()

      assert String.contains?(html, "Created")
      assert String.contains?(html, "Transitioned")
      assert String.contains?(html, "Retry")
      assert String.contains?(html, "Completed")
      assert String.contains?(html, "queued -&gt; ready")
      assert String.contains?(html, "terminal")
      assert String.contains?(html, "worker-a")
      assert String.contains?(html, "result_ref")
      assert String.contains?(html, "error_ref")
      assert String.contains?(html, ~s(href="#flow-event-))
      assert String.contains?(html, ~s(id="flow-event-))
      assert String.contains?(html, "ready")
      assert String.contains?(html, "State graph")
      assert String.contains?(html, ~s(class="flow-timeline-graph"))
      assert String.contains?(html, "<svg")
      assert String.contains?(html, ~s(class="flow-timeline-path"))
      assert String.contains?(html, ~s(class="flow-timeline-duration-segment))
      assert String.contains?(html, ~s(class="flow-timeline-node))
      assert String.contains?(html, ~s(class="flow-timeline-lane-label"))
      assert String.contains?(html, ~s(class="flow-timeline-node flow-timeline-node-retry"))
      assert String.contains?(html, ~s(class="flow-timeline-duration-segment bar-red"))
      assert Regex.match?(~r/class="flow-timeline-node-label"[^>]*>ready<\/text>/, html)
      assert String.contains?(html, "running")
      assert String.contains?(html, "100ms")

      refute Regex.match?(
               ~r/class="flow-timeline-lane-label"[^>]*>Retry<\/text>/,
               html
             )

      refute Regex.match?(
               ~r/class="flow-timeline-node-label"[^>]*>Transitioned<\/text>/,
               html
             )

      refute String.contains?(
               html,
               ~s(class="timeline-chip"><span class="timeline-chip-index">1</span>Transitioned)
             )
    end

    test "Flow detail renders rewind targets from existing history states only" do
      data = %{
        id: "dashboard-rewind-flow",
        partition_key: "tenant-rewind",
        record: %{
          id: "dashboard-rewind-flow",
          type: "dashboard-rewind",
          state: "completed",
          partition_key: "tenant-rewind",
          run_at_ms: 1_000,
          updated_at_ms: 3_000,
          fencing_token: 3
        },
        record_status: :ok,
        history: [
          {"1000-1", %{"event" => "created", "state" => "queued"}},
          {"2000-2", %{"event" => "transitioned", "state" => "ready"}},
          {"3000-3", %{"event" => "completed", "state" => "completed"}},
          {"4000-4", %{"event" => "note"}}
        ],
        history_status: :ok,
        history_page: nil,
        value_refs: [],
        values_by_ref: %{},
        values_status: :skipped,
        waiting_reason: "terminal"
      }

      html = Dashboard.render_flow_detail_page(data)

      assert String.contains?(html, "Rewind")
      assert String.contains?(html, ~s(action="/dashboard/flow/dashboard-rewind-flow/rewind"))
      assert String.contains?(html, ~s(name="to_event"))
      assert String.contains?(html, ~s(value="1000-1"))
      assert String.contains?(html, "queued")
      assert String.contains?(html, ~s(value="2000-2"))
      assert String.contains?(html, "ready")
      refute String.contains?(html, ~s(value="4000-4"))
      refute String.contains?(html, ~s(name="state"))
      assert String.contains?(html, ~s(name="partition_key" value="tenant-rewind"))
      assert String.contains?(html, "Rewind creates a durable FLOW.REWIND")

      assert String.contains?(
               html,
               ~s(title="Choose one of this flow&#39;s loaded history events")
             )

      assert String.contains?(html, ~s(title="Create a durable rewind to the selected event"))
    end

    test "Flow detail renders signal events as a focused section" do
      data = %{
        id: "dashboard-signal-flow",
        partition_key: "tenant-signal",
        record: %{
          id: "dashboard-signal-flow",
          type: "dashboard-signal",
          state: "verify_payment",
          partition_key: "tenant-signal",
          run_at_ms: 1_250,
          updated_at_ms: 1_100,
          fencing_token: 1
        },
        record_status: :ok,
        history: [
          {"1000-1", %{"event" => "created", "state" => "waiting_payment"}},
          {"1100-2",
           %{
             "event" => "signaled",
             "signal" => "payment_received",
             "from_state" => "waiting_payment",
             "state" => "verify_payment",
             "value_refs" => ~s({"payment_event":{"ref":"ref:payment","version":1}})
           }}
        ],
        history_status: :ok,
        history_page: nil,
        value_refs: [],
        values_by_ref: %{},
        values_status: :skipped,
        waiting_reason: "due"
      }

      html = Dashboard.render_flow_detail_page(data)

      assert String.contains?(html, "Signals")
      assert String.contains?(html, "payment_received")
      assert String.contains?(html, "waiting_payment -> verify_payment")
      assert String.contains?(html, "payment_event")
      assert String.contains?(html, ~s(href="#flow-event-))
    end

    test "dashboard rewind form restores a selected existing history state" do
      flow_type = "dashboard-rewind-apply-#{System.unique_integer([:positive])}"
      partition_key = "tenant-rewind-#{System.unique_integer([:positive])}"
      id = "dashboard-rewind-apply-flow-#{System.unique_integer([:positive])}"

      assert :ok =
               FerricStore.flow_create(id,
                 type: flow_type,
                 partition_key: partition_key,
                 state: "queued",
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )

      assert {:ok, [{created_event_id, %{"state" => "queued"}} | _]} =
               FerricStore.flow_history(id, partition_key: partition_key, count: 10)

      assert :ok =
               FerricStore.flow_transition(id, "queued", "ready",
                 partition_key: partition_key,
                 fencing_token: 0,
                 run_at_ms: 2_000,
                 now_ms: 2_000
               )

      assert {:ok, ready} = FerricStore.flow_get(id, partition_key: partition_key)
      assert ready.state == "ready"

      assert {:ok, ^id, ^partition_key} =
               Dashboard.apply_flow_rewind_form(%{
                 "id" => id,
                 "partition_key" => partition_key,
                 "to_event" => created_event_id,
                 "confirm_rewind" => "true"
               })

      assert {:ok, rewound} = FerricStore.flow_get(id, partition_key: partition_key)
      assert rewound.state == "queued"
      assert rewound.rewound_to_event_id == created_event_id
    end

    test "dashboard rewind form rejects target events outside existing history" do
      flow_type = "dashboard-rewind-reject-#{System.unique_integer([:positive])}"
      id = "dashboard-rewind-reject-flow-#{System.unique_integer([:positive])}"

      assert :ok =
               FerricStore.flow_create(id,
                 type: flow_type,
                 state: "queued",
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )

      assert {:error, reason} =
               Dashboard.apply_flow_rewind_form(%{
                 "id" => id,
                 "to_event" => "999999-9",
                 "confirm_rewind" => "true"
               })

      assert reason =~ "target event"

      assert {:ok, record} = FerricStore.flow_get(id)
      assert record.state == "queued"
      assert Map.get(record, :rewound_to_event_id) in [nil, ""]
    end

    test "dashboard rewind form rejects same-type events from another flow" do
      flow_type = "dashboard-rewind-same-type-#{System.unique_integer([:positive])}"
      this_id = "dashboard-rewind-this-flow-#{System.unique_integer([:positive])}"
      other_id = "dashboard-rewind-other-flow-#{System.unique_integer([:positive])}"

      assert :ok =
               FerricStore.flow_create(this_id,
                 type: flow_type,
                 state: "queued",
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )

      assert :ok =
               FerricStore.flow_create(other_id,
                 type: flow_type,
                 state: "queued",
                 run_at_ms: 2_000,
                 now_ms: 2_000
               )

      assert :ok =
               FerricStore.flow_transition(other_id, "queued", "approval",
                 fencing_token: 0,
                 run_at_ms: 3_000,
                 now_ms: 3_000
               )

      assert {:ok, other_history} = FerricStore.flow_history(other_id, count: 10)

      {other_transition_event_id, _fields} =
        Enum.find(other_history, fn {_event_id, fields} ->
          fields["event"] == "transitioned" and fields["state"] == "approval"
        end)

      assert {:error, reason} =
               Dashboard.apply_flow_rewind_form(%{
                 "id" => this_id,
                 "to_event" => other_transition_event_id,
                 "confirm_rewind" => "true"
               })

      assert reason =~ "target event"

      assert {:ok, record} = FerricStore.flow_get(this_id)
      assert record.state == "queued"
      assert Map.get(record, :rewound_to_event_id) in [nil, ""]
    end

    test "Flow detail value refs open modal without rendering a visible inspector section" do
      id = "dashboard-flow-values/#{System.unique_integer([:positive])}"
      previous_get = Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun)
      previous_history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)
      previous_values = Application.get_env(:ferricstore, :flow_dashboard_flow_value_mget_fun)
      test_pid = self()

      Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn ^id, opts ->
        send(test_pid, {:dashboard_get_opts, opts})

        {:ok,
         %{
           id: id,
           type: "dashboard-values",
           state: "queued",
           partition_key: "tenant-values",
           run_at_ms: 1_000,
           updated_at_ms: 1_000,
           payload_ref: "flow-value:payload",
           value_refs: %{"invoice" => "flow-value:invoice"}
         }}
      end)

      Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn ^id, _opts ->
        {:ok,
         [
           {"1000-1",
            %{
              "event" => "created",
              "to_state" => "queued",
              "payload_ref" => "flow-value:payload"
            }}
         ]}
      end)

      Application.put_env(:ferricstore, :flow_dashboard_flow_value_mget_fun, fn refs ->
        send(test_pid, {:dashboard_value_refs, refs})
        {:ok, [%{"message" => "payload-value"}, %{"invoice_id" => "invoice-42"}]}
      end)

      on_exit(fn ->
        restore_env(:flow_dashboard_flow_get_fun, previous_get)
        restore_env(:flow_dashboard_flow_history_fun, previous_history)
        restore_env(:flow_dashboard_flow_value_mget_fun, previous_values)
      end)

      data = Dashboard.collect_flow_detail_page(id)
      html = Dashboard.render_flow_detail_page(data)

      assert_receive {:dashboard_get_opts, get_opts}
      assert get_opts[:payload] == false

      assert_receive {:dashboard_value_refs, ["flow-value:payload", "flow-value:invoice"]}
      assert String.contains?(html, "Value Inspector")
      refute String.contains?(html, ~s(data-live-component="flow_values"))
      assert String.contains?(html, ~s(data-live-component="flow_value_store"))
      assert String.contains?(html, ~s(href="#flow-value-))
      assert String.contains?(html, ~s(id="flow-value-))
      assert String.contains?(html, ~s(id="flow-value-store" hidden aria-hidden="true"))
      assert String.contains?(html, ~s(data-flow-value-ref="flow-value:payload"))
      assert String.contains?(html, ~s(data-flow-value-preview))
      refute String.contains?(html, "Value Preview")
      refute String.contains?(html, "Payload/result/error refs are clickable.")
      assert String.contains?(html, ~s(id="flow-value-modal"))
      assert String.contains?(html, ~s(id="flow-value-modal-copy"))
      assert String.contains?(html, "openFromHash")
      assert String.contains?(html, "flowValueRequestUrl")
      assert String.contains?(html, "flowValueAnchorFromHref")
      assert String.contains?(html, "openFromRef")
      assert String.contains?(html, "payload-value")
      assert String.contains?(html, "invoice-42")
      assert String.contains?(html, "flow-value:payload")
    end

    test "Flow value live payload fetches one authorized value on demand" do
      id = "dashboard-flow-value-api/#{System.unique_integer([:positive])}"
      partition_key = "tenant-value-#{System.unique_integer([:positive])}"
      previous_get = Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun)
      previous_history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)
      previous_values = Application.get_env(:ferricstore, :flow_dashboard_flow_value_mget_fun)
      test_pid = self()

      Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn ^id, opts ->
        send(test_pid, {:dashboard_value_api_get_opts, opts})

        {:ok,
         %{
           id: id,
           type: "email",
           state: "queued",
           partition_key: partition_key,
           payload_ref: "flow-value:payload",
           run_at_ms: 1_000,
           updated_at_ms: 1_000
         }}
      end)

      Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn ^id, _opts ->
        {:ok, [{"1000-1", %{"event" => "created", "payload_ref" => "flow-value:payload"}}]}
      end)

      Application.put_env(:ferricstore, :flow_dashboard_flow_value_mget_fun, fn refs ->
        send(test_pid, {:dashboard_value_api_refs, refs})
        {:ok, [%{"message" => "payload-value"}]}
      end)

      on_exit(fn ->
        restore_env(:flow_dashboard_flow_get_fun, previous_get)
        restore_env(:flow_dashboard_flow_history_fun, previous_history)
        restore_env(:flow_dashboard_flow_value_mget_fun, previous_values)
      end)

      query =
        URI.encode_query(%{
          "flow" => id,
          "partition_key" => partition_key,
          "ref" => "flow-value:payload"
        })

      assert {:ok, payload} = Dashboard.live_payload("flow/value?" <> query)
      assert payload.status == "ok"
      assert payload.ref == "flow-value:payload"
      assert payload.value =~ "payload-value"

      assert_receive {:dashboard_value_api_get_opts, opts}
      assert opts[:partition_key] == partition_key
      assert_receive {:dashboard_value_api_refs, ["flow-value:payload"]}
    end

    test "Flow value live payload refuses refs outside the visible flow detail" do
      id = "dashboard-flow-value-forbidden/#{System.unique_integer([:positive])}"
      previous_get = Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun)
      previous_history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)
      previous_values = Application.get_env(:ferricstore, :flow_dashboard_flow_value_mget_fun)
      test_pid = self()

      Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn ^id, _opts ->
        {:ok,
         %{
           id: id,
           type: "email",
           state: "queued",
           payload_ref: "flow-value:payload",
           run_at_ms: 1_000,
           updated_at_ms: 1_000
         }}
      end)

      Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn ^id, _opts ->
        {:ok, []}
      end)

      Application.put_env(:ferricstore, :flow_dashboard_flow_value_mget_fun, fn refs ->
        send(test_pid, {:unexpected_dashboard_value_api_refs, refs})
        {:ok, ["must-not-load"]}
      end)

      on_exit(fn ->
        restore_env(:flow_dashboard_flow_get_fun, previous_get)
        restore_env(:flow_dashboard_flow_history_fun, previous_history)
        restore_env(:flow_dashboard_flow_value_mget_fun, previous_values)
      end)

      query = URI.encode_query(%{"flow" => id, "ref" => "flow-value:other"})

      assert {:ok, payload} = Dashboard.live_payload("flow/value?" <> query)
      assert payload.status == "error"
      assert payload.error =~ "not visible"
      refute_receive {:unexpected_dashboard_value_api_refs, _}
    end

    test "renders Flow debug inspector with lease, storage, values, and history hints" do
      data = %{
        id: "debug-flow-1",
        waiting_reason: "leased by worker-1",
        record: %{
          id: "debug-flow-1",
          type: "email",
          state: "running",
          partition_key: "tenant-7",
          worker: "worker-1",
          priority: 9,
          attempts: 2,
          max_attempts: 5,
          lease_token: "lease-abc",
          fencing_token: 12,
          run_at_ms: System.system_time(:millisecond) - 1_000,
          lease_expires_at_ms: System.system_time(:millisecond) + 30_000,
          updated_at_ms: System.system_time(:millisecond),
          payload_ref: "flow-value:payload",
          result_ref: "flow-value:result",
          value_refs: %{"invoice" => "flow-value:invoice"}
        },
        history: [
          {1, %{event: "create", to_state: "queued", payload_ref: "flow-value:payload"}},
          {2, %{event: "claim", from_state: "queued", to_state: "running"}}
        ]
      }

      html = Dashboard.render_flow_detail_page(data)

      assert String.contains?(html, "Debug Inspector")
      assert String.contains?(html, "Lease")
      assert String.contains?(html, "running until")
      assert String.contains?(html, "Storage")
      assert String.contains?(html, "tenant-7")
      assert String.contains?(html, "Values")
      assert String.contains?(html, "payload_ref")
      assert String.contains?(html, "invoice")
      assert String.contains?(html, "History")
      assert String.contains?(html, "2 events")
    end

    test "Flow detail treats lease_deadline_ms as lease expiry" do
      deadline = System.system_time(:millisecond) + 30_000

      html =
        Dashboard.render_flow_detail_page(%{
          id: "debug-flow-lease-deadline",
          waiting_reason: "leased by worker-1",
          record: %{
            id: "debug-flow-lease-deadline",
            type: "email",
            state: "running",
            partition_key: "tenant-7",
            lease_owner: "worker-1",
            lease_token: "lease-abc",
            fencing_token: 12,
            run_at_ms: System.system_time(:millisecond) - 1_000,
            lease_deadline_ms: deadline,
            updated_at_ms: System.system_time(:millisecond)
          },
          history: []
        })

      assert String.contains?(html, "running until")
      refute String.contains?(html, "running without lease expiry")
    end

    test "Flow overview exposes a search form for direct flow debugging" do
      html = Dashboard.collect_flow_page() |> Dashboard.render_flow_page()

      assert String.contains?(html, ~s(action="/dashboard/flow/lookup"))
      assert String.contains?(html, ~s(name="id"))
      assert String.contains?(html, ~s(name="partition_key"))
      assert String.contains?(html, "Search flow ID")
      assert String.contains?(html, ~s(title="Open a flow by ID."))

      assert String.contains?(
               html,
               ~s(title="With a Flow ID, scopes the detail lookup. Without a Flow ID, filters the overview to this partition.")
             )

      assert String.contains?(html, ~s(title="Open a flow by ID or filter overview by partition"))
    end

    test "Flow overview can be scoped to a searched partition key" do
      partition_key = "tenant-search-#{System.unique_integer([:positive])}"
      other_partition_key = "tenant-other-#{System.unique_integer([:positive])}"
      matching_id = "dashboard-partition-search-match-#{System.unique_integer([:positive])}"
      hidden_id = "dashboard-partition-search-hidden-#{System.unique_integer([:positive])}"

      assert :ok =
               FerricStore.flow_create(matching_id,
                 type: "dashboard-partition-search",
                 state: "queued",
                 partition_key: partition_key,
                 run_at_ms: 1_000
               )

      assert :ok =
               FerricStore.flow_create(hidden_id,
                 type: "dashboard-partition-search",
                 state: "queued",
                 partition_key: other_partition_key,
                 run_at_ms: 1_000
               )

      data = Dashboard.collect_flow_page(partition_key: partition_key)
      html = Dashboard.render_flow_page(data)

      assert Enum.all?(data.records, &(Map.get(&1, :partition_key) == partition_key))
      assert String.contains?(html, partition_key)
      assert String.contains?(html, matching_id)
      refute String.contains?(html, hidden_id)

      assert String.contains?(
               html,
               "/dashboard/api/flow?#{URI.encode_query(%{"partition_key" => partition_key})}"
             )
    end

    test "Flow overview detail links preserve partition key" do
      id = "dashboard-flow-partitioned/#{System.unique_integer([:positive])}"
      partition_key = "tenant/debug #{System.unique_integer([:positive])}"

      data =
        Dashboard.collect_flow_page()
        |> Map.merge(%{
          summary: %{},
          types: [],
          workers: [],
          records: [
            %{
              id: id,
              type: "dashboard-partitioned",
              state: "queued",
              partition_key: partition_key,
              run_at_ms: 1_000,
              updated_at_ms: 1_000
            }
          ],
          total_sampled: 1,
          sample_limit: 400
        })

      html = Dashboard.render_flow_page(data)
      encoded_id = URI.encode(id, &URI.char_unreserved?/1)
      encoded_partition = URI.encode_query(%{"partition_key" => partition_key})

      assert String.contains?(html, ~s(href="/dashboard/flow/#{encoded_id}?#{encoded_partition}"))
    end

    test "Flow detail page is live-watchable without full-page refresh" do
      id = "dashboard-flow-watch/#{System.unique_integer([:positive])}"

      assert :ok =
               FerricStore.flow_create(id,
                 type: "dashboard-watch",
                 state: "queued",
                 run_at_ms: 1_000
               )

      encoded_id = URI.encode(id, &URI.char_unreserved?/1)
      html = id |> Dashboard.collect_flow_detail_page() |> Dashboard.render_flow_detail_page()

      assert String.contains?(html, ~s(data-dashboard-live-page="flow-detail"))

      assert String.contains?(
               html,
               ~s(data-dashboard-live-url="/dashboard/api/flow/#{encoded_id}")
             )

      assert String.contains?(html, ~s(data-live-component="flow_detail"))
      assert String.contains?(html, ~s(data-live-component="flow_debug"))
      assert String.contains?(html, ~s(data-live-component="flow_history"))
      refute String.contains?(html, ~s(http-equiv="refresh"))
    end

    test "Flow detail uses partition key and consistent HISTORY projection" do
      id = "dashboard-flow-detail-partitioned/#{System.unique_integer([:positive])}"
      partition_key = "tenant-detail-#{System.unique_integer([:positive])}"
      previous_get = Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun)
      previous_history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)
      test_pid = self()

      Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn ^id, opts ->
        send(test_pid, {:dashboard_get_opts, opts})

        {:ok,
         %{
           id: id,
           type: "dashboard-partitioned",
           state: "queued",
           partition_key: partition_key,
           run_at_ms: 1_000,
           updated_at_ms: 1_000
         }}
      end)

      Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn ^id, opts ->
        send(test_pid, {:dashboard_history_opts, opts})
        {:ok, [{"1000-1", %{"event" => "created", "to_state" => "queued"}}]}
      end)

      on_exit(fn ->
        restore_env(:flow_dashboard_flow_get_fun, previous_get)
        restore_env(:flow_dashboard_flow_history_fun, previous_history)
      end)

      data = Dashboard.collect_flow_detail_page(id, partition_key: partition_key)
      html = Dashboard.render_flow_detail_page(data)

      assert data.record.id == id
      assert data.partition_key == partition_key
      assert String.contains?(html, partition_key)

      assert_receive {:dashboard_get_opts, get_opts}
      assert get_opts[:payload] == false
      assert get_opts[:partition_key] == partition_key

      assert_receive {:dashboard_history_opts, history_opts}
      assert history_opts[:partition_key] == partition_key
      assert history_opts[:consistent_projection] == true
    end

    test "Flow detail paginates older history with an inclusive event cursor" do
      id = "dashboard-flow-history-before/#{System.unique_integer([:positive])}"
      previous_get = Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun)
      previous_history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)
      test_pid = self()

      Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn ^id, _opts ->
        {:ok,
         %{
           id: id,
           type: "dashboard-history-page",
           state: "s3",
           partition_key: "tenant-history-page",
           run_at_ms: 1_000,
           updated_at_ms: 4_000
         }}
      end)

      Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn ^id, opts ->
        send(test_pid, {:dashboard_history_opts, opts})

        {:ok,
         [
           {"3000-3", %{"event" => "transitioned", "state" => "s3", "version" => "3"}},
           {"2000-2", %{"event" => "transitioned", "state" => "s2", "version" => "2"}},
           {"1000-1", %{"event" => "created", "state" => "s1", "version" => "1"}}
         ]}
      end)

      on_exit(fn ->
        restore_env(:flow_dashboard_flow_get_fun, previous_get)
        restore_env(:flow_dashboard_flow_history_fun, previous_history)
      end)

      data =
        Dashboard.collect_flow_detail_page(id,
          history_before: "3000-3",
          history_count: 1
        )

      assert_receive {:dashboard_history_opts, history_opts}
      assert history_opts[:to_event] == "3000-3"
      assert history_opts[:rev] == true
      assert history_opts[:count] == 3

      assert Enum.map(data.history, &elem(&1, 0)) == ["2000-2"]
      assert data.history_page.has_older == true
      assert data.history_page.has_newer == true
      assert data.history_page.oldest_event_id == "2000-2"
      assert data.history_page.newest_event_id == "2000-2"
    end

    test "Flow detail renders cursor pagination controls for history" do
      html =
        Dashboard.render_flow_detail_page(%{
          id: "dashboard-flow-history-links",
          partition_key: "tenant history links",
          record: %{
            id: "dashboard-flow-history-links",
            type: "debug",
            state: "s2",
            partition_key: "tenant history links",
            run_at_ms: 1_000,
            updated_at_ms: 2_000
          },
          record_status: :ok,
          history: [
            {"1000-1", %{"event" => "created", "state" => "s1", "version" => "1"}},
            {"2000-2", %{"event" => "transitioned", "state" => "s2", "version" => "2"}}
          ],
          history_status: :ok,
          history_page: %{
            id: "dashboard-flow-history-links",
            partition_key: "tenant history links",
            count: 2,
            has_older: true,
            has_newer: true,
            oldest_event_id: "1000-1",
            newest_event_id: "2000-2",
            before: nil,
            after: nil,
            older_url:
              "/dashboard/flow/dashboard-flow-history-links?history_before=1000-1&history_count=2&partition_key=tenant+history+links",
            newer_url:
              "/dashboard/flow/dashboard-flow-history-links?history_after=2000-2&history_count=2&partition_key=tenant+history+links"
          },
          value_refs: [],
          values_by_ref: %{},
          values_status: :skipped,
          waiting_reason: "ready"
        })

      assert String.contains?(html, "History page")
      assert String.contains?(html, "Older")
      assert String.contains?(html, "Newer")
      assert String.contains?(html, "history_before=1000-1")
      assert String.contains?(html, "history_after=2000-2")
      assert String.contains?(html, "history_count=2")
    end

    test "Flow detail lookup is bounded when durable lookup stalls" do
      previous_timeout =
        Application.get_env(:ferricstore, :flow_dashboard_detail_fetch_timeout_ms)

      previous_get = Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun)

      Application.put_env(:ferricstore, :flow_dashboard_detail_fetch_timeout_ms, 10)

      Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn _id ->
        Process.sleep(1_000)
        {:ok, %{id: "too-slow"}}
      end)

      on_exit(fn ->
        restore_env(:flow_dashboard_detail_fetch_timeout_ms, previous_timeout)
        restore_env(:flow_dashboard_flow_get_fun, previous_get)
      end)

      {elapsed_us, data} = :timer.tc(fn -> Dashboard.collect_flow_detail_page("slow-flow") end)
      html = Dashboard.render_flow_detail_page(data)

      assert elapsed_us < 300_000
      assert data.record == nil
      assert data.record_status == :timeout
      assert String.contains?(html, "lookup timed out")
    end

    test "Flow detail history is bounded when history lookup stalls" do
      previous_timeout =
        Application.get_env(:ferricstore, :flow_dashboard_detail_fetch_timeout_ms)

      previous_get = Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun)
      previous_history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)

      Application.put_env(:ferricstore, :flow_dashboard_detail_fetch_timeout_ms, 10)

      Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn id ->
        {:ok,
         %{
           id: id,
           type: "debug",
           state: "queued",
           partition_key: "tenant-1",
           run_at_ms: 1_000,
           updated_at_ms: 1_000
         }}
      end)

      Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn _id, _opts ->
        Process.sleep(1_000)
        {:ok, [{1, %{event: "create", to_state: "queued"}}]}
      end)

      on_exit(fn ->
        restore_env(:flow_dashboard_detail_fetch_timeout_ms, previous_timeout)
        restore_env(:flow_dashboard_flow_get_fun, previous_get)
        restore_env(:flow_dashboard_flow_history_fun, previous_history)
      end)

      {elapsed_us, data} =
        :timer.tc(fn -> Dashboard.collect_flow_detail_page("slow-history-flow") end)

      html = Dashboard.render_flow_detail_page(data)

      assert elapsed_us < 300_000
      assert data.record.id == "slow-history-flow"
      assert data.history == []
      assert data.history_status == :timeout
      assert String.contains?(html, "History temporarily unavailable")
    end

    test "Flow detail emits telemetry for bounded dashboard lookups" do
      previous_timeout =
        Application.get_env(:ferricstore, :flow_dashboard_detail_fetch_timeout_ms)

      previous_get = Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun)
      previous_history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)
      test_pid = self()

      :telemetry.attach(
        {__MODULE__, :dashboard_flow_lookup, test_pid},
        [:ferricstore, :dashboard, :flow, :lookup],
        &__MODULE__.handle_dashboard_flow_lookup_event/4,
        test_pid
      )

      Application.put_env(:ferricstore, :flow_dashboard_detail_fetch_timeout_ms, 10)

      Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn id ->
        {:ok,
         %{
           id: id,
           type: "debug",
           state: "queued",
           partition_key: "tenant-1",
           run_at_ms: 1_000,
           updated_at_ms: 1_000
         }}
      end)

      Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn _id, _opts ->
        Process.sleep(1_000)
        {:ok, [{1, %{event: "create", to_state: "queued"}}]}
      end)

      on_exit(fn ->
        :telemetry.detach({__MODULE__, :dashboard_flow_lookup, test_pid})
        restore_env(:flow_dashboard_detail_fetch_timeout_ms, previous_timeout)
        restore_env(:flow_dashboard_flow_get_fun, previous_get)
        restore_env(:flow_dashboard_flow_history_fun, previous_history)
      end)

      data = Dashboard.collect_flow_detail_page("profiled-history-flow")

      assert data.history_status == :timeout

      assert_receive {:dashboard_flow_lookup, [:ferricstore, :dashboard, :flow, :lookup],
                      %{duration_us: duration_us, timeout_ms: 10},
                      %{operation: :record, result: :ok}},
                     500

      assert is_integer(duration_us)
      assert duration_us >= 0

      assert_receive {:dashboard_flow_lookup, [:ferricstore, :dashboard, :flow, :lookup],
                      %{duration_us: history_duration_us, timeout_ms: 10},
                      %{operation: :history, result: :timeout}},
                     500

      assert is_integer(history_duration_us)
      assert history_duration_us >= 10_000
    end
  end

  # ---------------------------------------------------------------------------
  # Sub-page HTTP endpoints
  # ---------------------------------------------------------------------------

  describe "GET /dashboard/config sub-page" do
    test "returns 200 with config page HTML" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard/config")

      assert response =~ "HTTP/1.1 200 OK"
      assert response =~ "text/html"

      body = extract_body(response)
      assert String.contains?(body, "Namespace Config")
      assert String.contains?(body, "Configuration Commands")
      assert String.contains?(body, "Runtime Parameters")
    end

    test "shows built-in defaults when no overrides exist" do
      NamespaceConfig.reset_all()
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard/config")
      body = extract_body(response)

      assert String.contains?(body, "built-in default window")
    end
  end

  describe "collect_flow_policies_page/0 and render_flow_policies_page/1" do
    test "renders a create and update policy form" do
      html =
        Dashboard.collect_flow_policies_page()
        |> Dashboard.render_flow_policies_page()

      assert String.contains?(html, ~s(id="flow-policy-editor"))
      assert String.contains?(html, ~s(method="post"))
      assert String.contains?(html, ~s(action="/dashboard/flow/policies"))
      assert String.contains?(html, ~s(name="type"))
      assert String.contains?(html, ~s(name="max_retries"))
      assert String.contains?(html, ~s(name="retention_ttl_ms"))
      refute String.contains?(html, ~s(name="history_hot_max_events"))
      refute String.contains?(html, "Hot history")
      assert String.contains?(html, "Create / Update Policy")
      assert String.contains?(html, "Policies affect new Flow work and retry scheduling")
      assert String.contains?(html, ~s(title="Save this Flow policy"))

      assert String.contains?(
               html,
               ~s(title="Terminal state used when retry attempts are exhausted")
             )
    end

    test "shows configured policy-only Flow types and command reference" do
      type = "dashboard-policy-only-#{System.unique_integer([:positive])}"

      assert {:ok, _policy} =
               FerricStore.flow_policy_set(type,
                 retry: [
                   max_retries: 5,
                   backoff: [kind: :fixed, base_ms: 100, max_ms: 500, jitter_pct: 0],
                   exhausted_to: "dead"
                 ],
                 retention: [
                   ttl_ms: 60_000,
                   history_max_events: 50
                 ],
                 states: [
                   {"review",
                    [
                      retry: [max_retries: 1, exhausted_to: "review_failed"],
                      retention: [
                        ttl_ms: 30_000,
                        history_max_events: 10
                      ]
                    ]}
                 ]
               )

      data = Dashboard.collect_flow_policies_page()

      assert Enum.any?(data.policies, &(&1.type == type))

      html = Dashboard.render_flow_policies_page(data)

      assert String.contains?(html, "FerricFlow Policies")
      assert String.contains?(html, "FLOW.POLICY.SET &lt;type&gt;")
      assert String.contains?(html, "FLOW.POLICY.GET &lt;type&gt;")
      assert String.contains?(html, "MAX_RETRIES")
      assert String.contains?(html, "RETENTION_TTL_MS")
      assert String.contains?(html, type)
      assert String.contains?(html, "configured")
      assert String.contains?(html, "fixed 100ms")
      assert String.contains?(html, "dead")
      assert String.contains?(html, "review")
    end

    test "policy form updates global policy without dropping state overrides" do
      type = "dashboard-policy-form-#{System.unique_integer([:positive])}"

      assert {:ok, _policy} =
               FerricStore.flow_policy_set(type,
                 retry: [
                   max_retries: 2,
                   backoff: [kind: :fixed, base_ms: 100, max_ms: 200, jitter_pct: 0],
                   exhausted_to: "failed"
                 ],
                 retention: [
                   ttl_ms: 60_000,
                   history_max_events: 10
                 ],
                 states: [
                   {"review",
                    [
                      retry: [max_retries: 7, exhausted_to: "review_failed"],
                      retention: [
                        ttl_ms: 30_000,
                        history_max_events: 5
                      ]
                    ]}
                 ]
               )

      assert {:ok, ^type} =
               Dashboard.apply_flow_policy_form(%{
                 "type" => type,
                 "max_retries" => "4",
                 "backoff_kind" => "linear",
                 "base_ms" => "250",
                 "max_ms" => "2000",
                 "jitter_pct" => "10",
                 "exhausted_to" => "dead",
                 "retention_ttl_ms" => "120000",
                 "history_max_events" => "20"
               })

      assert {:ok, policy} = FerricStore.flow_policy_get(type)
      assert policy.retry.max_retries == 4
      assert policy.retry.backoff.kind == :linear
      assert policy.retry.exhausted_to == "dead"
      assert policy.retention.ttl_ms == 120_000
      refute Map.has_key?(policy.retention, :history_hot_max_events)
      assert policy.states["review"].retry.max_retries == 7
      assert policy.states["review"].retry.exhausted_to == "review_failed"
      assert policy.states["review"].retention.history_max_events == 5
    end
  end

  describe "collect_flow_retention_page/1 and render_flow_retention_page/1" do
    test "collects storage bytes from nested Flow/WARaft storage" do
      old_data_dir = Application.get_env(:ferricstore, :data_dir)
      old_shard_count = Application.get_env(:ferricstore, :shard_count)

      data_dir =
        Path.join(System.tmp_dir!(), "dashboard-storage-#{System.unique_integer([:positive])}")

      shard_dir = Ferricstore.DataDir.shard_data_path(data_dir, 0)
      history_dir = Path.join(shard_dir, "history")
      waraft_dir = Path.join([data_dir, "waraft", "shard_0"])

      File.mkdir_p!(history_dir)
      File.mkdir_p!(waraft_dir)
      File.write!(Path.join(shard_dir, "active.log"), String.duplicate("a", 7))
      File.write!(Path.join(history_dir, "history.segment"), String.duplicate("b", 11))
      File.write!(Path.join(waraft_dir, "raft.segment"), String.duplicate("c", 13))

      Application.put_env(:ferricstore, :data_dir, data_dir)
      Application.put_env(:ferricstore, :shard_count, 1)

      on_exit(fn ->
        File.rm_rf!(data_dir)
        restore_env(:data_dir, old_data_dir)
        restore_env(:shard_count, old_shard_count)
      end)

      storage = Dashboard.collect_storage_page()
      retention = Dashboard.collect_flow_retention_page()
      shard = Enum.find(storage.shards, &(&1.index == 0))

      assert storage.total_disk_bytes >= 31
      assert %{disk_bytes: shard_bytes, data_file_count: 1, hint_file_count: 0} = shard
      assert shard_bytes >= 18
      assert retention.storage.total_disk_bytes >= 31
    end

    test "renders retention controls, command reference, and sampled candidates" do
      now_ms = System.system_time(:millisecond)

      html =
        Dashboard.render_flow_retention_page(%{
          now_ms: now_ms,
          limit: 25,
          sample_limit: 400,
          total_sampled: 2,
          terminal_sampled: 1,
          active_sampled: 1,
          eligible_sampled: 1,
          storage: %{total_disk_bytes: 1_234_567},
          projection: Dashboard.default_flow_projection_health(),
          flash: nil,
          candidates: [
            %{
              id: "retention-flow-1",
              type: "email",
              state: "completed",
              partition_key: "tenant-a",
              terminal_retention_until_ms: now_ms - 1_000,
              updated_at_ms: now_ms - 2_000,
              attempts: 2
            }
          ]
        })

      assert String.contains?(html, "FerricFlow Retention")
      assert String.contains?(html, ~s(action="/dashboard/flow/retention"))
      assert String.contains?(html, "Dry Run")
      assert String.contains?(html, "Run Cleanup")
      assert String.contains?(html, "FLOW.RETENTION_CLEANUP")
      assert String.contains?(html, "retention-flow-1")
      assert String.contains?(html, "terminal Flow records")
    end

    test "dry-run form does not execute cleanup and cleanup form delegates with limit" do
      test_pid = self()

      Application.put_env(:ferricstore, :flow_dashboard_retention_cleanup_fun, fn opts ->
        send(test_pid, {:retention_cleanup, opts})
        {:ok, %{flows: 1, history: 2, values: 3}}
      end)

      on_exit(fn ->
        Application.delete_env(:ferricstore, :flow_dashboard_retention_cleanup_fun)
      end)

      assert {:ok, :dry_run, %{limit: 50}} =
               Dashboard.apply_flow_retention_form(%{
                 "action" => "dry_run",
                 "limit" => "50"
               })

      refute_received {:retention_cleanup, _opts}

      assert {:ok, :cleanup, %{flows: 1, history: 2, values: 3, limit: 7}} =
               Dashboard.apply_flow_retention_form(%{
                 "action" => "cleanup",
                 "limit" => "7",
                 "confirm_cleanup" => "true"
               })

      assert_received {:retention_cleanup, [limit: 7]}
    end
  end

  describe "GET /dashboard/flow sub-pages" do
    test "returns 200 with Flow page HTML" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard/flow")

      assert response =~ "HTTP/1.1 200 OK"
      assert response =~ "text/html"

      body = extract_body(response)
      assert String.contains?(body, "FerricFlow")
      assert String.contains?(body, "Flow Overview")
    end

    test "redirects removed Flow config page to global config" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard/flow/config")

      assert response =~ "HTTP/1.1 302 Found"
      assert response =~ "Location: /dashboard/config"
    end

    test "returns 200 with Flow detail page HTML" do
      id = "dashboard-flow-http-#{System.unique_integer([:positive])}"

      assert :ok =
               FerricStore.flow_create(id,
                 type: "dashboard-http",
                 state: "queued",
                 run_at_ms: 1_000
               )

      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard/flow/#{id}")

      assert response =~ "HTTP/1.1 200 OK"

      body = extract_body(response)
      assert String.contains?(body, id)
      assert String.contains?(body, "Flow Detail")
    end

    test "redirects Flow lookup searches to encoded detail URLs" do
      id = "dashboard-flow-lookup/#{System.unique_integer([:positive])} with space"

      assert :ok =
               FerricStore.flow_create(id,
                 type: "dashboard-lookup",
                 state: "queued",
                 run_at_ms: 1_000
               )

      port = HealthEndpoint.port()
      query = URI.encode_query(%{"id" => id})
      encoded_id = URI.encode(id, &URI.char_unreserved?/1)
      response = http_get(port, "/dashboard/flow/lookup?#{query}")

      assert response =~ "HTTP/1.1 302 Found"
      assert response =~ "Location: /dashboard/flow/#{encoded_id}"
    end

    test "redirects Flow lookup searches with partition key" do
      id = "dashboard-flow-lookup-partitioned/#{System.unique_integer([:positive])}"
      partition_key = "tenant lookup #{System.unique_integer([:positive])}"

      port = HealthEndpoint.port()
      query = URI.encode_query(%{"id" => id, "partition_key" => partition_key})
      encoded_id = URI.encode(id, &URI.char_unreserved?/1)
      encoded_partition = URI.encode_query(%{"partition_key" => partition_key})
      response = http_get(port, "/dashboard/flow/lookup?#{query}")

      assert response =~ "HTTP/1.1 302 Found"
      assert response =~ "Location: /dashboard/flow/#{encoded_id}?#{encoded_partition}"
    end

    test "redirects partition-only Flow lookup searches to scoped overview" do
      partition_key = "tenant only #{System.unique_integer([:positive])}"

      port = HealthEndpoint.port()
      query = URI.encode_query(%{"partition_key" => partition_key})
      encoded_partition = URI.encode_query(%{"partition_key" => partition_key})
      response = http_get(port, "/dashboard/flow/lookup?#{query}")

      assert response =~ "HTTP/1.1 302 Found"
      assert response =~ "Location: /dashboard/flow?#{encoded_partition}"
    end

    test "returns Flow detail page for explicitly partitioned records" do
      id = "dashboard-flow-http-partitioned/#{System.unique_integer([:positive])}"
      partition_key = "tenant-http-#{System.unique_integer([:positive])}"

      assert :ok =
               FerricStore.flow_create(id,
                 type: "dashboard-http-partitioned",
                 partition_key: partition_key,
                 state: "queued",
                 run_at_ms: 1_000
               )

      port = HealthEndpoint.port()
      encoded_id = URI.encode(id, &URI.char_unreserved?/1)
      encoded_partition = URI.encode_query(%{"partition_key" => partition_key})
      response = http_get(port, "/dashboard/flow/#{encoded_id}?#{encoded_partition}")

      assert response =~ "HTTP/1.1 200 OK"

      body = extract_body(response)
      assert String.contains?(body, id)
      assert String.contains?(body, partition_key)
      refute String.contains?(body, "was not found")
    end

    test "returns Flow detail live component JSON" do
      id = "dashboard-flow-api/#{System.unique_integer([:positive])}"

      assert :ok =
               FerricStore.flow_create(id,
                 type: "dashboard-api",
                 state: "queued",
                 run_at_ms: 1_000
               )

      port = HealthEndpoint.port()
      encoded_id = URI.encode(id, &URI.char_unreserved?/1)
      response = http_get(port, "/dashboard/api/flow/#{encoded_id}")

      assert response =~ "HTTP/1.1 200 OK"
      assert response =~ "application/json"

      {:ok, decoded} = response |> extract_body() |> Jason.decode()
      assert is_integer(decoded["generated_at_ms"])
      assert is_binary(decoded["components"]["flow_detail"])
      assert is_binary(decoded["components"]["flow_debug"])
      assert is_binary(decoded["components"]["flow_history"])
      assert decoded["components"]["flow_detail"] =~ id
      assert decoded["components"]["flow_debug"] =~ "Debug Inspector"
    end

    test "returns 200 for Flow states, workers, due, and policies pages" do
      port = HealthEndpoint.port()

      for {path, title} <- [
            {"/dashboard/flow/states", "Flow States"},
            {"/dashboard/flow/workers", "Flow Workers"},
            {"/dashboard/flow/due", "Due / Scheduled"},
            {"/dashboard/flow/failures", "Flow Failures"},
            {"/dashboard/flow/lineage", "Flow Lineage"},
            {"/dashboard/flow/query", "Flow Query Explorer"},
            {"/dashboard/flow/signals", "Flow Signals"},
            {"/dashboard/flow/policies", "FerricFlow Policies"},
            {"/dashboard/flow/retention", "FerricFlow Retention"}
          ] do
        response = http_get(port, path)

        assert response =~ "HTTP/1.1 200 OK"
        assert response =~ "text/html"
        assert response |> extract_body() |> String.contains?(title)
      end
    end

    test "returns 200 for KV dashboard pages" do
      port = HealthEndpoint.port()

      for {path, title} <- [
            {"/dashboard/keyspace", "Keyspace"},
            {"/dashboard/keyspace?key=missing-key", "Keyspace"},
            {"/dashboard/commands", "Commands"},
            {"/dashboard/reads", "Read Path"}
          ] do
        response = http_get(port, path)

        assert response =~ "HTTP/1.1 200 OK"
        assert response =~ "text/html"
        assert response |> extract_body() |> String.contains?(title)
      end
    end

    test "redirects removed Flow projections page back to overview" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard/flow/projections")

      assert response =~ "HTTP/1.1 302 Found"
      assert response =~ "Location: /dashboard/flow"
    end

    test "POST /dashboard/flow/policies creates policy and redirects with status" do
      port = HealthEndpoint.port()
      type = "dashboard-policy-http-#{System.unique_integer([:positive])}"

      response =
        http_post_form(port, "/dashboard/flow/policies", %{
          "type" => type,
          "max_retries" => "6",
          "backoff_kind" => "fixed",
          "base_ms" => "50",
          "max_ms" => "500",
          "jitter_pct" => "5",
          "exhausted_to" => "dead",
          "retention_ttl_ms" => "60000",
          "history_max_events" => "25"
        })

      assert response =~ "HTTP/1.1 302 Found"
      assert response =~ "Location: /dashboard/flow/policies?"
      assert response =~ "status=ok"

      assert {:ok, policy} = FerricStore.flow_policy_get(type)
      assert policy.retry.max_retries == 6
      assert policy.retry.backoff.kind == :fixed
      assert policy.retry.exhausted_to == "dead"
      assert policy.retention.ttl_ms == 60_000
      refute Map.has_key?(policy.retention, :history_hot_max_events)
      assert policy.retention.history_max_events == 25

      get_response = http_get(port, "/dashboard/flow/policies?status=ok&type=#{type}")
      assert get_response =~ "HTTP/1.1 200 OK"
      assert get_response |> extract_body() |> String.contains?("Policy saved")
    end

    test "POST /dashboard/flow/retention runs cleanup and redirects with counts" do
      port = HealthEndpoint.port()
      test_pid = self()

      Application.put_env(:ferricstore, :flow_dashboard_retention_cleanup_fun, fn opts ->
        send(test_pid, {:retention_cleanup, opts})
        {:ok, %{flows: 1, history: 2, values: 3}}
      end)

      on_exit(fn ->
        Application.delete_env(:ferricstore, :flow_dashboard_retention_cleanup_fun)
      end)

      response =
        http_post_form(port, "/dashboard/flow/retention", %{
          "action" => "cleanup",
          "limit" => "3",
          "confirm_cleanup" => "true"
        })

      assert extract_status_code(response) == 302
      location = extract_header(response, "location")
      assert location =~ "/dashboard/flow/retention?"
      assert location =~ "status=ok"
      assert location =~ "flows=1"
      assert location =~ "history=2"
      assert location =~ "values=3"
      assert_received {:retention_cleanup, [limit: 3]}

      get_response = http_get(port, location)
      assert get_response =~ "HTTP/1.1 200 OK"
      assert get_response |> extract_body() |> String.contains?("Cleanup completed")
    end

    test "POST /dashboard/flow/policies redirects invalid forms to a visible error" do
      port = HealthEndpoint.port()

      response =
        http_post_form(port, "/dashboard/flow/policies", %{
          "type" => "",
          "max_retries" => "1",
          "backoff_kind" => "fixed",
          "base_ms" => "50",
          "max_ms" => "500",
          "jitter_pct" => "0",
          "exhausted_to" => "failed",
          "retention_ttl_ms" => "60000",
          "history_max_events" => "25"
        })

      assert extract_status_code(response) == 302
      location = extract_header(response, "location")
      assert location =~ "/dashboard/flow/policies?"
      assert location =~ "status=error"

      get_response = http_get(port, location)
      assert get_response =~ "HTTP/1.1 200 OK"
      assert get_response |> extract_body() |> String.contains?("ERR flow type is required")
      assert get_response |> extract_body() |> String.contains?("flow-alert-error")
    end

    test "Flow detail page renders rewind success and error flash messages" do
      port = HealthEndpoint.port()
      id = "dashboard-rewind-flash-#{System.unique_integer([:positive])}"

      assert :ok =
               FerricStore.flow_create(id,
                 type: "dashboard-rewind-flash",
                 state: "queued",
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )

      encoded_id = URI.encode(id, &URI.char_unreserved?/1)

      success = http_get(port, "/dashboard/flow/#{encoded_id}?status=rewound")
      success_body = extract_body(success)

      assert success =~ "HTTP/1.1 200 OK"
      assert success_body =~ "flow-alert-ok"
      assert success_body =~ "Flow rewound"

      error =
        http_get(
          port,
          "/dashboard/flow/#{encoded_id}?status=error&message=ERR+rewind+target+event+is+required"
        )

      error_body = extract_body(error)

      assert error =~ "HTTP/1.1 200 OK"
      assert error_body =~ "flow-alert-error"
      assert error_body =~ "ERR rewind target event is required"
    end

    test "POST /dashboard/flow/:id/rewind restores selected history event" do
      port = HealthEndpoint.port()
      flow_type = "dashboard-rewind-http-#{System.unique_integer([:positive])}"
      partition_key = "tenant-rewind-http-#{System.unique_integer([:positive])}"
      id = "dashboard-rewind-http-flow-#{System.unique_integer([:positive])}"

      assert :ok =
               FerricStore.flow_create(id,
                 type: flow_type,
                 partition_key: partition_key,
                 state: "queued",
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )

      assert {:ok, [{created_event_id, _fields} | _]} =
               FerricStore.flow_history(id, partition_key: partition_key, count: 10)

      assert :ok =
               FerricStore.flow_transition(id, "queued", "ready",
                 partition_key: partition_key,
                 fencing_token: 0,
                 run_at_ms: 2_000,
                 now_ms: 2_000
               )

      encoded_id = URI.encode(id, &URI.char_unreserved?/1)

      response =
        http_post_form(port, "/dashboard/flow/#{encoded_id}/rewind", %{
          "partition_key" => partition_key,
          "to_event" => created_event_id,
          "confirm_rewind" => "true"
        })

      assert response =~ "HTTP/1.1 302 Found"
      assert response =~ "Location: /dashboard/flow/#{encoded_id}?"
      assert response =~ "status=rewound"

      assert {:ok, rewound} = FerricStore.flow_get(id, partition_key: partition_key)
      assert rewound.state == "queued"
      assert rewound.rewound_to_event_id == created_event_id
    end

    test "Flow states HTTP and live API preserve type filter" do
      flow_type = "dashboard-http-filter-#{System.unique_integer([:positive])}"
      other_type = "dashboard-http-other-#{System.unique_integer([:positive])}"

      assert :ok =
               FerricStore.flow_create(
                 "dashboard-http-filter-id-#{System.unique_integer([:positive])}",
                 type: flow_type,
                 state: "queued",
                 run_at_ms: 1_000
               )

      assert :ok =
               FerricStore.flow_create(
                 "dashboard-http-other-id-#{System.unique_integer([:positive])}",
                 type: other_type,
                 state: "queued",
                 run_at_ms: 1_000
               )

      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard/flow/states?type=#{URI.encode_www_form(flow_type)}")
      body = extract_body(response)

      assert response =~ "HTTP/1.1 200 OK"
      assert body =~ flow_type

      api_response =
        http_get(port, "/dashboard/api/flow/states?type=#{URI.encode_www_form(flow_type)}")

      assert api_response =~ "HTTP/1.1 200 OK"
      {:ok, decoded} = api_response |> extract_body() |> Jason.decode()
      table = decoded["components"]["flow_states_table"]
      assert table =~ flow_type
      refute table =~ other_type
    end
  end
end
