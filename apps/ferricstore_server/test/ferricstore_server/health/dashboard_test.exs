defmodule FerricstoreServer.Health.DashboardTest do
  @moduledoc """
  Tests for the built-in HTML dashboard (spec 7.3).

  Covers:
    - Dashboard data collection from all subsystems
    - HTML rendering with all expected sections
    - HTTP endpoint serving at /dashboard
    - Auto-refresh meta tag
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
    ShardHelpers.flush_all_keys()
    on_exit(fn -> Ferricstore.NamespaceConfig.reset_all() end)
    :ok
  end

  # ---------------------------------------------------------------------------
  # HTTP helpers
  # ---------------------------------------------------------------------------

  defp http_get(port, path) do
    {:ok, conn} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    :ok =
      :gen_tcp.send(conn, "GET #{path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")

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

    test "contains auto-refresh meta tag", %{html: html} do
      assert String.contains?(html, ~s(http-equiv="refresh"))
      assert String.contains?(html, ~s(content="2"))
    end

    test "contains page title", %{html: html} do
      assert String.contains?(html, "<title>FerricStore Dashboard</title>")
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

    test "response contains auto-refresh meta tag" do
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard")
      body = extract_body(response)

      assert String.contains?(body, ~s(http-equiv="refresh"))
      assert String.contains?(body, ~s(content="2"))
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

    test "does not render removed async durability state" do
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
    end

    test "shows built-in defaults when no overrides exist" do
      NamespaceConfig.reset_all()
      port = HealthEndpoint.port()
      response = http_get(port, "/dashboard/config")
      body = extract_body(response)

      assert String.contains?(body, "built-in default window")
    end
  end
end
