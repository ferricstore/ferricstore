defmodule FerricstoreServer.Health.DashboardTest.Sections.CollectionAndOverview do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Health.Dashboard
      alias FerricstoreServer.Health.Endpoint, as: HealthEndpoint
      alias Ferricstore.NamespaceConfig
      alias Ferricstore.Test.ShardHelpers

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

      describe "Doctor dashboard page" do
        test "renders doctor checks and admin command hints" do
          html =
            Dashboard.collect_doctor_page()
            |> Dashboard.render_doctor_page()

          assert html =~ "Doctor"
          assert html =~ "FERRICSTORE.DOCTOR CHECK"
          assert html =~ "flow_lmdb"
          assert html =~ "Start background check"
          assert html =~ "Repair Flow projection"
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
    end
  end
end
