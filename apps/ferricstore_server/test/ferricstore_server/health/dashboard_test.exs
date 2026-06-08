Code.require_file("dashboard_test/sections/collection_and_overview.exs", __DIR__)
Code.require_file("dashboard_test/sections/acl_flow_actions.exs", __DIR__)
Code.require_file("dashboard_test/sections/acl_filtering_and_config.exs", __DIR__)
Code.require_file("dashboard_test/sections/operational_pages.exs", __DIR__)
Code.require_file("dashboard_test/sections/flow_browse_and_queries.exs", __DIR__)
Code.require_file("dashboard_test/sections/flow_detail_actions.exs", __DIR__)
Code.require_file("dashboard_test/sections/flow_detail_policies_retention.exs", __DIR__)
Code.require_file("dashboard_test/sections/http_flow_routes.exs", __DIR__)

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
  @moduletag :dashboard
  @moduletag :global_state
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

  use FerricstoreServer.Health.DashboardTest.Sections.CollectionAndOverview
  use FerricstoreServer.Health.DashboardTest.Sections.AclFlowActions
  use FerricstoreServer.Health.DashboardTest.Sections.AclFilteringAndConfig
  use FerricstoreServer.Health.DashboardTest.Sections.OperationalPages
  use FerricstoreServer.Health.DashboardTest.Sections.FlowBrowseAndQueries
  use FerricstoreServer.Health.DashboardTest.Sections.FlowDetailActions
  use FerricstoreServer.Health.DashboardTest.Sections.FlowDetailPoliciesRetention
  use FerricstoreServer.Health.DashboardTest.Sections.HttpFlowRoutes
end
