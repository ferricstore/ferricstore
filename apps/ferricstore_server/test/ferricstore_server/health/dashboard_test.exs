Code.require_file("dashboard_test/sections/collection_and_overview.exs", __DIR__)
Code.require_file("dashboard_test/sections/acl_flow_actions.exs", __DIR__)
Code.require_file("dashboard_test/sections/acl_filtering_and_config.exs", __DIR__)
Code.require_file("dashboard_test/sections/operational_pages.exs", __DIR__)
Code.require_file("dashboard_test/sections/flow_browse_and_queries.exs", __DIR__)
Code.require_file("dashboard_test/sections/flow_detail_actions.exs", __DIR__)
Code.require_file("dashboard_test/sections/flow_detail_policies_retention.exs", __DIR__)
Code.require_file("dashboard_test/sections/flow_schedules.exs", __DIR__)
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
  alias FerricstoreServer.AuthRateLimiter
  alias FerricstoreServer.Health.Endpoint.Session, as: DashboardSession
  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Test.ShardHelpers

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)

    protected_mode = Application.get_env(:ferricstore, :protected_mode)

    Application.put_env(:ferricstore, :protected_mode, false)
    AuthRateLimiter.reset()
    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      restore_env(:protected_mode, protected_mode)
      Ferricstore.NamespaceConfig.reset_all()
      FerricstoreServer.Acl.reset!()
      AuthRateLimiter.reset()
    end)

    :ok
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  test "Flow governance budget table renders remaining overage and reservation columns" do
    html =
      FerricstoreServer.Health.Dashboard.Render.FlowGovernance.render_flow_governance_budgets([
        %{
          scope: "tenant-a",
          used: 120,
          remaining: 0,
          limit: 100,
          over_budget: true,
          reservations_count: 2,
          window_ms: 60_000,
          window_start_ms: 1_000
        }
      ])

    assert String.contains?(html, "Remaining")
    assert String.contains?(html, "Reservations")
    assert String.contains?(html, "tenant-a")
    assert String.contains?(html, "status-bad")
  end

  test "Flow governance circuit table renders status metrics and actions" do
    circuits = [
      %{
        scope: "effect:payment.charge",
        status: :open,
        failure_count: 5,
        failure_threshold: 3,
        retry_after_ms: 20_000,
        last_failure_ms: 1_000,
        updated_at_ms: 2_000,
        events: [
          %{at_ms: 2_000, kind: :opened, status: :open, failures: 5},
          %{
            at_ms: 1_900,
            kind: :slow_call,
            status: :closed,
            failures: 4,
            latency_ms: 2_500,
            error_class: "TimeoutError"
          }
        ]
      }
    ]

    html =
      FerricstoreServer.Health.Dashboard.Render.FlowGovernance.render_flow_governance_circuits(
        circuits
      )

    assert String.contains?(html, "effect:payment.charge")
    assert String.contains?(html, "status-bad")
    assert String.contains?(html, "20.0K ms")
    assert String.contains?(html, "close_circuit")

    timeline_html =
      FerricstoreServer.Health.Dashboard.Render.FlowGovernance.render_flow_governance_circuit_timeline(
        circuits
      )

    assert String.contains?(timeline_html, "Circuit Timeline")
    assert String.contains?(timeline_html, "slow_call")
    assert String.contains?(timeline_html, "TimeoutError")
    assert String.contains?(timeline_html, "2.5K ms")
  end

  test "Flow governance query parser supports circuit status filters and flash" do
    opts =
      Dashboard.flow_governance_opts_from_query(
        "scope=tenant-a&circuit_status=open&status=ok&message=closed"
      )

    assert Keyword.fetch!(opts, :scope) == "tenant-a"
    assert Keyword.fetch!(opts, :circuit_status) == "open"
    assert Keyword.fetch!(opts, :flash) == %{kind: :ok, message: "closed"}

    assert Dashboard.flow_governance_form_command(%{"action" => "open_circuit"}) ==
             "FLOW.CIRCUIT.OPEN"
  end

  test "Flow governance query parser supports indexed state metadata filters" do
    opts =
      Dashboard.flow_governance_opts_from_query(
        "meta_type=ai-review&meta_state=review&meta_key=ai.model&meta_value=gpt-5&meta_value_type=string&meta_partition_key=tenant-a&limit=25"
      )

    assert Keyword.fetch!(opts, :meta_type) == "ai-review"
    assert Keyword.fetch!(opts, :meta_state) == "review"
    assert Keyword.fetch!(opts, :meta_key) == "ai.model"
    assert Keyword.fetch!(opts, :meta_value) == "gpt-5"
    assert Keyword.fetch!(opts, :meta_value_type) == "string"
    assert Keyword.fetch!(opts, :meta_partition_key) == "tenant-a"
    assert Keyword.fetch!(opts, :limit) == 25
  end

  test "Flow governance metadata panel stays idle until query is complete" do
    parent = self()
    previous_query = Application.get_env(:ferricstore, :flow_dashboard_flow_query_fun)

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn query, params ->
      send(parent, {:flow_query_called, query, params})
      {:ok, []}
    end)

    on_exit(fn -> restore_env(:flow_dashboard_flow_query_fun, previous_query) end)

    data = Dashboard.collect_flow_governance_page(meta_type: "ai-review")
    html = Dashboard.render_flow_governance_page(data)

    assert data.state_meta_result.status == :idle
    assert String.contains?(html, "State Metadata")

    assert String.contains?(
             html,
             "Enter partition, workflow type, metadata state, key, and value"
           )

    refute_received {:flow_query_called, _query, _params}
  end

  test "Flow governance metadata panel uses a bounded parameterized query" do
    parent = self()
    previous_query = Application.get_env(:ferricstore, :flow_dashboard_flow_query_fun)

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn query, params ->
      send(parent, {:flow_query_called, query, params})

      {:ok,
       %{
         records: [
           %{
             id: "flow-1",
             type: "ai-review",
             state: "approved",
             partition_key: "tenant-a",
             updated_at_ms: 1_000,
             indexed_state_meta: "ai.model",
             state_meta: %{
               "review" => %{
                 "ai.model" => "gpt-5",
                 "risk_tier" => "high"
               }
             }
           }
         ]
       }}
    end)

    on_exit(fn -> restore_env(:flow_dashboard_flow_query_fun, previous_query) end)

    data =
      Dashboard.collect_flow_governance_page(
        meta_type: "ai-review",
        meta_state: "review",
        meta_key: "ai.model",
        meta_value: "gpt-5",
        meta_value_type: "string",
        meta_partition_key: "tenant-a",
        limit: 5
      )

    assert_receive {:flow_query_called, query, params}
    assert query =~ "partition_key = @partition_key"
    assert query =~ "state_meta['review']['ai.model']"
    assert query =~ "LIMIT 5 RETURN RECORDS"
    refute query =~ "tenant-a"
    refute query =~ "gpt-5"
    assert params["partition_key"] == "tenant-a"
    assert params["type"] == "ai-review"
    assert params["state_meta_value"] == "gpt-5"

    html = Dashboard.render_flow_governance_page(data)

    assert data.state_meta_result.status == :ok
    assert String.contains?(html, "FLOW.QUERY")
    assert String.contains?(html, "flow-1")
    assert String.contains?(html, "ai.model=gpt-5")
    assert String.contains?(html, "risk_tier=high")
  end

  test "Flow governance metadata panel finds real indexed state_meta records" do
    type = "dashboard-ai-governance-#{System.unique_integer([:positive])}"
    id = "dashboard-ai-flow-#{System.unique_integer([:positive])}"

    assert {:ok, %{indexed_state_meta: "ai.model"}} =
             FerricStore.flow_policy_set(type, indexed_state_meta: "ai.model")

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "review",
               partition_key: "tenant-a",
               state_meta: %{
                 "ai.model" => "gpt-5",
                 "risk_tier" => "high",
                 "prompt_version" => "refund-v3"
               },
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    ShardHelpers.eventually(
      fn ->
        data =
          Dashboard.collect_flow_governance_page(
            meta_type: type,
            meta_state: "review",
            meta_key: "ai.model",
            meta_value: "gpt-5",
            meta_value_type: "string",
            meta_partition_key: "tenant-a",
            limit: 10
          )

        data.state_meta_result.status == :ok and
          Enum.any?(data.state_meta_result.rows, &(Map.get(&1, :id) == id))
      end,
      "dashboard governance state_meta search should find the indexed flow",
      50,
      100
    )

    data =
      Dashboard.collect_flow_governance_page(
        meta_type: type,
        meta_state: "review",
        meta_key: "ai.model",
        meta_value: "gpt-5",
        meta_value_type: "string",
        meta_partition_key: "tenant-a",
        limit: 10
      )

    html = Dashboard.render_flow_governance_page(data)

    assert String.contains?(html, id)
    assert String.contains?(html, "ai.model=gpt-5")
    assert String.contains?(html, "prompt_version=refund-v3")
  end

  test "Flow governance metadata panel reports timeout instead of blocking dashboard" do
    previous_query = Application.get_env(:ferricstore, :flow_dashboard_flow_query_fun)
    previous_timeout = Application.get_env(:ferricstore, :flow_dashboard_list_fetch_timeout_ms)

    Application.put_env(:ferricstore, :flow_dashboard_list_fetch_timeout_ms, 10)

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn _query, _params ->
      Process.sleep(1_000)
      {:ok, []}
    end)

    on_exit(fn ->
      restore_env(:flow_dashboard_flow_query_fun, previous_query)
      restore_env(:flow_dashboard_list_fetch_timeout_ms, previous_timeout)
    end)

    data =
      Dashboard.collect_flow_governance_page(
        meta_type: "ai-review",
        meta_state: "review",
        meta_key: "risk_tier",
        meta_value: "high",
        meta_value_type: "string",
        meta_partition_key: "tenant-a",
        limit: 5
      )

    assert data.state_meta_result.status == :timeout

    html = Dashboard.render_flow_governance_page(data)
    assert String.contains?(html, "query timed out")
  end

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
    {csrf_token, csrf_set_cookie} = DashboardSession.csrf_pair()
    csrf_cookie = csrf_set_cookie |> String.split(";", parts: 2) |> hd()
    params = Map.put_new(params, "_csrf_token", csrf_token)
    headers = put_request_cookie(headers, csrf_cookie)
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

  defp put_request_cookie(headers, cookie) do
    {cookies, other_headers} =
      Enum.split_with(headers, fn {name, _value} -> String.downcase(name) == "cookie" end)

    combined =
      case cookies do
        [] -> cookie
        [{_name, value} | _rest] -> value <> "; " <> cookie
      end

    [{"Cookie", combined} | other_headers]
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
  use FerricstoreServer.Health.DashboardTest.Sections.FlowSchedules
  use FerricstoreServer.Health.DashboardTest.Sections.HttpFlowRoutes
end
