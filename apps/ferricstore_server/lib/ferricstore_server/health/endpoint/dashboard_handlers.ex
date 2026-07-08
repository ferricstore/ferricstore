defmodule FerricstoreServer.Health.Endpoint.DashboardHandlers do
  @moduledoc false

  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Endpoint.Auth
  alias FerricstoreServer.Health.Endpoint.FlowPaths
  alias FerricstoreServer.Health.Endpoint.Response

  def handle_slowlog_page(socket, transport, peer, headers) do
    render_static_page(
      socket,
      transport,
      peer,
      headers,
      &Dashboard.collect_slowlog_page/0,
      &Dashboard.render_slowlog_page/1
    )
  end

  def handle_merge_page(socket, transport, peer, headers) do
    render_static_page(
      socket,
      transport,
      peer,
      headers,
      &Dashboard.collect_merge_page/0,
      &Dashboard.render_merge_page/1
    )
  end

  def handle_config_page(socket, transport, peer, headers) do
    render_static_page(
      socket,
      transport,
      peer,
      headers,
      &Dashboard.collect_config_page/0,
      &Dashboard.render_config_page/1
    )
  end

  def handle_capabilities_page(socket, transport, peer, headers) do
    render_static_page(
      socket,
      transport,
      peer,
      headers,
      &Dashboard.collect_capabilities_page/0,
      &Dashboard.render_capabilities_page/1
    )
  end

  def handle_security_page(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      Response.send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        query
        |> URI.decode_query()
        |> Map.merge(Auth.dashboard_collect_opts(peer, headers))
        |> Dashboard.collect_security_page()

      body = Dashboard.render_security_page(data)
      Response.send_html_response(socket, transport, 200, "OK", body)
    end
  end

  def handle_raft_page(socket, transport, peer, headers) do
    unless Auth.observability_authorized?(peer, headers) do
      Response.send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      try do
        data = Dashboard.collect_raft_page()
        body = Dashboard.render_raft_page(data)
        Response.send_html_response(socket, transport, 200, "OK", body)
      catch
        kind, reason ->
          log_dashboard_page_error("/dashboard/raft", kind, reason)
          body = dashboard_internal_error_body("Consensus")
          Response.send_html_response(socket, transport, 200, "OK", body)
      end
    end
  end

  def handle_consensus_redirect(socket, transport, peer, headers) do
    unless Auth.observability_authorized?(peer, headers) do
      Response.send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      Response.send_redirect_response(socket, transport, "/dashboard/raft")
    end
  end

  def handle_clients_page(socket, transport, peer, headers) do
    render_static_page(
      socket,
      transport,
      peer,
      headers,
      &Dashboard.collect_clients_page/0,
      &Dashboard.render_clients_page/1
    )
  end

  def handle_storage_page(socket, transport, peer, headers) do
    render_static_page(
      socket,
      transport,
      peer,
      headers,
      &Dashboard.collect_storage_page/0,
      &Dashboard.render_storage_page/1
    )
  end

  def handle_commands_page(socket, transport, peer, headers) do
    render_static_page(
      socket,
      transport,
      peer,
      headers,
      &Dashboard.collect_commands_page/0,
      &Dashboard.render_commands_page/1
    )
  end

  def handle_reads_page(socket, transport, peer, headers) do
    render_static_page(
      socket,
      transport,
      peer,
      headers,
      &Dashboard.collect_reads_page/0,
      &Dashboard.render_reads_page/1
    )
  end

  def handle_streams_page(socket, transport, peer, headers) do
    unless Auth.observability_authorized?(peer, headers) do
      Response.send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = Dashboard.collect_streams_page(Auth.dashboard_collect_opts(peer, headers))
      body = Dashboard.render_streams_page(data)
      Response.send_html_response(socket, transport, 200, "OK", body)
    end
  end

  def handle_pubsub_page(socket, transport, peer, headers) do
    unless Auth.observability_authorized?(peer, headers) do
      Response.send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = Dashboard.collect_pubsub_page(Auth.dashboard_collect_opts(peer, headers))
      body = Dashboard.render_pubsub_page(data)
      Response.send_html_response(socket, transport, 200, "OK", body)
    end
  end

  def handle_prefixes_page(socket, transport, peer, headers) do
    render_static_page(
      socket,
      transport,
      peer,
      headers,
      &Dashboard.collect_prefixes_page/0,
      &Dashboard.render_prefixes_page/1
    )
  end

  def handle_flow_workers_page(socket, transport, peer, headers) do
    render_flow_static_page(
      socket,
      transport,
      peer,
      headers,
      &Dashboard.collect_flow_workers_page/1,
      &Dashboard.render_flow_workers_page/1
    )
  end

  def handle_flow_due_page(socket, transport, peer, headers) do
    render_flow_static_page(
      socket,
      transport,
      peer,
      headers,
      &Dashboard.collect_flow_due_page/1,
      &Dashboard.render_flow_due_page/1
    )
  end

  def handle_flow_schedules(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      Response.send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        query
        |> Dashboard.flow_schedules_opts_from_query()
        |> Auth.dashboard_flow_collect_opts(peer, headers)
        |> Dashboard.collect_flow_schedules_page()

      body = Dashboard.render_flow_schedules_page(data)
      Response.send_html_response(socket, transport, 200, "OK", body)
    end
  end

  def handle_keyspace_page(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      Response.send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        query
        |> URI.decode_query()
        |> Map.merge(Auth.dashboard_collect_opts(peer, headers))
        |> Dashboard.collect_keyspace_page()

      body = Dashboard.render_keyspace_page(data)
      Response.send_html_response(socket, transport, 200, "OK", body)
    end
  end

  def handle_doctor_page(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      Response.send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        query
        |> URI.decode_query()
        |> Dashboard.collect_doctor_page()

      body = Dashboard.render_doctor_page(data)
      Response.send_html_response(socket, transport, 200, "OK", body)
    end
  end

  def handle_flow_api(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      Response.send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        query
        |> Dashboard.flow_opts_from_query()
        |> Auth.dashboard_flow_collect_opts(peer, headers)
        |> Dashboard.collect_flow_page()

      body = data |> Dashboard.live_flow_payload() |> Jason.encode!()

      Response.send_response(
        socket,
        transport,
        200,
        "OK",
        "application/json; charset=utf-8",
        body
      )
    end
  end

  def handle_flow_overview(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      Response.send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        query
        |> Dashboard.flow_opts_from_query()
        |> Auth.dashboard_flow_collect_opts(peer, headers)
        |> Dashboard.collect_flow_page()

      body = Dashboard.render_flow_page(data)
      Response.send_html_response(socket, transport, 200, "OK", body)
    end
  end

  def handle_flow_states(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      Response.send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      opts =
        query
        |> Dashboard.flow_states_opts_from_query()
        |> Auth.dashboard_flow_collect_opts(peer, headers)

      data = Dashboard.collect_flow_states_page(opts)
      body = Dashboard.render_flow_states_page(data)
      Response.send_html_response(socket, transport, 200, "OK", body)
    end
  end

  def handle_flow_signals(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      Response.send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      opts =
        query
        |> Dashboard.flow_signals_opts_from_query()
        |> Auth.dashboard_flow_collect_opts(peer, headers)

      data = Dashboard.collect_flow_signals_page(opts)
      body = Dashboard.render_flow_signals_page(data)
      Response.send_html_response(socket, transport, 200, "OK", body)
    end
  end

  def handle_flow_policies(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      Response.send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      params = FlowPaths.decode_form_body(query)

      data =
        Dashboard.collect_flow_policies_page(
          flash: Dashboard.flow_policy_flash_from_query(query),
          edit_type: Map.get(params, "edit", "")
        )

      body = Dashboard.render_flow_policies_page(data)
      Response.send_html_response(socket, transport, 200, "OK", body)
    end
  end

  def handle_flow_governance(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      Response.send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        query
        |> Dashboard.flow_governance_opts_from_query()
        |> Auth.dashboard_flow_collect_opts(peer, headers)
        |> Dashboard.collect_flow_governance_page()

      body = Dashboard.render_flow_governance_page(data)
      Response.send_html_response(socket, transport, 200, "OK", body)
    end
  end

  def handle_flow_retention(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      Response.send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      params = FlowPaths.decode_form_body(query)

      data =
        [
          flash: Dashboard.flow_retention_flash_from_query(query),
          limit: Map.get(params, "limit", "")
        ]
        |> Auth.dashboard_flow_collect_opts(peer, headers)
        |> Dashboard.collect_flow_retention_page()

      body = Dashboard.render_flow_retention_page(data)
      Response.send_html_response(socket, transport, 200, "OK", body)
    end
  end

  def handle_flow_failures(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      Response.send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        query
        |> Dashboard.flow_failures_opts_from_query()
        |> Auth.dashboard_flow_collect_opts(peer, headers)
        |> Dashboard.collect_flow_failures_page()

      body = Dashboard.render_flow_failures_page(data)
      Response.send_html_response(socket, transport, 200, "OK", body)
    end
  end

  def handle_flow_lineage(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      Response.send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        query
        |> Dashboard.flow_lineage_opts_from_query()
        |> Auth.dashboard_flow_collect_opts(peer, headers)
        |> Dashboard.collect_flow_lineage_page()

      body = Dashboard.render_flow_lineage_page(data)
      Response.send_html_response(socket, transport, 200, "OK", body)
    end
  end

  def handle_flow_query(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      Response.send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        query
        |> Dashboard.flow_query_opts_from_query()
        |> Auth.dashboard_flow_collect_opts(peer, headers)
        |> Dashboard.collect_flow_query_page()

      body = Dashboard.render_flow_query_page(data)
      Response.send_html_response(socket, transport, 200, "OK", body)
    end
  end

  def handle_flow_lookup(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      Response.send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      decoded_query = URI.decode_query(query)

      id =
        decoded_query
        |> Map.get("id", "")
        |> String.trim()

      partition_key =
        decoded_query
        |> Map.get("partition_key", "")
        |> String.trim()

      location =
        case {id, partition_key} do
          {"", ""} ->
            "/dashboard/flow"

          {"", partition_key} ->
            "/dashboard/flow?" <> URI.encode_query(%{"partition_key" => partition_key})

          {id, partition_key} ->
            FlowPaths.flow_detail_location(id, partition_key)
        end

      Response.send_redirect_response(socket, transport, location)
    end
  end

  defp render_static_page(socket, transport, peer, headers, collect_fun, render_fun) do
    unless Auth.observability_authorized?(peer, headers) do
      Response.send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = collect_fun.()
      body = render_fun.(data)
      Response.send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp render_flow_static_page(socket, transport, peer, headers, collect_fun, render_fun) do
    unless Auth.observability_authorized?(peer, headers) do
      Response.send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        peer
        |> Auth.dashboard_flow_collect_opts(headers)
        |> collect_fun.()

      body = render_fun.(data)
      Response.send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp log_dashboard_page_error(path, kind, reason) do
    require Logger

    Logger.error(fn ->
      "FerricStore dashboard page error at #{path}: #{inspect({kind, reason}, limit: 20)}"
    end)
  end

  defp dashboard_internal_error_body(page_name) do
    """
    <html><body style="background:#0d1117;color:#f85149;padding:20px;font-family:monospace;">
    <h2>#{page_name} Page Error</h2>
    <pre>Internal dashboard error. See server logs.</pre>
    <a href="/dashboard" style="color:#58a6ff;">← Dashboard</a>
    </body></html>
    """
  end
end
