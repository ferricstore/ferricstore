defmodule FerricstoreServer.Health.Endpoint.DashboardHandlers do
  @moduledoc false

  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Endpoint.Auth
  alias FerricstoreServer.Health.Endpoint.FlowPaths
  alias FerricstoreServer.Health.Endpoint.Response

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
      Response.send_response(socket, transport, 200, "OK", "application/json; charset=utf-8", body)
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
end
