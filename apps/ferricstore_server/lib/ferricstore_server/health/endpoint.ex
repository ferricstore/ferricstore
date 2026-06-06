defmodule FerricstoreServer.Health.Endpoint do
  @moduledoc """
  Minimal HTTP/1.1 health endpoint for Kubernetes readiness and liveness
  probes and the built-in observability dashboard (spec 7.3).

  Runs a Ranch TCP listener on a configurable port (default: `4000`, or `0`
  for ephemeral in tests) that speaks just enough HTTP/1.1 to serve:

    * `GET /health/live`  -- 200 always (liveness probe: process is alive)
    * `GET /health/ready` -- 200 when ready, 503 during startup
    * `GET /dashboard`    -- HTML dashboard shell with live component updates
    * `GET /dashboard/api/*` -- JSON component payloads for live dashboards
    * All other paths      -- 404

  ## Architecture

  This intentionally avoids adding Cowboy or Plug as dependencies. The HTTP
  parsing is minimal: we read the first line to extract the method and path,
  then consume remaining headers until we see the blank line (`\\r\\n\\r\\n`).
  `GET` requests serve read-only pages and component APIs. `POST` is supported
  only for small dashboard operation forms.

  Each accepted connection is a short-lived Ranch protocol process that sends
  a single response and closes. There is no keep-alive or pipelining.

  ## Configuration

      config :ferricstore, :health_port, 4000

  Set to `0` in test to use an ephemeral port (see `port/0`).

  ## Kubernetes integration

      livenessProbe:
        httpGet:
          path: /health/live
          port: 4000
        initialDelaySeconds: 2
        periodSeconds: 10

      readinessProbe:
        httpGet:
          path: /health/ready
          port: 4000
        initialDelaySeconds: 2
        periodSeconds: 5
  """

  @behaviour :ranch_protocol

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Health.Endpoint.Auth
  alias FerricstoreServer.Health.Endpoint.Forbidden
  alias FerricstoreServer.Health.Endpoint.Login
  alias FerricstoreServer.Health.Endpoint.FlowPaths
  alias FerricstoreServer.Health.Endpoint.Request
  alias FerricstoreServer.Health.Endpoint.Response
  alias FerricstoreServer.Health.Endpoint.RouteRequirements
  alias FerricstoreServer.Health.Endpoint.Session

  require Logger

  @listener_ref :"#{__MODULE__}"
  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Returns the actual TCP port the health endpoint is bound to.

  Works for both fixed ports and ephemeral (port 0) bindings. Raises if the
  listener is not running.
  """
  @spec port() :: :inet.port_number()
  def port do
    :ranch.get_port(@listener_ref)
  end

  @doc """
  Returns the Ranch listener reference atom for this endpoint.
  """
  @spec ref() :: atom()
  def ref, do: @listener_ref

  @doc """
  Returns a Ranch child spec suitable for embedding in a supervisor.

  ## Parameters

    * `port` - TCP port to bind (0 for ephemeral)
  """
  @spec child_spec(port :: :inet.port_number()) :: Supervisor.child_spec()
  def child_spec(port) do
    transport_opts = %{
      socket_opts: [port: port],
      num_acceptors: 2,
      max_connections: 64
    }

    :ranch.child_spec(
      @listener_ref,
      :ranch_tcp,
      transport_opts,
      __MODULE__,
      %{}
    )
  end

  # ---------------------------------------------------------------------------
  # Ranch protocol callbacks
  # ---------------------------------------------------------------------------

  @doc false
  @spec start_link(ref :: atom(), transport :: module(), opts :: map()) :: {:ok, pid()}
  def start_link(ref, transport, opts) do
    pid = spawn_link(__MODULE__, :init, [ref, transport, opts])
    {:ok, pid}
  end

  @doc false
  @spec init(ref :: atom(), transport :: module(), opts :: map()) :: :ok
  def init(ref, transport, _opts) do
    {:ok, socket} = :ranch.handshake(ref)
    :ok = transport.setopts(socket, active: false)
    peer = Request.peer_addr(socket, transport)

    case Request.read_request(socket, transport) do
      {:ok, method, path, headers, body} ->
        handle_request(socket, transport, method, path, peer, headers, body)

      :error ->
        send_response(socket, transport, 400, "Bad Request", ~s({"error":"bad request"}))
    end

    transport.close(socket)
    :ok
  end

  # ---------------------------------------------------------------------------
  # HTTP request parsing (minimal)
  # ---------------------------------------------------------------------------

  # ---------------------------------------------------------------------------
  # Request routing
  # ---------------------------------------------------------------------------

  @spec handle_request(:inet.socket(), module(), String.t(), String.t(), term(), map(), binary()) ::
          :ok
  defp handle_request(socket, transport, method, path, peer, headers, body) do
    case Auth.authorize_request(method, path, peer, headers) do
      :ok ->
        dispatch_request(socket, transport, method, path, peer, headers, body)

      {:redirect_login, location} ->
        send_redirect_response(socket, transport, location)

      {:unauthorized, reason} ->
        send_response(
          socket,
          transport,
          401,
          "Unauthorized",
          "application/json",
          Jason.encode!(%{error: reason})
        )

      {:forbidden, requirement, reason} ->
        send_forbidden_response(socket, transport, path, requirement, reason)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/login", _peer, _headers, _body) do
    send_html_response(socket, transport, 200, "OK", Login.render_page("", nil))
  end

  defp dispatch_request(
         socket,
         transport,
         "GET",
         "/dashboard/login?" <> query,
         _peer,
         _headers,
         _body
       ) do
    params = URI.decode_query(query)

    send_html_response(
      socket,
      transport,
      200,
      "OK",
      Login.render_page(Map.get(params, "next", ""), nil)
    )
  end

  defp dispatch_request(
         socket,
         transport,
         "POST",
         "/dashboard/login",
         _peer,
         _headers,
         body
       ) do
    params = FlowPaths.decode_form_body(body)
    username = params |> Map.get("username", "") |> String.trim()
    password = Map.get(params, "password", "")
    next = Login.sanitize_next(Map.get(params, "next", ""))

    case Acl.authenticate(username, password) do
      {:ok, user} ->
        send_redirect_response(socket, transport, next, [
          {"Set-Cookie", Session.session_cookie(user)}
        ])

      {:error, reason} ->
        send_html_response(
          socket,
          transport,
          401,
          "Unauthorized",
          Login.render_page(next, reason)
        )
    end
  end

  defp dispatch_request(
         socket,
         transport,
         "POST",
         "/dashboard/logout",
         _peer,
         _headers,
         _body
       ) do
    send_redirect_response(socket, transport, "/dashboard/login", [
      {"Set-Cookie", Session.clear_session_cookie()}
    ])
  end

  defp dispatch_request(
         socket,
         transport,
         "POST",
         "/dashboard/doctor",
         peer,
         headers,
         body
       ) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      params = FlowPaths.decode_form_body(body)

      case Auth.authorize_command_request(peer, headers, {"FERRICSTORE.DOCTOR", []}, :html) do
        :ok ->
          location =
            case FerricstoreServer.Health.Dashboard.apply_doctor_form(params) do
              {:ok, message} ->
                "/dashboard/doctor?" <>
                  URI.encode_query(%{"status" => "ok", "message" => message})

              {:error, reason} ->
                "/dashboard/doctor?" <>
                  URI.encode_query(%{"status" => "error", "message" => reason})
            end

          send_redirect_response(socket, transport, location)

        {:redirect_login, location} ->
          send_redirect_response(socket, transport, location)

        {:unauthorized, reason} ->
          send_response(
            socket,
            transport,
            401,
            "Unauthorized",
            "application/json",
            Jason.encode!(%{error: reason})
          )

        {:forbidden, requirement, reason} ->
          send_forbidden_response(socket, transport, "/dashboard/doctor", requirement, reason)
      end
    end
  end

  defp dispatch_request(
         socket,
         transport,
         "POST",
         "/dashboard/flow/policies",
         peer,
         headers,
         body
       ) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      params = FlowPaths.decode_form_body(body)

      case Auth.authorize_command_request(
             peer,
             headers,
             RouteRequirements.flow_policy_form_requirement(params),
             :html
           ) do
        :ok ->
          location =
            case FerricstoreServer.Health.Dashboard.apply_flow_policy_form(params) do
              {:ok, type} ->
                "/dashboard/flow/policies?" <>
                  URI.encode_query(%{"status" => "ok", "type" => type, "edit" => type})

              {:error, reason} ->
                type = Map.get(params, "type", "")

                "/dashboard/flow/policies?" <>
                  URI.encode_query(%{"status" => "error", "message" => reason, "edit" => type})
            end

          send_redirect_response(socket, transport, location)

        {:redirect_login, location} ->
          send_redirect_response(socket, transport, location)

        {:unauthorized, reason} ->
          send_response(
            socket,
            transport,
            401,
            "Unauthorized",
            "application/json",
            Jason.encode!(%{error: reason})
          )

        {:forbidden, requirement, reason} ->
          send_forbidden_response(
            socket,
            transport,
            "/dashboard/flow/policies",
            requirement,
            reason
          )
      end
    end
  end

  defp dispatch_request(
         socket,
         transport,
         "POST",
         "/dashboard/flow/retention",
         peer,
         headers,
         body
       ) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      params = FlowPaths.decode_form_body(body)

      case Auth.authorize_command_request(
             peer,
             headers,
             RouteRequirements.flow_retention_form_requirement(params),
             :html
           ) do
        :ok ->
          location =
            case FerricstoreServer.Health.Dashboard.apply_flow_retention_form(params) do
              {:ok, :dry_run, result} ->
                "/dashboard/flow/retention?" <>
                  URI.encode_query(%{
                    "status" => "dry_run",
                    "limit" => Map.get(result, :limit, 100)
                  })

              {:ok, :cleanup, result} ->
                "/dashboard/flow/retention?" <>
                  URI.encode_query(%{
                    "status" => "ok",
                    "limit" => Map.get(result, :limit, 100),
                    "flows" => Map.get(result, :flows, 0),
                    "history" => Map.get(result, :history, 0),
                    "values" => Map.get(result, :values, 0)
                  })

              {:error, reason} ->
                "/dashboard/flow/retention?" <>
                  URI.encode_query(%{"status" => "error", "message" => reason})
            end

          send_redirect_response(socket, transport, location)

        {:redirect_login, location} ->
          send_redirect_response(socket, transport, location)

        {:unauthorized, reason} ->
          send_response(
            socket,
            transport,
            401,
            "Unauthorized",
            "application/json",
            Jason.encode!(%{error: reason})
          )

        {:forbidden, requirement, reason} ->
          send_forbidden_response(
            socket,
            transport,
            "/dashboard/flow/retention",
            requirement,
            reason
          )
      end
    end
  end

  defp dispatch_request(
         socket,
         transport,
         "POST",
         "/dashboard/flow/failures",
         peer,
         headers,
         body
       ) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      params = FlowPaths.decode_form_body(body)

      case Auth.authorize_command_request(
             peer,
             headers,
             RouteRequirements.flow_reclaim_form_requirement(params),
             :html
           ) do
        :ok ->
          location =
            case FerricstoreServer.Health.Dashboard.apply_flow_failures_form(params) do
              {:ok, result} ->
                "/dashboard/flow/failures?" <>
                  URI.encode_query(%{
                    "status" => "reclaimed",
                    "type" => Map.get(result, :type, ""),
                    "count" => Map.get(result, :reclaimed, 0)
                  })

              {:error, reason} ->
                "/dashboard/flow/failures?" <>
                  URI.encode_query(%{"status" => "error", "message" => reason})
            end

          send_redirect_response(socket, transport, location)

        {:redirect_login, location} ->
          send_redirect_response(socket, transport, location)

        {:unauthorized, reason} ->
          send_response(
            socket,
            transport,
            401,
            "Unauthorized",
            "application/json",
            Jason.encode!(%{error: reason})
          )

        {:forbidden, requirement, reason} ->
          send_forbidden_response(
            socket,
            transport,
            "/dashboard/flow/failures",
            requirement,
            reason
          )
      end
    end
  end

  defp dispatch_request(
         socket,
         transport,
         "POST",
         "/dashboard/flow/" <> encoded_action,
         peer,
         headers,
         body
       ) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      case FlowPaths.decode_flow_rewind_action(encoded_action) do
        {:ok, id} ->
          params =
            body
            |> FlowPaths.decode_form_body()
            |> Map.put_new("id", id)

          case Auth.authorize_command_request(
                 peer,
                 headers,
                 RouteRequirements.flow_rewind_form_requirement(id, params),
                 :html
               ) do
            :ok ->
              location =
                case FerricstoreServer.Health.Dashboard.apply_flow_rewind_form(params) do
                  {:ok, id, partition_key} ->
                    FlowPaths.flow_detail_location(id, partition_key, %{"status" => "rewound"})

                  {:error, reason} ->
                    partition_key = Map.get(params, "partition_key", "")

                    FlowPaths.flow_detail_location(id, partition_key, %{
                      "status" => "error",
                      "message" => reason
                    })
                end

              send_redirect_response(socket, transport, location)

            {:redirect_login, location} ->
              send_redirect_response(socket, transport, location)

            {:unauthorized, reason} ->
              send_response(
                socket,
                transport,
                401,
                "Unauthorized",
                "application/json",
                Jason.encode!(%{error: reason})
              )

            {:forbidden, requirement, reason} ->
              send_forbidden_response(
                socket,
                transport,
                "/dashboard/flow/" <> encoded_action,
                requirement,
                reason
              )
          end

        :not_found ->
          send_response(socket, transport, 404, "Not Found", ~s({"error":"not found"}))
      end
    end
  end

  defp dispatch_request(socket, transport, method, path, peer, headers, _body) do
    dispatch_request(socket, transport, method, path, peer, headers)
  end

  @spec dispatch_request(:inet.socket(), module(), String.t(), String.t(), term(), map()) :: :ok
  defp dispatch_request(socket, transport, "GET", "/health/live", _peer, _headers) do
    send_response(socket, transport, 200, "OK", ~s({"status":"alive"}))
  end

  defp dispatch_request(socket, transport, "GET", "/health/ready", _peer, _headers) do
    health = Ferricstore.Health.check()

    body =
      Jason.encode!(%{
        status: Atom.to_string(health.status),
        shard_count: health.shard_count,
        shards:
          Enum.map(health.shards, fn shard ->
            %{index: shard.index, status: shard.status, keys: shard.keys}
          end),
        uptime_seconds: health.uptime_seconds
      })

    case health.status do
      :ok ->
        send_response(socket, transport, 200, "OK", body)

      :starting ->
        send_response(socket, transport, 503, "Service Unavailable", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/favicon.ico", _peer, _headers) do
    send_response(socket, transport, 204, "No Content", "image/x-icon", "")
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard", peer, headers) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect()
      body = FerricstoreServer.Health.Dashboard.render(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/api/overview", peer, headers) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect()
      body = data |> FerricstoreServer.Health.Dashboard.live_overview_payload() |> Jason.encode!()
      send_response(socket, transport, 200, "OK", "application/json; charset=utf-8", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/api/flow", peer, headers) do
    handle_flow_api(socket, transport, peer, headers, "")
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/api/flow?" <> query, peer, headers) do
    handle_flow_api(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/api/" <> api_path, peer, headers) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      case FerricstoreServer.Health.Dashboard.live_payload(
             api_path,
             Auth.dashboard_collect_opts(peer, headers)
           ) do
        {:ok, payload} ->
          send_response(
            socket,
            transport,
            200,
            "OK",
            "application/json; charset=utf-8",
            Jason.encode!(payload)
          )

        :not_found ->
          send_response(socket, transport, 404, "Not Found", ~s({"error":"not found"}))
      end
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/slowlog", peer, headers) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect_slowlog_page()
      body = FerricstoreServer.Health.Dashboard.render_slowlog_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/merge", peer, headers) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect_merge_page()
      body = FerricstoreServer.Health.Dashboard.render_merge_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/config", peer, headers) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect_config_page()
      body = FerricstoreServer.Health.Dashboard.render_config_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/raft", peer, headers) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      try do
        data = FerricstoreServer.Health.Dashboard.collect_raft_page()
        body = FerricstoreServer.Health.Dashboard.render_raft_page(data)
        send_html_response(socket, transport, 200, "OK", body)
      catch
        kind, reason ->
          log_dashboard_page_error("/dashboard/raft", kind, reason)

          send_html_response(
            socket,
            transport,
            200,
            "OK",
            dashboard_internal_error_body("Consensus")
          )
      end
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/consensus", peer, headers) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      send_redirect_response(socket, transport, "/dashboard/raft")
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/clients", peer, headers) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect_clients_page()
      body = FerricstoreServer.Health.Dashboard.render_clients_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/storage", peer, headers) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect_storage_page()
      body = FerricstoreServer.Health.Dashboard.render_storage_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/doctor", peer, headers) do
    handle_doctor_page(socket, transport, peer, headers, "")
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/doctor?" <> query, peer, headers) do
    handle_doctor_page(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/keyspace", peer, headers) do
    handle_keyspace_page(socket, transport, peer, headers, "")
  end

  defp dispatch_request(
         socket,
         transport,
         "GET",
         "/dashboard/keyspace?" <> query,
         peer,
         headers
       ) do
    handle_keyspace_page(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/commands", peer, headers) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect_commands_page()
      body = FerricstoreServer.Health.Dashboard.render_commands_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/reads", peer, headers) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect_reads_page()
      body = FerricstoreServer.Health.Dashboard.render_reads_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/prefixes", peer, headers) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect_prefixes_page()
      body = FerricstoreServer.Health.Dashboard.render_prefixes_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow", peer, headers) do
    handle_flow_overview(socket, transport, peer, headers, "")
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow?" <> query, peer, headers) do
    handle_flow_overview(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/lookup", peer, headers) do
    handle_flow_lookup(socket, transport, peer, headers, "")
  end

  defp dispatch_request(
         socket,
         transport,
         "GET",
         "/dashboard/flow/lookup?" <> query,
         peer,
         headers
       ) do
    handle_flow_lookup(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/states", peer, headers) do
    handle_flow_states(socket, transport, peer, headers, "")
  end

  defp dispatch_request(
         socket,
         transport,
         "GET",
         "/dashboard/flow/states?" <> query,
         peer,
         headers
       ) do
    handle_flow_states(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/workers", peer, headers) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        peer
        |> Auth.dashboard_flow_collect_opts(headers)
        |> FerricstoreServer.Health.Dashboard.collect_flow_workers_page()

      body = FerricstoreServer.Health.Dashboard.render_flow_workers_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/due", peer, headers) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        peer
        |> Auth.dashboard_flow_collect_opts(headers)
        |> FerricstoreServer.Health.Dashboard.collect_flow_due_page()

      body = FerricstoreServer.Health.Dashboard.render_flow_due_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/failures", peer, headers) do
    handle_flow_failures(socket, transport, peer, headers, "")
  end

  defp dispatch_request(
         socket,
         transport,
         "GET",
         "/dashboard/flow/failures?" <> query,
         peer,
         headers
       ) do
    handle_flow_failures(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/lineage", peer, headers) do
    handle_flow_lineage(socket, transport, peer, headers, "")
  end

  defp dispatch_request(
         socket,
         transport,
         "GET",
         "/dashboard/flow/lineage?" <> query,
         peer,
         headers
       ) do
    handle_flow_lineage(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/query", peer, headers) do
    handle_flow_query(socket, transport, peer, headers, "")
  end

  defp dispatch_request(
         socket,
         transport,
         "GET",
         "/dashboard/flow/query?" <> query,
         peer,
         headers
       ) do
    handle_flow_query(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/signals", peer, headers) do
    handle_flow_signals(socket, transport, peer, headers, "")
  end

  defp dispatch_request(
         socket,
         transport,
         "GET",
         "/dashboard/flow/signals?" <> query,
         peer,
         headers
       ) do
    handle_flow_signals(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/policies", peer, headers) do
    handle_flow_policies(socket, transport, peer, headers, "")
  end

  defp dispatch_request(
         socket,
         transport,
         "GET",
         "/dashboard/flow/policies?" <> query,
         peer,
         headers
       ) do
    handle_flow_policies(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/retention", peer, headers) do
    handle_flow_retention(socket, transport, peer, headers, "")
  end

  defp dispatch_request(
         socket,
         transport,
         "GET",
         "/dashboard/flow/retention?" <> query,
         peer,
         headers
       ) do
    handle_flow_retention(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/config", peer, headers) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      send_redirect_response(socket, transport, "/dashboard/config")
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/projections", peer, headers) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      send_redirect_response(socket, transport, "/dashboard/flow")
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/" <> encoded_id, peer, headers) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      {id, opts} = FlowPaths.decode_flow_detail_request(encoded_id)
      data = FerricstoreServer.Health.Dashboard.collect_flow_detail_page(id, opts)
      body = FerricstoreServer.Health.Dashboard.render_flow_detail_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/metrics", peer, headers) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      body = Ferricstore.Metrics.scrape()
      send_text_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", _path, _peer, _headers) do
    send_response(socket, transport, 404, "Not Found", ~s({"error":"not found"}))
  end

  defp dispatch_request(socket, transport, _method, _path, _peer, _headers) do
    send_response(
      socket,
      transport,
      405,
      "Method Not Allowed",
      ~s({"error":"method not allowed"})
    )
  end

  defp handle_keyspace_page(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        query
        |> URI.decode_query()
        |> Map.merge(Auth.dashboard_collect_opts(peer, headers))
        |> FerricstoreServer.Health.Dashboard.collect_keyspace_page()

      body = FerricstoreServer.Health.Dashboard.render_keyspace_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_doctor_page(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        query
        |> URI.decode_query()
        |> FerricstoreServer.Health.Dashboard.collect_doctor_page()

      body = FerricstoreServer.Health.Dashboard.render_doctor_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_flow_api(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        query
        |> FerricstoreServer.Health.Dashboard.flow_opts_from_query()
        |> Auth.dashboard_flow_collect_opts(peer, headers)
        |> FerricstoreServer.Health.Dashboard.collect_flow_page()

      body = data |> FerricstoreServer.Health.Dashboard.live_flow_payload() |> Jason.encode!()
      send_response(socket, transport, 200, "OK", "application/json; charset=utf-8", body)
    end
  end

  defp handle_flow_overview(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        query
        |> FerricstoreServer.Health.Dashboard.flow_opts_from_query()
        |> Auth.dashboard_flow_collect_opts(peer, headers)
        |> FerricstoreServer.Health.Dashboard.collect_flow_page()

      body = FerricstoreServer.Health.Dashboard.render_flow_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_flow_states(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      opts =
        query
        |> FerricstoreServer.Health.Dashboard.flow_states_opts_from_query()
        |> Auth.dashboard_flow_collect_opts(peer, headers)

      data = FerricstoreServer.Health.Dashboard.collect_flow_states_page(opts)
      body = FerricstoreServer.Health.Dashboard.render_flow_states_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_flow_signals(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      opts =
        query
        |> FerricstoreServer.Health.Dashboard.flow_signals_opts_from_query()
        |> Auth.dashboard_flow_collect_opts(peer, headers)

      data = FerricstoreServer.Health.Dashboard.collect_flow_signals_page(opts)
      body = FerricstoreServer.Health.Dashboard.render_flow_signals_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_flow_policies(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      params = FlowPaths.decode_form_body(query)

      data =
        FerricstoreServer.Health.Dashboard.collect_flow_policies_page(
          flash: FerricstoreServer.Health.Dashboard.flow_policy_flash_from_query(query),
          edit_type: Map.get(params, "edit", "")
        )

      body = FerricstoreServer.Health.Dashboard.render_flow_policies_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_flow_retention(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      params = FlowPaths.decode_form_body(query)

      data =
        [
          flash: FerricstoreServer.Health.Dashboard.flow_retention_flash_from_query(query),
          limit: Map.get(params, "limit", "")
        ]
        |> Auth.dashboard_flow_collect_opts(peer, headers)
        |> FerricstoreServer.Health.Dashboard.collect_flow_retention_page()

      body = FerricstoreServer.Health.Dashboard.render_flow_retention_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_flow_failures(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        query
        |> FerricstoreServer.Health.Dashboard.flow_failures_opts_from_query()
        |> Auth.dashboard_flow_collect_opts(peer, headers)
        |> FerricstoreServer.Health.Dashboard.collect_flow_failures_page()

      body = FerricstoreServer.Health.Dashboard.render_flow_failures_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_flow_lineage(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        query
        |> FerricstoreServer.Health.Dashboard.flow_lineage_opts_from_query()
        |> Auth.dashboard_flow_collect_opts(peer, headers)
        |> FerricstoreServer.Health.Dashboard.collect_flow_lineage_page()

      body = FerricstoreServer.Health.Dashboard.render_flow_lineage_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_flow_query(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        query
        |> FerricstoreServer.Health.Dashboard.flow_query_opts_from_query()
        |> Auth.dashboard_flow_collect_opts(peer, headers)
        |> FerricstoreServer.Health.Dashboard.collect_flow_query_page()

      body = FerricstoreServer.Health.Dashboard.render_flow_query_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_flow_lookup(socket, transport, peer, headers, query) do
    unless Auth.observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
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

      send_redirect_response(socket, transport, location)
    end
  end

  defp send_forbidden_response(socket, transport, path, requirement, reason) do
    Forbidden.send_response(socket, transport, path, requirement, reason)
  end

  defp log_dashboard_page_error(path, kind, reason) do
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

  # ---------------------------------------------------------------------------
  # HTTP response writing
  # ---------------------------------------------------------------------------

  @spec send_response(:inet.socket(), module(), pos_integer(), String.t(), String.t()) :: :ok
  defp send_response(socket, transport, status_code, status_text, body) do
    Response.send_response(socket, transport, status_code, status_text, body)
  end

  @spec send_html_response(:inet.socket(), module(), pos_integer(), String.t(), String.t()) :: :ok
  defp send_html_response(socket, transport, status_code, status_text, body) do
    Response.send_html_response(socket, transport, status_code, status_text, body)
  end

  @spec send_text_response(:inet.socket(), module(), pos_integer(), String.t(), String.t()) :: :ok
  defp send_text_response(socket, transport, status_code, status_text, body) do
    Response.send_text_response(socket, transport, status_code, status_text, body)
  end

  @spec send_redirect_response(:inet.socket(), module(), binary()) :: :ok
  defp send_redirect_response(socket, transport, location) do
    Response.send_redirect_response(socket, transport, location)
  end

  @spec send_redirect_response(:inet.socket(), module(), binary(), [{binary(), binary()}]) :: :ok
  defp send_redirect_response(socket, transport, location, extra_headers) do
    Response.send_redirect_response(socket, transport, location, extra_headers)
  end

  @spec send_response(
          :inet.socket(),
          module(),
          pos_integer(),
          String.t(),
          String.t(),
          String.t()
        ) :: :ok
  defp send_response(socket, transport, status_code, status_text, content_type, body) do
    Response.send_response(socket, transport, status_code, status_text, content_type, body)
  end
end
