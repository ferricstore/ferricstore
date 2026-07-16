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

      config :ferricstore,
        health_port: 4000,
        health_probe_port: 4001

  `health_port` retains the combined dashboard, metrics, and health endpoint
  for compatibility. Point orchestrator probes at `health_probe_port`, which
  is served by an independent listener and cannot be exhausted by dashboard
  connections. Set either port to `0` in tests for an ephemeral binding.

  ## Kubernetes integration

      livenessProbe:
        httpGet:
          path: /health/live
          port: 4001
        initialDelaySeconds: 2
        periodSeconds: 10

      readinessProbe:
        httpGet:
          path: /health/ready
          port: 4001
        initialDelaySeconds: 2
        periodSeconds: 5
  """

  @behaviour :ranch_protocol

  alias Ferricstore.AuditLog
  alias FerricstoreServer.AuthRateLimiter
  alias FerricstoreServer.Health.Endpoint.Auth
  alias FerricstoreServer.Health.Endpoint.DashboardHandlers
  alias FerricstoreServer.Health.Endpoint.Forbidden
  alias FerricstoreServer.Health.Endpoint.Login
  alias FerricstoreServer.Health.Endpoint.FlowPaths
  alias FerricstoreServer.Health.Endpoint.Probes
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
  Returns the actual TCP port for the isolated liveness/readiness listener.

  Use this port for orchestrator probes. `port/0` intentionally continues to
  return the legacy combined dashboard and metrics listener port.
  """
  @spec probe_port() :: :inet.port_number()
  def probe_port do
    FerricstoreServer.Health.ProbeEndpoint.port()
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

  @doc false
  @spec observability_authorized?(term(), map()) :: boolean()
  defdelegate observability_authorized?(peer, headers), to: Auth

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
    :ok = Session.prepare_request(peer, headers)

    with :ok <- Session.validate_state_change(method, path, peer, headers, body) do
      authorize_and_dispatch(socket, transport, method, path, peer, headers, body)
    else
      {:error, reason} ->
        send_forbidden_response(socket, transport, path, {"CSRF", []}, reason)
    end
  end

  defp authorize_and_dispatch(socket, transport, method, path, peer, headers, body) do
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
    params = FerricstoreServer.Health.QueryDecoder.decode(query)

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
         peer,
         headers,
         body
       ) do
    params = FlowPaths.decode_form_body(body)
    username = Map.get(params, "username", "")
    password = Map.get(params, "password", "")
    next = Login.sanitize_next(Map.get(params, "next", ""))

    case AuthRateLimiter.permit(Session.client_peer(peer, headers), username, password) do
      {:ok, reservation} ->
        authenticate_dashboard_login(
          socket,
          transport,
          peer,
          headers,
          username,
          password,
          reservation,
          next
        )

      {:error, {:rate_limited, retry_after_ms}} ->
        audit_dashboard_login(:auth_failure, peer, username, true)

        send_html_response(
          socket,
          transport,
          429,
          "Too Many Requests",
          Login.render_page(next, "Too many login attempts. Try again later."),
          [{"Retry-After", Integer.to_string(div(retry_after_ms + 999, 1_000))}]
        )

      {:error, reason} ->
        audit_dashboard_login(:auth_failure, peer, username, false)

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
         peer,
         headers,
         _body
       ) do
    send_redirect_response(socket, transport, "/dashboard/login", [
      {"Set-Cookie", Session.clear_session_cookie(peer, headers)}
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
    case Auth.observability_authorized?(peer, headers) do
      false ->
        send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))

      true ->
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
    case Auth.observability_authorized?(peer, headers) do
      false ->
        send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))

      true ->
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
         "/dashboard/flow/governance",
         peer,
         headers,
         body
       ) do
    case Auth.observability_authorized?(peer, headers) do
      false ->
        send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))

      true ->
        params = FlowPaths.decode_form_body(body)
        requirement = RouteRequirements.flow_governance_form_requirement(params)

        case Auth.authorize_command_request(peer, headers, requirement, :html) do
          :ok ->
            location =
              case FerricstoreServer.Health.Dashboard.apply_flow_governance_form(params) do
                {:ok, message} ->
                  "/dashboard/flow/governance?" <>
                    URI.encode_query(%{"status" => "ok", "message" => message})

                {:error, reason} ->
                  "/dashboard/flow/governance?" <>
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
              "/dashboard/flow/governance",
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
         "/dashboard/flow/schedules",
         peer,
         headers,
         body
       ) do
    case Auth.observability_authorized?(peer, headers) do
      false ->
        send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))

      true ->
        params = FlowPaths.decode_form_body(body)
        requirement = RouteRequirements.flow_schedule_form_requirement(params)

        case Auth.authorize_command_request(peer, headers, requirement, :html) do
          :ok ->
            location =
              case FerricstoreServer.Health.Dashboard.apply_flow_schedule_form(params) do
                {:ok, message} ->
                  "/dashboard/flow/schedules?" <>
                    URI.encode_query(%{"status" => "ok", "message" => message})

                {:error, reason} ->
                  "/dashboard/flow/schedules?" <>
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
              "/dashboard/flow/schedules",
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
    case Auth.observability_authorized?(peer, headers) do
      false ->
        send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))

      true ->
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
                      "active_timeouts" => Map.get(result, :active_timeouts, 0),
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
    case Auth.observability_authorized?(peer, headers) do
      false ->
        send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))

      true ->
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
    case Auth.observability_authorized?(peer, headers) do
      false ->
        send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))

      true ->
        cond do
          match?({:ok, _id}, FlowPaths.decode_flow_rewind_action(encoded_action)) ->
            {:ok, id} = FlowPaths.decode_flow_rewind_action(encoded_action)

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

          match?({:ok, _id}, FlowPaths.decode_flow_signal_action(encoded_action)) ->
            {:ok, id} = FlowPaths.decode_flow_signal_action(encoded_action)

            params =
              body
              |> FlowPaths.decode_form_body()
              |> Map.put_new("id", id)

            case Auth.authorize_command_request(
                   peer,
                   headers,
                   RouteRequirements.flow_signal_form_requirement(id, params),
                   :html
                 ) do
              :ok ->
                location =
                  case FerricstoreServer.Health.Dashboard.apply_flow_signal_form(params) do
                    {:ok, id, partition_key} ->
                      FlowPaths.flow_detail_location(id, partition_key, %{"status" => "signaled"})

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

          true ->
            send_response(socket, transport, 404, "Not Found", ~s({"error":"not found"}))
        end
    end
  end

  defp dispatch_request(socket, transport, method, path, peer, headers, _body) do
    dispatch_request(socket, transport, method, path, peer, headers)
  end

  @spec dispatch_request(:inet.socket(), module(), String.t(), String.t(), term(), map()) :: :ok
  defp dispatch_request(socket, transport, "GET", "/health/live", _peer, _headers) do
    {status_code, status_text, body} = Probes.live_response()
    send_response(socket, transport, status_code, status_text, body)
  end

  defp dispatch_request(socket, transport, "GET", "/health/ready", _peer, _headers) do
    {status_code, status_text, body} = Probes.ready_response()
    send_response(socket, transport, status_code, status_text, body)
  end

  defp dispatch_request(socket, transport, "GET", "/favicon.ico", _peer, _headers) do
    send_response(socket, transport, 204, "No Content", "image/x-icon", "")
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard", peer, headers) do
    case Auth.observability_authorized?(peer, headers) do
      false ->
        send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))

      true ->
        data = FerricstoreServer.Health.Dashboard.collect()
        body = FerricstoreServer.Health.Dashboard.render(data)
        send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/api/overview", peer, headers) do
    case Auth.observability_authorized?(peer, headers) do
      false ->
        send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))

      true ->
        data = FerricstoreServer.Health.Dashboard.collect()
        payload = FerricstoreServer.Health.Dashboard.live_overview_payload(data)
        Response.send_live_json_response(socket, transport, payload)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/api/flow", peer, headers) do
    DashboardHandlers.handle_flow_api(socket, transport, peer, headers, "")
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/api/flow?" <> query, peer, headers) do
    DashboardHandlers.handle_flow_api(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/api/" <> api_path, peer, headers) do
    case Auth.observability_authorized?(peer, headers) do
      false ->
        send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))

      true ->
        case FerricstoreServer.Health.Dashboard.live_payload(
               api_path,
               Auth.dashboard_collect_opts(peer, headers)
             ) do
          {:ok, payload} ->
            Response.send_live_json_response(socket, transport, payload)

          :not_found ->
            send_response(socket, transport, 404, "Not Found", ~s({"error":"not found"}))
        end
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/slowlog", peer, headers) do
    DashboardHandlers.handle_slowlog_page(socket, transport, peer, headers)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/merge", peer, headers) do
    DashboardHandlers.handle_merge_page(socket, transport, peer, headers)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/config", peer, headers) do
    DashboardHandlers.handle_config_page(socket, transport, peer, headers)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/capabilities", peer, headers) do
    DashboardHandlers.handle_capabilities_page(socket, transport, peer, headers)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/security", peer, headers) do
    DashboardHandlers.handle_security_page(socket, transport, peer, headers, "")
  end

  defp dispatch_request(
         socket,
         transport,
         "GET",
         "/dashboard/security?" <> query,
         peer,
         headers
       ) do
    DashboardHandlers.handle_security_page(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/raft", peer, headers) do
    DashboardHandlers.handle_raft_page(socket, transport, peer, headers)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/consensus", peer, headers) do
    DashboardHandlers.handle_consensus_redirect(socket, transport, peer, headers)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/clients", peer, headers) do
    DashboardHandlers.handle_clients_page(socket, transport, peer, headers)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/storage", peer, headers) do
    DashboardHandlers.handle_storage_page(socket, transport, peer, headers)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/doctor", peer, headers) do
    DashboardHandlers.handle_doctor_page(socket, transport, peer, headers, "")
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/doctor?" <> query, peer, headers) do
    DashboardHandlers.handle_doctor_page(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/keyspace", peer, headers) do
    DashboardHandlers.handle_keyspace_page(socket, transport, peer, headers, "")
  end

  defp dispatch_request(
         socket,
         transport,
         "GET",
         "/dashboard/keyspace?" <> query,
         peer,
         headers
       ) do
    DashboardHandlers.handle_keyspace_page(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/commands", peer, headers) do
    DashboardHandlers.handle_commands_page(socket, transport, peer, headers)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/reads", peer, headers) do
    DashboardHandlers.handle_reads_page(socket, transport, peer, headers)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/streams", peer, headers) do
    DashboardHandlers.handle_streams_page(socket, transport, peer, headers)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/pubsub", peer, headers) do
    DashboardHandlers.handle_pubsub_page(socket, transport, peer, headers)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/prefixes", peer, headers) do
    DashboardHandlers.handle_prefixes_page(socket, transport, peer, headers)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow", peer, headers) do
    DashboardHandlers.handle_flow_overview(socket, transport, peer, headers, "")
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow?" <> query, peer, headers) do
    DashboardHandlers.handle_flow_overview(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/lookup", peer, headers) do
    DashboardHandlers.handle_flow_lookup(socket, transport, peer, headers, "")
  end

  defp dispatch_request(
         socket,
         transport,
         "GET",
         "/dashboard/flow/lookup?" <> query,
         peer,
         headers
       ) do
    DashboardHandlers.handle_flow_lookup(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/states", peer, headers) do
    DashboardHandlers.handle_flow_states(socket, transport, peer, headers, "")
  end

  defp dispatch_request(
         socket,
         transport,
         "GET",
         "/dashboard/flow/states?" <> query,
         peer,
         headers
       ) do
    DashboardHandlers.handle_flow_states(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/workers", peer, headers) do
    DashboardHandlers.handle_flow_workers_page(socket, transport, peer, headers)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/due", peer, headers) do
    DashboardHandlers.handle_flow_due_page(socket, transport, peer, headers)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/schedules", peer, headers) do
    DashboardHandlers.handle_flow_schedules(socket, transport, peer, headers, "")
  end

  defp dispatch_request(
         socket,
         transport,
         "GET",
         "/dashboard/flow/schedules?" <> query,
         peer,
         headers
       ) do
    DashboardHandlers.handle_flow_schedules(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/governance", peer, headers) do
    DashboardHandlers.handle_flow_governance(socket, transport, peer, headers, "")
  end

  defp dispatch_request(
         socket,
         transport,
         "GET",
         "/dashboard/flow/governance?" <> query,
         peer,
         headers
       ) do
    DashboardHandlers.handle_flow_governance(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/failures", peer, headers) do
    DashboardHandlers.handle_flow_failures(socket, transport, peer, headers, "")
  end

  defp dispatch_request(
         socket,
         transport,
         "GET",
         "/dashboard/flow/failures?" <> query,
         peer,
         headers
       ) do
    DashboardHandlers.handle_flow_failures(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/lineage", peer, headers) do
    DashboardHandlers.handle_flow_lineage(socket, transport, peer, headers, "")
  end

  defp dispatch_request(
         socket,
         transport,
         "GET",
         "/dashboard/flow/lineage?" <> query,
         peer,
         headers
       ) do
    DashboardHandlers.handle_flow_lineage(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/query", peer, headers) do
    DashboardHandlers.handle_flow_query(socket, transport, peer, headers, "")
  end

  defp dispatch_request(
         socket,
         transport,
         "GET",
         "/dashboard/flow/query?" <> query,
         peer,
         headers
       ) do
    DashboardHandlers.handle_flow_query(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/signals", peer, headers) do
    DashboardHandlers.handle_flow_signals(socket, transport, peer, headers, "")
  end

  defp dispatch_request(
         socket,
         transport,
         "GET",
         "/dashboard/flow/signals?" <> query,
         peer,
         headers
       ) do
    DashboardHandlers.handle_flow_signals(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/policies", peer, headers) do
    DashboardHandlers.handle_flow_policies(socket, transport, peer, headers, "")
  end

  defp dispatch_request(
         socket,
         transport,
         "GET",
         "/dashboard/flow/policies?" <> query,
         peer,
         headers
       ) do
    DashboardHandlers.handle_flow_policies(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/retention", peer, headers) do
    DashboardHandlers.handle_flow_retention(socket, transport, peer, headers, "")
  end

  defp dispatch_request(
         socket,
         transport,
         "GET",
         "/dashboard/flow/retention?" <> query,
         peer,
         headers
       ) do
    DashboardHandlers.handle_flow_retention(socket, transport, peer, headers, query)
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/config", peer, headers) do
    case Auth.observability_authorized?(peer, headers) do
      false ->
        send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))

      true ->
        send_redirect_response(socket, transport, "/dashboard/config")
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/projections", peer, headers) do
    case Auth.observability_authorized?(peer, headers) do
      false ->
        send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))

      true ->
        send_redirect_response(socket, transport, "/dashboard/flow")
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/" <> encoded_id, peer, headers) do
    case Auth.observability_authorized?(peer, headers) do
      false ->
        send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))

      true ->
        {id, opts} = FlowPaths.decode_flow_detail_request(encoded_id)
        opts = Auth.dashboard_flow_collect_opts(opts, peer, headers)
        data = FerricstoreServer.Health.Dashboard.collect_flow_detail_page(id, opts)
        body = FerricstoreServer.Health.Dashboard.render_flow_detail_page(data)
        send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/metrics", peer, headers) do
    case Auth.observability_authorized?(peer, headers) do
      false ->
        send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))

      true ->
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

  defp audit_dashboard_login(event, peer, username, rate_limited) do
    AuditLog.log(event, %{
      username: username,
      client_ip: Login.peer_string(peer),
      surface: :dashboard,
      rate_limited: rate_limited
    })
  end

  defp authenticate_dashboard_login(
         socket,
         transport,
         peer,
         headers,
         username,
         password,
         reservation,
         next
       ) do
    case Login.authenticate_session(username, password) do
      {:ok, user, auth_epoch} ->
        :ok = AuthRateLimiter.release_success(reservation)
        audit_dashboard_login(:auth_success, peer, username, false)

        send_redirect_response(socket, transport, next, [
          {"Set-Cookie", Session.session_cookie(user, auth_epoch, peer, headers)}
        ])

      {:error, reason} ->
        audit_dashboard_login(:auth_failure, peer, username, false)

        send_html_response(
          socket,
          transport,
          401,
          "Unauthorized",
          Login.render_page(next, reason)
        )
    end
  end

  defp send_forbidden_response(socket, transport, path, requirement, reason) do
    Forbidden.send_response(socket, transport, path, requirement, reason)
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

  defp send_html_response(socket, transport, status_code, status_text, body, extra_headers) do
    Response.send_html_response(
      socket,
      transport,
      status_code,
      status_text,
      body,
      extra_headers
    )
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
