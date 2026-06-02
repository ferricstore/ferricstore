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

  alias FerricstoreServer.Connection.Send, as: ConnSend
  alias FerricstoreServer.Acl

  require Logger

  @listener_ref :"#{__MODULE__}"
  @max_request_bytes 8_192
  @request_recv_timeout_ms 5_000
  @dashboard_session_cookie "ferricstore_dashboard"
  @dashboard_session_ttl_ms 8 * 60 * 60 * 1_000
  @dashboard_session_secret_key {__MODULE__, :dashboard_session_secret}

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
    peer = peer_addr(socket, transport)

    case read_request(socket, transport) do
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

  # Reads the HTTP request line and consumes headers.
  @spec read_request(:inet.socket(), module()) ::
          {:ok, String.t(), String.t(), map(), binary()} | :error
  defp read_request(socket, transport) do
    read_request(socket, transport, <<>>)
  end

  defp read_request(_socket, _transport, data) when byte_size(data) > @max_request_bytes do
    :error
  end

  defp read_request(socket, transport, data) do
    if :binary.match(data, "\r\n\r\n") != :nomatch do
      case parse_request_line(data) do
        {:ok, method, path, headers, body} ->
          read_request_body(socket, transport, method, path, headers, body)

        :error ->
          :error
      end
    else
      case transport.recv(socket, 0, @request_recv_timeout_ms) do
        {:ok, chunk} -> read_request(socket, transport, data <> chunk)
        {:error, _reason} -> :error
      end
    end
  end

  @spec parse_request_line(binary()) ::
          {:ok, String.t(), String.t(), map(), binary()} | :error
  defp parse_request_line(data) do
    case String.split(data, "\r\n\r\n", parts: 2) do
      [head, body] ->
        [request_line | header_lines] = String.split(head, "\r\n")

        case String.split(request_line, " ", parts: 3) do
          [method, path, _version] -> {:ok, method, path, parse_headers(header_lines), body}
          _ -> :error
        end

      _ ->
        :error
    end
  end

  defp read_request_body(socket, transport, method, path, headers, body) do
    with {:ok, content_length} <- request_content_length(headers),
         true <- content_length <= @max_request_bytes do
      cond do
        content_length == 0 ->
          {:ok, method, path, headers, <<>>}

        byte_size(body) >= content_length ->
          {:ok, method, path, headers, binary_part(body, 0, content_length)}

        true ->
          read_request_body(socket, transport, method, path, headers, body, content_length)
      end
    else
      _ -> :error
    end
  end

  defp read_request_body(_socket, _transport, _method, _path, _headers, body, content_length)
       when byte_size(body) > content_length or byte_size(body) > @max_request_bytes do
    :error
  end

  defp read_request_body(_socket, _transport, method, path, headers, body, content_length)
       when byte_size(body) == content_length do
    {:ok, method, path, headers, body}
  end

  defp read_request_body(socket, transport, method, path, headers, body, content_length) do
    needed = content_length - byte_size(body)

    case transport.recv(socket, needed, @request_recv_timeout_ms) do
      {:ok, chunk} ->
        read_request_body(socket, transport, method, path, headers, body <> chunk, content_length)

      {:error, _reason} ->
        :error
    end
  end

  defp request_content_length(headers) do
    case Map.get(headers, "content-length") do
      nil ->
        {:ok, 0}

      value ->
        case Integer.parse(value) do
          {n, ""} when n >= 0 -> {:ok, n}
          _ -> :error
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Request routing
  # ---------------------------------------------------------------------------

  @spec handle_request(:inet.socket(), module(), String.t(), String.t(), term(), map(), binary()) ::
          :ok
  defp handle_request(socket, transport, method, path, peer, headers, body) do
    case authorize_request(method, path, peer, headers) do
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
    send_html_response(socket, transport, 200, "OK", render_login_page("", nil))
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
      render_login_page(Map.get(params, "next", ""), nil)
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
    params = decode_form_body(body)
    username = params |> Map.get("username", "") |> String.trim()
    password = Map.get(params, "password", "")
    next = sanitize_dashboard_next(Map.get(params, "next", ""))

    case Acl.authenticate(username, password) do
      {:ok, user} ->
        send_redirect_response(socket, transport, next, [
          {"Set-Cookie", dashboard_session_cookie(user)}
        ])

      {:error, reason} ->
        send_html_response(
          socket,
          transport,
          401,
          "Unauthorized",
          render_login_page(next, reason)
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
      {"Set-Cookie", clear_dashboard_session_cookie()}
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
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      params = decode_form_body(body)

      case authorize_command_request(peer, headers, {"FERRICSTORE.DOCTOR", []}, :html) do
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
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      params = decode_form_body(body)

      case authorize_command_request(
             peer,
             headers,
             flow_policy_form_requirement(params),
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
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      params = decode_form_body(body)

      case authorize_command_request(
             peer,
             headers,
             flow_retention_form_requirement(params),
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
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      params = decode_form_body(body)

      case authorize_command_request(
             peer,
             headers,
             flow_reclaim_form_requirement(params),
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
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      case decode_flow_rewind_action(encoded_action) do
        {:ok, id} ->
          params =
            body
            |> decode_form_body()
            |> Map.put_new("id", id)

          case authorize_command_request(
                 peer,
                 headers,
                 flow_rewind_form_requirement(id, params),
                 :html
               ) do
            :ok ->
              location =
                case FerricstoreServer.Health.Dashboard.apply_flow_rewind_form(params) do
                  {:ok, id, partition_key} ->
                    flow_detail_location(id, partition_key, %{"status" => "rewound"})

                  {:error, reason} ->
                    partition_key = Map.get(params, "partition_key", "")

                    flow_detail_location(id, partition_key, %{
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
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect()
      body = FerricstoreServer.Health.Dashboard.render(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/api/overview", peer, headers) do
    unless observability_authorized?(peer, headers) do
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
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      case FerricstoreServer.Health.Dashboard.live_payload(
             api_path,
             dashboard_collect_opts(peer, headers)
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
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect_slowlog_page()
      body = FerricstoreServer.Health.Dashboard.render_slowlog_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/merge", peer, headers) do
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect_merge_page()
      body = FerricstoreServer.Health.Dashboard.render_merge_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/config", peer, headers) do
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect_config_page()
      body = FerricstoreServer.Health.Dashboard.render_config_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/raft", peer, headers) do
    unless observability_authorized?(peer, headers) do
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
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      send_redirect_response(socket, transport, "/dashboard/raft")
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/clients", peer, headers) do
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect_clients_page()
      body = FerricstoreServer.Health.Dashboard.render_clients_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/storage", peer, headers) do
    unless observability_authorized?(peer, headers) do
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
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect_commands_page()
      body = FerricstoreServer.Health.Dashboard.render_commands_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/reads", peer, headers) do
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect_reads_page()
      body = FerricstoreServer.Health.Dashboard.render_reads_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/prefixes", peer, headers) do
    unless observability_authorized?(peer, headers) do
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
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        peer
        |> dashboard_flow_collect_opts(headers)
        |> FerricstoreServer.Health.Dashboard.collect_flow_workers_page()

      body = FerricstoreServer.Health.Dashboard.render_flow_workers_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/due", peer, headers) do
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        peer
        |> dashboard_flow_collect_opts(headers)
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
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      send_redirect_response(socket, transport, "/dashboard/config")
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/projections", peer, headers) do
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      send_redirect_response(socket, transport, "/dashboard/flow")
    end
  end

  defp dispatch_request(socket, transport, "GET", "/dashboard/flow/" <> encoded_id, peer, headers) do
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      {id, opts} = decode_flow_detail_request(encoded_id)
      data = FerricstoreServer.Health.Dashboard.collect_flow_detail_page(id, opts)
      body = FerricstoreServer.Health.Dashboard.render_flow_detail_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp dispatch_request(socket, transport, "GET", "/metrics", peer, headers) do
    unless observability_authorized?(peer, headers) do
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
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        query
        |> URI.decode_query()
        |> Map.merge(dashboard_collect_opts(peer, headers))
        |> FerricstoreServer.Health.Dashboard.collect_keyspace_page()

      body = FerricstoreServer.Health.Dashboard.render_keyspace_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_doctor_page(socket, transport, peer, headers, query) do
    unless observability_authorized?(peer, headers) do
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
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        query
        |> FerricstoreServer.Health.Dashboard.flow_opts_from_query()
        |> dashboard_flow_collect_opts(peer, headers)
        |> FerricstoreServer.Health.Dashboard.collect_flow_page()

      body = data |> FerricstoreServer.Health.Dashboard.live_flow_payload() |> Jason.encode!()
      send_response(socket, transport, 200, "OK", "application/json; charset=utf-8", body)
    end
  end

  defp handle_flow_overview(socket, transport, peer, headers, query) do
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        query
        |> FerricstoreServer.Health.Dashboard.flow_opts_from_query()
        |> dashboard_flow_collect_opts(peer, headers)
        |> FerricstoreServer.Health.Dashboard.collect_flow_page()

      body = FerricstoreServer.Health.Dashboard.render_flow_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_flow_states(socket, transport, peer, headers, query) do
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      opts =
        query
        |> FerricstoreServer.Health.Dashboard.flow_states_opts_from_query()
        |> dashboard_flow_collect_opts(peer, headers)

      data = FerricstoreServer.Health.Dashboard.collect_flow_states_page(opts)
      body = FerricstoreServer.Health.Dashboard.render_flow_states_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_flow_signals(socket, transport, peer, headers, query) do
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      opts =
        query
        |> FerricstoreServer.Health.Dashboard.flow_signals_opts_from_query()
        |> dashboard_flow_collect_opts(peer, headers)

      data = FerricstoreServer.Health.Dashboard.collect_flow_signals_page(opts)
      body = FerricstoreServer.Health.Dashboard.render_flow_signals_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_flow_policies(socket, transport, peer, headers, query) do
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      params = decode_form_body(query)

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
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      params = decode_form_body(query)

      data =
        [
          flash: FerricstoreServer.Health.Dashboard.flow_retention_flash_from_query(query),
          limit: Map.get(params, "limit", "")
        ]
        |> dashboard_flow_collect_opts(peer, headers)
        |> FerricstoreServer.Health.Dashboard.collect_flow_retention_page()

      body = FerricstoreServer.Health.Dashboard.render_flow_retention_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_flow_failures(socket, transport, peer, headers, query) do
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        query
        |> FerricstoreServer.Health.Dashboard.flow_failures_opts_from_query()
        |> dashboard_flow_collect_opts(peer, headers)
        |> FerricstoreServer.Health.Dashboard.collect_flow_failures_page()

      body = FerricstoreServer.Health.Dashboard.render_flow_failures_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_flow_lineage(socket, transport, peer, headers, query) do
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        query
        |> FerricstoreServer.Health.Dashboard.flow_lineage_opts_from_query()
        |> dashboard_flow_collect_opts(peer, headers)
        |> FerricstoreServer.Health.Dashboard.collect_flow_lineage_page()

      body = FerricstoreServer.Health.Dashboard.render_flow_lineage_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_flow_query(socket, transport, peer, headers, query) do
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data =
        query
        |> FerricstoreServer.Health.Dashboard.flow_query_opts_from_query()
        |> dashboard_flow_collect_opts(peer, headers)
        |> FerricstoreServer.Health.Dashboard.collect_flow_query_page()

      body = FerricstoreServer.Health.Dashboard.render_flow_query_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_flow_lookup(socket, transport, peer, headers, query) do
    unless observability_authorized?(peer, headers) do
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
            flow_detail_location(id, partition_key)
        end

      send_redirect_response(socket, transport, location)
    end
  end

  defp decode_form_body(body) when is_binary(body) do
    URI.decode_query(body)
  rescue
    _ -> %{}
  end

  defp decode_flow_detail_request(encoded_id_with_query) do
    {encoded_id, query} =
      case String.split(encoded_id_with_query, "?", parts: 2) do
        [encoded_id, query] -> {encoded_id, query}
        [encoded_id] -> {encoded_id, ""}
      end

    opts = FerricstoreServer.Health.Dashboard.flow_detail_opts_from_query(query)

    {URI.decode(encoded_id), opts}
  end

  defp decode_flow_rewind_action(encoded_action) do
    suffix = "/rewind"

    if String.ends_with?(encoded_action, suffix) do
      encoded_id =
        binary_part(encoded_action, 0, byte_size(encoded_action) - byte_size(suffix))

      {:ok, URI.decode(encoded_id)}
    else
      :not_found
    end
  end

  defp flow_detail_location(id, ""),
    do: "/dashboard/flow/" <> URI.encode(id, &URI.char_unreserved?/1)

  defp flow_detail_location(id, nil),
    do: "/dashboard/flow/" <> URI.encode(id, &URI.char_unreserved?/1)

  defp flow_detail_location(id, partition_key) do
    "/dashboard/flow/" <>
      URI.encode(id, &URI.char_unreserved?/1) <>
      "?" <> URI.encode_query(%{"partition_key" => partition_key})
  end

  defp flow_detail_location(id, partition_key, extra_params) when is_map(extra_params) do
    params =
      extra_params
      |> Enum.reject(fn {_key, value} -> is_nil(value) or value == "" end)
      |> Map.new()

    params =
      case partition_key do
        key when is_binary(key) and key != "" -> Map.put(params, "partition_key", key)
        _ -> params
      end

    path = "/dashboard/flow/" <> URI.encode(id, &URI.char_unreserved?/1)

    if map_size(params) == 0, do: path, else: path <> "?" <> URI.encode_query(params)
  end

  @doc false
  @spec observability_authorized?(term(), map()) :: boolean()
  def observability_authorized?(peer, headers) do
    not Acl.protected_mode?() or dashboard_session_user(headers) != nil or
      loopback_peer_allowed_for_observability?(peer)
  end

  defp authorize_request(_method, "/dashboard/login", _peer, _headers), do: :ok
  defp authorize_request(_method, "/dashboard/login?" <> _query, _peer, _headers), do: :ok
  defp authorize_request("POST", "/dashboard/logout", _peer, _headers), do: :ok

  defp authorize_request(method, path, peer, headers) do
    cond do
      dashboard_path?(path) ->
        authorize_dashboard_request(method, path, peer, headers)

      path == "/metrics" ->
        authorize_command_request(peer, headers, {"FERRICSTORE.METRICS", []}, :json)

      true ->
        :ok
    end
  end

  defp authorize_dashboard_request(method, path, peer, headers) do
    requirement = dashboard_route_requirement(method, path)

    case dashboard_identity(peer, headers) do
      {:ok, :open} ->
        :ok

      {:ok, {:acl, username}} ->
        authorize_acl_requirement(username, requirement)

      :error ->
        if dashboard_api_path?(path) do
          {:unauthorized, "login required"}
        else
          {:redirect_login, dashboard_login_location(path)}
        end
    end
  end

  defp authorize_command_request(peer, headers, requirement, response_kind) do
    case dashboard_identity(peer, headers) do
      {:ok, :open} -> :ok
      {:ok, {:acl, username}} -> authorize_acl_requirement(username, requirement)
      :error when response_kind == :json -> {:unauthorized, "login required"}
      :error -> {:redirect_login, dashboard_login_location("/dashboard")}
    end
  end

  defp authorize_acl_requirement(username, {"*", opts}),
    do: require_enabled_acl_user({"*", opts}, username)

  defp authorize_acl_requirement(username, {command, opts}) do
    with :ok <- Acl.check_command(username, command),
         :ok <- authorize_acl_key(username, command, opts) do
      :ok
    else
      {:error, reason} -> {:forbidden, {command, opts}, reason}
    end
  end

  defp require_enabled_acl_user(requirement, username) do
    case Acl.check_permission(username, "*") do
      :ok -> :ok
      {:error, reason} -> {:forbidden, requirement, reason}
    end
  end

  defp authorize_acl_key(username, command, opts) do
    case Keyword.get(opts, :key) do
      nil ->
        :ok

      {key, access} ->
        case Acl.check_key_access(username, key, access) do
          :ok -> :ok
          {:error, reason} -> {:error, "#{reason} (#{command} key)"}
        end
    end
  end

  defp dashboard_identity(_peer, headers) do
    cond do
      not Acl.protected_mode?() ->
        {:ok, :open}

      true ->
        case dashboard_session_user(headers) do
          username when is_binary(username) -> {:ok, {:acl, username}}
          _ -> :error
        end
    end
  end

  defp dashboard_collect_opts(peer, headers) do
    case dashboard_identity(peer, headers) do
      {:ok, {:acl, username}} -> %{"acl_username" => username}
      _other -> %{}
    end
  end

  defp dashboard_flow_collect_opts(peer, headers),
    do: dashboard_flow_collect_opts([], peer, headers)

  defp dashboard_flow_collect_opts(opts, peer, headers) when is_list(opts) do
    case dashboard_collect_opts(peer, headers) do
      %{"acl_username" => username} when is_binary(username) ->
        Keyword.put(opts, :acl_username, username)

      _other ->
        opts
    end
  end

  defp dashboard_path?("/dashboard"), do: true
  defp dashboard_path?("/dashboard?" <> _query), do: true
  defp dashboard_path?("/dashboard/" <> _rest), do: true
  defp dashboard_path?(_path), do: false

  defp dashboard_api_path?("/dashboard/api"), do: true
  defp dashboard_api_path?("/dashboard/api/" <> _rest), do: true
  defp dashboard_api_path?(_path), do: false

  defp dashboard_route_requirement("GET", path) do
    {clean_path, query} = split_path_query(path)

    case clean_path do
      "/dashboard" -> {"INFO", []}
      "/dashboard/slowlog" -> {"SLOWLOG", []}
      "/dashboard/merge" -> {"INFO", []}
      "/dashboard/config" -> {"CONFIG", []}
      "/dashboard/raft" -> {"CLUSTER.STATUS", []}
      "/dashboard/consensus" -> {"CLUSTER.STATUS", []}
      "/dashboard/clients" -> {"CLIENT.LIST", []}
      "/dashboard/storage" -> {"INFO", []}
      "/dashboard/doctor" -> {"FERRICSTORE.DOCTOR", []}
      "/dashboard/keyspace" -> keyspace_requirement(query)
      "/dashboard/commands" -> {"INFO", []}
      "/dashboard/reads" -> {"INFO", []}
      "/dashboard/prefixes" -> {"SCAN", []}
      "/dashboard/flow" -> {"FLOW.LIST", []}
      "/dashboard/flow/lookup" -> flow_lookup_requirement(query)
      "/dashboard/flow/states" -> {"FLOW.LIST", []}
      "/dashboard/flow/workers" -> {"FLOW.LIST", []}
      "/dashboard/flow/due" -> {"FLOW.LIST", []}
      "/dashboard/flow/failures" -> flow_index_view_requirement("FLOW.FAILURES", query)
      "/dashboard/flow/lineage" -> flow_lineage_requirement(query)
      "/dashboard/flow/query" -> flow_query_requirement(query)
      "/dashboard/flow/signals" -> {"FLOW.HISTORY", []}
      "/dashboard/flow/policies" -> {"FLOW.POLICY.GET", []}
      "/dashboard/flow/retention" -> {"FLOW.LIST", []}
      "/dashboard/flow/config" -> {"CONFIG", []}
      "/dashboard/flow/projections" -> {"FLOW.LIST", []}
      "/dashboard/api/overview" -> {"INFO", []}
      "/dashboard/api/flow" -> {"FLOW.LIST", []}
      "/dashboard/api/flow/states" -> {"FLOW.LIST", []}
      "/dashboard/api/flow/workers" -> {"FLOW.LIST", []}
      "/dashboard/api/flow/due" -> {"FLOW.LIST", []}
      "/dashboard/api/flow/signals" -> {"FLOW.HISTORY", []}
      "/dashboard/api/flow/projections" -> {"FLOW.LIST", []}
      "/dashboard/api/flow/value" -> flow_value_requirement(query)
      "/dashboard/api/slowlog" -> {"SLOWLOG", []}
      "/dashboard/api/merge" -> {"INFO", []}
      "/dashboard/api/raft" -> {"CLUSTER.STATUS", []}
      "/dashboard/api/clients" -> {"CLIENT.LIST", []}
      "/dashboard/api/storage" -> {"INFO", []}
      "/dashboard/api/keyspace" -> keyspace_requirement(query)
      "/dashboard/api/commands" -> {"INFO", []}
      "/dashboard/api/reads" -> {"INFO", []}
      "/dashboard/api/prefixes" -> {"SCAN", []}
      _ -> flow_detail_or_default_requirement(clean_path, query)
    end
  end

  defp dashboard_route_requirement("POST", path) do
    {clean_path, _query} = split_path_query(path)

    case clean_path do
      "/dashboard/flow/failures" ->
        {"FLOW.RECLAIM", []}

      "/dashboard/flow/policies" ->
        {"FLOW.POLICY.SET", []}

      "/dashboard/flow/retention" ->
        {"FLOW.LIST", []}

      "/dashboard/doctor" ->
        {"FERRICSTORE.DOCTOR", []}

      _ ->
        flow_rewind_or_default_requirement(clean_path)
    end
  end

  defp dashboard_route_requirement(_method, _path), do: {"*", []}

  defp flow_retention_form_requirement(%{"action" => "cleanup"}) do
    # Retention cleanup is a global destructive command. Requiring write
    # access to "*" prevents a tenant-scoped dashboard user from cleaning
    # terminal rows outside their ACL key scope. Dry-run stays read-only.
    {"FLOW.RETENTION_CLEANUP", key: {"*", :write}}
  end

  defp flow_retention_form_requirement(_params), do: {"FLOW.LIST", []}

  defp flow_lookup_requirement(query) do
    id =
      query
      |> URI.decode_query()
      |> Map.get("id", "")
      |> String.trim()

    partition_key = flow_partition_key_from_query(query)

    cond do
      id == "" ->
        {"FLOW.GET", []}

      partition_key != "" ->
        {"FLOW.GET", key: {partition_key, :read}}

      true ->
        {"FLOW.GET", key: {id, :read}}
    end
  rescue
    _ -> {"FLOW.GET", []}
  end

  defp flow_detail_or_default_requirement("/dashboard/flow/" <> encoded_id, query) do
    id = URI.decode(encoded_id)
    {"FLOW.GET", key: {flow_acl_key_from_query(id, query), :read}}
  end

  defp flow_detail_or_default_requirement("/dashboard/api/flow/" <> encoded_id, query) do
    id = URI.decode(encoded_id)
    {"FLOW.GET", key: {flow_acl_key_from_query(id, query), :read}}
  end

  defp flow_detail_or_default_requirement(_path, _query), do: {"INFO", []}

  defp flow_value_requirement(query) do
    flow_id =
      query
      |> URI.decode_query()
      |> Map.get("flow", "")
      |> String.trim()

    partition_key = flow_partition_key_from_query(query)

    cond do
      flow_id == "" ->
        {"FLOW.GET", []}

      partition_key != "" ->
        {"FLOW.GET", key: {partition_key, :read}}

      true ->
        {"FLOW.GET", key: {flow_id, :read}}
    end
  rescue
    _ -> {"FLOW.GET", []}
  end

  defp keyspace_requirement(query) do
    key =
      query
      |> URI.decode_query()
      |> Map.get("key", "")
      |> String.trim()

    if key == "" do
      {"SCAN", []}
    else
      {"GET", key: {key, :read}}
    end
  rescue
    _ -> {"SCAN", []}
  end

  defp flow_lineage_requirement(query) do
    mode =
      query
      |> URI.decode_query()
      |> Map.get("mode", "root")

    command =
      case mode do
        "parent" -> "FLOW.BY_PARENT"
        "correlation" -> "FLOW.BY_CORRELATION"
        _ -> "FLOW.BY_ROOT"
      end

    case flow_partition_key_from_query(query) do
      "" -> {command, []}
      partition_key -> {command, key: {partition_key, :read}}
    end
  rescue
    _ -> {"FLOW.BY_ROOT", []}
  end

  defp flow_query_requirement(query) do
    params = URI.decode_query(query)
    kind = Map.get(params, "kind", "list")
    command = flow_query_command_requirement(kind)
    partition_key = flow_partition_key_from_query(query)
    type = params |> Map.get("type", "") |> String.trim()

    key =
      params
      |> Map.get("id", "")
      |> String.trim()
      |> flow_acl_key_from_query(query)

    flow_query_key_requirement(command, kind, key, partition_key, type)
  rescue
    _ -> {"FLOW.LIST", []}
  end

  defp flow_index_view_requirement(command, query) do
    params = URI.decode_query(query)

    partition_key =
      params
      |> Map.get("partition_key", "")
      |> String.trim()

    type =
      params
      |> Map.get("type", "")
      |> String.trim()

    cond do
      partition_key != "" -> {command, key: {partition_key, :read}}
      type != "" -> {command, key: {type, :read}}
      true -> {command, []}
    end
  rescue
    _ -> {command, []}
  end

  defp flow_query_key_requirement(command, "history", "", _partition_key, _type),
    do: {command, []}

  defp flow_query_key_requirement(command, "history", key, _partition_key, _type),
    do: {command, key: {key, :read}}

  defp flow_query_key_requirement(command, kind, _key, partition_key, type)
       when kind in ["failures", "list", "stuck", "terminals"] do
    cond do
      partition_key != "" -> {command, key: {partition_key, :read}}
      type != "" -> {command, key: {type, :read}}
      true -> {command, []}
    end
  end

  defp flow_query_key_requirement(command, _kind, _key, "", _type) do
    {command, []}
  end

  defp flow_query_key_requirement(command, _kind, _key, partition_key, _type) do
    {command, key: {partition_key, :read}}
  end

  defp flow_query_command_requirement(kind) when is_binary(kind) do
    case kind do
      "terminals" -> "FLOW.TERMINALS"
      "failures" -> "FLOW.FAILURES"
      "stuck" -> "FLOW.STUCK"
      "history" -> "FLOW.HISTORY"
      "by_parent" -> "FLOW.BY_PARENT"
      "by_root" -> "FLOW.BY_ROOT"
      "by_correlation" -> "FLOW.BY_CORRELATION"
      _ -> "FLOW.LIST"
    end
  end

  defp flow_query_command_requirement(_kind), do: "FLOW.LIST"

  defp flow_rewind_or_default_requirement("/dashboard/flow/" <> encoded_action) do
    case decode_flow_rewind_action(encoded_action) do
      {:ok, _id} -> {"FLOW.REWIND", []}
      :not_found -> {"FLOW.REWIND", []}
    end
  end

  defp flow_rewind_or_default_requirement(_path), do: {"INFO", []}

  defp flow_rewind_form_requirement(id, params) do
    key =
      params
      |> Map.get("partition_key", "")
      |> String.trim()
      |> case do
        "" -> id
        partition_key -> partition_key
      end

    if key == "" do
      {"FLOW.REWIND", []}
    else
      {"FLOW.REWIND", key: {key, :write}}
    end
  end

  defp flow_acl_key_from_query(id, query) do
    case flow_partition_key_from_query(query) do
      "" -> id
      partition_key -> partition_key
    end
  end

  defp flow_partition_key_from_query(query) do
    query
    |> URI.decode_query()
    |> Map.get("partition_key", "")
    |> String.trim()
  rescue
    _ -> ""
  end

  defp flow_policy_form_requirement(params) do
    type =
      params
      |> Map.get("type", "")
      |> String.trim()

    if type == "" do
      {"FLOW.POLICY.SET", []}
    else
      {"FLOW.POLICY.SET", key: {type, :write}}
    end
  end

  defp flow_reclaim_form_requirement(params) do
    type =
      params
      |> Map.get("type", "")
      |> String.trim()

    partition_key =
      params
      |> Map.get("partition_key", "")
      |> String.trim()

    cond do
      partition_key != "" -> {"FLOW.RECLAIM", key: {partition_key, :write}}
      type != "" -> {"FLOW.RECLAIM", key: {type, :write}}
      true -> {"FLOW.RECLAIM", []}
    end
  end

  defp split_path_query(path) do
    case String.split(path, "?", parts: 2) do
      [clean_path, query] -> {clean_path, query}
      [clean_path] -> {clean_path, ""}
    end
  end

  defp dashboard_login_location(path) do
    "/dashboard/login?" <> URI.encode_query(%{"next" => sanitize_dashboard_next(path)})
  end

  defp sanitize_dashboard_next(path) when is_binary(path) do
    cond do
      path == "" -> "/dashboard"
      has_control_byte?(path) -> "/dashboard"
      String.starts_with?(path, "//") -> "/dashboard"
      String.starts_with?(path, "/dashboard/login") -> "/dashboard"
      String.starts_with?(path, "/dashboard") -> path
      true -> "/dashboard"
    end
  end

  defp sanitize_dashboard_next(_path), do: "/dashboard"

  defp has_control_byte?(path) do
    :binary.match(path, [<<"\r">>, <<"\n">>]) != :nomatch or
      :binary.match(path, for(byte <- 0..31, byte not in [?\r, ?\n], do: <<byte>>)) != :nomatch or
      :binary.match(path, <<127>>) != :nomatch
  end

  defp render_login_page(next, error) do
    safe_next = sanitize_dashboard_next(next)
    error_html = if is_binary(error), do: "<p class=\"error\">#{html_escape(error)}</p>", else: ""

    """
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="utf-8">
      <title>FerricStore Dashboard Login</title>
      <style>
        body{margin:0;min-height:100vh;display:grid;place-items:center;background:#0d1117;color:#e6edf3;font-family:system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
        main{width:min(420px,calc(100vw - 32px));border:1px solid #30363d;background:#161b22;padding:24px}
        h1{font-size:22px;margin:0 0 8px}
        p{color:#8b949e;margin:0 0 18px}
        label{display:block;margin:14px 0 6px;color:#c9d1d9;font-size:13px}
        input{box-sizing:border-box;width:100%;border:1px solid #30363d;background:#0d1117;color:#e6edf3;padding:10px 12px;font:inherit}
        button{margin-top:18px;width:100%;border:0;background:#238636;color:white;padding:11px 12px;font:inherit;cursor:pointer}
        .error{border:1px solid #f85149;color:#ffb4ae;background:#2d1215;padding:10px 12px}
      </style>
    </head>
    <body>
      <main>
        <h1>FerricStore Dashboard</h1>
        <p>Sign in with a Redis ACL user. Page access follows that user's command and key permissions.</p>
        #{error_html}
        <form method="post" action="/dashboard/login">
          <input type="hidden" name="next" value="#{html_escape(safe_next)}">
          <label for="username">Username</label>
          <input id="username" name="username" autocomplete="username" autofocus>
          <label for="password">Password</label>
          <input id="password" name="password" type="password" autocomplete="current-password">
          <button type="submit">Sign in</button>
        </form>
      </main>
    </body>
    </html>
    """
  end

  defp send_forbidden_response(socket, transport, path, requirement, reason) do
    details = forbidden_requirement_details(requirement)

    if dashboard_api_path?(path) or path == "/metrics" do
      send_response(
        socket,
        transport,
        403,
        "Forbidden",
        "application/json",
        Jason.encode!(Map.merge(%{error: "forbidden", reason: reason}, details))
      )
    else
      send_html_response(
        socket,
        transport,
        403,
        "Forbidden",
        """
        <!DOCTYPE html>
        <html lang="en"><head><meta charset="utf-8"><title>Forbidden</title></head>
        <body><h1>Forbidden</h1>#{forbidden_details_html(details, reason)}<p><a href="/dashboard/login">Sign in as another user</a></p></body></html>
        """
      )
    end
  end

  defp forbidden_requirement_details({command, opts}) do
    command = String.upcase(to_string(command))

    %{
      required_command: command,
      required_acl_rule: acl_command_rule(command)
    }
    |> maybe_put_required_key(opts)
  end

  defp forbidden_requirement_details(command) do
    forbidden_requirement_details({command, []})
  end

  defp maybe_put_required_key(details, opts) do
    case Keyword.get(opts, :key) do
      {key, access} ->
        details
        |> Map.put(:required_key, key)
        |> Map.put(:required_key_access, to_string(access))
        |> Map.put(:required_key_rule, acl_key_rule(access, key))

      _ ->
        details
    end
  end

  defp acl_command_rule("*"), do: "+@all"
  defp acl_command_rule(command), do: "+" <> command

  defp acl_key_rule(:read, key), do: "%R~" <> key
  defp acl_key_rule(:write, key), do: "%W~" <> key
  defp acl_key_rule(_access, key), do: "~" <> key

  defp forbidden_details_html(details, reason) do
    key_html =
      case Map.get(details, :required_key) do
        nil ->
          ""

        key ->
          """
          <dt>Required key access</dt><dd><code>#{html_escape(Map.fetch!(details, :required_key_access))}</code> on <code>#{html_escape(key)}</code></dd>
          <dt>Key ACL rule</dt><dd><code>#{html_escape(Map.fetch!(details, :required_key_rule))}</code></dd>
          """
      end

    """
    <p>#{html_escape(reason)}</p>
    <dl>
      <dt>Required ACL command</dt><dd><code>#{html_escape(Map.fetch!(details, :required_acl_rule))}</code></dd>
      #{key_html}
    </dl>
    """
  end

  defp dashboard_session_cookie(username) do
    token = dashboard_session_token(username)
    max_age = div(@dashboard_session_ttl_ms, 1_000)

    "#{@dashboard_session_cookie}=#{token}; Path=/dashboard; Max-Age=#{max_age}; HttpOnly; SameSite=Lax"
  end

  defp clear_dashboard_session_cookie do
    "#{@dashboard_session_cookie}=; Path=/dashboard; Max-Age=0; HttpOnly; SameSite=Lax"
  end

  defp dashboard_session_token(username) do
    expires_at = System.system_time(:millisecond) + @dashboard_session_ttl_ms
    payload = :erlang.term_to_binary({username, expires_at})
    encoded_payload = Base.url_encode64(payload, padding: false)
    signature = dashboard_session_signature(encoded_payload)
    encoded_payload <> "." <> signature
  end

  defp dashboard_session_user(headers) do
    with token when is_binary(token) <- cookie_value(headers, @dashboard_session_cookie),
         [encoded_payload, signature] <- String.split(token, ".", parts: 2),
         true <- constant_time_equal?(signature, dashboard_session_signature(encoded_payload)),
         {:ok, payload} <- Base.url_decode64(encoded_payload, padding: false),
         {username, expires_at} when is_binary(username) and is_integer(expires_at) <-
           safe_binary_to_term(payload),
         true <- expires_at > System.system_time(:millisecond),
         %{enabled: true} <- Acl.get_user(username) do
      username
    else
      _ -> nil
    end
  end

  defp dashboard_session_signature(encoded_payload) do
    :crypto.mac(:hmac, :sha256, dashboard_session_secret(), encoded_payload)
    |> Base.url_encode64(padding: false)
  end

  defp dashboard_session_secret do
    case Application.get_env(:ferricstore, :dashboard_session_secret) do
      secret when is_binary(secret) and byte_size(secret) >= 32 ->
        secret

      _ ->
        case :persistent_term.get(@dashboard_session_secret_key, nil) do
          nil ->
            secret = :crypto.strong_rand_bytes(32)
            :persistent_term.put(@dashboard_session_secret_key, secret)
            secret

          secret ->
            secret
        end
    end
  end

  defp safe_binary_to_term(payload) do
    :erlang.binary_to_term(payload, [:safe])
  rescue
    _ -> :error
  end

  defp cookie_value(headers, name) do
    headers
    |> Map.get("cookie", "")
    |> String.split(";")
    |> Enum.find_value(fn part ->
      case String.split(String.trim(part), "=", parts: 2) do
        [^name, value] -> value
        _ -> nil
      end
    end)
  end

  defp html_escape(value) when is_binary(value) do
    value
    |> String.replace("&", "&amp;")
    |> String.replace("<", "&lt;")
    |> String.replace(">", "&gt;")
    |> String.replace("\"", "&quot;")
  end

  defp html_escape(value), do: value |> to_string() |> html_escape()

  defp loopback_peer_allowed_for_observability?(peer) do
    not Acl.protected_mode?() and loopback_peer?(peer)
  end

  defp peer_addr(socket, transport) do
    case transport.peername(socket) do
      {:ok, {addr, _port}} -> addr
      _ -> :unknown
    end
  end

  defp parse_headers(lines) do
    Enum.reduce(lines, %{}, fn line, acc ->
      case String.split(line, ":", parts: 2) do
        [name, value] ->
          Map.put(acc, String.downcase(String.trim(name)), String.trim(value))

        _ ->
          acc
      end
    end)
  end

  defp loopback_peer?({127, _, _, _}), do: true
  defp loopback_peer?({0, 0, 0, 0, 0, 0, 0, 1}), do: true
  defp loopback_peer?({0, 0, 0, 0, 0, 65_535, 32_512, _}), do: true
  defp loopback_peer?(_peer), do: false

  defp constant_time_equal?(left, right) when is_binary(left) and is_binary(right) do
    :crypto.hash(:sha256, left) == :crypto.hash(:sha256, right)
  end

  defp constant_time_equal?(_left, _right), do: false

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
    send_response(socket, transport, status_code, status_text, "application/json", body)
  end

  @spec send_html_response(:inet.socket(), module(), pos_integer(), String.t(), String.t()) :: :ok
  defp send_html_response(socket, transport, status_code, status_text, body) do
    send_response(socket, transport, status_code, status_text, "text/html; charset=utf-8", body)
  end

  @spec send_text_response(:inet.socket(), module(), pos_integer(), String.t(), String.t()) :: :ok
  defp send_text_response(socket, transport, status_code, status_text, body) do
    send_response(socket, transport, status_code, status_text, "text/plain; charset=utf-8", body)
  end

  @spec send_redirect_response(:inet.socket(), module(), binary()) :: :ok
  defp send_redirect_response(socket, transport, location) do
    send_redirect_response(socket, transport, location, [])
  end

  @spec send_redirect_response(:inet.socket(), module(), binary(), [{binary(), binary()}]) :: :ok
  defp send_redirect_response(socket, transport, location, extra_headers) do
    body = ""
    extra = encode_http_headers(extra_headers)

    response =
      "HTTP/1.1 302 Found\r\n" <>
        "Location: #{location}\r\n" <>
        extra <>
        "Content-Length: #{byte_size(body)}\r\n" <>
        "Connection: close\r\n" <>
        "\r\n" <>
        body

    ConnSend.send(socket, transport, response, :health_response)
    :ok
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
    content_length = byte_size(body)

    response =
      "HTTP/1.1 #{status_code} #{status_text}\r\n" <>
        "Content-Type: #{content_type}\r\n" <>
        "Content-Length: #{content_length}\r\n" <>
        "Connection: close\r\n" <>
        "\r\n" <>
        body

    ConnSend.send(socket, transport, response, :health_response)
    :ok
  end

  defp encode_http_headers(headers) do
    Enum.map_join(headers, "", fn {name, value} -> "#{name}: #{value}\r\n" end)
  end
end
