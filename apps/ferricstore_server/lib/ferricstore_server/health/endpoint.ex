defmodule FerricstoreServer.Health.Endpoint do
  @moduledoc """
  Minimal HTTP/1.1 health endpoint for Kubernetes readiness and liveness
  probes and the built-in observability dashboard (spec 7.3).

  Runs a Ranch TCP listener on a configurable port (default: `4000`, or `0`
  for ephemeral in tests) that speaks just enough HTTP/1.1 to serve:

    * `GET /health/live`  -- 200 always (liveness probe: process is alive)
    * `GET /health/ready` -- 200 when ready, 503 during startup
    * `GET /dashboard`    -- HTML dashboard with auto-refresh (spec 7.3)
    * All other paths      -- 404

  ## Architecture

  This intentionally avoids adding Cowboy or Plug as dependencies. The HTTP
  parsing is minimal: we read the first line to extract the method and path,
  then consume remaining headers until we see the blank line (`\\r\\n\\r\\n`).
  Only `GET` requests are supported -- all others return 405.

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

  @listener_ref :"#{__MODULE__}"
  @max_request_bytes 8_192
  @request_recv_timeout_ms 5_000

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
      {:ok, method, path, headers} ->
        handle_request(socket, transport, method, path, peer, headers)

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
          {:ok, String.t(), String.t(), map()} | :error
  defp read_request(socket, transport) do
    read_request(socket, transport, <<>>)
  end

  defp read_request(_socket, _transport, data) when byte_size(data) > @max_request_bytes do
    :error
  end

  defp read_request(socket, transport, data) do
    if :binary.match(data, "\r\n\r\n") != :nomatch do
      parse_request_line(data)
    else
      case transport.recv(socket, 0, @request_recv_timeout_ms) do
        {:ok, chunk} -> read_request(socket, transport, data <> chunk)
        {:error, _reason} -> :error
      end
    end
  end

  @spec parse_request_line(binary()) :: {:ok, String.t(), String.t(), map()} | :error
  defp parse_request_line(data) do
    case String.split(data, "\r\n\r\n", parts: 2) do
      [head, _body] ->
        [request_line | header_lines] = String.split(head, "\r\n")

        case String.split(request_line, " ", parts: 3) do
          [method, path, _version] -> {:ok, method, path, parse_headers(header_lines)}
          _ -> :error
        end

      _ ->
        :error
    end
  end

  # ---------------------------------------------------------------------------
  # Request routing
  # ---------------------------------------------------------------------------

  @spec handle_request(:inet.socket(), module(), String.t(), String.t(), term(), map()) :: :ok
  defp handle_request(socket, transport, "GET", "/health/live", _peer, _headers) do
    send_response(socket, transport, 200, "OK", ~s({"status":"alive"}))
  end

  defp handle_request(socket, transport, "GET", "/health/ready", _peer, _headers) do
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

  defp handle_request(socket, transport, "GET", "/dashboard", peer, headers) do
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect()
      body = FerricstoreServer.Health.Dashboard.render(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_request(socket, transport, "GET", "/dashboard/slowlog", peer, headers) do
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect_slowlog_page()
      body = FerricstoreServer.Health.Dashboard.render_slowlog_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_request(socket, transport, "GET", "/dashboard/merge", peer, headers) do
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect_merge_page()
      body = FerricstoreServer.Health.Dashboard.render_merge_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_request(socket, transport, "GET", "/dashboard/config", peer, headers) do
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect_config_page()
      body = FerricstoreServer.Health.Dashboard.render_config_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_request(socket, transport, "GET", "/dashboard/raft", peer, headers) do
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      try do
        data = FerricstoreServer.Health.Dashboard.collect_raft_page()
        body = FerricstoreServer.Health.Dashboard.render_raft_page(data)
        send_html_response(socket, transport, 200, "OK", body)
      catch
        kind, reason ->
          body =
            "<html><body style='background:#0d1117;color:#f85149;padding:20px;font-family:monospace;'><h2>Raft Page Error</h2><pre>#{inspect(kind)}: #{inspect(reason, pretty: true, limit: 10)}</pre><a href='/dashboard' style='color:#58a6ff;'>← Dashboard</a></body></html>"

          send_html_response(socket, transport, 200, "OK", body)
      end
    end
  end

  defp handle_request(socket, transport, "GET", "/dashboard/clients", peer, headers) do
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect_clients_page()
      body = FerricstoreServer.Health.Dashboard.render_clients_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_request(socket, transport, "GET", "/dashboard/storage", peer, headers) do
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect_storage_page()
      body = FerricstoreServer.Health.Dashboard.render_storage_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_request(socket, transport, "GET", "/dashboard/prefixes", peer, headers) do
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      data = FerricstoreServer.Health.Dashboard.collect_prefixes_page()
      body = FerricstoreServer.Health.Dashboard.render_prefixes_page(data)
      send_html_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_request(socket, transport, "GET", "/metrics", peer, headers) do
    unless observability_authorized?(peer, headers) do
      send_response(socket, transport, 403, "Forbidden", ~s({"error":"forbidden"}))
    else
      body = Ferricstore.Metrics.scrape()
      send_text_response(socket, transport, 200, "OK", body)
    end
  end

  defp handle_request(socket, transport, "GET", _path, _peer, _headers) do
    send_response(socket, transport, 404, "Not Found", ~s({"error":"not found"}))
  end

  defp handle_request(socket, transport, _method, _path, _peer, _headers) do
    send_response(
      socket,
      transport,
      405,
      "Method Not Allowed",
      ~s({"error":"method not allowed"})
    )
  end

  @doc false
  @spec observability_authorized?(term(), map()) :: boolean()
  def observability_authorized?(peer, headers) do
    loopback_peer?(peer) or bearer_token_authorized?(headers)
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

  defp bearer_token_authorized?(headers) do
    token = Application.get_env(:ferricstore, :observability_token)

    is_binary(token) and token != "" and
      constant_time_equal?(Map.get(headers, "authorization"), "Bearer " <> token)
  end

  defp constant_time_equal?(left, right) when is_binary(left) and is_binary(right) do
    :crypto.hash(:sha256, left) == :crypto.hash(:sha256, right)
  end

  defp constant_time_equal?(_left, _right), do: false

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
end
