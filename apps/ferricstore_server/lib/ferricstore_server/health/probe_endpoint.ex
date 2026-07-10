defmodule FerricstoreServer.Health.ProbeEndpoint do
  @moduledoc """
  Isolated HTTP listener for liveness and readiness probes.

  Dashboard and metrics traffic remains on `FerricstoreServer.Health.Endpoint`.
  Keeping probes on a separate Ranch listener ensures incomplete dashboard
  requests cannot consume the probe listener's connection budget.
  """

  @behaviour :ranch_protocol

  alias FerricstoreServer.Connection.Send, as: ConnSend
  alias FerricstoreServer.Health.Endpoint.Probes
  alias FerricstoreServer.Health.Endpoint.Request

  @listener_ref :"#{__MODULE__}"
  @max_request_bytes 1_024
  @request_timeout_ms 250
  @ready_response_timeout_ms 200
  @readiness_timeout_response {503, "Service Unavailable", ~s({"status":"starting"})}

  @doc "Returns the TCP port used by the isolated health probe listener."
  @spec port() :: :inet.port_number()
  def port do
    :ranch.get_port(@listener_ref)
  end

  @doc false
  @spec ref() :: atom()
  def ref, do: @listener_ref

  @doc false
  @spec child_spec(:inet.port_number()) :: Supervisor.child_spec()
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
  @spec start_link(term(), module(), map()) :: {:ok, pid()}
  def start_link(ref, transport, opts) do
    pid = spawn_link(__MODULE__, :init, [ref, transport, opts])
    {:ok, pid}
  end

  @doc false
  @spec init(term(), module(), map()) :: :ok
  def init(ref, transport, opts) do
    {:ok, socket} = :ranch.handshake(ref)
    :ok = transport.setopts(socket, active: false)

    request_deadline = System.monotonic_time(:millisecond) + @request_timeout_ms

    case read_request(socket, transport, <<>>, request_deadline) do
      {:ok, "GET", "/health/live"} ->
        send_response(socket, transport, Probes.live_response())

      {:ok, "GET", "/health/ready"} ->
        send_response(socket, transport, ready_response(opts))

      {:ok, _method, _path} ->
        send_response(socket, transport, {404, "Not Found", ~s({"error":"not found"})})

      :error ->
        send_response(socket, transport, {400, "Bad Request", ~s({"error":"bad request"})})
    end

    transport.close(socket)
    :ok
  end

  defp ready_response(opts) do
    ready_response_fun = Map.get(opts, :ready_response_fun, &Probes.ready_response/0)
    timeout_ms = Map.get(opts, :ready_response_timeout_ms, @ready_response_timeout_ms)

    call_with_deadline(ready_response_fun, timeout_ms)
  end

  defp call_with_deadline(fun, timeout_ms)
       when is_function(fun, 0) and is_integer(timeout_ms) and timeout_ms >= 0 do
    caller = self()
    reply_ref = make_ref()
    deadline_ms = System.monotonic_time(:millisecond) + timeout_ms

    {worker, monitor_ref} =
      spawn_monitor(fn ->
        send(caller, {reply_ref, fun.()})
      end)

    remaining_ms = max(deadline_ms - System.monotonic_time(:millisecond), 0)

    receive do
      {^reply_ref, response} ->
        Process.demonitor(monitor_ref, [:flush])
        response

      {:DOWN, ^monitor_ref, :process, ^worker, _reason} ->
        @readiness_timeout_response
    after
      remaining_ms ->
        Process.exit(worker, :kill)
        Process.demonitor(monitor_ref, [:flush])
        @readiness_timeout_response
    end
  end

  defp read_request(socket, transport, data, deadline_ms) do
    remaining_ms = deadline_ms - System.monotonic_time(:millisecond)

    cond do
      remaining_ms <= 0 ->
        :error

      byte_size(data) > @max_request_bytes ->
        :error

      :binary.match(data, "\r\n\r\n") != :nomatch ->
        case Request.parse_request_line(data) do
          {:ok, method, path, _headers, _body} -> {:ok, method, path}
          :error -> :error
        end

      true ->
        case transport.recv(socket, 0, remaining_ms) do
          {:ok, chunk} -> read_request(socket, transport, data <> chunk, deadline_ms)
          {:error, _reason} -> :error
        end
    end
  end

  defp send_response(socket, transport, {status_code, status_text, body}) do
    response = [
      "HTTP/1.1 ",
      Integer.to_string(status_code),
      " ",
      status_text,
      "\r\n",
      "Content-Type: application/json\r\n",
      "Cache-Control: no-store\r\n",
      "X-Content-Type-Options: nosniff\r\n",
      "Content-Length: ",
      Integer.to_string(byte_size(body)),
      "\r\n",
      "Connection: close\r\n",
      "\r\n",
      body
    ]

    _result = ConnSend.send(socket, transport, response, :health_probe_response)
    :ok
  end
end
