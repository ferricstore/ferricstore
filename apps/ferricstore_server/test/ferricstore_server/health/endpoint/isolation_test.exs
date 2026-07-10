defmodule FerricstoreServer.Health.Endpoint.IsolationTest do
  use ExUnit.Case, async: false

  @moduletag :global_state

  alias FerricstoreServer.Health.Endpoint
  alias FerricstoreServer.Health.ProbeEndpoint

  test "dashboard connection exhaustion does not starve liveness or readiness probes" do
    listener = Endpoint.ref()
    previous_max_connections = :ranch.get_max_connections(listener)
    :ok = :ranch.set_max_connections(listener, 4)

    partial_connections =
      for _ <- 1..4 do
        {:ok, socket} =
          :gen_tcp.connect({127, 0, 0, 1}, Endpoint.port(), [
            :binary,
            active: false,
            packet: :raw
          ])

        :ok =
          :gen_tcp.send(
            socket,
            "GET /dashboard HTTP/1.1\r\nHost: localhost\r\n"
          )

        socket
      end

    on_exit(fn ->
      Enum.each(partial_connections, &:gen_tcp.close/1)
      :ok = :ranch.set_max_connections(listener, previous_max_connections)
      Ferricstore.Health.set_ready(true)
    end)

    assert eventually(fn ->
             listener
             |> :ranch.procs(:connections)
             |> length()
             |> Kernel.==(4)
           end)

    Ferricstore.Health.set_ready(true)
    refute Endpoint.port() == Endpoint.probe_port()

    for path <- ["/health/live", "/health/ready"] do
      started_at = System.monotonic_time(:millisecond)
      response = http_get(Endpoint.probe_port(), path, 500)
      elapsed_ms = System.monotonic_time(:millisecond) - started_at

      assert response =~ "HTTP/1.1 200 OK"
      assert elapsed_ms < 500
    end
  end

  test "isolated listener exposes only probes and preserves readiness status" do
    Ferricstore.Health.set_ready(false)
    on_exit(fn -> Ferricstore.Health.set_ready(true) end)

    ready_response = http_get(ProbeEndpoint.port(), "/health/ready", 1_500)
    dashboard_response = http_get(ProbeEndpoint.port(), "/dashboard", 500)

    assert ready_response =~ "HTTP/1.1 503 Service Unavailable"
    assert ready_response =~ ~s("status":"starting")
    assert dashboard_response =~ "HTTP/1.1 404 Not Found"
  end

  test "probe request deadline is not extended by trickled bytes" do
    {:ok, socket} =
      :gen_tcp.connect({127, 0, 0, 1}, Endpoint.probe_port(), [
        :binary,
        active: false,
        packet: :raw
      ])

    on_exit(fn -> :gen_tcp.close(socket) end)

    :ok = :gen_tcp.send(socket, "G")
    Process.sleep(160)
    :ok = :gen_tcp.send(socket, "E")
    Process.sleep(160)

    assert {:ok, response} = :gen_tcp.recv(socket, 0, 50)
    assert response =~ "HTTP/1.1 400 Bad Request"
  end

  test "blocked readiness checks time out and cannot starve liveness" do
    test_pid = self()
    call_ref = make_ref()

    blocking_ready_response = fn ->
      send(test_pid, {:readiness_started, call_ref, self()})
      Process.sleep(:infinity)
    end

    {listener, port} =
      start_probe_listener!(
        max_connections: 4,
        ready_response_fun: blocking_ready_response,
        ready_response_timeout_ms: 100
      )

    on_exit(fn -> :ranch.stop_listener(listener) end)

    readiness_requests =
      for _ <- 1..4 do
        Task.async(fn -> http_get(port, "/health/ready", 500) end)
      end

    readiness_pids =
      for _ <- 1..4 do
        assert_receive {:readiness_started, ^call_ref, pid}, 500
        pid
      end

    liveness_response = http_get(port, "/health/live", 500)

    assert liveness_response =~ "HTTP/1.1 200 OK"
    assert liveness_response =~ ~s("status":"alive")

    Enum.each(readiness_requests, fn request ->
      response = Task.await(request, 500)
      assert response =~ "HTTP/1.1 503 Service Unavailable"
      assert response =~ ~s("status":"starting")
    end)

    assert eventually(fn -> Enum.all?(readiness_pids, &(not Process.alive?(&1))) end)
  end

  defp http_get(port, path, timeout_ms) do
    {:ok, socket} =
      :gen_tcp.connect(
        {127, 0, 0, 1},
        port,
        [:binary, active: false, packet: :raw],
        timeout_ms
      )

    :ok =
      :gen_tcp.send(
        socket,
        "GET #{path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
      )

    response = recv_all(socket, "", timeout_ms)
    :gen_tcp.close(socket)
    response
  end

  defp start_probe_listener!(opts) do
    listener = {__MODULE__, make_ref()}
    max_connections = Keyword.fetch!(opts, :max_connections)
    protocol_opts = Map.new(Keyword.delete(opts, :max_connections))

    transport_opts = %{
      socket_opts: [port: 0],
      num_acceptors: 1,
      max_connections: max_connections
    }

    {:ok, _pid} =
      :ranch.start_listener(
        listener,
        :ranch_tcp,
        transport_opts,
        ProbeEndpoint,
        protocol_opts
      )

    {listener, :ranch.get_port(listener)}
  end

  defp recv_all(socket, acc, timeout_ms) do
    case :gen_tcp.recv(socket, 0, timeout_ms) do
      {:ok, data} -> recv_all(socket, acc <> data, timeout_ms)
      {:error, :closed} -> acc
      {:error, reason} -> flunk("health probe failed: #{inspect(reason)}")
    end
  end

  defp eventually(fun, attempts \\ 50)

  defp eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      true
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end

  defp eventually(_fun, 0), do: false
end
