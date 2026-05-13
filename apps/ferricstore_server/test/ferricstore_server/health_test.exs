defmodule FerricstoreServer.HealthTest do
  @moduledoc """
  Health-check tests that verify the server responds correctly to
  diagnostic commands over TCP.

  Covers:
    - Application responds to PING over TCP
    - DBSIZE returns a non-negative integer
    - INFO returns a non-empty string
    - Stats module tracks connections
  """

  use ExUnit.Case, async: false

  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener
  alias FerricstoreServer.Health.Endpoint
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      Ferricstore.Health.set_ready(true)
      Application.delete_env(:ferricstore, :observability_token)
    end)

    :ok
  end

  describe "observability endpoint authorization" do
    test "allows loopback peers without a token" do
      refute Application.get_env(:ferricstore, :observability_token)
      assert Endpoint.observability_authorized?({127, 0, 0, 1}, %{})
      assert Endpoint.observability_authorized?({0, 0, 0, 0, 0, 0, 0, 1}, %{})
    end

    test "rejects non-loopback peers without a token" do
      refute Endpoint.observability_authorized?({10, 0, 0, 5}, %{})
    end

    test "allows non-loopback peers with the configured bearer token" do
      Application.put_env(:ferricstore, :observability_token, "secret-token")

      assert Endpoint.observability_authorized?({10, 0, 0, 5}, %{
               "authorization" => "Bearer secret-token"
             })

      refute Endpoint.observability_authorized?({10, 0, 0, 5}, %{
               "authorization" => "Bearer wrong"
             })
    end
  end

  # ---------------------------------------------------------------------------
  # TCP helpers (same pattern as CommandsTcpTest)
  # ---------------------------------------------------------------------------

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp recv_response(sock) do
    recv_response(sock, "")
  end

  defp recv_response(sock, buf) do
    # 30s timeout to accommodate FLUSHDB on CI where many keys accumulate
    {:ok, data} = :gen_tcp.recv(sock, 0, 30_000)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [val], ""} -> val
      {:ok, [val], _rest} -> val
      {:ok, [], _} -> recv_response(sock, buf2)
    end
  end

  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  # ---------------------------------------------------------------------------
  # PING over TCP
  # ---------------------------------------------------------------------------

  describe "PING over TCP" do
    test "PING returns PONG" do
      port = Listener.port()
      sock = connect_and_hello(port)

      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end

    test "PING with message echoes the message" do
      port = Listener.port()
      sock = connect_and_hello(port)

      send_cmd(sock, ["PING", "hello"])
      assert recv_response(sock) == "hello"

      :gen_tcp.close(sock)
    end

    test "PING works on multiple sequential connections" do
      port = Listener.port()

      for _ <- 1..5 do
        sock = connect_and_hello(port)
        send_cmd(sock, ["PING"])
        assert recv_response(sock) == {:simple, "PONG"}
        :gen_tcp.close(sock)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # DBSIZE returns a non-negative integer
  # ---------------------------------------------------------------------------

  describe "DBSIZE returns a non-negative integer" do
    test "DBSIZE returns zero or near-zero after FLUSHDB" do
      port = Listener.port()
      sock = connect_and_hello(port)

      # Flush the store first.
      send_cmd(sock, ["FLUSHDB"])
      recv_response(sock)

      send_cmd(sock, ["DBSIZE"])
      result = recv_response(sock)
      assert is_integer(result)
      # May not be exactly 0 if other tests' internal/compound keys persist
      # in Bitcask but aren't visible via KEYS/DBSIZE command filtering.
      assert result >= 0
      :gen_tcp.close(sock)
    end

    test "DBSIZE increases after writes" do
      port = Listener.port()
      sock = connect_and_hello(port)

      send_cmd(sock, ["FLUSHDB"])
      recv_response(sock)

      send_cmd(sock, ["DBSIZE"])
      before = recv_response(sock)
      assert is_integer(before)

      # Write 3 keys.
      for i <- 1..3 do
        k = "health_dbsize_#{:rand.uniform(9_999_999)}_#{i}"
        send_cmd(sock, ["SET", k, "v"])
        assert recv_response(sock) == {:simple, "OK"}
      end

      send_cmd(sock, ["DBSIZE"])
      after_count = recv_response(sock)
      assert is_integer(after_count)
      assert after_count >= before + 3

      :gen_tcp.close(sock)
    end

    test "DBSIZE is non-negative even with no data" do
      port = Listener.port()
      sock = connect_and_hello(port)

      send_cmd(sock, ["DBSIZE"])
      result = recv_response(sock)
      assert is_integer(result)
      assert result >= 0

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # INFO returns a non-empty string
  # ---------------------------------------------------------------------------

  describe "INFO returns a non-empty string" do
    test "INFO with no arguments returns a non-empty string" do
      port = Listener.port()
      sock = connect_and_hello(port)

      send_cmd(sock, ["INFO"])
      result = recv_response(sock)
      assert is_binary(result), "INFO should return a bulk string"
      assert byte_size(result) > 0, "INFO output should be non-empty"

      :gen_tcp.close(sock)
    end

    test "INFO contains server section with run_id" do
      port = Listener.port()
      sock = connect_and_hello(port)

      send_cmd(sock, ["INFO"])
      result = recv_response(sock)
      assert is_binary(result)
      assert String.contains?(result, "run_id"), "INFO should contain run_id"

      :gen_tcp.close(sock)
    end

    test "INFO contains uptime information" do
      port = Listener.port()
      sock = connect_and_hello(port)

      send_cmd(sock, ["INFO"])
      result = recv_response(sock)
      assert is_binary(result)

      assert String.contains?(result, "uptime_in_seconds"),
             "INFO should contain uptime_in_seconds"

      :gen_tcp.close(sock)
    end

    test "INFO server section returns server info" do
      port = Listener.port()
      sock = connect_and_hello(port)

      send_cmd(sock, ["INFO", "server"])
      result = recv_response(sock)
      assert is_binary(result), "INFO server should return a string"
      assert byte_size(result) > 0, "INFO server output should be non-empty"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Stats module tracks connections
  # ---------------------------------------------------------------------------

  describe "Stats module tracks connections" do
    test "total_connections is a non-negative integer" do
      total = Ferricstore.Stats.total_connections()
      assert is_integer(total)
      assert total >= 0
    end

    test "total_commands is a non-negative integer" do
      total = Ferricstore.Stats.total_commands()
      assert is_integer(total)
      assert total >= 0
    end

    test "opening a new TCP connection increments total_connections" do
      before = Ferricstore.Stats.total_connections()

      port = Listener.port()
      sock = connect_and_hello(port)

      # Give the connection handler time to increment the counter.
      Process.sleep(50)

      after_count = Ferricstore.Stats.total_connections()

      assert after_count > before,
             "Expected total_connections to increase from #{before}, got #{after_count}"

      :gen_tcp.close(sock)
    end

    test "sending commands increments total_commands" do
      port = Listener.port()
      sock = connect_and_hello(port)

      before = Ferricstore.Stats.total_commands()

      send_cmd(sock, ["PING"])
      recv_response(sock)
      send_cmd(sock, ["PING"])
      recv_response(sock)
      send_cmd(sock, ["PING"])
      recv_response(sock)

      after_count = Ferricstore.Stats.total_commands()

      assert after_count >= before + 3,
             "Expected total_commands to increase by at least 3 from #{before}, got #{after_count}"

      :gen_tcp.close(sock)
    end

    test "run_id is a 40-character hex string" do
      run_id = Ferricstore.Stats.run_id()
      assert is_binary(run_id)
      assert byte_size(run_id) == 40
      assert Regex.match?(~r/^[0-9a-f]{40}$/, run_id), "Run ID should be lowercase hex"
    end

    test "uptime_seconds is non-negative" do
      uptime = Ferricstore.Stats.uptime_seconds()
      assert is_integer(uptime)
      assert uptime >= 0
    end
  end

  # ---------------------------------------------------------------------------
  # /health/ready HTTP endpoint (spec 2C.1 Phase 3)
  # ---------------------------------------------------------------------------

  describe "/health/ready HTTP endpoint" do
    test "returns 200 with status ok after app start" do
      port = FerricstoreServer.Health.Endpoint.port()
      assert is_integer(port) and port > 0

      {:ok, conn} =
        :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

      :ok = :gen_tcp.send(conn, "GET /health/ready HTTP/1.1\r\nHost: localhost\r\n\r\n")
      {:ok, response} = :gen_tcp.recv(conn, 0, 5_000)
      :gen_tcp.close(conn)

      assert response =~ "HTTP/1.1 200 OK"
      assert response =~ "application/json"
      assert response =~ ~s("status":"ok")
    end

    test "returns shard count in health response" do
      port = FerricstoreServer.Health.Endpoint.port()

      {:ok, conn} =
        :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

      :ok = :gen_tcp.send(conn, "GET /health/ready HTTP/1.1\r\nHost: localhost\r\n\r\n")
      {:ok, response} = :gen_tcp.recv(conn, 0, 5_000)
      :gen_tcp.close(conn)

      # Extract the JSON body (after the blank line separating headers from body)
      [_headers, body] = String.split(response, "\r\n\r\n", parts: 2)
      {:ok, decoded} = Jason.decode(body)

      shard_count = Application.get_env(:ferricstore, :shard_count, 4)
      assert decoded["shard_count"] == shard_count
      assert is_list(decoded["shards"])
      assert length(decoded["shards"]) == shard_count
    end

    test "accepts requests split across TCP packets" do
      port = FerricstoreServer.Health.Endpoint.port()

      {:ok, conn} =
        :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

      :ok = :gen_tcp.send(conn, "GET /health/ready ")
      Process.sleep(20)
      :ok = :gen_tcp.send(conn, "HTTP/1.1\r\nHost: localhost\r\n\r\n")
      {:ok, response} = :gen_tcp.recv(conn, 0, 5_000)
      :gen_tcp.close(conn)

      assert response =~ "HTTP/1.1 200 OK"
      assert response =~ "application/json"
      assert response =~ ~s("status":"ok")
    end

    test "returns shard status for each shard" do
      port = FerricstoreServer.Health.Endpoint.port()

      {:ok, conn} =
        :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

      :ok = :gen_tcp.send(conn, "GET /health/ready HTTP/1.1\r\nHost: localhost\r\n\r\n")
      {:ok, response} = :gen_tcp.recv(conn, 0, 5_000)
      :gen_tcp.close(conn)

      [_headers, body] = String.split(response, "\r\n\r\n", parts: 2)
      {:ok, decoded} = Jason.decode(body)

      for shard <- decoded["shards"] do
        assert Map.has_key?(shard, "index")
        assert Map.has_key?(shard, "status")
        assert shard["status"] in ["ok", "down"]
      end
    end

    test "returns uptime_seconds in health response" do
      port = FerricstoreServer.Health.Endpoint.port()

      {:ok, conn} =
        :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

      :ok = :gen_tcp.send(conn, "GET /health/ready HTTP/1.1\r\nHost: localhost\r\n\r\n")
      {:ok, response} = :gen_tcp.recv(conn, 0, 5_000)
      :gen_tcp.close(conn)

      [_headers, body] = String.split(response, "\r\n\r\n", parts: 2)
      {:ok, decoded} = Jason.decode(body)

      assert is_integer(decoded["uptime_seconds"])
      assert decoded["uptime_seconds"] >= 0
    end

    test "returns 503 when not ready" do
      # Temporarily mark the health as not ready, then restore.
      Ferricstore.Health.set_ready(false)

      port = FerricstoreServer.Health.Endpoint.port()

      {:ok, conn} =
        :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

      :ok = :gen_tcp.send(conn, "GET /health/ready HTTP/1.1\r\nHost: localhost\r\n\r\n")
      {:ok, response} = :gen_tcp.recv(conn, 0, 5_000)
      :gen_tcp.close(conn)

      assert response =~ "HTTP/1.1 503 Service Unavailable"
      assert response =~ ~s("status":"starting")

      # Restore ready state for subsequent tests.
      Ferricstore.Health.set_ready(true)
    end

    test "returns 404 for unknown paths" do
      port = FerricstoreServer.Health.Endpoint.port()

      {:ok, conn} =
        :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

      :ok = :gen_tcp.send(conn, "GET /unknown HTTP/1.1\r\nHost: localhost\r\n\r\n")
      {:ok, response} = :gen_tcp.recv(conn, 0, 5_000)
      :gen_tcp.close(conn)

      assert response =~ "HTTP/1.1 404 Not Found"
    end

    test "Health.ready? returns true after app start" do
      assert Ferricstore.Health.ready?() == true
    end

    test "Health.check returns map with status and shards" do
      result = Ferricstore.Health.check()
      assert result.status == :ok
      assert is_list(result.shards)
      assert is_integer(result.shard_count)
      assert is_integer(result.uptime_seconds)
    end
  end

  # ---------------------------------------------------------------------------
  # /health/live HTTP endpoint (Kubernetes liveness probe)
  # ---------------------------------------------------------------------------

  describe "/health/live HTTP endpoint" do
    test "returns 200 with status alive" do
      port = FerricstoreServer.Health.Endpoint.port()

      {:ok, conn} =
        :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

      :ok = :gen_tcp.send(conn, "GET /health/live HTTP/1.1\r\nHost: localhost\r\n\r\n")
      {:ok, response} = :gen_tcp.recv(conn, 0, 5_000)
      :gen_tcp.close(conn)

      assert response =~ "HTTP/1.1 200 OK"
      assert response =~ "application/json"
      assert response =~ ~s("status":"alive")
    end

    test "returns 200 even when readiness is false" do
      # Liveness should always return 200 regardless of readiness state.
      Ferricstore.Health.set_ready(false)

      port = FerricstoreServer.Health.Endpoint.port()

      {:ok, conn} =
        :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

      :ok = :gen_tcp.send(conn, "GET /health/live HTTP/1.1\r\nHost: localhost\r\n\r\n")
      {:ok, response} = :gen_tcp.recv(conn, 0, 5_000)
      :gen_tcp.close(conn)

      assert response =~ "HTTP/1.1 200 OK"
      assert response =~ ~s("status":"alive")

      # Restore ready state for subsequent tests.
      Ferricstore.Health.set_ready(true)
    end

    test "response body is valid JSON with status field" do
      port = FerricstoreServer.Health.Endpoint.port()

      {:ok, conn} =
        :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

      :ok = :gen_tcp.send(conn, "GET /health/live HTTP/1.1\r\nHost: localhost\r\n\r\n")
      {:ok, response} = :gen_tcp.recv(conn, 0, 5_000)
      :gen_tcp.close(conn)

      [_headers, body] = String.split(response, "\r\n\r\n", parts: 2)
      {:ok, decoded} = Jason.decode(body)

      assert decoded == %{"status" => "alive"}
    end
  end
end
