defmodule FerricstoreServer.Commands.AuthGatingTest do
  @moduledoc """
  Tests for authentication gating in the connection handler.

  Verifies that when `requirepass` is configured, all commands except
  AUTH, HELLO, QUIT, and RESET are rejected with NOAUTH until the client
  authenticates.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Config
  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener

  # Reset requirepass after each test to avoid contaminating other tests.
  setup do
    on_exit(fn -> Config.set("requirepass", "") end)
    %{port: Listener.port()}
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp recv_response(sock) do
    recv_response(sock, "")
  end

  defp recv_response(sock, buf) do
    {:ok, data} = :gen_tcp.recv(sock, 0, 5000)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [val], ""} -> val
      {:ok, [val], _rest} -> val
      {:ok, [], _} -> recv_response(sock, buf2)
    end
  end

  defp connect_raw(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    sock
  end

  defp connect_and_hello(port) do
    sock = connect_raw(port)
    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  # ---------------------------------------------------------------------------
  # With requirepass set: commands rejected before AUTH
  # ---------------------------------------------------------------------------

  test "existing unauthenticated socket is gated when requirepass is enabled", %{port: port} do
    sock = connect_and_hello(port)

    send_cmd(sock, ["PING"])
    assert {:simple, "PONG"} = recv_response(sock)

    Config.set("requirepass", "secret123")

    send_cmd(sock, ["PING"])
    assert {:error, msg} = recv_response(sock)
    assert msg =~ "NOAUTH"

    send_cmd(sock, ["AUTH", "secret123"])
    assert {:simple, "OK"} = recv_response(sock)

    send_cmd(sock, ["PING"])
    assert {:simple, "PONG"} = recv_response(sock)

    :gen_tcp.close(sock)
  end

  describe "with requirepass set, GET returns NOAUTH before AUTH" do
    setup %{port: port} do
      Config.set("requirepass", "secret123")
      %{port: port}
    end

    test "GET is rejected before authentication", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["GET", "key"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "NOAUTH"

      :gen_tcp.close(sock)
    end

    test "SET is rejected before authentication", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["SET", "key", "value"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "NOAUTH"

      :gen_tcp.close(sock)
    end

    test "PING is rejected before authentication", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["PING"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "NOAUTH"

      :gen_tcp.close(sock)
    end

    test "DEL is rejected before authentication", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["DEL", "key"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "NOAUTH"

      :gen_tcp.close(sock)
    end

    test "DBSIZE is rejected before authentication", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["DBSIZE"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "NOAUTH"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # AUTH with correct password allows commands
  # ---------------------------------------------------------------------------

  describe "AUTH with correct password allows commands" do
    setup %{port: port} do
      Config.set("requirepass", "secret123")
      %{port: port}
    end

    test "commands work after successful AUTH", %{port: port} do
      sock = connect_and_hello(port)

      # Authenticate.
      send_cmd(sock, ["AUTH", "secret123"])
      assert recv_response(sock) == {:simple, "OK"}

      # Now commands should work.
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      send_cmd(sock, ["SET", "auth_gating_test_key", "value"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", "auth_gating_test_key"])
      assert recv_response(sock) == "value"

      :gen_tcp.close(sock)
    end

    test "AUTH with username and correct password works", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", "default", "secret123"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # HELLO is allowed before AUTH
  # ---------------------------------------------------------------------------

  describe "HELLO is allowed before AUTH" do
    setup %{port: port} do
      Config.set("requirepass", "secret123")
      %{port: port}
    end

    test "HELLO returns server info even when not authenticated", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["HELLO", "3"])
      response = recv_response(sock)

      assert is_map(response)
      assert response["server"] == "ferricstore"

      :gen_tcp.close(sock)
    end

    test "HELLO without version is allowed", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["HELLO"])
      response = recv_response(sock)

      assert is_map(response)

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # QUIT is allowed before AUTH
  # ---------------------------------------------------------------------------

  describe "QUIT is allowed before AUTH" do
    setup %{port: port} do
      Config.set("requirepass", "secret123")
      %{port: port}
    end

    test "QUIT returns OK and closes connection", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["QUIT"])
      response = recv_response(sock)

      assert response == {:simple, "OK"}
      # Socket should be closed by the server.
    end
  end

  # ---------------------------------------------------------------------------
  # RESET is allowed before AUTH
  # ---------------------------------------------------------------------------

  describe "RESET is allowed before AUTH" do
    setup %{port: port} do
      Config.set("requirepass", "secret123")
      %{port: port}
    end

    test "RESET returns RESET even when not authenticated", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["RESET"])
      response = recv_response(sock)

      assert response == {:simple, "RESET"}

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Without requirepass, commands work without AUTH
  # ---------------------------------------------------------------------------

  describe "without requirepass, commands work without AUTH" do
    test "PING works without authentication when no password set", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end

    test "SET/GET works without authentication when no password set", %{port: port} do
      sock = connect_and_hello(port)
      key = "auth_gating_nopass_#{:rand.uniform(9_999_999)}"

      send_cmd(sock, ["SET", key, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == "v"

      :gen_tcp.close(sock)
    end

    test "AUTH returns error when no password is configured", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", "somepassword"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "no password is set"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # After AUTH, commands work normally
  # ---------------------------------------------------------------------------

  describe "after AUTH, commands work normally" do
    setup %{port: port} do
      Config.set("requirepass", "mypass")
      %{port: port}
    end

    test "full workflow: NOAUTH -> AUTH -> commands work", %{port: port} do
      sock = connect_and_hello(port)

      # Should fail before auth.
      send_cmd(sock, ["PING"])
      assert {:error, noauth_msg} = recv_response(sock)
      assert noauth_msg =~ "NOAUTH"

      # Auth.
      send_cmd(sock, ["AUTH", "mypass"])
      assert {:simple, "OK"} = recv_response(sock)

      # Now should work.
      send_cmd(sock, ["PING"])
      assert {:simple, "PONG"} = recv_response(sock)

      key = "auth_workflow_#{:rand.uniform(9_999_999)}"
      send_cmd(sock, ["SET", key, "hello"])
      assert {:simple, "OK"} = recv_response(sock)

      send_cmd(sock, ["GET", key])
      assert "hello" = recv_response(sock)

      send_cmd(sock, ["DEL", key])
      assert 1 = recv_response(sock)

      :gen_tcp.close(sock)
    end

    test "wrong password rejected, correct password accepted", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", "wrong"])
      assert {:error, msg} = recv_response(sock)
      assert msg =~ "WRONGPASS"

      # Still not authenticated.
      send_cmd(sock, ["PING"])
      assert {:error, noauth_msg} = recv_response(sock)
      assert noauth_msg =~ "NOAUTH"

      # Now authenticate properly.
      send_cmd(sock, ["AUTH", "mypass"])
      assert {:simple, "OK"} = recv_response(sock)

      send_cmd(sock, ["PING"])
      assert {:simple, "PONG"} = recv_response(sock)

      :gen_tcp.close(sock)
    end
  end
end
