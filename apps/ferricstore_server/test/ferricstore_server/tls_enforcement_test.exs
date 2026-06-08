defmodule FerricstoreServer.TlsEnforcementTest do
  @moduledoc """
  Tests that the `require_tls` configuration flag correctly rejects
  plaintext TCP connections when set to `true`.

  These tests modify `Application` env temporarily and must run serially
  (async: false) to avoid interfering with other tests that use the same
  plaintext listener.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Acl
  alias FerricstoreServer.Listener

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp tcp_port, do: Listener.port()

  defp connect_tcp(port) do
    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw], 5_000)

    sock
  end

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp recv_response(sock) do
    recv_response(sock, "")
  end

  defp recv_response(sock, buf) do
    case :gen_tcp.recv(sock, 0, 5_000) do
      {:ok, data} ->
        buf2 = buf <> data

        case Parser.parse(buf2) do
          {:ok, [val], _rest} -> val
          {:ok, [], _} -> recv_response(sock, buf2)
        end

      {:error, :closed} ->
        if buf == "" do
          {:error, :closed}
        else
          case Parser.parse(buf) do
            {:ok, [val], _rest} -> val
            _ -> {:error, :closed}
          end
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Tests: require_tls = false (default) -- plaintext connections work
  # ---------------------------------------------------------------------------

  describe "when require_tls is false" do
    setup do
      Acl.reset!()

      # Ensure require_tls is false (the default)
      prev = Application.get_env(:ferricstore, :require_tls)
      Application.put_env(:ferricstore, :require_tls, false)

      on_exit(fn ->
        Acl.reset!()

        if prev do
          Application.put_env(:ferricstore, :require_tls, prev)
        else
          Application.delete_env(:ferricstore, :require_tls)
        end
      end)

      :ok
    end

    test "plaintext TCP connections are accepted" do
      port = tcp_port()
      sock = connect_tcp(port)

      send_cmd(sock, ["HELLO", "3"])
      greeting = recv_response(sock)

      assert is_map(greeting)
      assert greeting["server"] == "ferricstore"
      assert greeting["proto"] == 3

      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Tests: require_tls = true -- plaintext connections rejected
  # ---------------------------------------------------------------------------

  describe "when require_tls is true" do
    setup do
      Acl.reset!()

      prev = Application.get_env(:ferricstore, :require_tls)
      Application.put_env(:ferricstore, :require_tls, true)

      on_exit(fn ->
        Acl.reset!()

        if prev do
          Application.put_env(:ferricstore, :require_tls, prev)
        else
          Application.delete_env(:ferricstore, :require_tls)
        end
      end)

      :ok
    end

    test "plaintext TCP connections are rejected with TLS required error" do
      port = tcp_port()
      sock = connect_tcp(port)

      # The server should send an error and close the connection
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "TLS required"

      # The socket should be closed by the server
      assert :gen_tcp.recv(sock, 0, 1_000) == {:error, :closed}

      :gen_tcp.close(sock)
    end

    test "plaintext TCP connection is fully closed after rejection" do
      port = tcp_port()
      sock = connect_tcp(port)

      # Read the error response
      response = recv_response(sock)
      assert {:error, _msg} = response

      # The server closed its end -- recv should return :closed.
      # (TCP send may still succeed briefly due to OS buffering, so we
      # verify via recv which is the reliable indicator.)
      assert {:error, :closed} = :gen_tcp.recv(sock, 0, 1_000)

      :gen_tcp.close(sock)
    end
  end
end
