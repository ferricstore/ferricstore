Code.require_file("commands_tcp_test/sections/part_01.exs", __DIR__)
Code.require_file("commands_tcp_test/sections/part_02.exs", __DIR__)

defmodule FerricstoreServer.Integration.CommandsTcpTest do
  @moduledoc """
  End-to-end TCP integration tests for FerricStore Redis commands.

  These tests connect over a real TCP socket, send RESP3-encoded commands, and
  verify responses through the full stack:

      TCP → RESP3 parser → dispatcher → router → shard → Bitcask NIF

  A single Ranch TCP listener is started on an ephemeral port in `setup_all`
  and shared across all tests. Each test uses unique key names (via `ukey/1`)
  to avoid cross-test interference, except for FLUSHDB tests which explicitly
  clear the store and are grouped at the end.
  """

  use ExUnit.Case, async: false

  alias FerricstoreServer.Resp.Encoder
  alias FerricstoreServer.Resp.Parser
  alias FerricstoreServer.Listener

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
    # 30s timeout to accommodate FLUSHDB on CI where many keys accumulate
    {:ok, data} = :gen_tcp.recv(sock, 0, 30_000)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [val], ""} -> val
      {:ok, [val], _rest} -> val
      {:ok, [], _} -> recv_response(sock, buf2)
    end
  end

  # Receives exactly `n` RESP3 responses from the socket, accumulating
  # partial TCP reads as needed. Handles responses arriving in any number
  # of TCP segments (including one-per-segment from the sliding window).
  defp recv_n(sock, n) do
    do_recv_n(sock, n, "", [])
  end

  defp do_recv_n(_sock, 0, _buf, acc), do: acc

  defp do_recv_n(sock, remaining, buf, acc) when remaining > 0 do
    {:ok, data} = :gen_tcp.recv(sock, 0, 30_000)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [_ | _] = vals, rest} ->
        taken = Enum.take(vals, remaining)
        new_acc = acc ++ taken
        new_remaining = remaining - length(taken)
        do_recv_n(sock, new_remaining, rest, new_acc)

      {:ok, [], _} ->
        do_recv_n(sock, remaining, buf2, acc)
    end
  end

  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  # Generates a unique key name to avoid cross-test interference.
  defp ukey(name), do: "#{name}_#{:rand.uniform(999_999)}"

  # ---------------------------------------------------------------------------
  # Setup — single listener for all tests
  # ---------------------------------------------------------------------------

  setup_all do
    # The application supervisor already starts the Ranch listener.
    # Discover the actual bound port (ephemeral in test env).
    %{port: Listener.port()}
  end

  # Flush all keys before each test to keep the keydir small.
  # A growing keydir makes KEYS/DBSIZE calls progressively slower
  # and can cause GenServer timeouts in later tests.
  setup %{port: port} do
    sock = connect_and_hello(port)
    send_cmd(sock, ["FLUSHDB"])
    recv_response(sock)
    :gen_tcp.close(sock)
    :ok
  end

  # ---------------------------------------------------------------------------
  # SET and GET over TCP
  # ---------------------------------------------------------------------------

  use FerricstoreServer.Integration.CommandsTcpTest.Sections.Part01

  use FerricstoreServer.Integration.CommandsTcpTest.Sections.Part02
end
