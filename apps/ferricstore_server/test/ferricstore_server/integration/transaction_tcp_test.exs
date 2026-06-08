defmodule FerricstoreServer.Integration.TransactionTcpTest do
  @moduledoc """
  End-to-end TCP integration tests for MULTI/EXEC/DISCARD/WATCH transactions.

  These tests verify the complete transaction flow over real TCP connections
  through the full stack:

      TCP -> RESP3 parser -> connection (transaction state) -> dispatcher -> router -> shard

  Each test uses unique key names to avoid cross-test interference.
  """

  use ExUnit.Case, async: false
  @moduletag :integration

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
    {:ok, data} = :gen_tcp.recv(sock, 0, 5000)
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

  # Hash tag ensures all generated keys co-locate on the same shard.
  defp ukey(name), do: "{tcp_txn}:#{name}_#{:rand.uniform(999_999)}"

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    %{port: Listener.port()}
  end

  # ---------------------------------------------------------------------------
  # MULTI/EXEC end-to-end
  # ---------------------------------------------------------------------------

  describe "MULTI/EXEC end-to-end over TCP" do
    test "basic MULTI/EXEC round-trip", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("e2e_1")
      k2 = ukey("e2e_2")

      # Setup: SET k1 before the transaction
      send_cmd(sock, ["SET", k1, "pre_existing"])
      assert recv_response(sock) == {:simple, "OK"}

      # Start transaction
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k2, "new_value"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["GET", k1])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["GET", k2])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["DEL", k1])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXISTS", k1])
      assert recv_response(sock) == {:simple, "QUEUED"}

      # Execute
      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert is_list(result)
      assert length(result) == 5
      # SET k2
      assert Enum.at(result, 0) == {:simple, "OK"}
      # GET k1 (was set before MULTI)
      assert Enum.at(result, 1) == "pre_existing"
      # GET k2 (set within this transaction, already executed by this point)
      assert Enum.at(result, 2) == "new_value"
      # DEL k1
      assert Enum.at(result, 3) == 1
      # EXISTS k1 (just deleted)
      assert Enum.at(result, 4) == 0

      # Verify side effects persisted
      send_cmd(sock, ["GET", k2])
      assert recv_response(sock) == "new_value"

      send_cmd(sock, ["GET", k1])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end

    test "transaction with MSET and MGET", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("e2e_mset_1")
      k2 = ukey("e2e_mset_2")

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["MSET", k1, "m1", k2, "m2"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["MGET", k1, k2])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert is_list(result)
      assert length(result) == 2
      assert Enum.at(result, 0) == {:simple, "OK"}
      assert Enum.at(result, 1) == ["m1", "m2"]

      :gen_tcp.close(sock)
    end

    test "DISCARD cancels queued commands", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("e2e_discard")

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "should_not_exist"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["DISCARD"])
      assert recv_response(sock) == {:simple, "OK"}

      # Key should not exist
      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end

    test "normal commands work after EXEC", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("e2e_after_exec")

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["EXEC"])
      assert recv_response(sock) == []

      # Normal commands should work
      send_cmd(sock, ["SET", k, "normal"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "normal"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # WATCH abort scenario over TCP
  # ---------------------------------------------------------------------------

  describe "WATCH abort scenario over TCP" do
    test "concurrent modification of watched key aborts transaction", %{port: port} do
      k = ukey("e2e_watch_abort")

      # Connection 1: set initial value and WATCH
      sock1 = connect_and_hello(port)
      send_cmd(sock1, ["SET", k, "initial"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["WATCH", k])
      assert recv_response(sock1) == {:simple, "OK"}

      # Connection 2: modify the watched key
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", k, "concurrent_write"])
      assert recv_response(sock2) == {:simple, "OK"}
      :gen_tcp.close(sock2)

      # Connection 1: try to MULTI/EXEC — should abort
      send_cmd(sock1, ["MULTI"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["SET", k, "would_be_overwritten"])
      assert recv_response(sock1) == {:simple, "QUEUED"}

      send_cmd(sock1, ["EXEC"])
      result = recv_response(sock1)
      assert result == nil

      # Verify the value is from the concurrent write, not the transaction
      send_cmd(sock1, ["GET", k])
      assert recv_response(sock1) == "concurrent_write"

      :gen_tcp.close(sock1)
    end

    test "WATCH without concurrent modification succeeds", %{port: port} do
      k = ukey("e2e_watch_ok")

      sock = connect_and_hello(port)
      send_cmd(sock, ["SET", k, "safe"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["WATCH", k])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "updated_safely"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert is_list(result)
      assert Enum.at(result, 0) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "updated_safely"

      :gen_tcp.close(sock)
    end

    test "WATCH + UNWATCH + modify + EXEC succeeds", %{port: port} do
      k = ukey("e2e_unwatch")

      sock1 = connect_and_hello(port)
      send_cmd(sock1, ["SET", k, "v1"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["WATCH", k])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["UNWATCH"])
      assert recv_response(sock1) == {:simple, "OK"}

      # Modify from another connection — should not matter since we unwatched
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", k, "v2"])
      assert recv_response(sock2) == {:simple, "OK"}
      :gen_tcp.close(sock2)

      send_cmd(sock1, ["MULTI"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["SET", k, "v3"])
      assert recv_response(sock1) == {:simple, "QUEUED"}

      send_cmd(sock1, ["EXEC"])
      result = recv_response(sock1)
      assert is_list(result)
      assert Enum.at(result, 0) == {:simple, "OK"}

      send_cmd(sock1, ["GET", k])
      assert recv_response(sock1) == "v3"

      :gen_tcp.close(sock1)
    end

    test "failed EXEC (aborted) clears transaction state", %{port: port} do
      k = ukey("e2e_abort_clear")

      sock1 = connect_and_hello(port)
      send_cmd(sock1, ["SET", k, "original"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["WATCH", k])
      assert recv_response(sock1) == {:simple, "OK"}

      # Cause a conflict
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", k, "conflict"])
      assert recv_response(sock2) == {:simple, "OK"}
      :gen_tcp.close(sock2)

      send_cmd(sock1, ["MULTI"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["SET", k, "txn"])
      assert recv_response(sock1) == {:simple, "QUEUED"}

      send_cmd(sock1, ["EXEC"])
      assert recv_response(sock1) == nil

      # After failed EXEC, we should be back in normal mode
      send_cmd(sock1, ["SET", k, "after_abort"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["GET", k])
      assert recv_response(sock1) == "after_abort"

      :gen_tcp.close(sock1)
    end
  end
end
