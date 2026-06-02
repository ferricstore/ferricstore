defmodule FerricstoreServer.Commands.TransactionTest do
  @moduledoc """
  Tests for MULTI/EXEC/DISCARD/WATCH/UNWATCH transaction commands.

  Transactions are connection-level state, so these tests connect over TCP
  and exercise the full command pipeline. Each test opens a fresh connection
  to avoid cross-test interference with transaction state.
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

  # Hash tag ensures all generated keys co-locate on the same shard,
  # avoiding CROSSSLOT errors in MULTI/EXEC.
  defp ukey(name), do: "{txn}:#{name}_#{:rand.uniform(999_999)}"

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    %{port: Listener.port()}
  end

  # ---------------------------------------------------------------------------
  # MULTI basics
  # ---------------------------------------------------------------------------

  describe "MULTI" do
    test "MULTI returns OK", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      # Clean up — DISCARD to exit MULTI mode
      send_cmd(sock, ["DISCARD"])
      _discard = recv_response(sock)

      :gen_tcp.close(sock)
    end

    test "MULTI inside MULTI returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["MULTI"])
      response = recv_response(sock)
      assert match?({:error, "ERR MULTI calls can not be nested"}, response)

      send_cmd(sock, ["DISCARD"])
      _discard = recv_response(sock)

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Queuing behavior
  # ---------------------------------------------------------------------------

  describe "queuing behavior" do
    test "commands after MULTI return QUEUED", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("queued")

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "hello"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["DISCARD"])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
    end

    test "invalid command during MULTI returns error but stays in MULTI", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      # GET with no arguments is an arity error
      send_cmd(sock, ["GET"])
      response = recv_response(sock)
      assert match?({:error, "ERR wrong number of arguments" <> _}, response)

      # We should still be in MULTI mode — a valid command should still queue
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["DISCARD"])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
    end

    test "unknown command during MULTI returns error but stays in MULTI", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["BADCMD", "arg1"])
      response = recv_response(sock)
      assert match?({:error, "ERR unknown command" <> _}, response)

      # Still in MULTI mode
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["DISCARD"])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
    end

    test "compound write commands queue without executing validation handlers", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("hset_queue")

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["HSET", k, "field", "value"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert [1] = result

      send_cmd(sock, ["HGET", k, "field"])
      assert recv_response(sock) == "value"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # EXEC
  # ---------------------------------------------------------------------------

  describe "EXEC" do
    test "EXEC executes all queued commands and returns array of results", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("exec_basic")

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "hello"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert is_list(result)
      assert length(result) == 2
      assert Enum.at(result, 0) == {:simple, "OK"}
      assert Enum.at(result, 1) == "hello"

      :gen_tcp.close(sock)
    end

    test "EXEC aborts after queue-time syntax error and skips queued writes", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("execabort")

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "should_not_write"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["GET"])
      assert {:error, "ERR wrong number of arguments" <> _} = recv_response(sock)

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      assert {:error, "EXECABORT" <> _} = recv_response(sock)

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end

    test "EXEC without MULTI returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["EXEC"])
      response = recv_response(sock)
      assert match?({:error, "ERR EXEC without MULTI"}, response)

      :gen_tcp.close(sock)
    end

    test "empty EXEC (no commands queued) returns empty array", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert result == []

      :gen_tcp.close(sock)
    end

    test "EXEC clears transaction state (can issue normal commands after)", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("exec_clear")

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "val"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      _result = recv_response(sock)

      # Normal command should work, not return QUEUED
      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "val"

      :gen_tcp.close(sock)
    end

    test "SET then GET in same transaction returns correct results", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("exec_set_get")

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "txn_value"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert is_list(result)
      assert length(result) == 3
      assert Enum.at(result, 0) == {:simple, "OK"}
      assert Enum.at(result, 1) == "txn_value"
      assert Enum.at(result, 2) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end

    test "multiple SET/GET operations in transaction", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("exec_multi_1")
      k2 = ukey("exec_multi_2")

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k1, "aaa"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["SET", k2, "bbb"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["GET", k1])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["GET", k2])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert is_list(result)
      assert length(result) == 4
      assert Enum.at(result, 0) == {:simple, "OK"}
      assert Enum.at(result, 1) == {:simple, "OK"}
      assert Enum.at(result, 2) == "aaa"
      assert Enum.at(result, 3) == "bbb"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # DISCARD
  # ---------------------------------------------------------------------------

  describe "DISCARD" do
    test "DISCARD clears queue and returns OK", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("discard_basic")

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "should_not_persist"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["DISCARD"])
      assert recv_response(sock) == {:simple, "OK"}

      # The SET should not have been executed
      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end

    test "DISCARD without MULTI returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["DISCARD"])
      response = recv_response(sock)
      assert match?({:error, "ERR DISCARD without MULTI"}, response)

      :gen_tcp.close(sock)
    end

    test "DISCARD clears watched keys", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("discard_watch")

      # Set the key first
      send_cmd(sock, ["SET", k, "original"])
      assert recv_response(sock) == {:simple, "OK"}

      # WATCH then MULTI then DISCARD should clear watches
      send_cmd(sock, ["WATCH", k])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["DISCARD"])
      assert recv_response(sock) == {:simple, "OK"}

      # After DISCARD, a new MULTI/EXEC should succeed even if the key was modified
      # (because watches were cleared by DISCARD)
      # Modify the key from another connection
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", k, "modified"])
      assert recv_response(sock2) == {:simple, "OK"}
      :gen_tcp.close(sock2)

      # New transaction should succeed (no watches)
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert is_list(result)
      assert Enum.at(result, 0) == "modified"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # WATCH
  # ---------------------------------------------------------------------------

  describe "WATCH" do
    test "WATCH + no modify + EXEC succeeds", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("watch_nomod")

      # Set initial value
      send_cmd(sock, ["SET", k, "original"])
      assert recv_response(sock) == {:simple, "OK"}

      # WATCH the key
      send_cmd(sock, ["WATCH", k])
      assert recv_response(sock) == {:simple, "OK"}

      # MULTI/EXEC without any external modification should succeed
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "updated"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert is_list(result)
      assert Enum.at(result, 0) == {:simple, "OK"}

      # Verify the SET actually executed
      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "updated"

      :gen_tcp.close(sock)
    end

    test "WATCH + modify + EXEC returns nil (abort)", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("watch_mod")

      # Set initial value
      send_cmd(sock, ["SET", k, "original"])
      assert recv_response(sock) == {:simple, "OK"}

      # WATCH the key
      send_cmd(sock, ["WATCH", k])
      assert recv_response(sock) == {:simple, "OK"}

      # Modify the key from another connection
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", k, "modified_by_other"])
      assert recv_response(sock2) == {:simple, "OK"}
      :gen_tcp.close(sock2)

      # MULTI/EXEC should abort (return nil)
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "would_be_overwritten"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert result == nil

      # The queued SET should NOT have executed
      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "modified_by_other"

      :gen_tcp.close(sock)
    end

    test "WATCH multiple keys — abort if any one changes", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("watch_multi_1")
      k2 = ukey("watch_multi_2")

      send_cmd(sock, ["SET", k1, "a"])
      assert recv_response(sock) == {:simple, "OK"}
      send_cmd(sock, ["SET", k2, "b"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["WATCH", k1, k2])
      assert recv_response(sock) == {:simple, "OK"}

      # Modify only k2 from another connection
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", k2, "b_modified"])
      assert recv_response(sock2) == {:simple, "OK"}
      :gen_tcp.close(sock2)

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k1])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert result == nil

      :gen_tcp.close(sock)
    end

    test "WATCH inside MULTI returns error", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("watch_in_multi")

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["WATCH", k])
      response = recv_response(sock)
      assert match?({:error, "ERR WATCH inside MULTI is not allowed"}, response)

      send_cmd(sock, ["DISCARD"])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
    end

    test "WATCH with no keys returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["WATCH"])
      response = recv_response(sock)
      assert match?({:error, "ERR wrong number of arguments" <> _}, response)

      :gen_tcp.close(sock)
    end

    test "WATCH on non-existent key detects creation", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("watch_nonexist")

      # WATCH a key that does not exist
      send_cmd(sock, ["WATCH", k])
      assert recv_response(sock) == {:simple, "OK"}

      # Create the key from another connection
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", k, "created"])
      assert recv_response(sock2) == {:simple, "OK"}

      # Verify the key exists before proceeding — ensures ETS is updated
      send_cmd(sock2, ["GET", k])
      assert recv_response(sock2) == "created"
      :gen_tcp.close(sock2)

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      # Should abort because the watched key's value changed (nil → "created")
      assert result == nil

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # UNWATCH
  # ---------------------------------------------------------------------------

  describe "UNWATCH" do
    test "UNWATCH clears watches and allows EXEC to succeed", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("unwatch")

      send_cmd(sock, ["SET", k, "original"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["WATCH", k])
      assert recv_response(sock) == {:simple, "OK"}

      # Modify from another connection
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", k, "modified"])
      assert recv_response(sock2) == {:simple, "OK"}
      :gen_tcp.close(sock2)

      # UNWATCH should clear the watches
      send_cmd(sock, ["UNWATCH"])
      assert recv_response(sock) == {:simple, "OK"}

      # Now MULTI/EXEC should succeed despite the earlier modification
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "final"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert is_list(result)
      assert Enum.at(result, 0) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "final"

      :gen_tcp.close(sock)
    end

    test "UNWATCH returns OK even when no keys are watched", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["UNWATCH"])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # EXEC clears watches
  # ---------------------------------------------------------------------------

  describe "EXEC clears watches" do
    test "EXEC clears watched keys (second MULTI/EXEC succeeds without re-watch)", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("exec_clears_watch")

      send_cmd(sock, ["SET", k, "v1"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["WATCH", k])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "v2"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert is_list(result)

      # Now modify the key externally — but since EXEC cleared watches,
      # a new MULTI/EXEC should still succeed
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", k, "v3"])
      assert recv_response(sock2) == {:simple, "OK"}
      :gen_tcp.close(sock2)

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert is_list(result)
      assert Enum.at(result, 0) == "v3"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Transaction edge cases — spec-required behavior
  # ---------------------------------------------------------------------------

  describe "transaction edge cases" do
    test "WATCH key, modify from another connection, EXEC returns nil (aborted)", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("watch_abort")

      # Set initial value
      send_cmd(sock, ["SET", k, "initial"])
      assert recv_response(sock) == {:simple, "OK"}

      # WATCH the key
      send_cmd(sock, ["WATCH", k])
      assert recv_response(sock) == {:simple, "OK"}

      # Modify from another connection
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", k, "changed_externally"])
      assert recv_response(sock2) == {:simple, "OK"}
      :gen_tcp.close(sock2)

      # Start transaction and queue commands
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "should_not_apply"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      # EXEC should return nil (aborted)
      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert result == nil

      # Value should be the one set by the other connection
      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "changed_externally"

      :gen_tcp.close(sock)
    end

    test "DISCARD clears queue and watched keys", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("discard_clears")

      # Set key, watch it
      send_cmd(sock, ["SET", k, "v1"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["WATCH", k])
      assert recv_response(sock) == {:simple, "OK"}

      # Start MULTI, queue a command, then DISCARD
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "should_not_apply"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["DISCARD"])
      assert recv_response(sock) == {:simple, "OK"}

      # Verify the SET was not executed
      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "v1"

      # DISCARD should have cleared watches, so modifying the key externally
      # and then doing a new transaction should succeed
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", k, "modified_after_discard"])
      assert recv_response(sock2) == {:simple, "OK"}
      :gen_tcp.close(sock2)

      # New transaction without re-watching should succeed
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert is_list(result)
      assert Enum.at(result, 0) == "modified_after_discard"

      :gen_tcp.close(sock)
    end

    test "nested MULTI returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      # Second MULTI should error
      send_cmd(sock, ["MULTI"])
      response = recv_response(sock)
      assert {:error, "ERR MULTI calls can not be nested"} = response

      # Still in MULTI mode — commands should still queue
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["DISCARD"])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
    end

    test "commands after EXEC run normally (not queued)", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("after_exec")

      # MULTI/EXEC cycle
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "txn_val"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert is_list(result)

      # Commands after EXEC should execute normally, not return QUEUED
      send_cmd(sock, ["GET", k])
      response = recv_response(sock)
      assert response == "txn_val"

      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end

    test "MULTI queue preserves command order", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("order")

      # Queue: SET, SET (overwrite), GET — result should reflect ordered execution
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "first"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["SET", k, "second"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["SET", k, "third"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert is_list(result)
      assert length(result) == 5

      # Results should reflect sequential execution
      # SET first
      assert Enum.at(result, 0) == {:simple, "OK"}
      # SET second
      assert Enum.at(result, 1) == {:simple, "OK"}
      # GET -> "second"
      assert Enum.at(result, 2) == "second"
      # SET third
      assert Enum.at(result, 3) == {:simple, "OK"}
      # GET -> "third"
      assert Enum.at(result, 4) == "third"

      :gen_tcp.close(sock)
    end

    test "WATCH + SET from another connection aborts EXEC", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("watch_set2")

      send_cmd(sock, ["SET", k, "10"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["WATCH", k])
      assert recv_response(sock) == {:simple, "OK"}

      # SET from another connection modifies the key
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", k, "11"])
      assert recv_response(sock2) == {:simple, "OK"}
      :gen_tcp.close(sock2)

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert result == nil

      :gen_tcp.close(sock)
    end

    test "multiple MULTI/EXEC cycles on same connection work independently", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("cycle1")
      k2 = ukey("cycle2")

      # First transaction
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k1, "val1"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result1 = recv_response(sock)
      assert is_list(result1)
      assert Enum.at(result1, 0) == {:simple, "OK"}

      # Second transaction
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k2, "val2"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result2 = recv_response(sock)
      assert is_list(result2)
      assert Enum.at(result2, 0) == {:simple, "OK"}

      # Both keys should exist
      send_cmd(sock, ["GET", k1])
      assert recv_response(sock) == "val1"

      send_cmd(sock, ["GET", k2])
      assert recv_response(sock) == "val2"

      :gen_tcp.close(sock)
    end

    test "EXEC without MULTI returns error but connection remains usable", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["EXEC"])
      response = recv_response(sock)
      assert {:error, "ERR EXEC without MULTI"} = response

      # Connection should still work normally
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end

    test "DISCARD without MULTI returns error but connection remains usable", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["DISCARD"])
      response = recv_response(sock)
      assert {:error, "ERR DISCARD without MULTI"} = response

      # Connection should still work normally
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # DEL within transactions triggers version bump
  # ---------------------------------------------------------------------------

  describe "WATCH detects DEL" do
    test "WATCH + DEL from another connection aborts EXEC", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("watch_del")

      send_cmd(sock, ["SET", k, "val"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["WATCH", k])
      assert recv_response(sock) == {:simple, "OK"}

      # DEL the key from another connection
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["DEL", k])
      assert recv_response(sock2) == 1
      :gen_tcp.close(sock2)

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert result == nil

      :gen_tcp.close(sock)
    end
  end
end
