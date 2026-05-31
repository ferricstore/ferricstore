defmodule FerricstoreServer.Commands.TransactionBugHuntTest do
  @moduledoc """
  Bug-hunting tests for MULTI/EXEC/DISCARD/WATCH transaction edge cases.

  These tests exercise transaction semantics that are easy to get wrong,
  targeting the full TCP stack:

      TCP -> RESP3 parser -> connection (transaction state) -> dispatcher -> router -> shard

  Each test opens fresh TCP connections and uses unique keys via `ukey/1`
  to avoid cross-test interference.

  ## Bugs and behavioral gaps probed

  1. MULTI then MULTI -- must return error AND remain in MULTI mode.
  2. EXEC without MULTI -- must return error, connection still usable.
  3. DISCARD without MULTI -- must return error, connection still usable.
  4. WATCH key, no modification, EXEC -- must succeed.
  5. WATCH key, modify from same connection (outside MULTI), EXEC -- must fail.
  6. WATCH non-existent key, create it from another connection, EXEC -- must fail.
  7. WATCH key, DEL it from another connection, EXEC -- must fail.
  8. Queue 1000 commands in MULTI -- all must execute on EXEC.
  9. MULTI with syntax error in queue -- EXECABORT behavior.
  10. WATCH + MULTI + DISCARD -- must clear watched keys.
  11. Nested WATCH calls -- all keys must be watched.
  12. EXEC returns array of results in correct order.
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

  # Hash tag ensures all generated keys co-locate on the same shard.
  defp ukey(name), do: "{bughunt}:#{name}_#{:rand.uniform(999_999)}"

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    %{port: Listener.port()}
  end

  # ---------------------------------------------------------------------------
  # 1. MULTI then MULTI — error, still in MULTI mode
  # ---------------------------------------------------------------------------

  describe "MULTI then MULTI" do
    test "returns error on nested MULTI", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      # Second MULTI must return an error
      send_cmd(sock, ["MULTI"])
      response = recv_response(sock)
      assert {:error, "ERR MULTI calls can not be nested"} = response

      # Must still be in MULTI mode -- commands should queue
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      # EXEC should execute the queued PING (not the rejected MULTI)
      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert is_list(result)
      assert length(result) == 1
      assert Enum.at(result, 0) == {:simple, "PONG"}

      # After EXEC, connection should be back in normal mode
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # 2. EXEC without MULTI — error
  # ---------------------------------------------------------------------------

  describe "EXEC without MULTI" do
    test "returns error and connection remains usable", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["EXEC"])
      response = recv_response(sock)
      assert {:error, "ERR EXEC without MULTI"} = response

      # Connection must still be fully usable after the error
      k = ukey("exec_no_multi")
      send_cmd(sock, ["SET", k, "works"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "works"

      # A subsequent MULTI/EXEC cycle must work normally
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert is_list(result)
      assert Enum.at(result, 0) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # 3. DISCARD without MULTI — error
  # ---------------------------------------------------------------------------

  describe "DISCARD without MULTI" do
    test "returns error and connection remains usable", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["DISCARD"])
      response = recv_response(sock)
      assert {:error, "ERR DISCARD without MULTI"} = response

      # Connection must still be fully usable
      k = ukey("discard_no_multi")
      send_cmd(sock, ["SET", k, "works"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "works"

      # Verify we can still enter MULTI mode after the error
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["DISCARD"])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # 4. WATCH key, no modification, EXEC — succeeds
  # ---------------------------------------------------------------------------

  describe "WATCH key, no modification, EXEC" do
    test "transaction succeeds when watched key is unmodified", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("watch_nomod")

      # Set a value to watch
      send_cmd(sock, ["SET", k, "original"])
      assert recv_response(sock) == {:simple, "OK"}

      # WATCH the key
      send_cmd(sock, ["WATCH", k])
      assert recv_response(sock) == {:simple, "OK"}

      # MULTI/EXEC without any modification to the key should succeed
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "updated"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert is_list(result), "EXEC should return a list of results, got: #{inspect(result)}"
      assert length(result) == 1
      assert Enum.at(result, 0) == {:simple, "OK"}

      # Verify the SET was executed
      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "updated"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # 5. WATCH key, modify same connection, EXEC — fails
  #
  # BUG PROBE: In Redis, if the *same* connection modifies a watched key
  # (outside of MULTI), EXEC should return nil. FerricStore uses shard-level
  # write_version so any write to the same shard (including from the same
  # connection) increments the version. This should cause the EXEC to abort.
  # ---------------------------------------------------------------------------

  describe "WATCH key, modify from same connection, EXEC" do
    test "transaction fails when same connection modifies watched key before MULTI", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("watch_self_mod")

      # Set initial value
      send_cmd(sock, ["SET", k, "initial"])
      assert recv_response(sock) == {:simple, "OK"}

      # WATCH the key
      send_cmd(sock, ["WATCH", k])
      assert recv_response(sock) == {:simple, "OK"}

      # Modify the key from the SAME connection (before MULTI)
      send_cmd(sock, ["SET", k, "self_modified"])
      assert recv_response(sock) == {:simple, "OK"}

      # Now start a transaction
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "txn_value"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      # EXEC must fail because the watched key was modified (even by us)
      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert result == nil,
             "EXEC should return nil when watched key is modified by same connection, got: #{inspect(result)}"

      # Value should remain as the modification before MULTI
      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "self_modified"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # 6. WATCH non-existent key, create it, EXEC — fails
  # ---------------------------------------------------------------------------

  describe "WATCH non-existent key, create it, EXEC" do
    test "transaction fails when watched non-existent key is created", %{port: port} do
      sock1 = connect_and_hello(port)
      k = ukey("watch_create")

      # Key does NOT exist. WATCH it anyway.
      send_cmd(sock1, ["WATCH", k])
      assert recv_response(sock1) == {:simple, "OK"}

      # Create the key from another connection
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", k, "created"])
      assert recv_response(sock2) == {:simple, "OK"}
      :gen_tcp.close(sock2)

      # MULTI/EXEC on connection 1 should abort
      send_cmd(sock1, ["MULTI"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["GET", k])
      assert recv_response(sock1) == {:simple, "QUEUED"}

      send_cmd(sock1, ["EXEC"])
      result = recv_response(sock1)

      assert result == nil,
             "EXEC should return nil when a watched non-existent key was created, got: #{inspect(result)}"

      :gen_tcp.close(sock1)
    end
  end

  # ---------------------------------------------------------------------------
  # 7. WATCH key, DEL it, EXEC — fails
  # ---------------------------------------------------------------------------

  describe "WATCH key, DEL it, EXEC" do
    test "transaction fails when watched key is deleted", %{port: port} do
      sock1 = connect_and_hello(port)
      k = ukey("watch_del")

      # Create the key
      send_cmd(sock1, ["SET", k, "exists"])
      assert recv_response(sock1) == {:simple, "OK"}

      # WATCH it
      send_cmd(sock1, ["WATCH", k])
      assert recv_response(sock1) == {:simple, "OK"}

      # DEL it from another connection
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["DEL", k])
      assert recv_response(sock2) == 1
      :gen_tcp.close(sock2)

      # MULTI/EXEC should abort
      send_cmd(sock1, ["MULTI"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["GET", k])
      assert recv_response(sock1) == {:simple, "QUEUED"}

      send_cmd(sock1, ["EXEC"])
      result = recv_response(sock1)

      assert result == nil,
             "EXEC should return nil when a watched key was DEL'd, got: #{inspect(result)}"

      :gen_tcp.close(sock1)
    end
  end

  # ---------------------------------------------------------------------------
  # 8. Queue 1000 commands in MULTI — all execute on EXEC
  # ---------------------------------------------------------------------------

  describe "queue 1000 commands in MULTI" do
    test "all 1000 queued commands execute on EXEC", %{port: port} do
      sock = connect_and_hello(port)
      prefix = ukey("bulk")

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      # Queue 1000 SET commands
      for i <- 0..999 do
        send_cmd(sock, ["SET", "#{prefix}:#{i}", "val_#{i}"])
        assert recv_response(sock) == {:simple, "QUEUED"}
      end

      # EXEC should return 1000 results
      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert is_list(result), "EXEC result should be a list"
      assert length(result) == 1000, "Expected 1000 results, got #{length(result)}"

      # Every result should be OK
      assert Enum.all?(result, &(&1 == {:simple, "OK"})),
             "All SET results should be OK"

      # Spot-check a few keys outside the transaction
      send_cmd(sock, ["GET", "#{prefix}:0"])
      assert recv_response(sock) == "val_0"

      send_cmd(sock, ["GET", "#{prefix}:500"])
      assert recv_response(sock) == "val_500"

      send_cmd(sock, ["GET", "#{prefix}:999"])
      assert recv_response(sock) == "val_999"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # 9. MULTI with syntax error in queue — EXECABORT
  #
  # BUG PROBE: In Redis, if a command with wrong arity is sent during MULTI,
  # the command returns an error at queue time AND the transaction is marked
  # "dirty". Subsequent EXEC returns -EXECABORT.
  #
  # FerricStore marks the transaction dirty after queue-time syntax/unknown
  # command errors. Later valid commands can still queue, but EXEC aborts the
  # entire transaction instead of committing a partial write set.
  # ---------------------------------------------------------------------------

  describe "MULTI with syntax error in queue" do
    test "syntax error at queue time makes EXEC abort queued writes", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("syntax_err")

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      # Valid command: queued
      send_cmd(sock, ["SET", k, "hello"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      # Invalid command (wrong arity): should error immediately
      send_cmd(sock, ["GET"])
      response = recv_response(sock)
      assert {:error, "ERR wrong number of arguments" <> _} = response

      # Another valid command: should still queue (we're still in MULTI)
      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert {:error, "EXECABORT" <> _} = result

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end

    test "unknown command at queue time makes EXEC abort queued writes", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("unknown_cmd")

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "before"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      # Unknown command
      send_cmd(sock, ["TOTALLYNOTACOMMAND", "arg1"])
      response = recv_response(sock)
      assert {:error, "ERR unknown command" <> _} = response

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert {:error, "EXECABORT" <> _} = result

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # 10. WATCH + MULTI + DISCARD — clears watches
  # ---------------------------------------------------------------------------

  describe "WATCH + MULTI + DISCARD clears watches" do
    test "DISCARD clears watched keys so subsequent transaction succeeds", %{port: port} do
      sock1 = connect_and_hello(port)
      k = ukey("discard_watch")

      # Set the key
      send_cmd(sock1, ["SET", k, "v1"])
      assert recv_response(sock1) == {:simple, "OK"}

      # WATCH the key
      send_cmd(sock1, ["WATCH", k])
      assert recv_response(sock1) == {:simple, "OK"}

      # Enter MULTI
      send_cmd(sock1, ["MULTI"])
      assert recv_response(sock1) == {:simple, "OK"}

      # Queue a command
      send_cmd(sock1, ["SET", k, "should_not_happen"])
      assert recv_response(sock1) == {:simple, "QUEUED"}

      # DISCARD -- must clear both the queue AND the watches
      send_cmd(sock1, ["DISCARD"])
      assert recv_response(sock1) == {:simple, "OK"}

      # Now modify the key from another connection
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", k, "modified_after_discard"])
      assert recv_response(sock2) == {:simple, "OK"}
      # Verify the SET landed before proceeding
      send_cmd(sock2, ["GET", k])
      assert recv_response(sock2) == "modified_after_discard"
      :gen_tcp.close(sock2)

      # Start a new MULTI/EXEC without re-watching.
      # Because DISCARD cleared watches, this should succeed.
      send_cmd(sock1, ["MULTI"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["GET", k])
      assert recv_response(sock1) == {:simple, "QUEUED"}

      send_cmd(sock1, ["EXEC"])
      result = recv_response(sock1)

      assert is_list(result),
             "EXEC should succeed after DISCARD cleared watches, got: #{inspect(result)}"

      assert length(result) == 1
      assert Enum.at(result, 0) == "modified_after_discard"

      :gen_tcp.close(sock1)
    end

    test "queued SET is not executed after DISCARD", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("discard_no_exec")

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "queued_value"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["DISCARD"])
      assert recv_response(sock) == {:simple, "OK"}

      # Key should not exist
      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # 11. Nested WATCH calls — all keys watched
  # ---------------------------------------------------------------------------

  describe "nested WATCH calls" do
    test "multiple WATCH calls accumulate watched keys", %{port: port} do
      sock1 = connect_and_hello(port)
      k1 = ukey("nested_watch_1")
      k2 = ukey("nested_watch_2")

      # Set both keys
      send_cmd(sock1, ["SET", k1, "a"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["SET", k2, "b"])
      assert recv_response(sock1) == {:simple, "OK"}

      # WATCH k1 in one call
      send_cmd(sock1, ["WATCH", k1])
      assert recv_response(sock1) == {:simple, "OK"}

      # WATCH k2 in a separate call
      send_cmd(sock1, ["WATCH", k2])
      assert recv_response(sock1) == {:simple, "OK"}

      # Modify only k2 from another connection
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", k2, "b_changed"])
      assert recv_response(sock2) == {:simple, "OK"}
      :gen_tcp.close(sock2)

      # MULTI/EXEC should fail because k2 (watched in second WATCH call) changed
      send_cmd(sock1, ["MULTI"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["GET", k1])
      assert recv_response(sock1) == {:simple, "QUEUED"}

      send_cmd(sock1, ["EXEC"])
      result = recv_response(sock1)

      assert result == nil,
             "EXEC should return nil because k2 (from second WATCH) was modified, got: #{inspect(result)}"

      :gen_tcp.close(sock1)
    end

    test "multiple WATCH calls -- modifying first watched key also aborts", %{port: port} do
      sock1 = connect_and_hello(port)
      k1 = ukey("nested_watch_first_1")
      k2 = ukey("nested_watch_first_2")

      send_cmd(sock1, ["SET", k1, "a"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["SET", k2, "b"])
      assert recv_response(sock1) == {:simple, "OK"}

      # Two separate WATCH calls
      send_cmd(sock1, ["WATCH", k1])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["WATCH", k2])
      assert recv_response(sock1) == {:simple, "OK"}

      # Modify k1 (the first-watched key)
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", k1, "a_changed"])
      assert recv_response(sock2) == {:simple, "OK"}
      :gen_tcp.close(sock2)

      send_cmd(sock1, ["MULTI"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["GET", k2])
      assert recv_response(sock1) == {:simple, "QUEUED"}

      send_cmd(sock1, ["EXEC"])
      result = recv_response(sock1)

      assert result == nil,
             "EXEC should return nil because k1 (from first WATCH) was modified, got: #{inspect(result)}"

      :gen_tcp.close(sock1)
    end

    test "WATCH with multiple keys in one call watches all of them", %{port: port} do
      sock1 = connect_and_hello(port)
      k1 = ukey("multi_watch_a")
      k2 = ukey("multi_watch_b")
      k3 = ukey("multi_watch_c")

      send_cmd(sock1, ["SET", k1, "1"])
      assert recv_response(sock1) == {:simple, "OK"}
      send_cmd(sock1, ["SET", k2, "2"])
      assert recv_response(sock1) == {:simple, "OK"}
      send_cmd(sock1, ["SET", k3, "3"])
      assert recv_response(sock1) == {:simple, "OK"}

      # Watch all 3 keys in a single WATCH call
      send_cmd(sock1, ["WATCH", k1, k2, k3])
      assert recv_response(sock1) == {:simple, "OK"}

      # Modify only the last key from another connection
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", k3, "changed"])
      assert recv_response(sock2) == {:simple, "OK"}
      :gen_tcp.close(sock2)

      send_cmd(sock1, ["MULTI"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["PING"])
      assert recv_response(sock1) == {:simple, "QUEUED"}

      send_cmd(sock1, ["EXEC"])
      result = recv_response(sock1)

      assert result == nil,
             "EXEC should abort when any of the multi-WATCH keys is modified"

      :gen_tcp.close(sock1)
    end
  end

  # ---------------------------------------------------------------------------
  # 12. EXEC returns array of results in order
  # ---------------------------------------------------------------------------

  describe "EXEC returns array of results in order" do
    test "results correspond to queued commands in exact order", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("order")

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      # Command 0: SET
      send_cmd(sock, ["SET", k, "first"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      # Command 1: GET (should see "first")
      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      # Command 2: SET (overwrite)
      send_cmd(sock, ["SET", k, "second"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      # Command 3: GET (should see "second")
      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      # Command 4: DEL
      send_cmd(sock, ["DEL", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      # Command 5: EXISTS (should be 0 after DEL)
      send_cmd(sock, ["EXISTS", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      # Command 6: SET (re-create)
      send_cmd(sock, ["SET", k, "third"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      # Command 7: GET (should see "third")
      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      # Command 8: PING
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert is_list(result), "EXEC must return a list"
      assert length(result) == 9, "Expected 9 results, got #{length(result)}"

      # Verify each result matches the corresponding queued command
      assert Enum.at(result, 0) == {:simple, "OK"}, "result[0]: SET -> OK"
      assert Enum.at(result, 1) == "first", "result[1]: GET -> 'first'"
      assert Enum.at(result, 2) == {:simple, "OK"}, "result[2]: SET -> OK"
      assert Enum.at(result, 3) == "second", "result[3]: GET -> 'second'"
      assert Enum.at(result, 4) == 1, "result[4]: DEL -> 1"
      assert Enum.at(result, 5) == 0, "result[5]: EXISTS -> 0"
      assert Enum.at(result, 6) == {:simple, "OK"}, "result[6]: SET -> OK"
      assert Enum.at(result, 7) == "third", "result[7]: GET -> 'third'"
      assert Enum.at(result, 8) == {:simple, "PONG"}, "result[8]: PING -> PONG"

      :gen_tcp.close(sock)
    end

    test "EXEC with mixed data types returns correct types in order", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("mixed_types")

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      # SET returns OK
      send_cmd(sock, ["SET", k, "10"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      # INCR returns integer
      send_cmd(sock, ["INCR", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      # GET returns bulk string
      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      # EXISTS returns integer
      send_cmd(sock, ["EXISTS", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      # DEL returns integer
      send_cmd(sock, ["DEL", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      # GET on deleted key returns nil
      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)

      assert is_list(result)
      assert length(result) == 6

      assert Enum.at(result, 0) == {:simple, "OK"}, "SET -> OK"
      assert Enum.at(result, 1) == 11, "INCR 10 -> 11"
      assert Enum.at(result, 2) == "11", "GET -> '11' (string after INCR)"
      assert Enum.at(result, 3) == 1, "EXISTS -> 1"
      assert Enum.at(result, 4) == 1, "DEL -> 1"
      assert Enum.at(result, 5) == nil, "GET deleted -> nil"

      :gen_tcp.close(sock)
    end

    test "empty EXEC returns empty array", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert result == [], "Empty EXEC should return empty list, got: #{inspect(result)}"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Additional edge case: WATCH inside MULTI is forbidden
  # ---------------------------------------------------------------------------

  describe "WATCH inside MULTI" do
    test "returns error when WATCH is called inside MULTI", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("watch_inside_multi")

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["WATCH", k])
      response = recv_response(sock)
      assert {:error, "ERR WATCH inside MULTI is not allowed"} = response

      # Must still be in MULTI mode
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["DISCARD"])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Additional edge case: EXEC clears watches
  # ---------------------------------------------------------------------------

  describe "EXEC clears watches for subsequent transactions" do
    test "after successful EXEC, watches are cleared", %{port: port} do
      sock1 = connect_and_hello(port)
      k = ukey("exec_clears_watch")

      send_cmd(sock1, ["SET", k, "v1"])
      assert recv_response(sock1) == {:simple, "OK"}

      # WATCH + MULTI/EXEC
      send_cmd(sock1, ["WATCH", k])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["MULTI"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["SET", k, "v2"])
      assert recv_response(sock1) == {:simple, "QUEUED"}

      send_cmd(sock1, ["EXEC"])
      result = recv_response(sock1)
      assert is_list(result)

      # Now modify from another connection -- watches should be cleared
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", k, "v3_external"])
      assert recv_response(sock2) == {:simple, "OK"}
      :gen_tcp.close(sock2)

      # A new MULTI/EXEC without WATCH should succeed
      send_cmd(sock1, ["MULTI"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["GET", k])
      assert recv_response(sock1) == {:simple, "QUEUED"}

      send_cmd(sock1, ["EXEC"])
      result2 = recv_response(sock1)
      assert is_list(result2), "Second EXEC should succeed, got: #{inspect(result2)}"
      assert Enum.at(result2, 0) == "v3_external"

      :gen_tcp.close(sock1)
    end

    test "after aborted EXEC, watches are cleared", %{port: port} do
      sock1 = connect_and_hello(port)
      k = ukey("abort_clears_watch")

      send_cmd(sock1, ["SET", k, "v1"])
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

      # After aborted EXEC, modify the key again
      sock3 = connect_and_hello(port)
      send_cmd(sock3, ["SET", k, "post_abort"])
      assert recv_response(sock3) == {:simple, "OK"}
      :gen_tcp.close(sock3)

      # A new MULTI/EXEC without WATCH should succeed (watches were cleared)
      send_cmd(sock1, ["MULTI"])
      assert recv_response(sock1) == {:simple, "OK"}

      send_cmd(sock1, ["GET", k])
      assert recv_response(sock1) == {:simple, "QUEUED"}

      send_cmd(sock1, ["EXEC"])
      result = recv_response(sock1)
      assert is_list(result), "EXEC after abort should succeed without old watches"
      assert Enum.at(result, 0) == "post_abort"

      :gen_tcp.close(sock1)
    end
  end
end
