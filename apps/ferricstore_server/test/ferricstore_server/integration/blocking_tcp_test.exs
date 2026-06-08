defmodule FerricstoreServer.Integration.BlockingTcpTest do
  @moduledoc """
  End-to-end TCP integration tests for BLPOP and BRPOP blocking list commands.

  These tests verify the full blocking lifecycle through the TCP stack:

      TCP -> RESP3 parser -> Connection dispatch_blocking -> Waiters -> Shard

  Blocking commands are connection-level: when a list is empty the connection
  process registers as a waiter and enters a `receive` block. When another
  client pushes to the watched key, the waiter is notified and wakes up to
  pop the value.

  Tests cover:
    - Immediate return when the list already has data
    - Timeout expiry when the list stays empty
    - Cross-client wake: BLPOP blocks, another client RPUSHes, BLPOP returns
    - Multi-key watching
    - Response format validation (RESP3 array of [key, value])
    - Error handling for invalid arguments
  """

  use ExUnit.Case, async: false
  @moduletag :integration

  alias FerricstoreServer.Resp.Encoder
  alias FerricstoreServer.Resp.Parser
  alias FerricstoreServer.Listener

  @moduletag timeout: 30_000

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp recv_response(sock, timeout \\ 10_000) do
    recv_response_loop(sock, "", timeout)
  end

  defp recv_response_loop(sock, buf, timeout) do
    case :gen_tcp.recv(sock, 0, timeout) do
      {:ok, data} ->
        buf2 = buf <> data

        case Parser.parse(buf2) do
          {:ok, [val], ""} -> val
          {:ok, [val], _rest} -> val
          {:ok, [], _} -> recv_response_loop(sock, buf2, timeout)
        end

      {:error, :timeout} ->
        {:tcp_timeout}
    end
  end

  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  defp ukey(name), do: "blk_#{name}_#{:rand.uniform(999_999)}"

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    %{port: Listener.port()}
  end

  setup %{port: port} do
    sock = connect_and_hello(port)
    send_cmd(sock, ["FLUSHDB"])
    recv_response(sock)
    :gen_tcp.close(sock)
    :ok
  end

  # ---------------------------------------------------------------------------
  # BLPOP with existing data
  # ---------------------------------------------------------------------------

  describe "BLPOP with existing data" do
    test "BLPOP returns immediately when list has elements", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("blpop_exists")

      # Push two elements: list becomes [a, b]
      send_cmd(sock, ["RPUSH", k, "a", "b"])
      assert is_integer(recv_response(sock))

      # BLPOP should return immediately with the leftmost element
      send_cmd(sock, ["BLPOP", k, "5"])
      result = recv_response(sock)
      assert result == [k, "a"]

      :gen_tcp.close(sock)
    end

    test "BLPOP with 0 timeout on non-empty list returns immediately", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("blpop_zero_timeout")

      send_cmd(sock, ["RPUSH", k, "value1"])
      assert is_integer(recv_response(sock))

      # timeout=0 means block indefinitely in Redis, but since the list
      # is non-empty it should return immediately regardless
      send_cmd(sock, ["BLPOP", k, "0"])
      result = recv_response(sock)
      assert result == [k, "value1"]

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # BRPOP with existing data
  # ---------------------------------------------------------------------------

  describe "BRPOP with existing data" do
    test "BRPOP returns immediately when list has elements", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("brpop_exists")

      # Push two elements: list becomes [a, b]
      send_cmd(sock, ["RPUSH", k, "a", "b"])
      assert is_integer(recv_response(sock))

      # BRPOP should return immediately with the rightmost element
      send_cmd(sock, ["BRPOP", k, "5"])
      result = recv_response(sock)
      assert result == [k, "b"]

      :gen_tcp.close(sock)
    end

    test "BRPOP with 0 timeout on non-empty list returns immediately", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("brpop_zero_timeout")

      send_cmd(sock, ["RPUSH", k, "only"])
      assert is_integer(recv_response(sock))

      send_cmd(sock, ["BRPOP", k, "0"])
      result = recv_response(sock)
      assert result == [k, "only"]

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # BLPOP with timeout on empty list
  # ---------------------------------------------------------------------------

  describe "BLPOP with timeout on empty list" do
    test "BLPOP on empty key with timeout 1 returns nil after ~1s", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("blpop_timeout")

      start = System.monotonic_time(:millisecond)
      send_cmd(sock, ["BLPOP", k, "1"])
      result = recv_response(sock, 5_000)
      elapsed = System.monotonic_time(:millisecond) - start

      # Should return nil (RESP3 null) after the timeout
      assert result == nil

      # Should have waited approximately 1 second (allow 500ms to 2500ms range)
      assert elapsed >= 500,
             "BLPOP returned too quickly: #{elapsed}ms (expected ~1000ms)"

      assert elapsed <= 2500,
             "BLPOP took too long: #{elapsed}ms (expected ~1000ms)"

      :gen_tcp.close(sock)
    end

    test "BRPOP on empty key with timeout 1 returns nil after ~1s", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("brpop_timeout")

      start = System.monotonic_time(:millisecond)
      send_cmd(sock, ["BRPOP", k, "1"])
      result = recv_response(sock, 5_000)
      elapsed = System.monotonic_time(:millisecond) - start

      assert result == nil
      assert elapsed >= 500
      assert elapsed <= 2500

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # BLPOP wakes on push from another client
  # ---------------------------------------------------------------------------

  describe "BLPOP wakes on push from another client" do
    test "client A BLPOPs, client B RPUSHes, client A receives the value", %{port: port} do
      k = ukey("blpop_wake")

      # Client A: spawn a task that connects and BLPOPs on an empty key
      task =
        Task.async(fn ->
          sock_a = connect_and_hello(port)
          send_cmd(sock_a, ["BLPOP", k, "5"])
          result = recv_response(sock_a, 10_000)
          :gen_tcp.close(sock_a)
          result
        end)

      # Give client A time to register as a waiter
      Process.sleep(200)

      # Client B: connect and push a value to the key
      sock_b = connect_and_hello(port)
      send_cmd(sock_b, ["RPUSH", k, "wakeup_value"])
      push_result = recv_response(sock_b)
      assert push_result == 1
      :gen_tcp.close(sock_b)

      # Client A should have been woken and received the value
      blpop_result = Task.await(task, 10_000)
      assert blpop_result == [k, "wakeup_value"]
    end

    test "client A BRPOPs, client B LPUSHes, client A receives the value", %{port: port} do
      k = ukey("brpop_wake")

      task =
        Task.async(fn ->
          sock_a = connect_and_hello(port)
          send_cmd(sock_a, ["BRPOP", k, "5"])
          result = recv_response(sock_a, 10_000)
          :gen_tcp.close(sock_a)
          result
        end)

      Process.sleep(200)

      sock_b = connect_and_hello(port)
      send_cmd(sock_b, ["LPUSH", k, "brpop_wakeup"])
      push_result = recv_response(sock_b)
      assert push_result == 1
      :gen_tcp.close(sock_b)

      brpop_result = Task.await(task, 10_000)
      assert brpop_result == [k, "brpop_wakeup"]
    end
  end

  # ---------------------------------------------------------------------------
  # BLPOP multi-key
  # ---------------------------------------------------------------------------

  describe "BLPOP multi-key" do
    test "BLPOP on key1, key2 returns from whichever key has data", %{port: port} do
      k1 = ukey("multi_k1")
      k2 = ukey("multi_k2")

      # Only push to k2
      sock_setup = connect_and_hello(port)
      send_cmd(sock_setup, ["RPUSH", k2, "from_k2"])
      assert is_integer(recv_response(sock_setup))
      :gen_tcp.close(sock_setup)

      # BLPOP watching both keys - should return from k2 since k1 is empty
      sock = connect_and_hello(port)
      send_cmd(sock, ["BLPOP", k1, k2, "1"])
      result = recv_response(sock, 5_000)
      assert result == [k2, "from_k2"]

      :gen_tcp.close(sock)
    end

    test "BLPOP multi-key wakes when push arrives on watched key", %{port: port} do
      k1 = ukey("multi_wake_k1")
      k2 = ukey("multi_wake_k2")

      # Both keys are empty; client A watches both
      task =
        Task.async(fn ->
          sock_a = connect_and_hello(port)
          send_cmd(sock_a, ["BLPOP", k1, k2, "5"])
          result = recv_response(sock_a, 10_000)
          :gen_tcp.close(sock_a)
          result
        end)

      Process.sleep(200)

      # Client B pushes to k2
      sock_b = connect_and_hello(port)
      send_cmd(sock_b, ["RPUSH", k2, "multi_wake_val"])
      assert recv_response(sock_b) == 1
      :gen_tcp.close(sock_b)

      result = Task.await(task, 10_000)
      assert result == [k2, "multi_wake_val"]
    end

    test "BLPOP prefers first key when multiple keys have data", %{port: port} do
      k1 = ukey("multi_prefer_k1")
      k2 = ukey("multi_prefer_k2")

      sock = connect_and_hello(port)

      # Push to both keys
      send_cmd(sock, ["RPUSH", k1, "val_from_k1"])
      assert is_integer(recv_response(sock))
      send_cmd(sock, ["RPUSH", k2, "val_from_k2"])
      assert is_integer(recv_response(sock))

      # BLPOP should prefer k1 (first in the key list)
      send_cmd(sock, ["BLPOP", k1, k2, "1"])
      result = recv_response(sock, 5_000)
      assert result == [k1, "val_from_k1"]

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Response format validation
  # ---------------------------------------------------------------------------

  describe "BLPOP response format" do
    test "BLPOP returns a 2-element array of [key, value]", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("format_check")

      send_cmd(sock, ["RPUSH", k, "format_val"])
      assert is_integer(recv_response(sock))

      send_cmd(sock, ["BLPOP", k, "1"])
      result = recv_response(sock)

      assert is_list(result), "Expected list, got: #{inspect(result)}"
      assert length(result) == 2, "Expected 2-element list, got: #{inspect(result)}"

      [returned_key, returned_value] = result
      assert returned_key == k
      assert returned_value == "format_val"
      assert is_binary(returned_key)
      assert is_binary(returned_value)

      :gen_tcp.close(sock)
    end

    test "BRPOP returns a 2-element array of [key, value]", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("brpop_format")

      send_cmd(sock, ["RPUSH", k, "first", "second"])
      assert is_integer(recv_response(sock))

      send_cmd(sock, ["BRPOP", k, "1"])
      result = recv_response(sock)

      assert is_list(result)
      assert length(result) == 2

      [returned_key, returned_value] = result
      assert returned_key == k
      # BRPOP should return the rightmost element
      assert returned_value == "second"

      :gen_tcp.close(sock)
    end

    test "BLPOP timeout returns nil (not an array)", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("nil_format")

      send_cmd(sock, ["BLPOP", k, "1"])
      result = recv_response(sock, 5_000)

      assert result == nil, "Expected nil on timeout, got: #{inspect(result)}"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Error on negative timeout
  # ---------------------------------------------------------------------------

  describe "BLPOP/BRPOP error handling" do
    test "BLPOP with negative timeout returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["BLPOP", "anykey", "-1"])
      result = recv_response(sock)

      assert match?({:error, _msg}, result),
             "Expected error for negative timeout, got: #{inspect(result)}"

      :gen_tcp.close(sock)
    end

    test "BRPOP with negative timeout returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["BRPOP", "anykey", "-1"])
      result = recv_response(sock)

      assert match?({:error, _msg}, result),
             "Expected error for negative timeout, got: #{inspect(result)}"

      :gen_tcp.close(sock)
    end

    test "BLPOP with no arguments returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["BLPOP"])
      result = recv_response(sock)

      assert match?({:error, _msg}, result),
             "Expected error for missing arguments, got: #{inspect(result)}"

      :gen_tcp.close(sock)
    end

    test "BRPOP with no arguments returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["BRPOP"])
      result = recv_response(sock)

      assert match?({:error, _msg}, result),
             "Expected error for missing arguments, got: #{inspect(result)}"

      :gen_tcp.close(sock)
    end

    test "BLPOP with non-numeric timeout returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["BLPOP", "anykey", "notanumber"])
      result = recv_response(sock)

      assert match?({:error, _msg}, result),
             "Expected error for non-numeric timeout, got: #{inspect(result)}"

      :gen_tcp.close(sock)
    end

    test "connection remains usable after BLPOP error", %{port: port} do
      sock = connect_and_hello(port)

      # Send an invalid BLPOP
      send_cmd(sock, ["BLPOP", "anykey", "-1"])
      result = recv_response(sock)
      assert match?({:error, _}, result)

      # Connection should still work for other commands
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # BLMOVE with existing data
  # ---------------------------------------------------------------------------

  describe "BLMOVE with existing data" do
    test "BLMOVE returns moved element immediately when source has elements", %{port: port} do
      sock = connect_and_hello(port)
      src = ukey("blmove_src")
      dst = ukey("blmove_dst")

      # Push elements to source: [a, b, c]
      send_cmd(sock, ["RPUSH", src, "a", "b", "c"])
      assert is_integer(recv_response(sock))

      # BLMOVE src dst LEFT RIGHT 5
      send_cmd(sock, ["BLMOVE", src, dst, "LEFT", "RIGHT", "5"])
      result = recv_response(sock)
      assert result == "a"

      # Verify source lost the element
      send_cmd(sock, ["LRANGE", src, "0", "-1"])
      assert recv_response(sock) == ["b", "c"]

      # Verify destination received it at right end
      send_cmd(sock, ["LRANGE", dst, "0", "-1"])
      assert recv_response(sock) == ["a"]

      :gen_tcp.close(sock)
    end

    test "BLMOVE RIGHT LEFT moves from right end to left end", %{port: port} do
      sock = connect_and_hello(port)
      src = ukey("blmove_rl_src")
      dst = ukey("blmove_rl_dst")

      send_cmd(sock, ["RPUSH", src, "x", "y", "z"])
      assert is_integer(recv_response(sock))

      send_cmd(sock, ["BLMOVE", src, dst, "RIGHT", "LEFT", "5"])
      result = recv_response(sock)
      assert result == "z"

      send_cmd(sock, ["LRANGE", src, "0", "-1"])
      assert recv_response(sock) == ["x", "y"]

      send_cmd(sock, ["LRANGE", dst, "0", "-1"])
      assert recv_response(sock) == ["z"]

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # BLMOVE with timeout on empty source
  # ---------------------------------------------------------------------------

  describe "BLMOVE with timeout on empty source" do
    test "BLMOVE on empty source with timeout 1 returns nil after ~1s", %{port: port} do
      sock = connect_and_hello(port)
      src = ukey("blmove_timeout_src")
      dst = ukey("blmove_timeout_dst")

      start = System.monotonic_time(:millisecond)
      send_cmd(sock, ["BLMOVE", src, dst, "LEFT", "RIGHT", "1"])
      result = recv_response(sock, 5_000)
      elapsed = System.monotonic_time(:millisecond) - start

      assert result == nil
      assert elapsed >= 500, "BLMOVE returned too quickly: #{elapsed}ms"
      assert elapsed <= 2500, "BLMOVE took too long: #{elapsed}ms"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # BLMOVE wakes on push from another client
  # ---------------------------------------------------------------------------

  describe "BLMOVE wakes on push from another client" do
    test "client A BLMOVEs, client B RPUSHes to source, client A receives the moved value", %{port: port} do
      src = ukey("blmove_wake_src")
      dst = ukey("blmove_wake_dst")

      task =
        Task.async(fn ->
          sock_a = connect_and_hello(port)
          send_cmd(sock_a, ["BLMOVE", src, dst, "LEFT", "RIGHT", "5"])
          result = recv_response(sock_a, 10_000)
          :gen_tcp.close(sock_a)
          result
        end)

      Process.sleep(200)

      # Client B pushes to the source key
      sock_b = connect_and_hello(port)
      send_cmd(sock_b, ["RPUSH", src, "wake_val"])
      push_result = recv_response(sock_b)
      assert push_result == 1
      :gen_tcp.close(sock_b)

      blmove_result = Task.await(task, 10_000)
      assert blmove_result == "wake_val"

      # Verify the element landed in the destination
      sock_check = connect_and_hello(port)
      send_cmd(sock_check, ["LRANGE", dst, "0", "-1"])
      assert recv_response(sock_check) == ["wake_val"]
      :gen_tcp.close(sock_check)
    end
  end

  # ---------------------------------------------------------------------------
  # BLMOVE error handling
  # ---------------------------------------------------------------------------

  describe "BLMOVE error handling" do
    test "BLMOVE with negative timeout returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["BLMOVE", "src", "dst", "LEFT", "RIGHT", "-1"])
      result = recv_response(sock)
      assert match?({:error, _}, result)

      :gen_tcp.close(sock)
    end

    test "BLMOVE with wrong number of args returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["BLMOVE", "src", "dst"])
      result = recv_response(sock)
      assert match?({:error, _}, result)

      :gen_tcp.close(sock)
    end

    test "BLMOVE with invalid direction returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["BLMOVE", "src", "dst", "UP", "DOWN", "5"])
      result = recv_response(sock)
      assert match?({:error, _}, result)

      :gen_tcp.close(sock)
    end

    test "connection remains usable after BLMOVE error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["BLMOVE", "src", "dst", "LEFT", "RIGHT", "-1"])
      result = recv_response(sock)
      assert match?({:error, _}, result)

      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # BLMPOP with existing data
  # ---------------------------------------------------------------------------

  describe "BLMPOP with existing data" do
    test "BLMPOP returns [key, [element]] from first non-empty key", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("blmpop_k1")
      k2 = ukey("blmpop_k2")

      # k1 is empty, k2 has data
      send_cmd(sock, ["RPUSH", k2, "val1", "val2"])
      assert is_integer(recv_response(sock))

      # BLMPOP 0 2 k1 k2 LEFT
      send_cmd(sock, ["BLMPOP", "0", "2", k1, k2, "LEFT"])
      result = recv_response(sock)
      assert result == [k2, ["val1"]]

      :gen_tcp.close(sock)
    end

    test "BLMPOP LEFT pops from left end", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("blmpop_left")

      send_cmd(sock, ["RPUSH", k, "a", "b", "c"])
      assert is_integer(recv_response(sock))

      send_cmd(sock, ["BLMPOP", "0", "1", k, "LEFT"])
      result = recv_response(sock)
      assert result == [k, ["a"]]

      send_cmd(sock, ["LRANGE", k, "0", "-1"])
      assert recv_response(sock) == ["b", "c"]

      :gen_tcp.close(sock)
    end

    test "BLMPOP RIGHT pops from right end", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("blmpop_right")

      send_cmd(sock, ["RPUSH", k, "a", "b", "c"])
      assert is_integer(recv_response(sock))

      send_cmd(sock, ["BLMPOP", "0", "1", k, "RIGHT"])
      result = recv_response(sock)
      assert result == [k, ["c"]]

      send_cmd(sock, ["LRANGE", k, "0", "-1"])
      assert recv_response(sock) == ["a", "b"]

      :gen_tcp.close(sock)
    end

    test "BLMPOP with COUNT pops multiple elements", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("blmpop_count")

      send_cmd(sock, ["RPUSH", k, "a", "b", "c", "d"])
      assert is_integer(recv_response(sock))

      send_cmd(sock, ["BLMPOP", "0", "1", k, "LEFT", "COUNT", "3"])
      result = recv_response(sock)
      assert result == [k, ["a", "b", "c"]]

      send_cmd(sock, ["LRANGE", k, "0", "-1"])
      assert recv_response(sock) == ["d"]

      :gen_tcp.close(sock)
    end

    test "BLMPOP prefers first key when multiple have data", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("blmpop_prefer_k1")
      k2 = ukey("blmpop_prefer_k2")

      send_cmd(sock, ["RPUSH", k1, "from_k1"])
      assert is_integer(recv_response(sock))
      send_cmd(sock, ["RPUSH", k2, "from_k2"])
      assert is_integer(recv_response(sock))

      send_cmd(sock, ["BLMPOP", "0", "2", k1, k2, "LEFT"])
      result = recv_response(sock)
      assert result == [k1, ["from_k1"]]

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # BLMPOP with timeout on empty keys
  # ---------------------------------------------------------------------------

  describe "BLMPOP with timeout on empty keys" do
    test "BLMPOP on empty keys with timeout 1 returns nil after ~1s", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("blmpop_timeout")

      start = System.monotonic_time(:millisecond)
      send_cmd(sock, ["BLMPOP", "1", "1", k, "LEFT"])
      result = recv_response(sock, 5_000)
      elapsed = System.monotonic_time(:millisecond) - start

      assert result == nil
      assert elapsed >= 500, "BLMPOP returned too quickly: #{elapsed}ms"
      assert elapsed <= 2500, "BLMPOP took too long: #{elapsed}ms"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # BLMPOP wakes on push from another client
  # ---------------------------------------------------------------------------

  describe "BLMPOP wakes on push from another client" do
    test "client A BLMPOPs, client B RPUSHes, client A receives the value", %{port: port} do
      k = ukey("blmpop_wake")

      task =
        Task.async(fn ->
          sock_a = connect_and_hello(port)
          send_cmd(sock_a, ["BLMPOP", "5", "1", k, "LEFT"])
          result = recv_response(sock_a, 10_000)
          :gen_tcp.close(sock_a)
          result
        end)

      Process.sleep(200)

      sock_b = connect_and_hello(port)
      send_cmd(sock_b, ["RPUSH", k, "wake_value"])
      push_result = recv_response(sock_b)
      assert push_result == 1
      :gen_tcp.close(sock_b)

      blmpop_result = Task.await(task, 10_000)
      assert blmpop_result == [k, ["wake_value"]]
    end

    test "BLMPOP multi-key wakes when push arrives on second key", %{port: port} do
      k1 = ukey("blmpop_mw_k1")
      k2 = ukey("blmpop_mw_k2")

      task =
        Task.async(fn ->
          sock_a = connect_and_hello(port)
          send_cmd(sock_a, ["BLMPOP", "5", "2", k1, k2, "LEFT"])
          result = recv_response(sock_a, 10_000)
          :gen_tcp.close(sock_a)
          result
        end)

      Process.sleep(200)

      sock_b = connect_and_hello(port)
      send_cmd(sock_b, ["RPUSH", k2, "from_k2"])
      push_result = recv_response(sock_b)
      assert push_result == 1
      :gen_tcp.close(sock_b)

      result = Task.await(task, 10_000)
      assert result == [k2, ["from_k2"]]
    end
  end

  # ---------------------------------------------------------------------------
  # BLMPOP error handling
  # ---------------------------------------------------------------------------

  describe "BLMPOP error handling" do
    test "BLMPOP with negative timeout returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["BLMPOP", "-1", "1", "anykey", "LEFT"])
      result = recv_response(sock)
      assert match?({:error, _}, result)

      :gen_tcp.close(sock)
    end

    test "BLMPOP with no arguments returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["BLMPOP"])
      result = recv_response(sock)
      assert match?({:error, _}, result)

      :gen_tcp.close(sock)
    end

    test "BLMPOP with invalid direction returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["BLMPOP", "0", "1", "anykey", "UP"])
      result = recv_response(sock)
      assert match?({:error, _}, result)

      :gen_tcp.close(sock)
    end

    test "connection remains usable after BLMPOP error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["BLMPOP", "-1", "1", "anykey", "LEFT"])
      result = recv_response(sock)
      assert match?({:error, _}, result)

      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Regression: connection must stay alive after blocking command completes
  # ---------------------------------------------------------------------------

  describe "connection stays alive after blocking timeout" do
    test "BLPOP timeout does not deadlock subsequent commands", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("blpop_alive")

      # BLPOP on empty key — will timeout after 1s
      send_cmd(sock, ["BLPOP", k, "1"])
      result = recv_response(sock, 5_000)
      assert result == nil

      # First command after timeout — should work
      send_cmd(sock, ["PING"])
      assert recv_response(sock, 2_000) == {:simple, "PONG"}

      # Second command — this is the one that would deadlock if socket
      # is stuck in active: :once (the first command consumed the one
      # message, now socket is passive with no re-arm)
      send_cmd(sock, ["PING"])
      assert recv_response(sock, 2_000) == {:simple, "PONG"}

      # Third command — confirms connection is fully operational
      send_cmd(sock, ["SET", k, "alive"])
      assert recv_response(sock, 2_000) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock, 2_000) == "alive"

      :gen_tcp.close(sock)
    end

    test "BRPOP timeout does not deadlock subsequent commands", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("brpop_alive")

      send_cmd(sock, ["BRPOP", k, "1"])
      result = recv_response(sock, 5_000)
      assert result == nil

      # Multiple commands after timeout must all work
      send_cmd(sock, ["PING"])
      assert recv_response(sock, 2_000) == {:simple, "PONG"}

      send_cmd(sock, ["PING"])
      assert recv_response(sock, 2_000) == {:simple, "PONG"}

      send_cmd(sock, ["PING"])
      assert recv_response(sock, 2_000) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end

    test "BLMOVE timeout does not deadlock subsequent commands", %{port: port} do
      sock = connect_and_hello(port)
      src = ukey("blmove_alive_src")
      dst = ukey("blmove_alive_dst")

      # BLMOVE on empty source — will timeout after 1s
      send_cmd(sock, ["BLMOVE", src, dst, "LEFT", "RIGHT", "1"])
      result = recv_response(sock, 5_000)
      assert result == nil

      # First command after timeout
      send_cmd(sock, ["PING"])
      assert recv_response(sock, 2_000) == {:simple, "PONG"}

      # Second command — this would deadlock if socket stuck in active: :once
      send_cmd(sock, ["PING"])
      assert recv_response(sock, 2_000) == {:simple, "PONG"}

      # Third command
      send_cmd(sock, ["SET", src, "still_alive"])
      assert recv_response(sock, 2_000) == {:simple, "OK"}

      send_cmd(sock, ["GET", src])
      assert recv_response(sock, 2_000) == "still_alive"

      :gen_tcp.close(sock)
    end

    test "BLPOP with wake does not deadlock subsequent commands", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("blpop_wake_alive")

      # Start BLPOP in background — will block
      send_cmd(sock, ["BLPOP", k, "5"])

      # Push from another connection to wake the blocker
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["RPUSH", k, "wakeup"])
      recv_response(sock2)
      :gen_tcp.close(sock2)

      # BLPOP should return the pushed value
      result = recv_response(sock, 5_000)
      assert result == [k, "wakeup"]

      # Connection must still work after wake
      send_cmd(sock, ["PING"])
      assert recv_response(sock, 2_000) == {:simple, "PONG"}

      send_cmd(sock, ["PING"])
      assert recv_response(sock, 2_000) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end
  end
end
