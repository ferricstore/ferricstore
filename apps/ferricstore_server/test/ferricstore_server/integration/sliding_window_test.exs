defmodule FerricstoreServer.Integration.SlidingWindowTest do
  @moduledoc """
  Integration tests for the sliding window pipeline dispatch (spec section 2C.2).

  These tests verify that the connection's pipeline dispatch correctly:

  1. Preserves response ordering (responses arrive in pipeline order).
  2. Delivers responses incrementally (fast commands before a slow command
     get their responses sent before the slow command completes).
  3. Maintains correctness for MULTI/EXEC transactions in pipelines.
  4. Handles mixed fast/slow commands across different shards.
  5. Preserves causal ordering for commands on the same key.
  """

  use ExUnit.Case, async: false
  @moduletag :integration
  @moduletag :global_state

  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener

  @moduletag timeout: 30_000

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Store buffered responses in the process dictionary to handle TCP coalescing.
  @buffered_key :__sw_test_buffer__
  @binary_key :__sw_test_binary__

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  # Receives a single RESP3 response, buffering any extras that arrived
  # in the same TCP segment for subsequent calls.
  defp recv_response(sock) do
    case Process.get(@buffered_key, []) do
      [val | rest] ->
        Process.put(@buffered_key, rest)
        val

      [] ->
        buf = Process.get(@binary_key, "")
        do_recv_response(sock, buf)
    end
  end

  defp do_recv_response(sock, buf, retries \\ 1) do
    case Parser.parse(buf) do
      {:ok, [val | rest], remaining_bin} ->
        Process.put(@buffered_key, rest)
        Process.put(@binary_key, remaining_bin)
        val

      {:ok, [], _} ->
        case :gen_tcp.recv(sock, 0, 10_000) do
          {:ok, data} ->
            do_recv_response(sock, buf <> data, retries)

          {:error, :timeout} when retries > 0 ->
            # Under full suite load, the server may be slow. Retry once.
            do_recv_response(sock, buf, retries - 1)

          {:error, :timeout} ->
            raise "TCP recv timed out after retries (buf size: #{byte_size(buf)})"
        end
    end
  end

  # Clear the response buffer (call after each connection).
  defp clear_recv_buffer do
    Process.delete(@buffered_key)
    Process.delete(@binary_key)
  end

  defp recv_n(sock, n) do
    do_recv_n(sock, n, "", [])
  end

  defp do_recv_n(_sock, 0, _buf, acc), do: acc

  defp do_recv_n(sock, remaining, buf, acc, retries \\ 1) when remaining > 0 do
    case :gen_tcp.recv(sock, 0, 10_000) do
      {:ok, data} ->
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

      {:error, :timeout} when retries > 0 ->
        do_recv_n(sock, remaining, buf, acc, retries - 1)

      {:error, :timeout} ->
        raise "TCP recv timed out in recv_n (remaining: #{remaining}, buf: #{byte_size(buf)} bytes)"
    end
  end

  defp connect_and_hello(port) do
    # Clear any buffered responses from a previous connection in this test process.
    clear_recv_buffer()

    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  # Receives a single response with a short timeout (for timing tests).
  # Returns `{:ok, value}` or `:timeout`.
  defp recv_response_timeout(sock, timeout_ms) do
    case Process.get(@buffered_key, []) do
      [val | rest] ->
        Process.put(@buffered_key, rest)
        {:ok, val}

      [] ->
        buf = Process.get(@binary_key, "")
        do_recv_response_timeout(sock, buf, timeout_ms)
    end
  end

  defp do_recv_response_timeout(sock, buf, timeout_ms) do
    case Parser.parse(buf) do
      {:ok, [val | rest], remaining_bin} ->
        Process.put(@buffered_key, rest)
        Process.put(@binary_key, remaining_bin)
        {:ok, val}

      {:ok, [], _} ->
        case :gen_tcp.recv(sock, 0, timeout_ms) do
          {:ok, data} ->
            do_recv_response_timeout(sock, buf <> data, timeout_ms)

          {:error, :timeout} ->
            :timeout
        end
    end
  end

  defp ukey(name), do: "sw_#{name}_#{:rand.uniform(999_999)}"

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    %{port: Listener.port()}
  end

  setup do
    # Use direct GenServer calls to flush keys instead of FLUSHDB over TCP.
    # FLUSHDB iterates all keys and can time out under heavy test load.
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    :ok
  end

  # ---------------------------------------------------------------------------
  # Response ordering tests
  # ---------------------------------------------------------------------------

  describe "response ordering" do
    test "pipeline responses arrive in pipeline order", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("order_a")
      k2 = ukey("order_b")
      k3 = ukey("order_c")

      # Pre-set keys so GETs return known values
      send_cmd(sock, ["SET", k1, "alpha"])
      assert recv_response(sock) == {:simple, "OK"}
      send_cmd(sock, ["SET", k2, "beta"])
      assert recv_response(sock) == {:simple, "OK"}
      send_cmd(sock, ["SET", k3, "gamma"])
      assert recv_response(sock) == {:simple, "OK"}

      # Pipeline: GET k1, GET k2, GET k3
      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["GET", k1]),
          Encoder.encode(["GET", k2]),
          Encoder.encode(["GET", k3])
        ])

      :ok = :gen_tcp.send(sock, pipeline)
      responses = recv_n(sock, 3)

      assert Enum.at(responses, 0) == "alpha"
      assert Enum.at(responses, 1) == "beta"
      assert Enum.at(responses, 2) == "gamma"

      :gen_tcp.close(sock)
    end

    test "SET then GET on same key returns correct value in pipeline", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("causal")

      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["SET", k, "hello"]),
          Encoder.encode(["GET", k])
        ])

      :ok = :gen_tcp.send(sock, pipeline)
      responses = recv_n(sock, 2)

      assert Enum.at(responses, 0) == {:simple, "OK"}
      assert Enum.at(responses, 1) == "hello"

      :gen_tcp.close(sock)
    end

    test "causal ordering: SET, GET, DEL, GET on same key", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("causal_chain")

      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["SET", k, "exists"]),
          Encoder.encode(["GET", k]),
          Encoder.encode(["DEL", k]),
          Encoder.encode(["GET", k])
        ])

      :ok = :gen_tcp.send(sock, pipeline)
      responses = recv_n(sock, 4)

      assert Enum.at(responses, 0) == {:simple, "OK"}
      assert Enum.at(responses, 1) == "exists"
      assert Enum.at(responses, 2) == 1
      assert Enum.at(responses, 3) == nil

      :gen_tcp.close(sock)
    end

    test "large pipeline preserves order (20 SETs then 20 GETs)", %{port: port} do
      sock = connect_and_hello(port)
      keys = for i <- 1..20, do: ukey("big_pipe_#{i}")

      set_cmds =
        Enum.map(keys, fn k -> Encoder.encode(["SET", k, "val_#{k}"]) end)

      get_cmds =
        Enum.map(keys, fn k -> Encoder.encode(["GET", k]) end)

      pipeline = IO.iodata_to_binary(set_cmds ++ get_cmds)
      :ok = :gen_tcp.send(sock, pipeline)
      responses = recv_n(sock, 40)

      # First 20 responses: all OK
      for i <- 0..19 do
        assert Enum.at(responses, i) == {:simple, "OK"},
               "SET response #{i} should be OK"
      end

      # Last 20 responses: values in order
      for {k, i} <- Enum.with_index(keys) do
        assert Enum.at(responses, 20 + i) == "val_#{k}",
               "GET response #{20 + i} should be val_#{k}"
      end

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Sliding window delivery tests
  # ---------------------------------------------------------------------------

  describe "sliding window delivery" do
    @tag :bench
    test "fast commands before a slow command get responses delivered first", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("fast_before_slow")

      # Pipeline: GET (fast, returns nil) -> DEBUG SLEEP 2 (slow) -> PING (fast)
      # The GET response should arrive before DEBUG SLEEP completes.
      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["GET", k]),
          Encoder.encode(["DEBUG", "SLEEP", "2"]),
          Encoder.encode(["PING"])
        ])

      :ok = :gen_tcp.send(sock, pipeline)

      # The GET response should arrive within 500ms (well before the 2s sleep).
      # Use recv_response_timeout to prove the response is available early.
      case recv_response_timeout(sock, 500) do
        {:ok, val} ->
          assert val == nil, "GET response should be nil"

        :timeout ->
          flunk("GET response did not arrive within 500ms -- sliding window not working")
      end

      # DEBUG SLEEP should take ~2 seconds
      t0 = System.monotonic_time(:millisecond)
      r1 = recv_response(sock)
      t1 = System.monotonic_time(:millisecond)

      # PING comes after SLEEP in pipeline order
      r2 = recv_response(sock)

      assert r1 == {:simple, "OK"}
      assert r2 == {:simple, "PONG"}

      # Verify the SLEEP actually took time (it wasn't skipped)
      assert t1 - t0 >= 1500,
             "DEBUG SLEEP response took #{t1 - t0}ms, expected >= 1500ms"

      :gen_tcp.close(sock)
    end

    @tag :bench
    test "multiple fast commands before slow command all arrive early", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("multi_fast_a")
      k2 = ukey("multi_fast_b")
      k3 = ukey("multi_fast_c")

      # Pre-set keys
      for {k, v} <- [{k1, "a"}, {k2, "b"}, {k3, "c"}] do
        send_cmd(sock, ["SET", k, v])
        assert recv_response(sock) == {:simple, "OK"}
      end

      # Pipeline: GET k1, GET k2, GET k3 (all fast), DEBUG SLEEP 2 (slow)
      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["GET", k1]),
          Encoder.encode(["GET", k2]),
          Encoder.encode(["GET", k3]),
          Encoder.encode(["DEBUG", "SLEEP", "2"])
        ])

      :ok = :gen_tcp.send(sock, pipeline)

      # All 3 GETs should arrive within 500ms (well before 2s sleep)
      for {expected, label} <- [{"a", "k1"}, {"b", "k2"}, {"c", "k3"}] do
        case recv_response_timeout(sock, 500) do
          {:ok, val} ->
            assert val == expected, "GET #{label} should be #{expected}, got #{inspect(val)}"

          :timeout ->
            flunk("GET #{label} response did not arrive within 500ms")
        end
      end

      # DEBUG SLEEP arrives after ~2 seconds
      r3 = recv_response(sock)
      assert r3 == {:simple, "OK"}

      :gen_tcp.close(sock)
    end

    @tag :bench
    test "fast commands AFTER a slow command are blocked until slow completes", %{port: port} do
      sock = connect_and_hello(port)

      # Pipeline: DEBUG SLEEP 1 (slow), PING (fast)
      # PING response must wait for SLEEP (pipeline ordering)
      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["DEBUG", "SLEEP", "1"]),
          Encoder.encode(["PING"])
        ])

      :ok = :gen_tcp.send(sock, pipeline)

      t0 = System.monotonic_time(:millisecond)
      r0 = recv_response(sock)
      t1 = System.monotonic_time(:millisecond)
      r1 = recv_response(sock)
      t2 = System.monotonic_time(:millisecond)

      assert r0 == {:simple, "OK"}
      assert r1 == {:simple, "PONG"}

      # Both responses should arrive after ~1 second (SLEEP blocks the pipeline head)
      assert t1 - t0 >= 900, "SLEEP response should take >= 900ms"
      assert t2 - t0 >= 900, "PING response should also take >= 900ms"
    end
  end

  # ---------------------------------------------------------------------------
  # MULTI/EXEC in pipeline tests
  # ---------------------------------------------------------------------------

  describe "MULTI/EXEC in pipeline" do
    test "MULTI/EXEC with queued commands works in pipeline", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("multi_pipe")

      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["SET", k, "before"]),
          Encoder.encode(["MULTI"]),
          Encoder.encode(["SET", k, "inside"]),
          Encoder.encode(["GET", k]),
          Encoder.encode(["EXEC"])
        ])

      :ok = :gen_tcp.send(sock, pipeline)
      responses = recv_n(sock, 5)

      # SET before MULTI
      assert Enum.at(responses, 0) == {:simple, "OK"}
      # MULTI
      assert Enum.at(responses, 1) == {:simple, "OK"}
      # SET inside MULTI -> QUEUED
      assert Enum.at(responses, 2) == {:simple, "QUEUED"}
      # GET inside MULTI -> QUEUED
      assert Enum.at(responses, 3) == {:simple, "QUEUED"}
      # EXEC -> [OK, "inside"]
      exec_result = Enum.at(responses, 4)
      assert is_list(exec_result)
      assert Enum.at(exec_result, 0) == {:simple, "OK"}
      assert Enum.at(exec_result, 1) == "inside"

      :gen_tcp.close(sock)
    end

    test "DISCARD in pipeline returns to normal dispatch", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("discard_pipe")

      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["SET", k, "original"]),
          Encoder.encode(["MULTI"]),
          Encoder.encode(["SET", k, "transacted"]),
          Encoder.encode(["DISCARD"]),
          Encoder.encode(["GET", k])
        ])

      :ok = :gen_tcp.send(sock, pipeline)
      responses = recv_n(sock, 5)

      assert Enum.at(responses, 0) == {:simple, "OK"}
      assert Enum.at(responses, 1) == {:simple, "OK"}
      assert Enum.at(responses, 2) == {:simple, "QUEUED"}
      assert Enum.at(responses, 3) == {:simple, "OK"}
      # GET after DISCARD should see "original"
      assert Enum.at(responses, 4) == "original"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Cross-shard concurrency tests
  # ---------------------------------------------------------------------------

  describe "cross-shard concurrency" do
    test "commands on different keys execute concurrently", %{port: port} do
      sock = connect_and_hello(port)

      # Create keys that are likely to land on different shards.
      # Use a large number to increase probability of hitting different shards.
      keys = for i <- 1..10, do: ukey("xshard_#{i}")
      values = for i <- 1..10, do: "val_#{i}"

      # Pipeline: 10 SETs then 10 GETs
      set_cmds =
        Enum.zip(keys, values) |> Enum.map(fn {k, v} -> Encoder.encode(["SET", k, v]) end)

      get_cmds = Enum.map(keys, fn k -> Encoder.encode(["GET", k]) end)

      pipeline = IO.iodata_to_binary(set_cmds ++ get_cmds)
      :ok = :gen_tcp.send(sock, pipeline)
      responses = recv_n(sock, 20)

      # All SETs should return OK
      for i <- 0..9 do
        assert Enum.at(responses, i) == {:simple, "OK"}
      end

      # All GETs should return the correct values
      for {v, i} <- Enum.with_index(values) do
        assert Enum.at(responses, 10 + i) == v
      end

      :gen_tcp.close(sock)
    end

    test "barrier command (MGET) waits for preceding SETs", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("barrier_a")
      k2 = ukey("barrier_b")

      # Pipeline: SET k1, SET k2, MGET k1 k2
      # MGET must see both SETs
      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["SET", k1, "one"]),
          Encoder.encode(["SET", k2, "two"]),
          Encoder.encode(["MGET", k1, k2])
        ])

      :ok = :gen_tcp.send(sock, pipeline)
      responses = recv_n(sock, 3)

      assert Enum.at(responses, 0) == {:simple, "OK"}
      assert Enum.at(responses, 1) == {:simple, "OK"}
      assert Enum.at(responses, 2) == ["one", "two"]

      :gen_tcp.close(sock)
    end

    test "DBSIZE after SETs reflects correct count", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("dbsize_a")
      k2 = ukey("dbsize_b")

      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["SET", k1, "1"]),
          Encoder.encode(["SET", k2, "2"]),
          Encoder.encode(["DBSIZE"])
        ])

      :ok = :gen_tcp.send(sock, pipeline)
      responses = recv_n(sock, 3)

      assert Enum.at(responses, 0) == {:simple, "OK"}
      assert Enum.at(responses, 1) == {:simple, "OK"}
      dbsize = Enum.at(responses, 2)
      assert is_integer(dbsize)
      assert dbsize >= 2

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Error handling in pipeline
  # ---------------------------------------------------------------------------

  describe "error handling in pipeline" do
    test "error response in pipeline doesn't break subsequent commands", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("error_pipe")

      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["SET", k, "val"]),
          Encoder.encode(["GET"]),
          Encoder.encode(["GET", k])
        ])

      :ok = :gen_tcp.send(sock, pipeline)
      responses = recv_n(sock, 3)

      assert Enum.at(responses, 0) == {:simple, "OK"}
      assert match?({:error, _}, Enum.at(responses, 1))
      assert Enum.at(responses, 2) == "val"

      :gen_tcp.close(sock)
    end

    test "unknown command in pipeline doesn't affect other commands", %{port: port} do
      sock = connect_and_hello(port)

      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["PING"]),
          Encoder.encode(["NOSUCHCMD"]),
          Encoder.encode(["PING"])
        ])

      :ok = :gen_tcp.send(sock, pipeline)
      responses = recv_n(sock, 3)

      assert Enum.at(responses, 0) == {:simple, "PONG"}
      assert match?({:error, _}, Enum.at(responses, 1))
      assert Enum.at(responses, 2) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end
  end
end
