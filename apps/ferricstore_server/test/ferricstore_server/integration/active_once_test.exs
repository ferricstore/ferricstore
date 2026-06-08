# Suppress function clause grouping warnings (clauses added by different agents)
defmodule FerricstoreServer.Integration.ActiveOnceTest do
  @moduledoc """
  Integration tests verifying the active: :once event-driven TCP connection model
  (spec section 2C.2).

  These tests confirm that switching from blocking `recv` (active: false) to
  event-driven `active: :once` mode preserves correct behavior across all
  connection modes:

    - Basic request/response (PING/PONG, SET/GET)
    - Pipelined commands (multiple commands in one write)
    - Pub/Sub message delivery while subscribed
    - BLPOP waiter notifications from another client
    - Connection cleanup on client disconnect
    - Large payload handling across TCP chunk boundaries

  The key property of `active: :once`: the kernel delivers exactly one
  `{:tcp, socket, data}` message, then switches the socket to passive.
  The process re-arms after handling each batch. This allows the process
  to handle other messages (waiter notifications, pub/sub pushes) between
  TCP reads without risk of mailbox flooding.
  """

  use ExUnit.Case, async: false
  @moduletag :integration
  @moduletag :global_state

  alias Ferricstore.Test.ShardHelpers
  alias FerricstoreServer.Resp.Encoder
  alias FerricstoreServer.Resp.Parser
  alias FerricstoreServer.Listener

  @moduletag timeout: 60_000

  setup do
    ShardHelpers.reset_server_auth_state()

    on_exit(fn ->
      ShardHelpers.reset_server_auth_state()
    end)

    :ok
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp recv_response(sock, timeout \\ 5_000) do
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

      {:error, reason} ->
        {:tcp_error, reason}
    end
  end

  # Receives exactly `n` RESP3 responses from the socket, accumulating
  # partial TCP reads as needed.
  defp recv_n(sock, n, timeout \\ 5_000) do
    do_recv_n(sock, n, "", [], timeout)
  end

  defp do_recv_n(_sock, 0, _buf, acc, _timeout), do: Enum.reverse(acc)

  defp do_recv_n(sock, remaining, buf, acc, timeout) when remaining > 0 do
    case :gen_tcp.recv(sock, 0, timeout) do
      {:ok, data} ->
        buf2 = buf <> data

        case Parser.parse(buf2) do
          {:ok, [_ | _] = vals, rest} ->
            taken = Enum.take(vals, remaining)
            new_acc = Enum.reverse(taken) ++ acc
            new_remaining = remaining - length(taken)
            do_recv_n(sock, new_remaining, rest, new_acc, timeout)

          {:ok, [], _} ->
            do_recv_n(sock, remaining, buf2, acc, timeout)
        end

      {:error, :timeout} ->
        {:error, :timeout, Enum.reverse(acc)}
    end
  end

  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  defp ukey(name), do: "ao_#{name}_#{:rand.uniform(999_999)}"

  defp eventually(fun, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    eventually(fun, deadline, nil)
  end

  defp eventually(fun, deadline, last) do
    case fun.() do
      true ->
        :ok

      other ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("condition was not met, last value: #{inspect(other || last)}")
        else
          Process.sleep(20)
          eventually(fun, deadline, other)
        end
    end
  end

  # Polls PUBSUB NUMSUB until the channel has at least 1 subscriber.
  # Prevents race between SUBSCRIBE returning and PUBLISH finding the subscriber.
  defp wait_for_subscription(port, channel, timeout \\ 3_000) do
    sock = connect_and_hello(port)
    deadline = System.monotonic_time(:millisecond) + timeout

    result =
      Enum.reduce_while(Stream.repeatedly(fn -> :poll end), :waiting, fn _, _ ->
        send_cmd(sock, ["PUBSUB", "NUMSUB", channel])

        case recv_response(sock) do
          [^channel, count] when is_integer(count) and count > 0 ->
            {:halt, :ok}

          _ ->
            if System.monotonic_time(:millisecond) > deadline do
              {:halt, :timeout}
            else
              Process.sleep(20)
              {:cont, :waiting}
            end
        end
      end)

    :gen_tcp.close(sock)
    if result == :timeout, do: raise("subscription not registered within #{timeout}ms")
  end

  # Polls PUBSUB NUMPAT until at least 1 pattern subscription exists.
  defp wait_for_psubscription(port, _pattern, timeout \\ 3_000) do
    sock = connect_and_hello(port)
    deadline = System.monotonic_time(:millisecond) + timeout

    result =
      Enum.reduce_while(Stream.repeatedly(fn -> :poll end), :waiting, fn _, _ ->
        send_cmd(sock, ["PUBSUB", "NUMPAT"])

        case recv_response(sock) do
          count when is_integer(count) and count > 0 ->
            {:halt, :ok}

          _ ->
            if System.monotonic_time(:millisecond) > deadline do
              {:halt, :timeout}
            else
              Process.sleep(20)
              {:cont, :waiting}
            end
        end
      end)

    :gen_tcp.close(sock)
    if result == :timeout, do: raise("psubscription not registered within #{timeout}ms")
  end

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
  # Basic request/response — active: :once delivers data correctly
  # ---------------------------------------------------------------------------

  describe "basic request/response with active: :once" do
    test "PING returns PONG", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end

    test "SET then GET round-trips correctly", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("basic_set_get")

      send_cmd(sock, ["SET", k, "hello_active_once"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "hello_active_once"

      :gen_tcp.close(sock)
    end

    test "multiple sequential commands on same connection", %{port: port} do
      sock = connect_and_hello(port)

      # Execute 20 sequential SET/GET pairs to exercise the re-arm cycle
      for i <- 1..20 do
        k = ukey("seq_#{i}")
        v = "value_#{i}"

        send_cmd(sock, ["SET", k, v])
        assert recv_response(sock) == {:simple, "OK"}

        send_cmd(sock, ["GET", k])
        assert recv_response(sock) == v
      end

      :gen_tcp.close(sock)
    end

    test "ECHO command returns the argument", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ECHO", "active_once_test"])
      assert recv_response(sock) == "active_once_test"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Pipeline — multiple commands in one TCP write
  # ---------------------------------------------------------------------------

  describe "pipeline with active: :once" do
    test "two commands in one TCP write get separate responses", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("pipe_k1")
      k2 = ukey("pipe_k2")

      # Send two SET commands in a single TCP write
      data =
        IO.iodata_to_binary([
          Encoder.encode(["SET", k1, "v1"]),
          Encoder.encode(["SET", k2, "v2"])
        ])

      :ok = :gen_tcp.send(sock, data)

      # Should receive two responses
      responses = recv_n(sock, 2)
      assert responses == [{:simple, "OK"}, {:simple, "OK"}]

      # Verify both values were stored
      send_cmd(sock, ["GET", k1])
      assert recv_response(sock) == "v1"

      send_cmd(sock, ["GET", k2])
      assert recv_response(sock) == "v2"

      :gen_tcp.close(sock)
    end

    test "pipeline of 10 commands returns all 10 responses in order", %{port: port} do
      sock = connect_and_hello(port)

      # Build a pipeline of 10 SET commands
      keys = for i <- 1..10, do: ukey("pipe10_#{i}")

      pipeline_data =
        keys
        |> Enum.with_index(1)
        |> Enum.map(fn {k, i} -> IO.iodata_to_binary(Encoder.encode(["SET", k, "val_#{i}"])) end)
        |> IO.iodata_to_binary()

      :ok = :gen_tcp.send(sock, pipeline_data)

      # All 10 should return OK
      responses = recv_n(sock, 10)
      assert Enum.all?(responses, &(&1 == {:simple, "OK"}))

      # Verify all values by pipelining GETs
      get_pipeline =
        keys
        |> Enum.map(fn k -> IO.iodata_to_binary(Encoder.encode(["GET", k])) end)
        |> IO.iodata_to_binary()

      :ok = :gen_tcp.send(sock, get_pipeline)
      get_responses = recv_n(sock, 10)

      expected = for i <- 1..10, do: "val_#{i}"
      assert get_responses == expected

      :gen_tcp.close(sock)
    end

    test "mixed read/write pipeline preserves ordering", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("pipe_mixed")

      # Pipeline: SET k v1, GET k, SET k v2, GET k
      pipeline_data =
        IO.iodata_to_binary([
          Encoder.encode(["SET", k, "v1"]),
          Encoder.encode(["GET", k]),
          Encoder.encode(["SET", k, "v2"]),
          Encoder.encode(["GET", k])
        ])

      :ok = :gen_tcp.send(sock, pipeline_data)
      responses = recv_n(sock, 4)

      assert Enum.at(responses, 0) == {:simple, "OK"}
      assert Enum.at(responses, 1) == "v1"
      assert Enum.at(responses, 2) == {:simple, "OK"}
      assert Enum.at(responses, 3) == "v2"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Pub/Sub — messages arrive while subscribed (active: :once enables this)
  # ---------------------------------------------------------------------------

  describe "pub/sub with active: :once" do
    test "SUBSCRIBE then receive published message", %{port: port} do
      channel = ukey("pubsub_ch")

      # Client A: subscribe
      sock_a = connect_and_hello(port)
      send_cmd(sock_a, ["SUBSCRIBE", channel])
      sub_resp = recv_response(sock_a)
      assert sub_resp == {:push, ["subscribe", channel, 1]}

      # Wait for subscription to be registered in PubSub before publishing
      wait_for_subscription(port, channel)

      # Client B: publish a message
      sock_b = connect_and_hello(port)
      send_cmd(sock_b, ["PUBLISH", channel, "hello_from_b"])
      pub_count = recv_response(sock_b)
      assert pub_count == 1

      # Client A: should receive the published message
      msg = recv_response(sock_a, 5_000)
      assert msg == {:push, ["message", channel, "hello_from_b"]}

      # Client A: unsubscribe
      send_cmd(sock_a, ["UNSUBSCRIBE", channel])
      unsub_resp = recv_response(sock_a)
      assert unsub_resp == {:push, ["unsubscribe", channel, 0]}

      # Client A: should be back in normal mode — PING should work
      send_cmd(sock_a, ["PING"])
      assert recv_response(sock_a) == {:simple, "PONG"}

      :gen_tcp.close(sock_a)
      :gen_tcp.close(sock_b)
    end

    test "multiple pub/sub messages delivered in sequence", %{port: port} do
      channel = ukey("pubsub_multi")

      # Client A: subscribe
      sock_a = connect_and_hello(port)
      send_cmd(sock_a, ["SUBSCRIBE", channel])
      _sub_resp = recv_response(sock_a)

      # Wait for subscription to be registered before publishing
      wait_for_subscription(port, channel)

      # Client B: publish 5 messages
      sock_b = connect_and_hello(port)

      for i <- 1..5 do
        send_cmd(sock_b, ["PUBLISH", channel, "msg_#{i}"])
        assert recv_response(sock_b) == 1
      end

      # Client A: should receive all 5 messages in order.
      # Messages may arrive in one or more TCP chunks, so use recv_n
      # which accumulates partial reads.
      messages = recv_n(sock_a, 5, 5_000)

      for i <- 1..5 do
        assert Enum.at(messages, i - 1) == {:push, ["message", channel, "msg_#{i}"]}
      end

      :gen_tcp.close(sock_a)
      :gen_tcp.close(sock_b)
    end

    test "PSUBSCRIBE pattern matching works", %{port: port} do
      base = ukey("psub")
      pattern = "#{base}.*"
      channel = "#{base}.news"

      # Client A: pattern subscribe
      sock_a = connect_and_hello(port)
      send_cmd(sock_a, ["PSUBSCRIBE", pattern])
      sub_resp = recv_response(sock_a)
      assert sub_resp == {:push, ["psubscribe", pattern, 1]}

      # Wait for pattern subscription to be registered before publishing
      wait_for_psubscription(port, pattern)

      # Client B: publish to matching channel
      sock_b = connect_and_hello(port)
      send_cmd(sock_b, ["PUBLISH", channel, "pattern_msg"])
      assert recv_response(sock_b) == 1

      # Client A: should receive the pattern-matched message
      msg = recv_response(sock_a, 5_000)
      assert msg == {:push, ["pmessage", pattern, channel, "pattern_msg"]}

      :gen_tcp.close(sock_a)
      :gen_tcp.close(sock_b)
    end
  end

  # ---------------------------------------------------------------------------
  # BLPOP waiter notifications — active: :once allows process to receive
  # waiter_notify messages between TCP reads
  # ---------------------------------------------------------------------------

  describe "BLPOP waiter notifications with active: :once" do
    test "BLPOP wakes when another client pushes", %{port: port} do
      k = ukey("blpop_ao")

      # Client A: BLPOP on empty key (blocks)
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

      # Client B: push a value
      sock_b = connect_and_hello(port)
      send_cmd(sock_b, ["RPUSH", k, "wakeup"])
      assert recv_response(sock_b) == 1
      :gen_tcp.close(sock_b)

      # Client A should wake and receive the value
      result = Task.await(task, 10_000)
      assert result == [k, "wakeup"]
    end

    test "BLPOP timeout returns nil and connection remains usable", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("blpop_timeout_ao")

      send_cmd(sock, ["BLPOP", k, "1"])
      result = recv_response(sock, 5_000)
      assert result == nil

      # Connection should still work after timeout
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end

    test "BLPOP then immediate commands after wake", %{port: port} do
      k = ukey("blpop_then_cmd")

      # Client A: BLPOP on empty key
      task =
        Task.async(fn ->
          sock_a = connect_and_hello(port)
          send_cmd(sock_a, ["BLPOP", k, "5"])
          blpop_result = recv_response(sock_a, 10_000)

          # After BLPOP returns, immediately send more commands
          send_cmd(sock_a, ["SET", k <> "_after", "post_blpop"])
          set_result = recv_response(sock_a)

          send_cmd(sock_a, ["GET", k <> "_after"])
          get_result = recv_response(sock_a)

          :gen_tcp.close(sock_a)
          {blpop_result, set_result, get_result}
        end)

      Process.sleep(200)

      sock_b = connect_and_hello(port)
      send_cmd(sock_b, ["RPUSH", k, "wake_val"])
      assert recv_response(sock_b) == 1
      :gen_tcp.close(sock_b)

      {blpop_result, set_result, get_result} = Task.await(task, 10_000)
      assert blpop_result == [k, "wake_val"]
      assert set_result == {:simple, "OK"}
      assert get_result == "post_blpop"
    end
  end

  # ---------------------------------------------------------------------------
  # Connection cleanup — graceful close detected via active: :once
  # ---------------------------------------------------------------------------

  describe "connection cleanup with active: :once" do
    test "QUIT closes the connection gracefully", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["QUIT"])
      result = recv_response(sock)
      assert result == {:simple, "OK"}

      # The server should have closed the socket
      assert {:error, :closed} == :gen_tcp.recv(sock, 0, 1_000)
    end

    test "client disconnect is handled without error", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("disconnect_test")

      # Write some data
      send_cmd(sock, ["SET", k, "before_close"])
      assert recv_response(sock) == {:simple, "OK"}

      # Abruptly close the client socket
      :gen_tcp.close(sock)

      # Small delay for the server to process the close
      Process.sleep(100)

      # A new connection should work fine — the server is not affected
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["GET", k])
      assert recv_response(sock2) == "before_close"
      :gen_tcp.close(sock2)
    end

    test "blocked BLPOP client disconnect unregisters waiter promptly", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("blocked_disconnect")

      send_cmd(sock, ["BLPOP", k, "5"])
      eventually(fn -> Ferricstore.Waiters.count(k) == 1 end, 1_000)

      :gen_tcp.close(sock)

      eventually(fn -> Ferricstore.Waiters.count(k) == 0 end, 500)
    end

    test "blocked BLPOP enforces buffered input limit while waiting", %{port: port} do
      old_limit =
        Application.get_env(:ferricstore_server, :blocked_command_buffer_max_bytes)

      Application.put_env(:ferricstore_server, :blocked_command_buffer_max_bytes, 64)

      on_exit(fn ->
        if old_limit == nil do
          Application.delete_env(:ferricstore_server, :blocked_command_buffer_max_bytes)
        else
          Application.put_env(:ferricstore_server, :blocked_command_buffer_max_bytes, old_limit)
        end
      end)

      sock = connect_and_hello(port)
      k = ukey("blocked_buffer_limit")

      send_cmd(sock, ["BLPOP", k, "5"])
      eventually(fn -> Ferricstore.Waiters.count(k) == 1 end, 1_000)

      :ok = :gen_tcp.send(sock, :binary.copy("x", 128))

      assert {:error, "ERR blocked command buffer overflow" <> _} = recv_response(sock, 1_000)

      eventually(fn -> Ferricstore.Waiters.count(k) == 0 end, 500)
    end

    test "server handles rapid connect/disconnect cycles", %{port: port} do
      # Open and close 20 connections rapidly
      for _i <- 1..20 do
        sock = connect_and_hello(port)
        send_cmd(sock, ["PING"])
        assert recv_response(sock) == {:simple, "PONG"}
        :gen_tcp.close(sock)
      end

      # Server should still be healthy
      sock = connect_and_hello(port)
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}
      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Large payloads — verify active: :once handles TCP chunking correctly
  # ---------------------------------------------------------------------------

  describe "large payloads with active: :once" do
    test "SET and GET a 100KB value", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("large_100kb")
      large = :binary.copy("X", 100_000)

      send_cmd(sock, ["SET", k, large])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock, 10_000) == large

      :gen_tcp.close(sock)
    end

    test "pipeline with mixed large and small values", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("large_mix_1")
      k2 = ukey("large_mix_2")
      k3 = ukey("large_mix_3")
      large = :binary.copy("L", 50_000)

      # Pipeline: SET k1 (large), SET k2 (small), SET k3 (large)
      pipeline_data =
        IO.iodata_to_binary([
          Encoder.encode(["SET", k1, large]),
          Encoder.encode(["SET", k2, "small"]),
          Encoder.encode(["SET", k3, large])
        ])

      :ok = :gen_tcp.send(sock, pipeline_data)
      responses = recv_n(sock, 3, 10_000)
      assert Enum.all?(responses, &(&1 == {:simple, "OK"}))

      # Verify all values
      send_cmd(sock, ["GET", k1])
      assert recv_response(sock, 10_000) == large

      send_cmd(sock, ["GET", k2])
      assert recv_response(sock) == "small"

      send_cmd(sock, ["GET", k3])
      assert recv_response(sock, 10_000) == large

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Transaction commands — MULTI/EXEC work correctly with active: :once
  # ---------------------------------------------------------------------------

  describe "transactions with active: :once" do
    test "MULTI/EXEC executes queued commands atomically", %{port: port} do
      sock = connect_and_hello(port)
      k1 = ukey("tx_k1")
      k2 = ukey("tx_k2")

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k1, "tx_v1"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["SET", k2, "tx_v2"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      result = recv_response(sock)
      assert result == [{:simple, "OK"}, {:simple, "OK"}]

      # Verify both values were set
      send_cmd(sock, ["GET", k1])
      assert recv_response(sock) == "tx_v1"

      send_cmd(sock, ["GET", k2])
      assert recv_response(sock) == "tx_v2"

      :gen_tcp.close(sock)
    end

    test "DISCARD clears transaction queue", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("tx_discard")

      send_cmd(sock, ["SET", k, "original"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "should_not_persist"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["DISCARD"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "original"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrent connections — active: :once handles many connections
  # ---------------------------------------------------------------------------

  describe "concurrent connections with active: :once" do
    test "10 concurrent connections each doing SET/GET", %{port: port} do
      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            sock = connect_and_hello(port)
            k = ukey("conc_#{i}")

            send_cmd(sock, ["SET", k, "value_#{i}"])
            assert recv_response(sock) == {:simple, "OK"}

            send_cmd(sock, ["GET", k])
            result = recv_response(sock)
            :gen_tcp.close(sock)
            {i, result}
          end)
        end

      results = Task.await_many(tasks, 10_000)

      for {i, result} <- results do
        assert result == "value_#{i}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Interleaved operations — verifies the process can handle different
  # message types between TCP reads (the core benefit of active: :once)
  # ---------------------------------------------------------------------------

  describe "interleaved operations with active: :once" do
    test "pub/sub then normal commands after unsubscribe", %{port: port} do
      channel = ukey("interleave_ch")
      k = ukey("interleave_k")

      sock = connect_and_hello(port)

      # Subscribe
      send_cmd(sock, ["SUBSCRIBE", channel])
      assert recv_response(sock) == {:push, ["subscribe", channel, 1]}

      # Unsubscribe
      send_cmd(sock, ["UNSUBSCRIBE", channel])
      assert recv_response(sock) == {:push, ["unsubscribe", channel, 0]}

      # Normal commands should work
      send_cmd(sock, ["SET", k, "after_pubsub"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "after_pubsub"

      :gen_tcp.close(sock)
    end

    test "BLPOP timeout then normal commands work", %{port: port} do
      sock = connect_and_hello(port)
      k_block = ukey("interleave_block")
      k_after = ukey("interleave_after")

      # BLPOP with 1 second timeout on empty key
      send_cmd(sock, ["BLPOP", k_block, "1"])
      result = recv_response(sock, 5_000)
      assert result == nil

      # Normal commands after BLPOP timeout
      send_cmd(sock, ["SET", k_after, "still_works"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k_after])
      assert recv_response(sock) == "still_works"

      :gen_tcp.close(sock)
    end

    test "transaction then pub/sub then normal commands", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("interleave_all")
      channel = ukey("interleave_all_ch")

      # Transaction
      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", k, "from_tx"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["EXEC"])
      assert recv_response(sock) == [{:simple, "OK"}]

      # Pub/Sub round-trip
      send_cmd(sock, ["SUBSCRIBE", channel])
      assert recv_response(sock) == {:push, ["subscribe", channel, 1]}

      send_cmd(sock, ["UNSUBSCRIBE", channel])
      assert recv_response(sock) == {:push, ["unsubscribe", channel, 0]}

      # Normal GET
      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "from_tx"

      :gen_tcp.close(sock)
    end
  end
end
