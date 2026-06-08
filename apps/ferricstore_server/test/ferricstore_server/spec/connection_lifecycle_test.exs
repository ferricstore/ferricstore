defmodule FerricstoreServer.Spec.ConnectionLifecycleTest do
  @moduledoc """
  Connection lifecycle tests: verify that the server handles client misbehaviour
  (abrupt disconnects, partial writes, garbage input, rapid connect/disconnect)
  without crashing, leaking state, or affecting other connections.

  Each test uses raw `:gen_tcp` for the misbehaving client, then opens a fresh
  RESP3 connection to prove the server is still healthy.
  """

  use ExUnit.Case, async: false
  @moduletag :shard_kill

  alias Ferricstore.Config
  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias Ferricstore.Test.ShardHelpers
  alias FerricstoreServer.Listener

  # ---------------------------------------------------------------------------
  # Helpers — raw TCP (misbehaving client)
  # ---------------------------------------------------------------------------

  defp raw_connect(port) do
    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])

    sock
  end

  defp raw_send(sock, data), do: :gen_tcp.send(sock, data)

  defp raw_recv(sock, timeout \\ 2_000) do
    :gen_tcp.recv(sock, 0, timeout)
  end

  # ---------------------------------------------------------------------------
  # Helpers — RESP3 (health-check client)
  # ---------------------------------------------------------------------------

  defp send_cmd(sock, cmd) do
    :ok = :gen_tcp.send(sock, IO.iodata_to_binary(Encoder.encode(cmd)))
  end

  defp recv_response(sock, timeout \\ 5_000) do
    recv_response_loop(sock, "", timeout)
  end

  defp recv_response_loop(sock, buf, timeout) do
    case :gen_tcp.recv(sock, 0, timeout) do
      {:ok, data} ->
        buf2 = buf <> data

        case Parser.parse(buf2) do
          {:ok, [val], _rest} -> val
          {:ok, [], _} -> recv_response_loop(sock, buf2, timeout)
        end

      {:error, reason} ->
        {:tcp_error, reason}
    end
  end

  defp connect_and_hello(port) do
    sock = raw_connect(port)
    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  defp assert_server_healthy(port) do
    sock = connect_and_hello(port)
    key = "health_#{:rand.uniform(9_999_999)}"
    send_cmd(sock, ["SET", key, "ok"])
    assert {:simple, "OK"} = recv_response(sock)
    send_cmd(sock, ["GET", key])
    assert "ok" = recv_response(sock)
    send_cmd(sock, ["DEL", key])
    recv_response(sock)
    :gen_tcp.close(sock)
    :ok
  end

  defp closed_or_error?(sock) do
    case :gen_tcp.recv(sock, 0, 1_000) do
      {:error, :closed} -> true
      {:error, :econnreset} -> true
      {:error, :einval} -> true
      {:error, :enotconn} -> true
      {:ok, ""} -> true
      _ -> false
    end
  end

  defp ukey(base), do: "clc_#{base}_#{:rand.uniform(9_999_999)}"

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    ShardHelpers.wait_shards_alive(10_000)
    %{port: Listener.port()}
  end

  setup %{port: port} do
    # Flush keys to avoid cross-test contamination
    sock = connect_and_hello(port)
    send_cmd(sock, ["FLUSHDB"])
    recv_response(sock)
    :gen_tcp.close(sock)
    :ok
  end

  # ===========================================================================
  # 1. Client disconnect mid-command
  # ===========================================================================

  describe "client disconnect mid-command" do
    test "server survives when client sends incomplete RESP and disconnects", %{port: port} do
      sock = raw_connect(port)

      # Send HELLO 3 to initialise the connection
      raw_send(sock, IO.iodata_to_binary(Encoder.encode(["HELLO", "3"])))
      {:ok, _greeting} = raw_recv(sock)

      # Send the start of a bulk string command but never finish it:
      # "*2\r\n$3\r\nSET\r\n$5\r\n" — the value payload never arrives
      raw_send(sock, "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$10\r\nhello")

      # Abrupt close while the parser is waiting for more data
      :gen_tcp.close(sock)

      ShardHelpers.eventually(
        fn ->
          assert_server_healthy(port)
          true
        end,
        "server should be healthy after mid-command disconnect",
        20,
        50
      )
    end
  end

  # ===========================================================================
  # 2. Client disconnect mid-pipeline
  # ===========================================================================

  describe "client disconnect mid-pipeline" do
    test "server cleans up when client sends pipeline and disconnects before reading", %{
      port: port
    } do
      sock = connect_and_hello(port)

      # Send 5 SET commands as a pipeline
      pipeline =
        1..5
        |> Enum.map(fn i ->
          IO.iodata_to_binary(Encoder.encode(["SET", "pipe_#{i}", "val_#{i}"]))
        end)
        |> IO.iodata_to_binary()

      raw_send(sock, pipeline)

      # Close immediately without reading any responses
      :gen_tcp.close(sock)

      ShardHelpers.eventually(
        fn ->
          assert_server_healthy(port)
          true
        end,
        "server should be healthy after mid-pipeline disconnect",
        20,
        50
      )
    end
  end

  # ===========================================================================
  # 3. Client disconnect during MULTI
  # ===========================================================================

  describe "client disconnect during MULTI" do
    test "server cleans up transaction state on disconnect", %{port: port} do
      sock = connect_and_hello(port)

      # MULTI
      send_cmd(sock, ["MULTI"])
      assert {:simple, "OK"} = recv_response(sock)

      # Queue 3 commands
      for i <- 1..3 do
        send_cmd(sock, ["SET", "{clc_txn}:k#{i}", "v#{i}"])
        assert {:simple, "QUEUED"} = recv_response(sock)
      end

      # Close without EXEC or DISCARD
      :gen_tcp.close(sock)

      # Verify server is healthy and the queued commands were NOT executed
      check = connect_and_hello(port)
      send_cmd(check, ["GET", "{clc_txn}:k1"])
      assert nil == recv_response(check)
      :gen_tcp.close(check)
    end
  end

  # ===========================================================================
  # 4. Client disconnect during blocking command
  # ===========================================================================

  describe "client disconnect during blocking command" do
    test "server cleans up waiter when blocking client disconnects", %{port: port} do
      k = ukey("blpop_dc")

      # Start BLPOP in a separate task, then kill the connection
      task =
        Task.async(fn ->
          sock = connect_and_hello(port)
          send_cmd(sock, ["BLPOP", k, "10"])
          # intentional delay — let BLPOP register as a waiter before closing
          Process.sleep(200)
          :gen_tcp.close(sock)
        end)

      Task.await(task, 5_000)

      # Server should be healthy and the waiter should be cleaned up.
      # Pushing to the key should NOT crash the server.
      check = connect_and_hello(port)
      send_cmd(check, ["RPUSH", k, "after_dc"])
      result = recv_response(check)
      assert is_integer(result)

      send_cmd(check, ["LRANGE", k, "0", "-1"])
      assert ["after_dc"] = recv_response(check)

      :gen_tcp.close(check)
    end
  end

  # ===========================================================================
  # 5. Rapid connect/disconnect
  # ===========================================================================

  describe "rapid connect/disconnect" do
    test "100 rapid connect/close cycles leave server healthy", %{port: port} do
      for _ <- 1..100 do
        sock = raw_connect(port)
        :gen_tcp.close(sock)
      end

      ShardHelpers.eventually(
        fn ->
          assert_server_healthy(port)
          true
        end,
        "server should be healthy after rapid connect/disconnect",
        20,
        50
      )
    end
  end

  # ===========================================================================
  # 6. Idle connection timeout
  # ===========================================================================

  describe "idle connection timeout" do
    # FerricStore does not currently implement an idle connection timeout.
    # This test documents the expected behaviour: an idle connection stays
    # open indefinitely and remains usable after a period of inactivity.
    test "idle connection stays open and is usable after 2s of inactivity", %{port: port} do
      sock = connect_and_hello(port)

      # intentional delay — testing grace/timeout behavior
      Process.sleep(2_000)

      # Connection should still work
      send_cmd(sock, ["PING"])
      assert {:simple, "PONG"} = recv_response(sock)

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # 7. Half-closed socket (FIN from client)
  # ===========================================================================

  describe "half-closed socket" do
    test "server handles shutdown(:write) from client gracefully", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["PING"])
      assert {:simple, "PONG"} = recv_response(sock)

      # Half-close: send FIN to server (client can still read)
      :gen_tcp.shutdown(sock, :write)

      # Server should close its side; reading should return :closed or empty
      assert closed_or_error?(sock)

      :gen_tcp.close(sock)

      ShardHelpers.eventually(
        fn ->
          assert_server_healthy(port)
          true
        end,
        "server should be healthy after half-close",
        20,
        50
      )
    end
  end

  # ===========================================================================
  # 8. Many concurrent connections
  # ===========================================================================

  describe "many concurrent connections" do
    test "50 concurrent connections each doing SET/GET all succeed", %{port: port} do
      results =
        1..50
        |> Task.async_stream(
          fn i ->
            sock = connect_and_hello(port)
            key = "cc_#{i}"
            val = "val_#{i}"

            send_cmd(sock, ["SET", key, val])
            assert {:simple, "OK"} = recv_response(sock)

            send_cmd(sock, ["GET", key])
            assert ^val = recv_response(sock)

            :gen_tcp.close(sock)
            :ok
          end,
          max_concurrency: 50,
          timeout: 15_000
        )
        |> Enum.to_list()

      assert length(results) == 50
      assert Enum.all?(results, &match?({:ok, :ok}, &1))

      assert_server_healthy(port)
    end
  end

  # ===========================================================================
  # 9. Connection after server recovers from shard crash
  # ===========================================================================

  describe "connection after shard crash recovery" do
    @tag timeout: 120_000
    test "new TCP connection works for SET/GET after a shard crash and restart", %{port: port} do
      shard_name = :"Ferricstore.Store.Shard.0"

      # Pre-seed a key on shard 0
      k_before = ShardHelpers.key_for_shard(0)
      sock_pre = connect_and_hello(port)
      send_cmd(sock_pre, ["SET", k_before, "durable"])
      assert {:simple, "OK"} = recv_response(sock_pre)
      :gen_tcp.close(sock_pre)

      # Flush to disk so the value survives the crash
      ShardHelpers.flush_all_shards()

      # Kill shard 0 directly and wait for supervisor to restart it
      pid = Process.whereis(shard_name)
      assert pid != nil, "Shard 0 not found"
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 5_000

      # Poll until the shard process is back and responsive
      poll_until_shard_alive(shard_name, 30_000)

      # New connection: verify the pre-existing value survived and new writes work.
      # The shard may still be recovering its keydir from Bitcask on slow CI,
      # so retry the GET until the value is available.
      sock_post = connect_and_hello(port)

      ShardHelpers.eventually(
        fn ->
          send_cmd(sock_post, ["GET", k_before])
          recv_response(sock_post) == "durable"
        end,
        "pre-crash value should survive shard restart"
      )

      k_after = "after_crash_#{:rand.uniform(999_999)}"
      send_cmd(sock_post, ["SET", k_after, "new_val"])
      assert {:simple, "OK"} = recv_response(sock_post)
      send_cmd(sock_post, ["GET", k_after])
      assert "new_val" = recv_response(sock_post)

      :gen_tcp.close(sock_post)
    end
  end

  defp poll_until_shard_alive(name, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    Enum.reduce_while(Stream.repeatedly(fn -> :poll end), :waiting, fn _, _ ->
      if System.monotonic_time(:millisecond) > deadline do
        raise "Shard #{inspect(name)} did not restart within #{timeout_ms}ms"
      end

      case Process.whereis(name) do
        pid when is_pid(pid) ->
          if Process.alive?(pid) do
            # Make a synchronous call to confirm init/1 has completed
            try do
              GenServer.call(pid, :flush, 5_000)
              {:halt, :ok}
            catch
              :exit, _ ->
                Process.sleep(50)
                {:cont, :waiting}
            end
          else
            Process.sleep(50)
            {:cont, :waiting}
          end

        nil ->
          Process.sleep(50)
          {:cont, :waiting}
      end
    end)
  end

  # ===========================================================================
  # 10. SUBSCRIBE then disconnect
  # ===========================================================================

  describe "SUBSCRIBE then disconnect" do
    test "server cleans up subscription when subscriber disconnects without UNSUBSCRIBE", %{
      port: port
    } do
      channel = "clc_sub_#{:rand.uniform(999_999)}"

      # Subscribe on a connection, then close without unsubscribing
      sub_sock = connect_and_hello(port)
      send_cmd(sub_sock, ["SUBSCRIBE", channel])
      {:push, ["subscribe", ^channel, 1]} = recv_response(sub_sock)
      :gen_tcp.close(sub_sock)

      # Publishing to the channel should NOT crash the server
      pub_sock = connect_and_hello(port)
      send_cmd(pub_sock, ["PUBLISH", channel, "after_dc"])
      result = recv_response(pub_sock)
      # Should be 0 — no subscribers left
      assert result == 0

      # Verify channel has no subscribers via PUBSUB NUMSUB
      send_cmd(pub_sock, ["PUBSUB", "NUMSUB", channel])
      assert [^channel, 0] = recv_response(pub_sock)

      :gen_tcp.close(pub_sock)
    end
  end

  # ===========================================================================
  # 11. AUTH then disconnect
  # ===========================================================================

  describe "AUTH then disconnect" do
    test "no auth state leak when authenticated client disconnects", %{port: port} do
      # Enable requirepass for this test
      Config.set("requirepass", "testpass123")

      on_exit(fn -> Config.set("requirepass", "") end)

      # Connect, authenticate, then disconnect
      sock = connect_and_hello(port)
      send_cmd(sock, ["AUTH", "testpass123"])
      assert {:simple, "OK"} = recv_response(sock)

      # Do a command to confirm auth succeeded
      send_cmd(sock, ["PING"])
      assert {:simple, "PONG"} = recv_response(sock)

      :gen_tcp.close(sock)

      # New connection: should still need to authenticate
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["SET", "auth_test_key", "val"])
      result = recv_response(sock2)
      # Should be rejected because this new connection hasn't authenticated
      assert {:error, msg} = result
      assert msg =~ "NOAUTH" or msg =~ "Authentication required" or msg =~ "auth"

      # Authenticate on the new connection and verify it works
      send_cmd(sock2, ["AUTH", "testpass123"])
      assert {:simple, "OK"} = recv_response(sock2)
      send_cmd(sock2, ["PING"])
      assert {:simple, "PONG"} = recv_response(sock2)

      :gen_tcp.close(sock2)
    end
  end

  # ===========================================================================
  # 12. Binary garbage on connect
  # ===========================================================================

  describe "binary garbage on connect" do
    test "1KB of random bytes as first data does not crash the server", %{port: port} do
      sock = raw_connect(port)

      # Send 1KB of cryptographically random bytes
      garbage = :crypto.strong_rand_bytes(1024)
      raw_send(sock, garbage)

      # Server should either close the connection or return an error
      case raw_recv(sock, 2_000) do
        {:ok, data} ->
          # If the server responded, it should be an error (starts with -)
          assert String.starts_with?(data, "-") or byte_size(data) > 0

        {:error, reason} ->
          assert reason in [:closed, :econnreset, :timeout]
      end

      :gen_tcp.close(sock)

      ShardHelpers.eventually(
        fn ->
          assert_server_healthy(port)
          true
        end,
        "server should be healthy after garbage input",
        20,
        50
      )
    end

    test "garbage followed by valid RESP on new connection works", %{port: port} do
      # Send garbage on first connection
      bad_sock = raw_connect(port)
      raw_send(bad_sock, :crypto.strong_rand_bytes(512))
      :gen_tcp.close(bad_sock)

      # Fresh connection with valid RESP should work perfectly
      good_sock = connect_and_hello(port)
      send_cmd(good_sock, ["SET", "post_garbage", "works"])
      assert {:simple, "OK"} = recv_response(good_sock)
      send_cmd(good_sock, ["GET", "post_garbage"])
      assert "works" = recv_response(good_sock)
      :gen_tcp.close(good_sock)
    end
  end
end
