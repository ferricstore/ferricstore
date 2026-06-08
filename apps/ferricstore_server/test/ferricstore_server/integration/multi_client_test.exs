defmodule FerricstoreServer.Integration.MultiClientTest do
  @moduledoc false
  use ExUnit.Case, async: false
  @moduletag :integration

  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------


  defp connect(port) do
    {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])
    sock
  end

  defp send_cmd(sock, cmd) do
    :ok = :gen_tcp.send(sock, IO.iodata_to_binary(Encoder.encode(cmd)))
  end

  defp send_raw(sock, data) do
    :gen_tcp.send(sock, data)
  end

  defp recv_one(sock, buf \\ "") do
    case :gen_tcp.recv(sock, 0, 10_000) do
      {:ok, data} ->
        buf2 = buf <> data

        case Parser.parse(buf2) do
          {:ok, [val | _rest], _rem} -> val
          {:ok, [], _rem} -> recv_one(sock, buf2)
        end

      {:error, :closed} ->
        :closed

      {:error, :timeout} ->
        {:error, :timeout}
    end
  end

  defp recv_n_responses(sock, n), do: do_recv_n_responses(sock, n, "", [])
  defp do_recv_n_responses(_sock, 0, _buf, acc), do: acc

  defp do_recv_n_responses(sock, remaining, buf, acc) when remaining > 0 do
    case :gen_tcp.recv(sock, 0, 5_000) do
      {:ok, data} ->
        buf2 = buf <> data

        case Parser.parse(buf2) do
          {:ok, vals, rest} when vals != [] ->
            taken = Enum.take(vals, remaining)
            new_acc = acc ++ taken
            new_remaining = remaining - length(taken)
            do_recv_n_responses(sock, new_remaining, rest, new_acc)

          {:ok, [], _} ->
            do_recv_n_responses(sock, remaining, buf2, acc)
        end

      {:error, :closed} ->
        acc
    end
  end

  defp hello3(sock) do
    send_cmd(sock, ["HELLO", "3"])
    recv_one(sock)
  end

  defp connect_and_hello(port) do
    sock = connect(port)
    hello3(sock)
    sock
  end

  defp ukey(name), do: "intg_#{name}_#{:rand.uniform(9_999_999)}"

  defp closed_or_eof?(sock) do
    case :gen_tcp.recv(sock, 0, 1_000) do
      {:error, :closed} -> true
      {:error, :econnreset} -> true
      {:error, :einval} -> true
      {:error, :enotconn} -> true
      {:ok, ""} -> true
      {:ok, _data} -> false
    end
  end

  # ---------------------------------------------------------------------------
  # Setup — start listener once for all tests in this module
  # ---------------------------------------------------------------------------

  setup_all do
    # The application supervisor already starts the Ranch listener.
    # Discover the actual bound port (ephemeral in test env).
    %{port: Listener.port()}
  end

  # Flush keys between tests to avoid cross-test contamination
  setup %{port: port} do
    sock = connect_and_hello(port)
    send_cmd(sock, ["FLUSHDB"])
    recv_one(sock)
    :gen_tcp.close(sock)
    :ok
  end

  # ===========================================================================
  # Multiple concurrent clients
  # ===========================================================================

  describe "multiple concurrent clients" do
    test "10 clients can connect and exchange data simultaneously", %{port: port} do
      results =
        1..10
        |> Task.async_stream(
          fn i ->
            sock = connect_and_hello(port)
            key = "mc10_#{i}"
            val = "value_#{i}"

            send_cmd(sock, ["SET", key, val])
            assert {:simple, "OK"} == recv_one(sock)

            send_cmd(sock, ["GET", key])
            assert ^val = recv_one(sock)

            :gen_tcp.close(sock)
            :ok
          end,
          max_concurrency: 10,
          timeout: 10_000
        )
        |> Enum.to_list()

      assert length(results) == 10
      assert Enum.all?(results, &match?({:ok, :ok}, &1))
    end

    test "concurrent SET from different clients — no data corruption", %{port: port} do
      # 20 clients each SET a unique key concurrently
      results =
        1..20
        |> Task.async_stream(
          fn i ->
            sock = connect_and_hello(port)
            send_cmd(sock, ["SET", "cset_#{i}", "cval_#{i}"])
            {:simple, "OK"} = recv_one(sock)
            :gen_tcp.close(sock)
            :ok
          end,
          max_concurrency: 20,
          timeout: 10_000
        )
        |> Enum.to_list()

      assert Enum.all?(results, &match?({:ok, :ok}, &1))

      # Verify all 20 keys have correct values
      sock = connect_and_hello(port)

      for i <- 1..20 do
        send_cmd(sock, ["GET", "cset_#{i}"])
        assert "cval_#{i}" == recv_one(sock)
      end

      :gen_tcp.close(sock)
    end

    test "one client writing while others read", %{port: port} do
      key = ukey("rw")

      # Pre-seed the key so readers always have something to find
      seed_sock = connect_and_hello(port)
      send_cmd(seed_sock, ["SET", key, "init"])
      {:simple, "OK"} = recv_one(seed_sock)
      :gen_tcp.close(seed_sock)

      writer_task =
        Task.async(fn ->
          sock = connect_and_hello(port)

          for i <- 1..50 do
            send_cmd(sock, ["SET", key, "w_#{i}"])
            {:simple, "OK"} = recv_one(sock)
          end

          :gen_tcp.close(sock)
          :ok
        end)

      reader_results =
        1..5
        |> Task.async_stream(
          fn _r ->
            sock = connect_and_hello(port)

            for _i <- 1..50 do
              send_cmd(sock, ["GET", key])
              val = recv_one(sock)
              # Value must be a string (either "init" or one of the writer values)
              assert is_binary(val)
            end

            :gen_tcp.close(sock)
            :ok
          end,
          max_concurrency: 5,
          timeout: 30_000
        )
        |> Enum.to_list()

      assert Task.await(writer_task, 30_000) == :ok
      assert Enum.all?(reader_results, &match?({:ok, :ok}, &1))
    end

    test "50 clients each do SET + GET", %{port: port} do
      results =
        1..50
        |> Task.async_stream(
          fn i ->
            sock = connect_and_hello(port)
            key = "k50_#{i}"
            val = "v50_#{i}"

            send_cmd(sock, ["SET", key, val])
            assert {:simple, "OK"} == recv_one(sock)

            send_cmd(sock, ["GET", key])
            assert ^val = recv_one(sock)

            :gen_tcp.close(sock)
            :ok
          end,
          max_concurrency: 50,
          timeout: 30_000
        )
        |> Enum.to_list()

      assert length(results) == 50
      assert Enum.all?(results, &match?({:ok, :ok}, &1))
    end

    test "clients survive slow reads (TCP backpressure)", %{port: port} do
      results =
        1..5
        |> Task.async_stream(
          fn client_id ->
            sock = connect_and_hello(port)

            # Build a 100-command pipeline
            pipeline =
              1..100
              |> Enum.map(fn i ->
                Encoder.encode(["PING", "bp_#{client_id}_#{i}"])
              end)
              |> IO.iodata_to_binary()

            # Send the whole pipeline at once
            :ok = send_raw(sock, pipeline)

            # Sleep to let TCP buffers fill — simulating slow consumer
            Process.sleep(500)

            # Now read all 100 responses
            responses = recv_n_responses(sock, 100)
            assert length(responses) == 100

            for i <- 1..100 do
              assert Enum.at(responses, i - 1) == "bp_#{client_id}_#{i}"
            end

            :gen_tcp.close(sock)
            :ok
          end,
          max_concurrency: 5,
          timeout: 30_000
        )
        |> Enum.to_list()

      assert Enum.all?(results, &match?({:ok, :ok}, &1))
    end
  end

  # ===========================================================================
  # Protocol robustness
  # ===========================================================================

  describe "protocol robustness" do
    test "server handles malformed RESP gracefully", %{port: port} do
      # Connect and send garbage bytes terminated with \r\n so the server's
      # inline parser can attempt to process them (without \r\n the parser
      # would treat the bytes as an incomplete frame and wait forever).
      sock = connect(port)
      :ok = send_raw(sock, <<0xFF, 0xFE, 0x01, "\r\n">>)

      # The server should either close the connection or return an error.
      case :gen_tcp.recv(sock, 0, 2_000) do
        {:ok, data} ->
          # Server sent back an error response (simple error starts with -)
          assert String.starts_with?(data, "-")

        {:error, reason} ->
          # Server closed the connection
          assert reason in [:closed, :econnreset]
      end

      :gen_tcp.close(sock)

      # Confirm server is still alive by opening a fresh connection
      fresh = connect_and_hello(port)
      send_cmd(fresh, ["PING"])
      assert {:simple, "PONG"} == recv_one(fresh)
      :gen_tcp.close(fresh)
    end

    test "server handles partial RESP frame (TCP fragmentation)", %{port: port} do
      sock = connect_and_hello(port)

      # SET mykey myval encoded: "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nmyval\r\n"
      full = IO.iodata_to_binary(Encoder.encode(["SET", "frag_key", "frag_val"]))

      # Split into 3 fragments
      {chunk1, rest1} = String.split_at(full, div(byte_size(full), 3))
      {chunk2, chunk3} = String.split_at(rest1, div(byte_size(rest1), 2))

      :ok = send_raw(sock, chunk1)
      Process.sleep(10)
      :ok = send_raw(sock, chunk2)
      Process.sleep(10)
      :ok = send_raw(sock, chunk3)

      assert {:simple, "OK"} == recv_one(sock)

      # Verify the value was stored correctly
      send_cmd(sock, ["GET", "frag_key"])
      assert "frag_val" == recv_one(sock)

      :gen_tcp.close(sock)
    end

    test "very large pipeline (500 commands) all get responses", %{port: port} do
      sock = connect_and_hello(port)

      pipeline =
        1..500
        |> Enum.map(fn _ -> Encoder.encode(["PING"]) end)
        |> IO.iodata_to_binary()

      :ok = send_raw(sock, pipeline)

      responses = recv_n_responses(sock, 500)
      assert length(responses) == 500
      assert Enum.all?(responses, &(&1 == {:simple, "PONG"}))

      :gen_tcp.close(sock)
    end

    test "pipeline with mixed valid and invalid commands", %{port: port} do
      sock = connect_and_hello(port)

      key = ukey("mixed")

      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["SET", key, "mixval"]),
          Encoder.encode(["BADCMD"]),
          Encoder.encode(["GET", key]),
          Encoder.encode(["ANOTHERBAD"]),
          Encoder.encode(["EXISTS", key])
        ])

      :ok = send_raw(sock, pipeline)

      responses = recv_n_responses(sock, 5)
      assert length(responses) == 5

      # SET -> OK
      assert {:simple, "OK"} == Enum.at(responses, 0)

      # BADCMD -> error
      assert {:error, _} = Enum.at(responses, 1)

      # GET -> value
      assert "mixval" == Enum.at(responses, 2)

      # ANOTHERBAD -> error
      assert {:error, _} = Enum.at(responses, 3)

      # EXISTS -> 1
      assert 1 == Enum.at(responses, 4)

      # Connection still alive
      send_cmd(sock, ["PING"])
      assert {:simple, "PONG"} == recv_one(sock)

      :gen_tcp.close(sock)
    end

    test "HELLO 3 can be sent multiple times (re-negotiation)", %{port: port} do
      sock = connect(port)

      # First HELLO 3
      greeting1 = hello3(sock)
      assert is_map(greeting1)
      assert greeting1["server"] == "ferricstore"

      # SET a key
      send_cmd(sock, ["SET", "reneg_k", "reneg_v"])
      assert {:simple, "OK"} == recv_one(sock)

      # Second HELLO 3 (re-negotiation)
      greeting2 = hello3(sock)
      assert is_map(greeting2)
      assert greeting2["server"] == "ferricstore"

      # State is preserved — GET the key
      send_cmd(sock, ["GET", "reneg_k"])
      assert "reneg_v" == recv_one(sock)

      :gen_tcp.close(sock)
    end

    test "connection stays alive after receiving ERR response", %{port: port} do
      sock = connect_and_hello(port)

      # 1) Unknown command -> error
      send_cmd(sock, ["UNKNOWNCMD"])
      resp1 = recv_one(sock)
      assert {:error, _} = resp1

      # 2) PING -> PONG
      send_cmd(sock, ["PING"])
      assert {:simple, "PONG"} == recv_one(sock)

      # 3) SET k v -> OK
      key = ukey("alive")
      send_cmd(sock, ["SET", key, "alive_val"])
      assert {:simple, "OK"} == recv_one(sock)

      # 4) GET k -> value
      send_cmd(sock, ["GET", key])
      assert "alive_val" == recv_one(sock)

      :gen_tcp.close(sock)
    end

    test "empty pipeline (no commands) is handled", %{port: port} do
      sock = connect_and_hello(port)

      # Send empty bytes — should not crash the server
      :ok = send_raw(sock, "")

      # Connection should still work
      send_cmd(sock, ["PING"])
      assert {:simple, "PONG"} == recv_one(sock)

      :gen_tcp.close(sock)
    end

    test "QUIT in middle of pipeline closes after OK", %{port: port} do
      sock = connect_and_hello(port)

      key = ukey("quit_mid")

      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["SET", key, "qval"]),
          Encoder.encode(["QUIT"]),
          Encoder.encode(["GET", key])
        ])

      :ok = send_raw(sock, pipeline)

      # We should get at least SET OK and QUIT OK
      # The GET may or may not be processed — connection closes after QUIT
      responses = recv_n_responses(sock, 2)

      assert {:simple, "OK"} == Enum.at(responses, 0)
      assert {:simple, "OK"} == Enum.at(responses, 1)

      # Connection should be closed — GET k should NOT be processed
      assert closed_or_eof?(sock)

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # Data isolation between clients
  # ===========================================================================

  describe "data isolation between clients" do
    test "client A writes key, client B reads it", %{port: port} do
      key = ukey("shared")

      sock_a = connect_and_hello(port)
      sock_b = connect_and_hello(port)

      send_cmd(sock_a, ["SET", key, "from_A"])
      assert {:simple, "OK"} == recv_one(sock_a)

      send_cmd(sock_b, ["GET", key])
      assert "from_A" == recv_one(sock_b)

      :gen_tcp.close(sock_a)
      :gen_tcp.close(sock_b)
    end

    test "client A deletes key, client B sees it gone", %{port: port} do
      key = ukey("delshare")

      sock_a = connect_and_hello(port)
      sock_b = connect_and_hello(port)

      # A sets the key
      send_cmd(sock_a, ["SET", key, "exists"])
      assert {:simple, "OK"} == recv_one(sock_a)

      # B sees it
      send_cmd(sock_b, ["GET", key])
      assert "exists" == recv_one(sock_b)

      # A deletes it
      send_cmd(sock_a, ["DEL", key])
      assert 1 == recv_one(sock_a)

      # B sees it gone
      send_cmd(sock_b, ["GET", key])
      assert nil == recv_one(sock_b)

      :gen_tcp.close(sock_a)
      :gen_tcp.close(sock_b)
    end

    test "MSET from client A, MGET from client B", %{port: port} do
      k1 = ukey("mset1")
      k2 = ukey("mset2")
      k3 = ukey("mset3")

      sock_a = connect_and_hello(port)
      sock_b = connect_and_hello(port)

      send_cmd(sock_a, ["MSET", k1, "v1", k2, "v2", k3, "v3"])
      assert {:simple, "OK"} == recv_one(sock_a)

      send_cmd(sock_b, ["MGET", k1, k2, k3])
      result = recv_one(sock_b)
      assert result == ["v1", "v2", "v3"]

      :gen_tcp.close(sock_a)
      :gen_tcp.close(sock_b)
    end
  end

  # ===========================================================================
  # Connection lifecycle
  # ===========================================================================

  describe "connection lifecycle" do
    test "server accepts new connections after previous ones close", %{port: port} do
      # Open 5, close all
      socks1 = for _ <- 1..5, do: connect_and_hello(port)
      Enum.each(socks1, &:gen_tcp.close/1)

      # Small pause for server to clean up
      Process.sleep(50)

      # Open 5 more — all should work
      socks2 = for _ <- 1..5, do: connect_and_hello(port)

      for sock <- socks2 do
        send_cmd(sock, ["PING"])
        assert {:simple, "PONG"} == recv_one(sock)
      end

      Enum.each(socks2, &:gen_tcp.close/1)
    end

    test "abrupt disconnect does not affect server", %{port: port} do
      sock1 = connect_and_hello(port)
      sock2 = connect_and_hello(port)
      sock3 = connect_and_hello(port)

      # Abruptly close sock2 without QUIT
      :gen_tcp.close(sock2)

      Process.sleep(50)

      # sock1 and sock3 still work
      send_cmd(sock1, ["PING"])
      assert {:simple, "PONG"} == recv_one(sock1)

      send_cmd(sock3, ["PING"])
      assert {:simple, "PONG"} == recv_one(sock3)

      :gen_tcp.close(sock1)
      :gen_tcp.close(sock3)
    end

    test "server handles 100 sequential connect-PING-disconnect cycles", %{port: port} do
      for _ <- 1..100 do
        sock = connect_and_hello(port)
        send_cmd(sock, ["PING"])
        assert {:simple, "PONG"} == recv_one(sock)
        :gen_tcp.close(sock)
      end
    end

    test "connection with no activity and then command", %{port: port} do
      sock = connect_and_hello(port)

      # 500ms of no activity
      Process.sleep(500)

      send_cmd(sock, ["PING"])
      assert {:simple, "PONG"} == recv_one(sock)

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # RESP3 response types over TCP
  # ===========================================================================

  describe "RESP3 response types over TCP" do
    test "integer response decoded correctly", %{port: port} do
      sock = connect_and_hello(port)

      # DEL on a missing key -> integer 0
      send_cmd(sock, ["DEL", ukey("missing")])
      assert 0 == recv_one(sock)

      :gen_tcp.close(sock)
    end

    test "null bulk string decoded correctly", %{port: port} do
      sock = connect_and_hello(port)

      # GET on a missing key -> nil
      send_cmd(sock, ["GET", ukey("nonexistent")])
      assert nil == recv_one(sock)

      :gen_tcp.close(sock)
    end

    test "array response decoded correctly", %{port: port} do
      sock = connect_and_hello(port)

      k1 = ukey("arr1")
      k2 = ukey("arr2")

      send_cmd(sock, ["SET", k1, "a1"])
      recv_one(sock)
      send_cmd(sock, ["SET", k2, "a2"])
      recv_one(sock)

      send_cmd(sock, ["MGET", k1, k2])
      result = recv_one(sock)
      assert is_list(result)
      assert result == ["a1", "a2"]

      :gen_tcp.close(sock)
    end

    test "map response decoded correctly", %{port: port} do
      sock = connect(port)

      # HELLO 3 returns a map
      greeting = hello3(sock)
      assert is_map(greeting)
      assert Map.has_key?(greeting, "server")
      assert Map.has_key?(greeting, "version")
      assert Map.has_key?(greeting, "proto")
      assert greeting["proto"] == 3

      :gen_tcp.close(sock)
    end

    test "error response decoded correctly", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["BADCMD"])
      resp = recv_one(sock)
      assert {:error, msg} = resp
      assert is_binary(msg)
      assert String.contains?(msg, "ERR")

      :gen_tcp.close(sock)
    end
  end
end
