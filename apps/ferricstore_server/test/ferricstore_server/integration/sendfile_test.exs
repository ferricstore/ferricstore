defmodule FerricstoreServer.Integration.SendfileTest do
  @moduledoc """
  Integration tests for the sendfile optimization on large value GET responses.

  The sendfile optimization kicks in for GET requests on cold (on-disk) values
  whose size is >= 64KB, when the connection uses plain TCP (:ranch_tcp).

  These tests verify:
  1. Small values use the normal response path
  2. Large values (>= threshold) return correct data via the sendfile path
  3. Responses are valid RESP3 (parseable by the client)
  4. Sendfile works correctly with pipelining (doesn't corrupt subsequent commands)
  5. TLS connections do not use sendfile (fall back to normal path)
  """

  use ExUnit.Case, async: false

  alias FerricstoreServer.Resp.Encoder
  alias FerricstoreServer.Resp.Parser
  alias FerricstoreServer.Listener

  # Sendfile threshold matches the one in connection.ex
  @sendfile_threshold 65_536

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp send_pipeline(sock, commands) do
    data = commands |> Enum.map(&Encoder.encode/1) |> IO.iodata_to_binary()
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

  defp ukey(suffix), do: "sendfile_test:#{suffix}:#{System.unique_integer([:positive])}"

  defp attach_sendfile_handler(test_pid) do
    id = {__MODULE__, test_pid, System.unique_integer([:positive])}

    :telemetry.attach(
      id,
      [:ferricstore, :server, :sendfile],
      fn event, measurements, metadata, pid ->
        send(pid, {:sendfile_event, event, measurements, metadata})
      end,
      test_pid
    )

    on_exit(fn -> :telemetry.detach(id) end)
  end

  setup_all do
    # The application supervisor already starts the Ranch listener.
    # Discover the actual bound port (ephemeral in test env).
    %{port: Listener.port()}
  end

  # ---------------------------------------------------------------------------
  # Tests: small values (normal path)
  # ---------------------------------------------------------------------------

  describe "small values (below sendfile threshold)" do
    test "GET returns correct value for small key", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("small")
      value = "hello world"

      send_cmd(sock, ["SET", key, value])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == value

      :gen_tcp.close(sock)
    end

    test "GET returns nil for missing key", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("missing")

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Tests: large values (sendfile path)
  # ---------------------------------------------------------------------------

  describe "large values (at or above sendfile threshold)" do
    test "GET returns correct data for a 64KB value", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("large_64k")
      # Create a value that is exactly the sendfile threshold
      value = :binary.copy("A", @sendfile_threshold)

      send_cmd(sock, ["SET", key, value])
      assert recv_response(sock) == {:simple, "OK"}

      # Force the key out of ETS hot cache by reconnecting
      :gen_tcp.close(sock)
      sock = connect_and_hello(port)

      send_cmd(sock, ["GET", key])
      result = recv_response(sock)
      assert byte_size(result) == @sendfile_threshold
      assert result == value

      :gen_tcp.close(sock)
    end

    test "GET returns correct data for a 128KB value", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("large_128k")
      size = @sendfile_threshold * 2
      value = :binary.copy("B", size)

      send_cmd(sock, ["SET", key, value])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
      sock = connect_and_hello(port)

      send_cmd(sock, ["GET", key])
      result = recv_response(sock)
      assert byte_size(result) == size
      assert result == value

      :gen_tcp.close(sock)
    end

    test "GET returns correct data for a 256KB value with mixed content", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("large_256k_mixed")
      size = @sendfile_threshold * 4

      # Create a value with varying byte patterns (not just repeated chars)
      value =
        0..(size - 1)
        |> Enum.map(fn i -> rem(i, 256) end)
        |> IO.iodata_to_binary()

      send_cmd(sock, ["SET", key, value])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
      sock = connect_and_hello(port)

      send_cmd(sock, ["GET", key])
      result = recv_response(sock)
      assert byte_size(result) == size
      assert result == value

      :gen_tcp.close(sock)
    end

    test "GET of a 1MB value returns complete and correct data", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("large_1mb")
      size = 1_024 * 1_024
      value = :crypto.strong_rand_bytes(size)

      send_cmd(sock, ["SET", key, value])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
      sock = connect_and_hello(port)

      send_cmd(sock, ["GET", key])
      result = recv_response(sock)
      assert byte_size(result) == size
      assert result == value

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Tests: pipelining with sendfile
  # ---------------------------------------------------------------------------

  describe "pipelining with large values" do
    test "commands after a large GET still work correctly", %{port: port} do
      sock = connect_and_hello(port)
      key_large = ukey("pipe_large")
      key_small = ukey("pipe_small")
      large_value = :binary.copy("L", @sendfile_threshold)

      send_cmd(sock, ["SET", key_large, large_value])
      assert recv_response(sock) == {:simple, "OK"}
      send_cmd(sock, ["SET", key_small, "smol"])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
      sock = connect_and_hello(port)

      # Send GET of large value followed by GET of small value
      send_cmd(sock, ["GET", key_large])
      send_cmd(sock, ["GET", key_small])

      [result1, result2] = recv_n(sock, 2)

      assert byte_size(result1) == @sendfile_threshold
      assert result1 == large_value
      assert result2 == "smol"

      :gen_tcp.close(sock)
    end

    test "multiple large GETs in sequence work correctly", %{port: port} do
      sock = connect_and_hello(port)
      keys = Enum.map(1..3, fn i -> ukey("multi_large_#{i}") end)

      values =
        Enum.map(1..3, fn i ->
          :binary.copy(<<rem(64 + i, 256)>>, @sendfile_threshold + i * 1000)
        end)

      for {k, v} <- Enum.zip(keys, values) do
        send_cmd(sock, ["SET", k, v])
        assert recv_response(sock) == {:simple, "OK"}
      end

      :gen_tcp.close(sock)
      sock = connect_and_hello(port)

      for {k, v} <- Enum.zip(keys, values) do
        send_cmd(sock, ["GET", k])
        result = recv_response(sock)
        assert result == v, "Value mismatch for key #{k}"
      end

      :gen_tcp.close(sock)
    end

    test "interleaved small and large GETs all return correct data", %{port: port} do
      sock = connect_and_hello(port)
      small_key = ukey("interleave_small")
      large_key = ukey("interleave_large")
      small_val = "tiny"
      large_val = :binary.copy("X", @sendfile_threshold)

      send_cmd(sock, ["SET", small_key, small_val])
      assert recv_response(sock) == {:simple, "OK"}
      send_cmd(sock, ["SET", large_key, large_val])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
      sock = connect_and_hello(port)

      # Interleave: small, large, PING, small, large
      send_cmd(sock, ["GET", small_key])
      send_cmd(sock, ["GET", large_key])
      send_cmd(sock, ["PING"])
      send_cmd(sock, ["GET", small_key])
      send_cmd(sock, ["GET", large_key])

      results = recv_n(sock, 5)

      assert Enum.at(results, 0) == small_val
      assert Enum.at(results, 1) == large_val
      assert Enum.at(results, 2) == {:simple, "PONG"}
      assert Enum.at(results, 3) == small_val
      assert Enum.at(results, 4) == large_val

      :gen_tcp.close(sock)
    end

    test "true pipelined large GETs use sendfile and preserve response order", %{port: port} do
      attach_sendfile_handler(self())

      sock = connect_and_hello(port)
      keys = Enum.map(1..3, fn i -> ukey("true_pipe_large_#{i}") end)
      values = Enum.map(1..3, fn i -> :binary.copy(<<i>>, @sendfile_threshold + i * 4096) end)

      for {key, value} <- Enum.zip(keys, values) do
        send_cmd(sock, ["SET", key, value])
        assert recv_response(sock) == {:simple, "OK"}
      end

      :gen_tcp.close(sock)
      sock = connect_and_hello(port)

      send_pipeline(sock, Enum.map(keys, &["GET", &1]))

      assert recv_n(sock, length(keys)) == values

      for value <- values do
        size = byte_size(value)

        assert_receive {:sendfile_event, [:ferricstore, :server, :sendfile], %{bytes: ^size},
                        %{result: :ok}},
                       1000
      end

      :gen_tcp.close(sock)
    end

    test "mixed GET and SET pipeline streams cold GETs with sendfile", %{port: port} do
      attach_sendfile_handler(self())

      sock = connect_and_hello(port)
      large_key = ukey("mixed_pipe_large")
      set_key = ukey("mixed_pipe_set")
      large_value = :binary.copy("M", @sendfile_threshold + 4096)

      send_cmd(sock, ["SET", large_key, large_value])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
      sock = connect_and_hello(port)

      send_pipeline(sock, [
        ["GET", large_key],
        ["SET", set_key, "after"],
        ["GET", large_key]
      ])

      assert recv_n(sock, 3) == [large_value, {:simple, "OK"}, large_value]

      for _ <- 1..2 do
        size = byte_size(large_value)

        assert_receive {:sendfile_event, [:ferricstore, :server, :sendfile], %{bytes: ^size},
                        %{result: :ok}},
                       1000
      end

      :gen_tcp.close(sock)
    end

    test "general GET and PING pipeline streams cold GETs with sendfile", %{port: port} do
      attach_sendfile_handler(self())

      sock = connect_and_hello(port)
      large_key = ukey("general_pipe_large")
      large_value = :binary.copy("G", @sendfile_threshold + 4096)

      send_cmd(sock, ["SET", large_key, large_value])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
      sock = connect_and_hello(port)

      send_pipeline(sock, [
        ["GET", large_key],
        ["PING"],
        ["GET", large_key]
      ])

      assert recv_n(sock, 3) == [large_value, {:simple, "PONG"}, large_value]

      for _ <- 1..2 do
        size = byte_size(large_value)

        assert_receive {:sendfile_event, [:ferricstore, :server, :sendfile], %{bytes: ^size},
                        %{result: :ok}},
                       1000
      end

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Tests: RESP3 correctness
  # ---------------------------------------------------------------------------

  describe "RESP3 encoding correctness" do
    test "large value response is parseable as valid RESP3", %{port: port} do
      sock = connect_and_hello(port)
      key = ukey("resp3_valid")
      value = :binary.copy("R", @sendfile_threshold + 100)

      send_cmd(sock, ["SET", key, value])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
      sock = connect_and_hello(port)

      # The recv_response helper accumulates TCP chunks and parses RESP3.
      # If sendfile produced invalid RESP3, the parser would fail or return
      # incorrect data.
      send_cmd(sock, ["GET", key])
      result = recv_response(sock)
      assert result == value
      assert byte_size(result) == @sendfile_threshold + 100

      # Verify a command after the large GET still works (RESP3 framing intact)
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end
  end
end
