defmodule FerricstoreServer.ConnectionTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias FerricstoreServer.Resp.Encoder
  alias FerricstoreServer.Resp.Parser
  alias FerricstoreServer.Listener

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp connect(port) do
    {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])
    sock
  end

  defp send_raw(sock, data), do: :gen_tcp.send(sock, data)

  defp recv(sock, timeout \\ 500) do
    {:ok, data} = :gen_tcp.recv(sock, 0, timeout)
    data
  end

  defp hello3 do
    IO.iodata_to_binary(Encoder.encode(["HELLO", "3"]))
  end

  defp send_command(sock, args) do
    :gen_tcp.send(sock, IO.iodata_to_binary(Encoder.encode(args)))
  end

  # ---------------------------------------------------------------------------
  # Setup / teardown
  # ---------------------------------------------------------------------------

  setup do
    # The application supervisor manages the Ranch listener.
    # Use the actual bound port (ephemeral in test env).
    {:ok, port: Listener.port()}
  end

  # ---------------------------------------------------------------------------
  # TCP connection
  # ---------------------------------------------------------------------------

  test "server accepts TCP connection", %{port: port} do
    sock = connect(port)
    assert is_port(sock)
    :gen_tcp.close(sock)
  end

  test "server accepts multiple simultaneous connections", %{port: port} do
    socks = for _ <- 1..5, do: connect(port)
    Enum.each(socks, &:gen_tcp.close/1)
  end

  # ---------------------------------------------------------------------------
  # HELLO 3 handshake
  # ---------------------------------------------------------------------------

  test "HELLO 3 returns a map greeting", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    data = recv(sock)

    # The greeting must be a RESP3 map response (starts with %)
    assert String.starts_with?(data, "%")
    :gen_tcp.close(sock)
  end

  test "HELLO 3 greeting contains required fields: server, version, proto", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    data = recv(sock)

    {:ok, [greeting], ""} = Parser.parse(data)
    assert is_map(greeting)
    assert Map.has_key?(greeting, "server")
    assert Map.has_key?(greeting, "version")
    assert Map.has_key?(greeting, "proto")
    assert greeting["proto"] == 3
    :gen_tcp.close(sock)
  end

  test "HELLO 3 greeting server name is ferricstore", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    data = recv(sock)

    {:ok, [greeting], ""} = Parser.parse(data)
    assert greeting["server"] == "ferricstore"
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # RESP2 rejection
  # ---------------------------------------------------------------------------

  test "HELLO 2 is rejected with an error", %{port: port} do
    sock = connect(port)
    hello2 = IO.iodata_to_binary(Encoder.encode(["HELLO", "2"]))
    send_raw(sock, hello2)
    data = recv(sock)

    # Must be an error response
    assert String.starts_with?(data, "-") or String.starts_with?(data, "!")
    :gen_tcp.close(sock)
  end

  test "HELLO with unsupported version returns error", %{port: port} do
    sock = connect(port)
    hello_bad = IO.iodata_to_binary(Encoder.encode(["HELLO", "99"]))
    send_raw(sock, hello_bad)
    data = recv(sock)

    assert String.starts_with?(data, "-") or String.starts_with?(data, "!")
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # PING
  # ---------------------------------------------------------------------------

  test "PING without argument returns +PONG", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_command(sock, ["PING"])
    data = recv(sock)
    assert data == "+PONG\r\n"
    :gen_tcp.close(sock)
  end

  test "PING with argument returns bulk string echo", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_command(sock, ["PING", "hello world"])
    data = recv(sock)
    {:ok, [response], ""} = Parser.parse(data)
    assert response == "hello world"
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Inline commands
  # ---------------------------------------------------------------------------

  test "inline PING returns +PONG", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_raw(sock, "PING\r\n")
    data = recv(sock)
    assert data == "+PONG\r\n"
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Pipelined commands
  # ---------------------------------------------------------------------------

  test "pipelined PING commands all receive responses", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    pipeline =
      IO.iodata_to_binary([
        Encoder.encode(["PING"]),
        Encoder.encode(["PING"]),
        Encoder.encode(["PING"])
      ])

    send_raw(sock, pipeline)

    # Collect until we get 3 pong responses
    data = recv_all(sock, "+PONG\r\n", 3)
    count = count_occurrences(data, "+PONG\r\n")
    assert count == 3
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # QUIT
  # ---------------------------------------------------------------------------

  test "QUIT closes the connection after +OK", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_command(sock, ["QUIT"])
    data = recv(sock)
    assert data == "+OK\r\n"

    # Connection should be closed shortly after
    assert closed_or_eof?(sock)
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # RESET
  # ---------------------------------------------------------------------------

  test "RESET returns +RESET and keeps connection open", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_command(sock, ["RESET"])
    data = recv(sock)
    assert data == "+RESET\r\n"

    # Should still accept commands after RESET
    send_command(sock, ["PING"])
    data2 = recv(sock)
    assert data2 == "+PONG\r\n"
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Unknown command
  # ---------------------------------------------------------------------------

  test "unknown command returns error", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_command(sock, ["UNKNOWNCMD"])
    data = recv(sock)
    assert String.starts_with?(data, "-")
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Partial / split reads (TCP fragmentation)
  # ---------------------------------------------------------------------------

  test "command split across multiple TCP packets is handled", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    # Send "*1\r\n$4\r\nPING\r\n" in two fragments
    send_raw(sock, "*1\r\n")
    Process.sleep(10)
    send_raw(sock, "$4\r\nPING\r\n")

    data = recv(sock)
    assert data == "+PONG\r\n"
    :gen_tcp.close(sock)
  end

  test "multiple commands packed in one TCP segment all receive responses", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    packed =
      IO.iodata_to_binary([
        Encoder.encode(["PING"]),
        Encoder.encode(["PING", "check"])
      ])

    send_raw(sock, packed)

    data = recv_at_least(sock, 20, 500)
    {:ok, responses, ""} = Parser.parse(data)
    assert length(responses) == 2
    assert Enum.at(responses, 0) == {:simple, "PONG"}
    assert Enum.at(responses, 1) == "check"
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Connection close without QUIT (abrupt close)
  # ---------------------------------------------------------------------------

  test "server handles abrupt client disconnect gracefully", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)
    :gen_tcp.close(sock)
    # Give the server process time to handle the close — no crash expected
    Process.sleep(50)
  end

  # ---------------------------------------------------------------------------
  # HELLO command edge cases
  # ---------------------------------------------------------------------------

  test "HELLO with no version argument returns greeting", %{port: port} do
    sock = connect(port)
    hello_no_ver = IO.iodata_to_binary(Encoder.encode(["HELLO"]))
    send_raw(sock, hello_no_ver)
    data = recv(sock)

    assert String.starts_with?(data, "%")
    :gen_tcp.close(sock)
  end

  test "HELLO 3 mid-session returns greeting again", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_command(sock, ["PING"])
    _pong = recv(sock)

    send_raw(sock, hello3())
    data = recv(sock)

    assert is_binary(data)
    {:ok, [greeting], ""} = Parser.parse(data)
    assert is_map(greeting)
    :gen_tcp.close(sock)
  end

  test "HELLO 3 greeting contains mode field", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    data = recv(sock)

    {:ok, [greeting], ""} = Parser.parse(data)
    assert Map.has_key?(greeting, "mode")
    :gen_tcp.close(sock)
  end

  test "HELLO 3 greeting id is an integer", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    data = recv(sock)

    {:ok, [greeting], ""} = Parser.parse(data)
    assert is_integer(greeting["id"])
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # PING edge cases
  # ---------------------------------------------------------------------------

  test "PING command is case-insensitive", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_command(sock, ["ping"])
    data = recv(sock)
    assert data == "+PONG\r\n"
    :gen_tcp.close(sock)
  end

  test "PING with empty string argument returns empty bulk string", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_command(sock, ["PING", ""])
    data = recv(sock)
    {:ok, [response], ""} = Parser.parse(data)
    assert response == ""
    :gen_tcp.close(sock)
  end

  test "multiple PING with arguments in pipeline all respond", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    pipeline =
      IO.iodata_to_binary([
        Encoder.encode(["PING", "a"]),
        Encoder.encode(["PING", "b"]),
        Encoder.encode(["PING", "c"])
      ])

    send_raw(sock, pipeline)

    data = recv_at_least(sock, 20, 500)
    {:ok, responses, ""} = Parser.parse(data)
    assert length(responses) == 3
    assert Enum.at(responses, 0) == "a"
    assert Enum.at(responses, 1) == "b"
    assert Enum.at(responses, 2) == "c"
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Large payloads
  # ---------------------------------------------------------------------------

  test "large binary value in PING argument (10KB) echoes correctly", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    large_payload = :binary.copy(<<0x42>>, 10_000)
    send_command(sock, ["PING", large_payload])

    data = recv_at_least(sock, 10_009, 2000)
    {:ok, [response], ""} = Parser.parse(data)
    assert byte_size(response) == 10_000
    assert response == large_payload
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Connection state after RESET
  # ---------------------------------------------------------------------------

  test "RESET after pipelining leaves connection functional", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    pipeline =
      IO.iodata_to_binary([
        Encoder.encode(["PING"]),
        Encoder.encode(["PING"]),
        Encoder.encode(["PING"])
      ])

    send_raw(sock, pipeline)
    _responses = recv_all(sock, "+PONG\r\n", 3)

    send_command(sock, ["RESET"])
    _reset = recv(sock)

    send_command(sock, ["PING"])
    data = recv(sock)
    assert data == "+PONG\r\n"
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # QUIT in pipeline
  # ---------------------------------------------------------------------------

  test "QUIT in a pipeline sends OK and closes", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    pipeline =
      IO.iodata_to_binary([
        Encoder.encode(["PING"]),
        Encoder.encode(["QUIT"]),
        Encoder.encode(["PING"])
      ])

    send_raw(sock, pipeline)

    data = recv_at_least(sock, 14, 500)
    {:ok, responses, ""} = Parser.parse(data)
    assert Enum.at(responses, 0) == {:simple, "PONG"}
    assert Enum.at(responses, 1) == {:simple, "OK"}

    assert closed_or_eof?(sock)
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Error responses
  # ---------------------------------------------------------------------------

  test "wrong arity PING returns a response without crashing", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_command(sock, ["PING", "a", "b"])
    data = recv(sock)
    assert is_binary(data)
    assert byte_size(data) > 0
    :gen_tcp.close(sock)
  end

  test "empty command list is handled gracefully", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_raw(sock, "*0\r\n")
    assert {:error, :timeout} = :gen_tcp.recv(sock, 0, 50)

    send_command(sock, ["PING"])
    pong = recv(sock)
    assert pong == "+PONG\r\n"
    :gen_tcp.close(sock)
  end

  test "blank inline command is skipped and connection remains usable", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_raw(sock, "\r\n")
    assert {:error, :timeout} = :gen_tcp.recv(sock, 0, 50)

    send_command(sock, ["PING"])
    assert recv(sock) == "+PONG\r\n"
    :gen_tcp.close(sock)
  end

  test "empty bulk command name returns unknown command error without closing", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    send_raw(sock, "*1\r\n$0\r\n\r\n")
    data = recv(sock)
    assert String.contains?(data, "unknown command ''")

    send_command(sock, ["PING"])
    assert recv(sock) == "+PONG\r\n"
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Greeting map fields — all expected key/value pairs
  # ---------------------------------------------------------------------------

  test "HELLO 3 greeting contains all expected fields with correct values", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    data = recv(sock)

    {:ok, [greeting], ""} = Parser.parse(data)
    assert greeting["server"] == "ferricstore"
    assert greeting["proto"] == 3
    assert greeting["mode"] == "standalone"
    assert greeting["role"] == "master"
    assert greeting["modules"] == []
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # PING before HELLO
  # ---------------------------------------------------------------------------

  test "PING before HELLO returns +PONG without crashing", %{port: port} do
    sock = connect(port)
    # Do NOT send HELLO first — go straight to PING
    send_command(sock, ["PING"])
    data = recv(sock)
    assert data == "+PONG\r\n"
    :gen_tcp.close(sock)
  end

  test "PING with message before HELLO returns bulk string", %{port: port} do
    sock = connect(port)
    send_command(sock, ["PING", "early bird"])
    data = recv(sock)

    {:ok, [response], ""} = Parser.parse(data)
    assert response == "early bird"
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Large pipeline (100+ commands)
  # ---------------------------------------------------------------------------

  test "pipeline of 100 PING commands returns 100 PONG responses", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    pipeline =
      1..100
      |> Enum.map(fn _ -> Encoder.encode(["PING"]) end)
      |> IO.iodata_to_binary()

    send_raw(sock, pipeline)

    data = recv_all(sock, "+PONG\r\n", 100)
    count = count_occurrences(data, "+PONG\r\n")
    assert count == 100
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # CLIENT HELLO dispatch (two-token form)
  # ---------------------------------------------------------------------------

  test "CLIENT HELLO 3 returns greeting map", %{port: port} do
    sock = connect(port)
    # Build the three-element RESP array: CLIENT HELLO 3
    client_hello = "*3\r\n$6\r\nCLIENT\r\n$5\r\nHELLO\r\n$1\r\n3\r\n"
    send_raw(sock, client_hello)
    data = recv(sock)

    {:ok, [greeting], ""} = Parser.parse(data)
    assert is_map(greeting)
    assert greeting["server"] == "ferricstore"
    assert greeting["proto"] == 3
    assert greeting["mode"] == "standalone"
    assert greeting["role"] == "master"
    assert greeting["modules"] == []
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Half-close / zero-byte recv — clean exit
  # ---------------------------------------------------------------------------

  test "server connection exits cleanly on half-close (shutdown :write)", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    # Half-close the write side of the client socket.
    # This causes the server's recv to return {:ok, <<>>} or {:error, :closed}.
    :gen_tcp.shutdown(sock, :write)

    # The server should close its side cleanly without spinning.
    # Verify by confirming the socket reaches a closed state within a bounded time.
    assert closed_or_eof?(sock)
    :gen_tcp.close(sock)
  end

  test "server does not spin on half-close — connection process exits promptly", %{port: port} do
    sock = connect(port)
    send_raw(sock, hello3())
    _greeting = recv(sock)

    # Capture current BEAM process count before triggering half-close
    procs_before = length(Process.list())

    :gen_tcp.shutdown(sock, :write)
    # Give the server a moment to handle the close
    Process.sleep(100)

    procs_after = length(Process.list())

    # If the server spun infinitely, we'd see a process leak. The count should
    # stay the same or decrease (the connection process exits).
    assert procs_after <= procs_before
    :gen_tcp.close(sock)
  end

  # ---------------------------------------------------------------------------
  # Private test helpers
  # ---------------------------------------------------------------------------

  defp recv_all(_sock, _pattern, 0), do: ""

  defp recv_all(sock, pattern, count) do
    data = recv(sock)

    occurrences = count_occurrences(data, pattern)

    if occurrences >= count do
      data
    else
      data <> recv_all(sock, pattern, count - occurrences)
    end
  end

  defp recv_at_least(sock, min_bytes, timeout) do
    recv_at_least(sock, min_bytes, timeout, "")
  end

  defp recv_at_least(_sock, min_bytes, _timeout, acc) when byte_size(acc) >= min_bytes, do: acc

  defp recv_at_least(sock, min_bytes, timeout, acc) do
    case :gen_tcp.recv(sock, 0, timeout) do
      {:ok, chunk} -> recv_at_least(sock, min_bytes, timeout, acc <> chunk)
      {:error, _} -> acc
    end
  end

  defp count_occurrences(data, pattern) do
    data
    |> :binary.matches(pattern)
    |> length()
  end

  defp closed_or_eof?(sock) do
    case :gen_tcp.recv(sock, 0, 500) do
      {:error, :closed} -> true
      {:error, :econnreset} -> true
      {:error, :einval} -> true
      {:error, :enotconn} -> true
      {:ok, ""} -> true
      {:ok, _data} -> false
    end
  end
end
