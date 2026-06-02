defmodule FerricstoreServer.Integration.StreamTcpTest do
  @moduledoc """
  End-to-end TCP integration tests for FerricStore Redis Stream commands.

  These tests connect over a real TCP socket, send RESP3-encoded commands, and
  verify responses through the full stack:

      TCP -> RESP3 parser -> dispatcher -> stream handler -> shard -> Bitcask NIF

  The application-started Ranch TCP listener on an ephemeral port is shared
  across all tests. Each test uses unique stream key names to avoid interference.
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

  defp recv_n(sock, n) do
    do_recv_n(sock, n, "", [])
  end

  defp do_recv_n(_sock, 0, _buf, acc), do: acc

  defp do_recv_n(sock, remaining, buf, acc) when remaining > 0 do
    {:ok, data} = :gen_tcp.recv(sock, 0, 5000)
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

  defp ustream, do: "tcpstream_#{:rand.uniform(999_999)}"

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    %{port: Listener.port()}
  end

  # Clean ETS tables for stream metadata between tests.
  setup do
    for table <- [Ferricstore.Stream.Meta, Ferricstore.Stream.Groups] do
      try do
        :ets.delete_all_objects(table)
      rescue
        ArgumentError -> :ok
      end
    end

    :ok
  end

  # ===========================================================================
  # XADD and XLEN over TCP
  # ===========================================================================

  describe "XADD and XLEN over TCP" do
    test "XADD with * returns stream ID, XLEN increments", %{port: port} do
      sock = connect_and_hello(port)
      key = ustream()

      send_cmd(sock, ["XADD", key, "*", "temperature", "36.5"])
      id = recv_response(sock)
      assert is_binary(id)
      assert id =~ ~r/^\d+-\d+$/

      send_cmd(sock, ["XLEN", key])
      assert recv_response(sock) == 1

      send_cmd(sock, ["XADD", key, "*", "temperature", "37.0"])
      _id2 = recv_response(sock)

      send_cmd(sock, ["XLEN", key])
      assert recv_response(sock) == 2

      :gen_tcp.close(sock)
    end

    test "XADD with explicit ID", %{port: port} do
      sock = connect_and_hello(port)
      key = ustream()

      send_cmd(sock, ["XADD", key, "1000-0", "sensor", "A"])
      assert recv_response(sock) == "1000-0"

      send_cmd(sock, ["XADD", key, "2000-0", "sensor", "B"])
      assert recv_response(sock) == "2000-0"

      :gen_tcp.close(sock)
    end

    test "XADD with smaller explicit ID returns error", %{port: port} do
      sock = connect_and_hello(port)
      key = ustream()

      send_cmd(sock, ["XADD", key, "1000-0", "f", "v"])
      assert recv_response(sock) == "1000-0"

      send_cmd(sock, ["XADD", key, "500-0", "g", "w"])
      response = recv_response(sock)
      assert match?({:error, "ERR" <> _}, response)

      :gen_tcp.close(sock)
    end

    test "XLEN on nonexistent stream returns 0", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["XLEN", "nonexistent_stream_#{:rand.uniform(999_999)}"])
      assert recv_response(sock) == 0

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # XRANGE and XREVRANGE over TCP
  # ===========================================================================

  describe "XRANGE and XREVRANGE over TCP" do
    test "XRANGE - + returns all entries in order", %{port: port} do
      sock = connect_and_hello(port)
      key = ustream()

      send_cmd(sock, ["XADD", key, "1-0", "name", "alice"])
      assert recv_response(sock) == "1-0"

      send_cmd(sock, ["XADD", key, "2-0", "name", "bob"])
      assert recv_response(sock) == "2-0"

      send_cmd(sock, ["XADD", key, "3-0", "name", "charlie"])
      assert recv_response(sock) == "3-0"

      send_cmd(sock, ["XRANGE", key, "-", "+"])
      entries = recv_response(sock)
      assert is_list(entries)
      assert length(entries) == 3
      # Each entry is [id, field, value, ...]
      assert Enum.map(entries, &hd/1) == ["1-0", "2-0", "3-0"]

      :gen_tcp.close(sock)
    end

    test "XRANGE with COUNT", %{port: port} do
      sock = connect_and_hello(port)
      key = ustream()

      for i <- 1..5 do
        send_cmd(sock, ["XADD", key, "#{i}-0", "n", "#{i}"])
        _id = recv_response(sock)
      end

      send_cmd(sock, ["XRANGE", key, "-", "+", "COUNT", "2"])
      entries = recv_response(sock)
      assert length(entries) == 2
      assert Enum.map(entries, &hd/1) == ["1-0", "2-0"]

      :gen_tcp.close(sock)
    end

    test "XREVRANGE + - returns all entries in reverse", %{port: port} do
      sock = connect_and_hello(port)
      key = ustream()

      for i <- 1..3 do
        send_cmd(sock, ["XADD", key, "#{i}-0", "n", "#{i}"])
        _id = recv_response(sock)
      end

      send_cmd(sock, ["XREVRANGE", key, "+", "-"])
      entries = recv_response(sock)
      assert length(entries) == 3
      assert Enum.map(entries, &hd/1) == ["3-0", "2-0", "1-0"]

      :gen_tcp.close(sock)
    end

    test "XREVRANGE with COUNT", %{port: port} do
      sock = connect_and_hello(port)
      key = ustream()

      for i <- 1..5 do
        send_cmd(sock, ["XADD", key, "#{i}-0", "n", "#{i}"])
        _id = recv_response(sock)
      end

      send_cmd(sock, ["XREVRANGE", key, "+", "-", "COUNT", "2"])
      entries = recv_response(sock)
      assert length(entries) == 2
      assert Enum.map(entries, &hd/1) == ["5-0", "4-0"]

      :gen_tcp.close(sock)
    end

    test "XRANGE on nonexistent stream returns empty array", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["XRANGE", "no_such_stream_#{:rand.uniform(999_999)}", "-", "+"])
      assert recv_response(sock) == []

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # XREAD over TCP
  # ===========================================================================

  describe "XREAD over TCP" do
    test "XREAD returns entries after given ID", %{port: port} do
      sock = connect_and_hello(port)
      key = ustream()

      for i <- 1..3 do
        send_cmd(sock, ["XADD", key, "#{i}-0", "f", "#{i}"])
        _id = recv_response(sock)
      end

      send_cmd(sock, ["XREAD", "STREAMS", key, "1-0"])
      result = recv_response(sock)
      assert is_list(result)
      # Result format: [[stream_key, [[id, f, v], ...]]]
      assert length(result) == 1
      [stream_key, entries] = hd(result)
      assert stream_key == key
      assert length(entries) == 2
      assert Enum.map(entries, &hd/1) == ["2-0", "3-0"]

      :gen_tcp.close(sock)
    end

    test "XREAD with COUNT", %{port: port} do
      sock = connect_and_hello(port)
      key = ustream()

      for i <- 1..5 do
        send_cmd(sock, ["XADD", key, "#{i}-0", "f", "#{i}"])
        _id = recv_response(sock)
      end

      send_cmd(sock, ["XREAD", "COUNT", "2", "STREAMS", key, "0"])
      result = recv_response(sock)
      [_stream_key, entries] = hd(result)
      assert length(entries) == 2

      :gen_tcp.close(sock)
    end

    test "XREAD with no new entries returns empty", %{port: port} do
      sock = connect_and_hello(port)
      key = ustream()

      send_cmd(sock, ["XADD", key, "1-0", "f", "v"])
      _id = recv_response(sock)

      send_cmd(sock, ["XREAD", "STREAMS", key, "1-0"])
      # When no entries, returns empty array.
      result = recv_response(sock)
      assert result == []

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # XTRIM over TCP
  # ===========================================================================

  describe "XTRIM over TCP" do
    test "XTRIM MAXLEN removes oldest entries", %{port: port} do
      sock = connect_and_hello(port)
      key = ustream()

      for i <- 1..5 do
        send_cmd(sock, ["XADD", key, "#{i}-0", "f", "#{i}"])
        _id = recv_response(sock)
      end

      send_cmd(sock, ["XTRIM", key, "MAXLEN", "3"])
      deleted = recv_response(sock)
      assert deleted == 2

      send_cmd(sock, ["XLEN", key])
      assert recv_response(sock) == 3

      send_cmd(sock, ["XRANGE", key, "-", "+"])
      entries = recv_response(sock)
      assert Enum.map(entries, &hd/1) == ["3-0", "4-0", "5-0"]

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # XDEL over TCP
  # ===========================================================================

  describe "XDEL over TCP" do
    test "XDEL removes specific entries", %{port: port} do
      sock = connect_and_hello(port)
      key = ustream()

      for i <- 1..3 do
        send_cmd(sock, ["XADD", key, "#{i}-0", "f", "#{i}"])
        _id = recv_response(sock)
      end

      send_cmd(sock, ["XDEL", key, "2-0"])
      assert recv_response(sock) == 1

      send_cmd(sock, ["XLEN", key])
      assert recv_response(sock) == 2

      send_cmd(sock, ["XRANGE", key, "-", "+"])
      entries = recv_response(sock)
      assert Enum.map(entries, &hd/1) == ["1-0", "3-0"]

      :gen_tcp.close(sock)
    end

    test "XDEL nonexistent entry returns 0", %{port: port} do
      sock = connect_and_hello(port)
      key = ustream()

      send_cmd(sock, ["XADD", key, "1-0", "f", "v"])
      _id = recv_response(sock)

      send_cmd(sock, ["XDEL", key, "99-99"])
      assert recv_response(sock) == 0

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # XINFO STREAM over TCP
  # ===========================================================================

  describe "XINFO STREAM over TCP" do
    test "XINFO STREAM returns stream metadata map", %{port: port} do
      sock = connect_and_hello(port)
      key = ustream()

      send_cmd(sock, ["XADD", key, "1-0", "name", "alice"])
      assert recv_response(sock) == "1-0"

      send_cmd(sock, ["XADD", key, "2-0", "name", "bob"])
      assert recv_response(sock) == "2-0"

      send_cmd(sock, ["XINFO", "STREAM", key])
      info = recv_response(sock)
      assert is_map(info)
      assert info["length"] == 2
      assert info["last-generated-id"] == "2-0"

      :gen_tcp.close(sock)
    end

    test "XINFO STREAM on nonexistent stream returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["XINFO", "STREAM", "no_stream_#{:rand.uniform(999_999)}"])
      response = recv_response(sock)
      assert match?({:error, "ERR" <> _}, response)

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # Consumer groups over TCP
  # ===========================================================================

  describe "consumer groups over TCP" do
    test "XGROUP CREATE, XREADGROUP, XACK full lifecycle", %{port: port} do
      sock = connect_and_hello(port)
      key = ustream()

      # Add entries.
      send_cmd(sock, ["XADD", key, "1-0", "task", "build"])
      assert recv_response(sock) == "1-0"

      send_cmd(sock, ["XADD", key, "2-0", "task", "test"])
      assert recv_response(sock) == "2-0"

      send_cmd(sock, ["XADD", key, "3-0", "task", "deploy"])
      assert recv_response(sock) == "3-0"

      # Create consumer group starting from 0 (deliver all).
      send_cmd(sock, ["XGROUP", "CREATE", key, "workers", "0"])
      assert recv_response(sock) == {:simple, "OK"}

      # Read with consumer c1.
      send_cmd(sock, ["XREADGROUP", "GROUP", "workers", "c1", "COUNT", "2", "STREAMS", key, ">"])
      result = recv_response(sock)
      assert is_list(result)
      [^key, entries] = hd(result)
      assert length(entries) == 2
      assert Enum.map(entries, &hd/1) == ["1-0", "2-0"]

      # Acknowledge entry 1-0.
      send_cmd(sock, ["XACK", key, "workers", "1-0"])
      assert recv_response(sock) == 1

      # Read pending for c1 -- should only show 2-0.
      send_cmd(sock, ["XREADGROUP", "GROUP", "workers", "c1", "STREAMS", key, "0"])
      pending_result = recv_response(sock)
      [^key, pending_entries] = hd(pending_result)
      assert length(pending_entries) == 1
      assert hd(hd(pending_entries)) == "2-0"

      # Consumer c2 reads remaining new messages.
      send_cmd(sock, ["XREADGROUP", "GROUP", "workers", "c2", "STREAMS", key, ">"])
      result2 = recv_response(sock)
      [^key, entries2] = hd(result2)
      assert length(entries2) == 1
      assert hd(hd(entries2)) == "3-0"

      :gen_tcp.close(sock)
    end

    test "XGROUP CREATE with MKSTREAM on nonexistent stream", %{port: port} do
      sock = connect_and_hello(port)
      key = ustream()

      send_cmd(sock, ["XGROUP", "CREATE", key, "g1", "0", "MKSTREAM"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["XLEN", key])
      assert recv_response(sock) == 0

      :gen_tcp.close(sock)
    end

    test "XGROUP CREATE without MKSTREAM on nonexistent stream returns error", %{port: port} do
      sock = connect_and_hello(port)
      key = ustream()

      send_cmd(sock, ["XGROUP", "CREATE", key, "g1", "0"])
      response = recv_response(sock)
      assert match?({:error, "ERR" <> _}, response)

      :gen_tcp.close(sock)
    end

    test "XREADGROUP with nonexistent group returns NOGROUP error", %{port: port} do
      sock = connect_and_hello(port)
      key = ustream()

      send_cmd(sock, ["XADD", key, "1-0", "f", "v"])
      _id = recv_response(sock)

      send_cmd(sock, ["XREADGROUP", "GROUP", "nogroup", "c1", "STREAMS", key, ">"])
      response = recv_response(sock)
      assert match?({:error, "NOGROUP" <> _}, response)

      :gen_tcp.close(sock)
    end

    test "XREADGROUP BLOCK waits through the TCP connection path", %{port: port} do
      sock = connect_and_hello(port)
      key = ustream()

      send_cmd(sock, ["XADD", key, "1-0", "f", "v"])
      assert recv_response(sock) == "1-0"

      send_cmd(sock, ["XGROUP", "CREATE", key, "workers", "0"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["XREADGROUP", "GROUP", "workers", "c1", "STREAMS", key, ">"])
      result = recv_response(sock)
      [^key, entries] = hd(result)
      assert Enum.map(entries, &hd/1) == ["1-0"]

      started = System.monotonic_time(:millisecond)

      send_cmd(sock, [
        "XREADGROUP",
        "GROUP",
        "workers",
        "c1",
        "BLOCK",
        "50",
        "STREAMS",
        key,
        ">"
      ])

      assert recv_response(sock) == nil
      assert System.monotonic_time(:millisecond) - started >= 20

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # Pipelining stream commands
  # ===========================================================================

  describe "pipelining stream commands" do
    test "pipeline XREAD BLOCK holds the pipeline until stream data arrives", %{port: port} do
      sock = connect_and_hello(port)
      producer = connect_and_hello(port)
      key = ustream()

      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["XREAD", "BLOCK", "1000", "STREAMS", key, "0-0"]),
          Encoder.encode(["PING"])
        ])

      :ok = :gen_tcp.send(sock, pipeline)
      assert {:error, :timeout} = :gen_tcp.recv(sock, 0, 100)

      send_cmd(producer, ["XADD", key, "1-0", "f", "v"])
      assert recv_response(producer) == "1-0"

      responses = recv_n(sock, 2)
      assert [[[key, [["1-0", "f", "v"]]]], {:simple, "PONG"}] == responses

      :gen_tcp.close(sock)
      :gen_tcp.close(producer)
    end

    test "pipeline XADD then XLEN then XRANGE", %{port: port} do
      sock = connect_and_hello(port)
      key = ustream()

      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["XADD", key, "1-0", "a", "1"]),
          Encoder.encode(["XADD", key, "2-0", "b", "2"]),
          Encoder.encode(["XLEN", key]),
          Encoder.encode(["XRANGE", key, "-", "+"])
        ])

      :ok = :gen_tcp.send(sock, pipeline)

      responses = recv_n(sock, 4)
      assert Enum.at(responses, 0) == "1-0"
      assert Enum.at(responses, 1) == "2-0"
      assert Enum.at(responses, 2) == 2
      assert length(Enum.at(responses, 3)) == 2

      :gen_tcp.close(sock)
    end
  end
end
