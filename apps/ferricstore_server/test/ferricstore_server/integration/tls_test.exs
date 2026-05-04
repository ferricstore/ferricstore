defmodule FerricstoreServer.Integration.TlsTest do
  @moduledoc """
  End-to-end TLS integration tests for FerricStore.

  These tests generate self-signed certificates at runtime, start a TLS
  listener on an ephemeral port, connect via `:ssl`, and verify the full
  command stack works over an encrypted transport:

      TLS → RESP3 parser → dispatcher → router → shard → Bitcask NIF

  The TLS listener is started/stopped per `describe` group to keep tests
  isolated from the application-managed plaintext listener.
  """

  use ExUnit.Case, async: false

  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Connection.Sendfile
  alias FerricstoreServer.TlsListener
  alias Ferricstore.Test.TlsCertHelper

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Process dictionary keys for buffered state between recv_one calls.
  # TLS may deliver multiple RESP3 frames in a single :ssl.recv, so we
  # buffer parsed values and leftover binary across calls.
  @parsed_key :tls_parsed_queue
  @binary_key :tls_binary_buf

  defp send_cmd(ssl_sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :ssl.send(ssl_sock, data)
  end

  defp recv_response(ssl_sock) do
    case Process.get(@parsed_key, []) do
      [val | rest] ->
        Process.put(@parsed_key, rest)
        val

      [] ->
        buf = Process.get(@binary_key, "")
        fetch_and_recv(ssl_sock, buf)
    end
  end

  defp fetch_and_recv(ssl_sock, buf) do
    case Parser.parse(buf) do
      {:ok, [val | rest_vals], rest_bin} ->
        Process.put(@parsed_key, rest_vals)
        Process.put(@binary_key, rest_bin)
        val

      {:ok, [], _} ->
        {:ok, data} = :ssl.recv(ssl_sock, 0, 5_000)
        fetch_and_recv(ssl_sock, buf <> data)
    end
  end

  defp connect_tls(port) do
    {:ok, sock} =
      :ssl.connect(
        ~c"127.0.0.1",
        port,
        [
          :binary,
          active: false,
          packet: :raw,
          verify: :verify_none
        ],
        5_000
      )

    sock
  end

  defp connect_tls_and_hello(port) do
    sock = connect_tls(port)
    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  defp ukey(base), do: "tls_#{base}_#{:rand.uniform(9_999_999)}"

  defp attach_file_stream_handler(test_pid) do
    id = {__MODULE__, test_pid, :file_stream, System.unique_integer([:positive])}

    :telemetry.attach(
      id,
      [:ferricstore, :server, :file_stream],
      fn event, measurements, metadata, _config ->
        send(test_pid, {:file_stream_event, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)
  end

  # ---------------------------------------------------------------------------
  # Setup: generate certs + start TLS listener
  # ---------------------------------------------------------------------------

  setup_all do
    tmp_dir = System.tmp_dir!()
    {cert_path, key_path} = TlsCertHelper.generate_self_signed(tmp_dir)

    # Stop the TLS listener if it's somehow already running
    if TlsListener.running?(), do: TlsListener.stop()

    {:ok, _pid} =
      TlsListener.start(
        port: 0,
        certfile: cert_path,
        keyfile: key_path
      )

    port = TlsListener.port()

    on_exit(fn ->
      TlsListener.stop()
      File.rm(cert_path)
      File.rm(key_path)
    end)

    %{tls_port: port, cert_path: cert_path, key_path: key_path}
  end

  # ---------------------------------------------------------------------------
  # TLS connection and handshake tests
  # ---------------------------------------------------------------------------

  describe "TLS connection establishment" do
    test "connects via TLS and completes HELLO 3 handshake", %{tls_port: port} do
      sock = connect_tls(port)
      send_cmd(sock, ["HELLO", "3"])
      greeting = recv_response(sock)

      assert is_map(greeting)
      assert greeting["server"] == "ferricstore"
      assert greeting["proto"] == 3
      assert Map.has_key?(greeting, "version")
      assert Map.has_key?(greeting, "id")

      :ssl.close(sock)
    end

    test "HELLO 2 returns NOPROTO error over TLS", %{tls_port: port} do
      sock = connect_tls(port)
      send_cmd(sock, ["HELLO", "2"])
      result = recv_response(sock)

      assert {:error, msg} = result
      assert String.contains?(msg, "NOPROTO")

      :ssl.close(sock)
    end

    test "PING returns PONG over TLS", %{tls_port: port} do
      sock = connect_tls_and_hello(port)
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}
      :ssl.close(sock)
    end

    test "PING with message echoes over TLS", %{tls_port: port} do
      sock = connect_tls_and_hello(port)
      send_cmd(sock, ["PING", "tls-test"])
      assert recv_response(sock) == "tls-test"
      :ssl.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Data commands over TLS
  # ---------------------------------------------------------------------------

  describe "SET / GET / DEL over TLS" do
    test "SET then GET returns stored value", %{tls_port: port} do
      sock = connect_tls_and_hello(port)
      k = ukey("get")

      send_cmd(sock, ["SET", k, "tls_value"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "tls_value"

      :ssl.close(sock)
    end

    test "GET on missing key returns nil", %{tls_port: port} do
      sock = connect_tls_and_hello(port)
      k = ukey("missing")

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == nil

      :ssl.close(sock)
    end

    test "large cold GET streams bounded file chunks over TLS", %{tls_port: port} do
      attach_file_stream_handler(self())

      sock = connect_tls_and_hello(port)
      k = ukey("large_get_stream")
      value = :binary.copy("T", Sendfile.threshold_bytes() + 131_072)

      send_cmd(sock, ["SET", k, value])
      assert recv_response(sock) == {:simple, "OK"}

      :ssl.close(sock)
      sock = connect_tls_and_hello(port)

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == value

      size = byte_size(value)

      assert_receive {:file_stream_event, [:ferricstore, :server, :file_stream],
                      %{bytes: ^size, chunks: chunks}, %{result: :ok, transport: :ranch_ssl}},
                     1000

      assert chunks > 1

      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      :ssl.close(sock)
    end

    test "DEL removes a key", %{tls_port: port} do
      sock = connect_tls_and_hello(port)
      k = ukey("del")

      send_cmd(sock, ["SET", k, "to_delete"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["DEL", k])
      assert recv_response(sock) == 1

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == nil

      :ssl.close(sock)
    end

    test "EXISTS returns 1 for present key, 0 for absent", %{tls_port: port} do
      sock = connect_tls_and_hello(port)
      k = ukey("exists")

      send_cmd(sock, ["SET", k, "x"])
      recv_response(sock)

      send_cmd(sock, ["EXISTS", k])
      assert recv_response(sock) == 1

      send_cmd(sock, ["EXISTS", ukey("nope")])
      assert recv_response(sock) == 0

      :ssl.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Pipelining over TLS
  # ---------------------------------------------------------------------------

  describe "pipelining over TLS" do
    test "multiple commands in one write, responses in order", %{tls_port: port} do
      sock = connect_tls_and_hello(port)
      k1 = ukey("pipe_a")
      k2 = ukey("pipe_b")

      data =
        IO.iodata_to_binary([
          Encoder.encode(["SET", k1, "1"]),
          Encoder.encode(["SET", k2, "2"]),
          Encoder.encode(["MGET", k1, k2])
        ])

      :ok = :ssl.send(sock, data)

      assert recv_response(sock) == {:simple, "OK"}
      assert recv_response(sock) == {:simple, "OK"}
      assert recv_response(sock) == ["1", "2"]

      :ssl.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # TTL / EXPIRE over TLS
  # ---------------------------------------------------------------------------

  describe "TTL / EXPIRE over TLS" do
    test "SET with EX, TTL returns remaining seconds", %{tls_port: port} do
      sock = connect_tls_and_hello(port)
      k = ukey("ttl_ex")

      send_cmd(sock, ["SET", k, "v", "EX", "100"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["TTL", k])
      ttl = recv_response(sock)
      assert is_integer(ttl) and ttl > 0 and ttl <= 100

      :ssl.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Error handling over TLS
  # ---------------------------------------------------------------------------

  describe "error handling over TLS" do
    test "unknown command returns error", %{tls_port: port} do
      sock = connect_tls_and_hello(port)

      send_cmd(sock, ["NOTACOMMAND"])
      assert {:error, _msg} = recv_response(sock)

      :ssl.close(sock)
    end

    test "wrong arity returns error", %{tls_port: port} do
      sock = connect_tls_and_hello(port)

      send_cmd(sock, ["GET"])
      assert {:error, _msg} = recv_response(sock)

      :ssl.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # QUIT over TLS
  # ---------------------------------------------------------------------------

  describe "QUIT over TLS" do
    test "QUIT closes the TLS connection", %{tls_port: port} do
      sock = connect_tls_and_hello(port)

      send_cmd(sock, ["QUIT"])
      assert recv_response(sock) == {:simple, "OK"}

      # After QUIT the server closes the socket
      assert :ssl.recv(sock, 0, 500) in [{:error, :closed}, {:error, :einval}]
      :ssl.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Binary safety over TLS
  # ---------------------------------------------------------------------------

  describe "binary safety over TLS" do
    test "values with special characters round-trip correctly", %{tls_port: port} do
      sock = connect_tls_and_hello(port)
      k = ukey("binary_safe")
      v = "value\twith\nnewlines and \"quotes\""

      send_cmd(sock, ["SET", k, v])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == v

      :ssl.close(sock)
    end

    test "unicode value round-trips correctly", %{tls_port: port} do
      sock = connect_tls_and_hello(port)
      k = ukey("unicode")
      v = "hello world"

      send_cmd(sock, ["SET", k, v])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == v

      :ssl.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # CONFIG GET TLS parameters
  # ---------------------------------------------------------------------------

  describe "CONFIG GET TLS parameters" do
    test "tls-* parameters are accessible via CONFIG GET", %{tls_port: _port} do
      # Use the plaintext TCP connection (via the app-managed listener) to
      # query CONFIG GET for TLS params. They should return defaults.
      tcp_port = FerricstoreServer.Listener.port()

      {:ok, tcp_sock} =
        :gen_tcp.connect(~c"127.0.0.1", tcp_port, [:binary, active: false, packet: :raw])

      tcp_send(tcp_sock, ["CONFIG", "GET", "tls-port"])
      result = tcp_recv(tcp_sock)
      assert is_list(result)
      assert length(result) == 2
      [_key, _val] = result

      tcp_send(tcp_sock, ["CONFIG", "GET", "require-tls"])
      result = tcp_recv(tcp_sock)
      assert is_list(result)
      assert length(result) == 2
      [_key, val] = result
      assert val == "false"

      # CONFIG SET on TLS params should fail (read-only)
      tcp_send(tcp_sock, ["CONFIG", "SET", "tls-port", "9999"])
      result = tcp_recv(tcp_sock)
      assert {:error, msg} = result
      assert String.contains?(msg, "read-only")

      :gen_tcp.close(tcp_sock)
    end

    test "tls-* glob matches all TLS parameters", %{tls_port: _port} do
      tcp_port = FerricstoreServer.Listener.port()

      {:ok, tcp_sock} =
        :gen_tcp.connect(~c"127.0.0.1", tcp_port, [:binary, active: false, packet: :raw])

      tcp_send(tcp_sock, ["CONFIG", "GET", "tls-*"])
      result = tcp_recv(tcp_sock)
      assert is_list(result)

      # Should have at least tls-port, tls-cert-file, tls-key-file, tls-ca-cert-file = 4 pairs = 8 elements
      assert length(result) >= 8

      :gen_tcp.close(tcp_sock)
    end
  end

  # ---------------------------------------------------------------------------
  # TlsListener module tests
  # ---------------------------------------------------------------------------

  describe "TlsListener.running?/0" do
    test "returns true when the listener is active" do
      assert TlsListener.running?() == true
    end
  end

  # ---------------------------------------------------------------------------
  # TCP helpers for CONFIG GET tests
  # ---------------------------------------------------------------------------

  defp tcp_send(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp tcp_recv(sock) do
    tcp_recv(sock, "")
  end

  defp tcp_recv(sock, buf) do
    {:ok, data} = :gen_tcp.recv(sock, 0, 5_000)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [val], _rest} -> val
      {:ok, [], _} -> tcp_recv(sock, buf2)
    end
  end
end
