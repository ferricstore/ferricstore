defmodule FerricstoreServer.TlsRotationTest do
  @moduledoc """
  Tests that verify TLS certificate rotation behavior in FerricStore.

  Documented behavior under test:

    1. FerricStore reads TLS cert/key files at listener startup.
    2. Existing connections continue with the old certificate after cert
       files change on disk.
    3. After a listener restart, new connections use the new certificate.
    4. Without a restart, new connections still use the old (startup-time)
       certificate even though the files on disk have changed.

  Each test starts its own Ranch TLS listener with a unique ref to avoid
  conflicts with the application-managed TLS listener or with other test
  suites.
  """

  use ExUnit.Case, async: false

  alias FerricstoreServer.Resp.{Encoder, Parser}

  # ---------------------------------------------------------------------------
  # Constants
  # ---------------------------------------------------------------------------

  # Process-dictionary keys for the buffered RESP3 parser (TLS may deliver
  # multiple frames in a single :ssl.recv).
  @parsed_key :tls_rot_parsed_queue
  @binary_key :tls_rot_binary_buf

  # ---------------------------------------------------------------------------
  # Certificate generation helpers
  # ---------------------------------------------------------------------------

  # Generates a CA key + self-signed CA certificate.  Returns
  # `{ca_key, ca_cert_der, ca_cert_path, ca_key_path}`.
  defp generate_ca(dir, name) do
    ca_key = :public_key.generate_key({:rsa, 2048, 65_537})

    %{cert: ca_cert_der, key: ^ca_key} =
      :public_key.pkix_test_root_cert(name, key: ca_key)

    ca_cert_pem = :public_key.pem_encode([{:Certificate, ca_cert_der, :not_encrypted}])
    ca_key_der = :public_key.der_encode(:RSAPrivateKey, ca_key)
    ca_key_pem = :public_key.pem_encode([{:RSAPrivateKey, ca_key_der, :not_encrypted}])

    ca_cert_path = Path.join(dir, "#{name}-cert.pem")
    ca_key_path = Path.join(dir, "#{name}-key.pem")
    File.write!(ca_cert_path, ca_cert_pem)
    File.write!(ca_key_path, ca_key_pem)

    {ca_key, ca_cert_der, ca_cert_path, ca_key_path}
  end

  # Generates a self-signed server certificate (not CA-signed).
  # Returns `{cert_der, cert_path, key_path}`.
  defp generate_self_signed(dir, suffix) do
    key = :public_key.generate_key({:rsa, 2048, 65_537})
    %{cert: cert_der, key: ^key} = :public_key.pkix_test_root_cert("localhost", key: key)

    cert_pem = :public_key.pem_encode([{:Certificate, cert_der, :not_encrypted}])
    key_der = :public_key.der_encode(:RSAPrivateKey, key)
    key_pem = :public_key.pem_encode([{:RSAPrivateKey, key_der, :not_encrypted}])

    cert_path = Path.join(dir, "server-cert-#{suffix}.pem")
    key_path = Path.join(dir, "server-key-#{suffix}.pem")
    File.write!(cert_path, cert_pem)
    File.write!(key_path, key_pem)

    {cert_der, cert_path, key_path}
  end

  # Generates an expired certificate using openssl.
  # Returns `{:ok, cert_path, key_path}` or `:skip` if openssl is unavailable.
  defp generate_expired_cert(dir) do
    key_path = Path.join(dir, "expired-key.pem")
    cert_path = Path.join(dir, "expired-cert.pem")
    csr_path = Path.join(dir, "expired-csr.pem")

    with {_, 0} <-
           System.cmd("openssl", ["genrsa", "-out", key_path, "2048"], stderr_to_stdout: true),
         {_, 0} <-
           System.cmd(
             "openssl",
             ["req", "-new", "-key", key_path, "-out", csr_path, "-subj", "/CN=localhost"],
             stderr_to_stdout: true
           ) do
      two_days_ago =
        DateTime.utc_now()
        |> DateTime.add(-2, :day)
        |> Calendar.strftime("%Y%m%d%H%M%SZ")

      one_day_ago =
        DateTime.utc_now()
        |> DateTime.add(-1, :day)
        |> Calendar.strftime("%Y%m%d%H%M%SZ")

      case System.cmd(
             "openssl",
             [
               "x509",
               "-req",
               "-in",
               csr_path,
               "-signkey",
               key_path,
               "-out",
               cert_path,
               "-startdate",
               two_days_ago,
               "-enddate",
               one_day_ago
             ],
             stderr_to_stdout: true
           ) do
        {_, 0} -> {:ok, cert_path, key_path}
        _ -> :skip
      end
    else
      _ -> :skip
    end
  end

  # ---------------------------------------------------------------------------
  # Ranch listener helpers
  # ---------------------------------------------------------------------------

  # Starts a Ranch TLS listener with a unique ref.  Returns `{ref, port}`.
  defp start_tls_listener(ref, certfile, keyfile, extra_opts \\ []) do
    socket_opts =
      [
        port: 0,
        certfile: certfile,
        keyfile: keyfile,
        versions: [:"tlsv1.3", :"tlsv1.2"]
      ] ++ extra_opts

    transport_opts = %{socket_opts: socket_opts, num_acceptors: 4}

    {:ok, _pid} =
      :ranch.start_listener(
        ref,
        :ranch_ssl,
        transport_opts,
        FerricstoreServer.Connection,
        %{}
      )

    port = :ranch.get_port(ref)
    {ref, port}
  end

  defp stop_tls_listener(ref) do
    :ranch.stop_listener(ref)
  end

  # ---------------------------------------------------------------------------
  # TLS connection helpers
  # ---------------------------------------------------------------------------

  defp connect_tls(port, extra_ssl_opts \\ []) do
    connect_tls(port, extra_ssl_opts, 5)
  end

  defp connect_tls(port, extra_ssl_opts, retries) do
    base_opts = [
      :binary,
      active: false,
      packet: :raw,
      verify: :verify_none
    ]

    case :ssl.connect(~c"127.0.0.1", port, base_opts ++ extra_ssl_opts, 5_000) do
      {:ok, sock} ->
        sock

      {:error, _} when retries > 0 ->
        Process.sleep(200)
        connect_tls(port, extra_ssl_opts, retries - 1)
    end
  end

  defp connect_tls_and_hello(port, extra_ssl_opts \\ []) do
    sock = connect_tls(port, extra_ssl_opts)
    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  # Returns the DER-encoded peer certificate from an established SSL socket.
  defp peer_cert_der(ssl_sock) do
    {:ok, der} = :ssl.peercert(ssl_sock)
    der
  end

  # ---------------------------------------------------------------------------
  # RESP3 send/recv helpers (buffered, handles TLS coalescing)
  # ---------------------------------------------------------------------------

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

  # Clear the process-dictionary parser buffer between tests.
  defp clear_recv_buffer do
    Process.delete(@parsed_key)
    Process.delete(@binary_key)
  end

  # ---------------------------------------------------------------------------
  # Unique ref generator
  # ---------------------------------------------------------------------------

  defp unique_ref(base) do
    :"tls_rot_#{base}_#{System.unique_integer([:positive])}"
  end

  # ---------------------------------------------------------------------------
  # Setup: create temp directory with cert pairs v1 and v2
  # ---------------------------------------------------------------------------

  setup do
    clear_recv_buffer()

    tmp_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_tls_rot_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp_dir)

    # Two distinct self-signed certs for rotation testing.
    {v1_der, v1_cert, v1_key} = generate_self_signed(tmp_dir, "v1")
    {v2_der, v2_cert, v2_key} = generate_self_signed(tmp_dir, "v2")

    on_exit(fn -> File.rm_rf(tmp_dir) end)

    %{
      tmp_dir: tmp_dir,
      v1_der: v1_der,
      v1_cert: v1_cert,
      v1_key: v1_key,
      v2_der: v2_der,
      v2_cert: v2_cert,
      v2_key: v2_key
    }
  end

  # ===========================================================================
  # Test 1: Basic TLS connection works and uses the expected certificate
  # ===========================================================================

  describe "basic TLS connection" do
    test "connects via TLS, performs HELLO 3, and PING returns PONG", ctx do
      ref = unique_ref(:basic)
      {^ref, port} = start_tls_listener(ref, ctx.v1_cert, ctx.v1_key)

      sock = connect_tls_and_hello(port)
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      :ssl.close(sock)
      stop_tls_listener(ref)
    end

    test "connection uses the cert provided at listener startup", ctx do
      ref = unique_ref(:basic_cert)
      {^ref, port} = start_tls_listener(ref, ctx.v1_cert, ctx.v1_key)

      sock = connect_tls(port)
      assert peer_cert_der(sock) == ctx.v1_der

      :ssl.close(sock)
      stop_tls_listener(ref)
    end
  end

  # ===========================================================================
  # Test 2: Existing connection survives cert file replacement
  # ===========================================================================

  describe "existing connection after cert file replacement" do
    test "existing connection still works after cert files are overwritten", ctx do
      live_cert = Path.join(ctx.tmp_dir, "live-cert.pem")
      live_key = Path.join(ctx.tmp_dir, "live-key.pem")
      File.cp!(ctx.v1_cert, live_cert)
      File.cp!(ctx.v1_key, live_key)

      ref = unique_ref(:survive)
      {^ref, port} = start_tls_listener(ref, live_cert, live_key)

      # Establish a connection using cert v1.
      sock = connect_tls_and_hello(port)
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      # Overwrite the cert files on disk with cert v2.
      File.cp!(ctx.v2_cert, live_cert)
      File.cp!(ctx.v2_key, live_key)

      # The EXISTING connection should continue working (it uses the in-memory
      # TLS session, not the on-disk files).
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      send_cmd(sock, ["SET", "tls_rot_survive_#{System.unique_integer([:positive])}", "val"])
      assert recv_response(sock) == {:simple, "OK"}

      :ssl.close(sock)
      stop_tls_listener(ref)
    end

    test "existing connection still presents the original cert (v1)", ctx do
      live_cert = Path.join(ctx.tmp_dir, "live-cert2.pem")
      live_key = Path.join(ctx.tmp_dir, "live-key2.pem")
      File.cp!(ctx.v1_cert, live_cert)
      File.cp!(ctx.v1_key, live_key)

      ref = unique_ref(:survive_cert)
      {^ref, port} = start_tls_listener(ref, live_cert, live_key)

      sock = connect_tls(port)
      assert peer_cert_der(sock) == ctx.v1_der

      # Overwrite cert files with v2.
      File.cp!(ctx.v2_cert, live_cert)
      File.cp!(ctx.v2_key, live_key)

      # The already-established connection still has v1's cert in its TLS state.
      assert peer_cert_der(sock) == ctx.v1_der

      :ssl.close(sock)
      stop_tls_listener(ref)
    end
  end

  # ===========================================================================
  # Test 3: After listener restart, new connections use the new cert
  # ===========================================================================

  describe "new connections after listener restart use new cert" do
    test "restarting the listener with new cert files makes new connections use v2", ctx do
      live_cert = Path.join(ctx.tmp_dir, "live-cert3.pem")
      live_key = Path.join(ctx.tmp_dir, "live-key3.pem")
      File.cp!(ctx.v1_cert, live_cert)
      File.cp!(ctx.v1_key, live_key)

      ref = unique_ref(:restart)
      {^ref, port} = start_tls_listener(ref, live_cert, live_key)

      # Verify initial connection uses v1.
      sock_v1 = connect_tls(port)
      assert peer_cert_der(sock_v1) == ctx.v1_der
      :ssl.close(sock_v1)

      # Overwrite cert files with v2.
      File.cp!(ctx.v2_cert, live_cert)
      File.cp!(ctx.v2_key, live_key)

      # Restart the listener (stop + start with the same ref).
      stop_tls_listener(ref)
      {^ref, new_port} = start_tls_listener(ref, live_cert, live_key)

      # New connection after restart should use v2.
      sock_v2 = connect_tls(new_port)
      assert peer_cert_der(sock_v2) == ctx.v2_der

      :ssl.close(sock_v2)
      stop_tls_listener(ref)
    end

    test "new connections after restart are fully functional with the new cert", ctx do
      live_cert = Path.join(ctx.tmp_dir, "live-cert4.pem")
      live_key = Path.join(ctx.tmp_dir, "live-key4.pem")
      File.cp!(ctx.v1_cert, live_cert)
      File.cp!(ctx.v1_key, live_key)

      ref = unique_ref(:restart_func)
      {^ref, _port} = start_tls_listener(ref, live_cert, live_key)

      # Replace with v2 and restart.
      File.cp!(ctx.v2_cert, live_cert)
      File.cp!(ctx.v2_key, live_key)
      stop_tls_listener(ref)
      {^ref, new_port} = start_tls_listener(ref, live_cert, live_key)

      # Full round-trip test with v2 cert.
      sock = connect_tls_and_hello(new_port)
      key = "tls_rot_restart_#{System.unique_integer([:positive])}"

      send_cmd(sock, ["SET", key, "rotated_value"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == "rotated_value"

      # Confirm the cert is v2.
      assert peer_cert_der(sock) == ctx.v2_der

      :ssl.close(sock)
      stop_tls_listener(ref)
    end
  end

  # ===========================================================================
  # Test 4: Cert file replaced without restart -- new connections still use
  #         the old (startup-time) cert
  # ===========================================================================

  describe "cert file replaced without restart" do
    test "new connections still use the old cert when files change without restart", ctx do
      live_cert = Path.join(ctx.tmp_dir, "live-cert5.pem")
      live_key = Path.join(ctx.tmp_dir, "live-key5.pem")
      File.cp!(ctx.v1_cert, live_cert)
      File.cp!(ctx.v1_key, live_key)

      ref = unique_ref(:no_restart)
      {^ref, port} = start_tls_listener(ref, live_cert, live_key)

      # First connection -- should be v1.
      sock1 = connect_tls(port)
      assert peer_cert_der(sock1) == ctx.v1_der
      :ssl.close(sock1)

      # Overwrite files with v2, but do NOT restart the listener.
      File.cp!(ctx.v2_cert, live_cert)
      File.cp!(ctx.v2_key, live_key)

      # New connection -- should still be v1 because the listener cached the cert
      # at startup time.
      sock2 = connect_tls(port)
      assert peer_cert_der(sock2) == ctx.v1_der
      :ssl.close(sock2)

      stop_tls_listener(ref)
    end

    test "multiple new connections all use the startup cert, not the on-disk cert", ctx do
      live_cert = Path.join(ctx.tmp_dir, "live-cert6.pem")
      live_key = Path.join(ctx.tmp_dir, "live-key6.pem")
      File.cp!(ctx.v1_cert, live_cert)
      File.cp!(ctx.v1_key, live_key)

      ref = unique_ref(:no_restart_multi)
      {^ref, port} = start_tls_listener(ref, live_cert, live_key)

      # Overwrite with v2 immediately.
      File.cp!(ctx.v2_cert, live_cert)
      File.cp!(ctx.v2_key, live_key)

      # All new connections should get v1.
      for _i <- 1..5 do
        sock = connect_tls(port)
        assert peer_cert_der(sock) == ctx.v1_der
        :ssl.close(sock)
      end

      stop_tls_listener(ref)
    end
  end

  # ===========================================================================
  # Test 5: Expired certificate is rejected by the client
  # ===========================================================================

  describe "expired certificate" do
    @tag :requires_openssl
    test "client with verify_peer rejects an expired server certificate", ctx do
      case generate_expired_cert(ctx.tmp_dir) do
        :skip ->
          # openssl not available or failed -- skip gracefully.
          :ok

        {:ok, expired_cert, expired_key} ->
          ref = unique_ref(:expired)
          {^ref, port} = start_tls_listener(ref, expired_cert, expired_key)

          # Connect with certificate verification enabled.  A self-signed expired
          # cert will be rejected by verify_peer (either as unknown_ca or expired).
          result =
            :ssl.connect(
              ~c"127.0.0.1",
              port,
              [
                :binary,
                active: false,
                packet: :raw,
                verify: :verify_peer,
                cacerts: :public_key.cacerts_get()
              ],
              5_000
            )

          case result do
            {:error, _reason} ->
              # Expected: the TLS handshake fails due to certificate issues.
              assert true

            {:ok, sock} ->
              :ssl.close(sock)
              flunk("Expected TLS handshake to fail for expired cert, but it succeeded")
          end

          stop_tls_listener(ref)
      end
    end
  end

  # ===========================================================================
  # Test 6: Wrong CA rejected (mutual TLS)
  # ===========================================================================

  describe "wrong CA rejection (mutual TLS)" do
    test "server rejects a client cert not signed by the trusted CA", ctx do
      # Generate the "trusted" CA -- the server will only accept client certs
      # signed by this CA.
      {_trusted_ca_key, trusted_ca_der, _trusted_ca_cert_path, _trusted_ca_key_path} =
        generate_ca(ctx.tmp_dir, "TrustedCA")

      # Generate a "rogue" CA -- the client will present a cert signed by this CA.
      rogue_dir = Path.join(ctx.tmp_dir, "rogue")
      File.mkdir_p!(rogue_dir)

      {_rogue_ca_key, _rogue_ca_der, _rogue_ca_cert_path, _rogue_ca_key_path} =
        generate_ca(rogue_dir, "RogueCA")

      # Generate server cert.
      {_server_der, server_cert, server_key} =
        generate_self_signed(ctx.tmp_dir, "mtls_server")

      # Generate a rogue client cert (self-signed, not signed by TrustedCA).
      {_rogue_der, rogue_client_cert, rogue_client_key} =
        generate_self_signed(rogue_dir, "rogue_client")

      # Start TLS listener with mutual TLS.  The server trusts only TrustedCA.
      # Use a custom verify_fun that accepts self-signed CA certs (since
      # pkix_test_root_cert produces root certs) but only from the trusted CA.
      ref = unique_ref(:wrong_ca)

      {^ref, port} =
        start_tls_listener(ref, server_cert, server_key,
          cacerts: [trusted_ca_der],
          verify: :verify_peer,
          fail_if_no_peer_cert: true
        )

      # Attempt to connect with the rogue client cert.
      # The server should reject it because the rogue cert is not trusted.
      result =
        :ssl.connect(
          ~c"127.0.0.1",
          port,
          [
            :binary,
            active: false,
            packet: :raw,
            verify: :verify_none,
            certfile: String.to_charlist(rogue_client_cert),
            keyfile: String.to_charlist(rogue_client_key)
          ],
          5_000
        )

      case result do
        {:error, _reason} ->
          # Handshake failed immediately -- expected for wrong CA.
          assert true

        {:ok, sock} ->
          # TLS 1.3 may complete the handshake before the server sends the
          # fatal alert.  The next operation on the socket should fail.
          send_result = :ssl.send(sock, IO.iodata_to_binary(Encoder.encode(["PING"])))
          recv_result = :ssl.recv(sock, 0, 3_000)

          # At least one of send or recv must fail with an alert.
          assert send_result != :ok or match?({:error, _}, recv_result),
                 "Expected connection to fail after handshake for untrusted client cert"

          :ssl.close(sock)
      end

      stop_tls_listener(ref)
    end

    test "server accepts a client cert signed by the trusted CA", ctx do
      # Generate a CA that the server trusts.
      {_ca_key, _ca_der, _ca_cert_path, _ca_key_path} =
        generate_ca(ctx.tmp_dir, "MutualCA")

      # Generate server cert.
      {_server_der, server_cert, server_key} =
        generate_self_signed(ctx.tmp_dir, "mtls_accept_server")

      # Generate the client cert.  Since pkix_test_root_cert generates
      # self-signed root CA certs, the client's cert IS a root CA.
      {_ca_key2, ca2_der, _ca2_cert_path, _ca2_key_path} =
        generate_ca(ctx.tmp_dir, "ClientAsCA")

      # OTP's default SSL verification rejects self-signed peer certificates
      # with :selfsigned_peer even when they're in the cacerts trust store.
      # We need a custom verify_fun that accepts self-signed certs present
      # in the trust anchors -- this is the standard approach for mutual TLS
      # with self-signed root certificates.
      verify_fun =
        {fn
           _cert, {:bad_cert, :selfsigned_peer}, state ->
             {:valid, state}

           _cert, {:extension, _ext}, state ->
             {:unknown, state}

           _cert, :valid, state ->
             {:valid, state}

           _cert, :valid_peer, state ->
             {:valid, state}
         end, []}

      ref = unique_ref(:correct_ca)

      {^ref, port} =
        start_tls_listener(ref, server_cert, server_key,
          cacerts: [ca2_der],
          verify: :verify_peer,
          fail_if_no_peer_cert: true,
          verify_fun: verify_fun
        )

      # The client cert and key are the CA itself (since it's self-signed root).
      client_cert_path = Path.join(ctx.tmp_dir, "ClientAsCA-cert.pem")
      client_key_path = Path.join(ctx.tmp_dir, "ClientAsCA-key.pem")

      result =
        :ssl.connect(
          ~c"127.0.0.1",
          port,
          [
            :binary,
            active: false,
            packet: :raw,
            verify: :verify_none,
            certfile: String.to_charlist(client_cert_path),
            keyfile: String.to_charlist(client_key_path)
          ],
          5_000
        )

      case result do
        {:ok, sock} ->
          # Connection succeeded -- verify it's fully operational.
          send_cmd(sock, ["HELLO", "3"])
          _greeting = recv_response(sock)
          send_cmd(sock, ["PING"])
          assert recv_response(sock) == {:simple, "PONG"}
          :ssl.close(sock)

        {:error, reason} ->
          flunk(
            "Expected mutual TLS connection to succeed with trusted client cert, got: #{inspect(reason)}"
          )
      end

      stop_tls_listener(ref)
    end
  end

  # ===========================================================================
  # Test 7: Full rotation lifecycle (v1 -> v2 with verification at each step)
  # ===========================================================================

  describe "full rotation lifecycle" do
    test "v1 connection survives while v2 is adopted after restart", ctx do
      live_cert = Path.join(ctx.tmp_dir, "live-cert7.pem")
      live_key = Path.join(ctx.tmp_dir, "live-key7.pem")
      File.cp!(ctx.v1_cert, live_cert)
      File.cp!(ctx.v1_key, live_key)

      ref = unique_ref(:lifecycle)
      {^ref, port_v1} = start_tls_listener(ref, live_cert, live_key)

      # Step 1: Establish a long-lived connection with cert v1.
      sock_v1 = connect_tls_and_hello(port_v1)
      send_cmd(sock_v1, ["PING"])
      assert recv_response(sock_v1) == {:simple, "PONG"}
      assert peer_cert_der(sock_v1) == ctx.v1_der

      # Step 2: Replace cert files with v2 on disk.
      File.cp!(ctx.v2_cert, live_cert)
      File.cp!(ctx.v2_key, live_key)

      # Step 3: New connection (no restart) still gets v1.
      sock_pre_restart = connect_tls(port_v1)
      assert peer_cert_der(sock_pre_restart) == ctx.v1_der
      :ssl.close(sock_pre_restart)

      # Step 4: The v1 long-lived connection still works.
      send_cmd(sock_v1, ["PING"])
      assert recv_response(sock_v1) == {:simple, "PONG"}

      # Step 5: Restart the listener.
      stop_tls_listener(ref)

      # The old v1 connection's socket is now broken (listener gone).
      # A send may succeed briefly (OS buffer) or fail -- either is fine.
      _send_result = :ssl.send(sock_v1, IO.iodata_to_binary(Encoder.encode(["PING"])))
      :ssl.close(sock_v1)

      # Step 6: Start with new cert files (v2).
      {^ref, port_v2} = start_tls_listener(ref, live_cert, live_key)

      # Step 7: New connections use v2.
      sock_v2 = connect_tls_and_hello(port_v2)
      send_cmd(sock_v2, ["PING"])
      assert recv_response(sock_v2) == {:simple, "PONG"}
      assert peer_cert_der(sock_v2) == ctx.v2_der

      :ssl.close(sock_v2)
      stop_tls_listener(ref)
    end
  end
end
