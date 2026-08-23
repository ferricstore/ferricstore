defmodule FerricstoreHttp.Test.HttpHelpers do
  @moduledoc false

  @spec start_server(keyword()) :: FerricstoreHttp.Config.t()
  def start_server(overrides \\ []) do
    defaults = [
      enabled: true,
      port: 0,
      backend: FerricstoreHttp.TestBackend,
      max_body_bytes: 1_024
    ]

    {:ok, config} = FerricstoreHttp.Config.new(Keyword.merge(defaults, overrides))
    ExUnit.Callbacks.start_supervised!({FerricstoreHttp.Server, config})
    config
  end

  @spec request(atom(), binary(), binary(), [{binary(), binary()}], binary()) ::
          {non_neg_integer(), [{binary(), binary()}], binary()}
  def request(method, path, content_type \\ "application/json", headers \\ [], body \\ "") do
    :ok = ensure_inets()
    url = String.to_charlist("http://127.0.0.1:#{FerricstoreHttp.Listener.port()}#{path}")

    request =
      case method do
        :get ->
          {url, charlist_headers(headers)}

        _method ->
          {url, charlist_headers(headers), String.to_charlist(content_type), body}
      end

    assert_http_response(:httpc.request(method, request, [], body_format: :binary))
  end

  @spec connect() :: {:ok, port()}
  def connect do
    :gen_tcp.connect(
      {127, 0, 0, 1},
      FerricstoreHttp.Listener.port(),
      [:binary, active: false, nodelay: true],
      2_000
    )
  end

  @spec raw_request(port(), iodata(), timeout()) ::
          {:ok, non_neg_integer(), [{binary(), binary()}], binary()} | {:error, term()}
  def raw_request(socket, request, timeout \\ 2_000) do
    with :ok <- :gen_tcp.send(socket, request) do
      receive_response(socket, timeout)
    end
  end

  @spec receive_response(port(), timeout()) ::
          {:ok, non_neg_integer(), [{binary(), binary()}], binary()} | {:error, term()}
  def receive_response(socket, timeout \\ 2_000) do
    with {:ok, headers, remaining} <- receive_headers(socket, "", timeout),
         [status_line | header_lines] <- String.split(headers, "\r\n", trim: true),
         [_, status, _reason] <- String.split(status_line, " ", parts: 3),
         {status, ""} <- Integer.parse(status),
         headers <- normalize_raw_headers(header_lines),
         {:ok, body} <- receive_response_body(socket, headers, remaining, timeout) do
      {:ok, status, headers, body}
    else
      _invalid -> {:error, :malformed_http_response}
    end
  end

  @spec basic(binary(), binary()) :: {binary(), binary()}
  def basic(username, password) do
    {"authorization", "Basic " <> Base.encode64("#{username}:#{password}")}
  end

  @spec tls_files() :: map()
  def tls_files do
    root_key = rsa_key()
    root_key_identifier = key_identifier(root_key)

    root =
      :public_key.pkix_test_root_cert(
        ~c"FerricStore HTTP test CA",
        certificate_options(root_key, key_identifier_extensions(root_key_identifier))
      )

    peer_key = rsa_key()
    peer_key_identifier = key_identifier(peer_key)

    tls_config =
      :public_key.pkix_test_data(%{
        root: root,
        intermediates: [],
        peer:
          certificate_options(peer_key, [
            subject_alt_name_extension(),
            subject_key_identifier_extension(peer_key_identifier),
            authority_key_identifier_extension(root_key_identifier)
          ])
      })

    certificate = Keyword.fetch!(tls_config, :cert)
    {key_type, key_der} = Keyword.fetch!(tls_config, :key)
    directory = Path.join(System.tmp_dir!(), "ferricstore_http_tls_#{unique_id()}")
    certfile = Path.join(directory, "server-cert.pem")
    keyfile = Path.join(directory, "server-key.pem")
    cafile = Path.join(directory, "test-ca.pem")

    File.mkdir_p!(directory)
    File.write!(certfile, :public_key.pem_encode([{:Certificate, certificate, :not_encrypted}]))
    File.write!(keyfile, :public_key.pem_encode([{key_type, key_der, :not_encrypted}]))
    File.write!(cafile, encode_certificates(Keyword.fetch!(tls_config, :cacerts)))

    %{directory: directory, certfile: certfile, keyfile: keyfile, cafile: cafile}
  end

  @doc """
  Creates a conventional PEM CA and server certificate for external SDK tests.

  OTP's `pkix_test_data/1` output is ideal for Erlang/OTP tests, but some
  non-OTP trust stores reject its synthetic certificate extensions. The SDK
  integration suite deliberately uses OpenSSL here so Go, Elixir, Node.js,
  browsers, and other clients exercise the same portable TLS fixture.
  """
  @spec sdk_tls_files() :: map()
  def sdk_tls_files do
    openssl = System.find_executable("openssl") || raise "openssl is required for SDK TLS tests"
    directory = Path.join(System.tmp_dir!(), "ferricstore_http_sdk_tls_#{unique_id()}")
    cafile = Path.join(directory, "test-ca.pem")
    ca_keyfile = Path.join(directory, "test-ca-key.pem")
    certfile = Path.join(directory, "server-cert.pem")
    keyfile = Path.join(directory, "server-key.pem")
    certificate_request = Path.join(directory, "server.csr")
    extensions = Path.join(directory, "server-extensions.cnf")

    File.mkdir_p!(directory)

    openssl!(openssl, [
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-sha256",
      "-days",
      "1",
      "-subj",
      "/CN=FerricStore HTTP SDK test CA",
      "-addext",
      "basicConstraints=critical,CA:TRUE",
      "-addext",
      "keyUsage=critical,keyCertSign,cRLSign",
      "-keyout",
      ca_keyfile,
      "-out",
      cafile
    ])

    openssl!(openssl, [
      "req",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-sha256",
      "-subj",
      "/CN=localhost",
      "-keyout",
      keyfile,
      "-out",
      certificate_request
    ])

    File.write!(extensions, """
    basicConstraints=critical,CA:FALSE
    keyUsage=critical,digitalSignature,keyEncipherment
    extendedKeyUsage=serverAuth
    subjectAltName=DNS:localhost,IP:127.0.0.1
    """)

    openssl!(openssl, [
      "x509",
      "-req",
      "-sha256",
      "-days",
      "1",
      "-in",
      certificate_request,
      "-CA",
      cafile,
      "-CAkey",
      ca_keyfile,
      "-CAcreateserial",
      "-extfile",
      extensions,
      "-out",
      certfile
    ])

    %{directory: directory, certfile: certfile, keyfile: keyfile, cafile: cafile}
  end

  defp certificate_options(key, extensions) do
    [digest: :sha256, key: key, extensions: extensions]
  end

  defp rsa_key, do: :public_key.generate_key({:rsa, 2_048, 65_537})

  defp key_identifier(private_key) do
    public_key = {:RSAPublicKey, elem(private_key, 2), elem(private_key, 3)}
    :crypto.hash(:sha, :public_key.der_encode(:RSAPublicKey, public_key))
  end

  defp key_identifier_extensions(key_identifier) do
    [
      subject_key_identifier_extension(key_identifier),
      authority_key_identifier_extension(key_identifier)
    ]
  end

  defp subject_alt_name_extension do
    {:Extension, {2, 5, 29, 17}, false, [iPAddress: <<127, 0, 0, 1>>, dNSName: ~c"localhost"]}
  end

  defp subject_key_identifier_extension(key_identifier) do
    {:Extension, {2, 5, 29, 14}, false, key_identifier}
  end

  defp authority_key_identifier_extension(key_identifier) do
    value = {:AuthorityKeyIdentifier, key_identifier, :asn1_NOVALUE, :asn1_NOVALUE}
    {:Extension, {2, 5, 29, 35}, false, value}
  end

  defp encode_certificates(certificates) do
    certificates
    |> Enum.map(&{:Certificate, &1, :not_encrypted})
    |> :public_key.pem_encode()
  end

  defp charlist_headers(headers) do
    Enum.map(headers, fn {name, value} ->
      {String.to_charlist(name), String.to_charlist(value)}
    end)
  end

  defp receive_headers(socket, buffer, timeout) do
    case :binary.match(buffer, "\r\n\r\n") do
      {index, 4} ->
        headers = binary_part(buffer, 0, index)
        remaining = binary_part(buffer, index + 4, byte_size(buffer) - index - 4)
        {:ok, headers, remaining}

      :nomatch ->
        with {:ok, bytes} <- :gen_tcp.recv(socket, 0, timeout) do
          receive_headers(socket, buffer <> bytes, timeout)
        end
    end
  end

  defp normalize_raw_headers(lines) do
    Enum.flat_map(lines, fn line ->
      case String.split(line, ":", parts: 2) do
        [name, value] -> [{String.downcase(name), String.trim(value)}]
        _invalid -> []
      end
    end)
  end

  defp receive_response_body(socket, headers, remaining, timeout) do
    case List.keyfind(headers, "content-length", 0) do
      {"content-length", value} ->
        case Integer.parse(value) do
          {length, ""} -> receive_bytes(socket, remaining, length, timeout)
          _invalid -> {:error, :invalid_content_length}
        end

      nil ->
        {:ok, remaining}
    end
  end

  defp receive_bytes(_socket, buffer, length, _timeout) when byte_size(buffer) >= length,
    do: {:ok, binary_part(buffer, 0, length)}

  defp receive_bytes(socket, buffer, length, timeout) do
    with {:ok, bytes} <- :gen_tcp.recv(socket, length - byte_size(buffer), timeout) do
      receive_bytes(socket, buffer <> bytes, length, timeout)
    end
  end

  defp assert_http_response({:ok, {{_version, status, _reason}, headers, body}}) do
    normalized_headers =
      Enum.map(headers, fn {name, value} ->
        {List.to_string(name) |> String.downcase(), List.to_string(value)}
      end)

    {status, normalized_headers, body}
  end

  defp ensure_inets do
    case :inets.start() do
      :ok -> :ok
      {:error, {:already_started, :inets}} -> :ok
    end
  end

  defp openssl!(openssl, arguments) do
    case System.cmd(openssl, arguments, stderr_to_stdout: true) do
      {_output, 0} -> :ok
      {output, status} -> raise "openssl failed with status #{status}: #{output}"
    end
  end

  defp unique_id, do: System.unique_integer([:positive, :monotonic])
end
