defmodule FerricstoreHttp.TlsTest do
  use ExUnit.Case, async: false

  alias FerricstoreHttp.Test.HttpHelpers

  test "negotiates TLS 1.2 and TLS 1.3 independently" do
    :ok = ensure_inets()
    files = HttpHelpers.tls_files()
    on_exit(fn -> File.rm_rf!(files.directory) end)

    HttpHelpers.start_server(
      tls: [enabled: true, certfile: files.certfile, keyfile: files.keyfile]
    )

    url = ~c"https://127.0.0.1:#{FerricstoreHttp.Listener.port()}/health"

    for version <- [:"tlsv1.2", :"tlsv1.3"] do
      ssl_options = [verify: :verify_none, versions: [version]]

      assert {:ok, {{_http_version, 200, _reason}, _headers, response_body}} =
               :httpc.request(:get, {url, []}, [ssl: ssl_options], body_format: :binary)

      assert Jason.decode!(response_body) == %{"status" => "ok"}
    end
  end

  test "advertises HTTP/2 over TLS only when enabled" do
    files = HttpHelpers.tls_files()
    on_exit(fn -> File.rm_rf!(files.directory) end)

    HttpHelpers.start_server(
      http2_enabled: true,
      tls: [enabled: true, certfile: files.certfile, keyfile: files.keyfile]
    )

    options = [
      active: false,
      verify: :verify_none,
      alpn_advertised_protocols: ["h2", "http/1.1"]
    ]

    assert {:ok, socket} =
             :ssl.connect({127, 0, 0, 1}, FerricstoreHttp.Listener.port(), options, 2_000)

    assert {:ok, "h2"} = :ssl.negotiated_protocol(socket)
    :ok = :ssl.close(socket)
  end

  test "serves a certificate that clients can verify against the configured CA" do
    :ok = ensure_inets()
    files = HttpHelpers.sdk_tls_files()
    on_exit(fn -> File.rm_rf!(files.directory) end)

    HttpHelpers.start_server(
      tls: [enabled: true, certfile: files.certfile, keyfile: files.keyfile]
    )

    url = ~c"https://localhost:#{FerricstoreHttp.Listener.port()}/health"

    ssl_options = [
      verify: :verify_peer,
      cacertfile: String.to_charlist(files.cafile),
      server_name_indication: ~c"localhost",
      customize_hostname_check: [match_fun: :public_key.pkix_verify_hostname_match_fun(:https)]
    ]

    assert {:ok, {{_http_version, 200, _reason}, _headers, response_body}} =
             :httpc.request(:get, {url, []}, [ssl: ssl_options], body_format: :binary)

    assert Jason.decode!(response_body) == %{"status" => "ok"}
  end

  defp ensure_inets do
    case :inets.start() do
      :ok -> :ok
      {:error, {:already_started, :inets}} -> :ok
    end
  end
end
