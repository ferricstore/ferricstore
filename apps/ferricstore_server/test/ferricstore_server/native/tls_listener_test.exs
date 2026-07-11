defmodule FerricstoreServer.Native.TlsListenerTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Native.TlsListener

  describe "TLS socket options" do
    test "requires a client certificate when a CA file is configured" do
      socket_opts =
        child_socket_opts(
          port: 1,
          certfile: "/cert.pem",
          keyfile: "/key.pem",
          cacertfile: "/ca.pem"
        )

      assert socket_opts[:cacertfile] == "/ca.pem"
      assert socket_opts[:verify] == :verify_peer
      assert socket_opts[:fail_if_no_peer_cert] == true
    end

    test "does not enable peer certificate verification when no CA file is configured" do
      socket_opts =
        child_socket_opts(
          port: 1,
          certfile: "/cert.pem",
          keyfile: "/key.pem"
        )

      refute Keyword.has_key?(socket_opts, :cacertfile)
      refute Keyword.has_key?(socket_opts, :verify)
      refute Keyword.has_key?(socket_opts, :fail_if_no_peer_cert)
    end
  end

  defp child_socket_opts(opts) do
    assert %{
             start:
               {:ranch_embedded_sup, :start_link,
                [
                  TlsListener,
                  :ranch_ssl,
                  %{socket_opts: socket_opts},
                  FerricstoreServer.Native.Connection,
                  _protocol_opts
                ]}
           } = TlsListener.child_spec_if_configured(opts)

    socket_opts
  end
end
