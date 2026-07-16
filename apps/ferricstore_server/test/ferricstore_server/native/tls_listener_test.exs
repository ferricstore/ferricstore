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

    test "uses the configured global maxclients capacity" do
      transport_opts =
        child_transport_opts(
          port: 1,
          certfile: "/cert.pem",
          keyfile: "/key.pem"
        )

      assert transport_opts.max_connections ==
               Application.get_env(:ferricstore, :maxclients, 10_000)
    end

    test "bounds stalled TLS sends and closes timed out sockets" do
      socket_opts =
        child_socket_opts(
          port: 1,
          certfile: "/cert.pem",
          keyfile: "/key.pem"
        )

      assert socket_opts[:send_timeout] ==
               Application.get_env(:ferricstore, :native_send_timeout_ms, 15_000)

      assert socket_opts[:send_timeout_close] == true
    end

    @tag :tls_tcp_option_parity
    test "inherits the native listener TCP performance options" do
      socket_opts =
        child_socket_opts(
          port: 1,
          certfile: "/cert.pem",
          keyfile: "/key.pem",
          nodelay: false,
          backlog: 321
        )

      assert socket_opts[:nodelay] == false
      assert socket_opts[:backlog] == 321
      assert socket_opts[:keepalive] == true
      assert socket_opts[:recbuf] == Application.get_env(:ferricstore, :tcp_recbuf, 131_072)
      assert socket_opts[:sndbuf] == Application.get_env(:ferricstore, :tcp_sndbuf, 131_072)
    end
  end

  defp child_socket_opts(opts) do
    opts
    |> child_transport_opts()
    |> Map.fetch!(:socket_opts)
  end

  defp child_transport_opts(opts) do
    assert %{
             start:
               {:ranch_embedded_sup, :start_link,
                [
                  TlsListener,
                  :ranch_ssl,
                  transport_opts,
                  FerricstoreServer.Native.Connection,
                  _protocol_opts
                ]}
           } = TlsListener.child_spec_if_configured(opts)

    transport_opts
  end
end
