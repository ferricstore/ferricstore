defmodule FerricStore.SDK.Native.ConnectionTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.Connection

  test "TLS connections verify peer and hostname by default" do
    opts =
      Connection.tls_options(%{
        host: "db.internal",
        native_port: 6389,
        tls: true,
        cacertfile: "/tmp/ca.pem"
      })

    assert Keyword.fetch!(opts, :verify) == :verify_peer
    assert Keyword.fetch!(opts, :server_name_indication) == ~c"db.internal"
    assert Keyword.fetch!(opts, :cacertfile) == "/tmp/ca.pem"
    assert Keyword.has_key?(opts, :customize_hostname_check)
  end

  test "TLS verification can pin a server name while dialing a resolved address" do
    opts =
      Connection.tls_options(%{
        host: "93.184.216.34",
        server_name: "db.internal",
        native_port: 6389,
        tls: true,
        cacertfile: "/tmp/ca.pem"
      })

    assert Keyword.fetch!(opts, :verify) == :verify_peer
    assert Keyword.fetch!(opts, :server_name_indication) == ~c"db.internal"
    assert Keyword.fetch!(opts, :cacertfile) == "/tmp/ca.pem"
  end

  test "TLS verification can be explicitly disabled for local development" do
    opts =
      Connection.tls_options(%{
        host: "127.0.0.1",
        native_port: 6389,
        tls: true,
        verify: false
      })

    assert Keyword.fetch!(opts, :verify) == :verify_none
    refute Keyword.has_key?(opts, :customize_hostname_check)
  end
end
