defmodule FerricstoreServer.Native.ListenerBackpressureTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Native.Listener

  test "TCP listener bounds stalled sends" do
    socket_opts = Listener.socket_opts(1)

    assert socket_opts[:send_timeout] ==
             Application.get_env(:ferricstore, :native_send_timeout_ms, 15_000)

    assert socket_opts[:send_timeout_close] == true
  end

  test "native connection always uses one-shot socket activation" do
    source =
      File.read!(
        Path.expand(
          "../../../lib/ferricstore_server/native/connection.ex",
          __DIR__
        )
      )

    assert source =~ "transport.setopts(socket, active: :once)"
    refute source =~ "Application.get_env(:ferricstore, :socket_active_mode"
    refute source =~ "active_mode: 100"
  end
end
