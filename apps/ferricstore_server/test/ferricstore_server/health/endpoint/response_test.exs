defmodule FerricstoreServer.Health.Endpoint.ResponseTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Endpoint.Response

  defmodule CaptureTransport do
    def send(socket, payload) do
      Kernel.send(socket, {:socket_send, IO.iodata_to_binary(payload)})
      :ok
    end
  end

  test "redirects reject response-header injection" do
    assert_raise ArgumentError, ~r/invalid HTTP header value/, fn ->
      Response.send_redirect_response(
        self(),
        CaptureTransport,
        "/dashboard\r\nX-Injected: true"
      )
    end

    refute_receive {:socket_send, _payload}
  end

  test "extra response headers reject invalid names and values" do
    for headers <- [
          [{"X-Test\r\nX-Injected", "true"}],
          [{"X-Test", "true\nX-Injected: true"}]
        ] do
      assert_raise ArgumentError, ~r/invalid HTTP header/, fn ->
        Response.send_redirect_response(self(), CaptureTransport, "/dashboard", headers)
      end

      refute_receive {:socket_send, _payload}
    end
  end
end
