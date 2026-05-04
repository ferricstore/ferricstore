defmodule FerricstoreServer.ConnectionSendTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Connection.Send

  defmodule OkTransport do
    def send(socket, iodata) do
      Kernel.send(socket, {:transport_send, iodata})
      :ok
    end
  end

  defmodule ErrorTransport do
    def send(_socket, _iodata), do: {:error, :closed}
  end

  test "successful send returns ok without telemetry" do
    ref = make_ref()
    handler_id = "connection-send-ok-#{inspect(ref)}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore_server, :connection, :send_failed],
        fn event, measurements, metadata, _ ->
          send(test_pid, {:telemetry, ref, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert :ok = Send.send(self(), OkTransport, ["+OK", "\r\n"], :response)
    assert_receive {:transport_send, ["+OK", "\r\n"]}
    refute_receive {:telemetry, ^ref, _, _, _}, 50
  end

  test "failed send emits telemetry with phase and reason" do
    ref = make_ref()
    handler_id = "connection-send-failed-#{inspect(ref)}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore_server, :connection, :send_failed],
        fn event, measurements, metadata, _ ->
          send(test_pid, {:telemetry, ref, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert {:error, :closed} =
             Send.send(:socket, ErrorTransport, ["+OK", "\r\n"], :pubsub_message, %{
               client_id: 123
             })

    assert_receive {:telemetry, ^ref, [:ferricstore_server, :connection, :send_failed],
                    %{count: 1}, %{phase: :pubsub_message, reason: :closed, client_id: 123}}
  end
end
