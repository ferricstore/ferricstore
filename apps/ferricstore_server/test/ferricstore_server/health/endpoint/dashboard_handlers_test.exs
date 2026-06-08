defmodule FerricstoreServer.Health.Endpoint.DashboardHandlersTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Health.Endpoint.DashboardHandlers

  setup do
    old_protected_mode = Application.get_env(:ferricstore, :protected_mode)

    Acl.reset!()

    on_exit(fn ->
      restore_env(:protected_mode, old_protected_mode)
      Acl.reset!()
    end)

    :ok
  end

  test "flow lookup redirects to flow list when no id is given" do
    Application.put_env(:ferricstore, :protected_mode, false)

    assert :ok =
             DashboardHandlers.handle_flow_lookup(
               self(),
               __MODULE__.CaptureTransport,
               {10, 0, 0, 1},
               %{},
               ""
             )

    assert_receive {:response, response}
    assert response =~ "HTTP/1.1 302 Found"
    assert response =~ "Location: /dashboard/flow\r\n"
  end

  test "flow lookup redirects to encoded detail location with partition key" do
    Application.put_env(:ferricstore, :protected_mode, false)

    assert :ok =
             DashboardHandlers.handle_flow_lookup(
               self(),
               __MODULE__.CaptureTransport,
               {10, 0, 0, 1},
               %{},
               "id=flow%2F1&partition_key=tenant-a"
             )

    assert_receive {:response, response}
    assert response =~ "HTTP/1.1 302 Found"
    assert response =~ "Location: /dashboard/flow/flow%2F1?partition_key=tenant-a\r\n"
  end

  test "handlers return forbidden when observability is not authorized" do
    Application.put_env(:ferricstore, :protected_mode, true)

    assert :ok =
             DashboardHandlers.handle_flow_lookup(
               self(),
               __MODULE__.CaptureTransport,
               {10, 0, 0, 1},
               %{},
               "id=flow-1"
             )

    assert_receive {:response, response}
    assert response =~ "HTTP/1.1 403 Forbidden"
    assert response =~ ~s({"error":"forbidden"})
  end

  test "consensus handler redirects to raft page" do
    Application.put_env(:ferricstore, :protected_mode, false)

    assert :ok =
             DashboardHandlers.handle_consensus_redirect(
               self(),
               __MODULE__.CaptureTransport,
               {10, 0, 0, 1},
               %{}
             )

    assert_receive {:response, response}
    assert response =~ "HTTP/1.1 302 Found"
    assert response =~ "Location: /dashboard/raft\r\n"
  end

  defmodule CaptureTransport do
    def send(pid, iodata) do
      Kernel.send(pid, {:response, IO.iodata_to_binary(iodata)})
      :ok
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
