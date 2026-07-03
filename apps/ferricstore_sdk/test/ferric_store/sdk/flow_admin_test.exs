defmodule FerricStore.SDK.FlowAdminTest do
  use ExUnit.Case, async: false

  alias FerricStore.SDK.Admin
  alias FerricStore.SDK.Flow

  setup_all do
    {:ok, _apps} = Application.ensure_all_started(:ferricstore_server)
    :ok
  end

  setup do
    port = FerricstoreServer.Native.Listener.port()
    {:ok, client} = FerricStore.SDK.start_link(seeds: [{"127.0.0.1", port}])
    {:ok, client: client}
  end

  test "supports control requests and raw command execution", %{client: client} do
    assert {:ok, "PONG"} = FerricStore.SDK.ping(client)
    assert {:ok, "native"} = FerricStore.SDK.ping(client, "native")
    assert {:ok, "PONG"} = FerricStore.SDK.command_exec(client, "PING", [])
  end

  test "supports admin requests through the native client", %{client: client} do
    key = "{sdk-admin}:#{System.unique_integer([:positive])}"

    assert {:ok, slot} =
             Admin.cluster_keyslot(client, %{"key" => key, "args" => [key]})

    assert is_integer(slot)
    assert slot in 0..1023
  end

  test "supports Flow create and get through id-routed native wrappers", %{client: client} do
    id = "sdk-flow-#{System.unique_integer([:positive])}"

    assert {:ok, "OK"} =
             Flow.create(client, %{
               "id" => id,
               "type" => "sdk",
               "state" => "queued",
               "payload" => "payload"
             })

    assert {:ok, flow} = Flow.get(client, %{"id" => id, "full" => true})
    assert flow["id"] == id
    assert flow["type"] == "sdk"
    assert flow["state"] == "queued"
  end
end
