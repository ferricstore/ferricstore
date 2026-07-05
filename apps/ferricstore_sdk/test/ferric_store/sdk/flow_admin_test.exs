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

  test "supports Flow state_meta and indexed_state_meta through native wrappers", %{
    client: client
  } do
    suffix = System.unique_integer([:positive])
    type = "sdk-state-meta-#{suffix}"
    partition = "sdk-state-meta-partition"
    id = "sdk-state-meta-flow-#{suffix}"

    assert {:ok, policy} =
             Flow.policy_set(client, %{
               "type" => type,
               "indexed_state_meta" => "version"
             })

    assert policy["indexed_state_meta"] == "version"

    assert {:ok, "OK"} =
             Flow.create(client, %{
               "id" => id,
               "type" => type,
               "state" => "accept",
               "partition_key" => partition,
               "state_meta" => %{"version" => 1, "owner" => "risk"},
               "run_at_ms" => 1_000,
               "now_ms" => 1_000
             })

    assert {:ok, flow} = Flow.get(client, %{"id" => id, "partition_key" => partition})

    assert flow["state_meta"] == %{
             "accept" => %{"version" => 1, "owner" => "risk"}
           }

    assert_eventually(fn ->
      assert {:ok, records} =
               Flow.search(client, %{
                 "type" => type,
                 "partition_key" => partition,
                 "state_meta" => %{"accept" => %{"version" => 1}},
                 "consistent_projection" => true,
                 "count" => 10
               })

      assert Enum.map(records, & &1["id"]) == [id]
    end)
  end

  defp assert_eventually(fun, attempts \\ 40, interval_ms \\ 100)

  defp assert_eventually(fun, attempts, interval_ms) when attempts > 0 do
    fun.()
  rescue
    error in [ExUnit.AssertionError] ->
      if attempts == 1 do
        reraise(error, __STACKTRACE__)
      else
        Process.sleep(interval_ms)
        assert_eventually(fun, attempts - 1, interval_ms)
      end
  end
end
