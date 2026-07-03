defmodule FerricStore.SDK.Native.ClientTest do
  use ExUnit.Case, async: false

  alias FerricStore.SDK.Native.Client
  alias FerricStore.SDK.Native.Topology

  setup_all do
    {:ok, _apps} = Application.ensure_all_started(:ferricstore_server)
    :ok
  end

  test "routes GET and SET through native SHARDS topology" do
    port = FerricstoreServer.Native.Listener.port()
    {:ok, client} = Client.start_link(seeds: [{"127.0.0.1", port}])

    key = "{sdk-native}:#{System.unique_integer([:positive])}"

    assert :ok = Client.set(client, key, "value")
    assert {:ok, "value"} = Client.get(client, key)

    topology = Client.topology(client)
    assert map_size(topology.endpoints) >= 1
    assert {:ok, route} = FerricStore.SDK.Native.Topology.route_key(topology, key)
    assert {:ok, ^route} = Client.route(client, key)
    assert route.lane_id == route.shard + 1
  end

  test "validates normalized seed endpoints before opening connections" do
    parent = self()
    trap_exit? = Process.flag(:trap_exit, true)

    validator = fn endpoint ->
      send(parent, {:validated_endpoint, endpoint})
      {:error, :blocked_endpoint}
    end

    try do
      assert {:error, :blocked_endpoint} =
               Client.start_link(
                 seeds: [{"93.184.216.34", 7777}],
                 server_name: "fs-sdk.example.com",
                 endpoint_validator: validator
               )
    after
      Process.flag(:trap_exit, trap_exit?)
    end

    assert_receive {:validated_endpoint,
                    %{
                      host: "93.184.216.34",
                      native_port: 7777,
                      server_name: "fs-sdk.example.com"
                    }}
  end

  test "public SDK facade supports generic keyed native requests" do
    port = FerricstoreServer.Native.Listener.port()
    {:ok, client} = FerricStore.SDK.start_link(seeds: [{"127.0.0.1", port}])

    key = "{sdk-facade}:#{System.unique_integer([:positive])}"

    assert :ok = FerricStore.SDK.set(client, key, "facade-value")

    assert {:ok, "facade-value"} =
             FerricStore.SDK.request_by_key(client, 0x0101, key, %{"key" => key})
  end

  test "refreshes topology and retries when a cached endpoint is stale" do
    port = FerricstoreServer.Native.Listener.port()
    {:ok, client} = Client.start_link(seeds: [{"127.0.0.1", port}])

    key = "{sdk-refresh}:#{System.unique_integer([:positive])}"
    assert {:ok, route} = Client.route(client, key)
    bad_port = unused_port()

    :sys.replace_state(client, fn state ->
      bad_endpoint = %{route.endpoint | native_port: bad_port}
      bad_endpoint_key = Topology.endpoint_key(bad_endpoint)
      bad_route = %{route | endpoint: bad_endpoint, endpoint_key: bad_endpoint_key}

      bad_topology = %{
        state.topology
        | slots: put_elem(state.topology.slots, route.slot, bad_route),
          endpoints: Map.put(state.topology.endpoints, bad_endpoint_key, bad_endpoint)
      }

      %{
        state
        | topology: bad_topology,
          connections: Map.delete(state.connections, route.endpoint_key)
      }
    end)

    assert :ok = Client.set(client, key, "after-refresh")
    assert {:ok, "after-refresh"} = Client.get(client, key)
    assert {:ok, refreshed_route} = Client.route(client, key)
    assert refreshed_route.endpoint.native_port == port
  end

  test "replaces cached connection process when its socket is closed but pid is alive" do
    port = FerricstoreServer.Native.Listener.port()
    {:ok, client} = Client.start_link(seeds: [{"127.0.0.1", port}])

    key = "{sdk-closed-socket}:#{System.unique_integer([:positive])}"
    assert :ok = Client.set(client, key, "before-close")
    assert {:ok, route} = Client.route(client, key)

    conn = :sys.get_state(client).connections[route.endpoint_key]
    conn_state = :sys.get_state(conn)
    :ok = apply(conn_state.transport, :close, [conn_state.socket])
    assert Process.alive?(conn)

    assert :ok = Client.set(client, key, "after-close")
    assert {:ok, "after-close"} = Client.get(client, key)

    replacement = :sys.get_state(client).connections[route.endpoint_key]
    assert replacement != conn
    assert Process.alive?(replacement)
  end

  test "refreshes topology from learned endpoints when original seeds are unavailable" do
    port = FerricstoreServer.Native.Listener.port()
    {:ok, client} = Client.start_link(seeds: [{"127.0.0.1", port}])

    key = "{sdk-refresh-learned}:#{System.unique_integer([:positive])}"
    assert {:ok, route} = Client.route(client, key)
    bad_port = unused_port()

    :sys.replace_state(client, fn state ->
      bad_seed = %{node: "127.0.0.1", host: "127.0.0.1", native_port: bad_port, tls: false}
      bad_endpoint = %{route.endpoint | native_port: bad_port}
      bad_endpoint_key = Topology.endpoint_key(bad_endpoint)
      bad_route = %{route | endpoint: bad_endpoint, endpoint_key: bad_endpoint_key}

      bad_topology = %{
        state.topology
        | slots: put_elem(state.topology.slots, route.slot, bad_route),
          endpoints: Map.put(state.topology.endpoints, bad_endpoint_key, bad_endpoint)
      }

      %{
        state
        | seeds: [bad_seed],
          topology: bad_topology,
          connections: Map.delete(state.connections, route.endpoint_key)
      }
    end)

    assert :ok = Client.set(client, key, "learned-endpoint-refresh")
    assert {:ok, "learned-endpoint-refresh"} = Client.get(client, key)
  end

  defp unused_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end
end
