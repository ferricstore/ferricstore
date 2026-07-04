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

  test "public SDK facade supports URL connections, command compatibility, and close" do
    port = FerricstoreServer.Native.Listener.port()
    {:ok, client} = FerricStore.SDK.start_link(url: "ferric://127.0.0.1:#{port}")

    assert "PONG" = FerricStore.SDK.command(client, "PING", [])
    assert :ok = FerricStore.SDK.close(client)
    refute Process.alive?(client)
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

  test "rejects learned endpoints outside trusted seed hosts by default" do
    port = FerricstoreServer.Native.Listener.port()
    {:ok, client} = Client.start_link(seeds: [{"127.0.0.1", port}])

    key = "{sdk-untrusted}:#{System.unique_integer([:positive])}"
    assert {:ok, route} = Client.route(client, key)

    hostile_endpoint = %{
      node: "evil@metadata",
      host: "192.0.2.1",
      native_port: 1,
      tls: false,
      connect_timeout: 50
    }

    hostile_endpoint_key = Topology.endpoint_key(hostile_endpoint)
    hostile_route = %{route | endpoint: hostile_endpoint, endpoint_key: hostile_endpoint_key}

    :sys.replace_state(client, fn state ->
      bad_topology = %{
        state.topology
        | slots: put_elem(state.topology.slots, route.slot, hostile_route),
          endpoints: Map.put(state.topology.endpoints, hostile_endpoint_key, hostile_endpoint)
      }

      %{
        state
        | topology: bad_topology,
          connections: Map.delete(state.connections, hostile_endpoint_key)
      }
    end)

    assert {:error, :unsafe_endpoint} = Client.get(client, key)
  end

  test "already-routed requests on different endpoints do not serialize through the client" do
    port = FerricstoreServer.Native.Listener.port()
    {:ok, client} = Client.start_link(seeds: [{"127.0.0.1", port}])

    slow_key = "{sdk-slow}:#{System.unique_integer([:positive])}"
    fast_key = different_slot_key(slow_key)

    slow_endpoint = %{
      node: "slow@fixture",
      host: "127.0.0.1",
      native_port: unused_port(),
      tls: false
    }

    fast_endpoint = %{
      node: "fast@fixture",
      host: "127.0.0.1",
      native_port: unused_port(),
      tls: false
    }

    slow_endpoint_key = Topology.endpoint_key(slow_endpoint)
    fast_endpoint_key = Topology.endpoint_key(fast_endpoint)
    slow_slot = Topology.slot_for_key(slow_key)
    fast_slot = Topology.slot_for_key(fast_key)

    {:ok, slow_conn} =
      __MODULE__.FakeConnection.start_link(
        parent: self(),
        name: :slow,
        delay_ms: 250,
        reply: "slow"
      )

    {:ok, fast_conn} =
      __MODULE__.FakeConnection.start_link(
        parent: self(),
        name: :fast,
        delay_ms: 0,
        reply: "fast"
      )

    :sys.replace_state(client, fn state ->
      slow_route = %{
        shard: 1,
        lane_id: 2,
        endpoint_key: slow_endpoint_key,
        endpoint: slow_endpoint,
        leader_node: slow_endpoint.node
      }

      fast_route = %{
        shard: 2,
        lane_id: 3,
        endpoint_key: fast_endpoint_key,
        endpoint: fast_endpoint,
        leader_node: fast_endpoint.node
      }

      topology = %{
        state.topology
        | slots:
            state.topology.slots
            |> put_elem(slow_slot, slow_route)
            |> put_elem(fast_slot, fast_route),
          endpoints:
            state.topology.endpoints
            |> Map.put(slow_endpoint_key, slow_endpoint)
            |> Map.put(fast_endpoint_key, fast_endpoint)
      }

      %{
        state
        | topology: topology,
          connections:
            state.connections
            |> Map.put(slow_endpoint_key, slow_conn)
            |> Map.put(fast_endpoint_key, fast_conn)
      }
    end)

    slow_task = Task.async(fn -> Client.get(client, slow_key) end)
    assert_receive {:fake_request, :slow, ^slow_key}, 100

    fast_task = Task.async(fn -> Client.get(client, fast_key) end)

    assert {:ok, "fast"} = Task.await(fast_task, 100)
    assert {:ok, "slow"} = Task.await(slow_task, 1_000)
  end

  test "public TLS trust options are carried into seed endpoint validation" do
    parent = self()
    trap_exit? = Process.flag(:trap_exit, true)

    validator = fn endpoint ->
      send(parent, {:validated_endpoint, endpoint})
      {:error, :blocked_endpoint}
    end

    try do
      assert {:error, :blocked_endpoint} =
               Client.start_link(
                 seeds: [{"db.internal", 6389}],
                 tls: true,
                 server_name: "fs-sdk.example.com",
                 verify: false,
                 cacertfile: "/tmp/ca.pem",
                 endpoint_validator: validator
               )
    after
      Process.flag(:trap_exit, trap_exit?)
    end

    assert_receive {:validated_endpoint,
                    %{
                      host: "db.internal",
                      native_port: 6389,
                      tls: true,
                      server_name: "fs-sdk.example.com",
                      verify: false,
                      cacertfile: "/tmp/ca.pem"
                    }}
  end

  test "rejects unknown URL schemes instead of falling back to plaintext" do
    assert {:error, {:invalid_url_scheme, "https"}} =
             Client.from_url("https://127.0.0.1:6388")
  end

  defp unused_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp different_slot_key(existing_key) do
    existing_slot = Topology.slot_for_key(existing_key)

    Stream.iterate(1, &(&1 + 1))
    |> Enum.find_value(fn index ->
      key = "{sdk-fast-#{index}}:#{System.unique_integer([:positive])}"

      if Topology.slot_for_key(key) == existing_slot do
        nil
      else
        key
      end
    end)
  end

  defmodule FakeConnection do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, Map.new(opts))

    @impl true
    def init(opts), do: {:ok, opts}

    @impl true
    def handle_call({:request, _opcode, payload, _lane_id, _timeout}, _from, state) do
      send(state.parent, {:fake_request, state.name, payload["key"]})
      Process.sleep(state.delay_ms)
      {:reply, {:ok, state.reply}, state}
    end
  end
end
