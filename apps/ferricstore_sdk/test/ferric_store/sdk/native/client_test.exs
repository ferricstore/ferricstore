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

  test "refreshes topology and retries single-key requests after native reroute" do
    port = FerricstoreServer.Native.Listener.port()
    {:ok, client} = Client.start_link(seeds: [{"127.0.0.1", port}])

    key = "{sdk-single-reroute}:#{System.unique_integer([:positive])}"
    assert :ok = Client.set(client, key, "single-reroute-value")
    assert {:ok, route} = Client.route(client, key)

    {:ok, reroute_conn} =
      __MODULE__.FakeConnection.start_link(
        parent: self(),
        name: :single_reroute,
        delay_ms: 0,
        reply: {:error, {:reroute, %{"reason" => "leader_changed"}}}
      )

    :sys.replace_state(client, fn state ->
      %{state | connections: Map.put(state.connections, route.endpoint_key, reroute_conn)}
    end)

    assert {:ok, "single-reroute-value"} = Client.get(client, key)
    assert_receive {:fake_request, :single_reroute, ^key}, 100
  end

  test "grouped requests on different endpoints do not serialize through slow shards" do
    port = FerricstoreServer.Native.Listener.port()
    {:ok, client} = Client.start_link(seeds: [{"127.0.0.1", port}])

    slow_key = "{sdk-group-slow}:#{System.unique_integer([:positive])}"
    fast_key = different_slot_key(slow_key)

    slow_endpoint = %{
      node: "group-slow@fixture",
      host: "127.0.0.1",
      native_port: unused_port(),
      tls: false
    }

    fast_endpoint = %{
      node: "group-fast@fixture",
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
        name: :group_slow,
        delay_ms: 250,
        reply: ["slow"]
      )

    {:ok, fast_conn} =
      __MODULE__.FakeConnection.start_link(
        parent: self(),
        name: :group_fast,
        delay_ms: 0,
        reply: ["fast"]
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

    request_task =
      Task.async(fn ->
        Client.request_by_keys(client, :mget, [slow_key, fast_key], fn keys ->
          %{"keys" => keys}
        end)
      end)

    assert_receive {:fake_request, :group_slow, [^slow_key]}, 100
    assert_receive {:fake_request, :group_fast, [^fast_key]}, 100

    assert {:ok, [slow_group, fast_group]} = Task.await(request_task, 1_000)
    assert slow_group.value == ["slow"]
    assert fast_group.value == ["fast"]
  end

  test "grouped requests do not replay completed shard groups after partial failure" do
    port = FerricstoreServer.Native.Listener.port()
    {:ok, client} = Client.start_link(seeds: [{"127.0.0.1", port}])

    ok_key = "{sdk-group-ok}:#{System.unique_integer([:positive])}"
    failing_key = different_slot_key(ok_key)

    ok_endpoint = %{
      node: "group-ok@fixture",
      host: "127.0.0.1",
      native_port: unused_port(),
      tls: false
    }

    failing_endpoint = %{
      node: "group-failing@fixture",
      host: "127.0.0.1",
      native_port: unused_port(),
      tls: false
    }

    ok_endpoint_key = Topology.endpoint_key(ok_endpoint)
    failing_endpoint_key = Topology.endpoint_key(failing_endpoint)
    ok_slot = Topology.slot_for_key(ok_key)
    failing_slot = Topology.slot_for_key(failing_key)

    {:ok, ok_conn} =
      __MODULE__.FakeConnection.start_link(
        parent: self(),
        name: :group_ok,
        delay_ms: 0,
        reply: ["ok"]
      )

    {:ok, failing_conn} =
      __MODULE__.FakeConnection.start_link(
        parent: self(),
        name: :group_failing,
        delay_ms: 0,
        reply: {:error, {:reroute, %{"reason" => "leader_changed"}}}
      )

    :sys.replace_state(client, fn state ->
      ok_route = %{
        shard: 1,
        lane_id: 2,
        endpoint_key: ok_endpoint_key,
        endpoint: ok_endpoint,
        leader_node: ok_endpoint.node
      }

      failing_route = %{
        shard: 2,
        lane_id: 3,
        endpoint_key: failing_endpoint_key,
        endpoint: failing_endpoint,
        leader_node: failing_endpoint.node
      }

      topology = %{
        state.topology
        | slots:
            state.topology.slots
            |> put_elem(ok_slot, ok_route)
            |> put_elem(failing_slot, failing_route),
          endpoints:
            state.topology.endpoints
            |> Map.put(ok_endpoint_key, ok_endpoint)
            |> Map.put(failing_endpoint_key, failing_endpoint)
      }

      %{
        state
        | topology: topology,
          connections:
            state.connections
            |> Map.put(ok_endpoint_key, ok_conn)
            |> Map.put(failing_endpoint_key, failing_conn)
      }
    end)

    assert {:error, {:partial_group_failure, {:reroute, %{"reason" => "leader_changed"}}, 1}} =
             Client.request_by_keys(client, :mget, [ok_key, failing_key], fn keys ->
               %{"keys" => keys}
             end)

    assert_receive {:fake_request, :group_ok, [^ok_key]}, 100
    assert_receive {:fake_request, :group_failing, [^failing_key]}, 100
    refute_receive {:fake_request, :group_ok, [^ok_key]}, 100
  end

  test "refreshes topology and retries grouped requests after native reroute" do
    port = FerricstoreServer.Native.Listener.port()
    {:ok, client} = Client.start_link(seeds: [{"127.0.0.1", port}])

    key = "{sdk-group-reroute}:#{System.unique_integer([:positive])}"
    assert :ok = Client.set(client, key, "group-reroute-value")
    assert {:ok, route} = Client.route(client, key)

    {:ok, reroute_conn} =
      __MODULE__.FakeConnection.start_link(
        parent: self(),
        name: :reroute,
        delay_ms: 0,
        reply: {:error, {:reroute, %{"reason" => "leader_changed"}}}
      )

    :sys.replace_state(client, fn state ->
      %{state | connections: Map.put(state.connections, route.endpoint_key, reroute_conn)}
    end)

    assert {:ok, [group]} =
             Client.request_by_keys(client, :mget, [key], fn keys -> %{"keys" => keys} end)

    assert group.indexes == [0]
    assert group.items == [key]
    assert group.value == ["group-reroute-value"]
    assert_receive {:fake_request, :reroute, [^key]}, 100
  end

  test "refreshes topology and retries grouped requests when send fails before write" do
    port = FerricstoreServer.Native.Listener.port()
    {:ok, client} = Client.start_link(seeds: [{"127.0.0.1", port}])

    key = "{sdk-group-send-failed}:#{System.unique_integer([:positive])}"
    assert :ok = Client.set(client, key, "group-send-failed-value")
    assert {:ok, route} = Client.route(client, key)

    {:ok, send_failed_conn} =
      __MODULE__.FakeConnection.start_link(
        parent: self(),
        name: :send_failed,
        delay_ms: 0,
        reply: {:error, {:send_failed, :closed}}
      )

    :sys.replace_state(client, fn state ->
      %{state | connections: Map.put(state.connections, route.endpoint_key, send_failed_conn)}
    end)

    assert {:ok, [group]} =
             Client.request_by_keys(client, :mget, [key], fn keys -> %{"keys" => keys} end)

    assert group.indexes == [0]
    assert group.items == [key]
    assert group.value == ["group-send-failed-value"]
    assert_receive {:fake_request, :send_failed, [^key]}, 100
  end

  test "does not replay routed requests when the cached connection closes after send" do
    port = FerricstoreServer.Native.Listener.port()
    {:ok, client} = Client.start_link(seeds: [{"127.0.0.1", port}])

    key = "{sdk-no-replay-closed}:#{System.unique_integer([:positive])}"
    assert :ok = Client.set(client, key, "durable-value")
    assert {:ok, route} = Client.route(client, key)

    {:ok, closed_after_send_conn} =
      __MODULE__.FakeConnection.start_link(
        parent: self(),
        name: :closed_after_send,
        delay_ms: 0,
        reply: {:error, :closed}
      )

    :sys.replace_state(client, fn state ->
      %{
        state
        | connections: Map.put(state.connections, route.endpoint_key, closed_after_send_conn)
      }
    end)

    assert {:error, :closed} = Client.get(client, key)
    assert_receive {:fake_request, :closed_after_send, ^key}, 100
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
      send(state.parent, {:fake_request, state.name, payload["key"] || payload["keys"]})
      Process.sleep(state.delay_ms)

      case state.reply do
        {:error, _reason} = error -> {:reply, error, state}
        value -> {:reply, {:ok, value}, state}
      end
    end
  end
end
