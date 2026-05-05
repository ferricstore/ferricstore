defmodule FerricstoreServer.Connection.SendfileTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Store.ActiveFile
  alias Ferricstore.Test.IsolatedInstance
  alias FerricstoreServer.ClientTracking
  alias FerricstoreServer.Connection
  alias FerricstoreServer.Connection.Pipeline
  alias FerricstoreServer.Connection.Sendfile
  alias FerricstoreServer.Resp.Parser

  setup do
    ClientTracking.init_tables()
    ActiveFile.init(1)
    :ets.delete_all_objects(:ferricstore_tracking)
    :ets.delete_all_objects(:ferricstore_tracking_connections)

    ctx = IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1024)

    on_exit(fn ->
      IsolatedInstance.checkin(ctx)
      ClientTracking.cleanup(self())
    end)

    {:ok, ctx: ctx}
  end

  test "MGET reuses prefetched cold values below stream threshold instead of dispatching again",
       %{
         ctx: ctx
       } do
    key1 = "mget-small-cold:1"
    key2 = "mget-small-cold:2"
    value1 = :binary.copy("a", ctx.hot_cache_max_value_size + 256)
    value2 = :binary.copy("b", ctx.hot_cache_max_value_size + 512)

    :ok = Router.batch_put(ctx, [{key1, value1}, {key2, value2}])

    state = %{
      instance_ctx: ctx,
      sandbox_namespace: nil,
      pubsub_channels: nil,
      tracking: nil
    }

    fallback = fn _cmd, _args, _state ->
      flunk("MGET fallback should not run after batch_get_with_file_refs returned values")
    end

    assert {:continue, encoded, ^state} = Sendfile.dispatch_mget([key1, key2], state, fallback)
    assert {:ok, [[^value1, ^value2]], ""} = Parser.parse(IO.iodata_to_binary(encoded))
  end

  test "GET tracks client-visible sandbox key, not internal lookup key", %{ctx: ctx} do
    sandbox = "sandbox:" <> Integer.to_string(System.unique_integer([:positive])) <> ":"
    key = "tracked-hot-get"
    lookup_key = sandbox <> key

    :ok = Router.put(ctx, lookup_key, "v1", 0)
    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])

    state = %{
      instance_ctx: ctx,
      sandbox_namespace: sandbox,
      pubsub_channels: nil,
      tracking: tracking
    }

    fallback = fn _cmd, _args, _state ->
      flunk("GET fallback should not run for hot sandbox value")
    end

    assert {:continue, encoded, new_state} = Sendfile.dispatch_get([key], state, fallback)
    assert IO.iodata_to_binary(encoded) == "$2\r\nv1\r\n"
    assert new_state.tracking.enabled
    assert :ets.lookup(:ferricstore_tracking, key) == [{key, self()}]
    assert :ets.lookup(:ferricstore_tracking, lookup_key) == []
  end

  test "general pipeline keeps tracking from non-streaming reads when later cold reads stream",
       %{ctx: ctx} do
    hot_key = "pipeline-hot-tracked"
    cold_key = "pipeline-cold-streamed"
    cold_value = :binary.copy("c", 70_000)

    :ok = Router.put(ctx, hot_key, "hot", 0)
    :ok = Router.put(ctx, cold_key, cold_value, 0)

    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])
    {server_socket, client_socket} = tcp_pair()

    state = %Connection{
      socket: server_socket,
      transport: :ranch_tcp,
      instance_ctx: ctx,
      stats_counter: ctx.stats_counter,
      authenticated: true,
      require_auth: false,
      acl_cache: :full_access,
      tracking: tracking
    }

    commands = [
      {:command, "GET", [hot_key], {:get, hot_key}, [hot_key]},
      {:command, "GET", [cold_key], {:get, cold_key}, [cold_key]},
      {:command, "PING", [], :ping, []}
    ]

    send_response = fn socket, :ranch_tcp, response -> :gen_tcp.send(socket, response) end
    handle_command = fn command, _state -> flunk("unexpected fallback: #{inspect(command)}") end

    try do
      assert {:continue, _new_state} =
               Pipeline.pipeline_dispatch(commands, state, handle_command, send_response)

      assert :ets.lookup(:ferricstore_tracking, hot_key) == [{hot_key, self()}]
      assert :ets.lookup(:ferricstore_tracking, cold_key) == [{cold_key, self()}]
    after
      :gen_tcp.close(server_socket)
      :gen_tcp.close(client_socket)
    end
  end

  defp tcp_pair do
    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, {:packet, :raw}, {:active, false}, {:reuseaddr, true}])

    {:ok, port} = :inet.port(listen)
    acceptor = Task.async(fn -> :gen_tcp.accept(listen) end)
    {:ok, client_socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, {:active, false}])
    {:ok, server_socket} = Task.await(acceptor)
    :gen_tcp.close(listen)
    {server_socket, client_socket}
  end
end
