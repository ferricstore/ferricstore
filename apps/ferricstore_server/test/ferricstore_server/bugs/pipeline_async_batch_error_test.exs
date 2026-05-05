defmodule FerricstoreServer.Bugs.PipelineAsyncBatchErrorTest do
  use ExUnit.Case, async: false

  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers
  alias FerricstoreServer.Connection
  alias FerricstoreServer.Connection.Pipeline

  @ns "pipeline_quorum_fast_path"

  setup do
    ShardHelpers.flush_all_keys()
    NamespaceConfig.reset_all()

    on_exit(fn ->
      NamespaceConfig.reset_all()
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  test "SET fast path ignores stale async durability mode and writes through quorum" do
    ctx = FerricStore.Instance.get(:default)
    stale_ctx = %{ctx | durability_mode: :all_async}
    key = "#{@ns}:stale_async:#{System.unique_integer([:positive])}"

    state = connection_state(stale_ctx)
    send_response_fn = capture_response_fn()
    handle_command_fn = flunking_handle_fn("SET pipeline fast path")

    commands = [
      {:command, "SET", [key, "v1"], {:set, key, "v1"}, [key]},
      set_ast("#{@ns}:same_batch:#{System.unique_integer([:positive])}", "v2")
    ]

    assert {:continue, ^state} =
             Pipeline.pipeline_dispatch(commands, state, handle_command_fn, send_response_fn)

    assert_receive {:pipeline_response, response}
    assert response == "+OK\r\n+OK\r\n"
    assert Router.get(ctx, key) == "v1"
  end

  test "mixed GET and SET fast path ignores stale async durability mode" do
    ctx = FerricStore.Instance.get(:default)
    stale_ctx = %{ctx | durability_mode: :all_async}
    set_key = "#{@ns}:mixed_stale_async:#{System.unique_integer([:positive])}"

    state = connection_state(stale_ctx)
    send_response_fn = capture_response_fn()
    handle_command_fn = flunking_handle_fn("mixed GET/SET pipeline fast path")

    commands = [
      get_ast("#{@ns}:missing:#{System.unique_integer([:positive])}"),
      {:command, "SET", [set_key, "v1"], {:set, set_key, "v1"}, [set_key]}
    ]

    assert {:continue, ^state} =
             Pipeline.pipeline_dispatch(commands, state, handle_command_fn, send_response_fn)

    assert_receive {:pipeline_response, response}
    assert response == "_\r\n+OK\r\n"
    assert Router.get(ctx, set_key) == "v1"
  end

  test "SET pipeline fast path writes through sandbox namespace" do
    ctx = FerricStore.Instance.get(:default)
    sandbox = "sandbox_pipe:" <> Integer.to_string(System.unique_integer([:positive])) <> ":"
    key1 = "set1"
    key2 = "set2"

    state = connection_state(ctx) |> Map.put(:sandbox_namespace, sandbox)
    send_response_fn = capture_response_fn()
    handle_command_fn = flunking_handle_fn("sandbox SET pipeline fast path")

    commands = [
      set_ast(key1, "v1"),
      set_ast(key2, "v2")
    ]

    assert {:continue, ^state} =
             Pipeline.pipeline_dispatch(commands, state, handle_command_fn, send_response_fn)

    assert_receive {:pipeline_response, "+OK\r\n+OK\r\n"}
    assert Router.get(ctx, key1) == nil
    assert Router.get(ctx, key2) == nil
    assert Router.get(ctx, sandbox <> key1) == "v1"
    assert Router.get(ctx, sandbox <> key2) == "v2"
  end

  test "mixed GET and SET fast path writes SETs through sandbox namespace" do
    ctx = FerricStore.Instance.get(:default)
    sandbox = "sandbox_mixed:" <> Integer.to_string(System.unique_integer([:positive])) <> ":"
    get_key = "existing"
    set_key = "new"

    :ok = Router.put(ctx, sandbox <> get_key, "inside", 0)

    state = connection_state(ctx) |> Map.put(:sandbox_namespace, sandbox)
    send_response_fn = capture_response_fn()
    handle_command_fn = flunking_handle_fn("sandbox mixed GET/SET pipeline fast path")

    commands = [
      get_ast(get_key),
      set_ast(set_key, "written")
    ]

    assert {:continue, ^state} =
             Pipeline.pipeline_dispatch(commands, state, handle_command_fn, send_response_fn)

    assert_receive {:pipeline_response, "$6\r\ninside\r\n+OK\r\n"}
    assert Router.get(ctx, set_key) == nil
    assert Router.get(ctx, sandbox <> set_key) == "written"
  end

  test "general pure pipeline converts command raises into Redis error replies" do
    ctx = FerricStore.Instance.get(:default)
    raw_store_key = {:ferricstore_raw_store, ctx.name}
    old_raw_store = :persistent_term.get(raw_store_key, :missing)

    raising_store =
      ctx
      |> FerricstoreServer.Connection.Store.build_raw_store()
      |> Map.put(:incr, fn _key, _delta -> raise "async key latch timeout after 5ms" end)

    :persistent_term.put(raw_store_key, raising_store)

    on_exit(fn ->
      case old_raw_store do
        :missing -> :persistent_term.erase(raw_store_key)
        store -> :persistent_term.put(raw_store_key, store)
      end
    end)

    state = connection_state(ctx)
    send_response_fn = capture_response_fn()
    handle_command_fn = flunking_handle_fn("general pure pipeline error containment")

    commands = [
      {:command, "INCR", ["raise"], {:incr, "raise"}, ["raise"]},
      {:command, "PING", [], :ping, []}
    ]

    assert {:continue, ^state} =
             Pipeline.pipeline_dispatch(commands, state, handle_command_fn, send_response_fn)

    assert_receive {:pipeline_response, response}
    assert response =~ "-ERR internal error:"
    assert response =~ "+PONG\r\n"
  end

  defp connection_state(ctx) do
    %Connection{
      socket: :socket,
      transport: :test_transport,
      instance_ctx: ctx,
      stats_counter: ctx.stats_counter,
      authenticated: true,
      require_auth: false,
      acl_cache: :full_access
    }
  end

  defp capture_response_fn do
    fn :socket, :test_transport, response ->
      send(self(), {:pipeline_response, IO.iodata_to_binary(response)})
      :ok
    end
  end

  defp flunking_handle_fn(path) do
    fn command, _state ->
      flunk("expected #{path}, got fallback for #{inspect(command)}")
    end
  end

  defp get_ast(key), do: {:command, "GET", [key], {:get, key}, [key]}
  defp set_ast(key, value), do: {:command, "SET", [key, value], {:set, key, value}, [key]}
end
