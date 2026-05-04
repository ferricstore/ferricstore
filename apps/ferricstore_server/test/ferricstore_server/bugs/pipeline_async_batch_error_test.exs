defmodule FerricstoreServer.Bugs.PipelineAsyncBatchErrorTest do
  use ExUnit.Case, async: false

  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers
  alias FerricstoreServer.Connection
  alias FerricstoreServer.Connection.Pipeline

  @ns "pipeline_async_batch_error"

  setup do
    ShardHelpers.flush_all_keys()
    NamespaceConfig.reset_all()
    NamespaceConfig.set(@ns, "durability", "async")

    on_exit(fn ->
      NamespaceConfig.reset_all()
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  test "async SET fast path returns router errors instead of unconditional OK" do
    ctx = FerricStore.Instance.get(:default)
    key = "#{@ns}:overloaded:#{System.unique_integer([:positive])}"
    shard_idx = Router.shard_for(ctx, key)

    on_exit(fn -> Batcher.reset_pending(shard_idx) end)
    fill_async_pending(shard_idx, key)

    state = connection_state(ctx)
    send_response_fn = capture_response_fn()
    handle_command_fn = flunking_handle_fn("SET pipeline fast path")

    commands = [
      {:command, "SET", [key, "v1"], {:set, key, "v1"}, [key]},
      set_ast("#{@ns}:same_batch:#{System.unique_integer([:positive])}", "v2")
    ]

    assert {:continue, ^state} =
             Pipeline.pipeline_dispatch(commands, state, handle_command_fn, send_response_fn)

    assert_receive {:pipeline_response, response}
    refute response == "+OK\r\n+OK\r\n"

    assert response ==
             "-ERR async replication overloaded\r\n-ERR async replication overloaded\r\n"
  end

  test "mixed GET and async SET fast path returns async router errors in command order" do
    ctx = FerricStore.Instance.get(:default)
    set_key = "#{@ns}:mixed_overloaded:#{System.unique_integer([:positive])}"
    shard_idx = Router.shard_for(ctx, set_key)

    on_exit(fn -> Batcher.reset_pending(shard_idx) end)
    fill_async_pending(shard_idx, set_key)

    state = connection_state(ctx)
    send_response_fn = capture_response_fn()
    handle_command_fn = flunking_handle_fn("mixed GET/SET pipeline fast path")

    commands = [
      get_ast("#{@ns}:missing:#{System.unique_integer([:positive])}"),
      {:command, "SET", [set_key, "v1"], {:set, set_key, "v1"}, [set_key]}
    ]

    assert {:continue, ^state} =
             Pipeline.pipeline_dispatch(commands, state, handle_command_fn, send_response_fn)

    assert_receive {:pipeline_response, response}
    assert response == "_\r\n-ERR async replication overloaded\r\n"
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

  defp fill_async_pending(idx, key) do
    for _ <- 1..64 do
      Batcher.__inject_async_pending__(
        idx,
        make_ref(),
        [{:async, node(), {:put, key, "pending", 0}}],
        0
      )
    end
  end
end
