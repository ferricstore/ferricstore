defmodule FerricstoreServer.Commands.BlockingTrackingTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Commands.Stream
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers
  alias FerricstoreServer.ClientTracking
  alias FerricstoreServer.Connection
  alias FerricstoreServer.Connection.Blocking
  alias FerricstoreServer.Connection.Store, as: ConnStore
  alias FerricstoreServer.Resp.Parser

  setup do
    ClientTracking.init_tables()
    :ets.delete_all_objects(:ferricstore_tracking)
    :ets.delete_all_objects(:ferricstore_tracking_connections)
    ShardHelpers.flush_all_keys()
    NamespaceConfig.reset_all()

    on_exit(fn ->
      NamespaceConfig.reset_all()
      ShardHelpers.flush_all_keys()
      ClientTracking.cleanup(self())
    end)

    :ok
  end

  test "immediate BLPOP sends client tracking invalidation for popped key" do
    ctx = FerricStore.Instance.get(:default)
    key = "blocking_tracking:" <> Integer.to_string(System.unique_integer([:positive]))

    assert 1 = Router.list_op(ctx, key, {:rpush, ["v1"]})

    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])
    ClientTracking.track_key(self(), key, tracking)

    state = %Connection{
      instance_ctx: ctx,
      tracking: tracking,
      authenticated: true,
      require_auth: false
    }

    assert {:continue, encoded, ^state} = Blocking.dispatch_blpop_ast([key], 1, state)
    assert {:ok, [[^key, "v1"]], ""} = Parser.parse(IO.iodata_to_binary(encoded))
    assert [] = Router.list_op(ctx, key, {:lrange, 0, -1})

    assert_receive {:tracking_invalidation, _payload, [^key]}
    assert :ets.lookup(:ferricstore_tracking, key) == []
  end

  test "immediate BLMOVE invalidates tracked source and destination keys" do
    ctx = FerricStore.Instance.get(:default)
    suffix = Integer.to_string(System.unique_integer([:positive]))
    source = "blocking_tracking:src:" <> suffix
    destination = "blocking_tracking:dst:" <> suffix

    assert 1 = Router.list_op(ctx, source, {:rpush, ["v1"]})

    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])
    ClientTracking.track_key(self(), source, tracking)
    ClientTracking.track_key(self(), destination, tracking)

    state = %Connection{
      instance_ctx: ctx,
      tracking: tracking,
      authenticated: true,
      require_auth: false
    }

    assert {:continue, encoded, ^state} =
             Blocking.dispatch_blmove_ast(source, destination, :left, :right, 1, state)

    assert {:ok, ["v1"], ""} = Parser.parse(IO.iodata_to_binary(encoded))
    assert [] = Router.list_op(ctx, source, {:lrange, 0, -1})
    assert ["v1"] = Router.list_op(ctx, destination, {:lrange, 0, -1})

    assert_receive {:tracking_invalidation, _payload, invalidated}
    assert MapSet.new(invalidated) == MapSet.new([source, destination])
    assert :ets.lookup(:ferricstore_tracking, source) == []
    assert :ets.lookup(:ferricstore_tracking, destination) == []
  end

  test "blocked XREAD tracks stream key after wake" do
    ctx = FerricStore.Instance.get(:default)
    stream = "blocking_tracking:xread:" <> Integer.to_string(System.unique_integer([:positive]))

    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])

    state = %Connection{
      instance_ctx: ctx,
      tracking: tracking,
      authenticated: true,
      require_auth: false
    }

    store = ConnStore.build_store(ctx, nil)
    parent = self()

    Task.start_link(fn ->
      ShardHelpers.eventually(
        fn -> Stream.stream_waiter_count(stream) == 1 end,
        "XREAD waiter should be registered before XADD wakes it",
        100,
        10
      )

      assert is_binary(Stream.handle("XADD", [stream, "*", "f", "v"], store))
      send(parent, :xadd_done)
    end)

    ast = {:xread, :infinity, {:block, 2_000}, [{stream, "0-0"}]}
    args = ["BLOCK", "2000", "STREAMS", stream, "0-0"]

    assert {:continue, encoded, new_state} = Blocking.dispatch_xread_ast(ast, args, state)

    assert {:ok, [[[^stream, [[_id, "f", "v"]]]]], ""} =
             Parser.parse(IO.iodata_to_binary(encoded))

    assert new_state.tracking == tracking

    assert_receive :xadd_done
    assert :ets.lookup(:ferricstore_tracking, stream) == [{stream, self()}]
    assert :ets.lookup(:ferricstore_tracking, "BLOCK") == []
    assert :ets.lookup(:ferricstore_tracking, "STREAMS") == []
  end

  test "timed out XREAD BLOCK tracks stream key for nil cache entry" do
    ctx = FerricStore.Instance.get(:default)

    stream =
      "blocking_tracking:xread_timeout:" <> Integer.to_string(System.unique_integer([:positive]))

    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])

    state = %Connection{
      instance_ctx: ctx,
      tracking: tracking,
      authenticated: true,
      require_auth: false
    }

    ast = {:xread, :infinity, {:block, 1}, [{stream, "0-0"}]}
    args = ["BLOCK", "1", "STREAMS", stream, "0-0"]

    assert {:continue, encoded, new_state} = Blocking.dispatch_xread_ast(ast, args, state)
    assert {:ok, [nil], ""} = Parser.parse(IO.iodata_to_binary(encoded))
    assert new_state.tracking == tracking

    assert :ets.lookup(:ferricstore_tracking, stream) == [{stream, self()}]
    assert :ets.lookup(:ferricstore_tracking, "BLOCK") == []
    assert :ets.lookup(:ferricstore_tracking, "STREAMS") == []
  end
end
