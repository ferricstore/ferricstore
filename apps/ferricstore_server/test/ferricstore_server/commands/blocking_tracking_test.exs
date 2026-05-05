defmodule FerricstoreServer.Commands.BlockingTrackingTest do
  use ExUnit.Case, async: false

  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers
  alias FerricstoreServer.ClientTracking
  alias FerricstoreServer.Connection
  alias FerricstoreServer.Connection.Blocking
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

    assert_receive {:tracking_invalidation, _payload, [^source]}
    assert_receive {:tracking_invalidation, _payload, [^destination]}
    assert :ets.lookup(:ferricstore_tracking, source) == []
    assert :ets.lookup(:ferricstore_tracking, destination) == []
  end
end
