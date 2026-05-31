defmodule FerricstoreServer.Commands.TransactionTrackingTest do
  use ExUnit.Case, async: false

  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers
  alias FerricstoreServer.ClientTracking
  alias FerricstoreServer.Connection
  alias FerricstoreServer.Connection.Transaction
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

  test "EXEC sends client tracking invalidations for successful queued writes" do
    ctx = FerricStore.Instance.get(:default)
    key = "tx_tracking:" <> Integer.to_string(System.unique_integer([:positive]))

    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])
    ClientTracking.track_key(self(), key, tracking)

    state = %Connection{
      instance_ctx: ctx,
      multi_state: :queuing,
      multi_queue: [{"SET", [key, "v2"], {:set, key, "v2"}}],
      multi_queue_count: 1,
      watched_keys: %{},
      tracking: tracking,
      acl_cache: :full_access
    }

    assert {:continue, encoded, new_state} = Transaction.dispatch_exec([], state)
    assert {:ok, [[simple: "OK"]], ""} = Parser.parse(IO.iodata_to_binary(encoded))
    assert new_state.multi_state == :none
    assert Router.get(ctx, key) == "v2"

    assert_receive {:tracking_invalidation, _payload, [^key]}
    assert :ets.lookup(:ferricstore_tracking, key) == []
  end
end
