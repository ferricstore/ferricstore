defmodule FerricstoreServer.Bugs.TransactionSandboxWatchTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers
  alias FerricstoreServer.Connection.Transaction

  setup do
    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  test "WATCH in a sandbox observes the namespaced key used by EXEC" do
    ctx = FerricStore.Instance.get(:default)
    sandbox = "watch_sandbox:" <> Integer.to_string(System.unique_integer([:positive])) <> ":"
    key = "watched"

    :ok = Router.put(ctx, sandbox <> key, "initial", 0)

    state = %{
      instance_ctx: ctx,
      multi_state: :none,
      multi_queue: [],
      multi_queue_count: 0,
      watched_keys: %{},
      sandbox_namespace: sandbox
    }

    assert {:continue, _ok, watched_state} = Transaction.dispatch_watch([key], state)
    assert Map.has_key?(watched_state.watched_keys, sandbox <> key)
    refute Map.has_key?(watched_state.watched_keys, key)

    :ok = Router.put(ctx, sandbox <> key, "external", 0)

    assert {:continue, _ok, multi_state} = Transaction.dispatch_multi([], watched_state)

    assert {:continue, _queued, queued_state} =
             Transaction.dispatch_queue("SET", [key, "tx"], {:set, key, "tx"}, multi_state)

    assert {:continue, exec_response, _done_state} = Transaction.dispatch_exec([], queued_state)
    assert IO.iodata_to_binary(exec_response) == "_\r\n"
    assert Router.get(ctx, sandbox <> key) == "external"
    assert Router.get(ctx, key) == nil
  end
end
