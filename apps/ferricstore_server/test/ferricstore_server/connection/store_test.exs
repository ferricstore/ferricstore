defmodule FerricstoreServer.Connection.StoreTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers
  alias FerricstoreServer.Connection.Store

  setup do
    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  test "raw connection store exposes batch read helpers for pipeline performance" do
    ctx = FerricStore.Instance.get(:default)
    store = Store.build_store(ctx, nil)

    assert is_function(store.batch_get, 1)
    assert is_function(store.compound_batch_get, 2)
    assert is_function(store.compound_batch_get_meta, 2)
  end

  test "sandbox connection store batches reads with namespace applied once" do
    ctx = FerricStore.Instance.get(:default)
    ns = "sandbox_batch_#{System.unique_integer([:positive])}:"
    store = Store.build_store(ctx, ns)

    assert :ok = Router.put(ctx, ns <> "a", "sandbox-a", 0)
    assert :ok = Router.put(ctx, "a", "outside-a", 0)

    assert store.batch_get.(["a", "missing"]) == ["sandbox-a", nil]
  end
end
