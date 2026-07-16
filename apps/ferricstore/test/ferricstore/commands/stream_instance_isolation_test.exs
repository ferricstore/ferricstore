defmodule Ferricstore.Commands.StreamInstanceIsolationTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Stream
  alias Ferricstore.Commands.Stream.CacheKey
  alias Ferricstore.Commands.Strings.Delete
  alias Ferricstore.Test.MockStore

  setup do
    Stream.clear_local_state()
    on_exit(&Stream.clear_local_state/0)

    stores = %{
      a: Map.put(MockStore.make(), :cache_scope, :stream_instance_a),
      b: Map.put(MockStore.make(), :cache_scope, :stream_instance_b)
    }

    {:ok, stores}
  end

  test "metadata, range indexes, and groups are isolated by instance", %{a: store_a, b: store_b} do
    key = "shared-stream"

    assert "2-0" = Stream.handle("XADD", [key, "2-0", "source", "a"], store_a)
    assert "1-0" = Stream.handle("XADD", [key, "1-0", "source", "b"], store_b)

    assert 1 = Stream.handle("XLEN", [key], store_a)
    assert 1 = Stream.handle("XLEN", [key], store_b)

    assert [["2-0", "source", "a"]] =
             Stream.handle("XRANGE", [key, "-", "+", "COUNT", "1"], store_a)

    assert [["1-0", "source", "b"]] =
             Stream.handle("XRANGE", [key, "-", "+", "COUNT", "1"], store_b)

    assert :ok = Stream.handle("XGROUP", ["CREATE", key, "workers", "0"], store_a)
    assert :ok = Stream.handle("XGROUP", ["CREATE", key, "workers", "0"], store_b)
  end

  test "waiter notification and local flush cleanup are isolated by instance", %{
    a: store_a,
    b: store_b
  } do
    key = "shared-blocking-stream"

    assert :ok = Stream.register_stream_waiter(key, self(), "0-0", store_a)
    assert :ok = Stream.notify_stream_waiters(key, store_b)
    refute_receive {:stream_waiter_notify, ^key}
    assert 1 = Stream.stream_waiter_count(key, store_a)

    assert :ok = Stream.register_stream_waiter(key, self(), "0-0", store_b)
    assert :ok = Stream.clear_local_state(store_a)
    assert 0 = Stream.stream_waiter_count(key, store_a)
    assert 1 = Stream.stream_waiter_count(key, store_b)

    assert :ok = Stream.notify_stream_waiters(key, store_b)
    assert_receive {:stream_waiter_notify, ^key}
  end

  test "production instance contexts produce distinct cache keys" do
    assert {:instance_a, "shared"} =
             CacheKey.build(%FerricStore.Instance{name: :instance_a}, "shared")

    assert {:instance_b, "shared"} =
             CacheKey.build(%FerricStore.Instance{name: :instance_b}, "shared")
  end

  test "deferred stream cleanup retains the exact instance scope", %{a: store_a} do
    parent = self()

    store =
      Map.put(store_a, :defer_stream_cleanup, fn cache_key ->
        send(parent, {:deferred_stream_cleanup, cache_key})
        :ok
      end)

    assert :ok = Delete.cleanup_stream_metadata("shared", store)
    assert_receive {:deferred_stream_cleanup, {:stream_instance_a, "shared"}}
  end

  test "scoped cleanup does not materialize every local stream cache row" do
    source = File.read!(Path.expand("../../../lib/ferricstore/stream/local_state.ex", __DIR__))
    refute source =~ ":ets.tab2list"
  end
end
