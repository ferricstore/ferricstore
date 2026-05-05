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
    assert is_function(store.value_size, 1)
    assert is_function(store.object_lfu, 1)
    assert is_function(store.compound_batch_get, 2)
    assert is_function(store.compound_batch_get_meta, 2)
  end

  test "raw connection store rebuilds stale cached maps missing metadata helpers" do
    ctx = FerricStore.Instance.get(:default)
    cache_key = {:ferricstore_raw_store, ctx.name}
    previous = :persistent_term.get(cache_key, :missing)

    stale =
      ctx
      |> Store.build_store(nil)
      |> Map.drop([:value_size, :object_lfu])

    try do
      :persistent_term.put(cache_key, stale)

      store = Store.build_store(ctx, nil)

      assert is_function(store.value_size, 1)
      assert is_function(store.object_lfu, 1)
      refute store == stale
    after
      case previous do
        :missing -> :persistent_term.erase(cache_key)
        value -> :persistent_term.put(cache_key, value)
      end
    end
  end

  test "sandbox connection store batches reads with namespace applied once" do
    ctx = FerricStore.Instance.get(:default)
    ns = "sandbox_batch_#{System.unique_integer([:positive])}:"
    store = Store.build_store(ctx, ns)

    assert :ok = Router.put(ctx, ns <> "a", "sandbox-a", 0)
    assert :ok = Router.put(ctx, "a", "outside-a", 0)

    assert store.batch_get.(["a", "missing"]) == ["sandbox-a", nil]
  end

  test "sandbox connection store namespaces metadata helpers without reading values" do
    ctx = FerricStore.Instance.get(:default)
    ns = "sandbox_meta_#{System.unique_integer([:positive])}:"

    store =
      with_fake_raw_store(ctx, fn _calls ->
        Store.build_store(ctx, ns)
      end)

    assert 123 = store.value_size.("large")
    assert_receive {:raw_call, :value_size, [^ns <> "large"]}

    assert 7 = store.object_lfu.("large")
    assert_receive {:raw_call, :object_lfu, [^ns <> "large"]}

    refute_receive {:raw_call, :get, _}
  end

  test "sandbox connection store namespaces key-first mutation helpers" do
    ctx = FerricStore.Instance.get(:default)
    ns = "sandbox_ops_#{System.unique_integer([:positive])}:"

    store =
      with_fake_raw_store(ctx, fn calls ->
        Store.build_store(ctx, ns)
        |> Map.merge(%{_calls: calls})
      end)

    assert {:incr, 1} = store.incr.("counter", 1)
    assert_receive {:raw_call, :incr, [^ns <> "counter", 1]}

    assert {:append, 5} = store.append.("str", "hello")
    assert_receive {:raw_call, :append, [^ns <> "str", "hello"]}

    assert :ok = store.list_op.("list", {:lpush, ["a"]})
    assert_receive {:raw_call, :list_op, [^ns <> "list", {:lpush, ["a"]}]}

    assert ["allowed", 1, 2, 3] = store.ratelimit_add.("rl", 1000, 10, 1)
    assert_receive {:raw_call, :ratelimit_add, [^ns <> "rl", 1000, 10, 1]}
  end

  test "sandbox connection store namespaces compound redis keys and internal keys" do
    ctx = FerricStore.Instance.get(:default)
    ns = "sandbox_compound_#{System.unique_integer([:positive])}:"

    store =
      with_fake_raw_store(ctx, fn _calls ->
        Store.build_store(ctx, ns)
      end)

    raw_field = <<"H:hash", 0, "field">>
    namespaced_field = <<"H:", ns::binary, "hash", 0, "field">>
    raw_prefix = <<"H:hash", 0>>
    namespaced_prefix = <<"H:", ns::binary, "hash", 0>>

    assert :ok = store.compound_put.("hash", raw_field, "v", 0)
    assert_receive {:raw_call, :compound_put, [^ns <> "hash", ^namespaced_field, "v", 0]}

    assert "v" = store.compound_get.("hash", raw_field)
    assert_receive {:raw_call, :compound_get, [^ns <> "hash", ^namespaced_field]}

    assert [{"field", "v"}] = store.compound_scan.("hash", raw_prefix)
    assert_receive {:raw_call, :compound_scan, [^ns <> "hash", ^namespaced_prefix]}

    assert 1 = store.compound_count.("hash", raw_prefix)
    assert_receive {:raw_call, :compound_count, [^ns <> "hash", ^namespaced_prefix]}
  end

  test "sandbox connection store namespaces probabilistic write command keys" do
    ctx = FerricStore.Instance.get(:default)
    ns = "sandbox_prob_#{System.unique_integer([:positive])}:"

    store =
      with_fake_raw_store(ctx, fn _calls ->
        Store.build_store(ctx, ns)
      end)

    assert {:ok, namespaced_cmd} =
             store.prob_write.({:cms_merge, "dst", ["src1", "src2"], [1, 2], {:w, 10}})

    assert namespaced_cmd ==
             {:cms_merge, ns <> "dst", [ns <> "src1", ns <> "src2"], [1, 2], {:w, 10}}

    assert_receive {:raw_call, :prob_write, [^namespaced_cmd]}
  end

  defp with_fake_raw_store(ctx, fun) do
    key = {:ferricstore_raw_store, ctx.name}
    previous = :persistent_term.get(key, :missing)
    calls = self()
    :persistent_term.put(key, fake_raw_store(calls))

    try do
      fun.(calls)
    after
      case previous do
        :missing -> :persistent_term.erase(key)
        value -> :persistent_term.put(key, value)
      end
    end
  end

  defp fake_raw_store(calls) do
    %{
      get: fn key ->
        send(calls, {:raw_call, :get, [key]})
        nil
      end,
      get_meta: fn key ->
        send(calls, {:raw_call, :get_meta, [key]})
        nil
      end,
      batch_get: fn keys ->
        send(calls, {:raw_call, :batch_get, [keys]})
        []
      end,
      value_size: fn key ->
        send(calls, {:raw_call, :value_size, [key]})
        123
      end,
      object_lfu: fn key ->
        send(calls, {:raw_call, :object_lfu, [key]})
        7
      end,
      put: fn key, value, exp ->
        send(calls, {:raw_call, :put, [key, value, exp]})
        :ok
      end,
      delete: fn key ->
        send(calls, {:raw_call, :delete, [key]})
        :ok
      end,
      exists?: fn key ->
        send(calls, {:raw_call, :exists?, [key]})
        false
      end,
      keys: fn -> [] end,
      flush: fn -> :ok end,
      dbsize: fn -> 0 end,
      incr: fn key, delta ->
        send(calls, {:raw_call, :incr, [key, delta]})
        {:incr, delta}
      end,
      incr_float: fn key, delta ->
        send(calls, {:raw_call, :incr_float, [key, delta]})
        {:incr_float, delta}
      end,
      append: fn key, suffix ->
        send(calls, {:raw_call, :append, [key, suffix]})
        {:append, byte_size(suffix)}
      end,
      getset: fn key, value ->
        send(calls, {:raw_call, :getset, [key, value]})
        nil
      end,
      getdel: fn key ->
        send(calls, {:raw_call, :getdel, [key]})
        nil
      end,
      getex: fn key, exp ->
        send(calls, {:raw_call, :getex, [key, exp]})
        nil
      end,
      setrange: fn key, offset, value ->
        send(calls, {:raw_call, :setrange, [key, offset, value]})
        {:ok, byte_size(value)}
      end,
      cas: fn key, exp, new_val, ttl ->
        send(calls, {:raw_call, :cas, [key, exp, new_val, ttl]})
        :ok
      end,
      lock: fn key, owner, ttl ->
        send(calls, {:raw_call, :lock, [key, owner, ttl]})
        :ok
      end,
      unlock: fn key, owner ->
        send(calls, {:raw_call, :unlock, [key, owner]})
        1
      end,
      extend: fn key, owner, ttl ->
        send(calls, {:raw_call, :extend, [key, owner, ttl]})
        1
      end,
      ratelimit_add: fn key, window, max, count ->
        send(calls, {:raw_call, :ratelimit_add, [key, window, max, count]})
        ["allowed", 1, 2, 3]
      end,
      list_op: fn key, op ->
        send(calls, {:raw_call, :list_op, [key, op]})
        :ok
      end,
      compound_get: fn redis_key, compound_key ->
        send(calls, {:raw_call, :compound_get, [redis_key, compound_key]})
        "v"
      end,
      compound_get_meta: fn redis_key, compound_key ->
        send(calls, {:raw_call, :compound_get_meta, [redis_key, compound_key]})
        {"v", 0}
      end,
      compound_batch_get: fn redis_key, compound_keys ->
        send(calls, {:raw_call, :compound_batch_get, [redis_key, compound_keys]})
        []
      end,
      compound_batch_get_meta: fn redis_key, compound_keys ->
        send(calls, {:raw_call, :compound_batch_get_meta, [redis_key, compound_keys]})
        []
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        send(calls, {:raw_call, :compound_put, [redis_key, compound_key, value, expire_at_ms]})
        :ok
      end,
      compound_delete: fn redis_key, compound_key ->
        send(calls, {:raw_call, :compound_delete, [redis_key, compound_key]})
        :ok
      end,
      compound_scan: fn redis_key, prefix ->
        send(calls, {:raw_call, :compound_scan, [redis_key, prefix]})
        [{"field", "v"}]
      end,
      compound_count: fn redis_key, prefix ->
        send(calls, {:raw_call, :compound_count, [redis_key, prefix]})
        1
      end,
      compound_delete_prefix: fn redis_key, prefix ->
        send(calls, {:raw_call, :compound_delete_prefix, [redis_key, prefix]})
        :ok
      end,
      prob_write: fn command ->
        send(calls, {:raw_call, :prob_write, [command]})
        {:ok, command}
      end,
      prob_dir_for_key: fn key ->
        send(calls, {:raw_call, :prob_dir_for_key, [key]})
        "/prob"
      end,
      flush_prob_dirs: fn -> :ok end,
      on_push: fn msg ->
        send(calls, {:raw_call, :on_push, [msg]})
        :ok
      end
    }
  end
end
