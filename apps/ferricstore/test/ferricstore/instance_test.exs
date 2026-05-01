defmodule Ferricstore.InstanceTest do
  @moduledoc "Tests that use FerricStore pattern works end-to-end."
  use ExUnit.Case, async: false

  # Use the :default instance (created at app boot)
  # In future: test with a custom isolated instance

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
  end

  describe "FerricStore.Impl with default instance" do
    test "set and get" do
      ctx = FerricStore.Instance.get(:default)
      assert :ok = FerricStore.Impl.set(ctx, "impl_key", "impl_value")
      assert {:ok, "impl_value"} = FerricStore.Impl.get(ctx, "impl_key")
    end

    test "del" do
      ctx = FerricStore.Instance.get(:default)
      FerricStore.Impl.set(ctx, "impl_del", "val")
      assert {:ok, 1} = FerricStore.Impl.del(ctx, ["impl_del"])
      assert {:ok, nil} = FerricStore.Impl.get(ctx, "impl_del")
    end

    test "incr" do
      ctx = FerricStore.Instance.get(:default)
      assert {:ok, 1} = FerricStore.Impl.incr(ctx, "impl_counter", 1)
      assert {:ok, 6} = FerricStore.Impl.incr(ctx, "impl_counter", 5)
    end

    test "hash operations" do
      ctx = FerricStore.Instance.get(:default)
      assert {:ok, 2} = FerricStore.Impl.hset(ctx, "impl_hash", %{"f1" => "v1", "f2" => "v2"})
      assert {:ok, "v1"} = FerricStore.Impl.hget(ctx, "impl_hash", "f1")
      assert {:ok, map} = FerricStore.Impl.hgetall(ctx, "impl_hash")
      assert map == %{"f1" => "v1", "f2" => "v2"}
    end

    test "hash read operations return WRONGTYPE directly" do
      ctx = FerricStore.Instance.get(:default)
      assert :ok = FerricStore.Impl.set(ctx, "impl_hash:string", "plain")

      assert {:error, "WRONGTYPE" <> _} = FerricStore.Impl.hget(ctx, "impl_hash:string", "f")
      assert {:error, "WRONGTYPE" <> _} = FerricStore.Impl.hexists(ctx, "impl_hash:string", "f")
    end

    test "set operations" do
      ctx = FerricStore.Instance.get(:default)
      assert {:ok, 3} = FerricStore.Impl.sadd(ctx, "impl_set", ["a", "b", "c"])
      assert {:ok, true} = FerricStore.Impl.sismember(ctx, "impl_set", "a")
      assert {:ok, false} = FerricStore.Impl.sismember(ctx, "impl_set", "z")
      assert {:ok, 3} = FerricStore.Impl.scard(ctx, "impl_set")
    end

    test "set read operations return WRONGTYPE directly" do
      ctx = FerricStore.Instance.get(:default)
      assert :ok = FerricStore.Impl.set(ctx, "impl_set:string", "plain")

      assert {:error, "WRONGTYPE" <> _} = FerricStore.Impl.sismember(ctx, "impl_set:string", "a")
    end

    test "list operations" do
      ctx = FerricStore.Instance.get(:default)
      assert {:ok, 3} = FerricStore.Impl.lpush(ctx, "impl_list", ["a", "b", "c"])
      assert {:ok, 3} = FerricStore.Impl.llen(ctx, "impl_list")
    end

    test "sorted set read operations return WRONGTYPE directly" do
      ctx = FerricStore.Instance.get(:default)
      assert :ok = FerricStore.Impl.set(ctx, "impl_zset:string", "plain")

      assert {:error, "WRONGTYPE" <> _} = FerricStore.Impl.zscore(ctx, "impl_zset:string", "a")
    end

    test "bloom filter" do
      ctx = FerricStore.Instance.get(:default)
      assert :ok = FerricStore.Impl.bf_reserve(ctx, "impl_bf", 0.01, 100)
      assert {:ok, 1} = FerricStore.Impl.bf_add(ctx, "impl_bf", "hello")
      assert {:ok, 1} = FerricStore.Impl.bf_exists(ctx, "impl_bf", "hello")
      assert {:ok, 0} = FerricStore.Impl.bf_exists(ctx, "impl_bf", "missing")
    end

    test "CMS" do
      ctx = FerricStore.Instance.get(:default)
      assert :ok = FerricStore.Impl.cms_initbydim(ctx, "impl_cms", 100, 7)
      assert {:ok, [5]} = FerricStore.Impl.cms_incrby(ctx, "impl_cms", [{"apple", 5}])
      assert {:ok, [5]} = FerricStore.Impl.cms_query(ctx, "impl_cms", ["apple"])
    end

    test "cuckoo filter" do
      ctx = FerricStore.Instance.get(:default)
      assert :ok = FerricStore.Impl.cf_reserve(ctx, "impl_cf", 1024)
      assert {:ok, 1} = FerricStore.Impl.cf_add(ctx, "impl_cf", "elem")
      assert {:ok, 1} = FerricStore.Impl.cf_exists(ctx, "impl_cf", "elem")
    end

    test "topk" do
      ctx = FerricStore.Instance.get(:default)
      assert :ok = FerricStore.Impl.topk_reserve(ctx, "impl_topk", 3)
      FerricStore.Impl.topk_add(ctx, "impl_topk", ["a", "b", "c"])
      assert {:ok, items} = FerricStore.Impl.topk_list(ctx, "impl_topk")
      assert is_list(items)
    end

    test "tdigest" do
      ctx = FerricStore.Instance.get(:default)
      assert :ok = FerricStore.Impl.tdigest_create(ctx, "impl_td")
      assert :ok = FerricStore.Impl.tdigest_add(ctx, "impl_td", [1, 2, 3, 4, 5])
    end

    test "keys and dbsize" do
      ctx = FerricStore.Instance.get(:default)
      FerricStore.Impl.set(ctx, "impl_k1", "v1")
      FerricStore.Impl.set(ctx, "impl_k2", "v2")
      {:ok, keys} = FerricStore.Impl.keys(ctx)
      assert "impl_k1" in keys
      assert "impl_k2" in keys
    end

    test "flushdb" do
      ctx = FerricStore.Instance.get(:default)
      FerricStore.Impl.set(ctx, "impl_flush", "val")
      assert {:ok, "val"} = FerricStore.Impl.get(ctx, "impl_flush")
      :ok = FerricStore.Impl.flushdb(ctx)
      assert {:ok, nil} = FerricStore.Impl.get(ctx, "impl_flush")
    end
  end
end
