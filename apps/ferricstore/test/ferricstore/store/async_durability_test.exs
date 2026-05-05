defmodule Ferricstore.Store.AsyncDurabilityTest do
  @moduledoc """
  Tests that async durability namespace writes go through Raft (pipeline_command)
  and are functionally correct — values are readable after write.
  """
  use ExUnit.Case, async: false
  @moduletag skip: "async durability feature removed; quorum is the only supported durability"

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  @moduletag timeout: 120_000

  setup do
    ShardHelpers.flush_all_keys()
    Ferricstore.NamespaceConfig.set("async_test", "durability", "async")

    on_exit(fn ->
      Ferricstore.NamespaceConfig.set("async_test", "durability", "quorum")
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  describe "async namespace basic operations" do
    test "SET then GET returns value" do
      :ok = Router.put(FerricStore.Instance.get(:default), "async_test:k1", "v1", 0)
      assert Router.get(FerricStore.Instance.get(:default), "async_test:k1") == "v1"
    end

    test "SET with TTL then GET returns value before expiry" do
      exp = System.os_time(:millisecond) + 60_000
      :ok = Router.put(FerricStore.Instance.get(:default), "async_test:k2", "v2", exp)
      assert Router.get(FerricStore.Instance.get(:default), "async_test:k2") == "v2"
    end

    test "DELETE removes key" do
      Router.put(FerricStore.Instance.get(:default), "async_test:k3", "v3", 0)
      assert Router.get(FerricStore.Instance.get(:default), "async_test:k3") == "v3"
      Router.delete(FerricStore.Instance.get(:default), "async_test:k3")
      assert Router.get(FerricStore.Instance.get(:default), "async_test:k3") == nil
    end

    test "INCR works on async namespace" do
      Router.put(FerricStore.Instance.get(:default), "async_test:counter", "10", 0)
      assert {:ok, 11} = Router.incr(FerricStore.Instance.get(:default), "async_test:counter", 1)
      assert Router.get(FerricStore.Instance.get(:default), "async_test:counter") == "11"
    end

    test "multiple keys across shards" do
      # These keys will hash to different shards
      for i <- 1..20 do
        Router.put(FerricStore.Instance.get(:default), "async_test:multi:#{i}", "val#{i}", 0)
      end

      for i <- 1..20 do
        assert Router.get(FerricStore.Instance.get(:default), "async_test:multi:#{i}") == "val#{i}"
      end
    end
  end

  describe "async namespace via embedded API" do
    test "FerricStore.set and get on async namespace" do
      FerricStore.set("async_test:emb1", "embedded_val")
      assert {:ok, "embedded_val"} = FerricStore.get("async_test:emb1")
    end

    test "FerricStore.del on async namespace" do
      FerricStore.set("async_test:emb2", "to_delete")

      Ferricstore.Test.Utils.eventually(fn ->
        FerricStore.del("async_test:emb2")
        assert {:ok, nil} = FerricStore.get("async_test:emb2")
      end)
    end
  end

  describe "async namespace concurrent writes" do
    @tag timeout: 180_000
    test "50 concurrent writers all succeed" do
      tasks =
        for w <- 1..50 do
          Task.async(fn ->
            for i <- 1..100 do
              Router.put(FerricStore.Instance.get(:default), "async_test:conc:#{w}:#{i}", "v", 0)
            end
          end)
        end

      Task.await_many(tasks, 30_000)
      ShardHelpers.flush_all_shards()

      # Verify a sample — use eventually for async Raft lag on slow CI
      Ferricstore.Test.Utils.eventually(fn ->
        for w <- [1, 25, 50], i <- [1, 50, 100] do
          assert Router.get(FerricStore.Instance.get(:default), "async_test:conc:#{w}:#{i}") == "v",
                 "Key async_test:conc:#{w}:#{i} should exist"
        end
      end)
    end
  end

  describe "async namespace read-modify-write operations" do
    test "INCR on nonexistent key starts from delta" do
      assert {:ok, 5} = Router.incr(FerricStore.Instance.get(:default), "async_test:incr_new", 5)
      assert Router.get(FerricStore.Instance.get(:default), "async_test:incr_new") == "5"
    end

    test "INCR multiple times" do
      Router.put(FerricStore.Instance.get(:default), "async_test:incr_multi", "0", 0)
      assert {:ok, 1} = Router.incr(FerricStore.Instance.get(:default), "async_test:incr_multi", 1)
      assert {:ok, 3} = Router.incr(FerricStore.Instance.get(:default), "async_test:incr_multi", 2)
      assert {:ok, 13} = Router.incr(FerricStore.Instance.get(:default), "async_test:incr_multi", 10)
      assert Router.get(FerricStore.Instance.get(:default), "async_test:incr_multi") == "13"
    end

    test "INCR_FLOAT works" do
      Router.put(FerricStore.Instance.get(:default), "async_test:float", "10.5", 0)
      assert {:ok, result} = Router.incr_float(FerricStore.Instance.get(:default), "async_test:float", 2.5)
      assert result == 13.0
    end

    test "INCR_FLOAT on nonexistent key" do
      assert {:ok, result} = Router.incr_float(FerricStore.Instance.get(:default), "async_test:float_new", 3.14)
      assert result == 3.14
    end

    test "INCR on non-integer returns error" do
      Router.put(FerricStore.Instance.get(:default), "async_test:not_int", "hello", 0)
      assert {:error, _} = Router.incr(FerricStore.Instance.get(:default), "async_test:not_int", 1)
      # Original value unchanged
      assert Router.get(FerricStore.Instance.get(:default), "async_test:not_int") == "hello"
    end

    test "APPEND works" do
      Router.put(FerricStore.Instance.get(:default), "async_test:app", "hello", 0)
      assert {:ok, 11} = Router.append(FerricStore.Instance.get(:default), "async_test:app", " world")
      assert Router.get(FerricStore.Instance.get(:default), "async_test:app") == "hello world"
    end

    test "APPEND on nonexistent key creates it" do
      assert {:ok, 3} = Router.append(FerricStore.Instance.get(:default), "async_test:app_new", "foo")
      assert Router.get(FerricStore.Instance.get(:default), "async_test:app_new") == "foo"
    end

    test "GETSET returns old value and sets new" do
      Router.put(FerricStore.Instance.get(:default), "async_test:gs", "old", 0)
      assert Router.getset(FerricStore.Instance.get(:default), "async_test:gs", "new") == "old"
      assert Router.get(FerricStore.Instance.get(:default), "async_test:gs") == "new"
    end

    test "GETSET on nonexistent returns nil" do
      assert Router.getset(FerricStore.Instance.get(:default), "async_test:gs_new", "val") == nil
      assert Router.get(FerricStore.Instance.get(:default), "async_test:gs_new") == "val"
    end

    test "GETDEL returns value and deletes" do
      Router.put(FerricStore.Instance.get(:default), "async_test:gd", "val", 0)
      assert Router.getdel(FerricStore.Instance.get(:default), "async_test:gd") == "val"
      assert Router.get(FerricStore.Instance.get(:default), "async_test:gd") == nil
    end

    test "GETDEL on nonexistent returns nil" do
      assert Router.getdel(FerricStore.Instance.get(:default), "async_test:gd_missing") == nil
    end

    test "GETEX returns value and updates expiry" do
      Router.put(FerricStore.Instance.get(:default), "async_test:gx", "val", 0)
      exp = System.os_time(:millisecond) + 60_000
      assert Router.getex(FerricStore.Instance.get(:default), "async_test:gx", exp) == "val"
      assert Router.get(FerricStore.Instance.get(:default), "async_test:gx") == "val"
    end

    test "SETRANGE overwrites part of string" do
      Router.put(FerricStore.Instance.get(:default), "async_test:sr", "Hello World", 0)
      assert {:ok, 11} = Router.setrange(FerricStore.Instance.get(:default), "async_test:sr", 6, "Redis")
      assert Router.get(FerricStore.Instance.get(:default), "async_test:sr") == "Hello Redis"
    end

    test "SETRANGE on nonexistent key zero-pads" do
      assert {:ok, 8} = Router.setrange(FerricStore.Instance.get(:default), "async_test:sr_new", 5, "abc")
      result = Router.get(FerricStore.Instance.get(:default), "async_test:sr_new")
      assert byte_size(result) == 8
      assert binary_part(result, 5, 3) == "abc"
    end
  end

  describe "quorum and async coexist" do
    test "quorum namespace key is readable" do
      Router.put(FerricStore.Instance.get(:default), "quorum_ns:k1", "quorum_val", 0)
      assert Router.get(FerricStore.Instance.get(:default), "quorum_ns:k1") == "quorum_val"
    end

    test "async and quorum keys don't interfere" do
      Router.put(FerricStore.Instance.get(:default), "async_test:coexist", "async_val", 0)
      Router.put(FerricStore.Instance.get(:default), "quorum_ns:coexist", "quorum_val", 0)

      assert Router.get(FerricStore.Instance.get(:default), "async_test:coexist") == "async_val"
      assert Router.get(FerricStore.Instance.get(:default), "quorum_ns:coexist") == "quorum_val"
    end
  end
end
