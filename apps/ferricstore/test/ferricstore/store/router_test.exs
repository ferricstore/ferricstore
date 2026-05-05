defmodule Ferricstore.Store.RouterTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.{LFU, Router, SlotMap}
  alias Ferricstore.Test.IsolatedInstance

  describe "shard_for/1" do
    test "returns integer in valid range" do
      shard_count = :persistent_term.get(:ferricstore_shard_count, 4)
      assert Router.shard_for(FerricStore.Instance.get(:default), "key") in 0..(shard_count - 1)
    end

    test "same key always maps to same shard" do
      assert Router.shard_for(FerricStore.Instance.get(:default), "hello") ==
               Router.shard_for(FerricStore.Instance.get(:default), "hello")
    end

    test "empty binary key works" do
      shard_count = :persistent_term.get(:ferricstore_shard_count, 4)
      assert Router.shard_for(FerricStore.Instance.get(:default), "") in 0..(shard_count - 1)
    end

    test "large key works" do
      shard_count = :persistent_term.get(:ferricstore_shard_count, 4)
      big_key = String.duplicate("x", 10_000)
      assert Router.shard_for(FerricStore.Instance.get(:default), big_key) in 0..(shard_count - 1)
    end

    test "hash tags co-locate keys on the same shard" do
      assert Router.shard_for(FerricStore.Instance.get(:default), "{user:42}:session") ==
               Router.shard_for(FerricStore.Instance.get(:default), "{user:42}:profile")
    end
  end

  describe "slot_for/1" do
    test "returns integer in 0..1023" do
      assert Router.slot_for(FerricStore.Instance.get(:default), "key") in 0..1023
    end

    test "same key always maps to same slot" do
      assert Router.slot_for(FerricStore.Instance.get(:default), "hello") ==
               Router.slot_for(FerricStore.Instance.get(:default), "hello")
    end

    test "hash tags co-locate keys on the same slot" do
      assert Router.slot_for(FerricStore.Instance.get(:default), "{tag}:a") ==
               Router.slot_for(FerricStore.Instance.get(:default), "{tag}:b")
    end

    test "uses the shared slot map hash implementation" do
      for key <- ["plain", "{user:42}:session", "{}empty", String.duplicate("x", 1024)] do
        assert Router.slot_for(FerricStore.Instance.get(:default), key) ==
                 SlotMap.slot_for_key(key)
      end
    end
  end

  describe "shard_name/1" do
    test "returns unique atoms per index" do
      names = Enum.map(0..3, fn i -> Router.shard_name(FerricStore.Instance.get(:default), i) end)
      assert length(Enum.uniq(names)) == 4
    end

    test "returns atoms" do
      assert is_atom(Router.shard_name(FerricStore.Instance.get(:default), 0))
    end

    test "returns expected format" do
      assert Router.shard_name(FerricStore.Instance.get(:default), 0) ==
               :"Ferricstore.Store.Shard.0"

      assert Router.shard_name(FerricStore.Instance.get(:default), 3) ==
               :"Ferricstore.Store.Shard.3"
    end
  end

  describe "batch quorum forward failure classification" do
    test "remote RPC timeout is an unknown-outcome write timeout" do
      assert [
               {:error, {:timeout, :unknown_outcome}},
               {:error, {:timeout, :unknown_outcome}}
             ] == Router.__forward_batch_failure_results__({:erpc, :timeout}, 2)
    end

    test "timeout variants are treated as unknown outcome" do
      assert [{:error, {:timeout, :unknown_outcome}}] ==
               Router.__forward_batch_failure_results__(:timeout, 1)

      assert [{:error, {:timeout, :unknown_outcome}}] ==
               Router.__forward_batch_failure_results__({:timeout, :call}, 1)
    end

    test "non-timeout forward failures stay leader unavailable" do
      assert [
               {:error, "ERR leader unavailable"},
               {:error, "ERR leader unavailable"},
               {:error, "ERR leader unavailable"}
             ] == Router.__forward_batch_failure_results__({:erpc, :noconnection}, 3)
    end
  end

  describe "async list timeout classification" do
    test "list worker timeout is reported as unknown outcome" do
      source = File.read!("lib/ferricstore/store/router.ex")

      refute source =~ ~s({:error, "ERR list_op timeout"}),
             "list mutations may still complete after the worker call times out; classify as unknown outcome"

      assert source =~ "ErrorReasons.write_timeout_unknown()"
    end
  end

  describe "sendfile cold refs" do
    setup do
      ctx = IsolatedInstance.checkout(shard_count: 1)
      on_exit(fn -> IsolatedInstance.checkin(ctx) end)
      {:ok, ctx: ctx}
    end

    test "stale cold offset for another key is rejected before sendfile", %{ctx: ctx} do
      key_a = "sendfile_key_a"
      key_b = "sendfile_key_b"
      value_a = :binary.copy("a", ctx.hot_cache_max_value_size + 1024)
      value_b = :binary.copy("b", ctx.hot_cache_max_value_size + 1024)

      assert :ok = Router.put(ctx, key_a, value_a, 0)
      assert :ok = Router.put(ctx, key_b, value_b, 0)

      keydir = elem(ctx.keydir_refs, 0)
      assert [{^key_b, nil, _exp_b, _lfu_b, fid_b, off_b, vsize_b}] = :ets.lookup(keydir, key_b)

      :ets.insert(keydir, {key_a, nil, 0, LFU.initial(), fid_b, off_b, vsize_b})

      assert Router.get(ctx, key_a) == nil
      assert Router.get_with_file_ref(ctx, key_a) == :miss
    end
  end
end
