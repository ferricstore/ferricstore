defmodule Ferricstore.Store.BatchOperationsTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.{CompoundKey, Router}

  @ns_batch "batch_test_batch"
  @ns_quorum "batch_test_quorum"

  setup do
    ctx = Ferricstore.Test.IsolatedInstance.checkout()

    on_exit(fn ->
      Ferricstore.NamespaceConfig.reset(@ns_batch)
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
    end)

    Process.put(:test_ctx, ctx)
    :ok
  end

  defp ctx, do: Process.get(:test_ctx)
  defp default_ctx, do: FerricStore.Instance.get(:default)

  # ---------------------------------------------------------------------------
  # batch_put
  # ---------------------------------------------------------------------------

  describe "batch_put" do
    test "all-small batch: values readable immediately" do
      kvs = for i <- 1..20, do: {"#{@ns_batch}:bap_small_#{i}", "val_#{i}"}
      :ok = Router.batch_put(default_ctx(), kvs)

      for {key, value} <- kvs do
        assert Router.get(default_ctx(), key) == value
      end
    end

    test "small batch publish does not overwrite an already materialized Ra apply" do
      ctx = default_ctx()
      key = "#{@ns_batch}:bap_materialized_race"
      idx = Router.shard_for(ctx, key)
      keydir = elem(ctx.keydir_refs, idx)

      try do
        :ets.insert(keydir, {key, "value", 0, 0, 7, 123, byte_size("value")})

        Router.__install_batch_entries_for_test__(
          ctx,
          idx,
          [{key, "value", "value"}],
          %{}
        )

        assert [{^key, "value", 0, _lfu, 7, 123, 5}] = :ets.lookup(keydir, key)
      after
        :ets.delete(keydir, key)
      end
    end

    test "all-large batch: values > hot_cache_max written to disk" do
      big = :binary.copy("L", 100 * 1024)
      kvs = for i <- 1..5, do: {"#{@ns_batch}:bap_large_#{i}", big}
      :ok = Router.batch_put(default_ctx(), kvs)

      for {key, _} <- kvs do
        assert Router.get(default_ctx(), key) == big
      end
    end

    test "mixed large/small batch: all values readable" do
      small = "small_value"
      big = :binary.copy("M", 100 * 1024)

      kvs = [
        {"#{@ns_batch}:bap_mix_s1", small},
        {"#{@ns_batch}:bap_mix_l1", big},
        {"#{@ns_batch}:bap_mix_s2", small},
        {"#{@ns_batch}:bap_mix_l2", big},
        {"#{@ns_batch}:bap_mix_s3", small}
      ]

      :ok = Router.batch_put(default_ctx(), kvs)

      for {key, value} <- kvs do
        assert Router.get(default_ctx(), key) == value
      end
    end

    test "empty batch is a no-op" do
      assert :ok = Router.batch_put(default_ctx(), [])
    end

    test "single-element batch works" do
      kvs = [{"#{@ns_batch}:bap_single", "one"}]
      :ok = Router.batch_put(default_ctx(), kvs)
      assert Router.get(default_ctx(), "#{@ns_batch}:bap_single") == "one"
    end

    test "overwrites existing keys" do
      key = "#{@ns_batch}:bap_overwrite"
      :ok = Router.put(default_ctx(), key, "original", 0)
      assert Router.get(default_ctx(), key) == "original"

      :ok = Router.batch_put(default_ctx(), [{key, "updated"}])
      assert Router.get(default_ctx(), key) == "updated"
    end

    test "overwriting a compound key clears compound metadata and fields" do
      key = "#{@ns_batch}:bap_overwrite_hash"
      field_key = CompoundKey.hash_field(key, "field")

      :ok = Router.compound_put(default_ctx(), key, CompoundKey.type_key(key), "hash", 0)
      :ok = Router.compound_put(default_ctx(), key, field_key, "hash_val", 0)
      assert "hash" == Router.compound_get(default_ctx(), key, CompoundKey.type_key(key))
      assert "hash_val" == Router.compound_get(default_ctx(), key, field_key)

      :ok = Router.batch_put(default_ctx(), [{key, "string_val"}])

      assert "string_val" == Router.get(default_ctx(), key)
      assert nil == Router.compound_get(default_ctx(), key, CompoundKey.type_key(key))
      assert nil == Router.compound_get(default_ctx(), key, field_key)
    end

    test "duplicate keys in one batch use the last value" do
      key = "#{@ns_batch}:bap_duplicate"

      :ok = Router.batch_put(default_ctx(), [{key, "first"}, {key, "second"}])

      assert Router.get(default_ctx(), key) == "second"
    end
  end

  # ---------------------------------------------------------------------------
  # FerricStore.batch_set (public API)
  # ---------------------------------------------------------------------------

  describe "FerricStore.batch_set" do
    test "all-quorum namespace returns list of :ok" do
      kvs = for i <- 1..10, do: {"#{@ns_quorum}:bs_#{i}", "v#{i}"}
      results = FerricStore.batch_set(kvs)
      assert Enum.all?(results, &(&1 == :ok))

      for {key, value} <- kvs do
        assert {:ok, value} == FerricStore.get(key)
      end
    end

    test "empty quorum batch returns empty list" do
      assert [] = Router.batch_quorum_put(default_ctx(), [])
    end

    test "mixed prefixes preserve result order on quorum path" do
      kvs = [
        {"#{@ns_batch}:mix_1", "batch_val"},
        {"#{@ns_quorum}:mix_2", "quorum_val"},
        {"#{@ns_batch}:mix_3", "batch_val2"},
        {"#{@ns_quorum}:mix_4", "quorum_val2"}
      ]

      results = FerricStore.batch_set(kvs)
      assert length(results) == 4
      assert Enum.all?(results, &(&1 == :ok))

      assert {:ok, "batch_val"} == FerricStore.get("#{@ns_batch}:mix_1")
      assert {:ok, "quorum_val"} == FerricStore.get("#{@ns_quorum}:mix_2")
      assert {:ok, "batch_val2"} == FerricStore.get("#{@ns_batch}:mix_3")
      assert {:ok, "quorum_val2"} == FerricStore.get("#{@ns_quorum}:mix_4")
    end

    test "empty list returns empty list" do
      assert FerricStore.batch_set([]) == []
    end
  end

  # ---------------------------------------------------------------------------
  # FerricStore.batch_get (public API)
  # ---------------------------------------------------------------------------

  describe "FerricStore.batch_get" do
    test "returns values in same order as keys" do
      for i <- 1..5 do
        :ok = FerricStore.set("bg_order_#{i}", "val_#{i}")
      end

      keys = for i <- 1..5, do: "bg_order_#{i}"
      results = FerricStore.batch_get(keys)
      assert results == ["val_1", "val_2", "val_3", "val_4", "val_5"]
    end

    test "returns nil for missing keys" do
      :ok = FerricStore.set("bg_exists", "here")
      results = FerricStore.batch_get(["bg_exists", "bg_missing", "bg_also_missing"])
      assert results == ["here", nil, nil]
    end

    test "empty list returns empty list" do
      assert FerricStore.batch_get([]) == []
    end

    test "single key works" do
      :ok = FerricStore.set("bg_single", "solo")
      assert FerricStore.batch_get(["bg_single"]) == ["solo"]
    end

    test "falls back to shard read when direct keydir ref is unavailable" do
      key = "bg_stale_keydir_#{System.unique_integer([:positive])}"
      :ok = Router.put(ctx(), key, "from_shard", 0)

      idx = Router.shard_for(ctx(), key)

      stale_ctx = %{
        ctx()
        | keydir_refs: put_elem(ctx().keydir_refs, idx, :missing_keydir_for_test)
      }

      assert Router.get(stale_ctx, key) == "from_shard"
      assert Router.batch_get(stale_ctx, [key, "bg_stale_missing"]) == ["from_shard", nil]
    end

    @tag timeout: 120_000
    test "large batch (1000 keys) returns correct results" do
      kvs = for i <- 1..1000, do: {"bg_large_#{i}", "v#{i}"}
      FerricStore.batch_set(kvs)

      keys = for i <- 1..1000, do: "bg_large_#{i}"
      results = FerricStore.batch_get(keys)
      assert length(results) == 1000
      assert Enum.at(results, 0) == "v1"
      assert Enum.at(results, 999) == "v1000"
    end
  end

  # ---------------------------------------------------------------------------
  # Router.keys
  # ---------------------------------------------------------------------------

  describe "Router.keys" do
    test "removes expired keys from ETS and byte accounting" do
      key = "#{@ns_quorum}:keys_expired_cleanup:#{String.duplicate("k", 80)}"
      value = String.duplicate("v", 128)
      idx = Router.shard_for(ctx(), key)
      keydir = elem(ctx().keydir_refs, idx)
      expired_at = Ferricstore.HLC.now_ms() - 1

      before_bytes = :atomics.get(ctx().keydir_binary_bytes, idx + 1)
      assert :ok = Router.put(ctx(), key, value, expired_at)
      assert [{^key, ^value, ^expired_at, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir, key)
      assert :atomics.get(ctx().keydir_binary_bytes, idx + 1) > before_bytes

      refute key in Router.keys(ctx())
      assert :ets.lookup(keydir, key) == []
      assert :atomics.get(ctx().keydir_binary_bytes, idx + 1) == before_bytes
    end
  end

  # ---------------------------------------------------------------------------
  # Router.dbsize
  # ---------------------------------------------------------------------------

  describe "Router.dbsize" do
    test "does not count expired keys before the sweep removes them" do
      past = System.os_time(:millisecond) - 1_000

      :ok = Router.put(ctx(), "#{@ns_quorum}:dbsize_live", "live", 0)
      :ok = Router.put(ctx(), "#{@ns_quorum}:dbsize_expired", "expired", past)

      assert Router.dbsize(ctx()) == 1
    end

    test "removes expired keys from ETS and byte accounting" do
      key = "#{@ns_quorum}:dbsize_expired_cleanup:#{String.duplicate("k", 80)}"
      value = String.duplicate("v", 128)
      idx = Router.shard_for(ctx(), key)
      keydir = elem(ctx().keydir_refs, idx)
      expired_at = Ferricstore.HLC.now_ms() - 1

      before_bytes = :atomics.get(ctx().keydir_binary_bytes, idx + 1)
      assert :ok = Router.put(ctx(), key, value, expired_at)
      assert [{^key, ^value, ^expired_at, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir, key)
      assert :atomics.get(ctx().keydir_binary_bytes, idx + 1) > before_bytes

      assert Router.dbsize(ctx()) == 0
      assert :ets.lookup(keydir, key) == []
      assert :atomics.get(ctx().keydir_binary_bytes, idx + 1) == before_bytes
    end
  end

  # ---------------------------------------------------------------------------
  # Router.exists?
  # ---------------------------------------------------------------------------

  describe "Router.exists?" do
    test "removes expired keys from ETS and byte accounting" do
      key = "#{@ns_quorum}:exists_expired_cleanup:#{String.duplicate("k", 80)}"
      value = String.duplicate("v", 128)
      idx = Router.shard_for(ctx(), key)
      keydir = elem(ctx().keydir_refs, idx)
      expired_at = Ferricstore.HLC.now_ms() - 1

      before_bytes = :atomics.get(ctx().keydir_binary_bytes, idx + 1)
      assert :ok = Router.put(ctx(), key, value, expired_at)
      assert [{^key, ^value, ^expired_at, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir, key)
      assert :atomics.get(ctx().keydir_binary_bytes, idx + 1) > before_bytes

      assert Router.exists?(ctx(), key) == false
      assert :ets.lookup(keydir, key) == []
      assert :atomics.get(ctx().keydir_binary_bytes, idx + 1) == before_bytes
    end
  end

  # ---------------------------------------------------------------------------
  # Router.get_keydir_file_ref
  # ---------------------------------------------------------------------------

  describe "Router.get_keydir_file_ref" do
    test "removes expired keys from ETS and byte accounting" do
      key = "#{@ns_quorum}:file_ref_expired_cleanup:#{String.duplicate("k", 80)}"
      value = String.duplicate("v", 128)
      idx = Router.shard_for(ctx(), key)
      keydir = elem(ctx().keydir_refs, idx)
      expired_at = Ferricstore.HLC.now_ms() - 1

      before_bytes = :atomics.get(ctx().keydir_binary_bytes, idx + 1)
      assert :ok = Router.put(ctx(), key, value, expired_at)
      assert [{^key, ^value, ^expired_at, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir, key)
      assert :atomics.get(ctx().keydir_binary_bytes, idx + 1) > before_bytes

      assert Router.get_keydir_file_ref(ctx(), key) == :miss
      assert :ets.lookup(keydir, key) == []
      assert :atomics.get(ctx().keydir_binary_bytes, idx + 1) == before_bytes
    end
  end

  # ---------------------------------------------------------------------------
  # FerricStore.packed_batch_get (binary protocol)
  # ---------------------------------------------------------------------------

  describe "FerricStore.packed_batch_get" do
    test "round-trips correctly" do
      for i <- 1..3, do: FerricStore.set("pbg_#{i}", "val_#{i}")

      keys = ["pbg_1", "pbg_2", "pbg_3"]
      packed = pack_keys(keys)
      result = FerricStore.packed_batch_get(packed)

      values = unpack_values(result, 3)
      assert values == ["val_1", "val_2", "val_3"]
    end

    test "nil values encoded as 0xFFFFFFFF" do
      FerricStore.set("pbg_exists", "here")
      packed = pack_keys(["pbg_exists", "pbg_nope"])
      result = FerricStore.packed_batch_get(packed)

      values = unpack_values(result, 2)
      assert values == ["here", nil]
    end

    test "single key" do
      FerricStore.set("pbg_one", "solo")
      packed = pack_keys(["pbg_one"])
      result = FerricStore.packed_batch_get(packed)
      assert unpack_values(result, 1) == ["solo"]
    end
  end

  # ---------------------------------------------------------------------------
  # Delete origin-skip correctness
  # ---------------------------------------------------------------------------

  describe "delete replay" do
    test "delete is applied on all nodes (not skipped on origin)" do
      key = "#{@ns_batch}:del_origin_#{:erlang.unique_integer([:positive])}"
      :ok = Router.put(ctx(), key, "present", 0)
      assert Router.get(ctx(), key) == "present"

      Router.delete(ctx(), key)
      assert Router.get(ctx(), key) == nil

      Process.sleep(200)
      assert Router.get(ctx(), key) == nil
    end

    test "batch delete followed by set works correctly" do
      key = "#{@ns_batch}:del_then_set_#{:erlang.unique_integer([:positive])}"
      :ok = Router.put(ctx(), key, "first", 0)
      Router.delete(ctx(), key)
      :ok = Router.put(ctx(), key, "second", 0)
      assert Router.get(ctx(), key) == "second"
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp pack_keys(keys) do
    count = length(keys)
    body = for k <- keys, into: <<>>, do: <<byte_size(k)::16, k::binary>>
    <<count::32, body::binary>>
  end

  defp unpack_values(<<>>, 0), do: []
  defp unpack_values(<<0xFFFFFFFF::32, rest::binary>>, n), do: [nil | unpack_values(rest, n - 1)]

  defp unpack_values(<<len::32, val::binary-size(len), rest::binary>>, n),
    do: [val | unpack_values(rest, n - 1)]
end
