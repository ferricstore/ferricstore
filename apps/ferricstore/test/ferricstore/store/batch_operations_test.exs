defmodule Ferricstore.Store.BatchOperationsTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Store.{CompoundKey, Router}
  alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle

  @ns_async "batch_test_async"
  @ns_quorum "batch_test_quorum"

  setup do
    ctx = Ferricstore.Test.IsolatedInstance.checkout()
    Ferricstore.NamespaceConfig.set(@ns_async, "durability", "async")

    on_exit(fn ->
      Ferricstore.NamespaceConfig.set(@ns_async, "durability", "quorum")
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
    end)

    Process.put(:test_ctx, ctx)
    :ok
  end

  defp ctx, do: Process.get(:test_ctx)
  defp default_ctx, do: FerricStore.Instance.get(:default)

  # ---------------------------------------------------------------------------
  # batch_async_put
  # ---------------------------------------------------------------------------

  describe "batch_async_put" do
    test "submits origin-checked PUT commands for stale replay safety" do
      source =
        Path.expand("../../../lib/ferricstore/store/router.ex", __DIR__)
        |> File.read!()

      assert source =~
               "origin_checked_command(key, {:put, key, value, 0}, previous, value, 0)",
             """
             batch_async_put must not submit raw {:put, key, value, 0} origin commands.
             A delayed origin replay of the raw PUT can overwrite later local RMW writes.
             """
    end

    test "all-small batch: values readable immediately" do
      kvs = for i <- 1..20, do: {"#{@ns_async}:bap_small_#{i}", "val_#{i}"}
      :ok = Router.batch_async_put(default_ctx(), kvs)

      for {key, value} <- kvs do
        assert Router.get(default_ctx(), key) == value
      end
    end

    test "small batch publish does not overwrite an already materialized Ra apply" do
      ctx = default_ctx()
      key = "#{@ns_async}:bap_materialized_race"
      idx = Router.shard_for(ctx, key)
      keydir = elem(ctx.keydir_refs, idx)

      try do
        :ets.insert(keydir, {key, "value", 0, 0, 7, 123, byte_size("value")})

        Router.__install_batch_async_entries_for_test__(
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
      kvs = for i <- 1..5, do: {"#{@ns_async}:bap_large_#{i}", big}
      :ok = Router.batch_async_put(default_ctx(), kvs)

      for {key, _} <- kvs do
        assert Router.get(default_ctx(), key) == big
      end
    end

    test "mixed large/small batch: all values readable" do
      small = "small_value"
      big = :binary.copy("M", 100 * 1024)

      kvs = [
        {"#{@ns_async}:bap_mix_s1", small},
        {"#{@ns_async}:bap_mix_l1", big},
        {"#{@ns_async}:bap_mix_s2", small},
        {"#{@ns_async}:bap_mix_l2", big},
        {"#{@ns_async}:bap_mix_s3", small}
      ]

      :ok = Router.batch_async_put(default_ctx(), kvs)

      for {key, value} <- kvs do
        assert Router.get(default_ctx(), key) == value
      end
    end

    test "empty batch is a no-op" do
      assert :ok = Router.batch_async_put(default_ctx(), [])
    end

    test "single-element batch works" do
      kvs = [{"#{@ns_async}:bap_single", "one"}]
      :ok = Router.batch_async_put(default_ctx(), kvs)
      assert Router.get(default_ctx(), "#{@ns_async}:bap_single") == "one"
    end

    test "overwrites existing keys" do
      key = "#{@ns_async}:bap_overwrite"
      :ok = Router.put(default_ctx(), key, "original", 0)
      assert Router.get(default_ctx(), key) == "original"

      :ok = Router.batch_async_put(default_ctx(), [{key, "updated"}])
      assert Router.get(default_ctx(), key) == "updated"
    end

    test "overwriting a compound key clears compound metadata and fields" do
      key = "#{@ns_async}:bap_overwrite_hash"
      field_key = CompoundKey.hash_field(key, "field")

      :ok = Router.compound_put(default_ctx(), key, CompoundKey.type_key(key), "hash", 0)
      :ok = Router.compound_put(default_ctx(), key, field_key, "hash_val", 0)
      assert "hash" == Router.compound_get(default_ctx(), key, CompoundKey.type_key(key))
      assert "hash_val" == Router.compound_get(default_ctx(), key, field_key)

      :ok = Router.batch_async_put(default_ctx(), [{key, "string_val"}])

      assert "string_val" == Router.get(default_ctx(), key)
      assert nil == Router.compound_get(default_ctx(), key, CompoundKey.type_key(key))
      assert nil == Router.compound_get(default_ctx(), key, field_key)
    end

    test "duplicate keys in one async batch use the last value" do
      key = "#{@ns_async}:bap_duplicate"

      :ok = Router.batch_async_put(default_ctx(), [{key, "first"}, {key, "second"}])

      assert Router.get(default_ctx(), key) == "second"
    end

    test "mixed batch rolls back same-shard small keys when large disk write fails" do
      {small_key, large_key} = same_shard_keys(default_ctx(), "bap_disk_fail")
      idx = Router.shard_for(default_ctx(), small_key)
      original = Ferricstore.Store.ActiveFile.get(default_ctx(), idx)

      missing_dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore_missing_active_#{System.unique_integer([:positive])}"
        )

      missing_path = Path.join(missing_dir, "00000.log")
      Ferricstore.Store.ActiveFile.publish(default_ctx(), idx, 99_999, missing_path, missing_dir)

      try do
        large = :binary.copy("X", 100 * 1024)

        assert {:error, "ERR disk write failed" <> _} =
                 Router.batch_async_put(default_ctx(), [{small_key, "small"}, {large_key, large}])

        assert nil == Router.get(default_ctx(), small_key)
        assert nil == Router.get(default_ctx(), large_key)
      after
        {file_id, file_path, shard_data_path} = original

        Ferricstore.Store.ActiveFile.publish(
          default_ctx(),
          idx,
          file_id,
          file_path,
          shard_data_path
        )
      end
    end

    test "failed mixed batch restores overwritten same-shard keys" do
      {small_key, large_key} = same_shard_keys(default_ctx(), "bap_disk_fail_existing")
      idx = Router.shard_for(default_ctx(), small_key)
      :ok = Router.put(default_ctx(), small_key, "before", 0)
      original = Ferricstore.Store.ActiveFile.get(default_ctx(), idx)

      missing_dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore_missing_active_#{System.unique_integer([:positive])}"
        )

      missing_path = Path.join(missing_dir, "00000.log")
      Ferricstore.Store.ActiveFile.publish(default_ctx(), idx, 99_998, missing_path, missing_dir)

      try do
        large = :binary.copy("Y", 100 * 1024)

        assert {:error, "ERR disk write failed" <> _} =
                 Router.batch_async_put(default_ctx(), [{small_key, "after"}, {large_key, large}])

        assert "before" == Router.get(default_ctx(), small_key)
        assert nil == Router.get(default_ctx(), large_key)
      after
        {file_id, file_path, shard_data_path} = original

        Ferricstore.Store.ActiveFile.publish(
          default_ctx(),
          idx,
          file_id,
          file_path,
          shard_data_path
        )
      end
    end

    test "cross-shard failure does not leave earlier shard writes visible" do
      small_key = key_for_shard(default_ctx(), "bap_cross_fail_small", 0)
      large_key = key_for_shard(default_ctx(), "bap_cross_fail_large", 1)
      original = Ferricstore.Store.ActiveFile.get(default_ctx(), 1)

      missing_dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore_missing_active_#{System.unique_integer([:positive])}"
        )

      missing_path = Path.join(missing_dir, "00000.log")
      Ferricstore.Store.ActiveFile.publish(default_ctx(), 1, 99_997, missing_path, missing_dir)

      try do
        large = :binary.copy("Q", 100 * 1024)

        assert {:error, "ERR disk write failed" <> _} =
                 Router.batch_async_put(default_ctx(), [{small_key, "small"}, {large_key, large}])

        assert nil == Router.get(default_ctx(), small_key)
        assert nil == Router.get(default_ctx(), large_key)
      after
        {file_id, file_path, shard_data_path} = original

        Ferricstore.Store.ActiveFile.publish(
          default_ctx(),
          1,
          file_id,
          file_path,
          shard_data_path
        )
      end
    end

    test "cross-shard async replication overload does not leave earlier shard writes visible" do
      ok_key = key_for_shard(default_ctx(), "bap_cross_overload_ok", 0)
      overloaded_key = key_for_shard(default_ctx(), "bap_cross_overload_blocked", 1)

      on_exit(fn -> Batcher.reset_pending(1) end)
      fill_async_pending(1, overloaded_key)

      assert {:error, "ERR async replication overloaded"} =
               Router.batch_async_put(default_ctx(), [{ok_key, "ok"}, {overloaded_key, "blocked"}])

      assert nil == Router.get(default_ctx(), ok_key)
      assert nil == Router.get(default_ctx(), overloaded_key)
    end

    test "large batch does not recover unaccepted value when async replication is overloaded" do
      key = "#{@ns_async}:bap_overloaded_large_missing_#{System.unique_integer([:positive])}"
      idx = Router.shard_for(default_ctx(), key)
      large = :binary.copy("B", default_ctx().hot_cache_max_value_size + 1024)

      on_exit(fn -> Batcher.reset_pending(idx) end)
      fill_async_pending(idx, key)

      assert {:error, "ERR async replication overloaded"} =
               Router.batch_async_put(default_ctx(), [{key, large}])

      assert nil == Router.get(default_ctx(), key)
      assert nil == recovered_value_from_bitcask(default_ctx(), key)
    end

    test "large batch restores previous cold value when async replication is overloaded" do
      key = "#{@ns_async}:bap_overloaded_large_existing_#{System.unique_integer([:positive])}"
      idx = Router.shard_for(default_ctx(), key)
      old = :binary.copy("O", default_ctx().hot_cache_max_value_size + 1024)
      new = :binary.copy("N", default_ctx().hot_cache_max_value_size + 2048)

      :ok = Router.put(default_ctx(), key, old, 0)
      assert old == Router.get(default_ctx(), key)

      on_exit(fn -> Batcher.reset_pending(idx) end)
      fill_async_pending(idx, key)

      assert {:error, "ERR async replication overloaded"} =
               Router.batch_async_put(default_ctx(), [{key, new}])

      assert old == Router.get(default_ctx(), key)
      assert old == recovered_value_from_bitcask(default_ctx(), key)
    end
  end

  # ---------------------------------------------------------------------------
  # FerricStore.batch_set (public API)
  # ---------------------------------------------------------------------------

  describe "FerricStore.batch_set" do
    test "all-async namespace returns list of :ok" do
      kvs = for i <- 1..10, do: {"#{@ns_async}:bs_#{i}", "v#{i}"}
      results = FerricStore.batch_set(kvs)
      assert results == List.duplicate(:ok, 10)

      for {key, value} <- kvs do
        assert {:ok, value} == FerricStore.get(key)
      end
    end

    test "all-async batch reports overload per shard instead of failing every key" do
      ok_key = key_for_shard(default_ctx(), "bs_cross_overload_ok", 0)
      overloaded_key = key_for_shard(default_ctx(), "bs_cross_overload_blocked", 1)

      on_exit(fn -> Batcher.reset_pending(1) end)
      fill_async_pending(1, overloaded_key)

      assert [
               :ok,
               {:error, "ERR async replication overloaded"}
             ] =
               FerricStore.__async_batch_put_result_list__(
                 default_ctx(),
                 [{ok_key, "ok"}, {overloaded_key, "blocked"}]
               )

      assert "ok" == Router.get(default_ctx(), ok_key)
      assert nil == Router.get(default_ctx(), overloaded_key)
    end

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

    test "mixed async/quorum namespaces preserves result order" do
      kvs = [
        {"#{@ns_async}:mix_1", "async_val"},
        {"#{@ns_quorum}:mix_2", "quorum_val"},
        {"#{@ns_async}:mix_3", "async_val2"},
        {"#{@ns_quorum}:mix_4", "quorum_val2"}
      ]

      results = FerricStore.batch_set(kvs)
      assert length(results) == 4
      assert Enum.all?(results, &(&1 == :ok))

      assert {:ok, "async_val"} == FerricStore.get("#{@ns_async}:mix_1")
      assert {:ok, "quorum_val"} == FerricStore.get("#{@ns_quorum}:mix_2")
      assert {:ok, "async_val2"} == FerricStore.get("#{@ns_async}:mix_3")
      assert {:ok, "quorum_val2"} == FerricStore.get("#{@ns_quorum}:mix_4")
    end

    test "empty list returns empty list" do
      assert FerricStore.batch_set([]) == []
    end

    test "async disk failure is returned to every async batch_set key" do
      default_ctx = FerricStore.Instance.get(:default)
      {small_key, large_key} = same_shard_keys(default_ctx, "bs_disk_fail")
      idx = Router.shard_for(default_ctx, small_key)
      original = Ferricstore.Store.ActiveFile.get(default_ctx, idx)

      missing_dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore_missing_active_#{System.unique_integer([:positive])}"
        )

      missing_path = Path.join(missing_dir, "00000.log")
      Ferricstore.Store.ActiveFile.publish(default_ctx, idx, 98_998, missing_path, missing_dir)

      try do
        large = :binary.copy("Z", 100 * 1024)

        assert [
                 {:error, "ERR disk write failed" <> _},
                 {:error, "ERR disk write failed" <> _}
               ] = FerricStore.batch_set([{small_key, "small"}, {large_key, large}])

        assert {:ok, nil} == FerricStore.get(small_key)
        assert {:ok, nil} == FerricStore.get(large_key)
      after
        {file_id, file_path, shard_data_path} = original

        Ferricstore.Store.ActiveFile.publish(
          default_ctx,
          idx,
          file_id,
          file_path,
          shard_data_path
        )
      end
    end

    test "async disk pressure rejects only pressured async batch_set keys" do
      default_ctx = FerricStore.Instance.get(:default)
      pressured_key = "#{@ns_async}:bs_pressure_#{System.unique_integer([:positive])}"
      ok_key = different_shard_key(default_ctx, pressured_key, "#{@ns_async}:bs_pressure_ok")
      idx = Router.shard_for(default_ctx, pressured_key)

      Ferricstore.Store.DiskPressure.set(default_ctx, idx)

      try do
        assert [
                 {:error, "ERR disk pressure on shard " <> _},
                 :ok
               ] = FerricStore.batch_set([{pressured_key, "blocked"}, {ok_key, "allowed"}])

        assert {:ok, nil} == FerricStore.get(pressured_key)
        assert {:ok, "allowed"} == FerricStore.get(ok_key)
      after
        Ferricstore.Store.DiskPressure.clear(default_ctx, idx)
      end
    end

    test "async keydir pressure rejects new batch_set keys but allows updates" do
      existing_key = "#{@ns_async}:bs_keydir_existing_#{System.unique_integer([:positive])}"
      new_key = "#{@ns_async}:bs_keydir_new_#{System.unique_integer([:positive])}"

      assert [:ok] = FerricStore.batch_set([{existing_key, "old"}])

      Ferricstore.MemoryGuard.set_keydir_full(true)

      try do
        assert [
                 :ok,
                 {:error, "KEYDIR_FULL cannot accept new keys, keydir RAM limit reached"}
               ] = FerricStore.batch_set([{existing_key, "updated"}, {new_key, "blocked"}])

        assert {:ok, "updated"} == FerricStore.get(existing_key)
        assert {:ok, nil} == FerricStore.get(new_key)
      after
        Ferricstore.MemoryGuard.set_keydir_full(false)
      end
    end

    test "async batch_set rejects overlarge keys before writing" do
      default_ctx = FerricStore.Instance.get(:default)
      key = "#{@ns_async}:" <> String.duplicate("k", 65_536)
      keydir = elem(default_ctx.keydir_refs, Router.shard_for(default_ctx, key))

      assert [{:error, "ERR key too large (max 65535 bytes)"}] =
               FerricStore.batch_set([{key, "too-large"}])

      assert :ets.lookup(keydir, key) == []
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
    test "does not return pending async locations as disk file refs" do
      key = "#{@ns_async}:pending_file_ref"
      idx = Router.shard_for(default_ctx(), key)
      keydir = elem(default_ctx().keydir_refs, idx)

      :ok = Router.batch_async_put(default_ctx(), [{key, "small"}])
      assert [{^key, "small", 0, _lfu, :pending, 0, _vsize}] = :ets.lookup(keydir, key)

      assert Router.get_keydir_file_ref(default_ctx(), key) == :miss
      assert [{^key, "small", 0, _lfu, :pending, 0, _vsize}] = :ets.lookup(keydir, key)
      assert Router.get(default_ctx(), key) == "small"
    end

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
  # Async delete origin-skip correctness
  # ---------------------------------------------------------------------------

  describe "async delete" do
    test "delete is applied on all nodes (not skipped on origin)" do
      key = "#{@ns_async}:del_origin_#{:erlang.unique_integer([:positive])}"
      :ok = Router.put(ctx(), key, "present", 0)
      assert Router.get(ctx(), key) == "present"

      Router.delete(ctx(), key)
      assert Router.get(ctx(), key) == nil

      Process.sleep(200)
      assert Router.get(ctx(), key) == nil
    end

    test "batch delete followed by set works correctly" do
      key = "#{@ns_async}:del_then_set_#{:erlang.unique_integer([:positive])}"
      :ok = Router.put(ctx(), key, "first", 0)
      Router.delete(ctx(), key)
      :ok = Router.put(ctx(), key, "second", 0)
      assert Router.get(ctx(), key) == "second"
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp same_shard_keys(ctx, base) do
    prefix = "#{@ns_async}:#{base}:#{System.unique_integer([:positive])}"
    keys = for i <- 1..200, do: "#{prefix}:#{i}"

    Enum.find_value(keys, fn left ->
      Enum.find_value(keys, fn
        ^left ->
          nil

        right ->
          if Router.shard_for(ctx, left) == Router.shard_for(ctx, right), do: {left, right}
      end)
    end)
  end

  defp different_shard_key(ctx, key, base) do
    shard_idx = Router.shard_for(ctx, key)
    prefix = "#{base}:#{System.unique_integer([:positive])}"

    1..500
    |> Stream.map(fn i -> "#{prefix}:#{i}" end)
    |> Enum.find(fn candidate -> Router.shard_for(ctx, candidate) != shard_idx end)
  end

  defp key_for_shard(ctx, base, shard_idx) do
    prefix = "#{@ns_async}:#{base}:#{System.unique_integer([:positive])}"

    1..500
    |> Stream.map(fn i -> "#{prefix}:#{i}" end)
    |> Enum.find(fn key -> Router.shard_for(ctx, key) == shard_idx end)
  end

  defp fill_async_pending(idx, key) do
    for _ <- 1..64 do
      Batcher.__inject_async_pending__(
        idx,
        make_ref(),
        [{:async, node(), {:put, key, "pending", 0}}],
        0
      )
    end
  end

  defp recovered_value_from_bitcask(ctx, key) do
    idx = Router.shard_for(ctx, key)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
    keydir = :ets.new(:batch_operations_recovery, [:set, :public])

    try do
      ShardLifecycle.recover_keydir(shard_path, keydir, idx)

      case :ets.lookup(keydir, key) do
        [{^key, value, _exp, _lfu, _fid, _off, _vsize}] when value != nil ->
          value

        [{^key, nil, _exp, _lfu, fid, off, _vsize}] when is_integer(fid) ->
          path =
            Path.join(shard_path, "#{String.pad_leading(Integer.to_string(fid), 5, "0")}.log")

          {:ok, value} = NIF.v2_pread_at(path, off)
          value

        [] ->
          nil
      end
    after
      :ets.delete(keydir)
    end
  end

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
