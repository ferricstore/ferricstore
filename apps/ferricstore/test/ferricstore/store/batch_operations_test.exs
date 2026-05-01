defmodule Ferricstore.Store.BatchOperationsTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.{CompoundKey, Router}

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

  # ---------------------------------------------------------------------------
  # batch_async_put
  # ---------------------------------------------------------------------------

  describe "batch_async_put" do
    test "all-small batch: values readable immediately" do
      kvs = for i <- 1..20, do: {"#{@ns_async}:bap_small_#{i}", "val_#{i}"}
      :ok = Router.batch_async_put(ctx(), kvs)

      for {key, value} <- kvs do
        assert Router.get(ctx(), key) == value
      end
    end

    test "all-large batch: values > hot_cache_max written to disk" do
      big = :binary.copy("L", 100 * 1024)
      kvs = for i <- 1..5, do: {"#{@ns_async}:bap_large_#{i}", big}
      :ok = Router.batch_async_put(ctx(), kvs)

      for {key, _} <- kvs do
        assert Router.get(ctx(), key) == big
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

      :ok = Router.batch_async_put(ctx(), kvs)

      for {key, value} <- kvs do
        assert Router.get(ctx(), key) == value
      end
    end

    test "empty batch is a no-op" do
      assert :ok = Router.batch_async_put(ctx(), [])
    end

    test "single-element batch works" do
      kvs = [{"#{@ns_async}:bap_single", "one"}]
      :ok = Router.batch_async_put(ctx(), kvs)
      assert Router.get(ctx(), "#{@ns_async}:bap_single") == "one"
    end

    test "overwrites existing keys" do
      key = "#{@ns_async}:bap_overwrite"
      :ok = Router.put(ctx(), key, "original", 0)
      assert Router.get(ctx(), key) == "original"

      :ok = Router.batch_async_put(ctx(), [{key, "updated"}])
      assert Router.get(ctx(), key) == "updated"
    end

    test "overwriting a compound key clears compound metadata and fields" do
      key = "#{@ns_async}:bap_overwrite_hash"
      field_key = CompoundKey.hash_field(key, "field")

      :ok = Router.compound_put(ctx(), key, CompoundKey.type_key(key), "hash", 0)
      :ok = Router.compound_put(ctx(), key, field_key, "hash_val", 0)
      assert "hash" == Router.compound_get(ctx(), key, CompoundKey.type_key(key))
      assert "hash_val" == Router.compound_get(ctx(), key, field_key)

      :ok = Router.batch_async_put(ctx(), [{key, "string_val"}])

      assert "string_val" == Router.get(ctx(), key)
      assert nil == Router.compound_get(ctx(), key, CompoundKey.type_key(key))
      assert nil == Router.compound_get(ctx(), key, field_key)
    end

    test "duplicate keys in one async batch use the last value" do
      key = "#{@ns_async}:bap_duplicate"

      :ok = Router.batch_async_put(ctx(), [{key, "first"}, {key, "second"}])

      assert Router.get(ctx(), key) == "second"
    end

    test "mixed batch rolls back same-shard small keys when large disk write fails" do
      {small_key, large_key} = same_shard_keys(ctx(), "bap_disk_fail")
      idx = Router.shard_for(ctx(), small_key)
      original = Ferricstore.Store.ActiveFile.get(ctx(), idx)

      missing_dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore_missing_active_#{System.unique_integer([:positive])}"
        )

      missing_path = Path.join(missing_dir, "00000.log")
      Ferricstore.Store.ActiveFile.publish(ctx(), idx, 99_999, missing_path, missing_dir)

      try do
        large = :binary.copy("X", 100 * 1024)

        assert {:error, "ERR disk write failed" <> _} =
                 Router.batch_async_put(ctx(), [{small_key, "small"}, {large_key, large}])

        assert nil == Router.get(ctx(), small_key)
        assert nil == Router.get(ctx(), large_key)
      after
        {file_id, file_path, shard_data_path} = original
        Ferricstore.Store.ActiveFile.publish(ctx(), idx, file_id, file_path, shard_data_path)
      end
    end

    test "failed mixed batch restores overwritten same-shard keys" do
      {small_key, large_key} = same_shard_keys(ctx(), "bap_disk_fail_existing")
      idx = Router.shard_for(ctx(), small_key)
      :ok = Router.put(ctx(), small_key, "before", 0)
      original = Ferricstore.Store.ActiveFile.get(ctx(), idx)

      missing_dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore_missing_active_#{System.unique_integer([:positive])}"
        )

      missing_path = Path.join(missing_dir, "00000.log")
      Ferricstore.Store.ActiveFile.publish(ctx(), idx, 99_998, missing_path, missing_dir)

      try do
        large = :binary.copy("Y", 100 * 1024)

        assert {:error, "ERR disk write failed" <> _} =
                 Router.batch_async_put(ctx(), [{small_key, "after"}, {large_key, large}])

        assert "before" == Router.get(ctx(), small_key)
        assert nil == Router.get(ctx(), large_key)
      after
        {file_id, file_path, shard_data_path} = original
        Ferricstore.Store.ActiveFile.publish(ctx(), idx, file_id, file_path, shard_data_path)
      end
    end

    test "cross-shard failure does not leave earlier shard writes visible" do
      small_key = key_for_shard(ctx(), "bap_cross_fail_small", 0)
      large_key = key_for_shard(ctx(), "bap_cross_fail_large", 1)
      original = Ferricstore.Store.ActiveFile.get(ctx(), 1)

      missing_dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore_missing_active_#{System.unique_integer([:positive])}"
        )

      missing_path = Path.join(missing_dir, "00000.log")
      Ferricstore.Store.ActiveFile.publish(ctx(), 1, 99_997, missing_path, missing_dir)

      try do
        large = :binary.copy("Q", 100 * 1024)

        assert {:error, "ERR disk write failed" <> _} =
                 Router.batch_async_put(ctx(), [{small_key, "small"}, {large_key, large}])

        assert nil == Router.get(ctx(), small_key)
        assert nil == Router.get(ctx(), large_key)
      after
        {file_id, file_path, shard_data_path} = original
        Ferricstore.Store.ActiveFile.publish(ctx(), 1, file_id, file_path, shard_data_path)
      end
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

    test "all-quorum namespace returns list of :ok" do
      kvs = for i <- 1..10, do: {"#{@ns_quorum}:bs_#{i}", "v#{i}"}
      results = FerricStore.batch_set(kvs)
      assert Enum.all?(results, &(&1 == :ok))

      for {key, value} <- kvs do
        assert {:ok, value} == FerricStore.get(key)
      end
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
  # Router.dbsize
  # ---------------------------------------------------------------------------

  describe "Router.dbsize" do
    test "does not count expired keys before the sweep removes them" do
      past = System.os_time(:millisecond) - 1_000

      :ok = Router.put(ctx(), "#{@ns_quorum}:dbsize_live", "live", 0)
      :ok = Router.put(ctx(), "#{@ns_quorum}:dbsize_expired", "expired", past)

      assert Router.dbsize(ctx()) == 1
    end
  end

  # ---------------------------------------------------------------------------
  # Router.get_keydir_file_ref
  # ---------------------------------------------------------------------------

  describe "Router.get_keydir_file_ref" do
    test "does not return pending async locations as disk file refs" do
      key = "#{@ns_async}:pending_file_ref"

      :ok = Router.batch_async_put(ctx(), [{key, "small"}])

      assert Router.get_keydir_file_ref(ctx(), key) == :miss
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

  defp key_for_shard(ctx, base, shard_idx) do
    prefix = "#{@ns_async}:#{base}:#{System.unique_integer([:positive])}"

    1..500
    |> Stream.map(fn i -> "#{prefix}:#{i}" end)
    |> Enum.find(fn key -> Router.shard_for(ctx, key) == shard_idx end)
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
