defmodule Ferricstore.Store.AsyncLargeValueTest do
  use ExUnit.Case, async: false
  @moduletag skip: "async durability feature removed; quorum is the only supported durability"
  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Store.Router

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    Ferricstore.NamespaceConfig.set("alv_test", "durability", "async")

    on_exit(fn ->
      Ferricstore.Test.ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  defp eventually(fun, msg, attempts \\ 400) do
    result =
      try do
        fun.()
      rescue
        _ -> false
      catch
        :exit, _ -> false
      end

    if result do
      :ok
    else
      if attempts > 0 do
        Process.sleep(50)
        eventually(fun, msg, attempts - 1)
      else
        flunk("Timed out: #{msg}")
      end
    end
  end

  defp cold_value(byte) do
    ctx = FerricStore.Instance.get(:default)
    :binary.copy(<<byte>>, ctx.hot_cache_max_value_size + 1024)
  end

  defp assert_cold_key(key) do
    ctx = FerricStore.Instance.get(:default)
    keydir = elem(ctx.keydir_refs, Router.shard_for(ctx, key))

    assert Router.durability_for_key_public(ctx, key) == :async
    assert [{^key, nil, _exp, _lfu, fid, off, value_size}] = :ets.lookup(keydir, key)
    assert is_integer(fid) and fid >= 0
    assert is_integer(off) and off >= 0
    assert value_size > ctx.hot_cache_max_value_size
  end

  describe "async write with large values (>64KB)" do
    test "large value is readable immediately after write" do
      big_value = :binary.copy("x", 100_000)
      :ok = Router.put(FerricStore.Instance.get(:default), "alv_test:big1", big_value, 0)

      eventually(
        fn -> Router.get(FerricStore.Instance.get(:default), "alv_test:big1") == big_value end,
        "large value should be readable after async flush"
      )
    end

    test "large async PUT marks Bitcask checkpoint dirty before Ra apply" do
      ctx = FerricStore.Instance.get(:default)
      key = "alv_test:checkpoint_dirty_#{System.unique_integer([:positive])}"
      idx = Router.shard_for(ctx, key)
      flag_idx = idx + 1
      value = cold_value(?d)

      :atomics.put(ctx.checkpoint_flags, flag_idx, 0)
      :atomics.put(ctx.checkpoint_in_flight, flag_idx, 0)

      assert :ok = Router.put(ctx, key, value, 0)

      assert :atomics.get(ctx.checkpoint_flags, flag_idx) == 1,
             "Router nosync large writes must mark Bitcask dirty before release_cursor can advance"
    end

    test "small value still works (inline ETS)" do
      :ok = Router.put(FerricStore.Instance.get(:default), "alv_test:small1", "hello", 0)
      assert Router.get(FerricStore.Instance.get(:default), "alv_test:small1") == "hello"
    end

    test "value at exactly 64KB boundary" do
      exact = :binary.copy("y", 65_536)
      :ok = Router.put(FerricStore.Instance.get(:default), "alv_test:exact", exact, 0)

      eventually(
        fn -> Router.get(FerricStore.Instance.get(:default), "alv_test:exact") == exact end,
        "64KB boundary value should be readable after async flush"
      )
    end

    test "multiple large values" do
      for i <- 1..10 do
        val = :binary.copy("z", 100_000 + i)
        :ok = Router.put(FerricStore.Instance.get(:default), "alv_test:multi_#{i}", val, 0)
      end

      # Large values (>64KB) are stored as nil in ETS and written to Bitcask
      # asynchronously. Wait for each value to be readable from disk.
      for i <- 1..10 do
        expected_size = 100_000 + i
        key = "alv_test:multi_#{i}"

        eventually(
          fn ->
            val = Router.get(FerricStore.Instance.get(:default), key)
            val != nil and byte_size(val) == expected_size
          end,
          "Key #{key} should have size #{expected_size}"
        )
      end
    end

    test "APPEND preserves an existing cold large value" do
      key = "alv_test:append_cold"
      value = cold_value(?a)
      suffix = "tail"

      :ok = Router.put(FerricStore.Instance.get(:default), key, value, 0)
      assert_cold_key(key)

      assert {:ok, byte_size(value) + byte_size(suffix)} == FerricStore.append(key, suffix)
      assert {:ok, value <> suffix} == FerricStore.get(key)
    end

    test "APPEND retries when a cold location changes during compaction race" do
      ctx = FerricStore.Instance.get(:default)
      key = "alv_test:append_cold_retry_#{System.unique_integer([:positive])}"
      value = cold_value(?r)
      suffix = "tail"
      idx = Router.shard_for(ctx, key)
      keydir = elem(ctx.keydir_refs, idx)
      test_pid = self()

      assert Router.durability_for_key_public(ctx, key) == :async

      :ok = Router.put(ctx, key, value, 0)
      assert_cold_key(key)

      assert [{^key, nil, exp, lfu, file_id, _offset, value_size} = live_entry] =
               :ets.lookup(keydir, key)

      stale_file_id = file_id + 10_000
      :ets.insert(keydir, {key, nil, exp, lfu, stale_file_id, 0, value_size})

      Process.put(:ferricstore_router_cold_location_miss_hook, fn ->
        send(test_pid, :cold_location_retry_hook)
        :ets.insert(keydir, live_entry)
      end)

      try do
        assert {:ok, byte_size(value) + byte_size(suffix)} == FerricStore.append(key, suffix)
        assert_receive :cold_location_retry_hook, 500
        assert {:ok, value <> suffix} == FerricStore.get(key)
      after
        Process.delete(:ferricstore_router_cold_location_miss_hook)
      end
    end

    test "APPEND returns disk error instead of acknowledging failed large RMW write" do
      ctx = FerricStore.Instance.get(:default)
      key = "alv_test:append_large_disk_error"
      value = cold_value(?e)
      suffix = cold_value(?s)
      idx = Router.shard_for(ctx, key)
      {file_id, file_path, shard_path} = Ferricstore.Store.ActiveFile.get(idx)

      missing_path =
        Path.join([
          System.tmp_dir!(),
          "missing_ferricstore_#{System.unique_integer([:positive])}",
          "00000.log"
        ])

      :ok = Router.put(ctx, key, value, 0)
      assert_cold_key(key)
      :ok = Batcher.flush(idx)

      handler_id = {:async_large_value, self(), make_ref()}

      :telemetry.attach(
        handler_id,
        [:ferricstore, :batcher, :async_flush],
        fn _event, _measurements, meta, test_pid ->
          if meta.shard_index == idx and meta.origin do
            send(test_pid, :unexpected_async_flush)
          end
        end,
        self()
      )

      Ferricstore.Store.ActiveFile.publish(idx, file_id, missing_path, Path.dirname(missing_path))

      try do
        assert {:error, "ERR disk write failed" <> _} = FerricStore.append(key, suffix)
        assert {:ok, ^value} = FerricStore.get(key)
        refute_receive :unexpected_async_flush, 200
      after
        :telemetry.detach(handler_id)
        Ferricstore.Store.ActiveFile.publish(idx, file_id, file_path, shard_path)
      end
    end

    test "large PUT disk error does not inflate keydir byte accounting" do
      ctx = FerricStore.Instance.get(:default)
      key = "alv_test:" <> :binary.copy("put_large_disk_error", 8)
      value = cold_value(?p)
      idx = Router.shard_for(ctx, key)
      keydir = elem(ctx.keydir_refs, idx)
      before_bytes = :atomics.get(ctx.keydir_binary_bytes, idx + 1)
      {file_id, file_path, shard_path} = Ferricstore.Store.ActiveFile.get(idx)

      missing_path =
        Path.join([
          System.tmp_dir!(),
          "missing_ferricstore_#{System.unique_integer([:positive])}",
          "00000.log"
        ])

      Ferricstore.Store.ActiveFile.publish(idx, file_id, missing_path, Path.dirname(missing_path))

      try do
        assert {:error, "ERR disk write failed" <> _} = Router.put(ctx, key, value, 0)
        assert :ets.lookup(keydir, key) == []
        assert :atomics.get(ctx.keydir_binary_bytes, idx + 1) == before_bytes
      after
        Ferricstore.Store.ActiveFile.publish(idx, file_id, file_path, shard_path)
      end
    end

    test "missing-key APPEND disk error does not inflate keydir byte accounting" do
      ctx = FerricStore.Instance.get(:default)
      key = "alv_test:" <> :binary.copy("append_missing_disk_error", 8)
      suffix = cold_value(?m)
      idx = Router.shard_for(ctx, key)
      keydir = elem(ctx.keydir_refs, idx)
      before_bytes = :atomics.get(ctx.keydir_binary_bytes, idx + 1)
      {file_id, file_path, shard_path} = Ferricstore.Store.ActiveFile.get(idx)

      missing_path =
        Path.join([
          System.tmp_dir!(),
          "missing_ferricstore_#{System.unique_integer([:positive])}",
          "00000.log"
        ])

      Ferricstore.Store.ActiveFile.publish(idx, file_id, missing_path, Path.dirname(missing_path))

      try do
        assert {:error, "ERR disk write failed" <> _} = FerricStore.append(key, suffix)
        assert :ets.lookup(keydir, key) == []
        assert :atomics.get(ctx.keydir_binary_bytes, idx + 1) == before_bytes
      after
        Ferricstore.Store.ActiveFile.publish(idx, file_id, file_path, shard_path)
      end
    end

    test "GETSET returns and replaces an existing cold large value" do
      key = "alv_test:getset_cold"
      value = cold_value(?g)

      :ok = Router.put(FerricStore.Instance.get(:default), key, value, 0)
      assert_cold_key(key)

      assert {:ok, ^value} = FerricStore.getset(key, "replacement")
      assert {:ok, "replacement"} = FerricStore.get(key)
    end

    test "GETDEL returns and deletes an existing cold large value" do
      key = "alv_test:getdel_cold"
      value = cold_value(?d)

      :ok = Router.put(FerricStore.Instance.get(:default), key, value, 0)
      assert_cold_key(key)

      assert {:ok, ^value} = FerricStore.getdel(key)
      assert {:ok, nil} = FerricStore.get(key)
    end

    test "GETEX returns and updates expiry on an existing cold large value" do
      key = "alv_test:getex_cold"
      value = cold_value(?e)

      :ok = Router.put(FerricStore.Instance.get(:default), key, value, 0)
      assert_cold_key(key)

      assert {:ok, ^value} = FerricStore.getex(key, ttl: 60_000)
      assert {:ok, ttl} = FerricStore.ttl(key)
      assert ttl > 0
    end

    test "SETRANGE overlays an existing cold large value" do
      key = "alv_test:setrange_cold"
      prefix = cold_value(?s)
      suffix = :binary.copy("t", 1024)
      value = prefix <> suffix
      expected_size = byte_size(value)

      :ok = Router.put(FerricStore.Instance.get(:default), key, value, 0)
      assert_cold_key(key)

      assert {:ok, ^expected_size} = FerricStore.setrange(key, byte_size(prefix), "PATCH")
      assert {:ok, updated} = FerricStore.get(key)
      assert binary_part(updated, 0, byte_size(prefix)) == prefix
      assert binary_part(updated, byte_size(prefix), 5) == "PATCH"
      assert byte_size(updated) == byte_size(value)
    end

    test "get_with_file_ref returns value offset for cold sendfile reads" do
      key = "alv_test:file_ref"
      value = :binary.copy("F", 100_000)
      ctx = FerricStore.Instance.get(:default)
      idx = Router.shard_for(ctx, key)
      original = Ferricstore.Store.ActiveFile.get(idx)
      {_file_id, _file_path, shard_data_path} = original
      file_id = 1
      file_path = Path.join(shard_data_path, "00001.log")

      Ferricstore.Store.ActiveFile.publish(idx, file_id, file_path, shard_data_path)

      try do
        :ok = Router.batch_async_put(ctx, [{key, value}])

        assert {:cold_ref, path, offset, size} = Router.get_with_file_ref(ctx, key)

        assert size == byte_size(value)
        assert {:ok, file} = :file.open(String.to_charlist(path), [:read, :binary])

        try do
          assert {:ok, ^value} = :file.pread(file, offset, size)
        after
          :file.close(file)
        end
      after
        {original_file_id, original_file_path, original_shard_data_path} = original

        Ferricstore.Store.ActiveFile.publish(
          idx,
          original_file_id,
          original_file_path,
          original_shard_data_path
        )
      end
    end

    test "get_with_file_ref treats file id zero as a valid cold file ref" do
      key = "alv_test:file_ref_zero"
      value = :binary.copy("Z", 100_000)
      ctx = FerricStore.Instance.get(:default)
      idx = Router.shard_for(ctx, key)
      original = Ferricstore.Store.ActiveFile.get(idx)
      {_file_id, _file_path, shard_data_path} = original
      file_id = 0
      file_path = Path.join(shard_data_path, "00000.log")

      Ferricstore.Store.ActiveFile.publish(idx, file_id, file_path, shard_data_path)

      try do
        :ok = Router.batch_async_put(ctx, [{key, value}])

        assert {:cold_ref, path, offset, size} = Router.get_with_file_ref(ctx, key)

        assert size == byte_size(value)
        assert {:ok, file} = :file.open(String.to_charlist(path), [:read, :binary])

        try do
          assert {:ok, ^value} = :file.pread(file, offset, size)
        after
          :file.close(file)
        end
      after
        {original_file_id, original_file_path, original_shard_data_path} = original

        Ferricstore.Store.ActiveFile.publish(
          idx,
          original_file_id,
          original_file_path,
          original_shard_data_path
        )
      end
    end
  end
end
