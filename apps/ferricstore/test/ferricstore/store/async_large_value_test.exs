defmodule Ferricstore.Store.AsyncLargeValueTest do
  use ExUnit.Case, async: false
  alias Ferricstore.Store.Router

  setup do
    Ferricstore.NamespaceConfig.set("alv_test", "durability", "async")
    Ferricstore.Test.ShardHelpers.flush_all_keys()

    on_exit(fn ->
      Ferricstore.NamespaceConfig.set("alv_test", "durability", "quorum")
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

  describe "async write with large values (>64KB)" do
    test "large value is readable immediately after write" do
      big_value = :binary.copy("x", 100_000)
      :ok = Router.put(FerricStore.Instance.get(:default), "alv_test:big1", big_value, 0)

      eventually(
        fn -> Router.get(FerricStore.Instance.get(:default), "alv_test:big1") == big_value end,
        "large value should be readable after async flush"
      )
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
  end
end
