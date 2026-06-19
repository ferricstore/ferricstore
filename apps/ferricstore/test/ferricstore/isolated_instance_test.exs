defmodule Ferricstore.IsolatedInstanceTest do
  @moduledoc """
  Tests proving that isolated instances provide true test isolation.
  Each test gets its own FerricStore instance — no shared state.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Test.IsolatedInstance

  describe "instance isolation" do
    test "two instances don't see each other's keys" do
      ctx_a = IsolatedInstance.checkout()
      ctx_b = IsolatedInstance.checkout()

      FerricStore.Impl.set(ctx_a, "shared_name", "from_a")
      FerricStore.Impl.set(ctx_b, "shared_name", "from_b")

      assert {:ok, "from_a"} = FerricStore.Impl.get(ctx_a, "shared_name")
      assert {:ok, "from_b"} = FerricStore.Impl.get(ctx_b, "shared_name")

      IsolatedInstance.checkin(ctx_a)
      IsolatedInstance.checkin(ctx_b)
    end

    test "instance is empty on checkout" do
      ctx = IsolatedInstance.checkout()

      assert {:ok, nil} = FerricStore.Impl.get(ctx, "any_key")
      assert {:ok, keys} = FerricStore.Impl.keys(ctx)
      assert keys == []

      IsolatedInstance.checkin(ctx)
    end

    test "checkin cleans up everything" do
      ctx = IsolatedInstance.checkout()
      name = ctx.name
      data_dir = ctx.data_dir

      FerricStore.Impl.set(ctx, "cleanup_test", "value")
      IsolatedInstance.checkin(ctx)

      # Persistent term should be cleaned up
      assert_raise ArgumentError, fn ->
        FerricStore.Instance.get(name)
      end

      # Data dir should be removed
      refute File.exists?(data_dir)
    end

    test "hash operations in isolated instance" do
      ctx = IsolatedInstance.checkout()

      FerricStore.Impl.hset(ctx, "my_hash", %{"field1" => "val1", "field2" => "val2"})
      assert {:ok, "val1"} = FerricStore.Impl.hget(ctx, "my_hash", "field1")
      assert {:ok, map} = FerricStore.Impl.hgetall(ctx, "my_hash")
      assert map == %{"field1" => "val1", "field2" => "val2"}

      IsolatedInstance.checkin(ctx)
    end

    test "bloom filter in isolated instance" do
      ctx = IsolatedInstance.checkout()

      assert :ok = FerricStore.Impl.bf_reserve(ctx, "iso_bloom", 0.01, 100)
      assert {:ok, 1} = FerricStore.Impl.bf_add(ctx, "iso_bloom", "hello")
      assert {:ok, 1} = FerricStore.Impl.bf_exists(ctx, "iso_bloom", "hello")
      assert {:ok, 0} = FerricStore.Impl.bf_exists(ctx, "iso_bloom", "missing")

      IsolatedInstance.checkin(ctx)
    end

    @tag timeout: 120_000
    test "concurrent isolated instances do not interfere" do
      # Keep the module non-async so CI resource pressure from unrelated test
      # modules does not starve the isolated instance supervisors. The test
      # still proves concurrent isolated instances by running multiple checked
      # out instances at the same time.
      tasks =
        for i <- 1..5 do
          Task.async(fn ->
            ctx = IsolatedInstance.checkout()

            FerricStore.Impl.set(ctx, "key", "value_#{i}")
            Process.sleep(10)
            {:ok, val} = FerricStore.Impl.get(ctx, "key")
            assert val == "value_#{i}"

            IsolatedInstance.checkin(ctx)
            :ok
          end)
        end

      results = Task.await_many(tasks, 120_000)
      assert Enum.all?(results, &(&1 == :ok))
    end
  end
end
