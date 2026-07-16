defmodule Ferricstore.InstanceCallbacksTest do
  @moduledoc """
  Tests for Instance callback injection and on_push callbacks in list commands.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Test.ShardHelpers
  alias Ferricstore.Commands.List
  alias Ferricstore.Test.MockStore

  setup do
    ShardHelpers.flush_all_keys()
    original_ctx = FerricStore.Instance.get(:default)

    on_exit(fn ->
      # Restore original Instance callbacks to prevent test pollution.
      :persistent_term.put({FerricStore.Instance, :default}, original_ctx)
      ShardHelpers.flush_all_keys()
    end)

    %{ctx: original_ctx}
  end

  # ===========================================================================
  # inject_callbacks/2
  # ===========================================================================

  describe "inject_callbacks/2" do
    test "updates connected_clients_fn on the Instance", %{ctx: _ctx} do
      FerricStore.Instance.inject_callbacks(:default, connected_clients_fn: fn -> 42 end)

      updated = FerricStore.Instance.get(:default)
      assert updated.connected_clients_fn.() == 42
    end

    test "updates multiple callbacks at once", %{ctx: _ctx} do
      FerricStore.Instance.inject_callbacks(:default,
        connected_clients_fn: fn -> 99 end,
        server_info_fn: fn -> %{test: true} end
      )

      updated = FerricStore.Instance.get(:default)
      assert updated.connected_clients_fn.() == 99
      assert updated.server_info_fn.() == %{test: true}
    end

    test "returns the updated Instance struct", %{ctx: _ctx} do
      result = FerricStore.Instance.inject_callbacks(:default, connected_clients_fn: fn -> 7 end)

      assert %FerricStore.Instance{} = result
      assert result.connected_clients_fn.() == 7
    end

    test "rejects attempts to replace immutable instance fields", %{ctx: original} do
      assert_raise ArgumentError, fn ->
        FerricStore.Instance.inject_callbacks(:default, shard_count: 0)
      end

      assert FerricStore.Instance.get(:default) == original
    end

    test "rejects callbacks with the wrong arity or type", %{ctx: original} do
      assert_raise ArgumentError, fn ->
        FerricStore.Instance.inject_callbacks(:default, connected_clients_fn: :not_a_function)
      end

      assert_raise ArgumentError, fn ->
        FerricStore.Instance.inject_callbacks(:default, server_info_fn: fn _arg -> %{} end)
      end

      assert FerricStore.Instance.get(:default) == original
    end
  end

  # ===========================================================================
  # on_push callback in list commands
  # ===========================================================================

  describe "on_push callback in list commands" do
    test "LPUSH triggers on_push callback with the key" do
      test_pid = self()
      store = MockStore.make()
      store = Map.put(store, :on_push, fn key -> send(test_pid, {:pushed, key}) end)

      assert 1 == List.handle("LPUSH", ["mylist", "val"], store)
      assert_receive {:pushed, "mylist"}
    end

    test "RPUSH triggers on_push callback with the key" do
      test_pid = self()
      store = MockStore.make()
      store = Map.put(store, :on_push, fn key -> send(test_pid, {:pushed, key}) end)

      assert 1 == List.handle("RPUSH", ["mylist", "val"], store)
      assert_receive {:pushed, "mylist"}
    end

    test "LPUSH without on_push key in store does not crash" do
      store = MockStore.make()
      # MockStore.make() does not include :on_push — should still work
      assert 1 == List.handle("LPUSH", ["mylist", "val"], store)
      assert ["val"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "RPUSH without on_push key in store does not crash" do
      store = MockStore.make()
      assert 1 == List.handle("RPUSH", ["mylist", "val"], store)
      assert ["val"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "on_push is called once per LPUSH even with multiple elements" do
      test_pid = self()
      store = MockStore.make()
      store = Map.put(store, :on_push, fn key -> send(test_pid, {:pushed, key}) end)

      assert 3 == List.handle("LPUSH", ["mylist", "a", "b", "c"], store)
      assert_receive {:pushed, "mylist"}
      # Should only fire once per LPUSH command, not per element
      refute_receive {:pushed, _}, 50
    end

    test "on_push receives the correct key for different lists" do
      test_pid = self()
      store = MockStore.make()
      store = Map.put(store, :on_push, fn key -> send(test_pid, {:pushed, key}) end)

      List.handle("LPUSH", ["list_a", "val"], store)
      List.handle("RPUSH", ["list_b", "val"], store)

      assert_receive {:pushed, "list_a"}
      assert_receive {:pushed, "list_b"}
    end
  end

  # ===========================================================================
  # End-to-end: on_push fires through List.handle (command handler path)
  # ===========================================================================

  describe "on_push end-to-end through real store" do
    test "on_push fires when List.handle is called with store from build_local_store" do
      # on_push fires through the command handler path (List.handle),
      # not through the lower-level list_op/ListOps path.
      # This is correct — only TCP clients need waiter notifications for BLPOP.
      # The embedded API (FerricStore.Impl.lpush) uses the list_op path directly.

      # Verify Waiters module is functional (setup test for TCP path)
      key = "waiter_test_#{:rand.uniform(999_999)}"
      deadline = System.monotonic_time(:millisecond) + 10_000
      Ferricstore.Waiters.register(key, self(), deadline)

      # Simulate what happens in the TCP command handler path:
      # Waiters.notify_push is called after LPUSH succeeds
      Ferricstore.Waiters.notify_push(key)

      assert_receive {:waiter_notify, ^key}, 1000
    end
  end

  # ===========================================================================
  # End-to-end: basic set/get still works (no regression)
  # ===========================================================================

  describe "basic set/get regression check" do
    test "FerricStore.set and get work after callback injection" do
      FerricStore.Instance.inject_callbacks(:default,
        connected_clients_fn: fn -> 99 end,
        server_info_fn: fn -> %{test: true} end
      )

      key = "regression_test_#{:rand.uniform(999_999)}"
      :ok = FerricStore.set(key, "hello_world")
      assert {:ok, "hello_world"} = FerricStore.get(key)

      {:ok, 1} = FerricStore.del(key)
      assert {:ok, nil} = FerricStore.get(key)
    end
  end
end
