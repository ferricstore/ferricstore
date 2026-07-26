defmodule Ferricstore.Flow.LMDBFlushCoordinatorTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.LMDBFlushCoordinator

  test "shard permits wait for their writer without blocking other shards" do
    instance_name = unique_instance_name("exclusive")

    start_supervised!({LMDBFlushCoordinator, instance_name: instance_name, max_concurrent: 2})

    parent = self()

    holder =
      Task.async(fn ->
        LMDBFlushCoordinator.with_shard_permit(instance_name, 0, fn ->
          send(parent, :holder_acquired)

          receive do
            :release_holder -> :ok
          end
        end)
      end)

    assert_receive :holder_acquired

    exclusive =
      Task.async(fn ->
        LMDBFlushCoordinator.with_shard_permit(instance_name, 0, fn ->
          send(parent, :exclusive_acquired)

          receive do
            :release_exclusive -> :ok
          end
        end)
      end)

    ordinary =
      Task.async(fn ->
        LMDBFlushCoordinator.with_shard_permit(instance_name, 1, fn ->
          send(parent, :ordinary_acquired)
        end)
      end)

    refute_receive :exclusive_acquired, 50
    assert_receive :ordinary_acquired
    assert :ordinary_acquired = Task.await(ordinary)

    send(holder.pid, :release_holder)
    assert :ok = Task.await(holder)
    assert_receive :exclusive_acquired

    send(exclusive.pid, :release_exclusive)
    assert :ok = Task.await(exclusive)
  end

  test "a dead holder releases its shard scope and global permit" do
    instance_name = unique_instance_name("holder_down")

    start_supervised!({LMDBFlushCoordinator, instance_name: instance_name, max_concurrent: 1})

    parent = self()

    holder =
      spawn(fn ->
        LMDBFlushCoordinator.with_shard_permit(instance_name, 0, fn ->
          send(parent, :dead_holder_acquired)
          Process.sleep(:infinity)
        end)
      end)

    assert_receive :dead_holder_acquired

    waiter =
      Task.async(fn ->
        LMDBFlushCoordinator.with_shard_permit(instance_name, 0, fn ->
          send(parent, :waiter_acquired_after_down)
        end)
      end)

    refute_receive :waiter_acquired_after_down, 50
    Process.exit(holder, :kill)

    assert_receive :waiter_acquired_after_down
    assert :waiter_acquired_after_down = Task.await(waiter)
  end

  test "a missing coordinator fails closed without executing the mutation" do
    refute_received :unserialized_mutation

    assert {:error, :lmdb_flush_coordinator_unavailable} =
             LMDBFlushCoordinator.__with_acquired_permit_for_test__(
               :missing,
               {:shard, :missing, 0},
               fn -> send(self(), :unserialized_mutation) end,
               fn _instance_name -> nil end,
               fn _pid, _scope -> flunk("acquire must not run without a coordinator") end
             )

    refute_received :unserialized_mutation
  end

  test "a failed acquisition fails closed without executing the mutation" do
    coordinator = self()

    assert {:error, :lmdb_flush_coordinator_unavailable} =
             LMDBFlushCoordinator.__with_acquired_permit_for_test__(
               :failed_acquire,
               {:shard, :failed_acquire, 0},
               fn -> send(self(), :unserialized_mutation) end,
               fn _instance_name -> coordinator end,
               fn ^coordinator, _scope -> :unavailable end
             )

    refute_received :unserialized_mutation
  end

  defp unique_instance_name(suffix) do
    String.to_atom(
      "lmdb_flush_coordinator_#{suffix}_#{System.unique_integer([:positive, :monotonic])}"
    )
  end
end
