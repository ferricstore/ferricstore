defmodule Ferricstore.Flow.LMDBFlushCoordinatorTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.LMDBFlushCoordinator

  test "memory-budgeted default admits two independent shard rebuilds" do
    original_limit = Application.get_env(:ferricstore, :operational_memory_limit_bytes)
    original_concurrency = Application.get_env(:ferricstore, :flow_lmdb_max_concurrent_flushes)
    Application.put_env(:ferricstore, :operational_memory_limit_bytes, 5 * 1024 * 1024 * 1024)
    Application.delete_env(:ferricstore, :flow_lmdb_max_concurrent_flushes)

    on_exit(fn ->
      if original_limit == nil,
        do: Application.delete_env(:ferricstore, :operational_memory_limit_bytes),
        else: Application.put_env(:ferricstore, :operational_memory_limit_bytes, original_limit)

      if original_concurrency == nil,
        do: Application.delete_env(:ferricstore, :flow_lmdb_max_concurrent_flushes),
        else:
          Application.put_env(
            :ferricstore,
            :flow_lmdb_max_concurrent_flushes,
            original_concurrency
          )
    end)

    instance = unique_instance_name("memory_budgeted_default")

    start_supervised!(
      {LMDBFlushCoordinator, instance_name: instance, startup_fun: fn -> true end}
    )

    parent = self()

    first =
      Task.async(fn ->
        LMDBFlushCoordinator.with_shard_permit(instance, 0, fn ->
          send(parent, :first_rebuild_started)
          receive do: (:release_first -> :ok)
        end)
      end)

    assert_receive :first_rebuild_started

    second =
      Task.async(fn ->
        LMDBFlushCoordinator.with_shard_permit(instance, 1, fn ->
          send(parent, :second_rebuild_started)
          receive do: (:release_second -> :ok)
        end)
      end)

    try do
      assert_receive :second_rebuild_started, 500
    after
      send(first.pid, :release_first)
      send(second.pid, :release_second)
      Task.await(first)
      Task.await(second)
    end
  end

  test "a small memory budget keeps the default serial" do
    original_limit = Application.get_env(:ferricstore, :operational_memory_limit_bytes)
    original_concurrency = Application.get_env(:ferricstore, :flow_lmdb_max_concurrent_flushes)
    Application.put_env(:ferricstore, :operational_memory_limit_bytes, 2 * 1024 * 1024 * 1024)
    Application.delete_env(:ferricstore, :flow_lmdb_max_concurrent_flushes)

    on_exit(fn ->
      if original_limit == nil,
        do: Application.delete_env(:ferricstore, :operational_memory_limit_bytes),
        else: Application.put_env(:ferricstore, :operational_memory_limit_bytes, original_limit)

      if original_concurrency == nil,
        do: Application.delete_env(:ferricstore, :flow_lmdb_max_concurrent_flushes),
        else:
          Application.put_env(
            :ferricstore,
            :flow_lmdb_max_concurrent_flushes,
            original_concurrency
          )
    end)

    instance = unique_instance_name("small_memory")
    pid = start_supervised!({LMDBFlushCoordinator, instance_name: instance})
    assert :sys.get_state(pid).max == 1
  end

  test "adaptive startup parallelism returns to serial when the backend is ready" do
    original_limit = Application.get_env(:ferricstore, :operational_memory_limit_bytes)
    original_concurrency = Application.get_env(:ferricstore, :flow_lmdb_max_concurrent_flushes)
    Application.put_env(:ferricstore, :operational_memory_limit_bytes, 8 * 1024 * 1024 * 1024)
    Application.delete_env(:ferricstore, :flow_lmdb_max_concurrent_flushes)

    on_exit(fn ->
      if original_limit == nil,
        do: Application.delete_env(:ferricstore, :operational_memory_limit_bytes),
        else: Application.put_env(:ferricstore, :operational_memory_limit_bytes, original_limit)

      if original_concurrency == nil,
        do: Application.delete_env(:ferricstore, :flow_lmdb_max_concurrent_flushes),
        else:
          Application.put_env(
            :ferricstore,
            :flow_lmdb_max_concurrent_flushes,
            original_concurrency
          )
    end)

    {:ok, startup} = Agent.start_link(fn -> true end)
    on_exit(fn -> if Process.alive?(startup), do: Agent.stop(startup) end)

    instance = unique_instance_name("adaptive_startup")

    start_supervised!(
      {LMDBFlushCoordinator,
       instance_name: instance, startup_fun: fn -> Agent.get(startup, & &1) end}
    )

    parent = self()

    holder = fn shard ->
      Task.async(fn ->
        LMDBFlushCoordinator.with_shard_permit(instance, shard, fn ->
          send(parent, {:acquired, shard})
          receive do: (:release -> :ok)
        end)
      end)
    end

    first = holder.(0)
    second = holder.(1)
    assert_receive {:acquired, 0}
    assert_receive {:acquired, 1}

    Agent.update(startup, fn _ -> false end)
    third = holder.(2)
    send(first.pid, :release)

    try do
      assert :ok = Task.await(first)
      refute_receive {:acquired, 2}, 100
      send(second.pid, :release)
      assert :ok = Task.await(second)
      assert_receive {:acquired, 2}
    after
      send(first.pid, :release)
      send(second.pid, :release)
      send(third.pid, :release)
    end

    assert :ok = Task.await(third)
  end

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
