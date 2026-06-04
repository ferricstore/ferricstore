defmodule Ferricstore.Merge.SupervisorIntegrationTest do
  @moduledoc """
  Integration tests for the MergeSupervisor being started in the application
  supervision tree.

  Verifies that the merge subsystem (Semaphore + per-shard Schedulers) is
  started automatically alongside the rest of the application and that all
  processes are alive and responsive.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Merge.{Scheduler, Semaphore}

  defp shard_count, do: :persistent_term.get(:ferricstore_shard_count, 4)

  # -------------------------------------------------------------------
  # 1. MergeSupervisor is alive after app start
  # -------------------------------------------------------------------

  describe "MergeSupervisor in application tree" do
    test "MergeSupervisor is alive after application start" do
      pid = Process.whereis(Ferricstore.Merge.Supervisor)
      assert is_pid(pid), "Expected Ferricstore.Merge.Supervisor to be registered"
      assert Process.alive?(pid), "Expected Ferricstore.Merge.Supervisor to be alive"
    end

    test "MergeSupervisor is a direct child of Ferricstore.Supervisor" do
      children = Supervisor.which_children(Ferricstore.Supervisor)
      ids = Enum.map(children, fn {id, _pid, _type, _mods} -> id end)

      assert Ferricstore.Merge.Supervisor in ids,
             "Expected Ferricstore.Merge.Supervisor in top-level children, got: #{inspect(ids)}"
    end

    test "MergeSupervisor is a supervisor type" do
      children = Supervisor.which_children(Ferricstore.Supervisor)

      merge_child =
        Enum.find(children, fn {id, _, _, _} -> id == Ferricstore.Merge.Supervisor end)

      assert merge_child != nil
      {_, _pid, type, _mods} = merge_child
      assert type == :supervisor
    end
  end

  # -------------------------------------------------------------------
  # 2. Scheduler processes exist for each shard
  # -------------------------------------------------------------------

  describe "per-shard Scheduler processes" do
    test "one Scheduler process exists for each shard" do
      for i <- 0..(shard_count() - 1) do
        name = Scheduler.scheduler_name(i)
        pid = Process.whereis(name)

        assert is_pid(pid),
               "Expected Scheduler #{name} to be registered (shard #{i})"

        assert Process.alive?(pid),
               "Expected Scheduler #{name} to be alive (shard #{i})"
      end
    end

    test "MergeSupervisor has exactly shard_count + 1 children (Semaphore + N Schedulers)" do
      children = Supervisor.which_children(Ferricstore.Merge.Supervisor)

      expected_count = shard_count() + 1

      assert length(children) == expected_count,
             "Expected #{expected_count} children (1 Semaphore + #{shard_count()} Schedulers), " <>
               "got #{length(children)}: #{inspect(Enum.map(children, fn {id, _, _, _} -> id end))}"
    end

    test "Semaphore process is alive under MergeSupervisor" do
      pid = Process.whereis(Ferricstore.Merge.Semaphore)

      assert is_pid(pid),
             "Expected Ferricstore.Merge.Semaphore to be registered"

      assert Process.alive?(pid),
             "Expected Ferricstore.Merge.Semaphore to be alive"
    end
  end

  # -------------------------------------------------------------------
  # 3. Scheduler responds to status queries
  # -------------------------------------------------------------------

  describe "Scheduler status queries" do
    test "each Scheduler responds to status with correct shard_index" do
      for i <- 0..(shard_count() - 1) do
        status = Scheduler.status(i)

        assert is_map(status), "Expected status to be a map for shard #{i}"
        assert status.shard_index == i, "Expected shard_index #{i}, got #{status.shard_index}"
        assert status.merging == false, "Expected merging to be false on shard #{i}"
        assert is_integer(status.merge_count), "Expected merge_count to be integer on shard #{i}"

        assert is_integer(status.total_bytes_reclaimed),
               "Expected total_bytes_reclaimed to be integer on shard #{i}"

        assert status.total_bytes_reclaimed >= 0,
               "Expected total_bytes_reclaimed to be non-negative on shard #{i}"

        assert is_map(status.config), "Expected config to be a map on shard #{i}"
      end
    end

    test "Semaphore reports :free status on fresh start" do
      assert :free = Semaphore.status()
    end
  end

  # -------------------------------------------------------------------
  # 4. Triggering a merge check doesn't crash
  # -------------------------------------------------------------------

  describe "trigger_check resilience" do
    test "trigger_check on each shard returns :ok without crashing" do
      for i <- 0..(shard_count() - 1) do
        assert :ok = Scheduler.trigger_check(i),
               "Expected trigger_check to return :ok for shard #{i}"

        # Verify the scheduler is still alive after the check.
        name = Scheduler.scheduler_name(i)
        pid = Process.whereis(name)
        assert is_pid(pid) and Process.alive?(pid),
               "Scheduler #{name} should still be alive after trigger_check"
      end
    end

    test "trigger_check does not leave Semaphore in held state when fragmentation is low" do
      for i <- 0..(shard_count() - 1) do
        :ok = Scheduler.trigger_check(i)
      end

      # After all checks complete, semaphore should be free (no actual merges
      # should have been triggered because the test env has low fragmentation).
      assert :free = Semaphore.status(),
             "Semaphore should be free after trigger_check on all shards"
    end
  end
end
