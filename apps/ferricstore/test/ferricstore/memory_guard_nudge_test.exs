defmodule Ferricstore.MemoryGuardNudgeTest do
  @moduledoc """
  Tests for `MemoryGuard.nudge/0` — the async eviction trigger fired
  when a write is rejected due to keydir pressure.

  Covers: nudge triggers eviction, nudge is idempotent under storm,
  nudge updates persistent_term flags, nudge from router write path,
  nudge from shard write path, nudge when MemoryGuard is busy.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.MemoryGuard
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()

    original_state = :sys.get_state(MemoryGuard)
    original_ctx = FerricStore.Instance.get(:default)
    orig_reject = MemoryGuard.reject_writes?()
    orig_keydir = MemoryGuard.keydir_full?()

    # This file tests keydir/flag behavior. In the full umbrella suite the
    # server app may have injected a real RSS callback into the default
    # instance, which makes these unit assertions depend on host memory.
    FerricStore.Instance.inject_callbacks(:default, process_rss_fn: nil)

    on_exit(fn ->
      # Resume first in case a test suspended MemoryGuard
      try do
        :sys.resume(MemoryGuard)
      catch
        :exit, _ -> :ok
      end

      :sys.replace_state(MemoryGuard, fn _current -> original_state end)
      :persistent_term.put({FerricStore.Instance, :default}, original_ctx)
      MemoryGuard.set_reject_writes(orig_reject)
      MemoryGuard.set_keydir_full(orig_keydir)
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp force_reject_mode do
    :sys.replace_state(MemoryGuard, fn state ->
      %{state | last_pressure_level: :reject, eviction_policy: :noeviction}
    end)

    MemoryGuard.set_reject_writes(true)
    MemoryGuard.set_keydir_full(true)
  end

  # ---------------------------------------------------------------------------
  # nudge/0 unit tests
  # ---------------------------------------------------------------------------

  describe "nudge/0 basic behavior" do
    test "nudge returns :ok immediately (non-blocking)" do
      assert :ok = MemoryGuard.nudge()
    end

    test "nudge triggers a check cycle that emits telemetry" do
      test_pid = self()
      handler_id = "nudge-check-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:ferricstore, :memory, :check],
        fn _event, measurements, metadata, config ->
          send(config.test_pid, {:check_emitted, measurements, metadata})
        end,
        %{test_pid: test_pid}
      )

      MemoryGuard.nudge()

      assert_receive {:check_emitted, measurements, metadata}, 1000
      assert is_integer(measurements.total_bytes)
      assert metadata.pressure_level in [:ok, :warning, :pressure, :reject]

      :telemetry.detach(handler_id)
    end

    test "nudge updates persistent_term flags" do
      # Start with forced reject state
      force_reject_mode()
      assert MemoryGuard.reject_writes?() == true
      assert MemoryGuard.keydir_full?() == true

      # Give this unit assertion a deliberately huge budget so a full-suite
      # run cannot make it depend on allocator/RSS state left by earlier tests.
      MemoryGuard.reconfigure(%{
        max_memory_bytes: 1_000_000_000_000,
        keydir_max_ram: 1_000_000_000_000,
        eviction_policy: :volatile_lru
      })

      # Reset the GenServer state back to ok (but leave persistent_term stale).
      :sys.replace_state(MemoryGuard, fn state ->
        %{state | last_pressure_level: :ok, eviction_policy: :volatile_lru}
      end)

      # nudge should recalculate and update persistent_term based on actual ETS memory
      MemoryGuard.nudge()

      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          MemoryGuard.keydir_full?() == false and MemoryGuard.reject_writes?() == false
        end,
        "nudge did not clear persistent_term flags",
        20,
        10
      )

      # Under normal test conditions, actual memory is well below thresholds
      assert MemoryGuard.keydir_full?() == false
      assert MemoryGuard.reject_writes?() == false
    end

    test "nudge is idempotent — multiple rapid nudges don't crash" do
      # Fire 100 nudges rapidly (simulates storm of rejected writes)
      for _ <- 1..100 do
        assert :ok = MemoryGuard.nudge()
      end

      # MemoryGuard should still be alive and responsive
      assert Process.alive?(Process.whereis(MemoryGuard))
      assert is_map(MemoryGuard.stats())
    end

    test "nudge under concurrent load from multiple processes" do
      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            MemoryGuard.nudge()
          end)
        end

      results = Task.await_many(tasks, 5000)
      assert Enum.all?(results, &(&1 == :ok))

      # MemoryGuard survives the storm
      assert Process.alive?(Process.whereis(MemoryGuard))
      stats = MemoryGuard.stats()
      assert stats.pressure_level in [:ok, :warning, :pressure, :reject]
    end
  end

  # ---------------------------------------------------------------------------
  # nudge triggered from write rejection path
  # ---------------------------------------------------------------------------

  describe "nudge triggered by Router.put rejection" do
    test "rejected new-key write nudges MemoryGuard" do
      test_pid = self()
      handler_id = "nudge-router-#{System.unique_integer([:positive])}"

      # Attach telemetry to detect the check cycle triggered by nudge
      :telemetry.attach(
        handler_id,
        [:ferricstore, :memory, :check],
        fn _event, _measurements, _metadata, config ->
          send(config.test_pid, :nudge_check_fired)
        end,
        %{test_pid: test_pid}
      )

      force_reject_mode()

      # Suspend MemoryGuard to prevent the periodic timer from clearing
      # the reject flags before Router.put checks them.
      :sys.suspend(MemoryGuard)

      # This put should be rejected AND trigger a nudge
      result = Router.put(FerricStore.Instance.get(:default), "nudge_test_new_key", "value", 0)
      assert {:error, msg} = result
      assert msg =~ "KEYDIR_FULL"

      # Resume so the nudge (async cast) can be processed
      :sys.resume(MemoryGuard)

      # The nudge should have triggered a check cycle
      assert_receive :nudge_check_fired, 1000

      :telemetry.detach(handler_id)
    end

    test "existing key update does NOT nudge (no rejection)" do
      # Write key before pressure
      assert :ok = Router.put(FerricStore.Instance.get(:default), "pre_existing", "old", 0)

      test_pid = self()
      handler_id = "nudge-no-fire-#{System.unique_integer([:positive])}"

      force_reject_mode()

      # Suspend MemoryGuard to stop its periodic timer from emitting
      # telemetry that would confuse our refute_receive assertion.
      # The atomics flags are already set by force_reject_mode — they
      # don't require the GenServer to be running.
      :sys.suspend(MemoryGuard)

      :telemetry.attach(
        handler_id,
        [:ferricstore, :memory, :check],
        fn _event, _measurements, _metadata, config ->
          send(config.test_pid, :unexpected_nudge)
        end,
        %{test_pid: test_pid}
      )

      # Update existing key — should succeed, no nudge
      assert :ok = Router.put(FerricStore.Instance.get(:default), "pre_existing", "new", 0)

      # Should NOT receive a nudge-triggered check. MemoryGuard is
      # suspended so periodic timer can't fire — any message here
      # would prove the Router path incorrectly nudges on existing keys.
      refute_receive :unexpected_nudge, 100

      :telemetry.detach(handler_id)
      :sys.resume(MemoryGuard)
    end

    test "after nudge + eviction, next write may succeed" do
      # Write some keys with TTL that are evictable under volatile_lru
      for i <- 1..10 do
        expire = System.os_time(:millisecond) + 60_000
        Router.put(FerricStore.Instance.get(:default), "evictable_#{i}", "v", expire)
      end

      # Switch to reject with eviction enabled (volatile_lru, not noeviction).
      # But set keydir_full so check_keydir_full rejects new keys.
      MemoryGuard.reconfigure(%{
        max_memory_bytes: 1_000_000_000_000,
        keydir_max_ram: 1_000_000_000_000,
        eviction_policy: :volatile_lru
      })

      :sys.replace_state(MemoryGuard, fn state ->
        %{state | last_pressure_level: :reject, eviction_policy: :volatile_lru}
      end)

      MemoryGuard.set_keydir_full(true)
      MemoryGuard.set_reject_writes(false)

      # New key rejected via keydir_full check
      result = Router.put(FerricStore.Instance.get(:default), "blocked_key", "v", 0)
      assert {:error, msg} = result
      assert msg =~ "KEYDIR_FULL"

      # nudge fires → perform_check recalculates from actual ETS memory
      # Under normal test conditions, actual pressure is :ok
      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          MemoryGuard.keydir_full?() == false
        end,
        "nudge did not clear keydir_full",
        20,
        10
      )

      # persistent_term should be updated by the nudge check
      # (actual ETS memory is well below the keydir_max_ram threshold)
      assert MemoryGuard.keydir_full?() == false

      # Now the write should succeed
      assert :ok = Router.put(FerricStore.Instance.get(:default), "blocked_key", "v", 0)
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases
  # ---------------------------------------------------------------------------

  describe "edge cases" do
    test "nudge when MemoryGuard has tiny budget (always reject)" do
      # Set a 1-byte budget so every check lands on :reject
      MemoryGuard.reconfigure(%{max_memory_bytes: 1, keydir_max_ram: 1})

      MemoryGuard.nudge()

      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          MemoryGuard.stats().pressure_level == :reject
        end,
        "nudge did not reach reject pressure",
        20,
        10
      )

      # Should be in reject state after nudge recalculates
      stats = MemoryGuard.stats()
      assert stats.pressure_level == :reject
    end

    test "nudge when MemoryGuard has huge budget (always ok)" do
      MemoryGuard.reconfigure(%{
        max_memory_bytes: 100_000_000_000,
        keydir_max_ram: 100_000_000_000
      })

      force_reject_mode()
      assert MemoryGuard.keydir_full?() == true

      MemoryGuard.nudge()

      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          MemoryGuard.keydir_full?() == false and MemoryGuard.reject_writes?() == false
        end,
        "nudge did not clear flags with huge budget",
        20,
        10
      )

      # With a 100GB budget, actual usage is well below thresholds
      assert MemoryGuard.keydir_full?() == false
      assert MemoryGuard.reject_writes?() == false
    end

    test "nudge does not reschedule periodic timer" do
      # The periodic timer should continue independently of nudge calls.
      # Verify by checking that periodic checks still fire after nudges.
      test_pid = self()
      handler_id = "nudge-periodic-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:ferricstore, :memory, :check],
        fn _event, _measurements, _metadata, config ->
          send(config.test_pid, :periodic_check)
        end,
        %{test_pid: test_pid}
      )

      # Fire several nudges
      for _ <- 1..10, do: MemoryGuard.nudge()

      # Periodic checks should still fire (100ms interval)
      # We should see at least 2 checks within 300ms (nudge + periodic)
      assert_receive :periodic_check, 500
      assert_receive :periodic_check, 500

      :telemetry.detach(handler_id)
    end

    test "nudge while force_check is running concurrently" do
      # force_check is a synchronous call, nudge is async cast.
      # They should not deadlock or crash when concurrent.
      task = Task.async(fn -> MemoryGuard.force_check() end)

      for _ <- 1..10 do
        MemoryGuard.nudge()
      end

      assert :ok = Task.await(task, 5000)
      assert Process.alive?(Process.whereis(MemoryGuard))
    end

    test "nudge after reconfigure doesn't use stale budgets" do
      # Reconfigure to tiny budget
      MemoryGuard.reconfigure(%{max_memory_bytes: 1, keydir_max_ram: 1})
      MemoryGuard.nudge()

      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          MemoryGuard.stats().pressure_level == :reject
        end,
        "nudge did not reach reject after tiny budget",
        20,
        10
      )

      stats = MemoryGuard.stats()
      assert stats.pressure_level == :reject

      # Reconfigure to huge budget
      MemoryGuard.reconfigure(%{
        max_memory_bytes: 100_000_000_000,
        keydir_max_ram: 100_000_000_000
      })

      MemoryGuard.nudge()

      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          MemoryGuard.stats().pressure_level == :ok
        end,
        "nudge did not reach ok after huge budget",
        20,
        10
      )

      stats = MemoryGuard.stats()
      assert stats.pressure_level == :ok
    end

    test "rapid rejection storm: nudge doesn't flood MemoryGuard mailbox dangerously" do
      # Use a 1-byte budget so nudge's recalculation still yields :reject
      # (actual ETS memory > 1 byte). This prevents the nudge from clearing
      # the persistent_term flags mid-storm, which would make writes succeed.
      MemoryGuard.reconfigure(%{
        max_memory_bytes: 1,
        keydir_max_ram: 1,
        eviction_policy: :noeviction
      })

      MemoryGuard.force_check()

      assert MemoryGuard.reject_writes?() == true

      # Simulate 500 rejected writes from concurrent clients
      tasks =
        for i <- 1..500 do
          Task.async(fn ->
            Router.put(FerricStore.Instance.get(:default), "storm_key_#{i}", "v", 0)
          end)
        end

      results = Task.await_many(tasks, 10_000)

      # All should be rejected
      assert Enum.all?(results, fn
               {:error, msg} -> String.contains?(msg, "KEYDIR_FULL")
               _ -> false
             end)

      # MemoryGuard should still be alive and responsive despite 500 nudge casts
      assert Process.alive?(Process.whereis(MemoryGuard))

      # It should drain the mailbox and be responsive to a sync call
      assert is_map(MemoryGuard.stats())
    end
  end
end
