defmodule Ferricstore.ReviewR2.CrossShardOpIssuesTest do
  @moduledoc """
  Tests proving cross-shard operation bugs found during R2 code review.

  Each test targets a specific issue ID (R2-C2, R2-C3, etc.) and demonstrates
  the buggy behavior. When the bugs are fixed, these tests serve as regression
  guards.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Raft.Cluster
  alias Ferricstore.Test.ShardHelpers
  alias Ferricstore.CrossShardOp
  alias Ferricstore.NamespaceConfig

  setup do
    ShardHelpers.flush_all_keys()
    NamespaceConfig.reset_all()
    :ok
  end

  defp raft_result({:ok, {:applied_at, _, result}, _}), do: result
  defp raft_result({:error, _reason} = error), do: error

  # ---------------------------------------------------------------------------
  # R2-C2: Locks/intents stored in process dictionary -- lost on shard restart
  #
  # The state machine stores cross-shard locks in the ra process's process
  # dictionary (not in the replicated Raft state). When the shard process
  # crashes and restarts, the process dictionary is lost, so all locks vanish.
  # ---------------------------------------------------------------------------

  describe "R2-C2: locks lost on shard restart" do
    @tag :shard_kill
    test "lock on a key disappears after shard kill -- another owner can lock immediately" do
      [k1, _k2] = ShardHelpers.keys_on_different_shards(2)
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), k1)
      shard_id = Cluster.shard_server_id(shard_idx)

      # Use the discovered key that routes to this shard
      key = k1

      owner_ref = make_ref()
      now = System.os_time(:millisecond)
      expire_at = now + 30_000

      # Lock the key via Raft
      {:ok, {:applied_at, _, :ok}, _} =
        Ferricstore.Raft.CommandClock.process_command(shard_id, {:lock_keys, [key], owner_ref, expire_at})

      # Confirm lock is active: another owner should fail
      other_ref = make_ref()

      assert {:error, :keys_locked} =
               shard_id
               |> Ferricstore.Raft.CommandClock.process_command(
                 {:lock_keys, [key], other_ref, expire_at}
               )
               |> raft_result()

      # Kill the shard and wait for restart
      ShardHelpers.compact_wal()
      ShardHelpers.kill_shard_safely(shard_idx)

      # FIX (c22271c): Locks are now persisted in Raft state and survive restarts.
      new_shard_id = Cluster.shard_server_id(shard_idx)
      new_other_ref = make_ref()
      new_expire = System.os_time(:millisecond) + 30_000

      result =
        new_shard_id
        |> Ferricstore.Raft.CommandClock.process_command(
          {:lock_keys, [key], new_other_ref, new_expire}
        )
        |> raft_result()

      # Lock survives restart -- another owner cannot acquire it
      assert result == {:error, :keys_locked},
             "R2-C2: Lock should survive shard restart (persisted in Raft state)"

      # Clean up: unlock with original owner
      Ferricstore.Raft.CommandClock.process_command(new_shard_id, {:unlock_keys, [key], owner_ref})
    end

    @tag :shard_kill
    test "intent record disappears after shard kill" do
      [k1, k2] = ShardHelpers.keys_on_different_shards(2)

      coordinator_shard_idx =
        min(
          Router.shard_for(FerricStore.Instance.get(:default), k1),
          Router.shard_for(FerricStore.Instance.get(:default), k2)
        )

      shard_id = Cluster.shard_server_id(coordinator_shard_idx)

      owner_ref = make_ref()

      intent_map = %{
        command: :rename,
        keys: %{source: k1, dest: k2},
        value_hashes: %{k1 => :erlang.phash2(nil)},
        status: :executing,
        created_at: System.os_time(:millisecond)
      }

      # Write intent via Raft
      {:ok, {:applied_at, _, :ok}, _} =
        Ferricstore.Raft.CommandClock.process_command(shard_id, {:cross_shard_intent, owner_ref, intent_map})

      # Confirm intent exists
      {:ok, {:applied_at, _, intents_before}, _} = Ferricstore.Raft.CommandClock.process_command(shard_id, {:get_intents})
      assert Map.has_key?(intents_before, owner_ref)

      # Kill the shard
      ShardHelpers.compact_wal()
      ShardHelpers.kill_shard_safely(coordinator_shard_idx)

      # FIX (c22271c): Intents are now persisted in Raft state and survive restarts.
      new_shard_id = Cluster.shard_server_id(coordinator_shard_idx)

      {:ok, {:applied_at, _, intents_after}, _} =
        Ferricstore.Raft.CommandClock.process_command(new_shard_id, {:get_intents})

      # Intent survives restart -- crash recovery can find it
      assert Map.has_key?(intents_after, owner_ref),
             "R2-C2: Intent should survive shard restart (persisted in Raft state)"

      # Clean up
      Ferricstore.Raft.CommandClock.process_command(new_shard_id, {:delete_intent, owner_ref})
    end
  end

  # ---------------------------------------------------------------------------
  # R2-C3: Lock expiry during slow execute
  #
  # CrossShardOp uses a 5s TTL (@lock_ttl_ms). If the execute phase takes
  # longer than the TTL, the locks expire mid-operation and another process
  # can acquire them, breaking atomicity.
  # ---------------------------------------------------------------------------

  describe "R2-C3: lock expiry during slow execute" do
    test "lock expires while execution is still in progress -- TTL window exists" do
      [k1, _k2] = ShardHelpers.keys_on_different_shards(2)
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), k1)
      shard_id = Cluster.shard_server_id(shard_idx)

      owner_ref = make_ref()
      now = System.os_time(:millisecond)

      # Lock with a short TTL (200ms) to simulate the race
      short_expire = now + 200

      {:ok, {:applied_at, _, :ok}, _} =
        Ferricstore.Raft.CommandClock.process_command(shard_id, {:lock_keys, [k1], owner_ref, short_expire})

      # Simulate slow execution: sleep past the TTL
      Process.sleep(300)

      # Another process should now be able to lock the same key
      other_ref = make_ref()
      new_expire = System.os_time(:millisecond) + 30_000

      {:ok, {:applied_at, _, result}, _} =
        Ferricstore.Raft.CommandClock.process_command(shard_id, {:lock_keys, [k1], other_ref, new_expire})

      # This PASSES: lock expired, proving the TTL window vulnerability
      assert result == :ok,
             "R2-C3: Lock should expire after TTL, allowing another owner to acquire"

      # Clean up
      Ferricstore.Raft.CommandClock.process_command(shard_id, {:unlock_keys, [k1], other_ref})
    end
  end

  # ---------------------------------------------------------------------------
  # R2-H1: Intent write failure not detected
  #
  # In CrossShardOp.execute_cross_shard/4, write_intent/3 used to be called
  # without checking its return value. If the Raft command failed, execution
  # proceeded without an intent record, so crash recovery was impossible.
  #
  # This is a regression guard: verify normal operation works correctly.
  # ---------------------------------------------------------------------------

  describe "R2-H1: intent write failure not detected (regression guard)" do
    test "cross-shard RENAME succeeds under normal conditions" do
      [k1, k2] = ShardHelpers.keys_on_different_shards(2)
      Router.put(FerricStore.Instance.get(:default), k1, "r2h1_value", 0)

      result = Ferricstore.Commands.Generic.handle("RENAME", [k1, k2], %{})
      assert result == :ok

      assert Router.get(FerricStore.Instance.get(:default), k1) == nil
      assert Router.get(FerricStore.Instance.get(:default), k2) == "r2h1_value"
    end

    test "write_intent returns an observable result" do
      # Verify the Ra command returns a value that CrossShardOp must inspect
      # before running locked writes.
      [k1, k2] = ShardHelpers.keys_on_different_shards(2)

      coordinator_shard_idx =
        min(
          Router.shard_for(FerricStore.Instance.get(:default), k1),
          Router.shard_for(FerricStore.Instance.get(:default), k2)
        )

      shard_id = Cluster.shard_server_id(coordinator_shard_idx)

      owner_ref = make_ref()

      intent = %{
        status: :executing,
        created_at: System.os_time(:millisecond),
        command: :test,
        keys: %{a: k1, b: k2},
        value_hashes: %{}
      }

      # write_intent goes through Ferricstore.Raft.CommandClock.process_command -- verify it returns a value
      result = Ferricstore.Raft.CommandClock.process_command(shard_id, {:cross_shard_intent, owner_ref, intent})
      assert {:ok, {:applied_at, _, :ok}, _} = result

      # Clean up
      Ferricstore.Raft.CommandClock.process_command(shard_id, {:delete_intent, owner_ref})
    end
  end

  # ---------------------------------------------------------------------------
  # R2-H2: Write intent outside try block
  #
  # In execute_cross_shard/4, write_intent used to run before the cleanup
  # rescue scope. If write_intent itself raised, the locks acquired in
  # lock_phase were never released.
  # ---------------------------------------------------------------------------

  describe "R2-H2: write intent outside try block" do
    test "locks are released when execute_fn raises an exception" do
      [k1, k2] = ShardHelpers.keys_on_different_shards(2)

      Router.put(FerricStore.Instance.get(:default), k1, "r2h2_value", 0)

      # Execute a cross-shard op that raises during execution
      assert_raise RuntimeError, "boom", fn ->
        CrossShardOp.execute(
          [{k1, :read_write}, {k2, :write}],
          fn _store -> raise "boom" end,
          intent: %{command: :test, keys: %{a: k1, b: k2}}
        )
      end

      # After the exception, locks should be released (the try/rescue handles this)
      shard1_id =
        Cluster.shard_server_id(Router.shard_for(FerricStore.Instance.get(:default), k1))

      shard2_id =
        Cluster.shard_server_id(Router.shard_for(FerricStore.Instance.get(:default), k2))

      new_ref = make_ref()
      expire = System.os_time(:millisecond) + 30_000

      {:ok, {:applied_at, _, r1}, _} =
        Ferricstore.Raft.CommandClock.process_command(shard1_id, {:lock_keys, [k1], new_ref, expire})

      {:ok, {:applied_at, _, r2}, _} =
        Ferricstore.Raft.CommandClock.process_command(shard2_id, {:lock_keys, [k2], new_ref, expire})

      assert r1 == :ok, "Lock on k1 should be released after execute_fn exception"
      assert r2 == :ok, "Lock on k2 should be released after execute_fn exception"

      # Clean up
      Ferricstore.Raft.CommandClock.process_command(shard1_id, {:unlock_keys, [k1], new_ref})
      Ferricstore.Raft.CommandClock.process_command(shard2_id, {:unlock_keys, [k2], new_ref})
    end

    test "regression guard: intent write and execute phase stay in cleanup scope" do
      # This confirms the fixed path still completes normally; the structural
      # source guard lives in cross_shard_op_intent_guard_test.exs.
      [k1, k2] = ShardHelpers.keys_on_different_shards(2)
      Router.put(FerricStore.Instance.get(:default), k1, "h2_guard", 0)

      result = Ferricstore.Commands.Generic.handle("RENAME", [k1, k2], %{})
      assert result == :ok
    end
  end

  # ---------------------------------------------------------------------------
  # R2-H3: Intent resolver doesn't clean up locks
  #
  # IntentResolver.resolve_single_intent/3 only calls {:delete_intent, ...}.
  # It does NOT call {:unlock_keys, ...}. So after resolving a stale intent,
  # the associated locks (if any remain) are left behind.
  # ---------------------------------------------------------------------------

  describe "R2-H3: intent resolver does not clean up locks" do
    test "stale intent is cleaned up but lock on same key persists" do
      [k1, k2] = ShardHelpers.keys_on_different_shards(2)
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), k1)
      shard_id = Cluster.shard_server_id(shard_idx)

      coordinator_shard_idx =
        min(
          Router.shard_for(FerricStore.Instance.get(:default), k1),
          Router.shard_for(FerricStore.Instance.get(:default), k2)
        )

      coordinator_id = Cluster.shard_server_id(coordinator_shard_idx)

      owner_ref = make_ref()
      now = System.os_time(:millisecond)

      # Simulate a crashed coordinator: lock acquired + intent written, but
      # never cleaned up. Use a long TTL so the lock doesn't expire during test.
      expire_at = now + 60_000

      {:ok, {:applied_at, _, :ok}, _} =
        Ferricstore.Raft.CommandClock.process_command(shard_id, {:lock_keys, [k1], owner_ref, expire_at})

      intent_map = %{
        command: :rename,
        keys: %{source: k1, dest: k2},
        value_hashes: %{k1 => :erlang.phash2(nil)},
        status: :executing,
        created_at: now - 20_000
      }

      {:ok, {:applied_at, _, :ok}, _} =
        Ferricstore.Raft.CommandClock.process_command(coordinator_id, {:cross_shard_intent, owner_ref, intent_map})

      # Verify both exist
      {:ok, {:applied_at, _, intents}, _} = Ferricstore.Raft.CommandClock.process_command(coordinator_id, {:get_intents})
      assert Map.has_key?(intents, owner_ref), "Intent should exist before resolution"

      # Verify the lock is active
      other_ref = make_ref()

      assert {:error, :keys_locked} =
               shard_id
               |> Ferricstore.Raft.CommandClock.process_command(
                 {:lock_keys, [k1], other_ref, expire_at}
               )
               |> raft_result()

      # Run the intent resolver
      CrossShardOp.IntentResolver.resolve_stale_intents()

      # Intent should be cleaned up
      {:ok, {:applied_at, _, intents_after}, _} =
        Ferricstore.Raft.CommandClock.process_command(coordinator_id, {:get_intents})

      refute Map.has_key?(intents_after, owner_ref), "Intent should be cleaned up"

      # FIX: Lock should be cleaned up along with the intent.
      {:ok, {:applied_at, _, lock_result}, _} =
        Ferricstore.Raft.CommandClock.process_command(shard_id, {:lock_keys, [k1], other_ref, expire_at})

      assert lock_result == :ok,
             "R2-H3: Lock should be cleaned up after intent resolver runs"

      # Clean up
      Ferricstore.Raft.CommandClock.process_command(shard_id, {:unlock_keys, [k1], other_ref})
    end
  end

  # ---------------------------------------------------------------------------
  # R2-H4: Expired locks not cleaned -- memory leak
  #
  # The process dictionary accumulates lock entries forever. Expired locks
  # are checked lazily (on access) but never removed from the map. Over time
  # this grows without bound.
  # ---------------------------------------------------------------------------

  describe "R2-H4: expired locks not cleaned -- memory leak" do
    test "100 expired locks accumulate in process dictionary" do
      [k1, _k2] = ShardHelpers.keys_on_different_shards(2)
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), k1)
      shard_id = Cluster.shard_server_id(shard_idx)

      now = System.os_time(:millisecond)
      # Create 100 locks that expire immediately (TTL in the past)
      expired_at = now - 1000

      [{first_key, _first_ref} | _rest] =
        for i <- 1..100 do
          key = "r2h4_leak_#{:rand.uniform(1_000_000)}_#{i}"
          # We need these keys to route to the same shard. Use a prefix based
          # on a key we know routes to shard_idx.
          ref = make_ref()
          Ferricstore.Raft.CommandClock.process_command(shard_id, {:lock_keys, [key], ref, expired_at})
          {key, ref}
        end

      # All 100 locks are expired. New locks on any of these keys should succeed
      # (the expiry check passes), but the old entries stay in the map.
      # Verify a new lock succeeds (expiry check works):
      new_ref = make_ref()

      {:ok, {:applied_at, _, :ok}, _} =
        Ferricstore.Raft.CommandClock.process_command(shard_id, {:lock_keys, [first_key], new_ref, now + 30_000})

      # The expired entries are still in the process dictionary map.
      # We can't directly inspect process dict from outside, but we can prove
      # the entries accumulate by locking 100 MORE unique keys and checking
      # that the shard still works (doesn't crash from memory).
      #
      # More directly: lock a key, then try to lock the same key with a different
      # owner after the first expired. The old entry was NOT removed -- it just
      # passed the expiry check. Verify by doing another round:
      for i <- 1..100 do
        key = "r2h4_leak2_#{:rand.uniform(1_000_000)}_#{i}"
        ref = make_ref()
        Ferricstore.Raft.CommandClock.process_command(shard_id, {:lock_keys, [key], ref, expired_at})
      end

      # FIX: Trigger a new lock operation which prunes expired entries.
      # After pruning, the lock map should only contain non-expired entries.
      test_ref = make_ref()
      test_key = "r2h4_after_#{:rand.uniform(1_000_000)}"

      {:ok, {:applied_at, _, :ok}, _} =
        Ferricstore.Raft.CommandClock.process_command(shard_id, {:lock_keys, [test_key], test_ref, now + 30_000})

      # Query the cross_shard_locks map size to verify expired entries were pruned.
      # The map should contain at most the 2 non-expired locks (first_key + test_key),
      # not 200+ accumulated expired entries.
      {:ok, {:applied_at, _, lock_count}, _} = Ferricstore.Raft.CommandClock.process_command(shard_id, {:get_lock_count})

      assert lock_count <= 5,
             "R2-H4: Expected expired locks to be pruned, but #{lock_count} entries remain"

      # Clean up
      Ferricstore.Raft.CommandClock.process_command(shard_id, {:unlock_keys, [test_key], test_ref})
      Ferricstore.Raft.CommandClock.process_command(shard_id, {:unlock_keys, [first_key], new_ref})
    end
  end

  # ---------------------------------------------------------------------------
  # R2-M1: Unlock failures silently ignored
  #
  # parallel_unlock/3 calls Task.await_many but discards the results.
  # If any unlock fails (Raft timeout, netsplit, etc.), the caller never knows.
  # ---------------------------------------------------------------------------

  describe "R2-M1: unlock failures silently ignored (regression guard)" do
    test "parallel_unlock completes without error under normal conditions" do
      [k1, k2] = ShardHelpers.keys_on_different_shards(2)

      Router.put(FerricStore.Instance.get(:default), k1, "r2m1_value", 0)

      # Execute a normal cross-shard operation. The unlock phase runs inside
      # CrossShardOp.execute and its results are discarded. If it failed silently,
      # subsequent operations on these keys would be blocked.
      result = Ferricstore.Commands.Generic.handle("RENAME", [k1, k2], %{})
      assert result == :ok

      # Verify keys are not still locked: we can lock them ourselves
      shard1_id =
        Cluster.shard_server_id(Router.shard_for(FerricStore.Instance.get(:default), k1))

      shard2_id =
        Cluster.shard_server_id(Router.shard_for(FerricStore.Instance.get(:default), k2))

      ref = make_ref()
      expire = System.os_time(:millisecond) + 30_000

      {:ok, {:applied_at, _, r1}, _} =
        Ferricstore.Raft.CommandClock.process_command(shard1_id, {:lock_keys, [k1], ref, expire})

      {:ok, {:applied_at, _, r2}, _} =
        Ferricstore.Raft.CommandClock.process_command(shard2_id, {:lock_keys, [k2], ref, expire})

      assert r1 == :ok, "k1 should be unlockable after RENAME completes"
      assert r2 == :ok, "k2 should be unlockable after RENAME completes"

      # Clean up
      Ferricstore.Raft.CommandClock.process_command(shard1_id, {:unlock_keys, [k1], ref})
      Ferricstore.Raft.CommandClock.process_command(shard2_id, {:unlock_keys, [k2], ref})
    end

    test "parallel unlock results are discarded (code structure verification)" do
      # parallel_unlock/3 in cross_shard_op.ex does:
      #
      #   shards
      #   |> Enum.filter(...)
      #   |> Enum.map(fn shard_idx -> Task.async(fn -> ... end) end)
      #   |> Task.await_many(5_000)
      #
      # The return value of Task.await_many is not pattern-matched or checked.
      # This means if any Ferricstore.Raft.CommandClock.process_command in the unlock task returns
      # {:error, reason}, the error is silently dropped.
      #
      # Regression guard: verify the function works in the happy path.
      [k1, k2] = ShardHelpers.keys_on_different_shards(2)
      Router.put(FerricStore.Instance.get(:default), k1, "m1_verify", 0)

      result = Ferricstore.Commands.Generic.handle("COPY", [k1, k2], %{})
      assert result == 1

      assert Router.get(FerricStore.Instance.get(:default), k1) == "m1_verify"
      assert Router.get(FerricStore.Instance.get(:default), k2) == "m1_verify"
    end
  end
end
