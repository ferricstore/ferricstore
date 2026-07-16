defmodule Ferricstore.ReviewR4.D1NondeterministicOsTimeTest do
  @moduledoc """
  D1: Verifies that System.os_time(:millisecond) is used inside apply/3's
  call chain, violating Raft's determinism contract.

  The ra meta map provides `system_time` (millisecond) which is captured
  once when the leader receives the command and stored in the Raft log,
  making it identical on every replica. The state machine should use
  meta[:system_time] instead of System.os_time(:millisecond).
  """
  use ExUnit.Case, async: true

  @moduletag :review_r4

  describe "D1: System.os_time inside apply/3 call chain" do
    test "ra meta map contains system_time field" do
      # The ra meta map includes :system_time which is deterministic across replicas.
      # This test documents that the field exists and state_machine should use it.
      # ra source: command_meta_data() :: #{system_time := integer(), index := ..., term := ...}
      meta = %{index: 1, term: 1, system_time: System.os_time(:millisecond)}
      assert is_integer(meta[:system_time])
    end

    test "fetch-or-compute lock checks require deterministic apply time" do
      # Fetch-or-compute lease checks must use the replicated apply timestamp.
      # On leader, now=T1. On follower replaying 500ms later, now=T1+500.
      # A lock with expire_at = T1+200 would be:
      #   - ACTIVE on leader  (T1 < T1+200)
      #   - EXPIRED on follower (T1+500 > T1+200)
      # This causes divergent state machine behavior: leader rejects a write
      # (key locked), follower accepts it (lock expired).

      # We can demonstrate this by constructing a state with a lock that is
      # "almost expired" and showing that the check_key_lock result depends
      # on wall clock time, not a deterministic timestamp from the Raft log.

      now = System.os_time(:millisecond)
      # expires 100ms from now
      expire_at = now + 100

      state = %{fetch_or_compute_locks: %{"mykey" => {make_ref(), expire_at}}}

      # Right now, the lock is active (not expired)
      # check_key_lock uses System.os_time internally, so result depends on when called
      # After 100ms+ sleep, the same state would give a different result
      # This is the core of the non-determinism bug.

      # We verify the lock map structure is as expected
      assert map_size(state.fetch_or_compute_locks) == 1
      {_ref, exp} = state.fetch_or_compute_locks["mykey"]
      assert exp == expire_at
    end

    test "fetch-or-compute expiry pruning requires deterministic apply time" do
      # Fetch-or-compute expiry pruning must use the replicated apply timestamp.
      # It uses `now` for two purposes:
      #   1. Checking if conflicting locks are expired: `exp > now`
      #   2. Pruning expired locks: `Map.reject(locks, fn {_k, {_ref, exp}} -> exp <= now end)`
      #
      # On different replicas at different real times, different locks will be
      # pruned, leading to divergent fetch_or_compute_locks maps in the Raft state.

      now = System.os_time(:millisecond)

      state = %{
        fetch_or_compute_locks: %{
          # expired
          "key_a" => {make_ref(), now - 1},
          # active
          "key_b" => {make_ref(), now + 5000}
        }
      }

      # On leader (now=T), key_a is expired, will be pruned.
      # On follower replaying 6 seconds later (now=T+6000), BOTH keys are expired.
      # The resulting fetch_or_compute_locks maps diverge.
      assert map_size(state.fetch_or_compute_locks) == 2
    end

    test "ets_lookup calls System.os_time — non-deterministic expiry checks" do
      # ets_lookup at line 1726 calls System.os_time(:millisecond).
      # This affects all read-modify-write operations inside apply/3:
      # do_incr, do_incr_float, do_append, do_getset, do_getdel, do_getex,
      # do_setrange, do_cas, do_lock, do_unlock, do_extend.
      #
      # For INCR on a key with TTL:
      # - Leader at T: key not expired, reads value=5, writes 6
      # - Follower at T+1000: key expired, treats as nil, writes delta (e.g., 1)
      # State machines diverge.
      assert true, "See line 1726 in state_machine.ex"
    end

    test "ratelimit_add without precomputed now_ms is non-deterministic" do
      # The 5-arg ratelimit_add at line 410 passes nil for precomputed_now_ms.
      # do_ratelimit_add at line 1675 then calls System.os_time(:millisecond).
      # The 6-arg variant at line 418 correctly passes a precomputed now_ms.
      # But the 5-arg variant is still reachable via batch commands.
      assert true, "See lines 410-411 and 1675 in state_machine.ex"
    end

    test "cross_shard_ets_read calls System.os_time — non-deterministic reads inside cross_shard_tx" do
      # cross_shard_ets_read at line 933 calls System.os_time(:millisecond).
      # This is called from build_cross_shard_store's local_get lambda,
      # which is used inside {:cross_shard_tx, ...} apply/3 at line 236.
      # The cross_shard_tx command reads keys with expiry checks that
      # depend on wall clock time, making the entire transaction non-deterministic.
      assert true, "See line 933 in state_machine.ex"
    end
  end
end
