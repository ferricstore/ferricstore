defmodule Ferricstore.Raft.NativeEdgeCasesTest do
  @moduledoc """
  Edge-case tests for native commands (CAS, LOCK, RATELIMIT.ADD) routed
  through the Raft write path, plus concurrency tests that prove Raft
  serialization provides correct atomic semantics.

  These tests exercise the full Raft pipeline: Router -> Shard -> Batcher ->
  ra -> StateMachine -> ETS + Bitcask. The Raft infrastructure is set up in
  `setup_all` following the same pattern as `WritePathTest`.

  ## Test groups

    1. CAS edge cases -- TTL propagation, expiry semantics, concurrency,
       empty-string vs nil distinction.
    2. LOCK edge cases -- expired key acquisition, owner re-acquire with TTL
       update, multi-owner conflict, wrong-owner unlock, extend on unlocked.
    3. RATELIMIT.ADD edge cases -- window rollover, rapid calls, bulk count,
       independent keys, zero-window edge case.
    4. Concurrent Raft writes -- INCR, CAS, LOCK, LPUSH under concurrency to
       prove Raft serialization delivers correct atomic semantics.
  """

  use ExUnit.Case, async: false
  @moduletag :raft

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()

    :ok
  end

  setup do
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
  end

  # Helper to generate unique keys
  defp ukey(base), do: "raft_edge_#{base}_#{:rand.uniform(9_999_999)}"

  # ===========================================================================
  # CAS edge cases through Raft
  # ===========================================================================

  describe "CAS edge cases through Raft" do
    test "CAS with TTL -- swapped value has correct expiry" do
      k = ukey("cas_ttl_edge")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "original", 0)
      before_swap = System.os_time(:millisecond)

      assert 1 = Router.cas(FerricStore.Instance.get(:default), k, "original", "swapped", 5_000)

      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      after_swap = System.os_time(:millisecond)

      assert value == "swapped"
      # Expiry should be within the window [before_swap + 5000, after_swap + 5000]
      assert expire_at_ms >= before_swap + 5_000
      assert expire_at_ms <= after_swap + 5_000
    end

    test "CAS on expired key -- treats as missing and returns nil" do
      k = ukey("cas_expired_edge")
      past = System.os_time(:millisecond) - 1_000

      # Set a key with an already-expired TTL
      :ok = Router.put(FerricStore.Instance.get(:default), k, "expired_val", past)

      # CAS should treat the expired key as missing and return nil
      assert nil == Router.cas(FerricStore.Instance.get(:default), k, "expired_val", "new_val", nil)

      # The key should still not be readable
      assert nil == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "CAS concurrent: two CAS on same key -- only one succeeds" do
      k = ukey("cas_concurrent")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "initial", 0)

      # Launch two concurrent CAS operations that both expect "initial"
      tasks =
        for i <- 1..2 do
          Task.async(fn ->
            Router.cas(FerricStore.Instance.get(:default), k, "initial", "winner_#{i}", nil)
          end)
        end

      results = Task.await_many(tasks, 10_000)

      # Exactly one should return 1 (swapped) and one should return 0 (mismatch)
      assert Enum.sort(results) == [0, 1],
             "Expected exactly one CAS to succeed and one to fail, got: #{inspect(results)}"

      # The value should be one of the winners
      final = Router.get(FerricStore.Instance.get(:default), k)
      assert String.starts_with?(final, "winner_")
    end

    test "CAS empty string expected vs nil -- different semantics" do
      k_empty = ukey("cas_empty_str")
      k_missing = ukey("cas_nil")

      # Set a key to empty string
      :ok = Router.put(FerricStore.Instance.get(:default), k_empty, "", 0)

      # CAS with empty string expected on a key that has empty string -- should match
      assert 1 = Router.cas(FerricStore.Instance.get(:default), k_empty, "", "replaced", nil)
      assert "replaced" == Router.get(FerricStore.Instance.get(:default), k_empty)

      # CAS with empty string expected on a missing key -- should return nil (not 0)
      # because the key does not exist at all, which is different from having ""
      assert nil == Router.cas(FerricStore.Instance.get(:default), k_missing, "", "new_val", nil)
      assert nil == Router.get(FerricStore.Instance.get(:default), k_missing)
    end
  end

  # ===========================================================================
  # LOCK edge cases through Raft
  # ===========================================================================

  describe "LOCK edge cases through Raft" do
    test "LOCK on expired key -- acquires lock (treats as free)" do
      k = ukey("lock_expired_edge")
      past = System.os_time(:millisecond) - 1_000

      # Set a key with an already-expired TTL to simulate an expired lock
      :ok = Router.put(FerricStore.Instance.get(:default), k, "old_owner", past)

      # LOCK should treat the expired key as free and acquire
      assert :ok = Router.lock(FerricStore.Instance.get(:default), k, "new_owner", 30_000)
      assert "new_owner" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "LOCK same owner re-acquires -- updates TTL" do
      k = ukey("lock_reacq_ttl")

      # Acquire with a short TTL
      assert :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner_a", 5_000)
      {_, expire_first} = Router.get_meta(FerricStore.Instance.get(:default), k)

      # Small delay so the second lock gets a clearly different TTL
      Process.sleep(10)

      # Re-acquire with a much longer TTL
      assert :ok = Router.lock(FerricStore.Instance.get(:default), k, "owner_a", 60_000)
      {value, expire_second} = Router.get_meta(FerricStore.Instance.get(:default), k)

      assert value == "owner_a"
      # The TTL should have been extended significantly
      assert expire_second > expire_first
      assert expire_second >= System.os_time(:millisecond) + 59_000
    end

    test "two different owners -- second gets error" do
      k = ukey("lock_two_owners")

      assert :ok = Router.lock(FerricStore.Instance.get(:default), k, "alice", 30_000)

      assert {:error, "DISTLOCK lock is held by another owner"} =
               Router.lock(FerricStore.Instance.get(:default), k, "bob", 30_000)

      # Lock should still be held by alice
      assert "alice" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "UNLOCK wrong owner -- error, lock retained" do
      k = ukey("unlock_wrong")

      :ok = Router.lock(FerricStore.Instance.get(:default), k, "alice", 30_000)

      assert {:error, "DISTLOCK caller is not the lock owner"} =
               Router.unlock(FerricStore.Instance.get(:default), k, "bob")

      # Alice's lock should still be there
      assert "alice" == Router.get(FerricStore.Instance.get(:default), k)

      # Alice can still unlock
      assert 1 = Router.unlock(FerricStore.Instance.get(:default), k, "alice")
      assert nil == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "EXTEND on unlocked key -- error" do
      k = ukey("extend_no_lock")

      # No lock exists
      assert {:error, "DISTLOCK lock does not exist or has expired"} =
               Router.extend(FerricStore.Instance.get(:default), k, "owner_x", 30_000)
    end
  end

  # ===========================================================================
  # RATELIMIT.ADD edge cases through Raft
  # ===========================================================================

  describe "RATELIMIT.ADD edge cases through Raft" do
    test "window rollover -- old window counts decay" do
      k = ukey("rl_rollover")
      window_ms = 50
      max = 100

      # Add some requests in the current window
      ["allowed", 5, _rem, _ttl] = Router.ratelimit_add(FerricStore.Instance.get(:default), k, window_ms, max, 5)

      # Wait for TWO full windows so both current and previous counts
      # are completely zeroed out by the sliding window rotation logic.
      # After 2*window_ms, the algorithm hits the `now - cur_start >= window_ms * 2`
      # branch which resets cur_count=0, prv_count=0.
      Process.sleep(window_ms * 2 + 10)

      # After two full windows, all previous counts have decayed to zero.
      [status, count, remaining, _ttl] = Router.ratelimit_add(FerricStore.Instance.get(:default), k, window_ms, max, 1)

      assert status == "allowed"
      assert count == 1
      assert remaining == 99
    end

    test "multiple rapid calls -- count accurate" do
      k = ukey("rl_rapid")
      window_ms = 10_000
      max = 100

      # Fire 10 rapid calls
      results =
        for i <- 1..10 do
          [_status, count, _rem, _ttl] = Router.ratelimit_add(FerricStore.Instance.get(:default), k, window_ms, max, 1)
          {i, count}
        end

      # Each call should increment the count by exactly 1
      for {i, count} <- results do
        assert count == i,
               "After call #{i}, expected count #{i} but got #{count}"
      end
    end

    test "rate limit with count > 1 -- adds N to counter" do
      k = ukey("rl_bulk")
      window_ms = 10_000
      max = 100

      # Add 5 at once
      ["allowed", c1, r1, _ttl] = Router.ratelimit_add(FerricStore.Instance.get(:default), k, window_ms, max, 5)
      assert c1 == 5
      assert r1 == 95

      # Add 3 more
      ["allowed", c2, r2, _ttl] = Router.ratelimit_add(FerricStore.Instance.get(:default), k, window_ms, max, 3)
      assert c2 == 8
      assert r2 == 92

      # Try to add 93 more (would exceed max of 100)
      ["denied", c3, r3, _ttl] = Router.ratelimit_add(FerricStore.Instance.get(:default), k, window_ms, max, 93)
      assert c3 == 8
      assert r3 == 92
    end

    test "rate limit different keys -- independent windows" do
      k1 = ukey("rl_indep_a")
      k2 = ukey("rl_indep_b")
      window_ms = 10_000
      max = 5

      # Exhaust k1
      ["allowed", 5, 0, _] = Router.ratelimit_add(FerricStore.Instance.get(:default), k1, window_ms, max, 5)
      ["denied", 5, 0, _] = Router.ratelimit_add(FerricStore.Instance.get(:default), k1, window_ms, max, 1)

      # k2 should be completely independent and still allow
      ["allowed", 1, 4, _] = Router.ratelimit_add(FerricStore.Instance.get(:default), k2, window_ms, max, 1)
    end

    test "rate limit window_ms=0 -- edge case" do
      # window_ms=0 is rejected by the Native command parser (requires > 0),
      # but the StateMachine would handle it with a division-by-zero guard.
      # We verify the command layer rejects it properly.
      k = ukey("rl_zero_window")

      # The Native handler rejects window_ms <= 0
      result =
        Ferricstore.Commands.Native.handle(
          "RATELIMIT.ADD",
          [k, "0", "10", "1"],
          %{}
        )

      assert {:error, "ERR value is not an integer or out of range"} = result
    end
  end

  # ===========================================================================
  # Concurrent Raft writes -- proving serialization correctness
  # ===========================================================================

  describe "concurrent Raft writes -- serialization correctness" do
    test "10 concurrent INCR on same key -- final value = 10" do
      k = ukey("conc_incr")

      tasks =
        for _i <- 1..10 do
          Task.async(fn ->
            Router.incr(FerricStore.Instance.get(:default), k, 1)
          end)
        end

      results = Task.await_many(tasks, 15_000)

      # All should succeed
      assert Enum.all?(results, fn
               {:ok, _n} -> true
               _ -> false
             end),
             "All INCR calls should succeed, got: #{inspect(results)}"

      # The final value should be exactly 10
      assert "10" == Router.get(FerricStore.Instance.get(:default), k)

      # The individual results should be some permutation of 1..10
      values = Enum.map(results, fn {:ok, n} -> n end)
      assert Enum.sort(values) == Enum.to_list(1..10),
             "Each INCR should have seen a unique intermediate value, got: #{inspect(Enum.sort(values))}"
    end

    test "5 concurrent CAS on same key -- exactly 1 succeeds" do
      k = ukey("conc_cas")

      :ok = Router.put(FerricStore.Instance.get(:default), k, "starting_value", 0)

      tasks =
        for i <- 1..5 do
          Task.async(fn ->
            Router.cas(FerricStore.Instance.get(:default), k, "starting_value", "cas_winner_#{i}", nil)
          end)
        end

      results = Task.await_many(tasks, 15_000)

      # Exactly one should have returned 1 (swapped)
      successes = Enum.count(results, &(&1 == 1))
      failures = Enum.count(results, &(&1 == 0))

      assert successes == 1,
             "Expected exactly 1 CAS success, got #{successes}. Results: #{inspect(results)}"

      assert failures == 4,
             "Expected exactly 4 CAS failures, got #{failures}. Results: #{inspect(results)}"

      # The final value should be the winner
      final = Router.get(FerricStore.Instance.get(:default), k)
      assert String.starts_with?(final, "cas_winner_")
    end

    test "concurrent LOCK attempts -- exactly 1 acquires" do
      k = ukey("conc_lock")

      tasks =
        for i <- 1..5 do
          Task.async(fn ->
            Router.lock(FerricStore.Instance.get(:default), k, "owner_#{i}", 30_000)
          end)
        end

      results = Task.await_many(tasks, 15_000)

      # Exactly one should have returned :ok
      successes = Enum.count(results, &(&1 == :ok))

      errors =
        Enum.count(results, fn
          {:error, "DISTLOCK lock is held by another owner"} -> true
          _ -> false
        end)

      assert successes == 1,
             "Expected exactly 1 lock acquisition, got #{successes}. Results: #{inspect(results)}"

      assert errors == 4,
             "Expected exactly 4 lock rejections, got #{errors}. Results: #{inspect(results)}"

      # The lock should be held by exactly one owner
      owner = Router.get(FerricStore.Instance.get(:default), k)
      assert String.starts_with?(owner, "owner_")
    end

    test "concurrent LPUSH from multiple tasks -- all elements present" do
      k = ukey("conc_lpush")

      # Each task pushes a unique element
      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            Router.list_op(FerricStore.Instance.get(:default), k, {:lpush, ["elem_#{i}"]})
          end)
        end

      results = Task.await_many(tasks, 15_000)

      # All pushes should succeed with increasing lengths
      assert Enum.all?(results, fn
               n when is_integer(n) and n > 0 -> true
               _ -> false
             end),
             "All LPUSH calls should return positive lengths, got: #{inspect(results)}"

      # The returned lengths should be some permutation of 1..10
      assert Enum.sort(results) == Enum.to_list(1..10),
             "Each LPUSH should see a unique list length, got: #{inspect(Enum.sort(results))}"

      # All 10 elements should be present in the list
      elements = Router.list_op(FerricStore.Instance.get(:default), k, {:lrange, 0, -1})
      assert length(elements) == 10

      expected = for i <- 1..10, do: "elem_#{i}"

      assert Enum.sort(elements) == Enum.sort(expected),
             "All pushed elements should be present. Got: #{inspect(Enum.sort(elements))}"
    end
  end
end
