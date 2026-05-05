defmodule FerricstoreServer.Spec.NativeCommandsTest do
  @moduledoc """
  Spec sections 13, 14, 15 -- single-node adaptations of the distributed
  lock, rate limit, and FETCH_OR_COMPUTE stampede test plans.

  These tests verify the correctness of native FerricStore commands under
  concurrency on a single node. The cluster-specific tests (leader failover,
  cross-node Raft replication) are excluded; everything that can be validated
  with a single-node deployment is covered here.

  ## Sections covered

    * 13 -- Distributed Lock Tests (single-node applicable)
    * 14 -- Rate Limit Tests (single-node applicable)
    * 15 -- FETCH_OR_COMPUTE Stampede Tests (single-node)
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Native
  alias Ferricstore.FetchOrCompute
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  # Generates a unique key to prevent cross-test interference.
  defp ukey(base), do: "spec_#{base}_#{:rand.uniform(9_999_999)}"

  # Native lock/rate-limit commands execute through the store capability layer.
  # Use the real default instance here so this server-level spec exercises the
  # same quorum path as a production connection store.
  defp dummy_store, do: FerricStore.Instance.get(:default)

  # =========================================================================
  # Section 13: Distributed Lock Tests (single-node applicable)
  # =========================================================================

  describe "Section 13 -- concurrent LOCK race: 10 tasks try to LOCK same key simultaneously" do
    test "exactly 1 succeeds, 9 get lock-held error" do
      key = ukey("lock_race")

      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            Native.handle("LOCK", [key, "worker_#{i}", "10000"], dummy_store())
          end)
        end

      results = Task.await_many(tasks, 5_000)

      ok_count = Enum.count(results, &(&1 == :ok))
      error_count = Enum.count(results, &match?({:error, _}, &1))

      assert ok_count == 1,
             "exactly one task must win the lock race, got #{ok_count} winners"

      assert error_count == 9,
             "other 9 must see lock-held error, got #{error_count} errors"

      # Verify all errors are DISTLOCK
      Enum.each(results, fn
        :ok -> :ok
        {:error, msg} -> assert msg =~ "DISTLOCK"
      end)

      # Cleanup
      winning_owner =
        results
        |> Enum.with_index(1)
        |> Enum.find_value(fn
          {:ok, i} -> "worker_#{i}"
          _ -> nil
        end)

      if winning_owner, do: Native.handle("UNLOCK", [key, winning_owner], dummy_store())
    end
  end

  describe "Section 13 -- LOCK TTL expiry" do
    test "LOCK with 100ms TTL, wait 150ms, another LOCK succeeds" do
      key = ukey("lock_ttl_expiry")

      assert :ok = Native.handle("LOCK", [key, "owner_a", "100"], dummy_store())

      # Verify the lock is held -- another owner cannot acquire
      assert {:error, _} = Native.handle("LOCK", [key, "owner_b", "5000"], dummy_store())

      # Wait for TTL to expire
      ShardHelpers.eventually(
        fn ->
          :ok == Native.handle("LOCK", [key, "owner_b", "5000"], dummy_store())
        end,
        "lock TTL should expire and allow new acquisition",
        20,
        20
      )

      assert "owner_b" == Router.get(FerricStore.Instance.get(:default), key)

      # Cleanup
      Native.handle("UNLOCK", [key, "owner_b"], dummy_store())
    end
  end

  describe "Section 13 -- UNLOCK by owner only" do
    test "LOCK with owner 'a', UNLOCK with owner 'b' fails" do
      key = ukey("lock_owner_guard")

      assert :ok = Native.handle("LOCK", [key, "owner_a", "30000"], dummy_store())

      # Wrong owner cannot unlock
      assert {:error, msg} = Native.handle("UNLOCK", [key, "owner_b"], dummy_store())
      assert msg =~ "DISTLOCK"
      assert msg =~ "not the lock owner"

      # Lock is still held by owner_a
      assert "owner_a" == Router.get(FerricStore.Instance.get(:default), key)
      assert {:error, _} = Native.handle("LOCK", [key, "owner_c", "5000"], dummy_store())

      # Correct owner can unlock
      assert 1 == Native.handle("UNLOCK", [key, "owner_a"], dummy_store())
      assert nil == Router.get(FerricStore.Instance.get(:default), key)
    end
  end

  describe "Section 13 -- EXTEND refreshes TTL" do
    test "EXTEND pushes the expiry further into the future" do
      key = ukey("lock_extend_ttl")

      # Acquire lock with 500ms TTL
      assert :ok = Native.handle("LOCK", [key, "owner1", "500"], dummy_store())
      {_val, original_exp} = Router.get_meta(FerricStore.Instance.get(:default), key)

      # intentional delay — testing grace/timeout behavior
      Process.sleep(50)
      assert 1 == Native.handle("EXTEND", [key, "owner1", "60000"], dummy_store())

      {_val, new_exp} = Router.get_meta(FerricStore.Instance.get(:default), key)

      assert new_exp > original_exp,
             "EXTEND must push the expiry forward: #{new_exp} > #{original_exp}"

      # The new expiry should be approximately now + 60000ms
      now = System.os_time(:millisecond)
      assert new_exp > now + 50_000, "new TTL should be ~60s from now"
      assert new_exp <= now + 61_000

      # Cleanup
      Native.handle("UNLOCK", [key, "owner1"], dummy_store())
    end

    test "EXTEND keeps lock alive past original TTL" do
      key = ukey("lock_extend_keepalive")

      # Acquire lock with a TTL long enough to survive Raft commit latency (2s)
      assert :ok = Native.handle("LOCK", [key, "worker", "2000"], dummy_store())

      # Heartbeat: extend every 200ms for 5 iterations (1s total)
      # intentional delay — testing grace/timeout behavior
      for _ <- 1..5 do
        Process.sleep(200)
        assert 1 == Native.handle("EXTEND", [key, "worker", "2000"], dummy_store())
      end

      # After heartbeats the lock is still held
      assert "worker" == Router.get(FerricStore.Instance.get(:default), key)
      assert {:error, _} = Native.handle("LOCK", [key, "impostor", "100"], dummy_store())

      # Cleanup
      Native.handle("UNLOCK", [key, "worker"], dummy_store())
    end
  end

  describe "Section 13 -- LOCK idempotent for same owner" do
    test "same owner can re-acquire (refresh) lock without error" do
      key = ukey("lock_idempotent")

      assert :ok = Native.handle("LOCK", [key, "owner1", "1000"], dummy_store())
      {_val, exp1} = Router.get_meta(FerricStore.Instance.get(:default), key)

      # intentional delay — testing grace/timeout behavior
      Process.sleep(50)

      # Same owner re-locks with a longer TTL -- should succeed and refresh TTL
      assert :ok = Native.handle("LOCK", [key, "owner1", "60000"], dummy_store())
      {_val, exp2} = Router.get_meta(FerricStore.Instance.get(:default), key)

      assert exp2 > exp1,
             "re-lock by same owner should refresh TTL: #{exp2} > #{exp1}"

      # Value should still be owner1
      assert "owner1" == Router.get(FerricStore.Instance.get(:default), key)

      # Cleanup
      Native.handle("UNLOCK", [key, "owner1"], dummy_store())
    end
  end

  # =========================================================================
  # Section 14: Rate Limit Tests (single-node applicable)
  # =========================================================================

  describe "Section 14 -- RATELIMIT.ADD basic: first 10 succeed, 11th rejected" do
    test "rate limit enforced at exactly the specified max" do
      key = ukey("rl_basic_10")
      window_ms = "60000"
      max = "10"

      # First 10 requests should all be allowed
      results =
        for _ <- 1..10 do
          Native.handle("RATELIMIT.ADD", [key, window_ms, max], dummy_store())
        end

      Enum.each(results, fn result ->
        assert ["allowed" | _] = result
      end)

      # 11th request should be denied
      result = Native.handle("RATELIMIT.ADD", [key, window_ms, max], dummy_store())
      assert ["denied" | _] = result
    end
  end

  describe "Section 14 -- sliding window boundary" do
    test "requests near window boundary counted correctly" do
      key = ukey("rl_sliding_boundary")
      window_ms = "200"
      max = "5"

      # Fill the window
      for _ <- 1..5 do
        assert ["allowed" | _] =
                 Native.handle("RATELIMIT.ADD", [key, window_ms, max], dummy_store())
      end

      # 6th request should be denied
      assert ["denied" | _] = Native.handle("RATELIMIT.ADD", [key, window_ms, max], dummy_store())

      # The sliding window uses a weighted approximation: after one window_ms
      # rotation, the previous window's count still contributes weighted by
      # (1 - elapsed/window_ms). We need to wait >= 2 * window_ms so the
      # implementation fully clears both the current and previous windows.
      ShardHelpers.eventually(
        fn ->
          match?(
            ["allowed" | _],
            Native.handle("RATELIMIT.ADD", [key, window_ms, max], dummy_store())
          )
        end,
        "old requests should slide out of window",
        30,
        50
      )
    end
  end

  describe "Section 14 -- different keys are independent" do
    test "rate limit on key 'a' does not affect key 'b'" do
      key_a = ukey("rl_indep_a")
      key_b = ukey("rl_indep_b")
      window_ms = "60000"
      max = "5"

      # Exhaust the limit on key_a
      for _ <- 1..5 do
        assert ["allowed" | _] =
                 Native.handle("RATELIMIT.ADD", [key_a, window_ms, max], dummy_store())
      end

      assert ["denied" | _] =
               Native.handle("RATELIMIT.ADD", [key_a, window_ms, max], dummy_store())

      # key_b should be completely independent and have its full allowance
      for _ <- 1..5 do
        assert ["allowed" | _] =
                 Native.handle("RATELIMIT.ADD", [key_b, window_ms, max], dummy_store())
      end

      assert ["denied" | _] =
               Native.handle("RATELIMIT.ADD", [key_b, window_ms, max], dummy_store())
    end
  end

  describe "Section 14 -- window reset" do
    test "after window expires, counter resets and new requests are allowed" do
      key = ukey("rl_window_reset")
      window_ms = "100"
      max = "3"

      # Fill up the limit
      for _ <- 1..3 do
        assert ["allowed" | _] =
                 Native.handle("RATELIMIT.ADD", [key, window_ms, max], dummy_store())
      end

      assert ["denied" | _] =
               Native.handle("RATELIMIT.ADD", [key, window_ms, max], dummy_store())

      # intentional delay — testing grace/timeout behavior
      # Wait for window + margin (2x window to ensure full sliding window rotation)
      Process.sleep(250)

      # All 3 slots should be available again
      for _ <- 1..3 do
        assert ["allowed" | _] =
                 Native.handle("RATELIMIT.ADD", [key, window_ms, max], dummy_store())
      end
    end
  end

  describe "Section 14 -- concurrent rate limiting" do
    test "concurrent requests never exceed the limit" do
      key = ukey("rl_concurrent")
      window_ms = "60000"
      max = "50"

      # 100 concurrent requests -- exactly 50 should succeed
      tasks =
        for _ <- 1..100 do
          Task.async(fn ->
            Native.handle("RATELIMIT.ADD", [key, window_ms, max], dummy_store())
          end)
        end

      results = Task.await_many(tasks, 10_000)

      ok_count = Enum.count(results, &match?(["allowed" | _], &1))
      denied_count = Enum.count(results, &match?(["denied" | _], &1))

      assert ok_count == 50,
             "expected exactly #{max} allowed, got #{ok_count}"

      assert denied_count == 50,
             "expected 50 denied, got #{denied_count}"
    end
  end

  # =========================================================================
  # Section 15: FETCH_OR_COMPUTE Stampede Tests (single-node)
  # =========================================================================

  describe "Section 15 -- single miss triggers compute" do
    test "FETCH_OR_COMPUTE on missing key returns COMPUTE status" do
      key = ukey("foc_single_miss")

      result = FetchOrCompute.fetch_or_compute(key, 5_000, "")
      assert {:compute, _hint} = result

      # Cleanup: deliver a result to clear the compute lock
      FetchOrCompute.fetch_or_compute_result(key, "computed_val", 5_000)
    end
  end

  describe "Section 15 -- warm cache returns value, no compute" do
    test "FETCH_OR_COMPUTE on existing key returns hit, no COMPUTE issued" do
      key = ukey("foc_warm_cache")
      Router.put(FerricStore.Instance.get(:default), key, "cached_value", 0)

      # 20 concurrent callers -- all should get cached value, none get COMPUTE
      tasks =
        for _ <- 1..20 do
          Task.async(fn ->
            FetchOrCompute.fetch_or_compute(key, 5_000, "")
          end)
        end

      results = Task.await_many(tasks, 5_000)

      assert Enum.all?(results, &(&1 == {:hit, "cached_value"})),
             "all callers should receive cache hit, no COMPUTE expected"

      # Cleanup
      Router.delete(FerricStore.Instance.get(:default), key)
    end
  end

  describe "Section 15 -- concurrent misses: exactly 1 COMPUTE" do
    test "10 concurrent FETCH_OR_COMPUTE on same missing key, exactly 1 gets COMPUTE" do
      key = ukey("foc_stampede_10")
      compute_count = :counters.new(1, [:atomics])

      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            case FetchOrCompute.fetch_or_compute(key, 5_000, "hint") do
              {:compute, _hint} ->
                # This caller is the compute winner
                :counters.add(compute_count, 1, 1)
                # intentional delay — testing grace/timeout behavior
                Process.sleep(50)
                FetchOrCompute.fetch_or_compute_result(key, "rendered_html", 5_000)
                {:ok, "rendered_html"}

              {:ok, value} ->
                # This caller waited and received the computed value
                {:ok, value}
            end
          end)
        end

      results = Task.await_many(tasks, 10_000)

      # Exactly 1 compute winner
      assert :counters.get(compute_count, 1) == 1,
             "expected exactly 1 COMPUTE, got #{:counters.get(compute_count, 1)}"

      # All 10 callers received the value
      assert Enum.all?(results, &(&1 == {:ok, "rendered_html"})),
             "all callers should receive the computed value"

      # Cleanup
      Router.delete(FerricStore.Instance.get(:default), key)
    end

    test "50 concurrent FETCH_OR_COMPUTE on same missing key, exactly 1 COMPUTE" do
      key = ukey("foc_stampede_50")
      compute_count = :counters.new(1, [:atomics])

      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            case FetchOrCompute.fetch_or_compute(key, 5_000, "hint") do
              {:compute, _hint} ->
                :counters.add(compute_count, 1, 1)
                # intentional delay — testing grace/timeout behavior
                Process.sleep(100)
                FetchOrCompute.fetch_or_compute_result(key, "big_result", 5_000)
                {:ok, "big_result"}

              {:ok, value} ->
                {:ok, value}
            end
          end)
        end

      results = Task.await_many(tasks, 15_000)

      assert :counters.get(compute_count, 1) == 1,
             "expected exactly 1 COMPUTE, got #{:counters.get(compute_count, 1)}"

      assert Enum.all?(results, &(&1 == {:ok, "big_result"})),
             "all 50 callers should receive the computed value"

      # Cleanup
      Router.delete(FerricStore.Instance.get(:default), key)
    end
  end

  describe "Section 15 -- compute timeout unblocks waiters" do
    test "if compute does not respond, waiters eventually get promoted or error" do
      key = ukey("foc_timeout")

      # Become the computer (first caller)
      computer_task =
        Task.async(fn ->
          FetchOrCompute.fetch_or_compute(key, 5_000, "slow_hint")
        end)

      # intentional delay — let computer task register before spawning waiters
      Process.sleep(50)

      # Spawn waiters that will block
      waiter_tasks =
        for _ <- 1..3 do
          Task.async(fn ->
            FetchOrCompute.fetch_or_compute(key, 5_000, "slow_hint")
          end)
        end

      # intentional delay — let waiters register before checking results
      Process.sleep(50)

      # Computer got :compute
      assert {:compute, "slow_hint"} = Task.await(computer_task, 200)

      # Now simulate the compute never responding -- report an error instead
      # (In a real cluster test with short timeout the sweep would promote
      # the next waiter; here we use fetch_or_compute_error to unblock all
      # waiters, which is the fallback behaviour.)
      FetchOrCompute.fetch_or_compute_error(key, "compute_timeout")

      # All waiters should receive the error
      waiter_results = Task.await_many(waiter_tasks, 5_000)

      Enum.each(waiter_results, fn result ->
        assert {:error, "compute_timeout"} == result,
               "waiter should receive timeout error, got: #{inspect(result)}"
      end)
    end

    test "sweep promotes next waiter when computer times out" do
      # This test uses :sys.replace_state to manipulate the FetchOrCompute
      # ETS table from within the GenServer process (the table is :protected,
      # so only the owning process can write to it).
      key = ukey("foc_sweep_promote")
      table = :ferricstore_compute_locks

      # Spawn a task that will become the computer (first caller)
      computer_task =
        Task.async(fn ->
          FetchOrCompute.fetch_or_compute(key, 5_000, "sweep_hint")
        end)

      # intentional delay — let task register as the computer
      Process.sleep(50)
      result = Task.await(computer_task, 200)
      assert {:compute, "sweep_hint"} = result

      # Spawn a second caller that will become a waiter (blocks)
      waiter_task =
        Task.async(fn ->
          FetchOrCompute.fetch_or_compute(key, 5_000, "sweep_hint")
        end)

      # intentional delay — let waiter register before manipulating ETS
      Process.sleep(50)

      # Use :sys.replace_state to backdoor-modify the ETS entry's started_at
      # timestamp to simulate a long-stalled compute. This callback runs
      # inside the GenServer process, which owns the :protected ETS table.
      :sys.replace_state(FetchOrCompute, fn state ->
        case :ets.lookup(table, key) do
          [{^key, computer_pid, waiters, _started_at, hint, compute_id}] ->
            old_time = System.monotonic_time(:millisecond) - 60_000
            :ets.insert(table, {key, computer_pid, waiters, old_time, hint, compute_id})

          [] ->
            :ok
        end

        state
      end)

      # Trigger the sweep manually
      send(Process.whereis(FetchOrCompute), :sweep_timeouts)

      # The waiter should be promoted to computer
      result2 = Task.await(waiter_task, 5_000)
      assert {:compute, "sweep_hint"} = result2

      # Cleanup: deliver a result to clear the compute lock
      FetchOrCompute.fetch_or_compute_result(key, "swept_value", 5_000)
    end
  end

  describe "Section 15 -- cache hit after compute" do
    test "after compute delivers result, subsequent fetches return hit" do
      key = ukey("foc_post_compute_hit")

      # First call: triggers compute
      assert {:compute, ""} = FetchOrCompute.fetch_or_compute(key, 10_000, "")

      # Deliver the result
      FetchOrCompute.fetch_or_compute_result(key, "fresh_value", 10_000)

      # Subsequent calls should be cache hits
      for _ <- 1..5 do
        assert {:hit, "fresh_value"} = FetchOrCompute.fetch_or_compute(key, 10_000, "")
      end

      # Cleanup
      Router.delete(FerricStore.Instance.get(:default), key)
    end
  end

  describe "Section 15 -- compute error propagation" do
    test "error from computer is propagated to all waiting callers" do
      key = ukey("foc_error_propagation")
      error_count = :counters.new(1, [:atomics])

      tasks =
        for i <- 1..5 do
          Task.async(fn ->
            case FetchOrCompute.fetch_or_compute(key, 5_000, "err_hint") do
              {:compute, _hint} ->
                # intentional delay — let other tasks register as waiters
                Process.sleep(100)
                FetchOrCompute.fetch_or_compute_error(key, "db_down")
                {:compute_winner, i}

              {:error, msg} ->
                :counters.add(error_count, 1, 1)
                {:error, msg}
            end
          end)
        end

      results = Task.await_many(tasks, 10_000)

      # Exactly 1 compute winner
      compute_winners = Enum.filter(results, &match?({:compute_winner, _}, &1))
      assert length(compute_winners) == 1

      # All waiters received the error
      errors = Enum.filter(results, &match?({:error, _}, &1))
      assert length(errors) == 4
      Enum.each(errors, fn {:error, msg} -> assert msg == "db_down" end)
    end
  end

  describe "Section 15 -- FETCH_OR_COMPUTE via RESP command interface" do
    test "FETCH_OR_COMPUTE command returns ['compute', hint] on miss" do
      key = ukey("foc_cmd_miss")

      result = Native.handle("FETCH_OR_COMPUTE", [key, "5000", "my_hint"], dummy_store())
      assert ["compute", "my_hint"] = result

      # Cleanup via command
      Native.handle("FETCH_OR_COMPUTE_RESULT", [key, "done", "5000"], dummy_store())
    end

    test "FETCH_OR_COMPUTE command returns ['hit', value] on cache hit" do
      key = ukey("foc_cmd_hit")
      Router.put(FerricStore.Instance.get(:default), key, "already_cached", 0)

      result = Native.handle("FETCH_OR_COMPUTE", [key, "5000"], dummy_store())
      assert ["hit", "already_cached"] = result

      # Cleanup
      Router.delete(FerricStore.Instance.get(:default), key)
    end

    test "FETCH_OR_COMPUTE_RESULT stores value and subsequent fetch is hit" do
      key = ukey("foc_cmd_result_hit")

      ["compute", _] = Native.handle("FETCH_OR_COMPUTE", [key, "10000"], dummy_store())
      assert :ok = Native.handle("FETCH_OR_COMPUTE_RESULT", [key, "val", "10000"], dummy_store())

      result = Native.handle("FETCH_OR_COMPUTE", [key, "10000"], dummy_store())
      assert ["hit", "val"] = result

      # Cleanup
      Router.delete(FerricStore.Instance.get(:default), key)
    end
  end
end
