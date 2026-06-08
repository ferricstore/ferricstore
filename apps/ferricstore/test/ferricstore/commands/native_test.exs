defmodule Ferricstore.Commands.NativeTest do
  @moduledoc """
  Tests for FerricStore native commands: CAS, LOCK, UNLOCK, EXTEND, RATELIMIT.ADD.

  These tests run against the application-supervised shards via the Router,
  since native commands use atomic GenServer calls.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.Native
  alias Ferricstore.MemoryGuard
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    # These tests use the shared default instance, so reset leaked keyspace and
    # MemoryGuard pressure before each case. Several commands read through a
    # different layer than the seeding Router.put/4 call.
    ShardHelpers.flush_all_keys()
    force_memory_guard_ok()

    on_exit(fn ->
      ShardHelpers.reset_memory_guard_pressure()
    end)

    ShardHelpers.wait_shards_alive()
    :ok
  end

  # Generates a unique key to prevent cross-test interference.
  defp ukey(base), do: "#{base}_#{:rand.uniform(9_999_999)}"

  defp dummy_store, do: FerricStore.Instance.get(:default)

  defp default_ctx, do: FerricStore.Instance.get(:default)

  defp force_memory_guard_ok do
    try do
      MemoryGuard.reconfigure(%{
        max_memory_bytes: 100_000_000_000,
        keydir_max_ram: 100_000_000_000,
        hot_cache_min_ram: 0,
        hot_cache_max_ram: :auto,
        eviction_policy: :volatile_lru
      })

      :sys.replace_state(MemoryGuard, fn state ->
        %{state | last_pressure_level: :ok, keydir_pressure_level: :ok}
      end)

      MemoryGuard.reset_pressure_flags()
    catch
      :exit, _ -> :ok
    end
  end

  defp put_existing!(key, value, expire_at_ms \\ 0) do
    ctx = default_ctx()
    force_memory_guard_ok()
    assert :ok = Router.put(ctx, key, value, expire_at_ms)

    ShardHelpers.eventually(
      fn -> Router.get(ctx, key) == value end,
      "native command test seed key #{inspect(key)} should be readable",
      250,
      10
    )
  end

  defp observed_store(parent \\ self()) do
    %{
      cas: fn key, expected, new_value, ttl_ms ->
        send(parent, {:native_store, :cas, key, expected, new_value, ttl_ms})
        {:cas_result, ttl_ms}
      end,
      lock: fn key, owner, ttl_ms ->
        send(parent, {:native_store, :lock, key, owner, ttl_ms})
        :locked
      end,
      unlock: fn key, owner ->
        send(parent, {:native_store, :unlock, key, owner})
        7
      end,
      extend: fn key, owner, ttl_ms ->
        send(parent, {:native_store, :extend, key, owner, ttl_ms})
        8
      end,
      ratelimit_add: fn key, window_ms, max, count ->
        send(parent, {:native_store, :ratelimit_add, key, window_ms, max, count})
        ["observed", window_ms, max, count]
      end
    }
  end

  describe "store injection" do
    test "string command handlers use the provided store" do
      store = observed_store()

      assert {:cas_result, nil} == Native.handle("CAS", ["k", "old", "new"], store)
      assert_received {:native_store, :cas, "k", "old", "new", nil}

      assert {:cas_result, 2_000} == Native.handle("CAS", ["k", "old", "new", "EX", "2"], store)
      assert_received {:native_store, :cas, "k", "old", "new", 2_000}

      assert :locked == Native.handle("LOCK", ["lk", "owner", "123"], store)
      assert_received {:native_store, :lock, "lk", "owner", 123}

      assert 7 == Native.handle("UNLOCK", ["lk", "owner"], store)
      assert_received {:native_store, :unlock, "lk", "owner"}

      assert 8 == Native.handle("EXTEND", ["lk", "owner", "456"], store)
      assert_received {:native_store, :extend, "lk", "owner", 456}

      assert ["observed", 1_000, 5, 2] ==
               Native.handle("RATELIMIT.ADD", ["rl", "1000", "5", "2"], store)

      assert_received {:native_store, :ratelimit_add, "rl", 1_000, 5, 2}
    end

    test "AST command handlers use the provided store" do
      store = observed_store()

      assert {:cas_result, 100} == Native.handle_ast({:cas, "k", "old", "new", 100}, store)
      assert_received {:native_store, :cas, "k", "old", "new", 100}

      assert :locked == Native.handle_ast({:lock, "lk", "owner", 123}, store)
      assert_received {:native_store, :lock, "lk", "owner", 123}

      assert 7 == Native.handle_ast({:unlock, "lk", "owner"}, store)
      assert_received {:native_store, :unlock, "lk", "owner"}

      assert 8 == Native.handle_ast({:extend, "lk", "owner", 456}, store)
      assert_received {:native_store, :extend, "lk", "owner", 456}

      assert ["observed", 1_000, 5, 2] ==
               Native.handle_ast({:ratelimit_add, "rl", 1_000, 5, 2}, store)

      assert_received {:native_store, :ratelimit_add, "rl", 1_000, 5, 2}
    end
  end

  # ===========================================================================
  # CAS — Compare-and-Swap
  # ===========================================================================

  describe "CAS" do
    test "CAS on non-existent key returns nil" do
      key = ukey("cas_miss")
      assert nil == Native.handle("CAS", [key, "old", "new"], dummy_store())
    end

    test "CAS with matching expected value returns 1 and sets new value" do
      key = ukey("cas_match")
      put_existing!(key, "old")
      assert 1 == Native.handle("CAS", [key, "old", "new"], dummy_store())
      assert "new" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "CAS with non-matching expected returns 0 and does not change value" do
      key = ukey("cas_mismatch")
      put_existing!(key, "current")
      assert 0 == Native.handle("CAS", [key, "wrong", "new"], dummy_store())
      assert "current" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "CAS with EX sets TTL" do
      key = ukey("cas_ex")
      put_existing!(key, "old")
      assert 1 == Native.handle("CAS", [key, "old", "new", "EX", "60"], dummy_store())
      assert "new" == Router.get(FerricStore.Instance.get(:default), key)
      # The key should have a TTL now
      {_val, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), key)
      assert expire_at_ms > System.os_time(:millisecond)
      assert expire_at_ms <= System.os_time(:millisecond) + 61_000
    end

    test "CAS without EX preserves existing TTL" do
      key = ukey("cas_keep_ttl")
      future = System.os_time(:millisecond) + 120_000
      put_existing!(key, "old", future)
      assert 1 == Native.handle("CAS", [key, "old", "new"], dummy_store())
      assert "new" == Router.get(FerricStore.Instance.get(:default), key)
      {_val, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), key)
      assert expire_at_ms == future
    end

    test "CAS wrong number of args returns error" do
      assert {:error, msg} = Native.handle("CAS", ["key"], dummy_store())
      assert msg =~ "wrong number of arguments"
    end

    test "CAS with too many args returns error" do
      assert {:error, _} = Native.handle("CAS", ["k", "a", "b", "EX"], dummy_store())
    end

    test "CAS with EX 0 returns error (TTL must be positive)" do
      key = ukey("cas_ex_zero")
      put_existing!(key, "old")
      assert {:error, _} = Native.handle("CAS", [key, "old", "new", "EX", "0"], dummy_store())
    end

    test "CAS with EX non-integer returns error" do
      key = ukey("cas_ex_bad")
      put_existing!(key, "old")
      assert {:error, _} = Native.handle("CAS", [key, "old", "new", "EX", "abc"], dummy_store())
    end

    test "CAS on expired key returns nil" do
      key = ukey("cas_expired")
      past = System.os_time(:millisecond) - 1_000
      assert :ok = Router.put(default_ctx(), key, "old", past)
      # Give ETS time to recognize expiry
      assert nil == Native.handle("CAS", [key, "old", "new"], dummy_store())
    end
  end

  # ===========================================================================
  # LOCK
  # ===========================================================================

  describe "LOCK" do
    test "LOCK on non-existent key acquires lock" do
      key = ukey("lock_new")
      assert :ok = Native.handle("LOCK", [key, "owner1", "5000"], dummy_store())
      assert "owner1" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "LOCK with same owner returns OK (idempotent)" do
      key = ukey("lock_idem")
      assert :ok = Native.handle("LOCK", [key, "owner1", "5000"], dummy_store())
      assert :ok = Native.handle("LOCK", [key, "owner1", "5000"], dummy_store())
      assert "owner1" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "LOCK with different owner returns DISTLOCK error" do
      key = ukey("lock_conflict")
      assert :ok = Native.handle("LOCK", [key, "owner1", "5000"], dummy_store())
      assert {:error, msg} = Native.handle("LOCK", [key, "owner2", "5000"], dummy_store())
      assert msg =~ "DISTLOCK"
      assert msg =~ "held by another owner"
    end

    test "LOCK on expired lock acquires it" do
      key = ukey("lock_expired")
      # Set a lock with a very short TTL
      past = System.os_time(:millisecond) - 1_000
      assert :ok = Router.put(default_ctx(), key, "old_owner", past)
      # Wait for it to expire
      assert :ok = Native.handle("LOCK", [key, "new_owner", "5000"], dummy_store())
      assert "new_owner" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "LOCK sets TTL on the key" do
      key = ukey("lock_ttl")
      assert :ok = Native.handle("LOCK", [key, "owner1", "5000"], dummy_store())
      {_val, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), key)
      assert expire_at_ms > System.os_time(:millisecond)
      assert expire_at_ms <= System.os_time(:millisecond) + 6_000
    end

    test "LOCK wrong number of args returns error" do
      assert {:error, msg} = Native.handle("LOCK", ["key", "owner"], dummy_store())
      assert msg =~ "wrong number of arguments"
    end

    test "LOCK with non-integer ttl returns error" do
      assert {:error, _} = Native.handle("LOCK", ["key", "owner", "abc"], dummy_store())
    end

    test "LOCK with zero ttl returns error" do
      assert {:error, _} = Native.handle("LOCK", ["key", "owner", "0"], dummy_store())
    end
  end

  # ===========================================================================
  # UNLOCK
  # ===========================================================================

  describe "UNLOCK" do
    test "UNLOCK with matching owner releases lock" do
      key = ukey("unlock_match")
      assert :ok = Native.handle("LOCK", [key, "owner1", "5000"], dummy_store())
      assert 1 == Native.handle("UNLOCK", [key, "owner1"], dummy_store())
      assert nil == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "UNLOCK with wrong owner returns error" do
      key = ukey("unlock_wrong")
      assert :ok = Native.handle("LOCK", [key, "owner1", "5000"], dummy_store())
      assert {:error, msg} = Native.handle("UNLOCK", [key, "owner2"], dummy_store())
      assert msg =~ "DISTLOCK"
      assert msg =~ "not the lock owner"
    end

    test "UNLOCK on non-existent key returns 1 (already unlocked)" do
      key = ukey("unlock_none")
      assert 1 == Native.handle("UNLOCK", [key, "anyone"], dummy_store())
    end

    test "UNLOCK wrong number of args returns error" do
      assert {:error, msg} = Native.handle("UNLOCK", ["key"], dummy_store())
      assert msg =~ "wrong number of arguments"
    end

    test "UNLOCK after expiry returns 1 (already unlocked)" do
      key = ukey("unlock_after_exp")
      past = System.os_time(:millisecond) - 1_000
      assert :ok = Router.put(default_ctx(), key, "owner1", past)
      assert 1 == Native.handle("UNLOCK", [key, "owner1"], dummy_store())
    end
  end

  # ===========================================================================
  # EXTEND
  # ===========================================================================

  describe "EXTEND" do
    test "EXTEND with matching owner extends TTL" do
      key = ukey("extend_match")
      assert :ok = Native.handle("LOCK", [key, "owner1", "1000"], dummy_store())
      {_val, old_exp} = Router.get_meta(FerricStore.Instance.get(:default), key)

      assert 1 == Native.handle("EXTEND", [key, "owner1", "60000"], dummy_store())
      {_val, new_exp} = Router.get_meta(FerricStore.Instance.get(:default), key)
      assert new_exp > old_exp
    end

    test "EXTEND with wrong owner returns error" do
      key = ukey("extend_wrong")
      assert :ok = Native.handle("LOCK", [key, "owner1", "5000"], dummy_store())

      assert {:error, msg} = Native.handle("EXTEND", [key, "owner2", "5000"], dummy_store())
      assert msg =~ "DISTLOCK"
      assert msg =~ "not the lock owner"
    end

    test "EXTEND on non-existent key returns error" do
      key = ukey("extend_none")
      assert {:error, msg} = Native.handle("EXTEND", [key, "owner1", "5000"], dummy_store())
      assert msg =~ "DISTLOCK"
    end

    test "EXTEND on expired key returns error" do
      key = ukey("extend_expired")
      past = System.os_time(:millisecond) - 1_000
      assert :ok = Router.put(default_ctx(), key, "owner1", past)
      assert {:error, msg} = Native.handle("EXTEND", [key, "owner1", "5000"], dummy_store())
      assert msg =~ "DISTLOCK"
    end

    test "EXTEND wrong number of args returns error" do
      assert {:error, msg} = Native.handle("EXTEND", ["key", "owner"], dummy_store())
      assert msg =~ "wrong number of arguments"
    end

    test "EXTEND with non-integer ttl returns error" do
      assert {:error, _} = Native.handle("EXTEND", ["key", "owner", "abc"], dummy_store())
    end

    test "EXTEND with zero ttl returns error" do
      assert {:error, _} = Native.handle("EXTEND", ["key", "owner", "0"], dummy_store())
    end
  end

  # ===========================================================================
  # RATELIMIT.ADD
  # ===========================================================================

  describe "RATELIMIT.ADD" do
    test "first request within limit returns allowed" do
      key = ukey("rl_first")
      result = Native.handle("RATELIMIT.ADD", [key, "10000", "5"], dummy_store())
      assert ["allowed", count, remaining, _ms_reset] = result
      assert count == 1
      assert remaining == 4
    end

    test "requests up to max are allowed" do
      key = ukey("rl_up_to_max")

      for i <- 1..5 do
        result = Native.handle("RATELIMIT.ADD", [key, "10000", "5"], dummy_store())
        ["allowed", ^i, remaining, _ms_reset] = result
        assert remaining == 5 - i
      end
    end

    test "request beyond max returns denied" do
      key = ukey("rl_denied")

      # Fill up the limit
      for _ <- 1..5 do
        ["allowed" | _] = Native.handle("RATELIMIT.ADD", [key, "10000", "5"], dummy_store())
      end

      result = Native.handle("RATELIMIT.ADD", [key, "10000", "5"], dummy_store())
      assert ["denied", _count, 0, _ms_reset] = result
    end

    test "window resets after window_ms" do
      key = ukey("rl_reset")

      # Fill up the limit with a wider window to avoid CI timing flakes
      for _ <- 1..3 do
        ["allowed" | _] = Native.handle("RATELIMIT.ADD", [key, "200", "3"], dummy_store())
      end

      # Should be denied
      ["denied" | _] = Native.handle("RATELIMIT.ADD", [key, "200", "3"], dummy_store())

      # Wait for window to expire (2x window to ensure full expiry)
      Process.sleep(500)

      # Should be allowed again
      result = Native.handle("RATELIMIT.ADD", [key, "200", "3"], dummy_store())
      assert ["allowed" | _] = result
    end

    test "count parameter adds N instead of 1" do
      key = ukey("rl_count_n")

      result = Native.handle("RATELIMIT.ADD", [key, "10000", "10", "5"], dummy_store())
      assert ["allowed", 5, 5, _ms_reset] = result

      result = Native.handle("RATELIMIT.ADD", [key, "10000", "10", "3"], dummy_store())
      assert ["allowed", 8, 2, _ms_reset] = result
    end

    test "count parameter exceeding remaining returns denied" do
      key = ukey("rl_count_exceed")

      # Add 8 out of 10
      ["allowed" | _] = Native.handle("RATELIMIT.ADD", [key, "10000", "10", "8"], dummy_store())

      # Try to add 3 more (would exceed 10)
      result = Native.handle("RATELIMIT.ADD", [key, "10000", "10", "3"], dummy_store())
      assert ["denied", _count, _remaining, _ms_reset] = result
    end

    test "return array format: [status, count, remaining, ms_reset]" do
      key = ukey("rl_format")
      result = Native.handle("RATELIMIT.ADD", [key, "10000", "100"], dummy_store())
      assert [status, count, remaining, ms_reset] = result
      assert is_binary(status)
      assert status in ["allowed", "denied"]
      assert is_integer(count)
      assert is_integer(remaining)
      assert is_integer(ms_reset)
      assert ms_reset >= 0
    end

    test "wrong number of args returns error" do
      assert {:error, msg} = Native.handle("RATELIMIT.ADD", ["key"], dummy_store())
      assert msg =~ "wrong number of arguments"
    end

    test "too many args returns error" do
      assert {:error, _} =
               Native.handle("RATELIMIT.ADD", ["k", "1000", "5", "1", "extra"], dummy_store())
    end

    test "non-integer window_ms returns error" do
      assert {:error, _} = Native.handle("RATELIMIT.ADD", ["k", "abc", "5"], dummy_store())
    end

    test "non-integer max returns error" do
      assert {:error, _} = Native.handle("RATELIMIT.ADD", ["k", "1000", "abc"], dummy_store())
    end

    test "zero window_ms returns error" do
      assert {:error, _} = Native.handle("RATELIMIT.ADD", ["k", "0", "5"], dummy_store())
    end

    test "zero max returns error" do
      assert {:error, _} = Native.handle("RATELIMIT.ADD", ["k", "1000", "0"], dummy_store())
    end
  end

  # ===========================================================================
  # Dispatcher integration
  # ===========================================================================

  describe "Dispatcher routing for native commands" do
    alias Ferricstore.Commands.Dispatcher

    test "CAS is routed through dispatcher" do
      key = ukey("disp_cas")
      put_existing!(key, "old")
      assert 1 == Dispatcher.dispatch("CAS", [key, "old", "new"], dummy_store())
      assert "new" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "CAS is case-insensitive" do
      key = ukey("disp_cas_ci")
      put_existing!(key, "old")
      assert 1 == Dispatcher.dispatch("cas", [key, "old", "new"], dummy_store())
    end

    test "LOCK is routed through dispatcher" do
      key = ukey("disp_lock")
      assert :ok = Dispatcher.dispatch("LOCK", [key, "owner1", "5000"], dummy_store())
    end

    test "UNLOCK is routed through dispatcher" do
      key = ukey("disp_unlock")
      assert 1 == Dispatcher.dispatch("UNLOCK", [key, "nobody"], dummy_store())
    end

    test "EXTEND is routed through dispatcher" do
      key = ukey("disp_extend")

      put_existing!(key, "owner1", System.os_time(:millisecond) + 60_000)

      assert 1 == Dispatcher.dispatch("EXTEND", [key, "owner1", "10000"], dummy_store())
    end

    test "RATELIMIT.ADD is routed through dispatcher" do
      key = ukey("disp_rl")
      result = Dispatcher.dispatch("RATELIMIT.ADD", [key, "10000", "5"], dummy_store())
      assert ["allowed" | _] = result
    end

    test "RATELIMIT.ADD is case-insensitive" do
      key = ukey("disp_rl_ci")
      result = Dispatcher.dispatch("ratelimit.add", [key, "10000", "5"], dummy_store())
      assert ["allowed" | _] = result
    end

    test "MODULE is routed through dispatcher" do
      assert [] == Dispatcher.dispatch("MODULE", ["LIST"], dummy_store())
    end

    test "WAITAOF is routed through dispatcher" do
      assert [0, 0] == Dispatcher.dispatch("WAITAOF", ["0", "0", "100"], dummy_store())
    end

    # CLIENT commands moved to FerricstoreServer.Commands.Client (server app)

    test "DEBUG is routed through dispatcher" do
      assert :ok = Dispatcher.dispatch("DEBUG", ["RELOAD"], dummy_store())
    end
  end
end
