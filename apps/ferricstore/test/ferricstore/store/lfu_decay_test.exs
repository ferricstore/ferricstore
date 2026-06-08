defmodule Ferricstore.Store.LFUDecayTest do
  @moduledoc """
  Tests for time-based LFU counter decay matching Redis's implementation.

  The LFU field in keydir ETS tuples is now a packed 24-bit integer:
    - Upper 16 bits: ldt (last decrement time, minutes since epoch, wraps at 2^16)
    - Lower 8 bits: counter (logarithmic frequency, 0-255)

  On every access: decay first, then probabilistic increment, then update ldt.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  import Bitwise

  alias Ferricstore.Store.{LFU, Router}

  defp drain_all do
    shard_count = Application.get_env(:ferricstore, :shard_count, 4)

    Enum.each(0..(shard_count - 1), fn i ->
      Ferricstore.Raft.Batcher.flush(i)
    end)

    Ferricstore.Store.BitcaskWriter.flush_all(shard_count)
  end

  defp flush_all_keys do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
  end

  defp setup_router_backed_test(_context) do
    neutralize_memory_guard()
    flush_all_keys()

    on_exit(fn ->
      neutralize_memory_guard()
      Ferricstore.Test.ShardHelpers.wait_shards_alive()
    end)

    :ok
  end

  defp neutralize_memory_guard do
    try do
      FerricStore.Instance.inject_callbacks(:default, process_rss_fn: nil)

      Ferricstore.MemoryGuard.reconfigure(%{
        max_memory_bytes: 1_073_741_824,
        keydir_max_ram: 64 * 1024 * 1024,
        hot_cache_min_ram: 0,
        hot_cache_max_ram: :auto,
        eviction_policy: :volatile_lru
      })

      Ferricstore.MemoryGuard.reset_pressure_flags()
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end
  end

  defp keydir_for(key), do: :"keydir_#{Router.shard_for(FerricStore.Instance.get(:default), key)}"

  defp keydir_lookup(key) do
    :ets.lookup(keydir_for(key), key)
  end

  defp get_packed_lfu(key) do
    [{_, _, _, packed, _, _, _}] = keydir_lookup(key)
    packed
  end

  # =========================================================================
  # LFU module unit tests
  # =========================================================================

  describe "LFU.pack/unpack" do
    test "round-trips ldt and counter" do
      assert {1234, 42} == LFU.unpack(LFU.pack(1234, 42))
      assert {0, 0} == LFU.unpack(LFU.pack(0, 0))
      assert {0xFFFF, 255} == LFU.unpack(LFU.pack(0xFFFF, 255))
    end

    test "counter is masked to 8 bits" do
      {_ldt, counter} = LFU.unpack(LFU.pack(0, 300))
      assert counter == (300 &&& 0xFF)
    end

    test "ldt is masked to 16 bits" do
      {ldt, _counter} = LFU.unpack(LFU.pack(0x1FFFF, 5))
      assert ldt == (0x1FFFF &&& 0xFFFF)
    end
  end

  describe "LFU.elapsed_minutes/2" do
    test "simple case: now > ldt" do
      assert LFU.elapsed_minutes(100, 90) == 10
    end

    test "same time" do
      assert LFU.elapsed_minutes(50, 50) == 0
    end

    test "wraparound: now < ldt" do
      # 16-bit wrap: 0xFFFF - 65_530 + 5 + 1 = 40
      assert LFU.elapsed_minutes(5, 65_530) == 0xFFFF - 65_530 + 5 + 1
    end

    test "full wrap cycle" do
      # now=0, ldt=0xFFFF -> elapsed=1
      assert LFU.elapsed_minutes(0, 0xFFFF) == 1
    end
  end

  describe "LFU.initial/0" do
    test "returns packed value with counter=5 and current ldt" do
      packed = LFU.initial()
      {ldt, counter} = LFU.unpack(packed)
      assert counter == 5
      now_min = LFU.now_minutes()
      # ldt should be within 1 minute of now
      assert LFU.elapsed_minutes(now_min, ldt) <= 1
    end
  end

  describe "LFU.effective_counter/1" do
    test "returns counter when ldt is current (no decay)" do
      packed = LFU.pack(LFU.now_minutes(), 100)
      assert LFU.effective_counter(packed) == 100
    end

    test "decays counter based on elapsed minutes" do
      # ldt 10 minutes ago, counter=50, decay_time=1 -> effective = 50 - 10 = 40
      old_ldt = LFU.now_minutes() - 10 &&& 0xFFFF
      packed = LFU.pack(old_ldt, 50)

      decay_time = Application.get_env(:ferricstore, :lfu_decay_time, 1)

      if decay_time > 0 do
        effective = LFU.effective_counter(packed)
        expected = max(0, 50 - div(10, decay_time))
        assert effective == expected
      end
    end

    test "counter doesn't go below 0 after decay" do
      # ldt 300 minutes ago, counter=5 -> effective = max(0, 5 - 300) = 0
      old_ldt = LFU.now_minutes() - 300 &&& 0xFFFF
      packed = LFU.pack(old_ldt, 5)
      assert LFU.effective_counter(packed) >= 0
    end

    test "decay disabled when lfu_decay_time=0" do
      original = Application.get_env(:ferricstore, :lfu_decay_time, 1)
      Application.put_env(:ferricstore, :lfu_decay_time, 0)
      LFU.init_config_cache()

      try do
        old_ldt = LFU.now_minutes() - 100 &&& 0xFFFF
        packed = LFU.pack(old_ldt, 200)
        assert LFU.effective_counter(packed) == 200
      after
        Application.put_env(:ferricstore, :lfu_decay_time, original)
        LFU.init_config_cache()
      end
    end
  end

  # =========================================================================
  # Integration with keydir ETS
  # =========================================================================

  describe "packed LFU in keydir" do
    setup :setup_router_backed_test

    test "new key starts with packed LFU (counter=5, current ldt)" do
      Router.put(FerricStore.Instance.get(:default), "lfu_packed_new", "val")
      drain_all()

      packed = get_packed_lfu("lfu_packed_new")
      {ldt, counter} = LFU.unpack(packed)

      assert counter == 5, "new keys must start at counter 5, got #{counter}"
      now_min = LFU.now_minutes()

      assert LFU.elapsed_minutes(now_min, ldt) <= 1,
             "ldt should be approximately now"
    end

    test "packed LFU is a single integer (not bare counter)" do
      Router.put(FerricStore.Instance.get(:default), "lfu_packed_check", "val")
      drain_all()

      packed = get_packed_lfu("lfu_packed_check")
      {_ldt, counter} = LFU.unpack(packed)

      # The packed value should be larger than just the counter (ldt bits set)
      assert counter == 5
      # packed includes ldt in upper bits, so it should be > 255 unless ldt=0
      # (which only happens at minute 0 of the epoch, essentially never in tests)
      assert packed > 255, "packed LFU should include ldt bits, got #{packed}"
    end

    test "reading key updates packed LFU (touch increments counter)" do
      Router.put(FerricStore.Instance.get(:default), "lfu_touch_test", "val")
      drain_all()

      packed_before = get_packed_lfu("lfu_touch_test")
      {_ldt_before, counter_before} = LFU.unpack(packed_before)

      # Read many times to probabilistically increment counter
      for _ <- 1..500, do: Router.get(FerricStore.Instance.get(:default), "lfu_touch_test")

      packed_after = get_packed_lfu("lfu_touch_test")
      {_ldt_after, counter_after} = LFU.unpack(packed_after)

      assert counter_after >= counter_before,
             "counter should not decrease from reads: before=#{counter_before}, after=#{counter_after}"
    end

    test "counter doesn't exceed 255 even with many reads" do
      Router.put(FerricStore.Instance.get(:default), "lfu_cap_test", "val")
      drain_all()

      # Set counter to max via direct ETS manipulation
      keydir = keydir_for("lfu_cap_test")
      :ets.update_element(keydir, "lfu_cap_test", {4, LFU.pack(LFU.now_minutes(), 255)})

      # Read many times
      for _ <- 1..200, do: Router.get(FerricStore.Instance.get(:default), "lfu_cap_test")

      packed = get_packed_lfu("lfu_cap_test")
      {_ldt, counter} = LFU.unpack(packed)
      assert counter <= 255, "counter must not exceed 255, got #{counter}"
    end
  end

  # =========================================================================
  # Decay behavior tests
  # =========================================================================

  describe "time-based decay" do
    setup :setup_router_backed_test

    test "key with old ldt gets counter decayed on access" do
      Router.put(FerricStore.Instance.get(:default), "lfu_decay_access", "val")
      drain_all()

      # Simulate a key that was last touched 20 minutes ago with counter=50
      keydir = keydir_for("lfu_decay_access")
      old_ldt = LFU.now_minutes() - 20 &&& 0xFFFF
      :ets.update_element(keydir, "lfu_decay_access", {4, LFU.pack(old_ldt, 50)})

      # Read the key -- this triggers lfu_touch which decays first
      Router.get(FerricStore.Instance.get(:default), "lfu_decay_access")

      packed = get_packed_lfu("lfu_decay_access")
      {ldt, counter} = LFU.unpack(packed)

      decay_time = Application.get_env(:ferricstore, :lfu_decay_time, 1)

      if decay_time > 0 do
        # After 20 minutes with decay_time=1, counter should decay by ~20
        # Then probabilistic increment may add 1
        expected_decayed = max(0, 50 - div(20, decay_time))

        assert counter <= expected_decayed + 1,
               "counter should be at most #{expected_decayed + 1} after decay+increment, got #{counter}"

        assert counter >= expected_decayed,
               "counter should be at least #{expected_decayed} after decay, got #{counter}"
      end

      # ldt should be updated to approximately now
      now_min = LFU.now_minutes()

      assert LFU.elapsed_minutes(now_min, ldt) <= 1,
             "ldt should be updated to current time"
    end

    test "very old key decays counter to 0" do
      Router.put(FerricStore.Instance.get(:default), "lfu_decay_zero", "val")
      drain_all()

      # Simulate a key last touched 500 minutes ago with counter=10
      keydir = keydir_for("lfu_decay_zero")
      old_ldt = LFU.now_minutes() - 500 &&& 0xFFFF
      :ets.update_element(keydir, "lfu_decay_zero", {4, LFU.pack(old_ldt, 10)})

      # Read the key
      Router.get(FerricStore.Instance.get(:default), "lfu_decay_zero")

      packed = get_packed_lfu("lfu_decay_zero")
      {_ldt, counter} = LFU.unpack(packed)

      # After 500 minutes with counter=10, counter should decay to 0
      # then probabilistic increment: 1/(0*10+1) = 100% -> becomes 1
      assert counter <= 1,
             "very old key should decay to near 0 then increment to at most 1, got #{counter}"
    end

    test "frequently accessed key maintains high counter despite decay" do
      Router.put(FerricStore.Instance.get(:default), "lfu_hot_key", "val")
      drain_all()

      # Set a high initial counter with current ldt
      keydir = keydir_for("lfu_hot_key")
      :ets.update_element(keydir, "lfu_hot_key", {4, LFU.pack(LFU.now_minutes(), 50)})

      # Read many times (within same minute, so no significant decay)
      for _ <- 1..500, do: Router.get(FerricStore.Instance.get(:default), "lfu_hot_key")

      packed = get_packed_lfu("lfu_hot_key")
      {_ldt, counter} = LFU.unpack(packed)

      # Counter should be >= 50 (no decay since ldt is current, only increments)
      assert counter >= 50,
             "frequently accessed key should maintain or increase counter, got #{counter}"
    end

    test "decay=0 disables time-based decay" do
      # Temporarily set decay_time=0 on the default Instance
      ctx = FerricStore.Instance.get(:default)
      original_ctx = ctx
      updated = %{ctx | lfu_decay_time: 0}
      :persistent_term.put({FerricStore.Instance, :default}, updated)

      try do
        key = "lfu_no_decay_#{System.unique_integer([:positive])}"
        Router.put(updated, key, "val", 0)
        drain_all()

        # Simulate old ldt with high counter
        keydir = keydir_for(key)
        old_ldt = LFU.now_minutes() - 100 &&& 0xFFFF
        :ets.update_element(keydir, key, {4, LFU.pack(old_ldt, 200)})

        # Read the key — with decay=0, counter should not decrease
        Router.get(updated, key)

        packed = get_packed_lfu(key)
        {_ldt, counter} = LFU.unpack(packed)

        assert counter >= 200,
               "with decay=0, counter should not decrease, got #{counter}"
      after
        :persistent_term.put({FerricStore.Instance, :default}, original_ctx)
      end
    end
  end

  # =========================================================================
  # 16-bit minute wraparound
  # =========================================================================

  describe "16-bit minute wraparound" do
    test "LFU.touch handles wraparound correctly" do
      # Create an ETS table for isolated testing
      table = :ets.new(:lfu_wrap_test, [:set, :public])
      :ets.insert(table, {"wrap_key", "val", 0, LFU.pack(0xFFF0, 100), 0, 0, 0})

      # Touch with current time (which is much larger, wrapping around)
      LFU.touch(table, "wrap_key", LFU.pack(0xFFF0, 100))

      [{_, _, _, packed, _, _, _}] = :ets.lookup(table, "wrap_key")
      {ldt, counter} = LFU.unpack(packed)

      # The ldt should be updated to now
      now_min = LFU.now_minutes()
      assert LFU.elapsed_minutes(now_min, ldt) <= 1

      # Counter should have been decayed (large elapsed time due to wrap)
      # and then incremented
      assert counter <= 255

      :ets.delete(table)
    end

    test "elapsed_minutes handles all wrap cases" do
      # No wrap
      assert LFU.elapsed_minutes(100, 50) == 50

      # Exact wrap boundary
      assert LFU.elapsed_minutes(0, 0xFFFF) == 1

      # Large wrap
      assert LFU.elapsed_minutes(10, 0xFFF0) == 0xFFFF - 0xFFF0 + 10 + 1
    end
  end

  # =========================================================================
  # Eviction with effective counter
  # =========================================================================

  describe "eviction uses effective (decayed) counter" do
    setup :setup_router_backed_test

    test "recently-accessed key has higher effective counter than stale key" do
      Router.put(FerricStore.Instance.get(:default), "evict_recent", "val")
      Router.put(FerricStore.Instance.get(:default), "evict_stale", "val")
      drain_all()

      # Set stale key to old ldt with high raw counter
      keydir_stale = keydir_for("evict_stale")
      old_ldt = LFU.now_minutes() - 100 &&& 0xFFFF
      :ets.update_element(keydir_stale, "evict_stale", {4, LFU.pack(old_ldt, 50)})

      # Set recent key to current ldt with moderate counter
      keydir_recent = keydir_for("evict_recent")
      :ets.update_element(keydir_recent, "evict_recent", {4, LFU.pack(LFU.now_minutes(), 30)})

      # Effective counters: stale = max(0, 50 - 100/1) = 0, recent = 30
      stale_eff = LFU.effective_counter(get_packed_lfu("evict_stale"))
      recent_eff = LFU.effective_counter(get_packed_lfu("evict_recent"))

      assert stale_eff < recent_eff,
             "stale key effective counter (#{stale_eff}) should be less than recent (#{recent_eff})"
    end

    test "key with same raw counter but older ldt has lower effective counter" do
      Router.put(FerricStore.Instance.get(:default), "eff_old", "val")
      Router.put(FerricStore.Instance.get(:default), "eff_new", "val")
      drain_all()

      # Both keys: raw counter=40
      # Old key: ldt 30 minutes ago
      # New key: ldt now
      keydir_old = keydir_for("eff_old")
      old_ldt = LFU.now_minutes() - 30 &&& 0xFFFF
      :ets.update_element(keydir_old, "eff_old", {4, LFU.pack(old_ldt, 40)})

      keydir_new = keydir_for("eff_new")
      :ets.update_element(keydir_new, "eff_new", {4, LFU.pack(LFU.now_minutes(), 40)})

      eff_old = LFU.effective_counter(get_packed_lfu("eff_old"))
      eff_new = LFU.effective_counter(get_packed_lfu("eff_new"))

      decay_time = Application.get_env(:ferricstore, :lfu_decay_time, 1)

      if decay_time > 0 do
        assert eff_old < eff_new,
               "older key effective counter (#{eff_old}) should be less than newer (#{eff_new})"
      end
    end
  end

  # =========================================================================
  # OBJECT FREQ command
  # =========================================================================

  describe "OBJECT FREQ returns effective counter" do
    setup :setup_router_backed_test

    test "returns decayed counter for existing key" do
      Router.put(FerricStore.Instance.get(:default), "freq_key", "val")
      drain_all()

      # Set counter to known value with current ldt
      keydir = keydir_for("freq_key")
      :ets.update_element(keydir, "freq_key", {4, LFU.pack(LFU.now_minutes(), 42)})

      store = %{
        get: fn k -> Router.get(FerricStore.Instance.get(:default), k) end,
        exists?: fn k -> Router.exists?(FerricStore.Instance.get(:default), k) end,
        object_lfu: fn k -> Router.object_lfu(FerricStore.Instance.get(:default), k) end
      }

      result = Ferricstore.Commands.Generic.handle("OBJECT", ["FREQ", "freq_key"], store)
      assert result == 42
    end

    test "returns decayed value for old key" do
      Router.put(FerricStore.Instance.get(:default), "freq_old_key", "val")
      drain_all()

      # Set counter to 100 with ldt 50 minutes ago
      keydir = keydir_for("freq_old_key")
      old_ldt = LFU.now_minutes() - 50 &&& 0xFFFF
      :ets.update_element(keydir, "freq_old_key", {4, LFU.pack(old_ldt, 100)})

      store = %{
        get: fn k -> Router.get(FerricStore.Instance.get(:default), k) end,
        exists?: fn k -> Router.exists?(FerricStore.Instance.get(:default), k) end,
        object_lfu: fn k -> Router.object_lfu(FerricStore.Instance.get(:default), k) end
      }

      result = Ferricstore.Commands.Generic.handle("OBJECT", ["FREQ", "freq_old_key"], store)

      decay_time = Application.get_env(:ferricstore, :lfu_decay_time, 1)

      if decay_time > 0 do
        expected = max(0, 100 - div(50, decay_time))

        assert result == expected,
               "OBJECT FREQ should return decayed counter #{expected}, got #{result}"
      end
    end

    test "returns error for non-existent key" do
      store = %{
        get: fn k -> Router.get(FerricStore.Instance.get(:default), k) end,
        exists?: fn k -> Router.exists?(FerricStore.Instance.get(:default), k) end
      }

      assert {:error, "ERR no such key"} ==
               Ferricstore.Commands.Generic.handle("OBJECT", ["FREQ", "nonexistent_key"], store)
    end
  end

  # =========================================================================
  # Re-warm after eviction
  # =========================================================================

  describe "re-warm after eviction resets LFU" do
    setup :setup_router_backed_test

    test "re-warmed key gets fresh LFU (counter=5, current ldt)" do
      Router.put(FerricStore.Instance.get(:default), "rewarm_lfu", "val")
      drain_all()

      # Bump counter high
      keydir = keydir_for("rewarm_lfu")
      :ets.update_element(keydir, "rewarm_lfu", {4, LFU.pack(LFU.now_minutes(), 200)})

      # Evict
      :ets.update_element(keydir, "rewarm_lfu", {2, nil})

      # Ensure skip_promotion is off so re-warm actually caches in ETS
      Ferricstore.MemoryGuard.set_skip_promotion(false)

      # Re-warm by reading
      Router.get(FerricStore.Instance.get(:default), "rewarm_lfu")

      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          packed = get_packed_lfu("rewarm_lfu")
          {_ldt, counter} = LFU.unpack(packed)
          counter == 5
        end,
        "re-warmed key should reset to counter=5",
        20,
        50
      )

      packed = get_packed_lfu("rewarm_lfu")
      {ldt, counter} = LFU.unpack(packed)

      assert counter == 5, "re-warmed key should reset to counter=5, got #{counter}"
      now_min = LFU.now_minutes()

      assert LFU.elapsed_minutes(now_min, ldt) <= 1,
             "re-warmed key should have current ldt"
    end
  end
end
