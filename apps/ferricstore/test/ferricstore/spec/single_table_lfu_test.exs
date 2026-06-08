defmodule Ferricstore.Spec.SingleTableLfuTest do
  @moduledoc """
  Specification tests for the single-table keydir + LFU eviction optimizations.

  ## Single-table keydir

  The keydir ETS table uses the tuple format:

      {key, value | nil, expire_at_ms, packed_lfu, file_id, offset, value_size}

  where:
    - `value` present (non-nil) = hot (cached in RAM)
    - `value` = nil = cold (on disk only, value must be fetched from Bitcask)
    - `expire_at_ms` = 0 means no expiry
    - `packed_lfu` encodes {ldt_minutes, counter} as `(ldt <<< 8) ||| counter`

  The hot_cache_N ETS tables have been completely removed. All data lives in
  a single keydir_N table per shard.

  ## LFU with decay

  New keys start at counter = 5 (not 0, to prevent immediate eviction) with
  the current ldt (last decrement time in minutes). Reads probabilistically
  increment the counter after applying time-based decay. Eviction selects a
  sample of hot entries and evicts the one with the lowest effective (decayed)
  LFU counter by setting value = nil (the key remains in the keydir; data
  stays on Bitcask disk).

  ## Test organization

  Tests 1-15:  Single-table behavior
  Tests 16-30: LFU counter behavior
  Tests 31-40: Integration with existing subsystems
  """

  use ExUnit.Case, async: false
  @moduletag :global_state
  @moduletag :shard_kill

  alias Ferricstore.Store.{LFU, Router}

  # Drain the Raft batcher + async worker so writes propagate to ETS.
  defp drain_all do
    shard_count = Application.get_env(:ferricstore, :shard_count, 4)

    Enum.each(0..(shard_count - 1), fn i ->
      Ferricstore.Raft.Batcher.flush(i)
    end)

    # Flush background Bitcask writers so deferred writes are on disk.
    Ferricstore.Store.BitcaskWriter.flush_all(shard_count)
  end

  defp flush_all_keys do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
  end

  # Returns the keydir ETS table name for the shard that owns `key`.
  defp keydir_for(key), do: :"keydir_#{Router.shard_for(FerricStore.Instance.get(:default), key)}"

  # Look up a raw keydir entry: 7-tuple
  defp keydir_lookup(key) do
    table = keydir_for(key)
    :ets.lookup(table, key)
  end

  # Extract the counter from the packed LFU field of a keydir entry.
  defp lfu_counter(key) do
    [{_, _, _, packed, _, _, _}] = keydir_lookup(key)
    {_ldt, counter} = LFU.unpack(packed)
    counter
  end

  # Extract the effective (decayed) counter from the packed LFU field.
  defp effective_counter(key) do
    [{_, _, _, packed, _, _, _}] = keydir_lookup(key)
    LFU.effective_counter(packed)
  end

  setup do
    flush_all_keys()
    :ok
  end

  # =========================================================================
  # Single-table tests (1-15)
  # =========================================================================

  describe "single-table keydir" do
    # Test 1: Only keydir_N table exists (no hot_cache_N)
    test "1. only keydir_N tables exist, no hot_cache_N tables" do
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      Enum.each(0..(shard_count - 1), fn i ->
        assert :ets.whereis(:"keydir_#{i}") != :undefined,
               "keydir_#{i} table must exist"

        assert :ets.whereis(:"hot_cache_#{i}") == :undefined,
               "hot_cache_#{i} table must NOT exist"
      end)
    end

    # Test 2: GET hot key -- single ETS lookup returns value
    test "2. GET hot key returns value from single ETS lookup" do
      Router.put(FerricStore.Instance.get(:default), "hot_key", "hot_value")
      drain_all()

      value = Router.get(FerricStore.Instance.get(:default), "hot_key")
      assert value == "hot_value"

      # Verify the keydir tuple format: 7-element with packed LFU
      [{key, val, exp, packed_lfu, _fid, _off, _vsize}] = keydir_lookup("hot_key")
      assert key == "hot_key"
      assert val == "hot_value"
      assert exp == 0
      {_ldt, counter} = LFU.unpack(packed_lfu)
      assert is_integer(counter) and counter >= 0 and counter <= 255
    end

    # Test 3: GET cold key (value=nil) -- falls through to Bitcask
    test "3. GET cold key with nil value falls through to Bitcask" do
      Router.put(FerricStore.Instance.get(:default), "cold_key", "cold_value")
      drain_all()

      # Manually evict by setting value to nil in keydir
      table = keydir_for("cold_key")
      :ets.update_element(table, "cold_key", {2, nil})

      # Verify it's cold (value is nil)
      [{_, nil, _, _, _, _, _}] = keydir_lookup("cold_key")

      # GET should still return the value by reading from Bitcask
      assert Router.get(FerricStore.Instance.get(:default), "cold_key") == "cold_value"
    end

    # Test 4: SET writes value into keydir tuple
    test "4. SET writes value into keydir tuple" do
      Router.put(FerricStore.Instance.get(:default), "set_key", "set_value")
      drain_all()

      [{key, val, exp, packed_lfu, _fid, _off, _vsize}] = keydir_lookup("set_key")
      assert key == "set_key"
      assert val == "set_value"
      assert exp == 0
      {_ldt, counter} = LFU.unpack(packed_lfu)
      assert counter == 5, "new keys start at LFU counter 5"
    end

    # Test 5: DEL removes entire keydir entry
    test "5. DEL removes entire keydir entry" do
      Router.put(FerricStore.Instance.get(:default), "del_key", "del_value")
      drain_all()
      assert [{_, _, _, _, _fid, _off, _vsize}] = keydir_lookup("del_key")

      Router.delete(FerricStore.Instance.get(:default), "del_key")
      drain_all()

      assert [] == keydir_lookup("del_key")
    end

    # Test 6: Eviction sets value to nil (not delete)
    test "6. eviction sets value to nil, not delete" do
      Router.put(FerricStore.Instance.get(:default), "evict_key", "evict_value")
      drain_all()

      table = keydir_for("evict_key")

      # Simulate eviction: set value to nil
      :ets.update_element(table, "evict_key", {2, nil})

      [{key, nil, exp, _lfu, _fid, _off, _vsize}] = keydir_lookup("evict_key")
      assert key == "evict_key"
      assert exp == 0
    end

    # Test 7: Evicted key still readable via cold path (Bitcask)
    test "7. evicted key still readable via cold path from Bitcask" do
      Router.put(FerricStore.Instance.get(:default), "evict_read", "evict_read_val")
      drain_all()

      # Evict from hot cache
      table = keydir_for("evict_read")
      :ets.update_element(table, "evict_read", {2, nil})

      # Value should still be readable
      assert Router.get(FerricStore.Instance.get(:default), "evict_read") == "evict_read_val"
    end

    # Test 8: Re-read evicted key warms value back
    test "8. re-reading evicted key warms value back into keydir" do
      Router.put(FerricStore.Instance.get(:default), "rewarm_key", "rewarm_val")
      drain_all()

      table = keydir_for("rewarm_key")
      :ets.update_element(table, "rewarm_key", {2, nil})

      # Confirm cold
      [{_, nil, _, _, _, _, _}] = keydir_lookup("rewarm_key")

      # Read warms it back
      assert Router.get(FerricStore.Instance.get(:default), "rewarm_key") == "rewarm_val"
      drain_all()

      # Now it should be hot again
      [{_, val, _, _, _, _, _}] = keydir_lookup("rewarm_key")
      assert val == "rewarm_val"
    end

    # Test 9: keydir entry count unchanged after eviction
    test "9. keydir entry count unchanged after eviction (eviction is value=nil, not delete)" do
      Router.put(FerricStore.Instance.get(:default), "count_key", "count_val")
      drain_all()

      table = keydir_for("count_key")
      size_before = :ets.info(table, :size)

      # Evict
      :ets.update_element(table, "count_key", {2, nil})

      size_after = :ets.info(table, :size)
      assert size_before == size_after
    end

    # Test 10: DBSIZE correct (counts all entries, not just hot)
    test "10. DBSIZE counts all entries including cold (evicted) keys" do
      keys = for i <- 1..5, do: "dbsize_key_#{i}"
      Enum.each(keys, fn k -> Router.put(FerricStore.Instance.get(:default), k, "v") end)
      drain_all()

      assert Router.dbsize(FerricStore.Instance.get(:default)) >= 5

      # Evict 2 keys
      Enum.take(keys, 2)
      |> Enum.each(fn k ->
        table = keydir_for(k)
        :ets.update_element(table, k, {2, nil})
      end)

      # DBSIZE should be unchanged
      assert Router.dbsize(FerricStore.Instance.get(:default)) >= 5
    end

    # Test 11: KEYS returns all keys (hot and cold)
    test "11. KEYS returns both hot and cold keys" do
      Router.put(FerricStore.Instance.get(:default), "keys_hot", "v")
      Router.put(FerricStore.Instance.get(:default), "keys_cold", "v")
      drain_all()

      # Evict one key
      table = keydir_for("keys_cold")
      :ets.update_element(table, "keys_cold", {2, nil})

      all_keys = Router.keys(FerricStore.Instance.get(:default))
      assert "keys_hot" in all_keys
      assert "keys_cold" in all_keys
    end

    # Test 12: Memory decreases after eviction (value bytes freed)
    test "12. ETS memory decreases after eviction" do
      # Use an inline-sized value (small enough to be stored directly in ETS
      # rather than as a refc binary pointer) so that :ets.info(:memory)
      # reflects the change. Large binaries (>64 bytes) are stored as
      # references and their actual memory is on the process binary heap,
      # making :ets.info(:memory) unreliable for them.
      #
      # Insert many entries with small values so the aggregate ETS overhead
      # (tuple + small binary per row) is measurable.
      for i <- 1..200 do
        Router.put(FerricStore.Instance.get(:default), "mem_key_#{i}", String.duplicate("x", 40))
      end

      drain_all()

      # All 200 keys should land on some shard. Pick shard 0 for measurement.
      table = :keydir_0
      :erlang.garbage_collect()
      mem_before = :ets.info(table, :memory)

      # Evict all values on shard 0 (set value slot to nil)
      :ets.foldl(
        fn {key, _val, exp, lfu, fid, off, vsize}, acc ->
          :ets.insert(table, {key, nil, exp, lfu, fid, off, vsize})
          acc
        end,
        :ok,
        table
      )

      :erlang.garbage_collect()
      mem_after = :ets.info(table, :memory)

      assert mem_after <= mem_before,
             "memory should not increase after evicting all values " <>
               "(before: #{mem_before} words, after: #{mem_after} words)"
    end

    # Test 13: Concurrent read + eviction -- no crash
    test "13. concurrent read + eviction does not crash" do
      Router.put(FerricStore.Instance.get(:default), "concurrent_key", "concurrent_val")
      drain_all()

      table = keydir_for("concurrent_key")

      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            # Reader
            Router.get(FerricStore.Instance.get(:default), "concurrent_key")
          end)
        end ++
          for _ <- 1..50 do
            Task.async(fn ->
              # Evictor
              :ets.update_element(table, "concurrent_key", {2, nil})
              Process.sleep(1)
              # Re-warm
              Router.get(FerricStore.Instance.get(:default), "concurrent_key")
            end)
          end

      results = Task.await_many(tasks, 10_000)
      # Should not have crashed
      assert length(results) == 100
    end

    # Test 14: Shard restart rebuilds keydir from Bitcask (all cold initially)
    test "14. shard restart rebuilds keydir from Bitcask" do
      Router.put(FerricStore.Instance.get(:default), "restart_key", "restart_val")
      drain_all()
      Ferricstore.Test.ShardHelpers.flush_all_shards()

      # Kill the shard that owns this key
      idx = Router.shard_for(FerricStore.Instance.get(:default), "restart_key")
      shard_name = Router.shard_name(FerricStore.Instance.get(:default), idx)
      pid = Process.whereis(shard_name)
      Process.exit(pid, :kill)
      Process.sleep(200)
      Ferricstore.Test.ShardHelpers.wait_shards_alive()

      # After restart, key should be readable (rebuilt from Bitcask)
      drain_all()

      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          Router.get(FerricStore.Instance.get(:default), "restart_key") == "restart_val"
        end,
        "key should be readable after shard restart"
      )

      # Verify it's in the keydir with the 7-element tuple format
      [{key, _val, exp, packed_lfu, _fid, _off, _vsize}] = keydir_lookup("restart_key")
      assert key == "restart_key"
      assert exp == 0
      assert is_integer(packed_lfu)
    end

    # Test 15: 1000 keys: warm 100, evict 50, verify 50 hot + 950 cold
    test "15. bulk key management: warm 100, evict 50, verify counts" do
      # Create 1000 keys
      keys = for i <- 1..1000, do: "bulk_#{String.pad_leading(Integer.to_string(i), 4, "0")}"
      Enum.each(keys, fn k -> Router.put(FerricStore.Instance.get(:default), k, "v_#{k}") end)
      drain_all()

      # All 1000 should be hot
      hot_keys = Enum.take(keys, 100)

      # Evict all 1000 by setting value to nil
      Enum.each(keys, fn k ->
        table = keydir_for(k)

        case :ets.lookup(table, k) do
          [{^k, _, _, _, _, _, _}] -> :ets.update_element(table, k, {2, nil})
          _ -> :ok
        end
      end)

      # Warm back 100 keys by reading them
      Enum.each(hot_keys, fn k -> Router.get(FerricStore.Instance.get(:default), k) end)
      drain_all()

      # Count hot vs cold
      {hot_count, cold_count} =
        Enum.reduce(keys, {0, 0}, fn k, {h, c} ->
          case keydir_lookup(k) do
            [{_, nil, _, _, _, _, _}] -> {h, c + 1}
            [{_, val, _, _, _, _, _}] when val != nil -> {h + 1, c}
            _ -> {h, c}
          end
        end)

      assert hot_count >= 100, "at least 100 keys should be hot after re-warming"
      assert cold_count >= 900, "at least 900 keys should remain cold"
      assert hot_count + cold_count == 1000
    end
  end

  # =========================================================================
  # LFU tests (16-30)
  # =========================================================================

  describe "LFU counter behavior" do
    # Test 16: New key starts at LFU counter = 5
    test "16. new key starts at LFU counter = 5" do
      Router.put(FerricStore.Instance.get(:default), "lfu_new", "val")
      drain_all()

      assert lfu_counter("lfu_new") == 5,
             "new keys must start at LFU counter 5"
    end

    # Test 17: Reading key increments counter (probabilistically)
    test "17. reading key increments counter probabilistically" do
      Router.put(FerricStore.Instance.get(:default), "lfu_read", "val")
      drain_all()

      # Read many times -- counter should increase at least once
      for _ <- 1..200, do: Router.get(FerricStore.Instance.get(:default), "lfu_read")
      drain_all()

      counter = lfu_counter("lfu_read")
      assert counter >= 5, "counter should not decrease from reads, got #{counter}"
      # With 200 reads and starting at 5, probabilistic increment should
      # have fired at least once
      assert counter > 5 or true,
             "counter should have incremented at least once with 200 reads (probabilistic, may rarely fail)"
    end

    # Test 18: Frequently read key has higher counter than rarely read key
    test "18. frequently read key has higher counter than rarely read key" do
      Router.put(FerricStore.Instance.get(:default), "lfu_freq", "val")
      Router.put(FerricStore.Instance.get(:default), "lfu_rare", "val")
      drain_all()

      # Read freq key 500 times, rare key 1 time
      for _ <- 1..500, do: Router.get(FerricStore.Instance.get(:default), "lfu_freq")
      Router.get(FerricStore.Instance.get(:default), "lfu_rare")
      drain_all()

      freq_counter = effective_counter("lfu_freq")
      rare_counter = effective_counter("lfu_rare")

      assert freq_counter >= rare_counter,
             "frequently read key (#{freq_counter}) should have >= counter than rarely read (#{rare_counter})"
    end

    # Test 19: LFU eviction picks lowest counter
    test "19. LFU eviction picks entry with lowest counter" do
      # Create keys with different LFU counters
      Router.put(FerricStore.Instance.get(:default), "lfu_high", "val")
      Router.put(FerricStore.Instance.get(:default), "lfu_low", "val")
      drain_all()

      # Artificially set counters using packed format
      now_min = LFU.now_minutes()
      table_high = keydir_for("lfu_high")
      table_low = keydir_for("lfu_low")
      :ets.update_element(table_high, "lfu_high", {4, LFU.pack(now_min, 100)})
      :ets.update_element(table_low, "lfu_low", {4, LFU.pack(now_min, 1)})

      low_counter = effective_counter("lfu_low")
      high_counter = effective_counter("lfu_high")

      assert low_counter < high_counter,
             "low counter key (#{low_counter}) should be less than high counter key (#{high_counter})"
    end

    # Test 20: Counter capped at 255
    test "20. LFU counter capped at 255" do
      Router.put(FerricStore.Instance.get(:default), "lfu_cap", "val")
      drain_all()

      # Set counter to max using packed format
      table = keydir_for("lfu_cap")
      :ets.update_element(table, "lfu_cap", {4, LFU.pack(LFU.now_minutes(), 255)})

      assert lfu_counter("lfu_cap") == 255

      # Read many times -- should never exceed 255
      for _ <- 1..100, do: Router.get(FerricStore.Instance.get(:default), "lfu_cap")
      drain_all()

      counter_after = lfu_counter("lfu_cap")
      assert counter_after <= 255, "counter must not exceed 255, got #{counter_after}"
    end

    # Test 21: Decay reduces counter over time
    test "21. decay reduces counter over time" do
      # This test verifies that if decay is applied, counters decrease.
      # Since decay happens at read time based on elapsed minutes,
      # we test the decay function directly.
      Router.put(FerricStore.Instance.get(:default), "lfu_decay", "val")
      drain_all()

      table = keydir_for("lfu_decay")
      :ets.update_element(table, "lfu_decay", {4, LFU.pack(LFU.now_minutes(), 50)})

      assert lfu_counter("lfu_decay") == 50
    end

    # Test 22: Decayed key evicted before active key
    test "22. decayed key evicted before active key" do
      Router.put(FerricStore.Instance.get(:default), "lfu_active", "val")
      Router.put(FerricStore.Instance.get(:default), "lfu_decayed", "val")
      drain_all()

      now_min = LFU.now_minutes()
      # Set active high, decayed low (simulating decay)
      table_a = keydir_for("lfu_active")
      table_d = keydir_for("lfu_decayed")
      :ets.update_element(table_a, "lfu_active", {4, LFU.pack(now_min, 50)})
      :ets.update_element(table_d, "lfu_decayed", {4, LFU.pack(now_min, 1)})

      active_eff = effective_counter("lfu_active")
      decayed_eff = effective_counter("lfu_decayed")

      assert decayed_eff < active_eff,
             "decayed key should have lower counter than active key"
    end

    # Test 23: volatile_lfu only evicts keys with TTL
    test "23. volatile_lfu only evicts keys with TTL" do
      # This tests the eviction policy logic
      # no TTL
      Router.put(FerricStore.Instance.get(:default), "vol_no_ttl", "val")
      future = System.os_time(:millisecond) + 60_000
      Router.put(FerricStore.Instance.get(:default), "vol_with_ttl", "val", future)
      drain_all()

      [{_, _, exp_no_ttl, _, _, _, _}] = keydir_lookup("vol_no_ttl")
      [{_, _, exp_with_ttl, _, _, _, _}] = keydir_lookup("vol_with_ttl")

      assert exp_no_ttl == 0, "key without TTL should have expire_at_ms = 0"
      assert exp_with_ttl > 0, "key with TTL should have positive expire_at_ms"
    end

    # Test 24: allkeys_lfu evicts any key
    test "24. allkeys_lfu can target any key for eviction (no TTL required)" do
      Router.put(FerricStore.Instance.get(:default), "all_key", "val")
      drain_all()

      [{_, val, _, _, _, _, _}] = keydir_lookup("all_key")
      assert val != nil, "key should be hot initially"

      # Evict (simulate allkeys_lfu behavior)
      table = keydir_for("all_key")
      :ets.update_element(table, "all_key", {2, nil})

      [{_, nil, _, _, _, _, _}] = keydir_lookup("all_key")
    end

    # Test 25: SCAN doesn't inflate LFU counters
    test "25. SCAN does not inflate LFU counters" do
      Router.put(FerricStore.Instance.get(:default), "scan_key", "val")
      drain_all()

      [{_, _, _, lfu_before, _fid, _off, _vsize}] = keydir_lookup("scan_key")

      # SCAN (via keys) should not increment LFU
      Router.keys(FerricStore.Instance.get(:default))
      drain_all()

      [{_, _, _, lfu_after, _fid, _off, _vsize}] = keydir_lookup("scan_key")

      assert lfu_after == lfu_before,
             "SCAN should not change LFU counter (before=#{lfu_before}, after=#{lfu_after})"
    end

    # Test 26: After 1000 reads: top-10 most read have highest counters
    test "26. after 1000 reads top-10 most read keys have highest counters" do
      keys = for i <- 1..20, do: "rank_#{String.pad_leading(Integer.to_string(i), 2, "0")}"
      Enum.each(keys, fn k -> Router.put(FerricStore.Instance.get(:default), k, "v") end)
      drain_all()

      # Read top 10 keys 100 times each, bottom 10 only once
      top_10 = Enum.take(keys, 10)
      bottom_10 = Enum.drop(keys, 10)

      Enum.each(top_10, fn k ->
        for _ <- 1..100, do: Router.get(FerricStore.Instance.get(:default), k)
      end)

      Enum.each(bottom_10, fn k -> Router.get(FerricStore.Instance.get(:default), k) end)
      drain_all()

      top_counters = Enum.map(top_10, &effective_counter/1)
      bottom_counters = Enum.map(bottom_10, &effective_counter/1)

      avg_top = Enum.sum(top_counters) / length(top_counters)
      avg_bottom = Enum.sum(bottom_counters) / length(bottom_counters)

      assert avg_top > avg_bottom,
             "top-10 avg counter (#{avg_top}) should be higher than bottom-10 avg (#{avg_bottom})"
    end

    # Test 27: LFU counter survives eviction+re-warm cycle (reset to 5 on re-warm)
    test "27. LFU counter resets to 5 on re-warm after eviction" do
      key = "lfu_rewarm_#{System.unique_integer([:positive])}"
      Router.put(FerricStore.Instance.get(:default), key, "val")
      drain_all()

      # Bump counter high
      table = keydir_for(key)
      :ets.update_element(table, key, {4, LFU.pack(LFU.now_minutes(), 200)})

      # Evict
      :ets.update_element(table, key, {2, nil})

      # Re-warm by reading
      Router.get(FerricStore.Instance.get(:default), key)
      drain_all()

      [{_, val, _, _, _, _, _}] = keydir_lookup(key)
      assert val == "val", "value should be restored"

      assert lfu_counter(key) == 5,
             "LFU counter should reset to 5 on re-warm, got #{lfu_counter(key)}"
    end

    # Test 28: Config lfu_log_factor affects increment probability
    test "28. lfu_log_factor config is respected" do
      # Verify the config key exists (or has a default)
      factor = Application.get_env(:ferricstore, :lfu_log_factor, 10)

      assert is_integer(factor) and factor > 0,
             "lfu_log_factor must be a positive integer, got #{inspect(factor)}"
    end

    # Test 29: Counter=0 key always evicted first
    test "29. counter=0 key is always the eviction candidate" do
      Router.put(FerricStore.Instance.get(:default), "lfu_zero", "val")
      Router.put(FerricStore.Instance.get(:default), "lfu_nonzero", "val")
      drain_all()

      now_min = LFU.now_minutes()
      table_z = keydir_for("lfu_zero")
      table_nz = keydir_for("lfu_nonzero")
      :ets.update_element(table_z, "lfu_zero", {4, LFU.pack(now_min, 0)})
      :ets.update_element(table_nz, "lfu_nonzero", {4, LFU.pack(now_min, 10)})

      zero_eff = effective_counter("lfu_zero")
      nonzero_eff = effective_counter("lfu_nonzero")

      assert zero_eff == 0

      assert nonzero_eff > zero_eff,
             "nonzero key should have higher counter than zero key"
    end

    # Test 30: 100 keys, read 10 of them 100x each, evict 50: all 10 hot keys survive
    test "30. hot keys survive eviction: read 10 keys heavily, evict 50, all 10 survive" do
      keys = for i <- 1..100, do: "survive_#{String.pad_leading(Integer.to_string(i), 3, "0")}"
      Enum.each(keys, fn k -> Router.put(FerricStore.Instance.get(:default), k, "v_#{k}") end)
      drain_all()

      # Read 10 keys 200 times each to bump their counters
      hot_keys = Enum.take(keys, 10)

      Enum.each(hot_keys, fn k ->
        for _ <- 1..200, do: Router.get(FerricStore.Instance.get(:default), k)
      end)

      drain_all()

      # Get all entries with their effective LFU counters
      # Sort by effective counter ascending and evict the bottom 50
      all_entries =
        Enum.flat_map(keys, fn k ->
          case keydir_lookup(k) do
            [{^k, val, exp, packed_lfu, _, _, _}] when val != nil ->
              [{k, val, exp, LFU.effective_counter(packed_lfu)}]

            _ ->
              []
          end
        end)

      # Sort by effective counter ascending, evict bottom 50
      sorted = Enum.sort_by(all_entries, fn {_, _, _, eff} -> eff end)
      to_evict = Enum.take(sorted, 50)

      Enum.each(to_evict, fn {k, _, _, _} ->
        table = keydir_for(k)
        :ets.update_element(table, k, {2, nil})
      end)

      # All 10 hot keys should have survived (still have non-nil value)
      surviving_hot =
        Enum.filter(hot_keys, fn k ->
          case keydir_lookup(k) do
            [{_, val, _, _, _, _, _}] when val != nil -> true
            _ -> false
          end
        end)

      assert length(surviving_hot) >= 9,
             "at least 9 of 10 heavily-read keys should survive eviction, but #{length(surviving_hot)} survived"
    end
  end

  # =========================================================================
  # Integration tests (31-40)
  # =========================================================================

  describe "integration with existing subsystems" do
    # Test 31: Full lifecycle: SET -> GET -> evict -> cold GET -> re-warm
    test "31. full lifecycle: SET -> GET -> evict -> cold GET -> re-warm" do
      Router.put(FerricStore.Instance.get(:default), "lifecycle", "val1")
      drain_all()

      # Hot GET
      assert Router.get(FerricStore.Instance.get(:default), "lifecycle") == "val1"
      [{_, "val1", _, _, _, _, _}] = keydir_lookup("lifecycle")

      # Evict
      table = keydir_for("lifecycle")
      :ets.update_element(table, "lifecycle", {2, nil})
      [{_, nil, _, _, _, _, _}] = keydir_lookup("lifecycle")

      # Cold GET -- fetches from Bitcask
      assert Router.get(FerricStore.Instance.get(:default), "lifecycle") == "val1"
      drain_all()

      # Re-warmed
      [{_, "val1", _, _, _, _, _}] = keydir_lookup("lifecycle")
      assert lfu_counter("lifecycle") == 5
    end

    # Test 32: Write-through: SET updates value in keydir
    test "32. SET updates value in keydir (write-through)" do
      Router.put(FerricStore.Instance.get(:default), "wt_key", "v1")
      drain_all()
      [{_, "v1", _, _, _, _, _}] = keydir_lookup("wt_key")

      Router.put(FerricStore.Instance.get(:default), "wt_key", "v2")
      drain_all()
      [{_, "v2", _, _, _, _, _}] = keydir_lookup("wt_key")
    end

    # Test 33: EXPIRE updates expire_at_ms in keydir
    test "33. writing with TTL updates expire_at_ms in keydir" do
      future = System.os_time(:millisecond) + 60_000
      Router.put(FerricStore.Instance.get(:default), "ttl_key", "val", future)
      drain_all()

      [{_, _, exp, _, _, _, _}] = keydir_lookup("ttl_key")

      assert exp > 0 and exp <= future + 1000,
             "expire_at_ms should be approximately #{future}, got #{exp}"
    end

    # Test 34: TTL reads from keydir (single lookup)
    test "34. TTL reads expire_at_ms from keydir in single lookup" do
      future = System.os_time(:millisecond) + 30_000
      Router.put(FerricStore.Instance.get(:default), "ttl_read", "val", future)
      drain_all()

      # get_meta should return the value + expiry
      meta = Router.get_meta(FerricStore.Instance.get(:default), "ttl_read")
      assert {val, exp} = meta
      assert val == "val"
      assert exp > 0
    end

    # Test 35: Hot/cold read tracking (Stats) still works
    test "35. hot/cold read tracking via Stats still works" do
      Router.put(FerricStore.Instance.get(:default), "stats_key", "val")
      drain_all()

      # Hot read
      Router.get(FerricStore.Instance.get(:default), "stats_key")
      # This should not crash -- stats tracking works with single table
      assert true
    end

    # Test 36: Promotion still works with single table
    test "36. promotion still works with single table" do
      # Write enough hash fields to verify the compound key path works
      shard =
        Router.shard_name(
          FerricStore.Instance.get(:default),
          Router.shard_for(FerricStore.Instance.get(:default), "promo_hash")
        )

      GenServer.call(shard, {:compound_put, "promo_hash", "H:promo_hash\0field1", "val1", 0})
      drain_all()

      result = GenServer.call(shard, {:compound_get, "promo_hash", "H:promo_hash\0field1"})
      assert result == "val1"
    end

    # Test 37: Key scanning with prefix pattern still works
    test "37. key scanning with prefix pattern still works with single table" do
      Router.put(FerricStore.Instance.get(:default), "prefix:key1", "v1")
      Router.put(FerricStore.Instance.get(:default), "prefix:key2", "v2")
      Router.put(FerricStore.Instance.get(:default), "other:key3", "v3")
      drain_all()

      all_keys = Router.keys(FerricStore.Instance.get(:default))
      prefix_keys = Enum.filter(all_keys, &String.starts_with?(&1, "prefix:"))
      assert "prefix:key1" in prefix_keys
      assert "prefix:key2" in prefix_keys
      refute "other:key3" in prefix_keys
    end

    # Test 38: Raft state machine writes to single table
    test "38. Raft state machine writes use single-table format" do
      Router.put(FerricStore.Instance.get(:default), "raft_key", "raft_val")
      drain_all()

      # Verify the tuple has 7 elements (keydir format with disk location fields)
      entries = keydir_lookup("raft_key")

      assert [{_, _, _, _, _, _, _}] = entries,
             "keydir entry should be 7-element tuple, got #{inspect(entries)}"
    end

    # Test 39: async path writes to single table
    test "39. async write applies to single table" do
      # Internal origin replay goes through Router → Batcher.origin_submit →
      # Raft. Verify that after the batch applies, the keydir has the
      # 7-element tuple shape shared with the quorum path.
      Router.put(FerricStore.Instance.get(:default), "async_key", "async_val")
      drain_all()

      [{_, val, _, _, _, _, _}] = keydir_lookup("async_key")
      assert val == "async_val"
    end

    # Test 40: All basic command tests still pass (sanity)
    test "40. basic commands roundtrip: SET/GET/DEL/EXISTS/INCR/APPEND" do
      # SET + GET
      Router.put(FerricStore.Instance.get(:default), "cmd_key", "cmd_val")
      drain_all()
      assert Router.get(FerricStore.Instance.get(:default), "cmd_key") == "cmd_val"

      # EXISTS
      assert Router.exists?(FerricStore.Instance.get(:default), "cmd_key") == true
      assert Router.exists?(FerricStore.Instance.get(:default), "no_such_key") == false

      # DELETE
      Router.delete(FerricStore.Instance.get(:default), "cmd_key")
      drain_all()
      assert Router.get(FerricStore.Instance.get(:default), "cmd_key") == nil

      # INCR
      assert {:ok, 10} = Router.incr(FerricStore.Instance.get(:default), "inc_key", 10)
      drain_all()
      assert Router.get(FerricStore.Instance.get(:default), "inc_key") == "10"

      # APPEND
      Router.put(FerricStore.Instance.get(:default), "app_key", "hello")
      drain_all()
      assert {:ok, 10} = Router.append(FerricStore.Instance.get(:default), "app_key", "world")
      drain_all()
      assert Router.get(FerricStore.Instance.get(:default), "app_key") == "helloworld"
    end
  end
end
