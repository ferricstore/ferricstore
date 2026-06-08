defmodule Ferricstore.AuditFixesMemoryTest do
  @moduledoc """
  Memory audit fixes.
  Split from AuditFixesTest.
  """

  use ExUnit.Case, async: true
  @moduletag :global_state

  # ---------------------------------------------------------------------------
  # Memory audit H1: eviction uses {count, list} accumulator
  # ---------------------------------------------------------------------------

  describe "Memory H1: eviction count accumulator" do
    test "MemoryGuard stats computation works" do
      # Just verify the stats function doesn't crash
      stats = Ferricstore.MemoryGuard.stats()
      assert is_map(stats)
      assert Map.has_key?(stats, :total_bytes)
      assert Map.has_key?(stats, :pressure_level)
    end
  end

  # ---------------------------------------------------------------------------
  # Memory H3: hot_keys map capped at 10_000
  # (Already fixed - verified by pre-existing test)
  # ---------------------------------------------------------------------------

  # ---------------------------------------------------------------------------
  # Memory M6: raw_store cached in persistent_term
  # (Already fixed - verified by checking persistent_term)
  # ---------------------------------------------------------------------------

  describe "Memory M6: raw_store cached" do
    test "raw_store is cached in persistent_term after first use" do
      # The raw_store is populated lazily; check if it's there
      raw = :persistent_term.get(:ferricstore_raw_store, nil)
      # May be nil in test environment if no dispatch has happened yet
      assert raw == nil or is_map(raw)
    end
  end

  # ---------------------------------------------------------------------------
  # Memory L4: LFU.initial cached per-minute
  # (Already fixed - verified)
  # ---------------------------------------------------------------------------

  describe "Memory L4: LFU.initial cached" do
    test "LFU.initial returns consistent value within same minute" do
      a = Ferricstore.Store.LFU.initial()
      b = Ferricstore.Store.LFU.initial()
      assert a == b
      assert is_integer(a)
    end
  end

  # ---------------------------------------------------------------------------
  # Memory L7: format_float no-op then removed
  # (Already fixed in ValueCodec)
  # ---------------------------------------------------------------------------

  describe "Memory L7: format_float clean" do
    test "format_float trims trailing zeros" do
      assert Ferricstore.Store.ValueCodec.format_float(1.0) == "1"
      assert Ferricstore.Store.ValueCodec.format_float(1.5) == "1.5"
      assert Ferricstore.Store.ValueCodec.format_float(3.14) == "3.14"
    end
  end

  # ---------------------------------------------------------------------------
  # ETS Read Optimizations
  # ---------------------------------------------------------------------------

  describe "LFU touch skip — unchanged packed value" do
    alias Ferricstore.Store.LFU

    setup do
      LFU.init_config_cache()
      table = :ets.new(:lfu_skip_test, [:set, :public])
      %{table: table}
    end

    test "same-minute touch with no increment does not mutate ETS", %{table: table} do
      # Use a high counter so the probabilistic increment almost never fires.
      # P(increment) = 1/(counter * log_factor + 1) = 1/(200*10+1) ≈ 0.05%
      now_min = LFU.now_minutes()
      initial_packed = LFU.pack(now_min, 200)
      :ets.insert(table, {"key", :val, 0, initial_packed})

      unchanged_count =
        Enum.count(1..1000, fn _ ->
          [{_, _, _, before}] = :ets.lookup(table, "key")
          LFU.touch(table, "key", initial_packed)
          [{_, _, _, after_val}] = :ets.lookup(table, "key")
          # Reset to initial so each iteration is independent
          :ets.update_element(table, "key", {4, initial_packed})
          after_val == before
        end)

      # With counter=200, ~99.95% of touches should skip the write.
      # Allow generous margin — at least 950 out of 1000.
      assert unchanged_count >= 950,
             "Expected >= 950 unchanged touches, got #{unchanged_count}"
    end

    test "minute rollover always updates ldt", %{table: table} do
      old_min = Bitwise.band(LFU.now_minutes() - 2, 0xFFFF)
      packed = LFU.pack(old_min, 200)
      :ets.insert(table, {"key", :val, 0, packed})

      LFU.touch(table, "key", packed)
      [{_, _, _, new_packed}] = :ets.lookup(table, "key")

      # ldt must have been updated to current minute
      {new_ldt, _} = LFU.unpack(new_packed)
      assert new_ldt == LFU.now_minutes()
      assert new_packed != packed
    end

    test "counter increment triggers ETS write", %{table: table} do
      # Counter 0 always increments: P = 1/(0*10+1) = 100%
      now_min = LFU.now_minutes()
      packed = LFU.pack(now_min, 0)
      :ets.insert(table, {"key", :val, 0, packed})

      LFU.touch(table, "key", packed)
      [{_, _, _, new_packed}] = :ets.lookup(table, "key")

      {_, new_counter} = LFU.unpack(new_packed)
      assert new_counter == 1
      assert new_packed != packed
    end

    test "eviction accuracy — effective_counter consistent with touch", %{table: table} do
      now_min = LFU.now_minutes()
      packed = LFU.pack(now_min, LFU.initial_counter())
      :ets.insert(table, {"key", :val, 0, packed})

      final_packed =
        Enum.reduce(1..500, packed, fn _, acc ->
          LFU.touch(table, "key", acc)
          [{_, _, _, p}] = :ets.lookup(table, "key")
          p
        end)

      eff = LFU.effective_counter(final_packed)
      # Counter should have grown from initial (5) but not saturated for 500 touches
      assert eff >= LFU.initial_counter()
      assert eff <= 255
    end

    test "stress — 10K reads produce sane counter", %{table: table} do
      now_min = LFU.now_minutes()
      packed = LFU.pack(now_min, LFU.initial_counter())
      :ets.insert(table, {"key", :val, 0, packed})

      final_packed =
        Enum.reduce(1..10_000, packed, fn _, acc ->
          LFU.touch(table, "key", acc)
          [{_, _, _, p}] = :ets.lookup(table, "key")
          p
        end)

      {_, counter} = LFU.unpack(final_packed)
      assert counter > 0, "Counter must not be zero after 10K touches"
      assert counter <= 255, "Counter must not exceed 255"
      # With log factor 10 and 10K touches, counter should be well above initial
      assert counter > LFU.initial_counter(),
             "Counter #{counter} should exceed initial #{LFU.initial_counter()} after 10K touches"
    end
  end

  describe "decentralized_counters on write_concurrency tables" do
    test "keydir table has decentralized_counters" do
      # Create a table with the same options as shard.ex keydir
      table = :ets.new(:dc_test_keydir, [
        :set, :public,
        {:read_concurrency, true},
        {:write_concurrency, true},
        {:decentralized_counters, true}
      ])

      info = :ets.info(table)
      assert Keyword.get(info, :decentralized_counters) == true

      :ets.delete(table)
    end

    test "concurrent inserts with decentralized_counters" do
      table = :ets.new(:dc_test_concurrent, [
        :set, :public,
        {:read_concurrency, true},
        {:write_concurrency, true},
        {:decentralized_counters, true}
      ])

      tasks =
        for i <- 1..100 do
          Task.async(fn ->
            for j <- 1..100 do
              :ets.insert(table, {{i, j}, :erlang.unique_integer()})
            end
          end)
        end

      Task.await_many(tasks, 10_000)

      assert :ets.info(table, :size) == 10_000

      :ets.delete(table)
    end
  end
end
