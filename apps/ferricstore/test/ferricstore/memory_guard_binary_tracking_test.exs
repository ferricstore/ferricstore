defmodule Ferricstore.MemoryGuardBinaryTrackingTest do
  @moduledoc """
  Tests that keydir binary memory tracking accurately accounts for
  off-heap refc binaries (> 64 bytes) stored in ETS keydirs.

  :ets.info(:memory) only counts tuple overhead and small inlined binaries.
  Binaries > 64 bytes are stored off-heap as refc binaries and NOT counted.
  The keydir_binary_bytes atomics counter tracks these to give MemoryGuard
  accurate memory accounting.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Store.Router

  setup do
    ctx = Ferricstore.Test.IsolatedInstance.checkout()

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
    end)

    Process.put(:test_ctx, ctx)
    :ok
  end

  defp ctx, do: Process.get(:test_ctx)

  describe "binary bytes tracking" do
    test "small values (< 64 bytes) don't add to binary counter" do
      ctx = ctx()
      ref = ctx.keydir_binary_bytes

      # Get baseline for all shards
      baseline = total_binary_bytes(ref, ctx.shard_count)

      # Write 100 small keys with small values
      for i <- 1..100 do
        Router.put(ctx, "small_#{i}", "v", 0)
      end

      after_writes = total_binary_bytes(ref, ctx.shard_count)

      # Small keys ("small_1" = 7 bytes) and small values ("v" = 1 byte)
      # are both < 64 bytes, so they should be inlined in ETS tuples.
      # Binary counter should not increase significantly.
      assert after_writes - baseline < 1000,
             "Small values should not add to binary counter, but delta was #{after_writes - baseline}"
    end

    test "large values (> 64 bytes) are tracked in binary counter" do
      ctx = ctx()
      ref = ctx.keydir_binary_bytes

      baseline = total_binary_bytes(ref, ctx.shard_count)

      # Write keys with 1KB values (well above 64 byte threshold)
      large_value = String.duplicate("x", 1000)

      for i <- 1..100 do
        Router.put(ctx, "large_#{i}", large_value, 0)
      end

      after_writes = total_binary_bytes(ref, ctx.shard_count)
      delta = after_writes - baseline

      # 100 keys × 1000 bytes = ~100KB of off-heap binary data
      # Plus key names > 64 bytes would also count, but "large_1" is only 7 bytes
      # So we expect roughly 100 * 1000 = 100,000 bytes
      assert delta >= 50_000,
             "Expected ~100KB of tracked binary bytes, got #{delta}"

      assert delta <= 200_000,
             "Binary tracking seems too high: #{delta} (expected ~100KB)"
    end

    test "deleting keys decrements binary counter" do
      ctx = ctx()
      ref = ctx.keydir_binary_bytes

      large_value = String.duplicate("x", 1000)

      # Write
      for i <- 1..50 do
        Router.put(ctx, "del_track_#{i}", large_value, 0)
      end

      after_writes = total_binary_bytes(ref, ctx.shard_count)

      # Delete all
      for i <- 1..50 do
        Router.delete(ctx, "del_track_#{i}")
      end

      after_deletes = total_binary_bytes(ref, ctx.shard_count)

      # After deleting, the counter should be back near zero
      # (relative to after_writes)
      reclaimed = after_writes - after_deletes

      assert reclaimed >= 25_000,
             "Expected ~50KB reclaimed after deleting 50 keys, got #{reclaimed}"
    end

    test "updating a key adjusts binary counter (delta, not double-count)" do
      ctx = ctx()
      ref = ctx.keydir_binary_bytes

      baseline = total_binary_bytes(ref, ctx.shard_count)

      # Write a key with a 500 byte value
      Router.put(ctx, "update_test", String.duplicate("a", 500), 0)
      _after_first = total_binary_bytes(ref, ctx.shard_count)

      # Update the same key with a 1000 byte value
      Router.put(ctx, "update_test", String.duplicate("b", 1000), 0)
      after_update = total_binary_bytes(ref, ctx.shard_count)

      # The delta from baseline should be ~1000 (the current value),
      # NOT 1500 (first + second)
      total_delta = after_update - baseline

      assert total_delta >= 600 and total_delta <= 1500,
             "Expected ~1000 bytes tracked for updated key, got #{total_delta}"
    end

    test "MemoryGuard sees binary bytes in keydir accounting" do
      ctx = ctx()

      # Write large values to make binary bytes significant
      large_value = String.duplicate("x", 10_000)

      for i <- 1..100 do
        Router.put(ctx, "mg_test_#{i}", large_value, 0)
      end

      # Check that MemoryGuard reports more than just ETS :memory
      ets_only =
        Enum.reduce(0..(ctx.shard_count - 1), 0, fn i, acc ->
          case :ets.info(:"keydir_#{i}", :memory) do
            :undefined -> acc
            mem -> acc + mem * :erlang.system_info(:wordsize)
          end
        end)

      binary_tracked = total_binary_bytes(ctx.keydir_binary_bytes, ctx.shard_count)

      # binary_tracked should be ~1MB (100 × 10KB)
      assert binary_tracked >= 500_000,
             "Expected ~1MB tracked binary bytes, got #{binary_tracked}"

      # Binary bytes should be significant relative to ETS memory overhead.
      # Under full-suite load, ETS tables accumulate entries from other tests,
      # so we only assert binary_tracked is substantial (>500KB verified above).
      assert binary_tracked > ets_only,
             "Binary bytes (#{binary_tracked}) should exceed " <>
               "ETS :memory (#{ets_only}) for large-value workloads"
    end
  end

  defp total_binary_bytes(ref, shard_count) do
    Enum.reduce(0..(shard_count - 1), 0, fn i, acc ->
      val = :atomics.get(ref, i + 1)
      acc + max(val, 0)
    end)
  end
end
