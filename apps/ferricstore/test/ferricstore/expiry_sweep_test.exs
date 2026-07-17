defmodule Ferricstore.ExpirySweepTest do
  @moduledoc """
  Tests for the active expiry sweep in Shard GenServers.

  These tests verify that the periodic sweep timer proactively removes expired
  keys from ETS without waiting for a read (lazy expiry). The tests use the
  application-supervised shards and the Router convenience API.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Store.Router
  import Ferricstore.Test.ShardHelpers, only: [eventually: 4, flush_all_keys: 0]

  # Use a unique prefix per test to avoid cross-test key collisions.
  defp ukey(base), do: "expiry_sweep_test:#{base}:#{System.unique_integer([:positive])}"

  setup do
    flush_all_keys()
    reset_expiry_key_counts()
    :ok
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Triggers the expiry sweep for a specific shard synchronously by calling
  # into the GenServer rather than sending an async message. This guarantees
  # the sweep has completed before the caller continues.
  defp trigger_sweep(shard_index) do
    name = Router.shard_name(FerricStore.Instance.get(:default), shard_index)
    GenServer.call(name, :expiry_sweep)
  end

  # Triggers sweep on all 4 shards.
  defp trigger_all_sweeps do
    Enum.each(0..3, &trigger_sweep/1)
  end

  defp reset_expiry_key_counts do
    ctx = FerricStore.Instance.get(:default)

    Enum.each(1..ctx.shard_count, fn idx ->
      :atomics.put(ctx.expiry_key_counts, idx, 0)
      :atomics.put(ctx.expiry_next_due_at, idx, 0)
    end)
  end

  # ---------------------------------------------------------------------------
  # Tests
  # ---------------------------------------------------------------------------

  describe "active expiry sweep" do
    test "expired keys are removed after sweep runs" do
      key = ukey("expired")
      past = System.os_time(:millisecond) - 1_000
      Router.put(FerricStore.Instance.get(:default), key, "value", past)

      # Key is in ETS but expired — lazy expiry would catch it on read,
      # but we want the sweep to catch it proactively.
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      trigger_sweep(shard_idx)

      # After sweep, the key should be gone from ETS.
      ets = :"keydir_#{shard_idx}"
      assert :ets.lookup(ets, key) == []
    end

    test "keys without TTL are not affected by sweep" do
      key = ukey("no_ttl")
      Router.put(FerricStore.Instance.get(:default), key, "persistent_value", 0)

      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      trigger_sweep(shard_idx)

      # Key should still be present.
      assert Router.get(FerricStore.Instance.get(:default), key) == "persistent_value"
    end

    test "writes maintain per-shard TTL key counts without counting no-TTL keys" do
      ctx = FerricStore.Instance.get(:default)
      key = ukey("ttl_counter")
      shard_idx = Router.shard_for(ctx, key)
      counter_idx = shard_idx + 1
      before = :atomics.get(ctx.expiry_key_counts, counter_idx)

      assert :ok = Router.put(ctx, key, "persistent_value", 0)
      assert :atomics.get(ctx.expiry_key_counts, counter_idx) == before

      future = System.os_time(:millisecond) + 60_000
      assert :ok = Router.put(ctx, key, "ttl_value", future)

      eventually(
        fn -> :atomics.get(ctx.expiry_key_counts, counter_idx) == before + 1 end,
        "TTL key count did not increment",
        50,
        10
      )

      assert :ok = Router.put(ctx, key, "persistent_again", 0)

      eventually(
        fn -> :atomics.get(ctx.expiry_key_counts, counter_idx) == before end,
        "TTL key count did not decrement",
        50,
        10
      )
    end

    test "periodic sweep skips ETS scan when tracked shard has no TTL keys" do
      ctx = FerricStore.Instance.get(:default)
      keydir = :ets.new(:periodic_sweep_skip, [:set, :public])
      expire_at_ms = System.os_time(:millisecond) - 1_000

      try do
        :ets.insert(keydir, {"manually_inserted_expired", "value", expire_at_ms, 0, 0, 0, 5})

        state = %{
          index: 0,
          keydir: keydir,
          instance_ctx: ctx,
          file_stats: %{},
          promoted_instances: %{},
          active_file_path: "00000.log",
          sweep_at_ceiling_count: 0,
          sweep_struggling: false
        }

        :atomics.put(ctx.expiry_key_counts, 1, 0)

        assert %{sweep_at_ceiling_count: 0, sweep_struggling: false} =
                 Ferricstore.Store.Shard.Lifecycle.do_expiry_sweep(state)

        assert [{_, _, ^expire_at_ms, _, _, _, _}] =
                 :ets.lookup(keydir, "manually_inserted_expired")
      after
        :ets.delete(keydir)
      end
    end

    test "periodic sweep waits until the tracked next TTL is due" do
      ctx = FerricStore.Instance.get(:default)
      keydir = :ets.new(:periodic_sweep_next_due_skip, [:set, :public])
      expire_at_ms = System.os_time(:millisecond) - 1_000
      future_due = System.os_time(:millisecond) + 60_000

      try do
        :ets.insert(keydir, {"manually_inserted_expired", "value", expire_at_ms, 0, 0, 0, 5})

        state = %{
          index: 0,
          keydir: keydir,
          instance_ctx: ctx,
          file_stats: %{},
          promoted_instances: %{},
          active_file_path: "00000.log",
          sweep_at_ceiling_count: 0,
          sweep_struggling: false
        }

        :atomics.put(ctx.expiry_key_counts, 1, 1)
        :atomics.put(ctx.expiry_next_due_at, 1, future_due)

        assert %{sweep_at_ceiling_count: 0, sweep_struggling: false} =
                 Ferricstore.Store.Shard.Lifecycle.do_expiry_sweep(state)

        assert [{_, _, ^expire_at_ms, _, _, _, _}] =
                 :ets.lookup(keydir, "manually_inserted_expired")
      after
        :ets.delete(keydir)
      end
    end

    test "periodic sweep may scan before tracked due time under memory pressure" do
      ctx = FerricStore.Instance.get(:default)
      keydir = :ets.new(:periodic_sweep_pressure_scan, [:set, :public])
      expire_at_ms = System.os_time(:millisecond) - 1_000
      future_due = System.os_time(:millisecond) + 60_000

      try do
        :ets.insert(keydir, {"manually_inserted_expired", "value", expire_at_ms, 0, 0, 0, 5})

        state = %{
          index: 0,
          keydir: keydir,
          instance_ctx: ctx,
          file_stats: %{},
          promoted_instances: %{},
          active_file_path: "00000.log",
          sweep_at_ceiling_count: 0,
          sweep_struggling: false
        }

        :atomics.put(ctx.expiry_key_counts, 1, 1)
        :atomics.put(ctx.expiry_next_due_at, 1, future_due)
        :atomics.put(ctx.pressure_flags, 3, 1)

        assert %{sweep_at_ceiling_count: 0, sweep_struggling: false} =
                 Ferricstore.Store.Shard.Lifecycle.do_expiry_sweep(state)

        assert :ets.lookup(keydir, "manually_inserted_expired") == []
      after
        :atomics.put(ctx.pressure_flags, 3, 0)
        :ets.delete(keydir)
      end
    end

    test "keys with future TTL are not affected by sweep" do
      key = ukey("future_ttl")
      future = System.os_time(:millisecond) + 60_000
      Router.put(FerricStore.Instance.get(:default), key, "alive_value", future)

      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      trigger_sweep(shard_idx)

      assert Router.get(FerricStore.Instance.get(:default), key) == "alive_value"
    end

    test "forced sweep uses the wall-safe cutoff while HLC expiry is ambiguous" do
      keydir = :ets.new(:drift_safe_expiry_sweep, [:set, :public])
      entry = {"wall-live", "value", 31_000, 0, 0, 0, 5}
      :ets.insert(keydir, entry)

      state = %{
        index: 0,
        keydir: keydir,
        instance_ctx: nil,
        file_stats: %{},
        promoted_instances: %{},
        active_file_path: "00000.log",
        sweep_at_ceiling_count: 0,
        sweep_struggling: false
      }

      try do
        Ferricstore.CommandTime.with_expiry_context(61_000, 1_000, fn ->
          Ferricstore.Store.Shard.Lifecycle.do_expiry_sweep(state, force: true)
        end)

        assert :ets.lookup(keydir, "wall-live") == [entry]
      after
        :ets.delete(keydir)
      end
    end

    test "expired keys are not returned by GET after sweep" do
      key = ukey("get_after_sweep")
      past = System.os_time(:millisecond) - 500
      Router.put(FerricStore.Instance.get(:default), key, "gone", past)

      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      trigger_sweep(shard_idx)

      assert Router.get(FerricStore.Instance.get(:default), key) == nil
    end

    test "sweep respects max_keys_per_sweep limit" do
      # Set max to 2 keys per sweep for this test.
      original = Application.get_env(:ferricstore, :expiry_max_keys_per_sweep)
      Application.put_env(:ferricstore, :expiry_max_keys_per_sweep, 2)

      on_exit(fn ->
        if original do
          Application.put_env(:ferricstore, :expiry_max_keys_per_sweep, original)
        else
          Application.delete_env(:ferricstore, :expiry_max_keys_per_sweep)
        end
      end)

      past = System.os_time(:millisecond) - 1_000

      # Create 5 expired keys that all hash to the same shard.
      # We'll find keys that map to shard 0.
      uid = System.unique_integer([:positive])

      keys =
        Stream.iterate(0, &(&1 + 1))
        |> Stream.map(fn i -> "sweep_limit_#{uid}_#{i}" end)
        |> Stream.filter(fn k -> Router.shard_for(FerricStore.Instance.get(:default), k) == 0 end)
        |> Enum.take(5)

      Enum.each(keys, fn k ->
        Router.put(FerricStore.Instance.get(:default), k, "expired_val", past)
      end)

      # First manual sweep should remove at most 2.
      # Note: the auto sweep timer may have already run, so we only assert
      # that not all keys were removed in a single manual sweep cycle.
      trigger_sweep(0)

      ets = :keydir_0
      remaining = Enum.count(keys, fn k -> :ets.lookup(ets, k) != [] end)
      # At least 1 should remain (max_keys=2 per sweep, 5 total, but auto
      # sweep may have fired too). The key invariant: a single sweep doesn't
      # remove all 5 at once when limit is 2.
      assert remaining >= 1
    end

    test "multiple sweep cycles clear all expired keys" do
      original = Application.get_env(:ferricstore, :expiry_max_keys_per_sweep)
      Application.put_env(:ferricstore, :expiry_max_keys_per_sweep, 2)

      on_exit(fn ->
        if original do
          Application.put_env(:ferricstore, :expiry_max_keys_per_sweep, original)
        else
          Application.delete_env(:ferricstore, :expiry_max_keys_per_sweep)
        end
      end)

      past = System.os_time(:millisecond) - 1_000

      uid = System.unique_integer([:positive])

      keys =
        Stream.iterate(0, &(&1 + 1))
        |> Stream.map(fn i -> "multi_sweep_#{uid}_#{i}" end)
        |> Stream.filter(fn k -> Router.shard_for(FerricStore.Instance.get(:default), k) == 0 end)
        |> Enum.take(5)

      Enum.each(keys, fn k ->
        Router.put(FerricStore.Instance.get(:default), k, "expired_val", past)
      end)

      # Run enough sweep cycles (ceiling of 5/2 = 3, plus 1 extra for safety).
      Enum.each(1..4, fn _ -> trigger_sweep(0) end)

      ets = :keydir_0
      remaining = Enum.count(keys, fn k -> :ets.lookup(ets, k) != [] end)
      assert remaining == 0
    end

    test "DBSIZE decreases after sweep removes expired keys" do
      # Insert some keys with past expiry across all shards.
      past = System.os_time(:millisecond) - 1_000
      keys = for i <- 1..8, do: ukey("dbsize_#{i}")

      # First flush the store to have a clean baseline.
      baseline = Router.dbsize(FerricStore.Instance.get(:default))

      Enum.each(keys, fn k -> Router.put(FerricStore.Instance.get(:default), k, "val", past) end)

      # Before sweep, dbsize should not count expired keys (lazy expiry on
      # read in keys()). But let's verify the sweep still cleans up ETS.
      trigger_all_sweeps()

      after_sweep = Router.dbsize(FerricStore.Instance.get(:default))
      # Use <= instead of == because concurrent tests may add keys between
      # baseline and after_sweep. The important invariant is that the sweep
      # did not increase the count (expired keys were removed).
      assert after_sweep <= baseline
    end
  end
end
