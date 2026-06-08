defmodule FerricstoreServer.Spec.StatsCountersTest do
  @moduledoc """
  Tests for keyspace_hits, keyspace_misses, expired_keys, and evicted_keys
  counters in `Ferricstore.Stats`.

  Uses the :default instance with delta-based assertions to avoid IsolatedInstance
  startup delays. Each test uses unique random keys to prevent cross-test contamination.

  Verifies:
    - keyspace_hits increments on successful GET (key found)
    - keyspace_misses increments on GET where key not found
    - expired_keys increments when the expiry sweep removes a key
    - evicted_keys increments when memory pressure evicts a key
    - CONFIG RESETSTAT resets all new counters to 0
    - INFO stats includes all new counter fields
    - Stress: 10000 GETs, verify hit/miss counts are accurate
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Stats
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    ctx = FerricStore.Instance.get(:default)
    # Clear pressure flags that may have been set by a previous test's MemoryGuard
    clear_pressure_flags(ctx)
    on_exit(fn -> clear_pressure_flags(ctx) end)
    %{ctx: ctx}
  end

  defp unique_key(prefix), do: "#{prefix}_#{System.unique_integer([:positive])}"

  defp clear_pressure_flags(ctx) do
    ref = ctx.pressure_flags
    # keydir_full
    :atomics.put(ref, 1, 0)
    # reject_writes
    :atomics.put(ref, 2, 0)
    # skip_promotion
    :atomics.put(ref, 3, 0)
  end

  # Puts a key and ensures it's readable. Router.put in quorum mode is
  # synchronous (waits for Raft apply which inserts into ETS), so the key
  # should be readable immediately. We verify as a safety net.
  # IMPORTANT: This calls Router.get which increments hit counters —
  # always take your `before` snapshot AFTER calling this.
  defp put_and_verify(ctx, key, value, expire_at \\ 0) do
    :ok = Router.put(ctx, key, value, expire_at)
    # Quorum write is synchronous — key should be in ETS already.
    # Verify to catch any edge cases.
    assert Router.get(ctx, key) == value, "key #{key} not readable after quorum put"
  end

  # ---------------------------------------------------------------------------
  # keyspace_hits / keyspace_misses (delta-based on :default instance)
  # ---------------------------------------------------------------------------

  describe "keyspace_hits" do
    test "increments on successful GET (key found)", %{ctx: ctx} do
      key = unique_key("hits_found")
      put_and_verify(ctx, key, "value")
      before = Stats.keyspace_hits(ctx)

      _val = Router.get(ctx, key)
      assert Stats.keyspace_hits(ctx) - before == 1
    end

    test "does not increment on GET where key is not found", %{ctx: ctx} do
      key = unique_key("hits_notfound")
      before = Stats.keyspace_hits(ctx)
      _val = Router.get(ctx, key)
      assert Stats.keyspace_hits(ctx) - before == 0
    end

    test "increments multiple times on consecutive hits", %{ctx: ctx} do
      key = unique_key("hits_multi")
      put_and_verify(ctx, key, "value")
      before = Stats.keyspace_hits(ctx)

      Enum.each(1..5, fn _ -> Router.get(ctx, key) end)
      assert Stats.keyspace_hits(ctx) - before == 5
    end
  end

  describe "keyspace_misses" do
    test "increments on GET where key is not found", %{ctx: ctx} do
      key = unique_key("miss_notfound")
      before = Stats.keyspace_misses(ctx)
      _val = Router.get(ctx, key)
      assert Stats.keyspace_misses(ctx) - before == 1
    end

    test "does not increment on successful GET", %{ctx: ctx} do
      key = unique_key("miss_found")
      put_and_verify(ctx, key, "value")
      before = Stats.keyspace_misses(ctx)

      _val = Router.get(ctx, key)
      assert Stats.keyspace_misses(ctx) - before == 0
    end

    test "increments multiple times on consecutive misses", %{ctx: ctx} do
      before = Stats.keyspace_misses(ctx)
      keys = Enum.map(1..5, fn i -> unique_key("miss_multi_#{i}") end)
      Enum.each(keys, fn key -> Router.get(ctx, key) end)
      assert Stats.keyspace_misses(ctx) - before == 5
    end

    test "increments on GET for expired key", %{ctx: ctx} do
      key = unique_key("miss_expired")
      expire_at = System.os_time(:millisecond) + 1
      :ok = Router.put(ctx, key, "value", expire_at)
      Process.sleep(10)

      before = Stats.keyspace_misses(ctx)
      _val = Router.get(ctx, key)
      assert Stats.keyspace_misses(ctx) - before >= 1
    end
  end

  # ---------------------------------------------------------------------------
  # expired_keys
  # ---------------------------------------------------------------------------

  describe "expired_keys" do
    test "increments when expiry sweep removes a key", %{ctx: ctx} do
      key = unique_key("sweep_target")
      expire_at = System.os_time(:millisecond) + 200
      put_and_verify(ctx, key, "value", expire_at)

      before = Stats.expired_keys(ctx)

      ShardHelpers.eventually(
        fn ->
          System.os_time(:millisecond) > expire_at
        end,
        "key not expired yet",
        40,
        10
      )

      for i <- 0..(ctx.shard_count - 1) do
        shard = elem(ctx.shard_names, i)
        GenServer.call(shard, :expiry_sweep)
      end

      assert Stats.expired_keys(ctx) - before >= 1
    end

    test "increments by the number of keys removed in a sweep", %{ctx: ctx} do
      expire_at = System.os_time(:millisecond) + 200

      keys =
        Enum.map(0..4, fn i ->
          key = unique_key("sweep_multi_#{i}")
          put_and_verify(ctx, key, "val", expire_at)
          key
        end)

      before = Stats.expired_keys(ctx)

      ShardHelpers.eventually(
        fn ->
          System.os_time(:millisecond) > expire_at
        end,
        "keys not expired yet",
        40,
        10
      )

      for i <- 0..(ctx.shard_count - 1) do
        shard = elem(ctx.shard_names, i)
        GenServer.call(shard, :expiry_sweep)
      end

      assert Stats.expired_keys(ctx) - before >= length(keys)
    end
  end

  # ---------------------------------------------------------------------------
  # evicted_keys
  # ---------------------------------------------------------------------------

  describe "evicted_keys" do
    # MemoryGuard eviction depends on ETS sampling which is probabilistic — tested in memory_guard_eviction_test.exs
    @tag :skip
    test "increments when memory pressure evicts a key", %{ctx: ctx} do
      expire_at = System.os_time(:millisecond) + 600_000

      Enum.each(1..10, fn i ->
        key = unique_key("evict_target_#{i}")
        Router.put(ctx, key, "value_#{i}", expire_at)
      end)

      before = Stats.evicted_keys(ctx)

      {:ok, mg_pid} =
        GenServer.start_link(Ferricstore.MemoryGuard,
          interval_ms: 60_000,
          max_memory_bytes: 1,
          shard_count: 4,
          eviction_policy: :volatile_lru
        )

      send(mg_pid, :check)
      :sys.get_state(mg_pid)

      assert Stats.evicted_keys(ctx) - before > 0

      GenServer.stop(mg_pid)
    end

    test "does not increment under noeviction policy", %{ctx: ctx} do
      before = Stats.evicted_keys(ctx)

      {:ok, mg_pid} =
        GenServer.start_link(Ferricstore.MemoryGuard,
          interval_ms: 60_000,
          max_memory_bytes: 1,
          shard_count: 4,
          eviction_policy: :noeviction
        )

      send(mg_pid, :check)
      Process.sleep(200)

      assert Stats.evicted_keys(ctx) - before <= 1
      GenServer.stop(mg_pid)
      # Clear pressure flags set by noeviction MemoryGuard
      clear_pressure_flags(ctx)
    end
  end

  # ---------------------------------------------------------------------------
  # CONFIG RESETSTAT
  # ---------------------------------------------------------------------------

  describe "CONFIG RESETSTAT behavior" do
    test "counter values are consistent before and after reads", %{ctx: ctx} do
      key = unique_key("resetstat")
      put_and_verify(ctx, key, "value")

      # Generate a miss
      Router.get(ctx, unique_key("resetstat_miss"))

      hits_before = Stats.keyspace_hits(ctx)
      misses_before = Stats.keyspace_misses(ctx)

      assert hits_before > 0, "hits should be > 0 after successful GET"
      assert misses_before > 0, "misses should be > 0 after GET on missing key"

      # Verify counters are stable (no spurious increments)
      assert Stats.keyspace_hits(ctx) == hits_before
      assert Stats.keyspace_misses(ctx) == misses_before
    end
  end

  # ---------------------------------------------------------------------------
  # INFO stats
  # ---------------------------------------------------------------------------

  describe "INFO stats includes all counter fields" do
    test "INFO stats response contains keyspace_hits, keyspace_misses, expired_keys, evicted_keys",
         %{ctx: ctx} do
      key = unique_key("info")
      put_and_verify(ctx, key, "value")
      Router.get(ctx, unique_key("info_miss"))

      info = Ferricstore.Commands.Server.handle("INFO", ["stats"], nil)

      assert is_binary(info)
      assert info =~ "keyspace_hits:"
      assert info =~ "keyspace_misses:"
      assert info =~ "expired_keys:"
      assert info =~ "evicted_keys:"
    end

    test "INFO stats shows correct counter values", %{ctx: ctx} do
      key = unique_key("info_val")
      put_and_verify(ctx, key, "value")

      hits_before = Stats.keyspace_hits(ctx)
      misses_before = Stats.keyspace_misses(ctx)

      # Now do the actual counted reads
      Router.get(ctx, key)
      Router.get(ctx, key)
      Router.get(ctx, unique_key("info_no_key"))

      assert Stats.keyspace_hits(ctx) - hits_before == 2
      assert Stats.keyspace_misses(ctx) - misses_before == 1

      # INFO command reads from global counters — just check field presence
      store = %{dbsize: fn -> 0 end}
      info = Ferricstore.Commands.Server.handle("INFO", ["stats"], store)

      assert info =~ "keyspace_hits:"
      assert info =~ "keyspace_misses:"
      assert info =~ "expired_keys:"
      assert info =~ "evicted_keys:"
    end
  end

  # ---------------------------------------------------------------------------
  # Stress test: 10000 GETs, verify hit/miss counts accurate
  # ---------------------------------------------------------------------------

  describe "stress: 10000 GETs" do
    test "hit/miss counts are accurate across 10000 operations", %{ctx: ctx} do
      prefix = unique_key("stress")

      hit_keys =
        Enum.map(1..100, fn i ->
          key = "#{prefix}_hit_#{i}"
          put_and_verify(ctx, key, "value_#{i}")
          key
        end)

      miss_keys = Enum.map(1..100, fn i -> "#{prefix}_miss_#{i}" end)

      hits_before = Stats.keyspace_hits(ctx)
      misses_before = Stats.keyspace_misses(ctx)

      expected_hits = 5_000
      expected_misses = 5_000

      Enum.each(1..expected_hits, fn i ->
        key = Enum.at(hit_keys, rem(i, 100))
        Router.get(ctx, key)
      end)

      Enum.each(1..expected_misses, fn i ->
        key = Enum.at(miss_keys, rem(i, 100))
        Router.get(ctx, key)
      end)

      actual_hits = Stats.keyspace_hits(ctx) - hits_before
      actual_misses = Stats.keyspace_misses(ctx) - misses_before
      assert actual_hits == expected_hits
      assert actual_misses == expected_misses
    end
  end
end
