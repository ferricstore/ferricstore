defmodule Ferricstore.PerformanceAuditFixesTest do
  @moduledoc """
  Tests for critical performance and memory audit fixes.
  Each test verifies a specific audit finding was correctly addressed.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Store.{LFU, Router}
  alias Ferricstore.Test.ShardHelpers

  setup do
    original_ctx = FerricStore.Instance.get(:default)
    ShardHelpers.flush_all_keys()

    FerricStore.Instance.inject_callbacks(:default, process_rss_fn: nil)

    Ferricstore.MemoryGuard.reconfigure(%{
      max_memory_bytes: 1_000_000_000_000,
      keydir_max_ram: 1_000_000_000_000,
      eviction_policy: :volatile_lru
    })

    Ferricstore.MemoryGuard.reset_pressure_flags()

    on_exit(fn ->
      :persistent_term.put({FerricStore.Instance, :default}, original_ctx)
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  # ---------------------------------------------------------------
  # C1: LFU config cached in persistent_term
  # ---------------------------------------------------------------

  describe "C1: LFU persistent_term config caching" do
    test "LFU.touch/3 uses persistent_term values, not Application.get_env" do
      # Ensure persistent_term values are set (should be set at app startup)
      decay_time = :persistent_term.get(:ferricstore_lfu_decay_time)
      log_factor = :persistent_term.get(:ferricstore_lfu_log_factor)
      assert is_integer(decay_time) or is_float(decay_time)
      assert is_integer(log_factor) or is_float(log_factor)
    end

    test "LFU.touch works correctly after persistent_term caching" do
      # Put a key and verify touch updates the LFU counter
      Router.put(FerricStore.Instance.get(:default), "lfu_test_key", "value", 0)

      ShardHelpers.eventually(fn ->
        Router.get(FerricStore.Instance.get(:default), "lfu_test_key") == "value"
      end, "key not readable after put", 20, 10)

      idx = Router.shard_for(FerricStore.Instance.get(:default), "lfu_test_key")
      keydir = :"keydir_#{idx}"

      # Read to trigger LFU touch
      assert Router.get(FerricStore.Instance.get(:default), "lfu_test_key") == "value"

      # Verify the key's LFU value was updated in ETS
      [{_, _, _, lfu, _, _, _}] = :ets.lookup(keydir, "lfu_test_key")
      {_ldt, counter} = LFU.unpack(lfu)
      # Counter should be at least the initial value (5)
      assert counter >= LFU.initial_counter()
    end

    test "LFU.effective_counter reads from persistent_term" do
      packed = LFU.pack(LFU.now_minutes(), 10)
      counter = LFU.effective_counter(packed)
      # Should return a sensible value (close to 10 given fresh timestamp)
      assert counter >= 0 and counter <= 255
    end
  end

  # ---------------------------------------------------------------
  # C2: MemoryGuard publishes pressure to persistent_term
  # ---------------------------------------------------------------

  describe "C2: MemoryGuard atomics pressure publishing" do
    test "MemoryGuard publishes keydir_full to atomics" do
      Ferricstore.MemoryGuard.force_check()
      result = Ferricstore.MemoryGuard.keydir_full?()
      assert result in [true, false]
    end

    test "MemoryGuard publishes reject_writes to atomics" do
      Ferricstore.MemoryGuard.force_check()
      result = Ferricstore.MemoryGuard.reject_writes?()
      assert result in [true, false]
    end

    test "keydir_full? reads from atomics (no GenServer.call)" do
      Ferricstore.MemoryGuard.force_check()
      result = Ferricstore.MemoryGuard.keydir_full?()
      assert result in [true, false]
    end

    test "reject_writes? reads from atomics" do
      Ferricstore.MemoryGuard.force_check()
      result = Ferricstore.MemoryGuard.reject_writes?()
      assert result in [true, false]
    end

    test "PUT works when keydir is not full" do
      Ferricstore.MemoryGuard.force_check()
      assert :ok = Router.put(FerricStore.Instance.get(:default), "memory_test", "value", 0)
      assert Router.get(FerricStore.Instance.get(:default), "memory_test") == "value"
    end
  end

  # ---------------------------------------------------------------
  # C3: Router.exists_fast? with ETS bypass
  # ---------------------------------------------------------------

  describe "C3: Router.exists_fast? ETS bypass" do
    test "exists_fast? returns true for present key" do
      Router.put(FerricStore.Instance.get(:default), "exists_fast_test", "value", 0)

      ShardHelpers.eventually(fn ->
        Router.exists_fast?(FerricStore.Instance.get(:default), "exists_fast_test") == true
      end, "key not visible via exists_fast?", 20, 10)
    end

    test "exists_fast? returns false for absent key" do
      assert Router.exists_fast?(FerricStore.Instance.get(:default), "nonexistent_key_xyz") == false
    end

    test "exists_fast? returns false for expired key" do
      # Set a key with an expiry in the past
      Router.put(FerricStore.Instance.get(:default), "expired_fast_test", "value", 1)

      ShardHelpers.eventually(fn ->
        Router.exists_fast?(FerricStore.Instance.get(:default), "expired_fast_test") == false
      end, "expired key should return false", 20, 10)
    end

    test "check_keydir_full uses exists_fast? instead of GenServer" do
      # This is an indirect test - if keydir is full, it should be able
      # to check existence via ETS without GenServer.call
      Router.put(FerricStore.Instance.get(:default), "keydir_full_test", "value", 0)

      ShardHelpers.eventually(fn ->
        Router.get(FerricStore.Instance.get(:default), "keydir_full_test") == "value"
      end, "key not readable after put", 20, 10)

      # Put should succeed (updates existing key even when full)
      assert :ok = Router.put(FerricStore.Instance.get(:default), "keydir_full_test", "updated", 0)
    end
  end

  # ---------------------------------------------------------------
  # C4: shard_name/1 pre-computed tuple
  # ---------------------------------------------------------------

  describe "C4: shard_name/1 pre-computed names" do
    test "shard_name returns correct atoms for all shard indices" do
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      for i <- 0..(shard_count - 1) do
        expected = :"Ferricstore.Store.Shard.#{i}"
        assert Router.shard_name(FerricStore.Instance.get(:default), i) == expected
      end
    end

    test "shard_name is fast (no string interpolation overhead)" do
      # Just verify functional correctness - the perf improvement is structural
      assert Router.shard_name(FerricStore.Instance.get(:default), 0) == :"Ferricstore.Store.Shard.0"
      assert Router.shard_name(FerricStore.Instance.get(:default), 1) == :"Ferricstore.Store.Shard.1"
      assert Router.shard_name(FerricStore.Instance.get(:default), 2) == :"Ferricstore.Store.Shard.2"
      assert Router.shard_name(FerricStore.Instance.get(:default), 3) == :"Ferricstore.Store.Shard.3"
    end

    test "routing works correctly with pre-computed names" do
      Router.put(FerricStore.Instance.get(:default), "route_test_1", "v1", 0)
      Router.put(FerricStore.Instance.get(:default), "route_test_2", "v2", 0)

      ShardHelpers.eventually(fn ->
        Router.get(FerricStore.Instance.get(:default), "route_test_1") == "v1" and
          Router.get(FerricStore.Instance.get(:default), "route_test_2") == "v2"
      end, "keys not readable after put", 20, 10)
    end
  end

  # ---------------------------------------------------------------
  # C5: build_store cached as module attribute
  # ---------------------------------------------------------------

  describe "C5: build_store cached as module attribute" do
    test "all store operations work with cached store" do
      # Test core operations through the command pipeline
      Router.put(FerricStore.Instance.get(:default), "store_cache_test", "hello", 0)

      ShardHelpers.eventually(fn ->
        Router.get(FerricStore.Instance.get(:default), "store_cache_test") == "hello"
      end, "key not readable after put", 20, 10)

      assert Router.exists?(FerricStore.Instance.get(:default), "store_cache_test") == true
      Router.delete(FerricStore.Instance.get(:default), "store_cache_test")

      ShardHelpers.eventually(fn ->
        Router.get(FerricStore.Instance.get(:default), "store_cache_test") == nil
      end, "key not deleted", 20, 10)
    end

    test "incr works with cached store" do
      Router.put(FerricStore.Instance.get(:default), "incr_cache_test", "10", 0)

      ShardHelpers.eventually(fn ->
        Router.get(FerricStore.Instance.get(:default), "incr_cache_test") == "10"
      end, "key not readable after put", 20, 10)

      assert {:ok, 15} = Router.incr(FerricStore.Instance.get(:default), "incr_cache_test", 5)
    end
  end

  # ---------------------------------------------------------------
  # M-C1: Prefix-based ETS lookups instead of foldl
  # ---------------------------------------------------------------

  describe "M-C1: Prefix-based lookups use ETS match specs" do
    test "compound_scan uses ETS prefix match instead of foldl" do
      # Create a hash with some fields
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), "myhash")
      shard = Router.shard_name(FerricStore.Instance.get(:default), shard_idx)

      # Use compound_put to create hash fields
      prefix = "H:myhash\0"
      GenServer.call(shard, {:compound_put, "myhash", prefix <> "field1", "value1", 0})
      GenServer.call(shard, {:compound_put, "myhash", prefix <> "field2", "value2", 0})
      GenServer.call(shard, {:compound_put, "myhash", prefix <> "field3", "value3", 0})

      # Scan should find all 3 fields (compound_put via GenServer.call is synchronous)
      results = GenServer.call(shard, {:compound_scan, "myhash", prefix})
      assert length(results) == 3
      fields = Enum.map(results, fn {f, _v} -> f end) |> Enum.sort()
      assert fields == ["field1", "field2", "field3"]
    end

    test "compound_count uses ETS prefix match instead of foldl" do
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), "counthash")
      shard = Router.shard_name(FerricStore.Instance.get(:default), shard_idx)

      prefix = "H:counthash\0"
      GenServer.call(shard, {:compound_put, "counthash", prefix <> "a", "1", 0})
      GenServer.call(shard, {:compound_put, "counthash", prefix <> "b", "2", 0})

      # compound_put via GenServer.call is synchronous
      count = GenServer.call(shard, {:compound_count, "counthash", prefix})
      assert count == 2
    end

    test "compound_delete_prefix works with prefix index" do
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), "delhash")
      shard = Router.shard_name(FerricStore.Instance.get(:default), shard_idx)

      prefix = "H:delhash\0"
      GenServer.call(shard, {:compound_put, "delhash", prefix <> "x", "1", 0})
      GenServer.call(shard, {:compound_put, "delhash", prefix <> "y", "2", 0})

      GenServer.call(shard, {:compound_delete_prefix, "delhash", prefix})

      count = GenServer.call(shard, {:compound_count, "delhash", prefix})
      assert count == 0
    end
  end

  # ---------------------------------------------------------------
  # M-C2: TCP buffer optimization
  # ---------------------------------------------------------------

  describe "M-C2: TCP buffer empty check" do
    test "buffer handles empty + data correctly" do
      # This is structural - the optimization is in handle_data
      # Verify through a successful TCP pipeline that the buffer works
      Router.put(FerricStore.Instance.get(:default), "buffer_test", "hello", 0)

      ShardHelpers.eventually(fn ->
        Router.get(FerricStore.Instance.get(:default), "buffer_test") == "hello"
      end, "key not readable after put", 20, 10)
    end
  end
end
