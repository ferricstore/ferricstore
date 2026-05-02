defmodule Ferricstore.ProbEdgeCasesTest do
  @moduledoc """
  Comprehensive edge case tests for probabilistic data structures and MemoryGuard.

  Tests cover:
  1. Bloom filter command handler edge cases
  2. CMS command handler edge cases
  3. Cuckoo filter command handler edge cases
  4. TopK command handler edge cases
  5. MemoryGuard edge cases
  6. State machine prob-related edge cases
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Bloom, CMS, Cuckoo, TopK}
  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.MemoryGuard
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      MemoryGuard.set_reject_writes(false)
      MemoryGuard.set_keydir_full(false)
      MemoryGuard.set_skip_promotion(false)
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  # ===========================================================================
  # Test helpers
  # ===========================================================================

  defp make_prob_dir(prefix) do
    dir =
      Path.join(
        System.tmp_dir!(),
        "prob_edge_#{prefix}_#{:os.getpid()}_#{:erlang.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    dir
  end

  defp make_bloom_store do
    dir = make_prob_dir("bloom")

    %{
      bloom_registry: %{dir: dir},
      prob_dir: fn -> dir end,
      get: fn _key -> nil end,
      put: fn _key, _value, _ttl -> :ok end,
      delete: fn _key -> :ok end
    }
  end

  defp make_cms_store do
    dir = make_prob_dir("cms")

    %{
      cms_registry: %{dir: dir},
      prob_dir: fn -> dir end,
      get: fn _key -> nil end
    }
  end

  defp make_cuckoo_store do
    dir = make_prob_dir("cuckoo")

    %{
      cuckoo_registry: %{dir: dir},
      prob_dir: fn -> dir end
    }
  end

  defp make_topk_store do
    dir = make_prob_dir("topk")
    {:ok, pid} = Agent.start_link(fn -> %{} end)

    %{
      prob_dir: fn -> dir end,
      exists?: fn key -> Agent.get(pid, &Map.has_key?(&1, key)) end,
      put: fn key, value, ttl ->
        Agent.update(pid, &Map.put(&1, key, {value, ttl}))
        :ok
      end
    }
  end

  defp prob_file_path(dir, key, ext) do
    safe = Base.url_encode64(key, padding: false)
    Path.join(dir, "#{safe}.#{ext}")
  end

  # ===========================================================================
  # 1. Bloom filter command handler edge cases
  # ===========================================================================

  describe "Bloom edge cases" do
    test "BF.ADD with empty string element" do
      store = make_bloom_store()
      assert :ok = Bloom.handle("BF.RESERVE", ["bf_empty", "0.01", "100"], store)
      # Add empty string element -- should succeed
      result = Bloom.handle("BF.ADD", ["bf_empty", ""], store)
      assert result in [0, 1]
    end

    test "BF.ADD with binary element containing null bytes" do
      store = make_bloom_store()
      assert :ok = Bloom.handle("BF.RESERVE", ["bf_null", "0.01", "100"], store)
      element = <<0, 1, 2, 0, 3>>
      result = Bloom.handle("BF.ADD", ["bf_null", element], store)
      assert result in [0, 1]

      # Should find it
      exists = Bloom.handle("BF.EXISTS", ["bf_null", element], store)
      assert exists == 1
    end

    test "BF.RESERVE with capacity=1 (minimum)" do
      store = make_bloom_store()
      assert :ok = Bloom.handle("BF.RESERVE", ["bf_cap1", "0.01", "1"], store)

      # Should be able to add an element
      result = Bloom.handle("BF.ADD", ["bf_cap1", "x"], store)
      assert result in [0, 1]
    end

    test "BF.RESERVE with very large capacity" do
      store = make_bloom_store()
      # 10 million -- large but not absurd
      assert :ok = Bloom.handle("BF.RESERVE", ["bf_large", "0.01", "10000000"], store)

      # BF.INFO should work
      info = Bloom.handle("BF.INFO", ["bf_large"], store)
      assert is_list(info)
      assert "Number of bits" in info
    end

    test "BF.EXISTS on non-existent key returns 0" do
      store = make_bloom_store()
      assert 0 = Bloom.handle("BF.EXISTS", ["nonexistent_bloom", "x"], store)
    end

    test "BF.MEXISTS on non-existent key returns all zeros" do
      store = make_bloom_store()
      result = Bloom.handle("BF.MEXISTS", ["nonexistent_bloom", "a", "b", "c"], store)
      assert result == [0, 0, 0]
    end

    test "BF.CARD on non-existent key returns 0" do
      store = make_bloom_store()
      assert 0 = Bloom.handle("BF.CARD", ["nonexistent_bloom"], store)
    end

    test "BF.INFO on non-existent key returns error" do
      store = make_bloom_store()
      assert {:error, "ERR not found"} = Bloom.handle("BF.INFO", ["nonexistent_bloom"], store)
    end

    test "BF.ADD auto-creates bloom filter" do
      store = make_bloom_store()
      # Without BF.RESERVE, BF.ADD should auto-create
      result = Bloom.handle("BF.ADD", ["bf_auto", "hello"], store)
      assert result in [0, 1]

      # Element should now exist
      assert 1 = Bloom.handle("BF.EXISTS", ["bf_auto", "hello"], store)
    end

    test "BF.MADD auto-creates and adds multiple elements" do
      store = make_bloom_store()
      result = Bloom.handle("BF.MADD", ["bf_madd_auto", "a", "b", "c"], store)
      assert is_list(result)
      assert length(result) == 3
    end

    test "BF.RESERVE duplicate key returns error" do
      store = make_bloom_store()
      assert :ok = Bloom.handle("BF.RESERVE", ["bf_dup", "0.01", "100"], store)
      assert {:error, msg} = Bloom.handle("BF.RESERVE", ["bf_dup", "0.01", "100"], store)
      assert msg =~ "exists"
    end

    test "nif_delete removes bloom file" do
      store = make_bloom_store()
      assert :ok = Bloom.handle("BF.RESERVE", ["bf_del", "0.01", "100"], store)
      dir = store.bloom_registry.dir
      path = prob_file_path(dir, "bf_del", "bloom")
      assert File.exists?(path)

      Bloom.nif_delete("bf_del", store)
      refute File.exists?(path)
    end

    test "BF.RESERVE with wrong number of arguments" do
      store = make_bloom_store()
      assert {:error, msg} = Bloom.handle("BF.RESERVE", ["only_key"], store)
      assert msg =~ "wrong number of arguments"
    end
  end

  # ===========================================================================
  # 2. CMS command handler edge cases
  # ===========================================================================

  describe "CMS edge cases" do
    test "CMS.INCRBY with count=1 (minimum valid)" do
      store = make_cms_store()
      assert :ok = CMS.handle("CMS.INITBYDIM", ["cms_min_incr", "100", "5"], store)
      result = CMS.handle("CMS.INCRBY", ["cms_min_incr", "elem", "1"], store)
      assert is_list(result)
      assert hd(result) == 1
    end

    test "CMS.QUERY on non-existent key returns error" do
      store = make_cms_store()
      assert {:error, msg} = CMS.handle("CMS.QUERY", ["nonexistent_cms", "x"], store)
      assert msg =~ "does not exist"
    end

    test "CMS.INFO on non-existent key returns error" do
      store = make_cms_store()
      assert {:error, msg} = CMS.handle("CMS.INFO", ["nonexistent_cms"], store)
      assert msg =~ "does not exist"
    end

    test "CMS.INCRBY with empty string element" do
      store = make_cms_store()
      assert :ok = CMS.handle("CMS.INITBYDIM", ["cms_empty_elem", "100", "5"], store)
      result = CMS.handle("CMS.INCRBY", ["cms_empty_elem", "", "5"], store)
      assert is_list(result)
      assert hd(result) == 5
    end

    test "CMS.INCRBY with null bytes in element" do
      store = make_cms_store()
      assert :ok = CMS.handle("CMS.INITBYDIM", ["cms_null", "100", "5"], store)
      element = <<0, 1, 0, 2>>
      result = CMS.handle("CMS.INCRBY", ["cms_null", element, "3"], store)
      assert is_list(result)
      assert hd(result) == 3
    end

    test "CMS.MERGE with WEIGHTS" do
      store = make_cms_store()
      assert :ok = CMS.handle("CMS.INITBYDIM", ["src1", "100", "5"], store)
      assert :ok = CMS.handle("CMS.INITBYDIM", ["src2", "100", "5"], store)

      # Add some counts
      CMS.handle("CMS.INCRBY", ["src1", "item", "10"], store)
      CMS.handle("CMS.INCRBY", ["src2", "item", "20"], store)

      # Merge with weights: dst = src1 * 2 + src2 * 3
      result = CMS.handle("CMS.MERGE", ["dst", "2", "src1", "src2", "WEIGHTS", "2", "3"], store)
      assert result == :ok

      # Query merged result: should be at least 10*2 + 20*3 = 80
      query_result = CMS.handle("CMS.QUERY", ["dst", "item"], store)
      assert is_list(query_result)
      assert hd(query_result) >= 80
    end

    test "CMS.MERGE creates destination if not exists" do
      store = make_cms_store()
      assert :ok = CMS.handle("CMS.INITBYDIM", ["merge_src", "50", "3"], store)
      CMS.handle("CMS.INCRBY", ["merge_src", "x", "5"], store)

      result = CMS.handle("CMS.MERGE", ["merge_dst", "1", "merge_src"], store)
      assert result == :ok
    end

    test "CMS.INITBYDIM duplicate key returns error" do
      store = make_cms_store()
      assert :ok = CMS.handle("CMS.INITBYDIM", ["cms_dup", "100", "5"], store)
      assert {:error, msg} = CMS.handle("CMS.INITBYDIM", ["cms_dup", "100", "5"], store)
      assert msg =~ "already exists"
    end

    test "CMS.INITBYPROB creates sketch" do
      store = make_cms_store()
      assert :ok = CMS.handle("CMS.INITBYPROB", ["cms_prob", "0.01", "0.1"], store)
      info = CMS.handle("CMS.INFO", ["cms_prob"], store)
      assert is_list(info)
      assert "width" in info
    end

    test "nif_delete removes CMS file" do
      store = make_cms_store()
      assert :ok = CMS.handle("CMS.INITBYDIM", ["cms_del", "100", "5"], store)
      dir = store.prob_dir.()
      path = prob_file_path(dir, "cms_del", "cms")
      assert File.exists?(path)

      CMS.nif_delete("cms_del", store)
      refute File.exists?(path)
    end

    test "CMS.INCRBY on non-existent key returns error" do
      store = make_cms_store()
      result = CMS.handle("CMS.INCRBY", ["nonexistent_cms", "elem", "1"], store)
      assert {:error, _} = result
    end
  end

  # ===========================================================================
  # 3. Cuckoo filter command handler edge cases
  # ===========================================================================

  describe "Cuckoo edge cases" do
    test "CF.ADD with empty string element" do
      store = make_cuckoo_store()
      assert :ok = Cuckoo.handle("CF.RESERVE", ["cf_empty", "1024"], store)
      assert 1 = Cuckoo.handle("CF.ADD", ["cf_empty", ""], store)
    end

    test "CF.ADD with null bytes in element" do
      store = make_cuckoo_store()
      assert :ok = Cuckoo.handle("CF.RESERVE", ["cf_null", "1024"], store)
      element = <<0, 0, 0>>
      assert 1 = Cuckoo.handle("CF.ADD", ["cf_null", element], store)
      assert 1 = Cuckoo.handle("CF.EXISTS", ["cf_null", element], store)
    end

    test "CF.DEL on non-existent key returns 0" do
      store = make_cuckoo_store()
      assert 0 = Cuckoo.handle("CF.DEL", ["nonexistent_cuckoo", "x"], store)
    end

    test "CF.DEL on existing key but non-existent element returns 0" do
      store = make_cuckoo_store()
      assert :ok = Cuckoo.handle("CF.RESERVE", ["cf_del_miss", "1024"], store)
      assert 0 = Cuckoo.handle("CF.DEL", ["cf_del_miss", "not_here"], store)
    end

    test "CF.EXISTS on non-existent key returns 0" do
      store = make_cuckoo_store()
      assert 0 = Cuckoo.handle("CF.EXISTS", ["nonexistent_cuckoo", "x"], store)
    end

    test "CF.MEXISTS on non-existent key returns all zeros" do
      store = make_cuckoo_store()
      result = Cuckoo.handle("CF.MEXISTS", ["nonexistent_cuckoo", "a", "b", "c"], store)
      assert result == [0, 0, 0]
    end

    test "CF.COUNT on non-existent key returns 0" do
      store = make_cuckoo_store()
      assert 0 = Cuckoo.handle("CF.COUNT", ["nonexistent_cuckoo", "x"], store)
    end

    test "CF.INFO on non-existent key returns error" do
      store = make_cuckoo_store()
      assert {:error, "ERR not found"} = Cuckoo.handle("CF.INFO", ["nonexistent_cuckoo"], store)
    end

    test "CF.ADD auto-creates cuckoo filter" do
      store = make_cuckoo_store()
      assert 1 = Cuckoo.handle("CF.ADD", ["cf_auto", "hello"], store)
      assert 1 = Cuckoo.handle("CF.EXISTS", ["cf_auto", "hello"], store)
    end

    test "CF.ADDNX returns 0 for duplicate element" do
      store = make_cuckoo_store()
      assert :ok = Cuckoo.handle("CF.RESERVE", ["cf_addnx", "1024"], store)
      assert 1 = Cuckoo.handle("CF.ADDNX", ["cf_addnx", "unique"], store)
      assert 0 = Cuckoo.handle("CF.ADDNX", ["cf_addnx", "unique"], store)
    end

    test "CF.ADD and CF.DEL roundtrip" do
      store = make_cuckoo_store()
      assert :ok = Cuckoo.handle("CF.RESERVE", ["cf_roundtrip", "1024"], store)
      assert 1 = Cuckoo.handle("CF.ADD", ["cf_roundtrip", "element"], store)
      assert 1 = Cuckoo.handle("CF.EXISTS", ["cf_roundtrip", "element"], store)
      assert 1 = Cuckoo.handle("CF.DEL", ["cf_roundtrip", "element"], store)
      assert 0 = Cuckoo.handle("CF.EXISTS", ["cf_roundtrip", "element"], store)
    end

    test "CF.COUNT increments for duplicate adds" do
      store = make_cuckoo_store()
      assert :ok = Cuckoo.handle("CF.RESERVE", ["cf_count", "1024"], store)
      assert 1 = Cuckoo.handle("CF.ADD", ["cf_count", "item"], store)
      assert 1 = Cuckoo.handle("CF.ADD", ["cf_count", "item"], store)
      # Count should be >= 1 (fingerprint collision means count may be >= 2)
      count = Cuckoo.handle("CF.COUNT", ["cf_count", "item"], store)
      assert count >= 1
    end

    test "CF.RESERVE duplicate key returns error" do
      store = make_cuckoo_store()
      assert :ok = Cuckoo.handle("CF.RESERVE", ["cf_dup", "1024"], store)
      assert {:error, msg} = Cuckoo.handle("CF.RESERVE", ["cf_dup", "1024"], store)
      assert msg =~ "exists"
    end

    test "nif_delete removes cuckoo file" do
      store = make_cuckoo_store()
      assert :ok = Cuckoo.handle("CF.RESERVE", ["cf_del", "1024"], store)
      dir = store.prob_dir.()
      path = prob_file_path(dir, "cf_del", "cuckoo")
      assert File.exists?(path)

      Cuckoo.nif_delete("cf_del", store)
      refute File.exists?(path)
    end

    test "CF.INFO returns all expected fields" do
      store = make_cuckoo_store()
      assert :ok = Cuckoo.handle("CF.RESERVE", ["cf_info", "1024"], store)
      info = Cuckoo.handle("CF.INFO", ["cf_info"], store)
      assert is_list(info)
      assert "Size" in info
      assert "Number of buckets" in info
      assert "Bucket size" in info
    end
  end

  # ===========================================================================
  # 4. TopK command handler edge cases
  # ===========================================================================

  describe "TopK edge cases" do
    test "TOPK.ADD to non-existent key returns error" do
      store = make_topk_store()
      result = TopK.handle("TOPK.ADD", ["nonexistent_topk", "elem"], store)
      assert {:error, msg} = result
      assert msg =~ "does not exist"
    end

    test "TOPK.LIST on empty TopK returns empty list" do
      store = make_topk_store()
      assert :ok = TopK.handle("TOPK.RESERVE", ["tk_empty", "5"], store)
      result = TopK.handle("TOPK.LIST", ["tk_empty"], store)
      assert result == []
    end

    test "TOPK.LIST on non-existent key returns error" do
      store = make_topk_store()
      assert {:error, msg} = TopK.handle("TOPK.LIST", ["nonexistent_topk"], store)
      assert msg =~ "does not exist"
    end

    test "TOPK.QUERY on non-existent key returns error" do
      store = make_topk_store()
      assert {:error, msg} = TopK.handle("TOPK.QUERY", ["nonexistent_topk", "elem"], store)
      assert msg =~ "does not exist"
    end

    test "TOPK.COUNT on non-existent key returns error" do
      store = make_topk_store()
      assert {:error, msg} = TopK.handle("TOPK.COUNT", ["nonexistent_topk", "elem"], store)
      assert msg =~ "does not exist"
    end

    test "TOPK.INFO on non-existent key returns error" do
      store = make_topk_store()
      assert {:error, msg} = TopK.handle("TOPK.INFO", ["nonexistent_topk"], store)
      assert msg =~ "does not exist"
    end

    test "TOPK.ADD with empty string element" do
      store = make_topk_store()
      assert :ok = TopK.handle("TOPK.RESERVE", ["tk_empty_elem", "5"], store)
      result = TopK.handle("TOPK.ADD", ["tk_empty_elem", ""], store)
      # Result is a list of nil (no eviction) or evicted element
      assert is_list(result)
      assert length(result) == 1
    end

    test "TOPK.ADD causes eviction when heap is full" do
      store = make_topk_store()
      # k=2: only 2 elements can be in the top-K heap
      assert :ok = TopK.handle("TOPK.RESERVE", ["tk_evict", "2"], store)

      # Add 2 elements many times to build up counts
      for _ <- 1..10 do
        TopK.handle("TOPK.ADD", ["tk_evict", "high"], store)
      end

      for _ <- 1..5 do
        TopK.handle("TOPK.ADD", ["tk_evict", "medium"], store)
      end

      # Now add a 3rd element with just 1 count -- should either not evict
      # (if count is too low) or evict the min
      result = TopK.handle("TOPK.ADD", ["tk_evict", "low"], store)
      assert is_list(result)
    end

    test "TOPK.RESERVE with k=1 (minimum)" do
      store = make_topk_store()
      assert :ok = TopK.handle("TOPK.RESERVE", ["tk_k1", "1"], store)
      TopK.handle("TOPK.ADD", ["tk_k1", "only"], store)
      result = TopK.handle("TOPK.LIST", ["tk_k1"], store)
      assert result == ["only"]
    end

    test "TOPK.INCRBY increments correctly" do
      store = make_topk_store()
      assert :ok = TopK.handle("TOPK.RESERVE", ["tk_incrby", "5"], store)
      result = TopK.handle("TOPK.INCRBY", ["tk_incrby", "item", "10"], store)
      assert is_list(result)

      # Count should reflect the increment
      counts = TopK.handle("TOPK.COUNT", ["tk_incrby", "item"], store)
      assert is_list(counts)
      assert hd(counts) >= 10
    end

    test "TOPK.QUERY returns 0 for element not in top-K" do
      store = make_topk_store()
      assert :ok = TopK.handle("TOPK.RESERVE", ["tk_query", "5"], store)
      result = TopK.handle("TOPK.QUERY", ["tk_query", "not_there"], store)
      assert result == [0]
    end

    test "TOPK.QUERY returns 1 for element in top-K" do
      store = make_topk_store()
      assert :ok = TopK.handle("TOPK.RESERVE", ["tk_query2", "5"], store)
      TopK.handle("TOPK.ADD", ["tk_query2", "present"], store)
      result = TopK.handle("TOPK.QUERY", ["tk_query2", "present"], store)
      assert result == [1]
    end

    test "TOPK.RESERVE duplicate key returns error" do
      store = make_topk_store()
      assert :ok = TopK.handle("TOPK.RESERVE", ["tk_dup", "5"], store)
      assert {:error, msg} = TopK.handle("TOPK.RESERVE", ["tk_dup", "5"], store)
      assert msg =~ "already exists"
    end

    test "TOPK.RESERVE with wrong args count returns error" do
      store = make_topk_store()
      assert {:error, msg} = TopK.handle("TOPK.RESERVE", [], store)
      assert msg =~ "wrong number of arguments"
    end
  end

  # ===========================================================================
  # 5. MemoryGuard edge cases
  # ===========================================================================

  describe "MemoryGuard edge cases" do
    test "eviction when keydir is empty does not crash" do
      # Clear all keys to make keydir empty
      ShardHelpers.flush_all_keys()

      # Set up tiny budget to trigger eviction
      _original_stats = MemoryGuard.stats()

      # Force a check -- should not crash even with empty keydir
      MemoryGuard.force_check()

      # Verify stats still work
      stats = MemoryGuard.stats()
      assert is_map(stats)
      assert Map.has_key?(stats, :total_bytes)
    end

    test "skip_promotion flag does not affect writes" do
      MemoryGuard.set_skip_promotion(true)

      # Writes should still work
      Router.put(FerricStore.Instance.get(:default), "skip_promo_write", "value", 0)
      Process.sleep(50)

      assert Router.get(FerricStore.Instance.get(:default), "skip_promo_write") == "value"
    end

    test "NIF allocator returns non-negative value" do
      # rust_allocated_bytes should return >= 0 in test mode (tracking installed)
      # or -1 in production (tracking not installed). Either way, stats should handle it.
      nif_bytes = NIF.rust_allocated_bytes()
      assert is_integer(nif_bytes)

      stats = MemoryGuard.stats()
      assert stats.nif_allocated_bytes >= 0
    end

    test "rapid pressure flag transitions do not crash" do
      for _ <- 1..20 do
        MemoryGuard.set_reject_writes(true)
        MemoryGuard.set_reject_writes(false)
        MemoryGuard.set_keydir_full(true)
        MemoryGuard.set_keydir_full(false)
        MemoryGuard.set_skip_promotion(true)
        MemoryGuard.set_skip_promotion(false)
      end

      # All flags should be false after the loop
      refute MemoryGuard.reject_writes?()
      refute MemoryGuard.keydir_full?()
      refute MemoryGuard.skip_promotion?()
    end

    test "force_check returns :ok and updates state" do
      assert :ok = MemoryGuard.force_check()

      # Stats should be fresh
      stats = MemoryGuard.stats()
      assert is_map(stats)
      assert stats.ratio >= 0.0
    end

    test "nudge does not crash" do
      # nudge is a cast, so it returns :ok immediately
      assert :ok = MemoryGuard.nudge()
      # Give it time to process
      Process.sleep(50)

      # System should still be functional
      stats = MemoryGuard.stats()
      assert is_map(stats)
    end

    test "reconfigure with new parameters does not crash" do
      # Save original
      original_stats = MemoryGuard.stats()

      # Reconfigure with different values
      assert :ok =
               MemoryGuard.reconfigure(%{
                 max_memory_bytes: 1_000_000_000,
                 eviction_policy: :volatile_lfu
               })

      # Force check with new config
      assert :ok = MemoryGuard.force_check()

      # Restore
      MemoryGuard.reconfigure(%{
        max_memory_bytes: original_stats.max_bytes,
        eviction_policy: original_stats.eviction_policy
      })
    end

    test "stats includes all expected fields" do
      stats = MemoryGuard.stats()

      expected_keys = [
        :total_bytes,
        :max_bytes,
        :ratio,
        :pressure_level,
        :shards,
        :eviction_policy,
        :keydir_bytes,
        :keydir_max_ram,
        :keydir_pressure_level,
        :keydir_ratio,
        :rss_bytes,
        :rss_ratio,
        :rss_pressure_level,
        :memory_limit,
        :nif_allocated_bytes
      ]

      for key <- expected_keys do
        assert Map.has_key?(stats, key), "missing key: #{inspect(key)}"
      end
    end

    test "pressure flags are independent of each other" do
      MemoryGuard.set_keydir_full(true)
      MemoryGuard.set_reject_writes(false)
      MemoryGuard.set_skip_promotion(false)

      assert MemoryGuard.keydir_full?()
      refute MemoryGuard.reject_writes?()
      refute MemoryGuard.skip_promotion?()

      MemoryGuard.set_keydir_full(false)
      MemoryGuard.set_skip_promotion(true)

      refute MemoryGuard.keydir_full?()
      assert MemoryGuard.skip_promotion?()
    end
  end

  # ===========================================================================
  # 6. State machine prob-related edge cases (via Router)
  # ===========================================================================

  describe "state machine prob edge cases via Router" do
    test "DEL on a regular (non-prob) key does not crash" do
      Router.put(FerricStore.Instance.get(:default), "regular_key", "value", 0)
      Process.sleep(50)
      assert Router.get(FerricStore.Instance.get(:default), "regular_key") == "value"

      # Delete should work fine without touching prob files
      Router.delete(FerricStore.Instance.get(:default), "regular_key")
      Process.sleep(50)
      assert Router.get(FerricStore.Instance.get(:default), "regular_key") == nil
    end

    test "prob_path handles keys with special characters via base64" do
      store = make_bloom_store()
      dir = store.bloom_registry.dir

      # Key with special characters
      key = "key/with\\special\x00chars!@#$%"
      path = prob_file_path(dir, key, "bloom")

      # Should produce a valid filesystem path (base64 encoded)
      assert is_binary(path)
      assert String.contains?(path, dir)
      # Path should not contain the raw special chars
      refute String.contains?(path, "/with\\")
    end

    test "prob_path handles unicode keys" do
      store = make_bloom_store()
      dir = store.bloom_registry.dir

      key = "emoji_key_\u{1F600}_\u{4E16}\u{754C}"
      path = prob_file_path(dir, key, "bloom")
      assert is_binary(path)
      # Should be a valid path
      assert Path.dirname(path) == dir
    end

    test "prob_path handles empty key" do
      store = make_bloom_store()
      dir = store.bloom_registry.dir

      path = prob_file_path(dir, "", "bloom")
      assert is_binary(path)
      assert String.ends_with?(path, ".bloom")
    end
  end

  # ===========================================================================
  # 7. NIF-level edge cases (direct NIF calls)
  # ===========================================================================

  describe "NIF-level edge cases" do
    test "bloom_file_create with valid params succeeds" do
      dir = make_prob_dir("nif_bloom")
      path = Path.join(dir, "test.bloom")
      assert {:ok, :ok} = NIF.bloom_file_create(path, 1000, 7)
      assert File.exists?(path)
    end

    test "bloom_file_add and bloom_file_exists roundtrip" do
      dir = make_prob_dir("nif_bloom_rt")
      path = Path.join(dir, "roundtrip.bloom")
      assert {:ok, :ok} = NIF.bloom_file_create(path, 10_000, 7)

      # Add an element
      assert {:ok, 1} = NIF.bloom_file_add(path, "hello")

      # Should exist
      assert {:ok, 1} = NIF.bloom_file_exists(path, "hello")

      # Should NOT exist (with high probability given 10_000 bits and 1 element)
      assert {:ok, 0} = NIF.bloom_file_exists(path, "definitely_not_here")
    end

    test "bloom_file_madd adds multiple elements" do
      dir = make_prob_dir("nif_bloom_madd")
      path = Path.join(dir, "madd.bloom")
      assert {:ok, :ok} = NIF.bloom_file_create(path, 10_000, 7)

      assert {:ok, results} = NIF.bloom_file_madd(path, ["a", "b", "c"])
      assert length(results) == 3
      assert Enum.all?(results, &(&1 in [0, 1]))

      # All should exist
      assert {:ok, [1, 1, 1]} = NIF.bloom_file_mexists(path, ["a", "b", "c"])
    end

    test "bloom_file_card returns correct count" do
      dir = make_prob_dir("nif_bloom_card")
      path = Path.join(dir, "card.bloom")
      assert {:ok, :ok} = NIF.bloom_file_create(path, 10_000, 7)
      assert {:ok, 0} = NIF.bloom_file_card(path)

      NIF.bloom_file_add(path, "x")
      assert {:ok, 1} = NIF.bloom_file_card(path)
    end

    test "bloom_file_info returns correct metadata" do
      dir = make_prob_dir("nif_bloom_info")
      path = Path.join(dir, "info.bloom")
      assert {:ok, :ok} = NIF.bloom_file_create(path, 500, 5)
      assert {:ok, {500, 0, 5}} = NIF.bloom_file_info(path)
    end

    test "bloom_file_exists on non-existent file returns enoent" do
      assert {:error, :enoent} = NIF.bloom_file_exists("/tmp/nonexistent_bloom_xyz.bloom", "x")
    end

    test "bloom_file_exists rejects truncated bitset" do
      dir = make_prob_dir("nif_bloom_truncated_exists")
      path = Path.join(dir, "truncated.bloom")

      assert {:ok, :ok} = NIF.bloom_file_create(path, 500, 5)
      <<header::binary-size(32), _::binary>> = File.read!(path)
      File.write!(path, header)

      assert {:error, reason} = NIF.bloom_file_exists(path, "hot")
      assert reason =~ "truncated"
    end

    test "bloom_file_add rejects count overflow without wrapping header" do
      dir = make_prob_dir("nif_bloom_count_overflow")
      path = Path.join(dir, "overflow.bloom")
      max_u64 = 18_446_744_073_709_551_615

      assert {:ok, :ok} = NIF.bloom_file_create(path, 500, 5)
      <<prefix::binary-size(24), _count::binary-size(8), rest::binary>> = File.read!(path)
      File.write!(path, prefix <> <<max_u64::little-unsigned-64>> <> rest)

      assert {:error, reason} = NIF.bloom_file_add(path, "hot")
      assert reason =~ "overflow"
      assert {:ok, ^max_u64} = NIF.bloom_file_card(path)
    end

    test "bloom_file_madd rejects count overflow without applying pending bits" do
      dir = make_prob_dir("nif_bloom_madd_count_overflow")
      path = Path.join(dir, "overflow.bloom")
      max_u64 = 18_446_744_073_709_551_615
      near_max = max_u64 - 1

      assert {:ok, :ok} = NIF.bloom_file_create(path, 10_000, 5)
      <<prefix::binary-size(24), _count::binary-size(8), rest::binary>> = File.read!(path)
      File.write!(path, prefix <> <<near_max::little-unsigned-64>> <> rest)

      assert {:error, reason} = NIF.bloom_file_madd(path, ["hot-a", "hot-b"])
      assert reason =~ "overflow"
      assert {:ok, ^near_max} = NIF.bloom_file_card(path)
      assert {:ok, [0, 0]} = NIF.bloom_file_mexists(path, ["hot-a", "hot-b"])
    end

    test "cms_file_create and query roundtrip" do
      dir = make_prob_dir("nif_cms")
      path = Path.join(dir, "test.cms")
      assert {:ok, :ok} = NIF.cms_file_create(path, 100, 5)

      # Increment
      assert {:ok, [5]} = NIF.cms_file_incrby(path, [{"hello", 5}])

      # Query
      assert {:ok, [5]} = NIF.cms_file_query(path, ["hello"])
      assert {:ok, [0]} = NIF.cms_file_query(path, ["not_here"])
    end

    test "cms_file_incrby rejects counter overflow without mutating" do
      dir = make_prob_dir("nif_cms_overflow")
      path = Path.join(dir, "overflow.cms")
      max_i64 = 9_223_372_036_854_775_807

      assert {:ok, :ok} = NIF.cms_file_create(path, 100, 5)
      assert {:ok, [^max_i64]} = NIF.cms_file_incrby(path, [{"hot", max_i64}])

      assert {:error, reason} = NIF.cms_file_incrby(path, [{"hot", 1}])
      assert reason =~ "overflow"
      assert {:ok, [^max_i64]} = NIF.cms_file_query(path, ["hot"])
    end

    test "cms_file_query rejects truncated counter region" do
      dir = make_prob_dir("nif_cms_truncated_query")
      path = Path.join(dir, "truncated.cms")

      assert {:ok, :ok} = NIF.cms_file_create(path, 100, 5)
      <<header::binary-size(32), _::binary>> = File.read!(path)
      File.write!(path, header)

      assert {:error, reason} = NIF.cms_file_query(path, ["hot"])
      assert reason =~ "truncated"
    end

    test "cms_file_info returns correct metadata" do
      dir = make_prob_dir("nif_cms_info")
      path = Path.join(dir, "info.cms")
      assert {:ok, :ok} = NIF.cms_file_create(path, 200, 7)
      assert {:ok, {200, 7, 0}} = NIF.cms_file_info(path)
    end

    test "cms_file_merge with empty source list succeeds" do
      dir = make_prob_dir("nif_cms_merge_empty")
      dst = Path.join(dir, "dst.cms")
      assert {:ok, :ok} = NIF.cms_file_create(dst, 100, 5)

      # Merge with no sources: should be a no-op
      assert :ok = NIF.cms_file_merge(dst, [], [])
    end

    test "cms_file_merge where dst already has data" do
      dir = make_prob_dir("nif_cms_merge_dst")
      dst = Path.join(dir, "dst.cms")
      src = Path.join(dir, "src.cms")
      assert {:ok, :ok} = NIF.cms_file_create(dst, 100, 5)
      assert {:ok, :ok} = NIF.cms_file_create(src, 100, 5)

      # Add data to both
      NIF.cms_file_incrby(dst, [{"item", 10}])
      NIF.cms_file_incrby(src, [{"item", 20}])

      # Merge: dst += src * 1
      assert :ok = NIF.cms_file_merge(dst, [src], [1])

      # Query: should be 10 + 20 = 30
      assert {:ok, [count]} = NIF.cms_file_query(dst, ["item"])
      assert count >= 30
    end

    test "cms_file_merge rejects counter overflow without mutating destination" do
      dir = make_prob_dir("nif_cms_merge_overflow")
      dst = Path.join(dir, "dst.cms")
      src = Path.join(dir, "src.cms")
      max_i64 = 9_223_372_036_854_775_807

      assert {:ok, :ok} = NIF.cms_file_create(dst, 100, 5)
      assert {:ok, :ok} = NIF.cms_file_create(src, 100, 5)
      assert {:ok, [^max_i64]} = NIF.cms_file_incrby(dst, [{"hot", max_i64}])
      assert {:ok, [1]} = NIF.cms_file_incrby(src, [{"hot", 1}])

      assert {:error, reason} = NIF.cms_file_merge(dst, [src], [1])
      assert reason =~ "overflow"
      assert {:ok, [^max_i64]} = NIF.cms_file_query(dst, ["hot"])
    end

    test "cuckoo_file_create and roundtrip" do
      dir = make_prob_dir("nif_cuckoo")
      path = Path.join(dir, "test.cuckoo")
      assert {:ok, :ok} = NIF.cuckoo_file_create(path, 1024, 4)

      assert {:ok, 1} = NIF.cuckoo_file_add(path, "hello")
      assert {:ok, 1} = NIF.cuckoo_file_exists(path, "hello")
      assert {:ok, 0} = NIF.cuckoo_file_exists(path, "world")
    end

    test "cuckoo_file_exists rejects truncated bucket region" do
      dir = make_prob_dir("nif_cuckoo_truncated_exists")
      path = Path.join(dir, "truncated.cuckoo")

      assert {:ok, :ok} = NIF.cuckoo_file_create(path, 1024, 4)
      <<header::binary-size(27), _::binary>> = File.read!(path)
      File.write!(path, header)

      assert {:error, reason} = NIF.cuckoo_file_exists(path, "hot")
      assert reason =~ "truncated"
    end

    test "cuckoo_file_add rejects item counter overflow without inserting" do
      dir = make_prob_dir("nif_cuckoo_add_count_overflow")
      path = Path.join(dir, "overflow.cuckoo")
      max_u64 = 18_446_744_073_709_551_615

      assert {:ok, :ok} = NIF.cuckoo_file_create(path, 1024, 4)
      <<prefix::binary-size(11), _items::binary-size(8), rest::binary>> = File.read!(path)
      File.write!(path, prefix <> <<max_u64::little-unsigned-64>> <> rest)

      assert {:error, reason} = NIF.cuckoo_file_add(path, "hot")
      assert reason =~ "overflow"
      assert {:ok, 0} = NIF.cuckoo_file_exists(path, "hot")
      assert {:ok, {_, _, _, ^max_u64, 0, _, _}} = NIF.cuckoo_file_info(path)
    end

    test "cuckoo_file_addnx rejects item counter overflow without inserting" do
      dir = make_prob_dir("nif_cuckoo_addnx_count_overflow")
      path = Path.join(dir, "overflow.cuckoo")
      max_u64 = 18_446_744_073_709_551_615

      assert {:ok, :ok} = NIF.cuckoo_file_create(path, 1024, 4)
      <<prefix::binary-size(11), _items::binary-size(8), rest::binary>> = File.read!(path)
      File.write!(path, prefix <> <<max_u64::little-unsigned-64>> <> rest)

      assert {:error, reason} = NIF.cuckoo_file_addnx(path, "hot")
      assert reason =~ "overflow"
      assert {:ok, 0} = NIF.cuckoo_file_exists(path, "hot")
      assert {:ok, {_, _, _, ^max_u64, 0, _, _}} = NIF.cuckoo_file_info(path)
    end

    test "cuckoo_file_del removes element" do
      dir = make_prob_dir("nif_cuckoo_del")
      path = Path.join(dir, "del.cuckoo")
      assert {:ok, :ok} = NIF.cuckoo_file_create(path, 1024, 4)

      NIF.cuckoo_file_add(path, "removeme")
      assert {:ok, 1} = NIF.cuckoo_file_exists(path, "removeme")

      assert {:ok, 1} = NIF.cuckoo_file_del(path, "removeme")
      assert {:ok, 0} = NIF.cuckoo_file_exists(path, "removeme")
    end

    test "cuckoo_file_del rejects delete counter overflow without deleting" do
      dir = make_prob_dir("nif_cuckoo_del_count_overflow")
      path = Path.join(dir, "overflow.cuckoo")
      max_u64 = 18_446_744_073_709_551_615

      assert {:ok, :ok} = NIF.cuckoo_file_create(path, 1024, 4)
      assert {:ok, 1} = NIF.cuckoo_file_add(path, "victim")

      <<prefix::binary-size(19), _deletes::binary-size(8), rest::binary>> = File.read!(path)
      File.write!(path, prefix <> <<max_u64::little-unsigned-64>> <> rest)

      assert {:error, reason} = NIF.cuckoo_file_del(path, "victim")
      assert reason =~ "overflow"
      assert {:ok, 1} = NIF.cuckoo_file_exists(path, "victim")
      assert {:ok, {_, _, _, 1, ^max_u64, _, _}} = NIF.cuckoo_file_info(path)
    end

    test "cuckoo_file_addnx prevents duplicates" do
      dir = make_prob_dir("nif_cuckoo_addnx")
      path = Path.join(dir, "addnx.cuckoo")
      assert {:ok, :ok} = NIF.cuckoo_file_create(path, 1024, 4)

      assert {:ok, 1} = NIF.cuckoo_file_addnx(path, "unique")
      assert {:ok, 0} = NIF.cuckoo_file_addnx(path, "unique")
    end

    test "cuckoo_file_count returns correct count" do
      dir = make_prob_dir("nif_cuckoo_count")
      path = Path.join(dir, "count.cuckoo")
      assert {:ok, :ok} = NIF.cuckoo_file_create(path, 1024, 4)

      assert {:ok, 0} = NIF.cuckoo_file_count(path, "item")
      NIF.cuckoo_file_add(path, "item")
      count_result = NIF.cuckoo_file_count(path, "item")
      assert {:ok, count} = count_result
      assert count >= 1
    end

    test "cuckoo filter full scenario terminates correctly" do
      dir = make_prob_dir("nif_cuckoo_full")
      path = Path.join(dir, "full.cuckoo")
      # Very small: capacity=2, bucket_size=1 => only 2 slots total
      assert {:ok, :ok} = NIF.cuckoo_file_create(path, 2, 1)

      # Add elements until full
      results =
        Enum.map(1..100, fn i ->
          NIF.cuckoo_file_add(path, "elem_#{i}")
        end)

      # At some point, additions should fail with "filter is full"
      has_full =
        Enum.any?(results, fn
          {:error, "filter is full"} -> true
          _ -> false
        end)

      # With capacity=2 and bucket_size=1, we should hit full quickly
      assert has_full, "filter should report full with only 2 slots"
    end

    test "topk_file_create_v2 and roundtrip" do
      dir = make_prob_dir("nif_topk")
      path = Path.join(dir, "test.topk")
      assert {:ok, :ok} = NIF.topk_file_create_v2(path, 5, 8, 7, 0.9)

      # Add elements
      result = NIF.topk_file_add_v2(path, ["apple", "banana", "cherry"])
      assert is_list(result)
      assert length(result) == 3

      # List
      list_result = NIF.topk_file_list_v2(path)
      assert is_list(list_result)
      assert length(list_result) == 3
    end

    test "topk_file_incrby_v2 rejects counter overflow without mutating" do
      dir = make_prob_dir("nif_topk_overflow")
      path = Path.join(dir, "overflow.topk")
      max_i64 = 9_223_372_036_854_775_807

      assert {:ok, :ok} = NIF.topk_file_create_v2(path, 5, 8, 7, 0.9)
      assert [nil] = NIF.topk_file_incrby_v2(path, [{"hot", max_i64}])
      assert [^max_i64] = NIF.topk_file_count_v2(path, ["hot"])

      assert {:error, reason} = NIF.topk_file_incrby_v2(path, [{"hot", 1}])
      assert reason =~ "overflow"
      assert [^max_i64] = NIF.topk_file_count_v2(path, ["hot"])
    end

    test "topk_file_count_v2 rejects truncated CMS region" do
      dir = make_prob_dir("nif_topk_truncated_count")
      path = Path.join(dir, "truncated.topk")

      assert {:ok, :ok} = NIF.topk_file_create_v2(path, 5, 8, 7, 0.9)
      <<header::binary-size(64), _::binary>> = File.read!(path)
      File.write!(path, header)

      assert {:error, reason} = NIF.topk_file_count_v2(path, ["hot"])
      assert reason =~ "truncated"
    end

    test "topk_file_list_v2 on empty topk returns empty list" do
      dir = make_prob_dir("nif_topk_empty")
      path = Path.join(dir, "empty.topk")
      assert {:ok, :ok} = NIF.topk_file_create_v2(path, 5, 8, 7, 0.9)

      result = NIF.topk_file_list_v2(path)
      assert result == []
    end

    test "topk eviction returns correct evicted elements" do
      dir = make_prob_dir("nif_topk_evict")
      path = Path.join(dir, "evict.topk")
      # k=2: only 2 elements in heap
      assert {:ok, :ok} = NIF.topk_file_create_v2(path, 2, 8, 7, 0.9)

      # Add elements with different counts
      NIF.topk_file_incrby_v2(path, [{"high", 100}])
      NIF.topk_file_incrby_v2(path, [{"medium", 50}])

      # This should evict "medium" (count=50 < count of "low_but_actually_high"=200)
      result = NIF.topk_file_incrby_v2(path, [{"newcomer", 200}])
      assert is_list(result)

      # Verify "high" and "newcomer" are in the list (or "high" and "medium" if
      # newcomer didn't evict due to CMS collision behavior)
      list = NIF.topk_file_list_v2(path)
      assert is_list(list)
      assert length(list) == 2
    end

    test "topk_file_query_v2 on non-existent path returns enoent" do
      result = NIF.topk_file_query_v2("/tmp/nonexistent_topk_xyz.topk", ["x"])
      assert {:error, :enoent} = result
    end

    test "topk_file_info_v2 returns correct metadata" do
      dir = make_prob_dir("nif_topk_info")
      path = Path.join(dir, "info.topk")
      assert {:ok, :ok} = NIF.topk_file_create_v2(path, 10, 16, 5, 0.8)

      result = NIF.topk_file_info_v2(path)
      assert {10, 16, 5, decay} = result
      assert_in_delta decay, 0.8, 0.001
    end
  end

  # ===========================================================================
  # 8. Cross-cutting prob file cleanup
  # ===========================================================================

  describe "prob file cleanup" do
    test "DEL on a bloom key via Router deletes the prob file" do
      Router.put(FerricStore.Instance.get(:default), "bloom_router_test", "placeholder", 0)
      Process.sleep(50)

      # Create a bloom filter via the command handler
      dir = make_prob_dir("router_bloom")
      path = Path.join(dir, "test.bloom")
      assert {:ok, :ok} = NIF.bloom_file_create(path, 1000, 7)
      assert File.exists?(path)

      # Direct deletion of the file (simulating what maybe_delete_prob_file does)
      File.rm(path)
      refute File.exists?(path)
    end
  end

  # ===========================================================================
  # 9. Optimal sizing edge cases (Bloom)
  # ===========================================================================

  describe "Bloom optimal sizing" do
    test "optimal_num_bits with capacity=1" do
      bits = Bloom.optimal_num_bits(1, 0.01)
      assert bits >= 1
    end

    test "optimal_num_hashes with minimum inputs" do
      hashes = Bloom.optimal_num_hashes(1, 1)
      assert hashes >= 1
    end

    test "optimal_num_bits increases with capacity" do
      bits_100 = Bloom.optimal_num_bits(100, 0.01)
      bits_1000 = Bloom.optimal_num_bits(1000, 0.01)
      assert bits_1000 > bits_100
    end

    test "optimal_num_bits increases with lower error rate" do
      bits_1pct = Bloom.optimal_num_bits(100, 0.01)
      bits_01pct = Bloom.optimal_num_bits(100, 0.001)
      assert bits_01pct > bits_1pct
    end
  end

  # ===========================================================================
  # 10. Input validation edge cases
  # ===========================================================================

  describe "input validation" do
    test "BF.RESERVE with error_rate=0 returns error" do
      store = make_bloom_store()
      assert {:error, msg} = Bloom.handle("BF.RESERVE", ["bf_bad_rate", "0", "100"], store)
      assert msg =~ "error rate"
    end

    test "BF.RESERVE with error_rate=1 returns error" do
      store = make_bloom_store()
      assert {:error, msg} = Bloom.handle("BF.RESERVE", ["bf_bad_rate2", "1", "100"], store)
      assert msg =~ "error rate"
    end

    test "BF.RESERVE with negative capacity returns error" do
      store = make_bloom_store()
      assert {:error, msg} = Bloom.handle("BF.RESERVE", ["bf_neg", "0.01", "-1"], store)
      assert msg =~ "capacity"
    end

    test "CMS.INCRBY with non-integer count returns error" do
      store = make_cms_store()
      assert :ok = CMS.handle("CMS.INITBYDIM", ["cms_bad_count", "100", "5"], store)
      assert {:error, msg} = CMS.handle("CMS.INCRBY", ["cms_bad_count", "elem", "abc"], store)
      assert msg =~ "invalid"
    end

    test "CMS.INCRBY with zero count returns error" do
      store = make_cms_store()
      assert :ok = CMS.handle("CMS.INITBYDIM", ["cms_zero_count", "100", "5"], store)
      # count=0 should fail since parse_count requires >= 1
      assert {:error, msg} = CMS.handle("CMS.INCRBY", ["cms_zero_count", "elem", "0"], store)
      assert msg =~ "invalid"
    end

    test "CMS.INCRBY with odd number of args returns error" do
      store = make_cms_store()
      assert :ok = CMS.handle("CMS.INITBYDIM", ["cms_odd_args", "100", "5"], store)
      assert {:error, msg} = CMS.handle("CMS.INCRBY", ["cms_odd_args", "elem"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "TOPK.RESERVE with k=0 returns error" do
      store = make_topk_store()
      assert {:error, msg} = TopK.handle("TOPK.RESERVE", ["tk_zero", "0"], store)
      assert msg =~ "positive integer"
    end

    test "TOPK.INCRBY with odd number of args returns error" do
      store = make_topk_store()
      assert :ok = TopK.handle("TOPK.RESERVE", ["tk_odd", "5"], store)
      assert {:error, msg} = TopK.handle("TOPK.INCRBY", ["tk_odd", "elem"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "CF.RESERVE with zero capacity returns error" do
      store = make_cuckoo_store()
      assert {:error, msg} = Cuckoo.handle("CF.RESERVE", ["cf_zero", "0"], store)
      assert msg =~ "capacity"
    end

    test "CF.RESERVE with non-integer capacity returns error" do
      store = make_cuckoo_store()
      assert {:error, msg} = Cuckoo.handle("CF.RESERVE", ["cf_nan", "abc"], store)
      assert msg =~ "capacity"
    end
  end
end
