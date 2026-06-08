defmodule Ferricstore.ProbEdgeCasesTest.Sections.BloomEdgeCases do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.{Bloom, CMS, Cuckoo, TopK}
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.MemoryGuard
      alias Ferricstore.Store.Router
      alias Ferricstore.Test.ShardHelpers

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
    end
  end
end
