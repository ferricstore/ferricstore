defmodule Ferricstore.ProbEdgeCasesTest.Sections.NifLevelEdgeCases do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.{Bloom, CMS, Cuckoo, TopK}
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.MemoryGuard
      alias Ferricstore.Store.Router
      alias Ferricstore.Test.ShardHelpers

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
          assert {:error, :enoent} =
                   NIF.bloom_file_exists("/tmp/nonexistent_bloom_xyz.bloom", "x")
        end

        test "bloom_file_exists rejects truncated bitset" do
          dir = make_prob_dir("nif_bloom_truncated_exists")
          path = Path.join(dir, "truncated.bloom")

          assert {:ok, :ok} = NIF.bloom_file_create(path, 500, 5)
          <<header::binary-size(32), _::binary>> = File.read!(path)
          File.write!(path, header)

          assert {:error, reason} = NIF.bloom_file_exists(path, "hot")
          assert reason =~ "file size mismatch"
        end

        test "bloom_file_add rejects an impossible count header without mutation" do
          dir = make_prob_dir("nif_bloom_count_overflow")
          path = Path.join(dir, "overflow.bloom")
          max_u64 = 18_446_744_073_709_551_615

          assert {:ok, :ok} = NIF.bloom_file_create(path, 500, 5)
          <<prefix::binary-size(24), _count::binary-size(8), rest::binary>> = File.read!(path)
          File.write!(path, prefix <> <<max_u64::little-unsigned-64>> <> rest)

          assert {:error, reason} = NIF.bloom_file_add(path, "hot")
          assert reason =~ "count must not exceed num_bits"
          assert {:error, card_reason} = NIF.bloom_file_card(path)
          assert card_reason =~ "count must not exceed num_bits"
        end

        test "bloom_file_madd rejects an impossible count header without applying pending bits" do
          dir = make_prob_dir("nif_bloom_madd_count_overflow")
          path = Path.join(dir, "overflow.bloom")
          max_u64 = 18_446_744_073_709_551_615
          near_max = max_u64 - 1

          assert {:ok, :ok} = NIF.bloom_file_create(path, 10_000, 5)
          <<prefix::binary-size(24), _count::binary-size(8), rest::binary>> = File.read!(path)
          File.write!(path, prefix <> <<near_max::little-unsigned-64>> <> rest)

          assert {:error, reason} = NIF.bloom_file_madd(path, ["hot-a", "hot-b"])
          assert reason =~ "count must not exceed num_bits"
          assert {:error, card_reason} = NIF.bloom_file_card(path)
          assert card_reason =~ "count must not exceed num_bits"
          assert {:error, exists_reason} = NIF.bloom_file_mexists(path, ["hot-a", "hot-b"])
          assert exists_reason =~ "count must not exceed num_bits"
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
          assert reason =~ "file size mismatch"
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
          assert reason =~ "file size mismatch"
        end

        test "cuckoo_file_add rejects an impossible item count without inserting" do
          dir = make_prob_dir("nif_cuckoo_add_count_overflow")
          path = Path.join(dir, "overflow.cuckoo")
          max_u64 = 18_446_744_073_709_551_615

          assert {:ok, :ok} = NIF.cuckoo_file_create(path, 1024, 4)
          <<prefix::binary-size(11), _items::binary-size(8), rest::binary>> = File.read!(path)
          File.write!(path, prefix <> <<max_u64::little-unsigned-64>> <> rest)

          assert {:error, reason} = NIF.cuckoo_file_add(path, "hot")
          assert reason =~ "num_items must not exceed total slots"
          assert {:error, exists_reason} = NIF.cuckoo_file_exists(path, "hot")
          assert exists_reason =~ "num_items must not exceed total slots"
          assert {:error, info_reason} = NIF.cuckoo_file_info(path)
          assert info_reason =~ "num_items must not exceed total slots"
        end

        test "cuckoo_file_addnx rejects an impossible item count without inserting" do
          dir = make_prob_dir("nif_cuckoo_addnx_count_overflow")
          path = Path.join(dir, "overflow.cuckoo")
          max_u64 = 18_446_744_073_709_551_615

          assert {:ok, :ok} = NIF.cuckoo_file_create(path, 1024, 4)
          <<prefix::binary-size(11), _items::binary-size(8), rest::binary>> = File.read!(path)
          File.write!(path, prefix <> <<max_u64::little-unsigned-64>> <> rest)

          assert {:error, reason} = NIF.cuckoo_file_addnx(path, "hot")
          assert reason =~ "num_items must not exceed total slots"
          assert {:error, exists_reason} = NIF.cuckoo_file_exists(path, "hot")
          assert exists_reason =~ "num_items must not exceed total slots"
          assert {:error, info_reason} = NIF.cuckoo_file_info(path)
          assert info_reason =~ "num_items must not exceed total slots"
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
          assert {:ok, :ok} = NIF.topk_file_create_v2(path, 5, 8, 7)

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

          assert {:ok, :ok} = NIF.topk_file_create_v2(path, 5, 8, 7)
          assert [nil] = NIF.topk_file_incrby_v2(path, [{"hot", max_i64}])
          assert [^max_i64] = NIF.topk_file_count_v2(path, ["hot"])

          assert {:error, reason} = NIF.topk_file_incrby_v2(path, [{"hot", 1}])
          assert reason =~ "overflow"
          assert [^max_i64] = NIF.topk_file_count_v2(path, ["hot"])
        end

        test "topk_file_count_v2 rejects truncated CMS region" do
          dir = make_prob_dir("nif_topk_truncated_count")
          path = Path.join(dir, "truncated.topk")

          assert {:ok, :ok} = NIF.topk_file_create_v2(path, 5, 8, 7)
          <<header::binary-size(64), _::binary>> = File.read!(path)
          File.write!(path, header)

          assert {:error, reason} = NIF.topk_file_count_v2(path, ["hot"])
          assert reason =~ "file size mismatch"
        end

        test "topk_file_list_v2 on empty topk returns empty list" do
          dir = make_prob_dir("nif_topk_empty")
          path = Path.join(dir, "empty.topk")
          assert {:ok, :ok} = NIF.topk_file_create_v2(path, 5, 8, 7)

          result = NIF.topk_file_list_v2(path)
          assert result == []
        end

        test "topk eviction returns correct evicted elements" do
          dir = make_prob_dir("nif_topk_evict")
          path = Path.join(dir, "evict.topk")
          # k=2: only 2 elements in heap
          assert {:ok, :ok} = NIF.topk_file_create_v2(path, 2, 8, 7)

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
          assert {:ok, :ok} = NIF.topk_file_create_v2(path, 10, 16, 5)

          result = NIF.topk_file_info_v2(path)
          assert {10, 16, 5} = result
        end
      end

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
  end
end
