defmodule Ferricstore.Commands.ProbBugHuntTest do
  @moduledoc """
  Bug-hunting tests for the four probabilistic data structure command
  modules: Bloom, Cuckoo, CMS, and TopK.

  Each test targets a specific behavioral contract drawn from the Redis
  documentation. Failures here indicate a bug in the command handler,
  not in the test.

  ## Bugs found

    1. **CMS.INITBYPROB depth formula is wrong.**
       The current code computes `depth = ceil(ln(1 / (1 - prob)))`.
       Redis defines the probability parameter as delta (the probability of
       the estimate exceeding the error bound), so the correct formula is
       `depth = ceil(ln(1 / prob))`.  With `prob = 0.01`, the code produces
       `depth = 1` instead of the correct `depth = 5`.

    2. **Bloom/Cuckoo `load_filter` crashes on wrong-type keys.**
       If a key holds a non-binary value (e.g. a CMS tuple), the two-clause
       `case` in `load_filter` raises `CaseClauseError` instead of returning
       a WRONGTYPE error.

    3. **BF.CARD over-counts when near capacity due to hash collisions.**
       The size counter increments whenever a new bit is set, but two
       distinct elements may set overlapping bit positions. If element B
       shares all its hash positions with previously-added elements, B is
       treated as a duplicate (size does not increment), leading to an
       undercount rather than an exact count. This is inherent to Bloom
       filters and not technically a bug, but the counter's accuracy
       degrades faster than expected.

  ## Test organisation

  Tests are grouped by data structure and then by the specific scenario
  under test. Every test uses `MockStore` for full isolation and runs
  `async: true`.
  """

  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Bloom
  alias Ferricstore.Commands.Cuckoo
  alias Ferricstore.Commands.CMS
  alias Ferricstore.Commands.Strings
  alias Ferricstore.Commands.TopK
  alias Ferricstore.Test.MockStore

  describe "probabilistic direct writes surface directory fsync failures" do
    setup do
      Process.put(:ferricstore_prob_command_fsync_dir_hook, fn _path -> {:error, :eio} end)

      on_exit(fn ->
        Process.delete(:ferricstore_prob_command_fsync_dir_hook)
      end)

      :ok
    end

    test "BF.RESERVE returns an error when the prob file directory fsync fails" do
      assert {:error, {:fsync_dir_failed, :prob_file_dir, :eio}} =
               Bloom.handle("BF.RESERVE", ["bf_fsync", "0.01", "100"], MockStore.make())
    end

    test "CF.RESERVE returns an error when the prob file directory fsync fails" do
      assert {:error, {:fsync_dir_failed, :prob_file_dir, :eio}} =
               Cuckoo.handle("CF.RESERVE", ["cf_fsync", "100"], MockStore.make())
    end

    test "CMS.INITBYDIM returns an error when the prob file directory fsync fails" do
      assert {:error, {:fsync_dir_failed, :prob_file_dir, :eio}} =
               CMS.handle("CMS.INITBYDIM", ["cms_fsync", "100", "5"], MockStore.make())
    end

    test "TOPK.RESERVE returns an error when the prob file directory fsync fails" do
      assert {:error, {:fsync_dir_failed, :prob_file_dir, :eio}} =
               TopK.handle("TOPK.RESERVE", ["topk_fsync", "5"], MockStore.make())
    end
  end

  # ===========================================================================
  # Bloom filter (BF.*)
  # ===========================================================================

  describe "BF.ADD then BF.EXISTS returns true" do
    test "element added with BF.ADD is always found by BF.EXISTS (no false negatives)" do
      store = MockStore.make()
      assert 1 = Bloom.handle("BF.ADD", ["bf", "hello"], store)
      assert 1 = Bloom.handle("BF.EXISTS", ["bf", "hello"], store)
    end

    test "multiple distinct elements are all found after adding" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "1000"], store)

      elements = for i <- 1..50, do: "item_#{i}"

      Enum.each(elements, fn e ->
        Bloom.handle("BF.ADD", ["bf", e], store)
      end)

      Enum.each(elements, fn e ->
        assert 1 == Bloom.handle("BF.EXISTS", ["bf", e], store),
               "False negative: #{e} was added but BF.EXISTS returned 0"
      end)
    end

    test "element added on auto-created filter is found" do
      store = MockStore.make()
      # No BF.RESERVE -- auto-create
      assert 1 = Bloom.handle("BF.ADD", ["bf", "auto_elem"], store)
      assert 1 = Bloom.handle("BF.EXISTS", ["bf", "auto_elem"], store)
    end
  end

  describe "BF.EXISTS on non-added element returns false (within FP rate)" do
    test "non-added element returns 0 with high probability" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.001", "1000"], store)
      # Add a single element to create the filter state
      Bloom.handle("BF.ADD", ["bf", "present"], store)

      # Check a clearly distinct element
      assert 0 = Bloom.handle("BF.EXISTS", ["bf", "definitely_not_present"], store)
    end

    test "false positive rate stays within 2x target after filling to capacity" do
      store = MockStore.make()
      error_rate = 0.05
      capacity = 500

      Bloom.handle("BF.RESERVE", ["bf", "#{error_rate}", "#{capacity}"], store)

      for i <- 1..capacity do
        Bloom.handle("BF.ADD", ["bf", "added_#{i}"], store)
      end

      test_count = 5_000

      false_positives =
        Enum.count(1..test_count, fn i ->
          Bloom.handle("BF.EXISTS", ["bf", "not_added_#{i}"], store) == 1
        end)

      observed_rate = false_positives / test_count

      assert observed_rate < error_rate * 2,
             "FP rate #{Float.round(observed_rate * 100, 2)}% exceeds 2x target #{error_rate * 100}%"
    end
  end

  describe "BF.RESERVE with capacity 0 returns error" do
    test "capacity of 0 is rejected" do
      store = MockStore.make()
      assert {:error, msg} = Bloom.handle("BF.RESERVE", ["bf", "0.01", "0"], store)
      assert msg =~ "bad" or msg =~ "capacity"
    end

    test "negative capacity is rejected" do
      store = MockStore.make()
      assert {:error, _} = Bloom.handle("BF.RESERVE", ["bf", "0.01", "-1"], store)
    end

    test "non-numeric capacity is rejected" do
      store = MockStore.make()
      assert {:error, _} = Bloom.handle("BF.RESERVE", ["bf", "0.01", "abc"], store)
    end
  end

  describe "BF.CARD accuracy after many adds" do
    test "BF.CARD equals number of unique elements added" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "1000"], store)

      n = 100

      for i <- 1..n do
        Bloom.handle("BF.ADD", ["bf", "elem_#{i}"], store)
      end

      card = Bloom.handle("BF.CARD", ["bf"], store)

      # The Bloom filter size counter increments when at least one new bit
      # is set. With a low error rate and capacity >> n, the count should
      # equal n exactly. In the worst case, hash collisions cause an
      # undercount (some elements don't set any new bits), so we allow a
      # small margin.
      assert card >= n - 5,
             "BF.CARD returned #{card}, expected close to #{n}"

      assert card <= n,
             "BF.CARD returned #{card} which exceeds the number of unique adds #{n}"
    end

    test "BF.CARD does not increment for duplicate adds" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)

      Bloom.handle("BF.ADD", ["bf", "dup"], store)
      Bloom.handle("BF.ADD", ["bf", "dup"], store)
      Bloom.handle("BF.ADD", ["bf", "dup"], store)

      assert 1 = Bloom.handle("BF.CARD", ["bf"], store)
    end

    test "BF.CARD is 0 for empty filter" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      assert 0 = Bloom.handle("BF.CARD", ["bf"], store)
    end

    test "BF.CARD is 0 for non-existent key" do
      store = MockStore.make()
      assert 0 = Bloom.handle("BF.CARD", ["nonexistent"], store)
    end
  end

  describe "BF.INFO returns correct metadata" do
    test "returns capacity, size, error rate, hash count, and bit count" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "500"], store)

      for i <- 1..10 do
        Bloom.handle("BF.ADD", ["bf", "e_#{i}"], store)
      end

      result = Bloom.handle("BF.INFO", ["bf"], store)
      info = to_info_map(result)

      # Capacity and error_rate are derived from the mmap header when metadata
      # is not stored in Bitcask (MockStore), so they are approximate.
      assert_in_delta info["Capacity"], 500, 50
      assert info["Size"] == 10
      assert info["Number of items inserted"] == 10
      assert_in_delta info["Error rate"], 0.01, 0.005
      assert info["Number of hash functions"] > 0
      assert info["Number of bits"] > 0
      assert info["Number of filters"] == 1
      assert info["Expansion rate"] == 0
    end

    test "BF.INFO on non-existent key returns error" do
      store = MockStore.make()
      assert {:error, msg} = Bloom.handle("BF.INFO", ["missing"], store)
      assert msg =~ "not found"
    end
  end

  # ===========================================================================
  # Cuckoo filter (CF.*)
  # ===========================================================================

  describe "CF.ADD then CF.EXISTS returns true" do
    test "element added with CF.ADD is found by CF.EXISTS" do
      store = MockStore.make()
      assert 1 = Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
      assert 1 = Cuckoo.handle("CF.EXISTS", ["cf", "hello"], store)
    end

    test "multiple distinct elements are all found" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)

      elements = for i <- 1..50, do: "item_#{i}"

      Enum.each(elements, fn e ->
        Cuckoo.handle("CF.ADD", ["cf", e], store)
      end)

      Enum.each(elements, fn e ->
        assert 1 == Cuckoo.handle("CF.EXISTS", ["cf", e], store),
               "False negative: #{e} was added but CF.EXISTS returned 0"
      end)
    end
  end

  describe "CF.DEL then CF.EXISTS returns false" do
    test "deleted element is no longer found" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)
      Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
      assert 1 = Cuckoo.handle("CF.EXISTS", ["cf", "hello"], store)

      assert 1 = Cuckoo.handle("CF.DEL", ["cf", "hello"], store)
      assert 0 = Cuckoo.handle("CF.EXISTS", ["cf", "hello"], store)
    end

    test "deleting one occurrence of a duplicate leaves the other" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)
      Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
      Cuckoo.handle("CF.ADD", ["cf", "hello"], store)

      assert 2 = Cuckoo.handle("CF.COUNT", ["cf", "hello"], store)

      Cuckoo.handle("CF.DEL", ["cf", "hello"], store)
      assert 1 = Cuckoo.handle("CF.COUNT", ["cf", "hello"], store)
      assert 1 = Cuckoo.handle("CF.EXISTS", ["cf", "hello"], store)
    end

    test "deleting all occurrences means element is no longer found" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)
      Cuckoo.handle("CF.ADD", ["cf", "hello"], store)
      Cuckoo.handle("CF.ADD", ["cf", "hello"], store)

      Cuckoo.handle("CF.DEL", ["cf", "hello"], store)
      Cuckoo.handle("CF.DEL", ["cf", "hello"], store)

      assert 0 = Cuckoo.handle("CF.COUNT", ["cf", "hello"], store)
      assert 0 = Cuckoo.handle("CF.EXISTS", ["cf", "hello"], store)
    end
  end

  describe "CF.ADDNX duplicate returns 0" do
    test "CF.ADDNX returns 1 on first add and 0 on duplicate" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)

      assert 1 = Cuckoo.handle("CF.ADDNX", ["cf", "hello"], store)
      assert 0 = Cuckoo.handle("CF.ADDNX", ["cf", "hello"], store)
    end

    test "CF.ADDNX returns 0 even after many repeated attempts" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)

      assert 1 = Cuckoo.handle("CF.ADDNX", ["cf", "unique"], store)

      for _ <- 1..10 do
        assert 0 = Cuckoo.handle("CF.ADDNX", ["cf", "unique"], store)
      end

      # Count should still be 1 (only the initial ADDNX inserted)
      assert 1 = Cuckoo.handle("CF.COUNT", ["cf", "unique"], store)
    end

    test "CF.ADDNX on auto-created filter works" do
      store = MockStore.make()
      assert 1 = Cuckoo.handle("CF.ADDNX", ["cf", "first"], store)
      assert 0 = Cuckoo.handle("CF.ADDNX", ["cf", "first"], store)
    end
  end

  # ===========================================================================
  # Count-Min Sketch (CMS.*)
  # ===========================================================================

  describe "CMS.INCRBY then CMS.QUERY returns approximate count" do
    test "single element count is exact for a large sketch" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["sk", "2000", "7"], store)

      CMS.handle("CMS.INCRBY", ["sk", "apple", "42"], store)
      assert [42] = CMS.handle("CMS.QUERY", ["sk", "apple"], store)
    end

    test "CMS never undercounts" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["sk", "500", "5"], store)

      true_counts = %{
        "alpha" => 100,
        "beta" => 50,
        "gamma" => 200,
        "delta" => 10,
        "epsilon" => 1
      }

      for {elem, count} <- true_counts do
        CMS.handle("CMS.INCRBY", ["sk", elem, Integer.to_string(count)], store)
      end

      for {elem, true_count} <- true_counts do
        [estimated] = CMS.handle("CMS.QUERY", ["sk", elem], store)

        assert estimated >= true_count,
               "#{elem}: estimated #{estimated} < true #{true_count} (CMS must never undercount)"
      end
    end

    test "incremental INCRBY accumulates correctly" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["sk", "1000", "7"], store)

      CMS.handle("CMS.INCRBY", ["sk", "x", "10"], store)
      CMS.handle("CMS.INCRBY", ["sk", "x", "20"], store)
      CMS.handle("CMS.INCRBY", ["sk", "x", "30"], store)

      [est] = CMS.handle("CMS.QUERY", ["sk", "x"], store)
      assert est >= 60
    end

    test "querying non-existent element returns 0" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["sk", "100", "5"], store)
      assert [0] = CMS.handle("CMS.QUERY", ["sk", "never_seen"], store)
    end
  end

  describe "CMS.MERGE combines counts from two sketches" do
    test "merged sketch has summed counts" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["src1", "200", "5"], store)
      :ok = CMS.handle("CMS.INITBYDIM", ["src2", "200", "5"], store)

      CMS.handle("CMS.INCRBY", ["src1", "a", "10"], store)
      CMS.handle("CMS.INCRBY", ["src1", "b", "20"], store)
      CMS.handle("CMS.INCRBY", ["src2", "a", "30"], store)
      CMS.handle("CMS.INCRBY", ["src2", "c", "40"], store)

      assert :ok = CMS.handle("CMS.MERGE", ["dst", "2", "src1", "src2"], store)

      [est_a] = CMS.handle("CMS.QUERY", ["dst", "a"], store)
      [est_b] = CMS.handle("CMS.QUERY", ["dst", "b"], store)
      [est_c] = CMS.handle("CMS.QUERY", ["dst", "c"], store)

      # a: 10 + 30 = 40
      assert est_a >= 40
      # b: 20 + 0 = 20
      assert est_b >= 20
      # c: 0 + 40 = 40
      assert est_c >= 40
    end

    test "merged sketch with weights" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["s1", "200", "5"], store)
      :ok = CMS.handle("CMS.INITBYDIM", ["s2", "200", "5"], store)

      CMS.handle("CMS.INCRBY", ["s1", "x", "10"], store)
      CMS.handle("CMS.INCRBY", ["s2", "x", "10"], store)

      assert :ok =
               CMS.handle(
                 "CMS.MERGE",
                 ["dst", "2", "s1", "s2", "WEIGHTS", "3", "5"],
                 store
               )

      # Expected: 10*3 + 10*5 = 80
      [est] = CMS.handle("CMS.QUERY", ["dst", "x"], store)
      assert est >= 80
    end

    test "merge into existing destination adds to existing counts" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["dst", "200", "5"], store)
      :ok = CMS.handle("CMS.INITBYDIM", ["src", "200", "5"], store)

      CMS.handle("CMS.INCRBY", ["dst", "a", "100"], store)
      CMS.handle("CMS.INCRBY", ["src", "a", "50"], store)

      assert :ok = CMS.handle("CMS.MERGE", ["dst", "1", "src"], store)

      [est] = CMS.handle("CMS.QUERY", ["dst", "a"], store)
      assert est >= 150
    end

    test "merge with different dimensions returns error" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["s1", "100", "5"], store)
      :ok = CMS.handle("CMS.INITBYDIM", ["s2", "200", "5"], store)

      assert {:error, msg} = CMS.handle("CMS.MERGE", ["dst", "2", "s1", "s2"], store)
      assert msg =~ "width/depth"
    end
  end

  describe "CMS.INITBYPROB depth formula (BUG)" do
    @tag :bug
    test "BUG: depth formula uses ceil(ln(1/(1-prob))) instead of ceil(ln(1/prob))" do
      # Redis CMS.INITBYPROB: probability is delta (over-estimation probability).
      # Correct formula: depth = ceil(ln(1/prob))
      #
      # For prob=0.01: correct depth = ceil(ln(100)) = ceil(4.605) = 5
      # Fixed formula produces the correct depth.
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYPROB", ["sk", "0.01", "0.01"], store)

      correct_depth = ceil(:math.log(1.0 / 0.01))

      # After fix: depth matches the correct formula (query via CMS.INFO)
      assert correct_depth == 5
      info = CMS.handle("CMS.INFO", ["sk"], store)
      depth_idx = Enum.find_index(info, &(&1 == "depth"))
      assert Enum.at(info, depth_idx + 1) == correct_depth
    end

    @tag :bug
    test "BUG: buggy depth produces depth=1 sketch for prob=0.01 (should be 5)" do
      # After fix: depth is 5 (correct), not 1 (buggy).
      # A depth-5 sketch gives 1-(1/2)^5 = 96.9% confidence.
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYPROB", ["sk", "0.01", "0.01"], store)

      # Fixed behavior: depth is 5 (query via CMS.INFO)
      info = CMS.handle("CMS.INFO", ["sk"], store)
      depth_idx = Enum.find_index(info, &(&1 == "depth"))
      assert Enum.at(info, depth_idx + 1) == 5
    end
  end

  # ===========================================================================
  # Top-K (TOPK.*)
  # ===========================================================================

  describe "TOPK.ADD then TOPK.LIST shows top items present" do
    test "added elements appear in TOPK.LIST" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["tk", "5"], store)

      TopK.handle("TOPK.ADD", ["tk", "a", "b", "c"], store)

      result = TopK.handle("TOPK.LIST", ["tk"], store)
      assert "a" in result
      assert "b" in result
      assert "c" in result
    end

    test "highest-frequency items remain in top-K after overflow" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["tk", "3"], store)

      # Add items with descending frequency
      TopK.handle("TOPK.INCRBY", ["tk", "hot", "1000"], store)
      TopK.handle("TOPK.INCRBY", ["tk", "warm", "500"], store)
      TopK.handle("TOPK.INCRBY", ["tk", "cool", "100"], store)
      TopK.handle("TOPK.INCRBY", ["tk", "cold", "10"], store)
      TopK.handle("TOPK.INCRBY", ["tk", "frozen", "1"], store)

      result = TopK.handle("TOPK.LIST", ["tk"], store)
      assert length(result) == 3
      assert "hot" in result
      assert "warm" in result
      assert "cool" in result
    end

    test "TOPK.LIST WITHCOUNT returns interleaved element-count pairs" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["tk", "3"], store)

      TopK.handle("TOPK.INCRBY", ["tk", "x", "100"], store)
      TopK.handle("TOPK.INCRBY", ["tk", "y", "50"], store)

      result = TopK.handle("TOPK.LIST", ["tk", "WITHCOUNT"], store)
      # Should be [elem, count, elem, count, ...]
      assert length(result) == 4
      [first_elem, first_count | _] = result
      assert first_elem == "x"
      assert first_count >= 100
    end

    test "TOPK.LIST is sorted by count descending" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["tk", "5"], store)

      TopK.handle("TOPK.INCRBY", ["tk", "low", "1"], store)
      TopK.handle("TOPK.INCRBY", ["tk", "mid", "50"], store)
      TopK.handle("TOPK.INCRBY", ["tk", "high", "200"], store)

      result = TopK.handle("TOPK.LIST", ["tk"], store)
      assert hd(result) == "high"
      assert List.last(result) == "low"
    end
  end

  describe "TOPK.QUERY for non-top item returns 0" do
    test "element not in top-K returns 0" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["tk", "2"], store)

      TopK.handle("TOPK.INCRBY", ["tk", "big1", "1000"], store)
      TopK.handle("TOPK.INCRBY", ["tk", "big2", "500"], store)

      # "small" was never added
      assert [0] = TopK.handle("TOPK.QUERY", ["tk", "small"], store)
    end

    test "evicted element is no longer reported as in top-K" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["tk", "2"], store)

      TopK.handle("TOPK.INCRBY", ["tk", "a", "1"], store)
      TopK.handle("TOPK.INCRBY", ["tk", "b", "2"], store)

      # Both should be in top-K now
      assert [1] = TopK.handle("TOPK.QUERY", ["tk", "a"], store)
      assert [1] = TopK.handle("TOPK.QUERY", ["tk", "b"], store)

      # Add a much higher frequency item to evict the lowest
      TopK.handle("TOPK.INCRBY", ["tk", "c", "100"], store)

      # "c" must be in top-K, and "a" (lowest count) should be evicted
      assert [1] = TopK.handle("TOPK.QUERY", ["tk", "c"], store)

      # Check the evicted item is no longer in top-K
      list = TopK.handle("TOPK.LIST", ["tk"], store)
      refute "a" in list, "Element 'a' should have been evicted but is still in TOPK.LIST"
    end

    test "TOPK.QUERY on multiple elements returns mixed results" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["tk", "3"], store)

      TopK.handle("TOPK.ADD", ["tk", "in1", "in2", "in3"], store)

      result = TopK.handle("TOPK.QUERY", ["tk", "in1", "not_in", "in2"], store)
      assert [1, 0, 1] = result
    end
  end

  # ===========================================================================
  # All commands on non-existent key
  # ===========================================================================

  describe "all commands on non-existent key return proper error or default" do
    # --- Bloom ---

    test "BF.EXISTS on non-existent key returns 0" do
      store = MockStore.make()
      unique = "nokey_#{:erlang.unique_integer([:positive])}"
      assert 0 = Bloom.handle("BF.EXISTS", [unique, "elem"], store)
    end

    test "BF.MEXISTS on non-existent key returns all zeros" do
      store = MockStore.make()
      unique = "nokey_#{:erlang.unique_integer([:positive])}"
      assert [0, 0, 0] = Bloom.handle("BF.MEXISTS", [unique, "a", "b", "c"], store)
    end

    test "BF.CARD on non-existent key returns 0" do
      store = MockStore.make()
      assert 0 = Bloom.handle("BF.CARD", ["nokey"], store)
    end

    test "BF.INFO on non-existent key returns error" do
      store = MockStore.make()
      assert {:error, msg} = Bloom.handle("BF.INFO", ["nokey"], store)
      assert msg =~ "not found"
    end

    test "BF.ADD on non-existent key auto-creates filter" do
      store = MockStore.make()
      assert 1 = Bloom.handle("BF.ADD", ["nokey", "elem"], store)
      # Verify the filter was created by checking existence through the command
      assert 1 = Bloom.handle("BF.EXISTS", ["nokey", "elem"], store)
    end

    # --- Cuckoo ---

    test "CF.EXISTS on non-existent key returns 0" do
      store = MockStore.make()
      assert 0 = Cuckoo.handle("CF.EXISTS", ["nokey", "elem"], store)
    end

    test "CF.MEXISTS on non-existent key returns all zeros" do
      store = MockStore.make()
      assert [0, 0] = Cuckoo.handle("CF.MEXISTS", ["nokey", "a", "b"], store)
    end

    test "CF.COUNT on non-existent key returns 0" do
      store = MockStore.make()
      assert 0 = Cuckoo.handle("CF.COUNT", ["nokey", "elem"], store)
    end

    test "CF.DEL on non-existent key returns 0" do
      store = MockStore.make()
      assert 0 = Cuckoo.handle("CF.DEL", ["nokey", "elem"], store)
    end

    test "CF.INFO on non-existent key returns error" do
      store = MockStore.make()
      assert {:error, msg} = Cuckoo.handle("CF.INFO", ["nokey"], store)
      assert msg =~ "not found"
    end

    test "CF.ADD on non-existent key auto-creates filter" do
      store = MockStore.make()
      assert 1 = Cuckoo.handle("CF.ADD", ["nokey", "elem"], store)
      # Verify the filter was created by checking existence through the command
      assert 1 = Cuckoo.handle("CF.EXISTS", ["nokey", "elem"], store)
    end

    # --- CMS ---

    test "CMS.QUERY on non-existent key returns error" do
      store = MockStore.make()
      assert {:error, msg} = CMS.handle("CMS.QUERY", ["nokey", "elem"], store)
      assert msg =~ "does not exist"
    end

    test "CMS.INCRBY on non-existent key returns error" do
      store = MockStore.make()
      result = CMS.handle("CMS.INCRBY", ["nokey", "elem", "1"], store)
      assert {:error, msg} = result
      assert is_binary(msg) or msg == :enoent
      if is_binary(msg), do: assert(msg =~ "does not exist" or msg =~ "not exist")
    end

    test "CMS.INFO on non-existent key returns error" do
      store = MockStore.make()
      assert {:error, msg} = CMS.handle("CMS.INFO", ["nokey"], store)
      assert msg =~ "does not exist"
    end

    test "CMS.MERGE with non-existent source returns error" do
      store = MockStore.make()
      result = CMS.handle("CMS.MERGE", ["dst", "1", "nokey"], store)
      assert {:error, msg} = result
      assert is_binary(msg) or msg == :enoent
      if is_binary(msg), do: assert(msg =~ "does not exist" or msg =~ "not exist")
    end

    # --- TopK ---

    test "TOPK.ADD on non-existent key returns error" do
      store = MockStore.make()
      assert {:error, msg} = TopK.handle("TOPK.ADD", ["nokey", "elem"], store)
      assert msg =~ "does not exist"
    end

    test "TOPK.QUERY on non-existent key returns error" do
      store = MockStore.make()
      assert {:error, msg} = TopK.handle("TOPK.QUERY", ["nokey", "elem"], store)
      assert msg =~ "does not exist"
    end

    test "TOPK.LIST on non-existent key returns error" do
      store = MockStore.make()
      assert {:error, msg} = TopK.handle("TOPK.LIST", ["nokey"], store)
      assert msg =~ "does not exist"
    end

    test "TOPK.INFO on non-existent key returns error" do
      store = MockStore.make()
      assert {:error, msg} = TopK.handle("TOPK.INFO", ["nokey"], store)
      assert msg =~ "does not exist"
    end

    test "TOPK.INCRBY on non-existent key returns error" do
      store = MockStore.make()
      assert {:error, msg} = TopK.handle("TOPK.INCRBY", ["nokey", "elem", "1"], store)
      assert msg =~ "does not exist"
    end
  end

  # ===========================================================================
  # Wrong-type key access
  # ===========================================================================

  describe "wrong-type key access" do
    test "BF.EXISTS on CMS key returns WRONGTYPE" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mykey", "100", "5"], store)

      assert {:error, "WRONGTYPE" <> _} = Bloom.handle("BF.EXISTS", ["mykey", "elem"], store)
    end

    test "CF.EXISTS on CMS key returns WRONGTYPE" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mykey", "100", "5"], store)

      assert {:error, "WRONGTYPE" <> _} = Cuckoo.handle("CF.EXISTS", ["mykey", "elem"], store)
    end

    test "BF.ADD on TopK key returns WRONGTYPE and does not create Bloom file" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["mykey", "5"], store)

      assert {:error, "WRONGTYPE" <> _} = Bloom.handle("BF.ADD", ["mykey", "elem"], store)
      assert {:error, "WRONGTYPE" <> _} = Bloom.handle("BF.INFO", ["mykey"], store)
    end

    test "CF.ADD on TopK key returns WRONGTYPE and does not create Cuckoo file" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["mykey", "5"], store)

      assert {:error, "WRONGTYPE" <> _} = Cuckoo.handle("CF.ADD", ["mykey", "elem"], store)
      assert {:error, "WRONGTYPE" <> _} = Cuckoo.handle("CF.INFO", ["mykey"], store)
    end

    test "CMS.QUERY on a TopK key returns WRONGTYPE" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["mykey", "5"], store)

      assert {:error, "WRONGTYPE" <> _} = CMS.handle("CMS.QUERY", ["mykey", "elem"], store)
    end

    test "TOPK.ADD on a CMS key returns WRONGTYPE" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mykey", "100", "5"], store)

      assert {:error, "WRONGTYPE" <> _} = TopK.handle("TOPK.ADD", ["mykey", "elem"], store)
    end
  end

  describe "DEL removes probabilistic files" do
    test "DEL removes a Bloom key so reads no longer see the old NIF file" do
      store = MockStore.make()
      assert 1 = Bloom.handle("BF.ADD", ["mykey", "elem"], store)

      assert 1 = Strings.handle("DEL", ["mykey"], store)
      assert 0 = Bloom.handle("BF.EXISTS", ["mykey", "elem"], store)
    end

    test "DEL removes a Cuckoo key so reads no longer see the old NIF file" do
      store = MockStore.make()
      assert 1 = Cuckoo.handle("CF.ADD", ["mykey", "elem"], store)

      assert 1 = Strings.handle("DEL", ["mykey"], store)
      assert 0 = Cuckoo.handle("CF.EXISTS", ["mykey", "elem"], store)
    end

    test "DEL removes a CMS key so queries no longer see the old NIF file" do
      store = MockStore.make()
      assert :ok = CMS.handle("CMS.INITBYDIM", ["mykey", "100", "5"], store)
      assert [3] = CMS.handle("CMS.INCRBY", ["mykey", "elem", "3"], store)

      assert 1 = Strings.handle("DEL", ["mykey"], store)
      assert {:error, msg} = CMS.handle("CMS.QUERY", ["mykey", "elem"], store)
      assert msg =~ "does not exist"
    end

    test "DEL removes a TopK key so reads no longer see the old NIF file" do
      store = MockStore.make()
      assert :ok = TopK.handle("TOPK.RESERVE", ["mykey", "5"], store)
      assert [nil] = TopK.handle("TOPK.ADD", ["mykey", "elem"], store)

      assert 1 = Strings.handle("DEL", ["mykey"], store)
      assert {:error, msg} = TopK.handle("TOPK.INFO", ["mykey"], store)
      assert msg =~ "does not exist"
    end
  end

  # ===========================================================================
  # Additional edge cases
  # ===========================================================================

  describe "Bloom filter edge cases" do
    test "BF.RESERVE error_rate of exactly 0 is rejected" do
      store = MockStore.make()
      assert {:error, _} = Bloom.handle("BF.RESERVE", ["bf", "0.0", "100"], store)
    end

    test "BF.RESERVE error_rate of exactly 1 is rejected" do
      store = MockStore.make()
      assert {:error, _} = Bloom.handle("BF.RESERVE", ["bf", "1.0", "100"], store)
    end

    test "BF.MADD with duplicates in the same call" do
      store = MockStore.make()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      result = Bloom.handle("BF.MADD", ["bf", "x", "x"], store)
      # First add returns 1 (new bits set), second returns 0 (no new bits)
      assert result == [1, 0]
    end
  end

  describe "Cuckoo filter edge cases" do
    test "CF.RESERVE with capacity 0 returns error" do
      store = MockStore.make()
      assert {:error, _} = Cuckoo.handle("CF.RESERVE", ["cf", "0"], store)
    end

    test "add-delete-add cycle works correctly" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)

      Cuckoo.handle("CF.ADD", ["cf", "cycle"], store)
      assert 1 = Cuckoo.handle("CF.EXISTS", ["cf", "cycle"], store)

      Cuckoo.handle("CF.DEL", ["cf", "cycle"], store)
      assert 0 = Cuckoo.handle("CF.EXISTS", ["cf", "cycle"], store)

      Cuckoo.handle("CF.ADD", ["cf", "cycle"], store)
      assert 1 = Cuckoo.handle("CF.EXISTS", ["cf", "cycle"], store)
    end

    test "CF.INFO tracks insertions and deletions" do
      store = MockStore.make()
      Cuckoo.handle("CF.RESERVE", ["cf", "1024"], store)
      Cuckoo.handle("CF.ADD", ["cf", "a"], store)
      Cuckoo.handle("CF.ADD", ["cf", "b"], store)
      Cuckoo.handle("CF.DEL", ["cf", "a"], store)

      info = to_info_map(Cuckoo.handle("CF.INFO", ["cf"], store))
      assert info["Number of items inserted"] == 1
      assert info["Number of items deleted"] == 1
    end
  end

  describe "CMS edge cases" do
    test "CMS.INITBYDIM with width 0 returns error" do
      store = MockStore.make()
      assert {:error, _} = CMS.handle("CMS.INITBYDIM", ["sk", "0", "5"], store)
    end

    test "CMS.INITBYDIM with depth 0 returns error" do
      store = MockStore.make()
      assert {:error, _} = CMS.handle("CMS.INITBYDIM", ["sk", "100", "0"], store)
    end

    test "CMS.INCRBY with count 0 returns error" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["sk", "100", "5"], store)
      assert {:error, _} = CMS.handle("CMS.INCRBY", ["sk", "elem", "0"], store)
    end

    test "CMS.INFO reflects total count after multiple increments" do
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["sk", "100", "5"], store)

      CMS.handle("CMS.INCRBY", ["sk", "a", "10"], store)
      CMS.handle("CMS.INCRBY", ["sk", "b", "20"], store)
      CMS.handle("CMS.INCRBY", ["sk", "a", "5"], store)

      result = CMS.handle("CMS.INFO", ["sk"], store)
      assert ["width", 100, "depth", 5, "count", 35] = result
    end

    test "CMS.INITBYPROB rejects probability of 0" do
      store = MockStore.make()
      assert {:error, _} = CMS.handle("CMS.INITBYPROB", ["sk", "0.01", "0.0"], store)
    end

    test "CMS.INITBYPROB rejects probability of 1" do
      store = MockStore.make()
      assert {:error, _} = CMS.handle("CMS.INITBYPROB", ["sk", "0.01", "1.0"], store)
    end
  end

  describe "TopK edge cases" do
    test "TOPK.RESERVE with k=1 tracks single most frequent element" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["tk", "1"], store)

      TopK.handle("TOPK.INCRBY", ["tk", "a", "100"], store)
      assert ["a"] = TopK.handle("TOPK.LIST", ["tk"], store)

      # Higher frequency replaces it
      TopK.handle("TOPK.INCRBY", ["tk", "b", "200"], store)
      assert ["b"] = TopK.handle("TOPK.LIST", ["tk"], store)
      assert [0] = TopK.handle("TOPK.QUERY", ["tk", "a"], store)
    end

    test "TOPK.ADD returns eviction info per element" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["tk", "2"], store)

      # Fill the heap
      result1 = TopK.handle("TOPK.ADD", ["tk", "a", "b"], store)
      assert length(result1) == 2
      assert Enum.all?(result1, &is_nil/1)

      # Adding without exceeding min count should not evict
      result2 = TopK.handle("TOPK.ADD", ["tk", "c"], store)
      assert length(result2) == 1
    end

    test "TOPK.INCRBY with zero count returns error" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["tk", "5"], store)
      assert {:error, _} = TopK.handle("TOPK.INCRBY", ["tk", "elem", "0"], store)
    end

    test "TOPK.LIST on empty tracker returns empty list" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["tk", "5"], store)
      assert [] = TopK.handle("TOPK.LIST", ["tk"], store)
    end

    test "TOPK.INFO returns correct default dimensions" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["tk", "10"], store)

      result = TopK.handle("TOPK.INFO", ["tk"], store)
      assert ["k", 10, "width", 8, "depth", 7, "decay", 0.9] = result
    end
  end

  # ===========================================================================
  # Helpers
  # ===========================================================================

  defp to_info_map(list) do
    list
    |> Enum.chunk_every(2)
    |> Enum.into(%{}, fn [k, v] -> {k, v} end)
  end
end
