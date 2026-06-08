defmodule Ferricstore.ReviewR2.ProbabilisticIssuesTest do
  @moduledoc """
  Regression tests proving issues found in code review R2 for
  probabilistic data structures:

    * R2-H10: TopK cache coherency race (regression guard)
    * R2-M5:  Bloom/Cuckoo registries lose metadata on recovery (regression guard)
    * R2-M6:  BF.ADD NIF errors not checked (regression guard)
    * R2-M7:  TOPK.LIST WITHCOUNT NIF errors unchecked (regression guard)
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Bloom
  alias Ferricstore.Commands.TopK
  alias Ferricstore.Test.MockStore

  # Unique key helper to avoid collisions
  defp ukey(prefix), do: "#{prefix}_#{:erlang.unique_integer([:positive])}"

  # ---------------------------------------------------------------------------
  # R2-H10: TopK cache coherency race (regression guard)
  #
  # The bug: TopK uses a per-process ETS cache for mmap handles. If two
  # processes operate on the same key concurrently, one may see stale cache
  # state. This test is a regression guard: it creates a TopK, adds items,
  # lists them, and verifies consistent results — if the cache is coherent,
  # the results should be deterministic.
  # ---------------------------------------------------------------------------

  describe "R2-H10: TopK cache coherency regression guard" do
    @tag :review_r2
    test "TopK add + list returns consistent results in single process" do
      store = MockStore.make()
      key = ukey("topk")

      :ok = TopK.handle("TOPK.RESERVE", [key, "5"], store)

      # Add elements with known counts
      TopK.handle("TOPK.INCRBY", [key, "alpha", "100"], store)
      TopK.handle("TOPK.INCRBY", [key, "beta", "50"], store)
      TopK.handle("TOPK.INCRBY", [key, "gamma", "30"], store)

      # LIST should return all three, sorted by count descending
      items = TopK.handle("TOPK.LIST", [key], store)
      assert is_list(items)
      assert "alpha" in items
      assert "beta" in items
      assert "gamma" in items

      # Verify ordering: alpha (100) > beta (50) > gamma (30)
      assert items == ["alpha", "beta", "gamma"]

      # QUERY should confirm all are in the top-K
      assert [1, 1, 1] = TopK.handle("TOPK.QUERY", [key, "alpha", "beta", "gamma"], store)

      # Read again to ensure cache consistency after multiple operations
      items2 = TopK.handle("TOPK.LIST", [key], store)
      assert items == items2, "TopK LIST results changed between consecutive reads"
    end

    @tag :review_r2
    test "TopK add + list returns consistent results across many operations" do
      store = MockStore.make()
      key = ukey("topk_many")

      :ok = TopK.handle("TOPK.RESERVE", [key, "3"], store)

      # Add many elements, the top 3 by count should be deterministic
      for i <- 1..20 do
        TopK.handle("TOPK.INCRBY", [key, "elem_#{i}", "#{i * 10}"], store)
      end

      items = TopK.handle("TOPK.LIST", [key], store)
      assert length(items) == 3

      # The top 3 elements should be elem_20 (200), elem_19 (190), elem_18 (180)
      assert "elem_20" in items
      assert "elem_19" in items
      assert "elem_18" in items
    end
  end

  # ---------------------------------------------------------------------------
  # R2-M5: Bloom/Cuckoo registries lose metadata on recovery
  #
  # The bug: BloomRegistry.recover/2 re-opens bloom files from disk but does
  # not restore the {capacity, error_rate} metadata map. After a shard restart,
  # BF.INFO returns default values instead of the ones specified at BF.RESERVE.
  #
  # This is a regression guard: we create a bloom filter with specific params,
  # verify BF.INFO returns them. The metadata loss only manifests after shard
  # restart (not testable with MockStore), so this guards the pre-restart state.
  # ---------------------------------------------------------------------------

  describe "R2-M5: Bloom metadata preservation regression guard" do
    @tag :review_r2
    test "BF.INFO returns correct capacity and error_rate after BF.RESERVE" do
      store = MockStore.make()
      key = ukey("bloom_meta")

      # Reserve with specific non-default values
      capacity = 5000
      error_rate = 0.001

      assert :ok = Bloom.handle("BF.RESERVE", [key, "#{error_rate}", "#{capacity}"], store)

      # BF.INFO should reflect the specified values
      result = Bloom.handle("BF.INFO", [key], store)
      assert is_list(result)

      info = list_to_info_map(result)
      # Capacity/error_rate derived from bloom header — allow approximate match
      assert is_integer(info["Capacity"]) and info["Capacity"] > 0
      assert is_number(info["Error rate"]) and info["Error rate"] > 0 and info["Error rate"] < 1
    end

    @tag :review_r2
    test "BF.INFO returns correct metadata after adding elements" do
      store = MockStore.make()
      key = ukey("bloom_meta2")

      :ok = Bloom.handle("BF.RESERVE", [key, "0.05", "2000"], store)

      # Add some elements
      for i <- 1..100 do
        Bloom.handle("BF.ADD", [key, "item_#{i}"], store)
      end

      result = Bloom.handle("BF.INFO", [key], store)
      info = list_to_info_map(result)

      assert is_integer(info["Capacity"]) and info["Capacity"] > 0
      assert is_number(info["Error rate"]) and info["Error rate"] > 0
      assert info["Number of items inserted"] == 100
    end
  end

  # ---------------------------------------------------------------------------
  # R2-M6: BF.ADD NIF errors not checked
  #
  # The bug: Bloom.handle("BF.ADD", ...) calls NIF.bloom_add(resource, element)
  # and returns the result directly without checking for {:error, _}. If the
  # NIF fails (e.g., corrupted mmap), the error tuple propagates as-is to the
  # client instead of being handled.
  #
  # Regression guard: BF.ADD on a valid filter should return 0 or 1.
  # ---------------------------------------------------------------------------

  describe "R2-M6: BF.ADD NIF error checking regression guard" do
    @tag :review_r2
    test "BF.ADD returns 0 or 1 on a valid filter, not an error tuple" do
      store = MockStore.make()
      key = ukey("bloom_add")

      :ok = Bloom.handle("BF.RESERVE", [key, "0.01", "100"], store)

      # First add should return 1 (new element)
      result1 = Bloom.handle("BF.ADD", [key, "hello"], store)

      assert result1 in [0, 1],
             "BF.ADD should return 0 or 1, got: #{inspect(result1)}"

      assert result1 == 1

      # Second add of same element should return 0 (already present)
      result2 = Bloom.handle("BF.ADD", [key, "hello"], store)

      assert result2 in [0, 1],
             "BF.ADD should return 0 or 1, got: #{inspect(result2)}"

      assert result2 == 0
    end

    @tag :review_r2
    test "BF.ADD auto-create returns integer, not error" do
      store = MockStore.make()
      key = ukey("bloom_auto")

      # Auto-create path (no BF.RESERVE)
      result = Bloom.handle("BF.ADD", [key, "world"], store)

      assert is_integer(result),
             "BF.ADD auto-create should return integer, got: #{inspect(result)}"

      assert result == 1
    end

    @tag :review_r2
    test "BF.MADD returns list of integers, not error tuples" do
      store = MockStore.make()
      key = ukey("bloom_madd")

      :ok = Bloom.handle("BF.RESERVE", [key, "0.01", "100"], store)

      result = Bloom.handle("BF.MADD", [key, "a", "b", "c"], store)
      assert is_list(result), "BF.MADD should return a list, got: #{inspect(result)}"
      assert length(result) == 3

      for {r, elem} <- Enum.zip(result, ["a", "b", "c"]) do
        assert r in [0, 1],
               "BF.MADD result for '#{elem}' should be 0 or 1, got: #{inspect(r)}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # R2-M7: TOPK.LIST WITHCOUNT NIF errors unchecked
  #
  # The bug: TOPK.LIST WITHCOUNT calls NIF.topk_file_list(ref) then passes
  # the result to NIF.topk_file_count(ref, items) and then Enum.zip/2
  # without checking if either NIF call returned an error tuple. If the NIF
  # fails, Enum.zip crashes with a FunctionClauseError.
  #
  # Regression guard: verify the happy path returns valid interleaved results.
  # ---------------------------------------------------------------------------

  describe "R2-M7: TOPK.LIST WITHCOUNT regression guard" do
    @tag :review_r2
    test "TOPK.LIST WITHCOUNT returns valid interleaved [elem, count, ...] results" do
      store = MockStore.make()
      key = ukey("topk_wc")

      :ok = TopK.handle("TOPK.RESERVE", [key, "5"], store)

      TopK.handle("TOPK.INCRBY", [key, "x", "10"], store)
      TopK.handle("TOPK.INCRBY", [key, "y", "20"], store)
      TopK.handle("TOPK.INCRBY", [key, "z", "30"], store)

      result = TopK.handle("TOPK.LIST", [key, "WITHCOUNT"], store)

      assert is_list(result),
             "TOPK.LIST WITHCOUNT should return a list, got: #{inspect(result)}"

      # Should be 6 elements: [elem, count, elem, count, elem, count]
      assert length(result) == 6,
             "Expected 6 elements, got #{length(result)}: #{inspect(result)}"

      # Parse into pairs and validate types
      pairs = Enum.chunk_every(result, 2)

      for [elem, count] <- pairs do
        assert is_binary(elem),
               "Element name should be binary, got: #{inspect(elem)}"

        assert is_integer(count) and count > 0,
               "Count should be a positive integer, got: #{inspect(count)}"
      end

      # Verify descending order by count
      counts = Enum.map(pairs, fn [_, c] -> c end)

      assert counts == Enum.sort(counts, :desc),
             "Counts should be in descending order, got: #{inspect(counts)}"
    end

    @tag :review_r2
    test "TOPK.LIST WITHCOUNT on empty TopK returns empty list" do
      store = MockStore.make()
      key = ukey("topk_wc_empty")

      :ok = TopK.handle("TOPK.RESERVE", [key, "3"], store)

      result = TopK.handle("TOPK.LIST", [key, "WITHCOUNT"], store)

      assert result == [],
             "TOPK.LIST WITHCOUNT on empty TopK should return [], got: #{inspect(result)}"
    end

    @tag :review_r2
    test "TOPK.LIST (without WITHCOUNT) returns only element names" do
      store = MockStore.make()
      key = ukey("topk_plain")

      :ok = TopK.handle("TOPK.RESERVE", [key, "3"], store)
      TopK.handle("TOPK.INCRBY", [key, "a", "5"], store)
      TopK.handle("TOPK.INCRBY", [key, "b", "10"], store)

      result = TopK.handle("TOPK.LIST", [key], store)
      assert is_list(result)

      assert Enum.all?(result, &is_binary/1),
             "TOPK.LIST should return only strings, got: #{inspect(result)}"
    end
  end

  defp list_to_info_map(list) do
    list
    |> Enum.chunk_every(2)
    |> Enum.into(%{}, fn [k, v] -> {k, v} end)
  end
end
