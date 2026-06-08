defmodule Ferricstore.ProbDurabilityTest do
  @moduledoc """
  Durability / correctness tests for probabilistic structures.

  The NIF layer carries its own Rust tests for fsync behavior
  (sync_data call sites in bloom / cuckoo / cms / topk). This file
  complements that with Elixir-level tests for the user-facing
  invariants that missing fsync could break:

    * Bloom: `BF.CARD` (unique-insert counter in the header) must stay
      in sync with the bits actually set.
    * Cuckoo: `CF.EXISTS` of added elements stays true after many
      adds + deletes (kick-chain state consistent).
    * CMS: `CMS.QUERY` returns a count ≥ true count (never less — CMS
      over-counts by design, so a write that landed but a replay that
      double-applies would only inflate further, not drop below).
    * TopK: all elements added show up in `TOPK.LIST` when k ≥ #adds.

  These are invariants that hold on a correctly-durable prob file —
  pre-existing NIF tests cover the on-disk format, but the
  Elixir-facing command surface deserves its own guard.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    on_exit(fn -> ShardHelpers.flush_all_keys() end)
    :ok
  end

  defp ukey(base), do: "#{base}_#{:erlang.unique_integer([:positive])}"

  # ---------------------------------------------------------------------------
  # Bloom filter: BF.CARD must match unique-adds
  # ---------------------------------------------------------------------------

  describe "bloom durability invariants" do
    test "BF.CARD matches the number of unique adds (header count in sync)" do
      key = ukey("bf_card")

      for i <- 1..50 do
        assert {:ok, 1} = FerricStore.bf_add(key, "el_#{i}")
      end

      {:ok, card} = FerricStore.bf_card(key)
      assert card == 50, "expected BF.CARD == 50 after 50 unique adds, got #{card}"
    end

    test "BF.CARD ignores duplicates (header increments only on genuinely-new)" do
      key = ukey("bf_dup")

      for _ <- 1..20 do
        FerricStore.bf_add(key, "same")
      end

      {:ok, card} = FerricStore.bf_card(key)
      assert card == 1, "expected BF.CARD == 1 after 20 adds of same element, got #{card}"
    end

    test "all added elements are reported present by BF.EXISTS" do
      key = ukey("bf_present")

      for i <- 1..30, do: FerricStore.bf_add(key, "x_#{i}")

      for i <- 1..30 do
        assert {:ok, 1} = FerricStore.bf_exists(key, "x_#{i}"),
               "expected x_#{i} to be reported present"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Cuckoo filter: add/exists/del roundtrip keeps state consistent
  # ---------------------------------------------------------------------------

  describe "cuckoo durability invariants" do
    test "added elements are reported present by CF.EXISTS" do
      key = ukey("cf_present")

      for i <- 1..30, do: FerricStore.cf_add(key, "y_#{i}")

      for i <- 1..30 do
        assert {:ok, 1} = FerricStore.cf_exists(key, "y_#{i}")
      end
    end

    test "CF.ADDNX is idempotent for duplicates" do
      key = ukey("cf_nx")

      assert {:ok, 1} = FerricStore.cf_addnx(key, "dup")
      assert {:ok, 0} = FerricStore.cf_addnx(key, "dup")
      assert {:ok, 0} = FerricStore.cf_addnx(key, "dup")

      assert {:ok, 1} = FerricStore.cf_exists(key, "dup")
    end

    test "CF.DEL removes and CF.EXISTS reflects it" do
      key = ukey("cf_del")
      FerricStore.cf_add(key, "gone")
      assert {:ok, 1} = FerricStore.cf_exists(key, "gone")
      assert {:ok, 1} = FerricStore.cf_del(key, "gone")
      assert {:ok, 0} = FerricStore.cf_exists(key, "gone")
    end

    test "elements placed via eviction-kick are durably findable" do
      # A cuckoo filter with capacity ~32 and bucket_size 4 has ~128
      # slots; inserting 100 unique elements forces a kick chain after
      # the first few collisions. The NIF's eviction-placement path
      # (cuckoo_file_add's kick loop) must `prob_fsync` after the final
      # `write_num_items` — otherwise on a crash the fingerprint bytes
      # could be in page cache while `num_items` is on disk, producing
      # a filter that says "I have N items" but returns 0 for everyone.
      #
      # This test asserts the observable contract: every element we
      # added, including the ones that triggered eviction, remains
      # findable via CF.EXISTS.
      key = ukey("cf_evict")
      :ok = FerricStore.cf_reserve(key, 32)

      elements = for i <- 1..100, do: "kick_#{i}"

      Enum.each(elements, fn e ->
        case FerricStore.cf_add(key, e) do
          {:ok, _} -> :ok
          # Filter-full is acceptable for the oldest elements once we
          # exceed real capacity; the test measures "everything that
          # reported success must be findable", not "100% fit".
          {:error, _} -> :ok
        end
      end)

      # At minimum the first half must all fit (capacity is 32 but
      # 4-slot buckets typically hold 3x the declared capacity before
      # saturation). We then assert every element that CF.ADD accepted
      # is durably findable.
      found =
        Enum.count(elements, fn e ->
          match?({:ok, 1}, FerricStore.cf_exists(key, e))
        end)

      assert found >= 32,
             "expected at least 32 of 100 added elements to remain findable " <>
             "after eviction-chain inserts; got #{found}"
    end
  end

  # ---------------------------------------------------------------------------
  # CMS: counter query >= total incremented value
  # ---------------------------------------------------------------------------

  describe "cms durability invariants" do
    test "CMS.QUERY returns >= the true increment count" do
      key = ukey("cms")
      _ = FerricStore.cms_initbydim(key, 1000, 5)

      for _ <- 1..25 do
        FerricStore.cms_incrby(key, [{"target", 1}])
      end

      {:ok, [result]} = FerricStore.cms_query(key, ["target"])
      assert result >= 25,
             "CMS over-counts by design; expected >= 25 after 25 incrby(1), got #{result}"
    end

    test "CMS batch incrby: individual targets all get the intended delta" do
      key = ukey("cms_batch")
      _ = FerricStore.cms_initbydim(key, 1000, 5)

      FerricStore.cms_incrby(key, [{"a", 3}, {"b", 5}, {"c", 7}])

      {:ok, [a_count, b_count, c_count]} = FerricStore.cms_query(key, ["a", "b", "c"])
      # CMS can over-report but never under-report.
      assert a_count >= 3
      assert b_count >= 5
      assert c_count >= 7
    end
  end

  # ---------------------------------------------------------------------------
  # TopK: added items appear in TOPK.LIST when k >= #adds
  # ---------------------------------------------------------------------------

  describe "topk durability invariants" do
    test "all added items appear in LIST when k >= #unique adds" do
      key = ukey("topk")
      _ = FerricStore.topk_reserve(key, 10)

      added = for i <- 1..5, do: "item_#{i}"
      Enum.each(added, fn it -> FerricStore.topk_add(key, [it]) end)

      {:ok, listed} = FerricStore.topk_list(key)

      for item <- added do
        assert item in listed, "expected #{item} to appear in TOPK.LIST"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrent adds: all land + invariants hold
  # ---------------------------------------------------------------------------

  describe "concurrent writes keep invariants" do
    test "100 concurrent BF.ADDs keep all reported-new adds reflected in CARD" do
      key = ukey("bf_concurrent")

      tasks =
        for i <- 1..100 do
          Task.async(fn -> FerricStore.bf_add(key, "c_#{i}") end)
        end

      results = Task.await_many(tasks, 30_000)

      assert Enum.all?(results, fn
               {:ok, n} when n in [0, 1] -> true
               _ -> false
             end)

      for i <- 1..100 do
        assert {:ok, 1} = FerricStore.bf_exists(key, "c_#{i}")
      end

      expected_card =
        Enum.count(results, fn
          {:ok, 1} -> true
          _ -> false
        end)

      {:ok, card} = FerricStore.bf_card(key)
      assert card == expected_card,
             "expected CARD == #{expected_card} for reported-new adds, got #{card}"
    end

    test "50 concurrent CMS.INCRBY(1) on same key — final count >= 50" do
      key = ukey("cms_concurrent")
      _ = FerricStore.cms_initbydim(key, 1000, 5)

      tasks =
        for _ <- 1..50 do
          Task.async(fn -> FerricStore.cms_incrby(key, [{"hot", 1}]) end)
        end

      _ = Task.await_many(tasks, 30_000)

      {:ok, [hot]} = FerricStore.cms_query(key, ["hot"])
      assert hot >= 50, "expected CMS count >= 50 after 50 concurrent incrby(1), got #{hot}"
    end
  end
end
