defmodule Ferricstore.EmbeddedExtendedDataStructuresTest do
  @moduledoc """
  Advanced data structure and probabilistic tests extracted from
  EmbeddedExtendedTest.

  Covers: HyperLogLog, Bloom filter, Cuckoo filter, Count-Min Sketch,
  TopK, T-Digest, JSON, Geo, and embedded value size limits.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    :ok
  end

  # ===========================================================================
  # HyperLogLog smoke tests
  # ===========================================================================

  describe "HyperLogLog" do
    test "pfadd and pfcount" do
      assert {:ok, true} = FerricStore.pfadd("hll:key", ["a", "b", "c"])
      assert {:ok, count} = FerricStore.pfcount(["hll:key"])
      assert is_integer(count) and count >= 3
    end

    test "pfadd with duplicate elements" do
      FerricStore.pfadd("hll:dup", ["a", "b"])
      assert {:ok, false} = FerricStore.pfadd("hll:dup", ["a", "b"])
    end

    test "pfmerge combines HLLs" do
      FerricStore.pfadd("hll:m1", ["a", "b"])
      FerricStore.pfadd("hll:m2", ["c", "d"])
      assert :ok = FerricStore.pfmerge("hll:merged", ["hll:m1", "hll:m2"])
      assert {:ok, count} = FerricStore.pfcount(["hll:merged"])
      assert count >= 4
    end
  end

  # ===========================================================================
  # Bloom Filter smoke tests
  # ===========================================================================

  describe "Bloom filter" do
    test "bf_add and bf_exists" do
      assert {:ok, 1} = FerricStore.bf_add("bf:key", "hello")
      assert {:ok, 1} = FerricStore.bf_exists("bf:key", "hello")
      assert {:ok, 0} = FerricStore.bf_exists("bf:key", "unknown_element_xyz")
    end

    test "bf_madd and bf_mexists" do
      assert {:ok, results} = FerricStore.bf_madd("bf:multi", ["a", "b", "c"])
      assert is_list(results)
      assert {:ok, checks} = FerricStore.bf_mexists("bf:multi", ["a", "z"])
      assert is_list(checks)
      assert hd(checks) == 1
    end

    test "bf_reserve creates filter with specific parameters" do
      assert :ok = FerricStore.bf_reserve("bf:reserved", 0.01, 1000)
      assert {:ok, 1} = FerricStore.bf_add("bf:reserved", "test")
      assert {:ok, 1} = FerricStore.bf_exists("bf:reserved", "test")
    end

    test "bf_card returns count" do
      FerricStore.bf_add("bf:card", "a")
      FerricStore.bf_add("bf:card", "b")
      assert {:ok, count} = FerricStore.bf_card("bf:card")
      assert is_integer(count)
    end
  end

  # ===========================================================================
  # Cuckoo Filter smoke tests
  # ===========================================================================

  describe "Cuckoo filter" do
    test "cf_add and cf_exists" do
      assert {:ok, 1} = FerricStore.cf_add("cf:key", "hello")
      assert {:ok, 1} = FerricStore.cf_exists("cf:key", "hello")
      assert {:ok, 0} = FerricStore.cf_exists("cf:key", "unknown_element_xyz")
    end

    test "cf_del removes element" do
      FerricStore.cf_add("cf:del", "item")
      assert {:ok, 1} = FerricStore.cf_del("cf:del", "item")
      assert {:ok, 0} = FerricStore.cf_exists("cf:del", "item")
    end

    test "cf_addnx adds only if not present" do
      assert {:ok, 1} = FerricStore.cf_addnx("cf:nx", "item")
      assert {:ok, 0} = FerricStore.cf_addnx("cf:nx", "item")
    end

    test "cf_count returns count" do
      FerricStore.cf_add("cf:cnt", "item")
      assert {:ok, count} = FerricStore.cf_count("cf:cnt", "item")
      assert count >= 1
    end

    test "cf_mexists checks multiple elements" do
      FerricStore.cf_add("cf:mx", "a")
      assert {:ok, [1, 0]} = FerricStore.cf_mexists("cf:mx", ["a", "z"])
    end
  end

  # ===========================================================================
  # Count-Min Sketch smoke tests
  # ===========================================================================

  describe "Count-Min Sketch" do
    test "cms_initbydim, incrby, query" do
      assert :ok = FerricStore.cms_initbydim("cms:key", 100, 5)
      assert {:ok, counts} = FerricStore.cms_incrby("cms:key", [{"a", 5}, {"b", 3}])
      assert is_list(counts)
      assert {:ok, queried} = FerricStore.cms_query("cms:key", ["a", "b"])
      assert is_list(queried)
      assert Enum.at(queried, 0) >= 5
      assert Enum.at(queried, 1) >= 3
    end

    test "cms_initbyprob creates CMS" do
      assert :ok = FerricStore.cms_initbyprob("cms:prob", 0.01, 0.001)
    end
  end

  # ===========================================================================
  # TopK smoke tests
  # ===========================================================================

  describe "TopK" do
    test "topk_reserve, add, list" do
      assert :ok = FerricStore.topk_reserve("tk:key", 3)
      assert {:ok, _evicted} = FerricStore.topk_add("tk:key", ["a", "b", "c", "a", "a"])
      assert {:ok, top} = FerricStore.topk_list("tk:key")
      assert is_list(top)
      assert "a" in top
    end

    test "topk_query checks membership" do
      FerricStore.topk_reserve("tk:q", 3)
      FerricStore.topk_add("tk:q", ["x", "y", "z"])
      assert {:ok, results} = FerricStore.topk_query("tk:q", ["x", "missing"])
      assert is_list(results)
    end
  end

  # ===========================================================================
  # T-Digest smoke tests
  # ===========================================================================

  describe "T-Digest" do
    test "create, add, quantile, min, max" do
      assert :ok = FerricStore.tdigest_create("td:key")
      assert :ok = FerricStore.tdigest_add("td:key", [1.0, 2.0, 3.0, 4.0, 5.0])

      assert {:ok, quantiles} = FerricStore.tdigest_quantile("td:key", [0.5])
      assert is_list(quantiles)

      assert {:ok, min_val} = FerricStore.tdigest_min("td:key")
      assert is_binary(min_val)

      assert {:ok, max_val} = FerricStore.tdigest_max("td:key")
      assert is_binary(max_val)
    end

    test "tdigest_cdf returns CDF values" do
      FerricStore.tdigest_create("td:cdf")
      FerricStore.tdigest_add("td:cdf", [1.0, 2.0, 3.0, 4.0, 5.0])
      assert {:ok, cdfs} = FerricStore.tdigest_cdf("td:cdf", [3.0])
      assert is_list(cdfs)
    end

    test "tdigest_reset clears data" do
      FerricStore.tdigest_create("td:reset")
      FerricStore.tdigest_add("td:reset", [1.0, 2.0])
      assert :ok = FerricStore.tdigest_reset("td:reset")
    end

    test "tdigest_info returns metadata" do
      FerricStore.tdigest_create("td:info")
      assert {:ok, info} = FerricStore.tdigest_info("td:info")
      assert is_list(info)
    end
  end

  # Geo smoke tests
  # ===========================================================================

  describe "Geo" do
    test "geoadd and geodist" do
      assert {:ok, 2} =
               FerricStore.geoadd("geo:key", [
                 {13.361389, 38.115556, "Palermo"},
                 {15.087269, 37.502669, "Catania"}
               ])

      assert {:ok, dist} = FerricStore.geodist("geo:key", "Palermo", "Catania", "m")
      assert is_binary(dist)
      {d, _} = Float.parse(dist)
      assert d > 100_000
    end

    test "geopos returns positions" do
      FerricStore.geoadd("geo:pos", [{13.361389, 38.115556, "Palermo"}])
      assert {:ok, positions} = FerricStore.geopos("geo:pos", ["Palermo"])
      assert is_list(positions)
    end

    test "geohash returns hash strings" do
      FerricStore.geoadd("geo:hash", [{13.361389, 38.115556, "Palermo"}])
      assert {:ok, hashes} = FerricStore.geohash("geo:hash", ["Palermo"])
      assert is_list(hashes)
    end
  end

  # ===========================================================================
  # VALUE SIZE LIMITS (embedded mode)
  # ===========================================================================

  describe "embedded value size limits" do
    test "set rejects value larger than max_value_size" do
      snapshot =
        Ferricstore.Test.ShardHelpers.replace_default_apply_context(max_value_size: 100)

      on_exit(fn ->
        Ferricstore.Test.ShardHelpers.restore_default_apply_context(snapshot)
      end)

      large_value = :binary.copy("x", 101)
      assert {:error, msg} = FerricStore.set("valsize:big", large_value)
      assert msg =~ "value too large"
      assert msg =~ "101 bytes"
      assert msg =~ "max 100 bytes"
    end

    test "set accepts value exactly at max_value_size" do
      snapshot =
        Ferricstore.Test.ShardHelpers.replace_default_apply_context(max_value_size: 100)

      on_exit(fn ->
        Ferricstore.Test.ShardHelpers.restore_default_apply_context(snapshot)
      end)

      exact_value = :binary.copy("x", 100)
      assert :ok = FerricStore.set("valsize:exact", exact_value)
      assert {:ok, ^exact_value} = FerricStore.get("valsize:exact")
    end

    test "set uses default 1MB limit when no config is set" do
      snapshot =
        Ferricstore.Test.ShardHelpers.replace_default_apply_context(max_value_size: 1_048_576)

      on_exit(fn ->
        Ferricstore.Test.ShardHelpers.restore_default_apply_context(snapshot)
      end)

      # 1 MB should succeed
      mb_value = :binary.copy("y", 1_048_576)
      assert :ok = FerricStore.set("valsize:1mb", mb_value)

      # 1 MB + 1 byte should fail
      over_value = :binary.copy("z", 1_048_577)
      assert {:error, msg} = FerricStore.set("valsize:over1mb", over_value)
      assert msg =~ "value too large"
    end
  end
end
