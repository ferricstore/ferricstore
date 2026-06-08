defmodule Ferricstore.Commands.TDigestTest.Sections.TdigestCdf do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.Hash
      alias Ferricstore.Commands.TDigest, as: TDigestCmd
      alias Ferricstore.TDigest.Core
      alias Ferricstore.Test.MockStore

  describe "TDIGEST.CDF" do
    test "returns nan for empty digest" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      assert ["nan"] = TDigestCmd.handle("TDIGEST.CDF", ["mydigest", "50.0"], store)
    end

    test "CDF of min value is approximately 0" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      values = Enum.map(1..1000, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      [result] = TDigestCmd.handle("TDIGEST.CDF", ["mydigest", "1.0"], store)
      cdf_min = parse_float_str(result)
      assert cdf_min < 0.05, "CDF(min) should be near 0, got #{cdf_min}"
    end

    test "CDF of max value is approximately 1" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      values = Enum.map(1..1000, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      [result] = TDigestCmd.handle("TDIGEST.CDF", ["mydigest", "1000.0"], store)
      cdf_max = parse_float_str(result)
      assert cdf_max > 0.95, "CDF(max) should be near 1, got #{cdf_max}"
    end

    test "CDF of median is approximately 0.5" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      values = Enum.map(1..1000, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      [result] = TDigestCmd.handle("TDIGEST.CDF", ["mydigest", "500.0"], store)
      cdf_med = parse_float_str(result)

      assert_in_delta cdf_med,
                      0.5,
                      0.05,
                      "CDF(500) of uniform [1..1000] should be ~0.5, got #{cdf_med}"
    end

    test "CDF of value below min is 0" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "10.0", "20.0", "30.0"], store)

      [result] = TDigestCmd.handle("TDIGEST.CDF", ["mydigest", "5.0"], store)
      assert parse_float_str(result) == 0.0
    end

    test "CDF of value above max is 1" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "10.0", "20.0", "30.0"], store)

      [result] = TDigestCmd.handle("TDIGEST.CDF", ["mydigest", "50.0"], store)
      assert parse_float_str(result) == 1.0
    end

    test "handles multiple CDF values in one call" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      values = Enum.map(1..1000, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      results = TDigestCmd.handle("TDIGEST.CDF", ["mydigest", "250.0", "500.0", "750.0"], store)
      assert length(results) == 3

      [cdf25, cdf50, cdf75] = Enum.map(results, &parse_float_str/1)
      assert cdf25 < cdf50
      assert cdf50 < cdf75
    end

    test "CDF is monotonically increasing" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      values = Enum.map(1..1000, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      test_values = Enum.map(0..20, fn i -> Float.to_string(i * 50.0) end)
      results = TDigestCmd.handle("TDIGEST.CDF", ["mydigest" | test_values], store)
      floats = Enum.map(results, &parse_float_str/1)

      pairs = Enum.zip(floats, tl(floats))

      Enum.each(pairs, fn {a, b} ->
        assert a <= b, "CDF should be monotonically increasing: #{a} > #{b}"
      end)
    end

    test "CDF accuracy with 10K samples" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      values = Enum.map(1..10_000, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      [result] = TDigestCmd.handle("TDIGEST.CDF", ["mydigest", "5000.0"], store)
      cdf_50 = parse_float_str(result)
      assert_in_delta cdf_50, 0.5, 0.05

      [result] = TDigestCmd.handle("TDIGEST.CDF", ["mydigest", "9900.0"], store)
      cdf_99 = parse_float_str(result)
      assert_in_delta cdf_99, 0.99, 0.02
    end

    test "returns error for non-existent key" do
      store = MockStore.make()
      assert {:error, msg} = TDigestCmd.handle("TDIGEST.CDF", ["missing", "50.0"], store)
      assert msg =~ "does not exist"
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = TDigestCmd.handle("TDIGEST.CDF", [], store)
      assert {:error, _} = TDigestCmd.handle("TDIGEST.CDF", ["key"], store)
    end
  end
  describe "TDIGEST.MERGE" do
    test "merges two digests" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["src1"], store)
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["src2"], store)

      values1 = Enum.map(1..500, &Integer.to_string/1)
      values2 = Enum.map(501..1000, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["src1" | values1], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["src2" | values2], store)

      assert :ok = TDigestCmd.handle("TDIGEST.MERGE", ["dst", "2", "src1", "src2"], store)

      [p50_str] = TDigestCmd.handle("TDIGEST.QUANTILE", ["dst", "0.5"], store)
      p50 = parse_float_str(p50_str)
      assert_in_delta p50, 500.0, 500.0 * 0.1
    end

    test "merges three digests" do
      store = MockStore.make()

      for k <- ["s1", "s2", "s3"] do
        :ok = TDigestCmd.handle("TDIGEST.CREATE", [k], store)
      end

      :ok =
        TDigestCmd.handle("TDIGEST.ADD", ["s1" | Enum.map(1..333, &Integer.to_string/1)], store)

      :ok =
        TDigestCmd.handle("TDIGEST.ADD", ["s2" | Enum.map(334..666, &Integer.to_string/1)], store)

      :ok =
        TDigestCmd.handle(
          "TDIGEST.ADD",
          ["s3" | Enum.map(667..1000, &Integer.to_string/1)],
          store
        )

      assert :ok = TDigestCmd.handle("TDIGEST.MERGE", ["dst", "3", "s1", "s2", "s3"], store)

      info = TDigestCmd.handle("TDIGEST.INFO", ["dst"], store)
      merged_weight = parse_float_str(find_info_field(info, "Merged weight"))
      assert_in_delta merged_weight, 1000.0, 1.0
    end

    test "merges with different compressions" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["src1", "COMPRESSION", "50"], store)
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["src2", "COMPRESSION", "200"], store)

      :ok =
        TDigestCmd.handle("TDIGEST.ADD", ["src1" | Enum.map(1..100, &Integer.to_string/1)], store)

      :ok =
        TDigestCmd.handle(
          "TDIGEST.ADD",
          ["src2" | Enum.map(101..200, &Integer.to_string/1)],
          store
        )

      # Should use max compression (200) when COMPRESSION not specified
      assert :ok = TDigestCmd.handle("TDIGEST.MERGE", ["dst", "2", "src1", "src2"], store)

      info = TDigestCmd.handle("TDIGEST.INFO", ["dst"], store)
      assert find_info_field(info, "Compression") == 200
    end

    test "merges with COMPRESSION override" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["src1", "COMPRESSION", "100"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["src1", "1.0", "2.0", "3.0"], store)

      assert :ok =
               TDigestCmd.handle(
                 "TDIGEST.MERGE",
                 ["dst", "1", "src1", "COMPRESSION", "300"],
                 store
               )

      info = TDigestCmd.handle("TDIGEST.INFO", ["dst"], store)
      assert find_info_field(info, "Compression") == 300
    end

    test "merges with OVERRIDE flag" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["dst"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["dst", "1000.0"], store)

      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["src1"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["src1", "1.0", "2.0", "3.0"], store)

      assert :ok = TDigestCmd.handle("TDIGEST.MERGE", ["dst", "1", "src1", "OVERRIDE"], store)

      # After OVERRIDE, dst should only contain src1's data (not the old 1000.0)
      max_val = parse_float_str(TDigestCmd.handle("TDIGEST.MAX", ["dst"], store))
      # Should not contain 1000.0
      assert max_val <= 3.1
    end

    test "merge options are case-insensitive" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["dst"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["dst", "1000.0"], store)

      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["src1"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["src1", "1.0", "2.0", "3.0"], store)

      assert :ok =
               TDigestCmd.handle(
                 "TDIGEST.MERGE",
                 ["dst", "1", "src1", "compression", "300", "override"],
                 store
               )

      info = TDigestCmd.handle("TDIGEST.INFO", ["dst"], store)
      assert find_info_field(info, "Compression") == 300
      assert parse_float_str(TDigestCmd.handle("TDIGEST.MAX", ["dst"], store)) <= 3.1
    end

    test "merged quantiles close to combined reference" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["src1"], store)
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["src2"], store)
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["ref"], store)

      values1 = Enum.map(1..500, &Integer.to_string/1)
      values2 = Enum.map(501..1000, &Integer.to_string/1)
      all_values = Enum.map(1..1000, &Integer.to_string/1)

      :ok = TDigestCmd.handle("TDIGEST.ADD", ["src1" | values1], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["src2" | values2], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["ref" | all_values], store)

      :ok = TDigestCmd.handle("TDIGEST.MERGE", ["merged", "2", "src1", "src2"], store)

      for q_str <- ["0.5", "0.95", "0.99"] do
        [merged_result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["merged", q_str], store)
        [ref_result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["ref", q_str], store)

        merged_val = parse_float_str(merged_result)
        ref_val = parse_float_str(ref_result)

        # Merged result should be within 10% of the reference
        assert_in_delta merged_val,
                        ref_val,
                        ref_val * 0.10,
                        "q=#{q_str}: merged=#{merged_val} vs ref=#{ref_val}"
      end
    end

    test "merge into non-existent dest creates it" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["src1"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["src1", "1.0", "2.0", "3.0"], store)

      assert :ok = TDigestCmd.handle("TDIGEST.MERGE", ["new_dst", "1", "src1"], store)
      assert store.exists?.("new_dst")
    end

    test "merge preserves min/max" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["src1"], store)
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["src2"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["src1", "1.0", "50.0"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["src2", "25.0", "100.0"], store)

      :ok = TDigestCmd.handle("TDIGEST.MERGE", ["dst", "2", "src1", "src2"], store)

      min_val = parse_float_str(TDigestCmd.handle("TDIGEST.MIN", ["dst"], store))
      max_val = parse_float_str(TDigestCmd.handle("TDIGEST.MAX", ["dst"], store))

      assert_in_delta min_val, 1.0, 0.001
      assert_in_delta max_val, 100.0, 0.001
    end

    test "merge count = sum of source counts" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["src1"], store)
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["src2"], store)

      :ok =
        TDigestCmd.handle("TDIGEST.ADD", ["src1" | Enum.map(1..30, &Integer.to_string/1)], store)

      :ok =
        TDigestCmd.handle("TDIGEST.ADD", ["src2" | Enum.map(1..70, &Integer.to_string/1)], store)

      :ok = TDigestCmd.handle("TDIGEST.MERGE", ["dst", "2", "src1", "src2"], store)

      info = TDigestCmd.handle("TDIGEST.INFO", ["dst"], store)
      merged_weight = parse_float_str(find_info_field(info, "Merged weight"))
      unmerged_weight = parse_float_str(find_info_field(info, "Unmerged weight"))
      total = merged_weight + unmerged_weight
      assert_in_delta total, 100.0, 0.01
    end

    test "returns error for non-existent source key" do
      store = MockStore.make()
      assert {:error, msg} = TDigestCmd.handle("TDIGEST.MERGE", ["dst", "1", "missing"], store)
      assert msg =~ "does not exist"
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = TDigestCmd.handle("TDIGEST.MERGE", [], store)
      assert {:error, _} = TDigestCmd.handle("TDIGEST.MERGE", ["dst"], store)
    end

    test "returns error with fewer source keys than numkeys" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["src1"], store)
      assert {:error, _} = TDigestCmd.handle("TDIGEST.MERGE", ["dst", "3", "src1"], store)
    end

    test "returns write error when merge persistence fails" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["src1"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["src1", "1.0"], store)

      failing_store =
        Map.put(store, :put, fn "dst", _raw, 0 -> {:error, :disk_full} end)

      assert {:error, :disk_full} =
               TDigestCmd.handle("TDIGEST.MERGE", ["dst", "1", "src1"], failing_store)

      refute store.exists?.("dst")
    end
  end
  describe "TDIGEST.TRIMMED_MEAN" do
    test "trimmed mean 0.0 1.0 equals the overall mean" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      values = Enum.map(1..100, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      result = TDigestCmd.handle("TDIGEST.TRIMMED_MEAN", ["mydigest", "0.0", "1.0"], store)
      tm = parse_float_str(result)

      # True mean of 1..100 = 50.5
      assert_in_delta tm, 50.5, 50.5 * 0.05, "trimmed mean 0-1 should be ~50.5, got #{tm}"
    end

    test "trimmed mean 0.25 0.75 is the IQR mean" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      values = Enum.map(1..1000, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      result = TDigestCmd.handle("TDIGEST.TRIMMED_MEAN", ["mydigest", "0.25", "0.75"], store)
      tm = parse_float_str(result)

      # For uniform [1..1000], IQR mean should be ~500
      assert_in_delta tm, 500.0, 500.0 * 0.10
    end

    test "trimmed mean 0.0 0.5 is the lower half mean" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      values = Enum.map(1..1000, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      result = TDigestCmd.handle("TDIGEST.TRIMMED_MEAN", ["mydigest", "0.0", "0.5"], store)
      tm = parse_float_str(result)

      # Mean of 1..500 = 250.5
      assert_in_delta tm, 250.5, 250.5 * 0.10
    end

    test "returns nan for empty digest" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      result = TDigestCmd.handle("TDIGEST.TRIMMED_MEAN", ["mydigest", "0.0", "1.0"], store)
      assert result == "nan"
    end

    test "returns error with invalid range (low >= high)" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1.0"], store)

      assert {:error, msg} =
               TDigestCmd.handle("TDIGEST.TRIMMED_MEAN", ["mydigest", "0.5", "0.5"], store)

      assert msg =~ "less than"
    end

    test "returns error for non-existent key" do
      store = MockStore.make()

      assert {:error, msg} =
               TDigestCmd.handle("TDIGEST.TRIMMED_MEAN", ["missing", "0.0", "1.0"], store)

      assert msg =~ "does not exist"
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = TDigestCmd.handle("TDIGEST.TRIMMED_MEAN", [], store)
      assert {:error, _} = TDigestCmd.handle("TDIGEST.TRIMMED_MEAN", ["key"], store)
      assert {:error, _} = TDigestCmd.handle("TDIGEST.TRIMMED_MEAN", ["key", "0.0"], store)
    end
  end
  describe "TDIGEST.RANK" do
    test "rank of value below min is -1" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "10.0", "20.0", "30.0"], store)

      assert [-1] = TDigestCmd.handle("TDIGEST.RANK", ["mydigest", "5.0"], store)
    end

    test "rank of max is approximately count" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      values = Enum.map(1..100, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      [rank_max] = TDigestCmd.handle("TDIGEST.RANK", ["mydigest", "100.0"], store)
      assert rank_max == 100
    end

    test "rank of median is approximately count/2" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      values = Enum.map(1..1000, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      [rank_med] = TDigestCmd.handle("TDIGEST.RANK", ["mydigest", "500.0"], store)
      assert_in_delta rank_med, 500, 50
    end

    test "multiple ranks in one call" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      values = Enum.map(1..100, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      result = TDigestCmd.handle("TDIGEST.RANK", ["mydigest", "25.0", "50.0", "75.0"], store)
      assert length(result) == 3
      [r25, r50, r75] = result
      assert r25 < r50
      assert r50 < r75
    end

    test "rank on empty digest" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      assert [-2] = TDigestCmd.handle("TDIGEST.RANK", ["mydigest", "50.0"], store)
    end

    test "returns error for non-existent key" do
      store = MockStore.make()
      assert {:error, msg} = TDigestCmd.handle("TDIGEST.RANK", ["missing", "50.0"], store)
      assert msg =~ "does not exist"
    end
  end
  describe "TDIGEST.REVRANK" do
    test "REVRANK + RANK approximately equals count for a value in range" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      values = Enum.map(1..1000, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      [rank_val] = TDigestCmd.handle("TDIGEST.RANK", ["mydigest", "500.0"], store)
      [revrank_val] = TDigestCmd.handle("TDIGEST.REVRANK", ["mydigest", "500.0"], store)

      # rank + revrank + 1 should approximately equal count
      sum = rank_val + revrank_val + 1
      assert_in_delta sum, 1000, 50
    end

    test "revrank of min is approximately count" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "10.0", "20.0", "30.0"], store)

      [rr] = TDigestCmd.handle("TDIGEST.REVRANK", ["mydigest", "5.0"], store)
      # count
      assert rr == 3
    end

    test "returns error for non-existent key" do
      store = MockStore.make()
      assert {:error, msg} = TDigestCmd.handle("TDIGEST.REVRANK", ["missing", "50.0"], store)
      assert msg =~ "does not exist"
    end
  end
  describe "TDIGEST.BYRANK" do
    test "BYRANK 0 returns approximately min" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "10.0", "20.0", "30.0"], store)

      [result] = TDigestCmd.handle("TDIGEST.BYRANK", ["mydigest", "0"], store)
      val = parse_float_str(result)
      assert val >= 10.0 - 0.1 and val <= 15.0
    end

    test "BYRANK count-1 returns approximately max" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "10.0", "20.0", "30.0"], store)

      [result] = TDigestCmd.handle("TDIGEST.BYRANK", ["mydigest", "2"], store)
      val = parse_float_str(result)
      assert val >= 25.0 and val <= 30.1
    end

    test "BYRANK returns -inf for negative rank" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "10.0"], store)

      assert ["-inf"] = TDigestCmd.handle("TDIGEST.BYRANK", ["mydigest", "-1"], store)
    end

    test "BYRANK returns inf for rank >= count" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "10.0", "20.0"], store)

      assert ["inf"] = TDigestCmd.handle("TDIGEST.BYRANK", ["mydigest", "2"], store)
    end

    test "BYRANK returns nan for empty digest" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      assert ["nan"] = TDigestCmd.handle("TDIGEST.BYRANK", ["mydigest", "0"], store)
    end

    test "multiple ranks in one call" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      values = Enum.map(1..100, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      results = TDigestCmd.handle("TDIGEST.BYRANK", ["mydigest", "0", "49", "99"], store)
      assert length(results) == 3
    end

    test "returns error for non-existent key" do
      store = MockStore.make()
      assert {:error, msg} = TDigestCmd.handle("TDIGEST.BYRANK", ["missing", "0"], store)
      assert msg =~ "does not exist"
    end
  end
  describe "TDIGEST.BYREVRANK" do
    test "BYREVRANK 0 returns approximately max" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "10.0", "20.0", "30.0"], store)

      [result] = TDigestCmd.handle("TDIGEST.BYREVRANK", ["mydigest", "0"], store)
      val = parse_float_str(result)
      assert val >= 25.0 and val <= 30.1
    end

    test "BYREVRANK returns -inf for rank >= count" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "10.0"], store)

      assert ["-inf"] = TDigestCmd.handle("TDIGEST.BYREVRANK", ["mydigest", "1"], store)
    end

    test "BYREVRANK returns nan for empty digest" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      assert ["nan"] = TDigestCmd.handle("TDIGEST.BYREVRANK", ["mydigest", "0"], store)
    end

    test "returns error for non-existent key" do
      store = MockStore.make()
      assert {:error, msg} = TDigestCmd.handle("TDIGEST.BYREVRANK", ["missing", "0"], store)
      assert msg =~ "does not exist"
    end
  end
  describe "edge cases" do
    test "very large values (1e100)" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1.0e100", "1.5e100"], store)

      [result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5"], store)
      val = parse_float_str(result)
      assert val > 0
    end

    test "very small values (1e-100)" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1.0e-100", "2.0e-100"], store)

      min_val = parse_float_str(TDigestCmd.handle("TDIGEST.MIN", ["mydigest"], store))
      assert min_val >= 0
      max_val = parse_float_str(TDigestCmd.handle("TDIGEST.MAX", ["mydigest"], store))
      assert max_val >= min_val
    end

    test "mix of positive and negative" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      :ok =
        TDigestCmd.handle(
          "TDIGEST.ADD",
          ["mydigest", "-100.0", "-50.0", "0.0", "50.0", "100.0"],
          store
        )

      min_val = parse_float_str(TDigestCmd.handle("TDIGEST.MIN", ["mydigest"], store))
      max_val = parse_float_str(TDigestCmd.handle("TDIGEST.MAX", ["mydigest"], store))

      assert_in_delta min_val, -100.0, 0.001
      assert_in_delta max_val, 100.0, 0.001
    end

    test "NaN string rejected" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      assert {:error, _} = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "NaN"], store)
    end

    test "Infinity string rejected" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      assert {:error, _} = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "Infinity"], store)
    end

    test "single sample digest returns that value for all queries" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "99.0"], store)

      [p0] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.0"], store)
      [p50] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5"], store)
      [p100] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "1.0"], store)

      assert_in_delta parse_float_str(p0), 99.0, 0.1
      assert_in_delta parse_float_str(p50), 99.0, 0.1
      assert_in_delta parse_float_str(p100), 99.0, 0.1
    end

    test "two sample digest" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "0.0", "100.0"], store)

      min_val = parse_float_str(TDigestCmd.handle("TDIGEST.MIN", ["mydigest"], store))
      max_val = parse_float_str(TDigestCmd.handle("TDIGEST.MAX", ["mydigest"], store))

      assert_in_delta min_val, 0.0, 0.001
      assert_in_delta max_val, 100.0, 0.001
    end

    test "integer and float mixed" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1", "2.5", "3", "4.5", "5"], store)

      [result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5"], store)
      val = parse_float_str(result)
      assert val >= 1.0 and val <= 5.0
    end

    test "very high compression with few samples" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest", "COMPRESSION", "1000"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1.0", "2.0", "3.0"], store)

      [result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5"], store)
      val = parse_float_str(result)
      assert val >= 1.0 and val <= 3.0
    end

    test "very low compression (10) produces reasonable results" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest", "COMPRESSION", "10"], store)

      values = Enum.map(1..1000, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      [result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5"], store)
      p50 = parse_float_str(result)
      # Low compression = less accurate, but should still be in the right ballpark
      assert_in_delta p50, 500.0, 500.0 * 0.15
    end

    test "adding values in sorted order vs random order produces equivalent results" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["sorted"], store)
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["random"], store)

      sorted_values = Enum.map(1..500, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["sorted" | sorted_values], store)

      :rand.seed(:exsss, {123, 456, 789})
      random_values = Enum.shuffle(1..500) |> Enum.map(&Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["random" | random_values], store)

      for q_str <- ["0.5", "0.95", "0.99"] do
        [sorted_result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["sorted", q_str], store)
        [random_result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["random", q_str], store)

        sorted_val = parse_float_str(sorted_result)
        random_val = parse_float_str(random_result)

        # Both should be within 10% of the true value
        assert_in_delta sorted_val,
                        random_val,
                        500.0 * 0.15,
                        "q=#{q_str}: sorted=#{sorted_val} vs random=#{random_val}"
      end
    end

    test "empty digest handles all query commands gracefully" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      assert ["nan"] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5"], store)
      assert ["nan"] = TDigestCmd.handle("TDIGEST.CDF", ["mydigest", "50.0"], store)
      assert [-2] = TDigestCmd.handle("TDIGEST.RANK", ["mydigest", "50.0"], store)
      assert [-2] = TDigestCmd.handle("TDIGEST.REVRANK", ["mydigest", "50.0"], store)
      assert ["nan"] = TDigestCmd.handle("TDIGEST.BYRANK", ["mydigest", "0"], store)
      assert ["nan"] = TDigestCmd.handle("TDIGEST.BYREVRANK", ["mydigest", "0"], store)
      assert "nan" = TDigestCmd.handle("TDIGEST.TRIMMED_MEAN", ["mydigest", "0.0", "1.0"], store)
      assert "nan" = TDigestCmd.handle("TDIGEST.MIN", ["mydigest"], store)
      assert "nan" = TDigestCmd.handle("TDIGEST.MAX", ["mydigest"], store)
    end

    test "two independent digests do not interfere" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["d1"], store)
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["d2"], store)

      :ok = TDigestCmd.handle("TDIGEST.ADD", ["d1", "10.0"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["d2", "90.0"], store)

      min1 = parse_float_str(TDigestCmd.handle("TDIGEST.MIN", ["d1"], store))
      min2 = parse_float_str(TDigestCmd.handle("TDIGEST.MIN", ["d2"], store))

      assert_in_delta min1, 10.0, 0.001
      assert_in_delta min2, 90.0, 0.001
    end

    test "CMS command on TDIGEST key returns error (different storage)" do
      alias Ferricstore.Commands.CMS
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      assert {:error, _msg} = CMS.handle("CMS.QUERY", ["mydigest", "elem"], store)
    end

    test "TDIGEST command on CMS key returns WRONGTYPE" do
      alias Ferricstore.Commands.CMS
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)

      assert {:error, msg} = TDigestCmd.handle("TDIGEST.ADD", ["mysketch", "1.0"], store)
      assert msg =~ "WRONGTYPE"
    end
  end
    end
  end
end
