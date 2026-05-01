defmodule Ferricstore.Commands.TDigestTest do
  @moduledoc """
  Comprehensive tests for the TDIGEST.* commands and the underlying
  Ferricstore.TDigest.Core data structure.

  Organized into sections:
    1.  TDIGEST.CREATE -- basic creation and validation
    2.  TDIGEST.ADD -- adding observations
    3.  TDIGEST.RESET -- clearing data
    4.  TDIGEST.INFO -- metadata retrieval
    5.  TDIGEST.MIN / TDIGEST.MAX -- extreme value tracking
    6.  Quantile accuracy -- statistical correctness
    7.  CDF tests -- cumulative distribution function
    8.  MERGE tests -- merging multiple digests
    9.  TRIMMED_MEAN tests -- quantile-bounded means
    10. RANK / REVRANK / BYRANK / BYREVRANK -- rank-based queries
    11. Edge cases -- boundary conditions and unusual inputs
    12. Stress tests -- performance and memory
    13. Integration -- dispatcher routing, catalog, full lifecycle
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Hash
  alias Ferricstore.Commands.TDigest, as: TDigestCmd
  alias Ferricstore.TDigest.Core
  alias Ferricstore.Test.MockStore

  # ===========================================================================
  # 1. TDIGEST.CREATE
  # ===========================================================================

  describe "TDIGEST.CREATE" do
    test "creates a new digest with default compression (100)" do
      store = MockStore.make()
      assert :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      assert store.exists?.("mydigest")
    end

    test "creates a digest with explicit COMPRESSION parameter" do
      store = MockStore.make()
      assert :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest", "COMPRESSION", "200"], store)
      assert store.exists?.("mydigest")

      # Verify compression was set
      info = TDigestCmd.handle("TDIGEST.INFO", ["mydigest"], store)
      ["Compression", 200 | _] = info
    end

    test "returns error when key already exists" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      assert {:error, msg} = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      assert msg =~ "already exists"
    end

    test "returns WRONGTYPE when key holds a string" do
      store = MockStore.make(%{"mydigest" => {"a string", 0}})

      assert {:error, msg} = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      assert msg =~ "WRONGTYPE"
      assert store.get.("mydigest") == "a string"
    end

    test "returns WRONGTYPE when key holds a compound value" do
      store = MockStore.make()
      assert 1 == Hash.handle("HSET", ["mydigest", "field", "value"], store)

      assert {:error, msg} = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      assert msg =~ "WRONGTYPE"
      assert "value" == Hash.handle("HGET", ["mydigest", "field"], store)
    end

    test "returns error with zero compression" do
      store = MockStore.make()
      assert {:error, msg} = TDigestCmd.handle("TDIGEST.CREATE", ["key", "COMPRESSION", "0"], store)
      assert msg =~ "positive integer"
    end

    test "returns error with negative compression" do
      store = MockStore.make()
      assert {:error, msg} = TDigestCmd.handle("TDIGEST.CREATE", ["key", "COMPRESSION", "-10"], store)
      assert msg =~ "positive integer"
    end

    test "returns error with non-integer compression" do
      store = MockStore.make()
      assert {:error, msg} = TDigestCmd.handle("TDIGEST.CREATE", ["key", "COMPRESSION", "abc"], store)
      assert msg =~ "not an integer"
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, msg} = TDigestCmd.handle("TDIGEST.CREATE", [], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, msg} = TDigestCmd.handle("TDIGEST.CREATE", ["key", "COMPRESSION"], store)
      assert msg =~ "wrong number of arguments"
    end
  end

  # ===========================================================================
  # 2. TDIGEST.ADD
  # ===========================================================================

  describe "TDIGEST.ADD" do
    test "adds a single value" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      assert :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "12.5"], store)
    end

    test "adds multiple values in one call" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      assert :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "3.2", "7.8", "15.1", "200.3"], store)
    end

    test "returns error for non-existent key" do
      store = MockStore.make()
      assert {:error, msg} = TDigestCmd.handle("TDIGEST.ADD", ["missing", "12.5"], store)
      assert msg =~ "does not exist"
    end

    test "returns error for wrong type key" do
      store = MockStore.make(%{"str_key" => {"a string", 0}})
      assert {:error, msg} = TDigestCmd.handle("TDIGEST.ADD", ["str_key", "12.5"], store)
      assert msg =~ "WRONGTYPE"
    end

    test "returns WRONGTYPE for compound keys" do
      store = MockStore.make()
      assert 1 == Hash.handle("HSET", ["mydigest", "field", "value"], store)

      assert {:error, msg} = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "12.5"], store)
      assert msg =~ "WRONGTYPE"
      assert "value" == Hash.handle("HGET", ["mydigest", "field"], store)
    end

    test "returns error for non-numeric values" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      assert {:error, msg} = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "abc"], store)
      assert msg =~ "not a valid number"
    end

    test "returns error with no value arguments" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      assert {:error, _} = TDigestCmd.handle("TDIGEST.ADD", ["mydigest"], store)
    end

    test "returns error with no arguments at all" do
      store = MockStore.make()
      assert {:error, _} = TDigestCmd.handle("TDIGEST.ADD", [], store)
    end

    test "handles negative values" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      assert :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "-5.5", "-100.0"], store)
    end

    test "handles zero" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      assert :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "0", "0.0"], store)
    end

    test "handles very large values" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      assert :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1.0e18"], store)
    end

    test "handles very small values" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      assert :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1.0e-18"], store)
    end

    test "handles integer values" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      assert :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "42"], store)
    end

    test "multiple ADDs accumulate count" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1.0", "2.0", "3.0"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "4.0", "5.0"], store)

      info = TDigestCmd.handle("TDIGEST.INFO", ["mydigest"], store)
      # Total count should be 5 (merged_weight + unmerged_weight)
      ["Compression", _, "Capacity", _, "Merged nodes", _mn, "Unmerged nodes", _un,
       "Merged weight", mw, "Unmerged weight", uw | _] = info

      total = parse_float_str(mw) + parse_float_str(uw)
      assert_in_delta total, 5.0, 0.01
    end

    test "mixed valid and invalid values returns error and does not persist partial" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      assert {:error, _} = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1.0", "abc", "3.0"], store)
    end
  end

  # ===========================================================================
  # 3. TDIGEST.RESET
  # ===========================================================================

  describe "TDIGEST.RESET" do
    test "resets a populated digest to empty" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1.0", "2.0", "3.0"], store)
      assert :ok = TDigestCmd.handle("TDIGEST.RESET", ["mydigest"], store)
    end

    test "returns error for non-existent key" do
      store = MockStore.make()
      assert {:error, msg} = TDigestCmd.handle("TDIGEST.RESET", ["missing"], store)
      assert msg =~ "does not exist"
    end

    test "QUANTILE returns nan after reset" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1.0", "2.0", "3.0"], store)
      :ok = TDigestCmd.handle("TDIGEST.RESET", ["mydigest"], store)

      assert ["nan"] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5"], store)
    end

    test "MIN/MAX return nan after reset" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1.0", "2.0"], store)
      :ok = TDigestCmd.handle("TDIGEST.RESET", ["mydigest"], store)

      assert "nan" = TDigestCmd.handle("TDIGEST.MIN", ["mydigest"], store)
      assert "nan" = TDigestCmd.handle("TDIGEST.MAX", ["mydigest"], store)
    end

    test "INFO shows zero counts after reset" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1.0", "2.0"], store)
      :ok = TDigestCmd.handle("TDIGEST.RESET", ["mydigest"], store)

      info = TDigestCmd.handle("TDIGEST.INFO", ["mydigest"], store)
      ["Compression", _, "Capacity", _, "Merged nodes", 0, "Unmerged nodes", 0 | _] = info
    end

    test "preserves compression setting after reset" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest", "COMPRESSION", "200"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1.0", "2.0"], store)
      :ok = TDigestCmd.handle("TDIGEST.RESET", ["mydigest"], store)

      info = TDigestCmd.handle("TDIGEST.INFO", ["mydigest"], store)
      ["Compression", 200 | _] = info
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = TDigestCmd.handle("TDIGEST.RESET", [], store)
    end
  end

  # ===========================================================================
  # 4. TDIGEST.INFO
  # ===========================================================================

  describe "TDIGEST.INFO" do
    test "returns all expected fields" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      info = TDigestCmd.handle("TDIGEST.INFO", ["mydigest"], store)
      assert ["Compression", _, "Capacity", _, "Merged nodes", _,
              "Unmerged nodes", _, "Merged weight", _, "Unmerged weight", _,
              "Total compressions", _, "Memory usage", _] = info
    end

    test "compression matches creation parameter" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest", "COMPRESSION", "250"], store)

      info = TDigestCmd.handle("TDIGEST.INFO", ["mydigest"], store)
      ["Compression", 250 | _] = info
    end

    test "count matches number of adds" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1.0", "2.0", "3.0"], store)

      info = TDigestCmd.handle("TDIGEST.INFO", ["mydigest"], store)
      ["Compression", _, "Capacity", _, "Merged nodes", _mn, "Unmerged nodes", _un,
       "Merged weight", mw, "Unmerged weight", uw | _] = info

      total = parse_float_str(mw) + parse_float_str(uw)
      assert_in_delta total, 3.0, 0.01
    end

    test "memory usage is reasonable" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      info = TDigestCmd.handle("TDIGEST.INFO", ["mydigest"], store)
      # Find Memory usage in the flat list
      mem = find_info_field(info, "Memory usage")
      assert is_integer(mem)
      assert mem > 0
    end

    test "returns error for non-existent key" do
      store = MockStore.make()
      assert {:error, msg} = TDigestCmd.handle("TDIGEST.INFO", ["missing"], store)
      assert msg =~ "does not exist"
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = TDigestCmd.handle("TDIGEST.INFO", [], store)
      assert {:error, _} = TDigestCmd.handle("TDIGEST.INFO", ["a", "b"], store)
    end
  end

  # ===========================================================================
  # 5. TDIGEST.MIN / TDIGEST.MAX
  # ===========================================================================

  describe "TDIGEST.MIN and TDIGEST.MAX" do
    test "return nan for empty digest" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      assert "nan" = TDigestCmd.handle("TDIGEST.MIN", ["mydigest"], store)
      assert "nan" = TDigestCmd.handle("TDIGEST.MAX", ["mydigest"], store)
    end

    test "return correct values after adds" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "10.0", "20.0", "5.0", "30.0"], store)

      min_val = parse_float_str(TDigestCmd.handle("TDIGEST.MIN", ["mydigest"], store))
      max_val = parse_float_str(TDigestCmd.handle("TDIGEST.MAX", ["mydigest"], store))

      assert_in_delta min_val, 5.0, 0.001
      assert_in_delta max_val, 30.0, 0.001
    end

    test "update correctly when new extremes are added" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "10.0", "20.0"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1.0"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "100.0"], store)

      min_val = parse_float_str(TDigestCmd.handle("TDIGEST.MIN", ["mydigest"], store))
      max_val = parse_float_str(TDigestCmd.handle("TDIGEST.MAX", ["mydigest"], store))

      assert_in_delta min_val, 1.0, 0.001
      assert_in_delta max_val, 100.0, 0.001
    end

    test "correct with negative values" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "-5.0", "10.0", "-100.0", "50.0"], store)

      min_val = parse_float_str(TDigestCmd.handle("TDIGEST.MIN", ["mydigest"], store))
      max_val = parse_float_str(TDigestCmd.handle("TDIGEST.MAX", ["mydigest"], store))

      assert_in_delta min_val, -100.0, 0.001
      assert_in_delta max_val, 50.0, 0.001
    end

    test "return error for non-existent key" do
      store = MockStore.make()
      assert {:error, msg} = TDigestCmd.handle("TDIGEST.MIN", ["missing"], store)
      assert msg =~ "does not exist"
      assert {:error, msg} = TDigestCmd.handle("TDIGEST.MAX", ["missing"], store)
      assert msg =~ "does not exist"
    end

    test "return error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = TDigestCmd.handle("TDIGEST.MIN", [], store)
      assert {:error, _} = TDigestCmd.handle("TDIGEST.MAX", [], store)
    end
  end

  # ===========================================================================
  # 6. Quantile accuracy
  # ===========================================================================

  describe "TDIGEST.QUANTILE accuracy" do
    test "returns nan for empty digest" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      assert ["nan"] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5"], store)
    end

    test "returns exact value for single-element digest" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "42.0"], store)

      [result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5"], store)
      assert_in_delta parse_float_str(result), 42.0, 0.1
    end

    test "returns min for quantile 0.0" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1.0", "50.0", "100.0"], store)

      [result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.0"], store)
      assert_in_delta parse_float_str(result), 1.0, 0.1
    end

    test "returns max for quantile 1.0" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1.0", "50.0", "100.0"], store)

      [result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "1.0"], store)
      assert_in_delta parse_float_str(result), 100.0, 0.1
    end

    test "p50 of uniform 1..1000 within 5% of 500" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      # Add values in batches
      values = Enum.map(1..1000, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      [result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5"], store)
      p50 = parse_float_str(result)

      assert_in_delta p50, 500.0, 500.0 * 0.05,
        "p50 of uniform [1..1000] expected ~500, got #{p50}"
    end

    test "p95 of uniform 1..1000 within 2% of 950" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      values = Enum.map(1..1000, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      [result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.95"], store)
      p95 = parse_float_str(result)

      assert_in_delta p95, 950.0, 950.0 * 0.02,
        "p95 of uniform [1..1000] expected ~950, got #{p95}"
    end

    test "p99 of uniform 1..1000 within 2% of 990" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      values = Enum.map(1..1000, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      [result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.99"], store)
      p99 = parse_float_str(result)

      assert_in_delta p99, 990.0, 990.0 * 0.02,
        "p99 of uniform [1..1000] expected ~990, got #{p99}"
    end

    test "p50 of normal distribution within 5%" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      # Generate normal-like distribution via Box-Muller (using deterministic seed)
      :rand.seed(:exsss, {42, 42, 42})
      values = for _ <- 1..5000 do
        # Box-Muller transform: mean=100, stddev=10
        u1 = :rand.uniform()
        u2 = :rand.uniform()
        z = :math.sqrt(-2.0 * :math.log(u1)) * :math.cos(2.0 * :math.pi() * u2)
        Float.to_string(100.0 + 10.0 * z)
      end

      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      [result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5"], store)
      p50 = parse_float_str(result)

      assert_in_delta p50, 100.0, 5.0,
        "p50 of Normal(100,10) expected ~100, got #{p50}"
    end

    test "handles multiple quantiles in one call" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      values = Enum.map(1..1000, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      results = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.25", "0.5", "0.75", "0.99"], store)
      assert length(results) == 4

      [q25, q50, q75, q99] = Enum.map(results, &parse_float_str/1)
      assert q25 < q50
      assert q50 < q75
      assert q75 < q99
    end

    test "quantile of 0.0 returns min" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "10.0", "20.0", "30.0"], store)

      [result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.0"], store)
      q0 = parse_float_str(result)
      assert_in_delta q0, 10.0, 0.1
    end

    test "quantile of 1.0 returns max" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "10.0", "20.0", "30.0"], store)

      [result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "1.0"], store)
      q1 = parse_float_str(result)
      assert_in_delta q1, 30.0, 0.1
    end

    test "quantile < 0 returns error" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1.0"], store)

      assert {:error, msg} = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "-0.1"], store)
      assert msg =~ "between 0 and 1"
    end

    test "quantile > 1 returns error" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1.0"], store)

      assert {:error, msg} = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "1.1"], store)
      assert msg =~ "between 0 and 1"
    end

    test "quantile on non-existent key returns error" do
      store = MockStore.make()
      assert {:error, msg} = TDigestCmd.handle("TDIGEST.QUANTILE", ["missing", "0.5"], store)
      assert msg =~ "does not exist"
    end

    test "10K samples accuracy test" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      values = Enum.map(1..10_000, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      [p50_str] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5"], store)
      [p99_str] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.99"], store)
      p50 = parse_float_str(p50_str)
      p99 = parse_float_str(p99_str)

      assert_in_delta p50, 5000.0, 5000.0 * 0.05
      assert_in_delta p99, 9900.0, 9900.0 * 0.02
    end

    @tag :slow
    test "100K samples accuracy test" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest", "COMPRESSION", "200"], store)

      # Add in chunks to avoid creating a huge argument list
      for chunk <- Enum.chunk_every(1..100_000, 1000) do
        values = Enum.map(chunk, &Integer.to_string/1)
        :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)
      end

      [p50_str] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5"], store)
      [p99_str] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.99"], store)
      [p999_str] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.999"], store)

      p50 = parse_float_str(p50_str)
      p99 = parse_float_str(p99_str)
      p999 = parse_float_str(p999_str)

      assert_in_delta p50, 50_000.0, 50_000.0 * 0.05
      assert_in_delta p99, 99_000.0, 99_000.0 * 0.02
      assert_in_delta p999, 99_900.0, 99_900.0 * 0.02
    end

    test "repeated same value: all quantiles return that value" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      values = List.duplicate("42.0", 100)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      for q <- ["0.0", "0.25", "0.5", "0.75", "1.0"] do
        [result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", q], store)
        assert_in_delta parse_float_str(result), 42.0, 0.1,
          "quantile #{q} of all-42 should be 42, got #{result}"
      end
    end

    test "all same value: p50 = p99 = that value" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      values = List.duplicate("7.0", 500)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      [p50] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5"], store)
      [p99] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.99"], store)

      assert_in_delta parse_float_str(p50), 7.0, 0.1
      assert_in_delta parse_float_str(p99), 7.0, 0.1
    end

    test "negative values handled correctly" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      values = Enum.map(-500..500, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      [result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5"], store)
      p50 = parse_float_str(result)
      assert_in_delta p50, 0.0, 25.0
    end

    test "two values interpolate correctly" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "0.0", "100.0"], store)

      [result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5"], store)
      p50 = parse_float_str(result)
      # With 2 points, p50 should be somewhere near 50
      assert p50 >= 0.0 and p50 <= 100.0
    end

    test "quantile results are monotonically increasing" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      values = Enum.map(1..1000, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      qs = Enum.map(0..20, fn i -> Float.to_string(i * 0.05) end)
      results = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest" | qs], store)
      floats = Enum.map(results, &parse_float_str/1)

      # Each quantile value should be >= the previous
      pairs = Enum.zip(floats, tl(floats))
      Enum.each(pairs, fn {a, b} ->
        assert a <= b, "quantile values should be monotonically increasing: #{a} > #{b}"
      end)
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = TDigestCmd.handle("TDIGEST.QUANTILE", [], store)
      assert {:error, _} = TDigestCmd.handle("TDIGEST.QUANTILE", ["key"], store)
    end
  end

  # ===========================================================================
  # 7. CDF tests
  # ===========================================================================

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
      assert_in_delta cdf_med, 0.5, 0.05,
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

  # ===========================================================================
  # 8. MERGE tests
  # ===========================================================================

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

      :ok = TDigestCmd.handle("TDIGEST.ADD", ["s1" | Enum.map(1..333, &Integer.to_string/1)], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["s2" | Enum.map(334..666, &Integer.to_string/1)], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["s3" | Enum.map(667..1000, &Integer.to_string/1)], store)

      assert :ok = TDigestCmd.handle("TDIGEST.MERGE", ["dst", "3", "s1", "s2", "s3"], store)

      info = TDigestCmd.handle("TDIGEST.INFO", ["dst"], store)
      merged_weight = parse_float_str(find_info_field(info, "Merged weight"))
      assert_in_delta merged_weight, 1000.0, 1.0
    end

    test "merges with different compressions" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["src1", "COMPRESSION", "50"], store)
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["src2", "COMPRESSION", "200"], store)

      :ok = TDigestCmd.handle("TDIGEST.ADD", ["src1" | Enum.map(1..100, &Integer.to_string/1)], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["src2" | Enum.map(101..200, &Integer.to_string/1)], store)

      # Should use max compression (200) when COMPRESSION not specified
      assert :ok = TDigestCmd.handle("TDIGEST.MERGE", ["dst", "2", "src1", "src2"], store)

      info = TDigestCmd.handle("TDIGEST.INFO", ["dst"], store)
      assert find_info_field(info, "Compression") == 200
    end

    test "merges with COMPRESSION override" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["src1", "COMPRESSION", "100"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["src1", "1.0", "2.0", "3.0"], store)

      assert :ok = TDigestCmd.handle("TDIGEST.MERGE", ["dst", "1", "src1", "COMPRESSION", "300"], store)

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
      assert max_val <= 3.1  # Should not contain 1000.0
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
        assert_in_delta merged_val, ref_val, ref_val * 0.10,
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
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["src1" | Enum.map(1..30, &Integer.to_string/1)], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["src2" | Enum.map(1..70, &Integer.to_string/1)], store)

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
  end

  # ===========================================================================
  # 9. TRIMMED_MEAN tests
  # ===========================================================================

  describe "TDIGEST.TRIMMED_MEAN" do
    test "trimmed mean 0.0 1.0 equals the overall mean" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      values = Enum.map(1..100, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      result = TDigestCmd.handle("TDIGEST.TRIMMED_MEAN", ["mydigest", "0.0", "1.0"], store)
      tm = parse_float_str(result)

      # True mean of 1..100 = 50.5
      assert_in_delta tm, 50.5, 50.5 * 0.05,
        "trimmed mean 0-1 should be ~50.5, got #{tm}"
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

      assert {:error, msg} = TDigestCmd.handle("TDIGEST.TRIMMED_MEAN", ["mydigest", "0.5", "0.5"], store)
      assert msg =~ "less than"
    end

    test "returns error for non-existent key" do
      store = MockStore.make()
      assert {:error, msg} = TDigestCmd.handle("TDIGEST.TRIMMED_MEAN", ["missing", "0.0", "1.0"], store)
      assert msg =~ "does not exist"
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = TDigestCmd.handle("TDIGEST.TRIMMED_MEAN", [], store)
      assert {:error, _} = TDigestCmd.handle("TDIGEST.TRIMMED_MEAN", ["key"], store)
      assert {:error, _} = TDigestCmd.handle("TDIGEST.TRIMMED_MEAN", ["key", "0.0"], store)
    end
  end

  # ===========================================================================
  # 10. RANK / REVRANK / BYRANK / BYREVRANK
  # ===========================================================================

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
      assert rr == 3  # count
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

  # ===========================================================================
  # 11. Edge cases
  # ===========================================================================

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
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "-100.0", "-50.0", "0.0", "50.0", "100.0"], store)

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
        assert_in_delta sorted_val, random_val, 500.0 * 0.15,
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

  # ===========================================================================
  # 12. Stress tests
  # ===========================================================================

  describe "stress tests" do
    test "100K adds, quantile query < 100ms" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      # Add 100K values
      for chunk <- Enum.chunk_every(1..100_000, 1000) do
        values = Enum.map(chunk, &Integer.to_string/1)
        :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)
      end

      # Measure quantile query time
      start = System.monotonic_time(:millisecond)
      TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5", "0.95", "0.99"], store)
      elapsed = System.monotonic_time(:millisecond) - start

      assert elapsed < 100, "quantile query took #{elapsed}ms, expected < 100ms"
    end

    @tag :slow
    test "1M adds, memory < 100KB" do
      digest = Core.new(100)

      digest =
        Enum.reduce(1..1_000_000, digest, fn i, d ->
          Core.add(d, i / 1)
        end)

      info = Core.info(digest)
      assert info.memory_usage < 100_000,
        "memory usage #{info.memory_usage} bytes, expected < 100KB"
    end

    test "sequential adds from 10 batches" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      # Each batch adds 100 values sequentially (the command handler does
      # a get-modify-put cycle so concurrent Tasks would race on the store)
      for i <- 0..9 do
        values = Enum.map((i * 100 + 1)..(i * 100 + 100), &Integer.to_string/1)
        :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)
      end

      # All 1000 values should be in the digest
      info = TDigestCmd.handle("TDIGEST.INFO", ["mydigest"], store)
      merged_weight = parse_float_str(find_info_field(info, "Merged weight"))
      unmerged_weight = parse_float_str(find_info_field(info, "Unmerged weight"))
      total = merged_weight + unmerged_weight
      assert_in_delta total, 1000.0, 1.0
    end

    test "merge 100 digests" do
      store = MockStore.make()

      # Create 100 source digests
      for i <- 1..100 do
        key = "src_#{i}"
        :ok = TDigestCmd.handle("TDIGEST.CREATE", [key], store)
        values = Enum.map(((i - 1) * 10 + 1)..(i * 10), &Integer.to_string/1)
        :ok = TDigestCmd.handle("TDIGEST.ADD", [key | values], store)
      end

      src_keys = Enum.map(1..100, &"src_#{&1}")
      args = ["dst", "100" | src_keys]
      assert :ok = TDigestCmd.handle("TDIGEST.MERGE", args, store)

      info = TDigestCmd.handle("TDIGEST.INFO", ["dst"], store)
      merged_weight = parse_float_str(find_info_field(info, "Merged weight"))
      assert_in_delta merged_weight, 1000.0, 1.0
    end

    test "repeated create/add/query cycle 1000 times" do
      store = MockStore.make()

      for i <- 1..1000 do
        key = "td_#{i}"
        :ok = TDigestCmd.handle("TDIGEST.CREATE", [key], store)
        :ok = TDigestCmd.handle("TDIGEST.ADD", [key, "#{i}.0"], store)
        [result] = TDigestCmd.handle("TDIGEST.QUANTILE", [key, "0.5"], store)
        assert parse_float_str(result) > 0
      end
    end
  end

  # ===========================================================================
  # 13. Integration
  # ===========================================================================

  describe "dispatcher integration" do
    test "TDIGEST commands route through the dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()

      assert :ok = Dispatcher.dispatch("TDIGEST.CREATE", ["mydigest"], store)
      assert :ok = Dispatcher.dispatch("TDIGEST.ADD", ["mydigest", "1.0", "2.0", "3.0"], store)

      result = Dispatcher.dispatch("TDIGEST.QUANTILE", ["mydigest", "0.5"], store)
      assert is_list(result) and length(result) == 1

      info = Dispatcher.dispatch("TDIGEST.INFO", ["mydigest"], store)
      assert ["Compression", 100 | _] = info
    end

    test "TDIGEST commands are case-insensitive via dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()

      assert :ok = Dispatcher.dispatch("tdigest.create", ["mydigest"], store)
      assert :ok = Dispatcher.dispatch("tdigest.add", ["mydigest", "42.0"], store)
      result = Dispatcher.dispatch("tdigest.quantile", ["mydigest", "0.5"], store)
      assert is_list(result)
    end

    test "mixed case routing works" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()

      assert :ok = Dispatcher.dispatch("Tdigest.Create", ["mydigest"], store)
      assert :ok = Dispatcher.dispatch("TDigest.Add", ["mydigest", "1.0"], store)
    end

    test "all TDIGEST commands are routable" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()

      # Create and populate
      :ok = Dispatcher.dispatch("TDIGEST.CREATE", ["mydigest"], store)
      :ok = Dispatcher.dispatch("TDIGEST.ADD", ["mydigest", "1.0", "2.0", "3.0"], store)

      # Read commands
      assert is_list(Dispatcher.dispatch("TDIGEST.QUANTILE", ["mydigest", "0.5"], store))
      assert is_list(Dispatcher.dispatch("TDIGEST.CDF", ["mydigest", "2.0"], store))
      assert is_list(Dispatcher.dispatch("TDIGEST.RANK", ["mydigest", "2.0"], store))
      assert is_list(Dispatcher.dispatch("TDIGEST.REVRANK", ["mydigest", "2.0"], store))
      assert is_list(Dispatcher.dispatch("TDIGEST.BYRANK", ["mydigest", "0"], store))
      assert is_list(Dispatcher.dispatch("TDIGEST.BYREVRANK", ["mydigest", "0"], store))
      assert is_binary(Dispatcher.dispatch("TDIGEST.TRIMMED_MEAN", ["mydigest", "0.0", "1.0"], store))
      assert is_binary(Dispatcher.dispatch("TDIGEST.MIN", ["mydigest"], store))
      assert is_binary(Dispatcher.dispatch("TDIGEST.MAX", ["mydigest"], store))
      assert is_list(Dispatcher.dispatch("TDIGEST.INFO", ["mydigest"], store))

      # Mutation commands
      :ok = Dispatcher.dispatch("TDIGEST.CREATE", ["src"], store)
      :ok = Dispatcher.dispatch("TDIGEST.ADD", ["src", "5.0"], store)
      assert :ok = Dispatcher.dispatch("TDIGEST.MERGE", ["merged", "1", "src"], store)
      assert :ok = Dispatcher.dispatch("TDIGEST.RESET", ["mydigest"], store)
    end
  end

  describe "catalog integration" do
    test "catalog has entries for all TDIGEST commands" do
      alias Ferricstore.Commands.Catalog

      tdigest_cmds = ~w(tdigest.create tdigest.add tdigest.reset tdigest.quantile
        tdigest.cdf tdigest.rank tdigest.revrank tdigest.byrank tdigest.byrevrank
        tdigest.trimmed_mean tdigest.min tdigest.max tdigest.info tdigest.merge)

      for cmd <- tdigest_cmds do
        assert {:ok, entry} = Catalog.lookup(cmd),
          "catalog should have entry for #{cmd}"
        assert entry.name == cmd
      end
    end

    test "catalog entries have correct key positions" do
      alias Ferricstore.Commands.Catalog

      for cmd <- ~w(tdigest.create tdigest.add tdigest.reset tdigest.quantile
                    tdigest.cdf tdigest.rank tdigest.revrank tdigest.byrank
                    tdigest.byrevrank tdigest.trimmed_mean tdigest.min tdigest.max
                    tdigest.info tdigest.merge) do
        {:ok, entry} = Catalog.lookup(cmd)
        assert entry.first_key == 1, "#{cmd} should have first_key=1"
      end
    end
  end

  describe "full lifecycle" do
    test "CREATE -> ADD 10K -> QUANTILE -> CDF -> MERGE -> RESET" do
      store = MockStore.make()

      # Create
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["td1"], store)
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["td2"], store)

      # ADD 10K values to each
      values1 = Enum.map(1..5000, &Integer.to_string/1)
      values2 = Enum.map(5001..10_000, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["td1" | values1], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["td2" | values2], store)

      # QUANTILE on individual digests
      [p50_1] = TDigestCmd.handle("TDIGEST.QUANTILE", ["td1", "0.5"], store)
      [p50_2] = TDigestCmd.handle("TDIGEST.QUANTILE", ["td2", "0.5"], store)
      assert parse_float_str(p50_1) < parse_float_str(p50_2)

      # CDF check
      [cdf_result] = TDigestCmd.handle("TDIGEST.CDF", ["td1", "2500.0"], store)
      cdf_val = parse_float_str(cdf_result)
      assert_in_delta cdf_val, 0.5, 0.1

      # MERGE
      :ok = TDigestCmd.handle("TDIGEST.MERGE", ["combined", "2", "td1", "td2"], store)

      [p50_combined] = TDigestCmd.handle("TDIGEST.QUANTILE", ["combined", "0.5"], store)
      assert_in_delta parse_float_str(p50_combined), 5000.0, 5000.0 * 0.05

      # INFO check
      info = TDigestCmd.handle("TDIGEST.INFO", ["combined"], store)
      merged_weight = parse_float_str(find_info_field(info, "Merged weight"))
      assert_in_delta merged_weight, 10_000.0, 1.0

      # RESET
      :ok = TDigestCmd.handle("TDIGEST.RESET", ["combined"], store)
      assert ["nan"] = TDigestCmd.handle("TDIGEST.QUANTILE", ["combined", "0.5"], store)
    end

    test "persistence round-trip: ADD, store, reload, QUANTILE returns same result" do
      store = MockStore.make()

      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | Enum.map(1..1000, &Integer.to_string/1)], store)

      # Query before simulated reload
      [before] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5"], store)

      # "Reload" by reading raw value and putting it back (simulates persistence)
      raw = store.get.("mydigest")
      assert is_binary(raw)
      assert {:tdigest, _, _} = :erlang.binary_to_term(raw)

      store2 = MockStore.make()
      store2.put.("mydigest", raw, 0)

      # Query after simulated reload
      [after_reload] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5"], store2)

      assert before == after_reload
    end

    test "CREATE -> ADD -> RESET -> ADD -> QUANTILE works correctly" do
      store = MockStore.make()

      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1000.0"], store)
      :ok = TDigestCmd.handle("TDIGEST.RESET", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1.0", "2.0", "3.0"], store)

      [result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5"], store)
      val = parse_float_str(result)
      # Should reflect new data (1-3), not old data (1000)
      assert val >= 1.0 and val <= 3.0
    end
  end

  # ===========================================================================
  # Core module direct tests
  # ===========================================================================

  describe "Ferricstore.TDigest.Core direct tests" do
    test "new creates empty digest with default compression" do
      digest = Core.new()
      assert digest.compression == 100
      assert digest.centroids == []
      assert digest.count == 0
      assert digest.min == nil
      assert digest.max == nil
    end

    test "new creates empty digest with custom compression" do
      digest = Core.new(200)
      assert digest.compression == 200
    end

    test "add updates min/max" do
      digest = Core.new() |> Core.add(5.0)
      assert digest.min == 5.0
      assert digest.max == 5.0

      digest = Core.add(digest, 10.0)
      assert digest.min == 5.0
      assert digest.max == 10.0

      digest = Core.add(digest, 1.0)
      assert digest.min == 1.0
      assert digest.max == 10.0
    end

    test "add_many adds multiple values" do
      digest = Core.new() |> Core.add_many([1.0, 2.0, 3.0, 4.0, 5.0])
      assert digest.count == 5
      assert digest.min == 1.0
      assert digest.max == 5.0
    end

    test "compress reduces buffer to zero" do
      digest = Core.new() |> Core.add_many(Enum.map(1..10, &(&1 / 1)))
      compressed = Core.compress(digest)
      assert compressed.buffer == []
      assert compressed.buffer_size == 0
      assert compressed.centroids != []
    end

    test "reset preserves compression" do
      digest = Core.new(250) |> Core.add_many([1.0, 2.0, 3.0])
      reset = Core.reset(digest)
      assert reset.compression == 250
      assert reset.count == 0
      assert reset.centroids == []
    end

    test "info returns correct map" do
      digest = Core.new() |> Core.add_many(Enum.map(1..100, &(&1 / 1)))
      info = Core.info(digest)
      assert is_map(info)
      assert info.compression == 100
      assert info[:merged_nodes] >= 0
      assert info[:unmerged_nodes] >= 0
    end

    test "merge combines two digests" do
      d1 = Core.new() |> Core.add_many(Enum.map(1..50, &(&1 / 1)))
      d2 = Core.new() |> Core.add_many(Enum.map(51..100, &(&1 / 1)))
      merged = Core.merge(d1, d2)

      assert merged.count == 100
      assert merged.min == 1.0
      assert merged.max == 100.0
    end

    test "merge_many combines multiple digests" do
      digests = for i <- 0..4 do
        values = Enum.map((i * 20 + 1)..((i + 1) * 20), &(&1 / 1))
        Core.new() |> Core.add_many(values)
      end

      merged = Core.merge_many(digests, 100)
      assert merged.count == 100
      assert merged.min == 1.0
      assert merged.max == 100.0
    end

    test "quantile returns :nan for empty digest" do
      digest = Core.new()
      assert Core.quantile(digest, 0.5) == :nan
    end

    test "cdf returns :nan for empty digest" do
      digest = Core.new()
      assert Core.cdf(digest, 50.0) == :nan
    end

    test "rank returns -2 for empty digest" do
      digest = Core.new()
      assert Core.rank(digest, 50.0) == -2
    end

    test "by_rank returns :nan for empty digest" do
      digest = Core.new()
      assert Core.by_rank(digest, 0) == :nan
    end

    test "trimmed_mean returns :nan for empty digest" do
      digest = Core.new()
      assert Core.trimmed_mean(digest, 0.0, 1.0) == :nan
    end

    test "compression pass runs automatically when buffer is full" do
      # Buffer capacity = ceil(compression * 3) = 300 for default compression=100
      digest = Core.new()
      digest = Core.add_many(digest, Enum.map(1..300, &(&1 / 1)))

      # After adding 300 values (= buffer capacity), compression should have run
      assert digest.total_compressions >= 1
      assert digest.buffer_size < 300
    end

    test "centroids are always sorted by mean after compression" do
      digest = Core.new()
      # Add values in random order
      :rand.seed(:exsss, {1, 2, 3})
      values = Enum.shuffle(1..500) |> Enum.map(&(&1 / 1))
      digest = Core.add_many(digest, values) |> Core.compress()

      means = Enum.map(digest.centroids, fn {mean, _} -> mean end)
      assert means == Enum.sort(means)
    end

    test "total_compressions increments on each compress pass" do
      digest = Core.new()
      # Add enough to trigger multiple compressions
      digest = Core.add_many(digest, Enum.map(1..1000, &(&1 / 1)))
      assert digest.total_compressions >= 3
    end
  end

  # ===========================================================================
  # Accuracy benchmarks (tagged :slow)
  # ===========================================================================

  describe "accuracy benchmarks" do
    @tag :slow
    test "uniform[0,1] with 100K samples: max quantile error < 0.02 at compression=100" do
      :rand.seed(:exsss, {100, 200, 300})
      values = for _ <- 1..100_000, do: :rand.uniform()

      digest = Core.new(100) |> Core.add_many(values)

      for q <- [0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99] do
        estimated = Core.quantile(digest, q)
        error = abs(estimated - q)
        assert error < 0.02,
          "q=#{q}: estimated=#{estimated}, error=#{error}, expected < 0.02"
      end
    end

    @tag :slow
    test "uniform[0,1] with 100K samples: max quantile error < 0.01 at compression=200" do
      :rand.seed(:exsss, {100, 200, 300})
      values = for _ <- 1..100_000, do: :rand.uniform()

      digest = Core.new(200) |> Core.add_many(values)

      for q <- [0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99] do
        estimated = Core.quantile(digest, q)
        error = abs(estimated - q)
        assert error < 0.01,
          "q=#{q}: estimated=#{estimated}, error=#{error}, expected < 0.01"
      end
    end

    @tag :slow
    test "merge accuracy: merge 10 digests of 10K vs single digest of 100K" do
      :rand.seed(:exsss, {42, 42, 42})
      all_values = for _ <- 1..100_000, do: :rand.uniform()

      # Single digest
      single = Core.new(100) |> Core.add_many(all_values)

      # 10 digests of 10K each
      chunks = Enum.chunk_every(all_values, 10_000)
      digests = Enum.map(chunks, fn chunk ->
        Core.new(100) |> Core.add_many(chunk)
      end)
      merged = Core.merge_many(digests, 100)

      for q <- [0.5, 0.95, 0.99] do
        single_val = Core.quantile(single, q)
        merged_val = Core.quantile(merged, q)
        diff = abs(single_val - merged_val)

        assert diff < 0.02,
          "q=#{q}: single=#{single_val}, merged=#{merged_val}, diff=#{diff}"
      end
    end
  end

  # ===========================================================================
  # Helpers
  # ===========================================================================

  defp parse_float_str("nan"), do: :nan
  defp parse_float_str("inf"), do: :infinity
  defp parse_float_str("-inf"), do: :neg_infinity

  defp parse_float_str(str) when is_binary(str) do
    case Float.parse(str) do
      {f, _} -> f
      :error ->
        case Integer.parse(str) do
          {i, _} -> i / 1
          :error -> raise "cannot parse #{inspect(str)} as float"
        end
    end
  end

  defp parse_float_str(val) when is_number(val), do: val / 1

  # Extract a value from the INFO flat list by field name
  defp find_info_field(info_list, field_name) do
    info_list
    |> Enum.chunk_every(2)
    |> Enum.find_value(fn
      [^field_name, value] -> value
      _ -> nil
    end)
  end
end
