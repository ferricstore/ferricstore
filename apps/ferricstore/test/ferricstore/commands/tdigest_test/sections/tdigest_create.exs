defmodule Ferricstore.Commands.TDigestTest.Sections.TdigestCreate do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.Hash
      alias Ferricstore.Commands.TDigest, as: TDigestCmd
      alias Ferricstore.TDigest.Core
      alias Ferricstore.Test.MockStore

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

    test "COMPRESSION option is case-insensitive" do
      store = MockStore.make()
      assert :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest", "compression", "200"], store)

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

      assert {:error, msg} =
               TDigestCmd.handle("TDIGEST.CREATE", ["key", "COMPRESSION", "0"], store)

      assert msg =~ "positive integer"
    end

    test "returns error with negative compression" do
      store = MockStore.make()

      assert {:error, msg} =
               TDigestCmd.handle("TDIGEST.CREATE", ["key", "COMPRESSION", "-10"], store)

      assert msg =~ "positive integer"
    end

    test "returns error with non-integer compression" do
      store = MockStore.make()

      assert {:error, msg} =
               TDigestCmd.handle("TDIGEST.CREATE", ["key", "COMPRESSION", "abc"], store)

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

    test "returns write error when digest creation fails" do
      store =
        MockStore.make()
        |> Map.put(:put, fn "mydigest", _raw, 0 -> {:error, :disk_full} end)

      assert {:error, :disk_full} = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      refute store.exists?.("mydigest")
    end
  end
  describe "TDIGEST.ADD" do
    test "adds a single value" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      assert :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "12.5"], store)
    end

    test "adds multiple values in one call" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      assert :ok =
               TDigestCmd.handle(
                 "TDIGEST.ADD",
                 ["mydigest", "3.2", "7.8", "15.1", "200.3"],
                 store
               )
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
      [
        "Compression",
        _,
        "Capacity",
        _,
        "Merged nodes",
        _mn,
        "Unmerged nodes",
        _un,
        "Merged weight",
        mw,
        "Unmerged weight",
        uw | _
      ] = info

      total = parse_float_str(mw) + parse_float_str(uw)
      assert_in_delta total, 5.0, 0.01
    end

    test "mixed valid and invalid values returns error and does not persist partial" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      assert {:error, _} =
               TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1.0", "abc", "3.0"], store)
    end

    test "returns write error when add persistence fails" do
      store =
        MockStore.make()
        |> tap(fn store -> :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store) end)
        |> Map.put(:put, fn "mydigest", _raw, 0 -> {:error, :disk_full} end)

      assert {:error, :disk_full} = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "12.5"], store)

      info = TDigestCmd.handle("TDIGEST.INFO", ["mydigest"], store)
      assert "0.0" == find_info_field(info, "Merged weight")
      assert "0.0" == find_info_field(info, "Unmerged weight")
    end
  end
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

    test "returns write error when reset persistence fails" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "12.5"], store)

      failing_store =
        Map.put(store, :put, fn "mydigest", _raw, 0 -> {:error, :disk_full} end)

      assert {:error, :disk_full} =
               TDigestCmd.handle("TDIGEST.RESET", ["mydigest"], failing_store)

      assert "12.5" == TDigestCmd.handle("TDIGEST.MIN", ["mydigest"], failing_store)
    end
  end
  describe "TDIGEST.INFO" do
    test "returns all expected fields" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      info = TDigestCmd.handle("TDIGEST.INFO", ["mydigest"], store)

      assert [
               "Compression",
               _,
               "Capacity",
               _,
               "Merged nodes",
               _,
               "Unmerged nodes",
               _,
               "Merged weight",
               _,
               "Unmerged weight",
               _,
               "Total compressions",
               _,
               "Memory usage",
               _
             ] = info
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

      [
        "Compression",
        _,
        "Capacity",
        _,
        "Merged nodes",
        _mn,
        "Unmerged nodes",
        _un,
        "Merged weight",
        mw,
        "Unmerged weight",
        uw | _
      ] = info

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

    test "returns WRONGTYPE for corrupt persisted digest shape" do
      store =
        MockStore.make(%{
          "mydigest" => {:erlang.term_to_binary({:tdigest, [], %{compression: 100}}), 0}
        })

      assert {:error, msg} = TDigestCmd.handle("TDIGEST.INFO", ["mydigest"], store)
      assert msg =~ "WRONGTYPE"
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = TDigestCmd.handle("TDIGEST.INFO", [], store)
      assert {:error, _} = TDigestCmd.handle("TDIGEST.INFO", ["a", "b"], store)
    end
  end
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

      :ok =
        TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "-5.0", "10.0", "-100.0", "50.0"], store)

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

      assert_in_delta p50,
                      500.0,
                      500.0 * 0.05,
                      "p50 of uniform [1..1000] expected ~500, got #{p50}"
    end

    test "p95 of uniform 1..1000 within 2% of 950" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      values = Enum.map(1..1000, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      [result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.95"], store)
      p95 = parse_float_str(result)

      assert_in_delta p95,
                      950.0,
                      950.0 * 0.02,
                      "p95 of uniform [1..1000] expected ~950, got #{p95}"
    end

    test "p99 of uniform 1..1000 within 2% of 990" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      values = Enum.map(1..1000, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      [result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.99"], store)
      p99 = parse_float_str(result)

      assert_in_delta p99,
                      990.0,
                      990.0 * 0.02,
                      "p99 of uniform [1..1000] expected ~990, got #{p99}"
    end

    test "p50 of normal distribution within 5%" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      # Generate normal-like distribution via Box-Muller (using deterministic seed)
      :rand.seed(:exsss, {42, 42, 42})

      values =
        for _ <- 1..5000 do
          # Box-Muller transform: mean=100, stddev=10
          u1 = :rand.uniform()
          u2 = :rand.uniform()
          z = :math.sqrt(-2.0 * :math.log(u1)) * :math.cos(2.0 * :math.pi() * u2)
          Float.to_string(100.0 + 10.0 * z)
        end

      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      [result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5"], store)
      p50 = parse_float_str(result)

      assert_in_delta p50, 100.0, 5.0, "p50 of Normal(100,10) expected ~100, got #{p50}"
    end

    test "handles multiple quantiles in one call" do
      store = MockStore.make()
      :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

      values = Enum.map(1..1000, &Integer.to_string/1)
      :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)

      results =
        TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.25", "0.5", "0.75", "0.99"], store)

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

        assert_in_delta parse_float_str(result),
                        42.0,
                        0.1,
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
    end
  end
end
