defmodule Ferricstore.Commands.ProbabilisticAstValidationTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Bloom, CMS, Cuckoo, ProbParameters, TDigest, TopK}

  test "CMS merge work is bounded by source descriptors and counter visits" do
    assert :ok = ProbParameters.validate_cms_merge_work(128, 131_072, 1)

    assert {:error, :cms_merge_source_limit_exceeded} =
             ProbParameters.validate_cms_merge_work(129, 1, 1)

    assert {:error, :cms_merge_work_limit_exceeded} =
             ProbParameters.validate_cms_merge_work(128, 131_073, 1)
  end

  test "BF.RESERVE validates types and native sizing limits before storage access" do
    parent = self()

    store = %{
      get: fn key -> send(parent, {:storage_access, :get, key}) end,
      exists?: fn key -> send(parent, {:storage_access, :exists, key}) end,
      prob_write: fn command -> send(parent, {:storage_access, :prob_write, command}) end
    }

    invalid_asts = [
      {:bf_reserve, "key", "0.01", 100},
      {:bf_reserve, "key", 0.01, 0},
      {:bf_reserve, "key", 0.01, "100"},
      {:bf_reserve, "key", 0.01, 1_000_000_000}
    ]

    for ast <- invalid_asts do
      assert {:error, message} = Bloom.handle_ast(ast, store)
      assert is_binary(message)
    end

    assert {:error, message} =
             Bloom.handle(
               "BF.RESERVE",
               ["key", "0.01", String.duplicate("9", 1_000)],
               store
             )

    assert is_binary(message)
    refute_received {:storage_access, _, _}
  end

  test "CF.RESERVE prepared commands reject invalid capacities before storage access" do
    parent = self()

    store = %{
      get: fn key -> send(parent, {:storage_access, :get, key}) end,
      exists?: fn key -> send(parent, {:storage_access, :exists, key}) end,
      prob_write: fn command -> send(parent, {:storage_access, :prob_write, command}) end
    }

    for capacity <- [0, -1, "1024", 1_073_741_825] do
      assert {:error, message} = Cuckoo.handle_ast({:cf_reserve, "key", capacity}, store)
      assert is_binary(message)
    end

    refute_received {:storage_access, _, _}
  end

  test "CMS prepared create commands enforce native allocation limits before storage access" do
    parent = self()

    store = %{
      get: fn key -> send(parent, {:storage_access, :get, key}) end,
      exists?: fn key -> send(parent, {:storage_access, :exists, key}) end,
      prob_write: fn command -> send(parent, {:storage_access, :prob_write, command}) end
    }

    invalid_asts = [
      {:cms_initbydim, "key", 0, 1},
      {:cms_initbydim, "key", 1, 0},
      {:cms_initbydim, "key", "1", 1},
      {:cms_initbydim, "key", 1, 1_025},
      {:cms_initbydim, "key", 16_777_217, 1},
      {:cms_initbyprob, "key", 0.0, 0.5},
      {:cms_initbyprob, "key", 0.1, 1.0},
      {:cms_initbyprob, "key", 1.0e-300, 0.5}
    ]

    for ast <- invalid_asts do
      assert {:error, message} = CMS.handle_ast(ast, store)
      assert is_binary(message)
    end

    refute_received {:storage_access, _, _}
  end

  test "CMS prepared updates validate pair and merge shapes before storage access" do
    parent = self()

    store = %{
      get: fn key -> send(parent, {:storage_access, :get, key}) end,
      prob_write: fn command -> send(parent, {:storage_access, :prob_write, command}) end
    }

    invalid_asts = [
      {:cms_incrby, "key", []},
      {:cms_incrby, "key", [{"item", 0}]},
      {:cms_incrby, "key", [{"item", "1"}]},
      {:cms_incrby, "key", [{"item", 9_223_372_036_854_775_808}]},
      {:cms_incrby, "key", :invalid},
      {:cms_merge, "dst", [], []},
      {:cms_merge, "dst", ["src"], []},
      {:cms_merge, "dst", ["src"], ["1"]},
      {:cms_merge, "dst", ["src"], [9_223_372_036_854_775_808]}
    ]

    for ast <- invalid_asts do
      assert {:error, message} = CMS.handle_ast(ast, store)
      assert is_binary(message)
    end

    refute_received {:storage_access, _, _}
  end

  test "TOPK.RESERVE prepared commands enforce native allocation limits before storage access" do
    parent = self()

    store = %{
      get: fn key -> send(parent, {:storage_access, :get, key}) end,
      exists?: fn key -> send(parent, {:storage_access, :exists, key}) end,
      prob_write: fn command -> send(parent, {:storage_access, :prob_write, command}) end
    }

    invalid_asts = [
      {:topk_reserve, "key", 0, 8, 7},
      {:topk_reserve, "key", 100_001, 8, 7},
      {:topk_reserve, "key", 10, 0, 7},
      {:topk_reserve, "key", 10, 1_048_577, 1},
      {:topk_reserve, "key", 10, "8", 7},
      {:topk_reserve, "key", 10, 8, 7, 0.9}
    ]

    for ast <- invalid_asts do
      assert {:error, message} = TopK.handle_ast(ast, store)
      assert is_binary(message)
    end

    refute_received {:storage_access, _, _}
  end

  test "TopK prepared updates enforce element and count bounds before storage access" do
    parent = self()
    oversized_element = :binary.copy("x", 253)

    store = %{
      get: fn key -> send(parent, {:storage_access, :get, key}) end,
      prob_write: fn command -> send(parent, {:storage_access, :prob_write, command}) end
    }

    invalid_asts = [
      {:topk_add, ["key", oversized_element]},
      {:topk_incrby, "key", []},
      {:topk_incrby, "key", [{"item", 0}]},
      {:topk_incrby, "key", [{"item", "1"}]},
      {:topk_incrby, "key", [{oversized_element, 1}]},
      {:topk_incrby, "key", [{"item", 9_223_372_036_854_775_808}]},
      {:topk_incrby, "key", :invalid}
    ]

    for ast <- invalid_asts do
      assert {:error, message} = TopK.handle_ast(ast, store)
      assert is_binary(message)
    end

    refute_received {:storage_access, _, _}
  end

  test "TDIGEST.CREATE prepared commands bound compression before storage access" do
    parent = self()
    store = %{get: fn key -> send(parent, {:storage_access, :get, key}) end}

    for compression <- [0, -1, 1_001, "100"] do
      assert {:error, message} =
               TDigest.handle_ast({:tdigest_create, "key", compression}, store)

      assert is_binary(message)
    end

    refute_received {:storage_access, _, _}
  end

  test "TDigest prepared reads and writes validate numeric domains before storage access" do
    parent = self()
    store = %{get: fn key -> send(parent, {:storage_access, :get, key}) end}

    invalid_asts = [
      {:tdigest_add, "key", []},
      {:tdigest_add, "key", ["1.0"]},
      {:tdigest_quantile, "key", []},
      {:tdigest_quantile, "key", [-0.1]},
      {:tdigest_quantile, "key", [1.1]},
      {:tdigest_cdf, "key", []},
      {:tdigest_cdf, "key", ["1.0"]},
      {:tdigest_rank, "key", []},
      {:tdigest_revrank, "key", [1]},
      {:tdigest_byrank, "key", []},
      {:tdigest_byrank, "key", [1.0]},
      {:tdigest_byrevrank, "key", ["1"]},
      {:tdigest_trimmed_mean, "key", -0.1, 0.5},
      {:tdigest_trimmed_mean, "key", 0.5, 0.5},
      {:tdigest_trimmed_mean, "key", 0.7, 0.6},
      {:tdigest_trimmed_mean, "key", 0.0, "1.0"}
    ]

    for ast <- invalid_asts do
      assert {:error, message} = TDigest.handle_ast(ast, store)
      assert is_binary(message)
    end

    refute_received {:storage_access, _, _}
  end

  test "TDIGEST.MERGE prepared commands validate sources and options before storage access" do
    parent = self()
    store = %{get: fn key -> send(parent, {:storage_access, :get, key}) end}

    invalid_asts = [
      {:tdigest_merge, "dst", [], []},
      {:tdigest_merge, "dst", :invalid, []},
      {:tdigest_merge, "dst", [123], []},
      {:tdigest_merge, "dst", ["src"], :invalid},
      {:tdigest_merge, "dst", ["src"], compression: 0},
      {:tdigest_merge, "dst", ["src"], compression: 1_001},
      {:tdigest_merge, "dst", ["src"], override: "true"},
      {:tdigest_merge, "dst", ["src"], unknown: true}
    ]

    for ast <- invalid_asts do
      assert {:error, message} = TDigest.handle_ast(ast, store)
      assert is_binary(message)
    end

    refute_received {:storage_access, _, _}
  end
end
