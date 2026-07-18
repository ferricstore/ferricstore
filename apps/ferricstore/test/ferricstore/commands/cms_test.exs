defmodule Ferricstore.Commands.CMSTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.CMS
  alias Ferricstore.Test.ProbMockStore

  defp temp_prob_dir(prefix, suffix) do
    Path.join(System.tmp_dir!(), "#{prefix}_#{suffix}")
  end

  defp unique_suffix do
    "#{System.os_time(:nanosecond)}_#{System.unique_integer([:positive, :monotonic])}"
  end

  # ===========================================================================
  # CMS.INITBYDIM
  # ===========================================================================

  defp prob_file_path(store, key, ext) do
    dir = store.prob_dir.()
    Ferricstore.ProbFile.path(dir, key, ext)
  end

  describe "CMS.INITBYDIM" do
    test "creates a new sketch with given dimensions" do
      store = ProbMockStore.make_cms()
      assert :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert File.exists?(prob_file_path(store, "mysketch", "cms"))
    end

    test "uses key-specific probabilistic directory when available" do
      suffix = unique_suffix()
      wrong_dir = temp_prob_dir("cms_wrong_prob", suffix)
      right_dir = temp_prob_dir("cms_right_prob", suffix)

      key = "key_specific_cms_#{suffix}"
      wrong_path = Ferricstore.ProbFile.path(wrong_dir, key, "cms")
      right_path = Ferricstore.ProbFile.path(right_dir, key, "cms")

      store =
        ProbMockStore.make_cms()
        |> Map.put(:prob_dir, fn -> wrong_dir end)
        |> Map.put(:prob_dir_for_key, fn ^key -> right_dir end)

      try do
        assert :ok = CMS.handle("CMS.INITBYDIM", [key, "100", "7"], store)
        assert File.exists?(right_path)
        refute File.exists?(wrong_path)
      after
        File.rm_rf!(wrong_dir)
        File.rm_rf!(right_dir)
      end
    end

    test "returns error when key already exists" do
      store = ProbMockStore.make_cms()
      assert :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert {:error, msg} = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert msg =~ "already exists"
    end

    test "returns metadata write error when init registration fails" do
      store =
        ProbMockStore.make_cms()
        |> Map.put(:put, fn "mysketch", _raw, 0 -> {:error, :disk_full} end)

      assert {:error, :disk_full} = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
    end

    test "returns error with zero width" do
      store = ProbMockStore.make_cms()
      assert {:error, msg} = CMS.handle("CMS.INITBYDIM", ["key", "0", "7"], store)
      assert msg =~ "positive integer"
    end

    test "returns error with negative depth" do
      store = ProbMockStore.make_cms()
      assert {:error, msg} = CMS.handle("CMS.INITBYDIM", ["key", "100", "-1"], store)
      assert msg =~ "positive integer"
    end

    test "returns error with non-integer width" do
      store = ProbMockStore.make_cms()
      assert {:error, msg} = CMS.handle("CMS.INITBYDIM", ["key", "abc", "7"], store)
      assert msg =~ "not an integer"
    end

    test "returns error with non-integer depth" do
      store = ProbMockStore.make_cms()
      assert {:error, msg} = CMS.handle("CMS.INITBYDIM", ["key", "100", "abc"], store)
      assert msg =~ "not an integer"
    end

    test "returns error with wrong number of arguments (too few)" do
      store = ProbMockStore.make_cms()
      assert {:error, msg} = CMS.handle("CMS.INITBYDIM", ["key"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with wrong number of arguments (too many)" do
      store = ProbMockStore.make_cms()
      assert {:error, msg} = CMS.handle("CMS.INITBYDIM", ["key", "100", "7", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with no arguments" do
      store = ProbMockStore.make_cms()
      assert {:error, msg} = CMS.handle("CMS.INITBYDIM", [], store)
      assert msg =~ "wrong number of arguments"
    end
  end

  # ===========================================================================
  # CMS.INITBYPROB
  # ===========================================================================

  describe "CMS.INITBYPROB" do
    test "creates a sketch sized for error rate and confidence" do
      store = ProbMockStore.make_cms()
      assert :ok = CMS.handle("CMS.INITBYPROB", ["mysketch", "0.001", "0.999"], store)
      assert File.exists?(prob_file_path(store, "mysketch", "cms"))
    end

    test "sketch dimensions match theoretical formulas" do
      store = ProbMockStore.make_cms()
      assert :ok = CMS.handle("CMS.INITBYPROB", ["mysketch", "0.01", "0.99"], store)

      ["width", width, "depth", depth, "count", 0] =
        CMS.handle("CMS.INFO", ["mysketch"], store)

      expected_width = ceil(:math.exp(1) / 0.01)
      assert width == expected_width

      expected_depth = ceil(:math.log(1.0 / 0.99))
      assert depth == expected_depth
    end

    test "returns error when key already exists" do
      store = ProbMockStore.make_cms()
      assert :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert {:error, msg} = CMS.handle("CMS.INITBYPROB", ["mysketch", "0.01", "0.99"], store)
      assert msg =~ "already exists"
    end

    test "returns error with zero error rate" do
      store = ProbMockStore.make_cms()
      assert {:error, msg} = CMS.handle("CMS.INITBYPROB", ["key", "0", "0.99"], store)
      assert msg =~ "positive number"
    end

    test "returns error with negative error rate" do
      store = ProbMockStore.make_cms()
      assert {:error, msg} = CMS.handle("CMS.INITBYPROB", ["key", "-0.01", "0.99"], store)
      assert msg =~ "positive number"
    end

    test "returns error with probability >= 1" do
      store = ProbMockStore.make_cms()
      assert {:error, msg} = CMS.handle("CMS.INITBYPROB", ["key", "0.01", "1.0"], store)
      assert msg =~ "between 0 and 1"
    end

    test "returns error with probability <= 0" do
      store = ProbMockStore.make_cms()
      assert {:error, msg} = CMS.handle("CMS.INITBYPROB", ["key", "0.01", "0.0"], store)
      assert msg =~ "between 0 and 1"
    end

    test "returns error with wrong number of arguments" do
      store = ProbMockStore.make_cms()
      assert {:error, _} = CMS.handle("CMS.INITBYPROB", ["key"], store)
      assert {:error, _} = CMS.handle("CMS.INITBYPROB", [], store)
    end
  end

  # ===========================================================================
  # CMS.INCRBY
  # ===========================================================================

  describe "CMS.INCRBY" do
    test "increments a single element by 1" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert [1] = CMS.handle("CMS.INCRBY", ["mysketch", "elem1", "1"], store)
    end

    test "increments a single element by large count" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert [42] = CMS.handle("CMS.INCRBY", ["mysketch", "elem1", "42"], store)
    end

    test "increments multiple elements at once" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)

      result =
        CMS.handle("CMS.INCRBY", ["mysketch", "a", "5", "b", "10", "c", "3"], store)

      assert [5, 10, 3] = result
    end

    test "increments accumulate for the same element" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert [1] = CMS.handle("CMS.INCRBY", ["mysketch", "elem1", "1"], store)
      assert [2] = CMS.handle("CMS.INCRBY", ["mysketch", "elem1", "1"], store)
      assert [12] = CMS.handle("CMS.INCRBY", ["mysketch", "elem1", "10"], store)
    end

    test "returns error when key does not exist" do
      store = ProbMockStore.make_cms()
      assert {:error, _} = CMS.handle("CMS.INCRBY", ["missing", "elem1", "1"], store)
    end

    test "returns error with odd number of element/count args" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert {:error, _} = CMS.handle("CMS.INCRBY", ["mysketch", "elem1"], store)
    end

    test "returns error with zero count" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert {:error, msg} = CMS.handle("CMS.INCRBY", ["mysketch", "elem1", "0"], store)
      assert msg =~ "invalid count"
    end

    test "returns error with negative count" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert {:error, msg} = CMS.handle("CMS.INCRBY", ["mysketch", "elem1", "-1"], store)
      assert msg =~ "invalid count"
    end

    test "returns error with no arguments" do
      store = ProbMockStore.make_cms()
      assert {:error, _} = CMS.handle("CMS.INCRBY", [], store)
    end

    test "returns error with only key argument" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert {:error, _} = CMS.handle("CMS.INCRBY", ["mysketch"], store)
    end
  end

  # ===========================================================================
  # CMS.QUERY
  # ===========================================================================

  describe "CMS.QUERY" do
    test "returns 0 for elements never inserted" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      assert [0] = CMS.handle("CMS.QUERY", ["mysketch", "never_seen"], store)
    end

    test "returns correct count for inserted element" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", "elem1", "5"], store)
      assert [5] = CMS.handle("CMS.QUERY", ["mysketch", "elem1"], store)
    end

    test "queries multiple elements at once" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", "a", "3", "b", "7"], store)

      result = CMS.handle("CMS.QUERY", ["mysketch", "a", "b", "c"], store)
      assert [3, 7, 0] = result
    end

    test "returns error when key does not exist" do
      store = ProbMockStore.make_cms()
      assert {:error, _} = CMS.handle("CMS.QUERY", ["missing", "elem1"], store)
    end

    test "returns error with no element arguments" do
      store = ProbMockStore.make_cms()
      assert {:error, _} = CMS.handle("CMS.QUERY", ["key"], store)
    end

    test "returns error with no arguments" do
      store = ProbMockStore.make_cms()
      assert {:error, _} = CMS.handle("CMS.QUERY", [], store)
    end
  end

  # ===========================================================================
  # CMS.INFO
  # ===========================================================================

  describe "CMS.INFO" do
    test "returns width, depth, and count for sketch" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "200", "5"], store)

      assert ["width", 200, "depth", 5, "count", 0] =
               CMS.handle("CMS.INFO", ["mysketch"], store)
    end

    test "count reflects total insertions" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", "a", "3", "b", "7"], store)

      result = CMS.handle("CMS.INFO", ["mysketch"], store)
      assert ["width", 100, "depth", 7, "count", 10] = result
    end

    test "returns error when key does not exist" do
      store = ProbMockStore.make_cms()
      assert {:error, _} = CMS.handle("CMS.INFO", ["missing"], store)
    end

    test "returns error with wrong number of arguments" do
      store = ProbMockStore.make_cms()
      assert {:error, _} = CMS.handle("CMS.INFO", [], store)
      assert {:error, _} = CMS.handle("CMS.INFO", ["a", "b"], store)
    end
  end

  # ===========================================================================
  # CMS.MERGE
  # ===========================================================================

  describe "CMS.MERGE" do
    test "merges two source sketches into a new destination" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["src1", "100", "7"], store)
      :ok = CMS.handle("CMS.INITBYDIM", ["src2", "100", "7"], store)
      CMS.handle("CMS.INCRBY", ["src1", "a", "3"], store)
      CMS.handle("CMS.INCRBY", ["src2", "a", "5"], store)
      CMS.handle("CMS.INCRBY", ["src2", "b", "2"], store)

      assert :ok = CMS.handle("CMS.MERGE", ["dst", "2", "src1", "src2"], store)

      assert [8] = CMS.handle("CMS.QUERY", ["dst", "a"], store)
      assert [2] = CMS.handle("CMS.QUERY", ["dst", "b"], store)
    end

    test "replicates source keys instead of local absolute paths" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["src1", "100", "7"], store)
      :ok = CMS.handle("CMS.INITBYDIM", ["src2", "100", "7"], store)

      test_pid = self()

      store =
        Map.put(store, :prob_write, fn command ->
          send(test_pid, {:prob_write, command})
          :ok
        end)

      assert :ok = CMS.handle("CMS.MERGE", ["dst", "2", "src1", "src2"], store)

      assert_receive {:prob_write, {:cms_merge, "dst", ["src1", "src2"], [1, 1], params}}
      assert params == %{width: 100, depth: 7}
    end

    test "merges single source sketch" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["src1", "50", "5"], store)
      CMS.handle("CMS.INCRBY", ["src1", "x", "10"], store)

      assert :ok = CMS.handle("CMS.MERGE", ["dst", "1", "src1"], store)
      assert [10] = CMS.handle("CMS.QUERY", ["dst", "x"], store)
    end

    test "merges with weights" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["src1", "100", "7"], store)
      :ok = CMS.handle("CMS.INITBYDIM", ["src2", "100", "7"], store)
      CMS.handle("CMS.INCRBY", ["src1", "a", "4"], store)
      CMS.handle("CMS.INCRBY", ["src2", "a", "6"], store)

      assert :ok =
               CMS.handle(
                 "CMS.MERGE",
                 ["dst", "2", "src1", "src2", "WEIGHTS", "2", "3"],
                 store
               )

      assert [26] = CMS.handle("CMS.QUERY", ["dst", "a"], store)
    end

    test "WEIGHTS option is case-insensitive" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["src1", "100", "7"], store)
      :ok = CMS.handle("CMS.INITBYDIM", ["src2", "100", "7"], store)
      CMS.handle("CMS.INCRBY", ["src1", "a", "4"], store)
      CMS.handle("CMS.INCRBY", ["src2", "a", "6"], store)

      assert :ok =
               CMS.handle(
                 "CMS.MERGE",
                 ["dst", "2", "src1", "src2", "weights", "2", "3"],
                 store
               )

      assert [26] = CMS.handle("CMS.QUERY", ["dst", "a"], store)
    end

    test "overwrites an existing destination and is idempotent" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["dst", "100", "7"], store)
      :ok = CMS.handle("CMS.INITBYDIM", ["src1", "100", "7"], store)
      CMS.handle("CMS.INCRBY", ["dst", "a", "10"], store)
      CMS.handle("CMS.INCRBY", ["src1", "a", "5"], store)

      assert :ok = CMS.handle("CMS.MERGE", ["dst", "1", "src1"], store)
      assert [5] = CMS.handle("CMS.QUERY", ["dst", "a"], store)

      assert :ok = CMS.handle("CMS.MERGE", ["dst", "1", "src1"], store)
      assert [5] = CMS.handle("CMS.QUERY", ["dst", "a"], store)
    end

    test "returns error when source sketches have different dimensions" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["src1", "100", "7"], store)
      :ok = CMS.handle("CMS.INITBYDIM", ["src2", "200", "7"], store)

      assert {:error, msg} = CMS.handle("CMS.MERGE", ["dst", "2", "src1", "src2"], store)
      assert msg =~ "width/depth"
    end

    test "returns error when dst and src have different dimensions" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["dst", "200", "7"], store)
      :ok = CMS.handle("CMS.INITBYDIM", ["src1", "100", "7"], store)

      assert {:error, msg} = CMS.handle("CMS.MERGE", ["dst", "1", "src1"], store)
      assert msg =~ "width/depth"
    end

    test "returns error when source key does not exist" do
      store = ProbMockStore.make_cms()
      assert {:error, _} = CMS.handle("CMS.MERGE", ["dst", "1", "missing"], store)
    end

    test "returns error when numkeys is zero" do
      store = ProbMockStore.make_cms()
      assert {:error, _} = CMS.handle("CMS.MERGE", ["dst", "0"], store)
    end

    test "returns error with no arguments" do
      store = ProbMockStore.make_cms()
      assert {:error, _} = CMS.handle("CMS.MERGE", [], store)
    end

    test "returns error with only dst argument" do
      store = ProbMockStore.make_cms()
      assert {:error, _} = CMS.handle("CMS.MERGE", ["dst"], store)
    end
  end

  # ===========================================================================
  # Accuracy tests
  # ===========================================================================

  describe "accuracy" do
    test "never undercounts (CMS invariant)" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "1000", "7"], store)

      true_counts = %{
        "apple" => 100,
        "banana" => 50,
        "cherry" => 200,
        "date" => 10,
        "elderberry" => 75
      }

      for {element, count} <- true_counts do
        CMS.handle("CMS.INCRBY", ["mysketch", element, Integer.to_string(count)], store)
      end

      for {element, true_count} <- true_counts do
        [estimated] = CMS.handle("CMS.QUERY", ["mysketch", element], store)
        assert estimated >= true_count, "#{element}: estimated #{estimated} < true #{true_count}"
      end
    end

    test "total_count tracks cumulative insertions" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)

      CMS.handle("CMS.INCRBY", ["mysketch", "a", "5"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", "b", "3"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", "a", "2"], store)

      ["width", _, "depth", _, "count", total] = CMS.handle("CMS.INFO", ["mysketch"], store)
      assert total == 10
    end
  end

  # ===========================================================================
  # Edge cases
  # ===========================================================================

  describe "edge cases" do
    test "width=1 sketch (degenerate -- all elements hash to same bucket)" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "1", "1"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", "a", "5"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", "b", "3"], store)

      [est_a] = CMS.handle("CMS.QUERY", ["mysketch", "a"], store)
      [est_b] = CMS.handle("CMS.QUERY", ["mysketch", "b"], store)
      assert est_a == 8
      assert est_b == 8
    end

    test "element with empty string" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", "", "5"], store)
      assert [5] = CMS.handle("CMS.QUERY", ["mysketch", ""], store)
    end

    test "large increment values" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", "big", "1000000"], store)
      [est] = CMS.handle("CMS.QUERY", ["mysketch", "big"], store)
      assert est >= 1_000_000
    end
  end

  # ===========================================================================
  # mmap persistence
  # ===========================================================================

  describe "mmap persistence" do
    test "mmap file exists on disk after create" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      path = prob_file_path(store, "mysketch", "cms")
      assert File.exists?(path), "CMS mmap file should exist at #{path}"
    end

    test "data persists through the mmap resource" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)
      CMS.handle("CMS.INCRBY", ["mysketch", "elem", "42"], store)
      assert [42] = CMS.handle("CMS.QUERY", ["mysketch", "elem"], store)
    end

    test "multiple independent sketches do not interfere" do
      store = ProbMockStore.make_cms()
      :ok = CMS.handle("CMS.INITBYDIM", ["sketch_a", "100", "7"], store)
      :ok = CMS.handle("CMS.INITBYDIM", ["sketch_b", "100", "7"], store)
      CMS.handle("CMS.INCRBY", ["sketch_a", "x", "10"], store)
      CMS.handle("CMS.INCRBY", ["sketch_b", "x", "20"], store)
      [a_est] = CMS.handle("CMS.QUERY", ["sketch_a", "x"], store)
      [b_est] = CMS.handle("CMS.QUERY", ["sketch_b", "x"], store)
      assert a_est >= 10
      assert b_est >= 20
      assert a_est < b_est
    end
  end
end
