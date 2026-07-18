defmodule Ferricstore.Commands.TopKTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.TopK
  alias Ferricstore.Test.MockStore

  defp temp_prob_dir(prefix, suffix) do
    Path.join(System.tmp_dir!(), "#{prefix}_#{suffix}")
  end

  defp unique_suffix do
    "#{System.os_time(:nanosecond)}_#{System.unique_integer([:positive, :monotonic])}"
  end

  # ===========================================================================
  # TOPK.RESERVE
  # ===========================================================================

  describe "TOPK.RESERVE" do
    test "creates a Top-K tracker with default dimensions" do
      store = MockStore.make()
      assert :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "10"], store)
      assert store.exists?.("hot_keys")
    end

    test "uses key-specific probabilistic directory when available" do
      suffix = unique_suffix()
      wrong_dir = temp_prob_dir("topk_wrong_prob", suffix)
      right_dir = temp_prob_dir("topk_right_prob", suffix)

      key = "key_specific_topk_#{suffix}"
      filename = Ferricstore.ProbFile.filename(key, "topk")

      store =
        MockStore.make()
        |> Map.put(:prob_dir, fn -> wrong_dir end)
        |> Map.put(:prob_dir_for_key, fn ^key -> right_dir end)

      try do
        assert :ok = TopK.handle("TOPK.RESERVE", [key, "10"], store)
        assert File.exists?(Path.join(right_dir, filename))
        refute File.exists?(Path.join(wrong_dir, filename))
      after
        File.rm_rf!(wrong_dir)
        File.rm_rf!(right_dir)
      end
    end

    test "creates a Top-K tracker with custom dimensions" do
      store = MockStore.make()

      assert :ok =
               TopK.handle("TOPK.RESERVE", ["hot_keys", "10", "20", "5"], store)

      assert store.exists?.("hot_keys")
    end

    test "INFO reflects custom dimensions" do
      store = MockStore.make()

      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "10", "20", "5"], store)

      assert ["k", 10, "width", 20, "depth", 5] =
               TopK.handle("TOPK.INFO", ["hot_keys"], store)
    end

    test "returns error when TopK key already exists" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "10"], store)

      assert {:error, msg} = TopK.handle("TOPK.RESERVE", ["hot_keys", "10"], store)
      assert msg =~ "already exists"
    end

    test "returns metadata write error when reserve registration fails" do
      store =
        MockStore.make()
        |> Map.put(:put, fn "hot_keys", _raw, 0 -> {:error, :disk_full} end)

      assert {:error, :disk_full} = TopK.handle("TOPK.RESERVE", ["hot_keys", "10"], store)
    end

    test "returns WRONGTYPE when key holds a string" do
      store = MockStore.make(%{"hot_keys" => {{"existing", 0}, 0}})
      assert {:error, msg} = TopK.handle("TOPK.RESERVE", ["hot_keys", "10"], store)
      assert msg =~ "WRONGTYPE"
    end

    test "returns error with zero k" do
      store = MockStore.make()
      assert {:error, msg} = TopK.handle("TOPK.RESERVE", ["hot_keys", "0"], store)
      assert msg =~ "positive integer"
    end

    test "returns error with negative k" do
      store = MockStore.make()
      assert {:error, msg} = TopK.handle("TOPK.RESERVE", ["hot_keys", "-1"], store)
      assert msg =~ "positive integer"
    end

    test "returns error with non-integer k" do
      store = MockStore.make()
      assert {:error, msg} = TopK.handle("TOPK.RESERVE", ["hot_keys", "abc"], store)
      assert msg =~ "not an integer"
    end

    test "rejects the removed decay argument" do
      store = MockStore.make()

      assert {:error, msg} =
               TopK.handle("TOPK.RESERVE", ["hot_keys", "10", "8", "7", "0.9"], store)

      assert msg =~ "wrong number of arguments"
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = TopK.handle("TOPK.RESERVE", [], store)
      assert {:error, _} = TopK.handle("TOPK.RESERVE", ["key"], store)

      # 3 args is invalid (need 2 or 4)
      assert {:error, _} = TopK.handle("TOPK.RESERVE", ["key", "10", "20"], store)
    end
  end

  # ===========================================================================
  # TOPK.ADD
  # ===========================================================================

  describe "TOPK.ADD" do
    test "adds element to empty Top-K, returns nil (no eviction)" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "3"], store)
      assert [nil] = TopK.handle("TOPK.ADD", ["hot_keys", "product:42"], store)
    end

    test "adds multiple elements at once" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "5"], store)

      result = TopK.handle("TOPK.ADD", ["hot_keys", "a", "b", "c"], store)
      assert length(result) == 3
      assert Enum.all?(result, &is_nil/1)
    end

    test "returns evicted element when Top-K is full" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "2"], store)

      # Fill the heap
      TopK.handle("TOPK.ADD", ["hot_keys", "a"], store)
      TopK.handle("TOPK.ADD", ["hot_keys", "b"], store)

      # Increment "a" to make it clearly the top element
      TopK.handle("TOPK.INCRBY", ["hot_keys", "a", "100"], store)
      # Increment "b" moderately
      TopK.handle("TOPK.INCRBY", ["hot_keys", "b", "50"], store)

      # Adding a new element with a large count should evict the min
      result = TopK.handle("TOPK.INCRBY", ["hot_keys", "c", "200"], store)

      # "c" should have evicted the lowest count element
      # The result should contain the evicted element name
      [evicted] = result
      assert evicted in ["a", "b"]
    end

    test "does not evict when new element count is too low" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "2"], store)

      # Add elements with high counts
      TopK.handle("TOPK.INCRBY", ["hot_keys", "a", "100"], store)
      TopK.handle("TOPK.INCRBY", ["hot_keys", "b", "100"], store)

      # Adding element with count 1 should not evict anyone
      [evicted] = TopK.handle("TOPK.ADD", ["hot_keys", "c"], store)
      assert evicted == nil
    end

    test "updates count when element already in Top-K" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "3"], store)

      TopK.handle("TOPK.ADD", ["hot_keys", "a"], store)
      TopK.handle("TOPK.ADD", ["hot_keys", "a"], store)
      TopK.handle("TOPK.ADD", ["hot_keys", "a"], store)

      # "a" should be in the list with updated count
      assert [1] = TopK.handle("TOPK.QUERY", ["hot_keys", "a"], store)
    end

    test "returns error when key does not exist" do
      store = MockStore.make()
      assert {:error, msg} = TopK.handle("TOPK.ADD", ["missing", "elem"], store)
      assert msg =~ "does not exist"
    end

    test "returns WRONGTYPE when key holds wrong type" do
      store = MockStore.make(%{"str_key" => {"a string", 0}})
      assert {:error, msg} = TopK.handle("TOPK.ADD", ["str_key", "elem"], store)
      assert msg =~ "WRONGTYPE"
    end

    test "returns error with no element arguments" do
      store = MockStore.make()
      assert {:error, _} = TopK.handle("TOPK.ADD", ["key"], store)
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, _} = TopK.handle("TOPK.ADD", [], store)
    end
  end

  # ===========================================================================
  # TOPK.INCRBY
  # ===========================================================================

  describe "TOPK.INCRBY" do
    test "increments element by specific count" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "5"], store)
      result = TopK.handle("TOPK.INCRBY", ["hot_keys", "a", "10"], store)
      assert [nil] = result
    end

    test "increments multiple elements at once" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "5"], store)

      result =
        TopK.handle("TOPK.INCRBY", ["hot_keys", "a", "5", "b", "10", "c", "3"], store)

      assert length(result) == 3
    end

    test "returns error when key does not exist" do
      store = MockStore.make()

      assert {:error, msg} =
               TopK.handle("TOPK.INCRBY", ["missing", "elem", "1"], store)

      assert msg =~ "does not exist"
    end

    test "returns error with odd number of element/count args" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "5"], store)
      assert {:error, _} = TopK.handle("TOPK.INCRBY", ["hot_keys", "elem"], store)
    end

    test "returns error with zero count" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "5"], store)

      assert {:error, msg} =
               TopK.handle("TOPK.INCRBY", ["hot_keys", "elem", "0"], store)

      assert msg =~ "invalid count"
    end

    test "returns error with negative count" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "5"], store)

      assert {:error, msg} =
               TopK.handle("TOPK.INCRBY", ["hot_keys", "elem", "-5"], store)

      assert msg =~ "invalid count"
    end

    test "returns error with non-integer count" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "5"], store)

      assert {:error, msg} =
               TopK.handle("TOPK.INCRBY", ["hot_keys", "elem", "abc"], store)

      assert msg =~ "invalid count"
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = TopK.handle("TOPK.INCRBY", [], store)
      assert {:error, _} = TopK.handle("TOPK.INCRBY", ["key"], store)
    end
  end

  # ===========================================================================
  # TOPK.QUERY
  # ===========================================================================

  describe "TOPK.QUERY" do
    test "returns 1 for element in top-K" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "5"], store)
      TopK.handle("TOPK.ADD", ["hot_keys", "a"], store)
      assert [1] = TopK.handle("TOPK.QUERY", ["hot_keys", "a"], store)
    end

    test "returns 0 for element not in top-K" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "5"], store)
      assert [0] = TopK.handle("TOPK.QUERY", ["hot_keys", "missing"], store)
    end

    test "queries multiple elements at once" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "5"], store)
      TopK.handle("TOPK.ADD", ["hot_keys", "a", "b"], store)

      assert [1, 1, 0] =
               TopK.handle("TOPK.QUERY", ["hot_keys", "a", "b", "c"], store)
    end

    test "returns error when key does not exist" do
      store = MockStore.make()
      assert {:error, msg} = TopK.handle("TOPK.QUERY", ["missing", "a"], store)
      assert msg =~ "does not exist"
    end

    test "returns WRONGTYPE when key holds wrong type" do
      store = MockStore.make(%{"str_key" => {"a string", 0}})
      assert {:error, msg} = TopK.handle("TOPK.QUERY", ["str_key", "a"], store)
      assert msg =~ "WRONGTYPE"
    end

    test "returns error with no element arguments" do
      store = MockStore.make()
      assert {:error, _} = TopK.handle("TOPK.QUERY", ["key"], store)
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, _} = TopK.handle("TOPK.QUERY", [], store)
    end
  end

  # ===========================================================================
  # TOPK.LIST
  # ===========================================================================

  describe "TOPK.LIST" do
    test "returns empty list for empty Top-K" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "5"], store)
      assert [] = TopK.handle("TOPK.LIST", ["hot_keys"], store)
    end

    test "returns elements sorted by count descending" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "5"], store)
      TopK.handle("TOPK.INCRBY", ["hot_keys", "a", "10", "b", "50", "c", "30"], store)

      result = TopK.handle("TOPK.LIST", ["hot_keys"], store)
      assert result == ["b", "c", "a"]
    end

    test "LIST WITHCOUNT returns elements interleaved with counts" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "5"], store)
      TopK.handle("TOPK.INCRBY", ["hot_keys", "a", "10", "b", "50"], store)

      result = TopK.handle("TOPK.LIST", ["hot_keys", "WITHCOUNT"], store)
      # Sorted by count descending: b(50), a(10)
      assert result == ["b", 50, "a", 10]
    end

    test "LIST WITHCOUNT uses one native snapshot" do
      source = File.read!(Path.expand("../../../lib/ferricstore/commands/topk.ex", __DIR__))

      with_count_source =
        source
        |> String.split("defp do_list_with_count_after_type_check", parts: 2)
        |> List.last()
        |> String.split("defp do_reserve", parts: 2)
        |> List.first()

      assert with_count_source =~ "NIF.topk_file_list_with_count_async"
      refute with_count_source =~ "NIF.topk_file_list_v2_async"
      refute with_count_source =~ "NIF.topk_file_count_v2_async"
    end

    test "LIST WITHCOUNT option is case-insensitive" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "5"], store)
      TopK.handle("TOPK.INCRBY", ["hot_keys", "a", "10", "b", "50"], store)

      assert ["b", 50, "a", 10] = TopK.handle("TOPK.LIST", ["hot_keys", "withcount"], store)
      assert ["b", 50, "a", 10] = TopK.handle("TOPK.LIST", ["hot_keys", "WiThCoUnT"], store)
    end

    test "returns error when key does not exist" do
      store = MockStore.make()
      assert {:error, msg} = TopK.handle("TOPK.LIST", ["missing"], store)
      assert msg =~ "does not exist"
    end

    test "returns WRONGTYPE when key holds wrong type" do
      store = MockStore.make(%{"str_key" => {"a string", 0}})
      assert {:error, msg} = TopK.handle("TOPK.LIST", ["str_key"], store)
      assert msg =~ "WRONGTYPE"
    end

    test "returns error with no arguments" do
      store = MockStore.make()
      assert {:error, _} = TopK.handle("TOPK.LIST", [], store)
    end

    test "returns error with too many arguments" do
      store = MockStore.make()
      assert {:error, _} = TopK.handle("TOPK.LIST", ["key", "WITHCOUNT", "extra"], store)
    end
  end

  # ===========================================================================
  # TOPK.INFO
  # ===========================================================================

  describe "TOPK.INFO" do
    test "returns k, width, and depth for default-created tracker" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "10"], store)

      result = TopK.handle("TOPK.INFO", ["hot_keys"], store)
      assert ["k", 10, "width", 8, "depth", 7] = result
    end

    test "returns error when key does not exist" do
      store = MockStore.make()
      assert {:error, msg} = TopK.handle("TOPK.INFO", ["missing"], store)
      assert msg =~ "does not exist"
    end

    test "returns WRONGTYPE when key holds wrong type" do
      store = MockStore.make(%{"str_key" => {"a string", 0}})
      assert {:error, msg} = TopK.handle("TOPK.INFO", ["str_key"], store)
      assert msg =~ "WRONGTYPE"
    end

    test "returns error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = TopK.handle("TOPK.INFO", [], store)
      assert {:error, _} = TopK.handle("TOPK.INFO", ["a", "b"], store)
    end
  end

  # ===========================================================================
  # Heavy hitter detection (functional tests)
  # ===========================================================================

  describe "heavy hitter detection" do
    test "top-K tracks the most frequent elements" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "3"], store)

      # Insert elements with different frequencies
      TopK.handle("TOPK.INCRBY", ["hot_keys", "rare", "1"], store)
      TopK.handle("TOPK.INCRBY", ["hot_keys", "uncommon", "5"], store)
      TopK.handle("TOPK.INCRBY", ["hot_keys", "common", "20"], store)
      TopK.handle("TOPK.INCRBY", ["hot_keys", "very_common", "100"], store)
      TopK.handle("TOPK.INCRBY", ["hot_keys", "most_common", "500"], store)

      # The top-3 should include the highest frequency elements
      result = TopK.handle("TOPK.LIST", ["hot_keys"], store)
      assert "most_common" in result
      assert "very_common" in result
      assert "common" in result
    end

    test "low-frequency elements get evicted as higher ones arrive" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "2"], store)

      # Start with low-frequency elements
      TopK.handle("TOPK.INCRBY", ["hot_keys", "low1", "1"], store)
      TopK.handle("TOPK.INCRBY", ["hot_keys", "low2", "2"], store)

      # These are in the heap
      assert [1] = TopK.handle("TOPK.QUERY", ["hot_keys", "low1"], store)
      assert [1] = TopK.handle("TOPK.QUERY", ["hot_keys", "low2"], store)

      # Add a high-frequency element -- should evict the lowest
      TopK.handle("TOPK.INCRBY", ["hot_keys", "high1", "100"], store)
      assert [1] = TopK.handle("TOPK.QUERY", ["hot_keys", "high1"], store)

      # One of the lows should have been evicted
      low1_in = TopK.handle("TOPK.QUERY", ["hot_keys", "low1"], store)
      low2_in = TopK.handle("TOPK.QUERY", ["hot_keys", "low2"], store)
      assert [0] in [low1_in, low2_in], "at least one low element should be evicted"
    end

    test "TOPK.LIST WITHCOUNT returns items with estimated counts" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "3"], store)

      TopK.handle("TOPK.INCRBY", ["hot_keys", "a", "10"], store)
      TopK.handle("TOPK.INCRBY", ["hot_keys", "b", "20"], store)
      TopK.handle("TOPK.INCRBY", ["hot_keys", "c", "30"], store)

      result = TopK.handle("TOPK.LIST", ["hot_keys", "WITHCOUNT"], store)

      # Should be [elem, count, elem, count, ...]
      assert length(result) == 6

      # First element should have highest count
      [first_elem, first_count | _] = result
      assert first_elem == "c"
      assert first_count >= 30
    end
  end

  # ===========================================================================
  # Edge cases
  # ===========================================================================

  describe "edge cases" do
    test "k=1 Top-K tracks only the single most frequent element" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "1"], store)

      TopK.handle("TOPK.INCRBY", ["hot_keys", "a", "100"], store)
      assert [1] = TopK.handle("TOPK.QUERY", ["hot_keys", "a"], store)

      # Adding a higher-frequency element should evict "a"
      TopK.handle("TOPK.INCRBY", ["hot_keys", "b", "200"], store)
      assert [1] = TopK.handle("TOPK.QUERY", ["hot_keys", "b"], store)
      assert [0] = TopK.handle("TOPK.QUERY", ["hot_keys", "a"], store)
    end

    test "adding same element multiple times updates count" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "3"], store)

      for _ <- 1..10 do
        TopK.handle("TOPK.ADD", ["hot_keys", "repeat"], store)
      end

      # Element should be in top-K with accumulated count
      assert [1] = TopK.handle("TOPK.QUERY", ["hot_keys", "repeat"], store)
      result = TopK.handle("TOPK.LIST", ["hot_keys", "WITHCOUNT"], store)
      [_elem, count] = result
      assert count >= 10
    end

    test "element with empty string" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "3"], store)
      TopK.handle("TOPK.ADD", ["hot_keys", ""], store)
      assert [1] = TopK.handle("TOPK.QUERY", ["hot_keys", ""], store)
    end

    test "element with special characters" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "3"], store)

      TopK.handle("TOPK.ADD", ["hot_keys", "hello world!"], store)
      TopK.handle("TOPK.ADD", ["hot_keys", <<0, 1, 2, 3>>], store)

      assert [1] = TopK.handle("TOPK.QUERY", ["hot_keys", "hello world!"], store)
      assert [1] = TopK.handle("TOPK.QUERY", ["hot_keys", <<0, 1, 2, 3>>], store)
    end

    test "large k with few elements returns all elements" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "100"], store)

      TopK.handle("TOPK.ADD", ["hot_keys", "a", "b", "c"], store)

      result = TopK.handle("TOPK.LIST", ["hot_keys"], store)
      assert length(result) == 3
      assert "a" in result
      assert "b" in result
      assert "c" in result
    end

    test "many elements with same count are handled gracefully" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "5"], store)

      # Add 10 elements all with count 1
      for i <- 1..10 do
        TopK.handle("TOPK.ADD", ["hot_keys", "elem_#{i}"], store)
      end

      # Should have exactly k=5 elements in the list
      result = TopK.handle("TOPK.LIST", ["hot_keys"], store)
      assert length(result) == 5
    end

    test "INCRBY with large count value" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "3"], store)
      result = TopK.handle("TOPK.INCRBY", ["hot_keys", "big", "1000000"], store)
      assert [nil] = result
      assert [1] = TopK.handle("TOPK.QUERY", ["hot_keys", "big"], store)
    end

    test "multiple ADD calls for many elements in single call" do
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "5"], store)

      elements = Enum.map(1..50, &"elem_#{&1}")
      result = TopK.handle("TOPK.ADD", ["hot_keys" | elements], store)
      assert length(result) == 50
    end
  end

  # ===========================================================================
  # Interaction between CMS and TopK
  # ===========================================================================

  describe "CMS and TopK type isolation" do
    test "CMS command on TopK key returns error (type isolation by file extension)" do
      alias Ferricstore.Commands.CMS
      store = MockStore.make()
      :ok = TopK.handle("TOPK.RESERVE", ["hot_keys", "10"], store)

      # CMS.QUERY looks for .cms file, not .topk — returns "does not exist"
      assert {:error, _msg} = CMS.handle("CMS.QUERY", ["hot_keys", "a"], store)
    end

    test "TopK command on CMS key returns error (type isolation by file extension)" do
      alias Ferricstore.Commands.CMS
      store = MockStore.make()
      :ok = CMS.handle("CMS.INITBYDIM", ["mysketch", "100", "7"], store)

      # TOPK.ADD looks for .topk file, not .cms — returns "does not exist"
      assert {:error, _msg} = TopK.handle("TOPK.ADD", ["mysketch", "a"], store)
    end
  end

  # ===========================================================================
  # Dispatcher integration
  # ===========================================================================

  describe "dispatcher integration" do
    test "TOPK commands route through the dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()

      assert :ok = Dispatcher.dispatch("TOPK.RESERVE", ["hot_keys", "3"], store)
      assert [nil] = Dispatcher.dispatch("TOPK.ADD", ["hot_keys", "a"], store)
      assert [1] = Dispatcher.dispatch("TOPK.QUERY", ["hot_keys", "a"], store)

      result = Dispatcher.dispatch("TOPK.LIST", ["hot_keys"], store)
      assert result == ["a"]

      info = Dispatcher.dispatch("TOPK.INFO", ["hot_keys"], store)
      assert ["k", 3 | _] = info
    end

    test "TOPK commands are case-insensitive via dispatcher" do
      alias Ferricstore.Commands.Dispatcher
      store = MockStore.make()

      assert :ok = Dispatcher.dispatch("topk.reserve", ["hot_keys", "3"], store)
      assert [nil] = Dispatcher.dispatch("topk.add", ["hot_keys", "a"], store)
      assert [1] = Dispatcher.dispatch("topk.query", ["hot_keys", "a"], store)
    end
  end
end
