defmodule Ferricstore.Flow.OrderedIndexTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.OrderedIndex

  setup do
    index = :ets.new(:flow_ordered_index_test_index, [:ordered_set])
    lookup = :ets.new(:flow_ordered_index_test_lookup, [:set])
    {:ok, index: index, lookup: lookup}
  end

  test "range_slice returns members ordered by score then id", %{index: index, lookup: lookup} do
    assert :ok =
             OrderedIndex.put_members(index, lookup, "due:email", [
               {"b", 10},
               {"a", 10},
               {"c", 12}
             ])

    assert [{"a", 10.0}, {"b", 10.0}] =
             OrderedIndex.range_slice(
               index,
               "due:email",
               :neg_inf,
               {:inclusive, 10.0},
               false,
               0,
               10
             )

    assert [{"c", 12.0}, {"b", 10.0}] =
             OrderedIndex.range_slice(index, "due:email", :neg_inf, :inf, true, 0, 2)

    assert [{"b", 10.0}, {"c", 12.0}] =
             OrderedIndex.rank_range(index, "due:email", 1, 2, false)
  end

  test "put_entries batches updates without double-counting", %{index: index, lookup: lookup} do
    assert :ok =
             OrderedIndex.put_entries(index, lookup, [
               {"history:a", "1000-1", 1_000},
               {"history:b", "1000-1", 1_000},
               {"history:a", "2000-2", 2_000}
             ])

    assert 2 = OrderedIndex.count_all(lookup, "history:a")
    assert 1 = OrderedIndex.count_all(lookup, "history:b")

    assert :ok = OrderedIndex.put_entries(index, lookup, [{"history:a", "2000-2", 2_500}])
    assert 2 = OrderedIndex.count_all(lookup, "history:a")

    assert [{"1000-1", 1000.0}, {"2000-2", 2500.0}] =
             OrderedIndex.rank_range(index, "history:a", 0, 10, false)
  end

  test "put_new_entries is append-only for validated new members", %{
    index: index,
    lookup: lookup
  } do
    assert :ok =
             OrderedIndex.put_new_entries(index, lookup, [
               {"history:a", "1000-1", 1_000},
               {"history:b", "1000-1", 1_000},
               {"history:a", "2000-2", 2_000}
             ])

    assert 2 = OrderedIndex.count_all(lookup, "history:a")
    assert 1 = OrderedIndex.count_all(lookup, "history:b")

    assert [{"1000-1", 1000.0}, {"2000-2", 2000.0}] =
             OrderedIndex.rank_range(index, "history:a", 0, 10, false)
  end

  test "delete_members removes lookup, ordered index, and count", %{index: index, lookup: lookup} do
    assert :ok =
             OrderedIndex.put_members(index, lookup, "worker:a", [
               {"flow-1", 3},
               {"flow-2", 4}
             ])

    assert :ok = OrderedIndex.delete_members(index, lookup, "worker:a", ["flow-1", "missing"])

    assert 1 = OrderedIndex.count_all(lookup, "worker:a")

    assert [{"flow-2", 4.0}] =
             OrderedIndex.range_slice(index, "worker:a", :neg_inf, :inf, false, 0, 10)
  end
end
