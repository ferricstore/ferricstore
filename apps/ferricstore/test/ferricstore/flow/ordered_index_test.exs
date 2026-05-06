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

  test "move_entries moves members across keys and keeps counters consistent", %{
    index: index,
    lookup: lookup
  } do
    assert :ok =
             OrderedIndex.put_members(index, lookup, "queued", [
               {"flow-1", 10},
               {"flow-2", 20}
             ])

    assert :ok =
             OrderedIndex.put_members(index, lookup, "running", [
               {"flow-3", 30}
             ])

    assert :ok =
             OrderedIndex.move_entries(index, lookup, [
               {"queued", "running", "flow-1", 100},
               {"missing", "running", "flow-4", 40}
             ])

    assert [{"flow-2", 20.0}] =
             OrderedIndex.rank_range(index, "queued", 0, 10, false)

    assert [{"flow-3", 30.0}, {"flow-4", 40.0}, {"flow-1", 100.0}] =
             OrderedIndex.rank_range(index, "running", 0, 10, false)

    assert_index_invariants(index, lookup)
  end

  test "move_entries updates same-key scores without changing counts", %{
    index: index,
    lookup: lookup
  } do
    assert :ok = OrderedIndex.put_members(index, lookup, "due", [{"flow-1", 10}])

    assert :ok =
             OrderedIndex.move_entries(index, lookup, [
               {"due", "due", "flow-1", 50}
             ])

    assert 1 = OrderedIndex.count_all(lookup, "due")
    assert [{"flow-1", 50.0}] = OrderedIndex.rank_range(index, "due", 0, 10, false)
    assert_index_invariants(index, lookup)
  end

  defp assert_index_invariants(index, lookup) do
    lookup_entries =
      lookup
      |> :ets.tab2list()
      |> Enum.filter(fn
        {{:count, _key}, _count} -> false
        {{_key, _member}, _score} -> true
      end)

    index_entries = :ets.tab2list(index)

    counts =
      Enum.reduce(lookup_entries, %{}, fn {{key, member}, score}, acc ->
        assert :ets.member(index, {key, score, member})
        Map.update(acc, key, 1, &(&1 + 1))
      end)

    Enum.each(index_entries, fn
      {{key, score, member}, true} ->
        assert [{{^key, ^member}, ^score}] = :ets.lookup(lookup, {key, member})
    end)

    Enum.each(counts, fn {key, count} ->
      assert count == OrderedIndex.count_all(lookup, key)
    end)
  end
end
