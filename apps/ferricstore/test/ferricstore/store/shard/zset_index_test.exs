defmodule Ferricstore.Store.Shard.ZSetIndexTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.Shard.ZSetIndex

  setup do
    index = :ets.new(:zset_index_test, [:ordered_set])
    lookup = :ets.new(:zset_lookup_test, [:set])

    on_exit(fn ->
      delete_table(index)
      delete_table(lookup)
    end)

    {:ok, index: index, lookup: lookup}
  end

  test "reverse score range slice includes inclusive max-score ties", %{
    index: index,
    lookup: lookup
  } do
    insert_members(index, lookup, "zs", [{"a", "2"}, {"b", "2"}, {"c", "2"}, {"d", "1"}])

    assert [{"c", 2.0}, {"b", 2.0}] ==
             ZSetIndex.range_slice(
               index,
               "zs",
               {:inclusive, 2.0},
               {:inclusive, 2.0},
               true,
               0,
               2
             )
  end

  test "forward score range slice starts at inclusive lower bound", %{
    index: index,
    lookup: lookup
  } do
    insert_members(index, lookup, "zs", [
      {"a", "2"},
      {"b", "2"},
      {"c", "2"},
      {"d", "3"},
      {"before", "1"}
    ])

    assert [{"b", 2.0}, {"c", 2.0}] ==
             ZSetIndex.range_slice(
               index,
               "zs",
               {:inclusive, 2.0},
               {:inclusive, 3.0},
               false,
               1,
               2
             )
  end

  test "score range honors exclusive bounds through the ETS index", %{
    index: index,
    lookup: lookup
  } do
    insert_members(index, lookup, "zs", [{"a", "1"}, {"b", "2"}, {"c", "2"}, {"d", "3"}])

    assert [{"d", 3.0}] ==
             ZSetIndex.range(index, "zs", {:exclusive, 2.0}, {:inclusive, 3.0}, false)

    assert [{"a", 1.0}] ==
             ZSetIndex.range(index, "zs", {:inclusive, 1.0}, {:exclusive, 2.0}, true)

    assert 2 == ZSetIndex.count(index, lookup, "zs", {:inclusive, 2.0}, {:inclusive, 2.0})
    assert 4 == ZSetIndex.count(index, lookup, "zs", :neg_inf, :inf)
  end

  test "rank cursors stay inside the redis key namespace", %{index: index, lookup: lookup} do
    insert_members(index, lookup, "other", [{"z", "99"}])
    insert_members(index, lookup, "zs", [{"a", "1"}, {"b", "2"}, {"c", "3"}])

    assert [{"c", 3.0}, {"b", 2.0}] == ZSetIndex.rank_range(index, "zs", 0, 1, true)
    assert 1 == ZSetIndex.member_rank(index, lookup, "zs", "b", true)
  end

  test "bulk put updates members and keeps count exact", %{index: index, lookup: lookup} do
    insert_members(index, lookup, "zs", [{"a", "1"}, {"b", "2"}])

    assert :ok == ZSetIndex.put_members(index, lookup, "zs", [{"b", 4}, {"c", 3.0}])

    assert 3 == ZSetIndex.count(index, lookup, "zs", :neg_inf, :inf)
    assert [{"a", 1.0}, {"c", 3.0}, {"b", 4.0}] == ZSetIndex.rank_range(index, "zs", 0, 2, false)
    assert 2 == ZSetIndex.member_rank(index, lookup, "zs", "b", false)
  end

  test "bulk new put skips lookup and keeps count exact", %{index: index, lookup: lookup} do
    assert :ok == ZSetIndex.put_new_members(index, lookup, "zs", [{"a", 1}, {"b", 2.0}])

    assert 2 == ZSetIndex.count(index, lookup, "zs", :neg_inf, :inf)
    assert [{"a", 1.0}, {"b", 2.0}] == ZSetIndex.rank_range(index, "zs", 0, 2, false)
  end

  test "ready empty clears stale entries before accepting tracked puts", %{
    index: index,
    lookup: lookup
  } do
    insert_members(index, lookup, "zs", [{"old", "99"}])

    assert :ok == ZSetIndex.mark_ready_empty(index, lookup, "zs")
    assert ZSetIndex.ready?(lookup, "zs")
    assert 0 == ZSetIndex.count(index, lookup, "zs", :neg_inf, :inf)
    assert [] == ZSetIndex.rank_range(index, "zs", 0, 10, false)

    assert :ok == ZSetIndex.apply_put_to_tables(index, lookup, "zs", <<"Z:zs", 0, "new">>, "1")
    assert [{"new", 1.0}] == ZSetIndex.rank_range(index, "zs", 0, 10, false)
  end

  test "new ready empty marks a proven-new zset without clearing stale entries", %{
    index: index,
    lookup: lookup
  } do
    insert_members(index, lookup, "zs", [{"old", "99"}])

    assert :ok == ZSetIndex.mark_new_ready_empty(index, lookup, "zs")
    assert ZSetIndex.ready?(lookup, "zs")
    assert 1 == ZSetIndex.count(index, lookup, "zs", :neg_inf, :inf)
    assert [{"old", 99.0}] == ZSetIndex.rank_range(index, "zs", 0, 10, false)
  end

  test "bulk delete removes only present members and keeps count exact", %{
    index: index,
    lookup: lookup
  } do
    insert_members(index, lookup, "zs", [{"a", "1"}, {"b", "2"}, {"c", "3"}])

    assert :ok == ZSetIndex.delete_members(index, lookup, "zs", ["a", "missing", "c"])

    assert 1 == ZSetIndex.count(index, lookup, "zs", :neg_inf, :inf)
    assert [{"b", 2.0}] == ZSetIndex.rank_range(index, "zs", 0, 10, false)
    assert nil == ZSetIndex.member_rank(index, lookup, "zs", "a", false)
  end

  test "bulk delete handles duplicate delete requests once", %{index: index, lookup: lookup} do
    insert_members(index, lookup, "zs", [{"a", "1"}, {"b", "2"}])

    assert :ok == ZSetIndex.delete_members(index, lookup, "zs", ["a", "a", "missing"])

    assert 1 == ZSetIndex.count(index, lookup, "zs", :neg_inf, :inf)
    assert [{"b", 2.0}] == ZSetIndex.rank_range(index, "zs", 0, 10, false)
  end

  defp insert_members(index, lookup, redis_key, members) do
    Enum.each(members, fn {member, score} ->
      ZSetIndex.put_member(index, lookup, redis_key, member, score)
    end)
  end

  defp delete_table(table) do
    if :ets.info(table) != :undefined do
      :ets.delete(table)
    end
  end
end
