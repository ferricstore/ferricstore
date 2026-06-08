defmodule Ferricstore.Flow.IndexMergeTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.IndexMerge

  test "ids_from_scored_entries orders by score and id, deduplicates, and caps count" do
    ram_entries = [{"b", 20}, {"a", 20}, {"dup", 30}]
    lmdb_entries = [{"old", 10}, {"dup", 40}, {"new", 50}]

    assert IndexMerge.ids_from_scored_entries(ram_entries, lmdb_entries, 4, false) == [
             "old",
             "a",
             "b",
             "dup"
           ]
  end

  test "ids_from_scored_entries reverses before deduplication" do
    ram_entries = [{"dup", 10}, {"a", 20}]
    lmdb_entries = [{"dup", 30}, {"b", 40}]

    assert IndexMerge.ids_from_scored_entries(ram_entries, lmdb_entries, 3, true) == [
             "b",
             "dup",
             "a"
           ]
  end

  test "ids_from_query_entries scores LMDB entries by updated_at_ms" do
    ram_entries = [{"hot", 20}]
    lmdb_entries = [{"cold", 10, "state-cold"}, {"new", 30, "state-new"}]

    assert IndexMerge.ids_from_query_entries(ram_entries, lmdb_entries, 10, false) == [
             "cold",
             "hot",
             "new"
           ]
  end

  test "terminal_entries_from_chunks flattens, sorts, reverses, and caps" do
    chunks = [[{"b", 20}, {"a", 20}], [{"old", 10}]]

    assert IndexMerge.terminal_entries_from_chunks(chunks, 2, true) == [{"b", 20}, {"a", 20}]
  end

  test "query_entries_from_chunks flattens and sorts by updated_at_ms then id" do
    chunks = [[{"b", 20, "sb"}, {"a", 20, "sa"}], [{"old", 10, "so"}]]

    assert IndexMerge.query_entries_from_chunks(chunks) == [
             {"old", 10, "so"},
             {"a", 20, "sa"},
             {"b", 20, "sb"}
           ]
  end
end
