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
             "a",
             "dup"
           ]
  end

  test "RAM scores replace stale LMDB scores before ordering" do
    ram_entries = [{"dup", 50}]
    lmdb_entries = [{"dup", 10}, {"middle", 20}]

    assert IndexMerge.ids_from_scored_entries(ram_entries, lmdb_entries, 2, false) == [
             "middle",
             "dup"
           ]
  end

  test "priority list merge preserves RAM-first uniqueness and stops at the limit" do
    assert IndexMerge.ids_from_priority_lists(
             ["hot", "shared", "hot"],
             ["shared", "cold", "new"],
             3
           ) == ["hot", "shared", "cold"]

    assert IndexMerge.ids_from_priority_lists(["hot"], ["cold"], 0) == []
  end

  test "priority list merge matches concatenation, uniqueness, and take semantics" do
    :rand.seed(:exsss, {53, 71, 89})

    for _iteration <- 1..100 do
      ram_ids = Enum.map(1..30, fn _index -> "id-#{:rand.uniform(40)}" end)
      lmdb_ids = Enum.map(1..60, fn _index -> "id-#{:rand.uniform(60)}" end)

      for count <- [0, 1, 5, 25, 100] do
        assert IndexMerge.ids_from_priority_lists(ram_ids, lmdb_ids, count) ==
                 (ram_ids ++ lmdb_ids) |> Enum.uniq() |> Enum.take(count)
      end
    end
  end

  test "ordered scored merge shadows stale LMDB rows and stops at the forward limit" do
    ram_entries = [{"a", 10}, {"shared", 30}, {"z", 50}]

    lmdb_entries = [
      {"old", 5},
      {"shared", 20},
      {"cold", 40},
      {"cold", 45},
      {"new", 60}
    ]

    assert IndexMerge.ids_from_ordered_scored_entries(
             ram_entries,
             lmdb_entries,
             5,
             false
           ) == ["old", "a", "shared", "cold", "z"]
  end

  test "ordered scored merge preserves reverse ordering and highest LMDB duplicate" do
    ram_entries = [{"z", 50}, {"shared", 30}, {"a", 10}]

    lmdb_entries = [
      {"new", 60},
      {"cold", 45},
      {"cold", 40},
      {"shared", 20},
      {"old", 5}
    ]

    assert IndexMerge.ids_from_ordered_scored_entries(
             ram_entries,
             lmdb_entries,
             5,
             true
           ) == ["new", "z", "cold", "shared", "a"]
  end

  test "ordered scored merge matches the full-sort reference across mixed generations" do
    :rand.seed(:exsss, {17, 29, 43})

    for reverse? <- [false, true], _iteration <- 1..100 do
      ram_entries =
        1..30
        |> Enum.map(fn _index -> {"id-#{:rand.uniform(40)}", :rand.uniform(100)} end)
        |> Enum.uniq_by(&elem(&1, 0))
        |> sort_entries(reverse?)

      lmdb_entries =
        1..60
        |> Enum.map(fn _index -> {"id-#{:rand.uniform(60)}", :rand.uniform(100)} end)
        |> sort_entries(reverse?)

      for count <- [0, 1, 5, 25, 100] do
        assert IndexMerge.ids_from_ordered_scored_entries(
                 ram_entries,
                 lmdb_entries,
                 count,
                 reverse?
               ) ==
                 IndexMerge.ids_from_scored_entries(
                   ram_entries,
                   lmdb_entries,
                   count,
                   reverse?
                 )
      end
    end
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

  test "ids_from_query_entries normalizes ascending LMDB rows for reverse RAM merges" do
    ram_entries = [{"hot", 40}, {"shared", 30}]

    lmdb_entries = [
      {"old", 10, "state-old"},
      {"shared", 20, "state-shared"},
      {"new", 50, "state-new"}
    ]

    assert IndexMerge.ids_from_query_entries(ram_entries, lmdb_entries, 4, true) == [
             "new",
             "hot",
             "shared",
             "old"
           ]
  end

  test "query entry merge matches the full-sort reference in both directions" do
    :rand.seed(:exsss, {97, 101, 103})

    for reverse? <- [false, true], _iteration <- 1..100 do
      ram_entries =
        1..30
        |> Enum.map(fn _index -> {"id-#{:rand.uniform(40)}", :rand.uniform(100)} end)
        |> Enum.uniq_by(&elem(&1, 0))
        |> sort_entries(reverse?)

      lmdb_entries =
        1..60
        |> Enum.map(fn _index ->
          {"id-#{:rand.uniform(60)}", :rand.uniform(100), "state"}
        end)
        |> Enum.sort_by(fn {id, score, _state_key} -> {score, id} end)

      lmdb_scored = Enum.map(lmdb_entries, fn {id, score, _state_key} -> {id, score} end)

      for count <- [0, 1, 5, 25, 100] do
        assert IndexMerge.ids_from_query_entries(
                 ram_entries,
                 lmdb_entries,
                 count,
                 reverse?
               ) ==
                 IndexMerge.ids_from_scored_entries(
                   ram_entries,
                   lmdb_scored,
                   count,
                   reverse?
                 )
      end
    end
  end

  test "terminal_entries_from_chunks flattens, sorts, reverses, and caps" do
    chunks = [[{"b", 20}, {"a", 20}], [{"old", 10}]]

    assert IndexMerge.terminal_entries_from_chunks(chunks, 2, true) == [{"b", 20}, {"a", 20}]
  end

  test "terminal_entries_from_chunks preserves one-chunk ordering semantics" do
    chunk = [{"b", 20}, {"old", 10}, {"a", 20}]

    assert IndexMerge.terminal_entries_from_chunks([chunk], 2, true) == [{"b", 20}, {"a", 20}]
  end

  test "query_entries_from_chunks flattens and sorts by updated_at_ms then id" do
    chunks = [[{"b", 20, "sb"}, {"a", 20, "sa"}], [{"old", 10, "so"}]]

    assert IndexMerge.query_entries_from_chunks(chunks) == [
             {"old", 10, "so"},
             {"a", 20, "sa"},
             {"b", 20, "sb"}
           ]
  end

  test "query_entries_from_chunks preserves one-chunk ordering semantics" do
    chunk = [{"b", 20, "sb"}, {"old", 10, "so"}, {"a", 20, "sa"}]

    assert IndexMerge.query_entries_from_chunks([chunk]) == [
             {"old", 10, "so"},
             {"a", 20, "sa"},
             {"b", 20, "sb"}
           ]
  end

  defp sort_entries(entries, true),
    do: Enum.sort_by(entries, fn {id, score} -> {score, id} end, :desc)

  defp sort_entries(entries, false),
    do: Enum.sort_by(entries, fn {id, score} -> {score, id} end, :asc)
end
