defmodule Ferricstore.Flow.RecordQueryTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.RecordQuery

  test "fetch_count keeps count when no time filter is present" do
    assert RecordQuery.fetch_count(10, nil, nil, fn _count ->
             flunk("scan count should not be called without a time filter")
           end) == 10
  end

  test "fetch_count delegates when a time filter is present" do
    assert RecordQuery.fetch_count(10, 1_000, nil, &(&1 * 4)) == 40
    assert RecordQuery.fetch_count(10, nil, 2_000, &(&1 * 4)) == 40
  end

  test "filter_by_ms honors optional bounds" do
    records = [
      %{id: "old", updated_at_ms: 900},
      %{id: "match-a", updated_at_ms: 1_000},
      %{id: "match-b", updated_at_ms: 1_500},
      %{id: "new", updated_at_ms: 2_100}
    ]

    assert records |> RecordQuery.filter_by_ms(1_000, 2_000) |> Enum.map(& &1.id) ==
             ["match-a", "match-b"]

    assert records |> RecordQuery.filter_by_ms(nil, 1_000) |> Enum.map(& &1.id) ==
             ["old", "match-a"]

    assert records |> RecordQuery.filter_by_ms(1_500, nil) |> Enum.map(& &1.id) ==
             ["match-b", "new"]
  end

  test "sort_by_update uses timestamp then id order" do
    records = [
      %{id: "b", updated_at_ms: 2},
      %{id: "c", updated_at_ms: 1},
      %{id: "a", updated_at_ms: 2}
    ]

    assert records |> RecordQuery.sort_by_update() |> Enum.map(& &1.id) == ["c", "a", "b"]
  end

  test "maybe_reverse and chunk helpers preserve old flow query semantics" do
    assert RecordQuery.maybe_reverse([1, 2, 3], false) == [1, 2, 3]
    assert RecordQuery.maybe_reverse([1, 2, 3], true) == [3, 2, 1]

    chunks = []
    chunks = RecordQuery.prepend_chunk([3, 4], chunks)
    chunks = RecordQuery.prepend_chunk([1, 2], chunks)

    assert RecordQuery.flatten_chunks(chunks) == [1, 2, 3, 4]
  end
end
