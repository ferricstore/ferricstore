defmodule Ferricstore.Flow.IndexQueryTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.IndexQuery

  @terminal_states ["completed", "failed", "cancelled"]

  @base_query %{
    count: 10,
    from_ms: nil,
    to_ms: nil,
    rev?: false,
    state: nil,
    terminal_only?: false
  }

  test "filtering is false when index query has no filters" do
    refute IndexQuery.filtering?(@base_query)
  end

  test "fetch_count only asks scan count for filtered queries" do
    scan_count = fn count -> {:scan, count} end

    assert IndexQuery.fetch_count(@base_query, scan_count) == 10
    assert IndexQuery.fetch_count(%{@base_query | rev?: true}, scan_count) == {:scan, 10}
  end

  test "filter_records matches index value state terminal flag and time range" do
    queued = %{id: "queued", parent_flow_id: "parent", state: "queued", updated_at_ms: 1_000}

    completed = %{
      id: "completed",
      parent_flow_id: "parent",
      state: "completed",
      updated_at_ms: 2_000
    }

    failed = %{id: "failed", parent_flow_id: "parent", state: "failed", updated_at_ms: 3_000}
    other_parent = %{id: "other", parent_flow_id: "other", state: "failed", updated_at_ms: 2_500}

    query = %{
      @base_query
      | from_ms: 1_500,
        to_ms: 3_500,
        terminal_only?: true
    }

    assert IndexQuery.filter_records(
             [queued, completed, failed, other_parent],
             :parent_flow_id,
             "parent",
             query,
             @terminal_states
           ) == [completed, failed]
  end

  test "record_matches supports explicit state filter" do
    query = %{@base_query | state: "running"}

    assert IndexQuery.record_matches?(
             %{state: "running", updated_at_ms: 10},
             query,
             @terminal_states
           )

    refute IndexQuery.record_matches?(
             %{state: "queued", updated_at_ms: 10},
             query,
             @terminal_states
           )
  end

  test "cursor tie-breaking filters records and RAM index entries by timestamp and id" do
    query = Map.merge(@base_query, %{rev?: true, to_ms: 10, before_id: "flow-050"})

    assert IndexQuery.record_matches?(
             %{id: "flow-049", state: "queued", updated_at_ms: 10},
             query,
             @terminal_states
           )

    refute IndexQuery.record_matches?(
             %{id: "flow-050", state: "queued", updated_at_ms: 10},
             query,
             @terminal_states
           )

    refute IndexQuery.entry_before_cursor?({"flow-051", 10}, query)
    assert IndexQuery.entry_before_cursor?({"flow-999", 9}, query)
  end
end
