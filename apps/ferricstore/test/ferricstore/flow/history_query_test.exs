defmodule Ferricstore.Flow.HistoryQueryTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.HistoryQuery

  @base_query %{
    count: 10,
    rev?: false,
    from_event: nil,
    to_event: nil,
    from_ms: nil,
    to_ms: nil,
    from_version: nil,
    to_version: nil,
    event: nil,
    worker: nil
  }

  test "filtering is false when query has no filters" do
    refute HistoryQuery.filtering?(@base_query)
  end

  test "fetch_count only asks scan count for filtered queries" do
    scan_count = fn count, rev? -> {count, rev?} end

    assert HistoryQuery.fetch_count(@base_query, scan_count) == 10

    assert HistoryQuery.fetch_count(%{@base_query | event: "completed", rev?: true}, scan_count) ==
             {10, true}
  end

  test "apply filters by event worker timestamp version and event id ranges" do
    ignored =
      {"1000-a",
       %{
         "event" => "created",
         "lease_owner" => "worker-a",
         "version" => 1
       }}

    matched =
      {"2000-b",
       %{
         "event" => "transitioned",
         "lease_owner" => "worker-b",
         "version" => "2"
       }}

    too_new =
      {"3000-c",
       %{
         "event" => "transitioned",
         "lease_owner" => "worker-b",
         "version" => 3
       }}

    query = %{
      @base_query
      | event: "transitioned",
        worker: "worker-b",
        from_ms: 1500,
        to_ms: 2500,
        from_version: 2,
        to_version: 2,
        from_event: "1500-start",
        to_event: "2500-end"
    }

    assert HistoryQuery.apply([ignored, matched, too_new], query) == [matched]
  end

  test "apply reverses before taking count for reverse queries" do
    events = [
      {"1000-a", %{"event" => "created", "version" => 1}},
      {"2000-b", %{"event" => "claimed", "version" => 2}},
      {"3000-c", %{"event" => "completed", "version" => 3}}
    ]

    assert HistoryQuery.apply(events, %{@base_query | rev?: true, count: 2}) == [
             {"3000-c", %{"event" => "completed", "version" => 3}},
             {"2000-b", %{"event" => "claimed", "version" => 2}}
           ]
  end

  test "range validators reject inverted ranges" do
    assert HistoryQuery.validate_ms_range(2, 1) == {:error, "ERR flow from_ms must be <= to_ms"}

    assert HistoryQuery.validate_version_range(3, 2) ==
             {:error, "ERR flow from_version must be <= to_version"}

    assert HistoryQuery.validate_event_range("2000-b", "1000-a") ==
             {:error, "ERR flow from_event must be <= to_event"}
  end
end
