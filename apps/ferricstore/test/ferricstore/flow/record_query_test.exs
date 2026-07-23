defmodule Ferricstore.Flow.RecordQueryTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.RecordQuery

  setup do
    previous = Application.get_env(:ferricstore, :flow_auto_partition_candidate_limit)
    Application.put_env(:ferricstore, :flow_auto_partition_candidate_limit, 5)

    on_exit(fn ->
      case previous do
        nil -> Application.delete_env(:ferricstore, :flow_auto_partition_candidate_limit)
        value -> Application.put_env(:ferricstore, :flow_auto_partition_candidate_limit, value)
      end
    end)
  end

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

  test "maybe_reverse and chunk helpers preserve query semantics" do
    assert RecordQuery.maybe_reverse([1, 2, 3], false) == [1, 2, 3]
    assert RecordQuery.maybe_reverse([1, 2, 3], true) == [3, 2, 1]

    chunks = []
    chunks = RecordQuery.prepend_chunk([3, 4], chunks)
    chunks = RecordQuery.prepend_chunk([1, 2], chunks)

    assert RecordQuery.flatten_chunks(chunks) == [1, 2, 3, 4]
  end

  test "bounded ordered chunk merge preserves ascending rank and first-source duplicates" do
    chunks = [
      [
        %{id: "a", updated_at_ms: 1, owner: :first},
        %{id: "duplicate", updated_at_ms: 3, owner: :first},
        %{id: "z", updated_at_ms: 9, owner: :first}
      ],
      [
        %{id: "b", updated_at_ms: 2, owner: :second},
        %{id: "duplicate", updated_at_ms: 3, owner: :second},
        %{id: "c", updated_at_ms: 4, owner: :second}
      ],
      []
    ]

    assert RecordQuery.merge_ordered_record_chunks(chunks, 4, false) == [
             %{id: "a", updated_at_ms: 1, owner: :first},
             %{id: "b", updated_at_ms: 2, owner: :second},
             %{id: "duplicate", updated_at_ms: 3, owner: :first},
             %{id: "c", updated_at_ms: 4, owner: :second}
           ]
  end

  test "bounded ordered chunk merge preserves descending rank and first-source duplicates" do
    chunks = [
      [
        %{id: "z", updated_at_ms: 9, owner: :first},
        %{id: "duplicate", updated_at_ms: 3, owner: :first},
        %{id: "a", updated_at_ms: 1, owner: :first}
      ],
      [
        %{id: "c", updated_at_ms: 4, owner: :second},
        %{id: "duplicate", updated_at_ms: 3, owner: :second},
        %{id: "b", updated_at_ms: 2, owner: :second}
      ]
    ]

    assert RecordQuery.merge_ordered_record_chunks(chunks, 4, true) == [
             %{id: "z", updated_at_ms: 9, owner: :first},
             %{id: "c", updated_at_ms: 4, owner: :second},
             %{id: "duplicate", updated_at_ms: 3, owner: :first},
             %{id: "b", updated_at_ms: 2, owner: :second}
           ]

    assert RecordQuery.merge_ordered_record_chunks(chunks, 0, true) == []
    assert RecordQuery.merge_ordered_record_chunks([], 10, true) == []
  end

  test "auto-partition merge caps each fetch and fails instead of returning an inexact prefix" do
    parent = self()

    assert {:error, "ERR flow auto-partition query candidate limit exceeded (5)"} =
             RecordQuery.bounded_auto_partition_records(
               [:p1, :p2, :p3, :p4],
               2,
               false,
               fn partition, fetch_count ->
                 send(parent, {:fetch, partition, fetch_count})

                 {:ok,
                  for index <- 1..fetch_count do
                    %{id: "#{partition}:#{index}", updated_at_ms: index}
                  end}
               end
             )

    assert_received {:fetch, :p1, 2}
    assert_received {:fetch, :p2, 2}
    assert_received {:fetch, :p3, 2}
    refute_received {:fetch, :p4, _count}
  end

  test "auto-partition merge keeps only the globally ranked result window" do
    records = %{
      p1: [%{id: "late", updated_at_ms: 30}],
      p2: [%{id: "first", updated_at_ms: 10}, %{id: "last", updated_at_ms: 40}],
      p3: [%{id: "second", updated_at_ms: 20}]
    }

    assert {:ok, result} =
             RecordQuery.bounded_auto_partition_records(
               Map.keys(records),
               2,
               false,
               fn partition, fetch_count -> {:ok, Enum.take(records[partition], fetch_count)} end
             )

    assert Enum.map(result, & &1.id) == ["first", "second"]
  end

  test "auto-partition merge applies the same global order in reverse" do
    records = %{
      p1: [%{id: "oldest", updated_at_ms: 10}, %{id: "newest", updated_at_ms: 40}],
      p2: [%{id: "third", updated_at_ms: 20}, %{id: "second", updated_at_ms: 30}]
    }

    assert {:ok, result} =
             RecordQuery.bounded_auto_partition_records(
               Map.keys(records),
               2,
               true,
               fn partition, fetch_count -> {:ok, Enum.take(records[partition], fetch_count)} end
             )

    assert Enum.map(result, & &1.id) == ["newest", "second"]
  end

  test "auto-partition merge rejects malformed fetch results without raising" do
    assert {:error, {:invalid_flow_candidate_fetch, :invalid}} =
             RecordQuery.bounded_auto_partition_records(
               [:p1],
               1,
               false,
               fn _partition, _fetch_count -> :invalid end
             )
  end

  test "filtered auto-partition merge enforces the global scanned candidate budget" do
    assert {:error, "ERR flow auto-partition query candidate limit exceeded (5)"} =
             RecordQuery.bounded_auto_partition_filtered_records(
               [:p1, :p2],
               2,
               false,
               fn
                 :p1, _fetch_count, _scan_budget ->
                   {:ok, [%{id: "p1", updated_at_ms: 1}], 3}

                 :p2, _fetch_count, scan_budget ->
                   {:ok, [%{id: "p2", updated_at_ms: 2}], scan_budget + 1}
               end
             )
  end

  test "filtered source merge uses the caller's global candidate budget" do
    assert {:error, "ERR flow query candidate limit exceeded (3)"} =
             RecordQuery.bounded_filtered_records(
               [:completed, :failed],
               1,
               false,
               3,
               fn
                 :completed, _fetch_count, _scan_budget ->
                   {:ok, [], 2}

                 :failed, _fetch_count, scan_budget ->
                   {:ok, [], scan_budget + 1}
               end
             )
  end
end
