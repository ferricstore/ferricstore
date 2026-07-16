defmodule Ferricstore.Flow.RecordReadTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.RecordRead

  test "filter_index_records applies field filter sort reverse and count" do
    query = %{count: 1, from_ms: nil, to_ms: nil, rev?: true, state: nil, terminal_only?: false}

    records = [
      %{id: "a", parent_flow_id: "p", updated_at_ms: 1, state: "queued"},
      %{id: "b", parent_flow_id: "p", updated_at_ms: 3, state: "queued"},
      %{id: "c", parent_flow_id: "other", updated_at_ms: 4, state: "queued"}
    ]

    assert [%{id: "b"}] =
             RecordRead.filter_index_records(records, :parent_flow_id, "p", query, [])
  end

  test "filtered candidate scans resume after an underfilled heuristic window" do
    query = %{
      count: 2,
      from_ms: nil,
      to_ms: nil,
      rev?: false,
      state: nil,
      terminal_only?: false
    }

    records =
      Enum.map(1..80, fn index ->
        %{id: "flow-#{index}", updated_at_ms: index, match?: index in [70, 71]}
      end)

    parent = self()

    assert {:ok, [%{id: "flow-70"}, %{id: "flow-71"}]} =
             RecordRead.scan_filtered_candidate_windows(
               query,
               100,
               fn limit ->
                 send(parent, {:fetch_limit, limit})
                 {:ok, Enum.take(records, limit), limit >= length(records)}
               end,
               &Map.get(&1, :match?, false)
             )

    assert_received {:fetch_limit, 66}
    assert_received {:fetch_limit, 100}
  end

  test "filtered candidate scans fail explicitly when the bounded window is not exhaustive" do
    query = %{
      count: 2,
      from_ms: nil,
      to_ms: nil,
      rev?: false,
      state: nil,
      terminal_only?: false
    }

    assert {:error, "ERR flow query candidate limit exceeded (10)"} =
             RecordRead.scan_filtered_candidate_windows(
               query,
               10,
               fn limit ->
                 {:ok,
                  Enum.map(1..limit, fn index ->
                    %{id: "flow-#{index}", updated_at_ms: index, match?: false}
                  end), false}
               end,
               &Map.get(&1, :match?, false)
             )
  end
end
