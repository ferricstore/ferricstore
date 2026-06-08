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
end
