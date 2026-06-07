defmodule Ferricstore.Flow.ValueHydrationTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.ValueHydration

  test "payload_result preserves nil and non-record results" do
    assert ValueHydration.payload_result(:ctx, {:ok, nil}, %{enabled?: true, max_bytes: 10}) ==
             {:ok, nil}

    assert ValueHydration.payload_result(:ctx, {:error, "ERR"}, %{enabled?: true, max_bytes: 10}) ==
             {:error, "ERR"}
  end

  test "payload_records and named_value_records no-op disabled or empty cases" do
    records = [%{id: "flow-1"}]

    assert ValueHydration.payload_records(:ctx, records, %{enabled?: false}) == records
    assert ValueHydration.payload_records(:ctx, [], %{enabled?: true, max_bytes: 10}) == []
    assert ValueHydration.named_value_records(:ctx, records, nil) == records
    assert ValueHydration.named_value_records(:ctx, records, []) == records
    assert ValueHydration.named_value_records(:ctx, [], :all) == []
  end
end
