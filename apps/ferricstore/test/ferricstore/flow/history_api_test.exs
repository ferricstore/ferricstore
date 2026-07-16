defmodule Ferricstore.Flow.HistoryAPITest do
  use ExUnit.Case, async: false
  @moduletag :flow

  alias Ferricstore.Flow.HistoryAPI

  test "validates history time range before reading storage" do
    assert {:error, "ERR flow from_ms must be <= to_ms"} =
             HistoryAPI.history(%{}, "flow-1", from_ms: 10, to_ms: 1)
  end

  test "rejects non-keyword options" do
    assert {:error, "ERR flow opts must be a keyword list"} =
             HistoryAPI.history(%{}, "flow-1", %{})
  end

  test "rejects value hydration above the configured ceiling before reading storage" do
    assert {:error, "ERR flow payload_max_bytes exceeds maximum 65536"} =
             HistoryAPI.history(%{}, "flow-1", values: true, payload_max_bytes: 65_537)
  end

  test "prepare rejects malformed history event cursors" do
    assert HistoryAPI.prepare("flow-1", from_event: "not-an-event") ==
             {:error, "ERR flow from_event must be a history event id"}

    assert HistoryAPI.prepare("flow-1", to_event: "01-2") ==
             {:error, "ERR flow to_event must be a history event id"}

    assert {:ok, {_partition_key, _history_key, query, _include_cold?, _consistent?, _values}} =
             HistoryAPI.prepare("flow-1", from_event: "10-2", to_event: "11-3")

    assert query.from_event == "10-2"
    assert query.to_event == "11-3"
  end

  test "an omitted count respects a configured maximum below the built-in default" do
    previous = Application.get_env(:ferricstore, :flow_max_count)

    on_exit(fn ->
      if is_nil(previous) do
        Application.delete_env(:ferricstore, :flow_max_count)
      else
        Application.put_env(:ferricstore, :flow_max_count, previous)
      end
    end)

    Application.put_env(:ferricstore, :flow_max_count, 7)

    assert {:ok, {_partition_key, _history_key, query, _include_cold?, _consistent?, _values}} =
             HistoryAPI.prepare("flow-1", [])

    assert query.count == 7

    assert HistoryAPI.prepare("flow-1", count: 8) ==
             {:error, "ERR flow count exceeds maximum 7"}
  end
end
