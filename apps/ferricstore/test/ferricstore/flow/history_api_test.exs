defmodule Ferricstore.Flow.HistoryAPITest do
  use ExUnit.Case, async: true
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
end
