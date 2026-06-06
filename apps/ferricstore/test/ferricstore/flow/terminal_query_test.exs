defmodule Ferricstore.Flow.TerminalQueryTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.TerminalQuery

  @terminal_states ["failed", "completed", "cancelled"]

  test "state accepts any and known terminal states" do
    assert TerminalQuery.state([], @terminal_states) == {:ok, "any"}
    assert TerminalQuery.state([state: "failed"], @terminal_states) == {:ok, "failed"}
    assert TerminalQuery.state([state: "completed"], @terminal_states) == {:ok, "completed"}
  end

  test "state rejects non-terminal values" do
    assert TerminalQuery.state([state: "queued"], @terminal_states) ==
             {:error, "ERR flow terminal state must be failed, completed, cancelled, or any"}
  end

  test "ids_from_chunks flattens deduplicates and caps by terminal state count" do
    assert TerminalQuery.ids_from_chunks([["a", "b"], ["b", "c"], ["d"]], 1, @terminal_states) ==
             [
               "a",
               "b",
               "c"
             ]
  end

  test "record filters keep only terminal or exact state records" do
    records = [
      %{id: "queued", state: "queued"},
      %{id: "failed", state: "failed"},
      %{id: "completed", state: "completed"}
    ]

    assert Enum.map(TerminalQuery.filter_any(records, @terminal_states), & &1.id) == [
             "failed",
             "completed"
           ]

    assert Enum.map(TerminalQuery.filter_state(records, "failed"), & &1.id) == ["failed"]
  end
end
