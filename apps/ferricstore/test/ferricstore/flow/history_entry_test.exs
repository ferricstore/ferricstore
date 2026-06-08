defmodule Ferricstore.Flow.HistoryEntryTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.HistoryEntry

  test "converts tuple history entries to event id and fields map" do
    assert HistoryEntry.to_tuple({"1-0", ["event", "created", "state", "queued"]}) ==
             {"1-0", %{"event" => "created", "state" => "queued"}}
  end

  test "converts list history entries to event id and fields map" do
    assert HistoryEntry.to_tuple(["2-0", "event", "completed", "state", "completed"]) ==
             {"2-0", %{"event" => "completed", "state" => "completed"}}
  end

  test "last duplicate field wins" do
    assert HistoryEntry.fields_to_map(["event", "created", "event", "retry"]) == %{
             "event" => "retry"
           }
  end
end
