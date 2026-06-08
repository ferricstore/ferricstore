defmodule Ferricstore.Flow.HistoryEventTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.HistoryEvent

  test "parses millisecond prefix from generated event ids" do
    assert HistoryEvent.ms("12345-7") == 12_345
  end

  test "parses bare millisecond event ids" do
    assert HistoryEvent.ms("98765") == 98_765
  end

  test "returns zero for malformed and non-binary event ids" do
    assert HistoryEvent.ms("abc-1") == 0
    assert HistoryEvent.ms("123abc") == 0
    assert HistoryEvent.ms(nil) == 0
  end
end
