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

  test "orders event ids by millisecond, then version" do
    ids = ["1000-10", "999-12", "1000-9", "1001-1", "1000-1"]

    assert Enum.sort_by(ids, &HistoryEvent.sort_key/1) ==
             ["999-12", "1000-1", "1000-9", "1000-10", "1001-1"]
  end

  test "reads the version of generated event ids only" do
    assert HistoryEvent.version("1000-42") == 42
    assert HistoryEvent.version("1000-x") == 0
    assert HistoryEvent.version("ms-4") == 0
    assert HistoryEvent.version("1000") == 0
    assert HistoryEvent.version("1000-18446744073709551616") == 0
    assert HistoryEvent.version(nil) == 0
  end
end
