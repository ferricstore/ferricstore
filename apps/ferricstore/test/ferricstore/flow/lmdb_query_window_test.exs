defmodule Ferricstore.Flow.LMDBQueryWindowTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.LMDBQueryWindow

  test "pads u64 values for lexicographic timestamp ordering" do
    assert LMDBQueryWindow.pad_u64(7) == "00000000000000000007"
    assert LMDBQueryWindow.pad_u64(12_345_678_901_234_567_890) == "12345678901234567890"
  end

  test "builds lower and upper timestamp seek keys" do
    assert LMDBQueryWindow.time_seek_key("prefix:", 42) ==
             "prefix:00000000000000000042"

    assert LMDBQueryWindow.time_upper_seek_key("prefix:", 42) ==
             "prefix:00000000000000000042" <> <<255>>
  end

  test "query scan count expands small windows and respects caps" do
    assert LMDBQueryWindow.query_scan_count(10, 10_000) == 74
    assert LMDBQueryWindow.query_scan_count(5_000, 10_000) == 10_000
  end

  test "history scan count keeps reverse scans exact" do
    assert LMDBQueryWindow.history_query_scan_count(10, true, 1_000_000) == 10
  end

  test "history scan count expands forward scans and respects caps" do
    assert LMDBQueryWindow.history_query_scan_count(10, false, 1_000_000) == 74
    assert LMDBQueryWindow.history_query_scan_count(500_000, false, 1_000_000) == 1_000_000
  end
end
