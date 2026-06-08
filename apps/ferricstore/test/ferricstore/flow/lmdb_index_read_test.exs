defmodule Ferricstore.Flow.LMDBIndexReadTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.LMDBIndexRead

  test "query_entries returns no cold entries for non-positive count" do
    assert LMDBIndexRead.query_entries(:ctx, "idx", nil, 0, false, %{}, 1_000) == {:ok, []}
  end

  test "terminal_entries skips non-terminal states" do
    assert LMDBIndexRead.terminal_entries(
             :ctx,
             "idx",
             "queued",
             nil,
             10,
             true,
             false,
             nil,
             ["completed"],
             1_000
           ) == {:ok, []}
  end

  test "terminal_entries skips cold storage when disabled" do
    assert LMDBIndexRead.terminal_entries(
             :ctx,
             "idx",
             "completed",
             nil,
             10,
             false,
             false,
             nil,
             ["completed"],
             1_000
           ) == {:ok, []}
  end

  test "query_scan_count delegates to bounded LMDB query window" do
    assert LMDBIndexRead.query_scan_count(10, 1_000) == 74
  end
end
