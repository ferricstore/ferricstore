defmodule Ferricstore.Transaction.WatchTokenTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Transaction.WatchToken

  test "bitcask relocation does not change a logical token" do
    first = {"key", "value", 0, 1, 3, 100, 5}
    relocated = {"key", "value", 0, 1, 9, 800, 5}

    assert WatchToken.from_entry(first, 1_000, fn -> "value" end) ==
             WatchToken.from_entry(relocated, 1_000, fn -> "value" end)
  end

  test "a new Raft write changes the token even when the value is unchanged" do
    first = {"key", "same", 0, 1, {:waraft_segment, 41}, 0, 4}
    rewritten = {"key", "same", 0, 1, {:waraft_segment, 42}, 0, 4}

    refute WatchToken.from_entry(first, 1_000, fn -> "same" end) ==
             WatchToken.from_entry(rewritten, 1_000, fn -> "same" end)
  end

  test "accepts the raw one-row ETS lookup shape" do
    entry = {"key", "value", 0, 1, {:waraft_segment, 41}, 0, 5}

    assert WatchToken.from_entry([entry], 1_000, fn -> "value" end) ==
             WatchToken.from_entry(entry, 1_000, fn -> "value" end)
  end

  test "cold fallback hashes content rather than its physical location" do
    first = {"key", nil, 0, 1, 2, 10, 5}
    relocated = {"key", nil, 0, 1, 7, 90, 5}

    assert WatchToken.from_entry(first, 1_000, fn -> "value" end) ==
             WatchToken.from_entry(relocated, 1_000, fn -> "value" end)
  end

  test "opaque cached values use the authoritative materialized content" do
    opaque = {"key", {:external_value, 7}, 0, 1, 2, 10, 5}
    cold = {"key", nil, 0, 1, 2, 10, 5}

    assert WatchToken.from_entry(opaque, 1_000, fn -> "value" end) ==
             WatchToken.from_entry(cold, 1_000, fn -> "value" end)
  end

  test "unmaterializable opaque values fail closed" do
    opaque = {"key", {:external_value, 7}, 0, 1, 2, 10, 5}

    assert WatchToken.from_entry(opaque, 1_000, fn -> nil end) ==
             {:error, :watch_value_unavailable}
  end
end
