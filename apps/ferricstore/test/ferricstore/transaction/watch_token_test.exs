defmodule Ferricstore.Transaction.WatchTokenTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Transaction.WatchToken

  @context {:replicated_apply, 1_000}

  test "bitcask relocation does not change a logical token" do
    first = {"key", "value", 0, 1, 3, 100, 5}
    relocated = {"key", "value", 0, 1, 9, 800, 5}

    assert WatchToken.from_entry(first, @context, fn -> "value" end) ==
             WatchToken.from_entry(relocated, @context, fn -> "value" end)
  end

  test "a new Raft write changes the token even when the value is unchanged" do
    first = {"key", "same", 0, 1, {:waraft_segment, 41}, 0, 4}
    rewritten = {"key", "same", 0, 1, {:waraft_segment, 42}, 0, 4}

    refute WatchToken.from_entry(first, @context, fn -> "same" end) ==
             WatchToken.from_entry(rewritten, @context, fn -> "same" end)
  end

  test "accepts the raw one-row ETS lookup shape" do
    entry = {"key", "value", 0, 1, {:waraft_segment, 41}, 0, 5}

    assert WatchToken.from_entry([entry], @context, fn -> "value" end) ==
             WatchToken.from_entry(entry, @context, fn -> "value" end)
  end

  test "cold fallback hashes content rather than its physical location" do
    first = {"key", nil, 0, 1, 2, 10, 5}
    relocated = {"key", nil, 0, 1, 7, 90, 5}

    assert WatchToken.from_entry(first, @context, fn -> "value" end) ==
             WatchToken.from_entry(relocated, @context, fn -> "value" end)
  end

  test "opaque cached values use the authoritative materialized content" do
    opaque = {"key", {:external_value, 7}, 0, 1, 2, 10, 5}
    cold = {"key", nil, 0, 1, 2, 10, 5}

    assert WatchToken.from_entry(opaque, @context, fn -> "value" end) ==
             WatchToken.from_entry(cold, @context, fn -> "value" end)
  end

  test "unmaterializable opaque values fail closed" do
    opaque = {"key", {:external_value, 7}, 0, 1, 2, 10, 5}

    assert WatchToken.from_entry(opaque, @context, fn -> nil end) ==
             {:error, :watch_value_unavailable}
  end

  test "drift-ambiguous expiry fails closed without materializing" do
    entry = {"key", "value", 31_000, 1, 2, 10, 5}

    assert WatchToken.from_entry(entry, {:replicated_apply, 61_000, 1_000}, fn ->
             flunk("unsafe expiry must be rejected before materialization")
           end) == {:error, :hlc_drift_exceeded}
  end
end
