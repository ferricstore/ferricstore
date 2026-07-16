defmodule Ferricstore.MemoryGuardEvictionConcurrencyTest do
  use ExUnit.Case, async: true

  alias Ferricstore.MemoryGuard

  test "eviction does not coldify a row replaced after it was sampled" do
    table = :ets.new(:memory_guard_eviction_race, [:set, :public])
    sampled = {"key", :binary.copy("a", 128), 0, 1, 10, 20, 128}
    replacement = {"key", :binary.copy("b", 256), 0, 2, 11, 30, 256}

    :ets.insert(table, sampled)
    :ets.insert(table, replacement)

    assert :stale == MemoryGuard.coldify_candidate(table, sampled)
    assert :ets.lookup(table, "key") == [replacement]
  end

  test "eviction atomically coldifies an unchanged sampled row" do
    table = :ets.new(:memory_guard_eviction_success, [:set, :public])
    sampled = {"key", :binary.copy("a", 128), 0, 1, 10, 20, 128}

    :ets.insert(table, sampled)

    assert {:evicted, 128, 128} == MemoryGuard.coldify_candidate(table, sampled)
    assert :ets.lookup(table, "key") == [{"key", nil, 0, 1, 10, 20, 128}]
  end
end
