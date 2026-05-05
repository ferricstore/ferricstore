defmodule Ferricstore.Commands.ClusterNoDefaultInstanceTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Cluster
  alias Ferricstore.Test.MockStore

  @default_key {FerricStore.Instance, :default}
  @slot_map_key :ferricstore_slot_map
  @shard_count_key :ferricstore_shard_count

  setup do
    original_default = :persistent_term.get(@default_key, :missing)
    original_slot_map = :persistent_term.get(@slot_map_key, :missing)
    original_shard_count = :persistent_term.get(@shard_count_key, :missing)

    :persistent_term.erase(@default_key)
    :persistent_term.erase(@slot_map_key)
    :persistent_term.erase(@shard_count_key)

    on_exit(fn ->
      restore(@default_key, original_default)
      restore(@slot_map_key, original_slot_map)
      restore(@shard_count_key, original_shard_count)
    end)

    {:ok, store: MockStore.make()}
  end

  test "CLUSTER.HEALTH reports shards down before default instance init", %{store: store} do
    result = Cluster.handle("CLUSTER.HEALTH", [], store)

    assert result =~ "shard_0:"
    assert result =~ "status: down"
    assert result =~ "keys: 0"
  end

  test "CLUSTER.STATS reports zero totals before default instance init", %{store: store} do
    result = Cluster.handle("CLUSTER.STATS", [], store)

    assert result =~ "shard_0:"
    assert result =~ "total_keys: 0"
    assert result =~ ~r/total_memory_bytes: \d+/
  end

  test "CLUSTER.KEYSLOT still computes deterministic slots before init", %{store: store} do
    slot = Cluster.handle("CLUSTER.KEYSLOT", ["{acct}:1"], store)

    assert is_integer(slot)
    assert slot == Cluster.handle("CLUSTER.KEYSLOT", ["{acct}:2"], store)
  end

  test "CLUSTER.SLOTS returns a fallback uniform slot map before init", %{store: store} do
    ranges = Cluster.handle("CLUSTER.SLOTS", [], store)

    assert is_list(ranges)

    assert Enum.reduce(ranges, 0, fn [first, last, _shard], acc -> acc + last - first + 1 end) ==
             1024
  end

  test "FERRICSTORE.HOTNESS reports zero counters before default instance init", %{store: store} do
    result = Cluster.handle("FERRICSTORE.HOTNESS", [], store)

    assert Enum.chunk_every(result, 2) |> Enum.any?(&(&1 == ["hot_reads", "0"]))
    assert Enum.chunk_every(result, 2) |> Enum.any?(&(&1 == ["cold_reads", "0"]))
  end

  defp restore(key, :missing), do: :persistent_term.erase(key)
  defp restore(key, value), do: :persistent_term.put(key, value)
end
