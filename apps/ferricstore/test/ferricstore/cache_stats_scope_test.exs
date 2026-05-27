defmodule Ferricstore.CacheStatsScopeTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Stats
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    Stats.reset()

    on_exit(fn ->
      Stats.reset()
    end)

    :ok
  end

  test "Flow reads do not pollute user KV cache stats" do
    id = "cache-stats-flow-#{System.unique_integer([:positive])}"

    assert :ok =
             FerricStore.flow_create(id,
               type: "cache-stats",
               state: "queued",
               payload: "payload",
               run_at_ms: 1_000
             )

    Stats.reset()

    assert {:ok, %{id: ^id}} = FerricStore.flow_get(id, full: true)
    assert {:ok, [_ | _]} = FerricStore.flow_history(id, values: true)

    assert Stats.keyspace_hits() == 0
    assert Stats.keyspace_misses() == 0
    assert Stats.total_hot_reads() == 0
    assert Stats.total_cold_reads() == 0
    assert Stats.hotness_top(10) == []
  end

  test "direct user KV reads still update cache stats" do
    key = "cache-stats:user:#{System.unique_integer([:positive])}"

    assert :ok = FerricStore.set(key, "value")
    Stats.reset()

    assert {:ok, "value"} = FerricStore.get(key)

    assert Stats.keyspace_hits() == 1
    assert Stats.total_hot_reads() == 1
    assert [{"cache-stats", 1, 0, cold_pct}] = Stats.hotness_top(10)
    assert cold_pct == 0.0
  end

  test "metadata-only string type lookup does not count as a cache read" do
    key = "cache-stats:type:#{System.unique_integer([:positive])}"

    assert :ok = FerricStore.set(key, "value")
    Stats.reset()

    assert {:ok, "string"} = FerricStore.type(key)

    assert Stats.keyspace_hits() == 0
    assert Stats.total_hot_reads() == 0
    assert Stats.hotness_top(10) == []
  end

  test "internal storage keys are not tracked as user cache prefixes" do
    Stats.record_hot_read("f:{f}:s:flow-id")
    Stats.record_cold_read("f:{f}:h:flow-id")
    Stats.record_hot_read("H:hash" <> <<0>> <> "field")
    Stats.record_cold_read("T:hash")

    assert Stats.total_hot_reads() == 0
    assert Stats.total_cold_reads() == 0
    assert Stats.hotness_top(10) == []
  end
end
