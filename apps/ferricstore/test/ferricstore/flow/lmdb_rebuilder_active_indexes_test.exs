defmodule Ferricstore.Flow.LMDBRebuilder.ActiveIndexesTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.LMDBRebuilder.ActiveIndexes

  test "active index page walks preserve backend failures" do
    assert {:ok, 0} =
             ActiveIndexes.__rebuild_flow_indexes_from_lmdb_pages_for_test__(fn _after_key ->
               {:ok, []}
             end)

    assert {:error, :busy} =
             ActiveIndexes.__rebuild_flow_indexes_from_lmdb_pages_for_test__(fn _after_key ->
               {:error, :busy}
             end)

    assert {:error, {:invalid_active_index_page, :bad_reply}} =
             ActiveIndexes.__rebuild_flow_indexes_from_lmdb_pages_for_test__(fn _after_key ->
               :bad_reply
             end)

    assert {:error, {:invalid_active_index_value, "active-key"}} =
             ActiveIndexes.__rebuild_flow_indexes_from_lmdb_pages_for_test__(fn _after_key ->
               {:ok, [{"active-key", "corrupt"}]}
             end)
  end

  test "active index page walks reject a key and value identity mismatch" do
    value =
      Ferricstore.Flow.LMDB.encode_active_index_value(
        "expected-index",
        "flow-id",
        7,
        0,
        "state-key"
      )

    mismatched_key =
      Ferricstore.Flow.LMDB.active_index_key("other-index", "flow-id", 7)

    assert {:error, {:active_index_key_value_mismatch, ^mismatched_key}} =
             ActiveIndexes.__rebuild_flow_indexes_from_lmdb_pages_for_test__(fn
               <<>> -> {:ok, [{mismatched_key, value}]}
               ^mismatched_key -> {:ok, []}
             end)
  end

  test "active index page walks reject a non-advancing cursor" do
    calls = :atomics.new(1, signed: false)

    value =
      Ferricstore.Flow.LMDB.encode_active_index_value(
        "active-index",
        "flow-id",
        1,
        0,
        "state-key"
      )

    active_key = Ferricstore.Flow.LMDB.active_index_key("active-index", "flow-id", 1)

    fetch = fn _after_key ->
      case :atomics.add_get(calls, 1, 1) do
        count when count in [1, 2] -> {:ok, [{active_key, value}]}
        _ -> {:error, :would_loop}
      end
    end

    assert {:error, {:active_index_scan_stalled, ^active_key, ^active_key}} =
             ActiveIndexes.__rebuild_flow_indexes_from_lmdb_pages_for_test__(fetch)
  end

  test "active index pages batch score-index publication by key" do
    source =
      File.read!(
        Path.expand("../../../lib/ferricstore/flow/lmdb_rebuilder/active_indexes.ex", __DIR__)
      )

    assert source =~
             "do_rebuild_score_indexes(zset_score_index, zset_score_lookup, score_entries)"

    refute source =~ "Enum.each(score_entries"
  end

  test "score-index recovery does not duplicate native-only FIFO lanes" do
    index = :ets.new(:flow_rebuild_score_index, [:ordered_set])
    lookup = :ets.new(:flow_rebuild_score_lookup, [:set])

    record = %{
      id: "flow-id",
      type: "job",
      state: "queued",
      state_enter_seq: 7,
      updated_at_ms: 10,
      next_run_at_ms: 20,
      priority: 0,
      partition_key: "tenant-a"
    }

    assert :ok = ActiveIndexes.rebuild_score_indexes(index, lookup, record)

    lane_key = Ferricstore.Flow.FifoLane.lane_key("job", "queued", "tenant-a")

    assert [] =
             Ferricstore.Store.Shard.ZSetIndex.range(
               index,
               lane_key,
               :neg_inf,
               :inf,
               false
             )
  end
end
