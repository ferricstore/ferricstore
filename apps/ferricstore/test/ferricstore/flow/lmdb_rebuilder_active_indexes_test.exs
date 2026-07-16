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

    fetch = fn after_key ->
      case :atomics.add_get(calls, 1, 1) do
        1 -> {:ok, [{after_key, value}]}
        _ -> {:error, :would_loop}
      end
    end

    assert {:error, {:active_index_scan_stalled, <<>>, <<>>}} =
             ActiveIndexes.__rebuild_flow_indexes_from_lmdb_pages_for_test__(fetch)
  end
end
