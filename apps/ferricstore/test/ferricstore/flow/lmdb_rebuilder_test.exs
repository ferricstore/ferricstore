defmodule Ferricstore.Flow.LMDBRebuilderTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.LMDBRebuilder
  alias Ferricstore.Flow.Keys

  setup do
    previous = Application.get_env(:ferricstore, :flow_lmdb_history_rebuild_page_size)

    on_exit(fn ->
      case previous do
        nil -> Application.delete_env(:ferricstore, :flow_lmdb_history_rebuild_page_size)
        value -> Application.put_env(:ferricstore, :flow_lmdb_history_rebuild_page_size, value)
      end
    end)
  end

  test "state entry scans distinguish an empty shard from a missing keydir" do
    keydir = :ets.new(:lmdb_rebuilder_keydir, [:set])

    assert {:ok, []} = LMDBRebuilder.__select_state_entries_for_test__(keydir)

    :ets.delete(keydir)

    assert {:error, :source_keydir_unavailable} =
             LMDBRebuilder.__select_state_entries_for_test__(keydir)
  end

  test "history projection page sizes remain positive and bounded" do
    Application.put_env(:ferricstore, :flow_lmdb_history_rebuild_page_size, 0)
    assert LMDBRebuilder.__history_projection_scan_limit_for_test__() == 4_096

    Application.put_env(:ferricstore, :flow_lmdb_history_rebuild_page_size, "all")
    assert LMDBRebuilder.__history_projection_scan_limit_for_test__() == 4_096

    Application.put_env(:ferricstore, :flow_lmdb_history_rebuild_page_size, 2_000_000)
    assert LMDBRebuilder.__history_projection_scan_limit_for_test__() == 65_536

    Application.put_env(:ferricstore, :flow_lmdb_history_rebuild_page_size, 5_000)
    assert LMDBRebuilder.__history_projection_scan_limit_for_test__() == 5_000
  end

  test "state entry rebuilds reduce bounded pages without materializing the full keydir" do
    keydir = :ets.new(:lmdb_rebuilder_paged_keydir, [:set])

    rows =
      for index <- 1..1_200 do
        {Keys.state_key("flow-#{index}"), "value", 0, 0, 0, index, 5}
      end

    true = :ets.insert(keydir, rows)

    ordinary_rows =
      for index <- 1..100 do
        {"ordinary-#{index}", "value", 0, 0, 0, index, 5}
      end

    true = :ets.insert(keydir, ordinary_rows)

    assert {:ok, {1_200, page_sizes}} =
             LMDBRebuilder.__reduce_state_entries_for_test__(
               keydir,
               {0, []},
               fn entries, {count, page_sizes} ->
                 {count + length(entries), [length(entries) | page_sizes]}
               end
             )

    assert Enum.all?(page_sizes, &(&1 > 0 and &1 <= 512))
    assert length(page_sizes) > 1
  end

  test "history rebuild staging retains exact latest events without an all-events map" do
    history_key = "f:{f}:h:flow-1"

    entries = [
      {history_key, "300-1", 300, "compound-300"},
      {"f:{f}:h:flow-2", "250-1", 250, "other-flow"},
      {history_key, "100-1", 100, "compound-100"},
      {history_key, "200-1", 200, "compound-200"}
    ]

    assert LMDBRebuilder.__retained_staged_history_entries_for_test__(
             entries,
             history_key,
             2
           ) == [
             {"200-1", 200, "compound-200"},
             {"300-1", 300, "compound-300"}
           ]

    source =
      File.read!(Path.expand("../../../lib/ferricstore/flow/lmdb_rebuilder.ex", __DIR__))

    refute source =~ "history_entries_by_key"
  end
end
