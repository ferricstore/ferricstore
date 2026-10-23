defmodule Ferricstore.Flow.LMDBHistoryScanTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.LMDB.HistoryScan

  @history_key "f:{f}:h:scan"
  @wide_history_key "f:{f}:h:wide"

  setup do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lmdb_history_scan_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(path) end)

    # 999-1, 1000-2 .. 1000-12, 1001-13: the 1000 ms ids cross 9 -> 10.
    events =
      [{999, 1}] ++ Enum.map(2..12, &{1_000, &1}) ++ [{1_001, 13}]

    wide = Enum.map(1..300, &{2_000, &1})

    assert :ok =
             LMDB.write_batch(
               path,
               history_puts(@history_key, events) ++ history_puts(@wide_history_key, wide)
             )

    %{path: path}
  end

  test "reads the oldest and newest events in version order", %{path: path} do
    assert ids(HistoryScan.first(path, @history_key, 3)) == ["999-1", "1000-2", "1000-3"]

    assert ids(HistoryScan.first(path, @history_key, 10)) ==
             ["999-1" | Enum.map(2..10, &"1000-#{&1}")]

    assert ids(HistoryScan.last(path, @history_key, 4)) ==
             ["1001-13", "1000-12", "1000-11", "1000-10"]

    assert ids(HistoryScan.last(path, @history_key, 6)) ==
             ["1001-13", "1000-12", "1000-11", "1000-10", "1000-9", "1000-8"]

    assert ids(HistoryScan.first(path, @history_key, 100)) ==
             ["999-1" | Enum.map(2..12, &"1000-#{&1}")] ++ ["1001-13"]

    assert {:ok, []} = HistoryScan.first(path, @history_key, 0)
  end

  test "completes a millisecond wider than one read page", %{path: path} do
    assert ids(HistoryScan.first(path, @wide_history_key, 2)) == ["2000-1", "2000-2"]
    assert ids(HistoryScan.last(path, @wide_history_key, 2)) == ["2000-300", "2000-299"]

    assert ids(HistoryScan.after_event(path, @wide_history_key, "2000-9", 2)) ==
             ["2000-10", "2000-11"]

    assert ids(HistoryScan.before_event(path, @wide_history_key, "2000-100", 2)) ==
             ["2000-99", "2000-98"]
  end

  test "reads from and to a millisecond in version order", %{path: path} do
    assert ids(HistoryScan.from_ms(path, @history_key, 1_000, 3)) ==
             ["1000-2", "1000-3", "1000-4"]

    assert ids(HistoryScan.to_ms(path, @history_key, 1_000, 2)) == ["1000-12", "1000-11"]
    assert ids(HistoryScan.from_ms(path, @history_key, 1_001, 3)) == ["1001-13"]
    assert ids(HistoryScan.to_ms(path, @history_key, 999, 3)) == ["999-1"]
  end

  test "resumes after and before an event in version order", %{path: path} do
    assert ids(HistoryScan.after_event(path, @history_key, "1000-9", 3)) ==
             ["1000-10", "1000-11", "1000-12"]

    assert ids(HistoryScan.after_event(path, @history_key, "1000-12", 3)) == ["1001-13"]
    assert ids(HistoryScan.after_event(path, @history_key, "999-1", 2)) == ["1000-2", "1000-3"]
    assert ids(HistoryScan.after_event(path, @history_key, "1001-13", 2)) == []

    assert ids(HistoryScan.before_event(path, @history_key, "1000-10", 3)) ==
             ["1000-9", "1000-8", "1000-7"]

    assert ids(HistoryScan.before_event(path, @history_key, "1000-2", 3)) == ["999-1"]

    assert ids(HistoryScan.before_event(path, @history_key, "1001-13", 2)) == [
             "1000-12",
             "1000-11"
           ]
  end

  defp history_puts(history_key, events) do
    Enum.map(events, fn {ms, version} ->
      event_id = "#{ms}-#{version}"
      {:put, LMDB.history_index_key(history_key, event_id, ms), event_id}
    end)
  end

  defp ids({:ok, entries}), do: Enum.map(entries, fn {_key, event_id} -> event_id end)
end
