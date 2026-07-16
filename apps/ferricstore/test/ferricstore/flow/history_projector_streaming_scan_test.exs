defmodule Ferricstore.Flow.HistoryProjectorStreamingScanTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.HistoryProjector.Log

  test "production history scans fold bounded metadata pages" do
    log_source =
      File.read!(Path.expand("../../../lib/ferricstore/flow/history_projector/log.ex", __DIR__))

    recovery_source =
      File.read!(
        Path.expand("../../../lib/ferricstore/flow/history_projector/recovery.ex", __DIR__)
      )

    refute log_source =~ "NIF.v2_scan_file(file_path)"
    refute recovery_source =~ "NIF.v2_scan_file(file_path)"
    assert log_source =~ "NIF.v2_scan_file_page"
  end

  test "history page fold continues past one page and keeps latest tombstone semantics" do
    dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_history_streaming_scan_#{System.unique_integer([:positive])}"
      )

    history_dir = Path.join(dir, "history")
    history_path = Path.join(history_dir, "00000.log")
    File.mkdir_p!(history_dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    records =
      [{"target", "old", 0}] ++
        Enum.map(1..4_095, fn index -> {"filler-#{index}", "x", 0} end) ++
        [{"target", "latest", 0}]

    assert {:ok, locations} = NIF.v2_append_batch(history_path, records)
    assert length(locations) == 4_097
    assert {:ok, "latest"} = Log.scan_event_value(dir, "target")

    assert {:ok, {_offset, _record_size}} = NIF.v2_append_tombstone(history_path, "target")
    assert :miss = Log.scan_event_value(dir, "target")
  end

  test "history scan fallback does not return expired values" do
    dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_history_expired_scan_#{System.unique_integer([:positive])}"
      )

    history_dir = Path.join(dir, "history")
    history_path = Path.join(history_dir, "00000.log")
    File.mkdir_p!(history_dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    expired_at_ms = System.system_time(:millisecond) - 1

    assert {:ok, [_location]} =
             NIF.v2_append_batch(history_path, [{"expired", "secret", expired_at_ms}])

    assert :miss = Log.scan_event_value(dir, "expired")
  end
end
