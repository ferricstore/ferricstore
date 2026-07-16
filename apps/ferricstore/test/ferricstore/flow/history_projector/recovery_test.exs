defmodule Ferricstore.Flow.HistoryProjector.RecoveryTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.HistoryProjector.Recovery
  alias Ferricstore.Flow.HistoryProjector.Storage

  test "history log recovery never treats a directory as an empty safe log" do
    dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_history_recovery_path_#{System.unique_integer([:positive])}"
      )

    history_log = Path.join([dir, "history", "00000.log"])
    File.mkdir_p!(history_log)
    on_exit(fn -> File.rm_rf!(dir) end)

    refute Recovery.history_log_safe_to_skip?(dir)
    assert Storage.ensure_history_file(dir) == {:error, {:invalid_history_file_type, :directory}}
  end

  test "history recovery rejects unknown current-log keys instead of silently dropping them" do
    history_key = Ferricstore.Flow.Keys.history_key("recovery-schema")
    event_id = "1000-1"

    physical_key =
      Ferricstore.Flow.HistoryProjector.KeyCodec.history_entry_key(history_key, event_id)

    assert Recovery.live_history_records([{physical_key, 10, 20, 0, false}]) ==
             {:ok, %{physical_key => {10, 20, 0}}}

    value_key = Ferricstore.Flow.Keys.value_key("recovery-schema", :payload, 1)
    assert Recovery.live_history_records([{value_key, 30, 40, 0, false}]) == {:ok, %{}}

    assert Recovery.live_history_records([{"unknown-log-key", 0, 1, 0, false}]) ==
             {:error, {:invalid_history_log_key, "unknown-log-key"}}
  end
end
