defmodule Ferricstore.Flow.HistoryProjector.TrimTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.HistoryProjector.Trim

  test "history hard-cap trims use bounded publication batches" do
    assert Trim.history_trim_batch_size(1) == 1
    assert Trim.history_trim_batch_size(4_096) == 4_096
    assert Trim.history_trim_batch_size(10_000) == 4_096
  end

  test "tombstones sync the history log before trim publication continues" do
    dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_history_trim_sync_#{System.unique_integer([:positive])}"
      )

    file_path = Path.join(dir, "00000.log")
    File.mkdir_p!(dir)
    File.touch!(file_path)
    on_exit(fn -> File.rm_rf!(dir) end)
    parent = self()

    callbacks = %{
      sync_history_log_before_publish: fn ^file_path ->
        send(parent, :history_log_synced)
        :ok
      end
    }

    assert :ok = Trim.append_tombstones(file_path, ["old-history-entry"], callbacks)
    assert_receive :history_log_synced
  end

  test "history cap trim decoding requires an exact valid LMDB batch" do
    history_key = Ferricstore.Flow.Keys.history_key("trim-batch")
    event_id = "1000-1"

    history_index_key =
      Ferricstore.Flow.LMDB.history_index_key(history_key, event_id, 1_000)

    compound_key =
      Ferricstore.Flow.Keys.stream_entry_key_from_history_key(history_key, event_id)

    value =
      Ferricstore.Flow.LMDB.encode_history_index_value(
        event_id,
        1_000,
        compound_key,
        0
      )

    assert Trim.decode_lmdb_history_trim_items([{history_index_key, value}], 1) ==
             {:ok, [{event_id, compound_key, history_index_key}]}

    assert Trim.decode_lmdb_history_trim_items([{history_index_key, value}], 2) ==
             {:error, {:history_trim_batch_result_mismatch, 2, 1}}

    assert Trim.decode_lmdb_history_trim_items([{history_index_key, "corrupt"}], 1) ==
             {:error, {:invalid_history_trim_index, history_index_key}}
  end

  test "history trim event parsing uses the strict current-schema codec" do
    assert Trim.parse_event_ms("1000-1") == {:ok, 1_000}
    assert Trim.parse_event_ms("1000-1-extra") == :error
    assert Trim.parse_event_ms("01000-1") == :error
    assert Trim.parse_event_ms("1000-bad") == :error
  end

  test "history trim deletion fails closed on corrupt LMDB metadata" do
    shard_data_path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_history_trim_corrupt_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(shard_data_path) end)

    lmdb_path = Ferricstore.Flow.LMDB.path(shard_data_path)
    history_key = Ferricstore.Flow.Keys.history_key("trim-corrupt")
    event_id = "1000-1"

    history_index_key =
      Ferricstore.Flow.LMDB.history_index_key(history_key, event_id, 1_000)

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(lmdb_path, [
               {:put, history_index_key, "corrupt"}
             ])

    assert {:error, :invalid_history_index_value} =
             Trim.delete_lmdb_history_entries(shard_data_path, [
               {history_key, event_id, "compound-key", history_index_key}
             ])

    assert {:ok, "corrupt"} = Ferricstore.Flow.LMDB.get(lmdb_path, history_index_key)
  end

  test "history trim deletion rejects malformed event ids instead of skipping them" do
    shard_data_path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_history_trim_event_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(shard_data_path) end)

    history_key = Ferricstore.Flow.Keys.history_key("trim-event")

    assert {:error, {:invalid_history_event_id, "bad-event"}} =
             Trim.delete_lmdb_history_entries(shard_data_path, [
               {history_key, "bad-event", "compound-key"}
             ])
  end
end
