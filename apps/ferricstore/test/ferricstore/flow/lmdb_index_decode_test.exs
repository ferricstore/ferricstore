defmodule Ferricstore.Flow.LMDBIndexDecodeTest do
  use ExUnit.Case, async: false
  @moduletag :flow

  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.LMDBIndexDecode

  test "terminal entries keep live rows" do
    key = LMDB.terminal_index_key("terminal:type:done", "flow-1", 100)
    value = LMDB.encode_terminal_index_value("flow-1", 100, 1_000, "state-key")

    assert LMDBIndexDecode.terminal_entries([{key, value}], "unused", 999) == [{"flow-1", 100}]
  end

  test "query entries keep live rows and skip malformed values" do
    key = LMDB.query_index_key("parent:p1", "flow-1", 100)
    value = LMDB.encode_query_index_value("flow-1", 100, 0, "state-key")

    assert LMDBIndexDecode.query_entries([{key, value}, {"bad", "not-a-valid-index"}], "unused", 999) ==
             [{"flow-1", 100, "state-key"}]
  end

  test "history entries keep live rows" do
    key = LMDB.history_index_key("history:flow-1", "event-1", 100)
    value = LMDB.encode_history_index_value("event-1", 100, "history-key", 0)

    assert LMDBIndexDecode.history_entries([{key, value}], "unused", 999) == [{"event-1", 100}]
  end

  test "query entries delete expired rows" do
    path = tmp_lmdb_path()
    key = LMDB.query_index_key("parent:p1", "flow-1", 100)
    value = LMDB.encode_query_index_value("flow-1", 100, 10, "state-key")

    assert :ok = LMDB.write_batch(path, [{:put, key, value}])
    assert {:ok, ^value} = LMDB.get(path, key)

    assert LMDBIndexDecode.query_entries([{key, value}], path, 11) == []
    assert LMDB.get(path, key) == :not_found
  end

  defp tmp_lmdb_path do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-lmdb-index-decode-" <> Integer.to_string(System.unique_integer([:positive]))
      )

    File.rm_rf!(path)
    File.mkdir_p!(path)
    on_exit(fn -> File.rm_rf!(path) end)
    path
  end
end
