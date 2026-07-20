defmodule Ferricstore.Flow.LMDBIndexDecodeTest do
  use ExUnit.Case, async: false
  @moduletag :flow

  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.LMDBIndexDecode

  test "terminal entries keep live rows" do
    key = LMDB.terminal_index_key("terminal:type:done", "flow-1", 100)
    count_key = LMDB.terminal_count_key("terminal:type:done")
    value = LMDB.encode_terminal_index_value("flow-1", 100, 1_000, "state-key", count_key)

    assert LMDBIndexDecode.terminal_entries([{key, value}], "unused", 999) ==
             {:ok, [{"flow-1", 100}]}
  end

  test "query entries keep live rows and reject malformed values" do
    {key, value} = LMDB.query_index_entry("parent:p1", "flow-1", 100, 0, "state-key")

    assert {:error, {:invalid_query_index_value, "bad"}} =
             LMDBIndexDecode.query_entries(
               [{key, value}, {"bad", "not-a-valid-index"}],
               "unused",
               999
             )
  end

  test "history entries keep live rows" do
    key = LMDB.history_index_key("history:flow-1", "event-1", 100)
    value = LMDB.encode_history_index_value("event-1", 100, "history-key", 0)

    assert LMDBIndexDecode.history_entries([{key, value}], "unused", 999) ==
             {:ok, [{"event-1", 100}]}
  end

  test "query history decoding skips expired rows without deleting them" do
    path = tmp_lmdb_path()
    key = LMDB.history_index_key("history:flow-1", "event-1", 100)
    value = LMDB.encode_history_index_value("event-1", 100, "history-key", 10)

    assert :ok = LMDB.write_batch(path, [{:put, key, value}])
    assert LMDBIndexDecode.history_query_entries([{key, value}], 11) == {:ok, []}
    assert {:ok, ^value} = LMDB.get(path, key)
  end

  test "ordered index entries reject key and value identity mismatches" do
    terminal_key = LMDB.terminal_index_key("terminal:type:done", "flow-1", 100)
    terminal_count_key = LMDB.terminal_count_key("terminal:type:done")

    terminal_value =
      LMDB.encode_terminal_index_value(
        "other-flow",
        100,
        0,
        "state-key",
        terminal_count_key
      )

    assert {:error, {:invalid_terminal_index_value, ^terminal_key}} =
             LMDBIndexDecode.terminal_entries([{terminal_key, terminal_value}], "unused", 999)

    query_key = LMDB.query_index_key("parent:p1", "flow-1", 100)
    query_value = LMDB.encode_query_index_value("parent:p1", "flow-1", 101, 0, "state-key")

    assert {:error, {:invalid_query_index_value, ^query_key}} =
             LMDBIndexDecode.query_entries([{query_key, query_value}], "unused", 999)

    history_key = LMDB.history_index_key("history:flow-1", "event-1", 100)
    history_value = LMDB.encode_history_index_value("event-2", 100, "compound-key", 0)

    assert {:error, {:invalid_history_index_value, ^history_key}} =
             LMDBIndexDecode.history_entries([{history_key, history_value}], "unused", 999)
  end

  test "query entries delete expired rows" do
    path = tmp_lmdb_path()
    {key, value} = LMDB.query_index_entry("parent:p1", "flow-1", 100, 10, "state-key")

    assert :ok = LMDB.write_batch(path, [{:put, key, value}])
    assert {:ok, ^value} = LMDB.get(path, key)

    assert LMDBIndexDecode.query_entries([{key, value}], path, 11) == {:ok, []}
    assert LMDB.get(path, key) == :not_found
  end

  test "query read decoding skips expired rows without deleting them" do
    path = tmp_lmdb_path()

    {expired_key, expired_value} =
      LMDB.query_index_entry("parent:p1", "expired", 100, 10, "expired-state")

    {live_key, live_value} =
      LMDB.query_index_entry("parent:p1", "live", 101, 0, "live-state")

    assert :ok =
             LMDB.write_batch(path, [
               {:put, expired_key, expired_value},
               {:put, live_key, live_value}
             ])

    assert {:ok, [{"live", 101, "live-state"}]} =
             LMDBIndexDecode.query_entries_readonly(
               [{expired_key, expired_value}, {live_key, live_value}],
               11
             )

    assert {:ok, ^expired_value} = LMDB.get(path, expired_key)
  end

  @tag :lmdb_reverse_before_cursor
  test "reverse-before scan excludes an exact cursor key before applying its limit" do
    path = tmp_lmdb_path()
    prefix = "cursor:"
    first_key = prefix <> "a"
    cursor_key = prefix <> "b"

    assert :ok =
             LMDB.write_batch(path, [
               {:put, first_key, "first"},
               {:put, cursor_key, "cursor"},
               {:put, prefix <> "c", "last"}
             ])

    assert {:ok, [{^first_key, "first"}]} =
             LMDB.prefix_entries_reverse_before(path, prefix, cursor_key, 1)
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
