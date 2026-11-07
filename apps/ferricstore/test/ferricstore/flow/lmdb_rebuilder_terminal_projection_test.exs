defmodule Ferricstore.Flow.LMDBRebuilder.TerminalProjectionTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.LMDBRebuilder.TerminalProjection

  test "a digest reverse row whose owner cannot be found is left in place" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "terminal_reverse_unknown_#{System.unique_integer([:positive])}"
      )

    Ferricstore.DataDir.ensure_layout!(data_dir, 1)
    on_exit(fn -> File.rm_rf!(data_dir) end)
    path = Ferricstore.Flow.LMDB.path(Ferricstore.DataDir.shard_data_path(data_dir, 0))
    state_key = Ferricstore.Flow.Keys.state_key(String.duplicate("x", 250), "tenant")
    reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)
    refute String.ends_with?(reverse_key, state_key)

    :ok = Ferricstore.Flow.LMDB.write_batch(path, [{:put, reverse_key, "missing-terminal-key"}])
    keydir = :ets.new(:terminal_projection_unknown_keydir, [:set])

    assert {:ok, 0} =
             TerminalProjection.cleanup_stale_terminal_reverse(path, keydir, fn _entry -> [] end)

    assert {:ok, "missing-terminal-key"} = Ferricstore.Flow.LMDB.get(path, reverse_key)
  end

  test "terminal reverse scans preserve backend failures" do
    keydir = :ets.new(:terminal_projection_reverse_keydir, [:set])

    assert {:ok, []} =
             TerminalProjection.__cleanup_stale_terminal_reverse_scan_result_for_test__(
               {:ok, []},
               keydir,
               fn _entry -> [] end
             )

    assert {:error, :busy} =
             TerminalProjection.__cleanup_stale_terminal_reverse_scan_result_for_test__(
               {:error, :busy},
               keydir,
               fn _entry -> [] end
             )
  end

  test "terminal reverse scans preserve strict delete-planning failures" do
    keydir = :ets.new(:terminal_projection_delete_keydir, [:set])
    state_key = "flow-state-key"
    reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)

    assert {:error, :busy} =
             TerminalProjection.__cleanup_stale_terminal_reverse_scan_with_delete_for_test__(
               {:ok, [{reverse_key, "terminal-key"}]},
               keydir,
               fn _entry -> [] end,
               fn "terminal-key", ^state_key -> {:error, :busy} end
             )
  end

  test "stale terminal cleanup never deletes the current active state row" do
    state_key = "current-active-state"
    terminal_key = "stale-terminal-key"

    terminal_value =
      Ferricstore.Flow.LMDB.encode_terminal_index_value(
        "flow-id",
        10,
        0,
        state_key,
        "count-key"
      )

    parent = self()

    delete_fun = fn ^terminal_key, delete_state_key ->
      send(parent, {:delete_state_key, delete_state_key})
      {:ok, [{:delete, terminal_key}]}
    end

    assert {:ok, ops} =
             TerminalProjection.__cleanup_stale_terminal_entries_for_test__(
               {:ok, [{terminal_key, terminal_value}]},
               "flow-id",
               state_key,
               false,
               delete_fun
             )

    assert_receive {:delete_state_key, nil}
    refute {:delete, state_key} in ops
    assert {:delete, Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)} in ops
  end

  test "terminal rebuild scan limits reject non-positive, malformed, and excessive values" do
    assert TerminalProjection.__normalize_scan_limit_for_test__(0) == 4_096
    assert TerminalProjection.__normalize_scan_limit_for_test__("all") == 4_096
    assert TerminalProjection.__normalize_scan_limit_for_test__(2_000_000) == 65_536
    assert TerminalProjection.__normalize_scan_limit_for_test__(5_000) == 5_000
  end

  test "active-record cleanup does not fall back to a full terminal-index scan" do
    source =
      File.read!(
        Path.expand(
          "../../../lib/ferricstore/flow/lmdb_rebuilder/terminal_projection.ex",
          __DIR__
        )
      )

    [_before, cleanup_source] =
      String.split(source, "defp cleanup_stale_terminal_ops_by_id(", parts: 2)

    [cleanup_source, _after] =
      String.split(cleanup_source, "defp terminal_state_key_from_reverse_key", parts: 2)

    refute cleanup_source =~ "LMDB.terminal_index_global_prefix()"
  end
end
