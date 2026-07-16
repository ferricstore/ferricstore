defmodule Ferricstore.Flow.LMDBRebuilder.ColdStateTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.LMDBRebuilder.ColdState

  test "malformed source state is counted as a rebuild read failure" do
    Process.put(:flow_lmdb_rebuild_cold_read_errors, 0)

    on_exit(fn -> Process.delete(:flow_lmdb_rebuild_cold_read_errors) end)

    assert [] = ColdState.decode_state_record("flow-state", "corrupt", 0, nil, nil)
    assert Process.get(:flow_lmdb_rebuild_cold_read_errors) == 1
  end
end
