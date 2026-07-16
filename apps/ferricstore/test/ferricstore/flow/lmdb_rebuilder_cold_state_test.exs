defmodule Ferricstore.Flow.LMDBRebuilder.ColdStateTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.{Keys, LMDBRebuilder}
  alias Ferricstore.Flow.LMDBRebuilder.ColdState
  alias Ferricstore.Store.LFU

  test "malformed source state is counted as a rebuild read failure" do
    Process.put(:flow_lmdb_rebuild_cold_read_errors, 0)

    on_exit(fn -> Process.delete(:flow_lmdb_rebuild_cold_read_errors) end)

    assert [] = ColdState.decode_state_record("flow-state", "corrupt", 0, nil, nil)
    assert Process.get(:flow_lmdb_rebuild_cold_read_errors) == 1
  end

  test "active index rebuild rejects a partial decode" do
    keydir = :ets.new(:flow_active_index_partial_decode, [:set])
    state_key = Keys.state_key("partial-active-index")

    :ets.insert(
      keydir,
      {state_key, "corrupt", 0, LFU.initial(), :memory, 0, byte_size("corrupt")}
    )

    assert {:error, {:cold_read_errors, 1}} =
             LMDBRebuilder.rebuild_active_indexes_from_keydir(
               System.tmp_dir!(),
               keydir,
               0,
               nil,
               nil,
               nil,
               nil,
               nil
             )
  end
end
