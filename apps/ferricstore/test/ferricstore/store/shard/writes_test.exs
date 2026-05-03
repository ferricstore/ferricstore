defmodule Ferricstore.Store.Shard.WritesTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.Shard.Writes

  test "direct put returns append error and rolls back ETS when flush fails" do
    keydir = :ets.new(:"writes_test_#{System.unique_integer([:positive])}", [:set, :public])

    state = %{
      active_file_path: Path.join(System.tmp_dir!(), "missing/writes_test.log"),
      active_file_id: 0,
      active_file_size: 0,
      file_stats: %{0 => {0, 0}},
      flush_in_flight: nil,
      index: 0,
      instance_ctx: %{
        checkpoint_flags: :atomics.new(1, signed: false),
        disk_pressure: :atomics.new(1, signed: false),
        hot_cache_max_value_size: 65_536,
        keydir_binary_bytes: :atomics.new(1, signed: true)
      },
      keydir: keydir,
      max_active_file_size: 64 * 1024 * 1024,
      pending: [],
      pending_count: 0,
      raft?: false,
      shard_data_path: System.tmp_dir!(),
      write_version: 0
    }

    try do
      assert {:reply, {:error, _reason}, new_state} =
               Writes.handle_put("key", "value", 0, {self(), make_ref()}, state)

      assert [] == :ets.lookup(keydir, "key")
      assert new_state.pending == []
      assert new_state.pending_count == 0
    after
      :ets.delete(keydir)
    end
  end
end
