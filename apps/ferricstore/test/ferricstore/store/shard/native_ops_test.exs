defmodule Ferricstore.Store.Shard.NativeOpsTest do
  use ExUnit.Case, async: true

  alias Ferricstore.ErrorReasons
  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Shard.NativeOps

  test "forwarded compound result returns unknown outcome when local apply barrier times out" do
    shard_index = 0
    batcher = Batcher.batcher_name(shard_index)
    %{last_local_applied: last_local_applied} = :sys.get_state(batcher)

    assert ErrorReasons.write_timeout_unknown() ==
             NativeOps.__barrier_forwarded_result__(
               shard_index,
               {:remote_applied_at, last_local_applied + 1_000, :ok},
               25
             )
  end

  test "forwarded compound result returns leader result after local apply barrier passes" do
    shard_index = 0
    batcher = Batcher.batcher_name(shard_index)
    %{last_local_applied: last_local_applied} = :sys.get_state(batcher)

    assert :ok ==
             NativeOps.__barrier_forwarded_result__(
               shard_index,
               {:remote_applied_at, last_local_applied, :ok},
               25
             )
  end

  test "direct list compound_put does not update ETS when Bitcask append fails" do
    keydir = :ets.new(:"native_ops_test_#{System.unique_integer([:positive])}", [:set, :public])
    compound_key = CompoundKey.list_element("list", 0)

    state = %{
      active_file_path: Path.join(System.tmp_dir!(), "missing/native_ops.log"),
      active_file_id: 0,
      instance_ctx: nil,
      keydir: keydir,
      index: 0,
      shard_data_path: System.tmp_dir!()
    }

    try do
      store = NativeOps.build_list_compound_store_direct("list", state)

      assert {:error, _reason} = store.compound_put.("list", compound_key, "value", 0)
      assert [] == :ets.lookup(keydir, compound_key)
    after
      :ets.delete(keydir)
    end
  end

  test "direct list writes update active file accounting" do
    dir =
      Path.join(System.tmp_dir!(), "native_ops_accounting_#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)

    keydir =
      :ets.new(:"native_ops_accounting_#{System.unique_integer([:positive])}", [
        :set,
        :public
      ])

    active_file_path = Path.join(dir, "00000.log")
    File.touch!(active_file_path)

    state = %{
      active_file_path: active_file_path,
      active_file_id: 0,
      active_file_size: 0,
      file_stats: %{0 => {0, 0}},
      flush_in_flight: nil,
      instance_ctx: nil,
      keydir: keydir,
      index: 0,
      max_active_file_size: 64 * 1024 * 1024,
      pending: [],
      pending_count: 0,
      raft?: false,
      shard_data_path: dir,
      write_version: 0
    }

    try do
      {:reply, 1, new_state} = NativeOps.handle_list_op("list", {:rpush, ["value"]}, state)

      assert new_state.active_file_size > 0
      assert {total_bytes, 0} = Map.fetch!(new_state.file_stats, 0)
      assert total_bytes == new_state.active_file_size
    after
      :ets.delete(keydir)
      File.rm_rf!(dir)
    end
  end

  test "direct list deletes update dead-byte accounting" do
    dir =
      Path.join(System.tmp_dir!(), "native_ops_dead_bytes_#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)

    keydir =
      :ets.new(:"native_ops_dead_bytes_#{System.unique_integer([:positive])}", [
        :set,
        :public
      ])

    active_file_path = Path.join(dir, "00000.log")
    File.touch!(active_file_path)

    state = %{
      active_file_path: active_file_path,
      active_file_id: 0,
      active_file_size: 0,
      file_stats: %{0 => {0, 0}},
      flush_in_flight: nil,
      instance_ctx: nil,
      keydir: keydir,
      index: 0,
      max_active_file_size: 64 * 1024 * 1024,
      pending: [],
      pending_count: 0,
      raft?: false,
      shard_data_path: dir,
      write_version: 0
    }

    try do
      {:reply, 1, state} = NativeOps.handle_list_op("list", {:rpush, ["value"]}, state)
      {:reply, "value", state} = NativeOps.handle_list_op("list", {:lpop, 1}, state)

      assert {_total_bytes, dead_bytes} = Map.fetch!(state.file_stats, 0)
      assert dead_bytes > 0
    after
      :ets.delete(keydir)
      File.rm_rf!(dir)
    end
  end
end
