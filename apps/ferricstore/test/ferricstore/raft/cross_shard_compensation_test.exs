defmodule Ferricstore.Raft.CrossShardCompensationTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Raft.StateMachine

  test "compensation append failure is returned to caller" do
    dir =
      Path.join(
        System.tmp_dir!(),
        "cross_shard_compensation_append_fail_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    bad_file_path = Path.join(dir, "active_is_directory.log")
    File.mkdir_p!(bad_file_path)
    keydir = :ets.new(:"cross_shard_compensation_#{System.unique_integer([:positive])}", [:set])

    state = %{
      shard_index: 0,
      instance_ctx: nil,
      active_file_id: 0,
      active_file_path: bad_file_path,
      active_file_size: 0,
      file_stats: %{},
      shard_data_path: dir
    }

    successful_groups = [
      {0, bad_file_path, 0, keydir,
       [{:put, 0, keydir, bad_file_path, 0, "compensate_missing", "new", "new", 0}]}
    ]

    originals = %{{keydir, "compensate_missing"} => {0, :missing}}

    try do
      assert {:error, {:compensation_append_failed, _reason}, ^state} =
               StateMachine.__compensate_cross_shard_partial_writes_for_test__(
                 state,
                 successful_groups,
                 originals
               )
    after
      :ets.delete(keydir)
      File.rm_rf!(dir)
    end
  end

  test "compensation cold read failure is returned to caller" do
    dir =
      Path.join(
        System.tmp_dir!(),
        "cross_shard_compensation_cold_read_fail_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    file_path = Path.join(dir, "00000.log")
    File.write!(file_path, "")
    keydir = :ets.new(:"cross_shard_compensation_#{System.unique_integer([:positive])}", [:set])

    state = %{
      shard_index: 0,
      instance_ctx: nil,
      active_file_id: 0,
      active_file_path: file_path,
      active_file_size: 0,
      file_stats: %{},
      shard_data_path: dir
    }

    successful_groups = [
      {0, file_path, 0, keydir,
       [{:put, 0, keydir, file_path, 0, "retired_cold", "new", "new", 0}]}
    ]

    originals = %{
      {keydir, "retired_cold"} =>
        {0, {:entry, {"retired_cold", nil, 0, 0, 7, 0, byte_size("old")}}}
    }

    try do
      assert {:error, {:compensation_read_failed, "retired_cold", _reason}, ^state} =
               StateMachine.__compensate_cross_shard_partial_writes_for_test__(
                 state,
                 successful_groups,
                 originals
               )
    after
      :ets.delete(keydir)
      File.rm_rf!(dir)
    end
  end
end
