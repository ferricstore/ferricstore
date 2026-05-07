defmodule Ferricstore.ReplicationModeTest do
  use ExUnit.Case, async: false

  alias Ferricstore.ReplicationMode

  setup do
    dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-replication-mode-#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(dir)
    File.mkdir_p!(dir)

    old_mode = Application.get_env(:ferricstore, :raft_mode)

    on_exit(fn ->
      File.rm_rf!(dir)

      if old_mode == nil do
        Application.delete_env(:ferricstore, :raft_mode)
      else
        Application.put_env(:ferricstore, :raft_mode, old_mode)
      end

      ReplicationMode.put_current(:raft)
    end)

    {:ok, dir: dir}
  end

  test "manual env resolves to standalone when no marker exists", %{dir: dir} do
    Application.put_env(:ferricstore, :raft_mode, :manual)

    assert :standalone = ReplicationMode.resolve!(dir, 4)
  end

  test "raft marker overrides manual env", %{dir: dir} do
    Application.put_env(:ferricstore, :raft_mode, :manual)

    ReplicationMode.mark_raft!(dir, 4, 1, %{0 => 7})

    assert :raft = ReplicationMode.resolve!(dir, 4)

    assert {:ok, %{replication_mode: :raft, barrier_indices: %{0 => 7}}} =
             ReplicationMode.read(dir)
  end

  test "promotion markers preserve the data-dir cluster identity", %{dir: dir} do
    ReplicationMode.mark_standalone!(dir, 4)
    {:ok, %{cluster_id: cluster_id}} = ReplicationMode.read(dir)

    ReplicationMode.mark_enabling!(dir, 4, 11)

    assert {:ok, %{cluster_id: ^cluster_id, replication_mode: :enabling}} =
             ReplicationMode.read(dir)

    ReplicationMode.mark_raft!(dir, 4, 11, %{0 => 42})

    assert {:ok, %{cluster_id: ^cluster_id, replication_mode: :raft}} =
             ReplicationMode.read(dir)
  end

  test "corrupted marker fails closed", %{dir: dir} do
    File.write!(ReplicationMode.marker_path(dir), "not a marker")

    assert_raise RuntimeError, ~r/failed to read cluster_state marker/, fn ->
      ReplicationMode.resolve!(dir, 4)
    end
  end

  test "shard count mismatch fails closed", %{dir: dir} do
    ReplicationMode.mark_raft!(dir, 4, 1, %{})

    assert_raise RuntimeError, ~r/shard_count mismatch/, fn ->
      ReplicationMode.resolve!(dir, 8)
    end
  end
end
