defmodule Ferricstore.Cluster.TargetMarkerTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Cluster.TargetMarker
  alias Ferricstore.ReplicationMode

  setup do
    dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-target-marker-#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(dir)
    File.mkdir_p!(dir)

    on_exit(fn ->
      File.rm_rf!(dir)
      ReplicationMode.put_current(:raft)
    end)

    %{dir: dir}
  end

  test "persists target raft marker with local cluster identity", %{dir: dir} do
    ReplicationMode.mark_raft!(dir, 4, 9, %{0 => 7})
    {:ok, %{cluster_id: cluster_id}} = ReplicationMode.read(dir)

    parent = self()
    target_ctx = %{data_dir: "/target/data"}
    ctx = %{data_dir: dir, shard_count: 4}

    rpc = fn
      :target@local, FerricStore.Instance, :get, [:default], 5_000 ->
        target_ctx

      :target@local, ReplicationMode, :write!, ["/target/data", attrs], 5_000 ->
        send(parent, {:target_marker_attrs, attrs})
        :ok
    end

    assert :ok = TargetMarker.write(:target@local, ctx, %{0 => 42}, rpc: rpc)

    assert_receive {:target_marker_attrs,
                    %{
                      replication_mode: :raft,
                      shard_count: 4,
                      cluster_id: ^cluster_id,
                      promotion_epoch: 9,
                      barrier_indices: %{0 => 42}
                    }}
  end

  test "returns error when target marker write fails", %{dir: dir} do
    ReplicationMode.mark_raft!(dir, 4, 9, %{})

    ctx = %{data_dir: dir, shard_count: 4}

    rpc = fn
      :target@local, FerricStore.Instance, :get, [:default], 5_000 ->
        %{data_dir: "/target/data"}

      :target@local, ReplicationMode, :write!, ["/target/data", _attrs], 5_000 ->
        exit(:simulated_marker_failure)
    end

    assert {:error,
            {:target_cluster_marker_write_failed, :target@local,
             {:exit, :simulated_marker_failure}}} =
             TargetMarker.write(:target@local, ctx, %{0 => 42}, rpc: rpc)
  end

  test "returns error when local cluster identity is unavailable", %{dir: dir} do
    ctx = %{data_dir: dir, shard_count: 4}

    assert {:error, {:local_cluster_state_unreadable, :enoent}} =
             TargetMarker.write(:target@local, ctx, %{}, rpc: fn _, _, _, _, _ -> :ok end)
  end

  test "returns error when remote default instance context is malformed", %{dir: dir} do
    ReplicationMode.mark_raft!(dir, 4, 9, %{})

    ctx = %{data_dir: dir, shard_count: 4}

    rpc = fn
      :target@local, FerricStore.Instance, :get, [:default], 5_000 -> nil
    end

    assert {:error,
            {:target_cluster_marker_write_failed, :target@local, {:invalid_target_context, nil}}} =
             TargetMarker.write(:target@local, ctx, %{0 => 42}, rpc: rpc)
  end
end
