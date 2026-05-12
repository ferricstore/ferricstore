defmodule Ferricstore.Raft.ClusterStartErrorTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Raft.Cluster

  @root Path.expand("../../..", __DIR__)
  @application_path Path.join(@root, "lib/ferricstore/application.ex")
  @cluster_path Path.join(@root, "lib/ferricstore/raft/cluster.ex")

  describe "start error recovery action" do
    test "nested supervisor already_started uses same UID restart" do
      pid = self()

      reason =
        {:shutdown, {:failed_to_start_child, :ferricstore_shard_0, {:already_started, pid}}}

      assert Cluster.start_error_recovery_action(reason) == :same_uid_restart
    end

    test "direct already_started uses same UID restart" do
      assert Cluster.start_error_recovery_action({:already_started, self()}) ==
               :same_uid_restart
    end

    test "existing raft state restarts with the same UID" do
      assert Cluster.start_error_recovery_action(:not_new) == :existing_state_restart
    end

    test "other failures fail closed instead of deleting raft state" do
      assert Cluster.start_error_recovery_action(:shutdown) == :fail_closed
    end

    test "startup code does not use fresh UID recovery" do
      source = File.read!(@cluster_path)

      refute source =~ "fresh_uid"
      refute source =~ "fresh start with unique UID"
      refute source =~ "restart_uid"
    end

    test "application checks ra system startup result" do
      assert ok_match_to_start_system?(@application_path)
    end

    test "start_system reports ra directory fsync failure before starting ra" do
      root =
        Path.join(System.tmp_dir!(), "raft_cluster_fsync_#{System.unique_integer([:positive])}")

      parent = self()

      Process.put(:ferricstore_raft_cluster_fsync_dir_hook, fn path ->
        send(parent, {:raft_cluster_fsync, path})
        {:error, :eio}
      end)

      try do
        assert {:error, {:fsync_dir_failed, :create_ra_dir, :eio}} =
                 Cluster.start_system(root)

        assert_received {:raft_cluster_fsync, ^root}
      after
        Process.delete(:ferricstore_raft_cluster_fsync_dir_hook)
        File.rm_rf!(root)
      end
    end

    test "application waits for parallel shard elections before readiness" do
      source = File.read!(@application_path)

      election_pos =
        :binary.match(source, "Ferricstore.Raft.Cluster.trigger_shard_elections_parallel")

      ready_pos = :binary.match(source, "mark_started(shard_count)")

      assert match?({_, _}, election_pos)
      assert match?({_, _}, ready_pos)

      {election_offset, _} = election_pos
      {ready_offset, _} = ready_pos

      assert election_offset < ready_offset
    end
  end

  describe "startup election mode" do
    test "direct start waits for leader unless startup explicitly defers it" do
      assert Cluster.wait_for_leader_on_start?([]) == true
      assert Cluster.wait_for_leader_on_start?(wait_for_leader: true) == true
      assert Cluster.wait_for_leader_on_start?(wait_for_leader: false) == false
    end

    test "raft log maintenance is throttled separately from release cursor" do
      args = Cluster.log_init_args_for_shard(0)

      assert args.min_snapshot_interval == 10_000_000
      assert args.min_checkpoint_interval == 1_000_000
    end

    test "ra system segment size is configurable for tail-latency profiling" do
      original_segment = Application.get_env(:ferricstore, :ra_segment_max_entries)
      original_segment_size = Application.get_env(:ferricstore, :ra_segment_max_size_bytes)
      original_wal_size = Application.get_env(:ferricstore, :ra_wal_max_size_bytes)
      original_checksums = Application.get_env(:ferricstore, :ra_wal_compute_checksums)

      Application.put_env(:ferricstore, :ra_segment_max_entries, 123_456)
      Application.put_env(:ferricstore, :ra_segment_max_size_bytes, 456_789)
      Application.put_env(:ferricstore, :ra_wal_max_size_bytes, 987_654)
      Application.put_env(:ferricstore, :ra_wal_compute_checksums, false)

      on_exit(fn ->
        restore_env(:ra_segment_max_entries, original_segment)
        restore_env(:ra_segment_max_size_bytes, original_segment_size)
        restore_env(:ra_wal_max_size_bytes, original_wal_size)
        restore_env(:ra_wal_compute_checksums, original_checksums)
      end)

      config = Cluster.system_config("/tmp/ferricstore-ra-config-test")

      assert config.segment_max_entries == 123_456
      assert config.segment_max_size_bytes == 456_789
      assert config.wal_max_size_bytes == 987_654
      assert config.wal_max_batch_size == 32_768
      assert config.wal_compute_checksums == false
    end

    test "ra system default WAL cap avoids frequent high-throughput rollover" do
      original_wal_size = Application.get_env(:ferricstore, :ra_wal_max_size_bytes)
      Application.delete_env(:ferricstore, :ra_wal_max_size_bytes)

      on_exit(fn -> restore_env(:ra_wal_max_size_bytes, original_wal_size) end)

      config = Cluster.system_config("/tmp/ferricstore-ra-default-config-test")

      assert config.wal_max_size_bytes == 8_589_934_592
    end

    test "raft snapshot throttle can be tuned for recovery tests" do
      original_snapshot = Application.get_env(:ferricstore, :ra_min_snapshot_interval)
      original_checkpoint = Application.get_env(:ferricstore, :ra_min_checkpoint_interval)

      Application.put_env(:ferricstore, :ra_min_snapshot_interval, 1)
      Application.put_env(:ferricstore, :ra_min_checkpoint_interval, 1)

      on_exit(fn ->
        restore_env(:ra_min_snapshot_interval, original_snapshot)
        restore_env(:ra_min_checkpoint_interval, original_checkpoint)
      end)

      args = Cluster.log_init_args_for_shard(0)

      assert args.min_snapshot_interval == 1
      assert args.min_checkpoint_interval == 1
    end

    test "recovery starts from the highest explicit or persisted replay-safe index" do
      tmp_dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore_replay_skip_#{System.unique_integer([:positive])}"
        )

      on_exit(fn -> File.rm_rf!(tmp_dir) end)

      assert Cluster.replay_skip_below_index(tmp_dir, []) == 0
      assert Cluster.replay_skip_below_index(tmp_dir, skip_below_index: 40) == 40

      :ok = Ferricstore.Raft.ReplaySafeIndex.persist(tmp_dir, 123)

      assert Cluster.replay_skip_below_index(tmp_dir, []) == 123
      assert Cluster.replay_skip_below_index(tmp_dir, skip_below_index: 40) == 123
      assert Cluster.replay_skip_below_index(tmp_dir, skip_below_index: 200) == 200
    end
  end

  defp ok_match_to_start_system?(path) do
    {:ok, ast} =
      path
      |> File.read!()
      |> Code.string_to_quoted(columns: true)

    {_ast, found?} =
      Macro.prewalk(ast, false, fn
        {:=, _meta, [left, right]} = node, acc ->
          found? =
            acc or
              (Macro.to_string(left) == ":ok" and
                 Macro.to_string(right) == "Ferricstore.Raft.Cluster.start_system(data_dir)")

          {node, found?}

        node, acc ->
          {node, acc}
      end)

    found?
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
